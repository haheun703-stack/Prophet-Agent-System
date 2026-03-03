# -*- coding: utf-8 -*-
"""
동적 목표가 엔진 + 트레일링 스탑 — 매일 재평가하여 SL/TP 조정
================================================================
3 GAP 통합:
  GAP 1: ATR 기반 초기 SL/TP (고정% → ATR)
  GAP 2: 뉴스 감성 → 목표가 보정
  GAP 3: 매집원가 → SL 하한선

매일 재평가 요소:
  ① 뉴스 감성 (키워드 기반)
  ② 수급 (외인+기관 3일 연속)
  ③ RSI / 볼린저 밴드
  ④ 증권사 목표가 변동

★ 트레일링 스탑 (v5.1):
  - 수익 +3% 또는 TP 도달 시 트레일링 모드 활성화
  - 고점(high_watermark) 대비 하락폭으로 판정
  - 조정 vs 수급이탈 구분: 수급OK→홀딩, 수급이탈→매도
  - 고점 -2% 미만: 홀딩 (상승 중)
  - 고점 -2~3.5%: 수급이탈→전량매도, 수급OK→홀딩(조정)
  - 고점 -3.5~5%: 수급이탈→전량매도, 수급OK→부분매도
  - 고점 -5% 이상: 무조건 전량매도 (하드스탑)
  - SL ratchet: 한번 올라간 SL은 절대 내려가지 않음
"""

import logging
import numpy as np
import pandas as pd
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional

from data.extend_parquet_data import load_daily

logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent.parent
DAILY_DIR = BASE_DIR / "data_store" / "daily"
FLOW_DIR = BASE_DIR / "data_store" / "flow"

# 판정 액션
ACTION_FULL_SELL = "FULL_SELL"       # 전량매도
ACTION_PARTIAL_SELL = "PARTIAL_SELL"  # 부분매도 50%
ACTION_HOLD = "HOLD"                 # 홀딩
ACTION_ADD = "ADD"                   # 추가매수 검토
ACTION_STOP_LOSS = "STOP_LOSS"       # 손절


@dataclass
class TargetState:
    """포지션별 동적 목표가 상태"""
    code: str
    name: str
    entry_price: int
    entry_date: str

    # 초기 목표가 (ATR 기반)
    initial_tp: int = 0
    initial_sl: int = 0

    # 동적 목표가 (매일 재조정)
    dynamic_tp: int = 0
    dynamic_sl: int = 0

    # 매집원가 SL 하한선
    inst_cost_sl: int = 0

    # 보정 요인 기록
    news_adj: float = 0.0       # 뉴스 보정 (%)
    flow_adj: float = 0.0       # 수급 보정 (%)
    rsi_adj: float = 0.0        # RSI 보정 (%)
    analyst_adj: float = 0.0    # 증권사 보정 (%)

    # 판정
    action: str = ACTION_HOLD
    reason: str = ""
    current_price: int = 0
    pnl_pct: float = 0.0

    # 트레일링 스탑 (v5.1)
    high_watermark: int = 0           # 보유 이래 최고가
    trailing_activated: bool = False  # 트레일링 모드 활성화됨
    trailing_sl: int = 0             # 트레일링 손절가 (ratchet: 절대 하락 안 함)


class DynamicTargetEngine:
    """동적 목표가 재평가 엔진"""

    def __init__(self):
        pass

    # ═══════════════════════════════════════════════════
    #  1. 초기 목표가 설정 (ATR 기반 + 매집원가 SL)
    # ═══════════════════════════════════════════════════

    def initial_setup(self, code: str, name: str, entry_price: int,
                      entry_date: str, df: pd.DataFrame = None) -> TargetState:
        """매수 시점에 초기 SL/TP 계산

        - SL = Entry - ATR×0.8
        - TP = Entry + ATR×1.6 (2R)
        - 매집원가 SL 하한선 = max(매집원가×0.97, ATR SL)
        """
        if df is None:
            df = load_daily(code)
            if df is None:
                return self._fallback_state(code, name, entry_price, entry_date)

        if len(df) < 20:
            return self._fallback_state(code, name, entry_price, entry_date)

        close = df["종가"].values.astype(float)
        high = df["고가"].values.astype(float)
        low = df["저가"].values.astype(float)

        # ATR 14일
        tr = np.maximum(
            high[-14:] - low[-14:],
            np.maximum(
                np.abs(high[-14:] - np.append(close[-15:-14], close[-14:-1])),
                np.abs(low[-14:] - np.append(close[-15:-14], close[-14:-1]))
            )
        )
        atr = float(np.mean(tr))

        # 초기 SL/TP
        atr_sl = int(entry_price - atr * 0.5)
        # SL 최대 -5% 캡 (ATR이 너무 넓으면 제한)
        max_sl = int(entry_price * 0.95)
        initial_sl = max(atr_sl, max_sl)

        initial_tp = int(entry_price + atr * 1.6)

        # 매집원가 계산 (20일 VWAP 기관순매수 가중)
        inst_cost = self._calc_inst_cost(code, df)
        inst_cost_sl = int(inst_cost * 0.97) if inst_cost > 0 else 0

        # SL = max(매집원가 방어선, ATR SL) — 가장 타이트한 것 사용
        final_sl = max(inst_cost_sl, initial_sl) if inst_cost_sl > 0 else initial_sl

        state = TargetState(
            code=code,
            name=name,
            entry_price=entry_price,
            entry_date=entry_date,
            initial_tp=initial_tp,
            initial_sl=initial_sl,
            dynamic_tp=initial_tp,
            dynamic_sl=final_sl,
            inst_cost_sl=inst_cost_sl,
        )
        return state

    def _calc_inst_cost(self, code: str, df: pd.DataFrame) -> float:
        """기관 매집원가 계산 (20일 VWAP 기반)"""
        flow_path = FLOW_DIR / f"{code}_investor.csv"
        if not flow_path.exists():
            return 0.0

        try:
            fdf = pd.read_csv(flow_path, index_col=0, parse_dates=True)
            if len(fdf) < 20:
                return 0.0

            # 최근 20일 기관+외국인 순매수량
            inst_qty = fdf["기관_수량"].values[-20:].astype(float)
            frgn_qty = fdf["외국인_수량"].values[-20:].astype(float)
            net_qty = inst_qty + frgn_qty

            close = df["종가"].values[-20:].astype(float)

            # 순매수가 양수인 날만으로 VWAP
            buy_mask = net_qty > 0
            if buy_mask.sum() < 3:
                return 0.0

            vwap = np.sum(close[buy_mask] * net_qty[buy_mask]) / np.sum(net_qty[buy_mask])
            return float(vwap)

        except Exception as e:
            logger.debug(f"매집원가 계산 실패 {code}: {e}")
            return 0.0

    def _fallback_state(self, code: str, name: str, entry_price: int,
                        entry_date: str) -> TargetState:
        """데이터 부족 시 고정 % 기반 폴백"""
        return TargetState(
            code=code,
            name=name,
            entry_price=entry_price,
            entry_date=entry_date,
            initial_tp=int(entry_price * 1.05),
            initial_sl=int(entry_price * 0.97),
            dynamic_tp=int(entry_price * 1.05),
            dynamic_sl=int(entry_price * 0.97),
        )

    # ═══════════════════════════════════════════════════
    #  2. 매일 재평가
    # ═══════════════════════════════════════════════════

    def daily_reeval(self, state: TargetState, current_price: int,
                     eval_date: str = None,
                     df: pd.DataFrame = None,
                     flow_df: pd.DataFrame = None,
                     news_score: float = 0.0) -> TargetState:
        """매일 목표가 재평가

        Args:
            state: 현재 포지션 상태
            current_price: 당일 종가
            eval_date: 평가 날짜
            df: 일봉 DataFrame (없으면 파일에서 로드)
            flow_df: 수급 DataFrame (없으면 파일에서 로드)
            news_score: 뉴스 감성 점수 (-10 ~ +10)
        """
        code = state.code
        if df is None:
            df = load_daily(code)
            if df is None:
                state.current_price = current_price
                state.action = ACTION_HOLD
                return state

        if flow_df is None:
            flow_path = FLOW_DIR / f"{code}_investor.csv"
            if flow_path.exists():
                try:
                    flow_df = pd.read_csv(flow_path, index_col=0, parse_dates=True)
                except Exception:
                    flow_df = None

        state.current_price = current_price
        state.pnl_pct = (current_price / state.entry_price - 1) * 100

        # ─── 보정 계산 ───

        # ① 뉴스 감성 보정
        state.news_adj = self._news_adjustment(news_score)

        # ② 수급 보정 (3일 연속 체크)
        state.flow_adj = self._flow_adjustment(flow_df, eval_date)

        # ③ RSI/볼린저 보정
        state.rsi_adj = self._rsi_bollinger_adjustment(df)

        # ④ 증권사 목표가 (백테스트에서는 0 사용)
        # state.analyst_adj = self._analyst_adjustment(code)
        state.analyst_adj = 0.0

        # ─── 목표가 재조정 ───
        total_adj = state.news_adj + state.flow_adj + state.rsi_adj + state.analyst_adj
        # 보정 범위 제한: -15% ~ +15%
        total_adj = max(-15.0, min(15.0, total_adj))

        # 동적 TP = 초기 TP × (1 + 보정%)
        state.dynamic_tp = int(state.initial_tp * (1 + total_adj / 100))

        # 동적 SL = max(매집원가 SL, 초기 SL × (1 + 하향보정))
        # 부정적 보정 시 SL을 올림 (더 빨리 탈출)
        if total_adj < -5:
            sl_adj = int(state.initial_sl * (1 + abs(total_adj) / 200))
            state.dynamic_sl = max(sl_adj, state.inst_cost_sl) if state.inst_cost_sl > 0 else sl_adj
        else:
            state.dynamic_sl = max(state.initial_sl, state.inst_cost_sl) if state.inst_cost_sl > 0 else state.initial_sl

        # ─── 고점 갱신 (daily_reeval 진입 시) ───
        state.high_watermark = max(state.high_watermark or state.entry_price, current_price)

        # ─── 판정 (트레일링 + 수급 판정 포함) ───
        state.action, state.reason = self._decide_action(state, flow_df)

        return state

    def _news_adjustment(self, news_score: float) -> float:
        """뉴스 감성 → 목표가 보정%

        news_score: -10 ~ +10
        → 보정: -5% ~ +5%
        """
        return news_score * 0.5  # 1점당 0.5%

    def _flow_adjustment(self, flow_df: pd.DataFrame, eval_date: str = None) -> float:
        """수급 → 목표가 보정%

        3일 연속 순매수 → +2%
        3일 연속 순매도 → -3%
        """
        if flow_df is None or len(flow_df) < 3:
            return 0.0

        try:
            if eval_date:
                # 백테스트: eval_date 이전 데이터만 사용
                mask = flow_df.index <= eval_date
                fdf = flow_df[mask]
            else:
                fdf = flow_df

            if len(fdf) < 3:
                return 0.0

            # 최근 3일 기관+외인 순매수량
            inst_3d = fdf["기관_수량"].values[-3:].astype(float)
            frgn_3d = fdf["외국인_수량"].values[-3:].astype(float)
            net_3d = inst_3d + frgn_3d

            if all(n > 0 for n in net_3d):
                return 2.0   # 3일 연속 순매수
            elif all(n < 0 for n in net_3d):
                return -3.0  # 3일 연속 순매도
            else:
                # 최근 3일 합계 기반 미세 조정
                total = net_3d.sum()
                if total > 0:
                    return 0.5
                elif total < 0:
                    return -0.5
                return 0.0

        except Exception:
            return 0.0

    def _rsi_bollinger_adjustment(self, df: pd.DataFrame) -> float:
        """RSI + 볼린저 → 목표가 보정%

        RSI > 75: 과열 → -3% (익절 유도)
        RSI < 30: 과매도 → +2% (반등 기대)
        볼린저 하단 이탈: -5% (위험)
        볼린저 상단 돌파: -2% (과열)
        """
        if len(df) < 20:
            return 0.0

        try:
            close = df["종가"].values.astype(float)
            adj = 0.0

            # RSI 14일
            deltas = np.diff(close[-15:])
            gains = np.maximum(deltas, 0).mean()
            losses = np.abs(np.minimum(deltas, 0)).mean()
            rsi = 100 - (100 / (1 + gains / losses)) if losses > 0 else 100

            if rsi > 75:
                adj -= 3.0   # 과열 → 목표가 하향
            elif rsi > 70:
                adj -= 1.5
            elif rsi < 30:
                adj += 2.0   # 과매도 → 반등 기대
            elif rsi < 40:
                adj += 1.0

            # 볼린저 밴드
            ma20 = np.mean(close[-20:])
            std20 = np.std(close[-20:])
            if std20 > 0:
                upper = ma20 + 2 * std20
                lower = ma20 - 2 * std20

                if close[-1] < lower:
                    adj -= 5.0   # 볼린저 하단 이탈 → 위험
                elif close[-1] > upper:
                    adj -= 2.0   # 볼린저 상단 돌파 → 과열

            return adj

        except Exception:
            return 0.0

    def _decide_action(self, state: TargetState, flow_df=None) -> tuple:
        """동적 목표가 + 트레일링 스탑 판정

        트레일링 활성화 전: 기존 TP 기반 (단, TP 근접 시 매도 대신 트레일링 전환)
        트레일링 활성화 후: 고점 대비 하락폭 + 수급 이탈 여부로 판정
        """
        cp = state.current_price
        tp = state.dynamic_tp
        sl = state.dynamic_sl
        entry = state.entry_price

        # ─── 고점 갱신 ───
        state.high_watermark = max(state.high_watermark or entry, cp)
        hwm = state.high_watermark

        pnl_pct = (cp / entry - 1) * 100 if entry > 0 else 0
        drop_pct = (1 - cp / hwm) * 100 if hwm > 0 else 0

        # ═══ 1. 하드 손절 (최우선 — 트레일링 여부 무관) ═══
        if cp <= sl:
            return ACTION_STOP_LOSS, f"SL 도달 ({cp:,} ≤ {sl:,})"

        # ═══ 2. 트레일링 모드 활성화 조건 ═══
        #   - 수익 +3% 이상 경험
        #   - TP 도달/돌파
        #   - 이미 활성화됨
        should_trail = (
            state.trailing_activated
            or pnl_pct >= 3.0
            or (cp >= tp and tp > 0)
        )

        if should_trail:
            state.trailing_activated = True

            # ─── 트레일링 SL 계산 ───
            # 고점 × 0.97 (고점 대비 -3%)
            trail_sl = int(hwm * 0.97)
            # 수익 +3% 경험 → 본전 확보 (SL ≥ 진입가)
            if pnl_pct >= 3.0 or (hwm > entry * 1.03):
                trail_sl = max(trail_sl, entry)
            # ratchet: 절대 내려가지 않음
            state.trailing_sl = max(state.trailing_sl, trail_sl)

            # ─── 트레일링 SL 도달 체크 ───
            if cp <= state.trailing_sl:
                return ACTION_FULL_SELL, (
                    f"트레일링SL (현:{cp:,} ≤ SL:{state.trailing_sl:,}, "
                    f"고점:{hwm:,}, 하락:{drop_pct:.1f}%)"
                )

            # ─── 고점 대비 하락폭 판정 ───
            if drop_pct < 2.0:
                # 고점 근처 — 올라가는 중, 놔두기
                if pnl_pct >= 8.0:
                    return ACTION_ADD, f"고점추적+업사이드{pnl_pct:.1f}% → 추매검토"
                return ACTION_HOLD, (
                    f"고점추적 (고점:{hwm:,} 하락:{drop_pct:.1f}% 수익:{pnl_pct:+.1f}%)"
                )

            elif drop_pct < 3.5:
                # -2~3.5%: 조정인지 수급이탈인지 확인
                exiting, supply_reason = self._is_supply_exiting(flow_df)
                if exiting:
                    return ACTION_FULL_SELL, (
                        f"고점-{drop_pct:.1f}% + {supply_reason} → 전량매도"
                    )
                return ACTION_HOLD, (
                    f"건강한조정 (고점-{drop_pct:.1f}%, {supply_reason})"
                )

            elif drop_pct < 5.0:
                # -3.5~5%: 수급이탈이면 전량, 아니면 부분
                exiting, supply_reason = self._is_supply_exiting(flow_df)
                if exiting:
                    return ACTION_FULL_SELL, (
                        f"고점-{drop_pct:.1f}% + {supply_reason} → 전량매도"
                    )
                return ACTION_PARTIAL_SELL, (
                    f"고점-{drop_pct:.1f}% → 부분매도 ({supply_reason})"
                )

            else:
                # -5% 이상: 무조건 전량매도 (하드스탑)
                return ACTION_FULL_SELL, (
                    f"고점-{drop_pct:.1f}% 하드스탑 → 전량매도"
                )

        # ═══ 3. 트레일링 미활성 → 기존 TP 기반 로직 ═══
        ratio = tp / cp if cp > 0 else 1.0

        if ratio >= 1.08:
            return ACTION_ADD, f"업사이드 {(ratio-1)*100:.1f}% → 추매검토"
        elif ratio >= 1.03:
            return ACTION_HOLD, f"업사이드 {(ratio-1)*100:.1f}% → 홀딩"
        elif ratio >= 1.01:
            # ★ 기존: 부분매도 → 변경: 트레일링 모드 전환
            state.trailing_activated = True
            state.high_watermark = max(state.high_watermark, cp)
            return ACTION_HOLD, (
                f"TP근접 → 트레일링전환 (업사이드:{(ratio-1)*100:.1f}%)"
            )
        else:
            # ★ 기존: 전량매도 → 변경: TP 돌파 = 트레일링 모드 전환
            state.trailing_activated = True
            state.high_watermark = max(state.high_watermark, cp)
            return ACTION_HOLD, (
                f"TP돌파! 트레일링전환 (고점:{hwm:,}, 수익:{pnl_pct:+.1f}%)"
            )

    # ═══════════════════════════════════════════════════
    #  수급 이탈 판정
    # ═══════════════════════════════════════════════════

    def _is_supply_exiting(self, flow_df) -> tuple:
        """수급 이탈 여부 판정 (조정 vs 본격 이탈 구분)

        Returns: (이탈여부: bool, 사유: str)
        """
        if flow_df is None or len(flow_df) < 3:
            return False, "수급데이터없음→보수적홀딩"

        try:
            inst_3d = flow_df["기관_수량"].values[-3:].astype(float)
            frgn_3d = flow_df["외국인_수량"].values[-3:].astype(float)
            net_3d = inst_3d + frgn_3d

            # 3일 연속 순매도 → 수급 이탈 확정
            if all(n < 0 for n in net_3d):
                return True, "3일연속순매도"

            # 최근 3일 합산 대폭 순매도 (평균의 2배 이상)
            total = net_3d.sum()
            avg_abs = float(np.abs(net_3d).mean())
            if total < 0 and avg_abs > 0 and abs(total) > avg_abs * 2:
                return True, "수급대량이탈"

            # 최근일 급격 순매도 (직전일 대비 3배 이상 매도)
            if net_3d[-1] < 0 and len(net_3d) >= 2:
                prev_abs = max(abs(net_3d[-2]), 1)
                if abs(net_3d[-1]) > prev_abs * 3:
                    return True, "급격순매도"

            # 수급 양호 판정
            if all(n > 0 for n in net_3d):
                return False, "수급양호(3일순매수)"
            if total > 0:
                return False, "수급보통(합산순매수)"
            return False, "수급보통"

        except Exception as e:
            logger.debug(f"수급 판정 실패: {e}")
            return False, "수급판정실패→보수적홀딩"


# ═══════════════════════════════════════════════════
#  백테스트용 유틸리티
# ═══════════════════════════════════════════════════

def calc_rsi(close: np.ndarray, period: int = 14) -> float:
    """RSI 계산"""
    if len(close) < period + 1:
        return 50.0
    deltas = np.diff(close[-(period + 1):])
    gains = np.maximum(deltas, 0).mean()
    losses = np.abs(np.minimum(deltas, 0)).mean()
    if losses == 0:
        return 100.0
    return 100 - (100 / (1 + gains / losses))


def calc_bollinger_pct(close: np.ndarray, period: int = 20) -> float:
    """볼린저 밴드 %B (0~100)"""
    if len(close) < period:
        return 50.0
    ma = np.mean(close[-period:])
    std = np.std(close[-period:])
    if std == 0:
        return 50.0
    upper = ma + 2 * std
    lower = ma - 2 * std
    return (close[-1] - lower) / (upper - lower) * 100


def calc_atr(high: np.ndarray, low: np.ndarray, close: np.ndarray,
             period: int = 14) -> float:
    """ATR 계산"""
    if len(high) < period + 1:
        return 0.0
    h = high[-(period + 1):]
    l = low[-(period + 1):]
    c = close[-(period + 1):]
    tr = np.maximum(h[1:] - l[1:],
                    np.maximum(np.abs(h[1:] - c[:-1]),
                               np.abs(l[1:] - c[:-1])))
    return float(np.mean(tr))
