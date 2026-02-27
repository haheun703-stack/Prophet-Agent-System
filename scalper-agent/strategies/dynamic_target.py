# -*- coding: utf-8 -*-
"""
동적 목표가 엔진 — 매일 재평가하여 SL/TP 조정
===================================================
3 GAP 통합:
  GAP 1: ATR 기반 초기 SL/TP (고정% → ATR)
  GAP 2: 뉴스 감성 → 목표가 보정
  GAP 3: 매집원가 → SL 하한선

매일 재평가 요소:
  ① 뉴스 감성 (키워드 기반)
  ② 수급 (외인+기관 3일 연속)
  ③ RSI / 볼린저 밴드
  ④ 증권사 목표가 변동

판정 기준:
  재조정 목표가 > 현재가 × 1.08 → 추가매수 검토
  재조정 목표가 > 현재가 × 1.03 → 홀딩
  재조정 목표가 < 현재가 × 1.01 → 부분매도 50%
  재조정 목표가 < 현재가 × 0.97 → 전량매도
"""

import logging
import numpy as np
import pandas as pd
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional

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
            path = DAILY_DIR / f"{code}.csv"
            if not path.exists():
                return self._fallback_state(code, name, entry_price, entry_date)
            df = pd.read_csv(path, index_col=0, parse_dates=True)

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
            path = DAILY_DIR / f"{code}.csv"
            if not path.exists():
                state.current_price = current_price
                state.action = ACTION_HOLD
                return state
            df = pd.read_csv(path, index_col=0, parse_dates=True)

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

        # ─── 판정 ───
        state.action, state.reason = self._decide_action(state)

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

    def _decide_action(self, state: TargetState) -> tuple:
        """동적 목표가 vs 현재가 비교하여 판정"""
        cp = state.current_price
        tp = state.dynamic_tp
        sl = state.dynamic_sl

        # 1. 손절 체크 (최우선)
        if cp <= sl:
            return ACTION_STOP_LOSS, f"SL 도달 ({cp:,} ≤ {sl:,})"

        # 2. 재조정 목표가 기반 판정
        ratio = tp / cp if cp > 0 else 1.0

        if ratio >= 1.08:
            return ACTION_ADD, f"업사이드 {(ratio-1)*100:.1f}% → 추가매수 검토"
        elif ratio >= 1.02:
            return ACTION_HOLD, f"업사이드 {(ratio-1)*100:.1f}% → 홀딩"
        elif ratio >= 0.99:
            return ACTION_PARTIAL_SELL, f"업사이드 {(ratio-1)*100:.1f}% 소진 → 부분매도"
        else:
            return ACTION_FULL_SELL, f"목표가 소진 (TP:{tp:,} < CP×0.99:{int(cp*0.99):,})"


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
