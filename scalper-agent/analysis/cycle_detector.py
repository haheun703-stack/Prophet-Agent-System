"""
수급 사이클 감지기 (Supply-Demand Cycle Detector)
=================================================
4세력(외인/기관/개인/기타법인) 기반 사이클 위상 판정

7개 섹터 검증 완료된 법칙:
  [법칙1] 쌍매수 + 개인 바침 = 급등
  [법칙2] 기타법인 지속 매수 = 숨은 매집
  [법칙3] 개인 대량 바침(500억+) = 급등 전조
  [법칙4] 기타법인 + 쌍매수 동시 = 가장 강한 신호
  [법칙5] 개인만 매수 + 3세력 매도 = 하락 지속
  [법칙6] 기타법인 대량 이탈 = 고점 경고

사이클 위상:
  SURGE(급등임박) > ACCUMULATION(매집) > REVERSAL(전환) >
  NEUTRAL(중립) > DISTRIBUTION(물량분배) > PEAK_WARN(고점경고)
"""

import json
import logging
from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

DATA_DIR = Path(__file__).parent.parent / "data_store"
FLOW_DIR = DATA_DIR / "flow"
UNI_PATH = DATA_DIR / "universe.json"

# ── 사이클 위상 정의 ────────────────────────────────────

PHASE_SURGE       = "SURGE"         # 급등 임박
PHASE_ACCUMULATE  = "ACCUMULATION"  # 매집 진행
PHASE_REVERSAL    = "REVERSAL"      # 바닥 전환
PHASE_NEUTRAL     = "NEUTRAL"       # 중립
PHASE_DISTRIBUTE  = "DISTRIBUTION"  # 물량 분배(세력→개인)
PHASE_PEAK_WARN   = "PEAK_WARN"     # 고점 경고

PHASE_KR = {
    PHASE_SURGE:      "급등임박",
    PHASE_ACCUMULATE: "매집",
    PHASE_REVERSAL:   "전환",
    PHASE_NEUTRAL:    "중립",
    PHASE_DISTRIBUTE: "물량분배",
    PHASE_PEAK_WARN:  "고점경고",
}


# ── 데이터 구조 ─────────────────────────────────────────

@dataclass
class CycleSignal:
    """개별 감지 신호"""
    name: str           # twin_buy, retail_sacrifice, stealth_acc, ...
    name_kr: str        # 쌍매수, 개인바침, 기타매집, ...
    score: float        # 점수 기여
    detail: str = ""    # 상세 설명
    days: int = 0       # 신호 지속 일수


@dataclass
class CycleResult:
    """종목별 사이클 분석 결과"""
    code: str
    name: str
    phase: str                          # PHASE_* 상수
    phase_kr: str
    score: float                        # 종합 점수 (-100 ~ +100)
    signals: list = field(default_factory=list)  # CycleSignal 리스트
    latest_close: int = 0
    change_pct: float = 0.0
    cap_억: int = 0
    market: str = ""
    summary: str = ""                   # 한줄 요약
    surge_type: str = ""                # "지속"(연속매집→추가상승) / "원샷"(추격위험) / ""


# ── 핵심: 사이클 감지기 ─────────────────────────────────

class CycleDetector:
    """수급 사이클 감지기 — 4세력 기반"""

    # 신호별 점수 가중치
    W_TWIN_BUY           = 25   # 쌍매수
    W_TWIN_SELL          = -20  # 쌍매도
    W_RETAIL_SACRIFICE   = 20   # 개인 바침
    W_STEALTH_ACC        = 15   # 기타법인 매집
    W_STEALTH_EXIT       = -20  # 기타법인 이탈
    W_FORCE_REVERSAL     = 20   # 세력 매수전환
    W_RETAIL_TRAP        = -15  # 개인 함정(개인만 매수)
    W_TRIPLE_BUY         = 10   # 3세력 동시매수 보너스
    W_INST_LEAD          = 8    # 기관 주도 대량매수
    W_FOREIGN_LEAD       = 8    # 외인 주도 대량매수

    # 임계값 (억 단위, CSV는 백만원이므로 *100 변환)
    THRESHOLD_LARGE_CAP  = 10000  # 시총 1조+ = 대형주
    THRESHOLD_BIG_AMOUNT = 50     # 50억+ = 유의미한 수급

    def __init__(self):
        self._universe = None

    @property
    def universe(self) -> dict:
        if self._universe is None:
            try:
                with open(UNI_PATH, "r", encoding="utf-8") as f:
                    self._universe = json.load(f)
            except Exception:
                self._universe = {}
        return self._universe

    # ── 종목 단위 분석 ──────────────────────────────────

    def analyze(self, code: str, lookback: int = 10) -> Optional[CycleResult]:
        """
        종목 1개 수급 사이클 분석
        - code: 종목코드 (6자리)
        - lookback: 분석 기간 (거래일)
        """
        csv_path = FLOW_DIR / f"{code}_investor.csv"
        if not csv_path.exists():
            return None

        try:
            df = pd.read_csv(csv_path, index_col=0, parse_dates=True)
        except Exception as e:
            logger.warning(f"[CycleDetector] CSV 읽기 실패 {code}: {e}")
            return None

        if len(df) < lookback + 5:
            return None

        # 최근 lookback일 + 직전 5일(비교용)
        df = df.tail(lookback + 5).copy()

        # 금액 → 억 변환 (CSV 단위: 백만원 → /100 = 억)
        for col in ["기관_금액", "기타법인_금액", "개인_금액", "외국인_금액"]:
            if col in df.columns:
                df[col] = df[col].fillna(0) / 100

        # 최근 lookback일만 슬라이싱
        recent = df.tail(lookback)

        if recent.empty or "외국인_금액" not in recent.columns:
            return None

        # 종목 정보
        uni_info = self.universe.get(code, {})
        name = uni_info.get("name", code)
        cap = uni_info.get("cap_억", 0)
        market = uni_info.get("market", "")
        is_large = cap >= self.THRESHOLD_LARGE_CAP

        # 유의미 금액 임계값: 시총 비례 (대형주 50억, 소형주 10억)
        threshold = max(10, min(50, cap / 200)) if cap > 0 else 20

        latest_close = int(recent["종가"].iloc[-1]) if "종가" in recent.columns and pd.notna(recent["종가"].iloc[-1]) else 0
        change_pct = 0.0
        if "전일대비" in recent.columns and pd.notna(recent["전일대비"].iloc[-1]) and latest_close > 0:
            diff = float(recent["전일대비"].iloc[-1])
            prev_close = latest_close - diff
            change_pct = round(diff / prev_close * 100, 2) if prev_close != 0 else 0.0

        # ── 신호 감지 ──
        signals = []

        signals.extend(self._detect_twin_buy(recent, threshold))
        signals.extend(self._detect_twin_sell(recent, threshold))
        signals.extend(self._detect_retail_sacrifice(recent, threshold))
        signals.extend(self._detect_stealth_accumulation(recent, threshold))
        signals.extend(self._detect_stealth_exit(recent, threshold))
        signals.extend(self._detect_force_reversal(recent, df, threshold, is_large))
        signals.extend(self._detect_retail_trap(recent, threshold))
        signals.extend(self._detect_triple_buy(recent, threshold))

        # ── 종합 점수 ──
        total_score = sum(s.score for s in signals)
        total_score = max(-100, min(100, total_score))

        # ── 위상 판정 ──
        phase, surge_type = self._determine_phase(total_score, signals, change_pct)

        # ── 한줄 요약 ──
        summary = self._build_summary(signals, phase, name, surge_type)

        return CycleResult(
            code=code,
            name=name,
            phase=phase,
            phase_kr=PHASE_KR.get(phase, phase),
            score=round(total_score, 1),
            signals=[asdict(s) for s in signals],
            latest_close=latest_close,
            change_pct=change_pct,
            cap_억=cap,
            market=market,
            summary=summary,
            surge_type=surge_type,
        )

    # ── 8개 신호 감지 함수 ──────────────────────────────

    def _detect_twin_buy(self, df: pd.DataFrame, thr: float) -> list:
        """쌍매수: 외인+기관 동시 순매수 (최근 3일 내)"""
        signals = []
        recent_3 = df.tail(3)
        count = 0
        for _, row in recent_3.iterrows():
            fo = row.get("외국인_금액", 0)
            ins = row.get("기관_금액", 0)
            if fo > thr and ins > thr:
                count += 1

        if count >= 2:
            signals.append(CycleSignal(
                name="twin_buy", name_kr="쌍매수",
                score=self.W_TWIN_BUY * 1.5,  # 2일+ 연속 보너스
                detail=f"최근3일 중 {count}일 쌍매수",
                days=count,
            ))
        elif count == 1:
            signals.append(CycleSignal(
                name="twin_buy", name_kr="쌍매수",
                score=self.W_TWIN_BUY,
                detail="최근3일 중 1일 쌍매수",
                days=1,
            ))
        return signals

    def _detect_twin_sell(self, df: pd.DataFrame, thr: float) -> list:
        """쌍매도: 외인+기관 동시 순매도 (최근 3일 내)"""
        signals = []
        recent_3 = df.tail(3)
        count = 0
        for _, row in recent_3.iterrows():
            fo = row.get("외국인_금액", 0)
            ins = row.get("기관_금액", 0)
            if fo < -thr and ins < -thr:
                count += 1

        if count >= 2:
            signals.append(CycleSignal(
                name="twin_sell", name_kr="쌍매도",
                score=self.W_TWIN_SELL * 1.5,
                detail=f"최근3일 중 {count}일 쌍매도",
                days=count,
            ))
        elif count == 1:
            signals.append(CycleSignal(
                name="twin_sell", name_kr="쌍매도",
                score=self.W_TWIN_SELL,
                detail="최근3일 중 1일 쌍매도",
                days=1,
            ))
        return signals

    def _detect_retail_sacrifice(self, df: pd.DataFrame, thr: float) -> list:
        """개인 바침: 개인 대량 순매도 + 세력 순매수"""
        signals = []
        recent_3 = df.tail(3)
        for _, row in recent_3.iterrows():
            ind = row.get("개인_금액", 0)
            fo = row.get("외국인_금액", 0)
            ins = row.get("기관_금액", 0)
            etc = row.get("기타법인_금액", 0)

            # 개인 대량 매도 + 다른 세력 중 최소 1곳 대량 매수
            if ind < -(thr * 3) and (fo > thr or ins > thr or etc > thr * 0.5):
                score = self.W_RETAIL_SACRIFICE
                # 개인 매도 규모에 비례 보너스
                if abs(ind) > thr * 10:
                    score *= 1.5
                signals.append(CycleSignal(
                    name="retail_sacrifice", name_kr="개인바침",
                    score=score,
                    detail=f"개인{ind:+.0f}억 바침 → 세력유입",
                ))
                break  # 1건만
        return signals

    def _detect_stealth_accumulation(self, df: pd.DataFrame, thr: float) -> list:
        """기타법인 매집: N일 연속 순매수"""
        signals = []
        recent = df.tail(7)  # 7일 윈도우
        etc_vals = recent["기타법인_금액"].values if "기타법인_금액" in recent.columns else []

        if len(etc_vals) == 0:
            return signals

        # 연속 매수일 카운트 (최근부터 역순)
        consec = 0
        total_amt = 0
        for v in reversed(etc_vals):
            if pd.notna(v) and v > 0:
                consec += 1
                total_amt += v
            else:
                break

        # 3일+ 연속 또는 합산 유의미
        if consec >= 3:
            score = self.W_STEALTH_ACC * (1 + (consec - 3) * 0.3)
            signals.append(CycleSignal(
                name="stealth_acc", name_kr="기타매집",
                score=score,
                detail=f"기타법인 {consec}일 연속 매수 (합산{total_amt:+.0f}억)",
                days=consec,
            ))
        elif consec >= 2 and total_amt > thr * 2:
            signals.append(CycleSignal(
                name="stealth_acc", name_kr="기타매집",
                score=self.W_STEALTH_ACC * 0.7,
                detail=f"기타법인 {consec}일 연속 (합산{total_amt:+.0f}억)",
                days=consec,
            ))
        return signals

    def _detect_stealth_exit(self, df: pd.DataFrame, thr: float) -> list:
        """기타법인 이탈: 매수→매도 전환"""
        signals = []
        if "기타법인_금액" not in df.columns or len(df) < 5:
            return signals

        recent_5 = df.tail(5)["기타법인_금액"].values
        # 앞 2~3일 매수 → 최근 2일 매도 = 이탈
        early_avg = np.nanmean(recent_5[:3]) if len(recent_5) >= 5 else 0
        late_avg = np.nanmean(recent_5[-2:]) if len(recent_5) >= 2 else 0

        if early_avg > thr * 0.5 and late_avg < -thr * 0.5:
            signals.append(CycleSignal(
                name="stealth_exit", name_kr="기타이탈",
                score=self.W_STEALTH_EXIT,
                detail=f"기타법인 매수({early_avg:+.0f})→매도({late_avg:+.0f}) 전환",
            ))
        return signals

    def _detect_force_reversal(self, _recent: pd.DataFrame, full: pd.DataFrame,
                                thr: float, is_large: bool) -> list:
        """세력 매수전환: 3일+ 매도 후 매수 전환 (_recent: 미사용, 호환 유지)"""
        signals = []
        if len(full) < 8:
            return signals

        # 직전 5일 vs 최근 2일 비교
        prev_5 = full.iloc[-(7):-2]
        last_2 = full.tail(2)

        # 대형주: 기관 기준, 소형주: 외인 기준
        key_col = "기관_금액" if is_large else "외국인_금액"
        key_name = "기관" if is_large else "외인"

        if key_col not in full.columns:
            return signals

        prev_avg = prev_5[key_col].mean() if len(prev_5) > 0 else 0
        last_avg = last_2[key_col].mean() if len(last_2) > 0 else 0

        # 직전 평균 매도 → 최근 평균 매수 = 전환
        if prev_avg < -thr and last_avg > thr:
            signals.append(CycleSignal(
                name="force_reversal", name_kr=f"{key_name}전환",
                score=self.W_FORCE_REVERSAL,
                detail=f"{key_name} 매도({prev_avg:+.0f})→매수({last_avg:+.0f}) 전환",
            ))
        return signals

    def _detect_retail_trap(self, df: pd.DataFrame, thr: float) -> list:
        """개인 함정: 개인만 매수, 3세력 전부 매도 (최근 2일)"""
        signals = []
        recent_2 = df.tail(2)
        count = 0

        for _, row in recent_2.iterrows():
            ind = row.get("개인_금액", 0)
            fo = row.get("외국인_금액", 0)
            ins = row.get("기관_금액", 0)
            etc = row.get("기타법인_금액", 0)

            if ind > thr and fo < 0 and ins < 0 and etc < 0:
                count += 1

        if count >= 2:
            signals.append(CycleSignal(
                name="retail_trap", name_kr="개인함정",
                score=self.W_RETAIL_TRAP * 1.5,
                detail="2일 연속 개인만 매수 (3세력 전부 매도)",
                days=2,
            ))
        elif count == 1:
            signals.append(CycleSignal(
                name="retail_trap", name_kr="개인함정",
                score=self.W_RETAIL_TRAP,
                detail="개인만 매수 (3세력 전부 매도)",
                days=1,
            ))
        return signals

    def _detect_triple_buy(self, df: pd.DataFrame, thr: float) -> list:
        """3세력+ 동시매수: 외인+기관+기타 동시 매수 (가장 강한 신호)"""
        signals = []
        last = df.iloc[-1]
        fo = last.get("외국인_금액", 0)
        ins = last.get("기관_금액", 0)
        etc = last.get("기타법인_금액", 0)

        buy_count = sum([fo > thr * 0.5, ins > thr * 0.5, etc > 0])

        if buy_count >= 3:
            signals.append(CycleSignal(
                name="triple_buy", name_kr="3세력매수",
                score=self.W_TRIPLE_BUY,
                detail=f"외인{fo:+.0f} 기관{ins:+.0f} 기타{etc:+.0f} 동시매수",
            ))
        return signals

    # ── 위상 판정 ───────────────────────────────────────

    def _determine_phase(self, score: float, signals: list,
                         change_pct: float = 0.0) -> tuple:
        """점수 + 신호 조합으로 위상 판정.

        Returns:
            (phase, surge_type) — surge_type은 SURGE일 때만 의미.
              "지속": 3일+ 연속매집 후 급등 → 추가 상승 가능
              "원샷": 하루 몰빵 급등 → 추격 위험
              "": SURGE가 아닌 경우
        """
        sig_names = {s.name for s in signals}
        sig_map = {s.name: s for s in signals}

        # ── 매집 지속일수 산출 (쌍매수days + 기타매집days 중 최대) ──
        acc_days = 0
        for s in signals:
            if s.name in ("twin_buy", "stealth_acc", "force_reversal"):
                acc_days = max(acc_days, s.days)

        # 급등임박: 쌍매수 + (개인바침 or 기타매집)
        if "twin_buy" in sig_names and ("retail_sacrifice" in sig_names or "stealth_acc" in sig_names):
            st = self._classify_surge_type(acc_days, change_pct)
            return PHASE_SURGE, st
        if score >= 50:
            st = self._classify_surge_type(acc_days, change_pct)
            return PHASE_SURGE, st

        # 매집: 기타법인 연속매수 or 세력전환
        if "stealth_acc" in sig_names:
            return PHASE_ACCUMULATE, ""
        if "force_reversal" in sig_names:
            return PHASE_REVERSAL, ""

        # 쌍매수 단독
        if "twin_buy" in sig_names and score > 15:
            return PHASE_ACCUMULATE, ""

        # 고점경고: 기타이탈 or 개인함정 + 쌍매도
        if "stealth_exit" in sig_names or ("retail_trap" in sig_names and "twin_sell" in sig_names):
            return PHASE_PEAK_WARN, ""

        # 물량분배: 개인함정 단독
        if "retail_trap" in sig_names:
            return PHASE_DISTRIBUTE, ""

        # 쌍매도 단독 — 바닥 근접 가능성 (전환 대기)
        if "twin_sell" in sig_names:
            return PHASE_DISTRIBUTE, ""

        return PHASE_NEUTRAL, ""

    @staticmethod
    def _classify_surge_type(acc_days: int, change_pct: float) -> str:
        """급등 세분화: 지속 vs 원샷.

        - 3일+ 연속매집 → "지속" (추가 상승 가능)
        - 이미 15%+ 급등 + 매집 2일 이하 → "원샷" (추격 위험)
        - 그 외 → "지속" (아직 안 터졌으면 기본 지속)
        """
        if acc_days >= 3:
            return "지속"
        if change_pct >= 15.0 and acc_days <= 2:
            return "원샷"
        if change_pct >= 10.0 and acc_days <= 1:
            return "원샷"
        return "지속"

    # ── 한줄 요약 ───────────────────────────────────────

    def _build_summary(self, signals: list, phase: str, name: str,
                        surge_type: str = "") -> str:
        sig_names = [s.name_kr for s in signals]
        if not sig_names:
            return f"{name}: 특별 신호 없음"

        phase_kr = PHASE_KR.get(phase, phase)
        if surge_type:
            phase_kr = f"{phase_kr}({surge_type})"
        return f"{name}: [{phase_kr}] {' + '.join(sig_names)}"

    # ── 유니버스 전체 스캔 ──────────────────────────────

    def scan_universe(self, min_cap: int = 3000, top_n: int = 30,
                       lookback: int = 10, exclude_etf: bool = True) -> list:
        """
        전체 유니버스 스캔
        - min_cap: 최소 시총(억) 필터
        - top_n: 양쪽 각각 N개 (급등임박 top_n + 경고 top_n)
        - lookback: 분석 기간
        - exclude_etf: ETF 제외 (기본 True)
        Returns: CycleResult 리스트 (급등→매집→전환→물량분배→경고 순)
        """
        results = []
        uni = self.universe

        # 시총 필터 + ETF 필터
        codes = []
        for c, info in uni.items():
            if info.get("cap_억", 0) < min_cap:
                continue
            if exclude_etf and info.get("sector", "") in ("ETF", "기타"):
                continue
            codes.append(c)

        logger.info(f"[CycleDetector] 스캔 시작: {len(codes)}종목 (시총{min_cap}억+)")

        for code in codes:
            result = self.analyze(code, lookback=lookback)
            if result and result.phase != PHASE_NEUTRAL:
                results.append(result)

        # 양수(급등/매집)와 음수(경고) 분리
        positive = sorted([r for r in results if r.score > 0],
                          key=lambda r: r.score, reverse=True)
        negative = sorted([r for r in results if r.score <= 0],
                          key=lambda r: r.score)  # 가장 나쁜 것 먼저

        # 양쪽 각각 top_n 적용
        combined = positive[:top_n] + negative[:top_n]

        phase_cnt = {}
        for r in results:
            phase_cnt[r.phase] = phase_cnt.get(r.phase, 0) + 1

        logger.info(f"[CycleDetector] 스캔 완료: {len(results)}건 감지 "
                     f"(급등임박={phase_cnt.get(PHASE_SURGE,0)}, "
                     f"매집={phase_cnt.get(PHASE_ACCUMULATE,0)}, "
                     f"전환={phase_cnt.get(PHASE_REVERSAL,0)}, "
                     f"물량분배={phase_cnt.get(PHASE_DISTRIBUTE,0)}, "
                     f"고점경고={phase_cnt.get(PHASE_PEAK_WARN,0)})")

        return combined

    def scan_surge_candidates(self, min_cap: int = 3000) -> list:
        """급등임박 종목만 필터"""
        all_results = self.scan_universe(min_cap=min_cap, top_n=999)
        return [r for r in all_results if r.phase == PHASE_SURGE]

    def scan_accumulation(self, min_cap: int = 3000) -> list:
        """매집 진행 종목만 필터"""
        all_results = self.scan_universe(min_cap=min_cap, top_n=999)
        return [r for r in all_results if r.phase == PHASE_ACCUMULATE]

    def scan_peak_warnings(self, min_cap: int = 3000) -> list:
        """고점 경고 종목만 필터"""
        all_results = self.scan_universe(min_cap=min_cap, top_n=999)
        return [r for r in all_results if r.phase in (PHASE_PEAK_WARN, PHASE_DISTRIBUTE)]


# ── 텔레그램 포맷 ──────────────────────────────────────

PHASE_EMOJI = {
    PHASE_SURGE:      "🔥",
    PHASE_ACCUMULATE: "🔍",
    PHASE_REVERSAL:   "🔄",
    PHASE_NEUTRAL:    "⚪",
    PHASE_DISTRIBUTE: "⚠️",
    PHASE_PEAK_WARN:  "🚨",
}


def format_cycle_telegram(results: list, title: str = "수급 사이클 스캔") -> str:
    """텔레그램 메시지 포맷"""
    if not results:
        return f"📊 {title}\n\n감지된 신호 없음"

    lines = [f"📊 {title}", f"📅 {datetime.now().strftime('%m/%d %H:%M')}", ""]

    # 위상별 그룹핑
    phase_order = [PHASE_SURGE, PHASE_ACCUMULATE, PHASE_REVERSAL,
                   PHASE_DISTRIBUTE, PHASE_PEAK_WARN]

    for phase in phase_order:
        group = [r for r in results if r.phase == phase]
        if not group:
            continue

        emoji = PHASE_EMOJI.get(phase, "")
        phase_kr = PHASE_KR.get(phase, phase)
        lines.append(f"{emoji} [{phase_kr}] ({len(group)}종목)")

        for r in group[:10]:  # 위상별 최대 10개
            # 신호 태그
            sig_tags = " ".join(f"#{s['name_kr']}" for s in r.signals[:3])
            price_str = f"{r.latest_close:,}" if r.latest_close else "?"

            # 급등임박 surge_type 태그
            surge_tag = ""
            if phase == PHASE_SURGE and r.surge_type:
                surge_tag = f"[{r.surge_type}] " if r.surge_type == "지속" else f"[{r.surge_type}⚡] "

            lines.append(
                f"  {surge_tag}{r.name}({r.code}) "
                f"점수{r.score:+.0f} "
                f"{price_str} "
                f"{sig_tags}"
            )

            # 핵심 신호 상세 (급등임박만)
            if phase == PHASE_SURGE:
                for s in r.signals[:2]:
                    lines.append(f"    → {s['detail']}")

        lines.append("")

    # 요약 통계
    surge_cnt = sum(1 for r in results if r.phase == PHASE_SURGE)
    acc_cnt = sum(1 for r in results if r.phase == PHASE_ACCUMULATE)
    warn_cnt = sum(1 for r in results if r.phase in (PHASE_PEAK_WARN, PHASE_DISTRIBUTE))

    lines.append(f"총 {len(results)}종목 | "
                 f"🔥급등임박 {surge_cnt} | "
                 f"🔍매집 {acc_cnt} | "
                 f"⚠️주의 {warn_cnt}")

    return "\n".join(lines)


def format_single_stock(result: CycleResult) -> str:
    """단일 종목 상세 포맷"""
    if not result:
        return "분석 결과 없음"

    emoji = PHASE_EMOJI.get(result.phase, "")
    surge_info = f" [{result.surge_type}]" if result.surge_type else ""
    lines = [
        f"{emoji} {result.name}({result.code}) 수급 사이클{surge_info}",
        f"위상: {result.phase_kr} | 점수: {result.score:+.0f}",
        f"종가: {result.latest_close:,} ({result.change_pct:+.1f}%)" if result.latest_close else "",
        "",
        "감지 신호:",
    ]

    for s in result.signals:
        lines.append(f"  [{s['name_kr']}] {s['detail']} (점수{s['score']:+.0f})")

    if not result.signals:
        lines.append("  특별 신호 없음")

    return "\n".join(lines)


# ── 저장/로드 ──────────────────────────────────────────

RESULT_PATH = DATA_DIR / "cycle_scan.json"


def save_scan_results(results: list):
    """스캔 결과 JSON 저장"""
    data = {
        "updated": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "count": len(results),
        "results": [asdict(r) for r in results],
    }
    with open(RESULT_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    logger.info(f"[CycleDetector] 저장: {RESULT_PATH} ({len(results)}건)")


def load_scan_results() -> list:
    """최근 스캔 결과 로드"""
    if not RESULT_PATH.exists():
        return []
    try:
        with open(RESULT_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data.get("results", [])
    except Exception:
        return []


# ── CLI 실행 ───────────────────────────────────────────

def run_cycle_scan(min_cap: int = 3000, top_n: int = 30) -> list:
    """전체 스캔 실행 (COO/CLI 용)"""
    detector = CycleDetector()
    results = detector.scan_universe(min_cap=min_cap, top_n=top_n)
    save_scan_results(results)
    return results


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    if len(sys.argv) > 1:
        # 단일 종목: python cycle_detector.py 005380
        code = sys.argv[1]
        det = CycleDetector()
        r = det.analyze(code)
        if r:
            print(format_single_stock(r))
        else:
            print(f"분석 불가: {code}")
    else:
        # 전체 스캔
        results = run_cycle_scan(min_cap=3000, top_n=30)
        print(format_cycle_telegram(results))
