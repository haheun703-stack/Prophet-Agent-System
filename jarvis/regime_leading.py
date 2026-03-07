# -*- coding: utf-8 -*-
"""
레짐 전환 선행지표 크로스체크 (Regime Transition Leading Indicators)
=====================================================================
나이트워치가 "현재 레짐"을 보는 동행지표라면,
이 모듈은 "레짐 전환이 임박했는지"를 보는 선행지표.

채권시장이 주식시장보다 항상 먼저 움직인다:
  1. 신용 스프레드 (HY-IG) 확대 → 주식 하락 2~5일 선행
  2. MOVE 지수 급등 → 채권 변동성 = 금리 불확실성 = 주식 약세 선행
  3. 2Y-10Y 스프레드 역전 심화 → 경기 침체 신호
  4. TED 스프레드 (LIBOR-T-Bill) → 은행간 신뢰 붕괴 (2008급)

yfinance 티커:
  - HYG: 하이일드 채권 ETF (이미 nightwatch에서 수집 중)
  - LQD: 투자등급 채권 ETF (HYG-LQD 스프레드 = 신용 스프레드 proxy)
  - ^MOVE: MOVE 지수 (채권 VIX) — yfinance 미지원 시 폴백
  - ^IRX: 미국 3개월 T-Bill 금리
  - ^TNX: 미국 10년 국채 금리 (이미 수집 중)
  - ^TYX: 미국 30년 국채 금리

3일 연속 악화 감지 → "레짐 전환 임박" 경고
"""

import json
import logging
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from typing import Dict, Optional

logger = logging.getLogger(__name__)

DATA_DIR = Path(__file__).resolve().parent / "data"
HISTORY_PATH = DATA_DIR / "regime_leading_history.json"


@dataclass
class LeadingIndicator:
    """단일 선행지표"""
    name: str
    value: float = 0.0
    change_3d: float = 0.0       # 3일 변화량
    direction: str = ""           # WORSENING / STABLE / IMPROVING
    is_warning: bool = False      # 3일 연속 악화?
    signal: str = ""


@dataclass
class RegimeLeadingReport:
    """레짐 전환 선행 분석 보고"""
    date: str = ""
    # 개별 지표
    credit_spread: LeadingIndicator = field(
        default_factory=lambda: LeadingIndicator(name="신용 스프레드(HY-IG)"))
    yield_curve: LeadingIndicator = field(
        default_factory=lambda: LeadingIndicator(name="수익률 곡선(2Y-10Y)"))
    move_index: LeadingIndicator = field(
        default_factory=lambda: LeadingIndicator(name="MOVE 지수"))
    ted_spread: LeadingIndicator = field(
        default_factory=lambda: LeadingIndicator(name="TED 스프레드"))
    # 종합
    warning_count: int = 0        # 경고 지표 수 (0~4)
    transition_signal: str = ""   # IMMINENT / WATCH / CLEAR
    transition_direction: str = ""  # RISK_OFF / RISK_ON / UNCLEAR
    confidence: float = 0.0       # 전환 확신도 (0~1)
    signal: str = ""              # 텔레그램 1줄 요약
    # BRAIN 연동
    preemptive_adjust: dict = field(default_factory=dict)


# ═══════════════════════════════════════
#  데이터 수집
# ═══════════════════════════════════════

def _fetch_history(symbol: str, days: int = 10) -> Optional[list]:
    """yfinance에서 최근 N일 종가 리스트"""
    try:
        import yfinance as yf
        end = datetime.now()
        start = end - timedelta(days=days + 15)  # 주말/공휴일 여유분
        data = yf.Ticker(symbol).history(
            start=start.strftime("%Y-%m-%d"),
            end=end.strftime("%Y-%m-%d"),
        )
        if data is None or len(data) < 3:
            return None
        return data["Close"].dropna().tolist()[-days:]
    except Exception as e:
        logger.warning(f"[RegimeLeading] {symbol} 수집 실패: {e}")
        return None


def _calc_3d_change(values: list) -> float:
    """마지막 3일 변화량 (값 차이)"""
    if len(values) < 4:
        return 0.0
    return values[-1] - values[-4]


def _is_consecutive_worsening(values: list, worse_fn) -> bool:
    """3일 연속 악화 방향 확인

    Args:
        values: 일별 값 리스트
        worse_fn: 값이 커지면 나빠지는지(True) or 작아지면 나빠지는지(False)
    """
    if len(values) < 4:
        return False
    recent = values[-4:]
    # 3일 연속 같은 방향
    diffs = [recent[i+1] - recent[i] for i in range(3)]
    if worse_fn:
        # 커지면 나빠짐 (스프레드 확대, MOVE 상승)
        return all(d > 0 for d in diffs)
    else:
        # 작아지면 나빠짐 (yield curve 역전 심화)
        return all(d < 0 for d in diffs)


# ═══════════════════════════════════════
#  분석
# ═══════════════════════════════════════

def analyze_leading() -> RegimeLeadingReport:
    """레짐 전환 선행지표 분석"""
    report = RegimeLeadingReport(date=datetime.now().strftime("%Y-%m-%d"))

    # ── 1. 신용 스프레드 (HYG - LQD 가격 차이의 변화) ──
    # HYG(하이일드) 가격 하락 + LQD(투자등급) 안정 = 스프레드 확대
    hyg = _fetch_history("HYG", 10)
    lqd = _fetch_history("LQD", 10)

    if hyg and lqd:
        min_len = min(len(hyg), len(lqd))
        # 스프레드 proxy = LQD 수익률 - HYG 수익률 (HYG가 더 떨어지면 스프레드 확대)
        # 가격 기반: HYG/LQD 비율이 낮아지면 스프레드 확대
        spread = [hyg[i] / lqd[i] for i in range(min_len)]
        report.credit_spread.value = round(spread[-1], 4) if spread else 0
        report.credit_spread.change_3d = round(_calc_3d_change(spread), 4)

        # 스프레드 확대 = HYG/LQD 비율 하락 = 악화
        report.credit_spread.is_warning = _is_consecutive_worsening(spread, False)
        if report.credit_spread.is_warning:
            report.credit_spread.direction = "WORSENING"
            report.credit_spread.signal = f"🔴 3일 연속 확대 ({report.credit_spread.change_3d:+.4f})"
        elif report.credit_spread.change_3d < -0.005:
            report.credit_spread.direction = "WORSENING"
            report.credit_spread.signal = f"🟡 확대 중 ({report.credit_spread.change_3d:+.4f})"
        elif report.credit_spread.change_3d > 0.005:
            report.credit_spread.direction = "IMPROVING"
            report.credit_spread.signal = f"🟢 축소 중 ({report.credit_spread.change_3d:+.4f})"
        else:
            report.credit_spread.direction = "STABLE"
            report.credit_spread.signal = f"⬜ 안정 ({report.credit_spread.value:.4f})"
    else:
        report.credit_spread.signal = "데이터 부족"

    # ── 2. 수익률 곡선 (TNX - IRX = 10Y - 3M) ──
    tnx = _fetch_history("^TNX", 10)   # 10년
    irx = _fetch_history("^IRX", 10)   # 3개월

    if tnx and irx:
        min_len = min(len(tnx), len(irx))
        curve = [tnx[i] - irx[i] for i in range(min_len)]
        report.yield_curve.value = round(curve[-1], 3) if curve else 0
        report.yield_curve.change_3d = round(_calc_3d_change(curve), 3)

        # 역전 심화 = 값이 작아지면 나빠짐
        report.yield_curve.is_warning = _is_consecutive_worsening(curve, False)
        is_inverted = report.yield_curve.value < 0

        if report.yield_curve.is_warning:
            report.yield_curve.direction = "WORSENING"
            inv_str = "역전 심화" if is_inverted else "평탄화 가속"
            report.yield_curve.signal = f"🔴 3일 연속 {inv_str} ({report.yield_curve.change_3d:+.3f})"
        elif is_inverted:
            report.yield_curve.direction = "WORSENING"
            report.yield_curve.signal = f"🟡 역전 상태 ({report.yield_curve.value:+.3f}%p)"
        else:
            report.yield_curve.direction = "STABLE" if abs(report.yield_curve.change_3d) < 0.05 else "IMPROVING"
            report.yield_curve.signal = f"🟢 정상 ({report.yield_curve.value:+.3f}%p)"
    else:
        report.yield_curve.signal = "데이터 부족"

    # ── 3. MOVE 지수 (채권 VIX) ──
    # ^MOVE가 yfinance에서 안 될 수 있음 → TLT 변동성으로 폴백
    move = _fetch_history("^MOVE", 10)

    if move and len(move) >= 4:
        report.move_index.value = round(move[-1], 2)
        report.move_index.change_3d = round(_calc_3d_change(move), 2)

        # MOVE 상승 = 나빠짐
        report.move_index.is_warning = _is_consecutive_worsening(move, True)

        if report.move_index.is_warning:
            report.move_index.direction = "WORSENING"
            report.move_index.signal = f"🔴 3일 연속 상승 ({report.move_index.change_3d:+.1f})"
        elif report.move_index.value > 120:
            report.move_index.direction = "WORSENING"
            report.move_index.signal = f"🟡 고변동성 구간 ({report.move_index.value:.0f})"
        elif report.move_index.value > 100:
            report.move_index.direction = "STABLE"
            report.move_index.signal = f"⬜ 보통 ({report.move_index.value:.0f})"
        else:
            report.move_index.direction = "IMPROVING"
            report.move_index.signal = f"🟢 안정 ({report.move_index.value:.0f})"
    else:
        # 폴백: TLT 20일 변동성
        tlt = _fetch_history("TLT", 25)
        if tlt and len(tlt) >= 20:
            returns = np.diff(tlt) / np.array(tlt[:-1]) * 100
            vol = float(np.std(returns[-20:])) * np.sqrt(252)
            report.move_index.value = round(vol, 2)
            report.move_index.signal = f"⬜ TLT 변동성 proxy ({vol:.1f}%)"
            report.move_index.direction = "STABLE"
        else:
            report.move_index.signal = "데이터 부족 (MOVE + TLT 실패)"

    # ── 4. TED 스프레드 proxy (단기 자금시장 스트레스) ──
    # 실제 TED = 3M LIBOR - 3M T-Bill → yfinance에서 LIBOR 없음
    # Proxy: BIL(단기국채 ETF) vs SHV(초단기채 ETF) 가격 비율
    bil = _fetch_history("BIL", 10)  # 1-3M T-Bill ETF
    shv = _fetch_history("SHV", 10)  # 0-12M Treasury ETF

    if bil and shv:
        min_len = min(len(bil), len(shv))
        ted_proxy = [bil[i] / shv[i] for i in range(min_len)]
        report.ted_spread.value = round(ted_proxy[-1], 4) if ted_proxy else 0
        report.ted_spread.change_3d = round(_calc_3d_change(ted_proxy), 4)

        # BIL/SHV 비율 하락 = 단기 자금 스트레스
        report.ted_spread.is_warning = _is_consecutive_worsening(ted_proxy, False)

        if report.ted_spread.is_warning:
            report.ted_spread.direction = "WORSENING"
            report.ted_spread.signal = f"🔴 단기자금 스트레스 3일 연속 ({report.ted_spread.change_3d:+.4f})"
        elif report.ted_spread.change_3d < -0.001:
            report.ted_spread.direction = "WORSENING"
            report.ted_spread.signal = f"🟡 단기자금 긴장 ({report.ted_spread.change_3d:+.4f})"
        else:
            report.ted_spread.direction = "STABLE"
            report.ted_spread.signal = f"🟢 단기자금 안정"
    else:
        report.ted_spread.signal = "데이터 부족"

    # ── 종합 판정 ──
    indicators = [
        report.credit_spread,
        report.yield_curve,
        report.move_index,
        report.ted_spread,
    ]
    report.warning_count = sum(1 for ind in indicators if ind.is_warning)

    # 악화 방향 카운트 (warning 아니더라도 WORSENING이면 카운트)
    worsening_count = sum(1 for ind in indicators if ind.direction == "WORSENING")

    if report.warning_count >= 3:
        report.transition_signal = "IMMINENT"
        report.transition_direction = "RISK_OFF"
        report.confidence = 0.9
        report.signal = f"🚨 레짐 전환 임박 — {report.warning_count}/4 지표 3일 연속 악화"
    elif report.warning_count >= 2:
        report.transition_signal = "IMMINENT"
        report.transition_direction = "RISK_OFF"
        report.confidence = 0.7
        report.signal = f"⚠️ 레짐 전환 경계 — {report.warning_count}/4 경고"
    elif worsening_count >= 3:
        report.transition_signal = "WATCH"
        report.transition_direction = "RISK_OFF"
        report.confidence = 0.5
        report.signal = f"🟡 악화 추세 — {worsening_count}/4 지표 악화 방향"
    elif worsening_count >= 2:
        report.transition_signal = "WATCH"
        report.transition_direction = "UNCLEAR"
        report.confidence = 0.3
        report.signal = f"🟡 일부 악화 — {worsening_count}/4 지표 주시"
    else:
        # IMPROVING 신호 체크
        improving = sum(1 for ind in indicators if ind.direction == "IMPROVING")
        if improving >= 3:
            report.transition_signal = "WATCH"
            report.transition_direction = "RISK_ON"
            report.confidence = 0.5
            report.signal = f"🟢 호전 추세 — {improving}/4 지표 개선 중"
        else:
            report.transition_signal = "CLEAR"
            report.transition_direction = "UNCLEAR"
            report.confidence = 0.0
            report.signal = "🟢 전환 신호 없음"

    # BRAIN 선제 조정 제안
    report.preemptive_adjust = _calc_preemptive_adjust(report)

    # 히스토리 저장
    _save_history(report)

    return report


def _calc_preemptive_adjust(report: RegimeLeadingReport) -> dict:
    """선행지표 기반 선제 조정"""
    if report.transition_signal == "IMMINENT" and report.transition_direction == "RISK_OFF":
        return {
            "cash_shift_pct": 10,    # 현금으로 10%p 선제 이동
            "inverse_shift_pct": 5,  # 인버스 5%p 선제 이동
            "reason": f"채권시장 경고 {report.warning_count}/4 — 선제 방어",
        }
    elif report.transition_signal == "WATCH" and report.transition_direction == "RISK_OFF":
        return {
            "cash_shift_pct": 5,
            "inverse_shift_pct": 0,
            "reason": f"채권시장 악화 감지 — 소극적 방어",
        }
    elif report.transition_direction == "RISK_ON" and report.confidence >= 0.5:
        return {
            "cash_shift_pct": -5,   # 현금에서 5%p 리스크자산으로
            "inverse_shift_pct": -3,
            "reason": "채권시장 호전 — 리스크온 가속",
        }
    return {
        "cash_shift_pct": 0,
        "inverse_shift_pct": 0,
        "reason": "전환 신호 없음 — 현행 유지",
    }


# ═══════════════════════════════════════
#  히스토리
# ═══════════════════════════════════════

def _save_history(report: RegimeLeadingReport):
    """선행지표 히스토리 저장 (30일 롤링)"""
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    history = {}
    if HISTORY_PATH.exists():
        try:
            history = json.loads(HISTORY_PATH.read_text(encoding="utf-8"))
        except Exception:
            history = {}

    history[report.date] = {
        "warning_count": report.warning_count,
        "transition_signal": report.transition_signal,
        "transition_direction": report.transition_direction,
        "confidence": report.confidence,
        "credit_spread": report.credit_spread.direction,
        "yield_curve": report.yield_curve.direction,
        "move_index": report.move_index.direction,
        "ted_spread": report.ted_spread.direction,
    }

    dates = sorted(history.keys())
    while len(dates) > 30:
        del history[dates.pop(0)]

    HISTORY_PATH.write_text(
        json.dumps(history, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


# ═══════════════════════════════════════
#  텔레그램 포맷
# ═══════════════════════════════════════

def format_leading_report(report: RegimeLeadingReport) -> str:
    """텔레그램용 선행지표 리포트"""
    lines = [
        "🔮 레짐 전환 선행지표",
        f"📅 {report.date}",
        "",
        f"💡 {report.signal}",
        f"   확신도: {report.confidence:.0%} | 방향: {report.transition_direction}",
        "",
        "── 채권시장 선행지표 ──",
        f"  {report.credit_spread.signal}",
        f"  {report.yield_curve.signal}",
        f"  {report.move_index.signal}",
        f"  {report.ted_spread.signal}",
    ]

    if report.preemptive_adjust.get("reason"):
        lines.append("")
        lines.append(f"📌 {report.preemptive_adjust['reason']}")
        adj = report.preemptive_adjust
        if adj.get("cash_shift_pct", 0) != 0:
            lines.append(
                f"   현금 {adj['cash_shift_pct']:+d}%p"
                f" / 인버스 {adj['inverse_shift_pct']:+d}%p"
            )

    return "\n".join(lines)


# ═══════════════════════════════════════
#  CLI 테스트
# ═══════════════════════════════════════

if __name__ == "__main__":
    import sys
    sys.stdout.reconfigure(encoding="utf-8")
    logging.basicConfig(level=logging.INFO)
    print("레짐 전환 선행지표 분석 시작...")
    report = analyze_leading()
    print(format_leading_report(report))
    print(f"\n선제 조정: {report.preemptive_adjust}")
