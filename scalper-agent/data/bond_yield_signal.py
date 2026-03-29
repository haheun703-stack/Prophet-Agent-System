# -*- coding: utf-8 -*-
"""
Bond Yield Signal — 채권금리 시그널 모듈
==========================================
US10Y(TNX) + KR3Y + 크레딧 스프레드(HYG) 기반 채권 환경 분석.

금리 ↑ → 채권 매력 ↑ → 주식 자금 유출 → 방어 가중치 ↑
금리 ↓ → 채권 매력 ↓ → 주식 자금 유입 → 공격 가중치 ↑
금리 급등 + HYG ↓ → 크레딧 스트레스 → 패닉 경고

원칙:
  - nightwatch raw_indicators에서 TNX/HYG 데이터 수신
  - yfinance로 5일 추세 직접 수집 (실패 시 graceful fallback)
  - 모듈 오류가 매매를 차단하는 일은 절대 없음
"""

import logging
from dataclasses import dataclass, field
from typing import Dict, Optional, Tuple

logger = logging.getLogger("BH.BondYield")


# ═══════════════════════════════════════════
#  데이터 구조
# ═══════════════════════════════════════════

@dataclass
class BondEnvironment:
    """채권 환경 종합 평가"""
    # 원시 데이터
    us10y_value: float = 0.0        # 미국 10년물 수익률 (%)
    us10y_change: float = 0.0       # 전일 대비 변화 (%p)
    us10y_5d_change: float = 0.0    # 5일 누적 변화 (%p)
    kr3y_value: float = 0.0         # 한국 3년물 수익률 (%)
    kr3y_change: float = 0.0        # 전일 대비 변화 (%p)
    hyg_change: float = 0.0         # HYG(하이일드) 전일 등락률 (%)

    # 분류
    level_class: str = "MODERATE"   # LOW / MODERATE / ELEVATED / HIGH
    change_class: str = "STABLE"    # SHARP_DROP / DROP / STABLE / RISE / SHARP_RISE
    trend_class: str = "SIDEWAYS"   # FALLING / SIDEWAYS / RISING
    credit_class: str = "NEUTRAL"   # EASING / NEUTRAL / STRESS / SEVERE_STRESS
    spread_class: str = "UNKNOWN"   # NARROWING / STABLE / WIDENING (KR-US 스프레드)

    # 스코어 (±5.0 범위)
    level_score: float = 0.0        # 금리 수준 점수 (±1.5)
    change_score: float = 0.0       # 일간 변화 점수 (±1.5)
    trend_score: float = 0.0        # 5일 추세 점수 (±1.0)
    credit_score: float = 0.0       # 크레딧 콤보 점수 (±1.0)
    composite_score: float = 0.0    # 종합 점수 (±5.0)

    # 통합 출력
    verdict: str = "NEUTRAL"        # DOVISH / NEUTRAL / HAWKISH / TIGHTENING_SHOCK
    brain_adj: float = 0.0          # BRAIN Phase 6 보정값 (±5)
    nxt_adj: float = 0.0            # NXT 스코어 보정값 (±1.5)
    narrative: str = ""             # 한줄 요약


# ═══════════════════════════════════════════
#  금리 수준 분류 (US10Y 절대값)
# ═══════════════════════════════════════════

def _classify_level(us10y: float) -> Tuple[str, float]:
    """
    미국 10년물 수익률 수준 → 주식 밸류에이션 압력 평가.

    < 3.5% : 저금리 → 성장주 유리 (+1.5)
    3.5~4.0%: 적정 금리 → 중립 (+0.5)
    4.0~4.5%: 고금리 → 밸류에이션 압박 시작 (-0.5)
    > 4.5% : 초고금리 → 강한 압박 (-1.5)
    """
    if us10y <= 0:
        return "MODERATE", 0.0
    if us10y < 3.5:
        return "LOW", 1.5
    elif us10y < 4.0:
        return "MODERATE", 0.5
    elif us10y < 4.5:
        return "ELEVATED", -0.5
    else:
        return "HIGH", -1.5


# ═══════════════════════════════════════════
#  일간 변화 분류 (bp 단위)
# ═══════════════════════════════════════════

def _classify_change(change_abs: float) -> Tuple[str, float]:
    """
    전일 대비 금리 변화 (%p 단위) → 자금 흐름 방향 추정.

    급락(-8bp 이하): 금리 인하 기대 → 주식 유리 (+1.5)
    하락(-4bp~-8bp): 완만 하락 → 소폭 유리 (+0.7)
    안정(-4bp~+4bp): 중립 (0)
    상승(+4bp~+8bp): 긴축 우려 → 소폭 불리 (-0.7)
    급등(+8bp 이상): 금리 쇼크 → 주식 불리 (-1.5)
    """
    bp = change_abs * 100  # %p → bp 변환 (0.05 → 5bp)

    if bp <= -8:
        return "SHARP_DROP", 1.5
    elif bp <= -4:
        return "DROP", 0.7
    elif bp <= 4:
        return "STABLE", 0.0
    elif bp <= 8:
        return "RISE", -0.7
    else:
        return "SHARP_RISE", -1.5


# ═══════════════════════════════════════════
#  5일 추세 분류
# ═══════════════════════════════════════════

def _classify_trend(change_5d: float) -> Tuple[str, float]:
    """
    5일 누적 금리 변화 → 추세 방향 판단.

    -20bp 이상 하락: 하락 추세 → 주식 유리 (+1.0)
    -10bp~-20bp: 소폭 하락 → (+0.5)
    -10bp~+10bp: 횡보 (0)
    +10bp~+20bp: 소폭 상승 → (-0.5)
    +20bp 이상 상승: 상승 추세 → 주식 불리 (-1.0)
    """
    bp5d = change_5d * 100

    if bp5d <= -20:
        return "FALLING", 1.0
    elif bp5d <= -10:
        return "FALLING", 0.5
    elif bp5d <= 10:
        return "SIDEWAYS", 0.0
    elif bp5d <= 20:
        return "RISING", -0.5
    else:
        return "RISING", -1.0


# ═══════════════════════════════════════════
#  크레딧 스프레드 콤보 (TNX + HYG)
# ═══════════════════════════════════════════

def _classify_credit(tnx_change: float, hyg_change: float) -> Tuple[str, float]:
    """
    금리 변화 + 하이일드 채권 변화 조합 → 크레딧 스트레스 판단.

    금리↓ + HYG↑: 크레딧 완화 → 위험자산 유리 (+1.0)
    금리↑ + HYG↓: 크레딧 스트레스 → 위험자산 불리 (-1.0)
    금리↑↑ + HYG↓↓: 심각한 스트레스 → 패닉 (-1.5)

    *거시 교과서: "금리 오르는데 하이일드도 빠지면 자금이 나간다"*
    """
    tnx_bp = tnx_change * 100

    # 심각한 스트레스: 금리 5bp↑ + HYG -0.5%↓
    if tnx_bp > 5 and hyg_change < -0.5:
        return "SEVERE_STRESS", -1.5

    # 스트레스: 금리↑ + HYG↓
    if tnx_bp > 3 and hyg_change < -0.3:
        return "STRESS", -1.0

    # 완화: 금리↓ + HYG↑
    if tnx_bp < -3 and hyg_change > 0.3:
        return "EASING", 1.0

    return "NEUTRAL", 0.0


# ═══════════════════════════════════════════
#  KR-US 금리 스프레드 (선택적)
# ═══════════════════════════════════════════

def _classify_spread(kr3y: float, us10y: float) -> Tuple[str, float]:
    """
    한-미 금리차 → 외국인 자본 흐름 방향.

    스프레드 = KR3Y - US10Y
    스프레드 축소(음수 방향): 외국인 자본 유출 압력
    스프레드 확대(양수 방향): 외국인 자본 유입 유인

    현재는 level 기반 (추후 변화율 추적 가능)
    """
    if kr3y <= 0 or us10y <= 0:
        return "UNKNOWN", 0.0

    spread = kr3y - us10y

    # 한국 금리가 미국보다 높으면 자본 유입 유인
    if spread > 0.5:
        return "NARROWING", 0.3  # KR > US → 유입 유인 (약간 긍정)
    elif spread > -0.5:
        return "STABLE", 0.0
    else:
        return "WIDENING", -0.3  # US >> KR → 유출 압력 (약간 부정)


# ═══════════════════════════════════════════
#  5일 추세 수집 (yfinance)
# ═══════════════════════════════════════════

def _fetch_5day_trend() -> float:
    """
    US10Y 5거래일 누적 변화(%p) 수집.
    실패 시 0.0 반환 (graceful fallback).
    """
    try:
        import yfinance as yf
        from datetime import date, timedelta

        end = date.today()
        start = end - timedelta(days=12)  # 여유 있게 12일

        tnx = yf.Ticker("^TNX")
        hist = tnx.history(start=start.isoformat(), end=end.isoformat())

        if hist is not None and len(hist) >= 2:
            closes = hist["Close"].dropna()
            if len(closes) >= 2:
                current = closes.iloc[-1]
                # 5거래일 전 (또는 가용한 가장 오래된 데이터)
                lookback = min(5, len(closes) - 1)
                past = closes.iloc[-1 - lookback]
                change_5d = (current - past) / 100  # TNX는 %단위 → %p 변환
                return round(change_5d, 4)

    except Exception as e:
        logger.debug(f"[BondYield] 5일 추세 수집 실패 (fallback 0): {e}")

    return 0.0


def _fetch_kr3y() -> Tuple[float, float]:
    """
    한국 3년물 국채 수익률 수집 시도.
    yfinance로 시도 → 실패시 (0, 0) 반환.

    Returns: (현재값, 전일대비 변화 %p)
    """
    try:
        import yfinance as yf
        from datetime import date, timedelta

        # 방법 1: FRED 한국 3년물 (최근 데이터 지연 가능)
        # 방법 2: Investing.com 크롤링 (불안정)
        # 방법 3: KIS API 채권 시세 (미구현)
        # → 현재: 수집 불가 시 0 반환, 로그만 남김

        logger.debug("[BondYield] KR3Y 수집: 현재 직접 소스 미구현 (향후 KIS API 연동 예정)")
        return 0.0, 0.0

    except Exception as e:
        logger.debug(f"[BondYield] KR3Y 수집 실패: {e}")
        return 0.0, 0.0


# ═══════════════════════════════════════════
#  메인: 채권 환경 분석
# ═══════════════════════════════════════════

def analyze_bond_environment(
    raw_indicators: Dict = None,
    fetch_trend: bool = True,
) -> BondEnvironment:
    """
    채권 환경 종합 분석 — nightwatch raw_indicators 기반.

    Args:
        raw_indicators: nightwatch에서 수집한 글로벌 지표
            - TNX: {value, change_abs, change_pct}
            - HYG_div: {change_pct}
        fetch_trend: True이면 yfinance로 5일 추세 추가 수집

    Returns:
        BondEnvironment: 종합 평가 (composite_score, brain_adj, nxt_adj)
    """
    env = BondEnvironment()

    if not raw_indicators:
        env.narrative = "채권 데이터 없음"
        return env

    # ── 원시 데이터 추출 ──
    tnx = raw_indicators.get("TNX", {})
    env.us10y_value = tnx.get("value", 0) or 0
    env.us10y_change = tnx.get("change_abs", 0) or 0
    hyg = raw_indicators.get("HYG_div", {}) or raw_indicators.get("HYG", {})
    env.hyg_change = hyg.get("change_pct", 0) or 0

    if env.us10y_value <= 0:
        env.narrative = "US10Y 데이터 없음"
        return env

    # ── 5일 추세 수집 ──
    if fetch_trend:
        env.us10y_5d_change = _fetch_5day_trend()

    # ── KR3Y 수집 (향후 확장) ──
    env.kr3y_value, env.kr3y_change = _fetch_kr3y()

    # ── 5개 축 분류 ──
    env.level_class, env.level_score = _classify_level(env.us10y_value)
    env.change_class, env.change_score = _classify_change(env.us10y_change)
    env.trend_class, env.trend_score = _classify_trend(env.us10y_5d_change)
    env.credit_class, env.credit_score = _classify_credit(env.us10y_change, env.hyg_change)
    env.spread_class, _ = _classify_spread(env.kr3y_value, env.us10y_value)

    # ── 종합 스코어 ──
    env.composite_score = round(
        env.level_score + env.change_score + env.trend_score + env.credit_score,
        1
    )
    # 클램핑 ±5.0
    env.composite_score = max(-5.0, min(5.0, env.composite_score))

    # ── 판정 (verdict) ──
    if env.composite_score >= 2.0:
        env.verdict = "DOVISH"       # 금리 하락 → 주식 우호
    elif env.composite_score >= -1.0:
        env.verdict = "NEUTRAL"
    elif env.composite_score >= -3.0:
        env.verdict = "HAWKISH"      # 금리 상승 → 밸류에이션 압박
    else:
        env.verdict = "TIGHTENING_SHOCK"  # 금리 급등 + 크레딧 스트레스

    # ── BRAIN 보정값 (Phase 6 synthesis에 반영) ──
    # composite_score를 ±5 범위로 매핑
    env.brain_adj = env.composite_score

    # ── NXT 보정값 (calculate_nightwatch_score에 반영) ──
    # 일간 변화가 의미 있을 때만 적용 (±4bp 이상)
    if abs(env.us10y_change * 100) >= 4:  # 4bp 이상
        env.nxt_adj = round(env.change_score * 0.8, 1)  # 변화 점수의 80%
        # 크레딧 스트레스 가중
        if env.credit_class in ("STRESS", "SEVERE_STRESS"):
            env.nxt_adj += env.credit_score * 0.5
        env.nxt_adj = max(-1.5, min(1.5, round(env.nxt_adj, 1)))

    # ── 내러티브 ──
    parts = []
    parts.append(f"US10Y {env.us10y_value:.2f}%({env.us10y_change*100:+.1f}bp)")

    if env.us10y_5d_change != 0:
        parts.append(f"5일{env.us10y_5d_change*100:+.1f}bp")

    verdict_kr = {
        "DOVISH": "완화적(주식 우호)",
        "NEUTRAL": "중립",
        "HAWKISH": "긴축적(밸류에이션 압박)",
        "TIGHTENING_SHOCK": "금리쇼크(크레딧 스트레스)",
    }
    parts.append(verdict_kr.get(env.verdict, env.verdict))

    if env.credit_class not in ("NEUTRAL", ""):
        credit_kr = {"EASING": "크레딧완화", "STRESS": "크레딧스트레스",
                     "SEVERE_STRESS": "심각한 크레딧스트레스"}
        parts.append(credit_kr.get(env.credit_class, ""))

    env.narrative = " | ".join(p for p in parts if p)

    logger.info(
        f"[BondYield] {env.narrative} → "
        f"score={env.composite_score:+.1f} brain={env.brain_adj:+.1f} nxt={env.nxt_adj:+.1f}"
    )

    return env


# ═══════════════════════════════════════════
#  외부 API: BRAIN 보정값
# ═══════════════════════════════════════════

def get_brain_bond_adjustment(raw_indicators: Dict = None) -> Tuple[float, str]:
    """
    BRAIN Phase 6에서 호출하는 간편 API.

    Returns:
        (adj_score, detail_text)
        adj_score: -5.0 ~ +5.0 (BRAIN 종합 점수에 더할 값)
        detail_text: "DOVISH(+2.3)" 같은 요약
    """
    try:
        env = analyze_bond_environment(raw_indicators, fetch_trend=True)
        if env.us10y_value <= 0:
            return 0.0, ""
        detail = f"{env.verdict}({env.brain_adj:+.1f})"
        return env.brain_adj, detail
    except Exception as e:
        logger.warning(f"[BondYield] BRAIN 보정 실패: {e}")
        return 0.0, ""


# ═══════════════════════════════════════════
#  외부 API: NXT 보정값
# ═══════════════════════════════════════════

def get_nxt_bond_adjustment(raw_indicators: Dict = None) -> Tuple[float, str]:
    """
    NXT calculate_nightwatch_score에서 호출하는 간편 API.

    Returns:
        (adj_score, detail_text)
        adj_score: -1.5 ~ +1.5 (NXT total에 더할 값)
        detail_text: "금리↓ +0.8" 같은 요약
    """
    try:
        # NXT는 이미 raw_indicators를 가지고 있으므로 추세 수집 불필요
        env = analyze_bond_environment(raw_indicators, fetch_trend=False)
        if env.us10y_value <= 0 or env.nxt_adj == 0:
            return 0.0, ""

        direction = "금리↓" if env.nxt_adj > 0 else "금리↑"
        detail = f"{direction} {env.nxt_adj:+.1f}"
        return env.nxt_adj, detail
    except Exception as e:
        logger.warning(f"[BondYield] NXT 보정 실패: {e}")
        return 0.0, ""
