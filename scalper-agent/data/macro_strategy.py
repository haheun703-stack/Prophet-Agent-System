# -*- coding: utf-8 -*-
"""
Macro Strategy — 매크로 레짐별 대응 전략
==========================================
"감지"에서 끝나지 않고 "대처"까지.

감지: 스태그플레이션 → 유가60MA+39%, 구리-5%
대처: 투자비중 30%, VICTIM 섹터 회피, SL 타이트, ETF 방어전환

4가지 매크로 국면 대응:
  스태그플레이션 — 최악 시나리오 (비용↑ + 경기↓)
    → 비중 30%(방어) / SL -2.0% / 최대보유 3일
    → VICTIM 섹터 블랙리스트 / BENEFICIARY만 매수
    → ETF: 인버스/금 ETF 추천
  비용상승      — 비용 인상 국면
    → 비중 50%(축소) / SL -2.5% / 최대보유 4일
    → VICTIM 페널티 강화 / HEDGE 섹터 우대
    → ETF: 에너지/원자재 ETF 추천
  수요둔화      — 수요 위축
    → 비중 50%(축소) / SL -3.0% / 최대보유 5일
    → HEDGE(원자재) 회피 / 내수 BENEFICIARY 우대
    → ETF: 채권/배당 ETF 추천
  안정          — 정상
    → 비중 70~100%(표준) / SL -3.5% / 최대보유 5일
    → 제한 없음

연결점:
  - auto_trader: SL/보유기간/예산 조정
  - morning_recommendation: VICTIM 블랙리스트
  - market_brain: position_size_pct 직접 영향
  - telegram: 레짐 변경 알림 + 대응 안내

원칙:
  - 모든 조정은 config 기본값에 배수를 적용하는 방식 (기본값 자체를 변경하지 않음)
  - 모듈 오류 시 STABLE(기본 동작) 폴백
"""

import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger("BH.MacroStrategy")


# ═══════════════════════════════════════════
#  레짐별 대응 매트릭스
# ═══════════════════════════════════════════

@dataclass
class RegimeResponse:
    """레짐 대응 전략"""
    regime: str = "STABLE"

    # 1. 투자 비중 (BRAIN position_size_pct에 곱할 캡)
    max_position_pct: int = 100     # 최대 투자 비중 (%)
    budget_multiplier: float = 1.0  # 예산 배수 (CFO에 전달)

    # 2. 손절/보유 조정
    sl_pct: float = 0.035           # 손절 기준 (3.5% = 0.035)
    trailing_pct: float = 0.02      # 트레일링 스톱 (2%)
    max_hold_days: int = 5          # 최대 보유일

    # 3. 섹터 블랙리스트/화이트리스트
    blacklist_classes: List[str] = field(default_factory=list)   # 매수 금지 net_class
    preferred_classes: List[str] = field(default_factory=list)   # 우대 net_class
    sector_penalty_mult: float = 1.0  # VICTIM 페널티 강도 배수

    # 4. ETF 전략
    etf_defensive: bool = False     # 방어 ETF 전환 활성화
    etf_preferred_types: List[str] = field(default_factory=list)  # 선호 ETF 유형

    # 5. 요약
    strategy_name: str = ""         # "방어 전략" / "비용 인상 대응" 등
    action_summary: str = ""        # 텔레그램 대응 안내 문구


# ═══════════════════════════════════════════
#  4-레짐 대응 전략 정의
# ═══════════════════════════════════════════

REGIME_STRATEGIES: Dict[str, RegimeResponse] = {
    "스태그플레이션": RegimeResponse(
        regime="스태그플레이션",
        max_position_pct=30,
        budget_multiplier=0.4,
        sl_pct=0.020,           # 타이트: -2.0%
        trailing_pct=0.015,     # 타이트: -1.5%
        max_hold_days=3,
        blacklist_classes=["VICTIM"],
        preferred_classes=["BENEFICIARY"],
        sector_penalty_mult=2.0,  # VICTIM 페널티 2배
        etf_defensive=True,
        etf_preferred_types=["인버스", "금", "채권"],
        strategy_name="스태그플레이션 방어",
        action_summary=(
            "⚠️ 스태그플레이션 경고\n"
            "• 투자비중: 30% (최소화)\n"
            "• 손절: -2.0% (타이트)\n"
            "• 보유: 최대 3일\n"
            "• 회피: 전기가스/운송/건설/음식료 (비용 전가 불가)\n"
            "• 선호: 금융/IT/제약/통신 (물적 투입 적음)\n"
            "• ETF: 인버스/금 ETF 고려"
        ),
    ),

    "비용상승": RegimeResponse(
        regime="비용상승",
        max_position_pct=50,
        budget_multiplier=0.6,
        sl_pct=0.025,           # 약간 타이트: -2.5%
        trailing_pct=0.018,
        max_hold_days=4,
        blacklist_classes=[],   # 블랙은 아니지만 페널티 강화
        preferred_classes=["HEDGE", "BENEFICIARY"],
        sector_penalty_mult=1.5,
        etf_defensive=False,
        etf_preferred_types=["에너지", "원자재"],
        strategy_name="비용 인상 대응",
        action_summary=(
            "📊 비용 인상 국면\n"
            "• 투자비중: 50% (축소)\n"
            "• 손절: -2.5%\n"
            "• 보유: 최대 4일\n"
            "• 회피: 전기가스/운송 (에너지 비용 직격)\n"
            "• 선호: 금속/에너지 (원자재 가격=매출) + 금융/IT\n"
            "• ETF: 에너지/원자재 ETF 고려"
        ),
    ),

    "수요둔화": RegimeResponse(
        regime="수요둔화",
        max_position_pct=50,
        budget_multiplier=0.6,
        sl_pct=0.030,           # 보통: -3.0%
        trailing_pct=0.020,
        max_hold_days=5,
        blacklist_classes=[],
        preferred_classes=["VICTIM"],  # 디플레 시 비용↓ → VICTIM 수혜
        sector_penalty_mult=0.5,       # VICTIM 페널티 감소 (오히려 수혜)
        etf_defensive=True,
        etf_preferred_types=["채권", "배당"],
        strategy_name="수요둔화 대응",
        action_summary=(
            "📉 수요둔화 경고\n"
            "• 투자비중: 50% (축소)\n"
            "• 손절: -3.0%\n"
            "• 보유: 최대 5일\n"
            "• 회피: 금속/에너지 (원자재 하락=매출 하락)\n"
            "• 선호: 전기가스/운송 (비용↓수혜) + IT/제약\n"
            "• ETF: 채권/배당 ETF 고려"
        ),
    ),

    "안정": RegimeResponse(
        regime="안정",
        max_position_pct=100,
        budget_multiplier=1.0,
        sl_pct=0.035,           # 표준: -3.5%
        trailing_pct=0.020,
        max_hold_days=5,
        blacklist_classes=[],
        preferred_classes=[],
        sector_penalty_mult=1.0,
        etf_defensive=False,
        etf_preferred_types=[],
        strategy_name="표준 운영",
        action_summary="✅ 매크로 안정 — 표준 전략 유지",
    ),
}


# ═══════════════════════════════════════════
#  공개 API
# ═══════════════════════════════════════════

def get_current_regime() -> str:
    """현재 인플레이션 레짐 반환. 캐시 없거나 24h+ stale이면 '안정'."""
    try:
        from data.inflation_chain import analyze_inflation_chain
        import json
        from pathlib import Path
        from datetime import datetime

        # nightwatch raw_indicators 로드
        nw_path = Path(__file__).resolve().parent.parent / "data_store" / "nightwatch_report.json"
        if not nw_path.exists():
            return "안정"

        nw = json.loads(nw_path.read_text("utf-8"))

        # staleness 체크: 24시간 이상이면 경고 + 안정 폴백
        ts = nw.get("timestamp", "")
        if ts:
            try:
                nw_dt = datetime.strptime(ts[:19], "%Y-%m-%d %H:%M:%S")
                age_hours = (datetime.now() - nw_dt).total_seconds() / 3600
                if age_hours > 48:
                    logger.warning(
                        f"[MacroStrategy] nightwatch_report {age_hours:.0f}h stale → 안정 폴백")
                    return "안정"
            except (ValueError, TypeError):
                pass

        ri = nw.get("raw_indicators", {})
        result = analyze_inflation_chain(ri)
        return result.regime
    except Exception:
        pass
    return "안정"


def get_regime_response(regime: str = None) -> RegimeResponse:
    """
    레짐 대응 전략 반환.

    Args:
        regime: 레짐 문자열. None이면 현재 레짐 자동 감지.

    Returns:
        RegimeResponse: 대응 전략 (SL/비중/블랙리스트 등)
    """
    if regime is None:
        regime = get_current_regime()

    return REGIME_STRATEGIES.get(regime, REGIME_STRATEGIES["안정"])


def should_avoid_sector(sector: str, regime: str = None) -> Tuple[bool, str]:
    """
    특정 섹터가 현재 레짐에서 회피 대상인지 판단.

    Returns:
        (should_avoid: bool, reason: str)
    """
    try:
        from data.inflation_chain import get_inflation_class
        response = get_regime_response(regime)

        net_class = get_inflation_class(sector)

        if net_class in response.blacklist_classes:
            return True, f"{sector}: {net_class}→블랙리스트({response.strategy_name})"

        return False, ""
    except Exception:
        return False, ""


def get_sector_score_multiplier(sector: str, regime: str = None) -> float:
    """
    섹터의 점수 배수 반환 (추천 파이프라인에서 사용).

    VICTIM in 스태그플레이션 → 0.3 (70% 감점)
    BENEFICIARY in 스태그플레이션 → 1.5 (50% 가점)
    안정 → 1.0 (변화 없음)
    """
    try:
        from data.inflation_chain import get_inflation_class
        response = get_regime_response(regime)
        net_class = get_inflation_class(sector)

        if response.regime == "안정":
            return 1.0

        if net_class in response.blacklist_classes:
            return 0.3  # 강한 감점

        if net_class in response.preferred_classes:
            return 1.3  # 가점

        if net_class == "VICTIM":
            # 블랙리스트는 아니지만 페널티 강화
            penalty = response.sector_penalty_mult
            return max(0.5, 1.0 - 0.3 * penalty)

        return 1.0
    except Exception:
        return 1.0


def get_adjusted_sl(base_sl_pct: float = 0.035, regime: str = None) -> float:
    """
    레짐에 맞게 조정된 손절 비율 반환.

    Args:
        base_sl_pct: 기본 손절 비율 (config에서 읽은 값)
        regime: None이면 현재 레짐 자동 감지

    Returns:
        조정된 SL 비율 (예: 0.020 = -2.0%)
    """
    response = get_regime_response(regime)
    # 레짐 SL이 기본값보다 타이트하면 레짐 SL 사용
    return min(base_sl_pct, response.sl_pct)


def get_adjusted_max_hold(base_days: int = 5, regime: str = None) -> int:
    """레짐에 맞게 조정된 최대 보유일 반환."""
    response = get_regime_response(regime)
    return min(base_days, response.max_hold_days)


def get_budget_cap(regime: str = None) -> Tuple[int, float]:
    """
    레짐 기반 투자비중 상한 + 예산 배수.

    Returns:
        (max_position_pct: int, budget_multiplier: float)
    """
    response = get_regime_response(regime)
    return response.max_position_pct, response.budget_multiplier


def get_regime_alert() -> str:
    """
    현재 레짐 대응 안내 텍스트 (텔레그램 발송용).
    STABLE이면 빈 문자열 반환 (알림 불필요).
    """
    regime = get_current_regime()
    response = get_regime_response(regime)

    if regime == "안정":
        return ""

    return response.action_summary


def get_strategy_dashboard() -> str:
    """텔레그램 대시보드용 현재 전략 요약."""
    regime = get_current_regime()
    response = get_regime_response(regime)

    lines = [
        f"🎯 매크로 전략: {response.strategy_name}",
        f"  레짐: {regime}",
        f"  투자비중 상한: {response.max_position_pct}%",
        f"  예산 배수: ×{response.budget_multiplier:.1f}",
        f"  손절: -{response.sl_pct*100:.1f}%",
        f"  트레일링: -{response.trailing_pct*100:.1f}%",
        f"  최대 보유: {response.max_hold_days}일",
    ]

    if response.blacklist_classes:
        lines.append(f"  🚫 블랙리스트: {', '.join(response.blacklist_classes)} 섹터")
    if response.preferred_classes:
        lines.append(f"  ⭐ 선호: {', '.join(response.preferred_classes)} 섹터")
    if response.etf_preferred_types:
        lines.append(f"  📈 ETF: {'/'.join(response.etf_preferred_types)}")

    return "\n".join(lines)
