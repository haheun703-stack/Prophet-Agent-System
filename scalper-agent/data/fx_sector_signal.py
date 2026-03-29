# -*- coding: utf-8 -*-
"""
FX → Sector Signal — 환율-섹터 자동 매핑
==========================================
원/달러 환율 변화 → 수출주/수입주/내수주 자동 분류 → 섹터 부스트/페널티.

거시경제 원리:
  환율↑(원화 약세) → 수출주 수혜(달러 매출 환산 증가) + 수입주 피해(원재료 비용 증가)
  환율↓(원화 강세) → 수출주 피해(달러 매출 환산 감소) + 수입주 수혜(원재료 비용 감소)
  내수주 → 환율 영향 미미

원칙:
  - nightwatch raw_indicators의 USDKRW 데이터 사용
  - 23개 섹터 각각에 EXPORT/IMPORT/DOMESTIC 분류
  - 환율 변화 ±0.3% 이상일 때만 보정값 생성 (노이즈 방지)
  - 모듈 오류가 매매를 차단하는 일은 절대 없음
"""

import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger("BH.FXSector")


# ═══════════════════════════════════════════
#  23섹터 FX 민감도 분류
# ═══════════════════════════════════════════
#
# EXPORT: 해외 매출 비중 높은 섹터 → 원화 약세 수혜
# IMPORT: 수입 원재료 의존 높은 섹터 → 원화 약세 피해
# DOMESTIC: 환율 영향 미미한 내수 섹터
# MIXED: 수출+수입 혼재 (순효과 작음)

SECTOR_FX_MAP: Dict[str, dict] = {
    # === 수출 섹터 (원화 약세 수혜) ===
    "전기전자": {
        "fx_type": "EXPORT",
        "sensitivity": 0.8,   # 0~1 (반도체+디스플레이 해외 매출 80%+)
        "reason": "반도체/디스플레이 해외 매출 80%+, 달러 환산 직격",
    },
    "운송장비": {
        "fx_type": "EXPORT",
        "sensitivity": 0.7,   # 자동차 수출 비중 높음
        "reason": "자동차/조선 수출 비중 높음",
    },
    "기계장비": {
        "fx_type": "EXPORT",
        "sensitivity": 0.5,   # 산업기계 수출
        "reason": "산업기계 수출, 중간재 성격",
    },
    "금속": {
        "fx_type": "EXPORT",
        "sensitivity": 0.6,   # 철강 수출
        "reason": "철강/비철금속 수출, 원재료도 수입(MIXED 성격)",
    },
    "IT서비스": {
        "fx_type": "EXPORT",
        "sensitivity": 0.4,   # 소프트웨어/게임 해외 매출
        "reason": "게임/소프트웨어 해외 매출 증가 추세",
    },
    "종이목재": {
        "fx_type": "EXPORT",
        "sensitivity": 0.3,   # 일부 수출
        "reason": "포장재 수출 일부",
    },

    # === 수입 의존 섹터 (원화 약세 피해) ===
    "화학": {
        "fx_type": "IMPORT",
        "sensitivity": 0.6,   # 원유/나프타 수입 의존
        "reason": "나프타/에틸렌 원재료 수입 의존, 수출도 있어 MIXED 성격",
    },
    "운송창고": {
        "fx_type": "IMPORT",
        "sensitivity": 0.5,   # 항공 유류비(달러), 해운 운임
        "reason": "항공 유류비 달러 결제, 해운 운임 변동",
    },
    "섬유의류": {
        "fx_type": "IMPORT",
        "sensitivity": 0.4,   # 원자재(면화 등) 수입
        "reason": "면화/합성섬유 원재료 수입",
    },
    "음식료": {
        "fx_type": "IMPORT",
        "sensitivity": 0.3,   # 곡물/원재료 수입
        "reason": "곡물/원당 등 식자재 수입 의존",
    },
    "전기가스": {
        "fx_type": "IMPORT",
        "sensitivity": 0.5,   # 연료(LNG/석탄) 수입
        "reason": "LNG/석탄 연료 수입 의존 (한전 등)",
    },

    # === 내수 섹터 (환율 영향 미미) ===
    "금융": {
        "fx_type": "DOMESTIC",
        "sensitivity": 0.2,   # 간접 영향 (외국인 매매, 환율방어)
        "reason": "환율 간접 영향 — 외국인 매매 흐름",
    },
    "건설": {
        "fx_type": "DOMESTIC",
        "sensitivity": 0.15,
        "reason": "내수 중심, 해외 건설 일부 환율 영향",
    },
    "제약": {
        "fx_type": "DOMESTIC",
        "sensitivity": 0.2,   # 바이오 해외 라이선스 일부
        "reason": "내수 중심, 바이오 해외 라이선스/임상 일부 수혜",
    },
    "일반서비스": {
        "fx_type": "DOMESTIC",
        "sensitivity": 0.1,
        "reason": "순수 내수",
    },
    "유통": {
        "fx_type": "DOMESTIC",
        "sensitivity": 0.15,
        "reason": "내수 소비, 수입 명품 마진 환율 영향 일부",
    },
    "의료정밀": {
        "fx_type": "DOMESTIC",
        "sensitivity": 0.2,
        "reason": "의료기기 수출 일부, 대부분 내수",
    },
    "오락문화": {
        "fx_type": "DOMESTIC",
        "sensitivity": 0.15,
        "reason": "내수 엔터, K-콘텐츠 해외 매출 일부",
    },
    "통신": {
        "fx_type": "DOMESTIC",
        "sensitivity": 0.05,
        "reason": "순수 내수 인프라",
    },
    "비금속": {
        "fx_type": "MIXED",
        "sensitivity": 0.3,
        "reason": "시멘트 내수 + 유리 수출 혼재",
    },
    "출판매체": {
        "fx_type": "DOMESTIC",
        "sensitivity": 0.1,
        "reason": "내수 미디어",
    },
    "기타제조": {
        "fx_type": "MIXED",
        "sensitivity": 0.2,
        "reason": "다양한 업종 혼재",
    },
    "기타": {
        "fx_type": "DOMESTIC",
        "sensitivity": 0.0,
        "reason": "미분류",
    },
}


# ═══════════════════════════════════════════
#  데이터 구조
# ═══════════════════════════════════════════

@dataclass
class FXSectorImpact:
    """환율 변화에 따른 섹터별 영향"""
    usdkrw_value: float = 0.0       # 현재 환율
    usdkrw_change_pct: float = 0.0  # 전일 대비 변화율 (%)
    fx_regime: str = "STABLE"       # STRONG_WEAK / WEAK / STABLE / STRONG / STRONG_STRONG
    sector_adjustments: Dict[str, float] = field(default_factory=dict)  # {sector: adj_score}
    narrative: str = ""


# ═══════════════════════════════════════════
#  환율 레짐 분류
# ═══════════════════════════════════════════

def _classify_fx_regime(change_pct: float) -> str:
    """
    원/달러 환율 변화율 → 레짐 분류.

    STRONG_WEAK: +1.0%↑ (원화 급락 — 수출주 강한 수혜)
    WEAK:        +0.3%~+1.0% (원화 약세 — 수출주 수혜)
    STABLE:      -0.3%~+0.3% (영향 미미)
    STRONG:      -1.0%~-0.3% (원화 강세 — 수입주 수혜)
    STRONG_STRONG: -1.0%↓ (원화 급등 — 수입주 강한 수혜)
    """
    if change_pct >= 1.0:
        return "STRONG_WEAK"      # 원화 급락
    elif change_pct >= 0.3:
        return "WEAK"             # 원화 약세
    elif change_pct <= -1.0:
        return "STRONG_STRONG"    # 원화 급등
    elif change_pct <= -0.3:
        return "STRONG"           # 원화 강세
    else:
        return "STABLE"


# ═══════════════════════════════════════════
#  섹터별 FX 보정값 계산
# ═══════════════════════════════════════════

def _calc_sector_adjustments(change_pct: float, regime: str) -> Dict[str, float]:
    """
    환율 변화 → 23섹터 각각의 보정값.

    계산 공식:
      EXPORT 섹터: change_pct × sensitivity × fx_multiplier (원화 약세 → +)
      IMPORT 섹터: -change_pct × sensitivity × fx_multiplier (원화 약세 → -)
      DOMESTIC/MIXED: 미미한 간접 효과

    fx_multiplier (레짐별 강도):
      STRONG_WEAK/STRONG_STRONG: 3.0 (급변 시 강한 영향)
      WEAK/STRONG: 2.0 (의미 있는 변화)
      STABLE: 0.0 (노이즈 무시)

    보정값 범위: ±5.0 (sector_momentum boost_score에 더할 값)
    """
    if regime == "STABLE":
        return {}

    mult_map = {
        "STRONG_WEAK": 3.0,
        "WEAK": 2.0,
        "STRONG": 2.0,
        "STRONG_STRONG": 3.0,
    }
    fx_mult = mult_map.get(regime, 1.0)

    adjustments = {}
    for sector, info in SECTOR_FX_MAP.items():
        fx_type = info["fx_type"]
        sensitivity = info["sensitivity"]

        if sensitivity <= 0:
            continue

        if fx_type == "EXPORT":
            # 원화 약세(change_pct > 0) → 수출 수혜 → 양수
            adj = change_pct * sensitivity * fx_mult
        elif fx_type == "IMPORT":
            # 원화 약세(change_pct > 0) → 수입 피해 → 음수
            adj = -change_pct * sensitivity * fx_mult
        elif fx_type == "MIXED":
            # 순효과 약함
            adj = change_pct * sensitivity * fx_mult * 0.3
        else:  # DOMESTIC
            # 간접 효과 (외국인 자금 흐름)
            # 원화 약세 → 외국인 이탈 → 내수주도 소폭 하락 경향
            adj = -abs(change_pct) * sensitivity * fx_mult * 0.3

        # 클램핑 ±5.0
        adj = max(-5.0, min(5.0, round(adj, 1)))
        if abs(adj) >= 0.3:  # 0.3 미만은 무시
            adjustments[sector] = adj

    return adjustments


# ═══════════════════════════════════════════
#  메인: FX 섹터 영향 분석
# ═══════════════════════════════════════════

def analyze_fx_sector_impact(
    raw_indicators: Dict = None,
) -> FXSectorImpact:
    """
    환율 변화 기반 섹터 영향 종합 분석.

    Args:
        raw_indicators: nightwatch에서 수집한 글로벌 지표
            - USDKRW: {value, change_pct}

    Returns:
        FXSectorImpact: 섹터별 보정값 맵
    """
    impact = FXSectorImpact()

    if not raw_indicators:
        impact.narrative = "환율 데이터 없음"
        return impact

    usdkrw = raw_indicators.get("USDKRW", {})
    impact.usdkrw_value = usdkrw.get("value", 0) or 0
    impact.usdkrw_change_pct = usdkrw.get("change_pct", 0) or 0

    if impact.usdkrw_value <= 0:
        impact.narrative = "USDKRW 데이터 없음"
        return impact

    # 레짐 분류
    impact.fx_regime = _classify_fx_regime(impact.usdkrw_change_pct)

    # 섹터별 보정값 계산
    impact.sector_adjustments = _calc_sector_adjustments(
        impact.usdkrw_change_pct, impact.fx_regime
    )

    # 내러티브
    regime_kr = {
        "STRONG_WEAK": "원화 급락",
        "WEAK": "원화 약세",
        "STABLE": "환율 안정",
        "STRONG": "원화 강세",
        "STRONG_STRONG": "원화 급등",
    }
    parts = [
        f"$/₩ {impact.usdkrw_value:.0f}({impact.usdkrw_change_pct:+.2f}%)",
        regime_kr.get(impact.fx_regime, impact.fx_regime),
    ]

    if impact.sector_adjustments:
        # 수혜/피해 섹터 요약
        beneficiaries = sorted(
            [(s, a) for s, a in impact.sector_adjustments.items() if a > 0],
            key=lambda x: -x[1]
        )[:3]
        casualties = sorted(
            [(s, a) for s, a in impact.sector_adjustments.items() if a < 0],
            key=lambda x: x[1]
        )[:3]

        if beneficiaries:
            names = "/".join(s for s, _ in beneficiaries)
            parts.append(f"수혜: {names}")
        if casualties:
            names = "/".join(s for s, _ in casualties)
            parts.append(f"피해: {names}")

    impact.narrative = " | ".join(parts)
    logger.info(f"[FXSector] {impact.narrative}")

    return impact


# ═══════════════════════════════════════════
#  외부 API: 섹터 부스트 조회
# ═══════════════════════════════════════════

def get_fx_sector_boost(sector: str, raw_indicators: Dict = None) -> float:
    """
    특정 섹터의 FX 보정값 반환.
    sector_momentum.py에서 호출.

    Args:
        sector: 섹터명 (universe.json 기준)
        raw_indicators: nightwatch raw_indicators (없으면 캐시 사용)

    Returns:
        보정값 (-5.0 ~ +5.0). 영향 없으면 0.0
    """
    try:
        impact = analyze_fx_sector_impact(raw_indicators)
        return impact.sector_adjustments.get(sector, 0.0)
    except Exception as e:
        logger.warning(f"[FXSector] 부스트 조회 실패: {e}")
        return 0.0


def get_fx_all_sector_boosts(raw_indicators: Dict = None) -> Dict[str, float]:
    """
    전체 섹터 FX 보정값 맵 반환.
    BRAIN Phase 3 또는 morning_recommendation에서 활용.

    Returns:
        {sector_name: adj_score}
    """
    try:
        impact = analyze_fx_sector_impact(raw_indicators)
        return impact.sector_adjustments
    except Exception as e:
        logger.warning(f"[FXSector] 전체 부스트 실패: {e}")
        return {}


def get_fx_type(sector: str) -> str:
    """섹터의 FX 분류 반환 (EXPORT/IMPORT/DOMESTIC/MIXED)"""
    info = SECTOR_FX_MAP.get(sector, {})
    return info.get("fx_type", "DOMESTIC")
