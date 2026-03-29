# -*- coding: utf-8 -*-
"""
Inflation Chain — 인플레이션 비용 체인 모델
=============================================
CPI/PPI 대리변수(원자재 가격 변동)로 인플레이션 레짐 감지 → 섹터별 비용 전가력 분석.

거시경제 원리:
  원자재 상승 → 기업 투입비용 ↑ → 가격 전가력(Pricing Power) 높은 섹터 수혜
  원자재 하락 → 비용 하락 수혜 + 디플레이션 리스크
  금 상승 + 구리 하락 → 스태그플레이션 경고 (경기 둔화 + 인플레 동시)

4가지 인플레이션 레짐:
  COST_PUSH    — 유가+NG+구리 동반 상승 (원자재발 비용 인상)
  STAGFLATION  — 금 상승 + 구리 하락 + 금리 상승 (최악 시나리오)
  DEFLATIONARY — 유가+구리 동반 하락 (수요 위축)
  STABLE       — 변동 없음 (±0.5% 이내)

원칙:
  - nightwatch raw_indicators에서 원자재 데이터 수신
  - 23개 섹터별 비용 민감도 + 가격 전가력 정적 분류
  - 모듈 오류가 매매를 차단하는 일은 절대 없음
"""

import logging
from dataclasses import dataclass, field
from typing import Dict, List, Tuple

logger = logging.getLogger("BH.InflationChain")


# ═══════════════════════════════════════════
#  23섹터 비용 체인 분류
# ═══════════════════════════════════════════
#
# cost_sensitivity: 투입 비용이 원자재 가격에 얼마나 민감한지 (0~1)
# pricing_power: 비용 상승분을 소비자/고객에 전가할 수 있는 능력 (0~1)
# main_input: 주요 투입 원자재
# net_class: 인플레 시 순효과
#   - VICTIM: 비용↑ + 전가력↓ → 마진 압박
#   - SURVIVOR: 비용↑ + 전가력↑ → 중립~소폭 수혜
#   - BENEFICIARY: 비용↓ or 인플레 자체가 수혜 (금융 NIM 등)
#   - HEDGE: 원자재 가격 자체가 매출 (정유, 금속)

SECTOR_INFLATION_MAP: Dict[str, dict] = {
    # === HEDGE (원자재 가격 = 매출, 자연 헤지) ===
    "금속": {
        "cost_sensitivity": 0.7,
        "pricing_power": 0.8,
        "main_input": "철광석/니켈/알루미늄",
        "net_class": "HEDGE",
        "reason": "원자재 가격 상승 = 제품 가격 상승 (자연 헤지)",
    },

    # === BENEFICIARY (인플레이션 수혜) ===
    "금융": {
        "cost_sensitivity": 0.05,
        "pricing_power": 0.9,
        "main_input": "없음(자본)",
        "net_class": "BENEFICIARY",
        "reason": "인플레 → 금리 인상 → NIM 확대, 물적 투입 최소",
    },
    "보험": {
        "cost_sensitivity": 0.05,
        "pricing_power": 0.85,
        "main_input": "없음(자본)",
        "net_class": "BENEFICIARY",
        "reason": "인플레 → 보험료 인상 반영, 운용수익 ↑",
    },
    "IT서비스": {
        "cost_sensitivity": 0.1,
        "pricing_power": 0.8,
        "main_input": "인건비(원자재 무관)",
        "net_class": "BENEFICIARY",
        "reason": "물적 투입 최소, 높은 마진율, 가격 전가 용이",
    },
    "통신": {
        "cost_sensitivity": 0.1,
        "pricing_power": 0.7,
        "main_input": "장비(일회성)",
        "net_class": "BENEFICIARY",
        "reason": "구독 모델, 낮은 한계비용, 요금 인상 가능",
    },
    "제약": {
        "cost_sensitivity": 0.15,
        "pricing_power": 0.85,
        "main_input": "화학원료(소량)",
        "net_class": "BENEFICIARY",
        "reason": "비탄력적 수요, 특허 보호, 높은 가격 전가력",
    },

    # === SURVIVOR (비용↑ but 전가 가능) ===
    "화학": {
        "cost_sensitivity": 0.8,
        "pricing_power": 0.7,
        "main_input": "나프타/에틸렌/원유",
        "net_class": "SURVIVOR",
        "reason": "원재료비 높지만 석유화학 스프레드로 전가 가능",
    },
    "전기전자": {
        "cost_sensitivity": 0.4,
        "pricing_power": 0.6,
        "main_input": "실리콘/구리/희토류",
        "net_class": "SURVIVOR",
        "reason": "부품 원가 비중 중간, 기술 프리미엄으로 전가 가능",
    },
    "운송장비": {
        "cost_sensitivity": 0.5,
        "pricing_power": 0.6,
        "main_input": "철강/알루미늄/반도체",
        "net_class": "SURVIVOR",
        "reason": "원자재 비중 높으나 차량 가격 인상 가능",
    },
    "기계장비": {
        "cost_sensitivity": 0.5,
        "pricing_power": 0.5,
        "main_input": "철강/구리/특수강",
        "net_class": "SURVIVOR",
        "reason": "산업재 B2B, 계약 기반 전가 가능",
    },
    "종이목재": {
        "cost_sensitivity": 0.5,
        "pricing_power": 0.4,
        "main_input": "펄프/에너지",
        "net_class": "SURVIVOR",
        "reason": "비용 전가 일부 가능하나 경쟁 심함",
    },
    "유통": {
        "cost_sensitivity": 0.3,
        "pricing_power": 0.5,
        "main_input": "물류비/임차",
        "net_class": "SURVIVOR",
        "reason": "유통 마진으로 비용 전가, 소비 위축 리스크",
    },

    # === VICTIM (비용↑ + 전가력↓ → 마진 압박) ===
    "전기가스": {
        "cost_sensitivity": 0.9,
        "pricing_power": 0.2,
        "main_input": "LNG/석탄/원유",
        "net_class": "VICTIM",
        "reason": "연료비 직격 + 정부 규제로 요금 인상 제한 (한전)",
    },
    "운송창고": {
        "cost_sensitivity": 0.8,
        "pricing_power": 0.3,
        "main_input": "항공유/디젤/LNG",
        "net_class": "VICTIM",
        "reason": "연료비 비중 30%+, 운임 인상 한계 (경쟁)",
    },
    "건설": {
        "cost_sensitivity": 0.7,
        "pricing_power": 0.2,
        "main_input": "시멘트/철근/레미콘",
        "net_class": "VICTIM",
        "reason": "고정 계약 가격, 원자재 상승분 흡수 불가",
    },
    "음식료": {
        "cost_sensitivity": 0.6,
        "pricing_power": 0.3,
        "main_input": "곡물/원당/유지",
        "net_class": "VICTIM",
        "reason": "식자재 비용↑, 소비자 가격 저항 강함",
    },
    "섬유의류": {
        "cost_sensitivity": 0.5,
        "pricing_power": 0.25,
        "main_input": "면화/합성섬유/염료",
        "net_class": "VICTIM",
        "reason": "패션 경쟁 심화, 가격 인상 시 수요 급감",
    },

    # === DOMESTIC (영향 제한적) ===
    "일반서비스": {
        "cost_sensitivity": 0.2,
        "pricing_power": 0.5,
        "main_input": "인건비",
        "net_class": "SURVIVOR",
        "reason": "서비스업, 원자재 직접 영향 제한적",
    },
    "증권": {
        "cost_sensitivity": 0.05,
        "pricing_power": 0.7,
        "main_input": "없음(자본)",
        "net_class": "BENEFICIARY",
        "reason": "금리 인상 기대 → 거래 활성화, 물적 투입 없음",
    },
    "비금속": {
        "cost_sensitivity": 0.6,
        "pricing_power": 0.4,
        "main_input": "에너지/석회석",
        "net_class": "VICTIM",
        "reason": "시멘트/유리, 에너지 집약 산업, 전가 어려움",
    },
    "오락문화": {
        "cost_sensitivity": 0.15,
        "pricing_power": 0.6,
        "main_input": "인건비/IP",
        "net_class": "BENEFICIARY",
        "reason": "콘텐츠 가격 전가 용이, 물적 투입 적음",
    },
    "의료정밀": {
        "cost_sensitivity": 0.3,
        "pricing_power": 0.7,
        "main_input": "정밀부품/화학원료",
        "net_class": "SURVIVOR",
        "reason": "의료기기 비탄력적 수요, 기술 프리미엄",
    },
}


# ═══════════════════════════════════════════
#  데이터 구조
# ═══════════════════════════════════════════

@dataclass
class InflationRegime:
    """인플레이션 레짐 종합 평가"""
    regime: str = "STABLE"              # COST_PUSH / STAGFLATION / DEFLATIONARY / STABLE
    regime_score: float = 0.0           # -5.0(디플레) ~ +5.0(강한 인플레)

    # 원자재 입력 (전일비)
    oil_chg: float = 0.0
    ng_chg: float = 0.0
    copper_chg: float = 0.0
    gold_chg: float = 0.0
    tnx_change: float = 0.0            # 금리 변화 (bp)

    # 기준선 이탈도 (60MA 대비 %)
    oil_dev60: float = 0.0
    copper_dev60: float = 0.0
    gold_dev60: float = 0.0
    baseline_used: bool = False         # 기준선 데이터 사용 여부

    # 섹터 영향
    sector_adjustments: Dict[str, float] = field(default_factory=dict)
    victims: List[str] = field(default_factory=list)
    beneficiaries: List[str] = field(default_factory=list)
    detail: str = ""


# ═══════════════════════════════════════════
#  인플레이션 레짐 감지
# ═══════════════════════════════════════════

def _detect_regime(oil_chg: float, ng_chg: float, copper_chg: float,
                   gold_chg: float, tnx_bp: float,
                   oil_dev60: float = 0, copper_dev60: float = 0,
                   gold_dev60: float = 0) -> Tuple[str, float, str]:
    """
    원자재 가격 패턴으로 인플레이션 레짐 판단.

    2-Layer 판단:
      Layer 1 (주): 60MA 이탈도 — "3달간 유가가 +39% 올라왔다" (구조적 추세)
      Layer 2 (보조): 전일 변화율 — "오늘 +5% 올랐다" (단기 가속)

    기준선이 없으면 Layer 2만 사용 (기존 호환).

    Returns: (regime, regime_score, detail)
      regime_score: -5.0(디플레) ~ +5.0(강인플레)
    """
    has_baseline = (abs(oil_dev60) > 0 or abs(copper_dev60) > 0)

    # ── Layer 1: 기준선 기반 구조적 판단 ──
    if has_baseline:
        # STAGFLATION: 유가 고공 + 구리 약세 + 금리 상승 (경기 둔화 + 비용 인상)
        if oil_dev60 > 15 and copper_dev60 < -3 and tnx_bp > 0:
            severity = min(5.0, oil_dev60 * 0.08 + abs(copper_dev60) * 0.15)
            return ("STAGFLATION", severity,
                    f"유가60MA+{oil_dev60:.0f}%+구리60MA{copper_dev60:+.0f}%+금리↑")

        # COST_PUSH: 유가 60MA 크게 상회
        if oil_dev60 > 10:
            severity = min(5.0, oil_dev60 * 0.1)
            # 전일비로 가속 여부 확인
            if oil_chg > 1.0:
                severity = min(5.0, severity + 0.5)
            parts = [f"유가60MA+{oil_dev60:.0f}%"]
            if oil_chg > 1.0:
                parts.append(f"오늘+{oil_chg:.1f}%")
            return "COST_PUSH", severity, " ".join(parts)

        # DEFLATIONARY: 유가+구리 모두 60MA 크게 하회
        if oil_dev60 < -10 and copper_dev60 < -5:
            severity = max(-5.0, (oil_dev60 + copper_dev60) * 0.08)
            return ("DEFLATIONARY", severity,
                    f"유가60MA{oil_dev60:+.0f}%+구리60MA{copper_dev60:+.0f}%")

    # ── Layer 2: 전일비 기반 단기 판단 (기준선 없거나 Layer 1 미해당) ──
    industrial_avg = (oil_chg + ng_chg + copper_chg) / 3.0

    # STAGFLATION (단기): 금 상승 + 구리 하락 + 금리 상승
    if gold_chg > 1.0 and copper_chg < -0.5 and tnx_bp > 3:
        severity = min(5.0, gold_chg * 0.5 + abs(copper_chg) * 0.5 + tnx_bp * 0.1)
        # 기준선이 있으면 구조적 추세와 결합
        if has_baseline and oil_dev60 > 5:
            severity = min(5.0, severity + 0.5)
        return "STAGFLATION", severity, f"금↑{gold_chg:+.1f}%+구리↓{copper_chg:+.1f}%+금리↑{tnx_bp:+.0f}bp"

    # COST_PUSH (단기): 산업용 원자재 동반 상승
    rising_count = sum(1 for x in [oil_chg, ng_chg, copper_chg] if x > 0.5)
    if rising_count >= 2 and industrial_avg > 0.5:
        severity = min(5.0, industrial_avg * 1.5)
        # 기준선 가중: 이미 높은 수준에서 더 오르면 더 위험
        if has_baseline and oil_dev60 > 10:
            severity = min(5.0, severity + oil_dev60 * 0.03)
        rising = []
        if oil_chg > 0.5:
            rising.append(f"유가{oil_chg:+.1f}%")
        if ng_chg > 0.5:
            rising.append(f"NG{ng_chg:+.1f}%")
        if copper_chg > 0.5:
            rising.append(f"구리{copper_chg:+.1f}%")
        if has_baseline and oil_dev60 > 10:
            rising.append(f"(60MA+{oil_dev60:.0f}%)")
        return "COST_PUSH", severity, "+".join(rising)

    # DEFLATIONARY (단기): 산업용 원자재 동반 하락
    falling_count = sum(1 for x in [oil_chg, ng_chg, copper_chg] if x < -0.5)
    if falling_count >= 2 and industrial_avg < -0.5:
        severity = max(-5.0, industrial_avg * 1.5)
        falling = []
        if oil_chg < -0.5:
            falling.append(f"유가{oil_chg:+.1f}%")
        if ng_chg < -0.5:
            falling.append(f"NG{ng_chg:+.1f}%")
        if copper_chg < -0.5:
            falling.append(f"구리{copper_chg:+.1f}%")
        return "DEFLATIONARY", severity, "+".join(falling)

    # === STABLE ===
    return "STABLE", 0.0, "원자재안정"


def _calc_sector_impact(regime: str, regime_score: float) -> Dict[str, float]:
    """
    레짐 + 강도 → 23섹터 보정값 계산.

    COST_PUSH/STAGFLATION: VICTIM 페널티, BENEFICIARY/HEDGE 수혜
    DEFLATIONARY: VICTIM 수혜(비용↓), BENEFICIARY 약간 페널티(금리↓)
    STABLE: 전부 0
    """
    if abs(regime_score) < 0.3:
        return {}

    adjustments = {}
    intensity = abs(regime_score) / 5.0  # 0~1 정규화

    for sector, info in SECTOR_INFLATION_MAP.items():
        net_class = info["net_class"]
        cost_sens = info["cost_sensitivity"]
        pricing_pw = info["pricing_power"]

        if regime in ("COST_PUSH", "STAGFLATION"):
            # 인플레: 비용 민감도 높고 전가력 낮으면 피해
            if net_class == "VICTIM":
                adj = -cost_sens * (1 - pricing_pw) * intensity * 5.0
            elif net_class == "HEDGE":
                adj = cost_sens * pricing_pw * intensity * 3.0
            elif net_class == "BENEFICIARY":
                adj = pricing_pw * intensity * 2.0
            else:  # SURVIVOR
                adj = (pricing_pw - cost_sens) * intensity * 2.0

            # STAGFLATION은 전체적으로 더 부정적
            if regime == "STAGFLATION":
                adj -= intensity * 1.0

        elif regime == "DEFLATIONARY":
            # 디플레: VICTIM 수혜(비용↓), BENEFICIARY 약간 피해(금리↓)
            if net_class == "VICTIM":
                adj = cost_sens * (1 - pricing_pw) * intensity * 3.0
            elif net_class == "HEDGE":
                adj = -cost_sens * intensity * 2.0  # 원자재 하락 = 매출 하락
            elif net_class == "BENEFICIARY":
                adj = -pricing_pw * intensity * 1.0  # 금리 하락 → NIM 축소
            else:
                adj = (cost_sens - pricing_pw) * intensity * 1.5
        else:
            adj = 0.0

        # 클램프 ±5.0
        adj = max(-5.0, min(5.0, round(adj, 1)))
        if abs(adj) >= 0.3:
            adjustments[sector] = adj

    return adjustments


# ═══════════════════════════════════════════
#  공개 API
# ═══════════════════════════════════════════

def analyze_inflation_chain(raw_indicators: dict) -> InflationRegime:
    """
    nightwatch raw_indicators → 인플레이션 레짐 + 섹터 비용 체인 분석.

    Args:
        raw_indicators: nightwatch의 원자재/금리 데이터
            CL.change_pct, NG.change_pct, HG.change_pct, GOLD.change_pct, TNX.change_pct

    Returns:
        InflationRegime: 레짐 + 섹터별 보정값
    """
    result = InflationRegime()

    try:
        oil = raw_indicators.get("CL", {})
        ng = raw_indicators.get("NG", {})
        copper = raw_indicators.get("HG", {})
        gold = raw_indicators.get("GOLD", {})
        tnx = raw_indicators.get("TNX", {})

        result.oil_chg = (oil.get("change_pct") or 0)
        result.ng_chg = (ng.get("change_pct") or 0)
        result.copper_chg = (copper.get("change_pct") or 0)
        result.gold_chg = (gold.get("change_pct") or 0)

        # TNX change → bp (change_pct는 수익률 변화%이므로 가격 변화가 아님)
        # nightwatch에서 change_pct는 yield 값의 전일대비 변화
        tnx_chg_pct = (tnx.get("change_pct") or 0)
        tnx_value = (tnx.get("value") or tnx.get("close") or 0)
        # change_pct가 yield 변화율(%)이면 bp로 환산
        # 예: TNX 4.4%, change_pct=+0.5% → 실제 4.4 * 0.005 = 0.022%p = 2.2bp
        if tnx_value > 0 and abs(tnx_chg_pct) > 0:
            result.tnx_change = tnx_value * tnx_chg_pct / 100.0 * 100  # bp
        else:
            result.tnx_change = 0

        # 기준선 이탈도 로드 (있으면 2-Layer 판단)
        try:
            from data.macro_baseline import load_cached_baselines
            bl = load_cached_baselines()
            if bl and bl.oil.current > 0:
                result.oil_dev60 = bl.oil.dev60_pct
                result.copper_dev60 = bl.copper.dev60_pct
                result.gold_dev60 = bl.gold.dev60_pct
                result.baseline_used = True
        except Exception:
            pass

        # 레짐 감지 (기준선 있으면 2-Layer)
        result.regime, result.regime_score, result.detail = _detect_regime(
            result.oil_chg, result.ng_chg, result.copper_chg,
            result.gold_chg, result.tnx_change,
            oil_dev60=result.oil_dev60,
            copper_dev60=result.copper_dev60,
            gold_dev60=result.gold_dev60,
        )

        # 섹터 영향 계산
        result.sector_adjustments = _calc_sector_impact(result.regime, result.regime_score)

        # 피해/수혜 섹터 분류
        for sector, adj in sorted(result.sector_adjustments.items(), key=lambda x: x[1]):
            if adj <= -1.0:
                result.victims.append(sector)
            elif adj >= 1.0:
                result.beneficiaries.append(sector)

        if result.regime != "STABLE":
            logger.info(
                f"[Inflation] {result.regime} (score={result.regime_score:+.1f}): "
                f"{result.detail} | 피해={result.victims} 수혜={result.beneficiaries}"
            )

    except Exception as e:
        logger.warning(f"[Inflation] 분석 실패 (무시): {e}")
        result.regime = "STABLE"
        result.regime_score = 0.0

    return result


def get_inflation_brain_adjustment(raw_indicators: dict) -> Tuple[float, str]:
    """
    BRAIN Phase 6 Factor 13용: 인플레이션 레짐 → 전체 시장 보정.

    Returns:
        (adjustment: ±3.0, detail: str)
        - COST_PUSH → -1.0 ~ -3.0 (비용 부담 → 기업이익 압박)
        - STAGFLATION → -2.0 ~ -3.0 (최악)
        - DEFLATIONARY → -0.5 ~ -1.5 (수요 위축 우려)
        - STABLE → 0.0
    """
    try:
        regime = analyze_inflation_chain(raw_indicators)

        if regime.regime == "STABLE":
            return 0.0, ""

        if regime.regime == "COST_PUSH":
            # 비용 인상 → 기업 마진 압박 (강도에 비례)
            adj = max(-3.0, -regime.regime_score * 0.6)
            return adj, f"비용인상({regime.detail})"

        if regime.regime == "STAGFLATION":
            # 최악 시나리오 → 강한 방어
            adj = max(-3.0, -regime.regime_score * 0.8)
            return adj, f"스태그({regime.detail})"

        if regime.regime == "DEFLATIONARY":
            # 디플레 → 수요 위축 (약한 부정)
            adj = max(-1.5, regime.regime_score * 0.3)  # score가 음수이므로
            return adj, f"디플레({regime.detail})"

    except Exception as e:
        logger.debug(f"[Inflation] BRAIN 보정 실패: {e}")

    return 0.0, ""


def get_inflation_sector_boosts(raw_indicators: dict) -> Dict[str, float]:
    """
    Morning Recommendation용: 섹터별 인플레이션 비용 체인 보정값.

    Returns:
        {섹터명: 보정값(±5.0)} — 비어있으면 STABLE
    """
    try:
        regime = analyze_inflation_chain(raw_indicators)
        return regime.sector_adjustments
    except Exception:
        return {}


def get_inflation_class(sector: str) -> str:
    """섹터의 인플레이션 순효과 분류 반환."""
    info = SECTOR_INFLATION_MAP.get(sector, {})
    return info.get("net_class", "SURVIVOR")
