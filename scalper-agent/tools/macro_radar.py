"""
매크로 레이더 (Macro Risk Radar)
=================================
정보봇의 daily_intelligence에서 글로벌 시나리오 키워드 + 섹터 센티먼트 +
breaking_alerts를 분석하여 매크로 리스크 모드를 산출합니다.

데이터 소스:
  1) daily_intelligence.json → scenario_keywords, sector_sentiment
  2) breaking_alerts.json → alerts[].sectors_impact

출력:
  - 매크로 모드: RISK_OFF / NEUTRAL / RISK_ON
  - 시나리오별 섹터 영향 맵 (가점/감점)
  - 섹터 센티먼트 극단치 알림

스케줄: 08:10 KST (모든 알림 중 가장 먼저)
"""

import json
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Tuple

logger = logging.getLogger("BH.MacroRadar")

# ═══ 경로 상수 (OS 자동 감지) ═══
from utils.jgis_path import jgis_intel_path, jgis_breaking_path

INTEL_JSON = jgis_intel_path()
BREAKING_JSON = jgis_breaking_path()

# ═══ 시나리오 → 섹터 영향 매핑 ═══
# count가 높으면 해당 시나리오가 뉴스에서 빈번 → 영향력 높음
SCENARIO_SECTOR_IMPACT: Dict[str, Dict[str, float]] = {
    "WAR_MIDDLE_EAST": {
        "방산": +8, "정유": +5,         # 수혜
        "반도체": -3, "엔터": -3,       # 리스크
    },
    "WAR_UKRAINE": {
        "방산": +5,
        "반도체": -2,
    },
    "OIL_SPIKE": {
        "정유": +8, "전력에너지": +3,   # 수혜
        "반도체": -3, "바이오": -2,     # 비용 증가
    },
    "FED_RATE_CUT": {
        "반도체": +5, "바이오": +5, "소프트웨어": +3,  # 성장주 수혜
        "증권": +3,
    },
    "FED_RATE_HIKE": {
        "반도체": -5, "바이오": -5,     # 성장주 타격
        "증권": -3,
    },
    "SEMICONDUCTOR_CYCLE": {
        "반도체": +5,
    },
    "CHINA_RECOVERY": {
        "반도체": +3, "바이오": +2,
    },
    "CHINA_SLOWDOWN": {
        "반도체": -3, "정유": -2,
    },
}

# 시나리오 활성 임계값 (뉴스 카운트)
SCENARIO_ACTIVE_THRESHOLD = 3  # count ≥ 3이면 활성

# 섹터 센티먼트 임계값
SENTIMENT_BEARISH = 30    # 이하 → 약세
SENTIMENT_BULLISH = 70    # 이상 → 강세

# 센티먼트 기반 스코어
BEARISH_PENALTY = -5
BULLISH_BONUS = 5

# 매크로 모드 임계값
RISK_OFF_SCENARIOS = {"WAR_MIDDLE_EAST", "WAR_UKRAINE", "OIL_SPIKE", "FED_RATE_HIKE"}
RISK_ON_SCENARIOS = {"FED_RATE_CUT", "SEMICONDUCTOR_CYCLE", "CHINA_RECOVERY"}


@dataclass
class MacroState:
    """매크로 레이더 상태"""
    mode: str = "NEUTRAL"           # RISK_OFF / NEUTRAL / RISK_ON
    active_scenarios: List[str] = field(default_factory=list)
    sector_scores: Dict[str, float] = field(default_factory=dict)  # 섹터→스코어
    sentiment_alerts: List[str] = field(default_factory=list)
    breaking_risk_sectors: List[str] = field(default_factory=list)
    risk_score: float = 0.0         # 종합 리스크 (-100~+100)


# ═══ 캐시 ═══
_macro_cache: MacroState = None
_stock_cache: Dict[str, float] = {}
_cache_ts: float = 0.0
_CACHE_TTL = 300


def _is_fresh(date_str: str) -> bool:
    try:
        clean = date_str.replace("-", "")[:8]
        d = datetime.strptime(clean, "%Y%m%d").date()
        today = datetime.now().date()
        return d >= today - timedelta(days=2)
    except Exception:
        return False


def _load_intel() -> dict:
    if not INTEL_JSON.exists():
        return {}
    try:
        raw = json.loads(INTEL_JSON.read_text(encoding="utf-8"))
        if _is_fresh(raw.get("date", "")):
            return raw
    except Exception:
        pass
    return {}


def _load_breaking() -> list:
    if not BREAKING_JSON.exists():
        return []
    try:
        raw = json.loads(BREAKING_JSON.read_text(encoding="utf-8"))
        return raw.get("alerts", [])
    except Exception:
        return []


def scan_macro_radar() -> MacroState:
    """매크로 레이더 — 시나리오 + 센티먼트 + 속보 통합 분석."""
    global _macro_cache, _stock_cache, _cache_ts

    if _macro_cache and (time.time() - _cache_ts) < _CACHE_TTL:
        return _macro_cache

    state = MacroState()

    intel = _load_intel()
    if not intel:
        _macro_cache = state
        _cache_ts = time.time()
        return state

    # ── 시나리오 키워드 분석 ──
    risk_off_count = 0
    risk_on_count = 0

    for scenario, info in intel.get("scenario_keywords", {}).items():
        count = info.get("count", 0) if isinstance(info, dict) else 0
        if count < SCENARIO_ACTIVE_THRESHOLD:
            continue

        state.active_scenarios.append(f"{scenario}({count})")

        # 섹터 영향 계산 (count 비례 가중)
        weight = min(count / 10, 2.0)  # 최대 2배
        impacts = SCENARIO_SECTOR_IMPACT.get(scenario, {})
        for sector, base_score in impacts.items():
            if sector not in state.sector_scores:
                state.sector_scores[sector] = 0.0
            state.sector_scores[sector] += base_score * weight

        # 모드 판단
        if scenario in RISK_OFF_SCENARIOS:
            risk_off_count += count
        elif scenario in RISK_ON_SCENARIOS:
            risk_on_count += count

    # ── 섹터 센티먼트 분석 ──
    # sector_sentiment의 값이 dict인 경우와 int/float인 경우 모두 처리
    for sector_en, info in intel.get("sector_sentiment", {}).items():
        score = info if isinstance(info, (int, float)) else info.get("score", 50)

        if score <= SENTIMENT_BEARISH:
            state.sentiment_alerts.append(f"{sector_en}({score})")
            # 영문→한글 매핑 시도
            from tools.premarket_risk_scanner import EN_TO_KR_SECTOR
            kr = EN_TO_KR_SECTOR.get(sector_en, "")
            if kr and kr in state.sector_scores:
                state.sector_scores[kr] += BEARISH_PENALTY
            elif kr:
                state.sector_scores[kr] = BEARISH_PENALTY

        elif score >= SENTIMENT_BULLISH:
            from tools.premarket_risk_scanner import EN_TO_KR_SECTOR
            kr = EN_TO_KR_SECTOR.get(sector_en, "")
            if kr:
                state.sector_scores[kr] = state.sector_scores.get(kr, 0) + BULLISH_BONUS

    # ── 속보 "-영향" 섹터 ──
    for alert in _load_breaking():
        ts = alert.get("timestamp", "")
        if ts and not _is_fresh(ts[:10]):
            continue
        for sector, impact in alert.get("sectors_impact", {}).items():
            if "-영향" in str(impact):
                state.breaking_risk_sectors.append(sector)

    # ── 매크로 모드 결정 ──
    state.risk_score = risk_on_count - risk_off_count
    if risk_off_count >= 15 and risk_off_count > risk_on_count * 2:
        state.mode = "RISK_OFF"
    elif risk_on_count >= 10 and risk_on_count > risk_off_count * 2:
        state.mode = "RISK_ON"
    else:
        state.mode = "NEUTRAL"

    logger.info(
        f"[매크로] mode={state.mode} scenarios={len(state.active_scenarios)} "
        f"sectors={len(state.sector_scores)} risk={state.risk_score:+.0f}"
    )

    # 종목별 스코어 맵 빌드
    _stock_cache = _build_stock_map(state)
    _macro_cache = state
    _cache_ts = time.time()

    return state


def _build_stock_map(state: MacroState) -> Dict[str, float]:
    """섹터 스코어 → 종목별 스코어."""
    result: Dict[str, float] = {}
    try:
        from data.market_health import SECTOR_MAP
        for sector, sc in state.sector_scores.items():
            codes = SECTOR_MAP.get(sector, [])
            for code in codes:
                result[code] = result.get(code, 0) + sc
    except Exception:
        pass
    return result


def get_macro_score(code: str) -> Tuple[float, str]:
    """morning_recommendation 연동."""
    state = scan_macro_radar()
    if code in _stock_cache:
        sc = _stock_cache[code]
        scenarios = ", ".join(state.active_scenarios[:2]) if state.active_scenarios else ""
        return sc, f"MACRO_{state.mode}({scenarios})"
    return 0.0, ""


def get_macro_mode() -> str:
    """매크로 모드 — RISK_OFF / NEUTRAL / RISK_ON."""
    return scan_macro_radar().mode


def format_macro_radar_alert() -> str:
    """08:10 매크로 레이더 알림."""
    state = scan_macro_radar()

    now = datetime.now()
    lines = [
        "---",
        "  매크로 리스크 레이더",
        f"  {now.strftime('%m/%d(%a) %H:%M')}",
        "---",
    ]

    # 매크로 모드
    mode_emoji = {"RISK_OFF": "[!!!]", "RISK_ON": "[OK]", "NEUTRAL": "[--]"}
    lines.append("")
    lines.append(f"{mode_emoji.get(state.mode, '')} 매크로 모드: {state.mode}")
    lines.append(f"  리스크 지수: {state.risk_score:+.0f}")

    # 활성 시나리오
    if state.active_scenarios:
        lines.append("")
        lines.append("[시나리오]")
        for s in state.active_scenarios:
            lines.append(f"  {s}")

    # 섹터 영향
    if state.sector_scores:
        lines.append("")
        lines.append("[섹터 영향]")
        for sector, sc in sorted(state.sector_scores.items(), key=lambda x: x[1], reverse=True):
            if abs(sc) >= 3:
                sign = "+" if sc > 0 else ""
                lines.append(f"  {sector}: {sign}{sc:.0f}")

    # 센티먼트 경고
    if state.sentiment_alerts:
        lines.append("")
        lines.append(f"[센티먼트 약세] {len(state.sentiment_alerts)}섹터")
        for s in state.sentiment_alerts[:5]:
            lines.append(f"  {s}")

    # 속보 리스크
    if state.breaking_risk_sectors:
        lines.append("")
        lines.append(f"[속보 리스크] {', '.join(state.breaking_risk_sectors[:5])}")

    return "\n".join(lines)


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    sys.stdout.reconfigure(encoding="utf-8")
    state = scan_macro_radar()
    print(f"모드: {state.mode}, 시나리오: {state.active_scenarios}")
    print(format_macro_radar_alert())
