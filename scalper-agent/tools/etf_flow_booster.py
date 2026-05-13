"""
ETF 자금흐름 부스터 (ETF Flow Sector Booster)
===============================================
정보봇이 수집한 ETF 자금흐름 데이터를 분석하여
자금 유입 섹터 종목에 가점, 유출 섹터 종목에 감점을 부여합니다.

데이터 소스:
  D:/Global_Stock_Overview_Scripter_정보봇/data/etf_flow/etf_flow_{YYYYMMDD}.json

핵심 필드:
  - sector_summary: 섹터별 AUM 변화 (원 단위)
  - top_inflow/outflow: 유입/유출 TOP ETF
  - volume_surges: 거래량 급증 ETF (관심 폭발)
  - alerts: 섹터 유입 알림

활용: 섹터 자금 유입 → 해당 섹터 종목 가점 (로테이션 추종)
"""

import json
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Tuple

logger = logging.getLogger("BH.ETFFlowBooster")

# ═══ 경로 상수 ═══
ETF_FLOW_DIR = Path("D:/Global_Stock_Overview_Scripter_정보봇/data/etf_flow")

# ═══ 스코어 상수 ═══
# 섹터 자금 유입량 기준 (억원)
INFLOW_TIERS = [
    (500, 12),    # ≥500억 유입 → +12
    (200, 8),     # ≥200억 유입 → +8
    (50,  5),     # ≥50억 유입  → +5
]
OUTFLOW_TIERS = [
    (200, -8),    # ≥200억 유출 → -8
    (50,  -5),    # ≥50억 유출  → -5
]

# 거래량 급증 보너스
VOLUME_SURGE_BONUS = 3  # 거래량 3배+ 급증 섹터

# ═══ ETF 섹터 → SECTOR_MAP 매핑 (확장) ═══
# ETF flow의 섹터명 → market_health.SECTOR_MAP의 섹터명
ETF_TO_SECTOR = {
    "반도체": "반도체",
    "AI반도체": "반도체",
    "바이오": "바이오",
    "방산": "방산",
    "IT": "소프트웨어",
    "SW/IT": "소프트웨어",
    "금융": "증권",
    "원자력": "전력에너지",
    "화장품": "엔터",      # 소비재 대리
    "소비재": "엔터",
}

# 확장 섹터 → 직접 종목 매핑 (SECTOR_MAP에 없는 섹터)
EXTRA_SECTOR_CODES: Dict[str, List[str]] = {
    "자동차": ["005380", "000270", "012330", "018880"],  # 현대차 기아 현대모비스 한온시스템
    "2차전지": ["373220", "006400", "051910", "247540"],  # LG에너지 삼성SDI LG화학 에코프로비엠
    "조선": ["009540", "010140", "042660", "329180"],  # 한국조선해양 삼성중공업 한화오션 HD현대중공업
    "건설": ["000720", "047040", "006360", "028260"],  # 현대건설 대우건설 GS건설 삼성물산
    "철강": ["005490", "004020", "001230"],              # POSCO 현대제철 동국S&S
    "운송": ["003490", "180640", "089590"],              # 대한항공 한진칼 제주항공
    "여행레저": ["039130", "035250", "272210"],           # 하나투어 강원랜드 한화갤러리아
}


@dataclass
class SectorFlow:
    """섹터별 자금흐름"""
    sector: str
    aum_change_억: float = 0.0
    score: float = 0.0
    volume_surge: bool = False
    etf_count: int = 0


# ═══ 캐시 ═══
_etf_cache: Dict[str, float] = {}  # {stock_code: score}
_sector_cache: Dict[str, SectorFlow] = {}
_cache_ts: float = 0.0
_CACHE_TTL = 300


def _load_latest_etf_flow() -> dict:
    """최근 2일 ETF flow 파일 로드."""
    today = datetime.now()
    for delta in range(3):
        dt = today - timedelta(days=delta)
        date_str = dt.strftime("%Y%m%d")
        path = ETF_FLOW_DIR / f"etf_flow_{date_str}.json"
        if path.exists():
            try:
                return json.loads(path.read_text(encoding="utf-8"))
            except Exception as e:
                logger.warning(f"[ETF흐름] {path.name} 로드 실패: {e}")
    return {}


def _get_sector_codes(sector_name: str) -> List[str]:
    """섹터명 → 종목 코드 리스트."""
    # 1) EXTRA_SECTOR_CODES에서 직접 매핑
    if sector_name in EXTRA_SECTOR_CODES:
        return EXTRA_SECTOR_CODES[sector_name]

    # 2) ETF_TO_SECTOR → SECTOR_MAP 매핑
    mapped = ETF_TO_SECTOR.get(sector_name, sector_name)
    try:
        from data.market_health import SECTOR_MAP
        return list(SECTOR_MAP.get(mapped, []))
    except Exception:
        return []


def load_etf_flow_booster() -> Tuple[Dict[str, float], Dict[str, SectorFlow]]:
    """ETF 자금흐름 → 종목별 스코어 맵 + 섹터 흐름.

    Returns:
        (stock_score_map, sector_flow_map)
    """
    global _etf_cache, _sector_cache, _cache_ts

    if _etf_cache and (time.time() - _cache_ts) < _CACHE_TTL:
        return _etf_cache, _sector_cache

    raw = _load_latest_etf_flow()
    if not raw:
        return {}, {}

    stock_scores: Dict[str, float] = {}
    sector_flows: Dict[str, SectorFlow] = {}

    # 섹터별 AUM 변화 분석
    for sector, data in raw.get("sector_summary", {}).items():
        aum_chg = data.get("aum_change", 0) / 1e8  # 원 → 억원
        etf_count = len(data.get("etfs", []))

        sf = SectorFlow(
            sector=sector,
            aum_change_억=aum_chg,
            etf_count=etf_count,
        )

        # 스코어 계산
        abs_aum = abs(aum_chg)
        if aum_chg > 0:
            for threshold, score in INFLOW_TIERS:
                if abs_aum >= threshold:
                    sf.score = score
                    break
        elif aum_chg < 0:
            for threshold, score in OUTFLOW_TIERS:
                if abs_aum >= threshold:
                    sf.score = score
                    break

        sector_flows[sector] = sf

    # 거래량 급증 ETF → 해당 섹터 보너스
    for surge in raw.get("volume_surges", []):
        # surge에서 섹터 식별
        for sector, data in raw.get("sector_summary", {}).items():
            for etf in data.get("etfs", []):
                if etf.get("name") == surge.get("name"):
                    if sector in sector_flows:
                        sector_flows[sector].volume_surge = True
                        sector_flows[sector].score += VOLUME_SURGE_BONUS

    # 섹터 → 종목 스코어 전파
    for sector, sf in sector_flows.items():
        if sf.score == 0:
            continue
        codes = _get_sector_codes(sector)
        for code in codes:
            if code not in stock_scores:
                stock_scores[code] = 0.0
            stock_scores[code] += sf.score

    active = {s: sf for s, sf in sector_flows.items() if sf.score != 0}
    logger.info(
        f"[ETF흐름] {len(active)}섹터 활성, {len(stock_scores)}종목 반영 "
        f"(date={raw.get('date')})"
    )

    _etf_cache = stock_scores
    _sector_cache = sector_flows
    _cache_ts = time.time()

    return stock_scores, sector_flows


def get_etf_flow_score(code: str) -> Tuple[float, str]:
    """morning_recommendation 연동."""
    scores, sectors = load_etf_flow_booster()
    if code in scores:
        sc = scores[code]
        # 어느 섹터 때문인지 찾기
        for sector, sf in sectors.items():
            if sf.score != 0:
                codes = _get_sector_codes(sector)
                if code in codes:
                    surge = "+급증" if sf.volume_surge else ""
                    return sc, f"ETF_{sector}{surge}({sf.aum_change_억:+,.0f}억)"
        return sc, "ETF_FLOW"
    return 0.0, ""


def format_etf_flow_alert() -> str:
    """텔레그램 ETF 자금흐름 알림."""
    _, sectors = load_etf_flow_booster()
    if not sectors:
        return ""

    now = datetime.now()
    lines = [
        "---",
        "  ETF 섹터 자금흐름",
        f"  {now.strftime('%m/%d(%a) %H:%M')}",
        "---",
    ]

    inflow = sorted(
        [sf for sf in sectors.values() if sf.aum_change_억 > 0],
        key=lambda x: x.aum_change_억, reverse=True,
    )
    outflow = sorted(
        [sf for sf in sectors.values() if sf.aum_change_억 < 0],
        key=lambda x: x.aum_change_억,
    )

    if inflow:
        lines.append("")
        lines.append("[유입] 자금 유입 섹터")
        for sf in inflow[:8]:
            surge = " *급증" if sf.volume_surge else ""
            lines.append(
                f"  {sf.sector}: +{sf.aum_change_억:,.0f}억{surge} "
                f"(+{sf.score:.0f}점)"
            )

    if outflow:
        lines.append("")
        lines.append("[유출] 자금 유출 섹터")
        for sf in outflow[:5]:
            lines.append(
                f"  {sf.sector}: {sf.aum_change_억:,.0f}억 "
                f"({sf.score:.0f}점)"
            )

    return "\n".join(lines)


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    sys.stdout.reconfigure(encoding="utf-8")
    scores, sectors = load_etf_flow_booster()
    print(f"종목 스코어: {len(scores)}건")
    alert = format_etf_flow_alert()
    print(alert or "(데이터 없음)")
