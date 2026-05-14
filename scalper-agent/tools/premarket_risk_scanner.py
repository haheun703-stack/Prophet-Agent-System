"""
장전 종합 리스크 스캔 (Pre-market Comprehensive Risk Scanner)
=============================================================
RISK-01: DART 공시 블랙리스트 (block_deal → 유상증자/CB/합병/상폐)
RISK-02: 섹터 센티먼트 약세 (daily_intelligence → sector_sentiment < 30)
RISK-03: 뉴스 개별종목 리스크 (daily_intelligence → NEGATIVE headline tickers)
RISK-04: 속보 섹터 리스크 (breaking_alerts → sectors_impact "-영향")

데이터 소스 (정보봇 제공):
  1) D:/Global_Stock_Overview_Scripter_정보봇/data/block_deal/block_{YYYYMMDD}.json
  2) D:/shared-bot-data/jgis_to_quant/daily_intelligence.json
  3) D:/shared-bot-data/jgis_to_quant/breaking_alerts.json

스케줄: 08:20 KST (daily_intelligence 08:00 생성 후)
"""

import json
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Tuple

logger = logging.getLogger("BH.PremarketRisk")

# ═══ 경로 상수 ═══
BLOCK_DEAL_DIR = Path(
    "D:/Global_Stock_Overview_Scripter_정보봇/data/block_deal"
)
from utils.jgis_path import jgis_intel_path, jgis_breaking_path
INTEL_JSON = jgis_intel_path()
BREAKING_JSON = jgis_breaking_path()
STORE_DIR = Path(__file__).resolve().parent.parent / "data_store"
CACHE_FILE = STORE_DIR / "premarket_blacklist.json"

# ═══ 공시 패턴별 페널티 ═══
DISCLOSURE_PENALTIES: Dict[str, float] = {
    "상장폐지":                     -999.0,
    "유상증자결정":                   -15.0,
    "최대주주변경":                   -10.0,
    "회사합병결정":                   -10.0,
    "전환가액의조정":                  -8.0,
    "전환사채권발행결정":              -8.0,
    "신주인수권부사채권발행결정":       -8.0,
    "전환사채매수선택권행사":           -5.0,
    "주식교환":                       -5.0,
}

HARD_EXCLUDE_THRESHOLD = -999.0

# ═══ 센티먼트/뉴스 페널티 ═══
WEAK_SECTOR_THRESHOLD = 30
WEAK_SECTOR_PENALTY = -5.0
NEGATIVE_HEADLINE_PENALTY = -10.0
BREAKING_NEGATIVE_PENALTY = -5.0

# ═══ 영문→한글 섹터 매핑 ═══
EN_TO_KR_SECTOR: Dict[str, str] = {
    "Semiconductors": "반도체",
    "Memory Chips": "반도체",
    "Technology": "소프트웨어",
    "Healthcare": "바이오",
    "Pharma": "바이오",
    "Defense": "방산",
    "Energy": "정유",
    "Oil": "정유",
    "Oil & Gas": "정유",
    "Entertainment": "엔터",
    "Securities": "증권",
    "Financials": "증권",
    "Power": "전력에너지",
}


@dataclass
class PremarketRisk:
    """종목별 장전 리스크 분석 결과"""
    code: str
    name: str = ""
    penalty: float = 0.0
    hard_exclude: bool = False
    tag: str = ""
    reasons: list = field(default_factory=list)
    disclosure_penalties: list = field(default_factory=list)
    sector_penalty: float = 0.0
    headline_penalty: float = 0.0
    breaking_penalty: float = 0.0


# ═══ 캐시 (5분 TTL) ═══
_risk_cache: Dict[str, PremarketRisk] = {}
_risk_cache_ts: float = 0.0
_CACHE_TTL = 300


def _is_fresh_date(date_str: str) -> bool:
    """오늘 또는 어제 데이터만 유효."""
    try:
        fmt = "%Y-%m-%d" if "-" in date_str else "%Y%m%d"
        data_date = datetime.strptime(date_str, fmt).date()
        today = datetime.now().date()
        yesterday = today - timedelta(days=1)
        return data_date in (today, yesterday)
    except Exception:
        return False


def _get_codes_for_sector(sector_en: str) -> List[str]:
    """영문 섹터명 → 해당 섹터 종목 코드 리스트."""
    kr = EN_TO_KR_SECTOR.get(sector_en, "")
    if not kr:
        return []
    try:
        from data.market_health import SECTOR_MAP
        return list(SECTOR_MAP.get(kr, []))
    except Exception:
        return []


# ═══════════════════════════════════════════════════
#  RISK-01: DART 공시 블랙리스트
# ═══════════════════════════════════════════════════

def _load_block_deal() -> Dict[str, PremarketRisk]:
    """DART 공시에서 종목별 리스크 추출 (2일 룩백)."""
    results: Dict[str, PremarketRisk] = {}

    today_str = datetime.now().strftime("%Y%m%d")
    yesterday_str = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")

    for date_str in [today_str, yesterday_str]:
        path = BLOCK_DEAL_DIR / f"block_{date_str}.json"
        if not path.exists():
            continue
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except Exception as e:
            logger.warning(f"[장전리스크] block_{date_str} 로드 실패: {e}")
            continue

        file_date = raw.get("date", "")
        if file_date and not _is_fresh_date(file_date):
            continue

        all_disc = (
            raw.get("disclosures", [])
            + raw.get("key_disclosures", [])
            + raw.get("watchlist_hits", [])
        )

        seen = set()
        for disc in all_disc:
            stock_code = disc.get("stock_code", "")
            if not stock_code or len(stock_code) != 6 or not stock_code.isdigit():
                continue

            corp_name = disc.get("corp_name", "")
            report_nm = disc.get("report_nm", "")

            # 중복 제거 (같은 종목+같은 공시)
            key = f"{stock_code}:{report_nm[:20]}"
            if key in seen:
                continue
            seen.add(key)

            # 페널티 패턴 매칭 (최악 1건만)
            matched_pen = 0.0
            matched_tag = ""
            for pattern, pen in DISCLOSURE_PENALTIES.items():
                if pattern in report_nm and pen < matched_pen:
                    matched_pen = pen
                    matched_tag = pattern

            if matched_pen == 0.0:
                continue

            if stock_code not in results:
                results[stock_code] = PremarketRisk(
                    code=stock_code, name=corp_name
                )

            risk = results[stock_code]
            risk.disclosure_penalties.append((matched_tag, matched_pen))
            risk.reasons.append(f"DART:{matched_tag}")

            if matched_pen <= HARD_EXCLUDE_THRESHOLD:
                risk.hard_exclude = True
                risk.tag = f"DART_{matched_tag}"

    # 합산
    for risk in results.values():
        risk.penalty = sum(p for _, p in risk.disclosure_penalties)
        if risk.hard_exclude:
            risk.penalty = HARD_EXCLUDE_THRESHOLD
        if not risk.tag and risk.disclosure_penalties:
            worst = min(risk.disclosure_penalties, key=lambda x: x[1])
            risk.tag = f"DART_{worst[0]}"

    return results


# ═══════════════════════════════════════════════════
#  RISK-02/03: daily_intelligence
# ═══════════════════════════════════════════════════

def _load_daily_intelligence() -> Tuple[Dict[str, float], List[dict]]:
    """약세 섹터 + NEGATIVE 헤드라인 종목 추출."""
    weak_sectors: Dict[str, float] = {}
    neg_tickers: List[dict] = []

    if not INTEL_JSON.exists():
        return weak_sectors, neg_tickers

    try:
        raw = json.loads(INTEL_JSON.read_text(encoding="utf-8"))
    except Exception as e:
        logger.warning(f"[장전리스크] daily_intelligence 로드 실패: {e}")
        return weak_sectors, neg_tickers

    if not _is_fresh_date(raw.get("date", "")):
        return weak_sectors, neg_tickers

    # 약세 섹터
    for sector, info in raw.get("sector_sentiment", {}).items():
        score = info if isinstance(info, (int, float)) else info.get("score", 50)
        if score < WEAK_SECTOR_THRESHOLD:
            weak_sectors[sector] = score

    # NEGATIVE 헤드라인 → 6자리 한국 종목코드
    for hl in raw.get("korea_headlines", []):
        if hl.get("sentiment") == "NEGATIVE":
            for ticker in hl.get("tickers", []):
                if len(ticker) == 6 and ticker.isdigit():
                    neg_tickers.append({
                        "code": ticker,
                        "title": hl.get("title", "")[:50],
                    })

    return weak_sectors, neg_tickers


# ═══════════════════════════════════════════════════
#  RISK-04: breaking_alerts
# ═══════════════════════════════════════════════════

def _load_breaking_alerts() -> Dict[str, str]:
    """속보에서 '-영향' 섹터 추출."""
    neg_sectors: Dict[str, str] = {}

    if not BREAKING_JSON.exists():
        return neg_sectors

    try:
        raw = json.loads(BREAKING_JSON.read_text(encoding="utf-8"))
    except Exception as e:
        logger.warning(f"[장전리스크] breaking_alerts 로드 실패: {e}")
        return neg_sectors

    for alert in raw.get("alerts", []):
        ts = alert.get("timestamp", "")
        if ts and not _is_fresh_date(ts[:10]):
            continue
        for sector, impact in alert.get("sectors_impact", {}).items():
            if "-영향" in str(impact):
                neg_sectors[sector] = alert.get("title", "")[:50]

    return neg_sectors


# ═══════════════════════════════════════════════════
#  메인 스캔 함수
# ═══════════════════════════════════════════════════

def scan_premarket_risk() -> Dict[str, PremarketRisk]:
    """장전 종합 리스크 스캔 — 3개 소스 통합.

    Returns:
        {stock_code: PremarketRisk}  (위험 종목만 포함)
    """
    global _risk_cache, _risk_cache_ts

    if _risk_cache and (time.time() - _risk_cache_ts) < _CACHE_TTL:
        return _risk_cache

    result: Dict[str, PremarketRisk] = {}

    # Source 1: DART 공시
    try:
        dart = _load_block_deal()
        result.update(dart)
        if dart:
            logger.info(f"[장전리스크] DART 공시: {len(dart)}종목")
    except Exception as e:
        logger.warning(f"[장전리스크] DART 로드 실패(무시): {e}")

    # Source 2: 센티먼트 + NEGATIVE 헤드라인
    try:
        weak_sectors, neg_tickers = _load_daily_intelligence()

        for sector_en, score in weak_sectors.items():
            codes = _get_codes_for_sector(sector_en)
            for code in codes:
                if code not in result:
                    result[code] = PremarketRisk(code=code)
                result[code].sector_penalty = WEAK_SECTOR_PENALTY
                result[code].reasons.append(f"SECTOR_WEAK:{sector_en}({score})")

        seen_neg = set()
        for nt in neg_tickers:
            code = nt["code"]
            if code in seen_neg:
                continue
            seen_neg.add(code)
            if code not in result:
                result[code] = PremarketRisk(code=code)
            result[code].headline_penalty = NEGATIVE_HEADLINE_PENALTY
            result[code].reasons.append(f"NEWS_NEG:{nt['title']}")

        if weak_sectors:
            logger.info(f"[장전리스크] 약세섹터: {list(weak_sectors.keys())[:5]}")
    except Exception as e:
        logger.warning(f"[장전리스크] intelligence 로드 실패(무시): {e}")

    # Source 3: 속보 "-영향" 섹터
    try:
        breaking = _load_breaking_alerts()
        for sector_en, title in breaking.items():
            codes = _get_codes_for_sector(sector_en)
            for code in codes:
                if code not in result:
                    result[code] = PremarketRisk(code=code)
                result[code].breaking_penalty = BREAKING_NEGATIVE_PENALTY
                result[code].reasons.append(f"BREAKING:{sector_en}")
        if breaking:
            logger.info(f"[장전리스크] 속보 '-영향': {list(breaking.keys())[:5]}")
    except Exception as e:
        logger.warning(f"[장전리스크] breaking 로드 실패(무시): {e}")

    # 합산
    for risk in result.values():
        if not risk.hard_exclude:
            total = (risk.penalty + risk.sector_penalty
                     + risk.headline_penalty + risk.breaking_penalty)
            risk.penalty = total
        if not risk.tag and risk.reasons:
            risk.tag = risk.reasons[0].split(":")[0]

    _risk_cache = result
    _risk_cache_ts = time.time()

    _save_daily_cache(result)
    return result


def _save_daily_cache(risks: Dict[str, PremarketRisk]):
    """data_store/premarket_blacklist.json 저장."""
    try:
        data = {
            "date": datetime.now().strftime("%Y-%m-%d"),
            "generated_at": datetime.now().isoformat(),
            "count": len(risks),
            "hard_exclude_count": sum(1 for r in risks.values() if r.hard_exclude),
            "stocks": {
                code: {
                    "name": r.name,
                    "penalty": r.penalty,
                    "hard_exclude": r.hard_exclude,
                    "tag": r.tag,
                    "reasons": r.reasons,
                }
                for code, r in risks.items()
            },
        }
        CACHE_FILE.write_text(
            json.dumps(data, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    except Exception as e:
        logger.warning(f"[장전리스크] 캐시 저장 실패: {e}")


# ═══════════════════════════════════════════════════
#  Public API (recommendation / telegram 연동)
# ═══════════════════════════════════════════════════

def get_premarket_risk(code: str) -> Tuple[float, str]:
    """morning_recommendation에서 호출 — penalty + 사유."""
    risks = scan_premarket_risk()
    if code in risks:
        r = risks[code]
        detail = " | ".join(r.reasons[:3])
        return r.penalty, detail
    return 0.0, ""


def is_premarket_hard_exclude(code: str) -> bool:
    """상장폐지 등 Hard Filter."""
    risks = scan_premarket_risk()
    return risks.get(code, PremarketRisk(code=code)).hard_exclude


def format_premarket_risk_alert(risks: Dict[str, PremarketRisk] = None) -> str:
    """08:20 텔레그램 알림 포맷."""
    if risks is None:
        risks = scan_premarket_risk()

    if not risks:
        return ""

    now = datetime.now()
    today_str = now.strftime("%m/%d(%a)")
    time_str = now.strftime("%H:%M")

    lines = []
    lines.append("---")
    lines.append("  장전 종합 리스크 스캔")
    lines.append(f"  {today_str} {time_str}")
    lines.append("---")

    hard = [r for r in risks.values() if r.hard_exclude]
    dart_risk = sorted(
        [r for r in risks.values() if not r.hard_exclude and r.penalty <= -10],
        key=lambda x: x.penalty,
    )
    mild = sorted(
        [r for r in risks.values()
         if not r.hard_exclude and -10 < r.penalty <= -3],
        key=lambda x: x.penalty,
    )

    if hard:
        lines.append("")
        lines.append("[!!!] 매수 금지")
        for i, r in enumerate(hard[:5], 1):
            lines.append(f"{i}. {r.name or r.code}({r.code}) {r.tag}")

    if dart_risk:
        lines.append("")
        lines.append("[!!] DART 공시 위험")
        for i, r in enumerate(dart_risk[:7], 1):
            reason = r.reasons[0] if r.reasons else ""
            lines.append(
                f"{i}. {r.name or r.code}({r.code}) {r.penalty:+.0f}점 {reason}"
            )

    if mild:
        lines.append("")
        lines.append("[!] 주의")
        for i, r in enumerate(mild[:10], 1):
            reason = r.reasons[0] if r.reasons else ""
            lines.append(
                f"{i}. {r.name or r.code}({r.code}) {r.penalty:+.0f} {reason}"
            )

    lines.append("")
    lines.append(
        f"총 {len(risks)}종목 "
        f"(금지:{len(hard)} 위험:{len(dart_risk)} 주의:{len(mild)})"
    )

    return "\n".join(lines)


# ═══════════════════════════════════════════════════
#  CLI 테스트
# ═══════════════════════════════════════════════════

if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    sys.stdout.reconfigure(encoding="utf-8")

    risks = scan_premarket_risk()
    print(f"\n총 {len(risks)}종목 리스크 감지\n")

    alert = format_premarket_risk_alert(risks)
    if alert:
        print(alert)
    else:
        print("(위험 종목 없음)")
