# -*- coding: utf-8 -*-
"""막내 정보봇 daily_news 구독 클라이언트 (5/18 막내 v3 가이드)

매일 발행:
  - daily_news 테이블 (importance 1~5)
  - importance ≥5 = 강도 5 (시장 영향 큰 뉴스)
  - sentiment: POSI / NEUT / NEGA
  - region: KR / US / GL
  - tags + related_tickers JSONB

5/18 라이브 결과:
  - 강도 5 5건 (외인 7거래일 매도 + 엔비디아 D-1 + 케빈 워시 + S&P500 최고 + KOSPI 급락)
  - 강도 4 4건 (JP모건 + 한국 시총 7200조 + KCIF + 빅테크 7천억)
  → 5/20 D-Day 매크로 위험 경고

3봇 일치 황금 표준 적용:
  - 매수 차단: daily_news importance≥5 sentiment='NEGA' 관련 종목
  - 매수 우대: daily_news importance≥5 sentiment='POSI' 관련 종목
"""
import json
import logging
import sys
from datetime import datetime, date
from pathlib import Path
from typing import Dict, List, Optional

try:
    from zoneinfo import ZoneInfo
    KST = ZoneInfo("Asia/Seoul")
except ImportError:
    KST = None

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))

from dotenv import load_dotenv
load_dotenv(_ROOT.parent / ".env")

from utils.supabase_sql import query  # noqa: E402

logger = logging.getLogger("BH.DailyNews")


def _today_kst() -> date:
    return (datetime.now(KST) if KST else datetime.now()).date()


def fetch_today_news(
    importance_min: int = 5,
    sentiment: Optional[str] = None,
    region: Optional[str] = None,
) -> List[Dict]:
    """오늘 daily_news 조회.

    Args:
        importance_min: 임계값 (기본 5 = 강도 5 강력)
        sentiment: 'POSI' | 'NEUT' | 'NEGA' | None (전체)
        region: 'KR' | 'US' | 'GL' | None (전체)
    """
    sql_parts = [
        "SELECT id, date, title, body, sentiment, region, importance, tags, related_tickers",
        "FROM daily_news",
        "WHERE date = %s",
        "AND importance >= %s",
    ]
    params: List = [_today_kst(), importance_min]

    if sentiment:
        sql_parts.append("AND sentiment = %s")
        params.append(sentiment)
    if region:
        sql_parts.append("AND region = %s")
        params.append(region)

    sql_parts.append("ORDER BY importance DESC, id DESC")
    sql = " ".join(sql_parts)

    try:
        return query(sql, tuple(params))
    except Exception as e:
        logger.warning(f"daily_news 조회 실패 (테이블 없을 수도): {e}")
        return []


def fetch_negative_news(importance_min: int = 5) -> List[Dict]:
    """매수 차단용 — NEGA 강도 5+ 뉴스."""
    return fetch_today_news(importance_min=importance_min, sentiment="NEGA")


def fetch_positive_news(importance_min: int = 5) -> List[Dict]:
    """매수 우대용 — POSI 강도 5+ 뉴스."""
    return fetch_today_news(importance_min=importance_min, sentiment="POSI")


def _parse_tickers(raw) -> List[str]:
    """related_tickers JSONB → list."""
    if not raw:
        return []
    if isinstance(raw, list):
        return raw
    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except Exception:
            return [raw]
    return []


def collect_blocked_tickers_from_news() -> set:
    """매수 차단 종목 (NEGA 강도 5+ related_tickers 통합)."""
    blocked = set()
    for news in fetch_negative_news():
        tickers = _parse_tickers(news.get("related_tickers"))
        blocked.update(tickers)
    return blocked


def collect_opportunity_tickers_from_news() -> set:
    """매수 우대 종목 (POSI 강도 5+ related_tickers 통합)."""
    opps = set()
    for news in fetch_positive_news():
        tickers = _parse_tickers(news.get("related_tickers"))
        opps.update(tickers)
    return opps


def get_macro_risk_summary() -> Dict:
    """오늘 매크로 위험 요약 (3봇 일치 매수 결정 참고용).

    Returns:
        {
            "negative_high": [...],  # NEGA 강도 5+ 뉴스 제목
            "positive_high": [...],  # POSI 강도 5+ 뉴스 제목
            "blocked_count": N,      # 차단 종목 수
            "opportunity_count": N,  # 기회 종목 수
            "risk_level": "HIGH" | "MED" | "LOW"
        }
    """
    nega = fetch_negative_news()
    posi = fetch_positive_news()
    blocked = collect_blocked_tickers_from_news()
    opps = collect_opportunity_tickers_from_news()

    risk_level = "LOW"
    if len(nega) >= 3 or any(n.get("importance", 0) >= 5 for n in nega):
        risk_level = "HIGH"
    elif len(nega) >= 1:
        risk_level = "MED"

    return {
        "negative_high": [n.get("title", "")[:80] for n in nega],
        "positive_high": [p.get("title", "")[:80] for p in posi],
        "blocked_count": len(blocked),
        "opportunity_count": len(opps),
        "blocked_tickers": list(blocked),
        "opportunity_tickers": list(opps),
        "risk_level": risk_level,
    }


if __name__ == "__main__":
    print("=== daily_news_subscriber 자가 진단 ===")
    print()
    summary = get_macro_risk_summary()
    print(f"📊 매크로 위험 레벨: {summary['risk_level']}")
    print()
    print(f"🚨 NEGA 강도 5+ ({len(summary['negative_high'])}건):")
    for t in summary['negative_high']:
        print(f"  - {t}")
    print()
    print(f"🟢 POSI 강도 5+ ({len(summary['positive_high'])}건):")
    for t in summary['positive_high']:
        print(f"  - {t}")
    print()
    print(f"❌ 차단 종목 {summary['blocked_count']}: {summary['blocked_tickers'][:10]}")
    print(f"✅ 기회 종목 {summary['opportunity_count']}: {summary['opportunity_tickers'][:10]}")
