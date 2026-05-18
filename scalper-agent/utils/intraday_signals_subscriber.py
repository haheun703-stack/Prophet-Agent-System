# -*- coding: utf-8 -*-
"""막내 정보봇 intraday_signals 통합 채널 구독 클라이언트 (5/18 막내 v3 가이드)

5/19 신설, 5/20 자동 가동:
  - 4 소스 통합 (daily_news + trump_rss + truth_social + price_monitor)
  - 장중 매 15분 (09:00~15:30, 26회/일)
  - impact_score 0~100, HIGH(≥70) 즉시 텔레그램 알림
  - related_tickers + related_themes JSONB

★ 막내 권고 expire_at 필터 100% 채택 ★
  - 트럼프 RSS 24h / 가격 시그널 30분 / DART T+3거래일
  - 만료된 시그널이 잘못된 청산 결정 유도 방지
  - 매 SELECT에 WHERE expire_at > NOW() 자동 적용

3봇 일치 황금 표준 매수/청산:
  매수: 큰형 advisory POSI + 막내 NEUT/POSI + 둘째형 5게이트
  청산: 큰형 LEADING WARN OR 막내 NEGA impact 70+ OR 둘째형 -2%
"""
import logging
import sys
from datetime import datetime, timedelta
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

logger = logging.getLogger("BH.IntradaySignals")


def _today_start_iso() -> str:
    """KST 오늘 0시 ISO 문자열."""
    now = datetime.now(KST) if KST else datetime.now()
    return now.replace(hour=0, minute=0, second=0, microsecond=0).isoformat()


def fetch_active_signals(
    impact_score_min: int = 70,
    since: Optional[str] = None,
    sentiment: Optional[str] = None,
) -> List[Dict]:
    """오늘 활성 intraday_signals 조회 (★ 막내 권고 expire_at 필터 ★).

    Args:
        impact_score_min: 임계값 (기본 70 = HIGH)
        since: TIMESTAMP 이후만 (None이면 오늘 0시부터)
        sentiment: 'NEGA' | 'NEUT' | 'POSI' | None (전체)

    Returns:
        활성 시그널 리스트 (expire_at > NOW() 자동 필터)
    """
    sql_parts = [
        "SELECT * FROM intraday_signals",
        "WHERE expire_at > NOW()",       # ★ 막내 권고
        "AND impact_score >= %s",
        "AND signal_at >= %s",
    ]
    params: List = [impact_score_min, since or _today_start_iso()]

    if sentiment:
        sql_parts.append("AND sentiment = %s")
        params.append(sentiment)

    sql_parts.append("ORDER BY signal_at DESC")
    sql = " ".join(sql_parts)

    try:
        return query(sql, tuple(params))
    except Exception as e:
        logger.warning(f"intraday_signals 조회 실패 (테이블 없을 수도, 5/19 신설): {e}")
        return []


def fetch_negative_high_signals(since: Optional[str] = None) -> List[Dict]:
    """매수 차단용 — 활성 NEGA impact 70+ 시그널."""
    return fetch_active_signals(
        impact_score_min=70,
        since=since,
        sentiment="NEGA",
    )


def fetch_positive_high_signals(since: Optional[str] = None) -> List[Dict]:
    """매수 후보 발굴용 — 활성 POSI impact 70+ 시그널."""
    return fetch_active_signals(
        impact_score_min=70,
        since=since,
        sentiment="POSI",
    )


def collect_blocked_tickers(since: Optional[str] = None) -> set:
    """매수 차단 종목 set (NEGA impact 70+ related_tickers 통합)."""
    blocked = set()
    nega_signals = fetch_negative_high_signals(since=since)
    for sig in nega_signals:
        tickers = sig.get("related_tickers") or []
        if isinstance(tickers, str):
            # JSONB가 문자열로 올 수 있음
            import json
            try:
                tickers = json.loads(tickers)
            except Exception:
                tickers = [tickers]
        blocked.update(tickers)
    return blocked


def collect_opportunity_tickers(since: Optional[str] = None) -> set:
    """매수 후보 종목 set (POSI impact 70+ related_tickers 통합)."""
    opportunities = set()
    posi_signals = fetch_positive_high_signals(since=since)
    for sig in posi_signals:
        tickers = sig.get("related_tickers") or []
        if isinstance(tickers, str):
            import json
            try:
                tickers = json.loads(tickers)
            except Exception:
                tickers = [tickers]
        opportunities.update(tickers)
    return opportunities


def check_position_for_sell(ticker: str, since: Optional[str] = None) -> Optional[Dict]:
    """보유 종목이 새 NEGA impact 70+에 걸리는지 체크 → 즉시 청산 시그널.

    Returns:
        해당 종목이 NEGA에 매칭되면 시그널 dict, 아니면 None
    """
    nega = fetch_negative_high_signals(since=since)
    for sig in nega:
        tickers = sig.get("related_tickers") or []
        if isinstance(tickers, str):
            import json
            try:
                tickers = json.loads(tickers)
            except Exception:
                tickers = [tickers]
        if ticker in tickers:
            return sig
    return None


if __name__ == "__main__":
    # 5/18 (5/19 막내 작업 전) 테이블 없을 가능성 → 조회 실패 확인
    print("=== intraday_signals_subscriber 자가 진단 ===")
    print()
    print("[1] 오늘 활성 시그널 (impact 70+)")
    active = fetch_active_signals(impact_score_min=70)
    if active:
        print(f"  {len(active)}건 발견")
        for s in active[:5]:
            print(f"    - {s.get('signal_at')} [{s.get('sentiment'):<5}] impact={s.get('impact_score')} → {s.get('title', '')[:60]}")
    else:
        print("  0건 (5/19 막내 작업 전이라 테이블/데이터 없음 정상)")

    print()
    print("[2] 차단 종목 (NEGA 70+)")
    blocked = collect_blocked_tickers()
    print(f"  {len(blocked)}종목: {list(blocked)[:10]}")

    print()
    print("[3] 기회 종목 (POSI 70+)")
    opps = collect_opportunity_tickers()
    print(f"  {len(opps)}종목: {list(opps)[:10]}")

    print()
    print("★ 5/19 막내 intraday_signals 자동 가동 후 라이브 검증 예정")
    print("★ 5/20 09:00 매수 직전 fetch_active_signals + collect_blocked_tickers 활용")
