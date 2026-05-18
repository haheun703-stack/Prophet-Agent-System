# -*- coding: utf-8 -*-
"""단타봇 → 퀀트봇 형 feedback INSERT 클라이언트 (5/18 형 실제 스키마 기준)

형이 설계한 scalper_bot_feedback 테이블 (34컬럼, 5/18 70d57bb):
- 공통: id, created_at, feedback_date, feedback_time, msg_type, notes
- RECOMMENDATION: rec_grade, rec_tickers, gates_passed_n, gates_detail
- ENTRY: entry_ticker, entry_name, entry_price, entry_qty, entry_amount, entry_grade, entry_reason, entry_source, entry_advisory_ref
- CLOSE: close_ticker, close_price, close_pnl_pct, close_pnl_amount, close_reason, holding_minutes
- JOURNAL_ACCURACY: journal_total_trades, journal_wins, journal_losses, journal_accuracy_pct, journal_by_source, journal_avg_pnl_pct
- WEEKLY: weekly_summary, weekly_top_pattern, weekly_worst_pattern

동생 측 사용 패턴:
  from utils.scalper_feedback import log_recommendation, log_entry, log_close, log_journal_accuracy

  # 09:00 — 모닝 추천 + 5게이트 결과
  log_recommendation(grade='STRONG', tickers=['483650', '068270'], gates_passed_n=5,
                     gates_detail={'market': True, 'vwap': True, 'spread': True, 'strength': True, 'candle': True})

  # 09:05~14:00 — 매수 실행
  log_entry(ticker='483650', name='달바글로벌', price=236500, qty=1, grade='STRONG',
            reason='5게이트 ALL PASS + advisory CAUTION', source='verification', advisory_id=1)

  # 15:25 — 청산
  log_close(ticker='483650', price=240100, pnl_pct=1.52, pnl_amount=3600,
            reason='15:25 강제 청산', holding_minutes=385)

  # 15:40 — source별 적중률
  log_journal_accuracy(total=13, wins=8, losses=5,
                       by_source={'morning_rec': {'hit':6, 'miss':3}, 'intraday_scan': {'hit':2, 'miss':2}},
                       avg_pnl_pct=1.05)
"""
import json
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

try:
    from zoneinfo import ZoneInfo
    KST = ZoneInfo("Asia/Seoul")
except ImportError:
    KST = None

# 프로젝트 루트
_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))

from dotenv import load_dotenv
load_dotenv(_ROOT.parent / ".env")

from utils.supabase_sql import get_conn  # noqa: E402

logger = logging.getLogger("BH.ScalperFeedback")


def _now() -> datetime:
    return datetime.now(KST) if KST else datetime.now()


def _insert(sql: str, params: tuple) -> Optional[int]:
    """범용 INSERT 헬퍼. 성공 시 id 반환."""
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                row = cur.fetchone()
                conn.commit()
                if row:
                    # RealDictCursor 호환 (cursor 기본/dict 둘 다)
                    return row["id"] if isinstance(row, dict) else row[0]
        return None
    except Exception as e:
        logger.warning(f"scalper_feedback INSERT 실패: {e}")
        return None


def log_recommendation(
    grade: str,
    tickers: List[str],
    gates_passed_n: int,
    gates_detail: Dict[str, bool],
    notes: Optional[str] = None,
) -> Optional[int]:
    """09:00 모닝 추천 + 5게이트 결과 적재.

    Args:
        grade: 'STRONG' | 'MEDIUM' | 'WATCH' | 'SKIP'
        tickers: 추천 종목 코드 리스트
        gates_passed_n: PASS한 게이트 수 (0~5)
        gates_detail: {market: true, vwap: true, spread: true, strength: true, candle: true}
        notes: 추가 메모
    """
    now = _now()
    sql = """
        INSERT INTO scalper_bot_feedback
            (feedback_date, feedback_time, msg_type,
             rec_grade, rec_tickers, gates_passed_n, gates_detail, notes)
        VALUES (%s, %s, 'RECOMMENDATION', %s, %s, %s, %s::jsonb, %s)
        RETURNING id
    """
    return _insert(sql, (
        now.date(),
        now.strftime("%H:%M:%S"),
        grade,
        tickers,
        gates_passed_n,
        json.dumps(gates_detail),
        notes,
    ))


def log_entry(
    ticker: str,
    name: str,
    price: int,
    qty: int,
    grade: str,
    reason: str = "",
    source: str = "verification",
    advisory_id: Optional[int] = None,
    notes: Optional[str] = None,
) -> Optional[int]:
    """09:05~14:00 매수 실행 적재.

    Args:
        ticker: 종목코드 (6자리)
        name: 종목명
        price: 매수가
        qty: 수량
        grade: 'STRONG' | 'MEDIUM' | 'WATCH'
        reason: 진입 사유
        source: 'verification' | 'morning_rec' | 'intraday_scan'
        advisory_id: 참조한 형 advisory id
    """
    now = _now()
    sql = """
        INSERT INTO scalper_bot_feedback
            (feedback_date, feedback_time, msg_type,
             entry_ticker, entry_name, entry_price, entry_qty, entry_amount,
             entry_grade, entry_reason, entry_source, entry_advisory_ref, notes)
        VALUES (%s, %s, 'ENTRY', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id
    """
    return _insert(sql, (
        now.date(),
        now.strftime("%H:%M:%S"),
        ticker,
        name,
        price,
        qty,
        price * qty,
        grade,
        reason,
        source,
        advisory_id,
        notes,
    ))


def log_close(
    ticker: str,
    price: int,
    pnl_pct: float,
    pnl_amount: int,
    reason: str = "15:25 강제 청산",
    holding_minutes: Optional[int] = None,
    notes: Optional[str] = None,
) -> Optional[int]:
    """15:25 청산 적재."""
    now = _now()
    sql = """
        INSERT INTO scalper_bot_feedback
            (feedback_date, feedback_time, msg_type,
             close_ticker, close_price, close_pnl_pct, close_pnl_amount,
             close_reason, holding_minutes, notes)
        VALUES (%s, %s, 'CLOSE', %s, %s, %s, %s, %s, %s, %s)
        RETURNING id
    """
    return _insert(sql, (
        now.date(),
        now.strftime("%H:%M:%S"),
        ticker,
        price,
        round(pnl_pct, 2),
        pnl_amount,
        reason,
        holding_minutes,
        notes,
    ))


def log_journal_accuracy(
    total: int,
    wins: int,
    losses: int,
    by_source: Optional[Dict[str, Dict[str, int]]] = None,
    avg_pnl_pct: Optional[float] = None,
    notes: Optional[str] = None,
) -> Optional[int]:
    """15:40 trade_journal source별 적중률 적재.

    Args:
        total: 총 거래 건수
        wins: 적중 (PnL >= 0) 건수
        losses: 미적중 (PnL < 0) 건수
        by_source: {'morning_rec': {'hit': 6, 'miss': 3}, ...}
        avg_pnl_pct: 평균 PnL %
    """
    now = _now()
    accuracy = round(wins / total * 100, 2) if total else 0
    sql = """
        INSERT INTO scalper_bot_feedback
            (feedback_date, feedback_time, msg_type,
             journal_total_trades, journal_wins, journal_losses,
             journal_accuracy_pct, journal_by_source, journal_avg_pnl_pct, notes)
        VALUES (%s, %s, 'JOURNAL_ACCURACY', %s, %s, %s, %s, %s::jsonb, %s, %s)
        RETURNING id
    """
    return _insert(sql, (
        now.date(),
        now.strftime("%H:%M:%S"),
        total,
        wins,
        losses,
        accuracy,
        json.dumps(by_source) if by_source else None,
        round(avg_pnl_pct, 2) if avg_pnl_pct is not None else None,
        notes,
    ))


if __name__ == "__main__":
    print("=== scalper_bot_feedback 더미 INSERT 검증 (4종 모두) ===\n")

    fid = log_journal_accuracy(total=0, wins=0, losses=0, notes="test_insert_to_be_deleted")
    print(f"INSERT 1 (JOURNAL_ACCURACY): id={fid}")

    # 검증 SELECT
    from utils.supabase_sql import query
    if fid:
        rows = query("SELECT id, msg_type, journal_total_trades, notes FROM scalper_bot_feedback WHERE id = %s", (fid,))
        for r in rows:
            print(f"  id={r['id']} type={r['msg_type']} total={r['journal_total_trades']} notes={r['notes']}")
        # 테스트 데이터 정리
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM scalper_bot_feedback WHERE id = %s", (fid,))
                conn.commit()
        print(f"\n✅ 테스트 데이터 정리 (id={fid} 삭제)")
        print(f"✅ 양방향 채널 동생→형 INSERT 가동 검증 PASS!")
