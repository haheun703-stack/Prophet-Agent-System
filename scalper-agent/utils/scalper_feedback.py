# -*- coding: utf-8 -*-
"""단타봇 → 퀀트봇 형 feedback INSERT 클라이언트 (5/18 형과 양방향 채널 합의)

동생 → 형: scalper_bot_feedback 테이블 INSERT
- event_type: RECOMMENDATION (09:00), ENTRY (09:05~14:00), CLOSE (15:25), JOURNAL_ACCURACY (15:40)

동생 측 사용 패턴:
  from utils.scalper_feedback import log_recommendation, log_entry, log_close, log_journal_accuracy

  # 09:00 — 모닝 추천 + 5게이트 결과
  log_recommendation(grades={'STRONG': 3, 'MEDIUM': 5, 'WATCH': 5, 'SKIP': 0},
                     top_tickers=['483650', '068270', '204320'])

  # 09:05~14:00 — 매수 실행
  log_entry(code='483650', name='달바글로벌', price=236500, qty=1, grade='STRONG',
            gates={'market': True, 'vwap': True, 'spread': True, 'strength': True, 'candle': True},
            advisory_id=1)

  # 15:25 — 청산
  log_close(code='483650', entry_price=236500, close_price=240100)

  # 15:40 — source별 적중률
  log_journal_accuracy(source='morning_rec', hit=8, miss=5)
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


def _insert_feedback(
    msg_type: str,
    code: Optional[str] = None,
    name: Optional[str] = None,
    payload: Optional[Dict] = None,
    advisory_referenced: Optional[int] = None,
    gate_passes: Optional[Dict] = None,
    grade: Optional[str] = None,
) -> Optional[int]:
    """범용 INSERT 헬퍼. 성공 시 id 반환."""
    now = _now()
    sql = """
        INSERT INTO scalper_bot_feedback
            (feedback_date, feedback_time, msg_type, code, name,
             payload, advisory_referenced, gate_passes, grade)
        VALUES (%s, %s, %s, %s, %s, %s::jsonb, %s, %s::jsonb, %s)
        ON CONFLICT (feedback_date, feedback_time, msg_type, code)
        DO UPDATE SET payload = EXCLUDED.payload, gate_passes = EXCLUDED.gate_passes
        RETURNING id
    """
    params = (
        now.date(),
        now.strftime("%H:%M:%S"),
        msg_type,
        code,
        name,
        json.dumps(payload or {}, ensure_ascii=False),
        advisory_referenced,
        json.dumps(gate_passes) if gate_passes else None,
        grade,
    )
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                row = cur.fetchone()
                conn.commit()
                return row[0] if row else None
    except Exception as e:
        logger.warning(f"scalper_feedback INSERT 실패 ({msg_type}): {e}")
        return None


def log_recommendation(
    grades: Dict[str, int],
    top_tickers: List[str],
    advisory_id: Optional[int] = None,
) -> Optional[int]:
    """09:00 모닝 추천 + 5게이트 결과 적재.

    Args:
        grades: {'STRONG': 3, 'MEDIUM': 5, 'WATCH': 5, 'SKIP': 0}
        top_tickers: 추천 상위 종목 코드 리스트
        advisory_id: 참조한 형 advisory id (예: 09:30 advisory 없으면 None)
    """
    payload = {"grades": grades, "top_tickers": top_tickers, "n": sum(grades.values())}
    return _insert_feedback(
        msg_type="RECOMMENDATION",
        payload=payload,
        advisory_referenced=advisory_id,
    )


def log_entry(
    code: str,
    name: str,
    price: int,
    qty: int,
    grade: str,
    gates: Dict[str, bool],
    advisory_id: Optional[int] = None,
) -> Optional[int]:
    """09:05~14:00 매수 실행 적재.

    Args:
        code: 종목코드
        name: 종목명
        price: 매수가
        qty: 수량
        grade: 'STRONG' | 'MEDIUM' | 'WATCH'
        gates: 5게이트 PASS/FAIL (예: {market: true, vwap: true, ...})
        advisory_id: 참조한 형 advisory id
    """
    payload = {"price": price, "qty": qty, "amount": price * qty}
    return _insert_feedback(
        msg_type="ENTRY",
        code=code,
        name=name,
        payload=payload,
        advisory_referenced=advisory_id,
        gate_passes=gates,
        grade=grade,
    )


def log_close(
    code: str,
    name: str,
    entry_price: int,
    close_price: int,
    qty: int = 1,
) -> Optional[int]:
    """15:25 청산 적재 (PnL 자동 계산)."""
    pnl_amount = (close_price - entry_price) * qty
    pnl_pct = round((close_price / entry_price - 1) * 100, 2) if entry_price else 0
    payload = {
        "entry_price": entry_price,
        "close_price": close_price,
        "qty": qty,
        "pnl_amount": pnl_amount,
        "pnl_pct": pnl_pct,
    }
    return _insert_feedback(
        msg_type="CLOSE",
        code=code,
        name=name,
        payload=payload,
    )


def log_journal_accuracy(source: str, hit: int, miss: int) -> Optional[int]:
    """15:40 trade_journal source별 적중률 적재.

    Args:
        source: 'morning_rec' | 'intraday_scan' | 'verification'
        hit: 적중 건수
        miss: 미적중 건수
    """
    total = hit + miss
    accuracy = round(hit / total, 3) if total else 0
    payload = {"source": source, "hit": hit, "miss": miss, "total": total, "accuracy": accuracy}
    return _insert_feedback(
        msg_type="JOURNAL_ACCURACY",
        code=source,  # source를 code 자리에 (UNIQUE 위배 방지)
        payload=payload,
    )


if __name__ == "__main__":
    # 검증 모드: 더미 INSERT 1건 + 즉시 SELECT
    print("=== scalper_bot_feedback 더미 INSERT 검증 ===")
    fid = log_journal_accuracy("test_source", hit=0, miss=0)
    print(f"INSERT id: {fid}")
    # 검증 SELECT
    from utils.supabase_sql import query
    if fid:
        rows = query("SELECT id, msg_type, code, payload FROM scalper_bot_feedback WHERE id = %s", (fid,))
        for r in rows:
            print(f"  id={r['id']} type={r['msg_type']} code={r['code']} payload={r['payload']}")
        # 정리 (테스트 데이터 삭제)
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM scalper_bot_feedback WHERE id = %s", (fid,))
                conn.commit()
        print(f"✅ 테스트 데이터 정리 (id={fid} 삭제)")
