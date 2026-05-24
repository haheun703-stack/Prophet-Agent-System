# -*- coding: utf-8 -*-
"""실전매매 일지 — scalper_trade_journal 테이블 적재 헬퍼.

사장님 요청 2026-05-17: FLOWX 대시보드 마지막 탭 "실전매매 결과" 페이지 데이터 소스.
모든 매수/매도 이벤트를 timeline 형식으로 영구 기록 → 5개 차트 데이터 생산.

사용 패턴:
    from data import trade_journal as _tj

    # 매수 시점
    _tj.log_buy(code, name, qty, price, source="morning_rec",
                signal_tags="etf_lead(TOP10/4ETF:+13)", final_score=85.5)

    # 매도 시점 (PnL 자동 계산)
    _tj.log_sell(code, name, qty, sell_price, buy_price,
                 event_type="sell_tp", source="morning_rec")

설계 원칙:
  - 적재 실패해도 매매 자체는 진행 (best-effort)
  - DDL 자동 생성 옵션 (--create-table)
  - 검증 모드 verification_log와 별도 (검증은 verification_mode.py가 자체 기록)
  - 다만 trade_journal_api에서 두 테이블 통합 view 제공
"""
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional

try:
    from zoneinfo import ZoneInfo
    KST = ZoneInfo("Asia/Seoul")
except ImportError:
    KST = None

logger = logging.getLogger(__name__)


def _now_kst():
    return datetime.now(KST) if KST else datetime.now()


# 유효 event_type 화이트리스트 (DDL의 COMMENT와 일치)
_VALID_EVENT_TYPES = {
    "buy",
    "sell_tp",          # 익절
    "sell_sl",          # 손절
    "sell_close",       # 15:30 마감 청산 또는 사장님 강제 청산
    "partial_sell",     # 분할 익절
    "manual_buy",       # 사장님 수동 매수
    "manual_sell",      # 사장님 수동 매도
}

# ★ 5/24 사장님 결정 15번: 매매 직후 자동 callback (텔레그램 즉시 보고용) ★
# 영구 룰 [feedback_self_monitoring_realtime_5_22]: 매수/매도 즉시 사장님 인지
_trade_callbacks = []


def register_trade_callback(callback):
    """매매 직후 호출될 callback 등록.

    Signature: callback(side: str, code: str, name: str, qty: int, price: float,
                        source: str, entry_price: float = 0, note: str = "") -> None
        side: "BUY" or "SELL"
        entry_price: SELL인 경우 매수가 (PnL 계산용)
    """
    if callback not in _trade_callbacks:
        _trade_callbacks.append(callback)


def _fire_callbacks(side: str, code: str, name: str, qty: int, price: float,
                    source: str, entry_price: float = 0, note: str = "") -> None:
    """내부 — log_buy/log_sell 성공 후 호출 (best-effort)."""
    for cb in _trade_callbacks:
        try:
            cb(side=side, code=code, name=name, qty=qty, price=price,
               source=source, entry_price=entry_price, note=note or "")
        except Exception as _cb_e:
            logger.warning(f"[trade_journal] callback 실패 (무시): {type(_cb_e).__name__}: {_cb_e}")


_VALID_SOURCES = {
    "morning_rec",      # 09:00 morning_recommendation 자동
    "verification",     # 검증 모드 morning_rec (1주씩)
    "intraday_scan",    # 검증 모드 v2 — 장중 멀티시그널 진입 (5/17 사장님 결정)
    "nxt_auto",         # NXT 자동매매 (5/21+)
    "predawn",          # 선취매 (afterhours)
    "manual",           # 사장님 수동
    "guardian",         # position_guardian 강제 청산
    "eye",              # AI EYE 청산
    "external",         # 정보봇/웹봇 시그널
}


def log_buy(
    code: str,
    name: str,
    qty: int,
    price: float,
    source: str = "morning_rec",
    signal_tags: str = "",
    final_score: float = 0.0,
    order_no: Optional[str] = None,
    note: Optional[str] = None,
) -> bool:
    """매수 이벤트 기록.

    Returns:
        True = 적재 성공 / False = 실패 (best-effort, 매매는 진행됨)
    """
    if source not in _VALID_SOURCES:
        logger.warning(f"[trade_journal] 알 수 없는 source '{source}' → 'external' 처리")
        source = "external"

    now = _now_kst()
    try:
        from utils.supabase_sql import execute
        execute(
            """
            INSERT INTO scalper_trade_journal (
                event_time, event_date, event_type, source,
                stock_ticker, stock_name, qty, price, total_amount,
                signal_tags, final_score, order_no, note
            ) VALUES (
                %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s
            )
            """,
            (
                now, now.date(), "buy", source,
                code, name, int(qty), float(price),
                round(int(qty) * float(price), 2),
                signal_tags or "", float(final_score or 0),
                order_no, note,
            ),
        )
        logger.info(f"[trade_journal] BUY [{code}] {name} {qty}주 @{price:,.0f} ({source})")
        _fire_callbacks(side="BUY", code=code, name=name, qty=int(qty),
                        price=float(price), source=source, note=note or "")
        return True
    except Exception as e:
        logger.warning(f"[trade_journal] BUY 적재 실패 (무시): {type(e).__name__}: {e}")
        # 적재 실패해도 매매 자체는 발생했을 수 있으므로 callback은 발사
        _fire_callbacks(side="BUY", code=code, name=name, qty=int(qty),
                        price=float(price), source=source, note=note or "")
        return False


def log_sell(
    code: str,
    name: str,
    qty: int,
    sell_price: float,
    buy_price: float,
    event_type: str = "sell_close",
    source: str = "morning_rec",
    order_no: Optional[str] = None,
    note: Optional[str] = None,
) -> bool:
    """매도 이벤트 기록 — PnL 자동 계산.

    Args:
        buy_price: 매도 대상 종목의 평균 매수가 (PnL 계산용)
        event_type: sell_tp / sell_sl / sell_close / partial_sell / manual_sell

    Returns:
        True = 적재 성공
    """
    if event_type not in _VALID_EVENT_TYPES or not event_type.startswith("sell") and event_type != "manual_sell":
        logger.warning(f"[trade_journal] 비매도 event_type '{event_type}' → 'sell_close' 처리")
        event_type = "sell_close"
    if source not in _VALID_SOURCES:
        source = "external"

    sell_price_f = float(sell_price)
    buy_price_f = float(buy_price) if buy_price else 0.0
    qty_i = int(qty)

    pnl_pct = ((sell_price_f - buy_price_f) / buy_price_f * 100) if buy_price_f > 0 else 0.0
    pnl_amount = (sell_price_f - buy_price_f) * qty_i if buy_price_f > 0 else 0.0

    now = _now_kst()
    try:
        from utils.supabase_sql import execute
        execute(
            """
            INSERT INTO scalper_trade_journal (
                event_time, event_date, event_type, source,
                stock_ticker, stock_name, qty, price, total_amount,
                matched_buy_price, pnl_pct, pnl_amount,
                order_no, note
            ) VALUES (
                %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s
            )
            """,
            (
                now, now.date(), event_type, source,
                code, name, qty_i, sell_price_f, round(qty_i * sell_price_f, 2),
                buy_price_f, round(pnl_pct, 3), round(pnl_amount, 2),
                order_no, note,
            ),
        )
        logger.info(
            f"[trade_journal] SELL [{code}] {name} {qty_i}주 "
            f"{buy_price_f:,.0f} → {sell_price_f:,.0f} ({pnl_pct:+.2f}%) [{event_type}/{source}]"
        )
        _fire_callbacks(side="SELL", code=code, name=name, qty=qty_i,
                        price=sell_price_f, source=source,
                        entry_price=buy_price_f, note=f"{event_type}{(' '+note) if note else ''}")
        return True
    except Exception as e:
        logger.warning(f"[trade_journal] SELL 적재 실패 (무시): {type(e).__name__}: {e}")
        _fire_callbacks(side="SELL", code=code, name=name, qty=qty_i,
                        price=sell_price_f, source=source,
                        entry_price=buy_price_f, note=f"{event_type}{(' '+note) if note else ''}")
        return False


def create_table_via_raw_conn():
    """1회성 DDL 직접 실행 — scalper_trade_journal 테이블 + 5 인덱스 + 2 view 생성."""
    sql_path = (Path(__file__).resolve().parent.parent / "sql" /
                "scalper_trade_journal_migration.sql")
    if not sql_path.exists():
        raise FileNotFoundError(f"DDL 파일 없음: {sql_path}")
    ddl = sql_path.read_text(encoding="utf-8")

    import sys as _sys
    _sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from utils.supabase_sql import get_conn
    logger.info("DDL 실행 중 (scalper_trade_journal + view 2개)...")
    with get_conn(readonly=False) as conn:
        with conn.cursor() as cur:
            cur.execute(ddl)
    logger.info("DDL 실행 완료.")


if __name__ == "__main__":
    import sys, io, argparse
    # CLI 단독 실행 시 sys.path 보강 (utils.supabase_sql 해석)
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

    parser = argparse.ArgumentParser(description="실전매매 일지 admin")
    parser.add_argument("--create-table", action="store_true",
                        help="scalper_trade_journal 테이블 + view 1회성 자동 생성")
    parser.add_argument("--test", action="store_true",
                        help="더미 매수/매도 이벤트로 테이블 검증 (DB에 실제 기록됨)")
    args = parser.parse_args()

    if args.create_table:
        create_table_via_raw_conn()
        print("✅ scalper_trade_journal 테이블 + 인덱스 5개 + view 2개 생성/확인 완료")

    if args.test:
        print("\n=== 더미 매수/매도 검증 (테스트 종목 999999) ===")
        log_buy("999999", "테스트종목", 1, 10000.0,
                source="external", signal_tags="test", final_score=0)
        log_sell("999999", "테스트종목", 1, 10500.0, 10000.0,
                 event_type="sell_close", source="external", note="더미 테스트")
        print("✅ 더미 매수/매도 적재 완료 — DB에서 'DELETE FROM scalper_trade_journal WHERE stock_ticker = ''999999'';' 로 정리")
