"""
FLOWX 시그널 트래킹 — signals + scoreboard Supabase CRUD
============================================================
퀀트봇(QUANT): 추천 종목 → signals INSERT → 성과 추적 → 자동 청산
단타봇(DAYTRADING): 장중 매매 시그널 → 15:20 일괄 청산

Usage:
  python data/flowx_signals.py --log-test       # 현재 추천 → signals INSERT 테스트
  python data/flowx_signals.py --update-test    # OPEN 시그널 현재가 업데이트 테스트
  python data/flowx_signals.py --scoreboard     # scoreboard 집계 테스트
"""
import os
import sys
import json
import logging
import argparse
from datetime import date, datetime, timedelta
from pathlib import Path

logger = logging.getLogger("flowx_signals")

# ── 경로 설정 ────────────────────────────────────────
_DATA_STORE = Path(__file__).resolve().parent.parent / "data_store"
_ENV_PATH = Path(__file__).resolve().parent.parent.parent / ".env"

# ── Supabase 클라이언트 ─────────────────────────────
_supabase = None


def _get_client():
    """Supabase 클라이언트 lazy 초기화"""
    global _supabase
    if _supabase is not None:
        return _supabase

    from dotenv import load_dotenv
    load_dotenv(_ENV_PATH)

    url = os.environ.get("SUPABASE_URL", "")
    key = os.environ.get("SUPABASE_KEY", "")
    if not url or not key:
        logger.error("SUPABASE_URL / SUPABASE_KEY 미설정")
        return None

    from supabase import create_client
    _supabase = create_client(url, key)
    return _supabase


# ══════════════════════════════════════════
#  등급 / 시그널 타입 (upload_short.py 기준)
# ══════════════════════════════════════════

def _score_to_grade(total_score: float, confidence: str = "LOW",
                    nat_power_grade: str = "NEUTRAL") -> str:
    """점수+신뢰도+국적파워 → AAA~F 등급"""
    if total_score >= 85 and confidence == "HIGH" \
       and nat_power_grade in ("POWER_BUY", "BUY"):
        return "AAA"
    if total_score >= 75 and confidence == "HIGH":
        return "AA"
    if total_score >= 65 and confidence == "HIGH":
        return "A"
    if total_score >= 55:
        return "BBB"
    if total_score >= 45:
        return "BB"
    if total_score >= 35:
        return "B"
    if total_score >= 25:
        return "C"
    if total_score >= 15:
        return "D"
    return "F"


def _determine_signal_type(grade: str, inst_support: bool,
                           volume_ratio: float,
                           tv_pattern: str = "NORMAL") -> str:
    """등급+기관+거래대금 → FORCE_BUY/BUY/WATCH/AVOID"""
    top_grades = ("AAA", "AA", "A")
    buy_grades = ("AAA", "AA", "A", "BBB")

    if tv_pattern == "QUIET_ACCUMULATION" and grade in top_grades \
       and volume_ratio >= 2.0:
        return "FORCE_BUY"
    if grade in top_grades and inst_support and volume_ratio >= 1.5:
        return "FORCE_BUY"
    if grade in buy_grades and (inst_support or volume_ratio >= 1.3):
        return "BUY"
    if grade in (*buy_grades, "BB"):
        return "WATCH"
    return "AVOID"


# ══════════════════════════════════════════
#  1. QUANT 시그널 로깅
# ══════════════════════════════════════════

def log_quant_signals(rec_data: dict = None) -> int:
    """추천 종목 → signals 테이블 INSERT

    Args:
        rec_data: recommendation.json 데이터 (없으면 파일에서 로드)

    Returns:
        INSERT된 시그널 수
    """
    client = _get_client()
    if not client:
        return 0

    if not rec_data:
        rec_path = _DATA_STORE / "recommendation.json"
        if not rec_path.exists():
            logger.warning("recommendation.json 없음")
            return 0
        rec_data = json.loads(rec_path.read_text("utf-8"))

    stocks = rec_data.get("stocks", [])
    if not stocks:
        logger.info("추천 종목 없음 - 시그널 로깅 스킵")
        return 0

    today_str = date.today().isoformat()
    rows = []

    for s in stocks:
        code = s.get("code", "")
        name = _resolve_name(code, s.get("name", ""))
        total_score = s.get("total_score", 0)
        confidence = s.get("confidence", "LOW")
        nat_power_grade = s.get("nat_power_grade", "NEUTRAL")

        grade = _score_to_grade(total_score, confidence, nat_power_grade)

        # 기관 동반 판별
        nat_detail = s.get("nationality_detail", "")
        inst_support = any(k in nat_detail for k in ("기OK", "기+"))

        # 거래대금 비율
        tv_ratio = s.get("tv_ratio", 1.0)
        tv_pattern = s.get("tv_pattern", "NORMAL")

        signal_type = _determine_signal_type(
            grade, inst_support, tv_ratio, tv_pattern
        )

        # AVOID 제외
        if signal_type == "AVOID":
            continue

        rows.append({
            "bot_type": "QUANT",
            "ticker": code,
            "ticker_name": name,
            "signal_type": signal_type,
            "grade": grade,
            "score": int(round(total_score)),
            "entry_price": s.get("entry", s.get("close", 0)),
            "target_price": s.get("tp", 0),
            "stop_loss": s.get("sl", 0),
            "current_price": s.get("entry", s.get("close", 0)),
            "return_pct": 0.0,
            "status": "OPEN",
            "signal_date": today_str,
            "sources": s.get("sources", []),
        })

    if not rows:
        logger.info("업로드할 QUANT 시그널 없음")
        return 0

    try:
        client.table("signals").upsert(
            rows, on_conflict="bot_type,ticker,signal_date"
        ).execute()
        logger.info(f"[FLOWX] QUANT 시그널 {len(rows)}건 upsert 완료")
        return len(rows)
    except Exception as e:
        logger.error(f"[FLOWX] signals upsert 실패: {e}")
        return 0


# ══════════════════════════════════════════
#  2. DAYTRADING 시그널 로깅
# ══════════════════════════════════════════

def log_daytrading_signal(code: str, name: str, entry_price: int,
                          signal_type: str = "BUY",
                          target_price: int = 0, stop_loss: int = 0,
                          sources: list = None) -> bool:
    """장중 매매 시그널 → signals INSERT (bot_type=DAYTRADING)"""
    client = _get_client()
    if not client:
        return False

    row = {
        "bot_type": "DAYTRADING",
        "ticker": code,
        "ticker_name": name,
        "signal_type": signal_type,
        "grade": "",
        "score": 0,
        "entry_price": entry_price,
        "target_price": target_price,
        "stop_loss": stop_loss,
        "current_price": entry_price,
        "return_pct": 0.0,
        "status": "OPEN",
        "signal_date": date.today().isoformat(),
        "sources": sources or [],
    }

    try:
        client.table("signals").upsert(
            [row], on_conflict="bot_type,ticker,signal_date"
        ).execute()
        logger.info(f"[FLOWX] DAYTRADING 시그널: {name}({code}) @{entry_price}")
        return True
    except Exception as e:
        logger.error(f"[FLOWX] DAYTRADING upsert 실패: {e}")
        return False


# ══════════════════════════════════════════
#  3. 성과 업데이트 (16:10 KST)
# ══════════════════════════════════════════

def update_performance(bot_type: str = "QUANT") -> dict:
    """OPEN 시그널의 current_price / return_pct 업데이트 + 자동 청산

    Returns:
        {"updated": int, "closed": int, "stopped": int}
    """
    client = _get_client()
    if not client:
        return {"updated": 0, "closed": 0, "stopped": 0}

    # OPEN 시그널 조회
    try:
        resp = client.table("signals").select("*").eq(
            "status", "OPEN"
        ).eq("bot_type", bot_type).execute()
        signals = resp.data or []
    except Exception as e:
        logger.error(f"[FLOWX] OPEN 시그널 조회 실패: {e}")
        return {"updated": 0, "closed": 0, "stopped": 0}

    if not signals:
        logger.info(f"[FLOWX] {bot_type} OPEN 시그널 없음")
        return {"updated": 0, "closed": 0, "stopped": 0}

    # KIS API로 현재가 조회
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    try:
        from bot.kis_trader import KISTrader
        trader = KISTrader()
    except Exception as e:
        logger.error(f"KIS 트레이더 초기화 실패: {e}")
        return {"updated": 0, "closed": 0, "stopped": 0}

    import time
    updated = closed = stopped = 0
    today_str = date.today().isoformat()

    for sig in signals:
        code = sig["ticker"]
        entry = sig["entry_price"]

        if entry <= 0:
            continue

        try:
            pi = trader.fetch_price(code)
            current = pi.get("current_price", 0) if pi else 0
            time.sleep(0.15)
        except Exception:
            continue

        if current <= 0:
            continue

        ret_pct = round((current - entry) / entry * 100, 2)

        # 상태 판정
        tp = sig.get("target_price", 0)
        sl = sig.get("stop_loss", 0)
        new_status = "OPEN"

        # QUANT: 목표가 도달 → CLOSED, 손절가 도달 → STOPPED
        if bot_type == "QUANT":
            if tp > 0 and current >= tp:
                new_status = "CLOSED"
                closed += 1
            elif sl > 0 and current <= sl:
                new_status = "STOPPED"
                stopped += 1
        # DAYTRADING: 별도 close_daytrading()에서 일괄 처리

        update_data = {
            "current_price": current,
            "return_pct": ret_pct,
        }

        if new_status != "OPEN":
            update_data["status"] = new_status
            update_data["close_date"] = today_str

        try:
            client.table("signals").update(update_data).eq(
                "id", sig["id"]
            ).execute()
            updated += 1
        except Exception as e:
            logger.warning(f"시그널 업데이트 실패 {code}: {e}")

    result = {"updated": updated, "closed": closed, "stopped": stopped}
    logger.info(f"[FLOWX] {bot_type} 성과 업데이트: {result}")
    return result


# ══════════════════════════════════════════
#  4. DAYTRADING 일괄 청산 (15:20 KST)
# ══════════════════════════════════════════

def close_daytrading() -> int:
    """DAYTRADING OPEN 시그널 전량 CLOSED 처리"""
    client = _get_client()
    if not client:
        return 0

    today_str = date.today().isoformat()

    try:
        # 먼저 현재가 업데이트
        update_performance("DAYTRADING")

        # 남은 OPEN 시그널 전량 CLOSED
        resp = client.table("signals").select("id").eq(
            "status", "OPEN"
        ).eq("bot_type", "DAYTRADING").execute()

        open_sigs = resp.data or []
        if not open_sigs:
            return 0

        for sig in open_sigs:
            client.table("signals").update({
                "status": "CLOSED",
                "close_date": today_str,
            }).eq("id", sig["id"]).execute()

        logger.info(f"[FLOWX] DAYTRADING {len(open_sigs)}건 일괄 청산")
        return len(open_sigs)
    except Exception as e:
        logger.error(f"[FLOWX] DAYTRADING 일괄 청산 실패: {e}")
        return 0


# ══════════════════════════════════════════
#  5. 오래된 QUANT 시그널 자동 청산
# ══════════════════════════════════════════

def close_expired_quant(max_days: int = 10) -> int:
    """보유기간 초과 QUANT OPEN 시그널 CLOSED 처리"""
    client = _get_client()
    if not client:
        return 0

    cutoff = (date.today() - timedelta(days=max_days)).isoformat()
    today_str = date.today().isoformat()

    try:
        resp = client.table("signals").select("id,signal_date").eq(
            "status", "OPEN"
        ).eq("bot_type", "QUANT").lte("signal_date", cutoff).execute()

        expired = resp.data or []
        if not expired:
            return 0

        for sig in expired:
            client.table("signals").update({
                "status": "CLOSED",
                "close_date": today_str,
            }).eq("id", sig["id"]).execute()

        logger.info(f"[FLOWX] QUANT 만기 청산 {len(expired)}건 (>{max_days}일)")
        return len(expired)
    except Exception as e:
        logger.error(f"[FLOWX] QUANT 만기 청산 실패: {e}")
        return 0


# ══════════════════════════════════════════
#  6. SCOREBOARD 집계
# ══════════════════════════════════════════

def aggregate_scoreboard(bot_type: str = "QUANT") -> dict:
    """30/60/90일 성과 집계 → scoreboard upsert

    Returns:
        {30: {...}, 60: {...}, 90: {...}}
    """
    client = _get_client()
    if not client:
        return {}

    results = {}

    for period in (30, 60, 90):
        cutoff = (date.today() - timedelta(days=period)).isoformat()

        try:
            # 해당 기간의 CLOSED/STOPPED 시그널 조회
            resp = client.table("signals").select(
                "ticker,ticker_name,return_pct,signal_date,status"
            ).eq("bot_type", bot_type).in_(
                "status", ["CLOSED", "STOPPED"]
            ).gte("signal_date", cutoff).execute()

            closed_sigs = resp.data or []

            total = len(closed_sigs)
            if total == 0:
                results[period] = _empty_scoreboard(bot_type, period)
                continue

            hits = sum(1 for s in closed_sigs if float(s["return_pct"]) > 0)
            avg_ret = round(
                sum(float(s["return_pct"]) for s in closed_sigs) / total, 2
            )

            # 최고/최저
            sorted_by_ret = sorted(
                closed_sigs, key=lambda x: float(x["return_pct"])
            )
            best = sorted_by_ret[-1] if sorted_by_ret else {}
            worst = sorted_by_ret[0] if sorted_by_ret else {}

            # 최근 5건
            recent = sorted(
                closed_sigs, key=lambda x: x["signal_date"], reverse=True
            )[:5]

            board = {
                "bot_type": bot_type,
                "period_days": period,
                "total_signals": total,
                "hit_count": hits,
                "hit_rate": round(hits / total * 100, 2) if total else 0,
                "avg_return_pct": avg_ret,
                "best_signal": {
                    "ticker": best.get("ticker", ""),
                    "name": best.get("ticker_name", ""),
                    "return_pct": float(best.get("return_pct", 0)),
                    "date": best.get("signal_date", ""),
                } if best else {},
                "worst_signal": {
                    "ticker": worst.get("ticker", ""),
                    "name": worst.get("ticker_name", ""),
                    "return_pct": float(worst.get("return_pct", 0)),
                    "date": worst.get("signal_date", ""),
                } if worst else {},
                "recent_closed": [
                    {
                        "ticker": r["ticker"],
                        "name": r["ticker_name"],
                        "return_pct": float(r["return_pct"]),
                        "date": r["signal_date"],
                    } for r in recent
                ],
                "updated_at": datetime.now().isoformat(),
            }

            client.table("scoreboard").upsert(
                [board], on_conflict="bot_type,period_days"
            ).execute()

            results[period] = board

        except Exception as e:
            logger.error(f"[FLOWX] scoreboard {period}일 집계 실패: {e}")
            results[period] = _empty_scoreboard(bot_type, period)

    logger.info(
        f"[FLOWX] {bot_type} scoreboard 집계 완료: "
        + ", ".join(f"{p}d={r.get('hit_rate', 0):.1f}%" for p, r in results.items())
    )
    return results


def _empty_scoreboard(bot_type: str, period: int) -> dict:
    return {
        "bot_type": bot_type,
        "period_days": period,
        "total_signals": 0,
        "hit_count": 0,
        "hit_rate": 0,
        "avg_return_pct": 0,
        "best_signal": {},
        "worst_signal": {},
        "recent_closed": [],
    }


# ══════════════════════════════════════════
#  7. 통합 실행 함수
# ══════════════════════════════════════════

def run_signal_update():
    """16:10 KST 실행: 성과 업데이트 + 만기 청산 + scoreboard 집계"""
    # 1) QUANT 성과 업데이트 (목표가/손절 자동 청산 포함)
    quant_result = update_performance("QUANT")

    # 2) 만기 초과 QUANT 청산
    expired = close_expired_quant(max_days=10)

    # 3) QUANT scoreboard 집계
    quant_board = aggregate_scoreboard("QUANT")

    # 4) DAYTRADING도 실행 (있으면)
    dt_result = update_performance("DAYTRADING")
    dt_board = aggregate_scoreboard("DAYTRADING")

    return {
        "quant": quant_result,
        "quant_expired": expired,
        "quant_scoreboard": quant_board,
        "daytrading": dt_result,
        "daytrading_scoreboard": dt_board,
    }


# ══════════════════════════════════════════
#  종목명 해석 헬퍼
# ══════════════════════════════════════════

def _resolve_name(code: str, raw_name: str) -> str:
    """코드가 이름으로 들어온 경우 pykrx로 해석"""
    if raw_name and not raw_name.isdigit():
        return raw_name
    try:
        from pykrx import stock
        resolved = stock.get_market_ticker_name(code)
        if resolved:
            return resolved
    except Exception:
        pass
    return raw_name or code


# ══════════════════════════════════════════
#  CLI
# ══════════════════════════════════════════

def _print_scoreboard(boards: dict):
    """콘솔 스코어보드 출력"""
    for period, b in sorted(boards.items()):
        if not b or not b.get("total_signals"):
            print(f"  {period}일: 데이터 없음")
            continue
        print(
            f"  {period}일: {b['total_signals']}건 | "
            f"적중 {b['hit_count']}건({b['hit_rate']:.1f}%) | "
            f"평균수익 {b['avg_return_pct']:+.2f}%"
        )
        best = b.get("best_signal", {})
        worst = b.get("worst_signal", {})
        if best.get("ticker"):
            print(f"    Best: {best['name']}({best['ticker']}) {best['return_pct']:+.2f}%")
        if worst.get("ticker"):
            print(f"    Worst: {worst['name']}({worst['ticker']}) {worst['return_pct']:+.2f}%")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    parser = argparse.ArgumentParser(description="FLOWX 시그널 트래킹")
    parser.add_argument("--log-test", action="store_true",
                        help="현재 추천 -> signals INSERT 테스트")
    parser.add_argument("--update-test", action="store_true",
                        help="OPEN 시그널 현재가 업데이트 테스트")
    parser.add_argument("--scoreboard", action="store_true",
                        help="scoreboard 집계 테스트")
    parser.add_argument("--close-dt", action="store_true",
                        help="DAYTRADING 일괄 청산")
    parser.add_argument("--full", action="store_true",
                        help="전체 파이프라인 (update + scoreboard)")
    args = parser.parse_args()

    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

    if args.log_test:
        count = log_quant_signals()
        print(f"\nQUANT 시그널 {count}건 로깅 완료")

    elif args.update_test:
        result = update_performance("QUANT")
        print(f"\nQUANT 성과 업데이트: {result}")

    elif args.scoreboard:
        print("\n[QUANT 스코어보드]")
        boards = aggregate_scoreboard("QUANT")
        _print_scoreboard(boards)

        print("\n[DAYTRADING 스코어보드]")
        boards_dt = aggregate_scoreboard("DAYTRADING")
        _print_scoreboard(boards_dt)

    elif args.close_dt:
        count = close_daytrading()
        print(f"\nDAYTRADING {count}건 일괄 청산")

    elif args.full:
        result = run_signal_update()
        print(f"\n전체 업데이트 결과:")
        print(f"  QUANT: {result['quant']}")
        print(f"  QUANT 만기청산: {result['quant_expired']}건")
        print(f"  DAYTRADING: {result['daytrading']}")

        print("\n[QUANT 스코어보드]")
        _print_scoreboard(result.get("quant_scoreboard", {}))

    else:
        parser.print_help()
