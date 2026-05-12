# -*- coding: utf-8 -*-
"""
상한가 엔진 시그널 + 성적표 Supabase 업로드
=============================================
1. intelligence_limit_up_signals   — 일일 시그널 + 감시풀
2. intelligence_limit_up_performance — 페이퍼 트레이딩 성적

FLOWX 웹 대시보드 → "상한가 엔진" 패널에 표시됨.

Usage:
    python -m data.upload_limit_up                # 전체 업로드
    python -m data.upload_limit_up --signals      # 시그널만
    python -m data.upload_limit_up --performance  # 성적표만
"""

import json
import logging
from datetime import date, datetime
from pathlib import Path

logger = logging.getLogger("BH.UploadLimitUp")

DATA_STORE = Path(__file__).resolve().parent.parent / "data_store"
LIMIT_UP_DIR = DATA_STORE / "limit_up"
SIGNALS_PATH = LIMIT_UP_DIR / "signals.json"
WATCHLIST_PATH = LIMIT_UP_DIR / "watchlist.json"

SOURCE = "limit_up"


def _load_json(path: Path):
    if path.exists():
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {}


# ═══════════════════════════════════════════════════
#  1. 시그널 업로드
# ═══════════════════════════════════════════════════

def upload_limit_up_signals() -> bool:
    """오늘 시그널 + 감시풀 → intelligence_limit_up_signals"""
    try:
        from data.upload_swing import _get_client
        client = _get_client()
        if not client:
            logger.error("Supabase 클라이언트 없음")
            return False

        today = date.today().isoformat()

        # 시그널 로드
        sig_data = _load_json(SIGNALS_PATH)
        signals = sig_data.get("signals", [])

        # 감시풀 로드
        wl_data = _load_json(WATCHLIST_PATH)
        items = wl_data.get("items", [])

        # 트리거 시그널 정리 (v3 수급 필드 포함)
        triggered = []
        for s in signals:
            triggered.append({
                "code": s.get("code"),
                "name": s.get("name"),
                "entry_type": s.get("entry_type"),
                "signal_date": s.get("signal_date"),
                "signal_close": s.get("signal_close"),
                "entry_price": s.get("entry_price"),
                "entry_low": s.get("entry_low"),
                "entry_high": s.get("entry_high"),
                "tp_price": s.get("tp_price"),
                "limit_count": s.get("limit_count"),
                "overheat_pct": s.get("overheat_pct"),
                "origin_price": s.get("origin_price"),
                "sector": s.get("sector"),
                "market_cap": s.get("market_cap"),
                "volume_ratio": s.get("volume_ratio"),
                "reasons": s.get("reasons", []),
                # v3 수급 분석 필드
                "flow_grade": s.get("flow_grade", ""),
                "flow_grade_label": s.get("flow_grade_label", ""),
                "flow_smart_total": s.get("flow_smart_total", 0),
                "sell_exhaustion": s.get("sell_exhaustion", 0),
                "vol_compression": s.get("vol_compression", 0),
                "survival_ok": s.get("survival_ok"),
                "survival_reason": s.get("survival_reason", ""),
                "rotation_phase": s.get("rotation_phase", ""),
                "rotation_favorable": s.get("rotation_favorable"),
                "split_plan": s.get("split_plan"),
            })

        # 감시풀 정리 (monitoring 상태)
        watchlist = []
        for w in items:
            if w.get("status") not in ("watching", "monitoring"):
                continue
            watchlist.append({
                "code": w.get("code"),
                "name": w.get("name"),
                "signal_close": w.get("signal_close"),
                "signal_date": w.get("signal_date"),
                "monitor_until": w.get("monitor_until"),
                "entry_type": w.get("entry_type"),
                "sector": w.get("sector"),
                "market_cap": w.get("market_cap"),
                # v3 수급 필드
                "flow_grade": w.get("flow_grade", ""),
                "flow_smart_total": w.get("flow_smart_total", 0),
                "survival_ok": w.get("survival_ok"),
                "rotation_phase": w.get("rotation_phase", ""),
            })

        row = {
            "date": today,
            "scanned_at": sig_data.get("generated_at", datetime.now().isoformat()),
            "total_scanned": sig_data.get("total_scanned", 0),
            "triggered": triggered,
            "triggered_count": len(triggered),
            "watchlist": watchlist,
            "watchlist_count": len(watchlist),
        }

        client.table("intelligence_limit_up_signals") \
            .upsert(row, on_conflict="date") \
            .execute()

        logger.info(
            f"시그널 업로드 완료: {today} · "
            f"트리거 {len(triggered)}건 · 감시 {len(watchlist)}건"
        )
        return True

    except Exception as e:
        err = str(e)
        if "does not exist" in err or "Could not find" in err:
            logger.warning(
                "intelligence_limit_up_signals 테이블 없음 — "
                "sql/intelligence_limit_up_migration.sql 실행 필요"
            )
        else:
            logger.error(f"시그널 업로드 실패: {e}")
        return False


# ═══════════════════════════════════════════════════
#  2. 성적표 업로드
# ═══════════════════════════════════════════════════

def upload_limit_up_performance() -> bool:
    """페이퍼 트레이딩 성적 → intelligence_limit_up_performance"""
    try:
        from data.upload_swing import _get_client
        client = _get_client()
        if not client:
            logger.error("Supabase 클라이언트 없음")
            return False

        from data.paper_portfolio import PaperPortfolio
        pf = PaperPortfolio()
        today = date.today().isoformat()

        # 상한가 포지션만 필터
        lu_positions = {
            code: pos for code, pos in pf.positions.items()
            if pos.get("source") == SOURCE
        }
        lu_closed = [
            t for t in pf.closed_trades
            if t.get("source") == SOURCE
        ]
        today_closed = [
            t for t in lu_closed
            if t.get("exit_date") == today
        ]

        # 포지션 JSON
        positions_json = []
        for code, pos in lu_positions.items():
            hold = pf._calc_hold_days(pos.get("entry_date", ""))
            positions_json.append({
                "code": code,
                "name": pos["name"],
                "entry_price": pos["entry_price"],
                "current_price": pos.get("current_price", pos["entry_price"]),
                "pnl_pct": pos.get("unrealized_pnl", 0),
                "hold_days": hold,
                "tp_price": pos.get("tp", 0),
                "entry_date": pos.get("entry_date"),
                "shares": pos.get("shares", 0),
            })

        # 청산 JSON
        closed_json = []
        for t in today_closed:
            closed_json.append({
                "code": t["code"],
                "name": t["name"],
                "entry_price": t["entry_price"],
                "exit_price": t["exit_price"],
                "pnl_pct": t["pnl_pct"],
                "pnl_krw": t.get("pnl_krw", 0),
                "reason": t["reason"],
                "hold_days": t.get("hold_days", 0),
            })

        # 누적 통계
        total = len(lu_closed)
        wins = sum(1 for t in lu_closed if t["pnl_pct"] > 0)
        avg_pnl = sum(t["pnl_pct"] for t in lu_closed) / total if total else 0

        # 전략별 통계 — paper_trade_log에서 entry 전략 분류 후 closed와 매칭
        s1_trades = 0
        s1_wins = 0
        s2_trades = 0
        s2_wins = 0

        paper_log_path = LIMIT_UP_DIR / "paper_trade_log.json"
        paper_log = _load_json(paper_log_path)
        # code → strategy 매핑 (최신 entry 기준)
        code_strategy = {}
        if isinstance(paper_log, list):
            for day_log in paper_log:
                for action in day_log.get("actions", []):
                    if action.get("type") == "entry":
                        code_strategy[action.get("code", "")] = action.get("strategy", "")

        # closed_trades 기반 전략별 trades/wins 집계
        for t in lu_closed:
            strat = code_strategy.get(t.get("code", ""), "")
            if strat == "next_day":
                s1_trades += 1
                if t.get("pnl_pct", 0) > 0:
                    s1_wins += 1
            elif strat == "pullback":
                s2_trades += 1
                if t.get("pnl_pct", 0) > 0:
                    s2_wins += 1

        # 포트폴리오 평가액
        holdings_value = sum(
            pos.get("current_price", pos["entry_price"]) * pos.get("shares", 1)
            for pos in lu_positions.values()
        )
        total_value = pf.cash + sum(
            pos.get("current_price", pos["entry_price"]) * pos.get("shares", 1)
            for pos in pf.positions.values()
        )
        cum_return = (total_value - pf.initial_cash) / pf.initial_cash * 100

        row = {
            "date": today,
            "new_entries": len([
                a for d in (paper_log if isinstance(paper_log, list) else [])
                for a in d.get("actions", [])
                if a.get("type") == "entry" and d.get("date") == today
            ]),
            "closed_count": len(today_closed),
            "active_count": len(lu_positions),
            "positions": positions_json,
            "closed_trades": closed_json,
            "total_trades": total,
            "win_rate": round(wins / total * 100, 1) if total else 0,
            "avg_pnl": round(avg_pnl, 2),
            "cum_return": round(cum_return, 2),
            "mdd": round(pf._calc_mdd(pf._load_daily_log()), 2),
            "strategy1_trades": s1_trades,
            "strategy1_wins": s1_wins,
            "strategy2_trades": s2_trades,
            "strategy2_wins": s2_wins,
            "cash": pf.cash,
            "total_value": total_value,
        }

        client.table("intelligence_limit_up_performance") \
            .upsert(row, on_conflict="date") \
            .execute()

        logger.info(
            f"성적표 업로드 완료: {today} · "
            f"보유 {len(lu_positions)}건 · 청산 {len(today_closed)}건 · "
            f"누적 {total}건 승률 {row['win_rate']:.0f}%"
        )
        return True

    except Exception as e:
        err = str(e)
        if "does not exist" in err or "Could not find" in err:
            logger.warning(
                "intelligence_limit_up_performance 테이블 없음 — "
                "sql/intelligence_limit_up_migration.sql 실행 필요"
            )
        else:
            logger.error(f"성적표 업로드 실패: {e}")
        return False


# ═══════════════════════════════════════════════════
#  통합 실행
# ═══════════════════════════════════════════════════

def upload_all() -> dict:
    """시그널 + 성적표 모두 업로드"""
    result = {
        "signals": upload_limit_up_signals(),
        "performance": upload_limit_up_performance(),
    }
    return result


if __name__ == "__main__":
    import sys
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(message)s",
        datefmt="%H:%M:%S",
        handlers=[logging.StreamHandler(sys.stdout)],
    )

    parser = argparse.ArgumentParser(description="상한가 FLOWX 업로드")
    parser.add_argument("--signals", action="store_true", help="시그널만 업로드")
    parser.add_argument("--performance", action="store_true", help="성적표만 업로드")
    args = parser.parse_args()

    if args.signals:
        upload_limit_up_signals()
    elif args.performance:
        upload_limit_up_performance()
    else:
        result = upload_all()
        print(f"업로드 결과: {result}")
