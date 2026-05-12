# -*- coding: utf-8 -*-
"""
상한가 페이퍼 트레이더 (Limit-Up Paper Trader)
================================================
limit_up_engine 시그널 → PaperPortfolio 가상매매 자동 연결

흐름 (매일 장 마감 후 실행):
  1. signals.json에서 오늘 트리거된 시그널 로드
  2. 품질 필터: 과열도/수급생존/수급등급 체크 → 불량 시그널 스킵
  3. 전략1(즉시진입): 당일 종가 기준 진입가 범위 확인 → 가상매수
  4. 기존 상한가 포지션: 현재가 갱신 + TP/SL/만기 체크 → 가상매도
  5. watchlist에서 눌림목 트리거 체크 → 가상매수
  6. 결과 텔레그램 전송

Usage:
  python -m data.limit_up_paper_trader                  # 전체 실행
  python -m data.limit_up_paper_trader --status         # 상한가 포지션 현황
  python -m data.limit_up_paper_trader --no-telegram    # 텔레그램 미전송
"""
from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, date
from pathlib import Path
from typing import Optional

logger = logging.getLogger("BH.LimitUpPaper")

# ─── 경로 ────────────────────────────────────────
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_DIR = SCRIPT_DIR.parent
DATA_STORE = PROJECT_DIR / "data_store"
LIMIT_UP_DIR = DATA_STORE / "limit_up"
SIGNALS_PATH = LIMIT_UP_DIR / "signals.json"
WATCHLIST_PATH = LIMIT_UP_DIR / "watchlist.json"
PAPER_TRADE_LOG = LIMIT_UP_DIR / "paper_trade_log.json"

# ─── 상수 ────────────────────────────────────────
SOURCE = "limit_up"
# 백테스트 확정: TP +10%, SL -7%, 만기 20영업일
TP_PCT = 10.0
SL_PCT = 7.0
TIME_STOP_DAYS = 20
# 품질 필터 임계값
MIN_CONTINUATION_SCORE = 40.0  # 연속성 점수 하한 (0~19: 승률 0%, 20~39: 21%, 40+: 75%+)
MAX_OVERHEAT_PCT = 250.0  # 과열도 상한 (분석 결과 150→250 완화)
SKIP_FLOW_GRADES = ("C", "D")  # 수급 불량 등급
# 포지션당 최대 비중 (가상자금 1000만원 기준 ~200만원)
MAX_POSITION_PCT = 0.20
MAX_POSITIONS = 5  # 상한가 동시 보유 최대


def _load_json(path: Path) -> dict | list:
    if path.exists():
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logger.warning(f"JSON 로드 실패 {path.name}: {e}")
    return {}


def _save_json(path: Path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2, default=str)
    tmp.replace(path)


def _fetch_close_price(code: str, target_date: str = None) -> Optional[int]:
    """pykrx로 종가 조회 (장 마감 후 실행 전제)"""
    try:
        from pykrx import stock
        if target_date is None:
            target_date = date.today().strftime("%Y%m%d")
        else:
            target_date = target_date.replace("-", "")

        df = stock.get_market_ohlcv(target_date, target_date, code)
        if df.empty:
            return None
        return int(df.iloc[0]["종가"])
    except Exception as e:
        logger.warning(f"가격 조회 실패 {code}: {e}")
        return None


def _quality_filter(sig: dict) -> tuple[bool, str]:
    """시그널 품질 필터 — 진입 가능 여부 판단

    필터 우선순위 (613건 분석 기반):
      1. continuation_score >= 40 (0~19: 승률 0%, 40+: 75%+, 60+: 98%)
      2. overheat_pct <= 250%
      3. survival_ok == True
      4. flow_grade not in (C, D)

    Returns:
        (통과 여부, 스킵 사유)
    """
    # 1) 연속성 점수 체크 — 가장 강력한 예측 지표
    cont_score = sig.get("continuation_score", 0) or 0
    if cont_score < MIN_CONTINUATION_SCORE:
        return False, f"연속성 {cont_score:.0f} < {MIN_CONTINUATION_SCORE:.0f}"

    # 2) 과열도 250% 초과 → 스킵
    overheat = sig.get("overheat_pct", 0) or 0
    if overheat > MAX_OVERHEAT_PCT:
        return False, f"과열 {overheat:.0f}% > {MAX_OVERHEAT_PCT:.0f}%"

    # 3) 수급 생존 실패 → 스킵
    if sig.get("survival_ok") is False:
        return False, "수급 생존 실패"

    # 4) 수급 등급 C/D → 스킵
    flow_grade = sig.get("flow_grade", "")
    if flow_grade in SKIP_FLOW_GRADES:
        return False, f"수급 등급 {flow_grade} (불량)"

    return True, ""


# ═══════════════════════════════════════════════════
#  핵심 로직
# ═══════════════════════════════════════════════════

def run_paper_trading(send_telegram: bool = True) -> dict:
    """일일 페이퍼 트레이딩 실행

    Returns:
        결과 요약 dict
    """
    from data.paper_portfolio import PaperPortfolio

    logger.info("=" * 50)
    logger.info("상한가 페이퍼 트레이더 - 일일 실행")
    logger.info("=" * 50)

    pf = PaperPortfolio()
    today = date.today().strftime("%Y-%m-%d")
    result = {
        "date": today,
        "new_entries": 0,
        "closed": 0,
        "active": 0,
        "skipped": 0,
        "actions": [],
    }

    # ── 1. 기존 상한가 포지션 가격 갱신 + TP/만기 체크 ──
    closed_actions = _check_existing_positions(pf, today)
    result["closed"] = len(closed_actions)
    result["actions"].extend(closed_actions)

    # ── 2. 오늘 트리거된 시그널로 신규 진입 ──
    entry_actions, skipped = _process_new_signals(pf, today)
    result["new_entries"] = len(entry_actions)
    result["skipped"] = skipped
    result["actions"].extend(entry_actions)

    # ── 3. 감시풀 눌림목 트리거 체크 → 진입 ──
    pullback_actions, pb_skipped = _check_pullback_entries(pf, today)
    result["new_entries"] += len(pullback_actions)
    result["skipped"] += pb_skipped
    result["actions"].extend(pullback_actions)

    # ── 4. 현재 상한가 포지션 수 ──
    lu_positions = {
        code: pos for code, pos in pf.positions.items()
        if pos.get("source") == SOURCE
    }
    result["active"] = len(lu_positions)

    # ── 5. 이력 저장 ──
    _save_trade_log(result)

    # ── 6. 텔레그램 리포트 ──
    if result["actions"] and send_telegram:
        msg = _format_report(result, pf)
        _send_telegram(msg)
    elif result["actions"]:
        # 텔레그램 미전송이어도 콘솔 출력
        print(_format_report(result, pf))

    logger.info(
        f"완료: 신규진입 {result['new_entries']}건 | "
        f"청산 {result['closed']}건 | "
        f"보유 {result['active']}건 | "
        f"스킵 {result['skipped']}건"
    )
    return result


def _check_existing_positions(pf, today: str) -> list:
    """기존 상한가 포지션 가격 갱신 + TP/만기 체크"""
    actions = []
    lu_codes = [
        code for code, pos in pf.positions.items()
        if pos.get("source") == SOURCE
    ]

    for code in lu_codes:
        pos = pf.positions.get(code)
        if not pos:
            continue

        current = _fetch_close_price(code)
        if current is None:
            continue

        pos["current_price"] = current
        entry = pos["entry_price"]
        pnl_pct = (current - entry) / entry * 100 if entry > 0 else 0
        pos["unrealized_pnl"] = round(pnl_pct, 2)

        tp = pos.get("tp", 0)
        hold_days = pf._calc_hold_days(pos.get("entry_date", ""))

        # TP 도달 체크
        if tp > 0 and current >= tp:
            trade = pf.close_position(code, current, "TARGET")
            if trade:
                actions.append({
                    "type": "close",
                    "reason": "TARGET",
                    "code": code,
                    "name": pos["name"],
                    "entry": entry,
                    "exit": current,
                    "pnl_pct": trade["pnl_pct"],
                    "hold_days": hold_days,
                })
                logger.info(
                    f"[TARGET] {pos['name']} {entry:,}→{current:,} "
                    f"({trade['pnl_pct']:+.1f}%) D+{hold_days}"
                )
            continue

        # SL 도달 체크
        sl = pos.get("sl", 0)
        if sl > 1 and current <= sl:
            trade = pf.close_position(code, current, "STOP_LOSS")
            if trade:
                actions.append({
                    "type": "close",
                    "reason": "STOP_LOSS",
                    "code": code,
                    "name": pos["name"],
                    "entry": entry,
                    "exit": current,
                    "pnl_pct": trade["pnl_pct"],
                    "hold_days": hold_days,
                })
                logger.info(
                    f"[STOP_LOSS] {pos['name']} {entry:,}→{current:,} "
                    f"({trade['pnl_pct']:+.1f}%) D+{hold_days}"
                )
            continue

        # 만기(TIME_STOP) 체크
        if hold_days >= TIME_STOP_DAYS:
            trade = pf.close_position(code, current, "TIME_STOP")
            if trade:
                actions.append({
                    "type": "close",
                    "reason": "TIME_STOP",
                    "code": code,
                    "name": pos["name"],
                    "entry": entry,
                    "exit": current,
                    "pnl_pct": trade["pnl_pct"],
                    "hold_days": hold_days,
                })
                logger.info(
                    f"[TIME_STOP] {pos['name']} {entry:,}→{current:,} "
                    f"({trade['pnl_pct']:+.1f}%) D+{hold_days}"
                )

    pf._save()
    return actions


def _process_new_signals(pf, today: str) -> tuple[list, int]:
    """signals.json에서 전략1(즉시진입) 시그널 처리"""
    actions = []
    skipped = 0

    signals_data = _load_json(SIGNALS_PATH)
    if not signals_data:
        return actions, skipped

    signals = signals_data.get("signals", [])

    for sig in signals:
        if sig.get("entry_type") != "next_day":
            continue
        if sig.get("status") != "triggered":
            continue

        code = sig["code"]
        name = sig["name"]

        # 품질 필터 체크
        ok_quality, skip_reason = _quality_filter(sig)
        if not ok_quality:
            logger.info(f"[스킵] {name} — {skip_reason}")
            skipped += 1
            continue

        # 이미 보유 중이면 스킵
        if code in pf.positions:
            logger.info(f"[스킵] {name} 이미 보유 중")
            skipped += 1
            continue

        # 상한가 동시 보유 한도 체크
        lu_count = sum(
            1 for pos in pf.positions.values()
            if pos.get("source") == SOURCE
        )
        if lu_count >= MAX_POSITIONS:
            logger.info(f"[스킵] {name} — 상한가 보유 한도 {MAX_POSITIONS}건 도달")
            skipped += 1
            continue

        # 진입가: 추천 진입가(entry_price) 사용
        entry_price = sig.get("entry_price", 0)
        if entry_price <= 0:
            skipped += 1
            continue

        # 매수 수량 계산 (자금의 MAX_POSITION_PCT 이내)
        max_cost = int(pf.cash * MAX_POSITION_PCT)
        shares = max_cost // entry_price
        if shares <= 0:
            logger.info(f"[스킵] {name} 자금 부족")
            skipped += 1
            continue

        # TP / SL 계산
        tp_price = sig.get("tp_price", int(entry_price * (1 + TP_PCT / 100)))
        sl_price = int(entry_price * (1 - SL_PCT / 100))

        # 가상매수
        ok = pf.open_position(
            code=code,
            name=name,
            entry_price=entry_price,
            shares=shares,
            source=SOURCE,
            tp=tp_price,
            sl=sl_price,
            time_stop_days=TIME_STOP_DAYS,
        )

        if ok:
            cost = entry_price * shares
            actions.append({
                "type": "entry",
                "strategy": "next_day",
                "code": code,
                "name": name,
                "price": entry_price,
                "shares": shares,
                "cost": cost,
                "tp": tp_price,
                "limit_count": sig.get("limit_count", 0),
                "overheat_pct": sig.get("overheat_pct", 0),
            })
            logger.info(
                f"[진입-전략1] {name} {shares}주 @ {entry_price:,} "
                f"(TP {tp_price:,}) 상한가 {sig.get('limit_count', 0)}회차"
            )

    return actions, skipped


def _check_pullback_entries(pf, today: str) -> tuple[list, int]:
    """watchlist에서 눌림목 트리거 체크 → 가상매수"""
    actions = []
    skipped = 0

    watchlist_data = _load_json(WATCHLIST_PATH)
    if not watchlist_data:
        return actions, skipped

    items = watchlist_data.get("items", [])

    for item in items:
        if item.get("entry_type") != "pullback":
            continue
        if item.get("status") != "triggered":
            continue

        code = item["code"]
        name = item["name"]

        # 품질 필터 체크
        ok_quality, skip_reason = _quality_filter(item)
        if not ok_quality:
            logger.info(f"[스킵-눌림] {name} — {skip_reason}")
            skipped += 1
            continue

        # 이미 보유 중이면 스킵
        if code in pf.positions:
            skipped += 1
            continue

        # 한도 체크
        lu_count = sum(
            1 for pos in pf.positions.values()
            if pos.get("source") == SOURCE
        )
        if lu_count >= MAX_POSITIONS:
            skipped += 1
            continue

        # 현재가 조회
        current = _fetch_close_price(code)
        if current is None:
            skipped += 1
            continue

        # 눌림목 진입가 범위 확인
        signal_close = item.get("signal_close", 0)
        pullback_trigger = int(signal_close * 0.90)  # -10% 눌림
        pullback_floor = int(signal_close * 0.85)    # -15% 이하면 스킵

        if current > pullback_trigger:
            continue  # 아직 눌림 안 옴
        if current < pullback_floor:
            logger.info(f"[스킵] {name} 과대 하락 ({current:,} < {pullback_floor:,})")
            skipped += 1
            continue

        # 진입
        entry_price = current
        max_cost = int(pf.cash * MAX_POSITION_PCT)
        shares = max_cost // entry_price
        if shares <= 0:
            skipped += 1
            continue

        tp_price = int(entry_price * (1 + TP_PCT / 100))
        sl_price = int(entry_price * (1 - SL_PCT / 100))

        ok = pf.open_position(
            code=code,
            name=name,
            entry_price=entry_price,
            shares=shares,
            source=SOURCE,
            tp=tp_price,
            sl=sl_price,
            time_stop_days=TIME_STOP_DAYS,
        )

        if ok:
            actions.append({
                "type": "entry",
                "strategy": "pullback",
                "code": code,
                "name": name,
                "price": entry_price,
                "shares": shares,
                "cost": entry_price * shares,
                "tp": tp_price,
                "pullback_pct": round((current - signal_close) / signal_close * 100, 1),
            })
            logger.info(
                f"[진입-전략2] {name} {shares}주 @ {entry_price:,} "
                f"(눌림 {(current - signal_close) / signal_close * 100:.1f}%)"
            )

    return actions, skipped


# ═══════════════════════════════════════════════════
#  리포트 + 텔레그램
# ═══════════════════════════════════════════════════

def _format_report(result: dict, pf) -> str:
    """일일 페이퍼 트레이딩 리포트"""
    lines = [
        "━━ 상한가 Paper Trading ━━━━━━━━",
        f"날짜: {result['date']}",
    ]

    # 신규 진입
    entries = [a for a in result["actions"] if a["type"] == "entry"]
    if entries:
        lines.append(f"\n[신규 진입] {len(entries)}건")
        for e in entries:
            strat = "즉시" if e["strategy"] == "next_day" else "눌림"
            lines.append(
                f"  [{strat}] {e['name']}({e['code']}) "
                f"{e['shares']}주 @ {e['price']:,} (TP {e['tp']:,})"
            )

    # 청산
    closes = [a for a in result["actions"] if a["type"] == "close"]
    if closes:
        lines.append(f"\n[청산] {len(closes)}건")
        for c in closes:
            lines.append(
                f"  [{c['reason']}] {c['name']} "
                f"{c['entry']:,}→{c['exit']:,} ({c['pnl_pct']:+.1f}%) D+{c['hold_days']}"
            )

    # 현재 보유
    lu_positions = {
        code: pos for code, pos in pf.positions.items()
        if pos.get("source") == SOURCE
    }
    if lu_positions:
        lines.append(f"\n[보유중] {len(lu_positions)}건")
        for code, pos in lu_positions.items():
            hold = pf._calc_hold_days(pos.get("entry_date", ""))
            pnl = pos.get("unrealized_pnl", 0)
            lines.append(
                f"  {pos['name']} {pnl:+.1f}% D+{hold}/{TIME_STOP_DAYS}"
            )

    # 상한가 전용 통계
    lu_closed = [
        t for t in pf.closed_trades
        if t.get("source") == SOURCE
    ]
    if lu_closed:
        wins = sum(1 for t in lu_closed if t["pnl_pct"] > 0)
        avg = sum(t["pnl_pct"] for t in lu_closed) / len(lu_closed)
        lines.append(
            f"\n[상한가 누적] {len(lu_closed)}건 | "
            f"승률 {wins/len(lu_closed)*100:.0f}% | 평균 {avg:+.1f}%"
        )

    lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    lines.append("* 가상매매 — 실제 주문 없음")
    return "\n".join(lines)


def _send_telegram(message: str):
    """텔레그램 전송"""
    try:
        from output.telegram_alert import TelegramAlert
        tg = TelegramAlert()
        if tg.enabled:
            tg._send(message)
    except Exception as e:
        logger.warning(f"텔레그램 전송 실패: {e}")


def _save_trade_log(result: dict):
    """일일 거래 이력 누적"""
    log = _load_json(PAPER_TRADE_LOG)
    if not isinstance(log, list):
        log = []
    log.append(result)
    # 최근 90일만 유지
    if len(log) > 90:
        log = log[-90:]
    _save_json(PAPER_TRADE_LOG, log)


def get_paper_status() -> str:
    """상한가 페이퍼 포지션 현황"""
    from data.paper_portfolio import PaperPortfolio
    pf = PaperPortfolio()

    lu_positions = {
        code: pos for code, pos in pf.positions.items()
        if pos.get("source") == SOURCE
    }
    lu_closed = [
        t for t in pf.closed_trades
        if t.get("source") == SOURCE
    ]

    lines = ["━━ 상한가 Paper 현황 ━━━━━━━━"]

    if lu_positions:
        lines.append(f"\n보유: {len(lu_positions)}건")
        for code, pos in lu_positions.items():
            hold = pf._calc_hold_days(pos.get("entry_date", ""))
            pnl = pos.get("unrealized_pnl", 0)
            cp = pos.get("current_price", pos["entry_price"])
            lines.append(
                f"  {pos['name']}({code}) {pos['entry_price']:,}→{cp:,} "
                f"({pnl:+.1f}%) D+{hold}/{TIME_STOP_DAYS}"
            )
    else:
        lines.append("\n보유 포지션 없음")

    if lu_closed:
        wins = sum(1 for t in lu_closed if t["pnl_pct"] > 0)
        total_pnl = sum(t["pnl_krw"] for t in lu_closed)
        avg = sum(t["pnl_pct"] for t in lu_closed) / len(lu_closed)
        lines.append(
            f"\n청산 이력: {len(lu_closed)}건 | "
            f"승률 {wins/len(lu_closed)*100:.0f}% | "
            f"평균 {avg:+.1f}% | 누적 {total_pnl:+,}원"
        )
        # 최근 5건 상세
        lines.append("최근 청산:")
        for t in lu_closed[-5:]:
            lines.append(
                f"  {t['name']} {t['pnl_pct']:+.1f}% "
                f"({t['reason']}) D+{t.get('hold_days', 0)}"
            )
    else:
        lines.append("\n청산 이력 없음")

    lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    return "\n".join(lines)


# ═══════════════════════════════════════════════════
#  CLI
# ═══════════════════════════════════════════════════

if __name__ == "__main__":
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(message)s",
        datefmt="%H:%M:%S",
        handlers=[logging.StreamHandler(sys.stdout)],
    )

    parser = argparse.ArgumentParser(description="상한가 페이퍼 트레이더")
    parser.add_argument("--status", action="store_true", help="현재 포지션 현황")
    parser.add_argument("--no-telegram", action="store_true", help="텔레그램 미전송")
    args = parser.parse_args()

    if args.status:
        print(get_paper_status())
    else:
        run_paper_trading(send_telegram=not args.no_telegram)
