# -*- coding: utf-8 -*-
"""Read-only/paper position manager for limit-up D+0 entries.

This tool manages the part after a D+0 entry:

    D+0: record/sync a paper or manual entry
    D+1: gap protection, profit lock, base 40% sell plan
    D+2: base 30% sell plan or trailing extension when strength remains
    D+3: time stop on remaining quantity unless limit-up strength persists

It does not place orders. It writes a local ledger and action reports so the
sell plan is auditable before any future order wiring is considered.

Usage:
    python tools/limit_up_position_manager.py
    python tools/limit_up_position_manager.py --offline
    python tools/limit_up_position_manager.py --record-entry 149950 --name 아바텍 --qty 10 --entry-price 16100
    python tools/limit_up_position_manager.py --paper-enter-ready
    python tools/limit_up_position_manager.py --apply-paper-actions
"""
from __future__ import annotations

import argparse
import csv
import json
import math
import sys
from dataclasses import asdict, dataclass
from datetime import date, datetime, time
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data_store" / "limit_up"
LEDGER_PATH = DATA_DIR / "limit_up_position_ledger.json"
RESULT_DIR = ROOT / "results" / "limit_up_position_manager"
JOURNAL_PATH = RESULT_DIR / "limit_up_position_journal_v1.csv"
PREFLIGHT_DIR = ROOT / "results" / "limit_up_preflight"

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

try:
    from zoneinfo import ZoneInfo

    KST = ZoneInfo("Asia/Seoul")
except Exception:  # pragma: no cover - Windows py<3.9 fallback
    KST = None

try:
    from data.trading_calendar import get_trading_days_between, is_trading_day
except Exception:  # pragma: no cover
    get_trading_days_between = None
    is_trading_day = None

# ★ Rule Registry 단일진실 — trailing/hard_stop/익절·갭다운 임계는 SAJANG 경유 (하드코딩 금지) ★
# (E402: 위 trading_calendar try/except 이후 import 의도적 — sys.path 셋업 후 로드)
from data.sajang_rules import SAJANG


@dataclass
class PositionAction:
    code: str
    name: str
    day_label: str
    day_offset: int
    status: str
    action: str
    action_qty: int
    action_pct_of_remaining: float
    priority: str
    reason: str
    entry_date: str
    avg_entry_price: int
    initial_qty: int
    remaining_qty: int
    sold_qty: int
    current_price: int
    pnl_pct: float
    high_water_price: int
    trail_stop_price: int
    hard_stop_price: int
    vwap: int
    vwap_source: str
    vwap_gap_pct: float
    change_rate: float
    volume: int
    strength: float
    spread_pct: float
    checks: list[dict[str, Any]]


def _now() -> datetime:
    return datetime.now(KST) if KST else datetime.now()


def _today() -> date:
    return _now().date()


def _as_int(value: Any, default: int = 0) -> int:
    try:
        if value in (None, ""):
            return default
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        if value in (None, ""):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _pct(current: int, base: int) -> float:
    if current <= 0 or base <= 0:
        return 0.0
    return round((current / base - 1) * 100, 2)


def _round_price(value: float) -> int:
    return int(round(value))


def _parse_date(value: Any, default: date | None = None) -> date:
    raw = str(value or "").strip()[:10]
    if raw:
        try:
            return date.fromisoformat(raw)
        except ValueError:
            pass
    return default or _today()


def _load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def _save_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def _empty_ledger() -> dict[str, Any]:
    return {
        "version": 1,
        "strategy": "limit_up_d0_d1_d3",
        "created_at": _now().isoformat(timespec="seconds"),
        "updated_at": "",
        "positions": {},
        "closed_positions": [],
        "events": [],
    }


def load_ledger() -> dict[str, Any]:
    ledger = _load_json(LEDGER_PATH, _empty_ledger())
    if not isinstance(ledger, dict):
        ledger = _empty_ledger()
    ledger.setdefault("positions", {})
    ledger.setdefault("closed_positions", [])
    ledger.setdefault("events", [])
    ledger.setdefault("version", 1)
    ledger.setdefault("strategy", "limit_up_d0_d1_d3")
    return ledger


def save_ledger(ledger: dict[str, Any]) -> None:
    ledger["updated_at"] = _now().isoformat(timespec="seconds")
    _save_json(LEDGER_PATH, ledger)


def _event(ledger: dict[str, Any], code: str, event_type: str, detail: dict[str, Any]) -> None:
    ledger.setdefault("events", []).append(
        {
            "time": _now().isoformat(timespec="seconds"),
            "code": code,
            "event_type": event_type,
            "detail": detail,
        }
    )


def _trading_day_offset(entry: date, today: date) -> int:
    if today <= entry:
        return 0
    if get_trading_days_between and is_trading_day:
        try:
            days = get_trading_days_between(entry, today)
            if entry not in days and is_trading_day(entry):
                days = [entry] + days
            return max(0, len(days) - 1)
        except Exception:
            pass

    days = 0
    d = entry
    while d < today:
        d = date.fromordinal(d.toordinal() + 1)
        if d.weekday() < 5:
            days += 1
    return days


def _day_label(offset: int) -> str:
    return f"D+{max(0, offset)}"


def _time_gte(t: time, hh: int, mm: int) -> bool:
    return t >= time(hh, mm)


def _time_between(t: time, start_hh: int, start_mm: int, end_hh: int, end_mm: int) -> bool:
    return time(start_hh, start_mm) <= t <= time(end_hh, end_mm)


def _ceil_qty(qty: int, ratio: float) -> int:
    if qty <= 0 or ratio <= 0:
        return 0
    return max(1, int(math.ceil(qty * ratio)))


def _target_qty(pos: dict[str, Any], pct_of_initial: float) -> int:
    initial = _as_int(pos.get("initial_qty"))
    remaining = _as_int(pos.get("remaining_qty"))
    return min(remaining, _ceil_qty(initial, pct_of_initial / 100.0))


def _load_config() -> dict[str, Any]:
    path = ROOT / "config.yaml"
    if not path.exists():
        return {}
    try:
        import yaml

        return yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except Exception:
        return {}


def _make_trader(live: bool) -> tuple[Any, str]:
    if not live:
        return None, ""
    try:
        from bot.kis_trader import KISTrader

        return KISTrader(_load_config()), ""
    except Exception as exc:
        return None, f"{type(exc).__name__}: {exc}"


def _fetch_live_snapshot(code: str, trader: Any, live: bool) -> tuple[dict[str, Any], dict[str, Any]]:
    if not live or trader is None:
        return {}, {}

    try:
        price_info = trader.fetch_price(code) or {}
    except Exception as exc:
        price_info = {"success": False, "message": f"{type(exc).__name__}: {exc}"}

    try:
        spread_info = trader.check_spread(code) or {}
    except Exception as exc:
        spread_info = {"ok": None, "message": f"{type(exc).__name__}: {exc}"}

    return price_info, spread_info


def _price_fields(pos: dict[str, Any], price_info: dict[str, Any]) -> dict[str, Any]:
    entry = _as_int(pos.get("avg_entry_price"))
    fallback = _as_int(pos.get("last_price") or entry)
    current = _as_int(price_info.get("current_price") if price_info.get("success") else fallback, fallback)
    high = _as_int(price_info.get("high"), current)
    high_water = max(_as_int(pos.get("high_water_price") or entry), current, high)
    vwap = _as_int(price_info.get("vwap"))
    vwap_source = str(price_info.get("vwap_source") or "")
    if vwap <= 0:
        vwap = _as_int(pos.get("last_vwap"))
        vwap_source = str(pos.get("last_vwap_source") or "UNAVAILABLE")
    vwap_gap = _pct(current, vwap) if vwap > 0 else 0.0
    trail_stop = _round_price(high_water * (1 - SAJANG.TRAILING_PCT / 100)) if high_water > entry else 0

    return {
        "current_price": current,
        "high_water_price": high_water,
        "trail_stop_price": trail_stop,
        "vwap": vwap,
        "vwap_source": vwap_source or "UNAVAILABLE",
        "vwap_gap_pct": vwap_gap,
        "change_rate": _as_float(price_info.get("change_rate")),
        "volume": _as_int(price_info.get("volume")),
        "strength": _as_float(price_info.get("strength")),
    }


def _update_mark_to_market(pos: dict[str, Any], fields: dict[str, Any]) -> None:
    pos["last_seen_at"] = _now().isoformat(timespec="seconds")
    pos["last_price"] = fields["current_price"]
    pos["last_pnl_pct"] = _pct(fields["current_price"], _as_int(pos.get("avg_entry_price")))
    pos["high_water_price"] = fields["high_water_price"]
    pos["trail_stop_price"] = fields["trail_stop_price"]
    pos["last_vwap"] = fields["vwap"]
    pos["last_vwap_source"] = fields["vwap_source"]
    if SAJANG.is_trailing_activated(pos["last_pnl_pct"]) or _as_int(pos.get("day_offset", 0)) >= 1:
        pos["trailing_active"] = True


def record_entry(
    ledger: dict[str, Any],
    *,
    code: str,
    name: str,
    qty: int,
    entry_price: int,
    entry_date: date,
    source: str,
    note: str = "",
) -> dict[str, Any]:
    if qty <= 0:
        raise ValueError("qty must be positive")
    if entry_price <= 0:
        raise ValueError("entry_price must be positive")

    positions = ledger.setdefault("positions", {})
    now_iso = _now().isoformat(timespec="seconds")
    existing = positions.get(code)
    cost = qty * entry_price

    if existing and existing.get("status") == "OPEN":
        old_qty = _as_int(existing.get("remaining_qty"))
        old_initial = _as_int(existing.get("initial_qty"))
        old_avg = _as_int(existing.get("avg_entry_price"))
        new_remaining = old_qty + qty
        new_initial = old_initial + qty
        new_avg = _round_price(((old_avg * old_qty) + cost) / max(1, new_remaining))
        existing["avg_entry_price"] = new_avg
        existing["initial_qty"] = new_initial
        existing["remaining_qty"] = new_remaining
        existing["hard_stop_price"] = _round_price(new_avg * (1 - SAJANG.LIMIT_UP_HARD_STOP_PCT / 100))
        existing["high_water_price"] = max(_as_int(existing.get("high_water_price")), entry_price)
        existing.setdefault("buy_tranches", []).append(
            {"time": now_iso, "qty": qty, "price": entry_price, "source": source, "note": note}
        )
        _event(ledger, code, "ADD_ENTRY", {"qty": qty, "price": entry_price, "source": source, "note": note})
        return existing

    pos = {
        "code": code,
        "name": name or code,
        "status": "OPEN",
        "source": source,
        "entry_date": entry_date.isoformat(),
        "entry_time": now_iso,
        "avg_entry_price": entry_price,
        "initial_qty": qty,
        "remaining_qty": qty,
        "sold_qty": 0,
        "cost": cost,
        "hard_stop_price": _round_price(entry_price * (1 - SAJANG.LIMIT_UP_HARD_STOP_PCT / 100)),
        "high_water_price": entry_price,
        "trail_stop_price": 0,
        "trailing_active": False,
        "last_price": entry_price,
        "last_pnl_pct": 0.0,
        "flags": {
            "profit_half_sold": False,
            "d1_base_sold": False,
            "d2_base_sold": False,
            "d3_final_done": False,
        },
        "buy_tranches": [{"time": now_iso, "qty": qty, "price": entry_price, "source": source, "note": note}],
        "sell_events": [],
    }
    positions[code] = pos
    _event(ledger, code, "OPEN_ENTRY", {"qty": qty, "price": entry_price, "source": source, "note": note})
    return pos


def _latest_preflight_json() -> Path | None:
    if not PREFLIGHT_DIR.exists():
        return None
    files = sorted(PREFLIGHT_DIR.glob("limit_up_preflight_*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    return files[0] if files else None


def paper_enter_ready(ledger: dict[str, Any], entry_ratio_pct: int) -> list[dict[str, Any]]:
    latest = _latest_preflight_json()
    if latest is None:
        return []
    summary = _load_json(latest, {})
    added: list[dict[str, Any]] = []
    for d in summary.get("decisions", []):
        if d.get("decision") != "ENTRY_READY":
            continue
        code = str(d.get("code") or "")
        price = _as_int(d.get("current_price"))
        if not code or price <= 0:
            continue
        full_qty = _as_int((d.get("risk_plan") or {}).get("full_qty_hint"))
        qty = max(1, int(full_qty * max(1, entry_ratio_pct) / 100)) if full_qty > 0 else 1
        pos = record_entry(
            ledger,
            code=code,
            name=str(d.get("name") or code),
            qty=qty,
            entry_price=price,
            entry_date=_today(),
            source="paper_entry_ready",
            note=f"from {latest.name}; ratio={entry_ratio_pct}%",
        )
        added.append(pos)
    return added


def _check(name: str, status: str, detail: str) -> dict[str, str]:
    return {"name": name, "status": status, "detail": detail}


def _classify_position(
    pos: dict[str, Any],
    fields: dict[str, Any],
    spread_info: dict[str, Any],
    *,
    now: datetime,
) -> PositionAction:
    entry_date = _parse_date(pos.get("entry_date"))
    day_offset = _trading_day_offset(entry_date, now.date())
    pos["day_offset"] = day_offset
    day_label = _day_label(day_offset)

    entry = _as_int(pos.get("avg_entry_price"))
    current = fields["current_price"]
    remaining = _as_int(pos.get("remaining_qty"))
    initial = _as_int(pos.get("initial_qty"))
    sold = _as_int(pos.get("sold_qty"))
    hard_stop = _as_int(pos.get("hard_stop_price")) or _round_price(entry * (1 - SAJANG.LIMIT_UP_HARD_STOP_PCT / 100))
    pnl = _pct(current, entry)
    flags = pos.setdefault("flags", {})
    spread_pct = _as_float(spread_info.get("spread_pct"))
    live_ok = current > 0
    t = now.time()

    checks: list[dict[str, str]] = [
        _check("position_status", "PASS" if remaining > 0 else "FAIL", f"remaining_qty={remaining}"),
        _check("day_bucket", "PASS", day_label),
        _check("live_price", "PASS" if live_ok else "PENDING", f"current={current:,}" if live_ok else "no live/current price"),
        _check("hard_stop", "PASS" if current > hard_stop else "FAIL", f"hard_stop={hard_stop:,}, pnl={pnl:+.2f}%"),
    ]

    if fields["vwap"] > 0:
        checks.append(_check("vwap", "PASS" if fields["vwap_gap_pct"] >= 0 else "WARN", f"{fields['vwap_source']} gap={fields['vwap_gap_pct']:+.2f}%"))
    else:
        checks.append(_check("vwap", "PENDING", "VWAP unavailable"))

    if spread_info:
        status = "PASS" if spread_info.get("ok") is not False else "WARN"
        checks.append(_check("spread", status, f"spread={spread_pct:.2f}%"))
    else:
        checks.append(_check("spread", "PENDING", "spread not checked"))

    def make(action: str, qty: int, priority: str, reason: str, status: str = "OPEN") -> PositionAction:
        pct_remaining = round(qty / remaining * 100, 1) if remaining > 0 and qty > 0 else 0.0
        return PositionAction(
            code=str(pos.get("code") or ""),
            name=str(pos.get("name") or pos.get("code") or ""),
            day_label=day_label,
            day_offset=day_offset,
            status=status,
            action=action,
            action_qty=max(0, min(remaining, qty)),
            action_pct_of_remaining=pct_remaining,
            priority=priority,
            reason=reason,
            entry_date=entry_date.isoformat(),
            avg_entry_price=entry,
            initial_qty=initial,
            remaining_qty=remaining,
            sold_qty=sold,
            current_price=current,
            pnl_pct=pnl,
            high_water_price=fields["high_water_price"],
            trail_stop_price=fields["trail_stop_price"],
            hard_stop_price=hard_stop,
            vwap=fields["vwap"],
            vwap_source=fields["vwap_source"],
            vwap_gap_pct=fields["vwap_gap_pct"],
            change_rate=fields["change_rate"],
            volume=fields["volume"],
            strength=fields["strength"],
            spread_pct=round(spread_pct, 2),
            checks=checks,
        )

    if remaining <= 0:
        return make("NONE", 0, "closed", "position has no remaining quantity", "CLOSED")

    if not live_ok:
        return make("WAIT_PRICE", 0, "wait", "price unavailable; do not issue a sell instruction")

    if current <= hard_stop or SAJANG.is_limit_up_hard_stop_breached(pnl):
        return make("SELL_ALL", remaining, "risk", f"hard stop breached: current={current:,}, stop={hard_stop:,}, pnl={pnl:+.2f}%")

    if day_offset == 1 and _time_gte(t, 9, 1) and pnl <= SAJANG.D1_GAP_SELL_THRESHOLD:
        return make("SELL_ALL", remaining, "risk", f"D+1 gap protection: pnl={pnl:+.2f}% <= {SAJANG.D1_GAP_SELL_THRESHOLD:.2f}%")

    high_water = fields["high_water_price"]
    trail_stop = fields["trail_stop_price"]
    trailing_active = bool(pos.get("trailing_active")) or SAJANG.is_trailing_activated(pnl) or day_offset >= 1
    if trailing_active and trail_stop > 0:
        checks.append(_check("trailing", "PASS", f"high_water={high_water:,}, trail_stop={trail_stop:,}"))
        if current <= trail_stop and high_water > entry:
            return make("SELL_ALL", remaining, "risk", f"trailing stop hit: current={current:,} <= {trail_stop:,}")
    else:
        checks.append(_check("trailing", "PENDING", "trailing not active yet"))

    if day_offset >= 1 and pnl >= SAJANG.RULE_B_THRESHOLD and not flags.get("profit_half_sold"):
        qty = min(remaining, _ceil_qty(initial, 0.50))
        return make("SELL_PARTIAL", qty, "profit_lock", f"profit lock: pnl={pnl:+.2f}% >= +{SAJANG.RULE_B_THRESHOLD:.0f}%; sell 50% and trail rest")

    if day_offset == 0:
        if _time_between(t, 14, 45, 15, 20):
            return make("HOLD", 0, "d0_manage", "D+0 entry day; only hard stop/trailing risk rules are active")
        return make("HOLD", 0, "d0_manage", "D+0 position; wait for close-entry management window")

    if day_offset == 1:
        if not flags.get("d1_base_sold"):
            if _time_gte(t, 9, 30):
                qty = _target_qty(pos, 40)
                return make("SELL_PARTIAL", qty, "calendar_d1", "D+1 base plan: sell 40% unless profit-lock/risk rule already fired")
            return make("WAIT", 0, "calendar_d1", "D+1 before 09:30 base sell window")
        return make("HOLD_TRAIL", 0, "calendar_d1", "D+1 base sell already marked; trail remaining quantity")

    if day_offset == 2:
        strong_extension = (
            fields["vwap"] > 0
            and current >= fields["vwap"]
            and (current >= high_water * 0.99 or fields["change_rate"] >= 5.0)
        )
        if strong_extension:
            return make("HOLD_TRAIL", 0, "calendar_d2", "D+2 strength extension: above VWAP/new-high area; trail instead of blind sell")
        if not flags.get("d2_base_sold") and _time_gte(t, 10, 0):
            qty = _target_qty(pos, 30)
            return make("SELL_PARTIAL", qty, "calendar_d2", "D+2 base plan: sell 30% because strength extension is absent")
        return make("WAIT", 0, "calendar_d2", "D+2 before sell window or already sold; keep trailing watch")

    locked_limit_strength = fields["change_rate"] >= 25.0 and fields["vwap"] > 0 and current >= fields["vwap"]
    if day_offset >= 3:
        if locked_limit_strength:
            return make("HOLD_TRAIL", 0, "calendar_d3", "D+3 exception: locked limit-up strength persists; trail remaining")
        if _time_gte(t, 14, 30) or day_offset > 3:
            return make("SELL_ALL", remaining, "calendar_d3", "D+3 time stop: close remaining quantity")
        return make("WAIT", 0, "calendar_d3", "D+3 wait until 14:30~15:10 time-stop window")

    return make("HOLD", 0, "hold", "no sell rule fired")


def _apply_paper_action(ledger: dict[str, Any], action: PositionAction) -> None:
    if action.action not in {"SELL_PARTIAL", "SELL_ALL"} or action.action_qty <= 0:
        return
    pos = ledger.get("positions", {}).get(action.code)
    if not pos:
        return

    remaining = _as_int(pos.get("remaining_qty"))
    qty = min(remaining, action.action_qty)
    if qty <= 0:
        return

    pos["remaining_qty"] = remaining - qty
    pos["sold_qty"] = _as_int(pos.get("sold_qty")) + qty
    flags = pos.setdefault("flags", {})
    if action.priority == "profit_lock":
        flags["profit_half_sold"] = True
    elif action.priority == "calendar_d1":
        flags["d1_base_sold"] = True
    elif action.priority == "calendar_d2":
        flags["d2_base_sold"] = True
    elif action.priority == "calendar_d3":
        flags["d3_final_done"] = True

    sell_event = {
        "time": _now().isoformat(timespec="seconds"),
        "action": action.action,
        "qty": qty,
        "price": action.current_price,
        "pnl_pct": action.pnl_pct,
        "priority": action.priority,
        "reason": action.reason,
    }
    pos.setdefault("sell_events", []).append(sell_event)
    _event(ledger, action.code, "PAPER_SELL", sell_event)

    if pos["remaining_qty"] <= 0:
        pos["status"] = "CLOSED"
        pos["closed_at"] = _now().isoformat(timespec="seconds")
        ledger.setdefault("closed_positions", []).append(pos)
        ledger.get("positions", {}).pop(action.code, None)


def build_position_report(args: argparse.Namespace) -> dict[str, Any]:
    ledger_missing = not LEDGER_PATH.exists()
    ledger = load_ledger()

    if args.record_entry:
        record_entry(
            ledger,
            code=args.record_entry,
            name=args.name or args.record_entry,
            qty=max(1, args.qty),
            entry_price=max(1, args.entry_price),
            entry_date=_parse_date(args.entry_date),
            source=args.source,
            note=args.note or "manual ledger entry",
        )
        save_ledger(ledger)

    paper_added = []
    if args.paper_enter_ready:
        paper_added = paper_enter_ready(ledger, args.entry_ratio_pct)
        if paper_added:
            save_ledger(ledger)

    live = not args.offline
    trader, trader_error = _make_trader(live)
    now = _now()
    actions: list[PositionAction] = []

    for code, pos in list(ledger.get("positions", {}).items()):
        if pos.get("status") != "OPEN":
            continue
        price_info, spread_info = _fetch_live_snapshot(code, trader, live)
        if live and trader is None:
            price_info = {"success": False, "message": trader_error}
        fields = _price_fields(pos, price_info)
        _update_mark_to_market(pos, fields)
        action = _classify_position(pos, fields, spread_info, now=now)
        actions.append(action)

    if actions or ledger_missing:
        save_ledger(ledger)

    if args.apply_paper_actions:
        for action in actions:
            _apply_paper_action(ledger, action)
        save_ledger(ledger)

    counts: dict[str, int] = {}
    for a in actions:
        counts[a.action] = counts.get(a.action, 0) + 1

    return {
        "generated_at": now.isoformat(timespec="seconds"),
        "mode": "READ_ONLY_POSITION_MANAGER",
        "live_read": live,
        "offline": args.offline,
        "trader_error": trader_error,
        "ledger_path": str(LEDGER_PATH),
        "paper_added": len(paper_added),
        "open_positions": len(ledger.get("positions", {})),
        "closed_positions": len(ledger.get("closed_positions", [])),
        "action_counts": counts,
        "actions": [asdict(a) for a in actions],
        "policy": {
            "d1_base_sell_pct": 40,
            "d2_base_sell_pct": 30,
            "d3_remaining_time_stop": True,
            "hard_stop_pct": -SAJANG.LIMIT_UP_HARD_STOP_PCT,
            "d1_gap_protection_pct": SAJANG.D1_GAP_SELL_THRESHOLD,
            "profit_lock_pct": SAJANG.RULE_B_THRESHOLD,
            "trailing_pct": SAJANG.TRAILING_PCT,
            "apply_paper_actions": bool(args.apply_paper_actions),
        },
    }


def render_markdown(summary: dict[str, Any]) -> str:
    lines: list[str] = []
    lines.append("# Limit-up position manager")
    lines.append("")
    lines.append(f"- generated_at: {summary['generated_at']}")
    lines.append(f"- mode: {summary['mode']}")
    lines.append(f"- live_read: {summary['live_read']}")
    lines.append(f"- open_positions: {summary['open_positions']}")
    lines.append(f"- closed_positions: {summary['closed_positions']}")
    lines.append(f"- action_counts: {summary['action_counts']}")
    lines.append(f"- ledger: {summary['ledger_path']}")
    if summary.get("trader_error"):
        lines.append(f"- trader_error: {summary['trader_error']}")
    lines.append("")
    lines.append("## Actions")
    if not summary["actions"]:
        lines.append("")
        lines.append("No open limit-up D+0 positions are recorded in the ledger.")
    for a in summary["actions"]:
        lines.append("")
        lines.append(f"### {a['code']} {a['name']}")
        lines.append(
            f"- decision: {a['action']} qty={a['action_qty']} "
            f"({a['action_pct_of_remaining']:.1f}% remaining) / priority={a['priority']}"
        )
        lines.append(f"- reason: {a['reason']}")
        lines.append(
            f"- position: {a['day_label']}, entry={a['avg_entry_price']:,}, "
            f"initial={a['initial_qty']}, remaining={a['remaining_qty']}, sold={a['sold_qty']}"
        )
        lines.append(
            f"- live: price={a['current_price']:,}, pnl={a['pnl_pct']:+.2f}%, "
            f"high_water={a['high_water_price']:,}, trail_stop={a['trail_stop_price']:,}, "
            f"hard_stop={a['hard_stop_price']:,}"
        )
        lines.append(
            f"- vwap={a['vwap']:,} ({a['vwap_source']}), "
            f"vwap_gap={a['vwap_gap_pct']:+.2f}%, change={a['change_rate']:+.2f}%, "
            f"volume={a['volume']:,}, strength={a['strength']:.1f}, spread={a['spread_pct']:.2f}%"
        )
        lines.append("- checks:")
        for c in a["checks"]:
            lines.append(f"  - {c['status']} {c['name']}: {c['detail']}")

    lines.append("")
    lines.append("## Policy")
    lines.append("- This tool does not place, modify, or cancel real orders.")
    lines.append("- Quantity changes are recorded only with --apply-paper-actions.")
    lines.append("- Hard stop and trailing risk rules override calendar D+1/D+2/D+3 sells.")
    lines.append("- D+1 base: 40%, D+2 base: 30%, D+3: remaining time stop unless limit-up strength persists.")
    return "\n".join(lines) + "\n"


def _write_csv(summary: dict[str, Any], path: Path) -> None:
    fields = [
        "action",
        "action_qty",
        "priority",
        "reason",
        "code",
        "name",
        "day_label",
        "entry_date",
        "avg_entry_price",
        "initial_qty",
        "remaining_qty",
        "sold_qty",
        "current_price",
        "pnl_pct",
        "high_water_price",
        "trail_stop_price",
        "hard_stop_price",
        "vwap",
        "vwap_source",
        "vwap_gap_pct",
        "change_rate",
        "volume",
        "strength",
        "spread_pct",
    ]
    with path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for a in summary["actions"]:
            writer.writerow({k: a.get(k, "") for k in fields})


def _append_journal(summary: dict[str, Any]) -> None:
    fields = [
        "generated_at",
        "action",
        "action_qty",
        "priority",
        "code",
        "name",
        "day_label",
        "avg_entry_price",
        "remaining_qty",
        "current_price",
        "pnl_pct",
        "high_water_price",
        "trail_stop_price",
        "hard_stop_price",
        "vwap",
        "vwap_source",
        "vwap_gap_pct",
        "reason",
    ]
    write_header = not JOURNAL_PATH.exists()
    with JOURNAL_PATH.open("a", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        if write_header:
            writer.writeheader()
        for a in summary["actions"]:
            row = {k: a.get(k, "") for k in fields}
            row["generated_at"] = summary["generated_at"]
            writer.writerow(row)


def save_outputs(summary: dict[str, Any]) -> tuple[Path, Path, Path]:
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    stamp = _now().strftime("%Y%m%d_%H%M%S")
    json_path = RESULT_DIR / f"limit_up_position_manager_{stamp}.json"
    md_path = RESULT_DIR / "limit_up_position_latest.md"
    csv_path = RESULT_DIR / "limit_up_position_action_sheet.csv"
    json_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(render_markdown(summary), encoding="utf-8")
    _write_csv(summary, csv_path)
    _append_journal(summary)
    return json_path, md_path, csv_path


def print_action_summary(summary: dict[str, Any]) -> None:
    actions = summary.get("actions", [])
    if not actions:
        print("position detail: no open limit-up positions")
        return
    print("position detail:")
    for a in actions:
        print(
            f"- {a.get('code')} {a.get('name')}: {a.get('action')} "
            f"qty={a.get('action_qty')} | {a.get('day_label')} | "
            f"price={_as_int(a.get('current_price')):,} | pnl={_as_float(a.get('pnl_pct')):+.2f}% | "
            f"trail={_as_int(a.get('trail_stop_price')):,} | reason={a.get('reason')}"
        )


def main() -> int:
    parser = argparse.ArgumentParser(description="Manage read-only/paper limit-up D+0 positions")
    parser.add_argument("--offline", action="store_true", help="Skip live KIS reads and use ledger prices.")
    parser.add_argument("--record-entry", metavar="CODE", help="Record a D+0/manual/paper entry in the ledger.")
    parser.add_argument("--name", default="", help="Name for --record-entry.")
    parser.add_argument("--qty", type=int, default=0, help="Quantity for --record-entry.")
    parser.add_argument("--entry-price", type=int, default=0, help="Entry price for --record-entry.")
    parser.add_argument("--entry-date", default="", help="Entry date YYYY-MM-DD; default today.")
    parser.add_argument("--source", default="manual_limit_up_d0", help="Entry source label.")
    parser.add_argument("--note", default="", help="Entry note.")
    parser.add_argument(
        "--paper-enter-ready",
        action="store_true",
        help="Create paper ledger entries from latest ENTRY_READY preflight decisions.",
    )
    parser.add_argument(
        "--entry-ratio-pct",
        type=int,
        default=50,
        help="When --paper-enter-ready is used, enter this percent of full plan quantity.",
    )
    parser.add_argument(
        "--apply-paper-actions",
        action="store_true",
        help="Apply recommended SELL_PARTIAL/SELL_ALL actions to the paper ledger. Never places real orders.",
    )
    args = parser.parse_args()

    if args.record_entry and (args.qty <= 0 or args.entry_price <= 0):
        parser.error("--record-entry requires --qty and --entry-price")

    summary = build_position_report(args)
    json_path, md_path, csv_path = save_outputs(summary)

    print("limit-up position manager complete")
    print(f"mode: {summary['mode']}")
    print(f"live_read: {summary['live_read']}")
    print(f"open positions: {summary['open_positions']}")
    print(f"actions: {summary['action_counts']}")
    print_action_summary(summary)
    print(f"summary: {json_path}")
    print(f"latest report: {md_path}")
    print(f"action sheet: {csv_path}")
    print(f"ledger: {LEDGER_PATH}")
    print(f"journal: {JOURNAL_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
