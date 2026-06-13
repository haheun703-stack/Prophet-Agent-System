# -*- coding: utf-8 -*-
"""Limit-up 3-day pilot planner.

This script does not place orders. It reads the existing limit-up engine output
and produces a D+0 entry / D+1~D+3 exit plan for review.

Usage:
    python tools/limit_up_3day_pilot.py
    python tools/limit_up_3day_pilot.py --budget 3000000 --max-positions 2
"""
from __future__ import annotations

import argparse
import csv
import json
from dataclasses import asdict, dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parent.parent
LIMIT_UP_DIR = ROOT / "data_store" / "limit_up"
RESULT_DIR = ROOT / "results" / "limit_up_3day_pilot"

SIGNALS_PATH = LIMIT_UP_DIR / "signals.json"
WATCHLIST_PATH = LIMIT_UP_DIR / "watchlist.json"


@dataclass
class CandidatePlan:
    code: str
    name: str
    source: str
    status: str
    rank_score: float
    entry_price: int
    entry_low: int
    entry_high: int
    continuation_score: float
    flow_grade: str
    flow_foreign_5d: int
    flow_inst_5d: int
    flow_smart_total: int
    smart_consec: int
    dual_buy: bool
    sell_exhaustion: float
    vol_compression: float
    foreign_exh_rate: float
    volume_ratio: float
    overheat_pct: float
    survival_ok: bool
    monitor_until: str
    selected: bool
    reject_reason: str
    entry_gate: str
    entry_gate_reason: str
    preflight_checks: list[dict[str, Any]]
    risk_plan: dict[str, Any]
    buy_plan: list[dict[str, Any]]
    sell_plan: list[dict[str, Any]]
    stop_plan: list[dict[str, Any]]
    chase_rules: list[str]


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        if value in (None, ""):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _as_int(value: Any, default: int = 0) -> int:
    return int(round(_as_float(value, default)))


def _today_iso() -> str:
    return date.today().isoformat()


def _date_prefix(value: Any) -> str:
    raw = str(value or "").strip()
    if len(raw) >= 10:
        return raw[:10]
    return raw


def _source_metadata(today: str) -> dict[str, dict[str, Any]]:
    signals = _load_json(SIGNALS_PATH)
    watchlist = _load_json(WATCHLIST_PATH)

    sig_ts = signals.get("generated_at", "")
    wl_ts = watchlist.get("updated_at", "")
    return {
        "signals": {
            "path": str(SIGNALS_PATH),
            "timestamp": sig_ts,
            "fresh": _date_prefix(sig_ts) == today,
            "count": len(signals.get("signals", []) or []),
        },
        "watchlist": {
            "path": str(WATCHLIST_PATH),
            "timestamp": wl_ts,
            "fresh": _date_prefix(wl_ts) == today,
            "count": len(watchlist.get("items", []) or []),
        },
    }


def _is_stale(item: dict[str, Any], today: str) -> bool:
    until = str(item.get("monitor_until") or "")
    if not until:
        return False
    return until < today


def _candidate_event_date(item: dict[str, Any]) -> str:
    triggered = _date_prefix(item.get("triggered_date"))
    if triggered:
        return triggered
    return _date_prefix(item.get("signal_date"))


def _quality_score(item: dict[str, Any], source: str, today: str) -> tuple[float, str]:
    reasons: list[str] = []
    score = 0.0

    continuation = _as_float(item.get("continuation_score"))
    flow_grade = str(item.get("flow_grade") or "")
    volume_ratio = _as_float(item.get("volume_ratio"))
    overheat = _as_float(item.get("overheat_pct"))
    survival_ok = bool(item.get("survival_ok", True))
    status = str(item.get("status") or "")

    score += continuation
    if source == "signals":
        score += 12
    if status == "triggered":
        score += 8
    if flow_grade == "S":
        score += 18
    elif flow_grade == "A":
        score += 12
    elif flow_grade == "B":
        score += 5
    elif flow_grade in {"C", "F"}:
        score -= 20
        reasons.append(f"weak flow grade {flow_grade}")

    if volume_ratio >= 5:
        score += 10
    elif volume_ratio >= 2:
        score += 5
    elif volume_ratio > 0 and volume_ratio < 1:
        score -= 5
        reasons.append("volume ratio below 1x")

    if not survival_ok:
        score -= 40
        reasons.append("flow survival failed")
    if overheat > 250:
        score -= 35
        reasons.append("overheat above 250%")
    elif overheat > 150:
        score -= 15
        reasons.append("overheat above 150%")
    if _is_stale(item, today):
        score -= 50
        reasons.append("monitor window stale")

    return round(score, 2), "; ".join(reasons)


def _candidate_rows() -> list[tuple[str, dict[str, Any]]]:
    rows: list[tuple[str, dict[str, Any]]] = []

    signals = _load_json(SIGNALS_PATH).get("signals", [])
    if isinstance(signals, list):
        for item in signals:
            if isinstance(item, dict):
                rows.append(("signals", item))

    watch_items = _load_json(WATCHLIST_PATH).get("items", [])
    if isinstance(watch_items, list):
        for item in watch_items:
            if isinstance(item, dict):
                rows.append(("watchlist", item))

    dedup: dict[str, tuple[str, dict[str, Any]]] = {}
    for source, item in rows:
        code = str(item.get("code") or "").strip()
        if not code:
            continue
        prev = dedup.get(code)
        if prev is None or source == "signals":
            dedup[code] = (source, item)
    return list(dedup.values())


def _buy_plan(per_symbol_budget: int, entry_price: int) -> list[dict[str, Any]]:
    if entry_price <= 0:
        entry_price = 1
    return [
        {
            "tranche": 1,
            "ratio_pct": 50,
            "budget": int(per_symbol_budget * 0.50),
            "condition": "D+0 14:45~15:05, price inside entry band and no market-risk block",
            "qty_hint": max(1, int((per_symbol_budget * 0.50) // entry_price)),
        },
        {
            "tranche": 2,
            "ratio_pct": 30,
            "budget": int(per_symbol_budget * 0.30),
            "condition": "controlled pullback: -1.5% to -3.0% from tranche-1, VWAP not broken hard",
            "qty_hint": max(1, int((per_symbol_budget * 0.30) // entry_price)),
        },
        {
            "tranche": 3,
            "ratio_pct": 20,
            "budget": int(per_symbol_budget * 0.20),
            "condition": "confirmation/chase: reclaim VWAP or break D0 high with volume and strength",
            "qty_hint": max(1, int((per_symbol_budget * 0.20) // entry_price)),
        },
    ]


def _sell_plan() -> list[dict[str, Any]]:
    return [
        {
            "day": "D+1",
            "base_sell_pct": 40,
            "window": "09:30~10:30",
            "override": "if open pnl <= -7%, sell 100%; if pnl >= +10%, sell 50% and trail rest",
        },
        {
            "day": "D+2",
            "base_sell_pct": 30,
            "window": "10:00~14:30",
            "override": "if new high continues, use -3% trailing stop instead of blind sell",
        },
        {
            "day": "D+3",
            "base_sell_pct": 30,
            "window": "14:30~15:10",
            "override": "time stop: close remaining position unless locked limit-up strength persists",
        },
    ]


def _stop_plan(entry_price: int) -> list[dict[str, Any]]:
    hard_stop = int(entry_price * 0.955) if entry_price > 0 else 0
    return [
        {
            "name": "initial hard stop",
            "trigger": "-4.5% from weighted average entry",
            "price_hint": hard_stop,
            "action": "sell all; do not average down below this line",
        },
        {
            "name": "D+1 gap protection",
            "trigger": "D+1 open pnl <= -7%",
            "action": "sell all at/after 09:01 after opening print stabilizes",
        },
        {
            "name": "profit trailing",
            "trigger": "after pnl >= +3%",
            "action": "trail high-water mark by -3%",
        },
        {
            "name": "failed chase protection",
            "trigger": "chase add fails and price falls back below VWAP",
            "action": "cancel remaining adds; reduce 30~50% if liquidity weakens",
        },
    ]


def _chase_rules() -> list[str]:
    return [
        "Only chase while total allocation is below 100%.",
        "Require price above VWAP, rising volume, and execution strength confirmation.",
        "Do not chase if price is already more than +12% above weighted entry.",
        "Do not chase if spread is wide, sell wall is heavy, or market risk is bearish.",
        "A chase add must inherit the same hard stop and trailing rules as the base position.",
    ]


def _check(name: str, status: str, detail: str) -> dict[str, str]:
    return {"name": name, "status": status, "detail": detail}


def _preflight_checks(
    *,
    source: str,
    source_meta: dict[str, dict[str, Any]],
    allow_stale: bool,
    score: float,
    min_score: float,
    entry_price: int,
    stale: bool,
    survival_ok: bool,
    flow_grade: str,
    volume_ratio: float,
    overheat_pct: float,
) -> list[dict[str, str]]:
    fresh = bool(source_meta.get(source, {}).get("fresh"))
    checks = [
        _check(
            "fresh_source",
            "PASS" if fresh else ("WARN" if allow_stale else "FAIL"),
            "same-day source data" if fresh else f"{source} source file is not today's data",
        ),
        _check(
            "score_threshold",
            "PASS" if score >= min_score else "FAIL",
            f"score {score:.1f} vs min {min_score:.1f}",
        ),
        _check(
            "entry_price",
            "PASS" if entry_price > 0 else "FAIL",
            f"reference entry {entry_price:,}" if entry_price > 0 else "missing reference entry price",
        ),
        _check(
            "monitor_window",
            "PASS" if not stale else "FAIL",
            "monitor window active" if not stale else "monitor window is stale",
        ),
        _check(
            "flow_survival",
            "PASS" if survival_ok else "FAIL",
            "flow survival ok" if survival_ok else "flow survival failed",
        ),
    ]

    if flow_grade in {"S", "A", "B"}:
        checks.append(_check("flow_grade", "PASS", f"flow grade {flow_grade}"))
    elif flow_grade:
        checks.append(_check("flow_grade", "FAIL", f"weak flow grade {flow_grade}"))
    else:
        checks.append(_check("flow_grade", "WARN", "flow grade missing"))

    if volume_ratio >= 2:
        checks.append(_check("volume_ratio", "PASS", f"volume {volume_ratio:.2f}x"))
    elif volume_ratio >= 1:
        checks.append(_check("volume_ratio", "WARN", f"volume {volume_ratio:.2f}x"))
    else:
        checks.append(_check("volume_ratio", "FAIL", f"volume {volume_ratio:.2f}x"))

    if overheat_pct > 250:
        checks.append(_check("overheat", "FAIL", f"overheat {overheat_pct:.1f}%"))
    elif overheat_pct > 150:
        checks.append(_check("overheat", "WARN", f"overheat {overheat_pct:.1f}%"))
    else:
        checks.append(_check("overheat", "PASS", f"overheat {overheat_pct:.1f}%"))

    checks.append(
        _check(
            "live_vwap_orderbook",
            "PENDING",
            "must pass live price/VWAP/orderbook check before any D+0 entry",
        )
    )
    return checks


def _risk_plan(per_symbol_budget: int, entry_price: int) -> dict[str, Any]:
    qty_full = max(0, int(per_symbol_budget // max(1, entry_price)))
    hard_stop_price = int(entry_price * 0.955) if entry_price > 0 else 0
    max_loss = int(per_symbol_budget * 0.045)
    return {
        "full_qty_hint": qty_full,
        "hard_stop_pct": -4.5,
        "hard_stop_price": hard_stop_price,
        "max_loss_hint": max_loss,
        "loss_rule": "If hard stop hits, exit all and do not average down.",
    }


def build_plans(
    budget: int,
    max_positions: int,
    min_score: float,
    allow_stale: bool = False,
) -> dict[str, Any]:
    today = _today_iso()
    per_symbol_budget = budget // max(1, max_positions)
    raw_rows = _candidate_rows()
    source_meta = _source_metadata(today)

    plans: list[CandidatePlan] = []
    for source, item in raw_rows:
        score, quality_reason = _quality_score(item, source, today)
        entry_price = _as_int(item.get("entry_price") or item.get("triggered_price") or item.get("signal_close"))
        entry_low = _as_int(item.get("entry_low") or entry_price)
        entry_high = _as_int(item.get("entry_high") or entry_price)
        survival_ok = bool(item.get("survival_ok", True))
        stale = _is_stale(item, today)
        event_date = _candidate_event_date(item)
        flow_grade = str(item.get("flow_grade") or "")
        volume_ratio = _as_float(item.get("volume_ratio"))
        overheat_pct = _as_float(item.get("overheat_pct"))

        reject_reasons: list[str] = []
        if score < min_score:
            reject_reasons.append(f"rank score {score:.1f} < {min_score:.1f}")
        if not survival_ok:
            reject_reasons.append("survival failed")
        if stale:
            reject_reasons.append("stale monitor window")
        if not allow_stale and not source_meta.get(source, {}).get("fresh", False):
            reject_reasons.append(f"{source} data is not today's data")
        if not allow_stale and event_date != today:
            reject_reasons.append(f"candidate event date {event_date or '-'} is not today")
        if entry_price <= 0:
            reject_reasons.append("missing entry price")
        if quality_reason:
            reject_reasons.append(quality_reason)

        plans.append(
            CandidatePlan(
                code=str(item.get("code") or ""),
                name=str(item.get("name") or item.get("code") or ""),
                source=source,
                status=str(item.get("status") or ""),
                rank_score=score,
                entry_price=entry_price,
                entry_low=entry_low,
                entry_high=entry_high,
                continuation_score=_as_float(item.get("continuation_score")),
                flow_grade=str(item.get("flow_grade") or ""),
                flow_foreign_5d=_as_int(item.get("flow_foreign_5d")),
                flow_inst_5d=_as_int(item.get("flow_inst_5d")),
                flow_smart_total=_as_int(item.get("flow_smart_total")),
                smart_consec=_as_int(item.get("smart_consec")),
                dual_buy=bool(item.get("dual_buy", False)),
                sell_exhaustion=_as_float(item.get("sell_exhaustion"), 1.0),
                vol_compression=_as_float(item.get("vol_compression"), 1.0),
                foreign_exh_rate=_as_float(item.get("foreign_exh_rate")),
                volume_ratio=volume_ratio,
                overheat_pct=overheat_pct,
                survival_ok=survival_ok,
                monitor_until=str(item.get("monitor_until") or ""),
                selected=False,
                reject_reason="; ".join(reject_reasons),
                entry_gate="BLOCKED" if reject_reasons else "WATCH_CAPACITY",
                entry_gate_reason="; ".join(reject_reasons) if reject_reasons else "passed paper filters; waiting for capacity",
                preflight_checks=_preflight_checks(
                    source=source,
                    source_meta=source_meta,
                    allow_stale=allow_stale,
                    score=score,
                    min_score=min_score,
                    entry_price=entry_price,
                    stale=stale,
                    survival_ok=survival_ok,
                    flow_grade=flow_grade,
                    volume_ratio=volume_ratio,
                    overheat_pct=overheat_pct,
                ),
                risk_plan=_risk_plan(per_symbol_budget, entry_price),
                buy_plan=_buy_plan(per_symbol_budget, entry_price),
                sell_plan=_sell_plan(),
                stop_plan=_stop_plan(entry_price),
                chase_rules=_chase_rules(),
            )
        )

    plans.sort(key=lambda p: p.rank_score, reverse=True)
    selected_count = 0
    for plan in plans:
        if not plan.reject_reason and selected_count < max_positions:
            plan.selected = True
            plan.entry_gate = "PAPER_READY_LIVE_PREFLIGHT_REQUIRED"
            plan.entry_gate_reason = "paper filters passed; live VWAP/orderbook gate is still required"
            selected_count += 1

    return {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "today": today,
        "mode": "PAPER_PLAN_ONLY",
        "budget": budget,
        "max_positions": max_positions,
        "per_symbol_budget": per_symbol_budget,
        "min_score": min_score,
        "allow_stale": allow_stale,
        "source_freshness": source_meta,
        "raw_candidates": len(raw_rows),
        "selected_count": selected_count,
        "plans": [asdict(p) for p in plans],
    }


def _fmt_money(value: int) -> str:
    return f"{value:,}"


def _status_icon(status: str) -> str:
    return {
        "PASS": "PASS",
        "WARN": "WARN",
        "FAIL": "FAIL",
        "PENDING": "PENDING",
    }.get(status, status)


def render_markdown(summary: dict[str, Any]) -> str:
    lines: list[str] = []
    lines.append("# Limit-up 3-day pilot")
    lines.append("")
    lines.append(f"- generated_at: {summary['generated_at']}")
    lines.append(f"- mode: {summary['mode']}")
    lines.append(f"- budget: {_fmt_money(summary['budget'])}")
    lines.append(f"- max_positions: {summary['max_positions']}")
    lines.append(f"- per_symbol_budget: {_fmt_money(summary['per_symbol_budget'])}")
    lines.append(f"- raw_candidates: {summary['raw_candidates']}")
    lines.append(f"- selected_count: {summary['selected_count']}")
    lines.append(f"- allow_stale: {summary['allow_stale']}")
    lines.append("")

    lines.append("## Data Freshness")
    for name, meta in summary["source_freshness"].items():
        status = "fresh" if meta["fresh"] else "stale"
        lines.append(f"- {name}: {status}, timestamp={meta['timestamp'] or '-'}, count={meta['count']}")
    if not all(meta["fresh"] for meta in summary["source_freshness"].values()):
        lines.append("")
        lines.append("Freshness gate is active. Run the limit-up engine before considering D+0 entries.")
    lines.append("")

    selected = [p for p in summary["plans"] if p["selected"]]
    rejected = [p for p in summary["plans"] if not p["selected"]]

    lines.append("## Selected")
    if not selected:
        lines.append("")
        lines.append("No selected candidates. Keep observation mode.")
    for p in selected:
        lines.extend(_render_plan(p))

    lines.append("")
    lines.append("## Rejected / Watch")
    for p in rejected[:20]:
        lines.append(
            f"- {p['code']} {p['name']} score={p['rank_score']:.1f} "
            f"source={p['source']} status={p['status']} reason={p['reject_reason'] or 'capacity'}"
        )
    if len(rejected) > 20:
        lines.append(f"- ... {len(rejected) - 20} more")

    lines.append("")
    lines.append("## Policy")
    lines.append("- D+0 entry is a plan, not an order. Live order wiring stays disabled until explicitly enabled.")
    lines.append("- Default buy split: 50% close-entry, 30% controlled pullback, 20% confirmed chase.")
    lines.append("- Default sell split: D+1 40%, D+2 30%, D+3 30%.")
    lines.append("- Loss control has priority over the calendar sell schedule.")
    lines.append("- A D+0 entry needs fresh same-day limit-up data plus a live price/VWAP/orderbook preflight.")
    return "\n".join(lines) + "\n"


def _render_plan(p: dict[str, Any]) -> list[str]:
    lines: list[str] = []
    lines.append("")
    lines.append(f"### {p['code']} {p['name']}")
    lines.append(
        f"- score={p['rank_score']:.1f}, source={p['source']}, status={p['status']}, "
        f"continuation={p['continuation_score']:.1f}, flow={p['flow_grade'] or '-'}, "
        f"volume={p['volume_ratio']:.2f}x"
    )
    lines.append(
        f"- flow: foreign_5d={p['flow_foreign_5d']:,}, inst_5d={p['flow_inst_5d']:,}, "
        f"smart_total={p['flow_smart_total']:,}, smart_consec={p['smart_consec']}, "
        f"dual_buy={p['dual_buy']}, sell_exhaustion={p['sell_exhaustion']:.2f}, "
        f"turnover_compression={p['vol_compression']:.2f}"
    )
    lines.append(f"- entry gate: {p['entry_gate']} :: {p['entry_gate_reason']}")
    lines.append(f"- entry band: {p['entry_low']:,} ~ {p['entry_high']:,} / ref {p['entry_price']:,}")
    risk = p["risk_plan"]
    lines.append(
        f"- risk: full_qty_hint={risk['full_qty_hint']}, hard_stop={risk['hard_stop_price']:,}, "
        f"max_loss_hint={risk['max_loss_hint']:,}"
    )
    lines.append("- preflight:")
    for c in p["preflight_checks"]:
        lines.append(f"  - {_status_icon(c['status'])} {c['name']}: {c['detail']}")
    lines.append("- buy split:")
    for b in p["buy_plan"]:
        lines.append(
            f"  - T{b['tranche']} {b['ratio_pct']}% budget={b['budget']:,} "
            f"qty_hint={b['qty_hint']} :: {b['condition']}"
        )
    lines.append("- sell split:")
    for s in p["sell_plan"]:
        lines.append(f"  - {s['day']} {s['base_sell_pct']}% {s['window']} :: {s['override']}")
    lines.append("- stops:")
    for s in p["stop_plan"]:
        hint = f" price_hint={s['price_hint']:,}" if s.get("price_hint") else ""
        lines.append(f"  - {s['name']} :: {s['trigger']} -> {s['action']}{hint}")
    return lines


def _write_action_csv(summary: dict[str, Any], csv_path: Path) -> None:
    fields = [
        "selected",
        "entry_gate",
        "code",
        "name",
        "source",
        "status",
        "rank_score",
        "entry_price",
        "entry_low",
        "entry_high",
        "full_qty_hint",
        "hard_stop_price",
        "max_loss_hint",
        "reject_reason",
        "entry_gate_reason",
    ]
    with csv_path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for p in summary["plans"]:
            risk = p["risk_plan"]
            writer.writerow(
                {
                    "selected": p["selected"],
                    "entry_gate": p["entry_gate"],
                    "code": p["code"],
                    "name": p["name"],
                    "source": p["source"],
                    "status": p["status"],
                    "rank_score": p["rank_score"],
                    "entry_price": p["entry_price"],
                    "entry_low": p["entry_low"],
                    "entry_high": p["entry_high"],
                    "full_qty_hint": risk["full_qty_hint"],
                    "hard_stop_price": risk["hard_stop_price"],
                    "max_loss_hint": risk["max_loss_hint"],
                    "reject_reason": p["reject_reason"],
                    "entry_gate_reason": p["entry_gate_reason"],
                }
            )


def save_outputs(summary: dict[str, Any]) -> tuple[Path, Path, Path]:
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    json_path = RESULT_DIR / f"limit_up_3day_pilot_{stamp}.json"
    md_path = RESULT_DIR / "limit_up_3day_latest.md"
    csv_path = RESULT_DIR / "limit_up_3day_action_sheet.csv"
    json_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(render_markdown(summary), encoding="utf-8")
    _write_action_csv(summary, csv_path)
    return json_path, md_path, csv_path


def main() -> int:
    parser = argparse.ArgumentParser(description="Build limit-up 3-day paper plan")
    parser.add_argument("--budget", type=int, default=3_000_000)
    parser.add_argument("--max-positions", type=int, default=2)
    parser.add_argument("--min-score", type=float, default=55.0)
    parser.add_argument(
        "--allow-stale",
        action="store_true",
        help="Allow planning from stale source files for historical review only.",
    )
    args = parser.parse_args()

    summary = build_plans(
        budget=max(1, args.budget),
        max_positions=max(1, args.max_positions),
        min_score=args.min_score,
        allow_stale=args.allow_stale,
    )
    json_path, md_path, csv_path = save_outputs(summary)

    print("limit-up 3-day pilot complete")
    print(f"mode: {summary['mode']}")
    print(f"candidates: {summary['raw_candidates']} total / {summary['selected_count']} selected")
    print(f"summary: {json_path}")
    print(f"latest report: {md_path}")
    print(f"action sheet: {csv_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
