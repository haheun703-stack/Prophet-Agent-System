# -*- coding: utf-8 -*-
"""Read-only realtime preflight for limit-up D+0 entries.

This tool never places orders. It combines the paper plan from
tools/limit_up_3day_pilot.py with optional KIS read-only price/spread checks
and writes a report that classifies each candidate as:

    ENTRY_READY / WAIT / BLOCKED / REVIEW_ONLY

Usage:
    python tools/limit_up_realtime_preflight.py
    python tools/limit_up_realtime_preflight.py --offline
    python tools/limit_up_realtime_preflight.py --allow-stale --offline
"""
from __future__ import annotations

import argparse
import csv
import json
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, time
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parent.parent
RESULT_DIR = ROOT / "results" / "limit_up_preflight"
JOURNAL_PATH = RESULT_DIR / "limit_up_preflight_journal_v2.csv"

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.limit_up_3day_pilot import build_plans  # noqa: E402


@dataclass
class GateCheck:
    name: str
    status: str
    detail: str
    hard_block: bool = False


@dataclass
class PreflightDecision:
    code: str
    name: str
    source: str
    selected_by_plan: bool
    plan_score: float
    plan_gate: str
    decision: str
    action: str
    reason: str
    flow_grade: str
    flow_foreign_5d: int
    flow_inst_5d: int
    flow_smart_total: int
    smart_consec: int
    dual_buy: bool
    sell_exhaustion: float
    vol_compression: float
    current_price: int
    change_rate: float
    volume: int
    strength: float
    open_price: int
    high_price: int
    low_price: int
    vwap_proxy: int
    vwap_source: str
    vwap_gap_pct: float
    spread_pct: float
    bid_ask_ratio: float
    checks: list[dict[str, Any]]
    buy_plan: list[dict[str, Any]]
    sell_plan: list[dict[str, Any]]
    risk_plan: dict[str, Any]


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


def _market_time_check(now: datetime) -> GateCheck:
    t = now.time()
    if t < time(9, 0) or t > time(15, 20):
        return GateCheck(
            "d0_time_window",
            "FAIL",
            f"outside KRX entry review window ({now.strftime('%H:%M')})",
            True,
        )
    if time(14, 45) <= t <= time(15, 5):
        return GateCheck("d0_time_window", "PASS", f"inside D+0 close-entry window ({now.strftime('%H:%M')})")
    if t < time(14, 45):
        return GateCheck("d0_time_window", "PENDING", f"wait for 14:45~15:05 window ({now.strftime('%H:%M')})")
    return GateCheck("d0_time_window", "WARN", f"late entry review window ({now.strftime('%H:%M')})")


def _vwap_value(price_info: dict[str, Any]) -> tuple[int, float, str]:
    current = _as_int(price_info.get("current_price"))
    live_vwap = _as_int(price_info.get("vwap"))
    if current > 0 and live_vwap > 0:
        gap = (current / live_vwap - 1) * 100
        return live_vwap, round(gap, 2), str(price_info.get("vwap_source") or "KIS_WEIGHTED_AVG")

    opn = _as_int(price_info.get("open"))
    high = _as_int(price_info.get("high"))
    low = _as_int(price_info.get("low"))
    if min(current, opn, high, low) <= 0:
        return 0, 0.0, "UNAVAILABLE"
    proxy = int(round((opn + high + low + current) / 4))
    gap = (current / proxy - 1) * 100 if proxy > 0 else 0.0
    return proxy, round(gap, 2), "OHLC_PROXY"


def _add_price_checks(
    checks: list[GateCheck],
    plan: dict[str, Any],
    price_info: dict[str, Any],
    *,
    max_chase_premium_pct: float,
    max_vwap_gap_pct: float,
    min_strength: float,
    min_volume: int,
) -> tuple[int, float]:
    current = _as_int(price_info.get("current_price"))
    ref = _as_int(plan.get("entry_price"))
    change_rate = _as_float(price_info.get("change_rate"))
    volume = _as_int(price_info.get("volume"))
    strength = _as_float(price_info.get("strength"))

    if current <= 0:
        checks.append(GateCheck("live_price", "FAIL", "current price is missing", True))
        return 0, 0.0
    checks.append(GateCheck("live_price", "PASS", f"current={current:,}, change={change_rate:+.2f}%"))

    if ref > 0:
        premium = (current / ref - 1) * 100
        if premium > max_chase_premium_pct:
            checks.append(
                GateCheck(
                    "price_vs_plan",
                    "FAIL",
                    f"premium {premium:+.2f}% > max chase {max_chase_premium_pct:.1f}%",
                    True,
                )
            )
        elif premium > 1.5:
            checks.append(GateCheck("price_vs_plan", "WARN", f"above plan by {premium:+.2f}%; wait pullback or confirmation"))
        elif premium <= -4.5:
            checks.append(GateCheck("price_vs_plan", "FAIL", f"below plan by {premium:+.2f}%; hard stop zone", True))
        elif premium < -3.0:
            checks.append(GateCheck("price_vs_plan", "WARN", f"deep pullback {premium:+.2f}%; no average-down"))
        else:
            checks.append(GateCheck("price_vs_plan", "PASS", f"within controlled band ({premium:+.2f}%)"))
    else:
        checks.append(GateCheck("price_vs_plan", "FAIL", "plan reference price is missing", True))

    if volume >= min_volume:
        checks.append(GateCheck("volume", "PASS", f"volume={volume:,} >= {min_volume:,}"))
    elif volume > 0:
        checks.append(GateCheck("volume", "WARN", f"thin volume={volume:,} < {min_volume:,}"))
    else:
        checks.append(GateCheck("volume", "PENDING", "volume is not available"))

    if strength >= min_strength:
        checks.append(GateCheck("execution_strength", "PASS", f"strength={strength:.1f} >= {min_strength:.1f}"))
    elif strength >= 90:
        checks.append(GateCheck("execution_strength", "WARN", f"strength={strength:.1f}; not strong enough yet"))
    elif strength > 0:
        checks.append(GateCheck("execution_strength", "FAIL", f"weak strength={strength:.1f}", True))
    else:
        checks.append(GateCheck("execution_strength", "PENDING", "strength is not available"))

    vwap, gap, source = _vwap_value(price_info)
    gate_name = "vwap" if source != "OHLC_PROXY" else "vwap_proxy"
    if vwap <= 0:
        checks.append(GateCheck("vwap", "PENDING", "VWAP is unavailable"))
    elif gap < -0.7:
        checks.append(GateCheck(gate_name, "FAIL", f"below {source} by {gap:+.2f}%", True))
    elif gap < 0:
        checks.append(GateCheck(gate_name, "WARN", f"below {source} by {gap:+.2f}%; wait reclaim"))
    elif gap > max_vwap_gap_pct:
        checks.append(GateCheck(gate_name, "WARN", f"too far above {source} {gap:+.2f}%"))
    else:
        checks.append(GateCheck(gate_name, "PASS", f"above {source} by {gap:+.2f}%"))

    if change_rate >= 25.0:
        checks.append(GateCheck("limit_up_zone", "WARN", f"limit-up zone change={change_rate:+.2f}%; no blind chase"))
    else:
        checks.append(GateCheck("limit_up_zone", "PASS", f"not locked limit-up zone ({change_rate:+.2f}%)"))

    return vwap, gap


def _add_flow_checks(checks: list[GateCheck], plan: dict[str, Any]) -> None:
    grade = str(plan.get("flow_grade") or "")
    smart_total = _as_int(plan.get("flow_smart_total"))
    foreign_5d = _as_int(plan.get("flow_foreign_5d"))
    inst_5d = _as_int(plan.get("flow_inst_5d"))
    smart_consec = _as_int(plan.get("smart_consec"))
    dual_buy = bool(plan.get("dual_buy", False))
    sell_exhaustion = _as_float(plan.get("sell_exhaustion"), 1.0)
    vol_compression = _as_float(plan.get("vol_compression"), 1.0)

    if grade in {"S", "A"}:
        checks.append(GateCheck("flow_grade", "PASS", f"flow grade {grade}"))
    elif grade == "B":
        checks.append(GateCheck("flow_grade", "WARN", "neutral flow grade B"))
    elif grade:
        checks.append(GateCheck("flow_grade", "FAIL", f"weak/outflow grade {grade}", True))
    else:
        checks.append(GateCheck("flow_grade", "PENDING", "flow grade missing"))

    if smart_total > 0:
        checks.append(
            GateCheck(
                "smart_money_5d",
                "PASS",
                f"smart_total={smart_total:,}, foreign={foreign_5d:,}, inst={inst_5d:,}",
            )
        )
    elif smart_total < 0:
        hard = grade in {"C", "F"}
        checks.append(
            GateCheck(
                "smart_money_5d",
                "FAIL" if hard else "WARN",
                f"smart money outflow {smart_total:,}",
                hard,
            )
        )
    else:
        checks.append(GateCheck("smart_money_5d", "WARN", "smart money is flat/unknown"))

    if dual_buy:
        checks.append(GateCheck("dual_buy", "PASS", "foreign+institution dual buy detected"))
    elif smart_consec >= 2:
        checks.append(GateCheck("dual_buy", "WARN", f"smart buy streak {smart_consec}d without dual buy"))
    else:
        checks.append(GateCheck("dual_buy", "WARN", f"weak smart buy streak {smart_consec}d"))

    if sell_exhaustion <= 0.8:
        checks.append(GateCheck("sell_exhaustion", "PASS", f"selling exhausted ratio={sell_exhaustion:.2f}"))
    elif sell_exhaustion <= 1.2:
        checks.append(GateCheck("sell_exhaustion", "WARN", f"selling not fully exhausted ratio={sell_exhaustion:.2f}"))
    else:
        checks.append(GateCheck("sell_exhaustion", "FAIL", f"selling pressure remains ratio={sell_exhaustion:.2f}", True))

    if vol_compression <= 0.85:
        checks.append(GateCheck("flow_turnover", "PASS", f"flow turnover compressed={vol_compression:.2f}"))
    elif vol_compression <= 1.5:
        checks.append(GateCheck("flow_turnover", "WARN", f"normal/high flow turnover={vol_compression:.2f}"))
    else:
        checks.append(GateCheck("flow_turnover", "WARN", f"hot flow turnover={vol_compression:.2f}; avoid late chase"))


def _add_spread_checks(checks: list[GateCheck], spread_info: dict[str, Any]) -> tuple[float, float]:
    if not spread_info:
        checks.append(GateCheck("orderbook_spread", "PENDING", "spread/orderbook was not checked"))
        return 0.0, 0.0

    spread = _as_float(spread_info.get("spread_pct"))
    bid_ask_ratio = _as_float(spread_info.get("bid_ask_ratio"))
    has_l1 = _as_int(spread_info.get("ask")) > 0 and _as_int(spread_info.get("bid")) > 0

    if spread_info.get("ok") is False:
        checks.append(GateCheck("orderbook_spread", "FAIL", str(spread_info.get("message", "spread blocked")), True))
    elif has_l1:
        checks.append(GateCheck("orderbook_spread", "PASS", f"spread={spread:.2f}%"))
    else:
        checks.append(GateCheck("orderbook_spread", "PENDING", str(spread_info.get("message", "orderbook unavailable"))))

    if bid_ask_ratio >= 100:
        checks.append(GateCheck("bid_ask_ratio", "PASS", f"bid/ask={bid_ask_ratio:.1f}"))
    elif bid_ask_ratio >= 80:
        checks.append(GateCheck("bid_ask_ratio", "WARN", f"bid/ask={bid_ask_ratio:.1f}; not dominant"))
    elif bid_ask_ratio > 0:
        checks.append(GateCheck("bid_ask_ratio", "FAIL", f"sell pressure bid/ask={bid_ask_ratio:.1f}", True))
    else:
        checks.append(GateCheck("bid_ask_ratio", "PENDING", "bid/ask ratio unavailable"))

    return round(spread, 2), round(bid_ask_ratio, 1)


def _classify(checks: list[GateCheck], *, review_only: bool) -> tuple[str, str, str]:
    hard_failures = [c.detail for c in checks if c.status == "FAIL" and c.hard_block]
    failures = [c.detail for c in checks if c.status == "FAIL"]
    warnings = [c.detail for c in checks if c.status == "WARN"]
    pending = [c.detail for c in checks if c.status == "PENDING"]

    if review_only:
        reason = "; ".join(hard_failures or failures or warnings or pending or ["historical review mode"])
        return "REVIEW_ONLY", "NO_ORDER_REVIEW_ONLY", reason
    if hard_failures:
        return "BLOCKED", "NO_ORDER_BLOCKED", "; ".join(hard_failures[:3])
    if failures:
        return "BLOCKED", "NO_ORDER_BLOCKED", "; ".join(failures[:3])
    if pending:
        return "WAIT", "NO_ORDER_WAIT", "; ".join(pending[:3])
    if warnings:
        return "WAIT", "NO_ORDER_WAIT", "; ".join(warnings[:3])
    return "ENTRY_READY", "PAPER_ENTRY_ONLY", "all read-only preflight checks passed"


def _evaluate_plan(
    plan: dict[str, Any],
    *,
    source_freshness: dict[str, dict[str, Any]],
    trader: Any,
    live: bool,
    trader_error: str,
    now: datetime,
    allow_stale: bool,
    args: argparse.Namespace,
) -> PreflightDecision:
    checks: list[GateCheck] = []
    source = str(plan.get("source") or "")
    fresh = bool(source_freshness.get(source, {}).get("fresh"))
    stale_review = allow_stale and not fresh

    if fresh:
        checks.append(GateCheck("fresh_source", "PASS", f"{source} source is same-day"))
    else:
        checks.append(GateCheck("fresh_source", "FAIL", f"{source} source is not same-day", True))

    if plan.get("selected"):
        checks.append(GateCheck("paper_plan", "PASS", "selected by 3-day paper plan"))
    elif plan.get("reject_reason"):
        checks.append(GateCheck("paper_plan", "FAIL", str(plan.get("reject_reason")), True))
    else:
        checks.append(GateCheck("paper_plan", "WARN", "paper filters passed but capacity was full"))

    _add_flow_checks(checks, plan)
    checks.append(_market_time_check(now))

    price_info: dict[str, Any] = {}
    spread_info: dict[str, Any] = {}
    if not live:
        checks.append(GateCheck("live_read", "PENDING", "offline mode; run without --offline for live KIS read"))
    elif trader is None:
        checks.append(GateCheck("live_read", "FAIL", f"KIS trader unavailable: {trader_error or 'unknown'}", True))
    else:
        try:
            price_info = trader.fetch_price(str(plan["code"])) or {}
        except Exception as exc:
            price_info = {"success": False, "message": f"{type(exc).__name__}: {exc}"}

        if not price_info.get("success"):
            checks.append(GateCheck("live_read", "FAIL", f"fetch_price failed: {price_info.get('message', '?')}", True))
        else:
            checks.append(GateCheck("live_read", "PASS", "KIS price read ok"))
            _add_price_checks(
                checks,
                plan,
                price_info,
                max_chase_premium_pct=args.max_chase_premium_pct,
                max_vwap_gap_pct=args.max_vwap_gap_pct,
                min_strength=args.min_strength,
                min_volume=args.min_volume,
            )

        try:
            spread_info = trader.check_spread(str(plan["code"])) or {}
        except Exception as exc:
            spread_info = {"ok": None, "message": f"{type(exc).__name__}: {exc}"}
        _add_spread_checks(checks, spread_info)

    if not live or not price_info.get("success"):
        vwap_proxy, vwap_gap, vwap_source = 0, 0.0, "UNAVAILABLE"
    else:
        vwap_proxy, vwap_gap, vwap_source = _vwap_value(price_info)

    spread_pct = _as_float(spread_info.get("spread_pct"))
    bid_ask_ratio = _as_float(spread_info.get("bid_ask_ratio"))
    decision, action, reason = _classify(checks, review_only=stale_review)

    return PreflightDecision(
        code=str(plan.get("code") or ""),
        name=str(plan.get("name") or plan.get("code") or ""),
        source=source,
        selected_by_plan=bool(plan.get("selected")),
        plan_score=_as_float(plan.get("rank_score")),
        plan_gate=str(plan.get("entry_gate") or ""),
        decision=decision,
        action=action,
        reason=reason,
        flow_grade=str(plan.get("flow_grade") or ""),
        flow_foreign_5d=_as_int(plan.get("flow_foreign_5d")),
        flow_inst_5d=_as_int(plan.get("flow_inst_5d")),
        flow_smart_total=_as_int(plan.get("flow_smart_total")),
        smart_consec=_as_int(plan.get("smart_consec")),
        dual_buy=bool(plan.get("dual_buy", False)),
        sell_exhaustion=_as_float(plan.get("sell_exhaustion"), 1.0),
        vol_compression=_as_float(plan.get("vol_compression"), 1.0),
        current_price=_as_int(price_info.get("current_price")),
        change_rate=_as_float(price_info.get("change_rate")),
        volume=_as_int(price_info.get("volume")),
        strength=_as_float(price_info.get("strength")),
        open_price=_as_int(price_info.get("open")),
        high_price=_as_int(price_info.get("high")),
        low_price=_as_int(price_info.get("low")),
        vwap_proxy=vwap_proxy,
        vwap_source=vwap_source,
        vwap_gap_pct=vwap_gap,
        spread_pct=round(spread_pct, 2),
        bid_ask_ratio=round(bid_ask_ratio, 1),
        checks=[asdict(c) for c in checks],
        buy_plan=plan.get("buy_plan", []),
        sell_plan=plan.get("sell_plan", []),
        risk_plan=plan.get("risk_plan", {}),
    )


def _pick_candidates(plan_summary: dict[str, Any], *, top: int, include_watch: bool) -> list[dict[str, Any]]:
    plans = plan_summary.get("plans", [])
    selected = [p for p in plans if p.get("selected")]
    if selected and not include_watch:
        return selected[:top]

    picked: list[dict[str, Any]] = []
    seen: set[str] = set()
    for p in selected + plans:
        code = str(p.get("code") or "")
        if not code or code in seen:
            continue
        picked.append(p)
        seen.add(code)
        if len(picked) >= top:
            break
    return picked


def build_preflight(args: argparse.Namespace) -> dict[str, Any]:
    plan_summary = build_plans(
        budget=max(1, args.budget),
        max_positions=max(1, args.max_positions),
        min_score=args.min_score,
        allow_stale=args.allow_stale,
    )
    live = not args.offline
    trader, trader_error = _make_trader(live)
    now = datetime.now()
    candidates = _pick_candidates(plan_summary, top=max(1, args.top), include_watch=args.include_watch)

    decisions = [
        _evaluate_plan(
            p,
            source_freshness=plan_summary["source_freshness"],
            trader=trader,
            live=live,
            trader_error=trader_error,
            now=now,
            allow_stale=args.allow_stale,
            args=args,
        )
        for p in candidates
    ]

    counts: dict[str, int] = {}
    for d in decisions:
        counts[d.decision] = counts.get(d.decision, 0) + 1

    return {
        "generated_at": now.isoformat(timespec="seconds"),
        "mode": "READ_ONLY_PREFLIGHT",
        "live_read": live,
        "offline": args.offline,
        "allow_stale": args.allow_stale,
        "trader_error": trader_error,
        "thresholds": {
            "min_score": args.min_score,
            "min_volume": args.min_volume,
            "min_strength": args.min_strength,
            "max_chase_premium_pct": args.max_chase_premium_pct,
            "max_vwap_gap_pct": args.max_vwap_gap_pct,
        },
        "plan_source": {
            "raw_candidates": plan_summary["raw_candidates"],
            "selected_count": plan_summary["selected_count"],
            "source_freshness": plan_summary["source_freshness"],
        },
        "decision_counts": counts,
        "decisions": [asdict(d) for d in decisions],
    }


def _fmt_money(value: Any) -> str:
    return f"{_as_int(value):,}"


def render_markdown(summary: dict[str, Any]) -> str:
    lines: list[str] = []
    lines.append("# Limit-up realtime preflight")
    lines.append("")
    lines.append(f"- generated_at: {summary['generated_at']}")
    lines.append(f"- mode: {summary['mode']}")
    lines.append(f"- live_read: {summary['live_read']}")
    lines.append(f"- allow_stale: {summary['allow_stale']}")
    lines.append(f"- source_raw_candidates: {summary['plan_source']['raw_candidates']}")
    lines.append(f"- source_selected_count: {summary['plan_source']['selected_count']}")
    lines.append(f"- decision_counts: {summary['decision_counts']}")
    if summary.get("trader_error"):
        lines.append(f"- trader_error: {summary['trader_error']}")
    lines.append("")
    lines.append("## Source Freshness")
    for name, meta in summary["plan_source"]["source_freshness"].items():
        status = "fresh" if meta.get("fresh") else "stale"
        lines.append(f"- {name}: {status}, timestamp={meta.get('timestamp') or '-'}, count={meta.get('count')}")
    lines.append("")
    lines.append("## Decisions")
    if not summary["decisions"]:
        lines.append("")
        lines.append("No candidates to preflight.")
    for d in summary["decisions"]:
        lines.append("")
        lines.append(f"### {d['code']} {d['name']}")
        lines.append(
            f"- decision: {d['decision']} / action: {d['action']} / reason: {d['reason']}"
        )
        lines.append(
            f"- plan: selected={d['selected_by_plan']}, gate={d['plan_gate']}, "
            f"score={d['plan_score']:.1f}, source={d['source']}"
        )
        lines.append(
            f"- flow: grade={d.get('flow_grade') or '-'}, foreign_5d={d.get('flow_foreign_5d', 0):,}, "
            f"inst_5d={d.get('flow_inst_5d', 0):,}, smart_total={d.get('flow_smart_total', 0):,}, "
            f"smart_consec={d.get('smart_consec', 0)}, dual_buy={d.get('dual_buy', False)}, "
            f"sell_exhaustion={d.get('sell_exhaustion', 1.0):.2f}, "
            f"flow_turnover={d.get('vol_compression', 1.0):.2f}"
        )
        if d["current_price"] > 0:
            lines.append(
                f"- live: price={d['current_price']:,}, change={d['change_rate']:+.2f}%, "
                f"volume={d['volume']:,}, strength={d['strength']:.1f}"
            )
            lines.append(
                f"- vwap={d['vwap_proxy']:,} ({d.get('vwap_source', '-')}), "
                f"vwap_gap={d['vwap_gap_pct']:+.2f}%, "
                f"spread={d['spread_pct']:.2f}%, bid_ask={d['bid_ask_ratio']:.1f}"
            )
        risk = d.get("risk_plan") or {}
        if risk:
            lines.append(
                f"- risk: qty_hint={risk.get('full_qty_hint', 0)}, "
                f"hard_stop={_fmt_money(risk.get('hard_stop_price', 0))}, "
                f"max_loss_hint={_fmt_money(risk.get('max_loss_hint', 0))}"
            )
        lines.append("- checks:")
        for c in d["checks"]:
            lines.append(f"  - {c['status']} {c['name']}: {c['detail']}")

    lines.append("")
    lines.append("## Policy")
    lines.append("- This report is read-only. It does not place, modify, or cancel orders.")
    lines.append("- ENTRY_READY still means paper/manual review first while AUTO_TRADE_DISABLED is active.")
    lines.append("- Stale source data can only be used for REVIEW_ONLY, never for a D+0 live entry.")
    lines.append("- D+0 entries require same-day source data plus live price, VWAP proxy, and orderbook checks.")
    return "\n".join(lines) + "\n"


def _write_csv(summary: dict[str, Any], path: Path) -> None:
    fields = [
        "decision",
        "action",
        "code",
        "name",
        "selected_by_plan",
        "plan_score",
        "source",
        "flow_grade",
        "flow_foreign_5d",
        "flow_inst_5d",
        "flow_smart_total",
        "smart_consec",
        "dual_buy",
        "sell_exhaustion",
        "vol_compression",
        "current_price",
        "change_rate",
        "volume",
        "strength",
        "vwap_proxy",
        "vwap_source",
        "vwap_gap_pct",
        "spread_pct",
        "bid_ask_ratio",
        "reason",
    ]
    with path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for d in summary["decisions"]:
            writer.writerow({k: d.get(k, "") for k in fields})


def _append_journal(summary: dict[str, Any]) -> None:
    fields = [
        "generated_at",
        "decision",
        "action",
        "code",
        "name",
        "selected_by_plan",
        "plan_score",
        "source",
        "flow_grade",
        "flow_foreign_5d",
        "flow_inst_5d",
        "flow_smart_total",
        "smart_consec",
        "dual_buy",
        "sell_exhaustion",
        "vol_compression",
        "current_price",
        "change_rate",
        "volume",
        "strength",
        "vwap",
        "vwap_source",
        "vwap_gap_pct",
        "spread_pct",
        "bid_ask_ratio",
        "reason",
    ]
    write_header = not JOURNAL_PATH.exists()
    with JOURNAL_PATH.open("a", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        if write_header:
            writer.writeheader()
        for d in summary["decisions"]:
            row = {k: d.get(k, "") for k in fields}
            row["generated_at"] = summary["generated_at"]
            row["vwap"] = d.get("vwap_proxy", "")
            writer.writerow(row)


def save_outputs(summary: dict[str, Any]) -> tuple[Path, Path, Path]:
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    json_path = RESULT_DIR / f"limit_up_preflight_{stamp}.json"
    md_path = RESULT_DIR / "limit_up_preflight_latest.md"
    csv_path = RESULT_DIR / "limit_up_preflight_action_sheet.csv"
    json_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(render_markdown(summary), encoding="utf-8")
    _write_csv(summary, csv_path)
    _append_journal(summary)
    return json_path, md_path, csv_path


def print_decision_summary(summary: dict[str, Any]) -> None:
    decisions = summary.get("decisions", [])
    if not decisions:
        print("decision detail: no candidates")
        return

    print("decision detail:")
    for d in decisions:
        price = _as_int(d.get("current_price"))
        price_txt = f"{price:,}" if price > 0 else "-"
        vwap_gap = _as_float(d.get("vwap_gap_pct"))
        spread = _as_float(d.get("spread_pct"))
        print(
            f"- {d.get('code')} {d.get('name')}: {d.get('decision')} | "
            f"price={price_txt} | vwap_gap={vwap_gap:+.2f}% | "
            f"vwap_source={d.get('vwap_source', '-')} | "
            f"flow={d.get('flow_grade') or '-'} smart={_as_int(d.get('flow_smart_total')):,} | "
            f"spread={spread:.2f}% | reason={d.get('reason')}"
        )


def main() -> int:
    parser = argparse.ArgumentParser(description="Read-only limit-up realtime preflight")
    parser.add_argument("--budget", type=int, default=3_000_000)
    parser.add_argument("--max-positions", type=int, default=2)
    parser.add_argument("--min-score", type=float, default=55.0)
    parser.add_argument("--top", type=int, default=5)
    parser.add_argument("--include-watch", action="store_true")
    parser.add_argument("--allow-stale", action="store_true", help="Historical review only; never ENTRY_READY.")
    parser.add_argument("--offline", action="store_true", help="Skip live KIS reads and write a pending report.")
    parser.add_argument("--min-volume", type=int, default=5_000)
    parser.add_argument("--min-strength", type=float, default=100.0)
    parser.add_argument("--max-chase-premium-pct", type=float, default=12.0)
    parser.add_argument("--max-vwap-gap-pct", type=float, default=3.5)
    args = parser.parse_args()

    summary = build_preflight(args)
    json_path, md_path, csv_path = save_outputs(summary)

    print("limit-up realtime preflight complete")
    print(f"mode: {summary['mode']}")
    print(f"live_read: {summary['live_read']}")
    print(f"decisions: {summary['decision_counts']}")
    print_decision_summary(summary)
    print(f"summary: {json_path}")
    print(f"latest report: {md_path}")
    print(f"action sheet: {csv_path}")
    print(f"journal: {JOURNAL_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
