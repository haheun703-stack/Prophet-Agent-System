# -*- coding: utf-8 -*-
"""Read-only summaries for the FLOWX forward-paper event ledger.

The source JSONL ledger is never changed.  A caller may persist the derived
summary to a separate JSON file for the daily 18:00 KST integration.
"""

import json
import math
import os
import tempfile
import threading
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

from data.flowx_paper_adapter import (
    FLOWX_EVENT_DIR,
    FlowXPaperEventAdapter,
    PaperEventRejected,
    _iso_aware,
)


SCALPER_ROOT = Path(__file__).resolve().parents[1]
FLOWX_SUMMARY_PATH = (
    SCALPER_ROOT / "data_store" / "flowx_paper_summary" / "latest.json"
)
_IDENTITY_FIELDS = (
    "strategy_id", "strategy_version", "market", "currency", "ticker", "trade_id"
)
_COST_FIELDS = ("fee", "tax", "slippage", "spread")
_SUMMARY_PROCESS_LOCK = threading.Lock()


def _validate_source_event(row: Mapping[str, Any]) -> tuple[datetime, str]:
    """Validate the immutable fields used by the summary, failing closed."""
    if row.get("real_order") is not False:
        raise PaperEventRejected("paper summary refuses real_order or missing paper flag")
    missing = [field for field in _IDENTITY_FIELDS if not str(row.get(field) or "").strip()]
    if missing:
        raise PaperEventRejected(
            f"paper summary source is missing fields: {', '.join(missing)}"
        )
    event_dt, event_iso = _iso_aware(row.get("event_at"), "event_at")
    data_dt, _ = _iso_aware(row.get("data_asof"), "data_asof")
    if data_dt > event_dt:
        raise PaperEventRejected("paper summary source has data_asof after event_at")
    if row.get("cost_complete") is True:
        for field in _COST_FIELDS:
            try:
                cost = float(row.get(field))
            except (TypeError, ValueError) as exc:
                raise PaperEventRejected(
                    f"paper summary source has incomplete certified cost: {field}"
                ) from exc
            if cost < 0 or not math.isfinite(cost):
                raise PaperEventRejected(
                    f"paper summary source has invalid certified cost: {field}"
                )
    if row.get("event_type") == "EXIT":
        certified = row.get("net_return_certified") is True
        if certified != (row.get("cost_complete") is True):
            raise PaperEventRejected(
                "paper summary source has inconsistent EXIT certification"
            )
        if certified:
            try:
                net_return = float(row.get("net_return"))
            except (TypeError, ValueError) as exc:
                raise PaperEventRejected(
                    "paper summary source has invalid certified net_return"
                ) from exc
            if not math.isfinite(net_return):
                raise PaperEventRejected(
                    "paper summary source has invalid certified net_return"
                )
    return event_dt, event_iso


def _period(values: Iterable[str]) -> dict[str, str | None]:
    ordered = sorted(values)
    return {
        "start": ordered[0] if ordered else None,
        "end": ordered[-1] if ordered else None,
    }


def build_flowx_paper_summary(
    *,
    event_dir: Path | str | None = None,
    as_of: datetime | str | None = None,
) -> dict[str, Any]:
    """Return a deterministic strategy/market summary without changing the ledger."""
    cutoff_dt, cutoff_iso = _iso_aware(
        as_of or datetime.now(timezone.utc), "as_of"
    )
    adapter = FlowXPaperEventAdapter(event_dir or FLOWX_EVENT_DIR)
    groups: dict[tuple[str, str, str, str], list[dict[str, Any]]] = defaultdict(list)
    trade_identities: dict[str, tuple[str, str, str, str, str]] = {}
    exit_counts: dict[str, int] = defaultdict(int)

    for source_row in adapter.iter_events():
        if not isinstance(source_row, Mapping):
            raise PaperEventRejected("paper summary source row must be an object")
        event_dt, event_iso = _validate_source_event(source_row)
        if event_dt > cutoff_dt:
            continue
        row = dict(source_row)
        row["event_at"] = event_iso
        key = (
            str(row["strategy_id"]),
            str(row["strategy_version"]),
            str(row["market"]).upper(),
            str(row["currency"]).upper(),
        )
        trade_id = str(row["trade_id"])
        lifecycle_identity = key + (str(row["ticker"]).upper(),)
        previous_identity = trade_identities.setdefault(trade_id, lifecycle_identity)
        if previous_identity != lifecycle_identity:
            raise PaperEventRejected(
                "paper summary refuses a trade_id spanning multiple identities"
            )
        if row.get("event_type") == "EXIT":
            exit_counts[trade_id] += 1
            if exit_counts[trade_id] > 1:
                raise PaperEventRejected(
                    "paper summary refuses multiple EXIT events for one trade_id"
                )
        groups[key].append(row)

    strategies: list[dict[str, Any]] = []
    for (strategy_id, version, market, currency), rows in sorted(groups.items()):
        exits = [row for row in rows if row.get("event_type") == "EXIT"]
        complete = sum(row.get("cost_complete") is True for row in exits)
        exit_times = [str(row["event_at"]) for row in exits]
        strategies.append({
            "strategy_id": strategy_id,
            "strategy_version": version,
            "market": market,
            "currency": currency,
            "event_count": len(rows),
            "trade_count": len({str(row["trade_id"]) for row in rows}),
            "sample": {
                "closed_trade_count": len(exits),
                "cost_complete_count": complete,
                "cost_incomplete_count": len(exits) - complete,
                "cost_complete_rate": (
                    round(complete / len(exits), 10) if exits else None
                ),
            },
            "period": {
                "events": _period(str(row["event_at"]) for row in rows),
                "closed_trades": _period(exit_times),
            },
        })

    closed_count = sum(item["sample"]["closed_trade_count"] for item in strategies)
    complete_count = sum(item["sample"]["cost_complete_count"] for item in strategies)
    return {
        "schema_version": "flowx.paper.strategy-summary.v1",
        "paper_only": True,
        "as_of": cutoff_iso,
        "source": {
            "type": "append_only_jsonl",
            "pattern": "flowx_paper_events_*.jsonl",
        },
        "totals": {
            "strategy_market_count": len(strategies),
            "event_count": sum(item["event_count"] for item in strategies),
            "trade_count": sum(item["trade_count"] for item in strategies),
            "closed_trade_count": closed_count,
            "cost_complete_count": complete_count,
            "cost_incomplete_count": closed_count - complete_count,
        },
        "strategies": strategies,
    }


def write_flowx_paper_summary(
    summary: Mapping[str, Any],
    output_path: Path | str | None = None,
    *,
    source_event_dir: Path | str | None = None,
) -> Path:
    """Atomically replace only the derived summary artifact."""
    path = Path(output_path) if output_path is not None else FLOWX_SUMMARY_PATH
    source_dir = Path(source_event_dir or FLOWX_EVENT_DIR).resolve()
    resolved = path.resolve()
    if resolved.name.startswith("flowx_paper_events_") or resolved.suffix == ".jsonl":
        raise PaperEventRejected("paper summary output cannot be an event ledger")
    if resolved == source_dir or source_dir in resolved.parents:
        raise PaperEventRejected("paper summary output cannot be inside the event ledger")
    path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = path.with_name(f".{path.name}.lock")
    with _SUMMARY_PROCESS_LOCK:
        lock_fd = FlowXPaperEventAdapter._acquire_lock(lock_path)
        temporary: Path | None = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="w", encoding="utf-8", newline="", delete=False,
                dir=path.parent, prefix=f".{path.name}.", suffix=".tmp",
            ) as handle:
                temporary = Path(handle.name)
                json.dump(summary, handle, ensure_ascii=False, sort_keys=True, indent=2)
                handle.write("\n")
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, path)
            temporary = None
            if os.name != "nt":
                directory_fd = os.open(path.parent, os.O_RDONLY)
                try:
                    os.fsync(directory_fd)
                finally:
                    os.close(directory_fd)
        finally:
            if temporary is not None and temporary.exists():
                temporary.unlink()
            try:
                FlowXPaperEventAdapter._release_lock(lock_fd)
            finally:
                os.close(lock_fd)
    return path
