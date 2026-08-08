# -*- coding: utf-8 -*-
"""FLOWX append-only forward-paper event adapter.

This module has no broker dependency and cannot place a real order.  Callers
must explicitly identify every event as paper-only; anything else fails closed.
"""

from __future__ import annotations

import json
import math
import os
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping


SCALPER_ROOT = Path(__file__).resolve().parents[1]
FLOWX_EVENT_DIR = SCALPER_ROOT / "data_store" / "flowx_paper_events"
_EVENT_NAMESPACE = uuid.UUID("c5516eb4-6c77-4b35-b70b-d13535df223c")
_PROCESS_LOCK = threading.Lock()
_EVENT_TYPES = {"CANDIDATE", "ORDER_INTENT", "PAPER_FILL", "EXIT"}
_COST_FIELDS = ("fee", "tax", "slippage", "spread")


class PaperEventRejected(ValueError):
    """Raised before any write when an event is not provably paper-only."""


def _iso_aware(value: datetime | str, field: str) -> tuple[datetime, str]:
    if isinstance(value, datetime):
        parsed = value
    else:
        raw = str(value or "").strip().replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(raw)
        except ValueError as exc:
            raise PaperEventRejected(f"{field} must be an ISO-8601 datetime") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise PaperEventRejected(f"{field} must include a timezone")
    normalized = parsed.astimezone(timezone.utc)
    return normalized, normalized.isoformat().replace("+00:00", "Z")


def _optional_nonnegative(value: Any, field: str) -> float | None:
    if value is None or value == "":
        return None
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise PaperEventRejected(f"{field} must be numeric or null") from exc
    if number < 0:
        raise PaperEventRejected(f"{field} cannot be negative")
    if not math.isfinite(number):
        raise PaperEventRejected(f"{field} must be finite")
    return number


class FlowXPaperEventAdapter:
    """Write deterministic, idempotent FLOWX paper events to JSONL."""

    def __init__(self, event_dir: Path | str | None = None) -> None:
        self.event_dir = Path(event_dir) if event_dir is not None else FLOWX_EVENT_DIR

    def emit(
        self,
        *,
        event_type: str,
        trade_id: str,
        strategy_id: str,
        strategy_version: str,
        market: str,
        currency: str,
        ticker: str,
        event_at: datetime | str,
        data_asof: datetime | str,
        source_record_id: str,
        fill: Mapping[str, Any] | None = None,
        fee: float | None = None,
        tax: float | None = None,
        slippage: float | None = None,
        spread: float | None = None,
        benchmark: Any = None,
        exposure: Any = None,
        regime: Any = None,
        real_order: bool = False,
        paper: bool = True,
        details: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Validate and append one event, returning ``deduped=True`` on retry."""
        if real_order or paper is not True:
            raise PaperEventRejected("FLOWX adapter accepts paper events only")

        kind = str(event_type or "").upper()
        if kind not in _EVENT_TYPES:
            raise PaperEventRejected(f"unsupported event_type: {kind!r}")

        identity = {
            "trade_id": str(trade_id or "").strip(),
            "strategy_id": str(strategy_id or "").strip(),
            "strategy_version": str(strategy_version or "").strip(),
            "market": str(market or "").strip().upper(),
            "currency": str(currency or "").strip().upper(),
            "ticker": str(ticker or "").strip().upper(),
            "source_record_id": str(source_record_id or "").strip(),
        }
        missing = [name for name, value in identity.items() if not value]
        if missing:
            raise PaperEventRejected(f"missing required identity fields: {', '.join(missing)}")
        if len(identity["currency"]) != 3:
            raise PaperEventRejected("currency must be a 3-letter code")

        event_dt, event_iso = _iso_aware(event_at, "event_at")
        asof_dt, asof_iso = _iso_aware(data_asof, "data_asof")
        if asof_dt > event_dt:
            raise PaperEventRejected("data_asof cannot be later than event_at")

        normalized_fill = self._normalize_fill(kind, fill)
        costs = {
            "fee": _optional_nonnegative(fee, "fee"),
            "tax": _optional_nonnegative(tax, "tax"),
            "slippage": _optional_nonnegative(slippage, "slippage"),
            "spread": _optional_nonnegative(spread, "spread"),
        }
        cost_complete = kind in {"PAPER_FILL", "EXIT"} and all(
            costs[name] is not None for name in _COST_FIELDS
        )

        event_key = "|".join(
            [identity["trade_id"], kind, identity["strategy_id"],
             identity["strategy_version"], identity["market"],
             identity["ticker"], identity["source_record_id"]]
        )
        event_id = str(uuid.uuid5(_EVENT_NAMESPACE, event_key))
        event: dict[str, Any] = {
            "event_id": event_id,
            "trade_id": identity["trade_id"],
            "event_type": kind,
            "strategy_id": identity["strategy_id"],
            "strategy_version": identity["strategy_version"],
            "market": identity["market"],
            "currency": identity["currency"],
            "ticker": identity["ticker"],
            "event_at": event_iso,
            "data_asof": asof_iso,
            "fill": normalized_fill,
            **costs,
            "cost_complete": cost_complete,
            "net_return": None,
            "net_return_certified": False,
            "benchmark": benchmark,
            "exposure": exposure,
            "regime": regime,
            "source_record_id": identity["source_record_id"],
            "real_order": False,
        }
        if details:
            event["details"] = dict(details)

        existing = self._append_once(event, event_dt.date().isoformat())
        result = dict(existing or event)
        result["deduped"] = existing is not None
        return result

    @staticmethod
    def _normalize_fill(kind: str, fill: Mapping[str, Any] | None) -> dict[str, Any] | None:
        if kind not in {"PAPER_FILL", "EXIT"}:
            return None if fill is None else dict(fill)
        if not isinstance(fill, Mapping):
            raise PaperEventRejected(f"{kind} requires a fill object")
        try:
            price = float(fill.get("price"))
            raw_qty = fill.get("qty")
            qty = None if raw_qty in (None, "", 0, "0") else float(raw_qty)
        except (TypeError, ValueError) as exc:
            raise PaperEventRejected("fill.price must be numeric and fill.qty numeric or null") from exc
        if price <= 0 or (qty is not None and qty <= 0):
            raise PaperEventRejected("fill.price must be positive and fill.qty positive or null")
        if not math.isfinite(price) or (qty is not None and not math.isfinite(qty)):
            raise PaperEventRejected("fill.price and fill.qty must be finite")
        side = str(fill.get("side") or "").upper()
        expected_side = "BUY" if kind == "PAPER_FILL" else "SELL"
        if side != expected_side:
            raise PaperEventRejected(f"{kind} fill.side must be {expected_side}")
        normalized = dict(fill)
        normalized.update({"side": side, "price": price, "qty": qty, "paper": True})
        raw_notional = fill.get("notional")
        computed_notional = price * qty if qty is not None else None
        if raw_notional not in (None, "", 0, "0"):
            try:
                supplied_notional = float(raw_notional)
            except (TypeError, ValueError) as exc:
                raise PaperEventRejected("fill.notional must be numeric or null") from exc
            if computed_notional is None or not math.isclose(
                supplied_notional, computed_notional, rel_tol=1e-9, abs_tol=1e-9
            ):
                raise PaperEventRejected("fill.notional must equal fill.price * fill.qty")
        normalized["notional"] = computed_notional
        if normalized["notional"] is not None and (
            normalized["notional"] <= 0 or not math.isfinite(normalized["notional"])
        ):
            raise PaperEventRejected("fill.notional must be positive and finite")
        return normalized

    def _compute_net_return(self, exit_event: Mapping[str, Any]) -> float | None:
        if not all(exit_event.get(name) is not None for name in _COST_FIELDS):
            return None
        fills = [row for row in self.iter_events() if row.get("trade_id") == exit_event["trade_id"]]
        entries = [
            row for row in fills
            if row.get("event_type") == "PAPER_FILL"
            and str((row.get("fill") or {}).get("side", "BUY")).upper() == "BUY"
        ]
        if not entries:
            return None
        identity_fields = (
            "strategy_id", "strategy_version", "market", "currency", "ticker"
        )
        if any(
            any(entry.get(field) != exit_event.get(field) for field in identity_fields)
            for entry in entries
        ):
            return None
        exit_dt, _ = _iso_aware(str(exit_event["event_at"]), "event_at")
        for entry in entries:
            entry_dt, _ = _iso_aware(str(entry.get("event_at") or ""), "event_at")
            if entry_dt > exit_dt:
                return None
        if not all(entry.get("cost_complete") for entry in entries):
            return None
        exit_fill = exit_event.get("fill") or {}
        entry_notional = sum(float((entry.get("fill") or {}).get("notional") or 0) for entry in entries)
        entry_qty = sum(float((entry.get("fill") or {}).get("qty") or 0) for entry in entries)
        exit_qty = float(exit_fill.get("qty") or 0)
        exit_notional = float(exit_fill.get("notional") or 0)
        if entry_notional <= 0 or entry_qty <= 0 or abs(entry_qty - exit_qty) > 1e-9:
            return None
        total_cost = sum(
            float(entry[name]) for entry in entries for name in _COST_FIELDS
        )
        total_cost += sum(float(exit_event[name]) for name in _COST_FIELDS)
        return round((exit_notional - entry_notional - total_cost) / entry_notional, 10)

    def _append_once(
        self, event: dict[str, Any], date_key: str
    ) -> dict[str, Any] | None:
        self.event_dir.mkdir(parents=True, exist_ok=True)
        lock_path = self.event_dir / ".append.lock"
        with _PROCESS_LOCK:
            lock_fd = self._acquire_lock(lock_path)
            try:
                if event.get("event_type") == "EXIT":
                    event["net_return"] = self._compute_net_return(event)
                    event["net_return_certified"] = event["net_return"] is not None
                    event["cost_complete"] = event["net_return_certified"]
                existing = self._event_by_id(str(event["event_id"]))
                if existing is not None:
                    ignored = {"event_at"}
                    old_business = {k: v for k, v in existing.items() if k not in ignored}
                    new_business = {k: v for k, v in event.items() if k not in ignored}
                    if old_business != new_business:
                        raise PaperEventRejected(
                            "event_id already exists with different immutable content"
                        )
                    return existing
                path = self.event_dir / f"flowx_paper_events_{date_key}.jsonl"
                line = json.dumps(
                    dict(event), ensure_ascii=False, sort_keys=True, allow_nan=False
                ) + "\n"
                with path.open("a", encoding="utf-8", newline="") as handle:
                    handle.write(line)
                    handle.flush()
                    os.fsync(handle.fileno())
                return None
            finally:
                try:
                    self._release_lock(lock_fd)
                finally:
                    os.close(lock_fd)

    @staticmethod
    def _acquire_lock(lock_path: Path) -> int:
        fd = os.open(str(lock_path), os.O_CREAT | os.O_RDWR)
        try:
            if os.name == "nt":
                import msvcrt

                if os.fstat(fd).st_size == 0:
                    os.write(fd, b"0")
                    os.fsync(fd)
                os.lseek(fd, 0, os.SEEK_SET)
                msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return fd
        except (OSError, IOError) as exc:
            os.close(fd)
            raise PaperEventRejected("paper event ledger is locked; write refused") from exc

    @staticmethod
    def _release_lock(fd: int) -> None:
        if os.name == "nt":
            import msvcrt

            os.lseek(fd, 0, os.SEEK_SET)
            msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)
        else:
            import fcntl

            fcntl.flock(fd, fcntl.LOCK_UN)

    def _event_by_id(self, event_id: str) -> dict[str, Any] | None:
        return next((row for row in self.iter_events() if row.get("event_id") == event_id), None)

    def iter_events(self) -> Iterable[dict[str, Any]]:
        if not self.event_dir.exists():
            return []
        rows: list[dict[str, Any]] = []
        for path in sorted(self.event_dir.glob("flowx_paper_events_*.jsonl")):
            with path.open("r", encoding="utf-8", errors="replace") as handle:
                for line in handle:
                    try:
                        rows.append(json.loads(line))
                    except json.JSONDecodeError as exc:
                        raise PaperEventRejected(f"malformed paper ledger row: {path}") from exc
        return rows


def emit_flowx_paper_event(**kwargs: Any) -> dict[str, Any]:
    """Convenience entry point for existing paper-trading call sites."""
    return FlowXPaperEventAdapter().emit(**kwargs)
