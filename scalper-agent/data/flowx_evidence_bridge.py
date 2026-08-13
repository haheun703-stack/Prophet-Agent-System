# -*- coding: utf-8 -*-
"""Attach public FLOWX stock evidence to paper candidates without look-ahead."""

from __future__ import annotations

import copy
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping


class EvidenceBridgeRejected(ValueError):
    """Raised when a public evidence snapshot cannot be trusted for paper use."""


def _aware(value: datetime | str, field: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        try:
            parsed = datetime.fromisoformat(str(value or "").replace("Z", "+00:00"))
        except ValueError as exc:
            raise EvidenceBridgeRejected(f"{field} must be an ISO-8601 datetime") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise EvidenceBridgeRejected(f"{field} must include a timezone")
    return parsed.astimezone(timezone.utc)


def load_public_evidence(
    path: str | Path,
    decision_at: datetime | str,
    *,
    max_snapshot_age_hours: float = 36.0,
) -> dict[tuple[str, str], dict[str, Any]]:
    """Load a public information-bot snapshot and return decision-safe ticker refs."""
    decision = _aware(decision_at, "decision_at")
    source_path = Path(path)
    try:
        document = json.loads(source_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise EvidenceBridgeRejected("public evidence snapshot is unreadable") from exc

    if document.get("schema_version") != "flowx.stock-evidence-batch.v1":
        raise EvidenceBridgeRejected("unsupported evidence batch schema")
    if document.get("item_contract") != "flowx.stock-evidence.v1":
        raise EvidenceBridgeRejected("unsupported evidence item contract")
    if document.get("mode") != "paper_read_only":
        raise EvidenceBridgeRejected("evidence snapshot is not paper_read_only")

    generated = _aware(document.get("generated_at"), "generated_at")
    age_hours = (decision - generated).total_seconds() / 3600
    if age_hours < 0:
        raise EvidenceBridgeRejected("evidence snapshot was generated after decision_at")
    if age_hours > max_snapshot_age_hours:
        raise EvidenceBridgeRejected("evidence snapshot is stale")

    result: dict[tuple[str, str], dict[str, Any]] = {}
    for item in document.get("items") or []:
        if not isinstance(item, Mapping):
            continue
        instrument = item.get("instrument") or {}
        market = str(instrument.get("market") or "").upper()
        ticker = str(instrument.get("ticker") or "").upper()
        if market not in {"KR", "US"} or not ticker:
            continue

        freshness = item.get("freshness") or {}
        catalyst_freshness = (freshness.get("components") or {}).get("catalysts") or {}
        if catalyst_freshness.get("status") != "fresh":
            continue

        safe_catalysts = []
        for catalyst in item.get("catalysts") or []:
            if not isinstance(catalyst, Mapping):
                continue
            published_raw = catalyst.get("published_at")
            if not published_raw:
                continue
            try:
                published = _aware(published_raw, "published_at")
            except EvidenceBridgeRejected:
                continue
            catalyst_age_hours = (decision - published).total_seconds() / 3600
            if published <= generated and 0 <= catalyst_age_hours <= 24 * 7:
                safe_catalysts.append(catalyst)
        if not safe_catalysts:
            continue

        kinds = sorted({
            str(catalyst.get("kind") or "unknown") for catalyst in safe_catalysts
        })
        result[(market, ticker)] = {
            "evidence_ref": f"{document['generated_at']}:{market}:{ticker}",
            "evidence_asof": document["generated_at"],
            "evidence_freshness": catalyst_freshness.get("status"),
            "evidence_catalyst_kinds": kinds,
            "evidence_catalyst_count": len(safe_catalysts),
        }
    return result


def attach_public_evidence(
    candidate: Mapping[str, Any],
    evidence_index: Mapping[tuple[str, str], Mapping[str, Any]],
) -> dict[str, Any]:
    """Return a copied candidate with compact evidence metadata on trade_object."""
    enriched = copy.deepcopy(dict(candidate))
    trade_object = dict(enriched.get("trade_object") or {})
    market = str(trade_object.get("market") or enriched.get("market") or "KR").upper()
    ticker = str(
        trade_object.get("ticker") or enriched.get("ticker")
        or enriched.get("code") or ""
    ).upper()
    evidence = evidence_index.get((market, ticker))
    if evidence:
        trade_object.update(dict(evidence))
        sources = list(trade_object.get("sources") or [])
        if "flowx_public_evidence" not in sources:
            sources.append("flowx_public_evidence")
        trade_object["sources"] = sources
    enriched["trade_object"] = trade_object
    return enriched
