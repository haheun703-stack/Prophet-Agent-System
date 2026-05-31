# -*- coding: utf-8 -*-
"""Append-only order intent ledger.

Every KIS order path must write an intent before the network order call. The
ledger is used for forensics, daily limits, and restart readiness checks.
"""

import json
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any


SCALPER_ROOT = Path(__file__).resolve().parents[1]
INTENT_DIR = SCALPER_ROOT / "data_store" / "order_intents"


def _date_key(dt: datetime | None = None) -> str:
    return (dt or datetime.now()).strftime("%Y-%m-%d")


def intent_path(date_key: str | None = None) -> Path:
    return INTENT_DIR / f"order_intents_{date_key or _date_key()}.jsonl"


def _safe_int(value: Any) -> int:
    try:
        if value is None or value == "":
            return 0
        return int(float(str(value).replace(",", "")))
    except Exception:
        return 0


def record_order_intent(
    *,
    side: str,
    code: str,
    qty: int,
    source: str,
    manual: bool,
    allowed: bool,
    reason: str = "",
    message: str = "",
    strategy: str = "",
    estimate_amount_krw: int | None = None,
    order_no: str = "",
    rt_cd: str = "",
    filled_qty: int = 0,
    avg_fill_price: int = 0,
    dedupe_daily: bool = False,
) -> dict[str, Any]:
    now = datetime.now()
    record = {
        "intent_id": f"{now.strftime('%Y%m%d%H%M%S%f')}_{uuid.uuid4().hex[:8]}",
        "timestamp": now.isoformat(timespec="seconds"),
        "date": _date_key(now),
        "side": str(side or "").upper(),
        "code": str(code or "").zfill(6),
        "qty": _safe_int(qty),
        "source": str(source or ""),
        "strategy": str(strategy or ""),
        "mode": "manual" if manual else "auto",
        "manual": bool(manual),
        "allowed": bool(allowed),
        "reason": str(reason or ""),
        "message": str(message or ""),
        "estimate_amount_krw": _safe_int(estimate_amount_krw),
        "order_no": str(order_no or ""),
        "rt_cd": str(rt_cd or ""),
        "filled_qty": _safe_int(filled_qty),
        "avg_fill_price": _safe_int(avg_fill_price),
    }
    INTENT_DIR.mkdir(parents=True, exist_ok=True)
    if dedupe_daily and _has_daily_dedupe(record):
        record["deduped"] = True
        return record
    with intent_path(record["date"]).open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")
    return record


def _has_daily_dedupe(record: dict[str, Any]) -> bool:
    key = (
        record.get("date", ""),
        record.get("side", ""),
        record.get("code", ""),
        record.get("reason", ""),
        record.get("source", ""),
    )
    path = intent_path(str(record.get("date") or ""))
    if not path.exists():
        return False
    with path.open("r", encoding="utf-8", errors="replace") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except Exception:
                continue
            row_key = (
                str(row.get("date", "")),
                str(row.get("side", "")),
                str(row.get("code", "")),
                str(row.get("reason", "")),
                str(row.get("source", "")),
            )
            if row_key == key:
                return True
    return False


def iter_order_intents(date_key: str | None = None) -> list[dict[str, Any]]:
    path = intent_path(date_key)
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8", errors="replace") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except Exception:
                rows.append({"parse_error": line[:500]})
    return rows


def count_allowed_auto_buys_today(date_key: str | None = None) -> int:
    return sum(
        1
        for row in iter_order_intents(date_key)
        if row.get("allowed")
        and row.get("mode") == "auto"
        and str(row.get("side", "")).upper() == "BUY"
    )


def sum_allowed_auto_buy_amount_today(date_key: str | None = None) -> int:
    total = 0
    for row in iter_order_intents(date_key):
        if (
            row.get("allowed")
            and row.get("mode") == "auto"
            and str(row.get("side", "")).upper() == "BUY"
        ):
            total += _safe_int(row.get("estimate_amount_krw"))
    return total
