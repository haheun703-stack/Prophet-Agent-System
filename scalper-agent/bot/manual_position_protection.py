# -*- coding: utf-8 -*-
"""Central auto-sell protection for manual or unknown holdings."""

import json
from pathlib import Path
from typing import Any


SCALPER_ROOT = Path(__file__).resolve().parents[1]
POSITIONS_PATH = SCALPER_ROOT / "data_store" / "positions.json"

PROTECTED_SOURCE_PREFIXES = ("manual_sync", "sync_auto", "manual_protected")
PROTECTED_SOURCES = {"verification"}


def load_positions() -> dict[str, Any]:
    try:
        data = json.loads(POSITIONS_PATH.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {}
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


def auto_sell_block_reason(code: str) -> str | None:
    positions = load_positions()
    code = str(code or "").zfill(6)
    pos = positions.get(code)
    if not pos:
        return "manual_or_unknown_position"
    if pos.get("manual_protected"):
        return "manual_protected"
    if pos.get("sl_disabled"):
        return "sl_disabled"
    source = str(pos.get("source") or "")
    if source in PROTECTED_SOURCES:
        return f"protected_source:{source}"
    if source.startswith(PROTECTED_SOURCE_PREFIXES):
        return f"protected_source:{source}"
    return None


def audit_kis_vs_memory(kis_positions: list[dict[str, Any]]) -> dict[str, Any]:
    positions = load_positions()
    memory_codes = set(positions.keys())
    kis_codes = {str(p.get("code") or "").zfill(6) for p in kis_positions if p.get("code")}
    unknown_to_memory = sorted(kis_codes - memory_codes)
    protected = []
    unprotected = []
    for code in sorted(kis_codes):
        reason = auto_sell_block_reason(code)
        row = {"code": code, "reason": reason or ""}
        if reason:
            protected.append(row)
        else:
            unprotected.append(row)
    return {
        "memory_count": len(memory_codes),
        "kis_count": len(kis_codes),
        "unknown_to_memory": unknown_to_memory,
        "protected": protected,
        "unprotected": unprotected,
    }
