# -*- coding: utf-8 -*-
"""Runtime kill switch for automated trading.

This module intentionally reads a small file on every check. A long-running bot
process must not rely only on imported constants when the operator turns auto
trading off during the day.
"""

import json
import os
from pathlib import Path
from typing import Any


SCALPER_ROOT = Path(__file__).resolve().parents[1]
KILL_SWITCH_PATH = SCALPER_ROOT / "data" / "kill_switch.json"


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def file_auto_trade_disabled() -> bool:
    try:
        data = json.loads(KILL_SWITCH_PATH.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return False
    except Exception:
        return True
    return _truthy(data.get("AUTO_TRADE_DISABLED"))


def is_auto_trade_disabled() -> bool:
    if _truthy(os.getenv("AUTO_TRADE_DISABLED")):
        return True
    if file_auto_trade_disabled():
        return True
    try:
        from data.sajang_rules import SAJANG

        return bool(getattr(SAJANG, "AUTO_TRADE_DISABLED", False))
    except Exception:
        return False
