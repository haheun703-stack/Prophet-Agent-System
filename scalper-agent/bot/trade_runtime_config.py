# -*- coding: utf-8 -*-
"""Runtime trading config for safe restart.

This file is intentionally read at order time so operators can change live
limits and strategy switches without relying on a long-running Python import.
"""

import json
from datetime import datetime
from pathlib import Path
from typing import Any


SCALPER_ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = SCALPER_ROOT / "data" / "trade_runtime_config.json"

DEFAULT_CONFIG: dict[str, Any] = {
    "PAPER_ONLY": True,
    "runtime_mode": "paper_rehearsal",
    "strategy_switches": {
        "AUTO_BUY_ENABLED": False,
        "AUTO_SELL_ENABLED": False,
        "VWAP_SPLIT_BUY_ENABLED": False,
        "ENTRY_WATCH_ENABLED": False,
        "ASSET_POOL_ENABLED": False,
        "NXT_BUY_ENABLED": False,
        "QUANT_ENABLED": False,
    },
    "live_limits": {
        "max_auto_buy_orders_per_day": 1,
        "max_order_amount_krw": 300000,
        "max_total_auto_buy_amount_krw": 300000,
        "block_new_entry_after": "14:40",
    },
}


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = json.loads(json.dumps(base))
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def load_runtime_config() -> dict[str, Any]:
    try:
        data = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    except FileNotFoundError:
        data = {}
    except Exception:
        data = {"PAPER_ONLY": True, "runtime_mode": "config_error"}
    return _deep_merge(DEFAULT_CONFIG, data if isinstance(data, dict) else {})


def is_paper_only() -> bool:
    return _truthy(load_runtime_config().get("PAPER_ONLY"))


def strategy_enabled(name: str, default: bool = False) -> bool:
    switches = load_runtime_config().get("strategy_switches", {}) or {}
    return _truthy(switches.get(name, default))


def live_limits() -> dict[str, Any]:
    return load_runtime_config().get("live_limits", {}) or {}


def resolve_strategy(side: str, source: str) -> str:
    side_u = str(side or "").upper()
    source_l = str(source or "").lower()
    if side_u == "SELL":
        return "AUTO_SELL_ENABLED"
    if "vwap" in source_l:
        return "VWAP_SPLIT_BUY_ENABLED"
    if "entry_watch" in source_l or source_l in {"safe_buy", "chase_buy"}:
        return "ENTRY_WATCH_ENABLED"
    if "asset_pool" in source_l:
        return "ASSET_POOL_ENABLED"
    if "nxt" in source_l:
        return "NXT_BUY_ENABLED"
    if "quant" in source_l:
        return "QUANT_ENABLED"
    return "AUTO_BUY_ENABLED" if side_u == "BUY" else "AUTO_SELL_ENABLED"


def _after_cutoff(cutoff: str) -> bool:
    if not cutoff:
        return False
    try:
        hh, mm = [int(x) for x in cutoff.split(":", 1)]
    except Exception:
        return False
    now = datetime.now()
    return (now.hour, now.minute) >= (hh, mm)


def evaluate_auto_order(
    side: str,
    code: str,
    qty: int,
    source: str,
    estimate_amount_krw: int | None = None,
) -> dict[str, Any] | None:
    """Return a block dict when an automated order should not proceed."""
    side_u = str(side or "").upper()
    cfg = load_runtime_config()

    if _truthy(cfg.get("PAPER_ONLY")):
        return {
            "reason": "PAPER_ONLY",
            "message": "PAPER_ONLY=True - automated real order blocked",
        }

    if side_u == "BUY" and not strategy_enabled("AUTO_BUY_ENABLED"):
        return {
            "reason": "AUTO_BUY_DISABLED",
            "message": "AUTO_BUY_ENABLED=false - automated buy blocked",
        }
    if side_u == "SELL" and not strategy_enabled("AUTO_SELL_ENABLED"):
        return {
            "reason": "AUTO_SELL_DISABLED",
            "message": "AUTO_SELL_ENABLED=false - automated sell blocked",
        }

    strategy = resolve_strategy(side_u, source)
    if strategy and not strategy_enabled(strategy, default=False):
        return {
            "reason": f"{strategy}_DISABLED",
            "message": f"{strategy}=false - automated order blocked",
        }

    limits = live_limits()
    if side_u == "BUY" and _after_cutoff(str(limits.get("block_new_entry_after", ""))):
        return {
            "reason": "ENTRY_TIME_CUTOFF",
            "message": f"new entries blocked after {limits.get('block_new_entry_after')}",
        }

    max_amount = int(limits.get("max_order_amount_krw") or 0)
    if side_u == "BUY" and estimate_amount_krw is not None and max_amount > 0:
        if int(estimate_amount_krw) > max_amount:
            return {
                "reason": "ORDER_AMOUNT_LIMIT",
                "message": f"estimated order amount {estimate_amount_krw:,} > {max_amount:,}",
            }

    if side_u == "BUY":
        try:
            from bot.order_intent import count_allowed_auto_buys_today, sum_allowed_auto_buy_amount_today

            max_orders = int(limits.get("max_auto_buy_orders_per_day") or 0)
            if max_orders > 0 and count_allowed_auto_buys_today() >= max_orders:
                return {
                    "reason": "DAILY_BUY_COUNT_LIMIT",
                    "message": f"daily auto buy count limit reached ({max_orders})",
                }
            max_total = int(limits.get("max_total_auto_buy_amount_krw") or 0)
            if estimate_amount_krw is not None and max_total > 0:
                current_total = sum_allowed_auto_buy_amount_today()
                if current_total + int(estimate_amount_krw) > max_total:
                    return {
                        "reason": "DAILY_BUY_AMOUNT_LIMIT",
                        "message": (
                            f"daily auto buy amount {current_total + int(estimate_amount_krw):,} "
                            f"> {max_total:,}"
                        ),
                    }
        except Exception as exc:
            return {
                "reason": "ORDER_INTENT_LIMIT_ERROR",
                "message": f"order intent limit check failed: {exc}",
            }

    return None
