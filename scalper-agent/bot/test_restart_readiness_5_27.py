# -*- coding: utf-8 -*-
"""Restart readiness regression tests for limited-live relaunch."""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from bot.manual_position_protection import auto_sell_block_reason
from bot.order_intent import record_order_intent
from bot.trade_runtime_config import is_paper_only, load_runtime_config, resolve_strategy


def test_runtime_config_defaults_safe():
    cfg = load_runtime_config()
    assert is_paper_only() is True
    assert cfg["strategy_switches"]["AUTO_BUY_ENABLED"] is False
    assert cfg["strategy_switches"]["AUTO_SELL_ENABLED"] is False
    assert cfg["live_limits"]["max_order_amount_krw"] <= 300000


def test_strategy_resolution():
    assert resolve_strategy("BUY", "market") == "AUTO_BUY_ENABLED"
    assert resolve_strategy("BUY", "chase_buy") == "ENTRY_WATCH_ENABLED"
    assert resolve_strategy("BUY", "vwap_split_buy") == "VWAP_SPLIT_BUY_ENABLED"
    assert resolve_strategy("SELL", "market") == "AUTO_SELL_ENABLED"


def test_order_intent_can_record_blocked_probe():
    row = record_order_intent(
        side="BUY",
        code="005930",
        qty=1,
        source="restart_readiness_test",
        manual=False,
        allowed=False,
        reason="TEST_BLOCK",
        message="test blocked intent",
        strategy="AUTO_BUY_ENABLED",
        estimate_amount_krw=0,
    )
    assert row["allowed"] is False
    assert row["reason"] == "TEST_BLOCK"
    assert row["code"] == "005930"


def test_unknown_position_blocks_auto_sell():
    assert auto_sell_block_reason("000000") == "manual_or_unknown_position"


if __name__ == "__main__":
    tests = [
        test_runtime_config_defaults_safe,
        test_strategy_resolution,
        test_order_intent_can_record_blocked_probe,
        test_unknown_position_blocks_auto_sell,
    ]
    passed = 0
    for test in tests:
        test()
        passed += 1
        print(f"[PASS] {test.__name__}")
    print(f"restart readiness tests: {passed}/{len(tests)} PASS")
