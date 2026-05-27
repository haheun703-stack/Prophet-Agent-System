# -*- coding: utf-8 -*-
"""Runtime kill-switch regression tests.

These tests verify that AUTO_TRADE_DISABLED blocks both buy and sell order
paths before any KIS network order can be attempted.
"""

import asyncio
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from bot.kis_trader import KISTrader
from bot.trade_kill_switch import is_auto_trade_disabled
from bot.vwap_split_buy import execute_vwap_split_buy


def test_runtime_kill_switch_is_on():
    assert is_auto_trade_disabled() is True


def test_kis_order_gate_blocks_buy_and_sell():
    trader = KISTrader()
    buy_block = trader._order_gate("BUY", "007390", 1, source="test")
    sell_block = trader._order_gate("SELL", "007390", 1, source="test")
    assert buy_block and buy_block.get("blocked") is True
    assert sell_block and sell_block.get("blocked") is True


def test_vwap_split_buy_blocks_before_trader_call():
    class RaisingTrader:
        def chase_buy(self, *args, **kwargs):
            raise AssertionError("chase_buy must not be called while kill switch is on")

        def buy_market(self, *args, **kwargs):
            raise AssertionError("buy_market must not be called while kill switch is on")

    result = asyncio.run(
        execute_vwap_split_buy(
            RaisingTrader(),
            "007390",
            "NatureCell",
            33000,
            197,
        )
    )
    assert result.success is False
    assert result.total_bought_qty == 0
