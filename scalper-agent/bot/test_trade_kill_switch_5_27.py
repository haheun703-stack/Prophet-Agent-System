# -*- coding: utf-8 -*-
"""Runtime kill-switch regression tests.

These tests verify that AUTO_TRADE_DISABLED blocks both buy and sell order
paths before any KIS network order can be attempted.
"""

import asyncio
import importlib
import sys
import types
from pathlib import Path
from unittest.mock import MagicMock, patch

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from bot.kis_trader import KISTrader
from bot.trade_kill_switch import is_auto_trade_disabled
from bot.vwap_split_buy import execute_vwap_split_buy
from engine.trading_engine import TradingEngine
from strategies.base_strategy import Signal, TradeSignal


class _ApprovedRiskManager:
    def approve_buy(self, signal, price):
        return True, 3, "approved"

    def approve_sell(self, signal):
        return True, 3, "approved"


class _TradeLogStub:
    def log_signal(self, *args, **kwargs):
        return None


class _SignalStub:
    def connect(self, callback):
        return None


class _KiwoomCoreStub:
    sig_chejan = _SignalStub()

    def __init__(self):
        self.send_order = MagicMock(return_value=0)

    def get_next_screen(self):
        return "9000"


def _live_engine_with_kiwoom_order():
    engine = TradingEngine.__new__(TradingEngine)
    engine.mode = "live"
    engine.risk_manager = _ApprovedRiskManager()
    engine.trade_log = _TradeLogStub()
    engine.kiwoom_order = MagicMock()
    engine.order_manager = MagicMock()
    engine.paper_trader = None
    tick_buffer = MagicMock()
    tick_buffer.get_last_price.return_value = 70000
    engine.tick_buffers = {"005930": tick_buffer}
    engine.portfolio = MagicMock()
    return engine


def test_runtime_kill_switch_is_on():
    assert is_auto_trade_disabled() is True


def test_kill_switch_file_missing_fail_close(monkeypatch, tmp_path):
    from bot import trade_kill_switch

    monkeypatch.delenv("AUTO_TRADE_DISABLED", raising=False)
    monkeypatch.setattr(
        trade_kill_switch,
        "KILL_SWITCH_PATH",
        tmp_path / "missing_kill_switch.json",
    )

    assert trade_kill_switch.file_auto_trade_disabled() is True
    assert trade_kill_switch.is_auto_trade_disabled() is True


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


def test_kiwoom_order_kill_switch_guard():
    engine = _live_engine_with_kiwoom_order()
    buy_signal = TradeSignal(
        signal=Signal.BUY,
        code="005930",
        confidence=1.0,
        reason="test",
        stop_loss=60000,
        take_profit=80000,
    )
    sell_signal = TradeSignal(
        signal=Signal.SELL,
        code="005930",
        confidence=1.0,
        reason="test",
    )

    with patch("engine.trading_engine.is_auto_trade_disabled", return_value=True):
        engine._execute_buy(buy_signal)
        engine._execute_sell(sell_signal)

    engine.kiwoom_order.buy_market.assert_not_called()
    engine.kiwoom_order.sell_market.assert_not_called()
    engine.order_manager.register.assert_not_called()


def test_kiwoom_order_class_kill_switch_guard():
    kiwoom_core_stub = types.ModuleType("api.kiwoom_core")
    kiwoom_core_stub.KiwoomCore = object

    with patch.dict(sys.modules, {"api.kiwoom_core": kiwoom_core_stub}):
        kiwoom_order_mod = importlib.import_module("api.kiwoom_order")

    core = _KiwoomCoreStub()
    order = kiwoom_order_mod.KiwoomOrder(core, "12345678")

    with patch.object(kiwoom_order_mod, "is_auto_trade_disabled", return_value=True):
        assert order.buy_market("005930", 1) == -1
        assert order.buy_limit("005930", 1, 70000) == -1
        assert order.buy_best("005930", 1) == -1
        assert order.sell_market("005930", 1) == -1
        assert order.sell_limit("005930", 1, 70000) == -1

    core.send_order.assert_not_called()
