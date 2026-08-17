# -*- coding: utf-8 -*-
import json
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from data.flowx_paper_adapter import (  # noqa: E402
    FlowXPaperEventAdapter,
    PaperEventRejected,
)
from data.trade_tracker import (  # noqa: E402
    TradeTracker,
    _emit_flowx_paper_exit,
    _emit_flowx_paper_open,
)
import data.trade_tracker as trade_tracker_module  # noqa: E402
from data.trade_object import TradeObject  # noqa: E402


UTC_NOW = datetime(2026, 8, 9, 1, 0, tzinfo=timezone.utc)


class FlowXPaperAdapterTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.event_dir = Path(self.temp.name)
        self.adapter = FlowXPaperEventAdapter(self.event_dir)
        self.base = {
            "trade_id": "T-KR-1",
            "strategy_id": "scalper",
            "strategy_version": "4.0.0",
            "market": "KR",
            "currency": "KRW",
            "ticker": "005930",
            "event_at": UTC_NOW,
            "data_asof": UTC_NOW - timedelta(seconds=1),
            "benchmark": {"ticker": "KOSPI", "return": 0.001},
            "exposure": {"gross": 1000000},
            "regime": "RISK_ON",
            "real_order": False,
            "paper": True,
        }

    def tearDown(self):
        self.temp.cleanup()

    def test_required_schema_incomplete_cost_and_idempotency(self):
        first = self.adapter.emit(
            **self.base,
            event_type="PAPER_FILL",
            source_record_id="source-buy-1",
            fill={"side": "BUY", "price": 70000, "qty": 2},
        )
        path = next(self.event_dir.glob("*.jsonl"))
        before = path.read_bytes()
        retry_base = dict(self.base)
        retry_base["event_at"] = UTC_NOW + timedelta(seconds=5)
        retry = self.adapter.emit(
            **retry_base,
            event_type="PAPER_FILL",
            source_record_id="source-buy-1",
            fill={"side": "BUY", "price": 70000, "qty": 2},
        )

        required = {
            "event_id", "trade_id", "event_type", "strategy_id",
            "strategy_version", "market", "currency", "ticker",
            "event_at", "data_asof", "fill", "fee", "tax",
            "slippage", "spread", "net_return", "benchmark",
            "exposure", "regime", "source_record_id", "real_order",
            "cost_complete",
        }
        self.assertTrue(required.issubset(first))
        self.assertFalse(first["cost_complete"])
        self.assertIsNone(first["net_return"])
        self.assertFalse(first["net_return_certified"])
        self.assertFalse(first["real_order"])
        self.assertEqual(first["event_id"], retry["event_id"])
        self.assertEqual(first["event_at"], retry["event_at"])
        self.assertTrue(retry["deduped"])
        self.assertEqual(before, path.read_bytes(), "retry must not append or rewrite")
        changed_asof = dict(self.base)
        changed_asof["data_asof"] = UTC_NOW - timedelta(seconds=2)
        with self.assertRaises(PaperEventRejected):
            self.adapter.emit(
                **changed_asof,
                event_type="PAPER_FILL",
                source_record_id="source-buy-1",
                fill={"side": "BUY", "price": 70000, "qty": 2},
            )
        with self.assertRaises(PaperEventRejected):
            self.adapter.emit(
                **self.base,
                event_type="PAPER_FILL",
                source_record_id="source-buy-1",
                fill={"side": "BUY", "price": 71000, "qty": 2},
            )
        self.assertEqual(before, path.read_bytes(), "conflicting retry must fail closed")

    def test_notional_mismatch_and_lock_contention_fail_closed(self):
        with self.assertRaises(PaperEventRejected):
            self.adapter.emit(
                **self.base,
                event_type="PAPER_FILL",
                source_record_id="bad-notional",
                fill={"side": "BUY", "price": 100, "qty": 10, "notional": 1},
            )
        self.event_dir.mkdir(parents=True, exist_ok=True)
        lock_path = self.event_dir / ".append.lock"
        fd = self.adapter._acquire_lock(lock_path)
        try:
            with self.assertRaises(PaperEventRejected):
                self.adapter._acquire_lock(lock_path)
        finally:
            self.adapter._release_lock(fd)
            import os
            os.close(fd)

    def test_us_complete_costs_certify_net_return(self):
        base = dict(self.base)
        base.update({
            "trade_id": "T-US-1", "market": "US", "currency": "USD",
            "ticker": "AAPL", "strategy_version": "1.2.3",
        })
        costs = {"fee": 1, "tax": 1, "slippage": 1, "spread": 1}
        self.adapter.emit(
            **base,
            event_type="PAPER_FILL",
            source_record_id="us-buy",
            fill={"side": "BUY", "price": 100, "qty": 10},
            **costs,
        )
        result = self.adapter.emit(
            **base,
            event_type="EXIT",
            source_record_id="us-exit",
            fill={"side": "SELL", "price": 110, "qty": 10},
            **costs,
        )
        self.assertTrue(result["cost_complete"])
        self.assertTrue(result["net_return_certified"])
        self.assertAlmostEqual(result["net_return"], 0.092)

    def test_cross_market_identity_never_certifies_return(self):
        costs = {"fee": 0, "tax": 0, "slippage": 0, "spread": 0}
        self.adapter.emit(
            **self.base,
            event_type="PAPER_FILL",
            source_record_id="kr-buy",
            fill={"side": "BUY", "price": 1000, "qty": 1},
            **costs,
        )
        us_exit = dict(self.base)
        us_exit.update({"market": "US", "currency": "USD", "ticker": "AAPL"})
        result = self.adapter.emit(
            **us_exit,
            event_type="EXIT",
            source_record_id="us-exit-same-trade-id",
            fill={"side": "SELL", "price": 1100, "qty": 1},
            **costs,
        )
        self.assertFalse(result["net_return_certified"])
        self.assertFalse(result["cost_complete"])
        self.assertIsNone(result["net_return"])

    def test_real_order_and_future_data_fail_closed_without_file(self):
        bad = dict(self.base)
        bad["real_order"] = True
        with self.assertRaises(PaperEventRejected):
            self.adapter.emit(
                **bad, event_type="CANDIDATE", source_record_id="bad-real"
            )
        future = dict(self.base)
        future["data_asof"] = UTC_NOW + timedelta(seconds=1)
        with self.assertRaises(PaperEventRejected):
            self.adapter.emit(
                **future, event_type="CANDIDATE", source_record_id="bad-asof"
            )
        with self.assertRaises(PaperEventRejected):
            self.adapter.emit(
                **self.base, event_type="CANDIDATE", source_record_id="bad-nan",
                fee=float("nan"),
            )
        self.assertEqual(list(self.event_dir.glob("*.jsonl")), [])

    def test_malformed_existing_ledger_refuses_append(self):
        bad_path = self.event_dir / "flowx_paper_events_2026-08-09.jsonl"
        bad_path.write_text('{"truncated":', encoding="utf-8")
        before = bad_path.read_bytes()
        with self.assertRaises(PaperEventRejected):
            self.adapter.emit(
                **self.base, event_type="CANDIDATE", source_record_id="after-corruption"
            )
        self.assertEqual(before, bad_path.read_bytes())

    def test_trade_tracker_paper_path_emits_full_lifecycle(self):
        trade = {
            "trade_id": "TRACK-US-1",
            "strategy_id": "preclose",
            "strategy_version": "2",
            "market": "US",
            "currency": "USD",
            "ticker": "MSFT",
            "shares": 3,
            "position_krw": 0,
            "regime": "NEUTRAL",
        }
        with patch("data.flowx_paper_adapter.FLOWX_EVENT_DIR", self.event_dir):
            _emit_flowx_paper_open(trade, "MSFT", 400, "PAPER_OPEN")
            _emit_flowx_paper_exit(trade, "MSFT", 420, "TARGET")
        rows = []
        for path in self.event_dir.glob("*.jsonl"):
            rows.extend(json.loads(line) for line in path.read_text(encoding="utf-8").splitlines())
        self.assertEqual(
            [row["event_type"] for row in rows],
            ["CANDIDATE", "ORDER_INTENT", "PAPER_FILL", "ORDER_INTENT", "EXIT"],
        )
        self.assertTrue(all(row["market"] == "US" for row in rows))
        self.assertTrue(all(row["real_order"] is False for row in rows))
        self.assertIsNone(rows[-1]["net_return"])
        self.assertFalse(rows[-1]["cost_complete"])

    def test_callsite_crash_retry_recovers_persisted_data_asof(self):
        original = {
            "trade_id": "CRASH-RETRY-1", "strategy_id": "retry",
            "strategy_version": "1", "market": "US", "currency": "USD",
            "ticker": "NVDA", "shares": 1,
        }
        with patch("data.flowx_paper_adapter.FLOWX_EVENT_DIR", self.event_dir):
            _emit_flowx_paper_open(original, "NVDA", 180, "PAPER_OPEN")
            restarted = {k: v for k, v in original.items() if k != "data_asof"}
            _emit_flowx_paper_open(restarted, "NVDA", 180, "PAPER_OPEN")
            _emit_flowx_paper_exit(original, "NVDA", 190, "TARGET")
            restarted.pop("exit_data_asof", None)
            _emit_flowx_paper_exit(restarted, "NVDA", 190, "TARGET")
        rows = list(FlowXPaperEventAdapter(self.event_dir).iter_events())
        self.assertEqual(len(rows), 5)
        self.assertEqual(len({row["event_id"] for row in rows}), 5)

    def test_real_trade_tracker_call_sites_export_and_close_us_paper(self):
        candidate = {
            "code": "MSFT",
            "name": "Microsoft",
            "trade_object": {
                "trade_id": "TRACKER-CALLSITE-US",
                "entry_price": 400,
                "stop_loss": 390,
                "target_price": 410,
                "shares": 3,
                "sources": ["flowx_test"],
                "market": "US",
                "currency": "USD",
                "ticker": "MSFT",
                "strategy_id": "flowx_test",
                "strategy_version": "1",
                "regime": "NEUTRAL",
            },
        }

        class FakePaperPrice:
            @staticmethod
            def fetch_price(code):
                return {
                    "success": code == "MSFT", "high": 420, "low": 400,
                    "current_price": 415,
                }

        active_path = self.event_dir / "active_trades.json"
        with (
            patch("data.flowx_paper_adapter.FLOWX_EVENT_DIR", self.event_dir),
            patch.object(trade_tracker_module, "ACTIVE_PATH", active_path),
            patch("bot.order_intent.record_order_intent", return_value={}),
        ):
            tracker = TradeTracker()
            self.assertEqual(tracker.register_paper_preclose([candidate]), ["Microsoft"])
            messages = tracker.check_paper_prices(FakePaperPrice())
            self.assertEqual(len(messages), 1)
            self.assertFalse(tracker.is_tracked("MSFT"))

        rows = list(FlowXPaperEventAdapter(self.event_dir).iter_events())
        self.assertEqual(len(rows), 5)
        self.assertEqual(rows[-1]["event_type"], "EXIT")
        self.assertEqual(rows[-1]["market"], "US")
        self.assertEqual(rows[-1]["currency"], "USD")
        self.assertFalse(rows[-1]["real_order"])

    def test_main_paper_object_path_preserves_us_metadata_through_exit(self):
        trade = TradeObject(
            trade_id="MAIN-US-1", code="AAPL", name="Apple",
            total_score=88, sources=["main_us"], regime="RISK_ON",
            entry_price=100, stop_loss=95, target_price=105,
            rr_verdict="ACCEPT", shares=10,
        )
        for field, value in {
            "market": "US", "currency": "USD", "ticker": "AAPL",
            "strategy_id": "main_us", "strategy_version": "3",
            "entry_fee": 0, "entry_tax": 0, "entry_slippage": 0, "entry_spread": 0,
            "exit_fee": 0, "exit_tax": 0, "exit_slippage": 0, "exit_spread": 0,
        }.items():
            setattr(trade, field, value)

        class FakePaperPrice:
            @staticmethod
            def fetch_price(code):
                return {
                    "success": code == "AAPL", "open": 100, "high": 105,
                    "low": 100, "current_price": 105,
                }

        active_path = self.event_dir / "main_active.json"
        with (
            patch("data.flowx_paper_adapter.FLOWX_EVENT_DIR", self.event_dir),
            patch.object(trade_tracker_module, "ACTIVE_PATH", active_path),
            patch("data.trade_object.load_trade_objects", return_value=[trade]),
            patch("bot.order_intent.record_order_intent", return_value={}),
        ):
            tracker = TradeTracker()
            self.assertEqual(tracker.register_paper_from_objects(FakePaperPrice()), ["Apple"])
            self.assertEqual(tracker.get_active("AAPL")["market"], "US")
            self.assertEqual(len(tracker.check_paper_prices(FakePaperPrice())), 1)

        rows = list(FlowXPaperEventAdapter(self.event_dir).iter_events())
        self.assertEqual(len(rows), 5)
        self.assertTrue(all(row["market"] == "US" for row in rows))
        self.assertTrue(all(row["strategy_version"] == "3" for row in rows))
        self.assertTrue(rows[-1]["net_return_certified"])
        self.assertAlmostEqual(rows[-1]["net_return"], 0.05)

    def test_main_paper_object_path_attaches_public_evidence_before_emit(self):
        trade = TradeObject(
            trade_id="MAIN-KR-EVIDENCE-1", code="005930", name="Samsung",
            total_score=80, sources=["main_kr"], regime="NEUTRAL",
            entry_price=100, stop_loss=95, target_price=105,
            rr_verdict="ACCEPT", shares=10,
        )
        for field, value in {
            "market": "KR", "currency": "KRW", "ticker": "005930",
            "strategy_id": "main_kr", "strategy_version": "1",
            "created_at": "2026-08-18T08:00:00+09:00",
        }.items():
            setattr(trade, field, value)

        evidence_path = self.event_dir / "stock_evidence_latest.json"
        evidence_now = datetime(2026, 8, 17, 22, 0, tzinfo=timezone.utc)
        evidence_path.write_text(json.dumps({
            "schema_version": "flowx.stock-evidence-batch.v1",
            "item_contract": "flowx.stock-evidence.v1",
            "mode": "paper_read_only",
            "generated_at": evidence_now.isoformat(),
            "items": [{
                "instrument": {"market": "KR", "ticker": "005930"},
                "freshness": {"components": {"catalysts": {"status": "fresh"}}},
                "catalysts": [{
                    "kind": "disclosure",
                    "published_at": (evidence_now - timedelta(minutes=1)).isoformat(),
                }],
            }],
        }), encoding="utf-8")

        active_path = self.event_dir / "evidence_active.json"
        with (
            patch("data.flowx_paper_adapter.FLOWX_EVENT_DIR", self.event_dir),
            patch.object(trade_tracker_module, "ACTIVE_PATH", active_path),
            patch("data.trade_object.load_trade_objects", return_value=[trade]),
            patch("bot.order_intent.record_order_intent", return_value={}),
        ):
            tracker = TradeTracker()
            self.assertEqual(
                tracker.register_paper_from_objects(evidence_path=evidence_path),
                ["Samsung"],
            )

        candidate = next(
            row for row in FlowXPaperEventAdapter(self.event_dir).iter_events()
            if row["event_type"] == "CANDIDATE"
        )
        self.assertEqual(candidate["details"]["evidence"]["evidence_catalyst_count"], 1)
        self.assertEqual(
            candidate["details"]["evidence"]["evidence_catalyst_kinds"],
            ["disclosure"],
        )
        self.assertIn("flowx_public_evidence", trade.sources)

    def test_main_paper_object_rejects_evidence_created_after_candidate(self):
        trade = TradeObject(
            trade_id="MAIN-KR-NO-LOOKAHEAD", code="005930", name="Samsung",
            total_score=80, sources=["main_kr"], entry_price=100,
            rr_verdict="ACCEPT", shares=10,
            created_at="2026-08-17 16:58",
        )
        evidence_path = self.event_dir / "stock_evidence_latest.json"
        evidence_path.write_text(json.dumps({
            "schema_version": "flowx.stock-evidence-batch.v1",
            "item_contract": "flowx.stock-evidence.v1",
            "mode": "paper_read_only",
            "generated_at": "2026-08-17T09:00:00Z",
            "items": [{
                "instrument": {"market": "KR", "ticker": "005930"},
                "freshness": {"components": {"catalysts": {"status": "fresh"}}},
                "catalysts": [{
                    "kind": "news", "published_at": "2026-08-17T08:30:00Z",
                }],
            }],
        }), encoding="utf-8")

        active_path = self.event_dir / "no_lookahead_active.json"
        with (
            patch("data.flowx_paper_adapter.FLOWX_EVENT_DIR", self.event_dir),
            patch.object(trade_tracker_module, "ACTIVE_PATH", active_path),
            patch("data.trade_object.load_trade_objects", return_value=[trade]),
            patch("bot.order_intent.record_order_intent", return_value={}),
        ):
            tracker = TradeTracker()
            self.assertEqual(
                tracker.register_paper_from_objects(evidence_path=evidence_path),
                ["Samsung"],
            )

        candidate = next(
            row for row in FlowXPaperEventAdapter(self.event_dir).iter_events()
            if row["event_type"] == "CANDIDATE"
        )
        self.assertEqual(candidate["details"]["evidence"], {})
        self.assertNotIn("flowx_public_evidence", trade.sources)

    def test_same_day_same_ticker_reentry_gets_distinct_trade_ids(self):
        candidate = {
            "code": "005930", "name": "Samsung", "current_price": 100,
            "market": "KR", "currency": "KRW", "strategy_id": "reentry",
            "strategy_version": "1",
        }

        class StopPrice:
            @staticmethod
            def fetch_price(code):
                return {
                    "success": code == "005930", "high": 100, "low": 90,
                    "current_price": 90,
                }

        active_path = self.event_dir / "reentry_active.json"
        with (
            patch("data.flowx_paper_adapter.FLOWX_EVENT_DIR", self.event_dir),
            patch.object(trade_tracker_module, "ACTIVE_PATH", active_path),
            patch("bot.order_intent.record_order_intent", return_value={}),
        ):
            tracker = TradeTracker()
            trade_ids = []
            for _ in range(2):
                self.assertEqual(tracker.register_paper_preclose([candidate]), ["Samsung"])
                trade_ids.append(tracker.get_active("005930")["trade_id"])
                self.assertEqual(len(tracker.check_paper_prices(StopPrice())), 1)
            self.assertNotEqual(trade_ids[0], trade_ids[1])

        rows = list(FlowXPaperEventAdapter(self.event_dir).iter_events())
        self.assertEqual(len(rows), 10)
        self.assertEqual(len({row["trade_id"] for row in rows}), 2)


if __name__ == "__main__":
    unittest.main(verbosity=2)
