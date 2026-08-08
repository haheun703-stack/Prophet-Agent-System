# -*- coding: utf-8 -*-

import json
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from data.flowx_paper_adapter import (  # noqa: E402
    FlowXPaperEventAdapter,
    PaperEventRejected,
)
from data.flowx_paper_summary import (  # noqa: E402
    build_flowx_paper_summary,
    write_flowx_paper_summary,
)


NOW = datetime(2026, 8, 9, 9, 0, tzinfo=timezone.utc)  # 18:00 KST
COSTS = {"fee": 0, "tax": 0, "slippage": 0, "spread": 0}


class FlowXPaperSummaryTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.events = self.root / "events"
        self.adapter = FlowXPaperEventAdapter(self.events)

    def tearDown(self):
        self.temp.cleanup()

    def _emit_trade(self, trade_id, strategy, version, market, currency, ticker,
                    complete=True, at=NOW - timedelta(hours=1)):
        base = {
            "trade_id": trade_id,
            "strategy_id": strategy,
            "strategy_version": version,
            "market": market,
            "currency": currency,
            "ticker": ticker,
            "event_at": at,
            "data_asof": at - timedelta(seconds=1),
            "real_order": False,
            "paper": True,
        }
        costs = COSTS if complete else {}
        self.adapter.emit(
            **base, event_type="PAPER_FILL", source_record_id=f"{trade_id}-buy",
            fill={"side": "BUY", "price": 100, "qty": 1}, **costs,
        )
        self.adapter.emit(
            **base, event_type="EXIT", source_record_id=f"{trade_id}-exit",
            fill={"side": "SELL", "price": 101, "qty": 1}, **costs,
        )

    def test_groups_strategy_version_market_and_cost_completeness(self):
        self._emit_trade("KR-1", "scalper", "4", "KR", "KRW", "005930")
        self._emit_trade(
            "KR-2", "scalper", "4", "KR", "KRW", "000660", complete=False
        )
        self._emit_trade("US-1", "scalper", "5", "US", "USD", "AAPL")

        summary = build_flowx_paper_summary(event_dir=self.events, as_of=NOW)

        self.assertTrue(summary["paper_only"])
        self.assertEqual(summary["as_of"], "2026-08-09T09:00:00Z")
        self.assertEqual(summary["totals"]["strategy_market_count"], 2)
        self.assertEqual(summary["totals"]["closed_trade_count"], 3)
        kr = summary["strategies"][0]
        self.assertEqual(
            (kr["strategy_id"], kr["strategy_version"], kr["market"]),
            ("scalper", "4", "KR"),
        )
        self.assertEqual(kr["sample"]["closed_trade_count"], 2)
        self.assertEqual(kr["sample"]["cost_complete_count"], 1)
        self.assertEqual(kr["sample"]["cost_incomplete_count"], 1)
        self.assertEqual(kr["sample"]["cost_complete_rate"], 0.5)
        self.assertEqual(kr["period"]["events"]["start"], "2026-08-09T08:00:00Z")

    def test_cutoff_excludes_later_events_and_empty_is_valid(self):
        self._emit_trade(
            "LATER", "scalper", "4", "KR", "KRW", "005930",
            at=NOW + timedelta(minutes=1),
        )
        summary = build_flowx_paper_summary(event_dir=self.events, as_of=NOW)
        self.assertEqual(summary["totals"]["event_count"], 0)
        self.assertEqual(summary["strategies"], [])

    def test_tampered_real_order_fails_closed(self):
        self.events.mkdir(parents=True)
        row = {
            "event_id": "bad", "trade_id": "T", "event_type": "EXIT",
            "strategy_id": "s", "strategy_version": "1", "market": "KR",
            "currency": "KRW", "ticker": "005930", "event_at": NOW.isoformat(),
            "data_asof": (NOW - timedelta(seconds=1)).isoformat(),
            "cost_complete": True, "real_order": True,
        }
        (self.events / "flowx_paper_events_2026-08-09.jsonl").write_text(
            json.dumps(row) + "\n", encoding="utf-8"
        )
        with self.assertRaises(PaperEventRejected):
            build_flowx_paper_summary(event_dir=self.events, as_of=NOW)

    def test_writes_separate_atomic_artifact_without_touching_ledger(self):
        self._emit_trade("KR-1", "scalper", "4", "KR", "KRW", "005930")
        ledger = next(self.events.glob("*.jsonl"))
        before = ledger.read_bytes()
        output = self.root / "summary" / "latest.json"

        summary = build_flowx_paper_summary(event_dir=self.events, as_of=NOW)
        written = write_flowx_paper_summary(
            summary, output, source_event_dir=self.events
        )

        self.assertEqual(written, output)
        self.assertEqual(before, ledger.read_bytes())
        self.assertEqual(json.loads(output.read_text(encoding="utf-8")), summary)
        self.assertEqual(list(output.parent.glob("*.tmp")), [])

    def test_output_cannot_overwrite_or_enter_event_ledger(self):
        self._emit_trade("KR-1", "scalper", "4", "KR", "KRW", "005930")
        ledger = next(self.events.glob("*.jsonl"))
        before = ledger.read_bytes()
        summary = build_flowx_paper_summary(event_dir=self.events, as_of=NOW)

        with self.assertRaises(PaperEventRejected):
            write_flowx_paper_summary(
                summary, ledger, source_event_dir=self.events
            )
        with self.assertRaises(PaperEventRejected):
            write_flowx_paper_summary(
                summary, self.events / "latest.json", source_event_dir=self.events
            )
        self.assertEqual(ledger.read_bytes(), before)

    def test_multiple_exits_for_one_trade_fail_closed(self):
        self._emit_trade("KR-1", "scalper", "4", "KR", "KRW", "005930")
        at = NOW - timedelta(minutes=30)
        self.adapter.emit(
            trade_id="KR-1", strategy_id="scalper", strategy_version="4",
            market="KR", currency="KRW", ticker="005930", event_at=at,
            data_asof=at - timedelta(seconds=1), event_type="EXIT",
            source_record_id="KR-1-second-exit",
            fill={"side": "SELL", "price": 102, "qty": 1}, **COSTS,
        )
        with self.assertRaises(PaperEventRejected):
            build_flowx_paper_summary(event_dir=self.events, as_of=NOW)

    def test_forged_cost_complete_fails_closed(self):
        self.events.mkdir(parents=True)
        row = {
            "event_id": "forged", "trade_id": "T", "event_type": "EXIT",
            "strategy_id": "s", "strategy_version": "1", "market": "KR",
            "currency": "KRW", "ticker": "005930", "event_at": NOW.isoformat(),
            "data_asof": (NOW - timedelta(seconds=1)).isoformat(),
            "fee": None, "tax": 0, "slippage": 0, "spread": 0,
            "cost_complete": True, "net_return": 0.1,
            "net_return_certified": True, "real_order": False,
        }
        (self.events / "flowx_paper_events_2026-08-09.jsonl").write_text(
            json.dumps(row) + "\n", encoding="utf-8"
        )
        with self.assertRaises(PaperEventRejected):
            build_flowx_paper_summary(event_dir=self.events, as_of=NOW)

    def test_trade_id_cannot_span_market_or_ticker_groups(self):
        self._emit_trade("SAME", "scalper", "4", "KR", "KRW", "005930")
        at = NOW - timedelta(minutes=30)
        base = {
            "trade_id": "SAME", "strategy_id": "scalper",
            "strategy_version": "4", "market": "US", "currency": "USD",
            "ticker": "AAPL", "event_at": at,
            "data_asof": at - timedelta(seconds=1), "real_order": False,
            "paper": True,
        }
        self.adapter.emit(
            **base, event_type="PAPER_FILL", source_record_id="same-us-buy",
            fill={"side": "BUY", "price": 100, "qty": 1}, **COSTS,
        )
        self.adapter.emit(
            **base, event_type="EXIT", source_record_id="same-us-exit",
            fill={"side": "SELL", "price": 101, "qty": 1}, **COSTS,
        )
        with self.assertRaises(PaperEventRejected):
            build_flowx_paper_summary(event_dir=self.events, as_of=NOW)


if __name__ == "__main__":
    unittest.main(verbosity=2)
