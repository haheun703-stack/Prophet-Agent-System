# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from data.flowx_evidence_bridge import (
    EvidenceBridgeRejected,
    attach_public_evidence,
    candidate_decision_at,
    iter_public_evidence_paths,
    load_best_public_evidence,
    load_public_evidence,
)


DECISION = datetime(2026, 8, 13, 5, 30, tzinfo=timezone.utc)


def _snapshot(generated_at="2026-08-13T05:00:00Z"):
    return {
        "schema_version": "flowx.stock-evidence-batch.v1",
        "item_contract": "flowx.stock-evidence.v1",
        "mode": "paper_read_only",
        "generated_at": generated_at,
        "items": [{
            "instrument": {"market": "KR", "ticker": "005930"},
            "freshness": {"components": {"catalysts": {"status": "fresh"}}},
            "catalysts": [
                {"kind": "news", "published_at": "2026-08-13T04:50:00Z"},
                {"kind": "disclosure", "published_at": "2026-08-13T05:40:00Z"},
            ],
        }],
    }


class FlowXEvidenceBridgeTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.path = Path(self.temp.name) / "stock_evidence_latest.json"

    def tearDown(self):
        self.temp.cleanup()

    def _write(self, document):
        self.path.write_text(json.dumps(document), encoding="utf-8")

    def test_excludes_post_decision_catalyst_and_attaches_reference(self):
        self._write(_snapshot())
        index = load_public_evidence(self.path, DECISION)
        enriched = attach_public_evidence(
            {"code": "005930", "trade_object": {"market": "KR", "sources": ["v2"]}},
            index,
        )

        trade = enriched["trade_object"]
        self.assertEqual(trade["evidence_catalyst_count"], 1)
        self.assertEqual(trade["evidence_catalyst_kinds"], ["news"])
        self.assertTrue(trade["evidence_ref"].endswith(":KR:005930"))
        self.assertIn(":sha256-", trade["evidence_ref"])
        self.assertEqual(trade["sources"], ["v2", "flowx_public_evidence"])

    def test_fails_closed_for_untrusted_snapshot(self):
        for field, value in (
            ("mode", "live"),
            ("generated_at", "2026-08-13T05:31:00Z"),
            ("schema_version", "unknown"),
        ):
            with self.subTest(field=field):
                document = _snapshot()
                document[field] = value
                self._write(document)
                with self.assertRaises(EvidenceBridgeRejected):
                    load_public_evidence(self.path, DECISION)

    def test_ignores_stale_catalyst_component(self):
        document = _snapshot()
        document["items"][0]["freshness"]["components"]["catalysts"]["status"] = "stale"
        self._write(document)
        self.assertEqual(load_public_evidence(self.path, DECISION), {})

    def test_ignores_old_catalyst_even_if_component_claims_fresh(self):
        document = _snapshot()
        document["items"][0]["catalysts"] = [
            {"kind": "news", "published_at": "2026-08-01T04:50:00Z"},
        ]
        self._write(document)
        self.assertEqual(load_public_evidence(self.path, DECISION), {})

    def test_lists_latest_then_dated_handoff_paths(self):
        directory = Path(self.temp.name)
        latest = directory / "stock_evidence_latest.json"
        dated = directory / "stock_evidence_2026-08-13.json"
        latest.write_text("{}", encoding="utf-8")
        dated.write_text("{}", encoding="utf-8")
        self.assertEqual(iter_public_evidence_paths((directory,)), (latest, dated))

    def test_bad_or_post_decision_latest_falls_back_to_dated_snapshot(self):
        directory = Path(self.temp.name)
        latest = directory / "stock_evidence_latest.json"
        dated = directory / "stock_evidence_2026-08-12.json"
        latest.write_text("[]", encoding="utf-8")
        dated.write_text(json.dumps(_snapshot()), encoding="utf-8")
        paths = iter_public_evidence_paths((directory,))
        self.assertIn(("KR", "005930"), load_best_public_evidence(paths, DECISION))

    def test_newest_valid_snapshot_wins_across_directories(self):
        first = Path(self.temp.name) / "first"
        second = Path(self.temp.name) / "second"
        first.mkdir()
        second.mkdir()
        older = _snapshot("2026-08-13T04:00:00Z")
        newer = _snapshot("2026-08-13T05:00:00Z")
        newer["items"][0]["catalysts"][0]["kind"] = "disclosure"
        (first / "stock_evidence_2026-08-13.json").write_text(
            json.dumps(older), encoding="utf-8"
        )
        (second / "stock_evidence_latest.json").write_text(
            json.dumps(newer), encoding="utf-8"
        )

        index = load_best_public_evidence(
            iter_public_evidence_paths((first, second)), DECISION
        )
        self.assertEqual(index[("KR", "005930")]["evidence_catalyst_kinds"], ["disclosure"])

    def test_legacy_candidate_time_is_interpreted_as_kst(self):
        parsed = candidate_decision_at("2026-08-13 14:30")
        self.assertEqual(parsed.isoformat(), "2026-08-13T05:30:00+00:00")

    def test_candidate_time_without_value_is_rejected(self):
        with self.assertRaises(EvidenceBridgeRejected):
            candidate_decision_at("")


if __name__ == "__main__":
    unittest.main(verbosity=2)
