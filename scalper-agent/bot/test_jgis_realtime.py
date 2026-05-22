# -*- coding: utf-8 -*-
"""jgis_signal_consumer + intraday_supply_tracker 단위 테스트 (5/22 20:30)."""
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from bot.jgis_signal_consumer import (
    JgisSignal,
    compute_composite_score,
    classify_composite_signal,
)
from bot.intraday_supply_tracker import (
    IntradayScore,
    _score_5min_trend,
    _vwap_position,
    format_4times_report,
)


class TestJgisSignalConsumer(unittest.TestCase):

    def test_1_composite_strong_buy(self):
        """① sniper 90 + smart 130 + supply 150 + 핵심 시그널 → STRONG_BUY"""
        s = JgisSignal(
            code="403870", name="HPSP",
            sniper_score=90, sniper_signal="반등임박",
            smart_money_score=130, smart_money_signal="DUAL_FLOW",
            supply_score=150,
            rsi=55, adx=30, vol_ratio=1.0,
        )
        score = compute_composite_score(s)
        self.assertGreaterEqual(score, 80)
        self.assertEqual(classify_composite_signal(score), "STRONG_BUY")

    def test_2_composite_moderate(self):
        """② 중간 시그널 → HOLD/BUY"""
        s = JgisSignal(code="A", sniper_score=60, smart_money_score=60, supply_score=80,
                       rsi=55, adx=28)
        score = compute_composite_score(s)
        self.assertGreaterEqual(score, 40)

    def test_3_composite_sell(self):
        """③ 약한 시그널 → SELL"""
        s = JgisSignal(code="A", sniper_score=10, smart_money_score=10, supply_score=10)
        score = compute_composite_score(s)
        self.assertLess(score, 40)

    def test_4_no_data_zero(self):
        """④ 데이터 없음 → 0점"""
        score = compute_composite_score(JgisSignal(code="A"))
        self.assertEqual(score, 0.0)


class TestIntradaySupplyTracker(unittest.TestCase):

    def test_5_5min_trend_bullish(self):
        """⑤ 3연속 양봉 + 거래량 증가 → 65+"""
        bars = [
            {"open": 100, "close": 99, "high": 100, "low": 98, "volume": 1000},
            {"open": 99, "close": 98, "high": 99, "low": 97, "volume": 1000},
            {"open": 98, "close": 97, "high": 98, "low": 96, "volume": 1000},
            {"open": 97, "close": 99, "high": 99, "low": 97, "volume": 1500},
            {"open": 99, "close": 101, "high": 101, "low": 99, "volume": 1800},
            {"open": 101, "close": 103, "high": 103, "low": 101, "volume": 2000},
        ]
        score = _score_5min_trend(bars)
        self.assertGreaterEqual(score, 65)

    def test_6_5min_trend_empty(self):
        """⑥ 빈 캔들 → 50 중립"""
        self.assertEqual(_score_5min_trend([]), 50.0)

    def test_7_vwap_above_below(self):
        """⑦ VWAP 위/아래 판정"""
        bars = [{"open": 100, "high": 101, "low": 99, "close": 100, "volume": 1000}] * 5
        self.assertEqual(_vwap_position(bars, 105), "above")
        self.assertEqual(_vwap_position(bars, 95), "below")

    def test_8_format_report(self):
        """⑧ 4번 알람 보고서 포맷"""
        scores = {
            "A": IntradayScore(code="A", name="A명", composite=85.0, signal="STRONG_BUY",
                              sniper_signal="반등임박", smart_money_signal="DUAL_FLOW"),
            "B": IntradayScore(code="B", name="B명", composite=35.0, signal="WEAK",
                              alert_reason="수급 약화"),
        }
        report = format_4times_report(scores, "09:30")
        self.assertIn("09:30", report)
        self.assertIn("STRONG_BUY", report)
        self.assertIn("핵심 시그널", report)

    def test_9_format_empty(self):
        """⑨ 빈 데이터 → 데이터 없음"""
        self.assertIn("데이터 없음", format_4times_report({}, "09:30"))


if __name__ == "__main__":
    unittest.main(verbosity=2)
