# -*- coding: utf-8 -*-
"""동적 트레일링 단위 테스트 (사장님 5/22 09:50 명령)."""
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from bot.dynamic_trailing import (
    classify_trend_strength,
    decide_trailing,
    compute_trailing_sl,
    should_activate_trailing,
)


class TestDynamicTrailing(unittest.TestCase):

    def test_weak_strength_under_5pct(self):
        """+0~5% → weak / trail -3% / activation +3%"""
        d = decide_trailing(pnl_pct=2.0)
        self.assertEqual(d.strength, "weak")
        self.assertEqual(d.trail_pct, 3.0)
        self.assertEqual(d.activation_pct, 3.0)
        self.assertTrue(d.disable_fixed_tp)

    def test_moderate_strength_5_to_10pct(self):
        """+5~10% → moderate / trail -5% / activation +5%"""
        d = decide_trailing(pnl_pct=7.0)
        self.assertEqual(d.strength, "moderate")
        self.assertEqual(d.trail_pct, 5.0)

    def test_strong_strength_10pct_plus(self):
        """+10%+ → strong / trail -7%"""
        d = decide_trailing(pnl_pct=12.0)
        self.assertEqual(d.strength, "strong")
        self.assertEqual(d.trail_pct, 7.0)

    def test_limit_up_imminent_25pct_plus(self):
        """+25%+ → limit_up_imminent / trail -10%"""
        d = decide_trailing(pnl_pct=27.0)
        self.assertEqual(d.strength, "limit_up_imminent")
        self.assertEqual(d.trail_pct, 10.0)

    def test_moderate_upgrade_to_strong_with_signals(self):
        """+5~10% + 3연속 양봉 + 체결강도 100+ + VWAP 위 → strong 승격"""
        bars = [
            {"open": 100, "high": 103, "low": 99, "close": 102, "volume": 1000},
            {"open": 102, "high": 105, "low": 101, "close": 104, "volume": 1500},
            {"open": 104, "high": 107, "low": 103, "close": 106, "volume": 2000},
        ]
        d = decide_trailing(
            pnl_pct=6.0,
            five_min_bars=bars,
            flow_intensity=110.0,
            vwap_position="above",
        )
        self.assertEqual(d.strength, "strong", "추세 강화 시그널 3종 통과 시 moderate → strong")

    def test_compute_trailing_sl(self):
        """고점 1,418 / -5% 트레일링 → SL 1,347"""
        d = decide_trailing(pnl_pct=8.83)
        sl = compute_trailing_sl(1418, d)
        self.assertEqual(sl, 1347, "trail -5% × 1418 = 1347 (정수)")

    def test_should_activate_below_activation(self):
        """pnl 2% < activation 3% → 활성 X"""
        d = decide_trailing(pnl_pct=2.0)
        self.assertFalse(should_activate_trailing(2.0, d))

    def test_should_activate_above_activation(self):
        """pnl 5% ≥ activation 5% → 활성 O"""
        d = decide_trailing(pnl_pct=5.0)
        self.assertTrue(should_activate_trailing(5.0, d))

    def test_fixed_tp_always_disabled(self):
        """모든 케이스에서 disable_fixed_tp=True (사장님 영구 룰)"""
        for pnl in [1.0, 5.0, 10.0, 25.0]:
            d = decide_trailing(pnl_pct=pnl)
            self.assertTrue(d.disable_fixed_tp, f"pnl={pnl}% 케이스에서 고정 TP 비활성 X")

    def test_classify_trend_directly(self):
        """classify_trend_strength 함수 단독 호출"""
        self.assertEqual(classify_trend_strength(0.5), "weak")
        self.assertEqual(classify_trend_strength(5.0), "moderate")
        self.assertEqual(classify_trend_strength(10.0), "strong")
        self.assertEqual(classify_trend_strength(25.0), "limit_up_imminent")


if __name__ == "__main__":
    unittest.main(verbosity=2)
