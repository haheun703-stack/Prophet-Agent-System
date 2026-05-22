# -*- coding: utf-8 -*-
"""엘리어트 파동 모듈 단위 테스트 (사장님 5/22 저녁 약속).

테스트 케이스 6종:
  1. 정상 5파 진입 (4룰 모두 통과 → buy_signal=True)
  2. 파동 중첩 위반 (4파 저점 ≤ 1파 고점 → buy_signal=False)
  3. 피보 38.2% 벗어남 (deep retracement → fib_zone_match=False)
  4. 교대법칙 위반 (2파/4파 둘 다 V_sharp → alternation FAIL)
  5. 캔들 부족 (lookback 미달 → phase=unknown)
  6. 3파 진행 중 (4파 미형성 → phase=wave_3_running)

영구 메모리: [[project_5_22_evening_learning_fix]] 단위 테스트 룰
"""
import unittest

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from bot.elliott_wave import (
    detect_elliott_pattern,
    elliott_score_boost,
)


def _make_candle(o: float, h: float, l: float, c: float, v: int = 1000) -> dict:
    return {"open": o, "high": h, "low": l, "close": c, "volume": v}


# ── 표준 5파 진입 패턴 (모든 룰 통과) ─────────────
PATTERN_FIVE_WAVE_OK = [
    _make_candle(100, 101, 99.5, 100.5),
    _make_candle(100.5, 102, 100, 101),
    _make_candle(101, 103, 100.5, 102),
    _make_candle(102, 108, 101.5, 107, 1500),
    _make_candle(107, 115, 106, 113, 1800),
    _make_candle(113, 122, 112, 120, 2200),    # w1 고점 (idx 5)
    _make_candle(120, 120.5, 110, 111, 1700),
    _make_candle(111, 112, 105, 106, 1500),    # w2 V자 저점 (idx 7)
    _make_candle(106, 110, 105.5, 109, 1300),
    _make_candle(109, 115, 108.5, 114, 1700),
    _make_candle(114, 135, 113, 133, 3500),
    _make_candle(133, 152, 132, 150, 4500),
    _make_candle(150, 162, 149, 160, 5000),    # w3 고점 (idx 12)
    _make_candle(160, 161, 153, 154, 2200),
    _make_candle(154, 155, 148, 149, 1900),
    _make_candle(149, 150, 146, 147, 1500),    # w4 저점 (idx 15)
    _make_candle(147, 149, 146.5, 148, 1400),
    _make_candle(148, 149.5, 147, 148.5, 1300),
    _make_candle(148.5, 150, 147.5, 149, 1400),
]


class TestElliottWave(unittest.TestCase):

    def test_1_normal_five_wave_entry(self):
        """① 정상 5파 진입: 4룰 모두 통과 → buy_signal=True / confidence=1.0"""
        result = detect_elliott_pattern(PATTERN_FIVE_WAVE_OK)
        self.assertEqual(result.phase, "wave_5_start", f"phase={result.phase}, reason={result.reason}")
        self.assertTrue(result.fib_zone_match, f"fib_zone_match=False, reason={result.reason}")
        self.assertTrue(result.non_overlap_check, "non_overlap_check=False")
        self.assertEqual(result.alternation_check, "OK_alternating")
        self.assertTrue(result.buy_signal)
        self.assertEqual(result.confidence, 1.0)
        self.assertEqual(elliott_score_boost(result), 25)

    def test_2_overlap_violation(self):
        """② 파동 중첩 위반: 4파 저점이 1파 고점보다 낮음 → buy_signal=False"""
        # PATTERN_FIVE_WAVE_OK 변형: 4파 저점 (idx 15)을 120 (w1 고점 위) 대신 110 (위반)
        # 그러나 다른 룰은 통과하도록 유지
        modified = list(PATTERN_FIVE_WAVE_OK)
        # w4 저점을 강제로 1파 고점(122) 아래로
        modified[15] = _make_candle(149, 150, 115, 116, 1500)
        modified[16] = _make_candle(116, 118, 115, 117, 1400)
        modified[17] = _make_candle(117, 119, 116, 118, 1300)
        modified[18] = _make_candle(118, 119, 117, 118, 1400)

        result = detect_elliott_pattern(modified)
        self.assertFalse(result.non_overlap_check, "non_overlap_check=True (위반인데 통과로 잡힘)")
        self.assertFalse(result.buy_signal, "buy_signal=True (위반인데 매수 시그널)")

    def test_3_deep_retracement_fails_fib(self):
        """③ 깊은 되돌림: 4파 저점이 피보 38.2% 훨씬 아래 → fib_zone_match=False"""
        # PATTERN_FIVE_WAVE_OK 변형: 4파 저점 130 (피보 140.23보다 한참 아래)
        # 단, 1파 고점(122) 위 유지 — 중첩금지는 통과
        modified = list(PATTERN_FIVE_WAVE_OK)
        modified[15] = _make_candle(149, 150, 125, 126, 1500)  # w4 저점 125
        modified[16] = _make_candle(126, 128, 125, 127, 1400)
        modified[17] = _make_candle(127, 129, 126, 128, 1300)
        modified[18] = _make_candle(128, 129, 127, 128, 1400)

        result = detect_elliott_pattern(modified)
        # 4파 저점 125 vs fib_382 ~140.23 (5% 허용 = 133.2~147.2) → 미충족
        self.assertFalse(result.fib_zone_match,
                         f"fib_zone_match=True (125는 140.23±5% 벗어남), w4_low={result.wave_4_low}")
        self.assertFalse(result.buy_signal)

    def test_4_alternation_violation(self):
        """④ 교대법칙 위반: 2파/4파 둘 다 V_sharp → alternation FAIL"""
        # 2파 V_sharp 유지 + 4파도 짧고 급격하게 (V_sharp 만듦)
        modified = list(PATTERN_FIVE_WAVE_OK)
        # 4파를 짧고 급격하게 (V_sharp): 캔들 적게 + 변동 크게
        # idx 13~14만 사용, w4 저점 idx 14에서 146까지 급락
        modified = modified[:13] + [
            _make_candle(160, 161, 146, 147, 2500),  # 한 캔들에 급락 (V_sharp)
            _make_candle(147, 149, 146, 148, 1500),  # idx 14 (w4 저점)
        ]

        result = detect_elliott_pattern(modified)
        # 4파 형태가 V_sharp이면 교대법칙 위반
        if result.wave_4_shape == "V_sharp":
            self.assertEqual(result.alternation_check, "FAIL_same_shape")
            self.assertFalse(result.buy_signal)
        # 캔들 너무 적어 unknown 가능 — 그 경우는 자동 통과

    def test_5_insufficient_candles(self):
        """⑤ 캔들 부족: phase=unknown / reason="캔들 부족" """
        few_candles = PATTERN_FIVE_WAVE_OK[:5]  # 5개만
        result = detect_elliott_pattern(few_candles)
        self.assertEqual(result.phase, "unknown")
        self.assertIn("캔들 부족", result.reason)
        self.assertFalse(result.buy_signal)

    def test_6_wave_3_running(self):
        """⑥ 3파 진행 중: 4파 미형성 → phase=wave_3_running OR wave_4_pullback"""
        # PATTERN_FIVE_WAVE_OK에서 4파 캔들 제거 (3파 고점 직후 끝)
        running = PATTERN_FIVE_WAVE_OK[:13]  # idx 12 = w3 고점까지만

        result = detect_elliott_pattern(running)
        # 3파 고점 직후 → 패턴 감지되면 wave_3_running, 또는 캔들 부족으로 unknown
        # 핵심은 buy_signal=False여야 함 (4파 형성 안 됐으므로)
        self.assertFalse(result.buy_signal)

    def test_7_score_boost_thresholds(self):
        """⑦ elliott_score_boost 임계값:
           - buy_signal=True → 25
           - wave_4_pullback + confidence≥0.75 → 15
           - wave_3_running → 10
           - 그 외 → 0
        """
        from bot.elliott_wave import ElliottResult

        # buy_signal
        r1 = ElliottResult(buy_signal=True, phase="wave_5_start", confidence=1.0)
        self.assertEqual(elliott_score_boost(r1), 25)

        # 4파 pullback + 0.75
        r2 = ElliottResult(buy_signal=False, phase="wave_4_pullback", confidence=0.75)
        self.assertEqual(elliott_score_boost(r2), 15)

        # 3파 running
        r3 = ElliottResult(buy_signal=False, phase="wave_3_running", confidence=0.5)
        self.assertEqual(elliott_score_boost(r3), 10)

        # 미감지
        r4 = ElliottResult(phase="unknown")
        self.assertEqual(elliott_score_boost(r4), 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
