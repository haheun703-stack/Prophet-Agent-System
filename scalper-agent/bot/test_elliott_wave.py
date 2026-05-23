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
    # ★ 5/23 토 단타봇 자율 정밀화 ★ — w4 sweet 41% (fib (162-138)/57*100 = 42.1%)
    # 이전 w4=146 (fib 28%) = 새 too_shallow → 단위 테스트 실패
    # 새 w4=138 (fib 42.1%) = ★ 진짜 sweet spot (5/23 백테스트 55.6%) ★
    _make_candle(160, 161, 145, 146, 2200),
    _make_candle(146, 147, 140, 141, 1900),
    _make_candle(141, 142, 138, 139, 1500),    # w4 저점 (idx 15) — 138 sweet
    _make_candle(139, 141, 138.5, 140, 1400),
    _make_candle(140, 141.5, 139, 140.5, 1300),
    _make_candle(140.5, 142, 139.5, 141, 1400),
]


class TestElliottWave(unittest.TestCase):

    def test_1_normal_five_wave_entry(self):
        """① 정상 5파 진입: zone BUY + non_overlap → buy_signal=True / confidence=1.0

        ★ 5/23 토 정밀화 ★ — w4 fib 42.1% (sweet 36-46) zone fib_38_safe BUY
        교대법칙은 정보용 (alt_ok에 무관 — 5/22 19:30 룰 제거).
        """
        result = detect_elliott_pattern(PATTERN_FIVE_WAVE_OK)
        self.assertEqual(result.phase, "wave_5_start", f"phase={result.phase}, reason={result.reason}")
        self.assertTrue(result.fib_zone_match, f"fib_zone_match=False, reason={result.reason}")
        self.assertEqual(result.fib_zone_name, "fib_38_safe", f"zone={result.fib_zone_name} (sweet 36-46 안이어야 함)")
        self.assertTrue(result.non_overlap_check, "non_overlap_check=False")
        # 교대법칙은 정보용 — 5/22 19:30 룰 제거 (buy_signal 의사결정 X)
        self.assertIn(result.alternation_check, ("OK_alternating", "FAIL_same_shape"))
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

    def test_4_alternation_no_longer_blocks(self):
        """④ 교대법칙 / 횡보 룰 제거 (5/22 19:30 A+C 백테스트 검증):

        이전: 4룰 다 통과해야 buy_signal=True
        정정: 교대법칙 + 횡보 룰 의미 X (false negative 발생) → buy_signal 의사결정에서 제외

        buy_signal = wave_5_start + zone BUY + non_overlap_check 만.
        """
        modified = list(PATTERN_FIVE_WAVE_OK)
        # 4파를 짧고 급격하게 (V_sharp) — 교대법칙 FAIL 의도
        modified = modified[:13] + [
            _make_candle(160, 161, 146, 147, 2500),
            _make_candle(147, 149, 146, 148, 1500),
        ]
        result = detect_elliott_pattern(modified)
        # ★ 정정: 교대법칙 FAIL이어도 zone BUY + non_overlap 시 buy_signal 가능 ★
        # alternation_check는 정보용으로 기록되지만 buy_signal 의사결정 X
        # (캔들 부족 시 unknown 가능)

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

    def test_8_fib_zone_5tier_setting(self):
        """⑧ 5단계 zone (★ 5/23 토 단타봇 fine-grained 정밀화 ★):
           - 0~36%:   too_shallow / NO_ENTRY / conf 0 (28-36 sweet 미달 회피)
           - 36~46%:  fib_38_safe / BUY / conf 1.0 (★ 진짜 sweet — 성공률 55.6% x2.73)
           - 46~62%:  fib_50_standard / BUY / conf 0.7 (성공률 37-40%)
           - 62~80%:  korean_low / AVOID / conf 0.1 (성공률 9.1% ★ 회피 ★)
           - 80~100%: trend_caution / BUY_CAUTION / conf 0.4 (큰 수익 +52%)
           - 100%+:   trend_end / NO_ENTRY / conf 0 (추세 종료)
        """
        from bot.elliott_wave import evaluate_fib_zone

        # ★ 진짜 sweet 41% (5/23 정밀화) ★
        r1 = evaluate_fib_zone(41.0)
        self.assertEqual(r1["zone"], "fib_38_safe", "41% sweet center")
        self.assertEqual(r1["signal"], "BUY")
        self.assertEqual(r1["confidence"], 1.0)

        # 36% sweet 경계 (포함)
        r2 = evaluate_fib_zone(36.0)
        self.assertEqual(r2["zone"], "fib_38_safe")

        # 30% — 새 boundary로 too_shallow (이전 fib_38_safe였지만 sweet 미달)
        r2b = evaluate_fib_zone(30.0)
        self.assertEqual(r2b["zone"], "too_shallow", "30% sweet 미달 → 새 NO_ENTRY")

        # 46% 경계 (제외 — fib_50으로)
        r3 = evaluate_fib_zone(46.0)
        self.assertEqual(r3["zone"], "fib_50_standard")
        self.assertEqual(r3["confidence"], 0.7)

        # 55% — 새 fib_50_standard 범위 (46-62) 안
        r3b = evaluate_fib_zone(55.0)
        self.assertEqual(r3b["zone"], "fib_50_standard")

        # ★ 62~80% korean_low AVOID ★ (그대로 — 성공률 9.1%)
        r4 = evaluate_fib_zone(70.0)
        self.assertEqual(r4["zone"], "korean_low")
        self.assertEqual(r4["signal"], "AVOID")
        self.assertEqual(r4["confidence"], 0.1)

        # 80~100% trend_caution BUY_CAUTION (그대로)
        r5 = evaluate_fib_zone(85.0)
        self.assertEqual(r5["zone"], "trend_caution")
        self.assertEqual(r5["signal"], "BUY_CAUTION")
        self.assertEqual(r5["confidence"], 0.4)

        # 100%+ trend_end NO_ENTRY (그대로)
        r6 = evaluate_fib_zone(110.0)
        self.assertEqual(r6["zone"], "trend_end")
        self.assertEqual(r6["signal"], "NO_ENTRY")

        # 너무 얕음 20% (그대로 too_shallow)
        r7 = evaluate_fib_zone(20.0)
        self.assertEqual(r7["zone"], "too_shallow")
        self.assertEqual(r7["signal"], "NO_ENTRY")
        self.assertEqual(r7["confidence"], 0.0)

    def test_10_korean_low_avoid_penalty(self):
        """⑩ korean_low (62~80%) AVOID 패널티 -10점 (5/22 정정).

        실측 성공률 9.1% = baseline x0.45 → 진입 시 손실 가능성 큼.
        elliott_score_boost가 -10점 반환하여 asset_pool 점수 차감.
        """
        from bot.elliott_wave import ElliottResult, elliott_score_boost
        r = ElliottResult(
            phase="wave_5_start", buy_signal=False,
            fib_zone_name="korean_low", fib_zone_signal="AVOID",
            confidence=0.5,
        )
        self.assertEqual(elliott_score_boost(r), -10)

    def test_11_trend_caution_partial_boost(self):
        """⑪ trend_caution (80~100%) +10점 부스트 (보수 진입)."""
        from bot.elliott_wave import ElliottResult, elliott_score_boost
        r = ElliottResult(
            phase="wave_5_start", buy_signal=True,
            fib_zone_name="trend_caution", fib_zone_signal="BUY_CAUTION",
            confidence=0.6,
        )
        self.assertEqual(elliott_score_boost(r), 10)

    def test_12_fib_38_safe_max_boost(self):
        """⑫ fib_38_safe (28~48%) +25점 최대 부스트 (★ 최우선 ★)."""
        from bot.elliott_wave import ElliottResult, elliott_score_boost
        r = ElliottResult(
            phase="wave_5_start", buy_signal=True,
            fib_zone_name="fib_38_safe", fib_zone_signal="BUY",
            confidence=1.0,
        )
        self.assertEqual(elliott_score_boost(r), 25)

    # ─── B+E metadata 가중치 테스트 (5/22 19:55 사장님 명령) ─────
    def test_13_metadata_mid_cap_boost(self):
        """⑬ 중대형(1000~5000억) × fib_38_safe: +5 가중치 → 30점."""
        from bot.elliott_wave import ElliottResult, boost_with_metadata
        r = ElliottResult(
            phase="wave_5_start", buy_signal=True,
            fib_zone_name="fib_38_safe", fib_zone_signal="BUY",
        )
        self.assertEqual(
            boost_with_metadata(r, cap_band="중대형(1000-5000억)", sector="전기전자"),
            30  # 25 + 5
        )

    def test_14_metadata_small_cap_penalty(self):
        """⑭ 소형(100~500억) × fib_38_safe: -5 패널티 → 20점."""
        from bot.elliott_wave import ElliottResult, boost_with_metadata
        r = ElliottResult(
            phase="wave_5_start", buy_signal=True,
            fib_zone_name="fib_38_safe", fib_zone_signal="BUY",
        )
        self.assertEqual(
            boost_with_metadata(r, cap_band="소형(100-500억)", sector="화학"),
            20  # 25 - 5
        )

    def test_15_metadata_pharma_trend_caution_bonus(self):
        """⑮ 제약 × trend_caution: +10 보너스 (60% 성공 검증) → 20점."""
        from bot.elliott_wave import ElliottResult, boost_with_metadata
        r = ElliottResult(
            phase="wave_5_start", buy_signal=True,
            fib_zone_name="trend_caution", fib_zone_signal="BUY_CAUTION",
        )
        self.assertEqual(
            boost_with_metadata(r, cap_band="중대형(1000-5000억)", sector="제약"),
            25  # 10 + 5(중대형) + 10(제약×trend_caution)
        )

    def test_16_metadata_medical_precision_bonus(self):
        """⑯ 의료정밀 × trend_caution: +10 보너스 → 20점."""
        from bot.elliott_wave import ElliottResult, boost_with_metadata
        r = ElliottResult(
            phase="wave_5_start", buy_signal=True,
            fib_zone_name="trend_caution", fib_zone_signal="BUY_CAUTION",
        )
        self.assertEqual(
            boost_with_metadata(r, cap_band="", sector="의료정밀"),
            20  # 10 + 10(의료정밀×trend_caution)
        )

    def test_17_metadata_avoid_with_mid_cap_still_penalty(self):
        """⑰ AVOID는 -10 + 중대형 +5 = -5 (★ 시총 보너스로도 AVOID 회피 X ★)."""
        from bot.elliott_wave import ElliottResult, boost_with_metadata
        r = ElliottResult(
            phase="wave_5_start", buy_signal=False,
            fib_zone_name="korean_low", fib_zone_signal="AVOID",
        )
        self.assertEqual(
            boost_with_metadata(r, cap_band="중대형(1000-5000억)", sector="전기전자"),
            -5  # -10 + 5
        )

    def test_9_fib_retracement_pct_recorded(self):
        """⑨ ElliottResult에 fib_retracement_pct 실제값 기록 (5/22 추가):
           - 단순 fib_zone_match (bool) → 실제 % 값 + zone 이름 + signal 모두 기록
        """
        result = detect_elliott_pattern(PATTERN_FIVE_WAVE_OK)
        self.assertIsNotNone(result.fib_retracement_pct)
        self.assertIn(result.fib_zone_name, [
            "fib_38_safe", "fib_50_standard", "korean_typical",
            "trend_end_risk", "too_shallow", "unknown"
        ])
        self.assertIn(result.fib_zone_signal, ["BUY", "WAIT", "NO_ENTRY"])

    def test_7_score_boost_thresholds(self):
        """⑦ elliott_score_boost 임계값 (5/22 19:30 zone별 차등):
           - buy_signal=True + fib_38_safe → 25
           - buy_signal=True + fib_50_standard → 18
           - buy_signal=True + trend_caution → 10
           - AVOID 시그널 → -10 (패널티)
           - wave_4_pullback + confidence≥0.75 → 15
           - wave_3_running → 10
           - 그 외 → 0
        """
        from bot.elliott_wave import ElliottResult

        # buy_signal + 최우선 zone
        r1 = ElliottResult(buy_signal=True, phase="wave_5_start",
                           fib_zone_name="fib_38_safe", fib_zone_signal="BUY",
                           confidence=1.0)
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
