# -*- coding: utf-8 -*-
"""asset_pool_elliott 단위 테스트 (5/22 G 통합 검증)."""
import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from utils.asset_pool_elliott import (
    _cap_band,
    enrich_with_elliott,
    summarize_elliott_distribution,
)


class TestAssetPoolElliott(unittest.TestCase):

    def test_1_cap_band_classification(self):
        """① 시총 분류 — 백테스트 매트릭스와 동일 4단계."""
        self.assertEqual(_cap_band(300), "소형(100-500억)")
        self.assertEqual(_cap_band(800), "중소형(500-1000억)")
        self.assertEqual(_cap_band(3000), "중대형(1000-5000억)")
        self.assertEqual(_cap_band(10000), "대형(5000억+)")

    def test_2_enrich_empty_candidates(self):
        """② 빈 후보 → 빈 리스트 그대로."""
        mock_trader = MagicMock()
        result = enrich_with_elliott([], mock_trader)
        self.assertEqual(result, [])
        mock_trader.fetch_daily_chart.assert_not_called()

    def test_3_enrich_with_failed_bars(self):
        """③ 일봉 부족 — elliott_boost=0 그대로 유지."""
        mock_trader = MagicMock()
        mock_trader.fetch_daily_chart.return_value = []
        candidates = [{"code": "005930", "score": 100}]
        result = enrich_with_elliott(candidates, mock_trader, delay=0, log_each=False)
        self.assertEqual(result[0]["elliott_boost"], 0)
        self.assertEqual(result[0]["score"], 100)  # 변화 X

    def test_4_summarize_distribution(self):
        """④ 분포 요약 — zone별 카운트 + 평균 boost."""
        candidates = [
            {"code": "A", "name": "A명", "elliott_zone": "fib_38_safe",
             "elliott_boost": 30, "score": 130},
            {"code": "B", "elliott_zone": "fib_50_standard",
             "elliott_boost": 18, "score": 118},
            {"code": "C", "elliott_zone": "korean_low",
             "elliott_boost": -10, "score": 90},
        ]
        summary = summarize_elliott_distribution(candidates)
        self.assertEqual(summary["total"], 3)
        self.assertEqual(summary["zone_distribution"]["fib_38_safe"], 1)
        self.assertEqual(summary["avg_boost"], round((30 + 18 - 10) / 3, 1))
        self.assertEqual(summary["max_boost"], 30)
        self.assertEqual(summary["min_boost"], -10)
        self.assertEqual(summary["top_candidate"]["code"], "A")

    def test_5_circular_import_safe(self):
        """⑤ circular import 회피 — bot.elliott_wave는 함수 안에서 지연 import."""
        # utils.asset_pool_elliott 자체 import 시 bot.elliott_wave 미import
        import importlib
        import utils.asset_pool_elliott as ape
        # 모듈 attribute 확인 — bot이 모듈 레벨에 있으면 안 됨
        module_attrs = dir(ape)
        # detect_elliott_pattern은 모듈 레벨 X (함수 내부 import)
        self.assertNotIn("detect_elliott_pattern", module_attrs)
        self.assertIn("enrich_with_elliott", module_attrs)
        self.assertIn("summarize_elliott_distribution", module_attrs)


    def test_6_cross_validate_both(self):
        """⑥ 단타봇 + 정보봇 동시 시그널 → cross_status='both' / +50 보너스"""
        from utils.asset_pool_elliott import merge_and_cross_validate_jgis
        from unittest.mock import patch, MagicMock

        with patch("bot.jgis_signal_consumer.top_strong_buy_signals") as mock_top:
            # 정보봇 STRONG_BUY = HPSP (403870) + 단타봇 candidate에도 있음
            mock_sig = MagicMock()
            mock_sig.code = "403870"
            mock_sig.name = "HPSP"
            mock_sig.sector = "반도체"
            mock_sig.sniper_signal = "반등임박"
            mock_sig.sniper_score = 82
            mock_sig.smart_money_signal = "DUAL_FLOW"
            mock_sig.smart_money_score = 105
            mock_sig.supply_grade = "A+"
            mock_sig.supply_score = 98
            mock_sig.composite_score = 90.0
            mock_top.return_value = [mock_sig]

            dantabot = [{"code": "403870", "name": "HPSP", "score": 100}]
            merged = merge_and_cross_validate_jgis(dantabot, jgis_top_limit=10)

        self.assertEqual(len(merged), 1)
        self.assertEqual(merged[0]["cross_status"], "both")
        self.assertTrue(merged[0]["cross_validated"])
        self.assertEqual(merged[0]["cross_bonus"], 50)
        self.assertEqual(merged[0]["score"], 150)  # 100 + 50

    def test_7_cross_validate_jgis_only(self):
        """⑦ 정보봇 단독 → cross_status='jgis_only' / 새 종목 합류 +20"""
        from utils.asset_pool_elliott import merge_and_cross_validate_jgis
        from unittest.mock import patch, MagicMock

        with patch("bot.jgis_signal_consumer.top_strong_buy_signals") as mock_top:
            mock_sig = MagicMock()
            mock_sig.code = "999999"
            mock_sig.name = "정보봇단독"
            mock_sig.sector = ""
            mock_sig.sniper_signal = "반등임박"
            mock_sig.sniper_score = 80
            mock_sig.smart_money_signal = ""
            mock_sig.smart_money_score = 0
            mock_sig.supply_grade = ""
            mock_sig.supply_score = 0
            mock_sig.composite_score = 75.0
            mock_top.return_value = [mock_sig]

            dantabot = [{"code": "AAA", "score": 50}]
            merged = merge_and_cross_validate_jgis(dantabot, jgis_top_limit=10)

        self.assertEqual(len(merged), 2)
        jgis_only = [c for c in merged if c.get("cross_status") == "jgis_only"]
        self.assertEqual(len(jgis_only), 1)
        self.assertEqual(jgis_only[0]["code"], "999999")
        self.assertEqual(jgis_only[0]["score"], 95)  # composite 75 + 20

    def test_8_cross_block_jgis_only_avoid(self):
        """⑧ 정보봇 단독 + elliott AVOID → cross_block (-100 사실상 차단)"""
        from utils.asset_pool_elliott import apply_elliott_cross_block

        candidates = [
            {"code": "A", "name": "정보봇단독", "score": 95,
             "cross_status": "jgis_only", "elliott_signal": "AVOID"},
            {"code": "B", "name": "더블", "score": 150,
             "cross_status": "both", "elliott_signal": "AVOID"},  # both는 차단 X
        ]
        result = apply_elliott_cross_block(candidates)
        a = next(c for c in result if c["code"] == "A")
        b = next(c for c in result if c["code"] == "B")
        self.assertEqual(a["cross_status"], "jgis_blocked")
        self.assertEqual(a["score"], -5)  # 95 - 100
        self.assertEqual(b["cross_status"], "both")  # both는 그대로
        self.assertEqual(b["score"], 150)


if __name__ == "__main__":
    unittest.main(verbosity=2)
