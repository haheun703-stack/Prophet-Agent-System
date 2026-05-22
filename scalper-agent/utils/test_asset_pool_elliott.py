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


if __name__ == "__main__":
    unittest.main(verbosity=2)
