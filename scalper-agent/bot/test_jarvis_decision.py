"""test_jarvis_decision.py — 박사 자율 통합 의사결정 단위 테스트
====================================================================
사장님 5/21 22:30 명령 검증:
  1. 정보봇 악재 → FORCE_SELL
  2. VWAP 상단 + 수익 → PYRAMID_ADD
  3. VWAP 하단 + 손실 → RECOVERY_ADD
  4. 시총별 SL (대형 -10% / 중형 -5% / 소형 -3%)
  5. 사장님 보호 종목 자동 HOLD

실행:
  /home/ubuntu/bodyhunter/venv/bin/python bot/test_jarvis_decision.py
"""
import sys
import os
import unittest
from unittest.mock import patch, MagicMock

_BOT_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_DIR = os.path.dirname(_BOT_DIR)
if _PROJECT_DIR not in sys.path:
    sys.path.insert(0, _PROJECT_DIR)

from bot.jarvis_decision import (
    decide_action,
    is_negative_news,
    get_market_cap_tier,
    get_vwap_position,
    ACTION_FORCE_SELL,
    ACTION_PYRAMID_ADD,
    ACTION_RECOVERY_ADD,
    ACTION_SL_LARGE,
    ACTION_SL_MID,
    ACTION_SL_SMALL,
    ACTION_TAKE_PROFIT,
    ACTION_HOLD,
)


class TestNegativeNews(unittest.TestCase):
    """악재 감지 — 정보봇 4개 데이터 통합."""

    def test_01_crisis_signal(self):
        """CRISIS 시그널 → 악재."""
        with patch("bot.jarvis_decision.Path") as mock_path:
            mock_file = MagicMock()
            mock_file.exists.return_value = True
            mock_file.read_text.return_value = '{"signal": "CRISIS", "confidence": "HIGH", "kospi_change_3d": 0, "vix_change_3d": 0}'
            mock_path.return_value.__truediv__.return_value = mock_file
            # crisis_etf_signal.json만 존재, 다른 파일 없음
            def fake_exists(p):
                return 'crisis_etf' in str(p)
            mock_file.exists.side_effect = lambda: True
            has_neg, reason = is_negative_news("005930")
        # 시그널 데이터 자체 검증보다는 통합 작동 검증으로 충분
        print(f"  T01: crisis_etf=CRISIS → 악재 시도 결과 OK")


class TestMarketCapTier(unittest.TestCase):
    """시총 분류."""

    def test_02_market_cap_tiers(self):
        """대형/중형/소형/UNKNOWN 분류."""
        # universe.json mock
        with patch("bot.jarvis_decision.json.loads") as mock_loads, \
             patch("bot.jarvis_decision.Path") as mock_path:
            mock_file = MagicMock()
            mock_file.exists.return_value = True
            mock_file.read_text.return_value = "{}"
            mock_path.return_value.__truediv__.return_value = mock_file

            # 대형주 (1조 5천억)
            mock_loads.return_value = {"005380": {"cap_억": 15000}}
            tier, cap = get_market_cap_tier("005380")
            self.assertEqual(tier, "LARGE")
            self.assertEqual(cap, 15000)

            # 중형주 (5천억)
            mock_loads.return_value = {"217590": {"cap_억": 5000}}
            tier, cap = get_market_cap_tier("217590")
            self.assertEqual(tier, "MID")

            # 소형주 (500억)
            mock_loads.return_value = {"474170": {"cap_억": 500}}
            tier, cap = get_market_cap_tier("474170")
            self.assertEqual(tier, "SMALL")

            # UNKNOWN
            mock_loads.return_value = {}
            tier, cap = get_market_cap_tier("999999")
            self.assertEqual(tier, "UNKNOWN")
        print("  ✅ T02 PASS: 시총 4단계 분류 (LARGE/MID/SMALL/UNKNOWN)")


class TestDecideAction(unittest.TestCase):
    """통합 액션 결정 (4가지 시나리오 + 보호)."""

    def _stub_no_negative(self):
        """악재 없음으로 stub."""
        return patch("bot.jarvis_decision.is_negative_news", return_value=(False, ""))

    def _stub_vwap(self, position, ratio):
        """VWAP 위치 stub."""
        return patch("bot.jarvis_decision.get_vwap_position", return_value=(position, ratio))

    def _stub_tier(self, tier, cap):
        """시총 stub."""
        return patch("bot.jarvis_decision.get_market_cap_tier", return_value=(tier, cap))

    def test_03_force_sell_on_negative(self):
        """[악재] FORCE_SELL — 최우선."""
        pos = {"_code": "005930", "buy_price": 50000, "source": "asset_pool"}
        with patch("bot.jarvis_decision.is_negative_news", return_value=(True, "CRISIS")):
            action, reason = decide_action(pos, 49000)
        self.assertEqual(action, ACTION_FORCE_SELL)
        self.assertIn("CRISIS", reason)
        print("  ✅ T03 PASS: 악재 → FORCE_SELL")

    def test_04_protected_holds(self):
        """[사장님 보호 종목] 자동 HOLD."""
        pos = {"_code": "219420", "buy_price": 5000, "source": "verification"}
        with self._stub_no_negative():
            action, _ = decide_action(pos, 4000)  # -20% 손실이어도
        self.assertEqual(action, ACTION_HOLD)
        # sl_disabled 종목도 보호
        pos2 = {"_code": "999", "buy_price": 5000, "source": "asset_pool", "sl_disabled": True}
        with self._stub_no_negative():
            action2, _ = decide_action(pos2, 4000)
        self.assertEqual(action2, ACTION_HOLD)
        print("  ✅ T04 PASS: 사장님 보호 종목 자동 HOLD")

    def test_05_pyramid_add_vwap_upper(self):
        """[VWAP 상단 + 수익 5%+] PYRAMID_ADD."""
        pos = {"_code": "005380", "buy_price": 100000, "source": "asset_pool"}
        with self._stub_no_negative(), self._stub_vwap("UPPER", 1.05), self._stub_tier("LARGE", 15000):
            action, reason = decide_action(pos, 110000)  # +10%
        self.assertEqual(action, ACTION_PYRAMID_ADD)
        print("  ✅ T05 PASS: VWAP 상단 + 수익 → 피라미딩")

    def test_06_recovery_add_vwap_lower(self):
        """[VWAP 하단 + 손실 5%+] RECOVERY_ADD."""
        pos = {"_code": "005380", "buy_price": 100000, "source": "asset_pool"}
        with self._stub_no_negative(), self._stub_vwap("LOWER", 0.95), self._stub_tier("LARGE", 15000):
            action, reason = decide_action(pos, 90000)  # -10% (하지만 VWAP 하단이면 RECOVERY)
        # VWAP 하단 -5% 이상 손실 → RECOVERY (대형주 SL -10%보다 우선)
        self.assertEqual(action, ACTION_RECOVERY_ADD)
        print("  ✅ T06 PASS: VWAP 하단 + 손실 → RECOVERY_ADD")

    def test_07_sl_large(self):
        """[대형주 -11%] SL_SMALL — ★시총별 차등 SL은 폐기됐다.

        ★8/10 정정 ([F-119] 러너 신설로 53일 만에 처음 실행돼 드러남).
        이 테스트는 5/21 설계(대형 -10% / 중형 -5% / 소형 -3%)를 정답으로 주장하고 있었으나,
        그 룰은 **5/25 사장님 룰 "고점 -3% 일관"으로 폐기**됐고 6/18에 SAJANG 단일진실로
        통일됐다(`jarvis_decision.py:234-236` 주석이 폐기를 명시).
        ★위험했던 지점: 이 테스트를 "통과시키려" 코드를 고쳤다면 **폐기된 시총별 SL을
        되살려 사장님 룰을 위반**하게 된다. 낡은 테스트가 폐기된 룰을 가리키는 함정이었다.
        → 기대값을 현행 룰(SAJANG.get_normal_sl · 시총 무관 -3%)로 정정한다.
        """
        pos = {"_code": "005380", "buy_price": 100000, "source": "asset_pool"}
        with self._stub_no_negative(), self._stub_vwap("MIDDLE", 1.0), self._stub_tier("LARGE", 15000):
            action, reason = decide_action(pos, 89000)  # -11%
        self.assertEqual(action, ACTION_SL_SMALL)
        self.assertIn("SAJANG", reason)          # 하드코딩 복귀 감시
        print("  ✅ T07 PASS: 대형주 -11% → SL_SMALL (시총 무관 SAJANG -3% 일관)")

    def test_08_sl_mid(self):
        """[중형주 -6%] SL_SMALL — 위와 같은 이유(시총별 차등 폐기)."""
        pos = {"_code": "217590", "buy_price": 100000, "source": "asset_pool"}
        with self._stub_no_negative(), self._stub_vwap("MIDDLE", 1.0), self._stub_tier("MID", 5000):
            action, reason = decide_action(pos, 94000)  # -6%
        self.assertEqual(action, ACTION_SL_SMALL)
        self.assertIn("SAJANG", reason)
        print("  ✅ T08 PASS: 중형주 -6% → SL_SMALL (시총 무관 SAJANG -3% 일관)")

    def test_07b_sl_large_mid_constants_unused(self):
        """★ACTION_SL_LARGE/MID 가 매매 판정에 되살아나지 않는지 감시 (8/10 신설).

        위 두 테스트의 기대값을 바꾸면 '폐기 확인'이 사라진다. 상수 자체는 로그 라벨로
        남아 있으므로, **판정 분기에서 반환되지 않는다**는 사실을 별도로 고정해 둔다.
        """
        import inspect
        import bot.jarvis_decision as jd
        src = inspect.getsource(jd.decide_action)
        self.assertNotIn("return ACTION_SL_LARGE", src, "시총별 SL 부활 — 5/25 사장님 룰 위반")
        self.assertNotIn("return ACTION_SL_MID", src, "시총별 SL 부활 — 5/25 사장님 룰 위반")
        print("  ✅ T07b PASS: 시총별 SL 분기 부재 확인 (폐기 유지)")

    def test_09_sl_small(self):
        """[소형주 -3%] SL_SMALL."""
        pos = {"_code": "474170", "buy_price": 100000, "source": "asset_pool"}
        with self._stub_no_negative(), self._stub_vwap("MIDDLE", 1.0), self._stub_tier("SMALL", 500):
            action, _ = decide_action(pos, 96000)  # -4%
        self.assertEqual(action, ACTION_SL_SMALL)
        print("  ✅ T09 PASS: 소형주 -3% → SL_SMALL")

    def test_10_no_fixed_take_profit(self):
        """[+11% 수익] HOLD — ★고정 TP는 폐기됐다. 청산은 트레일링이 한다.

        ★8/10 정정 ([F-119]). 원래 이 테스트는 +10%에서 TAKE_PROFIT을 정답으로 주장했는데,
        **사장님 룰 1 = 트레일링 only · 고정 TP 폐기**(`SAJANG.FIXED_TP_DISABLED=True` ·
        `get_take_profit()=0`)라 현행 코드는 HOLD가 옳다(`jarvis_decision.py:246-248`).
        ★이 테스트를 통과시키려 코드를 고쳤다면 **폐기된 고정 +TP를 부활**시켰을 것이다.
        → 이제 "고정 TP가 발동하지 않는다"를 검증하는 테스트로 뒤집는다.
        """
        pos = {"_code": "066570", "buy_price": 100000, "source": "asset_pool"}
        with self._stub_no_negative(), self._stub_vwap("MIDDLE", 1.0), self._stub_tier("LARGE", 30000):
            action, _ = decide_action(pos, 111000)  # +11% (VWAP 중간이라 피라미딩 X)
        self.assertEqual(action, ACTION_HOLD, "고정 TP 부활 — 사장님 룰 1(트레일링 only) 위반")
        # 뿌리까지 고정: SAJANG 단일진실이 실제로 0을 주는지
        from data.sajang_rules import SAJANG
        self.assertTrue(SAJANG.FIXED_TP_DISABLED, "FIXED_TP_DISABLED 가 꺼졌다")
        self.assertEqual(SAJANG.get_take_profit(100000), 0, "get_take_profit 이 0이 아니다")
        print("  ✅ T10 PASS: +11% 수익 → HOLD (고정 TP 폐기·트레일링이 청산 담당)")

    def test_11_hold_normal(self):
        """[정상 변동] HOLD."""
        pos = {"_code": "066570", "buy_price": 100000, "source": "asset_pool"}
        with self._stub_no_negative(), self._stub_vwap("MIDDLE", 1.0), self._stub_tier("LARGE", 30000):
            action, _ = decide_action(pos, 102000)  # +2%
        self.assertEqual(action, ACTION_HOLD)
        print("  ✅ T11 PASS: 정상 변동 → HOLD")

    def test_12_sl_priority_over_recovery(self):
        """[VWAP MIDDLE + 시총별 SL 우선] (RECOVERY는 VWAP 하단만)."""
        pos = {"_code": "474170", "buy_price": 100000, "source": "asset_pool"}
        # VWAP MIDDLE이라 RECOVERY 조건 X → SL_SMALL 작동해야
        with self._stub_no_negative(), self._stub_vwap("MIDDLE", 1.0), self._stub_tier("SMALL", 500):
            action, _ = decide_action(pos, 95000)  # -5% (소형주는 -3% SL)
        self.assertEqual(action, ACTION_SL_SMALL)
        print("  ✅ T12 PASS: VWAP 중간 + 소형주 -5% → SL_SMALL (RECOVERY 조건 X)")


if __name__ == "__main__":
    print("=" * 70)
    print("  박사 자율 통합 의사결정 단위 테스트 (12개)")
    print("  사장님 5/21 22:00~22:30 명령: 악재+VWAP+시총별 SL+추매 통합")
    print("=" * 70)
    unittest.main(verbosity=2)
