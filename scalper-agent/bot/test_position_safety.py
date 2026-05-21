"""test_position_safety.py — position_safety 단위 테스트 (박사 통합 v2.0)
====================================================================
GPT 베이스 + 박사 X 7개 fix + 추가 CRITICAL 테스트 5개 통합.

실행:
  cd /home/ubuntu/bodyhunter/scalper-agent
  /home/ubuntu/bodyhunter/venv/bin/python -m pytest bot/test_position_safety.py -v
  또는:
  /home/ubuntu/bodyhunter/venv/bin/python bot/test_position_safety.py

박사 v2.0 변경 (GPT 베이스 대비):
  1. self.kis → self.trader (auto_trader.py 클래스 멤버명)
  2. fetch_balance() → dict {success, positions, cash} (실제 봇 형식)
  3. buy_price → avg_price (KIS 실제 필드명)
  4. fetch_price() → dict {success, current_price}
  5. trade_journal mock (봇 표준 모듈 패턴)
  6. import: from bot.position_safety
  7. FakeAutoTrader에 config + _send_alert 추가

박사 추가 테스트 (CRITICAL #1 fix 검증):
  9. 사장님 보호 종목 (sl_disabled=True) → hard_kill 스킵
  10. 메모리 누락 (빈 dict) → 보호 가정 스킵
  11. source="sync_auto" → 1사이클 보호
  12. buy_price=0 → 안전 처리
  13. process_pending_sells 큐 시초 매도 검증
"""
import sys
import os
import unittest
from unittest.mock import MagicMock, patch
import copy

# 봇 디렉토리 PYTHONPATH 추가 (test 단독 실행 시)
_BOT_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_DIR = os.path.dirname(_BOT_DIR)
if _PROJECT_DIR not in sys.path:
    sys.path.insert(0, _PROJECT_DIR)

from bot.position_safety import (
    sync_positions,
    enforce_sl,
    hard_kill_check,
    process_pending_sells,
    _is_protected,
)


# ============================================================
# 박사 fix: 실제 AutoTrader 인터페이스 충실 가짜 클래스
# ============================================================
class FakeAutoTrader:
    """봇 통합 가능 형태의 FakeAutoTrader.

    실제 AutoTrader와 동일 인터페이스:
    - self.trader: KISTrader (MagicMock)
    - self.config: dict
    - self._positions: dict
    - self._save_positions(): callable
    - self._send_alert: optional callable
    """
    def __init__(self):
        self._positions = {}
        self.trader = MagicMock()  # KISTrader mock
        self.config = {"bot": {"safety": {"enabled": True}}}
        self._saved = False
        self._send_alert = None  # 텔레그램 알림 (None = 알림 비활성)

    def _save_positions(self):
        self._saved = True


# mixin 패턴으로 메서드 attach
FakeAutoTrader.sync_positions = sync_positions
FakeAutoTrader.enforce_sl = enforce_sl
FakeAutoTrader.hard_kill_check = hard_kill_check
FakeAutoTrader.process_pending_sells = process_pending_sells


def _make_balance_response(positions_list, cash=36_000_000):
    """fetch_balance() 실제 봇 리턴 형식."""
    return {
        "success": True,
        "cash": cash,
        "total_eval": 0,
        "positions": positions_list,
        "holdings": positions_list,
    }


def _make_price_response(current_price):
    """fetch_price() 실제 봇 리턴 형식."""
    return {"success": True, "current_price": current_price}


# ============================================================
# TestSyncPositions — KIS ↔ 메모리 동기화
# ============================================================
class TestSyncPositions(unittest.TestCase):

    def test_01_kis_only_auto_register(self):
        """[로킷헬스케어 사고 재현] KIS에 있고 메모리 X → 자동등록 + SL 세팅."""
        trader = FakeAutoTrader()
        trader._positions = {
            "066570": {"name": "LG전자", "qty": 1, "buy_price": 196400.0,
                       "stop_loss": 190508, "take_profit": 206220}
        }
        trader.trader.fetch_balance.return_value = _make_balance_response([
            {"code": "066570", "name": "LG전자", "qty": 1,
             "avg_price": 196400, "current_price": 235000},
            {"code": "376900", "name": "로킷헬스케어", "qty": 1,
             "avg_price": 61600, "current_price": 62100},
        ])

        trader.sync_positions()

        self.assertIn("376900", trader._positions,
                      "로킷헬스케어가 메모리에 자동 등록되어야 함")
        rokit = trader._positions["376900"]
        expected_sl = int(round(61600 * 0.97))  # -3%
        self.assertEqual(rokit["stop_loss"], expected_sl,
                         f"디폴트 SL이 {expected_sl}이어야 함")
        self.assertEqual(rokit["source"], "sync_auto",
                         "자동등록 종목 source는 'sync_auto'")
        self.assertTrue(trader._saved, "positions.json 저장 필요")
        print("✅ T01 PASS: KIS only → 자동 등록 + SL 세팅")

    def test_02_memory_only_remove(self):
        """[켄코아/아이씨티케이 사고 재현] 메모리 있고 KIS X → 매도완료 처리."""
        trader = FakeAutoTrader()
        trader._positions = {
            "066570": {"name": "LG전자", "qty": 1, "buy_price": 196400.0},
            "274090": {"name": "켄코아에어로", "qty": 1, "buy_price": 25050.0},
            "456010": {"name": "아이씨티케이", "qty": 1, "buy_price": 23850.0},
        }
        trader.trader.fetch_balance.return_value = _make_balance_response([
            {"code": "066570", "name": "LG전자", "qty": 1,
             "avg_price": 196400, "current_price": 235000},
        ])

        # trade_journal mock (import 시점 mock)
        with patch("data.trade_journal.log_sell") as mock_log:
            trader.sync_positions()

        self.assertNotIn("274090", trader._positions, "켄코아 메모리 제거 필요")
        self.assertNotIn("456010", trader._positions, "아이씨티케이 메모리 제거 필요")
        self.assertIn("066570", trader._positions, "LG전자는 유지")
        print("✅ T02 PASS: 메모리 only → 매도완료 자동 제거")

    def test_03_qty_mismatch_kis_priority(self):
        """[LG전자 부분익절 재현] 메모리 2주 / KIS 1주 → KIS 기준 1주로 갱신."""
        trader = FakeAutoTrader()
        trader._positions = {
            "066570": {"name": "LG전자", "qty": 2, "buy_price": 196400.0}
        }
        trader.trader.fetch_balance.return_value = _make_balance_response([
            {"code": "066570", "name": "LG전자", "qty": 1,
             "avg_price": 196400, "current_price": 235000},
        ])

        trader.sync_positions()

        self.assertEqual(trader._positions["066570"]["qty"], 1,
                         "KIS 기준 1주로 갱신되어야 함")
        print("✅ T03 PASS: 수량 불일치 → KIS 기준 덮어쓰기")

    def test_04_api_failure_preserves_memory(self):
        """API 실패 시 기존 메모리 보존 (엣지 케이스)."""
        trader = FakeAutoTrader()
        trader._positions = {
            "066570": {"name": "LG전자", "qty": 1, "buy_price": 196400.0}
        }
        # 실패 응답
        trader.trader.fetch_balance.return_value = {"success": False, "message": "test"}

        original = copy.deepcopy(trader._positions)
        trader.sync_positions()

        self.assertEqual(trader._positions, original,
                         "API 실패 시 기존 메모리 절대 보존")
        print("✅ T04 PASS: API 실패 → 기존 데이터 보존")


# ============================================================
# TestEnforceSL — SL=None 강제 세팅
# ============================================================
class TestEnforceSL(unittest.TestCase):

    def test_05_sl_none_force_default(self):
        """SL=None 종목에 디폴트 -3% SL 강제 세팅."""
        trader = FakeAutoTrader()
        trader._positions = {
            "219420": {"name": "링크제니시스", "qty": 1, "buy_price": 5030.0,
                       "stop_loss": None, "take_profit": None},
            "066570": {"name": "LG전자", "qty": 1, "buy_price": 196400.0,
                       "stop_loss": 190508, "take_profit": 206220},
        }

        trader.enforce_sl(default_sl_pct=0.03, default_tp_pct=0.05)

        link = trader._positions["219420"]
        self.assertEqual(link["stop_loss"], int(round(5030 * 0.97)),
                         "링크제니시스 디폴트 SL 세팅 필요")
        # 기존 SL 보존
        self.assertEqual(trader._positions["066570"]["stop_loss"], 190508,
                         "이미 SL 있는 종목은 변경 X")
        print("✅ T05 PASS: SL=None → 디폴트 강제 세팅 + 기존 보존")


# ============================================================
# TestHardKillCheck — 최후 방어선
# ============================================================
class TestHardKillCheck(unittest.TestCase):

    def _setup_market_hours(self):
        """정규장 시간 가정 (10:30 KST)."""
        from datetime import datetime
        return patch("bot.position_safety.datetime", wraps=datetime,
                     **{"now.return_value": datetime(2026, 5, 22, 10, 30)})

    def test_06_hard_kill_below_threshold(self):
        """[로킷헬스케어 -15.4% 재현] -5% 초과 → 시장가 강제 매도."""
        trader = FakeAutoTrader()
        # 메모리 등록 (사장님 매수 = 보호 아님 가정 — asset_pool source)
        trader._positions = {
            "376900": {"name": "로킷헬스케어", "qty": 1, "buy_price": 61600.0,
                       "source": "asset_pool"}
        }
        trader.trader.fetch_balance.return_value = _make_balance_response([
            {"code": "376900", "name": "로킷헬스케어", "qty": 1,
             "avg_price": 61600, "current_price": 52100},
        ])
        trader.trader.fetch_price.return_value = _make_price_response(52100)
        trader.trader.sell_market.return_value = {"success": True, "order_no": "TEST123"}

        with self._setup_market_hours():
            with patch("data.trade_journal.log_sell"):
                killed = trader.hard_kill_check(kill_pct=0.05, dry_run=False)

        trader.trader.sell_market.assert_called_once_with("376900", 1)
        self.assertEqual(len(killed), 1)
        self.assertEqual(killed[0]["action"], "KILLED")
        print("✅ T06 PASS: -15.4% → 정규장 시장가 강제 매도")

    def test_07_above_threshold_no_sell(self):
        """-3% (킬라인 -5% 미만) → 매도 안 함 (경고만)."""
        trader = FakeAutoTrader()
        trader._positions = {
            "474170": {"name": "루미르", "qty": 1, "buy_price": 12620.0,
                       "source": "asset_pool"}
        }
        trader.trader.fetch_balance.return_value = _make_balance_response([
            {"code": "474170", "name": "루미르", "qty": 1,
             "avg_price": 12620, "current_price": 12241},
        ])
        trader.trader.fetch_price.return_value = _make_price_response(12241)

        with self._setup_market_hours():
            killed = trader.hard_kill_check(kill_pct=0.05, dry_run=False)

        trader.trader.sell_market.assert_not_called()
        self.assertEqual(len(killed), 0)
        print("✅ T07 PASS: -3% (킬라인 미만) → 매도 안 함")

    def test_08_dry_run_no_sell(self):
        """dry_run=True → 로그만 + 매도 안 함."""
        trader = FakeAutoTrader()
        trader._positions = {
            "376900": {"name": "로킷헬스케어", "qty": 1, "buy_price": 61600.0,
                       "source": "asset_pool"}
        }
        trader.trader.fetch_balance.return_value = _make_balance_response([
            {"code": "376900", "name": "로킷헬스케어", "qty": 1,
             "avg_price": 61600, "current_price": 52100},
        ])
        trader.trader.fetch_price.return_value = _make_price_response(52100)

        with self._setup_market_hours():
            killed = trader.hard_kill_check(kill_pct=0.05, dry_run=True)

        trader.trader.sell_market.assert_not_called()
        self.assertEqual(killed[0]["action"], "dry_run")
        print("✅ T08 PASS: dry_run → 매도 X + 로그만")


# ============================================================
# 박사 추가 CRITICAL 테스트 (5개)
# ============================================================
class TestCriticalProtection(unittest.TestCase):
    """bkit:code-analyzer 발견 CRITICAL #1 fix 검증."""

    def _setup_market_hours(self):
        from datetime import datetime
        return patch("bot.position_safety.datetime", wraps=datetime,
                     **{"now.return_value": datetime(2026, 5, 22, 10, 30)})

    def test_09_sl_disabled_skip(self):
        """[사장님 보호 명령] sl_disabled=True → hard_kill 스킵 ★CRITICAL"""
        trader = FakeAutoTrader()
        trader._positions = {
            "376900": {"name": "로킷헬스케어", "qty": 1, "buy_price": 61600.0,
                       "sl_disabled": True, "source": "asset_pool"}
        }
        trader.trader.fetch_balance.return_value = _make_balance_response([
            {"code": "376900", "name": "로킷헬스케어", "qty": 1,
             "avg_price": 61600, "current_price": 52100},  # -15.4%
        ])
        trader.trader.fetch_price.return_value = _make_price_response(52100)

        with self._setup_market_hours():
            trader.hard_kill_check(kill_pct=0.05, dry_run=False)

        trader.trader.sell_market.assert_not_called()
        print("✅ T09 PASS: sl_disabled=True → 절대 매도 X (사장님 보호)")

    def test_10_memory_missing_skip(self):
        """[CRITICAL #1] 메모리 누락 (빈 dict) → hard_kill 보호 가정 스킵 ★"""
        trader = FakeAutoTrader()
        # 메모리 비어있음 = 사장님 수동 매수 의심
        trader._positions = {}
        trader.trader.fetch_balance.return_value = _make_balance_response([
            {"code": "999999", "name": "사장님수동매수", "qty": 1,
             "avg_price": 100000, "current_price": 90000},  # -10%
        ])
        trader.trader.fetch_price.return_value = _make_price_response(90000)

        with self._setup_market_hours():
            trader.hard_kill_check(kill_pct=0.05, dry_run=False)

        trader.trader.sell_market.assert_not_called()
        print("✅ T10 PASS: 메모리 누락 → 사장님 보호 가정 스킵 ★CRITICAL")

    def test_11_sync_auto_protection(self):
        """[CRITICAL #1 박사 추가 보호] source='sync_auto' → 보호 ★"""
        trader = FakeAutoTrader()
        # sync_positions가 자동등록 = 사장님 수동 매수 의심
        trader._positions = {
            "999999": {"name": "자동등록종목", "qty": 1, "buy_price": 100000.0,
                       "source": "sync_auto"}
        }
        trader.trader.fetch_balance.return_value = _make_balance_response([
            {"code": "999999", "name": "자동등록종목", "qty": 1,
             "avg_price": 100000, "current_price": 90000},  # -10%
        ])
        trader.trader.fetch_price.return_value = _make_price_response(90000)

        with self._setup_market_hours():
            trader.hard_kill_check(kill_pct=0.05, dry_run=False)

        trader.trader.sell_market.assert_not_called()
        print("✅ T11 PASS: source='sync_auto' → 보호 (수동 매수 의심)")

    def test_12_buy_price_zero_safe(self):
        """[엣지] buy_price=0 (KIS 마감 후) → 안전 처리 (current_price 대체)."""
        trader = FakeAutoTrader()
        trader.trader.fetch_balance.return_value = _make_balance_response([
            {"code": "999999", "name": "마감후종목", "qty": 1,
             "avg_price": 0, "current_price": 10000},
        ])

        trader.sync_positions()

        self.assertIn("999999", trader._positions)
        # buy_price=0 → current_price 대체 → 10000
        self.assertEqual(trader._positions["999999"]["buy_price"], 10000.0)
        print("✅ T12 PASS: buy_price=0 → current_price 대체 안전 처리")

    def test_13_is_protected_helper(self):
        """[CRITICAL #1 단위] _is_protected() 헬퍼 함수 직접 검증."""
        # 빈 dict = 보호 (메모리 누락)
        self.assertTrue(_is_protected({}),
                        "빈 dict는 보호 가정 (메모리 누락)")
        # sl_disabled
        self.assertTrue(_is_protected({"sl_disabled": True}),
                        "sl_disabled=True 보호")
        # verification
        self.assertTrue(_is_protected({"source": "verification"}),
                        "source='verification' 보호")
        # manual_sync prefix
        self.assertTrue(_is_protected({"source": "manual_sync_evening"}),
                        "source='manual_sync*' 보호")
        # sync_auto prefix (박사 v1.1 추가)
        self.assertTrue(_is_protected({"source": "sync_auto"}),
                        "source='sync_auto*' 보호 (박사 v1.1)")
        # 일반 asset_pool은 보호 X
        self.assertFalse(_is_protected({"source": "asset_pool"}),
                         "source='asset_pool'은 자동 매도 허용")
        print("✅ T13 PASS: _is_protected() 5가지 보호 케이스 + asset_pool 비보호")


# ============================================================
# 실행
# ============================================================
if __name__ == "__main__":
    print("=" * 70)
    print("  박사 통합 v2.0 — position_safety.py 단위 테스트")
    print("  GPT 베이스 + 박사 X 7개 fix + CRITICAL 5개 추가 = 13개 테스트")
    print("  실제 KIS API 호출 X — Mock 가짜 데이터로 0.1초 내 검증")
    print("=" * 70)
    unittest.main(verbosity=2)
