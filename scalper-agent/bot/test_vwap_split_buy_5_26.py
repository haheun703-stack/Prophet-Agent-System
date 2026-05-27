# -*- coding: utf-8 -*-
"""★ 사장님 5/26 VWAP 3분할 매수 단위 테스트 (50/30/20) ★"""
import asyncio
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from bot import vwap_split_buy
from bot.vwap_split_buy import (
    execute_vwap_split_buy,
    SPLIT_RATIO_1, SPLIT_RATIO_2, SPLIT_RATIO_3,
    PULLBACK_PCT_2, PULLBACK_PCT_3,
)

# ★ 단위 테스트 chase 대기 짧게 patch (실제 매매는 60/180/300초) ★
vwap_split_buy.CHASE_WAIT_1 = 2
vwap_split_buy.CHASE_WAIT_2 = 2
vwap_split_buy.CHASE_WAIT_3 = 2


def _run_auto_enabled(coro):
    with patch("bot.vwap_split_buy.is_auto_trade_disabled", return_value=False):
        return asyncio.run(coro)


def test_split_ratios():
    """사장님 5/26 분할 비율 확인 (50/30/20)."""
    assert SPLIT_RATIO_1 == 0.50, f"1차 50% 기대: {SPLIT_RATIO_1}"
    assert SPLIT_RATIO_2 == 0.30, f"2차 30% 기대: {SPLIT_RATIO_2}"
    assert SPLIT_RATIO_3 == 0.20, f"3차 20% 기대: {SPLIT_RATIO_3}"
    assert SPLIT_RATIO_1 + SPLIT_RATIO_2 + SPLIT_RATIO_3 == 1.0
    print(f"[PASS] 분할 비율 50/30/20 (사장님 5/26 결정)")


def test_pullback_targets():
    """눌림 임계값 (-1.5% / -3%)."""
    assert PULLBACK_PCT_2 == -1.5
    assert PULLBACK_PCT_3 == -3.0, "3차 -3% (사장님 5/23 영구 룰)"
    print(f"[PASS] 눌림 임계값 -1.5% / -3% (사장님 룰)")


def test_qty_split_100주():
    """100주 분할 = 50/30/20."""
    qty_1 = int(100 * 0.50)
    qty_2 = int(100 * 0.30)
    qty_3 = 100 - qty_1 - qty_2
    assert qty_1 == 50
    assert qty_2 == 30
    assert qty_3 == 20
    print(f"[PASS] 100주 → 1차 50 / 2차 30 / 3차 20")


def test_qty_split_72주_삼화_시나리오():
    """5/26 사장님 삼화 47주 시뮬레이션 (만약 72주였다면)."""
    # max_buy = 5,400,000원 / 시초가 121,500 = 44주
    total = 44
    qty_1 = max(1, int(total * 0.50))
    qty_2 = max(1, int(total * 0.30))
    qty_3 = total - qty_1 - qty_2
    assert qty_1 == 22  # 50%
    assert qty_2 == 13  # 30%
    assert qty_3 == 9   # 20%
    print(f"[PASS] 44주 (삼화 시뮬) → 1차 22 / 2차 13 / 3차 9")


def test_async_scenario_A_strong_trend():
    """시나리오 A: 강한 추세 (1차만 진입, 2/3차 미도달)."""
    async def run():
        trader = MagicMock()
        # chase_buy 1차 성공
        trader.chase_buy = MagicMock(return_value={"success": True, "avg_price": 121500})
        trader.buy_market = MagicMock(return_value={"success": True, "avg_price": 121500})
        # fetch_price: 항상 상승 (눌림 X)
        trader.fetch_price = MagicMock(return_value={"current_price": 130000})

        result = await execute_vwap_split_buy(
            trader=trader, code="001820", name="삼화콘덴서",
            open_price=121500, total_qty=44,
        )
        assert result.success is True
        assert result.leg1_bought == 22
        # 2차/3차 미체결 (가격 상승 → 눌림 도달 X)
        assert result.leg2_bought == 0, f"강한 추세 2차 미체결 기대: got {result.leg2_bought}"
        assert result.leg3_bought == 0, f"강한 추세 3차 미체결 기대: got {result.leg3_bought}"
        # ★ chase_buy max_wait_sec를 짧게 만들지 않아 실제 호출에서 3분/5분 대기하므로 mock에서는 X
        return result.leg1_bought

    bought_1 = _run_auto_enabled(run())
    print(f"[PASS] 시나리오 A (강한 추세): 1차 {bought_1}주만 진입 ({bought_1/44*100:.0f}%)")


def test_async_scenario_C_deep_pullback():
    """시나리오 C: 깊은 눌림 (1차+2차+3차 100% 진입)."""
    async def run():
        trader = MagicMock()
        trader.chase_buy = MagicMock(return_value={"success": True, "avg_price": 121500})
        # fetch_price: 깊은 눌림 (-3% 즉시 도달)
        trader.fetch_price = MagicMock(return_value={"current_price": 117855})  # 121500 × 0.97

        result = await execute_vwap_split_buy(
            trader=trader, code="001820", name="삼화콘덴서",
            open_price=121500, total_qty=44,
        )
        assert result.success is True
        assert result.leg1_bought == 22
        assert result.leg2_bought == 13  # -1.5% 도달 (117855 < 119,677.5)
        assert result.leg3_bought == 9   # -3% 도달
        assert result.total_bought_qty == 44   # 100%
        return result.total_bought_qty

    bought_total = _run_auto_enabled(run())
    print(f"[PASS] 시나리오 C (깊은 눌림): {bought_total}/44주 100% 진입")


def test_async_scenario_market_fallback():
    """1차 chase 미체결 → 시장가 폴백 (사장님 5/26 통찰)."""
    async def run():
        trader = MagicMock()
        # chase_buy 1차 미체결 (강한 추세 = 가격 빠르게 상승 → 미체결)
        trader.chase_buy = MagicMock(return_value={"success": False, "message": "chase_buy 60초 미체결"})
        trader.buy_market = MagicMock(return_value={"success": True, "avg_price": 125000})  # 시장가 폴백
        trader.fetch_price = MagicMock(return_value={"current_price": 130000})

        result = await execute_vwap_split_buy(
            trader=trader, code="001820", name="삼화콘덴서",
            open_price=121500, total_qty=44,
        )
        assert result.success is True, "시장가 폴백으로 진입 성공"
        assert result.leg1_market_fallback is True, "1차 시장가 폴백 발동 마커"
        assert result.leg1_bought == 22
        # buy_market 호출 확인
        trader.buy_market.assert_called_once()
        return result

    result = _run_auto_enabled(run())
    print(f"[PASS] 시장가 폴백: 1차 chase 미체결 → 시장가 진입 보장 (사장님 5/26 통찰)")


if __name__ == "__main__":
    print("=" * 70)
    print("★ VWAP 3분할 매수 단위 테스트 (사장님 5/26 50/30/20) ★")
    print("=" * 70)
    tests = [
        test_split_ratios,
        test_pullback_targets,
        test_qty_split_100주,
        test_qty_split_72주_삼화_시나리오,
        test_async_scenario_A_strong_trend,
        test_async_scenario_C_deep_pullback,
        test_async_scenario_market_fallback,
    ]
    passed, failed = 0, 0
    for t in tests:
        try:
            t()
            passed += 1
        except AssertionError as e:
            failed += 1
            print(f"[FAIL] {t.__name__}: {e}")
        except Exception as e:
            failed += 1
            print(f"[ERROR] {t.__name__}: {type(e).__name__}: {e}")
    print("=" * 70)
    print(f"결과: {passed}/{passed + failed} PASS, {failed} FAIL")
    sys.exit(0 if failed == 0 else 1)
