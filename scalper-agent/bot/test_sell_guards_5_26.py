# -*- coding: utf-8 -*-
"""★ 5/26 사고 후 신설 — 매도 가드 통합 테스트 ★

_is_sell_protected 함수가 사장님 영구 룰에 맞게 매도 차단/허용 정확 작동 검증.

시나리오 5건:
  1. sync_auto 종목 → 매도 차단 (옛 자동등록 보호)
  2. manual_president 종목 → 매도 허용 (단타봇 트레일링 핸들링)
  3. sl_disabled=True 종목 → 매도 차단 (사장님 명령)
  4. manual_sync 종목 → 매도 차단 (수동 sync)
  5. 빈 메모리 (사장님 수동 매수 의심) → 매도 차단

5/26 사고 5건 모두 이 가드 미적용이 원인.
"""
import sys
from pathlib import Path
from unittest.mock import MagicMock

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


class FakeAutoTrader:
    """_is_sell_protected 함수만 테스트하기 위한 최소 wrapper."""
    def __init__(self):
        self._positions = {}
        self._send_alert = None

    # _is_sell_protected 메소드를 AutoTrader에서 import
    def __init_subclass__(cls):
        from bot.auto_trader import AutoTrader
        cls._is_sell_protected = AutoTrader._is_sell_protected


def get_protected_fn():
    """AutoTrader._is_sell_protected를 unbound 메소드로 가져오기."""
    from bot.auto_trader import AutoTrader
    return AutoTrader._is_sell_protected


def test_sync_auto_blocked():
    """시나리오 1: sync_auto 종목 → 매도 차단."""
    fn = get_protected_fn()
    fake = FakeAutoTrader()
    fake._positions["001820"] = {
        "name": "삼화콘덴서", "qty": 47,
        "source": "sync_auto",
        "buy_price": 122500,
    }
    blocked = fn(fake, "001820", "test_sync_auto")
    assert blocked is True, "sync_auto 종목은 매도 차단되어야 함"
    print("[PASS] 시나리오 1: sync_auto → 매도 차단 ✅")


def test_manual_president_allowed():
    """시나리오 2: manual_president 종목 → 매도 허용 (단타봇 트레일링 핸들링)."""
    fn = get_protected_fn()
    fake = FakeAutoTrader()
    fake._positions["184230"] = {
        "name": "SGA솔루션즈", "qty": 800,
        "source": "manual_president",  # ★ 사장님 5/26 명령: 단타봇 핸들링 OK
        "buy_price": 4247,
    }
    blocked = fn(fake, "184230", "test_manual_president")
    assert blocked is False, "manual_president 종목은 매도 허용되어야 함 (사장님 핸들링)"
    print("[PASS] 시나리오 2: manual_president → 매도 허용 (사장님 핸들링) ✅")


def test_sl_disabled_blocked():
    """시나리오 3: sl_disabled=True → 매도 차단."""
    fn = get_protected_fn()
    fake = FakeAutoTrader()
    fake._positions["005930"] = {
        "name": "삼성전자", "qty": 1,
        "source": "asset_pool",
        "sl_disabled": True,   # 사장님 보호 명령
        "buy_price": 300000,
    }
    blocked = fn(fake, "005930", "test_sl_disabled")
    assert blocked is True, "sl_disabled=True → 매도 차단"
    print("[PASS] 시나리오 3: sl_disabled=True → 매도 차단 ✅")


def test_manual_sync_blocked():
    """시나리오 4: manual_sync 종목 → 매도 차단."""
    fn = get_protected_fn()
    fake = FakeAutoTrader()
    fake._positions["007330"] = {
        "name": "푸른저축은행", "qty": 1,
        "source": "manual_sync_5_22",
        "buy_price": 11700,
    }
    blocked = fn(fake, "007330", "test_manual_sync")
    assert blocked is True, "manual_sync 종목은 매도 차단"
    print("[PASS] 시나리오 4: manual_sync → 매도 차단 ✅")


def test_empty_memory_blocked():
    """시나리오 5: 빈 메모리 (사장님 수동 매수 의심) → 매도 차단."""
    fn = get_protected_fn()
    fake = FakeAutoTrader()
    # positions에 코드 없음 → 빈 dict 반환
    blocked = fn(fake, "999999", "test_empty_memory")
    assert blocked is True, "빈 메모리 (메모리 미등록) → 보호 가정 차단"
    print("[PASS] 시나리오 5: 빈 메모리 → 매도 차단 (보호 가정) ✅")


def test_asset_pool_allowed():
    """추가 시나리오 6: asset_pool 종목 (단타봇 자율 매수) → 매도 허용."""
    fn = get_protected_fn()
    fake = FakeAutoTrader()
    fake._positions["069540"] = {
        "name": "빛과전자", "qty": 1016,
        "source": "asset_pool",   # 단타봇 자율 매수
        "buy_price": 6990,
    }
    blocked = fn(fake, "069540", "test_asset_pool")
    assert blocked is False, "asset_pool 종목은 매도 허용 (단타봇 자율 핸들링)"
    print("[PASS] 시나리오 6: asset_pool → 매도 허용 (단타봇 자율) ✅")


def test_pre_close_d_allowed():
    """추가 시나리오 7: pre_close_d 종목 (5/26 새 룰 D) → 매도 허용."""
    fn = get_protected_fn()
    fake = FakeAutoTrader()
    fake._positions["036010"] = {
        "name": "아비코전자", "qty": 859,
        "source": "pre_close_d",   # 새 룰 D
        "buy_price": 12858,
    }
    blocked = fn(fake, "036010", "test_pre_close_d")
    assert blocked is False, "pre_close_d 종목은 매도 허용 (단타봇 트레일링)"
    print("[PASS] 시나리오 7: pre_close_d → 매도 허용 (단타봇 자율) ✅")


if __name__ == "__main__":
    print("=" * 70)
    print("★ 매도 가드 통합 테스트 (5/26 사고 후 신설) ★")
    print("=" * 70)
    tests = [
        test_sync_auto_blocked,
        test_manual_president_allowed,
        test_sl_disabled_blocked,
        test_manual_sync_blocked,
        test_empty_memory_blocked,
        test_asset_pool_allowed,
        test_pre_close_d_allowed,
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
