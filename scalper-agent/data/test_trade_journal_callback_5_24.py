# -*- coding: utf-8 -*-
"""★ 5/24 15번 단위 테스트 ★ trade_journal callback -매매 즉시 텔레그램 보고 매커니즘.

검증:
    - register_trade_callback으로 등록 가능
    - log_buy/log_sell 후 callback 자동 호출 (DB 적재 실패해도 호출)
    - callback에 정확한 인자 전달 (side, code, name, qty, price, source, entry_price, note)
    - callback 예외 발생해도 매매 자체는 영향 없음 (best-effort)
"""
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))

import importlib

# 모듈 fresh import (이전 테스트 callback 누수 방지)
if "data.trade_journal" in sys.modules:
    del sys.modules["data.trade_journal"]
from data import trade_journal as tj


# ── 테스트 보조 ──
_received = []


def _capture_cb(**kwargs):
    """모든 인자를 _received에 append."""
    _received.append(kwargs)


def _raising_cb(**kwargs):
    """callback 예외 발생 시뮬레이션."""
    raise RuntimeError("intentional callback failure for test")


def _reset():
    _received.clear()
    tj._trade_callbacks.clear()


def test_register_and_buy_callback():
    """register_trade_callback 후 log_buy → callback 호출."""
    _reset()
    tj.register_trade_callback(_capture_cb)

    # log_buy는 supabase 없으면 False 반환하지만 callback은 fire
    tj.log_buy("999999", "테스트A", 10, 1000.0, source="external",
                signal_tags="test", final_score=80, note="test_note")

    assert len(_received) == 1, f"callback 1회 호출 기대, 실제 {len(_received)}"
    r = _received[0]
    assert r["side"] == "BUY"
    assert r["code"] == "999999"
    assert r["name"] == "테스트A"
    assert r["qty"] == 10
    assert r["price"] == 1000.0
    assert r["source"] == "external"
    assert r["note"] == "test_note"
    print(f"[PASS] BUY callback 인자 정확 전달")


def test_sell_callback_with_entry_price():
    """log_sell → entry_price 포함 callback 호출."""
    _reset()
    tj.register_trade_callback(_capture_cb)

    tj.log_sell("999999", "테스트B", 5, 1100.0, 1000.0,
                 event_type="sell_tp", source="external", note="익절_테스트")

    assert len(_received) == 1
    r = _received[0]
    assert r["side"] == "SELL"
    assert r["qty"] == 5
    assert r["price"] == 1100.0
    assert r["entry_price"] == 1000.0
    assert "sell_tp" in r["note"]
    print(f"[PASS] SELL callback + entry_price + event_type note 전달")


def test_callback_exception_isolated():
    """callback 예외 발생해도 다른 callback과 매매에 영향 X."""
    _reset()
    tj.register_trade_callback(_raising_cb)
    tj.register_trade_callback(_capture_cb)

    # log_buy 진행 -_raising_cb 예외 발생해도 _capture_cb 호출되어야
    tj.log_buy("999998", "테스트C", 1, 500.0, source="external")

    assert len(_received) == 1, f"_capture_cb 호출 기대, 실제 {len(_received)}"
    assert _received[0]["code"] == "999998"
    print(f"[PASS] callback 예외 격리 -다른 callback 정상 호출")


def test_duplicate_callback_registration():
    """동일 callback 중복 등록 시 1개만 유지."""
    _reset()
    tj.register_trade_callback(_capture_cb)
    tj.register_trade_callback(_capture_cb)   # 중복

    tj.log_buy("999997", "테스트D", 1, 100.0, source="external")
    assert len(_received) == 1, f"중복 등록 무시 기대, 실제 {len(_received)}"
    print(f"[PASS] 중복 등록 방지 -callback 1회만 호출")


if __name__ == "__main__":
    print("=" * 60)
    print("★ 5/24 15번 trade_journal callback 단위 테스트 ★")
    print("=" * 60)
    tests = [
        test_register_and_buy_callback,
        test_sell_callback_with_entry_price,
        test_callback_exception_isolated,
        test_duplicate_callback_registration,
    ]
    passed = 0
    failed = 0
    for t in tests:
        try:
            t()
            passed += 1
        except AssertionError as e:
            print(f"[FAIL] {t.__name__}: {e}")
            failed += 1
        except Exception as e:
            print(f"[ERROR] {t.__name__}: {type(e).__name__}: {e}")
            failed += 1
    print("=" * 60)
    print(f"결과: {passed}/{len(tests)} PASS, {failed} FAIL")
    sys.exit(0 if failed == 0 else 1)
