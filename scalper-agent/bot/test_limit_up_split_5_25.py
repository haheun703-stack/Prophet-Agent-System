# -*- coding: utf-8 -*-
"""★ 5/25 단위 테스트 ★ 사장님 룰 3 (상한가 분할 매도 + 이월) 검증.

사장님 5/25 통찰:
  - 상한가 = KRX +30% 가격제한선 → 트레일링 발동 X
  - 절반 매도 + 절반 NXT 이월 = 안전 락인 + 잭팟 추격
  - D+1 트레일링 즉시 활성 (갭다운 인내 / 최고점 -3%)
"""
import sys
from datetime import date, timedelta
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))

from bot.limit_up_split_sell import (
    should_trigger_split, is_next_day_position, get_trail_activation,
    LIMIT_UP_TRIGGER_PCT, SPLIT_RATIO, D1_TRAIL_PCT,
)


def test_below_trigger():
    """+24% (트리거 미달) → 분할 매도 X."""
    d = should_trigger_split(pnl_pct=24.0, qty=100)
    assert not d.should_split, "트리거 미달 시 분할 X"
    print("[PASS] +24% (트리거 24<25) → 분할 매도 X")


def test_at_trigger_threshold():
    """+25% 정확 (경계) → 분할 매도 발동."""
    d = should_trigger_split(pnl_pct=25.0, qty=100)
    assert d.should_split, "+25% 경계 정확 발동"
    assert d.sell_qty == 50, f"50주 매도 기대, 실제 {d.sell_qty}"
    assert d.hold_qty == 50, f"50주 보유 기대, 실제 {d.hold_qty}"
    print("[PASS] +25% 경계 → 50/50 분할 (50주 매도 + 50주 이월)")


def test_limit_up_29pct():
    """+29% 상한가 / 100주 → 50/50 분할 + next_day_marker."""
    d = should_trigger_split(pnl_pct=29.0, qty=100)
    assert d.should_split, "분할 발동"
    assert d.sell_qty == 50 and d.hold_qty == 50
    assert d.next_day_marker, "next_day_marker=True"
    print("[PASS] +29% 상한가 / 100주 → 절반 매도 + 절반 D+1 이월 마커")


def test_odd_qty_split():
    """+29% / 101주 → 절반 (50주 매도, 51주 이월)."""
    d = should_trigger_split(pnl_pct=29.0, qty=101)
    assert d.sell_qty == 50, f"50주 매도 기대 (101*0.5=50.5 → 50), 실제 {d.sell_qty}"
    assert d.hold_qty == 51, f"51주 이월 기대, 실제 {d.hold_qty}"
    print("[PASS] +29% / 101주 → 50주 매도 + 51주 이월 (홀수 처리)")


def test_single_share_no_split():
    """+29% / 1주만 → 분할 불가."""
    d = should_trigger_split(pnl_pct=29.0, qty=1)
    assert not d.should_split
    assert "분할 불가" in d.reason
    print("[PASS] +29% / 1주 → 분할 불가 (전량 매도/이월 선택은 호출자가)")


def test_duplicate_prevention():
    """already_split=True → 중복 분할 방지."""
    d = should_trigger_split(pnl_pct=30.0, qty=50, already_split=True)
    assert not d.should_split
    assert "이미 분할" in d.reason
    print("[PASS] 중복 분할 방지 (already_split=True)")


def test_next_day_position_today():
    """매수일 = 오늘 → D+1 이월 아직 X."""
    today = date.today()
    is_d1 = is_next_day_position(buy_date=today, current_date=today, pending_next_day=True)
    assert not is_d1, "오늘 매수 + 오늘 = D+1 X"
    print("[PASS] 매수일=오늘 → D+1 X (당일)")


def test_next_day_position_tomorrow():
    """매수일 = 어제 → D+1 이월 O."""
    today = date.today()
    yesterday = today - timedelta(days=1)
    is_d1 = is_next_day_position(buy_date=yesterday, current_date=today, pending_next_day=True)
    assert is_d1, "어제 매수 + 오늘 = D+1 O"
    print("[PASS] 매수일=어제 → D+1 이월 (트레일링 즉시 활성)")


def test_next_day_marker_required():
    """pending_next_day=False → 일반 매수 (D+1 X)."""
    today = date.today()
    yesterday = today - timedelta(days=1)
    is_d1 = is_next_day_position(buy_date=yesterday, current_date=today, pending_next_day=False)
    assert not is_d1, "마커 없으면 D+1 처리 X"
    print("[PASS] pending_next_day=False → 일반 매수 처리")


def test_d1_activation_immediate():
    """D+1 이월 + 갭다운 -8% → 트레일링 즉시 활성 (사장님 룰)."""
    active, trail, reason = get_trail_activation(is_next_day=True, pnl_pct=-8.0)
    assert active, "D+1 이월 = 갭다운에도 활성"
    assert trail == 3.0, f"trail=-3% 기대, 실제 -{trail}%"
    print(f"[PASS] D+1 이월 + 갭다운 -8% → 즉시 활성 -{trail}%")


def test_d1_activation_recovery():
    """D+1 이월 + 회복 +5% → 트레일링 활성 유지."""
    active, trail, reason = get_trail_activation(is_next_day=True, pnl_pct=5.0)
    assert active and trail == 3.0
    print("[PASS] D+1 이월 + 회복 +5% → 활성 -3%")


def test_normal_below_3pct():
    """일반 매수 +2% → 트레일링 비활성 (사장님 룰 1)."""
    active, trail, reason = get_trail_activation(is_next_day=False, pnl_pct=2.0)
    assert not active, "+2% < 3% 비활성"
    print("[PASS] 일반 매수 +2% → 비활성 (NORMAL SL -3%만)")


def test_normal_at_3pct():
    """일반 매수 +3% → 트레일링 활성."""
    active, trail, reason = get_trail_activation(is_next_day=False, pnl_pct=3.0)
    assert active and trail == 3.0
    print("[PASS] 일반 매수 +3% → 활성 -3% (경계 정확)")


def test_constants_consistency():
    """상수 검증: 사장님 5/25 룰과 일치."""
    assert LIMIT_UP_TRIGGER_PCT == 25.0, "트리거 +25%"
    assert SPLIT_RATIO == 0.5, "50/50 분할"
    assert D1_TRAIL_PCT == 3.0, "D+1 -3% 일관"
    print("[PASS] 상수 (25% trigger / 50% split / -3% trail) — 사장님 룰 정확")


if __name__ == "__main__":
    print("=" * 70)
    print("★ 5/25 사장님 룰 3 — 상한가 분할 매도 + NXT 이월 검증 ★")
    print("=" * 70)
    tests = [
        test_below_trigger,
        test_at_trigger_threshold,
        test_limit_up_29pct,
        test_odd_qty_split,
        test_single_share_no_split,
        test_duplicate_prevention,
        test_next_day_position_today,
        test_next_day_position_tomorrow,
        test_next_day_marker_required,
        test_d1_activation_immediate,
        test_d1_activation_recovery,
        test_normal_below_3pct,
        test_normal_at_3pct,
        test_constants_consistency,
    ]
    passed, failed = 0, 0
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
    print("=" * 70)
    print(f"결과: {passed}/{len(tests)} PASS, {failed} FAIL")
    sys.exit(0 if failed == 0 else 1)
