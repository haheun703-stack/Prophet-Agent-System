# -*- coding: utf-8 -*-
"""★ 5/25 재검수 회귀 테스트 ★ _compute_dynamic_trailing_sl (실제 매매 경로) 단순화 적용 검증.

어제 commit 7fa90b5에서 dynamic_trailing.py (모듈)만 단순화 했고
auto_trader.py의 _compute_dynamic_trailing_sl (실제 매매 호출)은 옛 multi-zone 그대로 둔 사고.

5/26 D-Day 재검수 (code-analyzer agent CRITICAL #1) 후 fix.
이 테스트는 실제 매매 코드가 정말 사장님 5/25 영구 룰 (-3% 일관) 적용하는지 증명.

영구 룰: [[feedback_trailing_only_tp]] + 사장님 5/25 룰 1
"""
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from bot.auto_trader import AutoTrader


def _fake_self():
    """AutoTrader._compute_dynamic_trailing_sl unbound 호출용 fake self.

    해당 함수는 self 멤버 안 씀 (pos 인자로만 동작) → fake 객체로 충분.
    """
    return type("FakeAutoTrader", (), {})()


def call_trail(pos: dict, current_price: int, trend_strength=None) -> int:
    """unbound 메소드 호출 helper."""
    return AutoTrader._compute_dynamic_trailing_sl(_fake_self(), pos, current_price, trend_strength)


def test_below_activation_no_trail():
    """+3% 미만 → trailing_sl 변동 없음 (NORMAL SL만 호출자가 적용)."""
    pos = {"entry_price": 10000, "trailing_sl": 0, "high_watermark": 0}
    sl = call_trail(pos, 10200)   # +2%
    assert sl == 0, f"Expected 0 (비활성), got {sl}"
    assert not pos.get("trailing_activated"), "활성화되면 안 됨"
    print("[PASS] +2% (비활성) → trailing_sl 0, trailing_activated=False")


def test_activation_at_3pct():
    """+3% 정확 도달 → 활성 (본전 확보는 +3% 초과 시만, strict > 비교)."""
    pos = {"entry_price": 10000, "trailing_sl": 0, "high_watermark": 0}
    sl = call_trail(pos, 10300)   # +3% 정확
    assert pos.get("trailing_activated"), "활성화되어야 함"
    # 고점 10,300 × 0.97 = 9,991 (본전 확보 조건 hwm > entry*1.03 → 10300 > 10300 False)
    assert sl == 9991, f"+3% 정확 = SL 9991 (본전 확보 strict 미발동): got {sl}"
    print(f"[PASS] +3% 정확 도달 → 활성 + SL {sl} (사장님 -3% 룰 정확)")


def test_activation_above_3pct_break_even():
    """+3% 초과 → 본전 확보 발동."""
    pos = {"entry_price": 10000, "trailing_sl": 0, "high_watermark": 0}
    sl = call_trail(pos, 10301)   # +3.01%
    assert pos.get("trailing_activated"), "활성화"
    # hwm 10,301 > 10,300 → 본전 확보 → max(int(10,301*0.97)=9,991, 10,000) = 10,000
    assert sl == 10000, f"+3.01% 초과 = 본전 확보 SL 10,000: got {sl}"
    print(f"[PASS] +3.01% 초과 → SL {sl} (본전 확보)")


def test_5pct_zone_uses_3pct_trail():
    """+5% (이전 -5% 폐기) → 고점 -3% 일관."""
    pos = {"entry_price": 10000, "trailing_sl": 0, "high_watermark": 10000}
    sl = call_trail(pos, 10500)   # +5%
    # 고점 10,500 × 0.97 = 10,185
    expected = int(10500 * 0.97)
    assert sl == expected, f"Expected {expected} (-3%), got {sl} — 옛 -5% 폐기 검증"
    print(f"[PASS] +5% zone → SL {sl} = -3% (옛 -5% 폐기)")


def test_12pct_strong_uses_3pct_trail():
    """+12% strong (이전 -7% 폐기) → 고점 -3% 일관. ★ 사장님 영구 룰 핵심 ★"""
    pos = {"entry_price": 10000, "trailing_sl": 0, "high_watermark": 10000}
    sl = call_trail(pos, 11200)   # +12%
    # 고점 11,200 × 0.97 = 10,864 (옛 코드는 11,200 × 0.93 = 10,416 → -7%)
    expected = int(11200 * 0.97)
    assert sl == expected, f"Expected {expected} (-3%), got {sl} — 옛 -7% 폐기 검증"
    # 옛 코드 검증 (실패 시 fix 미완료)
    old_value = int(11200 * 0.93)
    assert sl != old_value, f"★ 옛 -7% 적용됨 ({old_value}) — 5/25 fix 미적용 ★"
    print(f"[PASS] +12% strong → SL {sl} = -3% (옛 -7% 완전 폐기, 사장님 룰 정확)")


def test_20pct_zone_uses_3pct_trail():
    """+20% (이전 -10% 폐기) → 고점 -3% 일관."""
    pos = {"entry_price": 10000, "trailing_sl": 0, "high_watermark": 10000}
    sl = call_trail(pos, 12000)   # +20%
    expected = int(12000 * 0.97)
    assert sl == expected, f"Expected {expected} (-3%), got {sl} — 옛 -10% 폐기 검증"
    print(f"[PASS] +20% zone → SL {sl} = -3% (옛 -10% 폐기)")


def test_26pct_limit_up_imminent_3pct_trail():
    """+26% 상한가 임박 → trail -3% + limit_up_phase 마커.

    분리 매도는 limit_up_split_sell 모듈이 처리 (15:26 룰 B + 절반 +29% 매도).
    트레일링 자체는 -3% 일관.
    """
    pos = {"entry_price": 10000, "trailing_sl": 0, "high_watermark": 10000}
    sl = call_trail(pos, 12600)   # +26%
    expected = int(12600 * 0.97)
    assert sl == expected, f"Expected {expected} (-3%), got {sl}"
    assert pos.get("limit_up_phase"), "limit_up_phase 마커 설정되어야 함"
    print(f"[PASS] +26% limit_up_imminent → SL {sl} = -3% + 마커 설정")


def test_ratchet_sl_never_decreases():
    """ratchet: SL은 절대 내려가지 않음 (고점 후 하락해도 유지)."""
    pos = {"entry_price": 10000, "trailing_sl": 0, "high_watermark": 10000}
    sl1 = call_trail(pos, 11500)   # +15% 고점
    # 가격 빠짐 (+10% 회귀)
    sl2 = call_trail(pos, 11000)
    assert sl2 == sl1, f"SL 내려가면 안 됨: {sl1} → {sl2}"
    print(f"[PASS] ratchet — 고점 11,500 SL {sl1} / 회귀 11,000 SL {sl2} (유지)")


def test_high_watermark_tracking():
    """high_watermark는 항상 최대값 추적."""
    pos = {"entry_price": 10000, "trailing_sl": 0, "high_watermark": 0}
    call_trail(pos, 10500)   # hwm 10,500
    call_trail(pos, 11000)   # hwm 11,000
    call_trail(pos, 10800)   # 하락 — hwm 유지
    assert pos["high_watermark"] == 11000, f"hwm 11,000 유지: got {pos['high_watermark']}"
    print(f"[PASS] high_watermark 추적 — 11,000 정확 유지")


def test_no_old_multizone_logic_anywhere():
    """전 zone 트레일링 폭이 정확히 -3% 일관 (옛 -5/-7/-10/-12/-15% 0건 검증)."""
    scenarios = [
        (10100, 1.0, "weak"),
        (10300, 3.0, "weak"),
        (10500, 5.0, "moderate"),
        (10700, 7.0, "moderate"),
        (11000, 10.0, "strong"),
        (11500, 15.0, "strong"),
        (12000, 20.0, "strong"),
        (12500, 25.0, "limit_up_imminent"),
        (12900, 29.0, "limit_up_imminent"),
    ]
    for price, pnl, expected_strength in scenarios:
        pos = {"entry_price": 10000, "trailing_sl": 0, "high_watermark": 10000}
        sl = call_trail(pos, price)
        if pnl < 3.0:
            assert sl == 0, f"{pnl}% 비활성이어야 함"
            continue
        # 활성 zone — 고점 -3% 일관 검증
        expected = int(price * 0.97)
        # 본전 확보 적용
        if price > 10000 * 1.03:
            expected = max(expected, 10000)
        assert sl == expected, (
            f"★ {pnl}% zone SL {sl} != expected {expected} — "
            f"옛 multi-zone 잔여 (-5/-7/-10/-12/-15) 검출 ★"
        )
    print(f"[PASS] 9개 zone 모두 trail=-3% 일관 (옛 multi-zone 완전 폐기 증명)")


def test_dynamic_trailing_module_delegation():
    """위임 확인 — decide_trailing 모듈 호출."""
    from bot.dynamic_trailing import decide_trailing
    d = decide_trailing(pnl_pct=12.0)
    assert d.trail_pct == 3.0, f"모듈 trail_pct 3.0 기대: {d.trail_pct}"
    assert d.activation_pct == 3.0, f"모듈 activation_pct 3.0 기대: {d.activation_pct}"
    assert d.strength == "strong", f"+12% strong 분류"
    print(f"[PASS] 위임 — decide_trailing(+12%) trail=-3.0% activation=+3.0% strength=strong")


if __name__ == "__main__":
    print("=" * 70)
    print("★ 5/25 재검수 회귀 테스트 ★ _compute_dynamic_trailing_sl 실제 매매 검증")
    print("=" * 70)

    tests = [
        test_below_activation_no_trail,
        test_activation_at_3pct,
        test_activation_above_3pct_break_even,
        test_5pct_zone_uses_3pct_trail,
        test_12pct_strong_uses_3pct_trail,
        test_20pct_zone_uses_3pct_trail,
        test_26pct_limit_up_imminent_3pct_trail,
        test_ratchet_sl_never_decreases,
        test_high_watermark_tracking,
        test_no_old_multizone_logic_anywhere,
        test_dynamic_trailing_module_delegation,
    ]
    passed = 0
    failed = 0
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
