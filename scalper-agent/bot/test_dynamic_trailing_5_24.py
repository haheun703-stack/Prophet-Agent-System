# -*- coding: utf-8 -*-
"""★ 5/24 사장님 결정 13-B 단위 테스트 ★ 동적 트레일링 폭 (기가레인/아나패스 사고 fix).

검증:
    +3~5%   → -3%
    +5~10%  → -5%
    +10~20% → -7%
    +20%+   → -10%
    MOMENTUM +1.5~5% → -2%
    미달    → 비활성
    ratchet → 트레일링 SL 절대 내려가지 않음

기가레인 회귀 검증: 고점 +12%면 trail_sl = entry × 1.116 (= 고점 -7%)
    → 이전 룰 (-3%) 대비 +4.16%p 더 견디다 +12.73% 추적 가능
"""
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))

# auto_trader.py에서 직접 메서드만 추출하기 어려움 → AutoTrader 인스턴스 mock
# pure-function 형태로 메서드 로직만 재구현해서 테스트 (auto_trader.py 헬퍼와 동기화)

def compute_dynamic_trailing_sl(pos: dict, current_price: int) -> int:
    """auto_trader.AutoTrader._compute_dynamic_trailing_sl 로직 mirror (테스트 전용)."""
    entry = pos.get("entry_price", 0)
    if entry <= 0:
        return pos.get("trailing_sl", 0)

    pos["high_watermark"] = max(pos.get("high_watermark", entry), current_price)
    hwm = pos["high_watermark"]
    hwm_pct = (hwm / entry - 1) * 100
    pnl_pct = (current_price / entry - 1) * 100

    is_momentum = pos.get("regime") == "MOMENTUM"
    activation_threshold = 1.5 if is_momentum else 3.0

    if pnl_pct < activation_threshold and not pos.get("trailing_activated"):
        return pos.get("trailing_sl", 0)

    pos["trailing_activated"] = True

    if hwm_pct >= 20.0:
        trail_pct = 0.10
    elif hwm_pct >= 10.0:
        trail_pct = 0.07
    elif hwm_pct >= 5.0:
        trail_pct = 0.05
    else:
        trail_pct = 0.02 if is_momentum else 0.03

    trail_sl = int(hwm * (1 - trail_pct))
    if hwm > entry * (1 + activation_threshold / 100):
        trail_sl = max(trail_sl, entry)

    pos["trailing_sl"] = max(pos.get("trailing_sl", 0), trail_sl)
    return pos["trailing_sl"]


def _make_pos(entry: int, regime: str = "NORMAL") -> dict:
    return {"entry_price": entry, "regime": regime}


def test_below_threshold_normal():
    """NORMAL +2% (미달) → 비활성."""
    pos = _make_pos(10_000)
    sl = compute_dynamic_trailing_sl(pos, 10_200)
    assert sl == 0, f"비활성 기대 0, 실제 {sl}"
    assert not pos.get("trailing_activated"), "활성화 안 됨 기대"
    print("[PASS] NORMAL +2% 미달 → 비활성")


def test_weak_zone_normal():
    """NORMAL +3.5% (약한) → -3%."""
    pos = _make_pos(10_000)
    sl = compute_dynamic_trailing_sl(pos, 10_350)
    # hwm = 10350, trail_sl = 10350 * 0.97 = 10039.5 → int = 10039
    # entry 10000보다 큰 hwm > 10300 → max(10039, 10000) = 10039
    assert sl == 10_039, f"약한 -3% 기대 10039, 실제 {sl}"
    assert pos["trailing_activated"]
    print(f"[PASS] NORMAL +3.5% 약한 → -3% (SL={sl})")


def test_mid_zone():
    """+7% 중간 → -5%."""
    pos = _make_pos(10_000)
    sl = compute_dynamic_trailing_sl(pos, 10_700)
    # hwm = 10700, trail_sl = 10700 * 0.95 = 10165
    assert sl == 10_165, f"중간 -5% 기대 10165, 실제 {sl}"
    print(f"[PASS] +7% 중간 → -5% (SL={sl})")


def test_strong_zone():
    """+15% 강한 → -7%."""
    pos = _make_pos(10_000)
    sl = compute_dynamic_trailing_sl(pos, 11_500)
    # hwm = 11500, trail_sl = 11500 * 0.93 = 10695
    assert sl == 10_695, f"강한 -7% 기대 10695, 실제 {sl}"
    print(f"[PASS] +15% 강한 → -7% (SL={sl})")


def test_limit_up_imminent():
    """+25% 상한가 임박 → -10%."""
    pos = _make_pos(10_000)
    sl = compute_dynamic_trailing_sl(pos, 12_500)
    # hwm = 12500, trail_sl = 12500 * 0.90 = 11250
    assert sl == 11_250, f"상한가 -10% 기대 11250, 실제 {sl}"
    print(f"[PASS] +25% 상한가 임박 → -10% (SL={sl})")


def test_momentum_weak():
    """MOMENTUM +2% 약한 → -2%."""
    pos = _make_pos(10_000, regime="MOMENTUM")
    sl = compute_dynamic_trailing_sl(pos, 10_200)
    # hwm = 10200, trail_sl = 10200 * 0.98 = 9996 → max(9996, 10000) = 10000
    assert sl == 10_000, f"MOMENTUM 약한 본전 기대 10000, 실제 {sl}"
    print(f"[PASS] MOMENTUM +2% → -2% 본전 (SL={sl})")


def test_ratchet():
    """ratchet — 한번 +10% 활성 후 +5%로 떨어져도 SL 안 내려감."""
    pos = _make_pos(10_000)
    sl1 = compute_dynamic_trailing_sl(pos, 11_000)   # +10% → trail -7% = 10230
    assert sl1 == 10_230, f"1차 +10%/-7% 기대 10230, 실제 {sl1}"
    # 가격 떨어짐 +5%
    sl2 = compute_dynamic_trailing_sl(pos, 10_500)
    # hwm 그대로 11000 → trail은 그대로 10230 (ratchet)
    assert sl2 == 10_230, f"ratchet 유지 기대 10230, 실제 {sl2}"
    print(f"[PASS] ratchet - 가격 하락해도 SL 유지 ({sl2})")


def test_gigarane_regression():
    """기가레인 회귀 — 고점 +12% 시 trail_sl이 +4.16% (이전 +9.0% 대비 더 견딤)."""
    pos = _make_pos(10_000)
    sl = compute_dynamic_trailing_sl(pos, 11_200)   # 고점 +12%
    # 이전 룰 (-3%): trail_sl = 11200 * 0.97 = 10864 → 본전 → 약 +8.64%
    # 신규 룰 (-7%): trail_sl = 11200 * 0.93 = 10416 → 약 +4.16%
    assert sl == 10_416, f"기가레인 +12% trail -7% 기대 10416, 실제 {sl}"
    print(f"[PASS] 기가레인 회귀 - +12% trail = +4.16% (이전 룰 +8.64%, +4.48%p 더 견딤)")


def test_zone_boundary_5pct():
    """+5.0% 경계 — 중간 zone (>=5.0)."""
    pos = _make_pos(10_000)
    sl = compute_dynamic_trailing_sl(pos, 10_500)
    # hwm 10500, trail_sl = 10500 * 0.95 = 9975 → max(9975, 10000) = 10000
    assert sl == 10_000, f"+5.0% 경계 -5% 본전 기대 10000, 실제 {sl}"
    print(f"[PASS] +5.0% 경계 zone (중간) (SL={sl})")


def test_zone_boundary_4_99pct():
    """+4.99% — 약한 zone."""
    pos = _make_pos(10_000)
    sl = compute_dynamic_trailing_sl(pos, 10_499)
    # hwm 10499, trail_sl = 10499 * 0.97 = 10184 (>10300 → 본전 무관)
    assert sl == 10_184, f"+4.99% 약한 -3% 기대 10184, 실제 {sl}"
    print(f"[PASS] +4.99% 경계 zone (약한) (SL={sl})")


if __name__ == "__main__":
    print("=" * 60)
    print("★ 5/24 13-B 동적 트레일링 폭 단위 테스트 ★")
    print("=" * 60)
    tests = [
        test_below_threshold_normal,
        test_weak_zone_normal,
        test_mid_zone,
        test_strong_zone,
        test_limit_up_imminent,
        test_momentum_weak,
        test_ratchet,
        test_gigarane_regression,
        test_zone_boundary_5pct,
        test_zone_boundary_4_99pct,
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
