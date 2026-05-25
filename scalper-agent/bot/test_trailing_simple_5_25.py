# -*- coding: utf-8 -*-
"""★ 5/25 단위 테스트 ★ 사장님 룰 1 (고점 -3% 일관) 검증.

5/22 13-B 동적 트레일링 (-3/-5/-7/-10) 폐기 후 단순화 검증:
  - 모든 zone trail_pct = 3.0 일관
  - activation_pct = 3.0 일관
  - +25%+ 상한가는 limit_up_split_sell로 분리 (reason만 다름)
"""
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))

from bot.dynamic_trailing import (
    decide_trailing, classify_trend_strength, compute_trailing_sl, should_activate_trailing,
)


def test_weak_zone_3pct():
    """+3~5% (weak) → 고점 -3% 일관."""
    d = decide_trailing(pnl_pct=4.0)
    assert d.trail_pct == 3.0, f"trail_pct=3.0 기대, 실제 {d.trail_pct}"
    assert d.activation_pct == 3.0, f"activation_pct=3.0 기대, 실제 {d.activation_pct}"
    assert d.strength == "weak", f"strength=weak 기대, 실제 {d.strength}"
    print("[PASS] +4% (weak) → 고점 -3% 일관")


def test_moderate_zone_3pct():
    """+5~10% (moderate) → 고점 -3% 일관 (사장님 룰)."""
    d = decide_trailing(pnl_pct=7.0)
    assert d.trail_pct == 3.0, f"trail_pct=3.0 기대 (이전 -5%), 실제 {d.trail_pct}"
    assert d.strength == "moderate"
    print("[PASS] +7% (moderate) → 고점 -3% (이전 -5% 폐기)")


def test_strong_zone_3pct():
    """+10%+ (strong) → 고점 -3% 일관 (5/22 13-B -7% 폐기)."""
    d = decide_trailing(pnl_pct=12.0)
    assert d.trail_pct == 3.0, f"trail_pct=3.0 기대 (이전 -7%), 실제 {d.trail_pct}"
    assert d.strength == "strong"
    print("[PASS] +12% (strong) → 고점 -3% (이전 -7% 폐기)")


def test_limit_up_zone_3pct():
    """+25%+ (limit_up_imminent) → 고점 -3% (limit_up_split_sell로 별도 처리)."""
    d = decide_trailing(pnl_pct=26.0)
    assert d.trail_pct == 3.0, f"trail_pct=3.0 기대 (이전 -10%), 실제 {d.trail_pct}"
    assert d.strength == "limit_up_imminent"
    assert "limit_up_split_sell" in d.reason, "상한가는 limit_up_split_sell로 분리 안내"
    print("[PASS] +26% (limit_up) → trail -3% + reason 'limit_up_split_sell 처리'")


def test_compute_sl_consistency():
    """compute_trailing_sl: 고점 100,000 / -3% → SL 97,000."""
    d = decide_trailing(pnl_pct=10.0)
    sl = compute_trailing_sl(100000, d)
    assert sl == 97000, f"SL=97000 기대, 실제 {sl}"
    print("[PASS] 고점 100,000 → SL 97,000 (정확 -3%)")


def test_compute_sl_high_zone():
    """+25% 도달 후 고점 130,000 / -3% → SL 126,100 (이전 13-E -12%이면 SL 114,400)."""
    d = decide_trailing(pnl_pct=25.0)  # limit_up_imminent
    sl = compute_trailing_sl(130000, d)
    assert sl == 126100, f"SL=126,100 기대 (이전 -12%면 114,400), 실제 {sl}"
    print("[PASS] +25% 고점 130,000 → SL 126,100 (-3% 일관 / 이전 -12% 폐기)")


def test_activation_below_3pct():
    """+2% (활성 미달) → 트레일링 비활성."""
    d = decide_trailing(pnl_pct=2.0)
    active = should_activate_trailing(2.0, d)
    assert not active, "+2% < 3% → 비활성"
    print("[PASS] +2% → 트레일링 비활성")


def test_activation_at_3pct():
    """+3% 정확 → 트레일링 활성."""
    d = decide_trailing(pnl_pct=3.0)
    active = should_activate_trailing(3.0, d)
    assert active, "+3% >= 3% → 활성"
    print("[PASS] +3% → 트레일링 활성 (경계 정확)")


def test_disable_fixed_tp():
    """모든 zone disable_fixed_tp=True (사장님 5/21 영구 룰)."""
    for pnl in [3, 7, 12, 18, 25, 29]:
        d = decide_trailing(pnl_pct=pnl)
        assert d.disable_fixed_tp, f"+{pnl}% disable_fixed_tp=True 기대"
    print("[PASS] 모든 zone disable_fixed_tp=True (고정 TP 폐기)")


def test_no_dynamic_widening():
    """★ 핵심 회귀 테스트 ★ 모든 zone trail_pct = 3.0 동일 (이전 -3/-5/-7/-10 폐기 검증)."""
    pnls = [3.0, 5.0, 7.0, 10.0, 12.0, 15.0, 20.0, 25.0, 29.0]
    trails = [decide_trailing(pnl_pct=p).trail_pct for p in pnls]
    assert all(t == 3.0 for t in trails), f"모든 trail 3.0 기대, 실제 {trails}"
    print(f"[PASS] 9개 zone 모두 trail=-3.0% 일관 (이전 동적 폭 완전 폐기)")


if __name__ == "__main__":
    print("=" * 60)
    print("★ 5/25 사장님 룰 1 — 고점 -3% 일관 트레일링 검증 ★")
    print("=" * 60)
    tests = [
        test_weak_zone_3pct,
        test_moderate_zone_3pct,
        test_strong_zone_3pct,
        test_limit_up_zone_3pct,
        test_compute_sl_consistency,
        test_compute_sl_high_zone,
        test_activation_below_3pct,
        test_activation_at_3pct,
        test_disable_fixed_tp,
        test_no_dynamic_widening,
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
    print("=" * 60)
    print(f"결과: {passed}/{len(tests)} PASS, {failed} FAIL")
    sys.exit(0 if failed == 0 else 1)
