# -*- coding: utf-8 -*-
"""★ 5/24 사장님 13-C 단위 테스트 ★ 추세 강도 점수 + 등급 매핑 + 트레일링 보정.

검증:
    - 등급 매핑 정확 (WEAK<3 / MID 3~5 / STRONG 6~7 / EXPLOSIVE 8+)
    - 헬퍼 함수 (is_strong_trend, is_weak_trend, is_genuine_weakening)
    - 13-B 트레일링 폭 보정 (STRONG → upgrade, WEAK → downgrade)
"""
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))

# 모듈 fresh import
for m in list(sys.modules):
    if m.startswith("bot.trend_strength"):
        del sys.modules[m]

from bot.trend_strength import (
    _grade_from_score, is_strong_trend, is_weak_trend, is_genuine_weakening,
)


# ── 등급 매핑 ──
def test_grade_explosive():
    assert _grade_from_score(8) == "EXPLOSIVE"
    assert _grade_from_score(10) == "EXPLOSIVE"
    print("[PASS] grade EXPLOSIVE (>=8)")


def test_grade_strong():
    assert _grade_from_score(6) == "STRONG"
    assert _grade_from_score(7) == "STRONG"
    assert _grade_from_score(7.9) == "STRONG"
    print("[PASS] grade STRONG (6~7.9)")


def test_grade_mid():
    assert _grade_from_score(3) == "MID"
    assert _grade_from_score(5) == "MID"
    assert _grade_from_score(5.9) == "MID"
    print("[PASS] grade MID (3~5.9)")


def test_grade_weak():
    assert _grade_from_score(0) == "WEAK"
    assert _grade_from_score(2.9) == "WEAK"
    print("[PASS] grade WEAK (<3)")


# ── 헬퍼 ──
def test_is_strong_trend():
    assert is_strong_trend({"grade": "STRONG"}) is True
    assert is_strong_trend({"grade": "EXPLOSIVE"}) is True
    assert is_strong_trend({"grade": "MID"}) is False
    assert is_strong_trend({"grade": "WEAK"}) is False
    print("[PASS] is_strong_trend (STRONG/EXPLOSIVE만 True)")


def test_is_weak_trend():
    assert is_weak_trend({"grade": "WEAK"}) is True
    assert is_weak_trend({"grade": "MID"}) is False
    print("[PASS] is_weak_trend (WEAK만 True)")


def test_genuine_weakening_all_negative():
    """VWAP 하단 + 거래량 감소 + 체결강도<100 → 진짜 약화."""
    s = {"factors": {"vwap_diff_pct": -1.0, "volume_ratio": 0.7, "flow_intensity": 80}}
    assert is_genuine_weakening(s) is True
    print("[PASS] genuine_weakening 3 negatives")


def test_genuine_weakening_partial():
    """3개 중 일부만 → 일시 조정."""
    s1 = {"factors": {"vwap_diff_pct": -1.0, "volume_ratio": 0.7, "flow_intensity": 110}}
    s2 = {"factors": {"vwap_diff_pct": -1.0, "volume_ratio": 1.5, "flow_intensity": 80}}
    s3 = {"factors": {"vwap_diff_pct": 2.0, "volume_ratio": 0.5, "flow_intensity": 80}}
    assert is_genuine_weakening(s1) is False
    assert is_genuine_weakening(s2) is False
    assert is_genuine_weakening(s3) is False
    print("[PASS] genuine_weakening rejected (only some negatives)")


# ── 13-B 트레일링 보정 로직 (헬퍼와 동일 logic mirror) ──
def _compute_trail_pct_with_strength(hwm_pct: float, is_momentum: bool,
                                       grade: str) -> float:
    """auto_trader._compute_dynamic_trailing_sl 보정 로직 mirror."""
    if hwm_pct >= 20.0:
        trail_pct = 0.10
    elif hwm_pct >= 10.0:
        trail_pct = 0.07
    elif hwm_pct >= 5.0:
        trail_pct = 0.05
    else:
        trail_pct = 0.02 if is_momentum else 0.03

    UPGRADE = {0.02: 0.03, 0.03: 0.05, 0.05: 0.07, 0.07: 0.10, 0.10: 0.10}
    DOWNGRADE = {0.10: 0.07, 0.07: 0.05, 0.05: 0.03, 0.03: 0.03, 0.02: 0.02}
    if grade in ("STRONG", "EXPLOSIVE"):
        trail_pct = UPGRADE.get(trail_pct, trail_pct)
    elif grade == "WEAK":
        trail_pct = DOWNGRADE.get(trail_pct, trail_pct)
    return trail_pct


def test_strong_trend_upgrades_pct():
    """STRONG 추세 → 폭 한 단계 위 (조정 인내)."""
    # +4% 약한 (기본 -3%) + STRONG → -5%
    assert _compute_trail_pct_with_strength(4.0, False, "STRONG") == 0.05
    # +7% 중간 (기본 -5%) + STRONG → -7%
    assert _compute_trail_pct_with_strength(7.0, False, "STRONG") == 0.07
    # +15% 강한 (기본 -7%) + STRONG → -10%
    assert _compute_trail_pct_with_strength(15.0, False, "STRONG") == 0.10
    # +25% 상한가 (기본 -10%) + EXPLOSIVE → -10% (cap)
    assert _compute_trail_pct_with_strength(25.0, False, "EXPLOSIVE") == 0.10
    print("[PASS] STRONG 보정 - 한 단계 위 (조정 인내)")


def test_weak_trend_downgrades_pct():
    """WEAK 추세 → 폭 한 단계 아래 (빠른 익절)."""
    # +15% 강한 (기본 -7%) + WEAK → -5%
    assert _compute_trail_pct_with_strength(15.0, False, "WEAK") == 0.05
    # +7% 중간 (기본 -5%) + WEAK → -3%
    assert _compute_trail_pct_with_strength(7.0, False, "WEAK") == 0.03
    # +4% 약한 (기본 -3%) + WEAK → -3% (floor)
    assert _compute_trail_pct_with_strength(4.0, False, "WEAK") == 0.03
    print("[PASS] WEAK 보정 - 한 단계 아래 (빠른 익절)")


def test_mid_trend_no_change():
    """MID/UNKNOWN → 기본 폭 유지."""
    assert _compute_trail_pct_with_strength(15.0, False, "MID") == 0.07
    assert _compute_trail_pct_with_strength(7.0, False, "UNKNOWN") == 0.05
    print("[PASS] MID/UNKNOWN 보정 없음")


if __name__ == "__main__":
    print("=" * 60)
    print("★ 5/24 13-C 추세 강도 + 보정 단위 테스트 ★")
    print("=" * 60)
    tests = [
        test_grade_explosive, test_grade_strong, test_grade_mid, test_grade_weak,
        test_is_strong_trend, test_is_weak_trend,
        test_genuine_weakening_all_negative, test_genuine_weakening_partial,
        test_strong_trend_upgrades_pct, test_weak_trend_downgrades_pct,
        test_mid_trend_no_change,
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
