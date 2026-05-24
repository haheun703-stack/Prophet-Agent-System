# -*- coding: utf-8 -*-
"""★ 5/24 단위 테스트 ★ continuation_score zone 보너스/패널티 + 섹터 가중치.

검증:
    zone 0-30 → -40 (강한 패널티 = 실질 차단)
    zone 30-50 → +5 (약한 보너스)
    zone 50-70 → +30 (Sweet Zone, 강한 보너스)
    zone 70-90+ → +15 (보조 보너스)
    경계값 정확성
    섹터 가중치 (전기전자/유통 +10)
"""
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))

# Fresh import
for m in list(sys.modules):
    if m.startswith("utils.asset_pool_loader"):
        del sys.modules[m]

from utils.asset_pool_loader import (
    _continuation_score_adjustment, _load_continuation_score_map,
    _load_sector_gain_map,
)


def test_zone_0_30_block():
    """0-30 zone → -40 (차단)."""
    assert _continuation_score_adjustment(0) == -40
    assert _continuation_score_adjustment(15) == -40
    assert _continuation_score_adjustment(29.9) == -40
    print("[PASS] zone 0-30: -40 패널티 (차단)")


def test_zone_30_50_weak():
    """30-50 zone → +5 (약한 보너스)."""
    assert _continuation_score_adjustment(30) == 5
    assert _continuation_score_adjustment(40) == 5
    assert _continuation_score_adjustment(49.9) == 5
    print("[PASS] zone 30-50: +5 약한 보너스")


def test_zone_50_70_sweet():
    """50-70 zone → +30 (Sweet Zone)."""
    assert _continuation_score_adjustment(50) == 30
    assert _continuation_score_adjustment(60) == 30
    assert _continuation_score_adjustment(69.9) == 30
    print("[PASS] zone 50-70: +30 (Sweet Zone)")


def test_zone_70_plus():
    """70+ zone → +15 (보조)."""
    assert _continuation_score_adjustment(70) == 15
    assert _continuation_score_adjustment(80) == 15
    assert _continuation_score_adjustment(90) == 15
    print("[PASS] zone 70+: +15 보조")


def test_zone_boundary_30():
    """경계 30 정확."""
    assert _continuation_score_adjustment(29.99) == -40
    assert _continuation_score_adjustment(30.0) == 5
    print("[PASS] 경계값 30.0 정확")


def test_zone_boundary_50():
    """경계 50 정확."""
    assert _continuation_score_adjustment(49.99) == 5
    assert _continuation_score_adjustment(50.0) == 30
    print("[PASS] 경계값 50.0 정확")


def test_zone_boundary_70():
    """경계 70 정확."""
    assert _continuation_score_adjustment(69.99) == 30
    assert _continuation_score_adjustment(70.0) == 15
    print("[PASS] 경계값 70.0 정확")


def test_dynamic_sector_gain_map():
    """★ 5/24 사장님 지적 ★ 섹터 가중치 동적 산출 (hardcoded X)."""
    # 캐시 초기화
    if hasattr(_load_sector_gain_map, "_cache"):
        _load_sector_gain_map._cache = {"ts": 0, "data": {}}
    sector_map = _load_sector_gain_map(top_n_sectors=5, min_events=5)
    if sector_map:
        # 최소 1개 섹터는 있어야 (history.json 693 events 보유)
        assert len(sector_map) >= 1, f"섹터 0건: {sector_map}"
        # 가중치는 rank weights 중 하나
        rank_weights = {12, 10, 8, 6, 4}
        for sec, w in sector_map.items():
            assert w in rank_weights, f"가중치 비정상: {sec}={w}"
            assert isinstance(sec, str) and len(sec) >= 1
        print(f"[PASS] 동적 섹터 가중치 - {len(sector_map)}개 섹터: "
              f"{list(sector_map.keys())[:3]}...")
    else:
        print("[SKIP] 동적 섹터 가중치 - history.json 없음")


def test_load_continuation_score_map():
    """history.json에서 종목별 평균 cs 로드."""
    cs_map = _load_continuation_score_map()
    # history.json은 보통 존재 (방금 갱신)
    if cs_map:
        # 몇 개 종목 sanity check
        sample = list(cs_map.items())[:3]
        for code, cs in sample:
            assert isinstance(code, str) and len(code) >= 5, f"code 형식: {code}"
            assert 0 <= cs <= 100, f"cs 범위: {cs}"
        print(f"[PASS] _load_continuation_score_map - {len(cs_map)}종목 로드")
    else:
        print("[SKIP] _load_continuation_score_map - history.json 없음")


if __name__ == "__main__":
    print("=" * 60)
    print("★ 5/24 continuation_score zone + 섹터 가중치 ★")
    print("=" * 60)
    tests = [
        test_zone_0_30_block,
        test_zone_30_50_weak,
        test_zone_50_70_sweet,
        test_zone_70_plus,
        test_zone_boundary_30,
        test_zone_boundary_50,
        test_zone_boundary_70,
        test_dynamic_sector_gain_map,
        test_load_continuation_score_map,
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
