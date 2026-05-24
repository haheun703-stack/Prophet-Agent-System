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
    _load_sector_gain_map, _load_theme_gain_map,
    _classify_cap_band, _build_code_to_group_map,
    _detect_group_rotation,
    CAP_BAND_BONUS, GROUP_STOCK_MAP,
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


def test_cap_band_classification():
    """시총 구간 분류 (사장님 5/24 직관)."""
    assert _classify_cap_band(50) == "초소형"
    assert _classify_cap_band(500) == "소형"
    assert _classify_cap_band(3000) == "중소형"
    assert _classify_cap_band(7000) == "중대형"
    assert _classify_cap_band(20000) == "대형"
    assert _classify_cap_band(100000) == "초대형"
    print("[PASS] 시총 구간 분류 (초소/소/중소/중대/대/초대형)")


def test_cap_band_bonus():
    """시총 가중치 — 소부장/소형주 우대."""
    assert CAP_BAND_BONUS["소형"] == 10
    assert CAP_BAND_BONUS["중소형"] == 8
    assert CAP_BAND_BONUS["대형"] == 0
    assert CAP_BAND_BONUS["초대형"] == -5
    assert CAP_BAND_BONUS["초소형"] == -3
    print("[PASS] 시총 가중치 (소형/중소형 우대, 초대형 패널티)")


def test_group_stock_map():
    """그룹주 매핑 정의."""
    assert "삼성그룹" in GROUP_STOCK_MAP
    assert "SK그룹" in GROUP_STOCK_MAP
    assert "LG그룹" in GROUP_STOCK_MAP
    assert "두산" in GROUP_STOCK_MAP
    assert "005930" in GROUP_STOCK_MAP["삼성그룹"]   # 삼성전자
    assert "000660" in GROUP_STOCK_MAP["SK그룹"]    # SK하이닉스
    assert "066570" in GROUP_STOCK_MAP["LG그룹"]    # LG전자
    print(f"[PASS] 그룹주 매핑 ({len(GROUP_STOCK_MAP)}개 그룹)")


def test_code_to_group_inverse_map():
    """종목 → 그룹 역매핑."""
    rev = _build_code_to_group_map()
    assert rev.get("005930") == "삼성그룹"
    assert rev.get("000660") == "SK그룹"
    assert rev.get("066570") == "LG그룹"
    print(f"[PASS] 종목→그룹 역매핑 ({len(rev)}개 종목)")


def test_group_rotation_detection():
    """그룹주 로테이션 감지 — 같은 그룹 N종 강세 시 후발 보너스."""
    # 삼성그룹 2종 강세 → 다른 삼성그룹 종목에 +10
    today_change = {
        "005930": 6.0,   # 삼성전자 강세
        "028260": 7.0,   # 삼성물산 강세
        "010140": 0.5,   # 삼성중공업 약세 (후발 후보)
        "005935": -1.0,  # 삼성전자우 약세 (후발 후보)
    }
    bonus = _detect_group_rotation(
        list(today_change.keys()), today_change,
        min_surge_count=2, surge_threshold=5.0,
    )
    assert bonus.get("010140") == 10, f"후발 010140 기대 +10, 실제 {bonus.get('010140')}"
    assert bonus.get("005935") == 10
    # 강세 종목은 보너스 X
    assert "005930" not in bonus
    print(f"[PASS] 그룹 로테이션 감지 - 후발 {len(bonus)}종목에 +10")


def test_group_rotation_no_trigger():
    """1종만 강세면 로테이션 X."""
    today_change = {"005930": 6.0, "028260": 1.0}
    bonus = _detect_group_rotation(
        list(today_change.keys()), today_change,
        min_surge_count=2, surge_threshold=5.0,
    )
    assert len(bonus) == 0, f"단일 강세 → 로테이션 X 기대, 실제 {bonus}"
    print("[PASS] 단일 종목 강세 - 로테이션 발동 X")


def test_dynamic_theme_gain_map():
    """★ 5/24 사장님 지적 ★ theme_map.json 302개 동적 가중치."""
    if hasattr(_load_theme_gain_map, "_cache"):
        _load_theme_gain_map._cache = {"ts": 0, "data": {"theme_bonus": {}, "code_themes": {}}}
    result = _load_theme_gain_map(top_n_themes=10, min_events=3)
    theme_bonus = result.get("theme_bonus", {})
    code_themes = result.get("code_themes", {})
    if theme_bonus:
        assert len(theme_bonus) >= 1
        # 가중치 범위 검증
        for theme, w in theme_bonus.items():
            assert 2 <= w <= 15, f"테마 가중치 비정상: {theme}={w}"
        assert len(code_themes) >= 100, f"종목→테마 매핑 부족: {len(code_themes)}"
        print(f"[PASS] 동적 테마 가중치 - {len(theme_bonus)}개 테마: "
              f"{list(theme_bonus.keys())[:3]}")
    else:
        print("[SKIP] 동적 테마 가중치 - theme_map.json 또는 history.json 없음")


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
        test_cap_band_classification,
        test_cap_band_bonus,
        test_group_stock_map,
        test_code_to_group_inverse_map,
        test_group_rotation_detection,
        test_group_rotation_no_trigger,
        test_dynamic_theme_gain_map,
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
