# -*- coding: utf-8 -*-
"""
Shared Theme Service
====================
프로젝트 전역 테마 분류 서비스.
KIS 테마코드 마스터(302개 테마) 기반 종목 -> 테마 매핑.

모든 봇(스캘퍼, 프로펫, 블로그, 정보)이 동일한 테마 분류체계를 사용하도록
중앙화된 API를 제공한다.

데이터 소스: scalper-agent/data_store/theme_map.json
갱신: scalper-agent/data/theme_collector.py (KIS 마스터 다운로드)

Usage:
    from shared.theme_service import get_themes, group_by_theme

    # 종목의 테마 조회
    get_themes("005930")   # ["3D낸드", "CXL", "HBM", ...]

    # 종목 리스트를 테마별 그룹핑
    group_by_theme(["005930", "000660", "006400"])
    # [{"theme": "HBM", "codes": ["005930","000660"], "count": 2, ...}]
"""

import json
import logging
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_THEME_MAP_PATH = _PROJECT_ROOT / "scalper-agent" / "data_store" / "theme_map.json"

# ── 싱글턴 캐시 ──
_theme_map: Optional[Dict] = None
_c2t: Optional[Dict] = None          # code_to_themes
_themes_data: Optional[Dict] = None  # themes

# 세트매매와 무관한 노이즈 테마 (연도별 상장, 정책 테마 등)
NOISE_THEMES = ("신규 상장", "상장주", "FTA", "뉴딜", "배당")


def _load():
    """테마맵 로드 + 캐시."""
    global _theme_map, _c2t, _themes_data
    if _theme_map is not None:
        return

    if not _THEME_MAP_PATH.exists():
        logger.warning(f"theme_map.json 없음: {_THEME_MAP_PATH}")
        _theme_map, _c2t, _themes_data = {}, {}, {}
        return

    try:
        _theme_map = json.loads(_THEME_MAP_PATH.read_text(encoding="utf-8"))
        _c2t = _theme_map.get("code_to_themes", {})
        _themes_data = _theme_map.get("themes", {})
        logger.info(
            f"[ThemeService] {len(_themes_data)}개 테마, "
            f"{len(_c2t)}개 종목 로드"
        )
    except Exception as e:
        logger.error(f"[ThemeService] theme_map.json 로드 실패: {e}")
        _theme_map, _c2t, _themes_data = {}, {}, {}


def reload():
    """캐시 무효화 후 재로드 (마스터 갱신 후 호출)."""
    global _theme_map, _c2t, _themes_data
    _theme_map = None
    _c2t = None
    _themes_data = None
    _load()


# ═══════════════════════════════════════
#  종목 → 테마 조회
# ═══════════════════════════════════════

def get_themes(code: str) -> List[str]:
    """종목코드 -> 테마 이름 리스트.

    >>> get_themes("005930")
    ["3D낸드", "CXL", "HBM", ...]
    """
    _load()
    return [e["theme_name"] for e in _c2t.get(code, [])]


def get_theme_entries(code: str) -> List[dict]:
    """종목코드 -> [{theme_code, theme_name}] 전체 엔트리."""
    _load()
    return _c2t.get(code, [])


# ═══════════════════════════════════════
#  테마 → 종목 조회
# ═══════════════════════════════════════

def get_codes(theme_name: str) -> List[str]:
    """테마 이름 -> 소속 종목코드 리스트."""
    _load()
    for td in _themes_data.values():
        if td.get("name") == theme_name:
            return td.get("codes", [])
    return []


def get_theme_size(theme_name: str) -> int:
    """테마의 전체 종목수."""
    _load()
    for td in _themes_data.values():
        if td.get("name") == theme_name:
            return len(td.get("codes", []))
    return 0


# ═══════════════════════════════════════
#  필터 / 유틸리티
# ═══════════════════════════════════════

def is_noise_theme(theme_name: str) -> bool:
    """세트매매와 무관한 노이즈 테마인지 판별."""
    return any(p in theme_name for p in NOISE_THEMES)


def get_all_themes() -> Dict[str, dict]:
    """전체 테마 데이터 반환 {theme_code: {name, codes}}."""
    _load()
    return dict(_themes_data) if _themes_data else {}


def get_raw_map() -> Dict:
    """원본 theme_map 전체 반환 (하위호환용)."""
    _load()
    return dict(_theme_map) if _theme_map else {}


# ═══════════════════════════════════════
#  그룹핑 API (핵심)
# ═══════════════════════════════════════

def group_by_theme(
    codes: List[str],
    min_count: int = 2,
    filter_noise: bool = True,
    max_themes: int = 0,
) -> List[dict]:
    """종목 리스트를 테마별로 그룹핑.

    Args:
        codes: 종목코드 리스트
        min_count: 최소 종목수 (기본 2)
        filter_noise: 노이즈 테마 제외 여부
        max_themes: 최대 반환 테마수 (0=전체)

    Returns:
        [{"theme": str, "codes": [str], "count": int,
          "total_in_theme": int, "density": float}]
        density = 매칭종목수 / 테마전체종목수
    """
    _load()

    theme_codes = defaultdict(list)
    for code in codes:
        for entry in _c2t.get(code, []):
            name = entry["theme_name"]
            if filter_noise and is_noise_theme(name):
                continue
            if code not in theme_codes[name]:
                theme_codes[name].append(code)

    results = []
    for theme_name, matched_codes in theme_codes.items():
        if len(matched_codes) < min_count:
            continue

        total = get_theme_size(theme_name)
        density = len(matched_codes) / max(total, 1)

        results.append({
            "theme": theme_name,
            "codes": matched_codes,
            "count": len(matched_codes),
            "total_in_theme": total,
            "density": round(density, 4),
        })

    results.sort(key=lambda x: (-x["count"], -x["density"]))

    if max_themes > 0:
        results = results[:max_themes]

    return results
