# -*- coding: utf-8 -*-
"""
테마코드 수집기
===============
KIS 마스터 파일에서 테마코드-종목 매핑을 수집하여
ranking_scanner 교차 분석에 테마 정보를 제공한다.

데이터 소스: https://new.real.download.dws.co.kr/common/master/theme_code.mst.zip
포맷: 테마코드(3) + 테마명(가변) + 종목코드(6) + filler(3)

사용:
  python -m data.theme_collector          # 테마 마스터 다운로드 + JSON 저장
  python -m data.theme_collector --show   # 현재 테마맵 출력
"""

import json
import logging
import os
import ssl
import urllib.request
import zipfile
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional

logger = logging.getLogger("BH.Theme")

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data_store"
THEME_MAP_PATH = DATA_DIR / "theme_map.json"

_THEME_MST_URL = "https://new.real.download.dws.co.kr/common/master/theme_code.mst.zip"


def download_theme_master() -> Dict:
    """KIS 테마코드 마스터 다운로드 → theme_map.json 저장.

    Returns:
        {
            "themes": {
                "001": {"name": "초전도체", "codes": ["005930", "000660", ...]},
                ...
            },
            "code_to_themes": {
                "005930": [{"theme_code": "001", "theme_name": "초전도체"}, ...],
                ...
            },
            "theme_count": 200,
            "stock_count": 1500,
            "updated": "2026-04-14 09:00"
        }
    """
    import tempfile
    from datetime import datetime

    tmp_dir = tempfile.mkdtemp()
    zip_path = os.path.join(tmp_dir, "theme_code.zip")
    mst_path = os.path.join(tmp_dir, "theme_code.mst")

    try:
        # SSL 우회 + 다운로드 (로컬 컨텍스트만 — 글로벌 영향 없음)
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        req = urllib.request.Request(_THEME_MST_URL)
        with urllib.request.urlopen(req, context=ctx) as resp:
            with open(zip_path, "wb") as fp:
                fp.write(resp.read())
        logger.info(f"[THEME] 마스터 파일 다운로드 완료: {zip_path}")

        # ZIP 해제
        with zipfile.ZipFile(zip_path) as zf:
            zf.extractall(tmp_dir)

        # 파싱 (cp949 인코딩)
        themes = {}  # theme_code → {name, codes}
        code_to_themes = defaultdict(list)  # stock_code → [{theme_code, theme_name}]

        with open(mst_path, mode="r", encoding="cp949") as f:
            for row in f:
                row = row.rstrip("\n").rstrip("\r")
                if len(row) < 10:
                    continue
                theme_code = row[0:3].strip()
                stock_code = row[-9:].strip()[:6]  # 마지막 9자리에서 종목코드 6자리
                theme_name = row[3:-9].strip()

                if not theme_code or not stock_code:
                    continue

                if theme_code not in themes:
                    themes[theme_code] = {"name": theme_name, "codes": []}

                if stock_code not in themes[theme_code]["codes"]:
                    themes[theme_code]["codes"].append(stock_code)

                code_to_themes[stock_code].append({
                    "theme_code": theme_code,
                    "theme_name": theme_name,
                })

        # 중복 제거
        for code in code_to_themes:
            seen = set()
            unique = []
            for t in code_to_themes[code]:
                key = t["theme_code"]
                if key not in seen:
                    seen.add(key)
                    unique.append(t)
            code_to_themes[code] = unique

        result = {
            "themes": themes,
            "code_to_themes": dict(code_to_themes),
            "theme_count": len(themes),
            "stock_count": len(code_to_themes),
            "updated": datetime.now().strftime("%Y-%m-%d %H:%M"),
        }

        # 저장
        THEME_MAP_PATH.write_text(
            json.dumps(result, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        logger.info(
            f"[THEME] 테마 {len(themes)}개, 종목 {len(code_to_themes)}개 → {THEME_MAP_PATH}"
        )
        return result

    except Exception as e:
        logger.error(f"[THEME] 마스터 다운로드 실패: {e}")
        return {}
    finally:
        # 임시 파일 정리
        for f in [zip_path, mst_path]:
            try:
                os.remove(f)
            except OSError:
                pass
        try:
            os.rmdir(tmp_dir)
        except OSError:
            pass


def load_theme_map() -> Dict:
    """로컬 theme_map.json 로드. 없으면 빈 dict."""
    if THEME_MAP_PATH.exists():
        try:
            return json.loads(THEME_MAP_PATH.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {}


def get_themes_for_code(code: str, theme_map: Optional[Dict] = None) -> List[str]:
    """특정 종목의 테마 이름 리스트 반환.

    >>> get_themes_for_code("005930")
    ["반도체", "삼성그룹", "AI반도체"]
    """
    if theme_map is None:
        theme_map = load_theme_map()
    c2t = theme_map.get("code_to_themes", {})
    entries = c2t.get(code, [])
    return [e["theme_name"] for e in entries]


def detect_theme_surge(surge_codes: List[str], theme_map: Optional[Dict] = None,
                       min_count: int = 2) -> List[Dict]:
    """급등 종목 리스트에서 동일 테마 동시 급등 감지.

    Args:
        surge_codes: 급등 종목 코드 리스트 (등락률 5%+)
        theme_map: 테마맵 (없으면 자동 로드)
        min_count: 최소 동시 급등 종목수 (기본 2)

    Returns:
        [
            {"theme": "2차전지", "theme_code": "045", "count": 4,
             "codes": ["005930", "000660", ...], "strength": "강"},
            ...
        ]
    """
    if theme_map is None:
        theme_map = load_theme_map()

    if not theme_map:
        return []

    c2t = theme_map.get("code_to_themes", {})
    themes_data = theme_map.get("themes", {})

    # 테마별 급등 종목 카운트
    theme_hits = defaultdict(list)  # theme_code → [급등 종목코드]
    for code in surge_codes:
        for entry in c2t.get(code, []):
            theme_hits[entry["theme_code"]].append(code)

    # min_count 이상인 테마만 추출
    results = []
    for tc, codes in theme_hits.items():
        if len(codes) < min_count:
            continue

        theme_info = themes_data.get(tc, {})
        theme_name = theme_info.get("name", tc)
        total_in_theme = len(theme_info.get("codes", []))

        # 강도 판정: 테마 내 급등 비율
        ratio = len(codes) / max(total_in_theme, 1)
        if ratio >= 0.3 or len(codes) >= 5:
            strength = "폭발"
        elif ratio >= 0.15 or len(codes) >= 3:
            strength = "강"
        else:
            strength = "보통"

        results.append({
            "theme": theme_name,
            "theme_code": tc,
            "count": len(codes),
            "total": total_in_theme,
            "codes": codes,
            "strength": strength,
        })

    results.sort(key=lambda x: x["count"], reverse=True)
    return results


# ═══════════════════════════════════════
#  CLI
# ═══════════════════════════════════════

if __name__ == "__main__":
    import argparse
    import sys

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    parser = argparse.ArgumentParser(description="테마코드 수집기")
    parser.add_argument("--show", action="store_true", help="현재 테마맵 요약 출력")
    parser.add_argument("--code", type=str, help="특정 종목의 테마 조회")
    args = parser.parse_args()

    if args.show:
        tm = load_theme_map()
        if not tm:
            print("테마맵 없음. 먼저 다운로드하세요.")
            sys.exit(1)
        print(f"테마 {tm['theme_count']}개 / 종목 {tm['stock_count']}개 / 갱신 {tm.get('updated', '?')}")
        # 종목수 많은 테마 TOP 10
        themes = tm.get("themes", {})
        top = sorted(themes.items(), key=lambda x: len(x[1]["codes"]), reverse=True)[:10]
        for tc, info in top:
            print(f"  [{tc}] {info['name']}: {len(info['codes'])}종목")

    elif args.code:
        tm = load_theme_map()
        names = get_themes_for_code(args.code, tm)
        print(f"{args.code} → {', '.join(names) if names else '테마 없음'}")

    else:
        result = download_theme_master()
        if result:
            print(f"완료: 테마 {result['theme_count']}개 / 종목 {result['stock_count']}개")
        else:
            print("실패")
            sys.exit(1)
