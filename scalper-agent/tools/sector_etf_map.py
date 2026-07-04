# -*- coding: utf-8 -*-
"""
섹터 → ETF 매핑 (단타 TOP픽 ETF 대안 명시용)
==============================================
universe.json의 한국 섹터명(한글)을 대표 ETF로 매핑.

주린이가 개별 종목이 부담스러울 때 섹터 ETF로 분산 진입할 수 있도록
FLOWX/텔레그램 출력에 "🔗 ETF 대안"으로 함께 표시.

데이터 출처: etf_universe.py 의 ETF_UNIVERSE["sector"]
"""
from __future__ import annotations


# ─────────────────────────────────────────────
# 섹터(한글) → 대표 ETF 매핑
# ─────────────────────────────────────────────
# 각 섹터마다 1~2개 대표 ETF.
# primary: 대표 (FLOWX/텔레그램 기본 표시)
# secondary: 보조 (상세 페이지용)

SECTOR_ETF_MAP: dict[str, dict] = {
    # ── 🔥 핵심 섹터 ────────────────────────────
    "전기전자": {
        "primary": {"code": "091160", "name": "KODEX 반도체"},
        "secondary": [
            {"code": "396500", "name": "TIGER Fn반도체TOP10"},
            {"code": "266370", "name": "KODEX IT"},
        ],
        "theme": "반도체",
    },
    "운송장비": {
        # 운송장비에 방산(한화에어로/LIG넥스원/현대로템) + 자동차 혼재
        "primary": {"code": "449450", "name": "PLUS K방산"},
        "secondary": [
            {"code": "091180", "name": "KODEX 자동차"},
            {"code": "139230", "name": "TIGER 조선TOP10"},
        ],
        "theme": "방산/자동차/조선",
    },
    "화학": {
        "primary": {"code": "305720", "name": "KODEX 2차전지산업"},
        "secondary": [],
        "theme": "2차전지/화학",
    },
    "금속": {
        "primary": {"code": "139320", "name": "TIGER 금속선물(H)"},
        "secondary": [],
        "theme": "철강/금속",
    },
    "건설": {
        "primary": {"code": "117700", "name": "KODEX 건설"},
        "secondary": [],
        "theme": "건설",
    },
    "기계장비": {
        "primary": {"code": "139230", "name": "TIGER 조선TOP10"},
        "secondary": [
            {"code": "449450", "name": "PLUS K방산"},
        ],
        "theme": "조선/기계",
    },

    # ── 💎 중간 섹터 ────────────────────────────
    "금융": {
        "primary": {"code": "140700", "name": "KODEX 보험"},
        "secondary": [],
        "theme": "금융/은행",
    },
    "제약": {
        "primary": {"code": "244580", "name": "KODEX 바이오"},
        "secondary": [],
        "theme": "바이오/제약",
    },
    "의료정밀": {
        "primary": {"code": "244580", "name": "KODEX 바이오"},
        "secondary": [],
        "theme": "의료/바이오",
    },
    "통신": {
        "primary": {"code": "266370", "name": "KODEX IT"},
        "secondary": [],
        "theme": "통신/IT",
    },
    "IT서비스": {
        "primary": {"code": "266370", "name": "KODEX IT"},
        "secondary": [],
        "theme": "IT 소프트웨어",
    },
    "유통": {
        "primary": {"code": "102780", "name": "KODEX 삼성그룹"},
        "secondary": [],
        "theme": "유통/대형주",
    },
    "운송창고": {
        "primary": {"code": "139230", "name": "TIGER 조선TOP10"},
        "secondary": [],
        "theme": "해운/물류",
    },

    # ── ⭐ 소형 섹터 (기본 지수 ETF) ────────────
    "음식료": {
        "primary": {"code": "069500", "name": "KODEX 200"},
        "secondary": [],
        "theme": "지수 추종",
    },
    "전기가스": {
        "primary": {"code": "069500", "name": "KODEX 200"},
        "secondary": [],
        "theme": "지수 추종",
    },
    "섬유의류": {
        "primary": {"code": "069500", "name": "KODEX 200"},
        "secondary": [],
        "theme": "지수 추종",
    },
    "종이목재": {
        "primary": {"code": "069500", "name": "KODEX 200"},
        "secondary": [],
        "theme": "지수 추종",
    },
    "비금속": {
        "primary": {"code": "069500", "name": "KODEX 200"},
        "secondary": [],
        "theme": "지수 추종",
    },
    "출판매체": {
        "primary": {"code": "229200", "name": "KODEX 코스닥150"},
        "secondary": [],
        "theme": "코스닥/엔터",
    },
    "오락문화": {
        "primary": {"code": "229200", "name": "KODEX 코스닥150"},
        "secondary": [],
        "theme": "엔터/게임",
    },
    "기타제조": {
        "primary": {"code": "069500", "name": "KODEX 200"},
        "secondary": [],
        "theme": "지수 추종",
    },
    "기타": {
        "primary": {"code": "069500", "name": "KODEX 200"},
        "secondary": [],
        "theme": "지수 추종",
    },
    "일반서비스": {
        "primary": {"code": "069500", "name": "KODEX 200"},
        "secondary": [],
        "theme": "지수 추종",
    },
}


# ─────────────────────────────────────────────
# 특수 키워드 기반 오버라이드
# ─────────────────────────────────────────────
# 운송장비에 방산/자동차가 혼재되어 있어
# 종목명으로 더 정확히 매핑하기 위한 보조 테이블.
STOCK_NAME_OVERRIDE: dict[str, dict] = {
    # 방산
    "한화에어로": {"code": "449450", "name": "PLUS K방산"},
    "한화에어로스페이스": {"code": "449450", "name": "PLUS K방산"},
    "LIG넥스원": {"code": "449450", "name": "PLUS K방산"},
    "현대로템": {"code": "449450", "name": "PLUS K방산"},
    "한국항공우주": {"code": "449450", "name": "PLUS K방산"},
    "풍산": {"code": "449450", "name": "PLUS K방산"},
    # 자동차
    "현대차": {"code": "091180", "name": "KODEX 자동차"},
    "기아": {"code": "091180", "name": "KODEX 자동차"},
    "현대모비스": {"code": "091180", "name": "KODEX 자동차"},
    # 조선
    "HD현대중공업": {"code": "139230", "name": "TIGER 조선TOP10"},
    "삼성중공업": {"code": "139230", "name": "TIGER 조선TOP10"},
    "한화오션": {"code": "139230", "name": "TIGER 조선TOP10"},
    "HD한국조선해양": {"code": "139230", "name": "TIGER 조선TOP10"},
    # 2차전지
    "LG에너지솔루션": {"code": "305720", "name": "KODEX 2차전지산업"},
    "삼성SDI": {"code": "305720", "name": "KODEX 2차전지산업"},
    "에코프로": {"code": "305720", "name": "KODEX 2차전지산업"},
    "에코프로비엠": {"code": "305720", "name": "KODEX 2차전지산업"},
    "포스코퓨처엠": {"code": "305720", "name": "KODEX 2차전지산업"},
    "LG화학": {"code": "305720", "name": "KODEX 2차전지산업"},
    # 반도체
    "삼성전자": {"code": "091160", "name": "KODEX 반도체"},
    "SK하이닉스": {"code": "091160", "name": "KODEX 반도체"},
    "한미반도체": {"code": "091160", "name": "KODEX 반도체"},
    "HPSP": {"code": "091160", "name": "KODEX 반도체"},
    "삼성전기": {"code": "091160", "name": "KODEX 반도체"},
    # 바이오
    "삼성바이오로직스": {"code": "244580", "name": "KODEX 바이오"},
    "셀트리온": {"code": "244580", "name": "KODEX 바이오"},
    "유한양행": {"code": "244580", "name": "KODEX 바이오"},
    "한미약품": {"code": "244580", "name": "KODEX 바이오"},
}


# ─────────────────────────────────────────────
# API
# ─────────────────────────────────────────────
def get_etf_for_stock(stock_name: str, sector: str) -> dict:
    """
    종목 이름 + 섹터로 최적 ETF 대안 반환.

    우선순위:
    1) 종목 이름이 STOCK_NAME_OVERRIDE 에 있으면 그것 사용 (정확도 우선)
    2) 섹터 매핑 테이블의 primary 사용
    3) 기본값 KODEX 200

    Returns:
        {"code": "091160", "name": "KODEX 반도체", "theme": "반도체"}
    """
    # 1) 종목 이름 직접 오버라이드
    if stock_name in STOCK_NAME_OVERRIDE:
        etf = STOCK_NAME_OVERRIDE[stock_name].copy()
        # theme 찾기
        for sec_data in SECTOR_ETF_MAP.values():
            if sec_data["primary"]["code"] == etf["code"]:
                etf["theme"] = sec_data.get("theme", "")
                break
        else:
            etf["theme"] = ""
        return etf

    # 2) 섹터 매핑
    if sector in SECTOR_ETF_MAP:
        primary = SECTOR_ETF_MAP[sector]["primary"]
        return {
            "code": primary["code"],
            "name": primary["name"],
            "theme": SECTOR_ETF_MAP[sector].get("theme", ""),
        }

    # 3) 기본값
    return {"code": "069500", "name": "KODEX 200", "theme": "지수 추종"}


def get_sector_etf(sector: str) -> dict | None:
    """섹터명 → primary ETF 정보."""
    if sector in SECTOR_ETF_MAP:
        p = SECTOR_ETF_MAP[sector]["primary"]
        return {
            "code": p["code"],
            "name": p["name"],
            "theme": SECTOR_ETF_MAP[sector].get("theme", ""),
        }
    return None


def aggregate_etf_from_picks(picks: list[dict]) -> list[dict]:
    """
    TOP픽 리스트 → 섹터 ETF 집계 (중복 제거 + 종목 수 카운트).

    Returns:
        [{"code": "091160", "name": "KODEX 반도체", "theme": "반도체",
          "stock_count": 3, "stocks": ["SK하이닉스", "삼성전기", "삼성SDI"]}, ...]
    """
    etf_buckets: dict[str, dict] = {}
    for p in picks:
        name = p.get("name", "")
        sector = p.get("sector", "")
        etf = get_etf_for_stock(name, sector)
        code = etf["code"]
        if code not in etf_buckets:
            etf_buckets[code] = {
                "code": code,
                "name": etf["name"],
                "theme": etf["theme"],
                "stock_count": 0,
                "stocks": [],
            }
        etf_buckets[code]["stock_count"] += 1
        etf_buckets[code]["stocks"].append(name)

    # 종목 수 내림차순 정렬
    result = sorted(etf_buckets.values(), key=lambda x: -x["stock_count"])
    return result


if __name__ == "__main__":
    # 테스트
    test_picks = [
        {"name": "SK하이닉스", "sector": "전기전자"},
        {"name": "삼성전기", "sector": "전기전자"},
        {"name": "삼성SDI", "sector": "전기전자"},
        {"name": "한화에어로", "sector": "운송장비"},
        {"name": "LIG넥스원", "sector": "운송장비"},
        {"name": "대한해운", "sector": "운송창고"},
        {"name": "LS에코에너지", "sector": "금융"},  # 잘못 분류된 케이스
    ]

    print("=== 개별 매핑 ===")
    for p in test_picks:
        etf = get_etf_for_stock(p["name"], p["sector"])
        print(f"  {p['name']:12} ({p['sector']:8}) → {etf['name']} ({etf['code']}) [{etf['theme']}]")

    print()
    print("=== ETF 집계 ===")
    for e in aggregate_etf_from_picks(test_picks):
        print(f"  {e['name']:18} ({e['code']}) [{e['theme']:15}] "
              f"× {e['stock_count']}종목: {', '.join(e['stocks'])}")
