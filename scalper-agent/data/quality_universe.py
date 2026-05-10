# -*- coding: utf-8 -*-
"""
품질 유니버스 필터 (Quality Universe Filter)
=============================================
상한가 엔진 전략2(눌림목 대기)에서 잡주를 제외하기 위한 품질 필터.

4-Layer 유니버스:
  Layer 1: EWY 바스켓 종목 (80종목) — 외국인이 실제 보유하는 대형주
  Layer 2: FLOWX 섹터 유니버스 KR 종목 (~130종목) — 섹터별 핵심 종목
  Layer 3: 추가 섹터 종목 (~50종목) — 2차전지, 전력장비 등 테마 보완
  Layer 4: 동적 급등주 (자동) — 최근 30일 내 +10%+ 급등 종목 자동 편입/만료

Layer 4 동적 유니버스 원리:
  - 상한가 엔진이 +10%+ 급등 감지 시 surge_universe.json에 자동 기록
  - 30일 이내 종목만 품질 유니버스에 편입
  - 섹터 로테이션 돌 때 해당 종목이 전략2 감시 대상에 자동 포함
  - 30일 경과 시 자동 만료 (재급등 시 갱신)

전략별 적용:
  - 전략 1 (3번째+ 상한가 → 즉시진입): 유니버스 필터 미적용 (워낙 희귀)
  - 전략 2 (10%+ 급등 → 눌림목 대기): 품질 유니버스 내 종목만 허용
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta
from pathlib import Path

logger = logging.getLogger("BH.QualityUniverse")

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data_store"
EWY_PATH = DATA_DIR / "ewy_holdings.json"
SECTOR_UNIVERSE_PATH = BASE_DIR.parent / "FLOWX_SECTOR_UNIVERSE.json"
SURGE_UNIVERSE_PATH = DATA_DIR / "limit_up" / "surge_universe.json"

# 동적 유니버스 설정
SURGE_EXPIRE_DAYS = 30  # 급등 후 30일간 유니버스 편입

# ── 추가 섹터별 핵심 종목 (EWY/FLOWX에 없는 테마 보완) ──
# 유저 지정 섹터: 2차전지, 전력장비, 철강, 화장품, 사이버보안, 액침냉각, 우주항공 등
EXTRA_SECTOR_STOCKS: dict[str, list[str]] = {
    "2차전지": [
        "373220",  # LG에너지솔루션
        "006400",  # 삼성SDI
        "086520",  # 에코프로
        "247540",  # 에코프로비엠
        "003670",  # 포스코퓨처엠
        "051910",  # LG화학
        "012450",  # 한화에어로스페이스 (2차전지 장비도 포함)
        "108320",  # LX인터내셔널
        "298040",  # 효성중공업
        "066570",  # LG전자
    ],
    "전력장비/변압기": [
        "267260",  # HD현대일렉트릭
        "010120",  # LS ELECTRIC
        "298040",  # 효성중공업
        "272210",  # 한화시스템
        "015760",  # 한국전력
        "009450",  # 경동나비엔
        "010060",  # OCI홀딩스
        "281820",  # 케이사인
    ],
    "철강/금속": [
        "005490",  # POSCO홀딩스
        "004020",  # 현대제철
        "001230",  # 동국홀딩스
        "023960",  # 에쓰씨엔지니어링
        "103140",  # 풍산
    ],
    "석유/화학": [
        "096770",  # SK이노베이션
        "010950",  # S-Oil
        "011170",  # 롯데케미칼
        "006120",  # SK디스커버리
        "051910",  # LG화학
    ],
    "화장품/뷰티": [
        "090430",  # 아모레퍼시픽
        "278470",  # 에이피알
        "090435",  # 아모레퍼시픽우
        "285130",  # SK케미칼
    ],
    "통신/IT": [
        "017670",  # SK텔레콤
        "032640",  # LG유플러스
        "030200",  # KT
        "035420",  # NAVER
        "035720",  # 카카오
        "018260",  # 삼성에스디에스
    ],
    "사이버보안": [
        "053800",  # 안랩
        "012510",  # 더존비즈온
        "058850",  # KCE
        "218150",  # 에스에스알
    ],
    "액침냉각": [
        "039490",  # 키움증권 (→ 이건 아닌데, 실제 종목 추가 필요시 갱신)
        "014680",  # 한솔케미칼
        "005430",  # 한국공항
    ],
    "우주/항공": [
        "047810",  # 한국항공우주
        "012450",  # 한화에어로스페이스
        "079550",  # LIG디펜스앤에어로스페이스
        "272210",  # 한화시스템
        "064350",  # 현대로템
    ],
    "소비재/유통": [
        "028260",  # 삼성물산
        "023530",  # 롯데쇼핑
        "004170",  # 신세계
        "069960",  # 현대백화점
        "033780",  # KT&G
    ],
}

# 캐시
_quality_codes: set[str] | None = None
_quality_details: dict[str, dict] | None = None


def _load_ewy_codes() -> dict[str, dict]:
    """EWY 바스켓 종목코드 + 상세 정보 로드"""
    if not EWY_PATH.exists():
        logger.warning(f"EWY 데이터 없음: {EWY_PATH}")
        return {}

    with open(EWY_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)

    result = {}
    raw = data.get("_raw", {}).get("stocks", {})
    for code, info in raw.items():
        result[code] = {
            "source": "ewy",
            "name": info.get("name", ""),
            "weight": info.get("weight", 0),
            "sector": info.get("sector_kr", ""),
        }

    logger.info(f"EWY 바스켓: {len(result)}종목")
    return result


def _load_sector_universe_codes() -> dict[str, dict]:
    """FLOWX 섹터 유니버스에서 KR 종목 로드 (tier 1 + tier 2)"""
    if not SECTOR_UNIVERSE_PATH.exists():
        logger.warning(f"섹터 유니버스 없음: {SECTOR_UNIVERSE_PATH}")
        return {}

    with open(SECTOR_UNIVERSE_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)

    result = {}
    for sector_key, sector_data in data.get("sectors", {}).items():
        sector_name = sector_data.get("name", sector_key)
        tiers = sector_data.get("tiers", {})
        for tier_key in ("2", "1"):  # KR Majors + KR Supply Chain
            for stock in tiers.get(tier_key, []):
                if stock.get("market") != "KR":
                    continue
                code = stock["ticker"]
                if code not in result:
                    result[code] = {
                        "source": "sector_universe",
                        "name": stock.get("name", ""),
                        "sector_theme": sector_name,
                    }

    logger.info(f"섹터 유니버스: {len(result)}종목 ({len(data.get('sectors', {}))}섹터)")
    return result


def _load_extra_codes() -> dict[str, dict]:
    """추가 섹터 종목 로드"""
    result = {}
    for sector, codes in EXTRA_SECTOR_STOCKS.items():
        for code in codes:
            if code not in result:
                result[code] = {
                    "source": "extra_sector",
                    "sector_theme": sector,
                }
    logger.info(f"추가 섹터: {len(result)}종목 ({len(EXTRA_SECTOR_STOCKS)}섹터)")
    return result


def _load_surge_universe() -> dict[str, dict]:
    """Layer 4: 동적 급등주 유니버스 로드

    surge_universe.json에서 최근 30일 이내 +10%+ 급등 종목만 필터링.
    만료된 종목은 자동 제거하고 파일 업데이트.

    Returns:
        {code: {source, name, sector, surge_date, change_pct, ...}}
    """
    if not SURGE_UNIVERSE_PATH.exists():
        logger.debug("동적 급등주 파일 없음 (아직 스캔 전)")
        return {}

    try:
        with open(SURGE_UNIVERSE_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (json.JSONDecodeError, Exception) as e:
        logger.warning(f"surge_universe.json 로드 실패: {e}")
        return {}

    stocks = data.get("stocks", {})
    if not stocks:
        return {}

    cutoff = (datetime.now() - timedelta(days=SURGE_EXPIRE_DAYS)).strftime("%Y-%m-%d")
    active = {}
    expired_count = 0

    for code, info in stocks.items():
        # 최신 급등일 기준 만료 체크
        surge_date = info.get("last_surge_date", "")
        if surge_date < cutoff:
            expired_count += 1
            continue
        active[code] = {
            "source": "surge_dynamic",
            "name": info.get("name", ""),
            "sector_theme": info.get("sector", ""),
            "surge_date": surge_date,
            "change_pct": info.get("max_change_pct", 0),
            "surge_count": info.get("surge_count", 1),
        }

    # 만료 종목 정리 — 파일 업데이트
    if expired_count > 0:
        cleaned = {c: s for c, s in stocks.items()
                   if s.get("last_surge_date", "") >= cutoff}
        data["stocks"] = cleaned
        data["last_cleanup"] = datetime.now().strftime("%Y-%m-%d %H:%M")
        data["active_count"] = len(cleaned)
        try:
            with open(SURGE_UNIVERSE_PATH, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            logger.info(f"동적 유니버스 정리: {expired_count}종목 만료 제거")
        except Exception as e:
            logger.warning(f"surge_universe.json 정리 저장 실패: {e}")

    logger.info(f"동적 급등주: {len(active)}종목 (만료 {expired_count}건 제거)")
    return active


def save_surge_stock(code: str, name: str, sector: str,
                     change_pct: float, close_price: float,
                     market_cap: float = 0) -> None:
    """급등 종목을 동적 유니버스에 저장/갱신

    limit_up_engine.scan_new_signals()에서 +10%+ 감지 시 호출.
    품질 유니버스 필터 이전에 호출하여, 비유니버스 종목도 기록.

    Args:
        code: 종목코드
        name: 종목명
        sector: 섹터
        change_pct: 당일 등락률
        close_price: 종가
        market_cap: 시가총액(억원)
    """
    SURGE_UNIVERSE_PATH.parent.mkdir(parents=True, exist_ok=True)

    data = {"stocks": {}, "created_at": "", "active_count": 0}
    if SURGE_UNIVERSE_PATH.exists():
        try:
            with open(SURGE_UNIVERSE_PATH, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            pass

    stocks = data.get("stocks", {})
    today = datetime.now().strftime("%Y-%m-%d")

    existing = stocks.get(code)
    if existing:
        # 기존 종목 갱신: 최대 등락률, 급등 횟수, 최신 날짜
        existing["last_surge_date"] = today
        existing["surge_count"] = existing.get("surge_count", 1) + 1
        if change_pct > existing.get("max_change_pct", 0):
            existing["max_change_pct"] = round(change_pct, 1)
        existing["last_close"] = close_price
        existing["sector"] = sector or existing.get("sector", "")
    else:
        stocks[code] = {
            "name": name,
            "sector": sector,
            "first_surge_date": today,
            "last_surge_date": today,
            "max_change_pct": round(change_pct, 1),
            "last_close": close_price,
            "market_cap": market_cap,
            "surge_count": 1,
        }

    data["stocks"] = stocks
    data["last_updated"] = datetime.now().strftime("%Y-%m-%d %H:%M")
    data["active_count"] = len(stocks)
    if not data.get("created_at"):
        data["created_at"] = today

    with open(SURGE_UNIVERSE_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    action = "갱신" if existing else "신규"
    logger.info(
        f"동적유니버스 {action}: {name}({code}) {change_pct:+.1f}% "
        f"총 {data['active_count']}종목"
    )


def build_quality_universe() -> tuple[set[str], dict[str, dict]]:
    """품질 유니버스 구축 (EWY + 섹터유니버스 + 추가섹터)

    Returns:
        (codes_set, details_dict)
        - codes_set: 종목코드 set
        - details_dict: {code: {source, name, sector_theme, ...}}
    """
    global _quality_codes, _quality_details

    if _quality_codes is not None:
        return _quality_codes, _quality_details

    details = {}

    # Layer 1: EWY
    ewy = _load_ewy_codes()
    for code, info in ewy.items():
        details[code] = info

    # Layer 2: 섹터 유니버스
    sector = _load_sector_universe_codes()
    for code, info in sector.items():
        if code not in details:
            details[code] = info
        else:
            # EWY에 이미 있으면 sector_theme만 추가
            details[code]["sector_theme"] = info.get("sector_theme", "")

    # Layer 3: 추가 섹터
    extra = _load_extra_codes()
    for code, info in extra.items():
        if code not in details:
            details[code] = info
        elif "sector_theme" not in details[code]:
            details[code]["sector_theme"] = info.get("sector_theme", "")

    # Layer 4: 동적 급등주 (최근 30일 내 +10%+ 급등)
    surge = _load_surge_universe()
    surge_new = 0
    for code, info in surge.items():
        if code not in details:
            details[code] = info
            surge_new += 1
        else:
            # 이미 있는 종목이면 surge 정보만 추가
            details[code]["surge_date"] = info.get("surge_date", "")
            details[code]["surge_count"] = info.get("surge_count", 0)

    codes = set(details.keys())

    # 캐시 저장
    _quality_codes = codes
    _quality_details = details

    logger.info(
        f"품질 유니버스 구축 완료: {len(codes)}종목 "
        f"(EWY {len(ewy)} + 섹터 {len(sector)} + 추가 {len(extra)} "
        f"+ 동적급등 {len(surge)}[신규{surge_new}])"
    )
    return codes, details


def is_quality_stock(code: str) -> bool:
    """종목이 품질 유니버스에 포함되는지 확인"""
    codes, _ = build_quality_universe()
    return code in codes


def get_stock_info(code: str) -> dict | None:
    """종목의 품질 유니버스 정보 반환"""
    _, details = build_quality_universe()
    return details.get(code)


def reset_cache():
    """캐시 초기화 (테스트/리로드용)"""
    global _quality_codes, _quality_details
    _quality_codes = None
    _quality_details = None


if __name__ == "__main__":
    import sys

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(message)s",
        datefmt="%H:%M:%S",
        handlers=[logging.StreamHandler(sys.stdout)],
    )

    codes, details = build_quality_universe()
    print(f"\n품질 유니버스: {len(codes)}종목")

    # 소스별 분류
    from collections import Counter
    sources = Counter(d.get("source", "?") for d in details.values())
    print(f"소스별: {dict(sources)}")

    # 섹터별 분류
    themes = Counter(d.get("sector_theme", d.get("sector", "미분류"))
                     for d in details.values())
    print(f"\n섹터별 종목수:")
    for theme, cnt in themes.most_common():
        print(f"  {theme:20} {cnt}종목")

    # EWY 탑10 표시
    print(f"\nEWY 탑10:")
    ewy_stocks = [(c, d) for c, d in details.items() if d.get("source") == "ewy"]
    ewy_stocks.sort(key=lambda x: -x[1].get("weight", 0))
    for code, info in ewy_stocks[:10]:
        print(f"  {info['name']:20} ({code}) 비중 {info.get('weight', 0):.2f}%")

    # Layer 4 동적 급등주 표시
    surge_stocks = [(c, d) for c, d in details.items()
                    if d.get("source") == "surge_dynamic"]
    if surge_stocks:
        print(f"\n동적 급등주 (Layer 4): {len(surge_stocks)}종목")
        surge_stocks.sort(key=lambda x: x[1].get("surge_date", ""), reverse=True)
        for code, info in surge_stocks[:20]:
            print(
                f"  {info.get('name', '?'):16} ({code}) "
                f"{info.get('change_pct', 0):+.1f}% "
                f"[{info.get('surge_date', '?')}] "
                f"급등{info.get('surge_count', 1)}회"
            )
