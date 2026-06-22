# -*- coding: utf-8 -*-
"""
유니버스 자동 빌더 - pykrx 기반
================================
시총 기준으로 전종목 유니버스를 자동 생성하고
수급 데이터(투자자/외인소진율/공매도)를 수집한다.

사용법:
  python -m data.universe_builder              # 시총 1조+ (기본)
  python -m data.universe_builder --min-cap 5000  # 시총 5000억+
  python -m data.universe_builder --force       # 캐시 무시 강제 수집
"""

import sys
import os
import io
import time
import json
import logging
from pathlib import Path
from datetime import datetime, timedelta

import pandas as pd

logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data_store"
FLOW_DIR = DATA_DIR / "flow"
SHORT_DIR = DATA_DIR / "short"
DAILY_DIR = DATA_DIR / "daily"
UNIVERSE_FILE = DATA_DIR / "universe.json"


def _ensure_dirs():
    for d in [DATA_DIR, FLOW_DIR, SHORT_DIR, DAILY_DIR]:
        d.mkdir(parents=True, exist_ok=True)


def _load_existing_universe() -> dict:
    """기존 universe.json 로드 (pykrx 실패 시 폴백)"""
    if UNIVERSE_FILE.exists():
        with open(UNIVERSE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def _find_latest_trading_day():
    """최근 거래일 찾기 (주말/공휴일 안전 처리)"""
    from pykrx import stock
    today = datetime.now()
    for i in range(10):
        d = (today - timedelta(days=i)).strftime("%Y%m%d")
        try:
            cap = stock.get_market_cap_by_ticker(d, market="ALL")
            if cap is None or cap.empty or "시가총액" not in cap.columns:
                continue
            nonzero = cap[cap["시가총액"] > 0]
            if len(nonzero) > 100:
                return d
        except Exception:
            continue
    return today.strftime("%Y%m%d")


def _build_sector_mapping(date: str) -> dict:
    """pykrx 업종지수 → 종목코드 섹터 매핑 생성

    Returns:
        {code: sector_name}  예: {"005930": "전기전자", "035420": "일반서비스"}
    """
    from pykrx import stock

    # KOSPI 업종지수 (1005~1026)
    KOSPI_SECTORS = {
        "1005": "음식료", "1006": "섬유의류", "1007": "종이목재",
        "1008": "화학", "1009": "제약", "1010": "비금속",
        "1011": "금속", "1012": "기계장비", "1013": "전기전자",
        "1014": "의료정밀", "1015": "운송장비", "1016": "유통",
        "1017": "전기가스", "1018": "건설", "1019": "운송창고",
        "1020": "통신", "1021": "금융", "1024": "증권",
        "1025": "보험", "1026": "일반서비스",
    }
    # KOSDAQ 업종지수 (2012~2077)
    KOSDAQ_SECTORS = {
        "2012": "일반서비스", "2026": "건설", "2027": "유통",
        "2029": "운송창고", "2031": "금융", "2037": "오락문화",
        "2056": "음식료", "2058": "섬유의류", "2062": "종이목재",
        "2063": "출판매체", "2065": "화학", "2066": "제약",
        "2067": "비금속", "2068": "금속", "2070": "기계장비",
        "2072": "전기전자", "2074": "의료정밀", "2075": "운송장비",
        "2077": "기타제조", "2114": "통신", "2118": "IT서비스",
    }

    mapping = {}
    all_sectors = {**KOSPI_SECTORS, **KOSDAQ_SECTORS}

    for idx_code, sector_name in all_sectors.items():
        try:
            codes = stock.get_index_portfolio_deposit_file(idx_code, date)
            for c in codes:
                if c not in mapping:  # 첫 매핑 우선
                    mapping[c] = sector_name
        except Exception:
            continue

    print(f"  섹터 매핑: {len(mapping)}종목 완료")
    return mapping


def build_universe(min_cap_억: int = 200) -> dict:
    """시총 기준 유니버스 자동 생성

    Args:
        min_cap_억: 최소 시가총액 (억원). 기본 200 = 2백억 (v3: 500→200 소형주 확대)

    Returns:
        {code: {"name": ..., "market": ..., "cap": ...}}
    """
    # ★ KRX 전역 kill switch (6/22 IP 차단 대응) — pykrx=KRX. default 차단. 새 IP 후 1봇만.
    try:
        from data.krx_gate import krx_enabled, krx_block_reason
        if not krx_enabled():
            logger.warning("[krx_gate] %s", krx_block_reason())
            return {}
    except Exception:
        logger.warning("[krx_gate] 게이트 로드 실패 — KRX 보수적 차단(IP 보호)")
        return {}
    from pykrx import stock

    print(f"\n🔍 유니버스 빌드 - 시총 {min_cap_억:,}억원 이상")
    print("=" * 60)

    date = _find_latest_trading_day()
    print(f"  기준일: {date}")

    cap_df = None
    for _attempt_offset in range(10):
        _try_date = (datetime.strptime(date, "%Y%m%d") - timedelta(days=_attempt_offset)).strftime("%Y%m%d")
        try:
            cap_df = stock.get_market_cap_by_ticker(_try_date, market="ALL")
            if cap_df is not None and not cap_df.empty and "시가총액" in cap_df.columns:
                if (cap_df["시가총액"] > 0).sum() > 100:
                    print(f"  시가총액 기준일: {_try_date}")
                    break
            cap_df = None
        except Exception as _e:
            print(f"  시가총액 조회 실패 ({_try_date}): {_e}")
            cap_df = None
            continue

    if cap_df is None or cap_df.empty:
        print("  [WARN] pykrx 시가총액 조회 실패 → Naver Finance API fallback")
        return _build_universe_naver(min_cap_억)

    nonzero = cap_df[cap_df["시가총액"] > 0].copy()

    min_cap_won = min_cap_억 * 1_0000_0000  # 억 → 원
    filtered = nonzero[nonzero["시가총액"] >= min_cap_won].copy()

    # KOSPI 목록 1번만 조회 (성능)
    kospi_set = set(stock.get_market_ticker_list(date, market="KOSPI"))

    # PER/PBR 한 번에 수집 (밸류에이션 안전장치용)
    fund_df = None
    for offset in range(5):
        fund_date = (datetime.strptime(date, "%Y%m%d") - timedelta(days=offset)).strftime("%Y%m%d")
        try:
            _fund = stock.get_market_fundamental_by_ticker(fund_date, market="ALL")
            if _fund is not None and (_fund["PER"] > 0).sum() > 100:
                fund_df = _fund
                print(f"  PER/PBR 기준일: {fund_date}")
                break
        except Exception:
            continue

    # 섹터 매핑 (KRX 업종지수 기반)
    sector_map = _build_sector_mapping(date)

    # 스팩/리츠 제거 (우선주는 포함 - 미래에셋증권우 같은 유동성 높은 우선주 포착용)
    exclude_keywords = ["스팩", "SPAC", "리츠"]
    universe = {}

    for code in filtered.index:
        name = stock.get_market_ticker_name(code)
        if not name:
            continue

        # 스팩/리츠 제거
        skip = False
        for kw in exclude_keywords:
            if kw in name:
                skip = True
                break
        if skip:
            continue

        cap_억 = filtered.loc[code, "시가총액"] / 1_0000_0000
        vol = filtered.loc[code, "거래량"]

        # KOSPI vs KOSDAQ 판별 (캐시된 set 사용)
        market = "KOSPI" if code in kospi_set else "KOSDAQ"
        suffix = ".KS" if market == "KOSPI" else ".KQ"
        mkt_code = "J" if market == "KOSPI" else "Q"

        # PER/PBR 조회
        per_val = 0.0
        pbr_val = 0.0
        if fund_df is not None and code in fund_df.index:
            per_val = float(fund_df.loc[code, "PER"])
            pbr_val = float(fund_df.loc[code, "PBR"])

        universe[code] = {
            "name": name,
            "market": market,
            "suffix": suffix,
            "mkt_code": mkt_code,
            "sector": sector_map.get(code, "기타"),
            "cap_억": int(cap_억),
            "volume": int(vol),
            "per": round(per_val, 1),
            "pbr": round(pbr_val, 2),
        }

        time.sleep(0.05)  # KRX 속도 제한

    # 시총순 정렬
    universe = dict(sorted(universe.items(), key=lambda x: -x[1]["cap_억"]))

    print(f"  전체 시장: {len(nonzero):,}개")
    print(f"  시총 {min_cap_억:,}억+: {len(filtered)}개")
    print(f"  필터 후 유니버스: {len(universe)}개")
    print(f"  KOSPI: {sum(1 for v in universe.values() if v['market']=='KOSPI')}개")
    print(f"  KOSDAQ: {sum(1 for v in universe.values() if v['market']=='KOSDAQ')}개")
    per_zero = sum(1 for v in universe.values() if v.get("per", 0) == 0)
    per_high = sum(1 for v in universe.values() if v.get("per", 0) > 200)
    print(f"  PER=0(적자): {per_zero}개 | PER>200(고평가): {per_high}개")

    # 저장
    _ensure_dirs()
    with open(UNIVERSE_FILE, "w", encoding="utf-8") as f:
        json.dump(universe, f, ensure_ascii=False, indent=2)
    print(f"  저장: {UNIVERSE_FILE}")

    return universe


def load_universe() -> dict:
    """저장된 유니버스 로드"""
    if UNIVERSE_FILE.exists():
        with open(UNIVERSE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def patch_universe_from_quant(min_cap: int = 200) -> int:
    """quant_investor_extra에 있지만 universe에 없는 종목을 KRX Open API로 보충.

    pykrx 장애 시 build_universe()가 일부 종목을 놓칠 수 있으므로,
    quant_investor_extra.json과 대조하여 누락 종목을 추가한다.

    Returns:
        추가된 종목 수
    """
    quant_path = DATA_DIR / "quant_investor_extra.json"
    if not quant_path.exists():
        logger.info("quant_investor_extra.json 없음 — 패치 스킵")
        return 0

    universe = load_universe()
    if not universe:
        logger.warning("universe.json 비어있음 — 패치 스킵")
        return 0

    raw = json.loads(quant_path.read_text(encoding="utf-8"))
    daily = raw.get("daily", {})

    # 미등록 종목 찾기
    missing_codes = [code for code in daily if code not in universe]
    if not missing_codes:
        logger.info("universe 패치: 누락 종목 없음")
        return 0

    # KRX Open API로 시총 + 시장 정보 조회
    try:
        from data.krx_openapi_client import fetch_stock_daily
    except ImportError:
        logger.warning("krx_openapi_client 없음 — 패치 스킵")
        return 0

    today = datetime.now().strftime("%Y%m%d")
    krx = fetch_stock_daily(today, market="all")
    if not krx:
        # 어제 시도
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
        krx = fetch_stock_daily(yesterday, market="all")

    if not krx:
        logger.warning("KRX API 데이터 없음 — 패치 스킵")
        return 0

    # 기존 섹터 매핑 재활용
    old_sector = {}
    for code, info in universe.items():
        if isinstance(info, dict) and info.get("sector"):
            old_sector[code] = info["sector"]

    added = 0
    for code in missing_codes:
        krx_info = krx.get(code, {})
        if not krx_info:
            continue

        cap_val = krx_info.get("mktcap_b", 0)
        if cap_val < min_cap:
            continue

        market = krx_info.get("market", "KOSPI")
        name = daily[code].get("name", krx_info.get("name", ""))

        universe[code] = {
            "name": name,
            "market": market,
            "suffix": ".KS" if market == "KOSPI" else ".KQ",
            "mkt_code": "J" if market == "KOSPI" else "Q",
            "sector": old_sector.get(code, "기타"),
            "volume": krx_info.get("volume", 0),
            "per": 0,
            "pbr": 0,
            "cap_億": round(cap_val, 1),
        }
        added += 1

    if added > 0:
        with open(UNIVERSE_FILE, "w", encoding="utf-8") as f:
            json.dump(universe, f, ensure_ascii=False, indent=2)
        logger.info(f"universe 패치: {added}종목 추가 (총 {len(universe)})")
    else:
        logger.info("universe 패치: 추가 대상 없음")

    return added


# ═══════════════════════════════════════════════════
#  Naver Finance API 기반 유니버스 빌드 (pykrx 실패 시 fallback)
# ═══════════════════════════════════════════════════

def _build_universe_naver(min_cap_억: int = 200) -> dict:
    """Naver Finance API로 유니버스 빌드 (pykrx KRX API 차단 시 fallback)

    기존 universe.json의 섹터 매핑을 최대한 재활용하고,
    시총 기준으로 필터링하여 새 유니버스를 생성한다.
    """
    import requests

    print(f"\n  Naver Finance API 유니버스 빌드 - 시총 {min_cap_억:,}억원+")
    print("=" * 60)

    hdrs = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    exclude_keywords = ["스팩", "SPAC", "리츠"]

    # 기존 유니버스에서 섹터 매핑 로드
    old_uni = _load_existing_universe()
    old_sector_map = {}
    for code, info in old_uni.items():
        if isinstance(info, dict) and info.get("sector"):
            old_sector_map[code] = info["sector"]
    print(f"  기존 섹터 매핑: {len(old_sector_map)}종목")

    universe = {}

    for market, market_name in [("KOSPI", "KOSPI"), ("KOSDAQ", "KOSDAQ")]:
        page = 1
        reached_min = False
        mkt_count = 0

        while not reached_min:
            url = (
                f"https://m.stock.naver.com/api/stocks/marketValue/"
                f"{market}?page={page}&pageSize=100"
            )
            try:
                r = requests.get(url, headers=hdrs, timeout=10)
                if r.status_code != 200:
                    break
                data = r.json()
                stocks = data.get("stocks", [])
                if not stocks:
                    break
                total = data.get("totalCount", 0)

                for s in stocks:
                    code = s.get("itemCode", "")
                    if not code or len(code) != 6:
                        continue

                    name = s.get("stockName", code)
                    cap_str = s.get("marketValue", "0")
                    cap_억 = int(str(cap_str).replace(",", "")) if cap_str else 0
                    vol_str = s.get("accumulatedTradingVolume", "0")

                    try:
                        vol = int(str(vol_str).replace(",", "")) if vol_str else 0
                    except (ValueError, TypeError):
                        vol = 0

                    # 시총 필터 (내림차순이므로 min 이하 도달하면 종료)
                    if cap_억 < min_cap_억:
                        reached_min = True
                        break

                    # 스팩/리츠 제거
                    skip = False
                    for kw in exclude_keywords:
                        if kw in name:
                            skip = True
                            break
                    if skip:
                        continue

                    # 거래량 최소 기준 (1만주 미만 제외)
                    if vol < 10000:
                        continue

                    sosok = s.get("sosok", "")
                    mkt = "KOSPI" if sosok == "0" else "KOSDAQ"
                    suffix = ".KS" if mkt == "KOSPI" else ".KQ"
                    mkt_code = "J" if mkt == "KOSPI" else "Q"

                    # 섹터 매핑 (기존 universe에서 가져오기)
                    sector = old_sector_map.get(code, "기타")

                    # PER/PBR (기존 universe에서 가져오기)
                    old_info = old_uni.get(code, {})
                    per_val = old_info.get("per", 0.0) if isinstance(old_info, dict) else 0.0
                    pbr_val = old_info.get("pbr", 0.0) if isinstance(old_info, dict) else 0.0

                    universe[code] = {
                        "name": name,
                        "market": mkt,
                        "suffix": suffix,
                        "mkt_code": mkt_code,
                        "sector": sector,
                        "cap_億": cap_억,
                        "volume": vol,
                        "per": round(per_val, 1),
                        "pbr": round(pbr_val, 2),
                    }
                    mkt_count += 1

                if len(stocks) < 100 or page * 100 >= total:
                    break
                page += 1
                time.sleep(0.15)

            except Exception as e:
                print(f"  {market_name} p{page} 오류: {e}")
                break

        print(f"  {market_name}: {mkt_count}종목")

    # cap_億 → cap_억 통일
    for code, info in universe.items():
        if "cap_億" in info:
            info["cap_억"] = info.pop("cap_億")

    # 시총순 정렬
    universe = dict(sorted(universe.items(), key=lambda x: -x[1].get("cap_억", 0)))

    kospi_cnt = sum(1 for v in universe.values() if v["market"] == "KOSPI")
    kosdaq_cnt = sum(1 for v in universe.values() if v["market"] == "KOSDAQ")
    sector_cnt = sum(1 for v in universe.values() if v.get("sector") != "기타")
    print(f"\n  유니버스: {len(universe)}종목 (KOSPI {kospi_cnt} / KOSDAQ {kosdaq_cnt})")
    print(f"  섹터 매핑: {sector_cnt}종목 ({len(universe) - sector_cnt}종목 '기타')")

    # 저장
    _ensure_dirs()
    with open(UNIVERSE_FILE, "w", encoding="utf-8") as f:
        json.dump(universe, f, ensure_ascii=False, indent=2)
    print(f"  저장: {UNIVERSE_FILE}")

    return universe


# ═══════════════════════════════════════════════════
#  소형주 유니버스 (급등주 모멘텀 모듈용)
# ═══════════════════════════════════════════════════

SMALLCAP_FILE = DATA_DIR / "universe_smallcap.json"


def build_smallcap_universe(min_cap_억: int = 30, max_cap_억: int = 500) -> dict:
    """소형주 유니버스 빌드 (시총 300억~5000억) - 네이버 금융 기반

    대형주 유니버스와 완전 분리. 급등주 모멘텀 스캐너 전용.
    pykrx 인코딩 이슈 우회를 위해 네이버 금융에서 시총/거래량 수집.

    Args:
        min_cap_억: 최소 시총 (기본 30 = 300억)
        max_cap_억: 최대 시총 (기본 500 = 5000억)
    """
    import requests
    from bs4 import BeautifulSoup

    print(f"\n  소형주 유니버스 빌드 - 시총 {min_cap_억:,}~{max_cap_억:,}억원")
    print("=" * 60)

    hdrs = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
    exclude_keywords = ["스팩", "SPAC", "리츠", "우B", "우C"]
    universe = {}

    # 코스피(sosok=0) + 코스닥(sosok=1) 전종목 시총순 크롤링
    for sosok, market_name in [(0, "KOSPI"), (1, "KOSDAQ")]:
        page = 1
        last_page = 1
        reached_min = False

        while page <= last_page and not reached_min:
            url = f"https://finance.naver.com/sise/sise_market_sum.naver?sosok={sosok}&page={page}"
            try:
                resp = requests.get(url, headers=hdrs, timeout=10)
                resp.encoding = "euc-kr"
                soup = BeautifulSoup(resp.text, "html.parser")

                # 마지막 페이지 확인 (첫 페이지에서만)
                if page == 1:
                    pgn = soup.select(".pgRR a")
                    if pgn:
                        last_page = int(pgn[0]["href"].split("page=")[1])
                    print(f"  {market_name}: {last_page}페이지 스캔...")

                rows = soup.select("table.type_2 tr")
                for tr in rows:
                    tds = tr.select("td")
                    if len(tds) < 10:
                        continue

                    # 종목명 + 코드 추출
                    link = tds[1].select_one("a")
                    if not link:
                        continue
                    name = link.text.strip()
                    href = link.get("href", "")
                    if "code=" not in href:
                        continue
                    code = href.split("code=")[1][:6]

                    # 시총(억) - 7번째 컬럼 (인덱스 6)
                    cap_text = tds[6].text.strip().replace(",", "")
                    if not cap_text or not cap_text.isdigit():
                        continue
                    cap_억 = int(cap_text)

                    # 시총 범위: 네이버는 시총 내림차순
                    if cap_억 > max_cap_억:
                        continue  # 아직 범위 안 진입
                    if cap_억 < min_cap_억:
                        reached_min = True
                        break  # 이후 전부 작음 → 다음 시장으로

                    # 거래량 - 10번째 컬럼 (인덱스 9)
                    vol_text = tds[9].text.strip().replace(",", "")
                    vol = int(vol_text) if vol_text.isdigit() else 0

                    # 스팩/리츠/우선주 제거
                    skip = False
                    for kw in exclude_keywords:
                        if kw in name:
                            skip = True
                            break
                    if code[-1] in "56789" and "우" in name:
                        skip = True
                    if skip:
                        continue

                    # 유동성 최소 기준: 거래량 1만주+
                    if vol < 10000:
                        continue

                    universe[code] = {
                        "name": name,
                        "market": market_name,
                        "cap_억": cap_억,
                        "volume": vol,
                    }

            except Exception as e:
                print(f"  {market_name} p{page} 오류: {e}")

            page += 1
            time.sleep(0.3)  # 네이버 속도 제한

        mkt_count = sum(1 for v in universe.values() if v["market"] == market_name)
        print(f"  {market_name}: {mkt_count}종목")

    # 시총순 정렬
    universe = dict(sorted(universe.items(), key=lambda x: -x[1]["cap_억"]))

    print(f"\n  필터 후 총: {len(universe)}종목")

    _ensure_dirs()
    with open(SMALLCAP_FILE, "w", encoding="utf-8") as f:
        json.dump(universe, f, ensure_ascii=False, indent=2)
    print(f"  저장: {SMALLCAP_FILE}")

    return universe


def load_smallcap_universe() -> dict:
    """소형주 유니버스 로드"""
    if SMALLCAP_FILE.exists():
        with open(SMALLCAP_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def collect_smallcap_daily(months: int = 6, force: bool = False):
    """소형주 일봉 수집 (6개월, 경량화)"""
    universe = load_smallcap_universe()
    if not universe:
        print("소형주 유니버스 없음 - 먼저 build_smallcap_universe() 실행")
        return 0
    codes = list(universe.keys())
    print(f"\n  소형주 일봉 수집: {len(codes)}종목 ({months}개월)")
    return collect_daily_pykrx(codes, months, force)


def collect_daily_pykrx(codes: list, months: int = 24, force: bool = False,
                        n_workers: int = 4):
    """pykrx로 일봉 데이터 수집 (DC-06: 병렬화 + C1: 스레드 안전)"""
    # ★ KRX 전역 kill switch (6/22 IP 차단 대응) — pykrx=KRX 웹스크래핑. default 차단. 새 IP 후 1봇만.
    try:
        from data.krx_gate import krx_enabled, krx_block_reason
        if not krx_enabled():
            logger.warning("[krx_gate] %s", krx_block_reason())
            return 0
    except Exception:
        logger.warning("[krx_gate] 게이트 로드 실패 — KRX 보수적 차단(IP 보호)")
        return 0
    import threading
    from concurrent.futures import ThreadPoolExecutor
    from pykrx import stock

    _ensure_dirs()
    end_date = datetime.now().strftime("%Y%m%d")
    start_date = (datetime.now() - timedelta(days=months * 30)).strftime("%Y%m%d")

    # ── C1: pykrx 싱글톤 사전 초기화 (메인 스레드에서 1회) ──
    # pykrx 내부 @singleton StockTicker에 Lock이 없어 스레드 경합 발생
    # → 스레딩 시작 전 메인 스레드에서 강제 초기화
    try:
        stock.get_market_ticker_list(end_date, market="ALL")
        logger.info("[C1] pykrx 싱글톤 초기화 완료 (메인 스레드)")
    except Exception as e:
        logger.warning(f"[C1] pykrx 사전 초기화 실패: {e}")

    # ── C1: pykrx API 호출 직렬화 Lock ──
    # pykrx 내부 캐시(DataFrame)가 thread-safe하지 않으므로
    # API 호출 자체를 Lock으로 보호 (I/O 대기 중 다른 스레드는 CSV 저장 등 수행)
    _pykrx_lock = threading.Lock()

    # 캐시 필터링
    need_fetch = []
    for code in codes:
        cache_file = DAILY_DIR / f"{code}.csv"
        if not force and cache_file.exists():
            try:
                cached = pd.read_csv(cache_file, index_col=0, parse_dates=True)
                if len(cached) > 0:
                    last = cached.index[-1]
                    if hasattr(last, 'to_pydatetime'):
                        last = last.to_pydatetime().replace(tzinfo=None)
                    days_old = (datetime.now() - last).days
                    if days_old <= 3:
                        continue
            except Exception:
                pass
        need_fetch.append(code)

    if not need_fetch:
        print(f"  일봉: 전체 캐시 히트 ({len(codes)}종목)")
        return 0

    print(f"  일봉 수집: {len(need_fetch)}종목 (Lock 직렬 + {n_workers} workers)...")

    # ── C2: 이전 크래시에서 남은 .csv.tmp 파일 정리 ──
    for tmp_file in DAILY_DIR.glob("*.csv.tmp"):
        try:
            tmp_file.unlink()
            logger.info(f"[C2] 잔여 tmp 삭제: {tmp_file.name}")
        except Exception:
            pass

    def _fetch_single(code):
        """단일 종목 일봉 — C1: Lock + 지수 백오프 / C2: 원자적 쓰기"""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                # pykrx 호출은 Lock으로 직렬화 (내부 싱글톤 캐시 보호)
                with _pykrx_lock:
                    df = stock.get_market_ohlcv_by_date(start_date, end_date, code)
                if df is not None and len(df) > 20:
                    if len(df.columns) == 6:
                        df.columns = ["시가", "고가", "저가", "종가", "거래량", "등락률"]
                    # C2: 원자적 쓰기 (tmp → replace) — Windows/Linux 모두 atomic overwrite
                    # Path.rename()은 Windows에서 대상 존재 시 실패 → Path.replace() 사용
                    csv_tmp = DAILY_DIR / f"{code}.csv.tmp"
                    csv_path = DAILY_DIR / f"{code}.csv"
                    df.to_csv(csv_tmp)
                    csv_tmp.replace(csv_path)
                    return True
                time.sleep(0.15)
                return False
            except Exception as e:
                # C2: 실패 시 .tmp 정리
                tmp = DAILY_DIR / f"{code}.csv.tmp"
                try:
                    tmp.unlink(missing_ok=True)
                except Exception:
                    pass
                backoff = (2 ** attempt)  # 1초 → 2초 → 4초
                if attempt < max_retries - 1:
                    logger.warning(f"일봉 {code} 실패({attempt+1}차): {e} — {backoff}초 후 재시도")
                    time.sleep(backoff)
                else:
                    logger.warning(f"일봉 {code} 최종 실패: {e}")
        return False

    collected = 0
    with ThreadPoolExecutor(max_workers=n_workers) as executor:
        results = list(executor.map(_fetch_single, need_fetch))
        collected = sum(1 for r in results if r)

    print(f"  일봉 수집 완료: {collected}개 신규/갱신")
    return collected


def collect_all_universe(min_cap_억: int = 1000, months: int = 24, force: bool = False):
    """유니버스 빌드 + 전체 데이터 수집"""
    # 1. 유니버스 빌드
    universe = build_universe(min_cap_억)
    codes = list(universe.keys())

    print(f"\n📊 {len(codes)}종목 데이터 수집 시작")
    print("=" * 60)

    # 2. 일봉 데이터
    print(f"\n[1/5] 일봉 OHLCV 수집 ({len(codes)}종목)...")
    collect_daily_pykrx(codes, months, force)

    # 3. 투자자별 순매수
    print(f"\n[2/5] 투자자별 순매수 (기관/외인)...")
    from data.flow_collector import collect_investor_flow
    collect_investor_flow(codes, months, force)

    # 4. 외국인 소진율
    print(f"\n[3/5] 외국인 소진율...")
    from data.flow_collector import collect_foreign_exhaustion
    collect_foreign_exhaustion(codes, months, force)

    # 5. 공매도 잔고
    print(f"\n[4/5] 공매도 잔고...")
    from data.flow_collector import collect_short_balance
    collect_short_balance(codes, months, force)

    # 6. 공매도 거래량
    print(f"\n[5/5] 공매도 거래량...")
    from data.flow_collector import collect_short_volume
    collect_short_volume(codes, months, force)

    print(f"\n{'='*60}")
    print(f"  ✅ 전체 수집 완료: {len(codes)}종목")
    print(f"{'='*60}")

    return universe


# ============================================================
#  UNIVERSE dict 호환 (기존 코드와 호환)
# ============================================================

def get_universe_dict() -> dict:
    """기존 UNIVERSE 형식과 호환되는 dict 반환
    Returns: {code: (name, suffix, mkt_code)}
    """
    uni = load_universe()
    if not uni:
        # 폴백: 기존 하드코딩 UNIVERSE
        from data.kis_collector import UNIVERSE
        return UNIVERSE

    return {
        code: (info["name"], info["suffix"], info["mkt_code"])
        for code, info in uni.items()
    }


def get_valuation(code: str) -> dict:
    """종목의 PER/PBR 밸류에이션 조회

    Returns: {"per": float, "pbr": float, "warning": str or None}
    """
    uni = load_universe()
    if not uni or code not in uni:
        return {"per": 0, "pbr": 0, "warning": None}

    info = uni[code]
    per = info.get("per", 0)
    pbr = info.get("pbr", 0)

    warning = None
    if per == 0:
        warning = "적자"
    elif per > 200:
        warning = "고PER"
    if pbr > 0 and pbr < 0.3:
        warning = "저PBR"  # 좀비/구조조정 리스크

    return {"per": per, "pbr": pbr, "warning": warning}


def get_valuation_warnings(codes: list = None) -> dict:
    """여러 종목의 밸류에이션 경고 일괄 조회

    Returns: {code: {"per": float, "pbr": float, "warning": str or None}}
    """
    uni = load_universe()
    if not uni:
        return {}

    if codes is None:
        codes = list(uni.keys())

    result = {}
    for code in codes:
        if code not in uni:
            continue
        info = uni[code]
        per = info.get("per", 0)
        pbr = info.get("pbr", 0)

        warning = None
        if per == 0:
            warning = "적자"
        elif per > 200:
            warning = "고PER"
        if pbr > 0 and pbr < 0.3:
            warning = "저PBR"

        result[code] = {"per": per, "pbr": pbr, "warning": warning}

    return result


# ============================================================
#  CLI
# ============================================================

if __name__ == "__main__":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
    logging.basicConfig(level=logging.WARNING)

    import argparse
    parser = argparse.ArgumentParser(description="유니버스 빌더")
    parser.add_argument("--min-cap", type=int, default=200, help="최소 시총 (억원, 기본 200=2백억)")
    parser.add_argument("--force", action="store_true", help="캐시 무시")
    parser.add_argument("--months", type=int, default=24, help="수집 기간 (월)")
    parser.add_argument("--build-only", action="store_true", help="유니버스만 빌드 (수집X)")
    parser.add_argument("--smallcap", action="store_true", help="소형주 유니버스 빌드 (300억~5000억)")
    parser.add_argument("--max-cap", type=int, default=500, help="소형주 최대 시총 (억원, 기본 500=5000억)")
    args = parser.parse_args()

    if args.smallcap:
        sc_min = args.min_cap if args.min_cap != 500 else 30  # 소형주 기본 30억
        build_smallcap_universe(sc_min, args.max_cap)
        if not args.build_only:
            collect_smallcap_daily(args.months, args.force)
    elif args.build_only:
        build_universe(args.min_cap)
    else:
        collect_all_universe(args.min_cap, args.months, args.force)
