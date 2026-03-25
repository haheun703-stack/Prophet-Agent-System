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


def build_universe(min_cap_억: int = 500) -> dict:
    """시총 기준 유니버스 자동 생성

    Args:
        min_cap_억: 최소 시가총액 (억원). 기본 500 = 5백억 (v2: 1000→500 확대)

    Returns:
        {code: {"name": ..., "market": ..., "cap": ...}}
    """
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
        print("  [ERROR] pykrx 시가총액 조회 전체 실패 — 기존 universe.json 유지")
        return _load_existing_universe()

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


def collect_daily_pykrx(codes: list, months: int = 24, force: bool = False):
    """pykrx로 일봉 데이터 수집"""
    from pykrx import stock

    _ensure_dirs()
    end_date = datetime.now().strftime("%Y%m%d")
    start_date = (datetime.now() - timedelta(days=months * 30)).strftime("%Y%m%d")

    collected = 0
    for i, code in enumerate(codes):
        cache_file = DAILY_DIR / f"{code}.csv"

        if not force and cache_file.exists():
            cached = pd.read_csv(cache_file, index_col=0, parse_dates=True)
            if len(cached) > 0:
                last = cached.index[-1]
                if hasattr(last, 'to_pydatetime'):
                    last = last.to_pydatetime().replace(tzinfo=None)
                days_old = (datetime.now() - last).days
                if days_old <= 3:
                    continue

        if (i + 1) % 50 == 0 or i == 0:
            print(f"  일봉 [{i+1}/{len(codes)}] {code}...")

        try:
            df = stock.get_market_ohlcv_by_date(start_date, end_date, code)
            if df is not None and len(df) > 20:
                # 컬럼 표준화 (pykrx: 시가,고가,저가,종가,거래량,등락률)
                if len(df.columns) == 6:
                    df.columns = ["시가", "고가", "저가", "종가", "거래량", "등락률"]
                # 기존 7컬럼 형태면 그대로
                df.to_csv(cache_file)
                collected += 1
            time.sleep(0.15)
        except Exception as e:
            logger.warning(f"일봉 수집 실패 {code}: {e}")

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
    parser.add_argument("--min-cap", type=int, default=500, help="최소 시총 (억원, 기본 500=5천억)")
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
