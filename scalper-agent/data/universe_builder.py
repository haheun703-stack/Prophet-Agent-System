# -*- coding: utf-8 -*-
"""
유니버스 자동 빌더 — pykrx 기반
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


def _find_latest_trading_day():
    """최근 거래일 찾기"""
    from pykrx import stock
    today = datetime.now()
    for i in range(10):
        d = (today - timedelta(days=i)).strftime("%Y%m%d")
        cap = stock.get_market_cap_by_ticker(d, market="ALL")
        nonzero = cap[cap["시가총액"] > 0]
        if len(nonzero) > 100:
            return d
    return today.strftime("%Y%m%d")


def build_universe(min_cap_억: int = 10000) -> dict:
    """시총 기준 유니버스 자동 생성

    Args:
        min_cap_억: 최소 시가총액 (억원). 기본 10000 = 1조

    Returns:
        {code: {"name": ..., "market": ..., "cap": ...}}
    """
    from pykrx import stock

    print(f"\n🔍 유니버스 빌드 — 시총 {min_cap_억:,}억원 이상")
    print("=" * 60)

    date = _find_latest_trading_day()
    print(f"  기준일: {date}")

    cap_df = stock.get_market_cap_by_ticker(date, market="ALL")
    nonzero = cap_df[cap_df["시가총액"] > 0].copy()

    min_cap_won = min_cap_억 * 1_0000_0000  # 억 → 원
    filtered = nonzero[nonzero["시가총액"] >= min_cap_won].copy()

    # 우선주/스팩 제거
    exclude_keywords = ["우", "스팩", "SPAC", "리츠"]
    universe = {}

    for code in filtered.index:
        name = stock.get_market_ticker_name(code)
        if not name:
            continue

        # 우선주 제거 (코드 끝자리 5,7,8,9 = 우선주)
        if code[-1] in ("5", "7", "8", "9") and len(code) == 6:
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

        # KOSPI vs KOSDAQ 판별
        kospi_list = stock.get_market_ticker_list(date, market="KOSPI")
        market = "KOSPI" if code in kospi_list else "KOSDAQ"
        suffix = ".KS" if market == "KOSPI" else ".KQ"
        mkt_code = "J" if market == "KOSPI" else "Q"

        universe[code] = {
            "name": name,
            "market": market,
            "suffix": suffix,
            "mkt_code": mkt_code,
            "cap_억": int(cap_억),
            "volume": int(vol),
        }

        time.sleep(0.05)  # KRX 속도 제한

    # 시총순 정렬
    universe = dict(sorted(universe.items(), key=lambda x: -x[1]["cap_억"]))

    print(f"  전체 시장: {len(nonzero):,}개")
    print(f"  시총 {min_cap_억:,}억+: {len(filtered)}개")
    print(f"  필터 후 유니버스: {len(universe)}개")
    print(f"  KOSPI: {sum(1 for v in universe.values() if v['market']=='KOSPI')}개")
    print(f"  KOSDAQ: {sum(1 for v in universe.values() if v['market']=='KOSDAQ')}개")

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


def collect_daily_pykrx(codes: list, months: int = 6, force: bool = False):
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


def collect_all_universe(min_cap_억: int = 10000, months: int = 6, force: bool = False):
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


# ============================================================
#  CLI
# ============================================================

if __name__ == "__main__":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
    logging.basicConfig(level=logging.WARNING)

    import argparse
    parser = argparse.ArgumentParser(description="유니버스 빌더")
    parser.add_argument("--min-cap", type=int, default=10000, help="최소 시총 (억원)")
    parser.add_argument("--force", action="store_true", help="캐시 무시")
    parser.add_argument("--months", type=int, default=6, help="수집 기간 (월)")
    parser.add_argument("--build-only", action="store_true", help="유니버스만 빌드 (수집X)")
    args = parser.parse_args()

    if args.build_only:
        build_universe(args.min_cap)
    else:
        collect_all_universe(args.min_cap, args.months, args.force)
