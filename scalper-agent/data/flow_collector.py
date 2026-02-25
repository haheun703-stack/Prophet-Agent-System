"""
수급 데이터 수집기 — 외국인/기관/공매도/소진율

데이터 소스: pykrx (KRX 크롤링, API키 불필요)

수집 항목:
  1순위: 외국인/기관 순매수 (금액+수량), 프로그램 매매 (추후)
  2순위: 공매도 잔고, 외국인 소진율
  3순위: (추후 추가)

사용법:
  python -m data.flow_collector
"""

import os
import sys
import time
import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

DATA_DIR = Path(__file__).resolve().parent.parent / "data_store"
FLOW_DIR = DATA_DIR / "flow"          # 수급 데이터
SHORT_DIR = DATA_DIR / "short"        # 공매도 데이터


def _ensure_dirs():
    FLOW_DIR.mkdir(parents=True, exist_ok=True)
    SHORT_DIR.mkdir(parents=True, exist_ok=True)


# ============================================================
#  1순위: 투자자별 순매수 (외국인/기관)
# ============================================================

def collect_investor_flow(
    codes: List[str],
    months: int = 24,
    force: bool = False,
) -> Dict[str, pd.DataFrame]:
    """투자자별 순매수 금액+수량 수집 (pykrx)

    컬럼: 기관합계, 기타법인, 개인, 외국인합계 (금액 기준)
    + 수량 컬럼: 기관합계_vol, 기타법인_vol, 개인_vol, 외국인합계_vol

    Returns: {code: DataFrame(date index)}
    """
    from pykrx import stock

    _ensure_dirs()
    end_date = datetime.now().strftime("%Y%m%d")
    start_date = (datetime.now() - timedelta(days=months * 30)).strftime("%Y%m%d")

    results = {}
    for i, code in enumerate(codes):
        cache_file = FLOW_DIR / f"{code}_investor.csv"

        # 캐시 확인
        if not force and cache_file.exists():
            cached = pd.read_csv(cache_file, index_col=0, parse_dates=True)
            if len(cached) > 0:
                days_old = (datetime.now() - cached.index[-1].to_pydatetime().replace(tzinfo=None)).days
                if days_old <= 3:
                    results[code] = cached
                    continue

        print(f"  [{i+1}/{len(codes)}] {code} 투자자별 수급 수집중...")

        try:
            # 금액 기준
            df_val = stock.get_market_trading_value_by_date(start_date, end_date, code)
            # 수량 기준
            df_vol = stock.get_market_trading_volume_by_date(start_date, end_date, code)

            if df_val is None or len(df_val) == 0:
                continue

            # 컬럼 정리
            df_val.columns = ["기관_금액", "기타법인_금액", "개인_금액", "외국인_금액", "전체_금액"]
            if df_vol is not None and len(df_vol) > 0:
                df_vol.columns = ["기관_수량", "기타법인_수량", "개인_수량", "외국인_수량", "전체_수량"]
                df = pd.concat([df_val, df_vol], axis=1)
            else:
                df = df_val

            # 전체 컬럼 제거 (항상 0)
            df = df.drop(columns=[c for c in df.columns if "전체" in c], errors="ignore")

            if len(df) > 0:
                df.to_csv(cache_file)
                results[code] = df

            time.sleep(0.3)  # KRX 속도 제한

        except Exception as e:
            logger.warning(f"투자자별 수급 수집 실패 {code}: {e}")
            continue

    print(f"  투자자별 수급 수집 완료: {len(results)}종목")
    return results


# ============================================================
#  1순위: 외국인 소진율
# ============================================================

def collect_foreign_exhaustion(
    codes: List[str],
    months: int = 24,
    force: bool = False,
) -> Dict[str, pd.DataFrame]:
    """외국인 보유비율(소진율) 수집

    컬럼: 보유수량, 한도수량, 소진율(%)

    Returns: {code: DataFrame(date index)}
    """
    from pykrx import stock

    _ensure_dirs()
    end_date = datetime.now().strftime("%Y%m%d")
    start_date = (datetime.now() - timedelta(days=months * 30)).strftime("%Y%m%d")

    results = {}
    for i, code in enumerate(codes):
        cache_file = FLOW_DIR / f"{code}_foreign_exh.csv"

        if not force and cache_file.exists():
            cached = pd.read_csv(cache_file, index_col=0, parse_dates=True)
            if len(cached) > 0:
                days_old = (datetime.now() - cached.index[-1].to_pydatetime().replace(tzinfo=None)).days
                if days_old <= 3:
                    results[code] = cached
                    continue

        print(f"  [{i+1}/{len(codes)}] {code} 외국인 소진율 수집중...")

        try:
            df = stock.get_exhaustion_rates_of_foreign_investment_by_date(
                start_date, end_date, code
            )
            if df is None or len(df) == 0:
                continue

            # 컬럼 정리 (상장주식수, 보유수량, 소진율, 한도수량, 한도소진율)
            df.columns = ["상장주식수", "보유수량", "소진율", "한도수량", "한도소진율"]

            if len(df) > 0:
                df.to_csv(cache_file)
                results[code] = df

            time.sleep(0.3)

        except Exception as e:
            logger.warning(f"외국인 소진율 수집 실패 {code}: {e}")
            continue

    print(f"  외국인 소진율 수집 완료: {len(results)}종목")
    return results


# ============================================================
#  2순위: 공매도 잔고
# ============================================================

def collect_short_balance(
    codes: List[str],
    months: int = 24,
    force: bool = False,
) -> Dict[str, pd.DataFrame]:
    """공매도 잔고 수집

    컬럼: 공매도잔고, 상장주식수, 공매도금액, 시가총액, 비중(%)

    Returns: {code: DataFrame(date index)}
    """
    from pykrx import stock

    _ensure_dirs()
    end_date = datetime.now().strftime("%Y%m%d")
    start_date = (datetime.now() - timedelta(days=months * 30)).strftime("%Y%m%d")

    results = {}
    for i, code in enumerate(codes):
        cache_file = SHORT_DIR / f"{code}_short_bal.csv"

        if not force and cache_file.exists():
            cached = pd.read_csv(cache_file, index_col=0, parse_dates=True)
            if len(cached) > 0:
                days_old = (datetime.now() - cached.index[-1].to_pydatetime().replace(tzinfo=None)).days
                if days_old <= 3:
                    results[code] = cached
                    continue

        print(f"  [{i+1}/{len(codes)}] {code} 공매도 잔고 수집중...")

        try:
            df = stock.get_shorting_balance_by_date(start_date, end_date, code)
            if df is None or len(df) == 0:
                continue

            # 컬럼 정리
            df.columns = ["공매도잔고", "상장주식수", "공매도금액", "시가총액", "비중"]

            if len(df) > 0:
                df.to_csv(cache_file)
                results[code] = df

            time.sleep(0.3)

        except Exception as e:
            logger.warning(f"공매도 잔고 수집 실패 {code}: {e}")
            continue

    print(f"  공매도 잔고 수집 완료: {len(results)}종목")
    return results


# ============================================================
#  2순위: 공매도 거래량 (일별)
# ============================================================

def collect_short_volume(
    codes: List[str],
    months: int = 24,
    force: bool = False,
) -> Dict[str, pd.DataFrame]:
    """공매도 거래량/거래대금 수집

    Returns: {code: DataFrame(date index)}
    """
    from pykrx import stock

    _ensure_dirs()
    end_date = datetime.now().strftime("%Y%m%d")
    start_date = (datetime.now() - timedelta(days=months * 30)).strftime("%Y%m%d")

    results = {}
    for i, code in enumerate(codes):
        cache_file = SHORT_DIR / f"{code}_short_vol.csv"

        if not force and cache_file.exists():
            cached = pd.read_csv(cache_file, index_col=0, parse_dates=True)
            if len(cached) > 0:
                days_old = (datetime.now() - cached.index[-1].to_pydatetime().replace(tzinfo=None)).days
                if days_old <= 3:
                    results[code] = cached
                    continue

        print(f"  [{i+1}/{len(codes)}] {code} 공매도 거래량 수집중...")

        try:
            df = stock.get_shorting_volume_by_date(start_date, end_date, code)
            if df is None or len(df) == 0:
                continue

            if len(df) > 0:
                df.to_csv(cache_file)
                results[code] = df

            time.sleep(0.3)

        except Exception as e:
            logger.warning(f"공매도 거래량 수집 실패 {code}: {e}")
            continue

    print(f"  공매도 거래량 수집 완료: {len(results)}종목")
    return results


# ============================================================
#  통합 수집
# ============================================================

def collect_all_flow(
    codes: List[str] = None,
    months: int = 24,
    force: bool = False,
):
    """전체 수급 데이터 수집"""
    if codes is None:
        from data.kis_collector import UNIVERSE
        codes = list(UNIVERSE.keys())

    print("=" * 60)
    print("  수급 데이터 수집기 (pykrx)")
    print(f"  종목: {len(codes)}개 | 기간: {months}개월")
    print("=" * 60)

    # 1. 투자자별 순매수
    print(f"\n[1/4] 투자자별 순매수 (외국인/기관)...")
    investor = collect_investor_flow(codes, months, force)

    # 2. 외국인 소진율
    print(f"\n[2/4] 외국인 소진율...")
    foreign_exh = collect_foreign_exhaustion(codes, months, force)

    # 3. 공매도 잔고
    print(f"\n[3/4] 공매도 잔고...")
    short_bal = collect_short_balance(codes, months, force)

    # 4. 공매도 거래량
    print(f"\n[4/4] 공매도 거래량...")
    short_vol = collect_short_volume(codes, months, force)

    print(f"\n{'='*60}")
    print(f"  수급 데이터 수집 완료")
    print(f"  투자자별 수급: {len(investor)}종목")
    print(f"  외국인 소진율: {len(foreign_exh)}종목")
    print(f"  공매도 잔고:   {len(short_bal)}종목")
    print(f"  공매도 거래량: {len(short_vol)}종목")
    print(f"{'='*60}")

    return {
        "investor": investor,
        "foreign_exhaustion": foreign_exh,
        "short_balance": short_bal,
        "short_volume": short_vol,
    }


# ============================================================
#  빠른 조회 유틸
# ============================================================

def load_investor_flow(code: str) -> Optional[pd.DataFrame]:
    """캐시된 투자자별 수급 로드"""
    path = FLOW_DIR / f"{code}_investor.csv"
    if path.exists():
        return pd.read_csv(path, index_col=0, parse_dates=True)
    return None


def load_foreign_exhaustion(code: str) -> Optional[pd.DataFrame]:
    """캐시된 외국인 소진율 로드"""
    path = FLOW_DIR / f"{code}_foreign_exh.csv"
    if path.exists():
        return pd.read_csv(path, index_col=0, parse_dates=True)
    return None


def load_short_balance(code: str) -> Optional[pd.DataFrame]:
    """캐시된 공매도 잔고 로드"""
    path = SHORT_DIR / f"{code}_short_bal.csv"
    if path.exists():
        return pd.read_csv(path, index_col=0, parse_dates=True)
    return None


# ============================================================
#  CLI
# ============================================================

if __name__ == "__main__":
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
    logging.basicConfig(level=logging.WARNING)

    # 주요 종목만 수집 (전체는 시간 오래 걸림)
    top_codes = [
        "005930", "000660", "005380", "000270", "006400",
        "051910", "003670", "247540", "086520", "012330",
        "028260", "032830", "035420", "035720", "066570",
    ]

    collect_all_flow(codes=top_codes, months=24, force=False)
