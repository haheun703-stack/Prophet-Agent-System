"""
KIS API + yfinance 하이브리드 데이터 수집기

데이터 소스:
- KIS API: 일봉 OHLCV 6개월+ (페이지네이션)
- yfinance: 5분봉 OHLCV ~60일
- KIS API: 당일 1분봉 (실시간 수집용)

수집된 데이터는 로컬 CSV 파일로 저장하여 재사용
"""

import os
import sys
import time
import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

# 데이터 저장 경로
DATA_DIR = Path(__file__).resolve().parent.parent / "data_store"
DAILY_DIR = DATA_DIR / "daily"
MIN5_DIR = DATA_DIR / "5min"


# 거래량 상위 종목 (KOSPI + KOSDAQ)
UNIVERSE = {
    # KOSPI
    "005930": ("삼성전자", ".KS", "J"),
    "000660": ("SK하이닉스", ".KS", "J"),
    "005380": ("현대차", ".KS", "J"),
    "035420": ("NAVER", ".KS", "J"),
    "000270": ("기아", ".KS", "J"),
    "006400": ("삼성SDI", ".KS", "J"),
    "051910": ("LG화학", ".KS", "J"),
    "035720": ("카카오", ".KS", "J"),
    "003670": ("포스코퓨처엠", ".KS", "J"),
    "028260": ("삼성물산", ".KS", "J"),
    "066570": ("LG전자", ".KS", "J"),
    "003550": ("LG", ".KS", "J"),
    "055550": ("신한지주", ".KS", "J"),
    "105560": ("KB금융", ".KS", "J"),
    "086790": ("하나금융지주", ".KS", "J"),
    "096770": ("SK이노베이션", ".KS", "J"),
    "032830": ("삼성생명", ".KS", "J"),
    "017670": ("SK텔레콤", ".KS", "J"),
    "030200": ("KT", ".KS", "J"),
    "010950": ("S-Oil", ".KS", "J"),
    "005830": ("DB손해보험", ".KS", "J"),
    "034730": ("SK", ".KS", "J"),
    "036570": ("엔씨소프트", ".KS", "J"),
    "012330": ("현대모비스", ".KS", "J"),
    "011200": ("HMM", ".KS", "J"),
    "009540": ("HD한국조선해양", ".KS", "J"),
    "012450": ("한화에어로스페이스", ".KS", "J"),
    "011170": ("롯데케미칼", ".KS", "J"),
    # KOSDAQ
    "247540": ("에코프로비엠", ".KQ", "Q"),
    "086520": ("에코프로", ".KQ", "Q"),
    "403870": ("HPSP", ".KQ", "Q"),
    "041510": ("에스엠", ".KQ", "Q"),
    "293490": ("카카오게임즈", ".KQ", "Q"),
    # ETF
    "069500": ("KODEX200", ".KS", "J"),
    # 그룹 순환매 ETF
    "371160": ("KODEX현대차그룹플러스", ".KS", "J"),
    "102780": ("KODEX삼성그룹", ".KS", "J"),
    "305720": ("KODEX2차전지산업", ".KS", "J"),
    # 그룹 순환매 구성종목 (기존에 없는 것만)
    "018880": ("한온시스템", ".KS", "J"),
    "011210": ("현대위아", ".KS", "J"),
}


def _ensure_dirs():
    DAILY_DIR.mkdir(parents=True, exist_ok=True)
    MIN5_DIR.mkdir(parents=True, exist_ok=True)


def _get_broker():
    """KIS API 브로커 생성"""
    from dotenv import load_dotenv
    load_dotenv()
    import mojito

    broker = mojito.KoreaInvestment(
        api_key=os.getenv("KIS_APP_KEY"),
        api_secret=os.getenv("KIS_APP_SECRET"),
        acc_no=os.getenv("KIS_ACC_NO"),
        mock=False,
    )
    return broker


# ============================================================
#  KIS API 일봉 수집
# ============================================================

def collect_daily_kis(
    codes: List[str] = None,
    months: int = 8,
    force: bool = False,
) -> Dict[str, pd.DataFrame]:
    """KIS API에서 일봉 데이터 수집 (6개월+)

    Returns: {code: DataFrame(date index, OHLCV)}
    """
    _ensure_dirs()
    broker = _get_broker()

    if codes is None:
        codes = list(UNIVERSE.keys())

    today = datetime.now().strftime("%Y%m%d")
    start = (datetime.now() - timedelta(days=months * 30)).strftime("%Y%m%d")

    results = {}
    for i, code in enumerate(codes):
        cache_file = DAILY_DIR / f"{code}.csv"

        # 캐시 확인 (오늘 수집된 것이면 재사용)
        if not force and cache_file.exists():
            cached = pd.read_csv(cache_file, index_col=0, parse_dates=True)
            if len(cached) > 0:
                days_old = (datetime.now() - cached.index[-1].to_pydatetime().replace(tzinfo=None)).days
                if days_old <= 3:
                    results[code] = cached
                    continue

        name = UNIVERSE.get(code, (code,))[0]
        print(f"  [{i+1}/{len(codes)}] {code}({name}) 일봉 수집중...")

        all_rows = []
        end_day = today

        for page in range(10):  # 최대 10페이지 (1000거래일)
            try:
                resp = broker.fetch_ohlcv_domestic(
                    symbol=code,
                    timeframe="D",
                    start_day=start,
                    end_day=end_day,
                )
                data = resp.get("output2", [])
                if not data:
                    break

                all_rows.extend(data)

                # 마지막 날짜가 시작일 이전이면 종료
                last_date = data[-1]["stck_bsop_date"]
                if last_date <= start:
                    break

                # 다음 페이지
                dt = datetime.strptime(last_date, "%Y%m%d") - timedelta(days=1)
                end_day = dt.strftime("%Y%m%d")
                time.sleep(0.15)  # API 속도 제한

            except Exception as e:
                logger.warning(f"KIS API error for {code}: {e}")
                break

        if not all_rows:
            continue

        # DataFrame 변환
        df = _parse_daily_data(all_rows)
        if df is not None and len(df) > 0:
            df.to_csv(cache_file)
            results[code] = df

        # API 속도 제한 (초당 1회)
        time.sleep(0.2)

    print(f"  일봉 수집 완료: {len(results)}종목")
    return results


def _parse_daily_data(rows: List[dict]) -> Optional[pd.DataFrame]:
    """KIS 일봉 응답 → DataFrame 변환"""
    records = []
    seen = set()
    for r in rows:
        dt = r.get("stck_bsop_date", "")
        if not dt or dt in seen:
            continue
        seen.add(dt)

        try:
            records.append({
                "date": pd.Timestamp(dt),
                "open": int(r.get("stck_oprc", 0)),
                "high": int(r.get("stck_hgpr", 0)),
                "low": int(r.get("stck_lwpr", 0)),
                "close": int(r.get("stck_clpr", 0)),
                "volume": int(r.get("acml_vol", 0)),
            })
        except (ValueError, TypeError):
            continue

    if not records:
        return None

    df = pd.DataFrame(records)
    df = df.set_index("date").sort_index()
    df = df[df["volume"] > 0]  # 거래량 0 제외
    return df


# ============================================================
#  yfinance 5분봉 수집
# ============================================================

def collect_5min_yfinance(
    codes: List[str] = None,
    force: bool = False,
) -> Tuple[pd.DataFrame, Dict[str, pd.DataFrame]]:
    """yfinance에서 5분봉 데이터 수집 (~60일)

    Returns: (etf_df, {code: DataFrame})
    """
    import yfinance as yf

    _ensure_dirs()

    if codes is None:
        codes = [c for c in UNIVERSE.keys() if c != "069500"]

    etf_cache = MIN5_DIR / "069500.csv"
    etf_df = None

    # ETF (KODEX200)
    if not force and etf_cache.exists():
        cached = pd.read_csv(etf_cache, index_col=0, parse_dates=True)
        days_old = (datetime.now() - cached.index[-1].to_pydatetime().replace(tzinfo=None)).days
        if days_old <= 3:
            etf_df = cached

    if etf_df is None:
        print("  KODEX200 5분봉 다운로드...")
        etf_raw = yf.download("069500.KS", period="60d", interval="5m", progress=False)
        if isinstance(etf_raw.columns, pd.MultiIndex):
            etf_raw.columns = etf_raw.columns.get_level_values(0)
        etf_df = etf_raw.rename(columns={
            "Open": "open", "High": "high", "Low": "low",
            "Close": "close", "Volume": "volume",
        })
        etf_df = etf_df.dropna(subset=["close"])
        if len(etf_df) > 0:
            etf_df.to_csv(etf_cache)
        print(f"    {len(etf_df)}봉 ({etf_df.index[0].date()} ~ {etf_df.index[-1].date()})")

    # 개별 종목 (bulk download)
    yf_tickers = []
    code_list = []
    for code in codes:
        info = UNIVERSE.get(code)
        if info:
            yf_tickers.append(f"{code}{info[1]}")
            code_list.append(code)

    stock_data = {}
    cached_count = 0

    # 캐시 확인
    if not force:
        for code in code_list:
            cache_file = MIN5_DIR / f"{code}.csv"
            if cache_file.exists():
                cached = pd.read_csv(cache_file, index_col=0, parse_dates=True)
                days_old = (datetime.now() - cached.index[-1].to_pydatetime().replace(tzinfo=None)).days
                if days_old <= 3 and len(cached) > 50:
                    stock_data[code] = cached
                    cached_count += 1

    # 캐시에 없는 종목만 다운로드
    missing = [c for c in code_list if c not in stock_data]
    if missing:
        missing_tickers = [f"{c}{UNIVERSE[c][1]}" for c in missing]
        tickers_str = " ".join(missing_tickers)
        print(f"  개별종목 5분봉 다운로드 ({len(missing)}종목)...")

        bulk = yf.download(tickers_str, period="60d", interval="5m", progress=False, group_by="ticker")

        for code in missing:
            yf_ticker = f"{code}{UNIVERSE[code][1]}"
            try:
                if isinstance(bulk.columns, pd.MultiIndex):
                    df = bulk[yf_ticker].copy()
                else:
                    df = bulk.copy()
                df = df.rename(columns={
                    "Open": "open", "High": "high", "Low": "low",
                    "Close": "close", "Volume": "volume",
                })
                df = df.dropna(subset=["close"])
                if len(df) > 50:
                    cache_file = MIN5_DIR / f"{code}.csv"
                    df.to_csv(cache_file)
                    stock_data[code] = df
            except (KeyError, Exception):
                continue

    print(f"  5분봉 수집 완료: {len(stock_data)}종목 (캐시 {cached_count}개)")
    return etf_df, stock_data


# ============================================================
#  KIS 당일 1분봉 → 5분봉 변환 수집
# ============================================================

def collect_today_5min_kis(
    codes: List[str] = None,
) -> Dict[str, pd.DataFrame]:
    """KIS API 당일 1분봉 → 5분봉으로 집계

    향후 매일 실행하여 분봉 데이터 축적용
    """
    _ensure_dirs()
    broker = _get_broker()

    if codes is None:
        codes = list(UNIVERSE.keys())

    results = {}
    for code in codes:
        try:
            resp = broker.fetch_today_1m_ohlcv(code)
            data = resp.get("output2", [])
            if not data:
                continue

            df_1m = _parse_1min_data(data)
            if df_1m is None or len(df_1m) < 5:
                continue

            # 1분봉 → 5분봉 리샘플
            df_5m = df_1m.resample("5min").agg({
                "open": "first",
                "high": "max",
                "low": "min",
                "close": "last",
                "volume": "sum",
            }).dropna()

            if len(df_5m) > 0:
                results[code] = df_5m
                # 기존 파일에 append
                cache_file = MIN5_DIR / f"{code}.csv"
                if cache_file.exists():
                    existing = pd.read_csv(cache_file, index_col=0, parse_dates=True)
                    combined = pd.concat([existing, df_5m])
                    combined = combined[~combined.index.duplicated(keep="last")]
                    combined = combined.sort_index()
                    combined.to_csv(cache_file)
                else:
                    df_5m.to_csv(cache_file)

            time.sleep(0.15)

        except Exception as e:
            logger.warning(f"1분봉 수집 실패 {code}: {e}")
            continue

    print(f"  당일 5분봉 수집: {len(results)}종목")
    return results


def _parse_1min_data(rows: List[dict]) -> Optional[pd.DataFrame]:
    """KIS 1분봉 → DataFrame"""
    records = []
    for r in rows:
        dt_str = r.get("stck_bsop_date", "")
        tm_str = r.get("stck_cntg_hour", "")
        if not dt_str or not tm_str:
            continue

        try:
            ts = pd.Timestamp(f"{dt_str[:4]}-{dt_str[4:6]}-{dt_str[6:8]} "
                              f"{tm_str[:2]}:{tm_str[2:4]}:{tm_str[4:6]}")
            records.append({
                "datetime": ts,
                "open": int(r.get("stck_oprc", 0)),
                "high": int(r.get("stck_hgpr", 0)),
                "low": int(r.get("stck_lwpr", 0)),
                "close": int(r.get("stck_prpr", 0)),
                "volume": int(r.get("cntg_vol", 0)),
            })
        except (ValueError, TypeError):
            continue

    if not records:
        return None

    df = pd.DataFrame(records)
    df = df.set_index("datetime").sort_index()
    return df


# ============================================================
#  통합 수집
# ============================================================

def collect_all(months: int = 8, force: bool = False):
    """전체 데이터 수집 (일봉 + 5분봉)"""
    print("=" * 60)
    print("  하이브리드 데이터 수집기")
    print(f"  KIS 일봉: {months}개월 | yfinance 5분봉: ~60일")
    print("=" * 60)

    codes = [c for c in UNIVERSE.keys() if c != "069500"]

    # 1. KIS 일봉
    print(f"\n[1/2] KIS API 일봉 수집 ({months}개월)...")
    daily_data = collect_daily_kis(
        codes=list(UNIVERSE.keys()),
        months=months,
        force=force,
    )

    # 2. yfinance 5분봉
    print(f"\n[2/2] yfinance 5분봉 수집 (~60일)...")
    etf_df, stock_5min = collect_5min_yfinance(
        codes=codes,
        force=force,
    )

    print(f"\n{'='*60}")
    print(f"  수집 결과")
    print(f"  일봉: {len(daily_data)}종목")
    if daily_data:
        all_dates = set()
        for df in daily_data.values():
            all_dates.update(df.index.date)
        print(f"  일봉 기간: {min(all_dates)} ~ {max(all_dates)} ({len(all_dates)}거래일)")

    print(f"  5분봉: {len(stock_5min)}종목")
    if etf_df is not None:
        print(f"  ETF 5분봉: {len(etf_df)}봉")
    print(f"{'='*60}")

    return daily_data, etf_df, stock_5min


if __name__ == "__main__":
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
    logging.basicConfig(level=logging.WARNING)

    daily, etf, stocks = collect_all(months=8, force=False)
