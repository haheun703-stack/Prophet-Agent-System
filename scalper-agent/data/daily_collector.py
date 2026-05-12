"""
매일 자동 분봉 데이터 수집기

매일 장 마감 후 실행하면 당일 5분봉을 축적합니다.
데이터 소스 우선순위:
  1. KIS API: 당일 1분봉 → 5분봉 리샘플 (가장 정확)
  2. 네이버 증권: 최근 6거래일 5분봉 (빠져있는 날 보충)
  3. yfinance: ~60일 5분봉 (초기 백필용)

사용법:
  python daily_collector.py              # 오늘 데이터 수집
  python daily_collector.py --backfill   # yfinance 60일 + 네이버 6일 백필
  python daily_collector.py --status     # 수집 현황 확인

자동화:
  Windows 작업 스케줄러에 등록:
  매일 16:00 실행 → python daily_collector.py
"""

import os
import sys
import io
import time
import json
import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import pandas as pd
import numpy as np
import requests

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

logger = logging.getLogger(__name__)

# ============================================================
#  설정
# ============================================================

DATA_DIR = Path(__file__).resolve().parent.parent / "data_store"
MIN5_DIR = DATA_DIR / "5min"
MIN15_DIR = DATA_DIR / "15min"
LOG_FILE = DATA_DIR / "collection_log.csv"

# 동적 유니버스 (시총 1조+ 자동 필터)
from data.universe_builder import get_universe_dict
UNIVERSE = get_universe_dict()


def _ensure_dirs():
    MIN5_DIR.mkdir(parents=True, exist_ok=True)
    MIN15_DIR.mkdir(parents=True, exist_ok=True)


def _load_existing(code: str) -> Optional[pd.DataFrame]:
    """기존 CSV 로드"""
    f = MIN5_DIR / f"{code}.csv"
    if f.exists():
        try:
            df = pd.read_csv(f, index_col=0, parse_dates=True)
            return df
        except Exception:
            return None
    return None


def _to_kst_naive(df: pd.DataFrame) -> pd.DataFrame:
    """timezone을 KST로 변환 후 tz-naive로 통일"""
    if df.index.tz is not None:
        df = df.copy()
        df.index = df.index.tz_convert("Asia/Seoul").tz_localize(None)
    return df


def _save(code: str, df: pd.DataFrame):
    """CSV 저장 (기존 데이터에 병합, KST naive로 통일)"""
    df = _to_kst_naive(df)

    existing = _load_existing(code)
    if existing is not None and len(existing) > 0:
        existing = _to_kst_naive(existing)
        combined = pd.concat([existing, df])
        combined = combined[~combined.index.duplicated(keep="last")]
        combined = combined.sort_index()
        combined.to_csv(MIN5_DIR / f"{code}.csv")
    else:
        df.sort_index().to_csv(MIN5_DIR / f"{code}.csv")


def _log_collection(source: str, codes_count: int, candles_count: int, dates: str):
    """수집 로그 기록"""
    entry = {
        "timestamp": datetime.now().isoformat(),
        "source": source,
        "codes": codes_count,
        "candles": candles_count,
        "dates": dates,
    }
    log_exists = LOG_FILE.exists()
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        if not log_exists:
            f.write("timestamp\tsource\tcodes\tcandles\tdates\n")
        f.write(f"{entry['timestamp']}\t{source}\t{codes_count}\t{candles_count}\t{dates}\n")


# ============================================================
#  소스 1: KIS API (당일 1분봉 → 5분봉)
# ============================================================

def collect_today_kis() -> int:
    """KIS API로 당일 분봉 수집"""
    from dotenv import load_dotenv
    load_dotenv()
    import mojito

    broker = mojito.KoreaInvestment(
        api_key=os.getenv("KIS_APP_KEY"),
        api_secret=os.getenv("KIS_APP_SECRET"),
        acc_no=os.getenv("KIS_ACC_NO"),
        mock=False,
    )

    total_candles = 0
    collected = 0

    for code, info in UNIVERSE.items():
        name = info[0]
        try:
            resp = broker.fetch_today_1m_ohlcv(code)
            data = resp.get("output2", [])
            if not data or len(data) < 10:
                continue

            # 1분봉 파싱
            records = []
            for r in data:
                dt_str = r.get("stck_bsop_date", "")
                tm_str = r.get("stck_cntg_hour", "")
                if not dt_str or not tm_str:
                    continue
                try:
                    ts = pd.Timestamp(
                        f"{dt_str[:4]}-{dt_str[4:6]}-{dt_str[6:8]} "
                        f"{tm_str[:2]}:{tm_str[2:4]}:{tm_str[4:6]}"
                    )
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
                continue

            df_1m = pd.DataFrame(records).set_index("datetime").sort_index()

            # 5분봉으로 리샘플
            df_5m = df_1m.resample("5min").agg({
                "open": "first", "high": "max",
                "low": "min", "close": "last",
                "volume": "sum",
            }).dropna()

            if len(df_5m) > 0:
                _save(code, df_5m)
                total_candles += len(df_5m)
                collected += 1

            time.sleep(0.15)

        except Exception as e:
            logger.warning(f"KIS 수집 실패 {code}: {e}")
            continue

    today = datetime.now().strftime("%Y-%m-%d")
    _log_collection("KIS", collected, total_candles, today)
    print(f"  [KIS] {collected}종목 {total_candles}봉 수집 완료 ({today})")
    return collected


# ============================================================
#  소스 2: 네이버 증권 (최근 6일 5분봉)
# ============================================================

def collect_naver_recent() -> int:
    """네이버 증권에서 최근 ~6일 5분봉 수집 (빠진 날짜 보충용)"""

    total_candles = 0
    collected = 0
    dates_set = set()

    for code, info in UNIVERSE.items():
        name = info[0]
        try:
            url = f"https://api.stock.naver.com/chart/domestic/item/{code}/minute5"
            headers = {"User-Agent": "Mozilla/5.0"}
            params = {
                "startDateTime": (datetime.now() - timedelta(days=10)).strftime("%Y%m%d") + "090000",
                "endDateTime": datetime.now().strftime("%Y%m%d") + "153000",
            }

            resp = requests.get(url, headers=headers, params=params, timeout=10)
            data = resp.json()

            if not isinstance(data, list) or len(data) == 0:
                continue

            records = []
            for item in data:
                dt_str = item.get("localDateTime", "")
                if len(dt_str) < 14:
                    continue
                try:
                    ts = pd.Timestamp(
                        f"{dt_str[:4]}-{dt_str[4:6]}-{dt_str[6:8]} "
                        f"{dt_str[8:10]}:{dt_str[10:12]}:{dt_str[12:14]}"
                    )
                    records.append({
                        "datetime": ts,
                        "open": float(item.get("openPrice", 0)),
                        "high": float(item.get("highPrice", 0)),
                        "low": float(item.get("lowPrice", 0)),
                        "close": float(item.get("currentPrice", 0)),
                        "volume": int(item.get("accumulatedTradingVolume", 0)),
                    })
                    dates_set.add(ts.date().isoformat())
                except (ValueError, TypeError):
                    continue

            if not records:
                continue

            df = pd.DataFrame(records).set_index("datetime").sort_index()
            if len(df) > 0:
                _save(code, df)
                total_candles += len(df)
                collected += 1

            time.sleep(0.1)

        except Exception as e:
            logger.warning(f"네이버 수집 실패 {code}: {e}")
            continue

    dates_str = ",".join(sorted(dates_set)[:3]) + "..." if dates_set else "없음"
    _log_collection("Naver", collected, total_candles, dates_str)
    print(f"  [네이버] {collected}종목 {total_candles}봉 수집 완료 ({dates_str})")
    return collected


# ============================================================
#  소스 3: yfinance (초기 백필 ~60일)
# ============================================================

def backfill_yfinance() -> int:
    """yfinance로 ~60일 5분봉 백필"""
    import yfinance as yf

    all_tickers = []
    code_list = []
    for code, info in UNIVERSE.items():
        name, suffix = info[0], info[1]
        all_tickers.append(f"{code}{suffix}")
        code_list.append(code)

    tickers_str = " ".join(all_tickers)
    print(f"  [yfinance] {len(code_list)}종목 5분봉 다운로드중...")

    bulk = yf.download(tickers_str, period="60d", interval="5m", progress=False, group_by="ticker")

    total_candles = 0
    collected = 0

    for code in code_list:
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
                _save(code, df)
                total_candles += len(df)
                collected += 1

        except (KeyError, Exception):
            continue

    dates = f"{bulk.index[0].date()}~{bulk.index[-1].date()}" if len(bulk) > 0 else "없음"
    _log_collection("yfinance", collected, total_candles, dates)
    print(f"  [yfinance] {collected}종목 {total_candles}봉 백필 완료 ({dates})")
    return collected


# ============================================================
#  수집 현황 확인
# ============================================================

def show_status():
    """데이터 축적 현황"""
    _ensure_dirs()

    print(f"\n{'='*70}")
    print(f"  [분봉] 데이터 축적 현황")
    print(f"{'='*70}")

    total_files = 0
    total_candles = 0
    earliest = None
    latest = None

    rows = []
    for code, info in sorted(UNIVERSE.items()):
        name = info[0]
        f = MIN5_DIR / f"{code}.csv"
        if not f.exists():
            rows.append((code, name, 0, "-", "-", 0))
            continue

        df = _load_existing(code)
        if df is None or len(df) == 0:
            rows.append((code, name, 0, "-", "-", 0))
            continue

        total_files += 1
        n = len(df)
        total_candles += n
        d_first = df.index[0]
        d_last = df.index[-1]
        trading_days = df.index.normalize().nunique()

        if earliest is None or d_first < earliest:
            earliest = d_first
        if latest is None or d_last > latest:
            latest = d_last

        rows.append((code, name, n, str(d_first.date()), str(d_last.date()), trading_days))

    # 요약
    print(f"  종목: {total_files}/{len(UNIVERSE)}개")
    print(f"  총 봉수: {total_candles:,}개")
    if earliest and latest:
        print(f"  기간: {earliest.date()} ~ {latest.date()}")
    print(f"{'='*70}")

    # 상세
    print(f"  {'코드':<8} {'종목명':<12} {'봉수':>8} {'시작일':>12} {'종료일':>12} {'거래일':>6}")
    print(f"  {'-'*66}")
    for code, name, n, first, last, days in rows:
        if n > 0:
            print(f"  {code:<8} {name:<12} {n:>8,} {first:>12} {last:>12} {days:>6}")
        else:
            print(f"  {code:<8} {name:<12} {'없음':>8} {'-':>12} {'-':>12} {'-':>6}")
    print(f"{'='*70}")

    # 수집 로그
    if LOG_FILE.exists():
        print(f"\n  최근 수집 로그:")
        log_df = pd.read_csv(LOG_FILE, sep="\t")
        for _, row in log_df.tail(10).iterrows():
            print(f"    {str(row['timestamp'])[:19]} [{str(row['source']):<8}] {row['codes']}종목 {row['candles']}봉 ({row['dates']})")
    print()


# ============================================================
#  메인
# ============================================================

def run_daily():
    """일일 수집 루틴"""
    _ensure_dirs()

    print(f"\n{'='*60}")
    print(f"  분봉 데이터 일일 수집")
    print(f"  시각: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"{'='*60}")

    # 1. KIS API 당일 1분봉 → 5분봉
    print(f"\n[1/3] KIS API 당일 분봉 수집...")
    try:
        collect_today_kis()
    except Exception as e:
        print(f"  [KIS] 실패: {e}")

    # 2. 네이버 최근 6일 보충
    print(f"\n[2/3] 네이버 최근 분봉 보충...")
    try:
        collect_naver_recent()
    except Exception as e:
        print(f"  [네이버] 실패: {e}")

    # 3. 15분봉 리샘플
    print(f"\n[3/3] 5분봉 → 15분봉 리샘플...")
    resample_5min_to_15min()

    print(f"\n수집 완료!")
    show_status()


def resample_5min_to_15min():
    """5분봉 → 15분봉 리샘플링 (전종목)"""
    _ensure_dirs()

    files = sorted(MIN5_DIR.glob("*.csv"))
    total = 0
    for f in files:
        code = f.stem
        try:
            df = pd.read_csv(f, index_col=0, parse_dates=True)
            if len(df) < 10:
                continue

            df15 = df.resample("15min").agg({
                "open": "first", "high": "max",
                "low": "min", "close": "last",
                "volume": "sum",
            }).dropna()

            # 장중만 (09:00 ~ 15:25)
            df15 = df15.between_time("09:00", "15:25")

            if len(df15) > 0:
                df15.to_csv(MIN15_DIR / f"{code}.csv")
                total += 1
        except Exception:
            continue

    print(f"  [15분봉] {total}/{len(files)}종목 리샘플 완료")
    return total


def run_backfill():
    """초기 백필 (yfinance 60일 + 네이버 + KIS + 15분봉)"""
    _ensure_dirs()

    print(f"\n{'='*60}")
    print(f"  분봉 데이터 백필 (초기 축적)")
    print(f"{'='*60}")

    # 1. yfinance ~60일
    print(f"\n[1/4] yfinance 60일 백필...")
    backfill_yfinance()

    # 2. 네이버 최근 6일
    print(f"\n[2/4] 네이버 최근 분봉...")
    collect_naver_recent()

    # 3. KIS 당일
    print(f"\n[3/4] KIS 당일 분봉...")
    try:
        collect_today_kis()
    except Exception as e:
        print(f"  [KIS] 건너뜀: {e}")

    # 4. 5분봉 → 15분봉 리샘플
    print(f"\n[4/4] 5분봉 → 15분봉 리샘플...")
    resample_5min_to_15min()

    print(f"\n백필 완료!")
    show_status()


if __name__ == "__main__":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
    logging.basicConfig(level=logging.WARNING)

    if "--backfill" in sys.argv:
        run_backfill()
    elif "--status" in sys.argv:
        show_status()
    elif "--resample-15m" in sys.argv:
        resample_5min_to_15min()
    else:
        run_daily()
