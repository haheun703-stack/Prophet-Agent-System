"""
KIS API + yfinance 하이브리드 데이터 수집기

데이터 소스:
- KIS API: 일봉 OHLCV 24개월 (페이지네이션)
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
MIN15_DIR = DATA_DIR / "15min"
MIN1_DIR = DATA_DIR / "1min"


# 동적 유니버스 (시총 1조+ 자동 필터, universe_builder.py에서 생성)
from data.universe_builder import get_universe_dict
UNIVERSE = get_universe_dict()


def _ensure_dirs():
    DAILY_DIR.mkdir(parents=True, exist_ok=True)
    MIN5_DIR.mkdir(parents=True, exist_ok=True)
    MIN15_DIR.mkdir(parents=True, exist_ok=True)
    MIN1_DIR.mkdir(parents=True, exist_ok=True)


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
    months: int = 24,
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
#  KIS 당일 1분봉 → 5분봉 + 15분봉 수집 (매일 누적)
# ============================================================

def _fetch_1min_safe(broker, code: str) -> Optional[pd.DataFrame]:
    """KIS 당일 1분봉 수집 — rate limit 포함 자체 페이징

    mojito의 fetch_today_1m_ohlcv 내부 루프에 sleep이 없어서
    직접 _fetch_today_1m_ohlcv를 호출하며 rate limit 적용
    """
    now = datetime.now()
    to = now.strftime("%H%M%S")
    if to > "153000":
        to = "153000"

    all_rows = []
    for page in range(20):  # 최대 20페이지 (안전장치)
        try:
            resp = broker._fetch_today_1m_ohlcv(code, to)
            data = resp.get("output2", [])
            if not data:
                break

            all_rows.extend(data)

            last_hour = data[-1].get("stck_cntg_hour", "090000")
            if last_hour <= "090100":
                break

            # 다음 페이지: 마지막 시각 - 1분
            h, m = int(last_hour[:2]), int(last_hour[2:4])
            dt = datetime(now.year, now.month, now.day, h, m) - timedelta(minutes=1)
            to = dt.strftime("%H%M%S")

            time.sleep(0.08)  # KIS rate limit (초당 ~12회)

        except Exception as e:
            logger.warning(f"[{code}] 1분봉 페이지 {page} 실패: {e}")
            break

    if not all_rows:
        return None

    return _parse_1min_data(all_rows)


def _resample_minutes(df_1m: pd.DataFrame, freq: str) -> Optional[pd.DataFrame]:
    """1분봉 → N분봉 리샘플링"""
    if df_1m is None or len(df_1m) < 2:
        return None

    df = df_1m.resample(freq).agg({
        "open": "first",
        "high": "max",
        "low": "min",
        "close": "last",
        "volume": "sum",
    }).dropna()

    # 장중만 (09:00 ~ 15:25)
    df = df.between_time("09:00", "15:25")
    return df if len(df) > 0 else None


def _append_csv(df: pd.DataFrame, filepath: Path):
    """기존 CSV에 append (중복 제거, 날짜순 정렬)

    기존 yfinance 데이터(UTC)와 KIS 데이터(naive KST) 혼합 처리:
    모든 데이터를 timezone-naive KST로 통일
    """
    if filepath.exists():
        existing = pd.read_csv(filepath, index_col=0, parse_dates=True)
        # 기존 데이터가 tz-aware(UTC)이면 KST 변환 후 tz 제거
        if existing.index.tz is not None:
            existing.index = existing.index.tz_convert("Asia/Seoul").tz_localize(None)
        combined = pd.concat([existing, df])
        combined = combined[~combined.index.duplicated(keep="last")]
        combined = combined.sort_index()
        combined.to_csv(filepath)
    else:
        df.to_csv(filepath)


def collect_today_minutes(
    codes: List[str] = None,
    save_1min: bool = True,
) -> Dict[str, dict]:
    """KIS API 당일 1분봉 수집 → 5분봉 + 15분봉 리샘플 + CSV 누적

    매일 장 마감 후(15:40) 실행하여 분봉 데이터 축적.

    Args:
        codes: 종목코드 리스트 (None이면 전체 UNIVERSE)
        save_1min: True면 원본 1분봉도 저장

    Returns: {code: {"1min": n, "5min": n, "15min": n}}
    """
    _ensure_dirs()
    broker = _get_broker()

    if codes is None:
        codes = list(UNIVERSE.keys())

    results = {}
    ok_count = 0
    fail_count = 0

    for i, code in enumerate(codes):
        name = UNIVERSE.get(code, (code,))[0]

        try:
            df_1m = _fetch_1min_safe(broker, code)
            if df_1m is None or len(df_1m) < 5:
                fail_count += 1
                continue

            stats = {"1min": len(df_1m), "5min": 0, "15min": 0}

            # 1분봉 저장
            if save_1min:
                _append_csv(df_1m, MIN1_DIR / f"{code}.csv")

            # 5분봉
            df_5m = _resample_minutes(df_1m, "5min")
            if df_5m is not None:
                _append_csv(df_5m, MIN5_DIR / f"{code}.csv")
                stats["5min"] = len(df_5m)

            # 15분봉
            df_15m = _resample_minutes(df_1m, "15min")
            if df_15m is not None:
                _append_csv(df_15m, MIN15_DIR / f"{code}.csv")
                stats["15min"] = len(df_15m)

            results[code] = stats
            ok_count += 1

            if (i + 1) % 20 == 0:
                logger.info(f"분봉 수집 진행: {i+1}/{len(codes)} ({ok_count}성공)")

            time.sleep(0.2)  # 종목 간 대기

        except Exception as e:
            logger.warning(f"분봉 수집 실패 {code}({name}): {e}")
            fail_count += 1
            continue

    logger.info(f"분봉 수집 완료: {ok_count}성공 / {fail_count}실패 / {len(codes)}종목")
    return results


# 하위호환 별칭
def collect_today_5min_kis(codes=None):
    """기존 호환용 — collect_today_minutes() 사용 권장"""
    return collect_today_minutes(codes, save_1min=False)


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

def collect_all(months: int = 24, force: bool = False):
    """전체 데이터 수집 (일봉 + 5분봉/15분봉)"""
    print("=" * 60)
    print("  하이브리드 데이터 수집기")
    print(f"  KIS 일봉: {months}개월 | KIS 분봉: 당일 (5분/15분)")
    print("=" * 60)

    codes = list(UNIVERSE.keys())

    # 1. KIS 일봉
    print(f"\n[1/2] KIS API 일봉 수집 ({months}개월)...")
    daily_data = collect_daily_kis(
        codes=codes,
        months=months,
        force=force,
    )

    # 2. KIS 당일 분봉 (5분 + 15분)
    print(f"\n[2/2] KIS API 당일 분봉 수집 (5분+15분)...")
    minute_results = collect_today_minutes(codes=codes)

    print(f"\n{'='*60}")
    print(f"  수집 결과")
    print(f"  일봉: {len(daily_data)}종목")
    if daily_data:
        all_dates = set()
        for df in daily_data.values():
            all_dates.update(df.index.date)
        print(f"  일봉 기간: {min(all_dates)} ~ {max(all_dates)} ({len(all_dates)}거래일)")

    print(f"  분봉: {len(minute_results)}종목 (5분+15분)")
    print(f"{'='*60}")

    return daily_data, minute_results


if __name__ == "__main__":
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
    logging.basicConfig(level=logging.WARNING)

    if "--minutes" in sys.argv:
        # 분봉만 수집
        print("분봉 수집 시작...")
        results = collect_today_minutes()
        print(f"\n수집 완료: {len(results)}종목")
        for code, stats in list(results.items())[:5]:
            name = UNIVERSE.get(code, (code,))[0]
            print(f"  {name}({code}): 1분={stats['1min']}봉 5분={stats['5min']}봉 15분={stats['15min']}봉")
    else:
        daily, minutes = collect_all(months=24, force=False)
