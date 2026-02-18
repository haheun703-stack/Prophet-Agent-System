# -*- coding: utf-8 -*-
"""
네이버 금융 분봉 수집기 → 5분봉 리샘플링
pykrx는 분봉 미지원 → 네이버 차트 API 직접 사용
"""

import re
import time
import logging
from pathlib import Path
from typing import Optional

import requests
import pandas as pd

logger = logging.getLogger(__name__)

DATA_DIR = Path(__file__).resolve().parent.parent / "data_store" / "minute5"
NAVER_URL = "https://fchart.stock.naver.com/siseJson.nhn"
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}


def _ensure_dir():
    DATA_DIR.mkdir(parents=True, exist_ok=True)


def fetch_minute_naver(code: str, start: str, end: str,
                       count: int = 50000) -> Optional[pd.DataFrame]:
    """네이버 금융에서 1분봉(close+volume) 수집

    Args:
        code: 종목코드 (예: '005930')
        start: 시작일 YYYYMMDD
        end: 종료일 YYYYMMDD
        count: 최대 행 수 (50000 ≈ 약 3개월)
    """
    params = {
        "symbol": code,
        "requestType": 1,
        "startTime": start,
        "endTime": end,
        "timeframe": "minute",
        "count": count,
    }

    try:
        r = requests.get(NAVER_URL, params=params, headers=HEADERS, timeout=30)
        r.raise_for_status()
    except Exception as e:
        logger.error(f"[{code}] 네이버 분봉 요청 실패: {e}")
        return None

    # 파싱
    pattern = r'\["(\d+)",\s*([\d.]+|null),\s*([\d.]+|null),\s*([\d.]+|null),\s*([\d.]+|null),\s*([\d.]+|null)'
    matches = re.findall(pattern, r.text)

    if not matches:
        logger.warning(f"[{code}] 분봉 데이터 없음")
        return None

    rows = []
    for m in matches:
        close_str = m[4]
        vol_str = m[5]
        if close_str == "null" or vol_str == "null":
            continue
        rows.append({
            "timestamp": m[0],
            "close": float(close_str),
            "volume": float(vol_str),
        })

    if not rows:
        return None

    df = pd.DataFrame(rows)
    df["datetime"] = pd.to_datetime(df["timestamp"], format="%Y%m%d%H%M")
    df = df.sort_values("datetime").set_index("datetime")
    df = df.drop(columns=["timestamp"])

    return df


def resample_to_5min(df_1m: pd.DataFrame) -> pd.DataFrame:
    """1분봉(close+volume) → 5분봉 OHLCV 리샘플링"""
    df5 = df_1m["close"].resample("5min").agg(
        open="first", high="max", low="min", close="last"
    ).dropna()

    # 거래량: 누적 거래량의 diff → 구간 거래량
    df5["volume"] = df_1m["volume"].resample("5min").last().diff()
    # 첫 봉 거래량 보정
    df5["volume"] = df5["volume"].clip(lower=0).fillna(0)

    # 장중만 (09:00 ~ 15:30)
    df5 = df5.between_time("09:00", "15:25")

    return df5


def collect_5min(codes: list, start: str, end: str,
                 force: bool = False, sleep: float = 1.0):
    """주요 종목 5분봉 수집 + 저장

    Args:
        codes: 종목코드 리스트
        start: 시작일 YYYYMMDD
        end: 종료일 YYYYMMDD
        force: True면 캐시 무시
        sleep: 요청 간 대기 (초)
    """
    _ensure_dir()
    collected = 0

    for i, code in enumerate(codes):
        cache_file = DATA_DIR / f"{code}.csv"

        if not force and cache_file.exists():
            cached = pd.read_csv(cache_file, index_col=0, parse_dates=True)
            if len(cached) > 100:
                logger.info(f"[{i+1}/{len(codes)}] {code} 캐시 사용 ({len(cached)}행)")
                continue

        print(f"  5분봉 [{i+1}/{len(codes)}] {code}...", end=" ", flush=True)

        df_1m = fetch_minute_naver(code, start, end)
        if df_1m is None or len(df_1m) < 10:
            print(f"데이터 부족")
            continue

        df_5m = resample_to_5min(df_1m)
        if df_5m.empty:
            print(f"리샘플링 실패")
            continue

        df_5m.to_csv(cache_file)
        collected += 1

        days = df_5m.index.normalize().nunique()
        print(f"{len(df_5m)}봉 ({days}일)")

        time.sleep(sleep)

    print(f"\n  5분봉 수집 완료: {collected}/{len(codes)} 종목")
    return collected


if __name__ == "__main__":
    import sys
    sys.path.insert(0, "D:/Prophet_Agent_System_예언자/scalper-agent")

    # 주요 종목 5개 테스트
    test_codes = ["005930", "000660", "005380", "035420", "068270"]
    collect_5min(test_codes, "20251115", "20260213", force=True)
