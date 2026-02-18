# -*- coding: utf-8 -*-
"""
일일 5분봉 수집기 — 장 마감 후 실행
=====================================
KIS API로 현대차+NAVER 당일 1분봉 → 5분봉 변환 후 저장

사용법:
  python collect_daily.py              # 당일 수집
  python collect_daily.py --all        # UNIVERSE 전종목 수집

저장 위치:
  data_store/minute5/{code}.csv   (백테스트용 — real_5min_backtest.py)
  data_store/5min/{code}.csv      (수급분석용 — kis_collector.py)

Windows 작업 스케줄러 등록:
  schtasks /create /tn "BH_DailyCollect" /tr "python D:/Prophet_Agent_System/scalper-agent/collect_daily.py" /sc daily /st 15:40
"""

import sys
import os
import io
import time
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
load_dotenv(ROOT.parent / ".env")

import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("DailyCollector")

# 저장 경로 (백테스트 + 수급분석 양쪽)
MINUTE5_DIR = ROOT / "data_store" / "minute5"
MIN5_ALT_DIR = ROOT / "data_store" / "5min"

# 백테스트 검증 종목 (WR >= 40%)
QUALIFIED = {"005380": "현대차", "035420": "NAVER"}


def _ensure_dirs():
    MINUTE5_DIR.mkdir(parents=True, exist_ok=True)
    MIN5_ALT_DIR.mkdir(parents=True, exist_ok=True)


def _get_broker():
    import mojito
    return mojito.KoreaInvestment(
        api_key=os.getenv("KIS_APP_KEY"),
        api_secret=os.getenv("KIS_APP_SECRET"),
        acc_no=os.getenv("KIS_ACC_NO"),
        mock=False,
    )


def _parse_1min(rows: list) -> Optional[pd.DataFrame]:
    """KIS 1분봉 응답 → DataFrame"""
    records = []
    for r in rows:
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
        return None
    df = pd.DataFrame(records).set_index("datetime").sort_index()
    return df


def _append_to_csv(df_5m: pd.DataFrame, path: Path):
    """기존 CSV에 append (중복 제거)"""
    if path.exists():
        existing = pd.read_csv(path, index_col=0, parse_dates=True)
        # tz-aware/naive 통일
        if existing.index.tz is not None:
            existing.index = existing.index.tz_localize(None)
        if df_5m.index.tz is not None:
            df_5m.index = df_5m.index.tz_localize(None)
        combined = pd.concat([existing, df_5m])
        combined = combined[~combined.index.duplicated(keep="last")]
        combined = combined.sort_index()
        combined.to_csv(path)
        return len(combined)
    else:
        df_5m.to_csv(path)
        return len(df_5m)


def collect(codes: Dict[str, str]) -> int:
    """지정 종목 1분봉 → 5분봉 수집 + 양쪽 디렉토리 저장"""
    _ensure_dirs()
    broker = _get_broker()
    collected = 0

    for code, name in codes.items():
        try:
            logger.info(f"[{code}] {name} 1분봉 조회중...")
            resp = broker.fetch_today_1m_ohlcv(code)
            data = resp.get("output2", [])
            if not data:
                logger.warning(f"[{code}] 데이터 없음")
                continue

            df_1m = _parse_1min(data)
            if df_1m is None or len(df_1m) < 5:
                logger.warning(f"[{code}] 1분봉 부족 ({len(df_1m) if df_1m is not None else 0}봉)")
                continue

            # 5분봉 리샘플링
            df_5m = df_1m.resample("5min").agg({
                "open": "first",
                "high": "max",
                "low": "min",
                "close": "last",
                "volume": "sum",
            }).dropna()

            # 장중만 (09:00~15:25)
            df_5m = df_5m.between_time("09:00", "15:25")

            if df_5m.empty:
                logger.warning(f"[{code}] 5분봉 없음")
                continue

            date_str = df_5m.index[0].strftime("%Y-%m-%d")
            bars = len(df_5m)

            # 양쪽 디렉토리에 저장
            n1 = _append_to_csv(df_5m, MINUTE5_DIR / f"{code}.csv")
            n2 = _append_to_csv(df_5m, MIN5_ALT_DIR / f"{code}.csv")

            collected += 1
            logger.info(f"[{code}] {name} {date_str} {bars}봉 저장 (minute5:{n1}, 5min:{n2})")

            time.sleep(0.2)

        except Exception as e:
            logger.error(f"[{code}] 수집 실패: {e}")

    return collected


def main():
    import argparse
    parser = argparse.ArgumentParser(description="일일 5분봉 수집기")
    parser.add_argument("--all", action="store_true", help="UNIVERSE 전종목 수집")
    args = parser.parse_args()

    today = datetime.now().strftime("%Y-%m-%d %H:%M")
    print(f"\n{'='*50}")
    print(f"  일일 5분봉 수집기")
    print(f"  {today}")
    print(f"{'='*50}")

    if args.all:
        from data.kis_collector import UNIVERSE
        # ETF 제외
        exclude = {"069500", "371160", "102780", "305720"}
        codes = {c: info[0] for c, info in UNIVERSE.items() if c not in exclude}
        print(f"  전종목 수집: {len(codes)}종목")
    else:
        codes = QUALIFIED
        print(f"  대상: {', '.join(f'{n}({c})' for c, n in codes.items())}")

    print()
    n = collect(codes)
    print(f"\n  수집 완료: {n}/{len(codes)}종목")

    # 현재 축적 상황 출력
    print(f"\n  축적 현황 (minute5/):")
    for code, name in QUALIFIED.items():
        path = MINUTE5_DIR / f"{code}.csv"
        if path.exists():
            df = pd.read_csv(path, index_col=0, parse_dates=True)
            days = df.index.normalize().nunique()
            print(f"    {name}({code}): {len(df)}봉 | {days}일 | {df.index[0].date()} ~ {df.index[-1].date()}")
        else:
            print(f"    {name}({code}): 데이터 없음")

    print(f"{'='*50}\n")


if __name__ == "__main__":
    main()
