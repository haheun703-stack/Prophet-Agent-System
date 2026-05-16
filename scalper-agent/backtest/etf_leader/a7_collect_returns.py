# -*- coding: utf-8 -*-
"""A-7: 2025년 실제 수익률 수집 — 백테스트 정답지

목적:
  마스터 ETF 각각에 대해 25년 1월 ~ 25년 12월(현재까지) 실제 수익률 수집.
  이게 백테스트의 정답지가 된다.

지표:
  - return_ytd_pct:  2025-01-02 시가 → 최근 종가 수익률
  - return_peak_pct: 2025-01-02 시가 → 2025년 최고가 수익률 (이상적 익절)
  - return_q1_pct:   1~3월 수익률
  - peak_month:      2025년 고점 월

출력:
  data_store/etf_backtest/returns_2025.json
"""
import sys
import io
import json
import logging
import time
from datetime import datetime
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

from dotenv import load_dotenv
ROOT = Path(__file__).resolve().parents[3]
load_dotenv(ROOT / ".env")

from pykrx import stock

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("A7.Returns")

DATA_DIR = ROOT / "scalper-agent" / "data_store" / "etf_backtest"
MASTER_PATH = DATA_DIR / "master_2024.json"
OUT_PATH = DATA_DIR / "returns_2025.json"

# 백테스트 기준
START_2025 = "20250102"      # 2025 첫 거래일
END_2025 = "20251231"        # 2025 마지막 (현재 시점까지만 데이터 있음)
Q1_END = "20250331"


def load_master() -> dict:
    if not MASTER_PATH.exists():
        raise FileNotFoundError(f"마스터 파일 없음: {MASTER_PATH}")
    return json.loads(MASTER_PATH.read_text(encoding="utf-8"))


def compute_returns(ticker: str) -> dict:
    """단일 ETF의 25년 수익률 지표 계산."""
    try:
        df = stock.get_etf_ohlcv_by_date(START_2025, END_2025, ticker)
    except Exception as e:
        return {"error": f"조회실패: {e}"}

    if df is None or len(df) == 0:
        return {"error": "데이터없음"}

    # 시가는 2025-01-02 시가, 또는 25년 첫 시가
    start_open = float(df.iloc[0]["시가"])
    if start_open <= 0:
        # 시가 없으면 종가로 폴백
        start_open = float(df.iloc[0]["종가"])
    last_close = float(df.iloc[-1]["종가"])
    last_date = df.index[-1].strftime("%Y-%m-%d")

    # 25년 최고가 (장중 고점 기준)
    peak_high = float(df["고가"].max())
    peak_idx = df["고가"].idxmax()
    peak_month = peak_idx.strftime("%Y-%m")
    peak_date = peak_idx.strftime("%Y-%m-%d")

    # Q1 수익률 (1-3월말)
    q1_df = df.loc[:Q1_END]
    if not q1_df.empty:
        q1_close = float(q1_df.iloc[-1]["종가"])
        return_q1_pct = (q1_close / start_open - 1) * 100
    else:
        return_q1_pct = 0

    # MDD (Max Drawdown) - 25년 내 최대 낙폭
    cummax = df["종가"].cummax()
    drawdown = (df["종가"] / cummax - 1) * 100
    mdd_pct = float(drawdown.min())

    # 평균 거래대금 (25년)
    avg_value_bil = float(df["거래대금"].mean()) / 1e8

    return {
        "ticker": ticker,
        "data_days": len(df),
        "start_open": start_open,
        "last_close": last_close,
        "last_date": last_date,
        "peak_high": peak_high,
        "peak_month": peak_month,
        "peak_date": peak_date,
        "return_ytd_pct": round((last_close / start_open - 1) * 100, 2),
        "return_peak_pct": round((peak_high / start_open - 1) * 100, 2),
        "return_q1_pct": round(return_q1_pct, 2),
        "mdd_pct": round(mdd_pct, 2),
        "avg_value_25_bil": round(avg_value_bil, 2),
    }


def main():
    started = datetime.now()
    print(f"\n{'='*60}")
    print(f"  A-7 25년 수익률 수집 (정답지)")
    print(f"{'='*60}\n")

    master = load_master()
    etfs = master["etfs"]
    logger.info(f"마스터 ETF: {len(etfs)}개")

    results = []
    failed = []

    for i, e in enumerate(etfs, 1):
        if i % 30 == 0:
            logger.info(f"  진행: {i}/{len(etfs)}")

        ticker = e["ticker"]
        name = e["name"]

        ret = compute_returns(ticker)
        if "error" in ret:
            failed.append({"ticker": ticker, "name": name, "error": ret["error"]})
            continue

        ret["name"] = name
        results.append(ret)
        time.sleep(0.05)

    # 수익률 내림차순 정렬
    results.sort(key=lambda x: x.get("return_peak_pct", 0), reverse=True)

    # 저장
    final = {
        "collected_at": started.isoformat(),
        "elapsed_sec": round((datetime.now() - started).total_seconds(), 1),
        "period": f"{START_2025}~{END_2025}",
        "stats": {
            "total": len(etfs),
            "success": len(results),
            "failed": len(failed),
        },
        "returns": results,
        "failed": failed,
    }

    OUT_PATH.write_text(json.dumps(final, ensure_ascii=False, indent=2), encoding="utf-8")
    elapsed = round((datetime.now() - started).total_seconds(), 1)

    print(f"\n  완료 — 소요 {elapsed}초")
    print(f"  성공: {len(results)} / 실패: {len(failed)}")
    print(f"  저장: {OUT_PATH}")

    # 25년 폭등 TOP 30 미리보기
    print(f"\n[25년 폭등 ETF TOP 30 — 정답지]")
    print(f"  {'순위':<4}{'종목명':<28}{'YTD':>8}{'Peak':>8}{'Q1':>8}{'고점월':>10}")
    print(f"  {'-'*70}")
    for i, r in enumerate(results[:30], 1):
        print(
            f"  {i:<4}{r['name'][:26]:<28s}"
            f"{r['return_ytd_pct']:>7.1f}%"
            f"{r['return_peak_pct']:>7.1f}%"
            f"{r['return_q1_pct']:>7.1f}%"
            f"{r['peak_month']:>10}"
        )

    # 차트영웅 언급 ETF 어디 있는지
    print(f"\n[차트영웅 언급 ETF 25년 순위 확인]")
    chart_hero_tickers = {
        "102960": "KODEX 기계장비",
        "139230": "TIGER 200 중공업",
        "445290": "KODEX 로봇액티브",
        "433500": "ACE 원자력TOP10",
        "442320": "RISE 글로벌원자력",
        "494670": "TIGER 조선TOP10",
        "463250": "TIGER K방산&우주",
        "490480": "SOL K방산",
        "466920": "SOL 조선TOP3플러스",
        "449450": "PLUS K방산",
    }
    for t, name in chart_hero_tickers.items():
        for rank, r in enumerate(results, 1):
            if r["ticker"] == t:
                print(
                    f"  {rank:>3}위 {name:<28s} "
                    f"YTD {r['return_ytd_pct']:>+6.1f}% / "
                    f"Peak {r['return_peak_pct']:>+6.1f}% "
                    f"({r['peak_month']})"
                )
                break
        else:
            print(f"   -위 {name:<28s} (마스터에 없음)")


if __name__ == "__main__":
    main()
