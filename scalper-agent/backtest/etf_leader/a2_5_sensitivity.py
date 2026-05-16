# -*- coding: utf-8 -*-
"""A-2.5: 임계값 sensitivity 분석

목적:
  거래대금 임계를 추측 대신 데이터로 결정한다.
  키워드 통과 ETF 574개 전체에 대해 다음을 수집:
    - 24년 12월 평균 거래대금 (이미 A-2에서 한 것 + 빠진 337개 추가)
    - 25년 peak 수익률 (정답지)

  그 후 임계값별 (Recall, Precision, 마스터 크기) trade-off를 산출.

출력:
  data_store/etf_backtest/sensitivity.json
  콘솔: 임계값별 표
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
logger = logging.getLogger("A2.5.Sens")

DATA_DIR = ROOT / "scalper-agent" / "data_store" / "etf_backtest"
OUT_PATH = DATA_DIR / "sensitivity.json"

# 정답지 임계
SUCCESS_THRESHOLD = 50.0       # 25년 peak +50%+ = 폭등 ETF

# 분석할 거래대금 임계값 (억원)
TEST_THRESHOLDS = [0.0, 0.5, 1.0, 2.0, 3.0, 5.0, 10.0, 30.0, 100.0]

# 카테고리 키워드 (차트영웅 영상 강조)
CHART_HERO_KW = [
    "기계장비", "로봇", "Robot",
    "중공업", "조선", "방산", "K방산", "우주",
    "원자력", "원전",
    "AI", "반도체",
]

EXCLUDE_KEYWORDS = [
    "레버리지", "인버스", "2X", "3X",
    "채권", "국고채", "통안채", "회사채", "종합채", "단기채", "특수채",
    "CD금리", "KOFR", "머니마켓", "단기자금", "단기통안",
    "선물", "옵션", "VIX", "변동성",
    "합성", "스왑",
    "달러", "엔화", "위안", "유로",
    "금현물", "은현물",
]

BASE_DATE = "20241230"
DEC_START = "20241201"
DEC_END = "20241230"
RET_START = "20250102"
RET_END = "20251231"


def collect_all() -> list:
    """키워드 통과 574개 ETF에 대해 거래대금 + 25년 수익률 수집."""
    logger.info(f"기준일 {BASE_DATE} ETF 리스트 조회...")
    tickers = stock.get_etf_ticker_list(BASE_DATE)
    logger.info(f"전체 ETF: {len(tickers)}개")

    # 키워드 제외
    candidates = []
    for t in tickers:
        try:
            name = stock.get_etf_ticker_name(t)
        except Exception:
            continue
        if any(kw in name for kw in EXCLUDE_KEYWORDS):
            continue
        candidates.append({"ticker": t, "name": name})

    logger.info(f"키워드 통과: {len(candidates)}개")

    # 각 ETF에 대해 24-12 거래대금 + 25년 수익률 수집
    results = []
    failed = []

    for i, e in enumerate(candidates, 1):
        if i > 1:
            time.sleep(0.02)  # 매 iteration 시작에 일관 sleep (모든 path 보장)

        if i % 30 == 0:
            logger.info(f"  진행: {i}/{len(candidates)} (성공 {len(results)} / 실패 {len(failed)})")

        ticker = e["ticker"]
        name = e["name"]

        # 24-12 거래대금
        try:
            df24 = stock.get_etf_ohlcv_by_date(DEC_START, DEC_END, ticker)
            if df24 is None or len(df24) == 0:
                failed.append({"ticker": ticker, "name": name, "reason": "no_24dec"})
                continue
            avg_value_bil = float(df24["거래대금"].mean()) / 1e8
        except Exception as ex:
            failed.append({"ticker": ticker, "name": name, "reason": f"err_24dec: {ex}"})
            continue

        # 25년 수익률
        try:
            df25 = stock.get_etf_ohlcv_by_date(RET_START, RET_END, ticker)
            if df25 is None or len(df25) == 0:
                # 24년에는 있었지만 25년 데이터 없음 (상장폐지?)
                results.append({
                    "ticker": ticker, "name": name,
                    "avg_value_24dec_bil": round(avg_value_bil, 2),
                    "return_peak_pct": None,
                    "return_ytd_pct": None,
                    "peak_month": None,
                    "no_25_data": True,
                })
                continue

            start_open = float(df25.iloc[0]["시가"])
            if start_open <= 0:
                start_open = float(df25.iloc[0]["종가"])
            last_close = float(df25.iloc[-1]["종가"])
            peak_high = float(df25["고가"].max())
            peak_idx = df25["고가"].idxmax()

            return_ytd_pct = (last_close / start_open - 1) * 100
            return_peak_pct = (peak_high / start_open - 1) * 100

        except Exception as ex:
            failed.append({"ticker": ticker, "name": name, "reason": f"err_25: {ex}"})
            continue

        results.append({
            "ticker": ticker,
            "name": name,
            "avg_value_24dec_bil": round(avg_value_bil, 2),
            "return_ytd_pct": round(return_ytd_pct, 2),
            "return_peak_pct": round(return_peak_pct, 2),
            "peak_month": peak_idx.strftime("%Y-%m"),
            "is_chart_hero_kw": any(kw in name for kw in CHART_HERO_KW),
        })

    logger.info(f"수집 완료: 성공 {len(results)} / 실패 {len(failed)}")
    return results, failed


def analyze_thresholds(etfs: list) -> list:
    """임계값별 Recall/Precision 분석."""
    # 25년 데이터 있는 ETF만
    valid = [e for e in etfs if e.get("return_peak_pct") is not None]
    winners = [e for e in valid if e["return_peak_pct"] >= SUCCESS_THRESHOLD]
    total_winners = len(winners)

    logger.info(
        f"분석 대상: {len(valid)}개 / 25년 폭등 ETF(+{SUCCESS_THRESHOLD}%+): {total_winners}개"
    )

    rows = []
    for threshold in TEST_THRESHOLDS:
        passed = [e for e in valid if e["avg_value_24dec_bil"] >= threshold]
        passed_winners = [e for e in passed if e["return_peak_pct"] >= SUCCESS_THRESHOLD]

        # 차트영웅 카테고리만
        ch_passed = [e for e in passed if e.get("is_chart_hero_kw")]
        ch_winners_passed = [e for e in passed if e.get("is_chart_hero_kw") and e["return_peak_pct"] >= SUCCESS_THRESHOLD]

        recall = len(passed_winners) / total_winners if total_winners > 0 else 0
        precision = len(passed_winners) / len(passed) if len(passed) > 0 else 0

        rows.append({
            "threshold_bil": threshold,
            "master_size": len(passed),
            "winners_in_master": len(passed_winners),
            "recall": round(recall, 4),
            "precision": round(precision, 4),
            "chart_hero_size": len(ch_passed),
            "chart_hero_winners": len(ch_winners_passed),
        })
    return rows, total_winners


def main():
    started = datetime.now()
    print(f"\n{'='*70}")
    print(f"  A-2.5 임계값 sensitivity 분석")
    print(f"{'='*70}\n")

    # 수집 (또는 캐시)
    etfs, failed = collect_all()

    # 저장
    raw_path = DATA_DIR / "sensitivity_raw.json"
    raw_path.write_text(
        json.dumps({
            "collected_at": started.isoformat(),
            "elapsed_sec": round((datetime.now() - started).total_seconds(), 1),
            "etfs": etfs,
            "failed": failed,
        }, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    # 분석
    rows, total_winners = analyze_thresholds(etfs)

    elapsed = round((datetime.now() - started).total_seconds(), 1)
    print(f"\n{'='*70}")
    print(f"  📊 임계값 sensitivity (소요 {elapsed}초)")
    print(f"  25년 폭등 ETF(+{SUCCESS_THRESHOLD:.0f}%+) 총 {total_winners}개 정답지")
    print(f"{'='*70}")
    print(f"  {'임계(억)':>8}{'마스터':>8}{'폭등주':>8}{'Recall':>10}{'Precis':>10}{'차트영웅':>10}{'그중폭등':>10}")
    print(f"  {'-'*70}")
    for r in rows:
        print(
            f"  {r['threshold_bil']:>7,.1f}"
            f"{r['master_size']:>8}"
            f"{r['winners_in_master']:>8}"
            f"{r['recall']*100:>9.1f}%"
            f"{r['precision']*100:>9.1f}%"
            f"{r['chart_hero_size']:>10}"
            f"{r['chart_hero_winners']:>10}"
        )

    # 추천 임계
    print(f"\n{'='*70}")
    print(f"  💡 추천 (Recall ≥ 95% 유지하면서 Precision 최대화)")
    print(f"{'='*70}")
    target_recall = 0.95
    candidates = [r for r in rows if r["recall"] >= target_recall]
    if candidates:
        best = max(candidates, key=lambda x: x["precision"])
        print(f"  추천 임계: {best['threshold_bil']:.1f}억")
        print(f"  → 마스터 {best['master_size']}개, Recall {best['recall']*100:.1f}%, Precision {best['precision']*100:.1f}%")
    else:
        # 가장 높은 recall
        best = max(rows, key=lambda x: x["recall"])
        print(f"  Recall 95% 도달 안 됨. 최대 Recall: {best['threshold_bil']:.1f}억 ({best['recall']*100:.1f}%)")

    final = {
        "executed_at": started.isoformat(),
        "elapsed_sec": elapsed,
        "total_winners": total_winners,
        "thresholds": rows,
    }
    OUT_PATH.write_text(json.dumps(final, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\n  저장: {OUT_PATH}")
    print(f"  Raw 데이터: {raw_path}")

    # 25년 TOP 30 폭등주
    valid = [e for e in etfs if e.get("return_peak_pct") is not None]
    valid.sort(key=lambda x: x["return_peak_pct"], reverse=True)
    print(f"\n[25년 폭등 TOP 30]")
    print(f"  {'순위':<4}{'종목명':<32}{'Peak수익':>10}{'24-12 거래대금':>16}{'카테고리':>10}")
    for i, e in enumerate(valid[:30], 1):
        ch = "★" if e.get("is_chart_hero_kw") else " "
        print(
            f"  {i:<4}{e['name'][:30]:<32s}"
            f"{e['return_peak_pct']:>+9.1f}%"
            f"{e['avg_value_24dec_bil']:>15,.1f}억"
            f"      {ch}"
        )


if __name__ == "__main__":
    main()
