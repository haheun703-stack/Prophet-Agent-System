# -*- coding: utf-8 -*-
"""A-8: 백테스트 통합 실행 — 차트영웅 로직 검증

흐름:
  1) 마스터 ETF 로드 (24-12-30 시점)
  2) 각 ETF의 월봉 5년치 수집 (캐시)
  3) A-4 패턴 감지 → 점수화
  4) TOP 20 추출 (Plan B: 비중 없이 차트 패턴만)
  5) 25년 실제 수익률 매칭 (A-7 결과)
  6) 적중률 계산:
     - "25년 +50% 이상 폭등 ETF" 중 24-12 TOP20에 몇 % 포함되었는가
     - DoD 임계: 60%
  7) PASS / FAIL 판정 + 상세 리포트

출력:
  data_store/etf_backtest/backtest_result.json
  콘솔: 상세 리포트
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
sys.path.insert(0, str(ROOT / "scalper-agent" / "backtest" / "etf_leader"))

import pandas as pd
from pykrx import stock
from a4_detect_pattern import detect_chart_hero_pattern

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("A8.Backtest")

DATA_DIR = ROOT / "scalper-agent" / "data_store" / "etf_backtest"
SENSITIVITY_RAW = DATA_DIR / "sensitivity_raw.json"   # A-2.5 결과: 574개 거래대금+수익률
SENSITIVITY_RESULT = DATA_DIR / "sensitivity.json"    # A-2.5 임계값 분석 결과
OHLCV_CACHE_DIR = DATA_DIR / "ohlcv_monthly"
OUT_PATH = DATA_DIR / "backtest_result.json"

OHLCV_CACHE_DIR.mkdir(parents=True, exist_ok=True)

# 백테스트 파라미터
BASE_DATE = "20241230"
PATTERN_START = "20200101"
PATTERN_END = "20241231"
TOP_N = 20                   # 상위 N개 선정
SUCCESS_THRESHOLD = 50.0     # 25년 +50%+ = 폭등 ETF
DOD_THRESHOLD = 0.60         # 60% 적중률이 PASS 기준
DEFAULT_LIQUIDITY_BIL = None # 명시되지 않으면 sensitivity 추천값 사용


def load_or_collect_ohlcv(ticker: str) -> "pd.DataFrame":
    """월봉 OHLCV 로컬 캐시 우선, 없으면 KRX 조회. 캐시 키에 기간 포함."""
    cache_path = OHLCV_CACHE_DIR / f"{ticker}_{PATTERN_START}_{PATTERN_END}.csv"

    if cache_path.exists():
        try:
            df = pd.read_csv(cache_path, index_col=0, parse_dates=True)
            return df
        except Exception as e:
            logger.warning(f"[{ticker}] 캐시 손상 — 재호출: {e}")
            cache_path.unlink(missing_ok=True)

    try:
        df = stock.get_etf_ohlcv_by_date(PATTERN_START, PATTERN_END, ticker, freq="m")
        if df is not None and len(df) > 0:
            df.to_csv(cache_path)
        return df
    except Exception as e:
        logger.debug(f"[{ticker}] OHLCV 조회 실패: {e}")
        return None


def detect_all_patterns(etfs: list) -> list:
    """모든 ETF에 패턴 감지 실행."""
    logger.info(f"패턴 감지 시작 ({len(etfs)}개)...")
    results = []

    for i, e in enumerate(etfs, 1):
        if i % 30 == 0:
            logger.info(f"  진행: {i}/{len(etfs)}")

        ticker = e["ticker"]
        name = e["name"]

        # 캐시 hit/miss 판단을 위해 사전 체크 (sleep 적용 여부 결정)
        cache_path = OHLCV_CACHE_DIR / f"{ticker}_{PATTERN_START}_{PATTERN_END}.csv"
        cache_miss = not cache_path.exists()

        df = load_or_collect_ohlcv(ticker)
        if df is None or len(df) < 24:
            results.append({
                "ticker": ticker, "name": name,
                "score": 0, "pattern_type": "NO_DATA",
                "skip_reason": "data_insufficient",
                "avg_value_24dec_bil": e.get("avg_value_24dec_bil", 0),
                "data_months": e.get("data_months", 0),
            })
            continue

        # 데이터가 36개월 미만이면 가변 lookback
        pattern = detect_chart_hero_pattern(df)

        if pattern is None:
            results.append({
                "ticker": ticker, "name": name,
                "score": 0, "pattern_type": "NO_BREAKOUT",
                "avg_value_24dec_bil": e.get("avg_value_24dec_bil", 0),
                "data_months": len(df),
            })
            continue

        results.append({
            "ticker": ticker,
            "name": name,
            "avg_value_24dec_bil": e.get("avg_value_24dec_bil", 0),
            "data_months": len(df),
            **pattern,
        })

        # 캐시 미스(실제 KRX 호출) 시에만 sleep — cache hit은 즉시 진행
        if cache_miss:
            time.sleep(0.05)

    # 점수 내림차순 정렬
    results.sort(key=lambda x: x.get("score", 0), reverse=True)
    return results


def merge_with_returns(patterns: list, returns_data: dict) -> list:
    """패턴 결과 + 25년 실제 수익률 병합."""
    returns_map = {r["ticker"]: r for r in returns_data["returns"]}

    merged = []
    for p in patterns:
        ticker = p["ticker"]
        ret = returns_map.get(ticker, {})
        p["return_ytd_pct"] = ret.get("return_ytd_pct", None)
        p["return_peak_pct"] = ret.get("return_peak_pct", None)
        p["return_q1_pct"] = ret.get("return_q1_pct", None)
        p["peak_month"] = ret.get("peak_month", None)
        merged.append(p)
    return merged


def evaluate_backtest(merged: list, top_n: int = TOP_N) -> dict:
    """적중률 평가.

    정답지: 25년 peak 수익률 +50% 이상 폭등 ETF
    예측:   24-12 시점 패턴 점수 TOP N

    적중률 = (TOP N 중 폭등 ETF 개수) / (전체 폭등 ETF 개수)
            ↑ recall (재현율). 폭등주를 얼마나 미리 잡았나
    또는
            = (TOP N 중 폭등 ETF 개수) / N
            ↑ precision (정밀도). 예측 TOP의 적중도
    """
    # 정답지: 폭등 ETF
    actual_winners = [
        m for m in merged
        if m.get("return_peak_pct") is not None
        and m["return_peak_pct"] >= SUCCESS_THRESHOLD
    ]

    # 예측: TOP N (점수 > 0인 것 중 상위)
    candidates = [m for m in merged if m.get("score", 0) > 0]
    top_n_picks = candidates[:top_n]
    top_n_tickers = {m["ticker"] for m in top_n_picks}

    # TOP N 중 폭등 ETF
    hits = [m for m in top_n_picks if m["ticker"] in {w["ticker"] for w in actual_winners}]

    # 메트릭
    recall = len(hits) / len(actual_winners) if actual_winners else 0
    precision = len(hits) / len(top_n_picks) if top_n_picks else 0

    # 폭등 ETF가 TOP N에 못 든 경우 (False Negative)
    misses = [w for w in actual_winners if w["ticker"] not in top_n_tickers]

    # 평균 수익률 비교
    avg_top_n_return = sum(
        m["return_peak_pct"] for m in top_n_picks if m.get("return_peak_pct") is not None
    ) / len([m for m in top_n_picks if m.get("return_peak_pct") is not None]) if top_n_picks else 0

    avg_all_return = sum(
        m["return_peak_pct"] for m in merged if m.get("return_peak_pct") is not None
    ) / len([m for m in merged if m.get("return_peak_pct") is not None]) if merged else 0

    verdict = "PASS" if recall >= DOD_THRESHOLD else "FAIL"

    return {
        "verdict": verdict,
        "dod_threshold": DOD_THRESHOLD,
        "success_threshold_pct": SUCCESS_THRESHOLD,
        "total_etfs_evaluated": len(merged),
        "actual_winners_count": len(actual_winners),
        "top_n": top_n,
        "top_n_picks_count": len(top_n_picks),
        "hits": len(hits),
        "recall": round(recall, 4),
        "precision": round(precision, 4),
        "avg_top_n_peak_return_pct": round(avg_top_n_return, 2),
        "avg_all_peak_return_pct": round(avg_all_return, 2),
        "outperformance_pct": round(avg_top_n_return - avg_all_return, 2),
        "hits_detail": [
            {"ticker": h["ticker"], "name": h["name"],
             "score": h["score"], "return_peak_pct": h["return_peak_pct"]}
            for h in hits
        ],
        "misses_detail": [
            {"ticker": m["ticker"], "name": m["name"],
             "score": m.get("score", 0),
             "pattern_type": m.get("pattern_type", "-"),
             "return_peak_pct": m["return_peak_pct"]}
            for m in misses
        ],
    }


def load_from_sensitivity(threshold_bil: float = None):
    """sensitivity 결과에서 임계값 필터 적용한 마스터 + 수익률 동시 반환."""
    if not SENSITIVITY_RAW.exists():
        raise FileNotFoundError(f"A-2.5 먼저 실행하세요: {SENSITIVITY_RAW}")

    raw = json.loads(SENSITIVITY_RAW.read_text(encoding="utf-8"))

    # 임계값 미지정 시 sensitivity.json의 추천 임계 사용
    if threshold_bil is None and SENSITIVITY_RESULT.exists():
        sens = json.loads(SENSITIVITY_RESULT.read_text(encoding="utf-8"))
        valid_rows = [r for r in sens["thresholds"] if r["recall"] >= 0.95]
        if valid_rows:
            best = max(valid_rows, key=lambda x: x["precision"])
            threshold_bil = best["threshold_bil"]
            logger.info(f"sensitivity 추천 임계 적용: {threshold_bil}억")
        else:
            threshold_bil = 1.0
            logger.warning("Recall 95% 도달 임계 없음 → 1.0억 폴백")

    if threshold_bil is None:
        threshold_bil = 5.0

    # 임계값 필터
    master_etfs = []
    returns_map = {}
    for e in raw["etfs"]:
        if e["avg_value_24dec_bil"] < threshold_bil:
            continue
        master_etfs.append({
            "ticker": e["ticker"],
            "name": e["name"],
            "avg_value_24dec_bil": e["avg_value_24dec_bil"],
        })
        if e.get("return_peak_pct") is not None:
            returns_map[e["ticker"]] = {
                "ticker": e["ticker"],
                "name": e["name"],
                "return_ytd_pct": e.get("return_ytd_pct"),
                "return_peak_pct": e.get("return_peak_pct"),
                "peak_month": e.get("peak_month"),
            }

    logger.info(f"임계 {threshold_bil}억 적용 → 마스터 {len(master_etfs)}개")
    return master_etfs, {"returns": list(returns_map.values())}, threshold_bil


def main():
    started = datetime.now()

    # CLI: python a8_run_backtest.py [임계값(억)]
    cli_threshold = None
    if len(sys.argv) > 1:
        try:
            cli_threshold = float(sys.argv[1])
        except ValueError:
            pass

    print(f"\n{'='*70}")
    print(f"  A-8 백테스트 실행 — 차트영웅 ETF 주도주 발굴 시스템")
    print(f"  기준일: {BASE_DATE} / 검증기간: 2025년")
    print(f"{'='*70}\n")

    # 1) sensitivity 결과에서 마스터 + 수익률 동시 로드
    master_etfs, returns_data, applied_threshold = load_from_sensitivity(cli_threshold)

    if not master_etfs:
        print(f"[ERROR] 마스터가 비어있음 (임계값 너무 높음)")
        return

    # 2) 패턴 감지
    patterns = detect_all_patterns(master_etfs)

    # 3) 수익률 병합
    merged = merge_with_returns(patterns, returns_data)

    # 4) 평가
    evaluation = evaluate_backtest(merged, TOP_N)
    evaluation["applied_threshold_bil"] = applied_threshold

    # 5) 저장
    result = {
        "executed_at": started.isoformat(),
        "elapsed_sec": round((datetime.now() - started).total_seconds(), 1),
        "evaluation": evaluation,
        "top_30_predictions": [
            {k: v for k, v in m.items() if k not in ("score_detail",)}
            for m in merged[:30]
        ],
        "all_patterns_count": len([m for m in merged if m.get("score", 0) > 0]),
    }

    OUT_PATH.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")

    # 6) 콘솔 리포트
    print_report(evaluation, merged)
    print(f"\n  저장: {OUT_PATH}")


def print_report(ev: dict, merged: list):
    print(f"\n{'='*70}")
    print(f"  📊 백테스트 결과")
    print(f"{'='*70}")
    print(f"  판정: {ev['verdict']} (적중률 {ev['recall']*100:.1f}% / DoD {ev['dod_threshold']*100:.0f}%)")
    print()
    print(f"  ▣ 평가 대상:    {ev['total_etfs_evaluated']}개 ETF")
    print(f"  ▣ 실제 폭등주:  {ev['actual_winners_count']}개 (peak +{ev['success_threshold_pct']:.0f}%+)")
    print(f"  ▣ 예측 TOP{ev['top_n']}:    {ev['top_n_picks_count']}개")
    print(f"  ▣ 적중:         {ev['hits']}개")
    print()
    print(f"  ▣ Recall:       {ev['recall']*100:>5.1f}%  (실제 폭등주 중 TOP N에 든 비율)")
    print(f"  ▣ Precision:    {ev['precision']*100:>5.1f}%  (TOP N 중 실제 폭등 비율)")
    print()
    print(f"  ▣ TOP{ev['top_n']} 평균 peak수익률: {ev['avg_top_n_peak_return_pct']:>+7.2f}%")
    print(f"  ▣ 전체 평균:                {ev['avg_all_peak_return_pct']:>+7.2f}%")
    print(f"  ▣ Outperformance:           {ev['outperformance_pct']:>+7.2f}%p")

    # TOP 30 예측 결과
    print(f"\n{'='*70}")
    print(f"  🎯 24-12-30 시점 TOP 30 예측 (실제 25년 결과)")
    print(f"{'='*70}")
    print(f"  {'순위':<4}{'종목명':<28}{'점수':>6}{'타입':>22}{'Peak수익':>10}")
    print(f"  {'-'*70}")
    for i, m in enumerate(merged[:30], 1):
        if m.get("score", 0) == 0:
            break
        ret = m.get("return_peak_pct")
        ret_str = f"{ret:>+8.1f}%" if ret is not None else "    N/A"
        is_hit = " ★" if ret is not None and ret >= 50 else "  "
        print(
            f"  {i:<4}{m['name'][:26]:<28s}"
            f"{m['score']:>5.1f} "
            f"{m.get('pattern_type', '-')[:20]:>22s}"
            f"{ret_str}{is_hit}"
        )

    # 놓친 폭등주 (FN)
    if ev["misses_detail"]:
        print(f"\n{'='*70}")
        print(f"  ❌ 놓친 폭등주 (TOP{ev['top_n']}에 안 들어감, {len(ev['misses_detail'])}개)")
        print(f"{'='*70}")
        for m in ev["misses_detail"][:15]:
            print(
                f"  {m['ticker']} {m['name'][:30]:<30s} "
                f"점수 {m['score']:>5.1f}  타입 {m['pattern_type'][:20]:>20s}  "
                f"실제 +{m['return_peak_pct']:.1f}%"
            )


if __name__ == "__main__":
    main()
