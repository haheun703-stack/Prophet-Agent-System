# -*- coding: utf-8 -*-
"""A-2: ETF 마스터 수집 — 백테스트 기준일(2024-12-30) 시점

목표:
  - 24년말 시점 전 ETF 리스트 수집 (~878개)
  - 키워드 필터 (레버리지/인버스/채권/머니마켓 제외) → ~600개
  - 거래대금 필터 (24년 12월 평균 일 5억+ 거래대금) → ~200개
  - 장기상장 필터 (4년+ 데이터 보유) → ~150개

출력:
  data_store/etf_backtest/master_2024.json
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
ROOT = Path(__file__).resolve().parents[3]  # D:\Prophet_Agent_System_예언자
load_dotenv(ROOT / ".env")

from pykrx import stock

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("A2.Master")

# ── 기준일 ───────────────────────────────────────
BASE_DATE = "20241230"          # 백테스트 기준일 (24년말 마지막 거래일)
DEC_START = "20241201"          # 12월 평균 거래대금 계산용
DEC_END = "20241230"
DATA_DIR = ROOT / "scalper-agent" / "data_store" / "etf_backtest"
# 정확한 경로 형태: D:\Prophet_Agent_System_예언자\scalper-agent\data_store\etf_backtest
DATA_DIR.mkdir(parents=True, exist_ok=True)
OUT_PATH = DATA_DIR / "master_2024.json"

# ── 제외 키워드 (etf_scanner.py 패턴 차용) ──────────
EXCLUDE_KEYWORDS = [
    "레버리지", "인버스", "2X", "3X",
    "채권", "국고채", "통안채", "회사채", "종합채", "단기채", "특수채",
    "CD금리", "KOFR", "머니마켓", "단기자금", "단기통안",
    "선물", "옵션", "VIX", "변동성",
    "합성", "스왑",
    "달러", "엔화", "위안", "유로",  # 환율 ETF 제외
    "금현물", "은현물",  # 단일 원자재만 (섹터 ETF는 포함)
]

# ── 거래대금 임계치 ────────────────────────────────
MIN_AVG_VOL_BIL = 5.0           # 12월 평균 일 거래대금 5억+
MIN_LONG_TERM_MONTHS = 30       # 최소 30개월(2.5년) 데이터 (눌림목 패턴 분석 가능)


def collect_etf_list(base_date: str) -> list:
    """기준일 ETF 리스트 + 이름 수집."""
    logger.info(f"기준일 {base_date} ETF 리스트 조회...")
    tickers = stock.get_etf_ticker_list(base_date)

    result = []
    for t in tickers:
        try:
            name = stock.get_etf_ticker_name(t)
        except Exception:
            continue
        result.append({"ticker": t, "name": name})

    logger.info(f"전체 ETF: {len(result)}개")
    return result


def filter_keywords(etfs: list) -> list:
    """레버리지/인버스/채권 등 키워드 제외."""
    survivors = []
    excluded = []
    for e in etfs:
        name = e["name"]
        if any(kw in name for kw in EXCLUDE_KEYWORDS):
            excluded.append(e)
            continue
        survivors.append(e)

    logger.info(f"키워드 필터: {len(etfs)} → {len(survivors)} ({len(excluded)}개 제외)")
    return survivors


def filter_by_liquidity(etfs: list) -> list:
    """24년 12월 일평균 거래대금 기준 필터."""
    logger.info(f"거래대금 필터 시작 ({DEC_START}~{DEC_END}, 임계 {MIN_AVG_VOL_BIL}억원)...")

    survivors = []
    skipped = 0
    failed = 0

    for i, e in enumerate(etfs, 1):
        if i % 50 == 0:
            logger.info(f"  진행: {i}/{len(etfs)} ({len(survivors)}개 통과)")

        ticker = e["ticker"]
        try:
            df = stock.get_etf_ohlcv_by_date(DEC_START, DEC_END, ticker)
            if df is None or len(df) == 0:
                failed += 1
                continue

            # 거래대금 컬럼명: '거래대금' (원 단위)
            avg_value = df["거래대금"].mean() / 1e8  # 억원 환산

            if avg_value < MIN_AVG_VOL_BIL:
                skipped += 1
                continue

            e["avg_value_24dec_bil"] = round(avg_value, 2)
            e["data_rows_24dec"] = len(df)
            survivors.append(e)
            time.sleep(0.05)  # KRX 부하 방지
        except Exception as ex:
            logger.debug(f"  [{ticker}] {e['name'][:15]} 조회 실패: {ex}")
            failed += 1
            continue

    logger.info(
        f"거래대금 필터: {len(etfs)} → {len(survivors)} "
        f"(미달 {skipped}, 조회실패 {failed})"
    )
    return survivors


def filter_by_long_term(etfs: list) -> list:
    """5년치 월봉 데이터 보유 ETF만 필터 (장기 패턴 분석 가능)."""
    logger.info(f"장기상장 필터 시작 (최소 {MIN_LONG_TERM_MONTHS}개월 월봉)...")

    survivors = []
    skipped = 0

    for i, e in enumerate(etfs, 1):
        if i % 30 == 0:
            logger.info(f"  진행: {i}/{len(etfs)} ({len(survivors)}개 통과)")

        ticker = e["ticker"]
        try:
            # 2020-01부터 2024-12까지 월봉
            df = stock.get_etf_ohlcv_by_date("20200101", "20241231", ticker, freq="m")
            if df is None or len(df) < MIN_LONG_TERM_MONTHS:
                skipped += 1
                continue

            e["data_months"] = len(df)
            e["first_month"] = df.index[0].strftime("%Y-%m")
            e["last_month"] = df.index[-1].strftime("%Y-%m")
            survivors.append(e)
            time.sleep(0.05)
        except Exception:
            skipped += 1
            continue

    logger.info(f"장기상장 필터: {len(etfs)} → {len(survivors)} ({skipped}개 미달)")
    return survivors


def main():
    started = datetime.now()
    print(f"\n{'='*60}")
    print(f"  A-2 ETF 마스터 수집 (기준일: {BASE_DATE})")
    print(f"{'='*60}\n")

    # 1) 전체 ETF 리스트
    all_etfs = collect_etf_list(BASE_DATE)

    # 2) 키워드 필터
    after_kw = filter_keywords(all_etfs)

    # 3) 거래대금 필터 (24년 12월 평균)
    after_vol = filter_by_liquidity(after_kw)

    # 4) 장기상장 필터 생략 — A-8 백테스트 실행 시 OHLCV 부족하면 자동 skip
    #    중복 KRX 호출 회피 (속도 5배 개선)
    final = after_vol

    # 5) 정렬 (거래대금 내림차순)
    final.sort(key=lambda x: x.get("avg_value_24dec_bil", 0), reverse=True)

    # 6) 저장
    result = {
        "base_date": BASE_DATE,
        "collected_at": started.isoformat(),
        "elapsed_sec": round((datetime.now() - started).total_seconds(), 1),
        "filters": {
            "exclude_keywords": EXCLUDE_KEYWORDS,
            "min_avg_value_bil": MIN_AVG_VOL_BIL,
            "min_long_term_months": MIN_LONG_TERM_MONTHS,
        },
        "stats": {
            "total_etf": len(all_etfs),
            "after_keyword": len(after_kw),
            "after_liquidity": len(after_vol),
            "final": len(final),
        },
        "note": "장기상장 필터는 A-8 백테스트 실행 시 OHLCV 캐시와 함께 자동 적용",
        "etfs": final,
    }

    OUT_PATH.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    elapsed = round((datetime.now() - started).total_seconds(), 1)

    print(f"\n{'='*60}")
    print(f"  A-2 완료 — 소요 {elapsed}초")
    print(f"{'='*60}")
    print(f"  전체 ETF:       {len(all_etfs):>4}개")
    print(f"  키워드 필터:    {len(after_kw):>4}개")
    print(f"  거래대금 필터:  {len(after_vol):>4}개  ← 최종 마스터")
    print(f"  (장기상장 필터는 A-8 OHLCV 캐시에서 자동 적용)")
    print(f"\n  저장: {OUT_PATH}")

    # 상위 20개 미리보기
    print(f"\n[거래대금 상위 20개]")
    for i, e in enumerate(final[:20], 1):
        print(
            f"  {i:>2}. {e['ticker']} {e['name'][:30]:<30s} "
            f"24-12 평균 {e['avg_value_24dec_bil']:>7,.1f}억"
        )

    # 차트영웅 언급 ETF 포함 여부
    chart_hero_kw = ["기계장비", "로봇", "중공업", "조선", "방산", "원자력", "원전", "AI"]
    print(f"\n[차트영웅 언급 카테고리 포함 ETF]")
    for e in final:
        if any(kw in e["name"] for kw in chart_hero_kw):
            print(
                f"  {e['ticker']} {e['name'][:30]:<30s} "
                f"평균 {e['avg_value_24dec_bil']:>6,.1f}억"
            )


if __name__ == "__main__":
    main()
