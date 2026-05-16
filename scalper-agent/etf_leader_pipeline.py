# -*- coding: utf-8 -*-
"""ETF 주도주 발굴 파이프라인 — 본구축 (Step B)

목적:
  현재 시점(26-05) ETF 데이터로 26년 하반기 ~ 27년 폭등 후보 발굴.
  차트영웅 영상 로직 + sensitivity 분석에서 얻은 경험치 적용.

흐름:
  B-1) 현재 시점 ETF 마스터 수집 (878개 → 카테고리 키워드 → ~90개)
  B-2) 거래대금 임계 0.5억 (sensitivity에서 결정)
  B-3) 5년치 월봉 + 차트영웅 패턴 감지 (다년 박스권 돌파 + 눌림목)
  B-4) 점수화 + TOP 20 추출
  B-5) 결과 저장 + 콘솔 리포트

산출:
  data_store/etf_backtest/leader_picks_<YYYYMMDD>.json
  data_store/etf_backtest/ohlcv_now/   (월봉 캐시)
"""
import sys
import io
import json
import logging
import time
from datetime import datetime, timedelta
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

from dotenv import load_dotenv
ROOT = Path(__file__).resolve().parent.parent  # D:\Prophet_Agent_System_예언자
load_dotenv(ROOT / ".env")
sys.path.insert(0, str(ROOT / "scalper-agent" / "backtest" / "etf_leader"))

from pykrx import stock
import pandas as pd

from a4_detect_pattern import detect_chart_hero_pattern

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("ETFLeader")

# ── 경로 ───────────────────────────────────────────────
DATA_DIR = ROOT / "scalper-agent" / "data_store" / "etf_backtest"
OHLCV_DIR = DATA_DIR / "ohlcv_now"
DATA_DIR.mkdir(parents=True, exist_ok=True)
OHLCV_DIR.mkdir(parents=True, exist_ok=True)

# ── 파라미터 (sensitivity 결과 반영) ──────────────────
LIQUIDITY_THRESHOLD_BIL = 0.5     # A-2.5에서 결정: 0.5억
TOP_N = 20                          # TOP N개 후보 선정
PATTERN_LOOKBACK_YEARS = 5          # 5년치 월봉

# 차트영웅 카테고리 (sensitivity에서 67% 적중 확인)
CHART_HERO_KW = [
    "기계장비", "로봇", "Robot",
    "중공업", "조선", "방산", "K방산", "우주",
    "원자력", "원전", "전력", "AI전력",
    "AI", "반도체", "HBM",
    "2차전지", "수소", "신재생", "태양광", "ESS",
    "바이오", "헬스케어", "제약",
    "농업", "식량",
    "희토류", "자원",
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


def find_last_trading_date(today: datetime = None) -> str:
    """최근 거래일 찾기 (오늘 장중이거나 휴장이면 직전)."""
    if today is None:
        today = datetime.now()
    for delta in range(0, 7):
        dt = today - timedelta(days=delta)
        dt_str = dt.strftime("%Y%m%d")
        try:
            df = stock.get_etf_ohlcv_by_ticker(dt_str)
            if df is not None and len(df) > 0:
                return dt_str
        except Exception:
            continue
    return (today - timedelta(days=1)).strftime("%Y%m%d")


# ════════════════════════════════════════════════════════════
# B-1: 현재 시점 ETF 마스터 + 카테고리 필터
# ════════════════════════════════════════════════════════════
def step_b1_master(base_date: str) -> list:
    """현재 시점 ETF 리스트 → 키워드 필터 → 카테고리 ETF 추출."""
    logger.info(f"[B-1] 기준일 {base_date} ETF 리스트 조회...")
    tickers = stock.get_etf_ticker_list(base_date)
    logger.info(f"  전체 ETF: {len(tickers)}개")

    # 키워드 통과 + 차트영웅 카테고리만
    candidates = []
    for t in tickers:
        try:
            name = stock.get_etf_ticker_name(t)
        except Exception:
            continue

        if any(kw in name for kw in EXCLUDE_KEYWORDS):
            continue

        if not any(kw in name for kw in CHART_HERO_KW):
            continue

        candidates.append({"ticker": t, "name": name})

    logger.info(f"  카테고리 통과: {len(candidates)}개")
    return candidates


# ════════════════════════════════════════════════════════════
# B-2: 거래대금 필터 (현재 시점 1개월 평균)
# ════════════════════════════════════════════════════════════
def step_b2_liquidity(candidates: list, base_date: str) -> list:
    """현재 시점 직전 1개월 평균 거래대금 + NAV + 시가총액 추가."""
    logger.info(f"[B-2] 거래대금 임계 {LIQUIDITY_THRESHOLD_BIL}억 필터링...")

    end_dt = datetime.strptime(base_date, "%Y%m%d")
    start_dt = end_dt - timedelta(days=35)
    start_str = start_dt.strftime("%Y%m%d")
    end_str = base_date

    survivors = []
    failed = 0

    for i, e in enumerate(candidates, 1):
        if i % 20 == 0:
            logger.info(f"  진행: {i}/{len(candidates)}")

        ticker = e["ticker"]
        try:
            df = stock.get_etf_ohlcv_by_date(start_str, end_str, ticker)
            if df is None or len(df) == 0:
                failed += 1
                continue

            avg_value_bil = float(df["거래대금"].mean()) / 1e8
            last_close = float(df.iloc[-1]["종가"])
            last_nav = float(df.iloc[-1].get("NAV", 0))

            if avg_value_bil < LIQUIDITY_THRESHOLD_BIL:
                continue

            e["avg_value_bil"] = round(avg_value_bil, 2)
            e["last_close"] = last_close
            e["last_nav"] = last_nav
            e["nav_gap_pct"] = round((last_close - last_nav) / last_nav * 100, 3) if last_nav > 0 else 0
            survivors.append(e)
            time.sleep(0.02)
        except Exception as ex:
            logger.debug(f"  [{ticker}] 실패: {ex}")
            failed += 1

    logger.info(f"  거래대금 필터: {len(candidates)} → {len(survivors)} (실패 {failed})")
    return survivors


# ════════════════════════════════════════════════════════════
# B-3: 5년치 월봉 + 차트영웅 패턴 감지
# ════════════════════════════════════════════════════════════
def load_or_fetch_monthly(ticker: str, end_date: str) -> "pd.DataFrame":
    """월봉 5년치 캐시 우선, 없으면 KRX."""
    cache_path = OHLCV_DIR / f"{ticker}_{end_date}.csv"

    if cache_path.exists():
        try:
            df = pd.read_csv(cache_path, index_col=0, parse_dates=True)
            return df
        except Exception:
            pass

    end_dt = datetime.strptime(end_date, "%Y%m%d")
    start_dt = end_dt - timedelta(days=365 * PATTERN_LOOKBACK_YEARS + 30)
    start_str = start_dt.strftime("%Y%m%d")

    try:
        df = stock.get_etf_ohlcv_by_date(start_str, end_date, ticker, freq="m")
        if df is not None and len(df) > 0:
            df.to_csv(cache_path)
            return df
    except Exception as e:
        logger.debug(f"  [{ticker}] 월봉 실패: {e}")
    return None


def step_b3_pattern(etfs: list, base_date: str) -> list:
    """각 ETF에 차트영웅 패턴 감지."""
    logger.info(f"[B-3] 차트영웅 패턴 감지 ({len(etfs)}개)...")

    results = []
    for i, e in enumerate(etfs, 1):
        if i % 10 == 0:
            logger.info(f"  진행: {i}/{len(etfs)}")

        ticker = e["ticker"]
        df = load_or_fetch_monthly(ticker, base_date)

        if df is None or len(df) < 24:
            results.append({
                **e,
                "score": 0,
                "pattern_type": "NO_DATA" if df is None else "INSUFFICIENT_DATA",
                "data_months": len(df) if df is not None else 0,
            })
            continue

        pattern = detect_chart_hero_pattern(df)

        if pattern is None:
            results.append({
                **e,
                "score": 0,
                "pattern_type": "NO_BREAKOUT",
                "data_months": len(df),
            })
            continue

        results.append({
            **e,
            "data_months": len(df),
            **pattern,
        })

        time.sleep(0.02)

    # 점수 내림차순
    results.sort(key=lambda x: x.get("score", 0), reverse=True)
    return results


# ════════════════════════════════════════════════════════════
# B-4: TOP N + 카테고리 그룹화
# ════════════════════════════════════════════════════════════
def step_b4_pick_top(scored: list, top_n: int = TOP_N) -> dict:
    """TOP N + 카테고리별 분포 + 패턴 타입 분포."""
    valid = [s for s in scored if s.get("score", 0) > 0]
    top_picks = valid[:top_n]

    # 카테고리별 분포
    by_category = {}
    for s in top_picks:
        for kw in CHART_HERO_KW:
            if kw in s["name"]:
                by_category.setdefault(kw, []).append(s["ticker"])
                break

    # 패턴 타입 분포
    by_pattern = {}
    for s in top_picks:
        pt = s.get("pattern_type", "?")
        by_pattern[pt] = by_pattern.get(pt, 0) + 1

    return {
        "top_picks": top_picks,
        "by_category": by_category,
        "by_pattern_type": by_pattern,
        "valid_count": len(valid),
    }


# ════════════════════════════════════════════════════════════
# 메인
# ════════════════════════════════════════════════════════════
def main():
    started = datetime.now()
    base_date = find_last_trading_date()

    print(f"\n{'='*70}")
    print(f"  📈 ETF 주도주 발굴 파이프라인 (Step B 본구축)")
    print(f"  기준일: {base_date} (최근 거래일)")
    print(f"  목표: 26년 하반기 ~ 27년 폭등 후보")
    print(f"{'='*70}\n")

    # B-1: 마스터 + 카테고리 필터
    candidates = step_b1_master(base_date)

    # B-2: 거래대금 필터
    after_vol = step_b2_liquidity(candidates, base_date)

    # B-3: 패턴 감지
    scored = step_b3_pattern(after_vol, base_date)

    # B-4: TOP N
    result = step_b4_pick_top(scored, TOP_N)

    # 저장
    elapsed = round((datetime.now() - started).total_seconds(), 1)
    final = {
        "executed_at": started.isoformat(),
        "elapsed_sec": elapsed,
        "base_date": base_date,
        "parameters": {
            "liquidity_threshold_bil": LIQUIDITY_THRESHOLD_BIL,
            "top_n": TOP_N,
            "pattern_lookback_years": PATTERN_LOOKBACK_YEARS,
            "chart_hero_keywords": CHART_HERO_KW,
        },
        "stats": {
            "category_etfs": len(candidates),
            "after_liquidity": len(after_vol),
            "valid_patterns": result["valid_count"],
            "top_picks_count": len(result["top_picks"]),
        },
        "top_picks": result["top_picks"],
        "by_category": result["by_category"],
        "by_pattern_type": result["by_pattern_type"],
        "all_scored": scored,
    }

    out_path = DATA_DIR / f"leader_picks_{base_date}.json"
    out_path.write_text(json.dumps(final, ensure_ascii=False, indent=2), encoding="utf-8")

    print_report(final)
    print(f"\n  💾 저장: {out_path}")
    print(f"  ⏱  소요: {elapsed}초")


def print_report(r: dict):
    s = r["stats"]
    print(f"\n{'='*70}")
    print(f"  📊 ETF 주도주 발굴 결과 (기준일 {r['base_date']})")
    print(f"{'='*70}")
    print(f"  카테고리 ETF:    {s['category_etfs']:>4}개")
    print(f"  거래대금 통과:   {s['after_liquidity']:>4}개")
    print(f"  유효 패턴 ETF:   {s['valid_patterns']:>4}개")
    print(f"  TOP {len(r['top_picks'])} 후보:        {s['top_picks_count']:>4}개")

    print(f"\n  [패턴 타입 분포]")
    for pt, cnt in sorted(r["by_pattern_type"].items(), key=lambda x: -x[1]):
        print(f"    {pt:<28s} {cnt:>3}개")

    print(f"\n{'='*70}")
    print(f"  🎯 26-27년 폭등 후보 TOP {len(r['top_picks'])}")
    print(f"{'='*70}")
    print(f"  {'순위':<4}{'ETF명':<32}{'점수':>6}{'패턴':>26}{'눌림':>9}{'거래대금':>10}")
    print(f"  {'-'*78}")
    for i, p in enumerate(r["top_picks"], 1):
        dd = p.get("current_drawdown_pct")
        dd_str = f"{dd:>+8.1f}%" if dd is not None else "      -"
        vol = p.get("avg_value_bil", 0)
        print(
            f"  {i:<4}{p['name'][:30]:<32s}"
            f"{p['score']:>5.1f} "
            f"{p.get('pattern_type', '-')[:24]:>26s}"
            f"{dd_str}"
            f"{vol:>9,.1f}억"
        )

    print(f"\n  [카테고리별 분포 — 어떤 산업이 많이 나왔나]")
    for cat, tickers in sorted(r["by_category"].items(), key=lambda x: -len(x[1])):
        print(f"    {cat:<12s} {len(tickers)}개: {', '.join(tickers)}")


if __name__ == "__main__":
    main()
