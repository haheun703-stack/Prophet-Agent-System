"""
확장 백테스트: KIS일봉(8개월) + yfinance 5분봉(60일) 통합 검증

Part 1: 60일 5분봉 → Body Hunter v2 실전 진입/청산 검증
Part 2: 8개월 일봉 → 일봉필터 + 스크리닝 정확도 검증
"""

import sys
import io
import logging
from pathlib import Path
from typing import List, Dict, Optional
from dataclasses import dataclass, asdict, field

import pandas as pd
import numpy as np

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from data.kis_collector import (
    collect_5min_yfinance,
    collect_daily_kis,
    UNIVERSE,
    DAILY_DIR,
    MIN5_DIR,
    _ensure_dirs,
)
from data.csv_loader import CSVLoader
from strategies.daily_filter import DailyFilter
from strategies.body_hunter_v2 import BodyHunterV2, BodyState
from engine.body_hunter_master import DailyTradeResult

logger = logging.getLogger(__name__)


# ============================================================
#  Part 1: 60일 5분봉 백테스트
# ============================================================

def run_5min_backtest(use_cache: bool = True):
    """60일 5분봉 데이터로 Body Hunter v2 백테스트"""

    print(f"\n{'='*60}")
    print(f"  Part 1: 60일 5분봉 백테스트 (Body Hunter v2)")
    print(f"  데이터: yfinance 실제 5분봉 (~60일)")
    print(f"{'='*60}")

    # 데이터 로드
    etf_df, stock_data = collect_5min_yfinance(force=not use_cache)

    if etf_df is None or etf_df.empty:
        print("  ETF 데이터 없음!")
        return []

    code_to_name = {code: info[0] for code, info in UNIVERSE.items()}

    # 거래일 추출
    etf_df["date"] = etf_df.index.date
    trading_days = sorted(etf_df["date"].unique())
    print(f"  거래일: {len(trading_days)}일 ({trading_days[0]} ~ {trading_days[-1]})")

    # KIS 일봉 데이터로 일봉 필터
    print(f"\n  일봉 필터 적용중...")
    daily_whitelist = _build_daily_whitelist(stock_data.keys(), code_to_name)
    print(f"  일봉 필터 통과: {len(daily_whitelist)}종목")

    # 일별 백테스트
    all_results = []
    risk_per_trade = 50_000
    equity = [0.0]

    for day_date in trading_days:
        day_results = _simulate_day(
            day_date, etf_df, stock_data, daily_whitelist, code_to_name, risk_per_trade
        )
        all_results.extend(day_results)
        day_pnl = sum(risk_per_trade * r.rr_realized for r in day_results)
        equity.append(equity[-1] + day_pnl)

    # 리포트
    _print_5min_report(all_results, equity, trading_days)
    return all_results


def _build_daily_whitelist(codes, code_to_name):
    """KIS 일봉 데이터로 일봉 필터 수행"""
    daily_whitelist = {}

    # 먼저 KIS 일봉 캐시 시도
    _ensure_dirs()
    daily_filter = DailyFilter()

    for code in codes:
        df_daily = None

        # KIS 일봉 캐시 확인
        cache_file = DAILY_DIR / f"{code}.csv"
        if cache_file.exists():
            try:
                df_daily = pd.read_csv(cache_file, index_col=0, parse_dates=True)
            except Exception:
                pass

        # CSV loader 폴백
        if df_daily is None or len(df_daily) < 60:
            csv_loader = CSVLoader()
            df_daily = csv_loader.load(code)

        if df_daily is None or len(df_daily) < 60:
            continue

        name = code_to_name.get(code, code)
        score = daily_filter.score_stock(df_daily, code, name)
        if score and score.total_score >= 50:
            daily_whitelist[code] = (score.total_score, score.grade)

    return daily_whitelist


def _simulate_day(day_date, etf_df, stock_data, daily_whitelist, code_to_name, risk_per_trade):
    """하루 시뮬레이션"""

    etf_day = etf_df[etf_df["date"] == day_date].copy()
    if len(etf_day) < 10:
        return []

    # 첫 4봉 ETF 방향 판단
    first_bars = etf_day.iloc[:4]
    etf_open = float(first_bars.iloc[0]["open"])
    etf_close_20 = float(first_bars.iloc[-1]["close"])
    etf_change = (etf_close_20 - etf_open) / etf_open * 100

    if etf_change <= 0.15:
        return []

    # 후보 스캔
    candidates = []
    for code, df in stock_data.items():
        if code not in daily_whitelist:
            continue

        day_df = df[df.index.date == day_date]
        if len(day_df) < 10:
            continue

        stock_first = day_df.iloc[:4]
        s_open = float(stock_first.iloc[0]["open"])
        s_close = float(stock_first.iloc[-1]["close"])
        if s_open == 0:
            continue
        s_change = (s_close - s_open) / s_open * 100

        if s_change <= 0:
            continue

        rel_str = abs(s_change) / abs(etf_change) if abs(etf_change) > 0 else 0
        if rel_str < 1.5:
            continue

        recent_vol = float(stock_first["volume"].mean())
        if recent_vol < 1000:
            continue

        h4 = float(stock_first["high"].max())
        l4 = float(stock_first["low"].min())
        daily_s, daily_g = daily_whitelist.get(code, (0, ""))
        daily_bonus = (daily_s / 100.0) * 2.5
        score = rel_str * 0.3 + daily_bonus + abs(s_change) * 0.15
        name = code_to_name.get(code, code)

        candidates.append({
            "code": code, "name": name,
            "change": s_change, "rel_str": rel_str,
            "score": score, "h4": h4, "l4": l4,
            "daily_score": daily_s, "daily_grade": daily_g,
        })

    candidates.sort(key=lambda x: x["score"], reverse=True)
    top3 = candidates[:3]

    if not top3:
        return []

    # Body Hunter v2 실행
    day_results = []
    for cand in top3:
        code = cand["code"]
        day_df = stock_data[code][stock_data[code].index.date == day_date]

        if len(day_df) <= 4:
            continue

        first_candle = pd.Series({
            "open": float(day_df.iloc[0]["open"]),
            "high": cand["h4"],
            "low": cand["l4"],
            "close": float(day_df.iloc[3]["close"]),
            "volume": float(day_df.iloc[:4]["volume"].mean()),
        })

        hunter = BodyHunterV2(
            ticker=code, name=cand["name"], direction="LONG",
            retest_required=False, close_only_breakout=True,
            volume_surge_min=1.0, trailing_atr_mult=2.0,
            breakeven_rr=0.3, trailing_rr=1.0,
            exhaustion_bars=3, wick_ratio_min=0.005,
        )
        hunter.set_levels(first_candle, avg_volume=float(day_df.iloc[:4]["volume"].mean()))

        for idx in range(4, len(day_df)):
            row = day_df.iloc[idx]
            candle = pd.Series({
                "open": float(row["open"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "close": float(row["close"]),
                "volume": float(row["volume"]),
            })
            candle.name = day_df.index[idx]

            result = hunter.update(candle)

            if result["action"] == "EXIT":
                pos = result.get("position")
                if not pos:
                    break

                entry = pos.entry_price
                exit_p = result.get("exit_price", float(row["close"]))
                pnl_pct = (exit_p - entry) / entry * 100
                ex_signals = result["exhaustion"].signals if result.get("exhaustion") else []

                day_results.append(DailyTradeResult(
                    date=str(day_date),
                    ticker=code, name=cand["name"],
                    direction="LONG",
                    entry_price=entry, exit_price=exit_p,
                    exit_reason=result.get("reason", ""),
                    rr_realized=result.get("rr_realized", 0),
                    hold_bars=result.get("hold_bars", 0),
                    pnl_pct=pnl_pct,
                    exhaustion_signals=ex_signals,
                ))
                break

    return day_results


def _print_5min_report(results, equity, trading_days):
    """5분봉 백테스트 리포트"""
    if not results:
        print("\n  거래 결과 없음")
        return

    df = pd.DataFrame([asdict(r) for r in results])

    wins = (df["rr_realized"] > 0).sum()
    losses = (df["rr_realized"] < 0).sum()
    breakeven = (df["rr_realized"] == 0).sum()
    total = len(df)
    wr = wins / total * 100
    avg_rr = df["rr_realized"].mean()
    total_pnl = equity[-1]

    eq_series = pd.Series(equity)
    max_dd = (eq_series - eq_series.cummax()).min()

    # 수익팩터
    gross_profit = df[df["rr_realized"] > 0]["rr_realized"].sum()
    gross_loss = abs(df[df["rr_realized"] < 0]["rr_realized"].sum())
    profit_factor = gross_profit / gross_loss if gross_loss > 0 else float("inf")

    exit_dist = df["exit_reason"].value_counts()
    good_exits = df[df["exit_reason"].isin(["소진감지", "트레일링", "수익잠금"])].shape[0]

    # 월별 성과
    df["month"] = pd.to_datetime(df["date"]).dt.to_period("M")
    monthly = df.groupby("month").agg(
        trades=("rr_realized", "count"),
        wins=("rr_realized", lambda x: (x > 0).sum()),
        avg_rr=("rr_realized", "mean"),
        total_rr=("rr_realized", "sum"),
    )

    print(f"\n{'='*60}")
    print(f"  [Part 1] 60일 5분봉 백테스트 결과")
    print(f"  기간: {trading_days[0]} ~ {trading_days[-1]} ({len(trading_days)}거래일)")
    print(f"{'='*60}")
    print(f"  총 거래    : {total}회 ({wins}승/{losses}패/{breakeven}본전)")
    print(f"  승률       : {wr:.1f}%")
    print(f"  평균 RR    : {avg_rr:+.2f}")
    print(f"  수익팩터   : {profit_factor:.2f}")
    print(f"  총 PnL     : {total_pnl:+,.0f}원 (리스크 50,000원/회)")
    print(f"  최대 낙폭  : {max_dd:,.0f}원")
    print(f"  좋은청산   : {good_exits}/{total} ({good_exits/total*100:.1f}%)")
    print(f"{'-'*60}")

    print(f"  월별 성과:")
    for period, row in monthly.iterrows():
        wr_m = row["wins"] / row["trades"] * 100 if row["trades"] > 0 else 0
        pnl = row["total_rr"] * 50_000
        print(f"    {period}: {row['trades']:.0f}거래 승률{wr_m:.0f}% 평균RR:{row['avg_rr']:+.2f} PnL:{pnl:+,.0f}원")

    print(f"{'-'*60}")
    print(f"  청산사유 분포:")
    for reason, cnt in exit_dist.items():
        pct = cnt / total * 100
        bar = "#" * int(pct / 3)
        print(f"    {reason:<12} {cnt:3d}회 ({pct:.1f}%) {bar}")

    print(f"{'-'*60}")
    print(f"  거래 상세:")
    for r in results:
        icon = "+" if r.rr_realized > 0 else ("-" if r.rr_realized < 0 else "=")
        ex = f" [{','.join(r.exhaustion_signals[:2])}]" if r.exhaustion_signals else ""
        print(
            f"    {icon} {r.date} {r.ticker}({r.name}) "
            f"진입:{r.entry_price:,.0f}→청산:{r.exit_price:,.0f} "
            f"RR:{r.rr_realized:+.2f} {r.hold_bars}봉 [{r.exit_reason}]{ex}"
        )
    print(f"{'='*60}")


# ============================================================
#  Part 2: 8개월 일봉 스크리닝 검증
# ============================================================

def run_daily_screening_test(use_cache: bool = True):
    """8개월 일봉 데이터로 스크리닝 정확도 검증

    일봉 필터 통과 종목 중 실제로 상승한 비율 측정
    = '필터가 실제로 유용한가?'
    """

    print(f"\n{'='*60}")
    print(f"  Part 2: 8개월 일봉 스크리닝 검증")
    print(f"  KIS API 일봉 데이터로 필터 정확도 측정")
    print(f"{'='*60}")

    # KIS 일봉 캐시 로드
    daily_data = {}
    _ensure_dirs()
    for code in UNIVERSE:
        cache_file = DAILY_DIR / f"{code}.csv"
        if cache_file.exists():
            try:
                df = pd.read_csv(cache_file, index_col=0, parse_dates=True)
                if len(df) > 20:
                    daily_data[code] = df
            except Exception:
                continue

    if not daily_data:
        print("  일봉 데이터 없음! collect_daily_kis() 먼저 실행하세요.")
        return

    etf_df = daily_data.get("069500")
    if etf_df is None:
        print("  KODEX200 일봉 없음!")
        return

    code_to_name = {code: info[0] for code, info in UNIVERSE.items()}
    daily_filter = DailyFilter()

    # 슬라이딩 윈도우: 60일 lookback으로 필터 → 향후 5일 성과 측정
    dates = etf_df.index.tolist()
    lookback = 60
    forward = 5

    filter_results = []

    print(f"\n  총 {len(dates)}거래일, lookback={lookback}, forward={forward}일")
    print(f"  검증 가능 기간: {dates[lookback]} ~ {dates[-forward-1]}")

    for i in range(lookback, len(dates) - forward):
        eval_date = dates[i]

        # ETF 방향 (당일)
        etf_today = etf_df.iloc[i]
        etf_prev = etf_df.iloc[i - 1]
        etf_chg = (float(etf_today["close"]) - float(etf_prev["close"])) / float(etf_prev["close"]) * 100

        # 일봉 필터 적용 (lookback 데이터 사용)
        for code, df in daily_data.items():
            if code == "069500":
                continue

            # lookback 기간 데이터
            mask = df.index <= eval_date
            df_hist = df[mask].tail(lookback)
            if len(df_hist) < lookback:
                continue

            name = code_to_name.get(code, code)
            score = daily_filter.score_stock(df_hist, code, name)
            if not score:
                continue

            passed = score.total_score >= 50

            # 향후 5일 수익률
            future_mask = df.index > eval_date
            future = df[future_mask].head(forward)
            if len(future) < 1:
                continue

            entry_price = float(df_hist.iloc[-1]["close"])
            max_price = float(future["high"].max())
            end_price = float(future.iloc[-1]["close"])

            max_gain = (max_price - entry_price) / entry_price * 100
            end_gain = (end_price - entry_price) / entry_price * 100

            filter_results.append({
                "date": eval_date,
                "code": code,
                "name": name,
                "score": score.total_score,
                "grade": score.grade,
                "passed": passed,
                "etf_chg": etf_chg,
                "max_gain_5d": max_gain,
                "end_gain_5d": end_gain,
            })

    if not filter_results:
        print("  검증 결과 없음")
        return

    df_results = pd.DataFrame(filter_results)

    # 분석
    passed = df_results[df_results["passed"]]
    failed = df_results[~df_results["passed"]]

    print(f"\n{'-'*60}")
    print(f"  일봉 필터 검증 결과")
    print(f"{'-'*60}")
    print(f"  전체 샘플: {len(df_results)}건")
    print(f"  필터 통과: {len(passed)}건 ({len(passed)/len(df_results)*100:.1f}%)")
    print(f"  필터 탈락: {len(failed)}건 ({len(failed)/len(df_results)*100:.1f}%)")

    print(f"\n  향후 5일 성과 비교:")
    print(f"  {'구분':<12} {'평균종가등락':>10} {'평균최대이익':>10} {'양봉비율':>8}")

    for label, subset in [("필터통과", passed), ("필터탈락", failed), ("전체", df_results)]:
        avg_end = subset["end_gain_5d"].mean()
        avg_max = subset["max_gain_5d"].mean()
        pos_rate = (subset["end_gain_5d"] > 0).mean() * 100
        print(f"  {label:<12} {avg_end:>+9.2f}% {avg_max:>+9.2f}% {pos_rate:>7.1f}%")

    # 등급별 분석
    print(f"\n  등급별 성과:")
    for grade in ["S", "A", "B", "C", "D", "F"]:
        subset = df_results[df_results["grade"] == grade]
        if len(subset) < 5:
            continue
        avg_end = subset["end_gain_5d"].mean()
        avg_max = subset["max_gain_5d"].mean()
        pos_rate = (subset["end_gain_5d"] > 0).mean() * 100
        print(f"    [{grade}] {len(subset):4d}건 종가{avg_end:+.2f}% 최대{avg_max:+.2f}% 양봉{pos_rate:.0f}%")

    # ETF 상승일 vs 하락일
    print(f"\n  ETF 방향별 필터 효과:")
    for label, cond in [("ETF상승일", df_results["etf_chg"] > 0), ("ETF하락일", df_results["etf_chg"] <= 0)]:
        subset = df_results[cond]
        if len(subset) < 10:
            continue
        p = subset[subset["passed"]]
        f = subset[~subset["passed"]]
        if len(p) > 0 and len(f) > 0:
            p_gain = p["end_gain_5d"].mean()
            f_gain = f["end_gain_5d"].mean()
            edge = p_gain - f_gain
            print(f"    {label}: 통과{p_gain:+.2f}% vs 탈락{f_gain:+.2f}% → 엣지{edge:+.2f}%")

    print(f"{'='*60}")

    return df_results


# ============================================================
#  메인: 전체 실행
# ============================================================

if __name__ == "__main__":
    logging.basicConfig(level=logging.WARNING)

    # Part 1: 60일 5분봉 백테스트
    results_5min = run_5min_backtest(use_cache=True)

    # Part 2: 8개월 일봉 스크리닝 검증
    results_daily = run_daily_screening_test(use_cache=True)
