"""
실제 5분봉 데이터 기반 Body Hunter v2 백테스트
yfinance에서 실제 한국 주식 5분봉을 받아 검증

시뮬레이션 데이터 vs 실제 데이터 차이를 명확히 비교
"""

import sys
import logging
from pathlib import Path
from typing import List, Dict, Optional, Tuple
from dataclasses import asdict

import pandas as pd
import numpy as np
import yfinance as yf

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from data.csv_loader import CSVLoader
from strategies.etf_filter import ETFDirectionFilter, MarketDirection
from strategies.scanner import RelativeStrengthScanner, StockCandidate
from strategies.daily_filter import DailyFilter
from strategies.body_hunter_v2 import BodyHunterV2, BodyState
from engine.body_hunter_master import DailyTradeResult

logger = logging.getLogger(__name__)


# 거래량 상위 종목 (KOSPI + KOSDAQ 혼합)
TOP_STOCKS = {
    # KOSPI
    "005930": ("삼성전자", ".KS"),
    "000660": ("SK하이닉스", ".KS"),
    "005380": ("현대차", ".KS"),
    "035420": ("NAVER", ".KS"),
    "000270": ("기아", ".KS"),
    "006400": ("삼성SDI", ".KS"),
    "051910": ("LG화학", ".KS"),
    "035720": ("카카오", ".KS"),
    "003670": ("포스코퓨처엠", ".KS"),
    "028260": ("삼성물산", ".KS"),
    "066570": ("LG전자", ".KS"),
    "003550": ("LG", ".KS"),
    "055550": ("신한지주", ".KS"),
    "105560": ("KB금융", ".KS"),
    "086790": ("하나금융지주", ".KS"),
    "096770": ("SK이노베이션", ".KS"),
    "032830": ("삼성생명", ".KS"),
    "017670": ("SK텔레콤", ".KS"),
    "030200": ("KT", ".KS"),
    "010950": ("S-Oil", ".KS"),
    "005830": ("DB손해보험", ".KS"),
    "034730": ("SK", ".KS"),
    "036570": ("엔씨소프트", ".KS"),
    "012330": ("현대모비스", ".KS"),
    "011200": ("HMM", ".KS"),
    # KOSDAQ
    "247540": ("에코프로비엠", ".KQ"),
    "086520": ("에코프로", ".KQ"),
    "403870": ("HPSP", ".KQ"),
    "041510": ("에스엠", ".KQ"),
    "293490": ("카카오게임즈", ".KQ"),
}


def download_5min_data(period: str = "1mo") -> Tuple[pd.DataFrame, Dict[str, pd.DataFrame]]:
    """yfinance에서 실제 5분봉 데이터 다운로드"""

    print("  실제 5분봉 데이터 다운로드 중...")

    # 1. KODEX200 ETF
    etf_ticker = "069500.KS"
    etf_df = yf.download(etf_ticker, period=period, interval="5m", progress=False)
    if isinstance(etf_df.columns, pd.MultiIndex):
        etf_df.columns = etf_df.columns.get_level_values(0)
    etf_df = etf_df.rename(columns={
        "Open": "open", "High": "high", "Low": "low",
        "Close": "close", "Volume": "volume"
    })
    print(f"    KODEX200: {len(etf_df)}봉 ({etf_df.index[0].date()} ~ {etf_df.index[-1].date()})")

    # 2. 개별종목
    stock_data = {}
    tickers_str = " ".join(f"{code}{suffix}" for code, (_, suffix) in TOP_STOCKS.items())
    bulk = yf.download(tickers_str, period=period, interval="5m", progress=False, group_by="ticker")

    for code, (name, suffix) in TOP_STOCKS.items():
        yf_ticker = f"{code}{suffix}"
        try:
            if isinstance(bulk.columns, pd.MultiIndex):
                df = bulk[yf_ticker].copy()
            else:
                df = bulk.copy()
            df = df.rename(columns={
                "Open": "open", "High": "high", "Low": "low",
                "Close": "close", "Volume": "volume"
            })
            df = df.dropna(subset=["close"])
            if len(df) > 50:
                stock_data[code] = df
        except (KeyError, Exception):
            continue

    print(f"    개별종목: {len(stock_data)}개 로드 완료")
    return etf_df, stock_data


def run_real_backtest(period: str = "1mo"):
    """실제 5분봉 데이터로 Body Hunter v2 백테스트"""

    print(f"\n{'='*60}")
    print(f"  Body Hunter v2 - 실제 5분봉 백테스트")
    print(f"  데이터: yfinance (실제 시장 데이터)")
    print(f"  기간: 최근 {period}")
    print(f"{'='*60}")

    # 데이터 다운로드
    etf_df, stock_data = download_5min_data(period)

    if etf_df.empty:
        print("  ETF 데이터 없음!")
        return

    # 거래일 목록 추출
    etf_df["date"] = etf_df.index.date
    trading_days = sorted(etf_df["date"].unique())
    print(f"  거래일: {len(trading_days)}일")
    print()

    # CSV 일봉 데이터 로드 (일봉 필터용)
    csv_loader = CSVLoader()
    daily_filter = DailyFilter()
    code_to_name = {code: name for code, (name, _) in TOP_STOCKS.items()}

    # 일봉 필터 - CSV 데이터로 사전 스크리닝
    daily_whitelist = {}
    for code in stock_data.keys():
        df_daily = csv_loader.load(code)
        if df_daily is not None and len(df_daily) >= 60:
            name = code_to_name.get(code, code)
            score = daily_filter.score_stock(df_daily, code, name)
            if score and score.total_score >= 50:
                daily_whitelist[code] = (score.total_score, score.grade)

    print(f"  일봉 필터 통과: {len(daily_whitelist)}종목")
    for code, (s, g) in sorted(daily_whitelist.items(), key=lambda x: -x[1][0])[:10]:
        name = code_to_name.get(code, code)
        print(f"    [{g}] {code}({name}) {s:.0f}점")
    print()

    # 일별 백테스트
    all_results = []
    risk_per_trade = 50_000
    equity = [0.0]

    for day_date in trading_days:
        day_results = _simulate_real_day(
            day_date, etf_df, stock_data, daily_whitelist, code_to_name, risk_per_trade
        )
        all_results.extend(day_results)

        day_pnl = sum(risk_per_trade * r.rr_realized for r in day_results)
        equity.append(equity[-1] + day_pnl)

    # 리포트
    _print_report(all_results, equity, code_to_name)
    return all_results


def _simulate_real_day(
    day_date, etf_df, stock_data, daily_whitelist, code_to_name, risk_per_trade
) -> List[DailyTradeResult]:
    """하루 실제 데이터 시뮬레이션"""

    # 해당일 ETF 5분봉 추출
    etf_day = etf_df[etf_df["date"] == day_date].copy()
    if len(etf_day) < 10:
        return []

    # 첫 4봉 (9:00~9:20) → ETF 방향 판단
    first_bars = etf_day.iloc[:4]
    etf_open = float(first_bars.iloc[0]["open"])
    etf_close_20 = float(first_bars.iloc[-1]["close"])
    etf_change = (etf_close_20 - etf_open) / etf_open * 100

    # LONG ONLY
    if etf_change <= 0.15:
        return []

    # 9:20 시점 각 종목의 첫 4봉 기준 상대강도 스캔
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

        # 거래량 확인
        recent_vol = float(stock_first["volume"].mean())
        if recent_vol < 1000:
            continue

        h4 = float(stock_first["high"].max())
        l4 = float(stock_first["low"].min())
        mid = (h4 + l4) / 2

        daily_s, daily_g = daily_whitelist.get(code, (0, ""))
        daily_bonus = (daily_s / 100.0) * 2.5

        score = rel_str * 0.3 + daily_bonus + abs(s_change) * 0.15
        name = code_to_name.get(code, code)

        candidates.append({
            "code": code, "name": name,
            "change": s_change, "rel_str": rel_str,
            "score": score, "h4": h4, "l4": l4, "mid": mid,
            "daily_score": daily_s, "daily_grade": daily_g,
        })

    candidates.sort(key=lambda x: x["score"], reverse=True)
    top3 = candidates[:3]

    if not top3:
        return []

    # 각 종목 Body Hunter v2 실행 (9:20 이후 봉으로)
    day_results = []
    for cand in top3:
        code = cand["code"]
        day_df = stock_data[code][stock_data[code].index.date == day_date]

        # 9:20 이후 봉 = index 4부터
        if len(day_df) <= 4:
            continue

        # 첫봉 레벨: 처음 4봉의 H/L
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
            breakeven_bars=6, exhaustion_bars=3, wick_ratio_min=0.005,
        )
        hunter.set_levels(first_candle, avg_volume=float(day_df.iloc[:4]["volume"].mean()))

        # 5번째 봉부터 업데이트
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


def _print_report(results, equity, code_to_name):
    """결과 리포트"""
    if not results:
        print("\n  거래 결과 없음 (조건 맞는 날이 없었음)")
        return

    df = pd.DataFrame([asdict(r) for r in results])

    wins = (df["rr_realized"] > 0).sum()
    losses = (df["rr_realized"] <= 0).sum()
    total = len(df)
    wr = wins / total * 100
    avg_rr = df["rr_realized"].mean()
    total_pnl = equity[-1]

    eq_series = pd.Series(equity)
    max_dd = (eq_series - eq_series.cummax()).min()

    exit_dist = df["exit_reason"].value_counts()
    good_exits = df[df["exit_reason"].isin(["소진감지", "트레일링", "수익잠금"])].shape[0]

    print(f"\n{'='*60}")
    print(f"  실제 5분봉 백테스트 결과")
    print(f"  (yfinance 실데이터 - 시뮬레이션 아님)")
    print(f"{'='*60}")
    print(f"  총 거래    : {total}회 ({wins}승/{losses}패)")
    print(f"  승률       : {wr:.1f}%")
    print(f"  평균 RR    : {avg_rr:+.2f}")
    print(f"  총 PnL     : {total_pnl:+,.0f}원 (리스크 50,000원/회)")
    print(f"  최대 낙폭  : {max_dd:,.0f}원")
    print(f"  좋은청산   : {good_exits}/{total} ({good_exits/total*100:.1f}%)")
    print(f"{'-'*60}")
    print(f"  청산사유 분포:")
    for reason, cnt in exit_dist.items():
        pct = cnt / total * 100
        bar = "#" * int(pct / 3)
        print(f"    {reason:<12} {cnt:3d}회 ({pct:.1f}%) {bar}")
    print(f"{'-'*60}")

    print(f"  거래 상세:")
    for r in results:
        icon = "+" if r.rr_realized > 0 else "-"
        ex = f" [{','.join(r.exhaustion_signals[:2])}]" if r.exhaustion_signals else ""
        print(
            f"    {icon} {r.date} {r.ticker}({r.name}) "
            f"진입:{r.entry_price:,.0f} -> 청산:{r.exit_price:,.0f} "
            f"RR:{r.rr_realized:+.2f} {r.hold_bars}봉 [{r.exit_reason}]{ex}"
        )
    print(f"{'='*60}")


if __name__ == "__main__":
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    logging.basicConfig(level=logging.WARNING)

    results = run_real_backtest(period="1mo")
