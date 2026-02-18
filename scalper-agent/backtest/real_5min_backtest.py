# -*- coding: utf-8 -*-
"""
실제 5분봉 기반 v2.3 vs v3.1 비교 백테스트
=============================================
yfinance 5분봉 실데이터 (59일, 5종목) 사용

v3.1 핵심 수정:
  1. Engulfing 없으면 진입 금지 (기존 유지)
  2. FCR high/low 근처 FVG만 유효 (proximity 필터)
  3. SL = FVG 하단 바깥쪽 (buffer 추가)

WR 필터:
  종목별 WR < 40% → 수급 등급 무관 제외
  2-pass: 1차 전체 → 2차 WR>=40% 종목만
"""

import sys, os
os.environ["PYTHONIOENCODING"] = "utf-8"
sys.path.insert(0, "D:/Prophet_Agent_System_예언자/scalper-agent")
os.chdir("D:/Prophet_Agent_System_예언자/scalper-agent")

import logging
from pathlib import Path
from dataclasses import dataclass, asdict
from typing import List, Dict, Optional
import pandas as pd
import numpy as np

from strategies.body_hunter_v2 import BodyHunterV2, BodyState as StateV2
from strategies.body_hunter_v3 import BodyHunterV3, BodyState as StateV3

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)

MINUTE5_DIR = Path("data_store/minute5")

STOCKS = {
    "005930": "삼성전자",
    "000660": "SK하이닉스",
    "005380": "현대차",
    "035420": "NAVER",
    "068270": "셀트리온",
}


@dataclass
class TradeResult:
    date: str
    code: str
    name: str
    version: str
    direction: str
    entry_price: float
    exit_price: float
    exit_reason: str
    rr_realized: float
    hold_bars: int
    pnl_pct: float
    entered: bool
    sl_price: float = 0.0
    tp_price: float = 0.0


def load_5min(code: str) -> Optional[pd.DataFrame]:
    """실제 5분봉 로드"""
    path = MINUTE5_DIR / f"{code}.csv"
    if not path.exists():
        return None
    df = pd.read_csv(path, index_col=0, parse_dates=True)
    return df


def run_day_v23(candles: pd.DataFrame, code: str, name: str,
                date_str: str) -> TradeResult:
    """v2.3: 하루치 실제 5분봉으로 시뮬"""
    hunter = BodyHunterV2(
        ticker=code, name=name, direction="LONG",
        retest_required=True, volume_surge_min=1.3,
        trailing_atr_mult=1.2, breakeven_rr=0.3,
        sl_ratio=0.6, fixed_tp_rr=2.0,
        close_only_breakout=True,
        choppy_max_attempts=3,
    )

    # 첫봉으로 레벨 설정 (09:00 봉)
    first = candles.iloc[0]
    avg_vol = candles["volume"].iloc[:5].mean() if len(candles) >= 5 else first["volume"]
    first_series = pd.Series({
        "high": first["high"], "low": first["low"],
        "open": first["open"], "close": first["close"],
        "volume": first["volume"],
    })
    hunter.set_levels(first_series, avg_volume=avg_vol)

    for i in range(1, len(candles)):
        row = candles.iloc[i]
        candle = pd.Series({
            "open": row["open"], "high": row["high"],
            "low": row["low"], "close": row["close"],
            "volume": row["volume"],
        })
        candle.name = candles.index[i]

        result = hunter.update(candle)

        if result["action"] == "EXIT":
            pos = result.get("position")
            if not pos:
                break
            entry = pos.entry_price
            exit_p = result.get("exit_price", row["close"])
            pnl = (exit_p - entry) / entry * 100
            return TradeResult(
                date=date_str, code=code, name=name, version="v2.3",
                direction="LONG", entry_price=entry, exit_price=exit_p,
                exit_reason=result.get("reason", ""),
                rr_realized=result.get("rr_realized", 0),
                hold_bars=result.get("hold_bars", 0),
                pnl_pct=pnl, entered=True,
                sl_price=pos.stop_loss,
            )

    return TradeResult(
        date=date_str, code=code, name=name, version="v2.3",
        direction="LONG", entry_price=0, exit_price=0,
        exit_reason="미진입", rr_realized=0, hold_bars=0,
        pnl_pct=0, entered=False,
    )


def run_day_v3(candles: pd.DataFrame, code: str, name: str,
               date_str: str) -> TradeResult:
    """v3.1: FVG+Engulfing + FCR근처필터 + SL바깥쪽"""
    hunter = BodyHunterV3(
        ticker=code, name=name, direction="LONG",
        volume_surge_min=1.3,
        trailing_atr_mult=1.2, breakeven_rr=0.3,
        fixed_tp_rr=3.0,
        fvg_timeout_bars=20,
        fvg_min_size_pct=0.0005,
        engulf_timeout_bars=15,
        fvg_fcr_proximity=1.5,     # FCR range의 1.5배 이내 FVG만
        sl_buffer_ratio=0.1,       # FVG 하단에서 10% 바깥에 SL
        close_only_breakout=True,
    )

    first = candles.iloc[0]
    avg_vol = candles["volume"].iloc[:5].mean() if len(candles) >= 5 else first["volume"]
    first_series = pd.Series({
        "high": first["high"], "low": first["low"],
        "open": first["open"], "close": first["close"],
        "volume": first["volume"],
    })
    hunter.set_levels(first_series, avg_volume=avg_vol)

    for i in range(1, len(candles)):
        row = candles.iloc[i]
        candle = pd.Series({
            "open": row["open"], "high": row["high"],
            "low": row["low"], "close": row["close"],
            "volume": row["volume"],
        })
        candle.name = candles.index[i]

        result = hunter.update(candle)

        if result["action"] == "EXIT":
            pos = result.get("position")
            if not pos:
                break
            entry = pos.entry_price
            exit_p = result.get("exit_price", row["close"])
            pnl = (exit_p - entry) / entry * 100
            sl = pos.stop_loss
            risk = abs(entry - sl)
            tp = entry + risk * 3.0
            return TradeResult(
                date=date_str, code=code, name=name, version="v3",
                direction="LONG", entry_price=entry, exit_price=exit_p,
                exit_reason=result.get("reason", ""),
                rr_realized=result.get("rr_realized", 0),
                hold_bars=result.get("hold_bars", 0),
                pnl_pct=pnl, entered=True,
                sl_price=sl, tp_price=tp,
            )

    return TradeResult(
        date=date_str, code=code, name=name, version="v3",
        direction="LONG", entry_price=0, exit_price=0,
        exit_reason="미진입", rr_realized=0, hold_bars=0,
        pnl_pct=0, entered=False,
    )


def calc_stock_wr(results: List[TradeResult]) -> dict:
    """종목별 WR 계산"""
    by_stock = {}
    for r in results:
        if not r.entered:
            continue
        if r.code not in by_stock:
            by_stock[r.code] = {"wins": 0, "total": 0}
        by_stock[r.code]["total"] += 1
        if r.rr_realized > 0:
            by_stock[r.code]["wins"] += 1

    wr_map = {}
    for code, s in by_stock.items():
        wr_map[code] = s["wins"] / s["total"] * 100 if s["total"] > 0 else 0
    return wr_map


def main():
    WR_THRESHOLD = 40.0

    print(f"\n{'='*70}")
    print(f"  실제 5분봉 v2.3 vs v3.1 비교 백테스트")
    print(f"  데이터: yfinance 5분봉 (실데이터)")
    print(f"  종목: {', '.join(f'{n}({c})' for c, n in STOCKS.items())}")
    print(f"  v3.1: FCR근처FVG + Engulfing필수 + SL=FVG바깥")
    print(f"{'='*70}\n")

    # 데이터 로드
    all_data = {}
    for code, name in STOCKS.items():
        df = load_5min(code)
        if df is not None:
            all_data[code] = df
            days = df.index.normalize().nunique()
            print(f"  {name}({code}): {len(df)}봉, {days}일")
    print()

    # ─── PASS 1: 전체 종목 백테스트 ───
    print(f"{'─'*70}")
    print(f"  [PASS 1] 전체 5종목 백테스트")
    print(f"{'─'*70}")

    results_v23: List[TradeResult] = []
    results_v3: List[TradeResult] = []

    for code, name in STOCKS.items():
        df = all_data.get(code)
        if df is None:
            continue

        dates = sorted(df.index.normalize().unique())

        for date in dates:
            date_str = date.strftime("%Y-%m-%d")
            day_data = df[df.index.normalize() == date].copy()

            if len(day_data) < 20:
                continue

            r23 = run_day_v23(day_data, code, name, date_str)
            results_v23.append(r23)

            r3 = run_day_v3(day_data, code, name, date_str)
            results_v3.append(r3)

    # PASS 1 결과 출력
    print_results(results_v23, results_v3)

    # ─── WR 필터: 종목별 WR 계산 ───
    wr_v23 = calc_stock_wr(results_v23)
    wr_v3  = calc_stock_wr(results_v3)

    print(f"\n{'='*70}")
    print(f"  종목별 WR (PASS 1 기준)")
    print(f"{'='*70}")
    print(f"  {'종목':>12} {'v2.3 WR':>10} {'v3.1 WR':>10} {'필터':>8}")
    print(f"  {'-'*44}")
    qualified_codes = set()
    for code, name in STOCKS.items():
        w23 = wr_v23.get(code, 0)
        w3  = wr_v3.get(code, 0)
        # v2.3 기준 WR >= 40% 종목만 통과
        passed = w23 >= WR_THRESHOLD
        if passed:
            qualified_codes.add(code)
        mark = "PASS" if passed else "FAIL"
        print(f"  {name:>10}({code}) {w23:>7.1f}% {w3:>9.1f}% {mark:>8}")

    if not qualified_codes:
        print(f"\n  WR >= {WR_THRESHOLD}% 종목 없음. 필터 없이 종료.")
        return

    print(f"\n  통과 종목: {', '.join(STOCKS[c] for c in qualified_codes)}")

    # ─── PASS 2: WR 필터 적용 백테스트 ───
    print(f"\n{'─'*70}")
    print(f"  [PASS 2] WR >= {WR_THRESHOLD}% 종목만 ({len(qualified_codes)}종목)")
    print(f"{'─'*70}")

    filtered_v23: List[TradeResult] = []
    filtered_v3: List[TradeResult] = []

    for code in qualified_codes:
        name = STOCKS[code]
        df = all_data.get(code)
        if df is None:
            continue

        dates = sorted(df.index.normalize().unique())

        for date in dates:
            date_str = date.strftime("%Y-%m-%d")
            day_data = df[df.index.normalize() == date].copy()

            if len(day_data) < 20:
                continue

            r23 = run_day_v23(day_data, code, name, date_str)
            filtered_v23.append(r23)

            r3 = run_day_v3(day_data, code, name, date_str)
            filtered_v3.append(r3)

    print_results(filtered_v23, filtered_v3)

    # PASS 2 CSV 저장
    all_r2 = filtered_v23 + filtered_v3
    df2 = pd.DataFrame([asdict(r) for r in all_r2])
    out2 = Path("results/real_5min_wr_filtered.csv")
    out2.parent.mkdir(exist_ok=True)
    df2.to_csv(out2, index=False, encoding="utf-8-sig")
    print(f"PASS 2 저장: {out2}")


def print_results(results_v23: List[TradeResult], results_v3: List[TradeResult]):
    """비교 결과 출력"""
    def stats(results, label):
        total_attempts = len(results)
        entered = [r for r in results if r.entered]
        not_entered = total_attempts - len(entered)

        if not entered:
            print(f"\n  [{label}] 진입 거래 없음 (시도: {total_attempts})")
            return {}

        total = len(entered)
        wins = sum(1 for r in entered if r.rr_realized > 0)
        losses = total - wins
        wr = wins / total * 100 if total > 0 else 0
        avg_rr = sum(r.rr_realized for r in entered) / total
        total_rr = sum(r.rr_realized for r in entered)

        reasons = {}
        for r in entered:
            reasons[r.exit_reason] = reasons.get(r.exit_reason, 0) + 1

        avg_win = sum(r.rr_realized for r in entered if r.rr_realized > 0) / max(wins, 1)
        avg_loss = sum(r.rr_realized for r in entered if r.rr_realized <= 0) / max(losses, 1)
        ev = (wr / 100 * avg_win) + ((100 - wr) / 100 * avg_loss)

        # 에퀴티
        risk_amt = 50000
        equity = [0.0]
        for r in entered:
            equity.append(equity[-1] + r.rr_realized * risk_amt)
        dd = min(e - max(equity[:i+1]) for i, e in enumerate(equity))

        streak = 0
        max_streak = 0
        for r in entered:
            if r.rr_realized <= 0:
                streak += 1
                max_streak = max(max_streak, streak)
            else:
                streak = 0

        # 종목별
        by_stock = {}
        for r in entered:
            if r.code not in by_stock:
                by_stock[r.code] = {"wins": 0, "losses": 0, "rr": []}
            by_stock[r.code]["rr"].append(r.rr_realized)
            if r.rr_realized > 0:
                by_stock[r.code]["wins"] += 1
            else:
                by_stock[r.code]["losses"] += 1

        print(f"\n{'='*70}")
        print(f"  [{label}]")
        print(f"{'='*70}")
        print(f"  시도: {total_attempts}회 | 진입: {total}회 ({total/total_attempts*100:.0f}%) | 미진입: {not_entered}")
        print(f"  승률    : {wr:.1f}% ({wins}승/{losses}패)")
        print(f"  평균 RR : {avg_rr:+.2f}R")
        print(f"  누적 RR : {total_rr:+.1f}R")
        print(f"  기대값  : {ev:+.3f}R / 거래")
        print(f"  평균 승 : {avg_win:+.2f}R | 평균 패: {avg_loss:+.2f}R")
        print(f"  최대연패: {max_streak}연패")
        print(f"  누적PnL : {equity[-1]:+,.0f}원 (5만원/리스크)")
        print(f"  최대낙폭: {dd:+,.0f}원")
        print(f"  {'-'*68}")
        print(f"  청산사유:")
        for reason, cnt in sorted(reasons.items(), key=lambda x: -x[1]):
            pct = cnt / total * 100
            bar = '#' * int(pct / 2)
            print(f"    {reason:<14} {cnt:3d}회 ({pct:5.1f}%) {bar}")

        print(f"  {'-'*68}")
        print(f"  종목별:")
        for c, s in sorted(by_stock.items(), key=lambda x: -np.mean(x[1]["rr"])):
            n = STOCKS.get(c, c)
            total_s = s["wins"] + s["losses"]
            wr_s = s["wins"] / total_s * 100
            avg_s = np.mean(s["rr"])
            print(f"    {n:>10}({c}): {total_s:2d}회 WR:{wr_s:5.1f}% avgRR:{avg_s:+.2f}R")

        return {
            "total": total, "wins": wins, "wr": wr, "avg_rr": avg_rr,
            "total_rr": total_rr, "ev": ev, "max_streak": max_streak,
            "pnl": equity[-1], "dd": dd, "entry_rate": total / total_attempts * 100,
        }

    s23 = stats(results_v23, "v2.3 (리테스트, SL=range*0.6, TP=2R)")
    s3 = stats(results_v3, "v3.1 (FVG+Engulfing, FCR근처, SL=FVG바깥, TP=3R)")

    if s23 and s3:
        print(f"\n{'='*70}")
        print(f"  HEAD-TO-HEAD (실제 5분봉)")
        print(f"{'='*70}")
        print(f"  {'항목':<16} {'v2.3':>14} {'v3':>14} {'차이':>14}")
        print(f"  {'-'*58}")

        def fmt(v, f=".1f"):
            return f"{v:{f}}" if isinstance(v, float) else str(v)

        rows = [
            ("진입률", f"{s23['entry_rate']:.0f}%", f"{s3['entry_rate']:.0f}%",
             f"{s3['entry_rate']-s23['entry_rate']:+.0f}%p"),
            ("진입횟수", f"{s23['total']}회", f"{s3['total']}회",
             f"{s3['total']-s23['total']:+d}회"),
            ("승률", f"{s23['wr']:.1f}%", f"{s3['wr']:.1f}%",
             f"{s3['wr']-s23['wr']:+.1f}%p"),
            ("평균 RR", f"{s23['avg_rr']:+.2f}R", f"{s3['avg_rr']:+.2f}R",
             f"{s3['avg_rr']-s23['avg_rr']:+.2f}R"),
            ("기대값", f"{s23['ev']:+.3f}R", f"{s3['ev']:+.3f}R",
             f"{s3['ev']-s23['ev']:+.3f}R"),
            ("누적 RR", f"{s23['total_rr']:+.1f}R", f"{s3['total_rr']:+.1f}R",
             f"{s3['total_rr']-s23['total_rr']:+.1f}R"),
            ("누적 PnL", f"{s23['pnl']:+,.0f}", f"{s3['pnl']:+,.0f}",
             f"{s3['pnl']-s23['pnl']:+,.0f}"),
            ("최대낙폭", f"{s23['dd']:+,.0f}", f"{s3['dd']:+,.0f}", ""),
            ("최대연패", f"{s23['max_streak']}연패", f"{s3['max_streak']}연패", ""),
        ]

        for label, v23, v3, diff in rows:
            print(f"  {label:<16} {v23:>14} {v3:>14} {diff:>14}")

        print(f"\n  {'─'*58}")
        if s3["ev"] > s23["ev"]:
            diff_ev = s3["ev"] - s23["ev"]
            print(f"  >>> v3 기대값 우위 (+{diff_ev:.3f}R/거래) <<<")
        elif s3["ev"] < s23["ev"]:
            diff_ev = s23["ev"] - s3["ev"]
            print(f"  >>> v2.3 기대값 우위 (+{diff_ev:.3f}R/거래) <<<")
        else:
            print(f"  >>> 동등 <<<")

        # 손익분기 분석
        print(f"\n  손익분기 분석:")
        print(f"    v2.3: 2:1 RR → 손익분기 승률 33.3% | 실제 {s23['wr']:.1f}%", end="")
        print(f" → {'PASS' if s23['wr'] > 33.3 else 'FAIL'}")
        print(f"    v3.1: 3:1 RR → 손익분기 승률 25.0% | 실제 {s3['wr']:.1f}%", end="")
        print(f" → {'PASS' if s3['wr'] > 25.0 else 'FAIL'}")

        print(f"{'='*70}\n")

    # CSV 저장
    all_results = results_v23 + results_v3
    df = pd.DataFrame([asdict(r) for r in all_results])
    out = Path("results/real_5min_v2_vs_v3.csv")
    out.parent.mkdir(exist_ok=True)
    df.to_csv(out, index=False, encoding="utf-8-sig")
    print(f"결과 저장: {out}")


if __name__ == "__main__":
    main()
