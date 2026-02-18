# -*- coding: utf-8 -*-
"""
Body Hunter v2.3 vs v3 비교 백테스트
====================================
동일한 종목, 동일한 날짜, 동일한 합성 5분봉에서
v2.3 (리테스트 진입, 2:1 RR) vs v3 (FVG+Engulfing, 3:1 RR) 성과 비교

데이터: pykrx 일봉 (data_store/daily/) + 5D 수급 스캔으로 종목 선정
기간: 25년 3월 ~ 현재
"""

import sys, os
os.environ["PYTHONIOENCODING"] = "utf-8"
sys.path.insert(0, "D:/Prophet_Agent_System_예언자/scalper-agent")
os.chdir("D:/Prophet_Agent_System_예언자/scalper-agent")

import logging
from pathlib import Path
from dataclasses import dataclass, asdict
from typing import List, Dict, Optional, Tuple
import pandas as pd
import numpy as np

from strategies.body_hunter_v2 import BodyHunterV2, BodyState as StateV2
from strategies.body_hunter_v3 import BodyHunterV3, BodyState as StateV3

logging.basicConfig(level=logging.WARNING, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

DAILY_DIR = Path("data_store/daily")


@dataclass
class TradeResult:
    date: str
    code: str
    name: str
    version: str          # "v2.3" or "v3"
    direction: str
    entry_price: float
    exit_price: float
    exit_reason: str
    rr_realized: float
    hold_bars: int
    pnl_pct: float
    entered: bool         # 진입 성공 여부


def load_daily(code: str) -> Optional[pd.DataFrame]:
    """pykrx 일봉 CSV 로드 → 영문 컬럼"""
    path = DAILY_DIR / f"{code}.csv"
    if not path.exists():
        return None
    df = pd.read_csv(path, index_col=0, parse_dates=True)
    col_map = {"시가": "open", "고가": "high", "저가": "low",
               "종가": "close", "거래량": "volume", "등락률": "change_pct"}
    df.rename(columns=col_map, inplace=True)
    if "close" not in df.columns:
        return None
    return df


def daily_to_5min(row: dict, direction: str, seed: int, n_bars: int = 72) -> List[pd.Series]:
    """일봉 OHLCV → 72개 합성 5분봉 (실제 일봉 OHLCV 기반, 방향 무관)

    핵심: 당일 실제 OHLCV를 따라가되, 중간에 자연스러운 변동을 넣음.
    상승일이면 TP 맞을 수 있고, 하락일이면 SL 맞을 수 있음.
    """
    rng = np.random.default_rng(seed)

    o_day = row["open"]
    h_day = row["high"]
    l_day = row["low"]
    c_day = row["close"]
    v_day = row["volume"]
    day_range = h_day - l_day
    if day_range <= 0:
        day_range = o_day * 0.01

    candles = []
    price = o_day
    v_per_bar = v_day / n_bars

    # 장중 가격 경로 생성: 고가/저가 도달 시점을 랜덤하게 설정
    is_bullish = c_day >= o_day

    # 고가 도달 시점, 저가 도달 시점 설정
    if is_bullish:
        # 상승일: 초반 약간 빠지다가 → 상승 → 고가 → 마감
        low_bar = int(rng.integers(5, 20))      # 저가는 초반에
        high_bar = int(rng.integers(35, 60))     # 고가는 중후반에
    else:
        # 하락일: 초반 올랐다가 → 하락 → 저가 → 마감
        high_bar = int(rng.integers(5, 25))      # 고가는 초반에
        low_bar = int(rng.integers(35, 60))       # 저가는 중후반에

    # 각 봉의 목표 가격 경로 계산
    targets = []
    for i in range(n_bars):
        if i <= min(low_bar, high_bar):
            # 첫번째 극점으로 이동
            first_target = l_day if low_bar < high_bar else h_day
            progress = i / max(1, min(low_bar, high_bar))
            t = o_day + (first_target - o_day) * progress
        elif i <= max(low_bar, high_bar):
            # 두번째 극점으로 이동
            first_target = l_day if low_bar < high_bar else h_day
            second_target = h_day if low_bar < high_bar else l_day
            span = max(low_bar, high_bar) - min(low_bar, high_bar)
            progress = (i - min(low_bar, high_bar)) / max(1, span)
            t = first_target + (second_target - first_target) * progress
        else:
            # 마감으로 이동
            second_target = h_day if low_bar < high_bar else l_day
            remain = n_bars - max(low_bar, high_bar)
            progress = (i - max(low_bar, high_bar)) / max(1, remain)
            t = second_target + (c_day - second_target) * progress
        targets.append(t)

    for i in range(n_bars):
        target = targets[i]
        # 노이즈: 일봉 레인지의 1~3%
        noise = rng.normal(0, day_range * 0.015)
        bar_c = target + noise

        bar_o = price

        # 봉 내 변동
        bar_var = day_range * rng.uniform(0.005, 0.025)
        bar_h = max(bar_o, bar_c) + abs(rng.normal(0, bar_var))
        bar_l = min(bar_o, bar_c) - abs(rng.normal(0, bar_var))

        # 일봉 범위 제한 (약간의 여유)
        margin = day_range * 0.02
        bar_h = min(bar_h, h_day + margin)
        bar_l = max(bar_l, l_day - margin)
        bar_c = max(min(bar_c, h_day + margin), l_day - margin)

        # 거래량 패턴: 시초/장중/마감
        if i < 6:
            bar_v = v_per_bar * rng.uniform(1.5, 3.0)    # 시초 거래 폭증
        elif i > n_bars - 6:
            bar_v = v_per_bar * rng.uniform(1.0, 2.0)    # 마감 증가
        elif abs(i - low_bar) < 3 or abs(i - high_bar) < 3:
            bar_v = v_per_bar * rng.uniform(1.5, 2.5)    # 극점 근처
        else:
            bar_v = v_per_bar * rng.uniform(0.4, 1.2)

        candles.append(pd.Series({
            "open": bar_o, "high": bar_h, "low": bar_l,
            "close": bar_c, "volume": bar_v,
        }))
        price = bar_c

    return candles


def run_single_v23(candles: List[pd.Series], ticker: str, name: str,
                   direction: str, date: str) -> Optional[TradeResult]:
    """v2.3 엔진으로 단일 종목/일 시뮬"""
    hunter = BodyHunterV2(
        ticker=ticker, name=name, direction=direction,
        retest_required=True, volume_surge_min=1.5,
        trailing_atr_mult=1.2, breakeven_rr=0.3,
        sl_ratio=0.6, fixed_tp_rr=2.0,
        exhaustion_bars=2,
    )
    hunter.set_levels(candles[0], avg_volume=candles[0]["volume"])

    for i, candle in enumerate(candles[1:], 1):
        ts = pd.Timestamp(f"{date} {9+i//12:02d}:{(i%12)*5:02d}", tz="Asia/Seoul")
        candle.name = ts
        result = hunter.update(candle)

        if result["action"] == "EXIT":
            pos = result.get("position")
            if not pos:
                break
            entry = pos.entry_price
            exit_p = result.get("exit_price", candle["close"])
            pnl = (exit_p - entry) / entry * 100 if direction == "LONG" \
                else (entry - exit_p) / entry * 100
            return TradeResult(
                date=date, code=ticker, name=name, version="v2.3",
                direction=direction, entry_price=entry, exit_price=exit_p,
                exit_reason=result.get("reason", ""),
                rr_realized=result.get("rr_realized", 0),
                hold_bars=result.get("hold_bars", 0),
                pnl_pct=pnl, entered=True,
            )

    # 진입 안 했거나 청산 못 함
    entered = hunter.state in (StateV2.IN_BODY,)
    return TradeResult(
        date=date, code=ticker, name=name, version="v2.3",
        direction=direction, entry_price=0, exit_price=0,
        exit_reason="미진입" if not entered else "미청산",
        rr_realized=0, hold_bars=0, pnl_pct=0, entered=False,
    )


def run_single_v3(candles: List[pd.Series], ticker: str, name: str,
                  direction: str, date: str) -> Optional[TradeResult]:
    """v3 엔진으로 단일 종목/일 시뮬"""
    hunter = BodyHunterV3(
        ticker=ticker, name=name, direction=direction,
        volume_surge_min=1.5,
        trailing_atr_mult=1.2, breakeven_rr=0.3,
        fixed_tp_rr=3.0,           # 3:1 RR
        fvg_timeout_bars=15,
        fvg_min_size_pct=0.001,
        engulf_timeout_bars=10,
        exhaustion_bars=2,
    )
    hunter.set_levels(candles[0], avg_volume=candles[0]["volume"])

    for i, candle in enumerate(candles[1:], 1):
        ts = pd.Timestamp(f"{date} {9+i//12:02d}:{(i%12)*5:02d}", tz="Asia/Seoul")
        candle.name = ts
        result = hunter.update(candle)

        if result["action"] == "EXIT":
            pos = result.get("position")
            if not pos:
                break
            entry = pos.entry_price
            exit_p = result.get("exit_price", candle["close"])
            pnl = (exit_p - entry) / entry * 100 if direction == "LONG" \
                else (entry - exit_p) / entry * 100
            return TradeResult(
                date=date, code=ticker, name=name, version="v3",
                direction=direction, entry_price=entry, exit_price=exit_p,
                exit_reason=result.get("reason", ""),
                rr_realized=result.get("rr_realized", 0),
                hold_bars=result.get("hold_bars", 0),
                pnl_pct=pnl, entered=True,
            )

    entered = hunter.state in (StateV3.IN_BODY,)
    return TradeResult(
        date=date, code=ticker, name=name, version="v3",
        direction=direction, entry_price=0, exit_price=0,
        exit_reason="미진입" if not entered else "미청산",
        rr_realized=0, hold_bars=0, pnl_pct=0, entered=False,
    )


def select_candidates(date_str: str, universe: Dict[str, str],
                      daily_data: Dict[str, pd.DataFrame],
                      top_n: int = 10) -> List[Tuple[str, str]]:
    """전일 기준 모멘텀 종목 선정 (look-ahead bias 제거)

    전일 양봉 + 거래량 증가 종목을 후보로 선정.
    당일은 오를 수도 내릴 수도 있음 → 현실적 승패 발생.
    """
    scores = []
    target = pd.Timestamp(date_str)

    for code, name in universe.items():
        df = daily_data.get(code)
        if df is None:
            continue

        # 당일 포함해서 데이터 있는지 확인
        mask = df.index <= target
        sub = df[mask]
        if len(sub) < 21:
            continue

        today = sub.iloc[-1]
        if today.name.strftime("%Y-%m-%d") != date_str:
            continue

        # 전일 데이터로 후보 판단 (look-ahead bias 제거)
        yesterday = sub.iloc[-2]
        prev_data = sub.iloc[:-1]  # 전일까지

        # 전일 양봉 + 거래량 증가 = 모멘텀 종목
        prev_change = (yesterday["close"] - yesterday["open"]) / yesterday["open"] * 100
        avg_vol = prev_data["volume"].tail(20).mean()
        vol_ratio = yesterday["volume"] / avg_vol if avg_vol > 0 else 0

        # 전일 5일 수익률 (추세 확인)
        if len(prev_data) >= 5:
            momentum_5d = (yesterday["close"] - prev_data.iloc[-5]["close"]) / prev_data.iloc[-5]["close"] * 100
        else:
            momentum_5d = prev_change

        if prev_change > 0.3 and vol_ratio > 1.0 and momentum_5d > 0:
            scores.append((code, name, prev_change * vol_ratio + momentum_5d))

    scores.sort(key=lambda x: -x[2])
    return [(c, n) for c, n, _ in scores[:top_n]]


def run_comparison(start_date: str = "2025-03-01", end_date: str = "2026-02-19",
                   top_n: int = 5):
    """v2.3 vs v3 비교 백테스트 실행"""
    print(f"\n{'='*70}")
    print(f"  Body Hunter v2.3 vs v3 비교 백테스트")
    print(f"  기간: {start_date} ~ {end_date}")
    print(f"  종목: 일일 상위 {top_n}개 | 방향: LONG ONLY")
    print(f"{'='*70}\n")

    # 유니버스 로드
    from data.universe_builder import load_universe
    uni_raw = load_universe()
    universe = {}
    for code, val in uni_raw.items():
        if isinstance(val, (list, tuple)):
            universe[code] = val[0]
        else:
            universe[code] = val

    # ETF 제외
    exclude = {'069500', '371160', '102780', '305720'}
    universe = {k: v for k, v in universe.items() if k not in exclude}

    # 일봉 데이터 로드
    print(f"일봉 데이터 로드 중...")
    daily_data = {}
    for code in universe:
        df = load_daily(code)
        if df is not None and len(df) > 20:
            daily_data[code] = df
    print(f"  로드 완료: {len(daily_data)}종목\n")

    # 거래일 목록 (삼성전자 기준)
    ref = daily_data.get("005930")
    if ref is None:
        print("삼성전자 데이터 없음!")
        return

    start_ts = pd.Timestamp(start_date)
    end_ts = pd.Timestamp(end_date)
    dates = [d.strftime("%Y-%m-%d") for d in ref.index
             if start_ts <= d <= end_ts]
    print(f"거래일: {len(dates)}일  ({dates[0]} ~ {dates[-1]})\n")

    # 결과 수집
    results_v23: List[TradeResult] = []
    results_v3: List[TradeResult] = []

    for i, date in enumerate(dates):
        candidates = select_candidates(date, universe, daily_data, top_n=top_n)
        if not candidates:
            continue

        for code, name in candidates:
            df = daily_data[code]
            row_mask = df.index == pd.Timestamp(date)
            if not row_mask.any():
                continue
            row = df[row_mask].iloc[0]

            row_dict = {
                "open": row["open"], "high": row["high"],
                "low": row["low"], "close": row["close"],
                "volume": row["volume"],
            }

            # 같은 시드 → 같은 합성 봉
            seed = hash(f"{date}_{code}") % (2**32)
            candles = daily_to_5min(row_dict, "LONG", seed)

            # v2.3
            r23 = run_single_v23(candles, code, name, "LONG", date)
            results_v23.append(r23)

            # v3 (동일한 봉)
            r3 = run_single_v3(candles, code, name, "LONG", date)
            results_v3.append(r3)

        if (i + 1) % 20 == 0:
            e23 = [r for r in results_v23 if r.entered]
            e3 = [r for r in results_v3 if r.entered]
            print(f"  {i+1}/{len(dates)}일 | v2.3 진입:{len(e23)} | v3 진입:{len(e3)}")

    # 결과 출력
    print_results(results_v23, results_v3)


def print_results(results_v23: List[TradeResult], results_v3: List[TradeResult]):
    """비교 결과 출력"""
    def stats(results, label):
        entered = [r for r in results if r.entered]
        if not entered:
            print(f"\n  [{label}] 진입 거래 없음")
            return {}

        total = len(entered)
        wins = sum(1 for r in entered if r.rr_realized > 0)
        losses = total - wins
        wr = wins / total * 100
        avg_rr = sum(r.rr_realized for r in entered) / total
        total_rr = sum(r.rr_realized for r in entered)

        # 청산 사유
        reasons = {}
        for r in entered:
            reasons[r.exit_reason] = reasons.get(r.exit_reason, 0) + 1

        # 기대값 (EV)
        avg_win = sum(r.rr_realized for r in entered if r.rr_realized > 0) / max(wins, 1)
        avg_loss = sum(r.rr_realized for r in entered if r.rr_realized <= 0) / max(losses, 1)
        ev = (wr / 100 * avg_win) + ((100 - wr) / 100 * avg_loss)

        # 최대 연패
        streak = 0
        max_streak = 0
        for r in entered:
            if r.rr_realized <= 0:
                streak += 1
                max_streak = max(max_streak, streak)
            else:
                streak = 0

        # 에퀴티 커브
        equity = [0.0]
        for r in entered:
            equity.append(equity[-1] + r.rr_realized * 50000)
        peak = max(equity)
        dd = min(e - max(equity[:i+1]) for i, e in enumerate(equity))

        print(f"\n{'='*70}")
        print(f"  [{label}] 결과")
        print(f"{'='*70}")
        print(f"  전체 시도: {len(results)}회 | 진입: {total}회 ({total/len(results)*100:.0f}%)")
        print(f"  승률     : {wr:.1f}% ({wins}승/{losses}패)")
        print(f"  평균 RR  : {avg_rr:+.2f}R")
        print(f"  누적 RR  : {total_rr:+.1f}R")
        print(f"  기대값   : {ev:+.3f}R / 거래")
        print(f"  평균 승  : {avg_win:+.2f}R | 평균 패: {avg_loss:+.2f}R")
        print(f"  최대 연패: {max_streak}연패")
        print(f"  누적 PnL : {equity[-1]:+,.0f}원 (5만원/리스크)")
        print(f"  최대 낙폭: {dd:+,.0f}원")
        print(f"  {'-'*68}")
        print(f"  청산사유:")
        for reason, cnt in sorted(reasons.items(), key=lambda x: -x[1]):
            pct = cnt / total * 100
            bar = '#' * int(pct / 3)
            print(f"    {reason:<14} {cnt:4d}회 ({pct:5.1f}%) {bar}")

        return {
            "total": total, "wins": wins, "wr": wr, "avg_rr": avg_rr,
            "total_rr": total_rr, "ev": ev, "max_streak": max_streak,
            "pnl": equity[-1], "dd": dd,
        }

    s23 = stats(results_v23, "v2.3 (리테스트, 2:1 RR)")
    s3 = stats(results_v3, "v3 (FVG+Engulfing, 3:1 RR)")

    if s23 and s3:
        print(f"\n{'='*70}")
        print(f"  HEAD-TO-HEAD 비교")
        print(f"{'='*70}")
        print(f"  {'항목':<16} {'v2.3':>14} {'v3':>14} {'차이':>14}")
        print(f"  {'-'*58}")

        comparisons = [
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
            ("최대 낙폭", f"{s23['dd']:+,.0f}", f"{s3['dd']:+,.0f}", ""),
            ("최대 연패", f"{s23['max_streak']}연패", f"{s3['max_streak']}연패", ""),
        ]

        for label, v23_val, v3_val, diff in comparisons:
            print(f"  {label:<16} {v23_val:>14} {v3_val:>14} {diff:>14}")

        # 승자 판정
        print(f"\n  {'─'*58}")
        if s3["ev"] > s23["ev"]:
            print(f"  >>> v3 (FVG+Engulfing) 기대값 우위 <<<")
        elif s3["ev"] < s23["ev"]:
            print(f"  >>> v2.3 (리테스트) 기대값 우위 <<<")
        else:
            print(f"  >>> 동등 <<<")

        print(f"{'='*70}\n")

    # CSV 저장
    all_results = results_v23 + results_v3
    df = pd.DataFrame([asdict(r) for r in all_results])
    out_path = Path("results/v2_vs_v3_comparison.csv")
    out_path.parent.mkdir(exist_ok=True)
    df.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"결과 저장: {out_path}")


if __name__ == "__main__":
    run_comparison(
        start_date="2025-03-01",
        end_date="2026-02-19",
        top_n=5,
    )
