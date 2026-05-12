# -*- coding: utf-8 -*-
"""
상한가 전략 백테스트
====================
상한가 감지 후 두 가지 진입 전략을 비교:

  전략 A: 다음날 시초가 진입 (현재 설계)
  전략 B: 눌림목 대기 진입 (-5% 이상 눌린 날 진입)

각 전략에 대해:
  - 승률 (TP 도달 비율)
  - 평균 수익률
  - 최대 낙폭 (MDD)
  - R:R (Risk/Reward)

Usage:
  python tools/limit_up_backtest.py [--days 90] [--score-min 60]
"""

import json
import sys
import logging
from pathlib import Path
from dataclasses import dataclass, field

import numpy as np
import pandas as pd

BASE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BASE))

from data.limit_up_scanner import (
    _load_daily, _is_etf, LIMIT_UP_PCT, NEAR_LIMIT_PCT,
    analyze_short_proxy, score_continuation, LimitUpStock,
)

UNIVERSE_PATH = BASE / "data_store" / "universe.json"
logger = logging.getLogger("BH.LimitUpBacktest")


@dataclass
class TradeResult:
    """개별 트레이드 결과"""
    code: str
    name: str
    limit_date: str          # 상한가 발생일
    entry_date: str          # 실제 진입일
    entry_price: float       # 진입가
    exit_price: float        # 청산가
    exit_date: str           # 청산일
    exit_reason: str         # TP1/TP2/SL/TIMEOUT
    pnl_pct: float           # 수익률 %
    hold_days: int           # 보유일수
    strategy: str            # A_next_day / B_pullback
    continuation_score: float = 0.0
    max_gain_during: float = 0.0   # 보유 중 최대 수익
    max_loss_during: float = 0.0   # 보유 중 최대 손실


def _backtest_single(
    code: str,
    name: str,
    df: pd.DataFrame,
    limit_idx: int,
    cont_score: float,
    strategy: str = "A",
    tp1_pct: float = 5.0,
    tp2_pct: float = 10.0,
    sl_pct: float = -15.0,
    pullback_trigger: float = -5.0,   # B전략: 몇% 눌리면 진입
    pullback_wait: int = 5,           # B전략: 최대 며칠 기다릴지
    max_hold: int = 15,               # 최대 보유일
) -> TradeResult | None:
    """단일 상한가 이벤트에 대한 백테스트"""

    close_col = "종가" if "종가" in df.columns else "close"
    open_col = "시가" if "시가" in df.columns else "open"
    high_col = "고가" if "고가" in df.columns else "high"
    low_col = "저가" if "저가" in df.columns else "low"

    closes = df[close_col].values.astype(float)
    opens = df[open_col].values.astype(float)
    highs = df[high_col].values.astype(float)
    lows = df[low_col].values.astype(float)
    dates = df.index

    limit_close = closes[limit_idx]

    # ── 진입 결정 ──
    if strategy == "A":
        # A: 다음날 시초가 진입
        entry_idx = limit_idx + 1
        if entry_idx >= len(closes):
            return None
        entry_price = opens[entry_idx]
        entry_date = str(dates[entry_idx].date())

    elif strategy == "B":
        # B: 눌림목 대기 — 상한가 후 pullback_wait일 이내에
        # 종가 기준 pullback_trigger% 이상 눌린 날의 종가에 진입
        entry_idx = None
        for d in range(1, pullback_wait + 1):
            idx = limit_idx + d
            if idx >= len(closes):
                break
            drop_from_limit = (closes[idx] / limit_close - 1) * 100
            if drop_from_limit <= pullback_trigger:
                entry_idx = idx
                break

        if entry_idx is None:
            # 눌림 없이 계속 올라감 — 진입 못함
            return None
        entry_price = closes[entry_idx]
        entry_date = str(dates[entry_idx].date())
    else:
        return None

    if entry_price <= 0:
        return None

    # ── 보유 + 청산 시뮬레이션 ──
    max_gain = 0.0
    max_loss = 0.0
    exit_price = 0.0
    exit_date = ""
    exit_reason = "TIMEOUT"
    hold_days = 0

    for d in range(1, max_hold + 1):
        hold_idx = entry_idx + d
        if hold_idx >= len(closes):
            # 데이터 끝 — 마지막 종가로 청산
            exit_price = closes[-1]
            exit_date = str(dates[-1].date())
            hold_days = len(closes) - 1 - entry_idx
            exit_reason = "DATA_END"
            break

        # 장중 고가/저가로 TP/SL 체크
        intraday_high = (highs[hold_idx] / entry_price - 1) * 100
        intraday_low = (lows[hold_idx] / entry_price - 1) * 100
        close_pnl = (closes[hold_idx] / entry_price - 1) * 100

        if intraday_high > max_gain:
            max_gain = intraday_high
        if intraday_low < max_loss:
            max_loss = intraday_low

        # SL 먼저 체크 (장중 저가가 SL 터치)
        if intraday_low <= sl_pct:
            exit_price = entry_price * (1 + sl_pct / 100)
            exit_date = str(dates[hold_idx].date())
            hold_days = d
            exit_reason = "SL"
            break

        # TP2 체크 (장중 고가가 TP2 터치)
        if intraday_high >= tp2_pct:
            exit_price = entry_price * (1 + tp2_pct / 100)
            exit_date = str(dates[hold_idx].date())
            hold_days = d
            exit_reason = "TP2"
            break

        # TP1 체크 — 절반 익절이지만 단순화: TP1 도달 시 종가 청산
        if intraday_high >= tp1_pct and d >= 2:
            # TP1 도달했으면 종가에서 청산 (트레일링 스탑 간소화)
            exit_price = closes[hold_idx]
            exit_date = str(dates[hold_idx].date())
            hold_days = d
            exit_reason = "TP1"
            break

    else:
        # max_hold 도달 — 타임아웃 종가 청산
        last_idx = min(entry_idx + max_hold, len(closes) - 1)
        exit_price = closes[last_idx]
        exit_date = str(dates[last_idx].date())
        hold_days = last_idx - entry_idx
        exit_reason = "TIMEOUT"

    pnl_pct = (exit_price / entry_price - 1) * 100 if entry_price > 0 else 0

    return TradeResult(
        code=code,
        name=name,
        limit_date=str(dates[limit_idx].date()),
        entry_date=entry_date,
        entry_price=round(entry_price),
        exit_price=round(exit_price),
        exit_date=exit_date,
        exit_reason=exit_reason,
        pnl_pct=round(pnl_pct, 2),
        hold_days=hold_days,
        strategy=f"{'A_next_day' if strategy == 'A' else 'B_pullback'}",
        continuation_score=cont_score,
        max_gain_during=round(max_gain, 2),
        max_loss_during=round(max_loss, 2),
    )


def run_backtest(
    days: int = 90,
    score_min: float = 0,
    tp1: float = 5.0,
    tp2: float = 10.0,
    sl: float = -15.0,
    pullback_trigger: float = -5.0,
    pullback_wait: int = 5,
    max_hold: int = 15,
) -> dict:
    """전체 유니버스 백테스트 실행"""

    if not UNIVERSE_PATH.exists():
        print("ERROR: universe.json 없음")
        return {}

    with open(UNIVERSE_PATH, "r", encoding="utf-8") as f:
        universe = json.load(f)

    results_a: list[TradeResult] = []
    results_b: list[TradeResult] = []
    skipped = 0

    for code, info in universe.items():
        name = info.get("name", "")
        if _is_etf(name):
            continue

        df = _load_daily(code)
        if df is None or len(df) < days + 20:
            continue

        close_col = "종가" if "종가" in df.columns else "close"
        closes = df[close_col].values.astype(float)

        # 최근 days일 내 상한가 찾기
        start_idx = max(1, len(closes) - days)
        # 마지막 15일은 결과 확인용으로 남겨야 함
        end_idx = len(closes) - max_hold - 1

        for i in range(start_idx, end_idx):
            if closes[i - 1] <= 0:
                continue
            chg = (closes[i] / closes[i - 1] - 1) * 100
            if chg < LIMIT_UP_PCT:
                continue

            # 연속성 점수 계산 (간이)
            vol_col = "거래량" if "거래량" in df.columns else "volume"
            high_col = "고가" if "고가" in df.columns else "high"
            low_col = "저가" if "저가" in df.columns else "low"
            volumes = df[vol_col].values.astype(float)
            highs_arr = df[high_col].values.astype(float)
            lows_arr = df[low_col].values.astype(float)

            vol_avg20 = volumes[max(0, i-20):i].mean() if i >= 20 else volumes[:i].mean()
            vol_ratio = volumes[i] / vol_avg20 if vol_avg20 > 0 else 1.0
            high_v = highs_arr[i]
            low_v = lows_arr[i]
            rng = high_v - low_v
            close_strength = (closes[i] - low_v) / rng if rng > 0 else 0.5
            cap = info.get("cap_억", 0)
            tv = closes[i] * volumes[i] / 1e8
            turnover = (tv / cap * 100) if cap > 0 else 0

            stock = LimitUpStock(
                code=code, name=name,
                sector=info.get("sector", ""),
                close=int(closes[i]),
                change_pct=round(chg, 2),
                volume=int(volumes[i]),
                volume_ratio=round(vol_ratio, 2),
                trading_value_억=round(tv, 1),
                market_cap_억=cap,
                turnover_pct=round(turnover, 2),
                close_strength=round(close_strength, 3),
                consecutive_limit=1,
            )
            cont_score = score_continuation(stock)

            if cont_score < score_min:
                skipped += 1
                continue

            # 전략 A: 다음날 시초가
            r_a = _backtest_single(
                code, name, df, i, cont_score,
                strategy="A", tp1_pct=tp1, tp2_pct=tp2, sl_pct=sl,
                max_hold=max_hold,
            )
            if r_a:
                results_a.append(r_a)

            # 전략 B: 눌림목 대기
            r_b = _backtest_single(
                code, name, df, i, cont_score,
                strategy="B", tp1_pct=tp1, tp2_pct=tp2, sl_pct=sl,
                pullback_trigger=pullback_trigger,
                pullback_wait=pullback_wait,
                max_hold=max_hold,
            )
            if r_b:
                results_b.append(r_b)

    return {
        "A_next_day": results_a,
        "B_pullback": results_b,
        "skipped": skipped,
        "params": {
            "days": days, "score_min": score_min,
            "tp1": tp1, "tp2": tp2, "sl": sl,
            "pullback_trigger": pullback_trigger,
            "pullback_wait": pullback_wait,
            "max_hold": max_hold,
        },
    }


def print_summary(results: dict):
    """백테스트 결과 요약 출력"""
    params = results["params"]
    print(f"\n{'=' * 80}")
    print(f"  상한가 전략 백테스트 결과")
    print(f"  기간: 최근 {params['days']}일 | 점수 컷오프: {params['score_min']}+")
    print(f"  TP1: +{params['tp1']}% | TP2: +{params['tp2']}% | SL: {params['sl']}%")
    print(f"  눌림목 진입: {params['pullback_trigger']}% (최대 {params['pullback_wait']}일 대기)")
    print(f"  최대 보유: {params['max_hold']}일")
    print(f"{'=' * 80}")

    for label, key in [("A: 다음날 시초가 진입", "A_next_day"),
                        ("B: 눌림목 대기 진입", "B_pullback")]:
        trades = results[key]
        if not trades:
            print(f"\n  [{label}] 트레이드 없음")
            continue

        pnls = [t.pnl_pct for t in trades]
        wins = [t for t in trades if t.pnl_pct > 0]
        losses = [t for t in trades if t.pnl_pct <= 0]

        win_rate = len(wins) / len(trades) * 100
        avg_pnl = np.mean(pnls)
        median_pnl = np.median(pnls)
        avg_win = np.mean([t.pnl_pct for t in wins]) if wins else 0
        avg_loss = np.mean([t.pnl_pct for t in losses]) if losses else 0
        avg_hold = np.mean([t.hold_days for t in trades])
        max_win = max(pnls)
        max_loss = min(pnls)

        # 청산 사유 분포
        exit_counts = {}
        for t in trades:
            exit_counts[t.exit_reason] = exit_counts.get(t.exit_reason, 0) + 1

        # 점수 구간별 승률
        score_bins = [(80, 100), (60, 80), (40, 60), (0, 40)]

        print(f"\n  [{label}]")
        print(f"  {'─' * 60}")
        print(f"  총 트레이드: {len(trades)}건 (스킵: {results['skipped']}건)")
        print(f"  승률: {win_rate:.1f}% ({len(wins)}승 / {len(losses)}패)")
        print(f"  평균 수익률: {avg_pnl:+.2f}% (중앙값: {median_pnl:+.2f}%)")
        print(f"  평균 승리: {avg_win:+.2f}% | 평균 패배: {avg_loss:+.2f}%")
        print(f"  최대 수익: {max_win:+.2f}% | 최대 손실: {max_loss:+.2f}%")
        print(f"  평균 보유: {avg_hold:.1f}일")
        print(f"  Profit Factor: {abs(sum(t.pnl_pct for t in wins)) / abs(sum(t.pnl_pct for t in losses)):.2f}" if losses else "  Profit Factor: INF")

        # R:R
        if avg_loss != 0:
            rr = abs(avg_win / avg_loss)
            print(f"  R:R (평균승리/평균패배): {rr:.2f}")

        # 기대값
        ev = (win_rate / 100 * avg_win) + ((100 - win_rate) / 100 * avg_loss)
        print(f"  기대값 (EV): {ev:+.2f}% / 건")

        # 청산 사유
        print(f"\n  청산 사유:")
        for reason, cnt in sorted(exit_counts.items(), key=lambda x: -x[1]):
            pct = cnt / len(trades) * 100
            avg_r = np.mean([t.pnl_pct for t in trades if t.exit_reason == reason])
            print(f"    {reason:10s}: {cnt:>4}건 ({pct:5.1f}%) 평균 {avg_r:+.2f}%")

        # 점수 구간별
        print(f"\n  점수 구간별 성과:")
        for lo, hi in score_bins:
            bin_trades = [t for t in trades if lo <= t.continuation_score < hi]
            if not bin_trades:
                continue
            bin_wr = sum(1 for t in bin_trades if t.pnl_pct > 0) / len(bin_trades) * 100
            bin_avg = np.mean([t.pnl_pct for t in bin_trades])
            print(f"    {lo:>3}~{hi:<3}: {len(bin_trades):>4}건 | 승률 {bin_wr:5.1f}% | 평균 {bin_avg:+.2f}%")

        # TOP 5 / WORST 5
        sorted_trades = sorted(trades, key=lambda t: -t.pnl_pct)
        print(f"\n  TOP 5:")
        for t in sorted_trades[:5]:
            print(f"    {t.name:12s} {t.limit_date} -> {t.entry_date} {t.pnl_pct:+6.2f}% ({t.exit_reason}) 점수:{t.continuation_score:.0f}")
        print(f"  WORST 5:")
        for t in sorted_trades[-5:]:
            print(f"    {t.name:12s} {t.limit_date} -> {t.entry_date} {t.pnl_pct:+6.2f}% ({t.exit_reason}) 점수:{t.continuation_score:.0f}")


def main():
    import argparse
    logging.basicConfig(level=logging.WARNING)

    parser = argparse.ArgumentParser(description="상한가 전략 백테스트")
    parser.add_argument("--days", type=int, default=90, help="백테스트 기간 (일)")
    parser.add_argument("--score-min", type=float, default=0, help="연속성 점수 컷오프")
    parser.add_argument("--tp1", type=float, default=5.0, help="TP1 (%)")
    parser.add_argument("--tp2", type=float, default=10.0, help="TP2 (%)")
    parser.add_argument("--sl", type=float, default=-15.0, help="SL (%)")
    parser.add_argument("--pullback", type=float, default=-5.0, help="눌림목 진입 트리거 (%)")
    parser.add_argument("--pullback-wait", type=int, default=5, help="눌림목 대기일")
    parser.add_argument("--max-hold", type=int, default=15, help="최대 보유일")
    args = parser.parse_args()

    results = run_backtest(
        days=args.days,
        score_min=args.score_min,
        tp1=args.tp1,
        tp2=args.tp2,
        sl=args.sl,
        pullback_trigger=args.pullback,
        pullback_wait=args.pullback_wait,
        max_hold=args.max_hold,
    )

    print_summary(results)

    # 추가: 점수 60+ 필터 결과도 출력
    if args.score_min == 0:
        print(f"\n\n{'=' * 80}")
        print(f"  [비교] 점수 60+ 필터 적용 시")
        print(f"{'=' * 80}")
        filtered = {
            "A_next_day": [t for t in results["A_next_day"] if t.continuation_score >= 60],
            "B_pullback": [t for t in results["B_pullback"] if t.continuation_score >= 60],
            "skipped": 0,
            "params": {**results["params"], "score_min": 60},
        }
        print_summary(filtered)


if __name__ == "__main__":
    main()
