"""
그룹 순환매 백테스트
- 일봉 데이터 기반
- ETF 추세 확인 → 후행주 매수 → 청산 조건 체크
- 결과 요약 (승률, 평균 수익, 최대 낙폭)

사용법:
  python -m backtest.rotation_backtest
"""

import sys
import os
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, List

import pandas as pd
import numpy as np
import yaml

# 프로젝트 루트
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from strategies.group_rotation import (
    GroupRotationStrategy,
    GroupTrend,
    RotationSignal,
)

logger = logging.getLogger(__name__)

DAILY_DIR = ROOT / "data_store" / "daily"


def load_config() -> dict:
    cfg_path = ROOT / "config.yaml"
    with open(cfg_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def load_daily_data(codes: List[str]) -> Dict[str, pd.DataFrame]:
    """로컬 CSV에서 일봉 로드"""
    data = {}
    for code in codes:
        csv_path = DAILY_DIR / f"{code}.csv"
        if csv_path.exists():
            df = pd.read_csv(csv_path, index_col=0, parse_dates=True)
            if len(df) > 0:
                data[code] = df
    return data


def run_backtest(
    strategy: GroupRotationStrategy,
    daily_data: Dict[str, pd.DataFrame],
    start_date: str = None,
    end_date: str = None,
) -> List[Dict]:
    """일봉 기반 워크-포워드 백테스트

    매일:
      1. 보유 포지션 청산 조건 체크
      2. 빈 슬롯 있으면 스캔 → 진입
    """
    # 공통 날짜 인덱스 구하기
    all_dates = set()
    for df in daily_data.values():
        all_dates.update(df.index)
    all_dates = sorted(all_dates)

    if start_date:
        all_dates = [d for d in all_dates if d >= pd.Timestamp(start_date)]
    if end_date:
        all_dates = [d for d in all_dates if d <= pd.Timestamp(end_date)]

    # 최소 lookback 보장
    min_bars = max(strategy.etf_ma_period, strategy.lookback_days) + 5
    if len(all_dates) < min_bars:
        print(f"데이터 부족: {len(all_dates)}일 < 필요 {min_bars}일")
        return []

    trade_log = []
    equity_curve = []
    total_pnl = 0.0

    print(f"\n{'='*60}")
    print(f"  그룹 순환매 백테스트")
    print(f"  기간: {all_dates[min_bars].date()} ~ {all_dates[-1].date()}")
    print(f"  그룹: {', '.join(g.name for g in strategy.groups.values())}")
    print(f"{'='*60}\n")

    for i in range(min_bars, len(all_dates)):
        current_date = all_dates[i]
        date_str = str(current_date.date())

        # 현재 날짜까지의 데이터 슬라이스
        sliced = {}
        for code, df in daily_data.items():
            mask = df.index <= current_date
            sub = df[mask]
            if len(sub) > 0:
                sliced[code] = sub

        # 1. 청산 체크
        exits = strategy.check_exits(sliced, date_str)
        for ex in exits:
            total_pnl += ex["pnl_amount"]
            ex["date"] = date_str
            trade_log.append(ex)
            print(
                f"  [{date_str}] 청산 {ex['name']}({ex['code']}) "
                f"{ex['reason']} PnL:{ex['pnl_pct']:+.1f}% "
                f"= {ex['pnl_amount']:+,.0f}원 ({ex['hold_days']}일)"
            )

        # 2. 스캔 → 진입
        signals = strategy.scan(sliced)
        for sig in signals:
            if sig.etf_trend != GroupTrend.BULLISH or not sig.picks:
                continue

            for pick in sig.picks:
                code = pick["code"]
                price = float(sliced[code]["close"].iloc[-1])

                pos = strategy.enter_position(
                    group_id=sig.group_id,
                    code=code,
                    name=pick["name"],
                    price=price,
                    date_str=date_str,
                )
                if pos:
                    print(
                        f"  [{date_str}] 진입 {pick['name']}({code}) "
                        f"@{price:,.0f} x{pos.shares}주 "
                        f"| {pick['reason']}"
                    )

        # 3. 일별 equity
        unrealized = sum(
            (float(sliced.get(c, pd.DataFrame({"close": [p.entry_price]}))["close"].iloc[-1]) - p.entry_price) * p.shares
            for c, p in strategy.positions.items()
        )
        equity_curve.append({
            "date": date_str,
            "realized_pnl": total_pnl,
            "unrealized_pnl": unrealized,
            "total_equity": total_pnl + unrealized,
            "positions": len(strategy.positions),
        })

    # 잔여 포지션 강제 청산
    if strategy.positions:
        print(f"\n  --- 백테스트 종료: 잔여 {len(strategy.positions)}종목 강제 청산 ---")
        for code, pos in list(strategy.positions.items()):
            df = daily_data.get(code)
            if df is not None:
                pos.current_price = float(df["close"].iloc[-1])
            pnl = pos.pnl_amount
            total_pnl += pnl
            trade_log.append({
                "code": code,
                "name": pos.name,
                "group_id": pos.group_id,
                "reason": "백테스트종료",
                "entry_price": pos.entry_price,
                "exit_price": pos.current_price,
                "shares": pos.shares,
                "pnl_pct": round(pos.pnl_pct, 2),
                "pnl_amount": round(pnl),
                "hold_days": pos.hold_days,
                "date": date_str,
            })
            print(
                f"    {pos.name}({code}) PnL: {pos.pnl_pct:+.1f}% "
                f"= {pnl:+,.0f}원 ({pos.hold_days}일)"
            )
        strategy.positions.clear()

    # 결과 출력
    print_results(trade_log, equity_curve, total_pnl)

    # CSV 저장
    if trade_log:
        save_dir = ROOT / "results"
        save_dir.mkdir(exist_ok=True)
        df_trades = pd.DataFrame(trade_log)
        df_trades.to_csv(save_dir / "group_rotation_results.csv", index=False, encoding="utf-8-sig")
        df_equity = pd.DataFrame(equity_curve)
        df_equity.to_csv(save_dir / "group_rotation_equity.csv", index=False, encoding="utf-8-sig")
        print(f"\n  결과 저장: {save_dir}")

    return trade_log


def print_results(trade_log: List[Dict], equity_curve: List[Dict], total_pnl: float):
    """결과 요약 출력"""
    print(f"\n{'='*60}")
    print(f"  백테스트 결과 요약")
    print(f"{'='*60}")

    if not trade_log:
        print("  거래 없음")
        return

    n = len(trade_log)
    wins = [t for t in trade_log if t["pnl_amount"] > 0]
    losses = [t for t in trade_log if t["pnl_amount"] < 0]
    evens = [t for t in trade_log if t["pnl_amount"] == 0]

    win_rate = len(wins) / n * 100 if n > 0 else 0
    avg_win = np.mean([t["pnl_pct"] for t in wins]) if wins else 0
    avg_loss = np.mean([t["pnl_pct"] for t in losses]) if losses else 0
    avg_hold = np.mean([t["hold_days"] for t in trade_log])
    total_win = sum(t["pnl_amount"] for t in wins)
    total_loss = sum(t["pnl_amount"] for t in losses)
    profit_factor = abs(total_win / total_loss) if total_loss != 0 else float("inf")

    # 최대 낙폭
    if equity_curve:
        equities = [e["total_equity"] for e in equity_curve]
        peak = equities[0]
        max_dd = 0
        for eq in equities:
            if eq > peak:
                peak = eq
            dd = eq - peak
            if dd < max_dd:
                max_dd = dd
    else:
        max_dd = 0

    print(f"  총 거래:     {n}건 ({len(wins)}W / {len(losses)}L / {len(evens)}E)")
    print(f"  승률:        {win_rate:.1f}%")
    print(f"  평균 수익:   {avg_win:+.2f}% (승) / {avg_loss:+.2f}% (패)")
    print(f"  평균 보유:   {avg_hold:.1f}일")
    print(f"  총 PnL:      {total_pnl:+,.0f}원")
    print(f"  총 수익:     {total_win:+,.0f}원 / 총 손실: {total_loss:+,.0f}원")
    print(f"  Profit Factor: {profit_factor:.2f}")
    print(f"  최대 낙폭:   {max_dd:+,.0f}원")

    # 그룹별 요약
    groups = {}
    for t in trade_log:
        gid = t.get("group_id", "unknown")
        if gid not in groups:
            groups[gid] = {"trades": 0, "wins": 0, "pnl": 0}
        groups[gid]["trades"] += 1
        if t["pnl_amount"] > 0:
            groups[gid]["wins"] += 1
        groups[gid]["pnl"] += t["pnl_amount"]

    print(f"\n  그룹별:")
    for gid, stats in groups.items():
        wr = stats["wins"] / stats["trades"] * 100 if stats["trades"] > 0 else 0
        print(
            f"    {gid}: {stats['trades']}건 "
            f"WR:{wr:.0f}% PnL:{stats['pnl']:+,.0f}원"
        )

    print(f"{'='*60}")


def main():
    logging.basicConfig(level=logging.WARNING)

    config = load_config()
    rot_config = config.get("strategies", {}).get("group_rotation", {})

    if not rot_config:
        print("config.yaml에 strategies.group_rotation 섹션이 없습니다")
        return

    strategy = GroupRotationStrategy(rot_config)

    # 필요한 종목코드 수집
    codes_needed = set()
    for group in strategy.groups.values():
        codes_needed.add(group.etf_code)
        for m in group.members:
            codes_needed.add(m.code)

    print(f"필요 종목: {len(codes_needed)}개")
    print(f"데이터 경로: {DAILY_DIR}")

    # 데이터 로드
    daily_data = load_daily_data(list(codes_needed))
    loaded = set(daily_data.keys())
    missing = codes_needed - loaded

    print(f"로드 완료: {len(daily_data)}종목")
    if missing:
        print(f"누락: {missing}")
        # 그룹 ETF가 없으면 KODEX200(069500) 프록시 사용
        proxy_code = "069500"
        proxy_df = load_daily_data([proxy_code]).get(proxy_code)
        if proxy_df is not None:
            etf_codes = {g.etf_code for g in strategy.groups.values()}
            for etf_code in etf_codes & missing:
                daily_data[etf_code] = proxy_df
                print(f"  → {etf_code} 없음 → KODEX200 프록시 사용")
                missing.discard(etf_code)

        if missing:
            print(f"여전히 누락: {missing}")
            print("→ 'python -m data.kis_collector' 로 데이터 수집 필요")

    if len(daily_data) < 3:
        print("데이터 부족. 먼저 일봉 데이터를 수집하세요.")
        return

    # 백테스트 실행
    run_backtest(strategy, daily_data)


if __name__ == "__main__":
    main()
