# -*- coding: utf-8 -*-
"""
MOMENTUM 레짐 백테스트 — 숫자 검증 리포트
==========================================
1,564종목 × 약 1년 flow/daily 데이터 기반
기관+외인 3일 연속 매수 시그널 vs MACD 0선 크로스 진입 비교
"""

import pandas as pd
import numpy as np
import json
import sys
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent.parent / "data_store"
DAILY_DIR = DATA_DIR / "daily"
FLOW_DIR = DATA_DIR / "flow"


def calc_rsi(close, period=14):
    delta = close.diff()
    gain = delta.clip(lower=0).rolling(period).mean()
    loss = (-delta).clip(lower=0).rolling(period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))


def calc_macd_cross(close):
    ema12 = close.ewm(span=12).mean()
    ema26 = close.ewm(span=26).mean()
    macd = ema12 - ema26
    cross = (macd.shift(1) < 0) & (macd >= 0)
    return cross


def run_backtest():
    universe = json.load(open(DATA_DIR / "universe.json", "r", encoding="utf-8"))

    results = []
    scan_count = 0

    flow_files = list(FLOW_DIR.glob("*_investor.csv"))

    for ff in flow_files:
        code = ff.stem.replace("_investor", "")
        daily_path = DAILY_DIR / f"{code}.csv"
        if not daily_path.exists():
            continue

        try:
            flow = pd.read_csv(ff, index_col=0, parse_dates=True).sort_index()
            daily = pd.read_csv(daily_path, index_col=0, parse_dates=True).sort_index()
        except Exception:
            continue

        if len(flow) < 30 or len(daily) < 40:
            continue

        scan_count += 1
        name = universe.get(code, {}).get("name", code)

        # 기관+외인 합산
        inst = flow["기관_금액"].fillna(0) if "기관_금액" in flow.columns else pd.Series(0, index=flow.index)
        frgn = flow["외국인_금액"].fillna(0) if "외국인_금액" in flow.columns else pd.Series(0, index=flow.index)
        flow["smart_net"] = inst + frgn

        # 연속 매수일 계산
        consec = []
        cnt = 0
        for val in flow["smart_net"]:
            if val > 0:
                cnt += 1
            else:
                cnt = 0
            consec.append(cnt)
        flow["consec_buy"] = consec

        # MOMENTUM 시그널: 연속 매수 정확히 3일째
        momentum_days = flow[flow["consec_buy"] == 3].index

        for sig_date in momentum_days:
            sig_pos = daily.index.get_indexer([sig_date], method="pad")[0]
            if sig_pos < 20 or sig_pos >= len(daily) - 6:
                continue

            sig_close = daily["종가"].iloc[sig_pos]

            # 거래량
            vol_20d = daily["거래량"].iloc[sig_pos - 20 : sig_pos].mean()
            vol_today = daily["거래량"].iloc[sig_pos]
            vol_ratio = vol_today / vol_20d if vol_20d > 0 else 1

            # RSI
            rsi_series = calc_rsi(daily["종가"].iloc[: sig_pos + 1])
            if len(rsi_series) == 0:
                continue
            rsi_val = rsi_series.iloc[-1]
            if pd.isna(rsi_val):
                continue

            # MACD 0선 크로스 (시그널 이후)
            end_idx = min(sig_pos + 15, len(daily))
            macd_cross = calc_macd_cross(daily["종가"].iloc[:end_idx])
            macd_cross_after = macd_cross.iloc[sig_pos:]
            macd_days_delay = -1
            for i, crossed in enumerate(macd_cross_after):
                if crossed:
                    macd_days_delay = i
                    break

            # 시그널 이후 수익률
            future_5d = daily["종가"].iloc[sig_pos + 1 : sig_pos + 6]
            if len(future_5d) < 3:
                continue

            ret_1d = (daily["종가"].iloc[sig_pos + 1] / sig_close - 1) * 100
            ret_3d = (daily["종가"].iloc[min(sig_pos + 3, len(daily) - 1)] / sig_close - 1) * 100
            ret_5d = (future_5d.iloc[-1] / sig_close - 1) * 100
            max_ret = (future_5d.max() / sig_close - 1) * 100
            min_ret = (future_5d.min() / sig_close - 1) * 100

            # MACD 크로스 시점 진입 수익률
            macd_ret_5d = None
            if 0 <= macd_days_delay and sig_pos + macd_days_delay + 5 < len(daily):
                macd_entry = daily["종가"].iloc[sig_pos + macd_days_delay]
                macd_future = daily["종가"].iloc[
                    sig_pos + macd_days_delay + 1 : sig_pos + macd_days_delay + 6
                ]
                if len(macd_future) >= 3:
                    macd_ret_5d = (macd_future.iloc[-1] / macd_entry - 1) * 100

            results.append(
                {
                    "code": code,
                    "name": name,
                    "date": str(sig_date.date()),
                    "close": int(sig_close),
                    "rsi": round(rsi_val, 1),
                    "vol_ratio": round(vol_ratio, 1),
                    "ret_1d": round(ret_1d, 2),
                    "ret_3d": round(ret_3d, 2),
                    "ret_5d": round(ret_5d, 2),
                    "max_5d": round(max_ret, 2),
                    "min_5d": round(min_ret, 2),
                    "macd_delay": macd_days_delay,
                    "macd_ret_5d": round(macd_ret_5d, 2) if macd_ret_5d is not None else None,
                }
            )

    # ─── 리포트 출력 ───
    print("=" * 65)
    print("  MOMENTUM 레짐 백테스트 — 숫자 검증 리포트")
    print("=" * 65)
    print(f"  스캔: {scan_count}종목, 시그널: {len(results)}건")
    print()

    df = pd.DataFrame(results)

    # 1. 전체 통계
    print("─── 1. 전체 MOMENTUM 시그널 (기관+외인 3일 연속 매수) ───")
    print(f"  1일후 평균:      {df['ret_1d'].mean():+.2f}%")
    print(f"  3일후 평균:      {df['ret_3d'].mean():+.2f}%")
    print(f"  5일후 평균:      {df['ret_5d'].mean():+.2f}%")
    print(f"  5일 최대 상승:   {df['max_5d'].mean():+.2f}%")
    print(f"  5일 최대 하락:   {df['min_5d'].mean():+.2f}%")
    win_rate = (df["ret_5d"] > 0).mean() * 100
    print(f"  5일후 수익 확률: {win_rate:.1f}%")
    print()

    # 2. RSI 65+ 구간
    high_rsi = df[df["rsi"] >= 65]
    print(f"─── 2. RSI 65+ 구간 (기존이면 진입 주저/차단) ───")
    print(f"  건수: {len(high_rsi)}건 ({len(high_rsi) / len(df) * 100:.1f}%)")
    if len(high_rsi) > 0:
        print(f"  5일후 평균:      {high_rsi['ret_5d'].mean():+.2f}%")
        print(f"  5일 최대 상승:   {high_rsi['max_5d'].mean():+.2f}%")
        win_hi = (high_rsi["ret_5d"] > 0).mean() * 100
        print(f"  5일후 수익 확률: {win_hi:.1f}%")
    print()

    # 3. 거래량 2x+
    vol_surge = df[df["vol_ratio"] >= 2.0]
    print(f"─── 3. 거래량 2x+ (강한 MOMENTUM) ───")
    print(f"  건수: {len(vol_surge)}건 ({len(vol_surge) / len(df) * 100:.1f}%)")
    if len(vol_surge) > 0:
        print(f"  5일후 평균:      {vol_surge['ret_5d'].mean():+.2f}%")
        print(f"  5일 최대 상승:   {vol_surge['max_5d'].mean():+.2f}%")
        win_vol = (vol_surge["ret_5d"] > 0).mean() * 100
        print(f"  5일후 수익 확률: {win_vol:.1f}%")
    print()

    # 4. MOMENTUM vs MACD 비교
    macd_valid = df[df["macd_ret_5d"].notna()]
    print(f"─── 4. MOMENTUM 진입 vs MACD 0선 크로스 진입 비교 ───")
    print(f"  비교 가능: {len(macd_valid)}건")
    if len(macd_valid) > 0:
        print(f"  MOMENTUM 진입 5일후: {macd_valid['ret_5d'].mean():+.2f}%")
        print(f"  MACD 0선 진입 5일후: {macd_valid['macd_ret_5d'].mean():+.2f}%")
        print(f"  MACD 지연일수 평균:  {macd_valid['macd_delay'].mean():.1f}일")
        mtm_better = (macd_valid["ret_5d"] > macd_valid["macd_ret_5d"]).mean() * 100
        print(f"  MOMENTUM이 나은 비율: {mtm_better:.1f}%")
        diff = macd_valid["ret_5d"].mean() - macd_valid["macd_ret_5d"].mean()
        print(f"  차이 (MTM - MACD):    {diff:+.2f}%p")
    print()

    # 5. 강한 시그널
    strong = df[(df["vol_ratio"] >= 1.5) & (df["rsi"] >= 50)]
    print(f"─── 5. 강한 시그널 (3일연속 + 거래량1.5x+ + RSI50+) ───")
    print(f"  건수: {len(strong)}건")
    if len(strong) > 0:
        print(f"  5일후 평균:      {strong['ret_5d'].mean():+.2f}%")
        print(f"  5일 최대 상승:   {strong['max_5d'].mean():+.2f}%")
        strong_win = (strong["ret_5d"] > 0).mean() * 100
        print(f"  수익 확률:       {strong_win:.1f}%")
        strong_macd = strong[strong["macd_ret_5d"].notna()]
        if len(strong_macd) > 0:
            print(f"  vs MACD 진입:    {strong_macd['macd_ret_5d'].mean():+.2f}%")
    print()

    # 6. 손절 비교
    sl_hit_35 = (df["min_5d"] <= -3.5).mean() * 100
    sl_hit_80 = (df["min_5d"] <= -8.0).mean() * 100
    print(f"─── 6. 손절(SL) 비교 ───")
    print(f"  5일 내 -3.5% 터치 (MTM SL): {sl_hit_35:.1f}%")
    print(f"  5일 내 -8.0% 터치 (NRM SL): {sl_hit_80:.1f}%")

    # SL 적용 후 실현 수익
    realized_mtm = []
    realized_nrm = []
    for _, r in df.iterrows():
        if r["min_5d"] <= -3.5:
            realized_mtm.append(-3.5)
        else:
            realized_mtm.append(r["ret_5d"])
        if r["min_5d"] <= -8.0:
            realized_nrm.append(-8.0)
        else:
            realized_nrm.append(r["ret_5d"])

    print(f"  MTM SL(-3.5%) 적용 평균: {np.mean(realized_mtm):+.2f}%")
    print(f"  NRM SL(-8.0%) 적용 평균: {np.mean(realized_nrm):+.2f}%")
    print()

    # 7. 수급 이탈 시그널 효과
    # 시그널 후 기관+외인 순매도 전환일 찾기 (proxy: ret가 마이너스인 케이스)
    print(f"─── 7. 최종 요약 ───")
    print(f"  MOMENTUM 진입 (3일째):  {df['ret_5d'].mean():+.2f}% (승률 {win_rate:.0f}%)")
    if len(macd_valid) > 0:
        print(f"  MACD 0선 진입:          {macd_valid['macd_ret_5d'].mean():+.2f}%")
        print(f"  MOMENTUM 초과수익:      {diff:+.2f}%p")
    print(f"  MTM SL(-3.5%) 실현:    {np.mean(realized_mtm):+.2f}%")
    print(f"  NRM SL(-8.0%) 실현:    {np.mean(realized_nrm):+.2f}%")
    print()
    print("=" * 65)


if __name__ == "__main__":
    run_backtest()
