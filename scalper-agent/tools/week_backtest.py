# -*- coding: utf-8 -*-
"""
MOMENTUM v2 — 5일 백테스트 시뮬레이션 (3/9~3/13)
매일 아침: 전일까지 데이터로 MOMENTUM 감지 → 당일 수익률 측정
"""

import json
import numpy as np
import pandas as pd
from pathlib import Path
from collections import defaultdict

DATA_DIR = Path(__file__).resolve().parent.parent / "data_store"
DAILY_DIR = DATA_DIR / "daily"
FLOW_DIR = DATA_DIR / "flow"

MOMENTUM_THRESHOLD = 0.40

def run():
    universe = json.load(open(DATA_DIR / "universe.json", "r", encoding="utf-8"))

    # 시총 2000억+ 필터
    big_codes = [c for c in universe if universe[c].get("cap_억", 0) >= 2000]
    print(f"시총 2000억+ 종목: {len(big_codes)}개")

    dates = ["2026-03-09", "2026-03-10", "2026-03-11", "2026-03-12", "2026-03-13"]

    # 데이터 프리로드
    daily_cache = {}
    flow_cache = {}
    for code in big_codes:
        dp = DAILY_DIR / f"{code}.csv"
        fp = FLOW_DIR / f"{code}_investor.csv"
        try:
            if dp.exists():
                daily_cache[code] = pd.read_csv(dp, index_col=0, parse_dates=True).sort_index()
        except:
            pass
        try:
            if fp.exists():
                flow_cache[code] = pd.read_csv(fp, index_col=0, parse_dates=True).sort_index()
        except:
            pass

    all_results = []

    for date in dates:
        cutoff = pd.Timestamp(date)
        momentum_stocks = []

        for code in big_codes:
            daily = daily_cache.get(code)
            flow = flow_cache.get(code)
            if daily is None or len(daily) < 21:
                continue

            daily_pre = daily[daily.index < cutoff]
            if len(daily_pre) < 21:
                continue

            flow_pre = None
            if flow is not None and len(flow) > 0:
                flow_pre = flow[flow.index < cutoff]
                if len(flow_pre) < 5:
                    flow_pre = None

            # (A) 거래량
            latest_vol = daily_pre["거래량"].iloc[-1]
            avg_20d = daily_pre["거래량"].iloc[-21:-1].mean()
            vol_ratio = latest_vol / avg_20d if avg_20d > 0 else 1.0
            vol_score = 0.25 if vol_ratio >= 3.0 else (0.15 if vol_ratio >= 2.0 else (0.08 if vol_ratio >= 1.5 else 0.0))

            # (B) 연속매수
            consec = 0
            if flow_pre is not None and len(flow_pre) > 0:
                for j in range(len(flow_pre) - 1, -1, -1):
                    row = flow_pre.iloc[j]
                    inst = row.get("기관_금액", 0) or 0
                    frgn = row.get("외국인_금액", 0) or 0
                    if (inst + frgn) > 0:
                        consec += 1
                    else:
                        break
            consec_score = 0.30 if consec >= 5 else (0.25 if consec >= 3 else (0.15 if consec >= 2 else 0.0))

            # (D) 기관 프로그램
            prog_score = 0.0
            if flow_pre is not None and len(flow_pre) >= 5:
                recent5 = flow_pre.iloc[-5:]
                pos_days = sum(1 for _, r in recent5.iterrows() if (r.get("기관_금액", 0) or 0) > 0)
                prog_score = 0.15 if pos_days >= 4 else (0.10 if pos_days >= 3 else 0.0)

            # (E) 조용한 매집
            quiet_score = 0.0
            if flow_pre is not None and len(flow_pre) >= 5 and len(daily_pre) >= 16:
                recent_f5 = flow_pre.iloc[-5:]
                inst_total = sum((r.get("기관_금액", 0) or 0) for _, r in recent_f5.iterrows())
                if inst_total > 0:
                    vol_r5 = daily_pre["거래량"].iloc[-5:].mean()
                    vol_e10 = daily_pre["거래량"].iloc[-15:-5].mean()
                    vol_ch = vol_r5 / vol_e10 if vol_e10 > 0 else 1
                    close5 = daily_pre["종가"].iloc[-5:]
                    pr = (close5.max() - close5.min()) / close5.mean() * 100
                    if vol_ch <= 1.3 and pr <= 3.0:
                        quiet_score = 0.10

            total = vol_score + consec_score + prog_score + quiet_score

            if total >= MOMENTUM_THRESHOLD:
                # 당일 수익률
                daily_on = daily[daily.index >= cutoff]
                if len(daily_on) == 0:
                    continue
                target_row = daily_on.iloc[0]
                if str(target_row.name.date()) != date:
                    continue

                day_chg = target_row.get("등락률", 0)
                prev_close = int(daily_pre["종가"].iloc[-1])

                # 고가/저가 기반 장중 최대/최소
                day_high = target_row.get("고가", 0)
                day_low = target_row.get("저가", 0)
                max_intra = (day_high / prev_close - 1) * 100 if prev_close > 0 else 0
                min_intra = (day_low / prev_close - 1) * 100 if prev_close > 0 else 0

                name = universe[code].get("name", code)
                sector = universe[code].get("sector", "")

                momentum_stocks.append({
                    "code": code, "name": name, "sector": sector,
                    "score": round(total, 3), "vol_ratio": round(vol_ratio, 1),
                    "consec": consec,
                    "ret_1d": round(day_chg, 2),
                    "max_intra": round(max_intra, 2),
                    "min_intra": round(min_intra, 2),
                })

        momentum_stocks.sort(key=lambda x: x["score"], reverse=True)

        avg_ret = np.mean([s["ret_1d"] for s in momentum_stocks]) if momentum_stocks else 0
        win_rate = np.mean([1 if s["ret_1d"] > 0 else 0 for s in momentum_stocks]) * 100 if momentum_stocks else 0
        big5 = [s for s in momentum_stocks if s["ret_1d"] >= 5]

        print(f"\n{'=' * 95}")
        print(f"  {date} — MOMENTUM 감지 {len(momentum_stocks)}종목 (전일 데이터 기준)")
        print(f"  당일 평균: {avg_ret:+.2f}%  |  승률: {win_rate:.0f}%  |  +5%이상: {len(big5)}종목")
        print(f"{'=' * 95}")

        for s in momentum_stocks[:15]:
            tag = " *** 급등!" if s["ret_1d"] >= 5 else (" * 손절" if s["ret_1d"] <= -3.5 else "")
            print(f"  {s['name']:>14}({s['code']}) score={s['score']:.2f} vol={s['vol_ratio']}x "
                  f"연속={s['consec']}D  →  당일 {s['ret_1d']:+.2f}% "
                  f"(장중 {s['min_intra']:+.1f}~{s['max_intra']:+.1f}%){tag}")
        if len(momentum_stocks) > 15:
            rest = momentum_stocks[15:]
            rest_avg = np.mean([s["ret_1d"] for s in rest])
            print(f"  ... 외 {len(rest)}종목 (평균 {rest_avg:+.2f}%)")

        for s in momentum_stocks:
            s["date"] = date
        all_results.extend(momentum_stocks)

    # ─── 전체 요약 ───
    print(f"\n{'=' * 95}")
    print(f"  5일 종합 시뮬레이션 요약 (3/9~3/13)")
    print(f"{'=' * 95}")

    total_signals = len(all_results)
    total_avg = np.mean([s["ret_1d"] for s in all_results])
    total_win = np.mean([1 if s["ret_1d"] > 0 else 0 for s in all_results]) * 100
    total_big5 = len([s for s in all_results if s["ret_1d"] >= 5])
    total_sl = len([s for s in all_results if s["ret_1d"] <= -3.5])

    # SL -3.5% 적용 실현수익
    realized = [-3.5 if s["ret_1d"] <= -3.5 else s["ret_1d"] for s in all_results]
    realized_avg = np.mean(realized)

    # 장중 SL 터치 (-3.5%)
    sl_intra = len([s for s in all_results if s["min_intra"] <= -3.5])

    print(f"  총 시그널: {total_signals}건 (일평균 {total_signals/5:.0f}건)")
    print(f"  평균 당일 수익률: {total_avg:+.2f}%")
    print(f"  승률: {total_win:.0f}%")
    print(f"  +5% 이상 급등: {total_big5}건 ({total_big5/total_signals*100:.1f}%)")
    print(f"  종가 -3.5% 이하: {total_sl}건 ({total_sl/total_signals*100:.1f}%)")
    print(f"  장중 -3.5% 터치: {sl_intra}건 ({sl_intra/total_signals*100:.1f}%)")
    print(f"  SL(-3.5%) 적용 실현수익: {realized_avg:+.2f}%")
    print()

    # 날짜별 요약
    for date in dates:
        dd = [s for s in all_results if s["date"] == date]
        if dd:
            avg = np.mean([s["ret_1d"] for s in dd])
            wr = np.mean([1 if s["ret_1d"] > 0 else 0 for s in dd]) * 100
            b5 = len([s for s in dd if s["ret_1d"] >= 5])
            print(f"  {date}: {len(dd):>3}종목 → 평균 {avg:+.2f}% (승률 {wr:.0f}%, 급등 {b5}건)")

    # 연속 MOMENTUM (3일+ 감지)
    code_dates = defaultdict(list)
    for s in all_results:
        code_dates[s["code"]].append(s)

    repeat_stocks = {c: data for c, data in code_dates.items() if len(data) >= 3}
    print(f"\n  3일+ 연속 MOMENTUM 감지: {len(repeat_stocks)}종목")
    for code, data in sorted(repeat_stocks.items(), key=lambda x: sum(s["ret_1d"] for s in x[1]), reverse=True)[:10]:
        total_ret = sum(s["ret_1d"] for s in data)
        days_str = " → ".join(f"{s['date'][-5:]}({s['ret_1d']:+.1f}%)" for s in data)
        print(f"  {data[0]['name']:>14}: {len(data)}일 감지 → 합산 {total_ret:+.1f}%  [{days_str}]")

    # 섹터별 분석
    sector_data = defaultdict(list)
    for s in all_results:
        sector_data[s["sector"]].append(s)

    print(f"\n  섹터별 MOMENTUM 성과:")
    for sector, data in sorted(sector_data.items(), key=lambda x: np.mean([s["ret_1d"] for s in x[1]]), reverse=True):
        if len(data) >= 3:
            avg = np.mean([s["ret_1d"] for s in data])
            wr = np.mean([1 if s["ret_1d"] > 0 else 0 for s in data]) * 100
            print(f"    {sector:>10}: {len(data):>3}건 → 평균 {avg:+.2f}% (승률 {wr:.0f}%)")

    # 만약 매일 전종목 매수했다면 vs MOMENTUM만 매수했다면
    print(f"\n  ── MOMENTUM 선별 효과 ──")
    # 전체 시장 평균 (시총2000억+)
    market_avg = []
    for date in dates:
        cutoff = pd.Timestamp(date)
        for code in big_codes:
            daily = daily_cache.get(code)
            if daily is None:
                continue
            daily_on = daily[daily.index >= cutoff]
            if len(daily_on) == 0:
                continue
            r = daily_on.iloc[0]
            if str(r.name.date()) != date:
                continue
            market_avg.append(r.get("등락률", 0))

    mkt = np.mean(market_avg)
    print(f"  시장 전체 (시총2000억+): 평균 {mkt:+.2f}% ({len(market_avg)}건)")
    print(f"  MOMENTUM 선별:          평균 {total_avg:+.2f}% ({total_signals}건)")
    print(f"  초과수익:               {total_avg - mkt:+.2f}%p")

    print(f"\n{'=' * 95}")


if __name__ == "__main__":
    run()
