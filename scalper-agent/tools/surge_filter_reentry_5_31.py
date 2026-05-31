# -*- coding: utf-8 -*-
"""연속자 필터(종가강도·거래량·과열회피) × STOP_REENTER 리프트 — 진짜 워치리스트 필터 (사장님 5/31).

앞 발견(dormant_first_surge): 휴면→첫급등 후 연속자 vs 페이더 가르는 = 종가강도(≥0.8→연속66%)·거래량(20x)·과열아님(페이더 +39% blow-off).
이번: 그 필터로 '연속자만' 골랐을 때 STOP_REENTER 수익이 무필터 대비 얼마나 오르나? = 워치리스트 필터의 값어치.
regime 창(FULL/1Y/6M) — 사장님 "초강세장이 지금 장" 정정 반영. 베타 주의(상대 비교).

이벤트: 휴면(직전20일 최대상승<10% & 기지 pos20≤60%) → T0 급등 ≥SURGE.
필터(연속자 게이트): 종가강도≥CS_MIN AND 거래량≥VOL_MIN×MA20 AND 당일상승≤OVERHEAT(blow-off 회피).
청산: STOP_REENTER (고점-3% 손절→회복시 재진입, 보유5일, 비용 0.48%/leg).
★ 한계: 일봉/생존편향(상폐부재)/수급 미반영(OHLCV 필터만)/6M 표본 thin. 상대 리프트로 해석.
"""
from __future__ import annotations
import csv, glob, sys
from pathlib import Path
import numpy as np

ROOT = Path(__file__).resolve().parent.parent
PROJ = ROOT.parent
DAILY = PROJ / "stock_data_daily"

SURGE = 0.15
DORMANT_MAX = 0.10
BASE_POS = 0.60
TRAIL = 0.03
COST_LEG = 0.0048
HOLD_K = 5
PRE = 20
# 필터 (연속자 게이트)
CS_MIN = 0.80
VOL_MIN = 5.0
OVERHEAT = 0.30   # 당일상승 이 이상이면 blow-off로 제외
CUT_1Y = "2025-05-29"
CUT_6M = "2025-11-29"


def _load(p):
    rows = []
    try:
        with open(p, encoding="utf-8") as f:
            rd = csv.reader(f); next(rd, None)
            for r in rd:
                try:
                    dt = r[0]; o = float(r[1]); h = float(r[2]); l = float(r[3]); c = float(r[4]); v = float(r[5])
                except (ValueError, IndexError):
                    continue
                if o > 0 and c > 0:
                    rows.append((dt, o, h, l, c, v))
    except Exception:
        return []
    return rows


def _sim_reenter(rows, e):
    entry = rows[e][4]
    end = min(e + HOLD_K, len(rows) - 1)
    days = list(range(e + 1, end + 1))
    if not days:
        return None
    legs = 0.0; in_pos = True; p_in = entry; hw = entry; last_stop = entry * (1 - TRAIL)
    for d in days:
        _dt, o, h, l, c, v = rows[d]
        if in_pos:
            hw = max(hw, h); stop = hw * (1 - TRAIL); last_stop = stop
            if l <= stop:
                legs += (stop / p_in - 1) - COST_LEG; in_pos = False
        else:
            if c >= last_stop:
                in_pos = True; p_in = c; hw = c
    if in_pos:
        legs += (rows[end][4] / p_in - 1) - COST_LEG
    return legs * 100


def main():
    WINS = ("FULL", "1Y", "6M")
    # window -> {"all":[], "filt":[]}  (reenter%), + cont counts
    agg = {w: {"all": [], "filt": []} for w in WINS}
    for p in glob.glob(str(DAILY / "*.csv")):
        rows = _load(p)
        if len(rows) < PRE + HOLD_K + 2:
            continue
        n = len(rows)
        for i in range(PRE, n - HOLD_K):
            pc = rows[i - 1][4]
            if pc <= 0:
                continue
            up = rows[i][4] / pc - 1
            if up < SURGE:
                continue
            prior_max = 0.0
            for j in range(i - PRE, i):
                ppc = rows[j - 1][4]
                if ppc > 0:
                    prior_max = max(prior_max, rows[j][4] / ppc - 1)
            if prior_max >= DORMANT_MAX:
                continue
            lows = [rows[j][3] for j in range(i - 20, i)]; highs = [rows[j][2] for j in range(i - 20, i)]
            band = max(highs) - min(lows)
            pos20 = (pc - min(lows)) / band if band > 0 else 0.5
            if pos20 > BASE_POS:
                continue
            r = _sim_reenter(rows, i)
            if r is None:
                continue
            h0 = rows[i][2]; l0 = rows[i][3]; c0 = rows[i][4]
            cs = (c0 - l0) / (h0 - l0) if h0 > l0 else 0.5
            vma = sum(rows[j][5] for j in range(i - 20, i)) / 20
            volx = rows[i][5] / vma if vma > 0 else 0
            passed = (cs >= CS_MIN and volx >= VOL_MIN and up <= OVERHEAT)
            dt = rows[i][0]
            for w, ok in (("FULL", True), ("1Y", dt >= CUT_1Y), ("6M", dt >= CUT_6M)):
                if not ok:
                    continue
                agg[w]["all"].append(r)
                if passed:
                    agg[w]["filt"].append(r)

    print(f"휴면→첫급등(≥{SURGE:.0%}) × STOP_REENTER | 필터: 종가강도≥{CS_MIN} & 거래량≥{VOL_MIN}x & 당일상승≤{OVERHEAT:.0%}(과열회피)")
    print(f"보유{HOLD_K}일·고점-3%·비용{COST_LEG*100:.2f}%/leg | 창 FULL/1Y(≥{CUT_1Y})/6M(≥{CUT_6M})\n")
    print(f'  {"창":<5}{"무필터 n":>9}{"무필터 avg":>11}{"승률":>6}   {"필터 n":>8}{"선택률":>7}{"필터 avg":>10}{"승률":>6}{"리프트":>9}')
    for w in WINS:
        a = np.array(agg[w]["all"]); fz = np.array(agg[w]["filt"])
        if len(a) == 0:
            print(f"  {w:<5} 0"); continue
        if len(fz) == 0:
            print(f"  {w:<5}{len(a):>9,}{a.mean():>+10.2f}%{(a>0).mean()*100:>5.0f}%   필터통과 0"); continue
        lift = fz.mean() - a.mean()
        sel = len(fz) / len(a) * 100
        note = " ←thin" if len(fz) < 50 else ""
        print(f'  {w:<5}{len(a):>9,}{a.mean():>+10.2f}%{(a>0).mean()*100:>5.0f}%   {len(fz):>8,}{sel:>6.0f}%{fz.mean():>+9.2f}%{(fz>0).mean()*100:>5.0f}%{lift:>+8.2f}p{note}')
    print(f"\n★ 판정: 필터 avg가 무필터보다 확실히 높고(리프트>0) 승률↑면 = 종가강도·거래량·과열회피가 진짜 워치리스트 필터.")
    print("  선택률(필터통과%)이 낮을수록 적게 잡지만 질↑ 트레이드오프. 6M thin 주의.")
    print("★ 한계: 일봉/생존편향/수급 미반영(OHLCV 필터만)/초강세장 베타. 상대 리프트로 해석.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
