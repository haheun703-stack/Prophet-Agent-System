# -*- coding: utf-8 -*-
"""이벤트 앞/뒤 변동 족적 프로파일러 (사장님 5/31 "잡은 경우의 수 앞/뒤 변동").

상한가 티어(GENERIC/NEAR_LIMIT/LIMIT_3RD+) 각각, 잡은 날(T0) 기준:
  앞(BEFORE, T-15→T0): 어떻게 쌓여 들어왔나 (run-up/기지) — T-15 종가 대비 누적%.
  뒤(AFTER, T0→T+15): 잡힌 뒤 어떻게 움직였나 (연속/소멸) — T0 종가 대비 누적%.
  거래량 프로파일: 각 시점 vol/MA20(T0 기준) — 거래량 터지는 타이밍.
평균 궤적으로 '족적'을 그린다. (단순 평균 + median)
★ 한계: 일봉/생존편향(상폐부재)/+25%는 상한가 close-to-close 프록시. 상대비교·형태로 해석.
"""
from __future__ import annotations
import csv, glob, sys
from pathlib import Path
import numpy as np

ROOT = Path(__file__).resolve().parent.parent.parent
DAILY = ROOT / "stock_data_daily"
PRE = 15      # 앞 일수
POST = 15     # 뒤 일수
VOL_X = 2.0
LIMIT = 0.25
PRIOR_WIN = 15


def _load(path):
    rows = []
    try:
        with open(path, encoding="utf-8") as f:
            for r in csv.DictReader(f):
                try:
                    o = float(r["open"]); h = float(r["high"]); l = float(r["low"])
                    c = float(r["close"]); v = float(r["volume"])
                except (ValueError, KeyError):
                    continue
                if o > 0 and c > 0:
                    rows.append((o, h, l, c, v))
    except Exception:
        return []
    return rows


def _classify(rows, i):
    pc = rows[i - 1][3]
    if pc <= 0:
        return set()
    up = rows[i][3] / pc - 1
    vma = sum(rows[j][4] for j in range(i - 20, i)) / 20
    if vma <= 0:
        return set()
    tiers = set()
    if up >= 0.10 and rows[i][4] >= VOL_X * vma:
        tiers.add("GENERIC")
    if up >= LIMIT:
        tiers.add("NEAR_LIMIT")
        prior = sum(1 for j in range(max(1, i - PRIOR_WIN), i)
                    if rows[j - 1][3] > 0 and rows[j][3] / rows[j - 1][3] - 1 >= LIMIT)
        if prior >= 2:
            tiers.add("LIMIT_3RD+")
    return tiers


def main():
    files = glob.glob(str(DAILY / "*.csv"))
    # tier -> list of (before_path[PRE+1], after_path[POST+1], vol_path[PRE+POST+1])
    acc = {"GENERIC": [], "NEAR_LIMIT": [], "LIMIT_3RD+": []}
    for f in files:
        rows = _load(f)
        n = len(rows)
        if n < 70:
            continue
        for i in range(max(60, PRE), n - POST):
            tiers = _classify(rows, i)
            if not tiers:
                continue
            c0 = rows[i][3]; c_pre = rows[i - PRE][3]
            if c0 <= 0 or c_pre <= 0:
                continue
            vma = sum(rows[j][4] for j in range(i - 20, i)) / 20
            if vma <= 0:
                continue
            before = [rows[i + t][3] / c_pre - 1 for t in range(-PRE, 1)]   # T-15..T0 rel T-15
            after = [rows[i + t][3] / c0 - 1 for t in range(0, POST + 1)]    # T0..T+15 rel T0
            volp = [rows[i + t][4] / vma for t in range(-PRE, POST + 1)]     # vol/MA20
            for t in tiers:
                acc[t].append((before, after, volp))

    def col(rows_list, idx, which):
        return np.array([r[which][idx] for r in rows_list])

    for tier in ("GENERIC", "NEAR_LIMIT", "LIMIT_3RD+"):
        seg = acc[tier]
        if not seg:
            print(f"\n[{tier}] 0건"); continue
        n = len(seg)
        print(f"\n══ [{tier}] {n:,}건 — 앞/뒤 평균 족적 ══")
        # BEFORE (rel T-15): T-15,-10,-5,-2,-1,0
        print("  앞(들어오기까지, T-15 종가 대비 누적%):")
        for t in (-15, -10, -5, -2, -1, 0):
            a = col(seg, t + PRE, 0)
            print(f"    T{t:>+3}: {a.mean()*100:>+7.1f}% (median {np.median(a)*100:>+6.1f}%)")
        # AFTER (rel T0): +1,+2,+3,+5,+10,+15
        print("  뒤(잡힌 뒤, T0 종가 대비 누적%):")
        for t in (1, 2, 3, 5, 10, 15):
            a = col(seg, t, 1)
            print(f"    T{t:>+3}: {a.mean()*100:>+7.1f}% (median {np.median(a)*100:>+6.1f}%)")
        # VOLUME profile
        print("  거래량(vol/MA20) — 터지는 타이밍:")
        for t in (-5, -2, -1, 0, 1, 3):
            a = col(seg, t + PRE, 2)
            print(f"    T{t:>+3}: {a.mean():>5.1f}x")

    print("\n★ 해석: 앞=어떻게 쌓여 들어왔나(기지/run-up·거래량 선행), 뒤=잡힌 뒤 연속/소멸. 티어 올라갈수록 뒤 연속성↑면 상한가 신호 우위.")
    print("★ 한계: 일봉/생존편향(상폐부재, 절대치 과대)/+25% close-to-close 프록시/평균은 outlier 영향 → median 병기.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
