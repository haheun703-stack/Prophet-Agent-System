# -*- coding: utf-8 -*-
"""상한가 티어 × STOP_REENTER — regime 창별(FULL/1년/6개월) (사장님 5/31).

배경: breakout_reentry(제네릭+10%) STOP_REENTER +5.58%. 끼↑→수익↑. 상한가엔진=최고끼.
사장님 정정(중요): **10년은 약세·횡보·초강세장 혼재 → 지금 regime 엣지 왜곡.**
  코스피 1년 상승 초강세장이 실제 매매장 → 검증은 1년/6개월 창으로.
질문: 제네릭 +10% vs 상한가 티어, **regime 창별로** STOP_REENTER 엣지가 어떻게 다른가.

티어(겹치는 부분집합, 동일 STOP_REENTER 틀·비용 0.48%/leg·보유5일·고점-3%):
  GENERIC +10%&2×vol / NEAR_LIMIT +25% / LIMIT_3RD+ (+25% & 직전15일 +25%일수≥2)
창: FULL(전체) / 1Y(이벤트일 ≥ cutoff_1y) / 6M(≥ cutoff_6m). 데이터 끝 ~2026-05-29.
★ 정직: 초강세장은 베타(뭘사도↑)로 절대수익 과대 → 동일창 GENERIC(브레이크아웃 베이스) 대비 상대 gradient로 해석.
  6M은 샘플 급감(특히 LIMIT_3RD+) → 통계 약함. 생존편향(상폐부재)·일봉 +25%프록시 잔존.
"""
from __future__ import annotations
import csv, glob, sys
from pathlib import Path
import numpy as np

ROOT = Path(__file__).resolve().parent.parent.parent
DAILY = ROOT / "stock_data_daily"
VOL_X = 2.0
TRAIL = 0.03
COST_LEG = 0.0048
HOLD_K = 5
LIMIT = 0.25
PRIOR_WIN = 15
CUT_1Y = "2025-05-29"
CUT_6M = "2025-11-29"


def _load(path):
    rows = []
    try:
        with open(path, encoding="utf-8") as f:
            for r in csv.DictReader(f):
                try:
                    dt = r.get("날짜") or r.get("") or r.get("date") or ""
                    o = float(r["open"]); h = float(r["high"]); l = float(r["low"])
                    c = float(r["close"]); v = float(r["volume"])
                except (ValueError, KeyError):
                    continue
                if o > 0 and c > 0:
                    rows.append((dt, o, h, l, c, v))
    except Exception:
        return []
    return rows


def _classify(rows, i):
    pc = rows[i - 1][4]
    if pc <= 0:
        return set()
    up = rows[i][4] / pc - 1
    vma = sum(rows[j][5] for j in range(i - 20, i)) / 20
    if vma <= 0:
        return set()
    tiers = set()
    if up >= 0.10 and rows[i][5] >= VOL_X * vma:
        tiers.add("GENERIC")
    if up >= LIMIT:
        tiers.add("NEAR_LIMIT")
        prior = sum(1 for j in range(max(1, i - PRIOR_WIN), i)
                    if rows[j - 1][4] > 0 and rows[j][4] / rows[j - 1][4] - 1 >= LIMIT)
        if prior >= 2:
            tiers.add("LIMIT_3RD+")
    return tiers


def _sim(rows, e, hold_k=HOLD_K):
    entry = rows[e][4]
    end = min(e + hold_k, len(rows) - 1)
    days = list(range(e + 1, end + 1))
    if not days:
        return None
    hold_ret = (rows[end][4] / entry - 1 - COST_LEG) * 100
    legs = 0.0; in_pos = True; p_in = entry; hw = entry; last_stop = entry * (1 - TRAIL)
    for d in days:
        _dt, o, h, l, c, v = rows[d]
        if in_pos:
            hw = max(hw, h)
            stop = hw * (1 - TRAIL); last_stop = stop
            if l <= stop:
                legs += (stop / p_in - 1) - COST_LEG
                in_pos = False
        else:
            if c >= last_stop:
                in_pos = True; p_in = c; hw = c
    if in_pos:
        legs += (rows[end][4] / p_in - 1) - COST_LEG
    return {"hold": hold_ret, "reenter": legs * 100}


def main():
    files = glob.glob(str(DAILY / "*.csv"))
    # window -> tier -> list
    WINS = ("FULL", "1Y", "6M")
    data = {w: {"GENERIC": [], "NEAR_LIMIT": [], "LIMIT_3RD+": []} for w in WINS}
    for f in files:
        rows = _load(f)
        if len(rows) < 70:
            continue
        for i in range(60, len(rows) - 1):
            tiers = _classify(rows, i)
            if not tiers:
                continue
            r = _sim(rows, i)
            if not r:
                continue
            dt = rows[i][0]
            for t in tiers:
                data["FULL"][t].append(r)
                if dt >= CUT_1Y:
                    data["1Y"][t].append(r)
                if dt >= CUT_6M:
                    data["6M"][t].append(r)

    print(f"상한가 티어 × STOP_REENTER (보유{HOLD_K}일, 고점-3%, 비용 {COST_LEG*100:.2f}%/leg)")
    print(f"창: FULL(10년) / 1Y(≥{CUT_1Y}) / 6M(≥{CUT_6M}). ★초강세장 베타 → 동일창 GENERIC 대비 gradient로 해석\n")
    for w in WINS:
        print(f"── [{w}] ──")
        print(f'  {"티어":<12}{"이벤트":>8}{"HOLD":>9}{"STOP_REENTER":>14}{"median":>9}{"승률":>7}')
        for t in ("GENERIC", "NEAR_LIMIT", "LIMIT_3RD+"):
            seg = data[w][t]
            if not seg:
                print(f"  {t:<12}{0:>8}"); continue
            re = np.array([x["reenter"] for x in seg]); ho = np.array([x["hold"] for x in seg])
            note = " ←표본부족" if len(seg) < 100 else ""
            print(f'  {t:<12}{len(seg):>8,}{ho.mean():>+8.2f}%{re.mean():>+13.2f}%{np.median(re):>+8.2f}%{(re>0).mean()*100:>6.0f}%{note}')
        print()
    print("★ 판정: 동일창 내 GENERIC→NEAR→3RD gradient 유지되면 = 상한가 우위는 regime 무관 견고.")
    print("  단 절대수익(특히 6M)은 초강세장 베타 과대 → HOLD도 양수면 '뭘사도 오름' 주의. 상대비교만 신뢰.")
    print("★ 한계: 생존편향(상폐부재)/일봉+25%프록시/수급필터 미반영/6M 표본부족.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
