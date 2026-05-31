# -*- coding: utf-8 -*-
"""점화 + 맥점(눌림 할인) 진입 백테스트 (사장님 5/31 코워크 핵심 조각).

앞 단계(ignition_trailing_backtest): 점화일 종가 추격 진입 = 어떤 청산도 avg ~+0.2%(breakeven).
원인 = entry(끝물 추격). 빠진 조각 = 맥점(살 자리, 눌림 할인).
이번 검증: 점화 후 PB_WIN일 내, **점화 종가보다 싸게(할인)** 눌릴 때만 진입(추격 금지). 안 오면 스킵.
  눌림 깊이(고점 대비) 3%/5%/8% 비교 → 얼마나 할인을 요구해야 양전환되나.
  → 추격(+0.2%) 대비 양전환되면 무기 = 점화(WHAT)+끼(필터)+맥점(WHERE)+줄먹(EXIT).

진입: run_high(점화후 고점) 갱신하며, 어느날 저가 ≤ run_high×(1-PULL) AND 체결가 < 점화종가(진짜 할인) → 진입.
       갭다운 관통 시 시가 체결. 가격≤0(불량데이터)·할인 아님·미눌림 → 스킵.
청산: ATR 트레일링(1.5×ATR, 4~15% clamp), 최대 H일.
★ 한계: 일봉/갭 시가체결/상폐부재(생존편향)/수수료·세금 미반영. 미진입 런어웨이=기회비용(손실 아님).
"""
from __future__ import annotations
import csv, sys
from pathlib import Path
import numpy as np

ROOT = Path(__file__).resolve().parent.parent
PROJ = ROOT.parent
sys.path.insert(0, str(ROOT))
from data.sajang_rules import SAJANG

DAILY = PROJ / "stock_data_daily"
H = 20
IGNITE_GAIN = 0.10
IGNITE_VOLX = 2.5
BASE_POS = 0.6
LIQ_FLOOR = 30.0
COOLDOWN = H
PB_WIN = 5
DEPTHS = (0.03, 0.05, 0.08)   # 고점 대비 눌림 깊이 비교
ACT = getattr(SAJANG, "TRAILING_ACTIVATION_PCT", 3.0) / 100.0


def _load(p):
    o, h, l, c, v = [], [], [], [], []
    try:
        with open(p, encoding="utf-8") as f:
            for r in csv.DictReader(f):
                try:
                    cc = float(r["close"])
                    if cc <= 0:
                        continue
                    c.append(cc); o.append(float(r["open"])); h.append(float(r["high"]))
                    l.append(float(r["low"])); v.append(float(r["volume"]))
                except (ValueError, KeyError):
                    continue
    except Exception:
        return None
    if len(c) < 21 + H + PB_WIN + 1:
        return None
    return np.array(o), np.array(h), np.array(l), np.array(c), np.array(v)


def sim_trail(entry, o, h, l, c, trail):
    """ATR/고정 트레일링, 최대 len(c)일. 반환 실현수익률(엔트리 대비)."""
    if entry <= 0:
        return None
    hw = entry; activated = False; stop = entry * (1 - trail)
    for d in range(len(c)):
        op, hd, ld = o[d], h[d], l[d]
        if ld <= stop:
            ex = stop if op >= stop else op
            return ex / entry - 1.0
        if hd / entry - 1.0 >= ACT:
            activated = True
        if hd > hw:
            hw = hd
        if activated:
            stop = max(stop, hw * (1 - trail))
    return c[-1] / entry - 1.0


def _atr_pct(o, h, l, c, i):
    sh = h[max(0, i-14):i+1]; sl = l[max(0, i-14):i+1]; sc = c[max(0, i-14):i+1]
    trs = [max(sh[k]-sl[k], abs(sh[k]-sc[k-1]), abs(sl[k]-sc[k-1])) for k in range(1, len(sc))]
    return (sum(trs)/len(trs))/c[i] if trs and c[i] > 0 else 0.0


def _find_entry(o, h, l, c, i, pull):
    """점화 i 후 PB_WIN일 내, 고점-pull 눌림 + 점화종가보다 할인된 체결가. 반환 (entry_px, entry_day) or (None,None)."""
    ign_close = c[i]
    run_high = h[i]
    for dd in range(1, PB_WIN + 1):
        day = i + dd
        trig = run_high * (1 - pull)
        if l[day] <= trig:
            cand = trig if o[day] >= trig else o[day]
            if 0 < cand < ign_close:          # 진짜 할인(추격 아님)
                return cand, day
        run_high = max(run_high, h[day])
    return None, None


def main():
    n_fire = 0
    res = {p: {"ent": 0, "ret": []} for p in DEPTHS}
    files = sorted(DAILY.glob("*.csv"))
    print(f"종목 {len(files)} | 점화후 {PB_WIN}일내 '고점-깊이 눌림 & 점화종가보다 할인' 진입(추격금지) | ATR 트레일 | H={H}")
    print(f"비교 추격기준: 점화종가 진입 = avg +0.2%(breakeven)\n")

    for p in files:
        d = _load(p)
        if d is None:
            continue
        o, h, l, c, v = d
        n = len(c)
        last = -COOLDOWN - 1
        for i in range(21, n - H - PB_WIN):
            pc = c[i - 1]
            if pc <= 0:
                continue
            vavg = v[i - 20:i].mean()
            if vavg <= 0:
                continue
            if c[i] * v[i] / 1e8 < LIQ_FLOOR:
                continue
            gain = c[i] / pc - 1.0
            volx = v[i] / vavg
            lows = l[i - 20:i]; highs = h[i - 20:i]
            band = highs.max() - lows.min()
            pos20 = (pc - lows.min()) / band if band > 0 else 0.5
            if not (gain >= IGNITE_GAIN and volx >= IGNITE_VOLX and pos20 <= BASE_POS and i >= last + COOLDOWN):
                continue
            last = i
            n_fire += 1
            atrp = max(0.04, min(0.15, 1.5 * _atr_pct(o, h, l, c, i)))
            for pull in DEPTHS:
                epx, ej = _find_entry(o, h, l, c, i, pull)
                if epx is None:
                    continue
                fo = o[ej+1:ej+1+H]; fh = h[ej+1:ej+1+H]; fl = l[ej+1:ej+1+H]; fc = c[ej+1:ej+1+H]
                if len(fc) < 1:
                    continue
                r = sim_trail(epx, fo, fh, fl, fc, atrp)
                if r is None or not np.isfinite(r):
                    continue
                res[pull]["ent"] += 1
                res[pull]["ret"].append(r)

    if n_fire == 0:
        print("fire 0"); return 1
    print(f"── 점화 {n_fire:,}건 / 눌림깊이별 (진입=할인 잡힌 거래만, ATR 트레일 청산) ──")
    print(f'  {"깊이":<6}{"진입률":>7}{"건수":>8}{"avg":>9}{"median":>9}{"승률":>8}{"≥+10%":>8}')
    for pull in DEPTHS:
        a = np.array(res[pull]["ret"])
        if len(a) == 0:
            print(f"  -{pull*100:.0f}%   진입 0"); continue
        er = res[pull]["ent"] / n_fire * 100
        print(f'  -{pull*100:.0f}%{er:>6.0f}%{len(a):>8,}{a.mean()*100:>+8.1f}%{np.median(a)*100:>+8.1f}%{(a>0).mean()*100:>7.1f}%{(a>=0.10).mean()*100:>7.1f}%')
    print(f"\n★ 판정: 추격 avg +0.2% 대비, 맥점(할인) 진입 avg가 확실히 높고(양수) 깊을수록↑면 = 맥점이 무기 결정조각.")
    print("  단 깊을수록 진입률↓(많이 놓침) = 트레이드오프. '얼마나 기다릴까'의 데이터 근거.")
    print("★ 한계: 일봉/상폐부재/수수료·세금 미반영/미진입 런어웨이=기회비용. 정밀화=분봉 + 명분·끼 결합.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
