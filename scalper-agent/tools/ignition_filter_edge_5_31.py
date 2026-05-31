# -*- coding: utf-8 -*-
"""점화 × 끼·명분(기술위치) 필터 → 실현수익 양전환되나 (사장님 5/31 코워크 수렴).

앞 결론: 점화 신호 통계엣지 실재(고점 x1.6~2.3)지만 기계적 일봉(추격/맥점 × 트레일)=breakeven(avg ~0%).
이번: 점화 '전부'가 아니라 **좋은 끼(HUNTABLE+) + 기술위치(MA20 위·상승)** 만 골라도 실현수익이 양(+)전환되나?
  = 4단(명분·끼 필터) + 점화의 수렴 검증. 끼가 fwd_max에선 x2.13였음 — 실현수익에서도?

진입: 점화일 종가(추격, 가장 단순·보수). 청산: ATR 트레일(1.5×ATR, 4~15%), 최대 H.
필터: 끼 등급(score_kki, 일봉 산출) × 기술위치(close>MA20 & MA20 상승 = 명분 일부 프록시).
★ 명분 풀버전(재료·수급)은 외부데이터라 일봉 백테스트 불가 — 기술위치만 프록시. 한계 명시.
★ 한계: 일봉/상폐부재(생존편향)/수수료·세금 미반영/명분 부분프록시.
"""
from __future__ import annotations
import csv, json, sys
from pathlib import Path
import numpy as np

ROOT = Path(__file__).resolve().parent.parent
PROJ = ROOT.parent
sys.path.insert(0, str(ROOT))
from data.limit_up_scanner import LimitUpStock, score_kki, count_surge_limit_days
from data.sajang_rules import SAJANG

DAILY = PROJ / "stock_data_daily"
H = 20
IGNITE_GAIN = 0.10
IGNITE_VOLX = 2.5
BASE_POS = 0.6
LIQ_FLOOR = 30.0
COOLDOWN = H
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
    if len(c) < 25 + H:
        return None
    return np.array(o), np.array(h), np.array(l), np.array(c), np.array(v)


def sim_atr(entry, o, h, l, c, trail):
    if entry <= 0:
        return None
    hw = entry; act = False; stop = entry * (1 - trail)
    for d in range(len(c)):
        op, hd, ld = o[d], h[d], l[d]
        if ld <= stop:
            ex = stop if op >= stop else op
            return ex / entry - 1.0
        if hd / entry - 1.0 >= ACT:
            act = True
        if hd > hw:
            hw = hd
        if act:
            stop = max(stop, hw * (1 - trail))
    return c[-1] / entry - 1.0


def main():
    uni = {}
    try:
        uni = json.load(open(ROOT / "data_store" / "universe.json", encoding="utf-8"))
    except Exception:
        pass
    by_grade = {g: [] for g in ("EXPLOSIVE", "HUNTABLE", "MODERATE", "SLUGGISH")}
    combo = {"끼HUNT+_위치O": [], "끼HUNT+_위치X": [], "끼<HUNT": []}
    allr = []
    files = sorted(DAILY.glob("*.csv"))
    print(f"종목 {len(files)} | 진입=점화종가(추격) | 청산=ATR트레일 | 필터=끼등급 × 기술위치(MA20위·상승) | H={H}")
    print(f"비교 기준: 무필터 전체 점화 = breakeven(avg ~0%)\n")

    for p in files:
        stem = p.stem; name, _, code = stem.rpartition("_")
        d = _load(p)
        if d is None:
            continue
        o, h, l, c, v = d
        n = len(c)
        last = -COOLDOWN - 1
        cap = uni.get(code, {}).get("cap_억", 0)
        for i in range(21, n - H):
            pc = c[i - 1]
            if pc <= 0:
                continue
            vavg = v[i - 20:i].mean()
            if vavg <= 0:
                continue
            tv = c[i] * v[i] / 1e8
            if tv < LIQ_FLOOR:
                continue
            gain = c[i] / pc - 1.0
            volx = v[i] / vavg
            lows = l[i - 20:i]; highs = h[i - 20:i]
            band = highs.max() - lows.min()
            pos20 = (pc - lows.min()) / band if band > 0 else 0.5
            if not (gain >= IGNITE_GAIN and volx >= IGNITE_VOLX and pos20 <= BASE_POS and i >= last + COOLDOWN):
                continue
            last = i
            # 실현수익 (추격 + ATR)
            sh = h[max(0, i-14):i+1]; sl = l[max(0, i-14):i+1]; sc = c[max(0, i-14):i+1]
            trs = [max(sh[k]-sl[k], abs(sh[k]-sc[k-1]), abs(sl[k]-sc[k-1])) for k in range(1, len(sc))]
            atr_pct = (sum(trs)/len(trs))/c[i]*100 if trs else 0
            atrp = max(0.04, min(0.15, 1.5 * (atr_pct/100)))
            r = sim_atr(c[i], o[i+1:i+1+H], h[i+1:i+1+H], l[i+1:i+1+H], c[i+1:i+1+H], atrp)
            if r is None or not np.isfinite(r):
                continue
            allr.append(r)
            # 끼
            surge, limit = count_surge_limit_days(list(c[:i+1]))
            st = LimitUpStock(code=code, name=name, close=int(c[i]), volume_ratio=round(float(volx), 2),
                              trading_value_억=round(float(tv), 1),
                              turnover_pct=round(tv / cap * 100, 2) if cap else 0,
                              close_strength=round(float((c[i]-l[i])/(h[i]-l[i])), 3) if h[i] > l[i] else 0.5,
                              consecutive_limit=0)
            kki = score_kki(st, atr_pct, surge, limit); grade = SAJANG.kki_grade(kki)
            by_grade[grade].append(r)
            # 기술위치(명분 프록시): close>MA20 & MA20 상승
            ma20 = c[i-19:i+1].mean(); ma20_prev = c[i-24:i-4].mean() if i >= 24 else ma20
            pos_ok = c[i] > ma20 and ma20 > ma20_prev
            if grade in ("HUNTABLE", "EXPLOSIVE"):
                combo["끼HUNT+_위치O" if pos_ok else "끼HUNT+_위치X"].append(r)
            else:
                combo["끼<HUNT"].append(r)

    if not allr:
        print("fire 0"); return 1
    allr = np.array(allr)

    def stat(nm, lst):
        if not lst:
            print(f"  {nm:<16} 0건"); return
        a = np.array(lst)
        print(f"  {nm:<16}{len(a):>7,}{a.mean()*100:>+8.1f}%{np.median(a)*100:>+8.1f}%{(a>0).mean()*100:>7.1f}%{(a>=0.10).mean()*100:>7.1f}%")

    print(f"── 전체 점화 {len(allr):,}건 (무필터, 추격+ATR) ──")
    print(f"  avg {allr.mean()*100:+.1f}% | median {np.median(allr)*100:+.1f}% | 승률 {(allr>0).mean()*100:.1f}%\n")
    print(f'  {"버킷":<16}{"건수":>7}{"avg":>8}{"median":>8}{"승률":>7}{"≥+10%":>7}')
    print("── 끼 등급별 ──")
    for g in ("EXPLOSIVE", "HUNTABLE", "MODERATE", "SLUGGISH"):
        stat(g, by_grade[g])
    print("── 끼 × 기술위치(명분 프록시) ──")
    for k in ("끼HUNT+_위치O", "끼HUNT+_위치X", "끼<HUNT"):
        stat(k, combo[k])
    print(f"\n★ 판정: '끼HUNT+_위치O' avg가 전체(~0%) 대비 확실히 양수+승률↑면 = 끼·명분 필터가 점화를 무기로 만듦(4단 수렴).")
    print("  양수 안 되면 = 일봉+부분명분으론 부족, 진짜 명분(수급·재료)+분봉 필요 or 워치리스트용.")
    print("★ 한계: 일봉/상폐부재/수수료세금 미반영/명분=기술위치만 프록시(재료·수급 외부데이터 제외).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
