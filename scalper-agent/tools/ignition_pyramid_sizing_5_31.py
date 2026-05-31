# -*- coding: utf-8 -*-
"""점화 + 비대칭 사이징(피라미딩) 백테스트 — 마지막 기계 레버 (사장님 5/31).

병렬세션(ignition_trailing/pullback/filter)이 다 테스트한 건 **고정 사이즈**.
미테스트 = 사이징. 리버모어 피라미딩 = 실패엔 작게(10% 정찰), 승자엔 크게(불타기).
질문: 고정사이즈 breakeven 엣지를, 비대칭 사이징이 portfolio 양수로 뒤집나(tail x2.13 포착)?

동일성(비교 가능): 점화 정의·데이터·entry(점화종가)·H=20·보수적 stop = trailing 도구와 동일.
차이: 사이징만. ★ 비용 반영(피라미딩=레그↑=비용↑, 정직).

피라미딩: tranche 0=10%(entry) → 고가가 +ADD레벨 도달 시 20/30/40% 추가(불타기, 같은 트레일 공유).
  stop 도달 → 전량 청산. 손익 = Σ(청산가/트랜치진입가 -1)×weight − 비용. (full unit U 대비 %)
비교군: FULL_FIXED(weight 1.0 entry=병렬 베이스) vs PYRAMID(8%/ATR 트레일).
★ 한계: 일봉 보수적 stop / 갭 시가체결 / 상폐부재(생존편향) / 비용 추정.
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
IGNITE_GAIN = 0.10; IGNITE_VOLX = 2.5; BASE_POS = 0.6; LIQ_FLOOR = 30.0; COOLDOWN = H
ACT = getattr(SAJANG, "TRAILING_ACTIVATION_PCT", 3.0) / 100.0

# 피라미딩 스케줄: weight(전체 U 대비) / add_level(entry 대비 고가 도달선)
WEIGHTS = [0.10, 0.20, 0.30, 0.40]
ADD_LEVELS = [0.00, 0.05, 0.12, 0.20]   # 정찰 / +5% / +12% / +20% (신고가 = 판단 적중)
COST_BUY = 0.0015   # 매수 슬리피지(편도)
COST_SELL = 0.0033  # 매도 슬리피지+세금


def _load(p):
    o, h, l, c, v = [], [], [], [], []
    try:
        with open(p, encoding="utf-8") as f:
            for r in csv.DictReader(f):
                try:
                    c.append(float(r["close"])); o.append(float(r["open"]))
                    h.append(float(r["high"])); l.append(float(r["low"])); v.append(float(r["volume"]))
                except (ValueError, KeyError):
                    continue
    except Exception:
        return None
    if len(c) < 21 + H + 1:
        return None
    return np.array(o), np.array(h), np.array(l), np.array(c), np.array(v)


def sim_pyramid(entry, o, h, l, c, maxhold, trail, init_stop, weights, add_levels):
    """피라미딩. 반환 (실현손익[U대비], 청산사유, 투입자본[U대비])."""
    filled = [(entry, weights[0])]   # (진입가, weight)
    nxt = 1
    hw = entry; activated = False; stop = entry * (1 - init_stop)
    for d in range(maxhold):
        op, hd, ld = o[d], h[d], l[d]
        if ld <= stop:                                   # 전량 청산(보수적: stop 먼저)
            ex = stop if op >= stop else op              # 갭다운 시가체결
            deployed = sum(w for _, w in filled)
            pnl = sum((ex / fp - 1) * w for fp, w in filled)
            cost = sum(w for _, w in filled) * COST_BUY + deployed * COST_SELL
            return pnl - cost, ("TRAIL" if activated else "STOP"), deployed
        while nxt < len(weights) and hd >= entry * (1 + add_levels[nxt]):   # 불타기
            filled.append((entry * (1 + add_levels[nxt]), weights[nxt])); nxt += 1
        if hd / entry - 1.0 >= ACT:
            activated = True
        if hd > hw:
            hw = hd
        if activated:
            stop = max(stop, hw * (1 - trail))
    ex = c[maxhold - 1]                                  # 시간 청산
    deployed = sum(w for _, w in filled)
    pnl = sum((ex / fp - 1) * w for fp, w in filled)
    cost = sum(w for _, w in filled) * COST_BUY + deployed * COST_SELL
    return pnl - cost, "TIME", deployed


def sim_full(entry, o, h, l, c, maxhold, trail, init_stop):
    """고정 풀사이즈(weight 1.0) = 병렬 베이스. 비용 반영."""
    hw = entry; activated = False; stop = entry * (1 - init_stop)
    for d in range(maxhold):
        op, hd, ld = o[d], h[d], l[d]
        if ld <= stop:
            ex = stop if op >= stop else op
            return (ex / entry - 1.0) - COST_BUY - COST_SELL, ("TRAIL" if activated else "STOP")
        if hd / entry - 1.0 >= ACT:
            activated = True
        if hd > hw:
            hw = hd
        if activated:
            stop = max(stop, hw * (1 - trail))
    return (c[maxhold - 1] / entry - 1.0) - COST_BUY - COST_SELL, "TIME"


def _atr_pct(o, h, l, c, i):
    sh, sl, sc = h[max(0, i-14):i+1], l[max(0, i-14):i+1], c[max(0, i-14):i+1]
    trs = [max(sh[k]-sl[k], abs(sh[k]-sc[k-1]), abs(sl[k]-sc[k-1])) for k in range(1, len(sc))]
    return (sum(trs)/len(trs))/c[i] if trs and c[i] > 0 else 0.0


def main():
    full8, pyr8, pyrA = [], [], []
    deps = []
    files = sorted(DAILY.glob("*.csv"))
    print(f"종목 {len(files)} | 점화 entry=종가 | H={H} | 비용 매수{COST_BUY:.2%}+매도{COST_SELL:.2%}")
    print(f"피라미딩: {WEIGHTS} @ entry/+5/+12/+20% (신고가 불타기)\n")
    for p in files:
        d = _load(p)
        if d is None:
            continue
        o, h, l, c, v = d
        n = len(c); last = -COOLDOWN - 1
        for i in range(21, n - H):
            pc = c[i-1]
            if pc <= 0:
                continue
            vavg = v[i-20:i].mean()
            if vavg <= 0 or c[i]*v[i]/1e8 < LIQ_FLOOR:
                continue
            gain = c[i]/pc - 1.0; volx = v[i]/vavg
            lows = l[i-20:i]; highs = h[i-20:i]; band = highs.max()-lows.min()
            pos20 = (pc-lows.min())/band if band > 0 else 0.5
            if gain >= IGNITE_GAIN and volx >= IGNITE_VOLX and pos20 <= BASE_POS and i >= last+COOLDOWN:
                last = i; entry = c[i]
                fo, fh, fl, fc = o[i+1:i+1+H], h[i+1:i+1+H], l[i+1:i+1+H], c[i+1:i+1+H]
                atrp = max(0.04, min(0.15, 1.5*_atr_pct(o, h, l, c, i)))
                rf, _ = sim_full(entry, fo, fh, fl, fc, H, 0.08, 0.08); full8.append(rf)
                rp, _, dep = sim_pyramid(entry, fo, fh, fl, fc, H, 0.08, 0.08, WEIGHTS, ADD_LEVELS)
                pyr8.append(rp); deps.append(dep)
                ra, _, _ = sim_pyramid(entry, fo, fh, fl, fc, H, atrp, atrp, WEIGHTS, ADD_LEVELS)
                pyrA.append(ra)
    if not full8:
        print("fire 없음"); return 1
    f8 = np.array(full8); p8 = np.array(pyr8); pa = np.array(pyrA); dp = np.array(deps)
    nf = len(f8)

    def line(nm, a, dep=None):
        s = (f"  {nm:<24} avg {a.mean()*100:+6.2f}% | median {np.median(a)*100:+6.2f}% | "
             f"승률 {(a>0).mean()*100:4.1f}% | ≥+10% {(a>=0.10).mean()*100:4.1f}%")
        if dep is not None:
            s += f" | 평균투입 {dep.mean()*100:4.0f}%U"
        return s

    print(f"── 점화 {nf:,}건 / 사이징 비교 (U=풀유닛, 비용 반영) ──")
    print(line("FULL_FIXED(풀,트레일8%)", f8))
    print(line("PYRAMID(트레일8%)", p8, dp))
    print(line("PYRAMID(트레일ATR)", pa, dp))
    print()
    # 비대칭 효과: 손익을 투입자본 대비로도 (자본효율)
    roi8 = (p8 / np.maximum(dp, 0.01))
    print(f"  PYRAMID 투입자본대비 ROI avg {roi8.mean()*100:+.2f}% (자본효율)")
    print(f"  PYRAMID 평균 투입 {dp.mean()*100:.0f}%U (실패=10%만, 승자=불타기) | 풀투입(100%) 도달 {(dp>=0.99).mean()*100:.0f}%")
    print("\n★ 판정: PYRAMID avg > FULL avg & 0 초과(비용후)면 = 비대칭 사이징이 엣지를 살림 = 캐시카우 후보.")
    print("  PYRAMID ≈ FULL ≈ 0이면 = 사이징도 못 살림 → 일봉 기계화 최종 종결, 6/1 shadow(분봉·수급)로.")
    print("★ 한계: 일봉 보수적 stop / 갭 시가체결 / 상폐부재 / 비용 추정. 분봉 path는 못 봄.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
