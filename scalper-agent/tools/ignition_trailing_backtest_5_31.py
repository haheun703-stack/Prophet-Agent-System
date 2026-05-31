# -*- coding: utf-8 -*-
"""점화 entry + 트레일링/손절 수확 백테스트 (사장님 5/31 코워크 핵심).

엣지 측정(ignition_edge_study)이 밝힌 모순:
  점화 후 fwd_max avg +15.3% (스파이크 있음) BUT 20일 홀딩 median -5.2% (다 토해냄).
질문: SAJANG 매도규율(진입-3%손절 / +3%↑ 고점-3% 트레일링)로 그 스파이크를 실제로 잡나?
  = 무기가 '쓸 수 있는' 것인지의 결정적 검증. 못 잡으면 엣지는 허상.

비교 정책(전부 점화일 종가 entry, 동일 fire 집합):
  HOLD_H        : H일 홀딩 후 종가 (베이스 비교군, median -5.2%)
  TRAIL_H       : 트레일링/손절, 최대 H일
  TRAIL_D3      : 트레일링/손절, 최대 3일 (매도철학 ⑦ D+3 초과금지)
지표: 실현수익 avg/median / P(>0) / P(≥+10%) / 스파이크 수확률(실현/fwd_max) / 청산사유 분포.

★ 정직 한계: 일봉(분봉 path 불명 → stop을 high 갱신보다 먼저 검사=보수적/비관),
  갭다운은 시가 체결(slippage 반영), 상폐종목 부재(생존편향 잔존), 수수료/세금 미반영.
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

TRAIL = getattr(SAJANG, "TRAILING_PCT", 3.0) / 100.0          # 고점 -3%
ACT = getattr(SAJANG, "TRAILING_ACTIVATION_PCT", 3.0) / 100.0 # +3% 발동
INIT_STOP = getattr(SAJANG, "TRAILING_PCT", 3.0) / 100.0      # 진입 -3% 손절


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


def sim_trail(entry, o, h, l, c, maxhold, trail, init_stop, act=ACT):
    """점화 다음날부터 maxhold일. 보수적: 매일 stop(저가) 먼저 검사 후 고점 갱신.
    trail/init_stop = 비율(0.03 등). 반환 (실현수익률, 청산사유)."""
    hw = entry
    activated = False
    stop = entry * (1 - init_stop)
    for d in range(maxhold):
        op, hd, ld = o[d], h[d], l[d]
        if ld <= stop:                       # 손절/트레일 발동
            ex = stop if op >= stop else op   # 갭다운이면 시가 체결(슬리피지)
            return ex / entry - 1.0, ("TRAIL" if activated else "STOP")
        if hd / entry - 1.0 >= act:
            activated = True
        if hd > hw:
            hw = hd
        if activated:
            stop = max(stop, hw * (1 - trail))
    return c[maxhold - 1] / entry - 1.0, "TIME"   # 시간 청산(마지막 종가)


def _atr_pct(o, h, l, c, i):
    """점화일 i 시점까지 14일 ATR% (entry 대비)."""
    seg_h = h[max(0, i-14):i+1]; seg_l = l[max(0, i-14):i+1]; seg_c = c[max(0, i-14):i+1]
    trs = [max(seg_h[k]-seg_l[k], abs(seg_h[k]-seg_c[k-1]), abs(seg_l[k]-seg_c[k-1])) for k in range(1, len(seg_c))]
    return (sum(trs)/len(trs))/c[i] if trs and c[i] > 0 else 0.0


def main():
    # 정책: HOLD / 3%고정 / 8%고정 / ATR스케일(1.5×ATR, 4~15% clamp). 전부 entry=익일종가, max=H
    cols = {"HOLD_H": [], "TR_3%": [], "TR_8%": [], "TR_ATR": []}
    fmaxs = []
    reasons = {k: {"STOP": 0, "TRAIL": 0, "TIME": 0} for k in ("TR_3%", "TR_8%", "TR_ATR")}
    files = sorted(DAILY.glob("*.csv"))
    print(f"종목 {len(files)} | entry=점화익일 종가 | 발동 {ACT:.0%} | H={H} | 변형: 3%고정 / 8%고정 / ATR(1.5x,4~15%)\n")

    for p in files:
        d = _load(p)
        if d is None:
            continue
        o, h, l, c, v = d
        n = len(c)
        last = -COOLDOWN - 1
        for i in range(21, n - H):
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
            if gain >= IGNITE_GAIN and volx >= IGNITE_VOLX and pos20 <= BASE_POS and i >= last + COOLDOWN:
                last = i
                entry = c[i]
                fo, fh, fl, fc = o[i+1:i+1+H], h[i+1:i+1+H], l[i+1:i+1+H], c[i+1:i+1+H]
                fmaxs.append(fh.max() / entry - 1.0)
                cols["HOLD_H"].append(fc[-1] / entry - 1.0)
                atrp = max(0.04, min(0.15, 1.5 * _atr_pct(o, h, l, c, i)))
                for nm, tr in (("TR_3%", 0.03), ("TR_8%", 0.08), ("TR_ATR", atrp)):
                    r, rea = sim_trail(entry, fo, fh, fl, fc, H, tr, tr)
                    cols[nm].append(r); reasons[nm][rea] += 1

    if not fmaxs:
        print("fire 없음"); return 1
    fmaxs = np.array(fmaxs); nf = len(fmaxs)

    def line(nm):
        a = np.array(cols[nm]); m = fmaxs > 0
        cap = a[m] / fmaxs[m]
        return (f"  {nm:<8} avg {a.mean()*100:+6.1f}% | median {np.median(a)*100:+6.1f}% | "
                f"승률 {(a>0).mean()*100:4.1f}% | ≥+10% {(a>=0.10).mean()*100:4.1f}% | "
                f"스파이크수확(중앙) {np.median(cap)*100:4.0f}%")

    print(f"── 점화 {nf:,}건 / 정책별 실현수익 (fwd_max[고가] avg {fmaxs.mean()*100:+.1f}%) ──")
    for nm in cols:
        print(line(nm))
    print()
    for nm in ("TR_3%", "TR_8%", "TR_ATR"):
        r = reasons[nm]
        print(f"  {nm} 청산: STOP {r['STOP']/nf*100:.0f}% / TRAIL {r['TRAIL']/nf*100:.0f}% / TIME {r['TIME']/nf*100:.0f}%")
    print("\n★ 판정: 어떤 stop도 HOLD median(-5.2%)을 충분히 양전환+승률↑ 못하면 = '점화익일종가 진입'이 틀린 것(끝물 추격).")
    print("  다음 = 맥점(눌림) 진입 + 변동성 stop. entry를 점화 다음 눌림에서 잡아야 함(사장님 맥점 철학).")
    print("★ 한계: 일봉 보수적 stop / 갭 시가체결 / 상폐부재 / 수수료·세금 미반영.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
