# -*- coding: utf-8 -*-
"""분봉 털기 스터디 #1b — 손절 메커니즘 비교 (5/31 단타봇 코워크).

일봉 스터디는 '장중터치 vs 종가보유'만 봤지만, 실제 제안(VWAP/N분확정)은 종가 전에
자른다 → 분봉 경로로만 검증 가능. data_store/1min(1668종목, 2026-02-26~03-16, 8일).

entry = 당일 첫 봉 open(09시대 시가 매수 가정, 부분일 제외). 손절선 = entry×0.97(-3%).
'장중 -3% 터치한 날'만 비교 대상(=손절이 발동되는 날).

손절 메커니즘 5종 (터치일 평균 손익 %p 비교):
  M0 wick_touch  : 첫 분봉 LOW<=-3% 즉시 손절 → -3.0% 실현 (현 봇, 꼬리에도 반응)
  M1 close_1bar  : 첫 분봉 CLOSE<=-3% 손절 → 그 종가 (순간 꼬리 무시)
  M2 confirm_3min: CLOSE<=-3% 3분 연속 → 3번째 종가 (확정 손절)
  M3 vwap        : 터치 후 첫 'CLOSE<VWAP' 분봉 종가 (모멘텀 이탈 = 진짜 붕괴)
  M4 hold_close  : 당일 종가까지 보유
M1~M3가 M0(-3%)보다 평균 높으면 = 스마트 exit이 흔들기 회복을 살림 → 도입 가치.
★ 한계: 8일·생존종목·체결슬리피지 0가정(M0 -3%는 낙관). 상대비교로 해석.
"""
from __future__ import annotations
import csv
import glob
import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
MIN1 = ROOT / "data_store" / "1min"
STOP_PCT = 3.0
CONFIRM_N = 3


def _day_groups(path):
    """csv → {date: [(t, o,h,l,c,v)...]} (시간순)."""
    days = defaultdict(list)
    try:
        with open(path, encoding="utf-8") as f:
            for row in csv.DictReader(f):
                dt = row.get("datetime", "")
                if len(dt) < 16:
                    continue
                try:
                    o = float(row["open"]); h = float(row["high"])
                    l = float(row["low"]); c = float(row["close"]); v = float(row["volume"])
                except (ValueError, KeyError):
                    continue
                if o <= 0 or c <= 0:
                    continue
                days[dt[:10]].append((dt[11:16], o, h, l, c, v))
    except Exception:
        return {}
    return days


def _simulate(bars):
    """하루 분봉 → entry, 각 메커니즘 결과(% from entry) 또는 None(터치 안함)."""
    bars = sorted(bars, key=lambda x: x[0])
    if bars[0][0] > "09:10":      # 부분일(시초가 누락) 제외
        return None
    entry = bars[0][1]
    if entry <= 0:
        return None
    stop = entry * (1 - STOP_PCT / 100)
    day_close = bars[-1][4]
    rng = (max(b[2] for b in bars) - min(b[3] for b in bars)) / entry * 100

    # 장중 -3% 터치(LOW)?
    touch_i = next((i for i, b in enumerate(bars) if b[3] <= stop), None)
    if touch_i is None:
        return None  # 손절 미발동 = 비교 대상 아님

    def pct(p):
        return (p / entry - 1) * 100

    # M0: 꼬리 터치 즉시 = -3% 실현
    m0 = -STOP_PCT
    # M1: 첫 CLOSE<=-3%
    j = next((i for i, b in enumerate(bars) if b[4] <= stop), None)
    m1 = pct(bars[j][4]) if j is not None else pct(day_close)
    # M2: CLOSE<=-3% 3분 연속 → 3번째 종가
    run = 0; m2 = None
    for b in bars:
        run = run + 1 if b[4] <= stop else 0
        if run >= CONFIRM_N:
            m2 = pct(b[4]); break
    if m2 is None:
        m2 = pct(day_close)
    # M3: 터치 후 첫 CLOSE<VWAP (모멘텀 이탈)
    cum_pv = cum_v = 0.0; m3 = None
    for i, b in enumerate(bars):
        typ = (b[2] + b[3] + b[4]) / 3
        cum_pv += typ * b[5]; cum_v += b[5]
        vwap = cum_pv / cum_v if cum_v > 0 else b[4]
        if i >= touch_i and b[4] < vwap:
            m3 = pct(b[4]); break
    if m3 is None:
        m3 = pct(day_close)
    # M4: 종가 보유
    m4 = pct(day_close)
    return {"rng": rng, "m0": m0, "m1": m1, "m2": m2, "m3": m3, "m4": m4,
            "recovered": day_close > stop}


def main() -> int:
    files = glob.glob(str(MIN1 / "*.csv"))
    agg = []
    for f in files:
        for _date, bars in _day_groups(f).items():
            if len(bars) < 30:
                continue
            r = _simulate(bars)
            if r:
                agg.append(r)
    n = len(agg)
    if n == 0:
        print("터치 표본 0 — 데이터 확인 필요")
        return 1

    def report(rows, label):
        m = len(rows)
        if m == 0:
            print(f"=== {label}: 표본 0 ==="); return
        rec = sum(1 for r in rows if r["recovered"])
        print(f"=== {label} (터치 종목일 {m:,}, 종가회복 {rec/m*100:.0f}%) ===")
        for key, name in [("m0", "M0 꼬리터치(현봇,-3%)"), ("m1", "M1 종가1봉확정"),
                          ("m2", "M2 3분확정"), ("m3", "M3 VWAP이탈"), ("m4", "M4 종가보유")]:
            vals = [r[key] for r in rows]
            avg = sum(vals) / m
            wins = sum(1 for v in vals if v > -STOP_PCT)   # -3%보다 나은 결과 비율
            print(f"  {name:<20} 평균 {avg:>+6.2f}%   (-3%보다 나은날 {wins/m*100:>3.0f}%)")
        print()

    report(agg, "전체 터치일")
    report([r for r in agg if r["rng"] >= 10], "고변동 터치일(당일변동≥10% — 세력/모멘텀)")
    report([r for r in agg if r["rng"] >= 15], "초고변동 터치일(≥15% — 급등주)")
    print("해석: M1~M3 평균이 M0(-3%)보다 높으면 스마트 exit이 흔들기 회복을 살림.")
    print("      특히 고변동(나무기술류)에서 어느 메커니즘이 이기는지가 핵심 판정.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
