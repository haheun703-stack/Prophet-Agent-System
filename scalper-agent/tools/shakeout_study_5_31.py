# -*- coding: utf-8 -*-
"""털기(shakeout) 측정 — 장중 -X% 터치 후 종가 회복 비율 (5/31 단타봇 코워크 관문#1).

질문(사장님): 나무기술처럼 장중 -6~7% 찍고 회복하는 흔들기를 -3% 손절이 헛매도하나?
측정: 일봉 OHLC, entry=당일 시가(open, 09:15 시가매수 가정).
  low_from_open=(low/open-1)*100, close_from_open=(close/open-1)*100
  터치(-T)=low_from_open<=-T / 털기=터치 AND close>-T(종가회복) / 진짜붕괴=터치 AND close<=-T
세그먼트: 전체 + 고변동(당일 변동폭≥10%) + 초고변동(≥15%, 급등주).
결정근거: '장중 터치 손절(-T)' vs '종가까지 보유' 평균 비교 = 어느 손절 메커니즘이 유리한가.
★ 한계: 일봉 근사(분봉 아님), 생존종목 데이터. 절대수치보다 '장중 vs 종가' 상대비교로 해석.
"""
from __future__ import annotations
import glob
import sys
import warnings
from pathlib import Path

warnings.filterwarnings("ignore")
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent.parent
DAILY = ROOT / "stock_data_daily"
THRESHOLDS = [3.0, 4.5, 6.0]


def main() -> int:
    files = glob.glob(str(DAILY / "*.csv"))
    O, LO, C, RNG = [], [], [], []
    nfiles = 0
    dmin = dmax = ""
    for f in files:
        try:
            df = pd.read_csv(f)
        except Exception:
            continue
        if not {"open", "high", "low", "close"}.issubset(df.columns):
            continue
        o = df["open"].to_numpy(float)
        h = df["high"].to_numpy(float)
        lo = df["low"].to_numpy(float)
        c = df["close"].to_numpy(float)
        m = (o > 0) & (lo > 0) & (c > 0) & (h > 0)
        if not m.any():
            continue
        o, h, lo, c = o[m], h[m], lo[m], c[m]
        nfiles += 1
        O.append(o); LO.append(lo); C.append(c); RNG.append((h - lo) / o * 100)
        try:
            ds = df["날짜"].astype(str)
            lo_d, hi_d = ds.min(), ds.max()
            dmin = lo_d if not dmin else min(dmin, lo_d)
            dmax = hi_d if not dmax else max(dmax, hi_d)
        except Exception:
            pass

    O = np.concatenate(O); LO = np.concatenate(LO); C = np.concatenate(C); RNG = np.concatenate(RNG)
    low_pct = (LO / O - 1) * 100
    close_pct = (C / O - 1) * 100
    N = len(O)
    print(f"종목 {nfiles:,}개 / 종목일 {N:,} / 기간 {dmin}~{dmax}\n")

    def study(mask, label):
        lp = low_pct[mask]; cp = close_pct[mask]; n = len(lp)
        print(f"=== {label} (종목일 {n:,}) ===")
        print(f'{"손절선":>7}{"터치":>9}{"털기(종가회복)":>14}{"털기율":>8}{"진짜붕괴":>9}'
              f'{"장중손절":>9}{"종가보유":>9}{"종가우위":>9}')
        for T in THRESHOLDS:
            touched = lp <= -T
            nt = int(touched.sum())
            if nt == 0:
                print(f'{-T:>6.1f}%{0:>9}')
                continue
            recovered = touched & (cp > -T)
            breakdown = touched & (cp <= -T)
            shake_rate = recovered.sum() / nt * 100
            intraday_result = -T                      # 장중 터치 손절 = -T 실현
            close_result = float(cp[touched].mean())   # 종가까지 보유 = 터치일 종가수익 평균
            edge = close_result - intraday_result      # 종가보유가 장중손절 대비 우위(%p)
            print(f'{-T:>6.1f}%{nt:>9,}{recovered.sum():>14,}{shake_rate:>7.0f}%{breakdown.sum():>9,}'
                  f'{intraday_result:>+8.2f}%{close_result:>+8.2f}%{edge:>+8.2f}%p')
        print()

    study(np.ones(N, bool), "전체 종목일")
    study(RNG >= 10.0, "고변동일 (당일 변동폭≥10% — 세력/모멘텀)")
    study(RNG >= 15.0, "초고변동일 (변동폭≥15% — 급등주/상한가권)")

    print("해석:")
    print(" - 털기율 = 장중 -T 터치한 날 중 종가엔 회복(>-T)한 비율. 높을수록 '장중 터치 손절'이 헛매도.")
    print(" - 종가우위(+%p) = 터치일에 장중 손절(-T) 대신 종가까지 버텼을 때 평균 수익 차. +면 종가/확정손절 유리.")
    print(" - 변동폭 클수록(세력주) 털기율↑ 예상 — 나무기술류에 확정/VWAP 손절이 맞는지의 근거.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
