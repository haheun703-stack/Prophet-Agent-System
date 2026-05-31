# -*- coding: utf-8 -*-
"""상한가 티어 × STOP_REENTER — '상한가 엔진 써먹으면?' 데이터 답 (사장님 5/31).

배경: breakout_reentry_backtest(제네릭 +10%&2×vol)는 STOP_REENTER +5.58%(K5) 검증.
      끼↑→STOP_REENTER↑ 단조(EXPLOSIVE +9.98%). 상한가 엔진=최고끼(3번째+상한가)+수급.
질문: 제네릭 +10%보다 **상한가 티어**(+25% / 3번째+ 상한가)가 STOP_REENTER에서 더 버나?
      = 상한가 엔진의 신호가 (OHLCV로 보이는 부분만으로도) 더 날카로운가.

티어(겹치는 부분집합, 동일 STOP_REENTER 틀·동일 비용 0.48%/leg):
  GENERIC     : 종가상승 ≥10% & 거래량 ≥2×MA20 (병렬 baseline 재현 = sanity)
  NEAR_LIMIT  : 종가상승 ≥25% (상한가/근접 단일일)
  LIMIT_3RD+  : 종가상승 ≥25% AND 직전 15일내 ≥25% 일수 ≥2 (=3번째+ 상한가, 엔진 1층 thesis)
청산: 고점-3% 트레일 손절 → 종가가 직전손절선 회복 시 재진입(반복). 보유 K=5.
★ 한계: 일봉(상한가 close-to-close +25% 프록시, 장중정밀X) / 생존편향(절대수익 과대, 상대비교만) /
  ★★ 엔진의 핵심 '수급 생존' 필터는 OHLCV에 없어 미반영 — 실엔진은 이보다 나아야 정상 ★★ / 비용 0.48%/leg.
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
LIMIT = 0.25       # 상한가/근접 프록시 (종가상승 +25%)
PRIOR_WIN = 15     # 3번째+ 상한가 판정 윈도우


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


def _classify(rows):
    """각 돌파 진입일 i → 속한 티어 set 반환 [(i, tiers)]."""
    n = len(rows)
    out = []
    for i in range(60, n - 1):
        pc = rows[i - 1][3]
        if pc <= 0:
            continue
        up = rows[i][3] / pc - 1
        vma = sum(rows[j][4] for j in range(i - 20, i)) / 20
        if vma <= 0:
            continue
        tiers = set()
        if up >= 0.10 and rows[i][4] >= VOL_X * vma:
            tiers.add("GENERIC")
        if up >= LIMIT:
            tiers.add("NEAR_LIMIT")
            prior = 0
            for j in range(max(1, i - PRIOR_WIN), i):
                ppc = rows[j - 1][3]
                if ppc > 0 and rows[j][3] / ppc - 1 >= LIMIT:
                    prior += 1
            if prior >= 2:
                tiers.add("LIMIT_3RD+")
        if tiers:
            out.append((i, tiers))
    return out


def _sim(rows, e, hold_k=HOLD_K):
    entry = rows[e][3]
    end = min(e + hold_k, len(rows) - 1)
    days = list(range(e + 1, end + 1))
    if not days:
        return None
    hold_ret = (rows[end][3] / entry - 1 - COST_LEG) * 100
    legs = 0.0; in_pos = True; p_in = entry; hw = entry; last_stop = entry * (1 - TRAIL)
    for d in days:
        o, h, l, c, v = rows[d]
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
        legs += (rows[end][3] / p_in - 1) - COST_LEG
    return {"hold": hold_ret, "reenter": legs * 100}


def main():
    files = glob.glob(str(DAILY / "*.csv"))
    tiers = {"GENERIC": [], "NEAR_LIMIT": [], "LIMIT_3RD+": []}
    for f in files:
        rows = _load(f)
        if len(rows) < 70:
            continue
        for i, ts in _classify(rows):
            r = _sim(rows, i)
            if not r:
                continue
            for t in ts:
                tiers[t].append(r)

    print(f"상한가 티어 × STOP_REENTER (보유{HOLD_K}일, 고점-3%, 비용 {COST_LEG*100:.2f}%/leg)")
    print("비교: 제네릭 baseline STOP_REENTER ≈ +5.58%(병렬 검증) / HOLD ≈ 0%\n")
    print(f'{"티어":<14}{"이벤트":>9}{"HOLD":>10}{"STOP_REENTER":>15}{"median":>10}{"승률":>7}')
    for t in ("GENERIC", "NEAR_LIMIT", "LIMIT_3RD+"):
        seg = tiers[t]
        if not seg:
            print(f"{t:<14}{0:>9}"); continue
        re = np.array([r["reenter"] for r in seg]); ho = np.array([r["hold"] for r in seg])
        print(f'{t:<14}{len(seg):>9,}{ho.mean():>+9.2f}%{re.mean():>+14.2f}%{np.median(re):>+9.2f}%{(re>0).mean()*100:>6.0f}%')
    print(f"\n★ 판정: NEAR_LIMIT·LIMIT_3RD+ STOP_REENTER가 GENERIC(+5.6%)보다 높으면 = 상한가 엔진 신호가 더 날카로움(써먹는 게 맞다).")
    print("  + 실엔진은 여기 없는 '수급 생존' 필터로 더 걸러냄 → 실전은 이 숫자보다 나아야 정상.")
    print("★ 한계: 일봉 +25% 프록시 / 생존편향(상대비교만) / 수급필터 미반영 / 비용 0.48%/leg.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
