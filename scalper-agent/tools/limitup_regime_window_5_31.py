# -*- coding: utf-8 -*-
"""상한가 STOP_REENTER — regime(연도/장세) 분리 검증 (사장님 5/31 "10년 통짜 금지").

사장님 통찰: 코스피 오른지 1년 = 아무도 경험못한 초강세장. 10년 통짜 백테스트(tier_reentry
  +14~26%)는 약세·횡보·강세 혼합 → 현재 장 대표 못함. 단 1년/6개월만 보면 강세장 생존편향
  ("뭘 사도 오름" = 전략이 좋은 건지 장이 좋은 건지 구분 불가).
→ 해법: regime 명시 분리. 연도별/윈도우별 STOP_REENTER + 그 해 시장강도(전종목 median) 대조.
  약세장(2022 등)에 무너지고 강세장(2024~25)에만 좋으면 = 강세장 전략(우려 입증). 비슷하면 robust.
  과거 강세장 구간 = 현재 초강세장의 out-of-sample(1년만 보는 것보다 표본↑+regime 일치).

동일 STOP_REENTER 틀(고점-3%, 재진입, 비용 0.48%/leg, 보유5일).
★ 한계: 일봉/생존편향(절대 과대, 상대만)/regime proxy=전종목 median/수급필터 미반영.
  regime proxy는 지수(KODEX200=2년치뿐, 데이터 신뢰도 의심) 대신 전종목 median 채택.
"""
from __future__ import annotations
import csv, glob
from pathlib import Path
import numpy as np

ROOT = Path(__file__).resolve().parent.parent.parent
DAILY = ROOT / "stock_data_daily"
VOL_X = 2.0; TRAIL = 0.03; COST_LEG = 0.0048; HOLD_K = 5; LIMIT = 0.25; PRIOR_WIN = 15
WINDOWS = [("6M", "2025-12-01"), ("1Y", "2025-06-01"), ("2Y", "2024-06-01"), ("전체", "0000")]
TIERS = ("GENERIC", "NEAR_LIMIT", "LIMIT_3RD+")


def _load(path):
    dates, o, h, l, c, v = [], [], [], [], [], []
    try:
        with open(path, encoding="utf-8") as f:
            for r in csv.DictReader(f):
                try:
                    dt = r.get("") or r.get("날짜") or r.get("date") or ""
                    o.append(float(r["open"])); h.append(float(r["high"])); l.append(float(r["low"]))
                    c.append(float(r["close"])); v.append(float(r["volume"])); dates.append(dt)
                except (ValueError, KeyError):
                    continue
    except Exception:
        return None
    if len(c) < 70:
        return None
    return dates, np.array(o), np.array(h), np.array(l), np.array(c), np.array(v)


def _sim(h, l, c, e):
    entry = c[e]; end = min(e + HOLD_K, len(c) - 1); days = list(range(e + 1, end + 1))
    if not days:
        return None
    legs = 0.0; in_pos = True; p_in = entry; hw = entry; last_stop = entry * (1 - TRAIL)
    for d in days:
        if in_pos:
            hw = max(hw, h[d]); stop = hw * (1 - TRAIL); last_stop = stop
            if l[d] <= stop:
                legs += (stop / p_in - 1) - COST_LEG; in_pos = False
        else:
            if c[d] >= last_stop:
                in_pos = True; p_in = c[d]; hw = c[d]
    if in_pos:
        legs += (c[end] / p_in - 1) - COST_LEG
    return legs * 100


def main():
    EV = []
    YR_MKT = {}                       # year -> 종목별 연수익 list (regime 강도)
    files = glob.glob(str(DAILY / "*.csv"))
    for f in files:
        d = _load(f)
        if d is None:
            continue
        dates, o, h, l, c, v = d
        n = len(c)
        yr_first, yr_last = {}, {}
        for k in range(n):
            y = dates[k][:4]
            if not y:
                continue
            yr_first.setdefault(y, c[k]); yr_last[y] = c[k]
        for y, fst in yr_first.items():
            if fst > 0:
                YR_MKT.setdefault(y, []).append(yr_last[y] / fst - 1)
        for i in range(60, n - HOLD_K):
            pc = c[i - 1]
            if pc <= 0:
                continue
            vma = v[i - 20:i].mean()
            if vma <= 0:
                continue
            up = c[i] / pc - 1
            tiers = set()
            if up >= 0.10 and v[i] >= VOL_X * vma:
                tiers.add("GENERIC")
            if up >= LIMIT:
                tiers.add("NEAR_LIMIT")
                prior = sum(1 for j in range(max(1, i - PRIOR_WIN), i)
                            if c[j - 1] > 0 and c[j] / c[j - 1] - 1 >= LIMIT)
                if prior >= 2:
                    tiers.add("LIMIT_3RD+")
            if not tiers:
                continue
            r = _sim(h, l, c, i)
            if r is None:
                continue
            EV.append({"tiers": tiers, "date": dates[i], "year": dates[i][:4], "ret": r})

    print(f"상한가 STOP_REENTER regime 분리 / 이벤트 {len(EV):,}건 / 보유{HOLD_K}일·고점-3%·비용0.48%/leg\n")

    print("【시장 regime — 그 해 전종목 median 수익 (강세장 식별)】")
    yr_regime = {}
    for y in sorted(YR_MKT):
        med = np.median(YR_MKT[y]) * 100
        lab = "BULL" if med >= 10 else ("BEAR" if med <= -5 else "SIDE")
        yr_regime[y] = lab
        bar = "#" * max(0, int(med / 3)) if med > 0 else ""
        print(f"  {y}: median {med:>+6.1f}%  [{lab:<4}] n={len(YR_MKT[y]):>4}  {bar}")

    print("\n【A. 연도별 tier STOP_REENTER (강세장에만 좋은가?)】")
    years = sorted(set(e["year"] for e in EV))
    print(f'  {"연도":<6}{"regime":<7}{"GENERIC":>17}{"NEAR_LIMIT":>17}{"LIMIT_3RD+":>17}')
    for y in years:
        row = f'  {y:<6}{yr_regime.get(y, "?"):<7}'
        for t in TIERS:
            seg = [e["ret"] for e in EV if e["year"] == y and t in e["tiers"]]
            if seg:
                a = np.array(seg); row += f'{f"{a.mean():+.1f}% (n{len(seg)})":>17}'
            else:
                row += f'{"-":>17}'
        print(row)

    print("\n【B. 최근 윈도우 STOP_REENTER (사장님: 1년/6개월로 봐야)】")
    print(f'  {"윈도우":<6}{"GENERIC":>20}{"NEAR_LIMIT":>20}{"LIMIT_3RD+":>20}')
    for wl, wb in WINDOWS:
        row = f'  {wl:<6}'
        for t in TIERS:
            seg = [e["ret"] for e in EV if e["date"] >= wb and t in e["tiers"]]
            if seg:
                a = np.array(seg); row += f'{f"{a.mean():+.1f}%/{(a>0).mean()*100:.0f}% (n{len(seg)})":>20}'
            else:
                row += f'{"-":>20}'
        print(row)

    print("\n★ 판정:")
    print("  · 강세장(BULL) 연도만 +, 약세장(BEAR) - 또는 소멸 → 강세장 전략(사장님 우려 입증).")
    print("  · 전 regime 비슷 → robust(regime 무관).")
    print("  · 최근 6M/1Y가 10년·과거강세장보다 훨씬↑ → 현재 초강세장 과대효과(과신 금지, shadow 필수).")
    print("★ 한계: 일봉/생존편향(상대만)/regime proxy=전종목median/수급필터 미반영.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
