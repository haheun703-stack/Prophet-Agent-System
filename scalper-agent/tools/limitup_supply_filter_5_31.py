# -*- coding: utf-8 -*-
"""상한가 티어 × 수급(외인/기관) 필터 — 엔진 핵심필터 검증 (사장님 5/31 ②).

병렬세션·내 백테스트는 OHLCV만 → '수급 생존 필터'(엔진 핵심) 미반영.
수급 데이터(data_store/flow/{code}_investor.csv) = 2026-02~05 4개월만 존재.
→ 그 창에서 상한가 STOP_REENTER를 **수급동반 vs 수급없음**으로 갈라 수급필터 값어치 측정.

질문: 상한가 신호에 '외인+기관 동반매수' 필터를 더하면 STOP_REENTER가 더 나아지나?
  = 엔진의 핵심 thesis(상한가+수급생존)가 실데이터로 입증되나.
★ 한계(정직): 4개월 단일창 + 2026 강세장 regime + 작은 표본(특히 3rd+) = 예비 읽기.
  진짜 검증은 6/1 forward shadow(다국면 누적). 상대비교(수급동반 vs 없음)만 신뢰.
동일 STOP_REENTER 틀(고점-3%, 재진입, 비용 0.48%/leg, 보유5일).
"""
from __future__ import annotations
import csv, glob, sys
from pathlib import Path
import numpy as np

ROOT = Path(__file__).resolve().parent.parent
PROJ = ROOT.parent
DAILY = PROJ / "stock_data_daily"
FLOW = ROOT / "data_store" / "flow"
VOL_X = 2.0; TRAIL = 0.03; COST_LEG = 0.0048; HOLD_K = 5
LIMIT = 0.25; PRIOR_WIN = 15
SUP_WIN = 5                      # 수급 누적 윈도우(진입 직전 5일)
SUP_START = "2026-02-04"         # 수급 데이터 시작


def _load_daily(path):
    dates, o, h, l, c, v = [], [], [], [], [], []
    try:
        with open(path, encoding="utf-8") as f:
            for r in csv.DictReader(f):
                try:
                    dt = r.get("날짜") or r.get("date") or r.get("") or ""
                    o.append(float(r["open"])); h.append(float(r["high"])); l.append(float(r["low"]))
                    c.append(float(r["close"])); v.append(float(r["volume"])); dates.append(dt)
                except (ValueError, KeyError):
                    continue
    except Exception:
        return None
    if len(c) < 70:
        return None
    return dates, np.array(o), np.array(h), np.array(l), np.array(c), np.array(v)


def _load_flow(code):
    p = FLOW / f"{code}_investor.csv"
    if not p.exists():
        return {}
    m = {}
    try:
        with open(p, encoding="utf-8") as f:
            for r in csv.DictReader(f):
                try:
                    m[r["date"]] = (float(r.get("외국인_금액", 0) or 0), float(r.get("기관_금액", 0) or 0))
                except (ValueError, KeyError):
                    continue
    except Exception:
        return {}
    return m


def _sim(o, h, l, c, e, hold_k=HOLD_K):
    entry = c[e]; end = min(e + hold_k, len(c) - 1)
    days = list(range(e + 1, end + 1))
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
    # buckets: tier → {dual:[], nodual:[]}  (dual=외인&기관 동반매수)
    B = {t: {"dual": [], "single": [], "none": []} for t in ("GENERIC", "NEAR_LIMIT", "LIMIT_3RD+")}
    files = glob.glob(str(DAILY / "*.csv"))
    n_ev = 0
    for f in files:
        code = Path(f).stem.rsplit("_", 1)[-1].zfill(6) if "_" in Path(f).stem else Path(f).stem
        d = _load_daily(f)
        if d is None:
            continue
        dates, o, h, l, c, v = d
        flow = _load_flow(code)
        if not flow:
            continue
        n = len(c)
        for i in range(60, n - HOLD_K):
            if dates[i] < SUP_START:           # 수급 데이터 있는 구간만
                continue
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
            # 수급: 진입 직전 SUP_WIN일 외인/기관 누적
            fnet = inet = 0.0; got = 0
            for k in range(i - SUP_WIN + 1, i + 1):
                if 0 <= k < len(dates) and dates[k] in flow:
                    fnet += flow[dates[k]][0]; inet += flow[dates[k]][1]; got += 1
            if got < 2:
                continue
            kind = "dual" if (fnet > 0 and inet > 0) else ("single" if (fnet > 0 or inet > 0) else "none")
            r = _sim(o, h, l, c, i)
            if r is None:
                continue
            n_ev += 1
            for t in tiers:
                B[t][kind].append(r)

    print(f"수급창 {SUP_START}~2026-05 / 상한가 이벤트 {n_ev:,}건 (수급동반=외인&기관 둘다 매수)")
    print("★ 예비: 4개월·강세장·작은표본 → 상대비교(동반 vs 없음)만. 진짜검증=6/1 forward shadow\n")
    print(f'{"티어":<13}{"수급":<8}{"이벤트":>8}{"STOP_REENTER":>15}{"median":>10}{"승률":>7}')
    for t in ("GENERIC", "NEAR_LIMIT", "LIMIT_3RD+"):
        for kind, lbl in (("dual", "동반매수"), ("single", "한쪽"), ("none", "수급없음")):
            seg = B[t][kind]
            if not seg:
                print(f'{t:<13}{lbl:<8}{0:>8}'); continue
            a = np.array(seg)
            print(f'{t:<13}{lbl:<8}{len(seg):>8}{a.mean():>+14.2f}%{np.median(a):>+9.2f}%{(a>0).mean()*100:>6.0f}%')
        print()
    print("★ 판정: 같은 티어에서 '동반매수' > '수급없음'이면 = 수급필터가 신호를 날카롭게(엔진 thesis 입증).")
    print("  표본 작고 강세장이라 절대수치 X, 동반>없음 방향만. 6/1 shadow가 다국면 본검증.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
