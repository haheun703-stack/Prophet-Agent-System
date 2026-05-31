# -*- coding: utf-8 -*-
"""상한가×수급 인버전 — 교란 통제 검증 (사장님 5/31 "수급 인버전 파보기").

배경: limitup_supply_filter(8ae69ef)에서 전 티어 '수급없음 > 동반매수' 인버전 발견.
의심: 진짜 수급 신호인가, 아니면 표본 구성 교란인가?
  외인+기관은 대형주에 들어옴 → dual(동반매수)=대형주=덜 튐 / none(수급없음)=소형/고변동=더 튐.
  그럼 인버전은 "수급 무용"이 아니라 "소형·고변동이 더 튄다"는 다른 얘기일 뿐.

검증(RC-5, 발견을 룰로 쓰기 전 한 번 더 못 믿기):
  A. 헤드라인 재현 (8ae69ef와 같은 숫자 나오나 = sanity)
  B. 교란변수 분포 — none 그룹이 체계적으로 소형/고변동/저가/저유동에 쏠렸나 (median 비교)
  C. 시총 3분위 stratify — 같은 시총 구간 안에서도 none > dual 유지되나
  D. 변동성(ATR) 3분위 stratify — 같은 변동성 구간 안에서도 none > dual 유지되나
  → 구간 안에서도 인버전 유지 = 진짜 수급 신호 / 사라지면 = 교란이었다.

동일 STOP_REENTER 틀(고점-3%, 재진입, 비용 0.48%/leg, 보유5일). 수급창 2026-02~05 4개월.
★ 정직: 4개월·강세장·proxy 한계 동일. 여기선 '인버전이 시총/변동성 교란인가'만 가린다.
"""
from __future__ import annotations
import csv, glob, json
from pathlib import Path
import numpy as np

ROOT = Path(__file__).resolve().parent.parent
PROJ = ROOT.parent
DAILY = PROJ / "stock_data_daily"
FLOW = ROOT / "data_store" / "flow"
UNI = ROOT / "data_store" / "universe.json"
VOL_X = 2.0; TRAIL = 0.03; COST_LEG = 0.0048; HOLD_K = 5
LIMIT = 0.25; PRIOR_WIN = 15
SUP_WIN = 5
SUP_START = "2026-02-04"


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


def _atr_pct(h, l, c, i):
    lo = max(1, i - 13)
    trs = [max(h[k] - l[k], abs(h[k] - c[k - 1]), abs(l[k] - c[k - 1])) for k in range(lo, i + 1)]
    return (sum(trs) / len(trs)) / c[i] if trs and c[i] > 0 else 0.0


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


def _tercile_labels(vals):
    """vals(np.array) → 각 원소의 분위 라벨 array(0=하,1=중,2=상). 경계는 33/67 퍼센타일."""
    if len(vals) < 6:
        return None, None
    q1, q2 = np.percentile(vals, [33.33, 66.67])
    lab = np.where(vals <= q1, 0, np.where(vals <= q2, 1, 2))
    return lab, (q1, q2)


def main():
    cap_map = {}
    try:
        u = json.load(open(UNI, encoding="utf-8"))
        for code, info in u.items():
            cap = info.get("cap_억") or info.get("cap_억")
            if cap:
                cap_map[code] = float(cap)
    except Exception as e:
        print("universe 로드 실패:", e); return 1

    # 이벤트 수집: 각 dict(tiers:set, kind, ret, cap, atr, turn, price)
    EV = []
    files = glob.glob(str(DAILY / "*.csv"))
    for f in files:
        stem = Path(f).stem
        code = stem.rsplit("_", 1)[-1].zfill(6) if "_" in stem else stem.zfill(6)
        if code not in cap_map:                # 시총 있는 종목만(교란 통제 모집단)
            continue
        d = _load_daily(f)
        if d is None:
            continue
        dates, o, h, l, c, v = d
        flow = _load_flow(code)
        if not flow:
            continue
        cap = cap_map[code]
        n = len(c)
        for i in range(60, n - HOLD_K):
            if dates[i] < SUP_START:
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
            EV.append({"tiers": tiers, "kind": kind, "ret": r, "cap": cap,
                       "atr": _atr_pct(h, l, c, i), "turn": c[i] * v[i] / 1e8, "price": c[i]})

    print(f"교란통제 모집단(시총+수급 보유) 상한가 이벤트 {len(EV):,}건 / 수급창 {SUP_START}~2026-05\n")

    for T in ("GENERIC", "NEAR_LIMIT", "LIMIT_3RD+"):
        evs = [e for e in EV if T in e["tiers"]]
        if not evs:
            continue
        print("=" * 78)
        print(f"[{T}]  이벤트 {len(evs):,}건")
        # ── A. 헤드라인 재현 ──
        print("  A. 헤드라인(8ae69ef 재현):")
        for kind, lbl in (("dual", "동반매수"), ("single", "한쪽"), ("none", "수급없음")):
            seg = [e["ret"] for e in evs if e["kind"] == kind]
            if seg:
                a = np.array(seg)
                print(f"     {lbl:<6} n={len(seg):>5}  STOP_REENTER {a.mean():>+7.2f}%  승률 {(a>0).mean()*100:>3.0f}%")
            else:
                print(f"     {lbl:<6} n=    0")
        # ── B. 교란변수 분포 (none이 체계적으로 다른가) ──
        print("  B. 교란변수 median (none이 소형/고변동/저가/저유동에 쏠렸나):")
        print(f"     {'':6} {'시총억':>10} {'ATR%':>7} {'거래대금억':>10} {'가격':>9}")
        for kind, lbl in (("dual", "동반매수"), ("none", "수급없음")):
            seg = [e for e in evs if e["kind"] == kind]
            if seg:
                print(f"     {lbl:<6} {np.median([e['cap'] for e in seg]):>10,.0f} "
                      f"{np.median([e['atr'] for e in seg])*100:>6.1f}% "
                      f"{np.median([e['turn'] for e in seg]):>10,.0f} "
                      f"{np.median([e['price'] for e in seg]):>9,.0f}")
        # ── C/D. stratify (큰 표본 티어만) ──
        if len(evs) >= 60:
            for var, vlbl in (("cap", "시총"), ("atr", "변동성ATR")):
                vals = np.array([e[var] for e in evs])
                lab, bounds = _tercile_labels(vals)
                if lab is None:
                    continue
                _d = (lambda x: f"{x*100:.1f}%") if var == "atr" else (lambda x: f"{x:,.0f}억")
                print(f"  {'C' if var=='cap' else 'D'}. {vlbl} 3분위 stratify "
                      f"(경계 {_d(bounds[0])}/{_d(bounds[1])}) - 구간 안에서도 none>dual 유지되나:")
                print(f"     {'분위':<5}{'dual(동반) n/평균':>22}{'none(수급없음) n/평균':>24}{'차이(none-dual)':>16}")
                for b, blbl in ((0, "하"), (1, "중"), (2, "상")):
                    dl = [e["ret"] for e, L in zip(evs, lab) if L == b and e["kind"] == "dual"]
                    nn = [e["ret"] for e, L in zip(evs, lab) if L == b and e["kind"] == "none"]
                    dm = np.mean(dl) if dl else float("nan")
                    nm = np.mean(nn) if nn else float("nan")
                    diff = (nm - dm) if (dl and nn) else float("nan")
                    print(f"     {blbl:<5}{f'{len(dl)} / {dm:+.2f}%':>22}{f'{len(nn)} / {nm:+.2f}%':>24}"
                          f"{f'{diff:+.2f}%p' if diff==diff else '  (표본부족)':>16}")
        else:
            print("  C/D. 표본<60 → stratify 생략(절대 결론 금지, 노이즈)")
        print()

    print("=" * 78)
    print("★ 판정:")
    print("  · 시총·변동성 3분위 '모든 구간'에서 none>dual(차이 +) 유지 → 진짜 수급 인버전(교란 아님).")
    print("  · 특정 구간(예: 소형주)만 none>dual, 대형구간은 dual≈none → 인버전은 '소형/고변동 효과' 교란.")
    print("  · B에서 none이 시총↓·ATR↑·가격↓로 크게 쏠려있으면 교란 강력 의심 → C/D가 최종 판정.")
    print("  정직: 4개월·강세장·proxy 한계 동일. 본검증은 6/1 forward shadow.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
