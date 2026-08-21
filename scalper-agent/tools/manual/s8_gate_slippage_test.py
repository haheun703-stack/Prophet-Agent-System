# -*- coding: utf-8 -*-
"""[S-8] 등재 완성 — ①BROAD_DOWN 게이트 결합 ②슬리피지 현실성 실측."""
import csv, sys, os, statistics as st
from collections import defaultdict
sys.path.insert(0, os.getcwd())
from data.data_verifier import DAILY_DIR, _all_universe_codes

STORE = DAILY_DIR.parent
CAP, COST, NOTIONAL = 5, 0.2, 300_000

bars, idx, inv = {}, {}, {}
for c in _all_universe_codes():
    p = DAILY_DIR / f"{c}.csv"
    if not p.exists():
        continue
    rows = []
    with open(p, encoding="utf-8") as f:
        for r in csv.DictReader(f):
            try:
                rows.append((r["날짜"][:10], float(r["시가"]), float(r["고가"]),
                             float(r["저가"]), float(r["종가"]), float(r.get("거래량") or 0)))
            except Exception:
                pass
    if len(rows) < 30:
        continue
    bars[c] = rows; idx[c] = {d: i for i, (d, *_) in enumerate(rows)}
    m = {}
    fp = STORE / "flow" / f"{c}_investor.csv"
    if fp.exists():
        with open(fp, encoding="utf-8") as f:
            for r in csv.DictReader(f):
                if r.get("date"):
                    m[r["date"][:10]] = r
    inv[c] = m
print(f"로드 {len(bars)}종")

def _n(row, k):
    try:
        v = row.get(k); return float(v) if v not in (None, "") else None
    except Exception: return None

def streak(c, d, key, n, sign):
    i = idx[c].get(d)
    if i is None or i < n: return False
    for j in range(n):
        v = _n(inv[c].get(bars[c][i-j][0]) or {}, key)
        if v is None or v*sign <= 0: return False
    return True

def fwd(c, d, k):
    i = idx[c].get(d)
    if i is None or i+k >= len(bars[c]): return None
    e = bars[c][i+1][1]
    return (bars[c][i+k][4]-e)/e*100 if e > 0 else None

ref = max(bars.values(), key=len)
days = [b[0] for b in ref[-106:-6]]

# ── breadth (그날 상승 종목 비율) — 7/4 규약: D-1 breadth <= 0.45 = BROAD_DOWN
breadth = {}
for d in days:
    up = tot = 0
    for c in bars:
        i = idx[c].get(d)
        if i is None or i < 1: continue
        if bars[c][i][4] > bars[c][i-1][4]: up += 1
        tot += 1
    if tot: breadth[d] = up/tot
print(f"breadth 산출 {len(breadth)}일 · 중앙 {st.median(breadth.values()):.3f} · "
      f"<=0.45 인 날 {sum(1 for v in breadth.values() if v<=0.45)}일")

# ── 신호 수집 (거래대금 포함)
sig = {}
for d in days:
    sel = []
    for c in bars:
        try:
            if streak(c, d, "외국인_수량", 3, 1) and streak(c, d, "기관_수량", 3, -1):
                v = fwd(c, d, 3)
                if v is None: continue
                i = idx[c][d]
                amt = bars[c][i][5]*bars[c][i][4]     # 거래대금
                sel.append((c, v, amt))
        except Exception: pass
    if sel: sig[d] = sel

mkt = {}
for d in days:
    vs = [fwd(c, d, 3) for c in bars]; vs = [v for v in vs if v is not None]
    if vs: mkt[d] = st.mean(vs)

def run(label, gate_fn, pickkey, amt_min=0):
    lifts, nets, n, dd = [], [], 0, 0
    for d, sel in sig.items():
        if d not in mkt or not gate_fn(d): continue
        pool = [x for x in sel if x[2] >= amt_min]
        if not pool: continue
        pick = sorted(pool, key=pickkey)[:CAP]
        rets = [x[1] for x in pick]
        lifts.append(st.mean(rets)-mkt[d]); nets += [r-COST for r in rets]
        n += len(pick); dd += 1
    if not n: print(f"  {label:38s} 표본 0"); return
    sd = st.pstdev(lifts) if len(lifts) > 1 else 0
    t = st.mean(lifts)/(sd/len(lifts)**0.5) if sd else 0
    tot = sum(nets)
    print(f"  {label:38s} {dd:3d}일 {n:4d}건 · 리프트 {st.mean(lifts):+6.2f}%p(t {t:+5.2f}) · "
          f"순합 {tot:+8.2f}%p · 건당 {tot/n:+6.3f}%p · 승률 {sum(1 for x in nets if x>0)/n*100:4.1f}% · "
          f"{int(tot/100*NOTIONAL):+,}원")

no_gate = lambda d: True
def gate_up(d):
    i = days.index(d)
    return i > 0 and breadth.get(days[i-1], 1) > 0.45      # D-1 breadth > 0.45 만 진입

print("\n[게이트 없음] (재현 확인)")
run("코드순", no_gate, lambda x: x[0])
run("거래대금 하위", no_gate, lambda x: x[2])

print("\n[★ BROAD_DOWN 회피 게이트 결합] D-1 breadth > 0.45 인 날만")
run("코드순 + 게이트", gate_up, lambda x: x[0])
run("거래대금 하위 + 게이트", gate_up, lambda x: x[2])
run("거래대금 중위(상하위 배제)+게이트", gate_up, lambda x: abs(x[2]-1e9))

print("\n[★ 슬리피지 현실성] 거래대금 하한 필터 (게이트 적용·하위 우선)")
for lo, nm in [(0, "제한 없음"), (1e8, "1억원 이상"), (5e8, "5억원 이상"), (1e9, "10억원 이상")]:
    run(f"하위·거래대금 {nm}", gate_up, lambda x: x[2], amt_min=lo)

# 실제 거래대금 분포
picked = []
for d, sel in sig.items():
    if d in mkt and gate_up(d):
        picked += [x[2] for x in sorted(sel, key=lambda y: y[2])[:CAP]]
if picked:
    q = sorted(picked)
    print(f"\n거래대금 하위 5건의 실제 분포: 중앙 {q[len(q)//2]/1e8:.2f}억 · "
          f"하위10% {q[len(q)//10]/1e8:.2f}억 · 최소 {q[0]/1e8:.3f}억")
    print(f"→ 1건 {NOTIONAL:,}원 주문이 중앙값 거래대금의 "
          f"{NOTIONAL/q[len(q)//2]*100:.4f}% — 슬리피지 관점")
