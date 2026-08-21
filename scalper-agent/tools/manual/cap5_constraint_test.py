# -*- coding: utf-8 -*-
"""[T-1] 등재 전 최종 관문 — 상한 5건/일 제약을 통과하는가.

★ S-1을 죽인 것이 바로 이것이다(7/30 4층 규약): 전 신호 평균 +0.173%p가
  '하루 5건 고르기'를 통과하자 -0.609%p가 됐다. 선착순 역선택.
★ 구조적 차이 가설: 이 축은 **일봉 종가 신호**라 하루 86건이 **동시에** 나온다.
  S-1은 장중 순차 발생이라 '먼저 나온 것'이 곧 '나쁜 것'이었다(선착순 역선택).
  동시 발생이면 그 경로가 원천적으로 없다 — 이를 **실측으로 확인**한다.
"""
import csv, sys, os, statistics as st, random
sys.path.insert(0, os.getcwd())
from data.data_verifier import DAILY_DIR, _all_universe_codes

STORE = DAILY_DIR.parent
CAP = 5
COST = 0.2          # 왕복 비용 %p (S-1 장부와 동일 규약)

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
    bars[c] = rows
    idx[c] = {d: i for i, (d, *_) in enumerate(rows)}
    m = {}
    fp = STORE / "flow" / f"{c}_investor.csv"
    if fp.exists():
        with open(fp, encoding="utf-8") as f:
            for r in csv.DictReader(f):
                if r.get("date"):
                    m[r["date"][:10]] = r
    inv[c] = m
print(f"로드 {len(bars)}종")

def _num(row, k):
    try:
        v = row.get(k)
        return float(v) if v not in (None, "") else None
    except Exception:
        return None

def streak(c, d, key, n, sign):
    i = idx[c].get(d)
    if i is None or i < n:
        return False
    for j in range(n):
        v = _num(inv[c].get(bars[c][i - j][0]) or {}, key)
        if v is None or v * sign <= 0:
            return False
    return True

def fwd(c, d, k):
    i = idx[c].get(d)
    if i is None or i + k >= len(bars[c]):
        return None
    e = bars[c][i + 1][1]
    return (bars[c][i + k][4] - e) / e * 100 if e > 0 else None

def strength(c, d):
    """외인 3일 순매수 수량 합 (선택 순위용 — 클수록 강한 매집)."""
    i = idx[c].get(d)
    tot = 0.0
    for j in range(3):
        v = _num(inv[c].get(bars[c][i - j][0]) or {}, "외국인_수량")
        if v is None:
            return 0.0
        tot += v
    return tot

ref = max(bars.values(), key=len)
days = [b[0] for b in ref[-106:-6]]        # 뒤 6일은 forward 미확정

sig_by_day = {}
for d in days:
    sel = []
    for c in bars:
        try:
            if streak(c, d, "외국인_수량", 3, 1) and streak(c, d, "기관_수량", 3, -1):
                v = fwd(c, d, 3)
                if v is not None:
                    sel.append((c, v, strength(c, d), bars[c][idx[c][d]][5] * bars[c][idx[c][d]][4]))
        except Exception:
            pass
    if sel:
        sig_by_day[d] = sel

mkt = {}
for d in days:
    vs = [fwd(c, d, 3) for c in bars]
    vs = [v for v in vs if v is not None]
    if vs:
        mkt[d] = st.mean(vs)

print(f"신호일 {len(sig_by_day)}일 · 총 {sum(len(v) for v in sig_by_day.values())}건 · "
      f"일평균 {sum(len(v) for v in sig_by_day.values())/len(sig_by_day):.1f}건\n")

def run(name, keyfn, seed=None):
    rnd = random.Random(seed) if seed is not None else None
    lifts, nets, n = [], [], 0
    for d, sel in sig_by_day.items():
        if d not in mkt:
            continue
        if rnd is not None:
            pick = rnd.sample(sel, min(CAP, len(sel)))
        else:
            pick = sorted(sel, key=keyfn)[:CAP]
        rets = [x[1] for x in pick]
        lifts.append(st.mean(rets) - mkt[d])
        nets += [r - COST for r in rets]
        n += len(pick)
    sd = st.pstdev(lifts) if len(lifts) > 1 else 0
    t = st.mean(lifts) / (sd / len(lifts) ** 0.5) if sd else 0
    tot = sum(nets)
    print(f"  {name:26s} {n:4d}건 · 리프트 {st.mean(lifts):+6.2f}%p (t {t:+5.2f}) · "
          f"순합(비용 {COST}) {tot:+8.2f}%p · 건당 {tot/n:+6.3f}%p · 승률 {sum(1 for x in nets if x>0)/n*100:4.1f}%")
    return st.mean(lifts), tot / n

print("[전 신호 — 제약 없음(참고선)]")
alln, alllift = 0, []
allnet = []
for d, sel in sig_by_day.items():
    if d not in mkt:
        continue
    alllift.append(st.mean([x[1] for x in sel]) - mkt[d])
    allnet += [x[1] - COST for x in sel]
    alln += len(sel)
print(f"  {'전체':26s} {alln:4d}건 · 리프트 {st.mean(alllift):+6.2f}%p · "
      f"건당 {sum(allnet)/alln:+6.3f}%p · 승률 {sum(1 for x in allnet if x>0)/alln*100:4.1f}%")

print("\n[상한 5건/일 — 선택 규칙별] ★S-1을 죽인 관문")
run("코드순(임의·중립)", lambda x: x[0])
run("외인 매집강도 상위", lambda x: -x[2])
run("외인 매집강도 하위", lambda x: x[2])
run("거래대금 상위", lambda x: -x[3])
run("거래대금 하위", lambda x: x[3])
for s in (1, 7, 42):
    run(f"무작위(seed={s})", None, seed=s)
