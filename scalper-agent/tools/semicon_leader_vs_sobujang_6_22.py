# -*- coding: utf-8 -*-
"""반도체 대장주 vs 소부장 — 같이·시간순 relay 분석 (6/22 사장님 지시, read-only).

사장님: "대장주 우선이 아니고 대장주랑 소부장이랑 같이 보는 게 안되나?"
가설: 소부장(고베타) 먼저 터지고 → 대장주 나중(relay). 6/12→6/19만 보면 소부장이 패자처럼 보이나
      그 전(6/4~)에 이미 갔다가 끝물이었을 수 있음. 둘을 시간순으로 같이 본다.
read-only(daily csv). 매수/매도/주문 무접촉.
"""
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DAILY = ROOT / "data_store" / "daily"
UNIV = json.loads((ROOT / "data_store" / "universe.json").read_text(encoding="utf-8"))

대장주 = {"005930": "삼성전자", "000660": "SK하이닉스", "402340": "SK스퀘어", "009150": "삼성전기"}
소부장 = {"042700": "한미반도체", "357780": "솔브레인", "240810": "원익IPS", "095340": "ISC",
        "058470": "리노공업", "005290": "동진쎄미켐", "064760": "티씨케이", "036930": "주성엔지니어링",
        "000990": "DB하이텍", "067310": "하나마이크론"}


def load_daily(code):
    f = DAILY / f"{code}.csv"
    if not f.exists():
        return {}
    out = {}
    for ln in f.read_text(encoding="utf-8").splitlines():
        p = ln.split(",")
        if len(p) < 6 or not p[0][:4].isdigit():
            continue
        try:
            out[p[0]] = float(p[4])   # date → close
        except Exception:
            pass
    return out


DATES = ["2026-06-02", "2026-06-04", "2026-06-05", "2026-06-08", "2026-06-09",
         "2026-06-10", "2026-06-11", "2026-06-12", "2026-06-16", "2026-06-17",
         "2026-06-18", "2026-06-19"]


def daily_chg(closes, d_prev, d):
    if d_prev in closes and d in closes and closes[d_prev] > 0:
        return (closes[d] / closes[d_prev] - 1) * 100
    return None


def group_series(group):
    """그룹 평균 일간 등락률 시계열 + 6/2 기준 누적."""
    series = {c: load_daily(c) for c in group}
    daily_avg = {}
    for k in range(1, len(DATES)):
        dp, d = DATES[k - 1], DATES[k]
        vals = [daily_chg(series[c], dp, d) for c in group]
        vals = [v for v in vals if v is not None]
        daily_avg[d] = sum(vals) / len(vals) if vals else None
    # 6/2 기준 누적
    cum = {}
    base = DATES[0]
    for d in DATES[1:]:
        rs = []
        for c in group:
            cl = series[c]
            if base in cl and d in cl and cl[base] > 0:
                rs.append((cl[d] / cl[base] - 1) * 100)
        cum[d] = sum(rs) / len(rs) if rs else None
    return daily_avg, cum


def main():
    print("=" * 78)
    print("반도체 대장주 vs 소부장 — 일간 등락률(그룹평균) + 6/2 기준 누적")
    print("=" * 78)
    da_d, da_c = group_series(대장주)
    so_d, so_c = group_series(소부장)
    print(f"\n{'날짜':12s} | {'대장주 일간':>10s} {'대장주 누적':>10s} | {'소부장 일간':>10s} {'소부장 누적':>10s} | 누가앞서나")
    for d in DATES[1:]:
        ld, lc = da_d.get(d), da_c.get(d)
        sd, sc = so_d.get(d), so_c.get(d)
        lead = ""
        if lc is not None and sc is not None:
            lead = "소부장▲" if sc > lc else "대장주▲"
        def f(x): return f"{x:+7.2f}%" if x is not None else "   n/a "
        print(f"{d:12s} | {f(ld):>10s} {f(lc):>10s} | {f(sd):>10s} {f(sc):>10s} | {lead}")

    # 개별 종목 — 6/4 초입(소부장) vs 6/12 초입(대장주) 검증
    print(f"\n[개별 — 6/4→6/9 (소부장 초입 의심) vs 6/12→6/19 (대장주 초입)]")
    series_all = {**{c: (n, load_daily(c)) for c, n in 대장주.items()},
                  **{c: (n, load_daily(c)) for c, n in 소부장.items()}}
    def ret(cl, a, b):
        return (cl[b] / cl[a] - 1) * 100 if a in cl and b in cl and cl[a] > 0 else None
    print(f"  {'종목':14s} {'구분':6s} | {'6/4→6/9':>9s} | {'6/12→6/19':>9s}")
    for c, (n, cl) in series_all.items():
        grp = "대장주" if c in 대장주 else "소부장"
        r1 = ret(cl, "2026-06-04", "2026-06-09")
        r2 = ret(cl, "2026-06-12", "2026-06-19")
        def f(x): return f"{x:+7.2f}%" if x is not None else "   n/a "
        print(f"  {n:14s} {grp:6s} | {f(r1):>9s} | {f(r2):>9s}")
    print("\n★ read-only. 매수/매도/주문 무접촉. daily csv(6/19까지) 실측.")


if __name__ == "__main__":
    main()
