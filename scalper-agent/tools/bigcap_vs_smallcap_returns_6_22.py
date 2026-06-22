# -*- coding: utf-8 -*-
"""대형주 vs 소형 끼주 수익률 비교 (6/22 사장님 지시 — read-only 분석).

사장님: "SK하이닉스·SK스퀘어·삼성전기 등 대형주까지 보고 뭘 사야 수익금이 좋은지 찾으라."
방법: data_store/daily csv(6/19까지) 실데이터로 최근 구간 forward 수익률을 시총 티어별/종목별 계산.
      단타봇 실제 picks(paper 6/12 B/C)와 대형주 buy&hold 비교. -3% 트레일링 스타일도 적용.
순수 분석(매수/매도/주문 무접촉).
"""
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DAILY = ROOT / "data_store" / "daily"
UNIV = json.loads((ROOT / "data_store" / "universe.json").read_text(encoding="utf-8"))

NAMED_BIGCAPS = {  # 사장님 지목 + 대표 대형주
    "000660": "SK하이닉스", "402340": "SK스퀘어", "009150": "삼성전기",
    "005930": "삼성전자", "373220": "LG에너지솔루션", "207940": "삼성바이오로직스",
    "005380": "현대차", "000270": "기아", "005490": "POSCO홀딩스", "042700": "한미반도체",
    "035420": "NAVER", "035720": "카카오", "012450": "한화에어로스페이스", "105560": "KB금융",
}


def load_daily(code):
    f = DAILY / f"{code}.csv"
    if not f.exists():
        return []
    rows = []
    for ln in f.read_text(encoding="utf-8").splitlines():
        p = ln.split(",")
        if len(p) < 6 or not p[0][:4].isdigit():
            continue
        try:
            rows.append((p[0], float(p[4]), float(p[2]), float(p[3])))  # date, close, high, low
        except Exception:
            continue
    return rows


def window_ret(rows, d_start, d_end):
    """d_start 종가 → d_end 종가 수익률(%) + 구간 MAE(저가 최저 대비 진입). None if 데이터 부족."""
    idx = {r[0]: i for i, r in enumerate(rows)}
    if d_start not in idx or d_end not in idx:
        return None, None
    i0, i1 = idx[d_start], idx[d_end]
    if i1 <= i0:
        return None, None
    c0 = rows[i0][1]
    if c0 <= 0:
        return None, None
    ret = (rows[i1][1] / c0 - 1) * 100
    mae = min((rows[j][3] / c0 - 1) * 100 for j in range(i0 + 1, i1 + 1))  # 진입 후 저가 최저
    return round(ret, 2), round(mae, 2)


def trail_ret(rows, d_start, d_end, trail_pct=3.0):
    """고점 -3% 트레일링 스타일 수익률(단타봇 방식). 진입가 -3% 안전망→+활성후 고점-3%."""
    idx = {r[0]: i for i, r in enumerate(rows)}
    if d_start not in idx or d_end not in idx:
        return None
    i0, i1 = idx[d_start], idx[d_end]
    if i1 <= i0 or rows[i0][1] <= 0:
        return None
    entry = rows[i0][1]
    peak = rows[i0][2]
    for j in range(i0 + 1, i1 + 1):
        gain_peak = (peak / entry - 1) * 100
        if gain_peak >= 3.0:
            sl = peak * (1 - trail_pct / 100)      # 고점 -3%
        else:
            sl = entry * (1 - trail_pct / 100)      # 진입가 -3%
        if rows[j][3] <= sl:                        # 저가가 손절선 이하
            return round((sl / entry - 1) * 100, 2)
        peak = max(peak, rows[j][2])
    return round((rows[i1][1] / entry - 1) * 100, 2)  # 손절 없이 종료 종가


def tier(cap):
    if cap >= 50000: return "대형(5조+)"
    if cap >= 10000: return "중대형(1~5조)"
    if cap >= 3000: return "중형(3천억~1조)"
    return "소형(<3천억)"


def analyze(d_start, d_end, label):
    print(f"\n{'='*70}\n구간 {label}: {d_start} → {d_end}\n{'='*70}")
    by_tier = {}
    all_rows = []
    for code, info in UNIV.items():
        rows = load_daily(code)
        if not rows:
            continue
        ret, mae = window_ret(rows, d_start, d_end)
        if ret is None:
            continue
        tr = trail_ret(rows, d_start, d_end)
        cap = info.get("cap_억", 0)
        t = tier(cap)
        by_tier.setdefault(t, []).append(ret)
        all_rows.append((code, info.get("name", code), cap, t, ret, mae, tr))

    import statistics as st
    print(f"\n[시총 티어별 buy&hold 수익률]  (n / 평균 / 중앙 / 승률%)")
    order = ["대형(5조+)", "중대형(1~5조)", "중형(3천억~1조)", "소형(<3천억)"]
    for t in order:
        v = by_tier.get(t, [])
        if not v:
            continue
        win = sum(1 for x in v if x > 0) / len(v) * 100
        print(f"  {t:14s}: n={len(v):4d} | 평균 {st.mean(v):+6.2f}% | 중앙 {st.median(v):+6.2f}% | 승률 {win:4.0f}%")

    print(f"\n[사장님 지목 대형주 — buy&hold / 트레일링(-3%) / 구간MAE]")
    namedset = {r[0]: r for r in all_rows}
    for code, nm in NAMED_BIGCAPS.items():
        r = namedset.get(code)
        if r:
            print(f"  {nm:18s}({code}): buy&hold {r[4]:+6.2f}% | 트레일링 {str(r[6]):>7}% | MAE {r[5]:+6.2f}% | 시총 {r[2]/10000:.1f}조")
        else:
            print(f"  {nm:18s}({code}): 데이터 없음")

    print(f"\n[전체 상위 10 종목 (buy&hold 수익률) — 어느 티어가 잘 갔나]")
    for code, nm, cap, t, ret, mae, tr in sorted(all_rows, key=lambda x: -x[4])[:10]:
        print(f"  {ret:+7.2f}% | {nm[:14]:14s}({code}) | {t:14s} | MAE {mae:+6.2f}% | 트레일 {tr}")

    # ── 리스크조정: 상폐/도박주(MAE<-40% = 거래정지·반토막) 제외한 '실제로 살 수 있는' 시장 ──
    real = [r for r in all_rows if r[5] is not None and r[5] > -40]
    print(f"\n[리스크조정 — MAE<-40%(상폐·반토막 도박주) 제외 후 티어별]  제외 {len(all_rows)-len(real)}종목")
    rt = {}
    for code, nm, cap, t, ret, mae, tr in real:
        rt.setdefault(t, []).append((ret, tr, mae))
    for t in order:
        v = rt.get(t, [])
        if not v: continue
        rets = [x[0] for x in v]; trs = [x[1] for x in v if x[1] is not None]
        win = sum(1 for x in rets if x > 0)/len(rets)*100
        print(f"  {t:14s}: n={len(v):4d} | buy&hold {st.mean(rets):+6.2f}% | 트레일링 {st.mean(trs):+6.2f}% | 승률 {win:4.0f}%")

    # ── 섹터별(전체시장) — 어느 섹터가 돈 됐나 (n>=8 섹터만) ──
    sec = {}
    for code, info in UNIV.items():
        pass
    secmap = {}
    for code, nm, cap, t, ret, mae, tr in real:
        s = UNIV.get(code, {}).get("sector", "?")
        secmap.setdefault(s, []).append(ret)
    rows_sec = [(s, len(v), st.mean(v), sum(1 for x in v if x>0)/len(v)*100) for s, v in secmap.items() if len(v) >= 8]
    print(f"\n[섹터별 수익률 TOP8 / BOTTOM5 (n>=8, 도박주 제외)]")
    rows_sec.sort(key=lambda x: -x[2])
    for s, n, m, w in rows_sec[:8]:
        print(f"  +{m:6.2f}% | {s[:20]:20s} | n={n:3d} | 승률 {w:3.0f}%")
    print("  ---")
    for s, n, m, w in rows_sec[-5:]:
        print(f"  {m:+7.2f}% | {s[:20]:20s} | n={n:3d} | 승률 {w:3.0f}%")
    return all_rows


def bot_picks_6_12():
    print(f"\n{'='*70}\n[단타봇 실제 paper picks (6/12 B/C) — 같은 구간 6/12→6/19 수익률]\n{'='*70}")
    led = json.loads((ROOT / "data_store" / "paper_3type" / "ledger_2026-06-12.json").read_text(encoding="utf-8"))
    cands = led.get("candidates", {})
    items = []
    if isinstance(cands, dict):
        for v in cands.values():
            items.extend(v)
    else:
        items = cands
    rets = []; trails = []
    for c in items:
        code = str(c.get("ticker") or c.get("code") or "").zfill(6)
        nm = c.get("name", code)
        rows = load_daily(code)
        ret, mae = window_ret(rows, "2026-06-12", "2026-06-19") if rows else (None, None)
        tr = trail_ret(rows, "2026-06-12", "2026-06-19") if rows else None
        cap = UNIV.get(code, {}).get("cap_억", 0)
        if ret is not None:
            rets.append(ret)
            if tr is not None: trails.append(tr)
            print(f"  {ret:+7.2f}% | 트레일 {str(tr):>7} | {nm[:12]:12s}({code}) | 시총 {cap/10000:6.2f}조 | type {c.get('type','?')} | MAE {mae:+.1f}%")
    if rets:
        import statistics as st
        print(f"  → picks buy&hold 평균 {st.mean(rets):+.2f}% / 중앙 {st.median(rets):+.2f}% / 승률 {sum(1 for x in rets if x>0)/len(rets)*100:.0f}% (n={len(rets)})")
        if trails:
            print(f"  → picks 트레일링(-3%) 평균 {st.mean(trails):+.2f}% / 중앙 {st.median(trails):+.2f}% (단타봇 실제 스타일)")


if __name__ == "__main__":
    analyze("2026-06-12", "2026-06-19", "최근(picks forward 창)")
    analyze("2026-06-04", "2026-06-19", "급등구간(2주)")
    bot_picks_6_12()
    print("\n★ read-only 분석. 매수/매도/주문 무접촉. 데이터=data_store/daily(6/19까지) 실측.")
