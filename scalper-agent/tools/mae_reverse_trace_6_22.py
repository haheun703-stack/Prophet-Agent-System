# -*- coding: utf-8 -*-
"""MAE 역추적 분석 (6/22) — read-only.

발견(6/19): '-3% 안 터질 강한 종목 선정' = d5 +14 vs -12 (26%p) = 거대 엣지.
문제: MAE는 진입 후 값이라 종목 '선정'에는 직접 못 씀.
목표: MAE 깊었던 종목들의 '진입 시점 공통 특징'을 역추적 →
      진입 전(T0 종가)에 거를 수 있는 신호를 찾는다.

★ 안전 불변식 ★
- read-only: paper_learning._load_all_picks()로 ledger 읽기만.
  주문/매수/매도/picks/recommendation/SAJANG 0 접촉. 학습DB도 안 씀(분석만).
- 발견은 '재료'일 뿐. 룰 자동변경 X — 단타봇 제안→사장님 승인→shadow→live.
"""
from __future__ import annotations

import re
import sys
import statistics as st
from collections import defaultdict

sys.path.insert(0, ".")
try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

from data.paper_learning import _load_all_picks, _dedup_by_stock  # noqa: E402

_PB = re.compile(r"pullback\s+(-?\d+\.?\d*)%")
_MA10 = re.compile(r"ma10지지:(True|False)")
_MA20 = re.compile(r"ma20지지:(True|False)")


def _bool_from(rx, s):
    m = rx.search(s or "")
    return (m.group(1) == "True") if m else None


def feat(c: dict) -> dict:
    """한 pick → 진입 전 feature + 결과(MAE/forward) 평탄화."""
    er = c.get("entry_reason") or ""
    ex = c.get("extra") or {}
    sd = ex.get("sector_detail") or {}
    pb = _PB.search(er)
    return {
        # --- 결과(종속) ---
        "mae": c.get("MAE"),
        "d5": c.get("forward_d5"),
        "d1": c.get("forward_d1"),
        # --- 진입 전(독립) : entry_reason ---
        "pullback": abs(float(pb.group(1))) if pb else None,
        "ma10": _bool_from(_MA10, er),
        "ma20": _bool_from(_MA20, er),
        # --- 진입 전 : sector_detail ---
        "momentum_5d": sd.get("momentum_5d"),
        "vol_ratio": sd.get("vol_ratio"),
        "sd_breadth": sd.get("breadth"),
        "relay_gap": sd.get("relay_gap"),
        "tier": sd.get("tier"),
        # --- 진입 전 : 과열/위치 ---
        "return_1y": ex.get("return_1y_pct"),
        "overheat_grade": ex.get("overheat_grade"),
        "overheat_warn": ex.get("annual_overheat_warning"),
        "gap_pct": ex.get("gap_pct"),
        "gap_up": ex.get("gap_up"),
        "weekly_state": ex.get("weekly_open_state"),
        "monthly_state": ex.get("monthly_open_state"),
        "half_year_state": ex.get("half_year_open_state"),
        "sync_count": ex.get("sector_peer_sync_count"),
        "sync_state": ex.get("sector_peer_sync_state"),
        "first_float": ex.get("first_float_candle"),
        "float_state": ex.get("float_candle_state"),
        # --- 맥락 ---
        "type": c.get("type"),
        "sector": c.get("sector"),
        "regime": c.get("market_regime"),
        "breadth": c.get("_breadth_state"),
    }


def pearson(xs, ys):
    pts = [(x, y) for x, y in zip(xs, ys) if x is not None and y is not None]
    if len(pts) < 3:
        return None, len(pts)
    xs = [p[0] for p in pts]
    ys = [p[1] for p in pts]
    mx, my = st.mean(xs), st.mean(ys)
    num = sum((x - mx) * (y - my) for x, y in pts)
    den = (sum((x - mx) ** 2 for x in xs) * sum((y - my) ** 2 for y in ys)) ** 0.5
    return (round(num / den, 3) if den else None), len(pts)


def _avg(xs):
    xs = [x for x in xs if x is not None]
    return round(st.mean(xs), 2) if xs else None


def mae_band(m):
    if m <= -7:
        return "깊은손절(≤-7%)"
    if m <= -3:
        return "손절터치(-7~-3%)"
    return "버팀(>-3%)"


def main():
    # 종목 단위 분석이므로 (date,ticker) dedup — B·C 동시선정 2배 가중 제거(⑤-2와 동일)
    rows = [feat(c) for c in _dedup_by_stock(_load_all_picks())]
    rows = [r for r in rows if r["mae"] is not None]
    for r in rows:
        r["mae_band"] = mae_band(r["mae"])
    print(f"총 {len(rows)}표본 (MAE 역추적 — 진입 전 신호로 MAE 깊이 설명 시도)\n")

    conts = ["pullback", "momentum_5d", "vol_ratio", "sd_breadth",
             "relay_gap", "return_1y", "gap_pct", "sync_count"]

    # [1] 연속 feature ↔ MAE 상관 (음수 corr = feature 클수록 MAE 깊다)
    print("=== [1] 진입 전 연속 feature ↔ MAE 상관 (corr<0 = 클수록 MAE 깊음) ===")
    cor = []
    for f in conts:
        r_, n = pearson([x[f] for x in rows], [x["mae"] for x in rows])
        cor.append((f, r_, n))
    for f, r_, n in sorted(cor, key=lambda t: (t[1] is None, t[1] if t[1] is not None else 0)):
        print(f"  {f:14s} corr={r_}  (n={n})")

    # [2] MAE 밴드별 진입 전 feature 평균
    bands = ["버팀(>-3%)", "손절터치(-7~-3%)", "깊은손절(≤-7%)"]
    print("\n=== [2] MAE 밴드별 진입 전 feature 평균 ===")
    counts = {b: sum(1 for r in rows if r["mae_band"] == b) for b in bands}
    print("  feature        | " + " | ".join(f"{b}(n={counts[b]})" for b in bands))
    for f in conts + ["d5"]:
        cells = [str(_avg([r[f] for r in rows if r["mae_band"] == b])) for b in bands]
        print(f"  {f:14s} | " + " | ".join(f"{c:>16s}" for c in cells))

    # [3] 범주 feature별 MAE 평균 + 깊은손절 비율
    print("\n=== [3] 범주 feature별 MAE 평균 / 깊은손절율 (낮은 MAE 순) ===")
    cats = ["ma20", "ma10", "overheat_grade", "overheat_warn", "gap_up",
            "weekly_state", "monthly_state", "half_year_state", "sync_state",
            "first_float", "float_state", "tier", "regime", "breadth", "type"]
    for f in cats:
        g = defaultdict(list)
        for r in rows:
            g[r[f]].append(r["mae"])
        print(f"\n  [{f}]")
        for k, v in sorted(g.items(), key=lambda kv: st.mean(kv[1])):
            deep = sum(1 for m in v if m <= -7)
            print(f"    {str(k):22s} n={len(v):2d}  MAE평균={round(st.mean(v),2):7.2f}  깊은손절율={round(100*deep/len(v)):3d}%")

    # [4] breadth 층화 (시장 confounder 통제)
    print("\n=== [4] breadth 층화 — 같은 시장 안에서도 MAE 깊은가 (confounder 통제) ===")
    for bs in sorted(set(r["breadth"] for r in rows), key=lambda b: str(b)):
        sub = [r for r in rows if r["breadth"] == bs]
        deep = sum(1 for r in sub if r["mae"] <= -7)
        print(f"  {str(bs):14s} n={len(sub):2d}  MAE평균={round(st.mean([r['mae'] for r in sub]),2):7.2f}  깊은손절율={round(100*deep/len(sub)):3d}%")


if __name__ == "__main__":
    main()
