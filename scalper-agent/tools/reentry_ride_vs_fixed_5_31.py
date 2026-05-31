# -*- coding: utf-8 -*-
"""rolling-ride(추세 살아있으면 계속) vs 고정 D+3 — 정량 비교 (5/31 단타봇 코워크).

사장님 질문: "방향 맞으면 재진입 후 또 D+2~3 이렇게 rolling으로 가면 더 버나?"
RIDE_TRAIL = 트레일(고점-3%)만으로 보유, 재진입 max2, 캘린더 청산 없음(추세 죽을 때까지, 캡 30일).
고정 K = D+K에서 강제 청산. 둘 다 종가매수·종가재진입·비용 0.48%/leg.

핵심 산출: 평균수익 + ★실제 평균보유일★(단타냐 스윙이냐) + 승률 + 끼별 + OOS.
★ 한계: 일봉, 생존편향(긴보유일수록 과대) → 상대비교·OOS·보유일 trade-off로 해석.
"""
from __future__ import annotations
import csv
import glob
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
DAILY = ROOT / "stock_data_daily"
UP_THRESH = 0.10
VOL_X = 2.0
TRAIL = 0.03
COST_LEG = 0.0048
MAX_RE = 2
MAX_HORIZON = 30   # ride 모드 안전 캡
OOS_SPLIT = "2026-01-01"


def _load(path):
    rows, dates = [], []
    try:
        with open(path, encoding="utf-8") as f:
            for r in csv.DictReader(f):
                try:
                    o = float(r["open"]); h = float(r["high"])
                    l = float(r["low"]); c = float(r["close"]); v = float(r["volume"])
                except (ValueError, KeyError):
                    continue
                if o > 0 and c > 0:
                    rows.append((o, h, l, c, v))
                    dates.append(r.get("날짜") or r.get("date") or r.get("") or "")
    except Exception:
        return [], []
    return rows, dates


def _events(rows):
    ev = []
    for i in range(60, len(rows) - 1):
        pc = rows[i - 1][3]
        if pc <= 0:
            continue
        vma = sum(rows[j][4] for j in range(i - 20, i)) / 20
        if vma > 0 and rows[i][3] / pc - 1 >= UP_THRESH and rows[i][4] >= VOL_X * vma:
            ev.append(i)
    return ev


def _kki(rows, e):
    return sum(1 for j in range(max(1, e - 60), e)
               if rows[j - 1][3] > 0 and rows[j][3] / rows[j - 1][3] - 1 >= 0.15)


def _sim(rows, e, mode, k=3):
    """mode='fixed'(D+k 강제청산) | 'ride'(트레일만, 캘린더 없음). 종가매수/재진입, trail+reenter."""
    entry = rows[e][3]
    cap = min(e + (k if mode == "fixed" else MAX_HORIZON), len(rows) - 1)
    days = list(range(e + 1, cap + 1))
    if not days:
        return None
    pnl = 0.0
    in_pos = True
    p_in = entry
    hw = entry
    last_stop = entry * (1 - TRAIL)
    re_used = 0
    exit_idx = e
    for d in days:
        o, h, l, c, v = rows[d]
        if in_pos:
            hw = max(hw, h)
            last_stop = hw * (1 - TRAIL)
            if l <= last_stop:
                pnl += (last_stop / p_in - 1) - COST_LEG
                in_pos = False
                exit_idx = d
        else:
            if re_used < MAX_RE and c >= last_stop:   # reclaim → 재진입
                in_pos = True
                p_in = c
                hw = c
                re_used += 1
                exit_idx = d
        # fixed 모드는 days가 D+k까지라 자동으로 그날 종가청산됨(아래 처리)
    if in_pos:                                          # 캡/D+k까지 살아있으면 종가청산
        pnl += (rows[cap][3] / p_in - 1) - COST_LEG
        exit_idx = cap
    return {"pnl": pnl * 100, "span": exit_idx - e, "re": re_used}


def main() -> int:
    loaded = []
    for f in glob.glob(str(DAILY / "*.csv")):
        rows, dates = _load(f)
        if len(rows) >= 95:
            loaded.append((rows, dates))
    ev = [(rows, e, dates[e], _kki(rows, e)) for rows, dates in loaded for e in _events(rows)]
    print(f"돌파 이벤트 {len(ev):,}건\n")

    def stats(sub, mode, k=3):
        rs = [r for r in (_sim(rows, e, mode, k) for rows, e, _d, _k in sub) if r]
        if not rs:
            return None
        n = len(rs)
        return (sum(r["pnl"] for r in rs) / n,
                sum(1 for r in rs if r["pnl"] > 0) / n * 100,
                sum(r["span"] for r in rs) / n,
                sum(r["re"] for r in rs) / n)

    hi = [x for x in ev if x[3] >= 3]
    print(f'{"전략":<18}{"평균수익":>9}{"승률":>7}{"평균보유일":>10}{"재진입":>8}{"高끼수익":>9}')
    for label, mode, k in [("고정 D+3", "fixed", 3), ("고정 D+5", "fixed", 5),
                           ("고정 D+10", "fixed", 10), ("RIDE_TRAIL(추세끝까지)", "ride", 0)]:
        a = stats(ev, mode, k)
        hk = stats(hi, mode, k)
        print(f'{label:<18}{a[0]:>+8.2f}%{a[1]:>6.0f}%{a[2]:>9.1f}일{a[3]:>7.2f}{hk[0]:>+8.2f}%')

    print("\n[OOS] RIDE_TRAIL vs 고정D+3 (기간분할 견고성):")
    tr = [x for x in ev if x[2] and x[2] < OOS_SPLIT]
    te = [x for x in ev if x[2] and x[2] >= OOS_SPLIT]
    for nm, sub in [(f"train(<{OOS_SPLIT})", tr), (f"test(>={OOS_SPLIT})", te)]:
        rd = stats(sub, "ride")
        fx = stats(sub, "fixed", 3)
        print(f'  {nm:<20} 이벤트{len(sub):>7,}  RIDE {rd[0]:>+6.2f}%(보유{rd[2]:.1f}일)  고정D3 {fx[0]:>+6.2f}%')

    print("\n해석: RIDE 수익 > 고정D3면 'rolling이 더 번다' 입증. 단 ★평균보유일★이 단타(≤3)인지")
    print("      스윙(≥5)인지 = 사장님 스타일 결정. OOS train≈test면 견고.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
