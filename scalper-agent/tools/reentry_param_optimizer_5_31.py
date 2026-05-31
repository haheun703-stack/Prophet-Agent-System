# -*- coding: utf-8 -*-
"""재진입 규칙 5개 파라미터 데이터 최적화 — #1 설계전 (5/31 단타봇 코워크).

사장님 요청: 재진입 룰 5개를 감/질문 대신 데이터로 최적값 도출.
★ 과최적화 방지 규율: ①한번에 하나씩(나머지 baseline 고정) ②뾰족점 말고 plateau
  ③OOS(2024~25 train vs 2026 test) 안정성 ④高끼 구간 동일방향 확인. 출발값이지 정답 아님.

데이터: stock_data_daily(3885종목). 돌파(종가+10% & 거래량2×MA20) 종가매수.
baseline: max_re=2 / when=reclaim / at=close / holdK=5 / size=1.0. 비용 0.48%/leg.
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
                    # 날짜: '날짜'/'date' 헤더 또는 무명 인덱스('') 컬럼 (파일별 상이)
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


def _sim(rows, e, hold_k=5, max_re=2, when="reclaim", at="close", size=1.0):
    """재진입 전략 PnL(=레그 size가중 합, %). 1차 size 1.0, 재진입 size=size."""
    entry = rows[e][3]
    end = min(e + hold_k, len(rows) - 1)
    days = list(range(e + 1, end + 1))
    if not days:
        return None
    pnl = 0.0
    in_pos = True
    p_in = entry
    hw = entry
    last_stop = entry * (1 - TRAIL)
    leg_size = 1.0
    re_used = 0
    for d in days:
        o, h, l, c, v = rows[d]
        if in_pos:
            hw = max(hw, h)
            last_stop = hw * (1 - TRAIL)
            if l <= last_stop:
                pnl += ((last_stop / p_in - 1) - COST_LEG) * leg_size
                in_pos = False
        else:
            if re_used >= max_re:
                continue
            ma5 = sum(rows[k][3] for k in range(max(0, d - 4), d + 1)) / min(5, d + 1)
            ok = c >= last_stop if when == "reclaim" else (c >= last_stop and c >= ma5)
            if ok:
                # 재진입 체결가: 당일 종가 or 익일 시가
                if at == "next_open" and d + 1 <= end:
                    p_in = rows[d + 1][1] or c
                else:
                    p_in = c
                in_pos = True
                hw = p_in
                leg_size = size
                re_used += 1
    if in_pos:
        pnl += ((rows[end][3] / p_in - 1) - COST_LEG) * leg_size
    return {"pnl": pnl * 100, "re": re_used}


def main() -> int:
    loaded = []
    for f in glob.glob(str(DAILY / "*.csv")):
        rows, dates = _load(f)
        if len(rows) >= 70:
            loaded.append((rows, dates))
    ev = [(rows, e, dates[e], _kki(rows, e)) for rows, dates in loaded for e in _events(rows)]
    print(f"돌파 이벤트 {len(ev):,}건\n")

    def avg(sub, **kw):
        rs = [r for r in (_sim(rows, e, **kw) for rows, e, _d, _k in sub) if r]
        if not rs:
            return None
        return sum(r["pnl"] for r in rs) / len(rs), sum(r["re"] for r in rs) / len(rs)

    hi = [x for x in ev if x[3] >= 3]   # 高끼(HUNTABLE+)

    def line(label, all_kw):
        a = avg(ev, **all_kw); h = avg(hi, **all_kw)
        print(f'  {label:<22} 전체 {a[0]:>+6.2f}% (재진입{a[1]:.2f}회)   高끼 {h[0]:>+6.2f}%')

    print("[1] 최대 재진입 횟수 (baseline 외 고정):")
    for mr in (0, 1, 2, 3, 99):
        line(f"max_re={mr if mr<99 else '무제한'}", dict(max_re=mr))
    print("\n[2] 재진입 타이밍:")
    for at in ("close", "next_open"):
        line(f"at={at}", dict(at=at))
    print("\n[3] 재진입 조건:")
    for wh in ("reclaim", "reclaim+ma5"):
        line(f"when={wh}", dict(when=wh))
    print("\n[4] 보유 윈도우:")
    for k in (3, 5, 7, 10):
        line(f"holdK={k}", dict(hold_k=k))
    print("\n[5] 재진입 비중:")
    for sz in (1.0, 0.5):
        line(f"size={sz}", dict(size=sz))

    print("\n[OOS] baseline(max_re2/reclaim/close/K5/size1) 기간분할 안정성:")
    tr = [x for x in ev if x[2] and x[2] < OOS_SPLIT]
    te = [x for x in ev if x[2] and x[2] >= OOS_SPLIT]
    for name, sub in [(f"train(<{OOS_SPLIT})", tr), (f"test(>={OOS_SPLIT})", te)]:
        a = avg(sub); h = avg(hi if False else [x for x in sub if x[3] >= 3])
        print(f'  {name:<20} 이벤트 {len(sub):>6,}  전체 {a[0]:>+6.2f}%  高끼 {h[0]:>+6.2f}%')
    print("\n해석: 각 파라미터에서 '넓은 plateau'의 견고한 값 채택(뾰족점 회피). OOS train≈test면 안정.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
