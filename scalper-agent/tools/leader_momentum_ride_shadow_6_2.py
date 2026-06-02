# -*- coding: utf-8 -*-
"""leader_momentum_ride_shadow — 주도주 포착시점 + 올라타기 성과 검증 (6/2, read-only).

사장님 지시(6/2, 방향전환): 반등은 "안 켜는 이유" 찾고 닫음. 다음 = "강한 놈에 어떻게
올라타서 어디서 내릴지". 주도주(삼성SDS·LG전자·LG CNS·로보스타·두산로보틱스 등)를
우리 시스템이 ①언제 포착했는지 + ②진입가/익절가/수익금/보유일/트레일링 성과 검증.

탐지(점화 = 기지서 돌파): prior 20일 비과열(position≤0.6) + 당일 +10%↑ & 거래량 2.5x↑.
라이드: 탐지일 종가 진입 → 고점 -3% 트레일링(SAJANG.TRAILING_PCT) → 청산. 비용 0.48% 왕복.
끼(score_kki)로 포착시 등급 라벨.

★ 순수 read-only: 실주문 0 / 주문함수 0 / SAJANG 무변경 / scheduler·systemd 무변경 / 봇 OFF.
★★ 정직(생존편향 경고): 이 5종목은 '이미 아는 승자' = 후행/hindsight. 본 분석은 "포착됐다면
   라이드/청산이 어땠나"(트레일 캡처)를 봄. '시스템이 주도주를 사전에 잘 고르나'(precision)는
   별개 = 전수 prospective 스캔 필요(다음 단계). 일봉 트레일=장중 휩쏘 과소(현 한계).

사용: python tools/leader_momentum_ride_shadow_6_2.py [--start 20250101] [--selftest]
"""
from __future__ import annotations

import argparse
import csv
import glob
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
SA = Path(__file__).resolve().parent.parent
DAILY_DIR = ROOT / "stock_data_daily"
sys.path.insert(0, str(SA))

from data.limit_up_scanner import LimitUpStock, score_kki, count_surge_limit_days  # noqa: E402
from data.sajang_rules import SAJANG  # noqa: E402

LEADERS = {
    "018260": "삼성SDS", "066570": "LG전자", "064400": "LG CNS",
    "090360": "로보스타", "454910": "두산로보틱스",
}

# 탐지 파라미터 (점화 = 기지서 돌파)
IGNITE_SURGE = 10.0       # 당일 +10%↑
IGNITE_VOLX = 2.5         # 거래량 2.5x↑
BASE_POS_MAX = 0.60       # 직전일 60일레인지 위치 ≤0.6 (과열 아님)
TRAIL = SAJANG.TRAILING_PCT / 100      # 고점 -3% (영구룰)
HOLD_CAP = 90            # 보유 캡(거래일)
BUDGET = 10_000_000
BUY_COST, SELL_COST = 0.0015, 0.0033


def _paths():
    d = {}
    for f in glob.glob(str(DAILY_DIR / "*.csv")):
        d[os.path.basename(f)[:-4].split("_")[-1]] = f
    return d


def load_daily(path):
    with open(path, encoding="utf-8-sig") as f:
        rd = list(csv.reader(f))
    out = []
    for r in rd[1:]:
        try:
            out.append((r[0][:10].replace("/", "-"), float(r[1]), float(r[2]),
                        float(r[3]), float(r[4]), float(r[5]) if len(r) > 5 else 0.0))
        except (ValueError, IndexError):
            pass
    out.sort()
    return out


def _atr_pct(daily, i, n=14):
    a = max(1, i - n)
    trs = [max(daily[k][2] - daily[k][3], abs(daily[k][2] - daily[k - 1][4]),
               abs(daily[k][3] - daily[k - 1][4])) for k in range(a, i + 1)]
    return (sum(trs) / len(trs)) / daily[i][4] * 100 if trs and daily[i][4] > 0 else 0.0


def detect_ignitions(daily, start):
    """점화일 인덱스 리스트 (기지서 +10% & 2.5x 돌파)."""
    hits = []
    for i in range(20, len(daily)):
        d = daily[i]
        if d[0] < start:
            continue
        prev = daily[i - 1]
        chg = d[4] / prev[4] - 1 if prev[4] > 0 else 0
        if chg < IGNITE_SURGE / 100:
            continue
        vma = sum(x[5] for x in daily[i - 20:i]) / 20
        if vma <= 0 or d[5] / vma < IGNITE_VOLX:
            continue
        # 직전일 기지(60일레인지 위치 ≤0.6)
        win = daily[max(0, i - 60):i]
        lo = min(x[3] for x in win); hi = max(x[2] for x in win)
        pos = (prev[4] - lo) / (hi - lo) if hi > lo else 0.5
        if pos > BASE_POS_MAX:
            continue
        hits.append(i)
    return hits


def kki_at(code, name, daily, i):
    d = daily[i]
    tv = d[4] * d[5] / 1e8
    cs = (d[4] - d[3]) / (d[2] - d[3]) if d[2] > d[3] else 0.5
    vma = sum(x[5] for x in daily[max(0, i - 20):i]) / max(1, len(daily[max(0, i - 20):i]))
    volx = d[5] / vma if vma > 0 else 0.0
    surge, limit = count_surge_limit_days([x[4] for x in daily[:i + 1]])
    st = LimitUpStock(code=code, name=name, close=int(d[4]), volume_ratio=round(volx, 2),
                      trading_value_억=round(tv, 1), turnover_pct=0.0,
                      close_strength=round(cs, 3), consecutive_limit=0)
    return score_kki(st, _atr_pct(daily, i), surge, limit)


def ride(daily, i_entry, trail_pct=TRAIL):
    """탐지일 종가 진입 → 고점 트레일 → 청산. 일봉 기준."""
    entry = daily[i_entry][4]
    shares = int(BUDGET // entry)
    invested = shares * entry * (1 + BUY_COST)
    peak = entry
    peak_day = i_entry
    for k in range(i_entry + 1, min(len(daily), i_entry + 1 + HOLD_CAP)):
        d = daily[k]
        stop = peak * (1 - trail_pct)
        if d[3] <= stop:                       # 저가가 트레일선 닿음
            fill = min(d[1], stop) if d[1] <= stop else stop
            proceeds = shares * fill * (1 - SELL_COST)
            return _result(daily, i_entry, k, entry, fill, invested, shares, peak, peak_day, "trail_stop")
        if d[2] > peak:
            peak = d[2]; peak_day = k
    # 캡 도달 → 마지막 종가 청산
    k = min(len(daily) - 1, i_entry + HOLD_CAP)
    proceeds_px = daily[k][4]
    return _result(daily, i_entry, k, entry, proceeds_px, invested, shares, peak, peak_day, "hold_cap")


def _result(daily, i_e, i_x, entry, exit_px, invested, shares, peak, peak_day, reason):
    proceeds = shares * exit_px * (1 - SELL_COST)
    pnl = proceeds - invested
    peak_gain = (peak - entry) / entry * 100
    real_gain = (exit_px - entry) / entry * 100
    capture = (real_gain / peak_gain * 100) if peak_gain > 0 else 0.0
    return {
        "entry_date": daily[i_e][0], "entry": round(entry),
        "exit_date": daily[i_x][0], "exit": round(exit_px), "reason": reason,
        "hold_days": i_x - i_e,
        "peak_date": daily[peak_day][0], "peak": round(peak),
        "peak_gain_pct": round(peak_gain, 1), "real_gain_pct": round(real_gain, 1),
        "capture_pct": round(capture, 1),
        "pnl_won": round(pnl), "net_pct": round(pnl / BUDGET * 100, 2),
        "days_to_peak": peak_day - i_e,
    }


TRAIL_GRID = [0.03, 0.06, 0.10]   # 연구비교: 영구룰 3% vs 넓은 트레일(주도주 라이드용)


def run(start):
    paths = _paths()
    rows = []
    trail_cmp = {t: {"net": 0, "rg": [], "n": 0} for t in TRAIL_GRID}
    for code, name in LEADERS.items():
        p = paths.get(code)
        if not p:
            print(f"  [!] {code} {name}: 일봉 없음"); continue
        daily = load_daily(p)
        igs = detect_ignitions(daily, start)
        if not igs:
            rows.append({"code": code, "name": name, "ignitions": 0, "first": None}); continue
        i0 = igs[0]
        r = ride(daily, i0, TRAIL)
        r.update({"code": code, "name": name, "ignitions": len(igs),
                  "kki": round(kki_at(code, name, daily, i0), 0),
                  "kki_grade": SAJANG.kki_grade(kki_at(code, name, daily, i0))})
        rows.append(r)
        for t in TRAIL_GRID:
            rt = ride(daily, i0, t)
            trail_cmp[t]["net"] += rt["pnl_won"]; trail_cmp[t]["rg"].append(rt["real_gain_pct"])
            trail_cmp[t]["n"] += 1
    return rows, trail_cmp


def selftest():
    ok = []
    # 합성 일봉: 20일 횡보(기지) → 급등(점화) → 상승추세 → 하락(트레일청산)
    daily = [("2025-01-%02d" % (d + 1), 100, 101, 99, 100, 1000) for d in range(20)]
    daily.append(("2025-02-01", 100, 112, 100, 111, 5000))      # +11% & 5x 점화
    for d in range(5):                                           # 추세 상승
        c = 111 + d * 5
        daily.append(("2025-02-%02d" % (d + 2), c, c + 6, c - 1, c + 5, 2000))
    for d in range(5):                                           # 하락(트레일청산)
        c = 140 - d * 8
        daily.append(("2025-02-%02d" % (d + 10), c, c + 1, c - 9, c - 8, 2000))
    igs = detect_ignitions(daily, "2025-01-01")
    ok.append(("T1 점화 탐지(1건, idx20)", igs == [20]))
    r = ride(daily, 20)
    ok.append(("T2 라이드 청산(trail_stop)", r["reason"] == "trail_stop"))
    ok.append(("T3 peak>entry & capture 0~100", r["peak"] > r["entry"] and 0 <= r["capture_pct"] <= 100))
    ok.append(("T4 보유일>0", r["hold_days"] > 0))
    # T5 과열 종목(이미 고점)은 점화 탐지 X
    daily2 = [("2025-01-%02d" % (d + 1), 100 + d * 5, 106 + d * 5, 99 + d * 5, 105 + d * 5, 1000) for d in range(20)]
    daily2.append(("2025-02-01", 195, 220, 195, 218, 5000))     # 이미 고점서 급등
    ok.append(("T5 과열은 점화 제외", detect_ignitions(daily2, "20250101") == []))
    print("leader ride 셀프테스트:")
    for n, p in ok:
        print(f"  [{'PASS' if p else 'FAIL'}] {n}")
    return all(p for _, p in ok)


def main():
    sys.stdout.reconfigure(encoding="utf-8")
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2025-01-01", help="탐지 시작일 YYYY-MM-DD or YYYYMMDD")
    ap.add_argument("--selftest", action="store_true")
    a = ap.parse_args()
    if a.selftest:
        sys.exit(0 if selftest() else 1)
    start = a.start if "-" in a.start else f"{a.start[:4]}-{a.start[4:6]}-{a.start[6:8]}"

    rows, trail_cmp = run(start)
    print("=" * 92)
    print(f"leader_momentum_ride_shadow — 주도주 포착시점 + 올라타기 (탐지 {start}~, 고점-{SAJANG.TRAILING_PCT}% 트레일)")
    print(f"★ 증빙: SAJANG.AUTO_TRADE_DISABLED={SAJANG.AUTO_TRADE_DISABLED} / 실주문 0 / 주문함수 0 / SAJANG·scheduler·systemd 무변경")
    print("=" * 92)
    print(f"{'종목':<12}{'점화수':>5}{'포착일':>12}{'끼':>8}{'진입':>9}{'익절(청산)':>11}{'보유일':>6}"
          f"{'고점%':>7}{'실현%':>7}{'캡처%':>7}{'수익금':>13}")
    tot = 0
    n = 0
    for r in rows:
        if not r.get("entry_date"):
            print(f"{r['name']:<12}{r['ignitions']:>5}  (점화 미탐지)")
            continue
        n += 1; tot += r["pnl_won"]
        print(f"{r['name']:<12}{r['ignitions']:>5}{r['entry_date']:>12}{r['kki']:>5.0f}/{r['kki_grade'][:3]}"
              f"{r['entry']:>9,}{r['exit']:>11,}{r['hold_days']:>6}{r['peak_gain_pct']:>7}"
              f"{r['real_gain_pct']:>7}{r['capture_pct']:>7}{r['pnl_won']:>13,}")
    print("-" * 92)
    print(f"  합계 수익금(종목당 {BUDGET:,}원, {n}종목): {tot:,}원")
    avg_cap = round(sum(r["capture_pct"] for r in rows if r.get("entry_date")) / n, 1) if n else 0
    print(f"  평균 트레일 캡처율: {avg_cap}%  (고점 대비 실현 비율 — 트레일링 성과)")
    print(f"\n── 트레일 폭 비교 (어디서 내릴지, 연구용·SAJANG 무변경) ──")
    print(f"  {'트레일':<8}{'합계수익금':>14}{'평균실현%':>10}")
    for t in TRAIL_GRID:
        c = trail_cmp[t]
        rg = round(sum(c["rg"]) / c["n"], 1) if c["n"] else 0
        tag = " (영구룰)" if abs(t - TRAIL) < 1e-9 else ""
        print(f"  -{t*100:>4.0f}%{c['net']:>14,}{rg:>10}{tag}")
    print("\n★★ 정직: 이 5종목=이미 아는 승자(생존편향/hindsight). 본 분석=포착됐다면 라이드/청산 성과(트레일 캡처).")
    print("   '시스템이 주도주를 사전선별 잘하나'(precision)는 별개=전수 prospective 스캔 필요(다음). 일봉 트레일=장중 휩쏘 과소.")


if __name__ == "__main__":
    main()
