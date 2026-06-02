# -*- coding: utf-8 -*-
"""leader_exit_refine — 구조청산 정밀화 (#2a, 6/2, read-only).

전수 스캔(leader_prospective_scan)이 "구조청산(전저점/MA) >> 고정%트레일" 판정 →
이제 구조청산을 정밀화: 전저점 lookback(3/5/10) · MA(5/10/20) · tail 컷(-15%) 조합.
★ mean이 tail주도였으니 ★median(typical 거래)★ 필수 추가. 급등형/완만형 분리.

★ 순수 read-only: 실주문 0 / 주문함수 0 / SAJANG·scheduler·systemd 무변경 / 봇 OFF.
★ 정직: 생존편향(상폐부재 과대)·수급미반영·일봉(장중 휩쏘 과소). 상대·방향만.

재사용: leader_prospective_scan_6_2 의 detect/load_daily/_atr/_ma/_is_excluded.
사용: python tools/leader_exit_refine_6_2.py [--months 6] [--limit N] [--selftest]
"""
from __future__ import annotations

import argparse
import glob
import os
import sys
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
SA = Path(__file__).resolve().parent.parent
TOOLS = Path(__file__).resolve().parent
DAILY_DIR = ROOT / "stock_data_daily"
sys.path.insert(0, str(SA))
sys.path.insert(0, str(TOOLS))

from data.sajang_rules import SAJANG  # noqa: E402
from leader_prospective_scan_6_2 import (  # noqa: E402
    detect, load_daily, _ma, _is_excluded, STEADY_HIGH_N,
)

HOLD_CAP = 40
CATA = 0.15           # tail 컷(-15% 재앙스톱)
BUDGET = 10_000_000
BUY_COST, SELL_COST = 0.0015, 0.0033

# 구조청산 변형
VARIANTS = ["swinglow3", "swinglow5", "swinglow10", "ma5", "ma10", "ma20",
            "swinglow5+cata", "ma10+cata"]


def _swing_lo(d, k, n):
    return min(d[j][3] for j in range(max(0, k - n), k))


def exit_variant(d, i, kind):
    """진입=d[i] 종가. kind별 청산 → (net%, hold, exited_reason)."""
    entry = d[i][4]
    end = min(len(d), i + 1 + HOLD_CAP)
    exit_px = d[end - 1][4] if end - 1 > i else entry
    exit_k = end - 1
    cata = kind.endswith("+cata")
    base = kind.replace("+cata", "")
    for k in range(i + 1, end):
        x = d[k]
        # tail 재앙스톱(있으면 우선) — 저가가 -15% 닿으면
        if cata:
            cstop = entry * (1 - CATA)
            if x[3] <= cstop:
                exit_px = min(x[1], cstop) if x[1] <= cstop else cstop
                exit_k = k; break
        # 구조청산(종가기준)
        trig = False
        if base.startswith("swinglow"):
            n = int(base.replace("swinglow", ""))
            if x[4] < _swing_lo(d, k, n):
                trig = True
        elif base.startswith("ma"):
            n = int(base.replace("ma", ""))
            if x[4] < _ma(d, k, n):
                trig = True
        if trig:
            exit_px = x[4]; exit_k = k; break
    shares = int(BUDGET // entry)
    net = (shares * exit_px * (1 - SELL_COST) - shares * entry * (1 + BUY_COST)) / BUDGET * 100
    return net, exit_k - i


def _median(x):
    if not x:
        return 0.0
    s = sorted(x); n = len(s)
    return round((s[n // 2] if n % 2 else (s[n // 2 - 1] + s[n // 2]) / 2), 2)


def run(months, limit):
    files = glob.glob(str(DAILY_DIR / "*.csv"))
    if limit:
        files = files[:limit]
    cutoff = (date.today() - timedelta(days=int(months * 30.4))).isoformat()
    acc = {t: {v: {"net": [], "hold": []} for v in VARIANTS} for t in ("SURGE", "STEADY")}
    n_cand = {"SURGE": 0, "STEADY": 0}
    n_files = 0
    for f in files:
        name = os.path.basename(f)[:-4].rsplit("_", 1)[0]
        if _is_excluded(name):
            continue
        d = load_daily(f)
        if len(d) < STEADY_HIGH_N + 25:
            continue
        n_files += 1
        for i in range(STEADY_HIGH_N, len(d) - 21):
            if d[i][0] < cutoff:
                continue
            t = detect(d, i)
            if not t:
                continue
            n_cand[t] += 1
            for v in VARIANTS:
                net, hold = exit_variant(d, i, v)
                acc[t][v]["net"].append(net); acc[t][v]["hold"].append(hold)
    return acc, n_cand, n_files, cutoff


def _avg(x):
    return round(sum(x) / len(x), 2) if x else 0.0


def selftest():
    ok = []
    # 상승추세 후 MA/전저점 깨지는 합성
    d = [(f"s{k:03d}", 100 + k, 101 + k, 99 + k, 100 + k, 1000) for k in range(140)]  # 완만상승
    for k in range(10):                                   # 추가상승 후 급락
        c = 240 + k * 3
        d.append((f"u{k:02d}", c, c + 2, c - 1, c + 1, 1000))
    for k in range(10):
        c = 270 - k * 12
        d.append((f"x{k:02d}", c, c, c - 13, c - 12, 1000))
    net, hold = exit_variant(d, 150, "ma10")
    ok.append(("T1 ma10 청산 동작", isinstance(net, float) and hold > 0))
    net2, _ = exit_variant(d, 150, "swinglow5")
    ok.append(("T2 swinglow5 청산 동작", isinstance(net2, float)))
    netc, _ = exit_variant(d, 150, "ma10+cata")
    ok.append(("T3 +cata 변형 동작", isinstance(netc, float)))
    ok.append(("T4 median", _median([1, 3, 2]) == 2 and _median([1, 2, 3, 4]) == 2.5))
    ok.append(("T5 swing_lo", _swing_lo([(0, 0, 0, 9, 0), (0, 0, 0, 7, 0), (0, 0, 0, 8, 0)], 2, 2) == 7))
    print("exit refine 셀프테스트:")
    for n, p in ok:
        print(f"  [{'PASS' if p else 'FAIL'}] {n}")
    return all(p for _, p in ok)


def main():
    sys.stdout.reconfigure(encoding="utf-8")
    ap = argparse.ArgumentParser()
    ap.add_argument("--months", type=float, default=6.0)
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--selftest", action="store_true")
    a = ap.parse_args()
    if a.selftest:
        sys.exit(0 if selftest() else 1)

    acc, n_cand, n_files, cutoff = run(a.months, a.limit)
    print("=" * 94)
    print(f"leader_exit_refine — 구조청산 정밀화 (#2a, 전수 walk-forward, 최근 {a.months:g}개월 {cutoff}~)")
    print(f"스캔 {n_files}종목 / 후보 SURGE {n_cand['SURGE']} · STEADY {n_cand['STEADY']}")
    print(f"★ 증빙: SAJANG.AUTO_TRADE_DISABLED={SAJANG.AUTO_TRADE_DISABLED} / 실주문 0 / SAJANG·scheduler·systemd 무변경")
    print("=" * 94)
    for t in ("SURGE", "STEADY"):
        print(f"\n■ {t} ({n_cand[t]}건)")
        print(f"  {'exit변형':<16}{'mean%':>8}{'median%':>9}{'승률%':>7}{'최악%':>9}{'평균보유':>8}")
        rows = []
        for v in VARIANTS:
            nets = acc[t][v]["net"]
            wr = 100 * sum(1 for x in nets if x > 0) / len(nets) if nets else 0
            rows.append((v, _avg(nets), _median(nets), wr, round(min(nets), 1) if nets else 0,
                         _avg(acc[t][v]["hold"])))
        for v, mn, md, wr, wo, hd in rows:
            print(f"  {v:<16}{mn:>8}{md:>9}{wr:>7.0f}{wo:>9}{hd:>8}")
        best = max(rows, key=lambda r: r[1])
        print(f"  → mean 최고: {best[0]} ({best[1]}%) / median 최고: {max(rows, key=lambda r: r[2])[0]} ({max(rows, key=lambda r: r[2])[2]}%)")
    print("\n★★ 정직: mean=tail주도(median이 typical) / 생존편향(과대,상대만) / 수급미반영 / 일봉(분봉검증=#2b).")
    print("   tail컷(+cata)은 최악%를 줄이는지(꼬리리스크) 확인용. flip 금지·봇 OFF.")


if __name__ == "__main__":
    main()
