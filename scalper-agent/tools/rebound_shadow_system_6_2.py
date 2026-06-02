# -*- coding: utf-8 -*-
"""하락후반등+피라미딩 — 종목선정 게이트 포함 shadow 관측 체계 (6/2, read-only).

사장님 결정문(6/2) 구현. "바로 실전" 아니라 종목선정까지 포함한 shadow 검증.

파이프라인(사장님 지시):
  거래대금 hard filter (호가·시장경보=hook)
  → 전일 -3% 하락 setup
  → 당일 5분봉 전일종가 +1% 회복 confirm (1봉 primary / 2봉 secondary 병렬)
  → VWAP/양봉/전일저점 방어 점수화 → total_score
  → 끼 게이트(MODERATE+) & total_score>=70 만 shadow 후보
  → 후보·탈락 모두 reason_code 기록

시뮬: 10%→+1%30%→+2%30%→+3%30% (10/40/70/100), 손절/트레일 3%(STOP3) & 4%(STOP4) 병렬.
유니버스: 봇 active pool(1min 커버 종목) = primary / 사장님 블루칩 바스켓 = control.

★ 순수 read-only: 실주문 0건 / SAJANG 무변경 / 매도경로·picks 무접촉 / 봇 OFF.
  끼 게이트는 봇 실함수 score_kki 재사용(MODERATE컷·유동성50억 = 봇과 동일 진실).
  분봉=2026-02-19~03-30(~16일) → thin·방향성 검증용. forward 누적이 본검증.

사용: python tools/rebound_shadow_system_6_2.py [--limit N] [--selftest]
"""
from __future__ import annotations

import argparse
import csv
import glob
import os
import sys
from collections import Counter, defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent          # repo 루트
SA = Path(__file__).resolve().parent.parent                   # scalper-agent
DAILY_DIR = ROOT / "stock_data_daily"
MIN_DIR = SA / "data_store" / "1min"
sys.path.insert(0, str(SA))

from data.limit_up_scanner import LimitUpStock, score_kki, count_surge_limit_days  # noqa: E402
from data.sajang_rules import SAJANG  # noqa: E402

# ── 사장님 블루칩 바스켓 (control) — flowx 백테 종목 ──
CONTROL = ["005930","000660","035420","035720","005380","012450","373220","105560",
           "055550","086790","068270","207940","005490","010120","454910",
           "003080","012030","035900","041190","053950","064260","078340","089980",
           "101490","108490","112040","194480","215600","225570"]

# ── 파라미터 (사장님 결정문) ──
DOWN_TRIG = 0.03            # 전일 -3% setup
REBOUND = 0.01             # 전일종가 +1% 회복
ADD_LEVELS = (0.01, 0.02, 0.03)         # anchor 대비 +1/2/3% 추가매수
ALLOC = (0.10, 0.30, 0.30, 0.30)        # 10→40→70→100
STOPS = {"STOP3_REENTER": 0.03, "CODEX_STOP4": 0.04}   # 사장님 라벨
CONFIRMS = {"confirm1": 1, "confirm2": 2}
SCORE_MIN = 70.0
SURGE_OVERHEAT = 0.30      # 당일 +30%↑ 과열 추격 금지
HOLD_CAP_DAYS = 5
BUDGET = 10_000_000
BUY_COST, SELL_COST = 0.0015, 0.0033
MIN_PRICE = 1000          # 호가 hard filter(동전주 회피, 시장경보=hook)


# ════════════════════════ 데이터 로딩 ════════════════════════
def _daily_paths():
    d = {}
    for f in glob.glob(str(DAILY_DIR / "*.csv")):
        code = os.path.basename(f)[:-4].split("_")[-1]
        d[code] = f
    return d


def load_daily(path):
    """[(date, o, h, l, c, v)]. 컬럼: [date, open, high, low, close, volume] (date 헤더 빈문자)."""
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


def load_min_by_day(path):
    """{day: [(dt, o, h, l, c, v)]}. 컬럼: [datetime, o, h, l, c, v]."""
    with open(path, encoding="utf-8-sig") as f:
        rd = list(csv.reader(f))
    days = defaultdict(list)
    for r in rd[1:]:
        try:
            day = r[0][:10].replace("/", "-")
            days[day].append((r[0], float(r[1]), float(r[2]), float(r[3]),
                              float(r[4]), float(r[5]) if len(r) > 5 else 0.0))
        except (ValueError, IndexError):
            pass
    for d in days:
        days[d].sort()
    return days


def to_5min(bars):
    """1분봉 → 5분봉 (벽시계 5분 버킷). [(end_dt, o, h, l, c, v)]."""
    bucket = {}
    order = []
    for dt, o, h, l, c, v in bars:
        hhmm = dt[11:16]
        try:
            mm = int(hhmm[3:5]); key = dt[11:13] + f"{(mm // 5) * 5:02d}"
        except ValueError:
            continue
        if key not in bucket:
            bucket[key] = [dt, o, h, l, c, v]; order.append(key)
        else:
            b = bucket[key]
            b[0] = dt; b[2] = max(b[2], h); b[3] = min(b[3], l); b[4] = c; b[5] += v
    return [tuple(bucket[k]) for k in order]


# ════════════════════════ 파이프라인 ════════════════════════
def _atr_pct(daily, i, n=14):
    a = max(1, i - n)
    trs = [max(daily[k][2] - daily[k][3], abs(daily[k][2] - daily[k - 1][4]),
               abs(daily[k][3] - daily[k - 1][4])) for k in range(a, i + 1)]
    return (sum(trs) / len(trs)) / daily[i][4] * 100 if trs and daily[i][4] > 0 else 0.0


def kki_for(code, name, daily, i):
    d = daily[i]
    o, h, l, c, v = d[1], d[2], d[3], d[4], d[5]
    tv = c * v / 1e8
    cs = (c - l) / (h - l) if h > l else 0.5
    vma = sum(x[5] for x in daily[max(0, i - 20):i]) / max(1, len(daily[max(0, i - 20):i]))
    volx = v / vma if vma > 0 else 0.0
    surge, limit = count_surge_limit_days([x[4] for x in daily[:i + 1]])
    st = LimitUpStock(code=code, name=name, close=int(c), volume_ratio=round(volx, 2),
                      trading_value_억=round(tv, 1), turnover_pct=0.0,
                      close_strength=round(cs, 3), consecutive_limit=0)
    return score_kki(st, _atr_pct(daily, i), surge, limit), tv


def confirm_entry(bars5, rebound_price, n_bars):
    """5분봉 종가가 rebound_price 위에서 n_bars 연속 마감 → (entry_bar_idx, entry_price)."""
    streak = 0
    for j, b in enumerate(bars5):
        if b[4] >= rebound_price:
            streak += 1
            if streak >= n_bars:
                return j, b[4]
        else:
            streak = 0
    return None, None


def setup_score(daily, i, bars5, ent_j, prev_low):
    """VWAP(40)/양봉(30)/전일저점방어(30) → 0~100."""
    d = daily[i]; o, h, l, c = d[1], d[2], d[3], d[4]
    # VWAP (진입봉까지 누적)
    cum_pv = cum_v = 0.0
    for b in bars5[:ent_j + 1]:
        tp = (b[2] + b[3] + b[4]) / 3
        cum_pv += tp * b[5]; cum_v += b[5]
    vwap = cum_pv / cum_v if cum_v > 0 else c
    ent_close = bars5[ent_j][4]
    vwap_sc = 40.0 if ent_close >= vwap * 1.005 else (28.0 if ent_close >= vwap else 12.0)
    # 양봉 강도 (일봉 종가-시가 / 레인지)
    green = (c - o) / (h - l) if h > l else 0.0
    green_sc = max(0.0, min(1.0, green)) * 30.0
    # 전일 저점 방어 (당일 저가가 전일 저가 위 유지)
    defense_sc = 30.0 if l >= prev_low else (15.0 if l >= prev_low * 0.985 else 0.0)
    return round(vwap_sc + green_sc + defense_sc, 1)


def simulate(entry_price, entry_day_1min_after, daily_after, stop_pct, trail_pct):
    """피라미딩 시뮬 → (net_pct, mdd_pct, same_day_whipsaw, full_filled)."""
    anchor = entry_price
    shares = invested = 0.0
    peak = entry_price
    add_idx = 0
    worst = 0.0   # 최악 미실현 손익(원, ≤0) = 예산대비 MAE

    def buy(price, alloc_pct):
        nonlocal shares, invested
        spend = BUDGET * alloc_pct
        sh = int(spend // price)
        if sh <= 0:
            return
        shares += sh
        invested += sh * price * (1 + BUY_COST)

    buy(entry_price, ALLOC[0])

    def step(o, h, l):
        """1분/일봉 1스텝: MAE갱신→손절체크(저가)→추가매수(고가). 반환 stop_price or None(청산)."""
        nonlocal peak, add_idx, worst
        if shares:
            worst = min(worst, shares * l - invested)   # 이 봉 저가 기준 최악 미실현
        avg = invested / shares if shares else 0
        active_stop = max(avg * (1 - stop_pct), peak * (1 - trail_pct))
        if l <= active_stop:
            return min(o, active_stop) if o <= active_stop else active_stop
        peak = max(peak, h)
        while add_idx < len(ADD_LEVELS) and shares:
            add_px = anchor * (1 + ADD_LEVELS[add_idx])
            if h < add_px:
                break
            buy(max(o, add_px), ALLOC[add_idx + 1])
            add_idx += 1
        return None

    def out(price, same_day):
        proceeds = shares * price * (1 - SELL_COST)
        mdd = max(0.0, -worst / BUDGET * 100)
        return (proceeds - invested) / BUDGET * 100, round(mdd, 2), same_day, add_idx >= len(ADD_LEVELS)

    # 진입일 장중 (1분봉)
    for b in entry_day_1min_after:
        sp = step(b[1], b[2], b[3])
        if sp is not None:
            return out(sp, True)
    # 이후 일봉
    for d in daily_after[:HOLD_CAP_DAYS]:
        sp = step(d[1], d[2], d[3])
        if sp is not None:
            return out(sp, False)
    # 캡 도달 → 마지막 종가 청산
    last_c = daily_after[min(HOLD_CAP_DAYS, len(daily_after)) - 1][4] if daily_after else entry_price
    return out(last_c, False)


# ════════════════════════ 실행 ════════════════════════
def run(limit=0):
    dpaths = _daily_paths()
    min_files = {os.path.basename(f)[:-4]: f for f in glob.glob(str(MIN_DIR / "*.csv"))}
    active = [c for c in min_files if c not in CONTROL]
    if limit:
        active = active[:limit]
    universes = {"ACTIVE(봇 풀)": active, "CONTROL(사장님 바스켓)": [c for c in CONTROL if c in min_files]}

    real_orders = 0   # ★ 실주문 0 증빙: 이 하니스는 주문함수 0회 호출
    report = {}
    for uname, codes in universes.items():
        reasons = Counter()
        n_hardpass = n_setup = n_kki = 0
        cands = []   # (code, day, score, entry_price, e1min_after, daily_after, prev_low)
        for code in codes:
            dp = dpaths.get(code)
            mf = min_files.get(code)
            if not dp or not mf:
                continue
            daily = load_daily(dp)
            if len(daily) < 25:
                continue
            dmap = {d[0]: k for k, d in enumerate(daily)}
            mins = load_min_by_day(mf)
            name = os.path.basename(dp)[:-4].rsplit("_", 1)[0]
            for day in sorted(mins):
                i = dmap.get(day)
                if i is None or i < 22:
                    continue
                d = daily[i]
                # 1) hard filter (거래대금·호가; 시장경보=hook None)
                tv = d[4] * d[5] / 1e8
                if d[4] < MIN_PRICE:
                    reasons["PRICE_LOW"] += 1; continue
                if tv < SAJANG.KKI_MIN_LIQUIDITY_억:
                    reasons["LIQ_LOW"] += 1; continue
                n_hardpass += 1
                # 2) 전일 -3% setup
                prev, pp = daily[i - 1], daily[i - 2]
                if not (prev[4] <= pp[4] * (1 - DOWN_TRIG) or prev[3] <= pp[4] * (1 - DOWN_TRIG)):
                    reasons["NO_DOWN_SETUP"] += 1; continue
                n_setup += 1
                # 과열 회피
                if d[4] / prev[4] - 1 > SURGE_OVERHEAT:
                    reasons["OVERHEAT"] += 1; continue
                # 3) 끼 게이트 (MODERATE+)
                kki, _ = kki_for(code, name, daily, i)
                if kki < SAJANG.KKI_MODERATE:
                    reasons["KKI_LOW"] += 1; continue
                n_kki += 1
                # 4) 5분봉 confirm + 5) score (confirm1 기준으로 후보 판정/점수)
                bars5 = to_5min(mins[day])
                reb_px = prev[4] * (1 + REBOUND)
                ent_j, ent_px = confirm_entry(bars5, reb_px, 1)
                if ent_j is None:
                    reasons["NO_CONFIRM"] += 1; continue
                score = setup_score(daily, i, bars5, ent_j, prev[3])
                if score < SCORE_MIN:
                    reasons["LOW_SCORE"] += 1; continue
                # shadow 후보 확정
                e1min_after = [b for b in mins[day] if b[0] > bars5[ent_j][0]]
                daily_after = daily[i + 1:]
                cands.append((code, name, day, score, bars5, reb_px, e1min_after, daily_after, prev[3], mins[day]))
        # ── 셀별 시뮬 (confirm1/2 × stop3/4) ──
        cell = {}
        missed = {"confirm2": 0}
        for cf_name, n_bars in CONFIRMS.items():
            for st_name, st_pct in STOPS.items():
                key = f"{cf_name}|{st_name}"
                nets, mdds, whips, fills, n_ent, n_false = [], [], [], 0, 0, 0
                for (code, name, day, score, bars5, reb_px, e1_after, daily_after, prev_low, all1min) in cands:
                    ent_j, ent_px = confirm_entry(bars5, reb_px, n_bars)
                    if ent_j is None:
                        if cf_name == "confirm2":
                            missed["confirm2"] += 1
                        continue
                    n_ent += 1
                    e1after = [b for b in all1min if b[0] > bars5[ent_j][0]]
                    net, mdd, whip, full = simulate(ent_px, e1after, daily_after, st_pct, st_pct)
                    nets.append(net); mdds.append(mdd); whips.append(whip); fills += full
                    if whip:
                        n_false += 1
                cell[key] = {
                    "entries": n_ent,
                    "net_avg": round(sum(nets) / len(nets), 2) if nets else 0.0,
                    "net_sum_won": round(sum(nets) / 100 * BUDGET) if nets else 0,
                    "mdd_avg": round(sum(mdds) / len(mdds), 2) if mdds else 0.0,
                    "whipsaw_pct": round(100 * sum(whips) / len(whips)) if whips else 0,
                    "false_entry": n_false,
                    "full_filled": fills,
                }
        report[uname] = {
            "funnel": {"hard_pass": n_hardpass, "down_setup": n_setup, "kki_ok": n_kki,
                       "candidates(score>=70&confirm1)": len(cands)},
            "reject_top5": reasons.most_common(5),
            "missed_entry(confirm2 vs 1)": missed["confirm2"],
            "cells": cell,
        }
    return report, real_orders


def selftest():
    ok = []
    # T1 to_5min: 10개 1분봉 → 2개 5분봉
    bars = [(f"2026-03-02 09:0{m}:00", 100, 100 + m, 99, 100 + m, 10) for m in range(0, 10)]
    b5 = to_5min(bars)
    ok.append(("T1 5분봉 집계 2봉", len(b5) == 2))
    # T2 confirm: rebound 위 2연속
    bars5 = [(0, 100, 101, 99, 100.5, 1), (0, 100, 102, 100, 101.2, 1), (0, 0, 0, 0, 101.5, 1)]
    j1, _ = confirm_entry(bars5, 101.0, 1)
    j2, _ = confirm_entry(bars5, 101.0, 2)
    ok.append(("T2 confirm1=1봉째", j1 == 1)); ok.append(("T2 confirm2=2봉째", j2 == 2))
    # T3 simulate 손절: 진입후 즉시 -10% → 같은날 whipsaw
    e1 = [(f"2026-03-02 10:0{m}:00", 100, 100, 88, 90, 1) for m in range(3)]
    net, mdd, whip, full = simulate(100.0, e1, [], 0.04, 0.04)
    ok.append(("T3 즉시손절 whipsaw", whip and net < 0))
    # T4 simulate 상승: 추가매수 다 채우고 보유
    e1 = [("d", 100, 104, 100, 104, 1)]
    after = [("2026-03-03", 104, 110, 104, 110, 1), ("2026-03-04", 110, 112, 109, 111, 1)]
    net, mdd, whip, full = simulate(100.0, e1, after, 0.04, 0.04)
    ok.append(("T4 상승 풀필+양수", full and net > 0 and not whip))
    # T5 score 범위
    b5s = [(0, 100, 105, 99, 104, 100)]
    d = [("d0", 0, 0, 0, 0, 0)] * 23 + [("d", 100, 106, 100, 104, 100)]
    sc = setup_score(d, 23, b5s, 0, 95)
    ok.append(("T5 score 0~100", 0 <= sc <= 100))
    print("shadow 시스템 셀프테스트:")
    for n, p in ok:
        print(f"  [{'PASS' if p else 'FAIL'}] {n}")
    return all(p for _, p in ok)


def main():
    sys.stdout.reconfigure(encoding="utf-8")
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=0, help="active 유니버스 종목 제한(테스트)")
    ap.add_argument("--selftest", action="store_true")
    a = ap.parse_args()
    if a.selftest:
        sys.exit(0 if selftest() else 1)

    rep, real_orders = run(a.limit)
    print("=" * 80)
    print("하락후반등+피라미딩 — shadow 관측 (6/2)  분봉 2026-02-19~03-30 (~16일, thin)")
    print(f"★ 봇 OFF 증빙: SAJANG.AUTO_TRADE_DISABLED={SAJANG.AUTO_TRADE_DISABLED} / 실주문 호출 {real_orders}건 / SAJANG·매도·picks 무접촉")
    print("=" * 80)
    for uname, r in rep.items():
        f = r["funnel"]
        print(f"\n■ {uname}")
        print(f"  [선정 퍼널] hard통과 {f['hard_pass']} → 전일-3%setup {f['down_setup']} "
              f"→ 끼MODERATE+ {f['kki_ok']} → 후보(score>=70&confirm) {f['candidates(score>=70&confirm1)']}")
        print(f"  [탈락 TOP5] {r['reject_top5']}")
        print(f"  [missed_entry] confirm2가 confirm1 대비 놓친 진입: {r['missed_entry(confirm2 vs 1)']}건")
        print(f"  {'셀(confirm|stop)':<26}{'진입':>5}{'net%':>8}{'net원':>13}{'MDD%':>7}{'whip%':>7}{'false':>6}{'풀필':>5}")
        for key, c in r["cells"].items():
            print(f"  {key:<26}{c['entries']:>5}{c['net_avg']:>8}{c['net_sum_won']:>13,}{c['mdd_avg']:>7}{c['whipsaw_pct']:>7}{c['false_entry']:>6}{c['full_filled']:>5}")
    print("\n※ thin(16일·단일regime) → 절대수치 X, 방향·셀간 상대비교만. forward 누적이 본검증. flip 금지.")


if __name__ == "__main__":
    main()
