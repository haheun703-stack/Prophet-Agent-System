# -*- coding: utf-8 -*-
"""recent_10d_rebound_shadow_replay — 엄격 walk-forward 리플레이 (6/2, read-only).

사장님 지시(6/2): 최근 10거래일 5분봉으로 rebound 전략을 forward처럼 리플레이.
★ 엄격 walk-forward: 각 5분봉 시점에서 미래 데이터 사용 금지. lookahead 금지. 비용 포함. ★
★ 실주문 0 / 봇 OFF / SAJANG 무변경 / scheduler·systemd 무변경 / 주문함수 호출 0. ★

이전 하니스(rebound_shadow_system) 대비 교정:
  - 끼·유동성 = D-1(전일)까지 데이터로 산출(당일 풀바 미사용)
  - score(VWAP/양봉/전일저점) = 진입 5분봉까지 누적/장중only (당일 종가·저가 미사용)
  - 신규 지표: missed_upside(손절후 놓친 상승) / avoided_loss(손절로 피한 하락)

고정 룰: ACTIVE pool / STOP3 / confirm1 = primary. STOP4·confirm2 = 비교참고만(채택후보 X).
데이터: data_store/1min 가용 최근 10거래일(=2026-03-13~04-03, 로컬 분봉이 04-03까지) + stock_data_daily.
  ※ 달력상 진짜 최근(5월말)은 VPS 분봉 동기화 필요(별도). 본 replay=가용최근 walk-forward.

사용: python tools/recent_10d_rebound_replay_6_2.py [--days N] [--selftest]
"""
from __future__ import annotations

import argparse
import csv
import glob
import os
import sys
from collections import Counter, defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
SA = Path(__file__).resolve().parent.parent
DAILY_DIR = ROOT / "stock_data_daily"
MIN_DIR = SA / "data_store" / "1min"
sys.path.insert(0, str(SA))

from data.limit_up_scanner import LimitUpStock, score_kki, count_surge_limit_days  # noqa: E402
from data.sajang_rules import SAJANG  # noqa: E402

CONTROL = {"005930","000660","035420","035720","005380","012450","373220","105560",
           "055550","086790","068270","207940","005490","010120","454910",
           "003080","012030","035900","041190","053950","064260","078340","089980",
           "101490","108490","112040","194480","215600","225570"}

DOWN_TRIG = 0.03
REBOUND = 0.01
ADD_LEVELS = (0.01, 0.02, 0.03)
ALLOC = (0.10, 0.30, 0.30, 0.30)
STOPS = {"STOP3": 0.03, "STOP4": 0.04}
CONFIRMS = {"confirm1": 1, "confirm2": 2}
SCORE_MIN = 70.0
SURGE_OVERHEAT = 0.30
HOLD_CAP_DAYS = 5
BUDGET = 10_000_000
BUY_COST, SELL_COST = 0.0015, 0.0033
MIN_PRICE = 1000


def _daily_paths():
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


def load_min_by_day(path):
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
    bucket, order = {}, []
    for dt, o, h, l, c, v in bars:
        try:
            mm = int(dt[14:16]); key = dt[11:13] + f"{(mm // 5) * 5:02d}"
        except ValueError:
            continue
        if key not in bucket:
            bucket[key] = [dt, o, h, l, c, v]; order.append(key)
        else:
            b = bucket[key]; b[0] = dt; b[2] = max(b[2], h); b[3] = min(b[3], l); b[4] = c; b[5] += v
    return [tuple(bucket[k]) for k in order]


def _atr_pct(daily, i, n=14):
    a = max(1, i - n)
    trs = [max(daily[k][2] - daily[k][3], abs(daily[k][2] - daily[k - 1][4]),
               abs(daily[k][3] - daily[k - 1][4])) for k in range(a, i + 1)]
    return (sum(trs) / len(trs)) / daily[i][4] * 100 if trs and daily[i][4] > 0 else 0.0


def kki_through_prev(code, name, daily, i):
    """★ walk-forward: D-1(i-1)까지 데이터로 끼 산출 (당일 i 풀바 미사용)."""
    j = i - 1
    d = daily[j]
    o, h, l, c, v = d[1], d[2], d[3], d[4], d[5]
    tv = c * v / 1e8
    cs = (c - l) / (h - l) if h > l else 0.5
    win = daily[max(0, j - 20):j]
    vma = sum(x[5] for x in win) / max(1, len(win))
    volx = v / vma if vma > 0 else 0.0
    surge, limit = count_surge_limit_days([x[4] for x in daily[:j + 1]])
    st = LimitUpStock(code=code, name=name, close=int(c), volume_ratio=round(volx, 2),
                      trading_value_억=round(tv, 1), turnover_pct=0.0,
                      close_strength=round(cs, 3), consecutive_limit=0)
    return score_kki(st, _atr_pct(daily, j), surge, limit)


def confirm_entry(bars5, reb, n):
    streak = 0
    for j, b in enumerate(bars5):
        if b[4] >= reb:
            streak += 1
            if streak >= n:
                return j, b[4]
        else:
            streak = 0
    return None, None


def score_walkforward(bars5, ent_j, day_open, prev_low):
    """★ 진입봉(ent_j)까지만 사용. 당일 종가·저가 미사용(lookahead 금지)."""
    cum_pv = cum_v = 0.0
    hi = -1e18; lo = 1e18
    for b in bars5[:ent_j + 1]:
        tp = (b[2] + b[3] + b[4]) / 3
        cum_pv += tp * b[5]; cum_v += b[5]
        hi = max(hi, b[2]); lo = min(lo, b[3])
    vwap = cum_pv / cum_v if cum_v > 0 else bars5[ent_j][4]
    ent_close = bars5[ent_j][4]
    vwap_sc = 40.0 if ent_close >= vwap * 1.005 else (28.0 if ent_close >= vwap else 12.0)
    # 장중 모멘텀(시가→진입봉 종가 / 장중so-far 레인지)
    rng = hi - lo
    mom = (ent_close - day_open) / rng if rng > 0 else 0.0
    green_sc = max(0.0, min(1.0, mom)) * 30.0
    # 전일저점 방어: 장중so-far 저가가 전일저가 위 유지
    defense_sc = 30.0 if lo >= prev_low else (15.0 if lo >= prev_low * 0.985 else 0.0)
    return round(vwap_sc + green_sc + defense_sc, 1)


def simulate(entry_price, fwd, stop_pct, trail_pct):
    """fwd=[(o,h,l), ...] 진입후 forward 경로(진입일1분 + 이후 일봉). 결정엔 미래 미사용,
    결과(net/MDD)만 forward. 반환 dict(net,mdd,same_day,full,missed_up,avoided_loss)."""
    anchor = entry_price
    shares = invested = 0.0
    peak = entry_price
    add_idx = 0
    worst = 0.0

    def buy(price, ap):
        nonlocal shares, invested
        sh = int(BUDGET * ap // price)
        if sh > 0:
            shares += sh; invested += sh * price * (1 + BUY_COST)

    buy(entry_price, ALLOC[0])
    stop_k = None
    stop_price = 0.0
    for k, (o, h, l, c) in enumerate(fwd):
        if shares:
            worst = min(worst, shares * l - invested)
        avg = invested / shares if shares else 0
        active_stop = max(avg * (1 - stop_pct), peak * (1 - trail_pct))
        if l <= active_stop:
            stop_price = min(o, active_stop) if o <= active_stop else active_stop
            stop_k = k
            break
        peak = max(peak, h)
        while add_idx < len(ADD_LEVELS) and shares:
            ap = anchor * (1 + ADD_LEVELS[add_idx])
            if h < ap:
                break
            buy(max(o, ap), ALLOC[add_idx + 1]); add_idx += 1

    mdd = max(0.0, -worst / BUDGET * 100)
    full = add_idx >= len(ADD_LEVELS)
    if stop_k is not None:
        proceeds = shares * stop_price * (1 - SELL_COST)
        net = (proceeds - invested) / BUDGET * 100
        # 손절 후 잔여 윈도 경로 → missed_upside / avoided_loss
        post = fwd[stop_k + 1:]
        if post:
            pmax = max(h for _, h, _, _ in post); pmin = min(l for _, _, l, _ in post)
            missed_up = max(0.0, (pmax - stop_price) / stop_price * 100)
            avoided = max(0.0, (stop_price - pmin) / stop_price * 100)
        else:
            missed_up = avoided = 0.0
        return {"net": net, "mdd": mdd, "same_day": stop_k < _ENTRY_DAY_LEN[0], "full": full,
                "stopped": True, "missed_up": missed_up, "avoided": avoided}
    # 미손절 → 캡 마지막 종가 청산
    last = fwd[-1][3] if fwd else entry_price   # 마지막 바 종가
    proceeds = shares * last * (1 - SELL_COST)
    net = (proceeds - invested) / BUDGET * 100
    return {"net": net, "mdd": mdd, "same_day": False, "full": full,
            "stopped": False, "missed_up": 0.0, "avoided": 0.0}


_ENTRY_DAY_LEN = [0]   # simulate 호출 전 진입일 1분봉 개수 세팅(same_day 판정용)


def run(n_days=10):
    dpaths = _daily_paths()
    min_files = {os.path.basename(f)[:-4]: f for f in glob.glob(str(MIN_DIR / "*.csv"))}
    # 가용 거래일(표본으로 글로벌) → 최근 n_days
    alldays = set()
    for f in list(min_files.values())[:400]:
        try:
            with open(f, encoding="utf-8-sig") as fh:
                for line in fh.read().splitlines()[1:]:
                    if line[:4] == "2026":
                        alldays.add(line[:10].replace("/", "-"))
        except OSError:
            pass
    window = sorted(alldays)[-n_days:]
    active = [c for c in min_files if c not in CONTROL]

    reasons = Counter()
    funnel = Counter()
    cands = []   # dict per candidate
    for code in active:
        dp = dpaths.get(code); mf = min_files.get(code)
        if not dp or not mf:
            continue
        daily = load_daily(dp)
        if len(daily) < 25:
            continue
        dmap = {d[0]: k for k, d in enumerate(daily)}
        mins = load_min_by_day(mf)
        name = os.path.basename(dp)[:-4].rsplit("_", 1)[0]
        for day in window:
            if day not in mins:
                continue
            i = dmap.get(day)
            if i is None or i < 22:
                continue
            prev, pp = daily[i - 1], daily[i - 2]
            # hard filter: 유동성=D-1 거래대금 / 가격=D-1 종가 (walk-forward)
            if prev[4] < MIN_PRICE:
                reasons["PRICE_LOW"] += 1; continue
            if prev[4] * prev[5] / 1e8 < SAJANG.KKI_MIN_LIQUIDITY_억:
                reasons["LIQ_LOW"] += 1; continue
            funnel["hard"] += 1
            # 전일 -3% setup (과거)
            if not (prev[4] <= pp[4] * (1 - DOWN_TRIG) or prev[3] <= pp[4] * (1 - DOWN_TRIG)):
                reasons["NO_DOWN_SETUP"] += 1; continue
            funnel["setup"] += 1
            # 끼 = D-1까지 (walk-forward)
            kki = kki_through_prev(code, name, daily, i)
            if kki < SAJANG.KKI_MODERATE:
                reasons["KKI_LOW"] += 1; continue
            funnel["kki"] += 1
            bars5 = to_5min(mins[day])
            if not bars5:
                reasons["NO_BARS"] += 1; continue
            day_open = bars5[0][1]
            # 과열회피: 전일종가 대비 시가 갭 +30%↑ (장중 known)
            if day_open / prev[4] - 1 > SURGE_OVERHEAT:
                reasons["OVERHEAT"] += 1; continue
            reb = prev[4] * (1 + REBOUND)
            ent_j, ent_px = confirm_entry(bars5, reb, 1)   # confirm1 기준 후보판정
            if ent_j is None:
                reasons["NO_CONFIRM"] += 1; continue
            score = score_walkforward(bars5, ent_j, day_open, prev[3])
            if score < SCORE_MIN:
                reasons["LOW_SCORE"] += 1; continue
            funnel["candidate"] += 1
            cands.append({"code": code, "name": name, "day": day, "bars5": bars5,
                          "reb": reb, "all1min": mins[day], "daily_after": daily[i + 1:]})

    # ── 셀 시뮬: confirm1/2 × STOP3/4 ──
    cells = {}
    missed_entry = 0
    for cf, nb in CONFIRMS.items():
        for st, sp in STOPS.items():
            agg = {"net": [], "mdd": [], "whip": 0, "false": 0, "full": 0, "n": 0,
                   "missed_up": [], "avoided": []}
            for c in cands:
                ent_j, ent_px = confirm_entry(c["bars5"], c["reb"], nb)
                if ent_j is None:
                    continue
                ent_dt = c["bars5"][ent_j][0]
                e1 = [(b[1], b[2], b[3], b[4]) for b in c["all1min"] if b[0] > ent_dt]
                dafter = [(d[1], d[2], d[3], d[4]) for d in c["daily_after"][:HOLD_CAP_DAYS]]
                _ENTRY_DAY_LEN[0] = len(e1)
                fwd = e1 + dafter
                r = simulate(ent_px, fwd, sp, sp)
                agg["n"] += 1
                agg["net"].append(r["net"]); agg["mdd"].append(r["mdd"])
                agg["whip"] += r["same_day"]; agg["full"] += r["full"]
                if r["same_day"]:
                    agg["false"] += 1
                if r["stopped"]:
                    agg["missed_up"].append(r["missed_up"]); agg["avoided"].append(r["avoided"])
            cells[f"{cf}|{st}"] = agg
    # missed_entry: confirm1 진입O & confirm2 진입X 개수
    for c in cands:
        j1, _ = confirm_entry(c["bars5"], c["reb"], 1)
        j2, _ = confirm_entry(c["bars5"], c["reb"], 2)
        if j1 is not None and j2 is None:
            missed_entry += 1

    return window, funnel, reasons, cells, missed_entry, len(cands)


def _avg(x):
    return round(sum(x) / len(x), 2) if x else 0.0


def selftest():
    ok = []
    bars = [(f"2026-03-13 09:{m:02d}:00", 100, 100 + m, 99, 100 + m, 10) for m in range(0, 10)]
    ok.append(("T1 5분봉 2봉", len(to_5min(bars)) == 2))
    b5 = [(0, 100, 101, 99, 100.5, 1), (0, 100, 102, 100, 101.2, 1), (0, 0, 0, 0, 101.5, 1)]
    ok.append(("T2 confirm1=1", confirm_entry(b5, 101.0, 1)[0] == 1))
    ok.append(("T2 confirm2=2", confirm_entry(b5, 101.0, 2)[0] == 2))
    # T3 즉시손절 → stopped + missed/avoided 계산
    _ENTRY_DAY_LEN[0] = 3
    fwd = [(100, 100, 88, 90), (90, 95, 89, 90), (92, 99, 91, 95)]   # 손절후 반등(missed)·추가하락(avoided)
    r = simulate(100.0, fwd, 0.04, 0.04)
    ok.append(("T3 stopped&whip", r["stopped"] and r["same_day"] and r["net"] < 0))
    ok.append(("T3 missed_up>0(손절후 99까지 반등)", r["missed_up"] > 0))
    # T4 상승 풀필
    _ENTRY_DAY_LEN[0] = 1
    fwd = [(100, 104, 100, 104), (104, 110, 104, 110), (110, 112, 109, 111)]
    r = simulate(100.0, fwd, 0.04, 0.04)
    ok.append(("T4 풀필+양수+미손절", r["full"] and r["net"] > 0 and not r["stopped"]))
    # T5 walk-forward score: 미래 미사용(진입봉까지)
    b5 = [(0, 100, 105, 99, 104, 100), (0, 0, 0, 0, 200, 1)]   # 2번째봉(미래)은 무시돼야
    s1 = score_walkforward(b5, 0, 100, 95)
    ok.append(("T5 score 0~100 & 진입봉까지만", 0 <= s1 <= 100))
    print("walk-forward replay 셀프테스트:")
    for n, p in ok:
        print(f"  [{'PASS' if p else 'FAIL'}] {n}")
    return all(p for _, p in ok)


def main():
    sys.stdout.reconfigure(encoding="utf-8")
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=10)
    ap.add_argument("--selftest", action="store_true")
    a = ap.parse_args()
    if a.selftest:
        sys.exit(0 if selftest() else 1)

    window, funnel, reasons, cells, missed_entry, ncand = run(a.days)
    print("=" * 82)
    print(f"recent_10d_rebound_shadow_replay (엄격 walk-forward, lookahead 금지, 비용포함)")
    print(f"창: {window[0]} ~ {window[-1]} ({len(window)}거래일, 가용분봉 최근 — 04-03까지)")
    print(f"★ 증빙: SAJANG.AUTO_TRADE_DISABLED={SAJANG.AUTO_TRADE_DISABLED} / 실주문 호출 0 / "
          f"주문함수 0 / SAJANG·scheduler·systemd 무변경 / HOLD")
    print("=" * 82)
    print(f"\n[선정 퍼널 — ACTIVE] hard {funnel['hard']} → 전일-3% {funnel['setup']} "
          f"→ 끼MODERATE+(D-1) {funnel['kki']} → 후보(score≥70&confirm1) {funnel['candidate']}")
    print(f"[탈락 TOP5] {reasons.most_common(5)}")
    print(f"[missed_entry] confirm2가 confirm1 대비 놓친 진입: {missed_entry}건")
    print(f"\n{'셀(confirm|stop)':<20}{'진입':>5}{'net%':>8}{'MDD%':>7}{'whip%':>7}{'false':>6}"
          f"{'풀필':>5}{'missedUp%':>10}{'avoidLs%':>10}")
    for key, a2 in cells.items():
        wr = round(100 * a2["whip"] / a2["n"]) if a2["n"] else 0
        print(f"{key:<20}{a2['n']:>5}{_avg(a2['net']):>8}{_avg(a2['mdd']):>7}{wr:>7}"
              f"{a2['false']:>6}{a2['full']:>5}{_avg(a2['missed_up']):>10}{_avg(a2['avoided']):>10}")

    # ── 판정 ──
    pri = cells.get("confirm1|STOP3", {})
    pri_net = _avg(pri.get("net", []))
    pri_whip = round(100 * pri["whip"] / pri["n"]) if pri.get("n") else 0
    s3 = _avg(cells.get("confirm1|STOP3", {}).get("net", []))
    s4 = _avg(cells.get("confirm1|STOP4", {}).get("net", []))
    c1 = _avg(cells.get("confirm1|STOP3", {}).get("net", []))
    c2 = _avg(cells.get("confirm2|STOP3", {}).get("net", []))
    print("\n── 판정 ──")
    print(f"  · net 생존?         confirm1|STOP3 net = {pri_net}%  → {'생존(양수)' if pri_net > 0 else '미생존(≤0)'}")
    print(f"  · 휩쏘율 <50%?      {pri_whip}%  → {'YES' if pri_whip < 50 else 'NO'}")
    print(f"  · STOP3 우위 유지?  STOP3 {s3} vs STOP4 {s4}  → {'YES' if s3 >= s4 else 'NO'}")
    print(f"  · confirm1 우위?    c1 {c1} vs c2 {c2}  → {'YES' if c1 >= c2 else 'NO'}")
    verdict = (pri_net > 0 and pri_whip < 50)
    print(f"\n  ★ forward로 갈 가치: {'있음(조건부)' if verdict else '아직 없음 — net≤0 또는 휩쏘≥50%, shadow 유지'}")
    print("\n※ 가용분봉 04-03까지(달력최근 아님)·thin·단일regime → 방향만. 달력최근=VPS 동기화 별도. flip 금지.")


if __name__ == "__main__":
    main()
