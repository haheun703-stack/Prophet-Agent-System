# -*- coding: utf-8 -*-
"""leader_prospective_scan — 전수 유니버스 prospective walk-forward 스캔 (6/2, read-only).

사장님 결정문(6/2): leader_momentum_ride 다음 단계. 우선순위 = exit 튜닝 X, 전수 스캔.
"강한 종목을 사전에 잘 고르는가(precision)" + "어디서 내리는 게 돈이 되나(exit)"를
★ hindsight 없이 ★ 판정. (5종 분석은 exit 감 잡기용일 뿐 — 이게 진짜 검증.)

방법:
  1. 과거 N개월 전체 유니버스를 날짜별 순회.
  2. 매일 장마감 기준 '그날까지 데이터만'으로 점화 후보 선별 (walk-forward, lookahead 금지).
  3. 후보별 fwd D+3/5/10/20 수익, MFE(최대상승), MAE(최대낙폭) 기록.
  4. exit 후보 병렬: -3/-5/-10% 트레일 · ATR · MA5 · MA10 · 전저점이탈.
  5. 점화 분리: 급등형(+10%&2.5x) / 대형완만추세형(신고가돌파+거래대금급증).
  6. precision·평균수익·MDD·휩쏘율·상위/하위 사례 보고.

★ 순수 read-only: 실주문 0 / 주문함수 0 / SAJANG 무변경 / scheduler·systemd 무변경 / 봇 OFF.
★★ 정직 한계:
  - 기관/외인 수급·섹터 동행 = 과거 전수 유니버스에 없음(키움 opt10059 대기) → 완만추세형은
    가격/거래대금 프록시. 수급·섹터 = hook(미반영). 거짓 수급 X.
  - 생존편향: stock_data_daily = 현존종목만(상폐부재) → precision·수익 과대(상대비교만).
  - 일봉 → 장중 트레일 휩쏘 과소.

사용: python tools/leader_prospective_scan_6_2.py [--months 6] [--limit N] [--selftest]
"""
from __future__ import annotations

import argparse
import csv
import glob
import os
import sys
from collections import Counter, defaultdict
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
SA = Path(__file__).resolve().parent.parent
DAILY_DIR = ROOT / "stock_data_daily"
sys.path.insert(0, str(SA))
from data.sajang_rules import SAJANG  # noqa: E402

# 탐지 파라미터
SURGE_CHG = 0.10          # 급등형 당일 +10%↑
SURGE_VOLX = 2.5          # 거래량 2.5x↑
BASE_POS_MAX = 0.60       # 직전일 60일레인지 위치 ≤0.6
STEADY_HIGH_N = 120       # 완만형 신고가 = 120일 신고가
STEADY_TURN_X = 1.5       # 거래대금 1.5x↑(20일평균)
STEADY_TURN_FLOOR = 100.0  # 거래대금 100억↑(대형/유동)
STEADY_CHG_MAX = 0.10     # 완만 = 당일 +10% 미만(급등형과 분리)

# exit·forward
FWD = [3, 5, 10, 20]
HOLD_CAP = 40
ATR_MULT = 2.5
LEADER_MFE = 0.20         # precision: fwd MFE ≥ +20% = 주도주化
BUDGET = 10_000_000
BUY_COST, SELL_COST = 0.0015, 0.0033

ETF_KW = ("KODEX", "TIGER", "RISE", "PLUS", "ACE", "SOL", "KBSTAR", "ARIRANG",
          "HANARO", "KOSEF", "KINDEX", "TIMEFOLIO", "UNICORN", "WON ", "스팩",
          "레버리지", "인버스", "선물", "채권", "ETN")


def _is_excluded(name):
    if any(k in name for k in ETF_KW):
        return True
    if name.endswith("우") or name.endswith("우B") or "우B" in name:
        return True
    return False


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


def _atr(d, i, n=14):
    a = max(1, i - n)
    trs = [max(d[k][2] - d[k][3], abs(d[k][2] - d[k - 1][4]), abs(d[k][3] - d[k - 1][4]))
           for k in range(a, i + 1)]
    return sum(trs) / len(trs) if trs else 0.0


def _ma(d, i, n):
    a = max(0, i - n + 1)
    cs = [d[k][4] for k in range(a, i + 1)]
    return sum(cs) / len(cs) if cs else d[i][4]


def detect(d, i):
    """walk-forward 점화 판별 (data[:i+1]만). 반환 'SURGE'|'STEADY'|None."""
    if i < STEADY_HIGH_N:
        return None
    cur, prev = d[i], d[i - 1]
    chg = cur[4] / prev[4] - 1 if prev[4] > 0 else 0
    vma = sum(x[5] for x in d[i - 20:i]) / 20 if i >= 20 else 0
    turn = cur[4] * cur[5] / 1e8
    turn_ma = sum(x[4] * x[5] for x in d[i - 20:i]) / 20 / 1e8 if i >= 20 else 0
    # 급등형
    win60 = d[i - 60:i]
    lo = min(x[3] for x in win60); hi = max(x[2] for x in win60)
    pos = (prev[4] - lo) / (hi - lo) if hi > lo else 0.5
    if chg >= SURGE_CHG and vma > 0 and cur[5] / vma >= SURGE_VOLX and pos <= BASE_POS_MAX:
        return "SURGE"
    # 대형 완만추세형: 120일 신고가 돌파 + 거래대금 급증·대형 + 완만(+10%미만)
    hi120 = max(x[2] for x in d[i - STEADY_HIGH_N:i])
    if (cur[4] >= hi120 and chg < STEADY_CHG_MAX and turn >= STEADY_TURN_FLOOR
            and turn_ma > 0 and turn / turn_ma >= STEADY_TURN_X):
        return "STEADY"
    return None


def fwd_metrics(d, i):
    entry = d[i][4]
    end = min(len(d), i + 1 + HOLD_CAP)
    path = d[i + 1:end]
    if not path:
        return None
    mfe = max((x[2] - entry) / entry for x in path)
    mae = min((x[3] - entry) / entry for x in path)
    fwd = {}
    for h in FWD:
        k = i + h
        fwd[h] = (d[k][4] - entry) / entry * 100 if k < len(d) else None
    return {"entry": entry, "mfe": mfe * 100, "mae": mae * 100, "fwd": fwd}


def exit_sim(d, i, policy):
    """진입=d[i] 종가. policy별 청산 → (net%, hold_days, captured%)."""
    entry = d[i][4]
    peak = entry
    atr0 = _atr(d, i)
    end = min(len(d), i + 1 + HOLD_CAP)
    mfe_px = 0.0
    exit_px = d[end - 1][4] if end - 1 > i else entry
    exit_k = end - 1
    for k in range(i + 1, end):
        x = d[k]
        peak = max(peak, x[2])
        mfe_px = max(mfe_px, x[2] - entry)
        stop = None
        if policy == "trail3":
            stop = peak * (1 - 0.03)
        elif policy == "trail5":
            stop = peak * (1 - 0.05)
        elif policy == "trail10":
            stop = peak * (1 - 0.10)
        elif policy == "atr":
            stop = peak - ATR_MULT * atr0
        if stop is not None and x[3] <= stop:
            exit_px = min(x[1], stop) if x[1] <= stop else stop; exit_k = k; break
        if policy == "ma5" and x[4] < _ma(d, k, 5):
            exit_px = x[4]; exit_k = k; break
        if policy == "ma10" and x[4] < _ma(d, k, 10):
            exit_px = x[4]; exit_k = k; break
        if policy == "prevlow":            # 전저점(직전 5일 최저 저가) 이탈
            swing_lo = min(d[j][3] for j in range(max(0, k - 5), k))
            if x[4] < swing_lo:
                exit_px = x[4]; exit_k = k; break
    shares = int(BUDGET // entry)
    net = (shares * exit_px * (1 - SELL_COST) - shares * entry * (1 + BUY_COST)) / BUDGET * 100
    real_gain = (exit_px - entry)
    # 캡처는 '의미있는 상승(≥1%)이 있었을 때만' 정의 (없으면 None=집계제외)
    if mfe_px >= entry * 0.01:
        cap = max(-100.0, min(100.0, real_gain / mfe_px * 100))
    else:
        cap = None
    return net, exit_k - i, (round(cap, 1) if cap is not None else None)


POLICIES = ["trail3", "trail5", "trail10", "atr", "ma5", "ma10", "prevlow"]


def run(months, limit):
    sys.stdout.reconfigure(encoding="utf-8")
    files = glob.glob(str(DAILY_DIR / "*.csv"))
    if limit:
        files = files[:limit]
    # 윈도 시작 (대략 months*30일 전); 종목 마지막날 기준 상대
    cutoff = (date.today() - timedelta(days=int(months * 30.4))).isoformat()

    by_type = {"SURGE": [], "STEADY": []}        # 각 후보의 fwd metrics
    exit_acc = {t: {p: {"net": [], "cap": [], "hold": []} for p in POLICIES} for t in ("SURGE", "STEADY")}
    cases = []
    n_files = 0
    for f in files:
        name = os.path.basename(f)[:-4].rsplit("_", 1)[0]
        if _is_excluded(name):
            continue
        d = load_daily(f)
        if len(d) < STEADY_HIGH_N + 25:
            continue
        n_files += 1
        # 후보 탐지(윈도 내) — 단, fwd 20일 확보 위해 끝에서 21일 전까지만 진입
        for i in range(STEADY_HIGH_N, len(d) - 21):
            if d[i][0] < cutoff:
                continue
            t = detect(d, i)
            if not t:
                continue
            fm = fwd_metrics(d, i)
            if not fm:
                continue
            by_type[t].append(fm)
            for p in POLICIES:
                net, hold, cap = exit_sim(d, i, p)
                exit_acc[t][p]["net"].append(net)
                if cap is not None:
                    exit_acc[t][p]["cap"].append(cap)
                exit_acc[t][p]["hold"].append(hold)
            cases.append((t, name, d[i][0], round(fm["mfe"], 1), round(fm["mae"], 1),
                          fm["fwd"][20]))
    return by_type, exit_acc, cases, n_files, cutoff


def _avg(x):
    return round(sum(x) / len(x), 2) if x else 0.0


def _mdd_of(nets):  # 후보들 net 중 최악(단순 분포 하한)
    return round(min(nets), 2) if nets else 0.0


def selftest():
    ok = []
    # 급등형 합성: 130일 기지(flat) → idx130 +12%&6x 점화 → 20일 forward
    d2 = [(f"2025d{k:03d}", 100, 101, 99, 100, 1000) for k in range(130)]
    d2.append(("2025-06-01", 100, 113, 100, 112, 6000))                 # idx130 점화
    for k in range(20):                                                 # forward 상승후하락
        c = 112 + (8 - k) * 2
        d2.append((f"2025f{k:03d}", c, c + 2, c - 3, c, 1500))
    ok.append(("T1 SURGE 탐지(idx130)", detect(d2, 130) == "SURGE"))
    ms = fwd_metrics(d2, 130)
    ok.append(("T2 fwd_metrics MFE/MAE", ms is not None and ms["mfe"] > 0))
    net, hold, cap = exit_sim(d2, 130, "trail3")
    ok.append(("T3 exit trail3 동작", isinstance(net, float) and hold > 0))
    # 완만형 합성: 120일 완만상승 → idx120 신고가+거래대금급증(+5%, 완만)
    px = 100.0; d3 = []
    for k in range(120):
        px *= 1.002
        d3.append((f"s{k:03d}", px, px * 1.005, px * 0.997, px, 1000))
    hi = max(x[2] for x in d3)
    d3.append(("steady", hi, hi * 1.05, hi * 0.99, hi * 1.04, 100000000000))  # 신고가+대형거래대금
    ok.append(("T4 STEADY 탐지", detect(d3, 120) == "STEADY"))
    ok.append(("T5 ETF/우 제외 & 일반주 통과",
               _is_excluded("KODEX 200") and _is_excluded("삼성전자우") and not _is_excluded("삼성전자")))
    print("prospective scan 셀프테스트:")
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

    by_type, exit_acc, cases, n_files, cutoff = run(a.months, a.limit)
    print("=" * 96)
    print(f"leader_prospective_scan — 전수 walk-forward (hindsight 없음, 최근 {a.months:g}개월 {cutoff}~)")
    print(f"스캔 종목 {n_files} (ETF/우/스팩 제외) / 후보 SURGE {len(by_type['SURGE'])} · STEADY {len(by_type['STEADY'])}")
    print(f"★ 증빙: SAJANG.AUTO_TRADE_DISABLED={SAJANG.AUTO_TRADE_DISABLED} / 실주문 0 / 주문함수 0 / SAJANG·scheduler·systemd 무변경")
    print("=" * 96)
    for t in ("SURGE", "STEADY"):
        fms = by_type[t]
        if not fms:
            print(f"\n■ {t}: 후보 0"); continue
        prec = 100 * sum(1 for m in fms if m["mfe"] >= LEADER_MFE * 100) / len(fms)
        prec10 = 100 * sum(1 for m in fms if m["mfe"] >= 10) / len(fms)
        print(f"\n■ {t} 점화 후보 {len(fms)}건")
        print(f"  [선별력 precision] fwd MFE≥+20% = {prec:.0f}%  (≥+10% = {prec10:.0f}%)  "
              f"= '강한 종목 사전선별' 지표")
        print(f"  [fwd 수익 평균] D+3 {_avg([m['fwd'][3] for m in fms if m['fwd'][3] is not None])}% "
              f"D+5 {_avg([m['fwd'][5] for m in fms if m['fwd'][5] is not None])}% "
              f"D+10 {_avg([m['fwd'][10] for m in fms if m['fwd'][10] is not None])}% "
              f"D+20 {_avg([m['fwd'][20] for m in fms if m['fwd'][20] is not None])}%")
        print(f"  [MFE/MAE 평균] MFE +{_avg([m['mfe'] for m in fms])}% / MAE {_avg([m['mae'] for m in fms])}%")
        print(f"  {'exit':<10}{'평균net%':>9}{'평균캡처%':>10}{'평균보유':>8}{'최악net%(MDD대용)':>18}{'승률%':>7}")
        for p in POLICIES:
            nets = exit_acc[t][p]["net"]
            wr = 100 * sum(1 for x in nets if x > 0) / len(nets) if nets else 0
            print(f"  {p:<10}{_avg(nets):>9}{_avg(exit_acc[t][p]['cap']):>10}"
                  f"{_avg(exit_acc[t][p]['hold']):>8}{_mdd_of(nets):>18}{wr:>7.0f}")
    # 상위/하위 사례
    cases_v = [c for c in cases if c[5] is not None]
    cases_v.sort(key=lambda c: c[5], reverse=True)
    print("\n[상위 D+20 사례]")
    for c in cases_v[:6]:
        print(f"  {c[0]:<7}{c[1]:<14}{c[2]} MFE+{c[3]}% MAE{c[4]}% D+20 {c[5]:.1f}%")
    print("[하위 D+20 사례]")
    for c in cases_v[-6:]:
        print(f"  {c[0]:<7}{c[1]:<14}{c[2]} MFE+{c[3]}% MAE{c[4]}% D+20 {c[5]:.1f}%")
    print("\n★★ 정직: 수급·섹터 미반영(완만형=가격/거래대금 프록시, hook) / 생존편향(상폐부재→precision·수익 과대, 상대만) / 일봉 트레일 휩쏘 과소.")
    print("   목표 답: precision=사전선별력 / exit표=어디서 내려야 돈. 단 절대수치 X, 상대·방향. flip 금지·봇 OFF.")


if __name__ == "__main__":
    main()
