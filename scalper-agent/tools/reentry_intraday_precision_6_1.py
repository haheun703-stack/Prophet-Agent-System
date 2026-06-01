#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""라이브 숙제 #2 — 재진입 분봉정밀(intraday) 교차검증.

#1(reentry_cost_sensitivity_6_1)은 일봉 근사: 트레일peak=일봉high, 손절트리거=일봉low,
재진입=일봉close. 이건 장중 '순서'(딥→랠리 등)를 뭉갠다 + 슬리피지 미반영.
#2는 같은 STOP_REENTER 규칙을 1분봉으로 정밀 재현 → 일봉근사가 헤드룸을 과대/과소평가하나.

핵심 질문(paper go의 #2 조각):
  일봉근사 5.0배 헤드룸(#1)이 ① 장중 순서 정밀화 ② 슬리피지 반영 후에도 ≥2배로 견고한가?
  장중 whipsaw가 더 심하면(분봉서 세그먼트↑) 헤드룸 축소 → 비용차원 재검.

데이터: stock_data_daily(일봉, 이벤트 검출) + data_store/1min(홀딩윈도 정밀 재현).
  분봉 커버리지 2026-02~03(~6주) → 윈도 내 이벤트만 = thin sample(방향성 교차검증용).

실행:
  python tools/reentry_intraday_precision_6_1.py            # 실데이터 (노트북/VPS)
  python tools/reentry_intraday_precision_6_1.py --selftest # 합성 로직검증(데이터 불필요)

정직: thin sample(분봉 6주·단일 regime) + 일봉 진입가/분봉 정합 가정 → 절대수치 X, 일봉근사 대비 '상대 변화'만 신뢰.
"""
from __future__ import annotations
import csv
import glob
import os
import sys
from pathlib import Path

# 일봉 계약 재사용(#1의 FIX된 로더/이벤트/시뮬과 apples-to-apples)
sys.path.insert(0, str(Path(__file__).resolve().parent))
from reentry_cost_sensitivity_6_1 import (  # noqa: E402
    _load, _events, _sim_gross, _breakeven, TRAIL, HOLD_K, LIVE_COST,
)

ROOT = Path(__file__).resolve().parent.parent.parent
DAILY_DIR = ROOT / "stock_data_daily"
MIN_DIR = ROOT / "scalper-agent" / "data_store" / "1min"

SLIP_GRID = (0.0, 0.001, 0.002)   # 레그당 편도 슬리피지(분봉 체결 가정): 0 / 0.1% / 0.2%


def _load_min(path):
    """1분봉 CSV → [(date10, datetime, o,h,l,c)]. 헤더 datetime/open.. (utf-8-sig·다중키 방어)."""
    bars = []
    try:
        with open(path, encoding="utf-8-sig") as f:
            for r in csv.DictReader(f):
                dt = (r.get("datetime") or r.get("날짜") or r.get("date")
                      or next(iter(r.values()), ""))
                try:
                    o = float(r["open"]); h = float(r["high"])
                    l = float(r["low"]); c = float(r["close"])
                except (KeyError, ValueError):
                    continue
                if o > 0 and c > 0 and dt:
                    bars.append((str(dt)[:10], str(dt), o, h, l, c))
    except Exception:
        return []
    return bars


def _sim_intraday(entry, bars, end_close, reenter, slip, mode="intraday"):
    """STOP_REENTER를 1분봉으로 정밀 재현. 손절정밀은 동일, 재진입 규율만 mode로 분리.

    손절: 트레일peak=running 분봉high, peak*(1-TRAIL)을 분봉low가 닿으면 체결(slip 편도).
    재진입 mode:
      'intraday' = 장중 고가가 직전손절선 회복하면 즉시(공격적, 하루 여러번 가능).
      'eod'      = 일봉 규칙 미러 — 그날 마지막 분봉 종가가 직전손절선 회복시 1회/일만.
    → 두 mode 비교로 '헤드룸 붕괴가 손절정밀 vs 재진입과공격 중 무엇 탓인지' 분리.
    반환: (gross%, 세그먼트수).
    """
    n = len(bars)
    gross = 0.0
    seg = 0
    in_pos = True
    p_in = entry
    hw = entry
    last_stop = entry * (1 - TRAIL)
    for i, (_d, _dt, o, h, l, c) in enumerate(bars):
        if in_pos:
            hw = max(hw, h)
            stop = hw * (1 - TRAIL)
            last_stop = stop
            if l <= stop:                       # 손절(분봉 저가가 트레일 닿음)
                fill = stop * (1 - slip)
                gross += (fill / p_in - 1)
                seg += 1
                in_pos = False
        elif reenter:
            is_eod = (i == n - 1) or (bars[i + 1][0] != _d)
            if mode == "intraday" and h >= last_stop:
                fill = last_stop * (1 + slip)
            elif mode == "eod" and is_eod and c >= last_stop:
                fill = c * (1 + slip)            # 일봉 미러: 종가 재진입
            else:
                continue
            in_pos = True
            p_in = fill
            hw = fill
    if in_pos:
        gross += (end_close / p_in - 1)
        seg += 1
    return gross * 100, seg


def _headroom(re_g, re_s, on_g, on_s):
    """vs ONCE break-even fraction + 헤드룸 배수(현 LIVE_COST 대비)."""
    be = _breakeven(re_g - on_g, re_s - on_s)
    return be, (be / LIVE_COST if be else None)


# ───────────────────────── 실데이터 실행 ─────────────────────────

def _run_real():
    min_files = {os.path.basename(f).replace(".csv", ""): f
                 for f in glob.glob(str(MIN_DIR / "*.csv"))}
    if not min_files:
        print(f"[!] 분봉 데이터 없음: {MIN_DIR}\n    실데이터는 노트북/VPS 에 있음. 로직검증은 --selftest.",
              file=sys.stderr)
        return 2
    # 분봉 종목코드 → 일봉파일 매칭(일봉 파일명은 name_code.csv 또는 code.csv)
    daily_files = glob.glob(str(DAILY_DIR / "*.csv"))

    d_agg = []   # 일봉근사 (분봉보유·윈도내 이벤트만)
    i_intra = {s: [] for s in SLIP_GRID}   # 분봉 재진입=장중공격 (rg, rs)
    i_eod = {s: [] for s in SLIP_GRID}     # 분봉 재진입=EOD종가 1회/일 (rg, rs)
    i_once = {s: [] for s in SLIP_GRID}    # 분봉 ONCE 재진입X (og, os)
    n_ev = 0
    for f in daily_files:
        code = os.path.basename(f).replace(".csv", "").split("_")[-1]
        if code not in min_files:
            continue
        rows, dates = _load(f)
        if len(rows) < 30:
            continue
        mbars = _load_min(min_files[code])
        if not mbars:
            continue
        mdates = sorted({b[0] for b in mbars})
        if not mdates:
            continue
        mlo, mhi = mdates[0], mdates[-1]
        for e in _events(rows):
            end = min(e + HOLD_K, len(rows) - 1)
            win_dates = [dates[d] for d in range(e + 1, end + 1)]
            if not win_dates:
                continue
            # 홀딩윈도 전체가 분봉 커버리지 내여야 정밀 비교 가능
            if not (mlo <= win_dates[0] and win_dates[-1] <= mhi):
                continue
            wset = set(win_dates)
            wbars = [b for b in mbars if b[0] in wset]
            if not wbars:
                continue
            d = _sim_gross(rows, e)
            if d is None:
                continue
            entry = rows[e][3]
            end_close = wbars[-1][5]
            n_ev += 1
            d_agg.append(d)
            for slip in SLIP_GRID:
                rg, rs = _sim_intraday(entry, wbars, end_close, True, slip, "intraday")
                eg, es = _sim_intraday(entry, wbars, end_close, True, slip, "eod")
                og, os_ = _sim_intraday(entry, wbars, end_close, False, slip)
                i_intra[slip].append((rg, rs))
                i_eod[slip].append((eg, es))
                i_once[slip].append((og, os_))

    if n_ev == 0:
        print("[!] 분봉윈도 내 비교가능 이벤트 0건", file=sys.stderr)
        return 2

    print(f"분봉정밀 표본: {n_ev} 이벤트 (분봉보유 & 홀딩윈도 전체 커버)\n")

    def avg(lst, idx):
        return sum(x[idx] for x in lst) / len(lst)

    d_re_g = sum(x["re_g"] for x in d_agg) / n_ev
    d_re_s = sum(x["re_s"] for x in d_agg) / n_ev
    d_on_g = sum(x["on_g"] for x in d_agg) / n_ev
    d_on_s = sum(x["on_s"] for x in d_agg) / n_ev
    d_be, d_hr = _headroom(d_re_g, d_re_s, d_on_g, d_on_s)

    print("[A] 세그먼트(왕복) 비교 — 장중 whipsaw가 더 심한가 (slip0 기준):")
    print(f"  {'방식':<22}{'RE세그':>8}{'ON세그':>8}")
    print(f"  {'일봉근사':<20}{d_re_s:>8.2f}{d_on_s:>8.2f}")
    print(f"  {'분봉 재진입=장중':<19}{avg(i_intra[0.0],1):>8.2f}{avg(i_once[0.0],1):>8.2f}")
    print(f"  {'분봉 재진입=EOD종가':<18}{avg(i_eod[0.0],1):>8.2f}{avg(i_once[0.0],1):>8.2f}")
    print()

    print("[B] vs ONCE 헤드룸 (break-even/leg, 현 0.48% 대비 배수):")
    print(f"  {'방식':<22}{'RE_g%':>8}{'ON_g%':>8}{'BE/leg':>10}{'헤드룸':>9}")
    print(f"  {'일봉근사':<20}{d_re_g:>8.2f}{d_on_g:>8.2f}"
          f"{(d_be*100 if d_be else 0):>9.2f}%{(format(d_hr,'.1f')+'x' if d_hr else 'n/a'):>9}")

    def _row(label, re_lst, slip):
        rg = avg(re_lst[slip], 0); rs = avg(re_lst[slip], 1)
        og = avg(i_once[slip], 0); os_ = avg(i_once[slip], 1)
        be, hr = _headroom(rg, rs, og, os_)
        print(f"  {label:<20}{rg:>8.2f}{og:>8.2f}"
              f"{(be*100 if be else 0):>9.2f}%{(format(hr,'.1f')+'x' if hr else 'n/a'):>9}")
        return hr

    hr_intra = {}
    hr_eod = {}
    for slip in SLIP_GRID:
        tag = format(slip * 100, ".1f") + "%"
        hr_intra[slip] = _row("분봉 장중재진입 s" + tag, i_intra, slip)
        hr_eod[slip] = _row("분봉 EOD재진입 s" + tag, i_eod, slip)
    print()

    print("판정 (일봉근사 vs 분봉정밀):")
    print(f"  · 일봉근사 헤드룸 {d_hr:.1f}x (= #1 재현)")
    hi0, he0, he2 = hr_intra[0.0], hr_eod[0.0], hr_eod[0.002]
    if hi0 is not None:
        print(f"  · 장중 재진입(slip0): {hi0:.1f}x — 손절+재진입 둘 다 분봉정밀 → "
              f"{'붕괴' if hi0 < 2 else '유지'}(장중 whipsaw {avg(i_intra[0.0],1):.0f}세그)")
    if he0 is not None:
        print(f"  · EOD 재진입(slip0): {he0:.1f}x — 손절만 분봉정밀+재진입은 일봉규율 → "
              f"{'붕괴' if he0 < 2 else '유지'}")
    if he2 is not None:
        print(f"  · EOD 재진입(slip0.2% 실집행): {he2:.1f}x → "
              f"{'≥2배 견고' if he2 >= 2 else '<2배 재검'}")
    print("\n해석: ①EOD도 붕괴면 = 장중 '손절'이 일봉보다 자주 터짐(엣지 자체 위협). "
          "②EOD는 유지인데 장중만 붕괴면 = 재진입을 EOD/쿨다운으로 규율해야(naive 장중재진입 금지).")
    print("★ thin sample(분봉 ~6주·단일 regime) → 절대수치 X, 일봉근사 대비 '상대 변화'·mode 간 비교만 신뢰.")
    return 0


# ───────────────────────── 합성 셀프테스트 ─────────────────────────

def _mk_min(seq):
    """[(o,h,l,c)] → 분봉 bars [(date,dt,o,h,l,c)] (단일일 09:01~)."""
    bars = []
    for i, (o, h, l, c) in enumerate(seq):
        t = f"2026-03-02 {9 + i//60:02d}:{i % 60:02d}:00"
        bars.append(("2026-03-02", t, float(o), float(h), float(l), float(c)))
    return bars


def _selftest():
    checks = []

    # T1: 단조 하락 → 1회 손절(재진입X), RE=ON=1세그
    seq = [(100, 100, 99, 99), (99, 99, 96, 96), (96, 96, 90, 90)]
    b = _mk_min(seq)
    rg, rs = _sim_intraday(100, b, 90, True, 0.0)
    og, os_ = _sim_intraday(100, b, 90, False, 0.0)
    # entry100 trail=97; bar2 low96<=97 손절 @97. 이후 회복 안함 → RE도 1세그.
    checks.append(("T1 단조하락 RE=ON=1세그", rs == 1 and os_ == 1))
    checks.append(("T1 손절@97 gross=-3%", abs(rg - (-3.0)) < 1e-6))

    # T2: 장중 순서 — 손절 후 같은날 신고가 회복 → 재진입(RE 2세그), ONCE는 1세그
    #   peak100→딥96(손절@97)→회복100→신고가105 종가104
    seq = [(100, 100, 100, 100), (100, 100, 96, 98), (98, 100, 98, 100),
           (100, 105, 100, 104)]
    b = _mk_min(seq)
    rg, rs = _sim_intraday(100, b, 104, True, 0.0)
    og, os_ = _sim_intraday(100, b, 104, False, 0.0)
    checks.append(("T2 재진입 RE=2세그", rs == 2))
    checks.append(("T2 ONCE=1세그(재진입X)", os_ == 1))
    checks.append(("T2 RE_g>ON_g(재진입이 회복분 회수)", rg > og))

    # T3: 슬리피지 → net 악화(같은 경로서 slip↑ → gross↓)
    rg0, _ = _sim_intraday(100, b, 104, True, 0.0)
    rg2, _ = _sim_intraday(100, b, 104, True, 0.002)
    checks.append(("T3 slip↑→gross↓", rg2 < rg0))

    # T4: 손절 없음(계속 상승) → RE=ON=1세그, gross=end-entry
    seq = [(100, 101, 100, 101), (101, 103, 101, 103), (103, 106, 103, 106)]
    b = _mk_min(seq)
    rg, rs = _sim_intraday(100, b, 106, True, 0.0)
    checks.append(("T4 무손절 1세그 gross=+6%", rs == 1 and abs(rg - 6.0) < 1e-6))

    # T5: _load_min 실데이터 헤더(utf-8-sig·datetime) 파싱 — 임시파일
    import tempfile
    tmp = Path(tempfile.gettempdir()) / "_min_selftest_6_1.csv"
    tmp.write_text("﻿datetime,open,high,low,close,volume\n"
                   "2026-03-02 09:01:00,100,101,99,100,5\n"
                   "2026-03-02 09:02:00,100,100,97,97,8\n", encoding="utf-8")
    mb = _load_min(str(tmp))
    checks.append(("T5 분봉 BOM/datetime 파싱", len(mb) == 2 and mb[0][0] == "2026-03-02"))
    try:
        tmp.unlink()
    except Exception:
        pass

    print("분봉정밀 셀프테스트:")
    ok = 0
    for name, p in checks:
        print(f"  [{'PASS' if p else 'FAIL'}] {name}")
        ok += bool(p)
    print(f"\n결과: {ok}/{len(checks)} PASS")
    return 0 if ok == len(checks) else 1


def main(argv=None):
    import argparse
    ap = argparse.ArgumentParser(description="재진입 분봉정밀 교차검증 (라이브 숙제 #2)")
    ap.add_argument("--selftest", action="store_true", help="합성 로직검증(데이터 불필요)")
    args = ap.parse_args(argv)
    if args.selftest:
        return _selftest()
    return _run_real()


if __name__ == "__main__":
    raise SystemExit(main())
