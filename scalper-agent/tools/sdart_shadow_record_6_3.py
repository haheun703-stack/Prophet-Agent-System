# -*- coding: utf-8 -*-
"""sdart_shadow_record — STEADY_S_DART_RIDE forward shadow 기록 (6/3, read-only).

사장님 결정(6/3): 확인포인트 5개 '넓게 병행 기록' (단일 확정 X, forward 데이터로 결정).
  - 진입: T0 종가 + T+1 시가 둘 다
  - 손절: -8% / -10% / ATR 셋 다
  - 청산: ma10 / D+10 / 구조이탈 셋 다
  - 유니버스: 시총컷 고정 X → cap_억 원값 기록
  - S급: 매출대비 10% 고정 X → ratio 원값 기록

★ 순수 read-only(매매): 실주문 0 / 주문함수 0 / picks·asset_pool 불변 / SAJANG 무변경(상수추가 X)
  / scheduler·systemd 무변경(승인 전 배선 금지) / 봇 OFF. record_order_intent 없음(주문 자체 없음).
  단 --asof 스캔은 DART 본문 조회(네트워크)+dart_docs 캐시 쓰기 발생(매매·picks와 무관). selftest는 네트워크 0.
★★ shadow=기록만. 5개 확정값은 forward ≥10~20건 + 설계서 5장 숙제 후 사장님 승인 → SAJANG 박기.
★★★ 의존(전부 read-only·검증완료): leader_prospective_scan(detect/STEADY)·leader_catalyst_deep(grade_disclosure).

사용: python tools/sdart_shadow_record_6_3.py [--asof YYYY-MM-DD] [--rehearsal] [--max N] [--selftest]
"""
from __future__ import annotations

import argparse
import glob
import json
import os
import re
import sys
from collections import Counter, defaultdict
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
SA = Path(__file__).resolve().parent.parent
TOOLS = Path(__file__).resolve().parent
DAILY_DIR = ROOT / "stock_data_daily"
SHADOW_DIR = SA / "data_store" / "shadow"
sys.path.insert(0, str(SA)); sys.path.insert(0, str(TOOLS))

from data.sajang_rules import SAJANG  # noqa: E402
from leader_prospective_scan_6_2 import (  # noqa: E402
    detect, load_daily, _is_excluded, _ma, _atr, STEADY_HIGH_N)
from leader_catalyst_deep_6_2 import (  # noqa: E402
    grade_disclosure, fetch_doc, is_pos, _key)
from leader_catalyst_extend_6_2 import fetch_list_resumable, load_universe  # noqa: E402

CAT_LOOKBACK = 3
HOLD_CAP = 40
ATR_MULT = 2.5
GRADE_RANK = {"S": 3, "A": 2, "B": 1, "제외": 0, "없음": -1}
STOP_PCTS = (8.0, 10.0)        # 손절 후보(전용 트레일, -3% 단타룰 무침범)
CAP_FLOOR_억 = 1000.0          # 별도트랙 기준(필터 아님 — 기록·태깅만)


# ── detail 문자열 → 원값 (grade_disclosure 출력과 1:1, 재계산 불일치 0) ──
def grade_values(detail):
    """grade_disclosure detail → (sales_ratio_pct, buyback_amt_억). 미추출/해당없음=None."""
    sr = bb = None
    m = re.search(r"매출대비 ([\d.]+)%", detail or "")
    if m:
        sr = float(m.group(1))
    m = re.search(r"취득 (\d+)억", detail or "")
    if m:
        bb = float(m.group(1))
    return sr, bb


# ── 넓게 병행 기록: forward 부품들 (전부 entry 기준, 데이터 끝나면 reached=False) ──
def raw_fwd(d, entry_idx, entry):
    """entry_idx 다음 거래일부터 D+h 종가 수익%. 미래없으면 None."""
    r = {}
    for h in (1, 3, 5, 10, 20):
        k = entry_idx + h
        r[f"d{h}"] = round((d[k][4] - entry) / entry * 100, 2) if k < len(d) and entry > 0 else None
    return r


def mfe_mae(d, i, entry):
    path = d[i + 1:min(len(d), i + 1 + HOLD_CAP)]
    if not path or entry <= 0:
        return None, None
    return (round(max((x[2] - entry) / entry * 100 for x in path), 2),
            round(min((x[3] - entry) / entry * 100 for x in path), 2))


def _hit(d, k, entry, px):
    return {"date": d[k][0], "ret_pct": round((px - entry) / entry * 100, 2), "reached": True}


_MISS = {"date": None, "ret_pct": None, "reached": False}


def trail_stop(d, i, entry, drop_pct):
    """고점(peak) 대비 drop_pct% 트레일 손절. entry=진입가."""
    if entry <= 0:
        return dict(_MISS)
    peak = entry
    end = min(len(d), i + 1 + HOLD_CAP)
    for k in range(i + 1, end):
        x = d[k]; peak = max(peak, x[2])
        stop = peak * (1 - drop_pct / 100.0)
        if x[3] <= stop:
            px = min(x[1], stop) if x[1] <= stop else stop
            return _hit(d, k, entry, px)
    return dict(_MISS)


def atr_stop(d, i, entry):
    atr0 = _atr(d, i)
    if entry <= 0 or atr0 <= 0:   # M-2: ATR=0이면 stop=peak 즉시손절 비현실 → 미도달 처리
        return dict(_MISS)
    peak = entry
    end = min(len(d), i + 1 + HOLD_CAP)
    for k in range(i + 1, end):
        x = d[k]; peak = max(peak, x[2])
        stop = peak - ATR_MULT * atr0
        if x[3] <= stop:
            px = min(x[1], stop) if x[1] <= stop else stop
            return _hit(d, k, entry, px)
    return dict(_MISS)


def exit_ma10(d, i, entry):
    if entry <= 0:
        return dict(_MISS)
    end = min(len(d), i + 1 + HOLD_CAP)
    for k in range(i + 1, end):
        if d[k][4] < _ma(d, k, 10):
            return _hit(d, k, entry, d[k][4])
    return dict(_MISS)


def exit_d10(d, i, entry):
    k = i + 10
    if k < len(d) and entry > 0:
        return _hit(d, k, entry, d[k][4])
    return dict(_MISS)


def exit_struct(d, i, entry):
    """구조이탈 = 직전 5거래일 최저 저가를 종가가 이탈."""
    if entry <= 0:
        return dict(_MISS)
    end = min(len(d), i + 1 + HOLD_CAP)
    for k in range(i + 1, end):
        swing_lo = min(d[j][3] for j in range(max(0, k - 5), k))
        if d[k][4] < swing_lo:
            return _hit(d, k, entry, d[k][4])
    return dict(_MISS)


def build_record(code, name, d, i, cat, uni):
    """후보 1건 → 넓게 병행 기록 dict (진입2·손절3·청산3·cap원값·ratio원값)."""
    dates = [x[0] for x in d]
    t0_close = d[i][4]
    t1_open = d[i + 1][1] if i + 1 < len(d) else None
    prev = d[i - 1][4]
    hi120 = max(x[2] for x in d[i - STEADY_HIGH_N:i])
    turnover = round(t0_close * d[i][5] / 1e8, 1)
    surge = round((t0_close / prev - 1) * 100, 2) if prev > 0 else None
    cap = (uni.get(code) or uni.get(code.zfill(6)) or {}).get("cap_억", 0)
    sector = (uni.get(code) or uni.get(code.zfill(6)) or {}).get("sector", "?")
    kind, grade, detail, off, rcept, report_nm = cat  # H-1: best는 항상 6-튜플
    sr, bb = grade_values(detail)
    # forward (데이터 허락 만큼 — 진입 2종)
    t1_idx = i + 1
    rec = {
        "code": code, "name": name, "t0_date": dates[i], "sector": sector,
        "cap_억": cap, "track": "large_mid" if cap >= CAP_FLOOR_억 else "below_floor",
        "trigger": "steady_high_breakout", "high_120d": round(hi120, 2),
        "turnover_억": turnover, "surge_pct": surge,
        "catalyst": {
            "rcept_no": rcept, "report_nm": report_nm,
            "kind": kind, "grade": grade,
            "sales_ratio_pct": sr, "buyback_amt_억": bb,
            "detail": detail, "cat_offset_T": off - i,  # T-k (0=T0)
        },
        "entry": {"t0_close": round(t0_close, 2),
                  "t1_open": round(t1_open, 2) if t1_open is not None else None},
        "raw_fwd_from_t0close": raw_fwd(d, i, t0_close),
        "raw_fwd_from_t1open": raw_fwd(d, t1_idx, t1_open) if t1_open is not None else
                               {f"d{h}": None for h in (1, 3, 5, 10, 20)},
        "would_stop": {  # entry=t0_close 기준, 고점 트레일
            "minus8": trail_stop(d, i, t0_close, 8.0),
            "minus10": trail_stop(d, i, t0_close, 10.0),
            "atr_2_5x": atr_stop(d, i, t0_close),
        },
        "would_exit": {  # entry=t0_close 기준
            "ma10": exit_ma10(d, i, t0_close),
            "d10": exit_d10(d, i, t0_close),
            "struct": exit_struct(d, i, t0_close),
        },
    }
    mfe, mae = mfe_mae(d, i, t0_close)  # M-1: HOLD_CAP(40일) 고정보유 가정 (청산무관 원값 관찰용)
    rec["mfe_pct_hold40"], rec["mae_pct_hold40"] = mfe, mae
    # D+10 확보 여부로 관측완료/대기 구분 (둘 다 '주문 없음')
    rec["status"] = ("observed(no order)" if i + 10 < len(d) else "pending_forward(no order)")
    return rec


def scan_asof(asof, max_codes=0):
    """asof일(또는 직전거래일) STEADY × 호재공시(T-3..T0) 후보 → 넓게 기록."""
    K = _key()
    discl = fetch_list_resumable(False)
    pos_idx = defaultdict(lambda: defaultdict(list))
    for r in discl:
        if is_pos(r["nm"]):
            dd = r["date"]
            if len(dd) == 8:
                ds = f"{dd[:4]}-{dd[4:6]}-{dd[6:8]}"
                pos_idx[r["code"]][ds].append((r["nm"], r["rcept"]))
    uni = load_universe()
    dpaths = {}
    for f in glob.glob(str(DAILY_DIR / "*.csv")):
        dpaths[os.path.basename(f)[:-4].split("_")[-1]] = f

    items = list(dpaths.items())
    if max_codes:
        items = items[:max_codes]
    records = []
    n_steady = 0
    for code, dp in items:
        cm = pos_idx.get(code) or pos_idx.get(code.zfill(6))
        if not cm:
            continue
        name = os.path.basename(dp)[:-4].rsplit("_", 1)[0]
        if _is_excluded(name):
            continue
        d = load_daily(dp)
        if len(d) < STEADY_HIGH_N + 2:
            continue
        dates = [x[0] for x in d]
        # asof 이하 마지막 거래일 인덱스 (lookahead 없음)
        i = next((j for j in range(len(d) - 1, -1, -1) if dates[j] <= asof), None)
        if i is None or i < STEADY_HIGH_N:
            continue
        if detect(d, i) != "STEADY":
            continue
        n_steady += 1
        # T-3..T0 호재공시 최강등급 (등급=없음이면 후보 아님)
        best = ("없음", "없음", "", 0, None, None)
        for k in range(max(0, i - CAT_LOOKBACK), i + 1):
            for nm, rc in cm.get(dates[k], []):
                kind, g, det = grade_disclosure(nm, fetch_doc(rc, K) if rc else "")
                if GRADE_RANK.get(g, -1) > GRADE_RANK.get(best[1], -1):
                    best = (kind, g, det, k, rc, nm)
        if best[1] == "없음":
            continue
        records.append(build_record(code, name, d, i, best, uni))
    return records, n_steady


def write_shadow(asof, records):
    SHADOW_DIR.mkdir(parents=True, exist_ok=True)
    out = SHADOW_DIR / f"sdart_{asof}.json"
    payload = {
        "asof": asof, "track": "STEADY_S_DART_RIDE", "mode": "shadow(no order)",
        "record_basis": "wide(진입2·손절3·청산3·cap원값·ratio원값) — forward로 5개 확정",
        "auto_trade_disabled": bool(SAJANG.AUTO_TRADE_DISABLED),
        "count": len(records), "records": records,
    }
    out.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return out


# ─────────────────────── selftest (경계 — 합성데이터, 네트워크 0) ───────────────────────
def selftest():
    ok = []
    # 합성: 120일 완만상승 → idx120 신고가+대형거래대금(STEADY) → 이후 forward
    px = 100.0
    d = []
    for k in range(120):
        px *= 1.002
        d.append((f"2026-01-{k:03d}", px, px * 1.005, px * 0.997, px, 1000))
    hi = max(x[2] for x in d)
    d.append(("2026-05-01", hi, hi * 1.05, hi * 0.99, hi * 1.04, 100_000_000_000))  # STEADY i=120
    base = hi * 1.04
    for k in range(25):  # forward: 상승 후 하락 (트레일/청산 트리거)
        c = base * (1 + (10 - k) * 0.01)
        d.append((f"2026-05f{k:03d}", c, c * 1.01, c * 0.985, c, 2000))
    ok.append(("T1 STEADY 탐지(i=120)", detect(d, 120) == "STEADY"))
    entry = d[120][4]
    s8 = trail_stop(d, 120, entry, 8.0)
    ok.append(("T2 -8% 트레일 손절 도달", s8["reached"] is True))
    e10 = exit_d10(d, 120, entry)
    ok.append(("T3 D+10 청산 ret 계산", e10["reached"] is True and isinstance(e10["ret_pct"], float)))
    em = exit_ma10(d, 120, entry)
    ok.append(("T4 ma10 청산 동작", isinstance(em, dict) and "reached" in em))
    rf = raw_fwd(d, 120, entry)
    ok.append(("T5 raw_fwd D+10 키존재", "d10" in rf and rf["d10"] is not None))
    # detail 원값 파싱 (grade_disclosure 출력 1:1)
    sr, bb = grade_values("매출대비 12.3%")
    ok.append(("T6 ratio 원값 12.3", sr == 12.3 and bb is None))
    sr2, bb2 = grade_values("취득 600억")
    ok.append(("T7 자사주 원값 600억", bb2 == 600.0 and sr2 is None))
    sr3, bb3 = grade_values("금액/매출 미추출")
    ok.append(("T8 미추출→None", sr3 is None and bb3 is None))
    # 데이터 끝 → reached False (미래 없음)
    dshort = d[:121]
    ok.append(("T9 forward없음→stop reached False", trail_stop(dshort, 120, entry, 8.0)["reached"] is False))
    ok.append(("T10 grade_disclosure S급 일치", grade_disclosure("단일판매ㆍ공급계약체결", "계약금액 100 매출액 500")[1] == "S"))
    # T11/T12: build_record + json 통합 경로 (★ H-1 재발방지 — selftest가 글루를 직접 태움) ★
    cat = ("공급계약", "S", "매출대비 33.3%", 120, "20260312000001", "단일판매ㆍ공급계약체결")
    uni = {"999999": {"cap_억": 5000, "sector": "기계장비"}}
    rec = build_record("999999", "테스트종목", d, 120, cat, uni)
    ok.append(("T11 build_record 통합(6-튜플 unpack)",
               isinstance(rec, dict) and rec["catalyst"]["grade"] == "S"
               and rec["catalyst"]["sales_ratio_pct"] == 33.3
               and rec["catalyst"]["rcept_no"] == "20260312000001"
               and rec["catalyst"]["cat_offset_T"] == 0
               and rec["track"] == "large_mid" and rec["entry"]["t0_close"] > 0))
    s = json.dumps(rec, ensure_ascii=False)
    ok.append(("T12 json 직렬화 안전(NaN/타입)",
               isinstance(s, str) and '"grade": "S"' in s and "NaN" not in s))
    print("sdart_shadow_record 셀프테스트:")
    for n, p in ok:
        print(f"  [{'PASS' if p else 'FAIL'}] {n}")
    return all(p for _, p in ok)


def _print_summary(asof, records, n_steady, out):
    by = Counter(r["catalyst"]["grade"] for r in records)
    print("=" * 100)
    print(f"sdart_shadow_record — STEADY_S_DART_RIDE forward shadow (asof {asof}, 넓게 병행 기록)")
    print(f"STEADY 후보 {n_steady} 중 호재공시有 {len(records)}건 기록 → {out}")
    print(f"★ 증빙: SAJANG.AUTO_TRADE_DISABLED={SAJANG.AUTO_TRADE_DISABLED} / 실주문 0 / 주문함수 0 "
          f"/ picks·asset_pool 불변 / SAJANG·scheduler·systemd 무변경")
    print(f"★ lookahead 없음: detect=data[:i]만 / 공시 T-3..T0 / 진입=T0종가·T+1시가 / fwd만 미래")
    print(f"[등급 분포] {dict(by)}")
    print("=" * 100)
    for r in records:
        c = r["catalyst"]
        val = (f"매출대비{c['sales_ratio_pct']}%" if c["sales_ratio_pct"] is not None
               else (f"자사주{c['buyback_amt_억']}억" if c["buyback_amt_억"] is not None else c["detail"]))
        print(f"  [{c['grade']}] {r['name']:<14}{r['t0_date']} {c['kind']}/{val} "
              f"cap {r['cap_억']}억({r['track']}) surge{r['surge_pct']}% [{r['status']}]")
    print("\n★★ shadow=기록만(주문 0). 5개 확정값은 forward ≥10~20건 + 설계서 5장 숙제 후 사장님 승인 → SAJANG.")
    print("★★ 정직: 백테 생존편향 과대·thin → forward(상폐포함 실시간)가 본검증. 절대수익 신뢰 X.")


def main():
    sys.stdout.reconfigure(encoding="utf-8")
    ap = argparse.ArgumentParser()
    ap.add_argument("--asof", default=date.today().isoformat())
    ap.add_argument("--rehearsal", action="store_true",
                    help="과거 풍부데이터 날짜로 전체 스키마 채움 검증")
    ap.add_argument("--max", type=int, default=0, help="스캔 종목 수 제한(리허설/디버그)")
    ap.add_argument("--selftest", action="store_true")
    a = ap.parse_args()
    if a.selftest:
        sys.exit(0 if selftest() else 1)
    asof = "2026-04-15" if a.rehearsal else a.asof
    records, n_steady = scan_asof(asof, a.max)
    out = write_shadow(asof, records)
    _print_summary(asof, records, n_steady, out)


if __name__ == "__main__":
    main()
