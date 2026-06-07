# -*- coding: utf-8 -*-
"""ledger_integrity_check — 6/8 첫 관측 무결성 점검 (read-only, 매매경로 무접촉).

사장님 6/7 지시: 6/8 첫날은 "좋은 종목 찾았냐"보다 ★ 장부 무결성 ★ 을 먼저 본다.
이 도구가 사장님이 번호로 강조하신 7가지 + 'A/B/C 동일기준'을 한 번에 PASS/FAIL 판정.

  1. weekly_open_state 가 A/B/C 전부에 들어가는가
  2. half_year_open_state 누락 없이 들어가는가
  3. B 후보에 half_pullback_state 가 붙는가
  4. C 후보에 breakout_state 가 붙는가
  5. annual_overheat_warning 이 과열 후보에만(=True면 1년 +500%↑) 일관되게 뜨는가
  6. 계산 불가가 0/false 가 아니라 null 로 남는가 (state 필드 오염 검사)
  7. 새 feature 가 선정조건에 끼어들지 않았는가 (B=pullback_3 / C=break류 그대로)
  8. A/B/C 가 같은 기준으로 기록되는가 (summary 카운트·date 일치)

★ 순수 read-only: ledger json 만 읽음. 실주문 0 / 주문함수 0 / SAJANG·scheduler 미참조.
  ledger 를 수정하지 않음(점검만). 매매 경로·picks 무접촉.

사용:
  python tools/ledger_integrity_check_6_8.py [--asof YYYY-MM-DD]   # 해당일 ledger 점검(없으면 최신)
  python tools/ledger_integrity_check_6_8.py --dry                 # 실 코드로 임시 ledger 생성→점검(6/8 전 리허설)
  python tools/ledger_integrity_check_6_8.py --selftest            # 합성 ledger 로 점검 로직 검증
"""
from __future__ import annotations

import argparse
import glob
import json
import os
import sys
from pathlib import Path

SA = Path(__file__).resolve().parent.parent
TOOLS = Path(__file__).resolve().parent
LEDGER_DIR = SA / "data_store" / "paper_3type"

VALID_STATES = {
    "weekly_open_state": {"ABOVE", "BELOW", None},
    "monthly_open_state": {"ABOVE", "BELOW", None},
    "half_year_open_state": {"ABOVE", "BELOW", None},
    "float_candle_state": {"FIRST_FLOAT", "SECOND_FLOAT", "FLOAT_CONTINUED", "NOT_FLOAT", None},
    "half_pullback_state": {"HALF_PULLBACK_VALID", "HALF_PULLBACK_BROKEN", "NO_HALF_PULLBACK", None},
    "breakout_state": {"BREAKOUT", "BREAKOUT_FAIL", "NO_BREAKOUT", None},
    "sector_peer_sync_state": {"STRONG", "MODERATE", "WEAK", None},
}


def _ext(r):
    return r.get("extra") or {}


def check_ledger(data):
    """ledger dict → [(check, pass_bool, detail)] 8건. 매매 무관 순수 판정."""
    cands = data.get("candidates", {})
    A, B, C = cands.get("A", []), cands.get("B", []), cands.get("C", [])
    allc = A + B + C
    out = []

    # 1. weekly_open_state — A/B/C 전부 키 존재
    m = [r.get("ticker") for r in allc if "weekly_open_state" not in _ext(r)]
    out.append(("1. weekly_open_state A/B/C 전부", not m, f"누락 {len(m)}건 {m[:5]}"))

    # 2. half_year_open_state — 누락 0
    m = [r.get("ticker") for r in allc if "half_year_open_state" not in _ext(r)]
    out.append(("2. half_year_open_state 누락0", not m, f"누락 {len(m)}건 {m[:5]}"))

    # 3. B half_pullback_state
    m = [r.get("ticker") for r in B if "half_pullback_state" not in _ext(r)]
    out.append(("3. B half_pullback_state", not m, f"B {len(B)}건 중 누락 {len(m)} {m[:5]}"))

    # 4. C breakout_state
    m = [r.get("ticker") for r in C if "breakout_state" not in _ext(r)]
    out.append(("4. C breakout_state", not m, f"C {len(C)}건 중 누락 {len(m)} {m[:5]}"))

    # 5. annual_overheat_warning 일관: True면 return_1y_pct>=500, 타입은 bool/None 만
    bad_logic = [r.get("ticker") for r in allc if _ext(r).get("annual_overheat_warning") is True
                 and not (isinstance(_ext(r).get("return_1y_pct"), (int, float))
                          and _ext(r)["return_1y_pct"] >= 500)]
    bad_type = [r.get("ticker") for r in allc if "annual_overheat_warning" in _ext(r)
                and _ext(r)["annual_overheat_warning"] not in (True, False, None)]
    n_warn = sum(1 for r in allc if _ext(r).get("annual_overheat_warning") is True)
    out.append(("5. overheat_warning 일관(True→+500%↑)", not bad_logic and not bad_type,
                f"warning={n_warn}건 / 논리위반 {bad_logic[:5]} 타입위반 {bad_type[:5]}"))

    # 6. null 보존 — state 필드가 0/""/False 등으로 오염되지 않음(허용집합 외 = 오염)
    bad6 = []
    for r in allc:
        e = _ext(r)
        for k, vs in VALID_STATES.items():
            if k in e and e[k] not in vs:
                bad6.append((r.get("ticker"), k, e[k]))
    out.append(("6. null 보존(0/false 오염 없음)", not bad6, f"오염 {len(bad6)}건 {bad6[:5]}"))

    # 7. 선정조건 불변 — B=features.pullback_3 True / C=break_5|new_high|strong_bull 중 1+
    b7 = [r.get("ticker") for r in B if not (_ext(r).get("features") or {}).get("pullback_3")]
    c7 = [r.get("ticker") for r in C if not any((_ext(r).get("features") or {}).get(k)
          for k in ("break_5", "new_high", "strong_bull"))]
    out.append(("7. 선정조건 불변(B pullback_3 / C break류)", not b7 and not c7,
                f"B위반 {b7[:5]} C위반 {c7[:5]}"))

    # 8. A/B/C 동일기준 — summary 카운트·date 일치
    s = data.get("summary", {})
    cnt_ok = s.get("A") == len(A) and s.get("B") == len(B) and s.get("C") == len(C)
    date = data.get("date")
    date_ok = all(r.get("date") == date for r in allc)
    out.append(("8. A/B/C 동일기준(summary·date 일치)", cnt_ok and date_ok,
                f"summary {s.get('A')}/{s.get('B')}/{s.get('C')} 실제 {len(A)}/{len(B)}/{len(C)} date_ok={date_ok}"))
    return out


VARIANT_OK = {"A1", "A2", "B", "C"}


def check_meta(data):
    """[보조] 6/8 메타 명확화 필드 점검 — 핵심 8/8과 분리(장부 설명필드 + 트레일링only 보증)."""
    cands = data.get("candidates", {})
    A, B, C = cands.get("A", []), cands.get("B", []), cands.get("C", [])
    allc = A + B + C
    out = []

    # M1. variant 존재·유효(A1/A2/B/C 판정 라벨)
    bad = [r.get("ticker") for r in allc if r.get("variant") not in VARIANT_OK]
    out.append(("M1. variant 존재·유효(A1/A2/B/C)", not bad, f"위반 {bad[:5]}"))

    # M2. virtual_entry_date · entry_basis · capital_bucket 존재(장부 명확화)
    miss = [r.get("ticker") for r in allc if r.get("virtual_entry_date") is None
            or r.get("entry_basis") is None or r.get("capital_bucket") is None]
    out.append(("M2. entry_date·entry_basis·capital_bucket 존재", not miss, f"누락 {miss[:5]}"))

    # M3. capital_bucket 가 type prefix 와 일치(A_/B_/C_)
    badb = [r.get("ticker") for r in allc if r.get("capital_bucket") and r.get("type")
            and not str(r["capital_bucket"]).startswith(f"{r['type']}_")]
    out.append(("M3. capital_bucket type 일치(A_/B_/C_)", not badb, f"불일치 {badb[:5]}"))

    # M4. ★ would_take_profit 금지(없어야) + tp_touch_only=True (트레일링 only 영구룰 보증)
    forbidden = [r.get("ticker") for r in allc
                 if "would_take_profit" in _ext(r) or "would_take_profit" in r]
    bad_only = [r.get("ticker") for r in allc if _ext(r).get("tp_touch_only") is not True]
    out.append(("M4. would_take_profit 금지·tp_touch_only=True", not forbidden and not bad_only,
                f"금지필드 {forbidden[:5]} / only위반 {bad_only[:5]}"))

    # M5. 갭/시가 라벨 per-candidate 존재 (6/8 3묶음①)
    miss5 = [r.get("ticker") for r in allc
             if "gap_pct" not in _ext(r) or "close_above_open" not in _ext(r)]
    out.append(("M5. 갭/시가 라벨 존재(gap_pct·close_above_open)", not miss5, f"누락 {miss5[:5]}"))

    # M6. 꼬리/종가 라벨 per-candidate 존재 (6/8 3묶음②)
    miss6 = [r.get("ticker") for r in allc
             if "close_location_pct" not in _ext(r) or "candle_shape" not in _ext(r)]
    out.append(("M6. 꼬리/종가 라벨 존재(close_location·candle_shape)", not miss6, f"누락 {miss6[:5]}"))

    # M7. market_context(시장 폭넓이) 블록 존재 (6/8 3묶음③) — data 레벨, 후보 무관
    mc = data.get("market_context")
    out.append(("M7. market_context(breadth) 존재",
                isinstance(mc, dict) and mc.get("breadth_state") is not None,
                f"market_context={'있음' if isinstance(mc, dict) else mc}"))
    return out


def _print(title, data):
    res = check_ledger(data)
    meta = check_meta(data)
    npass = sum(1 for _, p, _ in res if p)
    mpass = sum(1 for _, p, _ in meta if p)
    print("=" * 90)
    print(f"ledger 무결성 점검 — {title}")
    print(f"  date={data.get('date')}  summary={data.get('summary')}")
    print("=" * 90)
    for nme, p, detail in res:
        print(f"  [{'PASS' if p else 'FAIL'}] {nme}")
        if not p:
            print(f"          ↳ {detail}")
    print("-" * 90)
    verdict = "PASS (8/8)" if npass == len(res) else f"FAIL ({npass}/{len(res)})"
    print(f"  ★ 핵심 무결성 판정: {verdict}   ← 8/8 PASS 전 수익률 해석 금지(사장님)")
    print("  [보조] 메타 명확화 점검(장부 설명필드 + 트레일링only 보증):")
    for nme, p, detail in meta:
        print(f"    [{'PASS' if p else 'FAIL'}] {nme}")
        if not p:
            print(f"            ↳ {detail}")
    print(f"  보조 메타: {mpass}/{len(meta)}" + ("" if mpass == len(meta) else "  (장부 명확화 미흡 — 보고에 명시)"))
    print("  ※ read-only 점검 — 실주문 0 / ledger 무수정 / 매매경로 무접촉 / 트레일링only 유지")
    return npass == len(res)   # verdict = 핵심 8/8 기준(사장님 프레임)


def _latest_ledger():
    fs = sorted(glob.glob(str(LEDGER_DIR / "ledger_*.json")))
    fs = [f for f in fs if "_bc_only" not in os.path.basename(f)]   # 통합 ledger만
    return fs[-1] if fs else None


def run_file(asof=None):
    if asof:
        path = LEDGER_DIR / f"ledger_{asof}.json"
        if not path.exists():
            print(f"[중단] {path} 없음 — 6/8 관측 후 생성됩니다.")
            return False
        path = str(path)
    else:
        path = _latest_ledger()
        if not path:
            print(f"[중단] ledger 없음 ({LEDGER_DIR}) — 6/8 관측 후 생성됩니다.")
            return False
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    return _print(os.path.basename(path), data)


def run_dry(asof, skip_breadth=False):
    """실 코드(collect_candidates + record_sdart_into)로 임시 ledger 생성 → 점검(save 없음, 6/8 전 리허설).
    skip_breadth=True면 전종목 폭넓이(~83초) 생략 → 빠른 핵심 8/8 점검(M7만 N/A=FAIL 표시)."""
    sys.path.insert(0, str(SA)); sys.path.insert(0, str(TOOLS))
    from rotation_paper_scan_6_6 import _code_to_path, scan_sectors, collect_candidates, record_bc_into
    from paper_3type_daily_run_6_6 import record_sdart_into
    from data.paper_3type_ledger import new_ledger
    c2p = _code_to_path()
    led = new_ledger(asof)
    # A: 실데이터 0건일 수 있으므로 합성 1건(삼성전자 실일봉)으로 A 경로 점검
    rec = {"code": "005930", "name": "삼성전자", "t0_date": asof, "sector": "반도체/AI",
           "track": "large_mid", "catalyst": {"grade": "S", "kind": "공급계약", "detail": "리허설"},
           "entry": {"t0_close": 351500.0}, "status": "rehearsal(no order)"}
    record_sdart_into(led, [rec], c2p=c2p, asof=asof)
    record_bc_into(led, collect_candidates(scan_sectors(c2p, asof)))
    # 6/8 시장 컨텍스트(전종목 ~83초) — M7 재현. skip_breadth면 생략(빠른 점검).
    if not skip_breadth:
        from market_breadth_6_8 import market_breadth
        from price_structure_features_6_8 import find_index_asof
        from leader_prospective_scan_6_2 import load_daily
        led.set_market_context(market_breadth(c2p, asof, load_daily, find_index_asof))
    data = {"date": asof, "summary": led.summary(),
            "market_context": led.market_context,
            "candidates": led.candidates, "rejects": led.rejects}
    tag = "breadth 포함" if not skip_breadth else "breadth skip→M7 N/A"
    return _print(f"[DRY 리허설 asof={asof}] 실 코드 생성 ledger (save 안 함, {tag})", data)


# ─────────────────────────── selftest (합성 ledger, 네트워크 0) ───────────────────────────
def selftest():
    ok = []
    # 정상 ledger — 7/7 + 8 전부 PASS 여야
    _gc = {"gap_pct": 1.2, "close_above_open": True,           # 갭/꼬리 공통(M5·M6 충족)
           "close_location_pct": 0.9, "candle_shape": "STRONG_CLOSE", "tp_touch_only": True}
    good = {
        "date": "2026-06-08",
        "summary": {"A": 1, "B": 1, "C": 1, "rejects": {"A": 0, "B": 0, "C": 0}},
        "market_context": {"breadth_state": "MIXED", "breadth_pct": 0.5,
                           "advancers": 100, "decliners": 100},
        "candidates": {
            "A": [{"ticker": "111", "type": "A", "variant": "A1", "date": "2026-06-08",
                   "virtual_entry_date": "2026-06-08", "entry_basis": "T0_CLOSE",
                   "capital_bucket": "A_30", "extra": {
                       "weekly_open_state": "ABOVE", "half_year_open_state": None,
                       "float_candle_state": "FIRST_FLOAT", "annual_overheat_warning": True,
                       "return_1y_pct": 531.1, "sector_peer_sync_state": None, **_gc}}],
            "B": [{"ticker": "222", "type": "B", "variant": "B", "date": "2026-06-08",
                   "virtual_entry_date": "2026-06-08", "entry_basis": "T0_CLOSE",
                   "capital_bucket": "B_35", "extra": {
                       "weekly_open_state": "BELOW", "half_year_open_state": "ABOVE",
                       "half_pullback_state": "HALF_PULLBACK_VALID", "annual_overheat_warning": False,
                       "return_1y_pct": 12.0, "sector_peer_sync_state": "STRONG",
                       "features": {"pullback_3": True}, **_gc}}],
            "C": [{"ticker": "333", "type": "C", "variant": "C", "date": "2026-06-08",
                   "virtual_entry_date": "2026-06-08", "entry_basis": "T0_CLOSE",
                   "capital_bucket": "C_35", "extra": {
                       "weekly_open_state": "ABOVE", "half_year_open_state": "BELOW",
                       "breakout_state": "BREAKOUT", "annual_overheat_warning": None,
                       "return_1y_pct": None, "sector_peer_sync_state": "WEAK",
                       "features": {"break_5": True, "new_high": False, "strong_bull": False}, **_gc}}],
        },
        "rejects": {"A": [], "B": [], "C": []},
    }
    res = check_ledger(good)
    ok.append(("T1 정상 ledger → 8/8 PASS", all(p for _, p, _ in res)))
    ok.append(("T1b 정상 ledger → 보조 메타 7/7 PASS", all(p for _, p, _ in check_meta(good))))

    # 오염 1: weekly_open_state 누락(B) → check1 FAIL
    bad1 = json.loads(json.dumps(good))
    del bad1["candidates"]["B"][0]["extra"]["weekly_open_state"]
    r1 = check_ledger(bad1)
    ok.append(("T2 weekly 누락 → check1 FAIL", r1[0][1] is False))

    # 오염 2: B에 half_pullback_state 누락 → check3 FAIL
    bad2 = json.loads(json.dumps(good))
    del bad2["candidates"]["B"][0]["extra"]["half_pullback_state"]
    r2 = check_ledger(bad2)
    ok.append(("T3 B half_pullback 누락 → check3 FAIL", r2[2][1] is False))

    # 오염 3: null 자리에 0/false 오염(weekly_open_state=0) → check6 FAIL
    bad3 = json.loads(json.dumps(good))
    bad3["candidates"]["C"][0]["extra"]["weekly_open_state"] = 0
    r3 = check_ledger(bad3)
    ok.append(("T4 null자리 0 오염 → check6 FAIL", r3[5][1] is False))

    # 오염 4: overheat warning=True인데 return_1y_pct<500 → check5 FAIL
    bad4 = json.loads(json.dumps(good))
    bad4["candidates"]["A"][0]["extra"]["return_1y_pct"] = 120.0
    r4 = check_ledger(bad4)
    ok.append(("T5 warning True인데 +500%미만 → check5 FAIL", r4[4][1] is False))

    # 오염 5: 선정조건 위반(B features.pullback_3=False) → check7 FAIL
    bad5 = json.loads(json.dumps(good))
    bad5["candidates"]["B"][0]["extra"]["features"]["pullback_3"] = False
    r5 = check_ledger(bad5)
    ok.append(("T6 B pullback_3=False(선정조건 깨짐) → check7 FAIL", r5[6][1] is False))

    # 오염 6: summary 카운트 불일치 → check8 FAIL
    bad6 = json.loads(json.dumps(good))
    bad6["summary"]["B"] = 5
    r6 = check_ledger(bad6)
    ok.append(("T7 summary 카운트 불일치 → check8 FAIL", r6[7][1] is False))

    # 후보 0건(약세장) → 누락 검사는 vacuously PASS, 동일기준도 PASS
    empty = {"date": "2026-06-08", "summary": {"A": 0, "B": 0, "C": 0, "rejects": {}},
             "market_context": {"breadth_state": "MIXED"},
             "candidates": {"A": [], "B": [], "C": []}, "rejects": {"A": [], "B": [], "C": []}}
    ok.append(("T8 후보0건(정상 표본) → 8/8 PASS", all(p for _, p, _ in check_ledger(empty))))
    ok.append(("T8b 후보0건 → 보조 메타도 7/7 PASS", all(p for _, p, _ in check_meta(empty))))

    # 메타 오염 6/8: would_take_profit 부활 → M4 FAIL (★ 고정 TP 부활 차단)
    badm1 = json.loads(json.dumps(good))
    badm1["candidates"]["C"][0]["extra"]["would_take_profit"] = True
    ok.append(("T9 would_take_profit 부활 → M4 FAIL", check_meta(badm1)[3][1] is False))
    # variant 누락 → M1 FAIL
    badm2 = json.loads(json.dumps(good))
    badm2["candidates"]["B"][0]["variant"] = None
    ok.append(("T10 variant 누락 → M1 FAIL", check_meta(badm2)[0][1] is False))
    # tp_touch_only False(터치 관측 아님) → M4 FAIL
    badm3 = json.loads(json.dumps(good))
    badm3["candidates"]["A"][0]["extra"]["tp_touch_only"] = False
    ok.append(("T11 tp_touch_only False → M4 FAIL", check_meta(badm3)[3][1] is False))
    # capital_bucket type 불일치(B인데 A_30) → M3 FAIL
    badm4 = json.loads(json.dumps(good))
    badm4["candidates"]["B"][0]["capital_bucket"] = "A_30"
    ok.append(("T12 capital_bucket type 불일치 → M3 FAIL", check_meta(badm4)[2][1] is False))
    # 6/8 3묶음 오염: gap_pct 누락 → M5 FAIL
    badm5 = json.loads(json.dumps(good))
    del badm5["candidates"]["B"][0]["extra"]["gap_pct"]
    ok.append(("T13 gap_pct 누락 → M5 FAIL", check_meta(badm5)[4][1] is False))
    # candle_shape 누락 → M6 FAIL
    badm6 = json.loads(json.dumps(good))
    del badm6["candidates"]["C"][0]["extra"]["candle_shape"]
    ok.append(("T14 candle_shape 누락 → M6 FAIL", check_meta(badm6)[5][1] is False))
    # market_context 없음 → M7 FAIL
    badm7 = json.loads(json.dumps(good))
    badm7["market_context"] = None
    ok.append(("T15 market_context None → M7 FAIL", check_meta(badm7)[6][1] is False))

    print("ledger_integrity_check 셀프테스트:")
    for nme, p in ok:
        print(f"  [{'PASS' if p else 'FAIL'}] {nme}")
    return all(p for _, p in ok)


def main():
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    ap = argparse.ArgumentParser()
    ap.add_argument("--asof", default=None, help="점검할 ledger 날짜(없으면 최신 통합 ledger)")
    ap.add_argument("--dry", action="store_true", help="실 코드로 임시 ledger 생성→점검(6/8 전 리허설)")
    ap.add_argument("--skip-breadth", action="store_true",
                    help="dry 시 전종목 폭넓이(~83초) 생략 — 빠른 핵심 8/8 점검(M7만 N/A)")
    ap.add_argument("--selftest", action="store_true")
    a = ap.parse_args()
    if a.selftest:
        sys.exit(0 if selftest() else 1)
    if a.dry:
        from datetime import date
        sys.exit(0 if run_dry(a.asof or date.today().isoformat(), a.skip_breadth) else 1)
    sys.exit(0 if run_file(a.asof) else 1)


if __name__ == "__main__":
    main()
