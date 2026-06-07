# -*- coding: utf-8 -*-
"""paper_3type_daily_run — A/B/C 3-Type 통합 일일러너 (6/6, read-only 기록 전용).

A(STEADY_S_DART_RIDE) + B(ROTATION_PULLBACK) + C(ROTATION_RIDE)를
★ 같은 paper_3type ledger(ledger_{date}.json) 한 파일 ★ 에 같은 기준으로 기록.
→ 6/12 타입별 1차 판정("눌림 vs 올라타기 vs S급명분, 돈 기준 비교")의 토대.

설계 6단계(통합 일일러너) + 1단계(A를 같은 ledger에 연결).

★ 원칙(사장님 6/6 재확인, 불변):
  - scheduler 자동 연결 금지 (수동 실행만 — 이 파일은 systemd/scheduler에 배선 X)
  - 실주문 0 / KIS 주문함수 호출 0 / SAJANG 무변경 / 봇 OFF / picks·asset_pool 불변
  - NEWS/DART/수급 = 진입 hard gate 아님, 기록·명분·fact layer (signal_source는 명분 기록일 뿐)
  - B/C 스캐너 결과만 보고 실전 flip 금지. 6/12 판정은 1차 판정이지 영구룰 확정 아님.

운영 순서(매일 수동): step6 sync(stock_data_daily 최신화) → 이 러너 → ledger 누적.
  A는 DART 본문 조회(네트워크) 발생(sdart 원래 동작, 매매·picks 무관). B/C는 네트워크 0.

사용: python tools/paper_3type_daily_run_6_6.py [--asof YYYY-MM-DD] [--skip-a] [--selftest]
  --skip-a : A(sdart, 네트워크) 건너뛰고 B/C만 (빠른 점검용)
"""
from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

SA = Path(__file__).resolve().parent.parent
TOOLS = Path(__file__).resolve().parent
sys.path.insert(0, str(SA))
sys.path.insert(0, str(TOOLS))

from data.paper_3type_ledger import new_ledger  # noqa: E402
from rotation_paper_scan_6_6 import (  # noqa: E402
    _code_to_path, scan_sectors, collect_candidates, record_bc_into, STRONG, tp_touch)
from price_structure_features_6_8 import (  # noqa: E402
    gap_open_features, candle_shape_features)

A_SEED = 30.0   # 설계 5장 A30/B35/C35 (B/C 시드는 rotation_paper_scan.TYPE_SEED)
# 6/8 갭/꼬리 None 기본(키 보존) — i<1·rng=0 케이스로 전 키 None dict 생성
_GAP_NONE = gap_open_features([("", 0, 0, 0, 0, 0)], 0)
_CANDLE_NONE = candle_shape_features([("", 1, 1, 1, 1, 1)], 0)


# ── A(sdart) 어댑터: sdart build_record dict → paper_3type ledger "A" record ──
def record_sdart_into(led, records, event_snapshot=None, c2p=None, asof=None):
    """sdart_shadow_record.scan_asof 의 records → led.record("A", ...).
    명분(grade)은 signal_source(fact layer)로 기록만 — hard gate 아님.
    event_snapshot 있으면 EVENT hook으로 A1/A2 variant + event_layer 태그(진입권한 0, 비교용).
    c2p(code→일봉경로)·asof 있으면 6/8 가격구조 라벨(주봉/월/반기 시가·첫봉투봉·연간과열)을
    A1/A2에도 동일 부착(지시서 9장) — hard gate 0, 계산 불가 시 전 키 None(키 보존)."""
    from price_structure_features_6_8 import price_structure_labels, find_index_asof  # noqa: E402
    n = len(records)
    per = round(A_SEED / n, 2) if n else 0.0
    pct = round(100.0 / n, 1) if n else 0.0
    if event_snapshot is not None:
        from event_signal_hook_6_6 import event_layer_for, variant_and_source  # noqa: E402
    if c2p is not None:
        from leader_prospective_scan_6_2 import load_daily  # noqa: E402
    for r in records:
        cat = r.get("catalyst", {}) or {}
        grade = cat.get("grade")
        sig = f"DART_{grade}" if grade in ("S", "A", "B") else "STEADY"
        entry = (r.get("entry") or {}).get("t0_close")
        val = (f"매출대비{cat.get('sales_ratio_pct')}%" if cat.get("sales_ratio_pct") is not None
               else (f"자사주{cat.get('buyback_amt_억')}억" if cat.get("buyback_amt_억") is not None
                     else cat.get("detail")))
        # EVENT hook: A2(보강) 판정 + event_layer 태그. NEWS=forward 태그일뿐, 매매 확정권 0.
        a_variant = "A1"
        event_layer = None
        if event_snapshot is not None:
            event_layer = event_layer_for(r.get("code"), event_snapshot)
            a_variant, sig = variant_and_source(sig, event_layer if event_layer.get("matched") else None)
        # 6/8 가격구조 라벨(asof 기준) + tp_touch 관측(t0_date 기준=sdart forward와 일관).
        # 일봉 로드 가능 시만 계산, 불가/미전달 시 ps 전 키 None·tp_obs None(키 보존).
        ps = price_structure_labels(None, None)
        tp_obs = None
        gap_lbl = dict(_GAP_NONE)
        candle_lbl = dict(_CANDLE_NONE)
        if c2p is not None:
            code = r.get("code")
            p = c2p.get(code) or c2p.get(str(code).zfill(6))
            if p:
                dd = load_daily(p)
                j_ps = find_index_asof(dd, asof or r.get("t0_date"))   # 가격구조=현재(asof)
                if j_ps is not None:
                    ps = price_structure_labels(dd, j_ps)
                t0d = r.get("t0_date")
                j_t0 = find_index_asof(dd, t0d) if t0d else None       # 터치·갭·꼬리=진입(t0_date) 캔들
                if j_t0 is not None:
                    gap_lbl = gap_open_features(dd, j_t0, ps.get("weekly_open"))
                    candle_lbl = candle_shape_features(dd, j_t0)
                    if entry and entry > 0:
                        tp_obs = tp_touch(dd, j_t0, entry)
        led.record(
            "A",
            variant=a_variant,                       # 6/8 메타: A1/A2 판정 라벨(variant 단일키)
            ticker=r.get("code"), name=r.get("name"), sector=r.get("sector"),
            group=r.get("track"),
            entry_reason=f"STEADY+{cat.get('kind')}/{grade} {val}",
            signal_source=sig,
            virtual_entry_date=r.get("t0_date"), virtual_entry_price=entry,
            entry_basis="T0_CLOSE",                  # 6/8 메타: A 현재 기준 명시(로직 불변)
            MFE=r.get("mfe_pct_hold40"), MAE=r.get("mae_pct_hold40"),
            holding_days=0, supply=None, market_regime=None,
            capital_allocated=per, capital_bucket=f"A_{int(A_SEED)}", position_size_pct=pct,
            # extra (넓게 병행 — sdart forward 부품 전체 보존 + EVENT hook 태그 + 가격구조 라벨)
            a_variant=a_variant, event_layer=event_layer,
            catalyst=cat, entry=r.get("entry"),
            raw_fwd_from_t0close=r.get("raw_fwd_from_t0close"),
            raw_fwd_from_t1open=r.get("raw_fwd_from_t1open"),
            would_stop=r.get("would_stop"), would_exit=r.get("would_exit"),
            tp_touch_observer=tp_obs, tp_touch_level_pct=[5.0, 10.0],   # 6/8 관측용(★익절 아님)
            tp_touch_only=True,
            cap_억=r.get("cap_억"), track=r.get("track"),
            surge_pct=r.get("surge_pct"), turnover_억=r.get("turnover_억"),
            high_120d=r.get("high_120d"), t0_date=r.get("t0_date"),
            sdart_status=r.get("status"),
            **ps, **gap_lbl, **candle_lbl,   # 6/8 가격구조 + 갭/시가 + 꼬리/종가
        )
    return n


def run(asof, skip_a=False, scan_events=False, skip_breadth=False):
    """A/B/C를 한 ledger에 기록 → save. 반환 dict(요약).
    scan_events=True면 event_detector 실시간 스캔(네트워크)으로 events.json 갱신 후 A2 판정.
    skip_breadth=False(기본)면 전종목 시장 폭넓이(~83초) 계산해 market_context 기록."""
    led = new_ledger(asof)
    c2p = _code_to_path()        # A(가격구조 라벨)·B/C 공용 — 네트워크 0
    # A — sdart (네트워크: DART 조회) + EVENT hook(A1/A2)
    a_n = 0
    n_steady = 0
    event_note = None
    a1 = a2 = 0
    if not skip_a:
        from event_signal_hook_6_6 import load_event_snapshot  # noqa: E402
        if scan_events:
            try:
                from data.event_detector import run_event_scan  # noqa: E402 (네트워크 — 옵션)
                run_event_scan(scan_dart=True, scan_news=True)   # events.json 갱신
            except Exception as e:                               # noqa: BLE001
                print(f"[event_scan 실패(무시 — A1만 기록): {e}]")
        snapshot = load_event_snapshot(asof)
        event_note = (f"events.json scanned={snapshot['scanned_date']} "
                      f"fresh={snapshot['fresh']} ({snapshot['reason']})")
        from sdart_shadow_record_6_3 import scan_asof  # noqa: E402 (무거운 의존 — 필요시만)
        records, n_steady = scan_asof(asof)
        a_n = record_sdart_into(led, records, event_snapshot=snapshot, c2p=c2p, asof=asof)
        for r in led.candidates["A"]:
            v = (r.get("extra") or {}).get("a_variant")
            a1 += (v == "A1")
            a2 += (v == "A2")
    # B/C — rotation (네트워크 0)
    sectors = scan_sectors(c2p, asof)
    cands = collect_candidates(sectors)
    record_bc_into(led, cands)
    # 6/8 시장 컨텍스트 — 전종목 폭넓이+거래대금(~83초). skip_breadth로 빠른 점검 시 생략.
    if not skip_breadth:
        from market_breadth_6_8 import market_breadth  # noqa: E402
        from price_structure_features_6_8 import find_index_asof  # noqa: E402
        from leader_prospective_scan_6_2 import load_daily  # noqa: E402
        led.set_market_context(market_breadth(c2p, asof, load_daily, find_index_asof))
    path = led.save()
    nb = sum(1 for t, _, _ in cands if t == "B")
    nc = sum(1 for t, _, _ in cands if t == "C")
    return {"asof": asof, "path": path, "sectors": sectors,
            "a_n": a_n, "a1_n": a1, "a2_n": a2, "n_steady": n_steady,
            "b_n": nb, "c_n": nc, "event_note": event_note,
            "summary": led.summary(), "skip_a": skip_a,
            "skip_breadth": skip_breadth, "breadth": led.market_context}


# ─────────────────────────── selftest (합성, 네트워크 0) ───────────────────────────
def selftest():
    ok = []
    # 합성 sdart record 1건 (build_record 스키마)
    rec = {
        "code": "999999", "name": "합성S종목", "t0_date": "2026-06-04",
        "sector": "기계장비", "cap_억": 5000, "track": "large_mid",
        "trigger": "steady_high_breakout", "high_120d": 12345.0,
        "turnover_억": 320.5, "surge_pct": 3.1,
        "catalyst": {"rcept_no": "20260604000001", "report_nm": "단일판매ㆍ공급계약체결",
                     "kind": "공급계약", "grade": "S", "sales_ratio_pct": 33.3,
                     "buyback_amt_억": None, "detail": "매출대비 33.3%", "cat_offset_T": 0},
        "entry": {"t0_close": 10000.0, "t1_open": 10100.0},
        "raw_fwd_from_t0close": {"d1": 1.5, "d3": None, "d5": None, "d10": None, "d20": None},
        "raw_fwd_from_t1open": {"d1": None, "d3": None, "d5": None, "d10": None, "d20": None},
        "would_stop": {"minus8": {"reached": False}, "minus10": {"reached": False}},
        "would_exit": {"ma10": {"reached": False}, "d10": {"reached": False}},
        "mfe_pct_hold40": 8.2, "mae_pct_hold40": -2.1, "status": "observed(no order)",
    }
    led = new_ledger("2026-06-04")
    a_n = record_sdart_into(led, [rec])
    ok.append(("T1 A record 1건", a_n == 1 and led.summary()["A"] == 1))
    row = led.candidates["A"][0]
    ok.append(("T2 A 17필드 매핑", row["ticker"] == "999999" and row["type"] == "A"
               and row["virtual_entry_price"] == 10000.0 and row["MFE"] == 8.2 and row["MAE"] == -2.1))
    ok.append(("T3 signal_source=DART_S (fact layer)", row["signal_source"] == "DART_S"))
    ok.append(("T4 A extra 넓게병행 보존",
               "extra" in row and "catalyst" in row["extra"]
               and "raw_fwd_from_t0close" in row["extra"] and "would_stop" in row["extra"]))
    ok.append(("T5 A capital 배분(seed30/n)", row["capital_allocated"] == 30.0
               and row["position_size_pct"] == 100.0))
    # 등급 없음 → STEADY (명분 없어도 추세는 기록)
    rec2 = dict(rec, code="888888", name="합성추세")
    rec2["catalyst"] = dict(rec["catalyst"], grade="없음", kind="없음")
    led2 = new_ledger("2026-06-04")
    record_sdart_into(led2, [rec2])
    ok.append(("T6 grade없음 → signal_source=STEADY",
               led2.candidates["A"][0]["signal_source"] == "STEADY"))
    # 2건 배분
    led3 = new_ledger("2026-06-04")
    record_sdart_into(led3, [rec, rec2])
    ok.append(("T7 A 2건 → seed30/2=15.0", led3.candidates["A"][0]["capital_allocated"] == 15.0))
    # 빈 records → A 0건 (정상)
    led4 = new_ledger("2026-06-04")
    ok.append(("T8 빈 records → A 0건", record_sdart_into(led4, []) == 0))
    # EVENT hook: fresh snapshot 매칭 → A2 variant + event_layer (진입권한 0, 태그만)
    snap = {"fresh": True, "scanned_date": "2026-06-04", "reason": "ok",
            "index": {"999999": {"ticker": "999999", "name": "합성S종목", "total_score": 150.0,
                                  "direction": "POSITIVE", "events": ["BIG_CONTRACT"], "metric": "x"}}}
    led5 = new_ledger("2026-06-04")
    record_sdart_into(led5, [rec], event_snapshot=snap)
    row5 = led5.candidates["A"][0]
    ok.append(("T9 EVENT hook 매칭 → A2 / DART_S+NEWS / event_layer",
               row5["extra"]["a_variant"] == "A2" and row5["signal_source"] == "DART_S+NEWS"
               and row5["extra"]["event_layer"]["matched"] is True))
    # snapshot 없음(기본) → A1, event_layer None (DART_S primary 유지)
    led6 = new_ledger("2026-06-04")
    record_sdart_into(led6, [rec])
    row6 = led6.candidates["A"][0]
    ok.append(("T10 snapshot없음 → A1 / DART_S primary",
               row6["extra"]["a_variant"] == "A1" and row6["signal_source"] == "DART_S"))
    # 6/8 가격구조 라벨 — c2p 미전달 시 전 키 None(키 보존, 0/거짓 채우기 금지)
    led7 = new_ledger("2026-06-04")
    record_sdart_into(led7, [rec])
    e7 = led7.candidates["A"][0]["extra"]
    ok.append(("T11 A 가격구조 키 보존(c2p없음 → None)",
               "weekly_open_state" in e7 and e7["weekly_open_state"] is None
               and "float_candle_state" in e7 and e7["overheat_grade"] is None))
    # 6/8 A 메타필드(장부 명확화) — variant=a_variant / bucket=A_30 / entry_basis / virtual_entry_date / tp_touch_only
    led8 = new_ledger("2026-06-04")
    record_sdart_into(led8, [rec])
    row8 = led8.candidates["A"][0]
    ok.append(("T12 A 메타(variant=A1·bucket=A_30·entry_basis·entry_date·tp_touch_only)",
               row8["variant"] == "A1" and row8["capital_bucket"] == "A_30"
               and row8["entry_basis"] == "T0_CLOSE" and row8["virtual_entry_date"] == "2026-06-04"
               and row8["extra"]["tp_touch_only"] is True and row8["extra"]["tp_touch_observer"] is None))
    ok.append(("T13 A 갭/꼬리 키 보존(c2p없음 → None)",
               "gap_pct" in e7 and e7["gap_pct"] is None
               and "candle_shape" in e7 and e7["candle_shape"] is None))
    print("paper_3type_daily_run 셀프테스트:")
    for nme, p in ok:
        print(f"  [{'PASS' if p else 'FAIL'}] {nme}")
    return all(p for _, p in ok)


def _print_summary(res):
    sectors = res["sectors"]
    strong = [s for s in sectors if s["status"] in STRONG]
    print("=" * 100)
    print(f"paper_3type_daily_run — A/B/C 통합 (asof {res['asof']})  → {res['path']}")
    print(f"★ 한 ledger에 3타입 동일기준 기록 / 실주문0·scheduler무연결·SAJANG무변경·봇OFF")
    if res["skip_a"]:
        print("  A: [skip] (--skip-a)")
    else:
        print(f"  A(STEADY_S_DART): STEADY {res['n_steady']}건 중 호재공시 {res['a_n']}건 "
              f"→ A1(DART단독) {res['a1_n']} · A2(DART+NEWS) {res['a2_n']}")
        if res.get("event_note"):
            print(f"     EVENT hook: {res['event_note']}")
    print(f"  B/C: 강한섹터 {len(strong)}/{len(sectors)} → B {res['b_n']}건 · C {res['c_n']}건")
    b = res.get("breadth")
    if b:
        print(f"  시장(breadth): {b['breadth_pct']}({b['breadth_state']}) "
              f"상승{b['advancers']}/하락{b['decliners']} +3%{b['up_3pct']}/-3%{b['down_3pct']} "
              f"상한{b['limit_up']}/하한{b['limit_down']} 52신고{b['new_high_52w']}/신저{b['new_low_52w']} "
              f"거래대금top10집중{b['turnover_top10_share']}")
    elif res.get("skip_breadth"):
        print("  시장(breadth): [skip] (--skip-breadth)")
    print(f"  summary: {res['summary']}")
    print("=" * 100)
    print("★★ shadow=기록만. 6/4~6/12 forward 누적 → 6/12 타입별 1차 판정(영구룰 확정 아님).")
    print("★★ 명분(DART/NEWS/수급)=fact layer 기록, hard gate 아님. B/C 결과로 실전 flip 금지.")
    print("★★ A2(NEWS 보강)=forward 비교 태그일뿐 진입권한 0. NEWS 백필불가→fresh(당일 스캔)만 A2.")


def main():
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    ap = argparse.ArgumentParser()
    ap.add_argument("--asof", default=date.today().isoformat())
    ap.add_argument("--skip-a", action="store_true", help="A(sdart, 네트워크) 건너뛰고 B/C만")
    ap.add_argument("--scan-events", action="store_true",
                    help="event_detector 실시간 스캔(네트워크: DART+네이버)으로 events.json 갱신 → A2 판정")
    ap.add_argument("--skip-breadth", action="store_true",
                    help="전종목 시장 폭넓이(~83초) 생략 — 빠른 점검용")
    ap.add_argument("--selftest", action="store_true")
    a = ap.parse_args()
    if a.selftest:
        sys.exit(0 if selftest() else 1)
    try:
        asof = date.fromisoformat(a.asof).isoformat()
    except ValueError:
        print(f"[중단] --asof 형식 오류: {a.asof} (YYYY-MM-DD 필요)")
        sys.exit(2)
    res = run(asof, skip_a=a.skip_a, scan_events=a.scan_events, skip_breadth=a.skip_breadth)
    _print_summary(res)


if __name__ == "__main__":
    main()
