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
    _code_to_path, scan_sectors, collect_candidates, record_bc_into, STRONG)

A_SEED = 30.0   # 설계 5장 A30/B35/C35 (B/C 시드는 rotation_paper_scan.TYPE_SEED)


# ── A(sdart) 어댑터: sdart build_record dict → paper_3type ledger "A" record ──
def record_sdart_into(led, records):
    """sdart_shadow_record.scan_asof 의 records → led.record("A", ...).
    명분(grade)은 signal_source(fact layer)로 기록만 — hard gate 아님."""
    n = len(records)
    per = round(A_SEED / n, 2) if n else 0.0
    pct = round(100.0 / n, 1) if n else 0.0
    for r in records:
        cat = r.get("catalyst", {}) or {}
        grade = cat.get("grade")
        sig = f"DART_{grade}" if grade in ("S", "A", "B") else "STEADY"
        entry = (r.get("entry") or {}).get("t0_close")
        val = (f"매출대비{cat.get('sales_ratio_pct')}%" if cat.get("sales_ratio_pct") is not None
               else (f"자사주{cat.get('buyback_amt_억')}억" if cat.get("buyback_amt_억") is not None
                     else cat.get("detail")))
        led.record(
            "A",
            ticker=r.get("code"), name=r.get("name"), sector=r.get("sector"),
            group=r.get("track"),
            entry_reason=f"STEADY+{cat.get('kind')}/{grade} {val}",
            signal_source=sig,
            virtual_entry_price=entry,
            MFE=r.get("mfe_pct_hold40"), MAE=r.get("mae_pct_hold40"),
            holding_days=0, supply=None, market_regime=None,
            capital_allocated=per, position_size_pct=pct,
            # extra (넓게 병행 — sdart forward 부품 전체 보존)
            catalyst=cat, entry=r.get("entry"),
            raw_fwd_from_t0close=r.get("raw_fwd_from_t0close"),
            raw_fwd_from_t1open=r.get("raw_fwd_from_t1open"),
            would_stop=r.get("would_stop"), would_exit=r.get("would_exit"),
            cap_억=r.get("cap_억"), track=r.get("track"),
            surge_pct=r.get("surge_pct"), turnover_억=r.get("turnover_억"),
            high_120d=r.get("high_120d"), t0_date=r.get("t0_date"),
            sdart_status=r.get("status"),
        )
    return n


def run(asof, skip_a=False):
    """A/B/C를 한 ledger에 기록 → save. 반환 dict(요약)."""
    led = new_ledger(asof)
    # A — sdart (네트워크: DART 조회)
    a_n = 0
    n_steady = 0
    if not skip_a:
        from sdart_shadow_record_6_3 import scan_asof  # noqa: E402 (무거운 의존 — 필요시만)
        records, n_steady = scan_asof(asof)
        a_n = record_sdart_into(led, records)
    # B/C — rotation (네트워크 0)
    c2p = _code_to_path()
    sectors = scan_sectors(c2p, asof)
    cands = collect_candidates(sectors)
    record_bc_into(led, cands)
    path = led.save()
    nb = sum(1 for t, _, _ in cands if t == "B")
    nc = sum(1 for t, _, _ in cands if t == "C")
    return {"asof": asof, "path": path, "sectors": sectors,
            "a_n": a_n, "n_steady": n_steady, "b_n": nb, "c_n": nc,
            "summary": led.summary(), "skip_a": skip_a}


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
        print(f"  A(STEADY_S_DART): STEADY {res['n_steady']}건 중 호재공시 {res['a_n']}건")
    print(f"  B/C: 강한섹터 {len(strong)}/{len(sectors)} → B {res['b_n']}건 · C {res['c_n']}건")
    print(f"  summary: {res['summary']}")
    print("=" * 100)
    print("★★ shadow=기록만. 6/4~6/12 forward 누적 → 6/12 타입별 1차 판정(영구룰 확정 아님).")
    print("★★ 명분(DART/NEWS/수급)=fact layer 기록, hard gate 아님. B/C 결과로 실전 flip 금지.")


def main():
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    ap = argparse.ArgumentParser()
    ap.add_argument("--asof", default=date.today().isoformat())
    ap.add_argument("--skip-a", action="store_true", help="A(sdart, 네트워크) 건너뛰고 B/C만")
    ap.add_argument("--selftest", action="store_true")
    a = ap.parse_args()
    if a.selftest:
        sys.exit(0 if selftest() else 1)
    try:
        asof = date.fromisoformat(a.asof).isoformat()
    except ValueError:
        print(f"[중단] --asof 형식 오류: {a.asof} (YYYY-MM-DD 필요)")
        sys.exit(2)
    res = run(asof, skip_a=a.skip_a)
    _print_summary(res)


if __name__ == "__main__":
    main()
