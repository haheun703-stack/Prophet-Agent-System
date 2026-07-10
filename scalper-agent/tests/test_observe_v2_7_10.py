# -*- coding: utf-8 -*-
"""OBSERVE 러너 v2 테스트 (7/10) — observe_v2_runner.

검증 범위:
  A. _scan_v2_signals : ⑲ replay와 동일 규칙(최초 조건행·v2 필터 3종)·enrich·불량행
  B. _merge_intents   : 멱등·신규만 detected_at·enrich만 갱신(additive)·날짜 리셋
  C. _compare         : matched/drift/missed/phantom/지연·verdict 판정
  D. 안전 불변식      : 주문/SAJANG 코드 0접촉(소스 정적 검사)·run_once 휴장/ticks없음 skip

★ OBSERVE = 실주문 0. 순수 함수+합성 ticks만 사용.
실행: python tests/test_observe_v2_7_10.py
"""
import json
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "tools"))

try:
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")
except Exception:
    pass

import playbook_shadow as pb  # noqa: E402
import observe_v2_runner as ov  # noqa: E402

PASS = 0
FAILS = []


def check(name, cond):
    global PASS
    if cond:
        PASS += 1
    else:
        FAILS.append(name)
        print(f"  ❌ {name}")


def _csv(rows):
    head = "time,price,change,change_rate,ask1,bid1,strength,volume,tick_volume\n"
    return head + "\n".join(
        f"{t},{p},0,{cr},0,0,{st},0,0" for t, p, cr, st in rows)


DAY = "20260710"
with tempfile.TemporaryDirectory() as td:
    tdir = Path(td) / DAY
    tdir.mkdir(parents=True)
    # v2 성립: 09:30 +5.5% 강도250 → 신호, 다음 행 = 진입
    (tdir / "111.csv").write_text(_csv([
        ("09:10:00", 1000, 1.0, 50), ("09:30:00", 1055, 5.5, 250),
        ("09:50:00", 1060, 6.0, 180)]), encoding="utf-8")
    # 추격 초과(+9%) → 제외
    (tdir / "222.csv").write_text(_csv([
        ("09:20:00", 2180, 9.0, 300), ("09:40:00", 2200, 10.0, 300)]), encoding="utf-8")
    # 강도 미달(150) → 제외
    (tdir / "333.csv").write_text(_csv([
        ("09:20:00", 3150, 5.2, 150), ("09:40:00", 3200, 6.8, 300)]), encoding="utf-8")
    # 신호가 10시 이후 → 제외 (최초 조건행 기준 — 이후 행이 조건 맞아도 재선택 없음)
    (tdir / "444.csv").write_text(_csv([
        ("10:30:00", 4210, 5.3, 400), ("10:50:00", 4300, 7.5, 400)]), encoding="utf-8")
    # 진입행 아직 없음(신호가 마지막 행) → 신호만, enrich 대기
    (tdir / "555.csv").write_text(_csv([
        ("09:15:00", 5000, 2.0, 100), ("09:55:00", 5300, 6.0, 220)]), encoding="utf-8")
    # 불량행(price=0)만 → 파서가 걸러 행<2 → skip
    (tdir / "666.csv").write_text(_csv([
        ("09:20:00", 0, 5.5, 300)]), encoding="utf-8")

    orig = pb.TICKS
    pb.TICKS = Path(td)
    try:
        sig = ov._scan_v2_signals(DAY)
    finally:
        pb.TICKS = orig

check("A1 v2 성립 1건+enrich 대기 1건", set(sig.keys()) == {"111", "555"})
check("A2 신호행 필드", sig["111"]["signal_time"] == "09:30:00"
      and sig["111"]["signal_chg"] == 5.5 and sig["111"]["strength"] == 250)
check("A3 진입 enrich(다음 관측행)", sig["111"]["entry_time"] == "09:50:00"
      and sig["111"]["entry_price"] == 1060)
check("A4 진입행 없으면 신호만", "entry_time" not in sig["555"])
check("A5 추격/강도/시간 필터", all(c not in sig for c in ("222", "333", "444")))

# ── B. _merge_intents ──
st1, fresh1 = ov._merge_intents({}, {"111": dict(sig["111"])}, DAY, "09:35:01")
check("B1 신규 detected_at 스탬프", fresh1[0]["detected_at"] == "09:35:01"
      and st1["intents"]["111"]["detected_at"] == "09:35:01")
# 재실행(멱등): 같은 신호 다시 → 신규 0·detected_at 불변
st2, fresh2 = ov._merge_intents(st1, {"111": dict(sig["111"])}, DAY, "09:40:01")
check("B2 멱등(신규 0·스탬프 불변)", not fresh2
      and st2["intents"]["111"]["detected_at"] == "09:35:01")
# enrich: 555 신호 먼저 → 다음 런에 진입행 등장
st3, f3 = ov._merge_intents(st2, {"555": dict(sig["555"])}, DAY, "10:00:01")
found_enriched = dict(sig["555"], entry_time="10:15:00", entry_price=5350)
st4, f4 = ov._merge_intents(st3, {"555": found_enriched}, DAY, "10:20:01")
check("B3 enrich만 갱신(신규 아님)", not f4
      and st4["intents"]["555"]["entry_time"] == "10:15:00"
      and st4["intents"]["555"]["detected_at"] == "10:00:01")
check("B4 날짜 바뀌면 리셋", ov._merge_intents(st4, {}, "20260713", "09:00:00")[0]
      ["intents"] == {})

# ── C. _compare ──
li = {"111": {"signal_time": "09:30:00", "detected_at": "09:35:01"},
      "555": {"signal_time": "09:55:00", "detected_at": "10:00:01"},
      "777": {"signal_time": "09:40:00", "detected_at": "09:45:00"}}   # phantom
tr = {"111": {"signal_time": "09:30:00"},
      "555": {"signal_time": "09:50:00"},                              # drift
      "888": {"signal_time": "09:20:00"}}                              # missed
r = ov._compare(li, tr)
check("C1 matched 1·drift 1·missed 1·phantom 1",
      r["matched"] == 1 and len(r["time_drift"]) == 1
      and r["missed"] == ["888"] and r["phantom"] == ["777"])
check("C2 지연 301초", r["latency_sec"]["median"] == 301)
check("C3 불일치=CHECK", r["verdict"] == "CHECK")
r_ok = ov._compare({"111": {"signal_time": "09:30:00", "detected_at": "09:31:00"}},
                   {"111": {"signal_time": "09:30:00"}})
check("C4 전건 일치=PASS", r_ok["verdict"] == "PASS")
check("C5 신호 0건=EMPTY", ov._compare({}, {})["verdict"] == "EMPTY")
check("C6 phantom-only=CHECK(EMPTY 위장 금지)", ov._compare(
    {"999": {"signal_time": "09:30:00"}}, {})["verdict"] == "CHECK")

# ── D. 안전 불변식 ──
src = (ROOT / "tools" / "observe_v2_runner.py").read_text(encoding="utf-8")
check("D1 주문 API 0접촉", all(k not in src for k in
      ("place_order", "buy_stock", "sell_stock", "order_cash", "kis_trader")))
check("D2 SAJANG 값 무접촉", "SAJANG." not in src and "sajang_rules" not in src)
check("D3 ticks 없으면 skip(로컬 안전)",
      ov.run_once(day="19990101") == 0)

print(f"\n{'✅' if not FAILS else '❌'} PASS {PASS} / FAIL {len(FAILS)}"
      + (f" — {FAILS}" if FAILS else ""))
sys.exit(1 if FAILS else 0)
