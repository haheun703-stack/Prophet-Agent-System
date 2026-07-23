# -*- coding: utf-8 -*-
"""페이퍼 판정지표 방어선 테스트 (7/23) — observe_v2_paper.

배경: 7/23 판정 기준을 "전 신호 무제한 합산"에서 "유효일 × 일일 상한 CAP_VIEW_N건"
으로 승격(사장님 확정)하면서, Tier1 검수가 지적한 **판정 지표의 공통 실패 모드**를
막았다. 그 실패 모드는 "틀린 값"이 아니라 **"증거 부재가 조용히 0이 되어 ❌미달로
위장"**이다. 이 테스트는 그 방어선들이 실제로 작동하는지 손상 상황을 재현해 검증한다.

검증 범위:
  A. H-1  상한 집계는 박제 키가 아니라 원본 trades에서 계산 → CAP_VIEW_N 변경 면역
  B. M-1  장부 파싱 실패 시 저장 차단(과거 전량 소실 방지) / 파일 부재는 정상 허용
  C. M-2  trades↔summary.n 불일치일은 유효일에서 제외 + torn_days로 표면화
  D. M-3  verdict 미확인일이 정상 분기로 새지 않음(자기모순 메시지 차단)
  E. M-4  CB 배지는 상한 수치 옆이 아니라 참고선에(안전핀 오독 차단)
  F. 안전 불변식 — 주문/SAJANG 0접촉·실장부 무접촉

★ 합성 장부만 사용(실장부·실주문 0접촉). 라이브 수치에 의존하지 않으므로 매일 유효.
실행: python tests/test_paper_judge_guards_7_23.py
"""
import ast
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

import observe_v2_paper as m  # noqa: E402

PASS = 0
FAILS = []


def check(name, cond):
    global PASS
    if cond:
        PASS += 1
    else:
        FAILS.append(name)


def _trade(net, t="09:2%d:00"):
    return {"code": "000000", "signal_time": "09:01:00", "entry_time": t,
            "entry_price": 1000.0, "exit_time": "10:00:00",
            "why": "TP" if net > 0 else "SL", "ret": net + 0.2, "net": net}


def _day(date_s, nets, verdict="OK", cb=False):
    """합성 1일 레코드 — summary.n은 trades와 정합하게 생성."""
    tr = [_trade(x) for x in nets]
    cap = tr[:m.CAP_VIEW_N]
    return {"date": date_s, "trades": tr, "skipped": [],
            "summary": {"n": len(tr), "wins": sum(1 for t in tr if t["net"] > 0),
                        "sum_net": round(sum(t["net"] for t in tr), 2),
                        "cb_triggered": cb, "cb_trigger_time": "09:47:59" if cb else None,
                        "skipped_cb": 0, "skipped_no_fill": 0,
                        "cap_sum_net": round(sum(t["net"] for t in cap), 2),
                        "cap_wins": sum(1 for t in cap if t["net"] > 0),
                        "cap_n": len(cap),
                        "ticks_health": ({"verdict": verdict} if verdict else {}),
                        "params": {"cap_limit": m.CAP_VIEW_N}}}


# 유효일 1개(7건: +1,+1,-1,-1,-1,+2,+2 → 상한5 = +1+1-1-1-1 = -1.0 / 승 2)
LED = {"days": {
    "20260720": _day("20260720", [1.0, 1.0, -1.0, -1.0, -1.0, 2.0, 2.0], "OK", cb=True),
    "20260721": _day("20260721", [-3.0, -3.0], "BROKEN"),      # 수집장애 — 판정 제외
}}

# ── A. H-1: 상한 집계가 CAP_VIEW_N 변경에 면역인가 ──
orig_n = m.CAP_VIEW_N
try:
    m.CAP_VIEW_N = 5
    c5 = m._cum(json.loads(json.dumps(LED)))
    m.CAP_VIEW_N = 3
    c3 = m._cum(json.loads(json.dumps(LED)))
finally:
    m.CAP_VIEW_N = orig_n
check("A1 상한5 = 5건 -1.00%p", (c5["valid_cap_n"], c5["valid_cap_sum"]) == (5, -1.0))
check("A2 상한3 = 3건 +1.00%p (박제키였다면 0건 붕괴)",
      (c3["valid_cap_n"], c3["valid_cap_sum"]) == (3, 1.0))
check("A3 BROKEN일은 판정에서 제외", c5["valid_days"] == 1 and c5["broken_days"] == 1)
check("A4 참고선(무제한)은 별도 유지", c5["valid_n"] == 7 and c5["valid_sum"] == 3.0)

# ── B. M-1: 장부 파싱 실패 시 저장 차단 ──
real_path = m.LEDGER_PATH
with tempfile.TemporaryDirectory() as td:
    tmp = Path(td)
    bad = tmp / "corrupt.json"
    bad.write_text("{ not valid json", encoding="utf-8")
    m.LEDGER_PATH = bad
    try:
        m.save_day({"date": "20260723", "trades": [], "skipped": [], "summary": {"n": 0}})
        check("B1 손상 장부 저장 차단", False)
    except m.LedgerLoadError:
        check("B1 손상 장부 저장 차단", True)
    except Exception:  # noqa: BLE001
        check("B1 손상 장부 저장 차단", False)
    check("B2 손상 원본 미변경(과거 소실 없음)",
          bad.read_text(encoding="utf-8").startswith("{ not valid"))

    absent = tmp / "absent.json"
    m.LEDGER_PATH = absent
    check("B3 파일 부재(정상 초회)는 빈 장부 허용", m._load_ledger(strict=True) == {"days": {}})

    # ── C. M-2: trades 훼손일 제외 + 표면화 ──
    torn = json.loads(json.dumps(LED))
    torn["days"]["20260720"]["trades"] = torn["days"]["20260720"]["trades"][:2]  # n=7인데 2건
    ct = m._cum(torn)
    check("C1 훼손일 탐지", ct["torn_days"] == ["20260720"])
    check("C2 훼손일 유효일 제외", ct["valid_days"] == 0 and ct["valid_cap_n"] == 0)
    check("C3 정상 장부는 훼손 0", c5["torn_days"] == [])

    # ── D/E. 리포트 분기 (합성 장부를 임시 경로로) ──
    led_f = tmp / "led.json"
    led_f.write_text(json.dumps(LED, ensure_ascii=False), encoding="utf-8")
    m.LEDGER_PATH = led_f
    rep = m.build_report("20260720").split("\n")
    check("E1 상한 라인에 CB 배지 없음", "CB" not in rep[1])
    check("E2 참고선에 CB + 미적용 명시", "CB" in rep[2] and "미적용" in rep[2])
    check("E3 상한 수치가 주 수치", "5건(상한)" in rep[1] and "-1.00%p" in rep[1])

    unk = json.loads(json.dumps(LED))
    unk["days"]["20260720"]["summary"]["ticks_health"] = {}      # verdict 없음
    led_f.write_text(json.dumps(unk, ensure_ascii=False), encoding="utf-8")
    rep_u = m.build_report("20260720").split("\n")
    check("D1 미확인일은 '건강도 미확인' 표기", "건강도 미확인" in rep_u[1])
    check("D2 미확인일에 상한 수치 미출력", "(상한)" not in rep_u[1])
m.LEDGER_PATH = real_path

# ── F. 안전 불변식 ──
src = (ROOT / "tools" / "observe_v2_paper.py").read_text(encoding="utf-8")
check("F1 주문 API 0접촉", all(k not in src for k in
      ("place_order", "buy_stock", "sell_stock", "order_cash", "kis_trader")))
# SAJANG 무접촉은 '문자열 부재'가 아니라 '결합 부재'로 판정한다(AST) — docstring이
# 근거로 SAJANG.CASH_RESERVE_PCT를 인용하는 건 정상이며 문자열 검사는 오탐한다(7/23).
_tree = ast.parse(src)
_imports = {a.name for n in ast.walk(_tree) if isinstance(n, ast.Import) for a in n.names}
_imports |= {n.module or "" for n in ast.walk(_tree) if isinstance(n, ast.ImportFrom)}
_names = {n.id for n in ast.walk(_tree) if isinstance(n, ast.Name)}
check("F2 SAJANG 모듈 미import", not any("sajang" in i.lower() for i in _imports))
check("F2b 코드에서 SAJANG 심볼 미사용(주석/독스트링 인용은 허용)", "SAJANG" not in _names)
check("F3 박제 키 동적 조회 잔존 0(H-1 재발 방지)", 'cap{CAP_VIEW_N}' not in src)
check("F4 실장부 경로 원복", m.LEDGER_PATH == real_path)

print(f"\n{'✅' if not FAILS else '❌'} PASS {PASS} / FAIL {len(FAILS)}"
      + (f" — {FAILS}" if FAILS else ""))
sys.exit(1 if FAILS else 0)
