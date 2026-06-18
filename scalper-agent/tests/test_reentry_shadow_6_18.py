# -*- coding: utf-8 -*-
"""재진입 shadow 셀프테스트 (6/18) — 네트워크 0·결정적 합성 일봉.

검증:
  A. SAJANG.should_reenter / effective_hold_days 경계값
  B. _simulate 결정적 케이스 (HOLD_EXIT·STOP만·reclaim 재진입·고점트레일링 락인·미해결None)
  C. 불변식: n_reentries ≤ REENTRY_MAX · n_legs ≤ REENTRY_MAX+1 (실 데이터 멱등)
  D. 리터럴 0.97 등 금지 — reentry_shadow.py가 SAJANG 헬퍼만 사용 (RULE-005/006/007 취지)
"""
import re
import sys
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE))

from data.sajang_rules import SAJANG  # noqa: E402
from data.reentry_shadow import _simulate  # noqa: E402


def _rows(*tuples):
    """(close, high, low) 튜플들 → [(date, close, high, low)] (date는 순번 문자열)."""
    return [(f"2026-01-{i+1:02d}", float(c), float(h), float(l)) for i, (c, h, l) in enumerate(tuples)]


def run():
    passed = failed = 0
    fails = []

    def check(name, cond):
        nonlocal passed, failed
        if cond:
            passed += 1
        else:
            failed += 1
            fails.append(name)

    # ── A. 헬퍼 경계 ──
    check("eff_hold EXPLOSIVE=RIDE", SAJANG.effective_hold_days("EXPLOSIVE") == SAJANG.RIDE_MAX_DAYS)
    check("eff_hold HUNTABLE=BASE", SAJANG.effective_hold_days("HUNTABLE") == SAJANG.HOLD_BASE_DAYS)
    check("reenter ok", SAJANG.should_reenter(100, 99, "MODERATE", 0, 2, True) is True)
    check("reenter below reclaim", SAJANG.should_reenter(98, 99, "MODERATE", 0, 2, True) is False)
    check("reenter SLUGGISH block", SAJANG.should_reenter(100, 99, "SLUGGISH", 0, 2, True) is False)
    check("reenter max block", SAJANG.should_reenter(100, 99, "HUNTABLE", SAJANG.REENTRY_MAX, 2, True) is False)
    check("reenter window block", SAJANG.should_reenter(100, 99, "HUNTABLE", 0, SAJANG.HOLD_BASE_DAYS + 1, True) is False)
    check("reenter RIDE window ok", SAJANG.should_reenter(100, 99, "EXPLOSIVE", 0, SAJANG.HOLD_BASE_DAYS + 1, True) is True)
    check("reenter thesis block", SAJANG.should_reenter(100, 99, "HUNTABLE", 0, 2, False) is False)

    # ── B. _simulate 결정적 케이스 (grade HUNTABLE = hold 3일) ──
    # B1. HOLD_EXIT: 계속 상승, 손절 미발동 → D+3 종가 청산
    r = _simulate(_rows((100, 100, 100), (102, 103, 101), (104, 105, 103), (106, 107, 105)), 0, "HUNTABLE")
    check("B1 hold_exit ret", r is not None and r["stop_reenter_ret"] == 6.0)
    check("B1 hold_exit no reentry", r and r["n_reentries"] == 0 and r["leg_reasons"] == ["HOLD_EXIT"])

    # B2. STOP만(reclaim 없음): 진입가-3% 손절 후 윈도우 내 회복 못함
    r = _simulate(_rows((100, 100, 100), (90, 95, 88), (89, 92, 87), (88, 90, 86),
                        (87, 89, 85), (86, 88, 84)), 0, "HUNTABLE")
    # 손절선 normal=int(100*0.97)=97, 갭다운 클램프 min(97, high95)=95 → -5.0%
    check("B2 stop ret -5", r is not None and r["stop_reenter_ret"] == -5.0)
    check("B2 stop only no reentry", r and r["n_reentries"] == 0 and r["leg_reasons"] == ["STOP"])

    # B3. reclaim 재진입 + 고점트레일링: 손절 후 회복→재진입→상승 종가청산
    r = _simulate(_rows((100, 100, 100), (96, 99, 96), (102, 103, 100), (101, 104, 100),
                        (105, 106, 103), (107, 108, 105)), 0, "HUNTABLE")
    check("B3 reentry happened", r is not None and r["n_reentries"] == 1)
    check("B3 legs STOP+HOLD_EXIT", r and r["leg_reasons"] == ["STOP", "HOLD_EXIT"])
    check("B3 stop_only=-3", r and r["stop_only_ret"] == -3.0)
    # 복리 0.97 * (107/102) ≈ +1.75%
    check("B3 compound ~1.75", r and abs(r["stop_reenter_ret"] - 1.75) < 0.05)

    # B4. 고점트레일링 락인: 급등 후 트레일 손절이 +수익으로 체결 (진입가-3% 아님)
    # 진입100 → 고가110(+10%, 트레일활성) → 저가가 트레일선 int(110*0.97)=106 이하로
    r = _simulate(_rows((100, 100, 100), (108, 110, 104), (104, 107, 103),
                        (103, 105, 102), (102, 104, 101)), 0, "HUNTABLE")
    # j1: peak갱신전 sl=normal97, low104>97 통과 → peak=110. j2: gain(110/100)=10%→trail sl=int(110*0.97)=106, low103<=106 → STOP fill min(106,high107)=106 → +6.0%
    check("B4 trail locks gain +6", r is not None and round(r["stop_only_ret"], 1) == 6.0)

    # B5. 미해결(bar 부족) → None
    r = _simulate(_rows((100, 100, 100), (102, 103, 101)), 0, "HUNTABLE")
    check("B5 unresolved None", r is None)

    # ── C. 불변식 (실 데이터가 있으면) ──
    try:
        import json
        from data.reentry_shadow import SHADOW_OUT
        if SHADOW_OUT.exists():
            recs = json.loads(SHADOW_OUT.read_text(encoding="utf-8")).get("records", [])
            ok_cap = all(rec["n_reentries"] <= SAJANG.REENTRY_MAX for rec in recs)
            ok_legs = all(rec["n_legs"] <= SAJANG.REENTRY_MAX + 1 for rec in recs)
            check("C reentry cap invariant", ok_cap)
            check("C legs invariant", ok_legs)
    except Exception as e:
        # 실데이터 없으면 skip이 정상이나, 진짜 예외는 FAIL로 집계(묻힘 방지)
        check(f"C unexpected error: {e}", False)

    # ── D. 리터럴 금지 (SAJANG 단일진실) ──
    src = (BASE / "data" / "reentry_shadow.py").read_text(encoding="utf-8")
    # 트레일 리터럴이 실제 산술연산에 쓰이면 안 됨 (산문 "리터럴 0.97 금지"는 오탐 제외 — 연산자 인접만)
    code_lines = [ln.split("#")[0] for ln in src.splitlines()]
    bad = [ln for ln in code_lines if re.search(r"[*/]\s*0\.9[0-9]|0\.9[0-9]\s*[*/]", ln)]
    check("D no trailing literal", not bad)
    check("D uses SAJANG helpers", "SAJANG.get_trailing_sl" in src and "SAJANG.get_normal_sl" in src)

    print(f"\n[test_reentry_shadow] PASS {passed} / FAIL {failed}")
    if fails:
        print("  실패:", fails)
    return failed == 0


if __name__ == "__main__":
    ok = run()
    sys.exit(0 if ok else 1)
