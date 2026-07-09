# -*- coding: utf-8 -*-
"""매매원칙 지시서 관찰 백테스트(7/9) 테스트 — playbook_shadow principles_7_9.

검증 범위:
  A. _chase_bands          : 대역 경계(CHASE_MAX=8.0 정확 경계)·합계 보존
  B. _cb_sim(streak)       : 연속 SL 3회 발동·TP 리셋·EOD 중립·발동 후 진입만 차단
  C. _cb_sim(loss)         : 누적 %p 한도 발동·미달 시 무발동
  D. _sector_cooldown_sim  : 동일 섹터 3SL → 당일 잔여+익일 차단·타 섹터 무접촉·중복 제거
  E. _principles_7_9       : 통합 출력 구조·차단 0(관찰 전용) 불변식

★ record-only 순수 함수만 — 매수/매도/SAJANG/주문 0접촉.
실행: python tests/test_principles_7_9.py
"""
import sys
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

PASS = 0
FAILS = []


def check(name, cond):
    global PASS
    if cond:
        PASS += 1
    else:
        FAILS.append(name)
        print(f"  ❌ {name}")


def _t(date="20260708", code="111", chg=5.5, e_t="09:30:00", x_t="10:00:00",
       ret=1.0, why="TP", strength=250.0, sig_t="09:20:00"):
    return {"date": date, "code": code, "signal_chg": chg, "signal_time": sig_t,
            "entry_time": e_t, "exit_time": x_t, "ret": ret, "why": why,
            "strength_at_signal": strength, "regime_gate": True,
            "strength_gate": True}


# ── A. _chase_bands 경계 ──
ref = [_t(chg=5.0), _t(chg=7.99), _t(chg=8.0), _t(chg=12.0)]
cb = pb._chase_bands(ref)
check("A1 under(<8)=2", cb["under_chase_max"]["trades"] == 2)
check("A2 over(≥8)=2", cb["over_chase_max"]["trades"] == 2)
check("A3 대역 합=전체", sum(cb[k].get("trades", 0) for k in cb
                          if k.startswith("chg_")) == 4)
check("A4 10+ 대역", cb["chg_10~"]["trades"] == 1)

# ── B. _cb_sim streak — SL,SL,TP(리셋),SL,SL,SL(발동)→이후 진입 차단 ──
day = [
    _t(e_t="09:10", x_t="09:20", why="SL", ret=-3.0),
    _t(e_t="09:15", x_t="09:30", why="SL", ret=-3.0),
    _t(e_t="09:25", x_t="09:40", why="TP", ret=5.0),     # 리셋
    _t(e_t="09:35", x_t="09:50", why="SL", ret=-3.0),
    _t(e_t="09:45", x_t="10:00", why="SL", ret=-3.0),
    _t(e_t="09:55", x_t="10:10", why="SL", ret=-3.0),    # 3연속 → 발동 @10:10
    _t(e_t="10:20", x_t="10:30", why="SL", ret=-3.0),    # 차단 대상
    _t(e_t="10:40", x_t="15:20", why="EOD", ret=2.0),    # 차단 대상
    _t(e_t="10:05", x_t="10:15", why="TP", ret=5.0),     # 발동 전 진입 → 통과
]
r = pb._cb_sim(day, "streak")
check("B1 발동 1일", r["days_triggered"] == 1)
check("B2 차단 2건(발동 후 진입만)", r["trades"] == 2)
check("B3 차단분 합 -1.0%p", r["sum_ret"] == -1.0)
r2 = pb._cb_sim([_t(e_t="09:10", x_t="09:20", why="SL", ret=-3.0),
                 _t(e_t="09:15", x_t="09:30", why="EOD", ret=0.0),   # EOD 중립
                 _t(e_t="09:25", x_t="09:40", why="SL", ret=-3.0),
                 _t(e_t="09:35", x_t="09:50", why="SL", ret=-3.0),   # 3연속(EOD 무시)
                 _t(e_t="10:00", x_t="10:10", why="TP", ret=5.0)], "streak")
check("B4 EOD 중립(리셋 아님)·발동", r2["days_triggered"] == 1 and r2["trades"] == 1)

# ── C. _cb_sim loss ──
day_l = [
    _t(e_t="09:10", x_t="09:20", why="SL", ret=-3.0),
    _t(e_t="09:15", x_t="09:30", why="SL", ret=-3.0),    # 누적 -6.0 → 발동 @09:30
    _t(e_t="09:40", x_t="10:00", why="SL", ret=-3.0),    # 차단
]
rl = pb._cb_sim(day_l, "loss", limit=-6.0)
check("C1 -6p 발동·차단 1건 합 -3", rl["days_triggered"] == 1 and rl["sum_ret"] == -3.0)
check("C2 경계 -9p 정확 도달=발동(차단 0)", (lambda r9: r9["days_triggered"] == 1
      and r9["trades"] == 0)(pb._cb_sim(day_l, "loss", limit=-9.0)))
check("C3 -10p 미달=무발동", pb._cb_sim(day_l, "loss", limit=-10.0)["days_triggered"] == 0)

# ── D. _sector_cooldown_sim ──
smap = {"111": "반도체", "222": "반도체", "333": "화장품"}
d1, d2 = "20260708", "20260709"
sec_trades = [
    _t(date=d1, code="111", e_t="09:10", x_t="09:20", why="SL", ret=-3.0),
    _t(date=d1, code="222", e_t="09:15", x_t="09:30", why="SL", ret=-3.0),
    _t(date=d1, code="111", e_t="09:25", x_t="09:40", why="SL", ret=-3.0),  # 3회 @09:40
    _t(date=d1, code="222", e_t="10:00", x_t="10:30", why="SL", ret=-3.0),  # 당일 차단
    _t(date=d1, code="333", e_t="10:05", x_t="10:35", why="TP", ret=5.0),   # 타 섹터 통과
    _t(date=d2, code="111", e_t="09:30", x_t="10:00", why="SL", ret=-3.0),  # 익일 차단
    _t(date=d2, code="333", e_t="09:35", x_t="10:05", why="TP", ret=5.0),   # 타 섹터 통과
]
rs = pb._sector_cooldown_sim(sec_trades, smap)
check("D1 발동 1회", rs["trigger_events"] == 1)
check("D2 차단 2건(당일1+익일1)·합 -6", rs["trades"] == 2 and rs["sum_ret"] == -6.0)
check("D3 섹터맵 없으면 차단 0", pb._sector_cooldown_sim(sec_trades, {})["trades"] == 0)
# 익일 자체 발동과 겹쳐도 중복 없음: d2에 반도체 3SL 추가 → d2 잔여 차단과 익일 차단 중복 dedup
sec_trades2 = sec_trades + [
    _t(date=d2, code="222", e_t="09:40", x_t="10:10", why="SL", ret=-3.0),
    _t(date=d2, code="222", e_t="09:50", x_t="10:20", why="SL", ret=-3.0),
    _t(date=d2, code="111", e_t="10:30", x_t="11:00", why="SL", ret=-3.0),
]
rs2 = pb._sector_cooldown_sim(sec_trades2, smap)
check("D4 중복 dedup(차단 건수=고유 거래)", rs2["trades"] == len({
    (t["date"], t["code"], t["entry_time"]) for t in sec_trades2
    if smap.get(t["code"]) == "반도체" and (
        (t["date"] == d1 and t["entry_time"] > "09:40") or t["date"] == d2)}))

# D5: gap-day 편향 fix — 익일은 전 ticked 거래일 기준(정제판 거래 없는 날 건너뛰기 금지)
gap_trades = [
    _t(date=d1, code="111", e_t="09:10", x_t="09:20", why="SL", ret=-3.0),
    _t(date=d1, code="222", e_t="09:15", x_t="09:30", why="SL", ret=-3.0),
    _t(date=d1, code="111", e_t="09:25", x_t="09:40", why="SL", ret=-3.0),  # 발동 d1
    _t(date="20260710", code="222", e_t="09:30", x_t="10:00", why="SL", ret=-3.0),
]
# all_days에 d2(정제판 거래 0인 거래일) 존재 → 익일=d2(빈 날)·20260710은 차단 안 됨
r_gap = pb._sector_cooldown_sim(gap_trades, smap, all_days=[d1, d2, "20260710"])
check("D5 gap-day 편향 fix(익일=실제 다음 거래일)", r_gap["trades"] == 0)
# fallback(all_days 미지정)이면 종전처럼 20260710이 익일로 착지(차단 1) — 편향 재현 확인
check("D6 fallback 편향 재현(대조)", pb._sector_cooldown_sim(gap_trades, smap)["trades"] == 1)

# ── E. _principles_7_9 통합 구조 ──
out = pb._principles_7_9([_t(strength=250.0), _t(strength=150.0)])  # 정제판 1건만
check("E1 출력 키", set(out.keys()) == {
    "rule2_chase", "rule3_circuit_breaker", "rule4b_sector_cooldown", "note"})
check("E2 정제판만 대상", out["rule2_chase"]["under_chase_max"]["trades"] == 1)
check("E3 CB 변형 3종", set(out["rule3_circuit_breaker"].keys()) == {
    "streak3", "daily_-6p", "daily_-9p"})
check("E4 관찰 전용 명시", "차단 0" in out["note"])
check("E5 빈 입력 안전", pb._principles_7_9([])["rule4b_sector_cooldown"]["trades"] == 0)

print(f"\n{'✅' if not FAILS else '❌'} PASS {PASS} / FAIL {len(FAILS)}"
      + (f" — {FAILS}" if FAILS else ""))
sys.exit(1 if FAILS else 0)
