# -*- coding: utf-8 -*-
"""3축 결합 관측(7/9 사장님 승인) 테스트 — playbook_shadow stack_7_9.

검증 범위:
  A. _is_refined      : 정제판 경계(강도 200 이상·신호 10:00:00 이하) 정확 판정
  B. _meongbun_map    : catalyst_archive 파싱(등급/무명분 기본값/손상 파일 무시)
  C. _label_stack     : D-1 라벨 부착 — look-ahead 0(첫날 None)·비수록/None 구분·
                        비정제 무수정(additive 불변식)
  D. _stack_cohorts   : 코호트 분해 정합(부분집합·any/strict)·빈도·coverage
  E. 상수 드리프트     : 정제판 원값(7/4 실측 200/10:00)·등급 문자열 고정

★ record-only 도구의 순수 함수만 검증 — 매수/매도/SAJANG/주문 0접촉.
실행: python tests/test_playbook_stack_7_9.py
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

PASS = 0
FAILS = []


def check(name, cond):
    global PASS
    if cond:
        PASS += 1
    else:
        FAILS.append(name)
        print(f"  ❌ {name}")


def _t(date="20260708", code="111", strength=250.0, sig_t="09:30:00",
       ret=1.0, regime=True, **extra):
    d = {"date": date, "code": code, "signal_time": sig_t,
         "strength_at_signal": strength, "ret": ret, "why": "TP",
         "strength_gate": strength >= 100, "regime_gate": regime}
    d.update(extra)
    return d


# ── A. _is_refined 경계 ──
check("A1 강도 200 정확히=정제판", pb._is_refined(_t(strength=200.0)))
check("A2 강도 199.9=제외", not pb._is_refined(_t(strength=199.9)))
check("A3 신호 10:00:00 정확히=정제판", pb._is_refined(_t(sig_t="10:00:00")))
check("A4 신호 10:00:01=제외", not pb._is_refined(_t(sig_t="10:00:01")))
check("A5 필드 결측=제외(보수)", not pb._is_refined({}))

# ── B. _meongbun_map ──
with tempfile.TemporaryDirectory() as td:
    tmp = Path(td)
    (tmp / "catalyst_2026-07-07.json").write_text(json.dumps({
        "items": [{"code": "111", "meongbun_grade": "강"},
                  {"code": "222", "meongbun_grade": None},
                  {"name": "코드없음"}],
    }), encoding="utf-8")
    (tmp / "catalyst_2026-07-08.json").write_text("{손상", encoding="utf-8")
    orig = pb.ARCHIVE_DIR
    pb.ARCHIVE_DIR = tmp
    try:
        m = pb._meongbun_map()
    finally:
        pb.ARCHIVE_DIR = orig
check("B1 등급 파싱", m.get("2026-07-07", {}).get("111") == "강")
check("B2 등급 None→무명분 기본값", m.get("2026-07-07", {}).get("222") == "무명분")
check("B3 손상 파일 무시", "2026-07-08" not in m)
check("B4 디렉터리 없음→빈 dict", (
    lambda: (setattr(pb, "ARCHIVE_DIR", Path(td) / "없음"),
             pb._meongbun_map() == {},
             setattr(pb, "ARCHIVE_DIR", orig))[1])())

# ── C. _label_stack — D-1 라벨·look-ahead 0·additive ──
prev = {"20260708": "2026-07-07"}          # 20260707은 첫 관측일(D-1 미상)
arch = {"2026-07-07": {"111": "강"}}


def kki_stub(code, d1):
    assert d1 == "2026-07-07", "D-1이 아닌 날짜로 끼 조회 = look-ahead 위반"
    return "HUNTABLE" if code == "111" else None


tr_ok = _t(code="111")                                  # 라벨 전부 성립
tr_unlisted = _t(code="999")                            # 아카이브 있는 날·미수록
tr_firstday = _t(date="20260707", code="111")           # D-1 미상(첫날)
tr_not_refined = _t(code="111", strength=150.0)         # 비정제 — 무수정이어야 함
trades = [tr_ok, tr_unlisted, tr_firstday, tr_not_refined]
pb._label_stack(trades, prev, arch, kki_fn=kki_stub)

check("C1 끼 D-1 라벨", tr_ok["kki_d1"] == "HUNTABLE")
check("C2 명분 D-1 라벨", tr_ok["meongbun_d1"] == "강")
check("C3 아카이브 미수록=비수록", tr_unlisted["meongbun_d1"] == "비수록")
check("C4 끼 계산실패=None", tr_unlisted["kki_d1"] is None)
check("C5 첫날(D-1 미상)=None·None",
      tr_firstday["kki_d1"] is None and tr_firstday["meongbun_d1"] is None)
check("C6 비정제 무수정(additive)",
      "kki_d1" not in tr_not_refined and "meongbun_d1" not in tr_not_refined)

# 아카이브 없는 D-1 날짜 → meongbun None (비수록과 구분)
tr_noarch = _t(code="111", date="20260710")
pb._label_stack([tr_noarch], {"20260710": "2026-07-09"}, arch,
                kki_fn=lambda c, d: "MODERATE")
check("C7 아카이브 없는 날=None(비수록과 구분)", tr_noarch["meongbun_d1"] is None)
check("C8 끼는 독립 부착", tr_noarch["kki_d1"] == "MODERATE")

# ── D. _stack_cohorts — 분해 정합 ──
def _lab(regime, kki, mb, ret=1.0, date="20260708"):
    return _t(date=date, regime=regime, ret=ret, kki_d1=kki, meongbun_d1=mb)


cohort_trades = [
    _lab(True, "HUNTABLE", "강"),            # 전 코호트 통과 (strict 유일)
    _lab(True, "HUNTABLE", "무명분"),         # any 통과·strict 제외
    _lab(True, "SLUGGISH", "중", ret=-3.0),   # any 통과(명분)·끼 제외
    _lab(True, "SLUGGISH", None, ret=-3.0),   # 레짐만
    _lab(False, "EXPLOSIVE", "강", date="20260709"),  # 레짐 제외 → any/strict 제외
    _t(strength=150.0),                       # 비정제 — 전 코호트 제외
]
st = pb._stack_cohorts(cohort_trades)
check("D1 refined=5", st["refined"]["trades"] == 5)
check("D2 ×레짐=4", st["refined_x_regime"]["trades"] == 4)
check("D3 ×끼=3", st["refined_x_kki"]["trades"] == 3)
check("D4 ×명분=3", st["refined_x_meongbun"]["trades"] == 3)
check("D5 stack3_any=3(레짐∧[끼or명분])", st["stack3_any"]["trades"] == 3)
check("D6 stack3_strict=1(전부∧)", st["stack3_strict"]["trades"] == 1)
check("D7 빈도 — refined 2일·2.5건/일",
      st["refined"]["days_with_trades"] == 2
      and st["refined"]["trades_per_day"] == 2.5)
check("D8 coverage 정직(끼5·명분4)",
      st["coverage"] == {"refined_total": 5, "kki_labeled": 5, "meongbun_labeled": 4})
check("D9 빈 코호트 안전", pb._stack_cohorts([])["stack3_strict"]["trades"] == 0)

# ── E. 상수 드리프트 가드 ──
check("E1 정제판 원값 고정(7/4 실측 200/10:00)",
      pb.REFINED_STRENGTH == 200.0 and pb.REFINED_CUTOFF == "10:00:00")
check("E2 끼 통과=상위 2등급", pb.KKI_OK == ("HUNTABLE", "EXPLOSIVE"))
check("E3 명분 통과=강/중(스캐너 동일)", pb.MEONGBUN_OK == ("강", "중"))
check("E4 레짐 임계 0.45 유지", pb.REGIME_BREADTH == 0.45)

print(f"\n{'✅' if not FAILS else '❌'} PASS {PASS} / FAIL {len(FAILS)}"
      + (f" — {FAILS}" if FAILS else ""))
sys.exit(1 if FAILS else 0)
