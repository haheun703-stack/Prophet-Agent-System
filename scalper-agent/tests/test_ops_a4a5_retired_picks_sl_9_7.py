# -*- coding: utf-8 -*-
"""test_ops_a4a5_retired_picks_sl_9_7.py — [F-187] 점검기 A4/A5 미등록 + [F-185] picks SL SAJANG 파생

배경(9/7):
  [F-187] 8/29 사장님 승인으로 [S-1] 러너 cron 중단·nightly ⑲-2/⑲-3 제거. 점검기
    `daily_ops_check`는 그 둘을 계속 찾아 8/31~9/7 6영업일 매일 🚨 A4·A5를 발송했다.
    매일 같은 오탐은 진짜 경보를 마모시킨다([F-153]). → run_checks에서 미등록(함수 보존).
  [F-185] `daytrading_picks.py`의 SL이 종가×96.5%(−3.5%) 리터럴로 SAJANG(−3%)을 우회.
    실주문 경로는 아니지만(auto_trader는 이 JSON에서 ewy_signal만 읽음) 사장님이 보는
    값·PaperPortfolio SL이 룰과 달랐다. → `price_levels()` 헬퍼로 SAJANG.get_normal_sl 파생.

이 테스트가 지키는 것:
  1) run_checks 결과에 A4·A5가 없다 / A1·A2·A3·A6·A9는 있다
  2) build_message 본문에 "A4"·"A5"·"OBSERVE v2 러너"·"⑲-2" 문구가 없다
  3) ★음성대조 — 보존된 check_a4/check_a5 함수는 여전히 존재한다(재개 시 복원 가능)
  4) price_levels(close)["sl"] == SAJANG.get_normal_sl(close) (−3%) 이고 −3.5%가 아니다
  5) ★소스 대조 — daytrading_picks.py에 구 리터럴이 없다
  6) 다른 레벨(entry_low/high·tp1/tp2)은 종전 값 그대로(표시 회귀 없음)

실행: python -X utf8 tests/test_ops_a4a5_retired_picks_sl_9_7.py
"""
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
for p in (str(BASE_DIR), str(BASE_DIR / "tools")):
    if p not in sys.path:
        sys.path.insert(0, p)

import daily_ops_check as ops                     # noqa: E402
from tools import daytrading_picks as dp          # noqa: E402
from data.sajang_rules import SAJANG              # noqa: E402

PASS_N = 0
FAIL_N = 0
_OLD_SL_LITERAL = "0." + "965"   # 소스 대조용(이 파일 자체가 검색에 걸리지 않게 분리)


def _check(name, cond):
    global PASS_N, FAIL_N
    if cond:
        PASS_N += 1
        print(f"  ✅ {name}")
    else:
        FAIL_N += 1
        print(f"  ❌ {name}")


def test_a4_a5_not_registered():
    # 로그 부재 환경(노트북)에서도 항목 코드 목록은 결정적이다
    rows, score, dl = ops.run_checks("2026-09-04")
    codes = [r[0] for r in rows]
    _check("A4 미등록", "A4" not in codes)
    _check("A5 미등록", "A5" not in codes)
    for c in ("A1", "A2", "A3", "A6", "A9"):
        _check(f"{c} 유지", c in codes)
    msg = ops.build_message("2026-09-04", rows, score, dl)
    _check("본문에 'OBSERVE v2 러너' 없음", "OBSERVE v2 러너" not in msg)
    _check("본문에 '⑲-2' 없음", "⑲-2" not in msg)
    _check("본문에 ' A4 ' 없음", " A4 " not in msg)
    _check("본문에 ' A5 ' 없음", " A5 " not in msg)


def test_functions_preserved():
    _check("check_a4_observe 보존", callable(getattr(ops, "check_a4_observe", None)))
    _check("check_a5_contrast 보존", callable(getattr(ops, "check_a5_contrast", None)))


def test_picks_sl_sajang():
    for close in (10000, 25550, 1647000, 3335):
        lv = dp.price_levels(close)
        _check(f"sl({close}) == SAJANG.get_normal_sl", lv["sl"] == SAJANG.get_normal_sl(close))
        _check(f"sl({close}) != 구 −3.5%", lv["sl"] != int(close * float(_OLD_SL_LITERAL)))
        _check(f"entry/tp 종전 동일({close})",
               (lv["entry_low"], lv["entry_high"], lv["tp1"], lv["tp2"])
               == (int(close * 0.985), int(close * 1.010), int(close * 1.050), int(close * 1.080)))
    _check("sl(10000) == 9700 (−3%)", dp.price_levels(10000)["sl"] == 9700)
    _check("close=0 → 전부 0", all(v == 0 for v in dp.price_levels(0).values()))


def test_no_literal_in_source():
    src = Path(dp.__file__).read_text("utf-8")
    _check("소스에 구 −3.5% 리터럴 없음", _OLD_SL_LITERAL not in src)
    _check("소스가 SAJANG.get_normal_sl 사용", "SAJANG.get_normal_sl" in src)


if __name__ == "__main__":
    for fn in (test_a4_a5_not_registered, test_functions_preserved,
               test_picks_sl_sajang, test_no_literal_in_source):
        print(f"[{fn.__name__}]")
        fn()
    print(f"\n결과: {PASS_N} PASS / {FAIL_N} FAIL")
    sys.exit(1 if FAIL_N else 0)
