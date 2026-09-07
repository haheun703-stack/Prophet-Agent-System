# -*- coding: utf-8 -*-
"""test_soback_tail_9_7.py — [F-186] 소급(--date) 날짜 판정의 꼬리행 자동 확장 회귀

배경(9/7):
  8/31~9/4 5거래일 소급 점검에서 `notify_data_freshness --dry-run --date 2026-08-31`이
  **투자자수급 0/2485**를 냈다. 파일에는 8/31 행이 있었다. 8/31 뒤에 9/1~9/4 4행과
  06:30 A0가 붙인 9/7 placeholder 1행 = 5행이 있어 `_csv_has_date`의 꼬리 5행 밖으로
  밀린 것이다. 8/11에 순위 스냅샷에서 같은 한계를 봤고 전수 재계수로 넘기고 도구는 그대로 뒀는데 재발했다.
  → 호출처가 아니라 `_csv_has_date` 자체가 기준일이 과거면 (오늘−기준일) 달력일만큼
    꼬리를 넓힌다. 기준일==오늘(운영 20:10·08:30 경로)이면 종전과 완전히 동일.

이 테스트가 지키는 것:
  1) 소급 기준일이 꼬리 5행 밖이어도 행이 있으면 True
  2) ★음성대조 — 정본 `csv_has_date`를 꼬리 5행으로 직접 부르면 같은 파일에서 False
     (= 구 동작이 실제로 놓쳤음을 증명)
  3) 기준일==오늘이면 확장 0 — 꼬리 5행 밖의 과거 행은 종전대로 False(동작 불변)
  4) compact(YYYYMMDD) 기준일도 같은 결과(정규화는 정본 위임)
  5) 없는 날짜는 확장해도 False(가짜 양성 없음)

실행: python -X utf8 tests/test_soback_tail_9_7.py
"""
import sys
import tempfile
from datetime import date
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from data import data_verifier as dv          # noqa: E402
from utils.dated_csv import csv_has_date      # noqa: E402

PASS_N = 0
FAIL_N = 0


def _check(name, cond):
    global PASS_N, FAIL_N
    if cond:
        PASS_N += 1
        print(f"  ✅ {name}")
    else:
        FAIL_N += 1
        print(f"  ❌ {name}")


def _fixture(tmp: Path) -> Path:
    # 8/24~9/4 거래일 10행 + 9/7 placeholder(0값) — 9/7 VPS flow/005930_investor.csv 실모양
    days = ["2026-08-24", "2026-08-25", "2026-08-26", "2026-08-27", "2026-08-28",
            "2026-08-31", "2026-09-01", "2026-09-02", "2026-09-03", "2026-09-04", "2026-09-07"]
    p = tmp / "005930_investor.csv"
    p.write_text("date,close,chg,foreign,inst\n" + "\n".join(f"{d},1000,0,1,-1" for d in days) + "\n",
                 encoding="utf-8")
    return p


def test_soback_beyond_tail5_found():
    with tempfile.TemporaryDirectory() as td:
        p = _fixture(Path(td))
        today = date(2026, 9, 7)
        _check("8/31 소급(오늘 9/7) → True", dv._csv_has_date(p, "2026-08-31", _today_override=today))
        _check("8/24 소급(꼬리 11행째) → True", dv._csv_has_date(p, "2026-08-24", _today_override=today))
        _check("compact 20260831 → True", dv._csv_has_date(p, "20260831", _today_override=today))


def test_negative_control_old_behavior_misses():
    with tempfile.TemporaryDirectory() as td:
        p = _fixture(Path(td))
        # 정본을 꼬리 5행으로 직접 부르면 8/31은 6행째라 안 보인다 = 9/7 실측 재현
        _check("음성대조: 정본 tail=5로 8/31 → False", csv_has_date(p, "2026-08-31", 5) is False)
        _check("음성대조: 정본 tail=5로 9/4 → True", csv_has_date(p, "2026-09-04", 5) is True)


def test_today_unchanged():
    with tempfile.TemporaryDirectory() as td:
        p = _fixture(Path(td))
        today = date(2026, 9, 7)
        _check("오늘 기준 9/7 → True", dv._csv_has_date(p, "2026-09-07", _today_override=today))
        # 기준일이 미래(gap<0)면 확장 없음
        _check("미래 기준일(9/8) → False",
               dv._csv_has_date(p, "2026-09-08", _today_override=today) is False)
        # ★불변식 — 기준일==오늘이면 gap 0 → 확장 없음 → 꼬리 5행 밖의 행은 종전처럼 False.
        # 오늘=8/24로 두고 8/24(11행째)를 물으면 안 보여야 한다(운영 20:10·08:30 경로 동작 불변 증명).
        # (첫 판은 오늘=8/31·기준 8/24로 썼다가 실패 — 그건 gap 7 소급이라 확장이 맞다. 테스트의 가정 오류.)
        _check("오늘=8/24·기준 8/24(꼬리 밖) → False(확장 0·종전 동일)",
               dv._csv_has_date(p, "2026-08-24", _today_override=date(2026, 8, 24)) is False)
        # 같은 파일·같은 기준일을 오늘=9/7로 물으면 소급이라 True — 두 결과의 차이가 곧 [F-186] fix
        _check("오늘=9/7·기준 8/24 → True(소급 확장)",
               dv._csv_has_date(p, "2026-08-24", _today_override=date(2026, 9, 7)) is True)


def test_missing_date_stays_false():
    with tempfile.TemporaryDirectory() as td:
        p = _fixture(Path(td))
        _check("없는 날짜 8/29(토) 소급 → False",
               dv._csv_has_date(p, "2026-08-29", _today_override=date(2026, 9, 7)) is False)
        _check("없는 파일 → False",
               dv._csv_has_date(Path(td) / "nope.csv", "2026-08-31", _today_override=date(2026, 9, 7)) is False)


if __name__ == "__main__":
    for fn in (test_soback_beyond_tail5_found, test_negative_control_old_behavior_misses,
               test_today_unchanged, test_missing_date_stays_false):
        print(f"[{fn.__name__}]")
        fn()
    print(f"\n결과: {PASS_N} PASS / {FAIL_N} FAIL")
    sys.exit(1 if FAIL_N else 0)
