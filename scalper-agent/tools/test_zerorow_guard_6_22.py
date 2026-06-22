# -*- coding: utf-8 -*-
"""zero-row 가드 실측/단위 검증 (6/22 신설).

supply_analyzer._trim_trailing_placeholder_rows() 회귀 테스트.
장 시작 전 생성되는 placeholder 행(수급 전부 0)이 5일 누적/연속일/마지막행
점수에 새지 않는지 실데이터 + 엣지케이스로 검증.

실행: python tools/test_zerorow_guard_6_22.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
from data.supply_analyzer import _trim_trailing_placeholder_rows, SupplyAnalyzer

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FLOW = os.path.join(ROOT, "data_store", "flow")

_passed = 0


def ok(cond, label):
    global _passed
    assert cond, "FAIL: " + label
    _passed += 1
    print("  PASS:", label)


def _last_dates(df, n=2):
    return [str(d.date()) for d in df.index[-n:]]


def _supply_cols(df):
    return [c for c in df.columns if ("금액" in str(c) or "수량" in str(c))]


print("=== [1] 실데이터 — zero-row 제거 ===")
# 삼성전자: 6/22 placeholder(전부 0) 존재 → 1행 제거, 마지막=6/19 실데이터
raw = pd.read_csv(os.path.join(FLOW, "005930_investor.csv"), index_col=0, parse_dates=True)
trimmed = _trim_trailing_placeholder_rows(raw)
print("  raw  last:", _last_dates(raw), "| trim last:", _last_dates(trimmed),
      "| removed:", len(raw) - len(trimmed))
ok(len(raw) - len(trimmed) == 1, "005930 placeholder 1행 제거")
cols = _supply_cols(trimmed)
ok(float(trimmed[cols].iloc[-1].abs().sum()) > 0, "005930 trim후 마지막행 수급 0 아님(실데이터)")
ok(str(trimmed.index[-1].date()) == "2026-06-19", "005930 trim후 마지막=2026-06-19")

print("=== [2] 엣지케이스 ===")
ok(_trim_trailing_placeholder_rows(None) is None, "None 입력 → None")
ok(len(_trim_trailing_placeholder_rows(pd.DataFrame())) == 0, "빈 df → 빈 df")
fake = pd.DataFrame({"종가": [100, 200], "전일대비": [0, 0]})
ok(len(_trim_trailing_placeholder_rows(fake)) == 2, "수급컬럼 식별불가 → 원본보존(fail-safe)")
normal = pd.DataFrame({"기관_금액": [100, -50], "외국인_금액": [10, 20]})
ok(len(_trim_trailing_placeholder_rows(normal)) == 2, "정상행만 → 제거 0")
one = pd.DataFrame({"기관_금액": [100, -50, 0], "외국인_금액": [10, 20, 0]})
ok(len(_trim_trailing_placeholder_rows(one)) == 2, "마지막만 zero → 1행 제거")
two = pd.DataFrame({"기관_금액": [100, 0, 0], "외국인_금액": [10, 0, 0]})
ok(len(_trim_trailing_placeholder_rows(two)) == 1, "trailing zero 2행 → 2행 제거")
mid = pd.DataFrame({"기관_금액": [100, 0, 50], "외국인_금액": [10, 0, 20]})
ok(len(_trim_trailing_placeholder_rows(mid)) == 3, "중간 zero는 보존(trailing만)")
# 부분 0(일부 컬럼만 0)은 placeholder 아님 → 보존
partial = pd.DataFrame({"기관_금액": [100, 0], "외국인_금액": [10, 5]})
ok(len(_trim_trailing_placeholder_rows(partial)) == 2, "일부 컬럼만 0 → 보존")

print("=== [3] analyze() 경로 회귀 — 기준일이 placeholder 아니어야 ===")
a = SupplyAnalyzer()
sc = a.analyze("005930")
print("  analyze('005930').date =", sc.date if sc else None)
ok(sc is not None, "analyze 결과 not None")
ok(sc.date == "2026-06-19", "analyze 기준일=2026-06-19 (6/22 placeholder 아님)")

# as_of 백테스트 경로 — 과거일 지정 시 정상(과거엔 placeholder 없음)
sc2 = a.analyze("005930", as_of="2026-06-17")
ok(sc2 is not None and sc2.date == "2026-06-17", "as_of=6/17 → 기준일 6/17 정상")

print()
print("ALL PASS  (%d checks)" % _passed)
