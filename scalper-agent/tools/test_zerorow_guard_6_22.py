# -*- coding: utf-8 -*-
"""zero-row 가드 + marker 정직화 검증 (6/22 신설).

(A) supply_analyzer._trim_trailing_placeholder_rows() — 개장전 placeholder(수급0) 방어
(B) flow_collector._compute_real_data_status() — 마커 data_through/investor_real 정직화

★ 데이터-무관 불변식 기반: 실데이터가 라이브로 변해도(EOD 충전 등) 깨지지 않게
  특정 날짜값 단정 대신 trim 전후 불변식 + 합성 데이터로 검증.

실행: python tools/test_zerorow_guard_6_22.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
from data.supply_analyzer import _trim_trailing_placeholder_rows, SupplyAnalyzer
from data.flow_collector import _compute_real_data_status

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FLOW = os.path.join(ROOT, "data_store", "flow")

_passed = 0


def ok(cond, label):
    global _passed
    assert cond, "FAIL: " + label
    _passed += 1
    print("  PASS:", label)


def _supply_cols(df):
    return [c for c in df.columns if ("금액" in str(c) or "수량" in str(c))]


def _last_day(df):
    t = _trim_trailing_placeholder_rows(df)
    if t is None or len(t) == 0:
        return None
    return str(t.index[-1].date())


print("=== [1] trim 불변식 (실파일 005930, 데이터-무관) ===")
raw = pd.read_csv(os.path.join(FLOW, "005930_investor.csv"), index_col=0, parse_dates=True)
cols = _supply_cols(raw)
trimmed = _trim_trailing_placeholder_rows(raw)
last_is_zero = bool((raw[cols].iloc[-1] == 0).all())
print("  raw last:", str(raw.index[-1].date()), "| last_is_zero:", last_is_zero,
      "| removed:", len(raw) - len(trimmed))
if last_is_zero:
    ok(len(trimmed) < len(raw), "마지막행 placeholder → 제거됨")
else:
    ok(len(trimmed) == len(raw), "마지막행 실데이터 → 보존(제거 0)")
ok(len(trimmed) > 0, "trim 후 비지 않음")
ok(float(trimmed[cols].iloc[-1].abs().sum()) > 0, "trim 후 마지막행=실데이터(수급≠0)")

print("=== [2] 합성 placeholder 명시 제거 (데이터-무관) ===")
base = trimmed.copy()
z1 = pd.DataFrame({c: [0] for c in base.columns}, index=pd.to_datetime(["2099-12-30"]))
z2 = pd.DataFrame({c: [0] for c in base.columns}, index=pd.to_datetime(["2099-12-31"]))
ok(len(_trim_trailing_placeholder_rows(pd.concat([base, z1]))) == len(base),
   "실데이터 + placeholder 1행 → 1행 제거")
ok(len(_trim_trailing_placeholder_rows(pd.concat([base, z1, z2]))) == len(base),
   "실데이터 + placeholder 2행 → 2행 제거")

print("=== [3] trim 엣지케이스 (순수 합성) ===")
ok(_trim_trailing_placeholder_rows(None) is None, "None 입력 → None")
ok(len(_trim_trailing_placeholder_rows(pd.DataFrame())) == 0, "빈 df → 빈 df")
fake = pd.DataFrame({"종가": [100, 200], "전일대비": [0, 0]})
ok(len(_trim_trailing_placeholder_rows(fake)) == 2, "수급컬럼 식별불가 → 원본보존(fail-safe)")
normal = pd.DataFrame({"기관_금액": [100, -50], "외국인_금액": [10, 20]})
ok(len(_trim_trailing_placeholder_rows(normal)) == 2, "정상행만 → 제거 0")
midz = pd.DataFrame({"기관_금액": [100, 0, 50], "외국인_금액": [10, 0, 20]})
ok(len(_trim_trailing_placeholder_rows(midz)) == 3, "중간 zero는 보존(trailing만)")
allz = pd.DataFrame({"기관_금액": [0, 0], "외국인_금액": [0, 0]})
ok(len(_trim_trailing_placeholder_rows(allz)) == 0, "전부 zero → 전부 제거(무한루프 없음)")
partial = pd.DataFrame({"기관_금액": [100, 0], "외국인_금액": [10, 5]})
ok(len(_trim_trailing_placeholder_rows(partial)) == 2, "일부 컬럼만 0 → 보존")

print("=== [4] analyze() — 캐시 마지막=실데이터 (데이터-무관) ===")
a = SupplyAnalyzer()
sc = a.analyze("005930")
ok(sc is not None, "analyze 결과 not None")
inv = a._cache_investor.get("005930")
icols = _supply_cols(inv)
ok(float(inv[icols].iloc[-1].abs().sum()) > 0, "analyze 캐시 마지막행=실데이터(placeholder 아님)")
sc2 = a.analyze("005930", as_of="2026-06-17")
ok(sc2 is not None and sc2.date == "2026-06-17", "as_of=6/17(과거 고정) → 기준일 6/17")

print("=== [5] (B) _compute_real_data_status (데이터-무관) ===")
df_a = pd.read_csv(os.path.join(FLOW, "005930_investor.csv"), index_col=0, parse_dates=True)
df_b = pd.read_csv(os.path.join(FLOW, "000020_investor.csv"), index_col=0, parse_dates=True)
exp = max(_last_day(df_a), _last_day(df_b))   # 동적 기대 최신일
rc, dt = _compute_real_data_status({"005930": df_a, "000020": df_b})
print("  real_count:", rc, "| data_through:", dt, "| expected:", exp)
ok(dt == exp, "data_through = 두 종목 실최신일의 max (동적 일치)")
ok(rc == 2, "real_count=2 (둘 다 실데이터)")
allzero = pd.DataFrame({"기관_금액": [0, 0], "외국인_금액": [0, 0]},
                       index=pd.to_datetime(["2026-06-19", "2026-06-22"]))
ok(_compute_real_data_status({"X": allzero}) == (0, None), "전부 placeholder 종목 → (0, None)")
ok(_compute_real_data_status({}) == (0, None), "빈 dict → (0, None)")
ok(_compute_real_data_status(None) == (0, None), "None → (0, None)")
rc3, dt3 = _compute_real_data_status({"005930": df_a, "X": allzero})
ok(rc3 == 1 and dt3 == _last_day(df_a), "혼합 → real_count 1 · through=005930 실최신일")

print()
print("ALL PASS  (%d checks)" % _passed)
