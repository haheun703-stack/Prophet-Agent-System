# -*- coding: utf-8 -*-
"""test_exhaustion_stale_8_29.py — [F-171] 외인소진율 게이트 stale 표면화 회귀

배경(8/13 등재 · 8/29 소진):
  `bot/risk_gate_helper.is_foreign_exhaustion_blocked()` 가 소진율 CSV의 **마지막 행을
  날짜 확인 없이** 읽어 실매수 차단 판정에 넣고 있었다(`auto_trader:650`).
  ★실측(8/29·2,531종): 최신 2,422종(95.7%) / 한 달+ 묵은 것 **49종(1.9%)**.
  그 49종의 소진율은 전부 0~16%라 차단 임계(50%)에 안 걸려 **실제 오차단은 0종**.
  ★그래서 위험 방향은 반대다 — 묵은 데이터가 *낮은* 값을 주면 **차단해야 할 종목을 통과**시킨다.

처방(이 테스트가 지키는 것):
  1) stale이어도 **판정 결과는 불변**('보이게 하되 동작 불변' — [F-164] 규약)
     → 지금 차단으로 바꾸면 49종이 영구 매수불가가 되는데 그 부작용을 정당화할 실측이 없다.
  2) stale이면 **경고 로그가 남는다**(안전장치가 낡은 값으로 판정 중이라는 사실 표면화)
  3) 신선하면 경고가 없다(마모 방지)
  4) ★음성대조 — 날짜 확인이 없던 구코드는 stale을 **전혀 알리지 못한다**
  5) 예외/결측에서 죽지 않는다(매수 경로라 graceful 필수)

실행: python -X utf8 tests/test_exhaustion_stale_8_29.py
"""
import logging
import sys
from datetime import date, timedelta
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import pandas as pd  # noqa: E402

import bot.risk_gate_helper as rgh  # noqa: E402

PASS_N = 0
FAIL_N = 0


def check(label, cond, extra=""):
    global PASS_N, FAIL_N
    if cond:
        PASS_N += 1
        print(f"  ✅ {label}")
    else:
        FAIL_N += 1
        print(f"  ❌ {label} {extra}")


class _FakeClient:
    """소진율 50%+ 면 차단 — 실제 SDK와 동일 규약."""
    def __init__(self):
        self.seen = []

    def is_blacklisted_for_exhaustion(self, rate):
        self.seen.append(rate)
        return rate >= 50.0


def _df(days_ago: int, rate: float):
    d = date.today() - timedelta(days=days_ago)
    return pd.DataFrame({"소진율": [rate]}, index=pd.to_datetime([d]))


def _run(days_ago, rate, caplog_list):
    """(판정결과, 경고에 F-171이 있었나)"""
    fake = _FakeClient()
    rgh._client_singleton = fake
    rgh._SDK_AVAILABLE = True

    import data.flow_collector as fc
    orig = getattr(fc, "load_foreign_exhaustion", None)
    fc.load_foreign_exhaustion = lambda code: _df(days_ago, rate)
    caplog_list.clear()
    try:
        out = rgh.is_foreign_exhaustion_blocked("005930")
    finally:
        if orig is not None:
            fc.load_foreign_exhaustion = orig
    warned = any("F-171" in m for m in caplog_list)
    return out, warned, fake


class _Cap(logging.Handler):
    def __init__(self, sink):
        super().__init__()
        self.sink = sink

    def emit(self, record):
        self.sink.append(record.getMessage())


def main() -> int:
    sink = []
    h = _Cap(sink)
    rgh.logger.addHandler(h)
    rgh.logger.setLevel(logging.WARNING)

    print("=" * 78)
    print(f"[0] 임계 상수 = {rgh.EXHAUSTION_MAX_AGE_DAYS}일")
    check("EXHAUSTION_MAX_AGE_DAYS 가 정의돼 있다",
          isinstance(rgh.EXHAUSTION_MAX_AGE_DAYS, int) and rgh.EXHAUSTION_MAX_AGE_DAYS > 0)

    print("\n[1] 신선한 데이터 — 경고 없음 · 판정 정상")
    out, warned, fake = _run(1, 60.0, sink)
    check("소진율 60% → 차단", out is True)
    check("신선하면 경고 없음(마모 방지)", not warned, f"\n     logs={sink}")
    out, warned, _ = _run(1, 10.0, sink)
    check("소진율 10% → 통과", out is False)

    print("\n[2] ★stale — 판정은 불변, 경고만 (동작 불변 규약)")
    out, warned, fake = _run(90, 16.0, sink)
    check("stale이어도 판정은 그대로(16% → 통과)", out is False)
    check("stale 경고가 남는다", warned, f"\n     logs={sink}")
    check("경고에 경과일이 있다", any("90일 전" in m for m in sink), f"\n     logs={sink}")
    check("판정 함수는 여전히 호출됐다(우회 아님)", fake.seen == [16.0], f"\n     seen={fake.seen}")

    out, warned, _ = _run(90, 60.0, sink)
    check("stale + 60% → 여전히 차단(동작 불변)", out is True)
    check("이 경우도 경고", warned)

    print("\n[3] 경계값")
    out, warned, _ = _run(rgh.EXHAUSTION_MAX_AGE_DAYS, 10.0, sink)
    check(f"정확히 {rgh.EXHAUSTION_MAX_AGE_DAYS}일 = 경고 없음(> 비교)", not warned)
    out, warned, _ = _run(rgh.EXHAUSTION_MAX_AGE_DAYS + 1, 10.0, sink)
    check(f"{rgh.EXHAUSTION_MAX_AGE_DAYS + 1}일 = 경고 있음", warned)

    print("\n[4] ★음성대조 — 날짜 확인이 없던 구코드는 stale을 못 알린다")

    def _legacy(df, client):
        """8/29 fix 이전 로직 재현: 마지막 행을 날짜 확인 없이 사용."""
        if df is None or len(df) == 0:
            return False, False
        rate = float(df["소진율"].iloc[-1])
        return client.is_blacklisted_for_exhaustion(rate), False   # 경고 경로 자체가 없음

    legacy_out, legacy_warn = _legacy(_df(90, 16.0), _FakeClient())
    check("구코드도 판정은 같다(통과) — 즉 fix는 동작을 안 바꿨다", legacy_out is False)
    check("★구코드는 stale 경고를 못 낸다", not legacy_warn)

    print("\n[5] graceful — 매수 경로라 죽으면 안 된다")
    import data.flow_collector as fc
    orig = fc.load_foreign_exhaustion
    try:
        fc.load_foreign_exhaustion = lambda code: None
        check("df=None 이면 False(차단 안 함)", rgh.is_foreign_exhaustion_blocked("005930") is False)
        fc.load_foreign_exhaustion = lambda code: (_ for _ in ()).throw(RuntimeError("boom"))
        check("예외가 나도 False 반환(전파 안 함)",
              rgh.is_foreign_exhaustion_blocked("005930") is False)
    finally:
        fc.load_foreign_exhaustion = orig
        rgh._client_singleton = None

    rgh.logger.removeHandler(h)
    print("\n" + "=" * 78)
    print(f"결과: {PASS_N} PASS / {FAIL_N} FAIL")
    return 1 if FAIL_N else 0


if __name__ == "__main__":
    raise SystemExit(main())
