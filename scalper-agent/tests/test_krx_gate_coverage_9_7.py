# -*- coding: utf-8 -*-
"""test_krx_gate_coverage_9_7.py — [F-217] KRX 게이트 커버리지 (사장님 6/22 절대 룰)

배경(9/7 전체검수 축1):
  `data/krx_gate.py` docstring은 *"KRX 실호출(**pykrx 웹스크래핑**·KRX nationality 크롤러·
  KRX OpenAPI)은 krx_enabled()가 True일 때만 허용"* 이라고 **명시**한다.
  그런데 실측: pykrx를 import하는 파일 18개 중 게이트가 있는 것은 **2개뿐**이었다.

  ★가장 급했던 것 — `data/morning_recommendation.py`는 게이트 0건인데
    `bot/trading_coo.py` C13(평일 16:30)이 매일 부르고, `_step3_tech_filter`는
    `ThreadPoolExecutor`로 **후보 종목마다 병렬 fan-out** 한다.
    자동매매 OFF와 무관하게 **지금도 나가고 있었다**(9/5·9/6 실행 로그 확인).
    6/22 사장님 지적 *"무자비하게 너무 들어가서 못 들어간다"* 가 정확히 이 모양이다.
  ★두 번째 — `bot/auto_trader.py`의 MACD 조건은 `job_monitor`의 **30초 루프** 안이라
    재가동 시 감시 종목 수 × 분당 2회로 나간다. `except: pass`가 실패를 삼켜 로그에도 없었다.

  ★7/31 교훈의 재현: *"문서가 '자동 차단한다'고 말하면 구현을 세어 확인한다."*
    krx_gate docstring이 pykrx를 대상이라 적었는데 아무도 세지 않았다.

이 테스트가 지키는 것:
  1) 게이트 자체는 default 차단(fail-closed)이고 로드 실패도 차단
  2) `_get_market_change_today()` — 차단 시 KRX 미호출·0.0 반환
  3) `_step3_tech_filter()` — 차단 시 KRX 미호출·전 종목 '데이터부족(KRX 차단)'
  4) ★음성대조 — 게이트를 통과시키면(monkeypatch) pykrx 경로로 들어간다
  5) ★소스 대조 — 두 라이브 파일에 `krx_enabled` 호출이 실재한다
  6) 회귀 — 차단 시 반환 구조(키 집합)가 정상 경로와 동일

실행: python -X utf8 tests/test_krx_gate_coverage_9_7.py
"""
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

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


def test_gate_fail_closed():
    from data.krx_gate import krx_enabled
    _check("krx_enabled() default 차단", krx_enabled() is False)
    import os
    orig = os.environ.get("KRX_ENABLED")
    try:
        os.environ["KRX_ENABLED"] = "1"
        _check("KRX_ENABLED=1이면 True(설계대로)", krx_enabled() is True)
    finally:
        if orig is None:
            os.environ.pop("KRX_ENABLED", None)
        else:
            os.environ["KRX_ENABLED"] = orig
    _check("복원 후 다시 차단", krx_enabled() is False)


def test_market_change_blocked():
    """차단 시 KRX를 부르지 않고 0.0을 돌려준다."""
    from data import morning_recommendation as mr
    called = []
    import builtins
    real_import = builtins.__import__

    def spy(name, *a, **kw):
        if name == "pykrx" or name.startswith("pykrx."):
            called.append(name)
        return real_import(name, *a, **kw)

    builtins.__import__ = spy
    try:
        v = mr._get_market_change_today()
    finally:
        builtins.__import__ = real_import
    _check("차단 시 0.0 반환", v == 0.0)
    _check("★pykrx import 0회(실호출 없음)", not called)


def test_tech_filter_blocked():
    from data import morning_recommendation as mr
    called = []
    import builtins
    real_import = builtins.__import__

    def spy(name, *a, **kw):
        if name == "pykrx" or name.startswith("pykrx."):
            called.append(name)
        return real_import(name, *a, **kw)

    codes = [("005930", "삼성전자"), ("000660", "SK하이닉스"), ("069500", "KODEX200")]
    builtins.__import__ = spy
    try:
        r = mr._step3_tech_filter(codes, 0.0)
    finally:
        builtins.__import__ = real_import
    _check("★pykrx import 0회(실호출 없음)", not called)
    _check("전 종목 반환", set(r.keys()) == {c for c, _ in codes})
    _check("사유가 'KRX 차단'으로 명시", all("KRX 차단" in v["detail"] for v in r.values()))
    # 회귀 — 반환 구조가 정상 경로와 같아야 호출자가 안 깨진다
    for v in r.values():
        _check("반환 키 집합 동일",
               set(v.keys()) == {"score", "detail", "today_chg", "relative_str", "close"})
        break
    _check("score 0(조건 미충족)", all(v["score"] == 0 for v in r.values()))


def test_negative_control_gate_open():
    """★음성대조 — 게이트를 열면 pykrx 경로로 들어간다(= 게이트가 실제로 갈림길이다).

    실제 KRX 호출은 하지 않는다. `krx_enabled`를 True로 바꾼 뒤
    pykrx import 시도가 일어나는지만 본다(네트워크 전에 import가 먼저다).
    """
    from data import morning_recommendation as mr
    import data.krx_gate as gate
    orig = gate.krx_enabled
    called = []
    import builtins
    real_import = builtins.__import__

    def spy(name, *a, **kw):
        if name == "pykrx" or name.startswith("pykrx."):
            called.append(name)
            raise ImportError("테스트: 실호출 차단")   # 여기서 끊어 네트워크로 안 나간다
        return real_import(name, *a, **kw)

    gate.krx_enabled = lambda: True
    builtins.__import__ = spy
    try:
        mr._get_market_change_today()
    except Exception:
        pass
    finally:
        builtins.__import__ = real_import
        gate.krx_enabled = orig
    _check("음성대조: 게이트 열면 pykrx 진입 시도", len(called) >= 1)
    _check("게이트 복원됨", gate.krx_enabled() is False)


def test_source_has_gate():
    """소스 대조 — 라이브 2파일에 게이트 호출이 실재한다."""
    mr = (BASE_DIR / "data" / "morning_recommendation.py").read_text("utf-8")
    at = (BASE_DIR / "bot" / "auto_trader.py").read_text("utf-8")
    _check("morning_recommendation에 krx_enabled 2곳 이상", mr.count("krx_enabled") >= 2)
    _check("auto_trader MACD 경로에 krx_enabled", "from data.krx_gate import krx_enabled" in at)
    # 게이트가 pykrx import보다 앞에 있어야 의미가 있다
    i_gate = at.index("from data.krx_gate import krx_enabled")
    i_pykrx = at.index("from pykrx import stock as pykrx_stock")
    _check("★auto_trader: 게이트가 pykrx import보다 앞", i_gate < i_pykrx)


if __name__ == "__main__":
    for fn in (test_gate_fail_closed, test_market_change_blocked, test_tech_filter_blocked,
               test_negative_control_gate_open, test_source_has_gate):
        print(f"[{fn.__name__}]")
        fn()
    print(f"\n결과: {PASS_N} PASS / {FAIL_N} FAIL")
    sys.exit(1 if FAIL_N else 0)
