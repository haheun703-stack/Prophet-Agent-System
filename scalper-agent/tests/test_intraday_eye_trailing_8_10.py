# -*- coding: utf-8 -*-
"""[F-90] 잔여 2분기 복구 회귀 — 장중 눈(intraday_eye) 트레일링이 사장님 룰 3을 지키는가.

★ 사장님 룰 3 (5/25) = **고점 -3% 일관**. `SAJANG.TRAILING_PCT = 3.0`.

배경 — 왜 '일관'이 중요한가:
  `synthesize()`는 판정(verdict)별로 trailing_sl 을 따로 만들었다. 4분기 중
    ALIVE      : 8/5 에 고점 -1.5% 하드코딩이 적발돼 SAJANG 으로 전환됨
    WEAKENING  : 원래 SAJANG
    BREAKING   : `nearest_res`(돌파한 저항선) 그대로  ← 8/10 이 파일이 잡는 잔여
    BOUNCING   : `nearest_sup * 0.995`(지지선 -0.5%)  ← 8/10 잔여
  소비 경로가 위험을 키운다: `auto_trader:4136` ratchet 이 `new_sl > 기존`일 때만 갱신하므로
  **조이는 값만 채택**되고, 그것이 `:4496 effective_sl` → 실매도까지 간다.
  즉 분기별 변형은 "가끔 다른 값"이 아니라 **항상 가장 타이트한 값이 이긴다**.

★ 이 테스트가 고정하는 계약:
  ① 4분기 전부 `SAJANG.get_trailing_sl(today_high)` 와 정확히 일치
  ② 어떤 분기도 룰보다 **조이지 않는다**(고점 -3% 보다 높은 SL 금지)
  ③ 소스에 트레일링 하드코딩 배수(0.985/0.995 등)가 되살아나지 않는다
     — [F-151] 교훈: 기대값만 고치면 '폐기 확인'이 사라지므로 부재를 따로 감시한다.

실행: python -X utf8 tests/test_intraday_eye_trailing_8_10.py
"""
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from data.intraday_eye import IntradayBuffer, synthesize   # noqa: E402
from data.sajang_rules import SAJANG                        # noqa: E402

TODAY_HIGH = 10_000
PRICE = 9_900          # 고점 대비 -1% (아직 룰 손절선 위)
EXPECTED = SAJANG.get_trailing_sl(TODAY_HIGH)   # = 9,700 (고점 -3%)


def _buf(price: int = PRICE) -> IntradayBuffer:
    """워밍업(5봉) 통과용 최소 버퍼."""
    b = IntradayBuffer("005930", "테스트")
    for i in range(6):
        b.add_bar({
            "current_price": price, "open": price, "high": TODAY_HIGH, "low": price - 100,
            "volume": 1000 * (i + 1), "strength": 150.0, "change_rate": 5.0,
        })
    return b


def _sr(**kw) -> dict:
    d = {"score": 50, "today_high": TODAY_HIGH, "supports": [], "resistances": []}
    d.update(kw)
    return d


# 각 분기를 정확히 켜는 최소 입력 (분기 조건은 synthesize 본문 순서를 따른다)
def _case_breaking():
    return dict(
        ma={"score": 50, "alignment": "MIXED"},
        vwap_pos={"score": 50, "position": "ABOVE"},
        sr=_sr(resistances=[{"price": 9_850}]),          # price(9,900) > 저항선 → 돌파
        flow={"score": 50, "volume_acceleration": 2.0, "flow_state": "INFLOW", "strength": 150},
        momentum={"score": 50, "momentum_state": "STEADY"},
        prev_verdict="ALIVE",
    )


def _case_bouncing():
    return dict(
        ma={"score": 40, "alignment": "MIXED"},
        vwap_pos={"score": 40, "position": "BELOW"},
        sr=_sr(supports=[{"price": 9_800, "touches": 3}]),
        flow={"score": 40, "volume_trend": "INCREASING", "flow_state": "DRYING",
              "strength": 120, "volume_acceleration": 1.0},
        momentum={"score": 40, "momentum_state": "DECELERATING"},
        prev_verdict="DYING",                            # BOUNCING 은 직전이 DYING/WEAKENING/BOUNCING
    )


def _case_alive():
    return dict(
        ma={"score": 80, "alignment": "BULLISH"},
        vwap_pos={"score": 80, "position": "STRONG_ABOVE"},
        sr=_sr(score=70),
        flow={"score": 80, "flow_state": "INFLOW", "strength": 200, "volume_acceleration": 1.0},
        momentum={"score": 80, "momentum_state": "ACCELERATING"},
        prev_verdict="ALIVE",
    )


def _case_weakening():
    return dict(
        ma={"score": 55, "alignment": "CONVERGING"},
        vwap_pos={"score": 55, "position": "ABOVE"},
        sr=_sr(score=55),
        flow={"score": 55, "flow_state": "DRYING", "strength": 85, "volume_acceleration": 1.0},
        momentum={"score": 55, "momentum_state": "DECELERATING"},
        prev_verdict="ALIVE",
    )


CASES = {
    "BREAKING": _case_breaking,
    "BOUNCING": _case_bouncing,
    "ALIVE": _case_alive,
    "WEAKENING": _case_weakening,
}


def _run(case_fn):
    kw = case_fn()
    return synthesize(_buf(), kw["ma"], kw["vwap_pos"], kw["sr"],
                      kw["flow"], kw["momentum"], kw["prev_verdict"])


def test_all_four_branches_reachable():
    """먼저 4분기가 실제로 켜지는지 확인 — 안 켜지면 아래 검증이 공회전한다."""
    got = {}
    for name, fn in CASES.items():
        verdict, *_ = _run(fn)
        got[name] = verdict
    for name in CASES:
        assert got[name] == name, f"{name} 분기 미도달 (실제 {got[name]}) — 픽스처가 낡았다"
    print(f"[PASS] 4분기 전부 도달: {got}")


def test_every_branch_uses_sajang_trailing():
    """★핵심 계약 — 4분기 trailing_sl 이 전부 SAJANG 고점 -3% 와 일치."""
    for name, fn in CASES.items():
        verdict, _conf, _sum, _act, params = _run(fn)
        sl = params.get("trailing_sl")
        assert sl is not None, f"{name}: trailing_sl 미산출"
        assert sl == EXPECTED, (
            f"{name}: trailing_sl {sl:,} != SAJANG {EXPECTED:,} "
            f"(고점 {TODAY_HIGH:,} 기준 {(1 - sl / TODAY_HIGH) * 100:.2f}% — 룰은 "
            f"{SAJANG.TRAILING_PCT}%)"
        )
    print(f"[PASS] 4분기 trailing_sl 전부 {EXPECTED:,} (고점 -{SAJANG.TRAILING_PCT}%)")


def test_no_branch_is_tighter_than_rule():
    """★조이는 방향만 실제로 유효하다(ratchet) — 그 방향의 위반을 따로 고정."""
    for name, fn in CASES.items():
        _v, _c, _s, _a, params = _run(fn)
        sl = params["trailing_sl"]
        assert sl <= EXPECTED, (
            f"{name}: 룰보다 타이트({sl:,} > {EXPECTED:,}). ratchet 이 조이는 값만 채택하므로 "
            f"이 값이 실매도 트리거가 된다 — 사장님 룰 3 위반"
        )
    print("[PASS] 어떤 분기도 룰보다 조이지 않음")


def test_no_hardcoded_trailing_multiplier_returns():
    """★[F-151] 교훈 — 기대값만 고치면 '폐기 확인'이 사라진다. 부재를 따로 감시.

    trailing_sl 을 만드는 줄에 리터럴 배수가 되살아나면 잡는다.
    (주석 안의 과거 기록은 대상 아님 — 코드 줄만 본다)
    """
    src = (ROOT / "data" / "intraday_eye.py").read_text(encoding="utf-8")
    bad = []
    for i, line in enumerate(src.splitlines(), 1):
        code = line.split("#", 1)[0]              # 주석 제거
        if "trailing_sl" not in code:
            continue
        for lit in ("0.985", "0.995", "0.975", "0.97"):
            if lit in code:
                bad.append(f"{i}: {code.strip()}")
    assert not bad, "trailing_sl 산출에 하드코딩 배수 부활:\n  " + "\n  ".join(bad)
    print("[PASS] trailing_sl 산출 줄에 하드코딩 배수 0건")


def test_cto_report_params_derive_from_sajang():
    """사장님께 보고되는 리포트 수치도 SAJANG 파생인지 (8/10 [F-90] 부수 정정).

    trading_cto.DEFAULT_PARAMS 는 매매 판정에 안 쓰이고 텔레그램 '주요 파라미터'
    출력에만 쓰인다. 그래도 폐기된 값(-1.5%·trailing 2.0)을 사장님께 보여주면
    룰이 두 개인 것과 같다([F-109] 동형).
    """
    from bot.trading_cto import TradingCTO
    p = TradingCTO.DEFAULT_PARAMS
    assert p["risk"]["trailing_pct"] == SAJANG.TRAILING_PCT
    assert p["risk"]["sl_pct"] == SAJANG.NORMAL_SL_PCT
    assert abs(p["eye"]["trailing_sl_ratio"] - (1 - SAJANG.TRAILING_PCT / 100)) < 1e-9
    for dead in ("alive_trailing_sl", "bouncing_sl", "weakening_trailing_sl"):
        assert dead not in p["eye"], f"폐기된 분기별 키 부활: {dead}"
    print("[PASS] CTO 리포트 파라미터 SAJANG 파생")


# ── ★ 음성 대조 — 구코드에서 실제로 잡히는가 ────────────────────────────
#   8/7·8/9 에 음성 대조를 `HEAD:` 에 묶는 실수를 이틀 연속 했다. 기준이 움직이면 대조가 아니다.
#   여기서는 **현재 소스에 역치환**을 가해 합성 구코드를 만든다 — 커밋·시간에 의존하지 않고
#   "이 검사가 그 결함을 잡는다"만 증명한다.
_REVERT = [
    # (현재 코드, 8/10 이전 코드)
    ('{"trailing_sl": SAJANG.get_trailing_sl(today_high), "tp_adjust": "UP"},',
     '{"trailing_sl": nearest_res, "tp_adjust": "UP"},'),
    ('{"trailing_sl": SAJANG.get_trailing_sl(today_high)},',
     '{"trailing_sl": int(nearest_sup * 0.995)},'),
]


def _load_pre_fix_module():
    """현재 소스를 역치환해 '8/10 이전' 모듈을 임시로 만든다."""
    import importlib.util
    import tempfile
    src = (ROOT / "data" / "intraday_eye.py").read_text(encoding="utf-8")
    for now, before in _REVERT:
        assert src.count(now) == 1, f"역치환 앵커가 유일하지 않다({src.count(now)}건): {now[:50]}"
        src = src.replace(now, before)
    tmpdir = tempfile.mkdtemp(prefix="f90_negctl_")
    path = Path(tmpdir) / "intraday_eye_prefix.py"
    path.write_text(src, encoding="utf-8")
    spec = importlib.util.spec_from_file_location("f90_prefix_eye", path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules["f90_prefix_eye"] = mod
    spec.loader.exec_module(mod)
    return mod


def test_negative_control_pre_fix_code_violates():
    """★구코드에서 BREAKING·BOUNCING 이 실제로 룰을 넘는지 확인.

    여기서 위반이 안 나오면 위의 계약 테스트는 **아무것도 지키지 않는 초록불**이다.
    """
    old = _load_pre_fix_module()

    def run_old(case_fn):
        kw = case_fn()
        b = old.IntradayBuffer("005930", "테스트")
        for i in range(6):
            b.add_bar({"current_price": PRICE, "open": PRICE, "high": TODAY_HIGH,
                       "low": PRICE - 100, "volume": 1000 * (i + 1),
                       "strength": 150.0, "change_rate": 5.0})
        return old.synthesize(b, kw["ma"], kw["vwap_pos"], kw["sr"],
                              kw["flow"], kw["momentum"], kw["prev_verdict"])

    violations = {}
    for name in ("BREAKING", "BOUNCING"):
        verdict, _c, _s, _a, params = run_old(CASES[name])
        assert verdict == name, f"구코드 {name} 분기 미도달"
        sl = params["trailing_sl"]
        if sl > EXPECTED:
            violations[name] = (sl, (1 - sl / TODAY_HIGH) * 100)

    assert "BREAKING" in violations, "구코드 BREAKING 이 위반을 안 냈다 — 검출력 미증명"
    assert "BOUNCING" in violations, "구코드 BOUNCING 이 위반을 안 냈다 — 검출력 미증명"

    # 고친 분기는 구코드에서도 정상이어야 한다(역치환이 엉뚱한 곳을 건드리지 않았다는 증거)
    for name in ("ALIVE", "WEAKENING"):
        _v, _c, _s, _a, params = run_old(CASES[name])
        assert params["trailing_sl"] == EXPECTED, f"역치환이 {name} 분기를 오염시켰다"

    detail = " / ".join(f"{k} {v[0]:,}(고점 -{v[1]:.2f}%)" for k, v in violations.items())
    print(f"[PASS] 음성 대조: 구코드 2분기 위반 검출 — {detail} (룰 {EXPECTED:,})")


if __name__ == "__main__":
    print("=" * 70)
    print("★ [F-90] 장중 눈 트레일링 — 사장님 룰 3(고점 -3% 일관) 회귀 ★")
    print("=" * 70)
    import inspect
    tests = [fn for name, fn in sorted(globals().items())
             if name.startswith("test_") and inspect.isfunction(fn)]
    passed = failed = 0
    for t in tests:
        try:
            t()
            passed += 1
        except AssertionError as e:
            failed += 1
            print(f"[FAIL] {t.__name__}: {e}")
        except Exception as e:
            import traceback
            traceback.print_exc()
            failed += 1
            print(f"[ERROR] {t.__name__}: {type(e).__name__}: {e}")
    print("=" * 70)
    print(f"결과: {passed}/{len(tests)} PASS, {failed} FAIL (발견 {len(tests)}건)")
    sys.exit(0 if failed == 0 and passed == len(tests) else 1)
