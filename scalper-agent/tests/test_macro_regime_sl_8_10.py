# -*- coding: utf-8 -*-
"""[F-158] 레짐별 손절이 사장님 룰 3(-3% 일관)보다 조여지던 것 — 8/10 전체검수.

★ 무엇이었나
  `macro_strategy.get_adjusted_sl()` 이 `min(base_sl_pct, response.sl_pct)` 였다.
  레짐 정의를 실측하면:
      스태그플레이션 sl_pct=0.020 → 실효 **-2.00%**
      비용상승       sl_pct=0.025 → 실효 **-2.50%**
  = 룰(-3%)보다 **조여진다**. 소비처 `auto_trader.py:3710` 은 라이브 매수 경로다.
  ★[F-90](장중 눈 트레일링 -1.5%)과 정확히 같은 유형이고, 이번엔 '레짐'이라는 이름 뒤에 있었다.

★ 왜 지금까지 사고가 안 났나 — 설계가 아니라 우연이다
  `get_current_regime()` 이 nightwatch_report 48h 초과면 '안정'으로 폴백하는데,
  금요일 산출분은 월요일에 72h 라 **레짐이 사실상 늘 '안정'**이었다.
  그 폴백을 고치는 순간 위반이 즉시 유효해진다 → 순서 의존을 없애려 임계 자체에 하한을 걸었다.

★ 검증 시 주의 (내가 처음에 틀린 지점)
  레짐 키는 **한글**이다("스태그플레이션"·"비용상승"·"수요둔화"·"안정").
  영어 키를 넘기면 `REGIME_STRATEGIES.get(regime, ...["안정"])` 폴백이라 전부 -3%로 보여
  **"이상 없음"으로 오판**한다. 아래 테스트는 반드시 정의된 키 전수를 돈다.

실행: python -X utf8 tests/test_macro_regime_sl_8_10.py
"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from data.macro_strategy import (                      # noqa: E402
    REGIME_STRATEGIES, get_adjusted_sl, get_regime_response,
)
from data.sajang_rules import SAJANG                   # noqa: E402

RULE = SAJANG.NORMAL_SL_PCT / 100                      # 0.03


def test_every_defined_regime_respects_rule():
    """★핵심 — **정의된 레짐 전수**에서 실효 SL 이 룰보다 조이지 않는다."""
    bad = []
    for key in REGIME_STRATEGIES:
        eff = get_adjusted_sl(RULE, regime=key)
        if eff < RULE - 1e-12:
            bad.append(f"{key}: -{eff*100:.2f}% (룰 -{RULE*100:.2f}%)")
    assert not bad, "룰보다 조이는 레짐:\n  " + "\n  ".join(bad)
    print(f"[PASS] 레짐 {len(REGIME_STRATEGIES)}종 전수 — 룰보다 조이지 않음")


def test_regime_keys_are_korean_not_english():
    """★내가 처음 틀린 지점을 고정 — 영어 키는 폴백이라 '이상 없음'으로 보인다.

    이 사실이 바뀌면(영어 키 도입 등) 위 전수 테스트의 의미가 달라지므로 같이 잠근다.
    """
    assert "안정" in REGIME_STRATEGIES, "한글 레짐 키가 사라졌다 — 검증 전제 변경"
    # 존재하지 않는 키는 '안정'으로 폴백한다(= 오판 통로)
    fallback = get_regime_response("STABLE")
    assert fallback is REGIME_STRATEGIES["안정"], (
        "미정의 키가 '안정'으로 폴백하지 않는다 — 폴백 규약 변경"
    )
    print("[PASS] 레짐 키는 한글 · 미정의 키는 '안정' 폴백(오판 통로 명시)")


def test_tight_regimes_still_defined_but_clamped():
    """정의 자체는 남아 있고(관측·리포트용) 실효값만 눌린다는 것을 명시.

    정의를 지우지 않은 이유: 레짐 표시·분석 문맥에서 쓰이고, 사장님이 레짐별 SL 을
    쓰기로 결정하면 그때 되살릴 값이다. 지금은 **매매에 반영되지 않는다**만 고정한다.
    """
        # 정의값은 여전히 타이트하다
    assert REGIME_STRATEGIES["스태그플레이션"].sl_pct < RULE
    assert REGIME_STRATEGIES["비용상승"].sl_pct < RULE
    # 그러나 실효값은 룰이다
    assert abs(get_adjusted_sl(RULE, regime="스태그플레이션") - RULE) < 1e-12
    assert abs(get_adjusted_sl(RULE, regime="비용상승") - RULE) < 1e-12
    print("[PASS] 타이트 정의는 보존 · 실효값은 룰로 고정")


def test_negative_control_pre_fix_violates():
    """★음성 대조 — 구식 `min()` 이었다면 실제로 룰을 넘는지 (합성·HEAD 무관)."""
    violations = {}
    for key, resp in REGIME_STRATEGIES.items():
        old = min(RULE, resp.sl_pct)          # 8/10 이전 계산식
        if old < RULE - 1e-12:
            violations[key] = round(old * 100, 2)
    assert "스태그플레이션" in violations and violations["스태그플레이션"] == 2.0, violations
    assert "비용상승" in violations and violations["비용상승"] == 2.5, violations
    print(f"[PASS] 음성 대조: 구식 min() 은 {violations} 로 룰 위반")


def test_live_call_site_passes_rule_value():
    """라이브 호출부가 넘기는 base 가 룰 값인지 — 실효 SL 이 결국 -3% 인지 확인."""
    import io
    import yaml
    cfg = yaml.safe_load(io.open(ROOT / "config.yaml", encoding="utf-8"))

    def find(d, key):
        if isinstance(d, dict):
            if key in d:
                return d[key]
            for v in d.values():
                r = find(v, key)
                if r is not None:
                    return r
        return None

    base = find(cfg, "stop_loss_pct")
    assert base is not None, "config stop_loss_pct 소실"
    assert abs(base - RULE) < 1e-12, f"config SL({base})이 SAJANG({RULE})과 다르다"
    assert abs(get_adjusted_sl(base) - RULE) < 1e-12
    print(f"[PASS] 라이브 경로 실효 SL = -{RULE*100:.1f}% (config {base})")


def test_call_site_still_wired():
    """소비처가 살아 있는지 — 죽었다면 이 fix 의 전제가 바뀐다."""
    src = (ROOT / "bot" / "auto_trader.py").read_text(encoding="utf-8")
    assert "loss_percent = get_adjusted_sl(loss_percent)" in src, (
        "라이브 소비처가 사라졌다 — 이 테스트의 전제를 재확인할 것"
    )
    print("[PASS] auto_trader 소비처 유지")


if __name__ == "__main__":
    print("=" * 72)
    print("★ [F-158] 레짐 손절 — 사장님 룰 3(-3% 일관) 하한 ★")
    print("=" * 72)
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
    print("=" * 72)
    print(f"결과: {passed}/{len(tests)} PASS, {failed} FAIL (발견 {len(tests)}건)")
    sys.exit(0 if failed == 0 and passed == len(tests) else 1)
