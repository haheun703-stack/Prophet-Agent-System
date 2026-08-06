# -*- coding: utf-8 -*-
"""Rule D D-day recognition regression tests for the 5/22 -> 5/27 miss."""

from datetime import date
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


def _signal(close=1550):
    from bot.rule_d_dday_guard import RuleDSignalInfo

    return RuleDSignalInfo(
        code="009190",
        name="Daeyang Metal",
        signal_date=date(2026, 5, 22),
        signal_close_price=close,
    )


def test_dday_d0_buy():
    """5/22 signal -> 5/22 D+0 passes with priority."""
    from bot.rule_d_dday_guard import evaluate_rule_d_signal

    d = evaluate_rule_d_signal(_signal(), current_price=1550, today=date(2026, 5, 22))
    assert d.allowed is True
    assert d.d_plus == 0
    assert d.priority_boost == 30


def test_dday_d3_block():
    """5/22 signal -> 5/27 late entry is blocked."""
    from bot.rule_d_dday_guard import evaluate_rule_d_signal

    d = evaluate_rule_d_signal(_signal(), current_price=2084, today=date(2026, 5, 27))
    assert d.allowed is False
    assert "block" in d.reason


def test_cumulative_20pct_block():
    """Cumulative +20%+ is always blocked."""
    from bot.rule_d_dday_guard import evaluate_rule_d_signal

    d = evaluate_rule_d_signal(_signal(), current_price=1860, today=date(2026, 5, 26))
    assert d.allowed is False
    assert d.cumulative_pct is not None and d.cumulative_pct >= 20.0 - 1e-9


def test_d1_within_5pct_pass():
    """D+1 within +5% remains eligible."""
    from bot.rule_d_dday_guard import evaluate_rule_d_signal

    d = evaluate_rule_d_signal(_signal(), current_price=1620, today=date(2026, 5, 26))
    assert d.allowed is True
    assert d.d_plus == 1
    assert d.priority_boost == 15


def test_watchlist_signal_close_price_alias(tmp_path):
    """watchlist.json rows expose signal_close_price for Rule D."""
    from bot.rule_d_dday_guard import load_signal_map

    path = tmp_path / "watchlist.json"
    path.write_text(
        json.dumps({
            "items": [{
                "code": "009190",
                "name": "Daeyang Metal",
                "signal_date": "2026-05-22",
                "signal_close": 1550,
                "status": "monitoring",
            }]
        }, ensure_ascii=False),
        encoding="utf-8",
    )
    signals = load_signal_map(path)
    assert signals["009190"].signal_close_price == 1550


if __name__ == "__main__":
    # ★8/6 [F-124-2] 자동 발견으로 교체 — 이전엔 테스트 5개 중 **4개만 손으로 적은
    # 목록**을 돌리고 `RESULT: 4/4 PASS`를 찍었다. 빠진 하나
    # (`test_watchlist_signal_close_price_alias`)는 pytest `tmp_path` 픽스처를 써서
    # 목록에 넣기 번거로웠던 것이고, 이 저장소엔 pytest가 없어([F-119]) **어떤 경로로도
    # 실행되지 않는 테스트**였다. 분모가 실행한 개수라 100%가 나오는 구조 —
    # 7/20 *"성공 카운트 ≠ 성공 증거"*와 정확히 같은 형태다.
    # 손으로 관리하는 목록은 언젠가 반드시 갈린다 → 모듈에서 직접 수집한다.
    import inspect
    import tempfile

    tests = [fn for name, fn in sorted(globals().items())
             if name.startswith("test_") and inspect.isfunction(fn)]
    passed = 0
    failed = 0
    for test in tests:
        try:
            kwargs = {}
            # pytest 없이도 픽스처 요구를 충족시킨다(현재는 tmp_path 하나뿐).
            if "tmp_path" in inspect.signature(test).parameters:
                kwargs["tmp_path"] = Path(tempfile.mkdtemp(prefix="ruled_"))
            test(**kwargs)
            print(f"[PASS] {test.__name__}")
            passed += 1
        except Exception as exc:
            print(f"[FAIL] {test.__name__}: {exc}")
            failed += 1
    # 분모는 '발견한 개수' — 실행한 개수를 분모로 쓰면 누락이 100%로 위장된다.
    print(f"RESULT: {passed}/{len(tests)} PASS (발견 {len(tests)}건)")
    sys.exit(0 if failed == 0 and passed == len(tests) else 1)
