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
    tests = [
        test_dday_d0_buy,
        test_dday_d3_block,
        test_cumulative_20pct_block,
        test_d1_within_5pct_pass,
    ]
    passed = 0
    failed = 0
    for test in tests:
        try:
            test()
            print(f"[PASS] {test.__name__}")
            passed += 1
        except Exception as exc:
            print(f"[FAIL] {test.__name__}: {exc}")
            failed += 1
    print(f"RESULT: {passed}/{passed + failed} PASS")
    sys.exit(0 if failed == 0 else 1)
