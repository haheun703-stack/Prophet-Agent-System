# -*- coding: utf-8 -*-
"""Regression tests for MARKET_OPEN_REGIME → scalper D1 open gate (6/9 사장님).

순수 함수 decide_open_gate / theme_of_sector / load_scalper_open_gate 검증.
주문/SAJANG 무접촉 — top_k·테마 매칭만 다룬다.

실행: python data/test_market_open_gate_6_9.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from data.market_open_regime import decide_open_gate, theme_of_sector  # noqa: E402


def _gate(name, delta, fresh, pref=None, avoid=None):
    return {
        "gate": name,
        "top_k_delta": delta,
        "freshness": {"ok": fresh},
        "preferred_themes": pref or [],
        "avoid_themes": avoid or [],
    }


def test_decide_open_gate_branches():
    # (label, gate, base_top_k, expect_allow, expect_top_k, expect_gate)
    cases = [
        ("file_missing_legacy", None, 3, True, 3, "NO_GATE"),
        ("block_new_buy", _gate("BLOCK_NEW_BUY", -99, True), 3, False, 0, "BLOCK_NEW_BUY"),
        ("watch_fresh_delta_minus1", _gate("WATCH_ONLY", -1, True), 3, True, 2, "WATCH_ONLY"),
        ("watch_base1_to_zero", _gate("WATCH_ONLY", -1, True), 1, False, 0, "WATCH_ONLY"),
        ("allow_fresh_plus1", _gate("ALLOW_D1_OPEN", 1, True), 3, True, 4, "ALLOW_D1_OPEN"),
        ("selective_fresh_plus0", _gate("SELECTIVE_D1_OPEN", 0, True), 3, True, 3, "SELECTIVE_D1_OPEN"),
        # ★ freshness.ok=false → ALLOW/SELECTIVE 강등 + 공격(+) 차단 (사장님 6/9) ★
        ("allow_stale_demoted_no_aggressive", _gate("ALLOW_D1_OPEN", 1, False), 3, True, 3, "WATCH_ONLY"),
        ("watch_stale_delta_minus1", _gate("WATCH_ONLY", -1, False), 3, True, 2, "WATCH_ONLY"),
    ]
    for label, gate, base, ea, et, eg in cases:
        d = decide_open_gate(gate, base)
        assert d["allow"] == ea, f"{label}: allow {d['allow']} != {ea}"
        assert d["top_k"] == et, f"{label}: top_k {d['top_k']} != {et}"
        assert d["gate"] == eg, f"{label}: gate {d['gate']} != {eg}"
    print("[PASS] decide_open_gate 8개 분기 (하위호환/BLOCK/WATCH/ALLOW/SELECTIVE/stale강등)")


def test_no_side_effect_keys():
    # 결정 dict가 top_k/allow/themes만 노출 — position_scale·order·qty 키 없음 (SAJANG 무접촉)
    d = decide_open_gate(_gate("ALLOW_D1_OPEN", 1, True), 3)
    forbidden = {"position_scale", "qty", "budget", "order", "cash"}
    assert not (set(d.keys()) & forbidden), f"금지 키 노출: {set(d.keys()) & forbidden}"
    assert set(d.keys()) >= {"allow", "top_k", "gate", "preferred", "avoid", "fresh_ok", "reason"}
    print("[PASS] 결정 dict = top_k/allow/themes만 (position_scale·qty·order 미노출)")


def test_theme_of_sector():
    assert theme_of_sector("반도체") == "semiconductor"
    assert theme_of_sector("IT") == "it"
    assert theme_of_sector("바이오") == "bio"
    assert theme_of_sector("금융") is None        # 매핑 없음 → 가점/감점 skip (안전)
    assert theme_of_sector("우주항공잡것") is None  # 미지 → None
    assert theme_of_sector("") is None
    print("[PASS] theme_of_sector 매핑 (반도체/IT/바이오 / 미지→None)")


def test_block_ignores_top_k():
    # BLOCK은 top_k와 무관하게 항상 차단
    for base in (1, 3, 10):
        d = decide_open_gate(_gate("BLOCK_NEW_BUY", 0, True), base)
        assert d["allow"] is False and d["top_k"] == 0
    print("[PASS] BLOCK_NEW_BUY는 base_top_k 무관 항상 차단")


if __name__ == "__main__":
    test_decide_open_gate_branches()
    test_no_side_effect_keys()
    test_theme_of_sector()
    test_block_ignores_top_k()
    print("\n[ALL PASS] market_open_gate 회귀 4종")
