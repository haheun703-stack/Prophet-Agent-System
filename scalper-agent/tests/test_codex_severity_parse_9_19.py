# -*- coding: utf-8 -*-
"""[F-246] Codex 심각도 판정이 문자열 존재만 봤다 (9/19 신설).

판정이 `"[CRITICAL]" in review_text` 였다. Codex 는 **해소된 항목**에도 심각도 태그를
붙인다 — 예: *"[CRITICAL] initial_tp 가 SAJANG.get_take_profit 으로 수정되어 사장님
영구 룰을 준수하고 있습니다"*.

그래서 **룰 위반을 없애는 커밋이 오히려 차단**됐다([F-245] 에서 2회 재현).
이런 오탐이 반복되면 사람이 매번 우회하게 되고 **hook 자체가 무의미해진다.**

★차단을 푸는 게 아니라 **줄 단위 판독**이다 — 해소 표현이 없는 CRITICAL 줄이
하나라도 있으면 **그대로 차단**한다.
"""
import importlib.util
from pathlib import Path

_spec = importlib.util.spec_from_file_location(
    "codex_pre_commit",
    Path(__file__).resolve().parent.parent.parent / "tools" / "codex_pre_commit.py")
cpc = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(cpc)

RESOLVED = ("   - [CRITICAL] dynamic_target.py 213: initial_tp 가 "
            "SAJANG.get_take_profit 으로 수정되어 사장님 영구 룰을 준수하고 있습니다.")
REAL = ("   - [CRITICAL] auto_trader.py 934: 매도 경로에 SL 가드가 누락되어 "
        "사고 위험이 있습니다.")


def test_resolved_tag_does_not_block():
    """★[F-245] 재현 — 칭찬 문장이 커밋을 막으면 안 된다."""
    real, resolved = cpc._severity_lines(RESOLVED, "CRITICAL")
    assert real == []
    assert len(resolved) == 1


def test_real_violation_still_blocks():
    """★차단이 약해지면 안 된다 — 이게 이 hook 의 존재 이유다."""
    real, resolved = cpc._severity_lines(REAL, "CRITICAL")
    assert len(real) == 1
    assert resolved == []


def test_mixed_blocks_conservatively():
    """해소와 진짜가 섞이면 **차단**(보수적)."""
    real, resolved = cpc._severity_lines(RESOLVED + "\n" + REAL, "CRITICAL")
    assert len(real) == 1 and len(resolved) == 1


def test_high_level_uses_same_parser():
    text = "   - [HIGH] 무언가 누락되어 위험합니다."
    real, _ = cpc._severity_lines(text, "HIGH")
    assert len(real) == 1


def test_untagged_lines_are_ignored():
    text = "이 줄은 CRITICAL 이라는 단어가 본문에 있지만 태그가 아니다"
    real, resolved = cpc._severity_lines(text, "CRITICAL")
    assert real == [] and resolved == []


def test_empty_and_none_are_safe():
    for t in ("", None):
        real, resolved = cpc._severity_lines(t, "CRITICAL")
        assert real == [] and resolved == []


def test_old_naive_check_would_have_blocked():
    """음성대조 — 옛 판정식이면 해소 문장도 차단했다."""
    assert ("[CRITICAL]" in RESOLVED) is True      # 옛 식 = 차단
    real, _ = cpc._severity_lines(RESOLVED, "CRITICAL")
    assert real == []                              # 새 식 = 통과
