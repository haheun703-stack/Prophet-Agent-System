# -*- coding: utf-8 -*-
"""[F-219] 동적 손절선 — 진입가 위 '방어선'과 룰 이탈 (9/19 신설).

★실측이 장부를 넘어섰다. 장부는 *"SL 이 진입가보다 높게 잡힐 수 있다"* 는 **가능성**으로
  적었는데, 자산풀 후보 198종을 재 보니 **70종(35.4%)**이 그 상태였고 최악은 진입가
  **+57.25%** 였다. 봇이 켜져 있었다면 자산풀 매수의 1/3이 **체결 즉시 손절**됐다.

원인: `final_sl = max(inst_cost * 0.97, initial_sl)` — 매집원가가 진입가보다 3.1%+
높으면 '방어선'이 진입가 위로 간다. **진입가 위의 방어선은 방어선이 아니다.**

그리고 이 파일은 SAJANG import **0건**이고 최종 수정이 3/8 — 사장님 영구 룰(5/21~)
**제정보다 앞선다**. 주입 3곳이 전부 실매도 경로다.

★손대지 않은 것: 더 **타이트**해지는 쪽(`sl_atr_mult=0.5`)은 전략 선택이라 사장님 결정 사안.
"""
from data.sajang_rules import SAJANG


def _final_sl(entry_price, inst_cost, initial_sl):
    """dynamic_target 의 최종 SL 결정과 **같은 프로덕션 헬퍼**를 경유한다."""
    ics = int(inst_cost * 0.97) if inst_cost > 0 else 0
    if 0 < ics < entry_price:
        final = max(ics, initial_sl)
    else:
        final = initial_sl
    return SAJANG.clamp_sl(entry_price, final)


def test_defense_line_above_entry_is_ignored():
    """★핵심 — 매집원가가 진입가보다 높으면 그 '방어선'을 쓰지 않는다."""
    entry = 10000
    sl = _final_sl(entry, inst_cost=12000, initial_sl=9500)
    assert sl < entry, "손절선이 진입가 이상이면 매수 즉시 청산"
    assert sl == SAJANG.get_normal_sl(entry)


def test_worst_observed_case_is_fixed():
    """실측 최악(475830: entry 37,400 / inst_cost*0.97 = 58,811)."""
    entry = 37400
    sl = _final_sl(entry, inst_cost=58811 / 0.97, initial_sl=int(entry * 0.95))
    assert sl < entry
    assert (sl - entry) / entry * 100 >= -3.05


def test_valid_defense_line_is_kept():
    """진입가 **아래**의 방어선은 그대로 쓴다 — 기존 의도 보존."""
    entry = 10000
    sl = _final_sl(entry, inst_cost=10100, initial_sl=9500)   # ics 9797
    assert sl == 9797


def test_deeper_than_rule_is_clamped():
    entry = 10000
    sl = _final_sl(entry, inst_cost=9500, initial_sl=9000)    # ics 9215, initial -10%
    assert sl == SAJANG.get_normal_sl(entry)


def test_no_inst_cost_falls_back_to_atr_then_rule():
    entry = 10000
    assert _final_sl(entry, inst_cost=0, initial_sl=9900) == 9900      # 타이트 유지
    assert _final_sl(entry, inst_cost=0, initial_sl=8000) == SAJANG.get_normal_sl(entry)


def test_source_guards_are_present():
    """구현이 실제로 두 가드를 갖는가 — 값이 아니라 코드를 본다."""
    from pathlib import Path
    src = (Path(__file__).resolve().parent.parent
           / "strategies" / "dynamic_target.py").read_text(encoding="utf-8")
    assert "if 0 < inst_cost_sl < entry_price:" in src, "진입가 위 방어선 가드 없음"
    assert "SAJANG.clamp_sl(entry_price, final_sl)" in src, "사장님 룰 하한 없음"
    assert "max(inst_cost_sl, initial_sl) if inst_cost_sl > 0 else initial_sl" not in src, \
        "옛 무가드 식이 남아 있다"


# ------------------------------------------------------------------
#  [F-245] 폐기된 고정 +5% 익절 · SAJANG 우회
# ------------------------------------------------------------------

def test_no_fixed_take_profit_literal():
    """★사장님 1번 영구 룰 — 트레일링 only, 고정 TP 폐기.

    폴백 경로가 `entry_price * 1.05`(= 고정 +5% 익절)를 쓰고 있었다.
    RULE-005 가 바로 이것을 잡는 규칙인데 이 파일은 staged 된 적이 없어
    **규칙이 단 한 번도 돌지 않았다**([F-241]).
    """
    from pathlib import Path
    src = (Path(__file__).resolve().parent.parent
           / "strategies" / "dynamic_target.py").read_text(encoding="utf-8")
    code_lines = [l for l in src.splitlines() if not l.strip().startswith("#")]
    body = "\n".join(code_lines)
    assert "* 1.05" not in body, "폐기된 고정 +5% 익절이 남아 있다"
    assert "SAJANG.get_take_profit(entry_price)" in body


def test_sajang_is_the_owner_of_sl_and_tp():
    """값이 맞는 것과 SAJANG 을 경유하는 것은 다르다 — 룰이 바뀌면 따라가야 한다."""
    from pathlib import Path
    src = (Path(__file__).resolve().parent.parent
           / "strategies" / "dynamic_target.py").read_text(encoding="utf-8")
    code_lines = [l for l in src.splitlines() if not l.strip().startswith("#")]
    body = "\n".join(code_lines)
    assert "SAJANG.get_normal_sl(entry_price)" in body
    assert "SAJANG.get_trailing_sl(hwm)" in body
    assert "int(hwm * 0.97)" not in body, "트레일링이 여전히 SAJANG 을 우회한다"
    assert "int(entry_price * 0.97)" not in body, "SL 이 여전히 SAJANG 을 우회한다"


def test_fixed_tp_is_actually_disabled():
    assert SAJANG.FIXED_TP_DISABLED is True
    assert SAJANG.get_take_profit(10000) == 0
