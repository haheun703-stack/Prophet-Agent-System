# -*- coding: utf-8 -*-
"""[F-226] 손절선이 사장님 -3% 룰보다 깊어지지 않는다 (9/19 신설).

[실측] 9/18 라이브 픽 8건 중 **7건이 룰 이탈**, 최악 **-28.01%**(SK스퀘어).
  경로가 둘이었다 — 장부는 하나만 지목했다:
    · `stocks`        ← morning_recommendation: `or` 체인이라 FIB이 있으면 SAJANG이 영영 안 쓰임
    · `momentum_stocks` ← momentum_scanner: 하한이 `entry * 0.90`(-10%) · SAJANG import 0건

★하한만 건다. 더 타이트한 SL(ATR이 좁은 경우)은 그대로 — 그쪽 논점은 [F-219] 별건.
"""
from data.sajang_rules import SAJANG


def _pct(entry, sl):
    return (sl - entry) / entry * 100


# ------------------------------------------------------------------
#  룰 자체
# ------------------------------------------------------------------

def test_sajang_normal_sl_is_three_percent():
    assert SAJANG.NORMAL_SL_PCT == 3.0
    e = 100000
    assert _pct(e, SAJANG.get_normal_sl(e)) == -3.0


# ------------------------------------------------------------------
#  경로 ① momentum_scanner — 하한이 룰이어야 한다
# ------------------------------------------------------------------

def _momentum_sl(entry, atr):
    """momentum_scanner 와 **같은 프로덕션 함수**를 부른다(복제 금지 — 9/19 T-1 교훈)."""
    return SAJANG.clamp_sl(entry, int(entry - atr * 1.0))


def test_momentum_deep_atr_is_clamped_to_rule():
    """★ATR이 크면 옛 코드는 -10%까지 허용했다 — 이제 -3%에서 멈춘다."""
    entry = 1200
    sl = _momentum_sl(entry, atr=200)           # ATR 기준이면 1000 = -16.7%
    assert _pct(entry, sl) >= -3.05, f"{_pct(entry, sl):.2f}%"
    old = max(int(entry - 200 * 1.0), int(entry * 0.90))
    assert _pct(entry, old) < -3.5, "음성대조: 옛 식은 룰을 이탈했어야 한다"


def test_momentum_tight_atr_is_kept():
    """ATR이 좁으면 그대로 둔다 — 하한만 거는 것이지 -3%로 고정하는 게 아니다."""
    entry = 100000
    sl = _momentum_sl(entry, atr=1000)          # -1%
    assert sl == 99000
    assert _pct(entry, sl) == -1.0


def test_momentum_scanner_source_uses_sajang():
    """구현이 실제로 SAJANG을 경유하는가 — 값이 아니라 **코드**를 본다."""
    from pathlib import Path
    src = (Path(__file__).resolve().parent.parent
           / "strategies" / "momentum_scanner.py").read_text(encoding="utf-8")
    assert "SAJANG.clamp_sl(entry" in src
    assert "int(entry * 0.90)" not in src, "옛 -10% 하한이 남아 있다"


# ------------------------------------------------------------------
#  경로 ② morning_recommendation — or 체인 뒤에 클램프
# ------------------------------------------------------------------

def _rec_sl(entry, fib_sl=0, p_sl=0, m_sl=0):
    """morning_recommendation 와 **같은 프로덕션 함수**를 부른다."""
    floor = SAJANG.get_normal_sl(entry)
    raw = int(fib_sl or p_sl or m_sl or floor)
    return SAJANG.clamp_sl(entry, raw), raw


def test_fib_deeper_than_rule_is_clamped():
    """★실측 재현 — SK스퀘어 -28.01%가 -3%로."""
    entry, fib = 1073000, 772484
    sl, raw = _rec_sl(entry, fib_sl=fib)
    assert _pct(entry, raw) < -28, "음성대조: 원래 값은 룰을 크게 이탈"
    assert round(_pct(entry, sl), 2) == -3.00


def test_fib_tighter_than_rule_is_kept():
    entry = 211500
    sl, _raw = _rec_sl(entry, fib_sl=209000)    # -1.18%
    assert sl == 209000


def test_no_source_falls_back_to_rule():
    entry = 50000
    sl, _ = _rec_sl(entry)
    assert sl == SAJANG.get_normal_sl(entry)


def test_all_live_picks_would_now_comply():
    """9/18 라이브 픽 8건 실값 — 전건이 룰 안으로 들어온다."""
    live = [                     # (entry, 기존 sl)
        (1073000, 772484), (202000, 177407), (211500, 205155),
        (4345, 3910), (1655, 1554), (954, 858), (1200, 1080), (2660, 2511),
    ]
    before_bad = sum(1 for e, s in live if _pct(e, s) < -3.05)
    assert before_bad == 7, f"실측 전제가 바뀌었다: {before_bad}"
    for e, s in live:
        fixed = max(s, SAJANG.get_normal_sl(e))
        assert _pct(e, fixed) >= -3.05, f"entry {e} sl {fixed} = {_pct(e, fixed):.2f}%"


def test_recommendation_source_has_clamp():
    from pathlib import Path
    src = (Path(__file__).resolve().parent.parent
           / "data" / "morning_recommendation.py").read_text(encoding="utf-8")
    assert "_sl_floor = SAJANG.get_normal_sl(entry)" in src, "기준가가 entry여야 한다"
    assert "SAJANG.clamp_sl(entry, _sl_raw)" in src
    assert "→SAJANG" in src, "클램프 사실을 sl_source에 남겨야 한다"


# ------------------------------------------------------------------
#  헬퍼 자체 — 경계·이상값
# ------------------------------------------------------------------

def test_clamp_handles_bad_input():
    e = 10000
    floor = SAJANG.get_normal_sl(e)
    assert SAJANG.clamp_sl(e, 0) == floor
    assert SAJANG.clamp_sl(e, -1) == floor
    assert SAJANG.clamp_sl(e, None) == floor
    assert SAJANG.clamp_sl(e, "abc") == floor


def test_clamp_is_idempotent():
    e = 10000
    once = SAJANG.clamp_sl(e, 5000)
    assert SAJANG.clamp_sl(e, once) == once


def test_clamp_never_loosens_a_tight_stop():
    e = 10000
    for proposed in (9900, 9800, 9750, 9700):
        assert SAJANG.clamp_sl(e, proposed) == proposed
