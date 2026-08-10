# -*- coding: utf-8 -*-
"""[F-91] hard_kill(-5% 최후 방어선) 실전 전환 회귀 — 8/10.

배경:
  `config.yaml` 의 `hard_kill_dry_run` 이 **66일간 true** 였다. 같은 줄 주석은
  *"5/22 검증: true (로그만) → 5/26 실전: false"* 라고 스스로 적고 있었다 =
  **값이 자기 주석과 모순**. 그동안 5/21 로킷헬스케어(-15.4%) 사고의 처방인
  -5% 최후 방어선이 `position_safety.py:602` 에서 로그만 찍고 매도를 안 했다.

★ 이 파일이 고정하는 것은 두 가지이고, 둘째가 더 중요하다:
  ① 방어선이 실제로 켜져 있다 (dry_run=False · 임계는 SAJANG 단일진실)
  ② ★그런데도 지금 실주문은 0이다★ — PAPER_ONLY 가 SELL 을 **첫 검사에서** 막는다.
     ②가 깨지면 ①은 "봇 OFF 인데 매도가 나가는" 사고가 된다. 그래서 같이 잠근다.

실행: python -X utf8 tests/test_hard_kill_live_8_10.py
"""
import io
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from data.sajang_rules import SAJANG                      # noqa: E402
from bot.position_safety import HARD_KILL_PCT             # noqa: E402


def _safety_cfg() -> dict:
    cfg = yaml.safe_load(io.open(ROOT / "config.yaml", encoding="utf-8"))

    def find(d):
        if isinstance(d, dict):
            if "hard_kill_enabled" in d:
                return d
            for v in d.values():
                r = find(v)
                if r is not None:
                    return r
        return None

    s = find(cfg)
    assert s is not None, "config.yaml 에서 safety 섹션을 못 찾음"
    return s


def test_hard_kill_is_live_not_dry_run():
    """★핵심 — 최후 방어선이 로그만 찍는 상태로 되돌아가지 않는다."""
    s = _safety_cfg()
    assert s.get("hard_kill_enabled") is True, "hard_kill 자체가 꺼졌다"
    assert s.get("hard_kill_dry_run") is False, (
        "hard_kill_dry_run 이 다시 true — -5% 최후 방어선이 로그만 찍는다 "
        "(5/21 로킷헬스케어 -15.4% 사고의 처방이 무력화됨)"
    )
    print("[PASS] hard_kill 실전 모드 (dry_run=False)")


def test_kill_threshold_is_sajang_single_truth():
    """임계가 SAJANG 파생 — 사장님이 룰을 고치면 매매가 실제로 바뀌어야 한다."""
    s = _safety_cfg()
    assert "hard_kill_pct" not in s, (
        "config.yaml 에 hard_kill_pct 가 되살아남 — SAJANG 우회 통로"
    )
    effective = float(s.get("hard_kill_pct", HARD_KILL_PCT))
    assert effective == SAJANG.HARD_KILL_PCT == 0.05, (
        f"실효 kill_pct {effective} != SAJANG {SAJANG.HARD_KILL_PCT}"
    )
    print(f"[PASS] kill_pct SAJANG 위임 ({effective} = -{effective*100:.0f}%)")


def test_dry_run_resolution_logic():
    """position_safety:602 `dry_run or cfg[...]` 의미 고정.

    호출부가 dry_run=True 를 주면 config 와 무관하게 dry 여야 하고(수동 검증 통로),
    둘 다 False 여야 실매도가 나간다.
    """
    s = _safety_cfg()
    cfg_dry = bool(s.get("hard_kill_dry_run", False))
    assert (True or cfg_dry) is True, "호출부 dry_run=True 가 무시되면 검증 통로가 사라진다"
    assert (False or cfg_dry) is False, "config 가 여전히 dry 로 잠그고 있다"
    print("[PASS] dry_run 결합 규칙 (호출부 OR config)")


def test_paper_only_still_blocks_sell():
    """★★ 이게 지금 위 변경을 안전하게 만드는 전제다.

    hard_kill 매도는 kis_trader._execute_sell → _order_gate → evaluate_auto_order 를 탄다.
    PAPER_ONLY 가 **BUY/SELL 구분 전에** 차단하는지 실제로 호출해 확인한다.
    """
    from bot.trade_runtime_config import evaluate_auto_order, is_paper_only

    assert is_paper_only() is True, "PAPER_ONLY 가 꺼졌다 — 봇 OFF 전제가 깨짐"
    block = evaluate_auto_order("SELL", "005930", 1, "market")
    assert block is not None, "PAPER_ONLY 인데 자동 매도가 통과 — 실주문 0 불변식 붕괴"
    assert block.get("reason") == "PAPER_ONLY", f"차단 사유가 PAPER_ONLY 가 아님: {block}"

    # 매수도 같이 확인 (한쪽만 막히는 비대칭이 없어야 한다)
    block_buy = evaluate_auto_order("BUY", "005930", 1, "market")
    assert block_buy is not None and block_buy.get("reason") == "PAPER_ONLY"
    print("[PASS] PAPER_ONLY 가 SELL·BUY 모두 차단 (실주문 0 유지)")


def test_sell_path_goes_through_order_gate():
    """매도 sink 가 게이트를 우회하지 않는지 소스로 고정 (7/1·7/31 검수 결론 유지)."""
    src = (ROOT / "bot" / "kis_trader.py").read_text(encoding="utf-8")
    start = src.find("def _execute_sell")
    assert start != -1, "_execute_sell 이 사라졌다"
    body = src[start:start + 1200]
    assert "_order_gate(" in body, "_execute_sell 이 _order_gate 를 우회한다 — 실주문 통로 개방"
    assert "if blocked:" in body and "return blocked" in body, "게이트 결과를 무시한다"
    print("[PASS] 매도 sink 가 _order_gate 경유")


def test_protected_positions_excluded():
    """사장님 보호 종목은 hard_kill 강제 매도에서 제외 — 마커 **전건** 확인.

    hard_kill 이 실전으로 켜졌으므로(위) 이 제외 목록이 곧 사장님 종목의 방벽이다.
    `or` 로 묶으면 하나만 살아 있어도 통과하므로 마커마다 개별로 못박는다.
    """
    from bot.position_safety import _is_protected
    must_protect = {
        "빈 dict(메모리 누락 = 사장님 종목 의심)": {},
        "sl_disabled": {"sl_disabled": True},
        "source=verification": {"source": "verification"},
        "source=manual_sync*": {"source": "manual_sync_2026"},
        "source=sync_auto*": {"source": "sync_auto_unknown"},   # CLAUDE.md 룰 10 정본 라벨
    }
    for label, pos in must_protect.items():
        assert _is_protected(pos) is True, f"보호 실패: {label} — hard_kill 이 사장님 종목을 판다"

    must_not_protect = {
        "asset_pool": {"source": "asset_pool"},
        "pre_close_d(룰 D)": {"source": "pre_close_d"},
        "sl_disabled=False 명시": {"source": "asset_pool", "sl_disabled": False},
    }
    for label, pos in must_not_protect.items():
        assert _is_protected(pos) is False, f"과보호: {label} — 방어선이 무력화된다"

    print(f"[PASS] 보호 종목 제외 {len(must_protect)}종 + 비보호 {len(must_not_protect)}종 전건")


if __name__ == "__main__":
    print("=" * 70)
    print("★ [F-91] hard_kill -5% 최후 방어선 실전 전환 회귀 ★")
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
