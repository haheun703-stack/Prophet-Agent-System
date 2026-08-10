# -*- coding: utf-8 -*-
"""[F-157] hard_kill 부분체결 자기무장해제 루프 차단 — 8/10 전체검수.

★ 무엇이 문제였나 (연쇄)
  1. hard_kill(-5%) → `sell_market(code, qty)` — 기본 **3분할**
  2. `sell_market` 의 `success = total_filled > 0` = **1청크만 접수돼도 True**
  3. hard_kill 이 그 success 만 보고 **메모리에서 포지션 삭제**
  4. KIS 에는 잔량이 남음
  5. 5분 뒤 `sync_positions` 케이스 A → `source="sync_auto_unknown"` 으로 재등록
  6. `_is_protected` 가 `sync_auto*` 를 **보호**로 판정
  → **그 종목은 영구히 -5% 최후 방어선 밖**. 방어선이 부분체결 1회로 스스로를 무장해제한다.

★ 이 경로는 8/10 dry_run 해제([F-91])로 **처음 열렸다.** 켠 쪽이 감사해서 찾았다.

★ fix
  - `sell_market` 반환에 `filled_qty`/`requested_qty`/`partial` 추가
    (체결 대조 정본 `_reconcile_fill` 재사용 — smart_* 3경로가 이미 쓰는데 시장가만 빠져 있었다.
     `success` 의미는 불변 → 호출처 10곳 영향 0)
  - hard_kill 은 **전량 확인 시에만 삭제**, 부분이면 수량만 줄이고 **원래 source 유지**
  - 저널은 `filled > 0` 이면 **실체결량으로** 적재(부분체결 손익이 장부에서 사라지지 않게)

실행: python -X utf8 tests/test_hard_kill_partial_fill_8_10.py
"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from bot.position_safety import _is_protected      # noqa: E402
from data.sajang_rules import SAJANG               # noqa: E402


# ── 가짜 트레이더 — 분할 매도에서 일부만 접수되는 상황 재현 ────────────────
class FakeBroker:
    def __init__(self, accept_chunks: int):
        self.accept_chunks = accept_chunks
        self.calls = 0

    def create_market_sell_order(self, symbol, quantity):
        self.calls += 1
        if self.calls <= self.accept_chunks:
            return {"rt_cd": "0", "output": {"ODNO": f"X{self.calls}"}}
        return {"rt_cd": "1", "msg1": "호가 없음"}


def _make_trader(accept_chunks: int, qty_before: int, qty_after: int):
    """KISTrader 를 상속해 네트워크 경계만 가짜로 바꾼다(주문 로직은 진짜)."""
    from bot.kis_trader import KISTrader

    class T(KISTrader):
        def __init__(self):
            self.config = {"risk": {"split_count": 3}}
            self._broker = FakeBroker(accept_chunks)
            self._balance_seq = [qty_before, qty_after]

        def _get_broker(self):
            return self._broker

        def _order_gate(self, *a, **kw):
            return None                      # 게이트 통과 상황만 재현(게이트 자체는 별 테스트)

        def _holding_qty_for_reconcile(self, code):
            return self._balance_seq.pop(0) if self._balance_seq else qty_after

        def _log_trade(self, *a, **kw):
            return None

    return T()


def test_sell_market_reports_partial_fill():
    """★핵심 — 3분할 중 1청크만 접수되면 filled_qty 가 실체결(계좌 델타)을 말한다."""
    t = _make_trader(accept_chunks=1, qty_before=30, qty_after=20)   # 10주만 실제로 나감
    r = t.sell_market("005930", 30)
    assert r["success"] is True, "구동작 유지(1청크라도 접수되면 success)"
    assert r["requested_qty"] == 30
    assert r["filled_qty"] == 10, f"실체결 오보: {r}"
    assert r["partial"] is True
    print(f"[PASS] 부분체결 보고: filled={r['filled_qty']}/{r['requested_qty']} partial={r['partial']}")


def test_sell_market_full_fill():
    t = _make_trader(accept_chunks=3, qty_before=30, qty_after=0)
    r = t.sell_market("005930", 30)
    assert r["filled_qty"] == 30 and r["partial"] is False, r
    print("[PASS] 전량체결 보고")


def test_sell_market_keys_are_additive():
    """호출처 10곳이 읽는 기존 키(success·message)가 그대로인지 — 회귀 차단."""
    t = _make_trader(accept_chunks=3, qty_before=30, qty_after=0)
    r = t.sell_market("005930", 30)
    assert "success" in r and "message" in r, "기존 키 소실 — 호출처 10곳 영향"
    assert isinstance(r["success"], bool)
    print("[PASS] 기존 반환 키 보존(추가만)")


# ── ★자기무장해제 루프 — 부품별로 못박는다 ──────────────────────────────
def test_sync_auto_source_is_protected():
    """루프의 마지막 고리: sync 재등록 라벨이 보호로 판정된다(사실 확인)."""
    assert SAJANG.SYNC_AUTO_SOURCE == "sync_auto_unknown", SAJANG.SYNC_AUTO_SOURCE
    assert _is_protected({"source": SAJANG.SYNC_AUTO_SOURCE}) is True, (
        "sync_auto 가 보호가 아니라면 이 루프는 성립하지 않는다 — 전제 변경 시 이 테스트를 갱신"
    )
    print("[PASS] 루프 전제 확인: sync_auto_unknown = 보호 대상")


def test_hard_kill_keeps_position_on_partial():
    """★계약 — 부분체결이면 지우지 않고 수량만 줄이며 **source 를 유지**한다.

    지우면 KIS 잔량이 sync 케이스 A 로 재등록되며 source 가 sync_auto_unknown 이 되고,
    그 순간 보호 대상이 되어 **다시는 hard_kill 이 손대지 못한다.**
    """
    src = (ROOT / "bot" / "position_safety.py").read_text(encoding="utf-8")
    start = src.find("def hard_kill_check")
    assert start != -1, "hard_kill_check 소실"
    body = src[start:start + 9000]

    assert "fully_closed" in body, "전량/부분 분기 부재 — 부분체결에도 삭제된다"
    assert "if fully_closed:" in body, "삭제 조건이 fully_closed 가 아니다"
    # 삭제문이 fully_closed 블록 안에 있는지 (success 단독 삭제 부활 차단)
    del_idx = body.find("del self._positions[code]")
    fc_idx = body.find("if fully_closed:")
    assert del_idx > fc_idx > 0, "포지션 삭제가 fully_closed 분기 밖에 있다"
    # 부분체결 경로가 수량만 줄이는지
    assert '["qty"] = max(0, qty - filled)' in body, "부분체결 시 수량 감액 부재"
    print("[PASS] 부분체결 시 포지션 유지 + 수량 감액 (source 보존)")


def test_hard_kill_journals_partial_fill():
    """부분체결 손익이 장부에서 사라지지 않는다 (5/22 회계 결함과 같은 자리)."""
    src = (ROOT / "bot" / "position_safety.py").read_text(encoding="utf-8")
    start = src.find("def hard_kill_check")
    body = src[start:start + 9000]
    assert "if filled > 0:" in body, "저널 적재 조건이 실체결 기준이 아니다"
    j = body.find("_tj.log_sell")
    assert j > 0, "저널 적재 소실"
    seg = body[j:j + 260]
    assert "qty=filled" in seg, "저널 수량이 요청량 — 부분체결이 과대 기록된다"
    print("[PASS] 저널이 실체결량 기준으로 적재")


def test_pending_queue_has_same_protection():
    """★큐(장외 도달 → 다음 시초 매도 예약) 경로도 같은 결함이었다 — 같은 계약을 건다.

    구코드: success 만 보고 ①메모리 삭제 ②`del pending[code]`.
    부분체결이면 잔량이 **메모리·큐 양쪽에서** 사라져 방어선 밖으로 나간다.
    큐는 "장외 도달분을 다음 시초에 반드시 판다"는 예약 안전망인데 그것까지 없어졌다.
    """
    src = (ROOT / "bot" / "position_safety.py").read_text(encoding="utf-8")
    q = src.find("[QUEUE] 🚨 시초 매도 큐 처리 시작")
    assert q != -1, "큐 처리 루프 소실"
    body = src[q:q + 5200]

    assert "fully_closed" in body, "큐 경로에 전량/부분 분기 부재"

    # ★주석·다른 분기를 계수하지 않는다.
    #   `del pending[code]` 는 세 군데에 '보인다': ①보호취소 분기(정상) ②전량청산 분기(정상)
    #   ③이 fix 를 설명하는 **주석**. 순진한 find() 는 ①이나 ③을 잡아 코드가 옳은데 FAIL 을 낸다
    #   (실제로 처음 작성 때 그렇게 틀렸다 — [F-89]/[F-100] 자기참조 계수와 같은 함정).
    #   → 주석을 벗기고, '보호취소' 이후 구간에서만 판정한다.
    code_only = "\n".join(ln.split("#", 1)[0] for ln in body.splitlines())
    after_protect = code_only[code_only.find("PROTECTED_CANCEL"):]
    fc_idx = after_protect.find("if fully_closed:")
    del_idx = after_protect.find("del pending[code]")
    assert fc_idx > 0, "전량청산 분기를 못 찾음"
    assert del_idx > fc_idx, "큐 제거가 fully_closed 분기 밖 — 부분체결에도 큐에서 사라진다"
    assert "pending[code] = info" in body, "부분체결 잔량을 큐에 되돌려 놓지 않는다"
    # 부분체결이 '실패'로 이중 기록되지 않게 조건이 명시적이어야 한다
    assert "elif not success:" in body, (
        "실패 분기가 else 면 부분체결(success=True)이 여기로 떨어져 FAILED 로 이중 기록된다"
    )
    print("[PASS] 큐 경로도 부분체결 시 메모리·큐 양쪽 유지 + 실패 분기 배타")


def test_market_sell_path_uses_reconcile_helper():
    """정본 재사용 확인 — 시장가 경로가 _reconcile_fill 계열을 탄다."""
    src = (ROOT / "bot" / "kis_trader.py").read_text(encoding="utf-8")
    start = src.find("def sell_market")
    body = src[start:start + 3600]
    assert "_holding_qty_for_reconcile" in body, "주문 前 기준선 미취득"
    assert "_actual_fill_qty" in body, "체결 대조 정본 미사용(로컬 복제 금지)"
    print("[PASS] 시장가 매도가 체결 대조 정본 경유")


# ── ★음성 대조 — 루프가 가설이 아니라 실재였음을 증명 ─────────────────────
#   8/7·8/9 에 음성 대조를 HEAD 에 묶는 실수를 이틀 연속 했다. 기준이 움직이면 대조가 아니다.
#   여기서는 **현재 소스에 역치환**해 구코드를 합성하거나, 실재 상수·실재 함수로 고리를 잇는다.
def test_negative_control_old_delete_completes_the_loop():
    """구동작(성공만 보고 삭제)이면 고리가 끝까지 이어지는지 — 실재 부품으로 확인.

    고리: 삭제 → KIS 잔량 → sync 케이스 A 재등록(source=SYNC_AUTO_SOURCE) → 보호
    각 고리를 **실제 코드/상수**로 확인한다. 하나라도 끊기면 이 fix 는 과교정이므로
    그때는 이 테스트가 실패해 알려준다.
    """
    ps = (ROOT / "bot" / "position_safety.py").read_text(encoding="utf-8")

    # 고리 ②→③ : sync 케이스 A 가 SYNC_AUTO_SOURCE 로 재등록한다
    a = ps.find("케이스 A")
    assert a != -1, "sync 케이스 A 소실 — 고리 전제 변경"
    seg_a = ps[a:a + 3000]
    assert "SAJANG.SYNC_AUTO_SOURCE" in seg_a, "케이스 A 가 sync_auto 라벨을 안 붙인다(고리 끊김)"

    # 고리 ③→④ : 그 라벨이 보호로 판정된다 (실함수)
    assert _is_protected({"source": SAJANG.SYNC_AUTO_SOURCE}) is True

    # 고리 ④ : 보호면 hard_kill 이 건너뛴다
    hk = ps.find("def hard_kill_check")
    body = ps[hk:hk + 9000]
    assert "_is_protected(" in body, "hard_kill 이 보호 판정을 안 본다면 고리가 성립하지 않는다"

    print("[PASS] 음성 대조: 삭제→sync재등록→보호→hard_kill제외 고리가 실재 부품으로 이어짐")


def test_negative_control_pre_fix_sell_market_hides_partial():
    """구 `sell_market`(반환에 체결량 없음)에서는 부분체결을 알 방법이 없었다.

    현재 소스를 역치환해 구반환을 합성하고, 호출부가 쓸 정보가 실제로 없음을 보인다.
    """
    src = (ROOT / "bot" / "kis_trader.py").read_text(encoding="utf-8")
    now = ('return {"success": success, "message": msg,\n'
           '                "filled_qty": actual, "requested_qty": qty, "partial": actual < qty}')
    assert src.count(now) == 1, "역치환 앵커가 유일하지 않다 — 테스트 갱신 필요"
    before = 'return {"success": success, "message": msg}'
    old_ret = src.replace(now, before)
    # 구반환에는 체결 정보가 없다 = 호출부(hard_kill)는 success 만 볼 수밖에 없었다
    seg = old_ret[old_ret.find("def sell_market"):][:3600]
    assert '"filled_qty"' not in seg.split("return {")[-1], "역치환 실패"
    print("[PASS] 음성 대조: 구 sell_market 반환에 체결량 없음(호출부가 알 수 없었다)")


if __name__ == "__main__":
    print("=" * 72)
    print("★ [F-157] hard_kill 부분체결 자기무장해제 차단 ★")
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
