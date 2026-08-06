# -*- coding: utf-8 -*-
"""[F-53] 순위 스냅샷 수집기 상태 분기 검증 (8/6).

배경: `saved[kind] = 0`이 **세 가지 다른 상태**를 한 값으로 뭉개고 있었다 —
장외(정상)·예외 실패(이상)·빈 응답(조용한 실패 후보). 그 결과 6종이 전멸해도
`{"status": "ok"}`가 반환돼 nightly 로그에 성공으로 남았다.

이 테스트가 지키는 것: **장외 skip은 실패가 아니고, 전멸은 ok가 아니다.**

실행:
    python -X utf8 tests/test_ranking_status_8_6.py
"""

import sys
import types
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent      # scalper-agent/
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

try:
    sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]
except Exception:
    pass

from data import ranking_snapshot_collector as rs  # noqa: E402

_fails: list = []
_total = 0


def check(name, got, want):
    global _total
    _total += 1
    ok = got == want
    print(f"  {'✅' if ok else '🚨'} {name}" + ("" if ok else f" — 실측 {got!r} / 기대 {want!r}"))
    if not ok:
        _fails.append(name)


class _FakeTrader:
    """KIS 대역 — 종류별로 rows / 예외 / 장외를 지정한다."""

    def __init__(self, rows=3, raise_kinds=(), night="available"):
        self.rows, self.raise_kinds, self.night = rows, set(raise_kinds), night

    def _mk(self, kind):
        if kind in self.raise_kinds:
            raise RuntimeError(f"{kind} 통신 실패")
        return [{"code": f"00000{i}"} for i in range(self.rows)]

    def fetch_ranking_fluctuation(self, top_n=50):
        return self._mk("fluctuation")

    def fetch_ranking_volume(self, top_n=50):
        return self._mk("volume")

    def fetch_ranking_strength(self, top_n=50):
        return self._mk("strength")

    def fetch_uplowprice(self, price_cls="0", div_cls="0"):
        return self._mk("uplowprice")

    def fetch_foreign_inst_total(self, target="1", sort_cls="0"):
        return self._mk("foreign_inst")

    def fetch_night_futures_price(self):
        if self.night == "raise":
            raise RuntimeError("night 통신 실패")
        if self.night == "available":
            return {"available": True, "price": 123}
        return {"available": False, "reason": "장외시간"}


def _run(trader, appended=None, empty_kinds=()):
    """수집기를 대역으로 돌린다 — 디스크 쓰기(_append_snapshot)는 대역으로 차단.

    empty_kinds: 이 종류만 0행을 반환(예외 없이 빈 응답 = 조용한 실패 후보).
    """
    orig = (rs._append_snapshot, rs._ensure_dirs, rs.time.sleep, rs.is_trading_day)
    fake_mod = types.ModuleType("bot.kis_trader")
    fake_mod.KISTrader = lambda: trader
    saved_mod = sys.modules.get("bot.kis_trader")
    sys.modules["bot.kis_trader"] = fake_mod
    try:
        # 반환 행수만 흉내 — CSV 파일은 건드리지 않는다(read-only 테스트).
        rs._append_snapshot = lambda kind, rows, today, dedup_extra=None: (
            0 if kind in empty_kinds
            else (appended if appended is not None else len(rows)))
        rs._ensure_dirs = lambda: None
        rs.time.sleep = lambda s: None
        rs.is_trading_day = lambda d=None: True
        return rs.collect_ranking_snapshots()
    finally:
        (rs._append_snapshot, rs._ensure_dirs, rs.time.sleep, rs.is_trading_day) = orig
        if saved_mod is not None:
            sys.modules["bot.kis_trader"] = saved_mod
        else:
            sys.modules.pop("bot.kis_trader", None)


def test_all_ok():
    print("■ 전 종목 정상")
    r = _run(_FakeTrader())
    check("status ok", r["status"], "ok")
    check("failed 없음", r["failed"], [])
    check("night_futures ok", r["state"]["night_futures"], "ok")


def test_night_offhours_is_not_failure():
    """★핵심 — 장외 skip을 실패로 세면 안 된다(오늘의 night_futures 상시 미생산 상황)."""
    print("■ 야간선물 장외 = 정상")
    r = _run(_FakeTrader(night="offhours"))
    check("★장외인데 status ok 유지", r["status"], "ok")
    check("state=skipped (failed 아님)", r["state"]["night_futures"], "skipped")
    check("사유 보존", r["state"]["night_futures_reason"], "장외시간")
    check("failed 목록에 없음", "night_futures" in r["failed"], False)


def test_partial_failure():
    print("■ 일부 실패")
    r = _run(_FakeTrader(raise_kinds=["volume"]))
    check("status partial", r["status"], "partial")
    check("failed에 volume", r["failed"], ["volume"])
    check("나머지는 ok", r["state"]["strength"], "ok")


def test_total_failure_is_not_ok():
    """★핵심 — 전멸이 ok로 보고되던 것이 [F-53]의 본체다."""
    print("■ 전멸")
    r = _run(_FakeTrader(raise_kinds=["fluctuation", "volume", "strength",
                                      "uplowprice", "foreign_inst"], night="raise"))
    check("★status failed (ok 아님)", r["status"], "failed")
    check("failed 6종", len(r["failed"]), 6)


def test_empty_response_is_flagged():
    """예외는 없는데 0행 — 조용한 실패 후보를 ok로 세지 않는다.

    ★8/6 테스트 자체 정정: 최초 픽스처는 **전 종목**을 0행으로 만들어 놓고
    `partial`을 기대했다. 전부 비면 그건 전멸이라 `failed`가 맞다 —
    **코드가 옳고 기대가 틀렸다.** 한 종류만 비우도록 픽스처를 고쳤다.
    """
    print("■ 빈 응답(1종)")
    r = _run(_FakeTrader(), empty_kinds=("volume",))
    check("status partial", r["status"], "partial")
    check("empty로 분류", r["state"]["volume"], "empty")
    check("예외가 아니므로 failed 아님", r["failed"], [])
    check("나머지는 ok", r["state"]["strength"], "ok")


def test_all_empty_is_total_failure():
    """전 종목이 빈 응답 = 하나도 못 담음 = 전멸(ok도 partial도 아니다)."""
    print("■ 빈 응답(전종목)")
    r = _run(_FakeTrader(), appended=0)
    check("★status failed", r["status"], "failed")
    check("failed 목록은 비어 있음(예외가 아니므로)", r["failed"], [])
    check("empty 6종", len(r["empty"]), 6)


def test_holiday_skip_unchanged():
    print("■ 휴장일 가드 불변")
    orig = rs.is_trading_day
    try:
        rs.is_trading_day = lambda d=None: False
        r = rs.collect_ranking_snapshots()
    finally:
        rs.is_trading_day = orig
    check("skip_holiday 유지", r["status"], "skip_holiday")


def main() -> int:
    import inspect
    tests = [fn for name, fn in sorted(globals().items())
             if name.startswith("test_") and inspect.isfunction(fn)]
    for t in tests:
        t()
    print("\n" + "=" * 58)
    if _fails:
        print(f"🚨 {_total}건 중 실패 {len(_fails)}건: {', '.join(_fails)}")
        return 1
    print(f"✅ 전건 PASS ({_total}/{_total} · 발견 {len(tests)}함수)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
