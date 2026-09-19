# -*- coding: utf-8 -*-
"""[F-225] 사고 알림 21곳이 만들어지고 버려지던 것 (9/19 신설).

`await asyncio.to_thread(alert_fn, msg)` 인데 `alert_fn` 이 **코루틴 함수**라
(`telegram_bot` 의 `async def _send_alert`) 스레드에서 코루틴 **객체만 만들고 끝난다**.
await 되지 않고, 예외도 없고, 로그는 성공으로 남는다.

9/19 재현: 현행 패턴 **발송 0건 · 예외 0건**.
버려지던 것 — `EOD 청산 실패 … 수동 청산 필요!` · `B3 모니터 크래시` ·
`모닝스캔 실패` · `이브닝분석 실패` 등 **사장님께 가야 할 사고 알림**.
"""
import asyncio
import inspect
import re
import warnings
from pathlib import Path

from bot.trading_coo import emit_alert

warnings.filterwarnings("ignore", message="coroutine .* was never awaited")

_SRC = (Path(__file__).resolve().parent.parent / "bot" / "trading_coo.py")


def _run(coro):
    return asyncio.run(coro)


# ------------------------------------------------------------------
#  동작 — 실제로 보내는가
# ------------------------------------------------------------------

def test_async_alert_fn_is_actually_awaited():
    """★핵심 — 코루틴 함수를 주면 **실제로 실행**돼야 한다."""
    sent = []

    async def _send_alert(text):
        sent.append(text)

    ok = _run(emit_alert(_send_alert, "🚨 EOD 청산 실패 — 수동 청산 필요!"))
    assert ok is True
    assert sent == ["🚨 EOD 청산 실패 — 수동 청산 필요!"]


def test_old_pattern_would_have_dropped_it():
    """음성대조 — 옛 패턴은 0건 발송이고 예외도 안 난다(그래서 12주간 아무도 몰랐다)."""
    sent = []

    async def _send_alert(text):
        sent.append(text)

    async def _old():
        await asyncio.to_thread(_send_alert, "버려지는 알림")

    _run(_old())
    assert sent == [], "옛 패턴이 발송에 성공하면 이 사고의 전제가 틀린 것"


def test_sync_alert_fn_still_works():
    sent = []
    ok = _run(emit_alert(lambda t: sent.append(t), "동기 함수"))
    assert ok is True
    assert sent == ["동기 함수"]


def test_awaitable_returning_sync_wrapper_is_awaited():
    """partial·bound wrapper 로 감싸이면 iscoroutinefunction 이 False 인데
    반환값이 awaitable 일 수 있다 — 그 경우에도 버리지 않는다."""
    sent = []

    async def _inner(text):
        sent.append(text)

    def _wrapper(text):          # sync 처럼 보이지만 코루틴을 돌려준다
        return _inner(text)

    assert not asyncio.iscoroutinefunction(_wrapper)
    ok = _run(emit_alert(_wrapper, "감싸진 코루틴"))
    assert ok is True
    assert sent == ["감싸진 코루틴"]


def test_none_alert_fn_is_safe():
    assert _run(emit_alert(None, "x")) is False


def test_exception_is_swallowed_not_propagated():
    """알림 실패가 잡(job)을 죽이면 안 된다."""
    async def _boom(text):
        raise RuntimeError("telegram down")

    assert _run(emit_alert(_boom, "x")) is False


# ------------------------------------------------------------------
#  소스 — 전환이 **전부** 됐는가 / 자기 재귀는 없는가
# ------------------------------------------------------------------

def _helper_body() -> str:
    src = _SRC.read_text(encoding="utf-8")
    m = re.search(r"^async def emit_alert.*?(?=^class )", src, re.S | re.M)
    assert m, "emit_alert 헬퍼를 찾지 못했다"
    return m.group(0)


def test_no_call_site_left_on_the_broken_pattern():
    """★헬퍼 **밖**에 `to_thread(alert_fn, ...)` 이 남아 있으면 그 알림은 여전히 버려진다."""
    src = _SRC.read_text(encoding="utf-8")
    outside = src.replace(_helper_body(), "")
    leftovers = re.findall(r"asyncio\.to_thread\(alert_fn", outside)
    assert leftovers == [], f"전환 누락 {len(leftovers)}곳 — 한 곳이라도 남으면 조용히 버려진다"


def test_all_sites_use_the_helper():
    src = _SRC.read_text(encoding="utf-8")
    assert len(re.findall(r"await emit_alert\(alert_fn", src)) >= 21


def test_helper_does_not_call_itself():
    """★9/19에 실제로 만들었던 버그 — 정규식이 헬퍼 내부까지 치환해 **무한 재귀**가 됐다.

    치환을 헬퍼 삽입 **앞**에 두는 순서로 막았고, 이 테스트가 그 순서를 고정한다.
    """
    body = _helper_body()
    after_def = body.split("\n", 1)[1]        # 정의 줄 제외
    assert "emit_alert(" not in after_def, "헬퍼가 자기 자신을 호출한다(무한 재귀)"
    assert "asyncio.to_thread(alert_fn, text)" in body, \
        "헬퍼 내부의 sync 경로까지 치환되면 안 된다"


def test_helper_is_async_and_takes_two_args():
    assert inspect.iscoroutinefunction(emit_alert)
    params = list(inspect.signature(emit_alert).parameters)
    assert params[:2] == ["alert_fn", "text"]
