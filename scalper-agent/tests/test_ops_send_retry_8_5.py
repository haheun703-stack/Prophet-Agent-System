# -*- coding: utf-8 -*-
"""[F-89] daily_ops_check 발송 재시도 + 미발송 누적 병기 검증 (8/5).

배경: 7/31 배선한 08:30 무인점검의 **첫 평일 실행(8/3)이 발송 실패**했다
(`RemoteDisconnected`). 본문·판정은 정상이었고 A1~A6 전부 ✅였는데 사장님께만
안 갔고, 그 로그를 읽을 세션이 그날 없었다 = 무인화 전제가 마지막 1마일에서 깨짐.

이 테스트가 지키는 것 — 누적 카운트를 **자기 로그 역산**으로 구하는 설계의 함정 2개:
  ① 자기참조 오염 : 병기문이 다음 실행에 '실패'로 잡히면 카운트가 영구 누적된다
  ② 중간 로그 오집계 : 재시도 중간 로그가 최종 실패 마커와 겹치면 1회 실패가 N회가 된다

실행:
    python3.11 -X utf8 tests/test_ops_send_retry_8_5.py
"""

import sys
import tempfile
import types
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent      # scalper-agent/
for p in (BASE_DIR, BASE_DIR / "tools"):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

try:
    sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]
except Exception:
    pass

import daily_ops_check as ops  # noqa: E402

TMP = Path(tempfile.mkdtemp(prefix="f89_"))
_fails: list = []

HDR = "[ops] === {d} 08:30:01 아침 점검 (기준 거래일 {d}) ==="
SENT = f"[ops] === 2026-08-04 08:30:03 {ops.MARK_SENT} ==="
FAIL = f"[ops] {ops.MARK_FAILED} — 사장님 미수신 가능(토큰/네트워크 확인·3회 재시도 소진)"


def check(name, got, want):
    ok = got == want
    print(f"  {'✅' if ok else '🚨'} {name}" + ("" if ok else f" — 실측 {got!r} / 기대 {want!r}"))
    if not ok:
        _fails.append(name)


def log(name, *lines) -> Path:
    p = TMP / name
    p.write_text("\n".join(lines), encoding="utf-8")
    return p


def test_pending_unsent():
    print("■ pending_unsent — 로그 역산")
    check("로그 부재 → []", ops.pending_unsent(TMP / "__none__.log"), [])
    check("빈 로그 → []", ops.pending_unsent(log("empty.log", "")), [])
    check("성공만 → []", ops.pending_unsent(log("ok.log", HDR.format(d="2026-08-04"), SENT)), [])
    check("실패 1회", ops.pending_unsent(log("f1.log", HDR.format(d="2026-08-03"), FAIL)),
          ["2026-08-03"])
    check("실패→성공 = 리셋",
          ops.pending_unsent(log("fo.log", HDR.format(d="2026-08-03"), FAIL,
                                 HDR.format(d="2026-08-04"), SENT)), [])
    check("연속 3회 실패",
          ops.pending_unsent(log("f3.log", HDR.format(d="2026-08-03"), FAIL,
                                 HDR.format(d="2026-08-04"), FAIL,
                                 HDR.format(d="2026-08-05"), FAIL)),
          ["2026-08-03", "2026-08-04", "2026-08-05"])
    check("성공 이후분만 집계",
          ops.pending_unsent(log("of.log", HDR.format(d="2026-08-03"), SENT,
                                 HDR.format(d="2026-08-04"), FAIL,
                                 HDR.format(d="2026-08-05"), FAIL)),
          ["2026-08-04", "2026-08-05"])


def test_self_reference():
    """★함정 ① — 병기문 자신이 다음 실행에 실패로 잡히면 안 된다."""
    print("■ 자기참조 오염 방지")
    notice = ops.pending_notice(["2026-08-03", "2026-08-04"])
    check("병기문에 실패 마커 미포함", ops.MARK_FAILED in notice, False)
    check("병기문에 성공 마커 미포함", ops.MARK_SENT in notice, False)
    check("병기문 포함 성공 로그 → []",
          ops.pending_unsent(log("self.log", HDR.format(d="2026-08-05"), notice, SENT)), [])
    n5 = ops.pending_notice([f"2026-08-0{i}" for i in range(1, 6)])
    check("5건 축약 표기", ("지난 5회" in n5) and ("외 2건" in n5), True)


def test_retry_log_not_counted():
    """★함정 ② — 재시도 중간 로그가 최종 실패로 집계되면 안 된다."""
    print("■ 재시도 중간 로그 오집계 방지")
    mid = f"[ops] 발송 재시도 대기 2s (1/{ops.SEND_ATTEMPTS} 실패)"
    check("중간 로그에 최종 마커 미포함", ops.MARK_FAILED in mid, False)
    check("재시도 2회 후 성공 → []",
          ops.pending_unsent(log("retry.log", HDR.format(d="2026-08-05"), mid, mid, SENT)), [])


class _FakeTG:
    """send_report 대역 — bool 또는 Exception 시퀀스를 소비한다."""

    def __init__(self, results):
        self.results = list(results)
        self.calls = 0
        self.enabled = True

    def send_report(self, msg):
        self.calls += 1
        r = self.results.pop(0)
        if isinstance(r, Exception):
            raise r
        return r


def _run_send(results):
    fake = _FakeTG(results)
    mod = types.ModuleType("output.telegram_alert")
    mod.TelegramAlert = lambda cfg: fake
    sys.modules["output.telegram_alert"] = mod
    if "yaml" not in sys.modules:          # 노트북엔 PyYAML 미설치(VPS venv엔 있음)
        y = types.ModuleType("yaml")
        y.safe_load = lambda *a, **k: {}
        sys.modules["yaml"] = y
    orig_sleep = ops.time.sleep
    ops.time.sleep = lambda s: None       # 백오프 대기 제거
    try:
        rc = ops._send_telegram("테스트 본문")
    finally:
        ops.time.sleep = orig_sleep
        sys.modules.pop("output.telegram_alert", None)
    return rc, fake.calls


def test_retry_loop():
    print("■ 재시도 루프")
    check("1회차 성공", _run_send([True]), (0, 1))
    check("2회차 성공", _run_send([False, True]), (0, 2))
    check("3회차 성공", _run_send([False, False, True]), (0, 3))
    check("3회 전부 실패 → exit 1", _run_send([False, False, False]), (1, 3))
    check("★8/3 재현: 예외 후 재시도 성공",
          _run_send([ConnectionError("RemoteDisconnected"), True]), (0, 2))
    check("예외 3회 → 죽지 않고 exit 1",
          _run_send([ConnectionError("a"), ConnectionError("b"), ConnectionError("c")]), (1, 3))


def main() -> int:
    test_pending_unsent()
    test_self_reference()
    test_retry_log_not_counted()
    test_retry_loop()
    print("\n" + "=" * 58)
    if _fails:
        print(f"🚨 실패 {len(_fails)}건: {', '.join(_fails)}")
        return 1
    print("✅ 전건 PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
