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
    global _total
    _total += 1
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
    """★함정 ① — 병기문 자신이 다음 실행에 실패로 잡히면 안 된다.

    ★8/5 Tier1 정정: 최초 픽스처는 마지막 줄이 SENT였다. `pending_unsent`는
    MARK_SENT를 만나면 clear()하므로 **병기문에 실패 마커를 넣어도 결과가 항상 []**였다
    = 이 assert는 구조적으로 실패할 수 없었다(가짜 통과). 오염이 드러나려면
    검사 대상 줄이 **마지막 성공 마커 뒤**에 있어야 한다.
    """
    print("■ 자기참조 오염 방지")
    notice = ops.pending_notice(["2026-08-03", "2026-08-04"])
    check("병기문에 실패 마커 미포함", ops.MARK_FAILED in notice, False)
    check("병기문에 성공 마커 미포함", ops.MARK_SENT in notice, False)
    # SENT 뒤에 병기문이 오는 배치 — 오염되면 ['2026-08-05']로 실패한다
    check("★병기문이 성공 뒤에 와도 오집계 없음",
          ops.pending_unsent(log("self.log",
                                 HDR.format(d="2026-08-04"), SENT,
                                 HDR.format(d="2026-08-05"), notice, SENT)), [])
    # 음성 대조 — 실패 마커가 실제로 들어가면 반드시 잡혀야 한다(검출력 증명)
    poisoned = f"⚠️ 지난 2회 미수신 {ops.MARK_FAILED} 흉내"
    check("음성 대조: 오염 주입 시 검출됨",
          ops.pending_unsent(log("poison.log",
                                 HDR.format(d="2026-08-04"), SENT,
                                 HDR.format(d="2026-08-05"), poisoned)), ["2026-08-05"])
    n5 = ops.pending_notice([f"2026-08-0{i}" for i in range(1, 6)])
    check("5건 축약 표기", ("지난 5회" in n5) and ("외 2건" in n5), True)


def test_retry_log_not_counted():
    """★함정 ② — 재시도 중간 로그가 최종 실패로 집계되면 안 된다.

    ★8/5 Tier1 정정: 최초 픽스처도 마지막이 SENT라 항상 []였다. 진짜 검증은
    **1회 실패가 N회로 부풀지 않는가**이므로, 중간 로그 2줄 + 최종 실패 1줄을
    두고 결과가 정확히 1건인지 본다.
    """
    print("■ 재시도 중간 로그 오집계 방지")
    mid = f"[ops] 발송 재시도 대기 2s (1/{ops.SEND_ATTEMPTS} 실패)"
    check("중간 로그에 최종 마커 미포함", ops.MARK_FAILED in mid, False)
    check("★중간 2회 + 최종 실패 1회 → 정확히 1건",
          ops.pending_unsent(log("retry.log",
                                 HDR.format(d="2026-08-04"), SENT,
                                 HDR.format(d="2026-08-05"), mid, mid, FAIL)),
          ["2026-08-05"])
    check("재시도 후 성공 → []",
          ops.pending_unsent(log("retry_ok.log",
                                 HDR.format(d="2026-08-04"), SENT,
                                 HDR.format(d="2026-08-05"), mid, mid, SENT)), [])


def test_unmarked_block():
    """★8/5 Tier1 신설 — 마커 없이 끝난 블록(프로세스 사망) = 미발송으로 세야 한다.

    ImportError/OOM/SIGKILL이면 성공·실패 어느 마커도 안 남는데 사장님은 못 받았다.
    이 상태 전이가 빠져 있어서 '미발송인데 미발송으로 안 세지는' 통로였다.
    """
    print("■ 마커 없이 끝난 블록 / dry-run 제외")
    check("헤더만 있고 마커 없음 → 미발송 계수",
          ops.pending_unsent(log("dead.log",
                                 HDR.format(d="2026-08-04"), SENT,
                                 HDR.format(d="2026-08-05"), "본문만 있고 끝")),
          ["2026-08-05"])
    check("죽은 블록 2개 연속 → 2건",
          ops.pending_unsent(log("dead2.log",
                                 HDR.format(d="2026-08-04"), "본문",
                                 HDR.format(d="2026-08-05"), "본문")),
          ["2026-08-04", "2026-08-05"])
    check("dry-run 블록은 미발송에서 제외",
          ops.pending_unsent(log("dry.log",
                                 HDR.format(d="2026-08-05"),
                                 f"{ops.DRY_RUN_MARK} [dry-run] 발송 생략")), [])
    check("죽은 블록 뒤 성공이 오면 리셋",
          ops.pending_unsent(log("dead_ok.log",
                                 HDR.format(d="2026-08-04"), "본문",
                                 HDR.format(d="2026-08-05"), SENT)), [])


def test_legacy_marker_migration():
    """★8/5 Tier1 신설 — 마커 문자열 교체 마이그레이션.

    마커를 `[ops][SEND_OK/FAIL]`로 바꾸면서 읽기 호환을 안 넣으면, 7/31~8/5에
    옛 문구로 기록된 **이미 발송된 블록들이 '마커 없는 죽은 블록'으로 잡혀**
    다음 실행에 "지난 4회 미수신"이라는 거짓 병기가 나간다. 없는 사고를 사장님께
    보고하는 것이므로 미수신 누락만큼이나 나쁘다.

    동시에 옛 문구는 평범한 한국어라 A3 본문(텔레그램 발송 성패를 보고한다)과
    충돌할 수 있어, **`[ops] ` 접두가 있을 때만** 마커로 인정한다.
    """
    print("■ 옛 마커 호환 / 본문 오인 방지")
    legacy_sent = "[ops] === 2026-08-04 08:30:03 텔레그램 발송 완료 ==="
    legacy_fail = "[ops] 텔레그램 발송 실패 — 사장님 미수신 가능(토큰/네트워크 확인)"
    check("옛 성공 마커 인식 → []",
          ops.pending_unsent(log("lg1.log", HDR.format(d="2026-08-04"), legacy_sent)), [])
    check("옛 실패 마커 인식 → 1건",
          ops.pending_unsent(log("lg2.log", HDR.format(d="2026-08-03"), legacy_fail)),
          ["2026-08-03"])
    check("옛 실패 뒤 옛 성공 = 리셋",
          ops.pending_unsent(log("lg3.log",
                                 HDR.format(d="2026-08-03"), legacy_fail,
                                 HDR.format(d="2026-08-04"), legacy_sent)), [])
    # ★본문 오인 방지 — `[ops] ` 접두가 없으면 마커가 아니다
    body = "✅ A3 20:10 발송 — 텔레그램 발송 완료 (스탬프 2026-08-05)"
    check("★A3 본문의 옛 문구는 마커로 인정 안 함",
          ops.pending_unsent(log("lg4.log",
                                 HDR.format(d="2026-08-04"), legacy_sent,
                                 HDR.format(d="2026-08-05"), body)), ["2026-08-05"])


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


_total = 0


def main() -> int:
    test_pending_unsent()
    test_self_reference()
    test_retry_log_not_counted()
    test_unmarked_block()
    test_legacy_marker_migration()
    test_retry_loop()
    print("\n" + "=" * 58)
    # ★검사 건수를 세서 출력한다 — 8/5에 업무일지가 "22/22"라 적었는데 실제 호출은
    #   19건이었다(scratchpad 버전 숫자를 커밋본에 그대로 옮긴 오류). 사람이 세면 틀린다.
    if _fails:
        print(f"🚨 {_total}건 중 실패 {len(_fails)}건: {', '.join(_fails)}")
        return 1
    print(f"✅ 전건 PASS ({_total}/{_total})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
