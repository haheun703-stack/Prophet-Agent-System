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
    # ★8/6 — 신규 마커도 접두 규칙: 본문 echo(줄머리가 [ops]가 아님)는 마커가 아니다.
    #   A5/A9가 타 로그 원문을 본문에 실어 나르므로 이 통로가 열려 있으면
    #   진짜 실패가 가짜 성공 echo에 지워질 수 있다(8/5 리네임과 같은 계열).
    check("★본문 echo의 신마커는 무시(접두 규칙)",
          ops.pending_unsent(log("lg5.log",
                                 HDR.format(d="2026-08-03"), FAIL,
                                 f"✅ A5 대조 — 원문 echo에 {ops.MARK_SENT} 포함")),
          ["2026-08-03"])


def _run_main_flow(seed_lines, argv_date="2026-08-06"):
    """main()을 cron 배선 그대로 통과시킨다 — stdout이 자기 로그로 즉시(`-u`) 흘러드는 조건.

    8/6 실전 재현 장치: 헤더 print가 pending_unsent의 읽기보다 **먼저 파일에 닿는**
    환경을 만들어야 순서 버그가 드러난다. 단위 픽스처만으론 이 층이 안 보였다
    (8/5 테스트 22건 전건 PASS였는데 첫 실전에서 거짓 병기가 나간 이유).
    """
    tmp = TMP / f"mainflow_{len(_fails)}_{_total}"
    tmp.mkdir(parents=True, exist_ok=True)
    selflog = tmp / ops.SELF_LOG
    selflog.write_text("\n".join(seed_lines) + "\n", encoding="utf-8")
    sent_msgs = []
    orig = (ops.LOGS_DIR, ops.run_checks, ops.build_message, ops._send_telegram,
            sys.argv, sys.stdout)
    fh = None
    try:
        # ★패치·open 모두 try 안 — open 실패 시에도 finally가 전부 복원한다(Tier1 L-1)
        ops.LOGS_DIR = tmp
        ops.run_checks = lambda ref: ([], "", [])
        ops.build_message = lambda ref, rows, score, dl: "본문"
        ops._send_telegram = lambda m: (sent_msgs.append(m), 0)[1]
        sys.argv = ["daily_ops_check.py", "--date", argv_date]
        fh = open(selflog, "a", encoding="utf-8", buffering=1)  # cron `>>` + `-u` 라인 플러시
        sys.stdout = fh
        rc = ops.main()
    finally:
        sys.stdout = orig[5]
        if fh is not None:
            fh.close()
        (ops.LOGS_DIR, ops.run_checks, ops.build_message, ops._send_telegram,
         sys.argv) = orig[:5]
    return rc, (sent_msgs[0] if sent_msgs else "")


def test_selfref_own_header():
    """★[F-89 잔여·8/6] — 자기 헤더를 '죽은 블록'으로 오집계하면 안 된다."""
    print("■ 자기 헤더 오집계 방지 (8/6 실전 재현)")
    # 8/6 실전 그대로: 성공 이력만 있는 로그 → 병기가 붙으면 안 된다
    rc, msg = _run_main_flow([HDR.format(d="2026-08-05"), SENT])
    check("★성공 이력만 → 거짓 미수신 병기 없음", "미수신" in msg, False)
    check("exit 0", rc, 0)
    # 검출력 증명(음성 대조) — 진짜 죽은 이전 블록은 순서 fix 후에도 잡혀야 한다
    rc2, msg2 = _run_main_flow([HDR.format(d="2026-08-04"), SENT,
                                HDR.format(d="2026-08-05"), "본문만 있고 끝"])
    check("★진짜 죽은 블록은 여전히 병기", "지난 1회 미수신(2026-08-05)" in msg2, True)


def test_heartbeat_marker_contract():
    """★8/6 Tier1 HIGH — 20:10 하트비트가 8/5 신마커를 알아봐야 한다.

    8/5 마커 리네임 후 notify_data_freshness._morning_ops_heartbeat만 옛 문구
    ("발송 완료")를 보고 있었다 = 성공한 날에도 매일 ⚠️, 실패해도 같은 ⚠️(구분 불능
    — '감시자를 감시하는' 계층이 조용히 죽은 상태). 마커 상수 단일진실은
    daily_ops_check 파일 경계 밖 소비자까지 적용돼야 한다.
    """
    print("■ 20:10 하트비트 ↔ 마커 단일진실")
    import notify_data_freshness as ndf
    d = "2026-08-06"
    hbdir = TMP / "hb"
    (hbdir / "logs").mkdir(parents=True, exist_ok=True)
    logp = hbdir / "logs" / "daily_ops_check.log"
    orig_base = ndf.BASE_DIR
    ndf.BASE_DIR = hbdir / "scalper-agent"    # 하트비트는 BASE_DIR.parent/logs를 읽는다
    try:
        logp.write_text(HDR.format(d=d) + "\n"
                        + f"[ops] === {d} 08:30:03 {ops.MARK_SENT} ===\n", encoding="utf-8")
        check("★신마커 성공 인식", ndf._morning_ops_heartbeat(d),
              "✅ 아침점검(08:30) 실행·발송")
        logp.write_text(HDR.format(d=d) + "\n"
                        + f"[ops] === {d} 08:30:03 텔레그램 발송 완료 ===\n", encoding="utf-8")
        check("옛 마커도 여전히 성공 인식", ndf._morning_ops_heartbeat(d),
              "✅ 아침점검(08:30) 실행·발송")
        logp.write_text(HDR.format(d=d) + "\n[ops] " + ops.MARK_FAILED + " — 재시도 소진\n",
                        encoding="utf-8")
        check("실패 마커 → ⚠️ 발송 미확인", ndf._morning_ops_heartbeat(d),
              "⚠️ 아침점검(08:30) 실행됐으나 발송 미확인")
        logp.write_text(HDR.format(d="2026-08-05") + "\n", encoding="utf-8")
        check("오늘 스탬프 없음 → 🚨 미실행", ndf._morning_ops_heartbeat(d),
              "🚨 아침점검(08:30) 미실행 — cron/프로세스 확인")
    finally:
        ndf.BASE_DIR = orig_base


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
    # ★8/6 Tier1 M-1 — 스텁은 '복원'한다. pop만 하면 원래 로드돼 있던 진짜 모듈이
    #   축출되고, yaml 스텁은 영구 잔류해 이후 같은 프로세스의 모든 import yaml이
    #   조용히 빈 config를 받는다(지금 안전한 건 실행 순서 우연뿐이었다).
    saved_tg = sys.modules.get("output.telegram_alert")
    yaml_injected = "yaml" not in sys.modules  # 노트북엔 PyYAML 미설치(VPS venv엔 있음)
    orig_sleep = ops.time.sleep
    try:
        sys.modules["output.telegram_alert"] = mod
        if yaml_injected:
            y = types.ModuleType("yaml")
            y.safe_load = lambda *a, **k: {}
            sys.modules["yaml"] = y
        ops.time.sleep = lambda s: None       # 백오프 대기 제거
        rc = ops._send_telegram("테스트 본문")
    finally:
        ops.time.sleep = orig_sleep
        if saved_tg is not None:
            sys.modules["output.telegram_alert"] = saved_tg
        else:
            sys.modules.pop("output.telegram_alert", None)
        if yaml_injected:
            sys.modules.pop("yaml", None)
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
    test_selfref_own_header()
    test_heartbeat_marker_contract()
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
