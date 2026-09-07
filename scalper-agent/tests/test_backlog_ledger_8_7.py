# -*- coding: utf-8 -*-
"""[LEDGER-1] 개선점 장부 자기모순 검사 검증 (8/7).

배경: 8/5 소진 커밋(70148cb)이 완료 **표만 덧붙이고** 원 불릿의 취소선을 긋지 않아
같은 ID가 §4 안에서 '열림'과 '완료'로 동시에 존재했다(9건). 8/7에 단타봇이
[F-100]을 미소진으로 오인해 **이미 고친 것을 다시 고칠 뻔했다.**

이 테스트가 지키는 것:
  ① 모순은 반드시 잡힌다        ② 정합 상태는 조용하다
  ③ 부분 소진 표기는 위반 아님   ④ 인라인 코드 인용은 계수하지 않는다(자기참조)
  ⑤ ★음성 대조 — **git HEAD의 구 장부**에 돌려 9건이 실제로 잡히는지 확인한다.
     (구 장부에서 0건이면 이 규칙은 아무것도 안 지키는 것이다)

실행:
    python -X utf8 scalper-agent/tests/test_backlog_ledger_8_7.py
"""

import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent      # 저장소 루트
if str(ROOT / "tools") not in sys.path:
    sys.path.insert(0, str(ROOT / "tools"))

try:
    sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]
except Exception:
    pass

from pre_commit_check import check_backlog_ledger  # noqa: E402

_fails: list = []
_total = 0
_TMP = Path(tempfile.mkdtemp(prefix="ledger_"))


def check(name, got, want):
    global _total
    _total += 1
    ok = got == want
    print(f"  {'✅' if ok else '🚨'} {name}" + ("" if ok else f" — 실측 {got!r} / 기대 {want!r}"))
    if not ok:
        _fails.append(name)


def doc(body: str) -> Path:
    p = _TMP / f"doc_{len(list(_TMP.iterdir()))}.md"
    p.write_text("## §4. 개선점 각주\n\n" + body, encoding="utf-8")
    return p


def ids(res):
    return sorted({r["msg"].split("]")[0].lstrip("[") for r in res})


# ══════════════════════════════════════════════════════════════════
def test_01_모순_검출():
    print("\n[1] 열림+완료 동시 존재 → 잡는다")
    p = doc("- **[F-100]** 진단 본문\n\n| ~~F-100~~ | 소진함 |\n")
    r = check_backlog_ledger(p)
    check("1건 발견", len(r), 1)
    check("ID 지목", ids(r), ["F-100"])
    check("HIGH(차단)", r[0]["severity"] if r else None, "HIGH")
    check("행 번호 지목", r[0]["line"] > 0 if r else False, True)


def test_02_정합상태는_조용():
    print("\n[2] 정합 상태 → 0건")
    check("닫힌 항목만", len(check_backlog_ledger(
        doc("- ~~**[F-100]**~~ ✅ 소진\n\n| ~~F-100~~ | 소진함 |\n"))), 0)
    check("열린 항목만", len(check_backlog_ledger(
        doc("- **[F-101]** 아직 미소진\n"))), 0)
    check("빈 §4", len(check_backlog_ledger(doc(""))), 0)


def test_03_부분소진_표기_허용():
    print("\n[3] 부분 소진은 위반 아님 (F-90이 실제로 이 상태)")
    p = doc("- **[F-90]** 룰3 위반 — 잔여 2분기\n\n| **F-90 (부분)** | ALIVE 분기만 복원 |\n")
    check("부분표기 통과", len(check_backlog_ledger(p)), 0)


def test_04_자기참조_가드():
    print("\n[4] ★인라인 코드 인용은 계수 안 함 (8/7 실제 발생)")
    p = doc("- **[F-90]** 정정 — 원래 `~~F-90~~` 로 그어져 있었으나 잔여가 있다\n")
    check("코드스팬 인용 무시", len(check_backlog_ledger(p)), 0)
    p2 = doc("- **[F-90]** 본문 `~~F-90~~` 인용\n\n| ~~F-90~~ | 진짜 완료표기 |\n")
    check("코드 밖 표기는 잡음", ids(check_backlog_ledger(p2)), ["F-90"])


# 정합 **직전** 커밋 — 이 해시의 장부에 모순 9건이 실재한다.
# ★8/7 VPS 실측에서 잡힌 내 결함: 원래 `HEAD:`로 읽었는데, 정합을 커밋하는 순간
#   HEAD가 **정합 후 장부**가 돼 음성 대조가 스스로 무너졌다(로컬 PASS → VPS FAIL).
#   기준이 움직이면 대조가 아니다. 8/6 [F-89](자기 로그를 쓰기 전에 읽어라)와 동형.
LEDGER_PRE_FIX_REV = "90a7a8d"


def test_06_긴_취소선_인식():
    """★9/7 [F-201] — 구 정규식은 `~~**[F-x]**~~`(ID 직후 곧바로 닫힘)만 잡았다.

    장부의 실제 표기는 `- ~~**[F-x]** (날짜) **제목 (등급)**~~ — ✅ 완료…` 로
    취소선이 문장 끝까지 이어진다. 실측 결과 취소선 불릿 **53건 중 19건만 인식**(64% 미탐)이었고,
    그래서 [F-117](8/6 완료)·[F-160](8/11 소진)이 각각 **32일·28일간 '열림'으로 남아** 있었다.
    8/7에 이 규칙을 만들고도 **막으려던 재발을 규칙 자신이 놓친 것**이다.
    """
    print("\n[6] ★긴 취소선(실제 장부 표기)도 완료로 인식")
    real = (
        "- **[F-117]** (8/5) **고아 문서가 KRX 복구를 유도한다 (MED)** — 원 진단 본문\n\n"
        "- ~~**[F-117]** (8/5) **고아 문서가 KRX 복구를 유도한다 (MED)**~~ — ✅ **8/6 완료**. 폐기 배너 추가\n"
    )
    check("긴 취소선 모순 검출", ids(check_backlog_ledger(doc(real))), ["F-117"])

    # ★오탐 방지 — 취소선 본문이 **다른** 항목을 참조하는 경우는 그 항목의 완료가 아니다
    ref = (
        "- **[F-90]** 아직 열림\n\n"
        "- ~~**[F-158]** (8/10) **레짐별 손절 — [F-90] 계열 (HIGH)**~~ — ✅ 8/10 완료\n"
    )
    check("본문 참조 ID는 완료로 세지 않음", len(check_backlog_ledger(doc(ref))), 0)

    # 같은 줄에 취소선 블록이 둘이면 각 블록의 첫 ID만
    two = "- **[F-1]** 열림\n\n- ~~**[F-2]** 완료~~ 그리고 ~~**[F-1]** 완료~~\n"
    check("블록별 첫 ID 각각 채택", ids(check_backlog_ledger(doc(two))), ["F-1"])


def test_07_실제_장부는_정합():
    """정본 DAILY_ROUTINE.md 자체가 모순 0이어야 한다(9/7 전체검수에서 2건 정합)."""
    print("\n[7] 실제 장부 정합 확인")
    real_doc = ROOT / "scalper-agent" / "docs" / "checklist" / "DAILY_ROUTINE.md"
    check("정본 장부 모순 0건", ids(check_backlog_ledger(real_doc)), [])


def test_05_음성대조_구장부():
    print(f"\n[5] ★음성 대조 — 정합 전 장부({LEDGER_PRE_FIX_REV})에서 실제로 잡히는가")
    rel = "scalper-agent/docs/checklist/DAILY_ROUTINE.md"
    try:
        old = subprocess.run(["git", "show", f"{LEDGER_PRE_FIX_REV}:{rel}"], cwd=ROOT,
                             capture_output=True, timeout=30).stdout.decode("utf-8")
    except Exception as e:  # noqa: BLE001
        print(f"  🚨 미검증 — git show 실패({e}). ★음성 대조가 돌지 않았다(통과 아님).")
        _fails.append("음성대조 미실행")
        return
    if not old.strip():
        print(f"  🚨 미검증 — {LEDGER_PRE_FIX_REV} 판독 실패. ★음성 대조가 돌지 않았다(통과 아님).")
        _fails.append("음성대조 미실행")
        return
    p = _TMP / "old_ledger.md"
    p.write_text(old, encoding="utf-8")
    got = ids(check_backlog_ledger(p))
    # ★테스트 자체 정정 1건(8/7) — 최초 기대에 F-88을 넣었으나 **코드가 옳고 내가 틀렸다**.
    #   F-88은 '모순'이 아니라 *완료 표기가 아예 없던* 건(조용한 소진)이라 이 규칙의
    #   사정권 밖이다.
    #
    # ★★9/7 [F-201] 기대값 갱신 9건 → **10건**(F-117 추가). 이것이 이 테스트의 최대 수확이다:
    #   8/7에 손으로 찾은 9건 목록에 **F-117이 빠져 있었다.** 그날의 정규식이
    #   `~~**[F-x]**~~`(ID 직후 곧바로 닫힘)만 잡아 긴 취소선 표기를 놓쳤고,
    #   손으로 센 목록도 같은 것을 놓쳤다. 그래서 F-117은 8/6 완료인데 원 불릿이
    #   **오늘(9/7)까지 32일간 '열림'으로 남아** 있었다 — 이 규칙이 막으려던 바로 그 재발을
    #   규칙 자신이 통과시킨 것이다. 새 판정이 구 판정의 **상위집합**임을 여기서 고정한다.
    want = ["F-100", "F-101", "F-108", "F-110", "F-111", "F-113", "F-114", "F-117", "F-90", "F-96"]
    check("구 장부에서 10건 검출(구 규칙은 9건만 봤다)", len(got), 10)
    check("8/7 목록 + F-117(그날 놓친 건)", got, want)
    check("★구 규칙이 놓친 F-117을 새 규칙은 잡는다", "F-117" in got, True)
    check("★F-88은 사정권 밖(조용한 소진)", "F-88" in got, False)
    # 현재 장부는 조용해야 한다 (F-90은 부분표기로 해소)
    check("현 장부는 0건", len(check_backlog_ledger()), 0)


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
