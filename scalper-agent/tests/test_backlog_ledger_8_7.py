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


def test_05_음성대조_구장부():
    print("\n[5] ★음성 대조 — git HEAD 구 장부에서 실제로 잡히는가")
    rel = "scalper-agent/docs/checklist/DAILY_ROUTINE.md"
    try:
        old = subprocess.run(["git", "show", f"HEAD:{rel}"], cwd=ROOT,
                             capture_output=True, timeout=30).stdout.decode("utf-8")
    except Exception as e:  # noqa: BLE001
        print(f"  ⏭  git show 실패({e}) — 음성 대조 skip")
        return
    if not old.strip():
        print("  ⏭  구 장부 판독 실패 — skip")
        return
    p = _TMP / "old_ledger.md"
    p.write_text(old, encoding="utf-8")
    got = ids(check_backlog_ledger(p))
    # ★테스트 자체 정정 1건(8/7) — 최초 기대에 F-88을 넣었으나 **코드가 옳고 내가 틀렸다**.
    #   F-88은 '모순'이 아니라 *완료 표기가 아예 없던* 건(조용한 소진)이라 이 규칙의
    #   사정권 밖이다. 구 장부의 진짜 모순은 정확히 9건.
    want = ["F-100", "F-101", "F-108", "F-110", "F-111", "F-113", "F-114", "F-90", "F-96"]
    check("구 장부에서 9건 검출", len(got), 9)
    check("8/7에 손으로 찾은 목록과 일치", got, want)
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
