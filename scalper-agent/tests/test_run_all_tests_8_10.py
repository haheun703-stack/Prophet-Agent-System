# -*- coding: utf-8 -*-
"""tools/run_all_tests.py 자기 검증 — [F-119] (8/10 신설).

★ 왜 러너에 테스트가 필요한가 ★
  이 러너는 "Tier 3를 일괄 증명하는 도구"다. **도구가 조용히 통과시키면 그 위의 모든 초록불이
  거짓이 된다.** 그래서 검증의 핵심은 "돌더라" 가 아니라 **"실패를 실패라고 하더라"** 다.

★ 기준을 HEAD에 묶지 않는다 ★
  8/7·8/9에 음성 대조를 `HEAD:` 파일에 묶는 실수를 **이틀 만에 두 번** 했다.
  기준이 움직이면 대조가 아니다. 여기서는 **합성 픽스처를 임시 디렉터리에 직접 써서**
  기대 동작을 고정한다 — 저장소 상태·커밋·시간에 의존하지 않는다.

실행: python -X utf8 tests/test_run_all_tests_8_10.py
"""
import io
import os
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.run_all_tests import (            # noqa: E402
    HAS_MAIN, PYTEST_STYLE, MODULE_LEVEL, UNREADABLE, UNRUNNABLE,
    MonkeyPatch, classify, discover, exec_one, _make_fixture,
)

# ── 합성 픽스처 (기준 고정 — 저장소 상태와 무관) ───────────────────────────
FX_PASSING_PYTEST = '''
def test_ok(tmp_path):
    p = tmp_path / "x.txt"
    p.write_text("hi", encoding="utf-8")
    assert p.read_text(encoding="utf-8") == "hi"
'''

FX_FAILING_PYTEST = '''
def test_bad():
    assert 1 == 2, "일부러 실패시킨 테스트"
'''

FX_UNKNOWN_FIXTURE = '''
def test_needs_capsys(capsys):
    assert True
'''

# ★[F-147]의 모양 — test_ 함수는 있는데 __main__도 없고 최상위도 아니라 부를 방법이 없다.
FX_UNRUNNABLE_NESTED = '''
class Thing:
    def test_hidden(self):
        assert False
'''

FX_MODULE_LEVEL = '''
VALUE = 1 + 1
assert VALUE == 2
'''

FX_MODULE_LEVEL_FAILING = '''
assert False, "모듈 레벨 실패"
'''

FX_HAS_MAIN_OK = '''
import sys
def test_x():
    assert True
if __name__ == "__main__":
    test_x()
    sys.exit(0)
'''

FX_HAS_MAIN_FAILING = '''
import sys
if __name__ == "__main__":
    sys.exit(1)
'''

# 오늘 잡은 러너 버그의 회귀 고정: unittest.main()/argparse 가 sys.argv 를 파싱한다.
FX_HAS_MAIN_ARGPARSE = '''
import argparse, sys, os, json
if __name__ == "__main__":
    json.dump(sys.argv, open(os.environ["ARGV_MARKER"], "w", encoding="utf-8"))
    argparse.ArgumentParser().parse_args()   # 러너 인자가 새어 들어오면 SystemExit(2)
    sys.exit(0)
'''

FX_SYNTAX_ERROR = 'def broken(:\n    pass\n'


def _write(d: Path, name: str, body: str) -> str:
    (d / name).write_text(body, encoding="utf-8")
    return name


def _run(d: Path, rel: str) -> int:
    """exec_one 을 합성 루트에서 실행하고 cwd 를 원복."""
    cwd = os.getcwd()
    try:
        return exec_one(rel, root=d)
    finally:
        os.chdir(cwd)


# ── 1. classify 판정 ─────────────────────────────────────────────────
def test_classify_kinds():
    with tempfile.TemporaryDirectory() as td:
        d = Path(td)
        _write(d, "test_a.py", FX_HAS_MAIN_OK)
        _write(d, "test_b.py", FX_PASSING_PYTEST)
        _write(d, "test_c.py", FX_MODULE_LEVEL)
        _write(d, "test_d.py", FX_UNRUNNABLE_NESTED)
        _write(d, "test_e.py", FX_SYNTAX_ERROR)
        assert classify(d / "test_a.py")[0] == HAS_MAIN
        assert classify(d / "test_b.py")[0] == PYTEST_STYLE
        assert classify(d / "test_c.py")[0] == MODULE_LEVEL
        assert classify(d / "test_d.py")[0] == UNRUNNABLE
        assert classify(d / "test_e.py")[0] == UNREADABLE
    print("[PASS] classify 5종 판정")


def test_classify_ignores_main_in_comment():
    """'__main__' 이 주석/문자열에만 있어도 HAS_MAIN 으로 오판하지 않는다(AST 판정)."""
    with tempfile.TemporaryDirectory() as td:
        d = Path(td)
        _write(d, "test_cm.py", '# 여기에 __main__ 이라고 적혀 있을 뿐이다\nX = "__main__"\n')
        assert classify(d / "test_cm.py")[0] == MODULE_LEVEL
    print("[PASS] 주석 속 __main__ 오판 없음")


# ── 2. ★음성 대조 — 실패를 실패라고 하는가 ────────────────────────────
def test_failing_pytest_style_is_reported():
    with tempfile.TemporaryDirectory() as td:
        d = Path(td)
        _write(d, "test_f.py", FX_FAILING_PYTEST)
        assert _run(d, "test_f.py") == 1, "실패 테스트가 통과로 보고됐다 — 러너 무의미"
    print("[PASS] 음성 대조: pytest 스타일 실패 → exit 1")


def test_failing_module_level_is_reported():
    with tempfile.TemporaryDirectory() as td:
        d = Path(td)
        _write(d, "test_g.py", FX_MODULE_LEVEL_FAILING)
        assert _run(d, "test_g.py") == 1, "모듈 레벨 실패가 묻혔다"
    print("[PASS] 음성 대조: 모듈 레벨 실패 → exit 1")


def test_failing_main_is_reported():
    with tempfile.TemporaryDirectory() as td:
        d = Path(td)
        _write(d, "test_h.py", FX_HAS_MAIN_FAILING)
        assert _run(d, "test_h.py") == 1, "__main__ 의 exit 1 이 묻혔다"
    print("[PASS] 음성 대조: __main__ exit 1 전달")


def test_unrunnable_is_fail_not_skip():
    """★[F-119]의 핵심 계약 — 못 돌리는 파일은 SKIP이 아니라 FAIL이다.

    여기서 0을 반환하면 [F-147]이 그대로 재발한다: 테스트처럼 생긴 파일이
    아무것도 실행하지 않고 초록불을 받는다.
    """
    with tempfile.TemporaryDirectory() as td:
        d = Path(td)
        _write(d, "test_i.py", FX_UNRUNNABLE_NESTED)
        assert _run(d, "test_i.py") == 1, "실행 경로 없는 파일이 조용히 통과 — F-147 재발"
    print("[PASS] 음성 대조: 실행 경로 없음 → FAIL (조용한 skip 아님)")


def test_unreadable_is_fail():
    with tempfile.TemporaryDirectory() as td:
        d = Path(td)
        _write(d, "test_j.py", FX_SYNTAX_ERROR)
        assert _run(d, "test_j.py") == 1, "파싱 실패가 통과로 처리됐다"
    print("[PASS] 음성 대조: 파싱 실패 → FAIL")


def test_unknown_fixture_raises_not_skips():
    """모르는 fixture 는 조용히 넘기지 않고 터진다 — 그래야 다음 사람이 추가한다."""
    raised = False
    try:
        _make_fixture("capsys", Path("."), MonkeyPatch())
    except NotImplementedError:
        raised = True
    assert raised, "미구현 fixture 가 조용히 통과 — 거짓 초록불 통로"

    with tempfile.TemporaryDirectory() as td:
        d = Path(td)
        _write(d, "test_k.py", FX_UNKNOWN_FIXTURE)
        assert _run(d, "test_k.py") == 1, "미구현 fixture 테스트가 통과로 보고됐다"
    print("[PASS] 음성 대조: 미구현 fixture → 예외 → FAIL")


# ── 3. 양성 대조 — 통과할 것은 통과하는가 ─────────────────────────────
def test_passing_cases_pass():
    with tempfile.TemporaryDirectory() as td:
        d = Path(td)
        _write(d, "test_p1.py", FX_PASSING_PYTEST)
        _write(d, "test_p2.py", FX_MODULE_LEVEL)
        _write(d, "test_p3.py", FX_HAS_MAIN_OK)
        assert _run(d, "test_p1.py") == 0
        assert _run(d, "test_p2.py") == 0
        assert _run(d, "test_p3.py") == 0
    print("[PASS] 양성 대조: 정상 3종 전부 exit 0")


def test_sys_argv_is_isolated_for_main_files():
    """★8/10 러너 버그 회귀 고정.

    첫 전체 실행에서 4개 파일이 `unrecognized arguments: --exec` 로 죽었다.
    runpy 가 같은 프로세스에서 도는데 sys.argv 에 러너 인자가 남아 있어서였다.
    (unittest.main()·argparse 를 쓰는 테스트가 자기 인자로 읽는다)
    """
    with tempfile.TemporaryDirectory() as td:
        d = Path(td)
        marker = d / "argv.json"
        _write(d, "test_argv.py", FX_HAS_MAIN_ARGPARSE)
        saved_argv = sys.argv[:]
        sys.argv = ["run_all_tests.py", "--exec", "test_argv.py"]   # 오염 상태 재현
        os.environ["ARGV_MARKER"] = str(marker)
        try:
            rc = _run(d, "test_argv.py")
        finally:
            sys.argv = saved_argv
            os.environ.pop("ARGV_MARKER", None)
        assert rc == 0, "argparse 테스트가 러너 인자를 읽고 죽었다 (argv 격리 실패)"
        import json
        seen = json.loads(marker.read_text(encoding="utf-8"))
        assert len(seen) == 1, f"대상 파일이 본 argv 가 오염됨: {seen}"
        assert "--exec" not in seen, f"러너 인자가 새어 들어감: {seen}"
        assert sys.argv == saved_argv, "exec_one 이 sys.argv 를 원복하지 않았다"
    print("[PASS] sys.argv 격리 + 원복 (8/10 러너 버그 회귀 고정)")


# ── 4. MonkeyPatch 최소 구현 ─────────────────────────────────────────
def test_monkeypatch_undo():
    import types
    m = types.SimpleNamespace(x=1)
    dic = {"k": "orig"}
    mp = MonkeyPatch()
    mp.setattr(m, "x", 99)
    mp.setitem(dic, "k", "patched")
    mp.setenv("BH_TEST_ENV_8_10", "on")
    assert m.x == 99 and dic["k"] == "patched" and os.environ["BH_TEST_ENV_8_10"] == "on"
    mp.undo()
    assert m.x == 1, "setattr undo 실패"
    assert dic["k"] == "orig", "setitem undo 실패"
    # ★이 줄이 실제로 버그를 잡았다: 원래 없던 키는 '되살리기'가 아니라 '지우기'다.
    #   sentinel을 그대로 써넣던 초판은 os.environ에서 TypeError로 터졌다.
    assert "BH_TEST_ENV_8_10" not in os.environ, "없던 env 키가 undo 후에도 남았다"
    print("[PASS] MonkeyPatch setattr/setitem/setenv + undo (없던 키는 삭제)")


def test_monkeypatch_undo_absent_key_is_removed():
    """없던 키 undo 계약을 dict 로도 고정 (env 는 str 강제라 타입 사고가 가려질 수 있다)."""
    dic = {}
    mp = MonkeyPatch()
    mp.setitem(dic, "새키", 1)
    assert dic["새키"] == 1
    mp.undo()
    assert "새키" not in dic, f"없던 키가 표식 객체로 남았다: {dic}"
    print("[PASS] MonkeyPatch 없던 키 → undo 시 삭제")


def test_monkeypatch_missing_attr_raises():
    """없는 속성 패치는 조용히 만들지 않고 터진다(오타로 엉뚱한 걸 검증하는 사고 차단)."""
    import types
    mp = MonkeyPatch()
    raised = False
    try:
        mp.setattr(types.SimpleNamespace(), "nope", 1)
    except AttributeError:
        raised = True
    assert raised, "없는 속성을 조용히 생성 — pytest 취지 위반"
    print("[PASS] MonkeyPatch 없는 속성 → AttributeError")


# ── 5. discover 제외 규칙 ────────────────────────────────────────────
def test_discover_excludes_archive_and_cache():
    with tempfile.TemporaryDirectory() as td:
        d = Path(td)
        _write(d, "test_live.py", FX_MODULE_LEVEL)
        for sub in ("_archive_dead_code_7_30", "__pycache__"):
            (d / sub).mkdir()
            (d / sub / "test_dead.py").write_text(FX_MODULE_LEVEL, encoding="utf-8")
        found = discover(d)
        assert "test_live.py" in found, found
        assert not any("_archive" in f or "__pycache__" in f for f in found), found
    print("[PASS] discover: 격리·캐시 제외")


def test_discover_finds_real_repo_tests():
    """실제 저장소에서 최소한 이 파일 자신은 발견돼야 한다(경로 규칙 회귀)."""
    found = discover(ROOT)
    assert "tests/test_run_all_tests_8_10.py" in found, "자기 자신을 발견 못 함"
    assert len(found) >= 40, f"발견 수가 급감했다: {len(found)}"
    print(f"[PASS] discover: 실저장소 {len(found)}개 발견")


if __name__ == "__main__":
    print("=" * 70)
    print("★ run_all_tests 자기 검증 — 음성 대조 포함 ([F-119]) ★")
    print("=" * 70)
    import inspect
    # 분모 = 발견 개수 ([F-124-2] — 손목록 금지)
    tests = [fn for name, fn in sorted(globals().items())
             if name.startswith("test_") and inspect.isfunction(fn)]
    passed, failed = 0, 0
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
