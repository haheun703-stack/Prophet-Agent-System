# -*- coding: utf-8 -*-
"""저장소 전체 테스트 러너 — [F-119] 소진 (8/10).

★ 왜 만들었나 ★
  4-Tier의 Tier 3("단위 테스트 + 실제 매매 경로 회귀 PASS")를 **일괄 증명할 수단이 없었다.**
  테스트는 40개 파일·280개 함수가 있는데 저마다 자기 `__main__`으로만 돌아서, 검수 때마다
  손으로 골라 돌렸다. 손으로 고르는 순간 [F-124-2]가 재발한다(목록에서 빠진 테스트가
  100% 초록불 뒤로 숨는다). 그리고 [F-147] — `tests/test_flow_collector_naver_frgn.py`는
  순수 pytest 스타일이라 **어떤 경로로도 실행되지 않는데 파일명과 구조는 보호가 있는 것처럼
  보였다.** pytest는 이 환경에 설치돼 있지 않고 `pytest.ini`/`conftest.py`도 0건이다.

★ 이 러너의 계약 (전부 같은 교훈에서 나왔다) ★
  1. **분모 = 발견한 개수**(실행한 개수가 아니라). 7/20 *"성공 카운트 ≠ 성공 증거"*.
  2. **돌릴 수 없는 파일은 SKIP이 아니라 FAIL이다.** 조용한 skip이 [F-147]을 만들었다.
     구현하지 않은 fixture를 만나면 그 자리에서 예외를 던진다 — 넘어가지 않는다.
  3. **파일마다 subprocess 격리.** 한 파일이 죽어도 나머지가 돌고, sys.modules 오염이 없다.
  4. 실패·타임아웃·미실행이 하나라도 있으면 **exit 1**.

★ pytest가 아니다 ★
  구현한 fixture는 `tmp_path`·`monkeypatch` 둘뿐이다(현재 저장소가 실제로 쓰는 전부).
  `capsys` 등 다른 fixture를 쓰는 테스트가 새로 생기면 **에러로 튄다** — 그게 의도다.
  조용히 통과하느니 시끄럽게 실패해야 다음 사람이 여기에 추가한다.

실행:
    python -X utf8 tools/run_all_tests.py              # 전체
    python -X utf8 tools/run_all_tests.py -k trailing  # 경로에 문자열 포함만
    python -X utf8 tools/run_all_tests.py --list       # 발견 목록만(실행 안 함)
    python -X utf8 tools/run_all_tests.py --exec <파일> # 단일 파일(내부 fan-out용)
"""
import argparse
import ast
import io
import os
import subprocess
import sys
import time
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent
if str(BASE) not in sys.path:
    sys.path.insert(0, str(BASE))

# 격리 이동된 데드코드·캐시는 대상 아님. (실 소스만 센다)
EXCLUDE_PARTS = ("_archive", "__pycache__", ".git", "node_modules", ".venv", "venv")

HAS_MAIN = "has_main"          # 자체 __main__ 러너 보유 → 스크립트로 실행
PYTEST_STYLE = "pytest_style"  # __main__ 없고 최상위 test_ 함수 보유 → fixture 주입해 실행
MODULE_LEVEL = "module_level"  # __main__ 없고 test_ 함수도 없음 → import 자체가 실행
UNREADABLE = "unreadable"      # 읽기/파싱 실패 → FAIL (skip 아님)
UNRUNNABLE = "unrunnable"      # test_ 함수는 있는데 부를 방법이 없음 → FAIL (★[F-147] 그 모양)


def discover(root: Path = BASE):
    """test_*.py 전수 발견. 정렬 고정(실행 순서 재현성)."""
    out = []
    for p in sorted(root.rglob("test_*.py")):
        rel = p.relative_to(root).as_posix()
        if any(part in rel for part in EXCLUDE_PARTS):
            continue
        out.append(rel)
    return out


def classify(path: Path):
    """실행 방식 판정 — ★러너와 --exec가 **같은 함수**를 쓴다(판정 갈림 방지).

    반환: (종류, test_ 함수 이름들, 사유문)
    """
    try:
        src = io.open(path, encoding="utf-8").read()
    except Exception as e:
        return UNREADABLE, [], f"읽기 실패: {type(e).__name__}: {e}"
    try:
        tree = ast.parse(src)
    except SyntaxError as e:
        return UNREADABLE, [], f"파싱 실패: {e}"

    # 최상위 def만 getattr(mod, name)으로 부를 수 있다.
    names = [n.name for n in tree.body
             if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))
             and n.name.startswith("test_")]
    # 중첩(클래스 안·함수 안) 포함 전수 — 최상위와 어긋나면 '부를 수 없는 테스트'가 있다는 뜻.
    nested_only = [n.name for n in ast.walk(tree)
                   if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))
                   and n.name.startswith("test_") and n.name not in names]

    # '__main__' 문자열이 주석에만 있어도 오판하지 않게 AST로 확인한다.
    has_main = False
    for n in ast.walk(tree):
        if isinstance(n, ast.Compare) and isinstance(n.left, ast.Name) and n.left.id == "__name__":
            for c in n.comparators:
                if isinstance(c, ast.Constant) and c.value == "__main__":
                    has_main = True
    if has_main:
        return HAS_MAIN, names, "자체 __main__ 러너"
    if names:
        why = f"pytest 스타일 {len(names)}건 (fixture 주입 실행)"
        if nested_only:
            why += f" · ★중첩 {len(nested_only)}건은 부를 수 없음: {', '.join(nested_only[:5])}"
        return PYTEST_STYLE, names, why
    if nested_only:
        # ★ __main__도 없고 최상위 def도 없는데 test_ 함수가 존재 = 부를 방법이 없다.
        #   여기서 MODULE_LEVEL로 접으면 import만 하고 초록불이 뜬다 — [F-147] 그 자체.
        return UNRUNNABLE, nested_only, (
            f"실행 경로 없음 — test_ 함수 {len(nested_only)}건이 중첩돼 있는데 __main__이 없다 "
            f"({', '.join(nested_only[:5])})"
        )
    return MODULE_LEVEL, names, "모듈 레벨 실행(import=실행)"


# ────────────────────────────────────────────────────────────────────
# 최소 fixture — pytest 없이 pytest 스타일 테스트를 돌리기 위한 최소분.
# ★ 구현하지 않은 것은 조용히 넘기지 않고 예외로 튄다.
# ────────────────────────────────────────────────────────────────────
class _Missing:
    """'원래 없었음'을 나타내는 단일 표식.

    ★ 모듈 레벨이어야 한다 — setitem 안에서 `object()`를 새로 만들면 undo가 그것을
      알아볼 수 없어 **표식 객체를 값으로 써넣는다**(자기 테스트가 실제로 잡아낸 버그:
      `os.environ`에 넣으면 `TypeError: str expected, not object`).
    """

    def __repr__(self):
        return "<MISSING>"


_MISSING = _Missing()


class MonkeyPatch:
    """pytest monkeypatch의 최소 호환 구현 (undo는 테스트 1건 끝날 때마다)."""

    def __init__(self):
        self._undo = []

    def setattr(self, target, name, value):
        old = getattr(target, name)          # 없는 속성 패치는 AttributeError로 튄다(pytest와 동일 취지)
        self._undo.append(("attr", target, name, old))
        setattr(target, name, value)

    def delattr(self, target, name):
        old = getattr(target, name)
        self._undo.append(("attr", target, name, old))
        delattr(target, name)

    def setitem(self, dic, name, value):
        old = dic[name] if name in dic else _MISSING
        self._undo.append(("item", dic, name, old))
        dic[name] = value

    def delitem(self, dic, name):
        old = dic[name]
        self._undo.append(("item", dic, name, old))
        del dic[name]

    def setenv(self, name, value):
        self.setitem(os.environ, name, str(value))

    def delenv(self, name, raising=True):
        if name in os.environ:
            self.delitem(os.environ, name)
        elif raising:
            raise KeyError(name)

    def syspath_prepend(self, path):
        self._undo.append(("syspath", None, None, list(sys.path)))
        sys.path.insert(0, str(path))

    def chdir(self, path):
        self._undo.append(("cwd", None, None, os.getcwd()))
        os.chdir(str(path))

    def undo(self):
        for kind, target, name, old in reversed(self._undo):
            if kind == "attr":
                setattr(target, name, old)
            elif kind == "item":
                if old is _MISSING:
                    target.pop(name, None)   # 원래 없던 키는 되살리지 말고 지운다
                else:
                    target[name] = old
            elif kind == "syspath":
                sys.path[:] = old
            elif kind == "cwd":
                os.chdir(old)
        self._undo.clear()


def _make_fixture(name, tmp_root, mp):
    if name == "tmp_path":
        return tmp_root
    if name == "monkeypatch":
        return mp
    # ★ 조용한 skip 금지 — 모르는 fixture는 여기서 죽는다.
    raise NotImplementedError(
        f"fixture '{name}' 미구현 — tools/run_all_tests.py 의 _make_fixture 에 추가하십시오. "
        f"(조용히 건너뛰면 [F-147] 재발: 테스트처럼 보이는데 안 도는 파일이 생긴다)"
    )


def exec_one(rel: str, root: Path = BASE) -> int:
    """단일 파일 실행 — 이 프로세스에서. 종류별 실행 방식은 classify() 단일 판정.

    root는 자기 테스트(tests/test_run_all_tests_8_10.py)가 합성 픽스처 디렉터리를
    지정할 수 있게 열어 둔 것이다 — 실제 실행에선 항상 BASE.
    """
    import inspect
    import runpy
    import tempfile

    path = root / rel
    kind, names, why = classify(path)
    print(f"[run_all_tests] {rel} — {why}")

    if kind in (UNREADABLE, UNRUNNABLE):
        print(f"[FAIL] {rel}: {why}")
        return 1

    os.chdir(root)  # 테스트들이 상대경로(data_store/...)를 쓰므로 루트 고정

    if kind == HAS_MAIN:
        # ★ sys.argv를 대상 파일 것으로 바꿔 준다.
        #   unittest.main()·argparse를 쓰는 테스트가 러너의 `--exec <file>`을 자기 인자로 읽고
        #   "unrecognized arguments: --exec"로 죽는다(첫 전체 실행에서 4건 발생 — 러너 버그였다).
        saved_argv = sys.argv[:]
        sys.argv = [str(path)]
        try:
            runpy.run_path(str(path), run_name="__main__")
            return 0
        except SystemExit as e:
            return int(e.code or 0)
        finally:
            sys.argv = saved_argv

    # import 경로 확보 (bot/, data/, tests/ ... 어디든 모듈로 로드)
    import importlib.util
    modname = "bh_test_" + rel.replace("/", "_").replace(".py", "")
    spec = importlib.util.spec_from_file_location(modname, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[modname] = mod
    try:
        spec.loader.exec_module(mod)          # MODULE_LEVEL 은 이 시점에 이미 실행된다
    except Exception as e:
        import traceback
        traceback.print_exc()
        print(f"[FAIL] {rel}: import 중 {type(e).__name__}: {e}")
        return 1

    if kind == MODULE_LEVEL:
        return 0

    # PYTEST_STYLE — 발견한 함수 전부 실행 (분모 = 발견 개수)
    fns = [getattr(mod, n) for n in names]
    passed = failed = 0
    for fn in fns:
        sig = inspect.signature(fn)
        mp = MonkeyPatch()
        with tempfile.TemporaryDirectory(prefix="bh_tmp_") as td:
            kwargs = {}
            try:
                for pname in sig.parameters:
                    kwargs[pname] = _make_fixture(pname, Path(td), mp)
                fn(**kwargs)
                passed += 1
                print(f"  [PASS] {fn.__name__}")
            except AssertionError as e:
                failed += 1
                print(f"  [FAIL] {fn.__name__}: {e}")
            except Exception as e:
                import traceback
                traceback.print_exc()
                failed += 1
                print(f"  [ERROR] {fn.__name__}: {type(e).__name__}: {e}")
            finally:
                mp.undo()
    print(f"  결과: {passed}/{len(fns)} PASS, {failed} FAIL (발견 {len(fns)}건)")
    return 0 if failed == 0 and passed == len(fns) else 1


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--exec", dest="exec_file", help="단일 파일 실행(내부 fan-out용)")
    ap.add_argument("-k", dest="filter", help="경로에 이 문자열이 포함된 파일만")
    ap.add_argument("--list", action="store_true", help="발견 목록만 출력")
    ap.add_argument("--timeout", type=int, default=180, help="파일당 초 (기본 180)")
    args = ap.parse_args()

    if args.exec_file:
        sys.exit(exec_one(args.exec_file))

    files = discover()
    if args.filter:
        files = [f for f in files if args.filter in f]

    if args.list:
        for f in files:
            kind, names, why = classify(BASE / f)
            print(f"{kind:13s} | test_ {len(names):3d} | {f} — {why}")
        print(f"\n발견 {len(files)}개 파일")
        return 0

    print("=" * 78)
    print(f"★ 저장소 전체 테스트 — 발견 {len(files)}개 파일 ★")
    print("=" * 78)

    t0 = time.time()
    ok, bad, timeout = [], [], []
    for i, f in enumerate(files, 1):
        started = time.time()
        try:
            r = subprocess.run(
                [sys.executable, "-X", "utf8", str(Path(__file__).resolve()), "--exec", f],
                cwd=str(BASE), capture_output=True, text=True,
                encoding="utf-8", errors="replace", timeout=args.timeout,
            )
            rc, out = r.returncode, (r.stdout or "") + (r.stderr or "")
        except subprocess.TimeoutExpired:
            rc, out = 124, f"TIMEOUT {args.timeout}s"
            timeout.append(f)
        el = time.time() - started
        mark = "✅" if rc == 0 else "❌"
        print(f"{mark} [{i:2d}/{len(files)}] {f}  ({el:.1f}s)")
        if rc != 0:
            if f not in timeout:
                bad.append(f)
            for ln in out.strip().splitlines()[-25:]:
                print(f"      | {ln}")
        else:
            ok.append(f)

    print("=" * 78)
    # ★ 분모 = 발견 개수. 실행한 개수를 분모로 쓰면 누락이 100%로 위장된다([F-124-2]).
    print(f"결과: {len(ok)}/{len(files)} 파일 PASS · 실패 {len(bad)} · 타임아웃 {len(timeout)} "
          f"(발견 {len(files)}건 · {time.time() - t0:.1f}s)")
    for f in bad:
        print(f"  ❌ FAIL    {f}")
    for f in timeout:
        print(f"  ⏱ TIMEOUT {f}")
    print("=" * 78)
    return 0 if not bad and not timeout and len(ok) == len(files) else 1


if __name__ == "__main__":
    sys.exit(main())
