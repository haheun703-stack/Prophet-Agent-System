# -*- coding: utf-8 -*-
"""[F-160] nightwatch 신선도 임계가 문서와 코드에서 서로 다른 값을 말하던 것 — 8/11 소진.

★ 무엇이었나
  같은 임계가 **세 곳**에 흩어져 서로 다른 값을 말했다:
      `get_current_regime` docstring : "24h+ stale"
      인라인 주석                    : "24시간 이상이면"
      실제 코드                      : `if age_hours > 48`
  읽는 사람은 "하루 지나면 폴백"이라 믿는데 실제로는 이틀이다. 매매 영향은 없었으나
  ("레짐이 동작한다"는 전제가 틀리면 그 위에 쌓는 판단이 틀린다) 같은 뿌리가
  [F-150](문서가 구현을 안 세고 적은 수치)·[F-110](CLAUDE.md 양방향 오류)이다.

★ 처방 — 문구를 고치지 않고 **중복될 숫자 자체를 없앴다**
  `NIGHTWATCH_STALE_HOURS` 모듈 상수 1개. 코드는 상수를 비교에 쓰고, docstring 은
  **숫자를 적지 않고 상수 이름을 가리킨다**. 숫자가 한 곳뿐이면 어긋날 수 없다.

★ 이 테스트를 쓰면서 조심한 것 — 자기참조 계수 (오늘 네 번째)
  모듈 **상단 주석**에는 사고 경위를 적느라 "24h+" 라는 옛 문구가 **인용**돼 있다.
  파일 전체를 grep 하는 검사기를 쓰면 그 인용을 위반으로 세어 **옳은 코드에 FAIL**을 낸다
  ([F-89]·[F-100]·8/10 [F-157] 과 동형). 그래서 검사 대상을 **함수 자신의 docstring**
  (`ast.get_docstring`)과 **비교식 노드**로 좁힌다 — 설명 주석은 사정권 밖이다.

★ 음성 대조 — 구 코드가 실제로 걸리는지 확인한다
  기준을 HEAD 에 묶지 않고(8/7·8/9 에 이틀 연속 저지른 실수) **합성 소스**를 임시 파일에
  써서 검사기에 먹인다. 구판(docstring "24h+" + `> 48`)이 통과하면 이 테스트는 무의미하다.

실행: python -X utf8 tests/test_macro_stale_threshold_8_11.py
"""
import ast
import re
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from data import macro_strategy                        # noqa: E402

SRC_PATH = Path(macro_strategy.__file__)
FUNC = "get_current_regime"
CONST = "NIGHTWATCH_STALE_HOURS"

# docstring 안의 "24h" / "48시간" 류 — 숫자 + 시간단위
_HOUR_CLAIM = re.compile(r"\d+\s*(?:h\b|시간)", re.IGNORECASE)


# ── 검사기 (음성 대조에서 재사용하므로 순수 함수로 분리) ──────────────────

def _find_func(tree, name):
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == name:
            return node
    return None


def docstring_states_hours(source: str, func: str = FUNC):
    """함수 **자신의 docstring** 이 시간 수치를 단언하는가 → 위반이면 매치 문자열.

    ★모듈 주석은 보지 않는다(사고 경위 인용이 자기 자신을 위반으로 만들지 않도록)."""
    node = _find_func(ast.parse(source), func)
    if node is None:
        raise AssertionError(f"{func} 를 소스에서 찾지 못함")
    doc = ast.get_docstring(node) or ""
    m = _HOUR_CLAIM.search(doc)
    return m.group(0) if m else None


def compares_against_literal(source: str, func: str = FUNC):
    """staleness 비교가 **숫자 리터럴**과 비교하는가 → 위반이면 그 숫자.

    상수 이름과 비교하면 None. 비교식 노드만 보므로 주석/문자열에 영향받지 않는다."""
    node = _find_func(ast.parse(source), func)
    if node is None:
        raise AssertionError(f"{func} 를 소스에서 찾지 못함")
    for sub in ast.walk(node):
        if not isinstance(sub, ast.Compare):
            continue
        left = sub.left
        if isinstance(left, ast.Name) and "age" in left.id.lower():
            for cmp_node in sub.comparators:
                if isinstance(cmp_node, ast.Constant) and isinstance(cmp_node.value, (int, float)):
                    return cmp_node.value
    return None


def uses_constant(source: str, const: str = CONST, func: str = FUNC) -> bool:
    """비교식이 그 상수 이름을 실제로 참조하는가."""
    node = _find_func(ast.parse(source), func)
    for sub in ast.walk(node):
        if isinstance(sub, ast.Compare):
            for n in ast.walk(sub):
                if isinstance(n, ast.Name) and n.id == const:
                    return True
    return False


# ── 본 검증 (현행 코드) ────────────────────────────────────────────────

def test_constant_exists_and_is_sane():
    """상수가 존재하고 양수 시간값이다."""
    assert hasattr(macro_strategy, CONST), f"{CONST} 상수가 없다"
    v = getattr(macro_strategy, CONST)
    assert isinstance(v, (int, float)), f"{CONST} 타입 이상: {type(v)}"
    assert v > 0, f"{CONST} 는 양수여야 한다: {v}"


def test_code_compares_against_constant_not_literal():
    """★핵심 — 코드가 숫자 리터럴이 아니라 상수와 비교한다."""
    src = SRC_PATH.read_text(encoding="utf-8")
    lit = compares_against_literal(src)
    assert lit is None, f"staleness 비교가 리터럴 {lit} 와 비교한다 — 상수로 바꿔야 한다"
    assert uses_constant(src), f"비교식이 {CONST} 를 참조하지 않는다"


def test_docstring_does_not_restate_the_number():
    """★핵심 — docstring 이 시간 수치를 다시 적지 않는다(적는 순간 어긋날 수 있다)."""
    src = SRC_PATH.read_text(encoding="utf-8")
    hit = docstring_states_hours(src)
    assert hit is None, (
        f"{FUNC} docstring 이 시간 수치 '{hit}' 를 단언한다 — "
        f"상수({CONST})를 가리키게 바꿔야 [F-160] 재발이 막힌다")


def test_docstring_points_at_the_constant():
    """숫자를 뺐으면 **어디를 보라**고는 알려줘야 한다(그냥 삭제는 정보 손실)."""
    node = _find_func(ast.parse(SRC_PATH.read_text(encoding="utf-8")), FUNC)
    doc = ast.get_docstring(node) or ""
    assert CONST in doc, f"{FUNC} docstring 이 {CONST} 를 가리키지 않는다"


def test_regime_keys_unchanged():
    """[F-158] 인접 불변식 — 이 fix 가 레짐 정의를 건드리지 않았다."""
    from data.macro_strategy import REGIME_STRATEGIES
    assert set(REGIME_STRATEGIES) == {"스태그플레이션", "비용상승", "수요둔화", "안정"}, \
        f"레짐 키가 변했다: {sorted(REGIME_STRATEGIES)}"


def test_sl_floor_still_holds():
    """[F-158] 하한이 이 커밋으로 깨지지 않았다 — 정의된 레짐 전수."""
    from data.macro_strategy import REGIME_STRATEGIES, get_adjusted_sl
    from data.sajang_rules import SAJANG
    rule = SAJANG.NORMAL_SL_PCT / 100
    bad = [k for k in REGIME_STRATEGIES if get_adjusted_sl(rule, regime=k) < rule - 1e-12]
    assert not bad, f"룰보다 조여진 레짐: {bad}"


# ── 음성 대조 (구판이 정말 걸리는가) ──────────────────────────────────

_OLD = '''
def get_current_regime() -> str:
    """현재 인플레이션 레짐 반환. 캐시 없거나 24h+ stale이면 '안정'."""
    age_hours = 0
    # staleness 체크: 24시간 이상이면 경고 + 안정 폴백
    if age_hours > 48:
        return "안정"
    return "안정"
'''

_ONLY_DOC_FIXED = '''
def get_current_regime() -> str:
    """현재 인플레이션 레짐 반환. 캐시 없거나 48h stale이면 '안정'."""
    age_hours = 0
    if age_hours > 48:
        return "안정"
    return "안정"
'''


def _write_tmp(src: str) -> Path:
    fh = tempfile.NamedTemporaryFile("w", suffix=".py", delete=False, encoding="utf-8")
    fh.write(src)
    fh.close()
    return Path(fh.name)


def test_negative_old_version_is_caught():
    """★음성 대조 — 구판(docstring 24h + 코드 48)은 **두 검사 모두** 걸려야 한다."""
    p = _write_tmp(_OLD)
    try:
        src = p.read_text(encoding="utf-8")
        assert docstring_states_hours(src) is not None, "구판 docstring 위반을 못 잡았다"
        assert compares_against_literal(src) == 48, "구판 리터럴 비교를 못 잡았다"
        assert not uses_constant(src), "구판이 상수를 쓴다고 잘못 판정했다"
    finally:
        p.unlink(missing_ok=True)


def test_negative_doc_only_fix_still_caught():
    """★음성 대조 2 — **문구만 48로 맞춘** 반쪽 fix 도 걸려야 한다.

    이게 걸리지 않으면 다음 사람이 숫자를 다시 두 곳에 적어도 초록불이 된다."""
    p = _write_tmp(_ONLY_DOC_FIXED)
    try:
        src = p.read_text(encoding="utf-8")
        assert docstring_states_hours(src) == "48h", "반쪽 fix 의 docstring 수치를 못 잡았다"
        assert compares_against_literal(src) == 48, "반쪽 fix 의 리터럴을 못 잡았다"
    finally:
        p.unlink(missing_ok=True)


def test_negative_checker_ignores_module_comments():
    """★음성 대조 3 — 설명 주석에 옛 문구를 인용해도 **위반이 아니다**(자기참조 차단).

    현행 `macro_strategy.py` 모듈 주석에는 "24h+" 가 사고 경위로 인용돼 있다.
    파일 전체 grep 이었다면 여기서 FAIL 이 났을 것이고, 그게 오늘 네 번째 자기참조 계수였다."""
    src = SRC_PATH.read_text(encoding="utf-8")
    assert _HOUR_CLAIM.search(src) is not None, \
        "전제 붕괴: 모듈에 시간 수치 인용이 없다면 이 대조는 의미가 없다(테스트를 갱신할 것)"
    assert docstring_states_hours(src) is None, \
        "docstring 검사기가 모듈 주석의 인용까지 세고 있다 — 사정권을 좁혀야 한다"


if __name__ == "__main__":
    print("=" * 72)
    print("★ [F-160] nightwatch 신선도 임계 — 숫자는 상수 한 곳에만 ★")
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
