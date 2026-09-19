# -*- coding: utf-8 -*-
"""[F-224] JobResult 생성자 시그니처 — 문법상 유효하지만 런타임에 죽는 호출 (9/19 신설).

`JobResult(..., timestamp=...)` 였는데 `__init__` 에 그 파라미터가 없다(내부 자동 설정).
파이썬 문법으로는 완전히 유효해서 **pyflakes·RULE-002(undefined-name)로는 안 잡힌다.**
그리고 이 호출은 `run_g7` 안이고 **try 로 감싸여 있지 않다**(AST 확인) —
DEGRADED 모드가 되는 순간 run_g7 이 Stage 3 준비에서 죽고
C16·C18~C21·C23~C34·Stage4 가 전부 미실행되며 `group_status["G7"]` 이 RUNNING 에
고착돼 재시작 자동복구(PENDING 요구)도 무력해진다.

★그래서 **전수 검사**를 테스트로 건다 — 한 곳만 고치면 다음에 또 생긴다.
"""
import ast
import inspect
from pathlib import Path

from bot.trading_coo import JobResult

_SRC = Path(__file__).resolve().parent.parent / "bot" / "trading_coo.py"


def _valid_kwargs() -> set:
    return set(list(inspect.signature(JobResult.__init__).parameters)[1:])


def _jobresult_calls():
    tree = ast.parse(_SRC.read_text(encoding="utf-8"))
    return [n for n in ast.walk(tree)
            if isinstance(n, ast.Call) and getattr(n.func, "id", "") == "JobResult"]


def test_every_jobresult_call_matches_signature():
    """★전수 — 하나라도 시그니처를 벗어나면 그 경로는 런타임에 죽는다."""
    valid = _valid_kwargs()
    calls = _jobresult_calls()
    assert len(calls) >= 15, f"호출을 못 찾았다({len(calls)}) — 파싱 전제 확인"
    bad = []
    for n in calls:
        extra = {k.arg for k in n.keywords if k.arg} - valid
        if extra or len(n.args) > len(valid):
            bad.append((n.lineno, sorted(extra), len(n.args)))
    assert bad == [], f"시그니처 불일치: {bad}"


def test_timestamp_is_set_internally_not_passed():
    r = JobResult(name="x", success=True, elapsed=1.0)
    assert r.timestamp, "JobResult 가 스스로 타임스탬프를 채워야 한다"
    try:
        JobResult(name="x", success=True, elapsed=1.0, timestamp="2026-09-19")
    except TypeError:
        pass
    else:
        raise AssertionError("timestamp 를 받도록 바뀌었다면 이 테스트를 갱신할 것")


def test_degraded_skip_path_constructs_without_error():
    """DEGRADED 스킵 경로가 만드는 것과 **같은 모양**으로 생성해 본다."""
    r = JobResult(name="C15_predawn_buy", success=False, elapsed=0.0,
                  error="SKIPPED: DEGRADED mode")
    assert r.success is False
    assert r.error == "SKIPPED: DEGRADED mode"
    d = r.to_dict()
    assert d["name"] == "C15_predawn_buy"


def test_degraded_branch_is_still_outside_try():
    """사정권 정직 표기 — 이 fix 는 **인자만** 고쳤다.

    `run_g7` 의 Stage 3 준비 구간은 여전히 try 밖이므로, 그 구간에서 **다른** 예외가
    나면 같은 방식으로 G7 전체가 죽는다. 구조 변경은 별건이라 이 사실을 고정해 둔다.
    (이 단언이 깨지면 = 누군가 try 로 감쌌다는 뜻이니 그때 이 테스트를 지울 것)
    """
    # ★같은 tree 를 써야 한다 — `_jobresult_calls()` 는 **다른 파싱 결과**를 돌려주므로
    #   그 노드로 parents 를 조회하면 처음부터 miss 나고 chain 이 빈 채로 통과한다
    #   (9/19 작성 중 실제로 그렇게 FAIL 했다: '두 개의 다른 것을 같은 것으로 취급').
    tree = ast.parse(_SRC.read_text(encoding="utf-8"))
    parents = {}
    for node in ast.walk(tree):
        for ch in ast.iter_child_nodes(node):
            parents[ch] = node
    target = None
    for n in ast.walk(tree):
        if not (isinstance(n, ast.Call) and getattr(n.func, "id", "") == "JobResult"):
            continue
        for kw in n.keywords:
            if kw.arg == "name" and getattr(kw.value, "value", None) == "C15_predawn_buy":
                target = n
    assert target is not None, "C15 스킵 경로를 못 찾았다"
    chain, cur = [], target
    while cur in parents:
        cur = parents[cur]
        chain.append(type(cur).__name__)
    assert "Try" not in chain, "try 로 감싸였다면 이 테스트를 제거하고 장부를 갱신할 것"
    assert "AsyncFunctionDef" in chain
