# -*- coding: utf-8 -*-
"""audit_silent_import.py — '조용히 죽은 기능' 전수 검출 (read-only·8/11 전체검수)

★왜 — 같은 사고가 최소 두 번 났고 둘 다 **손으로** 찾았다
  [F-40]  ETF 리더 관련 잡이 조용히 죽어 있었다.
  [F-156] `etf_recommender.py:232` 가 `from data.sector_momentum import load_sector_report`
          하는데 **그 이름이 저장소에 없다**. `try/except Exception: pass` 안이라
          조용히 실패하고 섹터 모멘텀 가산이 **영구 0** 이 됐다. 소비 경로는 살아 있었다.

  공통 모양 = **없는 이름을 import + 예외를 삼킴 → 기능은 죽었는데 초록불**.
  이건 사람이 읽어서 찾을 게 아니라 기계가 세어야 한다.

★방법 (정적·실행 없음)
  1) 모든 .py 에서 `try: ... from M import N ... except: <조용>` 형태를 찾는다.
  2) M 을 저장소 경로로 해석해 **그 모듈의 최상위 정의 이름 집합**을 AST 로 뽑는다.
  3) N 이 그 집합에 없으면 → **조용히 죽은 import** 로 보고한다.

  '조용한 handler' 판정: 본문이 pass / 로깅만 / 상수대입만 인 경우.
  (재발생(raise)·sys.exit·대체 구현 호출이 있으면 조용하지 않다고 본다.)

★오탐을 줄이는 장치
  - 표준/서드파티 모듈은 해석 대상에서 제외(저장소 내 모듈만 판정).
  - `__init__.py` 의 재수출(`from .x import y`)도 이름 집합에 포함.
  - `__all__`·조건부 정의(`if TYPE_CHECKING`) 등은 이름 집합에 넣어 보수적으로 통과시킨다.
  = **못 찾는 쪽으로 틀리게** 만든다. 없는 사고를 만드는 것보다 낫다.
"""
import ast
import sys
from pathlib import Path

ROOT = Path(sys.argv[1] if len(sys.argv) > 1 else ".").resolve()
SKIP_DIRS = {"_archive_slop_7_14", "_staging", "_upload", "from-jgis", "node_modules",
             ".git", "venv", "__pycache__", "docs"}


def py_files():
    for p in ROOT.rglob("*.py"):
        if any(part in SKIP_DIRS for part in p.parts):
            continue
        yield p


def module_to_path(mod: str):
    """'data.sector_momentum' → 저장소 내 파일 경로(없으면 None = 외부 모듈)."""
    rel = mod.replace(".", "/")
    for cand in (ROOT / f"{rel}.py", ROOT / rel / "__init__.py"):
        if cand.exists():
            return cand
    return None


_names_cache = {}


def top_level_names(path: Path):
    """모듈이 제공하는 최상위 이름 집합 (보수적으로 넓게)."""
    if path in _names_cache:
        return _names_cache[path]
    names = set()
    try:
        tree = ast.parse(path.read_text(encoding="utf-8", errors="replace"))
    except Exception:
        _names_cache[path] = None          # 파싱 실패 → 판정 포기(오탐 방지)
        return None
    for node in ast.walk(tree):            # walk = 조건부 정의도 포함(보수적)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            names.add(node.name)
        elif isinstance(node, ast.Assign):
            for t in node.targets:
                if isinstance(t, ast.Name):
                    names.add(t.id)
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            names.add(node.target.id)
        elif isinstance(node, ast.Import):
            for a in node.names:
                names.add(a.asname or a.name.split(".")[0])
        elif isinstance(node, ast.ImportFrom):
            for a in node.names:
                names.add(a.asname or a.name)
    _names_cache[path] = names
    return names


def handler_is_quiet(handlers):
    """except 블록이 '조용한가' — 재발생/종료/대체호출이 없으면 조용하다고 본다."""
    for h in handlers:
        for sub in ast.walk(h):
            if isinstance(sub, ast.Raise):
                return False
            if isinstance(sub, ast.Call):
                f = sub.func
                nm = getattr(f, "attr", None) or getattr(f, "id", None)
                if nm in ("exit", "_exit"):
                    return False
                # 로깅류는 조용한 것으로 간주
                if nm in ("warning", "error", "exception", "info", "debug", "print",
                          "warn", "critical"):
                    continue
                # 그 밖의 호출 = 대체 구현일 수 있음 → 조용하지 않다고 본다(보수적)
                return False
    return True


def main():
    # ★set — 중첩 try 안의 import 는 바깥/안쪽 Try 양쪽에서 방문돼 **같은 자리가 두 번**
    #   계수된다(1차 실행에서 `etf_recommender:232` 가 2건으로 찍혔다).
    #   건수를 세는 도구가 중복을 세면 그 숫자로 판단하는 사람이 틀린다.
    hits = set()
    scanned = 0
    for p in py_files():
        scanned += 1
        try:
            tree = ast.parse(p.read_text(encoding="utf-8", errors="replace"))
        except Exception:
            continue
        for node in ast.walk(tree):
            if not isinstance(node, ast.Try):
                continue
            if not handler_is_quiet(node.handlers):
                continue
            for sub in ast.walk(node):
                if not isinstance(sub, ast.ImportFrom) or not sub.module:
                    continue
                tgt = module_to_path(sub.module)
                if tgt is None:
                    continue                     # 외부 모듈 — 판정 안 함
                provided = top_level_names(tgt)
                if provided is None:
                    continue
                for a in sub.names:
                    if a.name == "*":
                        continue
                    if a.name in provided:
                        continue
                    # ★1차 스캔 오탐 fix — `from data import trade_journal` 처럼
                    #   **서브모듈**을 가져오는 형태는 `__init__.py` 의 이름 집합에
                    #   없어도 정상이다. 파이썬이 패키지 하위 모듈로 해석한다.
                    #   이걸 빼먹어 1차 실행이 48건을 냈고 그중 대부분이 이것이었다.
                    if module_to_path(f"{sub.module}.{a.name}") is not None:
                        continue
                    hits.add((str(p.relative_to(ROOT)), sub.lineno,
                              sub.module, a.name,
                              str(tgt.relative_to(ROOT))))

    print(f"스캔 {scanned} 파일 · 조용히 죽은 import **{len(hits)}건**\n")
    if hits:
        print(f"{'파일':44s} {'행':>5s}  {'없는 이름':26s} <- 대상 모듈")
        for f, ln, mod, nm, tgt in sorted(hits):
            print(f"{str(f):44s} {ln:5d}  {nm:26s} <- {mod} ({tgt})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
