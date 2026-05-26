# -*- coding: utf-8 -*-
"""Pre-commit 자동 검수 스크립트.

커밋 대상 Python 파일에서 반복 실수를 잡아냄.
.git/hooks/pre-commit 에서 호출.

검사 항목:
  1. 텔레그램 배선 오류 (hasattr.*_tg_alert)
  2. 유니코드 깨짐 (U+FFFD replacement char)
  3. CSV raw split (parts[숫자] 패턴 — C1 규칙 위반)
  4. 미사용 import (from X import Y 중 Y가 파일 내 1회만 등장)

종료코드: 0=통과, 1=위반 발견(커밋 차단)
"""
import ast
import re
import subprocess
import sys
from pathlib import Path

# ─── 설정 ───────────────────────────────────────────
SCALPER_DIR = "scalper-agent"

# 검사 제외 파일 패턴
EXCLUDE_PATTERNS = [
    "_tmp_",
    "test_",
    "__pycache__",
    "pre_commit_check",
]

# ─── 검사 규칙 ──────────────────────────────────────
RULES = [
    {
        "id": "TG-001",
        "name": "텔레그램 hasattr 패턴",
        "pattern": r'hasattr\(self,\s*["\']_tg',
        "msg": "hasattr(self, '_tg_alert') 패턴 금지 — context.bot.send_message 사용",
        "severity": "CRITICAL",
    },
    {
        "id": "UNI-001",
        "name": "유니코드 깨짐",
        "pattern": r"\ufffd",
        "msg": "깨진 유니코드 문자 발견 (U+FFFD)",
        "severity": "HIGH",
    },
    {
        "id": "CSV-001",
        "name": "CSV raw split",
        "pattern": r'parts\[\d+\]',
        "msg": "parts[N] 직접 인덱싱 금지 — _parse_flow_csv() 유틸 사용 (C1 규칙)",
        "severity": "CRITICAL",
        "exclude_files": ["telegram_bot.py"],
    },
    # ★ 사장님 5/25 영구 룰 위반 자동 차단 (REVIEW_3TIER_RULE.md) ★
    {
        "id": "RULE-001",
        "name": "사장님 5/25 트레일링 -3% 일관 룰 위반 (옛 multi-zone)",
        "pattern": r'trail_pct\s*=\s*0\.(07|10|12|15)\b',
        "msg": (
            "★ 사장님 5/25 영구 룰 위반 ★ trail_pct = 0.07/0.10/0.12/0.15 = 옛 multi-zone\n"
            "         → 사장님: '-7%, -10%, -12%, -15%까지 회귀를 놔둘 필요가 있냐'\n"
            "         → 모든 trail은 -3% 일관 (bot.dynamic_trailing.decide_trailing 위임)\n"
            "         → 상한가 +25%+는 limit_up_split_sell 모듈 분리"
        ),
        "severity": "CRITICAL",
        "exclude_files": ["dynamic_trailing.py", "test_compute_trailing_real_5_25.py"],
    },
    # ★ RULE-003 단순 패턴 매칭 제거 (5/26 false positive 사고) ★
    # AST 분석은 check_unbound_local() 함수에서 처리 (실제 UnboundLocalError만 검출)
    # ★★★ 5/26 사장님 분노 후 신설 — 사장님 영구 룰 위반 패턴 영구 차단 ★★★
    {
        "id": "RULE-005",
        "name": "사장님 5/21 영구 룰 위반 — 고정 TP +5% 자동 설정",
        "pattern": r'take_profit.*=.*\*.*1\.0[3-9]|take_profit.*int.*\*.*1\.[0-9]+',
        "msg": (
            "★ 사장님 [feedback_trailing_only_tp] 영구 룰 정면 위반 ★\n"
            "         → take_profit = buy_price * 1.05 (또는 비슷) = +5% TP 자동 매도 사고\n"
            "         → 5/26 사장님 삼화콘덴서 47주 자동 매도 사고 (-296,100원 손실)\n"
            "         → 사장님 룰: 트레일링만 / 고정 TP 폐기 영구\n"
            "         → 강제: take_profit = 0 (Rule Registry SAJANG.FIXED_TP_FORCE_ZERO)"
        ),
        "severity": "CRITICAL",
        "exclude_files": ["sajang_rules.py", "test_", "codex_review.py"],
    },
    {
        "id": "RULE-006",
        "name": "사장님 5/26 영구 룰 위반 — mode='day' default (D+0 강제 청산)",
        "pattern": r'["\']mode["\']\s*:\s*["\']day["\']',
        "msg": (
            "★ 사장님 5/26 영구 룰 위반 ★ mode='day' = D+0 강제 청산 = 사장님 매수 보호 위반\n"
            "         → 5/26 사고: SYNC mode='day' + TP+5% = 삼화콘덴서 자동 매도\n"
            "         → 사장님 룰: 트레일링 + 룰 B/C 동적 매도 → mode='swing' 통일\n"
            "         → 강제: mode='swing' (Rule Registry SAJANG.SYNC_AUTO_MODE)"
        ),
        "severity": "CRITICAL",
        "exclude_files": ["sajang_rules.py", "test_", "trading_cfo.py", "codex_review.py"],
    },
    {
        "id": "RULE-007",
        "name": "사장님 5/23 영구 룰 위반 — entry_mode='open' default (시가 매수)",
        "pattern": r'entry_mode.*=.*["\']open["\']|entry_mode.*get\(.*["\']open["\']',
        "msg": (
            "★ 사장님 [project_pullback_entry_5_23] 영구 룰 위반 ★ 시가 매수 = -6.95% 빠짐 (백테스트)\n"
            "         → 사장님 룰: -3% 눌림 매수 +9.19% / 차이 +15.14%p\n"
            "         → 5/26 단타봇 자율 검증: 추가 +3.7%p 우세\n"
            "         → 강제: entry_mode='pullback_3pct' default on (Rule Registry SAJANG.ENTRY_MODE_DEFAULT)"
        ),
        "severity": "HIGH",
        "exclude_files": ["sajang_rules.py", "test_"],
    },
]


def get_staged_py_files() -> list[str]:
    """git에 staged된 .py 파일 목록 반환."""
    try:
        result = subprocess.run(
            ["git", "diff", "--cached", "--name-only", "--diff-filter=ACM"],
            capture_output=True, text=True, encoding="utf-8",
        )
        files = []
        for f in result.stdout.strip().split("\n"):
            if not f.endswith(".py"):
                continue
            if any(ex in f for ex in EXCLUDE_PATTERNS):
                continue
            files.append(f)
        return files
    except Exception:
        return []


def check_unbound_local(filepath: str, content: str) -> list[dict]:
    """★ RULE-003 AST 정밀화 (5/26 false positive 사고 후 신설) ★

    UnboundLocalError 패턴 검출 — 함수 안에서 `await name()` 또는 `name()` 호출이
    같은 함수 안의 `async def name` / `def name` 정의보다 앞에 있는 경우.

    단순 정규식 매칭 (모든 await _send 패턴)으로는 false positive 다발.
    AST로 함수별 nested def + 호출 위치 비교 → 진짜 위반만 검출.
    """
    issues = []
    try:
        tree = ast.parse(content)
    except SyntaxError:
        return issues   # syntax 에러는 별도

    def check_func(node, parent_name=""):
        """함수 안에서 nested def 정의 위치 vs 호출 위치 비교."""
        if not isinstance(node, (ast.AsyncFunctionDef, ast.FunctionDef)):
            return
        # 1. 이 함수 안에 정의된 nested def 이름 + 첫 정의 라인
        nested_defs = {}
        for n in ast.walk(node):
            if isinstance(n, (ast.AsyncFunctionDef, ast.FunctionDef)) and n is not node:
                if n.name not in nested_defs:
                    nested_defs[n.name] = n.lineno
        # 2. 이 함수 안에서 await name() / name() 호출 (Name 노드)
        for n in ast.walk(node):
            if isinstance(n, (ast.Call,)) and isinstance(n.func, ast.Name):
                call_name = n.func.id
                if call_name in nested_defs:
                    def_line = nested_defs[call_name]
                    use_line = n.lineno
                    # 호출이 정의보다 앞 → UnboundLocalError
                    if use_line < def_line:
                        issues.append({
                            "file": filepath,
                            "line": use_line,
                            "rule": "RULE-003",
                            "severity": "CRITICAL",
                            "msg": (
                                f"★ UnboundLocalError 위험 ★ {node.name}() 안 {call_name}() 호출 "
                                f"L{use_line} but 정의 L{def_line}\n"
                                f"         → 5/25 1차 사고 패턴 (_send UnboundLocalError)\n"
                                f"         → fix: nested def를 호출보다 앞으로 이동"
                            ),
                        })
        # 3. 재귀 (nested def 안에 또 nested 있을 수 있음)
        for sub in ast.iter_child_nodes(node):
            check_func(sub, node.name)

    for node in ast.iter_child_nodes(tree):
        if isinstance(node, ast.ClassDef):
            for m in node.body:
                check_func(m, node.name)
        else:
            check_func(node)
    return issues


def check_unused_imports(filepath: str, content: str) -> list[dict]:
    """단순 미사용 import 검출 (from X import Y 패턴)."""
    issues = []
    lines = content.split("\n")

    for i, line in enumerate(lines, 1):
        # from X import Y, Z 패턴
        m = re.match(r'^from\s+\S+\s+import\s+(.+)$', line.strip())
        if not m:
            continue

        imports_str = m.group(1)
        # 멀티라인 import나 as 별칭은 스킵
        if "(" in imports_str or "\\" in line:
            continue

        names = [n.strip().split(" as ")[-1].strip() for n in imports_str.split(",")]
        for name in names:
            if not name or name.startswith("#"):
                continue
            # 파일 전체에서 해당 이름이 1번(import 줄)만 등장하면 미사용
            occurrences = len(re.findall(r'\b' + re.escape(name) + r'\b', content))
            if occurrences <= 1:
                issues.append({
                    "file": filepath,
                    "line": i,
                    "rule": "IMP-001",
                    "severity": "MEDIUM",
                    "msg": f"미사용 import: {name}",
                })
    return issues


def run_checks() -> list[dict]:
    """모든 검사 실행."""
    files = get_staged_py_files()
    if not files:
        return []

    all_issues = []

    for filepath in files:
        path = Path(filepath)
        if not path.exists():
            continue
        try:
            content = path.read_text(encoding="utf-8")
        except Exception:
            continue

        # 패턴 규칙 검사
        fname = path.name
        for rule in RULES:
            # 규칙별 파일 제외 (예: CSV-001은 telegram_bot.py 제외)
            if fname in rule.get("exclude_files", []):
                continue
            for i, line in enumerate(content.split("\n"), 1):
                if re.search(rule["pattern"], line):
                    all_issues.append({
                        "file": filepath,
                        "line": i,
                        "rule": rule["id"],
                        "severity": rule["severity"],
                        "msg": rule["msg"],
                    })

        # 미사용 import 검사
        all_issues.extend(check_unused_imports(filepath, content))

        # ★ RULE-003 AST 정밀 검사 (5/26 false positive 사고 후 신설) ★
        all_issues.extend(check_unbound_local(filepath, content))

    return all_issues


def main():
    issues = run_checks()
    if not issues:
        print("[pre-commit] 검수 통과 ✓")
        return 0

    # 심각도별 정렬
    severity_order = {"CRITICAL": 0, "HIGH": 1, "MEDIUM": 2, "LOW": 3}
    issues.sort(key=lambda x: severity_order.get(x["severity"], 9))

    print(f"\n[pre-commit] 검수 실패 — {len(issues)}건 발견\n")
    print(f"{'심각도':<10} {'규칙':<8} {'위치':<40} {'내용'}")
    print("-" * 90)

    has_blocking = False
    for issue in issues:
        loc = f"{issue['file']}:{issue['line']}"
        print(f"{issue['severity']:<10} {issue['rule']:<8} {loc:<40} {issue['msg']}")
        if issue["severity"] in ("CRITICAL", "HIGH"):
            has_blocking = True

    print()
    if has_blocking:
        print("CRITICAL/HIGH 발견 — 커밋 차단됨. 수정 후 다시 시도하세요.")
        return 1
    else:
        print("MEDIUM/LOW만 발견 — 커밋은 허용하되, 정리 권장합니다.")
        return 0


if __name__ == "__main__":
    sys.exit(main())
