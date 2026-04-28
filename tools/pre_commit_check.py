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
        for rule in RULES:
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
