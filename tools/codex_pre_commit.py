# -*- coding: utf-8 -*-
"""★ 사장님 5/26 통찰 — Codex 사전 검수 의무화 (commit 전 자동 호출) ★

매 commit 전 자동으로 Codex (gpt-4o)에 변경 코드 검수 의뢰.
사장님 영구 룰 위반 / 미친짓 패턴 / 통합 사고 자동 검출.

활용 패턴 5가지 (단타봇 자율 리딩):
  A. 매 commit — codex_pre_commit.py (이 파일) 자동 호출
  B. 사장님 영구 룰 변경 — codex_rule_change_review.py (별도)
  C. 사고 발생 — codex_incident_diagnose.py (별도)
  D. Daily Self-Audit 보완 — daily_self_audit.py + Codex 추가 검수
  E. 매매 패턴 학습 — codex_pattern_learning.py (별도)

종료 코드:
  0 — Codex 검수 OK (또는 LOW/MEDIUM만 / commit 허용)
  1 — CRITICAL/HIGH 발견 (commit 차단 권고 / 단, 사장님 override 가능)
  2 — Codex API 실패 (오프라인 등 — commit 허용, 경고만)
"""
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scalper-agent"))


def get_staged_diff() -> str:
    """git에 staged된 변경 사항 diff 가져오기."""
    try:
        result = subprocess.run(
            ["git", "diff", "--cached", "--no-color", "-U3"],
            capture_output=True, text=True, encoding="utf-8", timeout=10,
            cwd=str(ROOT),
        )
        diff = result.stdout or ""
        # 너무 길면 잘라냄 (Codex 토큰 제한)
        if len(diff) > 8000:
            diff = diff[:8000] + "\n\n...(이후 truncated, " + str(len(diff) - 8000) + " 자)..."
        return diff
    except Exception as e:
        print(f"[codex_pre_commit] git diff 실패: {e}", file=sys.stderr)
        return ""


def get_staged_files() -> list:
    """staged Python 파일 목록."""
    try:
        result = subprocess.run(
            ["git", "diff", "--cached", "--name-only", "--diff-filter=ACM"],
            capture_output=True, text=True, encoding="utf-8",
            cwd=str(ROOT),
        )
        files = [f for f in result.stdout.strip().split("\n") if f.endswith(".py")]
        return files
    except Exception:
        return []


def main():
    """commit 전 Codex 자동 검수."""
    diff = get_staged_diff()
    files = get_staged_files()

    if not diff or not files:
        print("[codex_pre_commit] 변경 사항 없음 → skip")
        return 0

    # 작은 변경 (< 50 라인) skip — Codex 토큰 절약
    diff_lines = diff.count("\n")
    if diff_lines < 30:
        print(f"[codex_pre_commit] 작은 변경 ({diff_lines}줄) → Codex skip (API 비용 절약)")
        return 0

    # Codex 검수 호출
    try:
        from utils.codex_review import codex_review_code
    except Exception as e:
        print(f"[codex_pre_commit] codex_review import 실패 (commit 허용): {e}", file=sys.stderr)
        return 2

    print(f"[codex_pre_commit] Codex 검수 호출 — 파일 {len(files)}건 / {diff_lines}줄")
    print(f"  대상: {', '.join(files[:5])}")

    result = codex_review_code(
        code_snippet=diff,
        review_focus=(
            "git commit 변경 사항 검수 — 사장님 영구 룰 위반 / 단타봇 미친짓 패턴 / "
            "매도 가드 누락 / default off / 통합 사고"
        ),
        max_tokens=2000,
    )

    if not result.get("success"):
        print(f"[codex_pre_commit] ⚠️ Codex API 실패: {result.get('error')}", file=sys.stderr)
        print("  → commit 허용 (오프라인 가능 / 단타봇이 수동 검수 의무)")
        return 2

    review_text = result.get("review", "")
    print()
    print("=" * 70)
    print("★ Codex 검수 결과 (gpt-4o) ★")
    print("=" * 70)
    print(review_text)
    print("=" * 70)

    # CRITICAL/HIGH 발견 시 차단 권고
    critical_found = "[CRITICAL]" in review_text or "CRITICAL]" in review_text
    high_found = "[HIGH]" in review_text or "HIGH]" in review_text

    if critical_found:
        print()
        print("🚨 Codex CRITICAL 발견 — commit 차단 권고")
        print("  단타봇 의무: fix 또는 사장님 confirm 후 --no-verify 사용")
        return 1
    elif high_found:
        print()
        print("⚠️ Codex HIGH 발견 — commit 허용 (단타봇이 사장님 보고 의무)")
        return 0
    else:
        print()
        print("✅ Codex 검수 통과 (LOW/MEDIUM 또는 깨끗함)")
        return 0


if __name__ == "__main__":
    sys.exit(main())
