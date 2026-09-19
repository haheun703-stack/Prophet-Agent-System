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


# ★9/19 [F-246] — 판정이 `"[CRITICAL]" in text` 단순 문자열 존재였다.
#   Codex 는 **"[CRITICAL] … SAJANG.get_take_profit 으로 수정되어 사장님 영구 룰을
#   준수하고 있습니다"** 처럼 *해소된 항목*에도 심각도 태그를 붙인다.
#   그래서 **룰 위반을 없애는 커밋이 오히려 차단**됐다(9/19 [F-245] 에서 2회 재현).
#   이런 오탐이 반복되면 사람이 매번 우회하게 되고 hook 자체가 무의미해진다 —
#   차단을 푸는 게 아니라 **줄 단위로 판독**해서 해소 문장을 분리한다.
#   ★판정 기준: 해소 표현이 하나라도 없는 CRITICAL 줄이 있으면 **그대로 차단**.
_RESOLVED_HINTS = (
    "준수하고", "준수합니다", "수정되어", "수정되었", "제거되었", "제거됨",
    "해결되었", "해결됨", "반영되었", "없습니다", "발견되지 않", "문제 없",
)


def _severity_lines(text: str, level: str):
    """(진짜 위반 줄, 해소로 판독한 줄) — 심각도 태그가 붙은 줄만 본다."""
    real, resolved = [], []
    for line in (text or "").splitlines():
        if f"[{level}]" not in line and f"{level}]" not in line:
            continue
        if any(h in line for h in _RESOLVED_HINTS):
            resolved.append(line)
        else:
            real.append(line)
    return real, resolved


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
    critical_found, resolved = _severity_lines(review_text, "CRITICAL")
    high_found, _hres = _severity_lines(review_text, "HIGH")
    if resolved:
        print()
        print(f"ℹ️ [CRITICAL] 태그가 붙었으나 **해소 문장**으로 판독한 줄 {len(resolved)}건 —")
        print("   Codex 가 '수정되어 준수하고 있습니다' 를 CRITICAL 로 태깅하는 사례입니다.")
        for ln in resolved[:5]:
            print(f"   · {ln.strip()[:150]}")

    if critical_found:
        print()
        print(f"🚨 Codex CRITICAL 발견 {len(critical_found)}건 — commit 차단 권고")
        for ln in critical_found[:5]:
            print(f"   · {ln.strip()[:150]}")
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
