"""Create Codex inbox requests automatically from local bot workflows."""

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore[assignment]


PROJECT_ROOT = Path(__file__).resolve().parent.parent
STATE_PATH = PROJECT_ROOT / "ops" / "codex_inbox" / "_codex_auto_last.txt"

RISK_ORDER = {"low": 0, "medium": 1, "high": 2, "critical": 3}

CRITICAL_PATTERNS = [
    "kill_switch",
    "paper_only",
    "trade_runtime",
    "kis_order",
    "mojito",
    "buy_market",
    "sell_market",
    "systemctl",
    "scheduler",
    "preflight",
]

HIGH_PATTERNS = [
    "wp_publisher",
    "legal_safe_filter",
    "hallucination",
    "qa_checker",
    "ai_opinion",
    "order_adapter",
    "auto_buy",
    "auto_sell",
    "cron",
]

MEDIUM_SUFFIXES = (
    ".py",
    ".yaml",
    ".yml",
    ".json",
    ".bat",
    ".ps1",
    ".html",
    ".jinja",
    ".toml",
    ".md",
)

DEFAULT_FOCUS = [
    "safety gate bypass",
    "schema compatibility",
    "graceful fallback",
    "test/preflight coverage",
    "owner approval boundary",
]

LIVE_APPROVAL_GATES = [
    "kill_switch=false",
    "PAPER_ONLY=false",
    "systemctl start",
    "systemctl enable",
    "systemctl unmask",
    "VPS deploy/pull for live trading",
    "real KIS order path",
    "force push/rebase/reset",
]


def now_kst() -> datetime:
    if ZoneInfo is not None:
        return datetime.now(ZoneInfo("Asia/Seoul"))
    return datetime.now(timezone.utc)


def slugify(text: str, default: str = "request") -> str:
    slug = re.sub(r"[^A-Za-z0-9._-]+", "-", text.strip()).strip("-._")
    return (slug or default)[:60]


def run(args: list[str], timeout: int = 60) -> tuple[int, str]:
    try:
        result = subprocess.run(
            args,
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=timeout,
        )
    except Exception as exc:
        return 99, f"{type(exc).__name__}: {exc}"
    text = "\n".join(part for part in [result.stdout, result.stderr] if part).strip()
    return result.returncode, text


def git(args: list[str]) -> str:
    try:
        result = subprocess.run(
            ["git", *args],
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=20,
        )
    except Exception:
        return ""
    return (result.stdout or "").strip()


def split_lines(text: str) -> list[str]:
    return [line.strip() for line in text.splitlines() if line.strip()]


def changed_files(staged: bool) -> list[str]:
    if staged:
        output = git(["diff", "--cached", "--name-only", "--diff-filter=ACMRT"])
    else:
        output = git(["diff", "--name-only", "HEAD"])
        untracked = git(["ls-files", "--others", "--exclude-standard"])
        output = "\n".join([output, untracked])
    seen: set[str] = set()
    files: list[str] = []
    for item in split_lines(output):
        if item not in seen:
            seen.add(item)
            files.append(item)
    return files


def classify_risk(files: list[str]) -> str:
    joined = "\n".join(files).lower()
    if any(pattern in joined for pattern in CRITICAL_PATTERNS):
        return "critical"
    if any(pattern in joined for pattern in HIGH_PATTERNS):
        return "high"
    if any(Path(item).suffix.lower() in MEDIUM_SUFFIXES for item in files):
        return "medium"
    return "low"


def diff_stat(staged: bool) -> str:
    if staged:
        return git(["diff", "--cached", "--stat"])
    return git(["diff", "--stat", "HEAD"])


def available_checks() -> list[tuple[str, list[str]]]:
    checks: list[tuple[str, list[str]]] = []
    if (PROJECT_ROOT / "tools" / "codex_lite.py").exists():
        checks.append(("codex_lite", [sys.executable, "tools/codex_lite.py"]))
    if (PROJECT_ROOT / "tools" / "self_review_checklist.py").exists():
        checks.append(("self_review_checklist", [sys.executable, "tools/self_review_checklist.py"]))
    if (PROJECT_ROOT / "tools" / "quant_preflight.py").exists():
        checks.append(("quant_preflight_blocked", [sys.executable, "tools/quant_preflight.py", "--expect", "blocked"]))
    return checks


def run_checks(no_checks: bool) -> list[tuple[str, int, str]]:
    if no_checks:
        return []
    results: list[tuple[str, int, str]] = []
    for name, command in available_checks():
        code, output = run(command, timeout=120)
        tail = "\n".join(output.splitlines()[-8:])
        results.append((name, code, tail))
    return results


def short_check_line(name: str, code: int, output: str) -> str:
    status = "PASS" if code == 0 else f"EXIT {code}"
    last = output.splitlines()[-1] if output.splitlines() else ""
    return f"{status} {name}: {last}"[:500]


def should_request(args: argparse.Namespace, files: list[str], risk: str, checks: list[tuple[str, int, str]]) -> bool:
    if args.always:
        return True
    if not files:
        return False
    if any(code != 0 for _, code, _ in checks):
        return True
    return RISK_ORDER[risk] >= RISK_ORDER[args.min_risk]


def fingerprint(files: list[str], risk: str, checks: list[tuple[str, int, str]], staged: bool) -> str:
    material = "\n".join([
        git(["rev-parse", "HEAD"]),
        "staged" if staged else "working",
        risk,
        *files,
        diff_stat(staged),
        *[f"{name}:{code}" for name, code, _ in checks],
    ])
    return hashlib.sha256(material.encode("utf-8", errors="replace")).hexdigest()


def is_duplicate(fp: str) -> bool:
    try:
        return STATE_PATH.exists() and STATE_PATH.read_text(encoding="utf-8").strip() == fp
    except Exception:
        return False


def remember(fp: str) -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    STATE_PATH.write_text(fp + "\n", encoding="utf-8")


def scoped_repo_state(files: list[str], staged: bool) -> dict[str, object]:
    return {
        "name": PROJECT_ROOT.name,
        "root": str(PROJECT_ROOT),
        "branch": git(["branch", "--show-current"]),
        "head": git(["rev-parse", "HEAD"]),
        "head_short": git(["rev-parse", "--short", "HEAD"]),
        "last_commit": git(["log", "-1", "--oneline"]),
        "status": git(["diff", "--cached", "--name-status"]) if staged else git(["status", "-sb", "--", "."]),
        "changed_files": files,
        "diff_stat": diff_stat(staged),
    }


def write_unique_json(stem: str, payload: dict[str, object]) -> Path:
    directory = PROJECT_ROOT / "ops" / "codex_inbox"
    directory.mkdir(parents=True, exist_ok=True)
    encoded = json.dumps(payload, ensure_ascii=False, indent=2) + "\n"
    for index in range(100):
        suffix = "" if index == 0 else f"-{index:02d}"
        path = directory / f"{stem}{suffix}.json"
        try:
            fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError:
            continue
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
            handle.write(encoded)
        return path
    raise RuntimeError(f"could not allocate unique file name for {stem}")


def create_request(args: argparse.Namespace, files: list[str], risk: str, checks: list[tuple[str, int, str]]) -> int:
    summary_lines = [
        args.summary or "Automatic Codex review request created by local bot workflow.",
        "",
        f"staged_mode: {args.staged}",
        f"risk: {risk}",
        "",
        "changed_files:",
        *[f"- {item}" for item in files[:40]],
    ]
    stat = diff_stat(args.staged)
    if stat:
        summary_lines.extend(["", "diff_stat:", stat])
    if checks:
        summary_lines.extend(["", "local_checks:"])
        summary_lines.extend(f"- {short_check_line(name, code, output)}" for name, code, output in checks)

    created_at = now_kst().isoformat(timespec="seconds")
    title = args.title or f"Automatic review: {risk} change"
    request_id = f"{now_kst().strftime('%Y%m%dT%H%M%S')}_{slugify(args.agent, 'agent')}_{slugify(title)}"
    payload: dict[str, object] = {
        "schema_version": "codex-collab-v1",
        "kind": "review_request",
        "request_id": request_id,
        "created_at": created_at,
        "agent": args.agent,
        "title": title,
        "summary": "\n".join(summary_lines),
        "risk": risk,
        "approval_required": bool(args.approval_required),
        "requested_focus": DEFAULT_FOCUS,
        "checks": [short_check_line(name, code, output) for name, code, output in checks],
        "repo": scoped_repo_state(files, args.staged),
        "safety": {
            "no_live_trade_actions": True,
            "owner_approval_required_for": LIVE_APPROVAL_GATES,
        },
    }
    if args.dry_run:
        print(json.dumps(payload, ensure_ascii=True, indent=2))
        return 0
    path = write_unique_json(request_id, payload)
    print(str(path.relative_to(PROJECT_ROOT)))
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Automatically create Codex inbox review requests")
    parser.add_argument("--agent", required=True)
    parser.add_argument("--title", default="")
    parser.add_argument("--summary", default="")
    parser.add_argument("--staged", action="store_true", help="use staged files for pre-commit hooks")
    parser.add_argument("--from-hook", action="store_true", help="do not fail the caller on request errors")
    parser.add_argument("--no-checks", action="store_true")
    parser.add_argument("--always", action="store_true")
    parser.add_argument("--force", action="store_true", help="ignore duplicate suppression")
    parser.add_argument("--approval-required", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--min-risk",
        choices=["low", "medium", "high", "critical"],
        default="medium",
    )
    args = parser.parse_args()

    files = changed_files(staged=args.staged)
    risk = classify_risk(files)
    checks = run_checks(args.no_checks)

    if not should_request(args, files, risk, checks):
        print(f"[codex_auto_request] skip: risk={risk}, files={len(files)}")
        return 0

    fp = fingerprint(files, risk, checks, staged=args.staged)
    if not args.force and not args.dry_run and is_duplicate(fp):
        print("[codex_auto_request] skip: duplicate request fingerprint")
        return 0

    code = create_request(args, files, risk, checks)
    if code == 0 and not args.dry_run:
        remember(fp)
    if args.from_hook and code != 0:
        print(f"[codex_auto_request] warning: request failed with exit {code}")
        return 0
    return code


if __name__ == "__main__":
    raise SystemExit(main())
