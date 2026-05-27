"""Codex collaboration inbox/outbox helper.

This tool gives local bots a small file-based protocol for asking Codex to
review changes without a human copy/paste relay.

Examples:
  python scripts/codex_collab.py request --agent blog-bot --title "News QA fix"
  python scripts/codex_collab.py approval --agent scalper-agent --title "Start paper rehearsal" --reason "systemctl start is required"
  python scripts/codex_collab.py review --request ops/codex_inbox/REQUEST.json --verdict pass --summary "No blocking issues"
  python scripts/codex_collab.py status
"""

import argparse
import json
import os
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover - Python 3.8 fallback only
    ZoneInfo = None  # type: ignore[assignment]


PROJECT_ROOT = Path(__file__).resolve().parent.parent
OPS_DIR = PROJECT_ROOT / "ops"
INBOX_DIR = OPS_DIR / "codex_inbox"
OUTBOX_DIR = OPS_DIR / "codex_outbox"
APPROVAL_DIR = OPS_DIR / "approval_required"

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


def run_git(args: list[str]) -> str:
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
    except Exception as exc:
        return f"git error: {exc}"
    output = (result.stdout if result.returncode == 0 else result.stderr or "").strip()
    return output


def split_lines(text: str) -> list[str]:
    return [line.strip() for line in text.splitlines() if line.strip()]


def normalize_project_path(path: str) -> str:
    prefix = f"{PROJECT_ROOT.name}/"
    normalized = path.replace("\\", "/")
    if normalized.startswith(prefix):
        return normalized[len(prefix):]
    return path


def normalize_git_text(text: str) -> str:
    return text.replace(f"{PROJECT_ROOT.name}/", "")


def emit(text: str = "") -> None:
    try:
        print(text)
    except UnicodeEncodeError:
        sys.stdout.buffer.write(text.encode("utf-8", errors="replace") + b"\n")


def git_changed_files() -> list[str]:
    tracked = split_lines(run_git(["diff", "--name-only", "HEAD", "--", "."]))
    untracked = split_lines(run_git(["ls-files", "--others", "--exclude-standard", "--", "."]))
    seen: set[str] = set()
    files: list[str] = []
    for item in tracked + untracked:
        item = normalize_project_path(item)
        if item not in seen:
            seen.add(item)
            files.append(item)
    return files


def repo_state() -> dict[str, Any]:
    root = str(PROJECT_ROOT)
    return {
        "name": Path(root).name,
        "root": root,
        "branch": run_git(["branch", "--show-current"]),
        "head": run_git(["rev-parse", "HEAD"]),
        "head_short": run_git(["rev-parse", "--short", "HEAD"]),
        "last_commit": run_git(["log", "-1", "--oneline"]),
        "status": normalize_git_text(run_git(["status", "-sb", "--", "."])),
        "changed_files": git_changed_files(),
        "diff_stat": normalize_git_text(run_git(["diff", "--stat", "HEAD", "--", "."])),
    }


def ensure_protocol_dirs() -> None:
    for path in [INBOX_DIR, OUTBOX_DIR, APPROVAL_DIR]:
        path.mkdir(parents=True, exist_ok=True)


def write_unique_json(directory: Path, stem: str, payload: dict[str, Any]) -> Path:
    ensure_protocol_dirs()
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


def write_unique_text(directory: Path, stem: str, content: str) -> Path:
    ensure_protocol_dirs()
    for index in range(100):
        suffix = "" if index == 0 else f"-{index:02d}"
        path = directory / f"{stem}{suffix}.md"
        try:
            fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError:
            continue
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
            handle.write(content.rstrip() + "\n")
        return path
    raise RuntimeError(f"could not allocate unique file name for {stem}")


def request_payload(args: argparse.Namespace) -> dict[str, Any]:
    created_at = now_kst().isoformat(timespec="seconds")
    request_id = f"{now_kst().strftime('%Y%m%dT%H%M%S')}_{slugify(args.agent, 'agent')}_{slugify(args.title)}"
    focus = args.focus or [
        "safety gate bypass",
        "schema compatibility",
        "graceful fallback",
        "test/preflight coverage",
    ]
    checks = args.check or []
    return {
        "schema_version": "codex-collab-v1",
        "kind": "review_request",
        "request_id": request_id,
        "created_at": created_at,
        "agent": args.agent,
        "title": args.title,
        "summary": args.summary,
        "risk": args.risk,
        "approval_required": bool(args.approval_required),
        "requested_focus": focus,
        "checks": checks,
        "repo": repo_state(),
        "safety": {
            "no_live_trade_actions": True,
            "owner_approval_required_for": LIVE_APPROVAL_GATES,
        },
    }


def command_request(args: argparse.Namespace) -> int:
    payload = request_payload(args)
    if args.dry_run:
        emit(json.dumps(payload, ensure_ascii=True, indent=2))
        return 0
    stem = payload["request_id"]
    path = write_unique_json(INBOX_DIR, stem, payload)
    emit(str(path.relative_to(PROJECT_ROOT)))
    return 0


def command_approval(args: argparse.Namespace) -> int:
    created_at = now_kst().isoformat(timespec="seconds")
    stem = f"{now_kst().strftime('%Y%m%dT%H%M%S')}_{slugify(args.agent, 'agent')}_{slugify(args.title, 'approval')}"
    lines = [
        f"# Approval Required: {args.title}",
        "",
        f"- schema_version: codex-collab-v1",
        f"- kind: approval_required",
        f"- created_at: {created_at}",
        f"- agent: {args.agent}",
        f"- risk: {args.risk}",
        "",
        "## Reason",
        args.reason,
        "",
        "## Requested Action",
        args.action or "(not specified)",
        "",
        "## Owner Approval Gates",
        *[f"- {gate}" for gate in LIVE_APPROVAL_GATES],
    ]
    if args.dry_run:
        emit("\n".join(lines))
        return 0
    path = write_unique_text(APPROVAL_DIR, stem, "\n".join(lines))
    emit(str(path.relative_to(PROJECT_ROOT)))
    return 0


def load_request(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as handle:
        return json.load(handle)


def command_review(args: argparse.Namespace) -> int:
    request_path = (PROJECT_ROOT / args.request).resolve()
    request = load_request(request_path)
    request_id = request.get("request_id", request_path.stem)
    created_at = now_kst().isoformat(timespec="seconds")
    stem = f"{now_kst().strftime('%Y%m%dT%H%M%S')}_{slugify(str(request_id), 'request')}_{args.verdict}"
    findings = args.finding or ["No additional findings recorded."]
    lines = [
        f"# Codex Review: {request.get('title', request_id)}",
        "",
        f"- schema_version: codex-collab-v1",
        f"- kind: codex_review",
        f"- created_at: {created_at}",
        f"- request_id: {request_id}",
        f"- verdict: {args.verdict}",
        "",
        "## Summary",
        args.summary,
        "",
        "## Findings",
        *[f"- {finding}" for finding in findings],
        "",
        "## Source Request",
        str(request_path.relative_to(PROJECT_ROOT)),
    ]
    if args.dry_run:
        emit("\n".join(lines))
        return 0
    path = write_unique_text(OUTBOX_DIR, stem, "\n".join(lines))
    emit(str(path.relative_to(PROJECT_ROOT)))
    return 0


def command_status(_: argparse.Namespace) -> int:
    ensure_protocol_dirs()
    for label, directory in [
        ("inbox", INBOX_DIR),
        ("outbox", OUTBOX_DIR),
        ("approval_required", APPROVAL_DIR),
    ]:
        files = [p for p in directory.iterdir() if p.name != ".gitkeep"]
        emit(f"{label}: {len(files)}")
        for path in sorted(files)[-5:]:
            emit(f"  - {path.relative_to(PROJECT_ROOT)}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Codex inbox/outbox collaboration helper")
    sub = parser.add_subparsers(dest="command", required=True)

    req = sub.add_parser("request", help="create a Codex review request JSON")
    req.add_argument("--agent", required=True)
    req.add_argument("--title", required=True)
    req.add_argument("--summary", default="")
    req.add_argument("--risk", choices=["low", "medium", "high", "critical"], default="medium")
    req.add_argument("--focus", action="append", help="review focus item; can be repeated")
    req.add_argument("--check", action="append", help="completed check/test; can be repeated")
    req.add_argument("--approval-required", action="store_true")
    req.add_argument("--dry-run", action="store_true")
    req.set_defaults(func=command_request)

    approval = sub.add_parser("approval", help="create an owner approval markdown item")
    approval.add_argument("--agent", required=True)
    approval.add_argument("--title", required=True)
    approval.add_argument("--reason", required=True)
    approval.add_argument("--action", default="")
    approval.add_argument("--risk", choices=["medium", "high", "critical"], default="high")
    approval.add_argument("--dry-run", action="store_true")
    approval.set_defaults(func=command_approval)

    review = sub.add_parser("review", help="create a Codex review response markdown")
    review.add_argument("--request", required=True)
    review.add_argument("--verdict", choices=["pass", "pass-with-notes", "changes-requested", "blocked"], required=True)
    review.add_argument("--summary", required=True)
    review.add_argument("--finding", action="append")
    review.add_argument("--dry-run", action="store_true")
    review.set_defaults(func=command_review)

    status = sub.add_parser("status", help="show protocol queue counts")
    status.set_defaults(func=command_status)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    sys.exit(main())
