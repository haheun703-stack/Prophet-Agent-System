#!/bin/bash
# install_codex_hook.sh
# Body Hunter v4 monorepo - Codex auto-inbox pre-commit hook installer
#
# Purpose:
#   Install/refresh .git/hooks/pre-commit so every commit in this repo
#   runs the 2-stage static review and creates a Codex inbox request.
#
# Owner approval boundary (per docs/codex_collaboration_protocol.md):
#   This installer NEVER touches:
#     - kill_switch
#     - PAPER_ONLY / trade_runtime_config
#     - systemctl start/enable/unmask
#     - quantum-scheduler mask
#     - real KIS order paths
#   It only installs a local git hook and (optionally) runs a dry-run.
#
# Usage:
#   bash scripts/install_codex_hook.sh            # install only
#   bash scripts/install_codex_hook.sh --verify   # install + dry-run check
#   bash scripts/install_codex_hook.sh --uninstall  # restore latest backup
#
# Exit codes:
#   0 = success
#   1 = not inside a git repo
#   2 = required helper files missing (run `git pull` first)
#   3 = python not found
#   4 = uninstall failed (no backup found)

set -euo pipefail

REPO_ROOT="$(git rev-parse --show-toplevel 2>/dev/null || true)"
if [ -z "$REPO_ROOT" ]; then
    echo "[install_codex_hook] ERROR: not inside a git repository." >&2
    exit 1
fi

HOOK_DIR="$REPO_ROOT/.git/hooks"
HOOK_FILE="$HOOK_DIR/pre-commit"
BACKUP_DIR="$HOOK_DIR/_backup"

REQUIRED=(
    "tools/pre_commit_check.py"
    "tools/codex_pre_commit.py"
    "scalper-agent/tools/codex_auto_request.py"
)
for rel in "${REQUIRED[@]}"; do
    if [ ! -f "$REPO_ROOT/$rel" ]; then
        echo "[install_codex_hook] ERROR: missing $rel" >&2
        echo "[install_codex_hook] Run \`git pull\` first." >&2
        exit 2
    fi
done

if ! command -v python >/dev/null 2>&1 && ! command -v python3 >/dev/null 2>&1; then
    echo "[install_codex_hook] ERROR: python not found in PATH." >&2
    exit 3
fi

mkdir -p "$HOOK_DIR" "$BACKUP_DIR"

# ----- uninstall path -----
if [ "${1:-}" = "--uninstall" ]; then
    LATEST_BACKUP="$(ls -1t "$BACKUP_DIR"/pre-commit.* 2>/dev/null | head -n 1 || true)"
    if [ -z "$LATEST_BACKUP" ]; then
        echo "[install_codex_hook] ERROR: no backup found in $BACKUP_DIR" >&2
        exit 4
    fi
    cp "$LATEST_BACKUP" "$HOOK_FILE"
    chmod +x "$HOOK_FILE"
    echo "[install_codex_hook] restored from $LATEST_BACKUP"
    exit 0
fi

# ----- backup existing hook -----
if [ -f "$HOOK_FILE" ]; then
    TS="$(date +%Y%m%d_%H%M%S)"
    BACKUP_FILE="$BACKUP_DIR/pre-commit.$TS"
    cp "$HOOK_FILE" "$BACKUP_FILE"
    echo "[install_codex_hook] backup: $BACKUP_FILE"
fi

# ----- write hook -----
cat > "$HOOK_FILE" <<'HOOK_EOF'
#!/bin/sh
# Body Hunter v4 monorepo pre-commit hook
# Stage 1: tools/pre_commit_check.py (static rules)
# Stage 2: tools/codex_pre_commit.py (Codex review, optional)
# Stage 3: scalper-agent/tools/codex_auto_request.py (always best-effort)
#
# This hook NEVER:
#   - flips kill_switch / PAPER_ONLY
#   - calls systemctl
#   - touches real KIS order paths
# It is install-managed by scripts/install_codex_hook.sh

# Stage 1
PYTHONIOENCODING=utf-8 python tools/pre_commit_check.py
STAGE1=$?
if [ $STAGE1 -ne 0 ]; then
    echo ""
    echo "[hook] Stage 1 (pre_commit_check) failed - commit blocked"
    exit $STAGE1
fi

# Stage 2 (skippable for offline/quota)
if [ "$CODEX_SKIP" = "1" ]; then
    echo "[hook] CODEX_SKIP=1 -> Codex review skipped"
else
    PYTHONIOENCODING=utf-8 python tools/codex_pre_commit.py
    STAGE2=$?
    if [ $STAGE2 -eq 1 ]; then
        echo ""
        echo "[hook] Stage 2 (Codex) CRITICAL - commit blocked"
        echo "  Override: CODEX_SKIP=1 git commit ..."
        # Stage 3 still runs below (best-effort) before exit
    fi
fi

# Stage 3 - auto Codex inbox (best-effort, never blocks)
if [ -f "scalper-agent/tools/codex_auto_request.py" ]; then
    PYTHONIOENCODING=utf-8 python scalper-agent/tools/codex_auto_request.py \
        --agent scalper-agent --staged --from-hook || true
fi

# Respect Stage 2 block decision
if [ "${STAGE2:-0}" = "1" ]; then
    exit 1
fi

exit 0
HOOK_EOF

chmod +x "$HOOK_FILE"
echo "[install_codex_hook] installed: $HOOK_FILE"

# ----- optional verify -----
if [ "${1:-}" = "--verify" ]; then
    echo ""
    echo "[install_codex_hook] verify (dry-run, no commit, no push)..."
    PY_BIN="python"
    if ! command -v python >/dev/null 2>&1; then
        PY_BIN="python3"
    fi
    cd "$REPO_ROOT"
    PYTHONIOENCODING=utf-8 "$PY_BIN" scalper-agent/tools/codex_auto_request.py \
        --agent scalper-agent --staged --always --no-checks --dry-run \
        | head -20
    echo ""
    echo "[install_codex_hook] verify done (above is dry-run JSON head)."
fi

echo ""
echo "[install_codex_hook] done."
echo "  hook : $HOOK_FILE"
echo "  verify: bash scripts/install_codex_hook.sh --verify"
echo "  undo  : bash scripts/install_codex_hook.sh --uninstall"
