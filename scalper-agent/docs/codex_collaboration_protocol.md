# Codex Collaboration Protocol

This repository uses a small file-based protocol so local bots can ask Codex
for review without a human copy/paste relay.

## Directories

- `ops/codex_inbox/`: bot-created JSON review requests for Codex.
- `ops/codex_outbox/`: Codex-created Markdown review responses.
- `ops/approval_required/`: actions that must wait for the owner.

Runtime files in these folders are ignored by git. The folders themselves are
tracked with `.gitkeep`.

## Owner Approval Boundary

The protocol may automate review, reporting, and test summaries. It must not
automate live trading activation.

Owner approval is required for:

- `kill_switch=false`
- `PAPER_ONLY=false`
- `systemctl start`, `systemctl enable`, or `systemctl unmask`
- VPS deployment for live trading
- real KIS order paths
- force push, rebase, reset, or destructive file operations

## Create A Review Request

```bash
python tools/codex_collab.py request \
  --agent blog-bot \
  --title "Stock analysis QA change" \
  --summary "Changed legal filter wording and QA checks" \
  --risk medium \
  --check "python main.py --naver-style 005930 --dry-run"
```

The command writes a JSON file under `ops/codex_inbox/`.

Use `--dry-run` to print the request without writing a file.

## Create An Approval Item

```bash
python tools/codex_collab.py approval \
  --agent scalper-agent \
  --title "Start 5/28 paper rehearsal service" \
  --reason "Starting the trading bot service requires owner approval" \
  --action "systemctl start bodyhunter-bot.service"
```

The command writes a Markdown item under `ops/approval_required/`.

## Codex Review Response

```bash
python tools/codex_collab.py review \
  --request ops/codex_inbox/REQUEST.json \
  --verdict pass-with-notes \
  --summary "No blocking issue; one logging note remains" \
  --finding "Keep PAPER_ONLY true during rehearsal"
```

The command writes a Markdown response under `ops/codex_outbox/`.

## Queue Status

```bash
python tools/codex_collab.py status
```

## Minimal Flow

1. A bot changes code or prepares an operational action.
2. The bot writes `ops/codex_inbox/*.json` or `ops/approval_required/*.md`.
3. Codex reads the newest item and writes `ops/codex_outbox/*.md`.
4. The bot consumes the response.
5. Owner approval is still required for all live-trading gates.
