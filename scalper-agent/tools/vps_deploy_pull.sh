#!/bin/bash
# vps_deploy_pull.sh — 노트북(개발) push → VPS(운영) 자동 동기화. 6/15 신설.
#
# 배경: 코드는 노트북에서 만들어 push하고 실행은 VPS(24/7 systemd)라, 노트북→VPS 배포가
#       수동이면 매번 "VPS 구버전"이 생긴다(6/15 H-3·F1이 노트북에만 있던 사례). 이 이원화가
#       "자꾸 덜 되어있고 다시 연결" 반복의 인프라 근본이라 자동 동기화로 종결.
#
# 안전:
#   - 런타임 상태 파일(kill_switch / trade_runtime_config / universe)을 stash 보존 후 pull.
#     이 파일들은 VPS 운영 상태(봇 OFF 스위치 등)라 origin이 덮으면 안 됨.
#   - git pull --ff-only (non-fast-forward면 중단 — 임의 머지 금지).
#   - ★봇 재시작 안 함★ — 데이터·관측 코드는 다음 nightly cron(18:00)이 새 코드로 자동 실행.
#     매매 코드 변경 반영(봇 재시작)은 사장님 승인 후 수동(안전윈도 20:00+/23:30~06:00).
#
# 배포: 이 파일을 VPS의 /home/ubuntu/deploy_pull.sh 로 복사(repo 밖 — pull이 스크립트 자체를
#       건드리지 않도록) 후 chmod +x. cron: 17:45 평일(nightly 직전) + 06:00 매일.
#         45 17 * * 1-5 /home/ubuntu/deploy_pull.sh
#         0  6 * * *   /home/ubuntu/deploy_pull.sh
set -u
REPO=/home/ubuntu/bodyhunter
LOG=/home/ubuntu/bodyhunter/logs/deploy_pull.log
RUNTIME="scalper-agent/data/kill_switch.json scalper-agent/data/trade_runtime_config.json scalper-agent/data_store/universe.json"

ts() { date '+%Y-%m-%d %H:%M:%S'; }
cd "$REPO" 2>/dev/null || { echo "[$(ts)] ERR: repo 없음" >> "$LOG"; exit 0; }
mkdir -p "$(dirname "$LOG")"

{
  git fetch origin main 2>&1
  LOCAL=$(git rev-parse HEAD)
  REMOTE=$(git rev-parse origin/main)
  if [ "$LOCAL" = "$REMOTE" ]; then
    echo "[$(ts)] up-to-date ($LOCAL)"
    exit 0
  fi
  echo "[$(ts)] update: $LOCAL -> $REMOTE"

  STASHED=0
  if ! git diff --quiet -- $RUNTIME 2>/dev/null; then
    if git stash push -m "deploy_pull runtime preserve" -- $RUNTIME 2>&1; then
      STASHED=1
      echo "[$(ts)] runtime stashed"
    fi
  fi

  if git pull --ff-only origin main 2>&1; then
    echo "[$(ts)] pull OK -> $(git rev-parse HEAD)"
  else
    echo "[$(ts)] pull FAILED (non-ff/conflict) — 수동 점검 필요"
  fi

  if [ "$STASHED" = "1" ]; then
    if git stash pop 2>&1; then
      echo "[$(ts)] runtime restored"
    else
      echo "[$(ts)] WARN: stash pop 충돌 — 런타임 파일 수동 점검(git stash list)"
    fi
  fi
  echo "[$(ts)] done (봇 재시작 안 함 — 코드만 갱신)"
} >> "$LOG" 2>&1
