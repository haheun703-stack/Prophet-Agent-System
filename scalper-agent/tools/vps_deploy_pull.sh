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
#   - ★06:00 봇 자동재시작★(7/1 사장님 승인) — 06:00 호출 시 봇 재시작해 항상 최신 코드 상주.
#     (deploy_pull이 코드만 pull하고 봇 옛코드 상주하던 문제 해소 — 6/28 상주→nightwatch stale).
#     17:45 호출(nightly 직전)은 재시작 안 함. 06:00은 장전 안전윈도우라 봇 OFF든 향후 ON이든 안전.
#
# 배포: 이 파일을 VPS의 /home/ubuntu/deploy_pull.sh 로 복사(repo 밖 — pull이 스크립트 자체를
#       건드리지 않도록) 후 chmod +x. cron: 17:45 평일(nightly 직전) + 06:00 매일.
#         45 17 * * 1-5 /home/ubuntu/deploy_pull.sh
#         0  6 * * *   /home/ubuntu/deploy_pull.sh
set -u
REPO=/home/ubuntu/bodyhunter
LOG=/home/ubuntu/bodyhunter/logs/deploy_pull.log
RUNTIME="scalper-agent/data/kill_switch.json scalper-agent/data/trade_runtime_config.json scalper-agent/data_store/universe.json"
HOUR=$(date +%H)   # ★ 진입 즉시 캡처 — pull 지연으로 시각이 넘어가도 06시 호출 보존 (code-analyzer M1)

ts() { date '+%Y-%m-%d %H:%M:%S'; }
cd "$REPO" 2>/dev/null || { echo "[$(ts)] ERR: repo 없음" >> "$LOG"; exit 0; }
mkdir -p "$(dirname "$LOG")"

{
  git fetch origin main 2>&1
  LOCAL=$(git rev-parse HEAD)
  REMOTE=$(git rev-parse origin/main)
  if [ "$LOCAL" = "$REMOTE" ]; then
    echo "[$(ts)] up-to-date ($LOCAL)"
  else
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
  fi

  # ★ 06시(새벽 안전윈도우) 봇 자동재시작 — 봇이 항상 최신 코드로 상주하도록 (7/1 사장님 승인)
  #   배경: deploy_pull이 코드만 pull하고 봇 재시작은 안 해서 봇이 옛 코드로 상주하던 문제.
  #         (6/28 상주 → 6/30 nightwatch 가드 미반영 → NXT stale). 06:00은 장전 안전윈도우라
  #         봇 OFF(실매매0)든 향후 ON이든 안전(재시작 중 매매 이벤트 없음·포지션은 파일 복원).
  #   17:45 호출(HOUR=17)은 재시작 안 함 — nightly 18:00 직전이라 데이터 코드만 갱신(현행 유지).
  if [ "$HOUR" = "06" ]; then
    if sudo systemctl restart bodyhunter-bot 2>&1; then
      echo "[$(ts)] bot restarted (06시 안전윈도우 — 최신코드 상주)"
    else
      echo "[$(ts)] WARN: bot restart 실패 — 수동 점검 필요"
    fi
  fi
  echo "[$(ts)] done"
} >> "$LOG" 2>&1
