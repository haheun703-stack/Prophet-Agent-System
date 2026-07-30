#!/bin/bash
# VPS 스왑 모니터 — 80% 초과 시 텔레그램 알림
# crontab: */10 * * * * /home/ubuntu/bodyhunter/tools/swap_monitor.sh

source /home/ubuntu/bodyhunter/.env

THRESHOLD=80
LOCKFILE="/tmp/swap_alert_sent"

# 스왑 사용률 계산
SWAP_TOTAL=$(free | awk '/Swap/ {print $2}')
SWAP_USED=$(free | awk '/Swap/ {print $3}')

[ "$SWAP_TOTAL" -eq 0 ] && exit 0

SWAP_PCT=$((SWAP_USED * 100 / SWAP_TOTAL))

if [ "$SWAP_PCT" -ge "$THRESHOLD" ]; then
    # 1시간 내 중복 알림 방지
    if [ -f "$LOCKFILE" ]; then
        LOCK_AGE=$(( $(date +%s) - $(stat -c %Y "$LOCKFILE") ))
        [ "$LOCK_AGE" -lt 3600 ] && exit 0
    fi

    MEM_USED=$(free -h | awk '/Mem/ {print $3}')
    MEM_TOTAL=$(free -h | awk '/Mem/ {print $2}')
    SWAP_USED_H=$(free -h | awk '/Swap/ {print $3}')
    SWAP_TOTAL_H=$(free -h | awk '/Swap/ {print $2}')

    MSG="⚠️ VPS 메모리 경고
━━━━━━━━━━━━━━
Swap: ${SWAP_PCT}% (임계: ${THRESHOLD}%)
RAM: ${MEM_USED}/${MEM_TOTAL}
Swap: ${SWAP_USED_H}/${SWAP_TOTAL_H}
━━━━━━━━━━━━━━
재부팅 권장: sudo reboot"

    curl -s -X POST "https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage" \
        -d chat_id="${TELEGRAM_CHAT_ID}" \
        -d text="${MSG}" > /dev/null 2>&1

    touch "$LOCKFILE"
fi

# 50% 이하 복구 시 락 해제
if [ "$SWAP_PCT" -lt 50 ] && [ -f "$LOCKFILE" ]; then
    rm -f "$LOCKFILE"
fi
