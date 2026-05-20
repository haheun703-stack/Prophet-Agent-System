#!/bin/bash
# 자비스 모듈 수동 실행 wrapper — sys.path 자동 설정
#
# 사용:
#   ./tools/run_jarvis_module.sh data.surge_pattern_learner
#   ./tools/run_jarvis_module.sh data.brain_state_builder
#   ./tools/run_jarvis_module.sh data.trade_style_decider
#
# cron 등에서도 사용 가능 (PYTHONPATH 정상 설정 보장).

set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCALPER_DIR="$(dirname "$SCRIPT_DIR")"
BOT_DIR="$(dirname "$SCALPER_DIR")"
VENV_PY="${BOT_DIR}/venv/bin/python3.11"

if [ -z "$1" ]; then
    echo "Usage: $0 <module.name> [args...]"
    echo "예: $0 data.surge_pattern_learner"
    exit 1
fi

cd "$SCALPER_DIR"
exec "$VENV_PY" -m "$1" "${@:2}"
