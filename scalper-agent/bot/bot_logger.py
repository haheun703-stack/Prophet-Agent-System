"""
bot_logger.py — 텔레그램 전송 대체 로그 시스템

텔레그램 메시지를 제거하면서도 모든 활동을 추적할 수 있도록
일별 로그 파일에 구조화된 이벤트를 기록.

사용:
    from bot.bot_logger import log_event
    log_event("TRADE_BUY", "삼성전자 1/3 체결", {"price": 56000, "qty": 10})
"""
import json
import os
from datetime import datetime, date, timedelta
from pathlib import Path

# ── 로그 디렉토리 ──
LOG_DIR = Path(__file__).resolve().parent.parent / "logs"
LOG_DIR.mkdir(exist_ok=True)

# ── 허용 카테고리 ──
CATEGORIES = {
    "GLOBAL_EVENT", "OPTIONS", "GUARDIAN", "PREMIUM", "UNIVERSE",
    "PREMOVE", "RECOMMEND", "BRAIN", "SIGNAL", "GAP", "OPENING_RANGE",
    "PORTFOLIO", "TRADE_BUY", "TRADE_SELL", "TV_SCAN", "MACD",
    "COLLECT", "LEARNER", "PAPER", "PRECLOSE", "WAR", "EMERGENCY",
    "SYSTEM", "CLOSING", "MORNING",
}


def log_event(category: str, message: str, data=None):
    """이벤트를 일별 로그 파일에 기록.

    Args:
        category: 이벤트 카테고리 (CATEGORIES 중 하나)
        message: 사람이 읽을 수 있는 메시지
        data: 추가 데이터 (dict/list — JSON 직렬화됨)

    포맷: HH:MM:SS [CATEGORY] message | {data json}
    """
    now = datetime.now()
    ts = now.strftime("%H:%M:%S")
    today = now.strftime("%Y-%m-%d")

    # 카테고리 정규화
    cat = category.upper()
    if cat not in CATEGORIES:
        cat = "SYSTEM"

    # 데이터 직렬화
    data_str = ""
    if data is not None:
        try:
            data_str = " | " + json.dumps(data, ensure_ascii=False, default=str)
        except Exception:
            data_str = f" | {data}"

    line = f"{ts} [{cat}] {message}{data_str}\n"

    log_path = LOG_DIR / f"{today}_bot.log"
    try:
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(line)
    except Exception:
        pass  # 로그 실패가 봇을 멈추면 안 됨


def read_today_log(n_lines: int = 20) -> str:
    """오늘 로그 최근 n줄 반환 (온디맨드 명령어용)"""
    today = date.today().strftime("%Y-%m-%d")
    log_path = LOG_DIR / f"{today}_bot.log"
    if not log_path.exists():
        return "오늘 로그 없음"
    try:
        with open(log_path, "r", encoding="utf-8") as f:
            lines = f.readlines()
        return "".join(lines[-n_lines:])
    except Exception as e:
        return f"로그 읽기 실패: {e}"


def cleanup_old_logs(keep_days: int = 30):
    """30일 이전 로그 파일 자동 삭제"""
    cutoff = date.today() - timedelta(days=keep_days)
    for f in LOG_DIR.glob("*_bot.log"):
        try:
            # 파일명에서 날짜 추출: YYYY-MM-DD_bot.log
            date_str = f.stem.replace("_bot", "")
            file_date = datetime.strptime(date_str, "%Y-%m-%d").date()
            if file_date < cutoff:
                f.unlink()
        except (ValueError, OSError):
            pass
