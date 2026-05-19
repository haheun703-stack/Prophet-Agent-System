# -*- coding: utf-8 -*-
"""Verifier 팀 공통 헬퍼 — 텔레그램 알림 + 로거 + 상태 저장."""
import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

# 프로젝트 루트
_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))

logger = logging.getLogger(__name__)

# 상태 저장 경로
STATE_DIR = _ROOT / "data_store" / "verifiers"
STATE_DIR.mkdir(parents=True, exist_ok=True)


# ── 텔레그램 알림 ──────────────────────────────────────────
def send_telegram(message: str, parse_mode: Optional[str] = None) -> bool:
    """텔레그램 알림 전송. 실패 시 False."""
    try:
        from bot.telegram_bot import send_telegram_message
        send_telegram_message(message, parse_mode=parse_mode)
        return True
    except Exception as e:
        # 폴백: HTTP API 직접 호출
        try:
            import requests
            from dotenv import load_dotenv
            load_dotenv(_ROOT.parent / ".env")
            token = os.getenv("TELEGRAM_BOT_TOKEN", "")
            chat_id = os.getenv("TELEGRAM_CHAT_ID", "")
            if not token or not chat_id:
                logger.warning(f"텔레그램 전송 실패 (token/chat_id 없음): {e}")
                return False
            url = f"https://api.telegram.org/bot{token}/sendMessage"
            payload = {"chat_id": chat_id, "text": message}
            if parse_mode:
                payload["parse_mode"] = parse_mode
            r = requests.post(url, json=payload, timeout=10)
            return r.status_code == 200
        except Exception as e2:
            logger.warning(f"텔레그램 전송 실패 (HTTP fallback): {e2}")
            return False


# ── 상태 저장/로드 ─────────────────────────────────────────
def save_state(name: str, data: Dict[str, Any]) -> None:
    """검수 결과 상태 저장. {name}.json"""
    try:
        path = STATE_DIR / f"{name}.json"
        data = dict(data)
        data["_saved_at"] = datetime.now().isoformat()
        path.write_text(json.dumps(data, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    except Exception as e:
        logger.warning(f"save_state 실패 ({name}): {e}")


def load_state(name: str) -> Dict[str, Any]:
    """저장된 상태 로드. 없으면 {}."""
    try:
        path = STATE_DIR / f"{name}.json"
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        logger.warning(f"load_state 실패 ({name}): {e}")
    return {}


# ── 결과 표준 포맷 ──────────────────────────────────────────
def make_result(name: str, ok: bool, summary: str, details: Optional[Dict] = None) -> Dict:
    return {
        "verifier": name,
        "ok": ok,
        "summary": summary,
        "details": details or {},
        "checked_at": datetime.now().isoformat(),
    }


# ── 메시지 포맷 헬퍼 ────────────────────────────────────────
def fmt_alert(name: str, icon: str, lines: list) -> str:
    """텔레그램 메시지 포맷팅."""
    header = f"{icon} [{name}]"
    body = "\n".join(lines)
    return f"{header}\n{body}"
