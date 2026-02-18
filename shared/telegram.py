"""
Shared Telegram Utility
========================
prophet-agent와 scalper-agent 공용 텔레그램 전송
"""

import logging
import requests

logger = logging.getLogger('Shared.Telegram')


def send_telegram(bot_token: str, chat_id: str, message: str,
                  parse_mode: str = 'HTML') -> bool:
    """텔레그램 메시지 전송"""
    if not bot_token or bot_token.startswith("YOUR_"):
        logger.info(f"[Telegram 미설정] {message}")
        return False

    try:
        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        resp = requests.post(url, json={
            'chat_id': chat_id,
            'text': message,
            'parse_mode': parse_mode,
        }, timeout=10)
        return resp.status_code == 200
    except Exception as e:
        logger.warning(f"텔레그램 에러: {e}")
        return False
