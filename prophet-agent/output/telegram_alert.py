"""
📢 Telegram Alert - 예언자 알림 시스템
=======================================
포물선 임박 종목 발견 시 텔레그램 알림 전송
"""

import logging
from typing import Dict
import pandas as pd

logger = logging.getLogger('Prophet.Telegram')


class TelegramAlert:
    """텔레그램 예언 알림"""
    
    def __init__(self, config: dict):
        self.config = config
        self.bot_token = config['api_keys']['telegram_bot_token']
        self.chat_id = config['api_keys']['telegram_chat_id']
        self.enabled = config['output']['telegram_alert']
    
    def send_prophet_alert(self, hot_stocks: pd.DataFrame):
        """
        포물선 임박 종목 알림 전송
        """
        if not self.enabled:
            logger.info("텔레그램 알림 비활성화 상태")
            return
        
        message = self._format_alert(hot_stocks)
        self._send(message)
    
    def send_single_alert(self, result: Dict):
        """단일 종목 예언 결과 알림"""
        if not self.enabled:
            return
        
        score = result['prophet_score']
        name = result['name']
        ticker = result['ticker']
        verdict = result['verdict']
        
        lines = [
            f"🔮 *예언자 알림*",
            f"",
            f"*{name}* ({ticker})",
            f"점수: *{score}점*",
            f"판정: {verdict}",
            f"",
        ]
        
        for key, signal in result.get('signals', {}).items():
            s = signal.get('score', 0)
            d = signal.get('detail', '')
            lines.append(f"  {'🟢' if s > 0 else '🔴' if s < 0 else '⚪'} {key}: {s:+d} | {d}")
        
        message = '\n'.join(lines)
        self._send(message)
    
    def _format_alert(self, df: pd.DataFrame) -> str:
        """DataFrame → 텔레그램 메시지 포맷"""
        lines = [
            f"🔮 *Prophet Agent - 포물선 임박 종목*",
            f"",
        ]
        
        for _, row in df.iterrows():
            ticker = row.get('ticker', '?')
            name = row.get('name', '?')
            score = row.get('prophet_score', 0)
            verdict = row.get('verdict', '')
            lines.append(f"  *{name}* ({ticker}): {score}점 {verdict}")
        
        lines.append(f"\n_상세 분석은 시스템에서 확인_")
        return '\n'.join(lines)
    
    def _send(self, message: str):
        """텔레그램 메시지 전송"""
        try:
            import requests
            
            url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
            payload = {
                'chat_id': self.chat_id,
                'text': message,
                'parse_mode': 'Markdown',
            }
            
            resp = requests.post(url, json=payload, timeout=10)
            if resp.status_code == 200:
                logger.info("텔레그램 알림 전송 완료")
            else:
                logger.warning(f"텔레그램 전송 실패: {resp.status_code}")
                
        except Exception as e:
            logger.warning(f"텔레그램 전송 에러: {e}")
