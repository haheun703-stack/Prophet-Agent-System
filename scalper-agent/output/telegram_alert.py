"""
Telegram Alert - 텔레그램 알림
================================
매매 체결, 손절, 일일 요약 등 알림 전송
prophet-agent의 telegram_alert.py 패턴 재사용
"""

import logging
from typing import Dict

logger = logging.getLogger('Scalper.Telegram')


class TelegramAlert:
    """텔레그램 알림 전송"""

    def __init__(self, config: dict):
        self.bot_token = config['api_keys']['telegram_bot_token']
        self.chat_id = config['api_keys']['telegram_chat_id']
        self.enabled = config['output']['telegram_alert']
        self.alert_on_trade = config['output'].get('alert_on_trade', True)

    def send_trade(self, trade: Dict):
        """매매 체결 알림"""
        if not self.enabled or not self.alert_on_trade:
            return

        side = "매수" if trade.get('type') == 'buy' or '+' in trade.get('order_type', '') else "매도"
        code = trade.get('code', '?')
        qty = trade.get('filled_qty', trade.get('qty', 0))
        price = trade.get('filled_price', trade.get('price', 0))

        msg = (
            f"{'[PAPER] ' if trade.get('order_no', '').startswith('PAPER') else ''}"
            f"{side} 체결\n"
            f"종목: {code}\n"
            f"수량: {qty}주\n"
            f"가격: {price:,}원"
        )
        self._send(msg)

    def send_stop_loss(self, code: str, qty: int, price: int, loss: int):
        """손절 알림"""
        if not self.enabled:
            return
        msg = (
            f"손절 실행\n"
            f"종목: {code}\n"
            f"수량: {qty}주 @ {price:,}원\n"
            f"손실: {loss:+,}원"
        )
        self._send(msg)

    def send_risk_alert(self, message: str):
        """리스크 알림 (일일 한도 등)"""
        if not self.enabled:
            return
        self._send(f"[리스크] {message}")

    def send_daily_summary(self, summary: Dict):
        """일일 요약"""
        if not self.enabled:
            return
        msg = (
            f"일일 매매 요약\n"
            f"{'='*25}\n"
            f"총 평가: {summary.get('total_eval', 0):,}원\n"
            f"현금: {summary.get('cash', 0):,} ({summary.get('cash_ratio', 0):.1%})\n"
            f"실현손익: {summary.get('realized_pnl', 0):+,}원\n"
            f"미실현손익: {summary.get('unrealized_pnl', 0):+,}원\n"
            f"총 손익: {summary.get('total_pnl', 0):+,}원\n"
            f"매매 횟수: {summary.get('trade_count', 0)}회\n"
            f"보유 종목: {summary.get('position_count', 0)}개"
        )
        self._send(msg)

    def _send(self, message: str):
        if not self.enabled:
            return
        if self.bot_token.startswith("YOUR_"):
            logger.info(f"[Telegram 미설정] {message}")
            return
        try:
            import requests
            url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
            payload = {
                'chat_id': self.chat_id,
                'text': message,
                'parse_mode': 'HTML',
            }
            resp = requests.post(url, json=payload, timeout=10)
            if resp.status_code == 200:
                logger.debug("텔레그램 전송 완료")
            else:
                logger.warning(f"텔레그램 전송 실패: {resp.status_code}")
        except Exception as e:
            logger.warning(f"텔레그램 에러: {e}")
