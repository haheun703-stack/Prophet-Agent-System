"""
Telegram Alert - 텔레그램 알림
================================
매매 체결, 손절, 일일 요약, 4D 리포트 등 알림 전송
"""

import os
import logging
from typing import Dict, List

logger = logging.getLogger('Scalper.Telegram')

# 텔레그램 메시지 최대 길이
TG_MAX_LEN = 4096


class TelegramAlert:
    """텔레그램 알림 전송"""

    def __init__(self, config: dict = None):
        if config:
            token = config['api_keys']['telegram_bot_token']
            chat_id = config['api_keys']['telegram_chat_id']
            # ${ENV_VAR} 패턴이면 환경변수에서 로드
            if token.startswith("${"):
                token = os.getenv(token[2:-1], "")
            if str(chat_id).startswith("${"):
                chat_id = os.getenv(str(chat_id)[2:-1], "")
            self.bot_token = token
            self.chat_id = str(chat_id)
            self.enabled = config.get('output', {}).get('telegram_alert', True)
            self.alert_on_trade = config.get('output', {}).get('alert_on_trade', True)
        else:
            # config 없이 .env에서 직접 로드
            self.bot_token = os.getenv('TELEGRAM_BOT_TOKEN', '')
            self.chat_id = os.getenv('TELEGRAM_CHAT_ID', '')
            self.enabled = bool(self.bot_token and self.chat_id)
            self.alert_on_trade = True

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

    def send_report(self, report_text: str) -> bool:
        """긴 리포트 전송 (4D 디스크법 등) - 자동 분할.

        모든 청크 전송 성공 시 True, 하나라도 실패/미설정 시 False (7/14 검수 M-1 —
        호출자가 발송 실패를 감지해 silent failure를 막게 함)."""
        if not self.enabled:
            return False

        # 4096자 제한 → 자동 분할
        chunks = self._split_message(report_text)
        all_ok = True
        for i, chunk in enumerate(chunks):
            if not self._send(chunk):
                all_ok = False
            if i < len(chunks) - 1:
                import time
                time.sleep(0.5)  # 순서 보장
        return all_ok

    def _split_message(self, text: str) -> List[str]:
        """텔레그램 4096자 제한에 맞춰 분할 (구분선 기준)"""
        if len(text) <= TG_MAX_LEN:
            return [text]

        chunks = []
        current = ""

        for line in text.split('\n'):
            if len(current) + len(line) + 1 > TG_MAX_LEN - 50:
                chunks.append(current)
                current = line + '\n'
            else:
                current += line + '\n'

        if current.strip():
            chunks.append(current)

        return chunks

    def _send(self, message: str) -> bool:
        """전송 성공 시 True. 미설정/실패 시 False (호출자가 실패 감지 가능·7/14 검수 M-1).

        반환값 추가는 하위호환 — 기존 호출자(send_trade 등)는 반환값을 무시하므로 동작 무변경."""
        if not self.enabled:
            return False
        if not self.bot_token or self.bot_token.startswith("YOUR_"):
            logger.info(f"[Telegram 미설정] {message[:80]}...")
            return False
        try:
            import requests
            url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
            payload = {
                'chat_id': self.chat_id,
                'text': message,
            }
            resp = requests.post(url, json=payload, timeout=15)
            if resp.status_code == 200:
                logger.debug("텔레그램 전송 완료")
                return True
            logger.warning(f"텔레그램 전송 실패: {resp.status_code}")
            return False
        except Exception as e:
            logger.warning(f"텔레그램 에러: {e}")
            return False
