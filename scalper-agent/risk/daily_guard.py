"""
Daily Guard - 일일 손실 한도 서킷 브레이커
=============================================
서보성: "무조건 절대 10은 안 깨" / 일일 최대 손실 한도 초과 시 매매 중단
"""

import logging
from datetime import datetime, date

logger = logging.getLogger('Scalper.Risk.Guard')


class DailyGuard:
    """일일 손실 한도 관리"""

    def __init__(self, config: dict):
        risk = config['risk']
        self.daily_loss_limit = risk['daily_loss_limit']  # 절대값 (원)
        self.realized_pnl: int = 0
        self.unrealized_pnl: int = 0
        self.is_locked: bool = False
        self._lock_reason: str = ""
        self._today: date = date.today()
        self.trade_count: int = 0

    def record_trade(self, pnl: int):
        """체결 완료된 매매 손익 기록"""
        self._check_date_reset()
        self.realized_pnl += pnl
        self.trade_count += 1
        self._check_limit()

    def update_unrealized(self, pnl: int):
        """미실현 손익 갱신"""
        self.unrealized_pnl = pnl
        self._check_limit()

    def is_trading_allowed(self) -> bool:
        """매매 허용 여부"""
        self._check_date_reset()
        return not self.is_locked

    @property
    def lock_reason(self) -> str:
        return self._lock_reason

    @property
    def total_pnl(self) -> int:
        return self.realized_pnl + self.unrealized_pnl

    @property
    def remaining_loss_budget(self) -> int:
        """남은 손실 허용액"""
        return max(0, self.daily_loss_limit + self.total_pnl)

    def _check_limit(self):
        """한도 초과 확인"""
        if self.total_pnl <= -self.daily_loss_limit:
            if not self.is_locked:
                self.is_locked = True
                self._lock_reason = (
                    f"일일 손실 한도 초과: {self.total_pnl:+,}원 "
                    f"(한도: -{self.daily_loss_limit:,}원)"
                )
                logger.warning(f"매매 중단! {self._lock_reason}")

    def _check_date_reset(self):
        """날짜 변경 시 자동 리셋"""
        today = date.today()
        if today != self._today:
            self.reset()
            self._today = today

    def reset(self):
        """일일 리셋"""
        self.realized_pnl = 0
        self.unrealized_pnl = 0
        self.is_locked = False
        self._lock_reason = ""
        self.trade_count = 0
        logger.info("DailyGuard 리셋")

    def get_summary(self) -> dict:
        return {
            'realized_pnl': self.realized_pnl,
            'unrealized_pnl': self.unrealized_pnl,
            'total_pnl': self.total_pnl,
            'is_locked': self.is_locked,
            'lock_reason': self._lock_reason,
            'trade_count': self.trade_count,
            'remaining_budget': self.remaining_loss_budget,
        }
