"""
Market State - 시장 상태 관리
==============================
장 시간, 거래일, 동시호가 등 시장 상태 감지
"""

from datetime import datetime, time, date


class MarketState:
    """한국 주식시장 상태 관리"""

    # KRX 시간
    PRE_MARKET_START = time(8, 30)
    MARKET_OPEN = time(9, 0)
    MARKET_CLOSE = time(15, 20)
    CLOSING_AUCTION = time(15, 20)
    MARKET_END = time(15, 30)

    def __init__(self, config: dict):
        self.config = config
        market_cfg = config.get('market', {})
        self.close_before_end = market_cfg.get('close_all_before_end', True)
        self.close_minutes = market_cfg.get('close_minutes_before', 10)

    def now(self) -> datetime:
        return datetime.now()

    def is_trading_day(self) -> bool:
        """오늘이 거래일인지 (주말 제외, 공휴일은 미포함)"""
        return self.now().weekday() < 5

    def is_pre_market(self) -> bool:
        """동시호가 시간 (08:30~09:00)"""
        t = self.now().time()
        return self.PRE_MARKET_START <= t < self.MARKET_OPEN

    def is_market_open(self) -> bool:
        """정규 거래 시간 (09:00~15:20)"""
        t = self.now().time()
        return self.MARKET_OPEN <= t < self.MARKET_CLOSE

    def is_closing_auction(self) -> bool:
        """장마감 동시호가 (15:20~15:30)"""
        t = self.now().time()
        return self.CLOSING_AUCTION <= t < self.MARKET_END

    def should_close_positions(self) -> bool:
        """포지션 청산 시간인지 (장마감 N분 전)"""
        if not self.close_before_end:
            return False
        now = self.now()
        close_time = datetime.combine(now.date(), self.MARKET_CLOSE)
        threshold = close_time - __import__('datetime').timedelta(minutes=self.close_minutes)
        return now >= threshold and now < close_time

    def minutes_until_close(self) -> int:
        """장마감까지 남은 분"""
        now = self.now()
        close_dt = datetime.combine(now.date(), self.MARKET_CLOSE)
        delta = close_dt - now
        return max(0, int(delta.total_seconds() / 60))

    def can_trade(self) -> bool:
        """현재 매매 가능한 상태인지"""
        return self.is_trading_day() and self.is_market_open()
