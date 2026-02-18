"""
Risk Manager - 중앙 리스크 관리자
==================================
모든 주문의 최종 게이트키퍼.
어떤 주문이든 이 모듈의 승인을 받아야 실행됨.

서보성 절대 원칙:
  - 현금 비중 최소 10% ("무조건 절대 10은 안 깨")
  - 일일 손실 한도 초과 시 매매 중단
  - 종목당 최대 비중 30%
  - 최대 동시 보유 5종목
"""

import logging
from typing import Tuple, Optional

from strategies.base_strategy import TradeSignal, Signal
from engine.portfolio import Portfolio
from risk.position_sizer import PositionSizer
from risk.daily_guard import DailyGuard

logger = logging.getLogger('Scalper.Risk')


class RiskManager:
    """중앙 리스크 관리자"""

    def __init__(self, config: dict, portfolio: Portfolio, daily_guard: DailyGuard):
        self.config = config
        self.portfolio = portfolio
        self.daily_guard = daily_guard
        self.sizer = PositionSizer(config)

        risk = config['risk']
        self.min_cash_ratio = risk['min_cash_ratio']        # 0.10
        self.max_positions = risk['max_positions']           # 5
        self.max_position_ratio = risk['max_position_ratio'] # 0.30
        self.stop_loss_pct = risk['stop_loss_pct']           # 0.02
        self.take_profit_pct = risk['take_profit_pct']       # 0.05

    def approve_buy(self, signal: TradeSignal, current_price: int) -> Tuple[bool, int, str]:
        """
        매수 주문 승인/거부

        Returns:
            (approved, quantity, reason)
        """
        # 1. 일일 손실 한도 확인
        if not self.daily_guard.is_trading_allowed():
            return False, 0, f"일일 한도 초과: {self.daily_guard.lock_reason}"

        # 2. 최대 보유 종목 수 확인
        if not self.portfolio.has_position(signal.code):
            if self.portfolio.position_count >= self.max_positions:
                return False, 0, f"최대 보유 종목 수 초과 ({self.max_positions})"

        # 3. 현금 비율 확인
        if self.portfolio.cash_ratio < self.min_cash_ratio + 0.01:
            return False, 0, f"현금 부족 (현재 {self.portfolio.cash_ratio:.1%}, 최소 {self.min_cash_ratio:.0%})"

        # 4. 종목 비중 확인
        existing = self.portfolio.get_position(signal.code)
        if existing:
            current_ratio = existing.eval_amount / self.portfolio.total_eval
            if current_ratio >= self.max_position_ratio:
                return False, 0, f"종목 비중 초과 ({current_ratio:.1%} >= {self.max_position_ratio:.0%})"

        # 5. 수량 계산
        stop_loss = signal.stop_loss
        if stop_loss == 0:
            stop_loss = int(current_price * (1 - self.stop_loss_pct))

        qty = self.sizer.calc_quantity(
            available_cash=self.portfolio.cash,
            total_eval=self.portfolio.total_eval,
            price=current_price,
            stop_loss=stop_loss,
            confidence=signal.confidence,
        )

        if qty <= 0:
            return False, 0, "계산된 주문 수량 0"

        return True, qty, f"승인: {qty}주 @ {current_price:,} (신뢰도 {signal.confidence:.2f})"

    def approve_sell(self, signal: TradeSignal) -> Tuple[bool, int, str]:
        """
        매도 주문 승인/거부

        Returns:
            (approved, quantity, reason)
        """
        pos = self.portfolio.get_position(signal.code)
        if pos is None or pos.quantity <= 0:
            return False, 0, "보유 종목 없음"

        return True, pos.quantity, f"매도 승인: {pos.quantity}주"

    def check_stop_loss(self, code: str, current_price: int) -> Optional[TradeSignal]:
        """손절 확인"""
        pos = self.portfolio.get_position(code)
        if pos is None:
            return None

        if pos.stop_loss > 0 and current_price <= pos.stop_loss:
            loss_pct = (current_price - pos.avg_price) / pos.avg_price * 100
            return TradeSignal(
                signal=Signal.SELL,
                code=code,
                confidence=1.0,
                reason=f"손절 ({current_price:,} <= {pos.stop_loss:,}, {loss_pct:+.1f}%)",
                strategy_name="StopLoss",
            )
        return None

    def check_take_profit(self, code: str, current_price: int) -> Optional[TradeSignal]:
        """익절 확인"""
        pos = self.portfolio.get_position(code)
        if pos is None:
            return None

        if pos.take_profit > 0 and current_price >= pos.take_profit:
            gain_pct = (current_price - pos.avg_price) / pos.avg_price * 100
            return TradeSignal(
                signal=Signal.SELL,
                code=code,
                confidence=1.0,
                reason=f"익절 ({current_price:,} >= {pos.take_profit:,}, {gain_pct:+.1f}%)",
                strategy_name="TakeProfit",
            )
        return None

    def calc_default_exits(self, price: int) -> Tuple[int, int]:
        """기본 손절/익절가 계산"""
        stop_loss = int(price * (1 - self.stop_loss_pct))
        take_profit = int(price * (1 + self.take_profit_pct))
        return stop_loss, take_profit
