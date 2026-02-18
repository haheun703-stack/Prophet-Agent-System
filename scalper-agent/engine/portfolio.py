"""
Portfolio - 보유 종목 및 손익 추적
====================================
실시간 포지션 관리, 손익 계산, 계좌 상태 추적
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Optional, List

logger = logging.getLogger('Scalper.Portfolio')


@dataclass
class Position:
    """개별 보유 종목"""
    code: str
    name: str
    quantity: int
    avg_price: int
    current_price: int = 0
    stop_loss: int = 0
    take_profit: int = 0
    entry_time: datetime = field(default_factory=datetime.now)

    @property
    def eval_amount(self) -> int:
        return self.current_price * self.quantity

    @property
    def purchase_amount(self) -> int:
        return self.avg_price * self.quantity

    @property
    def unrealized_pnl(self) -> int:
        return self.eval_amount - self.purchase_amount

    @property
    def unrealized_pnl_rate(self) -> float:
        if self.purchase_amount == 0:
            return 0.0
        return self.unrealized_pnl / self.purchase_amount * 100

    @property
    def hit_stop_loss(self) -> bool:
        return self.stop_loss > 0 and self.current_price <= self.stop_loss

    @property
    def hit_take_profit(self) -> bool:
        return self.take_profit > 0 and self.current_price >= self.take_profit


class Portfolio:
    """계좌 포트폴리오 관리"""

    def __init__(self, config: dict):
        self.config = config
        self.positions: Dict[str, Position] = {}
        self.cash: int = 0
        self.initial_cash: int = 0
        self.realized_pnl_today: int = 0

    def init_from_config(self):
        """config에서 초기 자금 로드 (모의매매용)"""
        self.cash = self.config['backtest']['initial_cash']
        self.initial_cash = self.cash

    def init_from_account(self, deposit: int, positions: List[Dict]):
        """키움 계좌 정보로 초기화 (실매매용)"""
        self.cash = deposit
        self.initial_cash = deposit
        for p in positions:
            self.positions[p['code']] = Position(
                code=p['code'],
                name=p.get('name', ''),
                quantity=p['quantity'],
                avg_price=p['avg_price'],
                current_price=p.get('current_price', p['avg_price']),
            )

    # === 포지션 관리 ===

    def add_position(self, code: str, name: str, qty: int, price: int,
                     stop_loss: int = 0, take_profit: int = 0):
        """매수 체결 반영"""
        cost = price * qty

        if code in self.positions:
            pos = self.positions[code]
            total_cost = pos.avg_price * pos.quantity + cost
            pos.quantity += qty
            pos.avg_price = total_cost // pos.quantity if pos.quantity > 0 else 0
            pos.current_price = price
            if stop_loss:
                pos.stop_loss = stop_loss
            if take_profit:
                pos.take_profit = take_profit
        else:
            self.positions[code] = Position(
                code=code, name=name, quantity=qty,
                avg_price=price, current_price=price,
                stop_loss=stop_loss, take_profit=take_profit,
            )

        self.cash -= cost
        logger.info(f"매수 반영: {name}({code}) {qty}주 @ {price:,} / 잔여현금: {self.cash:,}")

    def remove_position(self, code: str, qty: int, price: int):
        """매도 체결 반영"""
        if code not in self.positions:
            logger.warning(f"매도 시도: {code} 보유 없음")
            return

        pos = self.positions[code]
        pnl = (price - pos.avg_price) * qty
        self.realized_pnl_today += pnl
        self.cash += price * qty

        pos.quantity -= qty
        if pos.quantity <= 0:
            del self.positions[code]
            logger.info(f"매도 완료: {code} 전량 처분, 실현손익={pnl:+,}")
        else:
            logger.info(f"매도 반영: {code} {qty}주 @ {price:,}, 잔여={pos.quantity}주, 실현손익={pnl:+,}")

    def update_price(self, code: str, price: int):
        """실시간 가격 갱신"""
        if code in self.positions:
            self.positions[code].current_price = price

    # === 조회 ===

    def get_position(self, code: str) -> Optional[Position]:
        return self.positions.get(code)

    def has_position(self, code: str) -> bool:
        return code in self.positions

    @property
    def position_count(self) -> int:
        return len(self.positions)

    @property
    def total_eval(self) -> int:
        """총 평가금액 (현금 + 주식)"""
        stock_value = sum(p.eval_amount for p in self.positions.values())
        return self.cash + stock_value

    @property
    def stock_value(self) -> int:
        return sum(p.eval_amount for p in self.positions.values())

    @property
    def cash_ratio(self) -> float:
        """현금 비율"""
        total = self.total_eval
        return self.cash / total if total > 0 else 1.0

    @property
    def total_unrealized_pnl(self) -> int:
        return sum(p.unrealized_pnl for p in self.positions.values())

    @property
    def total_pnl_today(self) -> int:
        return self.realized_pnl_today + self.total_unrealized_pnl

    def get_positions_hitting_stop(self) -> List[Position]:
        """손절가에 도달한 포지션들"""
        return [p for p in self.positions.values() if p.hit_stop_loss]

    def get_positions_hitting_tp(self) -> List[Position]:
        """익절가에 도달한 포지션들"""
        return [p for p in self.positions.values() if p.hit_take_profit]

    def get_summary(self) -> Dict:
        """포트폴리오 요약"""
        return {
            'cash': self.cash,
            'stock_value': self.stock_value,
            'total_eval': self.total_eval,
            'cash_ratio': self.cash_ratio,
            'position_count': self.position_count,
            'unrealized_pnl': self.total_unrealized_pnl,
            'realized_pnl': self.realized_pnl_today,
            'total_pnl': self.total_pnl_today,
        }

    def reset_daily(self):
        """일일 리셋 (실현손익 초기화)"""
        self.realized_pnl_today = 0
