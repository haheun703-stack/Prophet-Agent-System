"""
Paper Trader - 모의매매 시뮬레이터
====================================
실제 주문 없이 시뮬레이션 매매 실행
KiwoomOrder와 동일한 인터페이스로 seamless 전환
"""

import logging
from datetime import datetime
from typing import Dict, List, Optional

from engine.portfolio import Portfolio

logger = logging.getLogger('Scalper.Paper')


class PaperTrader:
    """모의매매 시뮬레이터"""

    def __init__(self, config: dict, portfolio: Portfolio):
        self.config = config
        self.portfolio = portfolio
        self.slippage_bps = config['backtest']['slippage_bps']    # 10bp
        self.commission_bps = config['backtest']['commission_bps']  # 15bp
        self._order_counter = 0
        self._pending_orders: List[Dict] = []
        self._filled_orders: List[Dict] = []
        self._chejan_callbacks: list = []

    def on_chejan(self, callback):
        self._chejan_callbacks.append(callback)

    def buy_market(self, code: str, qty: int, current_price: int = 0) -> str:
        """시장가 매수 (즉시 체결)"""
        fill_price = self._apply_slippage(current_price, is_buy=True)
        order_no = self._next_order_no()

        order = {
            'order_no': order_no,
            'type': 'buy',
            'code': code,
            'qty': qty,
            'price': fill_price,
            'commission': self._calc_commission(fill_price * qty),
            'timestamp': datetime.now(),
            'status': 'filled',
        }

        name = self.config['watchlist'].get('codes_names', {}).get(code, code)
        self.portfolio.add_position(code, name, qty, fill_price)
        self._filled_orders.append(order)
        self._emit_chejan(order)

        logger.info(f"[PAPER] 매수 체결: {code} {qty}주 @ {fill_price:,}")
        return order_no

    def sell_market(self, code: str, qty: int, current_price: int = 0) -> str:
        """시장가 매도 (즉시 체결)"""
        fill_price = self._apply_slippage(current_price, is_buy=False)
        order_no = self._next_order_no()

        order = {
            'order_no': order_no,
            'type': 'sell',
            'code': code,
            'qty': qty,
            'price': fill_price,
            'commission': self._calc_commission(fill_price * qty),
            'timestamp': datetime.now(),
            'status': 'filled',
        }

        self.portfolio.remove_position(code, qty, fill_price)
        self._filled_orders.append(order)
        self._emit_chejan(order)

        logger.info(f"[PAPER] 매도 체결: {code} {qty}주 @ {fill_price:,}")
        return order_no

    def buy_limit(self, code: str, qty: int, price: int) -> str:
        """지정가 매수 (pending 등록)"""
        order_no = self._next_order_no()
        self._pending_orders.append({
            'order_no': order_no,
            'type': 'buy',
            'code': code,
            'qty': qty,
            'price': price,
            'timestamp': datetime.now(),
            'status': 'pending',
        })
        logger.info(f"[PAPER] 매수 대기: {code} {qty}주 @ {price:,}")
        return order_no

    def sell_limit(self, code: str, qty: int, price: int) -> str:
        """지정가 매도 (pending 등록)"""
        order_no = self._next_order_no()
        self._pending_orders.append({
            'order_no': order_no,
            'type': 'sell',
            'code': code,
            'qty': qty,
            'price': price,
            'timestamp': datetime.now(),
            'status': 'pending',
        })
        logger.info(f"[PAPER] 매도 대기: {code} {qty}주 @ {price:,}")
        return order_no

    def check_pending_fills(self, code: str, current_price: int):
        """체결 대기 주문 확인 (매 틱마다 호출)"""
        filled = []
        remaining = []

        for order in self._pending_orders:
            if order['code'] != code:
                remaining.append(order)
                continue

            if order['type'] == 'buy' and current_price <= order['price']:
                order['status'] = 'filled'
                name = code
                self.portfolio.add_position(code, name, order['qty'], order['price'])
                self._filled_orders.append(order)
                self._emit_chejan(order)
                filled.append(order)
            elif order['type'] == 'sell' and current_price >= order['price']:
                order['status'] = 'filled'
                self.portfolio.remove_position(code, order['qty'], order['price'])
                self._filled_orders.append(order)
                self._emit_chejan(order)
                filled.append(order)
            else:
                remaining.append(order)

        self._pending_orders = remaining

    def cancel_all(self):
        """모든 대기 주문 취소"""
        count = len(self._pending_orders)
        self._pending_orders.clear()
        logger.info(f"[PAPER] {count}건 주문 취소")

    def get_filled_today(self) -> List[Dict]:
        return self._filled_orders.copy()

    # === 내부 메서드 ===

    def _apply_slippage(self, price: int, is_buy: bool) -> int:
        """슬리피지 적용"""
        slippage = price * self.slippage_bps / 10000
        return int(price + slippage) if is_buy else int(price - slippage)

    def _calc_commission(self, amount: int) -> int:
        """수수료 계산"""
        return int(amount * self.commission_bps / 10000)

    def _next_order_no(self) -> str:
        self._order_counter += 1
        return f"PAPER{self._order_counter:06d}"

    def _emit_chejan(self, order: Dict):
        data = {
            'type': 'order',
            'order_no': order['order_no'],
            'code': order['code'],
            'order_status': 'filled',
            'filled_qty': order['qty'],
            'filled_price': order['price'],
            'remain_qty': 0,
            'order_type': '+매수' if order['type'] == 'buy' else '-매도',
            'timestamp': order['timestamp'],
        }
        for cb in self._chejan_callbacks:
            try:
                cb(data)
            except Exception as e:
                logger.error(f"체잔 콜백 에러: {e}")
