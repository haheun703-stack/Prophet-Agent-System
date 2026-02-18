"""
Order Manager - 주문 생명주기 관리
====================================
주문 접수 → 체결/취소 추적
"""

import logging
from datetime import datetime
from typing import Dict, List, Optional

logger = logging.getLogger('Scalper.OrderMgr')


class OrderManager:
    """주문 상태 추적"""

    def __init__(self):
        self._orders: Dict[str, Dict] = {}  # order_no -> order info
        self._filled_today: List[Dict] = []

    def register(self, order_no: str, code: str, side: str,
                 qty: int, price: int):
        """주문 등록"""
        self._orders[order_no] = {
            'order_no': order_no,
            'code': code,
            'side': side,
            'qty': qty,
            'price': price,
            'filled_qty': 0,
            'filled_price': 0,
            'status': 'submitted',
            'submit_time': datetime.now(),
            'fill_time': None,
        }

    def on_chejan(self, data: Dict):
        """체잔 이벤트 처리"""
        order_no = data.get('order_no', '')
        if not order_no:
            return

        if order_no not in self._orders:
            # 미등록 주문 (수동 주문 등)
            self._orders[order_no] = {
                'order_no': order_no,
                'code': data.get('code', ''),
                'side': 'buy' if '+' in data.get('order_type', '') else 'sell',
                'qty': data.get('order_qty', 0),
                'price': data.get('order_price', 0),
                'filled_qty': 0,
                'filled_price': 0,
                'status': 'submitted',
                'submit_time': datetime.now(),
                'fill_time': None,
            }

        order = self._orders[order_no]
        order['filled_qty'] = data.get('filled_qty', order['filled_qty'])
        order['filled_price'] = data.get('filled_price', order['filled_price'])
        order['status'] = data.get('order_status', order['status'])

        remain = data.get('remain_qty', -1)
        if remain == 0 or order['status'] == 'filled':
            order['status'] = 'filled'
            order['fill_time'] = datetime.now()
            self._filled_today.append(order.copy())
            logger.info(
                f"체결 완료: {order['code']} {order['side']} "
                f"{order['filled_qty']}주 @ {order['filled_price']:,}"
            )

    def get_pending(self) -> List[Dict]:
        """미체결 주문 목록"""
        return [o for o in self._orders.values() if o['status'] == 'submitted']

    def get_filled_today(self) -> List[Dict]:
        return self._filled_today.copy()

    @property
    def pending_count(self) -> int:
        return len(self.get_pending())

    def reset_daily(self):
        self._orders.clear()
        self._filled_today.clear()
