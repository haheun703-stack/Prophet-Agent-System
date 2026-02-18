"""
Position Sizer - 포지션 사이징
===============================
서보성: "비중 조절하고 자리 좋다 좀 더 들어가고"

계좌 잔고, 리스크, 신뢰도 기반 주문 수량 계산
"""

import logging
from typing import Optional

logger = logging.getLogger('Scalper.Risk.Sizer')


class PositionSizer:
    """포지션 크기(주문 수량) 계산"""

    def __init__(self, config: dict):
        self.config = config
        risk = config['risk']
        self.min_cash_ratio = risk['min_cash_ratio']
        self.max_position_ratio = risk['max_position_ratio']
        self.max_loss_per_trade = risk['max_loss_per_trade_pct']

    def calc_quantity(self, available_cash: int, total_eval: int,
                      price: int, stop_loss: int = 0,
                      confidence: float = 0.5) -> int:
        """
        최적 주문 수량 계산

        Args:
            available_cash: 주문 가능 현금
            total_eval: 총 평가금액
            price: 현재가 (매수가)
            stop_loss: 손절가 (0이면 기본 비율 적용)
            confidence: 전략 신뢰도 (0.0~1.0)

        Returns:
            주문 수량 (0이면 주문 불가)
        """
        if price <= 0 or total_eval <= 0:
            return 0

        # 1. 현금 보유 비율 제한 (10% 현금 유지)
        min_cash = int(total_eval * self.min_cash_ratio)
        usable_cash = max(0, available_cash - min_cash)

        if usable_cash < price:
            logger.debug("현금 부족 (10% 유지 후)")
            return 0

        # 2. 종목당 최대 비중 제한 (30%)
        max_by_ratio = int(total_eval * self.max_position_ratio)
        max_by_ratio_qty = max_by_ratio // price

        # 3. 리스크 기반 사이징 (손절가가 있으면)
        max_by_risk_qty = 999999
        if stop_loss > 0 and stop_loss < price:
            risk_per_share = price - stop_loss
            max_loss_amount = int(total_eval * self.max_loss_per_trade)
            max_by_risk_qty = max_loss_amount // risk_per_share

        # 4. 현금 기반 최대 수량
        max_by_cash = usable_cash // price

        # 5. 최소값 선택
        base_qty = min(max_by_cash, max_by_ratio_qty, max_by_risk_qty)

        # 6. 신뢰도 반영 (0.5~1.0 → 50%~100% 수량)
        confidence_factor = 0.5 + confidence * 0.5
        final_qty = max(1, int(base_qty * confidence_factor))

        # 다시 현금 제한 확인
        final_qty = min(final_qty, usable_cash // price)

        logger.debug(
            f"사이징: cash={max_by_cash}, ratio={max_by_ratio_qty}, "
            f"risk={max_by_risk_qty}, conf={confidence:.2f} → {final_qty}주"
        )

        return max(0, final_qty)

    def calc_add_quantity(self, current_qty: int, current_avg: int,
                          available_cash: int, total_eval: int,
                          price: int) -> int:
        """추가 매수 수량 계산 (이미 보유 중일 때)"""
        current_value = current_qty * current_avg
        max_value = int(total_eval * self.max_position_ratio)
        remaining = max(0, max_value - current_value)
        return min(remaining // price, available_cash // price) if price > 0 else 0
