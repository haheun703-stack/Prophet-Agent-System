"""
Composite Strategy - 복합 전략 (가중 합산)
=============================================
여러 전략의 신호를 가중치로 합산하여 최종 신호 생성
Prophet Agent의 synthesizer 패턴과 동일한 접근
"""

import logging
from typing import Dict, List, Optional

import pandas as pd

from strategies.base_strategy import BaseStrategy, Signal, TradeSignal

logger = logging.getLogger('Scalper.Strategy.Composite')


class CompositeStrategy(BaseStrategy):
    """복합 전략: 여러 전략의 신호를 가중치로 결합"""

    def __init__(self, config: dict, strategies: List[BaseStrategy]):
        super().__init__(config)
        self.strategies = strategies
        self.min_confidence = config['strategies']['composite']['min_confidence']

        # 전략별 가중치 (config에 설정되지 않으면 균등 배분)
        total = sum(getattr(s, 'weight', 1.0) for s in strategies)
        self.weights = {
            s.name: getattr(s, 'weight', 1.0) / total
            for s in strategies
        }

    def evaluate(self, code: str, candles: pd.DataFrame,
                 tick_data: Optional[Dict] = None,
                 orderbook: Optional[Dict] = None) -> Optional[TradeSignal]:

        buy_score = 0.0
        sell_score = 0.0
        reasons = []

        for strategy in self.strategies:
            try:
                if len(candles) < strategy.get_required_candle_count():
                    continue

                signal = strategy.evaluate(code, candles, tick_data, orderbook)
                if signal is None:
                    continue

                weight = self.weights.get(strategy.name, 0.1)

                if signal.is_buy:
                    buy_score += signal.confidence * weight
                    reasons.append(f"[BUY] {strategy.name}: {signal.reason}")
                elif signal.is_sell:
                    sell_score += signal.confidence * weight
                    reasons.append(f"[SELL] {strategy.name}: {signal.reason}")

            except Exception as e:
                logger.warning(f"전략 {strategy.name} 평가 에러: {e}")
                continue

        # 최종 신호 결정
        reason_str = " | ".join(reasons) if reasons else "신호 없음"

        if buy_score >= self.min_confidence and buy_score > sell_score:
            # 매수 신호
            best_stop = self._get_best_stop_loss(code, candles)
            return self._make_signal(
                Signal.BUY, code, min(buy_score, 1.0),
                f"복합 매수 ({buy_score:.2f}) {reason_str}",
                stop_loss=best_stop,
            )

        elif sell_score >= self.min_confidence and sell_score > buy_score:
            # 매도 신호
            return self._make_signal(
                Signal.SELL, code, min(sell_score, 1.0),
                f"복합 매도 ({sell_score:.2f}) {reason_str}",
            )

        return None

    def _get_best_stop_loss(self, code: str, candles: pd.DataFrame) -> int:
        """하위 전략들의 손절가 중 가장 보수적인 값"""
        stop_losses = []
        for strategy in self.strategies:
            try:
                signal = strategy.evaluate(code, candles)
                if signal and signal.stop_loss > 0:
                    stop_losses.append(signal.stop_loss)
            except:
                continue
        return max(stop_losses) if stop_losses else 0

    def get_required_candle_count(self) -> int:
        if not self.strategies:
            return 30
        return max(s.get_required_candle_count() for s in self.strategies)
