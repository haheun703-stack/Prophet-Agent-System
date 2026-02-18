"""
Base Strategy - 매매 전략 추상 베이스
======================================
모든 전략의 공통 인터페이스와 TradeSignal 정의
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Dict, Optional

import pandas as pd


class Signal(Enum):
    BUY = "buy"
    SELL = "sell"
    HOLD = "hold"


@dataclass
class TradeSignal:
    """매매 신호 데이터"""
    signal: Signal
    code: str
    confidence: float           # 0.0 ~ 1.0
    reason: str
    strategy_name: str = ""
    suggested_price: int = 0    # 0 = 시장가
    stop_loss: int = 0          # 손절가
    take_profit: int = 0        # 익절가
    timestamp: datetime = field(default_factory=datetime.now)

    @property
    def is_buy(self) -> bool:
        return self.signal == Signal.BUY

    @property
    def is_sell(self) -> bool:
        return self.signal == Signal.SELL


class BaseStrategy(ABC):
    """매매 전략 추상 베이스 클래스"""

    def __init__(self, config: dict):
        self.config = config
        self.name = self.__class__.__name__

    @abstractmethod
    def evaluate(self, code: str, candles: pd.DataFrame,
                 tick_data: Optional[Dict] = None,
                 orderbook: Optional[Dict] = None) -> Optional[TradeSignal]:
        """
        현재 시장 데이터를 평가하여 매매 신호 생성

        Args:
            code: 종목코드
            candles: 분봉 DataFrame [timestamp, open, high, low, close, volume]
            tick_data: 최신 틱 정보
            orderbook: 현재 호가 정보

        Returns:
            TradeSignal 또는 None (신호 없음)
        """
        pass

    @abstractmethod
    def get_required_candle_count(self) -> int:
        """이 전략이 필요로 하는 최소 분봉 수"""
        pass

    def _make_signal(self, signal: Signal, code: str, confidence: float,
                     reason: str, **kwargs) -> TradeSignal:
        """편의 메서드: TradeSignal 생성"""
        return TradeSignal(
            signal=signal,
            code=code,
            confidence=confidence,
            reason=reason,
            strategy_name=self.name,
            **kwargs
        )
