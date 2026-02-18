"""
MA Crossover Strategy - 이평선 교차 전략
==========================================
서보성: "이평선이라든지 추세선이라든지... 차트 보고 비중 조절"

5일/20일 이동평균선 교차 + 거래량 확인
골든크로스(매수), 데드크로스(매도)
"""

import logging
from typing import Dict, Optional

import pandas as pd

from strategies.base_strategy import BaseStrategy, Signal, TradeSignal
from data.indicator_calc import IndicatorCalc

logger = logging.getLogger('Scalper.Strategy.MA')


class MACrossoverStrategy(BaseStrategy):
    """이평선 교차 매매 전략"""

    def __init__(self, config: dict):
        super().__init__(config)
        params = config['strategies']['ma_crossover']
        self.fast_period = params['fast_period']      # 5
        self.slow_period = params['slow_period']      # 20
        self.min_vol_ratio = params['min_volume_ratio']  # 1.5
        self.weight = params.get('weight', 0.4)

    def evaluate(self, code: str, candles: pd.DataFrame,
                 tick_data: Optional[Dict] = None,
                 orderbook: Optional[Dict] = None) -> Optional[TradeSignal]:

        if len(candles) < self.get_required_candle_count():
            return None

        close = candles['close']
        volume = candles['volume']

        # 이평선 계산
        fast_ma = IndicatorCalc.sma(close, self.fast_period)
        slow_ma = IndicatorCalc.sma(close, self.slow_period)

        # 거래량 비율
        vol_ratio = IndicatorCalc.volume_ratio(volume, 20)
        current_vol_ratio = vol_ratio.iloc[-1] if not vol_ratio.empty else 0

        # 교차 판단
        cross = IndicatorCalc.ma_crossover_signal(fast_ma, slow_ma)

        if cross == 'golden_cross' and current_vol_ratio >= self.min_vol_ratio:
            # 골든크로스 + 거래량 확인 → 매수
            current_price = int(close.iloc[-1])
            stop_loss = int(slow_ma.iloc[-1])  # 20MA 하회 시 손절

            confidence = min(0.5 + (current_vol_ratio - self.min_vol_ratio) * 0.1, 1.0)

            return self._make_signal(
                Signal.BUY, code, confidence,
                f"골든크로스 (MA{self.fast_period}>{self.slow_period}) 거래량 {current_vol_ratio:.1f}x",
                stop_loss=stop_loss,
            )

        elif cross == 'dead_cross':
            # 데드크로스 → 매도
            confidence = 0.6
            return self._make_signal(
                Signal.SELL, code, confidence,
                f"데드크로스 (MA{self.fast_period}<{self.slow_period})",
            )

        # 추가: 가격이 20MA 크게 이탈 시 매도
        if not pd.isna(slow_ma.iloc[-1]) and close.iloc[-1] > 0:
            deviation = (close.iloc[-1] - slow_ma.iloc[-1]) / slow_ma.iloc[-1] * 100
            if deviation < -3:  # 20MA 대비 3% 하락
                return self._make_signal(
                    Signal.SELL, code, 0.5,
                    f"20MA 이탈 ({deviation:.1f}%)",
                )

        return None

    def get_required_candle_count(self) -> int:
        return self.slow_period + 5
