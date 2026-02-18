"""
Trend Breakout Strategy - 추세선 돌파 전략
=============================================
서보성: "220에서 돌파하길래 아 지금이다 지금 가겠다"

저항선 돌파 + 거래량 확인 시 매수
지지선 이탈 시 매도
"""

import logging
from typing import Dict, Optional

import pandas as pd
import numpy as np

from strategies.base_strategy import BaseStrategy, Signal, TradeSignal
from data.indicator_calc import IndicatorCalc

logger = logging.getLogger('Scalper.Strategy.Trend')


class TrendBreakoutStrategy(BaseStrategy):
    """추세선 돌파 매매 전략"""

    def __init__(self, config: dict):
        super().__init__(config)
        params = config['strategies']['trend_breakout']
        self.lookback = params['lookback_candles']             # 60
        self.breakout_pct = params['breakout_threshold_pct']   # 0.5%
        self.min_vol_ratio = params['min_volume_ratio']        # 2.0x
        self.weight = params.get('weight', 0.3)

    def evaluate(self, code: str, candles: pd.DataFrame,
                 tick_data: Optional[Dict] = None,
                 orderbook: Optional[Dict] = None) -> Optional[TradeSignal]:

        if len(candles) < self.get_required_candle_count():
            return None

        close = candles['close']
        volume = candles['volume']
        current_price = close.iloc[-1]

        # 지지/저항선 탐색
        levels = IndicatorCalc.find_support_resistance(candles, self.lookback)
        resistances = levels['resistance']
        supports = levels['support']

        # 거래량 비율
        vol_ratio = IndicatorCalc.volume_ratio(volume, 20)
        current_vol = vol_ratio.iloc[-1] if not vol_ratio.empty else 0

        # 저항선 돌파 검출
        if resistances and current_vol >= self.min_vol_ratio:
            nearest_resistance = resistances[0]  # 가장 가까운 저항선
            breakout_threshold = nearest_resistance * (1 + self.breakout_pct / 100)

            if current_price > breakout_threshold:
                # 돌파 확인
                prev_price = close.iloc[-2]
                if prev_price <= nearest_resistance:
                    confidence = min(0.5 + (current_vol - self.min_vol_ratio) * 0.1, 0.9)
                    stop_loss = int(nearest_resistance * 0.99)  # 돌파 실패 시 저항선 아래

                    return self._make_signal(
                        Signal.BUY, code, confidence,
                        f"저항선 돌파 ({nearest_resistance:,.0f}→{current_price:,.0f}) 거래량 {current_vol:.1f}x",
                        stop_loss=stop_loss,
                    )

        # 지지선 이탈 검출
        if supports:
            nearest_support = supports[0]
            breakdown_threshold = nearest_support * (1 - self.breakout_pct / 100)

            if current_price < breakdown_threshold:
                prev_price = close.iloc[-2]
                if prev_price >= nearest_support:
                    return self._make_signal(
                        Signal.SELL, code, 0.6,
                        f"지지선 이탈 ({nearest_support:,.0f}→{current_price:,.0f})",
                    )

        return None

    def get_required_candle_count(self) -> int:
        return self.lookback + 5
