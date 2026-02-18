"""
Volume Spike Strategy - 거래량 급증 감지
==========================================
서보성: 거래량 서서히 증가 (20일 평균 대비 2~3배, 폭발 아닌 점진적)

비정상 거래량 + VWAP 상향 돌파 시 매수
"""

import logging
from typing import Dict, Optional

import pandas as pd

from strategies.base_strategy import BaseStrategy, Signal, TradeSignal
from data.indicator_calc import IndicatorCalc

logger = logging.getLogger('Scalper.Strategy.Volume')


class VolumeSpikeStrategy(BaseStrategy):
    """거래량 급증 기반 매매 전략"""

    def __init__(self, config: dict):
        super().__init__(config)
        params = config['strategies']['volume_spike']
        self.avg_period = params['avg_period']           # 20
        self.spike_threshold = params['spike_threshold']  # 3.0x
        self.price_above_vwap = params.get('price_above_vwap', True)
        self.weight = params.get('weight', 0.3)

    def evaluate(self, code: str, candles: pd.DataFrame,
                 tick_data: Optional[Dict] = None,
                 orderbook: Optional[Dict] = None) -> Optional[TradeSignal]:

        if len(candles) < self.get_required_candle_count():
            return None

        close = candles['close']
        volume = candles['volume']

        # 거래량 비율 계산
        vol_ratio = IndicatorCalc.volume_ratio(volume, self.avg_period)
        current_ratio = vol_ratio.iloc[-1] if not vol_ratio.empty else 0

        if current_ratio < self.spike_threshold:
            return None

        # VWAP 계산
        vwap = IndicatorCalc.vwap(candles)
        current_price = close.iloc[-1]
        current_vwap = vwap.iloc[-1] if not vwap.empty else 0

        # VWAP 조건 확인
        price_condition = True
        if self.price_above_vwap and current_vwap > 0:
            price_condition = current_price > current_vwap

        if not price_condition:
            return None

        # 거래량 급증 방향 판단 (양봉인지 음봉인지)
        current_open = candles['open'].iloc[-1]
        is_bullish = current_price > current_open

        if is_bullish:
            # 양봉 + 거래량 급증 → 매수
            confidence = min(0.4 + (current_ratio - self.spike_threshold) * 0.1, 0.9)

            # 점진적 거래량 증가 확인 (3봉 연속 증가 → 보너스)
            if len(vol_ratio) >= 3:
                recent = vol_ratio.iloc[-3:]
                if all(recent.diff().dropna() > 0):
                    confidence = min(confidence + 0.15, 1.0)

            return self._make_signal(
                Signal.BUY, code, confidence,
                f"거래량 급증 {current_ratio:.1f}x (VWAP 상향)",
            )
        else:
            # 음봉 + 거래량 급증 → 매도 경고
            return self._make_signal(
                Signal.SELL, code, 0.5,
                f"거래량 급증 {current_ratio:.1f}x (음봉, 매도 압력)",
            )

    def get_required_candle_count(self) -> int:
        return self.avg_period + 5
