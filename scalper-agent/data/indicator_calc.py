"""
Indicator Calculator - 기술 지표 계산
=======================================
이평선, VWAP, RSI, 볼린저밴드, MACD 등 핵심 지표
"""

from typing import Dict, List, Tuple

import numpy as np
import pandas as pd


class IndicatorCalc:
    """기술 지표 계산 유틸리티 (모두 정적 메서드)"""

    @staticmethod
    def sma(series: pd.Series, period: int) -> pd.Series:
        """단순이동평균 (SMA)"""
        return series.rolling(window=period, min_periods=period).mean()

    @staticmethod
    def ema(series: pd.Series, period: int) -> pd.Series:
        """지수이동평균 (EMA)"""
        return series.ewm(span=period, adjust=False).mean()

    @staticmethod
    def vwap(candles: pd.DataFrame) -> pd.Series:
        """거래량가중평균가격 (VWAP)"""
        typical = (candles['high'] + candles['low'] + candles['close']) / 3
        cum_tp_vol = (typical * candles['volume']).cumsum()
        cum_vol = candles['volume'].cumsum()
        return cum_tp_vol / cum_vol.replace(0, np.nan)

    @staticmethod
    def rsi(series: pd.Series, period: int = 14) -> pd.Series:
        """RSI (Relative Strength Index)"""
        delta = series.diff()
        gain = delta.where(delta > 0, 0.0)
        loss = (-delta).where(delta < 0, 0.0)

        avg_gain = gain.ewm(alpha=1/period, min_periods=period).mean()
        avg_loss = loss.ewm(alpha=1/period, min_periods=period).mean()

        rs = avg_gain / avg_loss.replace(0, np.nan)
        return 100 - (100 / (1 + rs))

    @staticmethod
    def bollinger_bands(series: pd.Series, period: int = 20,
                        std_dev: float = 2.0) -> Tuple[pd.Series, pd.Series, pd.Series]:
        """볼린저밴드 → (upper, middle, lower)"""
        middle = series.rolling(window=period).mean()
        std = series.rolling(window=period).std()
        upper = middle + std_dev * std
        lower = middle - std_dev * std
        return upper, middle, lower

    @staticmethod
    def macd(series: pd.Series, fast: int = 12, slow: int = 26,
             signal: int = 9) -> Tuple[pd.Series, pd.Series, pd.Series]:
        """MACD → (macd_line, signal_line, histogram)"""
        ema_fast = series.ewm(span=fast, adjust=False).mean()
        ema_slow = series.ewm(span=slow, adjust=False).mean()
        macd_line = ema_fast - ema_slow
        signal_line = macd_line.ewm(span=signal, adjust=False).mean()
        histogram = macd_line - signal_line
        return macd_line, signal_line, histogram

    @staticmethod
    def volume_ratio(volume: pd.Series, period: int = 20) -> pd.Series:
        """현재 거래량 / N기간 평균 거래량"""
        avg = volume.rolling(window=period, min_periods=1).mean()
        return volume / avg.replace(0, np.nan)

    @staticmethod
    def stochastic(high: pd.Series, low: pd.Series, close: pd.Series,
                   k_period: int = 14, d_period: int = 3) -> Tuple[pd.Series, pd.Series]:
        """스토캐스틱 → (%K, %D)"""
        lowest = low.rolling(window=k_period).min()
        highest = high.rolling(window=k_period).max()
        k = 100 * (close - lowest) / (highest - lowest).replace(0, np.nan)
        d = k.rolling(window=d_period).mean()
        return k, d

    @staticmethod
    def atr(high: pd.Series, low: pd.Series, close: pd.Series,
            period: int = 14) -> pd.Series:
        """Average True Range"""
        prev_close = close.shift(1)
        tr1 = high - low
        tr2 = (high - prev_close).abs()
        tr3 = (low - prev_close).abs()
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        return tr.rolling(window=period).mean()

    @staticmethod
    def find_support_resistance(candles: pd.DataFrame, lookback: int = 60,
                                 num_levels: int = 5) -> Dict[str, List[float]]:
        """
        지지/저항선 탐색 (피봇 기반)

        최근 lookback 봉에서 스윙 하이/로우를 찾아 클러스터링
        """
        df = candles.tail(lookback).copy()
        if len(df) < 5:
            return {'support': [], 'resistance': []}

        highs = df['high'].values
        lows = df['low'].values
        current_price = df['close'].iloc[-1]

        # 스윙 하이/로우 감지 (좌우 2봉 비교)
        swing_highs = []
        swing_lows = []

        for i in range(2, len(highs) - 2):
            if highs[i] > highs[i-1] and highs[i] > highs[i-2] and \
               highs[i] > highs[i+1] and highs[i] > highs[i+2]:
                swing_highs.append(float(highs[i]))
            if lows[i] < lows[i-1] and lows[i] < lows[i-2] and \
               lows[i] < lows[i+1] and lows[i] < lows[i+2]:
                swing_lows.append(float(lows[i]))

        # 현재가 기준으로 분류
        resistance = sorted([h for h in swing_highs if h > current_price])[:num_levels]
        support = sorted([l for l in swing_lows if l < current_price], reverse=True)[:num_levels]

        return {'support': support, 'resistance': resistance}

    # ──────────────────────────────────────────────
    #  OBV (On Balance Volume)
    # ──────────────────────────────────────────────

    @staticmethod
    def obv(close: pd.Series, volume: pd.Series) -> pd.Series:
        """OBV 계산 — 가격 방향에 따라 거래량 누적"""
        direction = close.diff().apply(lambda x: 1 if x > 0 else (-1 if x < 0 else 0))
        return (direction * volume).cumsum()

    @staticmethod
    def obv_trend(close: pd.Series, volume: pd.Series,
                  lookback: int = 10) -> str:
        """OBV 추세 판정 (선형회귀 기울기)

        Returns: 'UP' | 'DOWN' | 'FLAT'
        """
        obv_series = IndicatorCalc.obv(close, volume)
        recent = obv_series.dropna().tail(lookback)
        if len(recent) < lookback:
            return 'FLAT'

        x = np.arange(len(recent), dtype=float)
        y = recent.values.astype(float)
        slope = np.polyfit(x, y, 1)[0]

        # 기울기를 OBV 범위 대비 정규화
        obv_range = max(y.max() - y.min(), 1)
        norm_slope = slope * lookback / obv_range

        if norm_slope > 0.15:
            return 'UP'
        elif norm_slope < -0.15:
            return 'DOWN'
        return 'FLAT'

    @staticmethod
    def obv_divergence(close: pd.Series, volume: pd.Series,
                       lookback: int = 20) -> str:
        """OBV 다이버전스 감지

        Returns: 'BEARISH_DIV' | 'BULLISH_DIV' | 'NONE'
        """
        obv_series = IndicatorCalc.obv(close, volume)
        recent_close = close.tail(lookback)
        recent_obv = obv_series.tail(lookback)

        if len(recent_close) < lookback:
            return 'NONE'

        price_new_high = recent_close.iloc[-1] >= recent_close.max() * 0.98
        obv_new_high = recent_obv.iloc[-1] >= recent_obv.max() * 0.98

        price_new_low = recent_close.iloc[-1] <= recent_close.min() * 1.02
        obv_new_low = recent_obv.iloc[-1] <= recent_obv.min() * 1.02

        if price_new_high and not obv_new_high:
            return 'BEARISH_DIV'
        if price_new_low and not obv_new_low:
            return 'BULLISH_DIV'
        return 'NONE'

    @staticmethod
    def ma_crossover_signal(fast_ma: pd.Series, slow_ma: pd.Series) -> str:
        """
        이평선 교차 판단

        Returns:
            'golden_cross': 골든크로스 (매수)
            'dead_cross': 데드크로스 (매도)
            'none': 교차 없음
        """
        if len(fast_ma) < 2 or len(slow_ma) < 2:
            return 'none'

        prev_fast = fast_ma.iloc[-2]
        prev_slow = slow_ma.iloc[-2]
        curr_fast = fast_ma.iloc[-1]
        curr_slow = slow_ma.iloc[-1]

        if pd.isna(prev_fast) or pd.isna(prev_slow):
            return 'none'

        if prev_fast <= prev_slow and curr_fast > curr_slow:
            return 'golden_cross'
        elif prev_fast >= prev_slow and curr_fast < curr_slow:
            return 'dead_cross'
        return 'none'
