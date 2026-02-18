"""
Candle Builder - 틱 데이터로 분봉 생성
=======================================
실시간 틱을 받아 OHLCV 분봉을 생성하고, 봉 마감 시 콜백 호출
"""

import logging
from datetime import datetime, timedelta
from typing import Callable, Dict, List, Optional

import pandas as pd

logger = logging.getLogger('Scalper.Candle')


class CandleBuilder:
    """틱 → 분봉 변환기"""

    def __init__(self, period_minutes: int = 1, max_candles: int = 500):
        self.period = timedelta(minutes=period_minutes)
        self.period_minutes = period_minutes
        self.max_candles = max_candles

        self._current: Optional[Dict] = None
        self._candles: List[Dict] = []
        self.on_candle_close: Optional[Callable] = None

    def add_tick(self, tick: Dict):
        """
        틱 데이터 추가 → 분봉 갱신/마감

        Args:
            tick: {'price': int, 'volume': int, 'timestamp': datetime, ...}
        """
        price = tick.get('price', 0)
        volume = abs(tick.get('volume', 0))
        ts = tick.get('timestamp', datetime.now())

        if price <= 0:
            return

        # 분봉 기준 시간 계산
        candle_start = self._get_candle_start(ts)

        if self._current is None:
            self._start_new_candle(price, volume, candle_start)
            return

        # 현재 봉의 기간이 지났으면 마감 후 새 봉 시작
        if candle_start > self._current['timestamp']:
            self._close_current_candle()
            self._start_new_candle(price, volume, candle_start)
        else:
            # 현재 봉 갱신
            self._current['high'] = max(self._current['high'], price)
            self._current['low'] = min(self._current['low'], price)
            self._current['close'] = price
            self._current['volume'] += volume

    def _get_candle_start(self, ts: datetime) -> datetime:
        """타임스탬프를 분봉 시작 시간으로 정규화"""
        minute = (ts.minute // self.period_minutes) * self.period_minutes
        return ts.replace(minute=minute, second=0, microsecond=0)

    def _start_new_candle(self, price: int, volume: int, timestamp: datetime):
        self._current = {
            'timestamp': timestamp,
            'open': price,
            'high': price,
            'low': price,
            'close': price,
            'volume': volume,
        }

    def _close_current_candle(self):
        """현재 봉 마감 → 리스트에 추가 → 콜백 호출"""
        if self._current is None:
            return

        self._candles.append(self._current.copy())

        # 최대 봉 수 제한
        if len(self._candles) > self.max_candles:
            self._candles = self._candles[-self.max_candles:]

        # 콜백 호출
        if self.on_candle_close:
            try:
                self.on_candle_close(self._current.copy())
            except Exception as e:
                logger.error(f"봉 마감 콜백 에러: {e}")

        self._current = None

    def force_close(self):
        """장마감 등으로 현재 봉 강제 마감"""
        self._close_current_candle()

    def get_candles(self, n: Optional[int] = None) -> pd.DataFrame:
        """완료된 분봉 DataFrame 반환"""
        candles = self._candles[-n:] if n else self._candles
        if not candles:
            return pd.DataFrame(columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        return pd.DataFrame(candles)

    def get_current_candle(self) -> Optional[Dict]:
        """현재 진행 중인 (미완료) 봉"""
        return self._current.copy() if self._current else None

    @property
    def candle_count(self) -> int:
        return len(self._candles)
