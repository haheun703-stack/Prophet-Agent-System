"""
Tick Buffer - 실시간 틱 데이터 링 버퍼
========================================
메모리 사용량을 제한하면서 최근 틱을 효율적으로 저장
"""

from collections import deque
from datetime import datetime
from typing import Dict, List, Optional

import numpy as np


class TickBuffer:
    """고정 크기 링 버퍼로 최근 틱 데이터 저장"""

    def __init__(self, max_size: int = 10000):
        self.buffer: deque = deque(maxlen=max_size)
        self.max_size = max_size

    def add(self, tick: Dict):
        """틱 추가"""
        self.buffer.append(tick)

    def get_recent(self, n: int = 100) -> List[Dict]:
        """최근 n개 틱"""
        return list(self.buffer)[-n:]

    def get_since(self, timestamp: datetime) -> List[Dict]:
        """특정 시간 이후 틱"""
        return [t for t in self.buffer if t.get('timestamp', datetime.min) >= timestamp]

    def get_vwap(self, n: Optional[int] = None) -> float:
        """VWAP (거래량가중평균가격) 계산"""
        ticks = self.get_recent(n) if n else list(self.buffer)
        if not ticks:
            return 0.0

        total_value = 0
        total_volume = 0
        for t in ticks:
            price = t.get('price', 0)
            volume = abs(t.get('volume', 0))
            if volume > 0:
                total_value += price * volume
                total_volume += volume

        return total_value / total_volume if total_volume > 0 else 0.0

    def get_last_price(self) -> int:
        """마지막 체결가"""
        if self.buffer:
            return self.buffer[-1].get('price', 0)
        return 0

    def get_total_volume(self) -> int:
        """누적 거래량"""
        if self.buffer:
            return self.buffer[-1].get('cumul_volume', 0)
        return 0

    @property
    def size(self) -> int:
        return len(self.buffer)

    @property
    def is_empty(self) -> bool:
        return len(self.buffer) == 0

    def clear(self):
        self.buffer.clear()
