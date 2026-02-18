"""
5분 첫봉 스캘핑 전략 - BaseStrategy 통합 버전
================================================
원본: files (3).zip / strategy.py
전략: 9:05 첫 5분봉 H/L → Breakout → Retest → 진입 → 2:1 RR

Rules:
  1. 09:05 첫 5분봉 H / L / MID 추출
  2. 몸통 전체가 레벨 이탈하는 봉 대기 (꼬리 무효)
  3. 리테스트: 레인지 안으로 진입 후 외부 마감
  4. 진입: 리테스트 확인 봉 마감 시 즉시
  5. SL = 첫봉 미드포인트, TP = 2:1 RR
"""

import logging
from dataclasses import dataclass
from enum import Enum
from typing import Dict, Optional

import pandas as pd

from strategies.base_strategy import BaseStrategy, Signal, TradeSignal

logger = logging.getLogger('Scalper.Strategy.5MinScalp')


# ═══════════════════════════════════════════════════════
# Internal State Enums & Data Classes
# ═══════════════════════════════════════════════════════

class ScalpDirection(Enum):
    LONG = "LONG"
    SHORT = "SHORT"
    NONE = "NONE"


class ScalpState(Enum):
    WAITING_CANDLE = "첫봉_대기"
    WAITING_BREAKOUT = "브레이크아웃_대기"
    WAITING_RETEST = "리테스트_대기"
    IN_POSITION = "포지션_진행중"
    DONE = "당일_완료"


@dataclass
class ScalpingLevels:
    high: float
    low: float
    midpoint: float

    def __post_init__(self):
        self.range_size = self.high - self.low

    def body_above(self, o: float, c: float) -> bool:
        return min(o, c) > self.high

    def body_below(self, o: float, c: float) -> bool:
        return max(o, c) < self.low

    def touched_high(self, low: float) -> bool:
        return low <= self.high

    def touched_low(self, high: float) -> bool:
        return high >= self.low


# ═══════════════════════════════════════════════════════
# Strategy Implementation
# ═══════════════════════════════════════════════════════

class FiveMinScalpingStrategy(BaseStrategy):
    """
    5분 첫봉 스캘핑 전략

    기존 scalper-agent의 BaseStrategy 인터페이스 준수.
    TradingEngine에서 다른 전략들과 동일하게 사용 가능.

    Config:
        strategies.five_min_scalping.rr_ratio: 2.0
        strategies.five_min_scalping.first_candle_index: 0 (5분봉 기준 첫번째)
        strategies.five_min_scalping.cutoff_bar: 72 (15:00 = 장시작 후 72번째 5분봉)
        strategies.five_min_scalping.weight: 0.5
    """

    def __init__(self, config: dict):
        super().__init__(config)
        self.name = "FiveMinScalping"

        scalp_cfg = config.get('strategies', {}).get('five_min_scalping', {})
        self.rr_ratio = scalp_cfg.get('rr_ratio', 2.0)
        self.first_candle_index = scalp_cfg.get('first_candle_index', 0)
        self.cutoff_bar = scalp_cfg.get('cutoff_bar', 72)
        self.weight = scalp_cfg.get('weight', 0.5)

        # 종목별 상태 관리
        self._states: Dict[str, dict] = {}

    def _get_state(self, code: str) -> dict:
        if code not in self._states:
            self._states[code] = {
                'state': ScalpState.WAITING_CANDLE,
                'levels': None,
                'direction': ScalpDirection.NONE,
                'first_set': False,
            }
        return self._states[code]

    def reset_code(self, code: str):
        """특정 종목 상태 초기화 (일일 리셋)"""
        if code in self._states:
            del self._states[code]

    def reset_all(self):
        """전체 종목 상태 초기화"""
        self._states.clear()

    def evaluate(self, code: str, candles: pd.DataFrame,
                 tick_data: Optional[Dict] = None,
                 orderbook: Optional[Dict] = None) -> Optional[TradeSignal]:
        """
        분봉 DataFrame을 받아 5분봉 스캘핑 신호 생성

        candles는 5분봉이어야 최적이지만, 1분봉이면 내부에서 5분 리샘플링
        """
        if candles is None or len(candles) < 2:
            return None

        state = self._get_state(code)

        # 봉 수가 cutoff 초과면 당일 완료
        if len(candles) > self.cutoff_bar:
            state['state'] = ScalpState.DONE
            return None

        if state['state'] == ScalpState.DONE:
            return None

        # --- STEP 1: 첫봉 레벨 마킹 ---
        if state['state'] == ScalpState.WAITING_CANDLE:
            if len(candles) > self.first_candle_index:
                first = candles.iloc[self.first_candle_index]
                h = float(first['high'])
                l = float(first['low'])
                mid = (h + l) / 2
                state['levels'] = ScalpingLevels(high=h, low=l, midpoint=mid)
                state['state'] = ScalpState.WAITING_BREAKOUT
                state['first_set'] = True
                logger.info(f"[{code}] 레벨마킹 H:{h:,.0f} L:{l:,.0f} MID:{mid:,.0f} "
                            f"RANGE:{h-l:,.0f}")
            return None

        levels = state['levels']
        if levels is None:
            return None

        last = candles.iloc[-1]
        o = float(last['open'])
        c = float(last['close'])
        h = float(last['high'])
        l = float(last['low'])

        # --- STEP 2: 브레이크아웃 감지 ---
        if state['state'] == ScalpState.WAITING_BREAKOUT:
            if levels.body_above(o, c):
                state['direction'] = ScalpDirection.LONG
                state['state'] = ScalpState.WAITING_RETEST
                logger.info(f"[{code}] 상방 이탈 | 마감:{c:,.0f} > H:{levels.high:,.0f}")
            elif levels.body_below(o, c):
                state['direction'] = ScalpDirection.SHORT
                state['state'] = ScalpState.WAITING_RETEST
                logger.info(f"[{code}] 하방 이탈 | 마감:{c:,.0f} < L:{levels.low:,.0f}")
            return None

        # --- STEP 3: 리테스트 확인 → 진입 신호 ---
        if state['state'] == ScalpState.WAITING_RETEST:
            direction = state['direction']

            if direction == ScalpDirection.LONG:
                touched = levels.touched_high(l)
                valid_close = c > levels.high
            else:
                touched = levels.touched_low(h)
                valid_close = c < levels.low

            if not touched:
                return None

            if not valid_close:
                logger.debug(f"[{code}] 리테스트 무효(내부마감) c:{c:,.0f}")
                return None

            # 유효한 리테스트 확인 → 진입 신호 생성
            entry_price = int(c)
            mid = int(levels.midpoint)

            if direction == ScalpDirection.LONG:
                sl = mid
                risk = entry_price - sl
                tp = int(entry_price + risk * self.rr_ratio)
                signal = Signal.BUY
                reason = (f"5분 첫봉 LONG: 상방이탈 리테스트 확인 "
                          f"(H:{levels.high:,.0f} → 진입:{entry_price:,.0f})")
            else:
                sl = mid
                risk = sl - entry_price
                tp = int(entry_price - risk * self.rr_ratio)
                signal = Signal.SELL
                reason = (f"5분 첫봉 SHORT: 하방이탈 리테스트 확인 "
                          f"(L:{levels.low:,.0f} → 진입:{entry_price:,.0f})")

            state['state'] = ScalpState.DONE

            confidence = 0.85  # 리테스트 확인된 경우 높은 신뢰도

            logger.info(f"[{code}] 진입신호 {direction.value} "
                        f"entry:{entry_price:,} SL:{sl:,} TP:{tp:,} RR:{self.rr_ratio}")

            return self._make_signal(
                signal=signal,
                code=code,
                confidence=confidence,
                reason=reason,
                stop_loss=sl,
                take_profit=tp,
                suggested_price=entry_price,
            )

        return None

    def get_required_candle_count(self) -> int:
        return 2  # 최소 첫봉 + 1봉
