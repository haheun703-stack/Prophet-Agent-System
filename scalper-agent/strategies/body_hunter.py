"""
몸통 포착 엔진 (Body Hunter Core)

핵심 철학:
  꼬리(X) 머리(X) -> 몸통(O)
  예측(X) -> 확인 후 진입(O)
  고정TP(X) -> 소진신호까지 홀딩(O)

상태 머신:
  READY -> WATCHING -> RETEST_WAIT -> IN_BODY -> DONE
"""

import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, List
import pandas as pd

logger = logging.getLogger(__name__)


class BodyState(Enum):
    READY          = "준비"
    WATCHING       = "감시중"
    RETEST_WAIT    = "리테스트대기"
    IN_BODY        = "몸통탑승"
    DONE           = "완료"


class ExitReason(Enum):
    TAKE_PROFIT    = "목표달성"
    STOP_LOSS      = "손절"
    EXHAUSTION     = "소진감지"
    TRAILING_STOP  = "트레일링"
    TIME_LIMIT     = "시간초과"
    MANUAL         = "수동청산"


@dataclass
class BodyLevels:
    high:     float
    low:      float
    mid:      float
    atr:      float = 0.0

    @property
    def range_size(self):
        return self.high - self.low


@dataclass
class BodyPosition:
    direction:    str
    entry_price:  float
    stop_loss:    float
    trailing_sl:  float
    peak_price:   float
    entry_time:   object
    hold_bars:    int = 0
    exhaustion_signals: List[str] = field(default_factory=list)


@dataclass
class ExhaustionSignal:
    detected:  bool
    signals:   List[str]
    urgency:   int


class BodyHunter:
    """
    몸통 포착 엔진

    ETF방향 + 상대강도 종목이 입력으로 들어오면
    해당 종목에서 몸통 구간을 자동 포착/탑승/탈출
    """

    def __init__(
        self,
        ticker:            str,
        name:              str   = "",
        direction:         str   = "LONG",
        volume_surge_min:  float = 1.5,
        retest_required:   bool  = True,
        sl_at_mid:         bool  = True,
        rr_initial:        float = 2.0,
        trailing_atr_mult: float = 1.5,
        breakeven_bars:    int   = 3,
        exhaustion_bars:   int   = 2,
        volume_drop_ratio: float = 0.65,
        wick_ratio_min:    float = 0.003,
        cutoff_time:       str   = "15:00",
    ):
        self.ticker             = ticker
        self.name               = name
        self.direction          = direction
        self.volume_surge_min   = volume_surge_min
        self.retest_required    = retest_required
        self.sl_at_mid          = sl_at_mid
        self.rr_initial         = rr_initial
        self.trailing_atr_mult  = trailing_atr_mult
        self.breakeven_bars     = breakeven_bars
        self.exhaustion_bars    = exhaustion_bars
        self.volume_drop_ratio  = volume_drop_ratio
        self.wick_ratio_min     = wick_ratio_min
        self.cutoff_time        = pd.Timestamp(f"2000-01-01 {cutoff_time}").time()

        self.state:    BodyState            = BodyState.READY
        self.levels:   Optional[BodyLevels] = None
        self.position: Optional[BodyPosition] = None
        self._recent_candles: List[pd.Series] = []
        self._avg_volume:     Optional[float]  = None

    def set_levels(self, first_candle: pd.Series, avg_volume: float = None):
        h   = first_candle["high"]
        l   = first_candle["low"]
        mid = (h + l) / 2
        atr = (h - l)

        self.levels       = BodyLevels(high=h, low=l, mid=mid, atr=atr)
        self._avg_volume  = avg_volume or first_candle["volume"]
        self.state        = BodyState.WATCHING

    def update(self, candle: pd.Series) -> dict:
        result = dict(action="WAIT", reason="", position=None, exhaustion=None)

        t = candle.name.time() if hasattr(candle.name, "time") and callable(candle.name.time) else None
        if t and t >= self.cutoff_time:
            if self.state == BodyState.IN_BODY:
                result = self._exit(candle, ExitReason.TIME_LIMIT, candle["close"])
            else:
                self.state = BodyState.DONE
                result["reason"] = "시간초과"
            return result

        self._recent_candles.append(candle)
        if len(self._recent_candles) > 10:
            self._recent_candles.pop(0)

        if self.state == BodyState.WATCHING:
            result = self._check_breakout(candle)
        elif self.state == BodyState.RETEST_WAIT:
            result = self._check_retest(candle)
        elif self.state == BodyState.IN_BODY:
            result = self._manage_position(candle)

        return result

    def _check_breakout(self, candle: pd.Series) -> dict:
        lv = self.levels
        o, c = candle["open"], candle["close"]
        v    = candle["volume"]

        vol_surge = v / self._avg_volume >= self.volume_surge_min

        if self.direction == "LONG":
            body_outside = min(o, c) > lv.high
        else:
            body_outside = max(o, c) < lv.low

        if body_outside and vol_surge:
            if self.retest_required:
                self.state = BodyState.RETEST_WAIT
                return dict(action="WAIT", reason="이탈확인-리테스트대기")
            else:
                return self._enter(candle, c)

        return dict(action="WAIT", reason="이탈대기중")

    def _check_retest(self, candle: pd.Series) -> dict:
        lv   = self.levels
        o, c = candle["open"], candle["close"]
        h, l = candle["high"],  candle["low"]

        if self.direction == "LONG":
            touched     = l <= lv.high
            valid_close = c > lv.high
        else:
            touched     = h >= lv.low
            valid_close = c < lv.low

        if touched and valid_close:
            return self._enter(candle, c)

        return dict(action="WAIT", reason="리테스트대기중")

    def _enter(self, candle: pd.Series, entry_price: float) -> dict:
        lv  = self.levels
        sl  = lv.mid

        self.position = BodyPosition(
            direction   = self.direction,
            entry_price = entry_price,
            stop_loss   = sl,
            trailing_sl = sl,
            peak_price  = entry_price,
            entry_time  = candle.name if hasattr(candle, "name") else None,
        )
        self.state = BodyState.IN_BODY

        risk = abs(entry_price - sl)
        logger.info(
            f"[{self.ticker}] 진입 [{self.direction}] "
            f"진입:{entry_price:,.0f} SL:{sl:,.0f} 리스크:{risk:,.0f}"
        )
        return dict(action="ENTER", reason=f"{self.direction} 진입", position=self.position)

    def _manage_position(self, candle: pd.Series) -> dict:
        pos = self.position
        pos.hold_bars += 1
        c, h, l = candle["close"], candle["high"], candle["low"]

        self._update_trailing_sl(candle)

        # SL 체크
        if self.direction == "LONG":
            sl_hit = l <= pos.trailing_sl
        else:
            sl_hit = h >= pos.trailing_sl

        if sl_hit:
            reason = ExitReason.STOP_LOSS if pos.trailing_sl == pos.stop_loss else ExitReason.TRAILING_STOP
            return self._exit(candle, reason, pos.trailing_sl)

        # 소진 감지
        exhaustion = self._detect_exhaustion(candle)
        if exhaustion.detected and exhaustion.urgency >= self.exhaustion_bars:
            return self._exit(candle, ExitReason.EXHAUSTION, c, exhaustion)

        return dict(action="HOLD", reason=f"몸통탑승중 ({pos.hold_bars}봉)", position=pos, exhaustion=exhaustion)

    def _update_trailing_sl(self, candle: pd.Series):
        pos = self.position
        lv  = self.levels

        if self.direction == "LONG":
            if candle["high"] > pos.peak_price:
                pos.peak_price = candle["high"]
        else:
            if candle["low"] < pos.peak_price:
                pos.peak_price = candle["low"]

        if pos.hold_bars <= self.breakeven_bars:
            pos.trailing_sl = pos.stop_loss
        elif pos.hold_bars <= self.breakeven_bars * 2:
            if self.direction == "LONG":
                pos.trailing_sl = max(pos.trailing_sl, pos.entry_price)
            else:
                pos.trailing_sl = min(pos.trailing_sl, pos.entry_price)
        else:
            atr_sl_dist = lv.atr * self.trailing_atr_mult
            if self.direction == "LONG":
                new_sl = pos.peak_price - atr_sl_dist
                pos.trailing_sl = max(pos.trailing_sl, new_sl)
            else:
                new_sl = pos.peak_price + atr_sl_dist
                pos.trailing_sl = min(pos.trailing_sl, new_sl)

    def _detect_exhaustion(self, candle: pd.Series) -> ExhaustionSignal:
        """
        소진 4신호:
          1. 거래량 다이버전스 (가격 올라가는데 거래량 줄어듦)
          2. 꼬리 증가 (매도/매수 압력 등장)
          3. 모멘텀 감소 (봉 크기 축소)
          4. 역봉 연속 (방향 반대 봉 2개 연속)
        """
        signals = []
        o, c = candle["open"], candle["close"]
        h, l = candle["high"],  candle["low"]
        v    = candle["volume"]

        if len(self._recent_candles) < 2:
            return ExhaustionSignal(False, [], 0)

        prev = self._recent_candles[-2]

        # 1. 거래량 다이버전스
        if self.direction == "LONG":
            price_continuing = c > prev["close"]
        else:
            price_continuing = c < prev["close"]

        recent_avg_vol = sum(x["volume"] for x in self._recent_candles[-3:]) / min(3, len(self._recent_candles))
        if price_continuing and v < recent_avg_vol * self.volume_drop_ratio:
            signals.append("거래량다이버전스")

        # 2. 꼬리 증가
        if self.direction == "LONG":
            wick = (h - max(o, c)) / max(o, c) if max(o, c) > 0 else 0
        else:
            wick = (min(o, c) - l) / min(o, c) if min(o, c) > 0 else 0

        if wick > self.wick_ratio_min:
            signals.append(f"꼬리증가({wick*100:.2f}%)")

        # 3. 모멘텀 감소
        curr_body = abs(c - o)
        prev_body = abs(prev["close"] - prev["open"])
        if prev_body > 0 and curr_body < prev_body * 0.45:
            signals.append("모멘텀감소")

        # 4. 역봉 연속
        if self.direction == "LONG":
            reverse_candle = c < o
            prev_reverse = prev["close"] < prev["open"]
        else:
            reverse_candle = c > o
            prev_reverse = prev["close"] > prev["open"]

        if reverse_candle and prev_reverse:
            signals.append("역봉연속2")

        urgency = len(signals)
        return ExhaustionSignal(detected=urgency >= 1, signals=signals, urgency=urgency)

    def _exit(self, candle, reason, exit_price, exhaustion=None) -> dict:
        pos  = self.position
        risk = abs(pos.entry_price - pos.stop_loss)

        if self.direction == "LONG":
            raw_pnl = exit_price - pos.entry_price
        else:
            raw_pnl = pos.entry_price - exit_price

        rr_realized = raw_pnl / risk if risk > 0 else 0
        self.state = BodyState.DONE

        icon_map = {
            ExitReason.STOP_LOSS: "X", ExitReason.EXHAUSTION: "!",
            ExitReason.TRAILING_STOP: "T", ExitReason.TIME_LIMIT: "C",
        }
        logger.info(
            f"[{icon_map.get(reason, '?')}] [{self.ticker}] 청산 [{reason.value}] "
            f"진입:{pos.entry_price:,.0f} -> 청산:{exit_price:,.0f} "
            f"RR:{rr_realized:+.2f} | {pos.hold_bars}봉"
        )

        return dict(
            action="EXIT", reason=reason.value, exit_price=exit_price,
            rr_realized=rr_realized, hold_bars=pos.hold_bars,
            position=pos, exhaustion=exhaustion,
        )

    def reset(self):
        self.state = BodyState.READY
        self.levels = None
        self.position = None
        self._recent_candles = []
