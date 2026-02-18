"""
몸통 포착 엔진 v2 (Body Hunter v2)

v1 대비 개선사항:
  1. LONG ONLY 기본 (SHORT은 한국시장 현실상 비효율)
  2. RR 1.0 도달 시 수익바닥 0.5로 잠금 (본전청산 방지)
  3. 시간대 인식 (9:30~11:30 골든타임 우대)
  4. 첫봉 기준 3~4번째 봉(9:15~9:20)으로 변경 가능
  5. 트레일링 SL 개선: 수익 구간별 점진적 잠금

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
    STOP_LOSS      = "손절"
    EXHAUSTION     = "소진감지"
    TRAILING_STOP  = "트레일링"
    PROFIT_LOCK    = "수익잠금"
    TIME_LIMIT     = "시간초과"


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
    stop_loss:    float    # 초기 SL (첫봉 mid)
    trailing_sl:  float    # 현재 트레일링 SL
    peak_price:   float    # 최고/최저 가격
    entry_time:   object
    hold_bars:    int = 0
    rr_current:   float = 0.0             # 현재 RR
    rr_floor:     float = -1.0            # v2: 최소 보장 RR (수익잠금)
    exhaustion_signals: List[str] = field(default_factory=list)


@dataclass
class ExhaustionSignal:
    detected:  bool
    signals:   List[str]
    urgency:   int


class BodyHunterV2:
    """
    몸통 포착 엔진 v2

    핵심 개선:
      - 수익잠금: RR 1.0 → 바닥 0.5, RR 2.0 → 바닥 1.2, RR 3.0 → 바닥 2.0
      - 본전이동: breakeven_bars 후 SL을 진입가로 이동
      - 시간대: 장중 시간에 따라 진입 허용/차단
    """

    # 수익잠금 테이블: (RR 도달, 바닥 RR)
    PROFIT_LOCK_TABLE = [
        (1.0, 0.5),
        (1.5, 0.8),
        (2.0, 1.2),
        (2.5, 1.6),
        (3.0, 2.0),
    ]

    def __init__(
        self,
        ticker:            str,
        name:              str   = "",
        direction:         str   = "LONG",
        volume_surge_min:  float = 1.5,
        retest_required:   bool  = True,
        close_only_breakout: bool = False,
        trailing_atr_mult: float = 1.2,
        breakeven_bars:    int   = 3,
        exhaustion_bars:   int   = 2,
        volume_drop_ratio: float = 0.65,
        wick_ratio_min:    float = 0.003,
        cutoff_time:       str   = "15:00",
        golden_start:      str   = "09:20",
        golden_end:        str   = "11:30",
    ):
        self.ticker             = ticker
        self.name               = name
        self.direction          = direction
        self.volume_surge_min   = volume_surge_min
        self.retest_required    = retest_required
        self.close_only_breakout = close_only_breakout
        self.trailing_atr_mult  = trailing_atr_mult
        self.breakeven_bars     = breakeven_bars
        self.exhaustion_bars    = exhaustion_bars
        self.volume_drop_ratio  = volume_drop_ratio
        self.wick_ratio_min     = wick_ratio_min
        self.cutoff_time        = pd.Timestamp(f"2000-01-01 {cutoff_time}").time()
        self.golden_start       = pd.Timestamp(f"2000-01-01 {golden_start}").time()
        self.golden_end         = pd.Timestamp(f"2000-01-01 {golden_end}").time()

        self.state:    BodyState               = BodyState.READY
        self.levels:   Optional[BodyLevels]    = None
        self.position: Optional[BodyPosition]  = None
        self._recent_candles: List[pd.Series]  = []
        self._avg_volume:     Optional[float]  = None

    def set_levels(self, first_candle: pd.Series, avg_volume: float = None):
        """첫봉으로 레벨 마킹"""
        h   = first_candle["high"]
        l   = first_candle["low"]
        mid = (h + l) / 2
        atr = h - l

        self.levels      = BodyLevels(high=h, low=l, mid=mid, atr=atr)
        self._avg_volume = avg_volume or first_candle["volume"]
        self.state       = BodyState.WATCHING

    def update(self, candle: pd.Series) -> dict:
        """봉별 업데이트 → 액션 반환"""
        result = dict(action="WAIT", reason="", position=None, exhaustion=None)

        # 시간 체크
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
        """이탈 확인"""
        lv = self.levels
        o, c = candle["open"], candle["close"]
        v    = candle["volume"]

        vol_surge = v / self._avg_volume >= self.volume_surge_min if self._avg_volume > 0 else False

        if self.close_only_breakout:
            # 완화: 종가만 레벨 상회/하회하면 이탈 인정
            if self.direction == "LONG":
                body_outside = c > lv.high
            else:
                body_outside = c < lv.low
        else:
            # 엄격: 몸통 전체(시가+종가)가 레벨 밖
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
        """리테스트 확인"""
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
        """진입"""
        lv  = self.levels
        sl  = lv.mid

        self.position = BodyPosition(
            direction   = self.direction,
            entry_price = entry_price,
            stop_loss   = sl,
            trailing_sl = sl,
            peak_price  = entry_price,
            entry_time  = candle.name if hasattr(candle, "name") else None,
            rr_floor    = -1.0,  # 초기에는 손실 허용
        )
        self.state = BodyState.IN_BODY

        risk = abs(entry_price - sl)
        logger.info(
            f"[{self.ticker}] 진입 [{self.direction}] "
            f"진입:{entry_price:,.0f} SL:{sl:,.0f} 리스크:{risk:,.0f}"
        )
        return dict(action="ENTER", reason=f"{self.direction} 진입", position=self.position)

    def _manage_position(self, candle: pd.Series) -> dict:
        """포지션 관리 (v2: 수익잠금 + 개선된 트레일링)"""
        pos = self.position
        pos.hold_bars += 1
        c, h, l = candle["close"], candle["high"], candle["low"]

        # 현재 RR 계산
        risk = abs(pos.entry_price - pos.stop_loss)
        if risk > 0:
            if self.direction == "LONG":
                pos.rr_current = (c - pos.entry_price) / risk
            else:
                pos.rr_current = (pos.entry_price - c) / risk

        # v2: 수익잠금 업데이트
        self._update_profit_lock(pos)

        # 트레일링 SL 업데이트
        self._update_trailing_sl(candle)

        # SL 체크
        if self.direction == "LONG":
            sl_hit = l <= pos.trailing_sl
        else:
            sl_hit = h >= pos.trailing_sl

        if sl_hit:
            # 수익잠금 발동 여부 판단
            if pos.rr_floor > 0:
                exit_price = self._calc_profit_lock_price(pos)
                return self._exit(candle, ExitReason.PROFIT_LOCK, exit_price)
            elif pos.trailing_sl == pos.stop_loss:
                return self._exit(candle, ExitReason.STOP_LOSS, pos.trailing_sl)
            else:
                return self._exit(candle, ExitReason.TRAILING_STOP, pos.trailing_sl)

        # 소진 감지
        exhaustion = self._detect_exhaustion(candle)
        if exhaustion.detected and exhaustion.urgency >= self.exhaustion_bars:
            return self._exit(candle, ExitReason.EXHAUSTION, c, exhaustion)

        return dict(
            action="HOLD",
            reason=f"몸통탑승 RR:{pos.rr_current:+.2f} 바닥:{pos.rr_floor:+.2f} ({pos.hold_bars}봉)",
            position=pos, exhaustion=exhaustion,
        )

    def _update_profit_lock(self, pos: BodyPosition):
        """v2: 수익잠금 테이블에 따라 바닥 RR 업데이트"""
        for rr_threshold, floor in self.PROFIT_LOCK_TABLE:
            if pos.rr_current >= rr_threshold:
                pos.rr_floor = max(pos.rr_floor, floor)

    def _calc_profit_lock_price(self, pos: BodyPosition) -> float:
        """수익잠금 가격 계산"""
        risk = abs(pos.entry_price - pos.stop_loss)
        if self.direction == "LONG":
            return pos.entry_price + risk * pos.rr_floor
        else:
            return pos.entry_price - risk * pos.rr_floor

    def _update_trailing_sl(self, candle: pd.Series):
        """v2: 개선된 트레일링 SL"""
        pos = self.position
        lv  = self.levels

        # 피크 업데이트
        if self.direction == "LONG":
            if candle["high"] > pos.peak_price:
                pos.peak_price = candle["high"]
        else:
            if candle["low"] < pos.peak_price:
                pos.peak_price = candle["low"]

        # Phase 1: 초기 (breakeven_bars 이내) → SL 고정
        if pos.hold_bars <= self.breakeven_bars:
            pos.trailing_sl = pos.stop_loss

        # Phase 2: 본전 이동 (breakeven_bars ~ 2x)
        elif pos.hold_bars <= self.breakeven_bars * 2:
            if self.direction == "LONG":
                pos.trailing_sl = max(pos.trailing_sl, pos.entry_price)
            else:
                pos.trailing_sl = min(pos.trailing_sl, pos.entry_price)

        # Phase 3: ATR 트레일링
        else:
            atr_dist = lv.atr * self.trailing_atr_mult
            if self.direction == "LONG":
                new_sl = pos.peak_price - atr_dist
                pos.trailing_sl = max(pos.trailing_sl, new_sl)
            else:
                new_sl = pos.peak_price + atr_dist
                pos.trailing_sl = min(pos.trailing_sl, new_sl)

        # v2: 수익잠금 가격과 비교 → 더 높은 것 적용
        if pos.rr_floor > 0:
            lock_price = self._calc_profit_lock_price(pos)
            if self.direction == "LONG":
                pos.trailing_sl = max(pos.trailing_sl, lock_price)
            else:
                pos.trailing_sl = min(pos.trailing_sl, lock_price)

    def _detect_exhaustion(self, candle: pd.Series) -> ExhaustionSignal:
        """
        소진 4신호:
          1. 거래량 다이버전스 (가격↑ + 거래량↓)
          2. 꼬리 증가 (매도압력 등장)
          3. 모멘텀 감소 (봉 크기 축소)
          4. 역봉 연속 (방향 반대 봉 2개)
        """
        signals = []
        o, c = candle["open"], candle["close"]
        h, l = candle["high"],  candle["low"]
        v    = candle["volume"]

        if len(self._recent_candles) < 3:
            return ExhaustionSignal(False, [], 0)

        prev = self._recent_candles[-2]

        # 1. 거래량 다이버전스
        if self.direction == "LONG":
            price_continuing = c > prev["close"]
        else:
            price_continuing = c < prev["close"]

        recent_vols = [x["volume"] for x in self._recent_candles[-3:]]
        recent_avg_vol = sum(recent_vols) / len(recent_vols)
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
            reverse = c < o
            prev_reverse = prev["close"] < prev["open"]
        else:
            reverse = c > o
            prev_reverse = prev["close"] > prev["open"]

        if reverse and prev_reverse:
            signals.append("역봉연속2")

        urgency = len(signals)
        return ExhaustionSignal(detected=urgency >= 1, signals=signals, urgency=urgency)

    def _exit(self, candle, reason, exit_price, exhaustion=None) -> dict:
        """청산"""
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
            ExitReason.PROFIT_LOCK: "$",
        }
        logger.info(
            f"[{icon_map.get(reason, '?')}] [{self.ticker}] 청산 [{reason.value}] "
            f"진입:{pos.entry_price:,.0f} -> 청산:{exit_price:,.0f} "
            f"RR:{rr_realized:+.2f} 바닥:{pos.rr_floor:+.2f} | {pos.hold_bars}봉"
        )

        return dict(
            action="EXIT", reason=reason.value, exit_price=exit_price,
            rr_realized=rr_realized, hold_bars=pos.hold_bars,
            position=pos, exhaustion=exhaustion,
        )

    def reset(self):
        """리셋"""
        self.state = BodyState.READY
        self.levels = None
        self.position = None
        self._recent_candles = []
