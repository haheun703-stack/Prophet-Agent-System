"""
몸통 포착 엔진 v2.3 (Body Hunter v2.3) — Prop Firm Edition

v2.3 (Prop Firm):
  9. Fixed TP 모드: 고정 2:1 TP (trailing/소진감지 대신 단순 청산)
 10. SL 축소: sl_ratio 0.6 (range의 60% → 더 타이트한 손절)
 11. DrawdownShield 연동: 연패 기반 리스크 자동 축소

v2.2:
  - sl_ratio 조절 (SL 위치 커스텀)
  - 수익잠금 테이블 촘촘화

v2.1 (First Candle Strategy):
  6. 박스권(Choppy) 감지: 이탈 시도 3회+ 실패 시 자동 포기
  7. FOMO 방지 로그: 리테스트 미확인 상태 명시 경고
  8. 소진감지 수익보호: 소진감지 시 수익잠금 가격 보장

v1:
  1. LONG ONLY 기본
  2. RR 기반 수익잠금
  3. 시간대 인식
  4. 첫봉 기준 변경 가능
  5. 트레일링 SL 개선

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
    TAKE_PROFIT    = "익절"          # v2.3: 고정 TP
    EXHAUSTION     = "소진감지"
    TRAILING_STOP  = "트레일링"
    PROFIT_LOCK    = "수익잠금"
    TIME_LIMIT     = "시간초과"
    CHOPPY         = "박스권"


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
    몸통 포착 엔진 v2.3 — Prop Firm Edition

    v2.3 핵심:
      - fixed_tp_rr > 0 이면 고정 TP 모드 (trailing/소진감지 비활성)
        → win +2.0R, lose -1.0R → 손익분기 승률 33.3%
      - sl_ratio=0.6 (SL 위치 = range의 60%, 더 타이트)

    v2 핵심:
      - 수익잠금: RR 1.0 → 바닥 0.5, RR 2.0 → 바닥 1.2, RR 3.0 → 바닥 2.0
      - RR 기반 본전이동: RR 0.3 도달 후에만 SL을 진입가로 이동
      - 시간대: 장중 시간에 따라 진입 허용/차단
    """

    # 수익잠금 테이블: (RR 도달, 바닥 RR) — v2.2: 더 촘촘하게
    PROFIT_LOCK_TABLE = [
        (0.8, 0.3),   # 작은 이익이라도 보호 시작
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
        breakeven_rr:      float = 0.3,
        trailing_rr:       float = 1.0,
        exhaustion_bars:   int   = 2,
        volume_drop_ratio: float = 0.65,
        wick_ratio_min:    float = 0.003,
        choppy_max_attempts: int = 3,
        sl_ratio:          float = 0.6,    # v2.3: SL 위치 (range의 60%) — 0.7→0.6 축소
        fixed_tp_rr:       float = 0.0,    # v2.3: 고정 TP (0=비활성, 2.0=2:1 TP)
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
        self.breakeven_rr       = breakeven_rr
        self.trailing_rr        = trailing_rr
        self.exhaustion_bars    = exhaustion_bars
        self.volume_drop_ratio  = volume_drop_ratio
        self.wick_ratio_min     = wick_ratio_min
        self.choppy_max_attempts = choppy_max_attempts
        self.sl_ratio           = sl_ratio
        self.fixed_tp_rr        = fixed_tp_rr
        self.cutoff_time        = pd.Timestamp(f"2000-01-01 {cutoff_time}").time()
        self.golden_start       = pd.Timestamp(f"2000-01-01 {golden_start}").time()
        self.golden_end         = pd.Timestamp(f"2000-01-01 {golden_end}").time()

        self.state:    BodyState               = BodyState.READY
        self.levels:   Optional[BodyLevels]    = None
        self.position: Optional[BodyPosition]  = None
        self._recent_candles: List[pd.Series]  = []
        self._avg_volume:     Optional[float]  = None

        # v2.1: 박스권 감지 카운터
        self._breakout_attempts: int  = 0   # 이탈 시도 횟수 (꼬리만 닿고 복귀)
        self._retest_fails:      int  = 0   # 리테스트 실패 횟수

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
        """이탈 확인 (v2.1: 박스권 감지 포함)"""
        lv = self.levels
        o, c = candle["open"], candle["close"]
        h, l = candle["high"], candle["low"]
        v    = candle["volume"]

        # v2.1: 박스권 감지 — 이탈 시도만 반복하고 확인 못 하면 포기
        if self._breakout_attempts >= self.choppy_max_attempts:
            self.state = BodyState.DONE
            logger.warning(
                f"[{self.ticker}] 박스권 감지: 이탈 시도 {self._breakout_attempts}회 실패 → 진입 포기"
            )
            return dict(action="WAIT", reason=f"박스권({self._breakout_attempts}회 실패)")

        vol_surge = v / self._avg_volume >= self.volume_surge_min if self._avg_volume > 0 else False

        if self.close_only_breakout:
            if self.direction == "LONG":
                body_outside = c > lv.high
            else:
                body_outside = c < lv.low
        else:
            if self.direction == "LONG":
                body_outside = min(o, c) > lv.high
            else:
                body_outside = max(o, c) < lv.low

        # 꼬리만 닿고 마감은 안쪽 = 이탈 시도 실패 (박스권 카운트)
        if self.direction == "LONG":
            wick_touched = h > lv.high and c <= lv.high
        else:
            wick_touched = l < lv.low and c >= lv.low

        if wick_touched:
            self._breakout_attempts += 1
            logger.info(
                f"[{self.ticker}] 이탈 시도 실패 ({self._breakout_attempts}/{self.choppy_max_attempts}) "
                f"꼬리:{h:,.0f} > 레벨:{lv.high:,.0f} but 마감:{c:,.0f}"
            )

        if body_outside and vol_surge:
            if self.retest_required:
                self.state = BodyState.RETEST_WAIT
                logger.warning(
                    f"[{self.ticker}] FOMO 방지: 이탈 확인됐지만 리테스트 대기 필수! "
                    f"마감:{c:,.0f} > 레벨:{lv.high:,.0f}"
                )
                return dict(action="WAIT", reason="이탈확인-리테스트대기")
            else:
                return self._enter(candle, c)

        return dict(action="WAIT", reason="이탈대기중")

    def _check_retest(self, candle: pd.Series) -> dict:
        """리테스트 확인 (v2.1: 실패 카운트 + FOMO 경고)"""
        lv   = self.levels
        o, c = candle["open"], candle["close"]
        h, l = candle["high"],  candle["low"]

        if self.direction == "LONG":
            touched     = l <= lv.high
            valid_close = c > lv.high
            # 리테스트 실패: 레벨 아래로 완전히 복귀
            fell_back   = c < lv.mid
        else:
            touched     = h >= lv.low
            valid_close = c < lv.low
            fell_back   = c > lv.mid

        if touched and valid_close:
            return self._enter(candle, c)

        # v2.1: 리테스트 실패 추적 → 박스권으로 전환
        if fell_back:
            self._retest_fails += 1
            self._breakout_attempts += 1  # 박스권 카운터에도 반영
            self.state = BodyState.WATCHING  # 다시 이탈 대기로 복귀
            logger.info(
                f"[{self.ticker}] 리테스트 실패 ({self._retest_fails}회) "
                f"마감:{c:,.0f} < MID:{lv.mid:,.0f} → 이탈 대기 복귀"
            )

            if self._breakout_attempts >= self.choppy_max_attempts:
                self.state = BodyState.DONE
                logger.warning(
                    f"[{self.ticker}] 박스권 확정: 리테스트 포함 {self._breakout_attempts}회 실패 → 진입 포기"
                )
                return dict(action="WAIT", reason=f"박스권({self._breakout_attempts}회 실패)")

            return dict(action="WAIT", reason=f"리테스트실패→재감시({self._retest_fails}회)")

        logger.debug(
            f"[{self.ticker}] FOMO 방지: 리테스트 대기 중 — 아직 레벨 터치 안 됨"
        )
        return dict(action="WAIT", reason="리테스트대기중")

    def _enter(self, candle: pd.Series, entry_price: float) -> dict:
        """진입 (v2.2: SL 위치 조절 가능)"""
        lv  = self.levels
        # v2.2: sl_ratio로 SL 위치 조절 — 기존 mid 대신 high↔low 사이를 비율로
        # sl_ratio=1.0 → mid (기존), sl_ratio=0.7 → high에서 range*0.7 아래
        if self.direction == "LONG":
            sl = lv.high - lv.range_size * self.sl_ratio
        else:
            sl = lv.low + lv.range_size * self.sl_ratio

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
        """포지션 관리 (v2.3: Fixed TP 모드 / v2: 수익잠금+트레일링)"""
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

        # ── v2.3: Fixed TP 모드 (단순 2:1 청산) ──
        if self.fixed_tp_rr > 0:
            return self._manage_fixed_tp(candle, pos, risk)

        # ── 기존 v2 모드: 수익잠금 + 트레일링 ──
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
            # v2.1: 소진감지 시 수익잠금 가격 보장
            exit_price = c
            if pos.rr_floor > 0:
                lock_price = self._calc_profit_lock_price(pos)
                if self.direction == "LONG":
                    exit_price = max(c, lock_price)
                else:
                    exit_price = min(c, lock_price)
            return self._exit(candle, ExitReason.EXHAUSTION, exit_price, exhaustion)

        return dict(
            action="HOLD",
            reason=f"몸통탑승 RR:{pos.rr_current:+.2f} 바닥:{pos.rr_floor:+.2f} ({pos.hold_bars}봉)",
            position=pos, exhaustion=exhaustion,
        )

    def _manage_fixed_tp(self, candle: pd.Series, pos, risk) -> dict:
        """v2.3: 고정 TP 모드 — SL or TP, 그 외 없음

        장중 고가/저가로 TP/SL 히트 판정 (봉 내 동시 히트 시 불리한 쪽 우선)
        """
        c, h, l = candle["close"], candle["high"], candle["low"]

        # TP 가격 계산
        if self.direction == "LONG":
            tp_price = pos.entry_price + risk * self.fixed_tp_rr
            tp_hit = h >= tp_price
            sl_hit = l <= pos.stop_loss
        else:
            tp_price = pos.entry_price - risk * self.fixed_tp_rr
            tp_hit = l <= tp_price
            sl_hit = h >= pos.stop_loss

        # 동시 히트: SL 우선 (보수적 — 봉 내에서 SL 먼저 맞았을 가능성)
        if sl_hit and tp_hit:
            return self._exit(candle, ExitReason.STOP_LOSS, pos.stop_loss)

        if tp_hit:
            return self._exit(candle, ExitReason.TAKE_PROFIT, tp_price)

        if sl_hit:
            return self._exit(candle, ExitReason.STOP_LOSS, pos.stop_loss)

        return dict(
            action="HOLD",
            reason=f"Fixed TP RR:{pos.rr_current:+.2f} TP@{self.fixed_tp_rr:.1f}R ({pos.hold_bars}봉)",
            position=pos, exhaustion=None,
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
        """v2.1: RR 기반 트레일링 SL (시간 기반 → 성과 기반)"""
        pos = self.position
        lv  = self.levels

        # 피크 업데이트
        if self.direction == "LONG":
            if candle["high"] > pos.peak_price:
                pos.peak_price = candle["high"]
        else:
            if candle["low"] < pos.peak_price:
                pos.peak_price = candle["low"]

        # Phase 1: RR < breakeven_rr → SL 고정 (원래 손절가)
        #   실제로 움직이지 않으면 절대 본전 안 줌
        if pos.rr_current < self.breakeven_rr:
            pos.trailing_sl = pos.stop_loss

        # Phase 2: RR >= breakeven_rr → 본전 이동
        #   실제로 RR 0.3 이상 갔다가 돌아오면 본전 탈출
        elif pos.rr_current < self.trailing_rr:
            if self.direction == "LONG":
                pos.trailing_sl = max(pos.trailing_sl, pos.entry_price)
            else:
                pos.trailing_sl = min(pos.trailing_sl, pos.entry_price)

        # Phase 3: RR >= trailing_rr → ATR 트레일링
        #   본격 추세구간 — 피크에서 ATR 만큼 뒤에서 따라감
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
            ExitReason.STOP_LOSS: "X", ExitReason.TAKE_PROFIT: "$",
            ExitReason.EXHAUSTION: "!", ExitReason.TRAILING_STOP: "T",
            ExitReason.TIME_LIMIT: "C", ExitReason.PROFIT_LOCK: "L",
            ExitReason.CHOPPY: "~",
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
        self._breakout_attempts = 0
        self._retest_fails = 0
