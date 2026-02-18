"""
몸통 포착 엔진 v3 (Body Hunter v3) — FVG + Engulfing Edition

v3 (vs v2.3):
  - FVG (Fair Value Gap) 감지: 돌파 후 3봉 구조의 갭 자동 탐지
  - Engulfing 진입 트리거: FVG 안에서 감싸기봉 확인 후 진입
  - 3:1 RR (v2.3은 2:1)
  - SL = FVG 하단 (v2.3은 range × 0.6)

핵심 차이:
  v2.3: high 돌파 → 리테스트 → 바로 진입  (SL=range×0.6, TP=2R)
  v3:   high 돌파 → FVG 형성 대기 → FVG 안 Engulfing → 진입  (SL=FVG하단, TP=3R)

  → SL이 더 타이트 → RR 3:1 가능 → 손익분기 승률 25%

상태 머신:
  READY → WATCHING → FVG_WAIT → ENGULF_WAIT → IN_BODY → DONE
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
    FVG_WAIT       = "FVG대기"       # v3: 돌파 후 FVG 형성 대기
    ENGULF_WAIT    = "감싸기대기"      # v3: FVG 안에서 Engulfing 대기
    IN_BODY        = "몸통탑승"
    DONE           = "완료"


class ExitReason(Enum):
    STOP_LOSS      = "손절"
    TAKE_PROFIT    = "익절"
    TRAILING_STOP  = "트레일링"
    PROFIT_LOCK    = "수익잠금"
    TIME_LIMIT     = "시간초과"
    CHOPPY         = "박스권"
    FVG_TIMEOUT    = "FVG미형성"      # v3: FVG 안 생겨서 포기


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
class FVGZone:
    """Fair Value Gap 영역"""
    top:    float     # FVG 상단 (candle3.low for bullish)
    bottom: float     # FVG 하단 (candle1.high for bullish)
    formed_at: int    # 형성된 봉 인덱스

    @property
    def size(self):
        return abs(self.top - self.bottom)

    @property
    def mid(self):
        return (self.top + self.bottom) / 2


@dataclass
class BodyPosition:
    direction:    str
    entry_price:  float
    stop_loss:    float
    trailing_sl:  float
    peak_price:   float
    entry_time:   object
    fvg:          Optional[FVGZone] = None   # v3: 진입 근거 FVG
    hold_bars:    int = 0
    rr_current:   float = 0.0
    rr_floor:     float = -1.0
    exhaustion_signals: List[str] = field(default_factory=list)


@dataclass
class ExhaustionSignal:
    detected:  bool
    signals:   List[str]
    urgency:   int


class BodyHunterV3:
    """
    몸통 포착 엔진 v3 — FVG + Engulfing Edition

    핵심:
      1. 첫봉 레벨 마킹 (FCR) — v2와 동일
      2. 돌파 확인 — v2와 동일
      3. FVG 감지 — v3 신규 (3봉 구조에서 갭 찾기)
      4. Engulfing 진입 — v3 신규 (FVG 안에서 감싸기봉)
      5. SL = FVG 하단, TP = 3:1 RR
    """

    PROFIT_LOCK_TABLE = [
        (1.0, 0.3),
        (1.5, 0.8),
        (2.0, 1.2),
        (2.5, 1.6),
        (3.0, 2.2),
        (4.0, 3.0),
    ]

    def __init__(
        self,
        ticker:            str,
        name:              str   = "",
        direction:         str   = "LONG",
        volume_surge_min:  float = 1.5,
        close_only_breakout: bool = False,
        trailing_atr_mult: float = 1.2,
        breakeven_rr:      float = 0.3,
        trailing_rr:       float = 1.0,
        exhaustion_bars:   int   = 2,
        volume_drop_ratio: float = 0.65,
        wick_ratio_min:    float = 0.003,
        choppy_max_attempts: int = 3,
        fixed_tp_rr:       float = 3.0,     # v3: 3:1 RR
        fvg_timeout_bars:  int   = 15,      # v3: FVG 안 생기면 포기할 봉 수
        fvg_min_size_pct:  float = 0.001,   # v3: 최소 FVG 크기 (가격 대비 %)
        engulf_timeout_bars: int = 10,      # v3: FVG 후 Engulfing 대기 봉 수
        fvg_fcr_proximity: float = 1.5,     # v3.1: FVG가 FCR high/low의 range*N 이내
        sl_buffer_ratio:   float = 0.1,     # v3.1: SL = FVG하단 바깥쪽 (FVG크기의 N%)
        cutoff_time:       str   = "15:00",
        golden_start:      str   = "09:20",
        golden_end:        str   = "11:30",
    ):
        self.ticker             = ticker
        self.name               = name
        self.direction          = direction
        self.volume_surge_min   = volume_surge_min
        self.close_only_breakout = close_only_breakout
        self.trailing_atr_mult  = trailing_atr_mult
        self.breakeven_rr       = breakeven_rr
        self.trailing_rr        = trailing_rr
        self.exhaustion_bars    = exhaustion_bars
        self.volume_drop_ratio  = volume_drop_ratio
        self.wick_ratio_min     = wick_ratio_min
        self.choppy_max_attempts = choppy_max_attempts
        self.fixed_tp_rr        = fixed_tp_rr
        self.fvg_timeout_bars   = fvg_timeout_bars
        self.fvg_min_size_pct   = fvg_min_size_pct
        self.engulf_timeout_bars = engulf_timeout_bars
        self.fvg_fcr_proximity  = fvg_fcr_proximity
        self.sl_buffer_ratio    = sl_buffer_ratio
        self.cutoff_time        = pd.Timestamp(f"2000-01-01 {cutoff_time}").time()
        self.golden_start       = pd.Timestamp(f"2000-01-01 {golden_start}").time()
        self.golden_end         = pd.Timestamp(f"2000-01-01 {golden_end}").time()

        self.state:    BodyState               = BodyState.READY
        self.levels:   Optional[BodyLevels]    = None
        self.position: Optional[BodyPosition]  = None
        self.fvg:      Optional[FVGZone]       = None
        self._recent_candles: List[pd.Series]  = []
        self._avg_volume:     Optional[float]  = None

        self._breakout_attempts: int = 0
        self._bars_since_breakout: int = 0   # v3: 돌파 후 경과 봉
        self._bars_since_fvg: int = 0        # v3: FVG 후 경과 봉
        self._bar_count: int = 0

    def set_levels(self, first_candle: pd.Series, avg_volume: float = None):
        """첫봉으로 레벨 마킹 (FCR)"""
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
        self._bar_count += 1

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
        if len(self._recent_candles) > 20:
            self._recent_candles.pop(0)

        if self.state == BodyState.WATCHING:
            result = self._check_breakout(candle)
        elif self.state == BodyState.FVG_WAIT:
            result = self._check_fvg(candle)
        elif self.state == BodyState.ENGULF_WAIT:
            result = self._check_engulfing(candle)
        elif self.state == BodyState.IN_BODY:
            result = self._manage_position(candle)

        return result

    # ═══════════════════════════════════════
    #  Phase 1: 돌파 확인 (v2.3과 동일)
    # ═══════════════════════════════════════

    def _check_breakout(self, candle: pd.Series) -> dict:
        """이탈 확인 — 박스권 감지 포함"""
        lv = self.levels
        o, c = candle["open"], candle["close"]
        h, l = candle["high"], candle["low"]
        v    = candle["volume"]

        if self._breakout_attempts >= self.choppy_max_attempts:
            self.state = BodyState.DONE
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

        # 꼬리만 닿고 마감은 안쪽 = 이탈 시도 실패
        if self.direction == "LONG":
            wick_touched = h > lv.high and c <= lv.high
        else:
            wick_touched = l < lv.low and c >= lv.low

        if wick_touched:
            self._breakout_attempts += 1

        if body_outside and vol_surge:
            # v3: 리테스트 대신 FVG 대기로 전환
            self.state = BodyState.FVG_WAIT
            self._bars_since_breakout = 0
            logger.info(
                f"[{self.ticker}] 돌파 확인 → FVG 대기 "
                f"마감:{c:,.0f} > 레벨:{lv.high:,.0f}"
            )
            return dict(action="WAIT", reason="돌파확인→FVG대기")

        return dict(action="WAIT", reason="이탈대기중")

    # ═══════════════════════════════════════
    #  Phase 2: FVG 감지 (v3 신규)
    # ═══════════════════════════════════════

    def _check_fvg(self, candle: pd.Series) -> dict:
        """FVG (Fair Value Gap) 감지 — FCR high/low 근처에서만 유효 (v3.1)"""
        self._bars_since_breakout += 1

        # 타임아웃: FVG 안 생기면 포기
        if self._bars_since_breakout > self.fvg_timeout_bars:
            self.state = BodyState.DONE
            logger.info(f"[{self.ticker}] FVG 미형성 ({self.fvg_timeout_bars}봉 초과) → 포기")
            return dict(action="WAIT", reason="FVG미형성→포기")

        # FVG 감지를 위해 최소 3봉 필요
        if len(self._recent_candles) < 3:
            return dict(action="WAIT", reason="FVG대기중")

        # 최근 3봉에서 FVG 찾기
        c1 = self._recent_candles[-3]  # 첫번째 봉
        c2 = self._recent_candles[-2]  # 임펄스 봉
        c3 = self._recent_candles[-1]  # 세번째 봉 (= candle)

        lv = self.levels
        fcr_range = lv.range_size if lv.range_size > 0 else 1

        if self.direction == "LONG":
            # Bullish FVG: c1.high < c3.low → 갭 존재
            if c1["high"] < c3["low"]:
                fvg_bottom = c1["high"]
                fvg_top = c3["low"]
                fvg_size_pct = (fvg_top - fvg_bottom) / fvg_bottom

                if fvg_size_pct >= self.fvg_min_size_pct:
                    # v3.1: FCR high 근처 FVG만 유효
                    # FVG 중심이 FCR high에서 range*N 이내여야 함
                    fvg_mid = (fvg_top + fvg_bottom) / 2
                    dist = abs(fvg_mid - lv.high) / fcr_range
                    if dist > self.fvg_fcr_proximity:
                        logger.debug(
                            f"[{self.ticker}] FVG 감지 but FCR 거리 초과 "
                            f"({dist:.1f}x > {self.fvg_fcr_proximity}x)"
                        )
                        return dict(action="WAIT",
                                    reason=f"FVG감지but FCR거리초과({dist:.1f}x)")

                    self.fvg = FVGZone(
                        top=fvg_top, bottom=fvg_bottom,
                        formed_at=self._bar_count,
                    )
                    self.state = BodyState.ENGULF_WAIT
                    self._bars_since_fvg = 0
                    logger.info(
                        f"[{self.ticker}] FVG 감지 (FCR근처 {dist:.1f}x)! "
                        f"구간: {fvg_bottom:,.0f}~{fvg_top:,.0f} "
                        f"크기: {fvg_size_pct*100:.2f}%"
                    )
                    return dict(action="WAIT", reason=f"FVG감지→Engulfing대기")

        else:  # SHORT
            # Bearish FVG: c1.low > c3.high → 갭 존재
            if c1["low"] > c3["high"]:
                fvg_top = c1["low"]
                fvg_bottom = c3["high"]
                fvg_size_pct = (fvg_top - fvg_bottom) / fvg_top

                if fvg_size_pct >= self.fvg_min_size_pct:
                    # v3.1: FCR low 근처 FVG만 유효
                    fvg_mid = (fvg_top + fvg_bottom) / 2
                    dist = abs(fvg_mid - lv.low) / fcr_range
                    if dist > self.fvg_fcr_proximity:
                        logger.debug(
                            f"[{self.ticker}] Bearish FVG but FCR 거리 초과"
                        )
                        return dict(action="WAIT",
                                    reason=f"FVG감지but FCR거리초과({dist:.1f}x)")

                    self.fvg = FVGZone(
                        top=fvg_top, bottom=fvg_bottom,
                        formed_at=self._bar_count,
                    )
                    self.state = BodyState.ENGULF_WAIT
                    self._bars_since_fvg = 0
                    logger.info(
                        f"[{self.ticker}] Bearish FVG 감지 (FCR근처 {dist:.1f}x)! "
                        f"구간: {fvg_bottom:,.0f}~{fvg_top:,.0f}"
                    )
                    return dict(action="WAIT", reason=f"FVG감지→Engulfing대기")

        return dict(action="WAIT", reason=f"FVG탐색중({self._bars_since_breakout}/{self.fvg_timeout_bars})")

    # ═══════════════════════════════════════
    #  Phase 3: Engulfing 진입 (v3 신규)
    # ═══════════════════════════════════════

    def _check_engulfing(self, candle: pd.Series) -> dict:
        """FVG 구간 안에서 Engulfing 패턴 확인 → 진입"""
        self._bars_since_fvg += 1

        # 타임아웃
        if self._bars_since_fvg > self.engulf_timeout_bars:
            self.state = BodyState.DONE
            logger.info(f"[{self.ticker}] Engulfing 미발생 → 포기")
            return dict(action="WAIT", reason="Engulfing미발생→포기")

        if len(self._recent_candles) < 2:
            return dict(action="WAIT", reason="Engulfing대기중")

        fvg = self.fvg
        prev = self._recent_candles[-2]
        curr = candle

        o, c = curr["open"], curr["close"]
        h, l = curr["high"], curr["low"]
        po, pc = prev["open"], prev["close"]

        if self.direction == "LONG":
            # 가격이 FVG 구간 안에 들어왔는지 확인
            price_in_fvg = l <= fvg.top  # 저가가 FVG 상단 이하로 내려옴

            # Bullish Engulfing: 이전봉 음봉 + 현재봉이 이전봉 감싸기
            prev_bearish = pc < po
            curr_bullish = c > o
            engulfing = (
                prev_bearish
                and curr_bullish
                and c > po           # 현재 종가 > 이전 시가
                and o <= pc          # 현재 시가 <= 이전 종가
            )

            # FVG 안에서 Engulfing 확인
            if price_in_fvg and engulfing:
                return self._enter_fvg(candle, c)

            # FVG 완전히 하향 이탈 (= SL 레벨 붕괴) → 포기
            if c < fvg.bottom:
                self.state = BodyState.DONE
                logger.info(f"[{self.ticker}] FVG 하단 이탈 → 포기")
                return dict(action="WAIT", reason="FVG하단이탈→포기")

        else:  # SHORT
            price_in_fvg = h >= fvg.bottom

            prev_bullish = pc > po
            curr_bearish = c < o
            engulfing = (
                prev_bullish
                and curr_bearish
                and c < po
                and o >= pc
            )

            if price_in_fvg and engulfing:
                return self._enter_fvg(candle, c)

            if c > fvg.top:
                self.state = BodyState.DONE
                return dict(action="WAIT", reason="FVG상단이탈→포기")

        return dict(action="WAIT", reason=f"Engulfing탐색중({self._bars_since_fvg}/{self.engulf_timeout_bars})")

    def _enter_fvg(self, candle: pd.Series, entry_price: float) -> dict:
        """FVG 기반 진입 — SL = FVG 하단 바깥쪽 (v3.1)"""
        fvg = self.fvg
        buffer = fvg.size * self.sl_buffer_ratio  # FVG 크기의 N% 바깥

        if self.direction == "LONG":
            sl = fvg.bottom - buffer   # SL = FVG 하단 바깥쪽
        else:
            sl = fvg.top + buffer      # SL = FVG 상단 바깥쪽

        self.position = BodyPosition(
            direction   = self.direction,
            entry_price = entry_price,
            stop_loss   = sl,
            trailing_sl = sl,
            peak_price  = entry_price,
            entry_time  = candle.name if hasattr(candle, "name") else None,
            fvg         = fvg,
            rr_floor    = -1.0,
        )
        self.state = BodyState.IN_BODY

        risk = abs(entry_price - sl)
        tp = entry_price + risk * self.fixed_tp_rr if self.direction == "LONG" \
            else entry_price - risk * self.fixed_tp_rr

        logger.info(
            f"[{self.ticker}] FVG Engulfing 진입 [{self.direction}] "
            f"진입:{entry_price:,.0f} SL:{sl:,.0f}(FVG하단) "
            f"TP:{tp:,.0f}({self.fixed_tp_rr:.0f}R) 리스크:{risk:,.0f}"
        )
        return dict(action="ENTER", reason=f"{self.direction} FVG진입", position=self.position)

    # ═══════════════════════════════════════
    #  Phase 4: 포지션 관리
    # ═══════════════════════════════════════

    def _manage_position(self, candle: pd.Series) -> dict:
        """포지션 관리 — Fixed TP 모드 (3:1 RR)"""
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

        # Fixed TP 모드
        return self._manage_fixed_tp(candle, pos, risk)

    def _manage_fixed_tp(self, candle: pd.Series, pos, risk) -> dict:
        """Fixed TP — SL or TP only (3:1 RR)"""
        c, h, l = candle["close"], candle["high"], candle["low"]

        if self.direction == "LONG":
            tp_price = pos.entry_price + risk * self.fixed_tp_rr
            tp_hit = h >= tp_price
            sl_hit = l <= pos.stop_loss
        else:
            tp_price = pos.entry_price - risk * self.fixed_tp_rr
            tp_hit = l <= tp_price
            sl_hit = h >= pos.stop_loss

        # 동시 히트: SL 우선 (보수적)
        if sl_hit and tp_hit:
            return self._exit(candle, ExitReason.STOP_LOSS, pos.stop_loss)

        if tp_hit:
            return self._exit(candle, ExitReason.TAKE_PROFIT, tp_price)

        if sl_hit:
            return self._exit(candle, ExitReason.STOP_LOSS, pos.stop_loss)

        return dict(
            action="HOLD",
            reason=f"FVG진입 RR:{pos.rr_current:+.2f} TP@{self.fixed_tp_rr:.1f}R ({pos.hold_bars}봉)",
            position=pos, exhaustion=None,
        )

    # ═══════════════════════════════════════
    #  공통
    # ═══════════════════════════════════════

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
            ExitReason.TRAILING_STOP: "T", ExitReason.TIME_LIMIT: "C",
            ExitReason.PROFIT_LOCK: "L", ExitReason.CHOPPY: "~",
            ExitReason.FVG_TIMEOUT: "?",
        }
        logger.info(
            f"[{icon_map.get(reason, '?')}] [{self.ticker}] 청산 [{reason.value}] "
            f"진입:{pos.entry_price:,.0f} → 청산:{exit_price:,.0f} "
            f"RR:{rr_realized:+.2f} | {pos.hold_bars}봉"
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
        self.fvg = None
        self._recent_candles = []
        self._breakout_attempts = 0
        self._bars_since_breakout = 0
        self._bars_since_fvg = 0
        self._bar_count = 0
