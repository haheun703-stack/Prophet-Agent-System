# -*- coding: utf-8 -*-
"""
Intraday AI Eye v2 — 장중 5-Layer 실시간 흐름 분석
═══════════════════════════════════════════════════
매 5분마다 보유종목을 보고 딱 하나만 판단한다: "이 종목 지금 살아있나, 죽어가나"

5-Layer 분석:
  L1. EMA 정배열/역배열 (EYE-01)
  L2. VWAP 위치 + σ밴드 (EYE-02)
  L3. 볼륨 프로파일 지지/저항 (EYE-03)
  L4. 거래량/수급 흐름 (EYE-04)
  L5. 모멘텀 RSI+MACD (EYE-05)

종합 판정 (EYE-06):
  ALIVE / WEAKENING / DYING / BOUNCING / BREAKING

데이터: 라이브 = KIS fetch_price 5분 버퍼 / 백테스트 = 1min CSV
"""

import csv
import logging
import math
import os
import sys
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, date
from pathlib import Path
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger("BH.IntradayEye")

BASE_DIR = Path(__file__).resolve().parent.parent
MIN1_DIR = BASE_DIR / "data_store" / "1min"


# ════════════════════════════════════════════
# 데이터 구조
# ════════════════════════════════════════════

@dataclass
class IntradayBar:
    """5분봉 1개"""
    timestamp: datetime
    price: int          # close
    open_price: int
    high: int
    low: int
    volume: int         # 5분간 순증 거래량
    cum_volume: int     # 누적 거래량 (VWAP용)
    strength: float     # 체결강도
    change_rate: float  # 전일대비 등락률


@dataclass
class EyeVerdict:
    """AI Eye 분석 결과"""
    code: str
    name: str
    timestamp: datetime

    # 레이어별 점수 (각 0~100)
    l1_price_structure: float
    l2_volume_supply: float
    l3_momentum: float

    # 종합
    composite_score: float
    verdict: str          # ALIVE/WEAKENING/DYING/BOUNCING/BREAKING
    prev_verdict: str
    confidence: float     # 0~1
    summary: str

    # v2 확장
    details: dict = field(default_factory=dict)    # 5-Layer 상세
    action: str = "HOLD"                            # HOLD/WATCH/EXIT
    action_params: dict = field(default_factory=dict)  # trailing_sl, tp_adjust


# ════════════════════════════════════════════
# 수학 헬퍼
# ════════════════════════════════════════════

def _clamp(val: float, lo: float = 0.0, hi: float = 100.0) -> float:
    return max(lo, min(hi, val))


def _ema_list(values: list, period: int) -> list:
    """EMA 계산 → 전체 리스트 반환"""
    if not values:
        return []
    if len(values) < period:
        avg = sum(values) / len(values)
        return [avg] * len(values)
    k = 2 / (period + 1)
    result = [float(values[0])]
    for i in range(1, len(values)):
        result.append(values[i] * k + result[-1] * (1 - k))
    return result


def _rsi(prices: list, period: int = 14) -> float:
    """RSI 계산 (Wilder 스무딩)"""
    if len(prices) < period + 1:
        return 50.0
    gains = []
    losses = []
    for i in range(1, len(prices)):
        d = prices[i] - prices[i - 1]
        gains.append(max(0, d))
        losses.append(max(0, -d))
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100 - 100 / (1 + rs)


def _macd(prices: list, fast: int = 5, slow: int = 13, signal: int = 4) -> Tuple[float, float, float]:
    """MACD(fast, slow, signal) → (macd_line, signal_line, histogram)"""
    if len(prices) < slow + signal:
        return 0, 0, 0
    ema_fast = _ema_list(prices, fast)
    ema_slow = _ema_list(prices, slow)
    macd_line = [f - s for f, s in zip(ema_fast, ema_slow)]
    sig_line = _ema_list(macd_line, signal)
    hist = [m - s for m, s in zip(macd_line, sig_line)]
    return macd_line[-1], sig_line[-1], hist[-1]


# ════════════════════════════════════════════
# IntradayBuffer — 종목별 5분봉 메모리
# ════════════════════════════════════════════

class IntradayBuffer:
    """종목별 5분봉 deque(maxlen=78) = 6.5시간/5분"""

    def __init__(self, code: str, name: str = ""):
        self.code = code
        self.name = name
        self.bars: deque = deque(maxlen=78)
        self._prev_cum_volume: int = 0
        self._prev_vwap_position: str = ""  # VWAP 이전 위치 (전이 감지)
        self._sr_touches: Dict[int, dict] = {}  # 가격대별 터치 기록

    def add_bar(self, price_data: dict) -> Optional[IntradayBar]:
        """fetch_price 결과 → IntradayBar 생성"""
        cum_vol = price_data.get("volume", 0)
        delta_vol = max(0, cum_vol - self._prev_cum_volume) if self._prev_cum_volume > 0 else 0
        self._prev_cum_volume = cum_vol

        bar = IntradayBar(
            timestamp=datetime.now(),
            price=price_data.get("current_price", 0),
            open_price=price_data.get("open", 0),
            high=price_data.get("high", 0),
            low=price_data.get("low", 0),
            volume=delta_vol,
            cum_volume=cum_vol,
            strength=price_data.get("strength", 0.0),
            change_rate=price_data.get("change_rate", 0.0),
        )
        self.bars.append(bar)
        return bar

    def load_from_1min_csv(self, today: str = None) -> int:
        """1분봉 CSV 로드 → 5분봉 리샘플링 → 버퍼 채움 (warm-start / 백테스트)"""
        today = today or date.today().isoformat()
        csv_path = MIN1_DIR / f"{self.code}.csv"
        if not csv_path.exists():
            return 0

        # 오늘 1분봉만 필터
        rows = []
        try:
            with open(csv_path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    dt_str = row.get("datetime", "")
                    if dt_str[:10] == today:
                        rows.append(row)
        except Exception as e:
            logger.warning(f"1min CSV 로드 실패 {self.code}: {e}")
            return 0

        if not rows:
            return 0

        # 5분봉 리샘플링
        bars_added = 0
        bucket = []
        bucket_key = ""

        for row in rows:
            dt_str = row["datetime"]
            # 5분 단위로 그룹핑: "2026-03-19 09:01" → "2026-03-19 09:00"
            try:
                dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                continue
            minute_5 = (dt.minute // 5) * 5
            key = f"{dt.strftime('%Y-%m-%d %H')}:{minute_5:02d}"

            if key != bucket_key and bucket:
                bar = self._resample_bucket(bucket, bucket_key)
                if bar:
                    self.bars.append(bar)
                    bars_added += 1
                bucket = []
            bucket_key = key
            bucket.append(row)

        # 마지막 버킷
        if bucket:
            bar = self._resample_bucket(bucket, bucket_key)
            if bar:
                self.bars.append(bar)
                bars_added += 1

        return bars_added

    def _resample_bucket(self, rows: list, key: str) -> Optional[IntradayBar]:
        """1분봉 버킷 → 5분봉 1개"""
        if not rows:
            return None
        try:
            dt = datetime.strptime(key, "%Y-%m-%d %H:%M")
            o = int(float(rows[0].get("open", 0)))
            c = int(float(rows[-1].get("close", 0)))
            h = max(int(float(r.get("high", 0))) for r in rows)
            l = min(int(float(r.get("low", 0))) for r in rows)
            v = sum(int(float(r.get("volume", 0))) for r in rows)
            return IntradayBar(
                timestamp=dt, price=c, open_price=o, high=h, low=l,
                volume=v, cum_volume=v, strength=0, change_rate=0,
            )
        except Exception:
            return None

    @property
    def count(self) -> int:
        return len(self.bars)

    @property
    def latest(self) -> Optional[IntradayBar]:
        return self.bars[-1] if self.bars else None

    def prices(self, n: int = 0) -> List[int]:
        bars = list(self.bars)[-n:] if n > 0 else list(self.bars)
        return [b.price for b in bars]

    def volumes(self, n: int = 0) -> List[int]:
        bars = list(self.bars)[-n:] if n > 0 else list(self.bars)
        return [b.volume for b in bars]

    def strengths(self, n: int = 0) -> List[float]:
        bars = list(self.bars)[-n:] if n > 0 else list(self.bars)
        return [b.strength for b in bars]


# ════════════════════════════════════════════
# EYE-01: EMA 정배열/역배열 판정
# ════════════════════════════════════════════

def calc_intraday_ma(buf: IntradayBuffer) -> dict:
    """5분봉 EMA(5/12/48) → 배열 판정"""
    prices = [float(p) for p in buf.prices()]
    if len(prices) < 5:
        return {"alignment": "UNKNOWN", "ema_spread": 0, "score": 50}

    ema5 = _ema_list(prices, 5)[-1]
    ema12 = _ema_list(prices, 12)[-1]
    # EMA48 = ~4시간 (48봉 × 5분 = 240분), 부족하면 가용 데이터로 근사
    ema48_period = min(48, max(12, len(prices) - 1))
    ema48 = _ema_list(prices, ema48_period)[-1]

    # 배열 판정
    spread = (ema5 - ema48) / ema48 * 100 if ema48 > 0 else 0
    if ema5 > ema12 > ema48:
        alignment = "BULLISH"
    elif ema5 < ema12 < ema48:
        alignment = "BEARISH"
    elif abs(spread) < 0.3:
        alignment = "CONVERGING"
    else:
        alignment = "MIXED"

    # 점수 (0~100)
    if alignment == "BULLISH":
        score = _clamp(60 + spread * 20)  # spread가 클수록 높은 점수
    elif alignment == "BEARISH":
        score = _clamp(40 + spread * 20)  # spread가 음수라 점수 낮아짐
    elif alignment == "CONVERGING":
        score = 50
    else:
        score = 45

    return {
        "ema_5": round(ema5),
        "ema_12": round(ema12),
        "ema_48": round(ema48),
        "alignment": alignment,
        "ema_spread": round(spread, 2),
        "score": round(score, 1),
    }


# ════════════════════════════════════════════
# EYE-02: VWAP 위치 + σ 밴드
# ════════════════════════════════════════════

def calc_vwap_position(buf: IntradayBuffer) -> dict:
    """VWAP + 1σ 밴드 → 위치 판정 + 전이 감지"""
    bars = list(buf.bars)
    if not bars or bars[-1].price <= 0:
        return {"position": "UNKNOWN", "distance_pct": 0, "score": 50}

    price = bars[-1].price

    # VWAP = Σ(price × volume) / Σ(volume)
    total_pv = sum(b.price * b.volume for b in bars if b.volume > 0)
    total_v = sum(b.volume for b in bars if b.volume > 0)
    vwap = total_pv / total_v if total_v > 0 else float(price)

    # σ = sqrt(Σ(vol × (price - vwap)²) / Σ(vol))
    variance_sum = sum(b.volume * (b.price - vwap) ** 2 for b in bars if b.volume > 0)
    sigma = math.sqrt(variance_sum / total_v) if total_v > 0 else 0

    upper = vwap + sigma
    lower = vwap - sigma
    distance_pct = (price - vwap) / vwap * 100 if vwap > 0 else 0

    # 위치 판정
    if sigma > 0:
        if price > upper:
            position = "STRONG_ABOVE"
        elif price > vwap + vwap * 0.003:  # +0.3%
            position = "ABOVE"
        elif price >= vwap - vwap * 0.003:
            position = "AT_VWAP"
        elif price > lower:
            position = "BELOW"
        else:
            position = "STRONG_BELOW"
    else:
        position = "AT_VWAP" if abs(distance_pct) < 0.3 else ("ABOVE" if distance_pct > 0 else "BELOW")

    # 전이 감지
    prev = buf._prev_vwap_position
    transition = "NONE"
    if prev:
        was_above = prev in ("STRONG_ABOVE", "ABOVE")
        is_above = position in ("STRONG_ABOVE", "ABOVE")
        was_below = prev in ("STRONG_BELOW", "BELOW")
        is_below = position in ("STRONG_BELOW", "BELOW")
        if was_below and is_above:
            transition = "BREAK_ABOVE"
        elif was_above and is_below:
            transition = "BREAK_BELOW"
    buf._prev_vwap_position = position

    # 점수
    score_map = {"STRONG_ABOVE": 90, "ABOVE": 70, "AT_VWAP": 50, "BELOW": 30, "STRONG_BELOW": 10}
    score = score_map.get(position, 50)

    return {
        "vwap": round(vwap),
        "vwap_upper": round(upper),
        "vwap_lower": round(lower),
        "sigma": round(sigma),
        "position": position,
        "distance_pct": round(distance_pct, 2),
        "transition": transition,
        "score": score,
    }


# ════════════════════════════════════════════
# EYE-03: 장중 지지/저항 (볼륨 프로파일)
# ════════════════════════════════════════════

def _price_bucket(price: int) -> int:
    """가격을 0.2% 단위 버킷으로 그룹핑"""
    if price <= 0:
        return 0
    bucket_size = max(1, int(price * 0.002))  # 0.2%
    return (price // bucket_size) * bucket_size


def calc_support_resistance(buf: IntradayBuffer) -> dict:
    """볼륨 프로파일 기반 지지/저항 계산"""
    bars = list(buf.bars)
    if len(bars) < 3:
        return {"supports": [], "resistances": [], "score": 50}

    price = bars[-1].price
    today_high = max(b.high for b in bars)
    today_low = min(b.low for b in bars)
    open_price = bars[0].open_price

    # 볼륨 프로파일: 가격대별 누적 거래량
    vol_profile: Dict[int, int] = {}
    for b in bars:
        bucket = _price_bucket(b.price)
        vol_profile[bucket] = vol_profile.get(bucket, 0) + b.volume

    # 거래량 상위 5개 노드
    nodes = sorted(vol_profile.items(), key=lambda x: x[1], reverse=True)[:5]

    # 지지/저항 분류
    supports = []
    resistances = []
    for node_price, node_vol in nodes:
        # 터치 횟수: 해당 가격대에 ±0.3% 이내로 접근한 횟수
        touches = sum(1 for b in bars if abs(b.price - node_price) / max(1, node_price) < 0.003)
        strength = "STRONG" if touches >= 3 else ("MODERATE" if touches >= 2 else "WEAK")

        entry = {"price": node_price, "strength": strength, "touches": touches, "volume": node_vol}
        if node_price < price:
            supports.append(entry)
        else:
            resistances.append(entry)

    # 오늘 고/저도 추가
    if today_high > price and not any(abs(r["price"] - today_high) / max(1, today_high) < 0.003 for r in resistances):
        resistances.append({"price": today_high, "strength": "HIGH_OF_DAY", "touches": 1, "volume": 0})
    if today_low < price and not any(abs(s["price"] - today_low) / max(1, today_low) < 0.003 for s in supports):
        supports.append({"price": today_low, "strength": "LOW_OF_DAY", "touches": 1, "volume": 0})

    # 점수: 가장 가까운 지지/저항 대비 위치
    nearest_sup = max((s["price"] for s in supports), default=today_low) if supports else today_low
    nearest_res = min((r["price"] for r in resistances), default=today_high) if resistances else today_high

    range_total = nearest_res - nearest_sup
    if range_total > 0:
        position_ratio = (price - nearest_sup) / range_total  # 0=지지선, 1=저항선
        score = _clamp(position_ratio * 60 + 20)  # 지지선 근처면 높은 점수
    else:
        score = 50

    return {
        "supports": sorted(supports, key=lambda x: x["price"], reverse=True)[:3],
        "resistances": sorted(resistances, key=lambda x: x["price"])[:3],
        "today_high": today_high,
        "today_low": today_low,
        "open": open_price,
        "score": round(score, 1),
    }


# ════════════════════════════════════════════
# EYE-04: 거래량/수급 흐름
# ════════════════════════════════════════════

def calc_volume_flow(buf: IntradayBuffer) -> dict:
    """거래량 가속도 + 체결강도 + 수급 상태"""
    bars = list(buf.bars)
    if len(bars) < 4:
        return {"flow_state": "UNKNOWN", "score": 50}

    # (1) 거래량 가속도: 최근 6봉(30분) vs 이전 6봉(30분)
    window = min(6, len(bars) // 2)
    recent_vols = [b.volume for b in bars[-window:]]
    prev_vols = [b.volume for b in bars[-window * 2:-window]]
    recent_avg = sum(recent_vols) / len(recent_vols) if recent_vols else 0
    prev_avg = sum(prev_vols) / len(prev_vols) if prev_vols else 0
    vol_accel = recent_avg / prev_avg if prev_avg > 0 else 1.0

    # 전체 평균 대비 비율
    all_vols = [b.volume for b in bars]
    total_avg = sum(all_vols) / len(all_vols) if all_vols else 0
    vol_ratio = recent_avg / total_avg if total_avg > 0 else 1.0

    # 거래량 트렌드
    if vol_accel >= 1.5:
        vol_trend = "SURGING"
    elif vol_accel >= 1.1:
        vol_trend = "INCREASING"
    elif vol_accel >= 0.9:
        vol_trend = "STABLE"
    else:
        vol_trend = "DECREASING"

    # (2) 체결강도 추이
    recent_strs = [b.strength for b in bars[-window:] if b.strength > 0]
    prev_strs = [b.strength for b in bars[-window * 2:-window] if b.strength > 0]
    latest_str = bars[-1].strength
    avg_recent_str = sum(recent_strs) / len(recent_strs) if recent_strs else 0
    avg_prev_str = sum(prev_strs) / len(prev_strs) if prev_strs else 0
    str_delta = avg_recent_str - avg_prev_str

    if str_delta >= 5:
        str_trend = "RISING"
    elif str_delta <= -5:
        str_trend = "FALLING"
    else:
        str_trend = "STABLE"

    # (3) 종합 수급 상태
    price_slope = 0.0
    if len(bars) >= 4:
        p0 = bars[-window].price if len(bars) >= window else bars[0].price
        p1 = bars[-1].price
        price_slope = (p1 - p0) / p0 * 100 if p0 > 0 else 0

    if vol_accel >= 1.2 and latest_str >= 100 and price_slope > 0:
        flow_state = "INFLOW"
    elif vol_accel >= 1.2 and latest_str < 100 and price_slope < 0:
        flow_state = "OUTFLOW"
    elif vol_accel < 0.8 and abs(price_slope) < 0.3:
        flow_state = "DRYING"
    elif vol_accel < 1.1 and latest_str >= 100 and price_slope >= 0:
        flow_state = "ACCUMULATION"
    else:
        flow_state = "NEUTRAL"

    # 점수
    score_map = {"INFLOW": 85, "ACCUMULATION": 70, "NEUTRAL": 50, "DRYING": 30, "OUTFLOW": 15}
    score = score_map.get(flow_state, 50)
    # 체결강도 보정
    if latest_str >= 120:
        score = min(100, score + 10)
    elif latest_str < 80:
        score = max(0, score - 10)

    return {
        "volume_acceleration": round(vol_accel, 2),
        "volume_ratio": round(vol_ratio, 2),
        "volume_trend": vol_trend,
        "strength": round(latest_str, 1),
        "strength_trend": str_trend,
        "flow_state": flow_state,
        "price_slope": round(price_slope, 2),
        "score": round(score, 1),
    }


# ════════════════════════════════════════════
# EYE-05: 모멘텀 (RSI + MACD)
# ════════════════════════════════════════════

def calc_momentum(buf: IntradayBuffer) -> dict:
    """5분봉 RSI(14) + MACD(5,13,4) + 가격 가속도"""
    prices = [float(p) for p in buf.prices()]
    if len(prices) < 6:
        return {"momentum_state": "UNKNOWN", "score": 50}

    # RSI(14) on 5분봉
    rsi_val = _rsi(prices, period=14)

    # MACD(5,13,4) on 5분봉
    macd_line, signal_line, histogram = _macd(prices, fast=5, slow=13, signal=4)

    # 히스토그램 트렌드
    # MACD 히스토그램을 다시 계산해서 최근 3개 비교
    ema_fast = _ema_list(prices, 5)
    ema_slow = _ema_list(prices, 13)
    macd_list = [f - s for f, s in zip(ema_fast, ema_slow)]
    sig_list = _ema_list(macd_list, 4)
    hist_list = [m - s for m, s in zip(macd_list, sig_list)]

    if len(hist_list) >= 3:
        hist_recent = hist_list[-1]
        hist_prev = hist_list[-3]
        if histogram > 0 and hist_recent > hist_prev:
            macd_trend = "EXPANDING"
        elif histogram > 0 and hist_recent <= hist_prev:
            macd_trend = "CONTRACTING"
        elif histogram <= 0 and hist_recent < hist_prev:
            macd_trend = "EXPANDING_BEAR"
        else:
            macd_trend = "CONTRACTING_BEAR"

        # 데드크로스/골든크로스 감지
        if len(hist_list) >= 2:
            if hist_list[-2] > 0 and hist_list[-1] <= 0:
                macd_trend = "DEATH_CROSS"
            elif hist_list[-2] <= 0 and hist_list[-1] > 0:
                macd_trend = "GOLDEN_CROSS"
    else:
        macd_trend = "UNKNOWN"

    # 가격 가속도: 최근 3봉 기울기 vs 이전 3봉 기울기
    if len(prices) >= 6:
        recent_chg = (prices[-1] - prices[-3]) / prices[-3] * 100 if prices[-3] > 0 else 0
        prev_chg = (prices[-3] - prices[-6]) / prices[-6] * 100 if prices[-6] > 0 else 0
        price_accel = recent_chg - prev_chg
    else:
        price_accel = 0

    # 모멘텀 상태
    if rsi_val >= 55 and histogram > 0 and macd_trend in ("EXPANDING", "GOLDEN_CROSS") and price_accel > 0:
        momentum_state = "ACCELERATING"
    elif rsi_val >= 45 and histogram > 0:
        momentum_state = "STEADY"
    elif rsi_val >= 40 and histogram > 0 and macd_trend == "CONTRACTING":
        momentum_state = "DECELERATING"
    elif macd_trend in ("DEATH_CROSS",) or rsi_val < 40:
        momentum_state = "REVERSING"
    else:
        momentum_state = "STEADY"

    # 점수
    score_map = {"ACCELERATING": 85, "STEADY": 65, "DECELERATING": 40, "REVERSING": 15, "UNKNOWN": 50}
    score = score_map.get(momentum_state, 50)
    # RSI 보정
    if rsi_val >= 70:
        score = min(100, score + 5)  # 과매수지만 강세
    elif rsi_val <= 30:
        score = max(0, score - 10)  # 과매도

    return {
        "rsi_5min": round(rsi_val, 1),
        "macd_histogram": round(histogram, 1),
        "macd_trend": macd_trend,
        "price_acceleration": round(price_accel, 2),
        "momentum_state": momentum_state,
        "score": round(score, 1),
    }


# ════════════════════════════════════════════
# EYE-06: 종합 판정 (synthesize)
# ════════════════════════════════════════════

def synthesize(buf: IntradayBuffer, ma: dict, vwap_pos: dict,
               sr: dict, flow: dict, momentum: dict,
               prev_verdict: str, entry_price: int = 0) -> Tuple[str, float, str, str, dict]:
    """5-Layer 종합 → (verdict, confidence, summary, action, action_params)"""

    if buf.count < 5:
        return "WARMUP", 0.3, f"워밍업 중 ({buf.count}/5봉)", "HOLD", {}

    price = buf.latest.price

    # ── 1차: 룰 기반 (명확한 케이스) ──

    # BREAKING: 저항선 돌파 + 거래량 폭발 + INFLOW
    resistances = sr.get("resistances", [])
    if resistances and flow.get("volume_acceleration", 0) >= 1.5 and flow.get("flow_state") == "INFLOW":
        nearest_res = resistances[0].get("price", 0)
        if nearest_res > 0 and price > nearest_res:
            return (
                "BREAKING", 0.85,
                f"{nearest_res:,} 돌파 + 거래량 {flow['volume_acceleration']:.1f}x",
                "HOLD",
                {"trailing_sl": nearest_res, "tp_adjust": "UP"},
            )

    # DYING: 역배열 + VWAP 하회 + OUTFLOW + REVERSING
    dying_signals = 0
    dying_reasons = []
    if ma.get("alignment") == "BEARISH":
        dying_signals += 1
        dying_reasons.append("역배열")
    if vwap_pos.get("position") in ("BELOW", "STRONG_BELOW"):
        dying_signals += 1
        dying_reasons.append("VWAP 하회")
    if flow.get("flow_state") == "OUTFLOW":
        dying_signals += 1
        dying_reasons.append("수급 이탈")
    if momentum.get("momentum_state") == "REVERSING":
        dying_signals += 1
        dying_reasons.append("모멘텀 소멸")

    if dying_signals >= 3:
        confidence = 0.85 if dying_signals >= 4 else 0.70
        return (
            "DYING", confidence,
            " + ".join(dying_reasons),
            "EXIT",
            {},
        )

    # BOUNCING: 지지선 터치(2회+) + 거래량 증가 + 체결강도>100
    supports = sr.get("supports", [])
    strong_supports = [s for s in supports if s.get("touches", 0) >= 2]
    if (strong_supports and flow.get("volume_trend") in ("INCREASING", "SURGING")
            and flow.get("strength", 0) >= 100 and prev_verdict in ("DYING", "WEAKENING", "BOUNCING")):
        nearest_sup = strong_supports[0]["price"]
        return (
            "BOUNCING", 0.70,
            f"{nearest_sup:,} 지지 {strong_supports[0]['touches']}회 + 체결강도 {flow['strength']:.0f}",
            "HOLD",
            {"trailing_sl": int(nearest_sup * 0.995)},
        )

    # ── 2차: 복합 점수 기반 ──

    # 가중 점수: L1(EMA) 20% + L2(VWAP) 20% + L3(S/R) 10% + L4(수급) 25% + L5(모멘텀) 25%
    composite = (
        ma.get("score", 50) * 0.20
        + vwap_pos.get("score", 50) * 0.20
        + sr.get("score", 50) * 0.10
        + flow.get("score", 50) * 0.25
        + momentum.get("score", 50) * 0.25
    )

    # ALIVE: 정배열 + VWAP 위 + INFLOW/ACCUMULATION + STEADY 이상
    alive_signals = 0
    if ma.get("alignment") == "BULLISH":
        alive_signals += 1
    if vwap_pos.get("position") in ("ABOVE", "STRONG_ABOVE"):
        alive_signals += 1
    if flow.get("flow_state") in ("INFLOW", "ACCUMULATION"):
        alive_signals += 1
    if momentum.get("momentum_state") in ("ACCELERATING", "STEADY"):
        alive_signals += 1

    if composite >= 65 or alive_signals >= 3:
        reasons = []
        if ma.get("alignment") == "BULLISH":
            reasons.append("정배열")
        if vwap_pos.get("position") in ("ABOVE", "STRONG_ABOVE"):
            reasons.append("VWAP 위")
        if flow.get("flow_state") in ("INFLOW", "ACCUMULATION"):
            reasons.append(flow["flow_state"])
        if momentum.get("momentum_state") in ("ACCELERATING", "STEADY"):
            reasons.append(f"모멘텀 {momentum['momentum_state']}")
        confidence = 0.85 if alive_signals >= 4 else (0.75 if alive_signals >= 3 else 0.65)
        summary = " + ".join(reasons) if reasons else f"종합 {composite:.0f}점"

        # 트레일링 SL: 고점 -1.5%
        today_high = sr.get("today_high", price)
        trailing_sl = int(today_high * 0.985)

        return "ALIVE", confidence, summary, "HOLD", {"trailing_sl": trailing_sl}

    # WEAKENING: 이평 수렴/혼합 + 거래량 감소/DRYING + 모멘텀 감속
    weak_signals = 0
    weak_reasons = []
    if ma.get("alignment") in ("CONVERGING", "MIXED"):
        weak_signals += 1
        weak_reasons.append("이평 수렴")
    if flow.get("flow_state") == "DRYING":
        weak_signals += 1
        weak_reasons.append("거래량 감소")
    if momentum.get("momentum_state") == "DECELERATING":
        weak_signals += 1
        weak_reasons.append("모멘텀 감속")
    if flow.get("strength", 100) < 90:
        weak_signals += 1
        weak_reasons.append(f"체결강도 {flow.get('strength', 0):.0f}")

    if composite >= 35 or weak_signals >= 2:
        summary = ", ".join(weak_reasons) if weak_reasons else f"약화 (점수 {composite:.0f})"
        today_high = sr.get("today_high", price)
        trailing_sl = int(today_high * 0.975)
        return "WEAKENING", 0.65, summary, "WATCH", {"trailing_sl": trailing_sl}

    # 최하위 → DYING
    reasons = []
    if flow.get("flow_state") == "OUTFLOW":
        reasons.append("수급 이탈")
    if momentum.get("momentum_state") == "REVERSING":
        reasons.append("모멘텀 소멸")
    if vwap_pos.get("position") in ("BELOW", "STRONG_BELOW"):
        reasons.append("VWAP 하회")
    summary = " + ".join(reasons) if reasons else f"위험 (점수 {composite:.0f})"
    return "DYING", 0.75, summary, "EXIT", {}


# ════════════════════════════════════════════
# IntradayEye — 메인 엔진
# ════════════════════════════════════════════

class IntradayEye:
    """장중 AI Eye v2 — 5-Layer 분석 → 종합 판정"""

    def __init__(self, trader=None):
        self.trader = trader
        self._buffers: Dict[str, IntradayBuffer] = {}
        self._prev_verdicts: Dict[str, str] = {}
        self._names: Dict[str, str] = {}
        self._history: Dict[str, list] = {}  # code → [(timestamp, verdict)]

    def _get_buffer(self, code: str) -> IntradayBuffer:
        if code not in self._buffers:
            name = self._names.get(code, code)
            self._buffers[code] = IntradayBuffer(code, name)
        return self._buffers[code]

    def set_name(self, code: str, name: str):
        self._names[code] = name
        if code in self._buffers:
            self._buffers[code].name = name

    def evaluate(self, code: str, entry_price: int = 0,
                 price_data: dict = None) -> Optional[EyeVerdict]:
        """1종목 5-Layer 분석

        price_data가 없으면 self.trader.fetch_price() 호출
        """
        # 데이터 수집
        if price_data is None:
            if self.trader is None:
                return None
            resp = self.trader.fetch_price(code)
            if not resp.get("success"):
                return None
            price_data = resp

        # 버퍼에 추가
        buf = self._get_buffer(code)
        buf.add_bar(price_data)

        # 5-Layer 분석
        ma = calc_intraday_ma(buf)
        vwap_pos = calc_vwap_position(buf)
        sr = calc_support_resistance(buf)
        flow = calc_volume_flow(buf)
        mom = calc_momentum(buf)

        prev_verdict = self._prev_verdicts.get(code, "WARMUP")

        # 종합 판정
        verdict, confidence, summary, action, action_params = synthesize(
            buf, ma, vwap_pos, sr, flow, mom, prev_verdict, entry_price
        )

        # 이전 verdict 업데이트
        self._prev_verdicts[code] = verdict

        # 히스토리 기록
        if code not in self._history:
            self._history[code] = []
        self._history[code].append((datetime.now().strftime("%H:%M"), verdict))
        # 최근 78개만 유지
        self._history[code] = self._history[code][-78:]

        # 점수 (auto_trader 호환: l1, l2, l3 = EMA, VWAP+Flow, Momentum)
        l1 = ma.get("score", 50)
        l2 = (vwap_pos.get("score", 50) + flow.get("score", 50)) / 2
        l3 = mom.get("score", 50)
        composite = l1 * 0.30 + l2 * 0.35 + l3 * 0.35

        name = self._names.get(code, code)
        details = {
            "ema": ma,
            "vwap": vwap_pos,
            "support_resistance": sr,
            "flow": flow,
            "momentum": mom,
        }

        return EyeVerdict(
            code=code, name=name, timestamp=datetime.now(),
            l1_price_structure=round(l1, 1),
            l2_volume_supply=round(l2, 1),
            l3_momentum=round(l3, 1),
            composite_score=round(composite, 1),
            verdict=verdict, prev_verdict=prev_verdict,
            confidence=round(confidence, 2),
            summary=summary,
            details=details,
            action=action,
            action_params=action_params,
        )

    def clear(self, code: str = None):
        if code:
            self._buffers.pop(code, None)
            self._prev_verdicts.pop(code, None)
            self._history.pop(code, None)
        else:
            self._buffers.clear()
            self._prev_verdicts.clear()
            self._history.clear()

    def get_status(self) -> Dict[str, dict]:
        result = {}
        for code, buf in self._buffers.items():
            verdict = self._prev_verdicts.get(code, "WARMUP")
            result[code] = {
                "name": buf.name,
                "bars": buf.count,
                "verdict": verdict,
                "latest_price": buf.latest.price if buf.latest else 0,
            }
        return result

    def get_history(self, code: str) -> list:
        """종목 판정 히스토리 → [(시각, verdict)]"""
        return self._history.get(code, [])

    def format_history_timeline(self, code: str) -> str:
        """종목 판정 타임라인 → "ALIVE(09:05~10:20) → WEAKENING(10:25~)" """
        history = self.get_history(code)
        if not history:
            return ""

        segments = []
        curr_verdict = history[0][1]
        start_time = history[0][0]

        for time_str, v in history[1:]:
            if v != curr_verdict:
                segments.append(f"{curr_verdict}({start_time}~{time_str})")
                curr_verdict = v
                start_time = time_str

        segments.append(f"{curr_verdict}({start_time}~)")
        return " → ".join(segments)


# ════════════════════════════════════════════
# --test 모드
# ════════════════════════════════════════════

def _test():
    """테스트: 1분봉 CSV → 5분봉 리샘플 → 5-Layer 분석"""
    import json

    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    sys.path.insert(0, project_root)
    os.chdir(project_root)

    # 테스트 종목
    test_codes = [("005930", "삼성전자", 0), ("000270", "기아", 0)]

    # positions.json에서 로드 시도
    pos_file = os.path.join(project_root, "data_store", "positions.json")
    if os.path.exists(pos_file):
        try:
            with open(pos_file, "r", encoding="utf-8") as f:
                positions = json.load(f)
            test_codes = []
            for code, info in positions.items():
                test_codes.append((code, info.get("name", code), info.get("entry_price", 0)))
        except Exception:
            pass

    yesterday = "2026-03-19"  # 가장 최근 데이터

    print("=" * 60)
    print("  Intraday AI Eye v2 — 1분봉 백테스트")
    print(f"  날짜: {yesterday}")
    print("=" * 60)

    for code, name, entry in test_codes:
        buf = IntradayBuffer(code, name)
        n = buf.load_from_1min_csv(yesterday)

        print(f"\n{'─' * 55}")
        print(f"  {name} ({code}) | {n}봉 로드" + (f" | 진입가: {entry:,}" if entry else ""))
        print(f"{'─' * 55}")

        if n < 5:
            print("  [스킵] 데이터 부족")
            continue

        # 5-Layer 분석
        ma = calc_intraday_ma(buf)
        vwap_pos = calc_vwap_position(buf)
        sr = calc_support_resistance(buf)
        flow = calc_volume_flow(buf)
        mom = calc_momentum(buf)

        verdict, confidence, summary, action, action_params = synthesize(
            buf, ma, vwap_pos, sr, flow, mom, "WARMUP", entry
        )

        print(f"  판정: {verdict} ({confidence:.0%}) → {action}")
        print(f"  사유: {summary}")
        print(f"  [L1 EMA] {ma['alignment']} | spread {ma['ema_spread']:+.2f}% | score {ma['score']:.0f}")
        print(f"  [L2 VWAP] {vwap_pos['position']} | {vwap_pos['distance_pct']:+.2f}% | "
              f"σ={vwap_pos.get('sigma', 0):,} | score {vwap_pos['score']}")
        print(f"  [L3 S/R] 지지 {len(sr['supports'])}개 | 저항 {len(sr['resistances'])}개 | score {sr['score']:.0f}")
        print(f"  [L4 Flow] {flow.get('flow_state', '?')} | 가속 {flow.get('volume_acceleration', 0):.2f}x | "
              f"강도 {flow.get('strength', 0):.0f} | score {flow['score']:.0f}")
        print(f"  [L5 Mom] {mom.get('momentum_state', '?')} | RSI {mom.get('rsi_5min', 0):.1f} | "
              f"MACD {mom.get('macd_trend', '?')} | score {mom['score']:.0f}")

        if action_params:
            print(f"  파라미터: {action_params}")

    print(f"\n{'=' * 60}")


def _test_live():
    """테스트: 실시간 fetch_price → 1회 평가"""
    import json

    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    sys.path.insert(0, project_root)
    os.chdir(project_root)

    from dotenv import load_dotenv
    parent_env = os.path.join(os.path.dirname(project_root), ".env")
    load_dotenv(parent_env)

    from bot.kis_trader import KISTrader
    trader = KISTrader()
    eye = IntradayEye(trader)

    test_codes = [("005930", "삼성전자", 0), ("000270", "기아", 0)]
    pos_file = os.path.join(project_root, "data_store", "positions.json")
    if os.path.exists(pos_file):
        try:
            with open(pos_file, "r", encoding="utf-8") as f:
                positions = json.load(f)
            test_codes = []
            for code, info in positions.items():
                name = info.get("name", code)
                eye.set_name(code, name)
                test_codes.append((code, name, info.get("entry_price", 0)))
        except Exception:
            pass

    print("=" * 60)
    print("  Intraday AI Eye v2 — 실시간 테스트 (1회)")
    print("=" * 60)

    for code, name, entry in test_codes:
        v = eye.evaluate(code, entry_price=entry)
        if v is None:
            print(f"\n  {name}: API 실패")
            continue
        print(f"\n  {name}: {v.verdict} ({v.confidence:.0%}) → {v.action}")
        print(f"    {v.summary}")
        print(f"    L1={v.l1_price_structure:.0f} L2={v.l2_volume_supply:.0f} L3={v.l3_momentum:.0f} "
              f"= {v.composite_score:.0f}")

    print(f"\n{'=' * 60}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    if "--test" in sys.argv:
        _test()
    elif "--live" in sys.argv:
        _test_live()
    else:
        print("사용법:")
        print("  python data/intraday_eye.py --test   # 1분봉 백테스트")
        print("  python data/intraday_eye.py --live   # 실시간 1회 평가")
