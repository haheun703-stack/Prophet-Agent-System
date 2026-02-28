# -*- coding: utf-8 -*-
"""
스윙매매 기술적 분석 엔진
=========================
OBV 거부권 → EMA 추세 → RSI 존 → MACD 히스토그램 (레이너 세팅)
4개 지표를 순서대로 적용하여 종합 시그널 도출.

사용법:
  from data.swing_indicators import analyze_stock
  result = analyze_stock(df)  # df: 일봉 DataFrame (close, volume 필수)
"""

import numpy as np
import pandas as pd
from typing import Optional


# ═══════════════════════════════════════════════════
#  1. EMA (60일 지수이동평균)
# ═══════════════════════════════════════════════════

def calc_ema(series: pd.Series, period: int = 60) -> pd.Series:
    return series.ewm(span=period, adjust=False).mean()


def check_ema_trend(close: pd.Series, period: int = 60) -> str:
    """60 EMA 기준 추세 판단 → BULLISH | BEARISH | SIDEWAYS"""
    if len(close) < period:
        return "UNKNOWN"
    ema = calc_ema(close, period)
    last_close = close.iloc[-1]
    last_ema = ema.iloc[-1]
    above_count = (close.iloc[-5:] > ema.iloc[-5:]).sum()
    if last_close > last_ema and above_count >= 4:
        return "BULLISH"
    elif last_close < last_ema and above_count <= 1:
        return "BEARISH"
    return "SIDEWAYS"


# ═══════════════════════════════════════════════════
#  2. RSI (14일, 추세 내 레벨 시프트)
# ═══════════════════════════════════════════════════

def calc_rsi(close: pd.Series, period: int = 14) -> pd.Series:
    delta = close.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = (-delta).where(delta < 0, 0.0)
    avg_gain = gain.ewm(alpha=1/period, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1/period, min_periods=period, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))
    return rsi.fillna(50)


def check_rsi_zone(rsi_value: float, trend: str) -> dict:
    """추세 내 RSI 레벨 시프트 → zone, size_pct, msg"""
    if trend == "BULLISH":
        if 40 <= rsi_value <= 65:
            return {"zone": "OPTIMAL", "size_pct": 1.0, "msg": f"RSI {rsi_value:.0f} — 추세 내 적정"}
        elif 65 < rsi_value <= 75:
            return {"zone": "CAUTION", "size_pct": 0.5, "msg": f"RSI {rsi_value:.0f} — 과열 주의, 절반만"}
        elif rsi_value > 75:
            return {"zone": "OVERHEAT", "size_pct": 0.0, "msg": f"RSI {rsi_value:.0f} — 과열! 진입 금지"}
        else:
            return {"zone": "DEEP_DIP", "size_pct": 0.8, "msg": f"RSI {rsi_value:.0f} — 깊은 눌림"}
    elif trend == "BEARISH":
        if 35 <= rsi_value <= 60:
            return {"zone": "OPTIMAL", "size_pct": 1.0, "msg": f"RSI {rsi_value:.0f} — 매도 적정"}
        elif rsi_value < 30:
            return {"zone": "OVERSOLD", "size_pct": 0.0, "msg": f"RSI {rsi_value:.0f} — 과매도"}
        else:
            return {"zone": "CAUTION", "size_pct": 0.5, "msg": f"RSI {rsi_value:.0f} — 주의"}
    return {"zone": "SIDEWAYS", "size_pct": 0.0, "msg": f"RSI {rsi_value:.0f} — 횡보, 대기"}


# ═══════════════════════════════════════════════════
#  3. OBV (On Balance Volume) + 거부권
# ═══════════════════════════════════════════════════

def calc_obv(close: pd.Series, volume: pd.Series) -> pd.Series:
    direction = np.sign(close.diff())
    direction.iloc[0] = 0
    return (direction * volume).cumsum()


def check_obv_trend(obv: pd.Series, lookback: int = 10) -> dict:
    """OBV 추세 + 거부권 판정 → trend, veto, msg"""
    if len(obv) < lookback:
        return {"trend": "UNKNOWN", "veto": False, "msg": "데이터 부족"}
    recent = obv.iloc[-lookback:]
    x = np.arange(len(recent))
    slope = np.polyfit(x, recent.values, 1)[0]
    obv_range = recent.max() - recent.min()
    norm_slope = (slope / obv_range * lookback) if obv_range > 0 else 0
    if norm_slope > 0.15:
        return {"trend": "UP", "veto": False, "msg": "OBV 상승 — 돈 유입 확인"}
    elif norm_slope < -0.15:
        return {"trend": "DOWN", "veto": True, "msg": "OBV 하락 — 돈 빠지는 중 → 진입 금지"}
    return {"trend": "FLAT", "veto": False, "msg": "OBV 횡보 — 관망"}


def check_obv_divergence(close: pd.Series, obv: pd.Series, lookback: int = 20) -> Optional[str]:
    """OBV 다이버전스 감지 → BEARISH_DIV | BULLISH_DIV | None"""
    if len(close) < lookback:
        return None
    recent_close = close.iloc[-lookback:]
    recent_obv = obv.iloc[-lookback:]
    mid = lookback // 2
    if (recent_close.iloc[mid:].max() > recent_close.iloc[:mid].max()
            and recent_obv.iloc[mid:].max() < recent_obv.iloc[:mid].max()):
        return "BEARISH_DIV"
    if (recent_close.iloc[mid:].min() < recent_close.iloc[:mid].min()
            and recent_obv.iloc[mid:].min() > recent_obv.iloc[:mid].min()):
        return "BULLISH_DIV"
    return None


# ═══════════════════════════════════════════════════
#  4. MACD 히스토그램 (레이너 세팅: Fast=1, Slow=60, Signal=9)
# ═══════════════════════════════════════════════════

def calc_histogram(close: pd.Series, fast: int = 1, slow: int = 60, signal: int = 9) -> pd.DataFrame:
    """레이너 MACD 히스토그램 → DataFrame (histogram, hist_color, hist_growing)"""
    ema_fast = calc_ema(close, fast)
    ema_slow = calc_ema(close, slow)
    macd_line = ema_fast - ema_slow
    signal_line = calc_ema(macd_line, signal)
    histogram = macd_line - signal_line

    df = pd.DataFrame({
        "macd": macd_line,
        "signal_line": signal_line,
        "histogram": histogram,
    })
    df["hist_color"] = np.where(df["histogram"] >= 0, "GREEN", "RED")
    abs_hist = df["histogram"].abs()
    df["hist_growing"] = abs_hist > abs_hist.shift(1)
    return df


def check_histogram_trigger(hist_df: pd.DataFrame, lookback: int = 5) -> dict:
    """레이너 히스토그램 트리거 → triggered, direction, strength"""
    if len(hist_df) < lookback + 2:
        return {"triggered": False, "direction": None, "strength": None, "msg": "데이터 부족"}

    recent = hist_df.iloc[-(lookback + 1):]
    current = recent.iloc[-1]

    # 색상 전환 감지
    colors = recent["hist_color"].values
    color_change_idx = None
    for i in range(len(colors) - 1, 0, -1):
        if colors[i] != colors[i-1]:
            color_change_idx = i
            break

    if color_change_idx is None:
        return {"triggered": False, "direction": None, "strength": None,
                "msg": f"색상 전환 없음 — {current['hist_color']} 유지"}

    # 전환 후 큰 막대 확인
    post_change = recent.iloc[color_change_idx:]
    has_growing = post_change["hist_growing"].any()

    # 강도
    current_abs = abs(current["histogram"])
    opposite_color = "RED" if current["hist_color"] == "GREEN" else "GREEN"
    opposite_bars = recent[recent["hist_color"] == opposite_color]["histogram"].abs()
    max_opposite = opposite_bars.max() if len(opposite_bars) > 0 else 0

    strength = "STRONG" if current_abs > max_opposite else ("NORMAL" if has_growing else "WEAK")
    direction = "BUY" if current["hist_color"] == "GREEN" else "SELL"
    triggered = has_growing and strength in ("STRONG", "NORMAL")

    return {
        "triggered": triggered,
        "direction": direction,
        "strength": strength,
        "histogram_value": float(current["histogram"]),
        "msg": f"{'BUY' if direction == 'BUY' else 'SELL'} "
               f"{'발동' if triggered else '대기'} | 강도: {strength}",
    }


# ═══════════════════════════════════════════════════
#  종합 시그널 (4지표 통합)
# ═══════════════════════════════════════════════════

def calc_composite_signal(
    obv_result: dict,
    ema_trend: str,
    rsi_result: dict,
    hist_result: dict,
    obv_divergence: Optional[str] = None,
) -> dict:
    """4개 지표 종합 → signal, score(0~100), position_size, reasons"""
    reasons = []
    score = 0

    # STEP 1: OBV 거부권
    if obv_result["veto"]:
        return {
            "signal": "NO_ENTRY", "score": 0, "position_size": 0.0,
            "reasons": [obv_result["msg"], "OBV 거부 — 진입 불가"],
        }

    score += 30 if obv_result["trend"] == "UP" else 15
    reasons.append(obv_result["msg"])

    if obv_divergence == "BEARISH_DIV":
        score -= 15
        reasons.append("OBV 약세 다이버전스 — 이탈 경고")

    # STEP 2: EMA + RSI
    if ema_trend == "BULLISH" and rsi_result["zone"] in ("OPTIMAL", "DEEP_DIP"):
        score += 30
    elif ema_trend == "BULLISH" and rsi_result["zone"] == "CAUTION":
        score += 15
    elif ema_trend == "BULLISH":
        score += 5
    elif ema_trend == "SIDEWAYS":
        score += 5
    reasons.append(f"추세: {ema_trend} | {rsi_result['msg']}")

    # STEP 3: 히스토그램 트리거
    if hist_result["triggered"]:
        score += 40 if hist_result["strength"] == "STRONG" else 25
    else:
        score += 5
    reasons.append(f"히스토그램: {hist_result['msg']}")

    # 종합
    position_size = rsi_result["size_pct"]
    if score >= 85:
        signal = "STRONG_BUY"
    elif score >= 65:
        signal = "BUY"
    elif score >= 45:
        signal = "WATCH"
    else:
        signal = "NO_ENTRY"
        position_size = 0.0

    return {
        "signal": signal,
        "score": min(100, score),
        "position_size": position_size,
        "reasons": reasons,
    }


# ═══════════════════════════════════════════════════
#  원스톱 분석 함수
# ═══════════════════════════════════════════════════

def analyze_stock(df: pd.DataFrame) -> dict:
    """일봉 DataFrame → 종합 기술적 분석 결과

    Args:
        df: 최소 60봉 이상의 일봉 데이터 (close, volume 필수)
            pykrx 형식(종가/거래량) 또는 영문(close/volume) 모두 지원

    Returns:
        signal, score, position_size, reasons, ema_trend, rsi, obv, histogram
    """
    # 컬럼 표준화
    col_map = {"종가": "close", "거래량": "volume", "시가": "open", "고가": "high", "저가": "low"}
    df = df.rename(columns=col_map)

    if "close" not in df.columns or "volume" not in df.columns:
        return {"signal": "NO_DATA", "score": 0, "position_size": 0.0, "reasons": ["데이터 없음"]}

    close = df["close"].astype(float)
    volume = df["volume"].astype(float)

    if len(close) < 65:
        return {"signal": "NO_DATA", "score": 0, "position_size": 0.0, "reasons": ["데이터 부족 (65봉 미만)"]}

    # 1. EMA 추세
    ema_trend = check_ema_trend(close, 60)

    # 2. RSI
    rsi_series = calc_rsi(close, 14)
    rsi_value = float(rsi_series.iloc[-1])
    rsi_result = check_rsi_zone(rsi_value, ema_trend)

    # 3. OBV
    obv_series = calc_obv(close, volume)
    obv_result = check_obv_trend(obv_series, 10)
    obv_div = check_obv_divergence(close, obv_series, 20)

    # 4. MACD 히스토그램 (레이너)
    hist_df = calc_histogram(close, fast=1, slow=60, signal=9)
    hist_result = check_histogram_trigger(hist_df, 5)

    # 종합
    composite = calc_composite_signal(obv_result, ema_trend, rsi_result, hist_result, obv_div)

    # ATR (스윙 SL/TP용)
    if "high" in df.columns and "low" in df.columns:
        high = df["high"].astype(float)
        low = df["low"].astype(float)
        tr = pd.concat([
            high - low,
            (high - close.shift(1)).abs(),
            (low - close.shift(1)).abs(),
        ], axis=1).max(axis=1)
        atr_14 = float(tr.rolling(14).mean().iloc[-1])
    else:
        atr_14 = 0

    composite["ema_trend"] = ema_trend
    composite["rsi"] = rsi_value
    composite["obv_trend"] = obv_result["trend"]
    composite["obv_divergence"] = obv_div
    composite["histogram_direction"] = hist_result.get("direction")
    composite["histogram_strength"] = hist_result.get("strength")
    composite["atr_14"] = round(atr_14, 0)
    composite["close"] = float(close.iloc[-1])

    # 스윙 SL/TP 계산 (ATR 기반)
    if atr_14 > 0:
        composite["swing_sl"] = round(close.iloc[-1] - atr_14 * 1.5, 0)  # 1.5 ATR
        composite["swing_tp"] = round(close.iloc[-1] + atr_14 * 3.0, 0)  # 3 ATR (2R)
    else:
        composite["swing_sl"] = 0
        composite["swing_tp"] = 0

    return composite


# ═══════════════════════════════════════════════════
#  5. 스토캐스틱 (5,3,3)
# ═══════════════════════════════════════════════════

def calc_stochastic(high: pd.Series, low: pd.Series, close: pd.Series,
                    k_period: int = 5, d_period: int = 3, smooth: int = 3) -> pd.DataFrame:
    """스토캐스틱 %K, %D 계산"""
    lowest = low.rolling(k_period).min()
    highest = high.rolling(k_period).max()
    raw_k = 100 * (close - lowest) / (highest - lowest).replace(0, np.nan)
    k = raw_k.rolling(smooth).mean()
    d = k.rolling(d_period).mean()
    return pd.DataFrame({"k": k, "d": d})


# ═══════════════════════════════════════════════════
#  6. 볼린저밴드 (20, 2)
# ═══════════════════════════════════════════════════

def calc_bollinger(close: pd.Series, period: int = 20, std_mult: float = 2.0) -> pd.DataFrame:
    """볼린저밴드 상/중/하단"""
    mid = close.rolling(period).mean()
    std = close.rolling(period).std()
    return pd.DataFrame({
        "upper": mid + std * std_mult,
        "mid": mid,
        "lower": mid - std * std_mult,
        "pct_b": (close - (mid - std * std_mult)) / ((mid + std * std_mult) - (mid - std * std_mult)).replace(0, np.nan),
    })


# ═══════════════════════════════════════════════════
#  7. 장중 진입 필터 (매수 직전 최종 확인)
# ═══════════════════════════════════════════════════

def check_entry_filter(code: str, name: str = "") -> dict:
    """매수 직전 차트 기반 최종 확인

    Returns:
        {
            "pass": True/False,
            "reason": "통과 사유 / 거부 사유",
            "rsi": float,
            "stoch_k": float, "stoch_d": float,
            "bb_pct_b": float,
            "size_mult": float (1.0=풀, 0.5=절반, 0.0=패스)
        }
    """
    from pykrx import stock as pykrx_stock
    from datetime import datetime, timedelta

    result = {
        "pass": True, "reason": "통과", "rsi": 0, "stoch_k": 0, "stoch_d": 0,
        "bb_pct_b": 0, "size_mult": 1.0, "checks": [],
    }

    try:
        # 최근 60일 일봉 데이터 가져오기
        end = datetime.now().strftime("%Y%m%d")
        start = (datetime.now() - timedelta(days=120)).strftime("%Y%m%d")
        df = pykrx_stock.get_market_ohlcv(start, end, code)

        if df is None or len(df) < 30:
            result["reason"] = "데이터 부족 — 필터 통과 (데이터 없음)"
            result["checks"].append("DATA_INSUFFICIENT")
            return result

        close = df["종가"].astype(float)
        high = df["고가"].astype(float)
        low = df["저가"].astype(float)

        # ── RSI 체크 ──
        rsi_series = calc_rsi(close, 14)
        rsi = float(rsi_series.iloc[-1])
        result["rsi"] = round(rsi, 1)

        # ── 스토캐스틱 체크 ──
        stoch = calc_stochastic(high, low, close)
        k_val = float(stoch["k"].iloc[-1]) if not pd.isna(stoch["k"].iloc[-1]) else 50
        d_val = float(stoch["d"].iloc[-1]) if not pd.isna(stoch["d"].iloc[-1]) else 50
        k_prev = float(stoch["k"].iloc[-2]) if len(stoch) > 1 and not pd.isna(stoch["k"].iloc[-2]) else k_val
        d_prev = float(stoch["d"].iloc[-2]) if len(stoch) > 1 and not pd.isna(stoch["d"].iloc[-2]) else d_val
        result["stoch_k"] = round(k_val, 1)
        result["stoch_d"] = round(d_val, 1)

        # 스토캐스틱 데드크로스: 이전에는 K>D였는데 지금 K<D
        stoch_death_cross = (k_prev > d_prev) and (k_val < d_val)

        # ── 볼린저밴드 체크 ──
        bb = calc_bollinger(close)
        pct_b = float(bb["pct_b"].iloc[-1]) if not pd.isna(bb["pct_b"].iloc[-1]) else 0.5
        result["bb_pct_b"] = round(pct_b, 3)

        # ═══════════════════════════════════════════
        #  필터 규칙 (3단계: 거부 → 절반 → 통과)
        # ═══════════════════════════════════════════

        reject_reasons = []
        caution_reasons = []

        # 규칙1: RSI 80+ → 극단적 과매수 → 거부
        if rsi > 80:
            reject_reasons.append(f"RSI {rsi:.0f} 극과매수")

        # 규칙2: RSI 75+ AND 스토캐스틱 데드크로스 → 과매수 + 하락 전환 → 거부
        if rsi > 75 and stoch_death_cross:
            reject_reasons.append(f"RSI {rsi:.0f} + 스토캐스틱 데드크로스")

        # 규칙3: 볼린저 %B > 1.05 → 밴드 이탈 → 거부
        if pct_b > 1.05:
            reject_reasons.append(f"볼린저밴드 이탈 (%B={pct_b:.2f})")

        # 규칙4: 스토캐스틱 K,D 모두 95+ → 극단적 과매수 → 거부
        if k_val > 95 and d_val > 95:
            reject_reasons.append(f"스토캐스틱 극과매수 (K={k_val:.0f}, D={d_val:.0f})")

        # 규칙5: RSI 70~80 → 과열 주의 → 절반 매수
        if 70 < rsi <= 80 and not reject_reasons:
            caution_reasons.append(f"RSI {rsi:.0f} 과열주의")

        # 규칙6: 볼린저 %B > 0.95 → 상단 근접 → 절반 매수
        if 0.95 < pct_b <= 1.05 and not reject_reasons:
            caution_reasons.append(f"볼린저 상단 근접 (%B={pct_b:.2f})")

        # 규칙7: 스토캐스틱 데드크로스 (RSI 70 미만이어도) → 절반 매수
        if stoch_death_cross and not reject_reasons:
            caution_reasons.append(f"스토캐스틱 데드크로스 (K={k_val:.0f}<D={d_val:.0f})")

        # ── 최종 판정 ──
        if reject_reasons:
            result["pass"] = False
            result["size_mult"] = 0.0
            result["reason"] = "진입 거부: " + " | ".join(reject_reasons)
            result["checks"] = reject_reasons
        elif caution_reasons:
            result["pass"] = True
            result["size_mult"] = 0.5
            result["reason"] = "절반 진입: " + " | ".join(caution_reasons)
            result["checks"] = caution_reasons
        else:
            result["pass"] = True
            result["size_mult"] = 1.0
            result["reason"] = f"통과 (RSI={rsi:.0f}, K={k_val:.0f}, %B={pct_b:.2f})"
            result["checks"] = ["ALL_CLEAR"]

        label = name or code
        icon = "PASS" if result["pass"] else "REJECT"
        mult = f"{result['size_mult']:.0%}"
        print(f"  진입필터 {label}: [{icon}] {mult} — {result['reason']}")

    except Exception as e:
        result["reason"] = f"필터 오류: {e} — 기본 통과"
        result["checks"] = [f"ERROR: {e}"]

    return result
