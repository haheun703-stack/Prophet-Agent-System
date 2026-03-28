# -*- coding: utf-8 -*-
"""
TRIX Divergence Scanner — 바닥 반전 시그널 감지
=================================================
KISOpenAPI 봇의 TRIX 다이버전스 기법을 Body Hunter 모닝추천에 통합.

TRIX = Triple EMA의 ROC → 3중 평활로 노이즈 극소화
Bullish Divergence: 가격 Lower Low + TRIX Higher Low → 반전 임박
Bearish Divergence: 가격 Higher High + TRIX Lower High → 고점 경고

출력: data_store/trix_signals.json (3시간 캐시)
"""

import json
import logging
import time
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Tuple, Optional

import numpy as np
import pandas as pd

logger = logging.getLogger("BH.TRIX")

DATA_DIR = Path(__file__).parent.parent / "data_store"
CACHE_PATH = DATA_DIR / "trix_signals.json"

# ── 파라미터 ──────────────────────────────
TRIX_PERIOD = 12            # Triple EMA 기간 (한국 시장 적응)
TRIX_SIGNAL_PERIOD = 9      # Signal line EMA
ADX_PERIOD = 14             # ADX/DI 기간
DIV_LOOKBACK = 20           # 다이버전스 탐색 윈도우 (영업일)
MIN_ADX = 20                # 최소 ADX (추세 존재 확인)
MIN_DATA_DAYS = 60          # 최소 데이터 일수
MIN_TRADING_VALUE = 1e9     # 최소 거래대금 (10억)
EXTREMA_ORDER = 2           # argrelextrema 로컬 극값 범위
CACHE_TTL_HOURS = 3         # 캐시 유효 시간


# ═══════════════════════════════════════
#  Core: TRIX 지표 계산
# ═══════════════════════════════════════

def _calc_trix_indicators(code: str) -> Optional[Dict]:
    """종목 코드 → TRIX(12) + Signal(9) + ADX(14) + DI 계산.

    Returns: {close, trix, signal, adx, plus_di, minus_di, trading_value}
             또는 데이터 부족 시 None
    """
    try:
        from data.extend_parquet_data import load_daily
        df = load_daily(code)
        if df is None or len(df) < MIN_DATA_DAYS:
            return None

        close = df["종가"].astype(float)
        high = df["고가"].astype(float)
        low = df["저가"].astype(float)
        volume = df["거래량"].astype(float)

        # 거래대금 필터 (최근 5일 평균)
        if "거래대금" in df.columns:
            tv = df["거래대금"].astype(float).tail(5).mean()
        else:
            tv = (close * volume).tail(5).mean()
        if tv < MIN_TRADING_VALUE:
            return None

        # TRIX 계산
        from data.indicator_calc import IndicatorCalc
        trix = IndicatorCalc.trix(close, TRIX_PERIOD)
        trix_signal = trix.ewm(span=TRIX_SIGNAL_PERIOD, adjust=False).mean()

        # ADX / DI 계산
        adx, plus_di, minus_di = IndicatorCalc.adx_di(high, low, close, ADX_PERIOD)

        return {
            "close": close.values,
            "trix": trix.values,
            "trix_signal": trix_signal.values,
            "adx": adx.values,
            "plus_di": plus_di.values,
            "minus_di": minus_di.values,
            "trading_value": tv,
        }
    except Exception as e:
        logger.debug(f"[TRIX] {code} 지표 계산 실패: {e}")
        return None


# ═══════════════════════════════════════
#  Divergence Detection
# ═══════════════════════════════════════

def detect_bullish_divergence(close: np.ndarray, trix: np.ndarray,
                               lookback: int = DIV_LOOKBACK) -> Tuple[bool, float]:
    """상승 다이버전스: 가격 Lower Low + TRIX Higher Low.

    scipy.signal.argrelextrema로 로컬 저점 탐색.
    Returns: (detected, strength 0~1)
    """
    try:
        from scipy.signal import argrelextrema
    except ImportError:
        return False, 0.0

    n = len(close)
    if n < lookback + 10:
        return False, 0.0

    # NaN 제거: TRIX 앞부분
    valid_start = 0
    for i in range(n):
        if not np.isnan(trix[i]):
            valid_start = i
            break

    # 최근 lookback 범위에서 극값 탐색
    window_start = max(valid_start, n - lookback - 10)
    c_window = close[window_start:]
    t_window = trix[window_start:]

    if len(c_window) < 10:
        return False, 0.0

    # 로컬 저점 탐색
    price_lows = argrelextrema(c_window, np.less, order=EXTREMA_ORDER)[0]
    trix_lows = argrelextrema(t_window, np.less, order=EXTREMA_ORDER)[0]

    if len(price_lows) < 2 or len(trix_lows) < 2:
        return False, 0.0

    # 최근 2개 저점 비교
    p_low1_idx, p_low2_idx = price_lows[-2], price_lows[-1]
    p_low1_val, p_low2_val = c_window[p_low1_idx], c_window[p_low2_idx]

    # 가격: Lower Low (두번째 저점이 첫번째보다 낮음)
    if p_low2_val >= p_low1_val:
        return False, 0.0

    # TRIX에서 대응하는 저점 찾기 (±7일 범위)
    def _find_nearest_trix_low(price_idx):
        best = None
        for tl in trix_lows:
            if abs(tl - price_idx) <= 7:
                if best is None or abs(tl - price_idx) < abs(best - price_idx):
                    best = tl
        return best

    t_match1 = _find_nearest_trix_low(p_low1_idx)
    t_match2 = _find_nearest_trix_low(p_low2_idx)

    if t_match1 is None or t_match2 is None:
        return False, 0.0

    t_low1_val = t_window[t_match1]
    t_low2_val = t_window[t_match2]

    # TRIX: Higher Low (두번째 저점이 첫번째보다 높음)
    if t_low2_val <= t_low1_val:
        return False, 0.0

    # Strength 계산
    price_diff_pct = abs(p_low2_val - p_low1_val) / max(p_low1_val, 1) * 100
    trix_diff = abs(t_low2_val - t_low1_val)
    strength = min(1.0, (price_diff_pct * 10 + trix_diff) / 2)

    return True, round(strength, 3)


def detect_bearish_divergence(close: np.ndarray, trix: np.ndarray,
                                lookback: int = DIV_LOOKBACK) -> Tuple[bool, float]:
    """하락 다이버전스: 가격 Higher High + TRIX Lower High.

    Returns: (detected, strength 0~1)
    """
    try:
        from scipy.signal import argrelextrema
    except ImportError:
        return False, 0.0

    n = len(close)
    if n < lookback + 10:
        return False, 0.0

    valid_start = 0
    for i in range(n):
        if not np.isnan(trix[i]):
            valid_start = i
            break

    window_start = max(valid_start, n - lookback - 10)
    c_window = close[window_start:]
    t_window = trix[window_start:]

    if len(c_window) < 10:
        return False, 0.0

    price_highs = argrelextrema(c_window, np.greater, order=EXTREMA_ORDER)[0]
    trix_highs = argrelextrema(t_window, np.greater, order=EXTREMA_ORDER)[0]

    if len(price_highs) < 2 or len(trix_highs) < 2:
        return False, 0.0

    p_hi1_idx, p_hi2_idx = price_highs[-2], price_highs[-1]
    p_hi1_val, p_hi2_val = c_window[p_hi1_idx], c_window[p_hi2_idx]

    # 가격: Higher High
    if p_hi2_val <= p_hi1_val:
        return False, 0.0

    def _find_nearest_trix_high(price_idx):
        best = None
        for th in trix_highs:
            if abs(th - price_idx) <= 7:
                if best is None or abs(th - price_idx) < abs(best - price_idx):
                    best = th
        return best

    t_match1 = _find_nearest_trix_high(p_hi1_idx)
    t_match2 = _find_nearest_trix_high(p_hi2_idx)

    if t_match1 is None or t_match2 is None:
        return False, 0.0

    t_hi1_val = t_window[t_match1]
    t_hi2_val = t_window[t_match2]

    # TRIX: Lower High
    if t_hi2_val >= t_hi1_val:
        return False, 0.0

    price_diff_pct = abs(p_hi2_val - p_hi1_val) / max(p_hi1_val, 1) * 100
    trix_diff = abs(t_hi2_val - t_hi1_val)
    strength = min(1.0, (price_diff_pct * 10 + trix_diff) / 2)

    return True, round(strength, 3)


def _check_trix_crossover(trix: np.ndarray, signal: np.ndarray) -> str:
    """TRIX-Signal 교차 확인.

    Returns: 'bullish_cross' | 'bearish_cross' | 'neutral'
    """
    if len(trix) < 2 or len(signal) < 2:
        return "neutral"

    if np.isnan(trix[-1]) or np.isnan(trix[-2]) or np.isnan(signal[-1]) or np.isnan(signal[-2]):
        return "neutral"

    if trix[-2] <= signal[-2] and trix[-1] > signal[-1]:
        return "bullish_cross"
    if trix[-2] >= signal[-2] and trix[-1] < signal[-1]:
        return "bearish_cross"
    return "neutral"


# ═══════════════════════════════════════
#  Main Scanner
# ═══════════════════════════════════════

def scan_trix_divergence(codes: List[str] = None, top_n: int = 10) -> List[Dict]:
    """전체 유니버스 TRIX 상승 다이버전스 스캔.

    Returns: [{code, name, div_strength, adx, di_diff, trix_cross,
               trix_value, composite_score}, ...]  (top_n개)
    """
    # 유니버스 로드
    uni_path = DATA_DIR / "universe.json"
    if not uni_path.exists():
        logger.warning("[TRIX] universe.json 없음")
        return []
    with open(uni_path, "r", encoding="utf-8") as f:
        universe = json.load(f)

    if codes is None:
        codes = [c for c in universe.keys()
                 if c[-1] not in {"5", "7", "8", "9", "K", "L"}]

    results = []
    scanned = 0
    t0 = time.time()

    for code in codes:
        indicators = _calc_trix_indicators(code)
        if indicators is None:
            continue
        scanned += 1

        close = indicators["close"]
        trix = indicators["trix"]
        trix_sig = indicators["trix_signal"]
        adx_arr = indicators["adx"]
        plus_di = indicators["plus_di"]
        minus_di = indicators["minus_di"]

        # ADX 필터
        current_adx = adx_arr[-1] if not np.isnan(adx_arr[-1]) else 0
        if current_adx < MIN_ADX:
            continue

        # DI 방향 (상승 추세)
        current_plus_di = plus_di[-1] if not np.isnan(plus_di[-1]) else 0
        current_minus_di = minus_di[-1] if not np.isnan(minus_di[-1]) else 0
        di_confirmed = current_plus_di > current_minus_di

        # 상승 다이버전스 감지
        detected, strength = detect_bullish_divergence(close, trix, DIV_LOOKBACK)
        if not detected:
            continue

        # TRIX-Signal 교차
        cross = _check_trix_crossover(trix, trix_sig)

        # Composite Score
        composite = strength * 0.5
        if di_confirmed:
            composite += 0.2
        if cross == "bullish_cross":
            composite += 0.3
        composite = min(1.0, composite)

        info = universe.get(code, {})
        results.append({
            "code": code,
            "name": info.get("name", code),
            "div_strength": strength,
            "adx": round(current_adx, 1),
            "di_diff": round(current_plus_di - current_minus_di, 1),
            "trix_cross": cross,
            "trix_value": round(float(trix[-1]), 4) if not np.isnan(trix[-1]) else 0,
            "composite_score": round(composite, 3),
        })

    # 상위 N개
    results.sort(key=lambda x: x["composite_score"], reverse=True)
    results = results[:top_n]

    elapsed = time.time() - t0
    logger.info(f"[TRIX] 스캔 완료: {scanned}종목 검사 → {len(results)}건 감지 ({elapsed:.1f}s)")

    # 캐시 저장
    if results:
        save_trix_cache(results)

    return results


def scan_candidates_trix(codes_names: List[Tuple[str, str]]) -> Dict:
    """기존 추천 후보에 대해 TRIX 다이버전스 분석 (Tier 1).

    Bearish divergence도 감지하여 페널티 제공.

    Returns: {code: {divergence_type, div_strength, adx, di_confirmed,
                     trix_cross, score_adj}}
    """
    result = {}

    for code, name in codes_names:
        indicators = _calc_trix_indicators(code)
        if indicators is None:
            continue

        close = indicators["close"]
        trix = indicators["trix"]
        trix_sig = indicators["trix_signal"]
        adx_arr = indicators["adx"]
        plus_di = indicators["plus_di"]
        minus_di = indicators["minus_di"]

        current_adx = adx_arr[-1] if not np.isnan(adx_arr[-1]) else 0
        di_confirmed = (plus_di[-1] if not np.isnan(plus_di[-1]) else 0) > \
                       (minus_di[-1] if not np.isnan(minus_di[-1]) else 0)
        cross = _check_trix_crossover(trix, trix_sig)

        # Bullish 먼저
        bull_det, bull_str = detect_bullish_divergence(close, trix, DIV_LOOKBACK)
        bear_det, bear_str = detect_bearish_divergence(close, trix, DIV_LOOKBACK)

        if bull_det:
            base = 12
            if current_adx >= MIN_ADX:
                base += 6
            if cross == "bullish_cross":
                base += 7
            score_adj = min(base * bull_str, 25)
            result[code] = {
                "name": name,
                "divergence_type": "bullish",
                "div_strength": bull_str,
                "adx": round(current_adx, 1),
                "di_confirmed": di_confirmed,
                "trix_cross": cross,
                "score_adj": round(score_adj, 1),
            }
        elif bear_det and bear_str > 0.3:
            score_adj = -10.0 * bear_str
            result[code] = {
                "name": name,
                "divergence_type": "bearish",
                "div_strength": bear_str,
                "adx": round(current_adx, 1),
                "di_confirmed": di_confirmed,
                "trix_cross": cross,
                "score_adj": round(score_adj, 1),
            }

    return result


# ═══════════════════════════════════════
#  Cache
# ═══════════════════════════════════════

def save_trix_cache(results: List[Dict]) -> None:
    """trix_signals.json 원자적 저장."""
    data = {
        "scan_date": datetime.now().strftime("%Y-%m-%d"),
        "scanned_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "total_detected": len(results),
        "candidates": results,
    }
    tmp = CACHE_PATH.with_suffix(".tmp")
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        tmp.replace(CACHE_PATH)
    except Exception as e:
        logger.error(f"[TRIX] 캐시 저장 실패: {e}")
        if tmp.exists():
            tmp.unlink()


def load_trix_cache() -> Optional[Dict]:
    """trix_signals.json 로드 (3시간 이내면 재사용).

    Returns: {code: {name, source, div_strength, ...}} dict 또는 None
    """
    if not CACHE_PATH.exists():
        return None
    try:
        with open(CACHE_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)

        scanned_at = data.get("scanned_at", "")
        if not scanned_at:
            return None

        cache_time = datetime.strptime(scanned_at, "%Y-%m-%d %H:%M:%S")
        if (datetime.now() - cache_time).total_seconds() > CACHE_TTL_HOURS * 3600:
            return None

        candidates = data.get("candidates", [])
        if not candidates:
            return None

        result = {}
        for c in candidates:
            result[c["code"]] = {
                "name": c.get("name", c["code"]),
                "source": "trix_divergence",
                "div_strength": c.get("div_strength", 0),
                "adx": c.get("adx", 0),
                "trix_cross": c.get("trix_cross", "neutral"),
                "composite_score": c.get("composite_score", 0),
            }
        return result

    except Exception:
        return None


# ═══════════════════════════════════════
#  CLI Test
# ═══════════════════════════════════════

if __name__ == "__main__":
    import sys
    sys.stdout.reconfigure(encoding="utf-8")

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(name)s] %(message)s")

    print("=" * 60)
    print("  TRIX Divergence Scanner")
    print("=" * 60)

    # 단일 종목 테스트 (삼성전자)
    print("\n[삼성전자 005930 TRIX 계산]")
    ind = _calc_trix_indicators("005930")
    if ind:
        trix = ind["trix"]
        adx = ind["adx"]
        valid_trix = trix[~np.isnan(trix)]
        print(f"  TRIX(12) 최근값: {valid_trix[-1]:.4f}")
        print(f"  ADX(14): {adx[-1]:.1f}")
        print(f"  Plus_DI: {ind['plus_di'][-1]:.1f}")
        print(f"  Minus_DI: {ind['minus_di'][-1]:.1f}")
        print(f"  거래대금: {ind['trading_value']/1e8:,.0f}억")

        bull, bull_str = detect_bullish_divergence(ind["close"], trix)
        bear, bear_str = detect_bearish_divergence(ind["close"], trix)
        cross = _check_trix_crossover(trix, ind["trix_signal"])
        print(f"  Bullish div: {bull} (strength={bull_str:.3f})")
        print(f"  Bearish div: {bear} (strength={bear_str:.3f})")
        print(f"  TRIX cross: {cross}")
    else:
        print("  데이터 부족")

    # 전체 스캔
    print("\n[전체 유니버스 스캔]")
    results = scan_trix_divergence(top_n=10)
    for i, r in enumerate(results, 1):
        print(f"  {i}. {r['name']}({r['code']}) "
              f"div={r['div_strength']:.3f} ADX={r['adx']:.0f} "
              f"DI_diff={r['di_diff']:+.1f} cross={r['trix_cross']} "
              f"score={r['composite_score']:.3f}")

    print("=" * 60)
