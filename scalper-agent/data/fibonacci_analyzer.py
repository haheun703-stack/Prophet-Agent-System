# -*- coding: utf-8 -*-
"""
피보나치 레벨 분석기
===================
종목별 피보나치 되돌림/확장 레벨 자동 계산.
떨어지면 → 어디서 살지 (지지 레벨)
올라가면 → 어디까지 갈지 (상방 타겟)

v1: 2026-04-04
"""

import logging
from dataclasses import dataclass, field, asdict
from typing import List, Optional, Tuple, Dict
from pathlib import Path

import numpy as np
import pandas as pd

from data.extend_parquet_data import load_daily

logger = logging.getLogger(__name__)

# 피보나치 되돌림 레벨
FIB_RETRACEMENT = [0.236, 0.382, 0.5, 0.618, 0.786]
# 피보나치 확장 레벨
FIB_EXTENSION = [1.0, 1.272, 1.618, 2.0, 2.618]


@dataclass
class FibLevel:
    """피보나치 레벨 하나"""
    ratio: float        # 0.382, 0.618 등
    price: int          # 가격
    label: str          # "0.382 지지", "1.618 타겟"
    distance_pct: float # 현재가 대비 거리 (%)


@dataclass
class FibResult:
    """피보나치 분석 결과"""
    code: str
    name: str
    current_price: int
    swing_high: int         # 스윙 고점
    swing_low: int          # 스윙 저점
    swing_high_date: str    # 고점 날짜
    swing_low_date: str     # 저점 날짜
    trend: str              # "UP" (저점→고점 순서) / "DOWN" (고점→저점 순서)

    # 되돌림 레벨 (하방 지지)
    retracement_levels: List[FibLevel] = field(default_factory=list)
    # 확장 레벨 (상방 타겟)
    extension_levels: List[FibLevel] = field(default_factory=list)

    # 현재 위치 판단
    position_label: str = ""      # "0.618 지지 부근", "1.272 타겟 접근" 등
    nearest_support: int = 0      # 가장 가까운 하방 지지
    nearest_resistance: int = 0   # 가장 가까운 상방 저항
    upside_pct: float = 0.0       # 다음 타겟까지 상방 여력 (%)
    downside_pct: float = 0.0     # 다음 지지까지 하방 (%)


def _find_swing_points(closes: np.ndarray, window: int = 5) -> Tuple[List, List]:
    """로컬 고점/저점 찾기 (zigzag 방식)"""
    highs = []  # (index, price)
    lows = []

    for i in range(window, len(closes) - window):
        # 로컬 고점: 앞뒤 window 내 최대값
        if closes[i] == max(closes[i - window:i + window + 1]):
            highs.append((i, closes[i]))
        # 로컬 저점: 앞뒤 window 내 최소값
        if closes[i] == min(closes[i - window:i + window + 1]):
            lows.append((i, closes[i]))

    return highs, lows


def _detect_major_swing(df: pd.DataFrame, lookback: int = 120) -> Tuple[int, int, str, str, str]:
    """
    주요 스윙 고점/저점 + 추세 방향 판단.

    Returns:
        (swing_high, swing_low, high_date, low_date, trend)
        trend: "UP" 상승추세(저점이 먼저) / "DOWN" 하락추세(고점이 먼저)
    """
    close_col = "종가" if "종가" in df.columns else "close"
    recent = df.tail(lookback)
    closes = recent[close_col].astype(float).values
    dates = recent.index

    # 전체 구간 최고/최저
    high_idx = int(np.argmax(closes))
    low_idx = int(np.argmin(closes))

    swing_high = int(closes[high_idx])
    swing_low = int(closes[low_idx])
    high_date = str(dates[high_idx])[:10]
    low_date = str(dates[low_idx])[:10]

    # 고점이 저점보다 나중이면 상승추세 (아직 올라가는 중)
    # 고점이 저점보다 먼저면 하락추세 (고점 찍고 내려오는 중)
    trend = "UP" if high_idx > low_idx else "DOWN"

    return swing_high, swing_low, high_date, low_date, trend


def fib_analyze(code: str, name: str = "", lookback: int = 120) -> Optional[FibResult]:
    """
    종목의 피보나치 레벨 분석.

    Args:
        code: 종목코드
        name: 종목명 (없으면 코드 사용)
        lookback: 분석 기간 (일)

    Returns:
        FibResult 또는 None
    """
    df = load_daily(code)
    if df is None or len(df) < 30:
        return None

    close_col = "종가" if "종가" in df.columns else "close"
    current_price = int(df[close_col].iloc[-1])
    if not name:
        name = code

    swing_high, swing_low, high_date, low_date, trend = _detect_major_swing(df, lookback)

    if swing_high <= swing_low:
        return None

    diff = swing_high - swing_low

    result = FibResult(
        code=code,
        name=name,
        current_price=current_price,
        swing_high=swing_high,
        swing_low=swing_low,
        swing_high_date=high_date,
        swing_low_date=low_date,
        trend=trend,
    )

    # ── 되돌림 레벨 (고점에서 내려오는 지지선) ──
    for ratio in FIB_RETRACEMENT:
        price = int(swing_high - diff * ratio)
        dist = (price - current_price) / current_price * 100
        label = f"{ratio} 지지" if dist <= 0 else f"{ratio} 저항"
        result.retracement_levels.append(FibLevel(ratio, price, label, round(dist, 1)))

    # ── 확장 레벨 (저점에서 올라가는 타겟) ──
    for ratio in FIB_EXTENSION:
        price = int(swing_low + diff * ratio)
        dist = (price - current_price) / current_price * 100
        label = f"{ratio} 타겟"
        result.extension_levels.append(FibLevel(ratio, price, label, round(dist, 1)))

    # ── 현재 위치 판단 ──
    all_levels = []
    for lv in result.retracement_levels:
        all_levels.append((lv.price, lv.ratio, "ret"))
    for lv in result.extension_levels:
        all_levels.append((lv.price, lv.ratio, "ext"))
    all_levels.sort(key=lambda x: x[0])

    # 가장 가까운 상/하 레벨 찾기
    below = [(p, r, t) for p, r, t in all_levels if p <= current_price]
    above = [(p, r, t) for p, r, t in all_levels if p > current_price]

    if below:
        sup_price, sup_ratio, sup_type = below[-1]
        result.nearest_support = sup_price
        result.downside_pct = round((current_price - sup_price) / current_price * 100, 1)

    if above:
        res_price, res_ratio, res_type = above[0]
        result.nearest_resistance = res_price
        result.upside_pct = round((res_price - current_price) / current_price * 100, 1)

    # 위치 라벨
    if below and above:
        _, br, bt = below[-1]
        _, ar, at = above[0]

        # 가장 가까운 레벨과의 거리
        dist_below = abs(current_price - below[-1][0])
        dist_above = abs(above[0][0] - current_price)

        if dist_below < diff * 0.03:  # 레벨 근처 3% 이내
            if bt == "ret":
                result.position_label = f"{br} 되돌림 지지 부근"
            else:
                result.position_label = f"{br} 확장 타겟 부근"
        elif dist_above < diff * 0.03:
            if at == "ret":
                result.position_label = f"{ar} 되돌림 저항 접근"
            else:
                result.position_label = f"{ar} 확장 타겟 접근"
        else:
            result.position_label = f"{br}~{ar} 구간"
    elif current_price >= swing_high:
        result.position_label = "신고가 돌파"
    elif current_price <= swing_low:
        result.position_label = "신저가 이탈"

    return result


def batch_fib_analyze(codes_names: List[Tuple[str, str]], lookback: int = 120) -> List[FibResult]:
    """여러 종목 일괄 분석."""
    results = []
    for code, name in codes_names:
        r = fib_analyze(code, name, lookback)
        if r:
            results.append(r)
    return results


def format_fib_telegram(result: FibResult) -> str:
    """텔레그램 메시지용 포맷."""
    lines = []
    lines.append(f"📐 {result.name} 피보나치 분석")
    lines.append(f"현재가: {result.current_price:,}원 | 추세: {'↗ 상승' if result.trend == 'UP' else '↘ 하락'}")
    lines.append(f"고점: {result.swing_high:,} ({result.swing_high_date})")
    lines.append(f"저점: {result.swing_low:,} ({result.swing_low_date})")
    lines.append(f"위치: {result.position_label}")

    lines.append("")
    lines.append("── 하방 지지 ──")
    for lv in result.retracement_levels:
        marker = "◀" if abs(lv.distance_pct) < 3 else ""
        lines.append(f"  {lv.ratio}: {lv.price:>10,}원 ({lv.distance_pct:+.1f}%) {marker}")

    lines.append("")
    lines.append("── 상방 타겟 ──")
    for lv in result.extension_levels:
        marker = "◀" if abs(lv.distance_pct) < 3 else ""
        lines.append(f"  {lv.ratio}: {lv.price:>10,}원 ({lv.distance_pct:+.1f}%) {marker}")

    if result.upside_pct > 0:
        lines.append(f"\n상방 여력: +{result.upside_pct:.1f}% → {result.nearest_resistance:,}원")
    if result.downside_pct > 0:
        lines.append(f"하방 지지: -{result.downside_pct:.1f}% → {result.nearest_support:,}원")

    return "\n".join(lines)


def fib_score_adjustment(code: str, name: str = "", lookback: int = 120) -> Dict:
    """
    추천 파이프라인용 피보나치 점수 조정.

    Returns:
        {
            "fib_adj": float,        # 점수 가감 (-10 ~ +20)
            "fib_position": str,     # 위치 설명
            "upside_pct": float,     # 상방 여력
            "nearest_support": int,  # 하방 지지가
            "nearest_target": int,   # 상방 타겟가
            "sl_fib": int,           # 피보나치 기반 손절가
            "tp_fib": int,           # 피보나치 기반 목표가
        }
    """
    result = fib_analyze(code, name, lookback)
    if not result:
        return {"fib_adj": 0, "fib_position": "분석불가"}

    adj = 0.0
    cp = result.current_price

    # 1. 되돌림 지지선 부근에서 반등 → 매수 기회 (+점수)
    # 4/26 학습: fib 팩터 46건 D+1 avg -0.30% → 가산 50% 축소
    for lv in result.retracement_levels:
        dist = abs(cp - lv.price) / cp * 100
        if dist < 3:  # 지지선 3% 이내
            if lv.ratio == 0.618:
                adj += 7   # 황금비 지지 (15→7, 4/26 학습)
            elif lv.ratio == 0.5:
                adj += 5   # (10→5)
            elif lv.ratio == 0.382:
                adj += 4   # (8→4)
            break  # 가장 가까운 것만

    # 2. 확장 타겟 접근 → 차익실현 구간 (-점수)
    for lv in result.extension_levels:
        dist = abs(cp - lv.price) / cp * 100
        if dist < 3 and lv.ratio >= 1.272:
            adj -= 5  # 타겟 근접 → 추격 매수 자제 (유지)
            break

    # 3. 상방 여력 반영
    # 4/26 학습: 상방 여력 가산도 축소 (5→3)
    if result.upside_pct > 15:
        adj += 3   # 상방 여력 충분 (5→3)
    elif result.upside_pct < 3:
        adj -= 5   # 상방 여력 부족 (유지)

    # 4. 피보나치 기반 TP/SL 계산
    # SL: 가장 가까운 아래 지지선 아래 2%
    sl_fib = int(result.nearest_support * 0.98) if result.nearest_support > 0 else 0
    # TP: 가장 가까운 위 저항/타겟
    tp_fib = result.nearest_resistance if result.nearest_resistance > 0 else 0

    return {
        "fib_adj": round(adj, 1),
        "fib_position": result.position_label,
        "upside_pct": result.upside_pct,
        "downside_pct": result.downside_pct,
        "nearest_support": result.nearest_support,
        "nearest_target": tp_fib,
        "sl_fib": sl_fib,
        "tp_fib": tp_fib,
        "trend": result.trend,
        "swing_high": result.swing_high,
        "swing_low": result.swing_low,
    }


# ── CLI 테스트 ──
if __name__ == "__main__":
    import sys
    code = sys.argv[1] if len(sys.argv) > 1 else "005930"
    name = sys.argv[2] if len(sys.argv) > 2 else code

    result = fib_analyze(code, name)
    if result:
        print(format_fib_telegram(result))
        print()
        adj = fib_score_adjustment(code, name)
        print(f"점수 조정: {adj}")
    else:
        print(f"{code}: 분석 불가")
