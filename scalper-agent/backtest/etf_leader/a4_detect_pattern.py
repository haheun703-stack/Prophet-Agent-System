# -*- coding: utf-8 -*-
"""A-4: ETF 차트 패턴 감지 — 차트영웅 "다년 저항선 돌파 + 눌림목" 로직

차트영웅 핵심 패턴:
  N년차: 다년간 박스권 저항선 돌파 (양봉)
  N+1년차: 눌림목 형성 (음봉/조정)
  N+2년차: 폭등 (다음 양봉)

본 모듈은 24-12-30 기준일 시점에서 위 패턴을 감지하여,
"내년에 폭등할 가능성이 높은 ETF"를 점수화한다.

입력: 월봉 60개월(5년) OHLCV DataFrame
출력: 패턴 정보 dict + 점수(0~100)

점수 구성:
  - 돌파 강도 (25점): 저항선 대비 돌파 폭 + 거래량 동반
  - 박스권 기간 (20점): 길수록 좋음 (12~36개월 베스트)
  - 현재 눌림 정도 (30점): -15~-25% 이상적
  - 돌파 후 경과 (15점): 6~18개월 베스트
  - 추세 강도 (10점): 24개월 이평선 대비 위치
"""
import logging
from typing import Dict, Optional
import pandas as pd

logger = logging.getLogger("A4.Pattern")


def find_resistance_breakout(
    monthly_df: pd.DataFrame,
    base_date_idx: int,
    max_lookback_months: int = 36,
    breakout_window: int = 18,
    min_box_months: int = 12,
) -> Optional[Dict]:
    """다년 저항선 돌파 감지 (가변 lookback).

    Args:
        monthly_df: 월봉 DataFrame
        base_date_idx: 기준일 인덱스
        max_lookback_months: 최대 저항선 형성 기간 (36=3년)
        breakout_window: 최근 N개월 내 돌파 인정 (18)
        min_box_months: 박스권 최소 기간 (12 = 1년)

    Returns:
        dict or None
    """
    # 가변 lookback: 데이터 가용 범위 내에서 자동 결정
    available_for_box = base_date_idx - breakout_window
    if available_for_box < min_box_months:
        return None  # 박스권 1년치도 안 됨

    lookback_months = min(max_lookback_months, available_for_box)

    # 1. 박스권 구간: [base - lookback ~ base - breakout_window]
    box_start = base_date_idx - lookback_months
    box_end = base_date_idx - breakout_window

    if box_start < 0 or box_end <= box_start:
        return None

    box_period = monthly_df.iloc[box_start:box_end]
    if box_period.empty:
        return None

    resistance = box_period["고가"].max()
    avg_value = box_period["거래대금"].mean()

    # 2. 최근 breakout_window 개월 내에서 돌파 시점 찾기
    recent = monthly_df.iloc[box_end:base_date_idx + 1]

    # 종가 기준 돌파 (월봉 종가가 저항선 위로 마감)
    breakout_mask = recent["종가"] > resistance
    if not breakout_mask.any():
        return None

    # 최초 돌파 시점
    breakout_idx_rel = breakout_mask.idxmax()  # 첫 True의 index
    breakout_row = recent.loc[breakout_idx_rel]

    breakout_idx = monthly_df.index.get_loc(breakout_idx_rel)

    return {
        "breakout_idx": int(breakout_idx),
        "breakout_month": breakout_idx_rel.strftime("%Y-%m"),
        "breakout_close": float(breakout_row["종가"]),
        "breakout_high": float(breakout_row["고가"]),
        "breakout_volume_value": float(breakout_row["거래대금"]),
        "pre_breakout_high": float(resistance),
        "pre_breakout_period_months": int(box_end - box_start),
        "pre_breakout_avg_value": float(avg_value),
        "volume_surge_ratio": round(float(breakout_row["거래대금"] / avg_value), 2) if avg_value > 0 else 0,
        "breakout_strength_pct": round((float(breakout_row["종가"]) - resistance) / resistance * 100, 2),
    }


def analyze_post_breakout(
    monthly_df: pd.DataFrame,
    breakout_idx: int,
    base_date_idx: int,
) -> Dict:
    """돌파 후 흐름 분석 — 최고가, 현재 눌림 정도.

    Returns:
        dict: {
            'post_breakout_high': 돌파 후 기록한 최고가,
            'post_breakout_high_month': 최고가 월,
            'current_close': 기준일 종가,
            'current_drawdown_pct': 최고가 대비 현재 하락률,
            'months_since_breakout': 돌파 후 경과 개월,
            'months_since_high': 최고가 후 경과 개월,
        }
    """
    post = monthly_df.iloc[breakout_idx:base_date_idx + 1]

    high = post["고가"].max()
    high_idx_rel = post["고가"].idxmax()
    high_idx = monthly_df.index.get_loc(high_idx_rel)

    current = monthly_df.iloc[base_date_idx]
    current_close = float(current["종가"])

    drawdown = (current_close - high) / high * 100 if high > 0 else 0

    return {
        "post_breakout_high": float(high),
        "post_breakout_high_month": high_idx_rel.strftime("%Y-%m"),
        "current_close": current_close,
        "current_drawdown_pct": round(drawdown, 2),
        "months_since_breakout": int(base_date_idx - breakout_idx),
        "months_since_high": int(base_date_idx - high_idx),
    }


def compute_trend(monthly_df: pd.DataFrame, base_date_idx: int) -> Dict:
    """24개월 이평선 대비 위치 + 최근 모멘텀."""
    closes = monthly_df["종가"]
    if base_date_idx < 24:
        return {"ma24": 0, "ma24_gap_pct": 0, "ret_6m": 0}

    window24 = closes.iloc[base_date_idx - 23:base_date_idx + 1]
    ma24 = float(window24.mean())

    current = float(closes.iloc[base_date_idx])
    ma24_gap = (current - ma24) / ma24 * 100 if ma24 > 0 else 0

    # 6개월 수익률
    if base_date_idx >= 6:
        ret_6m = (current / float(closes.iloc[base_date_idx - 6]) - 1) * 100
    else:
        ret_6m = 0

    return {
        "ma24": round(ma24, 2),
        "ma24_gap_pct": round(ma24_gap, 2),
        "ret_6m_pct": round(ret_6m, 2),
    }


def score_pattern(
    breakout: Dict,
    post: Dict,
    trend: Dict,
) -> Dict:
    """패턴 점수 계산 (0~100점) + 패턴 타입 분류."""
    score = 0
    detail = {}

    # 1) 돌파 강도 (25점)
    strength = breakout["breakout_strength_pct"]  # 저항선 대비 돌파 폭
    vol_surge = breakout["volume_surge_ratio"]

    if strength >= 10:
        s1 = 15
    elif strength >= 5:
        s1 = 12
    elif strength >= 2:
        s1 = 8
    else:
        s1 = 4
    # 거래량 동반
    if vol_surge >= 2.0:
        s1 += 10
    elif vol_surge >= 1.5:
        s1 += 7
    elif vol_surge >= 1.2:
        s1 += 4
    else:
        s1 += 1
    score += s1
    detail["s1_breakout_strength"] = s1

    # 2) 박스권 기간 (20점) — 길수록 좋음
    box_months = breakout["pre_breakout_period_months"]
    if box_months >= 24:
        s2 = 20
    elif box_months >= 18:
        s2 = 16
    elif box_months >= 12:
        s2 = 12
    elif box_months >= 6:
        s2 = 7
    else:
        s2 = 3
    score += s2
    detail["s2_box_period"] = s2

    # 3) 현재 눌림 정도 (30점) — 차트영웅 핵심
    dd = post["current_drawdown_pct"]
    if -25 <= dd <= -15:        # 이상적 눌림목
        s3 = 30
    elif -30 <= dd < -25:        # 깊은 눌림 (기회)
        s3 = 25
    elif -15 < dd <= -8:         # 얕은 눌림
        s3 = 22
    elif -35 < dd < -30:         # 너무 깊음
        s3 = 15
    elif -8 < dd <= 0:           # 거의 고점 (시기 X)
        s3 = 8
    elif dd > 0:                 # 신고가 갱신 (이미 늦음)
        s3 = 3
    else:                        # < -35: 추세 훼손
        s3 = 5
    score += s3
    detail["s3_drawdown"] = s3

    # 4) 돌파 후 경과 (15점) — 6~18개월이 베스트
    months_after = post["months_since_breakout"]
    if 6 <= months_after <= 18:
        s4 = 15
    elif 18 < months_after <= 30:
        s4 = 10
    elif 3 <= months_after < 6:
        s4 = 8
    elif months_after > 30:
        s4 = 5
    else:                         # < 3개월 (너무 일찍)
        s4 = 6
    score += s4
    detail["s4_months_after"] = s4

    # 5) 추세 강도 (10점) — 24M 이평선 위에 있어야
    ma_gap = trend["ma24_gap_pct"]
    if ma_gap >= 15:
        s5 = 10
    elif ma_gap >= 5:
        s5 = 8
    elif ma_gap >= 0:
        s5 = 6
    elif ma_gap >= -10:
        s5 = 4   # 24M 이평선 살짝 아래도 인정
    else:
        s5 = 1
    score += s5
    detail["s5_trend"] = s5

    # 패턴 타입 분류
    if dd <= -8 and 6 <= months_after <= 24 and box_months >= 12:
        pattern_type = "CLEAR_BREAKOUT_PULLBACK"      # 차트영웅 패턴 ★
    elif dd > -8 and months_after <= 12:
        pattern_type = "EARLY_BREAKOUT"                # 막 돌파, 아직 안 눌림
    elif dd <= -30:
        pattern_type = "DEEP_PULLBACK"                  # 깊은 조정
    elif months_after > 24:
        pattern_type = "EXTENDED_BREAKOUT"              # 돌파 후 너무 시간 경과
    else:
        pattern_type = "NORMAL"

    return {
        "score": round(score, 1),
        "pattern_type": pattern_type,
        "score_detail": detail,
    }


def detect_chart_hero_pattern(
    monthly_df: pd.DataFrame,
    base_date_idx: Optional[int] = None,
) -> Optional[Dict]:
    """차트영웅 패턴 종합 감지.

    Returns:
        None: 돌파 없음
        Dict: 패턴 정보 + 점수
    """
    if monthly_df is None or len(monthly_df) < 24:
        return None  # 24개월(2년) 미만이면 패턴 자체 의미 없음

    if base_date_idx is None:
        base_date_idx = len(monthly_df) - 1

    # 1) 다년 저항선 돌파 감지
    breakout = find_resistance_breakout(monthly_df, base_date_idx)
    if breakout is None:
        return None

    # 2) 돌파 후 흐름
    post = analyze_post_breakout(monthly_df, breakout["breakout_idx"], base_date_idx)

    # 3) 추세
    trend = compute_trend(monthly_df, base_date_idx)

    # 4) 점수화
    scoring = score_pattern(breakout, post, trend)

    return {
        **breakout,
        **post,
        **trend,
        **scoring,
    }


# 단독 테스트
if __name__ == "__main__":
    import sys, io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
    from dotenv import load_dotenv
    from pathlib import Path

    ROOT = Path(__file__).resolve().parent.parent.parent
    load_dotenv(ROOT.parent / ".env")
    from pykrx import stock

    # 차트영웅 언급 ETF로 검증
    test_etfs = {
        "102960": "KODEX 기계장비",
        "139230": "TIGER 200 중공업",
        "445290": "KODEX 로봇액티브",
        "433500": "ACE 원자력TOP10",
        "489250": "TIGER 200",  # 비교용 (전체 시장)
    }

    print(f"\n{'='*70}")
    print(f"  A-4 패턴 감지 단독 테스트 (24-12-30 기준)")
    print(f"{'='*70}\n")

    for ticker, name in test_etfs.items():
        try:
            df = stock.get_etf_ohlcv_by_date("20200101", "20241231", ticker, freq="m")
        except Exception as e:
            print(f"  [{ticker}] {name} 조회실패: {e}\n")
            continue

        if df is None or len(df) < 36:
            print(f"  [{ticker}] {name} 데이터부족 ({len(df) if df is not None else 0}개월)\n")
            continue

        result = detect_chart_hero_pattern(df)
        if result is None:
            print(f"  [{ticker}] {name}: 돌파 없음\n")
            continue

        print(f"━━ [{ticker}] {name} ━━")
        print(f"  돌파월: {result['breakout_month']}  강도: {result['breakout_strength_pct']:+.1f}%")
        print(f"  저항선: {result['pre_breakout_high']:>8,.0f}  돌파종가: {result['breakout_close']:>8,.0f}")
        print(f"  박스권: {result['pre_breakout_period_months']}개월  거래량서지: {result['volume_surge_ratio']}배")
        print(f"  돌파후고점: {result['post_breakout_high']:>8,.0f} ({result['post_breakout_high_month']})")
        print(f"  현재종가:   {result['current_close']:>8,.0f}  눌림: {result['current_drawdown_pct']:+.2f}%")
        print(f"  돌파후 {result['months_since_breakout']}개월 / 고점후 {result['months_since_high']}개월")
        print(f"  24M 이평선 대비: {result['ma24_gap_pct']:+.1f}%  6M수익: {result['ret_6m_pct']:+.1f}%")
        print(f"  ★ 점수: {result['score']:.1f}  타입: {result['pattern_type']}")
        print(f"  세부: {result['score_detail']}")
        print()
