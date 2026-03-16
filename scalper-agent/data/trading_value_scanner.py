# -*- coding: utf-8 -*-
"""
거래대금 폭발 스캐너 (Trading Value Scanner)
=============================================
"거래량이 아니라 돈의 흐름을 본다."

거래대금 = 종가 x 거래량  (실제 유입된 금액)
거래량만 보면 저가주의 왜곡을 놓치지만,
거래대금은 기관이 실제로 투입한 금액을 반영한다.

3가지 패턴 감지:
  - EXPLOSION:          거래대금 3x+ (급격한 자금 유입)
  - QUIET_ACCUMULATION: 거래대금 2x+ AND 가격 변동 ±3% (조용한 매집)
  - GRADUAL_BUILDUP:    5일간 거래대금 추세 상승 + 1.5x+ (점진적 증가)

Usage:
  python data/trading_value_scanner.py              # 전체 스캔 + 저장
  python data/trading_value_scanner.py --top 20     # TOP 20만 출력
  python data/trading_value_scanner.py --code 005930
  python data/trading_value_scanner.py --pattern QUIET
"""

import json
import logging
import sys
import time
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data_store"
DAILY_DIR = DATA_DIR / "daily"

logger = logging.getLogger("BH.TVScanner")

# 억원 단위 변환
_BILLION = 1e8


# =============================================================================
# 데이터 구조
# =============================================================================

@dataclass
class TVSignal:
    """거래대금 이상 신호"""
    code: str
    name: str
    sector: str
    close: int                    # 현재가
    change_pct: float             # 당일 등락률 (%)
    trading_value: float          # 오늘 거래대금 (억원)
    tv_avg20: float               # 20일 평균 거래대금 (억원)
    tv_ratio: float               # 거래대금 비율 (배수)
    tv_ratio_5d_trend: float      # 5일간 tv_ratio 추세 기울기
    pattern: str                  # EXPLOSION / QUIET_ACCUMULATION / GRADUAL_BUILDUP / NORMAL
    score: float                  # 0 ~ 100 종합 스코어
    detail: str                   # 요약 텍스트


# =============================================================================
# 핵심 계산
# =============================================================================

def _load_daily(code: str) -> Optional[pd.DataFrame]:
    """일봉 CSV 로드"""
    path = DAILY_DIR / f"{code}.csv"
    if not path.exists():
        return None
    try:
        df = pd.read_csv(path, index_col=0, parse_dates=True)
        if len(df) < 21:
            return None
        return df.sort_index()
    except Exception:
        return None


def _calc_tv_metrics(code: str) -> Optional[dict]:
    """단일 종목 거래대금 메트릭 계산

    Returns: {
        close, change_pct, trading_value, tv_avg20, tv_ratio,
        tv_ratio_5d_trend, high, low
    } or None
    """
    daily = _load_daily(code)
    if daily is None:
        return None

    try:
        closes = daily["종가"]
        volumes = daily["거래량"]
        highs = daily["고가"]

        # 거래대금 시리즈
        tv_series = closes * volumes / _BILLION  # 억원

        # 최근값
        latest_tv = float(tv_series.iloc[-1])
        avg_20d_tv = float(tv_series.iloc[-21:-1].mean())

        if avg_20d_tv <= 0:
            return None

        tv_ratio = latest_tv / avg_20d_tv

        # 5일 tv_ratio 추세 (선형 회귀 기울기)
        tv_ratio_5d_trend = 0.0
        if len(tv_series) >= 25:
            recent_5_ratios = []
            for i in range(-5, 0):
                day_tv = float(tv_series.iloc[i])
                day_avg = float(tv_series.iloc[i-20:i].mean())
                if day_avg > 0:
                    recent_5_ratios.append(day_tv / day_avg)
                else:
                    recent_5_ratios.append(1.0)
            if len(recent_5_ratios) == 5:
                x = np.arange(5)
                coeffs = np.polyfit(x, recent_5_ratios, 1)
                tv_ratio_5d_trend = float(coeffs[0])

        # 당일 등락률
        change_pct = float(daily["등락률"].iloc[-1]) if "등락률" in daily.columns else 0.0

        return {
            "close": int(closes.iloc[-1]),
            "change_pct": round(change_pct, 2),
            "trading_value": round(latest_tv, 1),
            "tv_avg20": round(avg_20d_tv, 1),
            "tv_ratio": round(tv_ratio, 3),
            "tv_ratio_5d_trend": round(tv_ratio_5d_trend, 4),
            "high": int(highs.iloc[-1]),
        }
    except Exception as e:
        logger.debug(f"[{code}] 메트릭 계산 실패: {e}")
        return None


def _classify_pattern(tv_ratio: float, change_pct: float, tv_5d_trend: float) -> str:
    """패턴 분류"""
    abs_change = abs(change_pct)

    # QUIET_ACCUMULATION: 돈은 들어오는데 가격은 안 움직임 (가장 강한 시그널)
    if tv_ratio >= 2.0 and abs_change <= 3.0:
        return "QUIET_ACCUMULATION"

    # EXPLOSION: 거래대금 급증 (방향 불문)
    if tv_ratio >= 3.0:
        return "EXPLOSION"

    # GRADUAL_BUILDUP: 서서히 증가 (5일 추세)
    if tv_5d_trend > 0.15 and tv_ratio >= 1.5:
        return "GRADUAL_BUILDUP"

    return "NORMAL"


def _score_signal(tv_ratio: float, change_pct: float, tv_5d_trend: float,
                  trading_value: float, close: int, high: int) -> float:
    """5팩터 스코어링 (0~100)

    (A) 거래대금 배수       max 30
    (B) 가격 안정성         max 25
    (C) 추세 지속성         max 20
    (D) 거래대금 절대값     max 15
    (E) 종가 위치           max 10
    """
    score = 0.0

    # (A) 거래대금 배수
    if tv_ratio >= 5.0:
        score += 30
    elif tv_ratio >= 3.0:
        score += 25
    elif tv_ratio >= 2.0:
        score += 20
    elif tv_ratio >= 1.5:
        score += 10

    # (B) 가격 안정성 (|등락률|이 작을수록 좋음 = 조용한 매집)
    abs_change = abs(change_pct)
    if abs_change <= 1.0:
        score += 25
    elif abs_change <= 2.0:
        score += 20
    elif abs_change <= 3.0:
        score += 15
    elif abs_change <= 5.0:
        score += 5

    # (C) 추세 지속성 (5일간 tv_ratio 기울기)
    if tv_5d_trend >= 0.30:
        score += 20
    elif tv_5d_trend >= 0.15:
        score += 10
    elif tv_5d_trend >= 0.05:
        score += 5

    # (D) 거래대금 절대값 (억원)
    if trading_value >= 100:
        score += 15
    elif trading_value >= 50:
        score += 10
    elif trading_value >= 20:
        score += 5

    # (E) 종가 위치 (고가 대비 종가 비율 — 상한 마감일수록 강세)
    if high > 0:
        close_high_ratio = close / high
        if close_high_ratio >= 0.97:
            score += 10
        elif close_high_ratio >= 0.93:
            score += 7
        elif close_high_ratio >= 0.90:
            score += 5

    return round(score, 1)


# =============================================================================
# 메인 스캔 함수
# =============================================================================

def scan_trading_value(
    universe: dict,
    min_tv_billion: float = 10.0,
) -> list[TVSignal]:
    """유니버스 전체 거래대금 스캔

    Args:
        universe: {code: {name, sector, cap_억, ...}}
        min_tv_billion: 최소 거래대금 (억원) — 잡주 필터

    Returns:
        TVSignal 리스트 (score 내림차순, NORMAL 제외)
    """
    signals = []
    scanned = 0
    skipped = 0

    codes = list(universe.keys())
    t0 = time.time()

    for code in codes:
        info = universe.get(code, {})
        name = info.get("name", code)
        sector = info.get("sector", "")

        metrics = _calc_tv_metrics(code)
        if metrics is None:
            skipped += 1
            continue

        scanned += 1

        # 최소 거래대금 필터
        if metrics["trading_value"] < min_tv_billion:
            continue

        pattern = _classify_pattern(
            metrics["tv_ratio"],
            metrics["change_pct"],
            metrics["tv_ratio_5d_trend"],
        )

        if pattern == "NORMAL":
            continue

        score = _score_signal(
            tv_ratio=metrics["tv_ratio"],
            change_pct=metrics["change_pct"],
            tv_5d_trend=metrics["tv_ratio_5d_trend"],
            trading_value=metrics["trading_value"],
            close=metrics["close"],
            high=metrics["high"],
        )

        # 상세 텍스트 생성
        parts = [f"거래대금 {metrics['tv_ratio']:.1f}x"]
        if abs(metrics["change_pct"]) <= 3.0:
            parts.append(f"가격보합 {metrics['change_pct']:+.1f}%")
        else:
            parts.append(f"등락 {metrics['change_pct']:+.1f}%")
        if metrics["tv_ratio_5d_trend"] > 0.05:
            parts.append(f"5일추세 +{metrics['tv_ratio_5d_trend']:.2f}")
        parts.append(f"= {pattern}")
        detail = " + ".join(parts)

        signals.append(TVSignal(
            code=code,
            name=name,
            sector=sector,
            close=metrics["close"],
            change_pct=metrics["change_pct"],
            trading_value=metrics["trading_value"],
            tv_avg20=metrics["tv_avg20"],
            tv_ratio=metrics["tv_ratio"],
            tv_ratio_5d_trend=metrics["tv_ratio_5d_trend"],
            pattern=pattern,
            score=score,
            detail=detail,
        ))

    signals.sort(key=lambda s: s.score, reverse=True)

    elapsed = time.time() - t0
    logger.info(
        f"[TV Scanner] {scanned}종목 스캔 ({skipped} 스킵) "
        f"→ {len(signals)}개 이상신호 감지 ({elapsed:.1f}s)"
    )

    return signals


# =============================================================================
# 저장 / 로드
# =============================================================================

def save_tv_results(signals: list[TVSignal], path: Path = None):
    """TV 스캐너 결과 저장"""
    if path is None:
        path = DATA_DIR / "tv_scanner.json"

    from datetime import datetime
    now = datetime.now()

    data = {
        "scan_date": now.strftime("%Y-%m-%d"),
        "scan_time": now.strftime("%H:%M"),
        "total_signals": len(signals),
        "patterns": {
            "EXPLOSION": sum(1 for s in signals if s.pattern == "EXPLOSION"),
            "QUIET_ACCUMULATION": sum(1 for s in signals if s.pattern == "QUIET_ACCUMULATION"),
            "GRADUAL_BUILDUP": sum(1 for s in signals if s.pattern == "GRADUAL_BUILDUP"),
        },
        "signals": [asdict(s) for s in signals],
    }

    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info(f"[TV Scanner] 저장 완료: {path} ({len(signals)}건)")


def load_tv_results(path: Path = None) -> list[dict]:
    """저장된 TV 스캐너 결과 로드"""
    if path is None:
        path = DATA_DIR / "tv_scanner.json"

    if not path.exists():
        return []

    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data.get("signals", [])
    except Exception as e:
        logger.warning(f"TV 스캐너 결과 로드 실패: {e}")
        return []


# =============================================================================
# CLI
# =============================================================================

def _print_signals(signals: list[TVSignal], top: int = 0, pattern_filter: str = None):
    """CLI 출력"""
    filtered = signals
    if pattern_filter:
        p = pattern_filter.upper()
        filtered = [s for s in signals if p in s.pattern]

    if top > 0:
        filtered = filtered[:top]

    # 패턴별 이모지
    _PATTERN_ICON = {
        "EXPLOSION": "BOOM",
        "QUIET_ACCUMULATION": "SILENT",
        "GRADUAL_BUILDUP": "TREND",
    }

    print(f"\n{'='*78}")
    print(f"  TRADING VALUE SCANNER — 거래대금 폭발 스캐너")
    print(f"  {len(signals)}개 이상신호 감지")
    print(f"{'='*78}")

    for i, s in enumerate(filtered, 1):
        icon = _PATTERN_ICON.get(s.pattern, "")
        bar = "#" * int(min(s.tv_ratio, 8) * 4)
        print(
            f"\n  #{i:2d} [{icon:7s}] {s.name}({s.code}) — {s.sector}"
            f"\n      Score: {s.score:.0f}점 | "
            f"거래대금 {s.trading_value:,.0f}억 (20일평균 {s.tv_avg20:,.0f}억)"
            f"\n      TV비율: x{s.tv_ratio:.2f} {bar} | "
            f"등락: {s.change_pct:+.1f}% | "
            f"5일추세: {s.tv_ratio_5d_trend:+.3f}"
            f"\n      현재가: {s.close:,}원 | {s.detail}"
        )

    print(f"\n{'='*78}")
    print(f"  패턴 분포:")
    for p in ["QUIET_ACCUMULATION", "EXPLOSION", "GRADUAL_BUILDUP"]:
        cnt = sum(1 for s in signals if s.pattern == p)
        if cnt > 0:
            print(f"    {p}: {cnt}개")
    print(f"{'='*78}\n")


def _print_single(code: str, universe: dict):
    """단일 종목 상세 분석"""
    info = universe.get(code, {})
    name = info.get("name", code)

    daily = _load_daily(code)
    if daily is None:
        print(f"  {name}({code}): 데이터 없음")
        return

    closes = daily["종가"]
    volumes = daily["거래량"]
    tv_series = closes * volumes / _BILLION

    print(f"\n{'='*65}")
    print(f"  {name}({code}) — 거래대금 상세 분석")
    print(f"{'='*65}")

    # 최근 20일 추이
    print(f"\n  최근 20일 거래대금 추이:")
    recent = daily.tail(20)
    for idx, row in recent.iterrows():
        date_str = str(idx)[:10]
        tv = float(row["종가"]) * float(row["거래량"]) / _BILLION
        avg_20 = float(tv_series.loc[:idx].iloc[-21:-1].mean()) if len(tv_series.loc[:idx]) > 21 else tv
        ratio = tv / avg_20 if avg_20 > 0 else 1.0
        bar = "#" * int(min(ratio, 6) * 5)
        flag = " <<<" if ratio >= 3.0 else ("  **" if ratio >= 2.0 else ("   *" if ratio >= 1.5 else ""))
        print(f"    {date_str} | {row['종가']:>8,} | {tv:>7,.0f}억 | x{ratio:.2f} {bar}{flag}")

    # 메트릭
    metrics = _calc_tv_metrics(code)
    if metrics:
        pattern = _classify_pattern(metrics["tv_ratio"], metrics["change_pct"], metrics["tv_ratio_5d_trend"])
        score = _score_signal(
            metrics["tv_ratio"], metrics["change_pct"],
            metrics["tv_ratio_5d_trend"], metrics["trading_value"],
            metrics["close"], metrics["high"],
        )
        print(f"\n  최종: tv_ratio=x{metrics['tv_ratio']:.2f} | "
              f"5일추세={metrics['tv_ratio_5d_trend']:+.3f} | "
              f"패턴={pattern} | 점수={score:.0f}")

    print(f"{'='*65}\n")


if __name__ == "__main__":
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )

    parser = argparse.ArgumentParser(description="거래대금 폭발 스캐너")
    parser.add_argument("--top", type=int, default=0, help="상위 N개만 출력")
    parser.add_argument("--code", type=str, default=None, help="단일 종목 상세 분석")
    parser.add_argument("--pattern", type=str, default=None, help="패턴 필터 (QUIET/EXPLOSION/GRADUAL)")
    parser.add_argument("--min-tv", type=float, default=10.0, help="최소 거래대금 (억원)")
    args = parser.parse_args()

    # universe 로드
    uni_path = DATA_DIR / "universe.json"
    with open(uni_path, "r", encoding="utf-8") as f:
        universe = json.load(f)

    if args.code:
        _print_single(args.code, universe)
    else:
        signals = scan_trading_value(universe, min_tv_billion=args.min_tv)
        save_tv_results(signals)
        _print_signals(signals, top=args.top, pattern_filter=args.pattern)
