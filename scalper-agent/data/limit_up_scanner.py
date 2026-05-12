# -*- coding: utf-8 -*-
"""
상한가 스캐너 (Limit-Up Scanner)
================================
"1번 상한가 간 종목이 다음번에 최소 10%~또다시 상한가 근방까지 간 경우가 많아"

3가지 핵심 기능:
  1. 상한가 감지  — 당일 30% 상한가 마감 종목 탐색
  2. 연속성 분석  — 상한가 후 눌림목 → 재상승 패턴 점수화
  3. 분할매수 플랜 — 기계적 진입/물타기/익절 규칙 생성

추가 패턴:
  - 5% 초기매집 → 순차급등 (대원전선우 타입)
  - 거래대금/시총 회전율 기반 세력 감지
  - 테마 연동 종목 동반 상승 확인

Usage:
  python -m data.limit_up_scanner                  # 전체 스캔
  python -m data.limit_up_scanner --days 30        # 최근 30일 상한가 이력
  python -m data.limit_up_scanner --mode sequential  # 5%→순차급등 패턴 스캔
"""

import json
import logging
import sys
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data_store"
DAILY_DIR = DATA_DIR / "daily"
FLOW_DIR = DATA_DIR / "flow"
UNIVERSE_PATH = DATA_DIR / "universe.json"
LIMIT_UP_DIR = DATA_DIR / "limit_up"
LIMIT_UP_HISTORY = LIMIT_UP_DIR / "history.json"
LIMIT_UP_CANDIDATES = LIMIT_UP_DIR / "candidates.json"

logger = logging.getLogger("BH.LimitUpScanner")

# ── 한국 시장 상한가 기준 ──
LIMIT_UP_PCT = 29.5      # 30% 상한가 (호가 단위 반올림 허용 → 29.5%+)
NEAR_LIMIT_PCT = 25.0    # 상한가 근접 (25%+)
STRONG_SURGE_PCT = 15.0  # 강한 급등 (15%+)

# ── 5% 초기매집 패턴 기준 ──
INITIAL_PUSH_MIN = 4.0   # 초기 밀어올리기 최소 %
INITIAL_PUSH_MAX = 8.0   # 초기 밀어올리기 최대 %

# ── ETF/ETN 필터 ──
_ETF_KEYWORDS = (
    "KODEX", "TIGER", "ACE", "KIWOOM", "SOL ", "HANARO", "KOSEF", "ARIRANG",
    "BNK", "PLUS ", "FOCUS", "TIMEFOLIO", "RISE ", "TIME ", "ITF ", "1Q ",
    "KoAct", "WON ", "UNICORN", "Active", "액티브", "KBSTAR",
    "ETF", "ETN", "인버스", "레버리지",
)


def _is_etf(name: str) -> bool:
    """ETF/ETN 종목 여부"""
    return any(kw in name for kw in _ETF_KEYWORDS)


@dataclass
class LimitUpStock:
    """상한가 종목 데이터"""
    code: str
    name: str
    sector: str = ""
    market: str = ""
    date: str = ""                    # 상한가 발생일
    close: int = 0                    # 종가
    change_pct: float = 0.0           # 등락률
    volume: int = 0                   # 거래량
    volume_ratio: float = 0.0         # 20일 평균 대비 거래량 배수
    trading_value_억: float = 0.0     # 거래대금 (억원)
    market_cap_억: float = 0.0        # 시가총액 (억원)
    turnover_pct: float = 0.0         # 회전율 = 거래대금/시총 %
    close_strength: float = 0.0       # 종가 강도 (1.0=상한가마감, 0=꼬리)
    frgn_net: float = 0.0             # 외국인 순매수 (백만원)
    inst_net: float = 0.0             # 기관 순매수 (백만원)
    consecutive_limit: int = 0        # 연속 상한가 일수
    score: float = 0.0                # 연속성 점수 (0~100)
    pattern: str = ""                 # LIMIT_UP / NEAR_LIMIT / SEQUENTIAL_PUSH
    tags: list = field(default_factory=list)


# ═══════════════════════════════════════════════════════
#  1. 상한가 감지
# ═══════════════════════════════════════════════════════

def _load_daily(code: str) -> Optional[pd.DataFrame]:
    """일봉 CSV 로드"""
    path = DAILY_DIR / f"{code}.csv"
    if not path.exists():
        return None
    try:
        df = pd.read_csv(path, index_col=0, parse_dates=True)
        if len(df) < 5:
            return None
        return df.sort_index()
    except Exception:
        return None


def _load_flow(code: str) -> Optional[pd.DataFrame]:
    """수급 CSV 로드"""
    path = FLOW_DIR / f"{code}_investor.csv"
    if not path.exists():
        return None
    try:
        df = pd.read_csv(path, index_col=0, parse_dates=True)
        return df.sort_index() if len(df) >= 3 else None
    except Exception:
        return None


def _load_foreign_exh(code: str) -> Optional[pd.DataFrame]:
    """외인소진율 CSV 로드"""
    path = FLOW_DIR / f"{code}_foreign_exh.csv"
    if not path.exists():
        return None
    try:
        df = pd.read_csv(path, index_col=0, parse_dates=True)
        return df.sort_index() if len(df) >= 3 else None
    except Exception:
        return None


# ═══════════════════════════════════════════════════════
#  공매도 우회 지표 (Short Proxy Indicators)
#  ─ KRX 공매도 데이터 제공 중단(2026-04~) 대체
#  ─ 거래량 패턴 + 외인소진율 변화 + 기관 급증으로 추정
# ═══════════════════════════════════════════════════════

@dataclass
class ShortProxy:
    """공매도 우회 분석 결과"""
    vol_spike_5d: float = 0.0       # 5일 거래량 / 20일 평균 (숏커버 시 급증)
    vol_spike_1d: float = 0.0       # 당일 거래량 / 20일 평균
    exh_change_5d: float = 0.0      # 외인소진율 5일 변화 (pp)
    exh_latest: float = 0.0         # 최신 외인소진율
    inst_surge: bool = False        # 기관 3일 합이 40일 상위 5%
    inst_3d_sum: float = 0.0        # 기관 최근 3일 순매수 합 (백만원)
    inst_accel: bool = False        # 기관 가속 (최근 2일 > 이전 3일)
    frgn_turning: bool = False      # 외인 순매도→순매수 전환
    short_cover_score: float = 0.0  # 숏커버 추정 점수 (0~30)
    tags: list = field(default_factory=list)


def analyze_short_proxy(code: str) -> ShortProxy:
    """공매도 우회 지표 분석

    3가지 데이터를 결합하여 숏커버링 가능성을 추정:
      1. 거래량 급변 — 갑자기 터지면 숏커버 가능성
      2. 외인소진율 변화 — 소진율 급등 = 외인 매수 (숏커버 포함)
      3. 기관 순매수 급증 — 상위 5% 수준 = 대량 커버링 동반

    Returns:
        ShortProxy with short_cover_score (0~30)
    """
    result = ShortProxy()

    # ── 1. 거래량 패턴 ──
    df = _load_daily(code)
    if df is not None and len(df) >= 20:
        vol_col = "거래량" if "거래량" in df.columns else "volume"
        volumes = df[vol_col].values.astype(float)

        avg_20 = volumes[-20:].mean()
        if avg_20 > 0:
            result.vol_spike_1d = round(volumes[-1] / avg_20, 2)
            result.vol_spike_5d = round(volumes[-5:].mean() / avg_20, 2)

        # 거래량 급증 점수 (0~10)
        if result.vol_spike_1d >= 5.0:
            result.short_cover_score += 10
            result.tags.append(f"거래량{result.vol_spike_1d:.0f}x폭증")
        elif result.vol_spike_1d >= 3.0:
            result.short_cover_score += 7
            result.tags.append(f"거래량{result.vol_spike_1d:.0f}x급증")
        elif result.vol_spike_5d >= 2.0:
            result.short_cover_score += 4
            result.tags.append(f"5일거래량{result.vol_spike_5d:.1f}x")

    # ── 2. 외인소진율 변화 ──
    exh = _load_foreign_exh(code)
    if exh is not None and "소진율" in exh.columns and len(exh) >= 5:
        rates = exh["소진율"].values.astype(float)
        result.exh_latest = round(rates[-1], 2)
        result.exh_change_5d = round(rates[-1] - rates[-5], 3)

        # 소진율 상승 = 외인 매수 증가 (숏커버 포함) (0~10)
        if result.exh_change_5d >= 2.0:
            result.short_cover_score += 10
            result.tags.append(f"소진율+{result.exh_change_5d:.1f}pp급등")
        elif result.exh_change_5d >= 1.0:
            result.short_cover_score += 7
            result.tags.append(f"소진율+{result.exh_change_5d:.1f}pp상승")
        elif result.exh_change_5d >= 0.3:
            result.short_cover_score += 3
            result.tags.append(f"소진율+{result.exh_change_5d:.2f}pp")

    # ── 3. 기관 순매수 급증 ──
    flow = _load_flow(code)
    if flow is not None and len(flow) >= 5:
        inst_cols = [c for c in flow.columns if "기관" in c and "금액" in c]
        frgn_cols = [c for c in flow.columns if "외국인" in c and "금액" in c]

        if inst_cols:
            inst_vals = flow[inst_cols[0]].values.astype(float)
            result.inst_3d_sum = float(inst_vals[-3:].sum())

            # 기관 3일 합이 40일 상위 5%
            if len(inst_vals) >= 40:
                # 3일 합의 롤링 계산
                rolling_3d = np.convolve(inst_vals[-40:], np.ones(3), mode='valid')
                threshold_95 = np.percentile(rolling_3d, 95)
                if result.inst_3d_sum >= threshold_95 and result.inst_3d_sum > 0:
                    result.inst_surge = True

            # 기관 가속: 최근 2일 > 이전 3일
            if len(inst_vals) >= 5:
                recent2 = inst_vals[-2:].mean()
                prev3 = inst_vals[-5:-2].mean()
                if recent2 > prev3 and recent2 > 0:
                    result.inst_accel = True

            # 기관 점수 (0~10)
            if result.inst_surge:
                result.short_cover_score += 10
                result.tags.append("기관대량매수(P95)")
            elif result.inst_accel and result.inst_3d_sum > 0:
                result.short_cover_score += 5
                result.tags.append("기관가속매수")
            elif result.inst_3d_sum > 0:
                result.short_cover_score += 2

        if frgn_cols:
            frgn_vals = flow[frgn_cols[0]].values.astype(float)
            # 외인 전환: 이전 3일 매도 → 최근 2일 매수
            if len(frgn_vals) >= 5:
                if frgn_vals[-2:].sum() > 0 and frgn_vals[-5:-2].sum() < 0:
                    result.frgn_turning = True
                    result.short_cover_score += 3
                    result.tags.append("외인매도→매수전환")

    result.short_cover_score = round(min(30, result.short_cover_score), 1)
    return result


def _calc_change_pct(df: pd.DataFrame, idx: int) -> float:
    """idx 위치의 전일 대비 등락률 계산"""
    if idx < 1 or idx >= len(df):
        return 0.0
    close_col = "종가" if "종가" in df.columns else "close"
    prev = float(df[close_col].iloc[idx - 1])
    curr = float(df[close_col].iloc[idx])
    if prev <= 0:
        return 0.0
    return (curr / prev - 1) * 100


def scan_limit_up(target_date: str = None,
                  universe: dict = None) -> list[LimitUpStock]:
    """당일(또는 지정일) 상한가 종목 탐색

    Returns:
        상한가/근접 종목 리스트 (LimitUpStock)
    """
    if universe is None:
        if UNIVERSE_PATH.exists():
            with open(UNIVERSE_PATH, "r", encoding="utf-8") as f:
                universe = json.load(f)
        else:
            logger.error("universe.json 없음")
            return []

    results = []
    scanned = 0

    for code, info in universe.items():
        name = info.get("name", "")

        # ETF/ETN/우선주 필터
        if _is_etf(name):
            continue

        df = _load_daily(code)
        if df is None:
            continue

        close_col = "종가" if "종가" in df.columns else "close"
        vol_col = "거래량" if "거래량" in df.columns else "volume"
        high_col = "고가" if "고가" in df.columns else "high"
        low_col = "저가" if "저가" in df.columns else "low"

        # target_date 필터
        if target_date:
            td = pd.Timestamp(target_date)
            if df.index[-1].normalize() != td.normalize():
                scanned += 1
                continue

        # 최근 거래일 등락률
        change = _calc_change_pct(df, len(df) - 1)
        if change < STRONG_SURGE_PCT:
            scanned += 1
            continue

        close = float(df[close_col].iloc[-1])
        high = float(df[high_col].iloc[-1])
        low = float(df[low_col].iloc[-1])
        volume = int(df[vol_col].iloc[-1])

        # 거래량 비율
        if len(df) >= 20:
            vol_avg20 = df[vol_col].iloc[-20:].mean()
            vol_ratio = volume / vol_avg20 if vol_avg20 > 0 else 1.0
        else:
            vol_ratio = 1.0

        # 거래대금 (억원)
        tv_억 = close * volume / 1e8

        # 시총 (억원)
        cap_억 = info.get("cap_억", 0)

        # 회전율 = 거래대금 / 시총
        turnover = (tv_억 / cap_억 * 100) if cap_억 > 0 else 0.0

        # 종가 강도: 1.0 = 고가마감(상한가), 0 = 저가마감
        price_range = high - low
        close_strength = (close - low) / price_range if price_range > 0 else 0.5

        # 패턴 분류
        if change >= LIMIT_UP_PCT:
            pattern = "LIMIT_UP"
        elif change >= NEAR_LIMIT_PCT:
            pattern = "NEAR_LIMIT"
        else:
            pattern = "STRONG_SURGE"

        # 수급 데이터
        frgn_net = 0.0
        inst_net = 0.0
        flow = _load_flow(code)
        if flow is not None and len(flow) >= 1:
            frgn_cols = [c for c in flow.columns if "외국인" in c and "금액" in c]
            inst_cols = [c for c in flow.columns if "기관" in c and "금액" in c]
            if frgn_cols:
                frgn_net = float(flow[frgn_cols[0]].iloc[-1])
            if inst_cols:
                inst_net = float(flow[inst_cols[0]].iloc[-1])

        # 연속 상한가 카운트
        consecutive = 0
        for i in range(len(df) - 1, 0, -1):
            c = _calc_change_pct(df, i)
            if c >= LIMIT_UP_PCT:
                consecutive += 1
            else:
                break

        # 태그
        tags = []
        if close_strength >= 0.95:
            tags.append("상한가마감")
        elif close_strength >= 0.8:
            tags.append("강한종가")
        if vol_ratio >= 5.0:
            tags.append("거래량5x+")
        elif vol_ratio >= 3.0:
            tags.append("거래량3x+")
        if turnover >= 10.0:
            tags.append("회전율10%+")
        if frgn_net > 0 and inst_net > 0:
            tags.append("쌍끌이매수")
        elif frgn_net > 0:
            tags.append("외인매수")
        elif inst_net > 0:
            tags.append("기관매수")
        if consecutive >= 2:
            tags.append(f"연속상한{consecutive}일")

        # 공매도 우회 지표 분석
        sp = analyze_short_proxy(code)
        if sp.tags:
            tags.extend(sp.tags)

        stock = LimitUpStock(
            code=code,
            name=name,
            sector=info.get("sector", ""),
            market=info.get("market", ""),
            date=str(df.index[-1].date()),
            close=int(close),
            change_pct=round(change, 2),
            volume=volume,
            volume_ratio=round(vol_ratio, 2),
            trading_value_억=round(tv_억, 1),
            market_cap_억=cap_억,
            turnover_pct=round(turnover, 2),
            close_strength=round(close_strength, 3),
            frgn_net=frgn_net,
            inst_net=inst_net,
            consecutive_limit=consecutive,
            pattern=pattern,
            tags=tags,
        )
        stock._short_proxy = sp  # 내부 참조용
        results.append(stock)
        scanned += 1

        if scanned % 300 == 0:
            logger.info(f"  스캔 진행: {scanned}종목 ({len(results)}건 감지)")

    results.sort(key=lambda x: -x.change_pct)
    logger.info(f"상한가 스캔 완료: {scanned}종목 → {len(results)}건 감지")
    return results


# ═══════════════════════════════════════════════════════
#  2. 5% 초기매집 → 순차급등 패턴 감지
# ═══════════════════════════════════════════════════════

def scan_sequential_push(universe: dict = None,
                         lookback: int = 5) -> list[LimitUpStock]:
    """5% 초기 밀어올리기 → 순차 급등 패턴 감지

    대원전선우 타입: 세력이 먼저 5% 올리고 순차적으로 쏘는 패턴

    기준:
      - 최근 {lookback}일 내 4~8% 상승일 존재
      - 그 이후 거래대금 유지 또는 증가
      - 종가 > 시가 (양봉 지속)
      - 회전율 상승 추세

    Returns:
        순차급등 후보 리스트
    """
    if universe is None:
        if UNIVERSE_PATH.exists():
            with open(UNIVERSE_PATH, "r", encoding="utf-8") as f:
                universe = json.load(f)
        else:
            return []

    results = []

    for code, info in universe.items():
        name = info.get("name", "")
        cap_억 = info.get("cap_억", 0)

        # ETF/ETN 필터
        if _is_etf(name):
            continue

        df = _load_daily(code)
        if df is None or len(df) < lookback + 20:
            continue

        close_col = "종가" if "종가" in df.columns else "close"
        open_col = "시가" if "시가" in df.columns else "open"
        vol_col = "거래량" if "거래량" in df.columns else "volume"
        high_col = "고가" if "고가" in df.columns else "high"
        low_col = "저가" if "저가" in df.columns else "low"

        closes = df[close_col].values.astype(float)
        opens = df[open_col].values.astype(float)
        volumes = df[vol_col].values.astype(float)
        highs = df[high_col].values.astype(float)
        lows = df[low_col].values.astype(float)

        # 최근 lookback일 등락률 계산
        recent_changes = []
        for i in range(len(closes) - lookback, len(closes)):
            if i >= 1 and closes[i - 1] > 0:
                chg = (closes[i] / closes[i - 1] - 1) * 100
                recent_changes.append((i, chg))

        # 4~8% 초기 밀어올리기 일 찾기
        push_day_idx = None
        for idx, chg in recent_changes:
            if INITIAL_PUSH_MIN <= chg <= INITIAL_PUSH_MAX:
                push_day_idx = idx
                break

        if push_day_idx is None:
            continue

        # 조건 1: 밀어올리기 당일 거래량 급증 (1.5x+)
        vol_avg20 = volumes[push_day_idx - 20:push_day_idx].mean()
        push_vol = volumes[push_day_idx]
        if vol_avg20 > 0 and push_vol / vol_avg20 < 1.5:
            continue

        # 조건 2: 이후 종가 > 밀어올리기 당일 종가 유지
        push_close = closes[push_day_idx]
        days_after = len(closes) - push_day_idx - 1
        if days_after >= 1:
            maintained = all(
                closes[j] >= push_close * 0.97
                for j in range(push_day_idx + 1, len(closes))
            )
            if not maintained:
                continue
        else:
            # 오늘이 밀어올리기 당일이면 통과
            pass

        # 조건 3: 이후 거래대금 유지 또는 증가 (위축 아닌지)
        tv_push = closes[push_day_idx] * volumes[push_day_idx]
        tv_after_avg = 0
        if days_after >= 1:
            tv_after = sum(
                closes[j] * volumes[j]
                for j in range(push_day_idx + 1, len(closes))
            )
            tv_after_avg = tv_after / days_after
        else:
            tv_after_avg = tv_push

        if tv_after_avg < tv_push * 0.5:
            continue  # 거래대금 절반 이하로 위축되면 제외

        # 스코어 계산
        score = 0.0
        tags = ["초기매집"]

        # F1: 밀어올리기 강도 (15점)
        push_chg = (closes[push_day_idx] / closes[push_day_idx - 1] - 1) * 100
        f1 = min(15, push_chg * 2.5)
        score += f1

        # F2: 거래량 폭증 (20점)
        vol_ratio = push_vol / vol_avg20 if vol_avg20 > 0 else 1.0
        f2 = min(20, (vol_ratio - 1.0) * 5)
        score += f2

        # F3: 가격 유지도 (20점)
        if days_after >= 1:
            price_maintain = min(
                closes[j] for j in range(push_day_idx + 1, len(closes))
            ) / push_close
            f3 = min(20, max(0, (price_maintain - 0.95) / 0.05 * 20))
        else:
            f3 = 10  # 당일이면 중립
        score += f3

        # F4: 거래대금 유지도 (15점)
        tv_maintain = tv_after_avg / tv_push if tv_push > 0 else 0
        f4 = min(15, max(0, tv_maintain * 15))
        score += f4

        # F5: 회전율 (15점) - 시총 대비 거래대금
        current_tv = closes[-1] * volumes[-1] / 1e8
        turnover = (current_tv / cap_억 * 100) if cap_억 > 0 else 0
        f5 = min(15, turnover * 3)
        score += f5
        if turnover >= 5:
            tags.append(f"회전율{turnover:.1f}%")

        # F6: 수급 (15점)
        flow = _load_flow(code)
        frgn_net = 0.0
        inst_net = 0.0
        if flow is not None and len(flow) >= 3:
            frgn_cols = [c for c in flow.columns if "외국인" in c and "금액" in c]
            inst_cols = [c for c in flow.columns if "기관" in c and "금액" in c]
            if frgn_cols:
                frgn_net = float(flow[frgn_cols[0]].tail(3).sum())
            if inst_cols:
                inst_net = float(flow[inst_cols[0]].tail(3).sum())

        if frgn_net > 0 and inst_net > 0:
            f6 = 15
            tags.append("쌍끌이")
        elif inst_net > 0:
            f6 = 10
            tags.append("기관매수")
        elif frgn_net > 0:
            f6 = 8
            tags.append("외인매수")
        else:
            f6 = 0
        score += f6

        # 이후 추가 상승 여부
        total_gain = (closes[-1] / closes[push_day_idx - 1] - 1) * 100
        if total_gain >= 15:
            tags.append(f"누적+{total_gain:.0f}%")

        # 양봉 연속
        bullish_count = sum(
            1 for j in range(push_day_idx, len(closes))
            if closes[j] > opens[j]
        )
        if bullish_count >= days_after:
            tags.append("연속양봉")

        # 공매도 우회 지표
        sp = analyze_short_proxy(code)
        if sp.tags:
            tags.extend(sp.tags)
        if sp.short_cover_score >= 15:
            score += min(15, sp.short_cover_score / 2)  # 보너스

        # 종가 강도
        close_strength = (
            (closes[-1] - lows[-1]) / (highs[-1] - lows[-1])
            if highs[-1] > lows[-1] else 0.5
        )

        stock = LimitUpStock(
            code=code,
            name=name,
            sector=info.get("sector", ""),
            market=info.get("market", ""),
            date=str(df.index[-1].date()),
            close=int(closes[-1]),
            change_pct=round(total_gain, 2),
            volume=int(volumes[-1]),
            volume_ratio=round(vol_ratio, 2),
            trading_value_억=round(current_tv, 1),
            market_cap_억=cap_억,
            turnover_pct=round(turnover, 2),
            close_strength=round(close_strength, 3),
            frgn_net=frgn_net,
            inst_net=inst_net,
            score=round(score, 1),
            pattern="SEQUENTIAL_PUSH",
            tags=tags,
        )
        results.append(stock)

    results.sort(key=lambda x: -x.score)
    logger.info(f"순차급등 스캔 완료: {len(results)}건 감지")
    return results


# ═══════════════════════════════════════════════════════
#  3. 상한가 후 연속성 분석 (히스토리 기반)
# ═══════════════════════════════════════════════════════

def analyze_post_limit_pattern(code: str,
                               limit_date_idx: int = None) -> dict:
    """상한가 발생 후 N일간 주가 행보 분석

    Returns:
        {
            "code": str,
            "limit_date": str,
            "post_days": [{day, change, cum_change, vol_ratio}, ...],
            "max_gain_after": float,   # 이후 최대 상승률
            "max_drop_after": float,   # 이후 최대 하락률
            "day_to_max": int,         # 최대 상승까지 소요일
            "reached_10pct": bool,     # 이후 10%+ 재상승 여부
            "reached_limit_again": bool, # 재상한가 여부
            "pullback_entry": float,   # 눌림목 진입가 (최저점)
            "continuation_score": float, # 연속성 점수
        }
    """
    df = _load_daily(code)
    if df is None:
        return {}

    close_col = "종가" if "종가" in df.columns else "close"
    vol_col = "거래량" if "거래량" in df.columns else "volume"

    closes = df[close_col].values.astype(float)
    volumes = df[vol_col].values.astype(float)

    # 상한가 일 찾기
    if limit_date_idx is None:
        # 가장 최근 상한가일 자동 탐색
        for i in range(len(closes) - 1, 0, -1):
            chg = (closes[i] / closes[i - 1] - 1) * 100 if closes[i - 1] > 0 else 0
            if chg >= LIMIT_UP_PCT:
                limit_date_idx = i
                break

    if limit_date_idx is None or limit_date_idx >= len(closes) - 1:
        return {}

    limit_close = closes[limit_date_idx]
    vol_avg20 = (
        volumes[max(0, limit_date_idx - 20):limit_date_idx].mean()
        if limit_date_idx >= 20 else volumes[:limit_date_idx].mean()
    )

    # 이후 행보 분석
    post_days = []
    max_gain = 0.0
    max_drop = 0.0
    day_to_max = 0
    reached_10 = False
    reached_limit = False
    min_close = limit_close

    for d in range(1, min(21, len(closes) - limit_date_idx)):
        idx = limit_date_idx + d
        cum_change = (closes[idx] / limit_close - 1) * 100
        day_change = (
            (closes[idx] / closes[idx - 1] - 1) * 100
            if closes[idx - 1] > 0 else 0
        )
        vr = volumes[idx] / vol_avg20 if vol_avg20 > 0 else 1.0

        post_days.append({
            "day": d,
            "change": round(day_change, 2),
            "cum_change": round(cum_change, 2),
            "vol_ratio": round(vr, 2),
        })

        if cum_change > max_gain:
            max_gain = cum_change
            day_to_max = d
        if cum_change < max_drop:
            max_drop = cum_change

        if closes[idx] < min_close:
            min_close = closes[idx]

        if cum_change >= 10.0:
            reached_10 = True
        if day_change >= LIMIT_UP_PCT:
            reached_limit = True

    # 연속성 점수 계산
    cont_score = 0.0
    if reached_limit:
        cont_score += 40
    elif reached_10:
        cont_score += 25
    elif max_gain >= 5:
        cont_score += 10

    # 눌림목 깊이 점수 (적당한 눌림 = 좋음)
    pullback_depth = abs(max_drop)
    if 3 <= pullback_depth <= 10:
        cont_score += 15  # 적절한 눌림
    elif pullback_depth < 3:
        cont_score += 10  # 눌림 거의 없음 (강함)
    elif pullback_depth <= 15:
        cont_score += 5   # 깊은 눌림

    # 회복 속도 점수
    if day_to_max <= 3 and max_gain >= 5:
        cont_score += 20  # 빠른 회복
    elif day_to_max <= 5 and max_gain >= 5:
        cont_score += 10

    # 거래량 유지
    if post_days:
        avg_post_vr = np.mean([d["vol_ratio"] for d in post_days[:5]])
        if avg_post_vr >= 2.0:
            cont_score += 15  # 관심 지속
        elif avg_post_vr >= 1.0:
            cont_score += 8

    pullback_entry = min_close

    return {
        "code": code,
        "limit_date": str(df.index[limit_date_idx].date()),
        "limit_close": int(limit_close),
        "post_days": post_days,
        "max_gain_after": round(max_gain, 2),
        "max_drop_after": round(max_drop, 2),
        "day_to_max": day_to_max,
        "reached_10pct": reached_10,
        "reached_limit_again": reached_limit,
        "pullback_entry": int(pullback_entry),
        "pullback_depth_pct": round((min_close / limit_close - 1) * 100, 2),
        "continuation_score": round(min(100, cont_score), 1),
    }


# ═══════════════════════════════════════════════════════
#  4. 상한가 히스토리 빌드 (최근 N일)
# ═══════════════════════════════════════════════════════

def build_limit_up_history(days: int = 60,
                           universe: dict = None) -> list[dict]:
    """최근 N일간 상한가 종목 이력 구축 + 사후 분석

    각 상한가 이벤트마다 이후 주가 행보(연속성)를 분석하여
    통계적 기대값을 산출한다.
    """
    if universe is None:
        if UNIVERSE_PATH.exists():
            with open(UNIVERSE_PATH, "r", encoding="utf-8") as f:
                universe = json.load(f)
        else:
            return []

    history = []
    scanned = 0

    for code, info in universe.items():
        name = info.get("name", "")
        cap_억 = info.get("cap_억", 0)

        df = _load_daily(code)
        if df is None or len(df) < days + 5:
            continue

        close_col = "종가" if "종가" in df.columns else "close"
        vol_col = "거래량" if "거래량" in df.columns else "volume"
        closes = df[close_col].values.astype(float)

        # 최근 days일 내 상한가 탐색
        start_idx = max(1, len(closes) - days)
        for i in range(start_idx, len(closes)):
            if closes[i - 1] <= 0:
                continue
            chg = (closes[i] / closes[i - 1] - 1) * 100
            if chg < LIMIT_UP_PCT:
                continue

            # 상한가 발견 → 사후 분석
            post = analyze_post_limit_pattern(code, i)
            if not post:
                continue

            event = {
                "code": code,
                "name": name,
                "sector": info.get("sector", ""),
                "cap_억": cap_억,
                "limit_date": str(df.index[i].date()),
                "limit_close": int(closes[i]),
                "change_pct": round(chg, 2),
                **post,
            }
            history.append(event)

        scanned += 1
        if scanned % 200 == 0:
            logger.info(f"  히스토리 빌드: {scanned}종목 ({len(history)}건)")

    # 정렬: 날짜 역순
    history.sort(key=lambda x: x["limit_date"], reverse=True)

    # 통계 요약
    if history:
        total = len(history)
        reached_10 = sum(1 for h in history if h.get("reached_10pct"))
        reached_limit = sum(1 for h in history if h.get("reached_limit_again"))
        avg_max_gain = np.mean([h["max_gain_after"] for h in history])
        avg_max_drop = np.mean([h["max_drop_after"] for h in history])

        summary = {
            "period_days": days,
            "total_limit_up_events": total,
            "reached_10pct_rate": round(reached_10 / total * 100, 1),
            "reached_limit_again_rate": round(reached_limit / total * 100, 1),
            "avg_max_gain_after": round(avg_max_gain, 2),
            "avg_max_drop_after": round(avg_max_drop, 2),
        }
        logger.info(
            f"상한가 히스토리: {total}건 | "
            f"10%+재상승: {summary['reached_10pct_rate']}% | "
            f"재상한가: {summary['reached_limit_again_rate']}% | "
            f"평균최대상승: {avg_max_gain:.1f}% | "
            f"평균최대하락: {avg_max_drop:.1f}%"
        )
    else:
        summary = {}

    # 저장
    LIMIT_UP_DIR.mkdir(parents=True, exist_ok=True)
    output = {
        "built_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "summary": summary,
        "events": history,
    }
    with open(LIMIT_UP_HISTORY, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2, default=str)

    return history


# ═══════════════════════════════════════════════════════
#  5. 분할매수 플랜 생성
# ═══════════════════════════════════════════════════════

def generate_split_buy_plan(code: str,
                            name: str,
                            entry_price: int,
                            portfolio_value: int = 100_000_000,
                            max_position_pct: float = 2.0) -> dict:
    """기계적 분할매수 플랜 생성

    유튜브 전략 기반:
      - 소수 포지션: 총 자산의 1~2%
      - 3단계 분할매수: 1차 진입 40%, 2차 -5% 30%, 3차 -10% 30%
      - 익절: +5~10%
      - 손절: -15% (최종)

    Args:
        code: 종목코드
        name: 종목명
        entry_price: 진입 가격 (상한가 다음날 시초 예상가)
        portfolio_value: 총 포트폴리오 금액
        max_position_pct: 최대 포지션 비중 (%)

    Returns:
        분할매수 플랜 딕셔너리
    """
    total_alloc = int(portfolio_value * max_position_pct / 100)

    # 3단계 분할
    tranche_1_pct = 0.40  # 1차: 진입가
    tranche_2_pct = 0.30  # 2차: -5%
    tranche_3_pct = 0.30  # 3차: -10%

    tranche_1_price = entry_price
    tranche_2_price = int(entry_price * 0.95)  # -5%
    tranche_3_price = int(entry_price * 0.90)  # -10%

    tranche_1_amount = int(total_alloc * tranche_1_pct)
    tranche_2_amount = int(total_alloc * tranche_2_pct)
    tranche_3_amount = int(total_alloc * tranche_3_pct)

    tranche_1_qty = tranche_1_amount // tranche_1_price if tranche_1_price > 0 else 0
    tranche_2_qty = tranche_2_amount // tranche_2_price if tranche_2_price > 0 else 0
    tranche_3_qty = tranche_3_amount // tranche_3_price if tranche_3_price > 0 else 0

    total_qty = tranche_1_qty + tranche_2_qty + tranche_3_qty
    total_cost = (
        tranche_1_qty * tranche_1_price +
        tranche_2_qty * tranche_2_price +
        tranche_3_qty * tranche_3_price
    )
    avg_price = total_cost // total_qty if total_qty > 0 else entry_price

    # 익절/손절 시나리오
    tp_5 = int(avg_price * 1.05)
    tp_10 = int(avg_price * 1.10)
    sl_15 = int(avg_price * 0.85)

    profit_5 = int(total_qty * (tp_5 - avg_price))
    profit_10 = int(total_qty * (tp_10 - avg_price))
    loss_15 = int(total_qty * (sl_15 - avg_price))

    return {
        "code": code,
        "name": name,
        "portfolio_value": portfolio_value,
        "max_position_pct": max_position_pct,
        "total_allocation": total_alloc,
        "tranches": [
            {
                "stage": 1,
                "trigger": "진입 (상한가 다음날)",
                "price": tranche_1_price,
                "amount": tranche_1_amount,
                "qty": tranche_1_qty,
                "pct_of_alloc": tranche_1_pct * 100,
            },
            {
                "stage": 2,
                "trigger": "1차 대비 -5% 하락시",
                "price": tranche_2_price,
                "amount": tranche_2_amount,
                "qty": tranche_2_qty,
                "pct_of_alloc": tranche_2_pct * 100,
            },
            {
                "stage": 3,
                "trigger": "1차 대비 -10% 하락시",
                "price": tranche_3_price,
                "amount": tranche_3_amount,
                "qty": tranche_3_qty,
                "pct_of_alloc": tranche_3_pct * 100,
            },
        ],
        "summary": {
            "total_qty": total_qty,
            "total_cost": total_cost,
            "avg_price": avg_price,
        },
        "exit_rules": {
            "tp_5pct": {"price": tp_5, "profit": profit_5, "action": "절반 익절"},
            "tp_10pct": {"price": tp_10, "profit": profit_10, "action": "전량 익절"},
            "sl_15pct": {"price": sl_15, "loss": loss_15, "action": "전량 손절"},
            "trailing_stop": "-3% (수익 +5% 이상 진입 시 본전 스탑 전환)",
        },
        "risk_reward": {
            "max_loss": loss_15,
            "max_loss_pct_of_portfolio": round(
                abs(loss_15) / portfolio_value * 100, 2
            ),
            "target_profit": profit_10,
            "risk_reward_ratio": round(
                abs(profit_10 / loss_15), 2
            ) if loss_15 != 0 else 0,
        },
    }


# ═══════════════════════════════════════════════════════
#  6. 종합 스코어링 - 연속성기대값
# ═══════════════════════════════════════════════════════

def score_continuation(stock: LimitUpStock,
                       history_stats: dict = None,
                       short_proxy: ShortProxy = None) -> float:
    """상한가 종목의 연속성(재상승) 기대 점수 (0~100)

    9팩터:
      F1. 종가 강도 (15)       — 상한가 마감 = 최강
      F2. 거래량 배수 (15)     — 관심/자금 유입
      F3. 회전율 (15)          — 시총 대비 거래 활발
      F4. 수급 (15)            — 외인/기관 합류
      F5. 연속 상한가 (10)     — 기세
      F6. 시총 규모 (10)       — 소형주 우위
      F7. 종가 vs 고가 (10)    — 매도 압력
      F8. 테마 모멘텀 (10)     — 섹터 전체 흐름
      F9. 숏커버 추정 (15)     — 공매도 우회 지표 (거래량+소진율+기관)
    """
    score = 0.0

    # F1: 종가 강도
    if stock.close_strength >= 0.95:
        score += 15  # 상한가 마감
    elif stock.close_strength >= 0.8:
        score += 10
    elif stock.close_strength >= 0.6:
        score += 5

    # F2: 거래량 배수
    vr = stock.volume_ratio
    if vr >= 10:
        score += 15
    elif vr >= 5:
        score += 12
    elif vr >= 3:
        score += 8
    elif vr >= 2:
        score += 5

    # F3: 회전율
    tr = stock.turnover_pct
    if tr >= 20:
        score += 15
    elif tr >= 10:
        score += 12
    elif tr >= 5:
        score += 8
    elif tr >= 2:
        score += 4

    # F4: 수급
    if stock.frgn_net > 0 and stock.inst_net > 0:
        score += 15  # 쌍끌이
    elif stock.inst_net > 0:
        score += 10  # 기관
    elif stock.frgn_net > 0:
        score += 8   # 외인
    # 둘 다 매도면 0

    # F5: 연속 상한가
    if stock.consecutive_limit >= 3:
        score += 10
    elif stock.consecutive_limit >= 2:
        score += 7
    elif stock.consecutive_limit >= 1:
        score += 3

    # F6: 시총 (소형주가 연속성 높은 경향)
    cap = stock.market_cap_억
    if 500 <= cap <= 3000:
        score += 10  # 소형주 (가장 폭발력)
    elif 3000 < cap <= 10000:
        score += 7   # 중형주
    elif cap < 500:
        score += 5   # 초소형 (유동성 리스크)
    else:
        score += 3   # 대형주 (안정적이나 폭발력 제한)

    # F7: 종가 위치 (종가=고가 → 매도 압력 없음)
    if stock.close_strength >= 0.98:
        score += 10
    elif stock.close_strength >= 0.90:
        score += 7
    elif stock.close_strength >= 0.70:
        score += 4

    # F8: (향후 테마 연동 데이터 연결 시 활용)
    # 현재는 섹터 기반 보너스만
    hot_sectors = {"전기전자", "운송장비", "의약품", "통신", "화학"}
    if stock.sector in hot_sectors:
        score += 5

    # F9: 숏커버 추정 — 공매도 데이터 대체 지표
    # short_cover_score (0~30) → 0~15 스케일링
    if short_proxy is not None and short_proxy.short_cover_score > 0:
        f9 = min(15, short_proxy.short_cover_score / 2)
        score += f9

    return round(min(100, score), 1)


# ═══════════════════════════════════════════════════════
#  7. 메인 실행
# ═══════════════════════════════════════════════════════

def main():
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(message)s",
        datefmt="%H:%M:%S",
    )

    parser = argparse.ArgumentParser(description="상한가 스캐너")
    parser.add_argument("--mode", choices=["limit", "sequential", "history", "all"],
                        default="all", help="스캔 모드")
    parser.add_argument("--days", type=int, default=60, help="히스토리 기간 (일)")
    parser.add_argument("--portfolio", type=int, default=100_000_000,
                        help="포트폴리오 금액 (원)")
    args = parser.parse_args()

    t0 = time.time()

    # 유니버스 로드
    if not UNIVERSE_PATH.exists():
        print("ERROR: universe.json 없음")
        sys.exit(1)

    with open(UNIVERSE_PATH, "r", encoding="utf-8") as f:
        universe = json.load(f)
    print(f"유니버스: {len(universe)}종목")

    # ── 모드별 실행 ──
    if args.mode in ("limit", "all"):
        print("\n" + "=" * 70)
        print("  [1] 상한가/근접 종목 스캔")
        print("=" * 70)

        limit_stocks = scan_limit_up(universe=universe)
        for s in limit_stocks:
            sp = getattr(s, '_short_proxy', None)
            s.score = score_continuation(s, short_proxy=sp)

        limit_stocks.sort(key=lambda x: -x.score)

        for i, s in enumerate(limit_stocks[:20], 1):
            tags_str = " | ".join(s.tags[:4])
            print(
                f"  #{i:>2} [{s.pattern:14s}] {s.name:14s} ({s.code}) "
                f"+{s.change_pct:5.1f}% "
                f"VR:{s.volume_ratio:.1f}x "
                f"회전:{s.turnover_pct:.1f}% "
                f"종가강도:{s.close_strength:.2f} "
                f"SCORE:{s.score:.0f} "
                f"| {tags_str}"
            )

        if limit_stocks:
            # TOP3 분할매수 플랜
            print(f"\n{'─' * 70}")
            print(f"  분할매수 플랜 (포트폴리오: {args.portfolio:,}원)")
            print(f"{'─' * 70}")

            for s in limit_stocks[:3]:
                plan = generate_split_buy_plan(
                    s.code, s.name, s.close, args.portfolio
                )
                print(f"\n  {s.name} ({s.code}) - 연속성{s.score:.0f}점")
                for t in plan["tranches"]:
                    print(
                        f"    {t['stage']}차: {t['price']:,}원 x {t['qty']}주 "
                        f"= {t['amount']:,}원 ({t['trigger']})"
                    )
                rr = plan["risk_reward"]
                print(
                    f"    TP +10%: +{plan['exit_rules']['tp_10pct']['profit']:,}원 | "
                    f"SL -15%: {plan['exit_rules']['sl_15pct']['loss']:,}원 | "
                    f"R:R = {rr['risk_reward_ratio']:.1f}"
                )

    if args.mode in ("sequential", "all"):
        print(f"\n{'=' * 70}")
        print("  [2] 5% 초기매집 → 순차급등 패턴 스캔")
        print("=" * 70)

        seq_stocks = scan_sequential_push(universe=universe)
        for i, s in enumerate(seq_stocks[:15], 1):
            tags_str = " | ".join(s.tags[:4])
            print(
                f"  #{i:>2} {s.name:14s} ({s.code}) "
                f"누적{s.change_pct:+5.1f}% "
                f"VR:{s.volume_ratio:.1f}x "
                f"회전:{s.turnover_pct:.1f}% "
                f"SCORE:{s.score:.0f} "
                f"| {tags_str}"
            )

    if args.mode in ("history", "all"):
        print(f"\n{'=' * 70}")
        print(f"  [3] 상한가 히스토리 분석 (최근 {args.days}일)")
        print("=" * 70)

        events = build_limit_up_history(days=args.days, universe=universe)

        if events:
            # 연속성 통계
            reached_10 = [e for e in events if e.get("reached_10pct")]
            reached_limit = [e for e in events if e.get("reached_limit_again")]

            print(f"\n  총 상한가 이벤트: {len(events)}건")
            print(f"  이후 10%+ 재상승: {len(reached_10)}건 ({len(reached_10)/len(events)*100:.1f}%)")
            print(f"  이후 재상한가:    {len(reached_limit)}건 ({len(reached_limit)/len(events)*100:.1f}%)")

            if events:
                avg_gain = np.mean([e["max_gain_after"] for e in events])
                avg_drop = np.mean([e["max_drop_after"] for e in events])
                avg_day = np.mean([e["day_to_max"] for e in events])
                print(f"  평균 최대상승: +{avg_gain:.1f}%")
                print(f"  평균 최대하락: {avg_drop:.1f}%")
                print(f"  최대상승까지 평균: {avg_day:.1f}일")

            # 연속성 점수 높은 이벤트
            scored = sorted(events, key=lambda x: -x.get("continuation_score", 0))
            print(f"\n  연속성 TOP 10:")
            for i, e in enumerate(scored[:10], 1):
                print(
                    f"    #{i} {e['name']:12s} {e['limit_date']} "
                    f"+{e['change_pct']:.0f}% → "
                    f"최대+{e['max_gain_after']:.1f}% "
                    f"최대-{e['max_drop_after']:.1f}% "
                    f"10%+재상승:{'O' if e['reached_10pct'] else 'X'} "
                    f"연속성:{e['continuation_score']:.0f}"
                )

    # 저장
    LIMIT_UP_DIR.mkdir(parents=True, exist_ok=True)

    if args.mode in ("limit", "all") and limit_stocks:
        candidates = {
            "scanned_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "mode": args.mode,
            "limit_up": [asdict(s) for s in limit_stocks],
        }
        if args.mode == "all" and 'seq_stocks' in dir():
            candidates["sequential_push"] = [asdict(s) for s in seq_stocks]

        with open(LIMIT_UP_CANDIDATES, "w", encoding="utf-8") as f:
            json.dump(candidates, f, ensure_ascii=False, indent=2, default=str)
        print(f"\n  저장: {LIMIT_UP_CANDIDATES}")

    elapsed = int(time.time() - t0)
    m, s = divmod(elapsed, 60)
    print(f"\n  총 소요: {m}분 {s}초")


if __name__ == "__main__":
    main()
