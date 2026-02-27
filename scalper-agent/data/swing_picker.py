# -*- coding: utf-8 -*-
"""
스윙 종목 선정기 — 7팩터 멀티스코어링 + TOP 5
===============================================
매일 16:30 실행 → 내일 진입 후보 TOP 5 확정
→ swing_candidates.json 저장 → 다음날 아침 스캔 대상

7팩터:
  ① 수급 (25%) — 외국인+기관 동반 순매수, 연속일
  ② 기술 (20%) — MA정배열 + RSI + 거래량 등
  ③ 괴리율 (20%) — 기관 매집원가 대비 현재가 위치
  ④ 모멘텀 (10%) — STO+볼린저+등락률
  ⑤ 이벤트 (10%) — 해외캘린더 + DART 공시 가중치
  ⑥ 변동성 (10%) — ATR% 적정 범위 (2~5% 선호)
  ⑦ 밸류 (5%) — PER/PBR 저평가 가산

필터:
  - 추세필터: MA20 위 + MA20 상승 중
  - 과열필터: 10일 수익률 >25% 제외
  - 전일급등필터: 전일 등락률 >10% 제외

사용법:
  python -m data.swing_picker                   # TOP 5 선정
  python -m data.swing_picker --top 3           # TOP 3
  python -m data.swing_picker --telegram        # 텔레그램 전송
"""

import json
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data_store"
DAILY_DIR = DATA_DIR / "daily"
FLOW_DIR = DATA_DIR / "flow"
CANDIDATES_PATH = DATA_DIR / "swing_candidates.json"


# ═══════════════════════════════════════════════════
#  가중치 설정
# ═══════════════════════════════════════════════════

WEIGHTS = {
    "supply": 0.25,     # 수급
    "tech": 0.20,       # 기술 등급
    "inst_gap": 0.20,   # 기관 매집원가 괴리율 (NEW)
    "momentum": 0.10,   # 모멘텀
    "event": 0.10,      # 이벤트
    "volatility": 0.10, # 변동성
    "value": 0.05,      # 밸류
}

TOP_N = 5
MAX_SAME_SECTOR = 2


# ═══════════════════════════════════════════════════
#  1. 수급 점수 (30%)
# ═══════════════════════════════════════════════════

def score_supply(code: str) -> float:
    """외국인+기관 순매수 기반 수급 점수 (0~100)"""
    inv_path = FLOW_DIR / f"{code}_investor.csv"
    if not inv_path.exists():
        return 0.0

    try:
        df = pd.read_csv(inv_path, index_col=0, parse_dates=True)
        if len(df) < 5:
            return 0.0

        recent = df.tail(10)

        # 외국인 순매수 금액 (최근 10일)
        foreign_col = [c for c in df.columns if "외국인" in c and "금액" in c]
        inst_col = [c for c in df.columns if "기관" in c and "금액" in c]

        score = 0.0

        if foreign_col:
            f_vals = recent[foreign_col[0]].values
            # 연속 순매수일 (양수)
            streak = 0
            for v in reversed(f_vals):
                if v > 0:
                    streak += 1
                else:
                    break
            score += min(streak * 8, 40)  # 5일 연속 → 40점

            # 최근 5일 총 순매수 금액
            f_sum = f_vals[-5:].sum()
            if f_sum > 50_000_000_000:  # 500억+
                score += 20
            elif f_sum > 10_000_000_000:  # 100억+
                score += 10

        if inst_col:
            i_vals = recent[inst_col[0]].values
            streak = 0
            for v in reversed(i_vals):
                if v > 0:
                    streak += 1
                else:
                    break
            score += min(streak * 6, 30)  # 5일 연속 → 30점

        # 외인+기관 동반 순매수 보너스
        if foreign_col and inst_col:
            f_last = recent[foreign_col[0]].iloc[-1]
            i_last = recent[inst_col[0]].iloc[-1]
            if f_last > 0 and i_last > 0:
                score += 10  # 동반 순매수 보너스

        return min(score, 100.0)

    except Exception as e:
        logger.debug(f"수급 점수 실패 {code}: {e}")
        return 0.0


# ═══════════════════════════════════════════════════
#  2. 기술 등급 점수 (25%)
# ═══════════════════════════════════════════════════

def score_tech(code: str) -> float:
    """6D 스캔 등급 기반 점수 (0~100)

    supply_analyzer 없이 일봉 데이터로 직접 계산
    """
    path = DAILY_DIR / f"{code}.csv"
    if not path.exists():
        return 0.0

    try:
        df = pd.read_csv(path, index_col=0, parse_dates=True)
        if len(df) < 60:
            return 0.0

        close = df["종가"].values.astype(float)
        volume = df["거래량"].values.astype(float)

        score = 0.0

        # MA 정배열 (5 > 20 > 60)
        ma5 = close[-5:].mean()
        ma20 = close[-20:].mean()
        ma60 = close[-60:].mean()

        if ma5 > ma20 > ma60:
            score += 35  # 정배열
        elif ma5 > ma20:
            score += 20  # 단기 상승
        elif close[-1] > ma20:
            score += 10  # 20일선 위

        # 종가 위치 (20일 고가 대비)
        high_20 = close[-20:].max()
        low_20 = close[-20:].min()
        if high_20 != low_20:
            pos = (close[-1] - low_20) / (high_20 - low_20)
            score += pos * 25  # 고가 근처 → 25점

        # 거래량 증가 (최근 5일 vs 20일)
        avg_vol_5 = volume[-5:].mean()
        avg_vol_20 = volume[-20:].mean()
        if avg_vol_20 > 0:
            vol_ratio = avg_vol_5 / avg_vol_20
            if vol_ratio >= 2.0:
                score += 20
            elif vol_ratio >= 1.5:
                score += 15
            elif vol_ratio >= 1.2:
                score += 10

        # RSI (14일)
        deltas = np.diff(close[-15:])
        gains = np.maximum(deltas, 0).mean()
        losses = np.abs(np.minimum(deltas, 0)).mean()
        if losses > 0:
            rs = gains / losses
            rsi = 100 - (100 / (1 + rs))
        else:
            rsi = 100

        if 40 <= rsi <= 70:  # 골디락스 구간
            score += 20
        elif 30 <= rsi < 40:  # 과매도 반등 가능
            score += 15

        return min(score, 100.0)

    except Exception as e:
        logger.debug(f"기술 점수 실패 {code}: {e}")
        return 0.0


# ═══════════════════════════════════════════════════
#  3. 모멘텀 점수 (15%)
# ═══════════════════════════════════════════════════

def score_momentum(code: str) -> float:
    """STO + 볼린저 + 등락률 기반 모멘텀 점수 (0~100)

    스윙 D+1~2 관점: 상승 지속 모멘텀 + 반등 시작 모두 포착
    """
    path = DAILY_DIR / f"{code}.csv"
    if not path.exists():
        return 0.0

    try:
        df = pd.read_csv(path, index_col=0, parse_dates=True)
        if len(df) < 30:
            return 0.0

        close = df["종가"].values.astype(float)
        high = df["고가"].values.astype(float)
        low = df["저가"].values.astype(float)

        score = 0.0

        # STO (14, 3, 3)
        period = 14
        h14 = pd.Series(high).rolling(period).max().values
        l14 = pd.Series(low).rolling(period).min().values
        denom = h14 - l14
        denom[denom == 0] = 1
        raw_k = (close - l14) / denom * 100
        k = pd.Series(raw_k).rolling(3).mean().values
        d = pd.Series(k).rolling(3).mean().values

        if not np.isnan(k[-1]) and not np.isnan(d[-1]):
            sto_k = k[-1]

            # 골든크로스 — 최근 3일 이내 발생 (기존 당일만 → 확대)
            gc_found = False
            for i in range(-1, -4, -1):
                if abs(i) < len(k) and not np.isnan(k[i-1]) and not np.isnan(d[i-1]):
                    if k[i] > d[i] and k[i-1] <= d[i-1]:
                        gc_found = True
                        break
            if gc_found:
                score += 25

            # K > D 상승 추세 (골든크로스 없어도 부분 점수)
            if k[-1] > d[-1]:
                score += 10
                if k[-1] > k[-2]:  # K 상승중
                    score += 5

            # STO 영역 점수
            if 20 < sto_k < 80:    # 건강한 영역
                score += 10
            elif sto_k <= 20:       # 과매도 반등 시작
                score += 5

        # 볼린저 밴드 %
        ma20 = pd.Series(close).rolling(20).mean().values
        std20 = pd.Series(close).rolling(20).std().values
        if not np.isnan(std20[-1]) and std20[-1] > 0:
            upper = ma20[-1] + 2 * std20[-1]
            lower = ma20[-1] - 2 * std20[-1]
            bb_pct = (close[-1] - lower) / (upper - lower) * 100

            if 40 <= bb_pct <= 80:   # 중상단 (모멘텀 구간)
                score += 25
            elif 20 <= bb_pct < 40:  # 중단 (안정적)
                score += 15
            elif bb_pct < 20:        # 극 하단 (반등 기대)
                score += 10

        # 최근 3일 등락률
        if len(close) >= 4:
            ret_3d = (close[-1] / close[-4] - 1) * 100
            if 0 < ret_3d <= 8:     # 상승 모멘텀
                score += 20
            elif -2 <= ret_3d <= 0:  # 소폭 조정 (진입 기회)
                score += 10

        return min(score, 100.0)

    except Exception as e:
        logger.debug(f"모멘텀 점수 실패 {code}: {e}")
        return 0.0


# ═══════════════════════════════════════════════════
#  4. 이벤트 점수 (15%)
# ═══════════════════════════════════════════════════

def score_event(code: str) -> float:
    """해외 캘린더 + DART 이벤트 기반 점수 (0~100)"""
    score = 0.0

    # 해외 이벤트 보너스
    try:
        from data.global_event_calendar import get_event_bonus
        bonus = get_event_bonus(code)
        score += min(bonus, 60)
    except ImportError:
        pass

    # DART 공시 이벤트
    events_path = DATA_DIR / "events.json"
    if events_path.exists():
        try:
            with open(events_path, "r", encoding="utf-8") as f:
                data = json.load(f)

            for b in data.get("beneficiaries", []):
                if b["ticker"] == code and b["direction"] == "POSITIVE":
                    score += min(b["total_score"], 40)
                    break
        except (json.JSONDecodeError, IOError):
            pass

    return min(score, 100.0)


# ═══════════════════════════════════════════════════
#  5. 변동성 점수 (10%)
# ═══════════════════════════════════════════════════

def score_volatility(code: str) -> float:
    """ATR% 기반 변동성 점수 — 적정 변동성(2~5%) 선호"""
    path = DAILY_DIR / f"{code}.csv"
    if not path.exists():
        return 0.0

    try:
        df = pd.read_csv(path, index_col=0, parse_dates=True)
        if len(df) < 20:
            return 0.0

        close = df["종가"].values.astype(float)
        high = df["고가"].values.astype(float)
        low = df["저가"].values.astype(float)

        # ATR (14일)
        tr = np.maximum(
            high[-14:] - low[-14:],
            np.maximum(
                np.abs(high[-14:] - np.append(close[-15], close[-14:-1])),
                np.abs(low[-14:] - np.append(close[-15], close[-14:-1])),
            ),
        )
        atr = tr.mean()
        atr_pct = (atr / close[-1]) * 100 if close[-1] > 0 else 0

        # 적정 변동성 2~5% → 최고점, 범위 밖 → 감점
        if 2.0 <= atr_pct <= 5.0:
            return 100.0
        elif 1.5 <= atr_pct < 2.0 or 5.0 < atr_pct <= 7.0:
            return 70.0
        elif 1.0 <= atr_pct < 1.5 or 7.0 < atr_pct <= 10.0:
            return 40.0
        else:
            return 10.0  # 너무 낮거나 너무 높음

    except Exception as e:
        logger.debug(f"변동성 점수 실패 {code}: {e}")
        return 0.0


# ═══════════════════════════════════════════════════
#  6. 밸류 점수 (5%)
# ═══════════════════════════════════════════════════

def score_value(code: str) -> float:
    """PER/PBR 저평가 점수 (0~100)

    universe.json에 PER/PBR 정보가 있으면 사용
    """
    uni_path = DATA_DIR / "universe.json"
    if not uni_path.exists():
        return 50.0  # 정보 없으면 중립

    try:
        with open(uni_path, "r", encoding="utf-8") as f:
            uni = json.load(f)

        info = uni.get(code, {})
        per = info.get("per", 0)
        pbr = info.get("pbr", 0)

        score = 50.0  # 기본 중립

        if per > 0:
            if per < 10:
                score += 25  # 저PER
            elif per < 15:
                score += 15
            elif per > 50:
                score -= 20  # 고PER 감점

        if pbr > 0:
            if pbr < 1.0:
                score += 25  # 저PBR
            elif pbr < 2.0:
                score += 10
            elif pbr > 5.0:
                score -= 10

        return max(0, min(score, 100.0))

    except (json.JSONDecodeError, IOError):
        return 50.0


# ═══════════════════════════════════════════════════
#  7. 기관 매집원가 괴리율 (20%)
# ═══════════════════════════════════════════════════

def score_inst_gap(code: str, days: int = 20) -> float:
    """기관+외인 매집원가 vs 현재가 괴리율 점수 (0~100)

    현재가 < 매집원가 + 아직 매수중 → 최고점
    현재가 ≈ 매집원가 + 매수 가속 → 높은 점수
    현재가 > 매집원가 크게 이탈 → 차익실현 경고
    """
    daily_path = DAILY_DIR / f"{code}.csv"
    flow_path = FLOW_DIR / f"{code}_investor.csv"

    if not daily_path.exists() or not flow_path.exists():
        return 30.0  # 데이터 없으면 중립 이하

    try:
        dd = pd.read_csv(daily_path, index_col=0, parse_dates=True).sort_index().tail(days)
        ff = pd.read_csv(flow_path, index_col=0, parse_dates=True).sort_index().tail(days)

        if len(dd) < 10 or len(ff) < 10:
            return 30.0

        current = float(dd["종가"].values[-1])

        # 공통 날짜
        common = dd.index.intersection(ff.index)
        if len(common) < 5:
            return 30.0

        dd_c = dd.loc[common]
        ff_c = ff.loc[common]

        vwap = ((dd_c["고가"] + dd_c["저가"] + dd_c["종가"]) / 3).values
        f_qty = ff_c["외국인_수량"].values.astype(float)
        i_qty = ff_c["기관_수량"].values.astype(float)
        combo = f_qty + i_qty

        # 순매수일만으로 매집원가 계산
        buy_mask = combo > 0
        if buy_mask.sum() < 3:
            return 30.0

        cost = float((vwap[buy_mask] * combo[buy_mask]).sum() / combo[buy_mask].sum())
        gap_pct = (current / cost - 1) * 100  # 음수 = 현재가 아래 (기회)

        # 최근 5일 순매수 추세
        recent_net = combo[-5:].sum()
        still_buying = recent_net > 0

        score = 0.0

        # 핵심 로직: "오르는데 기관도 따라 사는" = 추세 추종
        # 상승 추세 필터 통과 종목 기준 — 기관 수익률이 높을수록 확신 강함
        if still_buying:
            if 10 <= gap_pct <= 30:
                score = 100  # 적당히 수익 중 + 추가 매수 → 최적 구간
            elif gap_pct > 30:
                score = 75   # 크게 수익 중 + 매수 → 과열 주의
            elif 5 <= gap_pct < 10:
                score = 90   # 초기 수익 + 매수 가속
            elif 0 <= gap_pct < 5:
                score = 80   # 원가 부근 + 매수 → 상승 초입
            elif -5 <= gap_pct < 0:
                score = 60   # 소폭 손실 + 매수 → 확신은 있으나 아직
            else:
                score = 40   # 크게 물려있는데 매수 → 바닥잡기 리스크
        else:
            # 매수 중단 = 기관 이탈 → 감점
            if gap_pct > 10:
                score = 20   # 수익 실현 후 이탈 → 위험
            elif gap_pct > 0:
                score = 25   # 소폭 수익 후 이탈
            else:
                score = 15   # 손실 중 + 이탈 → 최악

        return min(score, 100.0)

    except Exception as e:
        logger.debug(f"기관괴리율 점수 실패 {code}: {e}")
        return 30.0


# ═══════════════════════════════════════════════════
#  통합 스코어링 + TOP N
# ═══════════════════════════════════════════════════

def pick_swing_candidates(
    top_n: int = TOP_N,
    max_same_sector: int = MAX_SAME_SECTOR,
) -> List[Dict]:
    """7팩터 스코어링 → 섹터 분산 → TOP N 선정"""

    # 유니버스 로드
    uni_path = DATA_DIR / "universe.json"
    if not uni_path.exists():
        print("universe.json 없음")
        return []

    with open(uni_path, "r", encoding="utf-8") as f:
        universe = json.load(f)

    # ETF/우선주 제외
    exclude_suffix = {"5", "7", "8", "9", "K", "L"}
    exclude_codes = {"069500", "371160", "102780", "305720"}
    codes = [
        c for c in universe.keys()
        if c not in exclude_codes and c[-1] not in exclude_suffix
    ]

    print(f"  1차 필터: {len(codes)}종목 (우선주/ETF 제외)")

    # 거래대금 필터 (10억+)
    filtered = []
    for code in codes:
        path = DAILY_DIR / f"{code}.csv"
        if not path.exists():
            continue
        try:
            df = pd.read_csv(path, index_col=0, parse_dates=True)
            if len(df) < 60:
                continue
            # 최근 5일 평균 거래대금
            vol = df["거래량"].values.astype(float)
            close = df["종가"].values.astype(float)
            avg_value = (vol[-5:] * close[-5:]).mean()
            if avg_value >= 1_000_000_000:  # 10억+
                filtered.append(code)
        except Exception:
            continue

    print(f"  거래대금 필터: {len(filtered)}종목 (10억+)")

    # 7팩터 스코어링 (추세+과열+전일급등 필터 포함)
    scored = []
    _filtered_trend = 0
    _filtered_overheat = 0
    _filtered_dayjump = 0
    for i, code in enumerate(filtered):
        if (i + 1) % 100 == 0:
            print(f"    스코어링 {i+1}/{len(filtered)}...", flush=True)

        # ── 추세 필터: MA20 위 + 상승 중인 종목만 ──
        # ── 과열 필터: 10일 수익률 >25% 제외 ──
        # ── 전일 급등 필터: 전일 등락률 >10% 제외 ──
        try:
            _df = pd.read_csv(DAILY_DIR / f"{code}.csv", index_col=0, parse_dates=True)
            _c = _df["종가"].values.astype(float)
            if len(_c) < 25:
                continue
            _ma20 = _c[-20:].mean()
            _ma20_prev = _c[-25:-5].mean()  # 5일 전 MA20
            # 현재가 > MA20 이어야 함 (상승 추세)
            if _c[-1] < _ma20:
                _filtered_trend += 1
                continue
            # MA20 자체가 상승 중이어야 함 (추세 방향)
            if _ma20 < _ma20_prev:
                _filtered_trend += 1
                continue
            # 과열 필터: 10일 수익률 >25% → 차익실현 리스크 높음
            if len(_c) >= 11:
                _ret10 = (_c[-1] / _c[-11] - 1) * 100
                if _ret10 > 25:
                    _filtered_overheat += 1
                    continue
            # 전일 급등 필터: 전일 등락률 >10% → 다음날 눌림 확률 높음
            if len(_c) >= 2:
                _ret1 = (_c[-1] / _c[-2] - 1) * 100
                if _ret1 > 10:
                    _filtered_dayjump += 1
                    continue
        except Exception:
            continue

        s_supply = score_supply(code)
        s_tech = score_tech(code)
        s_inst_gap = score_inst_gap(code)
        s_momentum = score_momentum(code)
        s_event = score_event(code)
        s_volatility = score_volatility(code)
        s_value = score_value(code)

        total = (
            s_supply * WEIGHTS["supply"]
            + s_tech * WEIGHTS["tech"]
            + s_inst_gap * WEIGHTS["inst_gap"]
            + s_momentum * WEIGHTS["momentum"]
            + s_event * WEIGHTS["event"]
            + s_volatility * WEIGHTS["volatility"]
            + s_value * WEIGHTS["value"]
        )

        if total < 30:
            continue

        info = universe.get(code, {})
        name = info.get("name", code) if isinstance(info, dict) else info[0] if isinstance(info, tuple) else code
        sector = info.get("sector", "기타") if isinstance(info, dict) else "기타"

        # 진입가/손절가/목표가 계산
        try:
            df = pd.read_csv(DAILY_DIR / f"{code}.csv", index_col=0, parse_dates=True)
            c = df["종가"].values.astype(float)
            h = df["고가"].values.astype(float)
            l = df["저가"].values.astype(float)

            entry = int(c[-1])  # 전일 종가 기준
            # ATR 기반 SL/TP
            tr = np.maximum(h[-14:] - l[-14:], 0)
            atr = tr.mean()
            sl = int(entry - atr * 0.8)
            tp = int(entry + atr * 1.6)  # 2R
        except Exception:
            entry = sl = tp = 0

        scored.append({
            "code": code,
            "name": name,
            "sector": sector,
            "total_score": round(total, 1),
            "scores": {
                "supply": round(s_supply, 1),
                "tech": round(s_tech, 1),
                "inst_gap": round(s_inst_gap, 1),
                "momentum": round(s_momentum, 1),
                "event": round(s_event, 1),
                "volatility": round(s_volatility, 1),
                "value": round(s_value, 1),
            },
            "entry": entry,
            "sl": sl,
            "tp": tp,
        })

    print(f"  추세 필터 제외: {_filtered_trend}종목")
    print(f"  과열 필터 제외: {_filtered_overheat}종목 (10일 >25%)")
    print(f"  전일급등 필터 제외: {_filtered_dayjump}종목 (전일 >10%)")
    print(f"  스코어링 통과: {len(scored)}종목")

    scored.sort(key=lambda x: -x["total_score"])

    # 섹터 분산 필터 ("기타"는 미분류이므로 제한 없음)
    final = []
    sector_count = {}
    for s in scored:
        sec = s["sector"]
        if sec != "기타" and sector_count.get(sec, 0) >= max_same_sector:
            continue
        final.append(s)
        if sec != "기타":
            sector_count[sec] = sector_count.get(sec, 0) + 1
        if len(final) >= top_n:
            break

    return final


def run_picker(top_n: int = TOP_N) -> Dict:
    """전체 파이프라인 실행 + 저장"""
    print("=" * 60)
    print("  스윙 종목 선정기 (7팩터)")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 60)

    candidates = pick_swing_candidates(top_n=top_n)

    result = {
        "picked_at": datetime.now().isoformat(),
        "top_n": top_n,
        "weights": WEIGHTS,
        "candidates": candidates,
    }

    with open(CANDIDATES_PATH, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    print(f"\n  선정 완료: {len(candidates)}종목 → {CANDIDATES_PATH.name}")
    return result


# ═══════════════════════════════════════════════════
#  텔레그램 포맷
# ═══════════════════════════════════════════════════

MEDALS = ["🥇", "🥈", "🥉", "4️⃣", "5️⃣", "6️⃣", "7️⃣", "8️⃣", "9️⃣", "🔟"]


def format_telegram_message(result: Dict) -> str:
    """선정 결과 → 텔레그램 메시지"""
    candidates = result.get("candidates", [])
    if not candidates:
        return "스윙 종목 선정: 후보 없음"

    now = result.get("picked_at", "")[:16]
    lines = [
        "━" * 24,
        f"📋 내일 진입 후보 TOP {len(candidates)}",
        f"🕐 {now}",
        "━" * 24,
    ]

    for i, c in enumerate(candidates):
        medal = MEDALS[i] if i < len(MEDALS) else f"{i+1}."
        sc = c["scores"]

        # 최고 팩터 표시
        top_factor = max(sc.items(), key=lambda x: x[1])
        factor_labels = {
            "supply": "수급", "tech": "기술", "inst_gap": "괴리율",
            "momentum": "모멘텀", "event": "이벤트",
            "volatility": "변동성", "value": "밸류",
        }
        best = factor_labels.get(top_factor[0], top_factor[0])

        lines.append("")
        lines.append(f"{medal} {c['name']}({c['code']})")
        lines.append(
            f"   총점: {c['total_score']}점 | 최강: {best}({top_factor[1]})"
        )
        lines.append(
            f"   수급:{sc['supply']:.0f} 기술:{sc['tech']:.0f} "
            f"괴리:{sc.get('inst_gap',0):.0f} 모멘텀:{sc['momentum']:.0f} "
            f"이벤트:{sc['event']:.0f}"
        )
        if c["entry"] > 0:
            rr = abs(c["tp"] - c["entry"]) / max(abs(c["entry"] - c["sl"]), 1)
            lines.append(
                f"   진입:{c['entry']:,} → SL:{c['sl']:,} / TP:{c['tp']:,} (R:{rr:.1f})"
            )
        lines.append(f"   섹터: {c['sector']}")

    lines.append("")
    lines.append("━" * 24)
    lines.append("⚠️ 아침 스캔에서 STRONG_BUY/BUY만 실제 진입")
    lines.append("⚠️ SL 도달시 즉시 손절 | D+2 미도달시 시간청산")
    lines.append("Prophet 예언자 | 7팩터 스윙")

    return "\n".join(lines)


# ═══════════════════════════════════════════════════
#  로드 유틸
# ═══════════════════════════════════════════════════

def load_candidates() -> Optional[Dict]:
    """저장된 swing_candidates.json 로드"""
    if not CANDIDATES_PATH.exists():
        return None
    try:
        with open(CANDIDATES_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError):
        return None


def get_candidate_codes() -> List[str]:
    """오늘 진입 후보 종목코드 리스트"""
    data = load_candidates()
    if not data:
        return []
    return [c["code"] for c in data.get("candidates", [])]


# ═══════════════════════════════════════════════════
#  CLI
# ═══════════════════════════════════════════════════

if __name__ == "__main__":
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
    sys.path.insert(0, str(BASE_DIR))

    from dotenv import load_dotenv
    load_dotenv(BASE_DIR.parent / ".env")

    logging.basicConfig(level=logging.WARNING)

    import argparse
    parser = argparse.ArgumentParser(description="스윙 종목 선정기")
    parser.add_argument("--top", type=int, default=TOP_N, help="TOP N")
    parser.add_argument("--telegram", action="store_true", help="텔레그램 전송")
    args = parser.parse_args()

    result = run_picker(top_n=args.top)

    # 결과 출력
    for i, c in enumerate(result["candidates"], 1):
        sc = c["scores"]
        print(
            f"  {i:2d}. {c['name']:12s}({c['code']}) "
            f"총:{c['total_score']:5.1f} | "
            f"수급:{sc['supply']:3.0f} 기술:{sc['tech']:3.0f} "
            f"모멘텀:{sc['momentum']:3.0f} 이벤트:{sc['event']:3.0f} | "
            f"진입:{c['entry']:>8,} SL:{c['sl']:>8,} TP:{c['tp']:>8,}"
        )

    if args.telegram:
        import requests
        msg = format_telegram_message(result)
        token = os.getenv("TELEGRAM_BOT_TOKEN")
        chat_id = os.getenv("TELEGRAM_CHAT_ID")
        if token and chat_id:
            url = f"https://api.telegram.org/bot{token}/sendMessage"
            resp = requests.post(url, json={"chat_id": chat_id, "text": msg}, timeout=15)
            print(f"\n텔레그램 전송: {'OK' if resp.status_code == 200 else 'FAIL'}")
        else:
            print(msg)
