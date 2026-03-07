# -*- coding: utf-8 -*-
"""
시장 수급 건전성 모니터 + CORTEX 체제/충격 분류
================================================
시장 전체의 수급 건강 상태를 진단하여 스윙매매 리스크 게이트 역할.

v1: 3단계 (NORMAL/WARNING/CRITICAL) 단순 분류
v2: CORTEX 통합 — 6단계 체제 + 충격 분류 + 섹터 매핑

체제 (Regime):
  NORMAL             — 평시: 풀사이즈 진입
  CAUTION            — 주의: 70% 사이즈
  SHOCK              — 충격: 신규매수 중단
  PANIC              — 패닉: 30% 방어만
  RECOVERY_EARLY     — 회복 초기: 줍줍 타이밍
  RECOVERY_CONFIRMED — 회복 확인: 80% 공격

충격 유형 (ShockType):
  GEOPOLITICAL — 지정학 (전쟁, 제재)
  RATE         — 금리 (Fed, 한은)
  EARNINGS     — 실적 (어닝 시즌)
  LIQUIDITY    — 유동성 (마진콜, 신용경색)
  COMPOUND     — 복합 (2개 이상)

사용법:
  python -m data.market_health              # 건전성 + 체제 진단
  python -m data.market_health --telegram   # 텔레그램 전송
"""

import sys
import io
import os
import json
import logging
from pathlib import Path
from datetime import datetime, timedelta
from dataclasses import dataclass, field, asdict
from typing import Optional
from enum import Enum

import pandas as pd
import numpy as np

from data.extend_parquet_data import load_daily

logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data_store"
FLOW_DIR = DATA_DIR / "flow"
DAILY_DIR = DATA_DIR / "daily"
REGIME_STATE_PATH = DATA_DIR / "regime_state.json"


# ═══════════════════════════════════════════════════
#  경보 임계값 (기존 유지)
# ═══════════════════════════════════════════════════

THRESHOLDS = {
    "foreign_5d_warning": -5000,
    "foreign_5d_critical": -15000,
    "inst_5d_warning": -3000,
    "inst_5d_critical": -8000,
    "decline_ratio_warning": 0.65,
    "decline_ratio_critical": 0.80,
    "kospi_drop_warning": -3.0,
    "kospi_drop_critical": -5.0,
}


# ═══════════════════════════════════════════════════
#  CORTEX Layer 1: 시장 체제 (Regime)
# ═══════════════════════════════════════════════════

class Regime(Enum):
    NORMAL = "NORMAL"
    CAUTION = "CAUTION"
    SHOCK = "SHOCK"
    PANIC = "PANIC"
    RECOVERY_EARLY = "RECOVERY_EARLY"
    RECOVERY_CONFIRMED = "RECOVERY_CONFIRMED"


# 체제별 봇 행동 규칙 (v2: SL/TP 리스크 파라미터 추가)
# 백테스트 검증 결과:
#   불장(NORMAL) → v1: 타이트 SL/빠른 TP (+18.2%)
#   회복장(RECOVERY) → v2: 넓은 SL/반분할 TP (승률+2.6%p)
REGIME_RULES = {
    Regime.NORMAL: {
        "new_buy": True, "capital_use": 1.0,
        "sl_atr_mult": 0.5, "sl_max_pct": 0.05,   # v1: 타이트
        "tp_atr_mult": 1.6,                         # v1: 빠른 TP
        "partial_tp": False,                         # 전량 청산
    },
    Regime.CAUTION: {
        "new_buy": True, "capital_use": 0.7,
        "sl_atr_mult": 0.6, "sl_max_pct": 0.06,   # 약간 완화
        "tp_atr_mult": 1.8,
        "partial_tp": False,
    },
    Regime.SHOCK: {
        "new_buy": False, "capital_use": 0.5,
        "sl_atr_mult": 0.7, "sl_max_pct": 0.07,   # v2: 넓은 SL
        "tp_atr_mult": 2.0,
        "partial_tp": True,                          # 반분할
    },
    Regime.PANIC: {
        "new_buy": False, "capital_use": 0.3,
        "sl_atr_mult": 0.8, "sl_max_pct": 0.08,   # 가장 넓은 SL
        "tp_atr_mult": 2.0,
        "partial_tp": True,
    },
    Regime.RECOVERY_EARLY: {
        "new_buy": True, "capital_use": 0.5,
        "sl_atr_mult": 0.7, "sl_max_pct": 0.07,   # v2: 줍줍 모드
        "tp_atr_mult": 2.0,
        "partial_tp": True,                          # 반분할 (추세 타기)
    },
    Regime.RECOVERY_CONFIRMED: {
        "new_buy": True, "capital_use": 0.8,
        "sl_atr_mult": 0.6, "sl_max_pct": 0.06,   # 중간
        "tp_atr_mult": 1.8,
        "partial_tp": True,
    },
}

# 체제 전환 임계값
REGIME_THRESHOLDS = {
    "panic_5d_return": -0.07,      # 5일 수익률 -7% → PANIC
    "shock_today_return": -0.03,   # 당일 -3% → SHOCK
    "caution_today_return": -0.02, # 당일 -2% → CAUTION
    "vix_high": 25,                # VIX 25+ → SHOCK 조건
    "vix_normal": 20,              # VIX 20 미만 → 정상 복귀 조건
    "recovery_vol_mult": 1.0,      # 거래량 20일평균 대비 배수
    "consecutive_down_panic": 3,   # 3일 연속 -1.5% → PANIC
    "consecutive_down_return": -0.015,
}


@dataclass
class RegimeState:
    current: str = "NORMAL"
    previous: str = "NORMAL"
    changed_at: str = ""
    kospi_5d_return: float = 0.0
    kospi_today_return: float = 0.0
    vix: float = 0.0
    consecutive_down_days: int = 0
    panic_low: Optional[float] = None


def _load_regime_state() -> RegimeState:
    """저장된 체제 상태 로드"""
    if not REGIME_STATE_PATH.exists():
        return RegimeState()
    try:
        with open(REGIME_STATE_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        return RegimeState(**{k: v for k, v in data.items() if k in RegimeState.__dataclass_fields__})
    except Exception:
        return RegimeState()


def _save_regime_state(state: RegimeState):
    """체제 상태 저장"""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with open(REGIME_STATE_PATH, "w", encoding="utf-8") as f:
        json.dump(asdict(state), f, ensure_ascii=False, indent=2)


def _collect_regime_inputs() -> dict:
    """체제 판단용 데이터 수집 — KODEX 200 parquet + Finnhub VIX"""
    result = {
        "kospi_close": 0, "kospi_prev_close": 0,
        "kospi_5d_ago_close": 0, "kospi_20ma": 0, "kospi_5ma": 0,
        "kospi_volume": 0, "kospi_vol_20avg": 0,
        "vix": 0, "foreign_net_buy": 0,
        "consecutive_down": 0, "is_bullish_candle": False,
    }

    # KODEX 200 (069500) parquet에서 KOSPI 데이터 추출
    kospi_df = load_daily("069500")
    if kospi_df is None:
        kospi_df = load_daily("005930")  # 삼성전자 폴백
    if kospi_df is not None and len(kospi_df) >= 21:
        close_col = "종가" if "종가" in kospi_df.columns else "close"
        open_col = "시가" if "시가" in kospi_df.columns else "open"
        vol_col = "거래량" if "거래량" in kospi_df.columns else "volume"

        closes = kospi_df[close_col].astype(float).values
        result["kospi_close"] = closes[-1]
        result["kospi_prev_close"] = closes[-2]
        result["kospi_5d_ago_close"] = closes[-6] if len(closes) >= 6 else closes[0]
        result["kospi_20ma"] = float(np.mean(closes[-20:]))
        result["kospi_5ma"] = float(np.mean(closes[-5:]))

        if vol_col in kospi_df.columns:
            vols = kospi_df[vol_col].astype(float).values
            result["kospi_volume"] = vols[-1]
            result["kospi_vol_20avg"] = float(np.mean(vols[-20:])) if len(vols) >= 20 else float(np.mean(vols))

        if open_col in kospi_df.columns:
            open_val = float(kospi_df[open_col].iloc[-1])
            result["is_bullish_candle"] = closes[-1] > open_val

        # 연속 하락일 계산
        consec = 0
        for i in range(len(closes) - 1, 0, -1):
            if closes[i] < closes[i-1]:
                consec += 1
            else:
                break
        result["consecutive_down"] = consec

    # VIX — Finnhub API
    try:
        api_key = os.getenv("FINNHUB_API_KEY")
        if api_key:
            import requests
            resp = requests.get(
                "https://finnhub.io/api/v1/quote?symbol=CBOE:VIX",
                headers={"X-Finnhub-Token": api_key}, timeout=5
            )
            if resp.status_code == 200:
                result["vix"] = resp.json().get("c", 0)
    except Exception as e:
        logger.debug(f"VIX 조회 실패: {e}")

    # 외국인 순매수 (최근 1일, flow 합산에서 추정)
    # _collect_flow_summary()는 5일 합산이므로, 여기서는 flow 데이터의 마지막 1일만 추출
    try:
        from data.universe_builder import load_universe
        universe = load_universe()
        if universe:
            foreign_today = 0
            for code in list(universe.keys())[:100]:  # 상위 100종목만 (속도)
                flow_file = FLOW_DIR / f"{code}.csv"
                if not flow_file.exists():
                    continue
                try:
                    df = pd.read_csv(flow_file, index_col=0)
                    if len(df) < 1:
                        continue
                    for col in ["외국인계", "외인순매수", "외국인"]:
                        if col in df.columns:
                            foreign_today += float(df[col].iloc[-1])
                            break
                except Exception:
                    continue
            result["foreign_net_buy"] = foreign_today
    except Exception:
        pass

    return result


def detect_regime() -> RegimeState:
    """6단계 체제 판단 (CORTEX Layer 1)

    전환 규칙:
    - 하향: 스킵 가능 (NORMAL → PANIC 직행 가능)
    - 상향: 한 단계씩만 (안전장치)
    """
    state = _load_regime_state()
    md = _collect_regime_inputs()
    RT = REGIME_THRESHOLDS

    # 기본 지표
    if md["kospi_prev_close"] > 0:
        today_ret = (md["kospi_close"] - md["kospi_prev_close"]) / md["kospi_prev_close"]
    else:
        today_ret = 0

    if md["kospi_5d_ago_close"] > 0:
        five_day_ret = (md["kospi_close"] - md["kospi_5d_ago_close"]) / md["kospi_5d_ago_close"]
    else:
        five_day_ret = 0

    vol_ratio = (md["kospi_volume"] / md["kospi_vol_20avg"]) if md["kospi_vol_20avg"] > 0 else 1.0

    state.kospi_5d_return = five_day_ret
    state.kospi_today_return = today_ret
    state.vix = md["vix"]
    state.consecutive_down_days = md["consecutive_down"]

    current = Regime[state.current]
    new_regime = current  # 기본: 유지

    # ── 하향 전환 (스킵 가능) ──

    # PANIC: 어디서든 즉시
    if (five_day_ret <= RT["panic_5d_return"] or
        (md["consecutive_down"] >= RT["consecutive_down_panic"]
         and today_ret < RT["consecutive_down_return"])):
        new_regime = Regime.PANIC
    # SHOCK
    elif (today_ret <= RT["shock_today_return"] and md["vix"] >= RT["vix_high"]):
        new_regime = Regime.SHOCK
    # CAUTION (NORMAL에서만)
    elif current == Regime.NORMAL:
        if today_ret < RT["caution_today_return"] or md["vix"] > RT["vix_normal"]:
            new_regime = Regime.CAUTION
        else:
            new_regime = Regime.NORMAL
    # ── 상향 전환 (한 단계씩) ──
    elif current == Regime.PANIC:
        if (md["is_bullish_candle"]
            and vol_ratio >= RT["recovery_vol_mult"]
            and state.panic_low is not None
            and md["kospi_close"] > state.panic_low):
            new_regime = Regime.RECOVERY_EARLY
        else:
            new_regime = Regime.PANIC
    elif current == Regime.RECOVERY_EARLY:
        if md["kospi_close"] > md["kospi_5ma"] and md["foreign_net_buy"] > 0:
            new_regime = Regime.RECOVERY_CONFIRMED
        elif today_ret <= RT["shock_today_return"]:
            new_regime = Regime.PANIC  # 다시 빠짐
        else:
            new_regime = Regime.RECOVERY_EARLY
    elif current == Regime.RECOVERY_CONFIRMED:
        if md["kospi_close"] > md["kospi_20ma"] and md["vix"] < RT["vix_normal"]:
            new_regime = Regime.NORMAL
        else:
            new_regime = Regime.RECOVERY_CONFIRMED
    elif current == Regime.SHOCK:
        if today_ret > 0 and md["vix"] < RT["vix_high"]:
            new_regime = Regime.CAUTION
        else:
            new_regime = Regime.SHOCK
    elif current == Regime.CAUTION:
        if today_ret > 0 and md["vix"] < RT["vix_normal"]:
            new_regime = Regime.NORMAL
        else:
            new_regime = Regime.CAUTION

    # 체제 전환 처리
    if new_regime != current:
        state.previous = current.value
        state.current = new_regime.value
        state.changed_at = datetime.now().strftime("%Y-%m-%d %H:%M")
        logger.info(f"체제 전환: {current.value} -> {new_regime.value}")

        if new_regime == Regime.PANIC:
            state.panic_low = md["kospi_close"]
    else:
        state.current = new_regime.value

    # PANIC 중 저점 갱신
    if new_regime == Regime.PANIC:
        if state.panic_low is None or md["kospi_close"] < state.panic_low:
            state.panic_low = md["kospi_close"]

    _save_regime_state(state)
    return state


# ═══════════════════════════════════════════════════
#  CORTEX Layer 2: 충격 분류 (Shock Classifier)
# ═══════════════════════════════════════════════════

class ShockType(Enum):
    NONE = "NONE"
    GEOPOLITICAL = "GEOPOLITICAL"
    RATE = "RATE"
    EARNINGS = "EARNINGS"
    LIQUIDITY = "LIQUIDITY"
    COMPOUND = "COMPOUND"


# 충격 분류 임계값
SHOCK_THRESHOLDS = {
    "gdelt_spike": 2.0,      # GDELT 평소 2배 → 지정학
    "oil_spike": 0.05,       # 유가 5% → 에너지 충격
    "rate_spike_bp": 0.10,   # 금리 10bp → 금리 충격
    "defense_surge": 0.03,   # 방산 +3% → 지정학 수혜 확인
    "refinery_surge": 0.03,  # 정유 +3% → 에너지 충격 확인
}

# 섹터 매핑 (종목코드 기반)
SECTOR_MAP = {
    "방산": ["012450", "079550", "079160", "047810", "103140",
             "003570", "272210", "014970"],  # 한화에어로/현대로템/LIG넥스원/KAI/풍산/SNT다이내믹스/한화시스템/금호에이치티
    "정유": ["096770", "010950", "267250"],  # SK이노/S-Oil/현대오일뱅크(HD현대)
    "반도체": ["005930", "000660", "042700", "234070", "240810"],
    "바이오": ["207940", "068270", "000100", "196170", "950160"],
    "소프트웨어": ["035420", "035720", "012510", "030520"],
    "전력에너지": ["267260", "010120", "298040"],  # HD현대일렉/LS일렉/효성중공업
    "엔터": ["352820", "041510", "035900", "122870"],
    "증권": ["039490", "006800", "016360"],
}

# 충격 유형별 직접 피해/수혜 섹터 (매수 제외 대상)
SHOCK_AFFECTED_SECTORS = {
    "GEOPOLITICAL": ["방산", "정유"],       # 수혜 → 이미 올랐으니 매수 제외
    "RATE":         ["바이오", "소프트웨어"],  # 금리 민감
    "EARNINGS":     [],                      # 개별 문제
    "LIQUIDITY":    [],                      # 전체 하락
    "COMPOUND":     ["방산", "정유"],        # 복합 시 보수적
}


@dataclass
class ShockAnalysis:
    shock_type: str = "NONE"          # ShockType.value
    confidence: float = 0.0           # 0~1
    description: str = ""
    affected_sectors: list = field(default_factory=list)
    opportunity_sectors: list = field(default_factory=list)
    signals: dict = field(default_factory=dict)
    scores: dict = field(default_factory=dict)


def _collect_shock_inputs() -> dict:
    """충격 분류용 데이터 수집 — 우리 데이터 소스에서"""
    result = {
        "gdelt_conflict_index": 0,
        "oil_change_pct": 0,
        "us10y_change_bp": 0,
        "kr3y_change_bp": 0,
        "vix": 0,
        "defense_sector_return": 0,
        "refinery_sector_return": 0,
        "credit_spread_change": 0,
        "margin_call_volume": 0,
        "earnings_surprise_count": 0,
    }

    # 방산 섹터 수익률 — 한화에어로(012450) 5일 수익률
    defense_codes = ["012450", "079550", "047810"]  # 대표 3종목
    defense_returns = []
    for code in defense_codes:
        df = load_daily(code)
        if df is not None and len(df) >= 6:
            close_col = "종가" if "종가" in df.columns else "close"
            if close_col in df.columns:
                closes = df[close_col].astype(float).values
                ret = (closes[-1] - closes[-6]) / closes[-6]
                defense_returns.append(ret)
    if defense_returns:
        result["defense_sector_return"] = float(np.mean(defense_returns))

    # 정유 섹터 수익률 — S-Oil(010950) 등
    refinery_codes = ["096770", "010950"]
    refinery_returns = []
    for code in refinery_codes:
        df = load_daily(code)
        if df is not None and len(df) >= 6:
            close_col = "종가" if "종가" in df.columns else "close"
            if close_col in df.columns:
                closes = df[close_col].astype(float).values
                ret = (closes[-1] - closes[-6]) / closes[-6]
                refinery_returns.append(ret)
    if refinery_returns:
        result["refinery_sector_return"] = float(np.mean(refinery_returns))
        result["oil_change_pct"] = result["refinery_sector_return"]  # 정유 수익률로 유가 대체

    # VIX — Finnhub
    try:
        api_key = os.getenv("FINNHUB_API_KEY")
        if api_key:
            import requests
            resp = requests.get(
                "https://finnhub.io/api/v1/quote?symbol=CBOE:VIX",
                headers={"X-Finnhub-Token": api_key}, timeout=5
            )
            if resp.status_code == 200:
                result["vix"] = resp.json().get("c", 0)
    except Exception:
        pass

    # 금리 — Finnhub (US10Y)
    try:
        api_key = os.getenv("FINNHUB_API_KEY")
        if api_key:
            import requests
            resp = requests.get(
                "https://finnhub.io/api/v1/quote?symbol=US10Y",
                headers={"X-Finnhub-Token": api_key}, timeout=5
            )
            if resp.status_code == 200:
                data = resp.json()
                current = data.get("c", 0)
                prev = data.get("pc", 0)
                if prev > 0:
                    result["us10y_change_bp"] = (current - prev) * 100  # bp
    except Exception:
        pass

    return result


def classify_shock() -> ShockAnalysis:
    """충격 유형 분류 (CORTEX Layer 2)"""
    nd = _collect_shock_inputs()
    ST = SHOCK_THRESHOLDS
    signals = {}
    scores = {
        "GEOPOLITICAL": 0,
        "RATE": 0,
        "EARNINGS": 0,
        "LIQUIDITY": 0,
    }

    # ── 지정학 시그널 ──
    gdelt = nd.get("gdelt_conflict_index", 0)
    oil = nd.get("oil_change_pct", 0)
    defense_ret = nd.get("defense_sector_return", 0)
    refinery_ret = nd.get("refinery_sector_return", 0)

    if gdelt >= ST["gdelt_spike"]:
        scores["GEOPOLITICAL"] += 35
        signals["gdelt_spike"] = gdelt
    if abs(oil) >= ST["oil_spike"]:
        scores["GEOPOLITICAL"] += 25
        signals["oil_change"] = oil
    if defense_ret > ST["defense_surge"]:
        scores["GEOPOLITICAL"] += 20
        signals["defense_surge"] = defense_ret
    if refinery_ret > ST["refinery_surge"]:
        scores["GEOPOLITICAL"] += 10
        signals["refinery_surge"] = refinery_ret

    # ── 금리 시그널 ──
    us10y = abs(nd.get("us10y_change_bp", 0))
    kr3y = abs(nd.get("kr3y_change_bp", 0))

    if us10y >= ST["rate_spike_bp"] * 100:  # bp 단위
        scores["RATE"] += 40
        signals["us10y_spike_bp"] = us10y
    if kr3y >= ST["rate_spike_bp"] * 70:
        scores["RATE"] += 25
        signals["kr3y_spike_bp"] = kr3y

    # ── 실적 시그널 (Phase 2) ──
    earnings = nd.get("earnings_surprise_count", 0)
    if earnings >= 5:
        scores["EARNINGS"] += 50
        signals["earnings_shock_count"] = earnings

    # ── 유동성 시그널 (Phase 2) ──
    credit = nd.get("credit_spread_change", 0)
    margin = nd.get("margin_call_volume", 0)
    if credit > 0.5:
        scores["LIQUIDITY"] += 35
        signals["credit_spread"] = credit
    if margin > 0:
        scores["LIQUIDITY"] += 30
        signals["margin_call"] = margin

    # ── 최종 판정 ──
    max_type = max(scores, key=scores.get)
    max_score = scores[max_type]

    high_scores = [k for k, v in scores.items() if v >= 40]

    if max_score < 30:
        shock_type = "NONE"
        confidence = 0.0
    elif len(high_scores) >= 2:
        shock_type = "COMPOUND"
        confidence = min(max_score / 100, 0.95)
    else:
        shock_type = max_type
        confidence = min(max_score / 100, 0.95)

    # 기회 섹터 결정
    all_sectors = list(SECTOR_MAP.keys())
    affected = SHOCK_AFFECTED_SECTORS.get(shock_type, [])
    opportunity = [s for s in all_sectors if s not in affected]

    description = _build_shock_description(shock_type, signals)

    logger.info(f"충격 분류: {shock_type} (신뢰도: {confidence:.0%})")

    return ShockAnalysis(
        shock_type=shock_type,
        confidence=confidence,
        description=description,
        affected_sectors=affected,
        opportunity_sectors=opportunity,
        signals=signals,
        scores=scores,
    )


def _build_shock_description(shock_type: str, signals: dict) -> str:
    """충격 요약 (텔레그램용)"""
    if shock_type == "NONE":
        return "특이 충격 없음"

    parts = []
    if shock_type == "GEOPOLITICAL":
        parts.append("지정학 충격")
        if "gdelt_spike" in signals:
            parts.append(f"GDELT {signals['gdelt_spike']:.1f}x")
        if "oil_change" in signals:
            parts.append(f"유가 {signals['oil_change']:+.1%}")
        if "defense_surge" in signals:
            parts.append(f"방산 {signals['defense_surge']:+.1%}")
    elif shock_type == "RATE":
        parts.append("금리 충격")
        if "us10y_spike_bp" in signals:
            parts.append(f"US10Y {signals['us10y_spike_bp']:+.0f}bp")
    elif shock_type == "LIQUIDITY":
        parts.append("유동성 충격")
        if "margin_call" in signals:
            parts.append("반대매매 급증")
    elif shock_type == "EARNINGS":
        parts.append("실적 충격")
    elif shock_type == "COMPOUND":
        parts.append("복합 충격 (다중 리스크)")

    return " | ".join(parts)


def get_stock_sector(code: str) -> str:
    """종목코드 → 섹터 매핑"""
    for sector, codes in SECTOR_MAP.items():
        if code in codes:
            return sector
    return ""


# ═══════════════════════════════════════════════════
#  MarketHealthReport (확장)
# ═══════════════════════════════════════════════════

@dataclass
class MarketHealthReport:
    """시장 건전성 진단 결과 (v2: 체제+충격 포함)"""
    timestamp: str = ""
    alert_level: str = "normal"      # 하위호환: normal/warning/critical
    alerts: list = field(default_factory=list)

    # 수급
    foreign_5d_net: float = 0
    inst_5d_net: float = 0

    # 시장 상태
    decline_ratio: float = 0
    kospi_5d_change: float = 0

    # 포지션 조절 (체제 기반)
    position_multiplier: float = 1.0

    # CORTEX v2 필드
    regime: str = "NORMAL"
    regime_previous: str = "NORMAL"
    regime_changed_at: str = ""
    regime_new_buy: bool = True
    shock_type: str = "NONE"
    shock_confidence: float = 0.0
    shock_detail: str = ""
    affected_sectors: list = field(default_factory=list)
    opportunity_sectors: list = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "timestamp": self.timestamp,
            "alert_level": self.alert_level,
            "alerts": self.alerts,
            "foreign_5d_net": self.foreign_5d_net,
            "inst_5d_net": self.inst_5d_net,
            "decline_ratio": self.decline_ratio,
            "kospi_5d_change": self.kospi_5d_change,
            "position_multiplier": self.position_multiplier,
            "regime": self.regime,
            "regime_previous": self.regime_previous,
            "regime_changed_at": self.regime_changed_at,
            "regime_new_buy": self.regime_new_buy,
            "shock_type": self.shock_type,
            "shock_confidence": self.shock_confidence,
            "shock_detail": self.shock_detail,
            "affected_sectors": self.affected_sectors,
            "opportunity_sectors": self.opportunity_sectors,
        }


# ═══════════════════════════════════════════════════
#  데이터 수집 (기존 유지)
# ═══════════════════════════════════════════════════

def _collect_flow_summary() -> dict:
    """유니버스 전체의 외국인/기관 순매수 합산"""
    from data.universe_builder import load_universe

    universe = load_universe()
    if not universe:
        return {"foreign_5d": 0, "inst_5d": 0, "stock_count": 0}

    foreign_total = 0
    inst_total = 0
    count = 0

    for code in universe:
        flow_file = FLOW_DIR / f"{code}.csv"
        if not flow_file.exists():
            continue

        try:
            df = pd.read_csv(flow_file, index_col=0, parse_dates=True)
            if len(df) < 5:
                continue

            recent = df.tail(5)

            for col_name in ["외국인계", "외인순매수", "외국인"]:
                if col_name in recent.columns:
                    foreign_total += recent[col_name].sum()
                    break

            for col_name in ["기관계", "기관순매수", "기관"]:
                if col_name in recent.columns:
                    inst_total += recent[col_name].sum()
                    break

            count += 1

        except Exception:
            continue

    logger.info(f"수급 합산: {count}종목 | 외인: {foreign_total:,.0f} | 기관: {inst_total:,.0f}")

    return {
        "foreign_5d": foreign_total,
        "inst_5d": inst_total,
        "stock_count": count,
    }


def _collect_market_breadth() -> dict:
    """시장 폭 (하락 종목 비율, KOSPI 변동률)"""
    from data.universe_builder import load_universe

    universe = load_universe()
    if not universe:
        return {"decline_ratio": 0, "kospi_5d_change": 0, "up_count": 0, "down_count": 0, "total": 0}

    up_count = 0
    down_count = 0
    total = 0

    for code in universe:
        df = load_daily(code)
        if df is None:
            continue

        try:
            if len(df) < 6:
                continue

            close_col = "종가" if "종가" in df.columns else "close"
            if close_col not in df.columns:
                continue

            close_5d_ago = df[close_col].iloc[-6]
            close_now = df[close_col].iloc[-1]

            if close_5d_ago > 0:
                change = (close_now - close_5d_ago) / close_5d_ago * 100
                if change < 0:
                    down_count += 1
                else:
                    up_count += 1
                total += 1

        except Exception:
            continue

    decline_ratio = down_count / max(total, 1)

    kospi_change = 0
    kospi_df = load_daily("069500")
    if kospi_df is None:
        kospi_df = load_daily("005930")
    if kospi_df is not None:
        try:
            close_col = "종가" if "종가" in kospi_df.columns else "close"
            if len(kospi_df) >= 6 and close_col in kospi_df.columns:
                kospi_change = (kospi_df[close_col].iloc[-1] / kospi_df[close_col].iloc[-6] - 1) * 100
        except Exception:
            pass

    return {
        "decline_ratio": round(decline_ratio, 3),
        "up_count": up_count,
        "down_count": down_count,
        "total": total,
        "kospi_5d_change": round(kospi_change, 2),
    }


# ═══════════════════════════════════════════════════
#  위기 모드 (수동 오버라이드, 기존 유지)
# ═══════════════════════════════════════════════════

CRISIS_PATH = DATA_DIR / "crisis_mode.json"


def set_crisis_mode(reason: str = "수동 위기 모드 활성화") -> dict:
    state = {
        "active": True,
        "reason": reason,
        "activated_at": datetime.now().isoformat(),
        "activated_by": "manual",
    }
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with open(CRISIS_PATH, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)
    logger.warning(f"위기 모드 활성화: {reason}")
    return state


def clear_crisis_mode() -> bool:
    if CRISIS_PATH.exists():
        CRISIS_PATH.unlink()
        logger.info("위기 모드 해제")
        return True
    return False


def is_crisis_mode() -> tuple[bool, str]:
    if not CRISIS_PATH.exists():
        return False, ""
    try:
        with open(CRISIS_PATH, "r", encoding="utf-8") as f:
            state = json.load(f)
        if state.get("active"):
            return True, state.get("reason", "위기 모드")
    except Exception:
        pass
    return False, ""


def _check_global_risk() -> list[dict]:
    """장전 해외시장 리스크 체크 (VIX)"""
    alerts = []
    try:
        api_key = os.getenv("FINNHUB_API_KEY")
        if not api_key:
            return alerts

        import requests
        headers = {"X-Finnhub-Token": api_key}

        symbols = {
            "CBOE:VIX": {"name": "VIX", "warn": 30, "crit": 40},
        }

        for symbol, cfg in symbols.items():
            try:
                resp = requests.get(
                    f"https://finnhub.io/api/v1/quote?symbol={symbol}",
                    headers=headers, timeout=5
                )
                if resp.status_code == 200:
                    data = resp.json()
                    price = data.get("c", 0)
                    if price > 0:
                        if price >= cfg["crit"]:
                            alerts.append({
                                "level": "critical", "type": "global",
                                "message": f"{cfg['name']} {price:.1f} — 극단 공포"
                            })
                        elif price >= cfg["warn"]:
                            alerts.append({
                                "level": "warning", "type": "global",
                                "message": f"{cfg['name']} {price:.1f} — 공포 구간"
                            })
            except Exception:
                continue

    except Exception as e:
        logger.warning(f"글로벌 리스크 체크 실패: {e}")

    return alerts


# ═══════════════════════════════════════════════════
#  통합 진단 (diagnose)
# ═══════════════════════════════════════════════════

def diagnose() -> MarketHealthReport:
    """시장 건전성 + CORTEX 체제/충격 통합 진단"""
    report = MarketHealthReport(timestamp=datetime.now().isoformat())
    T = THRESHOLDS

    print(f"\n  시장 건전성 + CORTEX 체제 진단")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 50)

    # 0. 위기 모드 체크 (최우선)
    crisis_active, crisis_reason = is_crisis_mode()
    if crisis_active:
        report.alert_level = "critical"
        report.position_multiplier = 0.0
        report.regime = "PANIC"
        report.regime_new_buy = False
        report.alerts = [{"level": "critical", "type": "crisis",
                          "message": f"위기 모드: {crisis_reason}"}]
        print(f"\n  위기 모드 활성 -> {crisis_reason}")
        _save_health(report)
        return report

    # 1. 수급 데이터
    print("\n[1] 투자자 수급 합산...")
    flow = _collect_flow_summary()
    report.foreign_5d_net = flow["foreign_5d"]
    report.inst_5d_net = flow["inst_5d"]
    print(f"  외국인 5일: {report.foreign_5d_net:+,.0f}")
    print(f"  기관 5일: {report.inst_5d_net:+,.0f}")

    # 2. 시장 폭
    print("\n[2] 시장 폭 분석...")
    breadth = _collect_market_breadth()
    report.decline_ratio = breadth["decline_ratio"]
    report.kospi_5d_change = breadth["kospi_5d_change"]
    print(f"  하락비율: {report.decline_ratio*100:.1f}% ({breadth['down_count']}/{breadth['total']})")
    print(f"  시장 5일: {report.kospi_5d_change:+.2f}%")

    # 3. 글로벌 리스크
    print("\n[3] 글로벌 리스크 체크...")
    global_alerts = _check_global_risk()
    for ga in global_alerts:
        print(f"  {ga['message']}")
    if not global_alerts:
        print("  해외 리스크 정상")

    # 4. CORTEX Layer 1 — 6단계 체제 판단
    print("\n[4] CORTEX 체제 판단...")
    regime_state = detect_regime()
    report.regime = regime_state.current
    report.regime_previous = regime_state.previous
    report.regime_changed_at = regime_state.changed_at
    rules = REGIME_RULES[Regime[regime_state.current]]
    report.regime_new_buy = rules["new_buy"]
    report.position_multiplier = rules["capital_use"]
    print(f"  체제: {regime_state.current}")
    if regime_state.changed_at:
        print(f"  전환: {regime_state.previous} -> {regime_state.current} ({regime_state.changed_at})")
    print(f"  매수: {'가능' if rules['new_buy'] else '중단'} | 사이즈: {rules['capital_use']*100:.0f}%")

    # 5. CORTEX Layer 2 — 충격 분류
    print("\n[5] CORTEX 충격 분류...")
    shock = classify_shock()
    report.shock_type = shock.shock_type
    report.shock_confidence = shock.confidence
    report.shock_detail = shock.description
    report.affected_sectors = shock.affected_sectors
    report.opportunity_sectors = shock.opportunity_sectors
    if shock.shock_type != "NONE":
        print(f"  충격: {shock.shock_type} (신뢰도: {shock.confidence:.0%})")
        print(f"  상세: {shock.description}")
        print(f"  영향섹터: {', '.join(shock.affected_sectors)}")
        print(f"  기회섹터: {', '.join(shock.opportunity_sectors)}")
    else:
        print("  충격: 없음 (일반 변동)")

    # ─── 기존 경보 판정 (하위호환) ───
    alerts = list(global_alerts)

    if report.foreign_5d_net <= T["foreign_5d_critical"]:
        alerts.append({"level": "critical", "type": "foreign",
                        "message": f"외국인 5일 순매도 {abs(report.foreign_5d_net):,.0f} — 대량 이탈"})
    elif report.foreign_5d_net <= T["foreign_5d_warning"]:
        alerts.append({"level": "warning", "type": "foreign",
                        "message": f"외국인 5일 순매도 {abs(report.foreign_5d_net):,.0f} — 주의"})

    if report.inst_5d_net <= T["inst_5d_critical"]:
        alerts.append({"level": "critical", "type": "institution",
                        "message": f"기관 5일 순매도 {abs(report.inst_5d_net):,.0f} — 기관 이탈"})
    elif report.inst_5d_net <= T["inst_5d_warning"]:
        alerts.append({"level": "warning", "type": "institution",
                        "message": f"기관 5일 순매도 {abs(report.inst_5d_net):,.0f} — 주의"})

    if report.decline_ratio >= T["decline_ratio_critical"]:
        alerts.append({"level": "critical", "type": "breadth",
                        "message": f"하락 종목 {report.decline_ratio*100:.0f}% — 전면 하락"})
    elif report.decline_ratio >= T["decline_ratio_warning"]:
        alerts.append({"level": "warning", "type": "breadth",
                        "message": f"하락 종목 {report.decline_ratio*100:.0f}% — 약세 구간"})

    if report.kospi_5d_change <= T["kospi_drop_critical"]:
        alerts.append({"level": "critical", "type": "kospi",
                        "message": f"시장 5일 {report.kospi_5d_change:+.1f}% — 급락"})
    elif report.kospi_5d_change <= T["kospi_drop_warning"]:
        alerts.append({"level": "warning", "type": "kospi",
                        "message": f"시장 5일 {report.kospi_5d_change:+.1f}% — 약세"})

    # 하위호환 alert_level (체제 기반 매핑)
    regime_enum = Regime[regime_state.current]
    if regime_enum in (Regime.PANIC, Regime.SHOCK):
        report.alert_level = "critical"
    elif regime_enum in (Regime.CAUTION, Regime.RECOVERY_EARLY):
        report.alert_level = "warning"
    else:
        report.alert_level = "normal"

    if not alerts:
        alerts.append({"level": "normal", "type": "all_clear",
                        "message": "수급 구조 정상 범위 내"})

    report.alerts = alerts

    # 출력
    print(f"\n  === 종합 ===")
    regime_icons = {
        "NORMAL": "  ", "CAUTION": "  ", "SHOCK": "  ",
        "PANIC": "  ", "RECOVERY_EARLY": "  ", "RECOVERY_CONFIRMED": "  ",
    }
    print(f"  체제: {regime_icons.get(regime_state.current, '')} {regime_state.current}")
    print(f"  포지션: {report.position_multiplier*100:.0f}%")
    if shock.shock_type != "NONE":
        print(f"  충격: {shock.shock_type} ({shock.confidence:.0%})")

    icons = {"critical": "  ", "warning": "  ", "normal": "  "}
    for a in alerts:
        print(f"  {icons.get(a['level'], '?')} {a['message']}")

    _save_health(report)
    return report


def _save_health(report: MarketHealthReport):
    """진단 결과 저장"""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    health_path = DATA_DIR / "market_health.json"
    with open(health_path, "w", encoding="utf-8") as f:
        json.dump(report.to_dict(), f, ensure_ascii=False, indent=2)
    print(f"\n  저장: {health_path}")


# ═══════════════════════════════════════════════════
#  외부 API (하위호환 + 신규)
# ═══════════════════════════════════════════════════

def get_position_multiplier() -> float:
    """시장 건전성 기반 포지션 배수 조회 (캐시)
    Returns: 1.0 / 0.8 / 0.7 / 0.5 / 0.3 / 0.0
    """
    health_path = DATA_DIR / "market_health.json"
    if not health_path.exists():
        return 1.0
    try:
        with open(health_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        ts = datetime.fromisoformat(data["timestamp"])
        if (datetime.now() - ts).total_seconds() > 6 * 3600:
            return 1.0
        return data.get("position_multiplier", 1.0)
    except Exception:
        return 1.0


def get_regime_rules() -> dict:
    """현재 체제 기반 행동 규칙 조회 (v2: SL/TP 파라미터 포함)

    Returns:
        {"regime": "NORMAL", "new_buy": True, "capital_use": 1.0,
         "sl_atr_mult": 0.5, "sl_max_pct": 0.05, "tp_atr_mult": 1.6,
         "partial_tp": False}
    """
    default_rules = REGIME_RULES[Regime.NORMAL]
    fallback = {"regime": "NORMAL", **default_rules}

    health_path = DATA_DIR / "market_health.json"
    if not health_path.exists():
        return fallback
    try:
        with open(health_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        ts = datetime.fromisoformat(data["timestamp"])
        if (datetime.now() - ts).total_seconds() > 6 * 3600:
            return fallback
        regime = data.get("regime", "NORMAL")
        rules = REGIME_RULES.get(Regime[regime], default_rules)
        return {"regime": regime, **rules}
    except Exception:
        return fallback


def get_shock_info() -> dict:
    """현재 충격 정보 조회

    Returns:
        {"shock_type": "NONE", "affected_sectors": [], "opportunity_sectors": []}
    """
    health_path = DATA_DIR / "market_health.json"
    if not health_path.exists():
        return {"shock_type": "NONE", "affected_sectors": [], "opportunity_sectors": []}
    try:
        with open(health_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return {
            "shock_type": data.get("shock_type", "NONE"),
            "shock_confidence": data.get("shock_confidence", 0),
            "shock_detail": data.get("shock_detail", ""),
            "affected_sectors": data.get("affected_sectors", []),
            "opportunity_sectors": data.get("opportunity_sectors", []),
        }
    except Exception:
        return {"shock_type": "NONE", "affected_sectors": [], "opportunity_sectors": []}


def format_health_report(report: MarketHealthReport) -> str:
    """텔레그램용 포맷 (v2: 체제+충격 포함)"""
    regime_icons = {
        "NORMAL": "NORMAL", "CAUTION": "CAUTION",
        "SHOCK": "SHOCK", "PANIC": "PANIC",
        "RECOVERY_EARLY": "RECOVERY_EARLY",
        "RECOVERY_CONFIRMED": "RECOVERY_CONFIRMED",
    }
    shock_icons = {
        "NONE": "", "GEOPOLITICAL": "GEOPOLITICAL",
        "RATE": "RATE", "EARNINGS": "EARNINGS",
        "LIQUIDITY": "LIQUIDITY", "COMPOUND": "COMPOUND",
    }

    lines = []
    lines.append("시장 건전성 + CORTEX 체제")
    lines.append(f"{datetime.now().strftime('%Y-%m-%d %H:%M')}")
    lines.append("=" * 30)
    lines.append("")

    # 체제
    r = report.regime
    lines.append(f"체제: {regime_icons.get(r, r)} ({r})")
    if report.regime_changed_at:
        lines.append(f"전환: {report.regime_previous} -> {r} ({report.regime_changed_at})")

    # 충격
    if report.shock_type != "NONE":
        si = shock_icons.get(report.shock_type, report.shock_type)
        lines.append(f"충격: {si} (신뢰도: {report.shock_confidence:.0%})")
        if report.shock_detail:
            lines.append(f"  -> {report.shock_detail}")
    else:
        lines.append("충격: 없음")

    lines.append("=" * 30)

    # 행동 규칙
    buy_str = "가능" if report.regime_new_buy else "중단"
    lines.append(f"매수: {buy_str}")
    lines.append(f"사이즈: {report.position_multiplier*100:.0f}%")

    if report.affected_sectors:
        lines.append(f"영향섹터: {', '.join(report.affected_sectors)} (매수제외)")
    if report.opportunity_sectors:
        lines.append(f"기회섹터: {', '.join(report.opportunity_sectors)}")

    lines.append("=" * 30)

    # 수급 지표
    lines.append(f"외국인 5일: {report.foreign_5d_net:+,.0f}")
    lines.append(f"기관 5일: {report.inst_5d_net:+,.0f}")
    lines.append(f"하락비율: {report.decline_ratio*100:.1f}%")
    lines.append(f"시장 5일: {report.kospi_5d_change:+.1f}%")
    lines.append("")

    alert_icons = {"critical": "[!]", "warning": "[?]", "normal": "[v]"}
    for a in report.alerts:
        lines.append(f"{alert_icons.get(a['level'], '?')} {a['message']}")

    lines.append("=" * 30)
    return "\n".join(lines)


# ═══════════════════════════════════════════════════
#  CLI
# ═══════════════════════════════════════════════════

if __name__ == "__main__":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
    logging.basicConfig(level=logging.WARNING)

    import argparse
    parser = argparse.ArgumentParser(description="시장 건전성 + CORTEX 체제 모니터")
    parser.add_argument("--telegram", action="store_true", help="텔레그램 전송")
    args = parser.parse_args()

    report = diagnose()

    if args.telegram:
        try:
            from bot.telegram_bot import send_message
            msg = format_health_report(report)
            send_message(msg)
            print("\n  텔레그램 전송 완료")
        except Exception as e:
            print(f"\n  텔레그램 전송 실패: {e}")
