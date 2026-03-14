# -*- coding: utf-8 -*-
"""
MOMENTUM 레짐 감지기 (Regime Detector)
=======================================
"한발 늦는" 문제의 핵심 해결책.

기관+외국인이 연속 매수 중이면 MOMENTUM 레짐 → RSI/MACD 무시하고 빠른 진입.
수급이 빠지면 즉시 탈출 (-3.5% SL + 수급 반전 EXIT).

5가지 팩터로 레짐 판별:
  (A) 거래량 폭증 (vol_ratio ≥ 2x)
  (B) 기관+외인 연속 매수 (3일+)
  (C) 섹터 동반 상승 (breadth ≥ 15%)
  (D) 기관 프로그램 매수 (5일 중 3일+)
  (E) 조용한 매집 (기관 매수 + 거래량 보합)

THRESHOLD: score ≥ 0.55 → MOMENTUM, 아니면 NORMAL
"""

import logging
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data_store"
DAILY_DIR = DATA_DIR / "daily"
FLOW_DIR = DATA_DIR / "flow"

logger = logging.getLogger("BH.Regime")

MOMENTUM_THRESHOLD = 0.55


# =============================================================================
# 데이터 구조
# =============================================================================

@dataclass
class RegimeResult:
    """레짐 감지 결과"""
    code: str
    name: str
    regime: str = "NORMAL"          # "MOMENTUM" or "NORMAL"
    score: float = 0.0              # 0.0 ~ 1.0
    vol_ratio: float = 1.0          # latest_vol / avg_20d
    consec_inst_foreign_days: int = 0
    factors: dict = field(default_factory=dict)
    # factors = {
    #   "vol_surge": 0.0~0.25,
    #   "consec_buy": 0.0~0.30,
    #   "sector_breadth": 0.0~0.20,
    #   "program_buy": 0.0~0.15,
    #   "quiet_accum": 0.0~0.10,
    # }


# =============================================================================
# 데이터 로드 헬퍼
# =============================================================================

def _load_daily(code: str) -> Optional[pd.DataFrame]:
    """일봉 CSV 로드 (최근 30일+)"""
    path = DAILY_DIR / f"{code}.csv"
    if not path.exists():
        return None
    try:
        df = pd.read_csv(path, index_col=0, parse_dates=True)
        if len(df) < 10:
            return None
        return df.sort_index()
    except Exception:
        return None


def _load_flow(code: str) -> Optional[pd.DataFrame]:
    """수급 CSV 로드 (기관/외국인/개인 순매수)"""
    path = FLOW_DIR / f"{code}_investor.csv"
    if not path.exists():
        return None
    try:
        df = pd.read_csv(path, index_col=0, parse_dates=True)
        if len(df) < 5:
            return None
        return df.sort_index()
    except Exception:
        return None


def _calc_rsi(close_series: pd.Series, period: int = 14) -> float:
    """RSI-14 계산"""
    if len(close_series) < period + 1:
        return 50.0
    delta = close_series.diff()
    gain = delta.clip(lower=0)
    loss = (-delta).clip(lower=0)
    avg_gain = gain.rolling(period).mean()
    avg_loss = loss.rolling(period).mean()
    last_gain = avg_gain.iloc[-1]
    last_loss = avg_loss.iloc[-1]
    if last_loss == 0:
        return 100.0
    rs = last_gain / last_loss
    return 100 - (100 / (1 + rs))


# =============================================================================
# 5가지 팩터 스코어링
# =============================================================================

def _score_volume_surge(daily: pd.DataFrame) -> tuple[float, float]:
    """(A) 거래량 폭증 스코어

    Returns: (score, vol_ratio)
    """
    if len(daily) < 21:
        return 0.0, 1.0

    latest_vol = daily["거래량"].iloc[-1]
    avg_20d = daily["거래량"].iloc[-21:-1].mean()

    if avg_20d <= 0:
        return 0.0, 1.0

    vol_ratio = latest_vol / avg_20d

    if vol_ratio >= 3.0:
        return 0.25, vol_ratio
    elif vol_ratio >= 2.0:
        return 0.15, vol_ratio
    elif vol_ratio >= 1.5:
        return 0.08, vol_ratio
    return 0.0, vol_ratio


def _count_consecutive_buy_days(flow: pd.DataFrame) -> int:
    """기관+외인 연속 순매수 일수 (최근 데이터부터 역산)"""
    if flow is None or len(flow) == 0:
        return 0

    count = 0
    for i in range(len(flow) - 1, -1, -1):
        row = flow.iloc[i]
        inst_net = row.get("기관_금액", 0) or 0
        frgn_net = row.get("외국인_금액", 0) or 0
        if (inst_net + frgn_net) > 0:
            count += 1
        else:
            break
    return count


def _score_consecutive_buy(consec_days: int) -> float:
    """(B) 기관+외인 연속 매수 스코어"""
    if consec_days >= 5:
        return 0.30
    elif consec_days >= 3:
        return 0.20
    elif consec_days >= 2:
        return 0.10
    return 0.0


def _score_sector_breadth(breadth: float) -> float:
    """(C) 섹터 동반상승 스코어 (breadth = 3%+ 상승 종목 비율)"""
    if breadth >= 0.30:
        return 0.20
    elif breadth >= 0.15:
        return 0.10
    return 0.0


def _score_program_buy(flow: pd.DataFrame) -> float:
    """(D) 기관 프로그램 매수: 최근 5일 중 3일+ 기관 순매수"""
    if flow is None or len(flow) < 5:
        return 0.0

    recent_5 = flow.iloc[-5:]
    positive_days = 0
    for _, row in recent_5.iterrows():
        inst_net = row.get("기관_금액", 0) or 0
        if inst_net > 0:
            positive_days += 1

    if positive_days >= 4:
        return 0.15
    elif positive_days >= 3:
        return 0.10
    return 0.0


def _score_quiet_accumulation(daily: pd.DataFrame, flow: pd.DataFrame) -> float:
    """(E) 조용한 매집: 기관 매수 + 거래량 보합 + 가격 변동 작음

    조건:
      - 최근 5일 기관 순매수 양수
      - 거래량 최근 5일/이전 10일 비율 < 1.3 (폭증 아님)
      - 가격 변동폭 < 3% (조용함)
    """
    if daily is None or flow is None:
        return 0.0
    if len(daily) < 16 or len(flow) < 5:
        return 0.0

    # 기관 5일 순매수 합계
    recent_flow = flow.iloc[-5:]
    inst_total = sum((row.get("기관_금액", 0) or 0) for _, row in recent_flow.iterrows())
    if inst_total <= 0:
        return 0.0

    # 거래량 비교 (최근 5일 / 이전 10일)
    vol_recent = daily["거래량"].iloc[-5:].mean()
    vol_earlier = daily["거래량"].iloc[-15:-5].mean()
    if vol_earlier > 0:
        vol_change = vol_recent / vol_earlier
        if vol_change > 1.3:
            return 0.0  # 거래량 폭증이면 조용한 매집 아님

    # 가격 변동폭 (최근 5일)
    close_5d = daily["종가"].iloc[-5:]
    price_range = (close_5d.max() - close_5d.min()) / close_5d.mean() * 100
    if price_range > 3.0:
        return 0.0  # 변동 크면 조용한 매집 아님

    return 0.10


# =============================================================================
# 메인 감지 함수
# =============================================================================

def detect_regime_batch(
    codes: list[str],
    universe: dict,
    sector_breadths: dict = None,
) -> dict[str, RegimeResult]:
    """배치 레짐 감지

    Args:
        codes: 분석할 종목 코드 리스트
        universe: universe.json (dict: code -> info)
        sector_breadths: 섹터별 breadth (rotation_detail에서 가져옴)

    Returns:
        {code: RegimeResult}
    """
    if sector_breadths is None:
        sector_breadths = {}

    results = {}

    for code in codes:
        info = universe.get(code, {})
        name = info.get("name", code)
        sector = info.get("sector", "")

        daily = _load_daily(code)
        flow = _load_flow(code)

        if daily is None:
            results[code] = RegimeResult(code=code, name=name)
            continue

        # (A) 거래량 폭증
        vol_score, vol_ratio = _score_volume_surge(daily)

        # (B) 기관+외인 연속 매수
        consec_days = _count_consecutive_buy_days(flow) if flow is not None else 0
        consec_score = _score_consecutive_buy(consec_days)

        # (C) 섹터 breadth
        breadth = sector_breadths.get(sector, 0)
        breadth_score = _score_sector_breadth(breadth)

        # (D) 기관 프로그램 매수
        program_score = _score_program_buy(flow)

        # (E) 조용한 매집
        quiet_score = _score_quiet_accumulation(daily, flow)

        # 종합 스코어
        total_score = vol_score + consec_score + breadth_score + program_score + quiet_score

        # 레짐 판정
        regime = "MOMENTUM" if total_score >= MOMENTUM_THRESHOLD else "NORMAL"

        factors = {
            "vol_surge": vol_score,
            "consec_buy": consec_score,
            "sector_breadth": breadth_score,
            "program_buy": program_score,
            "quiet_accum": quiet_score,
        }

        results[code] = RegimeResult(
            code=code,
            name=name,
            regime=regime,
            score=round(total_score, 3),
            vol_ratio=round(vol_ratio, 2),
            consec_inst_foreign_days=consec_days,
            factors=factors,
        )

        if regime == "MOMENTUM":
            logger.info(
                f"[MOMENTUM] {name}({code}): score={total_score:.2f} "
                f"vol={vol_ratio:.1f}x 기관연속={consec_days}D "
                f"factors={factors}"
            )

    return results


# =============================================================================
# 수급 이탈 체크 (포지션 탈출용)
# =============================================================================

def check_supply_withdrawal(code: str) -> bool:
    """수급 이탈 체크: 최신일 기관+외인 순매수 < 0 → True

    auto_trader.job_daily_reeval에서 MOMENTUM 포지션 탈출 판단에 사용.
    """
    flow = _load_flow(code)
    if flow is None or len(flow) == 0:
        return False

    latest = flow.iloc[-1]
    inst_net = latest.get("기관_금액", 0) or 0
    frgn_net = latest.get("외국인_금액", 0) or 0

    return (inst_net + frgn_net) < 0


# =============================================================================
# CLI 테스트
# =============================================================================

if __name__ == "__main__":
    import json

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )

    # universe 로드
    uni_path = DATA_DIR / "universe.json"
    with open(uni_path, "r", encoding="utf-8") as f:
        universe = json.load(f)

    # 인자로 받은 종목 또는 기본 종목
    codes = sys.argv[1:] if len(sys.argv) > 1 else [
        "005930", "000660", "035420", "051910", "034730",
        "006400", "028260", "017670",
    ]

    print("=" * 70)
    print("  REGIME DETECTOR — 레짐 감지 테스트")
    print("=" * 70)

    results = detect_regime_batch(codes, universe)

    for code, r in results.items():
        tag = "MTM" if r.regime == "MOMENTUM" else "NRM"
        print(
            f"\n  [{tag}] {r.name}({r.code}): "
            f"score={r.score:.3f} "
            f"vol={r.vol_ratio:.1f}x "
            f"기관연속={r.consec_inst_foreign_days}D"
        )
        for k, v in r.factors.items():
            bar = "+" * int(v * 20)
            print(f"    {k:18s}: {v:.3f} {bar}")

        # 수급 이탈 여부
        withdrawn = check_supply_withdrawal(code)
        print(f"    수급 이탈: {'YES' if withdrawn else 'NO'}")

    print(f"\n{'=' * 70}")
    mtm_count = sum(1 for r in results.values() if r.regime == "MOMENTUM")
    print(f"  결과: {mtm_count}/{len(results)} MOMENTUM, "
          f"{len(results) - mtm_count}/{len(results)} NORMAL")
    print(f"{'=' * 70}")
