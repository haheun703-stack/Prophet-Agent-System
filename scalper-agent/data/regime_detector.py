# -*- coding: utf-8 -*-
"""
MOMENTUM 레짐 감지기 (Regime Detector)
=======================================
"한발 늦는" 문제의 핵심 해결책.

기관+외국인이 연속 매수 중이면 MOMENTUM 레짐 → RSI/MACD 무시하고 빠른 진입.
수급이 빠지면 즉시 탈출 (-3.5% SL + 수급 반전 EXIT).

7가지 팩터로 레짐 판별:
  (A) 거래량 폭증 (vol_ratio ≥ 2x)
  (B) 기관+외인 연속 매수 (2일+)
  (C) 섹터 동반 상승 (breadth ≥ 10%)
  (D) 기관 프로그램 매수 (5일 중 3일+)
  (E) 조용한 매집 (기관 매수 + 거래량 보합)
  (F) 섹터 피어 부스트 (같은 섹터 수급 양수 비율)
  (G) 외국인 국적 파워 (싱가포르+케이맨 비중 → 헤지펀드 적극 베팅)

THRESHOLD: score ≥ 0.40 → MOMENTUM, 아니면 NORMAL
"""

import logging
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import pandas as pd

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data_store"
DAILY_DIR = DATA_DIR / "daily"
FLOW_DIR = DATA_DIR / "flow"

logger = logging.getLogger("BH.Regime")

MOMENTUM_THRESHOLD = 0.40


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
    #   "sector_peer": 0.0~0.15,
    #   "nationality": 0.0~0.20,
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
    """(B) 기관+외인 연속 매수 스코어 (v2: 가중치 상향)"""
    if consec_days >= 5:
        return 0.30
    elif consec_days >= 3:
        return 0.25  # v2: 0.20→0.25 (3일 매집도 강한 신호)
    elif consec_days >= 2:
        return 0.15  # v2: 0.10→0.15 (2일도 의미있는 신호)
    return 0.0


def _score_sector_breadth(breadth: float) -> float:
    """(C) 섹터 동반상승 스코어 (breadth = 3%+ 상승 종목 비율)"""
    if breadth >= 0.30:
        return 0.20
    elif breadth >= 0.15:
        return 0.15  # v2: 0.10→0.15
    elif breadth >= 0.10:
        return 0.08  # v2: 새로 추가 (10%도 감지)
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
# (F) 섹터 피어 부스트 — 같은 섹터 수급 양수 비율로 가산
# =============================================================================

def _calc_sector_peer_flow(sector: str, universe: dict,
                           exclude_codes: set = None,
                           max_sample: int = 30) -> float:
    """섹터 내 종목들의 최근 수급 양수 비율 계산

    같은 섹터 종목 중 최근 1일 기관+외인 순매수 양수인 비율 반환.
    시총 상위 max_sample개만 샘플링 (속도).

    Returns: 0.0 ~ 1.0 (양수 비율)
    """
    if not sector:
        return 0.0

    # 같은 섹터 종목 추출 (시총 상위)
    sector_stocks = [
        (code, info.get("cap_억", 0))
        for code, info in universe.items()
        if info.get("sector") == sector
        and (exclude_codes is None or code not in exclude_codes)
    ]
    sector_stocks.sort(key=lambda x: x[1], reverse=True)
    sector_stocks = sector_stocks[:max_sample]

    if len(sector_stocks) < 3:
        return 0.0

    positive = 0
    checked = 0
    for code, _ in sector_stocks:
        flow = _load_flow(code)
        if flow is None or len(flow) == 0:
            continue
        checked += 1
        latest = flow.iloc[-1]
        inst = latest.get("기관_금액", 0) or 0
        frgn = latest.get("외국인_금액", 0) or 0
        if (inst + frgn) > 0:
            positive += 1

    if checked < 3:
        return 0.0
    return positive / checked


def _score_sector_peer(peer_ratio: float) -> float:
    """(F) 섹터 피어 부스트 스코어

    같은 섹터 내 수급 양수 종목 비율이 높으면 가산.
    """
    if peer_ratio >= 0.60:
        return 0.15  # 섹터 60%+ 종목이 기관+외인 매수 → 강한 섹터 모멘텀
    elif peer_ratio >= 0.45:
        return 0.10  # 45%+ → 보통 섹터 모멘텀
    elif peer_ratio >= 0.35:
        return 0.05  # 35%+ → 약한 신호
    return 0.0


# =============================================================================
# (G) 외국인 국적 파워 — 싱가포르+케이맨 비중이 높으면 헤지펀드 적극 베팅
# =============================================================================

# 국적 분류
_HEDGE_COUNTRIES = {"케이맨 제도", "영국령 버진아일랜드", "버뮤다"}
_ASIA_ACTIVE = {"싱가포르", "홍콩"}  # 패시브가 아닌 적극적 아시아 자금
_NATIONALITY_DIR = DATA_DIR / "nationality"


def _load_nationality(code: str) -> dict:
    """최신 국적별 거래량 스냅샷 로드

    nationality/{code}_{YYYYMMDD}.csv 중 가장 최근 파일 사용.
    누적 프로파일(nationality_{code}.csv)도 fallback으로 사용.

    Returns: {국가명: 거래량} or {}
    """
    if not _NATIONALITY_DIR.exists():
        return {}

    # 날짜별 스냅샷 (가장 최신)
    snapshots = sorted(_NATIONALITY_DIR.glob(f"{code}_2*.csv"), reverse=True)
    if snapshots:
        try:
            df = pd.read_csv(snapshots[0], encoding="utf-8-sig")
            return dict(zip(df["국가명"], df["거래량"]))
        except Exception:
            pass

    # 누적 프로파일 fallback
    profile = _NATIONALITY_DIR / f"nationality_{code}.csv"
    if profile.exists():
        try:
            df = pd.read_csv(profile, encoding="utf-8-sig")
            return dict(zip(df["국가명"], df["거래량"]))
        except Exception:
            pass

    return {}


def _calc_nationality_ratio(nat_data: dict) -> tuple[float, float, float]:
    """싱가포르+케이맨 비중 계산

    Returns: (hedge_asia_pct, hedge_pct, asia_active_pct)
      - hedge_asia_pct: (싱가포르+홍콩+케이맨+BVI+버뮤다) / 전체
      - hedge_pct: (케이맨+BVI+버뮤다) / 전체
      - asia_active_pct: (싱가포르+홍콩) / 전체
    """
    if not nat_data:
        return 0.0, 0.0, 0.0

    total = sum(v for v in nat_data.values() if v > 0)
    if total <= 0:
        return 0.0, 0.0, 0.0

    hedge = sum(nat_data.get(c, 0) for c in _HEDGE_COUNTRIES)
    asia_active = sum(nat_data.get(c, 0) for c in _ASIA_ACTIVE)

    hedge_pct = hedge / total
    asia_pct = asia_active / total
    combined = (hedge + asia_active) / total

    return combined, hedge_pct, asia_pct


def _score_nationality(nat_data: dict) -> float:
    """(G) 국적 파워 스코어

    싱가포르+케이맨 합산 비중 기준:
      35%+ → 0.20 (강한 헤지펀드 베팅)
      25%+ → 0.12
      15%+ → 0.05
      <15% → 0.00 (패시브 주도, 모멘텀 약함)
    """
    combined, _, _ = _calc_nationality_ratio(nat_data)

    if combined >= 0.35:
        return 0.20  # 싱+케 35%+ → 강한 헤지펀드 적극 베팅
    elif combined >= 0.25:
        return 0.12  # 25%+ → 중간
    elif combined >= 0.15:
        return 0.05  # 15%+ → 약한 신호
    return 0.0


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

    # 섹터별 피어 수급 비율 캐시 (섹터당 1번만 계산)
    _sector_peer_cache: dict[str, float] = {}
    code_set = set(codes)

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

        # (C) 섹터 breadth (morning_recommendation에서 전달)
        breadth = sector_breadths.get(sector, 0)
        breadth_score = _score_sector_breadth(breadth)

        # (D) 기관 프로그램 매수
        program_score = _score_program_buy(flow)

        # (E) 조용한 매집
        quiet_score = _score_quiet_accumulation(daily, flow)

        # (F) 섹터 피어 부스트 (같은 섹터 내 수급 양수 비율)
        if sector and sector not in _sector_peer_cache:
            _sector_peer_cache[sector] = _calc_sector_peer_flow(
                sector, universe, exclude_codes=code_set
            )
        peer_ratio = _sector_peer_cache.get(sector, 0)
        peer_score = _score_sector_peer(peer_ratio)

        # (G) 외국인 국적 파워 (싱가포르+케이맨 비중)
        nat_data = _load_nationality(code)
        nat_score = _score_nationality(nat_data)

        # 종합 스코어
        total_score = (vol_score + consec_score + breadth_score
                       + program_score + quiet_score + peer_score
                       + nat_score)

        # 레짐 판정
        regime = "MOMENTUM" if total_score >= MOMENTUM_THRESHOLD else "NORMAL"

        factors = {
            "vol_surge": vol_score,
            "consec_buy": consec_score,
            "sector_breadth": breadth_score,
            "program_buy": program_score,
            "quiet_accum": quiet_score,
            "sector_peer": peer_score,
            "nationality": nat_score,
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
            nat_detail = ""
            if nat_score > 0:
                combined, h_pct, a_pct = _calc_nationality_ratio(nat_data)
                nat_detail = f" 국적={combined:.0%}(싱{a_pct:.0%}+케{h_pct:.0%})"
            logger.info(
                f"[MOMENTUM] {name}({code}): score={total_score:.2f} "
                f"vol={vol_ratio:.1f}x 기관연속={consec_days}D "
                f"peer={peer_ratio:.0%}{nat_detail} factors={factors}"
            )

    # 섹터 피어 캐시 로그
    for sec, ratio in _sector_peer_cache.items():
        if ratio >= 0.35:
            logger.info(f"[섹터 피어] {sec}: 수급양수 {ratio:.0%}")

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
