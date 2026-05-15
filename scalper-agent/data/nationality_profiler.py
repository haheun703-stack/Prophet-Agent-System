"""
외국인 국적별 행동 프로파일러 + 내일 수급 예측
─────────────────────────────────────────────
T-1 데이터로 T+1을 유추하는 방법:

전략 핵심:
  1. 연속성 모멘텀: 3일+ 연속 증가 국가 → 내일도 매수 확률 높음
  2. 기관국(영국,노르웨이) 방향: 꾸준하면 → 바닥 다짐 (안전)
  3. 헤지펀드(케이맨) 급증 후: 1~2일 후 빠질 수 있음 → 단기 경고
  4. 중국/홍콩 신규 진입: 관망→진입 전환 시 → 상승 초기 신호
  5. 복합 시그널: 기관+아시아 동시 증가 = 가장 강한 매수 신호

국가별 아키타입:
  🏛 패시브 기관 (영국,노르웨이,스위스): 천천히 움직이지만 방향 잡으면 오래감
  🦈 헤지펀드 (케이맨,버진): 빠르게 들어왔다 빠르게 나감 (1~5일)
  🐉 아시아 큰손 (중국,홍콩,싱가포르): 진입 느리지만 진입하면 추세 강함
  🦅 미국: 글로벌 방향성, ETF 리밸런싱 위주

사용:
  from data.nationality_profiler import analyze_nationality_behavior, predict_tomorrow_flow

  # 종목별 국가 행동 분석
  profiles = analyze_nationality_behavior(["000660", "005930"])

  # 내일 수급 예측 시그널
  signals = predict_tomorrow_flow(["000660", "005930"])

  # 텔레그램 보고
  report = format_prediction_report(signals, {"000660": "SK하이닉스"})

  # CLI
  python data/nationality_profiler.py 000660 005930 068270
"""

import json
import logging
import statistics
import time
from pathlib import Path
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from data.trading_calendar import is_trading_day
from typing import Dict, List, Optional, Tuple

import pandas as pd

logger = logging.getLogger(__name__)

DATA_DIR = Path(__file__).resolve().parent.parent / "data_store" / "nationality"
DATA_DIR.mkdir(parents=True, exist_ok=True)

# ── 국가 아키타입 분류 ──
ARCHETYPE = {
    # 패시브 기관 (방향 잡으면 오래감, 느림)
    "영국": ("기관", "🏛"),
    "노르웨이": ("기관", "🏛"),
    "스위스": ("기관", "🏛"),
    "스웨덴": ("기관", "🏛"),
    "아일랜드": ("기관", "🏛"),
    "룩셈부르크": ("기관", "🏛"),
    "덴마크": ("기관", "🏛"),
    # 헤지펀드 (빠른 진입/이탈, 1~5일)
    "케이맨 제도": ("헤지펀드", "🦈"),
    "영국령 버진아일랜드": ("헤지펀드", "🦈"),
    "버뮤다": ("헤지펀드", "🦈"),
    # 아시아 큰손 (진입 느리지만 추세 강함)
    "중국": ("아시아", "🐉"),
    "홍콩": ("아시아", "🐉"),
    "싱가포르": ("아시아", "🐉"),
    "대만": ("아시아", "🐉"),
    "일본": ("아시아", "🐉"),
    # 북미
    "미국": ("북미", "🦅"),
    "캐나다": ("북미", "🦅"),
}

# 분석 대상 주요국 (순서)
FOCUS_COUNTRIES = [
    "영국", "미국", "케이맨 제도", "싱가포르", "중국", "홍콩",
    "노르웨이", "일본", "스위스", "오스트레일리아", "프랑스",
]

# ── 7 SECRET: 백테스트 검증 국가 가중치 (2/1~3/11, 1달치) ──
SURGE_COUNTRY_WEIGHT = {
    "영국": +3.54, "케이맨 제도": +2.24, "중국": +1.41,
    "싱가포르": +1.01, "프랑스": +0.85, "미국": +0.62,
    "노르웨이": +0.50,
}
REVERSE_COUNTRIES = {"스위스": -1.93, "일본": -1.63, "오스트레일리아": -1.20}

COMBO_BONUS = {
    ("영국", "싱가포르"): +5.07,
    ("영국", "케이맨 제도"): +4.41,
    ("영국", "중국"): +3.90,
    ("케이맨 제도", "싱가포르"): +3.50,
}

SIGNAL_DECAY_DAYS = {
    "영국": 3, "노르웨이": 4, "케이맨 제도": 1, "영국령 버진아일랜드": 1,
    "중국": 5, "홍콩": 4, "싱가포르": 3, "일본": 2,
    "미국": 3, "프랑스": 3, "스위스": 2, "오스트레일리아": 2,
}


@dataclass
class NationalityPower:
    """7 SECRET 국적 파워 분석 결과"""
    score: float = 0.0           # 총합 점수 (-30 ~ +50)
    grade: str = "NEUTRAL"       # POWER_BUY / BUY / NEUTRAL / CAUTION / DANGER
    cascade_detected: bool = False  # SECRET 1: 헤지→기관 체인
    vpd_detected: bool = False      # SECRET 2: Volume-Price Divergence
    ghost_countries: list = field(default_factory=list)  # SECRET 3: 실종 국가
    breadth_signal: str = ""        # SECRET 4: "BROAD_SURGE" / ""
    decay_factor: float = 1.0       # SECRET 5: 시그널 수명 감쇠
    reverse_penalty: float = 0.0    # SECRET 6: 역지표 감점
    combo_bonus: float = 0.0        # SECRET 7: 조합 증폭
    detail: str = ""                # 요약 문자열

# ── 행동 패턴 정의 ──
PATTERN_ACCUMULATING = "매집중"     # 3일+ 연속 증가
PATTERN_DUMPING = "이탈중"          # 3일+ 연속 감소
PATTERN_STEADY = "꾸준유지"         # 변동성 낮고 매일 매수 (기관형)
PATTERN_DAYTRADING = "단타형"       # 거래량 변동 극심
PATTERN_WATCHING = "관망"           # 간헐적 거래
PATTERN_ENTERING = "신규진입"       # 없다가 최근 등장
PATTERN_NORMAL = "일반매매"         # 위 어느 것에도 해당 안됨


# ═══════════════════════════════════════════
# 일별 데이터 수집
# ═══════════════════════════════════════════

def _get_trading_days(n_days: int = 5) -> List[str]:
    """최근 N 영업일 리스트 (주말 제외)"""
    days = []
    dt = datetime.now()
    # KRX 데이터는 T-1 → 오늘 18시 전이면 어제부터
    if dt.hour < 18:
        dt -= timedelta(days=1)
    while len(days) < n_days:
        if is_trading_day(dt.date() if hasattr(dt, 'date') else dt):  # 거래일
            days.append(dt.strftime("%Y%m%d"))
        dt -= timedelta(days=1)
    return list(reversed(days))


def collect_daily_series(
    codes: List[str],
    n_days: int = 5,
    max_retries: int = 1,
) -> Dict[str, Dict[str, Dict[str, int]]]:
    """종목별 일별 국적별 거래량 시계열 수집

    Args:
        codes: 종목코드 리스트
        n_days: 최근 N 거래일
        max_retries: 0/n일 (전체 실패) 종목 재시도 횟수 (기본 1회)
                     KRX 일시 timeout/429 대응. 매 재시도 사이 30초 대기.

    Returns: {code: {YYYYMMDD: {국가명: 거래량}}}
    """
    from data.krx_nationality_crawler import (
        _get_valid_session, _fetch_nationality_http, REQUEST_INTERVAL,
    )

    days = _get_trading_days(n_days)
    logger.info(f"수집 기간: {days[0]}~{days[-1]} ({len(days)}일)")

    session = _get_valid_session()
    if not session:
        logger.error("KRX 세션 없음")
        return {}

    def _collect_one_code(code: str, sess) -> tuple:
        """단일 종목 5일치 수집. (수집 결과 dict, 새 session) 반환."""
        result = {}
        for day in days:
            cache_path = DATA_DIR / f"{code}_{day}.csv"
            if cache_path.exists():
                try:
                    df = pd.read_csv(cache_path, encoding="utf-8-sig")
                    result[day] = dict(zip(df.iloc[:, 0], df.iloc[:, 1].astype(int)))
                    continue
                except Exception:
                    pass

            for mkt in ["STK", "KSQ"]:
                df = _fetch_nationality_http(sess, code, day, day, mkt)
                if df is None:
                    sess = _get_valid_session()
                    if sess:
                        df = _fetch_nationality_http(sess, code, day, day, mkt)
                if df is not None and not df.empty:
                    data = dict(zip(df.iloc[:, 0], df.iloc[:, 1].astype(int)))
                    result[day] = data
                    df.to_csv(cache_path, index=False, encoding="utf-8-sig")
                    break
                time.sleep(REQUEST_INTERVAL)
            time.sleep(REQUEST_INTERVAL)
        return result, sess

    results = {}
    failed_codes = []  # 0/n일 종목 (재시도 대상)
    for code in codes:
        result, session = _collect_one_code(code, session)
        results[code] = result
        if len(result) == 0:
            failed_codes.append(code)
        logger.info(f"{code}: {len(result)}/{len(days)}일 수집")

    # 재시도: 0/n일 종목만 max_retries회 추가 시도 (KRX 일시 장애 대응)
    if failed_codes and max_retries > 0:
        for attempt in range(1, max_retries + 1):
            logger.info(f"[retry {attempt}/{max_retries}] {len(failed_codes)}개 실패 종목 재시도 (30s 대기)")
            time.sleep(30)
            # 새 세션 강제 갱신
            session = _get_valid_session()
            if not session:
                logger.warning(f"[retry {attempt}] KRX 세션 갱신 실패 — 중단")
                break
            still_failed = []
            for code in failed_codes:
                result, session = _collect_one_code(code, session)
                if len(result) > 0:
                    results[code] = result
                    logger.info(f"[retry {attempt}] {code}: {len(result)}/{len(days)}일 복구")
                else:
                    still_failed.append(code)
            recovered = len(failed_codes) - len(still_failed)
            logger.info(f"[retry {attempt}] {recovered}/{len(failed_codes)}개 복구")
            failed_codes = still_failed
            if not failed_codes:
                break

    return results


# ═══════════════════════════════════════════
# 국가별 행동 프로파일링
# ═══════════════════════════════════════════

def _classify_pattern(
    daily_vols: List[int],
    active_days: int,
    total_days: int,
) -> str:
    """일별 거래량 시계열 → 행동 패턴 분류"""
    if active_days == 0:
        return PATTERN_WATCHING

    # 최근 등장 (이전엔 없다가 마지막 1~2일에 진입)
    if active_days <= 2 and total_days >= 4:
        if daily_vols[-1] > 0 and sum(daily_vols[:-2]) == 0:
            return PATTERN_ENTERING

    if active_days <= 2:
        return PATTERN_WATCHING

    # 연속 증가/감소 (0 제외한 nonzero 기준)
    nonzero = [(i, v) for i, v in enumerate(daily_vols) if v > 0]
    if len(nonzero) >= 3:
        nz_vals = [v for _, v in nonzero]
        diffs = [nz_vals[i] - nz_vals[i - 1] for i in range(1, len(nz_vals))]
        inc_count = sum(1 for d in diffs if d > 0)
        dec_count = sum(1 for d in diffs if d < 0)

        # 3일+ 연속 증가
        if inc_count >= len(diffs) * 0.7 and len(diffs) >= 2:
            return PATTERN_ACCUMULATING
        # 3일+ 연속 감소
        if dec_count >= len(diffs) * 0.7 and len(diffs) >= 2:
            return PATTERN_DUMPING

        # 변동성 판단 (CV = 표준편차/평균)
        if len(nz_vals) >= 3:
            avg = statistics.mean(nz_vals)
            if avg > 0:
                cv = statistics.stdev(nz_vals) / avg
                if cv < 0.35:
                    return PATTERN_STEADY
                if cv > 0.8:
                    return PATTERN_DAYTRADING

    return PATTERN_NORMAL


def analyze_nationality_behavior(
    codes: List[str],
    n_days: int = 5,
    code_names: Dict[str, str] = None,
    daily_data: Dict[str, Dict[str, Dict[str, int]]] = None,
) -> Dict[str, List[dict]]:
    """종목별 국가 행동 프로파일 분석

    Returns: {code: [{"국가", "아키타입", "패턴", "일별거래량", "평균", "추세", "예측점수"}]}
    """
    if daily_data is None:
        daily_data = collect_daily_series(codes, n_days)

    profiles = {}
    for code in codes:
        daily = daily_data.get(code, {})
        if len(daily) < 2:
            profiles[code] = []
            continue

        sorted_days = sorted(daily.keys())
        all_countries = set()
        for d in daily.values():
            all_countries.update(d.keys())

        stock_profiles = []
        for country in FOCUS_COUNTRIES:
            if country not in all_countries:
                continue

            vols = [daily.get(day, {}).get(country, 0) for day in sorted_days]
            total = sum(vols)
            if total == 0:
                continue

            active_days = sum(1 for v in vols if v > 0)
            pattern = _classify_pattern(vols, active_days, len(sorted_days))

            arch_type, emoji = ARCHETYPE.get(country, ("기타", "🌐"))

            # 추세 점수 (-10 ~ +10): 양수=증가추세, 음수=감소추세
            nonzero = [v for v in vols if v > 0]
            if len(nonzero) >= 2:
                avg = statistics.mean(nonzero)
                # 최근 절반 vs 이전 절반 비교
                mid = len(nonzero) // 2
                if mid > 0:
                    early_avg = statistics.mean(nonzero[:mid])
                    late_avg = statistics.mean(nonzero[mid:])
                    if early_avg > 0:
                        trend = (late_avg - early_avg) / early_avg * 10
                        trend = max(-10, min(10, trend))
                    else:
                        trend = 5.0 if late_avg > 0 else 0
                else:
                    trend = 0
            else:
                avg = total
                trend = 0

            # 내일 예측 점수 (-20 ~ +20)
            pred_score = _calc_prediction_score(pattern, arch_type, trend, vols)

            stock_profiles.append({
                "국가": country,
                "아키타입": arch_type,
                "이모지": emoji,
                "패턴": pattern,
                "일별": vols,
                "날짜": sorted_days,
                "활성일": active_days,
                "총일수": len(sorted_days),
                "평균": int(avg),
                "추세": round(trend, 1),
                "예측점수": round(pred_score, 1),
            })

        # 예측점수 기준 정렬
        stock_profiles.sort(key=lambda x: x["예측점수"], reverse=True)
        profiles[code] = stock_profiles

    return profiles


def _calc_prediction_score(
    pattern: str,
    archetype: str,
    trend: float,
    daily_vols: List[int],
) -> float:
    """내일 매수 지속 확률 점수 (-20 ~ +20)

    핵심 로직:
      - 기관국이 꾸준하면 = 안전 바닥 (+5~10)
      - 아시아 큰손이 매집중이면 = 추세 초기 (+8~15)
      - 헤지펀드 급증이면 = 단기 +5, 하지만 3일 후 반전 위험
      - 이탈중이면 = 마이너스
    """
    score = 0.0

    # 패턴 기본 점수
    pattern_base = {
        PATTERN_ACCUMULATING: 8,
        PATTERN_STEADY: 6,
        PATTERN_ENTERING: 4,
        PATTERN_NORMAL: 1,
        PATTERN_DAYTRADING: -2,
        PATTERN_WATCHING: 0,
        PATTERN_DUMPING: -8,
    }
    score += pattern_base.get(pattern, 0)

    # 아키타입 보정
    if archetype == "기관":
        if pattern in (PATTERN_ACCUMULATING, PATTERN_STEADY):
            score += 4  # 기관이 매집 = 매우 긍정적
        elif pattern == PATTERN_DUMPING:
            score -= 3  # 기관 이탈 = 매우 부정적
    elif archetype == "아시아":
        if pattern in (PATTERN_ACCUMULATING, PATTERN_ENTERING):
            score += 5  # 중국/홍콩 진입 = 강한 추세 시작
        elif pattern == PATTERN_STEADY:
            score += 3  # 꾸준히 사고 있음
    elif archetype == "헤지펀드":
        if pattern == PATTERN_ACCUMULATING:
            score += 2  # 단기적 호재, 하지만 지속성 의문
            # 3일 이상이면 빠질 위험 경고
            active = sum(1 for v in daily_vols if v > 0)
            if active >= 4:
                score -= 3  # 헤지펀드 4일+ = 차익실현 임박
        elif pattern == PATTERN_DAYTRADING:
            score -= 3  # 변동성만 키움

    # 추세 반영 (±3)
    score += trend * 0.3

    return max(-20, min(20, score))


# ═══════════════════════════════════════════
# 7 SECRET 국적 파워 시스템
# ═══════════════════════════════════════════

def _get_country_averages(
    daily_data: Dict[str, Dict[str, int]],
    countries: List[str],
) -> Dict[str, float]:
    """종목의 일별 데이터에서 국가별 평균 거래량 계산"""
    avgs = {}
    days = sorted(daily_data.keys())
    for country in countries:
        vols = [daily_data.get(d, {}).get(country, 0) for d in days]
        nonzero = [v for v in vols if v > 0]
        avgs[country] = statistics.mean(nonzero) if nonzero else 0
    return avgs


def _count_consecutive_surge(
    daily_data: Dict[str, Dict[str, int]],
    country: str,
    avg: float,
    threshold: float = 2.0,
) -> int:
    """최근부터 역순으로 연속 급증 일수 카운트"""
    days = sorted(daily_data.keys(), reverse=True)
    count = 0
    for d in days:
        vol = daily_data.get(d, {}).get(country, 0)
        if avg > 0 and vol >= avg * threshold:
            count += 1
        else:
            break
    return count


def calc_nationality_power(
    codes: List[str],
    daily_data: Dict[str, Dict[str, Dict[str, int]]] = None,
    price_data: Dict[str, dict] = None,
    all_codes_data: Dict[str, Dict[str, Dict[str, int]]] = None,
    n_days: int = 5,
) -> Dict[str, NationalityPower]:
    """7 SECRET 국적 파워 분석

    Args:
        codes: 분석 대상 종목 코드
        daily_data: {code: {YYYYMMDD: {국가명: 거래량}}} — 미리 수집된 시계열
        price_data: {code: {"chg_5d": float}} — 5일 수익률
        all_codes_data: 전체 종목 데이터 (breadth 계산용)
        n_days: 수집 일수

    Returns:
        {code: NationalityPower}
    """
    if daily_data is None:
        daily_data = collect_daily_series(codes, n_days)
    if all_codes_data is None:
        all_codes_data = daily_data

    results = {}
    for code in codes:
        code_daily = daily_data.get(code, {})
        if len(code_daily) < 2:
            results[code] = NationalityPower()
            continue

        days = sorted(code_daily.keys())
        latest_day = days[-1]
        latest_data = code_daily.get(latest_day, {})

        # 국가별 평균 계산
        all_countries = set()
        for d_data in code_daily.values():
            all_countries.update(d_data.keys())
        avgs = _get_country_averages(code_daily, list(all_countries))

        # 급증 국가 판별 (최신일 기준, 평균 2x+)
        surging = []
        for country in FOCUS_COUNTRIES:
            vol = latest_data.get(country, 0)
            avg = avgs.get(country, 0)
            if avg > 0 and vol >= avg * 2.0:
                surging.append(country)

        # ── SECRET 1: Cascade Detection (+8) ──
        cascade_detected = False
        hedge_countries = ["케이맨 제도", "영국령 버진아일랜드"]
        inst_countries = ["영국", "노르웨이"]
        avg_hedge = sum(avgs.get(c, 0) for c in hedge_countries)
        avg_inst = sum(avgs.get(c, 0) for c in inst_countries)

        hedge_surge_day_idx = None
        for i, day in enumerate(days[:-1]):  # 마지막 날 제외
            d_data = code_daily.get(day, {})
            hedge_vol = sum(d_data.get(c, 0) for c in hedge_countries)
            if avg_hedge > 0 and hedge_vol >= avg_hedge * 2.0:
                hedge_surge_day_idx = i

        if hedge_surge_day_idx is not None:
            for j in range(hedge_surge_day_idx + 1, len(days)):
                d_data = code_daily.get(days[j], {})
                inst_vol = sum(d_data.get(c, 0) for c in inst_countries)
                if avg_inst > 0 and inst_vol >= avg_inst * 1.5:
                    cascade_detected = True
                    break

        cascade_bonus = 8.0 if cascade_detected else 0.0

        # ── SECRET 2: VPD — Volume-Price Divergence (+10) ──
        vpd_detected = False
        chg_5d = 0.0
        if price_data and code in price_data:
            chg_5d = price_data[code].get("chg_5d", 0.0)

        if chg_5d < -3.0:  # 주가 5일간 -3%+
            for c in ["영국", "케이맨 제도", "미국"]:
                vol = latest_data.get(c, 0)
                avg = avgs.get(c, 0)
                if avg > 0 and vol >= avg * 2.0:
                    vpd_detected = True
                    break

        vpd_bonus = 10.0 if vpd_detected else 0.0

        # ── SECRET 3: Ghost Detection (−감점) ──
        ghost_countries = []
        ghost_penalty = 0.0
        if len(days) >= 3:
            early_countries = set()
            for d in days[:len(days) // 2]:
                early_countries.update(code_daily.get(d, {}).keys())
            late_countries = set()
            for d in days[len(days) // 2:]:
                late_countries.update(code_daily.get(d, {}).keys())

            for country in FOCUS_COUNTRIES:
                if country in early_countries and country not in late_countries:
                    ghost_countries.append(country)
                    weight = SURGE_COUNTRY_WEIGHT.get(country, 0.5)
                    ghost_penalty -= min(weight, 3.0)

        # ── SECRET 4: Breadth Signal (+5) ──
        breadth_signal = ""
        if all_codes_data and len(all_codes_data) > 1:
            uk_surge_count = 0
            for other_code, other_daily in all_codes_data.items():
                other_latest = other_daily.get(latest_day, {})
                uk_vol = other_latest.get("영국", 0)
                other_avg = _get_country_averages(other_daily, ["영국"]).get("영국", 0)
                if other_avg > 0 and uk_vol >= other_avg * 2.0:
                    uk_surge_count += 1
            if uk_surge_count >= 3:
                breadth_signal = "BROAD_SURGE"

        breadth_bonus = 5.0 if breadth_signal else 0.0

        # ── SECRET 5: Signal Decay (×0.3~1.0) ──
        decay_factors = []
        for country in surging:
            max_days = SIGNAL_DECAY_DAYS.get(country, 2)
            consecutive = _count_consecutive_surge(code_daily, country, avgs.get(country, 0))
            if consecutive >= max_days:
                df = max(0.3, 1.0 - (consecutive - max_days) * 0.25)
            else:
                df = 1.0
            decay_factors.append(df)
        avg_decay = statistics.mean(decay_factors) if decay_factors else 1.0

        # ── SECRET 6: Reverse Indicator (−감점) ──
        reverse_penalty = 0.0
        for country, penalty_weight in REVERSE_COUNTRIES.items():
            vol = latest_data.get(country, 0)
            avg = avgs.get(country, 0)
            if avg > 0 and vol >= avg * 2.0:
                reverse_penalty += penalty_weight

        # ── SECRET 7: Non-linear Scoring ──
        # 혼합 신호 감쇠
        positive_surging = [c for c in surging if c in SURGE_COUNTRY_WEIGHT]
        negative_surging = [c for c in surging if c in REVERSE_COUNTRIES]
        mixed_dampen = 0.6 if (positive_surging and negative_surging) else 1.0

        # 조합 증폭
        combo_bonus = 0.0
        surging_set = set(surging)
        for (c1, c2), bonus in COMBO_BONUS.items():
            if c1 in surging_set and c2 in surging_set:
                combo_bonus += bonus * 0.3

        # ── 기본 국가 점수 (가중 합산) ──
        base_score = 0.0
        for country in surging:
            weight = SURGE_COUNTRY_WEIGHT.get(country, 0.3)
            base_score += weight

        # ── 최종 종합 ──
        final_score = (
            base_score * avg_decay * mixed_dampen
            + cascade_bonus + vpd_bonus + ghost_penalty
            + breadth_bonus + reverse_penalty + combo_bonus
        )
        final_score = max(-30, min(50, final_score))

        # Grade 결정
        if final_score >= 20:
            grade = "POWER_BUY"
        elif final_score >= 8:
            grade = "BUY"
        elif final_score >= -5:
            grade = "NEUTRAL"
        elif final_score >= -15:
            grade = "CAUTION"
        else:
            grade = "DANGER"

        # Detail 문자열
        detail_parts = []
        if cascade_detected:
            detail_parts.append("CASCADE")
        if vpd_detected:
            detail_parts.append("VPD")
        if ghost_countries:
            detail_parts.append(f"GHOST:{','.join(ghost_countries[:2])}")
        if breadth_signal:
            detail_parts.append("BREADTH")
        if combo_bonus > 0:
            detail_parts.append(f"COMBO+{combo_bonus:.1f}")
        if reverse_penalty < -0.5:
            detail_parts.append(f"REV{reverse_penalty:.1f}")
        if positive_surging:
            detail_parts.append(f"급증:{'+'.join(positive_surging[:3])}")
        if avg_decay < 0.9:
            detail_parts.append(f"감쇠{avg_decay:.1f}")

        results[code] = NationalityPower(
            score=round(final_score, 2),
            grade=grade,
            cascade_detected=cascade_detected,
            vpd_detected=vpd_detected,
            ghost_countries=ghost_countries,
            breadth_signal=breadth_signal,
            decay_factor=round(avg_decay, 2),
            reverse_penalty=round(reverse_penalty, 2),
            combo_bonus=round(combo_bonus, 2),
            detail="|".join(detail_parts) if detail_parts else "",
        )

        logger.debug(
            f"{code}: NatPower={final_score:+.1f} [{grade}] "
            f"surge={surging} cascade={cascade_detected} vpd={vpd_detected}"
        )

    return results


# ═══════════════════════════════════════════
# 내일 수급 예측 시그널
# ═══════════════════════════════════════════

def predict_tomorrow_flow(
    codes: List[str],
    n_days: int = 5,
    code_names: Dict[str, str] = None,
    price_data: Dict[str, dict] = None,
) -> List[dict]:
    """종목별 내일 외국인 수급 예측 + 7 SECRET 파워 분석

    Returns: [{
        "code", "name",
        "signal": "STRONG_BUY" | "BUY" | "NEUTRAL" | "CAUTION" | "SELL",
        "score": -100~+100,
        "reason": "기관매집+아시아진입",
        "key_countries": [{국가, 패턴, 예측점수}],
        "risk": "헤지펀드 차익실현 임박" | None,
        "nat_power": NationalityPower | None,
    }]
    """
    # daily_data를 먼저 수집하여 프로파일링 + 파워 양쪽에서 재사용 (API 중복 방지)
    daily_data = collect_daily_series(codes, n_days)
    profiles = analyze_nationality_behavior(codes, n_days, code_names, daily_data=daily_data)

    # 7 SECRET 파워 계산 (daily_data 재사용, API 중복 호출 방지)
    power_map = calc_nationality_power(
        codes, daily_data=daily_data, price_data=price_data,
        all_codes_data=daily_data, n_days=n_days,
    )

    predictions = []
    for code in codes:
        name = (code_names or {}).get(code, code)
        stock_profiles = profiles.get(code, [])
        if not stock_profiles:
            predictions.append({
                "code": code, "name": name,
                "signal": "NEUTRAL", "score": 0,
                "reason": "데이터부족", "key_countries": [], "risk": None,
            })
            continue

        # 총 예측점수 합산
        total_score = sum(p["예측점수"] for p in stock_profiles)

        # 아키타입별 합산
        arch_scores = {}
        for p in stock_profiles:
            arch = p["아키타입"]
            arch_scores[arch] = arch_scores.get(arch, 0) + p["예측점수"]

        # 시그널 결정
        reasons = []
        risks = []

        # 기관 방향
        inst_score = arch_scores.get("기관", 0)
        if inst_score >= 10:
            reasons.append("🏛기관매집")
        elif inst_score <= -5:
            risks.append("🏛기관이탈")

        # 아시아 큰손
        asia_score = arch_scores.get("아시아", 0)
        if asia_score >= 8:
            reasons.append("🐉아시아진입")
        elif asia_score <= -5:
            risks.append("🐉아시아이탈")

        # 헤지펀드
        hf_score = arch_scores.get("헤지펀드", 0)
        if hf_score >= 5:
            reasons.append("🦈헤지관심")
        elif hf_score <= -3:
            risks.append("🦈헤지이탈")
        # 헤지펀드 차익실현 경고
        for p in stock_profiles:
            if p["아키타입"] == "헤지펀드" and p["활성일"] >= 4:
                if p["패턴"] in (PATTERN_ACCUMULATING, PATTERN_STEADY):
                    risks.append("🦈차익실현임박")
                    break

        # 북미
        na_score = arch_scores.get("북미", 0)
        if na_score >= 5:
            reasons.append("🦅미국매수")

        # 복합 시그널 보너스
        if inst_score >= 5 and asia_score >= 5:
            total_score += 10  # 기관+아시아 = 최강 시그널
            reasons.append("⭐복합매수")
        if inst_score <= -5 and asia_score <= -5:
            total_score -= 10
            risks.append("⚠️복합이탈")

        # 시그널 등급 결정
        if total_score >= 25:
            signal = "STRONG_BUY"
        elif total_score >= 12:
            signal = "BUY"
        elif total_score >= -5:
            signal = "NEUTRAL"
        elif total_score >= -15:
            signal = "CAUTION"
        else:
            signal = "SELL"

        # 주요 국가 (예측점수 상위 5)
        key_countries = [
            {"국가": p["국가"], "패턴": p["패턴"],
             "이모지": p["이모지"], "예측점수": p["예측점수"]}
            for p in stock_profiles[:5]
        ]

        predictions.append({
            "code": code,
            "name": name,
            "signal": signal,
            "score": round(total_score, 1),
            "reason": " + ".join(reasons) if reasons else "특이사항 없음",
            "risk": " / ".join(risks) if risks else None,
            "key_countries": key_countries,
            "arch_scores": arch_scores,
            "nat_power": power_map.get(code),  # 7 SECRET NationalityPower
        })

    # 점수 기준 정렬
    predictions.sort(key=lambda x: x["score"], reverse=True)
    return predictions


# ═══════════════════════════════════════════
# 텔레그램 보고 포맷
# ═══════════════════════════════════════════

SIGNAL_EMOJI = {
    "STRONG_BUY": "🔴",
    "BUY": "🟠",
    "NEUTRAL": "⚪",
    "CAUTION": "🟡",
    "SELL": "🔵",
}

SIGNAL_KR = {
    "STRONG_BUY": "강력매수",
    "BUY": "매수",
    "NEUTRAL": "중립",
    "CAUTION": "주의",
    "SELL": "매도",
}


def format_prediction_report(
    predictions: List[dict],
    code_names: Dict[str, str] = None,
) -> str:
    """텔레그램 보고용 포맷"""
    lines = []
    lines.append("🌍 외국인 국적별 수급 예측")
    lines.append(f"📅 기준: {datetime.now().strftime('%m/%d')} 장마감 → 내일 전망")
    lines.append("─" * 28)

    for pred in predictions:
        name = (code_names or {}).get(pred["code"], pred.get("name", pred["code"]))
        sig = pred["signal"]
        sig_emoji = SIGNAL_EMOJI.get(sig, "⚪")
        sig_kr = SIGNAL_KR.get(sig, sig)
        score = pred["score"]

        lines.append(f"\n{sig_emoji} {name} [{sig_kr}] (점수:{score:+.0f})")

        # 매수 근거
        if pred["reason"] != "특이사항 없음":
            lines.append(f"  근거: {pred['reason']}")

        # 리스크
        if pred.get("risk"):
            lines.append(f"  ⚠️ {pred['risk']}")

        # 주요국 패턴 (상위 3)
        for kc in pred.get("key_countries", [])[:3]:
            lines.append(
                f"  {kc['이모지']}{kc['국가']}: {kc['패턴']} "
                f"({kc['예측점수']:+.0f}점)"
            )

    lines.append("\n" + "─" * 28)
    lines.append("🔴강력매수 🟠매수 ⚪중립 🟡주의 🔵매도")
    lines.append("※ 국적별 수급은 T-1 기준, 예측 참고용")

    return "\n".join(lines)


# ═══════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════

if __name__ == "__main__":
    import sys
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    # 기본 종목: WAR TOP + 주요
    default_targets = {
        "068270": "셀트리온",
        "000660": "SK하이닉스",
        "196170": "알테오젠",
        "373220": "LG에너지솔루션",
        "005930": "삼성전자",
        "065450": "RFHIC",
        "055550": "신한지주",
        "105560": "KB금융",
    }

    if len(sys.argv) > 1:
        codes = sys.argv[1:]
        code_names = {c: c for c in codes}
    else:
        codes = list(default_targets.keys())
        code_names = default_targets

    print(f"\n{'=' * 50}")
    print(f"  외국인 국적별 수급 예측 ({len(codes)}종목)")
    print(f"{'=' * 50}\n")

    predictions = predict_tomorrow_flow(codes, n_days=5, code_names=code_names)
    report = format_prediction_report(predictions, code_names)
    print(report)

    # JSON 저장
    out_path = Path(__file__).parent.parent / "data_store" / "nationality_prediction.json"
    safe_preds = []
    for p in predictions:
        sp = {k: v for k, v in p.items()}
        sp["arch_scores"] = {k: float(v) for k, v in sp.get("arch_scores", {}).items()}
        safe_preds.append(sp)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(safe_preds, f, ensure_ascii=False, indent=2, default=str)
    print(f"\n저장: {out_path}")
