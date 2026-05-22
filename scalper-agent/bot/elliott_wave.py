# -*- coding: utf-8 -*-
"""엘리어트 파동 4파 눌림목 감지 모듈 (사장님 5/21 23:00 영구 약속).

사장님 자료 (`3파,4파 상승.txt` + PNG 37장) 학습 기반:
  - 3파는 가장 강력한 상승 → 차익실현 → 4파 조정
  - 4파 = 5파 진입 직전 마지막 매수 기회

4가지 핵심 룰 (모두 통과 시 5파 매수 시그널):
  ① 피보나치 38.2% — 3파 길이의 38.2% 되돌림 지점 지지
  ② 파동 교대 법칙 — 2파 V자 → 4파 횡보 / 2파 횡보 → 4파 V자
  ③ 파동 중첩 금지 — 4파 저점 > 1파 고점 (절대 침범 X)
  ④ 횡보 매물 소화 — 4파 구간 옆으로 비비며 → 5파 임박

사용 위치:
  - asset_pool_loader 점수 보정 (5파 진입 시그널 +25점)
  - jarvis_decision 통합 의사결정 (PYRAMID_ADD 트리거 강화)
  - 5/26 D-Day 실전 통합

영구 메모리: [[project_elliott_wave_5_22_evening]] / [[project_5_22_evening_learning_fix]]
"""
import logging
from dataclasses import dataclass, asdict
from typing import Dict, List, Literal, Optional, Sequence, Tuple

logger = logging.getLogger(__name__)


# ── 파동 단계 ─────────────────────────────────────
WavePhase = Literal[
    "unknown",           # 패턴 미감지
    "wave_1_running",    # 1파 상승 중
    "wave_2_pullback",   # 2파 조정 중
    "wave_3_running",    # 3파 강력 상승 중
    "wave_4_pullback",   # 4파 조정 중 (★ 매수 후보 ★)
    "wave_5_start",      # 5파 진입 (★ 매수 시그널 ★)
]


# ── 5단계 피보 진입 셋팅 (사장님 5/22 19:30 A+C 정밀 백테스트 검증) ─────
# 표본 285건 × 5파 성공 58건 = baseline 20.35%
# zone별 실측 성공률 (universe 200종):
#   28~48%   (fib_38_safe):     46.7% (x2.3 ★ 최우선) / 평균 수익 +28.1%
#   48~62%   (fib_50_standard): 30.8% (x1.5)         / 평균 수익 +39.5%
#   62~80%   (korean_low):      9.1%  (x0.45 ★ 회피 ★) / 평균 수익 +38.8%
#   80~100%  (trend_caution):   20.8% (x1.0)         / 평균 수익 +48.9% (★ 큰 수익 ★)
#   100%+    (trend_end):       추세 종료 의심 — 회피
#   0~28%    (too_shallow):     얕은 되돌림 — 4파 미형성
#
# ★ 5/22 19:30 단타봇 진짜 정정 ★ A+C 최적 조합 TOP:
#   1. fib_38_safe + non_overlap=False → 75% (x3.69)
#   2. fib_38_safe + non_overlap=True → 66.7% (x3.28)
#   ⚠️ alternation_check + sideways_w4 룰 = baseline 이하 (false negative 발생) → 제거
FIB_ZONES = [
    # (lo, hi, confidence, zone_name, signal)
    (28, 48,   1.0, "fib_38_safe",     "BUY"),         # ★ 최우선 (46.7% 성공) ★
    (48, 62,   0.7, "fib_50_standard", "BUY"),         # 차선 (30.8%)
    (62, 80,   0.1, "korean_low",      "AVOID"),       # ★ 회피 (9.1% 최악) ★
    (80, 100,  0.4, "trend_caution",   "BUY_CAUTION"), # 큰 수익 (+49%, 성공 20.8%)
    (100, 9999, 0.0, "trend_end",      "NO_ENTRY"),    # 추세 종료
    (0,  28,   0.0, "too_shallow",     "NO_ENTRY"),    # 너무 얕음
]


def evaluate_fib_zone(fib_retracement_pct: float) -> Dict:
    """피보 되돌림 % → 진입 신호 + confidence (백테스트 285건 기반).

    Args:
        fib_retracement_pct: 4파 저점이 3파 길이의 몇 % 되돌림 (양수, 0~100+)

    Returns:
        {signal: BUY/WAIT/NO_ENTRY, confidence: 0~1, zone: zone_name}
    """
    p = fib_retracement_pct
    for lo, hi, conf, name, sig in FIB_ZONES:
        if lo <= p < hi:
            return {"signal": sig, "confidence": conf, "zone": name}
    return {"signal": "NO_ENTRY", "confidence": 0.0, "zone": "unknown"}


# ── 결과 데이터클래스 ─────────────────────────────
@dataclass
class ElliottResult:
    """엘리어트 파동 분석 결과.

    Attributes:
        phase: 현재 파동 단계
        wave_1_high: 1파 고점 (없으면 None)
        wave_2_low:  2파 저점
        wave_3_high: 3파 고점
        wave_4_low:  현재 4파 저점 (4파 진행 중일 때)
        fib_382_level: 3파 길이의 38.2% 되돌림 지지 가격
        fib_zone_match: 현재가 ∈ 38.2% 지지 구간 (± tolerance)
        non_overlap_check: 파동 중첩 금지 룰 통과 여부 (4파 저점 > 1파 고점)
        alternation_check: 파동 교대 법칙 — "OK_alternating" / "FAIL_same_shape"
        wave_2_shape: 2파 형태 ("V_sharp" / "sideways" / "unknown")
        wave_4_shape: 4파 형태 (현재 형성 중)
        buy_signal: 5파 진입 매수 시그널 (4룰 모두 통과 시 True)
        confidence: 0.0~1.0 (룰 통과 비율)
        reason: 시그널/거부 사유 (사람 읽기용)
    """
    phase: WavePhase = "unknown"
    wave_1_high: Optional[float] = None
    wave_2_low: Optional[float] = None
    wave_3_high: Optional[float] = None
    wave_4_low: Optional[float] = None
    fib_382_level: Optional[float] = None
    fib_zone_match: bool = False
    fib_retracement_pct: Optional[float] = None  # 5/22 추가 — 실제 측정값
    fib_zone_name: str = "unknown"               # 5/22 추가 — 4단계 zone 이름
    fib_zone_signal: str = "NO_ENTRY"            # 5/22 추가 — BUY/WAIT/NO_ENTRY
    non_overlap_check: bool = False
    alternation_check: str = "unknown"
    wave_2_shape: str = "unknown"
    wave_4_shape: str = "unknown"
    buy_signal: bool = False
    confidence: float = 0.0
    reason: str = ""

    def to_dict(self) -> Dict:
        return asdict(self)


# ── 캔들 헬퍼 ──────────────────────────────────────
# candles[i] = {"open": float, "high": float, "low": float, "close": float, "volume": int}
# 시간 순서 (오래된 것 → 최신).
Candle = Dict[str, float]


def _is_trading_halted(candle: Candle) -> bool:
    """거래정지 봉 판정 (5/22 단타봇 manual 검증 발견).

    O=H=L=C + Volume=0 = 거래정지 (예: 금호에이치티 4/9~5/4 한 달치).
    이런 봉을 스윙 분석에 포함하면 옛날 패턴 잘못 잡음.
    """
    return (
        candle.get("volume", 0) == 0
        and candle.get("open") == candle.get("high")
        == candle.get("low") == candle.get("close")
    )


def _swing_highs_lows(
    candles: Sequence[Candle], lookback: int = 3
) -> Tuple[List[int], List[int]]:
    """스윙 고점/저점 인덱스 추출.

    한 캔들이 좌우 lookback 캔들보다 high가 크면 스윙 고점,
    low가 작으면 스윙 저점.

    ★ 5/22 단타봇 fix ★ 거래정지 봉 (Volume=0 + O=H=L=C) 자동 제외.
    금호에이치티 manual 검증에서 한 달치 정지가 알고리즘 왜곡 발견.

    Args:
        candles: 시간순 캔들 리스트
        lookback: 좌우 비교 윈도우 (기본 3캔들)

    Returns:
        (swing_high_indices, swing_low_indices)
    """
    n = len(candles)
    highs: List[int] = []
    lows: List[int] = []

    for i in range(lookback, n - lookback):
        # 거래정지 봉 스킵 (단타봇 5/22 fix)
        if _is_trading_halted(candles[i]):
            continue

        hi = candles[i]["high"]
        lo = candles[i]["low"]

        # 좌우 비교 시에도 거래정지 봉 무시
        left_highs = [candles[i - k]["high"] for k in range(1, lookback + 1)
                      if not _is_trading_halted(candles[i - k])]
        right_highs = [candles[i + k]["high"] for k in range(1, lookback + 1)
                       if not _is_trading_halted(candles[i + k])]
        left_lows = [candles[i - k]["low"] for k in range(1, lookback + 1)
                     if not _is_trading_halted(candles[i - k])]
        right_lows = [candles[i + k]["low"] for k in range(1, lookback + 1)
                      if not _is_trading_halted(candles[i + k])]

        if not left_highs or not right_highs:
            continue

        is_high = all(hi > h for h in left_highs) and all(hi > h for h in right_highs)
        is_low = all(lo < lo2 for lo2 in left_lows) and all(lo < lo2 for lo2 in right_lows)

        if is_high:
            highs.append(i)
        if is_low:
            lows.append(i)

    return highs, lows


def _classify_wave_shape(
    candles: Sequence[Candle], start_idx: int, end_idx: int
) -> str:
    """파동 형태 분류 — "V_sharp" / "sideways" / "moderate" / "unknown".

    추세 강도 (변동률 / 캔들수) 기반:
      - V_sharp:  추세 강도 ≥ 3.0% per candle (적은 캔들 + 큰 변동)
      - sideways: 추세 강도 ≤ 1.5% per candle (많은 캔들 + 작은 변동)
      - moderate: 그 외

    사장님 자료 4룰 ② "2파 V자 → 4파 횡보" 검증 핵심.
    """
    if end_idx <= start_idx or end_idx >= len(candles):
        return "unknown"

    segment = candles[start_idx:end_idx + 1]
    num_candles = len(segment)
    if num_candles <= 0:
        return "unknown"

    high_max = max(c["high"] for c in segment)
    low_min = min(c["low"] for c in segment)
    price_range = high_max - low_min
    start_price = segment[0]["close"]

    if start_price <= 0:
        return "unknown"

    range_pct = price_range / start_price * 100.0  # 변동률 (%)
    intensity = range_pct / num_candles            # 캔들당 변동률 (%)

    if intensity >= 3.0:
        return "V_sharp"
    if intensity <= 1.5:
        return "sideways"
    return "moderate"


def _identify_waves(
    candles: Sequence[Candle], lookback: int = 3
) -> Tuple[Optional[int], Optional[int], Optional[int], Optional[int]]:
    """캔들에서 1파-2파-3파-4파 스윙 포인트 인덱스 추출.

    Returns:
        (wave_1_high_idx, wave_2_low_idx, wave_3_high_idx, wave_4_low_idx)
        각 None일 수 있음 (해당 파동 미감지)

    감지 전략 (보수적):
      - 가장 최근 스윙 패턴 = (high → low → high → low) 시퀀스
      - 3파 고점은 1파 고점보다 높아야 함 (상승 추세)
      - 2파 저점은 1파 고점보다 낮아야 함 (당연)
    """
    highs, lows = _swing_highs_lows(candles, lookback=lookback)

    if len(highs) < 2 or len(lows) < 2:
        return None, None, None, None

    # 최신 스윙부터 역순 시도 (가장 최근 패턴 우선)
    # 시퀀스: high1 < low1 < high2 < (low2 또는 현재) — 인덱스 순서
    for w3_idx in reversed(highs):
        # 1) w3 이전의 저점 찾기 (w2)
        prev_lows = [li for li in lows if li < w3_idx]
        if not prev_lows:
            continue
        w2_idx = prev_lows[-1]

        # 2) w2 이전의 고점 찾기 (w1)
        prev_highs = [hi for hi in highs if hi < w2_idx]
        if not prev_highs:
            continue
        w1_idx = prev_highs[-1]

        # 3) 상승 추세 검증 (w3 > w1)
        if candles[w3_idx]["high"] <= candles[w1_idx]["high"]:
            continue

        # 4) 2파 저점 < 1파 고점 (당연)
        if candles[w2_idx]["low"] >= candles[w1_idx]["high"]:
            continue

        # 5) w3 이후의 저점 (w4 — 진행 중일 수 있음)
        next_lows = [li for li in lows if li > w3_idx]
        w4_idx = next_lows[0] if next_lows else None

        return w1_idx, w2_idx, w3_idx, w4_idx

    return None, None, None, None


# ── 메인 분석 함수 ─────────────────────────────────
def detect_elliott_pattern(
    candles: Sequence[Candle],
    current_price: Optional[float] = None,
    fib_tolerance_pct: float = 5.0,
    lookback: int = 3,
) -> ElliottResult:
    """엘리어트 4파 눌림목 + 5파 진입 시그널 감지.

    Args:
        candles: 시간순 캔들 리스트 (최소 20개 권장 / 5분봉 기준 ~100분)
        current_price: 현재가 (없으면 마지막 캔들 close 사용)
        fib_tolerance_pct: 피보나치 38.2% 지지 구간 허용 오차 (% 단위)
        lookback: 스윙 감지 윈도우

    Returns:
        ElliottResult — phase, buy_signal, confidence, reason

    사장님 자료 4룰:
      ① fib_zone_match (피보 38.2% 지지)
      ② non_overlap_check (4파 저점 > 1파 고점)
      ③ alternation_check (2파 ↔ 4파 형태 교대)
      ④ wave_4_shape == "sideways" (횡보 매물 소화)
    """
    result = ElliottResult()

    if len(candles) < lookback * 2 + 4:
        result.reason = f"캔들 부족 ({len(candles)}개 < 최소 {lookback*2+4}개)"
        return result

    cur = float(current_price) if current_price is not None else candles[-1]["close"]

    # ── 1) 스윙 포인트 추출 ──
    w1_idx, w2_idx, w3_idx, w4_idx = _identify_waves(candles, lookback=lookback)
    if w1_idx is None or w3_idx is None:
        result.reason = "1파/3파 스윙 미감지"
        return result

    w1_high = candles[w1_idx]["high"]
    w2_low = candles[w2_idx]["low"]
    w3_high = candles[w3_idx]["high"]
    w4_low = candles[w4_idx]["low"] if w4_idx is not None else cur

    result.wave_1_high = w1_high
    result.wave_2_low = w2_low
    result.wave_3_high = w3_high
    result.wave_4_low = w4_low

    # ── 2) 피보나치 38.2% 되돌림 계산 ──
    # 3파 전체 길이 = w3_high - w2_low (보수적 — 1파 시작점 미사용)
    wave_3_length = w3_high - w2_low
    if wave_3_length <= 0:
        result.reason = "3파 길이 음수/0"
        return result

    fib_382 = w3_high - wave_3_length * 0.382
    result.fib_382_level = round(fib_382, 2)

    # ★ 5/22 19:13 사장님 명령 — 4단계 진입 셋팅 (200종 백테스트 검증) ★
    # 단순 38.2% ± tolerance → 4단계 zone 진입 (BUY confidence 1.0/0.7/0.5/0.2)
    check_price = w4_low if w4_idx is not None else cur
    actual_fib_pct = (w3_high - check_price) / wave_3_length * 100.0
    result.fib_retracement_pct = round(actual_fib_pct, 2)

    zone_info = evaluate_fib_zone(actual_fib_pct)
    result.fib_zone_name = zone_info["zone"]
    result.fib_zone_signal = zone_info["signal"]
    # fib_zone_match: BUY 또는 BUY_CAUTION 시 True (AVOID/NO_ENTRY는 False)
    result.fib_zone_match = bool(zone_info["signal"] in ("BUY", "BUY_CAUTION"))

    # ── 3) 파동 중첩 금지 — 4파 저점 > 1파 고점 ──
    result.non_overlap_check = bool(w4_low > w1_high)

    # ── 4) 파동 교대 법칙 ──
    result.wave_2_shape = _classify_wave_shape(candles, w1_idx, w2_idx)
    # 4파 형태: w3 고점 → 현재(마지막 캔들)까지 전체 측정.
    # 사장님 자료 "4파 횡보 매물 소화" = 저점 후 횡보 캔들까지 포함해야 정확.
    result.wave_4_shape = _classify_wave_shape(candles, w3_idx, len(candles) - 1)

    # 교대 = 2파/4파 형태가 서로 다름 ("V_sharp" ↔ "sideways")
    alt_ok = (
        (result.wave_2_shape == "V_sharp" and result.wave_4_shape == "sideways") or
        (result.wave_2_shape == "sideways" and result.wave_4_shape == "V_sharp")
    )
    result.alternation_check = "OK_alternating" if alt_ok else "FAIL_same_shape"

    # ── 5) 현재 파동 단계 추정 ──
    if w4_idx is None:
        # 4파 진행 중 — 현재가가 3파 고점 아래로 떨어진 상태
        if cur < w3_high:
            result.phase = "wave_4_pullback"
        else:
            result.phase = "wave_3_running"  # 아직 3파 진행 중
    else:
        # 4파 저점 형성 후 — 반등 시 5파 진입
        if cur > w4_low * 1.01:  # 4파 저점 대비 +1% 이상 반등
            result.phase = "wave_5_start"
        else:
            result.phase = "wave_4_pullback"

    # ── 6) 매수 시그널 종합 (5/22 19:30 A+C 정밀 백테스트 정정) ──
    # ★ 이전 4룰 (피보+비중첩+교대+횡보) 다 통과 = 성공률 0% (false negative) ★
    # ★ alternation_check + sideways_w4 룰 제거 (baseline 이하) ★
    # 진짜 유효 룰: zone confidence + non_overlap_check 만
    rules_passed = 0.0
    if result.non_overlap_check:
        rules_passed += 1.0
    # fib zone confidence (1.0/0.7/0.4/0.1/0.0)
    rules_passed += zone_info["confidence"]
    # 정보 보존: alt_ok / wave_4_shape는 reason에만 출력 (의사결정 X)
    result.confidence = round(rules_passed / 2.0, 2)

    # 매수 시그널 — wave_5_start + zone BUY/BUY_CAUTION + non_overlap만
    # (교대법칙/횡보 룰 제거 — A+C 검증 결과)
    result.buy_signal = bool(
        result.phase == "wave_5_start"
        and zone_info["signal"] in ("BUY", "BUY_CAUTION")
        and result.non_overlap_check
    )

    # ── 7) reason 작성 (5/22 갱신 — zone 정보 포함) ──
    reasons = []
    reasons.append(
        f"피보 되돌림 {actual_fib_pct:.1f}% [{result.fib_zone_name}] "
        f"signal={result.fib_zone_signal} conf={zone_info['confidence']}"
    )
    if result.non_overlap_check:
        reasons.append(f"파동중첩 금지 OK (w4 {w4_low:,} > w1 {w1_high:,})")
    else:
        reasons.append(f"파동중첩 위반 (w4 {w4_low:,} ≤ w1 {w1_high:,})")
    reasons.append(f"교대법칙 {result.alternation_check} (w2={result.wave_2_shape} / w4={result.wave_4_shape})")
    reasons.append(f"phase={result.phase}")
    result.reason = " / ".join(reasons)

    logger.info(
        f"[elliott_wave] phase={result.phase} fib={actual_fib_pct:.1f}% "
        f"zone={result.fib_zone_name} signal={result.fib_zone_signal} "
        f"buy={result.buy_signal} conf={result.confidence:.2f}"
    )

    return result


# ── 외부 통합 헬퍼 ─────────────────────────────────
def elliott_score_boost(result: ElliottResult, max_boost: int = 25) -> int:
    """엘리어트 분석 결과 → asset_pool 점수 보정 (zone별 차등, 5/22 백테스트 검증).

    A+C 정밀 백테스트 결과 (universe 200종, baseline 20.4%):
      - fib_38_safe   (28~48%): 46.7% 성공 → +25점 (★ 최우선)
      - fib_50_standard (48~62%): 30.8% 성공 → +18점
      - trend_caution (80~100%): 20.8% 성공 + 큰 수익 → +10점 (보수)
      - korean_low (62~80%): 9.1% 성공 → -10점 ★ 패널티 ★ (회피)
      - too_shallow / trend_end: 0점

    asset_pool_loader 호출 패턴:
        result = detect_elliott_pattern(candles)
        score += elliott_score_boost(result)
    """
    if result.buy_signal:
        zone = result.fib_zone_name
        if zone == "fib_38_safe":
            return max_boost      # 25점 (성공률 46.7%)
        elif zone == "fib_50_standard":
            return 18             # 18점 (성공률 30.8%)
        elif zone == "trend_caution":
            return 10             # 10점 (큰 수익 가능, 보수)

    # 회피 zone — 패널티 (asset_pool 점수에서 차감)
    if result.fib_zone_signal == "AVOID":
        return -10  # ★ 5/22 단타봇 정정 — 62~80% 진입 패널티 ★

    # 진행 중 — 미래 4파 형성 기대
    if result.phase == "wave_4_pullback" and result.confidence >= 0.75:
        return 15
    if result.phase == "wave_3_running":
        return 10
    return 0


if __name__ == "__main__":
    # 데모: 인위 캔들 패턴으로 검증
    import json

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

    # 패턴: 1파(100→120) - 2파V(120→105) - 3파(107→160) - 4파횡보(160→147)
    # 각 스윙 포인트가 좌우 3캔들보다 명확히 튀어나오도록 구성
    demo_candles = [
        # 베이스 (인덱스 0~2): 가격 안정
        {"open": 100, "high": 101, "low": 99.5, "close": 100.5, "volume": 1000},
        {"open": 100.5, "high": 102, "low": 100, "close": 101, "volume": 1000},
        {"open": 101, "high": 103, "low": 100.5, "close": 102, "volume": 1000},
        # 1파 상승 진입 (인덱스 3~4)
        {"open": 102, "high": 108, "low": 101.5, "close": 107, "volume": 1500},
        {"open": 107, "high": 115, "low": 106, "close": 113, "volume": 1800},
        # ★ 1파 고점 (인덱스 5) ★
        {"open": 113, "high": 122, "low": 112, "close": 120, "volume": 2200},
        # 2파 V자 하락 진입 (인덱스 6)
        {"open": 120, "high": 120.5, "low": 110, "close": 111, "volume": 1700},
        # ★ 2파 저점 V자 (인덱스 7) ★
        {"open": 111, "high": 112, "low": 105, "close": 106, "volume": 1500},
        # 2파 마감 + 3파 진입 준비 (인덱스 8~9)
        {"open": 106, "high": 110, "low": 105.5, "close": 109, "volume": 1300},
        {"open": 109, "high": 115, "low": 108.5, "close": 114, "volume": 1700},
        # 3파 폭발 상승 (인덱스 10~11)
        {"open": 114, "high": 135, "low": 113, "close": 133, "volume": 3500},
        {"open": 133, "high": 152, "low": 132, "close": 150, "volume": 4500},
        # ★ 3파 고점 (인덱스 12) ★
        {"open": 150, "high": 162, "low": 149, "close": 160, "volume": 5000},
        # 4파 조정 시작 + 횡보 진입 (인덱스 13~14)
        {"open": 160, "high": 161, "low": 153, "close": 154, "volume": 2200},
        {"open": 154, "high": 155, "low": 148, "close": 149, "volume": 1900},
        # ★ 4파 저점 (인덱스 15) — 횡보 ★
        {"open": 149, "high": 150, "low": 146, "close": 147, "volume": 1500},
        # 4파 횡보 지속 (인덱스 16~18)
        {"open": 147, "high": 149, "low": 146.5, "close": 148, "volume": 1400},
        {"open": 148, "high": 149.5, "low": 147, "close": 148.5, "volume": 1300},
        {"open": 148.5, "high": 150, "low": 147.5, "close": 149, "volume": 1400},
    ]

    print("\n=== 엘리어트 데모: 1파/2파(V)/3파/4파(횡보) 패턴 ===")
    result = detect_elliott_pattern(demo_candles)
    print(json.dumps(result.to_dict(), ensure_ascii=False, indent=2))
    print(f"\n점수 부스트: +{elliott_score_boost(result)}점")
