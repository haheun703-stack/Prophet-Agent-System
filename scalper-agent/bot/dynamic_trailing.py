# -*- coding: utf-8 -*-
"""동적 트레일링 모듈 (★ 5/25 사장님 룰 정확 복귀 ★).

★ 5/25 사장님 영구 룰 (단타봇 13-B 자율 룰 폐기 후 단순화) ★
> "오르는거는 최종 올랐는거에서 힘없이 떨어질때 -3% 되면 팔고 나오자"
> "-7%, -10%, -12%, -15%까지 회귀를 놔둘 필요가 있냐"

영구 룰 [[feedback_trailing_only_tp]] (사장님 5/21 08:25):
  - 고정 +5% TP 비활성 (모든 source)
  - 트레일링만 — 추세 끝까지 추적

★ 5/25 변경 사항 ★:
  - 13-B 동적 트레일링 (-3/-5/-7/-10) 폐기 — 단타봇 5/22 자율 설계 사장님 룰 위반
  - 13-E 상한가 직전 (-12/-15) 폐기 — limit_up_split_sell 모듈로 이관
  - ★ 모든 zone 고점 -3% 일관 ★

새 룰:
  - 매입~+3% : 비활성 (NORMAL SL = 매입가 -3%)
  - +3%+ : 트레일링 활성 → 고점 -3% 일관 추적
  - 상한가 (+25%+) : ★ limit_up_split_sell 모듈로 분리 ★
                    (절반 +29% 매도 + 절반 NXT 이월 + D+1 트레일링)

영구 메모리: [[feedback_trailing_only_tp]] + 5/25 사장님 룰 1
"""
import logging
from dataclasses import dataclass
from typing import Dict, Literal, Optional, Sequence

logger = logging.getLogger(__name__)


# ── 트렌드 강도 ─────────────────────────────
TrendStrength = Literal["weak", "moderate", "strong", "limit_up_imminent"]


@dataclass
class TrailingDecision:
    """동적 트레일링 결정.

    Attributes:
        trail_pct: 트레일링 추적 폭 (%, 양수)
        activation_pct: 트레일링 활성 임계값 (%, 양수)
        strength: 추세 강도 분류
        reason: 결정 사유 (사람 읽기용)
        disable_fixed_tp: 고정 TP 비활성 여부 (항상 True — 영구 룰)
    """
    trail_pct: float
    activation_pct: float
    strength: TrendStrength
    reason: str
    disable_fixed_tp: bool = True


def classify_trend_strength(
    pnl_pct: float,
    five_min_bars: Optional[Sequence[Dict]] = None,
    flow_intensity: Optional[float] = None,
    vwap_position: Optional[str] = None,
) -> TrendStrength:
    """추세 강도 분류 — 현재 수익률 + 5분봉 패턴 + 체결강도 + VWAP 위치.

    Args:
        pnl_pct: 현재 평가손익 % (예: +12.73)
        five_min_bars: 5분봉 OHLCV (선택 — 추세 강화 신호 확인)
        flow_intensity: 체결강도 (0~200, 100이 중립, 100+ = 강세)
        vwap_position: "above" / "below" / None

    Returns:
        "weak" / "moderate" / "strong" / "limit_up_imminent"

    분류 룰:
        +25%+ → limit_up_imminent (트레일링 OFF, 상한가 임박)
        +10%+ 또는 (5분봉 3연속 양봉 + flow_intensity ≥ 100 + vwap above) → strong
        +5~10% → moderate
        그 외 → weak
    """
    # 상한가 임박 (+25% 이상) — 최우선
    if pnl_pct >= 25.0:
        return "limit_up_imminent"

    # 강한 추세 = +10%+ 단독 OR (+5~10% + 추세 강화 시그널 3종 모두)
    if pnl_pct >= 10.0:
        return "strong"

    consec_bullish = False
    if five_min_bars and len(five_min_bars) >= 3:
        last3 = list(five_min_bars)[-3:]
        consec_bullish = all(b["close"] > b["open"] for b in last3)

    flow_strong = (flow_intensity is not None and flow_intensity >= 100.0)
    vwap_above = (vwap_position == "above")

    if pnl_pct >= 5.0 and consec_bullish and flow_strong and vwap_above:
        return "strong"

    if pnl_pct >= 5.0:
        return "moderate"

    return "weak"


def decide_trailing(
    pnl_pct: float,
    five_min_bars: Optional[Sequence[Dict]] = None,
    flow_intensity: Optional[float] = None,
    vwap_position: Optional[str] = None,
) -> TrailingDecision:
    """트레일링 결정 — ★ 5/25 사장님 룰 ★ 고점 -3% 일관.

    Args:
        pnl_pct: 현재 평가손익 % (양수 = 수익)
        five_min_bars: 5분봉 OHLCV (선택 — strength 분류용)
        flow_intensity: 체결강도 (선택)
        vwap_position: VWAP 위치 (선택)

    Returns:
        TrailingDecision (trail_pct=3.0, activation_pct=3.0, strength, reason)

    ★ 5/25 사장님 룰 ★:
        매입~+3%        : 비활성 (NORMAL SL = 매입가 -3%, dynamic_trailing 미적용)
        +3%+ (전 zone) : activation +3%, trail -3% ★ 고점 -3% 일관 ★
        상한가 (+25%+) : limit_up_split_sell 모듈로 분리 (절반 +29% + 절반 D+1 이월)

    strength 분류는 다른 모듈에서 활용 (학습/로그) 가능 — trail_pct는 일관.
    """
    strength = classify_trend_strength(pnl_pct, five_min_bars, flow_intensity, vwap_position)

    # ★ 5/25 사장님 영구 룰 ★ 모든 zone 고점 -3% 일관
    # 13-B 동적 (-3/-5/-7/-10) + 13-E 상한가 직전 (-12/-15) ★ 폐기 ★
    # 사장님: "-7%, -10%, -12%, -15%까지 회귀를 놔둘 필요가 있냐"
    trail = 3.0
    activation = 3.0

    if strength == "limit_up_imminent":
        # 상한가 +25%+ — limit_up_split_sell 모듈에서 처리 (참고용 reason만)
        reason = f"상한가 임박 (+{pnl_pct:.2f}%) → limit_up_split_sell 처리 (절반 매도 + 절반 D+1 이월)"
    elif strength == "strong":
        reason = f"강한 추세 (+{pnl_pct:.2f}%) → 고점 -3% (사장님 5/25 일관 룰)"
    elif strength == "moderate":
        reason = f"중간 추세 (+{pnl_pct:.2f}%) → 고점 -3% (사장님 5/25 일관 룰)"
    else:  # weak
        reason = f"약한 추세 (+{pnl_pct:.2f}%) → 고점 -3% (사장님 5/25 일관 룰)"

    decision = TrailingDecision(
        trail_pct=trail,
        activation_pct=activation,
        strength=strength,
        reason=reason,
        disable_fixed_tp=True,  # 사장님 영구 룰 [[feedback_trailing_only_tp]]
    )

    logger.info(
        f"[dynamic_trailing] pnl={pnl_pct:.2f}% strength={strength} "
        f"trail=-{trail:.1f}% activation=+{activation:.1f}% (★ 5/25 사장님 일관 룰 ★)"
    )
    return decision


def compute_trailing_sl(
    high_watermark: float,
    decision: TrailingDecision,
) -> int:
    """고점 워터마크 + 트레일링 폭 → 동적 SL 가격 계산.

    Args:
        high_watermark: 매수 후 도달한 최고가
        decision: decide_trailing 결과

    Returns:
        새 SL 가격 (정수, 호가단위 무시 — 단타봇이 호가 단위 보정)
    """
    sl_price = high_watermark * (1 - decision.trail_pct / 100.0)
    return int(sl_price)


# ── 사용 예시 통합 헬퍼 ─────────────────────────
def should_activate_trailing(pnl_pct: float, decision: Optional[TrailingDecision] = None) -> bool:
    """트레일링 활성 여부 — pnl이 activation_pct 도달했는지."""
    if decision is None:
        decision = decide_trailing(pnl_pct)
    return pnl_pct >= decision.activation_pct


if __name__ == "__main__":
    import json

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

    print("\n=== 동적 트레일링 데모 ===\n")

    # 5/22 기가레인 사례 — 매수 1,303 / 09:19 1,418 (+8.83%) → 매도. 진짜는 +12.73% 까지 갔음.
    # 만약 동적 트레일링 적용했다면? +12.73%에서 strong 분류
    print("【기가레인 9:19 시점 (+8.83%)】")
    d1 = decide_trailing(pnl_pct=8.83)
    print(f"  → {d1.reason}")
    print(f"  → strength={d1.strength}, trail=-{d1.trail_pct}%, activation=+{d1.activation_pct}%")
    print(f"  → 고점 1,418 → SL {compute_trailing_sl(1418, d1):,}원")

    print()
    print("【아나패스 9:11 시점 (+2.07%)】")
    d2 = decide_trailing(pnl_pct=2.07)
    print(f"  → {d2.reason}")
    print(f"  → strength={d2.strength}, trail=-{d2.trail_pct}%, activation=+{d2.activation_pct}%")
    print(f"  → 아직 활성 X (pnl {2.07}% < activation {d2.activation_pct}%)")

    print()
    print("【LG전자 9:02 시점 (+19.91%) — 정상 큰 익절】")
    d3 = decide_trailing(pnl_pct=19.91)
    print(f"  → {d3.reason}")
    print(f"  → strength={d3.strength}, trail=-{d3.trail_pct}%, activation=+{d3.activation_pct}%")
    print(f"  → 고점 235,500 → SL {compute_trailing_sl(235500, d3):,}원")

    print()
    print("【상한가 임박 시뮬 (+26%)】")
    d4 = decide_trailing(pnl_pct=26.0)
    print(f"  → {d4.reason}")
    print(f"  → strength={d4.strength}, trail=-{d4.trail_pct}%, activation=+{d4.activation_pct}%")

    print()
    print("【추세 강화 시그널 모두 통과 (+6% + 3연속 양봉 + 체결강도 110 + VWAP 위)】")
    bars = [
        {"open": 100, "high": 103, "low": 99, "close": 102, "volume": 1000},
        {"open": 102, "high": 105, "low": 101, "close": 104, "volume": 1500},
        {"open": 104, "high": 107, "low": 103, "close": 106, "volume": 2000},
    ]
    d5 = decide_trailing(pnl_pct=6.0, five_min_bars=bars, flow_intensity=110.0, vwap_position="above")
    print(f"  → {d5.reason}")
    print(f"  → strength={d5.strength} (moderate 대신 strong 강화)")
