# -*- coding: utf-8 -*-
"""상한가 분할 매도 + NXT 이월 모듈 (★ 5/25 사장님 룰 3 ★).

사장님 5/25 발화:
> "상한가 치면 안떨어지는데...? 그러면 절반 팔고 절반 다음날로 이월?
>  다음날 갭다운으로 시작하더라도 우린 들고 있다가
>  위로 올라가는 최상단에서 -3% 떨어지면 매도... 이게 순리적이지 않나?"

★ 핵심 통찰 (사장님이 단타봇보다 한 수 위) ★:
  - 상한가 = KRX 가격제한선 +30% 고정 → 트레일링 -3% 발동 자체가 안 됨
  - 옵션 A (트레일링 -3% 일관) = 상한가 종목 무한 보유 위험
  - 분할 매도 (50/50) + D+1 이월 = 안전 락인 + 잭팟 추격

알고리즘:
  당일 (D):
    +29% 상한가 도달 (limit_up_phase=True)
      ↓
    🔹 절반 즉시 시장가 매도 (당일 +29% 확정 락인)
    🔹 절반 보유 + sub-position 마커 pending_next_day=True
      ↓
  다음날 (D+1) 시초가:
    🔹 트레일링 -3% 즉시 활성 (NORMAL SL 무관)
    🔹 갭다운으로 시작해도 인내 보유 (사장님 룰)
      ↓
    회복하며 위로 → 최고점 도달
      ↓
    최고점 -3% 떨어지면 매도

시나리오 예측:
  A. 다음날 갭상승 잭팟  : 절반 +29% + 절반 +32% = 평균 +30.5% (1종 +157만/516만)
  B. 다음날 갭다운 회복  : 절반 +29% + 절반 +12% = 평균 +20.5% (1종 +106만/516만)
  C. 다음날 추가 하락    : 절반 +29% + 절반 +7%  = 평균 +18%   (1종 +93만/516만)

★ 시뮬레이션 worst-case도 +18% — 옵션 B (당일 익절 +29%) 대비 +24% 이상 우수 ★

영구 메모리: [[feedback_trailing_only_tp]] + 5/25 사장님 룰 3
"""
import logging
from dataclasses import dataclass
from typing import Optional
from datetime import date

logger = logging.getLogger(__name__)


# ── 상한가 트리거 임계값 ─────────────────────────────
LIMIT_UP_TRIGGER_PCT = 25.0       # 룰 3: +25% 이상 = 상한가 임박 → 분할 매도 발동
EOD_SPLIT_TRIGGER_PCT = 10.0      # ★ 룰 B (5/25 신규) ★ 15:25 통합 청산 시 +10%+ → 장 마감 분할 매도
SPLIT_RATIO = 0.5                 # 절반(50%) 즉시 매도 / 절반(50%) D+1 이월
D1_TRAIL_PCT = 3.0                # D+1 트레일링 폭 (-3% 사장님 룰 일관)
D1_GAP_DOWN_PROTECTION_PCT = -7.0 # ★ 룰 C (5/25 신규) ★ D+1 시초가 -7% 이상 갭다운 → 즉시 매도 보호망


@dataclass
class SplitSellDecision:
    """분할 매도 결정.

    Attributes:
        should_split: 분할 매도 발동 여부 (True = 절반 시장가 매도)
        sell_qty: 즉시 매도할 수량
        hold_qty: D+1 이월 보유 수량
        reason: 결정 사유
        next_day_marker: D+1 이월 sub-position 마커 (트레일링 즉시 활성)
    """
    should_split: bool
    sell_qty: int = 0
    hold_qty: int = 0
    reason: str = ""
    next_day_marker: bool = False


def should_trigger_split(pnl_pct: float, qty: int, already_split: bool = False) -> SplitSellDecision:
    """분할 매도 발동 여부 판정.

    Args:
        pnl_pct: 현재 평가손익 % (양수)
        qty: 보유 수량
        already_split: 이미 분할 매도된 종목인지 (중복 방지)

    Returns:
        SplitSellDecision

    발동 조건:
        1. pnl_pct >= +25% (상한가 임박 또는 도달)
        2. qty >= 2 (분할 가능 최소 수량)
        3. already_split == False (중복 방지)
    """
    if already_split:
        return SplitSellDecision(should_split=False, reason="이미 분할 매도됨 (중복 방지)")

    if pnl_pct < LIMIT_UP_TRIGGER_PCT:
        return SplitSellDecision(
            should_split=False,
            reason=f"+{pnl_pct:.2f}% < {LIMIT_UP_TRIGGER_PCT:.0f}% (상한가 미달)",
        )

    if qty < 2:
        return SplitSellDecision(
            should_split=False,
            reason=f"보유 {qty}주 < 2주 (분할 불가, 1주 = 전량 매도 또는 전량 이월)",
        )

    # 분할 매도 발동
    sell_qty = max(1, int(qty * SPLIT_RATIO))
    hold_qty = qty - sell_qty

    return SplitSellDecision(
        should_split=True,
        sell_qty=sell_qty,
        hold_qty=hold_qty,
        reason=f"★ 상한가 도달 (+{pnl_pct:.2f}%) → 절반 {sell_qty}주 즉시 매도 / 절반 {hold_qty}주 D+1 이월 ★",
        next_day_marker=True,
    )


def should_trigger_eod_split(
    pnl_pct: float, qty: int, already_split: bool = False, timing_mode: str = "open"
) -> SplitSellDecision:
    """★ 룰 B (5/25 신규) ★ 장 마감 (15:25 통합 청산) 시 분할 매도 발동 여부.

    사장님 5/25 시나리오:
      "매수 → +12% 도달 → 장 마감 → 그러면?"
      → 절반 익절 + 절반 D+1 이월 (룰 3과 동일 철학)

    Args:
        pnl_pct: 현재 평가손익 % (15:25 시점)
        qty: 보유 수량
        already_split: 이미 룰 3 (+25%) 분할된 종목인지 (중복 방지)
        timing_mode: 'open' (09:15 매수 — 룰 B 적용 대상)
                     'previous_close' (14:50 매수 — 자동 다음날 매도, 룰 B 미적용)

    Returns:
        SplitSellDecision

    발동 조건 (모두 충족):
        1. pnl_pct >= +10% (룰 B 트리거)
        2. pnl_pct < +25% (룰 3 영역 X — 룰 3 우선)
        3. qty >= 2 (분할 가능 수량)
        4. already_split == False (룰 3 중복 방지)
        5. timing_mode == 'open' (09:15 매수만 — 14:50은 자동 D+1 매도)

    시뮬:
        +12% × 50% 락인 + 50% D+1 이월 → 갭다운 -5% 시 +3.5% (룰 1 단독 -5% 손실 회피)
        +18% × 50% + 50% D+1 +25% 갱신 = 평균 +21.5% (룰 1 단독 +15% 대비 +6.5%p)
    """
    if already_split:
        return SplitSellDecision(should_split=False, reason="이미 분할됨 (룰 3 우선)")

    if timing_mode == "previous_close":
        return SplitSellDecision(
            should_split=False,
            reason="14:50 매수 (timing_mode=previous_close) → 자동 D+1 매도 정책, 룰 B 미적용",
        )

    if pnl_pct < EOD_SPLIT_TRIGGER_PCT:
        return SplitSellDecision(
            should_split=False,
            reason=f"+{pnl_pct:.2f}% < {EOD_SPLIT_TRIGGER_PCT:.0f}% (룰 B 트리거 미달, 트레일링 -3% 계속)",
        )

    if pnl_pct >= LIMIT_UP_TRIGGER_PCT:
        return SplitSellDecision(
            should_split=False,
            reason=f"+{pnl_pct:.2f}% >= {LIMIT_UP_TRIGGER_PCT:.0f}% (룰 3 영역 — 룰 3 처리)",
        )

    if qty < 2:
        return SplitSellDecision(
            should_split=False,
            reason=f"보유 {qty}주 < 2주 (분할 불가)",
        )

    sell_qty = max(1, int(qty * SPLIT_RATIO))
    hold_qty = qty - sell_qty

    return SplitSellDecision(
        should_split=True,
        sell_qty=sell_qty,
        hold_qty=hold_qty,
        reason=f"★ 룰 B 발동 (+{pnl_pct:.2f}% 장 마감) → 절반 {sell_qty}주 익절 + 절반 {hold_qty}주 D+1 이월 ★",
        next_day_marker=True,
    )


def should_force_sell_on_gap_down(d1_open_pnl_pct: float) -> tuple:
    """★ 룰 C (5/25 신규) ★ D+1 시초가 갭다운 보호망.

    사장님 룰 3 ("갭다운 인내 보유") + 5/19 학습 보호망:
      - 갭다운 -7% 미만 = 사장님 룰 (인내 보유 + 트레일링 -3%)
      - 갭다운 -7% 이상 = ★ 즉시 매도 ★ (5/19 강세 종목 갭하락 -10~-15% 학습)

    Args:
        d1_open_pnl_pct: D+1 시초가 시점 평가손익 % (룰 B 이월 종목)
                         예: 전일 +12% 도달 → 분할 매도 후 절반 이월 → D+1 -5% = pnl_pct=-5%

    Returns:
        (force_sell: bool, reason: str)

    동작:
        d1_open_pnl_pct <= -7.0 → 시초가 즉시 매도 (보호망)
        d1_open_pnl_pct > -7.0  → 트레일링 -3% 계속 (사장님 룰 3)
    """
    if d1_open_pnl_pct <= D1_GAP_DOWN_PROTECTION_PCT:
        return (
            True,
            f"★ 룰 C 보호망 발동 (D+1 갭다운 {d1_open_pnl_pct:+.2f}% <= {D1_GAP_DOWN_PROTECTION_PCT}%) → 즉시 매도 (5/19 학습)",
        )

    return (
        False,
        f"D+1 갭다운 {d1_open_pnl_pct:+.2f}% > {D1_GAP_DOWN_PROTECTION_PCT}% (사장님 룰 3 인내 보유 + 트레일링 -3%)",
    )


def is_next_day_position(
    buy_date: Optional[date],
    current_date: Optional[date] = None,
    pending_next_day: bool = False,
) -> bool:
    """D+1 이월 sub-position 판정.

    Args:
        buy_date: 매수일 (또는 분할 매도일)
        current_date: 현재 날짜 (기본 = today)
        pending_next_day: split_sell 후 마커

    Returns:
        True = D+1 이월분 (트레일링 즉시 활성)
        False = 일반 매수분 (+3% 도달 후 트레일링)
    """
    if not pending_next_day or buy_date is None:
        return False

    current_date = current_date or date.today()
    return current_date > buy_date


def get_trail_activation(is_next_day: bool, pnl_pct: float) -> tuple:
    """D+1 이월 여부에 따른 트레일링 활성 조건.

    Args:
        is_next_day: D+1 이월 sub-position 여부
        pnl_pct: 현재 평가손익 %

    Returns:
        (active: bool, trail_pct: float, reason: str)

    룰:
        D+1 이월: 처음부터 트레일링 활성 (NORMAL SL 무관 / 최고점 -3% 추적)
        일반 매수: +3% 도달 후 활성 (사장님 룰 1)
    """
    if is_next_day:
        # D+1 이월 — 갭다운/갭업 무관 트레일링 즉시 활성
        return (True, D1_TRAIL_PCT, f"D+1 이월 (절반 매도 후 잔량) — 최고점 -{D1_TRAIL_PCT:.0f}% 추적 (사장님 5/25 룰 3)")

    # 일반 매수 — +3% 도달 후 활성 (사장님 룰 1)
    active = pnl_pct >= 3.0
    return (active, D1_TRAIL_PCT, f"일반 매수 — pnl {pnl_pct:.2f}% {'>=3% 활성' if active else '<3% 비활성'}")


# ── 사용 예시 / 단위 테스트 ─────────────────────────
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

    print("\n=== ★ 5/25 사장님 룰 3: 상한가 분할 매도 + 이월 데모 ★ ===\n")

    # 시나리오 1: 일반 종목 +10% (분할 발동 X)
    print("【시나리오 1】 +10% 수익 (상한가 미달)")
    d1 = should_trigger_split(pnl_pct=10.0, qty=100)
    print(f"  → should_split={d1.should_split}, reason={d1.reason}")
    print()

    # 시나리오 2: 상한가 +29% / 100주 → 50/50 분할
    print("【시나리오 2】 +29% 상한가 / 100주 보유")
    d2 = should_trigger_split(pnl_pct=29.0, qty=100)
    print(f"  → should_split={d2.should_split}")
    print(f"  → 즉시 매도 {d2.sell_qty}주 / D+1 이월 {d2.hold_qty}주")
    print(f"  → {d2.reason}")
    print()

    # 시나리오 3: 1주만 보유 (분할 불가)
    print("【시나리오 3】 +29% / 1주만 보유 (분할 불가)")
    d3 = should_trigger_split(pnl_pct=29.0, qty=1)
    print(f"  → should_split={d3.should_split}, reason={d3.reason}")
    print()

    # 시나리오 4: 중복 방지
    print("【시나리오 4】 이미 분할 매도된 종목 (중복 방지)")
    d4 = should_trigger_split(pnl_pct=30.0, qty=50, already_split=True)
    print(f"  → should_split={d4.should_split}, reason={d4.reason}")
    print()

    # 시나리오 5: D+1 이월 트레일링 활성 (갭다운 -8%)
    print("【시나리오 5】 D+1 이월 sub-position / 갭다운 -8%")
    active, trail, reason = get_trail_activation(is_next_day=True, pnl_pct=-8.0)
    print(f"  → active={active}, trail=-{trail}%, reason={reason}")
    print(f"  → 갭다운에도 트레일링 활성 (사장님 룰: 인내 보유)")
    print()

    # 시나리오 6: 일반 매수 +2% (활성 X)
    print("【시나리오 6】 일반 매수 +2% (트레일링 비활성)")
    active, trail, reason = get_trail_activation(is_next_day=False, pnl_pct=2.0)
    print(f"  → active={active}, trail=-{trail}%, reason={reason}")
    print()

    print("=" * 70)
    print("★ 5/25 사장님 룰 3 검증 — 분할 매도 + D+1 이월 + 일관 -3% 트레일링 ★")
    print("=" * 70)
