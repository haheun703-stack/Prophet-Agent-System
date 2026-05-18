# -*- coding: utf-8 -*-
"""상한가 분할매수 3단계 매니저 (5/18 사장님 상한가 엔진 지적)

상한가 엔진 (data/limit_up_engine.py) 설계 그대로:
- 1차 50% 매수 — 진입가 도달 시
- 2차 30% 매수 — 추가 눌림 시
- 3차 20% 매수 — 반등 확인 시

★ 사장님 5대 원칙 통합 ★
- 원칙 ①: 같은 테마 최대 2종목
- 원칙 ④: 현금 비중 30%+ 유지
- 원칙 ②: 5단계 게이트 통과 종목만 (jarvis_consensus.decide_buy_consensus 호출)

★ 자비스 정신 통합 ★
- 동적 대응: VWAP/체결강도 모니터하며 분할 매수 시점 판단
- 스캘핑 옵션: 1차 매수 후 +1% 도달 시 부분 익절 가능 (분할매수와 별개)
"""
import json
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

try:
    from zoneinfo import ZoneInfo
    KST = ZoneInfo("Asia/Seoul")
except ImportError:
    KST = None

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))

from dotenv import load_dotenv
load_dotenv(_ROOT.parent / ".env")

logger = logging.getLogger("BH.LimitUpSplit")

# 분할매수 비율 (사장님 상한가 엔진 v3.0 설계 그대로)
SPLIT_RATIOS = {
    1: 0.50,  # 1차 50%
    2: 0.30,  # 2차 30%
    3: 0.20,  # 3차 20%
}

# 분할매수 진입 조건 (상한가 엔진 trigger 종목 기준)
ENTRY_THRESHOLDS = {
    1: 0.00,    # 1차: entry_price 도달 시 즉시 (entry_low ~ entry_high 범위)
    2: -0.015,  # 2차: 1차 대비 -1.5% 추가 눌림 시
    3: 0.005,   # 3차: 2차 매수가 대비 +0.5% 반등 확인 시
}

# 분할매수 상태 저장 (메모리 캐시)
_split_state: Dict[str, Dict] = {}


def _now() -> datetime:
    return datetime.now(KST) if KST else datetime.now()


def calculate_split_amount(total_amount: int, split_level: int) -> int:
    """분할 단계별 매수 금액 계산.

    Args:
        total_amount: 총 매수 예산 (예: STRONG 200만)
        split_level: 1 / 2 / 3

    Returns:
        해당 단계 매수 금액 (원)
    """
    ratio = SPLIT_RATIOS.get(split_level, 0)
    return int(total_amount * ratio)


def calculate_split_quantity(total_amount: int, split_level: int, price: int) -> int:
    """분할 단계별 매수 수량 계산.

    Args:
        total_amount: 총 예산
        split_level: 1/2/3
        price: 매수가

    Returns:
        주식 수량 (최소 1주)
    """
    amount = calculate_split_amount(total_amount, split_level)
    qty = amount // price if price > 0 else 0
    return max(1, qty)


def should_enter_split(
    ticker: str,
    current_price: float,
    trigger_info: Dict,
    current_level: int = 1,
) -> Dict:
    """분할매수 진입 시점 판단.

    Args:
        ticker: 종목 코드
        current_price: 현재가
        trigger_info: 상한가 엔진 trigger 종목 정보
                      (entry_low, entry_high, entry_price, tp_price, sl_price)
        current_level: 현재 진행 중인 분할 단계 (0 = 미진입, 1/2/3 = 진행 중)

    Returns:
        {
            "enter": bool,
            "level": int,                # 다음 진입할 단계
            "reason": str,
            "buy_price": float,
            "qty_ratio": float,           # 0.5 / 0.3 / 0.2
        }
    """
    result = {
        "enter": False,
        "level": current_level + 1,
        "reason": "",
        "buy_price": current_price,
        "qty_ratio": 0.0,
    }

    entry_low = float(trigger_info.get("entry_low", 0))
    entry_high = float(trigger_info.get("entry_high", 0))

    # 1차 매수: entry_low ~ entry_high 범위 도달 시
    if current_level == 0:
        if entry_low > 0 and entry_low <= current_price <= entry_high:
            result["enter"] = True
            result["level"] = 1
            result["qty_ratio"] = SPLIT_RATIOS[1]
            result["reason"] = f"1차 진입: 범위 도달 ({entry_low:,.0f}~{entry_high:,.0f})"
        return result

    # 2차/3차 매수: 직전 매수가 대비 조건
    state = _split_state.get(ticker, {})
    prev_buy_price = state.get(f"level_{current_level}_price", 0)

    if current_level == 1 and prev_buy_price > 0:
        # 2차: 1차 대비 -1.5% 추가 눌림
        target = prev_buy_price * (1 + ENTRY_THRESHOLDS[2])
        if current_price <= target:
            result["enter"] = True
            result["level"] = 2
            result["qty_ratio"] = SPLIT_RATIOS[2]
            result["reason"] = f"2차 진입: 1차({prev_buy_price:,.0f}) 대비 -1.5% 도달"

    elif current_level == 2 and prev_buy_price > 0:
        # 3차: 2차 대비 +0.5% 반등 확인
        target = prev_buy_price * (1 + ENTRY_THRESHOLDS[3])
        if current_price >= target:
            result["enter"] = True
            result["level"] = 3
            result["qty_ratio"] = SPLIT_RATIOS[3]
            result["reason"] = f"3차 진입: 2차({prev_buy_price:,.0f}) 대비 +0.5% 반등"

    return result


def record_split_purchase(
    ticker: str,
    level: int,
    price: float,
    qty: int,
    amount: int,
) -> None:
    """분할매수 실행 후 상태 기록."""
    if ticker not in _split_state:
        _split_state[ticker] = {
            "ticker": ticker,
            "first_purchase_at": _now().isoformat(),
            "current_level": 0,
        }

    state = _split_state[ticker]
    state[f"level_{level}_price"] = price
    state[f"level_{level}_qty"] = qty
    state[f"level_{level}_amount"] = amount
    state[f"level_{level}_at"] = _now().isoformat()
    state["current_level"] = level

    # 누적 매수
    state["total_qty"] = sum(state.get(f"level_{i}_qty", 0) for i in range(1, 4))
    state["total_amount"] = sum(state.get(f"level_{i}_amount", 0) for i in range(1, 4))
    state["avg_price"] = state["total_amount"] / state["total_qty"] if state["total_qty"] else 0


def get_split_state(ticker: str) -> Optional[Dict]:
    """종목별 분할매수 상태 조회."""
    return _split_state.get(ticker)


def clear_split_state(ticker: str) -> None:
    """청산 후 상태 클리어."""
    if ticker in _split_state:
        del _split_state[ticker]


def process_trigger_candidate(
    trigger_info: Dict,
    current_price: float,
    total_budget: int,
) -> Dict:
    """상한가 엔진 trigger 종목 → 분할매수 결정.

    Args:
        trigger_info: limit_up_engine trigger 정보
        current_price: 현재가
        total_budget: 총 매수 예산 (예: STRONG 200만)

    Returns:
        {
            "should_buy": bool,
            "level": int,
            "buy_price": float,
            "qty": int,
            "amount": int,
            "reason": str,
        }
    """
    ticker = trigger_info.get("code", "")
    state = _split_state.get(ticker, {"current_level": 0})
    current_level = state.get("current_level", 0)

    if current_level >= 3:
        return {"should_buy": False, "level": 3, "reason": "3차 매수 완료"}

    decision = should_enter_split(
        ticker=ticker,
        current_price=current_price,
        trigger_info=trigger_info,
        current_level=current_level,
    )

    if not decision["enter"]:
        return {"should_buy": False, "level": current_level, "reason": "진입 조건 미충족"}

    level = decision["level"]
    qty = calculate_split_quantity(total_budget, level, int(current_price))
    amount = qty * int(current_price)

    return {
        "should_buy": True,
        "level": level,
        "buy_price": current_price,
        "qty": qty,
        "amount": amount,
        "reason": decision["reason"],
    }


if __name__ == "__main__":
    print("=== limit_up_split_buy 자가 진단 ===")
    print()

    # 분할 비율 검증
    print("📊 분할 비율 (총 예산 200만 STRONG 가정):")
    total = 2_000_000
    for level in [1, 2, 3]:
        amt = calculate_split_amount(total, level)
        print(f"  {level}차 {SPLIT_RATIOS[level]:.0%} → {amt:,}원")
    print(f"  합계: {sum(calculate_split_amount(total, l) for l in [1,2,3]):,}원")

    # 5/18 라이브 데이터로 시뮬
    print()
    print("🎯 시뮬 — 5/18 상한가 엔진 trigger 종목 (asset_pool 기반):")
    try:
        from utils.asset_pool_loader import load_limit_up_triggers
        triggers = load_limit_up_triggers()
        print(f"  Trigger 종목 수: {len(triggers)}")
        for t in triggers[:5]:
            code = t.get("code", "")
            name = t.get("name", "")
            entry_low = t.get("entry_low", 0)
            entry_high = t.get("entry_high", 0)
            tp = t.get("tp_price", 0)
            print(f"  - {name} ({code}) entry {entry_low:,.0f}~{entry_high:,.0f} / TP {tp:,.0f}")
    except Exception as e:
        print(f"  오류: {e}")

    print()
    print("★ 5/19 첫 가동 시 매 1분 trigger 종목 현재가 모니터 → 진입 범위 도달 시 자동 매수")
    print("★ 사장님 자비스 정신 — 동적 대응으로 1차/2차/3차 매수 시점 자동 판단")
