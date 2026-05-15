# -*- coding: utf-8 -*-
"""
Recovery Add-On — 하락 시 자동 분할 추매 시스템 (차트 영웅식)

배경:
    차트 영웅 3주 매매 실험 (2026-02~03) 결과 분석:
    - 1차 매수 후 -10%/-20% 도달 시 자동 분할 추매로 평단 낮추기
    - 3월 폭락(-20% 시장)에서 -2,200만원 손실 → 다음날 14% 급등 → 회복
    - 55종목 중 43종목 수익 (78% 승률)
    - 핵심 = "좋은 종목 + 작은 비중 + 분할 추매로 평단 낮춤"

우리 봇의 기존 ACTION_ADD는 "업사이드 8%+" 즉 상승 추세 시만 작동(피라미드형).
하락 시 평단 낮추기 메커니즘이 부재 → 본 모듈로 보완.

설계 원칙:
    1. 자격: source 화이트리스트 (manual_sync 자동 자격 + day_scan/swing_buy 등)
    2. 트리거: 최초 entry_price 대비 -10% / -20% (config trigger_pcts)
    3. 금액: 보유 평가금액의 50% (config add_amount_ratio)
    4. 최대 횟수: 종목당 2회 (config max_add_on_count)
    5. 안전장치: DANGER+ 차단 / 일일손실 / MDD / 횟수 / 자격
    6. 기본 enabled: false (사장님이 텔레그램으로 ON)

사용:
    decision = evaluate_add_on(pos, current_price, code, trader, bot_config)
    if decision.should_add:
        # 매수 큐에 추가 + record_add_on(pos, decision)
"""
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import List, Tuple

logger = logging.getLogger(__name__)

# manual_sync로 시작하는 source는 자동 자격 (사장님 보호 종목)
PROTECTED_AUTO_ELIGIBLE_PREFIXES: Tuple[str, ...] = ("manual_sync",)

# config eligible_sources의 기본값 (이걸로 시작하면 자격)
DEFAULT_ELIGIBLE_SOURCES: List[str] = [
    "day_scan", "swing_buy", "premove", "user_pick",
    "pipeline", "war_relay", "etf_rec",
]


@dataclass
class AddOnDecision:
    """추매 판정 결과"""
    should_add: bool
    trigger_pct: float        # 도달한 트리거 (-10.0, -20.0)
    add_amount: int           # 추매 금액 (원)
    count: int                # 이번 추매가 N차 (1 또는 2)
    reason: str               # 사유 (텔레그램 알림용)


def load_config(bot_config: dict) -> dict:
    """config.yaml의 bot.recovery_add_on 로드 (기본값 포함)"""
    cfg = (bot_config or {}).get("recovery_add_on", {}) or {}
    return {
        "enabled": bool(cfg.get("enabled", False)),
        "trigger_pcts": list(cfg.get("trigger_pcts", [-10.0, -20.0])),
        "add_amount_ratio": float(cfg.get("add_amount_ratio", 0.50)),
        "max_add_on_count": int(cfg.get("max_add_on_count", 2)),
        "block_risk_levels": list(cfg.get("block_risk_levels", ["DANGER", "CRISIS"])),
        "eligible_sources": list(cfg.get("eligible_sources", DEFAULT_ELIGIBLE_SOURCES)),
        "default_add_amount": int(cfg.get("default_add_amount", 1_000_000)),
        "min_add_amount": int(cfg.get("min_add_amount", 100_000)),
    }


def is_eligible_position(pos: dict, cfg: dict) -> Tuple[bool, str]:
    """추매 자격 종목인지 판정.

    Returns:
        (eligible, reason)
    """
    source = pos.get("source", "")

    # 1순위: manual_sync 자동 자격 (사장님 보호 종목)
    if source.startswith(PROTECTED_AUTO_ELIGIBLE_PREFIXES):
        return True, f"manual_sync 보호 종목 ({source})"

    # 2순위: config 화이트리스트
    for prefix in cfg["eligible_sources"]:
        if prefix and source.startswith(prefix):
            return True, f"화이트리스트: {prefix}"

    return False, f"자격 없음 (source={source!r})"


def _calc_add_amount(pos: dict, code: str, trader, cfg: dict) -> int:
    """추매 금액 계산 (3단 fallback)

    1순위: positions.original_buy_amount × ratio
    2순위: 실잔고 (qty × entry_price) × ratio
    3순위: config.default_add_amount
    """
    ratio = cfg["add_amount_ratio"]

    # 1순위: 최초 매수 금액 (positions에 저장된 경우)
    orig = pos.get("original_buy_amount", 0) or 0
    if orig > 0:
        return int(orig * ratio)

    # 2순위: 실잔고 평가금액 × ratio
    try:
        bal = trader.fetch_balance()
        if bal and bal.get("success"):
            for p in bal.get("positions", []) or []:
                if p.get("code") == code:
                    qty = p.get("qty", 0) or 0
                    entry = pos.get("entry_price", 0) or 0
                    if qty > 0 and entry > 0:
                        held_value = qty * entry
                        return int(held_value * ratio)
                    break
    except Exception as e:
        logger.warning("[recovery_add_on] 잔고 조회 실패 %s: %s", code, e)

    # 3순위: config 기본값
    return cfg["default_add_amount"]


def evaluate_add_on(
    pos: dict, current_price: int, code: str, trader, bot_config: dict
) -> AddOnDecision:
    """추매 판정 (모든 안전장치 통과 시 should_add=True)

    Args:
        pos: positions[code] dict (entry_price, source, add_on_count 등)
        current_price: 현재가
        code: 종목코드
        trader: KISTrader 인스턴스 (잔고 조회용)
        bot_config: config.yaml의 bot 섹션

    Returns:
        AddOnDecision
    """
    cfg = load_config(bot_config)

    # ── 안전장치 1: 시스템 활성화 ──
    if not cfg["enabled"]:
        return AddOnDecision(False, 0.0, 0, 0, "시스템 OFF")

    # ── 안전장치 2: 자격 화이트리스트 ──
    eligible, reason = is_eligible_position(pos, cfg)
    if not eligible:
        return AddOnDecision(False, 0.0, 0, 0, reason)

    # ── 안전장치 3: 가격 유효성 ──
    entry = pos.get("entry_price", 0) or 0
    if entry <= 0 or current_price <= 0:
        return AddOnDecision(False, 0.0, 0, 0, f"가격 무효 (entry={entry}, cp={current_price})")

    # ── 안전장치 4: 최대 횟수 ──
    add_on_count = int(pos.get("add_on_count", 0) or 0)
    max_count = cfg["max_add_on_count"]
    if add_on_count >= max_count:
        return AddOnDecision(False, 0.0, 0, add_on_count, f"최대 {max_count}회 도달")

    # ── 트리거 도달 판정 ──
    pnl_pct = (current_price - entry) / entry * 100.0
    triggers = cfg["trigger_pcts"]
    if add_on_count >= len(triggers):
        return AddOnDecision(False, 0.0, 0, add_on_count, "트리거 정의 부족")
    target_trigger = float(triggers[add_on_count])

    if pnl_pct > target_trigger:
        return AddOnDecision(
            False, target_trigger, 0, add_on_count,
            f"미도달 (PnL {pnl_pct:+.1f}% > {target_trigger:.1f}%)"
        )

    # ── 추매 금액 계산 ──
    add_amount = _calc_add_amount(pos, code, trader, cfg)
    if add_amount < cfg["min_add_amount"]:
        return AddOnDecision(
            False, target_trigger, add_amount, add_on_count,
            f"금액 부족 ({add_amount:,}원 < 최소 {cfg['min_add_amount']:,}원)"
        )

    return AddOnDecision(
        should_add=True,
        trigger_pct=target_trigger,
        add_amount=add_amount,
        count=add_on_count + 1,
        reason=f"PnL {pnl_pct:+.1f}% (목표 {target_trigger:.1f}%) — {add_on_count + 1}차 추매",
    )


def record_add_on(pos: dict, decision: AddOnDecision) -> None:
    """추매 체결 후 positions[code]에 기록.

    호출자가 _save_positions()로 디스크에 저장해야 영구화됨.
    """
    pos["add_on_count"] = decision.count
    history = pos.get("add_on_history", []) or []
    history.append({
        "trigger_pct": decision.trigger_pct,
        "amount": decision.add_amount,
        "count": decision.count,
        "ts": datetime.now().isoformat(timespec="seconds"),
    })
    pos["add_on_history"] = history


def format_alert_message(name: str, code: str, current_price: int, decision: AddOnDecision) -> str:
    """추매 트리거 텔레그램 메시지"""
    return (
        f"📉 추매 트리거 발동: {name}({code})\n"
        f"   현재가: {current_price:,}원\n"
        f"   {decision.reason}\n"
        f"   추매 금액: {decision.add_amount:,}원\n"
        f"   ※ confirm_real_order 활성 시 사장님 확인 필요"
    )
