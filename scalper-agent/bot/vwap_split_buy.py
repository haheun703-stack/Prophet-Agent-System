# -*- coding: utf-8 -*-
"""★ 사장님 5/26 통찰 — VWAP + 3분할 지정가 매수 (50/30/20) ★

사장님 명령 (5/26 19:30):
> "VWAP 상/중/하로 진행하면서 예약매수 지정가로 걸어놓고 매수
>  ... 미체결 위험 = 어쩔 수 없이 시장가 폴백 (1차만)
>  ... 분할 비율 50/30/20"

기존 trade-off:
  - 시장가 = 즉시 체결 / 추격 매수 (사장님 -6.95% 빠짐)
  - 지정가 = 정확 가격 / 도달 안 하면 미체결
  - ★ 해결: 1차 chase + 시장가 폴백 (강한 종목) + 2/3차 chase only (눌림 기다림) ★

분할 비율 (사장님 5/26 결정):
  1차 50%: 시초가 chase_buy (60초) + 미체결 시 시장가 폴백 ★ 진입 보장 ★
  2차 30%: VWAP 또는 -1.5% 눌림 chase_buy (3분) / 미체결 시 종료
  3차 20%: -3% 눌림 chase_buy (마감까지) / 미체결 시 종료 (깊은 눌림 보너스)

시나리오 결과:
  A. 강한 추세 (삼화 +30%): 1차만 50% 진입 → +30% 수익 (안 놓침)
  B. 평이 추세 (켄코아 +20%): 1차+2차 80% 진입 → 평균가 양호
  C. 깊은 눌림 (사장님 5/23 룰): 100% 진입 → 최저 평균가

영구 메모리:
  - [project_pullback_entry_5_23] (5/23 눌림 매수 룰)
  - [feedback_trailing_only_tp] (5/21 트레일링만)
  - [feedback_president_manual_sell_backup] (사장님 매수 보호)
  - 5/26 새 룰: 1차 시장가 폴백 OK (사장님 통찰)
"""
import asyncio
import logging
from dataclasses import dataclass

logger = logging.getLogger(__name__)


def _resolve_order_gate(trader):
    """Return a real KISTrader order gate, ignoring MagicMock auto-attrs."""
    gate = getattr(type(trader), "_order_gate", None)
    if callable(gate):
        return getattr(trader, "_order_gate")
    instance_gate = getattr(getattr(trader, "__dict__", {}), "get", lambda *_: None)("_order_gate")
    if callable(instance_gate):
        return instance_gate
    return None


try:
    from bot.trade_kill_switch import is_auto_trade_disabled
except Exception:
    def is_auto_trade_disabled() -> bool:
        try:
            from data.sajang_rules import SAJANG

            return bool(getattr(SAJANG, "AUTO_TRADE_DISABLED", False))
        except Exception:
            return False


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# ★ 사장님 5/26 분할 비율 ★
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
SPLIT_RATIO_1 = 0.50   # 1차 시초가 (진입 보장 + 시장가 폴백)
SPLIT_RATIO_2 = 0.30   # 2차 VWAP/-1.5% 눌림 (chase only)
SPLIT_RATIO_3 = 0.20   # 3차 -3% 눌림 보너스 (chase only)

# 단계별 chase 시간
CHASE_WAIT_1 = 60      # 1차 60초 (진입 보장 의도)
CHASE_WAIT_2 = 180     # 2차 3분 (눌림 대기)
CHASE_WAIT_3 = 300     # 3차 5분 (깊은 눌림)

# 눌림 임계값 (시초가 대비 %)
PULLBACK_PCT_2 = -1.5  # 2차 진입 = 시초가 -1.5%
PULLBACK_PCT_3 = -3.0  # 3차 진입 = 시초가 -3% (사장님 영구 룰)


@dataclass
class SplitBuyResult:
    """3분할 매수 결과."""
    code: str
    total_target_qty: int    # 전체 목표 수량
    total_bought_qty: int    # 실제 매수된 수량
    avg_price: float         # 평균 매수가
    leg1_bought: int = 0     # 1차 매수
    leg2_bought: int = 0     # 2차 매수
    leg3_bought: int = 0     # 3차 매수
    leg1_market_fallback: bool = False  # 1차 시장가 폴백 발동
    success: bool = False    # 1차 진입 성공 여부 (최소 진입 보장)
    blocked: bool = False
    block_reason: str = ""
    block_message: str = ""


async def execute_vwap_split_buy(
    trader,
    code: str,
    name: str,
    open_price: int,
    total_qty: int,
    send_alert=None,
) -> SplitBuyResult:
    """★ 사장님 5/26 통찰 ★ VWAP 3분할 지정가 매수 (50/30/20 + 1차 시장가 폴백).

    Args:
        trader: KISTrader 인스턴스
        code: 종목 코드
        name: 종목명
        open_price: 시초가 (참조용)
        total_qty: 전체 목표 수량
        send_alert: 텔레그램 알림 함수 (선택)

    Returns:
        SplitBuyResult — 매수 결과 (success=1차 진입 성공 여부)
    """
    result = SplitBuyResult(
        code=code,
        total_target_qty=total_qty,
        total_bought_qty=0,
        avg_price=0.0,
    )
    result.blocked = False
    result.block_reason = ""
    result.block_message = ""

    order_gate = _resolve_order_gate(trader)
    if order_gate:
        try:
            blocked = await asyncio.to_thread(
                order_gate,
                "BUY",
                code,
                total_qty,
                source="vwap_split_buy",
                record_allowed=False,
            )
        except TypeError:
            blocked = await asyncio.to_thread(
                order_gate,
                "BUY",
                code,
                total_qty,
                source="vwap_split_buy",
            )
        if blocked:
            result.blocked = True
            result.block_reason = str(blocked.get("reason", "ORDER_GATE_BLOCKED"))
            result.block_message = str(blocked.get("message", ""))
            logger.warning(
                "[vwap_split] %s - blocked split buy %s(%s) qty=%s",
                result.block_reason,
                name,
                code,
                total_qty,
            )
            return result
    elif is_auto_trade_disabled():
        result.blocked = True
        result.block_reason = "AUTO_TRADE_DISABLED"
        result.block_message = f"AUTO_TRADE_DISABLED - blocked split buy {code} x {total_qty}"
        logger.warning(
            "[vwap_split] AUTO_TRADE_DISABLED - blocked split buy %s(%s) qty=%s",
            name,
            code,
            total_qty,
        )
        return result

    if total_qty < 3:
        # 너무 적으면 단일 매수
        logger.warning(f"[vwap_split] {code} qty={total_qty} 너무 적음 → 1차만 (단일 매수)")
        qty_1 = total_qty
        qty_2 = qty_3 = 0
    else:
        qty_1 = max(1, int(total_qty * SPLIT_RATIO_1))
        qty_2 = max(1, int(total_qty * SPLIT_RATIO_2))
        qty_3 = total_qty - qty_1 - qty_2

    total_spent = 0
    total_filled = 0

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # 1차 (50%): 시초가 chase_buy (60초) + 시장가 폴백 (★ 진입 보장 ★)
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    logger.info(f"[vwap_split] {name}({code}) 1차 진입 시작 — {qty_1}주 chase (60초)")
    try:
        resp_1 = await asyncio.to_thread(trader.chase_buy, code, qty_1, CHASE_WAIT_1)
        if resp_1 and resp_1.get("success"):
            result.leg1_bought = qty_1
            # ★ HIGH #6 fix — chase_buy 반환 키 final_price 우선 (avg_price 키 없음) ★
            buy_price_1 = float(
                resp_1.get("final_price") or resp_1.get("avg_price") or open_price
            )
            total_spent += buy_price_1 * qty_1
            total_filled += qty_1
            result.success = True
            logger.info(f"[vwap_split] {code} 1차 chase 성공 — {qty_1}주 @{buy_price_1:,.0f}")
        else:
            # ★ 사장님 5/26 통찰 — 1차 미체결 시 시장가 폴백 ★
            logger.warning(f"[vwap_split] {code} 1차 chase 미체결 → ★ 시장가 폴백 ★")
            # ★ HIGH #7 fix — split=1 명시 (사장님 5/25 룰, 즉시 1회 시장가) ★
            resp_mkt = await asyncio.to_thread(trader.buy_market, code, qty_1, 1)
            if resp_mkt and resp_mkt.get("success"):
                result.leg1_bought = qty_1
                result.leg1_market_fallback = True
                # ★ Codex 발견 fix — fetch_price fallback 강화 (3회 재시도) ★
                buy_price_1 = open_price
                fetch_success = False
                for attempt in range(3):
                    try:
                        p = await asyncio.to_thread(trader.fetch_price, code)
                        if p and p.get("current_price"):
                            buy_price_1 = float(p["current_price"])
                            fetch_success = True
                            break
                    except Exception as _fe:
                        logger.warning(f"[vwap_split] {code} fetch_price 시도 {attempt+1}/3 실패: {_fe}")
                        await asyncio.sleep(0.5)
                if not fetch_success:
                    logger.warning(f"[vwap_split] {code} fetch_price 3회 실패 → open_price {open_price} fallback")
                total_spent += buy_price_1 * qty_1
                total_filled += qty_1
                result.success = True
                # ★ Codex 발견 fix — send_alert 실패 시 로그 추가 ★
                if send_alert:
                    try:
                        await send_alert(
                            f"⚠️ [VWAP 분할 1차 — 시장가 폴백] {name}({code}) {qty_1}주 @{buy_price_1:,.0f}\n"
                            f"  사유: chase 60초 미체결 → 시장가 진입 (강한 추세 추정)"
                        )
                    except Exception as _alert_e:
                        logger.error(f"[vwap_split] {code} send_alert 실패: {_alert_e} (텔레그램 다운 가능성)")
            else:
                logger.error(f"[vwap_split] {code} 1차 시장가 폴백도 실패 — 전체 매수 중단")
                return result  # 1차 실패 시 2/3차 진행 X

    except Exception as e:
        logger.error(f"[vwap_split] {code} 1차 예외: {e}")
        return result

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # 2차 (30%): -1.5% 눌림 chase_buy (3분) / 미체결 시 종료
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    if qty_2 > 0:
        target_2 = int(open_price * (1 + PULLBACK_PCT_2 / 100))
        logger.info(f"[vwap_split] {name}({code}) 2차 대기 — {qty_2}주 @ -1.5% ({target_2:,}) chase (3분)")
        try:
            # 대기: 가격이 target_2 이하로 도달할 때까지 (max 3분)
            wait_start = asyncio.get_event_loop().time()
            target_reached = False
            while (asyncio.get_event_loop().time() - wait_start) < CHASE_WAIT_2:
                p = await asyncio.to_thread(trader.fetch_price, code)
                if not p:
                    await asyncio.sleep(3)
                    continue
                curr = int(p.get("current_price", 0) or 0)
                if curr > 0 and curr <= target_2:
                    target_reached = True
                    break
                await asyncio.sleep(3)

            if target_reached:
                resp_2 = await asyncio.to_thread(trader.chase_buy, code, qty_2, 30)
                if resp_2 and resp_2.get("success"):
                    result.leg2_bought = qty_2
                    # ★ HIGH #6 fix — final_price 키 우선 ★
                    buy_price_2 = float(
                        resp_2.get("final_price") or resp_2.get("avg_price") or target_2
                    )
                    total_spent += buy_price_2 * qty_2
                    total_filled += qty_2
                    logger.info(f"[vwap_split] {code} 2차 chase 성공 — {qty_2}주 @{buy_price_2:,.0f}")
                else:
                    logger.info(f"[vwap_split] {code} 2차 chase 미체결 → 종료 (강한 추세)")
            else:
                logger.info(f"[vwap_split] {code} 2차 미도달 → 종료 (강한 추세 / 1차만 진입)")
        except Exception as e:
            logger.warning(f"[vwap_split] {code} 2차 예외: {e}")

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # 3차 (20%): -3% 눌림 chase_buy (5분) / 미체결 시 종료
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    if qty_3 > 0:
        target_3 = int(open_price * (1 + PULLBACK_PCT_3 / 100))
        logger.info(f"[vwap_split] {name}({code}) 3차 대기 — {qty_3}주 @ -3% ({target_3:,}) chase (5분)")
        try:
            wait_start = asyncio.get_event_loop().time()
            target_reached = False
            while (asyncio.get_event_loop().time() - wait_start) < CHASE_WAIT_3:
                p = await asyncio.to_thread(trader.fetch_price, code)
                if not p:
                    await asyncio.sleep(3)
                    continue
                curr = int(p.get("current_price", 0) or 0)
                if curr > 0 and curr <= target_3:
                    target_reached = True
                    break
                await asyncio.sleep(5)

            if target_reached:
                resp_3 = await asyncio.to_thread(trader.chase_buy, code, qty_3, 60)
                if resp_3 and resp_3.get("success"):
                    result.leg3_bought = qty_3
                    # ★ HIGH #6 fix — final_price 키 우선 ★
                    buy_price_3 = float(
                        resp_3.get("final_price") or resp_3.get("avg_price") or target_3
                    )
                    total_spent += buy_price_3 * qty_3
                    total_filled += qty_3
                    logger.info(f"[vwap_split] {code} 3차 chase 성공 — {qty_3}주 @{buy_price_3:,.0f} (깊은 눌림)")
                else:
                    logger.info(f"[vwap_split] {code} 3차 chase 미체결 → 종료")
            else:
                logger.info(f"[vwap_split] {code} 3차 미도달 → 종료")
        except Exception as e:
            logger.warning(f"[vwap_split] {code} 3차 예외: {e}")

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # 최종 결과
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    result.total_bought_qty = total_filled
    result.avg_price = total_spent / total_filled if total_filled > 0 else 0.0
    fill_pct = total_filled / total_qty * 100 if total_qty > 0 else 0

    if send_alert:
        try:
            scenario = "A 강한 추세" if total_filled == result.leg1_bought else (
                "B 평이 추세" if result.leg3_bought == 0 else "C 깊은 눌림 (100%)"
            )
            await send_alert(
                f"✅ [VWAP 3분할 매수 완료] {name}({code})\n"
                f"  목표 {total_qty}주 / 실매수 {total_filled}주 ({fill_pct:.0f}%) / 평균가 {result.avg_price:,.0f}\n"
                f"  1차 {result.leg1_bought}주 / 2차 {result.leg2_bought}주 / 3차 {result.leg3_bought}주\n"
                f"  시나리오: {scenario} {'(시장가 폴백)' if result.leg1_market_fallback else ''}"
            )
        except Exception as _final_alert_e:
            # ★ Codex 발견 fix — 텔레그램 실패 로그 + 결과 보존 ★
            logger.error(f"[vwap_split] {code} 최종 알림 실패: {_final_alert_e} (매수 결과는 보존)")

    return result
