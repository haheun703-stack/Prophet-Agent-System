"""jarvis_decision.py — 박사 자율 통합 의사결정 모듈 (5/21 22:30)
====================================================================
사장님 5/21 22:00~22:30 명령:
  1. "정보봇한테 정보를 받아서 너가 결정" — 악재 자동 감지
  2. "상승 추매도 나쁘지 않다 5~10% 먹고 지정가" — 피라미딩
  3. "VWAP 상/중/하로 나누어서" — VWAP 기반 분기
  4. "소형주는 -3% 팔아야" — 시총별 SL

박사 자율 4가지 시나리오 통합:

  악재 발생 → FORCE_SELL (정보봇 시그널)
  VWAP 상단 + 수익 → PYRAMID_ADD (상승 피라미딩)
  VWAP 하단 + 손실 → RECOVERY_ADD (4시그널 통과 시 평단 낮춤)
  시총별 SL:
    - 대형주 (1조+) -10% → SL_LARGE
    - 중형주 (3000억+) -5% → SL_MID
    - 소형주 (3000억-) -3% → SL_SMALL

기본 dry_run = True (5/22~5/23 워밍업 검증).
5/26 실전 D-Day = dry_run = False (자동 실행).

의존:
  - data_store/crisis_etf_signal.json
  - data_store/global_events.json
  - data_store/jgis_morning_context.json
  - data_store/universe.json
  - bot/portfolio_monitor._get_vwap
"""
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Tuple, Optional

logger = logging.getLogger("BH.Jarvis")

_DATA_DIR = Path(__file__).resolve().parent.parent / "data_store"

# 시총 분류 기준 (억 단위)
MARKET_CAP_LARGE = 10_000   # 1조+
MARKET_CAP_MID = 3_000      # 3,000억+
# 그 외 = 소형

# VWAP 분기 기준
VWAP_UPPER = 1.02   # 현재가 / VWAP > 1.02 = 상단
VWAP_LOWER = 0.98   # 현재가 / VWAP < 0.98 = 하단

# 액션 결과 enum
ACTION_FORCE_SELL = "FORCE_SELL"      # 악재 발생 즉시 손절
ACTION_PYRAMID_ADD = "PYRAMID_ADD"    # 상승 피라미딩
ACTION_RECOVERY_ADD = "RECOVERY_ADD"  # 하락 추매 (평단 낮춤)
ACTION_SL_LARGE = "SL_LARGE"          # 대형주 SL -10%
ACTION_SL_MID = "SL_MID"              # 중형주 SL -5%
ACTION_SL_SMALL = "SL_SMALL"          # 소형주 SL -3%
ACTION_TAKE_PROFIT = "TAKE_PROFIT"    # 익절 (지정가)
ACTION_HOLD = "HOLD"                  # 관망


# ============================================================
# 1. 악재 감지 (정보봇 4개 데이터 통합)
# ============================================================
def is_negative_news(code: str) -> Tuple[bool, str]:
    """4가지 악재 자동 감지.

    Returns:
        (악재 여부 True/False, 사유 텍스트)
    """
    reasons = []

    # ① crisis_etf_signal — 시장 위기 시그널
    try:
        path = _DATA_DIR / "crisis_etf_signal.json"
        if path.exists():
            d = json.loads(path.read_text(encoding="utf-8"))
            signal = d.get("signal", "")
            confidence = d.get("confidence", "")
            # CRISIS/DANGER/HIGH 신호 = 악재
            if signal in ("CRISIS", "ENTER_INVERSE", "DANGER") and confidence in ("HIGH", "MEDIUM"):
                reasons.append(f"crisis_etf={signal}/{confidence}")
            # KOSPI 3일 -8%+ 폭락
            kospi_3d = float(d.get("kospi_change_3d", 0))
            if kospi_3d <= -8:
                reasons.append(f"KOSPI 3D {kospi_3d:.1f}%")
            # VIX 급등
            vix_3d = float(d.get("vix_change_3d", 0))
            if vix_3d >= 20:
                reasons.append(f"VIX 3D +{vix_3d:.1f}%")
    except Exception as e:
        logger.debug(f"[악재] crisis_etf 체크 실패: {e}")

    # ② global_events — 종목별 NEGATIVE 이벤트
    try:
        path = _DATA_DIR / "global_events.json"
        if path.exists():
            d = json.loads(path.read_text(encoding="utf-8"))
            # HIGH 영향 + NEGATIVE 방향 이벤트
            for event in d.get("economic", []):
                if event.get("impact") == "HIGH" and event.get("direction") == "NEGATIVE":
                    # 종목 코드 일치 또는 섹터 일치
                    if code in event.get("kr_tickers", []):
                        reasons.append(f"event_HIGH_NEG:{event.get('event','?')[:30]}")
                        break
            # alerts에 종목 직접 있으면
            for alert in d.get("alerts", []):
                if isinstance(alert, dict) and code in str(alert.get("tickers", [])):
                    reasons.append(f"alert:{alert.get('title','?')[:30]}")
                    break
    except Exception as e:
        logger.debug(f"[악재] global_events 체크 실패: {e}")

    # ③ jgis_morning_context — 외인 강한 약세 + 종목 섹터 매도
    try:
        path = _DATA_DIR / "jgis_morning_context.json"
        if path.exists():
            d = json.loads(path.read_text(encoding="utf-8"))
            f_streak = int(d.get("foreign_streak", 0))
            # 외인 15일+ 연속 매도 = 강한 약세장
            if f_streak <= -15:
                reasons.append(f"외인 -{abs(f_streak)}일 연속 매도")
    except Exception as e:
        logger.debug(f"[악재] jgis 체크 실패: {e}")

    has_negative = len(reasons) > 0
    return has_negative, " / ".join(reasons) if reasons else ""


# ============================================================
# 2. 시총 분류 (대형/중형/소형)
# ============================================================
def get_market_cap_tier(code: str) -> Tuple[str, int]:
    """시총 분류.

    Returns:
        ("LARGE"/"MID"/"SMALL"/"UNKNOWN", 시총 억 단위)
    """
    try:
        path = _DATA_DIR / "universe.json"
        if path.exists():
            u = json.loads(path.read_text(encoding="utf-8"))
            info = u.get(code, {})
            if isinstance(info, dict):
                cap = int(info.get("cap_억", 0) or 0)
                if cap >= MARKET_CAP_LARGE:
                    return "LARGE", cap
                elif cap >= MARKET_CAP_MID:
                    return "MID", cap
                elif cap > 0:
                    return "SMALL", cap
    except Exception as e:
        logger.debug(f"[시총] {code} 조회 실패: {e}")
    return "UNKNOWN", 0


# ============================================================
# 3. VWAP 비율 (상/중/하)
# ============================================================
def get_vwap_position(code: str, current_price: int) -> Tuple[str, float]:
    """VWAP 대비 현재가 위치.

    Returns:
        ("UPPER"/"MIDDLE"/"LOWER", ratio)
    """
    try:
        from bot.portfolio_monitor import _get_vwap
        vwap = _get_vwap(code, n_days=20)
        if vwap and vwap > 0 and current_price > 0:
            ratio = current_price / vwap
            if ratio > VWAP_UPPER:
                return "UPPER", ratio
            elif ratio < VWAP_LOWER:
                return "LOWER", ratio
            else:
                return "MIDDLE", ratio
    except Exception as e:
        logger.debug(f"[VWAP] {code} 계산 실패: {e}")
    return "UNKNOWN", 1.0


# ============================================================
# 4. 통합 액션 결정 (박사 자율 핵심)
# ============================================================
def decide_action(pos: dict, current_price: int) -> Tuple[str, str]:
    """포지션별 박사 자율 액션 결정 (4가지 시나리오 통합).

    Args:
        pos: positions.json 단일 종목 dict (name, qty, buy_price, source, ...)
        current_price: 현재가

    Returns:
        (action, reason) — action enum, 텍스트 사유
    """
    code = None
    # pos에 code 없을 수 있음 (호출자가 외부에서 알려줘야 함)
    code = pos.get("code") or pos.get("_code")
    if not code:
        return ACTION_HOLD, "code 없음 — 안전 관망"

    buy_price = float(pos.get("buy_price", 0))
    if buy_price <= 0:
        return ACTION_HOLD, "buy_price 0 — 관망"

    pnl_pct = (current_price - buy_price) / buy_price * 100

    # ── 1️⃣ 악재 체크 (최우선) ──
    has_neg, neg_reason = is_negative_news(code)
    if has_neg:
        return ACTION_FORCE_SELL, f"악재 감지: {neg_reason}"

    # 사장님 보호 종목은 자동 매도 X (사전 차단)
    if (pos.get("sl_disabled")
            or str(pos.get("source", "")).startswith("manual_sync")
            or str(pos.get("source", "")).startswith("sync_auto")
            or pos.get("source") == "verification"):
        return ACTION_HOLD, "사장님 보호 종목 — 자동 액션 X"

    # ── 2️⃣ VWAP 위치 ──
    vwap_pos, vwap_ratio = get_vwap_position(code, current_price)

    # VWAP 상단 + 수익 5%+ → 상승 피라미딩 (사장님 "5~10% 먹고 지정가")
    if vwap_pos == "UPPER" and pnl_pct >= 5:
        return ACTION_PYRAMID_ADD, (
            f"VWAP 상단 {vwap_ratio:.3f} / 수익 +{pnl_pct:.1f}% → 피라미딩 (5~10% 익절 목표)"
        )

    # VWAP 하단 + 손실 -5%+ → 하락 추매 (recovery_add_on 시그널)
    if vwap_pos == "LOWER" and pnl_pct <= -5:
        return ACTION_RECOVERY_ADD, (
            f"VWAP 하단 {vwap_ratio:.3f} / 손실 {pnl_pct:.1f}% → 평단 낮춤 (4시그널 검증)"
        )

    # ── 3️⃣ 시총별 SL (사장님 5/21 22:00 결정) ──
    tier, cap = get_market_cap_tier(code)

    if tier == "LARGE":
        if pnl_pct <= -10:
            return ACTION_SL_LARGE, f"대형주 {cap:,}억 / 손실 {pnl_pct:.1f}% < -10%"
    elif tier == "MID":
        if pnl_pct <= -5:
            return ACTION_SL_MID, f"중형주 {cap:,}억 / 손실 {pnl_pct:.1f}% < -5%"
    elif tier == "SMALL":
        if pnl_pct <= -3:
            return ACTION_SL_SMALL, f"소형주 {cap:,}억 / 손실 {pnl_pct:.1f}% < -3%"
    else:
        # UNKNOWN = 안전 우선 (-3% 손절)
        if pnl_pct <= -3:
            return ACTION_SL_SMALL, f"시총 미상 / 손실 {pnl_pct:.1f}% < -3% (안전 우선)"

    # ── 4️⃣ 익절 (트레일링 활성 시 별도 처리 — 여기선 일반 익절만) ──
    # 트레일링은 _job_monitor_fallback에서 별도 처리. 여기는 단순 +10% 익절.
    if pnl_pct >= 10:
        return ACTION_TAKE_PROFIT, f"수익 +{pnl_pct:.1f}% → 익절 (smart_sell 지정가)"

    return ACTION_HOLD, f"관망 (손익 {pnl_pct:+.1f}% / VWAP {vwap_pos} / 시총 {tier})"


# ============================================================
# 5. 액션 실행 (dry_run / 실전)
# ============================================================
def execute_action(
    self,
    code: str,
    action: str,
    reason: str,
    pos: dict,
    current_price: int,
    dry_run: bool = True,
) -> dict:
    """액션 실행 (auto_trader mixin 메서드).

    dry_run=True: 텔레그램 알림만 + 로그 기록
    dry_run=False: 실제 매수/매도 실행 (5/26 D-Day 이후)
    """
    name = pos.get("name", code)
    log_msg = (
        f"[JARVIS] {action} | {name}({code}) "
        f"@ {current_price:,} | {reason} | dry_run={dry_run}"
    )

    if action == ACTION_HOLD:
        logger.debug(log_msg)
        return {"action": action, "executed": False}

    logger.warning(log_msg)

    # 텔레그램 알림 (모든 액션 — dry_run 여부 무관)
    if hasattr(self, "_send_alert") and self._send_alert:
        try:
            import asyncio
            mode = "🧪 DRY" if dry_run else "🔥 LIVE"
            msg = (
                f"🧠 [박사 자율 의사결정] {mode}\n"
                f"  종목: {name}({code}) @ {current_price:,}\n"
                f"  액션: {action}\n"
                f"  사유: {reason}"
            )
            coro = self._send_alert(msg)
            if asyncio.iscoroutine(coro):
                try:
                    loop = asyncio.get_running_loop()
                    loop.create_task(coro)
                except RuntimeError:
                    pass  # 동기 환경에서 silent
        except Exception as _ae:
            logger.debug(f"[JARVIS] 알림 실패: {_ae}")

    if dry_run:
        # 결정 로그 누적 (백테스트 데이터)
        try:
            log_path = _DATA_DIR / "jarvis_decision_log.json"
            history = []
            if log_path.exists():
                history = json.loads(log_path.read_text(encoding="utf-8"))
            history.append({
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "code": code,
                "name": name,
                "action": action,
                "reason": reason,
                "current_price": current_price,
                "buy_price": pos.get("buy_price"),
                "pnl_pct": (current_price - float(pos.get("buy_price", 0))) / float(pos.get("buy_price", 1)) * 100,
            })
            # 최근 500건만 유지
            if len(history) > 500:
                history = history[-500:]
            log_path.write_text(json.dumps(history, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception as _le:
            logger.debug(f"[JARVIS] log 적재 실패: {_le}")
        return {"action": action, "executed": False, "dry_run": True}

    # ── LIVE 실행 (5/26 D-Day 이후) ──
    try:
        if action == ACTION_FORCE_SELL:
            # 악재 = 즉시 시장가 매도 (긴급)
            return self.trader.sell_market(code, pos.get("qty", 1))
        elif action in (ACTION_SL_LARGE, ACTION_SL_MID, ACTION_SL_SMALL):
            # 시총별 SL = 시장가 (빠른 손절)
            return self.trader.sell_market(code, pos.get("qty", 1))
        elif action == ACTION_TAKE_PROFIT:
            # 익절 = smart_sell 지정가 (사장님 명령)
            return self.trader.smart_sell(code, pos.get("qty", 1))
        elif action == ACTION_PYRAMID_ADD:
            # 상승 추매 = smart_buy 지정가 (1주 모드 호환)
            return self.trader.smart_buy(code, 1)
        elif action == ACTION_RECOVERY_ADD:
            # 하락 추매 = recovery_add_on 모듈 (4시그널 게이트)
            from bot.recovery_add_on import evaluate_add_on
            decision = evaluate_add_on(pos, current_price, code, self.trader, self.config)
            if decision.should_add:
                return self.trader.smart_buy(code, 1)  # 1주 모드
            return {"action": action, "executed": False, "reason": "4시그널 통과 실패"}
    except Exception as _xe:
        logger.error(f"[JARVIS] 실행 실패: {_xe}")
        return {"action": action, "executed": False, "error": str(_xe)}

    return {"action": action, "executed": False}


def attach_to(cls):
    """AutoTrader 클래스에 jarvis_decision 메서드 추가 (mixin)."""
    cls.jarvis_execute_action = execute_action
    return cls
