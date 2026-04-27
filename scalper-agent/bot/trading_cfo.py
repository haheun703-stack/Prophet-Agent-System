# -*- coding: utf-8 -*-
"""
TradingCFO - 재무 총괄 (Chief Financial Officer)
=================================================
이벤트 리스크, 예산, DrawdownShield, P&L을 중앙에서 관리.
auto_trader의 분산된 재무 로직을 파사드로 통합.

원칙:
  - 기존 모듈 수정 최소화 (파사드 패턴)
  - CFO 오류가 매매를 차단하는 일은 절대 없음 (폴백)
  - EVENT_RISK_MATRIX가 유일한 진실 (분산 금지)
"""

import json
import logging
from datetime import date, datetime
from pathlib import Path

logger = logging.getLogger("BH.CFO")

BASE_DIR = Path(__file__).resolve().parent.parent
CFO_STATE_PATH = BASE_DIR / "data_store" / "cfo_daily_pnl.json"


class TradingCFO:
    """재무 총괄 파사드"""

    # ═══════════════════════════════════════════
    #  이벤트 리스크 매트릭스 (MEMORY.md 정본)
    #  이 테이블만 수정하면 모든 모드에 적용됨
    # ═══════════════════════════════════════════
    EVENT_RISK_MATRIX = {
        "predawn": {"EXTREME": 0.3, "HIGH": 0.5, "MEDIUM": 1.0, "LOW": 1.0},
        "swing":   {"EXTREME": 0.5, "HIGH": 0.7, "MEDIUM": 1.0, "LOW": 1.0},
        "day":     {"EXTREME": 0.5, "HIGH": 0.7, "MEDIUM": 1.0, "LOW": 1.0},
        "nxt":     {"EXTREME": 0.5, "HIGH": 0.7, "MEDIUM": 1.0, "LOW": 1.0},
    }

    def __init__(self, config: dict, auto_trader=None):
        self.config = config
        self._auto_trader = auto_trader

        # DrawdownShield 연결
        self._shield = None
        try:
            from risk.drawdown_shield import DrawdownShield
            self._shield = DrawdownShield()
        except Exception as e:
            logger.warning(f"DrawdownShield 로드 실패 (무시): {e}")

        # 일일 P&L 상태
        self._daily_state = self._load_state()

    def set_auto_trader(self, auto_trader):
        """지연 연결 (telegram_bot에서 호출)"""
        self._auto_trader = auto_trader

    # ═══════════════════════════════════════════
    #  1. 이벤트 리스크 배수 (중앙 정본)
    # ═══════════════════════════════════════════

    def get_event_multiplier(self, risk_level: str, mode: str) -> float:
        """이벤트 리스크 배수 조회

        Args:
            risk_level: "EXTREME" / "HIGH" / "MEDIUM" / "LOW"
            mode: "predawn" / "swing" / "day" / "nxt"

        Returns:
            배수 (0.3 ~ 1.0)
        """
        mode_map = self.EVENT_RISK_MATRIX.get(mode, {})
        return mode_map.get(risk_level, 1.0)

    # ═══════════════════════════════════════════
    #  2. 통합 예산 계산 (4-Mode)
    # ═══════════════════════════════════════════

    def get_budget(
        self,
        mode: str,
        available_cash: int,
        num_targets: int = 1,
        regime_capital_use: float = 1.0,
        brain_pct: int = 100,
        event_risk_level: str = "LOW",
    ) -> dict:
        """통합 예산 계산

        Args:
            mode: "swing" / "day" / "nxt" / "predawn"
            available_cash: 가용현금 (원)
            num_targets: 매수 대상 종목 수
            regime_capital_use: CORTEX 체제 자본사용 배수
            brain_pct: BRAIN 비중 (0~100)
            event_risk_level: "EXTREME"/"HIGH"/"MEDIUM"/"LOW"

        Returns:
            {"total_budget", "per_stock", "event_mult", "formula"}
        """
        num_targets = max(num_targets, 1)
        event_mult = self.get_event_multiplier(event_risk_level, mode)

        if mode == "swing":
            # cash x (1-reserve) x capital_use x brain% x event_mult / targets
            reserve = self.config.get("risk", {}).get("min_cash_ratio", 0.10)
            total = int(
                available_cash
                * (1 - reserve)
                * regime_capital_use
                * (brain_pct / 100)
                * event_mult
            )
            formula = (
                f"{available_cash:,} x {1-reserve:.0%} x {regime_capital_use:.1f} "
                f"x {brain_pct}% x {event_mult:.1f} = {total:,}"
            )

        elif mode == "day":
            # config.auto_buy_amount x event_mult
            base = self.config.get("bot", {}).get("auto_buy_amount", 500000)
            total = int(base * event_mult)
            formula = f"{base:,} x {event_mult:.1f} = {total:,}"

        elif mode == "nxt":
            # cash x nxt_budget_pct / max_pos (이벤트 미적용 — 기존 동작)
            nw = self.config.get("nightwatch", {})
            budget_pct = nw.get("nxt_budget_pct", 30) / 100.0
            total = int(available_cash * budget_pct)
            event_mult = 1.0  # NXT는 이벤트 리스크 미적용
            formula = f"{available_cash:,} x {budget_pct:.0%} = {total:,}"

        elif mode == "predawn":
            # cash x (predawn_budget_pct x event_mult) / targets
            # 주의: 배수를 pct에 적용 (cash에 직접 적용 아님)
            nw = self.config.get("nightwatch", {})
            base_pct = nw.get("predawn_budget_pct", 40) / 100.0
            adj_pct = base_pct * event_mult
            total = int(available_cash * adj_pct)
            formula = (
                f"{available_cash:,} x ({base_pct:.0%} x {event_mult:.1f}) = {total:,}"
            )

        else:
            total = available_cash
            formula = f"unknown mode: {mode}"

        per_stock = total // num_targets if num_targets > 0 else total

        return {
            "total_budget": total,
            "per_stock": per_stock,
            "event_mult": event_mult,
            "formula": formula,
        }

    # ═══════════════════════════════════════════
    #  3. 리스크 게이트 (auto_trader 위임 + DrawdownShield)
    # ═══════════════════════════════════════════

    def check_can_trade(self) -> tuple:
        """리스크 게이트 체크

        Returns:
            (allowed: bool, reason: str, drawdown_tier: int)
        """
        tier = 0

        # auto_trader 리스크 게이트 위임
        if self._auto_trader:
            try:
                ok, reason = self._auto_trader.check_risk_gate()
                if not ok:
                    return False, reason, self._get_shield_tier()
            except Exception as e:
                logger.warning(f"CFO: auto_trader risk gate 실패 (무시): {e}")

        # DrawdownShield 티어 확인
        tier = self._get_shield_tier()

        return True, "", tier

    def _get_shield_tier(self) -> int:
        """DrawdownShield 현재 티어"""
        if self._shield:
            return self._shield.current_tier
        return 0

    # ═══════════════════════════════════════════
    #  4. P&L 기록
    # ═══════════════════════════════════════════

    def record_trade(
        self,
        code: str,
        pnl: int,
        source_signal: str = "",
        mode: str = "swing",
    ):
        """매매 P&L 기록

        Args:
            code: 종목코드
            pnl: 손익 (원화, 음수=손실)
            source_signal: 시그널 소스
            mode: "swing"/"day"/"nxt"/"predawn"
        """
        # 1) auto_trader 일일 손실 기록 위임
        if self._auto_trader and pnl < 0:
            try:
                self._auto_trader.record_realized_loss(pnl)
            except Exception as e:
                logger.warning(f"CFO: record_realized_loss 실패 (무시): {e}")

        # 2) DrawdownShield 업데이트
        if self._shield:
            try:
                new_risk = self._shield.update(pnl)
                logger.info(
                    f"[CFO] DrawdownShield: pnl={pnl:+,}원 "
                    f"→ 다음 리스크={new_risk:,}원/건 "
                    f"(연패={self._shield.losses_from_peak})"
                )
            except Exception as e:
                logger.warning(f"CFO: DrawdownShield update 실패 (무시): {e}")

        # 3) 일일 P&L 누적
        self._check_date_reset()
        self._daily_state["trades"].append({
            "code": code,
            "pnl": pnl,
            "source": source_signal,
            "mode": mode,
            "time": datetime.now().strftime("%H:%M:%S"),
        })
        self._daily_state["total_pnl"] += pnl
        self._daily_state["trade_count"] += 1
        if pnl > 0:
            self._daily_state["win_count"] += 1
        elif pnl < 0:
            self._daily_state["loss_count"] += 1
        self._save_state()

    # ═══════════════════════════════════════════
    #  5. 대시보드
    # ═══════════════════════════════════════════

    def get_status(self) -> str:
        """CFO 상태 대시보드"""
        self._check_date_reset()
        s = self._daily_state
        lines = [
            "=== CFO 재무 대시보드 ===",
            f"날짜: {s.get('date', '?')}",
            f"거래: {s.get('trade_count', 0)}건 "
            f"({s.get('win_count', 0)}W/{s.get('loss_count', 0)}L)",
            f"일일 P&L: {s.get('total_pnl', 0):+,}원",
        ]

        # 리스크 게이트 상태
        if self._auto_trader:
            try:
                ok, reason = self._auto_trader.check_risk_gate()
                lines.append(f"리스크: {'통과' if ok else reason}")
            except Exception:
                lines.append("리스크: 조회 실패")

        # DrawdownShield 상태
        if self._shield:
            lines.append(f"\n{self._shield.summary()}")

        # 이벤트 리스크 현황
        try:
            from data.event_calendar import get_event_risk_for_recommendation
            ev = get_event_risk_for_recommendation(date.today())
            rl = ev.get("risk_level", "LOW")
            events = [e["name"] for e in ev.get("events", []) if e.get("impact") == "HIGH"][:3]
            lines.append(f"\n이벤트: {rl}" + (f" ({', '.join(events)})" if events else ""))
            lines.append(
                f"  swing: x{self.get_event_multiplier(rl, 'swing'):.1f} / "
                f"day: x{self.get_event_multiplier(rl, 'day'):.1f} / "
                f"predawn: x{self.get_event_multiplier(rl, 'predawn'):.1f} / "
                f"nxt: x{self.get_event_multiplier(rl, 'nxt'):.1f}"
            )
        except Exception:
            lines.append("\n이벤트: 조회 실패")

        return "\n".join(lines)

    # ═══════════════════════════════════════════
    #  내부: 상태 영속화
    # ═══════════════════════════════════════════

    def _load_state(self) -> dict:
        """일일 P&L 상태 로드"""
        today = date.today().isoformat()
        default = {
            "date": today,
            "total_pnl": 0,
            "trade_count": 0,
            "win_count": 0,
            "loss_count": 0,
            "trades": [],
        }
        try:
            if CFO_STATE_PATH.exists():
                with open(CFO_STATE_PATH, "r", encoding="utf-8") as f:
                    state = json.load(f)
                if state.get("date") == today:
                    return state
        except Exception as e:
            logger.warning(f"CFO 상태 로드 실패: {e}")
        return default

    def _save_state(self):
        """일일 P&L 상태 저장 (atomic write)"""
        try:
            CFO_STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
            tmp = CFO_STATE_PATH.with_suffix(".tmp")
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(self._daily_state, f, ensure_ascii=False, indent=2)
            tmp.replace(CFO_STATE_PATH)
        except Exception as e:
            logger.error(f"CFO 상태 저장 실패: {e}")
            try:
                CFO_STATE_PATH.with_suffix(".tmp").unlink(missing_ok=True)
            except OSError:
                pass

    def _check_date_reset(self):
        """날짜 변경 시 자동 리셋"""
        today = date.today().isoformat()
        if self._daily_state.get("date") != today:
            self._daily_state = {
                "date": today,
                "total_pnl": 0,
                "trade_count": 0,
                "win_count": 0,
                "loss_count": 0,
                "trades": [],
            }
            # DrawdownShield는 리셋하지 않음 (연패는 일 단위가 아님)
