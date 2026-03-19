# -*- coding: utf-8 -*-
"""
Trade Tracker — 실행 중 TradeObject 상태 관리
===============================================
PLANNED → ACTIVE → CLOSED 라이프사이클 추적.
auto_trader와 연동하여 실제 매수/매도 가격 기록.

Usage:
    tracker = TradeTracker()
    tracker.activate(code, filled_price, shares)
    tracker.close(code, exit_price, reason)
    summary = tracker.get_summary()
"""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, List

logger = logging.getLogger("BH.TradeTracker")

_STORE = Path(__file__).resolve().parent.parent / "data_store"
ACTIVE_PATH = _STORE / "active_trades.json"


class TradeTracker:
    """실행 중 TradeObject 추적기"""

    def __init__(self):
        self._active: Dict[str, dict] = {}
        self._load()

    def _load(self):
        if ACTIVE_PATH.exists():
            try:
                with open(ACTIVE_PATH, "r", encoding="utf-8") as f:
                    data = json.load(f)
                self._active = data.get("trades", {})
            except Exception:
                self._active = {}

    def _save(self):
        _STORE.mkdir(parents=True, exist_ok=True)
        data = {
            "updated": datetime.now().strftime("%Y-%m-%d %H:%M"),
            "count": len(self._active),
            "trades": self._active,
        }
        tmp = ACTIVE_PATH.with_suffix(".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        tmp.replace(ACTIVE_PATH)

    def register(self, trade_obj) -> bool:
        """TradeObject를 PLANNED 상태로 등록"""
        if hasattr(trade_obj, "code"):
            code = trade_obj.code
            self._active[code] = {
                "trade_id": trade_obj.trade_id,
                "code": code,
                "name": trade_obj.name,
                "status": "PLANNED",
                "entry_price": trade_obj.entry_price,
                "stop_loss": trade_obj.stop_loss,
                "target_price": trade_obj.target_price,
                "rr_ratio": trade_obj.rr_ratio,
                "rr_verdict": trade_obj.rr_verdict,
                "expected_return": trade_obj.reward_pct,
                "expected_hold_days": trade_obj.expected_hold_days,
                "time_stop_days": trade_obj.time_stop_days,
                "total_score": trade_obj.total_score,
                "sources": trade_obj.sources,
                "conviction": trade_obj.conviction,
                "position_krw": trade_obj.position_krw,
                "shares": trade_obj.shares,
                "registered_at": datetime.now().strftime("%Y-%m-%d %H:%M"),
                "actual_entry": 0,
                "actual_exit": 0,
                "actual_pnl": 0.0,
                "hold_start": "",
                "hold_days": 0,
            }
        elif isinstance(trade_obj, dict):
            code = trade_obj["code"]
            self._active[code] = trade_obj
        else:
            return False

        self._save()
        return True

    def activate(self, code: str, filled_price: int, shares: int = 0) -> bool:
        """매수 체결 시 ACTIVE로 전환"""
        if code not in self._active:
            return False

        t = self._active[code]
        t["status"] = "ACTIVE"
        t["actual_entry"] = filled_price
        if shares > 0:
            t["shares"] = shares
        t["hold_start"] = datetime.now().strftime("%Y-%m-%d")
        self._save()
        logger.info(f"[TradeTracker] ACTIVE: {t.get('name', code)} @ {filled_price:,}")
        return True

    def close(self, code: str, exit_price: int, reason: str) -> Optional[dict]:
        """매도 체결 시 CLOSED 처리 + trade_learner에 기록"""
        if code not in self._active:
            return None

        t = self._active.pop(code)
        t["status"] = "CLOSED"
        t["actual_exit"] = exit_price
        t["close_reason"] = reason
        t["closed_at"] = datetime.now().strftime("%Y-%m-%d %H:%M")

        # P&L 계산
        entry = t.get("actual_entry", 0) or t.get("entry_price", 0)
        if entry > 0:
            t["actual_pnl"] = round((exit_price - entry) / entry * 100, 2)

        # 보유일 계산
        if t.get("hold_start"):
            try:
                start = datetime.strptime(t["hold_start"], "%Y-%m-%d")
                t["hold_days"] = (datetime.now() - start).days
            except Exception:
                pass

        self._save()

        # trade_learner에 기록
        try:
            from data.trade_learner import record_trade_close
            record_trade_close(
                trade_id=t.get("trade_id", ""),
                code=code,
                name=t.get("name", ""),
                entry_price=entry,
                exit_price=exit_price,
                total_score=t.get("total_score", 0),
                sources=t.get("sources", []),
                close_reason=reason,
                hold_days=t.get("hold_days", 0),
                expected_return=t.get("expected_return", 0),
                rr_ratio=t.get("rr_ratio", 0),
            )
        except Exception as e:
            logger.warning(f"[TradeTracker] trade_learner 기록 실패: {e}")

        logger.info(f"[TradeTracker] CLOSED: {t.get('name', code)} "
                     f"@ {exit_price:,} ({t['actual_pnl']:+.2f}%) [{reason}]")
        return t

    def get_active(self, code: str) -> Optional[dict]:
        return self._active.get(code)

    def get_all_active(self) -> Dict[str, dict]:
        return dict(self._active)

    def is_tracked(self, code: str) -> bool:
        return code in self._active

    def check_time_stops(self) -> List[str]:
        """시간 손절 대상 종목 반환"""
        expired = []
        now = datetime.now()
        for code, t in self._active.items():
            if t.get("status") != "ACTIVE":
                continue
            if not t.get("hold_start"):
                continue
            try:
                start = datetime.strptime(t["hold_start"], "%Y-%m-%d")
                days = (now - start).days
                if days >= t.get("time_stop_days", 5):
                    expired.append(code)
            except Exception:
                pass
        return expired

    def get_summary(self) -> str:
        """활성 트레이드 요약 (텔레그램용)"""
        if not self._active:
            return ""

        lines = ["━━ Active Trades ━━━"]
        for code, t in self._active.items():
            name = t.get("name", code)
            status = t.get("status", "?")
            rr = t.get("rr_ratio", 0)

            if status == "ACTIVE":
                entry = t.get("actual_entry", 0)
                lines.append(f"  {name} [{status}] R:R {rr:.1f}")
                lines.append(f"    매수 {entry:,} | SL {t.get('stop_loss', 0):,} "
                             f"| TP {t.get('target_price', 0):,}")
            else:
                lines.append(f"  {name} [{status}] R:R {rr:.1f}")

        return "\n".join(lines)
