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

    # ═══════════════════════════════════════════
    #  PAPER 트레이딩 시스템
    # ═══════════════════════════════════════════

    def register_paper_from_objects(self, kis=None) -> list:
        """trade_objects.json에서 ACCEPT 종목을 PAPER_ACTIVE로 등록

        Args:
            kis: KISTrader 인스턴스 (시가 조회용, 없으면 entry_price 사용)
        Returns:
            등록된 종목명 리스트
        """
        from data.trade_object import load_trade_objects
        trade_objs = load_trade_objects()
        if not trade_objs:
            return []

        registered = []
        for to in trade_objs:
            if to.rr_verdict == "REJECT":
                continue
            if self.is_tracked(to.code):
                continue

            # 시가 조회 (장 시작 후)
            entry = to.entry_price
            if kis:
                try:
                    p = kis.fetch_price(to.code)
                    if p.get("success"):
                        open_p = p.get("open", 0)
                        if open_p > 0:
                            entry = open_p
                except Exception:
                    pass

            self.register(to)
            self.activate(to.code, entry)
            self._active[to.code]["paper"] = True
            registered.append(to.name)

        if registered:
            self._save()
        return registered

    def check_paper_prices(self, kis) -> list:
        """장중 현재가 vs TP/SL 체크 → 도달 시 auto-close

        Returns:
            이벤트 메시지 리스트
        """
        events = []
        to_close = []

        for code, t in self._active.items():
            if t.get("status") != "ACTIVE":
                continue

            try:
                p = kis.fetch_price(code)
                if not p.get("success"):
                    continue

                high = p.get("high", 0)
                low = p.get("low", 0)
                current = p.get("current_price", 0)
                tp = t.get("target_price", 0)
                sl = t.get("stop_loss", 0)

                if tp > 0 and high >= tp:
                    to_close.append((code, tp, "TARGET"))
                    pnl = (tp - t.get("actual_entry", tp)) / t.get("actual_entry", tp) * 100
                    events.append(
                        f"[PAPER] {t['name']} 목표가 도달!\n"
                        f"  {t.get('actual_entry', 0):,} → {tp:,} ({pnl:+.1f}%)"
                    )
                elif sl > 0 and low <= sl:
                    to_close.append((code, sl, "STOP"))
                    pnl = (sl - t.get("actual_entry", sl)) / t.get("actual_entry", sl) * 100
                    events.append(
                        f"[PAPER] {t['name']} 손절 도달!\n"
                        f"  {t.get('actual_entry', 0):,} → {sl:,} ({pnl:+.1f}%)"
                    )
            except Exception as e:
                logger.warning(f"PAPER 가격체크 실패 {code}: {e}")

        for code, price, reason in to_close:
            self.close(code, price, reason)

        return events

    def paper_close_eod(self, kis) -> list:
        """장마감 시간손절 — time_stop_days 경과 종목 종가 기준 강제 클로즈

        Returns:
            이벤트 메시지 리스트
        """
        events = []
        expired = self.check_time_stops()

        for code in expired:
            t = self._active.get(code)
            if not t:
                continue

            # 종가(현재가) 조회
            close_price = t.get("actual_entry", 0)
            try:
                p = kis.fetch_price(code)
                if p.get("success") and p.get("current_price", 0) > 0:
                    close_price = p["current_price"]
            except Exception:
                pass

            entry = t.get("actual_entry", 0) or t.get("entry_price", 0)
            pnl = (close_price - entry) / entry * 100 if entry > 0 else 0
            name = t.get("name", code)
            days = t.get("time_stop_days", 5)

            result = self.close(code, close_price, "TIME_STOP")
            if result:
                events.append(
                    f"[PAPER] {name} 시간손절 D+{days}\n"
                    f"  {entry:,} → {close_price:,} ({pnl:+.1f}%)"
                )

        return events

    def get_paper_dashboard(self, kis=None) -> str:
        """PAPER 트레이딩 현황 텔레그램 메시지"""
        if not self._active:
            return "[PAPER] 추적 중인 종목 없음"

        lines = ["━━ [PAPER] Trade Tracker ━━━"]
        now = datetime.now()

        for code, t in self._active.items():
            name = t.get("name", code)
            status = t.get("status", "?")
            entry = t.get("actual_entry", 0) or t.get("entry_price", 0)
            tp = t.get("target_price", 0)
            sl = t.get("stop_loss", 0)
            rr = t.get("rr_ratio", 0)
            verdict = t.get("rr_verdict", "")

            # 보유일수
            days = 0
            if t.get("hold_start"):
                try:
                    start = datetime.strptime(t["hold_start"], "%Y-%m-%d")
                    days = (now - start).days
                except Exception:
                    pass
            time_stop = t.get("time_stop_days", 5)

            # 현재가 조회 (가능하면)
            current = 0
            if kis and status == "ACTIVE":
                try:
                    p = kis.fetch_price(code)
                    if p.get("success"):
                        current = p.get("current_price", 0)
                except Exception:
                    pass

            pnl_str = ""
            if current > 0 and entry > 0:
                pnl = (current - entry) / entry * 100
                pnl_str = f" 현재 {current:,}({pnl:+.1f}%)"

            lines.append(f"  [{verdict}] {name} R:R {rr:.1f} D+{days}/{time_stop}")
            lines.append(f"    진입 {entry:,} | TP {tp:,} | SL {sl:,}{pnl_str}")

        lines.append("━━━━━━━━━━━━━━━━━━━━━━━")

        # 종료된 트레이드 통계
        stats = self.get_paper_review_stats()
        if stats.get("total", 0) > 0:
            lines.append(f"\n━━ 2주 리뷰 ({stats['total']}건) ━━━")
            lines.append(f"  승률: {stats['win_rate']:.0f}% (목표 50%+)")
            lines.append(f"  평균R: {stats['avg_r_realized']:.2f} (목표 0.7+)")
            lines.append(f"  TARGET_HIT: {stats['target_pct']:.0f}%")
            lines.append(f"  STOP_HIT: {stats['stop_pct']:.0f}%")
            lines.append(f"  TIME_STOP: {stats['time_pct']:.0f}%")

        return "\n".join(lines)

    # ═══════════════════════════════════════════
    #  PAPER-PRECLOSE 트레이딩
    # ═══════════════════════════════════════════

    def register_paper_preclose(self, candidates: list, kis=None) -> list:
        """프리클로즈 후보를 PAPER-PRECLOSE로 등록

        Args:
            candidates: PrecloseCandidate 리스트 (또는 dict 리스트)
            kis: KISTrader 인스턴스 (현재가 조회용)
        Returns:
            등록된 종목명 리스트
        """
        registered = []
        for c in candidates:
            # dict 또는 PrecloseCandidate 지원
            if hasattr(c, "code"):
                code = c.code
                name = c.name
                to = c.trade_object if hasattr(c, "trade_object") else None
            elif isinstance(c, dict):
                code = c["code"]
                name = c.get("name", code)
                to = c.get("trade_object")
            else:
                continue

            if self.is_tracked(code):
                continue

            # 현재가 조회
            entry = 0
            if kis:
                try:
                    p = kis.fetch_price(code)
                    if p.get("success"):
                        entry = p.get("current_price", 0)
                except Exception:
                    pass

            # trade_object가 있으면 그 정보 사용
            if isinstance(to, dict) and to.get("entry_price"):
                if entry <= 0:
                    entry = to["entry_price"]
                from data.sajang_rules import SAJANG
                trade_data = {
                    "trade_id": to.get("trade_id", f"PC_{datetime.now():%Y%m%d}_{code}"),
                    "code": code,
                    "name": name,
                    "status": "ACTIVE",
                    "entry_price": to.get("entry_price", entry),
                    "stop_loss": to.get("stop_loss", 0),
                    "target_price": to.get("target_price", 0),
                    "rr_ratio": to.get("rr_ratio", 0),
                    "rr_verdict": to.get("rr_verdict", ""),
                    "expected_return": to.get("reward_pct", 0),
                    "expected_hold_days": to.get("expected_hold_days", 3),
                    "time_stop_days": to.get("time_stop_days", 5),
                    "total_score": to.get("total_score", 0),
                    "sources": to.get("sources", []),
                    "conviction": to.get("conviction", 0.7),
                    "position_krw": int(to.get("position_krw", 0) * 0.7),
                    "shares": to.get("shares", 0),
                    "registered_at": datetime.now().strftime("%Y-%m-%d %H:%M"),
                    "actual_entry": entry,
                    "actual_exit": 0,
                    "actual_pnl": 0.0,
                    "hold_start": datetime.now().strftime("%Y-%m-%d"),
                    "hold_days": 0,
                    "paper": True,
                    "preclose": True,
                    "source": "preclose",
                }
            else:
                # trade_object 없이 기본 등록
                preclose_score = 0
                if hasattr(c, "preclose_score"):
                    preclose_score = c.preclose_score
                elif isinstance(c, dict):
                    preclose_score = c.get("preclose_score", 0)

                if entry <= 0:
                    entry = c.current_price if hasattr(c, "current_price") else c.get("current_price", 0)

                trade_data = {
                    "trade_id": f"PC_{datetime.now():%Y%m%d}_{code}",
                    "code": code,
                    "name": name,
                    "status": "ACTIVE",
                    "entry_price": entry,
                    "stop_loss": SAJANG.get_normal_sl(entry),
                    "target_price": SAJANG.get_take_profit(entry),
                    "rr_ratio": 1.4,
                    "rr_verdict": "MARGINAL",
                    "expected_return": 5.0,
                    "expected_hold_days": 3,
                    "time_stop_days": 5,
                    "total_score": preclose_score,
                    "sources": ["preclose"],
                    "conviction": 0.7,
                    "position_krw": 0,
                    "shares": 0,
                    "registered_at": datetime.now().strftime("%Y-%m-%d %H:%M"),
                    "actual_entry": entry,
                    "actual_exit": 0,
                    "actual_pnl": 0.0,
                    "hold_start": datetime.now().strftime("%Y-%m-%d"),
                    "hold_days": 0,
                    "paper": True,
                    "preclose": True,
                    "source": "preclose",
                }

            self._active[code] = trade_data
            registered.append(name)
            logger.info(f"[TradeTracker] PAPER-PRECLOSE: {name} @ {entry:,}")

        if registered:
            self._save()
        return registered

    def get_preclose_codes(self) -> list:
        """현재 활성 PAPER-PRECLOSE 종목 코드 리스트"""
        return [code for code, t in self._active.items()
                if t.get("preclose") and t.get("status") == "ACTIVE"]

    @staticmethod
    def get_paper_review_stats() -> dict:
        """2주 리뷰용 핵심 4 지표

        Returns:
            {total, win_rate, avg_r_realized, target_pct, stop_pct, time_pct}
        """
        try:
            from data.trade_learner import load_trade_log
            log = load_trade_log()
        except Exception:
            return {"total": 0}

        if not log:
            return {"total": 0}

        total = len(log)
        wins = sum(1 for t in log if t.get("actual_pnl", 0) > 0)

        # 사유별 비율
        target_hit = sum(1 for t in log if t.get("close_reason") == "TARGET")
        stop_hit = sum(1 for t in log if t.get("close_reason") == "STOP")
        time_stop = sum(1 for t in log if t.get("close_reason") == "TIME_STOP")

        # 평균 R 실현 = 실제수익 / 기대수익
        r_values = []
        for t in log:
            expected = t.get("expected_return", 0)
            actual = t.get("actual_pnl", 0)
            if expected > 0:
                r_values.append(actual / expected)

        return {
            "total": total,
            "win_rate": wins / total * 100 if total else 0,
            "avg_r_realized": sum(r_values) / len(r_values) if r_values else 0,
            "target_pct": target_hit / total * 100 if total else 0,
            "stop_pct": stop_hit / total * 100 if total else 0,
            "time_pct": time_stop / total * 100 if total else 0,
        }
