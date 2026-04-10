# -*- coding: utf-8 -*-
"""
Paper Portfolio — 가상 자금 관리 + 2주 검증 시스템
=================================================
모닝 추천 + NXT 야간매매를 가상 포트폴리오로 추적.
TradeTracker와 독립 운영 — 포트폴리오 레벨 자금관리 + 일일 P&L.

Usage:
    portfolio = PaperPortfolio()
    portfolio.open_position("005930", "삼성전자", 52000, 15, "morning", 55000, 50000, 5)
    portfolio.close_position("005930", 54500, "TARGET")
    report = portfolio.get_daily_report()
"""

import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Dict, List

logger = logging.getLogger("BH.PaperPortfolio")

_STORE = Path(__file__).resolve().parent.parent / "data_store"
PORTFOLIO_PATH = _STORE / "paper_portfolio.json"
DAILY_LOG_PATH = _STORE / "paper_daily_log.json"

INITIAL_CASH = 10_000_000  # 1,000만원


class PaperPortfolio:
    """가상 포트폴리오 — 2주 검증용"""

    def __init__(self):
        self.start_date: str = ""
        self.initial_cash: int = INITIAL_CASH
        self.cash: int = INITIAL_CASH
        self.positions: Dict[str, dict] = {}
        self.closed_trades: List[dict] = []
        self._load()

    def _load(self):
        if PORTFOLIO_PATH.exists():
            try:
                with open(PORTFOLIO_PATH, "r", encoding="utf-8") as f:
                    data = json.load(f)
                self.start_date = data.get("start_date", "")
                self.initial_cash = data.get("initial_cash", INITIAL_CASH)
                self.cash = data.get("cash", INITIAL_CASH)
                self.positions = data.get("positions", {})
                self.closed_trades = data.get("closed_trades", [])
            except Exception as e:
                logger.warning(f"paper_portfolio.json 로드 실패: {e}")
        else:
            self.start_date = datetime.now().strftime("%Y-%m-%d")

    def _save(self):
        _STORE.mkdir(parents=True, exist_ok=True)
        data = {
            "start_date": self.start_date,
            "initial_cash": self.initial_cash,
            "cash": self.cash,
            "positions": self.positions,
            "closed_trades": self.closed_trades,
            "updated": datetime.now().strftime("%Y-%m-%d %H:%M"),
        }
        tmp = PORTFOLIO_PATH.with_suffix(".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        tmp.replace(PORTFOLIO_PATH)

    # ─── 포지션 관리 ───────────────────────────────

    def open_position(self, code: str, name: str, entry_price: int,
                      shares: int, source: str, tp: int = 0,
                      sl: int = 0, time_stop_days: int = 5) -> bool:
        """가상 매수"""
        if code in self.positions:
            logger.warning(f"[PaperPortfolio] {name} 이미 보유 중 — 스킵")
            return False

        cost = entry_price * shares
        if cost > self.cash:
            # 자금 부족 시 매수 가능 수량으로 조정
            shares = self.cash // entry_price
            if shares <= 0:
                logger.warning(f"[PaperPortfolio] {name} 자금 부족 — 스킵")
                return False
            cost = entry_price * shares

        self.cash -= cost
        self.positions[code] = {
            "code": code,
            "name": name,
            "entry_price": entry_price,
            "shares": shares,
            "cost": cost,
            "source": source,
            "tp": tp,
            "sl": sl,
            "time_stop_days": time_stop_days,
            "entry_date": datetime.now().strftime("%Y-%m-%d"),
            "current_price": entry_price,
            "unrealized_pnl": 0.0,
        }
        self._save()
        logger.info(f"[PaperPortfolio] 매수: {name} {shares}주 @ {entry_price:,} "
                     f"[{source}] (잔여: {self.cash:,})")
        return True

    def close_position(self, code: str, exit_price: int,
                       reason: str) -> Optional[dict]:
        """가상 매도"""
        if code not in self.positions:
            return None

        pos = self.positions.pop(code)
        entry = pos["entry_price"]
        shares = pos.get("shares", pos.get("qty", 1))
        proceeds = exit_price * shares
        pnl_pct = (exit_price - entry) / entry * 100 if entry > 0 else 0
        pnl_krw = proceeds - pos["cost"]

        self.cash += proceeds

        trade = {
            "code": code,
            "name": pos["name"],
            "source": pos["source"],
            "entry_price": entry,
            "exit_price": exit_price,
            "shares": shares,
            "pnl_pct": round(pnl_pct, 2),
            "pnl_krw": pnl_krw,
            "reason": reason,
            "entry_date": pos["entry_date"],
            "exit_date": datetime.now().strftime("%Y-%m-%d"),
            "hold_days": self._calc_hold_days(pos["entry_date"]),
        }
        self.closed_trades.append(trade)
        self._save()

        logger.info(f"[PaperPortfolio] 매도: {pos['name']} @ {exit_price:,} "
                     f"({pnl_pct:+.2f}%) [{reason}] (잔여: {self.cash:,})")
        return trade

    def _calc_hold_days(self, entry_date: str) -> int:
        try:
            start = datetime.strptime(entry_date, "%Y-%m-%d")
            return (datetime.now() - start).days
        except Exception:
            return 0

    # ─── 시가총액 갱신 ─────────────────────────────

    def mark_to_market(self, kis) -> None:
        """장중 현재가 갱신 → unrealized_pnl 계산"""
        for code, pos in self.positions.items():
            try:
                p = kis.fetch_price(code)
                if p.get("success") and p.get("current_price", 0) > 0:
                    current = p["current_price"]
                    pos["current_price"] = current
                    entry = pos["entry_price"]
                    pos["unrealized_pnl"] = round(
                        (current - entry) / entry * 100, 2
                    ) if entry > 0 else 0.0
            except Exception as e:
                logger.warning(f"[PaperPortfolio] MTM 실패 {code}: {e}")
        self._save()

    # ─── 일일 스냅샷 ──────────────────────────────

    def record_daily_snapshot(self) -> dict:
        """당일 끝 스냅샷 → paper_daily_log.json 추가"""
        today = datetime.now().strftime("%Y-%m-%d")

        # 포트폴리오 평가액 = 현금 + 보유 주식 평가액
        holdings_value = sum(
            pos.get("current_price", pos["entry_price"]) * pos.get("shares", pos.get("qty", 1))
            for pos in self.positions.values()
        )
        total_value = self.cash + holdings_value
        total_return = (total_value - self.initial_cash) / self.initial_cash * 100

        # 오늘 청산된 거래
        today_closed = [t for t in self.closed_trades if t["exit_date"] == today]

        snapshot = {
            "date": today,
            "cash": self.cash,
            "holdings_value": holdings_value,
            "total_value": total_value,
            "total_return_pct": round(total_return, 2),
            "positions_count": len(self.positions),
            "today_closed": len(today_closed),
            "today_realized_pnl": sum(t["pnl_krw"] for t in today_closed),
        }

        # paper_daily_log.json에 추가
        log = self._load_daily_log()
        # 같은 날짜 이미 있으면 교체
        log = [s for s in log if s["date"] != today]
        log.append(snapshot)
        self._save_daily_log(log)

        return snapshot

    def _load_daily_log(self) -> list:
        if DAILY_LOG_PATH.exists():
            try:
                with open(DAILY_LOG_PATH, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception:
                return []
        return []

    def _save_daily_log(self, log: list):
        _STORE.mkdir(parents=True, exist_ok=True)
        tmp = DAILY_LOG_PATH.with_suffix(".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(log, f, ensure_ascii=False, indent=2)
        tmp.replace(DAILY_LOG_PATH)

    # ─── 통계 ─────────────────────────────────────

    def get_day_count(self) -> int:
        """시작일로부터 경과 거래일수"""
        if not self.start_date:
            return 0
        try:
            start = datetime.strptime(self.start_date, "%Y-%m-%d")
            return (datetime.now() - start).days
        except Exception:
            return 0

    def get_two_week_stats(self) -> dict:
        """2주 누적 통계"""
        trades = self.closed_trades
        total = len(trades)
        if total == 0:
            return {"total": 0}

        wins = sum(1 for t in trades if t["pnl_pct"] > 0)
        win_rate = wins / total * 100

        # 소스별 분리
        morning = [t for t in trades if t.get("source") == "morning"]
        nxt = [t for t in trades if t.get("source") == "nxt"]

        # 평균 R 실현
        r_values = []
        for t in trades:
            r_values.append(t["pnl_pct"])
        avg_pnl = sum(r_values) / len(r_values) if r_values else 0

        # 누적 수익률
        holdings_value = sum(
            pos.get("current_price", pos["entry_price"]) * pos.get("shares", pos.get("qty", 1))
            for pos in self.positions.values()
        )
        total_value = self.cash + holdings_value
        cum_return = (total_value - self.initial_cash) / self.initial_cash * 100

        # MDD (daily log 기반)
        log = self._load_daily_log()
        mdd = self._calc_mdd(log)

        # 사유별 분류
        target_hit = sum(1 for t in trades if t["reason"] == "TARGET")
        stop_hit = sum(1 for t in trades if t["reason"] == "STOP")
        time_stop = sum(1 for t in trades if t["reason"] in ("TIME_STOP", "NXT_MORNING_SELL"))

        return {
            "total": total,
            "wins": wins,
            "win_rate": round(win_rate, 1),
            "avg_pnl": round(avg_pnl, 2),
            "cum_return": round(cum_return, 2),
            "mdd": round(mdd, 2),
            "target_hit": target_hit,
            "stop_hit": stop_hit,
            "time_stop": time_stop,
            "morning_total": len(morning),
            "morning_wins": sum(1 for t in morning if t["pnl_pct"] > 0),
            "morning_win_rate": round(
                sum(1 for t in morning if t["pnl_pct"] > 0) / len(morning) * 100, 1
            ) if morning else 0,
            "nxt_total": len(nxt),
            "nxt_wins": sum(1 for t in nxt if t["pnl_pct"] > 0),
            "nxt_win_rate": round(
                sum(1 for t in nxt if t["pnl_pct"] > 0) / len(nxt) * 100, 1
            ) if nxt else 0,
        }

    def _calc_mdd(self, daily_log: list) -> float:
        """MDD (Maximum Drawdown) 계산"""
        if not daily_log:
            return 0.0
        values = [s["total_value"] for s in daily_log]
        if not values:
            return 0.0
        peak = values[0]
        mdd = 0.0
        for v in values:
            if v > peak:
                peak = v
            dd = (v - peak) / peak * 100
            if dd < mdd:
                mdd = dd
        return mdd

    def check_pass_fail(self) -> dict:
        """PASS/FAIL 자동 판정 (5개 중 4개 충족 시 PASS)"""
        stats = self.get_two_week_stats()
        if stats["total"] == 0:
            return {"verdict": "INSUFFICIENT_DATA", "passed": 0, "criteria": {}}

        criteria = {
            "win_rate_45": stats["win_rate"] >= 45,
            "avg_pnl_positive": stats["avg_pnl"] >= 0.5,
            "cum_return_positive": stats["cum_return"] >= 0,
            "mdd_within_15": stats["mdd"] >= -15,
            "sample_size_5": stats["total"] >= 5,
        }
        passed = sum(criteria.values())
        verdict = "PASS" if passed >= 4 else "FAIL"

        return {
            "verdict": verdict,
            "passed": passed,
            "total_criteria": 5,
            "criteria": criteria,
            "stats": stats,
        }

    # ─── 텔레그램 리포트 ──────────────────────────

    def get_daily_report(self) -> str:
        """일일 성적표 텔레그램 메시지"""
        day = self.get_day_count()
        today = datetime.now().strftime("%Y-%m-%d")

        # 포트폴리오 평가액
        holdings_value = sum(
            pos.get("current_price", pos["entry_price"]) * pos.get("shares", pos.get("qty", 1))
            for pos in self.positions.values()
        )
        total_value = self.cash + holdings_value
        total_return = (total_value - self.initial_cash) / self.initial_cash * 100

        # MDD
        log = self._load_daily_log()
        mdd = self._calc_mdd(log)

        lines = [
            f"━━ Paper Trading D+{day} ━━━━━━━━━",
            f"가상 자금: {self.initial_cash:,} -> {total_value:,} ({total_return:+.2f}%)",
            f"MDD: {mdd:.1f}%",
        ]

        # 오늘 청산
        today_closed = [t for t in self.closed_trades if t["exit_date"] == today]
        if today_closed:
            lines.append(f"\n[오늘 청산] {len(today_closed)}건")
            for t in today_closed:
                src = "모닝" if t["source"] == "morning" else "NXT"
                lines.append(
                    f"  {t['name']} {t['pnl_pct']:+.1f}% ({t['reason']}) [{src}]"
                )

        # 보유중
        if self.positions:
            lines.append(f"\n[보유중] {len(self.positions)}건")
            for code, pos in self.positions.items():
                src = "모닝" if pos["source"] == "morning" else "NXT"
                hold = self._calc_hold_days(pos["entry_date"])
                pnl = pos.get("unrealized_pnl", 0)
                lines.append(
                    f"  {pos['name']} {pnl:+.1f}% D+{hold} [{src}]"
                )

        # 누적 통계
        stats = self.get_two_week_stats()
        if stats["total"] > 0:
            lines.append(
                f"\n[누적] 승률 {stats['win_rate']:.0f}% | "
                f"평균 {stats['avg_pnl']:+.2f}% | "
                f"거래 {stats['total']}건"
            )
            parts = []
            if stats["morning_total"]:
                parts.append(
                    f"모닝: 승률 {stats['morning_win_rate']:.0f}% ({stats['morning_total']}건)"
                )
            if stats["nxt_total"]:
                parts.append(
                    f"NXT: 승률 {stats['nxt_win_rate']:.0f}% ({stats['nxt_total']}건)"
                )
            if parts:
                lines.append(f"  {' | '.join(parts)}")

        lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━")
        return "\n".join(lines)

    def format_two_week_report(self) -> str:
        """주간 종합 리포트"""
        result = self.check_pass_fail()
        stats = result.get("stats", self.get_two_week_stats())
        verdict = result["verdict"]
        week = self.get_day_count() // 7

        v_emoji = "PASS" if verdict == "PASS" else "FAIL"

        lines = [
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
            f"  Paper Trading Week {week} 종합 리포트",
            f"  판정: {v_emoji}",
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
            "",
            f"[성과 요약]",
            f"  총 거래: {stats['total']}건 (모닝 {stats['morning_total']} / NXT {stats['nxt_total']})",
            f"  승률: {stats['win_rate']:.1f}% (기준 45%+)",
            f"  평균 수익: {stats['avg_pnl']:+.2f}% (기준 0.5%+)",
            f"  누적 수익: {stats['cum_return']:+.2f}% (기준 0%+)",
            f"  MDD: {stats['mdd']:.1f}% (기준 -15% 이내)",
            "",
            f"[소스별]",
        ]

        if stats["morning_total"]:
            lines.append(
                f"  모닝: 승률 {stats['morning_win_rate']:.1f}% / {stats['morning_total']}건"
            )
        if stats["nxt_total"]:
            lines.append(
                f"  NXT: 승률 {stats['nxt_win_rate']:.1f}% / {stats['nxt_total']}건"
            )

        lines.extend([
            "",
            f"[청산 사유]",
            f"  TARGET: {stats['target_hit']}건 | "
            f"STOP: {stats['stop_hit']}건 | "
            f"TIME: {stats['time_stop']}건",
        ])

        # PASS/FAIL 상세
        criteria = result.get("criteria", {})
        lines.extend(["", "[판정 기준]"])
        labels = {
            "win_rate_45": "승률 45%+",
            "avg_pnl_positive": "평균수익 0.5%+",
            "cum_return_positive": "누적수익 0%+",
            "mdd_within_15": "MDD -15% 이내",
            "sample_size_5": "거래 5건+",
        }
        for key, label in labels.items():
            passed = criteria.get(key, False)
            mark = "O" if passed else "X"
            lines.append(f"  [{mark}] {label}")

        lines.append(f"\n  {result['passed']}/5 충족 -> {verdict}")

        if verdict == "PASS":
            lines.append("\n-> 자동매매 ON 전환 검토 가능")
        else:
            lines.append("\n-> 추가 검증 필요 (전략 조정 후 재시도)")

        lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
        return "\n".join(lines)
