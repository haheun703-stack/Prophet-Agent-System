# -*- coding: utf-8 -*-
"""scalper_bot_feedback 자동 적재 4종 hook + 자산별 추적 (P1, 5/18 형 요청)

형이 5/18 11:40 KST 요청:
- 14:00 중간 보고 (형이 새로 SELECT하는 시점)
- 15:30 즉시 정산 (평소 15:40 → 15:30 10분 앞당김 = 형 BAT-D 사전 데이터)

4종 적재 hook:
- hook_morning_recommendation() — 09:00 모닝 추천 + 5게이트 결과 (RECOMMENDATION)
- hook_buy_execution() — 매수 실행 직후 (ENTRY) + 자산 source 추적
- hook_sell_execution() — 청산 직후 (CLOSE) + PnL 자동 계산
- hook_daily_journal() — 15:30 일일 정산 (JOURNAL_ACCURACY) + source별 적중률

★ 자산별 추적 (사장님 5/18 지적) ★
어디서 발굴된 종목인지 entry_reason에 자동 명시:
- "limit_up_trigger" / "limit_up_watchlist" / "massive_dual_buy" / "oneshot_stealth"
- "accumulation_radar" / "foreign_accumulation" / "nationality_signal"
- "morning_rec" / "daily_news_opportunity" / "intraday_signal_positive"
- "quant_advisory_related"

→ 5/27+ 적중률 분석 시 "어떤 소스가 가장 정확한가" 자동 학습 가능
"""
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

logger = logging.getLogger("BH.ScalperHooks")


def hook_morning_recommendation(
    candidates: List[Dict],
    consensus_results: Optional[List[Dict]] = None,
) -> Optional[int]:
    """09:00 모닝 추천 + 5게이트 결과 자동 적재.

    Args:
        candidates: morning_recommendation 결과 [{code, name, sector, ...}]
        consensus_results: jarvis_consensus.decide_buy_consensus 결과 리스트
                          (없으면 기본값 사용)

    Returns:
        scalper_bot_feedback INSERT id
    """
    try:
        from utils.scalper_feedback import log_recommendation

        # 등급 분포 집계
        grade_counts = {"STRONG": 0, "MEDIUM": 0, "WATCH": 0, "SKIP": 0}
        if consensus_results:
            for r in consensus_results:
                grade = r.get("grade", "SKIP")
                grade_counts[grade] = grade_counts.get(grade, 0) + 1

        # 추천 종목 ticker 리스트
        tickers = [c.get("code", "") for c in candidates if c.get("code")]

        # 게이트 통과 통계
        gates_passed_n = 0
        gates_detail = {}
        if consensus_results:
            avg_gates = sum(
                len([r for r in consensus_results if r.get("decision") == "BUY"])
                for r in consensus_results
            )
            gates_passed_n = avg_gates
            gates_detail = {
                "total_candidates": len(candidates),
                "buy_decisions": sum(1 for r in consensus_results if r.get("decision") == "BUY"),
                "skip_decisions": sum(1 for r in consensus_results if r.get("decision") == "SKIP"),
                "strong_count": grade_counts["STRONG"],
                "medium_count": grade_counts["MEDIUM"],
                "watch_count": grade_counts["WATCH"],
            }

        return log_recommendation(
            grade="MIXED" if len(set(grade_counts.values())) > 1 else "STRONG",
            tickers=tickers,
            gates_passed_n=gates_passed_n,
            gates_detail=gates_detail,
            notes=f"morning_rec {len(candidates)}종목 + jarvis_consensus {len(consensus_results) if consensus_results else 0}건",
        )
    except Exception as e:
        logger.warning(f"hook_morning_recommendation 실패: {e}")
        return None


def determine_entry_source(ticker: str) -> List[str]:
    """매수 종목이 어떤 자산 소스에서 발굴됐는지 추적.

    Returns:
        ["limit_up_trigger", "massive_dual_buy", ...] 소스 리스트
    """
    sources = []
    try:
        from utils.asset_pool_loader import get_candidate_source_map
        source_map = get_candidate_source_map()
        sources.extend(source_map.get(ticker, []))
    except Exception as e:
        logger.debug(f"asset_pool source 조회 실패: {e}")

    # 막내 daily_news opportunity
    try:
        from utils.daily_news_subscriber import collect_opportunity_tickers_from_news
        if ticker in collect_opportunity_tickers_from_news():
            sources.append("daily_news_opportunity")
    except Exception:
        pass

    # 막내 intraday_signals positive
    try:
        from utils.intraday_signals_subscriber import collect_opportunity_tickers
        if ticker in collect_opportunity_tickers():
            sources.append("intraday_signal_positive")
    except Exception:
        pass

    # 큰형 advisory related
    try:
        from utils.quant_advisory_subscriber import fetch_latest_advisory
        for mt in ["MORNING_BRIEFING", "ADVICE", "LEADING"]:
            adv = fetch_latest_advisory(msg_type=mt)
            if adv and ticker in (adv.get("related_tickers") or []):
                sources.append(f"quant_advisory_{mt.lower()}")
                break
    except Exception:
        pass

    return sources or ["unknown"]


def hook_buy_execution(
    ticker: str,
    name: str,
    price: int,
    qty: int,
    grade: str,
    advisory_id: Optional[int] = None,
    source: str = "verification",
    extra_reason: str = "",
) -> Optional[int]:
    """매수 실행 직후 적재 (자산 source 자동 추적).

    Args:
        ticker: 종목코드
        name: 종목명
        price: 매수가
        qty: 수량
        grade: STRONG/MEDIUM/WATCH
        advisory_id: 참조한 형 advisory id
        source: 'verification' | 'morning_rec' | 'intraday_scan' | 'limit_up_split'
        extra_reason: 추가 진입 사유 (예: "5게이트 PASS, 막내 차단 X")
    """
    try:
        from utils.scalper_feedback import log_entry

        # 자산 소스 자동 추적
        asset_sources = determine_entry_source(ticker)

        # 진입 사유 통합
        reason_parts = [extra_reason] if extra_reason else []
        reason_parts.append(f"sources=[{','.join(asset_sources)}]")
        if advisory_id:
            reason_parts.append(f"advisory_id={advisory_id}")
        reason = " | ".join(reason_parts)

        return log_entry(
            ticker=ticker,
            name=name,
            price=price,
            qty=qty,
            grade=grade,
            reason=reason,
            source=source,
            advisory_id=advisory_id,
            notes=f"asset_sources: {asset_sources}",
        )
    except Exception as e:
        logger.warning(f"hook_buy_execution 실패: {e}")
        return None


def hook_sell_execution(
    ticker: str,
    name: str,
    entry_price: int,
    close_price: int,
    qty: int = 1,
    sell_reason: str = "15:25 강제 청산",
    holding_minutes: Optional[int] = None,
) -> Optional[int]:
    """청산 실행 직후 적재 (PnL 자동 계산)."""
    try:
        from utils.scalper_feedback import log_close
        pnl_amount = (close_price - entry_price) * qty
        pnl_pct = round((close_price / entry_price - 1) * 100, 2) if entry_price else 0
        return log_close(
            ticker=ticker,
            price=close_price,
            pnl_pct=pnl_pct,
            pnl_amount=pnl_amount,
            reason=sell_reason,
            holding_minutes=holding_minutes,
            notes=f"entry={entry_price:,} qty={qty}",
        )
    except Exception as e:
        logger.warning(f"hook_sell_execution 실패: {e}")
        return None


def hook_midday_report() -> Optional[int]:
    """14:00 중간 보고 (형 요청 — 형 새 SELECT 시점 맞춤).

    오전 매수 결과 + 보유 종목 상태 요약 적재.
    """
    try:
        from utils.scalper_feedback import _insert
        import json

        # 오늘 entry 종목 카운트 (TODO: DB SELECT로 정확히)
        # 임시: notes로 14:00 시점 보고
        now = datetime.now(KST) if KST else datetime.now()
        sql = """
            INSERT INTO scalper_bot_feedback
                (feedback_date, feedback_time, msg_type, notes)
            VALUES (%s, %s, 'MIDDAY_REPORT', %s)
            RETURNING id
        """
        params = (
            now.date(),
            now.strftime("%H:%M:%S"),
            f"14:00 중간 보고 — 형 새 SELECT 시점 (오전 매수/청산 요약)",
        )
        return _insert(sql, params)
    except Exception as e:
        logger.warning(f"hook_midday_report 실패: {e}")
        return None


def hook_daily_journal(
    total_trades: int,
    wins: int,
    losses: int,
    by_source: Optional[Dict] = None,
    avg_pnl_pct: Optional[float] = None,
) -> Optional[int]:
    """15:30 일일 정산 (평소 15:40 → 15:30 10분 앞당김, 형 BAT-D 사전 데이터)."""
    try:
        from utils.scalper_feedback import log_journal_accuracy
        return log_journal_accuracy(
            total=total_trades,
            wins=wins,
            losses=losses,
            by_source=by_source,
            avg_pnl_pct=avg_pnl_pct,
            notes="15:30 정산 (형 BAT-D 사전 데이터, 평소 15:40 → 10분 앞당김)",
        )
    except Exception as e:
        logger.warning(f"hook_daily_journal 실패: {e}")
        return None


if __name__ == "__main__":
    print("=== scalper_journal_hooks 자가 진단 ===")
    print()

    # 1. 자산 source 추적 테스트 — 제주반도체
    print("🎯 제주반도체 (080220) 자산 source 추적:")
    sources = determine_entry_source("080220")
    print(f"  {sources}")

    print()
    print("🎯 HL만도 (204320) 자산 source 추적:")
    sources = determine_entry_source("204320")
    print(f"  {sources}")

    print()
    print("🎯 두산로보틱스 (454910) 자산 source 추적:")
    sources = determine_entry_source("454910")
    print(f"  {sources}")

    print()
    print("★ 5/19 첫 가동 시 4종 hook 자동 호출:")
    print("  09:00 hook_morning_recommendation")
    print("  09:00~14:00 hook_buy_execution (자산 source 자동 추적)")
    print("  14:00 hook_midday_report (형 새 SELECT 시점)")
    print("  15:25 hook_sell_execution (청산 + PnL)")
    print("  15:30 hook_daily_journal (평소 15:40 → 15:30)")
