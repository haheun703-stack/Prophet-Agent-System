# -*- coding: utf-8 -*-
"""PAPER_MODE 안전망 가드 + 형 실전 vs 동생 페이퍼 비교 시스템 (P2)

5/20 D-Day 출격일 안전망:
- .env PAPER_MODE_2026_05_20=true → 단타봇 실거래 차단, 페이퍼만 적재
- 형(퀀트봇) 실전 매수 결과 vs 동생(단타봇) 페이퍼 결과 = 진짜 학습 데이터

사장님 자비스 정신 + 안전망 균형:
- 자비스 정신: 두려움 X, 시장 전체 동적 발굴
- 안전망 PAPER: 5/20 첫 실전이라 일부만 페이퍼로 비교 학습

운영 시나리오 (5/20):
- 사장님 KIS HTS에서 1주 / 10만원만 실제 매수 (사장님 직접)
- 단타봇은 v3.0 페이퍼 모드 — 형 advisory + 막내 시그널 + 5게이트 결정만 적재
- 16:30 형 BAT-D SELECT → 동생 페이퍼 entry vs 사장님 실전 entry 비교
- 17:00 4봇 회의 — 같은 시그널 받았는데 누가 더 잘했나 분석
"""
import logging
import os
import sys
from datetime import datetime, date
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

logger = logging.getLogger("BH.PaperGuard")


# 5/20 D-Day 페이퍼 모드 활성 날짜 범위
PAPER_MODE_START = date(2026, 5, 20)
PAPER_MODE_END = date(2026, 5, 20)  # 1일만 (5/20 D-Day)


def is_paper_mode_active() -> bool:
    """페이퍼 모드 활성 여부.

    조건:
    1. .env PAPER_MODE_2026_05_20=true
    2. 또는 오늘이 PAPER_MODE_START ~ END 범위 + .env에 명시적 X 비활성화 안 됨
    """
    # .env 명시적 토글 우선
    env_toggle = os.getenv("PAPER_MODE_2026_05_20", "").strip().lower()
    if env_toggle == "true":
        return True
    if env_toggle == "false":
        return False

    # 날짜 범위 자동 활성화
    today = (datetime.now(KST) if KST else datetime.now()).date()
    return PAPER_MODE_START <= today <= PAPER_MODE_END


def should_block_real_trade(reason_hint: str = "") -> Dict:
    """실거래 차단 결정.

    Args:
        reason_hint: 호출 컨텍스트 (예: "_decide_verification")

    Returns:
        {
            "block": bool,
            "reason": str,
            "alternative": str,  # 차단 시 대체 행동
        }
    """
    if not is_paper_mode_active():
        return {"block": False, "reason": "PAPER_MODE 비활성", "alternative": ""}

    return {
        "block": True,
        "reason": f"5/20 D-Day PAPER 모드 ({reason_hint})",
        "alternative": "scalper_bot_feedback에 페이퍼 entry 적재 (source='paper_mode_5_20')",
    }


def log_paper_entry(
    ticker: str,
    name: str,
    intended_price: int,
    intended_qty: int,
    grade: str,
    advisory_id: Optional[int] = None,
    extra_reason: str = "",
) -> Optional[int]:
    """페이퍼 모드 entry 적재 (실거래 X, scalper_bot_feedback만).

    Args:
        ticker, name, intended_price, intended_qty, grade: 의도했던 매수 정보
        advisory_id: 참조한 형 advisory id
        extra_reason: 페이퍼 진입 사유

    Returns:
        scalper_bot_feedback INSERT id
    """
    try:
        from utils.scalper_journal_hooks import hook_buy_execution
        return hook_buy_execution(
            ticker=ticker,
            name=name,
            price=intended_price,
            qty=intended_qty,
            grade=grade,
            advisory_id=advisory_id,
            source="paper_mode_5_20",  # ★ 페이퍼 모드 표시
            extra_reason=f"[PAPER 모드] {extra_reason} (5/20 안전망 — 실거래 X)",
        )
    except Exception as e:
        logger.warning(f"log_paper_entry 실패: {e}")
        return None


def get_today_paper_entries() -> List[Dict]:
    """오늘 페이퍼 모드 entry 조회 (형 BAT-D SELECT용)."""
    try:
        from utils.supabase_sql import query
        rows = query(
            """
            SELECT id, feedback_time, entry_ticker, entry_name,
                   entry_price, entry_qty, entry_amount, entry_grade,
                   entry_reason, entry_advisory_ref
            FROM scalper_bot_feedback
            WHERE feedback_date = %s
              AND msg_type = 'ENTRY'
              AND entry_source = 'paper_mode_5_20'
            ORDER BY feedback_time ASC
            """,
            ((datetime.now(KST) if KST else datetime.now()).date(),),
        )
        return rows
    except Exception as e:
        logger.warning(f"오늘 페이퍼 entry 조회 실패: {e}")
        return []


def compare_with_hyung_real_trades() -> Dict:
    """형(퀀트봇) 실전 vs 동생(단타봇) 페이퍼 비교 (5/20 저녁 호출).

    형이 quant_bot_advisory에 실전 매수 결과를 적재했다면 비교.

    Returns:
        {
            "동생_paper_entries": N,
            "형_real_entries": N,  # 형이 실전 적재했다면
            "공통_종목": [...],
            "동생만": [...],
            "형만": [...],
        }
    """
    동생_papers = get_today_paper_entries()
    동생_tickers = {p.get("entry_ticker") for p in 동생_papers if p.get("entry_ticker")}

    # 형 실전 종목 조회 (quant_bot_advisory의 related_tickers)
    형_tickers = set()
    try:
        from utils.quant_advisory_subscriber import today_all
        for adv in today_all():
            if adv.get("msg_type") in ("ADVICE", "LEADING"):
                for t in adv.get("related_tickers", []) or []:
                    형_tickers.add(t)
    except Exception as e:
        logger.debug(f"형 advisory 조회 실패: {e}")

    return {
        "동생_paper_entries": len(동생_papers),
        "형_advisory_tickers": len(형_tickers),
        "공통_종목": list(동생_tickers & 형_tickers),
        "동생만": list(동생_tickers - 형_tickers),
        "형만": list(형_tickers - 동생_tickers),
        "동생_paper_details": 동생_papers,
    }


if __name__ == "__main__":
    print("=== paper_mode_guard 자가 진단 ===")
    print()

    today = (datetime.now(KST) if KST else datetime.now()).date()
    print(f"📅 오늘: {today}")
    print(f"📅 PAPER 활성 기간: {PAPER_MODE_START} ~ {PAPER_MODE_END}")
    print(f"📅 .env PAPER_MODE_2026_05_20: {os.getenv('PAPER_MODE_2026_05_20', '미설정')}")
    print(f"⚙️  is_paper_mode_active(): {is_paper_mode_active()}")
    print()

    decision = should_block_real_trade(reason_hint="자가진단")
    print(f"🛡️ 실거래 차단 결정:")
    print(f"  block: {decision['block']}")
    print(f"  reason: {decision['reason']}")
    print(f"  alternative: {decision['alternative']}")
    print()

    # 비교 시뮬
    cmp = compare_with_hyung_real_trades()
    print(f"📊 형 실전 vs 동생 페이퍼 비교 (오늘 시뮬):")
    print(f"  동생 paper entries: {cmp['동생_paper_entries']}건")
    print(f"  형 advisory tickers: {cmp['형_advisory_tickers']}건")
    print(f"  공통 종목: {cmp['공통_종목']}")
    print(f"  동생만: {cmp['동생만'][:5]}")
    print(f"  형만: {cmp['형만'][:5]}")
    print()
    print("★ 5/20 D-Day:")
    print("  - .env에 PAPER_MODE_2026_05_20=true 설정 시 단타봇 실거래 차단")
    print("  - 또는 5/20 날짜 자동 감지 (PAPER_MODE_START ~ END 범위)")
    print("  - 사장님 KIS HTS에서 1주/10만 실전 매수 (직접)")
    print("  - 단타봇은 자비스 v3.0 결정만 페이퍼로 적재")
    print("  - 17:00 형 BAT-D + 4봇 회의 시 비교 분석")
