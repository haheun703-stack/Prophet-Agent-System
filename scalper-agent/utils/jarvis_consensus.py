# -*- coding: utf-8 -*-
"""자비스 v3.0 3봇 일치 황금 표준 매수/청산 결정 로직 (5/18 사장님 자비스 정신 부여)

★ 봇 가족 위계 (사장님 결선) ★
- 큰형 (퀀트봇): quant_bot_advisory (msg_type 4종)
- 둘째형 (단타봇, 이 모듈): 5게이트 + 등급 차등 + 분산 원칙
- 막내 (정보봇): daily_news + intraday_signals (expire_at 필터)

★ 3봇 일치 황금 표준 ★
매수: 큰형 advisory != CAUTION + 막내 NEGA 차단 X + 둘째형 5게이트 PASS
청산: 큰형 LEADING WARN OR 막내 새 NEGA impact 70+ OR 둘째형 -2% 안전망

★ 자비스 정신 5원칙 ★
1. 두려움 X — 시스템 신뢰
2. 시장 전체 동적 발굴 (4종 자산 + 형 + 막내)
3. VWAP/호가/체결강도 실시간 동적 대응
4. 스캘핑 +1/+2/+3% 분할 익절
5. 유연한 행동
"""
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

try:
    from zoneinfo import ZoneInfo
    KST = ZoneInfo("Asia/Seoul")
except ImportError:
    KST = None

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))

from dotenv import load_dotenv
load_dotenv(_ROOT.parent / ".env")

logger = logging.getLogger("BH.JarvisConsensus")


# ─────────────────────────────────────────────────────
# 매수 결정 — 3봇 일치 황금 표준
# ─────────────────────────────────────────────────────
def check_macro_safety() -> Dict:
    """매크로 안전 게이트 — 큰형 + 막내 종합 판단.

    Returns:
        {
            "safe": bool,              # 진입 안전 여부
            "strict_mode": bool,       # STRICT 모드 (STRONG만 진입)
            "skip_all": bool,          # 진입 전체 자제
            "regime": str,             # 큰형 regime
            "risk_level": str,         # 막내 risk_level
            "reasons": [str, ...],     # 판단 근거
        }
    """
    result = {
        "safe": True,
        "strict_mode": False,
        "skip_all": False,
        "regime": None,
        "risk_level": None,
        "reasons": [],
    }

    # 1축. 큰형 MORNING_BRIEFING
    try:
        from utils.quant_advisory_subscriber import fetch_latest_advisory
        mb = fetch_latest_advisory(msg_type="MORNING_BRIEFING")
        if mb:
            result["regime"] = mb.get("market_regime")
            if mb["market_regime"] == "CAUTION":
                result["strict_mode"] = True
                result["reasons"].append(f"큰형 MORNING_BRIEFING regime=CAUTION → STRICT")
            reasoning = mb.get("reasoning") or {}
            vix = reasoning.get("vix", 0) if isinstance(reasoning, dict) else 0
            if vix > 25:
                result["strict_mode"] = True
                result["reasons"].append(f"VIX {vix} > 25 → STRICT")
    except Exception as e:
        logger.debug(f"큰형 MORNING_BRIEFING 조회 실패: {e}")

    # 1축. 큰형 SNAPSHOT (인버스 매크로)
    try:
        from utils.quant_advisory_subscriber import fetch_latest_advisory
        snap = fetch_latest_advisory(msg_type="SNAPSHOT")
        if snap:
            inv = snap.get("inverse_etf_strength") or 0
            mkt = snap.get("market_strength_avg") or 100
            if inv and inv >= 120 and mkt < 90:
                result["skip_all"] = True
                result["reasons"].append(f"큰형 SNAPSHOT 인버스 {inv}+시장 {mkt} → SKIP ALL")
    except Exception as e:
        logger.debug(f"큰형 SNAPSHOT 조회 실패: {e}")

    # 2축. 막내 daily_news 매크로
    try:
        from utils.daily_news_subscriber import get_macro_risk_summary
        macro = get_macro_risk_summary()
        result["risk_level"] = macro["risk_level"]
        if macro["risk_level"] == "HIGH":
            result["strict_mode"] = True
            result["reasons"].append(f"막내 daily_news risk=HIGH (NEGA {len(macro['negative_high'])}건) → STRICT")
    except Exception as e:
        logger.debug(f"막내 daily_news 조회 실패: {e}")

    if result["skip_all"]:
        result["safe"] = False

    return result


def collect_blocked_tickers() -> Set[str]:
    """매수 차단 종목 통합 — 막내 daily_news NEGA + intraday_signals NEGA."""
    blocked: Set[str] = set()

    # 막내 daily_news NEGATIVE 강도 5+
    try:
        from utils.daily_news_subscriber import collect_blocked_tickers_from_news
        blocked.update(collect_blocked_tickers_from_news())
    except Exception as e:
        logger.debug(f"daily_news 차단 종목 실패: {e}")

    # 막내 intraday_signals NEGA impact 70+ (expire_at 필터 자동)
    try:
        from utils.intraday_signals_subscriber import collect_blocked_tickers as _block_intraday
        blocked.update(_block_intraday())
    except Exception as e:
        logger.debug(f"intraday_signals 차단 종목 실패: {e}")

    return blocked


def collect_high_confidence_candidates() -> List[Dict]:
    """4종 자산 통합 고신뢰 매수 후보 (2개+ 자산 일치)."""
    try:
        from utils.asset_pool_loader import (
            get_candidate_source_map,
            get_high_confidence_candidates,
        )
        codes = get_high_confidence_candidates()
        source_map = get_candidate_source_map()
        return [{"code": c, "sources": source_map[c]} for c in codes]
    except Exception as e:
        logger.warning(f"asset_pool 조회 실패: {e}")
        return []


def decide_buy_consensus(
    candidate: Dict,
    five_gates_passed_n: int,
) -> Dict:
    """3봇 일치 매수 결정 (단일 종목 후보 + 5게이트 결과 → 매수 결정).

    Args:
        candidate: {"code": "...", "name": "...", ...}
        five_gates_passed_n: 둘째형 5게이트 PASS 개수 (0~5)

    Returns:
        {
            "decision": "BUY" | "SKIP",
            "grade": "STRONG" | "MEDIUM" | "WATCH" | "SKIP",
            "amount": int,           # 매수 금액 (원)
            "reasons": [str, ...],   # 판단 근거
            "consensus_score": int,  # 3봇 합의 점수 (0~3)
        }
    """
    code = candidate.get("code", "")
    reasons = []
    consensus = 0

    # 1축. 큰형 + 막내 매크로 안전
    macro = check_macro_safety()
    if macro["skip_all"]:
        return {"decision": "SKIP", "grade": "SKIP", "amount": 0,
                "reasons": ["매크로 SKIP_ALL"] + macro["reasons"], "consensus_score": 0}

    if not macro["strict_mode"]:
        consensus += 1
        reasons.append("큰형 advisory OK")
    else:
        reasons.append("큰형/막내 STRICT 모드")

    # 2축. 막내 차단 종목 체크
    blocked = collect_blocked_tickers()
    if code in blocked:
        return {"decision": "SKIP", "grade": "SKIP", "amount": 0,
                "reasons": [f"막내 차단 종목 ({code})"] + reasons, "consensus_score": 0}
    consensus += 1
    reasons.append("막내 차단 X")

    # 3축. 둘째형 5게이트
    if five_gates_passed_n >= 5:
        grade = "STRONG"
        consensus += 1
        reasons.append("둘째형 5/5 게이트 PASS")
    elif five_gates_passed_n >= 4:
        grade = "MEDIUM"
        reasons.append("둘째형 4/5 게이트 PASS")
    elif five_gates_passed_n >= 3:
        grade = "WATCH"
        reasons.append("둘째형 3/5 게이트 PASS (관찰만)")
    else:
        return {"decision": "SKIP", "grade": "SKIP", "amount": 0,
                "reasons": [f"둘째형 {five_gates_passed_n}/5 게이트 부족"] + reasons,
                "consensus_score": consensus}

    # STRICT 모드일 때는 STRONG만 진입
    if macro["strict_mode"] and grade != "STRONG":
        return {"decision": "SKIP", "grade": grade, "amount": 0,
                "reasons": [f"STRICT 모드 → STRONG만 진입 ({grade} SKIP)"] + reasons,
                "consensus_score": consensus}

    # 매수 금액 결정 (사장님 자비스 정신 + STRICT 조정)
    if grade == "STRONG":
        amount = 1_000_000 if macro["strict_mode"] else 2_000_000  # STRICT 100만 / 정상 200만
    elif grade == "MEDIUM":
        amount = 500_000 if macro["strict_mode"] else 1_000_000     # STRICT 50만 / 정상 100만
    else:  # WATCH
        amount = 0  # 관찰만, 보유 X

    decision = "BUY" if amount > 0 else "SKIP"
    return {
        "decision": decision,
        "grade": grade,
        "amount": amount,
        "reasons": reasons,
        "consensus_score": consensus,
        "macro": macro,
    }


# ─────────────────────────────────────────────────────
# 청산 결정 — 한 축이라도 위험 시 즉시 청산
# ─────────────────────────────────────────────────────
def check_position_for_sell(
    ticker: str,
    entry_price: int,
    current_price: int,
    last_check_at: Optional[datetime] = None,
) -> Dict:
    """보유 종목 실시간 청산 결정.

    Args:
        ticker: 종목 코드
        entry_price: 매수가
        current_price: 현재가
        last_check_at: 직전 체크 시각 (새 시그널만 필터)

    Returns:
        {
            "sell": bool,
            "reason": str,
            "pnl_pct": float,
        }
    """
    pnl_pct = (current_price - entry_price) / entry_price * 100 if entry_price else 0
    result = {"sell": False, "reason": "", "pnl_pct": round(pnl_pct, 2)}

    # 3축. 둘째형 -2% 안전망
    if pnl_pct <= -2.0:
        result["sell"] = True
        result["reason"] = f"둘째형 -2% 안전망 ({pnl_pct:.2f}%)"
        return result

    # 1축. 큰형 LEADING WARN (related_tickers 매칭)
    try:
        from utils.quant_advisory_subscriber import fetch_latest_advisory
        leading = fetch_latest_advisory(msg_type="LEADING")
        if leading and leading.get("severity") == "WARN":
            tickers = leading.get("related_tickers") or []
            if ticker in tickers:
                result["sell"] = True
                result["reason"] = f"큰형 LEADING WARN ({leading.get('title', '')[:50]})"
                return result
    except Exception as e:
        logger.debug(f"큰형 LEADING 체크 실패: {e}")

    # 2축. 막내 새 NEGA impact 70+ (expire_at 활성)
    try:
        from utils.intraday_signals_subscriber import check_position_for_sell as _check_intraday
        since = last_check_at.isoformat() if last_check_at else None
        sig = _check_intraday(ticker, since=since)
        if sig:
            result["sell"] = True
            result["reason"] = f"막내 NEGA impact {sig.get('impact_score', '?')} ({sig.get('title', '')[:50]})"
            return result
    except Exception as e:
        logger.debug(f"막내 intraday_signals 체크 실패: {e}")

    return result


# ─────────────────────────────────────────────────────
# 스캘핑 분할 익절 결정
# ─────────────────────────────────────────────────────
def check_scalping_profit_take(
    entry_price: int,
    current_price: int,
    partial_taken: int = 0,
) -> Dict:
    """스캘핑 분할 익절 결정 (+1%/+2%/+3% 분할).

    Args:
        partial_taken: 이미 분할 익절한 횟수 (0/1/2)

    Returns:
        {
            "action": "HOLD" | "PARTIAL_SELL" | "FULL_SELL",
            "ratio": float,          # 매도 비율 (1/3, 1/3, 1.0)
            "reason": str,
            "pnl_pct": float,
        }
    """
    pnl_pct = (current_price - entry_price) / entry_price * 100 if entry_price else 0
    result = {"action": "HOLD", "ratio": 0.0, "reason": "", "pnl_pct": round(pnl_pct, 2)}

    if pnl_pct >= 3.0:
        result["action"] = "FULL_SELL"
        result["ratio"] = 1.0
        result["reason"] = f"+3% 도달 ({pnl_pct:.2f}%) → 전체 익절"
    elif pnl_pct >= 2.0 and partial_taken < 2:
        result["action"] = "PARTIAL_SELL"
        result["ratio"] = 1 / 3
        result["reason"] = f"+2% 도달 ({pnl_pct:.2f}%) → 2차 분할 익절"
    elif pnl_pct >= 1.0 and partial_taken < 1:
        result["action"] = "PARTIAL_SELL"
        result["ratio"] = 1 / 3
        result["reason"] = f"+1% 도달 ({pnl_pct:.2f}%) → 1차 분할 익절"
    return result


# ─────────────────────────────────────────────────────
# 사장님 5대 원칙 자동 체크
# ─────────────────────────────────────────────────────
def check_lifelong_principles(
    candidate: Dict,
    current_positions: List[Dict],
    cash_ratio: float,
) -> Dict:
    """사장님 5대 원칙 자동 적용 (5/18 영구 약속).

    Args:
        candidate: 매수 후보
        current_positions: 현재 보유 종목 리스트 [{"code", "sector", ...}]
        cash_ratio: 현재 cash / total_eval

    Returns:
        {
            "principles_ok": bool,
            "violations": [str, ...],  # 위반 사항
        }
    """
    violations = []
    sector = candidate.get("sector", "")

    # 원칙 ① 한 테마 집중 절대 금지 (같은 섹터 최대 2종목)
    same_sector_count = sum(1 for p in current_positions if p.get("sector") == sector)
    if sector and same_sector_count >= 2:
        violations.append(f"원칙 ① 위반: 같은 섹터({sector}) 이미 2종목 보유")

    # 원칙 ④ 현금 비중 30%+ 항상 유지
    if cash_ratio < 0.30:
        violations.append(f"원칙 ④ 위반: 현금 비중 {cash_ratio:.0%} < 30%")

    # 원칙 ②, ③, ⑤는 다른 모듈에서 처리:
    # ② 매수 전 5단계 게이트 → decide_buy_consensus
    # ③ 매수 후 5분 대응 → check_scalping_profit_take + check_position_for_sell
    # ⑤ 자책 금지 → 코드 아닌 행동 원칙

    return {
        "principles_ok": len(violations) == 0,
        "violations": violations,
    }


if __name__ == "__main__":
    print("=== jarvis_consensus 자가 진단 ===")
    print()

    # 1. 매크로 안전 체크
    macro = check_macro_safety()
    print(f"📊 매크로 안전 체크:")
    print(f"  safe: {macro['safe']}, strict_mode: {macro['strict_mode']}, skip_all: {macro['skip_all']}")
    print(f"  큰형 regime: {macro['regime']} / 막내 risk_level: {macro['risk_level']}")
    for r in macro['reasons']:
        print(f"  - {r}")

    # 2. 차단 종목
    blocked = collect_blocked_tickers()
    print()
    print(f"❌ 차단 종목 {len(blocked)}: {list(blocked)[:10]}")

    # 3. 고신뢰 후보
    high_conf = collect_high_confidence_candidates()
    print()
    print(f"⭐ 고신뢰 매수 후보 {len(high_conf)}:")
    for c in high_conf[:10]:
        print(f"  {c['code']} ← {c['sources']}")

    # 4. 시뮬 매수 결정 (제주반도체 - 어제 적중)
    print()
    print(f"🎯 시뮬 매수 결정 — 제주반도체 (5/5 게이트 가정):")
    decision = decide_buy_consensus(
        candidate={"code": "080220", "name": "제주반도체", "sector": "반도체"},
        five_gates_passed_n=5,
    )
    print(f"  결정: {decision['decision']} / 등급: {decision['grade']} / 금액: {decision['amount']:,}원")
    print(f"  합의 점수: {decision['consensus_score']}/3")
    for r in decision['reasons']:
        print(f"  - {r}")
