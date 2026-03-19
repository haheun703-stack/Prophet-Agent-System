# -*- coding: utf-8 -*-
"""
14:30 프리클로즈 스캐너 — 장중 데이터 기반 "내일 후보" 도출
=============================================================
저녁 분석(16:45): 확정 일봉 기반 → 정밀하지만 늦음
프리클로즈(14:30): 장중 미확정 데이터 → 덜 정밀하지만 빠름

둘 다 돌린다. 기존 저녁 분석은 그대로 유지.
프리클로즈는 "오늘 장중에 뭔 일이 있었나" 기반으로
"내일 움직일 가능성이 높은 종목"을 찾는다.

Usage:
  python data/preclose_scanner.py                # 14:30 스캔 시뮬레이션
  python data/preclose_scanner.py --test         # 저장된 intraday 결과 기반 테스트
"""

import json
import logging
import time
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Optional

logger = logging.getLogger("BH.PrecloseScanner")

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data_store"

# 억원 단위
_BILLION = 1e8


# =============================================================================
# 데이터 구조
# =============================================================================

@dataclass
class PrecloseCandidate:
    """프리클로즈 후보 종목"""
    code: str
    name: str
    sector: str

    # 프리클로즈 스코어링 (100점 만점)
    preclose_score: float
    score_detail: dict       # {factor: points}

    # 장중 데이터 요약
    intraday_tv_ratio: float   # 장중 TV 비율
    tv_pattern: str            # EXPLOSION / QUIET_ACCUMULATION / ...
    change_pct: float          # 등락률
    current_price: int
    acceleration: float        # 거래대금 가속도

    # 수급 요약
    inst_net_buy: float        # 기관 순매수 (억원)
    foreign_net_buy: float     # 외인 순매수 (억원)

    # Trade Object (R:R 게이트 통과 시)
    trade_object: Optional[dict] = None
    rr_verdict: str = ""


# =============================================================================
# 프리클로즈 가중치 (100점 만점)
# =============================================================================

PRECLOSE_WEIGHTS = {
    "intraday_tv_pattern": 30,     # 장중 거래대금 패턴 (A의 핵심)
    "intraday_supply": 25,         # 장중 기관/외인 누적 순매수
    "intraday_price_action": 15,   # 장중 가격 행동
    "sector_today": 15,            # 오늘 섹터 상태
    "macro_current": 10,           # 현재 미국선물/VIX/환율
    "news_today": 5,               # 오늘 뉴스
}


# =============================================================================
# 개별 팩터 스코어링
# =============================================================================

def _score_tv_pattern(tv_ratio: float, pattern: str, acceleration: float) -> float:
    """팩터 1: 장중 거래대금 패턴 (max 30)"""
    score = 0.0

    # TV 비율 기본점수
    if tv_ratio >= 5.0:
        score += 18
    elif tv_ratio >= 3.0:
        score += 14
    elif tv_ratio >= 2.0:
        score += 10
    elif tv_ratio >= 1.5:
        score += 5

    # 패턴 보너스
    if pattern == "QUIET_ACCUMULATION":
        score += 8  # 가장 강한 시그널
    elif pattern == "EXPLOSION":
        score += 5
    elif pattern == "GRADUAL_BUILDUP":
        score += 6

    # 가속도 보너스
    if acceleration >= 2.0:
        score += 4
    elif acceleration >= 1.5:
        score += 2

    return min(30, score)


def _score_supply(inst_net: float, foreign_net: float) -> float:
    """팩터 2: 장중 수급 (max 25)

    inst_net, foreign_net: 억원 단위 순매수
    """
    score = 0.0

    # 기관 (max 15)
    if inst_net >= 50:
        score += 15
    elif inst_net >= 20:
        score += 12
    elif inst_net >= 5:
        score += 8
    elif inst_net > 0:
        score += 4

    # 외인 (max 10)
    if foreign_net >= 30:
        score += 10
    elif foreign_net >= 10:
        score += 7
    elif foreign_net >= 3:
        score += 4
    elif foreign_net > 0:
        score += 2

    return min(25, score)


def _score_price_action(change_pct: float, high_price: int = 0,
                        current_price: int = 0) -> float:
    """팩터 3: 장중 가격 행동 (max 15)

    눌림 후 횡보가 가장 좋음 (매집 + 안정)
    """
    score = 0.0
    abs_change = abs(change_pct)

    # 가격 안정성 (|등락률| 작을수록 좋음)
    if abs_change <= 1.0:
        score += 10
    elif abs_change <= 2.0:
        score += 8
    elif abs_change <= 3.0:
        score += 5
    elif abs_change <= 5.0:
        score += 2

    # 종가 위치 (고가 대비 — 상한 마감에 가까울수록)
    if high_price > 0 and current_price > 0:
        ratio = current_price / high_price
        if ratio >= 0.97:
            score += 5
        elif ratio >= 0.93:
            score += 3
        elif ratio >= 0.90:
            score += 1

    return min(15, score)


def _score_sector(sector: str) -> float:
    """팩터 4: 오늘 섹터 상태 (max 15)

    HOT 섹터의 종목이면 가점
    """
    score = 0.0
    try:
        sector_path = DATA_DIR / "sector_flow.json"
        if sector_path.exists():
            data = json.loads(sector_path.read_text(encoding="utf-8"))
            sectors = data.get("sectors", {})
            info = sectors.get(sector, {})
            phase = info.get("phase", "COLD")
            if phase == "HOT":
                score += 15
            elif phase == "WARMING":
                score += 10
            elif phase == "NORMAL":
                score += 5
    except Exception:
        score += 5  # 섹터 데이터 없으면 기본 5점
    return min(15, score)


def _score_macro() -> float:
    """팩터 5: 현재 매크로 환경 (max 10)"""
    score = 5.0  # 기본 중립

    try:
        nw_path = DATA_DIR / "nightwatch.json"
        if nw_path.exists():
            nw = json.loads(nw_path.read_text(encoding="utf-8"))
            sentiment = nw.get("sentiment", "NEUTRAL")
            if sentiment == "BULLISH":
                score = 10.0
            elif sentiment == "BEARISH":
                score = 2.0
            else:
                score = 5.0
    except Exception:
        pass

    return score


def _score_news(code: str) -> float:
    """팩터 6: 오늘 뉴스 감성 (max 5)"""
    # 현재 뉴스 감성분석이 없으므로 중립 기본값
    return 2.5


# =============================================================================
# 메인 스캔
# =============================================================================

def scan_preclose(
    trader=None,
    tv_avgs: dict = None,
    min_preclose_score: float = 50.0,
    max_candidates: int = 5,
    equity: int = 50_000_000,
) -> list[PrecloseCandidate]:
    """14:30 프리클로즈 스캔

    1단계: 장중 TV 실시간 결과에서 패턴 감지된 종목 1차 필터
    2단계: 프리클로즈 6팩터 스코어링 (100점 만점)
    3단계: Trade Object 생성 (R:R 게이트 적용)
    4단계: R:R 높은 순 정렬 → 상위 max_candidates개 추천

    Args:
        trader: KISTrader 인스턴스
        tv_avgs: precompute_tv_averages() 결과
        min_preclose_score: 최소 프리클로즈 점수
        max_candidates: 최종 추천 종목 수
        equity: 총 자산 (원)

    Returns:
        PrecloseCandidate 리스트 (score 내림차순)
    """
    from data.trading_value_scanner import load_intraday_results

    # ── 1단계: 장중 TV 신호 로드 ──
    intraday_signals = load_intraday_results()
    if not intraday_signals:
        logger.info("[Preclose] 장중 TV 신호 없음 — 스킵")
        return []

    logger.info(f"[Preclose] 장중 TV 신호 {len(intraday_signals)}건 로드")

    # ── 2단계: 프리클로즈 스코어링 ──
    candidates = []
    for sig in intraday_signals:
        code = sig.get("code", "")
        name = sig.get("name", "")
        sector = sig.get("sector", "")
        if not code:
            continue

        # 장중 수급 조회 (KIS API)
        inst_net = 0.0
        foreign_net = 0.0
        high_price = sig.get("current_price", 0)
        if trader:
            try:
                p = trader.fetch_price(code)
                if p and p.get("success"):
                    high_price = p.get("high", high_price)
                # 투자자별 매매동향 조회 (일별)
                inv = trader.fetch_investor(code)
                if inv and inv.get("success"):
                    inst_net = inv.get("inst_net", 0) / _BILLION  # 억원
                    foreign_net = inv.get("foreign_net", 0) / _BILLION
            except Exception:
                pass

        # 6팩터 스코어링
        s1 = _score_tv_pattern(
            sig.get("estimated_tv_ratio", 0),
            sig.get("pattern", ""),
            sig.get("acceleration", 1.0),
        )
        s2 = _score_supply(inst_net, foreign_net)
        s3 = _score_price_action(
            sig.get("change_pct", 0),
            high_price,
            sig.get("current_price", 0),
        )
        s4 = _score_sector(sector)
        s5 = _score_macro()
        s6 = _score_news(code)

        total = s1 + s2 + s3 + s4 + s5 + s6

        if total < min_preclose_score:
            continue

        candidates.append(PrecloseCandidate(
            code=code,
            name=name,
            sector=sector,
            preclose_score=round(total, 1),
            score_detail={
                "tv_pattern": round(s1, 1),
                "supply": round(s2, 1),
                "price_action": round(s3, 1),
                "sector": round(s4, 1),
                "macro": round(s5, 1),
                "news": round(s6, 1),
            },
            intraday_tv_ratio=sig.get("estimated_tv_ratio", 0),
            tv_pattern=sig.get("pattern", ""),
            change_pct=sig.get("change_pct", 0),
            current_price=sig.get("current_price", 0),
            acceleration=sig.get("acceleration", 1.0),
            inst_net_buy=round(inst_net, 1),
            foreign_net_buy=round(foreign_net, 1),
        ))

    logger.info(f"[Preclose] {len(candidates)}종목 점수 통과 (>={min_preclose_score})")

    # ── 3단계: Trade Object 생성 ──
    try:
        from data.trade_object import TradeBuilder
        builder = TradeBuilder(equity=equity)

        for cand in candidates:
            try:
                # TradeBuilder가 기대하는 형태로 변환
                class _FakeStock:
                    def __init__(self, c):
                        self.code = c.code
                        self.name = c.name
                        self.total_score = c.preclose_score
                        self.sources = [f"preclose:{c.tv_pattern}({c.preclose_score:.0f})"]
                        self.entry = c.current_price
                        self.sl = 0
                        self.tp = 0
                        self.regime = "MOMENTUM"
                        self.sector = c.sector

                fake = _FakeStock(cand)
                to = builder.build(fake)
                if to:
                    cand.trade_object = asdict(to) if hasattr(to, '__dataclass_fields__') else None
                    cand.rr_verdict = to.rr_verdict if to else ""
            except Exception as e:
                logger.debug(f"[Preclose] TradeObject 생성 실패 {cand.code}: {e}")

    except ImportError:
        logger.warning("[Preclose] trade_object.py 미발견 — R:R 게이트 스킵")

    # ── 4단계: 정렬 ──
    # R:R verdict가 REJECT이 아닌 것 우선, 그 안에서 score 순
    def _sort_key(c):
        is_accept = 0 if c.rr_verdict in ("REJECT", "") else 1
        rr = c.trade_object.get("rr_ratio", 0) if c.trade_object else 0
        return (is_accept, rr, c.preclose_score)

    candidates.sort(key=_sort_key, reverse=True)

    final = candidates[:max_candidates]
    logger.info(f"[Preclose] 최종 {len(final)}종목 선정")

    return final


# =============================================================================
# 저장 / 로드
# =============================================================================

def save_preclose_results(candidates: list[PrecloseCandidate],
                          path: Path = None):
    """프리클로즈 결과 저장"""
    if path is None:
        path = DATA_DIR / "preclose_candidates.json"

    data = {
        "scan_time": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "total": len(candidates),
        "candidates": [asdict(c) for c in candidates],
    }

    path.write_text(
        json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    logger.info(f"[Preclose] 저장: {path} ({len(candidates)}건)")


def load_preclose_results(path: Path = None) -> list[dict]:
    """저장된 프리클로즈 결과 로드"""
    if path is None:
        path = DATA_DIR / "preclose_candidates.json"

    if not path.exists():
        return []

    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data.get("candidates", [])
    except Exception as e:
        logger.warning(f"프리클로즈 결과 로드 실패: {e}")
        return []


# =============================================================================
# 텔레그램 리포트 포맷
# =============================================================================

def format_preclose_report(candidates: list[PrecloseCandidate]) -> str:
    """프리클로즈 리포트 텔레그램 메시지"""
    if not candidates:
        return "[프리클로즈] 14:30 — 후보 없음"

    now = datetime.now()
    lines = [f"[프리클로즈] 내일 후보 — {now:%H:%M}"]
    lines.append("━━━━━━━━━━━━━━━━━━━")

    for i, c in enumerate(candidates, 1):
        # TV 패턴 한글 변환
        pattern_kr = {"QUIET_ACCUMULATION": "조용한매집",
                      "EXPLOSION": "거래대금폭발",
                      "GRADUAL_BUILDUP": "점진적증가"}.get(
            c.tv_pattern, c.tv_pattern)

        # 기본 정보
        lines.append(f"\n{i}. {c.name} ({c.code}) — PRECLOSE {c.preclose_score:.0f}점")
        lines.append(f"   촉발: {pattern_kr} (TV {c.intraday_tv_ratio:.1f}x)")

        # 수급
        supply_parts = []
        if c.inst_net_buy > 0:
            supply_parts.append(f"기관 +{c.inst_net_buy:.0f}억")
        if c.foreign_net_buy > 0:
            supply_parts.append(f"외인 +{c.foreign_net_buy:.0f}억")
        if supply_parts:
            lines.append(f"   수급: {' / '.join(supply_parts)}")

        # Trade Object
        to = c.trade_object
        if to:
            entry = to.get("entry_price", 0)
            target = to.get("target_price", 0)
            stop = to.get("stop_loss", 0)
            rr = to.get("rr_ratio", 0)
            verdict = c.rr_verdict

            reward_pct = (target / entry - 1) * 100 if entry > 0 and target > 0 else 0
            risk_pct = (1 - stop / entry) * 100 if entry > 0 and stop > 0 else 0

            verdict_icon = {"STRONG_BUY": "S", "GOOD": "G",
                            "MARGINAL_PASS": "M", "REJECT": "X"}.get(verdict, "?")
            lines.append(f"   진입: {entry:,} | "
                         f"목표 {target:,}(+{reward_pct:.1f}%) | "
                         f"손절 {stop:,}(-{risk_pct:.1f}%)")
            lines.append(f"   R:R {rr:.2f} [{verdict_icon}] | "
                         f"보유 {to.get('expected_hold_days', '?')}일")
        else:
            lines.append(f"   (Trade Object 미생성)")

    lines.append("\n━━━━━━━━━━━━━━━━━━━")
    lines.append("15:00~15:20 진입 권장")
    lines.append("프리클로즈 사이즈: 정규의 70%")

    return "\n".join(lines)


# =============================================================================
# 프리클로즈 vs 저녁 분석 교차 검증
# =============================================================================

def cross_validate_with_evening(preclose_codes: list[str]) -> list[str]:
    """프리클로즈 후보와 저녁 분석 후보 겹침 체크

    Args:
        preclose_codes: 프리클로즈 후보 종목코드 리스트

    Returns:
        겹치는 종목코드 리스트 (있으면 conviction UP 대상)
    """
    rec_path = DATA_DIR / "recommendation.json"
    if not rec_path.exists():
        return []

    try:
        data = json.loads(rec_path.read_text(encoding="utf-8"))
        evening_codes = {s.get("code", "") for s in data.get("stocks", [])}
        return [c for c in preclose_codes if c in evening_codes]
    except Exception:
        return []


# =============================================================================
# CLI
# =============================================================================

if __name__ == "__main__":
    import sys
    sys.path.insert(0, str(BASE_DIR))

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )

    test_mode = "--test" in sys.argv

    if test_mode:
        # 저장된 intraday 결과 기반 테스트 (KIS API 없이)
        print("=== 프리클로즈 테스트 모드 (저장된 데이터 기반) ===\n")
        candidates = scan_preclose(
            trader=None,
            min_preclose_score=30.0,
            max_candidates=10,
        )
    else:
        # 실제 KIS API 사용
        from bot.kis_trader import KISTrader
        trader = KISTrader()
        candidates = scan_preclose(
            trader=trader,
            min_preclose_score=50.0,
            max_candidates=5,
        )

    if candidates:
        report = format_preclose_report(candidates)
        print(report)
        save_preclose_results(candidates)
    else:
        print("[Preclose] 후보 없음")
