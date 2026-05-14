"""
투자자 수급 통합 인텔리전스 (Investor Flow Intelligence)
=========================================================
정보봇이 pykrx + KIS로 수집한 외인/기관 수급 데이터를 분석하여
종목별 가점/감점 맵과 시장 레벨 시그널을 제공합니다.

데이터 소스 (우선순위):
  1) daily_intelligence.json → investor_flow_summary  (요약, TOP 10)
  2) supply_daily/{date}_investor_flow.json           (원본, TOP 15)

수급 시그널 6종:
  시장: FOREIGN_MASS_SELL/BUY, INST_BUYING/SELLING_HEAVY
  종목: FOREIGN_MEGA_BUY/SELL

스케줄: 08:00 이후 소비 (정보봇 16:22 수집 → 08:00 요약 생성)
"""

import json
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger("BH.InvestorFlowIntel")

# ═══ 경로 상수 (OS 자동 감지) ═══
from utils.jgis_path import jgis_intel_path

INTEL_JSON = jgis_intel_path()
SUPPLY_DAILY_DIR = Path(
    "D:/Global_Stock_Overview_Scripter_정보봇/data/supply_daily"
)

# ═══ 스코어 상수 ═══
# 외인 매수/매도 가점 (억원 기준)
FOREIGN_SCORE_TIERS = [
    (1000, 15),   # ≥1,000억 → +15
    (500,  10),   # ≥500억  → +10
    (100,   5),   # ≥100억  → +5
]
# 기관 매수/매도 가점 (외인 대비 50%)
INST_SCORE_TIERS = [
    (1000, 8),    # ≥1,000억 → +8
    (500,  5),    # ≥500억  → +5
    (100,  3),    # ≥100억  → +3
]

# 시장 레벨 시그널 임계값 (억원)
FOREIGN_MASS_THRESHOLD = -3000    # 외인 대량 매도
FOREIGN_MASS_BUY_THRESHOLD = 3000  # 외인 대량 매수
INST_HEAVY_BUY = 5000
INST_HEAVY_SELL = -5000
FOREIGN_MEGA_THRESHOLD = 1000     # 종목당 1,000억

# ═══ 교차 위험 상수 ═══
# SHORT_EXTREME + FOREIGN_MEGA_SELL = CRITICAL (-20 추가)
CROSS_CRITICAL_PENALTY = -20


@dataclass
class FlowScore:
    """종목별 수급 스코어"""
    code: str
    name: str = ""
    score: float = 0.0
    tag: str = ""
    reasons: List[str] = field(default_factory=list)
    foreign_amt: float = 0.0    # 억원
    inst_amt: float = 0.0       # 억원


@dataclass
class MarketFlow:
    """시장 레벨 수급 정보"""
    foreign_total: float = 0.0      # 억원
    institution_total: float = 0.0
    individual_total: float = 0.0
    kospi_foreign: float = 0.0
    kosdaq_foreign: float = 0.0
    signals: List[dict] = field(default_factory=list)
    source: str = ""
    date: str = ""
    mode: str = "NEUTRAL"  # AGGRESSIVE / NEUTRAL / DEFENSIVE


# ═══ 캐시 (5분 TTL) ═══
_flow_cache: Dict[str, FlowScore] = {}
_market_cache: Optional[MarketFlow] = None
_cache_ts: float = 0.0
_CACHE_TTL = 300


def _is_fresh_date(date_str: str) -> bool:
    """오늘 또는 어제 데이터만 유효."""
    try:
        clean = date_str.replace("-", "")
        if len(clean) >= 8:
            data_date = datetime.strptime(clean[:8], "%Y%m%d").date()
        else:
            return False
        today = datetime.now().date()
        yesterday = today - timedelta(days=1)
        return data_date in (today, yesterday)
    except Exception:
        return False


def _calc_score(amt_억: float, tiers: list, is_sell: bool = False) -> float:
    """금액에 따른 스코어 계산."""
    abs_amt = abs(amt_억)
    for threshold, score in tiers:
        if abs_amt >= threshold:
            return -score if is_sell else score
    return 0.0


# ═══════════════════════════════════════════════════
#  데이터 로드: daily_intelligence → investor_flow_summary
# ═══════════════════════════════════════════════════

def _load_from_intel() -> Tuple[Optional[dict], Optional[dict]]:
    """daily_intelligence.json에서 investor_flow_summary 로드.
    Returns: (summary_dict, raw_intel) or (None, None)
    """
    if not INTEL_JSON.exists():
        return None, None
    try:
        raw = json.loads(INTEL_JSON.read_text(encoding="utf-8"))
    except Exception as e:
        logger.warning(f"[수급인텔] daily_intelligence 로드 실패: {e}")
        return None, None

    if not _is_fresh_date(raw.get("date", "")):
        return None, None

    summary = raw.get("investor_flow_summary")
    if summary and summary.get("market_total"):
        return summary, raw
    return None, raw


# ═══════════════════════════════════════════════════
#  데이터 로드: raw investor_flow.json (폴백)
# ═══════════════════════════════════════════════════

def _load_from_raw_flow() -> Optional[dict]:
    """supply_daily/{date}_investor_flow.json 로드 (최근 2일 검색)."""
    today = datetime.now()
    for delta in range(3):
        dt = today - timedelta(days=delta)
        date_str = dt.strftime("%Y-%m-%d")
        path = SUPPLY_DAILY_DIR / f"{date_str}_investor_flow.json"
        if not path.exists():
            continue
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
            # source=pykrx인 경우만 TOP 데이터 유효
            if raw.get("foreign_top_buy"):
                logger.info(f"[수급인텔] raw flow 로드: {path.name} (source={raw.get('source')})")
                return raw
        except Exception:
            continue
    return None


# ═══════════════════════════════════════════════════
#  종목별 스코어 맵 빌드
# ═══════════════════════════════════════════════════

def _build_score_map(flow_data: dict) -> Dict[str, FlowScore]:
    """외인/기관 TOP 매수/매도 → 종목별 스코어 맵 생성."""
    result: Dict[str, FlowScore] = {}

    # 외인 매수 TOP
    for item in flow_data.get("foreign_top_buy", [])[:10]:
        code = item.get("ticker", "")
        if not code or len(code) != 6:
            continue
        amt_억 = item.get("net_amt", 0) / 1_0000_0000
        sc = _calc_score(amt_억, FOREIGN_SCORE_TIERS, is_sell=False)
        if sc == 0:
            continue
        if code not in result:
            result[code] = FlowScore(code=code, name=item.get("name", ""))
        result[code].score += sc
        result[code].foreign_amt = amt_억
        result[code].reasons.append(f"외인매수+{amt_억:,.0f}억")

    # 외인 매도 TOP
    for item in flow_data.get("foreign_top_sell", [])[:10]:
        code = item.get("ticker", "")
        if not code or len(code) != 6:
            continue
        amt_억 = item.get("net_amt", 0) / 1_0000_0000
        sc = _calc_score(amt_억, FOREIGN_SCORE_TIERS, is_sell=True)
        if sc == 0:
            continue
        if code not in result:
            result[code] = FlowScore(code=code, name=item.get("name", ""))
        result[code].score += sc
        result[code].foreign_amt = amt_억
        result[code].reasons.append(f"외인매도{amt_억:,.0f}억")

    # 기관 매수 TOP
    for item in flow_data.get("institution_top_buy", [])[:10]:
        code = item.get("ticker", "")
        if not code or len(code) != 6:
            continue
        amt_억 = item.get("net_amt", 0) / 1_0000_0000
        sc = _calc_score(amt_억, INST_SCORE_TIERS, is_sell=False)
        if sc == 0:
            continue
        if code not in result:
            result[code] = FlowScore(code=code, name=item.get("name", ""))
        result[code].score += sc
        result[code].inst_amt = amt_억
        result[code].reasons.append(f"기관매수+{amt_억:,.0f}억")

    # 기관 매도 TOP
    for item in flow_data.get("institution_top_sell", [])[:10]:
        code = item.get("ticker", "")
        if not code or len(code) != 6:
            continue
        amt_억 = item.get("net_amt", 0) / 1_0000_0000
        sc = _calc_score(amt_억, INST_SCORE_TIERS, is_sell=True)
        if sc == 0:
            continue
        if code not in result:
            result[code] = FlowScore(code=code, name=item.get("name", ""))
        result[code].score += sc
        result[code].inst_amt = amt_억
        result[code].reasons.append(f"기관매도{amt_억:,.0f}억")

    # 태그 설정
    for fs in result.values():
        if fs.score > 0:
            fs.tag = "FLOW_BUY"
        elif fs.score < 0:
            fs.tag = "FLOW_SELL"
        else:
            fs.tag = "FLOW_CROSS"  # 외인매수+기관매도 등 상쇄

    return result


def _build_market_flow(flow_data: dict, signals: list = None) -> MarketFlow:
    """시장 레벨 수급 정보 빌드."""
    mf = MarketFlow()
    mf.source = flow_data.get("source", "unknown")
    mf.date = str(flow_data.get("date", ""))

    # market_total (daily_intelligence 형식)
    mt = flow_data.get("market_total", {})
    if mt:
        mf.foreign_total = mt.get("foreign", 0)
        mf.institution_total = mt.get("institution", 0)
        mf.individual_total = mt.get("individual", 0)
    else:
        # raw investor_flow.json 형식 (이미 억원)
        mf.foreign_total = flow_data.get("foreign", 0)
        mf.institution_total = flow_data.get("institution", 0)
        mf.individual_total = flow_data.get("individual", 0)

    # KOSPI/KOSDAQ 분리
    kospi = flow_data.get("kospi", {})
    kosdaq = flow_data.get("kosdaq", {})
    mf.kospi_foreign = kospi.get("foreign", 0)
    mf.kosdaq_foreign = kosdaq.get("foreign", 0)

    # 시그널 (daily_intelligence 형식이면 직접 사용)
    if signals:
        mf.signals = signals
    else:
        mf.signals = flow_data.get("signals", [])

    # 모드 결정
    if mf.foreign_total <= FOREIGN_MASS_THRESHOLD:
        mf.mode = "DEFENSIVE"
    elif mf.foreign_total >= FOREIGN_MASS_BUY_THRESHOLD:
        mf.mode = "AGGRESSIVE"
    else:
        mf.mode = "NEUTRAL"

    return mf


# ═══════════════════════════════════════════════════
#  공매도 8시그널 교차 분석
# ═══════════════════════════════════════════════════

def _cross_with_short_signals(
    score_map: Dict[str, FlowScore],
    intel_raw: Optional[dict],
) -> None:
    """공매도 위험 종목 + 외인 대량 매도 = CRITICAL 추가 감점."""
    if not intel_raw:
        return

    short_summary = intel_raw.get("short_selling_summary", {})
    short_signals = short_summary.get("signals", [])

    # SHORT_EXTREME / SHORT_CREDIT_DIVERGE 종목 추출
    danger_tickers = set()
    for sig in short_signals:
        if sig.get("type") in ("SHORT_EXTREME", "SHORT_CREDIT_DIVERGE"):
            tk = sig.get("ticker", "")
            if tk:
                danger_tickers.add(tk)

    # 교차: 공매도 위험 + 외인 대량 매도 → CRITICAL
    for code in danger_tickers:
        if code in score_map and score_map[code].score < 0:
            score_map[code].score += CROSS_CRITICAL_PENALTY
            score_map[code].tag = "FLOW_CRITICAL"
            score_map[code].reasons.append("공매도위험+외인매도=CRITICAL")
            logger.info(
                f"[수급인텔] CRITICAL: {score_map[code].name}({code}) "
                f"공매도+외인매도 교차 → {CROSS_CRITICAL_PENALTY:+d}"
            )


# ═══════════════════════════════════════════════════
#  메인 로드 함수
# ═══════════════════════════════════════════════════

def load_investor_flow_intel() -> Tuple[Dict[str, FlowScore], MarketFlow]:
    """투자자 수급 인텔리전스 통합 로드.

    Returns:
        (score_map, market_flow)
        score_map: {stock_code: FlowScore}
        market_flow: MarketFlow (시장 레벨)
    """
    global _flow_cache, _market_cache, _cache_ts

    if _flow_cache and _market_cache and (time.time() - _cache_ts) < _CACHE_TTL:
        return _flow_cache, _market_cache

    score_map: Dict[str, FlowScore] = {}
    market = MarketFlow()

    # 소스 1: daily_intelligence.json → investor_flow_summary
    intel_summary, intel_raw = _load_from_intel()

    if intel_summary:
        score_map = _build_score_map(intel_summary)
        market = _build_market_flow(
            intel_summary, intel_summary.get("signals", [])
        )
        logger.info(
            f"[수급인텔] daily_intelligence 로드: "
            f"{len(score_map)}종목 (source={market.source})"
        )
    else:
        # 소스 2: raw investor_flow.json (폴백)
        raw_flow = _load_from_raw_flow()
        if raw_flow:
            score_map = _build_score_map(raw_flow)
            market = _build_market_flow(raw_flow)
            logger.info(
                f"[수급인텔] raw flow 폴백: "
                f"{len(score_map)}종목 (source={market.source})"
            )
            # raw에서 로드한 경우 intel_raw도 시도
            if not intel_raw:
                _, intel_raw = _load_from_intel()

    # 공매도 교차 분석
    if score_map:
        _cross_with_short_signals(score_map, intel_raw)

    _flow_cache = score_map
    _market_cache = market
    _cache_ts = time.time()

    return score_map, market


# ═══════════════════════════════════════════════════
#  Public API (recommendation / telegram 연동)
# ═══════════════════════════════════════════════════

def get_investor_flow_score(code: str) -> Tuple[float, str]:
    """morning_recommendation 연동 — 종목별 수급 스코어 + 사유."""
    score_map, _ = load_investor_flow_intel()
    if code in score_map:
        fs = score_map[code]
        detail = " | ".join(fs.reasons[:3])
        return fs.score, detail
    return 0.0, ""


def get_market_flow_mode() -> Tuple[str, float]:
    """시장 수급 모드 — AGGRESSIVE / NEUTRAL / DEFENSIVE.
    Returns: (mode, foreign_total_억)
    """
    _, market = load_investor_flow_intel()
    return market.mode, market.foreign_total


def format_investor_flow_alert() -> str:
    """08:25 텔레그램 수급 시그널 알림 포맷."""
    score_map, market = load_investor_flow_intel()

    if not score_map and market.foreign_total == 0:
        return ""

    now = datetime.now()
    today_str = now.strftime("%m/%d(%a)")
    time_str = now.strftime("%H:%M")

    lines = []
    lines.append("---")
    lines.append("  투자자 수급 인텔리전스")
    lines.append(f"  {today_str} {time_str}")
    lines.append("---")

    # 시장 전체 수급
    lines.append("")
    lines.append(f"[시장 수급] {market.mode}")
    lines.append(
        f"  외인: {market.foreign_total:+,.0f}억 | "
        f"기관: {market.institution_total:+,.0f}억 | "
        f"개인: {market.individual_total:+,.0f}억"
    )
    if market.kospi_foreign or market.kosdaq_foreign:
        lines.append(
            f"  KOSPI 외인: {market.kospi_foreign:+,.0f}억 | "
            f"KOSDAQ 외인: {market.kosdaq_foreign:+,.0f}억"
        )

    # 시그널
    if market.signals:
        lines.append("")
        lines.append("[수급 시그널]")
        for sig in market.signals[:5]:
            lines.append(f"  {sig.get('type', '')}: {sig.get('msg', '')}")

    # 외인 매수 TOP
    buy_stocks = sorted(
        [fs for fs in score_map.values() if fs.score > 0],
        key=lambda x: x.score, reverse=True,
    )
    if buy_stocks:
        lines.append("")
        lines.append("[외인/기관 순매수 TOP]")
        for i, fs in enumerate(buy_stocks[:7], 1):
            amt_parts = []
            if fs.foreign_amt:
                amt_parts.append(f"외인{fs.foreign_amt:+,.0f}억")
            if fs.inst_amt:
                amt_parts.append(f"기관{fs.inst_amt:+,.0f}억")
            amt_str = " ".join(amt_parts)
            lines.append(
                f"  {i}. {fs.name or fs.code}({fs.code}) "
                f"+{fs.score:.0f}점 [{amt_str}]"
            )

    # 외인 매도 TOP
    sell_stocks = sorted(
        [fs for fs in score_map.values() if fs.score < 0],
        key=lambda x: x.score,
    )
    if sell_stocks:
        lines.append("")
        lines.append("[외인/기관 순매도 TOP]")
        for i, fs in enumerate(sell_stocks[:7], 1):
            amt_parts = []
            if fs.foreign_amt:
                amt_parts.append(f"외인{fs.foreign_amt:+,.0f}억")
            if fs.inst_amt:
                amt_parts.append(f"기관{fs.inst_amt:+,.0f}억")
            amt_str = " ".join(amt_parts)
            lines.append(
                f"  {i}. {fs.name or fs.code}({fs.code}) "
                f"{fs.score:.0f}점 [{amt_str}]"
            )

    # 교차 위험
    critical = [fs for fs in score_map.values() if fs.tag == "FLOW_CRITICAL"]
    if critical:
        lines.append("")
        lines.append("[!!! CRITICAL — 공매도+외인매도 교차]")
        for fs in critical:
            lines.append(f"  {fs.name}({fs.code}) {fs.score:.0f}점")

    lines.append("")
    lines.append(
        f"총 {len(score_map)}종목 "
        f"(매수:{len(buy_stocks)} 매도:{len(sell_stocks)} "
        f"CRITICAL:{len(critical)})"
    )
    lines.append(f"source: {market.source or 'N/A'}")

    return "\n".join(lines)


# ═══════════════════════════════════════════════════
#  CLI 테스트
# ═══════════════════════════════════════════════════

if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    sys.stdout.reconfigure(encoding="utf-8")

    score_map, market = load_investor_flow_intel()
    print(f"\n시장 모드: {market.mode}")
    print(f"외인: {market.foreign_total:+,.0f}억")
    print(f"종목 스코어: {len(score_map)}건\n")

    alert = format_investor_flow_alert()
    if alert:
        print(alert)
    else:
        print("(수급 데이터 없음)")
