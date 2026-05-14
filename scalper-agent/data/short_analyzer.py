"""
공매도 3종 통합 8시그널 분석 + 스코어링
=======================================
SHORT-01: 정보봇 데이터 소스 연동 (daily_intelligence.json + supply_tracker CSV)
SHORT-02: 8시그널 기반 스코어링 (-8 ~ +8)
SHORT-03: Market Journal 연동
SHORT-04: 매매 위험 필터 (Hard Filter)

데이터 소스 (정보봇 제공):
  1) D:/shared-bot-data/jgis_to_quant/daily_intelligence.json
     → short_selling_summary.data (TOP 30) + signals (8시그널)
  2) D:/Global_Stock_Overview_Scripter_정보봇/data/supply_tracker/{code}.csv
     → 2,536종목 일별 24컬럼 (신용잔고, 대차거래 포함)

8시그널:
  SHORT_CREDIT_DIVERGE  공매도≥10% & 신용≥3%     → -8 (최고 위험)
  SHORT_EXTREME         공매도 거래비중 ≥ 20%     → -6 (대량 공매도)
  SHORT_COVER_RALLY     대차상환 > 신규×3          → +8 (매수 기회)
  LOAN_SURGE            대차잔고 전일대비 +20%     → -4 (기관 숏 확대)
  CREDIT_OVERHEAT       신용잔고율 ≥ 5%            → -5 (반대매매 리스크)
  SHORT_SURGE           공매도 비중 전일대비 +5%p  → -3 (신규 유입)
  LOAN_MOMENTUM         대차잔고 5일 ±15%+         → ±2 (추세 변화)
  SHORT_BALANCE_HIGH    공매도 잔고비율 ≥ 3%       → -1 (숏커버 잠재력)
"""

import csv
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

STORE_DIR = Path(__file__).resolve().parent.parent / "data_store"
SHORT_DIR = STORE_DIR / "short"

# 정보봇 데이터 경로 (OS 자동 감지)
from utils.jgis_path import jgis_intel_path

INTEL_JSON = jgis_intel_path()
SUPPLY_TRACKER_DIR = Path(
    "D:/Global_Stock_Overview_Scripter_정보봇/data/supply_tracker"
)

# 8시그널별 스코어 가중치
SIGNAL_SCORES: Dict[str, float] = {
    "SHORT_CREDIT_DIVERGE": -8.0,
    "SHORT_EXTREME": -6.0,
    "SHORT_COVER_RALLY": +8.0,
    "LOAN_SURGE": -4.0,
    "CREDIT_OVERHEAT": -5.0,
    "SHORT_SURGE": -3.0,
    "LOAN_MOMENTUM": -2.0,     # 방향에 따라 부호 전환
    "SHORT_BALANCE_HIGH": -1.0,
}

# Hard Filter 대상 시그널 (추천 제외)
DANGER_SIGNALS = {"SHORT_CREDIT_DIVERGE"}


@dataclass
class ShortAnalysis:
    """종목별 공매도 분석 결과"""
    code: str
    name: str = ""
    short_ratio: float = 0.0         # 공매도 거래비중 %
    short_level: str = "UNKNOWN"      # HIGH(5%+) / MODERATE(2~5%) / LOW(<2%)
    short_trend: str = "NEUTRAL"      # COVERING / BUILDING / NEUTRAL
    trend_days: int = 0               # 추세 지속일
    ratio_change_5d: float = 0.0      # 5일 잔고율 변화 (pp)
    signal: str = "NEUTRAL"           # BULLISH(숏커버) / BEARISH(숏빌딩) / NEUTRAL
    score_adjustment: float = 0.0     # 스코어링 반영 점수
    detail: str = ""                  # 요약 텍스트
    # 신규 필드 (3종 통합)
    credit_balance_rate: float = 0.0  # 신용잔고율 %
    loan_balance_qty: int = 0         # 대차잔고 수량
    cover_ratio: float = 0.0          # 상환/신규 비율
    loan_momentum_5d: float = 0.0     # 5일 대차잔고 변화율 %
    signals_fired: list = None        # 발동된 시그널 타입 리스트

    def __post_init__(self):
        if self.signals_fired is None:
            self.signals_fired = []


# ═══════════════════════════════════════════════════
#  데이터 로더 (캐시)
# ═══════════════════════════════════════════════════

_intel_cache: Dict = {}
_intel_cache_ts: float = 0.0


def _load_intel_summary() -> Dict:
    """daily_intelligence.json에서 short_selling_summary 로드 (5분 캐시)."""
    global _intel_cache, _intel_cache_ts
    import time
    now = time.time()
    if _intel_cache and (now - _intel_cache_ts) < 300:
        return _intel_cache

    if not INTEL_JSON.exists():
        return {}
    try:
        raw = json.loads(INTEL_JSON.read_text(encoding="utf-8"))
        ss = raw.get("short_selling_summary", {})
        if not ss or not ss.get("data"):
            return {}
        # 날짜 검증: 오늘 또는 어제 데이터만 사용
        intel_date = raw.get("date", "")
        today = datetime.now().strftime("%Y-%m-%d")
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        if intel_date not in (today, yesterday):
            logger.debug(f"[SHORT] intel 날짜 만료: {intel_date}")
            return {}
        _intel_cache = ss
        _intel_cache_ts = now
        return ss
    except Exception as e:
        logger.warning(f"[SHORT] daily_intelligence 로드 실패: {e}")
        return {}


def _load_supply_csv(code: str, days: int = 10) -> List[Dict]:
    """정보봇 supply_tracker CSV에서 최근 N일 로드."""
    path = SUPPLY_TRACKER_DIR / f"{code}.csv"
    if not path.exists():
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            rows = list(csv.DictReader(f))
        return rows[-days:] if len(rows) >= days else rows
    except Exception:
        return []


def _safe_float(val, default=0.0) -> float:
    """CSV 값 → float 안전 변환 (빈칸/None 대응)."""
    try:
        return float(val) if val else default
    except (ValueError, TypeError):
        return default


def _safe_int(val, default=0) -> int:
    try:
        return int(float(val)) if val else default
    except (ValueError, TypeError):
        return default


# ═══════════════════════════════════════════════════
#  SHORT-01: 8시그널 감지
# ═══════════════════════════════════════════════════

def _detect_signals_from_intel(code: str, ss: Dict) -> Tuple[List[str], Dict]:
    """daily_intelligence.json의 pre-computed 시그널에서 해당 종목 추출.

    Returns:
        (signal_types: list, stock_data: dict)
    """
    signals_for_code = []
    for sig in ss.get("signals", []):
        if sig.get("ticker") == code:
            signals_for_code.append(sig["type"])

    stock_data = ss.get("data", {}).get(code, {})
    return signals_for_code, stock_data


def _detect_signals_from_csv(code: str) -> Tuple[List[str], Dict]:
    """supply_tracker CSV에서 직접 8시그널 계산.

    daily_intelligence에 없는 종목용 (2,536종목 커버리지).
    """
    rows = _load_supply_csv(code, 10)
    if not rows:
        return [], {}

    latest = rows[-1]
    prev = rows[-2] if len(rows) >= 2 else {}

    short_qty = _safe_float(latest.get("short_selling_qty"))
    credit_rate = _safe_float(latest.get("credit_balance_rate"))
    loan_bal = _safe_int(latest.get("loan_balance_qty"))
    loan_new = _safe_int(latest.get("loan_new_qty"))
    loan_repay = _safe_int(latest.get("loan_repay_qty"))
    loan_rate = _safe_float(latest.get("loan_balance_rate"))

    # 전일 데이터
    prev_short_qty = _safe_float(prev.get("short_selling_qty")) if prev else 0
    prev_loan_bal = _safe_int(prev.get("loan_balance_qty")) if prev else 0

    # 공매도 비중 추정 (short_overheat 필드 사용)
    short_overheat = _safe_float(latest.get("short_overheat"))
    # short_overheat이 0이면 short_qty 기반 대략 추정 불가 → 0 사용
    short_ratio = short_overheat  # 과열 지표를 비중 프록시로 사용

    # 전일대비 공매도 비중 변화
    prev_overheat = _safe_float(prev.get("short_overheat")) if prev else 0
    short_ratio_delta = short_ratio - prev_overheat

    # 대차잔고 변화율
    loan_change_pct = 0.0
    if prev_loan_bal > 0:
        loan_change_pct = (loan_bal - prev_loan_bal) / prev_loan_bal * 100

    # 5일 대차잔고 모멘텀
    loan_momentum_5d = 0.0
    if len(rows) >= 6:
        loan_5d_ago = _safe_int(rows[-6].get("loan_balance_qty"))
        if loan_5d_ago > 0:
            loan_momentum_5d = (loan_bal - loan_5d_ago) / loan_5d_ago * 100

    # 상환/신규 비율
    cover_ratio = loan_repay / max(loan_new, 1) if loan_new > 0 else 0.0

    # 8시그널 판정
    signals = []

    # 1. SHORT_EXTREME: 공매도 거래비중 ≥ 20%
    if short_ratio >= 20:
        signals.append("SHORT_EXTREME")

    # 2. SHORT_SURGE: 전일대비 +5%p
    if short_ratio_delta >= 5:
        signals.append("SHORT_SURGE")

    # 3. SHORT_BALANCE_HIGH: 잔고비율 ≥ 3%
    if loan_rate >= 3:
        signals.append("SHORT_BALANCE_HIGH")

    # 4. CREDIT_OVERHEAT: 신용잔고율 ≥ 5%
    if credit_rate >= 5:
        signals.append("CREDIT_OVERHEAT")

    # 5. LOAN_SURGE: 대차잔고 전일대비 +20%
    if loan_change_pct >= 20:
        signals.append("LOAN_SURGE")

    # 6. SHORT_COVER_RALLY: 대차상환 > 신규×3
    if loan_new > 0 and loan_repay > loan_new * 3:
        signals.append("SHORT_COVER_RALLY")

    # 7. LOAN_MOMENTUM: 5일 변화 ±15%+
    if abs(loan_momentum_5d) >= 15:
        signals.append("LOAN_MOMENTUM")

    # 8. SHORT_CREDIT_DIVERGE: 공매도 ≥10% & 신용 ≥3%
    if short_ratio >= 10 and credit_rate >= 3:
        signals.append("SHORT_CREDIT_DIVERGE")

    stock_data = {
        "ticker": code,
        "short_ratio": round(short_ratio, 2),
        "credit_balance_rate": round(credit_rate, 2),
        "loan_balance_qty": loan_bal,
        "loan_new_qty": loan_new,
        "loan_repay_qty": loan_repay,
        "cover_ratio": round(cover_ratio, 2),
        "loan_momentum_5d": round(loan_momentum_5d, 1),
        "short_ratio_delta": round(short_ratio_delta, 2),
        "loan_change_pct": round(loan_change_pct, 1),
    }
    return signals, stock_data


# ═══════════════════════════════════════════════════
#  SHORT-01: 통합 분석
# ═══════════════════════════════════════════════════

def analyze_short_interest(code: str, name: str = "") -> ShortAnalysis:
    """공매도 3종 통합 분석 — 8시그널 기반 스코어링.

    데이터 소스 우선순위:
      1) daily_intelligence.json (TOP 30 + pre-computed signals)
      2) supply_tracker CSV (2,536종목, 직접 시그널 계산)

    Returns:
        ShortAnalysis with score_adjustment (-8 ~ +8)
    """
    result = ShortAnalysis(code=code, name=name)

    # 1) daily_intelligence에서 먼저 시도
    ss = _load_intel_summary()
    signals, stock_data = [], {}

    if ss and code in ss.get("data", {}):
        signals, stock_data = _detect_signals_from_intel(code, ss)
    else:
        # 2) CSV에서 직접 계산
        signals, stock_data = _detect_signals_from_csv(code)

    if not stock_data:
        result.detail = "데이터없음"
        return result

    # 기본 필드 채우기
    result.short_ratio = stock_data.get("short_ratio", 0)
    result.credit_balance_rate = stock_data.get("credit_balance_rate", 0)
    result.loan_balance_qty = stock_data.get("loan_balance_qty", 0)
    result.cover_ratio = stock_data.get("cover_ratio", 0)
    result.loan_momentum_5d = stock_data.get("loan_momentum_5d", 0)
    result.signals_fired = signals

    # 잔고 레벨
    cr = result.credit_balance_rate
    sr = result.short_ratio
    if cr >= 5 or sr >= 20:
        result.short_level = "HIGH"
    elif cr >= 2 or sr >= 5:
        result.short_level = "MODERATE"
    else:
        result.short_level = "LOW"

    # 추세 판단 (cover_ratio / loan_momentum 기반)
    if result.cover_ratio >= 3:
        result.short_trend = "COVERING"
    elif result.loan_momentum_5d >= 15:
        result.short_trend = "BUILDING"
    elif result.loan_momentum_5d <= -15:
        result.short_trend = "COVERING"
    else:
        result.short_trend = "NEUTRAL"

    # 8시그널 기반 스코어 계산
    score = 0.0
    detail_parts = []

    for sig_type in signals:
        base_score = SIGNAL_SCORES.get(sig_type, 0)
        # LOAN_MOMENTUM: 감소 추세면 부호 반전 (긍정)
        if sig_type == "LOAN_MOMENTUM" and result.loan_momentum_5d < 0:
            base_score = abs(base_score)  # +2 (숏커버 방향)
        score += base_score

    # 시그널별 상세
    if "SHORT_CREDIT_DIVERGE" in signals:
        detail_parts.append(
            f"공매도{sr:.0f}%+신용{cr:.1f}% 동시과열"
        )
        result.signal = "BEARISH"
    elif "SHORT_COVER_RALLY" in signals:
        detail_parts.append(
            f"숏커버 상환/신규 {result.cover_ratio:.1f}x"
        )
        result.signal = "BULLISH"
    elif "SHORT_EXTREME" in signals:
        detail_parts.append(f"공매도 {sr:.0f}% 극단")
        result.signal = "BEARISH"
    elif "CREDIT_OVERHEAT" in signals:
        detail_parts.append(f"신용잔고율 {cr:.1f}% 과열")
        result.signal = "BEARISH"
    elif "LOAN_SURGE" in signals:
        detail_parts.append(
            f"대차잔고 급증 +{stock_data.get('loan_change_pct', 0):.0f}%"
        )
        result.signal = "BEARISH"
    elif signals:
        detail_parts.append("+".join(signals))
        result.signal = "BEARISH" if score < 0 else (
            "BULLISH" if score > 0 else "NEUTRAL"
        )
    else:
        # 시그널 없음 — 기본 데이터 기반 판단
        if cr >= 3:
            detail_parts.append(f"신용{cr:.1f}% 주의")
            score -= 1.0
        elif result.cover_ratio >= 2:
            detail_parts.append(f"숏커버 {result.cover_ratio:.1f}x")
            score += 2.0
            result.signal = "BULLISH"

    if not detail_parts:
        detail_parts.append("공매도 안정")

    result.score_adjustment = round(max(-8.0, min(8.0, score)), 1)
    result.detail = " | ".join(detail_parts)
    return result


# ═══════════════════════════════════════════════════
#  SHORT-02: 스코어링 연동용 조회 함수
# ═══════════════════════════════════════════════════

def get_short_score(code: str, name: str = "") -> Tuple[float, str]:
    """morning_recommendation에서 호출 — score + 사유.

    정보봇 3종 데이터 기반 8시그널 스코어링.
    Returns:
        (score: -8.0 ~ +8.0, detail: str)
    """
    r = analyze_short_interest(code, name)
    return r.score_adjustment, r.detail


def is_short_building_danger(code: str) -> bool:
    """공매도+신용 동시과열 → 추천 제외용 Hard Filter.

    True 조건:
      - SHORT_CREDIT_DIVERGE 발동 (공매도≥10% & 신용≥3%)
      - CREDIT_OVERHEAT (신용잔고율 ≥ 7%, 극도 과열)
    """
    r = analyze_short_interest(code)
    if "SHORT_CREDIT_DIVERGE" in r.signals_fired:
        return True
    if "CREDIT_OVERHEAT" in r.signals_fired and r.credit_balance_rate >= 7:
        return True
    return False


# ═══════════════════════════════════════════════════
#  SHORT-03: Market Journal 연동
# ═══════════════════════════════════════════════════

def get_short_journal_data(universe_codes: List[str] = None) -> Dict:
    """Market Journal용 공매도 3종 섹션 데이터.

    daily_intelligence signals를 우선 사용, 없으면 CSV 스캔.
    """
    ss = _load_intel_summary()

    # intel signals가 있으면 그것을 사용
    if ss and ss.get("signals"):
        return _journal_from_intel(ss)

    # 없으면 CSV 기반 스캔
    return _journal_from_csv(universe_codes)


def _journal_from_intel(ss: Dict) -> Dict:
    """daily_intelligence signals 기반 저널 데이터."""
    signals = ss.get("signals", [])
    data = ss.get("data", {})

    danger_list = []   # 위험 (SHORT_CREDIT_DIVERGE, CREDIT_OVERHEAT, SHORT_EXTREME)
    cover_list = []    # 숏커버 기회 (SHORT_COVER_RALLY)
    watch_list = []    # 모니터링 (나머지)

    danger_types = {"SHORT_CREDIT_DIVERGE", "SHORT_EXTREME", "CREDIT_OVERHEAT"}
    cover_types = {"SHORT_COVER_RALLY"}

    seen_codes = set()
    for sig in signals:
        code = sig.get("ticker", "")
        sig_type = sig.get("type", "")
        if code in seen_codes:
            continue
        seen_codes.add(code)

        sd = data.get(code, {})
        entry = {
            "code": code,
            "name": sig.get("name", sd.get("name", code)),
            "type": sig_type,
            "msg": sig.get("msg", ""),
            "short_ratio": sd.get("short_ratio", 0),
            "credit_rate": sd.get("credit_balance_rate", 0),
            "cover_ratio": sd.get("cover_ratio", 0),
        }

        if sig_type in danger_types:
            danger_list.append(entry)
        elif sig_type in cover_types:
            cover_list.append(entry)
        else:
            watch_list.append(entry)

    return {
        "danger": danger_list[:10],
        "cover": cover_list[:5],
        "watch": watch_list[:10],
        "signal_count": len(signals),
        "stock_count": len(data),
        # 하위호환
        "building_top5": [
            {"code": d["code"], "name": d["name"],
             "ratio": d["short_ratio"], "trend_days": 0,
             "score": SIGNAL_SCORES.get(d["type"], -5)}
            for d in danger_list[:5]
        ],
        "covering_top5": [
            {"code": c["code"], "name": c["name"],
             "ratio": c["cover_ratio"], "trend_days": 0,
             "score": 8.0}
            for c in cover_list[:5]
        ],
        "high_short_count": len(danger_list),
    }


def _journal_from_csv(universe_codes: Optional[List[str]] = None) -> Dict:
    """CSV 스캔 기반 저널 데이터 (폴백)."""
    if universe_codes is None:
        if not SUPPLY_TRACKER_DIR.exists():
            return {"danger": [], "cover": [], "watch": [],
                    "building_top5": [], "covering_top5": [],
                    "high_short_count": 0, "signal_count": 0, "stock_count": 0}
        codes = []
        for p in SUPPLY_TRACKER_DIR.glob("*.csv"):
            code = p.stem
            if len(code) == 6 and code.isdigit():
                codes.append(code)
        universe_codes = codes[:500]

    danger_list = []
    cover_list = []
    high_count = 0

    for code in universe_codes:
        r = analyze_short_interest(code)
        if r.short_level == "HIGH":
            high_count += 1
        if r.score_adjustment <= -5:
            danger_list.append({
                "code": code, "name": r.name,
                "ratio": r.short_ratio, "trend_days": r.trend_days,
                "score": r.score_adjustment,
            })
        elif r.score_adjustment >= 5:
            cover_list.append({
                "code": code, "name": r.name,
                "ratio": r.short_ratio, "trend_days": r.trend_days,
                "score": r.score_adjustment,
            })

    danger_list.sort(key=lambda x: x["score"])
    cover_list.sort(key=lambda x: -x["score"])

    return {
        "danger": [], "cover": [], "watch": [],
        "building_top5": danger_list[:5],
        "covering_top5": cover_list[:5],
        "high_short_count": high_count,
        "signal_count": 0, "stock_count": len(universe_codes),
    }


# ═══════════════════════════════════════════════════
#  SHORT-04: 텔레그램 포맷
# ═══════════════════════════════════════════════════

def format_journal_section(data: Dict) -> str:
    """Market Journal 텔레그램 포맷 — 3종 통합 버전."""
    lines = []

    # 위험 시그널
    danger = data.get("danger", [])
    cover = data.get("cover", [])

    if danger or cover:
        lines.append("--- 공매도 3종 시그널 ---")

    if danger:
        lines.append("  [위험] 매수 주의:")
        for d in danger[:5]:
            sig_label = {
                "SHORT_CREDIT_DIVERGE": "공매도+신용 동시과열",
                "SHORT_EXTREME": "공매도 극단",
                "CREDIT_OVERHEAT": "신용과열",
            }.get(d.get("type", ""), d.get("type", ""))
            lines.append(f"    {d['name']}({d['code']}) {sig_label}")
            if d.get("msg"):
                lines.append(f"      {d['msg']}")

    if cover:
        lines.append("  [기회] 숏커버 반등:")
        for c in cover[:5]:
            cr = c.get("cover_ratio", 0)
            lines.append(
                f"    {c['name']}({c['code']}) "
                f"상환/신규 {cr:.1f}x"
            )

    # 하위호환 (기존 building/covering)
    if not danger and not cover:
        building = data.get("building_top5", [])
        covering = data.get("covering_top5", [])
        if covering:
            lines.append("  숏커버링:")
            for s in covering:
                lines.append(
                    f"    {s['name']} 잔고{s['ratio']:.2f}%"
                )
        if building:
            lines.append("  숏빌딩 (주의):")
            for s in building:
                lines.append(
                    f"    {s['name']} 잔고{s['ratio']:.2f}%"
                )

    if data.get("signal_count", 0) > 0:
        lines.append(
            f"  시그널 {data['signal_count']}건 "
            f"({data.get('stock_count', 0)}종목 분석)"
        )

    if not lines:
        return ""
    return "\n".join(lines)


def format_short_alert(data: Dict) -> str:
    """텔레그램 독립 알림용 포맷 — 8시그널 요약.

    Returns:
        텔레그램 전송용 텍스트 (시그널 없으면 빈 문자열)
    """
    danger = data.get("danger", [])
    cover = data.get("cover", [])
    if not danger and not cover:
        return ""

    now = datetime.now()
    today_str = now.strftime("%m/%d(%a)")
    time_str = now.strftime("%H:%M")

    lines = []
    lines.append("---")
    lines.append(f"  공매도 3종 8시그널 알림")
    lines.append(f"  {today_str} {time_str}")
    lines.append("---")

    if danger:
        lines.append("")
        lines.append("[위험] 매수 회피 권고")
        for i, d in enumerate(danger[:7], 1):
            sig_emoji = {
                "SHORT_CREDIT_DIVERGE": "[!!!]",
                "SHORT_EXTREME": "[!!]",
                "CREDIT_OVERHEAT": "[!]",
            }.get(d.get("type", ""), "[!]")
            lines.append(
                f"{i}. {sig_emoji} {d['name']}({d['code']})"
            )
            if d.get("msg"):
                lines.append(f"   {d['msg']}")

    if cover:
        lines.append("")
        lines.append("[기회] 숏커버 반등 후보")
        for i, c in enumerate(cover[:5], 1):
            cr = c.get("cover_ratio", 0)
            lines.append(
                f"{i}. {c['name']}({c['code']}) "
                f"상환/신규 {cr:.1f}x"
            )
            if c.get("msg"):
                lines.append(f"   {c['msg']}")

    sig_count = data.get("signal_count", len(danger) + len(cover))
    lines.append("")
    lines.append("---")
    lines.append(f"시그널 {sig_count}건 감지")
    lines.append("주의: 시그널은 보조지표, 단독 매매근거 아님")
    lines.append("---")

    return "\n".join(lines)


# ═══════════════════════════════════════════════════
#  CLI 테스트
# ═══════════════════════════════════════════════════

if __name__ == "__main__":
    import sys

    logging.basicConfig(level=logging.INFO, format="%(message)s")

    if "--test" in sys.argv:
        test_codes = [
            ("005930", "삼성전자"),
            ("000660", "SK하이닉스"),
            ("042670", "두산에너빌리티"),
            ("373220", "LG에너지솔루션"),
            ("012450", "한화에어로스페이스"),
        ]
        for code, name in test_codes:
            r = analyze_short_interest(code, name)
            emoji = {"BULLISH": "+", "BEARISH": "-", "NEUTRAL": "o"}.get(
                r.signal, "o"
            )
            sigs = ",".join(r.signals_fired) if r.signals_fired else "none"
            print(
                f"[{emoji}] {r.name:12s} | "
                f"공매도{r.short_ratio:.1f}% 신용{r.credit_balance_rate:.1f}% | "
                f"{r.short_trend:10s} | "
                f"점수 {r.score_adjustment:+.1f} | "
                f"시그널: {sigs} | {r.detail}"
            )

    elif "--journal" in sys.argv:
        data = get_short_journal_data()
        print(format_journal_section(data))

    elif "--alert" in sys.argv:
        data = get_short_journal_data()
        alert = format_short_alert(data)
        if alert:
            print(alert)
        else:
            print("(시그널 없음)")

    elif "--scan" in sys.argv:
        # CSV 전체 스캔 — 위험/기회 종목 발굴
        print("supply_tracker CSV 스캔 중...")
        data = _journal_from_csv()
        print(f"위험 {len(data['building_top5'])}종목:")
        for d in data["building_top5"]:
            print(f"  {d['code']} {d['name']} score={d['score']}")
        print(f"기회 {len(data['covering_top5'])}종목:")
        for c in data["covering_top5"]:
            print(f"  {c['code']} {c['name']} score={c['score']}")

    else:
        print("사용법:")
        print("  python -m data.short_analyzer --test      주요 종목 분석")
        print("  python -m data.short_analyzer --journal   저널용 섹션")
        print("  python -m data.short_analyzer --alert     텔레그램 알림 포맷")
        print("  python -m data.short_analyzer --scan      전체 CSV 스캔")
