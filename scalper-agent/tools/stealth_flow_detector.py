"""
스텔스 수급 탐지기 (Stealth Flow Detector)
============================================
supply_tracker CSV의 멀티데이 추세를 분석하여 3가지 시크릿 패턴을 탐지합니다.

전략 1: STEALTH_ACCUMULATION — 외인 연속 매집 + 숏커버 겹침
  조건: foreign_net_amt 3일+ 연속 양수 + (loan_balance 감소 or short 잔고 높음)
  의미: 기관 숏 청산 + 외인 진입 = 상승 전 마지막 신호

전략 2: SUPPLY_DIVERGENCE — 외인 매도 vs 가격 방어
  조건: foreign 대량 매도 + 가격 -1% 이내 + 기관/개인 흡수
  의미: 외인 매도 소화 완료 → 반등 임박

전략 3: CREDIT_BOTTOM — 신용 청산 바닥
  조건: credit_balance_rate 최근 급감 (5일 전 대비 -2%p+) + 외인 매수 전환
  의미: 반대매매 종료 + 외인 진입 → 바닥 매수

데이터 소스:
  D:/Global_Stock_Overview_Scripter_정보봇/data/supply_tracker/{ticker}.csv
"""

import csv
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger("BH.StealthFlow")

# ═══ 경로 상수 ═══
SUPPLY_TRACKER_DIR = Path(
    "D:/Global_Stock_Overview_Scripter_정보봇/data/supply_tracker"
)

# ═══ 전략 1: 스텔스 매집 + 숏커버 ═══
STEALTH_MIN_POSITIVE_DAYS = 3     # 외인 양수 최소 일수 (최근 5일 중)
STEALTH_MIN_TOTAL_AMT = 50        # 최소 누적 금액 (억원)
STEALTH_SHORT_BOOST = 3.0         # 공매도 잔고비율 이 이상이면 숏커버 보너스
STEALTH_BASE_SCORE = 12           # 기본 점수
STEALTH_SHORT_BONUS = 8           # 숏커버 겹침 보너스
STEALTH_DUAL_BUY_BONUS = 5        # 외인+기관 동시 매수 보너스

# ═══ 전략 2: 수급 다이버전스 ═══
DIVERGE_FOREIGN_SELL_MIN = -50    # 외인 매도 최소 (억원)
DIVERGE_PRICE_HOLD_MAX = -1.0     # 가격 하락 제한 (%)
DIVERGE_ABSORB_MIN = 30           # 흡수 주체 매수 최소 (억원)
DIVERGE_BASE_SCORE = 10
DIVERGE_STRONG_ABSORB_BONUS = 5   # 기관 흡수량 > 외인 매도량 50%+

# ═══ 전략 3: 신용 청산 바닥 ═══
CREDIT_HIGH_THRESHOLD = 3.0       # 이전 고점 신용잔고율 (%)
CREDIT_DROP_MIN = 1.5             # 최소 하락폭 (%p)
CREDIT_FOREIGN_BUY_MIN = 10       # 외인 순매수 최소 (억원)
CREDIT_BASE_SCORE = 10
CREDIT_DEEP_DROP_BONUS = 5        # 3%p+ 급감 보너스

# ═══ CSV 관련 상수 ═══
LOOKBACK_DAYS = 7                 # CSV 최근 N일 검사
AMT_UNIT_DIVISOR = 100            # 백만원 → 억원


@dataclass
class StealthSignal:
    """스텔스 수급 탐지 결과"""
    code: str
    name: str = ""
    strategy: str = ""        # STEALTH_ACCUMULATION / SUPPLY_DIVERGENCE / CREDIT_BOTTOM
    score: float = 0.0
    tag: str = ""
    reasons: List[str] = field(default_factory=list)
    # 상세 데이터
    foreign_5d_total: float = 0.0    # 억원
    inst_5d_total: float = 0.0       # 억원
    positive_days: int = 0
    credit_drop: float = 0.0         # %p
    price_change: float = 0.0        # %


# ═══ 캐시 (5분 TTL) ═══
_stealth_cache: Dict[str, StealthSignal] = {}
_cache_ts: float = 0.0
_CACHE_TTL = 300


def _load_csv(ticker: str, days: int = LOOKBACK_DAYS) -> List[dict]:
    """supply_tracker CSV에서 최근 N일 로드."""
    path = SUPPLY_TRACKER_DIR / f"{ticker}.csv"
    if not path.exists():
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            rows = list(csv.DictReader(f))
        return rows[-days:] if len(rows) >= days else rows
    except Exception:
        return []


def _safe_float(val, default=0.0) -> float:
    """안전한 float 변환 (빈 문자열, None 처리)."""
    try:
        return float(val) if val else default
    except (ValueError, TypeError):
        return default


# ═══════════════════════════════════════════════════
#  전략 1: 스텔스 매집 + 숏커버 겹침
# ═══════════════════════════════════════════════════

def _detect_stealth_accumulation(ticker: str, rows: List[dict]) -> Optional[StealthSignal]:
    """외인 연속 매집 + 숏커버 겹침 탐지.

    조건:
      - 최근 5일 중 3일+ 외인 순매수 양수
      - 누적 순매수 50억+ (백만원→억원)
      - 보너스: loan_balance 감소 추세 or short_ratio 높음
    """
    if len(rows) < 3:
        return None

    recent = rows[-5:] if len(rows) >= 5 else rows

    # 외인 순매수 일수 + 합계
    foreign_amts = []
    inst_amts = []
    for r in recent:
        fr = _safe_float(r.get("foreign_net_amt"))
        inst = _safe_float(r.get("inst_net_amt"))
        foreign_amts.append(fr)
        inst_amts.append(inst)

    positive_days = sum(1 for a in foreign_amts if a > 0)
    total_fr_억 = sum(foreign_amts) / AMT_UNIT_DIVISOR
    total_inst_억 = sum(inst_amts) / AMT_UNIT_DIVISOR

    if positive_days < STEALTH_MIN_POSITIVE_DAYS:
        return None
    if total_fr_억 < STEALTH_MIN_TOTAL_AMT:
        return None

    # 스코어 계산
    score = STEALTH_BASE_SCORE
    reasons = [f"외인{positive_days}일연속매수(+{total_fr_억:,.0f}억)"]

    # 숏커버 보너스: loan_balance 감소 추세
    loan_vals = [_safe_float(r.get("loan_balance_qty")) for r in recent]
    loan_nonzero = [v for v in loan_vals if v > 0]
    if len(loan_nonzero) >= 2 and loan_nonzero[-1] < loan_nonzero[0]:
        score += STEALTH_SHORT_BONUS
        drop_pct = (loan_nonzero[0] - loan_nonzero[-1]) / loan_nonzero[0] * 100
        reasons.append(f"대차잔고감소{drop_pct:.0f}%")

    # short_ratio가 높으면 숏커버 연료 풍부
    last_row = rows[-1]
    loan_rate = _safe_float(last_row.get("loan_balance_rate"))
    if loan_rate >= STEALTH_SHORT_BOOST:
        score += STEALTH_SHORT_BONUS // 2
        reasons.append(f"공매도잔고{loan_rate:.1f}%")

    # 외인+기관 쌍매수 보너스
    inst_positive = sum(1 for a in inst_amts if a > 0)
    if inst_positive >= 3 and total_inst_억 > 30:
        score += STEALTH_DUAL_BUY_BONUS
        reasons.append(f"기관동시매수(+{total_inst_억:,.0f}억)")

    # 금액 비례 보너스 (100억당 +2, 최대 +10)
    amt_bonus = min(int(total_fr_억 / 100) * 2, 10)
    score += amt_bonus

    return StealthSignal(
        code=ticker,
        strategy="STEALTH_ACCUMULATION",
        score=score,
        tag="STEALTH_BUY",
        reasons=reasons,
        foreign_5d_total=total_fr_억,
        inst_5d_total=total_inst_억,
        positive_days=positive_days,
    )


# ═══════════════════════════════════════════════════
#  전략 2: 수급 다이버전스 반전
# ═══════════════════════════════════════════════════

def _detect_supply_divergence(ticker: str, rows: List[dict]) -> Optional[StealthSignal]:
    """외인 매도 + 가격 방어 + 기관/개인 흡수 탐지.

    조건:
      - 최근 1일 외인 순매도 50억+
      - 가격 변화 -1% 이내 (방어 성공)
      - 기관 or 개인 순매수 30억+
    """
    if not rows:
        return None

    last = rows[-1]
    fr_억 = _safe_float(last.get("foreign_net_amt")) / AMT_UNIT_DIVISOR
    inst_억 = _safe_float(last.get("inst_net_amt")) / AMT_UNIT_DIVISOR
    indiv_억 = _safe_float(last.get("indiv_net_amt")) / AMT_UNIT_DIVISOR
    chg_pct = _safe_float(last.get("change_pct"))

    # 외인 매도 필요
    if fr_억 > DIVERGE_FOREIGN_SELL_MIN:
        return None

    # 가격 방어 (하락 -1% 이내)
    if chg_pct < DIVERGE_PRICE_HOLD_MAX:
        return None

    # 흡수 주체 확인
    absorber = ""
    absorb_amt = 0.0
    if inst_억 >= DIVERGE_ABSORB_MIN:
        absorber = "기관"
        absorb_amt = inst_억
    elif indiv_억 >= DIVERGE_ABSORB_MIN:
        absorber = "개인"
        absorb_amt = indiv_억
    else:
        return None

    # 스코어 계산
    score = DIVERGE_BASE_SCORE
    reasons = [
        f"외인매도{fr_억:,.0f}억+가격{chg_pct:+.1f}%방어",
        f"{absorber}흡수+{absorb_amt:,.0f}억",
    ]

    # 흡수량이 매도량 50%+ → 강한 흡수
    if absorb_amt >= abs(fr_억) * 0.5:
        score += DIVERGE_STRONG_ABSORB_BONUS
        reasons.append("강력흡수(50%+)")

    # 가격 상승이면 추가 보너스
    if chg_pct > 0:
        score += 3
        reasons.append(f"가격상승{chg_pct:+.1f}%")

    # 2일 연속 다이버전스면 추가 보너스
    if len(rows) >= 2:
        prev = rows[-2]
        prev_fr = _safe_float(prev.get("foreign_net_amt")) / AMT_UNIT_DIVISOR
        prev_chg = _safe_float(prev.get("change_pct"))
        if prev_fr < DIVERGE_FOREIGN_SELL_MIN and prev_chg > DIVERGE_PRICE_HOLD_MAX:
            score += 5
            reasons.append("2일연속다이버전스")

    return StealthSignal(
        code=ticker,
        strategy="SUPPLY_DIVERGENCE",
        score=score,
        tag="DIVERGE_BUY",
        reasons=reasons,
        foreign_5d_total=fr_억,
        inst_5d_total=inst_억 if absorber == "기관" else 0,
        price_change=chg_pct,
    )


# ═══════════════════════════════════════════════════
#  전략 3: 신용 청산 바닥
# ═══════════════════════════════════════════════════

def _detect_credit_bottom(ticker: str, rows: List[dict]) -> Optional[StealthSignal]:
    """신용잔고 급감 + 외인 매수 전환 탐지.

    조건:
      - 과거 신용잔고율 3%+ → 현재 1.5%p+ 하락
      - 외인 순매수 전환 (10억+)
    """
    if len(rows) < 3:
        return None

    credit_rates = [_safe_float(r.get("credit_balance_rate")) for r in rows]
    # 0이 아닌 값만 추출
    nonzero_rates = [(i, v) for i, v in enumerate(credit_rates) if v > 0]

    if len(nonzero_rates) < 2:
        return None

    peak_rate = max(v for _, v in nonzero_rates)
    current_rate = nonzero_rates[-1][1]
    drop = peak_rate - current_rate

    # 고점이 3%+ 이상이고 1.5%p+ 하락해야 함
    if peak_rate < CREDIT_HIGH_THRESHOLD:
        return None
    if drop < CREDIT_DROP_MIN:
        return None

    # 외인 매수 전환 확인
    last = rows[-1]
    fr_억 = _safe_float(last.get("foreign_net_amt")) / AMT_UNIT_DIVISOR
    if fr_억 < CREDIT_FOREIGN_BUY_MIN:
        return None

    # 스코어 계산
    score = CREDIT_BASE_SCORE
    reasons = [
        f"신용잔고율{peak_rate:.1f}%→{current_rate:.1f}%(-{drop:.1f}%p)",
        f"외인매수전환+{fr_억:,.0f}억",
    ]

    # 급감 보너스 (3%p+)
    if drop >= 3.0:
        score += CREDIT_DEEP_DROP_BONUS
        reasons.append("신용급감3%p+")

    # 가격 반등 시 추가
    chg = _safe_float(last.get("change_pct"))
    if chg > 0:
        score += 3
        reasons.append(f"가격반등{chg:+.1f}%")

    return StealthSignal(
        code=ticker,
        strategy="CREDIT_BOTTOM",
        score=score,
        tag="CREDIT_BOT",
        reasons=reasons,
        foreign_5d_total=fr_억,
        credit_drop=drop,
        price_change=chg,
    )


# ═══════════════════════════════════════════════════
#  메인 스캔 함수
# ═══════════════════════════════════════════════════

def scan_stealth_flow(codes: List[str] = None) -> Dict[str, StealthSignal]:
    """전체 supply_tracker를 스캔하여 3전략 시그널 탐지.

    Args:
        codes: 스캔 대상 종목 리스트. None이면 전체 CSV 스캔.

    Returns:
        {stock_code: StealthSignal}  (시그널 감지된 종목만)
    """
    global _stealth_cache, _cache_ts

    if _stealth_cache and (time.time() - _cache_ts) < _CACHE_TTL:
        return _stealth_cache

    result: Dict[str, StealthSignal] = {}

    # 대상 종목 결정
    if codes:
        tickers = codes
    else:
        # 전체 CSV 파일에서 ticker 추출
        csv_files = list(SUPPLY_TRACKER_DIR.glob("*.csv"))
        tickers = [f.stem for f in csv_files]

    scanned = 0
    for ticker in tickers:
        if not ticker.isdigit() or len(ticker) != 6:
            continue

        rows = _load_csv(ticker, LOOKBACK_DAYS)
        if len(rows) < 3:
            continue

        scanned += 1

        # 전략 1: 스텔스 매집
        sig1 = _detect_stealth_accumulation(ticker, rows)
        if sig1:
            # 이름은 CSV에 없으므로 나중에 매핑
            if ticker not in result or sig1.score > result[ticker].score:
                result[ticker] = sig1

        # 전략 2: 수급 다이버전스
        sig2 = _detect_supply_divergence(ticker, rows)
        if sig2:
            if ticker not in result or sig2.score > result[ticker].score:
                result[ticker] = sig2

        # 전략 3: 신용 청산 바닥
        sig3 = _detect_credit_bottom(ticker, rows)
        if sig3:
            if ticker not in result or sig3.score > result[ticker].score:
                result[ticker] = sig3

    # 종목명 매핑 시도
    _fill_names(result)

    logger.info(
        f"[스텔스탐지] {scanned}종목 스캔 → {len(result)}시그널 "
        f"(매집:{sum(1 for s in result.values() if s.strategy == 'STEALTH_ACCUMULATION')} "
        f"다이버전스:{sum(1 for s in result.values() if s.strategy == 'SUPPLY_DIVERGENCE')} "
        f"신용바닥:{sum(1 for s in result.values() if s.strategy == 'CREDIT_BOTTOM')})"
    )

    _stealth_cache = result
    _cache_ts = time.time()

    return result


def _fill_names(signals: Dict[str, StealthSignal]):
    """종목명 매핑 — daily_intelligence 또는 investor_flow에서."""
    try:
        import json
        from utils.jgis_path import jgis_intel_path
        intel_path = jgis_intel_path()
        if intel_path.exists():
            raw = json.loads(intel_path.read_text(encoding="utf-8"))
            # short_selling_summary.data에서 이름 추출
            short_data = raw.get("short_selling_summary", {}).get("data", {})
            for code, sig in signals.items():
                if not sig.name and code in short_data:
                    sig.name = short_data[code].get("name", "")
            # investor_flow_summary에서도 추출
            flow = raw.get("investor_flow_summary", {})
            for key in ("foreign_top_buy", "foreign_top_sell",
                        "institution_top_buy", "institution_top_sell"):
                for item in flow.get(key, []):
                    code = item.get("ticker", "")
                    if code in signals and not signals[code].name:
                        signals[code].name = item.get("name", "")
    except Exception:
        pass

    # 폴백 1: raw investor_flow.json
    try:
        import json
        from datetime import timedelta
        today = datetime.now()
        for delta in range(3):
            dt = today - timedelta(days=delta)
            fpath = Path(
                f"D:/Global_Stock_Overview_Scripter_정보봇/data/supply_daily/"
                f"{dt.strftime('%Y-%m-%d')}_investor_flow.json"
            )
            if not fpath.exists():
                continue
            raw = json.loads(fpath.read_text(encoding="utf-8"))
            for key in ("foreign_top_buy", "foreign_top_sell",
                        "institution_top_buy", "institution_top_sell"):
                for item in raw.get(key, []):
                    code = item.get("ticker", "")
                    if code in signals and not signals[code].name:
                        signals[code].name = item.get("name", "")
            break
    except Exception:
        pass

    # 폴백 2: universe.json (가장 넓은 커버리지)
    try:
        import json
        uni_path = Path(__file__).resolve().parent.parent / "data_store" / "universe.json"
        if uni_path.exists():
            uni = json.loads(uni_path.read_text(encoding="utf-8"))
            for code, sig in signals.items():
                if not sig.name and code in uni:
                    sig.name = uni[code].get("name", "")
    except Exception:
        pass


# ═══════════════════════════════════════════════════
#  Public API
# ═══════════════════════════════════════════════════

def get_stealth_score(code: str) -> Tuple[float, str]:
    """morning_recommendation 연동 — 스텔스 스코어 + 사유."""
    signals = scan_stealth_flow()
    if code in signals:
        sig = signals[code]
        detail = " | ".join(sig.reasons[:3])
        return sig.score, detail
    return 0.0, ""


def format_stealth_flow_alert(signals: Dict[str, StealthSignal] = None) -> str:
    """08:15 텔레그램 스텔스 수급 알림."""
    if signals is None:
        signals = scan_stealth_flow()

    if not signals:
        return ""

    now = datetime.now()
    today_str = now.strftime("%m/%d(%a)")
    time_str = now.strftime("%H:%M")

    lines = []
    lines.append("---")
    lines.append("  스텔스 수급 탐지 리포트")
    lines.append(f"  {today_str} {time_str}")
    lines.append("---")

    # 전략별 분류
    stealth = sorted(
        [s for s in signals.values() if s.strategy == "STEALTH_ACCUMULATION"],
        key=lambda x: x.score, reverse=True,
    )
    diverge = sorted(
        [s for s in signals.values() if s.strategy == "SUPPLY_DIVERGENCE"],
        key=lambda x: x.score, reverse=True,
    )
    credit = sorted(
        [s for s in signals.values() if s.strategy == "CREDIT_BOTTOM"],
        key=lambda x: x.score, reverse=True,
    )

    if stealth:
        lines.append("")
        lines.append("[1] 스텔스 매집 + 숏커버")
        lines.append("  외인 연속매수 + 공매도잔고 감소 = 폭발 직전")
        for i, s in enumerate(stealth[:7], 1):
            name = s.name or s.code
            reasons_str = " | ".join(s.reasons[:2])
            lines.append(
                f"  {i}. {name}({s.code}) +{s.score:.0f}점 "
                f"[{reasons_str}]"
            )

    if diverge:
        lines.append("")
        lines.append("[2] 수급 다이버전스")
        lines.append("  외인 매도에도 가격 방어 = 반등 임박")
        for i, s in enumerate(diverge[:5], 1):
            name = s.name or s.code
            reasons_str = " | ".join(s.reasons[:2])
            lines.append(
                f"  {i}. {name}({s.code}) +{s.score:.0f}점 "
                f"[{reasons_str}]"
            )

    if credit:
        lines.append("")
        lines.append("[3] 신용 청산 바닥")
        lines.append("  반대매매 종료 + 외인 진입 = 바닥 매수")
        for i, s in enumerate(credit[:5], 1):
            name = s.name or s.code
            reasons_str = " | ".join(s.reasons[:2])
            lines.append(
                f"  {i}. {name}({s.code}) +{s.score:.0f}점 "
                f"[{reasons_str}]"
            )

    lines.append("")
    lines.append(
        f"총 {len(signals)}시그널 "
        f"(매집:{len(stealth)} 다이버전스:{len(diverge)} 신용바닥:{len(credit)})"
    )

    return "\n".join(lines)


# ═══════════════════════════════════════════════════
#  CLI 테스트
# ═══════════════════════════════════════════════════

if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    sys.stdout.reconfigure(encoding="utf-8")

    signals = scan_stealth_flow()
    print(f"\n총 {len(signals)} 시그널\n")

    alert = format_stealth_flow_alert(signals)
    if alert:
        print(alert)
    else:
        print("(시그널 없음)")
