"""
공매도 잔고 분석 + 스코어링
===========================
SHORT-01: 잔고율 + 추세 + 대차잔고 → 점수화
SHORT-02: 스코어링 연동 (숏커버 +5, 숏빌딩 -5)
SHORT-03: Market Journal 연동

데이터 소스: data_store/short/{code}_short_bal.csv (캐시, 2년치)
  컬럼: 공매도잔고, 상장주식수, 공매도금액, 시가총액, 비중(%)
  최신: 2026-02-24 (pykrx API 깨짐으로 신규 수집 불가)
"""

import json
import logging
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd

logger = logging.getLogger(__name__)

STORE_DIR = Path(__file__).resolve().parent.parent / "data_store"
SHORT_DIR = STORE_DIR / "short"


@dataclass
class ShortAnalysis:
    """종목별 공매도 분석 결과"""
    code: str
    name: str = ""
    short_ratio: float = 0.0         # 최신 공매도 잔고율 %
    short_level: str = "UNKNOWN"      # HIGH(5%+) / MODERATE(2~5%) / LOW(<2%)
    short_trend: str = "NEUTRAL"      # COVERING / BUILDING / NEUTRAL
    trend_days: int = 0               # 추세 지속일
    ratio_change_5d: float = 0.0      # 5일 잔고율 변화 (pp)
    signal: str = "NEUTRAL"           # BULLISH(숏커버) / BEARISH(숏빌딩) / NEUTRAL
    score_adjustment: float = 0.0     # 스코어링 반영 점수
    detail: str = ""                  # 요약 텍스트


# ═══════════════════════════════════════════════════
#  SHORT-01: 분석
# ═══════════════════════════════════════════════════

def _load_short_balance(code: str) -> Optional[pd.DataFrame]:
    """공매도 잔고 캐시 로드"""
    path = SHORT_DIR / f"{code}_short_bal.csv"
    if not path.exists():
        return None
    try:
        df = pd.read_csv(path, index_col=0, parse_dates=True)
        if df.empty or "비중" not in df.columns:
            return None
        return df
    except Exception:
        return None


def analyze_short_interest(code: str, name: str = "") -> ShortAnalysis:
    """공매도 잔고 분석 — 잔고율 + 추세 + 스코어

    Returns:
        ShortAnalysis with score_adjustment (-8 ~ +8)
    """
    result = ShortAnalysis(code=code, name=name)

    df = _load_short_balance(code)
    if df is None or len(df) < 10:
        result.detail = "데이터부족"
        return result

    # DC-04: 데이터 만료 체크 (14일 이상 오래되면 중립 반환)
    try:
        last_date = df.index[-1]
        if hasattr(last_date, 'to_pydatetime'):
            last_date = last_date.to_pydatetime().replace(tzinfo=None)
        days_stale = (datetime.now() - last_date).days
        if days_stale > 14:
            result.detail = f"데이터만료({days_stale}일전)"
            return result
    except Exception:
        pass

    ratios = df["비중"].astype(float)

    # (1) 최신 잔고율
    latest_ratio = ratios.iloc[-1]
    result.short_ratio = round(latest_ratio, 3)

    if latest_ratio >= 5.0:
        result.short_level = "HIGH"
    elif latest_ratio >= 2.0:
        result.short_level = "MODERATE"
    else:
        result.short_level = "LOW"

    # (2) 5일 추세 분석
    recent = ratios.iloc[-5:]
    diffs = recent.diff().dropna()

    if len(diffs) >= 3:
        # 연속 감소/증가 일수 (끝에서부터 역산)
        neg_streak = 0
        pos_streak = 0
        for d in reversed(list(diffs)):
            if d < 0:
                if pos_streak > 0:
                    break
                neg_streak += 1
            elif d > 0:
                if neg_streak > 0:
                    break
                pos_streak += 1
            else:
                break

        if neg_streak >= 3:
            result.short_trend = "COVERING"
            result.trend_days = neg_streak
        elif pos_streak >= 3:
            result.short_trend = "BUILDING"
            result.trend_days = pos_streak
        else:
            result.short_trend = "NEUTRAL"

    # 5일 변화폭 (pp)
    if len(ratios) >= 6:
        result.ratio_change_5d = round(ratios.iloc[-1] - ratios.iloc[-6], 4)

    # (3) 스코어 계산
    score = 0.0

    if result.short_trend == "COVERING":
        score += 5.0
        result.signal = "BULLISH"
        # 거래량 급증 동반 → 숏 스퀴즈 가능
        if "공매도잔고" in df.columns:
            bal_recent = df["공매도잔고"].iloc[-5:]
            bal_ratio = bal_recent.iloc[0] / max(bal_recent.iloc[-1], 1)
            if bal_ratio >= 1.3:  # 잔고 30%+ 감소
                score += 3.0  # +8 총
                result.signal = "BULLISH"
                result.detail = f"잔고율{latest_ratio:.2f}%↓{result.trend_days}일 숏스퀴즈"
            else:
                result.detail = f"잔고율{latest_ratio:.2f}%↓{result.trend_days}일 숏커버"

    elif result.short_trend == "BUILDING":
        score -= 5.0
        result.signal = "BEARISH"
        if result.short_level == "HIGH":
            score -= 3.0  # -8 총
            result.detail = f"잔고율{latest_ratio:.2f}%↑{result.trend_days}일 위험"
        else:
            result.detail = f"잔고율{latest_ratio:.2f}%↑{result.trend_days}일 숏빌딩"

    else:
        result.signal = "NEUTRAL"
        if result.short_level == "HIGH":
            score -= 2.0  # HIGH 잔고는 그 자체로 부담
            result.detail = f"잔고율{latest_ratio:.2f}% HIGH (주의)"
        else:
            result.detail = f"잔고율{latest_ratio:.2f}% 안정"

    result.score_adjustment = round(max(-8.0, min(8.0, score)), 1)
    return result


# ═══════════════════════════════════════════════════
#  SHORT-02: 스코어링 연동용 조회 함수
# ═══════════════════════════════════════════════════

def get_short_score(code: str, name: str = "") -> Tuple[float, str]:
    """morning_recommendation에서 호출 — score + 사유

    Returns:
        (score: -8~+8, detail: str)
    """
    result = analyze_short_interest(code, name)
    return result.score_adjustment, result.detail


def is_short_building_danger(code: str) -> bool:
    """숏빌딩 + HIGH 잔고 → 추천 제외용 Hard Filter"""
    result = analyze_short_interest(code)
    return result.short_trend == "BUILDING" and result.short_level == "HIGH"


# ═══════════════════════════════════════════════════
#  SHORT-03: Market Journal 연동
# ═══════════════════════════════════════════════════

def get_short_journal_data(universe_codes: List[str] = None) -> Dict:
    """Market Journal용 공매도 섹션 데이터

    Returns:
        {
            "building_top5": [{"code", "name", "ratio", "trend_days"}],
            "covering_top5": [{"code", "name", "ratio", "trend_days"}],
            "high_short_count": int,
        }
    """
    if universe_codes is None:
        # 캐시 디렉토리에서 종목 코드 추출
        if not SHORT_DIR.exists():
            return {"building_top5": [], "covering_top5": [], "high_short_count": 0}
        codes = []
        for p in SHORT_DIR.glob("*_short_bal.csv"):
            code = p.stem.replace("_short_bal", "")
            if len(code) == 6 and code.isdigit():
                codes.append(code)
        universe_codes = codes[:500]  # 상위 500개만 분석 (성능)

    buildings = []
    coverings = []
    high_count = 0

    for code in universe_codes:
        r = analyze_short_interest(code)
        if r.short_level == "HIGH":
            high_count += 1
        if r.short_trend == "BUILDING" and r.score_adjustment <= -5:
            buildings.append({
                "code": code, "name": r.name,
                "ratio": r.short_ratio, "trend_days": r.trend_days,
                "score": r.score_adjustment,
            })
        elif r.short_trend == "COVERING" and r.score_adjustment >= 5:
            coverings.append({
                "code": code, "name": r.name,
                "ratio": r.short_ratio, "trend_days": r.trend_days,
                "score": r.score_adjustment,
            })

    buildings.sort(key=lambda x: x["score"])
    coverings.sort(key=lambda x: -x["score"])

    return {
        "building_top5": buildings[:5],
        "covering_top5": coverings[:5],
        "high_short_count": high_count,
    }


def format_journal_section(data: Dict) -> str:
    """Market Journal 텔레그램 포맷"""
    lines = ["📉 공매도 잔고"]
    if data.get("covering_top5"):
        lines.append("  숏커버링:")
        for s in data["covering_top5"]:
            lines.append(f"    {s['name']} 잔고{s['ratio']:.2f}%↓{s['trend_days']}일")
    if data.get("building_top5"):
        lines.append("  숏빌딩 (주의):")
        for s in data["building_top5"]:
            lines.append(f"    {s['name']} 잔고{s['ratio']:.2f}%↑{s['trend_days']}일")
    if data.get("high_short_count", 0) > 0:
        lines.append(f"  잔고율 5%+ 종목: {data['high_short_count']}개")
    return "\n".join(lines)


# ═══════════════════════════════════════════════════
#  CLI 테스트
# ═══════════════════════════════════════════════════

if __name__ == "__main__":
    import sys

    logging.basicConfig(level=logging.INFO, format="%(message)s")

    if "--test" in sys.argv:
        # 주요 종목 테스트
        test_codes = [
            ("005930", "삼성전자"),
            ("000660", "SK하이닉스"),
            ("042670", "두산에너빌리티"),
            ("373220", "LG에너지솔루션"),
            ("012450", "한화에어로스페이스"),
        ]
        for code, name in test_codes:
            r = analyze_short_interest(code, name)
            emoji = {"BULLISH": "🟢", "BEARISH": "🔴", "NEUTRAL": "⚪"}.get(r.signal, "⚪")
            print(
                f"{emoji} {r.name:12s} | 잔고율 {r.short_ratio:.3f}% {r.short_level:8s} | "
                f"{r.short_trend:10s} {r.trend_days}일 | "
                f"점수 {r.score_adjustment:+.1f} | {r.detail}"
            )
    elif "--journal" in sys.argv:
        data = get_short_journal_data()
        print(format_journal_section(data))
    else:
        print("사용법:")
        print("  python -m data.short_analyzer --test      주요 종목 분석")
        print("  python -m data.short_analyzer --journal   저널용 TOP5")
