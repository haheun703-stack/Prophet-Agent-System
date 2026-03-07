# -*- coding: utf-8 -*-
"""
크로스에셋 스트레스 지수 (Cross-Asset Stress Index)
====================================================
4쌍의 자산간 롤링 상관관계를 모니터링하여 "상관 붕괴" 감지.

핵심 원리:
  평소: Gold↔Stock 역상관(-0.3), Dollar↔Stock 역상관, Bond↔Stock 역상관
  위기 임박: 상관 0으로 수렴 (관계 소멸)
  진짜 위기: 양상관(+0.3+) = 모든 자산 동시 하락 = 유동성 위기

4쌍:
  1. Gold ↔ KOSPI     (평소 역상관 → 양상관이면 위기)
  2. USD/KRW ↔ KOSPI  (평소 역상관 → 양상관이면 자본유출)
  3. Bond(TNX) ↔ KOSPI (평소 약한 역상관 → 동시 상승은 정상, 동시 하락은 위기)
  4. Oil(WTI) ↔ KOSPI  (평소 양상관 → 역상관이면 수요 붕괴)

스트레스 레벨:
  0~2: NORMAL (정상 상관관계)
  3~4: ELEVATED (일부 상관 이탈)
  5~6: HIGH (3쌍 이상 이탈)
  7~10: CRITICAL (유동성 위기 가능성)

데이터 소스: yfinance (nightwatch와 동일)
저장: jarvis/data/cross_asset_history.json (30일 롤링)
"""

import json
import logging
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
from dataclasses import dataclass, field, asdict
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = Path(__file__).resolve().parent / "data"
HISTORY_PATH = DATA_DIR / "cross_asset_history.json"

# ═══════════════════════════════════════
#  자산 쌍 정의
# ═══════════════════════════════════════

ASSET_PAIRS = {
    "gold_kospi": {
        "name": "Gold↔KOSPI",
        "asset_a": "GC=F",       # 금 선물
        "asset_b": "^KS11",      # KOSPI 지수
        "normal_corr": -0.2,     # 평소 약한 역상관
        "crisis_threshold": 0.2,  # 이 이상이면 상관 붕괴
        "weight": 2.5,           # 가장 중요 (유동성 위기 1차 신호)
    },
    "usd_kospi": {
        "name": "USD/KRW↔KOSPI",
        "asset_a": "KRW=X",     # USD/KRW (원 약세 = 양수)
        "asset_b": "^KS11",
        "normal_corr": -0.25,    # 원 약세 ↔ 주가 하락 (역상관)
        "crisis_threshold": 0.15, # 동시 상승(원 약세+주가 상승)은 흔치않으니 낮게
        "weight": 2.0,
    },
    "bond_kospi": {
        "name": "TNX↔KOSPI",
        "asset_a": "^TNX",       # 미국 10년 국채 금리
        "asset_b": "^KS11",
        "normal_corr": 0.1,      # 평소 약한 양상관 (경기 좋으면 둘 다 상승)
        "crisis_threshold": -0.3, # 금리 상승 + 주가 하락 = 채권 자경단
        "weight": 2.0,
    },
    "oil_kospi": {
        "name": "WTI↔KOSPI",
        "asset_a": "CL=F",      # WTI 원유 선물
        "asset_b": "^KS11",
        "normal_corr": 0.3,      # 평소 양상관 (경기 연동)
        "crisis_threshold": -0.2, # 유가↓ + 주가↓는 정상이지만, 유가↑ + 주가↓는 스태그
        "weight": 1.5,
    },
}


@dataclass
class PairCorrelation:
    """자산 쌍의 상관관계 분석 결과"""
    pair_id: str
    pair_name: str
    correlation_20d: float = 0.0   # 20일 롤링 상관
    correlation_60d: float = 0.0   # 60일 롤링 상관 (중기 기준선)
    normal_corr: float = 0.0       # 이론적 정상 상관
    deviation: float = 0.0         # 정상에서의 이탈도 (0~1)
    is_anomaly: bool = False       # 상관 붕괴 여부
    stress_contribution: float = 0.0  # 스트레스 기여도 (0~2.5)
    signal: str = ""


@dataclass
class StressReport:
    """크로스에셋 스트레스 종합 보고"""
    date: str = ""
    stress_index: float = 0.0      # 0~10 종합 스트레스
    stress_level: str = "NORMAL"   # NORMAL / ELEVATED / HIGH / CRITICAL
    pairs: list = field(default_factory=list)  # [PairCorrelation]
    anomaly_count: int = 0         # 이상 쌍 수 (0~4)
    signal: str = ""               # 텔레그램용 1줄 요약
    # BRAIN 연동용
    allocation_adjust: dict = field(default_factory=dict)
    # 히스토리
    stress_trend: str = ""         # UP / FLAT / DOWN (최근 3일 추세)


# ═══════════════════════════════════════
#  데이터 수집
# ═══════════════════════════════════════

def _fetch_returns(symbol: str, days: int = 90) -> Optional[np.ndarray]:
    """yfinance로 일간 수익률 시계열 수집 (최근 N일)"""
    try:
        import yfinance as yf
        end = datetime.now()
        start = end - timedelta(days=days + 30)  # 여유분

        data = yf.Ticker(symbol).history(
            start=start.strftime("%Y-%m-%d"),
            end=end.strftime("%Y-%m-%d"),
        )
        if data is None or len(data) < 20:
            logger.warning(f"[CrossAsset] {symbol}: 데이터 부족 ({len(data) if data is not None else 0}일)")
            return None

        closes = data["Close"].dropna().values
        if len(closes) < 20:
            return None

        # 일간 수익률 (%)
        returns = np.diff(closes) / closes[:-1] * 100
        return returns[-days:] if len(returns) > days else returns

    except Exception as e:
        logger.warning(f"[CrossAsset] {symbol} 수집 실패: {e}")
        return None


def _rolling_correlation(a: np.ndarray, b: np.ndarray, window: int = 20) -> float:
    """두 시계열의 롤링 상관관계 (마지막 window일 기준)"""
    min_len = min(len(a), len(b))
    if min_len < window:
        return 0.0

    a_win = a[-window:]
    b_win = b[-window:]

    # numpy corrcoef
    corr_matrix = np.corrcoef(a_win, b_win)
    corr = corr_matrix[0, 1]

    if np.isnan(corr):
        return 0.0
    return round(float(corr), 4)


# ═══════════════════════════════════════
#  스트레스 분석
# ═══════════════════════════════════════

def analyze_stress() -> StressReport:
    """크로스에셋 스트레스 지수 계산

    Returns:
        StressReport (stress_index: 0~10)
    """
    report = StressReport(date=datetime.now().strftime("%Y-%m-%d"))

    # 모든 자산의 수익률 수집 (캐시)
    returns_cache = {}

    for pair_id, pair_def in ASSET_PAIRS.items():
        sym_a = pair_def["asset_a"]
        sym_b = pair_def["asset_b"]

        # 수익률 수집 (캐시 활용)
        if sym_a not in returns_cache:
            returns_cache[sym_a] = _fetch_returns(sym_a, 90)
        if sym_b not in returns_cache:
            returns_cache[sym_b] = _fetch_returns(sym_b, 90)

        ret_a = returns_cache[sym_a]
        ret_b = returns_cache[sym_b]

        pc = PairCorrelation(
            pair_id=pair_id,
            pair_name=pair_def["name"],
            normal_corr=pair_def["normal_corr"],
        )

        if ret_a is None or ret_b is None:
            pc.signal = "데이터 부족"
            report.pairs.append(pc)
            continue

        # 20일 / 60일 롤링 상관
        pc.correlation_20d = _rolling_correlation(ret_a, ret_b, 20)
        pc.correlation_60d = _rolling_correlation(ret_a, ret_b, 60)

        # 이탈도 = |현재 상관 - 정상 상관| / 이론적 범위
        # 정상에서 멀어질수록 1에 가까움
        threshold = pair_def["crisis_threshold"]
        normal = pair_def["normal_corr"]

        # 방향성 이탈 (정상 대비 반대쪽으로 갔는지)
        if pair_id == "bond_kospi":
            # 채권: crisis_threshold가 음수 → 상관이 음수로 가면 위기
            is_crisis_direction = pc.correlation_20d < threshold
        elif pair_id == "oil_kospi":
            # 원유: crisis_threshold가 음수 → 양상관이 역상관으로 가면 위기
            is_crisis_direction = pc.correlation_20d < threshold
        else:
            # 금/달러: crisis_threshold가 양수 → 역상관이 양상관으로 가면 위기
            is_crisis_direction = pc.correlation_20d > threshold

        # 이탈도 계산 (0~1)
        deviation = abs(pc.correlation_20d - normal) / max(abs(threshold - normal), 0.1)
        pc.deviation = round(min(deviation, 2.0), 3)  # 2.0 캡

        # 이상 여부
        pc.is_anomaly = is_crisis_direction
        if pc.is_anomaly:
            report.anomaly_count += 1

        # 스트레스 기여도 = weight * deviation (이상 방향일 때만)
        if pc.is_anomaly:
            pc.stress_contribution = round(pair_def["weight"] * min(pc.deviation, 1.5), 2)
        else:
            pc.stress_contribution = 0.0

        # 신호 텍스트
        if pc.is_anomaly and pc.deviation > 1.0:
            pc.signal = f"🚨 상관 붕괴 (20D:{pc.correlation_20d:+.2f} vs 정상:{normal:+.2f})"
        elif pc.is_anomaly:
            pc.signal = f"⚠️ 상관 이탈 (20D:{pc.correlation_20d:+.2f})"
        elif abs(pc.correlation_20d - normal) > 0.3:
            pc.signal = f"🟡 주의 (20D:{pc.correlation_20d:+.2f})"
        else:
            pc.signal = f"🟢 정상 (20D:{pc.correlation_20d:+.2f})"

        report.pairs.append(pc)

    # ── 종합 스트레스 지수 ──
    total_stress = sum(p.stress_contribution for p in report.pairs)
    # 복수 이상 시 증폭: 3쌍 이상이면 1.5배
    if report.anomaly_count >= 3:
        total_stress *= 1.5
    elif report.anomaly_count >= 2:
        total_stress *= 1.2

    report.stress_index = round(min(total_stress, 10.0), 1)

    # 레벨 판정
    if report.stress_index >= 7.0:
        report.stress_level = "CRITICAL"
    elif report.stress_index >= 5.0:
        report.stress_level = "HIGH"
    elif report.stress_index >= 3.0:
        report.stress_level = "ELEVATED"
    else:
        report.stress_level = "NORMAL"

    # BRAIN 배분 조정 제안
    report.allocation_adjust = _calc_allocation_adjust(report)

    # 1줄 요약
    anomaly_names = [p.pair_name for p in report.pairs if p.is_anomaly]
    if report.stress_level == "CRITICAL":
        report.signal = f"🚨 유동성 위기 가능성 — {report.anomaly_count}/4쌍 붕괴"
    elif report.stress_level == "HIGH":
        report.signal = f"⚠️ 크로스에셋 스트레스 HIGH — {', '.join(anomaly_names)}"
    elif report.stress_level == "ELEVATED":
        report.signal = f"🟡 스트레스 상승 — {', '.join(anomaly_names)}"
    else:
        report.signal = "🟢 크로스에셋 정상"

    # 히스토리 저장 + 추세 계산
    _save_history(report)
    report.stress_trend = _calc_stress_trend()

    return report


def _calc_allocation_adjust(report: StressReport) -> dict:
    """스트레스 수준에 따른 BRAIN 배분 조정 제안"""
    if report.stress_level == "CRITICAL":
        return {
            "cash_add_pct": 20,      # 현금 20%p 추가
            "inverse_add_pct": 10,   # 인버스 10%p 추가
            "swing_reduce_pct": 25,  # 스윙 25%p 축소
            "reason": "유동성 위기 — 방어 최우선",
        }
    elif report.stress_level == "HIGH":
        return {
            "cash_add_pct": 10,
            "inverse_add_pct": 5,
            "swing_reduce_pct": 15,
            "reason": "크로스에셋 스트레스 — 사이즈 축소",
        }
    elif report.stress_level == "ELEVATED":
        return {
            "cash_add_pct": 5,
            "inverse_add_pct": 0,
            "swing_reduce_pct": 5,
            "reason": "스트레스 상승 — 신중한 진입",
        }
    else:
        return {
            "cash_add_pct": 0,
            "inverse_add_pct": 0,
            "swing_reduce_pct": 0,
            "reason": "정상 — 조정 불필요",
        }


# ═══════════════════════════════════════
#  히스토리 관리
# ═══════════════════════════════════════

def _save_history(report: StressReport):
    """스트레스 히스토리 저장 (30일 롤링)"""
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    history = {}
    if HISTORY_PATH.exists():
        try:
            history = json.loads(HISTORY_PATH.read_text(encoding="utf-8"))
        except Exception:
            history = {}

    # 오늘 데이터 추가
    history[report.date] = {
        "stress_index": report.stress_index,
        "stress_level": report.stress_level,
        "anomaly_count": report.anomaly_count,
        "pairs": {
            p.pair_id: {
                "corr_20d": p.correlation_20d,
                "corr_60d": p.correlation_60d,
                "deviation": p.deviation,
                "is_anomaly": p.is_anomaly,
            }
            for p in report.pairs
        },
    }

    # 30일 초과 데이터 삭제
    dates = sorted(history.keys())
    while len(dates) > 30:
        del history[dates.pop(0)]

    HISTORY_PATH.write_text(
        json.dumps(history, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _calc_stress_trend() -> str:
    """최근 3일 스트레스 추세"""
    if not HISTORY_PATH.exists():
        return "FLAT"

    try:
        history = json.loads(HISTORY_PATH.read_text(encoding="utf-8"))
    except Exception:
        return "FLAT"

    dates = sorted(history.keys())[-3:]
    if len(dates) < 2:
        return "FLAT"

    values = [history[d]["stress_index"] for d in dates]
    if values[-1] > values[0] + 0.5:
        return "UP"
    elif values[-1] < values[0] - 0.5:
        return "DOWN"
    return "FLAT"


def load_stress_history() -> dict:
    """히스토리 로드"""
    if HISTORY_PATH.exists():
        try:
            return json.loads(HISTORY_PATH.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}


# ═══════════════════════════════════════
#  텔레그램 포맷
# ═══════════════════════════════════════

def format_stress_report(report: StressReport) -> str:
    """텔레그램용 크로스에셋 스트레스 리포트"""
    level_emoji = {
        "NORMAL": "🟢",
        "ELEVATED": "🟡",
        "HIGH": "🟠",
        "CRITICAL": "🔴",
    }
    emoji = level_emoji.get(report.stress_level, "⬜")

    lines = [
        "📊 크로스에셋 스트레스 지수",
        f"📅 {report.date}",
        "",
        f"{emoji} 스트레스: {report.stress_index:.1f}/10 [{report.stress_level}]",
        f"   이상 쌍: {report.anomaly_count}/4 | 추세: {report.stress_trend}",
        "",
        "── 자산간 상관관계 ──",
    ]

    for p in report.pairs:
        lines.append(
            f"  {p.signal}"
        )
        lines.append(
            f"    {p.pair_name}: 20D={p.correlation_20d:+.2f}"
            f" 60D={p.correlation_60d:+.2f}"
            f" (정상:{p.normal_corr:+.2f})"
        )

    if report.allocation_adjust.get("reason"):
        lines.append("")
        lines.append(f"💡 {report.allocation_adjust['reason']}")
        adj = report.allocation_adjust
        if adj.get("swing_reduce_pct", 0) > 0:
            lines.append(f"   스윙 -{adj['swing_reduce_pct']}%p / 현금 +{adj['cash_add_pct']}%p")

    return "\n".join(lines)


# ═══════════════════════════════════════
#  CLI 테스트
# ═══════════════════════════════════════

if __name__ == "__main__":
    import sys
    sys.stdout.reconfigure(encoding="utf-8")
    logging.basicConfig(level=logging.INFO)
    print("크로스에셋 스트레스 분석 시작...")
    report = analyze_stress()
    print(format_stress_report(report))
    print(f"\n배분 조정: {report.allocation_adjust}")
