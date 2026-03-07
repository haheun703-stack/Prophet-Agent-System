# -*- coding: utf-8 -*-
"""
유동성 사이클 분석 (5D Ultra-Slow Eye)
═══════════════════════════════════════════
FRED 공개 데이터로 시중 유동성 사이클을 추적.

핵심 공식:
  Net Liquidity ≈ Fed BS - RRP - TGA
  라이트 버전: RRP 변화율 + TGA 변화율 + M2 전년비 증가율
  → 세 지표의 z-score 가중 합산 → 유동성 스코어(-10 ~ +10)

데이터 소스 (FRED, API 키 불필요):
  1. RRPONTSYD — 역레포 잔고 (일일, 10억$)
     RRP↓ = 유동성 확장 (역레포에서 돈이 빠져나와 시중으로)
  2. WTREGEN — 재무부 일반계정 TGA (주간, 100만$)
     TGA↓ = 유동성 확장 (재무부가 지출하면서 시중에 돈 풀림)
  3. M2SL — M2 통화량 (월간, 10억$)
     M2↑ = 유동성 확장 (통화량 자체가 늘어남)

가중치: RRP 40% / TGA 35% / M2 25%
  (RRP가 일일 업데이트로 반응성 가장 높아 가중치 최대)

판정:
  +5 이상: LIQUIDITY_EXPANSION (위험자산 우호적)
  +2~+5:  MILD_EXPANSION
  -2~+2:  NEUTRAL
  -5~-2:  MILD_CONTRACTION
  -5 이하: LIQUIDITY_DRAIN (위험자산 비우호적)

주기: 매일 (RRP 일일, TGA/M2는 최신값 forward-fill)
저장: jarvis/data/liquidity_history.json (90일 롤링)
"""

import csv
import io
import json
import logging
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple
from urllib.request import urlopen, Request
from urllib.error import URLError

logger = logging.getLogger(__name__)

DATA_DIR = Path(__file__).resolve().parent / "data"
CACHE_PATH = DATA_DIR / "liquidity_raw_cache.json"
HISTORY_PATH = DATA_DIR / "liquidity_history.json"

# ═══════════════════════════════════════
#  FRED 시리즈 정의
# ═══════════════════════════════════════

FRED_SERIES = {
    "rrp": {
        "series_id": "RRPONTSYD",
        "name": "역레포(RRP) 잔고",
        "unit": "10억$",
        "frequency": "daily",
        "weight": 0.40,
        # RRP 감소 = 유동성 확장 → z-score 부호 반전
        "invert": True,
        "change_window": 30,   # 30일 변화율 사용
    },
    "tga": {
        "series_id": "WTREGEN",
        "name": "재무부 TGA 잔고",
        "unit": "100만$",
        "frequency": "weekly",
        "weight": 0.35,
        # TGA 감소 = 유동성 확장 → z-score 부호 반전
        "invert": True,
        "change_window": 30,
    },
    "m2": {
        "series_id": "M2SL",
        "name": "M2 통화량",
        "unit": "10억$",
        "frequency": "monthly",
        "weight": 0.25,
        # M2 증가 = 유동성 확장 → 부호 유지
        "invert": False,
        "change_window": 365,  # 전년비(YoY) 증가율
    },
}

# FRED CSV URL (API 키 불필요)
FRED_CSV_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv?id={series_id}&cosd={start}&coed={end}"

# 캐시 유효 기간
CACHE_MAX_AGE_DAYS = 1  # RRP가 일일이므로 매일 갱신


# ═══════════════════════════════════════
#  데이터 클래스
# ═══════════════════════════════════════

@dataclass
class IndicatorStatus:
    """단일 유동성 지표"""
    name: str
    current_value: float = 0.0
    change_pct: float = 0.0      # 변화율 (%)
    z_score: float = 0.0         # 히스토리 대비 z-score
    weighted_z: float = 0.0      # 가중 z-score
    direction: str = ""          # EXPANDING / NEUTRAL / CONTRACTING
    data_date: str = ""          # 최신 데이터 날짜
    signal: str = ""


@dataclass
class LiquidityReport:
    """유동성 사이클 종합 보고"""
    date: str = ""
    # 개별 지표
    rrp: IndicatorStatus = field(
        default_factory=lambda: IndicatorStatus(name="역레포(RRP)"))
    tga: IndicatorStatus = field(
        default_factory=lambda: IndicatorStatus(name="재무부 TGA"))
    m2: IndicatorStatus = field(
        default_factory=lambda: IndicatorStatus(name="M2 통화량"))
    # 종합
    liquidity_score: float = 0.0    # -10 ~ +10
    liquidity_level: str = "NEUTRAL"
    confidence: float = 0.0
    signal: str = ""
    # BRAIN 연동
    allocation_adjust: dict = field(default_factory=dict)
    # 히스토리
    score_trend: str = ""           # UP / FLAT / DOWN


# ═══════════════════════════════════════
#  FRED 데이터 다운로드
# ═══════════════════════════════════════

def _download_fred_csv(series_id: str, years_back: int = 2) -> List[Tuple[str, float]]:
    """FRED에서 시계열 CSV 다운로드 (API 키 불필요)

    Returns:
        [(date_str, value), ...] 시간순
    """
    end = datetime.now().strftime("%Y-%m-%d")
    start = (datetime.now() - timedelta(days=years_back * 365 + 30)).strftime("%Y-%m-%d")
    url = FRED_CSV_URL.format(series_id=series_id, start=start, end=end)

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    }
    try:
        req = Request(url, headers=headers)
        with urlopen(req, timeout=30) as resp:
            text = resp.read().decode("utf-8", errors="replace")

        rows = []
        reader = csv.reader(io.StringIO(text))
        header = next(reader, None)  # 헤더 스킵
        for row in reader:
            if len(row) < 2:
                continue
            date_str = row[0].strip()
            val_str = row[1].strip()
            if val_str == "." or not val_str:
                continue  # FRED uses "." for missing
            try:
                val = float(val_str)
                rows.append((date_str, val))
            except ValueError:
                continue

        logger.info(f"[Liquidity] FRED {series_id}: {len(rows)}건 다운로드")
        return rows

    except Exception as e:
        logger.warning(f"[Liquidity] FRED {series_id} 다운로드 실패: {e}")
        return []


# ═══════════════════════════════════════
#  캐시 관리
# ═══════════════════════════════════════

def _load_cache() -> Optional[Dict]:
    if not CACHE_PATH.exists():
        return None
    try:
        return json.loads(CACHE_PATH.read_text(encoding="utf-8"))
    except Exception:
        return None


def _save_cache(data: Dict):
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    data["fetched_at"] = datetime.now().isoformat()
    CACHE_PATH.write_text(
        json.dumps(data, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _fetch_all_series() -> Dict[str, List[Tuple[str, float]]]:
    """모든 FRED 시리즈 수집 (캐시 우선)

    Returns:
        {"rrp": [(date, val), ...], "tga": [...], "m2": [...]}
    """
    # 캐시 확인
    cached = _load_cache()
    if cached:
        fetched_at = cached.get("fetched_at", "")
        try:
            age = (datetime.now() - datetime.fromisoformat(fetched_at)).days
            if age < CACHE_MAX_AGE_DAYS:
                logger.info(f"[Liquidity] 캐시 사용 (age={age}일)")
                return {
                    k: [(r[0], r[1]) for r in cached.get(k, [])]
                    for k in FRED_SERIES
                }
        except ValueError:
            pass

    # 다운로드
    result = {}
    for key, spec in FRED_SERIES.items():
        rows = _download_fred_csv(spec["series_id"], years_back=2)
        result[key] = rows

    # 캐시 저장
    if any(result.values()):
        cache_data = {k: v for k, v in result.items()}
        _save_cache(cache_data)
    elif cached:
        # 다운로드 실패 시 오래된 캐시 사용
        logger.warning("[Liquidity] 다운로드 실패 → 캐시 사용")
        return {
            k: [(r[0], r[1]) for r in cached.get(k, [])]
            for k in FRED_SERIES
        }

    return result


# ═══════════════════════════════════════
#  분석 계산
# ═══════════════════════════════════════

def _calc_change_rate(values: List[float], window: int) -> float:
    """최근 window일 변화율 (%)"""
    if len(values) < 2:
        return 0.0

    current = values[-1]
    # window에 해당하는 과거값 찾기
    idx = max(0, len(values) - 1 - window)
    past = values[idx]

    if abs(past) < 1e-10:
        return 0.0
    return round((current - past) / abs(past) * 100, 2)


def _calc_z_score_series(change_rates: List[float], current: float) -> float:
    """변화율 시계열 대비 현재값의 z-score"""
    if len(change_rates) < 10:
        return 0.0
    arr = np.array(change_rates[-104:])  # 최대 2년(104주) 사용
    mean = arr.mean()
    std = arr.std()
    if std < 1e-8:
        return 0.0
    return round(float((current - mean) / std), 2)


def _analyze_indicator(
    key: str,
    data: List[Tuple[str, float]],
    spec: Dict,
) -> IndicatorStatus:
    """단일 지표 분석"""
    ind = IndicatorStatus(name=spec["name"])

    if not data or len(data) < 2:
        ind.signal = "데이터 부족"
        return ind

    # 최신 값
    ind.data_date = data[-1][0]
    ind.current_value = data[-1][1]

    values = [v for _, v in data]
    window = spec["change_window"]

    # 변화율
    ind.change_pct = _calc_change_rate(values, window)

    # 히스토리 변화율 시계열 구축 (각 시점에서의 변화율)
    change_series = []
    step = max(1, window // 30)  # 샘플링 간격
    for i in range(window, len(values), step):
        past_val = values[max(0, i - window)]
        curr_val = values[i]
        if abs(past_val) > 1e-10:
            change_series.append((curr_val - past_val) / abs(past_val) * 100)

    # z-score
    raw_z = _calc_z_score_series(change_series, ind.change_pct)

    # 부호 반전 (RRP/TGA: 감소=유동성확장=양수)
    if spec["invert"]:
        ind.z_score = -raw_z
    else:
        ind.z_score = raw_z

    # 가중 z-score
    ind.weighted_z = round(ind.z_score * spec["weight"], 2)

    # 방향 판정
    if ind.z_score > 0.5:
        ind.direction = "EXPANDING"
    elif ind.z_score < -0.5:
        ind.direction = "CONTRACTING"
    else:
        ind.direction = "NEUTRAL"

    # 신호 텍스트
    if key == "m2":
        change_label = f"YoY {ind.change_pct:+.1f}%"
    else:
        change_label = f"30D {ind.change_pct:+.1f}%"

    direction_emoji = {
        "EXPANDING": "🟢",
        "NEUTRAL": "⬜",
        "CONTRACTING": "🔴",
    }
    emoji = direction_emoji.get(ind.direction, "⬜")

    if key == "rrp":
        unit_val = f"{ind.current_value:.1f}B$"
    elif key == "tga":
        unit_val = f"{ind.current_value/1000:.0f}B$"
    elif key == "m2":
        unit_val = f"{ind.current_value/1000:.1f}T$"
    else:
        unit_val = f"{ind.current_value:.1f}"

    ind.signal = f"{emoji} {ind.name}: {unit_val} ({change_label}, z={ind.z_score:+.1f})"

    return ind


# ═══════════════════════════════════════
#  메인 분석
# ═══════════════════════════════════════

def analyze_liquidity() -> LiquidityReport:
    """유동성 사이클 분석

    Returns:
        LiquidityReport (liquidity_score: -10 ~ +10)
    """
    report = LiquidityReport(date=datetime.now().strftime("%Y-%m-%d"))

    # 데이터 수집
    all_data = _fetch_all_series()

    # 개별 지표 분석
    report.rrp = _analyze_indicator("rrp", all_data.get("rrp", []), FRED_SERIES["rrp"])
    report.tga = _analyze_indicator("tga", all_data.get("tga", []), FRED_SERIES["tga"])
    report.m2 = _analyze_indicator("m2", all_data.get("m2", []), FRED_SERIES["m2"])

    # 종합 스코어 = 가중 z-score 합산
    indicators = [report.rrp, report.tga, report.m2]
    raw_score = sum(ind.weighted_z for ind in indicators)

    # -10 ~ +10 클램프 (가중합은 보통 -3~+3 범위이므로 3배 스케일)
    scaled = raw_score * 3.0
    report.liquidity_score = round(max(-10.0, min(10.0, scaled)), 1)

    # 유효 지표 수
    valid_count = sum(1 for ind in indicators if ind.z_score != 0.0)
    report.confidence = round(valid_count / 3.0, 2)

    # 레벨 판정
    s = report.liquidity_score
    if s >= 5.0:
        report.liquidity_level = "LIQUIDITY_EXPANSION"
    elif s >= 2.0:
        report.liquidity_level = "MILD_EXPANSION"
    elif s > -2.0:
        report.liquidity_level = "NEUTRAL"
    elif s > -5.0:
        report.liquidity_level = "MILD_CONTRACTION"
    else:
        report.liquidity_level = "LIQUIDITY_DRAIN"

    # 텔레그램 1줄 요약
    level_labels = {
        "LIQUIDITY_EXPANSION": "유동성 확장 — 위험자산 우호",
        "MILD_EXPANSION": "완만한 확장",
        "NEUTRAL": "중립",
        "MILD_CONTRACTION": "완만한 수축",
        "LIQUIDITY_DRAIN": "유동성 고갈 — 위험자산 비우호",
    }
    level_emojis = {
        "LIQUIDITY_EXPANSION": "🟢",
        "MILD_EXPANSION": "🟢",
        "NEUTRAL": "⬜",
        "MILD_CONTRACTION": "🟡",
        "LIQUIDITY_DRAIN": "🔴",
    }
    emoji = level_emojis.get(report.liquidity_level, "⬜")
    label = level_labels.get(report.liquidity_level, "")
    report.signal = f"{emoji} 유동성 {report.liquidity_score:+.1f}/10 [{report.liquidity_level}] — {label}"

    # BRAIN 배분 조정
    report.allocation_adjust = _calc_allocation_adjust(report)

    # 히스토리 저장 + 추세
    _save_history(report)
    report.score_trend = _calc_score_trend()

    return report


def _calc_allocation_adjust(report: LiquidityReport) -> dict:
    """유동성 수준 기반 BRAIN 배분 조정

    5D는 중장기 시그널이라 조정 폭을 2D/3D보다 작게 가져감.
    방향만 잡아주는 역할.
    """
    level = report.liquidity_level

    if level == "LIQUIDITY_EXPANSION":
        return {
            "swing_shift_pct": 5,      # 스윙 +5%p (공격적)
            "cash_shift_pct": -3,      # 현금 -3%p
            "inverse_shift_pct": -2,   # 인버스 -2%p
            "reason": f"유동성 확장 ({report.liquidity_score:+.1f}) — 위험자산 비중 확대",
        }
    elif level == "MILD_EXPANSION":
        return {
            "swing_shift_pct": 3,
            "cash_shift_pct": -2,
            "inverse_shift_pct": 0,
            "reason": f"완만한 확장 ({report.liquidity_score:+.1f}) — 소폭 공격적",
        }
    elif level == "MILD_CONTRACTION":
        return {
            "swing_shift_pct": -3,
            "cash_shift_pct": 2,
            "inverse_shift_pct": 0,
            "reason": f"완만한 수축 ({report.liquidity_score:+.1f}) — 소폭 방어적",
        }
    elif level == "LIQUIDITY_DRAIN":
        return {
            "swing_shift_pct": -10,
            "cash_shift_pct": 8,
            "inverse_shift_pct": 3,
            "reason": f"유동성 고갈 ({report.liquidity_score:+.1f}) — 방어 최우선",
        }
    return {
        "swing_shift_pct": 0,
        "cash_shift_pct": 0,
        "inverse_shift_pct": 0,
        "reason": "유동성 중립 — 조정 불필요",
    }


# ═══════════════════════════════════════
#  히스토리
# ═══════════════════════════════════════

def _save_history(report: LiquidityReport):
    """유동성 히스토리 저장 (90일 롤링)"""
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    history = {}
    if HISTORY_PATH.exists():
        try:
            history = json.loads(HISTORY_PATH.read_text(encoding="utf-8"))
        except Exception:
            history = {}

    history[report.date] = {
        "liquidity_score": report.liquidity_score,
        "liquidity_level": report.liquidity_level,
        "confidence": report.confidence,
        "rrp_z": report.rrp.z_score,
        "tga_z": report.tga.z_score,
        "m2_z": report.m2.z_score,
        "rrp_value": report.rrp.current_value,
        "tga_value": report.tga.current_value,
        "m2_value": report.m2.current_value,
    }

    # 90일 초과 삭제
    dates = sorted(history.keys())
    while len(dates) > 90:
        del history[dates.pop(0)]

    HISTORY_PATH.write_text(
        json.dumps(history, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _calc_score_trend() -> str:
    """최근 7일 유동성 스코어 추세"""
    if not HISTORY_PATH.exists():
        return "FLAT"
    try:
        history = json.loads(HISTORY_PATH.read_text(encoding="utf-8"))
    except Exception:
        return "FLAT"

    dates = sorted(history.keys())[-7:]
    if len(dates) < 2:
        return "FLAT"

    values = [history[d]["liquidity_score"] for d in dates]
    if values[-1] > values[0] + 1.0:
        return "UP"
    elif values[-1] < values[0] - 1.0:
        return "DOWN"
    return "FLAT"


def load_liquidity_history() -> dict:
    """히스토리 로드 (외부 참조용)"""
    if HISTORY_PATH.exists():
        try:
            return json.loads(HISTORY_PATH.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}


# ═══════════════════════════════════════
#  텔레그램 포맷
# ═══════════════════════════════════════

def format_liquidity_report(report: LiquidityReport) -> str:
    """텔레그램용 유동성 사이클 리포트"""
    level_emoji = {
        "LIQUIDITY_EXPANSION": "🟢",
        "MILD_EXPANSION": "🟢",
        "NEUTRAL": "⬜",
        "MILD_CONTRACTION": "🟡",
        "LIQUIDITY_DRAIN": "🔴",
    }
    emoji = level_emoji.get(report.liquidity_level, "⬜")

    lines = [
        "💧 유동성 사이클 (5D)",
        f"📅 {report.date}",
        "",
        f"{emoji} 유동성 스코어: {report.liquidity_score:+.1f}/10"
        f" [{report.liquidity_level}]",
        f"   확신도: {report.confidence:.0%}"
        f" | 추세: {report.score_trend}",
        "",
        "── 유동성 지표 ──",
        f"  {report.rrp.signal}",
        f"    가중 z: {report.rrp.weighted_z:+.2f}"
        f" (가중치 40%)",
        f"  {report.tga.signal}",
        f"    가중 z: {report.tga.weighted_z:+.2f}"
        f" (가중치 35%)",
        f"  {report.m2.signal}",
        f"    가중 z: {report.m2.weighted_z:+.2f}"
        f" (가중치 25%)",
    ]

    adj = report.allocation_adjust
    if adj.get("reason"):
        lines.append("")
        lines.append(f"💡 {adj['reason']}")
        swing = adj.get("swing_shift_pct", 0)
        cash = adj.get("cash_shift_pct", 0)
        if swing != 0 or cash != 0:
            lines.append(f"   스윙 {swing:+d}%p / 현금 {cash:+d}%p")

    return "\n".join(lines)


# ═══════════════════════════════════════
#  CLI 테스트
# ═══════════════════════════════════════

if __name__ == "__main__":
    import sys
    sys.stdout.reconfigure(encoding="utf-8")
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(message)s",
        datefmt="%H:%M:%S",
    )
    print("유동성 사이클 분석 시작...")
    report = analyze_liquidity()
    print(format_liquidity_report(report))
    print(f"\n배분 조정: {report.allocation_adjust}")
