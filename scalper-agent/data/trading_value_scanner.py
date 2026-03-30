# -*- coding: utf-8 -*-
"""
거래대금 폭발 스캐너 (Trading Value Scanner)
=============================================
"거래량이 아니라 돈의 흐름을 본다."

거래대금 = 종가 x 거래량  (실제 유입된 금액)
거래량만 보면 저가주의 왜곡을 놓치지만,
거래대금은 기관이 실제로 투입한 금액을 반영한다.

3가지 패턴 감지:
  - EXPLOSION:          거래대금 3x+ (급격한 자금 유입)
  - QUIET_ACCUMULATION: 거래대금 2x+ AND 가격 변동 ±3% (조용한 매집)
  - GRADUAL_BUILDUP:    5일간 거래대금 추세 상승 + 1.5x+ (점진적 증가)

Usage:
  python data/trading_value_scanner.py              # 전체 스캔 + 저장
  python data/trading_value_scanner.py --top 20     # TOP 20만 출력
  python data/trading_value_scanner.py --code 005930
  python data/trading_value_scanner.py --pattern QUIET
"""

import json
import logging
import sys
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data_store"
DAILY_DIR = DATA_DIR / "daily"

logger = logging.getLogger("BH.TVScanner")

# 억원 단위 변환
_BILLION = 1e8


# =============================================================================
# 데이터 구조
# =============================================================================

@dataclass
class TVSignal:
    """거래대금 이상 신호"""
    code: str
    name: str
    sector: str
    close: int                    # 현재가
    change_pct: float             # 당일 등락률 (%)
    trading_value: float          # 오늘 거래대금 (억원)
    tv_avg20: float               # 20일 평균 거래대금 (억원)
    tv_ratio: float               # 거래대금 비율 (배수)
    tv_ratio_5d_trend: float      # 5일간 tv_ratio 추세 기울기
    pattern: str                  # EXPLOSION / QUIET_ACCUMULATION / GRADUAL_BUILDUP / NORMAL
    score: float                  # 0 ~ 100 종합 스코어
    detail: str                   # 요약 텍스트


# =============================================================================
# 핵심 계산
# =============================================================================

def _load_daily(code: str) -> Optional[pd.DataFrame]:
    """일봉 CSV 로드"""
    path = DAILY_DIR / f"{code}.csv"
    if not path.exists():
        return None
    try:
        df = pd.read_csv(path, index_col=0, parse_dates=True)
        if len(df) < 21:
            return None
        return df.sort_index()
    except Exception:
        return None


def _calc_tv_metrics(code: str) -> Optional[dict]:
    """단일 종목 거래대금 메트릭 계산

    Returns: {
        close, change_pct, trading_value, tv_avg20, tv_ratio,
        tv_ratio_5d_trend, high, low
    } or None
    """
    daily = _load_daily(code)
    if daily is None:
        return None

    try:
        closes = daily["종가"]
        volumes = daily["거래량"]
        highs = daily["고가"]

        # 거래대금 시리즈
        tv_series = closes * volumes / _BILLION  # 억원

        # 최근값
        latest_tv = float(tv_series.iloc[-1])
        avg_20d_tv = float(tv_series.iloc[-21:-1].mean())  # 오늘 제외 직전 20일 평균 (의도적)

        if avg_20d_tv <= 0:
            return None

        tv_ratio = latest_tv / avg_20d_tv

        # 5일 tv_ratio 추세 (선형 회귀 기울기)
        tv_ratio_5d_trend = 0.0
        if len(tv_series) >= 25:
            recent_5_ratios = []
            for i in range(-5, 0):
                day_tv = float(tv_series.iloc[i])
                day_avg = float(tv_series.iloc[i-20:i].mean())
                if day_avg > 0:
                    recent_5_ratios.append(day_tv / day_avg)
                else:
                    recent_5_ratios.append(1.0)
            if len(recent_5_ratios) == 5:
                x = np.arange(5)
                coeffs = np.polyfit(x, recent_5_ratios, 1)
                tv_ratio_5d_trend = float(coeffs[0])

        # 당일 등락률
        change_pct = float(daily["등락률"].iloc[-1]) if "등락률" in daily.columns else 0.0

        return {
            "close": int(closes.iloc[-1]),
            "change_pct": round(change_pct, 2),
            "trading_value": round(latest_tv, 1),
            "tv_avg20": round(avg_20d_tv, 1),
            "tv_ratio": round(tv_ratio, 3),
            "tv_ratio_5d_trend": round(tv_ratio_5d_trend, 4),
            "high": int(highs.iloc[-1]),
        }
    except Exception as e:
        logger.debug(f"[{code}] 메트릭 계산 실패: {e}")
        return None


def _classify_pattern(tv_ratio: float, change_pct: float, tv_5d_trend: float) -> str:
    """패턴 분류

    시그널 지연 개선(v2): EARLY_ACCUMULATION 패턴 추가
    기존: 2.0x 이상에서 QUIET_ACCUMULATION 감지 → 이미 2일째
    개선: 1.3x~2.0x + 가격안정 → EARLY_ACCUMULATION (1일 선제 감지)

    타임라인 비교:
      기존: Day0 1.3x(NORMAL) → Day1 2.0x(QA감지) → Day2 아침 알림 → 이미 늦음
      개선: Day0 1.3x(EA감지!) → Day1 아침 알림 → 1일 먼저 포착
    """
    abs_change = abs(change_pct)

    # EXPLOSION: 거래대금 급증 + 가격도 크게 변동 (먼저 체크하여 QUIET_ACC와 겹침 방지)
    if tv_ratio >= 3.0 and abs_change > 3.0:
        return "EXPLOSION"

    # QUIET_ACCUMULATION: 돈은 들어오는데 가격은 안 움직임 (가장 강한 시그널)
    if tv_ratio >= 2.0 and abs_change <= 3.0:
        return "QUIET_ACCUMULATION"

    # EXPLOSION(순수): 거래대금 급증이지만 가격 미분류 (위에서 안 걸린 나머지)
    if tv_ratio >= 3.0:
        return "EXPLOSION"

    # EARLY_ACCUMULATION: 초기 매집 감지 — 1일 선제 포착
    # 거래대금 1.3~2.0x + 가격 ±2% 이내 = 누군가 조용히 모으기 시작
    if tv_ratio >= 1.3 and abs_change <= 2.0:
        return "EARLY_ACCUMULATION"

    # GRADUAL_BUILDUP: 서서히 증가 (5일 추세)
    if tv_5d_trend > 0.15 and tv_ratio >= 1.5:
        return "GRADUAL_BUILDUP"

    return "NORMAL"


def _score_signal(tv_ratio: float, change_pct: float, tv_5d_trend: float,
                  trading_value: float, close: int, high: int) -> float:
    """5팩터 스코어링 (0~100)

    (A) 거래대금 배수       max 30
    (B) 가격 안정성         max 25
    (C) 추세 지속성         max 20
    (D) 거래대금 절대값     max 15
    (E) 종가 위치           max 10
    """
    score = 0.0

    # (A) 거래대금 배수
    if tv_ratio >= 5.0:
        score += 30
    elif tv_ratio >= 3.0:
        score += 25
    elif tv_ratio >= 2.0:
        score += 20
    elif tv_ratio >= 1.5:
        score += 10

    # (B) 가격 안정성 (|등락률|이 작을수록 좋음 = 조용한 매집)
    abs_change = abs(change_pct)
    if abs_change <= 1.0:
        score += 25
    elif abs_change <= 2.0:
        score += 20
    elif abs_change <= 3.0:
        score += 15
    elif abs_change <= 5.0:
        score += 5

    # (C) 추세 지속성 (5일간 tv_ratio 기울기)
    if tv_5d_trend >= 0.30:
        score += 20
    elif tv_5d_trend >= 0.15:
        score += 10
    elif tv_5d_trend >= 0.05:
        score += 5

    # (D) 거래대금 절대값 (억원)
    if trading_value >= 100:
        score += 15
    elif trading_value >= 50:
        score += 10
    elif trading_value >= 20:
        score += 5

    # (E) 종가 위치 (고가 대비 종가 비율 — 상한 마감일수록 강세)
    if high > 0:
        close_high_ratio = close / high
        if close_high_ratio >= 0.97:
            score += 10
        elif close_high_ratio >= 0.93:
            score += 7
        elif close_high_ratio >= 0.90:
            score += 5

    return round(score, 1)


# =============================================================================
# 메인 스캔 함수
# =============================================================================

def scan_trading_value(
    universe: dict,
    min_tv_billion: float = 10.0,
) -> list[TVSignal]:
    """유니버스 전체 거래대금 스캔

    Args:
        universe: {code: {name, sector, cap_억, ...}}
        min_tv_billion: 최소 거래대금 (억원) — 잡주 필터

    Returns:
        TVSignal 리스트 (score 내림차순, NORMAL 제외)
    """
    signals = []
    scanned = 0
    skipped = 0

    codes = list(universe.keys())
    t0 = time.time()

    for code in codes:
        info = universe.get(code, {})
        name = info.get("name", code)
        sector = info.get("sector", "")

        metrics = _calc_tv_metrics(code)
        if metrics is None:
            skipped += 1
            continue

        scanned += 1

        # 최소 거래대금 필터
        if metrics["trading_value"] < min_tv_billion:
            continue

        pattern = _classify_pattern(
            metrics["tv_ratio"],
            metrics["change_pct"],
            metrics["tv_ratio_5d_trend"],
        )

        if pattern == "NORMAL":
            continue

        score = _score_signal(
            tv_ratio=metrics["tv_ratio"],
            change_pct=metrics["change_pct"],
            tv_5d_trend=metrics["tv_ratio_5d_trend"],
            trading_value=metrics["trading_value"],
            close=metrics["close"],
            high=metrics["high"],
        )

        # 상세 텍스트 생성
        parts = [f"거래대금 {metrics['tv_ratio']:.1f}x"]
        if abs(metrics["change_pct"]) <= 3.0:
            parts.append(f"가격보합 {metrics['change_pct']:+.1f}%")
        else:
            parts.append(f"등락 {metrics['change_pct']:+.1f}%")
        if metrics["tv_ratio_5d_trend"] > 0.05:
            parts.append(f"5일추세 +{metrics['tv_ratio_5d_trend']:.2f}")
        parts.append(f"= {pattern}")
        detail = " + ".join(parts)

        signals.append(TVSignal(
            code=code,
            name=name,
            sector=sector,
            close=metrics["close"],
            change_pct=metrics["change_pct"],
            trading_value=metrics["trading_value"],
            tv_avg20=metrics["tv_avg20"],
            tv_ratio=metrics["tv_ratio"],
            tv_ratio_5d_trend=metrics["tv_ratio_5d_trend"],
            pattern=pattern,
            score=score,
            detail=detail,
        ))

    signals.sort(key=lambda s: s.score, reverse=True)

    elapsed = time.time() - t0
    logger.info(
        f"[TV Scanner] {scanned}종목 스캔 ({skipped} 스킵) "
        f"→ {len(signals)}개 이상신호 감지 ({elapsed:.1f}s)"
    )

    return signals


# =============================================================================
# 저장 / 로드
# =============================================================================

def save_tv_results(signals: list[TVSignal], path: Path = None):
    """TV 스캐너 결과 저장"""
    if path is None:
        path = DATA_DIR / "tv_scanner.json"

    from datetime import datetime
    now = datetime.now()

    data = {
        "scan_date": now.strftime("%Y-%m-%d"),
        "scan_time": now.strftime("%H:%M"),
        "total_signals": len(signals),
        "patterns": {
            "EXPLOSION": sum(1 for s in signals if s.pattern == "EXPLOSION"),
            "QUIET_ACCUMULATION": sum(1 for s in signals if s.pattern == "QUIET_ACCUMULATION"),
            "EARLY_ACCUMULATION": sum(1 for s in signals if s.pattern == "EARLY_ACCUMULATION"),
            "GRADUAL_BUILDUP": sum(1 for s in signals if s.pattern == "GRADUAL_BUILDUP"),
        },
        "signals": [asdict(s) for s in signals],
    }

    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info(f"[TV Scanner] 저장 완료: {path} ({len(signals)}건)")

    # ── 히스토리 아카이빙 (최근 7일 유지) ──
    try:
        history_dir = DATA_DIR / "tv_history"
        history_dir.mkdir(parents=True, exist_ok=True)
        hist_path = history_dir / f"{now.strftime('%Y-%m-%d')}.json"
        hist_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        # 7일 초과 파일 정리
        cutoff = now - timedelta(days=7)
        for old in sorted(history_dir.glob("*.json")):
            try:
                fdate = datetime.strptime(old.stem, "%Y-%m-%d")
                if fdate < cutoff:
                    old.unlink()
            except (ValueError, OSError):
                pass
    except Exception as e:
        logger.warning(f"[TV Scanner] 히스토리 아카이빙 실패: {e}")


def load_tv_results(path: Path = None) -> list[dict]:
    """저장된 TV 스캐너 결과 로드"""
    if path is None:
        path = DATA_DIR / "tv_scanner.json"

    if not path.exists():
        return []

    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data.get("signals", [])
    except Exception as e:
        logger.warning(f"TV 스캐너 결과 로드 실패: {e}")
        return []


# =============================================================================
# TV 시그널 잔존 효과 (Signal Persistence)
# =============================================================================

# Decay 테이블: (pattern, min_score) → {days_ago: bonus}
_PERSISTENCE_TABLE = {
    ("EXPLOSION", 80):           {1: 15, 2: 10, 3: 5},
    ("EXPLOSION", 70):           {1: 10, 2: 5,  3: 2},
    ("QUIET_ACCUMULATION", 80):  {1: 18, 2: 12, 3: 5},
    ("QUIET_ACCUMULATION", 70):  {1: 12, 2: 6,  3: 3},
    ("GRADUAL_BUILDUP", 70):     {1: 8,  2: 4,  3: 2},
    ("EARLY_ACCUMULATION", 50):  {1: 10, 2: 5,  3: 2},  # 초기 매집 → 다음날 관찰 강화
}


def calc_tv_persistence(today_codes: set = None, max_lookback: int = 3) -> dict:
    """T-1 ~ T-N 히스토리에서 잔존 시그널 점수 계산

    Args:
        today_codes: 오늘 이미 감지된 종목 (중복 방지). None이면 자동 로드.
        max_lookback: 최대 며칠 전까지 조회 (기본 3)

    Returns: {code: {
        "days_ago": int, "peak_score": float, "peak_pattern": str,
        "peak_ratio": float, "change_pct": float,
        "persistence_score": float, "consecutive": int,
    }}
    """
    history_dir = DATA_DIR / "tv_history"
    if not history_dir.exists():
        return {}

    # 오늘 감지 종목 로드
    if today_codes is None:
        today_codes = set()
        today_signals = load_tv_results()
        for s in today_signals:
            today_codes.add(s.get("code", ""))

    # 히스토리 파일 로드 (최신순)
    hist_files = sorted(history_dir.glob("*.json"), reverse=True)

    # 오늘 파일 제외
    today = datetime.now().date()
    today_str = today.strftime("%Y-%m-%d")
    hist_files = [f for f in hist_files if f.stem != today_str]

    if not hist_files:
        return {}

    # 실제 날짜 차이로 days_ago 계산 (파일 갭/주말 대응)
    code_history = {}  # {code: [(days_ago, signal_dict), ...]}
    for hf in hist_files:
        try:
            file_date = datetime.strptime(hf.stem, "%Y-%m-%d").date()
            days_ago = (today - file_date).days
            if days_ago < 1 or days_ago > max_lookback:
                continue
            data = json.loads(hf.read_text(encoding="utf-8"))
            for sig in data.get("signals", []):
                code = sig.get("code", "")
                if not code or code in today_codes:
                    continue
                if code not in code_history:
                    code_history[code] = []
                code_history[code].append((days_ago, sig))
        except Exception:
            continue

    # 잔존 점수 계산
    result = {}
    for code, entries in code_history.items():
        # 가장 강한 시그널 찾기 (peak)
        best_entry = max(entries, key=lambda e: e[1].get("score", 0))
        days_ago, peak_sig = best_entry

        peak_score = peak_sig.get("score", 0)
        peak_pattern = peak_sig.get("pattern", "NORMAL")
        peak_ratio = peak_sig.get("tv_ratio", 0)
        change_pct = peak_sig.get("change_pct", 0)
        name = peak_sig.get("name", code)

        # Decay 테이블 매칭
        persistence_score = 0.0
        for (pat, min_sc), decay_map in _PERSISTENCE_TABLE.items():
            if peak_pattern == pat and peak_score >= min_sc:
                persistence_score = decay_map.get(days_ago, 0)
                break

        if persistence_score <= 0:
            continue

        # 연속 감지 일수
        consecutive = len(set(d for d, _ in entries))

        result[code] = {
            "days_ago": days_ago,
            "name": name,
            "peak_score": peak_score,
            "peak_pattern": peak_pattern,
            "peak_ratio": peak_ratio,
            "change_pct": change_pct,
            "persistence_score": persistence_score,
            "consecutive": consecutive,
        }

    logger.info(f"[TV Persistence] {len(result)}종목 잔존 시그널 감지")
    return result


# =============================================================================
# 장중 실시간 모드 (Intraday)
# =============================================================================

@dataclass
class IntradayTVSignal:
    """장중 실시간 거래대금 신호"""
    code: str
    name: str
    sector: str
    current_price: int
    change_pct: float
    intraday_tv: float            # 현재까지 누적 거래대금 (억)
    estimated_daily_tv: float     # 하루 예상 거래대금 (억)
    avg_20d_tv: float             # 20일 평균 거래대금 (억)
    estimated_tv_ratio: float     # 예상 TV 비율
    pattern: str                  # EXPLOSION / QUIET_ACCUMULATION / ...
    acceleration: float           # 최근 30분 가속도
    confidence: str               # VERY_LOW / LOW / MEDIUM / HIGH
    score: float                  # 0~100
    detail: str


class IntradayHistory:
    """장중 스캔 스냅샷 히스토리 — 가속도 계산용

    매 스캔마다 record()를 호출하면, 최근 30분 vs 이전 30분
    거래대금 증분을 비교하여 가속도를 산출한다.
    """

    def __init__(self, max_snapshots: int = 120):
        self._data: dict = {}  # {code: [(timestamp, cum_volume, price), ...]}
        self._max = max_snapshots

    def record(self, code: str, cumulative_volume: int, price: int):
        if code not in self._data:
            self._data[code] = []
        self._data[code].append((datetime.now(), cumulative_volume, price))
        if len(self._data[code]) > self._max:
            self._data[code] = self._data[code][-self._max:]

    def get_acceleration(self, code: str, window_min: int = 30) -> float:
        """최근 window_min 거래대금 / 이전 window_min 거래대금

        Returns: >1.0 가속, <1.0 감속, 데이터 부족 시 1.0
        """
        snapshots = self._data.get(code, [])
        if len(snapshots) < 3:
            return 1.0

        now = datetime.now()
        recent_cutoff = now - timedelta(minutes=window_min)
        prev_cutoff = recent_cutoff - timedelta(minutes=window_min)

        recent_tv = 0.0
        prev_tv = 0.0

        sorted_snaps = sorted(snapshots, key=lambda x: x[0])
        for i in range(1, len(sorted_snaps)):
            ts, vol, price = sorted_snaps[i]
            _, prev_vol, prev_price = sorted_snaps[i - 1]

            delta_vol = max(0, vol - prev_vol)
            avg_price = (price + prev_price) / 2 if prev_price > 0 else price
            segment_tv = avg_price * delta_vol / _BILLION

            if ts >= recent_cutoff:
                recent_tv += segment_tv
            elif ts >= prev_cutoff:
                prev_tv += segment_tv

        if prev_tv <= 0:
            return 1.0 if recent_tv <= 0 else 2.0

        return round(recent_tv / prev_tv, 2)

    def clear_day(self):
        """일변경 시 초기화"""
        self._data.clear()


# ── 시간 보정 ──

# 30분 단위 거래량 가중치 (한국 시장 경험적 분포, 총합 ≈ 1.0)
_VOLUME_WEIGHT_SLOTS = [
    ((9, 0),   0.12),   # 09:00~09:30 개장 충격
    ((9, 30),  0.09),   # 09:30~10:00
    ((10, 0),  0.08),   # 10:00~10:30
    ((10, 30), 0.07),   # 10:30~11:00
    ((11, 0),  0.06),   # 11:00~11:30
    ((11, 30), 0.04),   # 11:30~12:00 점심
    ((12, 0),  0.04),   # 12:00~12:30 점심
    ((12, 30), 0.06),   # 12:30~13:00
    ((13, 0),  0.07),   # 13:00~13:30
    ((13, 30), 0.07),   # 13:30~14:00
    ((14, 0),  0.08),   # 14:00~14:30
    ((14, 30), 0.10),   # 14:30~15:00 마감 접근
    ((15, 0),  0.12),   # 15:00~15:30 마감
]


def _calc_elapsed_ratio(now: datetime = None) -> float:
    """장 시작(09:00)부터 현재까지 거래량 가중 경과 비율

    단순 시간비율이 아닌 시간대별 거래량 가중치 적용:
    - 09:00~09:30: 높음 (개장 충격)
    - 11:30~12:30: 낮음 (점심)
    - 14:30~15:30: 높음 (마감)

    Returns: 0.01 ~ 1.0
    """
    if now is None:
        now = datetime.now()

    market_open = now.replace(hour=9, minute=0, second=0, microsecond=0)
    market_close = now.replace(hour=15, minute=30, second=0, microsecond=0)

    if now <= market_open:
        return 0.01
    if now >= market_close:
        return 1.0

    elapsed_weight = 0.0
    for (h, m), weight in _VOLUME_WEIGHT_SLOTS:
        slot_start = now.replace(hour=h, minute=m, second=0, microsecond=0)
        slot_end = slot_start + timedelta(minutes=30)

        if now >= slot_end:
            elapsed_weight += weight
        elif now > slot_start:
            slot_elapsed = (now - slot_start).total_seconds()
            elapsed_weight += weight * (slot_elapsed / 1800)

    return min(1.0, max(0.01, round(elapsed_weight, 4)))


def _get_confidence(now: datetime = None) -> str:
    """현재 시간 기반 장중 데이터 신뢰도"""
    if now is None:
        now = datetime.now()
    t = now.hour * 60 + now.minute
    if t < 600:        # ~10:00
        return "VERY_LOW"
    elif t < 690:      # ~11:30
        return "LOW"
    elif t < 780:      # ~13:00
        return "MEDIUM"
    else:
        return "HIGH"


def precompute_tv_averages(universe: dict, min_avg_tv: float = 5.0) -> dict:
    """전 종목 20일 평균 거래대금 사전 계산

    Returns: {code: {"avg_tv": float, "name": str, "sector": str}}
    """
    avgs = {}
    for code, info in universe.items():
        daily = _load_daily(code)
        if daily is None or len(daily) < 21:
            continue
        try:
            tv_series = daily["종가"] * daily["거래량"] / _BILLION
            avg_20 = float(tv_series.iloc[-21:-1].mean())
            if avg_20 >= min_avg_tv:
                avgs[code] = {
                    "avg_tv": round(avg_20, 1),
                    "name": info.get("name", code),
                    "sector": info.get("sector", ""),
                }
        except Exception:
            continue
    logger.info(f"[TV] 20일 평균 거래대금 사전계산: {len(avgs)}종목 (>={min_avg_tv}억)")
    return avgs


def scan_intraday_stock(
    code: str,
    name: str,
    sector: str,
    current_price: int,
    volume: int,
    change_pct: float,
    high: int,
    avg_20d_tv: float,
    acceleration: float = 1.0,
) -> Optional[IntradayTVSignal]:
    """단일 종목 장중 TV 분석

    KIS API fetch_price() 결과를 받아서 분석.
    trader 의존성 없이 순수 계산만 수행.
    """
    if current_price <= 0 or volume <= 0 or avg_20d_tv <= 0:
        return None

    # 1. 누적 거래대금 (억원)
    intraday_tv = current_price * volume / _BILLION

    # 2. 시간 보정 → 예상 일거래대금
    elapsed = _calc_elapsed_ratio()
    if elapsed < 0.05:
        return None  # 09:00~09:03 — 데이터 부족
    estimated_daily_tv = intraday_tv / elapsed

    # 3. TV 비율
    estimated_tv_ratio = estimated_daily_tv / avg_20d_tv

    # 4. 패턴 분류 (기존 로직 재사용)
    pattern = _classify_pattern(estimated_tv_ratio, change_pct, 0.0)

    confidence = _get_confidence()

    if pattern == "NORMAL":
        # 가속도가 높으면 GRADUAL_BUILDUP 재분류
        if acceleration >= 1.5 and estimated_tv_ratio >= 1.3:
            pattern = "GRADUAL_BUILDUP"
        else:
            return None

    # 5. 스코어링 (기존 5팩터 + 가속도 보너스)
    base_score = _score_signal(
        tv_ratio=estimated_tv_ratio,
        change_pct=change_pct,
        tv_5d_trend=0.0,
        trading_value=estimated_daily_tv,
        close=current_price,
        high=high,
    )

    accel_bonus = 0.0
    if acceleration >= 2.0:
        accel_bonus = 10.0
    elif acceleration >= 1.5:
        accel_bonus = 5.0
    elif acceleration >= 1.2:
        accel_bonus = 2.0

    score = min(100, base_score + accel_bonus)

    # 6. 상세 텍스트
    parts = [f"장중TV {intraday_tv:.0f}억→예상{estimated_daily_tv:.0f}억"]
    parts.append(f"비율 {estimated_tv_ratio:.1f}x")
    if acceleration > 1.1:
        parts.append(f"가속 {acceleration:.1f}x")
    parts.append(f"[{confidence}]")
    parts.append(f"= {pattern}")

    return IntradayTVSignal(
        code=code,
        name=name,
        sector=sector,
        current_price=current_price,
        change_pct=round(change_pct, 2),
        intraday_tv=round(intraday_tv, 1),
        estimated_daily_tv=round(estimated_daily_tv, 1),
        avg_20d_tv=round(avg_20d_tv, 1),
        estimated_tv_ratio=round(estimated_tv_ratio, 3),
        pattern=pattern,
        acceleration=acceleration,
        confidence=confidence,
        score=score,
        detail=" + ".join(parts),
    )


def scan_intraday_universe(
    trader,
    tv_avgs: dict,
    history: IntradayHistory = None,
    prev_alerted: set = None,
    max_scan: int = 500,
    min_score: float = 50.0,
) -> list:
    """유니버스 전체 장중 TV 스캔

    Args:
        trader: KISTrader 인스턴스
        tv_avgs: precompute_tv_averages() 결과
        history: IntradayHistory 인스턴스 (가속도 계산용)
        prev_alerted: 이미 알림 보낸 종목 코드 세트 (스킵용)
        max_scan: 최대 스캔 종목 수
        min_score: 최소 스코어

    Returns:
        IntradayTVSignal 리스트 (score 내림차순)
    """
    if prev_alerted is None:
        prev_alerted = set()

    signals = []
    scanned = 0

    # avg TV 높은 순 (유동성 큰 종목 우선)
    candidates = sorted(
        tv_avgs.items(),
        key=lambda x: x[1]["avg_tv"],
        reverse=True,
    )[:max_scan]

    t0 = time.time()

    for code, info in candidates:
        try:
            p = trader.fetch_price(code)
            if not p or not p.get("success"):
                continue

            current_price = p["current_price"]
            volume = p.get("volume", 0)
            if current_price <= 0 or volume <= 0:
                continue

            scanned += 1

            # 히스토리 기록 (가속도 계산용)
            if history:
                history.record(code, volume, current_price)

            accel = history.get_acceleration(code) if history else 1.0

            sig = scan_intraday_stock(
                code=code,
                name=info["name"],
                sector=info.get("sector", ""),
                current_price=current_price,
                volume=volume,
                change_pct=p.get("change_rate", 0),
                high=p.get("high", current_price),
                avg_20d_tv=info["avg_tv"],
                acceleration=accel,
            )

            if sig and sig.score >= min_score:
                if code not in prev_alerted:
                    signals.append(sig)

            time.sleep(0.05)  # KIS API rate limit

        except Exception:
            continue

    signals.sort(key=lambda s: s.score, reverse=True)

    elapsed = time.time() - t0
    conf = _get_confidence()
    logger.info(
        f"[TV Intraday] {scanned}종목 스캔 → {len(signals)}개 신호 "
        f"({elapsed:.1f}s, {conf})"
    )

    return signals


def save_intraday_results(signals: list, path: Path = None):
    """장중 TV 스캔 결과 저장"""
    if path is None:
        path = DATA_DIR / "tv_intraday.json"

    data = {
        "scan_time": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "confidence": _get_confidence(),
        "elapsed_ratio": round(_calc_elapsed_ratio(), 3),
        "total_signals": len(signals),
        "signals": [asdict(s) for s in signals],
    }

    path.write_text(
        json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    logger.info(f"[TV Intraday] 저장: {path} ({len(signals)}건)")


def load_intraday_results(path: Path = None) -> list:
    """저장된 장중 TV 스캔 결과 로드"""
    if path is None:
        path = DATA_DIR / "tv_intraday.json"

    if not path.exists():
        return []

    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data.get("signals", [])
    except Exception as e:
        logger.warning(f"장중 TV 결과 로드 실패: {e}")
        return []


# =============================================================================
# CLI
# =============================================================================

def _print_signals(signals: list[TVSignal], top: int = 0, pattern_filter: str = None):
    """CLI 출력"""
    filtered = signals
    if pattern_filter:
        p = pattern_filter.upper()
        filtered = [s for s in signals if p in s.pattern]

    if top > 0:
        filtered = filtered[:top]

    # 패턴별 이모지
    _PATTERN_ICON = {
        "EXPLOSION": "BOOM",
        "QUIET_ACCUMULATION": "SILENT",
        "EARLY_ACCUMULATION": "EARLY",
        "GRADUAL_BUILDUP": "TREND",
    }

    print(f"\n{'='*78}")
    print(f"  TRADING VALUE SCANNER — 거래대금 폭발 스캐너")
    print(f"  {len(signals)}개 이상신호 감지")
    print(f"{'='*78}")

    for i, s in enumerate(filtered, 1):
        icon = _PATTERN_ICON.get(s.pattern, "")
        bar = "#" * int(min(s.tv_ratio, 8) * 4)
        print(
            f"\n  #{i:2d} [{icon:7s}] {s.name}({s.code}) — {s.sector}"
            f"\n      Score: {s.score:.0f}점 | "
            f"거래대금 {s.trading_value:,.0f}억 (20일평균 {s.tv_avg20:,.0f}억)"
            f"\n      TV비율: x{s.tv_ratio:.2f} {bar} | "
            f"등락: {s.change_pct:+.1f}% | "
            f"5일추세: {s.tv_ratio_5d_trend:+.3f}"
            f"\n      현재가: {s.close:,}원 | {s.detail}"
        )

    print(f"\n{'='*78}")
    print(f"  패턴 분포:")
    for p in ["QUIET_ACCUMULATION", "EARLY_ACCUMULATION", "EXPLOSION", "GRADUAL_BUILDUP"]:
        cnt = sum(1 for s in signals if s.pattern == p)
        if cnt > 0:
            print(f"    {p}: {cnt}개")
    print(f"{'='*78}\n")


def _print_single(code: str, universe: dict):
    """단일 종목 상세 분석"""
    info = universe.get(code, {})
    name = info.get("name", code)

    daily = _load_daily(code)
    if daily is None:
        print(f"  {name}({code}): 데이터 없음")
        return

    closes = daily["종가"]
    volumes = daily["거래량"]
    tv_series = closes * volumes / _BILLION

    print(f"\n{'='*65}")
    print(f"  {name}({code}) — 거래대금 상세 분석")
    print(f"{'='*65}")

    # 최근 20일 추이
    print(f"\n  최근 20일 거래대금 추이:")
    recent = daily.tail(20)
    for idx, row in recent.iterrows():
        date_str = str(idx)[:10]
        tv = float(row["종가"]) * float(row["거래량"]) / _BILLION
        avg_20 = float(tv_series.loc[:idx].iloc[-21:-1].mean()) if len(tv_series.loc[:idx]) > 21 else tv
        ratio = tv / avg_20 if avg_20 > 0 else 1.0
        bar = "#" * int(min(ratio, 6) * 5)
        flag = " <<<" if ratio >= 3.0 else ("  **" if ratio >= 2.0 else ("   *" if ratio >= 1.5 else ""))
        print(f"    {date_str} | {row['종가']:>8,} | {tv:>7,.0f}억 | x{ratio:.2f} {bar}{flag}")

    # 메트릭
    metrics = _calc_tv_metrics(code)
    if metrics:
        pattern = _classify_pattern(metrics["tv_ratio"], metrics["change_pct"], metrics["tv_ratio_5d_trend"])
        score = _score_signal(
            metrics["tv_ratio"], metrics["change_pct"],
            metrics["tv_ratio_5d_trend"], metrics["trading_value"],
            metrics["close"], metrics["high"],
        )
        print(f"\n  최종: tv_ratio=x{metrics['tv_ratio']:.2f} | "
              f"5일추세={metrics['tv_ratio_5d_trend']:+.3f} | "
              f"패턴={pattern} | 점수={score:.0f}")

    print(f"{'='*65}\n")


if __name__ == "__main__":
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )

    parser = argparse.ArgumentParser(description="거래대금 폭발 스캐너")
    parser.add_argument("--top", type=int, default=0, help="상위 N개만 출력")
    parser.add_argument("--code", type=str, default=None, help="단일 종목 상세 분석")
    parser.add_argument("--pattern", type=str, default=None, help="패턴 필터 (QUIET/EXPLOSION/GRADUAL)")
    parser.add_argument("--min-tv", type=float, default=10.0, help="최소 거래대금 (억원)")
    args = parser.parse_args()

    # universe 로드
    uni_path = DATA_DIR / "universe.json"
    with open(uni_path, "r", encoding="utf-8") as f:
        universe = json.load(f)

    if args.code:
        _print_single(args.code, universe)
    else:
        signals = scan_trading_value(universe, min_tv_billion=args.min_tv)
        save_tv_results(signals)
        _print_signals(signals, top=args.top, pattern_filter=args.pattern)
