# -*- coding: utf-8 -*-
"""
Macro Baseline — 매크로 지표 기준선 모듈
==========================================
주요 매크로 지표의 20일/60일 이동평균을 계산하여 "현재 수준"이
역사적으로 어디에 위치하는지 판단.

왜 필요한가:
  전일비 +5%만 보면 "오늘 좀 올랐네"
  60일 기준 +39%면 "3달째 폭등 중"  ← 완전히 다른 판단

6개 핵심 지표:
  CL=F       유가(WTI)        → 인플레/에너지 비용
  ^TNX       미국 10년물 금리  → 채권/금리 환경
  GC=F       금               → 안전자산/인플레 헤지
  HG=F       구리             → 경기 선행지표
  USDKRW=X   원/달러           → 수출/수입 영향
  ^VIX       공포지수          → 리스크 수준

캐싱:
  - data_store/macro_baseline.json에 저장 (1일 1회 갱신)
  - timestamp 기반 staleness 체크
  - 실패 시 캐시 사용, 캐시도 없으면 graceful fallback

원칙:
  - yfinance로 120일 히스토리 수집 → 20MA/60MA 계산
  - 모듈 오류가 매매를 차단하는 일은 절대 없음
"""

import json
import logging
from dataclasses import dataclass, field, asdict
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Dict, Optional, Tuple

logger = logging.getLogger("BH.MacroBaseline")

DATA_DIR = Path(__file__).resolve().parent.parent / "data_store"
CACHE_PATH = DATA_DIR / "macro_baseline.json"


# ═══════════════════════════════════════════
#  데이터 구조
# ═══════════════════════════════════════════

@dataclass
class IndicatorBaseline:
    """개별 지표의 기준선 정보"""
    ticker: str = ""
    name: str = ""
    current: float = 0.0           # 최신 종가
    ma20: float = 0.0              # 20일 이동평균
    ma60: float = 0.0              # 60일 이동평균
    dev20_pct: float = 0.0         # 20MA 이탈도 (%)
    dev60_pct: float = 0.0         # 60MA 이탈도 (%)
    pct_rank_60d: float = 50.0     # 60일 내 백분위 (0=최저, 100=최고)
    trend_20d: str = "보합"        # 급상승 / 상승 / 보합 / 하락 / 급하락
    data_days: int = 0             # 수집된 영업일 수


@dataclass
class MacroBaselines:
    """전체 매크로 기준선"""
    timestamp: str = ""
    oil: IndicatorBaseline = field(default_factory=IndicatorBaseline)
    tnx: IndicatorBaseline = field(default_factory=IndicatorBaseline)
    gold: IndicatorBaseline = field(default_factory=IndicatorBaseline)
    copper: IndicatorBaseline = field(default_factory=IndicatorBaseline)
    usdkrw: IndicatorBaseline = field(default_factory=IndicatorBaseline)
    vix: IndicatorBaseline = field(default_factory=IndicatorBaseline)


# ═══════════════════════════════════════════
#  지표 정의
# ═══════════════════════════════════════════

INDICATOR_DEFS = {
    "oil":    {"ticker": "CL=F",      "name": "유가(WTI)"},
    "tnx":    {"ticker": "^TNX",      "name": "US10Y금리"},
    "gold":   {"ticker": "GC=F",      "name": "금"},
    "copper": {"ticker": "HG=F",      "name": "구리"},
    "usdkrw": {"ticker": "USDKRW=X",  "name": "원달러"},
    "vix":    {"ticker": "^VIX",      "name": "VIX"},
}


# ═══════════════════════════════════════════
#  기준선 계산
# ═══════════════════════════════════════════

def _calc_one_baseline(ticker: str, name: str) -> IndicatorBaseline:
    """yfinance로 단일 지표 120일 히스토리 → 기준선 계산."""
    import yfinance as yf
    import pandas as pd

    bl = IndicatorBaseline(ticker=ticker, name=name)

    end = date.today()
    start = end - timedelta(days=150)  # 영업일 60일 + 여유

    data = yf.download(
        ticker,
        start=start.strftime("%Y-%m-%d"),
        end=end.strftime("%Y-%m-%d"),
        progress=False,
    )

    if data.empty:
        logger.warning(f"[Baseline] {name}({ticker}): 데이터 없음")
        return bl

    # yfinance MultiIndex 컬럼 처리
    if isinstance(data.columns, pd.MultiIndex):
        data.columns = data.columns.droplevel("Ticker")

    closes = data["Close"].dropna()
    if len(closes) < 5:
        return bl

    # 현재가
    bl.current = round(float(closes.iloc[-1]), 4)
    bl.data_days = len(closes)

    # 20일 이동평균
    if len(closes) >= 20:
        bl.ma20 = round(closes.tail(20).mean().item(), 4)
        if bl.ma20 > 0:
            bl.dev20_pct = round((bl.current - bl.ma20) / bl.ma20 * 100, 2)
    else:
        bl.ma20 = bl.current

    # 60일 이동평균
    if len(closes) >= 60:
        bl.ma60 = round(closes.tail(60).mean().item(), 4)
        if bl.ma60 > 0:
            bl.dev60_pct = round((bl.current - bl.ma60) / bl.ma60 * 100, 2)
    else:
        bl.ma60 = bl.ma20

    # 60일 백분위 (현재가가 60일 범위에서 어디 위치?)
    recent = closes.tail(min(60, len(closes)))
    below = sum(1 for v in recent.values if float(v) <= bl.current)
    bl.pct_rank_60d = round(below / len(recent) * 100, 1)

    # 20일 추세 (20MA 기울기)
    if len(closes) >= 25:
        ma20_now = closes.tail(20).mean().item()
        ma20_5ago = closes.iloc[-25:-5].tail(20).mean().item()
        if ma20_5ago > 0:
            slope_pct = (ma20_now - ma20_5ago) / ma20_5ago * 100
            if slope_pct > 3.0:
                bl.trend_20d = "급상승"
            elif slope_pct > 1.0:
                bl.trend_20d = "상승"
            elif slope_pct < -3.0:
                bl.trend_20d = "급하락"
            elif slope_pct < -1.0:
                bl.trend_20d = "하락"
            else:
                bl.trend_20d = "보합"

    return bl


def fetch_all_baselines(force: bool = False) -> MacroBaselines:
    """
    6개 매크로 지표 기준선 일괄 수집.

    캐시 로직:
      - force=True → 강제 갱신
      - 오늘 이미 수집했으면 캐시 반환
      - 캐시 없거나 오래됐으면 수집

    Returns:
        MacroBaselines
    """
    # 캐시 확인
    if not force:
        cached = _load_cache()
        if cached:
            return cached

    logger.info("[Baseline] 매크로 기준선 수집 시작 (yfinance)...")
    result = MacroBaselines(
        timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    )

    for key, defn in INDICATOR_DEFS.items():
        try:
            bl = _calc_one_baseline(defn["ticker"], defn["name"])
            setattr(result, key, bl)
            logger.info(
                f"  {bl.name:10s}: 현재={bl.current:,.2f} "
                f"20MA={bl.ma20:,.2f}({bl.dev20_pct:+.1f}%) "
                f"60MA={bl.ma60:,.2f}({bl.dev60_pct:+.1f}%) "
                f"추세={bl.trend_20d}"
            )
        except Exception as e:
            logger.warning(f"  {defn['name']}: 수집 실패 ({e})")

    # 캐시 저장
    _save_cache(result)

    logger.info("[Baseline] 수집 완료")
    return result


# ═══════════════════════════════════════════
#  캐시 I/O
# ═══════════════════════════════════════════

def _save_cache(baselines: MacroBaselines) -> None:
    """macro_baseline.json 저장 (atomic write)."""
    try:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        data = asdict(baselines)
        tmp = CACHE_PATH.with_suffix(".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        tmp.replace(CACHE_PATH)
    except Exception as e:
        logger.warning(f"[Baseline] 캐시 저장 실패: {e}")


def _load_cache() -> Optional[MacroBaselines]:
    """오늘 수집된 캐시가 있으면 반환, 없으면 None."""
    try:
        if not CACHE_PATH.exists():
            return None

        data = json.loads(CACHE_PATH.read_text("utf-8"))
        ts = data.get("timestamp", "")
        if not ts:
            return None

        # 오늘 날짜 확인 (같은 날이면 캐시 유효)
        cache_date = ts[:10]
        today_str = date.today().strftime("%Y-%m-%d")
        if cache_date != today_str:
            logger.info(f"[Baseline] 캐시 만료 ({cache_date} != {today_str})")
            return None

        # 복원
        result = MacroBaselines(timestamp=ts)
        for key in INDICATOR_DEFS:
            if key in data and isinstance(data[key], dict):
                bl = IndicatorBaseline(**{
                    k: v for k, v in data[key].items()
                    if k in IndicatorBaseline.__dataclass_fields__
                })
                setattr(result, key, bl)

        logger.info(f"[Baseline] 캐시 로드 ({ts})")
        return result

    except Exception as e:
        logger.warning(f"[Baseline] 캐시 로드 실패: {e}")
        return None


def load_cached_baselines() -> Optional[MacroBaselines]:
    """
    캐시 로드 전용 (수집 안 함).
    staleness 무시하고 있으면 반환. 빠른 참조용.
    """
    try:
        if not CACHE_PATH.exists():
            return None

        data = json.loads(CACHE_PATH.read_text("utf-8"))
        result = MacroBaselines(timestamp=data.get("timestamp", ""))
        for key in INDICATOR_DEFS:
            if key in data and isinstance(data[key], dict):
                bl = IndicatorBaseline(**{
                    k: v for k, v in data[key].items()
                    if k in IndicatorBaseline.__dataclass_fields__
                })
                setattr(result, key, bl)
        return result

    except Exception:
        return None


# ═══════════════════════════════════════════
#  공개 API — 다른 모듈에서 사용
# ═══════════════════════════════════════════

def get_deviation(indicator: str) -> Tuple[float, float, str]:
    """
    특정 지표의 (20MA 이탈도%, 60MA 이탈도%, 추세) 반환.

    Args:
        indicator: "oil", "tnx", "gold", "copper", "usdkrw", "vix"

    Returns:
        (dev20_pct, dev60_pct, trend_20d) — 캐시 없으면 (0, 0, "FLAT")
    """
    try:
        baselines = load_cached_baselines()
        if not baselines:
            return 0.0, 0.0, "FLAT"
        bl = getattr(baselines, indicator, None)
        if not bl or bl.current == 0:
            return 0.0, 0.0, "FLAT"
        return bl.dev20_pct, bl.dev60_pct, bl.trend_20d
    except Exception:
        return 0.0, 0.0, "FLAT"


def get_baseline_summary() -> str:
    """텔레그램 대시보드용 기준선 요약 텍스트."""
    baselines = load_cached_baselines()
    if not baselines:
        return "매크로 기준선: 데이터 없음 (수집 필요)"

    lines = [f"📊 매크로 기준선 ({baselines.timestamp})"]

    trend_icons = {
        "급상승": "⬆️⬆️", "상승": "⬆️", "보합": "➡️",
        "하락": "⬇️", "급하락": "⬇️⬇️",
    }

    for key, defn in INDICATOR_DEFS.items():
        bl = getattr(baselines, key, None)
        if not bl or bl.current == 0:
            continue

        icon = trend_icons.get(bl.trend_20d, "")

        # 포맷 (금리는 %, 나머지는 가격)
        if key == "tnx":
            cur_str = f"{bl.current:.2f}%"
            ma20_str = f"{bl.ma20:.2f}%"
            ma60_str = f"{bl.ma60:.2f}%"
        elif key == "vix":
            cur_str = f"{bl.current:.1f}"
            ma20_str = f"{bl.ma20:.1f}"
            ma60_str = f"{bl.ma60:.1f}"
        elif key == "usdkrw":
            cur_str = f"{bl.current:,.0f}원"
            ma20_str = f"{bl.ma20:,.0f}"
            ma60_str = f"{bl.ma60:,.0f}"
        else:
            cur_str = f"${bl.current:,.1f}"
            ma20_str = f"${bl.ma20:,.1f}"
            ma60_str = f"${bl.ma60:,.1f}"

        lines.append(
            f"  {bl.name:8s} {icon} {cur_str} "
            f"| 20MA {ma20_str}({bl.dev20_pct:+.1f}%) "
            f"| 60MA {ma60_str}({bl.dev60_pct:+.1f}%)"
        )

    return "\n".join(lines)


def get_regime_context() -> Dict[str, str]:
    """
    기준선 이탈도 기반 매크로 컨텍스트 요약.
    다른 모듈(인플레/FX/채권)이 참조할 수 있는 딕셔너리.

    Returns:
        {
            "oil_trend": "급상승",
            "oil_dev60": "+39.5",
            "tnx_trend": "UP",
            "tnx_dev60": "+6.0",
            "usdkrw_trend": "UP",
            ...
            "inflation_pressure": "HIGH",     # HIGH/MODERATE/LOW
            "risk_environment": "ELEVATED",   # ELEVATED/NORMAL/CALM
        }
    """
    ctx = {}
    baselines = load_cached_baselines()
    if not baselines:
        return ctx

    for key in INDICATOR_DEFS:
        bl = getattr(baselines, key, None)
        if not bl or bl.current == 0:
            continue
        ctx[f"{key}_trend"] = bl.trend_20d
        ctx[f"{key}_dev20"] = f"{bl.dev20_pct:+.1f}"
        ctx[f"{key}_dev60"] = f"{bl.dev60_pct:+.1f}"
        ctx[f"{key}_pct_rank"] = f"{bl.pct_rank_60d:.0f}"

    # 파생 판단: 인플레이션 압력
    oil_dev = getattr(baselines.oil, "dev60_pct", 0)
    copper_dev = getattr(baselines.copper, "dev60_pct", 0)
    if oil_dev > 15 and copper_dev > 5:
        ctx["inflation_pressure"] = "HIGH"
    elif oil_dev > 5 or copper_dev > 3:
        ctx["inflation_pressure"] = "MODERATE"
    else:
        ctx["inflation_pressure"] = "LOW"

    # 파생 판단: 리스크 환경
    vix_dev = getattr(baselines.vix, "dev60_pct", 0)
    if vix_dev > 30:
        ctx["risk_environment"] = "ELEVATED"
    elif vix_dev > 10:
        ctx["risk_environment"] = "CAUTIOUS"
    else:
        ctx["risk_environment"] = "CALM"

    return ctx
