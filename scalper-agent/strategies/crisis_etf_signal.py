# -*- coding: utf-8 -*-
"""
위기 ETF 시그널 모듈
====================
VIX / S&P500 선물 / 유가 등을 2~3일 추세로 감시해서
인버스/레버리지 ETF 진입 시그널을 생성.

시그널 종류:
  INVERSE_ENTRY  - 하락장 대비 인버스 진입 추천
  LEVERAGE_ENTRY - 바닥 후 반등 레버리지 진입 추천
  HOLD           - 관망
  EXIT_INVERSE   - 인버스 청산 (반등 시작)
  EXIT_LEVERAGE  - 레버리지 청산 (추가 하락)

사용법:
  python -m strategies.crisis_etf_signal
"""

import os
import sys
import io
import json
import logging
from pathlib import Path
from datetime import datetime, timedelta
from dataclasses import dataclass, field, asdict
from typing import Optional

logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data_store"


# ═══════════════════════════════════════════════════
#  ETF 종목 정보
# ═══════════════════════════════════════════════════

ETF_MAP = {
    "inverse_1x":  {"code": "114800", "name": "KODEX 인버스",        "mult": -1},
    "inverse_2x":  {"code": "252670", "name": "KODEX 200선물인버스2X", "mult": -2},
    "leverage_2x": {"code": "122630", "name": "KODEX 레버리지",       "mult": 2},
    "kospi_1x":    {"code": "069500", "name": "KODEX 200",           "mult": 1},
}


# ═══════════════════════════════════════════════════
#  시그널 데이터 클래스
# ═══════════════════════════════════════════════════

@dataclass
class CrisisSignal:
    timestamp: str = ""
    signal: str = "HOLD"          # INVERSE_ENTRY / LEVERAGE_ENTRY / HOLD / EXIT_*
    confidence: str = "LOW"       # LOW / MEDIUM / HIGH
    reason: str = ""
    recommended_etf: dict = field(default_factory=dict)

    # 지표 스냅샷
    vix_current: float = 0
    vix_change_1d: float = 0      # VIX 전일 대비 변화율(%)
    vix_change_3d: float = 0      # VIX 3일 변화율(%)
    sp500_change_1d: float = 0    # S&P500 전일 대비(%)
    sp500_change_3d: float = 0    # S&P500 3일 변화율(%)
    wti_change_1d: float = 0      # WTI 원유(%)
    kospi_change_1d: float = 0    # KOSPI(KODEX200) 전일 대비(%)
    kospi_change_3d: float = 0    # KOSPI 3일 변화율(%)

    # 서킷브레이커 카운트다운
    circuit_breaker_d: int = -1   # 서킷브레이커 발동 후 D+N일 (-1=미발동)

    def to_dict(self):
        return asdict(self)


# ═══════════════════════════════════════════════════
#  데이터 수집 (yfinance)
# ═══════════════════════════════════════════════════

def _fetch_global_indicators() -> dict:
    """VIX, S&P500, WTI 3일치 데이터 수집"""
    import yfinance as yf

    result = {}
    targets = {
        "vix":   "^VIX",
        "sp500": "^GSPC",
        "wti":   "CL=F",
    }

    for key, ticker in targets.items():
        try:
            data = yf.Ticker(ticker)
            hist = data.history(period="5d")
            if hist is not None and len(hist) >= 2:
                closes = hist["Close"].values
                result[key] = {
                    "current": float(closes[-1]),
                    "prev_1d": float(closes[-2]),
                    "prev_3d": float(closes[-4]) if len(closes) >= 4 else float(closes[0]),
                    "change_1d": (float(closes[-1]) / float(closes[-2]) - 1) * 100,
                    "change_3d": (float(closes[-1]) / (float(closes[-4]) if len(closes) >= 4 else float(closes[0])) - 1) * 100,
                }
            else:
                result[key] = None
        except Exception as e:
            logger.warning(f"{key} 데이터 수집 실패: {e}")
            result[key] = None

    # KOSPI (KODEX200 ETF)
    try:
        from data.extend_parquet_data import load_daily
        kospi_df = load_daily("069500")
        if kospi_df is not None and len(kospi_df) >= 4:
            close_col = "종가" if "종가" in kospi_df.columns else "close"
            closes = kospi_df[close_col].values
            result["kospi"] = {
                "current": float(closes[-1]),
                "change_1d": (float(closes[-1]) / float(closes[-2]) - 1) * 100,
                "change_3d": (float(closes[-1]) / float(closes[-4]) - 1) * 100,
            }
        else:
            result["kospi"] = None
    except Exception:
        result["kospi"] = None

    return result


# ═══════════════════════════════════════════════════
#  서킷브레이커 추적
# ═══════════════════════════════════════════════════

CB_PATH = DATA_DIR / "circuit_breaker.json"


def record_circuit_breaker(reason: str = "서킷브레이커 발동"):
    """서킷브레이커 발동 기록"""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    state = {
        "date": datetime.now().strftime("%Y-%m-%d"),
        "reason": reason,
        "recorded_at": datetime.now().isoformat(),
    }
    with open(CB_PATH, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)
    logger.info(f"서킷브레이커 기록: {reason}")


def get_circuit_breaker_d() -> int:
    """서킷브레이커 발동 후 D+N일 반환 (-1=미발동/만료)"""
    if not CB_PATH.exists():
        return -1
    try:
        with open(CB_PATH, "r", encoding="utf-8") as f:
            state = json.load(f)
        cb_date = datetime.strptime(state["date"], "%Y-%m-%d").date()
        today = datetime.now().date()
        delta = (today - cb_date).days
        if delta > 30:  # 30일 지나면 만료
            return -1
        return delta
    except Exception:
        return -1


# ═══════════════════════════════════════════════════
#  시그널 판정 엔진
# ═══════════════════════════════════════════════════

def generate_signal() -> CrisisSignal:
    """글로벌 지표 기반 인버스/레버리지 ETF 시그널 생성

    판정 기준 (2~3일 전 감지 목표):

    [INVERSE_ENTRY 조건] - 3개 중 2개 이상 충족
      1. VIX 3일 변화율 +30%+ (공포 급등 추세)
      2. S&P500 3일 -3%+ (글로벌 하락 추세)
      3. VIX 절대값 30+ (공포 구간 진입)

    [LEVERAGE_ENTRY 조건] - 서킷브레이커 D+2~5 & 3개 중 2개 충족
      1. VIX 전일 대비 하락 (-5%+)
      2. S&P500 전일 반등 (+0.5%+)
      3. KOSPI 전일 반등 (+1%+)

    [EXIT_INVERSE] - 인버스 청산
      1. VIX 전일 대비 -10%+ 급락
      2. S&P500 +1%+ 반등
    """
    sig = CrisisSignal(timestamp=datetime.now().strftime("%Y-%m-%d %H:%M"))

    print("\n📊 위기 ETF 시그널 분석")
    print("=" * 50)

    # 데이터 수집
    indicators = _fetch_global_indicators()

    vix = indicators.get("vix")
    sp = indicators.get("sp500")
    wti = indicators.get("wti")
    kospi = indicators.get("kospi")

    # 지표 채우기
    if vix:
        sig.vix_current = vix["current"]
        sig.vix_change_1d = vix["change_1d"]
        sig.vix_change_3d = vix["change_3d"]
        print(f"  VIX: {vix['current']:.1f} (1D: {vix['change_1d']:+.1f}%, 3D: {vix['change_3d']:+.1f}%)")
    else:
        print("  VIX: 데이터 없음")

    if sp:
        sig.sp500_change_1d = sp["change_1d"]
        sig.sp500_change_3d = sp["change_3d"]
        print(f"  S&P500: {sp['current']:,.0f} (1D: {sp['change_1d']:+.1f}%, 3D: {sp['change_3d']:+.1f}%)")
    else:
        print("  S&P500: 데이터 없음")

    if wti:
        sig.wti_change_1d = wti["change_1d"]
        print(f"  WTI: ${wti['current']:.1f} (1D: {wti['change_1d']:+.1f}%)")

    if kospi:
        sig.kospi_change_1d = kospi["change_1d"]
        sig.kospi_change_3d = kospi["change_3d"]
        print(f"  KOSPI: {kospi['current']:,.0f} (1D: {kospi['change_1d']:+.1f}%, 3D: {kospi['change_3d']:+.1f}%)")

    cb_d = get_circuit_breaker_d()
    sig.circuit_breaker_d = cb_d
    if cb_d >= 0:
        print(f"  서킷브레이커: D+{cb_d}")

    print()

    # ─── INVERSE 진입 판정 ───
    inv_score = 0
    inv_reasons = []

    if vix and vix["change_3d"] >= 30:
        inv_score += 1
        inv_reasons.append(f"VIX 3일 +{vix['change_3d']:.0f}% 급등")
    if sp and sp["change_3d"] <= -3:
        inv_score += 1
        inv_reasons.append(f"S&P500 3일 {sp['change_3d']:.1f}% 하락")
    if vix and vix["current"] >= 30:
        inv_score += 1
        inv_reasons.append(f"VIX {vix['current']:.0f} 공포구간")

    # 추가: WTI 급등 (지정학 리스크)
    if wti and wti["change_1d"] >= 5:
        inv_score += 1
        inv_reasons.append(f"WTI +{wti['change_1d']:.1f}% 유가급등")

    # ─── LEVERAGE 진입 판정 (반등) ───
    lev_score = 0
    lev_reasons = []

    if cb_d >= 2 and cb_d <= 5:
        # 서킷브레이커 D+2~5 구간에서만 레버리지 추천
        if vix and vix["change_1d"] <= -5:
            lev_score += 1
            lev_reasons.append(f"VIX 전일 {vix['change_1d']:.1f}% 하락")
        if sp and sp["change_1d"] >= 0.5:
            lev_score += 1
            lev_reasons.append(f"S&P500 전일 +{sp['change_1d']:.1f}% 반등")
        if kospi and kospi["change_1d"] >= 1.0:
            lev_score += 1
            lev_reasons.append(f"KOSPI 전일 +{kospi['change_1d']:.1f}% 반등")

    # ─── EXIT INVERSE 판정 ───
    exit_inv = False
    exit_reasons = []
    if vix and vix["change_1d"] <= -10:
        exit_inv = True
        exit_reasons.append(f"VIX 전일 {vix['change_1d']:.1f}% 급락")
    if sp and sp["change_1d"] >= 1.0:
        exit_inv = True
        exit_reasons.append(f"S&P500 +{sp['change_1d']:.1f}% 반등")

    # ─── 시그널 결정 ───
    if inv_score >= 3:
        sig.signal = "INVERSE_ENTRY"
        sig.confidence = "HIGH"
        sig.reason = " | ".join(inv_reasons)
        sig.recommended_etf = ETF_MAP["inverse_2x"]
    elif inv_score >= 2:
        sig.signal = "INVERSE_ENTRY"
        sig.confidence = "MEDIUM"
        sig.reason = " | ".join(inv_reasons)
        sig.recommended_etf = ETF_MAP["inverse_1x"]
    elif lev_score >= 2:
        sig.signal = "LEVERAGE_ENTRY"
        sig.confidence = "MEDIUM" if lev_score >= 3 else "LOW"
        sig.reason = " | ".join(lev_reasons)
        sig.recommended_etf = ETF_MAP["leverage_2x"]
    elif exit_inv:
        sig.signal = "EXIT_INVERSE"
        sig.confidence = "MEDIUM"
        sig.reason = " | ".join(exit_reasons)
    else:
        sig.signal = "HOLD"
        sig.confidence = "LOW"
        sig.reason = "뚜렷한 시그널 없음"

    # 출력
    icons = {
        "INVERSE_ENTRY": "🔴",
        "LEVERAGE_ENTRY": "🟢",
        "EXIT_INVERSE": "🟡",
        "EXIT_LEVERAGE": "🟡",
        "HOLD": "⚪",
    }
    print(f"  {icons.get(sig.signal, '?')} 시그널: {sig.signal} ({sig.confidence})")
    print(f"  사유: {sig.reason}")
    if sig.recommended_etf:
        etf = sig.recommended_etf
        print(f"  추천 ETF: {etf['name']} ({etf['code']})")

    # 저장
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    sig_path = DATA_DIR / "crisis_etf_signal.json"
    with open(sig_path, "w", encoding="utf-8") as f:
        json.dump(sig.to_dict(), f, ensure_ascii=False, indent=2)
    print(f"\n  저장: {sig_path}")

    return sig


def format_signal_telegram(sig: CrisisSignal) -> str:
    """텔레그램용 포맷"""
    icons = {
        "INVERSE_ENTRY": "🔴",
        "LEVERAGE_ENTRY": "🟢",
        "EXIT_INVERSE": "🟡",
        "EXIT_LEVERAGE": "🟡",
        "HOLD": "⚪",
    }
    lines = [
        f"📊 위기 ETF 시그널",
        f"━━━━━━━━━━━━━━━━━━━",
        f"",
        f"{icons.get(sig.signal, '?')} {sig.signal} ({sig.confidence})",
        f"",
        f"VIX: {sig.vix_current:.1f} (1D:{sig.vix_change_1d:+.1f}% 3D:{sig.vix_change_3d:+.1f}%)",
        f"S&P: {sig.sp500_change_1d:+.1f}% (3D:{sig.sp500_change_3d:+.1f}%)",
        f"KOSPI: {sig.kospi_change_1d:+.1f}% (3D:{sig.kospi_change_3d:+.1f}%)",
    ]

    if sig.circuit_breaker_d >= 0:
        lines.append(f"서킷브레이커: D+{sig.circuit_breaker_d}")

    lines.append(f"")
    lines.append(f"사유: {sig.reason}")

    if sig.recommended_etf:
        etf = sig.recommended_etf
        lines.append(f"")
        lines.append(f"👉 추천: {etf['name']} ({etf['code']})")

    lines.append(f"━━━━━━━━━━━━━━━━━━━")
    return "\n".join(lines)


def load_signal() -> Optional[CrisisSignal]:
    """저장된 시그널 로드"""
    sig_path = DATA_DIR / "crisis_etf_signal.json"
    if not sig_path.exists():
        return None
    try:
        with open(sig_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        sig = CrisisSignal(**{k: v for k, v in data.items()
                              if k in CrisisSignal.__dataclass_fields__})
        return sig
    except Exception:
        return None


# ═══════════════════════════════════════════════════
#  CLI
# ═══════════════════════════════════════════════════

if __name__ == "__main__":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
    logging.basicConfig(level=logging.WARNING)
    generate_signal()
