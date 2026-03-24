"""
옵션/선물 시그널 — 시장 심리 계량화
====================================
OPT-01: P/C Ratio 대용 (인버스/레버리지 ETF 거래대금 비율)
OPT-02: NXT 연동 (VIX 의존도 감소)
OPT-03: BRAIN Phase 통합

데이터 소스:
  - 인버스 ETF 거래대금 vs 레버리지 ETF 거래대금 비율 (pykrx)
    → P/C Ratio 대용: 인버스 매수 = 풋옵션 매수와 동일 심리
  - VIX (yfinance, 이미 nightwatch에서 수집)
  - 기관/외인 순매수 (pykrx, nightwatch에서 수집)

핵심 아이디어:
  인버스 ETF 거래대금 ÷ 레버리지 ETF 거래대금 = Fear/Greed Ratio
  Ratio > 1.5 → EXTREME_FEAR (극단 공포 = 역발상 매수)
  Ratio > 1.0 → FEAR (공포)
  Ratio 0.5~1.0 → NEUTRAL
  Ratio < 0.5 → GREED (탐욕)
  Ratio < 0.3 → EXTREME_GREED (과열)
"""

import json
import logging
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Optional, Tuple

logger = logging.getLogger(__name__)

STORE_DIR = Path(__file__).resolve().parent.parent / "data_store"

# ── ETF 코드 ──
INVERSE_ETFS = {
    "114800": "KODEX 인버스",
    "252670": "KODEX 200선물인버스2X",
    "145670": "KBSTAR 200선물인버스",
}
LEVERAGE_ETFS = {
    "122630": "KODEX 레버리지",
    "233740": "KODEX 코스닥150레버리지",
    "278540": "KODEX MSCI Korea TR",
}

# ── 심리 구간 ──
EXTREME_FEAR_THRESHOLD = 1.5
FEAR_THRESHOLD = 1.0
GREED_THRESHOLD = 0.5
EXTREME_GREED_THRESHOLD = 0.3


@dataclass
class OptionsSignal:
    """시장 심리 시그널"""
    date: str = ""
    # P/C Ratio 대용
    fear_greed_ratio: float = 0.0       # 인버스/레버리지 거래대금 비율
    pc_level: str = "NEUTRAL"           # EXTREME_FEAR / FEAR / NEUTRAL / GREED / EXTREME_GREED
    inverse_value: int = 0              # 인버스 ETF 총 거래대금 (억)
    leverage_value: int = 0             # 레버리지 ETF 총 거래대금 (억)
    # 5일 추이
    ratio_5d_avg: float = 0.0           # 5일 평균 비율
    ratio_trend: str = "FLAT"           # RISING(공포↑) / FALLING(탐욕↑) / FLAT
    # NXT 보정값
    nxt_score_adj: float = 0.0          # NXT에 반영할 점수 (-2.0 ~ +2.0)
    # BRAIN 반영값
    brain_score_adj: float = 0.0        # BRAIN에 반영할 점수 (-5 ~ +5)
    detail: str = ""


# ═══════════════════════════════════════════════════
#  OPT-01: 데이터 수집
# ═══════════════════════════════════════════════════

def _get_etf_trading_value(date_str: str = None) -> Tuple[int, int]:
    """인버스/레버리지 ETF 거래대금 합산

    Returns:
        (inverse_total_억, leverage_total_억)
    """
    try:
        from pykrx import stock

        if date_str is None:
            date_str = datetime.now().strftime("%Y%m%d")

        inverse_total = 0
        for code in INVERSE_ETFS:
            try:
                df = stock.get_market_ohlcv(date_str, date_str, code)
                if not df.empty:
                    # 거래대금 = 종가 × 거래량 (또는 '거래대금' 컬럼)
                    if "거래대금" in df.columns:
                        inverse_total += int(df["거래대금"].iloc[-1])
                    else:
                        inverse_total += int(df["종가"].iloc[-1] * df["거래량"].iloc[-1])
            except Exception:
                pass

        leverage_total = 0
        for code in LEVERAGE_ETFS:
            try:
                df = stock.get_market_ohlcv(date_str, date_str, code)
                if not df.empty:
                    if "거래대금" in df.columns:
                        leverage_total += int(df["거래대금"].iloc[-1])
                    else:
                        leverage_total += int(df["종가"].iloc[-1] * df["거래량"].iloc[-1])
            except Exception:
                pass

        # 억 단위 변환
        return inverse_total // 100_000_000, leverage_total // 100_000_000

    except ImportError:
        logger.warning("[OPTIONS] pykrx 미설치")
        return 0, 0
    except Exception as e:
        logger.error(f"[OPTIONS] ETF 거래대금 수집 실패: {e}")
        return 0, 0


def _get_5d_ratios() -> list:
    """최근 5거래일 인버스/레버리지 비율 (batch 조회 — API 6회로 최적화)"""
    try:
        from pykrx import stock

        end = datetime.now()
        start = (end - timedelta(days=14)).strftime("%Y%m%d")
        end_str = end.strftime("%Y%m%d")

        # 6 ETF를 기간 범위로 한번에 조회 (60회 → 6회)
        inv_daily = {}  # {date_str: total_억}
        for code in INVERSE_ETFS:
            try:
                df = stock.get_market_ohlcv(start, end_str, code)
                if not df.empty:
                    for dt, row in df.iterrows():
                        ds = dt.strftime("%Y%m%d")
                        val = int(row.get("거래대금", row["종가"] * row["거래량"]))
                        inv_daily[ds] = inv_daily.get(ds, 0) + val
            except Exception:
                pass

        lev_daily = {}
        for code in LEVERAGE_ETFS:
            try:
                df = stock.get_market_ohlcv(start, end_str, code)
                if not df.empty:
                    for dt, row in df.iterrows():
                        ds = dt.strftime("%Y%m%d")
                        val = int(row.get("거래대금", row["종가"] * row["거래량"]))
                        lev_daily[ds] = lev_daily.get(ds, 0) + val
            except Exception:
                pass

        # 날짜 내림차순 → 최근 5거래일 비율
        all_dates = sorted(set(inv_daily.keys()) & set(lev_daily.keys()), reverse=True)
        ratios = []
        for ds in all_dates:
            inv = inv_daily[ds] // 100_000_000
            lev = lev_daily[ds] // 100_000_000
            if inv > 0 and lev > 0:
                ratios.append(inv / lev)
                if len(ratios) >= 5:
                    break

        return ratios

    except Exception:
        return []


# ═══════════════════════════════════════════════════
#  OPT-01/02/03: 통합 분석
# ═══════════════════════════════════════════════════

def collect_options_signal(date_str: str = None) -> OptionsSignal:
    """옵션/선물 심리 시그널 수집 + 분석

    Returns:
        OptionsSignal with pc_level, nxt_score_adj, brain_score_adj
    """
    sig = OptionsSignal(
        date=date_str or datetime.now().strftime("%Y-%m-%d"),
    )

    # 1. 당일 인버스/레버리지 거래대금
    inv, lev = _get_etf_trading_value(date_str)
    sig.inverse_value = inv
    sig.leverage_value = lev

    if inv == 0 and lev == 0:
        sig.detail = "ETF 데이터 없음"
        return sig

    # 2. Fear/Greed Ratio 계산
    if lev > 0:
        sig.fear_greed_ratio = round(inv / lev, 3)
    else:
        sig.fear_greed_ratio = 2.0 if inv > 0 else 0.5

    # 3. P/C Level 판정
    r = sig.fear_greed_ratio
    if r >= EXTREME_FEAR_THRESHOLD:
        sig.pc_level = "EXTREME_FEAR"
    elif r >= FEAR_THRESHOLD:
        sig.pc_level = "FEAR"
    elif r >= GREED_THRESHOLD:
        sig.pc_level = "NEUTRAL"
    elif r >= EXTREME_GREED_THRESHOLD:
        sig.pc_level = "GREED"
    else:
        sig.pc_level = "EXTREME_GREED"

    # 4. 5일 추이
    ratios_5d = _get_5d_ratios()
    if len(ratios_5d) >= 3:
        sig.ratio_5d_avg = round(sum(ratios_5d) / len(ratios_5d), 3)
        if ratios_5d[0] > ratios_5d[-1] * 1.15:
            sig.ratio_trend = "RISING"   # 공포 증가
        elif ratios_5d[0] < ratios_5d[-1] * 0.85:
            sig.ratio_trend = "FALLING"  # 탐욕 증가
        else:
            sig.ratio_trend = "FLAT"

    # ── OPT-02: NXT 보정값 ──
    # P/C가 NEUTRAL이면 VIX 영향 추가 감소 (NXT divergence dampener)
    if sig.pc_level == "EXTREME_FEAR":
        sig.nxt_score_adj = 1.0    # 극단 공포 = 역발상 매수 (과매도)
    elif sig.pc_level == "FEAR":
        sig.nxt_score_adj = -0.5   # 공포 = 주의
    elif sig.pc_level == "NEUTRAL":
        sig.nxt_score_adj = 0.0    # VIX 공포지만 한국은 중립 → VIX 감쇄
    elif sig.pc_level == "GREED":
        sig.nxt_score_adj = -0.5   # 과열 주의
    elif sig.pc_level == "EXTREME_GREED":
        sig.nxt_score_adj = -1.0   # 극단 탐욕 = 과열 경고

    # ── OPT-03: BRAIN 보정값 ──
    if sig.pc_level == "EXTREME_FEAR" and r >= 2.0:
        sig.brain_score_adj = 5.0   # 극단 공포 = 바닥 근처 (역발상)
    elif sig.pc_level == "EXTREME_FEAR":
        sig.brain_score_adj = 3.0
    elif sig.pc_level == "FEAR":
        sig.brain_score_adj = -3.0  # 공포: 보수적
    elif sig.pc_level == "NEUTRAL":
        sig.brain_score_adj = 0.0
    elif sig.pc_level == "GREED":
        sig.brain_score_adj = -2.0  # 탐욕: 약간 보수적
    elif sig.pc_level == "EXTREME_GREED":
        sig.brain_score_adj = -5.0  # 극단 탐욕: 과열 위험

    sig.detail = (
        f"인버스{inv}억/레버리지{lev}억 "
        f"비율{sig.fear_greed_ratio:.2f} "
        f"{sig.pc_level} "
        f"5d평균{sig.ratio_5d_avg:.2f}"
    )

    # 저장
    _save_signal(sig)

    return sig


def _save_signal(sig: OptionsSignal):
    """최신 시그널 저장"""
    path = STORE_DIR / "options_signal_latest.json"
    try:
        tmp = path.with_suffix(".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(asdict(sig), f, ensure_ascii=False, indent=2)
        tmp.replace(path)
    except Exception as e:
        logger.error(f"[OPTIONS] 저장 실패: {e}")


def load_latest_signal() -> Optional[OptionsSignal]:
    """저장된 최신 시그널 로드"""
    path = STORE_DIR / "options_signal_latest.json"
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return OptionsSignal(**data)
    except Exception:
        return None


# ═══════════════════════════════════════════════════
#  외부 호출 인터페이스
# ═══════════════════════════════════════════════════

def get_nxt_options_adjustment() -> Tuple[float, str]:
    """NXT에서 호출 — 옵션 심리 보정값

    Returns:
        (score_adj: -2.0~+2.0, detail: str)
    """
    sig = load_latest_signal()
    if sig is None:
        return 0.0, ""

    # 당일 데이터만 유효
    if sig.date != datetime.now().strftime("%Y-%m-%d"):
        return 0.0, ""

    return sig.nxt_score_adj, f"opt:{sig.pc_level}({sig.fear_greed_ratio:.2f})"


def get_brain_options_adjustment() -> Tuple[float, str]:
    """BRAIN에서 호출 — 옵션 심리 보정값

    Returns:
        (score_adj: -5~+5, detail: str)
    """
    sig = load_latest_signal()
    if sig is None:
        return 0.0, ""

    if sig.date != datetime.now().strftime("%Y-%m-%d"):
        return 0.0, ""

    return sig.brain_score_adj, f"opt:{sig.pc_level}({sig.fear_greed_ratio:.2f})"


def format_telegram(sig: OptionsSignal) -> str:
    """텔레그램 표시용"""
    emoji = {
        "EXTREME_FEAR": "😱", "FEAR": "😰",
        "NEUTRAL": "😐", "GREED": "🤑", "EXTREME_GREED": "🔥",
    }.get(sig.pc_level, "❓")
    trend_emoji = {"RISING": "↗️", "FALLING": "↘️", "FLAT": "→"}.get(sig.ratio_trend, "→")
    return (
        f"{emoji} 시장심리: {sig.pc_level} "
        f"(인버스/레버리지 {sig.fear_greed_ratio:.2f}x {trend_emoji})\n"
        f"   인버스 {sig.inverse_value}억 / 레버리지 {sig.leverage_value}억"
    )


# ═══════════════════════════════════════════════════
#  CLI 테스트
# ═══════════════════════════════════════════════════

if __name__ == "__main__":
    import sys
    from dotenv import load_dotenv

    env_path = Path(__file__).resolve().parent.parent.parent / ".env"
    load_dotenv(env_path)

    logging.basicConfig(level=logging.INFO, format="%(message)s")

    if "--test" in sys.argv:
        print("옵션/선물 심리 시그널 수집 중...")
        sig = collect_options_signal()
        print(format_telegram(sig))
        print(f"\nNXT 보정: {sig.nxt_score_adj:+.1f}")
        print(f"BRAIN 보정: {sig.brain_score_adj:+.1f}")
        print(f"상세: {sig.detail}")
    else:
        print("사용법: python -m data.options_signal --test")
