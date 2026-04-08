# -*- coding: utf-8 -*-
"""
미국장 야간 데이터 수집기 (US Market Overnight Collector)
=========================================================
매일 08:05 KST — 미국장 마감 후 yfinance로 전체 데이터 수집.
us_overnight_filter.py 가 이 데이터로 진입 모드를 결정함.

출력: data_store/us_market_overnight.json

수집 항목:
  - 지수: S&P500, NASDAQ, VIX, SOX(반도체)
  - 달러: DXY
  - 금리: 5Y, 10Y → 스프레드 계산
  - 섹터 ETF: XLK, XLF, XLE 등 11개
  - 개별 미국주: NVDA, TSLA, AAPL 등 (릴레이 매핑용)
"""

import json
import logging
from datetime import datetime, date
from pathlib import Path

logger = logging.getLogger("BH.USCollector")

DATA_DIR = Path(__file__).parent.parent / "data_store"
OUTPUT_PATH = DATA_DIR / "us_market_overnight.json"

# ── 수집 대상 티커 ────────────────────────────────────────

# 주요 지수
INDEX_TICKERS = {
    "^GSPC":  "sp500",
    "^IXIC":  "nasdaq",
    "^VIX":   "vix",
    "^SOX":   "sox",       # PHLX Semiconductor Index
}

# 달러 + 금리
MACRO_TICKERS = {
    "DX-Y.NYB": "dxy",     # Dollar Index
    "^FVX":     "us_5y",    # 5Y Treasury Yield
    "^TNX":     "us_10y",   # 10Y Treasury Yield
}

# 섹터 ETF (S&P 500 SPDR 시리즈)
SECTOR_TICKERS = {
    "XLK":  "기술",
    "XLF":  "금융",
    "XLE":  "에너지",
    "XLV":  "헬스케어",
    "XLI":  "산업재",
    "XLY":  "경기소비재",
    "XLP":  "필수소비재",
    "XLU":  "유틸리티",
    "XLB":  "소재",
    "XLRE": "부동산",
    "XLC":  "커뮤니케이션",
}

# 개별 미국주 (릴레이 매핑용)
INDIVIDUAL_TICKERS = [
    "NVDA", "AMD", "AVGO", "TSLA", "AAPL",
    "LMT", "RTX", "XOM", "LNG", "GM",
]


def collect_us_overnight() -> dict:
    """미국장 마감 데이터 전체 수집 → dict 반환 + JSON 저장."""
    import yfinance as yf

    logger.info("[US수집] 미국장 야간 데이터 수집 시작")

    result = {
        "date": date.today().isoformat(),
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }

    # ── 1) 지수 수집 ─────────────────────────────────────
    for ticker, key in INDEX_TICKERS.items():
        chg = _get_change_pct(yf, ticker)
        if key == "vix":
            # VIX는 등락률이 아니라 절대값이 중요
            val = _get_last_close(yf, ticker)
            result["vix"] = round(val, 1) if val else 0
            result["vix_change"] = round(chg, 2) if chg is not None else 0
        elif key == "sox":
            result["soxx_change"] = round(chg, 2) if chg is not None else 0
        elif key == "nasdaq":
            result["nasdaq_change"] = round(chg, 2) if chg is not None else 0
        elif key == "sp500":
            result["sp500_change"] = round(chg, 2) if chg is not None else 0

    # ── 2) 달러 + 금리 ───────────────────────────────────
    for ticker, key in MACRO_TICKERS.items():
        val = _get_last_close(yf, ticker)
        if key == "dxy":
            result["dxy"] = round(val, 2) if val else 0
        elif key == "us_5y":
            result["us_5y_yield"] = round(val, 3) if val else 0
        elif key == "us_10y":
            result["us_10y_yield"] = round(val, 3) if val else 0

    # 스프레드 계산 (5Y - 10Y, 역전 감지용)
    y5 = result.get("us_5y_yield", 0)
    y10 = result.get("us_10y_yield", 0)
    if y5 > 0 and y10 > 0:
        result["spread_5y_10y"] = round(y5 - y10, 3)
    # us_3y_yield 는 5Y로 근사 (yfinance에 3Y 없음)
    result["us_3y_yield"] = result.get("us_5y_yield", 0)
    result["spread_3y_10y"] = result.get("spread_5y_10y", 0)

    # ── 3) 섹터 ETF ──────────────────────────────────────
    sector_etf = {}
    for ticker, kr_name in SECTOR_TICKERS.items():
        chg = _get_change_pct(yf, ticker)
        if chg is not None:
            sector_etf[ticker] = round(chg, 2)
    result["sector_etf"] = sector_etf

    # ── 4) 개별 미국주 ───────────────────────────────────
    individual_stocks = {}
    for ticker in INDIVIDUAL_TICKERS:
        chg = _get_change_pct(yf, ticker)
        if chg is not None:
            individual_stocks[ticker] = round(chg, 2)
    result["individual_stocks"] = individual_stocks

    # ── 5) Fear & Greed (VIX 기반 대리 판단) ─────────────
    vix_val = result.get("vix", 20)
    if vix_val >= 35:
        fg, fg_label = 10, "Extreme Fear"
    elif vix_val >= 25:
        fg, fg_label = 30, "Fear"
    elif vix_val >= 20:
        fg, fg_label = 45, "Neutral"
    elif vix_val >= 15:
        fg, fg_label = 65, "Greed"
    else:
        fg, fg_label = 80, "Extreme Greed"
    result["fear_greed"] = fg
    result["fear_greed_label"] = fg_label

    # ── 6) 위험 플래그 ───────────────────────────────────
    risk_flags = []
    if result.get("vix", 0) >= 25:
        risk_flags.append(f"VIX {result['vix']:.0f} 경계 구간")
    if result.get("dxy", 0) >= 106:
        risk_flags.append(f"DXY {result['dxy']:.1f} 초강달러")
    if result.get("nasdaq_change", 0) <= -2:
        risk_flags.append(f"나스닥 {result['nasdaq_change']:+.1f}% 급락")
    if result.get("soxx_change", 0) <= -3:
        risk_flags.append(f"반도체(SOX) {result['soxx_change']:+.1f}% 급락")
    if result.get("spread_5y_10y", 0) < -0.1:
        risk_flags.append("장단기 금리역전")
    result["risk_flags"] = risk_flags

    # ── 7) 한국 영향 요약 ────────────────────────────────
    nq = result.get("nasdaq_change", 0)
    sox = result.get("soxx_change", 0)
    if nq >= 1.0 and sox >= 1.5:
        kr_impact = "강세 (나스닥+반도체 동반 상승)"
    elif nq >= 0.5:
        kr_impact = "약강세"
    elif nq <= -1.0 and sox <= -1.5:
        kr_impact = "약세 (나스닥+반도체 동반 하락)"
    elif nq <= -0.5:
        kr_impact = "약약세"
    else:
        kr_impact = "보합"
    result["kr_impact"] = kr_impact

    # ── 저장 ─────────────────────────────────────────────
    _save_json(result)

    logger.info(
        f"[US수집] 완료: 나스닥 {result.get('nasdaq_change', 0):+.2f}% | "
        f"SOXX {result.get('soxx_change', 0):+.2f}% | "
        f"VIX {result.get('vix', 0):.1f} | DXY {result.get('dxy', 0):.1f}"
    )
    return result


def _get_change_pct(yf, ticker: str) -> float | None:
    """yfinance로 전일 대비 등락률(%) 계산."""
    try:
        data = yf.Ticker(ticker)
        hist = data.history(period="5d")
        if hist is not None and len(hist) >= 2:
            prev = float(hist["Close"].iloc[-2])
            last = float(hist["Close"].iloc[-1])
            if prev > 0:
                return (last / prev - 1) * 100
    except Exception as e:
        logger.warning(f"[US수집] {ticker} 수집 실패: {e}")
    return None


def _get_last_close(yf, ticker: str) -> float | None:
    """yfinance로 최근 종가(절대값) 가져오기."""
    try:
        data = yf.Ticker(ticker)
        hist = data.history(period="5d")
        if hist is not None and len(hist) >= 1:
            return float(hist["Close"].iloc[-1])
    except Exception as e:
        logger.warning(f"[US수집] {ticker} 절대값 수집 실패: {e}")
    return None


def _save_json(data: dict):
    """JSON 원자적 저장."""
    try:
        tmp = OUTPUT_PATH.with_suffix(".tmp")
        tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(OUTPUT_PATH)
    except Exception as e:
        logger.warning(f"[US수집] 저장 실패: {e}")


# ── CLI 실행 ─────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    result = collect_us_overnight()
    print(json.dumps(result, ensure_ascii=False, indent=2))
