"""KRX Open API 클라이언트 — OHLCV + 시가총액 일괄 수집.

승인된 API (2026-04-17):
  - stk_bydd_trd: 유가증권(KOSPI) 일별매매정보 → OHLCV + MKTCAP
  - ksq_bydd_trd: 코스닥 일별매매정보 → OHLCV + MKTCAP
  - kospi_dd_trd: KOSPI 시리즈 일별시세 → 지수 OHLCV
  - kosdaq_dd_trd: KOSDAQ 시리즈 일별시세 → 지수 OHLCV
  - krx_dd_trd: KRX 시리즈 일별시세 → 지수 OHLCV
  - etf_bydd_trd: ETF 일별매매정보 (데이터 미제공 상태)

Notes:
  - Base URL: http://data-dbg.krx.co.kr/svc/apis
  - AUTH_KEY: .env의 KRX_OPEN_API_KEY
  - 일 호출 10,000건 제한
  - 투자자별(11주체) 데이터는 미제공 (OHLCV/시총만)
"""
import os
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import requests

logger = logging.getLogger(__name__)

# ── 설정 ──────────────────────────────────────────────────────
_BASE_URL = "http://data-dbg.krx.co.kr/svc/apis"
_CACHE_DIR = Path(__file__).resolve().parent.parent / "data_store" / "krx_cache"

# .env 경로: 프로젝트 루트의 부모 (D:\Prophet_Agent_System_예언자\.env)
_ENV_PATH = Path(__file__).resolve().parent.parent.parent / ".env"


def _get_api_key() -> str:
    """KRX_OPEN_API_KEY를 .env에서 로드."""
    key = os.environ.get("KRX_OPEN_API_KEY", "")
    if key:
        return key
    # .env 파일에서 직접 읽기
    if _ENV_PATH.exists():
        for line in _ENV_PATH.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line.startswith("KRX_OPEN_API_KEY="):
                return line.split("=", 1)[1].strip().strip('"').strip("'")
    return ""


def _fetch(endpoint: str, bas_dd: str) -> List[Dict]:
    """KRX Open API 호출 → OutBlock_1 리스트 반환."""
    key = _get_api_key()
    if not key:
        logger.warning("KRX_OPEN_API_KEY 미설정")
        return []

    url = f"{_BASE_URL}/{endpoint}"
    try:
        r = requests.get(url, params={"basDd": bas_dd, "AUTH_KEY": key}, timeout=20)
        if r.status_code != 200:
            logger.warning("KRX API %s HTTP %d", endpoint, r.status_code)
            return []
        data = r.json()
        return data.get("OutBlock_1", [])
    except Exception as e:
        logger.error("KRX API %s 오류: %s", endpoint, e)
        return []


# ── 주식 OHLCV + MKTCAP ──────────────────────────────────────

def fetch_stock_daily(bas_dd: str, market: str = "all") -> Dict[str, Dict]:
    """전종목 일별 OHLCV + 시가총액 수집.

    Args:
        bas_dd: 기준일 (YYYYMMDD)
        market: "kospi", "kosdaq", "all"

    Returns:
        {종목코드: {open, high, low, close, volume, value, mktcap, shares, change_pct, name}}
    """
    result = {}
    endpoints = []
    if market in ("kospi", "all"):
        endpoints.append("sto/stk_bydd_trd")
    if market in ("kosdaq", "all"):
        endpoints.append("sto/ksq_bydd_trd")

    for ep in endpoints:
        items = _fetch(ep, bas_dd)
        for item in items:
            code = item.get("ISU_CD", "")
            if not code or len(code) != 6:
                continue
            try:
                result[code] = {
                    "name": item.get("ISU_NM", ""),
                    "market": item.get("MKT_NM", ""),
                    "open": int(item.get("TDD_OPNPRC", 0) or 0),
                    "high": int(item.get("TDD_HGPRC", 0) or 0),
                    "low": int(item.get("TDD_LWPRC", 0) or 0),
                    "close": int(item.get("TDD_CLSPRC", 0) or 0),
                    "volume": int(item.get("ACC_TRDVOL", 0) or 0),
                    "value": int(item.get("ACC_TRDVAL", 0) or 0),
                    "mktcap": int(item.get("MKTCAP", 0) or 0),  # 원 단위
                    "mktcap_b": round(int(item.get("MKTCAP", 0) or 0) / 1e8, 1),  # 억원
                    "shares": int(item.get("LIST_SHRS", 0) or 0),
                    "change_pct": float(item.get("FLUC_RT", 0) or 0),
                }
            except (ValueError, TypeError):
                continue

    logger.info("KRX stock daily %s: %d종목 (%s)", bas_dd, len(result), market)
    return result


def get_mktcap_dict(bas_dd: Optional[str] = None) -> Dict[str, float]:
    """전종목 시가총액(억원) dict 반환. 당일 캐시 사용.

    Returns:
        {종목코드: 시총(억원)}
    """
    if bas_dd is None:
        bas_dd = datetime.now().strftime("%Y%m%d")

    # 캐시 확인
    _CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cache_file = _CACHE_DIR / f"mktcap_{bas_dd}.json"
    if cache_file.exists():
        try:
            with open(cache_file, "r", encoding="utf-8") as f:
                cached = json.load(f)
            if len(cached) > 100:
                logger.info("KRX MKTCAP 캐시 사용: %s (%d종목)", bas_dd, len(cached))
                return cached
        except Exception:
            pass

    # API 호출
    stocks = fetch_stock_daily(bas_dd, market="all")
    mktcap = {code: info["mktcap_b"] for code, info in stocks.items() if info["mktcap_b"] > 0}

    # 캐시 저장
    if mktcap:
        try:
            with open(cache_file, "w", encoding="utf-8") as f:
                json.dump(mktcap, f)
            logger.info("KRX MKTCAP 캐시 저장: %s (%d종목)", bas_dd, len(mktcap))
        except Exception as e:
            logger.warning("KRX MKTCAP 캐시 저장 실패: %s", e)

    return mktcap


# ── 지수 OHLCV ────────────────────────────────────────────────

def fetch_index_daily(bas_dd: str, market: str = "all") -> List[Dict]:
    """지수 일별시세 수집.

    Args:
        bas_dd: 기준일 (YYYYMMDD)
        market: "kospi", "kosdaq", "krx", "all"

    Returns:
        [{name, class, close, change, change_pct, open, high, low, volume, value, mktcap}]
    """
    result = []
    endpoints = []
    if market in ("kospi", "all"):
        endpoints.append("idx/kospi_dd_trd")
    if market in ("kosdaq", "all"):
        endpoints.append("idx/kosdaq_dd_trd")
    if market in ("krx", "all"):
        endpoints.append("idx/krx_dd_trd")

    for ep in endpoints:
        items = _fetch(ep, bas_dd)
        for item in items:
            try:
                result.append({
                    "name": item.get("IDX_NM", ""),
                    "class": item.get("IDX_CLSS", ""),
                    "close": float(item.get("CLSPRC_IDX", 0) or 0),
                    "change": float(item.get("CMPPREVDD_IDX", 0) or 0),
                    "change_pct": float(item.get("FLUC_RT", 0) or 0),
                    "open": float(item.get("OPNPRC_IDX", 0) or 0),
                    "high": float(item.get("HGPRC_IDX", 0) or 0),
                    "low": float(item.get("LWPRC_IDX", 0) or 0),
                    "volume": int(item.get("ACC_TRDVOL", 0) or 0),
                    "value": int(item.get("ACC_TRDVAL", 0) or 0),
                })
            except (ValueError, TypeError):
                continue

    logger.info("KRX index daily %s: %d건 (%s)", bas_dd, len(result), market)
    return result


# ── 테스트 ────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    today = datetime.now().strftime("%Y%m%d")

    print(f"\n=== KRX Open API 테스트 ({today}) ===\n")

    # 주식
    stocks = fetch_stock_daily(today)
    print(f"주식: {len(stocks)}종목")
    if "005930" in stocks:
        s = stocks["005930"]
        print(f"  삼성전자: 종가 {s['close']:,}원 / 시총 {s['mktcap_b']:,.0f}억원")

    # 시총
    mktcap = get_mktcap_dict(today)
    print(f"\n시총 dict: {len(mktcap)}종목")

    # 지수
    indices = fetch_index_daily(today, "kospi")
    print(f"\nKOSPI 지수: {len(indices)}건")
    for idx in indices[:5]:
        if idx["close"] > 0:
            print(f"  {idx['name']}: {idx['close']:,.2f} ({idx['change_pct']:+.2f}%)")
