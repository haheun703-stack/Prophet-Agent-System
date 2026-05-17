# -*- coding: utf-8 -*-
"""ETF 비중(holdings) 수집기 — 네이버 모바일 stock API 단일 경로.

엔드포인트: https://m.stock.naver.com/api/stock/{ticker}/etfAnalysis
응답 키:    etfTop10MajorConstituentAssets

운용사별 분기 불필요. KODEX/TIGER/ACE/PLUS/RISE/HANARO 등 모두 동일 스키마
({seq, itemCode, itemName, stockCount, etfWeight}) 반환.

5/17 일요일 단타봇 ETF Step C-2 구축. 운용사 직접 사이트(KODEX→samsungfund,
TIGER→investments.miraeasset) 모두 SPA 통합으로 비공식 ajax 막힘.
KRX 정보데이터시스템은 회원 로그인 필요. 네이버가 유일하게 통합 제공.
"""
import json
import logging
import time
from pathlib import Path
from typing import Dict, List, Optional

import requests

logger = logging.getLogger(__name__)

# ── 설정 ──────────────────────────────────────────────
_BASE_URL = "https://m.stock.naver.com/api/stock/{ticker}/etfAnalysis"
_TIMEOUT = 15
_DEFAULT_SLEEP_SEC = 0.3  # 네이버 부하 방지 — 일괄 수집 시 적용

# M-1: Retry 정책 (네이버 5xx/timeout 일시 장애 대응) — 5/17 보강
_MAX_RETRIES = 2          # 총 시도 = 1 + retries = 3회
_RETRY_BACKOFF_SEC = 1.0  # 지수 백오프 기준 (1초, 2초, 4초)
_RETRY_STATUS = (500, 502, 503, 504, 408, 429)  # 일시 장애 코드만

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/123.0 Safari/537.36"
    ),
    "Accept": "application/json",
    "Accept-Language": "ko-KR,ko;q=0.9",
    "Referer": "https://m.stock.naver.com/",
}


def _parse_weight(weight_str: str) -> float:
    """'25.63%' → 25.63 (float)."""
    if not isinstance(weight_str, str):
        return 0.0
    return float(weight_str.rstrip("%").strip() or 0)


def _parse_count(count_str: str) -> int:
    """'2,222' → 2222 (int)."""
    if not isinstance(count_str, str):
        return 0
    return int(count_str.replace(",", "").strip() or 0)


def _fetch_with_retry(url: str, session) -> Optional[Dict]:
    """M-1: 네이버 API 호출 + 일시 장애 시 지수 백오프 재시도.

    재시도 트리거:
      - HTTP 5xx (500/502/503/504), 408 timeout, 429 rate limit
      - Connection 에러, Timeout 에러
    비재시도 (즉시 None):
      - HTTP 4xx (404 등) — 데이터 없음/잘못된 ticker
      - JSON 파싱 실패 — 응답 형식 이상

    Returns:
        파싱된 JSON dict 또는 None
    """
    for attempt in range(_MAX_RETRIES + 1):  # 0, 1, 2 = 총 3회
        try:
            r = session.get(url, headers=_HEADERS, timeout=_TIMEOUT)
            if r.status_code == 200:
                return r.json()
            if r.status_code in _RETRY_STATUS and attempt < _MAX_RETRIES:
                backoff = _RETRY_BACKOFF_SEC * (2 ** attempt)
                logger.info(
                    f"  HTTP {r.status_code} — {backoff:.1f}s 대기 후 재시도 "
                    f"({attempt + 1}/{_MAX_RETRIES})"
                )
                time.sleep(backoff)
                continue
            # 비재시도 코드 (4xx 등)
            logger.warning(f"  HTTP {r.status_code} (비재시도)")
            return None
        except (requests.Timeout, requests.ConnectionError) as e:
            if attempt < _MAX_RETRIES:
                backoff = _RETRY_BACKOFF_SEC * (2 ** attempt)
                logger.info(
                    f"  {type(e).__name__} — {backoff:.1f}s 대기 후 재시도 "
                    f"({attempt + 1}/{_MAX_RETRIES})"
                )
                time.sleep(backoff)
                continue
            logger.warning(f"  {type(e).__name__} 최대 재시도 초과: {e}")
            return None
        except (requests.RequestException, json.JSONDecodeError) as e:
            # 기타 에러는 재시도하지 않음 (파싱 에러 등은 재시도해도 동일)
            logger.warning(f"  {type(e).__name__}: {e}")
            return None
    return None


def fetch_etf_holdings(ticker: str, session: Optional[requests.Session] = None) -> Optional[Dict]:
    """단일 ETF 비중 + 메타 수집 (M-1 retry 적용).

    Returns:
        {
            "ticker": "457990",
            "name": "PLUS 태양광&ESS",
            "issuer": "한화자산운용",
            "base_index": "FnGuide 태양광&ESS 지수",
            "market_value": "1,989억",
            "total_nav": "2,285억",
            "holdings": [
                {"seq": 1, "ticker": "010120", "name": "LS ELECTRIC",
                 "stock_count": 2222, "weight_pct": 25.63},
                ...
            ],
            "fetched_at": "2026-05-17T15:50:00",
        }
        실패 시 None.
    """
    s = session or requests
    url = _BASE_URL.format(ticker=ticker)
    d = _fetch_with_retry(url, s)
    if d is None:
        return None

    holdings_raw = d.get("etfTop10MajorConstituentAssets") or []
    if not isinstance(holdings_raw, list):
        logger.warning(f"[{ticker}] etfTop10MajorConstituentAssets 비정상 타입: {type(holdings_raw).__name__}")
        return None

    holdings = []
    for h in holdings_raw:
        if not isinstance(h, dict):
            continue
        item_code = h.get("itemCode", "").strip()
        item_name = h.get("itemName", "").strip()
        if not item_code:
            continue
        holdings.append({
            "seq": int(h.get("seq", 0) or 0),
            "ticker": item_code,
            "name": item_name,
            "stock_count": _parse_count(h.get("stockCount", "")),
            "weight_pct": _parse_weight(h.get("etfWeight", "")),
        })

    from datetime import datetime
    try:
        from zoneinfo import ZoneInfo
        now_iso = datetime.now(ZoneInfo("Asia/Seoul")).isoformat()
    except ImportError:
        now_iso = datetime.now().isoformat()

    return {
        "ticker": ticker,
        "name": d.get("itemName", "").strip(),
        "issuer": d.get("issuerName", "").strip(),
        "base_index": d.get("etfBaseIndex", "").strip(),
        "market_value": d.get("marketValue", "").strip(),
        "total_nav": d.get("totalNav", "").strip(),
        "holdings": holdings,
        "fetched_at": now_iso,
    }


def collect_holdings_bulk(
    tickers: List[str],
    sleep_sec: float = _DEFAULT_SLEEP_SEC,
    save_dir: Optional[Path] = None,
) -> Dict[str, Optional[Dict]]:
    """여러 ETF 비중 일괄 수집.

    Args:
        tickers: ETF 종목코드 리스트
        sleep_sec: 호출 간 대기 (네이버 부하 방지, 기본 0.3초)
        save_dir: 지정 시 종목별 JSON 파일로 저장 ({save_dir}/{ticker}.json)

    Returns:
        {ticker: fetch_etf_holdings 결과 or None} 딕셔너리
    """
    if save_dir:
        save_dir.mkdir(parents=True, exist_ok=True)

    session = requests.Session()
    session.headers.update(_HEADERS)

    results: Dict[str, Optional[Dict]] = {}
    succeeded = 0
    failed = 0

    for i, ticker in enumerate(tickers, 1):
        data = fetch_etf_holdings(ticker, session=session)
        results[ticker] = data
        if data is not None and len(data.get("holdings", [])) > 0:
            succeeded += 1
            if save_dir:
                out_path = save_dir / f"{ticker}.json"
                out_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        else:
            failed += 1

        if i % 5 == 0:
            logger.info(f"진행 {i}/{len(tickers)} (성공 {succeeded}, 실패 {failed})")

        if i < len(tickers):
            time.sleep(sleep_sec)

    logger.info(f"완료: 성공 {succeeded}/{len(tickers)}, 실패 {failed}")
    return results


if __name__ == "__main__":
    import sys, io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s: %(message)s")

    # 테스트: 4종목
    test_tickers = ["457990", "102960", "469150", "445150"]
    print(f"\n{'='*70}\n  ETF 비중 수집기 테스트 (4종목)\n{'='*70}\n")
    results = collect_holdings_bulk(test_tickers, sleep_sec=0.3)

    for ticker, data in results.items():
        if data is None:
            print(f"\n[{ticker}] ❌ 수집 실패")
            continue
        print(f"\n[{ticker}] {data['name']} ({data['issuer']})")
        print(f"  기초지수: {data['base_index']} / 시총 {data['market_value']}")
        print(f"  TOP 10 비중 (합계 {sum(h['weight_pct'] for h in data['holdings']):.2f}%):")
        for h in data["holdings"]:
            print(f"    {h['seq']:2d}. [{h['ticker']}] {h['name']:<25s} {h['weight_pct']:>6.2f}%  ({h['stock_count']:>8,}주)")
