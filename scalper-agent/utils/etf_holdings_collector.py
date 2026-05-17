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


def fetch_etf_holdings(ticker: str, session: Optional[requests.Session] = None) -> Optional[Dict]:
    """단일 ETF 비중 + 메타 수집.

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
    try:
        r = s.get(url, headers=_HEADERS, timeout=_TIMEOUT)
        if r.status_code != 200:
            logger.warning(f"[{ticker}] HTTP {r.status_code}")
            return None
        d = r.json()
    except (requests.RequestException, json.JSONDecodeError) as e:
        logger.warning(f"[{ticker}] 요청 실패: {type(e).__name__}: {e}")
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
