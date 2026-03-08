# -*- coding: utf-8 -*-
"""네이버 금융 수급 검증 - 외국인/기관 5일 누적 순매매
───────────────────────────────────────────────
추천 파이프라인 Step 5a 에서 사용.
기존 nationality_signal.py(KRX 국적별)는 데이터 3종목만 존재 → 사실상 미작동.
→ 네이버 금융 외국인/기관 순매매 크롤링으로 대체.

백테스트(pipeline_backtest.py)와 동일한 점수 로직:
  외국인_누적5 > 0 → min(금액/1e8, 15)  (최대 15점)
  기관_누적5 > 0   → min(금액/1e8, 15)  (최대 15점)
  합산 0~30점 범위

사용:
  from data.supply_naver import score_supply_batch
  scores = score_supply_batch(["005930", "000660"], close_prices={"005930": 82000, ...})
  # {code: (score, detail_str)}
"""

import logging
import time
from typing import Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger("data.supply_naver")

_HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
_REQUEST_DELAY = 0.12  # 네이버 속도 제한 (초)


def _parse_number(text: str) -> int:
    """'+1,554,880' or '-19,602,376' → int"""
    text = text.replace(",", "").replace("+", "").strip()
    if not text or text == "-":
        return 0
    try:
        return int(text)
    except (ValueError, TypeError):
        return 0


def fetch_naver_supply(code: str, days: int = 5) -> Optional[List[dict]]:
    """네이버 금융에서 외국인/기관 일별 순매매 최근 N일 가져오기

    Returns:
        [{"date": "2026.03.07", "close": 82000, "inst_net": 100000, "frgn_net": -50000}, ...]
        최신 날짜가 인덱스 0 (내림차순)
        실패 시 None
    """
    url = "https://finance.naver.com/item/frgn.naver"
    params = {"code": code, "page": "1"}

    try:
        r = requests.get(url, params=params, headers=_HEADERS, timeout=10)
        if r.status_code != 200:
            return None
    except requests.RequestException:
        return None

    soup = BeautifulSoup(r.content.decode("euc-kr", errors="replace"), "html.parser")
    tables = soup.find_all("table")

    # 헤더로 올바른 테이블 찾기 (종목별로 table index가 다름)
    table = None
    for t in tables:
        headers = [th.get_text(strip=True) for th in t.find_all("th")]
        if "날짜" in headers and "기관" in headers and "외국인" in headers:
            table = t
            break

    if table is None:
        return None

    rows = table.find_all("tr")

    results = []
    for row in rows:
        cells = row.find_all("td")
        if len(cells) < 7:
            continue

        date_text = cells[0].get_text(strip=True)
        if not date_text or "." not in date_text:
            continue

        close_text = cells[1].get_text(strip=True)
        close = _parse_number(close_text)
        inst_net = _parse_number(cells[5].get_text(strip=True))
        frgn_net = _parse_number(cells[6].get_text(strip=True))

        results.append({
            "date": date_text,
            "close": close,
            "inst_net": inst_net,
            "frgn_net": frgn_net,
        })

        if len(results) >= days:
            break

    return results if results else None


def score_supply(
    code: str,
    close_price: int = 0,
    days: int = 5,
) -> Tuple[float, str]:
    """단일 종목 네이버 수급 점수 (필터 모드)

    백테스트 검증 결과:
      수급 가산(+점) → PF 1.49 → 1.05 (악화!) - 하위종목 진입 허용
      수급 필터(-점만) → 나쁜 종목 걸러냄 → 핵심 개선점
    → 양수 수급은 0점 (중립), 음수 수급만 감점 (거부권)

    Args:
        code: 종목코드
        close_price: 현재 종가 (순매매량→금액 변환용, 0이면 naver 종가 사용)
        days: 누적 일수 (기본 5)

    Returns:
        (score, detail_str)  score: -25 ~ 0
    """
    rows = fetch_naver_supply(code, days)
    if not rows:
        return 0.0, "데이터없음"

    # 종가 결정 (파라미터 우선, 없으면 최신 naver 종가)
    price = close_price or rows[0].get("close", 0)
    if price <= 0:
        return 0.0, "가격0"

    # 5일 누적 순매매 금액 (원)
    frgn_cum = sum(r["frgn_net"] for r in rows) * price
    inst_cum = sum(r["inst_net"] for r in rows) * price

    # 5일 연속성
    frgn_days_buy = sum(1 for r in rows if r["frgn_net"] > 0)
    inst_days_buy = sum(1 for r in rows if r["inst_net"] > 0)
    frgn_days_sell = sum(1 for r in rows if r["frgn_net"] < 0)
    inst_days_sell = sum(1 for r in rows if r["inst_net"] < 0)

    score = 0.0
    details = []

    # ── 외국인 (양수=참고표시만, 음수=감점) ──
    if frgn_cum > 0:
        details.append(f"외OK({frgn_days_buy}d)")
    elif frgn_cum < 0:
        frgn_pts = max(frgn_cum / 1e8, -10)
        score += frgn_pts
        details.append(f"외{frgn_pts:.0f}({frgn_days_sell}d)")

    # ── 기관 (양수=참고표시만, 음수=감점) ──
    if inst_cum > 0:
        details.append(f"기OK({inst_days_buy}d)")
    elif inst_cum < 0:
        inst_pts = max(inst_cum / 1e8, -10)
        score += inst_pts
        details.append(f"기{inst_pts:.0f}({inst_days_sell}d)")

    # ── 동반이탈 추가 페널티 ──
    if frgn_days_sell >= 4 and inst_days_sell >= 4:
        score -= 5
        details.append("동반이탈-5")

    detail_str = " ".join(details) if details else "변화미미"
    return round(score, 1), detail_str


def score_supply_batch(
    codes: List[str],
    close_prices: Dict[str, int] = None,
    days: int = 5,
) -> Dict[str, Tuple[float, str]]:
    """여러 종목 네이버 수급 점수 배치 계산

    Args:
        codes: 종목코드 리스트
        close_prices: {code: close_price} (없으면 naver에서 자동)
        days: 누적 일수

    Returns: {code: (score, detail)}
    """
    if close_prices is None:
        close_prices = {}

    results = {}
    for i, code in enumerate(codes):
        try:
            price = close_prices.get(code, 0)
            results[code] = score_supply(code, close_price=price, days=days)
        except Exception as e:
            logger.warning(f"수급 점수 실패 {code}: {e}")
            results[code] = (0.0, f"에러:{e}")

        if i < len(codes) - 1:
            time.sleep(_REQUEST_DELAY)

    scored = sum(1 for sc, _ in results.values() if sc != 0)
    logger.info(f"네이버 수급: {scored}/{len(codes)}종목 점수 반영")
    return results
