# -*- coding: utf-8 -*-
"""
컨센서스 목표가 크롤러
=======================
증권사 컨센서스 목표가를 수집하여 JSON 캐시에 저장.

Primary: navercomp.wisereport.co.kr (네이버 증권 → WiseReport)
Backup:  comp.fnguide.com (FnGuide Main Snapshot)

Usage:
    from data.consensus_scraper import fetch_consensus, fetch_consensus_batch
    result = fetch_consensus("005930")          # 단일 종목
    results = fetch_consensus_batch(["005930", "000660"])  # 배치
"""

import json
import logging
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger("BH.Consensus")

_STORE_DIR = Path(__file__).resolve().parent.parent / "data_store"
_CACHE_PATH = _STORE_DIR / "consensus.json"

# ─── 캐시 관리 ───────────────────────────────────────

def _load_cache() -> dict:
    if _CACHE_PATH.exists():
        try:
            with open(_CACHE_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def _save_cache(cache: dict):
    _STORE_DIR.mkdir(parents=True, exist_ok=True)
    with open(_CACHE_PATH, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)


# ─── WiseReport 크롤링 (Primary) ─────────────────────

_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8",
}


def _fetch_wisereport(code: str) -> Optional[dict]:
    """WiseReport에서 컨센서스 요약 크롤링

    Returns: {"target_price": int, "rating": float, "eps": int,
              "per": float, "analyst_count": int, "brokers": [...]} or None
    """
    url = f"https://navercomp.wisereport.co.kr/v2/company/c1010001.aspx?cmp_cd={code}"
    headers = {
        **_HEADERS,
        "Referer": f"https://finance.naver.com/item/coinfo.naver?code={code}",
    }

    try:
        resp = requests.get(url, headers=headers, timeout=10)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        # ── 컨센서스 요약 테이블 (id=cTB15) ──
        table = soup.find("table", {"id": "cTB15"})
        if not table:
            logger.debug(f"{code}: cTB15 테이블 없음")
            return None

        rows = table.find_all("tr")
        if len(rows) < 2:
            return None

        # 헤더: 투자의견 | 목표주가 | EPS | PER | 추정기관수
        cells = rows[1].find_all("td")
        if len(cells) < 5:
            return None

        def _parse_num(text: str) -> float:
            """쉼표/공백 제거 후 숫자 파싱"""
            cleaned = text.strip().replace(",", "").replace(" ", "")
            if not cleaned or cleaned == "-" or cleaned == "N/A":
                return 0.0
            try:
                return float(cleaned)
            except ValueError:
                return 0.0

        rating = _parse_num(cells[0].get_text())
        target_price = int(_parse_num(cells[1].get_text()))
        eps = int(_parse_num(cells[2].get_text()))
        per = _parse_num(cells[3].get_text())
        analyst_count = int(_parse_num(cells[4].get_text()))

        if target_price <= 0:
            logger.debug(f"{code}: 목표주가 0 → 스킵")
            return None

        # ── 증권사별 목표가 (id=cTB24, optional) ──
        brokers = []
        broker_table = soup.find("table", {"id": "cTB24"})
        if broker_table:
            for row in broker_table.find_all("tr")[1:]:  # 헤더 제외
                tds = row.find_all("td")
                if len(tds) >= 3:
                    broker_name = tds[0].get_text(strip=True)
                    broker_target = int(_parse_num(tds[2].get_text()))
                    if broker_name and broker_target > 0:
                        brokers.append({
                            "broker": broker_name,
                            "target": broker_target,
                        })

        return {
            "target_price": target_price,
            "rating": rating,
            "eps": eps,
            "per": per,
            "analyst_count": analyst_count,
            "brokers": brokers[:10],  # 상위 10개만
        }

    except requests.RequestException as e:
        logger.warning(f"WiseReport 요청 실패 {code}: {e}")
        return None
    except Exception as e:
        logger.warning(f"WiseReport 파싱 실패 {code}: {e}")
        return None


# ─── FnGuide 백업 크롤링 ──────────────────────────────

def _fetch_fnguide(code: str) -> Optional[dict]:
    """FnGuide Main Snapshot에서 컨센서스 요약 (백업)"""
    url = f"https://comp.fnguide.com/SVO2/asp/SVD_Main.asp?pGB=1&gicode=A{code}"
    headers = {
        **_HEADERS,
        "Referer": "https://comp.fnguide.com/",
    }

    try:
        resp = requests.get(url, headers=headers, timeout=10)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        # FnGuide TABLE 7 (0-indexed) has consensus: rating, target, eps, per, count
        tables = soup.find_all("table")
        for table in tables:
            rows = table.find_all("tr")
            for row in rows:
                cells = row.find_all("td")
                texts = [c.get_text(strip=True).replace(",", "") for c in cells]
                # 투자의견 | 목표주가 | EPS | PER | 추정기관수
                if len(texts) >= 5:
                    try:
                        rating = float(texts[0]) if texts[0] else 0
                        target = int(float(texts[1])) if texts[1] else 0
                        eps = int(float(texts[2])) if texts[2] else 0
                        per = float(texts[3]) if texts[3] else 0
                        count = int(float(texts[4])) if texts[4] else 0
                        if 1.0 <= rating <= 5.0 and target > 10000 and count >= 3:
                            return {
                                "target_price": target,
                                "rating": rating,
                                "eps": eps,
                                "per": per,
                                "analyst_count": count,
                                "brokers": [],
                            }
                    except (ValueError, TypeError):
                        continue

        return None
    except Exception as e:
        logger.debug(f"FnGuide 실패 {code}: {e}")
        return None


# ─── Public API ───────────────────────────────────────

def fetch_consensus(code: str, use_cache: bool = True, cache_hours: int = 24) -> Optional[dict]:
    """단일 종목 컨센서스 조회

    Args:
        code: 종목코드 (예: "005930")
        use_cache: 캐시 사용 여부
        cache_hours: 캐시 유효시간 (시간)

    Returns: {"target_price": int, "rating": float, "eps": int,
              "per": float, "analyst_count": int, "brokers": [...],
              "source": str, "updated": str} or None
    """
    # 캐시 체크
    if use_cache:
        cache = _load_cache()
        cached = cache.get(code)
        if cached:
            updated = cached.get("updated", "")
            if updated:
                try:
                    age_hours = (
                        datetime.now() - datetime.strptime(updated, "%Y-%m-%d %H:%M")
                    ).total_seconds() / 3600
                    if age_hours < cache_hours:
                        cached["source"] = "cache"
                        return cached
                except ValueError:
                    pass

    # Primary: WiseReport
    result = _fetch_wisereport(code)
    source = "wisereport"

    # Backup: FnGuide
    if result is None:
        result = _fetch_fnguide(code)
        source = "fnguide"

    if result is None:
        return None

    result["source"] = source
    result["updated"] = datetime.now().strftime("%Y-%m-%d %H:%M")

    # 캐시 저장
    cache = _load_cache()
    cache[code] = result
    _save_cache(cache)

    return result


def fetch_consensus_batch(
    codes: list[str],
    delay: float = 0.3,
    use_cache: bool = True,
    cache_hours: int = 24,
) -> dict[str, dict]:
    """배치 컨센서스 조회

    Args:
        codes: 종목코드 리스트
        delay: 요청 간 딜레이(초) — 서버 부하 방지
        use_cache: 캐시 사용
        cache_hours: 캐시 유효시간

    Returns: {code: consensus_dict}
    """
    results = {}
    success = 0
    cached = 0

    for i, code in enumerate(codes):
        result = fetch_consensus(code, use_cache=use_cache, cache_hours=cache_hours)
        if result:
            results[code] = result
            if result.get("source") == "cache":
                cached += 1
            else:
                success += 1

        # 딜레이 (캐시 히트 시 불필요)
        if i < len(codes) - 1 and result and result.get("source") != "cache":
            time.sleep(delay)

    logger.info(
        f"컨센서스 배치: {len(codes)}종목 요청 → "
        f"{success}건 신규 + {cached}건 캐시 = {len(results)}건 성공"
    )
    return results


# ─── CLI ──────────────────────────────────────────────

if __name__ == "__main__":
    import sys

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")

    if len(sys.argv) > 1:
        code = sys.argv[1]
    else:
        code = "005930"

    print(f"컨센서스 조회: {code}")
    result = fetch_consensus(code, use_cache=False)
    if result:
        print(f"  목표주가: {result['target_price']:,}원")
        print(f"  투자의견: {result['rating']:.1f}")
        print(f"  EPS: {result['eps']:,}원")
        print(f"  PER: {result['per']:.1f}")
        print(f"  추정기관: {result['analyst_count']}곳")
        print(f"  소스: {result['source']}")
        if result.get("brokers"):
            print(f"  증권사별:")
            for b in result["brokers"][:5]:
                print(f"    {b['broker']}: {b['target']:,}원")
    else:
        print("  실패!")
