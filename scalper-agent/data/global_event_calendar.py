# -*- coding: utf-8 -*-
"""
해외 이벤트 캘린더 - 실적발표/FOMC/CPI 사전 감지 + 한국 수혜주 매핑
=====================================================================
D-3 사전 알림 → 한국 관련 섹터 종목 가중치 부여

데이터 소스 (6-Source):
  1. yfinance - 미국 빅테크 실적발표 일정
  2. 한국은행 ECOS API - 기준금리/환율/소비자물가 (BOK_API_KEY)
  3. Alpha Vantage - USD/KRW 실시간 + WTI + 천연가스 (ALPHA_VANTAGE_API_KEY)
  4. Tavily - 실시간 뉴스/이벤트 웹검색 (TAVILY_API_KEY)
  5. Perplexity API - AI 분석 + 경제 캘린더 (PERPLEXITY_API_KEY)
  6. 하드코딩 경제 캘린더 - FOMC/CPI/고용 등 정기 일정

사용법:
  python -m data.global_event_calendar              # 전체 스캔
  python -m data.global_event_calendar --verify     # API 키 전체 검증
  python -m data.global_event_calendar --telegram   # 텔레그램 전송
"""

import json
import logging
import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

import requests

logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data_store"
CALENDAR_PATH = DATA_DIR / "global_events.json"


# ═══════════════════════════════════════════════════
#  미국 핵심기업 → 한국 수혜 섹터/종목 매핑 DB
# ═══════════════════════════════════════════════════

US_KR_SECTOR_MAP = {
    "NVDA": {
        "name": "엔비디아",
        "kr_sectors": ["반도체", "반도체장비", "반도체부품소재", "반도체패키징"],
        "kr_stocks": [
            ("000660", "SK하이닉스", 95, "HBM 납품"),
            ("005930", "삼성전자", 90, "메모리+파운드리"),
            ("042700", "한미반도체", 85, "패키징장비"),
            ("403870", "HPSP", 80, "반도체장비"),
            ("058470", "리노공업", 78, "테스트소켓"),
            ("036930", "주성엔지니어링", 75, "ALD장비"),
            ("025560", "미래산업", 70, "반도체부품"),
        ],
    },
    "AMD": {
        "name": "AMD",
        "kr_sectors": ["반도체", "반도체장비"],
        "kr_stocks": [
            ("000660", "SK하이닉스", 90, "HBM 납품"),
            ("005930", "삼성전자", 85, "파운드리"),
            ("042700", "한미반도체", 75, "패키징"),
        ],
    },
    "AAPL": {
        "name": "애플",
        "kr_sectors": ["IT부품", "디스플레이", "카메라모듈", "2차전지"],
        "kr_stocks": [
            ("066570", "LG전자", 80, "디스플레이+부품"),
            ("034220", "LG디스플레이", 85, "OLED패널"),
            ("005930", "삼성전자", 75, "AP+메모리"),
        ],
    },
    "TSLA": {
        "name": "테슬라",
        "kr_sectors": ["2차전지", "자동차부품", "양극재", "음극재"],
        "kr_stocks": [
            ("373220", "LG에너지솔루션", 90, "배터리셀"),
            ("006400", "삼성SDI", 85, "배터리"),
            ("247540", "에코프로비엠", 80, "양극재"),
            ("003670", "포스코퓨처엠", 78, "양극재+음극재"),
        ],
    },
    "MSFT": {
        "name": "마이크로소프트",
        "kr_sectors": ["클라우드", "AI", "소프트웨어"],
        "kr_stocks": [
            ("035420", "NAVER", 80, "클라우드+AI"),
            ("035720", "카카오", 70, "AI플랫폼"),
            ("034020", "두산에너빌리티", 65, "데이터센터 전력"),
        ],
    },
    "AMZN": {
        "name": "아마존",
        "kr_sectors": ["클라우드", "물류", "이커머스"],
        "kr_stocks": [
            ("035420", "NAVER", 75, "클라우드"),
            ("267260", "HD현대일렉트릭", 70, "데이터센터 전력인프라"),
        ],
    },
    "GOOG": {
        "name": "구글",
        "kr_sectors": ["AI", "광고", "클라우드"],
        "kr_stocks": [
            ("035420", "NAVER", 80, "검색+AI"),
            ("035720", "카카오", 70, "AI"),
        ],
    },
    "META": {
        "name": "메타",
        "kr_sectors": ["AI", "VR/AR", "광고"],
        "kr_stocks": [
            ("035420", "NAVER", 70, "AI"),
            ("005930", "삼성전자", 65, "VR디바이스+메모리"),
        ],
    },
    "ASML": {
        "name": "ASML",
        "kr_sectors": ["반도체장비", "반도체"],
        "kr_stocks": [
            ("000660", "SK하이닉스", 85, "EUV 도입"),
            ("005930", "삼성전자", 85, "EUV 파운드리"),
            ("403870", "HPSP", 75, "반도체장비"),
        ],
    },
    "AVGO": {
        "name": "브로드컴",
        "kr_sectors": ["반도체", "네트워크"],
        "kr_stocks": [
            ("000660", "SK하이닉스", 80, "AI반도체"),
            ("058470", "리노공업", 70, "테스트"),
        ],
    },
}

# 추적 대상 미국 종목 (실적 캘린더)
US_WATCHLIST = list(US_KR_SECTOR_MAP.keys())


# ═══════════════════════════════════════════════════
#  1. yfinance 실적 캘린더 조회
# ═══════════════════════════════════════════════════

def fetch_earnings_calendar(symbols: List[str] = None, days_ahead: int = 7) -> List[Dict]:
    """yfinance로 향후 N일간 미국 실적발표 일정 조회"""
    try:
        import yfinance as yf
    except ImportError:
        logger.warning("yfinance 미설치 - pip install yfinance")
        return []

    if symbols is None:
        symbols = US_WATCHLIST

    events = []
    today = datetime.now().date()
    deadline = today + timedelta(days=days_ahead)

    for sym in symbols:
        try:
            t = yf.Ticker(sym)
            cal = t.calendar
            if cal is None or cal.empty if hasattr(cal, 'empty') else not cal:
                continue

            # yfinance calendar: dict or DataFrame
            earnings_date = None
            if isinstance(cal, dict):
                ed = cal.get("Earnings Date", [])
                if ed:
                    earnings_date = ed[0] if isinstance(ed, list) else ed
            else:
                # DataFrame
                if "Earnings Date" in cal.index:
                    val = cal.loc["Earnings Date"]
                    if hasattr(val, 'iloc'):
                        earnings_date = val.iloc[0]
                    else:
                        earnings_date = val

            if earnings_date is None:
                continue

            # 날짜 파싱
            if hasattr(earnings_date, 'date'):
                ed = earnings_date.date()
            elif isinstance(earnings_date, str):
                ed = datetime.strptime(earnings_date[:10], "%Y-%m-%d").date()
            else:
                continue

            if today <= ed <= deadline:
                days_until = (ed - today).days
                info = US_KR_SECTOR_MAP.get(sym, {})
                events.append({
                    "symbol": sym,
                    "name": info.get("name", sym),
                    "earnings_date": str(ed),
                    "days_until": days_until,
                    "kr_sectors": info.get("kr_sectors", []),
                    "kr_stocks": info.get("kr_stocks", []),
                    "event_type": "EARNINGS",
                })

            time.sleep(0.2)

        except Exception as e:
            logger.debug(f"yfinance {sym} calendar 실패: {e}")
            continue

    events.sort(key=lambda x: x["days_until"])
    return events


# ═══════════════════════════════════════════════════
#  2. Perplexity API - 실시간 이벤트 + 한국 수혜 분석
# ═══════════════════════════════════════════════════

def query_perplexity(prompt: str, model: str = "sonar") -> Optional[str]:
    """Perplexity API 질의 (실시간 웹검색 포함)"""
    api_key = os.getenv("PERPLEXITY_API_KEY", "")
    if not api_key:
        logger.warning("PERPLEXITY_API_KEY 미설정")
        return None

    url = "https://api.perplexity.ai/chat/completions"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": model,
        "messages": [
            {
                "role": "system",
                "content": (
                    "당신은 한국 주식시장 전문 애널리스트입니다. "
                    "해외 이벤트가 한국 증시에 미치는 영향을 분석하세요. "
                    "반드시 한국어로 답변하고, 구체적 종목코드(6자리)와 섹터를 포함하세요."
                ),
            },
            {"role": "user", "content": prompt},
        ],
        "max_tokens": 1500,
        "temperature": 0.3,
    }

    try:
        resp = requests.post(url, json=payload, headers=headers, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        return data["choices"][0]["message"]["content"]
    except Exception as e:
        logger.error(f"Perplexity API 실패: {e}")
        return None


def analyze_upcoming_events(events: List[Dict]) -> List[Dict]:
    """Perplexity로 향후 이벤트의 한국 시장 영향 분석"""
    if not events:
        return events

    # D-3 이내 이벤트만 분석 (API 호출 절약)
    urgent = [e for e in events if e["days_until"] <= 3]
    if not urgent:
        return events

    names = ", ".join(f"{e['name']}({e['symbol']})" for e in urgent)
    dates = ", ".join(f"{e['earnings_date']}" for e in urgent)

    prompt = (
        f"향후 3일 내 미국 주요 기업 실적발표 예정:\n"
        f"기업: {names}\n"
        f"일정: {dates}\n\n"
        f"질문:\n"
        f"1. 각 기업 실적 시장 컨센서스와 전망은?\n"
        f"2. 한국 증시에 직접 영향받는 섹터와 대표 종목 3~5개씩 추천\n"
        f"3. 실적 호/악재 시 각각 어떤 한국 종목이 가장 큰 영향을 받는지\n"
        f"4. 매수 타이밍 제안 (실적 전/후)"
    )

    analysis = query_perplexity(prompt)
    if analysis:
        for e in urgent:
            e["perplexity_analysis"] = analysis

    return events


# ═══════════════════════════════════════════════════
#  3. 경제 캘린더 (정기 이벤트)
# ═══════════════════════════════════════════════════

def fetch_economic_calendar(days_ahead: int = 7) -> List[Dict]:
    """Perplexity로 향후 1주일 글로벌 경제 이벤트 조회"""
    today = datetime.now().strftime("%Y-%m-%d")
    end = (datetime.now() + timedelta(days=days_ahead)).strftime("%Y-%m-%d")

    prompt = (
        f"오늘 날짜: {today}\n"
        f"{today} ~ {end} 기간 중 한국 증시에 영향을 줄 수 있는 "
        f"주요 글로벌 경제 이벤트를 알려주세요.\n\n"
        f"포함: FOMC, CPI, 고용지표, 주요국 금리결정, 미국 빅테크 실적발표\n\n"
        f"각 이벤트마다:\n"
        f"- 날짜, 이벤트명, 영향도(HIGH/MEDIUM/LOW)\n"
        f"- 한국 영향 섹터/종목\n"
        f"- 예상 방향(POSITIVE/NEGATIVE/NEUTRAL)\n\n"
        f"JSON 형식으로 반환: "
        f'[{{"date":"YYYY-MM-DD","event":"이벤트명","impact":"HIGH",'
        f'"direction":"POSITIVE","kr_sectors":["섹터"],"kr_tickers":["종목코드"]}}]'
    )

    raw = query_perplexity(prompt)
    if not raw:
        return []

    # JSON 파싱 시도
    events = _extract_json_from_text(raw)
    return events


def _extract_json_from_text(text: str) -> List[Dict]:
    """텍스트에서 JSON 배열 추출 (코드블록 포함)"""
    import re

    # ```json ... ``` 코드블록 안의 JSON 추출
    code_match = re.search(r'```(?:json)?\s*(\[[\s\S]*?\])\s*```', text)
    if code_match:
        try:
            return json.loads(code_match.group(1))
        except json.JSONDecodeError:
            pass

    # 코드블록 없이 [...] 패턴 (greedy로 전체 배열 매칭)
    match = re.search(r'\[[\s\S]+\]', text)
    if match:
        try:
            return json.loads(match.group())
        except json.JSONDecodeError:
            pass

    # 실패 시 텍스트 자체를 이벤트로 저장
    return [{"raw_text": text, "source": "perplexity"}]


# ═══════════════════════════════════════════════════
#  A-1. 한국은행 ECOS API — 매크로 경제지표
# ═══════════════════════════════════════════════════

_BOK_QUERIES = [
    # (key, label, stat_code, freq, item_code, days_back)
    ("base_rate", "기준금리(%)", "722Y001", "D", "0101000", 90),
    ("usd_krw", "원/달러", "731Y003", "D", "0000001", 7),
    ("cpi_yoy", "소비자물가(%YoY)", "901Y009", "M", "0", 180),
]


def fetch_bok_indicators() -> Dict:
    """한국은행 ECOS API — 기준금리 / 환율 / 소비자물가"""
    api_key = os.getenv("BOK_API_KEY", "").strip()
    if not api_key:
        logger.warning("BOK_API_KEY 미설정")
        return {}

    base_url = "https://ecos.bok.or.kr/api/StatisticSearch"
    today = datetime.now()
    indicators = {}

    for key, label, stat_code, freq, item_code, days_back in _BOK_QUERIES:
        try:
            if freq == "M":
                start = (today - timedelta(days=days_back)).strftime("%Y%m")
                end = today.strftime("%Y%m")
            else:
                start = (today - timedelta(days=days_back)).strftime("%Y%m%d")
                end = today.strftime("%Y%m%d")

            url = (
                f"{base_url}/{api_key}/json/kr/1/10"
                f"/{stat_code}/{freq}/{start}/{end}/{item_code}"
            )
            resp = requests.get(url, timeout=10)
            data = resp.json()
            rows = data.get("StatisticSearch", {}).get("row", [])
            if rows:
                latest = rows[-1]  # 가장 최근 데이터
                indicators[key] = {
                    "value": float(latest.get("DATA_VALUE", 0)),
                    "date": latest.get("TIME", ""),
                    "label": label,
                }
        except Exception as e:
            logger.warning(f"BOK {key} 조회 실패: {e}")

    return indicators


# ═══════════════════════════════════════════════════
#  A-2. Tavily 웹검색 — 실시간 뉴스/이벤트
# ═══════════════════════════════════════════════════


def fetch_tavily_news(days_ahead: int = 7) -> Dict:
    """Tavily 웹검색 — 한국 증시 영향 글로벌 뉴스/이벤트"""
    api_key = os.getenv("TAVILY_API_KEY", "").strip()
    if not api_key:
        logger.warning("TAVILY_API_KEY 미설정")
        return {"answer": "", "articles": []}

    today_s = datetime.now().strftime("%Y-%m-%d")
    end_s = (datetime.now() + timedelta(days=days_ahead)).strftime("%Y-%m-%d")

    payload = {
        "api_key": api_key,
        "query": (
            f"한국 증시 영향 글로벌 경제 이벤트 {today_s}~{end_s} "
            "FOMC CPI 실적발표 금리 환율 반도체 원자재"
        ),
        "search_depth": "advanced",
        "max_results": 8,
        "include_answer": True,
    }

    try:
        resp = requests.post(
            "https://api.tavily.com/search", json=payload, timeout=30
        )
        resp.raise_for_status()
        data = resp.json()

        articles = []
        for item in data.get("results", []):
            articles.append({
                "title": item.get("title", ""),
                "url": item.get("url", ""),
                "content": item.get("content", "")[:300],
                "score": item.get("score", 0),
            })

        return {
            "answer": data.get("answer", ""),
            "articles": articles,
        }
    except Exception as e:
        logger.warning(f"Tavily 검색 실패: {e}")
        return {"answer": "", "articles": []}


# ═══════════════════════════════════════════════════
#  A-3. Alpha Vantage — 글로벌 시장 (환율 / 원자재)
# ═══════════════════════════════════════════════════


def fetch_alpha_vantage_global() -> Dict:
    """Alpha Vantage — USD/KRW 실시간 + WTI + 천연가스"""
    api_key = os.getenv("ALPHA_VANTAGE_API_KEY", "").strip()
    if not api_key:
        logger.warning("ALPHA_VANTAGE_API_KEY 미설정")
        return {}

    base = "https://www.alphavantage.co/query"
    markets = {}

    # ── USD/KRW 실시간 ──
    try:
        resp = requests.get(base, params={
            "function": "CURRENCY_EXCHANGE_RATE",
            "from_currency": "USD", "to_currency": "KRW",
            "apikey": api_key,
        }, timeout=10)
        info = resp.json().get("Realtime Currency Exchange Rate", {})
        if info:
            markets["usd_krw"] = {
                "value": float(info.get("5. Exchange Rate", 0)),
                "date": info.get("6. Last Refreshed", ""),
                "label": "원/달러(실시간)",
            }
    except Exception as e:
        logger.warning(f"AV USD/KRW 실패: {e}")

    time.sleep(1)

    # ── WTI 원유 ──
    try:
        resp = requests.get(base, params={
            "function": "WTI", "interval": "daily", "apikey": api_key,
        }, timeout=10)
        for d in resp.json().get("data", []):
            if d.get("value", ".") != ".":
                markets["wti"] = {
                    "value": float(d["value"]),
                    "date": d.get("date", ""),
                    "label": "WTI유가($)",
                }
                break
    except Exception as e:
        logger.warning(f"AV WTI 실패: {e}")

    time.sleep(1)

    # ── 천연가스 ──
    try:
        resp = requests.get(base, params={
            "function": "NATURAL_GAS", "interval": "daily", "apikey": api_key,
        }, timeout=10)
        for d in resp.json().get("data", []):
            if d.get("value", ".") != ".":
                markets["natural_gas"] = {
                    "value": float(d["value"]),
                    "date": d.get("date", ""),
                    "label": "천연가스($)",
                }
                break
    except Exception as e:
        logger.warning(f"AV 천연가스 실패: {e}")

    return markets


# ═══════════════════════════════════════════════════
#  A-4. 전체 API 연결 검증
# ═══════════════════════════════════════════════════


def verify_all_apis() -> Dict:
    """모든 데이터 소스 API 키 연결 테스트"""
    if not os.getenv("BOK_API_KEY"):
        try:
            from dotenv import load_dotenv
            load_dotenv(BASE_DIR.parent / ".env")
        except ImportError:
            pass

    results = {}
    today_s = datetime.now().strftime("%Y%m%d")
    start_s = (datetime.now() - timedelta(days=30)).strftime("%Y%m%d")

    # ── A-1 한국은행 ──
    k = os.getenv("BOK_API_KEY", "").strip()
    if not k:
        results["A-1 한국은행"] = ("NO_KEY", "BOK_API_KEY 미설정")
    else:
        try:
            url = (
                f"https://ecos.bok.or.kr/api/StatisticSearch/{k}/json/kr/1/1"
                f"/722Y001/D/{start_s}/{today_s}/0101000"
            )
            data = requests.get(url, timeout=10).json()
            rows = data.get("StatisticSearch", {}).get("row", [])
            if rows:
                results["A-1 한국은행"] = ("OK", f"기준금리 {rows[0]['DATA_VALUE']}%")
            else:
                msg = data.get("RESULT", {}).get("MESSAGE", str(data)[:60])
                results["A-1 한국은행"] = ("FAIL", msg)
        except Exception as e:
            results["A-1 한국은행"] = ("ERROR", str(e)[:80])

    # ── A-2 Tavily ──
    k = os.getenv("TAVILY_API_KEY", "").strip()
    if not k:
        results["A-2 Tavily"] = ("NO_KEY", "TAVILY_API_KEY 미설정")
    else:
        try:
            data = requests.post("https://api.tavily.com/search", json={
                "api_key": k, "query": "한국 증시", "max_results": 1,
            }, timeout=15).json()
            if "results" in data:
                results["A-2 Tavily"] = ("OK", f"{len(data['results'])}건 검색 성공")
            else:
                results["A-2 Tavily"] = ("FAIL", str(data)[:80])
        except Exception as e:
            results["A-2 Tavily"] = ("ERROR", str(e)[:80])

    # ── A-3 Alpha Vantage ──
    k = os.getenv("ALPHA_VANTAGE_API_KEY", "").strip()
    if not k:
        results["A-3 Alpha Vantage"] = ("NO_KEY", "ALPHA_VANTAGE_API_KEY 미설정")
    else:
        try:
            data = requests.get("https://www.alphavantage.co/query", params={
                "function": "CURRENCY_EXCHANGE_RATE",
                "from_currency": "USD", "to_currency": "KRW",
                "apikey": k,
            }, timeout=10).json()
            info = data.get("Realtime Currency Exchange Rate", {})
            if info:
                rate = float(info.get("5. Exchange Rate", 0))
                results["A-3 Alpha Vantage"] = ("OK", f"USD/KRW {rate:,.2f}")
            else:
                results["A-3 Alpha Vantage"] = ("FAIL", str(data)[:80])
        except Exception as e:
            results["A-3 Alpha Vantage"] = ("ERROR", str(e)[:80])

    # ── A-3 네이버 (스크래핑, 키 불필요) ──
    try:
        resp = requests.get(
            "https://finance.naver.com/marketindex/",
            headers={"User-Agent": "Mozilla/5.0"}, timeout=10,
        )
        if resp.status_code == 200:
            results["A-3 네이버"] = ("OK", "환율 페이지 접근 성공")
        else:
            results["A-3 네이버"] = ("FAIL", f"HTTP {resp.status_code}")
    except Exception as e:
        results["A-3 네이버"] = ("ERROR", str(e)[:80])

    # ── Perplexity ──
    k = os.getenv("PERPLEXITY_API_KEY", "").strip()
    if not k:
        results["Perplexity"] = ("NO_KEY", "PERPLEXITY_API_KEY 미설정")
    else:
        try:
            resp = requests.post(
                "https://api.perplexity.ai/chat/completions",
                headers={"Authorization": f"Bearer {k}", "Content-Type": "application/json"},
                json={"model": "sonar", "messages": [{"role": "user", "content": "hello"}], "max_tokens": 5},
                timeout=15,
            )
            if resp.status_code == 200:
                results["Perplexity"] = ("OK", "연결 성공")
            else:
                results["Perplexity"] = ("FAIL", f"HTTP {resp.status_code}")
        except Exception as e:
            results["Perplexity"] = ("ERROR", str(e)[:80])

    # ── Finnhub ──
    k = os.getenv("FINNHUB_API_KEY", "").strip()
    if not k:
        results["Finnhub"] = ("NO_KEY", "FINNHUB_API_KEY 미설정")
    else:
        try:
            data = requests.get("https://finnhub.io/api/v1/quote", params={
                "symbol": "AAPL", "token": k,
            }, timeout=10).json()
            if "c" in data and data["c"] > 0:
                results["Finnhub"] = ("OK", f"AAPL ${data['c']}")
            else:
                results["Finnhub"] = ("FAIL", str(data)[:80])
        except Exception as e:
            results["Finnhub"] = ("ERROR", str(e)[:80])

    # ── DART ──
    k = os.getenv("DART_API_KEY", "").strip()
    if not k:
        results["DART"] = ("NO_KEY", "DART_API_KEY 미설정")
    else:
        try:
            data = requests.get("https://opendart.fss.or.kr/api/list.json", params={
                "crtfc_key": k,
                "bgn_de": (datetime.now() - timedelta(days=1)).strftime("%Y%m%d"),
                "end_de": today_s, "page_count": 1,
            }, timeout=10).json()
            if data.get("status") == "000":
                results["DART"] = ("OK", f"공시 {data.get('total_count', '?')}건")
            else:
                results["DART"] = ("FAIL", data.get("message", "?")[:80])
        except Exception as e:
            results["DART"] = ("ERROR", str(e)[:80])

    return results


# ═══════════════════════════════════════════════════
#  4. 통합 스캔 + 저장
# ═══════════════════════════════════════════════════

def scan_global_events(
    days_ahead: int = 7,
    use_perplexity: bool = True,
) -> Dict:
    """해외 이벤트 전체 스캔

    Returns:
        {
            "scanned_at": str,
            "earnings": [...],       # 미국 실적 캘린더
            "economic": [...],       # 경제 이벤트
            "alerts": [...],         # D-3 이내 긴급 알림
            "kr_beneficiaries": [...] # 한국 수혜주 통합
        }
    """
    # .env 로드 (텔레그램 봇/외부 호출 시)
    if not os.getenv("PERPLEXITY_API_KEY"):
        try:
            from dotenv import load_dotenv
            load_dotenv(BASE_DIR.parent / ".env")
        except ImportError:
            pass

    print("=" * 60)
    print("  글로벌 이벤트 캘린더 스캔 (6-Source)")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 60)

    # 1. 실적 캘린더 (yfinance)
    print("\n[1/6] 미국 실적 캘린더 조회...")
    earnings = fetch_earnings_calendar(days_ahead=days_ahead)
    print(f"  → {len(earnings)}개 이벤트 감지")
    for e in earnings:
        print(f"    D-{e['days_until']} | {e['name']}({e['symbol']}) → {e['earnings_date']}")

    # 2. 한국은행 매크로 지표 (BOK)
    print("\n[2/6] 한국은행 매크로 지표...")
    macro_indicators = {}
    if os.getenv("BOK_API_KEY", "").strip():
        macro_indicators = fetch_bok_indicators()
        if macro_indicators:
            for k, v in macro_indicators.items():
                val = v["value"]
                fmt = f"{val:,.2f}" if val > 100 else f"{val}"
                print(f"  → {v['label']}: {fmt}")
        else:
            print("  → 데이터 없음")
    else:
        print("  → BOK_API_KEY 미설정 (스킵)")

    # 3. 글로벌 시장 데이터 (Alpha Vantage)
    print("\n[3/6] 글로벌 시장 데이터 (Alpha Vantage)...")
    global_markets = {}
    if os.getenv("ALPHA_VANTAGE_API_KEY", "").strip():
        global_markets = fetch_alpha_vantage_global()
        if global_markets:
            for k, v in global_markets.items():
                val = v["value"]
                fmt = f"{val:,.2f}" if val > 100 else f"${val}"
                print(f"  → {v['label']}: {fmt}")
        else:
            print("  → 데이터 없음")
    else:
        print("  → ALPHA_VANTAGE_API_KEY 미설정 (스킵)")

    # 4. 실시간 뉴스 검색 (Tavily)
    print("\n[4/6] 실시간 뉴스 검색 (Tavily)...")
    news = {"answer": "", "articles": []}
    if os.getenv("TAVILY_API_KEY", "").strip():
        news = fetch_tavily_news(days_ahead=days_ahead)
        n_articles = len(news.get("articles", []))
        print(f"  → {n_articles}건 뉴스 수집")
        if news.get("answer"):
            print(f"  → AI 요약: {news['answer'][:100]}...")
    else:
        print("  → TAVILY_API_KEY 미설정 (스킵)")

    # 5. Perplexity AI 분석 (D-3 이내)
    if use_perplexity and earnings:
        print("\n[5/6] Perplexity AI 분석...")
        earnings = analyze_upcoming_events(earnings)
        print("  → 분석 완료")
    else:
        print("\n[5/6] Perplexity 분석 스킵")

    # 6. 경제 캘린더 (Perplexity)
    economic = []
    if use_perplexity:
        print("\n[6/6] 경제 캘린더 조회 (Perplexity)...")
        economic = fetch_economic_calendar(days_ahead)
        print(f"  → {len(economic)}개 이벤트")
    else:
        print("\n[6/6] 경제 캘린더 스킵")

    # 7. D-3 알림 생성
    alerts = [e for e in earnings if e["days_until"] <= 3]

    # 5. 한국 수혜주 통합
    kr_bene = {}
    # 5-a. 실적발표 기반
    for e in earnings:
        for ticker, name, relevance, metric in e.get("kr_stocks", []):
            key = ticker
            if key not in kr_bene:
                kr_bene[key] = {
                    "ticker": ticker,
                    "name": name,
                    "events": [],
                    "total_relevance": 0,
                    "sectors": set(),
                }
            kr_bene[key]["events"].append(f"{e['name']} 실적(D-{e['days_until']})")
            kr_bene[key]["total_relevance"] += relevance
            kr_bene[key]["sectors"].update(e.get("kr_sectors", []))

    # 5-b. 경제이벤트 기반 (Perplexity)
    for ev in economic:
        if "raw_text" in ev:
            continue  # 미파싱 데이터 스킵
        tickers = ev.get("kr_tickers", [])
        sectors = ev.get("kr_sectors", [])
        ev_name = ev.get("event", "경제이벤트")
        ev_date = ev.get("date", "")
        impact = ev.get("impact", "MEDIUM")
        relevance = 80 if impact == "HIGH" else 50

        for ticker in tickers:
            if ticker not in kr_bene:
                kr_bene[ticker] = {
                    "ticker": ticker,
                    "name": ticker,  # 이름은 나중에 매핑
                    "events": [],
                    "total_relevance": 0,
                    "sectors": set(),
                }
            kr_bene[ticker]["events"].append(f"{ev_name}({ev_date})")
            kr_bene[ticker]["total_relevance"] += relevance
            kr_bene[ticker]["sectors"].update(sectors)

    # set → list 변환
    beneficiaries = []
    for v in kr_bene.values():
        v["sectors"] = list(v["sectors"])
        beneficiaries.append(v)
    beneficiaries.sort(key=lambda x: -x["total_relevance"])

    result = {
        "scanned_at": datetime.now().isoformat(),
        "earnings": earnings,
        "economic": economic,
        "alerts": alerts,
        "kr_beneficiaries": beneficiaries,
        "macro_indicators": macro_indicators,
        "global_markets": global_markets,
        "news": news,
    }

    # 저장
    with open(CALENDAR_PATH, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2, default=str)
    print(f"\n저장: {CALENDAR_PATH}")

    return result


# ═══════════════════════════════════════════════════
#  5. 텔레그램 메시지 포맷 — 지역별 그룹핑
# ═══════════════════════════════════════════════════

_REGION_KEYWORDS = {
    "US": ["미국", "FOMC", "Fed", "CPI", "고용", "ISM", "GDP", "PCE", "연준", "파월",
           "나스닥", "S&P", "다우", "월가"],
    "JP": ["일본", "BOJ", "엔화", "닛케이", "도쿄"],
    "EU": ["유럽", "ECB", "독일", "프랑스", "유로존", "영란은행", "BOE"],
    "CN": ["중국", "PBOC", "위안", "항셍", "상하이"],
}

_REGION_FLAG = {"US": "🇺🇸 미국", "JP": "🇯🇵 일본", "EU": "🇪🇺 유럽", "CN": "🇨🇳 중국", "OTHER": "🌏 기타"}
_REGION_ORDER = ["US", "JP", "EU", "CN", "OTHER"]


def _classify_region(event_text: str) -> str:
    """이벤트 텍스트에서 지역 분류"""
    for region, keywords in _REGION_KEYWORDS.items():
        if any(kw in event_text for kw in keywords):
            return region
    return "OTHER"


def _resolve_name(ticker: str) -> str:
    """종목코드 → 이름 매핑 (US_KR_SECTOR_MAP 기반)"""
    for info in US_KR_SECTOR_MAP.values():
        for t, name, *_ in info.get("kr_stocks", []):
            if t == ticker:
                return name
    return ticker


def format_telegram_message(result: Dict) -> str:
    """스캔 결과 → 텔레그램 메시지 (지역별 그룹핑)"""
    lines = [
        "━" * 24,
        "🌍 글로벌 이벤트 캘린더 (6-Source)",
        "━" * 24,
    ]

    # ── 매크로 지표 (BOK + Alpha Vantage) ──
    macro = result.get("macro_indicators", {})
    gm = result.get("global_markets", {})
    if macro or gm:
        lines.append("")
        lines.append("📊 매크로 지표")
        lines.append("─" * 24)
        parts = []
        # BOK 데이터
        if "base_rate" in macro:
            parts.append(f"기준금리 {macro['base_rate']['value']}%")
        if "usd_krw" in macro:
            parts.append(f"원/달러 {macro['usd_krw']['value']:,.0f}")
        elif "usd_krw" in gm:
            parts.append(f"원/달러 {gm['usd_krw']['value']:,.0f}")
        if "cpi_yoy" in macro:
            parts.append(f"CPI {macro['cpi_yoy']['value']}%")
        if parts:
            lines.append(f"  {' | '.join(parts)}")
        # Alpha Vantage 원자재
        parts2 = []
        if "wti" in gm:
            parts2.append(f"WTI ${gm['wti']['value']:.2f}")
        if "natural_gas" in gm:
            parts2.append(f"NG ${gm['natural_gas']['value']:.2f}")
        if parts2:
            lines.append(f"  {' | '.join(parts2)}")

    # ── 경제 이벤트를 지역별로 분류 ──
    economic = result.get("economic", [])
    econ_list = [e for e in economic if isinstance(e, dict) and "event" in e]
    region_events: Dict[str, list] = {r: [] for r in _REGION_ORDER}

    for e in econ_list:
        ev_text = e.get("event", "")
        region = _classify_region(ev_text)
        region_events[region].append(e)

    # ── 미국 실적발표를 US 그룹에 병합 ──
    earnings = result.get("earnings", [])

    # ── 지역별 섹션 출력 ──
    for region in _REGION_ORDER:
        evts = region_events[region]
        us_earnings = earnings if region == "US" else []
        total = len(evts) + len(us_earnings)
        if total == 0:
            continue

        flag = _REGION_FLAG[region]
        lines.append("")
        lines.append(f"{flag} ({total}건)")
        lines.append("─" * 24)

        # 경제 이벤트
        for e in evts:
            imp = {"HIGH": "🔴", "MEDIUM": "🟡", "LOW": "⚪"}.get(
                e.get("impact", ""), "⚪"
            )
            date_str = e.get("date", "?")
            if len(date_str) >= 10:
                date_str = date_str[5:]  # "2026-03-17" → "03-17"
            lines.append(f"  {imp} {date_str} {e.get('event', '?')}")

        # 미국 실적발표 (US 그룹에만)
        if us_earnings:
            lines.append("  📊 실적발표")
            for e in us_earnings:
                d = e["days_until"]
                urgency = "🔴" if d <= 1 else "🟡" if d <= 3 else "⚪"
                lines.append(f"    {urgency} D-{d} | {e['name']}({e['symbol']}) → {e['earnings_date']}")

            # D-3 이내 수혜 종목 요약
            alerts = [e for e in us_earnings if e["days_until"] <= 3]
            if alerts:
                kr_names = []
                seen = set()
                for a in alerts:
                    for t, n, r, m in a.get("kr_stocks", [])[:3]:
                        if n not in seen:
                            kr_names.append(n)
                            seen.add(n)
                if kr_names:
                    lines.append(f"  → 수혜: {', '.join(kr_names[:5])}")

    # ── Perplexity AI 분석 ──
    alerts = result.get("alerts", [])
    if alerts and alerts[0].get("perplexity_analysis"):
        analysis = alerts[0]["perplexity_analysis"]
        if len(analysis) > 500:
            analysis = analysis[:497] + "..."
        lines.append("")
        lines.append("🤖 AI 분석 (Perplexity)")
        lines.append("─" * 24)
        lines.append(analysis)

    # ── 실시간 뉴스 (Tavily) ──
    news = result.get("news", {})
    articles = news.get("articles", [])[:5]
    if articles:
        lines.append("")
        lines.append("📰 실시간 뉴스 (Tavily)")
        lines.append("─" * 24)
        if news.get("answer"):
            answer = news["answer"]
            if len(answer) > 300:
                answer = answer[:297] + "..."
            lines.append(f"  {answer}")
            lines.append("")
        for a in articles:
            title = a.get("title", "")[:50]
            lines.append(f"  • {title}")

    # ── 한국 수혜주 종합 TOP 5 ──
    bene = result.get("kr_beneficiaries", [])[:5]
    if bene:
        lines.append("")
        lines.append("🇰🇷 한국 수혜주 종합 TOP 5")
        lines.append("─" * 24)
        for i, b in enumerate(bene, 1):
            name = b["name"] if b["name"] != b["ticker"] else _resolve_name(b["ticker"])
            evts = " + ".join(b["events"][:2])
            lines.append(f"  {i}. {name}({b['ticker']}) {b['total_relevance']}점")
            lines.append(f"     {evts}")

    lines.append("")
    lines.append("━" * 24)
    lines.append("Prophet 예언자 | 해외 이벤트 드리븐")

    return "\n".join(lines)


# ═══════════════════════════════════════════════════
#  6. 로드 유틸 (swing_picker 등에서 호출)
# ═══════════════════════════════════════════════════

def load_global_events() -> Optional[Dict]:
    """저장된 global_events.json 로드"""
    if not CALENDAR_PATH.exists():
        return None
    try:
        with open(CALENDAR_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError):
        return None


def get_event_bonus(code: str) -> float:
    """종목코드에 대한 이벤트 보너스 점수 (0~100)

    swing_picker에서 호출하여 이벤트 수혜주에 가산점 부여
    """
    data = load_global_events()
    if not data:
        return 0.0

    for b in data.get("kr_beneficiaries", []):
        if b["ticker"] == code:
            # D-3 이내 알림이 있으면 추가 보너스
            has_alert = any(
                code in str(a.get("kr_stocks", []))
                for a in data.get("alerts", [])
            )
            base = min(b["total_relevance"], 100)
            return base * 1.3 if has_alert else base

    return 0.0


# ═══════════════════════════════════════════════════
#  CLI
# ═══════════════════════════════════════════════════

if __name__ == "__main__":
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

    # .env 로드
    sys.path.insert(0, str(BASE_DIR))
    from dotenv import load_dotenv
    load_dotenv(BASE_DIR.parent / ".env")

    logging.basicConfig(level=logging.WARNING)

    # ── --verify: API 키 전체 검증 ──
    if "--verify" in sys.argv:
        print("=" * 60)
        print("  API 연결 검증 (A-4 풀스캔)")
        print("=" * 60)
        results = verify_all_apis()
        for name, (status, detail) in results.items():
            icon = {"OK": "✅", "FAIL": "❌", "ERROR": "⚠️", "NO_KEY": "⬜"}.get(status, "?")
            print(f"  {icon} {name:20s} {status:7s} {detail}")
        ok = sum(1 for s, _ in results.values() if s == "OK")
        total = len(results)
        print(f"\n  결과: {ok}/{total} 정상")
        sys.exit(0)

    use_tg = "--telegram" in sys.argv

    result = scan_global_events(days_ahead=7, use_perplexity=True)

    # 결과 출력
    print(f"\n{'='*60}")
    print(f"  실적 이벤트: {len(result['earnings'])}개")
    print(f"  경제 이벤트: {len(result['economic'])}개")
    print(f"  매크로 지표: {len(result.get('macro_indicators', {}))}개")
    print(f"  글로벌 시장: {len(result.get('global_markets', {}))}개")
    print(f"  뉴스 기사: {len(result.get('news', {}).get('articles', []))}개")
    print(f"  D-3 알림: {len(result['alerts'])}개")
    print(f"  한국 수혜주: {len(result['kr_beneficiaries'])}개")
    print(f"{'='*60}")

    if use_tg:
        msg = format_telegram_message(result)
        token = os.getenv("TELEGRAM_BOT_TOKEN")
        chat_id = os.getenv("TELEGRAM_CHAT_ID")
        if token and chat_id:
            url = f"https://api.telegram.org/bot{token}/sendMessage"
            resp = requests.post(url, json={"chat_id": chat_id, "text": msg}, timeout=15)
            print(f"텔레그램 전송: {'OK' if resp.status_code == 200 else 'FAIL'}")
        else:
            print(msg)
    else:
        msg = format_telegram_message(result)
        print(msg)
