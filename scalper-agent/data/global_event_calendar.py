# -*- coding: utf-8 -*-
"""
해외 이벤트 캘린더 — 실적발표/FOMC/CPI 사전 감지 + 한국 수혜주 매핑
=====================================================================
D-3 사전 알림 → 한국 관련 섹터 종목 가중치 부여

데이터 소스:
  1. yfinance — 미국 빅테크 실적발표 일정
  2. Perplexity API — 실시간 웹검색 (향후 1주 이벤트 + 한국 수혜 분석)
  3. 하드코딩 경제 캘린더 — FOMC/CPI/고용 등 정기 일정

사용법:
  python -m data.global_event_calendar              # 전체 스캔
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
        logger.warning("yfinance 미설치 — pip install yfinance")
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
#  2. Perplexity API — 실시간 이벤트 + 한국 수혜 분석
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
    print("  글로벌 이벤트 캘린더 스캔")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 60)

    # 1. 실적 캘린더
    print("\n[1/3] 미국 실적 캘린더 조회...")
    earnings = fetch_earnings_calendar(days_ahead=days_ahead)
    print(f"  → {len(earnings)}개 이벤트 감지")
    for e in earnings:
        print(f"    D-{e['days_until']} | {e['name']}({e['symbol']}) → {e['earnings_date']}")

    # 2. Perplexity 분석 (D-3 이내)
    if use_perplexity and earnings:
        print("\n[2/3] Perplexity AI 분석...")
        earnings = analyze_upcoming_events(earnings)
        print("  → 분석 완료")
    else:
        print("\n[2/3] Perplexity 분석 스킵")

    # 3. 경제 캘린더
    economic = []
    if use_perplexity:
        print("\n[3/3] 경제 캘린더 조회 (Perplexity)...")
        economic = fetch_economic_calendar(days_ahead)
        print(f"  → {len(economic)}개 이벤트")
    else:
        print("\n[3/3] 경제 캘린더 스킵")

    # 4. D-3 알림 생성
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
    }

    # 저장
    with open(CALENDAR_PATH, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2, default=str)
    print(f"\n저장: {CALENDAR_PATH}")

    return result


# ═══════════════════════════════════════════════════
#  5. 텔레그램 메시지 포맷
# ═══════════════════════════════════════════════════

def format_telegram_message(result: Dict) -> str:
    """스캔 결과 → 텔레그램 메시지"""
    lines = [
        "━" * 24,
        "🌍 글로벌 이벤트 캘린더",
        "━" * 24,
    ]

    # 실적 캘린더
    earnings = result.get("earnings", [])
    if earnings:
        lines.append("")
        lines.append("📊 미국 실적발표 일정")
        lines.append("─" * 24)
        for e in earnings:
            d = e["days_until"]
            urgency = "🔴" if d <= 1 else "🟡" if d <= 3 else "⚪"
            lines.append(f"  {urgency} D-{d} | {e['name']}({e['symbol']}) → {e['earnings_date']}")
            if e.get("kr_sectors"):
                lines.append(f"      한국: {', '.join(e['kr_sectors'][:3])}")

    # D-3 알림
    alerts = result.get("alerts", [])
    if alerts:
        lines.append("")
        lines.append("⚠️ D-3 긴급 알림")
        lines.append("─" * 24)
        for a in alerts:
            lines.append(f"  🔔 {a['name']} 실적 D-{a['days_until']}")
            for t, n, r, m in a.get("kr_stocks", [])[:5]:
                lines.append(f"    → {n}({t}) [{m}] 관련도:{r}")

    # Perplexity 분석
    if alerts and alerts[0].get("perplexity_analysis"):
        analysis = alerts[0]["perplexity_analysis"]
        # 500자 이내로 자르기
        if len(analysis) > 500:
            analysis = analysis[:497] + "..."
        lines.append("")
        lines.append("🤖 AI 분석 (Perplexity)")
        lines.append("─" * 24)
        lines.append(analysis)

    # 한국 수혜주 TOP 5
    bene = result.get("kr_beneficiaries", [])[:5]
    if bene:
        lines.append("")
        lines.append("🇰🇷 한국 수혜주 TOP 5")
        lines.append("─" * 24)
        for i, b in enumerate(bene, 1):
            evts = " + ".join(b["events"][:2])
            lines.append(f"  {i}. {b['name']}({b['ticker']}) 점수:{b['total_relevance']}")
            lines.append(f"     {evts}")

    # 경제 캘린더
    economic = result.get("economic", [])
    econ_list = [e for e in economic if isinstance(e, dict) and "event" in e]
    if econ_list:
        lines.append("")
        lines.append("📅 주요 경제 일정")
        lines.append("─" * 24)
        for e in econ_list[:5]:
            imp = {"HIGH": "🔴", "MEDIUM": "🟡", "LOW": "⚪"}.get(
                e.get("impact", ""), "⚪"
            )
            lines.append(f"  {imp} {e.get('date', '?')} {e.get('event', '?')}")

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

    use_tg = "--telegram" in sys.argv

    result = scan_global_events(days_ahead=7, use_perplexity=True)

    # 결과 출력
    print(f"\n{'='*60}")
    print(f"  실적 이벤트: {len(result['earnings'])}개")
    print(f"  경제 이벤트: {len(result['economic'])}개")
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
