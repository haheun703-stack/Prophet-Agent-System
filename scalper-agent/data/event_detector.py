# -*- coding: utf-8 -*-
"""
이벤트 감지기 - DART 공시 + 네이버 뉴스 테마 스캔
====================================================
DART 전자공시에서 자사주/대규모수주/임상 등을 자동 분류하고,
네이버 뉴스에서 10대 테마 키워드를 스캔하여 수혜주를 매칭한다.

파이프라인:
  1. DART API → 공시 분류 → 직접 수혜주
  2. 네이버 뉴스 → 테마 감지 → 간접 수혜주 (BENEFICIARY_DB)
  3. 수혜주 스코어 집계 → 스윙 파이프라인 전달

사용법:
  python -m data.event_detector                    # 전체 스캔
  python -m data.event_detector --dart-only        # DART만
  python -m data.event_detector --news-only        # 뉴스만
  python -m data.event_detector --telegram         # 텔레그램 전송

환경변수:
  DART_API_KEY - DART OpenAPI 인증키 (opendart.fss.or.kr)
"""

import os
import sys
import io
import json
import logging
import time
import re
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Optional

import requests

logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data_store"
MACRO_THEMES_PATH = DATA_DIR / "macro_themes.json"


# ═══════════════════════════════════════════════════
#  공시 분류 규칙
# ═══════════════════════════════════════════════════

EVENT_RULES = [
    # (keywords, event_type, impact, direction)
    (["자기주식취득", "자사주취득", "자사주매입", "자기주식 취득"],
     "TREASURY_BUY", 85, "POSITIVE"),
    (["자기주식소각", "자사주소각"],
     "TREASURY_CANCEL", 90, "POSITIVE"),
    (["자기주식처분", "자사주처분"],
     "TREASURY_SELL", 60, "NEGATIVE"),
    (["단일판매공급계약", "대규모공급계약", "수주공시", "대규모 수주"],
     "BIG_CONTRACT", 80, "POSITIVE"),
    (["임상", "FDA", "식약처"],
     "CLINICAL_TRIAL", 80, "POSITIVE"),
    (["특허취득", "특허등록"],
     "PATENT", 60, "POSITIVE"),
    (["최대주주변경"],
     "OWNER_CHANGE", 75, "NEUTRAL"),
    (["유상증자"],
     "RIGHTS_ISSUE", 70, "NEGATIVE"),
    (["무상증자"],
     "FREE_ISSUE", 65, "POSITIVE"),
    (["합병", "흡수합병"],
     "MERGER", 70, "NEUTRAL"),
    (["분할", "물적분할", "인적분할"],
     "SPLIT", 65, "NEGATIVE"),
    (["영업정지", "상장폐지", "감사의견거절"],
     "DELISTING_RISK", 95, "NEGATIVE"),
    (["잠정실적", "영업이익"],
     "EARNINGS", 70, "NEUTRAL"),
]


# ═══════════════════════════════════════════════════
#  10대 테마 키워드
# ═══════════════════════════════════════════════════

THEME_KEYWORDS = {
    "상법개정": {
        "keywords": ["상법 개정", "자사주 소각", "자사주 매입", "주주환원", "배당 확대"],
        "impact": 85,
        "direction": "POSITIVE",
        "tag": "treasury_stock",
    },
    "반도체슈퍼사이클": {
        "keywords": ["HBM", "AI 반도체", "HBM4", "고대역폭", "엔비디아 실적", "파운드리"],
        "impact": 80,
        "direction": "POSITIVE",
        "tag": "semiconductor",
    },
    "원전르네상스": {
        "keywords": ["원전 수출", "SMR", "소형모듈원전", "원전 재가동", "체코 원전"],
        "impact": 75,
        "direction": "POSITIVE",
        "tag": "nuclear",
    },
    "방산수주": {
        "keywords": ["방산 수출", "K-방산", "K2 전차", "FA-50", "천무", "K9 자주포"],
        "impact": 75,
        "direction": "POSITIVE",
        "tag": "defense",
    },
    "AI투자": {
        "keywords": ["AI 투자", "데이터센터", "AI 인프라", "클라우드 투자", "GPU 수요", "AI 서버"],
        "impact": 80,
        "direction": "POSITIVE",
        "tag": "ai",
    },
    "금리인하": {
        "keywords": ["금리 인하", "기준금리 인하", "Fed 금리", "한은 금리", "피벗"],
        "impact": 70,
        "direction": "POSITIVE",
        "tag": "rate_sensitive",
    },
    "금리인상경고": {
        "keywords": ["금리 인상", "금리 동결 장기화", "인플레이션 우려", "스태그플레이션"],
        "impact": 80,
        "direction": "NEGATIVE",
        "tag": "market_risk",
    },
    "관세리스크": {
        "keywords": ["관세 부과", "무역전쟁", "반도체 관세", "자동차 관세"],
        "impact": 65,
        "direction": "NEGATIVE",
        "tag": "tariff_risk",
    },
    "MSCI선진국": {
        "keywords": ["MSCI 선진국", "MSCI 편입", "모건스탠리 지수"],
        "impact": 85,
        "direction": "POSITIVE",
        "tag": "msci",
    },
    "바이오임상": {
        "keywords": ["FDA 승인", "임상 3상", "신약 허가", "긴급사용승인", "블록버스터"],
        "impact": 80,
        "direction": "POSITIVE",
        "tag": "bio",
    },
}


# ═══════════════════════════════════════════════════
#  수혜주 DB (테마 → 종목)
# ═══════════════════════════════════════════════════

BENEFICIARY_DB = {
    "treasury_stock": [
        ("085620", "미래에셋생명", 95, "자사주 26.3%"),
        ("088350", "한화생명", 90, "자사주 18.5%"),
        ("005830", "DB손해보험", 85, "자사주 14.5%"),
        ("032830", "삼성생명", 85, "자사주 10.8%"),
        ("000810", "삼성화재", 80, "자사주 9.5%"),
        ("001450", "현대해상", 78, "자사주 8.2%"),
        ("003690", "코리안리", 75, "자사주 7.8%"),
        ("005930", "삼성전자", 65, "자사주 5.2%"),
    ],
    "semiconductor": [
        ("000660", "SK하이닉스", 95, "HBM 세계 1위"),
        ("005930", "삼성전자", 90, "메모리+파운드리"),
        ("403870", "HPSP", 80, "반도체 장비"),
        ("058470", "리노공업", 78, "반도체 테스트"),
        ("042700", "한미반도체", 75, "패키징 장비"),
    ],
    "nuclear": [
        ("034020", "두산에너빌리티", 90, "원전 핵심 기자재"),
        ("000720", "현대건설", 80, "원전 건설"),
    ],
    "defense": [
        ("012450", "한화에어로스페이스", 90, "항공엔진+방산"),
        ("047810", "한국항공우주", 85, "FA-50 항공기"),
    ],
    "ai": [
        ("035420", "NAVER", 85, "AI+클라우드"),
        ("035720", "카카오", 75, "AI+플랫폼"),
        ("012510", "더존비즈온", 70, "AI ERP"),
    ],
    "bio": [
        ("207940", "삼성바이오로직스", 85, "CMO 세계 1위"),
        ("068270", "셀트리온", 80, "바이오시밀러"),
    ],
    "rate_sensitive": [
        ("005830", "DB손해보험", 70, "보험"),
        ("088350", "한화생명", 70, "생명보험"),
    ],
    "msci": [
        ("005930", "삼성전자", 90, "MSCI 대형주"),
        ("000660", "SK하이닉스", 85, "MSCI 편입 수혜"),
    ],
    "market_risk": [],   # 리스크 경고 (매수 수혜주 없음)
    "tariff_risk": [],   # 리스크 경고
}


# ═══════════════════════════════════════════════════
#  매크로 테마 동적 로드
# ═══════════════════════════════════════════════════

def load_macro_themes():
    """macro_themes.json에서 ACTIVE 테마를 THEME_KEYWORDS/BENEFICIARY_DB에 merge"""
    if not MACRO_THEMES_PATH.exists():
        return 0

    try:
        with open(MACRO_THEMES_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (json.JSONDecodeError, IOError) as e:
        logger.warning(f"macro_themes.json 로드 실패: {e}")
        return 0

    merged = 0
    for theme in data.get("themes", []):
        if theme.get("status") != "ACTIVE":
            continue

        tag = theme.get("tag", "")
        name = theme.get("name", tag)
        if not tag:
            continue

        # THEME_KEYWORDS에 추가 (이미 있으면 스킵)
        if name not in THEME_KEYWORDS:
            THEME_KEYWORDS[name] = {
                "keywords": theme.get("keywords", []),
                "impact": theme.get("impact", 70),
                "direction": theme.get("direction", "NEUTRAL"),
                "tag": tag,
            }

        # BENEFICIARY_DB에 추가
        if tag not in BENEFICIARY_DB:
            BENEFICIARY_DB[tag] = []

        existing_tickers = {t[0] for t in BENEFICIARY_DB[tag]}
        for b in theme.get("beneficiaries", []):
            if b["ticker"] not in existing_tickers:
                BENEFICIARY_DB[tag].append(
                    (b["ticker"], b["name"], b["relevance"], b["metric"])
                )

        merged += 1

    if merged:
        logger.info(f"매크로 테마 {merged}개 merge 완료")
    return merged


def get_macro_themes() -> list:
    """macro_themes.json 전체 반환 (텔레그램 명령용)"""
    if not MACRO_THEMES_PATH.exists():
        return []
    try:
        with open(MACRO_THEMES_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data.get("themes", [])
    except (json.JSONDecodeError, IOError):
        return []


def update_macro_theme_status(theme_id: str, new_status: str) -> bool:
    """테마 상태 변경 (ACTIVE/WATCH/ARCHIVE)"""
    if new_status not in ("ACTIVE", "WATCH", "ARCHIVE"):
        return False
    if not MACRO_THEMES_PATH.exists():
        return False
    try:
        with open(MACRO_THEMES_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        for theme in data.get("themes", []):
            if theme.get("id") == theme_id:
                theme["status"] = new_status
                data["_meta"]["updated_at"] = datetime.now().strftime("%Y-%m-%d")
                with open(MACRO_THEMES_PATH, "w", encoding="utf-8") as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
                return True
        return False
    except (json.JSONDecodeError, IOError):
        return False


def add_macro_theme(name: str, keywords: list, tag: str,
                    impact: int = 75, direction: str = "POSITIVE",
                    beneficiaries: list = None) -> str:
    """새 매크로 테마 추가, theme_id 반환"""
    theme_id = tag + "_" + datetime.now().strftime("%Y%m%d")

    if MACRO_THEMES_PATH.exists():
        with open(MACRO_THEMES_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
    else:
        data = {"_meta": {"description": "매크로 테마 시나리오", "updated_at": "", "usage": "ACTIVE만 merge"}, "themes": []}

    new_theme = {
        "id": theme_id,
        "name": name,
        "status": "ACTIVE",
        "created": datetime.now().strftime("%Y-%m-%d"),
        "keywords": keywords,
        "impact": impact,
        "direction": direction,
        "tag": tag,
        "beneficiaries": beneficiaries or [],
    }
    data["themes"].append(new_theme)
    data["_meta"]["updated_at"] = datetime.now().strftime("%Y-%m-%d")

    with open(MACRO_THEMES_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    return theme_id


def remove_macro_theme(theme_id: str) -> bool:
    """테마 삭제"""
    if not MACRO_THEMES_PATH.exists():
        return False
    try:
        with open(MACRO_THEMES_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        before = len(data.get("themes", []))
        data["themes"] = [t for t in data.get("themes", []) if t.get("id") != theme_id]
        if len(data["themes"]) == before:
            return False
        data["_meta"]["updated_at"] = datetime.now().strftime("%Y-%m-%d")
        with open(MACRO_THEMES_PATH, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        return True
    except (json.JSONDecodeError, IOError):
        return False


# ═══════════════════════════════════════════════════
#  DART 공시 스캔
# ═══════════════════════════════════════════════════

def fetch_dart_disclosures(days_back: int = 3) -> list:
    """DART OpenAPI에서 최근 공시 수집

    Returns: [{corp_name, ticker, report_nm, rcept_dt, event_type, impact, direction}]
    """
    api_key = os.getenv("DART_API_KEY", "")
    if not api_key:
        logger.info("DART_API_KEY 미설정 - 공시 스캔 건너뜀")
        return []

    end_date = datetime.now().strftime("%Y%m%d")
    start_date = (datetime.now() - timedelta(days=days_back)).strftime("%Y%m%d")

    url = "https://opendart.fss.or.kr/api/list.json"
    all_disclosures = []

    for page in range(1, 4):  # 최대 3페이지
        params = {
            "crtfc_key": api_key,
            "bgn_de": start_date,
            "end_de": end_date,
            "page_no": page,
            "page_count": 100,
            "sort": "date",
            "sort_mth": "desc",
        }

        try:
            resp = requests.get(url, params=params, timeout=10)
            data = resp.json()
            if data.get("status") != "000":
                break

            items = data.get("list", [])
            if not items:
                break

            for item in items:
                report_nm = item.get("report_nm", "")
                classified = _classify_event(report_nm)

                if classified:
                    all_disclosures.append({
                        "corp_name": item.get("corp_name", ""),
                        "ticker": item.get("stock_code", ""),
                        "report_nm": report_nm,
                        "rcept_dt": item.get("rcept_dt", ""),
                        "event_type": classified["event_type"],
                        "impact": classified["impact"],
                        "direction": classified["direction"],
                        "source": "DART",
                    })

            time.sleep(0.3)

        except Exception as e:
            logger.error(f"DART API 오류: {e}")
            break

    logger.info(f"DART 공시: {len(all_disclosures)}건 감지")
    return all_disclosures


def _classify_event(report_title: str) -> Optional[dict]:
    """공시 제목 → 이벤트 분류"""
    for keywords, event_type, impact, direction in EVENT_RULES:
        for kw in keywords:
            if kw in report_title:
                return {
                    "event_type": event_type,
                    "impact": impact,
                    "direction": direction,
                }
    return None


# ═══════════════════════════════════════════════════
#  네이버 뉴스 테마 스캔
# ═══════════════════════════════════════════════════

def scan_naver_news() -> list:
    """네이버 뉴스에서 10대 테마 키워드 스캔

    Returns: [{theme, keyword, news_count, impact, direction, tag, source}]
    """
    detected = []
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }

    for theme_name, theme_info in THEME_KEYWORDS.items():
        for keyword in theme_info["keywords"]:
            try:
                url = "https://search.naver.com/search.naver"
                params = {
                    "where": "news",
                    "query": keyword,
                    "sort": "1",   # 최신순
                    "pd": "4",     # 1주일
                    "start": "1",
                }

                resp = requests.get(url, params=params, headers=headers, timeout=8)
                if resp.status_code != 200:
                    continue

                # 뉴스 건수 추출 (간단한 정규식)
                count_match = re.search(r'약 ([\d,]+)건', resp.text)
                if not count_match:
                    count_match = re.search(r'([\d,]+)건', resp.text)
                news_count = int(count_match.group(1).replace(",", "")) if count_match else 0

                # 최소 3건 이상이면 테마 활성
                if news_count >= 3:
                    detected.append({
                        "theme": theme_name,
                        "keyword": keyword,
                        "news_count": news_count,
                        "impact": theme_info["impact"],
                        "direction": theme_info["direction"],
                        "tag": theme_info["tag"],
                        "source": "NAVER_NEWS",
                        "date": datetime.now().strftime("%Y%m%d"),
                    })
                    break  # 테마당 1개 키워드만

                time.sleep(0.5)

            except Exception as e:
                logger.warning(f"뉴스 스캔 실패 ({keyword}): {e}")
                continue

    logger.info(f"네이버 뉴스: {len(detected)}개 테마 감지")
    return detected


# ═══════════════════════════════════════════════════
#  수혜주 매칭
# ═══════════════════════════════════════════════════

def match_beneficiaries(events: list) -> list:
    """이벤트 → 수혜주 매칭 & 스코어 집계

    Returns: [{ticker, name, total_score, direction, events, metric}] 정렬됨
    """
    # ticker별 집계
    ticker_data = {}  # ticker → {name, scores[], events[], directions[]}

    for evt in events:
        if evt["source"] == "DART" and evt.get("ticker"):
            # DART 직접 수혜
            ticker = evt["ticker"]
            if ticker not in ticker_data:
                ticker_data[ticker] = {
                    "name": evt.get("corp_name", ticker),
                    "scores": [],
                    "events": [],
                    "directions": [],
                    "metric": "",
                }
            ticker_data[ticker]["scores"].append(evt["impact"])
            ticker_data[ticker]["events"].append(f'{evt["event_type"]}')
            ticker_data[ticker]["directions"].append(evt["direction"])

        elif evt["source"] == "NAVER_NEWS":
            # 뉴스 간접 수혜 → BENEFICIARY_DB 매칭
            tag = evt.get("tag", "")
            beneficiaries = BENEFICIARY_DB.get(tag, [])
            for ticker, name, relevance, metric in beneficiaries:
                score = evt["impact"] * relevance / 100
                if ticker not in ticker_data:
                    ticker_data[ticker] = {
                        "name": name,
                        "scores": [],
                        "events": [],
                        "directions": [],
                        "metric": metric,
                    }
                ticker_data[ticker]["scores"].append(score)
                ticker_data[ticker]["events"].append(f'THEME:{evt["theme"]}')
                ticker_data[ticker]["directions"].append(evt["direction"])
                if not ticker_data[ticker]["metric"]:
                    ticker_data[ticker]["metric"] = metric

    # 집계
    results = []
    for ticker, data in ticker_data.items():
        total = sum(data["scores"])
        dirs = set(data["directions"])
        if "POSITIVE" in dirs and "NEGATIVE" in dirs:
            direction = "MIXED"
        elif "NEGATIVE" in dirs:
            direction = "NEGATIVE"
        elif "POSITIVE" in dirs:
            direction = "POSITIVE"
        else:
            direction = "NEUTRAL"

        results.append({
            "ticker": ticker,
            "name": data["name"],
            "total_score": round(total, 1),
            "direction": direction,
            "events": list(set(data["events"])),
            "metric": data["metric"],
        })

    results.sort(key=lambda x: -x["total_score"])
    return results


# ═══════════════════════════════════════════════════
#  메인 스캔 + 저장
# ═══════════════════════════════════════════════════

def run_event_scan(scan_dart: bool = True, scan_news: bool = True) -> dict:
    """이벤트 스캔 실행

    Returns: {scanned_at, events[], beneficiaries[]}
    """
    print(f"\n🛰  이벤트 감지기")
    print(f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 50)

    # 0. 매크로 테마 동적 로드
    merged = load_macro_themes()
    if merged:
        print(f"  매크로 테마 {merged}개 로드 (ACTIVE)")

    all_events = []

    # 1. DART 공시
    if scan_dart:
        print("\n[1] DART 공시 스캔...")
        dart_events = fetch_dart_disclosures(days_back=3)
        all_events.extend(dart_events)
        for evt in dart_events:
            icon = {"POSITIVE": "+", "NEGATIVE": "-", "NEUTRAL": "~"}.get(evt["direction"], "?")
            print(f"  [{icon}] {evt['corp_name']} - {evt['event_type']} ({evt['impact']})")

    # 2. 네이버 뉴스
    if scan_news:
        print("\n[2] 네이버 뉴스 테마 스캔...")
        news_events = scan_naver_news()
        all_events.extend(news_events)
        for evt in news_events:
            icon = {"POSITIVE": "+", "NEGATIVE": "-"}.get(evt["direction"], "~")
            print(f"  [{icon}] {evt['theme']} - '{evt['keyword']}' ({evt['news_count']}건)")

    # 3. 수혜주 매칭
    print(f"\n[3] 수혜주 매칭...")
    beneficiaries = match_beneficiaries(all_events)

    print(f"  이벤트: {len(all_events)}건 | 수혜주: {len(beneficiaries)}종목")

    if beneficiaries:
        print(f"\n  ━━ 수혜주 TOP 10 ━━")
        for i, b in enumerate(beneficiaries[:10], 1):
            icon = {"POSITIVE": "+", "NEGATIVE": "-", "MIXED": "~"}.get(b["direction"], "?")
            print(f"  {i:>2}. [{icon}] {b['name']}({b['ticker']}) - {b['total_score']:.0f}점 | {', '.join(b['events'])}")

    # 저장
    result = {
        "scanned_at": datetime.now().isoformat(),
        "total_events": len(all_events),
        "events": all_events,
        "beneficiaries": beneficiaries,
    }

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    event_path = DATA_DIR / "events.json"
    with open(event_path, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    print(f"\n  저장: {event_path}")

    return result


def format_event_report(result: dict) -> str:
    """텔레그램용 리포트 포맷"""
    lines = []
    lines.append("🛰 이벤트 감지기 리포트")
    lines.append(f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    lines.append("━━━━━━━━━━━━━━━━━━━")

    # DART 공시
    dart = [e for e in result["events"] if e["source"] == "DART"]
    if dart:
        lines.append(f"\n📋 DART 공시 ({len(dart)}건)")
        for e in dart[:5]:
            icon = {"POSITIVE": "🟢", "NEGATIVE": "🔴", "NEUTRAL": "🟡"}.get(e["direction"], "⚪")
            lines.append(f"  {icon} {e['corp_name']} - {e['event_type']}")

    # 테마 뉴스
    news = [e for e in result["events"] if e["source"] == "NAVER_NEWS"]
    if news:
        lines.append(f"\n📰 테마 뉴스 ({len(news)}개)")
        for e in news:
            icon = {"POSITIVE": "🟢", "NEGATIVE": "🔴"}.get(e["direction"], "🟡")
            lines.append(f"  {icon} {e['theme']} - '{e['keyword']}' ({e['news_count']}건)")

    # 수혜주
    bens = result.get("beneficiaries", [])
    if bens:
        lines.append(f"\n🎯 수혜주 TOP 5")
        for i, b in enumerate(bens[:5], 1):
            icon = {"POSITIVE": "🟢", "NEGATIVE": "🔴", "MIXED": "🟡"}.get(b["direction"], "⚪")
            events_str = ", ".join(b["events"][:2])
            lines.append(f"  {i}. {icon} {b['name']}({b['ticker']}) - {b['total_score']:.0f}점")
            lines.append(f"     {events_str}")

    lines.append("\n━━━━━━━━━━━━━━━━━━━")
    return "\n".join(lines)


# ═══════════════════════════════════════════════════
#  CLI
# ═══════════════════════════════════════════════════

if __name__ == "__main__":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
    logging.basicConfig(level=logging.WARNING)

    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parent.parent.parent / ".env")

    import argparse
    parser = argparse.ArgumentParser(description="이벤트 감지기")
    parser.add_argument("--dart-only", action="store_true", help="DART만")
    parser.add_argument("--news-only", action="store_true", help="뉴스만")
    parser.add_argument("--telegram", action="store_true", help="텔레그램 전송")
    args = parser.parse_args()

    scan_dart = not args.news_only
    scan_news = not args.dart_only

    result = run_event_scan(scan_dart=scan_dart, scan_news=scan_news)

    if args.telegram and result["beneficiaries"]:
        try:
            from bot.telegram_bot import send_message
            msg = format_event_report(result)
            send_message(msg)
            print("\n  텔레그램 전송 완료")
        except Exception as e:
            print(f"\n  텔레그램 전송 실패: {e}")
