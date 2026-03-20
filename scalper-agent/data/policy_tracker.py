# -*- coding: utf-8 -*-
"""
정부 정책 트래커 — 대통령실/부처 정책 수집 + 섹터 매핑
=======================================================
korea.kr RSS + Tavily 웹검색 → 키워드→수혜 섹터/종목 매핑

사용법:
  python -m data.policy_tracker              # 전체 스캔
  python -m data.policy_tracker --telegram   # 텔레그램 포맷
"""

import json
import logging
import os
import re
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

import warnings

import requests

# korea.kr SSL 인증서 이슈 → verify=False 사용
warnings.filterwarnings("ignore", message="Unverified HTTPS request")

logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data_store"
POLICY_PATH = DATA_DIR / "policy_latest.json"

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36"
    )
}

# 캐시 TTL: 6시간
_CACHE_TTL_HOURS = 6


# ═══════════════════════════════════════════════════
#  정책 → 섹터/종목 매핑 DB (10개 섹터)
# ═══════════════════════════════════════════════════

POLICY_SECTOR_MAP = {
    "반도체": {
        "keywords": [
            "반도체", "HBM", "K-Semiconductor", "용인클러스터",
            "시스템반도체", "메모리", "팹", "파운드리",
        ],
        "kr_stocks": [
            ("000660", "SK하이닉스", 95, "HBM+팹투자"),
            ("005930", "삼성전자", 90, "파운드리+메모리"),
            ("042700", "한미반도체", 80, "패키징장비"),
            ("036930", "주성엔지니어링", 70, "ALD장비"),
        ],
    },
    "방산": {
        "keywords": [
            "방위산업", "방산", "K방산", "무기수출",
            "방산예산", "국방비", "국방", "군사",
        ],
        "kr_stocks": [
            ("012450", "한화에어로스페이스", 95, "항공엔진+탄약"),
            ("047810", "한국항공우주", 90, "항공기"),
            ("079550", "LIG넥스원", 85, "미사일"),
            ("064350", "현대로템", 80, "전차+장갑차"),
        ],
    },
    "부동산_건설": {
        "keywords": [
            "부동산", "주택공급", "재건축", "재개발",
            "분양가", "공공주택", "GTX", "SOC", "건설",
        ],
        "kr_stocks": [
            ("000720", "현대건설", 85, "주택+토목"),
            ("047040", "대우건설", 80, "주택"),
            ("006360", "GS건설", 80, "주택"),
        ],
    },
    "원전_에너지": {
        "keywords": [
            "원전", "원자력", "SMR", "소형모듈원전",
            "에너지정책", "탄소중립", "RE100", "신재생",
        ],
        "kr_stocks": [
            ("042670", "두산에너빌리티", 90, "원전주기기"),
            ("298040", "효성중공업", 75, "전력인프라"),
            ("267260", "HD현대일렉트릭", 70, "변압기"),
        ],
    },
    "바이오_헬스": {
        "keywords": [
            "바이오", "제약", "의약품", "K-바이오",
            "신약", "건강보험", "의료", "헬스케어",
        ],
        "kr_stocks": [
            ("207940", "삼성바이오로직스", 90, "CMO"),
            ("068270", "셀트리온", 85, "바이오시밀러"),
            ("128940", "한미약품", 80, "신약"),
        ],
    },
    "AI_디지털": {
        "keywords": [
            "AI", "인공지능", "디지털전환", "데이터센터",
            "클라우드", "디지털뉴딜", "소프트웨어", "ICT",
        ],
        "kr_stocks": [
            ("035420", "NAVER", 85, "AI+클라우드"),
            ("035720", "카카오", 75, "AI"),
            ("018260", "삼성SDS", 70, "IT서비스"),
        ],
    },
    "이차전지": {
        "keywords": [
            "이차전지", "전기차", "배터리", "전고체",
            "양극재", "리튬", "EV",
        ],
        "kr_stocks": [
            ("373220", "LG에너지솔루션", 90, "배터리"),
            ("006400", "삼성SDI", 85, "배터리"),
            ("247540", "에코프로비엠", 80, "양극재"),
        ],
    },
    "조선_해운": {
        "keywords": [
            "조선", "해운", "LNG선", "친환경선박",
            "해양", "수주", "K-조선",
        ],
        "kr_stocks": [
            ("009540", "HD한국조선해양", 90, "대형선박"),
            ("042660", "한화오션", 85, "군함+상선"),
            ("010620", "HD현대미포", 75, "중형선박"),
        ],
    },
    "금융": {
        "keywords": [
            "금융규제", "핀테크", "가계부채", "기준금리",
            "금융지주", "은행", "금융",
        ],
        "kr_stocks": [
            ("105560", "KB금융", 80, "금융지주"),
            ("086790", "하나금융지주", 75, "금융"),
            ("055550", "신한지주", 75, "금융"),
        ],
    },
    "로봇_자동화": {
        "keywords": [
            "로봇", "자동화", "스마트팩토리", "제조혁신",
            "무인화", "물류자동화",
        ],
        "kr_stocks": [
            ("012450", "한화에어로스페이스", 80, "로봇팔"),
            ("298040", "효성중공업", 65, "산업자동화"),
        ],
    },
}


# ═══════════════════════════════════════════════════
#  1. korea.kr RSS 수집
# ═══════════════════════════════════════════════════

_KOREA_KR_RSS_URLS = [
    "https://www.korea.kr/rss/policy.xml",
    "https://www.korea.kr/rss/news.xml",
]


def fetch_korea_kr_rss(days: int = 3, max_items: int = 20) -> List[Dict]:
    """korea.kr 정책브리핑 RSS 파싱 — 최근 N일 기사"""
    articles = []
    cutoff = datetime.now() - timedelta(days=days)
    seen_titles = set()

    for rss_url in _KOREA_KR_RSS_URLS:
        try:
            resp = requests.get(
                rss_url, headers=_HEADERS, timeout=10, verify=False
            )
            resp.encoding = "utf-8"
            xml = resp.text

            # RSS XML 파싱 (news_ai_scanner 패턴)
            titles = re.findall(r'<title><!\[CDATA\[(.*?)\]\]></title>', xml)
            if not titles:
                titles = re.findall(r'<title>(.*?)</title>', xml)

            links = re.findall(r'<link>(https?://[^<]+)</link>', xml)
            descriptions = re.findall(
                r'<description><!\[CDATA\[(.*?)\]\]></description>', xml
            )
            if not descriptions:
                descriptions = re.findall(r'<description>(.*?)</description>', xml)

            pub_dates = re.findall(r'<pubDate>(.*?)</pubDate>', xml)

            # 첫 번째는 피드 제목이므로 스킵
            for i in range(1, min(len(titles), max_items + 1)):
                title = re.sub(r'<[^>]+>', '', titles[i]).strip()
                if not title or title in seen_titles:
                    continue
                seen_titles.add(title)

                link = links[i] if i < len(links) else ""
                desc = descriptions[i] if i < len(descriptions) else ""
                desc = re.sub(r'<[^>]+>', '', desc).strip()[:300]

                # 날짜 파싱 (RFC 822: "Thu, 20 Mar 2026 09:00:00 +0900")
                pub_dt = None
                if i < len(pub_dates):
                    try:
                        # email.utils 대신 간단 파싱
                        raw = pub_dates[i].strip()
                        # "Thu, 20 Mar 2026 09:00:00 +0900" → 날짜 추출
                        date_match = re.search(
                            r'(\d{1,2})\s+(\w{3})\s+(\d{4})', raw
                        )
                        if date_match:
                            day = int(date_match.group(1))
                            month_s = date_match.group(2)
                            year = int(date_match.group(3))
                            months = {
                                "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4,
                                "May": 5, "Jun": 6, "Jul": 7, "Aug": 8,
                                "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12,
                            }
                            month = months.get(month_s, 1)
                            pub_dt = datetime(year, month, day)
                    except Exception:
                        pass

                # 날짜 필터: 최근 N일만
                if pub_dt and pub_dt < cutoff:
                    continue

                articles.append({
                    "title": title,
                    "link": link,
                    "description": desc,
                    "pub_date": pub_dt.strftime("%Y-%m-%d") if pub_dt else "",
                    "source": "korea.kr",
                })

        except Exception as e:
            logger.warning(f"korea.kr RSS 실패 ({rss_url}): {e}")

    logger.info(f"korea.kr RSS: {len(articles)}건 수집")
    return articles


# ═══════════════════════════════════════════════════
#  2. Tavily 웹검색 — 정책 뉴스 보강
# ═══════════════════════════════════════════════════

_TAVILY_QUERIES = [
    "대통령실 정책 발표 경제 산업",
    "정부 산업정책 지원 반도체 방산 에너지",
]


def fetch_policy_news_tavily(max_results: int = 6) -> List[Dict]:
    """Tavily로 정책 관련 뉴스 검색"""
    api_key = os.getenv("TAVILY_API_KEY", "").strip()
    if not api_key:
        logger.info("TAVILY_API_KEY 미설정 → 정책 Tavily 스킵")
        return []

    articles = []
    seen_titles = set()

    for query in _TAVILY_QUERIES:
        try:
            resp = requests.post(
                "https://api.tavily.com/search",
                json={
                    "api_key": api_key,
                    "query": query,
                    "search_depth": "basic",
                    "max_results": max_results,
                    "include_answer": False,
                },
                timeout=15,
            )
            resp.raise_for_status()
            data = resp.json()

            for item in data.get("results", []):
                title = item.get("title", "").strip()
                if not title or title in seen_titles:
                    continue
                seen_titles.add(title)
                articles.append({
                    "title": title,
                    "link": item.get("url", ""),
                    "description": item.get("content", "")[:300],
                    "pub_date": "",
                    "source": "tavily",
                })

            time.sleep(0.5)
        except Exception as e:
            logger.warning(f"Tavily 정책검색 실패 ({query}): {e}")

    logger.info(f"Tavily 정책: {len(articles)}건 수집")
    return articles


# ═══════════════════════════════════════════════════
#  3. 키워드 매칭 — 정책 → 수혜 섹터/종목
# ═══════════════════════════════════════════════════


def match_policy_sectors(articles: List[Dict]) -> List[Dict]:
    """기사 제목+설명에서 POLICY_SECTOR_MAP 키워드 매칭"""
    sector_scores: Dict[str, Dict] = {}

    for article in articles:
        text = f"{article.get('title', '')} {article.get('description', '')}"

        for sector, info in POLICY_SECTOR_MAP.items():
            found_keywords = []
            for kw in info["keywords"]:
                if kw in text:
                    found_keywords.append(kw)

            if not found_keywords:
                continue

            if sector not in sector_scores:
                sector_scores[sector] = {
                    "sector": sector,
                    "keywords_found": set(),
                    "article_count": 0,
                    "relevance": 0,
                    "kr_stocks": info["kr_stocks"],
                    "articles": [],
                }

            ss = sector_scores[sector]
            ss["keywords_found"].update(found_keywords)
            ss["article_count"] += 1
            # 점수: 키워드 수 x 10 + 기사 수 x 5
            ss["relevance"] = len(ss["keywords_found"]) * 10 + ss["article_count"] * 5
            ss["articles"].append({
                "title": article["title"][:60],
                "source": article.get("source", ""),
            })

    # set → list 변환 + 정렬
    matched = []
    for ss in sector_scores.values():
        ss["keywords_found"] = list(ss["keywords_found"])
        ss["articles"] = ss["articles"][:5]  # 최대 5건
        matched.append(ss)

    matched.sort(key=lambda x: -x["relevance"])
    return matched


# ═══════════════════════════════════════════════════
#  4. 메인 스캔
# ═══════════════════════════════════════════════════


def scan_policy(days: int = 3) -> Dict:
    """정책 트래커 메인 — korea.kr + Tavily → 키워드 매칭 → 저장"""
    # .env 로드
    if not os.getenv("TAVILY_API_KEY"):
        try:
            from dotenv import load_dotenv
            load_dotenv(BASE_DIR.parent / ".env")
        except ImportError:
            pass

    # 캐시 체크
    if POLICY_PATH.exists():
        try:
            with open(POLICY_PATH, "r", encoding="utf-8") as f:
                cached = json.load(f)
            scanned_at = cached.get("scanned_at", "")
            if scanned_at:
                cached_time = datetime.fromisoformat(scanned_at)
                if (datetime.now() - cached_time).total_seconds() < _CACHE_TTL_HOURS * 3600:
                    logger.info(f"정책 캐시 유효 ({scanned_at})")
                    return cached
        except Exception:
            pass

    print("=" * 50)
    print("  정책 트래커 — 대통령실/정부 정책 수집")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 50)

    # 1. korea.kr RSS 수집
    print("\n[1/3] korea.kr 정책브리핑 RSS...")
    rss_articles = fetch_korea_kr_rss(days=days)
    print(f"  -> {len(rss_articles)}건 수집")

    # 2. Tavily 보강
    print("\n[2/3] Tavily 정책 뉴스 검색...")
    tavily_articles = fetch_policy_news_tavily()
    print(f"  -> {len(tavily_articles)}건 수집")

    # 합산
    all_articles = rss_articles + tavily_articles

    # 3. 키워드 매칭
    print("\n[3/3] 키워드 -> 섹터 매칭...")
    matched_sectors = match_policy_sectors(all_articles)
    print(f"  -> {len(matched_sectors)}개 섹터 매칭")
    for ms in matched_sectors[:5]:
        kws = ", ".join(ms["keywords_found"][:3])
        print(f"    {ms['sector']}: {kws} ({ms['article_count']}건, {ms['relevance']}점)")

    # 한줄 요약
    if matched_sectors:
        top_names = [ms["sector"] for ms in matched_sectors[:3]]
        summary = f"정책 수혜: {', '.join(top_names)}"
    else:
        summary = "특별한 정책 이슈 없음"

    result = {
        "scanned_at": datetime.now().isoformat(),
        "articles": all_articles,
        "matched_sectors": matched_sectors,
        "summary": summary,
        "stats": {
            "korea_kr_count": len(rss_articles),
            "tavily_count": len(tavily_articles),
            "matched_sector_count": len(matched_sectors),
        },
    }

    # atomic write
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    tmp_path = POLICY_PATH.with_suffix(".tmp")
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2, default=str)
    tmp_path.replace(POLICY_PATH)
    print(f"\n저장: {POLICY_PATH}")

    return result


# ═══════════════════════════════════════════════════
#  5. 보너스 점수 (morning_recommendation 호출용)
# ═══════════════════════════════════════════════════


def get_policy_bonus(code: str) -> float:
    """종목코드에 대한 정책 수혜 보너스 (0~30)"""
    if not POLICY_PATH.exists():
        return 0.0

    try:
        with open(POLICY_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (json.JSONDecodeError, IOError):
        return 0.0

    # 최근 12시간 이내 데이터만 유효
    scanned_at = data.get("scanned_at", "")
    if scanned_at:
        try:
            dt = datetime.fromisoformat(scanned_at)
            if (datetime.now() - dt).total_seconds() > 12 * 3600:
                return 0.0
        except Exception:
            pass

    for ms in data.get("matched_sectors", []):
        for ticker, name, relevance, desc in ms.get("kr_stocks", []):
            if ticker == code:
                # 기사 수에 비례 (max 30)
                base = min(ms.get("article_count", 1) * 10, 30)
                return float(base)

    return 0.0


# ═══════════════════════════════════════════════════
#  6. 텔레그램 포맷
# ═══════════════════════════════════════════════════


def format_telegram_message(result: Dict) -> str:
    """스캔 결과 → 텔레그램 메시지"""
    lines = [
        "━" * 20,
        "🏛 정책 트래커 — 대통령실/정부",
        "━" * 20,
    ]

    articles = result.get("articles", [])
    matched = result.get("matched_sectors", [])
    stats = result.get("stats", {})

    # ── 정책 브리핑 ──
    rss_articles = [a for a in articles if a.get("source") == "korea.kr"]
    if rss_articles:
        lines.append("")
        lines.append(f"📋 정책 브리핑 ({len(rss_articles)}건)")
        lines.append("─" * 20)
        for a in rss_articles[:8]:
            title = a["title"][:50]
            date_s = a.get("pub_date", "")
            if date_s:
                date_s = f" ({date_s[5:]})"  # "03-20"
            lines.append(f"  • {title}{date_s}")

    # ── Tavily 뉴스 ──
    tavily_articles = [a for a in articles if a.get("source") == "tavily"]
    if tavily_articles:
        lines.append("")
        lines.append(f"📰 정책 뉴스 ({len(tavily_articles)}건)")
        lines.append("─" * 20)
        for a in tavily_articles[:5]:
            title = a["title"][:50]
            lines.append(f"  • {title}")

    # ── 수혜 섹터 매핑 ──
    if matched:
        lines.append("")
        lines.append("📊 수혜 섹터")
        lines.append("─" * 20)
        for i, ms in enumerate(matched[:5], 1):
            sector = ms["sector"]
            stock_names = [s[1] for s in ms.get("kr_stocks", [])[:3]]
            kws = ", ".join(ms.get("keywords_found", [])[:3])
            lines.append(f"  {i}. {sector} [{kws}]")
            lines.append(f"     → {', '.join(stock_names)}")
    else:
        lines.append("")
        lines.append("📊 매칭된 정책 섹터 없음")

    # ── 통계 ──
    lines.append("")
    lines.append(
        f"korea.kr {stats.get('korea_kr_count', 0)}건 "
        f"+ Tavily {stats.get('tavily_count', 0)}건 "
        f"→ {stats.get('matched_sector_count', 0)}개 섹터"
    )
    lines.append("━" * 20)
    lines.append("Prophet 예언자 | 정책 드리븐")

    return "\n".join(lines)


# ═══════════════════════════════════════════════════
#  CLI
# ═══════════════════════════════════════════════════

if __name__ == "__main__":
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

    sys.path.insert(0, str(BASE_DIR))
    from dotenv import load_dotenv
    load_dotenv(BASE_DIR.parent / ".env")

    logging.basicConfig(level=logging.WARNING)

    # 캐시 무시 (CLI 실행 시 항상 새로 스캔)
    if POLICY_PATH.exists():
        POLICY_PATH.unlink()

    result = scan_policy(days=3)

    print(f"\n{'='*50}")
    print(f"  기사: {len(result['articles'])}건")
    print(f"  매칭 섹터: {len(result['matched_sectors'])}개")
    print(f"  요약: {result['summary']}")
    print(f"{'='*50}")

    if "--telegram" in sys.argv:
        msg = format_telegram_message(result)
        token = os.getenv("TELEGRAM_BOT_TOKEN")
        chat_id = os.getenv("TELEGRAM_CHAT_ID")
        if token and chat_id and "--send" in sys.argv:
            resp = requests.post(
                f"https://api.telegram.org/bot{token}/sendMessage",
                json={"chat_id": chat_id, "text": msg},
                timeout=15,
            )
            print(f"텔레그램 전송: {'OK' if resp.status_code == 200 else 'FAIL'}")
        else:
            print(msg)
