"""
뉴스 AI 스캐너 (News AI Scanner)
─────────────────────────────────
다중 소스 뉴스 수집 → Claude AI 통합 분석
→ 종목별 뉴스 점수 + 매수/경고 시그널

소스:
  1. 네이버 뉴스 검색 (종목명 + 업종 키워드)
  2. 네이버 금융 종목별 뉴스 (기존 news_collector 재사용)
  3. DART 공시 (기존 event_detector 재사용)
  4. 섹터/테마 뉴스 (반도체, 2차전지, 바이오 등)

AI 분석:
  - Sonnet 4.6 + Opus Advisor → Sonnet → Haiku 3단계 폴백
  - 종목별 뉴스 감성 + 촉매 이벤트 + 리스크 분석
  - 비용 추적 로그 (모델/토큰/Advisor 사용 여부/지연시간)
"""
import os
import re
import json
import logging
import requests
import time
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Tuple
from datetime import datetime, timedelta
from pathlib import Path

logger = logging.getLogger(__name__)

DATA_STORE = Path(__file__).parent.parent / "data_store" / "news_ai"

# ── 섹터 키워드 (네이버 뉴스 검색용) ──
SECTOR_KEYWORDS = {
    "반도체": ["반도체", "HBM", "파운드리", "DRAM", "낸드", "AI칩"],
    "2차전지": ["2차전지", "배터리", "리튬", "양극재", "음극재", "전고체"],
    "바이오": ["바이오", "임상시험", "신약", "FDA", "바이오시밀러"],
    "자동차": ["전기차", "자율주행", "현대차", "테슬라", "모빌리티"],
    "방산": ["방산", "무기수출", "K방산", "한화에어로", "LIG넥스원"],
    "AI": ["인공지능", "AI", "엔비디아", "챗GPT", "LLM", "GPU"],
    "원전": ["원전", "SMR", "소형모듈원전", "두산에너빌리티", "원자력"],
    "조선": ["조선", "LNG선", "HD한국조선", "삼성중공업"],
}

# ── 종목↔섹터 매핑 ──
STOCK_SECTOR_MAP = {
    "005930": "반도체",   # 삼성전자
    "000660": "반도체",   # SK하이닉스
    "006400": "2차전지",  # 삼성SDI
    "051910": "2차전지",  # LG화학
    "003670": "2차전지",  # 포스코퓨처엠
    "005380": "자동차",   # 현대차
    "000270": "자동차",   # 기아
    "207940": "바이오",   # 삼성바이오
    "068270": "바이오",   # 셀트리온
    "012450": "AI",       # 한화에어로스페이스
    "042700": "AI",       # 한미반도체
    "034730": "AI",       # SK
    "009540": "조선",     # HD한국조선해양
}


@dataclass
class NewsItem:
    """개별 뉴스 항목"""
    title: str
    source: str           # naver_search, naver_finance, dart, sector
    date: str             # YYYY-MM-DD
    url: str = ""
    snippet: str = ""
    relevance: str = ""   # AI가 판단한 관련도


@dataclass
class NewsAIResult:
    """AI 분석 결과"""
    code: str
    name: str
    news_score: float           # -100 ~ +100
    news_grade: str             # STRONG_POSITIVE, POSITIVE, NEUTRAL, NEGATIVE, STRONG_NEGATIVE
    catalyst_count: int         # 촉매 이벤트 수
    risk_count: int             # 리스크 이벤트 수
    catalysts: List[str]        # 촉매 목록
    risks: List[str]            # 리스크 목록
    ai_summary: str             # AI 요약 (1~2줄)
    ai_recommendation: str      # AI 추천 코멘트
    sector_sentiment: str       # 섹터 전체 분위기
    news_count: int             # 수집된 뉴스 수
    top_headlines: List[str]    # 주요 헤드라인 TOP 5


def fetch_naver_news_search(query: str, max_items: int = 10) -> List[NewsItem]:
    """Google News RSS로 한국 뉴스 검색 (네이버 검색은 JS 렌더링이라 사용 불가)"""
    items = []
    try:
        import urllib.parse
        encoded = urllib.parse.quote(query)
        url = f"https://news.google.com/rss/search?q={encoded}&hl=ko&gl=KR&ceid=KR:ko"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                          "AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36"
        }
        resp = requests.get(url, headers=headers, timeout=10)
        resp.encoding = "utf-8"
        xml = resp.text

        # RSS XML에서 제목 추출
        titles = re.findall(r'<title><!\[CDATA\[(.*?)\]\]></title>', xml)
        if not titles:
            titles = re.findall(r'<title>(.*?)</title>', xml)

        # 첫 번째는 피드 제목이므로 스킵
        for title in titles[1:max_items + 1]:
            title = re.sub(r'<[^>]+>', '', title).strip()
            # " - 매체명" 분리
            parts = title.rsplit(" - ", 1)
            headline = parts[0].strip()
            source_name = parts[1].strip() if len(parts) > 1 else ""

            if headline and len(headline) > 5:
                items.append(NewsItem(
                    title=headline,
                    source=f"google_news({source_name})" if source_name else "google_news",
                    date=datetime.now().strftime("%Y-%m-%d"),
                ))

    except Exception as e:
        logger.warning(f"Google News RSS 실패 ({query}): {e}")

    return items


def fetch_stock_news(code: str, name: str) -> List[NewsItem]:
    """네이버 금융 종목별 뉴스 (NewsCollector 클래스 사용)"""
    items = []
    try:
        from data.news_collector import NewsCollector
        nc = NewsCollector()
        headlines = nc.fetch_naver_news(code, count=10)
        for h in headlines:
            title = h.get("title", "") if isinstance(h, dict) else str(h)
            if title:
                items.append(NewsItem(
                    title=title,
                    source="naver_finance",
                    date=datetime.now().strftime("%Y-%m-%d"),
                ))
    except Exception as e:
        logger.warning(f"종목 뉴스 수집 실패 ({code}): {e}")
    return items


def fetch_dart_disclosures(code: str, name: str) -> List[NewsItem]:
    """DART 공시 - 전체 공시에서 해당 종목 필터링"""
    items = []
    try:
        from data.event_detector import fetch_dart_disclosures as dart_fetch
        disclosures = dart_fetch(days_back=7)
        for d in disclosures:
            if not isinstance(d, dict):
                continue
            # 종목명 매칭
            corp = d.get("corp_name", "")
            if name and name[:2] in corp:  # 앞 2글자 매칭
                title = d.get("report_nm", "")
                event_type = d.get("event_type", "")
                items.append(NewsItem(
                    title=f"[공시] {title}" + (f" ({event_type})" if event_type else ""),
                    source="dart",
                    date=d.get("rcept_dt", ""),
                ))
        items = items[:5]
    except Exception as e:
        logger.debug(f"DART 공시 수집 실패 ({code}): {e}")
    return items


def fetch_sector_news(code: str) -> List[NewsItem]:
    """섹터/테마 뉴스 검색"""
    sector = STOCK_SECTOR_MAP.get(code, "")
    if not sector:
        return []

    keywords = SECTOR_KEYWORDS.get(sector, [])
    if not keywords:
        return []

    # 섹터의 첫 번째 키워드로 검색
    items = fetch_naver_news_search(keywords[0], max_items=5)
    for item in items:
        item.source = f"sector_{sector}"
    return items


def collect_all_news(code: str, name: str) -> List[NewsItem]:
    """다중 소스 뉴스 통합 수집"""
    all_items = []

    # 1. 종목명으로 네이버 뉴스 검색
    all_items.extend(fetch_naver_news_search(name, max_items=8))

    # 2. 네이버 금융 종목 뉴스
    all_items.extend(fetch_stock_news(code, name))

    # 3. DART 공시
    all_items.extend(fetch_dart_disclosures(code, name))

    # 4. 섹터 뉴스
    all_items.extend(fetch_sector_news(code))

    # 중복 제거 (제목 기준)
    seen = set()
    unique = []
    for item in all_items:
        key = item.title[:30]  # 앞 30자로 중복 체크
        if key not in seen:
            seen.add(key)
            unique.append(item)

    return unique


def _extract_text_from_content(content_blocks):
    """Advisor 패턴 응답에서 text 블록만 추출"""
    if isinstance(content_blocks, list):
        return "".join(
            b.get("text", "") for b in content_blocks
            if isinstance(b, dict) and b.get("type") == "text"
        )
    return str(content_blocks)


def analyze_with_claude(code: str, name: str,
                        news_items: List[NewsItem],
                        supply_context: str = "") -> Optional[NewsAIResult]:
    """Claude AI로 뉴스 통합 분석 (Sonnet+Opus Advisor → Sonnet → Haiku 3단계 폴백)

    Args:
        code: 종목코드
        name: 종목명
        news_items: 수집된 뉴스 목록
        supply_context: 수급 컨텍스트 (선택)

    Returns:
        NewsAIResult or None
    """
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        logger.error("ANTHROPIC_API_KEY 없음")
        return None

    if not news_items:
        return NewsAIResult(
            code=code, name=name, news_score=0, news_grade="NEUTRAL",
            catalyst_count=0, risk_count=0, catalysts=[], risks=[],
            ai_summary="뉴스 없음", ai_recommendation="데이터 부족",
            sector_sentiment="UNKNOWN", news_count=0, top_headlines=[],
        )

    # 뉴스 텍스트 구성
    news_text = ""
    for i, item in enumerate(news_items[:20], 1):
        news_text += f"{i}. [{item.source}] {item.title}"
        if item.snippet:
            news_text += f"\n   {item.snippet[:100]}"
        news_text += "\n"

    base_prompt = f"""당신은 한국 주식시장 전문 뉴스 분석가입니다.

종목: {name} ({code})
{f'수급 컨텍스트: {supply_context}' if supply_context else ''}

아래 뉴스를 분석하고 JSON으로 응답하세요:

{news_text}

다음 JSON 형식으로만 응답하세요 (설명 없이 JSON만):
{{
  "news_score": <-100~+100 정수. 양수=호재, 음수=악재>,
  "news_grade": "<STRONG_POSITIVE|POSITIVE|NEUTRAL|NEGATIVE|STRONG_NEGATIVE>",
  "catalysts": ["촉매1", "촉매2"],
  "risks": ["리스크1", "리스크2"],
  "ai_summary": "1~2줄 핵심 요약",
  "ai_recommendation": "매수/관망/경고 + 이유 1줄",
  "sector_sentiment": "<BULLISH|NEUTRAL|BEARISH>",
  "top_headlines": ["가장 중요한 헤드라인 최대 3개"]
}}

분석 기준:
- 실적/매출 관련: 실적 서프라이즈=+30~50, 실적 미스=-30~50
- 수주/계약: 대형 수주=+20~40
- 규제/법적 이슈: -20~-50
- 테마/섹터 호재: +10~30
- 단순 언급/중립: 0
- 뉴스 없으면 score=0, grade=NEUTRAL"""

    advisor_prompt = base_prompt + """

중요: 뉴스의 주가 영향을 판단하기 전에, advisor 도구를 사용하여
해당 뉴스의 시장 맥락과 과거 유사 사례를 검증받으세요."""

    # ── 3단계 폴백: Sonnet+Opus → Sonnet → Haiku ──
    common_headers = {
        "x-api-key": api_key,
        "anthropic-version": "2023-06-01",
        "content-type": "application/json",
    }
    tiers = [
        {
            "label": "Sonnet+Opus",
            "headers": {**common_headers, "anthropic-beta": "advisor-tool-2026-03-01"},
            "body": {
                "model": "claude-sonnet-4-6",
                "max_tokens": 2000,
                "messages": [{"role": "user", "content": advisor_prompt}],
                "tools": [{
                    "type": "advisor_20260301",
                    "name": "advisor",
                    "model": "claude-opus-4-7",
                    "max_uses": 1,
                }],
            },
            "timeout": 60,
        },
        {
            "label": "Sonnet",
            "headers": common_headers,
            "body": {
                "model": "claude-sonnet-4-6",
                "max_tokens": 2000,
                "messages": [{"role": "user", "content": base_prompt}],
            },
            "timeout": 45,
        },
        {
            "label": "Haiku",
            "headers": common_headers,
            "body": {
                "model": "claude-haiku-4-5-20251001",
                "max_tokens": 1024,
                "messages": [{"role": "user", "content": base_prompt}],
            },
            "timeout": 30,
        },
    ]

    text = None
    model_used = "N/A"
    for tier in tiers:
        try:
            t0 = time.time()
            resp = requests.post(
                "https://api.anthropic.com/v1/messages",
                headers=tier["headers"],
                json=tier["body"],
                timeout=tier["timeout"],
            )
            elapsed = time.time() - t0

            # Advisor beta 미지원 → 다음 티어
            if resp.status_code == 400 and "Opus" in tier["label"]:
                logger.warning(f"[NEWS_AI] {tier['label']} advisor beta 미지원 → 폴백")
                continue

            if resp.status_code != 200:
                logger.warning(
                    f"[NEWS_AI] {tier['label']} API {resp.status_code}: "
                    f"{resp.text[:200]}"
                )
                continue

            data = resp.json()
            text = _extract_text_from_content(data.get("content", []))
            model_used = tier["label"]

            # 비용 추적 로그
            usage = data.get("usage", {})
            has_advisor = any(
                b.get("type") in ("server_tool_use", "advisor_tool_result")
                for b in data.get("content", [])
                if isinstance(b, dict)
            )
            logger.info(
                f"[NEWS_AI] {name}({code}) | {model_used} | "
                f"입력:{usage.get('input_tokens', 0)} "
                f"출력:{usage.get('output_tokens', 0)} | "
                f"Advisor:{'Y' if has_advisor else 'N'} | "
                f"{elapsed:.1f}초"
            )
            break

        except requests.exceptions.Timeout:
            logger.warning(f"[NEWS_AI] {tier['label']} 타임아웃 ({tier['timeout']}초) → 폴백")
            continue
        except Exception as e:
            logger.warning(f"[NEWS_AI] {tier['label']} 실패: {e} → 폴백")
            continue

    if not text:
        logger.error(f"[NEWS_AI] {name}({code}) 전 티어 실패")
        return None

    try:
        # JSON 파싱 (```json 블록 처리)
        text = text.strip()
        if "```json" in text:
            text = text.split("```json")[1].split("```")[0].strip()
        elif "```" in text:
            text = text.split("```")[1].split("```")[0].strip()

        result = json.loads(text)

        return NewsAIResult(
            code=code,
            name=name,
            news_score=int(result.get("news_score", 0)),
            news_grade=result.get("news_grade", "NEUTRAL"),
            catalyst_count=len(result.get("catalysts", [])),
            risk_count=len(result.get("risks", [])),
            catalysts=result.get("catalysts", []),
            risks=result.get("risks", []),
            ai_summary=result.get("ai_summary", ""),
            ai_recommendation=result.get("ai_recommendation", ""),
            sector_sentiment=result.get("sector_sentiment", "NEUTRAL"),
            news_count=len(news_items),
            top_headlines=result.get("top_headlines", []),
        )

    except json.JSONDecodeError as e:
        logger.error(f"[NEWS_AI] JSON 파싱 실패 ({model_used}): {e}\n응답: {text[:300]}")
        return None
    except Exception as e:
        logger.error(f"[NEWS_AI] 결과 처리 실패 ({model_used}): {e}")
        return None


def scan_news_ai(candidates: list, save: bool = True) -> List[NewsAIResult]:
    """사전감지 후보에 대한 뉴스 AI 분석

    Args:
        candidates: PreMoveCandidate 또는 {code, name} dict 리스트
        save: 결과 저장 여부

    Returns:
        종목별 NewsAIResult 리스트
    """
    results = []

    for c in candidates:
        code = c.code if hasattr(c, "code") else c.get("code", "")
        name = c.name if hasattr(c, "name") else c.get("name", "")

        if not code or not name:
            continue

        print(f"  뉴스 수집: {name}({code})...")
        news_items = collect_all_news(code, name)
        print(f"    수집 완료: {len(news_items)}건")

        # 수급 컨텍스트 구성
        supply_ctx = ""
        if hasattr(c, "supply_grade"):
            supply_ctx = (
                f"수급등급:{c.supply_grade}, 모멘텀:{c.momentum_signal}, "
                f"에너지:{c.energy_grade}, 기관연속매수:{c.inst_streak}일"
            )

        print(f"    AI 분석 중...")
        result = analyze_with_claude(code, name, news_items, supply_ctx)

        if result:
            results.append(result)
            grade_emoji = {
                "STRONG_POSITIVE": "🔥",
                "POSITIVE": "✅",
                "NEUTRAL": "⚪",
                "NEGATIVE": "⚠️",
                "STRONG_NEGATIVE": "🚨",
            }
            emoji = grade_emoji.get(result.news_grade, "⚪")
            print(f"    → {emoji} {result.news_grade} (점수: {result.news_score:+d})")
        else:
            print(f"    → 분석 실패")

    # 저장
    if save and results:
        DATA_STORE.mkdir(parents=True, exist_ok=True)
        today = datetime.now().strftime("%Y%m%d")
        path = DATA_STORE / f"news_ai_{today}.json"
        data = []
        for r in results:
            data.append({
                "code": r.code, "name": r.name,
                "news_score": r.news_score, "news_grade": r.news_grade,
                "catalyst_count": r.catalyst_count, "risk_count": r.risk_count,
                "catalysts": r.catalysts, "risks": r.risks,
                "ai_summary": r.ai_summary, "ai_recommendation": r.ai_recommendation,
                "sector_sentiment": r.sector_sentiment,
                "news_count": r.news_count, "top_headlines": r.top_headlines,
            })
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        print(f"\n저장: {path}")

    return results


def format_news_ai_report(results: List[NewsAIResult]) -> str:
    """텔레그램용 뉴스 AI 리포트"""
    if not results:
        return "📰 뉴스 AI: 분석 결과 없음"

    grade_emoji = {
        "STRONG_POSITIVE": "🔥",
        "POSITIVE": "✅",
        "NEUTRAL": "⚪",
        "NEGATIVE": "⚠️",
        "STRONG_NEGATIVE": "🚨",
    }

    lines = ["📰 뉴스 AI 분석", "━━━━━━━━━━━━━━━━━━━━"]

    for r in sorted(results, key=lambda x: x.news_score, reverse=True):
        emoji = grade_emoji.get(r.news_grade, "⚪")
        lines.append(f"\n{emoji} {r.name}({r.code})")
        lines.append(f"  점수: {r.news_score:+d} | {r.news_grade}")
        lines.append(f"  뉴스: {r.news_count}건 | 섹터: {r.sector_sentiment}")

        if r.catalysts:
            lines.append(f"  촉매: {' / '.join(r.catalysts[:3])}")
        if r.risks:
            lines.append(f"  리스크: {' / '.join(r.risks[:2])}")

        lines.append(f"  AI: {r.ai_summary}")
        lines.append(f"  추천: {r.ai_recommendation}")

    lines.append(f"\n{datetime.now().strftime('%Y-%m-%d %H:%M')}")
    return "\n".join(lines)


# ── CLI ──
if __name__ == "__main__":
    import sys
    sys.path.insert(0, str(Path(__file__).parent.parent))
    from dotenv import load_dotenv
    load_dotenv(str(Path(__file__).parent.parent.parent / ".env"))

    print("=" * 60)
    print("  📰 뉴스 AI 스캐너 테스트")
    print("=" * 60)

    # 사전감지 후보 로드
    try:
        from data.premove_scanner import scan_premove
        print("\n사전감지 스캔 실행...")
        candidates = scan_premove(top_n=5)
    except Exception as e:
        print(f"사전감지 실패: {e}")
        # 폴백: 수동 테스트 종목
        candidates = [
            {"code": "005930", "name": "삼성전자"},
            {"code": "000660", "name": "SK하이닉스"},
        ]

    if not candidates:
        print("후보 없음 - 테스트 종목 사용")
        candidates = [
            {"code": "005930", "name": "삼성전자"},
            {"code": "000660", "name": "SK하이닉스"},
        ]

    print(f"\n뉴스 AI 분석 시작 ({len(candidates)}종목)...")
    print("─" * 60)

    results = scan_news_ai(candidates)

    print("\n" + "=" * 60)
    print(format_news_ai_report(results))
