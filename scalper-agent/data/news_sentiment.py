"""
장중 뉴스 감성분석 + Kill-Switch
================================
NEWS-01: 보유종목 장중 뉴스 크롤링 (09:30/11:00/13:30)
NEWS-02: 감성 -0.7 → Guardian +25점 → EXIT 검토
NEWS-03: KILL_SWITCH만 텔레그램 알림
NEWS-04: 스코어링/프리클로즈/저널 반영

아키텍처:
  1단계: 네이버 금융 헤드라인 수집 (빠름, API 불필요)
  2단계: 키워드 기반 사전 필터 (즉시, 비용 0)
  3단계: 부정 의심 종목만 Sonnet+Opus Advisor 정밀 분석 (→Sonnet→Haiku 폴백)
  → Kill-Switch 발동 시 Guardian에 +25점 주입
"""

import json
import logging
import os
import re
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests

logger = logging.getLogger(__name__)

STORE_DIR = Path(__file__).resolve().parent.parent / "data_store"
SENTIMENT_DIR = STORE_DIR / "news_sentiment"

# ── 감성 키워드 (news_collector.py 호환 + 확장) ──
NEGATIVE_KEYWORDS = [
    "하락", "급락", "적자", "손실", "감소", "하향", "매도", "공매도",
    "조정", "경고", "우려", "위기", "부진", "축소", "약세", "악재",
    "폭락", "리스크", "하방", "부정", "적자전환", "순매도", "감자",
    "횡령", "제재", "소송", "리콜", "파산", "상폐",
    "수율", "불량", "차질", "지연", "철회", "해제", "반덤핑",
    "하향조정", "실적쇼크", "영업정지", "분식", "배임",
]
POSITIVE_KEYWORDS = [
    "상승", "급등", "신고가", "호실적", "수주", "매출증가", "흑자", "상향",
    "목표가", "매수", "추천", "기대", "성장", "확대", "호재", "반등",
    "돌파", "최고", "강세", "실적개선", "흑자전환", "사상최대", "순매수",
]

# ── Kill-Switch 임계값 ──
KILL_SWITCH_THRESHOLD = -0.7   # 감성 이 이하 → KILL_SWITCH
WARNING_THRESHOLD = -0.4       # 감성 이 이하 → WARNING
GUARDIAN_KILL_BONUS = 25       # Kill-Switch 시 Guardian risk_score 가산점


@dataclass
class NewsSentimentResult:
    """종목별 장중 뉴스 감성 결과"""
    code: str
    name: str
    sentiment: float             # -1.0 ~ +1.0
    action: str                  # "KILL_SWITCH" | "WARNING" | "CLEAR"
    headline: str = ""           # 트리거 헤드라인
    headlines_count: int = 0     # 수집된 헤드라인 수
    negative_count: int = 0      # 부정 키워드 수
    guardian_bonus: float = 0.0  # Guardian에 주입할 추가 점수
    analyzed_at: str = ""
    analysis_method: str = ""    # "keyword" | "claude"


@dataclass
class IntradayNewsReport:
    """장중 뉴스 스캔 전체 결과"""
    scanned_at: str
    stocks_scanned: int = 0
    kill_switches: List[NewsSentimentResult] = field(default_factory=list)
    warnings: List[NewsSentimentResult] = field(default_factory=list)
    clear: List[NewsSentimentResult] = field(default_factory=list)
    all_results: Dict[str, NewsSentimentResult] = field(default_factory=dict)


# ═══════════════════════════════════════════════════
#  NEWS-01: 뉴스 수집
# ═══════════════════════════════════════════════════

def _fetch_naver_headlines(code: str, count: int = 10) -> List[Dict]:
    """네이버 금융 종목뉴스 헤드라인 수집 (빠름, 2~3초)"""
    url = (
        f"https://finance.naver.com/item/news_news.naver"
        f"?code={code}&page=1&sm=title_entity_id.basic&clusterId="
    )
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Referer": f"https://finance.naver.com/item/main.naver?code={code}",
    }
    try:
        resp = requests.get(url, headers=headers, timeout=8)
        resp.encoding = "euc-kr"
        html_text = resp.text

        # title 속성 패턴
        pattern = r'class="title"[^>]*>\s*<a[^>]*?title="([^"]+)"'
        matches = re.findall(pattern, html_text)

        if not matches:
            pattern2 = r'class="title"[^>]*>\s*<a[^>]*>(.*?)</a>'
            matches = re.findall(pattern2, html_text, re.DOTALL)
            matches = [m.strip() for m in matches if m.strip()]

        headlines = []
        import html as html_mod
        for title in matches[:count]:
            title = re.sub(r'<[^>]+>', '', title).strip()
            title = html_mod.unescape(title)
            if title and len(title) > 5:
                headlines.append({"title": title})

        return headlines

    except Exception as e:
        logger.warning(f"[NEWS_SENTIMENT] 네이버뉴스 수집 실패 {code}: {e}")
        return []


# ═══════════════════════════════════════════════════
#  NEWS-02: 감성분석 + Kill-Switch
# ═══════════════════════════════════════════════════

def _keyword_sentiment(headlines: List[Dict]) -> Tuple[float, int]:
    """키워드 기반 빠른 감성 사전필터 (API 불필요)

    Returns:
        (normalized_score: -1.0~+1.0, negative_count: int)
    """
    if not headlines:
        return 0.0, 0

    raw_score = 0.0
    neg_count = 0
    for h in headlines:
        title = h.get("title", "")
        for word in NEGATIVE_KEYWORDS:
            if word in title:
                raw_score -= 1.5
                neg_count += 1
        for word in POSITIVE_KEYWORDS:
            if word in title:
                raw_score += 1.5

    # normalize to -1.0 ~ +1.0
    normalized = max(-1.0, min(1.0, raw_score / max(len(headlines), 1) / 3.0))
    return normalized, neg_count


def _extract_text_from_content(content_blocks):
    """Advisor 패턴 응답에서 text 블록만 추출"""
    if isinstance(content_blocks, list):
        return "".join(
            b.get("text", "") for b in content_blocks
            if isinstance(b, dict) and b.get("type") == "text"
        )
    return str(content_blocks)


def _claude_headline_sentiment(
    code: str, name: str, headlines: List[Dict]
) -> Optional[float]:
    """Sonnet+Opus Advisor로 헤드라인 정밀 감성분석 (3단계 폴백)

    Returns:
        -1.0 ~ +1.0 감성 점수 또는 None (실패 시)
    """
    api_key = os.getenv("ANTHROPIC_API_KEY", "")
    if not api_key:
        return None

    headline_text = "\n".join(
        f"{i}. {h['title']}" for i, h in enumerate(headlines[:10], 1)
    )

    base_prompt = f"""한국 주식 {name}({code}) 관련 뉴스 헤드라인입니다.
주가에 미치는 영향을 분석하세요.

{headline_text}

JSON으로만 응답하세요:
{{"sentiment": <-1.0~+1.0 소수점1자리. -1.0=극단적 악재, +1.0=극단적 호재>, "reason": "핵심 이유 1줄", "worst_headline": "가장 부정적인 헤드라인 원문"}}"""

    advisor_prompt = base_prompt + """

중요: 감성 점수를 매기기 전에, advisor 도구를 사용하여
해당 종목의 최근 수급/차트 맥락에서 이 뉴스의 실질 영향을 검증받으세요."""

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
                "max_tokens": 512,
                "messages": [{"role": "user", "content": advisor_prompt}],
                "tools": [{
                    "type": "advisor_20260301",
                    "name": "advisor",
                    "model": "claude-opus-4-7",
                    "max_uses": 1,
                }],
            },
            "timeout": 45,
        },
        {
            "label": "Sonnet",
            "headers": common_headers,
            "body": {
                "model": "claude-sonnet-4-6",
                "max_tokens": 512,
                "messages": [{"role": "user", "content": base_prompt}],
            },
            "timeout": 30,
        },
        {
            "label": "Haiku",
            "headers": common_headers,
            "body": {
                "model": "claude-haiku-4-5-20251001",
                "max_tokens": 256,
                "messages": [{"role": "user", "content": base_prompt}],
            },
            "timeout": 15,
        },
    ]

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
                logger.warning(f"[NEWS_SENT] {tier['label']} advisor beta 미지원 → 폴백")
                continue

            if resp.status_code != 200:
                logger.warning(f"[NEWS_SENT] {tier['label']} API {resp.status_code}")
                continue

            data = resp.json()
            text = _extract_text_from_content(data.get("content", []))

            # 비용 추적 로그
            usage = data.get("usage", {})
            has_advisor = any(
                b.get("type") in ("server_tool_use", "advisor_tool_result")
                for b in data.get("content", [])
                if isinstance(b, dict)
            )
            logger.info(
                f"[NEWS_SENT] {name}({code}) | {tier['label']} | "
                f"입력:{usage.get('input_tokens', 0)} "
                f"출력:{usage.get('output_tokens', 0)} | "
                f"Advisor:{'Y' if has_advisor else 'N'} | "
                f"{elapsed:.1f}초"
            )

            # JSON 파싱
            text = text.strip()
            if "```json" in text:
                text = text.split("```json")[1].split("```")[0].strip()
            elif "```" in text:
                text = text.split("```")[1].split("```")[0].strip()

            result = json.loads(text)
            return float(result.get("sentiment", 0.0))

        except requests.exceptions.Timeout:
            logger.warning(f"[NEWS_SENT] {tier['label']} 타임아웃 → 폴백")
            continue
        except json.JSONDecodeError as e:
            logger.warning(f"[NEWS_SENT] {tier['label']} JSON 파싱 실패: {e}")
            continue
        except Exception as e:
            logger.warning(f"[NEWS_SENT] {tier['label']} 실패: {e} → 폴백")
            continue

    logger.error(f"[NEWS_SENT] {name}({code}) 전 티어 실패")
    return None


def analyze_stock_sentiment(
    code: str, name: str
) -> NewsSentimentResult:
    """종목 1개에 대한 장중 뉴스 감성분석

    2단계 분석:
      1. 키워드 사전필터 (즉시)
      2. 부정 의심 시 Claude haiku 정밀 분석 (2~3초)
    """
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M")

    # 1단계: 뉴스 수집
    headlines = _fetch_naver_headlines(code, count=10)
    if not headlines:
        return NewsSentimentResult(
            code=code, name=name, sentiment=0.0, action="CLEAR",
            headlines_count=0, analyzed_at=now_str,
            analysis_method="no_news",
        )

    # 2단계: 키워드 사전필터
    kw_score, neg_count = _keyword_sentiment(headlines)

    # 키워드에서 이미 부정 강함 → Claude 정밀 확인
    if kw_score <= -0.3 and neg_count >= 2:
        claude_score = _claude_headline_sentiment(code, name, headlines)
        if claude_score is not None:
            final_sentiment = claude_score
            method = "claude"
        else:
            final_sentiment = kw_score
            method = "keyword_fallback"
    else:
        final_sentiment = kw_score
        method = "keyword"

    # 3단계: Kill-Switch 판정
    if final_sentiment <= KILL_SWITCH_THRESHOLD:
        action = "KILL_SWITCH"
        guardian_bonus = GUARDIAN_KILL_BONUS
        worst = headlines[0]["title"] if headlines else ""
    elif final_sentiment <= WARNING_THRESHOLD:
        action = "WARNING"
        guardian_bonus = 10.0  # WARNING은 가디언 +10
        worst = headlines[0]["title"] if headlines else ""
    else:
        action = "CLEAR"
        guardian_bonus = 0.0
        worst = ""

    return NewsSentimentResult(
        code=code,
        name=name,
        sentiment=round(final_sentiment, 2),
        action=action,
        headline=worst,
        headlines_count=len(headlines),
        negative_count=neg_count,
        guardian_bonus=guardian_bonus,
        analyzed_at=now_str,
        analysis_method=method,
    )


# ═══════════════════════════════════════════════════
#  메인 스캔 함수 (telegram_bot에서 호출)
# ═══════════════════════════════════════════════════

def scan_intraday_news(
    positions: List[Dict],
    watchlist: List[Dict] = None,
) -> IntradayNewsReport:
    """보유종목 + 감시종목 장중 뉴스 감성 스캔

    Args:
        positions: [{"code": "005930", "name": "삼성전자"}, ...]
        watchlist: 추가 감시 종목 (선택)

    Returns:
        IntradayNewsReport (kill_switches, warnings, clear)
    """
    targets = []
    seen = set()

    # 보유종목 우선
    for p in (positions or []):
        code = p.get("code", "")
        if code and code not in seen:
            targets.append({"code": code, "name": p.get("name", code)})
            seen.add(code)

    # 감시종목 추가 (최대 10종목)
    for w in (watchlist or []):
        code = w.get("code", "")
        if code and code not in seen and len(targets) < 10:
            targets.append({"code": code, "name": w.get("name", code)})
            seen.add(code)

    report = IntradayNewsReport(
        scanned_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        stocks_scanned=len(targets),
    )

    for t in targets:
        result = analyze_stock_sentiment(t["code"], t["name"])
        report.all_results[t["code"]] = result

        if result.action == "KILL_SWITCH":
            report.kill_switches.append(result)
            logger.warning(
                f"[KILL_SWITCH] {result.name}({result.code}) "
                f"감성 {result.sentiment:.2f} | {result.headline}"
            )
        elif result.action == "WARNING":
            report.warnings.append(result)
        else:
            report.clear.append(result)

        time.sleep(0.5)  # 네이버 요청 간격

    # 결과 저장
    _save_report(report)

    return report


def _save_report(report: IntradayNewsReport):
    """최신 감성 분석 결과 atomic 저장"""
    SENTIMENT_DIR.mkdir(parents=True, exist_ok=True)

    # 1. 최신 결과 (다른 모듈에서 읽기용)
    latest_path = SENTIMENT_DIR / "sentiment_latest.json"
    data = {
        "scanned_at": report.scanned_at,
        "stocks_scanned": report.stocks_scanned,
        "kill_switches": [asdict(r) for r in report.kill_switches],
        "warnings": [asdict(r) for r in report.warnings],
        "results": {
            code: asdict(r) for code, r in report.all_results.items()
        },
    }

    tmp = latest_path.with_suffix(".tmp")
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        tmp.replace(latest_path)
    except Exception as e:
        logger.error(f"[NEWS_SENTIMENT] 저장 실패: {e}")
        if tmp.exists():
            tmp.unlink()

    # 2. 일별 히스토리
    today = datetime.now().strftime("%Y%m%d")
    hist_path = SENTIMENT_DIR / f"sentiment_{today}.json"
    history = []
    if hist_path.exists():
        try:
            history = json.loads(hist_path.read_text(encoding="utf-8"))
        except Exception:
            history = []
    history.append(data)
    try:
        hist_path.write_text(
            json.dumps(history, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    except Exception:
        pass


# ═══════════════════════════════════════════════════
#  외부 조회 함수 (Guardian, auto_trader에서 호출)
# ═══════════════════════════════════════════════════

def get_news_risk_adjustment(code: str) -> float:
    """특정 종목의 뉴스 기반 Guardian 가산점 반환

    Guardian의 eye_risk_adj에 합산하여 사용:
      KILL_SWITCH → +25
      WARNING → +10
      CLEAR → 0

    Returns:
        0.0 ~ 25.0 (양수 = 리스크 증가)
    """
    latest_path = SENTIMENT_DIR / "sentiment_latest.json"
    if not latest_path.exists():
        return 0.0

    try:
        data = json.loads(latest_path.read_text(encoding="utf-8"))

        # 1시간 이내 결과만 유효
        scanned = datetime.strptime(data["scanned_at"], "%Y-%m-%d %H:%M:%S")
        if (datetime.now() - scanned).total_seconds() > 3600:
            return 0.0

        result = data.get("results", {}).get(code, {})
        return float(result.get("guardian_bonus", 0.0))

    except Exception:
        return 0.0


def get_news_sentiment_for_scoring(code: str) -> Dict:
    """추천 스코어링용 뉴스 감성 정보 반환

    Returns:
        {"sentiment": float, "action": str, "headline": str}
        또는 빈 dict
    """
    latest_path = SENTIMENT_DIR / "sentiment_latest.json"
    if not latest_path.exists():
        return {}

    try:
        data = json.loads(latest_path.read_text(encoding="utf-8"))
        result = data.get("results", {}).get(code, {})
        if not result:
            return {}
        return {
            "sentiment": result.get("sentiment", 0.0),
            "action": result.get("action", "CLEAR"),
            "headline": result.get("headline", ""),
        }
    except Exception:
        return {}


# ═══════════════════════════════════════════════════
#  NEWS-03: 텔레그램 포맷
# ═══════════════════════════════════════════════════

def format_kill_switch_alert(result: NewsSentimentResult) -> str:
    """Kill-Switch 발동 시 텔레그램 긴급 알림 포맷"""
    return (
        f"🚨 [뉴스 경고] {result.name} — {result.analyzed_at}\n"
        f'   "{result.headline}" (감성 {result.sentiment:.2f})\n'
        f"   → Guardian +{result.guardian_bonus:.0f}점 → EXIT 검토 중"
    )


def format_scan_summary(report: IntradayNewsReport) -> str:
    """스캔 결과 요약 (로그용, 텔레그램에는 KILL만)"""
    lines = [f"📰 장중 뉴스 스캔 ({report.scanned_at})"]
    lines.append(f"   스캔: {report.stocks_scanned}종목")

    if report.kill_switches:
        lines.append(f"   🚨 KILL: {len(report.kill_switches)}종목")
        for r in report.kill_switches:
            lines.append(f"      • {r.name} {r.sentiment:.2f}")
    if report.warnings:
        lines.append(f"   ⚠️ WARNING: {len(report.warnings)}종목")
    lines.append(f"   ✅ CLEAR: {len(report.clear)}종목")

    return "\n".join(lines)


# ═══════════════════════════════════════════════════
#  CLI 테스트
# ═══════════════════════════════════════════════════

if __name__ == "__main__":
    import sys
    from dotenv import load_dotenv

    env_path = Path(__file__).resolve().parent.parent.parent / ".env"
    load_dotenv(env_path)

    logging.basicConfig(level=logging.INFO, format="%(message)s")

    if "--test" in sys.argv:
        # 테스트: 삼성전자 + SK하이닉스
        test_positions = [
            {"code": "005930", "name": "삼성전자"},
            {"code": "000660", "name": "SK하이닉스"},
        ]
        report = scan_intraday_news(test_positions)
        print(format_scan_summary(report))
        print()
        for r in report.kill_switches:
            print(format_kill_switch_alert(r))
        print(f"\n저장 완료: {SENTIMENT_DIR / 'sentiment_latest.json'}")
    else:
        print("사용법: python -m data.news_sentiment --test")
        print("  --test: 삼성전자/SK하이닉스 테스트 스캔")
