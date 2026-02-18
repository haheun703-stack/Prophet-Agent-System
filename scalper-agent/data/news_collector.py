"""
뉴스 수집 + 감성분석 — 네이버증권 + Grok API
================================================
배치 스캔: 키워드 기반 빠른 감성분석 (API 호출 없음)
개별 분석: Grok API (grok-3-mini-fast) 상세 감성분석
"""

import html
import json
import logging
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import requests

logger = logging.getLogger(__name__)

CACHE_DIR = Path(__file__).resolve().parent.parent / "data_store" / "news"

# 감성 키워드
POSITIVE_WORDS = [
    "상승", "급등", "신고가", "호실적", "수주", "매출증가", "흑자", "상향",
    "목표가", "매수", "추천", "기대", "성장", "확대", "호재", "반등",
    "돌파", "최고", "강세", "실적개선", "흑자전환", "사상최대", "순매수",
    "신규투자", "대규모", "특허", "승인", "계약", "상장",
]
NEGATIVE_WORDS = [
    "하락", "급락", "적자", "손실", "감소", "하향", "매도", "공매도",
    "조정", "경고", "우려", "위기", "부진", "축소", "약세", "악재",
    "폭락", "리스크", "하방", "부정", "적자전환", "순매도", "감자",
    "횡령", "제재", "소송", "리콜", "파산", "상폐",
]


class NewsCollector:
    """뉴스 수집 + 감성분석"""

    def __init__(self):
        self.xai_key = os.getenv("XAI_API_KEY", "")
        self.cache_dir = CACHE_DIR
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def fetch_naver_news(self, code: str, count: int = 5) -> List[Dict]:
        """네이버증권에서 최신 뉴스 헤드라인 수집

        Returns: [{"title": str, "date": str, "link": str}, ...]
        """
        url = f"https://finance.naver.com/item/news_news.naver?code={code}&page=1&sm=title_entity_id.basic&clusterId="
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Referer": f"https://finance.naver.com/item/main.naver?code={code}",
        }

        try:
            resp = requests.get(url, headers=headers, timeout=8)
            resp.encoding = "euc-kr"
            page_html = resp.text

            # 뉴스 제목 추출 — <a> 태그 내 title 속성 또는 텍스트
            # 패턴: <td class="title"><a ... title="뉴스제목" ...>뉴스제목</a>
            pattern = r'class="title"[^>]*>\s*<a[^>]*?title="([^"]+)"'
            matches = re.findall(pattern, page_html)

            if not matches:
                # 대체 패턴: <a> 텍스트 직접 추출
                pattern2 = r'class="title"[^>]*>\s*<a[^>]*>(.*?)</a>'
                matches = re.findall(pattern2, page_html, re.DOTALL)
                matches = [m.strip() for m in matches if m.strip()]

            headlines = []
            for title in matches[:count]:
                title = re.sub(r'<[^>]+>', '', title).strip()
                title = html.unescape(title)  # &ldquo; → " 등
                if title and len(title) > 5:
                    headlines.append({"title": title})

            # 캐시 저장
            if headlines:
                self._save_cache(code, headlines)

            return headlines

        except Exception as e:
            logger.warning(f"네이버뉴스 수집 실패 {code}: {e}")
            # 캐시에서 로드 시도
            return self._load_cache(code) or []

    def quick_sentiment(self, headlines: List[Dict]) -> float:
        """키워드 기반 빠른 감성분석 — 배치 스캔용

        Returns: -10 ~ +10 점수
        """
        score = 0.0
        for h in headlines:
            title = h.get("title", "")
            for word in POSITIVE_WORDS:
                if word in title:
                    score += 1.5
            for word in NEGATIVE_WORDS:
                if word in title:
                    score -= 1.5
        return max(-10, min(10, score))

    def grok_sentiment(self, code: str, name: str, headlines: List[Dict]) -> Dict:
        """Grok API로 상세 감성분석 — 개별 분석용

        Returns: {"score": int, "summary": str, "sentiment": str, "key_factor": str}
        """
        if not self.xai_key:
            return {"score": 0, "summary": "API키 미설정", "sentiment": "neutral", "key_factor": ""}

        if not headlines:
            return {"score": 0, "summary": "뉴스 없음", "sentiment": "neutral", "key_factor": ""}

        headline_text = "\n".join(f"- {h['title']}" for h in headlines[:7])

        prompt = f"""다음은 한국 주식 {name}({code})의 최신 뉴스 헤드라인입니다:

{headline_text}

위 뉴스를 종합하여 주가에 미치는 영향을 분석하고, 아래 JSON 형식으로만 응답하세요:
{{"sentiment": "positive" 또는 "negative" 또는 "neutral", "score": -10에서 +10 사이 정수 (매우부정=-10, 매우긍정=+10), "summary": "한줄 요약 (30자 이내)", "key_factor": "핵심 요인 (20자 이내)"}}
JSON만 응답하세요. 다른 텍스트 없이."""

        try:
            resp = requests.post(
                "https://api.x.ai/v1/chat/completions",
                headers={
                    "Authorization": f"Bearer {self.xai_key}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": "grok-3-mini-fast",
                    "messages": [{"role": "user", "content": prompt}],
                    "temperature": 0.1,
                    "max_tokens": 200,
                },
                timeout=15,
            )

            if resp.status_code != 200:
                logger.warning(f"Grok API HTTP {resp.status_code}: {resp.text[:200]}")
                return {"score": 0, "summary": f"API오류({resp.status_code})", "sentiment": "neutral", "key_factor": ""}

            data = resp.json()
            content = data["choices"][0]["message"]["content"].strip()

            # JSON 파싱 (코드블록 제거)
            content = re.sub(r'^```json\s*', '', content)
            content = re.sub(r'\s*```$', '', content)
            result = json.loads(content)

            # 점수 범위 제한
            result["score"] = max(-10, min(10, int(result.get("score", 0))))
            return result

        except json.JSONDecodeError as e:
            logger.warning(f"Grok 응답 파싱 실패 {code}: {e}")
            return {"score": 0, "summary": "파싱실패", "sentiment": "neutral", "key_factor": ""}
        except Exception as e:
            logger.warning(f"Grok API 실패 {code}: {e}")
            return {"score": 0, "summary": "분석실패", "sentiment": "neutral", "key_factor": ""}

    def get_news_score(self, code: str, name: str = "", use_grok: bool = False) -> Dict:
        """뉴스 점수 + 요약 반환

        Args:
            code: 종목코드
            name: 종목명
            use_grok: True면 Grok API 상세 분석, False면 키워드 빠른 분석

        Returns: {"score": float, "summary": str, "headlines": list, "sentiment": str}
        """
        headlines = self.fetch_naver_news(code)

        if not headlines:
            return {"score": 0, "summary": "뉴스 없음", "headlines": [], "sentiment": "neutral"}

        if use_grok and self.xai_key:
            grok_result = self.grok_sentiment(code, name, headlines)
            return {
                "score": grok_result["score"],
                "summary": grok_result.get("summary", ""),
                "headlines": headlines,
                "sentiment": grok_result.get("sentiment", "neutral"),
                "key_factor": grok_result.get("key_factor", ""),
            }
        else:
            quick_score = self.quick_sentiment(headlines)
            sentiment = "positive" if quick_score > 2 else ("negative" if quick_score < -2 else "neutral")
            return {
                "score": quick_score,
                "summary": headlines[0]["title"][:30] if headlines else "",
                "headlines": headlines,
                "sentiment": sentiment,
            }

    def _save_cache(self, code: str, headlines: List[Dict]):
        """뉴스 캐시 저장"""
        try:
            today = datetime.now().strftime("%Y%m%d")
            path = self.cache_dir / f"{code}_{today}.json"
            with open(path, "w", encoding="utf-8") as f:
                json.dump({"code": code, "date": today, "headlines": headlines}, f, ensure_ascii=False)
        except Exception:
            pass

    def _load_cache(self, code: str) -> Optional[List[Dict]]:
        """뉴스 캐시 로드 (당일 것만)"""
        try:
            today = datetime.now().strftime("%Y%m%d")
            path = self.cache_dir / f"{code}_{today}.json"
            if path.exists():
                with open(path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                return data.get("headlines", [])
        except Exception:
            pass
        return None


# CLI 테스트
if __name__ == "__main__":
    import sys
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parent.parent.parent / ".env")

    collector = NewsCollector()
    code = sys.argv[1] if len(sys.argv) > 1 else "005930"
    name = sys.argv[2] if len(sys.argv) > 2 else "삼성전자"

    print(f"\n  {name}({code}) 뉴스 분석")
    print("=" * 50)

    # 네이버 뉴스
    headlines = collector.fetch_naver_news(code)
    print(f"\n  네이버 헤드라인 ({len(headlines)}개):")
    for h in headlines:
        print(f"    - {h['title']}")

    # 키워드 감성
    quick = collector.quick_sentiment(headlines)
    print(f"\n  키워드 감성점수: {quick:+.1f}")

    # Grok 분석
    if collector.xai_key:
        print(f"\n  Grok API 분석중...")
        grok = collector.grok_sentiment(code, name, headlines)
        print(f"  Grok 결과: {grok}")
    else:
        print("\n  XAI_API_KEY 없음 — Grok 분석 생략")
