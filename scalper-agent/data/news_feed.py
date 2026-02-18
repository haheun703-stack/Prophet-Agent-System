"""
뉴스/공시 피드 — 3순위 데이터

촉매(catalyst) 감지용:
  - 실적 발표, 대규모 수주, 지분 변동 등
  - 몸통의 시작점(촉매)을 확인하거나
  - 꼬리의 끝(악재)을 감지

현재: 구조만 준비 (향후 데이터 소스 연결)
후보 소스:
  - OpenDartReader (DART 공시) — 이미 설치됨
  - 네이버 금융 크롤링
  - 한경/매경 RSS
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional

logger = logging.getLogger(__name__)


@dataclass
class NewsItem:
    """뉴스/공시 아이템"""
    code: str
    title: str
    source: str          # "dart" | "naver" | "rss"
    category: str        # "실적" | "수주" | "지분" | "기타"
    published: datetime
    url: str = ""
    sentiment: float = 0.0   # -1.0 ~ +1.0 (향후 NLP)

    @property
    def is_positive(self) -> bool:
        return self.sentiment > 0.3

    @property
    def is_negative(self) -> bool:
        return self.sentiment < -0.3


def fetch_dart_disclosures(code: str, days: int = 30) -> List[NewsItem]:
    """DART 공시 조회 (OpenDartReader)

    향후 구현:
      import OpenDartReader
      dart = OpenDartReader(api_key)
      disclosures = dart.list(code, kind='A')  # 정기공시
    """
    # TODO: DART API 키 설정 후 활성화
    logger.info(f"[NewsFeed] DART 공시 조회 — 미구현 ({code})")
    return []


def fetch_naver_news(code: str, count: int = 10) -> List[NewsItem]:
    """네이버 금융 뉴스 크롤링

    향후 구현:
      url = f"https://finance.naver.com/item/news.nhn?code={code}"
      BeautifulSoup으로 파싱
    """
    logger.info(f"[NewsFeed] 네이버 뉴스 — 미구현 ({code})")
    return []
