"""
📰 Naver Finance Collector - 센티먼트 + 신용잔고 파이프
======================================================
수집 항목:
  - 신용잔고 (증시자금동향)
  - 종목별 뉴스
  - 종목토론실 센티먼트

핵심 포물선 신호:
  - 신용잔고 28~29조 (사상최고) = 진입 금지 🚫
  - 토론실 극단적 비관 ("손절", "물타기", "망했다") + 기관 매수 = 역발상 매수
  
서보성: "신용이 28~29조, 역대 최고치야. 삼성이 12만원에서 빠지면 
        반은 반대매매 당해. 다 죽는 거야."
"""

import logging
from typing import Dict, Optional
from datetime import datetime

import pandas as pd

logger = logging.getLogger('Prophet.naver')


class NaverCollector:
    """네이버 금융 크롤링 기반 센티먼트 수집기"""
    
    def __init__(self, config: dict):
        self.config = config
        self._ensure_deps()
    
    def _ensure_deps(self):
        try:
            import requests
            from bs4 import BeautifulSoup
            self.requests = requests
            self.bs4 = BeautifulSoup
        except ImportError:
            logger.error("bs4 미설치. pip install beautifulsoup4 requests")
            raise
    
    def collect(self, ticker: str) -> Dict:
        """
        네이버 금융 센티먼트 데이터 수집
        
        Returns:
            {
                'credit_balance': float,       # 전체 신용잔고 (조원)
                'credit_percentile': float,    # 역사적 백분위
                'news_sentiment': float,       # 뉴스 센티먼트 (-1 ~ +1)
                'board_sentiment': float,      # 토론실 센티먼트 (-1 ~ +1)
                'fear_words_count': int,       # 공포 키워드 출현 빈도
            }
        """
        logger.info(f"[Naver] {ticker} 센티먼트 수집")
        
        result = {
            'credit_balance': self._get_credit_balance(),
            'credit_percentile': 0.0,  # Phase 2에서 역사적 데이터 기반 계산
            'news_sentiment': self._get_news_sentiment(ticker),
            'board_sentiment': self._get_board_sentiment(ticker),
            'fear_words_count': 0,
        }
        
        return result
    
    def _get_credit_balance(self) -> float:
        """
        전체 시장 신용잔고 (조원)
        
        출처: 네이버 증시자금동향
        https://finance.naver.com/sise/sise_market_sum.nhn
        
        서보성 기준:
          - 28조 이상 = 극단적 위험 → 진입 금지
          - 15~20조 = 정상
          - 10조 이하 = 공포 바닥 → 매수 기회
        """
        try:
            url = "https://finance.naver.com/sise/sise_deposit.naver"
            headers = {'User-Agent': 'Mozilla/5.0'}
            resp = self.requests.get(url, headers=headers)
            soup = self.bs4(resp.text, 'html.parser')
            
            # TODO: 실제 파싱 로직 구현
            # 네이버 증시자금동향 페이지에서 신용잔고 추출
            logger.debug("  신용잔고: 파싱 로직 구현 필요")
            return 0.0
            
        except Exception as e:
            logger.warning(f"  신용잔고 수집 실패: {e}")
            return 0.0
    
    def _get_news_sentiment(self, ticker: str) -> float:
        """
        종목 뉴스 센티먼트 분석
        
        URL: finance.naver.com/item/news_news.nhn?code={ticker}
        
        Returns:
            -1.0 (극단적 부정) ~ +1.0 (극단적 긍정)
        """
        try:
            url = f"https://finance.naver.com/item/news_news.naver?code={ticker}"
            headers = {'User-Agent': 'Mozilla/5.0'}
            resp = self.requests.get(url, headers=headers)
            soup = self.bs4(resp.text, 'html.parser')
            
            # 뉴스 제목 수집
            titles = []
            for a_tag in soup.select('.title a'):
                titles.append(a_tag.get_text(strip=True))
            
            if not titles:
                return 0.0
            
            # 간단한 키워드 기반 센티먼트 (Phase 2에서 LLM 기반으로 업그레이드)
            positive_words = ['상승', '호실적', '신고가', '매수', '성장', '흑자', '수주']
            negative_words = ['하락', '적자', '손실', '매도', '위기', '폭락', '공매도']
            
            pos_count = sum(1 for t in titles for w in positive_words if w in t)
            neg_count = sum(1 for t in titles for w in negative_words if w in t)
            
            total = pos_count + neg_count
            if total == 0:
                return 0.0
            
            sentiment = (pos_count - neg_count) / total
            logger.debug(f"  뉴스: {len(titles)}건, 센티먼트={sentiment:.2f}")
            return sentiment
            
        except Exception as e:
            logger.warning(f"  뉴스 센티먼트 수집 실패: {e}")
            return 0.0
    
    def _get_board_sentiment(self, ticker: str) -> float:
        """
        종목토론실 센티먼트
        
        URL: finance.naver.com/item/board.nhn?code={ticker}
        
        포물선 신호: 
          극단적 비관 (공포 키워드 폭증) + 기관 매수 = 바닥 근처
        """
        try:
            url = f"https://finance.naver.com/item/board.naver?code={ticker}"
            headers = {'User-Agent': 'Mozilla/5.0'}
            resp = self.requests.get(url, headers=headers)
            soup = self.bs4(resp.text, 'html.parser')
            
            titles = []
            for td in soup.select('.title a'):
                titles.append(td.get_text(strip=True))
            
            if not titles:
                return 0.0
            
            # 공포 키워드 (서보성 대화 기반)
            fear_words = ['손절', '물타기', '망했다', '폭락', '반대매매', 
                         '탈출', '버티기', '고통', '멘붕', '패닉']
            
            fear_count = sum(1 for t in titles for w in fear_words if w in t)
            
            # 공포 비율이 높을수록 역발상 매수 신호
            fear_ratio = fear_count / len(titles) if titles else 0
            
            # -1 (극단적 공포=역발상 매수) ~ +1 (극단적 낙관=경계)
            # 반전: 공포가 높으면 양수 (매수 신호)
            sentiment = -1.0 + (fear_ratio * 2)  # 공포 많을수록 -1에 가까움
            
            logger.debug(f"  토론실: {len(titles)}건, 공포키워드={fear_count}개")
            return sentiment
            
        except Exception as e:
            logger.warning(f"  토론실 수집 실패: {e}")
            return 0.0
