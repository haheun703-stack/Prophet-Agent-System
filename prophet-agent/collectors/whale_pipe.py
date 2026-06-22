"""
🐋 Whale Collector - 고래 추적 파이프
======================================
수집 항목:
  - 한국: 연기금/외국인 대량 순매수 (pykrx 연계)
  - 한국: DART 5% 이상 지분 변동 공시
  - 글로벌: SEC 13F (워렌 버핏, 캐시 우드 등) - Phase 2

포물선 신호:
  서보성: "피터 린치가 원자재 비중 0%였다가 갑자기 3% 넣으면 
          그건 엄청난 신호야"
  
  - 슈퍼투자자 신규 진입 (0% → 3%+) = 강력한 매수 신호
  - 연기금 5일 연속 순매수 = 장기 상승 준비
"""

import logging
from typing import Dict
from datetime import datetime, timedelta

import pandas as pd

logger = logging.getLogger('Prophet.whale')


class WhaleCollector:
    """고래(기관/슈퍼투자자) 추적 수집기"""
    
    def __init__(self, config: dict):
        self.config = config
    
    def collect(self, ticker: str) -> Dict:
        """
        고래 동향 수집
        
        Returns:
            {
                'pension_fund_flow': DataFrame,   # 연기금 순매수 추이
                'large_shareholder': DataFrame,   # 5% 이상 지분 변동
                'foreign_flow_trend': DataFrame,  # 외국인 순매수 트렌드
            }
        """
        # ★ KRX 전역 kill switch (6/22 IP 차단 대응) — default 차단. 새 IP 후 1봇만.
        try:
            from collectors.krx_gate import krx_enabled, krx_block_reason
            if not krx_enabled():
                logger.warning("[krx_gate] %s", krx_block_reason())
                return {}
        except Exception:
            logger.warning("[krx_gate] 게이트 로드 실패 — KRX 보수적 차단(IP 보호)")
            return {}
        logger.info(f"[Whale] {ticker} 고래 추적")
        
        result = {
            'pension_fund_flow': self._get_pension_flow(ticker),
            'large_shareholder': self._get_large_shareholder(ticker),
            'foreign_flow_trend': self._get_foreign_trend(ticker),
        }
        
        return result
    
    def _get_pension_flow(self, ticker: str) -> pd.DataFrame:
        """
        연기금 순매수 추이
        
        pykrx의 투자자별 거래 데이터에서 '연기금' 추출
        연기금 = 국민연금 등 장기 투자자 → 이들의 매수는 장기 관점
        """
        try:
            from pykrx import stock
            
            end = datetime.now().strftime('%Y%m%d')
            start = (datetime.now() - timedelta(days=60)).strftime('%Y%m%d')
            
            df = stock.get_market_trading_value_by_date(start, end, ticker)
            
            if df.empty:
                return pd.DataFrame()
            
            # 연기금 컬럼 추출
            if '연기금등' in df.columns:
                pension = df[['연기금등']].copy()
                pension.columns = ['pension_net_buy']
                return pension
            
            return pd.DataFrame()
            
        except Exception as e:
            logger.warning(f"  연기금 추적 실패: {e}")
            return pd.DataFrame()
    
    def _get_large_shareholder(self, ticker: str) -> pd.DataFrame:
        """
        5% 이상 대량보유 변동 공시
        
        DART에서 주식등의대량보유상황보고서 검색
        → 신규 진입자 = 고래 관심 종목
        """
        try:
            import OpenDartReader
            
            api_key = self.config['api_keys']['dart_api_key']
            dart = OpenDartReader.OpenDartReader(api_key)
            
            filings = dart.list(corp=ticker, kind='G', duration=180)
            
            if filings is not None and not filings.empty:
                # 대량보유 관련 공시만 필터
                mask = filings['report_nm'].str.contains('대량보유|주식등의대량', na=False)
                result = filings[mask]
                logger.debug(f"  대량보유 공시: {len(result)}건")
                return result
            
            return pd.DataFrame()
            
        except Exception as e:
            logger.warning(f"  대량보유 공시 수집 실패: {e}")
            return pd.DataFrame()
    
    def _get_foreign_trend(self, ticker: str) -> pd.DataFrame:
        """
        외국인 순매수 트렌드 (60일)
        
        외국인 연속 매수일수 + 누적 금액 → 트렌드 방향
        """
        try:
            from pykrx import stock
            
            end = datetime.now().strftime('%Y%m%d')
            start = (datetime.now() - timedelta(days=60)).strftime('%Y%m%d')
            
            df = stock.get_market_trading_value_by_date(start, end, ticker)
            
            if df.empty:
                return pd.DataFrame()
            
            foreign_col = None
            for col in ['외국인합계', '외국인']:
                if col in df.columns:
                    foreign_col = col
                    break
            
            if foreign_col is None:
                return pd.DataFrame()
            
            result = df[[foreign_col]].copy()
            result.columns = ['foreign_net_buy']
            result['cumulative'] = result['foreign_net_buy'].cumsum()
            
            return result
            
        except Exception as e:
            logger.warning(f"  외국인 트렌드 수집 실패: {e}")
            return pd.DataFrame()
