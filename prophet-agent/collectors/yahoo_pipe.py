"""
🌍 Yahoo Finance Collector - 글로벌 매크로 데이터 파이프
======================================================
수집 항목:
  - VIX (공포지수)
  - S&P 500 + EPS (매크로 방향)
  - SOXX (반도체 ETF → 한국 반도체 선행지표)
  - USD/KRW 환율
  - WTI 원유, 구리 등 원자재

포물선 신호:
  - VIX 25→15 급락 + 외국인 순매수 = 한국 시장 상승 환경
  - SOXX 랠리 → 한국 반도체 (삼전, 하이닉스) 후행 상승
  - S&P500 EPS↑ = 글로벌 경기 확장 → 한국 수출주 수혜
"""

import logging
from datetime import datetime, timedelta
from typing import Dict

import pandas as pd

logger = logging.getLogger('Prophet.yahoo')


class YahooCollector:
    """yfinance 기반 글로벌 매크로 데이터 수집기"""
    
    def __init__(self, config: dict):
        self.config = config
        self.macro_tickers = config['macro']
        self.lookback = config['collection']['lookback_days']
        self._ensure_yfinance()
    
    def _ensure_yfinance(self):
        try:
            import yfinance as yf
            self.yf = yf
        except ImportError:
            logger.error("yfinance 미설치. pip install yfinance")
            raise
    
    def collect(self, ticker: str = None) -> Dict[str, pd.DataFrame]:
        """
        글로벌 매크로 데이터 수집
        
        Returns:
            {
                'vix': DataFrame,
                'sp500': DataFrame,
                'soxx': DataFrame,
                'usdkrw': DataFrame,
                'wti': DataFrame,
            }
        """
        end = datetime.now()
        start = end - timedelta(days=self.lookback)
        
        logger.info("[Yahoo] 글로벌 매크로 데이터 수집")
        
        result = {}
        
        for name, symbol in self.macro_tickers.items():
            if symbol is None:
                result[name] = pd.DataFrame()
                continue
            try:
                data = self.yf.download(symbol, start=start, end=end, progress=False)
                result[name] = data
                logger.debug(f"  {name} ({symbol}): {len(data)}일치")
            except Exception as e:
                logger.warning(f"  {name} 수집 실패: {e}")
                result[name] = pd.DataFrame()
        
        return result
    
    # ==========================================
    # 매크로 신호 전처리
    # ==========================================
    
    def get_vix_regime(self, vix_df: pd.DataFrame) -> str:
        """
        VIX 기반 시장 레짐 판단
        
        Returns:
            'fear' (VIX > 25): 공포 → 역발상 매수 기회
            'caution' (20 < VIX <= 25): 경계
            'calm' (VIX <= 20): 평온 → 정상 운영
            'euphoria' (VIX < 12): 극단적 낙관 → 경계
        """
        if vix_df.empty:
            return 'unknown'
        
        current_vix = vix_df['Close'].iloc[-1]
        
        if current_vix > 25:
            return 'fear'
        elif current_vix > 20:
            return 'caution'
        elif current_vix > 12:
            return 'calm'
        else:
            return 'euphoria'
    
    def get_soxx_momentum(self, soxx_df: pd.DataFrame, window: int = 20) -> float:
        """SOXX 20일 모멘텀 (%) → 한국 반도체 선행지표"""
        if soxx_df.empty or len(soxx_df) < window:
            return 0.0
        
        current = soxx_df['Close'].iloc[-1]
        past = soxx_df['Close'].iloc[-window]
        return (current - past) / past * 100
    
    def get_vix_change(self, vix_df: pd.DataFrame, window: int = 5) -> float:
        """VIX 5일 변화율 → 급락 = 리스크온"""
        if vix_df.empty or len(vix_df) < window:
            return 0.0
        
        current = vix_df['Close'].iloc[-1]
        past = vix_df['Close'].iloc[-window]
        return (current - past) / past * 100
