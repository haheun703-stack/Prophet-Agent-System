"""
📊 pykrx Collector - 한국 주식 핵심 데이터 파이프
=================================================
수집 항목:
  - OHLCV (일봉)
  - 투자자별 수급 (외국인/기관/개인/연기금)
  - 공매도 잔고 및 거래량
  - 시가총액, 외국인 지분율
  - PER, PBR, 배당수익률

포물선 예언에 사용되는 신호:
  - 거래량 서서히 증가 (20일 평균 대비 2~3배, 폭발 아닌 점진적)
  - 외국인 연속 5일 순매수
  - 기관 연속 3일 순매수
  - 공매도 잔고 급감 (숏커버 압력)
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, Optional

import pandas as pd

logger = logging.getLogger('Prophet.pykrx')


class PykrxCollector:
    """pykrx 기반 한국 시장 데이터 수집기"""
    
    def __init__(self, config: dict):
        self.config = config
        self.lookback = config['collection']['lookback_days']
        self._ensure_pykrx()
    
    def _ensure_pykrx(self):
        """pykrx 임포트 확인"""
        try:
            from pykrx import stock
            self.stock = stock
        except ImportError:
            logger.error("pykrx 미설치. pip install pykrx")
            raise
    
    def collect(self, ticker: str) -> Dict[str, pd.DataFrame]:
        """
        종목의 전체 시장 데이터 수집
        
        Returns:
            {
                'ohlcv': DataFrame,          # 일봉 데이터
                'investor_flow': DataFrame,   # 투자자별 순매수
                'short_selling': DataFrame,   # 공매도
                'fundamental': DataFrame,     # PER/PBR/DIV
                'market_cap': DataFrame,      # 시총/외국인지분
            }
        """
        end_date = datetime.now().strftime('%Y%m%d')
        start_date = (datetime.now() - timedelta(days=self.lookback)).strftime('%Y%m%d')
        
        logger.info(f"[pykrx] {ticker} 데이터 수집 ({start_date}~{end_date})")
        
        result = {}
        
        # 1. OHLCV
        result['ohlcv'] = self._get_ohlcv(ticker, start_date, end_date)
        
        # 2. 투자자별 순매수
        result['investor_flow'] = self._get_investor_flow(ticker, start_date, end_date)
        
        # 3. 공매도
        result['short_selling'] = self._get_short_selling(ticker, start_date, end_date)
        
        # 4. 펀더멘털 (PER/PBR/DIV)
        result['fundamental'] = self._get_fundamental(ticker, start_date, end_date)
        
        # 5. 시가총액 + 외국인 지분율
        result['market_cap'] = self._get_market_cap(ticker, start_date, end_date)
        
        return result
    
    def _get_ohlcv(self, ticker: str, start: str, end: str) -> pd.DataFrame:
        """일봉 OHLCV 데이터"""
        try:
            df = self.stock.get_market_ohlcv_by_date(start, end, ticker)
            logger.debug(f"  OHLCV: {len(df)}일치 수집")
            return df
        except Exception as e:
            logger.warning(f"  OHLCV 수집 실패: {e}")
            return pd.DataFrame()
    
    def _get_investor_flow(self, ticker: str, start: str, end: str) -> pd.DataFrame:
        """투자자별 순매수 (외국인/기관/개인/연기금 등)"""
        try:
            df = self.stock.get_market_trading_value_by_date(start, end, ticker)
            logger.debug(f"  수급: {len(df)}일치 수집")
            return df
        except Exception as e:
            logger.warning(f"  수급 수집 실패: {e}")
            return pd.DataFrame()
    
    def _get_short_selling(self, ticker: str, start: str, end: str) -> pd.DataFrame:
        """공매도 잔고 및 거래량"""
        try:
            # 공매도 잔고
            df = self.stock.get_shorting_balance_by_date(start, end, ticker)
            logger.debug(f"  공매도: {len(df)}일치 수집")
            return df
        except Exception as e:
            logger.warning(f"  공매도 수집 실패: {e}")
            return pd.DataFrame()
    
    def _get_fundamental(self, ticker: str, start: str, end: str) -> pd.DataFrame:
        """PER, PBR, 배당수익률"""
        try:
            df = self.stock.get_market_fundamental(start, end, ticker)
            logger.debug(f"  펀더멘털: {len(df)}일치 수집")
            return df
        except Exception as e:
            logger.warning(f"  펀더멘털 수집 실패: {e}")
            return pd.DataFrame()
    
    def _get_market_cap(self, ticker: str, start: str, end: str) -> pd.DataFrame:
        """시가총액 + 외국인 보유비율"""
        try:
            df = self.stock.get_market_cap_by_date(start, end, ticker)
            logger.debug(f"  시총: {len(df)}일치 수집")
            return df
        except Exception as e:
            logger.warning(f"  시총 수집 실패: {e}")
            return pd.DataFrame()
    
    # ==========================================
    # 유니버스 관련
    # ==========================================
    
    def get_universe(self, markets: list, min_market_cap: int, max_stocks: int) -> list:
        """
        분석 대상 종목 유니버스 생성
        시총 기준 상위 N종목 추출
        """
        today = datetime.now().strftime('%Y%m%d')
        all_tickers = []
        
        for market in markets:
            try:
                tickers = self.stock.get_market_ticker_list(today, market=market)
                all_tickers.extend(tickers)
            except Exception as e:
                logger.warning(f"{market} 종목 리스트 수집 실패: {e}")
        
        # 시총 기준 필터링
        cap_data = []
        for ticker in all_tickers:
            try:
                cap = self.stock.get_market_cap_by_date(today, today, ticker)
                if not cap.empty:
                    market_cap = cap.iloc[-1]['시가총액']
                    if market_cap >= min_market_cap:
                        cap_data.append({'ticker': ticker, 'market_cap': market_cap})
            except:
                continue
        
        # 시총 순 정렬 후 상위 N개
        df = pd.DataFrame(cap_data)
        df = df.sort_values('market_cap', ascending=False).head(max_stocks)
        
        universe = df['ticker'].tolist()
        logger.info(f"유니버스 생성: {len(universe)}종목 (시총 {min_market_cap/1e8:.0f}억 이상)")
        
        return universe
    
    # ==========================================
    # 포물선 신호 전처리
    # ==========================================
    
    def calc_volume_surge(self, ohlcv: pd.DataFrame, window: int = 20) -> pd.Series:
        """거래량 증가율 (20일 평균 대비 배수)"""
        avg_vol = ohlcv['거래량'].rolling(window).mean()
        return ohlcv['거래량'] / avg_vol
    
    def calc_foreign_streak(self, investor_flow: pd.DataFrame) -> int:
        """외국인 연속 순매수 일수"""
        if investor_flow.empty:
            return 0
        foreign = investor_flow.get('외국인합계', investor_flow.get('외국인', pd.Series()))
        if foreign.empty:
            return 0
        
        streak = 0
        for val in reversed(foreign.values):
            if val > 0:
                streak += 1
            else:
                break
        return streak
    
    def calc_institutional_streak(self, investor_flow: pd.DataFrame) -> int:
        """기관 연속 순매수 일수"""
        if investor_flow.empty:
            return 0
        inst = investor_flow.get('기관합계', investor_flow.get('기관', pd.Series()))
        if inst.empty:
            return 0
        
        streak = 0
        for val in reversed(inst.values):
            if val > 0:
                streak += 1
            else:
                break
        return streak
