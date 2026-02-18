"""
📋 DART Collector - 전자공시 데이터 파이프
==========================================
수집 항목:
  - 분기별 EPS (주당순이익)
  - 영업이익 추이
  - 주요 공시 (대규모 계약, 지분 변동, 합병 등)
  - 대주주 담보 제공 현황 (반대매매 바닥 감지)

핵심 포물선 신호:
  - EPS↑ + 주가↓ = 괴리도 확대 → 수렴 예상 (서보성 핵심 원칙)
  - 적자→흑자 전환 공시
  - 대주주 주식담보 대출 → 반대매매 가격 = 절대 바닥
"""

import logging
from typing import Dict, Optional, List
from datetime import datetime

import pandas as pd

logger = logging.getLogger('Prophet.dart')


class DartCollector:
    """DART 전자공시 데이터 수집기"""
    
    def __init__(self, config: dict):
        self.config = config
        self.api_key = config['api_keys']['dart_api_key']
        self.eps_quarters = config['collection']['eps_quarters']
        self._ensure_dart()
    
    def _ensure_dart(self):
        """OpenDartReader 임포트 확인"""
        try:
            import OpenDartReader
            self.dart = OpenDartReader.OpenDartReader(self.api_key)
        except ImportError:
            logger.error("OpenDartReader 미설치. pip install opendartreader")
            self.dart = None
        except Exception as e:
            logger.warning(f"DART API 초기화 실패 (API키 확인): {e}")
            self.dart = None
    
    def collect(self, ticker: str) -> Dict:
        """
        종목의 DART 데이터 수집
        
        Returns:
            {
                'eps_history': DataFrame,      # 분기별 EPS 추이
                'operating_profit': DataFrame,  # 영업이익 추이
                'filings': DataFrame,           # 최근 공시 목록
                'major_shareholder': DataFrame, # 대주주 현황
            }
        """
        if self.dart is None:
            logger.warning("[DART] API 미연결, 빈 데이터 반환")
            return self._empty_result()
        
        logger.info(f"[DART] {ticker} 공시 데이터 수집")
        
        result = {}
        
        # 1. 분기별 재무제표 → EPS 추출
        result['eps_history'] = self._get_eps_history(ticker)
        
        # 2. 영업이익 추이
        result['operating_profit'] = self._get_operating_profit(ticker)
        
        # 3. 최근 공시 목록
        result['filings'] = self._get_recent_filings(ticker)
        
        # 4. 대주주 현황 (반대매매 바닥용)
        result['major_shareholder'] = self._get_major_shareholder(ticker)
        
        return result
    
    def _get_eps_history(self, ticker: str) -> pd.DataFrame:
        """
        분기별 EPS 추이 (최근 8분기)
        
        이것이 예언자의 핵심 데이터:
        "주가는 EPS에 수렴한다" - 서보성
        """
        try:
            current_year = datetime.now().year
            eps_data = []
            
            for year in range(current_year - 2, current_year + 1):
                for quarter in ['11013', '11012', '11014', '11011']:
                    # 11013=1분기, 11012=반기, 11014=3분기, 11011=연간
                    try:
                        fs = self.dart.finstate(
                            corp=ticker,
                            bsns_year=year,
                            reprt_code=quarter
                        )
                        if fs is not None and not fs.empty:
                            # 당기순이익 추출
                            net_income = fs[
                                fs['account_nm'].str.contains('당기순이익|순이익', na=False)
                            ]
                            if not net_income.empty:
                                eps_data.append({
                                    'year': year,
                                    'quarter': quarter,
                                    'net_income': net_income.iloc[0].get('thstrm_amount', 0),
                                })
                    except:
                        continue
            
            df = pd.DataFrame(eps_data)
            logger.debug(f"  EPS: {len(df)}분기 수집")
            return df
            
        except Exception as e:
            logger.warning(f"  EPS 수집 실패: {e}")
            return pd.DataFrame()
    
    def _get_operating_profit(self, ticker: str) -> pd.DataFrame:
        """영업이익 추이 → 적자→흑자 전환 감지"""
        try:
            current_year = datetime.now().year
            profit_data = []
            
            for year in range(current_year - 2, current_year + 1):
                try:
                    fs = self.dart.finstate(corp=ticker, bsns_year=year, reprt_code='11011')
                    if fs is not None and not fs.empty:
                        op = fs[fs['account_nm'].str.contains('영업이익', na=False)]
                        if not op.empty:
                            profit_data.append({
                                'year': year,
                                'operating_profit': op.iloc[0].get('thstrm_amount', 0),
                            })
                except:
                    continue
            
            return pd.DataFrame(profit_data)
            
        except Exception as e:
            logger.warning(f"  영업이익 수집 실패: {e}")
            return pd.DataFrame()
    
    def _get_recent_filings(self, ticker: str) -> pd.DataFrame:
        """최근 공시 목록 (90일)"""
        try:
            filings = self.dart.list(corp=ticker, kind='', duration=90)
            if filings is not None:
                logger.debug(f"  공시: {len(filings)}건 수집")
                return filings
            return pd.DataFrame()
        except Exception as e:
            logger.warning(f"  공시 수집 실패: {e}")
            return pd.DataFrame()
    
    def _get_major_shareholder(self, ticker: str) -> pd.DataFrame:
        """
        대주주 현황
        → 주식담보 대출이 있는 경우 반대매매 가격 = 절대 바닥
        
        서보성: "이재용이 삼성 4.7~4.8만원에 담보 잡혔는데, 
                거기까지 내릴 리가 없지"
        """
        try:
            # 대주주 지분 변동 공시 검색
            filings = self.dart.list(
                corp=ticker,
                kind='G',  # 지분공시
                duration=365
            )
            if filings is not None:
                logger.debug(f"  대주주 공시: {len(filings)}건")
                return filings
            return pd.DataFrame()
        except Exception as e:
            logger.warning(f"  대주주 수집 실패: {e}")
            return pd.DataFrame()
    
    def _empty_result(self) -> Dict:
        """빈 결과 반환"""
        return {
            'eps_history': pd.DataFrame(),
            'operating_profit': pd.DataFrame(),
            'filings': pd.DataFrame(),
            'major_shareholder': pd.DataFrame(),
        }
