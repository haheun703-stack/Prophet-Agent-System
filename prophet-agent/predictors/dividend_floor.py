"""
💰 배당수익률 바닥 감지기
=========================
서보성: "삼성 5만원이면 배당수익률 3.88%. 여기 앉아있으면 배당 받고, 
        위는 무한대야."

로직: 5년 배당수익률 히스토리에서 현재가 역사적 고점 구간이면 = 바닥
"""

import logging
from typing import Dict
import pandas as pd

logger = logging.getLogger('Prophet.Dividend')


class DividendFloorPredictor:
    """배당수익률 기반 바닥 감지기"""
    
    def __init__(self, config: dict):
        self.config = config
        self.floor_pct = config['thresholds']['div_yield_floor_pct']
        self.max_score = config['weights']['dividend_floor']  # 15점
    
    def predict(self, data: Dict) -> Dict:
        try:
            market = data.get('market', {})
            fundamental = market.get('fundamental', pd.DataFrame())
            
            if fundamental.empty or 'DIV' not in fundamental.columns:
                return {'score': 0, 'detail': '배당 데이터 없음', 'div_yield': 0, 'percentile': 0}
            
            div_series = fundamental['DIV'].dropna()
            if len(div_series) < 20:
                return {'score': 0, 'detail': '배당 데이터 부족', 'div_yield': 0, 'percentile': 0}
            
            current_div = div_series.iloc[-1]
            percentile = (div_series < current_div).sum() / len(div_series) * 100
            
            # 배당수익률이 역사적으로 높을수록(=주가가 낮음) = 바닥 신호
            if percentile >= 80:
                score = self.max_score
                detail = f'💰 배당 바닥 구간! (DIV {current_div:.2f}%, 상위 {100-percentile:.0f}%)'
            elif percentile >= 60:
                score = int(self.max_score * 0.5)
                detail = f'📊 배당 양호 (DIV {current_div:.2f}%, 상위 {100-percentile:.0f}%)'
            else:
                score = 0
                detail = f'배당수익률 보통 (DIV {current_div:.2f}%)'
            
            return {
                'score': score,
                'detail': detail,
                'div_yield': current_div,
                'percentile': percentile,
            }
            
        except Exception as e:
            logger.warning(f"배당 분석 실패: {e}")
            return {'score': 0, 'detail': f'분석 불가: {e}', 'div_yield': 0, 'percentile': 0}
