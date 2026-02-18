"""
🔒 반대매매 바닥 감지기
========================
서보성: "이재용이 삼성 4.7~4.8만원에 담보 잡혔는데, 거기까지 내릴 리가 없지"

로직: 대주주 담보대출 가격 = 절대 바닥 → 그 근처에서 매수
Phase 1에서는 DART 공시 기반 단순 감지, Phase 2에서 자동 계산
"""

import logging
from typing import Dict
import pandas as pd

logger = logging.getLogger('Prophet.Liquidation')


class LiquidationFloorPredictor:
    """반대매매 바닥 감지기"""
    
    def __init__(self, config: dict):
        self.config = config
    
    def predict(self, data: Dict) -> Dict:
        try:
            fundamental = data.get('fundamental', {})
            filings = fundamental.get('major_shareholder', pd.DataFrame())
            
            # Phase 1: 대주주 담보 관련 공시 존재 여부만 체크
            has_pledge = False
            if not filings.empty and 'report_nm' in filings.columns:
                pledge_mask = filings['report_nm'].str.contains(
                    '담보|질권|주식등의대량', na=False
                )
                has_pledge = pledge_mask.any()
            
            if has_pledge:
                return {
                    'score': 5,  # 존재만으로 약간의 가산점
                    'detail': '🔒 대주주 담보 공시 존재 → 반대매매 바닥 가능성 (수동 확인 필요)',
                    'has_pledge': True,
                }
            
            return {
                'score': 0,
                'detail': '담보 공시 없음',
                'has_pledge': False,
            }
            
        except Exception as e:
            logger.warning(f"반대매매 분석 실패: {e}")
            return {'score': 0, 'detail': f'분석 불가: {e}', 'has_pledge': False}
