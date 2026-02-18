"""
🐋 고래 추적기
===============
서보성: "피터 린치가 원자재 0%에서 3% 넣으면 그건 엄청난 신호"

로직:
  - 연기금 5일 연속 순매수 = 장기 상승 준비 (10점)
  - 외국인 10일 연속 순매수 = 강력한 매수 (15점)
  - 5% 이상 대량보유 신규 공시 = 고래 진입 (15점)
"""

import logging
from typing import Dict
import pandas as pd

logger = logging.getLogger('Prophet.Whale')


class WhaleTracker:
    """고래 추적 기반 예측기"""
    
    def __init__(self, config: dict):
        self.config = config
        self.max_score = config['weights']['whale_tracking']  # 15점
    
    def predict(self, data: Dict) -> Dict:
        try:
            whale = data.get('whale', {})
            score = 0
            details = []
            
            # 1. 연기금 연속 순매수
            pension = whale.get('pension_fund_flow', pd.DataFrame())
            if not pension.empty and 'pension_net_buy' in pension.columns:
                streak = self._calc_streak(pension['pension_net_buy'])
                if streak >= 5:
                    score += 10
                    details.append(f'연기금 {streak}일 연속 순매수')
                elif streak >= 3:
                    score += 5
                    details.append(f'연기금 {streak}일 연속 순매수')
            
            # 2. 외국인 연속 순매수
            foreign = whale.get('foreign_flow_trend', pd.DataFrame())
            if not foreign.empty and 'foreign_net_buy' in foreign.columns:
                streak = self._calc_streak(foreign['foreign_net_buy'])
                if streak >= 10:
                    score += 10
                    details.append(f'외국인 {streak}일 연속 순매수')
                elif streak >= 5:
                    score += 5
                    details.append(f'외국인 {streak}일 연속 순매수')
            
            # 3. 대량보유 신규 공시
            large = whale.get('large_shareholder', pd.DataFrame())
            if not large.empty and len(large) > 0:
                score += 5
                details.append(f'대량보유 공시 {len(large)}건')
            
            score = min(score, self.max_score)
            detail = ' | '.join(details) if details else '고래 움직임 없음'
            
            if score >= 10:
                detail = f'🐋 {detail}'
            
            return {'score': score, 'detail': detail}
            
        except Exception as e:
            logger.warning(f"고래 추적 실패: {e}")
            return {'score': 0, 'detail': f'분석 불가: {e}'}
    
    def _calc_streak(self, series: pd.Series) -> int:
        """연속 양수 일수 계산"""
        streak = 0
        for val in reversed(series.values):
            if val > 0:
                streak += 1
            else:
                break
        return streak
