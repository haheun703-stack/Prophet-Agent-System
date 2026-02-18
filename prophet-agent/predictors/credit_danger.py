"""
🚫 신용잔고 위험 감지기 - 안전장치
===================================
서보성: "신용이 28~29조, 역대 최고치. 다 죽는 거야."

역할: 시장 전체가 과열되면 모든 매수를 차단하는 비상 브레이크
점수: -50 ~ 0 (감점 전용, 가산점 없음)

로직:
  - 신용잔고 역사적 상위 90% 이상 → -50점 (절대 금지)
  - 상위 70~90% → -20점 (경계)
  - 하위 30% 이하 → 0점 + 보너스 신호 (공포 바닥)
"""

import logging
from typing import Dict

logger = logging.getLogger('Prophet.Credit')


class CreditDangerPredictor:
    """신용잔고 기반 시장 과열 감지기"""
    
    def __init__(self, config: dict):
        self.config = config
        self.extreme_pct = config['thresholds']['credit_extreme_pct']
    
    def predict(self, data: Dict) -> Dict:
        """
        신용잔고 위험도 분석
        
        Returns:
            {
                'score': -50 ~ 0,
                'detail': str,
                'danger_level': 'extreme' | 'caution' | 'normal' | 'fear_bottom',
                'credit_balance': float,
                'credit_percentile': float,
            }
        """
        try:
            sentiment = data.get('sentiment', {})
            credit_balance = sentiment.get('credit_balance', 0)
            credit_percentile = sentiment.get('credit_percentile', 50)
            
            # 위험도 판단
            if credit_percentile >= 90:
                score = -50
                danger_level = 'extreme'
                detail = f'🚫 신용잔고 극단적 과열 ({credit_balance:.1f}조, 상위 {credit_percentile:.0f}%) → 진입 금지'
            elif credit_percentile >= 70:
                score = -20
                danger_level = 'caution'
                detail = f'⚠️ 신용잔고 경계 ({credit_balance:.1f}조, 상위 {credit_percentile:.0f}%)'
            elif credit_percentile <= 30:
                score = 0  # 감점은 안 하되, 바닥 신호로 활용
                danger_level = 'fear_bottom'
                detail = f'💚 신용잔고 저점 ({credit_balance:.1f}조, 하위 {100-credit_percentile:.0f}%) → 공포 바닥 가능성'
            else:
                score = 0
                danger_level = 'normal'
                detail = f'✅ 신용잔고 정상 ({credit_balance:.1f}조)'
            
            return {
                'score': score,
                'detail': detail,
                'danger_level': danger_level,
                'credit_balance': credit_balance,
                'credit_percentile': credit_percentile,
            }
            
        except Exception as e:
            logger.warning(f"신용잔고 분석 실패: {e}")
            return {
                'score': 0,
                'detail': f'분석 불가: {e}',
                'danger_level': 'unknown',
                'credit_balance': 0,
                'credit_percentile': 0,
            }
