"""
⭐ EPS 괴리도 감지기 - 예언자의 핵심 두뇌
==========================================
서보성 핵심 원칙: "주가는 EPS에 수렴한다"

패턴 분류:
  1. EPS↑ + 주가↓ = "스프링 로딩" → 3개월 내 수렴 상승 예상 (매수)
  2. EPS↓ + 주가↑ = "공중부양"   → 임박한 조정 예상 (매도/관망)
  3. EPS↑ + 주가↑ = "건강한 상승" → 추세 유지 (보유)
  4. EPS↓ + 주가↓ = "추세 하락"   → 바닥 확인 전 관망

스프링 로딩(패턴 1)이 포물선의 가장 강력한 선행 신호.
"""

import logging
from typing import Dict

import pandas as pd
import numpy as np

logger = logging.getLogger('Prophet.EPS')


class EPSDivergencePredictor:
    """EPS-주가 괴리도 기반 포물선 예측기"""
    
    def __init__(self, config: dict):
        self.config = config
        self.divergence_threshold = config['thresholds']['eps_divergence_pct']
        self.max_score = config['weights']['eps_divergence']  # 30점
    
    def predict(self, data: Dict) -> Dict:
        """
        EPS 괴리도 분석 → 스코어 산출
        
        Args:
            data: {
                'market': pykrx 데이터 (OHLCV 포함),
                'fundamental': DART 데이터 (EPS 포함),
                ...
            }
        
        Returns:
            {
                'score': 0~30,
                'detail': '설명 문자열',
                'pattern': 'spring_loading' | 'floating' | 'healthy' | 'falling',
                'eps_growth': float,    # EPS 성장률 (%)
                'price_growth': float,  # 주가 성장률 (%)
                'divergence': float,    # 괴리도 (%)
            }
        """
        try:
            # 1. EPS 성장률 계산
            eps_growth = self._calc_eps_growth(data.get('fundamental', {}))
            
            # 2. 주가 성장률 계산 (같은 기간)
            price_growth = self._calc_price_growth(data.get('market', {}))
            
            # 3. 괴리도 = EPS 성장률 - 주가 성장률
            divergence = eps_growth - price_growth
            
            # 4. 패턴 분류
            pattern = self._classify_pattern(eps_growth, price_growth)
            
            # 5. 스코어 산출
            score = self._calc_score(pattern, divergence)
            
            # 6. 설명 생성
            detail = self._generate_detail(pattern, eps_growth, price_growth, divergence)
            
            return {
                'score': score,
                'detail': detail,
                'pattern': pattern,
                'eps_growth': eps_growth,
                'price_growth': price_growth,
                'divergence': divergence,
            }
            
        except Exception as e:
            logger.warning(f"EPS 괴리도 분석 실패: {e}")
            return {
                'score': 0,
                'detail': f'분석 불가: {e}',
                'pattern': 'unknown',
                'eps_growth': 0,
                'price_growth': 0,
                'divergence': 0,
            }
    
    def _calc_eps_growth(self, fundamental: Dict) -> float:
        """
        EPS 성장률 계산 (YoY 또는 QoQ)
        
        최근 분기 EPS vs 전년 동기 EPS
        """
        eps_df = fundamental.get('eps_history', pd.DataFrame())
        
        if eps_df.empty or len(eps_df) < 2:
            return 0.0
        
        # 최근 2개 분기의 순이익으로 성장률 계산
        try:
            recent = float(eps_df.iloc[-1].get('net_income', 0))
            previous = float(eps_df.iloc[-2].get('net_income', 0))
            
            if previous == 0:
                # 적자→흑자 전환 = 극단적 성장
                if recent > 0:
                    return 100.0  # 흑자 전환 = 100% 성장
                return 0.0
            
            growth = (recent - previous) / abs(previous) * 100
            return round(growth, 2)
            
        except (ValueError, IndexError):
            return 0.0
    
    def _calc_price_growth(self, market: Dict) -> float:
        """
        주가 성장률 계산 (분기 기준, ~60 거래일)
        """
        ohlcv = market.get('ohlcv', pd.DataFrame())
        
        if ohlcv.empty or len(ohlcv) < 60:
            if not ohlcv.empty and len(ohlcv) >= 20:
                # 최소 20일이라도 있으면 계산
                current = ohlcv['종가'].iloc[-1]
                past = ohlcv['종가'].iloc[0]
            else:
                return 0.0
        else:
            current = ohlcv['종가'].iloc[-1]
            past = ohlcv['종가'].iloc[-60]  # 약 1분기 전
        
        if past == 0:
            return 0.0
        
        growth = (current - past) / past * 100
        return round(growth, 2)
    
    def _classify_pattern(self, eps_growth: float, price_growth: float) -> str:
        """
        4가지 패턴 분류
        
        핵심: 스프링 로딩(EPS↑ 주가↓)이 포물선 최강 신호
        """
        eps_up = eps_growth > 5   # EPS 5% 이상 성장
        price_up = price_growth > 0
        
        if eps_up and not price_up:
            return 'spring_loading'  # 🎯 포물선 최강 신호
        elif not eps_up and price_up:
            return 'floating'        # ⚠️ 공중부양 = 위험
        elif eps_up and price_up:
            return 'healthy'         # ✅ 건강한 상승
        else:
            return 'falling'         # 📉 추세 하락
    
    def _calc_score(self, pattern: str, divergence: float) -> int:
        """
        패턴 기반 스코어 산출 (0~30점)
        
        스프링 로딩 + 괴리도 클수록 높은 점수
        """
        if pattern == 'spring_loading':
            # 괴리도가 클수록 스프링이 강하게 눌려있음
            base = 15
            divergence_bonus = min(abs(divergence) / self.divergence_threshold * 15, 15)
            return min(int(base + divergence_bonus), self.max_score)
        
        elif pattern == 'healthy':
            # 건강한 상승이지만 이미 반영된 상태
            return min(int(abs(divergence) / self.divergence_threshold * 10), 10)
        
        elif pattern == 'floating':
            # 공중부양 = 감점은 안 하지만 0점
            return 0
        
        elif pattern == 'falling':
            # 하락 추세 = 0점 (바닥 확인 전)
            return 0
        
        return 0
    
    def _generate_detail(self, pattern: str, eps_growth: float, 
                         price_growth: float, divergence: float) -> str:
        """사람이 읽을 수 있는 분석 설명"""
        
        pattern_names = {
            'spring_loading': '🎯 스프링 로딩 (EPS↑ 주가↓)',
            'floating': '⚠️ 공중부양 (EPS↓ 주가↑)',
            'healthy': '✅ 건강한 상승 (EPS↑ 주가↑)',
            'falling': '📉 추세 하락 (EPS↓ 주가↓)',
            'unknown': '❓ 분석 불가',
        }
        
        name = pattern_names.get(pattern, '❓')
        return f"{name} | EPS {eps_growth:+.1f}% vs 주가 {price_growth:+.1f}% | 괴리도 {divergence:+.1f}%"
