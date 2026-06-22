"""
🔮 Prophet Synthesizer - 예언 종합 판정
========================================
6개 예측 두뇌의 결과를 종합하여 최종 점수 산출

점수 체계:
  EPS 괴리도:    0~30점 (최고 가중치) ⭐
  신용 안전도:   -50~0점 (위험하면 모든 걸 차단)
  배당 바닥:     0~15점
  고래 진입:     0~15점
  치킨게임:      0~10점
  반대매매 바닥: 0~5점 (보조)
  ──────────────
  총점 100점 만점

판정:
  80+ = 🔮🔮🔮 "포물선 임박 - 적극 매수 준비"
  60+ = 🔮🔮   "포물선 가능성 높음 - 모니터링 강화"
  40+ = 🔮     "관심 종목 - 관찰"
  0~39 =        "신호 미약"
  음수 =  🚫   "위험 - 절대 금지"
"""

import logging
from datetime import datetime
from typing import Dict, List

logger = logging.getLogger('Prophet.Synthesizer')


class ProphetSynthesizer:
    """예언 종합 판정기"""
    
    def __init__(self, config: dict):
        self.config = config
        self.alert_threshold = config['thresholds']['prophet_alert']
        self.danger_threshold = config['thresholds']['danger_zone']
    
    def synthesize(self, ticker: str, predictors: Dict[str, Dict]) -> Dict:
        """
        모든 예측기 결과를 종합하여 최종 예언 산출
        
        Args:
            ticker: 종목코드
            predictors: {
                'eps_divergence': {'score': 25, 'detail': '...'},
                'credit_danger': {'score': -50, 'detail': '...'},
                ...
            }
        
        Returns:
            {
                'ticker': '005930',
                'name': '삼성전자',  # TODO: 종목명 매핑
                'prophet_score': 78,
                'signals': predictors,
                'verdict': '🔮🔮🔮 포물선 임박',
                'timestamp': '2026-02-16T12:00:00',
            }
        """
        # 1. 총점 계산
        total_score = sum(p.get('score', 0) for p in predictors.values())
        
        # 2. 신용잔고 차단 로직 (가장 중요한 안전장치)
        credit = predictors.get('credit_danger', {})
        credit_danger = credit.get('danger_level', 'normal')
        
        if credit_danger == 'extreme':
            # 신용 극단 과열 시 모든 점수를 0 이하로 강제
            total_score = min(total_score, -10)
            logger.warning(f"🚫 {ticker}: 신용잔고 극단 과열 → 점수 강제 차단")
        
        # 3. 판정
        verdict = self._get_verdict(total_score)
        
        # 4. 결과 조립
        result = {
            'ticker': ticker,
            'name': self._get_name(ticker),
            'prophet_score': total_score,
            'signals': predictors,
            'verdict': verdict,
            'timestamp': datetime.now().isoformat(),
        }
        
        # 5. 상세 로그
        self._log_result(result)
        
        return result
    
    def _get_verdict(self, score: int) -> str:
        """점수 → 판정 문구"""
        if score < self.danger_threshold:
            return '🚫 절대 금지 - 시장 과열'
        elif score < 0:
            return '⚠️ 위험 - 진입 자제'
        elif score < 40:
            return '📊 신호 미약 - 관찰'
        elif score < 60:
            return '🔮 관심 종목 - 모니터링'
        elif score < 80:
            return '🔮🔮 포물선 가능성 높음 - 모니터링 강화'
        else:
            return '🔮🔮🔮 포물선 임박 - 적극 매수 준비'
    
    def _get_name(self, ticker: str) -> str:
        """종목코드 → 종목명 매핑"""
        try:
            from collectors.krx_gate import krx_enabled
            if not krx_enabled():
                return ticker  # KRX 차단(6/22 IP 대응) — 이름 조회 skip, ticker 그대로
            from pykrx import stock
            today = datetime.now().strftime('%Y%m%d')
            name = stock.get_market_ticker_name(ticker)
            return name if name else ticker
        except Exception:
            return ticker
    
    def _log_result(self, result: Dict):
        """상세 분석 결과 로그"""
        logger.info(f"{'='*60}")
        logger.info(f"🔮 예언 결과: {result['name']} ({result['ticker']})")
        logger.info(f"   총점: {result['prophet_score']}점")
        logger.info(f"   판정: {result['verdict']}")
        logger.info(f"{'─'*60}")
        
        for key, signal in result['signals'].items():
            score = signal.get('score', 0)
            detail = signal.get('detail', '')
            marker = '🔴' if score < 0 else ('🟢' if score > 0 else '⚪')
            logger.info(f"   {marker} {key}: {score:+d}점 | {detail}")
        
        logger.info(f"{'='*60}")
    
    def rank_universe(self, results: List[Dict]) -> List[Dict]:
        """
        전체 유니버스 결과를 순위별로 정렬
        
        Returns:
            정렬된 결과 리스트 (고득점 순)
        """
        # 위험 종목 분리
        safe = [r for r in results if r['prophet_score'] >= 0]
        danger = [r for r in results if r['prophet_score'] < 0]
        
        # 안전 종목 내 고득점 순
        safe.sort(key=lambda x: x['prophet_score'], reverse=True)
        
        return safe + danger
