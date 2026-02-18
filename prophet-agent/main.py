"""
🔮 Prophet Agent v1.0 - "예언자"
=================================
"3개월 뒤 포물선이 시작될 종목을 미리 찾는다"

현재 시스템(포물선의 초점 v8.1)과의 역할 분담:
  - 예언자: "어디가 터질지" (종목 선정, 3개월 시야)
  - 포물선 초점: "언제 들어갈지" (진입 타이밍, 실시간)

Usage:
  python main.py                    # 전체 분석 실행
  python main.py --ticker 005930    # 삼성전자만 분석
  python main.py --scan             # 전종목 스캔 → 상위 20 출력
  python main.py --monitor          # 실시간 모니터링 모드
"""

import argparse
import logging
import yaml
import pandas as pd
from datetime import datetime
from pathlib import Path

# --- Collectors ---
from collectors.pykrx_pipe import PykrxCollector
from collectors.dart_pipe import DartCollector
from collectors.yahoo_pipe import YahooCollector
from collectors.naver_pipe import NaverCollector
from collectors.whale_pipe import WhaleCollector

# --- Predictors ---
from predictors.eps_divergence import EPSDivergencePredictor
from predictors.credit_danger import CreditDangerPredictor
from predictors.dividend_floor import DividendFloorPredictor
from predictors.liquidation_floor import LiquidationFloorPredictor
from predictors.whale_tracker import WhaleTracker
from predictors.chicken_survivor import ChickenSurvivorPredictor

# --- Synthesizer ---
from synthesizer.prophet_score import ProphetSynthesizer

# --- Output ---
from output.telegram_alert import TelegramAlert


def load_config(path="config.yaml"):
    """설정 파일 로드"""
    with open(path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)


def setup_logging(config):
    """로깅 설정"""
    log_level = getattr(logging, config['output']['log_level'])
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s [%(name)s] %(levelname)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    return logging.getLogger('Prophet')


def get_universe(config):
    """분석 대상 종목 유니버스 생성"""
    collector = PykrxCollector(config)
    universe = collector.get_universe(
        markets=config['universe']['market'],
        min_market_cap=config['universe']['min_market_cap'],
        max_stocks=config['universe']['max_stocks']
    )
    return universe


def analyze_single(ticker: str, config: dict, logger: logging.Logger) -> dict:
    """
    단일 종목 예언자 분석
    
    Returns:
        {
            'ticker': '005930',
            'name': '삼성전자',
            'prophet_score': 78,
            'signals': {
                'eps_divergence': {'score': 25, 'detail': 'EPS↑15% vs 주가↓8%'},
                'credit_danger': {'score': 0, 'detail': '신용잔고 정상'},
                ...
            },
            'verdict': '🔮 포물선 시작 임박',
            'timestamp': '2026-02-16 12:00:00'
        }
    """
    logger.info(f"분석 시작: {ticker}")
    
    # Step 1: 데이터 수집 (5개 파이프에서 동시 수집)
    pykrx = PykrxCollector(config)
    dart = DartCollector(config)
    yahoo = YahooCollector(config)
    naver = NaverCollector(config)
    whale = WhaleCollector(config)
    
    data = {
        'market': pykrx.collect(ticker),      # OHLCV, 수급, 공매도, 시총
        'fundamental': dart.collect(ticker),    # EPS, 영업이익, 공시
        'macro': yahoo.collect(),               # VIX, SOXX, 환율
        'sentiment': naver.collect(ticker),     # 신용잔고, 뉴스, 토론실
        'whale': whale.collect(ticker),         # 고래 동향
    }
    
    # Step 2: 예측 두뇌 각각 실행
    predictors = {
        'eps_divergence': EPSDivergencePredictor(config).predict(data),
        'credit_danger': CreditDangerPredictor(config).predict(data),
        'dividend_floor': DividendFloorPredictor(config).predict(data),
        'liquidation_floor': LiquidationFloorPredictor(config).predict(data),
        'whale_tracking': WhaleTracker(config).predict(data),
        'chicken_survivor': ChickenSurvivorPredictor(config).predict(data),
    }
    
    # Step 3: 종합 스코어링
    synthesizer = ProphetSynthesizer(config)
    result = synthesizer.synthesize(ticker, predictors)
    
    logger.info(f"분석 완료: {ticker} → {result['prophet_score']}점 ({result['verdict']})")
    
    return result


def scan_universe(config: dict, logger: logging.Logger) -> pd.DataFrame:
    """전체 유니버스 스캔 → 상위 종목 추출"""
    universe = get_universe(config)
    logger.info(f"유니버스 {len(universe)}종목 스캔 시작")
    
    results = []
    for ticker in universe:
        try:
            result = analyze_single(ticker, config, logger)
            results.append(result)
        except Exception as e:
            logger.warning(f"{ticker} 분석 실패: {e}")
            continue
    
    # DataFrame 변환 및 정렬
    df = pd.DataFrame(results)
    df = df.sort_values('prophet_score', ascending=False)
    
    # 결과 저장
    save_dir = Path(config['output']['save_dir'])
    save_dir.mkdir(parents=True, exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filepath = save_dir / f"prophet_scan_{timestamp}.csv"
    df.to_csv(filepath, index=False, encoding='utf-8-sig')
    logger.info(f"스캔 결과 저장: {filepath}")
    
    return df


def monitor_mode(config: dict, logger: logging.Logger):
    """실시간 모니터링 모드 (장중 1시간 간격)"""
    import time
    
    alert = TelegramAlert(config)
    interval = config['collection']['update_interval']
    threshold = config['thresholds']['prophet_alert']
    
    logger.info(f"모니터링 모드 시작 (간격: {interval}초, 알림 기준: {threshold}점)")
    
    while True:
        try:
            df = scan_universe(config, logger)
            
            # 기준 이상 종목 알림
            hot_stocks = df[df['prophet_score'] >= threshold]
            if not hot_stocks.empty:
                alert.send_prophet_alert(hot_stocks)
                logger.info(f"🔮 알림 전송: {len(hot_stocks)}종목")
            
            time.sleep(interval)
            
        except KeyboardInterrupt:
            logger.info("모니터링 종료")
            break
        except Exception as e:
            logger.error(f"모니터링 에러: {e}")
            time.sleep(60)  # 에러 시 1분 대기 후 재시도


def main():
    parser = argparse.ArgumentParser(description='🔮 Prophet Agent - 포물선 예언자')
    parser.add_argument('--ticker', type=str, help='단일 종목 분석 (예: 005930)')
    parser.add_argument('--scan', action='store_true', help='전종목 스캔')
    parser.add_argument('--monitor', action='store_true', help='실시간 모니터링')
    parser.add_argument('--config', type=str, default='config.yaml', help='설정 파일 경로')
    args = parser.parse_args()
    
    config = load_config(args.config)
    logger = setup_logging(config)
    
    logger.info("=" * 50)
    logger.info("🔮 Prophet Agent v1.0 시작")
    logger.info("=" * 50)
    
    if args.ticker:
        result = analyze_single(args.ticker, config, logger)
        print(f"\n{'='*50}")
        print(f"🔮 예언 결과: {result['name']} ({result['ticker']})")
        print(f"   점수: {result['prophet_score']}점")
        print(f"   판정: {result['verdict']}")
        print(f"{'='*50}")
        for key, signal in result['signals'].items():
            print(f"   {key}: {signal['score']}점 - {signal['detail']}")
            
    elif args.scan:
        df = scan_universe(config, logger)
        print(f"\n🔮 상위 20 종목:")
        print(df.head(20).to_string())
        
    elif args.monitor:
        monitor_mode(config, logger)
        
    else:
        # 기본: 전종목 스캔
        df = scan_universe(config, logger)
        print(f"\n🔮 상위 20 종목:")
        print(df.head(20).to_string())


if __name__ == '__main__':
    main()
