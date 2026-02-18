"""
Scalper Agent v1.0 - 단타 자동매매
====================================
키움증권 Open API 기반 단타 자동매매 시스템

Usage:
  python main.py                        # 모의매매 (기본)
  python main.py --mode live            # 실매매 (실제 돈!)
  python main.py --codes 005930,000660  # 종목 지정
  python main.py --mode paper --demo    # 데모 모드 (시뮬레이션 틱)
  python main.py --backtest                       # 백테스트 (CSV 일봉)
  python main.py --backtest --codes 005930        # 특정 종목 백테스트
  python main.py --backtest --strategy five_min_scalping  # 5분 스캘핑 백테스트
  python main.py --backtest --start 2025-01-01 --end 2025-12-31  # 기간 지정
"""

import sys
import argparse
import logging
import time
from datetime import datetime
from pathlib import Path

import yaml

# 로깅 설정
def setup_logging(config: dict):
    level = getattr(logging, config['output']['log_level'], logging.INFO)
    log_dir = Path(config['output']['trade_log_dir'])
    log_dir.mkdir(parents=True, exist_ok=True)

    today = datetime.now().strftime('%Y%m%d')
    handlers = [
        logging.StreamHandler(),
        logging.FileHandler(log_dir / f"scalper_{today}.log", encoding='utf-8'),
    ]

    logging.basicConfig(
        level=level,
        format='%(asctime)s [%(name)s] %(levelname)s: %(message)s',
        datefmt='%H:%M:%S',
        handlers=handlers,
    )
    return logging.getLogger('Scalper')


def load_config(path: str = "config.yaml") -> dict:
    with open(path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)


def get_watch_codes(config: dict) -> list:
    """감시 종목 목록"""
    source = config['watchlist']['source']
    if source == 'prophet':
        import pandas as pd
        scan_path = config['watchlist']['prophet_scan_path']
        min_score = config['watchlist']['prophet_min_score']
        try:
            df = pd.read_csv(scan_path)
            codes = df[df['prophet_score'] >= min_score]['ticker'].tolist()
            return [str(c).zfill(6) for c in codes]
        except Exception as e:
            logging.warning(f"Prophet 스캔 파일 로드 실패: {e}")
    return config['watchlist']['codes']


def run_live(config, logger, watch_codes):
    """실매매 모드 (키움 HTS 필요)"""
    from PyQt5.QtWidgets import QApplication
    from PyQt5.QtCore import QTimer

    from api.kiwoom_core import KiwoomCore
    from api.kiwoom_data import KiwoomData
    from api.kiwoom_realtime import KiwoomRealtime
    from api.kiwoom_order import KiwoomOrder
    from engine.trading_engine import TradingEngine

    app = QApplication(sys.argv)

    # 키움 연결
    core = KiwoomCore()
    ret = core.login()
    if ret != 0:
        logger.error("키움 로그인 실패!")
        return

    # 계좌 설정
    accounts = core.get_account_list()
    if not accounts:
        logger.error("계좌 없음!")
        return

    account = config['kiwoom'].get('account') or accounts[0]
    config['kiwoom']['account'] = account
    logger.info(f"계좌: {account}")

    # 컴포넌트 초기화
    data_api = KiwoomData(core)
    realtime = KiwoomRealtime(core)
    order = KiwoomOrder(core, account)

    # 엔진 초기화
    engine = TradingEngine(config, mode='live')
    engine.set_kiwoom(core, realtime, order, data_api)

    # 종목명 출력
    for code in watch_codes:
        name = core.get_code_name(code)
        logger.info(f"감시 종목: {name} ({code})")

    # 시작
    QTimer.singleShot(1000, lambda: engine.start(watch_codes))
    sys.exit(app.exec_())


def run_paper(config, logger, watch_codes):
    """모의매매 모드"""
    from PyQt5.QtWidgets import QApplication
    from PyQt5.QtCore import QTimer

    from engine.trading_engine import TradingEngine

    app = QApplication(sys.argv)

    engine = TradingEngine(config, mode='paper')

    logger.info(f"모의매매 모드")
    logger.info(f"초기 자금: {config['backtest']['initial_cash']:,}원")
    logger.info(f"감시 종목: {watch_codes}")

    QTimer.singleShot(1000, lambda: engine.start(watch_codes))
    sys.exit(app.exec_())


def run_demo(config, logger, watch_codes):
    """
    데모 모드: 시뮬레이션 틱으로 전략 테스트
    PyQt5 없이 간단히 실행 가능
    """
    import numpy as np
    import pandas as pd
    from engine.trading_engine import TradingEngine
    from data.candle_builder import CandleBuilder
    from data.tick_buffer import TickBuffer
    from data.indicator_calc import IndicatorCalc
    from strategies.base_strategy import Signal

    logger.info("=== 데모 모드 ===")
    logger.info(f"감시 종목: {watch_codes}")
    logger.info(f"초기 자금: {config['backtest']['initial_cash']:,}원")

    # 엔진은 생성하되 QApplication 없이 수동으로 작동
    engine = TradingEngine.__new__(TradingEngine)
    engine.config = config
    engine.mode = 'paper'
    engine.is_running = True

    # 필요 컴포넌트만 초기화
    from engine.portfolio import Portfolio
    from risk.daily_guard import DailyGuard
    from risk.risk_manager import RiskManager
    from engine.order_manager import OrderManager
    from engine.market_state import MarketState
    from data.trade_log import TradeLog
    from output.telegram_alert import TelegramAlert
    from output.daily_report import DailyReport
    from backtest.paper_trader import PaperTrader

    engine.portfolio = Portfolio(config)
    engine.portfolio.init_from_config()
    engine.daily_guard = DailyGuard(config)
    engine.risk_manager = RiskManager(config, engine.portfolio, engine.daily_guard)
    engine.order_manager = OrderManager()
    engine.market_state = MarketState(config)
    engine.trade_log = TradeLog(config)
    engine.telegram = TelegramAlert(config)
    engine.daily_report = DailyReport(config)
    engine.paper_trader = PaperTrader(config, engine.portfolio)
    engine.paper_trader.on_chejan(engine._on_order_filled)
    engine.tick_buffers = {}
    engine.candle_builders = {}
    engine.watch_codes = watch_codes
    engine.kiwoom_core = None
    engine.kiwoom_rt = None
    engine.kiwoom_order = None
    # 데모용: 복합 전략 임계값 낮춤 (0.6 → 0.25)
    config['strategies']['composite']['min_confidence'] = 0.25
    engine.strategy = engine._load_strategy()

    code = watch_codes[0]
    logger.info(f"\n시뮬레이션 종목: {code}")

    # 시뮬레이션 분봉 데이터 생성 (명확한 트렌드 전환 포함)
    np.random.seed(42)

    # 틱 버퍼 & 봉 빌더 초기화
    engine.tick_buffers[code] = TickBuffer()
    cb = CandleBuilder(period_minutes=1)
    engine.candle_builders[code] = cb

    candles = []
    price = 70000
    for i in range(120):
        noise = np.random.normal(0, 80)
        if i < 30:
            trend = -30     # 하락
        elif i < 50:
            trend = -10     # 횡보
        elif i < 80:
            trend = 80      # 강한 상승 (골든크로스 유발)
        elif i < 100:
            trend = -50     # 하락 전환 (데드크로스)
        else:
            trend = 40      # 재상승

        o = price
        h = int(price + abs(np.random.normal(0, 200)))
        l = int(price - abs(np.random.normal(0, 200)))
        price = max(int(price + trend + noise), 50000)
        vol = int(np.random.exponential(10000) + 1000)
        if 45 <= i <= 55:
            vol = int(vol * 4)  # 트렌드 전환점에서 거래량 스파이크

        candles.append({
            'timestamp': datetime(2026, 2, 18, 9, 0) + __import__('datetime').timedelta(minutes=i),
            'open': o, 'high': h, 'low': l, 'close': price, 'volume': vol,
        })

    candles_df = pd.DataFrame(candles)
    logger.info(f"생성된 분봉: {len(candles_df)}개")
    logger.info(f"가격 범위: {candles_df['low'].min():,} ~ {candles_df['high'].max():,}")

    # 전략 평가
    signals_found = 0
    for i in range(30, len(candles_df)):
        partial = candles_df.iloc[:i+1].copy()

        # 마지막 틱 시뮬레이션
        last_price = int(partial['close'].iloc[-1])
        tick = {'price': last_price, 'volume': int(partial['volume'].iloc[-1]),
                'timestamp': datetime.now(), 'cumul_volume': 0}
        engine.tick_buffers[code].add(tick)

        signal = engine.strategy.evaluate(code, partial, tick)

        if signal:
            signals_found += 1
            logger.info(
                f"  봉 #{i}: {signal.signal.value.upper()} "
                f"(신뢰도 {signal.confidence:.2f}) - {signal.reason}"
            )

            # 매매 실행
            if signal.is_buy:
                approved, qty, reason = engine.risk_manager.approve_buy(signal, last_price)
                if approved:
                    engine.paper_trader.buy_market(code, qty, last_price)
                    pos = engine.portfolio.get_position(code)
                    if pos:
                        sl, tp = engine.risk_manager.calc_default_exits(last_price)
                        pos.stop_loss = signal.stop_loss or sl
                        pos.take_profit = signal.take_profit or tp
                    logger.info(f"    -> 매수 {qty}주 @ {last_price:,}")
                else:
                    logger.info(f"    -> 거부: {reason}")
            elif signal.is_sell:
                approved, qty, reason = engine.risk_manager.approve_sell(signal)
                if approved:
                    engine.paper_trader.sell_market(code, qty, last_price)
                    logger.info(f"    -> 매도 {qty}주 @ {last_price:,}")

        # 손절/익절 체크
        engine._check_exits(code, last_price)

    # 결과 출력
    summary = engine.portfolio.get_summary()
    logger.info(f"\n{'='*50}")
    logger.info(f"  데모 결과")
    logger.info(f"{'='*50}")
    logger.info(f"  신호 발생: {signals_found}회")
    logger.info(f"  총 평가: {summary['total_eval']:,}원")
    logger.info(f"  현금: {summary['cash']:,}원 ({summary['cash_ratio']:.1%})")
    logger.info(f"  실현손익: {summary['realized_pnl']:+,}원")
    logger.info(f"  미실현손익: {summary['unrealized_pnl']:+,}원")
    logger.info(f"  총 손익: {summary['total_pnl']:+,}원")
    logger.info(f"  보유 종목: {summary['position_count']}개")
    for pos in engine.portfolio.positions.values():
        logger.info(f"    {pos.code}: {pos.quantity}주 @ {pos.avg_price:,} (현재 {pos.current_price:,}, {pos.unrealized_pnl_rate:+.1f}%)")
    logger.info(f"{'='*50}")


def run_backtest(config, logger, watch_codes, strategy_name=None,
                 start_date=None, end_date=None):
    """
    백테스트 모드: stock_data_daily CSV 데이터로 전략 검증
    PyQt5 불필요
    """
    import pandas as pd
    from data.csv_loader import CSVLoader
    from backtest.backtester import ScalpingBacktester, BacktestConfig
    from backtest.performance import PerformanceAnalyzer
    from engine.trading_engine import STRATEGY_MAP

    logger.info("=" * 55)
    logger.info("  백테스트 모드")
    logger.info("=" * 55)

    # CSV 로더 초기화
    csv_dir = config.get('data', {}).get('csv_dir', '../stock_data_daily')
    loader = CSVLoader(csv_dir)
    available = loader.get_available_codes()
    logger.info(f"CSV 데이터: {len(available)}개 종목 로드 가능")

    # 전략 선택
    if strategy_name and strategy_name in STRATEGY_MAP:
        strategy_cls = STRATEGY_MAP[strategy_name]
        strategies_to_test = {strategy_name: strategy_cls(config)}
    elif strategy_name == 'all':
        strategies_to_test = {name: cls(config) for name, cls in STRATEGY_MAP.items()}
    else:
        # config의 active 전략 사용
        active = config['strategies']['active']
        strategies_to_test = {}
        for name in active:
            cls = STRATEGY_MAP.get(name)
            if cls:
                strategies_to_test[name] = cls(config)

    if not strategies_to_test:
        logger.error("활성 전략 없음!")
        return

    logger.info(f"전략: {list(strategies_to_test.keys())}")
    logger.info(f"종목: {watch_codes}")
    if start_date:
        logger.info(f"기간: {start_date} ~ {end_date or '최신'}")

    # 종목별 × 전략별 백테스트
    all_results = {}
    for code in watch_codes:
        # CSV 데이터 확인
        if code not in available:
            logger.warning(f"  {code}: CSV 데이터 없음 - 스킵")
            continue

        name = loader.get_code_name(code)
        logger.info(f"\n{'─'*55}")
        logger.info(f"  {name} ({code})")
        logger.info(f"{'─'*55}")

        for strat_name, strategy in strategies_to_test.items():
            bt_config = BacktestConfig(
                ticker=code,
                ticker_name=name,
                initial_cash=config['backtest']['initial_cash'],
                risk_per_trade=config['backtest'].get('risk_per_trade', 50000),
                commission_rate=config['backtest']['commission_bps'] / 10000,
                slippage_bps=config['backtest']['slippage_bps'],
            )

            bt = ScalpingBacktester(bt_config, strategy)
            results = bt.run_on_csv(loader, code, start_date, end_date)

            if results:
                stats = bt.print_report()
                bt.save_results(f"./results/backtest_{code}_{strat_name}.csv")
                key = f"{code}_{strat_name}"
                from dataclasses import asdict
                all_results[key] = pd.DataFrame([asdict(r) for r in results])
            else:
                logger.info(f"  [{strat_name}] 거래 없음")

    # 종합 비교
    if len(all_results) > 1:
        logger.info(f"\n{'='*55}")
        logger.info("  종합 비교")
        logger.info(f"{'='*55}")
        comparison = PerformanceAnalyzer.compare_tickers(all_results)
        PerformanceAnalyzer.print_comparison(comparison)

    logger.info(f"\n결과 저장 위치: ./results/")


def main():
    parser = argparse.ArgumentParser(description='Scalper Agent v1.0 - 단타 자동매매')
    parser.add_argument('--mode', choices=['paper', 'live'], default='paper',
                        help='매매 모드 (기본: paper)')
    parser.add_argument('--config', default='config.yaml', help='설정 파일 경로')
    parser.add_argument('--codes', type=str, help='종목코드 (쉼표 구분)')
    parser.add_argument('--demo', action='store_true', help='데모 모드 (시뮬레이션 데이터)')
    parser.add_argument('--backtest', action='store_true',
                        help='백테스트 모드 (CSV 일봉 데이터)')
    parser.add_argument('--strategy', type=str, default=None,
                        help='백테스트 전략 (ma_crossover, volume_spike, '
                             'trend_breakout, five_min_scalping, all)')
    parser.add_argument('--start', type=str, default=None, help='백테스트 시작일 (YYYY-MM-DD)')
    parser.add_argument('--end', type=str, default=None, help='백테스트 종료일 (YYYY-MM-DD)')
    args = parser.parse_args()

    config = load_config(args.config)
    logger = setup_logging(config)

    if args.codes:
        config['watchlist']['codes'] = args.codes.split(',')
        config['watchlist']['source'] = 'manual'

    watch_codes = get_watch_codes(config)

    logger.info("=" * 50)
    logger.info("  Scalper Agent v1.0 - 단타 자동매매")
    logger.info(f"  모드: {'backtest' if args.backtest else args.mode}")
    logger.info(f"  종목: {watch_codes}")
    logger.info("=" * 50)

    if args.backtest:
        run_backtest(config, logger, watch_codes, args.strategy,
                     args.start, args.end)
    elif args.demo:
        run_demo(config, logger, watch_codes)
    elif args.mode == 'live':
        logger.warning("*** 실매매 모드 - 실제 돈이 사용됩니다! ***")
        run_live(config, logger, watch_codes)
    else:
        run_paper(config, logger, watch_codes)


if __name__ == '__main__':
    main()
