"""
Trading Engine - 핵심 매매 엔진
=================================
모든 컴포넌트를 연결하는 중앙 오케스트레이터

데이터 흐름:
  틱 수신 → 분봉 갱신 → 봉 마감 시 전략 평가 → 리스크 승인 → 주문 실행

모드:
  - paper: 모의매매 (PaperTrader)
  - live: 실매매 (KiwoomOrder)
"""

import logging
from datetime import datetime
from typing import Dict, List, Optional

import yaml
try:
    from PyQt5.QtCore import QTimer
except ImportError:
    QTimer = None

from strategies.base_strategy import BaseStrategy, TradeSignal, Signal
from strategies.ma_crossover import MACrossoverStrategy
from strategies.volume_spike import VolumeSpikeStrategy
from strategies.trend_breakout import TrendBreakoutStrategy
from strategies.five_min_scalping import FiveMinScalpingStrategy
from strategies.composite import CompositeStrategy

from engine.portfolio import Portfolio
from engine.market_state import MarketState
from engine.order_manager import OrderManager

from data.tick_buffer import TickBuffer
from data.candle_builder import CandleBuilder
from data.indicator_calc import IndicatorCalc
from data.trade_log import TradeLog

from risk.risk_manager import RiskManager
from risk.daily_guard import DailyGuard

from output.telegram_alert import TelegramAlert
from output.daily_report import DailyReport

from backtest.paper_trader import PaperTrader

logger = logging.getLogger('Scalper.Engine')

# 전략 레지스트리
STRATEGY_MAP = {
    'ma_crossover': MACrossoverStrategy,
    'volume_spike': VolumeSpikeStrategy,
    'trend_breakout': TrendBreakoutStrategy,
    'five_min_scalping': FiveMinScalpingStrategy,
}


class TradingEngine:
    """단타 자동매매 핵심 엔진"""

    def __init__(self, config: dict, mode: str = 'paper'):
        self.config = config
        self.mode = mode
        self.is_running = False

        # 포트폴리오
        self.portfolio = Portfolio(config)

        # 리스크
        self.daily_guard = DailyGuard(config)
        self.risk_manager = RiskManager(config, self.portfolio, self.daily_guard)

        # 주문
        self.order_manager = OrderManager()

        # 시장 상태
        self.market_state = MarketState(config)

        # 데이터
        self.tick_buffers: Dict[str, TickBuffer] = {}
        self.candle_builders: Dict[str, CandleBuilder] = {}

        # 전략
        self.strategy = self._load_strategy()

        # 출력
        self.telegram = TelegramAlert(config)
        self.trade_log = TradeLog(config)
        self.daily_report = DailyReport(config)

        # 모의매매 / 실매매
        self.paper_trader: Optional[PaperTrader] = None
        self.kiwoom_core = None
        self.kiwoom_rt = None
        self.kiwoom_order = None

        if mode == 'paper':
            self.portfolio.init_from_config()
            self.paper_trader = PaperTrader(config, self.portfolio)
            self.paper_trader.on_chejan(self._on_order_filled)
        # live 모드는 외부에서 kiwoom 컴포넌트 주입

        # 타이머 (1분마다 상태 체크)
        self._status_timer = None
        if QTimer is not None:
            self._status_timer = QTimer()
            self._status_timer.timeout.connect(self._periodic_check)

    def set_kiwoom(self, core, realtime, order, data_api):
        """실매매 모드: 키움 컴포넌트 주입"""
        self.kiwoom_core = core
        self.kiwoom_rt = realtime
        self.kiwoom_order = order
        self.kiwoom_order.on_chejan(self._on_order_filled)

        # 실계좌 정보 로드
        account = self.config['kiwoom']['account']
        deposit = data_api.get_deposit(account)
        balance = data_api.get_account_balance(account)
        self.portfolio.init_from_account(deposit, balance.get('positions', []))

    # === 시작/종료 ===

    def start(self, watch_codes: List[str]):
        """매매 시작"""
        self.watch_codes = watch_codes
        self.is_running = True

        period = self.config['candles']['period_minutes']

        # 종목별 데이터 구조 초기화
        for code in watch_codes:
            self.tick_buffers[code] = TickBuffer()
            cb = CandleBuilder(period_minutes=period)
            cb.on_candle_close = lambda candle, c=code: self._on_candle_close(c, candle)
            self.candle_builders[code] = cb

        # 실시간 구독 (live 모드)
        if self.mode == 'live' and self.kiwoom_rt:
            self.kiwoom_rt.subscribe(watch_codes, on_tick=self._on_tick)

        # 상태 체크 타이머 시작 (60초)
        if self._status_timer:
            self._status_timer.start(60000)

        logger.info(f"매매 시작: {watch_codes} (모드: {self.mode})")
        logger.info(f"전략: {self.strategy.name}")
        logger.info(f"초기 자금: {self.portfolio.total_eval:,}원")

        self.telegram._send(
            f"Scalper Agent 시작\n"
            f"모드: {self.mode}\n"
            f"종목: {', '.join(watch_codes)}\n"
            f"자금: {self.portfolio.total_eval:,}원"
        )

    def stop(self):
        """매매 종료"""
        self.is_running = False
        if self._status_timer:
            self._status_timer.stop()

        if self.mode == 'live' and self.kiwoom_rt:
            self.kiwoom_rt.unsubscribe_all()

        # 일일 리포트 생성
        report = self.daily_report.generate(
            self.portfolio.get_summary(),
            self.order_manager.get_filled_today(),
            self.daily_guard.get_summary(),
        )

        # 텔레그램 요약
        self.telegram.send_daily_summary({
            **self.portfolio.get_summary(),
            **self.daily_guard.get_summary(),
        })

        # 로그 저장
        self.trade_log.save_daily_summary()

        logger.info("매매 종료")

    # === 이벤트 핸들러 ===

    def _on_tick(self, code: str, tick: Dict):
        """실시간 틱 수신"""
        if not self.is_running:
            return

        # 틱 버퍼에 저장
        if code in self.tick_buffers:
            self.tick_buffers[code].add(tick)

        # 분봉 갱신
        if code in self.candle_builders:
            self.candle_builders[code].add_tick(tick)

        # 포트폴리오 가격 갱신
        price = tick.get('price', 0)
        self.portfolio.update_price(code, price)

        # 모의매매: 대기 주문 체결 확인
        if self.paper_trader:
            self.paper_trader.check_pending_fills(code, price)

        # 손절/익절 체크
        self._check_exits(code, price)

    def _on_candle_close(self, code: str, candle: Dict):
        """분봉 마감 → 전략 평가"""
        if not self.is_running:
            return
        if not self.market_state.can_trade():
            return
        if not self.daily_guard.is_trading_allowed():
            return

        # 전략 실행
        candles_df = self.candle_builders[code].get_candles()
        tick_data = self.tick_buffers[code].get_recent(1)[0] if not self.tick_buffers[code].is_empty else None

        signal = self.strategy.evaluate(code, candles_df, tick_data)

        if signal is None:
            return

        logger.info(f"신호 감지: {code} {signal.signal.value} (신뢰도 {signal.confidence:.2f}) - {signal.reason}")

        # 매매 실행
        self._execute_signal(signal)

    def _on_order_filled(self, data: Dict):
        """주문 체결 이벤트"""
        self.order_manager.on_chejan(data)
        self.trade_log.log_trade(data)

        # 텔레그램 알림
        self.telegram.send_trade(data)

        # 일일 가드 갱신
        self.daily_guard.update_unrealized(self.portfolio.total_unrealized_pnl)

    # === 매매 실행 ===

    def _execute_signal(self, signal: TradeSignal):
        """신호에 따라 매매 실행"""
        if signal.is_buy:
            self._execute_buy(signal)
        elif signal.is_sell:
            self._execute_sell(signal)

    def _execute_buy(self, signal: TradeSignal):
        """매수 실행"""
        price = self.tick_buffers[signal.code].get_last_price() if signal.code in self.tick_buffers else 0
        if price <= 0:
            return

        approved, qty, reason = self.risk_manager.approve_buy(signal, price)
        self.trade_log.log_signal(signal, approved, reason)

        if not approved:
            logger.info(f"매수 거부: {signal.code} - {reason}")
            return

        # 손절/익절 설정
        stop_loss = signal.stop_loss
        take_profit = signal.take_profit
        if stop_loss == 0 or take_profit == 0:
            default_sl, default_tp = self.risk_manager.calc_default_exits(price)
            if stop_loss == 0:
                stop_loss = default_sl
            if take_profit == 0:
                take_profit = default_tp

        if self.mode == 'paper':
            order_no = self.paper_trader.buy_market(signal.code, qty, price)
            # 손절/익절 설정
            pos = self.portfolio.get_position(signal.code)
            if pos:
                pos.stop_loss = stop_loss
                pos.take_profit = take_profit
        elif self.kiwoom_order:
            ret = self.kiwoom_order.buy_market(signal.code, qty)
            self.order_manager.register(str(ret), signal.code, 'buy', qty, price)

        logger.info(f"매수 실행: {signal.code} {qty}주 @ {price:,} (SL={stop_loss:,} TP={take_profit:,})")

    def _execute_sell(self, signal: TradeSignal):
        """매도 실행"""
        approved, qty, reason = self.risk_manager.approve_sell(signal)
        self.trade_log.log_signal(signal, approved, reason)

        if not approved:
            logger.info(f"매도 거부: {signal.code} - {reason}")
            return

        price = self.tick_buffers[signal.code].get_last_price() if signal.code in self.tick_buffers else 0

        if self.mode == 'paper':
            self.paper_trader.sell_market(signal.code, qty, price)
        elif self.kiwoom_order:
            ret = self.kiwoom_order.sell_market(signal.code, qty)
            self.order_manager.register(str(ret), signal.code, 'sell', qty, price)

        # 손익 기록
        pos = self.portfolio.get_position(signal.code)
        if pos:
            pnl = (price - pos.avg_price) * qty
            self.daily_guard.record_trade(pnl)

        logger.info(f"매도 실행: {signal.code} {qty}주 @ {price:,} - {signal.reason}")

    def _check_exits(self, code: str, current_price: int):
        """손절/익절 확인"""
        if current_price <= 0:
            return

        # 손절
        sl_signal = self.risk_manager.check_stop_loss(code, current_price)
        if sl_signal:
            logger.warning(f"손절 트리거: {code} @ {current_price:,}")
            self._execute_sell(sl_signal)
            return

        # 익절
        tp_signal = self.risk_manager.check_take_profit(code, current_price)
        if tp_signal:
            logger.info(f"익절 트리거: {code} @ {current_price:,}")
            self._execute_sell(tp_signal)

    # === 주기적 체크 ===

    def _periodic_check(self):
        """1분마다 실행되는 상태 점검"""
        if not self.is_running:
            return

        # 장마감 청산
        if self.market_state.should_close_positions():
            self._close_all_positions("장마감 청산")

        # 일일 한도 체크
        if not self.daily_guard.is_trading_allowed():
            self.telegram.send_risk_alert(self.daily_guard.lock_reason)

    def _close_all_positions(self, reason: str):
        """전 종목 청산"""
        for code, pos in list(self.portfolio.positions.items()):
            signal = TradeSignal(
                signal=Signal.SELL, code=code, confidence=1.0,
                reason=reason, strategy_name="CloseAll",
            )
            self._execute_sell(signal)

    # === 전략 로드 ===

    def _load_strategy(self) -> BaseStrategy:
        """config에서 전략 로드"""
        active = self.config['strategies']['active']

        strategies = []
        for name in active:
            cls = STRATEGY_MAP.get(name)
            if cls:
                strategies.append(cls(self.config))
                logger.info(f"전략 로드: {name}")

        if len(strategies) == 1:
            return strategies[0]
        elif len(strategies) > 1:
            return CompositeStrategy(self.config, strategies)
        else:
            logger.warning("활성 전략 없음, 기본 MA 교차 전략 사용")
            return MACrossoverStrategy(self.config)

    # === 수동 틱 주입 (모의매매/백테스트용) ===

    def inject_tick(self, code: str, tick: Dict):
        """외부에서 틱 데이터 주입 (모의매매 시 사용)"""
        self._on_tick(code, tick)
