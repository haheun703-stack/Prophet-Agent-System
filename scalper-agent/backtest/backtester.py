"""
Scalping Backtester - 5분봉 스캘핑 백테스터
=============================================
원본: files (3).zip / backtester.py를 기존 scalper-agent 구조에 통합

기능:
  - 일봉 CSV → 5분봉 시뮬레이션 → 전략 평가 → 매매 기록
  - 날짜별 순차 시뮬레이션
  - 통계 리포트 + CSV 저장
"""

import logging
from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

from strategies.base_strategy import BaseStrategy, Signal, TradeSignal
from data.csv_loader import CSVLoader

logger = logging.getLogger('Scalper.Backtester')


@dataclass
class BacktestConfig:
    """백테스트 설정"""
    ticker: str = "005930"
    ticker_name: str = ""
    initial_cash: int = 10_000_000
    risk_per_trade: float = 50_000    # 트레이드당 위험금액 (원)
    rr_ratio: float = 2.0
    commission_rate: float = 0.00015  # 편도 수수료 0.015%
    slippage_bps: int = 10            # 슬리피지 (bps)
    max_trades_day: int = 1           # 일 최대 거래 횟수
    candle_period: int = 5            # 분봉 주기 (분)


@dataclass
class BacktestResult:
    """개별 거래 결과"""
    date: object
    ticker: str
    direction: str      # "BUY" | "SELL"
    entry_price: float
    exit_price: float
    stop_loss: float
    take_profit: float
    result: str         # "TP_HIT" | "SL_HIT" | "TIMEOUT"
    pnl: float
    quantity: int
    entry_bar: int
    exit_bar: int
    strategy_name: str
    reason: str


class ScalpingBacktester:
    """
    전략 백테스터

    Usage:
        loader = CSVLoader()
        bt = ScalpingBacktester(config, strategy)
        results = bt.run_on_csv(loader, "005930", "2025-01-01", "2025-12-31")
        bt.print_report()
    """

    def __init__(self, config: BacktestConfig, strategy: BaseStrategy):
        self.config = config
        self.strategy = strategy
        self.results: List[BacktestResult] = []
        self._equity_curve: List[float] = []
        self._cash = config.initial_cash

    def run_on_csv(self, loader: CSVLoader, code: str,
                   start_date: str = None, end_date: str = None) -> List[BacktestResult]:
        """
        CSV 일봉 데이터를 5분봉으로 변환하여 백테스트 실행

        Args:
            loader: CSVLoader 인스턴스
            code: 종목코드
            start_date: 시작일
            end_date: 종료일
        """
        self.config.ticker = code
        self.config.ticker_name = loader.get_code_name(code)

        daily_df = loader.load(code, start_date, end_date)
        if daily_df is None or daily_df.empty:
            logger.warning(f"데이터 없음: {code}")
            return []

        return self.run_on_daily(daily_df)

    def run_on_daily(self, daily_df: pd.DataFrame) -> List[BacktestResult]:
        """
        일봉 데이터에서 직접 백테스트 실행
        (5분봉 시뮬레이션 없이 일봉 기반 전략 평가)
        """
        self.results = []
        self._equity_curve = [0.0]

        for i in range(30, len(daily_df)):
            partial = daily_df.iloc[:i+1].copy()
            result = self._evaluate_day(partial, i)
            if result:
                self.results.append(result)
                cum_pnl = self._equity_curve[-1] + result.pnl
                self._equity_curve.append(cum_pnl)

        logger.info(f"백테스트 완료: {len(self.results)}거래 "
                     f"({self.config.ticker} {self.config.ticker_name})")
        return self.results

    def run_on_5min(self, five_min_df: pd.DataFrame) -> List[BacktestResult]:
        """
        5분봉 DataFrame으로 직접 백테스트 (날짜별 그룹)
        """
        self.results = []
        self._equity_curve = [0.0]

        if 'date' in five_min_df.columns:
            five_min_df = five_min_df.copy()
            five_min_df['_date'] = pd.to_datetime(five_min_df['date']).dt.date
        elif 'timestamp' in five_min_df.columns:
            five_min_df = five_min_df.copy()
            five_min_df['_date'] = pd.to_datetime(five_min_df['timestamp']).dt.date
        else:
            logger.warning("날짜 컬럼 없음")
            return []

        for date, day_df in five_min_df.groupby('_date'):
            if len(day_df) < 3:
                continue
            result = self._simulate_5min_day(day_df, date)
            if result:
                self.results.append(result)
                cum_pnl = self._equity_curve[-1] + result.pnl
                self._equity_curve.append(cum_pnl)

        logger.info(f"5분봉 백테스트 완료: {len(self.results)}거래")
        return self.results

    def _evaluate_day(self, candles: pd.DataFrame, bar_idx: int) -> Optional[BacktestResult]:
        """일봉 기반 전략 평가 (기존 전략 호환)"""
        if hasattr(self.strategy, 'reset_code'):
            self.strategy.reset_code(self.config.ticker)

        signal = self.strategy.evaluate(self.config.ticker, candles)
        if signal is None:
            return None

        last = candles.iloc[-1]
        entry_price = float(last['close'])
        date = last.get('date', last.name)

        # 슬리피지 적용
        slip = entry_price * self.config.slippage_bps / 10000
        if signal.is_buy:
            entry_price += slip
        else:
            entry_price -= slip

        sl = signal.stop_loss if signal.stop_loss > 0 else int(entry_price * 0.98)
        tp = signal.take_profit if signal.take_profit > 0 else int(entry_price * 1.04)

        # 다음 봉으로 결과 판단 (Next_Close 활용)
        if 'next_close' in candles.columns:
            next_price = float(last['next_close']) if not pd.isna(last.get('next_close')) else entry_price
        else:
            next_price = entry_price

        # TP/SL 체크
        if signal.is_buy:
            if next_price >= tp:
                result_type = "TP_HIT"
                exit_price = tp
            elif next_price <= sl:
                result_type = "SL_HIT"
                exit_price = sl
            else:
                result_type = "TIMEOUT"
                exit_price = next_price
            pnl = exit_price - entry_price
        else:
            if next_price <= tp:
                result_type = "TP_HIT"
                exit_price = tp
            elif next_price >= sl:
                result_type = "SL_HIT"
                exit_price = sl
            else:
                result_type = "TIMEOUT"
                exit_price = next_price
            pnl = entry_price - exit_price

        # 수수료 차감
        comm = (entry_price + exit_price) * self.config.commission_rate
        pnl -= comm

        # 수량 계산 (리스크 기반)
        risk = abs(entry_price - sl)
        if risk > 0:
            qty = max(1, int(self.config.risk_per_trade / risk))
        else:
            qty = 1
        total_pnl = pnl * qty

        return BacktestResult(
            date=date,
            ticker=self.config.ticker,
            direction=signal.signal.value.upper(),
            entry_price=entry_price,
            exit_price=exit_price,
            stop_loss=sl,
            take_profit=tp,
            result=result_type,
            pnl=total_pnl,
            quantity=qty,
            entry_bar=bar_idx,
            exit_bar=bar_idx + 1,
            strategy_name=signal.strategy_name,
            reason=signal.reason,
        )

    def _simulate_5min_day(self, day_df: pd.DataFrame, date) -> Optional[BacktestResult]:
        """5분봉 데이터로 일일 시뮬레이션"""
        if hasattr(self.strategy, 'reset_code'):
            self.strategy.reset_code(self.config.ticker)

        entry_signal = None
        entry_bar = 0

        for i in range(len(day_df)):
            partial = day_df.iloc[:i+1].copy()

            signal = self.strategy.evaluate(self.config.ticker, partial)
            if signal is not None and entry_signal is None:
                entry_signal = signal
                entry_bar = i
                break

        if entry_signal is None:
            return None

        entry_price = float(day_df.iloc[entry_bar]['close'])
        sl = entry_signal.stop_loss if entry_signal.stop_loss > 0 else int(entry_price * 0.98)
        tp = entry_signal.take_profit if entry_signal.take_profit > 0 else int(entry_price * 1.04)

        # 진입 후 봉들에서 TP/SL 체크
        for j in range(entry_bar + 1, len(day_df)):
            bar = day_df.iloc[j]
            bar_h = float(bar['high'])
            bar_l = float(bar['low'])

            if entry_signal.is_buy:
                if bar_l <= sl:
                    return self._make_result(date, entry_price, sl, sl, tp,
                                             "SL_HIT", entry_signal, entry_bar, j)
                if bar_h >= tp:
                    return self._make_result(date, entry_price, tp, sl, tp,
                                             "TP_HIT", entry_signal, entry_bar, j)
            else:
                if bar_h >= sl:
                    return self._make_result(date, entry_price, sl, sl, tp,
                                             "SL_HIT", entry_signal, entry_bar, j)
                if bar_l <= tp:
                    return self._make_result(date, entry_price, tp, sl, tp,
                                             "TP_HIT", entry_signal, entry_bar, j)

        # 장마감 강제 청산
        last_price = float(day_df.iloc[-1]['close'])
        return self._make_result(date, entry_price, last_price, sl, tp,
                                 "TIMEOUT", entry_signal, entry_bar, len(day_df) - 1)

    def _make_result(self, date, entry_price, exit_price, sl, tp,
                     result_type, signal, entry_bar, exit_bar) -> BacktestResult:
        """BacktestResult 생성 헬퍼"""
        if signal.is_buy:
            pnl = exit_price - entry_price
        else:
            pnl = entry_price - exit_price

        comm = (entry_price + exit_price) * self.config.commission_rate
        pnl -= comm

        risk = abs(entry_price - sl)
        qty = max(1, int(self.config.risk_per_trade / risk)) if risk > 0 else 1
        total_pnl = pnl * qty

        return BacktestResult(
            date=date,
            ticker=self.config.ticker,
            direction=signal.signal.value.upper(),
            entry_price=entry_price,
            exit_price=exit_price,
            stop_loss=sl,
            take_profit=tp,
            result=result_type,
            pnl=total_pnl,
            quantity=qty,
            entry_bar=entry_bar,
            exit_bar=exit_bar,
            strategy_name=signal.strategy_name,
            reason=signal.reason,
        )

    # === 리포트 ===

    def print_report(self) -> dict:
        """백테스트 결과 리포트 출력"""
        if not self.results:
            print("결과 없음")
            return {}

        df = pd.DataFrame([asdict(r) for r in self.results])
        cfg = self.config

        wins = (df["result"] == "TP_HIT").sum()
        losses = (df["result"] == "SL_HIT").sum()
        timeouts = (df["result"] == "TIMEOUT").sum()
        total = len(df)

        win_rate = wins / total * 100 if total else 0
        total_pnl = df["pnl"].sum()
        avg_pnl = df["pnl"].mean()
        max_dd = self._max_drawdown()
        profit_fac = self._profit_factor(df)
        sharpe = self._sharpe_ratio(df)

        print(f"\n{'='*55}")
        print(f"  백테스트 결과 리포트")
        print(f"{'='*55}")
        print(f"  종목      : {cfg.ticker} {cfg.ticker_name}")
        print(f"  기간      : {df['date'].min()} ~ {df['date'].max()}")
        print(f"  전략      : {df['strategy_name'].iloc[0] if len(df) > 0 else '-'}")
        print(f"  총 거래   : {total}회")
        print(f"  승률      : {win_rate:.1f}%  ({wins}승 {losses}패 {timeouts}타임아웃)")
        print(f"{'─'*55}")
        print(f"  총 PnL    : {total_pnl:+,.0f}원")
        print(f"  평균 PnL  : {avg_pnl:+,.0f}원/거래")
        print(f"  최대 낙폭 : {max_dd:,.0f}원")
        print(f"  Profit Factor: {profit_fac:.2f}")
        print(f"  Sharpe Ratio : {sharpe:.2f}")
        print(f"{'='*55}")

        stats = dict(
            total=total, wins=wins, losses=losses, timeouts=timeouts,
            win_rate=win_rate, total_pnl=total_pnl, avg_pnl=avg_pnl,
            max_drawdown=max_dd, profit_factor=profit_fac, sharpe=sharpe,
        )
        return stats

    def save_results(self, path: str = None):
        """결과를 CSV로 저장"""
        if not self.results:
            return
        if path is None:
            path = f"./results/backtest_{self.config.ticker}.csv"
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        df = pd.DataFrame([asdict(r) for r in self.results])
        df.to_csv(path, index=False, encoding='utf-8-sig')
        logger.info(f"결과 저장: {path}")

    def equity_curve(self) -> pd.Series:
        return pd.Series(self._equity_curve, name="equity")

    def _max_drawdown(self) -> float:
        if not self._equity_curve:
            return 0.0
        equity = np.array(self._equity_curve)
        peak = np.maximum.accumulate(equity)
        dd = equity - peak
        return float(dd.min())

    @staticmethod
    def _profit_factor(df: pd.DataFrame) -> float:
        gross_win = df[df["pnl"] > 0]["pnl"].sum()
        gross_loss = abs(df[df["pnl"] < 0]["pnl"].sum())
        return gross_win / gross_loss if gross_loss > 0 else float("inf")

    @staticmethod
    def _sharpe_ratio(df: pd.DataFrame, rfr: float = 0.0) -> float:
        daily = df.groupby("date")["pnl"].sum()
        if daily.std() == 0:
            return 0.0
        return (daily.mean() - rfr) / daily.std() * np.sqrt(252)
