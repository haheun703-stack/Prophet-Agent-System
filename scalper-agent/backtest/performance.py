"""
Performance Analyzer - 성과 분석
=================================
백테스트 결과 통계 분석 + 복수 종목 비교
"""

import logging
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

logger = logging.getLogger('Scalper.Performance')


class PerformanceAnalyzer:
    """백테스트 성과 분석기"""

    @staticmethod
    def analyze(results_df: pd.DataFrame) -> dict:
        """
        BacktestResult DataFrame → 성과 지표 산출

        Args:
            results_df: BacktestResult의 asdict() 변환 DataFrame
        """
        if results_df.empty:
            return {}

        total = len(results_df)
        wins = (results_df['result'] == 'TP_HIT').sum()
        losses = (results_df['result'] == 'SL_HIT').sum()
        timeouts = (results_df['result'] == 'TIMEOUT').sum()

        total_pnl = results_df['pnl'].sum()
        avg_pnl = results_df['pnl'].mean()

        # 승률
        win_rate = wins / total * 100 if total else 0

        # Profit Factor
        gross_win = results_df[results_df['pnl'] > 0]['pnl'].sum()
        gross_loss = abs(results_df[results_df['pnl'] < 0]['pnl'].sum())
        profit_factor = gross_win / gross_loss if gross_loss > 0 else float('inf')

        # 최대 낙폭
        equity = results_df['pnl'].cumsum().values
        equity = np.insert(equity, 0, 0)
        peak = np.maximum.accumulate(equity)
        drawdown = equity - peak
        max_drawdown = float(drawdown.min())

        # Sharpe Ratio
        daily_pnl = results_df.groupby('date')['pnl'].sum()
        sharpe = 0.0
        if daily_pnl.std() > 0:
            sharpe = (daily_pnl.mean() / daily_pnl.std()) * np.sqrt(252)

        # 연속 손실
        streaks = PerformanceAnalyzer._calc_streaks(results_df['pnl'].values)

        # 평균 보유 기간
        if 'entry_bar' in results_df.columns and 'exit_bar' in results_df.columns:
            avg_hold = (results_df['exit_bar'] - results_df['entry_bar']).mean()
        else:
            avg_hold = 0

        return {
            'total_trades': total,
            'wins': wins,
            'losses': losses,
            'timeouts': timeouts,
            'win_rate': win_rate,
            'total_pnl': total_pnl,
            'avg_pnl': avg_pnl,
            'profit_factor': profit_factor,
            'max_drawdown': max_drawdown,
            'sharpe_ratio': sharpe,
            'max_win_streak': streaks['max_win_streak'],
            'max_loss_streak': streaks['max_loss_streak'],
            'avg_hold_bars': avg_hold,
            'avg_win': results_df[results_df['pnl'] > 0]['pnl'].mean() if wins > 0 else 0,
            'avg_loss': results_df[results_df['pnl'] < 0]['pnl'].mean() if losses > 0 else 0,
        }

    @staticmethod
    def compare_strategies(results_dict: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """
        복수 전략 비교

        Args:
            results_dict: {"전략명": results_df, ...}

        Returns:
            비교 DataFrame
        """
        rows = []
        for name, df in results_dict.items():
            stats = PerformanceAnalyzer.analyze(df)
            stats['strategy'] = name
            rows.append(stats)
        return pd.DataFrame(rows).set_index('strategy')

    @staticmethod
    def compare_tickers(results_dict: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """
        복수 종목 비교

        Args:
            results_dict: {"종목코드": results_df, ...}
        """
        rows = []
        for ticker, df in results_dict.items():
            stats = PerformanceAnalyzer.analyze(df)
            stats['ticker'] = ticker
            rows.append(stats)
        return pd.DataFrame(rows).set_index('ticker')

    @staticmethod
    def _calc_streaks(pnl_array) -> dict:
        """연속 승/패 계산"""
        max_win = 0
        max_loss = 0
        cur_win = 0
        cur_loss = 0

        for p in pnl_array:
            if p > 0:
                cur_win += 1
                cur_loss = 0
                max_win = max(max_win, cur_win)
            elif p < 0:
                cur_loss += 1
                cur_win = 0
                max_loss = max(max_loss, cur_loss)
            else:
                cur_win = 0
                cur_loss = 0

        return {'max_win_streak': max_win, 'max_loss_streak': max_loss}

    @staticmethod
    def print_comparison(comparison_df: pd.DataFrame):
        """비교 표 출력"""
        cols = ['total_trades', 'win_rate', 'total_pnl', 'profit_factor',
                'max_drawdown', 'sharpe_ratio']
        available = [c for c in cols if c in comparison_df.columns]
        print(comparison_df[available].to_string(float_format='{:,.1f}'.format))
