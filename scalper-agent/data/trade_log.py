"""
Trade Log - 매매 기록
======================
모든 매매/신호를 CSV로 저장
"""

import logging
from datetime import datetime
from pathlib import Path
from typing import Dict

import pandas as pd

from strategies.base_strategy import TradeSignal

logger = logging.getLogger('Scalper.TradeLog')


class TradeLog:
    """매매 기록 관리"""

    def __init__(self, config: dict):
        self.log_dir = Path(config['output']['trade_log_dir'])
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self._trades: list = []
        self._signals: list = []

    def log_trade(self, trade: Dict):
        """체결 기록"""
        trade['logged_at'] = datetime.now().isoformat()
        self._trades.append(trade)
        self._save_trades()

    def log_signal(self, signal: TradeSignal, approved: bool, reason: str):
        """신호 기록 (승인/거부 포함)"""
        record = {
            'timestamp': signal.timestamp.isoformat(),
            'code': signal.code,
            'signal': signal.signal.value,
            'strategy': signal.strategy_name,
            'confidence': signal.confidence,
            'reason': signal.reason,
            'approved': approved,
            'approve_reason': reason,
        }
        self._signals.append(record)

    def get_trades_today(self) -> pd.DataFrame:
        if not self._trades:
            return pd.DataFrame()
        return pd.DataFrame(self._trades)

    def get_signals_today(self) -> pd.DataFrame:
        if not self._signals:
            return pd.DataFrame()
        return pd.DataFrame(self._signals)

    def _save_trades(self):
        """CSV 저장"""
        if not self._trades:
            return
        today = datetime.now().strftime('%Y%m%d')
        filepath = self.log_dir / f"trades_{today}.csv"
        df = pd.DataFrame(self._trades)
        df.to_csv(filepath, index=False, encoding='utf-8-sig')

    def save_daily_summary(self):
        """일일 요약 저장"""
        today = datetime.now().strftime('%Y%m%d')

        if self._signals:
            sig_path = self.log_dir / f"signals_{today}.csv"
            pd.DataFrame(self._signals).to_csv(sig_path, index=False, encoding='utf-8-sig')

        self._save_trades()

    def reset_daily(self):
        self.save_daily_summary()
        self._trades.clear()
        self._signals.clear()
