"""
Daily Report - 일일 매매 리포트
=================================
장마감 후 당일 매매 요약 생성
"""

import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List

import pandas as pd

logger = logging.getLogger('Scalper.Report')


class DailyReport:
    """일일 매매 리포트 생성"""

    def __init__(self, config: dict):
        self.save_dir = Path(config['output']['save_dir'])
        self.save_dir.mkdir(parents=True, exist_ok=True)

    def generate(self, portfolio_summary: Dict, filled_orders: List[Dict],
                 daily_guard_summary: Dict) -> Dict:
        """일일 리포트 생성"""
        today = datetime.now().strftime('%Y-%m-%d')

        # 매매 통계
        trades = pd.DataFrame(filled_orders) if filled_orders else pd.DataFrame()
        buy_count = len(trades[trades.get('type', trades.get('side', '')) == 'buy']) if not trades.empty else 0
        sell_count = len(trades) - buy_count if not trades.empty else 0

        report = {
            'date': today,
            'portfolio': portfolio_summary,
            'daily_guard': daily_guard_summary,
            'trade_stats': {
                'total_trades': len(filled_orders),
                'buy_count': buy_count,
                'sell_count': sell_count,
            },
            'generated_at': datetime.now().isoformat(),
        }

        # 파일 저장
        self._save_report(report)

        return report

    def _save_report(self, report: Dict):
        today = datetime.now().strftime('%Y%m%d')
        filepath = self.save_dir / f"daily_report_{today}.txt"

        lines = [
            f"{'='*50}",
            f"  Scalper Agent - 일일 리포트",
            f"  {report['date']}",
            f"{'='*50}",
            "",
            f"  [포트폴리오]",
            f"  총 평가: {report['portfolio'].get('total_eval', 0):>15,}원",
            f"  현금:    {report['portfolio'].get('cash', 0):>15,}원 ({report['portfolio'].get('cash_ratio', 0):.1%})",
            f"  주식:    {report['portfolio'].get('stock_value', 0):>15,}원",
            "",
            f"  [손익]",
            f"  실현손익:   {report['daily_guard'].get('realized_pnl', 0):>+15,}원",
            f"  미실현손익: {report['daily_guard'].get('unrealized_pnl', 0):>+15,}원",
            f"  총 손익:    {report['daily_guard'].get('total_pnl', 0):>+15,}원",
            "",
            f"  [매매]",
            f"  총 매매: {report['trade_stats']['total_trades']}회",
            f"  매수: {report['trade_stats']['buy_count']}회 / 매도: {report['trade_stats']['sell_count']}회",
            "",
            f"  한도 초과: {'예' if report['daily_guard'].get('is_locked') else '아니오'}",
            f"{'='*50}",
        ]

        filepath.write_text('\n'.join(lines), encoding='utf-8')
        logger.info(f"일일 리포트 저장: {filepath}")
