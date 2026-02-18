"""
Kiwoom Data - TR 기반 데이터 요청
==================================
주식 기본정보, 분봉, 계좌잔고 등 조회
"""

import logging
from typing import Dict, Optional
from datetime import datetime

import pandas as pd

from api.kiwoom_core import KiwoomCore
from api.kiwoom_constants import (
    TR_OPT10001, TR_OPT10080, TR_OPT10081, TR_OPW00018, TR_OPW00001
)

logger = logging.getLogger('Scalper.Data')


class KiwoomData:
    """TR 기반 고수준 데이터 요청"""

    def __init__(self, core: KiwoomCore):
        self.core = core

    def get_current_price(self, code: str) -> Dict:
        """주식 기본정보 (현재가, 시가, 고가, 저가, 거래량)"""
        self.core.set_input_value("종목코드", code)
        result = self.core.request_tr("주식기본정보", TR_OPT10001)

        if not result:
            return {}

        tr_code = result['tr_code']
        data = {
            'code': code,
            'name': self.core.get_code_name(code),
            'current_price': abs(int(self.core.get_comm_data(tr_code, "", 0, "현재가") or 0)),
            'diff': int(self.core.get_comm_data(tr_code, "", 0, "전일대비") or 0),
            'diff_rate': float(self.core.get_comm_data(tr_code, "", 0, "등락율") or 0),
            'volume': int(self.core.get_comm_data(tr_code, "", 0, "거래량") or 0),
            'open': abs(int(self.core.get_comm_data(tr_code, "", 0, "시가") or 0)),
            'high': abs(int(self.core.get_comm_data(tr_code, "", 0, "고가") or 0)),
            'low': abs(int(self.core.get_comm_data(tr_code, "", 0, "저가") or 0)),
        }
        return data

    def get_minute_candles(self, code: str, period: int = 1, count: int = 200) -> pd.DataFrame:
        """
        분봉 차트 데이터 조회

        Args:
            code: 종목코드
            period: 분봉 단위 (1, 3, 5, 10, 15, 30, 60)
            count: 요청 봉 수

        Returns:
            DataFrame[open, high, low, close, volume, timestamp]
        """
        self.core.set_input_value("종목코드", code)
        self.core.set_input_value("틱범위", str(period))
        self.core.set_input_value("수정주가구분", "1")
        result = self.core.request_tr("분봉조회", TR_OPT10080)

        if not result:
            return pd.DataFrame()

        tr_code = result['tr_code']
        repeat = self.core.get_repeat_cnt(tr_code, "")
        repeat = min(repeat, count)

        rows = []
        for i in range(repeat):
            row = {
                'timestamp': self.core.get_comm_data(tr_code, "", i, "체결시간"),
                'open': abs(int(self.core.get_comm_data(tr_code, "", i, "시가") or 0)),
                'high': abs(int(self.core.get_comm_data(tr_code, "", i, "고가") or 0)),
                'low': abs(int(self.core.get_comm_data(tr_code, "", i, "저가") or 0)),
                'close': abs(int(self.core.get_comm_data(tr_code, "", i, "현재가") or 0)),
                'volume': int(self.core.get_comm_data(tr_code, "", i, "거래량") or 0),
            }
            rows.append(row)

        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame(rows)
        df['timestamp'] = pd.to_datetime(df['timestamp'], format='%Y%m%d%H%M%S', errors='coerce')
        df = df.sort_values('timestamp').reset_index(drop=True)
        return df

    def get_daily_candles(self, code: str, count: int = 100) -> pd.DataFrame:
        """일봉 차트 데이터"""
        today = datetime.now().strftime('%Y%m%d')
        self.core.set_input_value("종목코드", code)
        self.core.set_input_value("기준일자", today)
        self.core.set_input_value("수정주가구분", "1")
        result = self.core.request_tr("일봉조회", TR_OPT10081)

        if not result:
            return pd.DataFrame()

        tr_code = result['tr_code']
        repeat = min(self.core.get_repeat_cnt(tr_code, ""), count)

        rows = []
        for i in range(repeat):
            row = {
                'date': self.core.get_comm_data(tr_code, "", i, "일자"),
                'open': abs(int(self.core.get_comm_data(tr_code, "", i, "시가") or 0)),
                'high': abs(int(self.core.get_comm_data(tr_code, "", i, "고가") or 0)),
                'low': abs(int(self.core.get_comm_data(tr_code, "", i, "저가") or 0)),
                'close': abs(int(self.core.get_comm_data(tr_code, "", i, "현재가") or 0)),
                'volume': int(self.core.get_comm_data(tr_code, "", i, "거래량") or 0),
            }
            rows.append(row)

        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame(rows)
        df['date'] = pd.to_datetime(df['date'], format='%Y%m%d', errors='coerce')
        df = df.sort_values('date').reset_index(drop=True)
        return df

    def get_account_balance(self, account: str) -> Dict:
        """계좌 평가잔고 내역"""
        self.core.set_input_value("계좌번호", account)
        self.core.set_input_value("비밀번호", "")
        self.core.set_input_value("비밀번호입력매체구분", "00")
        self.core.set_input_value("조회구분", "1")
        result = self.core.request_tr("계좌잔고", TR_OPW00018)

        if not result:
            return {}

        tr_code = result['tr_code']

        summary = {
            'total_purchase': int(self.core.get_comm_data(tr_code, "", 0, "총매입금액") or 0),
            'total_eval': int(self.core.get_comm_data(tr_code, "", 0, "총평가금액") or 0),
            'total_pnl': int(self.core.get_comm_data(tr_code, "", 0, "총평가손익금액") or 0),
            'total_pnl_rate': float(self.core.get_comm_data(tr_code, "", 0, "총수익률(%)") or 0),
        }

        # 보유종목 목록
        repeat = self.core.get_repeat_cnt(tr_code, "")
        positions = []
        for i in range(repeat):
            pos = {
                'code': self.core.get_comm_data(tr_code, "", i, "종목번호").replace("A", "").strip(),
                'name': self.core.get_comm_data(tr_code, "", i, "종목명"),
                'quantity': int(self.core.get_comm_data(tr_code, "", i, "보유수량") or 0),
                'avg_price': int(self.core.get_comm_data(tr_code, "", i, "매입가") or 0),
                'current_price': int(self.core.get_comm_data(tr_code, "", i, "현재가") or 0),
                'eval_amount': int(self.core.get_comm_data(tr_code, "", i, "평가금액") or 0),
                'pnl': int(self.core.get_comm_data(tr_code, "", i, "평가손익") or 0),
                'pnl_rate': float(self.core.get_comm_data(tr_code, "", i, "수익률(%)") or 0),
            }
            positions.append(pos)

        summary['positions'] = positions
        return summary

    def get_deposit(self, account: str) -> int:
        """예수금 조회"""
        self.core.set_input_value("계좌번호", account)
        self.core.set_input_value("비밀번호", "")
        self.core.set_input_value("비밀번호입력매체구분", "00")
        self.core.set_input_value("조회구분", "2")
        result = self.core.request_tr("예수금조회", TR_OPW00001)

        if not result:
            return 0

        tr_code = result['tr_code']
        deposit = int(self.core.get_comm_data(tr_code, "", 0, "주문가능금액") or 0)
        return deposit
