"""
Kiwoom Realtime - 실시간 데이터 구독 관리
==========================================
체결 데이터, 호가 데이터를 실시간으로 수신하여 콜백 호출
"""

import logging
from typing import Callable, Dict, Optional
from datetime import datetime

from api.kiwoom_core import KiwoomCore
from api.kiwoom_constants import (
    FID_CURRENT_PRICE, FID_DIFF, FID_DIFF_RATE, FID_VOLUME,
    FID_CUMUL_VOLUME, FID_TRADE_TIME, FID_OPEN, FID_HIGH, FID_LOW,
    FID_ASK_PRICE_1, FID_BID_PRICE_1, FID_ASK_VOL_1, FID_BID_VOL_1,
    FID_TOTAL_ASK_VOL, FID_TOTAL_BID_VOL,
    REAL_TYPE_TICK, REAL_TYPE_ORDERBOOK,
    TICK_FIDS, ORDERBOOK_FIDS,
)

logger = logging.getLogger('Scalper.Realtime')


class KiwoomRealtime:
    """실시간 데이터 구독 및 디스패치"""

    def __init__(self, core: KiwoomCore):
        self.core = core
        self._tick_callbacks: list[Callable] = []
        self._orderbook_callbacks: list[Callable] = []
        self._subscribed_codes: set = set()
        self._screen_no = core.get_next_screen()

        # 실시간 이벤트 연결
        core.sig_real_data.connect(self._on_real_data)

    def subscribe(self, codes: list, on_tick: Optional[Callable] = None,
                  on_orderbook: Optional[Callable] = None):
        """
        실시간 데이터 구독

        Args:
            codes: 종목코드 리스트
            on_tick: 체결 콜백 fn(code, tick_data)
            on_orderbook: 호가 콜백 fn(code, orderbook_data)
        """
        if on_tick:
            self._tick_callbacks.append(on_tick)
        if on_orderbook:
            self._orderbook_callbacks.append(on_orderbook)

        codes_str = ";".join(codes)
        all_fids = TICK_FIDS + ";" + ORDERBOOK_FIDS

        opt = "0" if not self._subscribed_codes else "1"
        ret = self.core.set_real_reg(self._screen_no, codes_str, all_fids, opt)

        self._subscribed_codes.update(codes)
        logger.info(f"실시간 등록: {codes} (결과: {ret})")

    def unsubscribe(self, codes: list):
        for code in codes:
            self.core.set_real_remove(self._screen_no, code)
            self._subscribed_codes.discard(code)
        logger.info(f"실시간 해제: {codes}")

    def unsubscribe_all(self):
        self.core.set_real_remove("ALL", "ALL")
        self._subscribed_codes.clear()
        logger.info("실시간 전체 해제")

    def _on_real_data(self, code: str, real_type: str, data: str):
        """실시간 데이터 수신 → 파싱 → 콜백 호출"""
        if real_type == REAL_TYPE_TICK:
            tick = self._parse_tick(code)
            for cb in self._tick_callbacks:
                try:
                    cb(code, tick)
                except Exception as e:
                    logger.error(f"틱 콜백 에러 ({code}): {e}")

        elif real_type == REAL_TYPE_ORDERBOOK:
            ob = self._parse_orderbook(code)
            for cb in self._orderbook_callbacks:
                try:
                    cb(code, ob)
                except Exception as e:
                    logger.error(f"호가 콜백 에러 ({code}): {e}")

    def _parse_tick(self, code: str) -> Dict:
        """체결 데이터 파싱"""
        get = self.core.get_comm_real_data
        return {
            'code': code,
            'price': abs(int(get(code, FID_CURRENT_PRICE) or 0)),
            'diff': int(get(code, FID_DIFF) or 0),
            'diff_rate': float(get(code, FID_DIFF_RATE) or 0),
            'volume': int(get(code, FID_VOLUME) or 0),
            'cumul_volume': int(get(code, FID_CUMUL_VOLUME) or 0),
            'open': abs(int(get(code, FID_OPEN) or 0)),
            'high': abs(int(get(code, FID_HIGH) or 0)),
            'low': abs(int(get(code, FID_LOW) or 0)),
            'time': get(code, FID_TRADE_TIME),
            'timestamp': datetime.now(),
        }

    def _parse_orderbook(self, code: str) -> Dict:
        """호가 데이터 파싱"""
        get = self.core.get_comm_real_data
        return {
            'code': code,
            'ask_price_1': abs(int(get(code, FID_ASK_PRICE_1) or 0)),
            'bid_price_1': abs(int(get(code, FID_BID_PRICE_1) or 0)),
            'ask_vol_1': int(get(code, FID_ASK_VOL_1) or 0),
            'bid_vol_1': int(get(code, FID_BID_VOL_1) or 0),
            'total_ask_vol': int(get(code, FID_TOTAL_ASK_VOL) or 0),
            'total_bid_vol': int(get(code, FID_TOTAL_BID_VOL) or 0),
            'timestamp': datetime.now(),
        }
