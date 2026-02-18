"""
Kiwoom Order - 주문 실행
=========================
매수/매도/취소/정정 주문 인터페이스
"""

import logging
from typing import Dict, Optional
from datetime import datetime

from api.kiwoom_core import KiwoomCore
from api.kiwoom_constants import (
    ORDER_BUY_NEW, ORDER_SELL_NEW, ORDER_BUY_CANCEL, ORDER_SELL_CANCEL,
    ORDER_BUY_MODIFY, ORDER_SELL_MODIFY,
    HOGA_MARKET, HOGA_LIMIT, HOGA_BEST,
    FID_ORDER_NO, FID_ORDER_STATUS, FID_ORDER_QTY, FID_ORDER_PRICE,
    FID_FILLED_QTY, FID_FILLED_PRICE, FID_STOCK_CODE,
    FID_ORDER_TYPE, FID_REMAIN_QTY,
    ERR_NONE,
)

logger = logging.getLogger('Scalper.Order')


class KiwoomOrder:
    """키움 주문 실행"""

    def __init__(self, core: KiwoomCore, account: str):
        self.core = core
        self.account = account
        self._screen_no = core.get_next_screen()
        self._chejan_callbacks: list = []

        core.sig_chejan.connect(self._on_chejan)

    def on_chejan(self, callback):
        """체잔(체결/잔고) 콜백 등록"""
        self._chejan_callbacks.append(callback)

    # === 매수 ===

    def buy_market(self, code: str, qty: int) -> int:
        """시장가 매수"""
        logger.info(f"시장가 매수: {code} x {qty}")
        return self.core.send_order(
            "매수주문", self._screen_no, self.account,
            ORDER_BUY_NEW, code, qty, 0, HOGA_MARKET, ""
        )

    def buy_limit(self, code: str, qty: int, price: int) -> int:
        """지정가 매수"""
        logger.info(f"지정가 매수: {code} x {qty} @ {price}")
        return self.core.send_order(
            "매수주문", self._screen_no, self.account,
            ORDER_BUY_NEW, code, qty, price, HOGA_LIMIT, ""
        )

    def buy_best(self, code: str, qty: int) -> int:
        """최유리지정가 매수"""
        logger.info(f"최유리 매수: {code} x {qty}")
        return self.core.send_order(
            "매수주문", self._screen_no, self.account,
            ORDER_BUY_NEW, code, qty, 0, HOGA_BEST, ""
        )

    # === 매도 ===

    def sell_market(self, code: str, qty: int) -> int:
        """시장가 매도"""
        logger.info(f"시장가 매도: {code} x {qty}")
        return self.core.send_order(
            "매도주문", self._screen_no, self.account,
            ORDER_SELL_NEW, code, qty, 0, HOGA_MARKET, ""
        )

    def sell_limit(self, code: str, qty: int, price: int) -> int:
        """지정가 매도"""
        logger.info(f"지정가 매도: {code} x {qty} @ {price}")
        return self.core.send_order(
            "매도주문", self._screen_no, self.account,
            ORDER_SELL_NEW, code, qty, price, HOGA_LIMIT, ""
        )

    # === 취소/정정 ===

    def cancel_buy(self, org_order_no: str, code: str, qty: int) -> int:
        """매수 취소"""
        logger.info(f"매수 취소: {code}, 원주문={org_order_no}")
        return self.core.send_order(
            "매수취소", self._screen_no, self.account,
            ORDER_BUY_CANCEL, code, qty, 0, HOGA_LIMIT, org_order_no
        )

    def cancel_sell(self, org_order_no: str, code: str, qty: int) -> int:
        """매도 취소"""
        logger.info(f"매도 취소: {code}, 원주문={org_order_no}")
        return self.core.send_order(
            "매도취소", self._screen_no, self.account,
            ORDER_SELL_CANCEL, code, qty, 0, HOGA_LIMIT, org_order_no
        )

    # === 체잔 이벤트 ===

    def _on_chejan(self, gubun: str, item_cnt: int, fid_list: str):
        """체결/잔고 이벤트 수신"""
        get = self.core.dynamicCall

        if gubun == "0":  # 주문접수/체결
            data = {
                'type': 'order',
                'order_no': get("GetChejanData(int)", FID_ORDER_NO).strip(),
                'code': get("GetChejanData(int)", FID_STOCK_CODE).strip().replace("A", ""),
                'order_status': get("GetChejanData(int)", FID_ORDER_STATUS).strip(),
                'order_qty': int(get("GetChejanData(int)", FID_ORDER_QTY).strip() or 0),
                'order_price': int(get("GetChejanData(int)", FID_ORDER_PRICE).strip() or 0),
                'filled_qty': int(get("GetChejanData(int)", FID_FILLED_QTY).strip() or 0),
                'filled_price': int(get("GetChejanData(int)", FID_FILLED_PRICE).strip() or 0),
                'remain_qty': int(get("GetChejanData(int)", FID_REMAIN_QTY).strip() or 0),
                'order_type': get("GetChejanData(int)", FID_ORDER_TYPE).strip(),
                'timestamp': datetime.now(),
            }
        elif gubun == "1":  # 잔고변경
            data = {
                'type': 'balance',
                'code': get("GetChejanData(int)", FID_STOCK_CODE).strip().replace("A", ""),
                'timestamp': datetime.now(),
            }
        else:
            return

        for cb in self._chejan_callbacks:
            try:
                cb(data)
            except Exception as e:
                logger.error(f"체잔 콜백 에러: {e}")
