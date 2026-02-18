# -*- coding: utf-8 -*-
"""
KIS API 실매매 래퍼 — mojito2 기반
===================================
한국투자증권 REST API를 통한 주문/잔고/현재가 조회

사용법:
    trader = KISTrader(config)
    trader.fetch_balance()
    trader.buy_market("005930", 1)
"""

import os
import time
import logging
from typing import Dict, List, Optional

logger = logging.getLogger("BH.KISTrader")

# 종목명 매핑
from data.kis_collector import UNIVERSE
NAME_TO_CODE = {info[0]: code for code, info in UNIVERSE.items()}
CODE_TO_NAME = {code: info[0] for code, info in UNIVERSE.items()}


def resolve_stock(query: str):
    """종목명 or 코드 → (code, name). 부분매칭 지원"""
    query = query.strip()
    # 코드 매칭
    if query in UNIVERSE:
        return query, UNIVERSE[query][0]
    # 정확한 이름 매칭
    if query in NAME_TO_CODE:
        return NAME_TO_CODE[query], query
    # 부분 이름 매칭
    for name, code in NAME_TO_CODE.items():
        if query in name:
            return code, name
    return None, None


class KISTrader:
    """KIS API 실매매 래퍼"""

    def __init__(self, config: dict = None):
        self.config = config or {}
        self.risk = self.config.get("risk", {})
        self._broker = None

    def _get_broker(self):
        """싱글턴 브로커 (kis_collector.py 패턴 재사용)"""
        if self._broker is not None:
            return self._broker

        from dotenv import load_dotenv
        load_dotenv()
        import mojito

        self._broker = mojito.KoreaInvestment(
            api_key=os.getenv("KIS_APP_KEY"),
            api_secret=os.getenv("KIS_APP_SECRET"),
            acc_no=os.getenv("KIS_ACC_NO"),
            mock=False,
        )
        logger.info("KIS API 브로커 생성 완료")
        return self._broker

    # ═══════════════════════════════════════
    #  조회
    # ═══════════════════════════════════════

    def fetch_balance(self) -> dict:
        """계좌 잔고 조회

        Returns: {success, cash, total_eval, positions: [{code, name, qty, avg_price, current_price, pnl_amount, pnl_rate}]}
        """
        try:
            broker = self._get_broker()
            resp = broker.fetch_balance()

            if resp is None:
                return {"success": False, "message": "잔고 조회 실패 (응답 없음)"}

            output = resp.get("output1", [])
            summary = resp.get("output2", [{}])

            positions = []
            for item in output:
                qty = int(item.get("hldg_qty", 0))
                if qty <= 0:
                    continue
                code = item.get("pdno", "")
                positions.append({
                    "code": code,
                    "name": item.get("prdt_name", CODE_TO_NAME.get(code, code)),
                    "qty": qty,
                    "avg_price": int(float(item.get("pchs_avg_pric", 0))),
                    "current_price": int(item.get("prpr", 0)),
                    "pnl_amount": int(item.get("evlu_pfls_amt", 0)),
                    "pnl_rate": float(item.get("evlu_pfls_rt", 0)),
                })

            s = summary[0] if summary else {}
            cash = int(s.get("dnca_tot_amt", 0))
            total_eval = int(s.get("tot_evlu_amt", 0))

            return {
                "success": True,
                "cash": cash,
                "total_eval": total_eval,
                "positions": positions,
            }

        except Exception as e:
            logger.error(f"잔고 조회 실패: {e}")
            return {"success": False, "message": f"잔고 조회 실패: {e}"}

    def fetch_price(self, code: str) -> dict:
        """현재가 조회

        Returns: {success, current_price, change_rate, volume, ...}
        """
        try:
            broker = self._get_broker()
            resp = broker.fetch_price(code)

            if resp is None:
                return {"success": False, "message": "현재가 조회 실패"}

            output = resp.get("output", {})
            return {
                "success": True,
                "current_price": int(output.get("stck_prpr", 0)),
                "change_rate": float(output.get("prdy_ctrt", 0)),
                "volume": int(output.get("acml_vol", 0)),
                "high": int(output.get("stck_hgpr", 0)),
                "low": int(output.get("stck_lwpr", 0)),
            }

        except Exception as e:
            logger.error(f"현재가 조회 실패 {code}: {e}")
            return {"success": False, "message": f"현재가 조회 실패: {e}"}

    def fetch_open_orders(self) -> dict:
        """미체결 주문 조회"""
        try:
            broker = self._get_broker()
            param = {
                "CTX_AREA_FK100": "",
                "CTX_AREA_NK100": "",
                "INQR_DVSN_1": "0",
                "INQR_DVSN_2": "0",
            }
            resp = broker.fetch_open_order(param)

            if resp is None:
                return {"success": True, "orders": []}

            orders = []
            for item in resp.get("output", []):
                orders.append({
                    "order_no": item.get("odno", ""),
                    "code": item.get("pdno", ""),
                    "name": item.get("prdt_name", ""),
                    "side": "매수" if item.get("sll_buy_dvsn_cd") == "02" else "매도",
                    "qty": int(item.get("ord_qty", 0)),
                    "price": int(item.get("ord_unpr", 0)),
                    "filled_qty": int(item.get("tot_ccld_qty", 0)),
                })

            return {"success": True, "orders": orders}

        except Exception as e:
            logger.error(f"미체결 조회 실패: {e}")
            return {"success": False, "message": f"미체결 조회 실패: {e}"}

    # ═══════════════════════════════════════
    #  주문
    # ═══════════════════════════════════════

    def buy_market(self, code: str, qty: int) -> dict:
        """시장가 매수"""
        try:
            broker = self._get_broker()
            resp = broker.create_market_buy_order(symbol=code, quantity=qty)
            logger.info(f"시장가 매수 주문: {code} {qty}주 → {resp}")

            if resp and resp.get("rt_cd") == "0":
                order_no = resp.get("output", {}).get("ODNO", "?")
                name = CODE_TO_NAME.get(code, code)
                return {
                    "success": True,
                    "order_no": order_no,
                    "message": f"매수 주문 완료\n{name}({code}) {qty}주\n주문번호: {order_no}",
                }
            else:
                msg = resp.get("msg1", "알 수 없는 오류") if resp else "응답 없음"
                return {"success": False, "message": f"매수 주문 실패: {msg}"}

        except Exception as e:
            logger.error(f"매수 실패 {code}: {e}")
            return {"success": False, "message": f"매수 주문 실패: {e}"}

    def sell_market(self, code: str, qty: int) -> dict:
        """시장가 매도"""
        try:
            broker = self._get_broker()
            resp = broker.create_market_sell_order(symbol=code, quantity=qty)
            logger.info(f"시장가 매도 주문: {code} {qty}주 → {resp}")

            if resp and resp.get("rt_cd") == "0":
                order_no = resp.get("output", {}).get("ODNO", "?")
                name = CODE_TO_NAME.get(code, code)
                return {
                    "success": True,
                    "order_no": order_no,
                    "message": f"매도 주문 완료\n{name}({code}) {qty}주\n주문번호: {order_no}",
                }
            else:
                msg = resp.get("msg1", "알 수 없는 오류") if resp else "응답 없음"
                return {"success": False, "message": f"매도 주문 실패: {msg}"}

        except Exception as e:
            logger.error(f"매도 실패 {code}: {e}")
            return {"success": False, "message": f"매도 주문 실패: {e}"}

    def cancel_order(self, order_no: str) -> dict:
        """주문 취소"""
        try:
            broker = self._get_broker()
            resp = broker.cancel_order(order_no)
            logger.info(f"주문 취소: {order_no} → {resp}")

            if resp and resp.get("rt_cd") == "0":
                return {"success": True, "message": f"주문 취소 완료: {order_no}"}
            else:
                msg = resp.get("msg1", "알 수 없는 오류") if resp else "응답 없음"
                return {"success": False, "message": f"주문 취소 실패: {msg}"}

        except Exception as e:
            logger.error(f"취소 실패 {order_no}: {e}")
            return {"success": False, "message": f"주문 취소 실패: {e}"}

    # ═══════════════════════════════════════
    #  안전 매수 (리스크 체크)
    # ═══════════════════════════════════════

    def safe_buy(self, code: str, amount: int) -> dict:
        """금액 기반 안전 매수

        1. 현금 잔고 확인
        2. 포지션 수 확인 (max_positions)
        3. 비중 확인 (max_position_ratio)
        4. 현재가로 수량 계산
        5. 시장가 매수
        """
        # 잔고 조회
        bal = self.fetch_balance()
        if not bal.get("success"):
            return {"success": False, "message": f"잔고 조회 실패: {bal.get('message')}"}

        cash = bal["cash"]
        total_eval = bal["total_eval"] or cash
        positions = bal["positions"]

        # 리스크 체크
        max_positions = self.risk.get("max_positions", 5)
        max_ratio = self.risk.get("max_position_ratio", 0.30)
        min_cash = self.risk.get("min_cash_ratio", 0.10)

        if len(positions) >= max_positions:
            return {"success": False, "message": f"최대 보유 종목({max_positions}개) 초과"}

        # 현금 잔고 여유 확인
        min_cash_amount = int(total_eval * min_cash)
        available = cash - min_cash_amount
        if available < amount:
            return {
                "success": False,
                "message": f"현금 부족\n가용: {available:,}원 (최소 현금 {min_cash_amount:,}원 유지)\n요청: {amount:,}원",
            }

        # 비중 확인
        max_amount = int(total_eval * max_ratio)
        buy_amount = min(amount, max_amount)

        # 현재가 조회
        price_info = self.fetch_price(code)
        if not price_info.get("success"):
            return {"success": False, "message": f"현재가 조회 실패: {price_info.get('message')}"}

        current_price = price_info["current_price"]
        if current_price <= 0:
            return {"success": False, "message": "현재가가 0원입니다"}

        qty = buy_amount // current_price
        if qty <= 0:
            return {"success": False, "message": f"매수 가능 수량 없음 (현재가: {current_price:,}원, 금액: {buy_amount:,}원)"}

        name = CODE_TO_NAME.get(code, code)
        est_cost = qty * current_price

        logger.info(f"안전 매수: {name}({code}) {qty}주 @ {current_price:,}원 ≈ {est_cost:,}원")

        # 실제 매수
        return self.buy_market(code, qty)

    def liquidate_one(self, code: str) -> dict:
        """특정 종목 전량 청산"""
        bal = self.fetch_balance()
        if not bal.get("success"):
            return {"success": False, "message": "잔고 조회 실패"}

        for pos in bal["positions"]:
            if pos["code"] == code:
                return self.sell_market(code, pos["qty"])

        name = CODE_TO_NAME.get(code, code)
        return {"success": False, "message": f"{name}({code}) 보유 없음"}

    def liquidate_all(self) -> dict:
        """전종목 시장가 청산"""
        bal = self.fetch_balance()
        if not bal.get("success"):
            return {"success": False, "message": "잔고 조회 실패"}

        if not bal["positions"]:
            return {"success": True, "message": "보유 종목 없음 (청산 불필요)"}

        results = []
        for pos in bal["positions"]:
            r = self.sell_market(pos["code"], pos["qty"])
            results.append(f"{pos['name']}: {'성공' if r.get('success') else '실패'}")
            time.sleep(0.2)

        return {
            "success": True,
            "message": f"전량 청산 완료\n" + "\n".join(results),
        }
