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
import json
import time
import logging
from pathlib import Path
from datetime import datetime, date
from typing import Dict, List, Optional

logger = logging.getLogger("BH.KISTrader")

# 매매일지 저장 경로
JOURNAL_DIR = Path(__file__).resolve().parent.parent / "logs"
JOURNAL_DIR.mkdir(parents=True, exist_ok=True)

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
    #  위험시간 / 거래량 체크
    # ═══════════════════════════════════════

    def _check_danger_time(self) -> Optional[str]:
        """위험 시간대 체크 — 14:00~14:50은 매수 금지 (알고리즘 장난 구간)"""
        now = datetime.now()
        danger = self.config.get("risk", {}).get("danger_hours", {"start": "14:00", "end": "14:50"})
        start_h, start_m = map(int, danger["start"].split(":"))
        end_h, end_m = map(int, danger["end"].split(":"))
        t = now.hour * 60 + now.minute
        ds = start_h * 60 + start_m
        de = end_h * 60 + end_m
        if ds <= t <= de:
            return f"위험 시간대 ({danger['start']}~{danger['end']}) — 매수 차단"
        return None

    def _check_volume_ratio(self, code: str, qty: int) -> Optional[str]:
        """내 주문이 당일 거래량의 10% 넘으면 경고"""
        max_pct = self.risk.get("max_volume_pct", 0.10)
        price_info = self.fetch_price(code)
        if not price_info.get("success"):
            return None  # 조회 실패 시 통과
        vol = price_info.get("volume", 0)
        if vol > 0 and qty / vol > max_pct:
            return f"거래량 비중 초과: {qty}주/{vol:,}주 = {qty/vol*100:.1f}% (한도 {max_pct*100:.0f}%)"
        return None

    # ═══════════════════════════════════════
    #  주문 (분할 매수/매도)
    # ═══════════════════════════════════════

    def buy_market(self, code: str, qty: int, split: int = None) -> dict:
        """시장가 매수 — 분할 주문 지원

        Args:
            split: 분할 횟수 (None=config 기본값, 1=원샷)
        """
        if split is None:
            split = self.config.get("risk", {}).get("split_count", 3)
        split = max(1, min(split, 10))

        name = CODE_TO_NAME.get(code, code)

        if split <= 1 or qty <= 1:
            return self._execute_buy(code, qty, name)

        # 분할 주문
        chunk = qty // split
        remainder = qty - chunk * split
        results = []
        total_filled = 0

        for i in range(split):
            q = chunk + (1 if i < remainder else 0)
            if q <= 0:
                continue
            r = self._execute_buy(code, q, name)
            results.append(r)
            if r.get("success"):
                total_filled += q
            else:
                break  # 실패 시 중단
            if i < split - 1:
                time.sleep(0.3)  # 호가 안정화 대기

        success = total_filled > 0
        msg = f"분할 매수 {'완료' if success else '실패'}\n{name}({code}) {total_filled}/{qty}주 ({split}분할)"
        if total_filled < qty:
            msg += f"\n미체결: {qty - total_filled}주"

        self._log_trade("BUY", code, name, total_filled, split)
        return {"success": success, "message": msg}

    def _execute_buy(self, code: str, qty: int, name: str) -> dict:
        """단건 시장가 매수"""
        try:
            broker = self._get_broker()
            resp = broker.create_market_buy_order(symbol=code, quantity=qty)
            logger.info(f"시장가 매수: {code} {qty}주 → {resp}")

            if resp and resp.get("rt_cd") == "0":
                order_no = resp.get("output", {}).get("ODNO", "?")
                return {"success": True, "order_no": order_no,
                        "message": f"매수 {name}({code}) {qty}주 #{order_no}"}
            else:
                msg = resp.get("msg1", "알 수 없는 오류") if resp else "응답 없음"
                return {"success": False, "message": f"매수 실패: {msg}"}
        except Exception as e:
            logger.error(f"매수 실패 {code}: {e}")
            return {"success": False, "message": f"매수 실패: {e}"}

    def sell_market(self, code: str, qty: int, split: int = None) -> dict:
        """시장가 매도 — 분할 주문 지원"""
        if split is None:
            split = self.config.get("risk", {}).get("split_count", 3)
        split = max(1, min(split, 10))

        name = CODE_TO_NAME.get(code, code)

        if split <= 1 or qty <= 1:
            return self._execute_sell(code, qty, name)

        # 분할 주문
        chunk = qty // split
        remainder = qty - chunk * split
        results = []
        total_filled = 0

        for i in range(split):
            q = chunk + (1 if i < remainder else 0)
            if q <= 0:
                continue
            r = self._execute_sell(code, q, name)
            results.append(r)
            if r.get("success"):
                total_filled += q
            else:
                break
            if i < split - 1:
                time.sleep(0.3)

        success = total_filled > 0
        msg = f"분할 매도 {'완료' if success else '실패'}\n{name}({code}) {total_filled}/{qty}주 ({split}분할)"
        if total_filled < qty:
            msg += f"\n미체결: {qty - total_filled}주"

        self._log_trade("SELL", code, name, total_filled, split)
        return {"success": success, "message": msg}

    def _execute_sell(self, code: str, qty: int, name: str) -> dict:
        """단건 시장가 매도"""
        try:
            broker = self._get_broker()
            resp = broker.create_market_sell_order(symbol=code, quantity=qty)
            logger.info(f"시장가 매도: {code} {qty}주 → {resp}")

            if resp and resp.get("rt_cd") == "0":
                order_no = resp.get("output", {}).get("ODNO", "?")
                return {"success": True, "order_no": order_no,
                        "message": f"매도 {name}({code}) {qty}주 #{order_no}"}
            else:
                msg = resp.get("msg1", "알 수 없는 오류") if resp else "응답 없음"
                return {"success": False, "message": f"매도 실패: {msg}"}
        except Exception as e:
            logger.error(f"매도 실패 {code}: {e}")
            return {"success": False, "message": f"매도 실패: {e}"}

    # ═══════════════════════════════════════
    #  스마트 지정가 매수/매도
    # ═══════════════════════════════════════

    @staticmethod
    def _tick_size(price: int) -> int:
        """한국 주식 호가 단위"""
        if price < 2000: return 1
        if price < 5000: return 5
        if price < 20000: return 10
        if price < 50000: return 50
        if price < 200000: return 100
        if price < 500000: return 500
        return 1000

    @staticmethod
    def _round_to_tick(price: float, tick: int, direction: str = "down") -> int:
        """호가 단위로 반올림 (down=매수유리, up=매도유리)"""
        if direction == "down":
            return int(price // tick) * tick
        else:
            return int(-(-price // tick)) * tick  # ceil

    def smart_buy(self, code: str, qty: int, max_wait_sec: int = 90) -> dict:
        """스마트 지정가 매수 — 3단계 에스컬레이션

        1단계: 현재가 -0.5% 지정가 → 30초 대기
        2단계: 미체결 시 현재가 -0.2% 수정 → 30초 대기
        3단계: 미체결 시 현재가로 수정 (사실상 시장가)

        시장가 대비 평균 0.2~0.5% 절약 효과
        """
        name = CODE_TO_NAME.get(code, code)

        # 현재가 조회
        price_info = self.fetch_price(code)
        if not price_info.get("success"):
            return {"success": False, "message": f"현재가 조회 실패"}

        current = price_info["current_price"]
        tick = self._tick_size(current)

        # ── 1단계: -0.5% 지정가 ──
        price_1 = self._round_to_tick(current * 0.995, tick, "down")
        logger.info(f"스마트매수 1단계: {name} {qty}주 @ {price_1:,}원 (-0.5%)")

        try:
            broker = self._get_broker()
            resp = broker.create_limit_buy_order(symbol=code, price=price_1, quantity=qty)

            if not resp or resp.get("rt_cd") != "0":
                msg = resp.get("msg1", "알 수 없는 오류") if resp else "응답 없음"
                logger.warning(f"스마트매수 1단계 실패: {msg} → 시장가 폴백")
                return self.buy_market(code, qty, split=1)

            order_no = resp.get("output", {}).get("ODNO", "")
            org_no = resp.get("output", {}).get("KRX_FWDG_ORD_ORGNO", "")

        except Exception as e:
            logger.error(f"스마트매수 주문 실패: {e} → 시장가 폴백")
            return self.buy_market(code, qty, split=1)

        # ── 체결 대기 (1단계: 30초) ──
        wait_per_step = max_wait_sec // 3
        filled = self._wait_for_fill(order_no, wait_per_step)

        if filled:
            self._log_trade("SMART_BUY", code, name, qty, 1)
            return {
                "success": True, "order_no": order_no,
                "message": f"스마트매수 {name}({code}) {qty}주 @ {price_1:,}원 (-0.5%)",
                "saved_pct": 0.5,
            }

        # ── 2단계: -0.2%로 수정 ──
        price_info2 = self.fetch_price(code)
        current2 = price_info2["current_price"] if price_info2.get("success") else current
        price_2 = self._round_to_tick(current2 * 0.998, tick, "down")
        logger.info(f"스마트매수 2단계: {name} 수정 @ {price_2:,}원 (-0.2%)")

        try:
            broker.modify_order(
                org_no=org_no, order_no=order_no,
                order_type="00", price=price_2, quantity=qty, total=True,
            )
        except Exception as e:
            logger.warning(f"주문 수정 실패: {e}")

        filled = self._wait_for_fill(order_no, wait_per_step)

        if filled:
            self._log_trade("SMART_BUY", code, name, qty, 2)
            return {
                "success": True, "order_no": order_no,
                "message": f"스마트매수 {name}({code}) {qty}주 @ {price_2:,}원 (-0.2%)",
                "saved_pct": 0.2,
            }

        # ── 3단계: 현재가로 수정 (사실상 시장가) ──
        price_info3 = self.fetch_price(code)
        current3 = price_info3["current_price"] if price_info3.get("success") else current2
        price_3 = self._round_to_tick(current3 * 1.002, tick, "up")  # 약간 높게 (체결 보장)
        logger.info(f"스마트매수 3단계: {name} 수정 @ {price_3:,}원 (현재가)")

        try:
            broker.modify_order(
                org_no=org_no, order_no=order_no,
                order_type="00", price=price_3, quantity=qty, total=True,
            )
        except Exception as e:
            logger.warning(f"주문 수정 실패: {e} → 취소 후 시장가")
            self.cancel_order(order_no)
            return self.buy_market(code, qty, split=1)

        filled = self._wait_for_fill(order_no, wait_per_step)

        if filled:
            self._log_trade("SMART_BUY", code, name, qty, 3)
            return {
                "success": True, "order_no": order_no,
                "message": f"스마트매수 {name}({code}) {qty}주 @ {price_3:,}원 (3단계)",
                "saved_pct": 0,
            }

        # 최종 실패 → 취소
        self.cancel_order(order_no)
        self._log_trade("SMART_BUY_FAIL", code, name, 0, 3)
        return {"success": False, "message": f"스마트매수 실패 — {max_wait_sec}초 내 미체결 ({name})"}

    def smart_sell(self, code: str, qty: int, max_wait_sec: int = 60) -> dict:
        """스마트 지정가 매도 — 3단계 에스컬레이션

        1단계: 현재가 +0.5% 지정가 → 20초 대기
        2단계: 미체결 시 현재가 +0.2% 수정 → 20초 대기
        3단계: 미체결 시 현재가로 수정 (사실상 시장가)

        긴급 매도(SL)에는 사용하지 않음 — 시장가 직행
        """
        name = CODE_TO_NAME.get(code, code)

        price_info = self.fetch_price(code)
        if not price_info.get("success"):
            return {"success": False, "message": f"현재가 조회 실패"}

        current = price_info["current_price"]
        tick = self._tick_size(current)

        # ── 1단계: +0.5% 지정가 ──
        price_1 = self._round_to_tick(current * 1.005, tick, "up")
        logger.info(f"스마트매도 1단계: {name} {qty}주 @ {price_1:,}원 (+0.5%)")

        try:
            broker = self._get_broker()
            resp = broker.create_limit_sell_order(symbol=code, price=price_1, quantity=qty)

            if not resp or resp.get("rt_cd") != "0":
                msg = resp.get("msg1", "알 수 없는 오류") if resp else "응답 없음"
                logger.warning(f"스마트매도 1단계 실패: {msg} → 시장가 폴백")
                return self.sell_market(code, qty, split=1)

            order_no = resp.get("output", {}).get("ODNO", "")
            org_no = resp.get("output", {}).get("KRX_FWDG_ORD_ORGNO", "")

        except Exception as e:
            logger.error(f"스마트매도 주문 실패: {e} → 시장가 폴백")
            return self.sell_market(code, qty, split=1)

        # ── 체결 대기 ──
        wait_per_step = max_wait_sec // 3
        filled = self._wait_for_fill(order_no, wait_per_step)

        if filled:
            self._log_trade("SMART_SELL", code, name, qty, 1)
            return {
                "success": True, "order_no": order_no,
                "message": f"스마트매도 {name}({code}) {qty}주 @ {price_1:,}원 (+0.5%)",
                "saved_pct": 0.5,
            }

        # ── 2단계: +0.2% ──
        price_info2 = self.fetch_price(code)
        current2 = price_info2["current_price"] if price_info2.get("success") else current
        price_2 = self._round_to_tick(current2 * 1.002, tick, "up")
        logger.info(f"스마트매도 2단계: {name} 수정 @ {price_2:,}원 (+0.2%)")

        try:
            broker.modify_order(
                org_no=org_no, order_no=order_no,
                order_type="00", price=price_2, quantity=qty, total=True,
            )
        except Exception as e:
            logger.warning(f"주문 수정 실패: {e}")

        filled = self._wait_for_fill(order_no, wait_per_step)

        if filled:
            self._log_trade("SMART_SELL", code, name, qty, 2)
            return {
                "success": True, "order_no": order_no,
                "message": f"스마트매도 {name}({code}) {qty}주 @ {price_2:,}원 (+0.2%)",
                "saved_pct": 0.2,
            }

        # ── 3단계: 현재가 ──
        price_info3 = self.fetch_price(code)
        current3 = price_info3["current_price"] if price_info3.get("success") else current2
        price_3 = self._round_to_tick(current3 * 0.998, tick, "down")
        logger.info(f"스마트매도 3단계: {name} 수정 @ {price_3:,}원 (현재가)")

        try:
            broker.modify_order(
                org_no=org_no, order_no=order_no,
                order_type="00", price=price_3, quantity=qty, total=True,
            )
        except Exception as e:
            logger.warning(f"주문 수정 실패: {e} → 취소 후 시장가")
            self.cancel_order(order_no)
            return self.sell_market(code, qty, split=1)

        filled = self._wait_for_fill(order_no, wait_per_step)

        if filled:
            self._log_trade("SMART_SELL", code, name, qty, 3)
            return {
                "success": True, "order_no": order_no,
                "message": f"스마트매도 {name}({code}) {qty}주 @ {price_3:,}원 (3단계)",
                "saved_pct": 0,
            }

        # 최종 → 시장가 폴백
        self.cancel_order(order_no)
        logger.warning(f"스마트매도 실패 → 시장가 매도 폴백")
        return self.sell_market(code, qty, split=1)

    def _wait_for_fill(self, order_no: str, wait_sec: int) -> bool:
        """주문 체결 대기 (polling)

        부분 체결 시에도 잔량이 0이 되면 True 반환.
        타임아웃 시 부분 체결된 수량이 있으면 잔량 취소 후 True 반환.
        """
        check_interval = 3  # 3초마다 체크
        elapsed = 0

        while elapsed < wait_sec:
            time.sleep(check_interval)
            elapsed += check_interval

            try:
                orders = self.fetch_open_orders()
                if not orders.get("success"):
                    continue

                # 미체결 목록에 없으면 → 체결 완료
                pending = [o for o in orders["orders"] if o["order_no"] == order_no]
                if not pending:
                    logger.info(f"주문 {order_no} 체결 완료 ({elapsed}초)")
                    return True

                # 부분 체결 확인
                filled = pending[0]["filled_qty"]
                total = pending[0]["qty"]
                if filled > 0:
                    logger.info(f"주문 {order_no} 부분 체결: {filled}/{total}주")

            except Exception as e:
                logger.warning(f"체결 확인 실패: {e}")

        # 타임아웃 — 부분 체결 확인
        try:
            orders = self.fetch_open_orders()
            if orders.get("success"):
                pending = [o for o in orders["orders"] if o["order_no"] == order_no]
                if pending and pending[0]["filled_qty"] > 0:
                    # 부분 체결 있음 → 잔량 취소, 체결분만 인정
                    filled = pending[0]["filled_qty"]
                    logger.info(f"주문 {order_no} 부분 체결 {filled}주 → 잔량 취소")
                    self.cancel_order(order_no)
                    return True  # 부분이라도 체결됐으면 성공 처리
        except Exception:
            pass

        logger.info(f"주문 {order_no} 미체결 ({wait_sec}초 초과)")
        return False

    def check_spread(self, code: str) -> dict:
        """호가 스프레드 체크

        Returns: {ok, spread_pct, ask, bid, message}
        """
        try:
            broker = self._get_broker()
            headers = {
                "content-type": "application/json; charset=utf-8",
                "authorization": broker.access_token,
                "appKey": broker.api_key,
                "appSecret": broker.api_secret,
                "tr_id": "FHKST01010200",
            }
            params = {"fid_cond_mrkt_div_code": "J", "fid_input_iscd": code}
            import requests
            resp = requests.get(
                f"{broker.base_url}/uapi/domestic-stock/v1/quotations/inquire-asking-price-exp-ccn",
                headers=headers, params=params, timeout=5,
            )
            d = resp.json().get("output1", {})
            ask1 = int(d.get("askp1", 0))
            bid1 = int(d.get("bidp1", 0))

            if ask1 <= 0 or bid1 <= 0:
                return {"ok": True, "spread_pct": 0, "message": "호가 조회 실패 — 통과"}

            mid = (ask1 + bid1) / 2
            spread_pct = (ask1 - bid1) / mid * 100

            max_spread = self.risk.get("max_spread_pct", 0.5)
            if spread_pct > max_spread:
                return {
                    "ok": False, "spread_pct": round(spread_pct, 2),
                    "ask": ask1, "bid": bid1,
                    "message": f"스프레드 {spread_pct:.2f}% > {max_spread}% — 주문 보류",
                }
            return {
                "ok": True, "spread_pct": round(spread_pct, 2),
                "ask": ask1, "bid": bid1,
                "message": f"스프레드 {spread_pct:.2f}% OK",
            }
        except Exception as e:
            logger.warning(f"스프레드 체크 실패 {code}: {e}")
            return {"ok": True, "spread_pct": 0, "message": f"스프레드 체크 실패: {e} — 통과"}

    # ═══════════════════════════════════════
    #  매매 일지
    # ═══════════════════════════════════════

    def _log_trade(self, side: str, code: str, name: str, qty: int, split: int):
        """매매 기록을 일지 파일(JSON)에 저장"""
        today = date.today().isoformat()
        journal_file = JOURNAL_DIR / f"journal_{today}.json"

        entry = {
            "time": datetime.now().strftime("%H:%M:%S"),
            "side": side,
            "code": code,
            "name": name,
            "qty": qty,
            "split": split,
        }

        # 현재가 정보 추가
        price_info = self.fetch_price(code)
        if price_info.get("success"):
            entry["price"] = price_info["current_price"]
            entry["est_amount"] = price_info["current_price"] * qty

        # 기존 일지 로드
        entries = []
        if journal_file.exists():
            try:
                with open(journal_file, "r", encoding="utf-8") as f:
                    entries = json.load(f)
            except (json.JSONDecodeError, Exception):
                entries = []

        entries.append(entry)

        with open(journal_file, "w", encoding="utf-8") as f:
            json.dump(entries, f, ensure_ascii=False, indent=2)

        logger.info(f"매매일지 기록: {side} {name}({code}) {qty}주 (분할{split})")

    def get_trade_journal(self, target_date: str = None) -> dict:
        """매매 일지 조회

        Args:
            target_date: "2026-02-19" 형식. None이면 오늘.

        Returns:
            {success, date, trades: [...], summary: {buy_count, sell_count, total_amount}}
        """
        if target_date is None:
            target_date = date.today().isoformat()

        journal_file = JOURNAL_DIR / f"journal_{target_date}.json"

        if not journal_file.exists():
            return {"success": True, "date": target_date, "trades": [], "summary": {
                "buy_count": 0, "sell_count": 0, "total_buy_amount": 0, "total_sell_amount": 0}}

        try:
            with open(journal_file, "r", encoding="utf-8") as f:
                trades = json.load(f)
        except (json.JSONDecodeError, Exception):
            return {"success": False, "message": "일지 파일 손상"}

        buy_count = sum(1 for t in trades if t["side"] == "BUY")
        sell_count = sum(1 for t in trades if t["side"] == "SELL")
        total_buy = sum(t.get("est_amount", 0) for t in trades if t["side"] == "BUY")
        total_sell = sum(t.get("est_amount", 0) for t in trades if t["side"] == "SELL")

        return {
            "success": True,
            "date": target_date,
            "trades": trades,
            "summary": {
                "buy_count": buy_count,
                "sell_count": sell_count,
                "total_buy_amount": total_buy,
                "total_sell_amount": total_sell,
            },
        }

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
        """금액 기반 안전 매수 (8단계 리스크 체크)

        1. 위험시간대 차단 (14:00~14:50)
        2. 호가 스프레드 체크 (0.5% 초과 시 보류)
        3. 현금 잔고 확인
        4. 포지션 수 확인 (max_positions)
        5. 비중 확인 (max_position_ratio)
        6. 현재가로 수량 계산
        7. 거래량 대비 비중 확인 (10% 이하)
        8. 스마트 지정가 매수
        """
        # 1. 위험시간 체크
        danger_msg = self._check_danger_time()
        if danger_msg:
            return {"success": False, "message": f"⚠️ {danger_msg}"}

        # 2. 호가 스프레드 체크
        spread = self.check_spread(code)
        if not spread["ok"]:
            return {"success": False, "message": f"⚠️ {spread['message']}"}

        # 2. 잔고 조회
        bal = self.fetch_balance()
        if not bal.get("success"):
            return {"success": False, "message": f"잔고 조회 실패: {bal.get('message')}"}

        cash = bal["cash"]
        total_eval = bal["total_eval"] or cash
        positions = bal["positions"]

        # 3. 포지션 수 체크
        max_positions = self.risk.get("max_positions", 5)
        max_ratio = self.risk.get("max_position_ratio", 0.30)
        min_cash = self.risk.get("min_cash_ratio", 0.10)

        if len(positions) >= max_positions:
            return {"success": False, "message": f"최대 보유 종목({max_positions}개) 초과"}

        # 4. 현금 잔고 여유 확인
        min_cash_amount = int(total_eval * min_cash)
        available = cash - min_cash_amount
        if available < amount:
            return {
                "success": False,
                "message": f"현금 부족\n가용: {available:,}원 (최소 현금 {min_cash_amount:,}원 유지)\n요청: {amount:,}원",
            }

        # 5. 비중 확인 + 수량 계산
        max_amount = int(total_eval * max_ratio)
        buy_amount = min(amount, max_amount)

        price_info = self.fetch_price(code)
        if not price_info.get("success"):
            return {"success": False, "message": f"현재가 조회 실패: {price_info.get('message')}"}

        current_price = price_info["current_price"]
        if current_price <= 0:
            return {"success": False, "message": "현재가가 0원입니다"}

        qty = buy_amount // current_price
        if qty <= 0:
            return {"success": False, "message": f"매수 가능 수량 없음 (현재가: {current_price:,}원, 금액: {buy_amount:,}원)"}

        # 6. 거래량 대비 비중 체크
        vol_warn = self._check_volume_ratio(code, qty)
        if vol_warn:
            # 비중 초과 시 거래량의 10%로 수량 조정
            vol = price_info.get("volume", 0)
            max_pct = self.risk.get("max_volume_pct", 0.10)
            if vol > 0:
                max_qty = int(vol * max_pct)
                if max_qty <= 0:
                    return {"success": False, "message": f"⚠️ {vol_warn}\n거래량 부족으로 매수 불가"}
                qty = max_qty
                logger.warning(f"거래량 비중 조정: {qty}주로 축소 ({vol_warn})")

        name = CODE_TO_NAME.get(code, code)
        est_cost = qty * current_price

        logger.info(f"안전 매수: {name}({code}) {qty}주 @ {current_price:,}원 ≈ {est_cost:,}원")

        # 7. 스마트 지정가 매수 (시장가 대비 0.2~0.5% 절약)
        return self.smart_buy(code, qty)

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
