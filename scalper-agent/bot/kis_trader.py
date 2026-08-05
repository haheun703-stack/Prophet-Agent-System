# -*- coding: utf-8 -*-
"""
KIS API 실매매 래퍼 - mojito2 기반
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
import threading
from pathlib import Path
from datetime import datetime, date
from typing import Optional

logger = logging.getLogger("BH.KISTrader")

# 매매일지 저장 경로
JOURNAL_DIR = Path(__file__).resolve().parent.parent / "logs"
JOURNAL_DIR.mkdir(parents=True, exist_ok=True)

# mojito token.dat 절대경로 (CWD 변경 최소화)
_SCALPER_DIR = str(Path(__file__).resolve().parent.parent)
_CWD_LOCK = threading.RLock()  # 프로세스 전역 CWD 보호 (mojito token.dat이 상대경로 hardcoded)

# 종목명 매핑
from data.kis_collector import UNIVERSE
NAME_TO_CODE = {info[0]: code for code, info in UNIVERSE.items()}
CODE_TO_NAME = {code: info[0] for code, info in UNIVERSE.items()}

try:
    from data.sajang_rules import SAJANG
except Exception:
    SAJANG = None


def _safe_int(val, default=0) -> int:
    """KIS API가 빈 문자열/None 반환 시 안전하게 int 변환"""
    if val is None or val == "":
        return default
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return default


def _safe_float(val, default=0.0) -> float:
    """KIS API가 빈 문자열/None 반환 시 안전하게 float 변환"""
    if val is None or val == "":
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


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
        self._broker_created_at: float = 0.0  # 브로커 생성 시각 (epoch)
        self._consecutive_failures: int = 0   # 연속 실패 카운터
        self._lock = threading.Lock()  # 스레드 안전: tracker+summary 동시 호출 방지
        self._last_balance = None              # tr_cont 에러 시 캐시 fallback
        self._last_balance_at: float = 0.0     # 캐시 저장 시각 (epoch)

    def _is_auto_trade_disabled(self) -> bool:
        try:
            from bot.trade_kill_switch import is_auto_trade_disabled

            return bool(is_auto_trade_disabled())
        except Exception:
            return bool(getattr(SAJANG, "AUTO_TRADE_DISABLED", False))

    def _estimate_order_amount(self, side: str, code: str, qty: int) -> Optional[int]:
        if str(side or "").upper() != "BUY" or int(qty or 0) <= 0:
            return None
        try:
            price_info = self.fetch_price(code)
            current = int(price_info.get("current_price", 0) or 0) if price_info else 0
            if current <= 0:
                return None
            return current * int(qty)
        except Exception:
            return None

    def _record_order_intent_or_block(
        self,
        *,
        side: str,
        code: str,
        qty: int,
        manual: bool,
        source: str,
        allowed: bool,
        reason: str = "",
        message: str = "",
        strategy: str = "",
        estimate_amount_krw: Optional[int] = None,
        order_no: str = "",
        rt_cd: str = "",
        filled_qty: int = 0,
        avg_fill_price: int = 0,
    ) -> Optional[dict]:
        try:
            from bot.order_intent import record_order_intent

            record_order_intent(
                side=side,
                code=code,
                qty=qty,
                manual=manual,
                source=source,
                allowed=allowed,
                reason=reason,
                message=message,
                strategy=strategy,
                estimate_amount_krw=estimate_amount_krw,
                order_no=order_no,
                rt_cd=rt_cd,
                filled_qty=filled_qty,
                avg_fill_price=avg_fill_price,
            )
            return None
        except Exception as exc:
            msg = f"ORDER_INTENT_RECORD_FAILED - blocked {side} order: {code} x {qty} ({exc})"
            logger.error("[ORDER BLOCKED] %s source=%s", msg, source)
            logger.critical("[ORDER INTENT AUDIT] %s", msg)
            alert_sent = self._send_order_intent_audit_alert(msg)
            return {
                "success": False,
                "blocked": True,
                "reason": "ORDER_INTENT_RECORD_FAILED",
                "message": msg,
                "intent_logged": False,
                "audit_warning": msg,
                "alert_sent": alert_sent,
            }

    def _send_order_intent_audit_alert(self, message: str) -> bool:
        token = os.getenv("TELEGRAM_BOT_TOKEN")
        chat_id = os.getenv("TELEGRAM_CHAT_ID")
        if not token or not chat_id:
            return False
        try:
            from urllib import parse, request

            payload = parse.urlencode({
                "chat_id": chat_id,
                "text": f"[ORDER INTENT AUDIT]\n{message}",
            }).encode("utf-8")
            request.urlopen(
                f"https://api.telegram.org/bot{token}/sendMessage",
                data=payload,
                timeout=3,
            )
            return True
        except Exception as exc:
            logger.warning("[ORDER INTENT AUDIT] telegram alert failed: %s", exc)
            return False

    def _order_gate(self, side: str, code: str, qty: int, *, manual: bool = False,
                    source: str = "", record_allowed: bool = True) -> Optional[dict]:
        """Central real-order gate.

        All order paths must write an order intent before a KIS network order.
        Manual Telegram/CLI orders pass manual=True explicitly and are still
        recorded, but automated orders must also pass kill switch, strategy
        switch, daily limit, and manual-position protection checks.
        """
        side_u = str(side or "").upper()
        source_s = str(source or "")
        strategy = ""
        estimate_amount = None

        if manual:
            if not record_allowed:
                return None
            blocked = self._record_order_intent_or_block(
                side=side_u,
                code=code,
                qty=qty,
                manual=True,
                source=source_s,
                allowed=True,
                reason="MANUAL_ORDER",
                message="manual order allowed",
            )
            if blocked:
                return blocked
            return None

        if not self._is_auto_trade_disabled():
            try:
                from bot.trade_runtime_config import evaluate_auto_order, resolve_strategy

                strategy = resolve_strategy(side_u, source_s)
                runtime_block = evaluate_auto_order(side_u, code, qty, source_s)
                if runtime_block is None:
                    estimate_amount = self._estimate_order_amount(side_u, code, qty)
                    runtime_block = evaluate_auto_order(
                        side_u,
                        code,
                        qty,
                        source_s,
                        estimate_amount_krw=estimate_amount,
                    )
                if runtime_block:
                    msg = f"{runtime_block['reason']} - blocked auto {side_u} order: {code} x {qty}"
                    blocked = self._record_order_intent_or_block(
                        side=side_u,
                        code=code,
                        qty=qty,
                        manual=False,
                        source=source_s,
                        allowed=False,
                        reason=str(runtime_block.get("reason", "")),
                        message=str(runtime_block.get("message", msg)),
                        strategy=strategy,
                        estimate_amount_krw=estimate_amount,
                    )
                    logger.warning("[ORDER BLOCKED] %s source=%s", msg, source_s)
                    if blocked:
                        return blocked
                    return {
                        "success": False,
                        "blocked": True,
                        "reason": runtime_block["reason"],
                        "message": msg,
                    }
            except Exception as exc:
                msg = f"RUNTIME_CONFIG_ERROR - blocked auto {side_u} order: {code} x {qty} ({exc})"
                blocked = self._record_order_intent_or_block(
                    side=side_u,
                    code=code,
                    qty=qty,
                    manual=False,
                    source=source_s,
                    allowed=False,
                    reason="RUNTIME_CONFIG_ERROR",
                    message=msg,
                    strategy=strategy,
                )
                logger.error("[ORDER BLOCKED] %s source=%s", msg, source_s)
                if blocked:
                    return blocked
                return {
                    "success": False,
                    "blocked": True,
                    "reason": "RUNTIME_CONFIG_ERROR",
                    "message": msg,
                }

            if side_u == "SELL":
                try:
                    from bot.manual_position_protection import auto_sell_block_reason

                    protect_reason = auto_sell_block_reason(code)
                except Exception as exc:
                    protect_reason = f"manual_protection_error:{exc}"
                if protect_reason:
                    msg = f"MANUAL_POSITION_PROTECTED - blocked auto SELL order: {code} x {qty}"
                    blocked = self._record_order_intent_or_block(
                        side=side_u,
                        code=code,
                        qty=qty,
                        manual=False,
                        source=source_s,
                        allowed=False,
                        reason="MANUAL_POSITION_PROTECTED",
                        message=f"{msg} ({protect_reason})",
                        strategy=strategy,
                    )
                    logger.warning("[ORDER BLOCKED] %s reason=%s source=%s", msg, protect_reason, source_s)
                    if blocked:
                        return blocked
                    return {
                        "success": False,
                        "blocked": True,
                        "reason": "MANUAL_POSITION_PROTECTED",
                        "message": f"{msg} ({protect_reason})",
                    }

            if record_allowed:
                blocked = self._record_order_intent_or_block(
                    side=side_u,
                    code=code,
                    qty=qty,
                    manual=False,
                    source=source_s,
                    allowed=True,
                    reason="AUTO_ORDER_ALLOWED",
                    message="automated order passed runtime gates",
                    strategy=strategy,
                    estimate_amount_krw=estimate_amount,
                )
                if blocked:
                    return blocked
            return None

        msg = (
            f"AUTO_TRADE_DISABLED=True - blocked auto {side_u} order: "
            f"{code} x {qty}"
        )
        blocked = self._record_order_intent_or_block(
            side=side_u,
            code=code,
            qty=qty,
            manual=False,
            source=source_s,
            allowed=False,
            reason="AUTO_TRADE_DISABLED",
            message=msg,
            strategy=strategy,
        )
        logger.warning("[ORDER BLOCKED] %s source=%s", msg, source_s)
        if blocked:
            return blocked
        return {
            "success": False,
            "blocked": True,
            "reason": "AUTO_TRADE_DISABLED",
            "message": msg,
        }

    def _get_broker(self, force_refresh=False):
        """브로커 (토큰 자동 갱신, 스레드 안전)

        - 토큰 만료 감지: 생성 후 20시간 경과 시 자동 재발급
        - 연속 실패 3회 시 강제 재발급
        - mojito token.dat은 상대경로 → CWD를 scalper-agent로 고정
        - threading.Lock으로 tracker/summary 동시 호출 시 안전
        """
        with self._lock:
            now = time.time()
            need_refresh = force_refresh

            # 토큰 수명 체크: 20시간(72000초) 경과 → 재발급 (KIS 토큰 24시간 만료)
            if self._broker is not None and (now - self._broker_created_at) > 72000:
                logger.warning(f"KIS 토큰 수명 초과 ({(now - self._broker_created_at)/3600:.1f}h) - 재발급")
                need_refresh = True

            # 연속 실패 3회 → 토큰 문제 의심, 강제 재발급
            if self._consecutive_failures >= 3:
                logger.warning(f"KIS API 연속 {self._consecutive_failures}회 실패 - 토큰 재발급 시도")
                need_refresh = True

            if self._broker is not None and not need_refresh:
                return self._broker

            from dotenv import load_dotenv
            load_dotenv()
            import mojito

            # mojito token.dat은 CWD 기준 상대경로 → 모듈 레벨 락으로 CWD 보호
            # (mojito 라이브러리가 절대경로를 지원하지 않아 os.chdir 불가피)
            with _CWD_LOCK:
                original_cwd = os.getcwd()
                try:
                    os.chdir(_SCALPER_DIR)
                    self._broker = mojito.KoreaInvestment(
                        api_key=os.getenv("KIS_APP_KEY"),
                        api_secret=os.getenv("KIS_APP_SECRET"),
                        acc_no=os.getenv("KIS_ACC_NO"),
                        mock=False,
                    )
                finally:
                    os.chdir(original_cwd)

            self._broker_created_at = now
            self._consecutive_failures = 0
            action = "재발급" if need_refresh else "생성"
            logger.info(f"KIS API 브로커 {action} 완료 (token_dir={_SCALPER_DIR})")
            return self._broker

    def _reset_broker(self):
        """토큰 에러 시 브로커 강제 초기화 (다음 호출에서 재발급)"""
        with self._lock:
            self._broker = None
            self._broker_created_at = 0.0
            self._last_balance = None      # 캐시도 초기화 (stale 방지)
            self._last_balance_at = 0.0
            logger.info("KIS 브로커 초기화됨 - 다음 호출에서 재발급")

    def ensure_fresh_token(self):
        """토큰 사전 갱신 — G7 시작 시 호출하여 파이프라인 내 토큰 에러 방지.

        기존 브로커를 강제 폐기 → 새 브로커 생성 → 테스트 API 호출로 토큰 유효성 확인.
        이후 파이프라인에서 매 종목마다 브로커 재생성하는 낭비 방지.
        """
        logger.info("[KIS] 토큰 사전 갱신 시작...")
        self._reset_broker()
        try:
            # 강제로 새 브로커 생성
            broker = self._get_broker(force_refresh=True)
            # 테스트 호출: 삼성전자 현재가 (확실한 종목)
            resp = broker.fetch_price("005930")
            if resp is not None and not self._is_token_error(resp):
                logger.info("[KIS] 토큰 사전 갱신 완료 — 유효한 토큰 확인")
                self._consecutive_failures = 0
                return True
            else:
                logger.warning("[KIS] 토큰 사전 갱신: 테스트 호출 실패 — "
                               "재시도 중...")
                self._reset_broker()
                time.sleep(1)
                broker = self._get_broker(force_refresh=True)
                logger.info("[KIS] 토큰 사전 갱신: 2차 시도 완료")
                return True
        except Exception as e:
            logger.error(f"[KIS] 토큰 사전 갱신 실패: {e}")
            return False

    # ═══════════════════════════════════════
    #  조회
    # ═══════════════════════════════════════

    def fetch_balance(self) -> dict:
        """계좌 잔고 조회 (토큰 만료 시 자동 재발급 + 1회 재시도)

        Returns: {success, cash, total_eval, positions: [{code, name, qty, avg_price, current_price, pnl_amount, pnl_rate}]}
        """
        for attempt in range(2):
            try:
                broker = self._get_broker()
                # mojito 내부에서 data['tr_cont'] 직접 접근 → KeyError 방어
                # 원본 응답을 감싸서 tr_cont 누락 시 빈 값 주입
                try:
                    resp = broker.fetch_balance()
                except KeyError as ke:
                    if "tr_cont" in str(ke):
                        logger.warning(f"잔고 조회 tr_cont KeyError — mojito 페이징 버그 방어")
                        # 캐시된 잔고가 5분 이내이면 반환 (stale 방지)
                        if (self._last_balance
                                and self._last_balance.get("success")
                                and time.time() - self._last_balance_at < 300):
                            age = int(time.time() - self._last_balance_at)
                            logger.info(f"캐시된 잔고 반환 ({age}초 전)")
                            return self._last_balance
                        return {"success": False, "message": "잔고 조회 tr_cont KeyError (장후)"}
                    raise

                if resp is None:
                    if attempt == 0:
                        logger.warning("잔고 조회 None - 토큰 재발급 후 재시도")
                        self._reset_broker()
                        time.sleep(0.5)
                        continue
                    return {"success": False, "message": "잔고 조회 실패 (응답 없음)"}

                if self._is_token_error(resp):
                    if attempt == 0:
                        logger.warning(f"잔고 조회 토큰 에러 - 재발급 후 재시도")
                        self._reset_broker()
                        time.sleep(0.5)
                        continue
                    return {"success": False, "message": "토큰 재발급 후에도 잔고 조회 실패"}

                # ★8/5 전체검수 [F-96] — rt_cd 미검사로 거부 응답을 "보유 0종목"으로 읽던 통로.
                #   ★파급이 가장 무겁다: position_safety:417이 positions=[]로 루프 0회를 돌고
                #     **아무 로그도 안 남긴다** → hard_kill·룰4·룰B·룰C가 무증상 소멸.
                #     auto_trader:2180은 _real_holdings_codes=set()이 되어 09:15 중복 차단이
                #     메모리 장부에만 의존 → **보유 종목 재매수**. 실패 분기(:2189)는 텔레그램
                #     경고라도 나가는데 rt_cd=1은 성공 분기로 들어가 경고조차 없었다.
                _rt = str(resp.get("rt_cd", "0"))
                if _rt != "0":
                    with self._lock:
                        self._consecutive_failures += 1
                    return {"success": False,
                            "message": f"잔고 조회 거부 rt_cd={_rt} msg_cd={resp.get('msg_cd','')}"}

                output = resp.get("output1", [])
                summary = resp.get("output2", [{}])

                positions = []
                for item in output:
                    qty = _safe_int(item.get("hldg_qty", 0))
                    if qty <= 0:
                        continue
                    code = item.get("pdno", "")
                    positions.append({
                        "code": code,
                        "name": item.get("prdt_name", CODE_TO_NAME.get(code, code)),
                        "qty": qty,
                        "avg_price": _safe_int(item.get("pchs_avg_pric", 0)),
                        "current_price": _safe_int(item.get("prpr", 0)),
                        "pnl_amount": _safe_int(item.get("evlu_pfls_amt", 0)),
                        "pnl_rate": _safe_float(item.get("evlu_pfls_rt", 0)),
                    })

                s = summary[0] if summary else {}
                # KIS API 주문가능현금 우선순위 (5/18 사장님 지적):
                #   psbl_ord_amt (mojito 매핑) → prvs_rcdl_excc_amt (가수도정산 D+2, HTS 주문가능원화) → dnca_tot_amt (예수금 D+0)
                #   기존: dnca_tot_amt만 fallback → 107만 / 실제 HTS 5,684만 → 5,500만 누락 사고
                cash = _safe_int(
                    s.get("psbl_ord_amt", 0)
                    or s.get("prvs_rcdl_excc_amt", 0)
                    or s.get("dnca_tot_amt", 0)
                )
                total_eval = _safe_int(s.get("tot_evlu_amt", 0))

                with self._lock:
                    self._consecutive_failures = 0
                result = {
                    "success": True,
                    "cash": cash,
                    "total_eval": total_eval,
                    "positions": positions,
                    # ★ 5/20 사고 fix: 'holdings' 키 추가 (portfolio_monitor 등 호환) ★
                    # 기존 'positions' 키만 반환 → 일부 모듈이 'holdings' 호출하면 빈 리스트 받음
                    # 'holdings'는 'positions'의 alias (동일 데이터)
                    "holdings": positions,
                }
                self._last_balance = result     # 장후 tr_cont 에러 시 캐시 활용
                self._last_balance_at = time.time()
                return result

            except Exception as e:
                with self._lock:
                    self._consecutive_failures += 1
                err_str = str(e).lower()
                if attempt == 0 and ("token" in err_str or "auth" in err_str or "401" in err_str or "tr_cont" in err_str):
                    logger.warning(f"잔고 조회 예외(토큰/세션): {e} - 재발급 후 재시도")
                    self._reset_broker()
                    time.sleep(0.5)
                    continue
                logger.error(f"잔고 조회 실패: {e}")
                return {"success": False, "message": f"잔고 조회 실패: {e}"}

        return {"success": False, "message": "fetch_balance 재시도 소진"}

    def _is_token_error(self, resp) -> bool:
        """KIS API 응답에서 토큰/인증 에러 감지"""
        if resp is None:
            return False
        # KIS API 에러코드: EGW00123 = 토큰 만료, EGW00121 = 유효하지 않은 토큰
        msg_cd = resp.get("msg_cd", "") or resp.get("rt_cd", "")
        msg1 = resp.get("msg1", "")
        if any(ec in msg_cd for ec in ("EGW00123", "EGW00121", "EGW00105")):
            return True
        if "token" in msg1.lower() or "접근토큰" in msg1:
            return True
        return False

    def fetch_price_nx(self, code: str, fallback: bool = True) -> dict:
        """NX 시장(KRX+NXT 통합) 현재가/거래량 조회 — Phase 1 신규.

        Why: fetch_price()는 'J'(KRX 정규장)만 → NXT 거래량 누락.
        자동매매 진입 평가 시 NXT 비중 큰 종목은 진짜 수급 못 봄.

        Args:
            code: 6자리 종목코드
            fallback: NX 실패 시 KRX(fetch_price) 자동 fallback (기본 True)

        Returns:
            success 시: fetch_price와 동일 키 + 'market': 'NX' 또는 'J' (fallback)
        """
        from utils.kis_nxt_kit import fetch_nx_price
        r = fetch_nx_price(code)
        if r.get("success"):
            return r
        if fallback:
            r2 = self.fetch_price(code)
            if r2.get("success"):
                r2["market"] = "J"
                r2["nx_fallback_reason"] = r.get("message", "?")
            return r2
        return r

    def fetch_price(self, code: str) -> dict:
        """현재가 조회 (토큰 만료 시 자동 재발급 + 1회 재시도)

        Returns: {success, current_price, change_rate, volume, ...}
        """
        for attempt in range(2):  # 최대 2회 (원본 + 재시도)
            try:
                broker = self._get_broker()
                resp = broker.fetch_price(code)

                if resp is None:
                    with self._lock:
                        self._consecutive_failures += 1
                    if attempt == 0:
                        logger.warning(f"현재가 조회 None {code} - 토큰 재발급 후 재시도")
                        self._reset_broker()
                        time.sleep(0.5)
                        continue
                    return {"success": False, "message": "현재가 조회 실패 (resp=None)"}

                # 토큰 에러 감지 → 브로커 재발급 후 재시도
                if self._is_token_error(resp):
                    with self._lock:
                        self._consecutive_failures += 1
                    if attempt == 0:
                        msg = resp.get("msg1", resp.get("msg_cd", ""))
                        logger.warning(f"KIS 토큰 에러 {code}: {msg} - 재발급 후 재시도")
                        self._reset_broker()
                        time.sleep(0.5)
                        continue
                    return {"success": False, "message": f"토큰 재발급 후에도 실패: {resp.get('msg1', '')}"}

                output = resp.get("output", {})
                cp = _safe_int(output.get("stck_prpr", 0))
                if cp > 0:
                    with self._lock:
                        self._consecutive_failures = 0  # 성공 시 카운터 리셋
                    return {
                        "success": True,
                        "current_price": cp,
                        "change_rate": _safe_float(output.get("prdy_ctrt", 0)),
                        "volume": _safe_int(output.get("acml_vol", 0)),
                        "high": _safe_int(output.get("stck_hgpr", 0)),
                        "low": _safe_int(output.get("stck_lwpr", 0)),
                        "open": _safe_int(output.get("stck_oprc", 0)),
                        "vwap": _safe_int(output.get("wghn_avrg_stck_prc", 0)),
                        "vwap_source": "KIS_WEIGHTED_AVG",
                        "strength": _safe_float(output.get("tday_rltv", 0) or 0),
                        "listed_shares": _safe_int(output.get("lstn_stcn", 0)),
                    }
                # cp == 0: API 응답은 왔지만 가격 없음 (장외시간/정지 등)
                if attempt == 0:
                    logger.warning(f"현재가 0원 {code} - 재시도")
                    time.sleep(0.3)
                    continue
                return {"success": False, "message": f"현재가 0원 (output={list(output.keys())[:3]})"}

            except Exception as e:
                with self._lock:
                    self._consecutive_failures += 1
                err_str = str(e).lower()
                # 토큰/인증 관련 예외 → 재시도
                if attempt == 0 and ("token" in err_str or "auth" in err_str or "401" in err_str):
                    logger.warning(f"현재가 조회 예외(토큰) {code}: {e} - 재발급 후 재시도")
                    self._reset_broker()
                    time.sleep(0.5)
                    continue
                logger.error(f"현재가 조회 실패 {code}: {e}")
                return {"success": False, "message": f"현재가 조회 실패: {e}"}

        return {"success": False, "message": "fetch_price 재시도 소진"}

    def fetch_investor_daily(self, code: str) -> dict:
        """투자자별 매매동향 (30일치, KIS API)

        pykrx 수급 API 깨짐 → KIS API 대체 (2026-03-04)
        tr_id: FHKST01010900 (주식현재가 투자자)

        Returns: {success, data: [{date, close, foreign_net_qty, inst_net_qty, indi_net_qty, ...}]}
        """
        try:
            broker = self._get_broker()
            import requests

            token = broker.access_token
            if token.startswith("Bearer "):
                token = token.replace("Bearer ", "")

            headers = {
                "content-type": "application/json; charset=utf-8",
                "authorization": f"Bearer {token}",
                "appkey": os.getenv("KIS_APP_KEY"),
                "appsecret": os.getenv("KIS_APP_SECRET"),
                "tr_id": "FHKST01010900",
                "custtype": "P",
            }

            params = {
                "FID_COND_MRKT_DIV_CODE": "J",
                "FID_INPUT_ISCD": code,
            }

            base_url = "https://openapi.koreainvestment.com:9443"
            resp = requests.get(
                f"{base_url}/uapi/domestic-stock/v1/quotations/inquire-investor",
                headers=headers, params=params, timeout=10,
            )
            data = resp.json()

            if data.get("rt_cd") != "0":
                return {"success": False, "message": data.get("msg1", "API 오류")}

            rows = []
            for item in data.get("output", []):
                rows.append({
                    "date": item.get("stck_bsop_date", ""),
                    "close": _safe_int(item.get("stck_clpr", 0)),
                    "change": _safe_int(item.get("prdy_vrss", 0)),
                    # 순매수 수량
                    "foreign_net_qty": _safe_int(item.get("frgn_ntby_qty", 0)),
                    "inst_net_qty": _safe_int(item.get("orgn_ntby_qty", 0)),
                    "indi_net_qty": _safe_int(item.get("prsn_ntby_qty", 0)),
                    # 순매수 금액 (백만원)
                    "foreign_net_amount": _safe_int(item.get("frgn_ntby_tr_pbmn", 0)),
                    "inst_net_amount": _safe_int(item.get("orgn_ntby_tr_pbmn", 0)),
                    "indi_net_amount": _safe_int(item.get("prsn_ntby_tr_pbmn", 0)),
                    # 매수 수량
                    "foreign_buy_qty": _safe_int(item.get("frgn_shnu_vol", 0)),
                    "inst_buy_qty": _safe_int(item.get("orgn_shnu_vol", 0)),
                    # 매도 수량
                    "foreign_sell_qty": _safe_int(item.get("frgn_seln_vol", 0)),
                    "inst_sell_qty": _safe_int(item.get("orgn_seln_vol", 0)),
                })

            return {"success": True, "data": rows}

        except Exception as e:
            logger.error(f"투자자별 매매동향 조회 실패 {code}: {e}")
            return {"success": False, "message": str(e)}

    def fetch_daily_short_sale(self, code: str, days: int = 40) -> dict:
        """공매도 일별추이 (FHPST04830000) — KIS API.

        KRX/pykrx 공매도 데이터 중단(2026-04~) 대체. 변동성 장 급락 선행지표.
        엔드포인트: /uapi/domestic-stock/v1/quotations/daily-short-sale
        output2 = 일별 배열. record-only 적재 전용(매매 무접촉).

        Returns: {success, data: [{date, close, short_qty, short_amt, short_ratio, avg_price}]}
        주: output2 필드명 = 2026-06-27 노트북+VPS 실측 확정(005930 등 실제값 적재 검증).
        """
        try:
            broker = self._get_broker()
            import requests
            from datetime import datetime, timedelta

            token = broker.access_token
            if token.startswith("Bearer "):
                token = token.replace("Bearer ", "")

            headers = {
                "content-type": "application/json; charset=utf-8",
                "authorization": f"Bearer {token}",
                "appkey": os.getenv("KIS_APP_KEY"),
                "appsecret": os.getenv("KIS_APP_SECRET"),
                "tr_id": "FHPST04830000",
                "custtype": "P",
            }
            today = datetime.now()
            params = {
                "FID_COND_MRKT_DIV_CODE": "J",
                "FID_INPUT_ISCD": code,
                "FID_INPUT_DATE_1": (today - timedelta(days=days)).strftime("%Y%m%d"),
                "FID_INPUT_DATE_2": today.strftime("%Y%m%d"),
            }
            base_url = "https://openapi.koreainvestment.com:9443"
            resp = requests.get(
                f"{base_url}/uapi/domestic-stock/v1/quotations/daily-short-sale",
                headers=headers, params=params, timeout=10,
            )
            data = resp.json()
            if data.get("rt_cd") != "0":
                return {"success": False, "message": data.get("msg1", "API 오류")}

            out2 = data.get("output2", []) or []
            if out2 and not getattr(self, "_short_keys_logged", False):
                logger.info(f"[SHORT] output2 첫행 키: {list(out2[0].keys())}")
                self._short_keys_logged = True

            rows = []
            for item in out2:
                d = item.get("stck_bsop_date", "")
                if not d:
                    continue
                rows.append({
                    "date": d,
                    "close": _safe_int(item.get("stck_clpr", 0)),
                    # 공매도(실측 확정 2026-06-27): 당일 체결수량/거래대금/거래량비중%,
                    #   누적 공매도잔고수량, 공매도 평균가
                    "short_qty": _safe_int(item.get("ssts_cntg_qty", 0)),
                    "short_amt": _safe_int(item.get("ssts_tr_pbmn", 0)),
                    "short_ratio": _safe_float(item.get("ssts_vol_rlim", 0)),
                    "short_bal_qty": _safe_int(item.get("acml_ssts_cntg_qty", 0)),
                    "avg_price": _safe_float(item.get("avrg_prc", 0)),
                })
            return {"success": True, "data": rows}

        except Exception as e:
            logger.error(f"공매도 일별추이 조회 실패 {code}: {e}")
            return {"success": False, "message": str(e)}

    def fetch_daily_credit_balance(self, code: str) -> dict:
        """신용잔고 일별추이 (FHPST04760000) — KIS API.

        변동성 장 반대매매 급락 선행지표. record-only 적재 전용(매매 무접촉).
        엔드포인트: /uapi/domestic-stock/v1/quotations/daily-credit-balance
        output = 일별 배열(KIS 페이지네이션 가능, 1페이지로 최근분 충분).

        Returns: {success, data: [{date, close, credit_buy_qty, credit_buy_amt,
                  credit_buy_rate, credit_sell_qty}]}
        주: output 필드명 = 2026-06-27 노트북+VPS 실측 확정(deal_date/stck_prpr/whol_loan_* 검증).
        """
        try:
            broker = self._get_broker()
            import requests
            from datetime import datetime

            token = broker.access_token
            if token.startswith("Bearer "):
                token = token.replace("Bearer ", "")

            headers = {
                "content-type": "application/json; charset=utf-8",
                "authorization": f"Bearer {token}",
                "appkey": os.getenv("KIS_APP_KEY"),
                "appsecret": os.getenv("KIS_APP_SECRET"),
                "tr_id": "FHPST04760000",
                "custtype": "P",
            }
            params = {
                "FID_COND_MRKT_DIV_CODE": "J",
                "FID_COND_SCR_DIV_CODE": "20476",
                "FID_INPUT_ISCD": code,
                "FID_INPUT_DATE_1": datetime.now().strftime("%Y%m%d"),
            }
            base_url = "https://openapi.koreainvestment.com:9443"
            resp = requests.get(
                f"{base_url}/uapi/domestic-stock/v1/quotations/daily-credit-balance",
                headers=headers, params=params, timeout=10,
            )
            data = resp.json()
            if data.get("rt_cd") != "0":
                return {"success": False, "message": data.get("msg1", "API 오류")}

            out = data.get("output", []) or []
            if isinstance(out, dict):
                out = [out]
            if out and not getattr(self, "_credit_keys_logged", False):
                logger.info(f"[CREDIT] output 첫행 키: {list(out[0].keys())}")
                self._credit_keys_logged = True

            rows = []
            for item in out:
                # 신용 응답은 영업일=deal_date, 종가=stck_prpr (공매도와 키 다름·실측 확정 2026-06-27)
                d = item.get("deal_date", "")
                if not d:
                    continue
                rows.append({
                    "date": d,
                    "close": _safe_int(item.get("stck_prpr", 0)),
                    # 신용융자: 잔고주수/잔고금액/잔고율, 당일 신규/상환, 대주(공매도성) 잔고주수
                    "credit_buy_qty": _safe_int(item.get("whol_loan_rmnd_stcn", 0)),
                    "credit_buy_amt": _safe_int(item.get("whol_loan_rmnd_amt", 0)),
                    "credit_buy_rate": _safe_float(item.get("whol_loan_rmnd_rate", 0)),
                    "credit_new_qty": _safe_int(item.get("whol_loan_new_stcn", 0)),
                    "credit_redeem_qty": _safe_int(item.get("whol_loan_rdmp_stcn", 0)),
                    "loan_sell_qty": _safe_int(item.get("whol_stln_rmnd_stcn", 0)),
                })
            return {"success": True, "data": rows}

        except Exception as e:
            logger.error(f"신용잔고 일별추이 조회 실패 {code}: {e}")
            return {"success": False, "message": str(e)}

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

            # ★8/5 전체검수 [F-96] — 조회 실패를 "성공 + 미체결 0건"으로 반환하던 통로.
            #   같은 파일의 다른 조회 11곳(691·762·832·1175·1311·1424·2168·2365·2434·
            #   2639·2649)은 전부 `rt_cd != "0"`를 검사하므로 설계가 아니라 **누락**이었다.
            #   KIS는 레이트리밋을 HTTP 200 + rt_cd="1"(EGW00201)로 주고 output이 없다.
            #   ★파급: `_wait_for_fill`(:1529)이 첫 폴링에서 pending=[]을 보고 "체결 완료"로
            #     오판 → 지정가 주문이 취소되지 않은 채 거래소에 남는다. 7/20 ticks 사고
            #     ("성공 카운트 ≠ 성공 증거")와 같은 뿌리.
            #   fail-open → fail-closed로 되돌린다(재시도/경보 경로가 살아난다).
            if resp is None:
                return {"success": False, "message": "미체결 조회 응답 없음"}
            _rt = str(resp.get("rt_cd", "0"))
            if _rt != "0":
                return {"success": False,
                        "message": f"미체결 조회 거부 rt_cd={_rt} msg_cd={resp.get('msg_cd','')}"}

            orders = []
            for item in resp.get("output", []):
                orders.append({
                    "order_no": item.get("odno", ""),
                    "org_no": item.get("orgn_odno", ""),  # 원주문번호 (취소 시 사용)
                    "code": item.get("pdno", ""),
                    "name": item.get("prdt_name", ""),
                    "side": "매수" if item.get("sll_buy_dvsn_cd") == "02" else "매도",
                    "qty": _safe_int(item.get("ord_qty", 0)),
                    "price": _safe_int(item.get("ord_unpr", 0)),
                    "filled_qty": _safe_int(item.get("tot_ccld_qty", 0)),
                })

            return {"success": True, "orders": orders}

        except Exception as e:
            logger.error(f"미체결 조회 실패: {e}")
            return {"success": False, "message": f"미체결 조회 실패: {e}"}

    # ═══════════════════════════════════════
    #  위험시간 / 거래량 체크
    # ═══════════════════════════════════════

    def _check_danger_time(self) -> Optional[str]:
        """위험 시간대 체크 - 14:00~14:50은 매수 금지 (알고리즘 장난 구간)"""
        now = datetime.now()
        danger = self.config.get("risk", {}).get("danger_hours", {"start": "14:00", "end": "14:50"})
        start_h, start_m = map(int, danger["start"].split(":"))
        end_h, end_m = map(int, danger["end"].split(":"))
        t = now.hour * 60 + now.minute
        ds = start_h * 60 + start_m
        de = end_h * 60 + end_m
        if ds <= t <= de:
            return f"위험 시간대 ({danger['start']}~{danger['end']}) - 매수 차단"
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

    def buy_market(self, code: str, qty: int, split: int = None,
                   manual: bool = False) -> dict:
        """시장가 매수 - 분할 주문 지원

        Args:
            split: 분할 횟수 (None=config 기본값, 1=원샷)
        """
        if split is None:
            split = self.config.get("risk", {}).get("split_count", 3)
        split = max(1, min(split, 10))

        name = CODE_TO_NAME.get(code, code)

        if split <= 1 or qty <= 1:
            return self._execute_buy(code, qty, name, manual=manual)

        # 분할 주문
        chunk = qty // split
        remainder = qty - chunk * split
        results = []
        total_filled = 0

        for i in range(split):
            q = chunk + (1 if i < remainder else 0)
            if q <= 0:
                continue
            r = self._execute_buy(code, q, name, manual=manual)
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

        try:
            self._log_trade("BUY", code, name, total_filled, split)
        except Exception as e:
            logger.warning(f"매매일지 기록 실패 (BUY {code}): {e}")
        return {"success": success, "message": msg}

    def _execute_buy(self, code: str, qty: int, name: str,
                     manual: bool = False) -> dict:
        """단건 시장가 매수"""
        blocked = self._order_gate("BUY", code, qty, manual=manual, source="market")
        if blocked:
            return blocked
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

    def sell_market(self, code: str, qty: int, split: int = None,
                    manual: bool = False) -> dict:
        """시장가 매도 - 분할 주문 지원"""
        if split is None:
            split = self.config.get("risk", {}).get("split_count", 3)
        split = max(1, min(split, 10))

        name = CODE_TO_NAME.get(code, code)

        if split <= 1 or qty <= 1:
            return self._execute_sell(code, qty, name, manual=manual)

        # 분할 주문
        chunk = qty // split
        remainder = qty - chunk * split
        results = []
        total_filled = 0

        for i in range(split):
            q = chunk + (1 if i < remainder else 0)
            if q <= 0:
                continue
            r = self._execute_sell(code, q, name, manual=manual)
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

        try:
            self._log_trade("SELL", code, name, total_filled, split)
        except Exception as e:
            logger.warning(f"매매일지 기록 실패 (SELL {code}): {e}")
        return {"success": success, "message": msg}

    def _execute_sell(self, code: str, qty: int, name: str,
                      manual: bool = False) -> dict:
        """단건 시장가 매도"""
        blocked = self._order_gate("SELL", code, qty, manual=manual, source="market")
        if blocked:
            return blocked
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

    def _holding_qty_for_reconcile(self, code: str) -> Optional[int]:
        """Read current account holding quantity for fill reconciliation."""
        try:
            bal = self.fetch_balance()
            if not bal or not bal.get("success"):
                return None
            for pos in bal.get("positions", []) or bal.get("holdings", []) or []:
                if str(pos.get("code", "")).zfill(6) == str(code).zfill(6):
                    return int(pos.get("qty", 0) or 0)
            return 0
        except Exception as exc:
            logger.warning("[fill_reconcile] balance read failed for %s: %s", code, exc)
            return None

    def _reconcile_fill(
        self,
        code: str,
        qty_before: Optional[int],
        side: str,
        requested_qty: int,
    ) -> dict:
        """Infer actual filled quantity from account balance delta."""
        if qty_before is None:
            return {"filled_qty": int(requested_qty or 0), "reconciled": False}
        qty_after = self._holding_qty_for_reconcile(code)
        if qty_after is None:
            return {"filled_qty": int(requested_qty or 0), "reconciled": False}
        if str(side or "").upper() == "SELL":
            delta = qty_before - qty_after
        else:
            delta = qty_after - qty_before
        filled_qty = max(0, min(int(requested_qty or 0), int(delta)))
        if filled_qty != int(requested_qty or 0):
            logger.warning(
                "[fill_reconcile] %s %s requested=%s actual=%s before=%s after=%s",
                side,
                code,
                requested_qty,
                filled_qty,
                qty_before,
                qty_after,
            )
        return {"filled_qty": filled_qty, "reconciled": True}

    def _actual_fill_qty(
        self,
        code: str,
        qty_before: Optional[int],
        side: str,
        requested_qty: int,
    ) -> int:
        reconciled = self._reconcile_fill(code, qty_before, side, requested_qty)
        actual_qty = int(reconciled.get("filled_qty", 0) or 0)
        if actual_qty <= 0 and not reconciled.get("reconciled"):
            return int(requested_qty or 0)
        return actual_qty

    def smart_buy(self, code: str, qty: int, max_wait_sec: int = 90,
                  manual: bool = False) -> dict:
        """스마트 지정가 매수 - 3단계 에스컬레이션

        1단계: 현재가 -0.5% 지정가 → 30초 대기
        2단계: 미체결 시 현재가 -0.2% 수정 → 30초 대기
        3단계: 미체결 시 현재가로 수정 (사실상 시장가)

        시장가 대비 평균 0.2~0.5% 절약 효과
        """
        name = CODE_TO_NAME.get(code, code)
        blocked = self._order_gate("BUY", code, qty, manual=manual, source="smart_buy")
        if blocked:
            return blocked
        qty_before = self._holding_qty_for_reconcile(code)

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
                return self.buy_market(code, qty, split=1, manual=manual)

            order_no = resp.get("output", {}).get("ODNO", "")
            org_no = resp.get("output", {}).get("KRX_FWDG_ORD_ORGNO", "")

        except Exception as e:
            logger.error(f"스마트매수 주문 실패: {e} → 시장가 폴백")
            return self.buy_market(code, qty, split=1, manual=manual)

        # ── 체결 대기 (1단계: 30초) ──
        wait_per_step = max_wait_sec // 3
        filled = self._wait_for_fill(order_no, wait_per_step, org_no=org_no, qty=qty)

        if filled:
            actual_qty = self._actual_fill_qty(code, qty_before, "BUY", qty)
            self._log_trade("SMART_BUY", code, name, actual_qty, 1)
            return {
                "success": True, "order_no": order_no,
                "message": f"스마트매수 {name}({code}) {qty}주 @ {price_1:,}원 (-0.5%)",
                "filled_qty": actual_qty,
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

        filled = self._wait_for_fill(order_no, wait_per_step, org_no=org_no, qty=qty)

        if filled:
            actual_qty = self._actual_fill_qty(code, qty_before, "BUY", qty)
            self._log_trade("SMART_BUY", code, name, actual_qty, 2)
            return {
                "success": True, "order_no": order_no,
                "message": f"스마트매수 {name}({code}) {qty}주 @ {price_2:,}원 (-0.2%)",
                "filled_qty": actual_qty,
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
            self.cancel_order(order_no, org_no=org_no, qty=qty)
            return self.buy_market(code, qty, split=1, manual=manual)

        filled = self._wait_for_fill(order_no, wait_per_step, org_no=org_no, qty=qty)

        if filled:
            actual_qty = self._actual_fill_qty(code, qty_before, "BUY", qty)
            self._log_trade("SMART_BUY", code, name, actual_qty, 3)
            return {
                "success": True, "order_no": order_no,
                "message": f"스마트매수 {name}({code}) {qty}주 @ {price_3:,}원 (3단계)",
                "filled_qty": actual_qty,
                "saved_pct": 0,
            }

        # 최종 실패 → 취소
        self.cancel_order(order_no, org_no=org_no, qty=qty)
        self._log_trade("SMART_BUY_FAIL", code, name, 0, 3)
        return {"success": False, "message": f"스마트매수 실패 - {max_wait_sec}초 내 미체결 ({name})"}

    def chase_buy(self, code: str, qty: int, max_wait_sec: int = 180,
                  initial_discount_pct: float = -0.3,
                  cycle_sec: int = 5,
                  manual: bool = False) -> dict:
        """지정가 추격 매수 — 시장가 폴백 X (사장님 5/21 23:00 명령).

        smart_buy 진화 버전:
          - smart_buy = 3단계 고정 (-0.5%/-0.2%/현재가+0.2%) / 시장가 폴백
          - chase_buy = N회 동적 추격 (호가 따라 올림) / 시장가 폴백 X

        알고리즘:
          1. 초기 주문 = 현재가 -0.3% 지정가 (할인)
          2. 5초 사이클: 체결 체크 + 가격 갱신
          3. 현재가가 우리 주문가보다 위면 → modify_order로 따라잡기 (+1 tick)
          4. 시간 초과 → 취소 (시장가 X)

        Args:
            code: 종목코드
            qty: 수량
            max_wait_sec: 최대 대기 (기본 180초 = 3분)
            initial_discount_pct: 초기 할인율 (기본 -0.3%)
            cycle_sec: 추격 사이클 (기본 5초)

        Returns:
            {success, order_no, message, chase_count, final_price}
        """
        name = CODE_TO_NAME.get(code, code)
        blocked = self._order_gate("BUY", code, qty, manual=manual, source="chase_buy")
        if blocked:
            return blocked
        qty_before = self._holding_qty_for_reconcile(code)

        # 초기 가격 조회
        price_info = self.fetch_price(code)
        if not price_info.get("success"):
            return {"success": False, "message": "현재가 조회 실패"}

        current = price_info["current_price"]
        tick = self._tick_size(current)
        # 초기 주문가 = 현재가 + 할인 (음수면 -0.3%)
        ratio = 1.0 + initial_discount_pct / 100
        our_price = self._round_to_tick(current * ratio, tick,
                                          "down" if initial_discount_pct < 0 else "up")

        # 1단계: 초기 주문
        broker = self._get_broker()
        try:
            resp = broker.create_limit_buy_order(symbol=code, price=our_price, quantity=qty)
        except Exception as e:
            return {"success": False, "message": f"주문 예외: {e}"}

        if not resp or resp.get("rt_cd") != "0":
            msg = resp.get("msg1", "?") if resp else "응답 없음"
            return {"success": False, "message": f"주문 실패: {msg}"}

        order_no = resp.get("output", {}).get("ODNO", "")
        org_no = resp.get("output", {}).get("KRX_FWDG_ORD_ORGNO", "")

        logger.info(
            f"[chase_buy] {name}({code}) 시작 @ {our_price:,} "
            f"(현재가 {current:,}, 할인 {initial_discount_pct:+.2f}%)"
        )

        # 2단계: 추격 사이클
        start_time = time.time()
        chase_count = 0
        last_logged_price = our_price

        while time.time() - start_time < max_wait_sec:
            time.sleep(cycle_sec)

            # 체결 체크 (짧은 timeout = 1초)
            filled = self._wait_for_fill(order_no, 1, org_no=org_no, qty=qty)
            if filled:
                actual_qty = self._actual_fill_qty(code, qty_before, "BUY", qty)
                self._log_trade("CHASE_BUY", code, name, actual_qty, chase_count + 1)
                logger.info(
                    f"[chase_buy] ✅ {name}({code}) 체결 @ {our_price:,} "
                    f"({chase_count}회 추격, {int(time.time()-start_time)}초)"
                )
                return {
                    "success": True, "order_no": order_no,
                    "message": f"chase 매수 {name}({code}) {qty}주 @ {our_price:,}원 ({chase_count}회 추격)",
                    "filled_qty": actual_qty,
                    "chase_count": chase_count,
                    "final_price": our_price,
                }

            # 현재가 재조회
            try:
                new_info = self.fetch_price(code)
                if not new_info.get("success"):
                    continue
                new_current = new_info["current_price"]
            except Exception:
                continue

            # 가격 추격 — 현재가가 우리 주문가보다 높으면 +1 tick으로 따라잡기
            if new_current > our_price:
                new_tick = self._tick_size(new_current)
                new_our_price = self._round_to_tick(new_current + new_tick, new_tick, "up")
                # 너무 자주 모디파이 안 함 (1 tick 차이만 나면 스킵)
                if new_our_price > our_price:
                    try:
                        broker.modify_order(
                            org_no=org_no, order_no=order_no,
                            order_type="00", price=new_our_price, quantity=qty, total=True,
                        )
                        chase_count += 1
                        if abs(new_our_price - last_logged_price) >= new_tick * 5:
                            # 5 tick 이상 차이 날 때만 로그 (스팸 방지)
                            logger.info(
                                f"[chase_buy] {name} #{chase_count} 추격: "
                                f"{our_price:,} → {new_our_price:,} (현재 {new_current:,})"
                            )
                            last_logged_price = new_our_price
                        our_price = new_our_price
                    except Exception as me:
                        logger.warning(f"[chase_buy] modify 실패: {me}")

        # 3단계: 시간 초과 → 취소 (시장가 폴백 X — 사장님 명령)
        self.cancel_order(order_no, org_no=org_no, qty=qty)
        self._log_trade("CHASE_BUY_FAIL", code, name, 0, chase_count)
        logger.warning(
            f"[chase_buy] ❌ {name}({code}) 시간 초과 — "
            f"{max_wait_sec}초 / {chase_count}회 추격 / 시장가 폴백 X (사장님 명령)"
        )
        return {
            "success": False,
            "message": f"chase 매수 시간 초과 ({max_wait_sec}초, {chase_count}회 추격, 시장가 폴백 X)",
            "chase_count": chase_count,
        }

    def smart_sell(self, code: str, qty: int, max_wait_sec: int = 60,
                   manual: bool = False) -> dict:
        """스마트 지정가 매도 - 3단계 에스컬레이션

        1단계: 현재가 +0.5% 지정가 → 20초 대기
        2단계: 미체결 시 현재가 +0.2% 수정 → 20초 대기
        3단계: 미체결 시 현재가로 수정 (사실상 시장가)

        긴급 매도(SL)에는 사용하지 않음 - 시장가 직행
        """
        name = CODE_TO_NAME.get(code, code)
        blocked = self._order_gate("SELL", code, qty, manual=manual, source="smart_sell")
        if blocked:
            return blocked
        qty_before = self._holding_qty_for_reconcile(code)

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
                return self.sell_market(code, qty, split=1, manual=manual)

            order_no = resp.get("output", {}).get("ODNO", "")
            org_no = resp.get("output", {}).get("KRX_FWDG_ORD_ORGNO", "")

        except Exception as e:
            logger.error(f"스마트매도 주문 실패: {e} → 시장가 폴백")
            return self.sell_market(code, qty, split=1, manual=manual)

        # ── 체결 대기 ──
        wait_per_step = max_wait_sec // 3
        filled = self._wait_for_fill(order_no, wait_per_step, org_no=org_no, qty=qty)

        if filled:
            actual_qty = self._actual_fill_qty(code, qty_before, "SELL", qty)
            self._log_trade("SMART_SELL", code, name, actual_qty, 1)
            return {
                "success": True, "order_no": order_no,
                "filled_qty": actual_qty,
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

        filled = self._wait_for_fill(order_no, wait_per_step, org_no=org_no, qty=qty)

        if filled:
            actual_qty = self._actual_fill_qty(code, qty_before, "SELL", qty)
            self._log_trade("SMART_SELL", code, name, actual_qty, 2)
            return {
                "success": True, "order_no": order_no,
                "filled_qty": actual_qty,
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
            self.cancel_order(order_no, org_no=org_no, qty=qty)
            return self.sell_market(code, qty, split=1, manual=manual)

        filled = self._wait_for_fill(order_no, wait_per_step, org_no=org_no, qty=qty)

        if filled:
            actual_qty = self._actual_fill_qty(code, qty_before, "SELL", qty)
            self._log_trade("SMART_SELL", code, name, actual_qty, 3)
            return {
                "success": True, "order_no": order_no,
                "filled_qty": actual_qty,
                "message": f"스마트매도 {name}({code}) {qty}주 @ {price_3:,}원 (3단계)",
                "saved_pct": 0,
            }

        # 최종 → 시장가 폴백
        self.cancel_order(order_no, org_no=org_no, qty=qty)
        logger.warning(f"스마트매도 실패 → 시장가 매도 폴백")
        return self.sell_market(code, qty, split=1, manual=manual)

    def _wait_for_fill(self, order_no: str, wait_sec: int,
                       org_no: str = "", qty: int = 0) -> bool:
        """주문 체결 대기 (polling)

        부분 체결 시에도 잔량이 0이 되면 True 반환.
        타임아웃 시 부분 체결된 수량이 있으면 잔량 취소 후 True 반환.

        Args:
            order_no: 주문번호 (ODNO)
            wait_sec: 최대 대기 시간 (초)
            org_no: KRX 전송 주문 기관번호 (취소 시 필요)
            qty: 원주문 수량 (취소 시 잔량 계산용)
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

        # 타임아웃 - 부분 체결 확인
        try:
            orders = self.fetch_open_orders()
            if orders.get("success"):
                pending = [o for o in orders["orders"] if o["order_no"] == order_no]
                if pending and pending[0]["filled_qty"] > 0:
                    # 부분 체결 있음 → 잔량 취소, 체결분만 인정
                    filled = pending[0]["filled_qty"]
                    remain = pending[0]["qty"] - filled
                    logger.info(f"주문 {order_no} 부분 체결 {filled}주 → 잔량 {remain}주 취소")
                    self.cancel_order(order_no, org_no=org_no, qty=remain)
                    return True  # 부분이라도 체결됐으면 성공 처리
        except Exception:
            pass

        logger.info(f"주문 {order_no} 미체결 ({wait_sec}초 초과)")
        return False

    def check_spread(self, code: str) -> dict:
        """호가 스프레드 + 호가잔량 비율 체크

        Returns: {ok, spread_pct, ask, bid, bid_ask_ratio, message}
        """
        try:
            broker = self._get_broker()
            token = broker.access_token
            if not token.startswith("Bearer "):
                token = f"Bearer {token}"
            headers = {
                "content-type": "application/json; charset=utf-8",
                "authorization": token,
                "appkey": broker.api_key,
                "appsecret": broker.api_secret,
                "tr_id": "FHKST01010200",
            }
            params = {"fid_cond_mrkt_div_code": "J", "fid_input_iscd": code}
            import requests
            resp = requests.get(
                f"{broker.base_url}/uapi/domestic-stock/v1/quotations/inquire-asking-price-exp-ccn",
                headers=headers, params=params, timeout=5,
            )
            d = resp.json().get("output1", {})
            ask1 = _safe_int(d.get("askp1", 0))
            bid1 = _safe_int(d.get("bidp1", 0))

            # 호가잔량 합계 → bid_ask_ratio (매수우위 비율)
            total_ask = _safe_int(d.get("total_askp_rsqn", 0))
            total_bid = _safe_int(d.get("total_bidp_rsqn", 0))
            bid_ask_ratio = round(total_bid / total_ask * 100, 1) if total_ask > 0 else 0

            if ask1 <= 0 or bid1 <= 0:
                return {"ok": True, "spread_pct": 0, "bid_ask_ratio": bid_ask_ratio,
                        "message": "호가 조회 실패 - 통과"}

            mid = (ask1 + bid1) / 2
            spread_pct = (ask1 - bid1) / mid * 100

            max_spread = self.risk.get("max_spread_pct", 0.5)
            if spread_pct > max_spread:
                return {
                    "ok": False, "spread_pct": round(spread_pct, 2),
                    "ask": ask1, "bid": bid1, "bid_ask_ratio": bid_ask_ratio,
                    "message": f"스프레드 {spread_pct:.2f}% > {max_spread}% - 주문 보류",
                }
            return {
                "ok": True, "spread_pct": round(spread_pct, 2),
                "ask": ask1, "bid": bid1, "bid_ask_ratio": bid_ask_ratio,
                "message": f"스프레드 {spread_pct:.2f}% OK",
            }
        except Exception as e:
            logger.warning(f"스프레드 체크 실패 {code}: {e}")
            return {"ok": True, "spread_pct": 0, "bid_ask_ratio": 0,
                    "message": f"스프레드 체크 실패: {e} - 통과"}

    def fetch_expected_price(self, code: str) -> dict:
        """동시호가 예상체결가 조회 (08:30~09:00, 15:20~15:30)

        check_spread와 동일 API (inquire-asking-price-exp-ccn)의 output2 사용.

        Returns: {success, expected_price, expected_volume, change_rate,
                  prev_close, total_ask_vol, total_bid_vol, bid_ask_ratio}
        """
        try:
            broker = self._get_broker()
            token = broker.access_token
            if not token.startswith("Bearer "):
                token = f"Bearer {token}"
            headers = {
                "content-type": "application/json; charset=utf-8",
                "authorization": token,
                "appkey": broker.api_key,
                "appsecret": broker.api_secret,
                "tr_id": "FHKST01010200",
            }
            params = {"fid_cond_mrkt_div_code": "J", "fid_input_iscd": code}
            import requests
            resp = requests.get(
                f"{broker.base_url}/uapi/domestic-stock/v1/quotations/inquire-asking-price-exp-ccn",
                headers=headers, params=params, timeout=5,
            )
            data = resp.json()
            o1 = data.get("output1", {})
            o2 = data.get("output2", {})

            # output2: 예상체결 데이터
            exp_price = _safe_int(o2.get("antc_cnpr", 0))
            exp_vol = _safe_int(o2.get("antc_vol", 0))
            exp_chg_rate = _safe_float(o2.get("antc_cntg_prdy_ctrt", 0))
            exp_vs = _safe_int(o2.get("antc_cntg_vrss", 0))  # 전일대비

            # output1: 호가잔량 합계
            total_ask = _safe_int(o1.get("total_askp_rsqn", 0))
            total_bid = _safe_int(o1.get("total_bidp_rsqn", 0))
            bid_ask_ratio = (total_bid / total_ask * 100) if total_ask > 0 else 0

            if exp_price <= 0:
                # 동시호가 시간 외이면 현재가로 대체
                cp = _safe_int(o2.get("stck_prpr", 0))
                if cp > 0:
                    return {
                        "success": True, "expected_price": cp,
                        "expected_volume": 0, "change_rate": 0,
                        "prev_close": cp, "is_auction": False,
                        "total_ask_vol": total_ask, "total_bid_vol": total_bid,
                        "bid_ask_ratio": round(bid_ask_ratio, 1),
                    }
                return {"success": False, "message": "예상체결가 없음 (장외시간)"}

            prev_close = exp_price - exp_vs if exp_vs != 0 else exp_price
            return {
                "success": True,
                "expected_price": exp_price,
                "expected_volume": exp_vol,
                "change_rate": round(exp_chg_rate, 2),
                "prev_close": prev_close,
                "is_auction": True,
                "total_ask_vol": total_ask,
                "total_bid_vol": total_bid,
                "bid_ask_ratio": round(bid_ask_ratio, 1),
            }
        except Exception as e:
            logger.warning(f"예상체결가 조회 실패 {code}: {e}")
            return {"success": False, "message": str(e)}

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

        tmp_file = journal_file.with_suffix(".tmp")
        with open(tmp_file, "w", encoding="utf-8") as f:
            json.dump(entries, f, ensure_ascii=False, indent=2)
        tmp_file.replace(journal_file)

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

    def cancel_order(self, order_no: str, org_no: str = "", qty: int = 0) -> dict:
        """주문 취소

        Args:
            order_no: 주문번호 (ODNO)
            org_no: 한국거래소 원주문번호 (KRX_FWDG_ORD_ORGNO), 없으면 빈 문자열
            qty: 취소 수량 (0이면 잔량 전부 취소)
        """
        try:
            broker = self._get_broker()
            resp = broker.cancel_order(
                org_no=org_no,
                order_no=order_no,
                quantity=qty,
                total=True,  # 잔량 전부 취소
            )
            logger.info(f"주문 취소: {order_no} (org={org_no}) → {resp}")

            if resp and resp.get("rt_cd") == "0":
                return {"success": True, "message": f"주문 취소 완료: {order_no}"}
            else:
                msg = resp.get("msg1", "알 수 없는 오류") if resp else "응답 없음"
                return {"success": False, "message": f"주문 취소 실패: {msg}"}

        except Exception as e:
            logger.error(f"취소 실패 {order_no}: {e}")
            return {"success": False, "message": f"주문 취소 실패: {e}"}

    # ═══════════════════════════════════════
    #  시간외 주문 (NXT 대체거래소 + KRX 시간외 단일가)
    # ═══════════════════════════════════════
    # NXT 대체거래소 등록 종목 → TTTC0012U/0011U + EXCG_ID_DVSN_CD="NXT" (20:00까지)
    # NXT 미등록 소형주 → TTTC0803U/0801U KRX 시간외 단일가 (18:00까지)
    # 전략: NXT 먼저 시도 → 실패 시 KRX 시간외 폴백

    def _afterhours_order(self, code: str, qty: int, price: int,
                          side: str, exchange: str,
                          manual: bool = False) -> dict:
        """시간외 주문 공통 로직.

        Args:
            side: "buy" or "sell"
            exchange: "NXT" or "KRX"
        """
        name = CODE_TO_NAME.get(code, code)
        blocked = self._order_gate(
            side.upper(), code, qty, manual=manual, source=f"afterhours:{exchange}"
        )
        if blocked:
            return blocked
        broker = self._get_broker()
        path = "/uapi/domestic-stock/v1/trading/order-cash"
        acc_clean = broker.acc_no.replace("-", "")

        body = {
            "CANO": acc_clean[:8],
            "ACNT_PRDT_CD": acc_clean[8:],
            "PDNO": code,
            "ORD_DVSN": "00",       # 지정가
            "ORD_QTY": str(qty),
            "ORD_UNPR": str(price),
        }

        if exchange == "NXT":
            # NXT 대체거래소 애프터마켓 (15:30~20:00)
            tr_id = "TTTC0012U" if side == "buy" else "TTTC0011U"
            body["EXCG_ID_DVSN_CD"] = "NXT"
            label = "NXT"
        else:
            # KRX 시간외 단일가 (16:00~18:00)
            tr_id = "TTTC0803U" if side == "buy" else "TTTC0801U"
            label = "KRX시간외"

        _token = broker.access_token
        if not _token.startswith("Bearer "):
            _token = f"Bearer {_token}"
        headers = {
            "Content-Type": "application/json; charset=utf-8",
            "authorization": _token,
            "appkey": broker.api_key,
            "appsecret": broker.api_secret,
            "tr_id": tr_id,
            "custtype": "P",
        }

        import requests
        url = f"{broker.base_url}{path}"
        resp = requests.post(url, json=body, headers=headers, timeout=10)
        data = resp.json()

        side_kr = "매수" if side == "buy" else "매도"
        if data.get("rt_cd") == "0":
            order_no = data.get("output", {}).get("ODNO", "?")
            trade_tag = f"NXT_{side.upper()}"
            self._log_trade(trade_tag, code, name, qty, 1)
            msg = f"{label} {side_kr} 성공: {name}({code}) {qty}주 @{price:,}원 #{order_no}"
            logger.info(msg)
            return {"success": True, "order_no": order_no,
                    "exchange": exchange, "message": msg}
        else:
            err = data.get("msg1", "알 수 없는 오류")
            logger.warning(f"{label} {side_kr} 실패: {name}({code}) → {err}")
            return {"success": False, "exchange": exchange,
                    "message": f"{label} {side_kr} 실패: {err}"}

    def afterhours_buy(self, code: str, qty: int, price: int = 0,
                       exchange: str = "auto", manual: bool = False) -> dict:
        """시간외 매수 (NXT 대체거래소 우선 + KRX 폴백)

        exchange: "auto" → NXT 시도 후 실패 시 KRX 시간외
                  "NXT"  → NXT 대체거래소만
                  "KRX"  → KRX 시간외 단일가만
        """
        name = CODE_TO_NAME.get(code, code)
        try:
            if price <= 0:
                pi = self.fetch_price(code)
                if not pi.get("success"):
                    return {"success": False, "message": "현재가 조회 실패"}
                price = pi["current_price"]

            if exchange == "auto":
                # 1차: NXT 대체거래소 시도
                result = self._afterhours_order(
                    code, qty, price, "buy", "NXT", manual=manual
                )
                if result["success"]:
                    return result
                logger.info(f"NXT 매수 실패 → KRX 시간외 폴백: {name}({code})")
                # 2차: KRX 시간외 단일가 폴백
                return self._afterhours_order(
                    code, qty, price, "buy", "KRX", manual=manual
                )
            else:
                return self._afterhours_order(
                    code, qty, price, "buy", exchange, manual=manual
                )

        except Exception as e:
            logger.error(f"시간외 매수 예외 {code}: {e}")
            return {"success": False, "message": f"시간외 매수 예외: {e}"}

    def afterhours_sell(self, code: str, qty: int, price: int = 0,
                        exchange: str = "auto", manual: bool = False) -> dict:
        """시간외 매도 (NXT 대체거래소 우선 + KRX 폴백)

        exchange: "auto" → NXT 시도 후 실패 시 KRX 시간외
                  "NXT"  → NXT 대체거래소만
                  "KRX"  → KRX 시간외 단일가만
        """
        name = CODE_TO_NAME.get(code, code)
        try:
            if price <= 0:
                pi = self.fetch_price(code)
                if not pi.get("success"):
                    return {"success": False, "message": "현재가 조회 실패"}
                price = pi["current_price"]

            if exchange == "auto":
                result = self._afterhours_order(
                    code, qty, price, "sell", "NXT", manual=manual
                )
                if result["success"]:
                    return result
                logger.info(f"NXT 매도 실패 → KRX 시간외 폴백: {name}({code})")
                return self._afterhours_order(
                    code, qty, price, "sell", "KRX", manual=manual
                )
            else:
                return self._afterhours_order(
                    code, qty, price, "sell", exchange, manual=manual
                )

        except Exception as e:
            logger.error(f"시간외 매도 예외 {code}: {e}")
            return {"success": False, "message": f"시간외 매도 예외: {e}"}

    def _is_one_share_mode(self) -> bool:
        """5/19~5/20 D-Day 1주 모드 (사장님 5/19 보수 검증)

        config.yaml `bot.one_share_mode_until` (YYYY-MM-DD)까지 활성.
        - 적용: safe_buy / nxt_safe_buy / afterhours_buy 호출부
        - 비적용: 청산/SL/TP/추매 외 매도 경로 일체
        - 만료(다음날 00:00) 시 자동 False → 풀 회전 복원
        """
        try:
            until_str = self.config.get("bot", {}).get("one_share_mode_until", "")
            if not until_str:
                return False
            from datetime import date
            until = date.fromisoformat(until_str)
            return date.today() <= until
        except Exception as e:
            logger.warning(f"_is_one_share_mode 파싱 실패 (비활성 처리): {e}")
            return False

    def nxt_safe_buy(self, code: str, amount: int) -> dict:
        """NXT 금액 기반 시간외 매수 (잔고 확인 포함)"""
        name = CODE_TO_NAME.get(code, code)
        blocked = self._order_gate("BUY", code, 0, source="nxt_safe_buy")
        if blocked:
            return blocked

        pi = self.fetch_price(code)
        if not pi.get("success"):
            return {"success": False, "message": f"현재가 조회 실패"}

        price = pi["current_price"]
        if price <= 0:
            return {"success": False, "message": "현재가 0원 — 매수 불가"}

        # 5/19~5/20 D-Day 1주 모드 — amount 사전 축소 (현금 부족 차단 방지)
        one_share = self._is_one_share_mode()
        if one_share and price > 0:
            _orig = amount
            amount = price + 5000  # 1주 + 수수료/슬리피지 버퍼
            logger.info(f"[1주모드/NXT] {name}({code}) amount {_orig:,}원 → {amount:,}원 (1주 가격)")

        bal = self.fetch_balance()
        if not bal.get("success"):
            return {"success": False, "message": f"잔고 조회 실패"}

        cash = bal["cash"]
        if cash < amount:
            return {"success": False, "message": f"현금 부족: {cash:,}원 < {amount:,}원"}

        qty = amount // price
        if one_share:
            qty = 1
            logger.info(f"[1주모드/NXT] {name}({code}) qty 강제=1")
        if qty <= 0:
            return {"success": False, "message": f"매수 가능 수량 없음 (@{price:,}원)"}

        return self.afterhours_buy(code, qty, price)

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
        blocked = self._order_gate("BUY", code, 0, source="safe_buy")
        if blocked:
            return blocked

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

        # 5/19~5/20 D-Day 1주 모드 — 가격 사전 조회 + amount 축소
        # (사장님 5/19 07:50 결정: 자비스 v3.0 첫 라이브 보수 검증, 5/21 자동 만료)
        one_share = self._is_one_share_mode()
        _pi_cached = None
        name = CODE_TO_NAME.get(code, code)
        if one_share:
            _pi_cached = self.fetch_price(code)
            _cp = _pi_cached.get("current_price", 0) if _pi_cached.get("success") else 0
            if _cp > 0:
                _orig_amount = amount
                amount = _cp + 5000  # 1주 + 수수료/슬리피지 버퍼
                logger.info(
                    f"[1주모드] {name}({code}) amount {_orig_amount:,}원 → {amount:,}원 (1주 @{_cp:,}원)"
                )

        # 3. 포지션 수 체크
        max_positions = self.risk.get("max_positions", 5)
        max_ratio = self.risk.get("max_position_ratio", 0.30)
        min_cash = SAJANG.CASH_RESERVE_PCT if SAJANG is not None else 0.30

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

        price_info = _pi_cached if _pi_cached else self.fetch_price(code)
        if not price_info.get("success"):
            return {"success": False, "message": f"현재가 조회 실패: {price_info.get('message')}"}

        current_price = price_info["current_price"]
        if current_price <= 0:
            return {"success": False, "message": "현재가가 0원입니다"}

        qty = buy_amount // current_price
        if one_share:
            qty = 1
            logger.info(f"[1주모드] {name}({code}) qty 강제=1")
        if qty <= 0:
            return {"success": False, "message": f"매수 가능 수량 없음 (현재가: {current_price:,}원, 금액: {buy_amount:,}원)"}

        # 6. 거래량 대비 비중 체크 (1주 모드는 skip — 1주는 거래량 영향 미미)
        if not one_share:
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
        est_cost = qty * current_price

        logger.info(f"안전 매수: {name}({code}) {qty}주 @ {current_price:,}원 ~{est_cost:,}원")

        # 7. 스마트 지정가 매수 (시장가 대비 0.2~0.5% 절약)
        return self.smart_buy(code, qty)

    def liquidate_one(self, code: str, urgent: bool = False,
                      manual: bool = False) -> dict:
        """특정 종목 전량 청산.

        ★ 5/21 09:50 사장님 박사 명령: 호가창 보고 지정가 매매 ★
        기본: smart_sell (지정가 +0.5% → +0.2% → 시장가 폴백, 60초)
        urgent=True: 긴급 SL 시 시장가 직행 (사고/패닉 시만)

        사유: 5/21 09:35 뷰티스킨 시장가 매도 슬리피지 -6.2% 사고 (-227원/주)
              → smart_sell 사용했어야 함 → 박사 학습 → 영구 fix
        """
        bal = self.fetch_balance()
        if not bal or not bal.get("success"):
            return {"success": False, "message": "잔고 조회 실패"}

        for pos in bal["positions"]:
            if pos["code"] == code:
                # 박사 자율: 긴급 SL은 시장가 / 일반 익절/리벨런싱은 smart_sell
                if urgent:
                    return self.sell_market(code, pos["qty"], manual=manual)
                return self.smart_sell(code, pos["qty"], manual=manual)

        name = CODE_TO_NAME.get(code, code)
        return {"success": False, "message": f"{name}({code}) 보유 없음"}

    # ═══════════════════════════════════════
    #  국내종목 순위분석 API (5개)
    # ═══════════════════════════════════════

    def _ranking_get(self, path: str, tr_id: str, params: dict) -> list:
        """순위 API 공통 GET 호출 → output 리스트 반환."""
        import requests
        broker = self._get_broker()
        token = broker.access_token
        if not token.startswith("Bearer "):
            token = f"Bearer {token}"
        headers = {
            "content-type": "application/json; charset=utf-8",
            "authorization": token,
            "appkey": broker.api_key,
            "appsecret": broker.api_secret,
            "tr_id": tr_id,
            "custtype": "P",
        }
        url = f"{broker.base_url}{path}"
        resp = requests.get(url, headers=headers, params=params, timeout=10)
        data = resp.json()
        if data.get("rt_cd") != "0":
            logger.warning(f"[RANK] {tr_id} 실패: {data.get('msg1', '')}")
            return []
        return data.get("output", [])

    def fetch_ranking_fluctuation(self, market: str = "J", top_n: int = 30,
                                   min_vol: int = 100000) -> list:
        """등락률 순위 (FHPST01700000) — 급등/급락 종목.

        Returns: [{"code","name","price","change_rate","volume"}, ...]
        """
        params = {
            "fid_cond_mrkt_div_code": market,
            "fid_cond_scr_div_code": "20170",
            "fid_input_iscd": "0000",
            "fid_rank_sort_cls_code": "0",
            "fid_input_cnt_1": str(top_n),
            "fid_prc_cls_code": "0",
            "fid_input_price_1": "0",
            "fid_input_price_2": "1000000",
            "fid_vol_cnt": str(min_vol),
            "fid_trgt_cls_code": "0",
            "fid_trgt_exls_cls_code": "0000000000",
            "fid_div_cls_code": "0",
            "fid_rsfl_rate1": "0",
            "fid_rsfl_rate2": "30",
        }
        raw = self._ranking_get(
            "/uapi/domestic-stock/v1/ranking/fluctuation",
            "FHPST01700000", params,
        )
        results = []
        for r in raw[:top_n]:
            results.append({
                "code": r.get("stck_shrn_iscd", ""),
                "name": r.get("hts_kor_isnm", ""),
                "price": _safe_int(r.get("stck_prpr")),
                "change_rate": _safe_float(r.get("prdy_ctrt")),
                "change_amt": _safe_int(r.get("prdy_vrss")),
                "volume": _safe_int(r.get("acml_vol")),
                "high": _safe_int(r.get("stck_hgpr")),
            })
        return results

    def fetch_ranking_volume(self, market: str = "J", top_n: int = 30,
                              sort_by: str = "3") -> list:
        """거래량순위 (FHPST01710000) — 거래 폭발 종목.

        sort_by: "0"=평균거래량, "1"=거래증가율, "3"=거래금액순
        Returns: [{"code","name","price","change_rate","volume","vol_rate"}, ...]
        """
        params = {
            "FID_COND_MRKT_DIV_CODE": market,
            "FID_COND_SCR_DIV_CODE": "20171",
            "FID_INPUT_ISCD": "0000",
            "FID_DIV_CLS_CODE": "0",
            "FID_BLNG_CLS_CODE": sort_by,
            "FID_TRGT_CLS_CODE": "0",
            "FID_TRGT_EXLS_CLS_CODE": "0000000000",
            "FID_INPUT_PRICE_1": "",
            "FID_INPUT_PRICE_2": "",
            "FID_VOL_CNT": "",
            "FID_INPUT_DATE_1": "",
        }
        raw = self._ranking_get(
            "/uapi/domestic-stock/v1/quotations/volume-rank",
            "FHPST01710000", params,
        )
        results = []
        for r in raw[:top_n]:
            results.append({
                "code": r.get("mksc_shrn_iscd", ""),
                "name": r.get("hts_kor_isnm", ""),
                "price": _safe_int(r.get("stck_prpr")),
                "change_rate": _safe_float(r.get("prdy_ctrt")),
                "volume": _safe_int(r.get("acml_vol")),
                "vol_rate": _safe_float(r.get("vol_inrt")),
                "tr_amount": _safe_int(r.get("acml_tr_pbmn")),
            })
        return results

    def fetch_ranking_strength(self, market: str = "J", top_n: int = 30,
                                min_volume: int = 100000,
                                exclude_etf: bool = True) -> list:
        """체결강도 상위 (FHPST01680000) — 매수세 강한 종목.

        Args:
            market: J=주식
            top_n: 반환 종목 수
            min_volume: 최소 누적거래량 (기본 10만주) — 5/19 발견 결함 수정.
                        빈값으로 두면 ETN/채권 등 거래량 200주짜리가 체결강도 1000.0
                        동률로 상위 점유 → 의미없는 결과. min_volume=0 으로
                        하면 무필터(이전 동작).
            exclude_etf: ETF/ETN/펀드/채권 prefix 클라이언트 측 제외 (기본 True).
                         단타 대상 외 — KODEX/TIGER/RISE/ACE/PLUS/SOL/KOSEF/ARIRANG/
                         HANARO/KIWOOM/KoAct/BNK/Q/0(채권코드).

        Returns: [{"code","name","price","change_rate","strength","volume"}, ...]
        """
        # KIS API는 결과량을 미리 알 수 없으므로 충분히 크게 받고 클라 필터 후 잘라냄
        fetch_n = top_n * 5 if exclude_etf else top_n
        params = {
            "fid_cond_mrkt_div_code": market,
            "fid_cond_scr_div_code": "20168",
            "fid_input_iscd": "0000",
            "fid_div_cls_code": "0",
            "fid_input_price_1": "",
            "fid_input_price_2": "",
            "fid_vol_cnt": str(min_volume) if min_volume else "",
            "fid_trgt_cls_code": "0",
            "fid_trgt_exls_cls_code": "0",
        }
        raw = self._ranking_get(
            "/uapi/domestic-stock/v1/ranking/volume-power",
            "FHPST01680000", params,
        )

        # ETF/ETN/펀드/채권 prefix 차단 (5/19 발견 결함)
        ETF_PREFIXES = (
            "KODEX", "TIGER", "RISE", "PLUS", "ACE", "SOL", "KOSEF",
            "ARIRANG", "HANARO", "KIWOOM", "KoAct", "BNK", "TIMEFOLIO",
            "TIME", "FOCUS", "WOORI", "KB", "TREX",
        )
        # 채권/ETN 코드 패턴: Q로 시작(ETN) 또는 0으로 시작 + 알파벳 포함 (펀드)
        def _is_excluded(code: str, name: str) -> bool:
            if not exclude_etf:
                return False
            if any(name.startswith(p) for p in ETF_PREFIXES):
                return True
            # 6자리 종목코드가 아니면 (ETN/펀드/채권) 제외
            if not (len(code) == 6 and code.isdigit()):
                return True
            return False

        results = []
        for r in raw:
            code = r.get("stck_shrn_iscd", "")
            name = r.get("hts_kor_isnm", "")
            if _is_excluded(code, name):
                continue
            results.append({
                "code": code,
                "name": name,
                "price": _safe_int(r.get("stck_prpr")),
                "change_rate": _safe_float(r.get("prdy_ctrt")),
                "strength": _safe_float(r.get("tday_rltv")),
                "volume": _safe_int(r.get("acml_vol")),
                "sell_vol": _safe_int(r.get("seln_cnqn_smtn")),
                "buy_vol": _safe_int(r.get("shnu_cnqn_smtn")),
            })
            if len(results) >= top_n:
                break
        return results

    def fetch_minute_chart(self, code: str, count: int = 30,
                           minutes: int = 1, n: int = None) -> list:
        """분봉 차트 (FHKST03010200) — 5/19 사장님 요청: 5분봉 보조 지표용.

        Args:
            code: 종목코드
            count: 분봉 개수 (KIS API 최대 30개, 1분 단위)
            minutes: 분봉 단위 (5/28 P0 fix — 현재 1분봉만 지원, 5분봉 요청 시 1분봉 30개 반환)
            n: count 별칭 (5/28 P0 fix — intraday_learning_v2.py 호환)

        Returns:
            [{"time","open","high","low","close","volume"}, ...] — 시간순(오래된 것 먼저)

        Note:
            KIS는 1분봉만 직접 지원. 5분봉은 5개 묶어서 집계 (향후 구현).
            5/28 P0 fix: intraday_learning_v2 5분봉 호출 호환 → 1분봉 30개 반환 (집계는 호출자가 처리)
        """
        if n is not None:
            count = n
        params = {
            "FID_ETC_CLS_CODE": "",
            "FID_COND_MRKT_DIV_CODE": "J",
            "FID_INPUT_ISCD": code,
            "FID_INPUT_HOUR_1": "",  # 빈값=현재시각부터 과거
            "FID_PW_DATA_INCU_YN": "N",
        }
        try:
            import requests
            broker = self._get_broker()
            token = broker.access_token
            if not token.startswith("Bearer "):
                token = f"Bearer {token}"
            headers = {
                "content-type": "application/json; charset=utf-8",
                "authorization": token,
                "appkey": broker.api_key,
                "appsecret": broker.api_secret,
                "tr_id": "FHKST03010200",
                "custtype": "P",
            }
            url = f"{broker.base_url}/uapi/domestic-stock/v1/quotations/inquire-time-itemchartprice"
            resp = requests.get(url, headers=headers, params=params, timeout=10)
            data = resp.json()
            if data.get("rt_cd") != "0":
                logger.warning(f"[분봉] {code} 실패: {data.get('msg1', '')}")
                return []
            output2 = data.get("output2", [])
            bars = []
            for r in output2[:count]:
                bars.append({
                    "time": r.get("stck_cntg_hour", ""),
                    "open": _safe_int(r.get("stck_oprc")),
                    "high": _safe_int(r.get("stck_hgpr")),
                    "low": _safe_int(r.get("stck_lwpr")),
                    "close": _safe_int(r.get("stck_prpr")),
                    "volume": _safe_int(r.get("cntg_vol")),
                })
            bars.reverse()  # 오래된 것 먼저 (시간순)
            return bars
        except Exception as e:
            logger.warning(f"[분봉] {code} 예외: {e}")
            return []

    def fetch_daily_chart(self, code: str, start_date: str = "", end_date: str = "",
                          period: str = "D", adj: bool = True) -> list:
        """일/주/월/년 차트 (FHKST03010100) — 5/22 단타봇 자율 추가 (엘리어트 백테스트용).

        Args:
            code: 종목코드
            start_date: 시작일 (YYYYMMDD, 빈값=과거 100영업일 정도)
            end_date:   종료일 (YYYYMMDD, 빈값=오늘)
            period: D=일봉 / W=주봉 / M=월봉 / Y=년봉
            adj:    수정주가 True(0) / 원주가 False(1)

        Returns:
            [{"date","open","high","low","close","volume"}, ...] 시간순 (오래된 것 먼저)

        Note:
            KIS 일봉 API 한 번 호출당 최대 100봉 (~4.5개월).
            사장님 영구 룰 [feedback_verify_external_knowledge] — 외부 지식 (38.2%) 백테스트 의무.
        """
        from datetime import datetime, timedelta
        if not end_date:
            end_date = datetime.now().strftime("%Y%m%d")
        if not start_date:
            start_date = (datetime.now() - timedelta(days=200)).strftime("%Y%m%d")

        params = {
            "FID_COND_MRKT_DIV_CODE": "J",
            "FID_INPUT_ISCD": code,
            "FID_INPUT_DATE_1": start_date,
            "FID_INPUT_DATE_2": end_date,
            "FID_PERIOD_DIV_CODE": period,
            "FID_ORG_ADJ_PRC": "0" if adj else "1",
        }
        try:
            import requests
            broker = self._get_broker()
            token = broker.access_token
            if not token.startswith("Bearer "):
                token = f"Bearer {token}"
            headers = {
                "content-type": "application/json; charset=utf-8",
                "authorization": token,
                "appkey": broker.api_key,
                "appsecret": broker.api_secret,
                "tr_id": "FHKST03010100",
                "custtype": "P",
            }
            url = f"{broker.base_url}/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice"
            resp = requests.get(url, headers=headers, params=params, timeout=10)
            data = resp.json()
            if data.get("rt_cd") != "0":
                logger.warning(f"[일봉] {code} 실패: {data.get('msg1', '')}")
                return []
            output2 = data.get("output2", [])
            bars = []
            for r in output2:
                if not r.get("stck_bsop_date"):
                    continue
                bars.append({
                    "date": r.get("stck_bsop_date", ""),
                    "open": _safe_int(r.get("stck_oprc")),
                    "high": _safe_int(r.get("stck_hgpr")),
                    "low": _safe_int(r.get("stck_lwpr")),
                    "close": _safe_int(r.get("stck_clpr")),
                    "volume": _safe_int(r.get("acml_vol")),
                })
            bars.reverse()  # 오래된 것 먼저 (시간순)
            return bars
        except Exception as e:
            logger.warning(f"[일봉] {code} 예외: {e}")
            return []

    def fetch_5min_candles(self, code: str, count_5min: int = 6) -> list:
        """5분봉 집계 (1분봉 N×5개 → 5분봉 N개).

        Args:
            code: 종목코드
            count_5min: 원하는 5분봉 개수 (최대 6, 즉 30분 분량)

        Returns:
            [{"time","open","high","low","close","volume"}, ...] 5분봉
        """
        bars_1m = self.fetch_minute_chart(code, count=count_5min * 5)
        if not bars_1m:
            return []
        # 5개씩 묶어서 5분봉 집계
        candles = []
        for i in range(0, len(bars_1m) - 4, 5):
            chunk = bars_1m[i:i + 5]
            if len(chunk) < 5:
                break
            candles.append({
                "time": chunk[-1]["time"],
                "open": chunk[0]["open"],
                "high": max(c["high"] for c in chunk),
                "low": min(c["low"] for c in chunk if c["low"] > 0),
                "close": chunk[-1]["close"],
                "volume": sum(c["volume"] for c in chunk),
            })
        return candles

    def fetch_uplowprice(self, price_cls: str = "0", div_cls: str = "0",
                          market_iscd: str = "0000") -> list:
        """상하한가 포착 (FHKST130000C0).

        price_cls: "0"=상한가, "1"=하한가
        div_cls: "0"=상하한가종목, "6"=8%근접, "5"=10%근접, "1"=15%근접
        market_iscd: "0000"=전체, "0001"=코스피, "1001"=코스닥
        Returns: [{"code","name","price","change_rate","volume","ask_remain","bid_remain"}, ...]
        """
        params = {
            "FID_COND_MRKT_DIV_CODE": "J",
            "FID_COND_SCR_DIV_CODE": "11300",
            "FID_PRC_CLS_CODE": price_cls,
            "FID_DIV_CLS_CODE": div_cls,
            "FID_INPUT_ISCD": market_iscd,
            "FID_TRGT_CLS_CODE": "",
            "FID_TRGT_EXLS_CLS_CODE": "",
            "FID_INPUT_PRICE_1": "",
            "FID_INPUT_PRICE_2": "",
            "FID_VOL_CNT": "",
        }
        raw = self._ranking_get(
            "/uapi/domestic-stock/v1/quotations/capture-uplowprice",
            "FHKST130000C0", params,
        )
        results = []
        for r in raw:
            results.append({
                "code": r.get("mksc_shrn_iscd", ""),
                "name": r.get("hts_kor_isnm", ""),
                "price": _safe_int(r.get("stck_prpr")),
                "change_rate": _safe_float(r.get("prdy_ctrt")),
                "volume": _safe_int(r.get("acml_vol")),
                "ask_remain": _safe_int(r.get("total_askp_rsqn")),
                "bid_remain": _safe_int(r.get("total_bidp_rsqn")),
                "prev_vol": _safe_int(r.get("prdy_vol")),
                "vol_rate": _safe_float(r.get("prdy_vrss_vol_rate")),
            })
        return results

    def fetch_foreign_inst_total(self, target: str = "0",
                                  sort_cls: str = "0") -> list:
        """외국인/기관 매매종목 가집계 (FHPTJ04400000).

        target: "0"=전체, "1"=외국인, "2"=기관계
        sort_cls: "0"=순매수상위, "1"=순매도상위
        Returns: [{"code","name","price","change_rate","volume","amount"(원)}, ...]
        """
        params = {
            "FID_COND_MRKT_DIV_CODE": "V",
            "FID_COND_SCR_DIV_CODE": "16449",
            "FID_INPUT_ISCD": "0000",
            "FID_DIV_CLS_CODE": "1",      # 금액정렬
            "FID_RANK_SORT_CLS_CODE": sort_cls,
            "FID_ETC_CLS_CODE": target,
        }
        raw = self._ranking_get(
            "/uapi/domestic-stock/v1/quotations/foreign-institution-total",
            "FHPTJ04400000", params,
        )
        if raw and len(raw) > 0:
            logger.debug(f"[가집계] target={target} 첫행 키: {list(raw[0].keys())[:10]}")
        results = []
        for r in raw:
            # 순매수금액(원): target에 맞는 필드 우선 사용
            if target == "1":
                net_amt = _safe_int(r.get("frgn_ntby_tr_pbmn", 0))
            elif target == "2":
                net_amt = _safe_int(r.get("orgn_ntby_tr_pbmn", 0))
            else:
                net_amt = _safe_int(r.get("frgn_ntby_tr_pbmn", 0)) + \
                          _safe_int(r.get("orgn_ntby_tr_pbmn", 0))
            # fallback: 개별 필드 없으면 generic 필드 시도
            if net_amt == 0:
                net_amt = _safe_int(r.get("ntby_tr_pbmn", 0))
            results.append({
                "code": r.get("mksc_shrn_iscd", ""),
                "name": r.get("hts_kor_isnm", ""),
                "price": _safe_int(r.get("stck_prpr")),
                "change_rate": _safe_float(r.get("prdy_ctrt")),
                "volume": _safe_int(r.get("acml_vol")),
                "amount": net_amt,
            })
        return results

    # ═══════════════════════════════════════
    #  KOSPI200 야간선물 시세 조회
    # ═══════════════════════════════════════

    def fetch_night_futures_price(self) -> dict:
        """KOSPI200 야간선물 현재가 조회.

        KIS API: FHMIF10000000 (선물옵션 시세)
        종목코드: 101S + 월물코드 (근월물 자동 탐색)
        운영시간: 18:00 ~ 익일 06:00

        Returns:
            {
                "code": "101SC000",
                "price": 371.50,
                "prev_close": 370.00,
                "change": 1.50,
                "change_pct": 0.41,
                "volume": 12345,
                "timestamp": "2026-04-14 22:30",
                "available": True,
            }
            또는 장외시간이면 {"available": False}
        """
        import requests
        from datetime import datetime

        try:
            broker = self._get_broker()
            token = broker.access_token
            if not token.startswith("Bearer "):
                token = f"Bearer {token}"

            headers = {
                "content-type": "application/json; charset=utf-8",
                "authorization": token,
                "appkey": broker.api_key,
                "appsecret": broker.api_secret,
                "tr_id": "FHMIF10000000",
                "custtype": "P",
            }

            # 근월물 코드 생성: 101S + 월코드
            # 월코드: 1=A, 2=B, ..., 9=I, 10=J, 11=K, 12=L
            now = datetime.now()
            month_codes = {1: "A", 2: "B", 3: "C", 4: "D", 5: "E", 6: "F",
                           7: "G", 8: "H", 9: "I", 10: "J", 11: "K", 12: "L"}

            # 야간선물은 근월물 (만기일 전이면 현재월, 만기일 후면 다음달)
            # 간단히: 매월 2번째 목요일 만기 → 15일 이후면 다음달
            m = now.month
            y = now.year % 100  # 26
            if now.day >= 15:
                m += 1
                if m > 12:
                    m = 1
                    y += 1
            month_char = month_codes.get(m, "F")
            fut_code = f"101S{month_char}{y:02d}0"

            url = f"{broker.base_url}/uapi/domestic-futureoption/v1/quotations/inquire-price"
            params = {
                "FID_COND_MRKT_DIV_CODE": "F",
                "FID_INPUT_ISCD": fut_code,
            }

            resp = requests.get(url, headers=headers, params=params, timeout=10)
            data = resp.json()

            if data.get("rt_cd") != "0":
                # 코드가 맞지 않으면 대안 시도
                logger.debug(f"[NIGHT_FUT] {fut_code} 실패, 대안 시도")
                # 현재월 재시도
                month_char2 = month_codes.get(now.month, "F")
                fut_code2 = f"101S{now.year % 100:02d}{month_char2}0"
                params["FID_INPUT_ISCD"] = fut_code2
                resp = requests.get(url, headers=headers, params=params, timeout=10)
                data = resp.json()

                if data.get("rt_cd") != "0":
                    return {"available": False, "reason": data.get("msg1", "조회 실패")}

            out = data.get("output1", data.get("output", {}))
            if isinstance(out, list):
                out = out[0] if out else {}

            price = _safe_float(out.get("stck_prpr") or out.get("futs_prpr"))
            prev = _safe_float(out.get("stck_sdpr") or out.get("futs_prdy_clpr"))
            vol = _safe_int(out.get("acml_vol"))

            if price <= 0:
                return {"available": False, "reason": "시세 없음 (장외시간)"}

            change = price - prev if prev > 0 else 0
            change_pct = (change / prev * 100) if prev > 0 else 0

            return {
                "code": params["FID_INPUT_ISCD"],
                "price": price,
                "prev_close": prev,
                "change": round(change, 2),
                "change_pct": round(change_pct, 2),
                "volume": vol,
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M"),
                "available": True,
            }

        except Exception as e:
            logger.warning(f"[NIGHT_FUT] 야간선물 조회 실패: {e}")
            return {"available": False, "reason": str(e)}

    def liquidate_all(self, manual: bool = False) -> dict:
        """전종목 시장가 청산"""
        bal = self.fetch_balance()
        if not bal or not bal.get("success"):
            return {"success": False, "message": "잔고 조회 실패"}

        if not bal["positions"]:
            return {"success": True, "message": "보유 종목 없음 (청산 불필요)"}

        results = []
        failed_codes = []
        for pos in bal["positions"]:
            r = self.sell_market(pos["code"], pos["qty"], manual=manual)
            ok = r.get("success", False)
            results.append(f"{pos['name']}: {'성공' if ok else '실패'}")
            if not ok:
                failed_codes.append(pos["code"])
            time.sleep(0.2)

        all_ok = len(failed_codes) == 0
        return {
            "success": all_ok,
            "message": f"전량 청산 {'완료' if all_ok else '부분실패'}\n" + "\n".join(results),
            "failed_codes": failed_codes,
        }
