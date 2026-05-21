# -*- coding: utf-8 -*-
"""
Body Hunter v4 자동매매 루프 - 동적 목표가 + 보이지않는 목표가 통합
================================================================
로직 축적:
  1. ATR 기반 SL/TP (고정% → 동적)
  2. 뉴스 감성 → 목표가 보정
  3. 매집원가 → SL 하한선
  4. 매수 금액: 실시간 잔고 / 종목수 (동적 계산, 하드코딩 X)
  5. 갭업 판단: 고정 5% X → TP(보이지않는 목표가) 대비 업사이드 판단
  6. 추매(ACTION_ADD): 업사이드 8%+ 시 가용현금 30% 추가매수
  7. MACD 0선 크로스: 진입 조건 6개 중 1개로 통합
  8. 진입 조건: 6개 중 3개 충족 시 매수
     (가격안정/양봉/체결강도/AI EYE/MACD 0선/목표가 업사이드)

모드 2개:
  day  - 당일 매매 (15:10 전량 청산)
  swing - 스윙 매매 (동적 목표가 재평가, 최대 N일 보유)

JobQueue (python-telegram-bot)로 스케줄:
  09:00 → 추천종목 로드 + 실시간 관찰 시작
  매 30초 → 진입감시 + 포지션 SL/TP 감시
  15:00 → 스윙 모드: 동적 목표가 재평가 + 추매/매도 판정
  15:10 → 데이 모드: 전량 청산 | 스윙 모드: 요약만
"""

import asyncio
import json
import logging
import time
from datetime import datetime, date
from data.trading_calendar import is_trading_day, next_trading_day
from pathlib import Path
from typing import Callable, Optional

logger = logging.getLogger("BH.AutoTrader")

# P0-8: 정보봇 위험감지 SDK 통합
try:
    from .risk_gate_helper import (
        check_market_risk_blocked, is_msci_blacklisted, get_risk_info_brief,
        is_foreign_exhaustion_blocked,
        should_trigger_inverse_alert, get_inverse_alert_message,
    )
    _RISK_GATE_AVAILABLE = True
except ImportError:
    try:
        from risk_gate_helper import (
            check_market_risk_blocked, is_msci_blacklisted, get_risk_info_brief,
            is_foreign_exhaustion_blocked,
            should_trigger_inverse_alert, get_inverse_alert_message,
        )
        _RISK_GATE_AVAILABLE = True
    except ImportError:
        _RISK_GATE_AVAILABLE = False
        # fallback 함수들 — SDK 미가용 시 통과 (운영 지속)
        def check_market_risk_blocked(): return False, ""
        def is_msci_blacklisted(_): return False
        def get_risk_info_brief(): return ""
        def is_foreign_exhaustion_blocked(_code): return False
        def should_trigger_inverse_alert(): return False
        def get_inverse_alert_message(): return ""
# P0-8-PATCH-APPLIED

# Recovery Add-On — 하락 시 자동 분할 추매 (차트 영웅식, 2026-05-16)
try:
    from .recovery_add_on import evaluate_add_on, record_add_on, format_alert_message
except ImportError:
    from recovery_add_on import evaluate_add_on, record_add_on, format_alert_message


BASE_DIR = Path(__file__).resolve().parent.parent
CANDIDATES_PATH = BASE_DIR / "data_store" / "swing_candidates.json"
RISK_STATE_PATH = BASE_DIR / "data_store" / "risk_state.json"
POSITIONS_PATH = BASE_DIR / "data_store" / "positions.json"


class AutoTrader:
    """Body Hunter v4 자동매매 - 사전감지 + AI 모니터 통합"""

    def __init__(self, config: dict, trader):
        self.config = config
        self.trader = trader
        self.is_running = False
        self._send_alert: Optional[Callable] = None
        self._positions = {}  # {code: {entry_price, stop_loss, take_profit, target_state, ...}}
        self._load_positions()  # VPS 재시작 후 복원

        # 진입 감시 대기열: 장 시작 후 실시간으로 관찰 → 조건 충족 시 매수
        # {code: {name, buy_amount, sl, tp, score, prev_close, checks, ...}}
        self._entry_watch = {}

        # ── Phase 3-B: WebSocket 실시간 시세 (자동매매 진입 정확도 ↑) ──
        # auto_trade=true일 때만 활성화. 진입감시 종목 자동 구독 + 캐시 저장.
        # _check_entry_watch가 캐시 5초 이내면 우선 사용, 만료/None이면 REST fallback.
        self._ws_client = None       # KISWebSocketClient (지연 초기화)
        self._ws_task = None         # 백그라운드 run_forever 태스크
        self._ws_cache = {}          # {code: {price, volume, ts, updated_at}}
        # 5/19 fix — config.yaml 키 경로는 "bot.auto_trade" ("trader" 오타 수정, 사장님 5/18 19:50 지적)
        self._ws_enabled = bool(config.get("bot", {}).get("auto_trade", False))

        # 모드: "day" or "swing"
        self.mode = config.get("bot", {}).get("trade_mode", "swing")

    def _get_jarvis_dynamic_qty(self) -> tuple:
        """★ 5/21 아이디어 #1 — 자비스 자율 다주 결정 (70억 트레이더 미션 1단계) ★

        시장 상황 보고 1주/2주/3주 자율 결정. config 토글로 활성화/비활성화.

        결정 로직 (보수 → 강세):
          - EWY +5%+ (미국장 야간 강세 후속 반응 기대) AND regime != BEARISH → 3주
          - KOSPI +1%+ (한국장 명확 강세) AND regime != BEARISH → 3주
          - KOSPI +0.5%+ AND regime BULLISH+ → 2주
          - 기타 (NEUTRAL/약세) → 1주 (안전 기본)
          - 큰형 advisory PANIC/BEARISH → 1주 강제

        Returns:
            (qty, reason) — 매수 수량, 사유 텍스트
        """
        # 토글 체크 — 기본 false (사장님 5/21 단계적 적용: 5/22부터 on)
        dynamic_enabled = (
            self.config.get("bot", {}).get("asset_pool", {}).get("dynamic_qty", False)
        )
        if not dynamic_enabled:
            return 1, "1주 모드 (dynamic_qty=off, 사장님 5/19 결정)"

        qty = 1
        reason = "기본 1주 (시그널 미달)"
        try:
            import json
            from pathlib import Path
            base = Path(__file__).resolve().parent.parent / "data_store"

            # EWY (미국장 한국 ETF, 야간 시그널)
            picks_path = base / "daytrading_picks.json"
            ewy_1d = 0.0
            ks200_1d = 0.0
            if picks_path.exists():
                pd = json.loads(picks_path.read_text("utf-8"))
                ewy_1d = float(pd.get("ewy_signal", {}).get("ewy_1d", 0))
                ks200_1d = float(pd.get("ewy_signal", {}).get("ks200_1d", 0))

            # brain_state (큰형 advisory regime)
            brain_path = base / "brain_state.json"
            regime = "NEUTRAL"
            if brain_path.exists():
                bd = json.loads(brain_path.read_text("utf-8"))
                regime = bd.get("regime", "NEUTRAL")

            # 안전 우선: BEARISH/PANIC이면 강제 1주
            if regime in ("BEARISH", "PANIC"):
                return 1, f"안전 (regime={regime}) → 1주 강제"

            # 강세 단계 판정
            if ewy_1d >= 5.0:
                qty = 3
                reason = f"강세 (EWY {ewy_1d:+.2f}% 미국장 후속) → 3주"
            elif ks200_1d >= 1.0:
                qty = 3
                reason = f"강세 (KOSPI {ks200_1d:+.2f}%) → 3주"
            elif ks200_1d >= 0.5 and regime in ("BULLISH", "STRONG_BULL"):
                qty = 2
                reason = f"중강세 (KOSPI {ks200_1d:+.2f}% / {regime}) → 2주"
            else:
                reason = f"보수 (EWY {ewy_1d:+.2f}% / KOSPI {ks200_1d:+.2f}% / {regime}) → 1주"

        except Exception as e:
            logger.warning(f"[dynamic_qty] 결정 실패 (기본 1주): {e}")
            return 1, f"기본 1주 (예외: {type(e).__name__})"

        return qty, reason

    def _is_sell_protected(self, code: str, reason: str = "") -> bool:
        """[5/20 사고 후 추가] 사장님 보호 명령 단일 진입점.

        모든 매도 함수(job_eod_close / job_daily_reeval / job_monitor /
        intraday_loop 등)는 매도 실행 직전 반드시 이 함수를 호출해야 함.

        True 반환 = 매도 차단. False = 매도 허용.

        배경: 5/20 -293만 사고. sl_disabled=True 명령(사장님 5/19)을 8개 매도 함수
        중 2개만 체크. job_daily_reeval(L3208)이 사각지대 — 보호 4종 모두 매도.
        """
        pos = self._positions.get(code) or {}
        if pos.get("sl_disabled") or str(pos.get("source", "")).startswith("manual_sync"):
            logger.warning(
                f"[SELL BLOCKED] {pos.get('name', code)}({code}) — "
                f"reason={reason} / sl_disabled={pos.get('sl_disabled')} "
                f"source={pos.get('source')}"
            )
            # RED ALERT 텔레그램 (사일런트 차단 방지)
            if self._send_alert:
                try:
                    import asyncio as _asyncio
                    _asyncio.create_task(self._send_alert(
                        f"🛡️ [매도 차단] {pos.get('name', code)}({code})\n"
                        f"  사유: {reason}\n"
                        f"  사장님 보호 명령 (sl_disabled) 적용"
                    ))
                except Exception:
                    pass
            return True
        return False

        # AI 실시간 모니터
        self._rt_monitor = None

        # positions.json에서 복원된 종목을 RealtimeMonitor에도 등록 (재시작 후 SL/TP 자동 청산 작동)
        # source 필드를 함께 전달하여 _decide()에서 manual_sync 가드 분기 가능
        self._restore_positions_to_rt_monitor()

        # Intraday AI Eye (5분 주기 흐름 분석)
        self._eye = None
        self._eye_counter = 0  # 30초 카운터 (10 = 5분)

        # 정보봇 CRISIS 인버스 ETF 알림 (1일 1회 송출 제한)
        self._inverse_alert_date = ""  # ISO 날짜 문자열 (예: "2026-05-16")

        # ── 리스크 게이트 (일일손실한도 + MDD) ──
        risk = config.get("risk", {})
        self._daily_loss_limit = risk.get("daily_loss_limit", 500000)
        self._mdd_limit_pct = risk.get("mdd_limit_pct", 4.5)
        self._risk_state = self._load_risk_state()
        self._risk_blocked = False  # True면 신규 매수 차단
        self._feed_alert_sent = False  # 피드 중단 알림 1회만  # SILENT: MSG-REDUX

        # ── 자동매수 확인 대기열 ──
        self._confirm_auto = config.get("bot", {}).get("confirm_real_order", True)
        self._pending_auto_buys = []  # [{code, name, amount, sl, tp, tp1_quick, score}]

        # ── NIGHTWATCH NXT ──
        self._nxt_positions = {}
        self._nightwatch_report = None

        # ── 시간외 선취매 (Pre-Dawn) ──
        self._predawn_positions = {}  # {code: {name, entry_price, sl, tp, score, ...}}

        # ── CFO 파사드 (set_cfo로 연결) ──
        self._cfo = None

    def set_cfo(self, cfo):
        """CFO 파사드 연결 (telegram_bot에서 호출)"""
        self._cfo = cfo

    def _get_rt_monitor(self):
        """RealtimeMonitor lazy init"""
        if self._rt_monitor is None:
            from data.realtime_monitor import RealtimeMonitor
            self._rt_monitor = RealtimeMonitor(self.config)
        return self._rt_monitor

    # ═══════════════════════════════════════
    #  리스크 게이트 (일일 손실 한도 + MDD)
    # ═══════════════════════════════════════

    def _load_risk_state(self) -> dict:
        """리스크 상태 파일 로드 (일일 리셋)"""
        today = date.today().isoformat()
        default = {
            "date": today,
            "daily_realized_loss": 0,
            "peak_equity": 0,
            "current_mdd_pct": 0.0,
            "blocked_reason": "",
        }
        try:
            if RISK_STATE_PATH.exists():
                with open(RISK_STATE_PATH, "r", encoding="utf-8") as f:
                    state = json.load(f)
                if state.get("date") == today:
                    return state
                # 날짜 변경 → 일일 손실 리셋, MDD peak은 유지
                state["date"] = today
                state["daily_realized_loss"] = 0
                state["blocked_reason"] = ""
                return state
        except Exception as e:
            logger.warning(f"리스크 상태 로드 실패: {e}")
        return default

    def _save_risk_state(self):
        """리스크 상태 저장 (atomic write)"""
        try:
            RISK_STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
            tmp = RISK_STATE_PATH.with_suffix(".tmp")
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(self._risk_state, f, ensure_ascii=False, indent=2)
            tmp.replace(RISK_STATE_PATH)
        except Exception as e:
            logger.error(f"리스크 상태 저장 실패: {e}")
            # tmp 잔여파일 정리
            tmp = RISK_STATE_PATH.with_suffix(".tmp")
            if tmp.exists():
                try:
                    tmp.unlink()
                except OSError:
                    pass

    def record_realized_loss(self, loss_amount: int):
        """실현 손실 기록 (매도 시 호출)"""
        if loss_amount >= 0:
            return  # 이익이면 무시
        self._risk_state["daily_realized_loss"] += abs(loss_amount)
        self._save_risk_state()
        logger.info(f"일일 실현 손실 누적: {self._risk_state['daily_realized_loss']:,}원")

    def _record_trade_pnl(self, code: str, pnl: int, source: str = ""):
        """매매 P&L 통합 기록 (기존 리스크 + CFO)

        모든 매도 완료 지점에서 record_realized_loss 대신 이 메서드를 호출.
        """
        # 1) 기존 일일 손실 추적 (손실만)
        self.record_realized_loss(pnl)
        # 2) CFO P&L 추적 (손익 모두 — DrawdownShield 포함)
        if self._cfo:
            try:
                self._cfo.record_trade(code, pnl, source)
            except Exception as e:
                logger.warning(f"CFO record_trade 실패 (무시): {e}")

    def check_risk_gate(self) -> tuple[bool, str]:
        """리스크 게이트 체크 → (통과여부, 사유)

        1. 일일 손실 한도 체크
        2. MDD 체크
        """
        # 날짜 변경 체크
        today = date.today().isoformat()
        if self._risk_state.get("date") != today:
            self._risk_state = self._load_risk_state()
            self._risk_blocked = False

        # 1) 일일 손실 한도
        daily_loss = self._risk_state.get("daily_realized_loss", 0)
        if daily_loss >= self._daily_loss_limit:
            self._risk_blocked = True
            reason = f"일일 손실 한도 초과: {daily_loss:,}원 / {self._daily_loss_limit:,}원"
            self._risk_state["blocked_reason"] = reason
            self._save_risk_state()
            return False, reason

        # 2) MDD 체크
        try:
            bal = self.trader.fetch_balance()
            if bal.get("success"):
                equity = bal["total_eval"]
                peak = self._risk_state.get("peak_equity", 0)

                if equity > peak:
                    self._risk_state["peak_equity"] = equity
                    peak = equity
                    self._save_risk_state()

                if peak > 0:
                    mdd = (peak - equity) / peak * 100
                    self._risk_state["current_mdd_pct"] = round(mdd, 2)
                    self._save_risk_state()

                    if mdd >= self._mdd_limit_pct:
                        self._risk_blocked = True
                        reason = f"MDD 한도 초과: -{mdd:.1f}% (한도 -{self._mdd_limit_pct}%)"
                        self._risk_state["blocked_reason"] = reason
                        self._save_risk_state()
                        return False, reason
        except Exception as e:
            logger.warning(f"MDD 체크 실패: {e}")

        # ─── P0-8: 정보봇 시장 위험감지 (DANGER/CRISIS 자동 차단) ───
        try:
            jgis_blocked, jgis_reason = check_market_risk_blocked()
            if jgis_blocked:
                self._risk_blocked = True
                self._risk_state["blocked_reason"] = jgis_reason
                self._save_risk_state()
                return False, jgis_reason
        except Exception as _e:
            logger.warning("정보봇 위험감지 체크 실패: %s", _e)
        # ─── P0-8 끝 ───

        return True, ""

    def get_risk_status(self) -> str:
        """리스크 상태 리포트"""
        s = self._risk_state
        daily_loss = s.get("daily_realized_loss", 0)
        mdd = s.get("current_mdd_pct", 0)
        peak = s.get("peak_equity", 0)
        blocked = s.get("blocked_reason", "")

        lines = [
            f"일일 손실: {daily_loss:,}원 / {self._daily_loss_limit:,}원",
            f"MDD: -{mdd:.1f}% (한도 -{self._mdd_limit_pct}%)",
            f"고점 자산: {peak:,}원",
        ]
        if blocked:
            lines.append(f"차단: {blocked}")
        else:
            lines.append("상태: 정상 (매수 가능)")
        return "\n".join(lines)

    def start(self, send_alert_func: Callable):
        """자동매매 시작"""
        self.is_running = True
        self._send_alert = send_alert_func
        logger.info(f"자동매매 시작 (모드: {self.mode})")

    def stop(self):
        """자동매매 정지"""
        self.is_running = False
        self._pending_auto_buys.clear()
        logger.info("자동매매 정지")

    def execute_pending_auto_buys(self) -> list[dict]:
        """대기 중인 자동매수 전부 실행 → 결과 리스트 반환

        분할매수 시: split_done 카운터 여기서 올림 + 포지션 최초 1회만 생성
        """
        results = []
        for item in self._pending_auto_buys:
            code, name = item["code"], item["name"]
            amount = item["amount"]

            # ─── P0-8: MSCI 차단 종목 자동 거부 ───
            try:
                if is_msci_blacklisted(code):
                    logger.warning("[자동매수] MSCI 차단 종목 거부: %s %s", code, name)
                    results.append({"code": code, "name": name, "success": False, "reason": "MSCI 차단 종목"})
                    continue
            except Exception as _e:
                logger.warning("MSCI 차단 체크 실패: %s", _e)
            # ─── P0-8 끝 ───

            # ─── P0-8 추가: 외인소진율 차단 (위험구간 + 임계값 초과 시) ───
            # 5/15 폭락에서 외인소진율 높은 종목이 패닉셀 직격탄
            try:
                if is_foreign_exhaustion_blocked(code):
                    logger.warning("[자동매수] 외인소진율 위험 거부: %s %s", code, name)
                    results.append({"code": code, "name": name, "success": False, "reason": "외인소진율 위험"})
                    continue
            except Exception as _e:
                logger.warning("외인소진율 차단 체크 실패: %s", _e)
            # ─── P0-8 추가 끝 ───

            result = self.trader.safe_buy(code, amount)
            if result.get("success"):
                # Recovery Add-On 추매 체결 시 카운터 + 이력 기록
                rao_decision = item.get("_recovery_add_on")
                if rao_decision and code in self._positions:
                    try:
                        record_add_on(self._positions[code], rao_decision)
                        self._save_positions()
                        logger.info(
                            f"[Recovery Add-On] 체결 기록: {name}({code}) "
                            f"{rao_decision.count}차, {rao_decision.add_amount:,}원"
                        )
                    except Exception as _e:
                        logger.warning(f"[Recovery Add-On] 기록 실패 {code}: {_e}")

                # 분할매수: entry_watch 상태 업데이트
                if item.get("_is_split") and code in self._entry_watch:
                    watch = self._entry_watch[code]
                    watch["split_done"] = watch.get("split_done", 0) + 1
                    watch["last_split_check"] = watch.get("checks", 0)
                    done = watch["split_done"]
                    total = watch.get("split_count", 3)
                    logger.info(f"분할매수 체결: {name} {done}/{total}차")
                    # 전부 완료 → 감시 제거 + WebSocket 구독 자동 해제 (H1 fix)
                    if done >= total:
                        self._entry_watch.pop(code, None)
                        try:
                            asyncio.create_task(self._unsubscribe_entry_watch(code))
                        except Exception:
                            pass

                # 포지션 최초 1회만 생성 (분할매수 시 덮어쓰기 방지)
                if code not in self._positions:
                    price_info = self.trader.fetch_price(code)
                    cp = price_info.get("current_price", 0) if price_info and price_info.get("success") else 0
                    if cp <= 0:
                        # 매수 완료됐으나 가격 조회 실패 → 추천데이터 기반 포지션 등록
                        logger.error(f"가격 조회 실패 {code} — 추천데이터 기반 포지션 등록")
                        cp = item.get("close", 0) or item.get("entry", 0) or item.get("tp", 0) or item.get("sl", 0)
                        if cp <= 0:
                            logger.error(f"가격 fallback 전부 실패 {code} — 포지션 등록 불가")
                            continue
                    target_state = self._init_dynamic_target(code, name, cp)
                    sl = target_state.dynamic_sl if target_state else item.get("sl", int(cp * 0.95))
                    tp = target_state.dynamic_tp if target_state else item.get("tp", int(cp * 1.10))

                    self._positions[code] = {
                        "entry_price": cp,
                        "stop_loss": sl,
                        "take_profit": tp,
                        "entry_date": datetime.now().strftime("%Y-%m-%d"),
                        "name": name,
                        "target_state": target_state,
                        "high_watermark": cp,
                        "trailing_activated": False,
                        "trailing_sl": 0,
                        "regime": item.get("regime", "NORMAL"),
                        "source": item.get("source", ""),
                    }
                    self._save_positions()
                    try:
                        rtm = self._get_rt_monitor()
                        rtm.register_position(code, name, cp, sl, tp,
                                              source=item.get("source", ""))
                    except Exception as e:
                        logger.warning(f"AI 모니터 등록 실패 {code}: {e}")

                sl = self._positions[code]["stop_loss"]
                tp = self._positions[code]["take_profit"]
                results.append({"success": True, "name": name, "code": code,
                                "message": result.get("message"), "sl": sl, "tp": tp})
            else:
                results.append({"success": False, "name": name, "code": code,
                                "message": result.get("message")})

        self._pending_auto_buys.clear()
        return results

    def cancel_pending_auto_buys(self) -> int:
        """대기 중인 자동매수 전부 취소 → 취소 건수 반환"""
        count = len(self._pending_auto_buys)
        self._pending_auto_buys.clear()
        return count

    def _is_market_hours(self) -> bool:
        now = datetime.now()
        if not is_trading_day(now.date()):
            return False
        h = now.hour * 100 + now.minute
        return 900 <= h <= 1520

    async def _alert(self, text: str):
        """텔레그램 알림 (실패 시 로컬 로그 폴백)"""
        sent = False
        if self._send_alert:
            try:
                await self._send_alert(text)
                sent = True
            except Exception as e:
                logger.error(f"텔레그램 알림 전송 실패: {e}")

        if not sent:
            # 폴백: 로컬 알림 로그 파일에 저장
            try:
                alert_log = BASE_DIR / "logs" / "alert_fallback.log"
                alert_log.parent.mkdir(parents=True, exist_ok=True)
                with open(alert_log, "a", encoding="utf-8") as f:
                    f.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {text}\n{'='*50}\n")
                logger.info(f"알림 폴백 저장: {alert_log}")
            except Exception as e2:
                logger.error(f"알림 폴백도 실패: {e2}")

    # ═══════════════════════════════════════
    #  스케줄 Job 핸들러
    # ═══════════════════════════════════════

    async def job_morning_scan(self, context):
        """아침 스캔 → 리포트 전송 + 자동 매수

        스윙 모드: swing_candidates.json에서 ATR SL/TP + 매집원가 SL 적용
        데이 모드: 기존 5D 스캔 + 고정 SL/TP
        """
        if not is_trading_day():
            return

        chat_id = None
        if not self._send_alert:
            import os
            chat_id = os.getenv("TELEGRAM_CHAT_ID")

        async def _send(text):
            if self._send_alert:
                await self._send_alert(text)
            elif chat_id:
                await context.bot.send_message(chat_id=chat_id, text=text)

        logger.info(f"아침 스캔 시작 (모드: {self.mode})")
        await _send(f"🌅 아침 스캔 시작 ({self.mode} 모드)...")

        if self.mode == "swing":
            await self._morning_swing(context, _send)
            # await self._morning_momentum(context, _send)  # PF 0.70 → 비활성화 (2026-03-07)
        else:
            await self._morning_day(context, _send)

    async def _morning_swing(self, context, _send):
        """스윙 모드 아침: 추천 종목 로드 → 진입감시 대기열 등록

        즉시 매수 X → KIS API로 실시간 관찰 → 조건 충족 시 매수
        실제 매수 판단은 job_monitor 30초 루프의 _check_entry_watch()에서 처리
        """

        # 0) CORTEX 체제 체크 (최우선) - 위기모드 수동 오버라이드 포함
        from data.market_health import is_crisis_mode, get_regime_rules
        crisis_active, crisis_reason = is_crisis_mode()
        if crisis_active:
            await _send(f"위기 모드 - 스캔 중단\n{crisis_reason}")
            return

        regime_rules = get_regime_rules()
        if not regime_rules["new_buy"]:
            await _send(
                f"CORTEX 체제: {regime_rules['regime']} - 신규매수 중단\n"
                f"자본사용: {regime_rules['capital_use']*100:.0f}%"
            )
            return

        candidates = []
        manual_picks = []  # 수동 모드 종목 (알림만)

        # 0.5) /내일 사용자 지정 종목 우선
        user_picks_used = False
        try:
            picks_path = Path(__file__).parent.parent / "data_store" / "tomorrow_picks.json"
            if picks_path.exists():
                with open(picks_path, "r", encoding="utf-8") as f:
                    picks_data = json.load(f)
                pick_date = picks_data.get("date", "")
                today_str = date.today().strftime("%Y-%m-%d")
                if pick_date == today_str and picks_data.get("picks"):
                    # 유효한 오늘자 picks → recommendation 대신 사용
                    from data.morning_recommendation import load_recommendation
                    rec_for_picks = load_recommendation()
                    # 추천 데이터에서 SL/TP/score 가져오기
                    rec_map = {}
                    if rec_for_picks and rec_for_picks.stocks:
                        for s in rec_for_picks.stocks:
                            rec_map[s.code] = s

                    # Trade Object에서 SL/TP 가져오기
                    to_map_picks = {}
                    try:
                        to_path = Path(__file__).parent.parent / "data_store" / "trade_objects.json"
                        if to_path.exists():
                            with open(to_path, "r", encoding="utf-8") as f:
                                to_data = json.load(f)
                            for t in to_data.get("objects", []):
                                to_map_picks[t.get("code", "")] = t
                    except Exception as _to_e:
                        # trade_objects.json 로드 실패 시 기본 SL/TP가 적용됨 → 로깅 필수
                        logger.warning(
                            f"[auto_trader] trade_objects.json 로드 실패 → 기본 SL/TP 사용: {_to_e}"
                        )

                    for p in picks_data["picks"]:
                        code, name = p["code"], p["name"]
                        if p.get("mode") == "manual":
                            manual_picks.append({"code": code, "name": name})
                            continue

                        # SL/TP: trade_object > recommendation > 기본값
                        to = to_map_picks.get(code, {})
                        rs = rec_map.get(code)
                        entry = to.get("entry_price", 0) or (rs.entry if rs else 0)
                        sl = to.get("stop_loss", 0) or (rs.sl if rs else 0)
                        tp = to.get("target_price", 0) or (rs.tp if rs else 0)

                        # entry/sl/tp 없으면 현재가 기반 기본값
                        if not entry:
                            try:
                                cp_data = await asyncio.to_thread(
                                    self.trader.fetch_price, code
                                )
                                entry = cp_data.get("current_price", 0) if cp_data else 0
                            except Exception:
                                entry = 0
                        if entry and not sl:
                            # 매크로 전략 기반 SL 조정
                            _base_sl = 0.035
                            try:
                                from data.macro_strategy import get_adjusted_sl
                                _base_sl = get_adjusted_sl(0.035)
                            except Exception as _ms_e:
                                # macro_strategy 실패 시 고정 SL(3.5%) 적용 → 변동성 미반영
                                logger.warning(
                                    f"[auto_trader] macro_strategy 실패 → 기본 SL 3.5% 적용: {_ms_e}"
                                )
                            sl = int(entry * (1 - _base_sl))
                        if entry and not tp:
                            tp = int(entry * 1.05)   # 기본 +5%

                        candidates.append({
                            "code": code, "name": name,
                            "total_score": to.get("total_score", rs.total_score if rs else 50),
                            "entry": entry, "sl": sl, "tp": tp,
                            "tp1_quick": tp,
                            "source": "user_pick",
                            "confidence": to.get("confidence", rs.confidence if rs else "USER"),
                            "regime": to.get("regime", getattr(rs, "regime", "NORMAL") if rs else "NORMAL"),
                        })

                    user_picks_used = True
                    auto_cnt = len(candidates)
                    manual_cnt = len(manual_picks)
                    await _send(
                        f"📋 사용자 지정 종목 로드: 자동 {auto_cnt} + 수동 {manual_cnt}"
                    )
                    # 사용 후 파일 삭제 (1회성)
                    picks_path.unlink(missing_ok=True)
        except Exception as e:
            logger.warning(f"tomorrow_picks 로드 실패: {e}")

        # 1) 저녁 추천 파이프라인 결과 (사용자 지정 없을 때만)
        cross_regime = ""
        rec = None
        if not user_picks_used:
            try:
                from data.morning_recommendation import load_recommendation
                rec = load_recommendation()
                if rec and rec.stocks:
                    candidates = [
                        {
                            "code": s.code, "name": s.name,
                            "total_score": s.total_score,
                            "entry": s.entry, "sl": s.sl, "tp": s.tp,
                            "tp1_quick": s.tp,
                            "source": "pipeline",
                            "confidence": s.confidence,
                            "regime": getattr(s, "regime", "NORMAL"),
                            "regime_score": getattr(s, "regime_score", 0.0),
                        }
                        for s in rec.stocks
                    ]
                    cross_regime = getattr(rec, "cross_regime", "")
                    await _send(f"저녁 추천 {len(candidates)}종목 로드 완료")
                    if rec.warning:
                        await _send(f"{rec.warning}")
            except Exception as e:
                logger.warning(f"추천 로드 실패: {e}")

        # 1.2) 전쟁→재건 릴레이 종목 추가
        war_relay_count = 0
        try:
            if rec and rec.war_relay_stocks:
                existing_codes = {c["code"] for c in candidates}
                for s in rec.war_relay_stocks:
                    if s["code"] in existing_codes:
                        continue  # 메인 파이프라인에 이미 있으면 스킵
                    candidates.append({
                        "code": s["code"], "name": s["name"],
                        "total_score": 50 - s.get("priority", 99),  # 우선순위 역변환
                        "entry": s["entry"], "sl": s["sl"],
                        "tp": s["tp1"], "tp1_quick": s["tp1"],
                        "source": "war_relay",
                        "confidence": "WAR_RELAY",
                        "tier": s.get("tier", ""),
                    })
                    war_relay_count += 1
                if war_relay_count:
                    await _send(f"전쟁릴레이 {war_relay_count}종목 추가 로드")
        except Exception as e:
            logger.warning(f"전쟁릴레이 로드 실패: {e}")

        # 1.3) ETF 추천 후보 추가 (Phase 3)
        etf_count = 0
        try:
            if rec and rec.etf_recommendations:
                existing_codes = {c["code"] for c in candidates}
                for etf_r in rec.etf_recommendations:
                    code = etf_r.get("code", "")
                    if not code or code in existing_codes:
                        continue
                    if etf_r.get("signal") != "BUY":
                        continue
                    candidates.append({
                        "code": code,
                        "name": etf_r.get("name", ""),
                        "total_score": etf_r.get("score", 50),
                        "entry": etf_r.get("entry", 0),
                        "sl": etf_r.get("sl", 0),
                        "tp": etf_r.get("tp", 0),
                        "tp1_quick": etf_r.get("tp", 0),
                        "source": "etf_rec",
                        "confidence": etf_r.get("confidence", "LOW"),
                        "is_etf": True,
                        "etf_category": etf_r.get("category", ""),
                        "holding_days": etf_r.get("holding_days", 5),
                    })
                    etf_count += 1
                if etf_count:
                    await _send(f"ETF 추천 {etf_count}종목 추가 로드")
        except Exception as e:
            logger.warning(f"ETF 추천 로드 실패: {e}")

        # 1.5) NIGHTWATCH 채권 자경단 신호등 게이트
        if cross_regime == "DIVERGENCE":
            await _send(
                f"NIGHTWATCH: DIVERGENCE (채권 자경단)\n"
                f"주식하락 + 금리상승 = 절대 진입 금지\n"
                f"→ 오늘 매수 0건"
            )
            return
        elif cross_regime == "CORRECTION":
            # 최대 1종목, capital_use 50%로 제한
            if len(candidates) > 1:
                candidates = candidates[:1]
            regime_rules["capital_use"] = min(regime_rules.get("capital_use", 1.0), 0.5)
            await _send(
                f"NIGHTWATCH: CORRECTION (리스크오프)\n"
                f"→ 최대 1종목, 사이즈 50%"
            )

        # 1.9) 동시호가 스캔 결과 반영 (P0 auction_scanner 연동)
        auction_filtered = 0
        auction_boosted = 0
        smart_money_count = 0
        try:
            # ── 스마트머니 맵 로드 (pattern_scan → DUAL_SURGE 90점↑ + 외인 100억↑) ──
            _smart_money = {}  # {code: {score, foreign_amt, name}}
            _ps_dir = Path(__file__).parent.parent / "data_store" / "learning" / "pattern_scan"
            if _ps_dir.exists():
                # 어제 날짜 패턴스캔 (전일 장마감 후 생성됨)
                _ps_files = sorted(_ps_dir.glob("*.json"), reverse=True)
                if _ps_files:
                    try:
                        _ps_data = json.loads(_ps_files[0].read_text(encoding="utf-8"))
                        for item in _ps_data.get("items", []):
                            if (item.get("pattern") == "DUAL_SURGE"
                                    and item.get("score", 0) >= 90):
                                _ent = item.get("entities_today", {})
                                _foreign = abs(float(_ent.get("foreign", 0) or 0))
                                if _foreign >= 100:  # 외인 100억↑
                                    _smart_money[item["code"]] = {
                                        "score": item["score"],
                                        "foreign": _foreign,
                                        "name": item.get("name", item["code"]),
                                    }
                        if _smart_money:
                            logger.info(f"[스마트머니] {len(_smart_money)}종목 감지: "
                                        f"{', '.join(v['name'] for v in _smart_money.values())}")
                    except Exception as e:
                        logger.warning(f"pattern_scan 로드 실패: {e}")

            auc_path = Path(__file__).parent.parent / "data_store" / "auction_scan.json"
            if auc_path.exists():
                auc_data = json.loads(auc_path.read_text(encoding="utf-8"))
                auc_ts = auc_data.get("timestamp", "")
                # 오늘 날짜 스캔만 사용
                if date.today().strftime("%Y-%m-%d") in auc_ts:
                    auc_map = {}
                    for ar in auc_data.get("results", []):
                        auc_map[ar.get("code", "")] = ar

                    new_candidates = []
                    for c in candidates:
                        code = c["code"]
                        auc = auc_map.get(code)
                        if not auc:
                            new_candidates.append(c)
                            continue

                        sig = auc.get("signal", "NORMAL")
                        is_smart = code in _smart_money

                        # 갭다운 위험 → 제외 (스마트머니라도 폭락은 제외)
                        if sig == "GAP_DOWN_DANGER":
                            logger.info(f"[동시호가] {c['name']} 폭락({auc.get('change_rate', 0):+.1f}%) → 제외")
                            auction_filtered += 1
                            continue

                        # 갭다운 → 보류 경고 (제외하진 않되 점수 감소)
                        if sig == "GAP_DOWN":
                            c["total_score"] = c.get("total_score", 0) - 15
                            c["auction_warning"] = f"갭다운 {auc.get('change_rate', 0):+.1f}%"
                            logger.info(f"[동시호가] {c['name']} 갭다운 → 점수 -15")

                        # 갭업 → 스마트머니 면제 or 추격 주의
                        elif sig == "GAP_UP_STRONG":
                            if is_smart:
                                sm = _smart_money[code]
                                c["total_score"] = c.get("total_score", 0) + 10
                                c["smart_money"] = True
                                c["auction_warning"] = (
                                    f"스마트머니 추격 (외인{sm['foreign']:.0f}억 "
                                    f"DUAL_SURGE {sm['score']}점)"
                                )
                                smart_money_count += 1
                                auction_boosted += 1
                                logger.info(
                                    f"[스마트머니] {c['name']} 강갭업이지만 "
                                    f"DUAL_SURGE {sm['score']}점+외인{sm['foreign']:.0f}억 "
                                    f"→ 감점면제 + 보너스 +10"
                                )
                            else:
                                c["total_score"] = c.get("total_score", 0) - 10
                                c["auction_warning"] = f"강갭업 {auc.get('change_rate', 0):+.1f}% (추격주의)"
                                logger.info(f"[동시호가] {c['name']} 강갭업 → 점수 -10 (추격주의)")

                        # 거래폭발 + 매수우위 → 보너스
                        elif sig == "VOL_BULL":
                            c["total_score"] = c.get("total_score", 0) + 10
                            auction_boosted += 1
                            logger.info(f"[동시호가] {c['name']} 거래폭발+매수 → 점수 +10")

                        # 갭업 (적정) → 보너스 (스마트머니면 추가)
                        elif sig == "GAP_UP":
                            bonus = 10 if is_smart else 5
                            c["total_score"] = c.get("total_score", 0) + bonus
                            if is_smart:
                                c["smart_money"] = True
                                smart_money_count += 1
                            auction_boosted += 1
                            logger.info(f"[동시호가] {c['name']} 갭업 → 점수 +{bonus}"
                                        f"{' (스마트머니)' if is_smart else ''}")

                        new_candidates.append(c)

                    candidates = new_candidates
                    # 점수 재정렬
                    candidates.sort(key=lambda x: x.get("total_score", 0), reverse=True)

                    if auction_filtered or auction_boosted:
                        _sm_tag = f" / 스마트머니 {smart_money_count}" if smart_money_count else ""
                        await _send(
                            f"📊 동시호가 반영: 제외 {auction_filtered} / 보너스 {auction_boosted}{_sm_tag}"
                        )
        except Exception as e:
            logger.warning(f"동시호가 연동 실패 (무시): {e}")

        # 2) 추천 없으면 사전감지 폴백
        if not candidates:
            try:
                await _send("추천 없음 → 사전감지 실행...")
                from data.premove_scanner import scan_premove
                premove = await asyncio.to_thread(scan_premove, 5)
                if premove:
                    candidates = [
                        {
                            "code": c.code, "name": c.name,
                            "total_score": c.premove_score,
                            "entry": int(c.entry), "sl": int(c.sl),
                            "tp": int(c.tp2),
                            "tp1_quick": int(c.tp1_quick),
                            "source": "premove",
                        }
                        for c in premove
                    ]
            except Exception as e:
                logger.error(f"사전감지 실패: {e}")

        # 3) 그래도 없으면 swing_candidates 폴백
        if not candidates:
            candidates = self._load_swing_candidates()

        if not candidates:
            await _send("매수 후보 없음 - 오늘 관망")
            return

        # ── 검증 모드 분기 (5/18~5/19 1주 실전 검증, 사장님 제안) ──
        # ON 시 max_pos 무시 + 전체 1주씩 시장가 매수 + 15:25 강제 청산
        # 5/18 사장님 결정: 자동매매 ON/OFF 무관하게 작동 (자금 신중성 + 검증만)
        try:
            from data import verification_mode as _vm
            if _vm.is_active():
                await _send(
                    f"🧪 [검증모드 ACTIVE] {_vm.get_config_summary()['start_date']} ~ "
                    f"{_vm.get_config_summary()['end_date']}\n"
                    f"전체 {len(candidates)}종목 1주씩 시장가 매수 진행..."
                )
                await self._decide_verification(candidates)
                return
        except Exception as _vm_e:
            logger.warning(f"[verification] 분기 실패 (무시, 일반 모드 진행): {_vm_e}")

        if not self.is_running:
            lines = ["📋 매수 후보 (자동매매 OFF - 리포트만)"]
            for c in candidates:
                _aw = c.get("auction_warning", "")
                _aw_tag = f"\n    ⚠️ {_aw}" if _aw else ""
                lines.append(
                    f"  {c['name']}({c['code']}) 점수:{c['total_score']:.0f} "
                    f"SL:{c['sl']:,} TP:{c['tp']:,}{_aw_tag}"
                )
                # 프리미엄 레벨 1줄 추가
                try:
                    from strategies.premium_levels import format_telegram_levels
                    pl_line = format_telegram_levels(
                        c['code'], c['name'], c.get('entry') or c.get('close', 0)
                    )
                    if pl_line:
                        lines.append(f"    📐 {pl_line}")
                except Exception:
                    pass
                # Equal Level 1줄 추가
                try:
                    from strategies.equal_level_detector import format_telegram_eq
                    eq_line = format_telegram_eq(
                        c['code'], c['name'], c.get('entry') or c.get('close', 0)
                    )
                    if eq_line:
                        lines.append(f"    🔲 {eq_line}")
                except Exception:
                    pass
                # 갭 레벨 1줄 추가
                try:
                    from strategies.gap_support import format_telegram_gap
                    gap_line = format_telegram_gap(
                        c['code'], c['name'], c.get('entry') or c.get('close', 0)
                    )
                    if gap_line:
                        lines.append(f"    📊 {gap_line}")
                except Exception:
                    pass
            await _send("\n".join(lines))
            return

        # ── 리스크 게이트 체크 ──
        risk_ok, risk_reason = self.check_risk_gate()
        if not risk_ok:
            await _send(f"⛔ 리스크 게이트 차단\n{risk_reason}")
            return

        # ── 진입감시 대기열에 등록 (즉시 매수 X) ──
        bot_conf = self.config.get("bot", {})
        max_pos = bot_conf.get("max_auto_positions", 2)

        # ── BRAIN 교차 신호 — 진입 캡 + 관망 차단 (FIX-02) ──
        brain_alloc = self._load_brain_allocation()
        brain_pct = brain_alloc.get("position_size_pct", 100)
        brain_regime = brain_alloc.get("effective_regime", "")

        # 관망(0%): 완전 매수 차단
        if brain_alloc.get("_block_all_buys"):
            await _send(
                f"🚫 BRAIN 관망모드 — 신규 매수 전면 중단\n"
                f"  {brain_alloc.get('overall_verdict', '')}"
            )
            return

        # 4/8: 방어/경계 레짐 → 최대 1종목 (학습: 방어장에서 2종목 동시진입 시 승률 급락)
        if brain_regime in ("CAUTIOUS", "DEFENSIVE", "방어", "경계"):
            original_max = max_pos
            max_pos = min(max_pos, 1)
            if max_pos < original_max:
                await _send(
                    f"🛡️ BRAIN [{brain_regime}모드]"
                    f" — 방어레짐 진입 캡 {original_max}→{max_pos}"
                )

        # 포지션 수 캡 (cross_signal)
        cross = brain_alloc.get("cross_signal", {})
        if cross.get("max_positions_cap") is not None:
            cap_ratio = cross["max_positions_cap"]
            original_max = max_pos
            max_pos = max(1, int(max_pos * cap_ratio)) if cap_ratio > 0 else 0
            if max_pos < original_max:
                await _send(
                    f"🛡️ BRAIN [{brain_regime}모드]"
                    f" — 진입 캡 {original_max}→{max_pos}"
                )

        bal = self.trader.fetch_balance()
        current_positions = len(bal.get("positions", [])) if bal.get("success") else 0
        slots = max_pos - current_positions

        if slots <= 0:
            await _send(f"보유 {current_positions}종목 - 추가 매수 불가")
            return

        # ── 연말 계절성 필터 (12/15~1/5): 사이즈 축소 ──
        now = datetime.now()
        is_yearend = (now.month == 12 and now.day >= 15) or (now.month == 1 and now.day <= 5)

        # ── 매수 금액: 실제 잔고 기반 동적 계산 ──
        # CORTEX 체제 기반 자본사용 배수 적용
        available_cash = bal.get("cash", 0) if bal.get("success") else 0
        # 5/19 D-Day — 단타봇 할당 자금 한도 적용 (사장님 위임, 퀀트봇 자금 별도)
        scalper_budget = bot_conf.get("scalper_budget", 0)
        if scalper_budget > 0 and available_cash > scalper_budget:
            _original_cash = available_cash
            available_cash = scalper_budget
            _quant_reserved = _original_cash - scalper_budget
            logger.info(
                f"[자금정책] 단타봇 한도 적용: {_original_cash:,}원 → {available_cash:,}원 "
                f"(퀀트봇 자금 {_quant_reserved:,}원 별도 분리)"
            )
            await _send(
                f"💰 단타봇 자금 정책\n"
                f"  · 전체 현금: {_original_cash:,}원\n"
                f"  · 단타봇 한도: {scalper_budget:,}원\n"
                f"  · 퀀트봇 별도: {_quant_reserved:,}원"
            )
        # ── 큰형(퀀트봇) advisory 게이트 (5/19 wire-up 사장님 08:09 결정) ──
        # 매수 진입 직전 quant_bot_advisory 다중 type 조회 → regime + 시장강도 + 인버스 종합
        # 우선순위: LEADING (긴급 수동) > SNAPSHOT/ADVICE (최신 스냅) > MORNING_BRIEFING (장전)
        # 차단 기준:
        #   - regime BEARISH/PANIC → swing 신규 매수 차단
        #   - market_strength_avg >= 95 (시장 약세 매우 강력) → BEARISH로 격상 차단
        #   - regime CAUTION 또는 market_strength_avg >= 90 → 예산 30% 축소
        #   - inverse_etf_strength >= 120 → 경고
        try:
            from utils.quant_advisory_subscriber import fetch_latest_advisory
            # [5/19 10:10 수정] 오늘 데이터 절대 우선 → 모두 None이면 어제 fallback
            # 기존: LEADING이 어제만 있어도 SNAPSHOT(오늘)보다 우선 잡힘 → 잘못된 신호
            adv = None
            # Pass 1: 오늘 (KST) 데이터 — type 우선순위 LEADING > SNAPSHOT > ADVICE > MORNING
            for _mt in ('LEADING', 'SNAPSHOT', 'ADVICE', 'MORNING_BRIEFING'):
                adv = fetch_latest_advisory(msg_type=_mt, fallback_to_yesterday=False)
                if adv:
                    break
            # Pass 2: 그래도 없으면 어제 fallback (type 우선순위)
            if not adv:
                for _mt in ('LEADING', 'SNAPSHOT', 'ADVICE', 'MORNING_BRIEFING'):
                    adv = fetch_latest_advisory(msg_type=_mt, fallback_to_yesterday=True)
                    if adv:
                        break

            if adv:
                regime = (adv.get('market_regime') or '').upper()
                inverse = float(adv.get('inverse_etf_strength') or 0)
                strength = float(adv.get('market_strength_avg') or 0)
                adv_date = adv.get('advisory_date', '?')
                adv_time = adv.get('advisory_time', '?')
                adv_type = adv.get('msg_type', '?')
                await _send(
                    f"🤖 큰형 advisory 게이트\n"
                    f"  · {adv_date} {adv_time} [{adv_type}]\n"
                    f"  · regime: {regime or 'N/A'}\n"
                    f"  · 시장 강도: {strength:.1f} (90+ 약세 / 95+ 강력약세)\n"
                    f"  · 인버스 강도: {inverse:.1f} (120+ 약세 베팅 우세)"
                )

                # 게이트 ①: regime 기반 PANIC만 차단 (사장님 5/20 결정)
                # [자비스 정신 v2 원칙] BEARISH는 어필/경고만, 자비스 자율 판단으로 진입.
                # PANIC만 시장 자체 붕괴 신호 → 완전 차단.
                # market_strength_avg는 큰형 강력포착 TOP 9의 체결강도 평균
                # (100=균형, 80~90=매도 우세=약세, 110+=매수 우세=강세)
                if regime == 'PANIC':
                    await _send(
                        f"🛑 큰형 PANIC → 단타봇 신규 swing 매수 전면 차단\n"
                        f"  · 기존 5종목 SL/TP는 정상 작동\n"
                        f"  · PANIC은 시장 자체 붕괴 신호"
                    )
                    logger.warning(f"[quant_advisory] regime=PANIC → swing 매수 차단")
                    return
                if regime == 'BEARISH':
                    await _send(
                        f"⚠️ 큰형 BEARISH 경고 — 자비스 자율 판단 진행\n"
                        f"  · 매수는 계속, 자비스가 강한 종목만 선별"
                    )
                    logger.info(f"[quant_advisory] regime=BEARISH (경고만, 통과)")

                # 게이트 ②: CAUTION/CAUTION_TO_NEUTRAL → 예산 30% 축소
                # 강도 80 이하 = 매도 매우 우세 → 30% 축소
                # 인버스 강도 120+ = 시장 약세 베팅 우세 → 30% 축소
                cap_reasons = []
                if regime in ('CAUTION', 'CAUTION_TO_NEUTRAL'):
                    cap_reasons.append(f"regime={regime}")
                if 0 < strength <= 80:
                    cap_reasons.append(f"강도 {strength:.1f}(매도 우세)")
                if inverse >= 120:
                    cap_reasons.append(f"인버스 {inverse:.1f}(약세 베팅)")
                if cap_reasons:
                    _old_cash = available_cash
                    available_cash = int(available_cash * 0.7)
                    await _send(
                        f"⛔ 큰형 신호 [{' + '.join(cap_reasons)}] → 예산 30% 축소: "
                        f"{_old_cash:,}원 → {available_cash:,}원"
                    )
                    logger.info(
                        f"[quant_advisory] cap_30%: reasons={cap_reasons} | "
                        f"cash {_old_cash:,} → {available_cash:,}"
                    )
            else:
                await _send("🤖 큰형 advisory: 데이터 없음 (5/19 오전 큰형 작업 중) — 1주 모드 단독 가동")
                logger.info("[quant_advisory] no data — proceed with 1-share mode")
        except Exception as e:
            logger.warning(f"[quant_advisory] 게이트 실패 (continue): {e}")
            try:
                await _send(f"⚠️ 큰형 advisory 게이트 실패 (continue): {e}")
            except Exception:
                pass

        num_targets = min(len(candidates), slots)
        cash_reserve_ratio = self.config.get("risk", {}).get("min_cash_ratio", 0.10)
        capital_use = regime_rules.get("capital_use", 1.0)
        if is_yearend:
            capital_use *= 0.5  # 연말 시즌: 50% 축소
            await _send("연말 계절성 필터: 사이즈 50% 축소 (12/15~1/5)")
        usable_cash = int(available_cash * (1 - cash_reserve_ratio) * capital_use)

        # FIX-02: BRAIN 비중 캡 — pct 기반 자금 조절
        if brain_pct < 100 and brain_alloc:
            old_usable = usable_cash
            usable_cash = int(usable_cash * brain_pct / 100)
            await _send(
                f"BRAIN 비중 적용: {brain_regime} {brain_pct}%\n"
                f"  가용자금 {old_usable:,}원 → {usable_cash:,}원"
            )

        # ── 이벤트 리스크 → 스윙 예산 축소 ──
        try:
            from data.event_calendar import get_event_risk_for_recommendation
            ev_risk = get_event_risk_for_recommendation(date.today())
            _rl = ev_risk.get("risk_level", "LOW")
            if self._cfo:
                _mult = self._cfo.get_event_multiplier(_rl, "swing")
            else:
                _mult = {"EXTREME": 0.5, "HIGH": 0.7}.get(_rl, 1.0)
            if _mult < 1.0:
                usable_cash = int(usable_cash * _mult)
                await _send(f"{_rl} 이벤트 → 스윙 예산 x{_mult:.1f}")
        except Exception:
            pass

        buy_amount = usable_cash // num_targets if num_targets > 0 else 0

        if buy_amount < 50000:
            await _send(f"가용 현금 부족: {available_cash:,}원 → 매수 불가")
            return

        await _send(
            f"자금 배분: 현금 {available_cash:,}원 "
            f"→ {num_targets}종목 x {buy_amount:,}원"
            + (f" (BRAIN {brain_regime} {brain_pct}%)" if brain_pct < 100 else "")
        )

        # ── Trade Object PAPER 로깅 ──
        try:
            from data.trade_object import load_trade_objects
            trade_objs = load_trade_objects()
            if trade_objs:
                to_map = {t.code: t for t in trade_objs}
                paper_lines = ["[PAPER] Trade Object R:R 분석:"]
                for c in candidates[:slots]:
                    to = to_map.get(c["code"])
                    if to:
                        tag = to.rr_verdict
                        paper_lines.append(
                            f"  {to.name} R:R {to.rr_ratio:.2f} [{tag}] "
                            f"목표 {to.target_price:,} 손절 {to.stop_loss:,}"
                        )
                        # Trade Object의 SL/TP로 보강 (기존 값 유지하되 로그)
                        if tag == "REJECT":
                            paper_lines[-1] += " → PAPER REJECT (매수 진행)"
                await _send("\n".join(paper_lines))
        except Exception as e:
            logger.warning(f"Trade Object PAPER 로드 실패: {e}")

        registered = 0
        skipped = 0
        for c in candidates[:slots]:
            code = c["code"]
            if code in self._positions or code in self._entry_watch:
                continue

            # 진입 필터 (차트 기반 사전 체크)
            size_mult = 1.0
            try:
                from data.swing_indicators import check_entry_filter
                entry_check = await asyncio.to_thread(
                    check_entry_filter, code, c["name"]
                )
                if not entry_check["pass"]:
                    skipped += 1
                    await _send(f"⛔ 차트 거부: {c['name']} - {entry_check['reason']}")
                    continue
                size_mult = entry_check["size_mult"]
            except Exception as e:
                logger.warning(f"진입필터 오류 {code}: {e}")

            # 정보봇 컨텍스트 entry_filter (auto_trade=true 시만)
            # 외국인 streak / 섹터 매크로 / 프로그램매매 비대칭 통합 차단 판단
            if self._ws_enabled:
                try:
                    from utils.jgis_context import check_entry_blocked
                    sector = c.get("sector", "")
                    market = c.get("market", "")  # 'KOSPI' or 'KOSDAQ'
                    jgis_check = await asyncio.to_thread(
                        check_entry_blocked, code, sector, market
                    )
                    if jgis_check["blocked"]:
                        skipped += 1
                        reasons = " / ".join(jgis_check["reasons"])
                        await _send(f"⛔ 정보봇 차단: {c['name']} - {reasons}")
                        continue
                    if jgis_check["warnings"]:
                        # 경고만 — 보수적 모드 (size_mult 70%로 축소)
                        size_mult *= 0.7
                        warns = " / ".join(jgis_check["warnings"])
                        await _send(f"⚠️ 정보봇 경고: {c['name']} [매수비중 70%] - {warns}")
                except Exception as e:
                    logger.warning(f"[JGIS] entry_filter 오류 {code}: {e}")

            actual_amount = int(buy_amount * size_mult)

            # 분할매수 설정 (config split_count, 기본 3)
            split_count = self.config.get("risk", {}).get("split_count", 3)

            # war_relay: 긴 감시 시간 + 눌림목 진입 대기
            is_war_relay = c.get("source") == "war_relay"
            watch_max_checks = 420 if is_war_relay else 60  # 3.5시간 vs 30분

            # 진입감시 대기열에 등록
            self._entry_watch[code] = {
                "name": c["name"],
                "buy_amount": actual_amount,
                "sl": c["sl"],
                "tp": c["tp"],
                "tp1_quick": c.get("tp1_quick", c["tp"]),
                "score": c["total_score"],
                "source": c.get("source", "swing"),
                "confidence": c.get("confidence", ""),
                "size_mult": size_mult,
                # 실시간 감시 상태
                "prev_close": c["entry"],  # 전일 종가 (기준가)
                "open_price": 0,           # 시가 (첫 체크에서 기록)
                "min_price": 999999999,    # 장중 저가
                "max_price": 0,            # 장중 고가
                "checks": 0,              # 관찰 횟수 (30초마다 +1)
                "max_checks": watch_max_checks,
                "ai_scores": [],          # 최근 AI 점수 기록
                "registered_at": datetime.now().strftime("%H:%M"),
                "entry_triggered": False,  # 진입 조건 충족 여부
                # ── 분할매수 상태 ──
                "split_count": split_count,    # 총 분할 횟수 (기본 3)
                "split_done": 0,               # 완료된 분할 횟수
                "split_amount": actual_amount // split_count,  # 1회 매수금액
                "last_split_check": 0,         # 마지막 분할 매수 시 checks 값
                "split_interval": 10,          # 분할 간격 (10회 = 5분)
                # ── MOMENTUM 레짐 ──
                "regime": c.get("regime", "NORMAL"),
                "regime_score": c.get("regime_score", 0.0),
                # ── ETF Phase 3 ──
                "is_etf": c.get("is_etf", False),
                "etf_category": c.get("etf_category", ""),
                "holding_days": c.get("holding_days", 10),
                # ── 스마트머니 (DUAL_SURGE 90점↑ + 외인100억↑) ──
                "smart_money": c.get("smart_money", False),
                "auction_warning": c.get("auction_warning", ""),
            }
            registered += 1
            # Phase 3-B: WebSocket 자동 구독 (auto_trade 활성 시만)
            try:
                await self._subscribe_entry_watch(code)
            except Exception:
                pass  # 구독 실패해도 REST polling fallback 동작

        lines = [f"👁 장 시작 - {registered}종목 실시간 감시 시작"]
        for code, w in self._entry_watch.items():
            etf_tag = " [ETF]" if w.get("is_etf") else ""
            mtm_tag = " [MTM]" if w.get("regime") == "MOMENTUM" else ""
            sm_tag = " [스마트머니]" if w.get("smart_money") else ""
            extra_tag = sm_tag + etf_tag + mtm_tag
            _aw = w.get("auction_warning", "")
            _aw_line = f"\n    ⚠️ {_aw}" if _aw else ""
            lines.append(
                f"  📡 {w['name']}({code}){extra_tag} 점수:{w['score']:.0f} "
                f"금액:{w['buy_amount']:,}원 "
                f"({w['split_count']}분할×{w['split_amount']:,}원){_aw_line}"
            )
        if skipped:
            lines.append(f"  ⛔ {skipped}종목 차트필터 거부")
        lines.append(f"\n30초마다 KIS API로 가격/거래량/체결강도 관찰 중...")
        lines.append(f"진입 조건 충족 시 자동 매수 (최대 30분 관찰)")
        await _send("\n".join(lines))

        # 수동 모드 종목 알림 (자동매수 안 함)
        if manual_picks:
            manual_lines = ["📋 수동 종목 (알림만, 자동매수 안 함):"]
            for mp in manual_picks:
                manual_lines.append(f"   {mp['name']}({mp['code']}) — 직접 진입 판단")
            await _send("\n".join(manual_lines))

    # _morning_momentum 삭제됨 (PF 0.70 비활성화 → 데드코드 정리 2026-03-14)

    # ─────────────────────────────────────────────
    # Phase 3-B: WebSocket 실시간 시세 (하이브리드 — REST fallback 유지)
    # ─────────────────────────────────────────────
    async def start_websocket_monitor(self):
        """WebSocket 백그라운드 시작 (auto_trade=true 시만).

        Why: 30초 polling은 자동매매 진입 평가 지연 큼. WebSocket으로 체결 이벤트
        받으면 캐시에 즉시 저장. _check_entry_watch가 캐시 5초 이내면 REST 호출
        대신 캐시 사용 → 빠른 평가 + KIS API 호출 절감.
        """
        if not self._ws_enabled:
            logger.info("[WS-Auto] auto_trade=false → WebSocket 비활성")
            return
        if self._ws_task and not self._ws_task.done():
            logger.info("[WS-Auto] 이미 실행 중")
            return

        try:
            from utils.kis_websocket import KISWebSocketClient
            self._ws_client = KISWebSocketClient(max_subscriptions=40)
            # [5/19 사장님 10:45 fix] _entry_watch + 보유 포지션 일괄 구독
            # 기존 버그: _entry_watch만 구독 → 재시작 후 5종목 시세 안 옴 → "데이터 피드 중단 5회"
            # 5/14 14:36 / 5/19 10:10 / 10:43 재시작에서 동일 문제 반복.
            watch_codes = set(self._entry_watch.keys()) if self._entry_watch else set()
            pos_codes = set(self._positions.keys()) if self._positions else set()
            existing = list(watch_codes | pos_codes)
            if existing:
                await self._ws_client.subscribe(existing, self._on_websocket_tick)
            self._ws_task = asyncio.create_task(self._ws_client.run_forever())
            logger.info(
                f"[WS-Auto] 시작 — 초기 구독 {len(existing)}종목 "
                f"(감시={len(watch_codes)} 보유={len(pos_codes)})"
            )
        except Exception as e:
            logger.warning(f"[WS-Auto] 시작 실패 (REST 폴백 사용): {e}")
            self._ws_client = None
            self._ws_task = None

    async def _on_websocket_tick(self, code, price, volume, ts, fields):
        """WebSocket 체결 콜백 — 캐시 저장만.

        _check_entry_watch가 캐시 5초 이내면 우선 사용. 실제 6조건 평가는
        기존 30초 polling 유지 (안정성).
        """
        self._ws_cache[code] = {
            "price": price,
            "volume": volume,
            "ts": ts,
            "updated_at": time.time(),
        }

    async def _subscribe_entry_watch(self, code):
        """진입감시 등록 시 WebSocket 자동 구독."""
        if not (self._ws_client and self._ws_enabled):
            return
        try:
            await self._ws_client.subscribe([code], self._on_websocket_tick)
        except Exception as e:
            logger.debug(f"[WS-Auto] {code} 구독 실패 (무시): {e}")

    async def _unsubscribe_entry_watch(self, code):
        """진입감시 해제 시 WebSocket 자동 해제."""
        if not (self._ws_client and self._ws_enabled):
            return
        try:
            await self._ws_client.unsubscribe([code])
        except Exception as e:
            logger.debug(f"[WS-Auto] {code} 해제 실패 (무시): {e}")
        self._ws_cache.pop(code, None)

    def _get_ws_price(self, code, max_age_sec=5.0):
        """WebSocket 캐시 5초 이내 가격 (없거나 만료면 None).

        _check_entry_watch에서 fetch_price 호출 전에 먼저 시도.
        """
        c = self._ws_cache.get(code)
        if not c:
            return None
        if time.time() - c["updated_at"] > max_age_sec:
            return None
        return c

    async def stop_websocket_monitor(self):
        """WebSocket 종료 (봇 종료 시)."""
        if self._ws_task:
            try:
                if self._ws_client:
                    await self._ws_client.stop()
                self._ws_task.cancel()
            except Exception:
                pass
            self._ws_task = None
            self._ws_client = None
            self._ws_cache.clear()
            logger.info("[WS-Auto] 종료")

    async def _decide_verification(self, candidates: list):
        """검증 모드 진입 (사장님 제안 2026-05-17, 1주 실전 수익률 검증).

        모든 후보를 1주씩 시장가 즉시 매수. SL/TP/6조건/max_pos 모두 우회.
        15:25 강제 청산은 trading_coo의 verification_close 스케줄에서.

        Args:
            candidates: morning_recommendation 추천 리스트
                       [{code, name, total_score, entry, sources, ...}]
        """
        from data import verification_mode as _vm

        async def _send(text):
            if self._send_alert:
                try:
                    await self._send_alert(text)
                except Exception:
                    pass

        bought = []
        failed = []
        skipped = []
        budget_skipped = []
        grade_filtered = []  # ★ 5/20 fix #10 ★

        active_codes = {p["code"] for p in _vm.get_active_positions()}

        # ★ 5/20 fix #10: signal_type AVOID + grade D/F 필터 ★
        # 5/20 사고: morning_recommendation이 grade D + F 2종만 추천했는데
        # 검증모드가 필터 없이 1주씩 매수 → 손실. signal_type=AVOID 종목 차단.
        _ALLOWED_GRADES = {"A", "B", "C", "STRONG", "AAA", "AA"}
        _BLOCKED_SIGNAL_TYPES = {"AVOID"}
        _MIN_SCORE = 30.0  # total_score 임계

        _filtered_candidates = []
        for c in candidates:
            grade = str(c.get("grade", "")).upper()
            sig = str(c.get("signal_type", "")).upper()
            score = float(c.get("total_score") or c.get("final_score", 0))
            if sig in _BLOCKED_SIGNAL_TYPES:
                grade_filtered.append(f"{c.get('name', c.get('code'))}(AVOID)")
                continue
            if grade and grade not in _ALLOWED_GRADES:
                grade_filtered.append(f"{c.get('name', c.get('code'))}(grade={grade})")
                continue
            if score < _MIN_SCORE:
                grade_filtered.append(f"{c.get('name', c.get('code'))}(score={score:.0f}<{_MIN_SCORE})")
                continue
            _filtered_candidates.append(c)
        candidates = _filtered_candidates
        if grade_filtered:
            await _send(
                f"🚫 [검증모드 사전 필터] {len(grade_filtered)}종 차단 "
                f"(AVOID/grade D,F/score<{_MIN_SCORE})\n"
                f"  · {', '.join(grade_filtered[:5])}"
                + (f" 외 {len(grade_filtered)-5}" if len(grade_filtered) > 5 else "")
            )
        if not candidates:
            await _send("🟡 [검증모드] 필터 후 매수 후보 0종 — 노옵")
            return

        # Critical 1: KIS 잔고 안전 한도 (검증 모드 예산 = 가용 현금의 30%)
        # 13종목 + 장중 추가 최대 10종목 = 23종목 1주씩, 평균 30,000원/주 = 약 70만원
        try:
            bal = await asyncio.to_thread(self.trader.fetch_balance)
            available_cash = bal.get("cash", 0) if bal.get("success") else 0
            verification_budget = int(available_cash * 0.30)
            await _send(
                f"💰 [검증모드 예산] 가용 현금 {available_cash:,}원 × 30% = {verification_budget:,}원"
            )
        except Exception as _bal_e:
            logger.warning(f"[verification] 잔고 조회 실패 — 예산 한도 X: {_bal_e}")
            verification_budget = 999_999_999  # 한도 비활성 (조회 실패 시)

        budget_used = 0

        for c in candidates:
            code = c["code"]
            name = c["name"]

            # 중복 매수 차단 (_positions + _entry_watch + _vm 모두 체크)
            if (code in active_codes or code in self._positions
                    or code in getattr(self, "_entry_watch", {})):
                skipped.append(name)
                continue

            # Critical 1: 예산 한도 체크 — 추정가 (entry 또는 close)로 사전 차단
            est_price = float(c.get("entry") or c.get("close", 0))
            if budget_used + est_price > verification_budget:
                budget_skipped.append(name)
                continue

            try:
                # 1주 시장가 매수
                resp = await asyncio.to_thread(self.trader.buy_market, code, 1)
                if resp.get("success"):
                    # Critical 2: KIS 응답에는 avg_price/price 없음 → 매수 직후 fetch_price로 체결가 조회
                    # 폴백 chain: fetch_price → entry → close
                    buy_price = 0.0
                    try:
                        price_resp = await asyncio.to_thread(self.trader.fetch_price, code)
                        if price_resp and price_resp.get("success"):
                            buy_price = float(price_resp.get("price", 0))
                    except Exception:
                        pass
                    if not buy_price:
                        buy_price = float(c.get("entry") or c.get("close", 0))
                    budget_used += buy_price
                    signal_tags = (
                        c.get("sources") or c.get("key_reasons") or ""
                    )
                    if isinstance(signal_tags, list):
                        signal_tags = " ".join(str(t) for t in signal_tags)
                    _vm.record_buy(
                        code=code,
                        name=name,
                        buy_price=buy_price,
                        signal_tags=str(signal_tags),
                        final_score=float(
                            c.get("total_score") or c.get("final_score", 0)
                        ),
                    )
                    # trade_journal에도 적재 (FLOWX 대시보드 데이터 소스)
                    try:
                        from data import trade_journal as _tj
                        _tj.log_buy(
                            code=code, name=name, qty=1, price=buy_price,
                            source="verification",
                            signal_tags=str(signal_tags),
                            final_score=float(c.get("total_score") or c.get("final_score", 0)),
                            order_no=resp.get("order_no") or resp.get("ODNO"),
                        )
                    except Exception as _tj_e:
                        logger.warning(f"[trade_journal] verification buy 적재 실패: {_tj_e}")
                    # ★ scalper_bot_feedback 양방향 채널 적재 (5/18 사장님 지적 즉시 통합) ★
                    try:
                        from utils.scalper_journal_hooks import hook_buy_execution
                        hook_buy_execution(
                            ticker=code, name=name, price=int(buy_price), qty=1,
                            grade=str(c.get("grade", "STRONG")),
                            source="verification",
                            extra_reason=f"signal_tags={signal_tags} score={c.get('total_score', 0):.0f}",
                        )
                    except Exception as _h_e:
                        logger.debug(f"[scalper_hooks] buy 적재 실패 (무시): {_h_e}")
                    # _positions 등록 (Eye/Guardian 등 다른 모니터가 충돌 안 하도록 source 명시)
                    self._positions[code] = {
                        "name": name,
                        "qty": 1,
                        "buy_price": buy_price,
                        "source": "verification",
                    }
                    bought.append(f"{name}({buy_price:,.0f})")
                else:
                    failed.append(f"{name}({str(resp.get('msg', '?'))[:25]})")
            except Exception as e:
                failed.append(f"{name}({type(e).__name__})")
                logger.warning(f"[verification] 매수 실패 [{code}] {name}: {e}")

        # 결과 알림
        lines = [
            f"🧪 [검증모드 매수 결과]",
            f"  ✅ 성공: {len(bought)}/{len(candidates)}종목 (예산 {budget_used:,}원 사용)",
        ]
        if bought:
            shown = ", ".join(bought[:8])
            if len(bought) > 8:
                shown += f" 외 {len(bought) - 8}"
            lines.append(f"  매수: {shown}")
        if failed:
            lines.append(f"  ❌ 실패 {len(failed)}: " + ", ".join(failed[:5]))
        if skipped:
            lines.append(f"  ⏭ 중복 스킵 {len(skipped)}: " + ", ".join(skipped[:5]))
        if budget_skipped:
            lines.append(f"  💸 예산 초과 스킵 {len(budget_skipped)}: " + ", ".join(budget_skipped[:5]))
        lines.append("")
        lines.append("15:25 강제 청산 + 15:35 정산 리포트 자동 발송")
        await _send("\n".join(lines))

    async def intraday_verification_scan_and_buy(self):
        """장중 멀티시그널 신규 진입 (사장님 결정 2026-05-17, 검증 모드 v2).

        09:05~14:00 5분마다 호출. 체결강도+거래량+tipping 3개 동시 충족 종목을
        1주씩 즉시 시장가 매수. 일일 최대 10종목 추가.

        검증 모드 OFF / 시간 외 / 후보 없음 → 노옵.
        """
        from data import verification_mode as _vm

        async def _send(text):
            if self._send_alert:
                try:
                    await self._send_alert(text)
                except Exception:
                    pass

        if not _vm.is_active():
            return

        # 일일 추가 매수 한도 체크 (verification_log에서 source='intraday' 카운트)
        try:
            from utils.supabase_sql import query_one
            row = query_one(
                """
                SELECT COUNT(*) AS n FROM scalper_trade_journal
                WHERE event_date = CURRENT_DATE
                  AND source = 'intraday_scan'
                  AND event_type = 'buy'
                """
            )
            today_intraday_count = row["n"] if row else 0
        except Exception:
            today_intraday_count = 0

        if today_intraday_count >= 10:
            logger.info(f"[intraday] 일일 최대 10종목 도달 ({today_intraday_count}) — 스킵")
            return

        # ── 큰형(퀀트봇) advisory 게이트 (intraday 검증 모드 v2 wire-up) ──
        # 5분 반복 호출 — BEARISH/PANIC 시 장중 신규 매수 차단
        # (regime 신호만 사용. strength/inverse는 보강 정보로만 로깅)
        try:
            from utils.quant_advisory_subscriber import fetch_latest_advisory
            # [5/19 10:10 수정] 오늘 데이터 절대 우선 → 모두 None이면 어제 fallback
            _adv = None
            for _mt in ('LEADING', 'SNAPSHOT', 'ADVICE', 'MORNING_BRIEFING'):
                _adv = fetch_latest_advisory(msg_type=_mt, fallback_to_yesterday=False)
                if _adv:
                    break
            if not _adv:
                for _mt in ('LEADING', 'SNAPSHOT', 'ADVICE', 'MORNING_BRIEFING'):
                    _adv = fetch_latest_advisory(msg_type=_mt, fallback_to_yesterday=True)
                    if _adv:
                        break
            if _adv:
                _regime = (_adv.get('market_regime') or '').upper()
                # 사장님 5/20 결정: BEARISH는 경고만, PANIC만 차단 ([자비스 정신 v2])
                if _regime == 'PANIC':
                    await _send(
                        f"🛑 큰형 PANIC → 장중 검증 모드 신규 매수 차단 "
                        f"({_adv.get('advisory_date')} {_adv.get('advisory_time')})"
                    )
                    logger.warning(f"[quant_advisory/intraday] regime=PANIC → 차단")
                    return
                if _regime == 'BEARISH':
                    await _send(
                        f"⚠️ 큰형 BEARISH 경고 → 장중 자비스 자율 판단 진행"
                    )
                    logger.info(f"[quant_advisory/intraday] regime=BEARISH (경고만, 통과)")
                else:
                    logger.info(f"[quant_advisory/intraday] regime={_regime} 통과")
        except Exception as e:
            logger.warning(f"[quant_advisory/intraday] 게이트 실패 (continue): {e}")

        # 제외 종목: 보유 종목 + verification 보유 + 기존 morning_rec 후보
        excluded = set(self._positions.keys())
        try:
            for p in _vm.get_active_positions():
                excluded.add(p["code"])
        except Exception:
            pass

        try:
            from data.intraday_scanner import scan_intraday_signals
            candidates = await asyncio.to_thread(scan_intraday_signals, self.trader, excluded)
        except Exception as e:
            logger.warning(f"[intraday] 스캔 실패 (무시): {e}")
            return

        if not candidates:
            return  # quiet (5분마다 도는 거라 노이즈 방지)

        # 잔여 슬롯 (일일 10종목 한도 차감)
        slots = max(0, 10 - today_intraday_count)
        candidates = candidates[:slots]

        await _send(
            f"🚀 [장중 멀티시그널 진입] {len(candidates)}종목 발견 — 1주씩 매수 진행"
        )

        bought = []
        failed = []
        for c in candidates:
            code = c["code"]
            name = c["name"]
            try:
                resp = await asyncio.to_thread(self.trader.buy_market, code, 1)
                if resp.get("success"):
                    # KIS 응답에 체결가 없음 → fetch_price로 조회 (시장가라 ≈ 체결가)
                    buy_price = 0.0
                    try:
                        price_resp = await asyncio.to_thread(self.trader.fetch_price, code)
                        if price_resp and price_resp.get("success"):
                            buy_price = float(price_resp.get("price", 0))
                    except Exception:
                        pass
                    if not buy_price:
                        buy_price = float(c.get("current_price", 0))
                    _vm.record_buy(
                        code=code, name=name, buy_price=buy_price,
                        signal_tags=c["signal_tags"],
                        final_score=c.get("total_score", 0),
                    )
                    # trade_journal — source='intraday_scan'으로 구분
                    try:
                        from data import trade_journal as _tj
                        _tj.log_buy(
                            code=code, name=name, qty=1, price=buy_price,
                            source="intraday_scan",
                            signal_tags=c["signal_tags"],
                            final_score=c.get("total_score", 0),
                            order_no=resp.get("order_no") or resp.get("ODNO"),
                            note=f"intraday strength={c['strength']:.0f} tipping={c['tipping_score']:.0f}",
                        )
                    except Exception as _tj_e:
                        logger.warning(f"[trade_journal] intraday 적재 실패: {_tj_e}")
                    # ★ scalper_bot_feedback 양방향 채널 적재 (5/18 사장님 지적 즉시 통합) ★
                    try:
                        from utils.scalper_journal_hooks import hook_buy_execution
                        hook_buy_execution(
                            ticker=code, name=name, price=int(buy_price), qty=1,
                            grade="MEDIUM",  # 장중 멀티시그널은 MEDIUM 등급
                            source="intraday_scan",
                            extra_reason=f"strength={c.get('strength',0):.0f} tipping={c.get('tipping_score',0):.0f}",
                        )
                    except Exception as _h_e:
                        logger.debug(f"[scalper_hooks] intraday buy 적재 실패 (무시): {_h_e}")
                    self._positions[code] = {
                        "name": name, "qty": 1, "buy_price": buy_price,
                        "source": "verification_intraday",
                    }
                    bought.append(f"{name}(강도{c['strength']:.0f})")
                else:
                    failed.append(f"{name}({str(resp.get('msg', '?'))[:20]})")
            except Exception as e:
                failed.append(f"{name}({type(e).__name__})")
                logger.warning(f"[intraday] 매수 실패 [{code}] {name}: {e}")

        msg = [
            f"🚀 [장중 진입 결과] ✅ {len(bought)}/{len(candidates)}종목",
        ]
        if bought:
            msg.append("  " + ", ".join(bought[:5]) + (f" 외 {len(bought)-5}" if len(bought) > 5 else ""))
        if failed:
            msg.append(f"  ❌ 실패 {len(failed)}: " + ", ".join(failed[:3]))
        await _send("\n".join(msg))

    def _jarvis_3gate_check(self, code: str) -> dict:
        """5/20 사장님 결정: 진입 직전 자비스 3게이트 체크.

        [평생 원칙] ② "매수 전 5단계 게이트 필수" 부분 구현 (간소 3게이트).
        * 5/20은 첫 자율 가동 → 간소 3게이트로 시작, 5/21+ 경험 보고 5게이트 확장.
        * 호가 API 미구현 → 유동성(거래량)으로 대체.

        Gates:
            ① 유동성: volume >= 5000주 (최소 거래)
            ② 체결강도: strength >= 100 (균형 이상, 100=균형)
            ③ 가격안정성: (current - open) / open >= -3% (시초가 갭다운 한정)

        Returns:
            {ok: bool, reason: str, price_info: dict}
            ok=True → 통과 (매수 진행), ok=False → 차단 (다음 종목)
        """
        try:
            price_resp = self.trader.fetch_price(code)
            if not price_resp or not price_resp.get("success"):
                return {"ok": False, "reason": "fetch_price 실패", "price_info": {}}

            current = price_resp.get("current_price", 0)
            opn = price_resp.get("open", 0)
            volume = price_resp.get("volume", 0)
            strength = price_resp.get("strength", 0)

            if current <= 0:
                return {"ok": False, "reason": "현재가 0", "price_info": price_resp}

            # ① 유동성
            if volume < 5000:
                return {"ok": False, "reason": f"유동성 부족 vol={volume:,}", "price_info": price_resp}

            # ② 체결강도 (균형 이상)
            # ★ 5/20 사고 fix: 0.0 = 데이터 부재 (시초가 5분, 안정화 전) → 약세로 판정 X ★
            # 5/20 09:05 자비스 5종 일괄 차단 사고: 모든 종목 strength=0.0 → 약세 판정
            # 실제는 KIS API가 시초가 직후 strength 미반영 = 데이터 부재
            if strength == 0.0:
                # 데이터 부재 = 통과 처리 (이후 사이클 재평가)
                logger.info(f"[3gate] {code} 체결강도 0.0 = 데이터 부재 → 통과")
            elif strength < 100:
                return {"ok": False, "reason": f"체결강도 약세 {strength:.1f}", "price_info": price_resp}

            # ③ 가격안정성 (시초가 갭다운 -3% 한정)
            if opn > 0:
                gap_pct = (current - opn) / opn * 100
                if gap_pct < -3.0:
                    return {"ok": False, "reason": f"갭다운 {gap_pct:+.1f}%", "price_info": price_resp}

            return {
                "ok": True,
                "reason": f"vol={volume:,} 강도={strength:.0f} 현재가={current:,}",
                "price_info": price_resp,
            }
        except Exception as e:
            logger.warning(f"[3gate] {code} 체크 예외: {e}")
            return {"ok": False, "reason": f"예외: {type(e).__name__}", "price_info": {}}

    async def asset_pool_scan_and_buy(self, top_k: int = 5, qty_per_stock: int = 1):
        """자비스 자산 풀 → 매수 (5/19 결함 2 수정, 5/20 D-Day 배포).

        사장님 5/19 지적: "asset_pool_loader가 자동 매수 경로에 wire-up 안 됨"
        → 4종 자산 통합 풀에서 고신뢰(2개+ 소스 일치) 종목 TOP K 1주씩 매수.

        호출 시점: 09:00 morning_rec 실행 직후 1회 (장 시작 후 5분 이내).
        사장님 1주 모드 기본 (qty_per_stock=1).

        안전망 (intraday와 동일 표준):
          - verification_mode 체크
          - 큰형 advisory 게이트 (BEARISH/PANIC 차단)
          - 보유/morning_rec 중복 차단
          - ETF 차단 (asset_pool_loader 단계에서 prefix 필터)
          - 일일 최대 5종목 추가 (top_k 한도)

        Args:
            top_k: 최대 매수 종목 수 (기본 5)
            qty_per_stock: 종목당 수량 (기본 1주, 사장님 1주 모드).
                            ★ 5/21 아이디어 #1: dynamic_qty 토글 활성 시 자비스 자율 결정 ★
        """
        from data import verification_mode as _vm

        # ★★ 5/21 09:35 절대 원칙 — 매매 전 KIS 실제 계좌 강제 조회 ★★
        # 사장님 분노 사건 후 영구 룰: positions.json (메모리) ≠ KIS 실제 잔고
        # 매수 결정 전 반드시 (1) 실제 보유 종목 (2) 현금 (3) 평가손익 확인 → 텔레그램 보고
        kis_bal = await asyncio.to_thread(self.trader.fetch_balance)
        if kis_bal and kis_bal.get("success"):
            actual_positions = kis_bal.get("positions", [])
            actual_codes = [p.get("code") for p in actual_positions]
            actual_cash = kis_bal.get("cash", 0)
            actual_eval_pnl = kis_bal.get("eval_pnl", 0)

            # 매수 전 상태 텔레그램 보고 (절대 원칙)
            pos_summary = " | ".join([
                f"{p.get('name','?')}({p.get('qty',0)}주 {p.get('pnl_pct',0):+.1f}%)"
                for p in actual_positions[:10]
            ]) or "보유 0"
            await _send(
                f"📊 [매매 전 계좌 조회 — 사장님 5/21 절대 원칙]\n"
                f"  실제 보유: {len(actual_positions)}종 | 현금: {actual_cash:,} | 평가손익: {actual_eval_pnl:+,}\n"
                f"  {pos_summary}"
            )
            logger.info(
                f"[asset_pool] KIS 실제: {len(actual_positions)}종 / 현금 {actual_cash:,} / "
                f"PnL {actual_eval_pnl:+,} / 종목 {actual_codes}"
            )

            # 매수 제외 set에 실제 보유 종목 추가 (메모리 + 실제 합집합)
            self._real_holdings_codes = set(actual_codes)
        else:
            await _send("⚠️ [KIS 잔고 조회 실패] 메모리 기준으로만 매수 진행 — 사장님 확인 필요")
            self._real_holdings_codes = set()

        # ★ 5/21 아이디어 #1 — 자비스 자율 다주 결정 ★
        # config.bot.asset_pool.dynamic_qty=true 시 시장 상황 보고 1/2/3주 동적 결정
        # 70억 트레이더 미션 1단계 (5/22 D-Day로 활성화 예정)
        dyn_qty, dyn_reason = self._get_jarvis_dynamic_qty()
        if dyn_qty > 1:
            qty_per_stock = dyn_qty
            logger.info(f"[dynamic_qty] {dyn_reason}")

        async def _send(text):
            if self._send_alert:
                try:
                    await self._send_alert(text)
                except Exception:
                    pass

        if not _vm.is_active():
            return

        # ── 큰형(퀀트봇) advisory 게이트 ──
        try:
            from utils.quant_advisory_subscriber import fetch_latest_advisory
            _adv = None
            for _mt in ('LEADING', 'SNAPSHOT', 'ADVICE', 'MORNING_BRIEFING'):
                _adv = fetch_latest_advisory(msg_type=_mt, fallback_to_yesterday=False)
                if _adv:
                    break
            if not _adv:
                for _mt in ('LEADING', 'SNAPSHOT', 'ADVICE', 'MORNING_BRIEFING'):
                    _adv = fetch_latest_advisory(msg_type=_mt, fallback_to_yesterday=True)
                    if _adv:
                        break
            if _adv:
                _regime = (_adv.get('market_regime') or '').upper()
                # 사장님 5/20 결정: BEARISH는 경고만, PANIC만 차단 ([자비스 정신 v2])
                if _regime == 'PANIC':
                    await _send(
                        f"🛑 큰형 PANIC → 자산풀 매수 차단 "
                        f"({_adv.get('advisory_date')} {_adv.get('advisory_time')})"
                    )
                    logger.warning(f"[asset_pool] regime=PANIC → 차단")
                    return
                if _regime == 'BEARISH':
                    await _send(
                        f"⚠️ 큰형 BEARISH 경고 → 자비스 자율 판단 진행 "
                        f"({_adv.get('advisory_date')} {_adv.get('advisory_time')})\n"
                        f"  · 자산풀 매수 계속, 강한 종목만 선별"
                    )
                    logger.info(f"[asset_pool] regime=BEARISH (경고만, 통과)")
                else:
                    logger.info(f"[asset_pool] regime={_regime} 통과")
        except Exception as e:
            logger.warning(f"[asset_pool] advisory 게이트 실패 (continue): {e}")

        # ── ★ 5/20 사장님 비전: 점수 기반 정렬 + 이상한 종목 차단 ★ ──
        # 5/20 사고: 검증모드 grade D+F 매수 (링크제니시스/롯데이노베이트)
        # → asset_pool도 점수 기반 정렬 + 최소 점수 임계값 + AVOID 차단 적용
        try:
            from utils.asset_pool_loader import (
                get_top_candidates,
                get_candidate_source_map,
                load_limit_up_triggers,
            )
            # ★ 점수순 TOP K*3 가져와서 필터 후 K개 매수 ★
            ranked = await asyncio.to_thread(get_top_candidates, top_k * 3)
            source_map = await asyncio.to_thread(get_candidate_source_map)
            triggers = await asyncio.to_thread(load_limit_up_triggers)
            trigger_codes = {t.get("code", "") for t in triggers if isinstance(t, dict)}
        except Exception as e:
            logger.warning(f"[asset_pool] 자산 풀 로드 실패: {e}")
            return

        # ★ 5/20 fix: 최소 점수 임계값 30 (이상한 종목 자동 제외) ★
        # 진원생명과학(90점)/티웨이(65점) 같은 고점수만 통과
        _MIN_SCORE = 30
        ranked = [r for r in ranked if r.get("score", 0) >= _MIN_SCORE]
        if not ranked:
            logger.info(f"[asset_pool] 점수 {_MIN_SCORE}+ 후보 0종목 — 스킵 (이상한 종목 차단)")
            return

        # ★ 5/20 fix: recommendation.json AVOID 시그널 차단 ★
        try:
            import json as _json
            rec_path = Path(__file__).resolve().parent.parent / "data_store" / "recommendation.json"
            avoid_codes = set()
            if rec_path.exists():
                rec = _json.loads(rec_path.read_text(encoding="utf-8"))
                stocks = rec.get("stocks", []) if isinstance(rec, dict) else rec
                for s in stocks:
                    if isinstance(s, dict):
                        sig = str(s.get("signal_type", "")).upper()
                        grade = str(s.get("grade", "")).upper()
                        if sig == "AVOID" or grade in ("D", "F"):
                            code = s.get("code")
                            if code:
                                avoid_codes.add(code)
            if avoid_codes:
                _before = len(ranked)
                ranked = [r for r in ranked if r["code"] not in avoid_codes]
                _blocked = _before - len(ranked)
                if _blocked > 0:
                    logger.info(f"[asset_pool] AVOID/grade D,F {_blocked}종 차단 (이상한 종목)")
        except Exception as _ae:
            logger.warning(f"[asset_pool] AVOID 필터 실패 (계속): {_ae}")

        # 점수순 정렬된 코드 리스트
        all_codes = [r["code"] for r in ranked]
        if not all_codes:
            logger.info("[asset_pool] AVOID 차단 후 0종목 — 스킵")
            return

        # 제외: 보유 종목 (메모리 + KIS 실제) + verification 보유 + ETF prefix
        # ★ 5/21 09:35 사장님 분노 사건 후 — 메모리만으로 X, KIS 실제 합집합 강제
        excluded = set(self._positions.keys()) | getattr(self, "_real_holdings_codes", set())
        try:
            for p in _vm.get_active_positions():
                excluded.add(p["code"])
        except Exception:
            pass

        ETF_PREFIXES = (
            "KODEX", "TIGER", "RISE", "PLUS", "ACE", "SOL",
            "KOSEF", "ARIRANG", "HANARO", "KoAct",
        )

        # 종목명 조회를 위해 trigger/source_map에서 name 추출 보강
        name_map = {t.get("code"): t.get("name", "") for t in triggers if isinstance(t, dict)}

        filtered = []
        for code in all_codes:
            if code in excluded:
                continue
            name = name_map.get(code, "")
            if any(name.startswith(p) for p in ETF_PREFIXES):
                continue
            filtered.append({"code": code, "name": name, "sources": source_map.get(code, [])})
            if len(filtered) >= top_k:
                break

        if not filtered:
            logger.info("[asset_pool] 필터 후 0종목 — 스킵")
            return

        await _send(
            f"💎 [자산풀 후보] {len(filtered)}종목 — 자비스 3게이트 체크 진행"
        )

        bought, failed, gate_blocked = [], [], []
        for c in filtered:
            code = c["code"]
            name = c["name"] or code
            sources_tag = ",".join(c["sources"])
            try:
                # ── 자비스 3게이트 (사장님 5/20 결정: 매수 전 자비스가 종목 봄) ──
                gate = await asyncio.to_thread(self._jarvis_3gate_check, code)
                if not gate.get("ok"):
                    gate_blocked.append(f"{name}({gate['reason']})")
                    logger.info(f"[asset_pool] 3게이트 차단 {code}: {gate['reason']}")
                    continue

                # 게이트 통과 시 현재가 확보 (signal_tags용)
                price_info = gate.get("price_info", {})
                pre_buy_price = float(price_info.get("current_price", 0))

                # ── smart_buy (지정가 -0.5% → -0.2% → 시장가 폴백) ──
                # 사장님 원칙: "왠만하면 지정가, 정 안되면 시장가"
                resp = await asyncio.to_thread(self.trader.smart_buy, code, qty_per_stock)
                if resp.get("success"):
                    # 체결 후 실제 가격 재조회 (smart_buy는 saved_pct 반환하지만 체결가 미반환)
                    buy_price = pre_buy_price
                    try:
                        price_resp = await asyncio.to_thread(self.trader.fetch_price, code)
                        if price_resp and price_resp.get("success"):
                            buy_price = float(price_resp.get("current_price", pre_buy_price))
                    except Exception:
                        pass

                    # 5/20 사장님 결정: asset_pool 종목은 verification HOLD 정책 미적용
                    # → _vm.record_buy 호출 안 함 (15:25 강제 청산 제외)
                    # → trade_journal에는 정상 기록 (5/21 회고 데이터)
                    # → 본 시스템 mode="day" 트레일링 + TP가 알아서 매도/HOLD 결정

                    try:
                        from data import trade_journal as _tj
                        _tj.log_buy(
                            code=code, name=name, qty=qty_per_stock, price=buy_price,
                            source="asset_pool",
                            signal_tags=f"asset_pool({sources_tag})",
                            final_score=len(c["sources"]) * 20,
                            order_no=resp.get("order_no") or resp.get("ODNO"),
                            note=f"sources={sources_tag} jarvis_v1",
                        )
                    except Exception as _tj_e:
                        logger.warning(f"[trade_journal] asset_pool 적재 실패: {_tj_e}")

                    try:
                        from utils.scalper_journal_hooks import hook_buy_execution
                        hook_buy_execution(
                            ticker=code, name=name, price=int(buy_price),
                            qty=qty_per_stock,
                            grade="HIGH" if len(c["sources"]) >= 3 else "MEDIUM",
                            source="asset_pool",
                            extra_reason=f"sources={sources_tag}",
                        )
                    except Exception as _h_e:
                        logger.debug(f"[scalper_hooks] asset_pool buy 적재 실패: {_h_e}")

                    # 5/20 사장님 결정 + 5/19 23:30 자아성찰 fix:
                    #   · mode="day": 자동 SL/TP 시스템 가동 (line 3084)
                    #   · take_profit: 매수가 +5% (자비스 정신 v2 "5~8% 익절")
                    #   · stop_loss: 매수가 × 0.97 (-3% 안전망)
                    #     → [평생 원칙] ③ "매수 후 5분 대응 SL -3%" 살려냄
                    #     → 트레일링 활성 전 (+3% 미달) 매수 직후 폭락 데드존 차단
                    #     → 트레일링 활성 시 max(stop_loss, trailing_sl)로 자동 교체
                    #   · regime="NORMAL": +3% 도달 시 트레일링 활성, 고점 대비 -3% 스탑
                    #   · entry_price/high_watermark: 트레일링 시스템 진입 키
                    # ★ 5/20 사장님 비전 — trade_style_decider 자율 결정 ★
                    # 종목 + 시장 보고 단타/스윙 자율 선택 (SL/TP/보유일 동적 설정)
                    style_sl_pct = -3.0
                    style_tp_pct = 5.0
                    style_max_hold = 0
                    style_name = "DAY_TRADE"
                    style_reason = "default"
                    try:
                        from data.trade_style_decider import decide_trade_style
                        # 거래량 비율 (price_info에 있다면)
                        vol_ratio = float(price_info.get("volume_ratio", 1.0))
                        chg_pct = float(price_info.get("change_pct", 0.0))
                        style = decide_trade_style(
                            code=code, name=name,
                            change_pct=chg_pct, volume_ratio=vol_ratio,
                            source_signals=c["sources"],
                        )
                        style_name = style.get("style", "DAY_TRADE")
                        style_sl_pct = style.get("sl_pct") or -3.0
                        style_tp_pct = style.get("tp_pct") or 5.0
                        style_max_hold = style.get("max_hold_days", 0)
                        style_reason = style.get("reason", "")
                        logger.info(
                            f"[style] {name}({code}) → {style_name} "
                            f"SL{style_sl_pct}% TP{style_tp_pct}% {style_max_hold}d "
                            f"({style_reason})"
                        )
                    except Exception as _se:
                        logger.warning(f"[style] decider 실패 (default 적용): {_se}")

                    self._positions[code] = {
                        "name": name, "qty": qty_per_stock,
                        "buy_price": buy_price,
                        "entry_price": buy_price,
                        "high_watermark": buy_price,
                        "trailing_activated": False,
                        "trailing_sl": 0,
                        "stop_loss": int(buy_price * (1 + style_sl_pct / 100)),
                        "take_profit": int(buy_price * (1 + style_tp_pct / 100)),
                        "regime": "NORMAL",
                        # ★ A2 fix (5/21) — mode 판정을 style_name 기준으로 변경 ★
                        # 이전 버그: style_max_hold == 0 체크 → DAY_TRADE(max_hold=1)도 mode="swing"
                        # 결과: 어제 만든 trailing TP fix (L3329 self.mode=="day") 무력화
                        # 신규: DAY_TRADE → mode="day" 명확히 부여
                        "mode": "day" if style_name == "DAY_TRADE" else "swing",
                        "source": "asset_pool",
                        "entry_date": datetime.now().strftime("%Y-%m-%d"),
                        # ★ A3 fix (5/21) — 사장님 보호 토글 ★
                        # 어제 -293만 사고 재발 방지 + 70억 미션 자동 익절 충돌 해결
                        # config.bot.asset_pool.auto_protect=true 시 매수 종목 자동 sl_disabled
                        # 평소: false (자동 익절 작동) / "들고 있어" 명령 시: true (전체 차단)
                        "sl_disabled": (
                            self.config.get("bot", {})
                            .get("asset_pool", {})
                            .get("auto_protect", False)
                        ),
                        # ★ 5/20 비전 추가 메타 ★
                        "trade_style": style_name,
                        "style_max_hold_days": style_max_hold,
                        "style_reason": style_reason,
                    }
                    self._save_positions()
                    bought.append(f"{name}[{style_name}]")
                else:
                    failed.append(f"{name}({str(resp.get('msg', '?'))[:20]})")
            except Exception as e:
                failed.append(f"{name}({type(e).__name__})")
                logger.warning(f"[asset_pool] 매수 실패 [{code}] {name}: {e}")

        msg = [
            f"💎 [자산풀 매수 결과] ✅ {len(bought)}/{len(filtered)}종목",
            f"  · 후보 {len(filtered)} → 게이트차단 {len(gate_blocked)} / 실패 {len(failed)} / 체결 {len(bought)}",
        ]
        if bought:
            msg.append("  ✅ " + ", ".join(bought))
        if gate_blocked:
            msg.append(f"  🚪 3게이트 차단: " + ", ".join(gate_blocked[:5]))
        if failed:
            msg.append(f"  ❌ 매수 실패: " + ", ".join(failed[:3]))
        await _send("\n".join(msg))

    async def close_verification_positions(self):
        """15:25 강제 청산 (trading_coo에서 호출). verification 보유 전체 시장가 매도."""
        from data import verification_mode as _vm

        async def _send(text):
            if self._send_alert:
                try:
                    await self._send_alert(text)
                except Exception:
                    pass

        if not _vm.is_active():
            return

        active = _vm.get_active_positions()
        if not active:
            await _send("🧪 [검증모드 청산] 보유 종목 없음 (스킵)")
            return

        await _send(f"🧪 [검증모드 15:25 청산 시작] {len(active)}종목 시장가 매도...")
        sold = []
        failed = []

        for p in active:
            code = p["code"]
            name = p["name"]
            # Critical 3: 청산 retry 2회 (총 3회 시도, 1초 간격)
            resp = None
            last_err = None
            for attempt in range(3):
                try:
                    resp = await asyncio.to_thread(self.trader.sell_market, code, 1)
                    if resp and resp.get("success"):
                        break
                    last_err = str(resp.get("msg", "?"))[:25] if resp else "응답없음"
                except Exception as e:
                    last_err = f"{type(e).__name__}: {str(e)[:25]}"
                    logger.warning(f"[verification] 청산 실패 [{code}] {name} 시도 {attempt+1}/3: {e}")
                if attempt < 2:
                    await asyncio.sleep(1.0)

            if resp and resp.get("success"):
                # KIS 응답에 체결가 없음 → fetch_price로 조회
                sell_price = 0.0
                try:
                    price_resp = await asyncio.to_thread(self.trader.fetch_price, code)
                    if price_resp and price_resp.get("success"):
                        sell_price = float(price_resp.get("price", 0))
                except Exception:
                    pass
                if not sell_price:
                    sell_price = float(p.get("buy_price", 0))
                result = _vm.record_sell(code, sell_price, sell_reason="1525_close")
                # trade_journal 적재
                try:
                    from data import trade_journal as _tj
                    _tj.log_sell(
                        code=code, name=name, qty=1,
                        sell_price=sell_price,
                        buy_price=p.get("buy_price", 0),
                        event_type="sell_close",
                        source="verification",
                        order_no=resp.get("order_no") or resp.get("ODNO"),
                        note="검증모드 15:25 강제 청산",
                    )
                except Exception as _tj_e:
                    logger.warning(f"[trade_journal] verification sell 적재 실패: {_tj_e}")
                # ★ scalper_bot_feedback 양방향 채널 청산 적재 (5/18 사장님 지적 즉시 통합) ★
                try:
                    from utils.scalper_journal_hooks import hook_sell_execution
                    hook_sell_execution(
                        ticker=code, name=name,
                        entry_price=int(p.get("buy_price", 0)),
                        close_price=int(sell_price), qty=1,
                        sell_reason="검증모드 15:25 강제 청산",
                    )
                except Exception as _h_e:
                    logger.debug(f"[scalper_hooks] sell 적재 실패 (무시): {_h_e}")
                self._positions.pop(code, None)
                if result:
                    sold.append(f"{name} {result['pnl_pct']:+.2f}%")
                else:
                    sold.append(f"{name}(?)")
            else:
                failed.append(f"{name}({last_err})")

        msg_lines = [
            f"🧪 [검증모드 청산 결과]",
            f"  ✅ 청산: {len(sold)}/{len(active)}종목",
        ]
        if sold:
            msg_lines.append("  " + ", ".join(sold[:8])
                             + (f" 외 {len(sold)-8}" if len(sold) > 8 else ""))
        if failed:
            msg_lines.append("")
            msg_lines.append(f"  🚨 청산 실패 {len(failed)}종목 (3회 재시도 후) — 다음 거래일까지 보유 위험:")
            msg_lines.append("  " + ", ".join(failed[:5]))
            msg_lines.append("  ⚠️ 사장님 수동 확인 필요 (텔레그램 봇 또는 KIS HTS)")
        await _send("\n".join(msg_lines))

    async def _check_entry_watch(self):
        """진입감시 대기열 체크 - job_monitor에서 30초마다 호출

        각 종목의 KIS API 실시간 데이터를 확인하고:
        1. 갭업 체크 → TP(보이지않는 목표가) 대비 업사이드 판단
        2. 가격 안정화 (시가 대비 -2% 이상 안 빠짐)
        3. 양봉 (현재가 > 시가)
        4. 체결강도 (100+ = 매수 우위)
        5. AI EYE 점수 (50+ = 진입 OK)
        6. MACD 0선 크로스 상태 (일봉 기반)
        → 6개 중 3개 이상 충족 시 매수 실행
        """
        if not self._entry_watch:
            return

        expired = []
        for code, watch in list(self._entry_watch.items()):
            watch["checks"] += 1

            # 최대 관찰 시간 초과 → 만료
            if watch["checks"] > watch["max_checks"]:
                expired.append(code)
                split_done = watch.get("split_done", 0)
                split_count = watch.get("split_count", 3)
                if split_done > 0:
                    await self._alert(
                        f"⏰ 진입 관찰 만료: {watch['name']}({code})\n"
                        f"   {split_done}/{split_count}차 분할매수 완료 상태로 종료\n"
                        f"   나머지 {split_count - split_done}차는 시간 초과로 취소"
                    )
                else:
                    await self._alert(
                        f"⏰ 진입 관찰 만료: {watch['name']}({code})\n"
                        f"   30분간 진입 조건 미충족 - 오늘 매수 안 함"
                    )
                continue

            # Phase 3-B: WebSocket 캐시 5초 이내면 우선 사용 (API 호출 절감)
            ws_cached = self._get_ws_price(code, max_age_sec=5.0)
            if ws_cached and ws_cached["price"] > 0:
                cp = ws_cached["price"]
                price_info = {
                    "success": True,
                    "current_price": cp,
                    "volume": ws_cached["volume"],
                    "ws_cache": True,
                }
            else:
                # REST fallback (기존 경로)
                try:
                    price_info = self.trader.fetch_price(code)
                    if not price_info or not price_info.get("success"):
                        continue
                    cp = price_info["current_price"]
                except Exception:
                    continue

            try:

                # 시가 기록 (첫 체크)
                if watch["open_price"] == 0:
                    watch["open_price"] = price_info.get("open", cp)

                # 장중 고저 업데이트
                if cp > watch["max_price"]:
                    watch["max_price"] = cp
                if cp < watch["min_price"]:
                    watch["min_price"] = cp

                prev_close = watch["prev_close"]
                open_price = watch["open_price"]

                # ── 진입 조건 체크 (NORMAL 3/6, MOMENTUM 2/6, ETF 2/3) ──
                is_momentum = watch.get("regime") == "MOMENTUM"
                is_etf_entry = watch.get("is_etf", False)
                conditions_met = 0
                conditions_detail = []

                # 1) 갭업 체크 - "보이지않는 목표가" 기반 판단
                #    고정 5% 거부 X → TP 대비 업사이드가 충분하면 갭업도 매수
                gap_pct = (open_price / prev_close - 1) * 100 if prev_close > 0 else 0
                tp = watch.get("tp", 0)
                upside_to_tp = (tp / cp - 1) * 100 if tp > 0 and cp > 0 else 0

                # 갭업이어도 목표가까지 5% 이상 남으면 → 매수 OK
                # 갭업인데 목표가까지 5% 미만 → 리스크 대비 수익 부족 → 패스
                # 단, 스마트머니(DUAL_SURGE 90점+외인100억) 종목은 면제
                is_smart = watch.get("smart_money", False)
                if gap_pct >= 3.0 and upside_to_tp < 5.0 and not is_smart:
                    expired.append(code)
                    await self._alert(
                        f"⛔ 갭업+업사이드 부족: {watch['name']}({code})\n"
                        f"   전일 {prev_close:,} → 시가 {open_price:,} ({gap_pct:+.1f}%)\n"
                        f"   현재 {cp:,} → 목표 {tp:,} (업사이드 {upside_to_tp:+.1f}%)\n"
                        f"   R:R 불리 - 오늘 패스"
                    )
                    continue
                elif gap_pct >= 3.0 and upside_to_tp < 5.0 and is_smart:
                    conditions_detail.append(f"스마트머니 갭업면제({gap_pct:+.1f}%)")
                    logger.info(f"[스마트머니] {watch['name']} 갭업+업사이드부족이지만 면제")
                elif gap_pct >= 3.0 and upside_to_tp >= 5.0:
                    # 갭업이지만 목표가 여유 있음 → 매수 계속 진행
                    conditions_detail.append(f"갭업{gap_pct:+.1f}%→목표{upside_to_tp:.0f}%남음")

                # 1.5) 전쟁릴레이 가격 게이트: 진입가 대비 3% 이상 위면 대기
                if watch.get("source") == "war_relay":
                    entry_target = watch.get("prev_close", 0)
                    price_vs_entry = (cp / entry_target - 1) * 100 if entry_target > 0 else 0
                    if price_vs_entry > 3.0:
                        # 진입가보다 3%+ 위 → 아직 안 빠짐, 조건 체크 스킵
                        if watch["checks"] % 20 == 0:  # 10분마다 1번 로그
                            logger.info(
                                f"[전쟁릴레이 대기] {watch['name']}({code}) "
                                f"현재 {cp:,} vs 진입 {entry_target:,} "
                                f"({price_vs_entry:+.1f}%) - 눌림 대기중"
                            )
                        continue

                # 2) 가격 안정화: 시가 대비 -2% 이상 하락 안 함
                from_open = (cp / open_price - 1) * 100 if open_price > 0 else 0
                if from_open >= -2.0:
                    conditions_met += 1
                    conditions_detail.append(f"가격안정({from_open:+.1f}%)")

                # 3) 양봉 (현재가 > 시가)
                if cp > open_price:
                    conditions_met += 1
                    conditions_detail.append("양봉")

                # 4) 체결강도 100+ (매수 우위) — ETF 스킵
                if is_etf_entry:
                    conditions_met += 1
                    conditions_detail.append("체결스킵(ETF)")
                else:
                    strength = price_info.get("strength", 0)
                    if strength >= 100:
                        conditions_met += 1
                        conditions_detail.append(f"체결{strength:.0f}")

                # 5) AI EYE 점수 체크 — ETF 스킵
                if is_etf_entry:
                    conditions_met += 1
                    conditions_detail.append("AI스킵(ETF)")
                else:
                    ai_score = -1
                    try:
                        rtm = self._get_rt_monitor()
                        # 임시 등록 → 평가 → 해제
                        rtm.register_position(
                            code, watch["name"], cp, watch["sl"], watch["tp"]
                        )
                        snap = await asyncio.to_thread(rtm.evaluate_position, code)
                        if snap:
                            ai_score = snap.realtime_score
                            watch["ai_scores"].append(ai_score)
                        rtm.unregister_position(code)
                    except Exception:
                        pass

                    ai_threshold = 30 if is_momentum else 50
                    if ai_score >= ai_threshold:
                        conditions_met += 1
                        mtm_s = "(MTM)" if is_momentum and ai_score < 50 else ""
                        conditions_detail.append(f"AI{ai_score}{mtm_s}")
                    elif ai_score >= 0:
                        conditions_detail.append(f"AI{ai_score}(약)")

                # 6) MACD 0선 크로스 상태 (일봉 기반)
                # MOMENTUM/ETF: MACD 스킵
                if is_momentum or is_etf_entry:
                    conditions_met += 1
                    tag = "MTM" if is_momentum else "ETF"
                    conditions_detail.append(f"MACD스킵({tag})")
                else:
                    try:
                        from strategies.macd_zero_scanner import _calc_macd
                        from pykrx import stock as pykrx_stock
                        from datetime import timedelta
                        end_d = datetime.now().strftime("%Y%m%d")
                        start_d = (datetime.now() - timedelta(days=60)).strftime("%Y%m%d")
                        day_df = pykrx_stock.get_market_ohlcv(start_d, end_d, code)
                        if day_df is not None and len(day_df) >= 30:
                            close_arr = day_df["종가"].astype(float).values
                            macd_l, macd_s, macd_h = _calc_macd(close_arr)
                            if macd_h[-1] > 0 and macd_l[-1] > 0:
                                conditions_met += 1
                                conditions_detail.append("MACD0↑")
                            elif macd_h[-1] > 0:
                                conditions_detail.append("MACD+")
                            else:
                                conditions_detail.append("MACD-")
                    except Exception:
                        pass

                # 5분마다 관찰 로그 (매 10회 = 30초 * 10 = 5분)
                _total_conds = 6
                if watch["checks"] % 10 == 0:
                    etf_tag = "[ETF] " if is_etf_entry else ""
                    logger.info(
                        f"진입감시 {etf_tag}{watch['name']}: "
                        f"현재{cp:,} 시가{open_price:,} 갭{gap_pct:+.1f}% | "
                        f"조건 {conditions_met}/{_total_conds} | "
                        f"{' '.join(conditions_detail)}"
                    )

                # ── 진입 조건 충족! → 분할매수 ──
                # ETF: 5개 (가격안정+양봉+체결스킵+AI스킵+MACD스킵 → 5/6)
                # MOMENTUM: 3개 / NORMAL: 3개 (연말 4개)
                _now = datetime.now()
                _yearend = (_now.month == 12 and _now.day >= 15) or (_now.month == 1 and _now.day <= 5)
                if is_etf_entry:
                    entry_threshold = 5  # 자동패스3 + 가격안정+양봉 = 5
                elif is_momentum:
                    entry_threshold = 3  # MACD스킵 포함 6조건 중 3개 (실제 5개 평가 중 60%)
                else:
                    entry_threshold = 4 if _yearend else 3
                if conditions_met >= entry_threshold:
                    detail_str = " + ".join(conditions_detail)
                    split_done = watch.get("split_done", 0)
                    split_count = watch.get("split_count", 3)
                    split_amount = watch.get("split_amount", watch["buy_amount"])
                    last_split = watch.get("last_split_check", 0)
                    split_interval = watch.get("split_interval", 10)

                    # 첫 분할이거나, 이전 분할 후 5분(10회) 경과
                    can_split = (split_done == 0) or \
                                (watch["checks"] - last_split >= split_interval)

                    if not can_split:
                        # 아직 다음 분할 시간 안 됨 → 대기
                        remaining = split_interval - (watch["checks"] - last_split)
                        if watch["checks"] % 10 == 0:
                            logger.info(
                                f"분할대기 {watch['name']}: "
                                f"{split_done}/{split_count}차 완료, "
                                f"다음 분할까지 {remaining * 30}초"
                            )
                        continue

                    # 마지막 분할이면 남은 금액 전부 투입
                    is_last = (split_done + 1 >= split_count)
                    this_amount = split_amount
                    if is_last:
                        used = split_amount * split_done
                        this_amount = max(50000, watch["buy_amount"] - used)  # 최소 5만원 보장

                    split_label = f"[{split_done+1}/{split_count}차]"

                    if self._confirm_auto:
                        # 이미 대기 중인 매수가 있으면 스킵 (중복 방지)
                        already_pending = any(
                            p["code"] == code for p in self._pending_auto_buys
                        )
                        if already_pending:
                            continue

                        self._pending_auto_buys.append({
                            "code": code, "name": watch["name"],
                            "amount": this_amount, "sl": watch["sl"],
                            "tp": watch["tp"],
                            "tp1_quick": watch.get("tp1_quick", watch["tp"]),
                            "score": watch["score"],
                            "regime": watch.get("regime", "NORMAL"),
                            "source": watch.get("source", "swing"),
                            "_is_split": True,
                            "_split_label": split_label,
                        })
                        await self._alert(
                            f"⚠️ 분할매수 {split_label} 확인 대기\n"
                            f"   {watch['name']}({code}) @ {cp:,}원\n"
                            f"   시가 {open_price:,} → 현재 {cp:,} ({from_open:+.1f}%)\n"
                            f"   조건: {detail_str}\n"
                            f"   금액: {this_amount:,}원 "
                            f"(총 {watch['buy_amount']:,}원 중)\n\n"
                            f"   실행: '자동확인' | 취소: '자동취소'"
                        )
                        # split_done은 여기서 안 올림!
                        # execute_pending_auto_buys에서 실제 체결 후 올림
                    else:
                        result = self.trader.safe_buy(code, this_amount)
                        if result.get("success"):
                            watch["split_done"] = split_done + 1
                            watch["last_split_check"] = watch["checks"]

                            # 포지션 등록/업데이트
                            if code not in self._positions:
                                target_state = self._init_dynamic_target(
                                    code, watch["name"], cp
                                )
                                sl = target_state.dynamic_sl if target_state else watch["sl"]
                                tp = target_state.dynamic_tp if target_state else watch["tp"]
                                self._positions[code] = {
                                    "entry_price": cp,
                                    "stop_loss": sl,
                                    "take_profit": tp,
                                    "entry_date": datetime.now().strftime("%Y-%m-%d"),
                                    "name": watch["name"],
                                    "source": watch.get("source", "swing"),
                                    "target_state": target_state,
                                    "high_watermark": cp,
                                    "trailing_activated": False,
                                    "trailing_sl": 0,
                                    "regime": watch.get("regime", "NORMAL"),
                                    "is_etf": watch.get("is_etf", False),
                                    "etf_category": watch.get("etf_category", ""),
                                    "holding_days": watch.get("holding_days", 10),
                                }
                                # MOMENTUM: SL (-4.5%) — 4/8: -3.5%→-4.5% 완화 (변동성 여유)
                                if watch.get("regime") == "MOMENTUM":
                                    mtm_sl = int(cp * 0.955)
                                    self._positions[code]["stop_loss"] = max(mtm_sl, watch["sl"])
                                self._save_positions()
                                try:
                                    rtm = self._get_rt_monitor()
                                    final_sl = self._positions[code]["stop_loss"]
                                    rtm.register_position(code, watch["name"], cp, final_sl, tp,
                                                          source=watch.get("source", ""))
                                except Exception:
                                    pass

                            _regime_tag = " [MTM]" if is_momentum else ""
                            await self._alert(
                                f"✅ 분할매수 {split_label} 체결!{_regime_tag}\n"
                                f"   {watch['name']}({code}) @ {cp:,}원\n"
                                f"   시가 {open_price:,} → 매수 {cp:,}\n"
                                f"   조건: {detail_str}\n"
                                f"   금액: {this_amount:,}원 "
                                f"(총 {watch['buy_amount']:,}원 중)\n"
                                f"   SL:{watch['sl']:,} TP:{watch['tp']:,}"
                            )

                            if is_last:
                                await self._alert(
                                    f"🎯 분할매수 완료: {watch['name']}({code})\n"
                                    f"   {split_count}차 분할 전량 체결!"
                                )
                                expired.append(code)
                            # 아직 분할 남음 → 감시 유지
                        else:
                            await self._alert(
                                f"❌ 분할매수 {split_label} 실패: "
                                f"{watch['name']}({code})\n"
                                f"   {result.get('message')}"
                            )
                            # 매수 실패해도 다음 분할 시도 위해 감시 유지

                # 조건 미충족 + 이미 분할 진행 중 → 악화 체크
                elif watch.get("split_done", 0) > 0 and conditions_met < 2:
                    await self._alert(
                        f"⚠️ 분할매수 중단: {watch['name']}({code})\n"
                        f"   {watch['split_done']}/{watch['split_count']}차까지 완료\n"
                        f"   조건 악화 ({conditions_met}/6) - 나머지 취소"
                    )
                    expired.append(code)

            except Exception as e:
                logger.error(f"진입감시 오류 {code}: {e}")

        # 만료/완료 항목 제거 + WebSocket 구독 자동 해제
        for code in expired:
            self._entry_watch.pop(code, None)
            try:
                await self._unsubscribe_entry_watch(code)
            except Exception:
                pass


    async def _morning_day(self, context, _send):
        """데이 모드 아침 스캔: 기존 5D + 고정 SL/TP"""
        try:
            candidates = await asyncio.to_thread(self._scan_and_filter)
        except Exception as e:
            logger.error(f"스캔 실패: {e}")
            await _send(f"❌ 스캔 실패: {e}")
            return

        if not candidates:
            await _send("스캔 결과: 매수 후보 없음")
            return

        lines = ["🎯 매수 후보"]
        for f in candidates:
            from bot.kis_trader import CODE_TO_NAME
            name = CODE_TO_NAME.get(f.score.code, f.score.code)
            lines.append(
                f"  {name}({f.score.code}) {f.risk_label} "
                f"5D:{f.stability_grade}({f.stability.stability_score:.0f})"
            )
        if not self.is_running:
            lines.append("\n⏸ 자동매매 OFF - 리포트만 전송")
        await _send("\n".join(lines))

        if not self.is_running:
            return

        # ── 리스크 게이트 체크 ──
        risk_ok, risk_reason = self.check_risk_gate()
        if not risk_ok:
            await _send(f"⛔ 리스크 게이트 차단\n{risk_reason}")
            return

        bot_conf = self.config.get("bot", {})
        max_pos = bot_conf.get("max_auto_positions", 2)
        buy_amount = bot_conf.get("auto_buy_amount", 500000)
        # ── 이벤트 리스크 → 데이 예산 축소 ──
        try:
            from data.event_calendar import get_event_risk_for_recommendation
            ev_risk = get_event_risk_for_recommendation(date.today())
            _rl = ev_risk.get("risk_level", "LOW")
            if self._cfo:
                _mult = self._cfo.get_event_multiplier(_rl, "day")
            else:
                _mult = {"EXTREME": 0.5, "HIGH": 0.7}.get(_rl, 1.0)
            if _mult < 1.0:
                buy_amount = int(buy_amount * _mult)
        except Exception:
            pass

        # FIX-02: BRAIN 교차 신호 — 관망 차단 + 포지션 캡
        brain_alloc = self._load_brain_allocation()
        if brain_alloc.get("_block_all_buys"):
            await _send(f"🚫 BRAIN 관망모드 — day mode 매수 중단")
            return

        # 4/8: 방어/경계 레짐 → 최대 1종목
        _day_regime = brain_alloc.get("effective_regime", "")
        if _day_regime in ("CAUTIOUS", "DEFENSIVE", "방어", "경계"):
            max_pos = min(max_pos, 1)

        cross = brain_alloc.get("cross_signal", {})
        if cross.get("max_positions_cap") is not None:
            cap_ratio = cross["max_positions_cap"]
            max_pos = max(1, int(max_pos * cap_ratio)) if cap_ratio > 0 else 0

        bal = self.trader.fetch_balance()
        current_positions = len(bal.get("positions", [])) if bal.get("success") else 0
        slots = max_pos - current_positions

        if slots <= 0:
            await _send(f"보유 종목 {current_positions}개 - 추가 매수 불가")
            return

        # 5/19 D-Day — day mode 단타봇 자금 한도 안전망 (swing이 메인, day는 보조)
        _scalper_budget = bot_conf.get("scalper_budget", 0)
        if _scalper_budget > 0 and bal.get("success"):
            _cash = bal.get("cash", 0)
            if _cash > _scalper_budget and buy_amount > 0:
                _max_slots_by_budget = _scalper_budget // buy_amount
                if _max_slots_by_budget < slots:
                    logger.info(
                        f"[자금정책-day] slots {slots} → {_max_slots_by_budget} "
                        f"(단타봇 한도 {_scalper_budget:,}원 / buy_amount {buy_amount:,}원)"
                    )
                    slots = max(0, _max_slots_by_budget)
                    if slots <= 0:
                        await _send(f"⛔ day mode: 단타봇 자금 한도로 매수 불가 (예산 {_scalper_budget:,}원)")
                        return

        bought = 0
        risk_conf = self.config.get("risk", {})
        sl_pct = risk_conf.get("stop_loss_pct", 0.02)
        # 매크로 전략 기반 SL 조정 (타이트하면 더 좁은 값 사용)
        try:
            from data.macro_strategy import get_adjusted_sl
            sl_pct = get_adjusted_sl(sl_pct)
        except Exception:
            pass
        tp_pct = risk_conf.get("take_profit_pct", 0.05)

        for f in candidates[:slots]:
            code = f.score.code
            result = self.trader.safe_buy(code, buy_amount)

            if result.get("success"):
                bought += 1
                price_info = self.trader.fetch_price(code)
                cp = price_info.get("current_price", 0) if price_info and price_info.get("success") else 0
                if cp <= 0:
                    # 매수 성공 후 가격 조회 실패 → 근사치 사용
                    cp = int(buy_amount / max(1, result.get("qty", 1)))
                    if cp <= 0:
                        cp = 10000
                    logger.error(f"day mode 가격 조회 실패 {code} — 근사치 {cp:,}원 사용")
                self._positions[code] = {
                    "entry_price": cp,
                    "stop_loss": int(cp * (1 - sl_pct)),
                    "take_profit": int(cp * (1 + tp_pct)),
                    "high_watermark": cp,
                    "trailing_activated": False,
                    "trailing_sl": 0,
                    "entry_date": datetime.now().strftime("%Y-%m-%d"),
                    "name": code,
                    "target_state": None,
                    "regime": "NORMAL",
                    "source": "day_scan",
                }
                self._save_positions()
                await _send(f"✅ 자동 매수: {result.get('message')}")
            else:
                await _send(f"❌ 매수 실패 {code}: {result.get('message')}")

        await _send(f"아침 스캔 완료: {bought}/{len(candidates[:slots])} 매수")

    async def job_monitor(self, context):
        """포지션 감시 - AI 4팩터 실시간 분석 (JobQueue 반복 호출)"""
        if not self._is_market_hours():
            return

        # ── 진입감시 대기열 체크 (장 시작 후 실시간 관찰 → 조건 충족 시 매수) ──
        if self._entry_watch:
            await self._check_entry_watch()

        if not self.is_running:
            return
        if not self._positions:
            return

        try:
            rtm = self._get_rt_monitor()

            # 데이터 피드 중단 감지 — 첫 1회만 알림  # SILENT: MSG-REDUX
            if rtm._feed_suspended:
                if not self._feed_alert_sent:
                    await self._alert(
                        "⚠️ 데이터 피드 중단 감지!\n"
                        f"   {rtm._consecutive_failures}회 연속 API 실패\n"
                        "   신규 매매 중지, SL/TP 폴백으로 전환"
                    )
                    self._feed_alert_sent = True
                else:
                    logger.info(f"피드 중단 지속 ({rtm._consecutive_failures}회)")
                self._risk_blocked = True
                await self._job_monitor_fallback()
                return

            snapshots = await asyncio.to_thread(rtm.evaluate_all)
            # 피드 복구 시 플래그 리셋  # SILENT: MSG-REDUX
            if self._feed_alert_sent:
                self._feed_alert_sent = False
                logger.info("데이터 피드 복구 — 알림 플래그 리셋")
        except Exception as e:
            logger.error(f"AI 모니터 평가 실패: {e}")
            # 폴백: 기존 단순 SL/TP 체크
            await self._job_monitor_fallback()
            return

        for snap in snapshots:
            code = snap.code
            pos = self._positions.get(code)
            if not pos:
                continue

            try:
                # SL 동기화 (트레일링 반영)
                pos["stop_loss"] = snap.current_sl

                # ── 고점 추적 (AI 모니터에서도) ──
                entry = pos["entry_price"]
                pos["high_watermark"] = max(pos.get("high_watermark", entry), snap.price)
                pnl_pct = (snap.price / entry - 1) * 100 if entry > 0 else 0

                # MOMENTUM vs NORMAL 트레일링 분기
                if pos.get("regime") == "MOMENTUM":
                    # MOMENTUM: +1.5%부터 트레일링, 고점 대비 -2%
                    if pnl_pct >= 1.5 or pos.get("trailing_activated"):
                        pos["trailing_activated"] = True
                        hwm = pos["high_watermark"]
                        trail_sl = int(hwm * 0.98)  # -2% (vs NORMAL -3%)
                        if pnl_pct >= 1.5:
                            trail_sl = max(trail_sl, entry)
                        pos["trailing_sl"] = max(pos.get("trailing_sl", 0), trail_sl)
                else:
                    # NORMAL: 기존 +3%부터 트레일링, 고점 대비 -3%
                    if pnl_pct >= 3.0 or pos.get("trailing_activated"):
                        pos["trailing_activated"] = True
                        hwm = pos["high_watermark"]
                        trail_sl = int(hwm * 0.97)
                        if pnl_pct >= 3.0 or hwm > entry * 1.03:
                            trail_sl = max(trail_sl, entry)
                        pos["trailing_sl"] = max(pos.get("trailing_sl", 0), trail_sl)

                if snap.decision == "FULL_SELL":
                    logger.info(f"AI 전량매도: {code} @ {snap.price:,} ({snap.decision_reason})")
                    # 매도 전 실제 보유수량 조회 (qty 보정)
                    pre_bal = self.trader.fetch_balance()
                    actual_qty = pos.get("qty", 1)
                    if pre_bal and pre_bal.get("success"):
                        for p in pre_bal.get("positions", []):
                            if p["code"] == code:
                                actual_qty = p.get("qty", 1)
                                break
                    # ★ 5/20 사고 fix: 사장님 보호 명령 체크 ★
                    if self._is_sell_protected(code, "ai_monitor_FULL_SELL"):
                        continue
                    # 매도 실행
                    result = self.trader.liquidate_one(code)
                    if not result or not result.get("success"):
                        logger.error(f"AI 매도 실패 {code}: {result} — 포지션 유지")
                        continue
                    # 실현 손익 기록
                    pnl_amount = (snap.price - pos["entry_price"]) * actual_qty
                    self._record_trade_pnl(code, pnl_amount, "ai_monitor")
                    self._positions.pop(code, None)
                    self._save_positions()
                    rtm.unregister_position(code)
                    await self._alert(rtm.format_decision_alert(snap))

                elif snap.decision == "PARTIAL_SELL":
                    if pos.get("partial_sold"):
                        logger.info(f"AI 부분매도 이미 완료: {code} — 스킵")
                    else:
                        logger.info(f"AI 부분매도: {code} @ {snap.price:,} ({snap.decision_reason})")
                        bal = self.trader.fetch_balance()
                        for p in bal.get("positions", []):
                            if p["code"] == code:
                                half = max(1, p["qty"] // 2)
                                sell_r = self.trader.smart_sell(code, half)
                                if sell_r and sell_r.get("success"):
                                    pos["partial_sold"] = True
                                    await self._alert(rtm.format_decision_alert(snap))
                                else:
                                    logger.error(f"AI 부분매도 실패 {code}: {sell_r}")
                                break

                # ── 하락 시 자동 추매 (Recovery Add-On + 4시그널) ──
                # 청산 결정이 아닌 보유 유지 종목 대상으로 추매 트리거 체크
                # snap에서 장중 데이터(realtime_score/strength/bid_qty/ask_qty/vwap) 추출 → multi-signal 평가
                if snap.decision not in ("FULL_SELL", "PARTIAL_SELL"):
                    try:
                        # 중복 큐 진입 방지
                        already_queued = any(
                            it.get("code") == code for it in self._pending_auto_buys
                        )
                        if not already_queued:
                            # snap → multi-signal 데이터 추출
                            snap_data = {
                                "realtime_score": snap.realtime_score,
                                "strength": snap.strength,
                                "bid_qty": snap.bid_qty,
                                "ask_qty": snap.ask_qty,
                                "price": snap.price,
                                "vwap": snap.vwap,
                            }
                            decision = evaluate_add_on(
                                pos, snap.price, code, self.trader,
                                self.config.get("bot", {}),
                                snap_data=snap_data,
                            )
                            if decision.should_add:
                                # 위험 게이트 통과 필수
                                risk_ok, risk_reason = self.check_risk_gate()
                                if not risk_ok:
                                    logger.info(
                                        f"[Recovery Add-On] {code} 위험 차단: {risk_reason}"
                                    )
                                elif decision.signal_mode == "auto":
                                    # ── 자동 매수 (시그널 3/4 이상) ──
                                    pos_name = pos.get("name", code)
                                    logger.info(
                                        f"[Recovery Add-On AUTO] {pos_name}({code}) "
                                        f"{decision.count}차 시그널 {decision.signal_score}/4 → 즉시 매수"
                                    )
                                    buy_result = self.trader.safe_buy(code, decision.add_amount)
                                    if buy_result and buy_result.get("success"):
                                        record_add_on(pos, decision)
                                        self._save_positions()
                                        await self._alert(
                                            f"✅ 추매 자동매수 체결: {pos_name}({code})\n"
                                            f"   {decision.reason}\n"
                                            f"   금액: {decision.add_amount:,}원"
                                        )
                                    else:
                                        logger.error(f"[Recovery Add-On AUTO] {code} 매수 실패: {buy_result}")
                                        await self._alert(
                                            f"❌ 추매 자동매수 실패: {pos_name}({code}) — {buy_result}"
                                        )
                                else:
                                    # confirm 모드: 기존 큐 진입 + 사장님 승인 대기
                                    self._pending_auto_buys.append({
                                        "code": code,
                                        "name": pos.get("name", code),
                                        "amount": decision.add_amount,
                                        "sl": pos["stop_loss"],
                                        "tp": pos["take_profit"],
                                        "tp1_quick": pos["take_profit"],
                                        "score": 0,
                                        "regime": pos.get("regime", "NORMAL"),
                                        "source": pos.get("source", ""),
                                        "_recovery_add_on": decision,
                                    })
                                    await self._alert(format_alert_message(
                                        pos.get("name", code), code, snap.price, decision
                                    ))
                                    logger.info(
                                        f"[Recovery Add-On CONFIRM] 큐 진입: {code} "
                                        f"({decision.count}차, 시그널 {decision.signal_score}/4)"
                                    )
                            elif decision.signal_mode == "skip" and decision.signal_score >= 0:
                                # 시그널 부족으로 skip (로그만, 알림 X)
                                logger.info(
                                    f"[Recovery Add-On SKIP] {code} {decision.reason}"
                                )
                    except Exception as _e:
                        logger.warning(f"[Recovery Add-On] 평가 실패 {code}: {_e}")
                # ── Recovery Add-On 끝 ──

                # 10분마다 전체 리포트 (매 20회차)
                # (30초 * 20 = 10분)

            except Exception as e:
                logger.error(f"AI 모니터 처리 실패 {code}: {e}")

        # ── Intraday AI Eye (5분 주기) ──
        self._eye_counter += 1
        if self._eye_counter >= 10:  # 30초 × 10 = 5분
            self._eye_counter = 0
            try:
                await self._run_intraday_eye()
            except Exception as e:
                logger.error(f"Intraday Eye 실패: {e}")

            # ── 정보봇 CRISIS 인버스 ETF 알림 (1일 1회 제한) ──
            # 자동 매수 X, 사장님 수동 결정용 권유 텔레그램
            try:
                from datetime import date as _date
                today_iso = _date.today().isoformat()
                if self._inverse_alert_date != today_iso and should_trigger_inverse_alert():
                    msg = get_inverse_alert_message()
                    if msg:
                        await self._alert(msg)
                        self._inverse_alert_date = today_iso
                        logger.info("[CRISIS 인버스 알림] 송출 완료")
            except Exception as e:
                logger.warning(f"인버스 알림 체크 실패: {e}")

    async def _run_intraday_eye(self):
        """5분마다 보유+감시 종목 흐름 분석 → 상태 전이 알림 + DYING 자동청산"""
        if self._eye is None:
            from data.intraday_eye import IntradayEye
            self._eye = IntradayEye(self.trader)

        # 대상: 보유 종목 + 진입 감시 종목
        codes = list(self._positions.keys())
        codes += [c for c in self._entry_watch if c not in codes]
        if not codes:
            return

        for code in codes:
            pos = self._positions.get(code, {})
            entry_price = pos.get("entry_price", 0)
            name = pos.get("name", self._entry_watch.get(code, {}).get("name", code))

            # 종목명 설정
            self._eye.set_name(code, name)

            try:
                verdict = await asyncio.to_thread(
                    self._eye.evaluate, code, entry_price=entry_price
                )
            except Exception as e:
                logger.warning(f"Eye 평가 실패 {code}: {e}")
                continue

            if verdict is None or verdict.verdict == "WARMUP":
                continue

            # 상태 전이 시에만 텔레그램 알림
            if verdict.verdict != verdict.prev_verdict:
                await self._send_eye_alert(verdict)

            # ── EYE-07: Guardian 연동 + 자동 대응 (보유 종목만) ──
            if code in self._positions:
                await self._eye_guardian_action(code, name, verdict, pos)

    async def _eye_guardian_action(self, code: str, name: str, verdict, pos: dict):
        """EYE-07: Eye 판정 → Guardian 재평가 → 자동 매도/SL 조정"""
        # 수동 동기화 종목: AI Eye + Guardian 자동 청산 면제
        # (사장님 "회복 시나리오 신뢰" 의지 보존 — SL/TP만 청산 트리거)
        if pos.get("source", "").startswith("manual_sync"):
            logger.info(f"[Eye+Guardian SKIP] {name}({code}) — 수동 동기화 보유, SL/TP만 적용")
            return

        # Eye→Guardian 리스크 보정: DYING +30, WEAKENING +10, ALIVE 0, BREAKING -10
        EYE_RISK_MAP = {"DYING": 30, "WEAKENING": 10, "ALIVE": 0,
                        "BREAKING": -10, "BOUNCING": 0, "WARMUP": 0}
        eye_adj = EYE_RISK_MAP.get(verdict.verdict, 0)

        # ── 뉴스 감성 Kill-Switch → Guardian 가산 ──
        try:
            from data.news_sentiment import get_news_risk_adjustment
            news_adj = get_news_risk_adjustment(code)
            if news_adj > 0:
                eye_adj += news_adj
                logger.info(f"[NEWS→GUARDIAN] {name} 뉴스 리스크 +{news_adj:.0f} → 합산 {eye_adj}")
        except Exception:
            pass

        # (1) DYING + HIGH confidence → Guardian 즉시 재평가
        #     뉴스 Kill-Switch → WEAKENING에서도 Guardian 재평가
        news_triggered = eye_adj > EYE_RISK_MAP.get(verdict.verdict, 0)
        if (verdict.verdict == "DYING" and verdict.confidence >= 0.70) or news_triggered:
            try:
                from data.position_guardian import evaluate_position
                # 현재가: Eye 버퍼의 최신 바 → 없으면 진입가 fallback
                cp = 0
                if self._eye and code in self._eye._buffers:
                    latest = self._eye._buffers[code].latest
                    if latest:
                        cp = latest.price
                if not cp:
                    cp = pos.get("entry_price", 0)
                gv = await asyncio.to_thread(
                    evaluate_position,
                    code, name,
                    current_price=cp,
                    entry=pos.get("entry_price", 0),
                    tp1=pos.get("tp1", 0),
                    sl=pos.get("stop_loss", 0),
                    eye_risk_adj=eye_adj,
                )
                if gv.action == "EXIT":
                    # 즉시 전량 매도
                    pre_bal = self.trader.fetch_balance()
                    actual_qty = pos.get("qty", 1)
                    if pre_bal and pre_bal.get("success"):
                        for p_item in pre_bal.get("positions", []):
                            if p_item["code"] == code:
                                actual_qty = p_item.get("qty", 1)
                                break
                    # ★ 5/20 사고 fix: 사장님 보호 명령 체크 (Eye+Guardian EXIT) ★
                    # _eye_guardian_action은 단일 종목 함수 → return으로 차단 (continue 안 됨)
                    if self._is_sell_protected(code, "eye_guardian_EXIT"):
                        return
                    result = self.trader.liquidate_one(code)
                    if result and result.get("success"):
                        pi_exit = self.trader.fetch_price(code)
                        cp = pi_exit.get("current_price", 0) if pi_exit and pi_exit.get("success") else 0
                        if cp == 0:
                            cp = pos.get("entry_price", 0)
                        pnl = (cp - pos["entry_price"]) * actual_qty
                        self._record_trade_pnl(code, pnl, "eye_guardian")
                        self._positions.pop(code, None)
                        self._save_positions()
                        await self._alert(
                            f"🛑 [Eye+Guardian] {name} EXIT 매도\n"
                            f"   Eye: {verdict.summary}\n"
                            f"   Guardian risk={gv.risk_score:.0f} → {gv.key_reason}"
                        )
                    else:
                        logger.error(f"Eye+Guardian EXIT 매도 실패 {code}")
                    return
                elif gv.action == "REDUCE":
                    await self._alert(
                        f"⚠️ [Eye+Guardian] {name} REDUCE 권고\n"
                        f"   Eye: {verdict.summary}\n"
                        f"   Guardian risk={gv.risk_score:.0f}\n"
                        f"   수동 반매도 검토"
                    )
                    return
            except Exception as e:
                logger.warning(f"Eye+Guardian 연동 실패 {code}: {e}")

        # (2) 트레일링 SL 동적 조정 (verdict별 차별화)
        ap = verdict.action_params or {}
        new_sl = ap.get("trailing_sl", 0)
        if new_sl > 0 and new_sl > pos.get("trailing_sl", 0):
            pos["trailing_sl"] = new_sl
            ts = pos.get("target_state")
            if ts:
                ts.trailing_sl = max(ts.trailing_sl, new_sl)

    async def _send_eye_alert(self, verdict):
        """Eye 상태 전이 텔레그램 알림"""
        icon = "🚨" if verdict.verdict == "DYING" else "🔍"
        msg = (
            f"{icon} Eye: {verdict.name} "
            f"{verdict.prev_verdict} → {verdict.verdict}\n"
            f"  점수 {verdict.composite_score:.0f} "
            f"(L1:{verdict.l1_price_structure:.0f} "
            f"L2:{verdict.l2_volume_supply:.0f} "
            f"L3:{verdict.l3_momentum:.0f})\n"
            f"  {verdict.summary}"
        )
        await self._alert(msg)

    def _check_quick_exit(self, pos: dict, current_price: int, pnl_pct: float):
        """빠른 익절 모드 — 차트 영웅식 유연 회전 (3-tier).

        config.bot.quick_exit:
          - enabled: 시스템 활성화
          - mode: aggressive(기본) / balanced(50% 익절) / defensive(전량 익절)
          - auto_switch: 정보봇 위험등급에 따라 자동 모드 전환
          - auto_defensive_levels: ["CRISIS"]
          - auto_balanced_levels: ["DANGER", "WARNING"]

        Returns:
            (action, reason) or (None, None)
        """
        from strategies.dynamic_target import ACTION_PARTIAL_SELL, ACTION_FULL_SELL

        qe_cfg = (self.config.get("bot", {}) or {}).get("quick_exit", {}) or {}
        if not qe_cfg.get("enabled", False):
            return None, None

        mode = qe_cfg.get("mode", "aggressive")

        # 정보봇 위험등급 자동 전환
        if qe_cfg.get("auto_switch", True):
            try:
                from bot.risk_gate_helper import _get_client
                client = _get_client()
                if client:
                    level = client.get_current_level()
                    defensive_levels = qe_cfg.get("auto_defensive_levels", ["CRISIS"])
                    balanced_levels = qe_cfg.get("auto_balanced_levels", ["DANGER", "WARNING"])
                    if level in defensive_levels:
                        mode = "defensive"
                    elif level in balanced_levels:
                        mode = "balanced"
                    # 그 외 (NORMAL/CAUTION) → mode 유지 (기본 aggressive)
            except Exception:
                pass  # 자동 전환 실패 시 수동 모드 사용

        # aggressive: 기존 트레일링 그대로 (변환 없음)
        if mode == "aggressive":
            return None, None

        # balanced: +5% 도달 시 50% 부분 익절 (1회만)
        if mode == "balanced":
            partial_tp = float(qe_cfg.get("balanced_tp_partial", 5.0))
            if pnl_pct >= partial_tp and not pos.get("quick_exit_partial_done"):
                pos["quick_exit_partial_done"] = True  # 중복 방지
                return ACTION_PARTIAL_SELL, (
                    f"빠른익절(BALANCED) +{pnl_pct:.1f}% 도달 → 50% 익절"
                )

        # defensive: +5% 도달 시 100% 전량 익절
        if mode == "defensive":
            full_tp = float(qe_cfg.get("defensive_tp_full", 5.0))
            if pnl_pct >= full_tp:
                return ACTION_FULL_SELL, (
                    f"빠른익절(DEFENSIVE) +{pnl_pct:.1f}% 도달 → 전량 익절"
                )

        return None, None

    async def _job_monitor_fallback(self):
        """AI 모니터 실패 시 폴백: SL + 인트라데이 트레일링 스탑 체크"""
        for code, pos in list(self._positions.items()):
            try:
                price_info = self.trader.fetch_price(code)
                if not price_info or not price_info.get("success"):
                    continue

                cp = price_info["current_price"]
                entry = pos["entry_price"]
                name = pos.get("name", code)

                # ── 고점(high_watermark) 갱신 ──
                pos["high_watermark"] = max(pos.get("high_watermark", entry), cp)
                hwm = pos["high_watermark"]
                pnl_pct = (cp / entry - 1) * 100 if entry > 0 else 0

                # ── 인트라데이 트레일링 SL 계산 ──
                if pnl_pct >= 3.0 or pos.get("trailing_activated"):
                    pos["trailing_activated"] = True
                    trail_sl = int(hwm * 0.97)  # 고점 -3%
                    if pnl_pct >= 3.0 or hwm > entry * 1.03:
                        trail_sl = max(trail_sl, entry)  # 본전 확보
                    # ratchet: 절대 내려가지 않음
                    pos["trailing_sl"] = max(pos.get("trailing_sl", 0), trail_sl)

                    # target_state에도 동기화
                    ts = pos.get("target_state")
                    if ts:
                        ts.high_watermark = hwm
                        ts.trailing_activated = True
                        ts.trailing_sl = max(ts.trailing_sl, pos["trailing_sl"])

                # ── 유효 SL = max(기존 SL, 트레일링 SL) ──
                effective_sl = max(pos["stop_loss"], pos.get("trailing_sl", 0))

                # ── 5/19 사장님 명령: SL 비활성화 플래그 + manual_sync 종목 보호 ──
                # 일진전기 -25.10% 사고 (-626,400원) 후 사장님 결정:
                # "다른 종목들 -25%가 되어도 팔지마라. 2분기 실적 7월까지 들고 간다"
                if pos.get("sl_disabled") or pos.get("source", "").startswith("manual_sync"):
                    if cp <= effective_sl and effective_sl > 0:
                        logger.info(
                            f"[SL 면제] {name}({code}) — sl_disabled OR manual_sync. "
                            f"현재가 {cp:,} ≤ SL {effective_sl:,} 이지만 매도 차단."
                        )
                    continue  # SL 매도 절대 차단

                if cp <= effective_sl:
                    # 매도 전 실제 보유수량 조회
                    pre_bal = self.trader.fetch_balance()
                    actual_qty = pos.get("qty", 1)
                    if pre_bal and pre_bal.get("success"):
                        for p_item in pre_bal.get("positions", []):
                            if p_item["code"] == code:
                                actual_qty = p_item.get("qty", 1)
                                break
                    # 매도 실행
                    result = self.trader.liquidate_one(code)
                    if not result or not result.get("success"):
                        logger.error(f"SL 매도 실패 {code}: {result} — 포지션 유지")
                        continue
                    pnl = (cp - entry) * actual_qty
                    self._record_trade_pnl(code, pnl, "stop_loss")

                    # ── 5/19 사장님 일진전기 사고 후 추가: trade_journal + scalper_hook 적재 ──
                    # 기존 코드는 _record_trade_pnl만 호출 → DB에 매도 흔적 안 남음 (둘째형이
                    # "기록 0건"으로 잘못 답한 원인). 손절/트레일링도 매수와 동일 표준 적재.
                    sell_event_type = "sell_sl" if not pos.get("trailing_activated") else "sell_close"
                    try:
                        from data import trade_journal as _tj
                        _tj.log_sell(
                            code=code, name=name, qty=actual_qty,
                            sell_price=cp, buy_price=entry,
                            event_type=sell_event_type,
                            source=pos.get("source", "auto_trader"),
                            order_no=(result or {}).get("order_no") or (result or {}).get("ODNO"),
                            note=("trailing_stop" if pos.get("trailing_activated") else "stop_loss"),
                        )
                    except Exception as _tj_e:
                        logger.warning(f"[trade_journal] SL 매도 적재 실패: {_tj_e}")
                    try:
                        from utils.scalper_journal_hooks import hook_sell_execution
                        hook_sell_execution(
                            ticker=code, name=name, price=int(cp), qty=actual_qty,
                            buy_price=int(entry),
                            reason=("trailing_stop" if pos.get("trailing_activated") else "stop_loss"),
                            source=pos.get("source", "auto_trader"),
                        )
                    except Exception as _h_e:
                        logger.debug(f"[scalper_hooks] SL 매도 적재 실패: {_h_e}")

                    self._positions.pop(code, None)
                    self._save_positions()

                    if pos.get("trailing_activated"):
                        drop_pct = (1 - cp / hwm) * 100 if hwm > 0 else 0
                        await self._alert(
                            f"📉 트레일링스탑\n{name}({code}) @ {cp:,}원\n"
                            f"진입:{entry:,} → 고점:{hwm:,} → 매도:{cp:,}\n"
                            f"고점대비 -{drop_pct:.1f}% | 수익:{pnl:+,}원"
                        )
                    else:
                        await self._alert(
                            f"⛔ 손절\n{name}({code}) @ {cp:,}원\n"
                            f"진입:{entry:,} -> 현재:{cp:,} ({pnl:+,})"
                        )

                elif pos.get("mode") == "day" and cp >= pos["take_profit"] and not pos.get("trailing_activated", False):
                    # ★ 5/21 A1 fix: self.mode (봇 전역=swing) → pos.get("mode") (개별 종목) ★
                    # 어제 fix 버그: self.mode == "day"는 항상 False (config trade_mode=swing)
                    # → 모든 종목 elif 분기 진입 실패 → trailing TP fix 실효성 0
                    # 신규: pos.get("mode")로 개별 종목 모드 체크 → DAY_TRADE 종목 정상 진입
                    # 트레일링 활성 시 고정 TP +5% 매도 비활성화 (5/20 전력주 +10% 놓침 재발 방지)
                    # +3% 도달 → 트레일링 활성 → TP 무력화, 고점 -3% 추적
                    # 안전망: +3% 미달 시 (트레일링 비활성) TP +5% 도달하면 익절 (이론상 미발생)
                    # TP 분기 진입 시 actual_qty 보장 (if 분기에서 미정의일 수 있음)
                    tp_pre_bal = self.trader.fetch_balance()
                    tp_actual_qty = pos.get("qty", 1)
                    if tp_pre_bal and tp_pre_bal.get("success"):
                        for p_item in tp_pre_bal.get("positions", []):
                            if p_item["code"] == code:
                                tp_actual_qty = p_item.get("qty", 1)
                                break

                    # ★ 5/20 사고 fix: 사장님 보호 명령 체크 (TP 익절도 차단) ★
                    if self._is_sell_protected(code, "take_profit"):
                        continue
                    result = self.trader.liquidate_one(code)
                    if not result or not result.get("success"):
                        logger.error(f"TP 매도 실패 {code}: {result} — 포지션 유지")
                        continue
                    gain = (cp - entry) * tp_actual_qty
                    self._record_trade_pnl(code, gain, "take_profit")

                    # ── 5/19 사장님 일진전기 사고 후 추가: trade_journal + scalper_hook 적재 ──
                    try:
                        from data import trade_journal as _tj
                        _tj.log_sell(
                            code=code, name=name, qty=tp_actual_qty,
                            sell_price=cp, buy_price=entry,
                            event_type="sell_tp",
                            source=pos.get("source", "auto_trader"),
                            order_no=(result or {}).get("order_no") or (result or {}).get("ODNO"),
                            note="take_profit",
                        )
                    except Exception as _tj_e:
                        logger.warning(f"[trade_journal] TP 매도 적재 실패: {_tj_e}")
                    try:
                        from utils.scalper_journal_hooks import hook_sell_execution
                        hook_sell_execution(
                            ticker=code, name=name, price=int(cp), qty=tp_actual_qty,
                            buy_price=int(entry),
                            reason="take_profit",
                            source=pos.get("source", "auto_trader"),
                        )
                    except Exception as _h_e:
                        logger.debug(f"[scalper_hooks] TP 매도 적재 실패: {_h_e}")

                    self._positions.pop(code, None)
                    self._save_positions()
                    await self._alert(
                        f"익절\n{name}({code}) @ {cp:,}원\n"
                        f"진입:{entry:,} -> 현재:{cp:,} (+{gain:,})"
                    )

                # ── 자비스 추세 약화 매도 (5/20 사장님 결정) ──
                #   asset_pool 종목 전용: +1.5% 이상 수익 + 체결강도 < 90 → 매도
                #   "오를 것 같다 / 떨어질 것 같다" 판단의 첫 구현체
                #   고점 대비 -3% 트레일링 활성 전(+3% 미달) 단계에서도 약화 감지
                elif (
                    self.mode == "day"
                    and pos.get("source") == "asset_pool"
                    and pnl_pct >= 1.5
                    and price_info.get("strength", 100) < 90
                ):
                    strength_val = price_info.get("strength", 100)
                    logger.info(
                        f"[자비스 약화] {name}({code}) 매도 — "
                        f"수익 +{pnl_pct:.1f}% / 체결강도 {strength_val:.0f}"
                    )
                    jw_pre_bal = self.trader.fetch_balance()
                    jw_actual_qty = pos.get("qty", 1)
                    if jw_pre_bal and jw_pre_bal.get("success"):
                        for p_item in jw_pre_bal.get("positions", []):
                            if p_item["code"] == code:
                                jw_actual_qty = p_item.get("qty", 1)
                                break

                    # ★ 5/20 사고 fix: 사장님 보호 명령 체크 (자비스 약화도 차단) ★
                    if self._is_sell_protected(code, "jarvis_weak"):
                        continue
                    result = self.trader.liquidate_one(code)
                    if not result or not result.get("success"):
                        logger.error(f"[자비스 약화] 매도 실패 {code}: {result} — 포지션 유지")
                        continue

                    gain = (cp - entry) * jw_actual_qty
                    self._record_trade_pnl(code, gain, "jarvis_weak")

                    try:
                        from data import trade_journal as _tj
                        _tj.log_sell(
                            code=code, name=name, qty=jw_actual_qty,
                            sell_price=cp, buy_price=entry,
                            event_type="sell_jarvis_weak",
                            source=pos.get("source", "auto_trader"),
                            order_no=(result or {}).get("order_no") or (result or {}).get("ODNO"),
                            note=f"strength={strength_val:.0f} pnl={pnl_pct:+.1f}%",
                        )
                    except Exception as _tj_e:
                        logger.warning(f"[trade_journal] 자비스 약화 매도 적재 실패: {_tj_e}")

                    self._positions.pop(code, None)
                    self._save_positions()
                    await self._alert(
                        f"🦾 자비스 약화 감지 매도\n"
                        f"{name}({code}) @ {cp:,}원\n"
                        f"진입:{entry:,} → 현재:{cp:,} ({gain:+,})\n"
                        f"  · 수익 +{pnl_pct:.1f}% / 체결강도 {strength_val:.0f} (< 90)\n"
                        f"  · 자비스 판단: 추세 약화"
                    )

            except Exception as e:
                logger.error(f"폴백 감시 실패 {code}: {e}")

    async def job_daily_reeval(self, context):
        """스윙 모드: 장마감 전 동적 목표가 재평가 (15:00)

        뉴스 + 수급 + RSI/볼린저 → 목표가 재조정 → 판정
        """
        if not self.is_running or self.mode != "swing":
            return
        if not self._positions:
            return

        from strategies.dynamic_target import (
            DynamicTargetEngine, ACTION_FULL_SELL, ACTION_PARTIAL_SELL,
            ACTION_STOP_LOSS, ACTION_HOLD, ACTION_ADD,
        )

        engine = DynamicTargetEngine()
        lines = ["📊 동적 목표가 재평가"]
        max_hold = self.config.get("bot", {}).get("max_hold_days", 10)

        # ── REVERSAL 섹터 방어: 보유 종목이 반전 섹터에 속하면 SL 강화 ──
        reversal_codes = set()
        try:
            from data.rotation_detector import analyze_rotation, get_next_sector_stocks
            rotation = analyze_rotation()
            rot_stocks = get_next_sector_stocks(rotation)
            for rcode, rinfo in rot_stocks.items():
                if rinfo.get("rotation_source") == "reversal_exit":
                    reversal_codes.add(rcode)
        except Exception as e:
            logger.warning(f"로테이션 분석 실패 (REVERSAL 방어 스킵): {e}")

        # ── MOMENTUM 수급 이탈 체크 (기관+외인 순매도 전환) ──
        momentum_exit_codes = set()
        try:
            from data.regime_detector import check_supply_withdrawal
            for code, pos in list(self._positions.items()):
                if pos.get("regime") == "MOMENTUM":
                    if check_supply_withdrawal(code):
                        momentum_exit_codes.add(code)
                        logger.info(f"[MOMENTUM 수급이탈] {pos.get('name', code)}({code})")
        except Exception as e:
            logger.warning(f"수급 이탈 체크 실패: {e}")

        for code, pos in list(self._positions.items()):
            try:
                price_info = self.trader.fetch_price(code)
                if not price_info or not price_info.get("success"):
                    continue

                cp = price_info["current_price"]
                pnl = (cp / pos["entry_price"] - 1) * 100

                # 보유일 계산
                entry_date = pos.get("entry_date", "")
                if entry_date:
                    try:
                        dt = datetime.strptime(entry_date, "%Y-%m-%d %H:%M")
                    except ValueError:
                        dt = datetime.strptime(entry_date, "%Y-%m-%d")
                    hold_days = (datetime.now() - dt).days
                else:
                    hold_days = 0

                # 뉴스 감성
                news_score = self._get_news_score(code, pos.get("name", ""))

                # 동적 재평가 (트레일링 스탑 연동)
                target_state = pos.get("target_state")
                if target_state:
                    # ── 인트라데이에서 추적한 고점/트레일링을 target_state에 동기화 ──
                    target_state.high_watermark = max(
                        target_state.high_watermark,
                        pos.get("high_watermark", pos["entry_price"])
                    )
                    if pos.get("trailing_activated"):
                        target_state.trailing_activated = True
                    target_state.trailing_sl = max(
                        target_state.trailing_sl,
                        pos.get("trailing_sl", 0)
                    )

                    target_state = engine.daily_reeval(
                        target_state, cp, news_score=news_score
                    )
                    action = target_state.action
                    reason = target_state.reason
                    pos["target_state"] = target_state
                    pos["stop_loss"] = target_state.dynamic_sl
                    pos["take_profit"] = target_state.dynamic_tp

                    # ── 재평가 후 트레일링 상태를 position dict에 역동기화 ──
                    pos["high_watermark"] = target_state.high_watermark
                    pos["trailing_activated"] = target_state.trailing_activated
                    pos["trailing_sl"] = target_state.trailing_sl
                else:
                    action = ACTION_HOLD
                    reason = "타겟 상태 없음"

                # ── 빠른 익절 모드 (Quick Exit) — 차트 영웅식 유연 회전 ──
                # 사장님 수동 모드 또는 정보봇 위험등급에 따라 자동 익절
                try:
                    qe_action, qe_reason = self._check_quick_exit(pos, cp, pnl)
                    if qe_action:
                        action = qe_action
                        reason = qe_reason
                except Exception as _qe:
                    logger.warning(f"[Quick Exit] {code} 평가 실패: {_qe}")

                # ── REVERSAL 섹터 방어: 손실 중이면 즉시 청산, 수익 중이면 SL 강화 ──
                if code in reversal_codes and action not in (ACTION_FULL_SELL, ACTION_STOP_LOSS):
                    entry = pos["entry_price"]

                    if cp <= entry:
                        # 이미 물려있는 상태 → REVERSAL + 손실 = 즉시 청산
                        action = ACTION_FULL_SELL
                        reason = f"REVERSAL 섹터 + 손실({pnl:+.1f}%) → 즉시 정리"
                        lines.append(
                            f"  🔴 {pos.get('name', code)} REVERSAL 즉시 청산\n"
                            f"     진입:{entry:,} 현재:{cp:,} ({pnl:+.1f}%)"
                        )
                    else:
                        # 수익 중 → SL을 진입가로 올려 본전 확보 + TP 축소
                        reversal_sl = max(entry, int(cp * 0.98))
                        old_sl = pos["stop_loss"]
                        if reversal_sl > old_sl:
                            pos["stop_loss"] = reversal_sl
                            if target_state:
                                target_state.dynamic_sl = reversal_sl
                            lines.append(
                                f"  ⚠️ {pos.get('name', code)} REVERSAL 방어\n"
                                f"     SL 강화: {old_sl:,} → {reversal_sl:,}"
                            )
                        # TP도 축소 (현재가 +3%로 제한)
                        reversal_tp = int(cp * 1.03)
                        old_tp = pos["take_profit"]
                        if reversal_tp < old_tp:
                            pos["take_profit"] = reversal_tp
                            if target_state:
                                target_state.dynamic_tp = reversal_tp

                # ── MOMENTUM 수급 이탈 → 즉시 청산 ──
                if code in momentum_exit_codes and action not in (ACTION_FULL_SELL, ACTION_STOP_LOSS):
                    action = ACTION_FULL_SELL
                    reason = f"MOMENTUM 수급 이탈 (기관+외인 순매도 전환)"
                    lines.append(
                        f"  🔴 {pos.get('name', code)} [MTM] 수급 이탈 청산\n"
                        f"     진입:{pos['entry_price']:,} 현재:{cp:,} ({pnl:+.1f}%)"
                    )

                # 최대 보유일 초과 (모멘텀 포지션은 5일, 스윙은 config값)
                # REVERSAL 섹터 종목은 최대 3일로 단축
                # MOMENTUM 레짐 포지션은 5일로 제한
                # 매크로 전략 기반 보유일 단축
                effective_max = 5 if pos.get("source") == "momentum" else max_hold
                try:
                    from data.macro_strategy import get_adjusted_max_hold
                    effective_max = get_adjusted_max_hold(effective_max)
                except Exception:
                    pass
                if pos.get("regime") == "MOMENTUM":
                    effective_max = min(effective_max, 5)
                if code in reversal_codes:
                    effective_max = min(effective_max, 3)
                # ETF 보유일 제한 (레버리지/인버스=3일, 섹터=5일, 원자재=10일)
                if pos.get("is_etf"):
                    effective_max = min(effective_max, pos.get("holding_days", 5))
                if hold_days >= effective_max:
                    action = ACTION_FULL_SELL
                    reason = f"최대 보유일 {effective_max}일 도달" + (
                        " (REVERSAL 섹터)" if code in reversal_codes else ""
                    )

                # 판정 실행
                name = pos.get("name", code)
                icon = {
                    ACTION_HOLD: "🟢", ACTION_ADD: "🔵",
                    ACTION_PARTIAL_SELL: "🟡", ACTION_FULL_SELL: "🔴",
                    ACTION_STOP_LOSS: "⛔",
                }.get(action, "⚪")

                lines.append(
                    f"  {icon} {name}({code}) {pnl:+.1f}% D{hold_days}\n"
                    f"     {action} - {reason}\n"
                    f"     SL:{pos['stop_loss']:,} TP:{pos['take_profit']:,}"
                )

                if action == ACTION_STOP_LOSS:
                    # ★ 5/20 사고 fix: 사장님 보호 명령 체크 ★
                    if self._is_sell_protected(code, "daily_reeval_STOP_LOSS"):
                        continue
                    # 매도 전 수량 확인 (PnL 계산용)
                    pre_bal = self.trader.fetch_balance()
                    actual_qty = pos.get("qty", 1)
                    if pre_bal and pre_bal.get("success"):
                        for p_item in pre_bal.get("positions", []):
                            if p_item["code"] == code:
                                actual_qty = p_item.get("qty", 1)
                                break
                    result = self.trader.liquidate_one(code)
                    if result and result.get("success"):
                        realized_pnl = (cp - pos["entry_price"]) * actual_qty
                        self._record_trade_pnl(code, realized_pnl, "dynamic_sl")
                        # ── 5/19 사고 후 추가: trade_journal 적재 ──
                        try:
                            from data import trade_journal as _tj
                            _tj.log_sell(
                                code=code, name=name, qty=actual_qty,
                                sell_price=cp, buy_price=pos["entry_price"],
                                event_type="sell_sl",
                                source=pos.get("source", "auto_trader"),
                                order_no=(result or {}).get("order_no") or (result or {}).get("ODNO"),
                                note=f"dynamic_sl: {reason}",
                            )
                        except Exception as _tj_e:
                            logger.warning(f"[trade_journal] dynamic_sl 적재 실패: {_tj_e}")
                        try:
                            from utils.scalper_journal_hooks import hook_sell_execution
                            hook_sell_execution(
                                ticker=code, name=name, price=int(cp), qty=actual_qty,
                                buy_price=int(pos["entry_price"]), reason="dynamic_sl",
                                source=pos.get("source", "auto_trader"),
                            )
                        except Exception:
                            pass
                        self._positions.pop(code, None)
                        self._save_positions()
                        await self._alert(f"⛔ 동적 손절: {name}({code}) @ {cp:,} (PnL {realized_pnl:+,}원)")
                    else:
                        logger.error(f"동적 손절 매도 실패 {code}: {result}")
                        await self._alert(f"❌ 손절 매도 실패: {name}({code}) — 수동 확인 필요")
                elif action == ACTION_FULL_SELL:
                    # ★ 5/20 사고 fix: 사장님 보호 명령 체크 ★
                    if self._is_sell_protected(code, "daily_reeval_FULL_SELL"):
                        continue
                    # 체제별 반분할 익절: TP 히트 + partial_tp 체제 + 미분할 상태
                    from data.market_health import get_regime_rules as _get_rules
                    _rules = _get_rules()
                    is_tp_hit = pnl > 0 and "트레일링" not in reason and "하드스탑" not in reason
                    if (is_tp_hit and _rules.get("partial_tp", False)
                            and not pos.get("partial_sold", False)):
                        # 반분할: 50%만 매도, 나머지 트레일링
                        bal_pt = self.trader.fetch_balance()
                        for p in bal_pt.get("positions", []):
                            if p["code"] == code:
                                half = max(1, p["qty"] // 2)
                                sell_r = self.trader.smart_sell(code, half)
                                if sell_r and sell_r.get("success"):
                                    pos["partial_sold"] = True
                                    partial_pnl = (cp - pos["entry_price"]) * half
                                    self._record_trade_pnl(code, partial_pnl, "partial_tp")
                                    await self._alert(
                                        f"🟡 반분할 익절: {name}({code}) {half}주 @ {cp:,}\n"
                                        f"   나머지 트레일링 전환 ({reason}, PnL {partial_pnl:+,}원)"
                                )
                                break
                    else:
                        # 매도 전 수량 확인 (PnL 계산용)
                        pre_bal_fs = self.trader.fetch_balance()
                        actual_qty_fs = 1
                        for p_item in (pre_bal_fs or {}).get("positions", []):
                            if p_item["code"] == code:
                                actual_qty_fs = p_item.get("qty", 1)
                                break
                        result = self.trader.liquidate_one(code)
                        if not result or not result.get("success"):
                            logger.error(f"동적 전량매도 실패 {code}: {result} — 포지션 유지")
                            continue
                        realized_pnl = (cp - pos["entry_price"]) * actual_qty_fs
                        self._record_trade_pnl(code, realized_pnl, "dynamic_sell")
                        # ── 5/19 사고 후 추가: trade_journal 적재 ──
                        try:
                            from data import trade_journal as _tj
                            _tj.log_sell(
                                code=code, name=name, qty=actual_qty_fs,
                                sell_price=cp, buy_price=pos["entry_price"],
                                event_type="sell_close",
                                source=pos.get("source", "auto_trader"),
                                order_no=(result or {}).get("order_no") or (result or {}).get("ODNO"),
                                note=f"dynamic_sell: {reason}",
                            )
                        except Exception as _tj_e:
                            logger.warning(f"[trade_journal] dynamic_sell 적재 실패: {_tj_e}")
                        try:
                            from utils.scalper_journal_hooks import hook_sell_execution
                            hook_sell_execution(
                                ticker=code, name=name, price=int(cp),
                                qty=actual_qty_fs, buy_price=int(pos["entry_price"]),
                                reason="dynamic_sell",
                                source=pos.get("source", "auto_trader"),
                            )
                        except Exception:
                            pass
                        self._positions.pop(code, None)
                        self._save_positions()
                        await self._alert(f"🔴 동적 전량매도: {name}({code}) @ {cp:,} ({reason}, PnL {realized_pnl:+,}원)")
                elif action == ACTION_ADD:
                    # manual_sync 종목 자동 추격매수 면제 (사장님 5/19 08:30 "전력주 가만히 둬" 결정)
                    # — 전력주 5종목(대한전선/SKC/제룡전기/산일전기/일진전기) 자연 반등 대기 보호
                    # — EYE+Guardian SKIP과 동일 정책: SL/TP만 청산 트리거
                    if pos.get("source", "").startswith("manual_sync"):
                        logger.info(
                            f"[ACTION_ADD SKIP] {name}({code}) — manual_sync 보유, "
                            f"자동 추격매수 면제 (사장님 가만히 둬)"
                        )
                        continue

                    # 5/19 D-Day 신규 — ACTION_ADD 일일 한도 (사장님 "추매 2종목" 컨셉)
                    _max_add = int(self.config.get("bot", {}).get("action_add_max_per_day", 999))
                    _today_str = date.today().isoformat()
                    if not hasattr(self, "_action_add_history"):
                        self._action_add_history = {}
                    _today_add_count = self._action_add_history.get(_today_str, 0)
                    if _today_add_count >= _max_add:
                        logger.info(
                            f"[ACTION_ADD SKIP] {name}({code}) — "
                            f"일일 추매 한도 도달 ({_today_add_count}/{_max_add})"
                        )
                        continue

                    # ── ACTION_ADD multi-signal 검증 (잘못된 추격매수 방지) ──
                    # 업사이드 8%+ 조건만으로는 부족 → 장중 시그널(AI/강도/호가/VWAP) 추가 검증
                    try:
                        rao_full_cfg = self.config.get("bot", {}).get("recovery_add_on", {}) or {}
                        ms_cfg = rao_full_cfg.get("multi_signal", {}) or {}
                        if ms_cfg.get("enabled", True):
                            rtm_add = self._get_rt_monitor()
                            snap_add = await asyncio.to_thread(rtm_add.evaluate_position, code)
                            if snap_add:
                                from bot.recovery_add_on import evaluate_signals, load_config as _load_rao_cfg
                                snap_data_add = {
                                    "realtime_score": snap_add.realtime_score,
                                    "strength": snap_add.strength,
                                    "bid_qty": snap_add.bid_qty,
                                    "ask_qty": snap_add.ask_qty,
                                    "price": snap_add.price,
                                    "vwap": snap_add.vwap,
                                }
                                cfg_full_add = _load_rao_cfg(self.config.get("bot", {}))
                                ms_score, _ = evaluate_signals(snap_data_add, cfg_full_add)
                                min_score = int(ms_cfg.get("min_score_for_action_add", 3))
                                if ms_score < min_score:
                                    logger.info(
                                        f"[ACTION_ADD SKIP] {name}({code}) 시그널 부족 "
                                        f"({ms_score}/4 < {min_score}) — 추격매수 건너뜀"
                                    )
                                    continue  # 다음 종목으로
                    except Exception as _ms:
                        logger.warning(f"[ACTION_ADD multi-signal] {code} 평가 실패: {_ms}")

                    # ── 추매: 업사이드 8%+ → 추가 매수 실행 ──
                    risk_ok, risk_reason = self.check_risk_gate()
                    if risk_ok:
                        bal_add = self.trader.fetch_balance()
                        add_cash = bal_add.get("cash", 0) if bal_add.get("success") else 0
                        # 추매 금액 = 가용 현금 × add_on_buy_ratio (config, 기본 30%)
                        add_ratio = self.config.get("bot", {}).get("add_on_buy_ratio", 0.30)
                        add_amount = int(add_cash * add_ratio)
                        if add_amount >= 100000:
                            if self._confirm_auto:
                                self._pending_auto_buys.append({
                                    "code": code, "name": name,
                                    "amount": add_amount,
                                    "sl": pos["stop_loss"],
                                    "tp": pos["take_profit"],
                                    "tp1_quick": pos["take_profit"],
                                    "score": 0,
                                    "regime": pos.get("regime", "NORMAL"),
                                    "source": pos.get("source", "add_on"),
                                })
                                # 5/19 — 일일 추매 카운트 증가 (pending 예약 시점)
                                self._action_add_history[_today_str] = _today_add_count + 1
                                await self._alert(
                                    f"🔵 추매 확인 대기: {name}({code})\n"
                                    f"   {reason}\n"
                                    f"   현재 {cp:,}원 ({pnl:+.1f}%)\n"
                                    f"   추매 금액: {add_amount:,}원\n"
                                    f"   (오늘 추매 {_today_add_count + 1}/{_max_add})\n\n"
                                    f"   실행: '자동확인' | 취소: '자동취소'"
                                )
                            else:
                                result = self.trader.safe_buy(code, add_amount)
                                if result.get("success"):
                                    # 5/19 — 일일 추매 카운트 증가 (직접 매수 성공)
                                    self._action_add_history[_today_str] = _today_add_count + 1
                                    await self._alert(
                                        f"🔵 추매 완료: {name}({code}) @ {cp:,}원\n"
                                        f"   {reason}\n"
                                        f"   추매 금액: {add_amount:,}원\n"
                                        f"   (오늘 추매 {_today_add_count + 1}/{_max_add})"
                                    )
                        else:
                            await self._alert(
                                f"🔵 추매 판정: {name}({code}) - 현금 부족({add_cash:,}원)"
                            )
                    else:
                        await self._alert(
                            f"🔵 추매 판정: {name}({code}) - 리스크 차단: {risk_reason}"
                        )

                elif action == ACTION_PARTIAL_SELL:
                    # 부분매도: 보유수량의 50% (스마트 지정가)
                    bal = self.trader.fetch_balance()
                    for p in bal.get("positions", []):
                        if p["code"] == code:
                            half = max(1, p["qty"] // 2)
                            sell_r = self.trader.smart_sell(code, half)
                            if sell_r and sell_r.get("success"):
                                await self._alert(f"🟡 부분매도: {name}({code}) {half}주 @ {cp:,}")
                            else:
                                logger.error(f"재평가 부분매도 실패 {code}: {sell_r}")
                            break

            except Exception as e:
                logger.error(f"재평가 실패 {code}: {e}")

        await self._alert("\n".join(lines))

    async def job_eod_close(self, context):
        """장마감 처리

        데이 모드: 전량 청산
        스윙 모드: 요약만 (청산하지 않음)
        """
        if not self.is_running:
            return

        if self.mode == "day":
            # 데이 모드: 전량 청산 (preclose/predawn 태그 포지션은 제외)
            keep_sources = ("preclose", "predawn")
            preclose_codes = {c for c, p in self._positions.items()
                              if p.get("source") in keep_sources}
            if preclose_codes:
                logger.info(f"장마감 청산 ({len(preclose_codes)}종목 제외: preclose/predawn)")
                await self._alert(
                    f"🏁 장마감 청산 시작...\n"
                    f"  ({len(preclose_codes)}종목 제외 — 내일용)"
                )
                # preclose 제외하고 개별 청산 + PnL 기록
                eod_total_pnl = 0
                for code, pos in list(self._positions.items()):
                    if code in preclose_codes:
                        continue
                    # ★ 5/20 사고 fix: 사장님 보호 명령 체크 ★
                    if self._is_sell_protected(code, "eod_close_individual"):
                        continue
                    try:
                        # 매도 전 수량/현재가 확인
                        pi = self.trader.fetch_price(code)
                        cp_eod = pi.get("current_price", 0) if pi and pi.get("success") else 0
                        bal_eod = self.trader.fetch_balance()
                        qty_eod = pos.get("qty", 1)  # fallback: 내부 positions 데이터
                        if bal_eod and bal_eod.get("success"):
                            for p_item in bal_eod.get("positions", []):
                                if p_item["code"] == code:
                                    qty_eod = p_item.get("qty", 1)
                                    break
                        result_one = self.trader.liquidate_one(code)
                        if result_one and result_one.get("success"):
                            if cp_eod > 0:
                                pnl_eod = (cp_eod - pos.get("entry_price", cp_eod)) * qty_eod
                                self._record_trade_pnl(code, pnl_eod, "eod_close")
                                eod_total_pnl += pnl_eod
                            self._positions.pop(code, None)
                        else:
                            logger.warning(f"EOD 청산 실패 {code}: {result_one} — 포지션 유지")
                    except Exception as e:
                        logger.warning(f"EOD 청산 예외 {code}: {e} — 포지션 유지")
                self._save_positions()
                if eod_total_pnl != 0:
                    logger.info(f"[EOD] 청산 총 PnL: {eod_total_pnl:+,}원")
                result = {"success": True, "message": "preclose 제외 청산 완료"}
            else:
                # ★ 5/20 사고 fix: liquidate_all 우회 — 사장님 보호 종목 제외 ★
                protected_codes = [c for c in self._positions
                                   if self._is_sell_protected(c, "eod_close_liquidate_all")]
                if protected_codes:
                    logger.warning(
                        f"[EOD] 보호 종목 {len(protected_codes)}개 제외 후 개별 청산: "
                        f"{protected_codes}"
                    )
                    await self._alert(
                        f"🏁 장마감 청산 시작...\n"
                        f"  🛡️ 보호 종목 {len(protected_codes)}개 제외 (사장님 명령)"
                    )
                    eod_total_pnl_p = 0
                    for code, pos in list(self._positions.items()):
                        if code in protected_codes:
                            continue
                        try:
                            pi = self.trader.fetch_price(code)
                            cp_eod = pi.get("current_price", 0) if pi and pi.get("success") else 0
                            bal_eod = self.trader.fetch_balance()
                            qty_eod = pos.get("qty", 1)
                            if bal_eod and bal_eod.get("success"):
                                for p_item in bal_eod.get("positions", []):
                                    if p_item["code"] == code:
                                        qty_eod = p_item.get("qty", 1)
                                        break
                            result_one = self.trader.liquidate_one(code)
                            if result_one and result_one.get("success"):
                                if cp_eod > 0:
                                    pnl_eod = (cp_eod - pos.get("entry_price", cp_eod)) * qty_eod
                                    self._record_trade_pnl(code, pnl_eod, "eod_close")
                                    eod_total_pnl_p += pnl_eod
                                self._positions.pop(code, None)
                        except Exception as e:
                            logger.warning(f"EOD 보호제외 청산 예외 {code}: {e}")
                    self._save_positions()
                    if eod_total_pnl_p != 0:
                        logger.info(f"[EOD] 보호제외 청산 총 PnL: {eod_total_pnl_p:+,}원")
                    result = {"success": True,
                              "message": f"보호 {len(protected_codes)}종목 제외 청산 완료"}
                else:
                    logger.info("장마감 전량 청산")
                    await self._alert("🏁 장마감 전량 청산 시작...")
                    result = self.trader.liquidate_all()
                    if result.get("success"):
                        self._positions.clear()
                    else:
                        # 부분 실패 — 성공한 종목만 제거
                        failed = set(result.get("failed_codes", []))
                        for code in list(self._positions.keys()):
                            if code not in failed:
                                self._positions.pop(code, None)
                        logger.warning(f"전량 청산 부분실패: {failed}")
                    self._save_positions()
            await self._alert(f"{'✅' if result.get('success') else '❌'} {result.get('message')}")
        else:
            # 스윙 모드: 요약만
            logger.info("장마감 스윙 요약")

        # 일일 요약 (공통)
        bal = self.trader.fetch_balance()
        if bal.get("success"):
            pos_lines = []
            for p in bal.get("positions", []):
                pos_lines.append(f"  {p.get('name', p['code'])} {p['qty']}주 {p.get('pnl_rate', 0):+.1f}%")

            await self._alert(
                f"📊 일일 마감 ({self.mode} 모드)\n"
                f"현금: {bal['cash']:,}원\n"
                f"총평가: {bal['total_eval']:,}원\n"
                f"보유: {len(bal['positions'])}종목\n"
                + ("\n".join(pos_lines) if pos_lines else "")
            )

    # ═══════════════════════════════════════
    #  추천 파이프라인 (저녁분석 + 미국장 + 아침확인)
    # ═══════════════════════════════════════

    async def job_evening_analysis(self, context):
        """Stage 1: 저녁 분석 (16:45) - 5단계 추천 파이프라인

        한국장 마감 + 데이터 수집 완료 후 실행
        릴레이 → 사전감지 → 기술필터 → 뉴스AI → 교차검증
        """
        if not is_trading_day():
            return

        chat_id = None
        if not self._send_alert:
            import os
            chat_id = os.getenv("TELEGRAM_CHAT_ID")

        async def _send(text):
            try:
                if self._send_alert:
                    await self._send_alert(text)
                elif chat_id:
                    await context.bot.send_message(chat_id=chat_id, text=text)
            except Exception as e_tg:
                logger.warning(f"[C13] 텔레그램 전송 실패 (무시): {e_tg}")

        await _send("🌙 저녁 분석 시작 - 5단계 추천 파이프라인...")

        try:
            from data.morning_recommendation import (
                run_evening_recommendation, format_recommendation,
                save_recommendation,
            )
            report = await asyncio.to_thread(run_evening_recommendation)
            save_recommendation(report)

            # FLOWX 시그널 로깅 (추천 → signals 테이블)
            try:
                from data.flowx_signals import log_quant_signals
                await asyncio.to_thread(log_quant_signals)
            except Exception as e_sig:
                logger.warning(f"[FLOWX] 시그널 로깅 실패 (무시): {e_sig}")

            msg = format_recommendation(report)
            # 긴 메시지 분할
            if len(msg) > 4000:
                for i in range(0, len(msg), 4000):
                    await _send(msg[i:i + 4000])
            else:
                await _send(msg)

            if report.stocks:
                await _send(
                    f"💡 06:30 미국장 체크 → 조정 여부 알림\n"
                    f"   09:00 장 시작 → 실시간 관찰 → 조건 충족 시 매수\n"
                    f"   매수 금액: 장 시작 시 실제 잔고 기반 자동 계산"
                )

            # 국적별 수급 보고 (추천 + 보유 종목)
            try:
                await self._report_nationality_signal(_send)
            except Exception as e2:
                logger.warning(f"국적별 수급 보고 실패 (무시): {e2}")

        except Exception as e:
            logger.error(f"저녁 분석 실패: {e}")
            await _send(f"❌ 저녁 분석 실패: {e}")

    async def _report_nationality_signal(self, _send):
        """국적별 수급 변화 보고 (저녁분석 후 자동 실행)

        추천 종목 + 보유 종목의 전일 대비 국적별 거래량 변화 보고
        """
        from data.nationality_signal import (
            collect_daily_snapshots, generate_nationality_report,
            score_nationality_batch,
        )

        # 대상 종목 = 추천 + 보유 (중복 제거)
        target_codes = set()
        target_names = {}

        # 추천 종목
        try:
            from data.morning_recommendation import load_recommendation
            rec = load_recommendation()
            if rec and rec.stocks:
                for s in rec.stocks:
                    target_codes.add(s.code)
                    target_names[s.code] = s.name
        except Exception:
            pass

        # 보유 종목
        if self.trader:
            try:
                bal = self.trader.fetch_balance()
                for pos in bal.get("positions", []):
                    code = pos.get("code", "")
                    if code:
                        target_codes.add(code)
                        if code not in target_names:
                            target_names[code] = pos.get("name", code)
            except Exception:
                pass

        if not target_codes:
            return

        target_list = list(target_codes)

        # 스냅샷 수집 (오늘 + 전일 2일치)
        await _send(f"🌍 국적별 수급 분석 중... ({len(target_list)}종목)")

        from data.nationality_signal import _get_latest_data_date
        date_new = _get_latest_data_date()

        await asyncio.to_thread(collect_daily_snapshots, target_list, date_new)

        # 전일 스냅샷도 수집 (없으면 비교 불가)
        from data.nationality_signal import _find_prev_trading_day
        date_old = _find_prev_trading_day(date_new)
        if date_old:
            await asyncio.to_thread(collect_daily_snapshots, target_list, date_old)

        # 보고서 생성
        report = await asyncio.to_thread(
            generate_nationality_report, target_list, target_names, date_new,
        )

        if report:
            # 긴 메시지 분할
            if len(report) > 4000:
                for i in range(0, len(report), 4000):
                    await _send(report[i:i + 4000])
            else:
                await _send(report)

        # 점수 요약
        scores = await asyncio.to_thread(
            score_nationality_batch, target_list, date_new,
        )
        if scores:
            score_lines = []
            for code in target_list:
                sc, reason = scores.get(code, (0, ""))
                name = target_names.get(code, code)
                if sc != 0:
                    score_lines.append(f"  {name}: {sc:+.0f}점 ({reason})")
            if score_lines:
                await _send(
                    "📊 국적별 수급 점수\n" + "\n".join(score_lines)
                )

    async def job_us_market_check(self, context):
        """Stage 2: 미국장 체크 (06:30) - 전일 저녁 추천 조정

        미국 S&P500/나스닥/VIX 체크 → 추천 조정/유지
        """
        if not is_trading_day():
            return

        chat_id = None
        if not self._send_alert:
            import os
            chat_id = os.getenv("TELEGRAM_CHAT_ID")

        async def _send(text):
            if self._send_alert:
                await self._send_alert(text)
            elif chat_id:
                await context.bot.send_message(chat_id=chat_id, text=text)

        try:
            from data.morning_recommendation import (
                load_recommendation, run_us_market_check,
                format_recommendation, save_recommendation,
            )

            prev = load_recommendation()
            if not prev or not prev.stocks:
                await _send("🇺🇸 미국장 체크: 저녁 추천 없음 - 스킵")
                return

            report = await asyncio.to_thread(run_us_market_check, prev)
            save_recommendation(report)

            msg = format_recommendation(report)
            await _send(msg)

            if report.warning:
                await _send(f"⚠️ 주의: {report.warning}")
            else:
                await _send("✅ 미국장 정상 - 저녁 추천 유지")

            # ETF 시그널 별도 전송
            if report.etf_signal and report.etf_signal.get("signal") != "HOLD":
                try:
                    from strategies.crisis_etf_signal import (
                        CrisisSignal, format_signal_telegram
                    )
                    sig = CrisisSignal(**{
                        k: v for k, v in report.etf_signal.items()
                        if k in CrisisSignal.__dataclass_fields__
                    })
                    await _send(format_signal_telegram(sig))
                except Exception as e2:
                    logger.warning(f"ETF 시그널 전송 실패: {e2}")

            # FLOWX 모닝 브리핑 생성 + 업로드
            try:
                from data.flowx_briefing import run_morning_briefing
                result = await asyncio.to_thread(
                    run_morning_briefing, upload=True, telegram=True
                )
                if result:
                    logger.info(f"[FLOWX] 모닝 브리핑 업로드 완료: {result['date']}")
                else:
                    logger.warning("[FLOWX] 모닝 브리핑 생성 실패")
            except Exception as e_fx:
                logger.error(f"[FLOWX] 브리핑 실패: {e_fx}")

        except Exception as e:
            logger.error(f"미국장 체크 실패: {e}")
            await _send(f"❌ 미국장 체크 실패: {e}")

    # ═══════════════════════════════════════
    #  JARVIS BRAIN 자본 배분
    # ═══════════════════════════════════════

    def _load_brain_allocation(self) -> dict:
        """brain_allocation.json 로드 - 매수금액 캡에 사용
        FIX-02: data_store 우선 → jarvis fallback"""
        # 1차: market_brain이 생성한 data_store/brain_allocation.json
        primary = BASE_DIR / "data_store" / "brain_allocation.json"
        # 2차: 기존 jarvis/brain 호환 (레거시)
        fallback = BASE_DIR.parent / "jarvis" / "data" / "brain_allocation.json"

        for path in (primary, fallback):
            if not path.exists():
                continue
            try:
                with open(path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                # 관망 모드(0%): 완전 매수 차단 플래그 추가
                if data.get("position_size_pct", 100) == 0:
                    data["_block_all_buys"] = True
                return data
            except Exception as e:
                logger.warning(f"BRAIN 배분 로드 실패 ({path.name}): {e}")
        return {}

    async def _run_brain_after_nightwatch(self, _send):
        """NIGHTWATCH 완료 후 BRAIN 자본 배분 자동 실행"""
        import sys
        sys.path.insert(0, str(BASE_DIR.parent))

        from jarvis.brain import run_brain

        # 총 자금: 실시간 잔고 기준 (현금 + 평가액)
        bal = self.trader.fetch_balance()
        if bal.get("success"):
            total = bal.get("cash", 0) + sum(
                p.get("eval_amount", 0) for p in bal.get("positions", [])
            )
            total = max(total, 500_000)  # 최소 50만원
        else:
            total = 1_150_000  # 조회 실패 시 기본값

        result = await asyncio.to_thread(run_brain, None, total)

        msg = result.get("telegram_message", "")
        if msg:
            await _send(msg)
        logger.info(
            f"[BRAIN] 자동 실행 완료: {result['effective_regime']} "
            f"(자금 {total:,}원)"
        )

        # jarvis brain_allocation → data_store 동기화
        try:
            import shutil
            src = BASE_DIR.parent / "jarvis" / "data" / "brain_allocation.json"
            dst = BASE_DIR / "data_store" / "brain_allocation.json"
            if src.exists():
                shutil.copy2(src, dst)
                logger.info("[BRAIN] data_store/brain_allocation.json 동기화 완료")
        except Exception as e:
            logger.warning(f"[BRAIN] data_store 동기화 실패: {e}")

        # brain_report.json 독립 생성 (C13 타임아웃과 무관하게 보장)
        try:
            from data.market_brain import generate_brain_report, save_brain_report
            brain = await asyncio.to_thread(generate_brain_report)
            await asyncio.to_thread(save_brain_report, brain)
            logger.info(
                f"[BRAIN] brain_report.json 독립 생성 완료: "
                f"{brain.overall_verdict[:50]} | 비중 {brain.position_size_pct}%"
            )
        except Exception as e:
            logger.warning(f"[BRAIN] brain_report.json 독립 생성 실패: {e}")

    async def job_brain_allocation(self, context):
        """16:36 - BRAIN 자본 배분 백업 스케줄 (NIGHTWATCH 실패 대비)"""
        if not is_trading_day():
            return

        # NIGHTWATCH에서 이미 실행했으면 스킵
        brain_path = BASE_DIR.parent / "jarvis" / "data" / "brain_allocation.json"
        if brain_path.exists():
            try:
                with open(brain_path, "r", encoding="utf-8") as f:
                    alloc = json.load(f)
                alloc_date = alloc.get("timestamp", "")[:10]
                if alloc_date == date.today().isoformat():
                    logger.info("[BRAIN] 오늘 이미 실행됨 - 백업 스킵")
                    return
            except Exception:
                pass

        chat_id = None
        if not self._send_alert:
            import os
            chat_id = os.getenv("TELEGRAM_CHAT_ID")

        async def _send(text):
            if self._send_alert:
                await self._send_alert(text)
            elif chat_id:
                await context.bot.send_message(chat_id=chat_id, text=text)

        try:
            await _send("BRAIN 백업 실행 (NIGHTWATCH 미실행)")
            await self._run_brain_after_nightwatch(_send)
        except Exception as e:
            logger.error(f"BRAIN 백업 실패: {e}")
            await _send(f"BRAIN 백업 실패: {e}")

    # ═══════════════════════════════════════
    #  ICT 프리미엄 레벨 + Opening Range
    # ═══════════════════════════════════════

    async def job_premium_levels(self, context):
        """08:30 - 전일/전주/전월 프리미엄 레벨 계산"""
        if not is_trading_day():
            return

        chat_id = None
        if not self._send_alert:
            import os
            chat_id = os.getenv("TELEGRAM_CHAT_ID")

        async def _send(text):
            if self._send_alert:
                await self._send_alert(text)
            elif chat_id:
                await context.bot.send_message(chat_id=chat_id, text=text)

        try:
            from strategies.premium_levels import run_premium_levels, load_premium_levels, PL_DIR
            results = await asyncio.to_thread(run_premium_levels)

            # Equal Level 탐지 + PL 머지
            eq_count = 0
            try:
                from strategies.equal_level_detector import (
                    run_equal_levels, merge_eq_to_premium_levels,
                )
                eq_results = await asyncio.to_thread(run_equal_levels)
                if eq_results:
                    pl_data = load_premium_levels()
                    for code, eq_info in eq_results.items():
                        if code in pl_data:
                            merge_eq_to_premium_levels(pl_data[code], eq_info)
                            eq_count += 1
                    if eq_count > 0:
                        out_path = PL_DIR / f"{date.today().isoformat()}.json"
                        with open(out_path, "w", encoding="utf-8") as f:
                            json.dump(pl_data, f, ensure_ascii=False, indent=2)
            except Exception as e:
                logger.warning(f"Equal Level 실패: {e}")

            logger.info(f"프리미엄 레벨 {len(results)}종목 완료 (EQ {eq_count}종목 머지)")
        except Exception as e:
            logger.error(f"프리미엄 레벨 실패: {e}")

    async def job_gap_support(self, context):
        """09:05 - 갭 지지/저항 탐지 + PL 머지"""
        if not is_trading_day():
            return

        chat_id = None
        if not self._send_alert:
            import os
            chat_id = os.getenv("TELEGRAM_CHAT_ID")

        async def _send(text):
            if self._send_alert:
                await self._send_alert(text)
            elif chat_id:
                await context.bot.send_message(chat_id=chat_id, text=text)

        try:
            from strategies.gap_support import run_gap_detection, merge_gap_to_premium_levels
            gap_results = await asyncio.to_thread(run_gap_detection)

            # PL 머지
            merged = 0
            if gap_results:
                try:
                    from strategies.premium_levels import load_premium_levels, PL_DIR
                    pl_data = load_premium_levels()
                    for code, gap_info in gap_results.items():
                        if code in pl_data:
                            merge_gap_to_premium_levels(pl_data[code], gap_info)
                            merged += 1
                    if merged > 0:
                        out_path = PL_DIR / f"{date.today().isoformat()}.json"
                        with open(out_path, "w", encoding="utf-8") as f:
                            json.dump(pl_data, f, ensure_ascii=False, indent=2)
                except Exception as e:
                    logger.warning(f"GAP→PL 머지 실패: {e}")

            up = sum(1 for r in gap_results.values() if r["gap_type"] == "gap_up")
            down = len(gap_results) - up
            logger.info(f"갭 레벨 {len(gap_results)}종목 (갭업:{up} 갭다운:{down} PL머지:{merged})")
        except Exception as e:
            logger.error(f"갭 탐지 실패: {e}")

    async def job_opening_range(self, context):
        """10:05 - OR/IR 확정 + daily_bias 계산 (Silent)"""  # SILENT: MSG-REDUX
        if not is_trading_day():
            return

        try:
            from strategies.opening_range import run_opening_range
            results = await asyncio.to_thread(run_opening_range)

            bullish = sum(1 for r in results.values() if r["daily_bias"] == "bullish")
            bearish = sum(1 for r in results.values() if r["daily_bias"] == "bearish")
            neutral = len(results) - bullish - bearish

            # OR/IR을 프리미엄 레벨에 병합
            try:
                from strategies.premium_levels import (
                    load_premium_levels, merge_or_levels, PL_DIR,
                )
                pl_data = load_premium_levels()
                merged = 0
                for code, or_info in results.items():
                    if code in pl_data:
                        merge_or_levels(pl_data[code], or_info)
                        merged += 1
                if merged > 0:
                    out_path = PL_DIR / f"{date.today().isoformat()}.json"
                    with open(out_path, "w", encoding="utf-8") as f:
                        json.dump(pl_data, f, ensure_ascii=False, indent=2)
            except Exception as e:
                logger.warning(f"OR→PL 병합 실패: {e}")

            logger.info(f"OR/IR 확정 {len(results)}종목 (bull:{bullish} bear:{bearish} neutral:{neutral})")
        except Exception as e:
            logger.error(f"Opening Range 실패: {e}")

    # ═══════════════════════════════════════
    #  NIGHTWATCH NXT 야간매매
    # ═══════════════════════════════════════

    async def job_nightwatch_collect(self, context):
        """16:00 - 유럽장 개장, NIGHTWATCH 데이터 수집 시작"""
        if not is_trading_day():
            return

        nw_cfg = self.config.get("nightwatch", {})
        if not nw_cfg.get("enabled", False):
            return

        chat_id = None
        if not self._send_alert:
            import os
            chat_id = os.getenv("TELEGRAM_CHAT_ID")

        async def _send(text):
            if self._send_alert:
                await self._send_alert(text)
            elif chat_id:
                await context.bot.send_message(chat_id=chat_id, text=text)

        try:
            logger.info("NIGHTWATCH 수집 시작 (유럽장 개장)")

            from data.nightwatch import collect_asian_risk
            asian_score, asian_detail = await asyncio.to_thread(collect_asian_risk)
            logger.info(f"NIGHTWATCH 1단 아시안 리스크: {asian_score:+.1f}")

        except Exception as e:
            logger.error(f"NIGHTWATCH 수집 실패: {e}")

    async def job_nightwatch_decide(self, context):
        """16:35 - NIGHTWATCH 최종 판단 + NXT 매수 결정"""
        if not is_trading_day():
            return

        nw_cfg = self.config.get("nightwatch", {})
        if not nw_cfg.get("enabled", False):
            return

        chat_id = None
        if not self._send_alert:
            import os
            chat_id = os.getenv("TELEGRAM_CHAT_ID")

        async def _send(text):
            if self._send_alert:
                await self._send_alert(text)
            elif chat_id:
                await context.bot.send_message(chat_id=chat_id, text=text)

        try:
            from data.nightwatch import run_nightwatch, format_nightwatch_report

            report = await asyncio.to_thread(run_nightwatch)
            msg = format_nightwatch_report(report)
            await _send(msg)

            # ── BRAIN 자본 배분 자동 실행 ──
            try:
                await self._run_brain_after_nightwatch(_send)
            except Exception as e_brain:
                logger.error(f"BRAIN 실행 실패: {e_brain}")
                await _send(f"BRAIN 배분 실패: {e_brain}")

            # NXT 포지션 저장
            self._nightwatch_report = report

            min_score = nw_cfg.get("min_score", 3)
            alert_only = nw_cfg.get("alert_only", True)

            if report.total_score < min_score:
                await _send(f"NXT 진입 조건 미충족 (점수 {report.total_score:+.1f} < {min_score})")
                return

            # 진입 대상 종목 결정 - JARVIS 섹터 매핑 사용
            nxt_targets = getattr(report, 'nxt_targets', [])
            if not nxt_targets:
                # 폴백: config prefer_sectors (하위 호환)
                if report.total_score >= 5:
                    sector_key = "strong_buy"
                else:
                    sector_key = "buy"
                fallback_codes = nw_cfg.get("prefer_sectors", {}).get(sector_key, [])
                for c in fallback_codes:
                    from bot.kis_trader import CODE_TO_NAME
                    nxt_targets.append({"code": c, "name": CODE_TO_NAME.get(c, c),
                                        "sector": "config", "tier": 1, "priority": 1})

            if not nxt_targets:
                await _send("NXT 매수 대상 종목 없음 (JARVIS 섹터 매핑 결과 없음)")
                return

            # 예산 계산
            bal = self.trader.fetch_balance()
            if not bal.get("success"):
                await _send(f"NXT 잔고 조회 실패")
                return

            budget_pct = nw_cfg.get("nxt_budget_pct", 30) / 100.0
            nxt_budget = int(bal["cash"] * budget_pct)
            max_pos = nw_cfg.get("max_nxt_positions", 1)
            per_stock = nxt_budget // max_pos

            # Tier1 1순위 섹터만 필터 (NXT 매수 대상)
            buy_targets = [t for t in nxt_targets if t.get("tier", 1) == 1][:max_pos]

            # 알림만 모드
            if alert_only:
                reason = getattr(report, 'selection_reason', '')
                code_names = []
                for t in nxt_targets[:8]:  # 최대 8종목 표시
                    tier_mark = "★" if t.get("tier") == 1 else "☆"
                    code_names.append(f"  {tier_mark} {t['name']}({t['code']}) [{t.get('sector', '')}]")

                buy_names = [f"  → {t['name']}({t['code']})" for t in buy_targets]

                await _send(
                    f"{report.signal} NXT 매수 신호!\n"
                    f"점수: {report.total_score:+.1f}\n"
                    f"판단: {reason}\n"
                    f"예산: {nxt_budget:,}원 (현금의 {int(budget_pct*100)}%)\n"
                    f"종목당: {per_stock:,}원\n\n"
                    f"JARVIS 추천:\n" + "\n".join(code_names) + "\n\n"
                    f"매수 실행 대상 (Tier1 {max_pos}종목):\n" + "\n".join(buy_names) + "\n\n"
                    f"[알림만 모드] 수동 매수 필요\n"
                    f"자동매매 전환: NXT켜기"
                )
                return

            # 자동매매 모드 - NXT 매수 실행
            for t in buy_targets:
                code = t["code"]
                name = t["name"]

                result = self.trader.nxt_safe_buy(code, per_stock)
                if result.get("success"):
                    # NXT 포지션 기록
                    pi = self.trader.fetch_price(code)
                    entry_price = pi.get("current_price", 0) if pi and pi.get("success") else 0
                    self._nxt_positions[code] = {
                        "name": name,
                        "entry_price": entry_price,
                        "entry_date": datetime.now().strftime("%Y-%m-%d %H:%M"),
                        "nw_score": report.total_score,
                        "nw_signal": report.signal,
                    }
                    self._save_nxt_positions()
                    await _send(f"NXT 매수 완료: {result['message']}")
                else:
                    await _send(f"NXT 매수 실패: {name}({code}) - {result['message']}")

        except Exception as e:
            logger.error(f"NIGHTWATCH 판단 실패: {e}")
            await _send(f"NIGHTWATCH 판단 실패: {e}")

    async def job_nxt_morning_sell(self, context):
        """08:00 - NXT 포지션 매도 (장전 시간외)"""
        if not is_trading_day():
            return

        nw_cfg = self.config.get("nightwatch", {})
        if not nw_cfg.get("enabled", False):
            return

        self._load_nxt_positions()

        if not self._nxt_positions:
            return

        chat_id = None
        if not self._send_alert:
            import os
            chat_id = os.getenv("TELEGRAM_CHAT_ID")

        async def _send(text):
            if self._send_alert:
                await self._send_alert(text)
            elif chat_id:
                await context.bot.send_message(chat_id=chat_id, text=text)

        await _send(f"NXT 아침 매도 시작 ({len(self._nxt_positions)}종목)")

        for code, pos in list(self._nxt_positions.items()):
            name = pos.get("name", code)
            try:
                # 현재 잔고에서 보유 수량 확인
                bal = self.trader.fetch_balance()
                if not bal.get("success"):
                    await _send(f"NXT 매도 잔고 조회 실패: {name}")
                    continue

                held_qty = 0
                for p in bal.get("positions", []):
                    if p["code"] == code:
                        held_qty = p["qty"]
                        break

                if held_qty <= 0:
                    await _send(f"NXT {name}: 보유 없음 (이미 매도?)")
                    self._nxt_positions.pop(code, None)
                    continue

                # 수익률 계산
                pi = self.trader.fetch_price(code)
                curr_price = pi.get("current_price", 0) if pi and pi.get("success") else 0
                entry = pos.get("entry_price", 0)
                pnl_pct = ((curr_price - entry) / entry * 100) if entry > 0 else 0

                # 장전 시간외 매도
                alert_only = nw_cfg.get("alert_only", True)
                if alert_only:
                    await _send(
                        f"NXT 매도 알림: {name}({code})\n"
                        f"  진입: {entry:,}원 → 현재: {curr_price:,}원\n"
                        f"  수익: {pnl_pct:+.1f}% ({held_qty}주)\n"
                        f"  [알림만 모드] 수동 매도 필요"
                    )
                else:
                    result = self.trader.afterhours_sell(code, held_qty)
                    if result.get("success"):
                        await _send(
                            f"NXT 매도 완료: {name}({code})\n"
                            f"  수익: {pnl_pct:+.1f}% | {result['message']}"
                        )
                    else:
                        await _send(f"NXT 매도 실패: {name} - {result['message']}")

                self._nxt_positions.pop(code, None)

            except Exception as e:
                logger.error(f"NXT 매도 실패 {code}: {e}")
                await _send(f"NXT 매도 예외: {name} - {e}")

        self._save_nxt_positions()

    def _save_positions(self):
        """정규 포지션 JSON 영속화 (VPS 재시작 대비, target_state 직렬화 포함)"""
        from dataclasses import asdict
        tmp = POSITIONS_PATH.with_suffix(".tmp")
        POSITIONS_PATH.parent.mkdir(parents=True, exist_ok=True)
        # target_state (dataclass)를 dict로 변환하여 직렬화
        serializable = {}
        for code, pos in self._positions.items():
            p = dict(pos)
            ts = p.get("target_state")
            if ts is not None and hasattr(ts, "__dataclass_fields__"):
                p["target_state"] = asdict(ts)
            serializable[code] = p
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(serializable, f, ensure_ascii=False, indent=2)
        tmp.replace(POSITIONS_PATH)

    def _load_positions(self):
        """정규 포지션 JSON 복원 (target_state dict→TargetState 변환 포함)"""
        if POSITIONS_PATH.exists():
            try:
                with open(POSITIONS_PATH, "r", encoding="utf-8") as f:
                    self._positions = json.load(f)
                if self._positions:
                    # target_state가 dict로 저장되어 있으면 TargetState로 복원
                    from strategies.dynamic_target import TargetState
                    for code, pos in self._positions.items():
                        ts = pos.get("target_state")
                        if isinstance(ts, dict):
                            try:
                                pos["target_state"] = TargetState(**{
                                    k: v for k, v in ts.items()
                                    if k in TargetState.__dataclass_fields__
                                })
                            except Exception:
                                pos["target_state"] = None
                    logger.info(f"[POSITIONS] 복원: {len(self._positions)}종목")
            except (json.JSONDecodeError, IOError):
                self._positions = {}

    def _restore_positions_to_rt_monitor(self):
        """positions.json에서 복원된 종목을 RealtimeMonitor에도 등록.

        재시작 후 RealtimeMonitor가 비어있으면 evaluate_all()이 빈 결과를 반환하여
        SL/TP 자동 청산이 작동하지 않는다(_job_monitor_fallback 예외 경로에서만 SL 체크).
        이 메서드는 __init__에서 _load_positions() 직후 호출되어 정합성을 보장한다.

        source 필드를 함께 전달하여 _decide()의 manual_sync 가드 분기를 활성화.
        """
        if not self._positions:
            return
        try:
            rtm = self._get_rt_monitor()
            registered = 0
            for code, pos in self._positions.items():
                try:
                    rtm.register_position(
                        code=code,
                        name=pos.get("name", code),
                        entry_price=int(pos.get("entry_price", 0)),
                        sl=int(pos.get("stop_loss", 0)),
                        tp=int(pos.get("take_profit", 0)),
                        source=pos.get("source", ""),
                    )
                    registered += 1
                except Exception as e:
                    logger.warning(f"AI 모니터 복원 등록 실패 {code}: {e}")
            logger.info(f"[POSITIONS] AI 모니터 복원 등록: {registered}/{len(self._positions)}종목")
        except Exception as e:
            logger.warning(f"AI 모니터 복원 단계 실패: {e}")

    def _save_nxt_positions(self):
        """NXT 포지션 JSON 저장"""
        path = Path(__file__).resolve().parent.parent / "data_store" / "nxt_positions.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(self._nxt_positions, f, ensure_ascii=False, indent=2)
        tmp.replace(path)

    def _load_nxt_positions(self):
        """NXT 포지션 JSON 로드"""
        path = Path(__file__).resolve().parent.parent / "data_store" / "nxt_positions.json"
        if path.exists():
            try:
                with open(path, "r", encoding="utf-8") as f:
                    self._nxt_positions = json.load(f)
            except (json.JSONDecodeError, IOError):
                self._nxt_positions = {}
        else:
            self._nxt_positions = {}

    # ═══════════════════════════════════════
    #  시간외 선취매 (Pre-Dawn Buy)
    #  "추천 TOP picks → 시간외 종가매매 → 다음날 갭업 수익"
    # ═══════════════════════════════════════

    async def job_predawn_buy(self, context):
        """16:52 — 시간외 선취매 (추천 TOP → 시간외 종가매매)

        morning_recommendation (16:45) 완료 후 실행.
        recommendation.json에서 AAA/AA + FORCE_BUY + 100점+ 필터 →
        시간외 종가매매(18:00까지)로 매수.

        NXT와의 차이:
        - NXT: 매크로 시그널 기반, 다음날 08:00 자동 매도 (overnight flip)
        - 선취매: 추천 엔진 기반, 정규 포지션으로 전환 (Eye+Guardian 관리)
        """
        if not is_trading_day():
            return

        nw_cfg = self.config.get("nightwatch", {})
        if not nw_cfg.get("enabled", False):
            return

        chat_id = None
        if not self._send_alert:
            import os
            chat_id = os.getenv("TELEGRAM_CHAT_ID")

        async def _send(text):
            if self._send_alert:
                await self._send_alert(text)
            elif chat_id:
                await context.bot.send_message(chat_id=chat_id, text=text)

        try:
            from data.nightwatch import (
                select_predawn_targets, save_predawn_positions,
                format_predawn_alert,
            )

            min_score = nw_cfg.get("predawn_min_score", 100)
            max_targets = nw_cfg.get("predawn_max_targets", 3)
            targets = await asyncio.to_thread(
                select_predawn_targets, min_score, max_targets
            )

            if not targets:
                logger.info("[PREDAWN] 선취매 대상 없음")
                return

            # 예산 계산
            bal = self.trader.fetch_balance()
            if not bal.get("success"):
                await _send("선취매: 잔고 조회 실패")
                return

            budget_pct = nw_cfg.get("predawn_budget_pct", 40) / 100.0

            # ── 이벤트 리스크 체크 → HIGH/EXTREME이면 예산 축소 ──
            event_warning = ""
            try:
                from data.event_calendar import get_event_risk_for_recommendation
                # 내일 이벤트 체크 (선취매는 내일 장에서 보유)
                tomorrow = next_trading_day()
                ev_risk = get_event_risk_for_recommendation(tomorrow)
                risk_level = ev_risk.get("risk_level", "LOW")

                if self._cfo:
                    _mult = self._cfo.get_event_multiplier(risk_level, "predawn")
                else:
                    _mult = {"EXTREME": 0.3, "HIGH": 0.5}.get(risk_level, 1.0)
                if _mult < 1.0:
                    budget_pct *= _mult
                    event_warning = f"{risk_level} 이벤트 → 예산 x{_mult:.1f}"

                if event_warning:
                    ev_names = [e["name"] for e in ev_risk.get("events", []) if e.get("impact") == "HIGH"][:3]
                    event_warning += f" ({', '.join(ev_names)})"
                    logger.info(f"[PREDAWN] {event_warning}")
            except Exception as e_ev:
                logger.debug(f"[PREDAWN] 이벤트 리스크 체크 실패 (무시): {e_ev}")

            predawn_budget = int(bal["cash"] * budget_pct)
            per_stock = predawn_budget // len(targets)

            alert_only = nw_cfg.get("alert_only", True)

            if alert_only:
                # ── 알림만 모드 ──
                msg = format_predawn_alert(targets, predawn_budget, per_stock)
                if event_warning:
                    msg += f"\n\n⚠️ {event_warning}"
                await _send(msg)
                await _send(
                    "[알림만 모드] 수동으로 시간외 종가매매 필요\n"
                    "자동매매 전환: NXT켜기"
                )
                return

            # ── 자동매매 모드 — 시간외 종가매수 실행 ──
            bought = {}
            for t in targets:
                code = t["code"]
                name = t["name"]
                close_price = t["close"]

                if close_price <= 0:
                    await _send(f"선취매: {name} 종가 0원 — 스킵")
                    continue

                qty = per_stock // close_price
                # 5/19~5/20 D-Day 1주 모드 — qty 강제=1 (사장님 5/19 보수 검증)
                try:
                    if self.trader._is_one_share_mode():
                        qty = 1
                        logger.info(f"[1주모드/선취매] {name}({code}) qty 강제=1")
                except Exception:
                    pass
                if qty <= 0:
                    await _send(f"선취매: {name} 수량 부족 (예산 {per_stock:,}원 / @{close_price:,}원)")
                    continue

                result = self.trader.afterhours_buy(code, qty, close_price)
                if result.get("success"):
                    bought[code] = {
                        "name": name,
                        "entry_price": close_price,
                        "qty": qty,
                        "sl": t["sl"],
                        "tp": t["tp"],
                        "score": t["total_score"],
                        "grade": t["grade"],
                        "sources": t.get("sources", []),
                        "entry_date": datetime.now().strftime("%Y-%m-%d %H:%M"),
                        "type": "predawn",
                    }
                    await _send(
                        f"선취매 매수 완료: {name}({code})\n"
                        f"  {qty}주 @ {close_price:,}원 (시간외 종가)\n"
                        f"  SL: {t['sl']:,}원 | TP: {t['tp']:,}원\n"
                        f"  점수: {t['total_score']:.0f} ({t['grade']})"
                    )
                else:
                    await _send(f"선취매 매수 실패: {name} — {result.get('message', '')}")

            if bought:
                self._predawn_positions = bought
                save_predawn_positions(bought)
                logger.info(f"[PREDAWN] {len(bought)}종목 선취매 완료")

        except Exception as e:
            logger.error(f"[PREDAWN] 선취매 실패: {e}")
            await _send(f"선취매 실패: {e}")

    def merge_predawn_on_open(self):
        """장 시작 시 선취매 포지션 → 정규 포지션으로 전환

        08:55에 호출 (morning_recommendation 실행 전).
        predawn_positions.json → self._positions에 주입.
        이후 regular auto_trader가 Eye + Guardian으로 관리.
        """
        from data.nightwatch import load_predawn_positions, clear_predawn_positions

        predawn = load_predawn_positions()
        if not predawn:
            return

        merged = 0
        for code, pos in predawn.items():
            if code in self._positions:
                logger.info(f"[PREDAWN] {pos['name']} 이미 정규 포지션 — 스킵")
                continue

            ep = pos.get("entry_price", 0)
            self._positions[code] = {
                "entry_price": ep,
                "stop_loss": pos.get("sl", 0),
                "take_profit": pos.get("tp", 0),
                "target_state": None,
                "entry_date": pos.get("entry_date", ""),
                "source": "predawn",
                "score": pos.get("score", 0),
                "name": pos.get("name", code),
                "regime": "NORMAL",
                "high_watermark": ep,
                "trailing_activated": False,
                "trailing_sl": 0,
                "partial_sold": False,
                "qty": pos.get("qty", 0),
            }
            merged += 1
            logger.info(
                f"[PREDAWN] 정규 포지션 전환: {pos['name']}({code}) "
                f"@{pos['entry_price']:,}원 SL={pos.get('sl', 0):,} TP={pos.get('tp', 0):,}"
            )

        if merged > 0:
            self._save_positions()
            clear_predawn_positions()
            logger.info(f"[PREDAWN] {merged}종목 정규 포지션 전환 완료")

    # ═══════════════════════════════════════
    #  내부 로직
    # ═══════════════════════════════════════

    def _load_swing_candidates(self):
        """swing_candidates.json 로드"""
        if not CANDIDATES_PATH.exists():
            return []
        try:
            with open(CANDIDATES_PATH, "r", encoding="utf-8") as f:
                data = json.load(f)
            return data.get("candidates", [])
        except (json.JSONDecodeError, IOError):
            return []

    def _init_dynamic_target(self, code, name, entry_price):
        """동적 목표가 초기 설정"""
        try:
            from strategies.dynamic_target import DynamicTargetEngine
            engine = DynamicTargetEngine()
            return engine.initial_setup(
                code, name, entry_price,
                datetime.now().strftime("%Y-%m-%d")
            )
        except Exception as e:
            logger.error(f"동적 목표가 초기화 실패 {code}: {e}")
            return None

    def _get_news_score(self, code, name=""):
        """뉴스 감성 점수 조회"""
        try:
            from data.news_collector import NewsCollector
            nc = NewsCollector()
            result = nc.get_news_score(code, name)
            return result.get("score", 0.0)
        except Exception:
            return 0.0

    def _scan_and_filter(self):
        """5D 스캔 → STRONG_BUY/BUY + EXPLOSIVE/HUNTABLE 필터"""
        from data.supply_analyzer import SupplyAnalyzer
        from data.kis_collector import UNIVERSE

        analyzer = SupplyAnalyzer()

        # ETF 제외 (Phase 3: ETF 추천 시스템 별도 관리 → 스캔에서만 제외)
        from data.etf_universe import get_all_etf_codes
        etf_codes = set(get_all_etf_codes())
        exclude = etf_codes | {"371160", "018880", "011210"}
        codes = [c for c in UNIVERSE.keys() if c not in exclude]

        fulls = analyzer.scan_all_full(codes)

        # 필터: BUY 이상 + 에너지 HUNTABLE 이상
        candidates = [
            f for f in fulls
            if f.action in ("STRONG_BUY", "BUY")
            and f.stability_grade in ("EXPLOSIVE", "HUNTABLE")
        ]

        logger.info(f"스캔 결과: {len(fulls)}종목 중 {len(candidates)}개 후보")
        return candidates
