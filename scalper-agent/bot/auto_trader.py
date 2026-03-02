# -*- coding: utf-8 -*-
"""
Body Hunter v4 자동매매 루프 — 동적 목표가 통합
================================================
3 GAP 해결:
  GAP 1: ATR 기반 SL/TP (고정% → 동적)
  GAP 2: 뉴스 감성 → 목표가 보정
  GAP 3: 매집원가 → SL 하한선

모드 2개:
  day  — 당일 매매 (15:10 전량 청산) ← 기존
  swing — 스윙 매매 (동적 목표가 재평가, 최대 N일 보유)

JobQueue (python-telegram-bot)로 스케줄:
  09:20 → 스캔 + 자동 매수
  매 30초 → 포지션 SL/TP 감시
  15:00 → 스윙 모드: 동적 목표가 재평가 + 판정
  15:10 → 데이 모드: 전량 청산 | 스윙 모드: 요약만
"""

import asyncio
import json
import logging
from datetime import datetime, date
from pathlib import Path
from typing import Callable, Optional

logger = logging.getLogger("BH.AutoTrader")

BASE_DIR = Path(__file__).resolve().parent.parent
CANDIDATES_PATH = BASE_DIR / "data_store" / "swing_candidates.json"
RISK_STATE_PATH = BASE_DIR / "data_store" / "risk_state.json"


class AutoTrader:
    """Body Hunter v4 자동매매 — 사전감지 + AI 모니터 통합"""

    def __init__(self, config: dict, trader):
        self.config = config
        self.trader = trader
        self.is_running = False
        self._send_alert: Optional[Callable] = None
        self._positions = {}  # {code: {entry_price, stop_loss, take_profit, target_state, ...}}

        # 돌파 대기 워치리스트: {code: {name, resistance, avg_vol, sl, tp, ...}}
        self._breakout_watch = {}

        # 모드: "day" or "swing"
        self.mode = config.get("bot", {}).get("trade_mode", "swing")

        # AI 실시간 모니터
        self._rt_monitor = None

        # ── 리스크 게이트 (일일손실한도 + MDD) ──
        risk = config.get("risk", {})
        self._daily_loss_limit = risk.get("daily_loss_limit", 500000)
        self._mdd_limit_pct = risk.get("mdd_limit_pct", 4.5)
        self._risk_state = self._load_risk_state()
        self._risk_blocked = False  # True면 신규 매수 차단

        # ── 자동매수 확인 대기열 ──
        self._confirm_auto = config.get("bot", {}).get("confirm_real_order", True)
        self._pending_auto_buys = []  # [{code, name, amount, sl, tp, tp1_quick, score}]

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
        """리스크 상태 저장"""
        try:
            RISK_STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
            with open(RISK_STATE_PATH, "w", encoding="utf-8") as f:
                json.dump(self._risk_state, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"리스크 상태 저장 실패: {e}")

    def record_realized_loss(self, loss_amount: int):
        """실현 손실 기록 (매도 시 호출)"""
        if loss_amount >= 0:
            return  # 이익이면 무시
        self._risk_state["daily_realized_loss"] += abs(loss_amount)
        self._save_risk_state()
        logger.info(f"일일 실현 손실 누적: {self._risk_state['daily_realized_loss']:,}원")

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
        """대기 중인 자동매수 전부 실행 → 결과 리스트 반환"""
        results = []
        for item in self._pending_auto_buys:
            code, name = item["code"], item["name"]
            amount = item["amount"]

            result = self.trader.safe_buy(code, amount)
            if result.get("success"):
                price_info = self.trader.fetch_price(code)
                cp = price_info.get("current_price", item.get("sl", 0))
                target_state = self._init_dynamic_target(code, name, cp)
                sl = target_state.dynamic_sl if target_state else item["sl"]
                tp = target_state.dynamic_tp if target_state else item["tp"]

                self._positions[code] = {
                    "entry_price": cp,
                    "stop_loss": sl,
                    "take_profit": tp,
                    "entry_date": datetime.now().strftime("%Y-%m-%d"),
                    "name": name,
                    "target_state": target_state,
                }
                try:
                    rtm = self._get_rt_monitor()
                    rtm.register_position(code, name, cp, sl, tp)
                except Exception as e:
                    logger.warning(f"AI 모니터 등록 실패 {code}: {e}")

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
        if now.weekday() >= 5:
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
        from datetime import date
        if date.today().weekday() >= 5:
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
        else:
            await self._morning_day(context, _send)

    async def _morning_swing(self, context, _send):
        """스윙 모드 아침 스캔: 사전감지 우선 → 7팩터 폴백"""

        # 0) 위기 모드 체크 (최우선)
        from data.market_health import is_crisis_mode
        crisis_active, crisis_reason = is_crisis_mode()
        if crisis_active:
            await _send(f"🚨 위기 모드 — 스캔 중단\n{crisis_reason}")
            return

        candidates = []

        # 1) 사전감지 스캐너 우선
        try:
            await _send("사전감지 스캔 실행 중...")
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
                await _send(f"사전감지: {len(candidates)}개 후보 발견")
        except Exception as e:
            logger.error(f"사전감지 실패: {e}")
            await _send(f"사전감지 실패 — 7팩터 폴백: {e}")

        # 2) 사전감지 결과 없으면 기존 swing_candidates 폴백
        if not candidates:
            candidates = self._load_swing_candidates()

        # 3) 그래도 없으면 swing_picker 실행
        if not candidates:
            try:
                await _send("스윙 후보 없음 — 7팩터 스캔 실행 중...")
                candidates = await asyncio.to_thread(self._run_swing_picker)
            except Exception as e:
                logger.error(f"스윙 피커 실패: {e}")
                await _send(f"스윙 피커 실패: {e}")
                return

        if not candidates:
            await _send("스캔 결과: 매수 후보 없음")
            return

        # 리포트 전송
        src_label = candidates[0].get("source", "swing")
        lines = [f"매수 후보 ({src_label})"]
        for c in candidates:
            lines.append(
                f"  {c['name']}({c['code']}) 점수:{c['total_score']:.0f} "
                f"진입:{c['entry']:,} SL:{c['sl']:,} TP:{c['tp']:,}"
            )

        if not self.is_running:
            lines.append("\n⏸ 자동매매 OFF — 리포트만 전송")
        await _send("\n".join(lines))

        if not self.is_running:
            return

        # ── 리스크 게이트 체크 (일일손실한도 + MDD) ──
        risk_ok, risk_reason = self.check_risk_gate()
        if not risk_ok:
            await _send(f"⛔ 리스크 게이트 차단 — 신규 매수 불가\n{risk_reason}")
            return

        # 자동 매수 실행
        bot_conf = self.config.get("bot", {})
        max_pos = bot_conf.get("max_auto_positions", 5)
        buy_amount = bot_conf.get("auto_buy_amount", 500000)

        bal = self.trader.fetch_balance()
        current_positions = len(bal.get("positions", [])) if bal.get("success") else 0
        slots = max_pos - current_positions

        if slots <= 0:
            await _send(f"보유 종목 {current_positions}개 — 추가 매수 불가")
            return

        bought = 0
        skipped = 0
        watching = 0
        for c in candidates[:slots]:
            code = c["code"]
            if code in self._positions:
                continue

            # ── 진입 필터: 차트 기반 최종 확인 ──
            try:
                from data.swing_indicators import check_entry_filter
                entry_check = await asyncio.to_thread(
                    check_entry_filter, code, c["name"]
                )

                if not entry_check["pass"]:
                    skipped += 1
                    await _send(
                        f"⛔ 진입 거부: {c['name']}({code})\n"
                        f"   {entry_check['reason']}"
                    )
                    continue

                # 절반 매수 (size_mult=0.5)
                actual_amount = int(buy_amount * entry_check["size_mult"])
                if entry_check["size_mult"] < 1.0:
                    await _send(
                        f"⚠️ {c['name']}: 절반 매수 ({actual_amount:,}원)\n"
                        f"   {entry_check['reason']}"
                    )
            except Exception as e:
                logger.warning(f"진입필터 오류 {code}: {e} — 기본 매수")
                actual_amount = buy_amount
                entry_check = {"pass": True, "size_mult": 1.0}

            # ── 저항대 감지: 고점 근접 시 돌파 대기 ──
            try:
                resistance = await asyncio.to_thread(
                    self._detect_resistance, code
                )
                if resistance:
                    res_price = resistance["resistance"]
                    res_dist = resistance["distance_pct"]

                    # 현재가가 저항대의 3% 이내 → 돌파 대기 모드
                    if res_dist <= 3.0:
                        watching += 1
                        self._breakout_watch[code] = {
                            "name": c["name"],
                            "resistance": res_price,
                            "avg_volume": resistance["avg_volume"],
                            "buy_amount": actual_amount,
                            "sl": c["sl"],
                            "tp": c["tp"],
                            "tp1_quick": c.get("tp1_quick", c["tp"]),
                            "registered_at": datetime.now().strftime("%H:%M"),
                            "premove_score": c["total_score"],
                            "checks": 0,        # 모니터링 횟수
                            "max_checks": 720,   # 최대 6시간 (30초 * 720)
                        }
                        await _send(
                            f"👁 돌파 대기: {c['name']}({code})\n"
                            f"   저항: {res_price:,}원 (현재가 대비 {res_dist:+.1f}%)\n"
                            f"   조건: 종가 {res_price:,}원 돌파 + 거래량 1.5배\n"
                            f"   30초마다 KIS API로 감시 중..."
                        )
                        continue  # 즉시 매수 안 하고 돌파 대기
            except Exception as e:
                logger.warning(f"저항대 감지 오류 {code}: {e} — 즉시 매수")

            # ── 매수 실행 (저항대 없거나 멀리 떨어진 경우) ──
            if self._confirm_auto:
                # 확인 모드: 대기열에 추가 → 텔레그램 확인 후 매수
                self._pending_auto_buys.append({
                    "code": code, "name": c["name"],
                    "amount": actual_amount, "sl": c["sl"],
                    "tp": c["tp"], "tp1_quick": c.get("tp1_quick", c["tp"]),
                    "score": c["total_score"],
                })
                price_info = self.trader.fetch_price(code)
                cp = price_info.get("current_price", c["entry"])
                await _send(
                    f"⚠️ 자동매수 확인 대기\n"
                    f"종목: {c['name']}({code})\n"
                    f"금액: {actual_amount:,}원 | 현재가: {cp:,}원\n"
                    f"SL: {c['sl']:,} → TP: {c['tp']:,}\n"
                    f"점수: {c['total_score']:.0f}\n\n"
                    f"실행: '자동확인' 입력 | 취소: '자동취소'"
                )
                bought += 1  # pending count
            else:
                # 즉시 매수 (확인 없이)
                result = self.trader.safe_buy(code, actual_amount)
                if result.get("success"):
                    bought += 1
                    price_info = self.trader.fetch_price(code)
                    cp = price_info.get("current_price", c["entry"])
                    target_state = self._init_dynamic_target(code, c["name"], cp)
                    sl = target_state.dynamic_sl if target_state else c["sl"]
                    tp = target_state.dynamic_tp if target_state else c["tp"]
                    self._positions[code] = {
                        "entry_price": cp,
                        "stop_loss": sl,
                        "take_profit": tp,
                        "entry_date": datetime.now().strftime("%Y-%m-%d"),
                        "name": c["name"],
                        "target_state": target_state,
                    }
                    try:
                        rtm = self._get_rt_monitor()
                        rtm.register_position(code, c["name"], cp, sl, tp)
                    except Exception as e:
                        logger.warning(f"AI 모니터 등록 실패 {code}: {e}")
                    await _send(
                        f"스윙 매수: {result.get('message')}\n"
                        f"   SL:{sl:,} TP:{tp:,} (동적)"
                    )
                else:
                    await _send(f"❌ 매수 실패 {code}: {result.get('message')}")

        label = "확인대기" if self._confirm_auto else "매수"
        summary = f"아침 스캔 완료: {bought}{label}"
        if watching:
            summary += f" / {watching}돌파대기"
        if skipped:
            summary += f" / {skipped}거부(차트필터)"
        if self._confirm_auto and bought:
            summary += "\n📱 '자동확인' 입력으로 매수 실행"
        await _send(summary)

    # ═══════════════════════════════════════
    #  저항대 감지 + 돌파 대기 매수
    # ═══════════════════════════════════════

    def _detect_resistance(self, code: str) -> dict | None:
        """최근 N일 고점 기반 저항대 감지

        Returns: {resistance, distance_pct, avg_volume} or None (저항 없음)
        """
        from pykrx import stock as pykrx_stock
        from datetime import timedelta

        try:
            end = datetime.now().strftime("%Y%m%d")
            start = (datetime.now() - timedelta(days=60)).strftime("%Y%m%d")
            df = pykrx_stock.get_market_ohlcv(start, end, code)

            if df is None or len(df) < 10:
                return None

            close = df["종가"].astype(float)
            high = df["고가"].astype(float)
            volume = df["거래량"].astype(float)
            current = float(close.iloc[-1])

            # 최근 20일 고점 (오늘 제외)
            recent_high = float(high.iloc[-21:-1].max()) if len(high) > 21 else float(high.iloc[:-1].max())

            # 평균 거래량 (20일)
            avg_vol = float(volume.iloc[-20:].mean())

            # 저항대까지 거리 (%)
            dist_pct = (recent_high / current - 1) * 100

            # 고점이 현재가 위에 있고, 5% 이내면 저항대
            if 0 < dist_pct <= 5.0:
                return {
                    "resistance": int(recent_high),
                    "distance_pct": round(dist_pct, 1),
                    "avg_volume": int(avg_vol),
                }

            # 현재가가 고점 부근(위아래 3% 이내)이면 돌파 시도 중
            if abs(dist_pct) <= 3.0:
                return {
                    "resistance": int(recent_high),
                    "distance_pct": round(dist_pct, 1),
                    "avg_volume": int(avg_vol),
                }

            return None  # 저항대 없음 (멀리 떨어짐)

        except Exception as e:
            logger.warning(f"저항대 감지 실패 {code}: {e}")
            return None

    async def _check_breakout_watch(self):
        """돌파 대기 워치리스트 모니터링 (30초마다 job_monitor에서 호출)

        돌파 조건:
          1. 현재가 > 저항대 (종가 기준 돌파)
          2. 당일 거래량 > 평균 거래량 * 1.3 (거래량 동반)
        """
        if not self._breakout_watch:
            return

        expired = []
        for code, watch in list(self._breakout_watch.items()):
            watch["checks"] += 1

            # 최대 감시 시간 초과 → 만료
            if watch["checks"] > watch["max_checks"]:
                expired.append(code)
                await self._alert(
                    f"⏰ 돌파 대기 만료: {watch['name']}({code})\n"
                    f"   {watch['resistance']:,}원 돌파 실패 — 오늘 매수 안 함"
                )
                continue

            # 14:30 이후면 더 이상 안 삼 (장마감 가까움)
            now = datetime.now()
            if now.hour >= 14 and now.minute >= 30:
                expired.append(code)
                await self._alert(
                    f"⏰ 돌파 대기 종료: {watch['name']}({code})\n"
                    f"   14:30 이후 — 오늘 매수 안 함"
                )
                continue

            # KIS API로 현재가 조회
            try:
                price_info = self.trader.fetch_price(code)
                if not price_info.get("success"):
                    continue

                cp = price_info["current_price"]
                today_vol = price_info["volume"]
                today_high = price_info["high"]
                resistance = watch["resistance"]
                avg_vol = watch["avg_volume"]

                # 돌파 조건 체크
                vol_ratio = today_vol / avg_vol if avg_vol > 0 else 0
                broke_resistance = cp > resistance
                volume_confirm = vol_ratio >= 1.3

                # 10분마다 상태 로그 (매 20회차 = 30초 * 20 = 10분)
                if watch["checks"] % 20 == 0:
                    logger.info(
                        f"돌파감시 {watch['name']}: "
                        f"현재{cp:,} vs 저항{resistance:,} | "
                        f"거래량 {vol_ratio:.1f}x | "
                        f"돌파{'O' if broke_resistance else 'X'} "
                        f"거래량{'O' if volume_confirm else 'X'}"
                    )

                # ── 돌파 확인! → AI EYE 검증 후 매수 ──
                if broke_resistance and volume_confirm:

                    # 👁 AI 눈(EYE): 4팩터 실시간 점수 확인
                    ai_score = -1
                    try:
                        rtm = self._get_rt_monitor()
                        rtm.register_position(
                            code, watch["name"], cp, watch["sl"], watch["tp"]
                        )
                        snap = await asyncio.to_thread(rtm.evaluate_position, code)
                        if snap:
                            ai_score = snap.realtime_score
                        rtm.unregister_position(code)
                    except Exception as e:
                        logger.warning(f"AI EYE 실패 {code}: {e}")

                    # AI 점수 40 미만 → 허위 돌파 가능성 → 매수 보류
                    if 0 <= ai_score < 40:
                        await self._alert(
                            f"👁 AI EYE 거부: {watch['name']}({code})\n"
                            f"   가격 돌파 OK + 거래량 {vol_ratio:.1f}x OK\n"
                            f"   BUT AI 점수 {ai_score}/100 (체결강도/호가 약함)\n"
                            f"   → 허위 돌파 의심, 계속 감시 중"
                        )
                        continue  # 매수 안 하고 다음 체크에서 재시도

                    buy_amount = watch["buy_amount"]
                    ai_msg = f" | AI {ai_score}점" if ai_score >= 0 else ""

                    if self._confirm_auto:
                        # 확인 모드: 대기열에 추가
                        self._pending_auto_buys.append({
                            "code": code, "name": watch["name"],
                            "amount": buy_amount, "sl": watch["sl"],
                            "tp": watch["tp"],
                            "tp1_quick": watch.get("tp1_quick", watch["tp"]),
                            "score": watch.get("premove_score", 0),
                        })
                        await self._alert(
                            f"⚠️ 돌파 매수 확인 대기\n"
                            f"   {watch['name']}({code}) @ {cp:,}원\n"
                            f"   저항 {resistance:,}원 돌파 확인\n"
                            f"   거래량 {vol_ratio:.1f}x{ai_msg}\n"
                            f"   금액: {buy_amount:,}원\n\n"
                            f"   실행: '자동확인' | 취소: '자동취소'"
                        )
                    else:
                        result = self.trader.safe_buy(code, buy_amount)
                        if result.get("success"):
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
                                "target_state": target_state,
                            }
                            try:
                                rtm = self._get_rt_monitor()
                                rtm.register_position(code, watch["name"], cp, sl, tp)
                            except Exception:
                                pass
                            await self._alert(
                                f"🚀 돌파 매수 성공!\n"
                                f"   {watch['name']}({code}) @ {cp:,}원\n"
                                f"   저항 {resistance:,}원 돌파 확인\n"
                                f"   거래량 {vol_ratio:.1f}x{ai_msg}\n"
                                f"   SL:{sl:,} TP:{tp:,}"
                            )
                        else:
                            await self._alert(
                                f"❌ 돌파 매수 실패: {watch['name']}({code})\n"
                                f"   {result.get('message')}"
                            )

                    expired.append(code)

                # ── 저항대 아래로 크게 하락 (-3%) → 오늘 포기 ──
                elif cp < resistance * 0.97:
                    expired.append(code)
                    await self._alert(
                        f"📉 돌파 포기: {watch['name']}({code})\n"
                        f"   현재 {cp:,}원 — 저항대 대비 -3% 이탈"
                    )

            except Exception as e:
                logger.error(f"돌파 감시 오류 {code}: {e}")

        # 만료/완료 항목 제거
        for code in expired:
            self._breakout_watch.pop(code, None)

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
            lines.append("\n⏸ 자동매매 OFF — 리포트만 전송")
        await _send("\n".join(lines))

        if not self.is_running:
            return

        bot_conf = self.config.get("bot", {})
        max_pos = bot_conf.get("max_auto_positions", 3)
        buy_amount = bot_conf.get("auto_buy_amount", 500000)

        bal = self.trader.fetch_balance()
        current_positions = len(bal.get("positions", [])) if bal.get("success") else 0
        slots = max_pos - current_positions

        if slots <= 0:
            await _send(f"보유 종목 {current_positions}개 — 추가 매수 불가")
            return

        bought = 0
        risk_conf = self.config.get("risk", {})
        sl_pct = risk_conf.get("stop_loss_pct", 0.02)
        tp_pct = risk_conf.get("take_profit_pct", 0.05)

        for f in candidates[:slots]:
            code = f.score.code
            result = self.trader.safe_buy(code, buy_amount)

            if result.get("success"):
                bought += 1
                price_info = self.trader.fetch_price(code)
                cp = price_info.get("current_price", 0)
                if cp > 0:
                    self._positions[code] = {
                        "entry_price": cp,
                        "stop_loss": int(cp * (1 - sl_pct)),
                        "take_profit": int(cp * (1 + tp_pct)),
                    }
                await _send(f"✅ 자동 매수: {result.get('message')}")
            else:
                await _send(f"❌ 매수 실패 {code}: {result.get('message')}")

        await _send(f"아침 스캔 완료: {bought}/{len(candidates[:slots])} 매수")

    async def job_monitor(self, context):
        """포지션 감시 — AI 4팩터 실시간 분석 (JobQueue 반복 호출)"""
        if not self._is_market_hours():
            return

        # ── 돌파 대기 워치리스트 체크 (자동매매 ON/OFF 무관) ──
        if self._breakout_watch:
            await self._check_breakout_watch()

        if not self.is_running:
            return
        if not self._positions:
            return

        try:
            rtm = self._get_rt_monitor()

            # 데이터 피드 중단 감지
            if rtm._feed_suspended:
                await self._alert(
                    "⚠️ 데이터 피드 중단 감지!\n"
                    f"   {rtm._consecutive_failures}회 연속 API 실패\n"
                    "   신규 매매 중지, SL/TP 폴백으로 전환"
                )
                self._risk_blocked = True
                await self._job_monitor_fallback()
                return

            snapshots = await asyncio.to_thread(rtm.evaluate_all)
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

                if snap.decision == "FULL_SELL":
                    logger.info(f"AI 전량매도: {code} @ {snap.price:,} ({snap.decision_reason})")
                    # 실현 손익 기록
                    pnl_amount = snap.price - pos["entry_price"]
                    bal_info = self.trader.fetch_balance()
                    for p in bal_info.get("positions", []):
                        if p["code"] == code:
                            pnl_amount = p.get("pnl_amount", pnl_amount)
                            break
                    self.record_realized_loss(pnl_amount)

                    result = self.trader.liquidate_one(code)
                    self._positions.pop(code, None)
                    rtm.unregister_position(code)
                    await self._alert(rtm.format_decision_alert(snap))

                elif snap.decision == "PARTIAL_SELL":
                    logger.info(f"AI 부분매도: {code} @ {snap.price:,} ({snap.decision_reason})")
                    bal = self.trader.fetch_balance()
                    for p in bal.get("positions", []):
                        if p["code"] == code:
                            half = max(1, p["qty"] // 2)
                            self.trader.smart_sell(code, half)  # 스마트 매도
                            await self._alert(rtm.format_decision_alert(snap))
                            break

                # 10분마다 전체 리포트 (매 20회차)
                # (30초 * 20 = 10분)

            except Exception as e:
                logger.error(f"AI 모니터 처리 실패 {code}: {e}")

    async def _job_monitor_fallback(self):
        """AI 모니터 실패 시 폴백: 단순 SL/TP 체크"""
        for code, pos in list(self._positions.items()):
            try:
                price_info = self.trader.fetch_price(code)
                if not price_info.get("success"):
                    continue

                cp = price_info["current_price"]

                if cp <= pos["stop_loss"]:
                    # 실현 손실 기록
                    loss = cp - pos["entry_price"]
                    self.record_realized_loss(loss)

                    result = self.trader.liquidate_one(code)
                    self._positions.pop(code, None)
                    await self._alert(
                        f"손절\n{pos.get('name', code)}({code}) @ {cp:,}원\n"
                        f"진입: {pos['entry_price']:,} -> 현재: {cp:,} ({loss:+,})"
                    )

                elif self.mode == "day" and cp >= pos["take_profit"]:
                    result = self.trader.liquidate_one(code)
                    self._positions.pop(code, None)
                    gain = cp - pos["entry_price"]
                    await self._alert(
                        f"익절\n{pos.get('name', code)}({code}) @ {cp:,}원\n"
                        f"진입: {pos['entry_price']:,} -> 현재: {cp:,} (+{gain:,})"
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

        for code, pos in list(self._positions.items()):
            try:
                price_info = self.trader.fetch_price(code)
                if not price_info.get("success"):
                    continue

                cp = price_info["current_price"]
                pnl = (cp / pos["entry_price"] - 1) * 100

                # 보유일 계산
                entry_date = pos.get("entry_date", "")
                if entry_date:
                    hold_days = (datetime.now() - datetime.strptime(entry_date, "%Y-%m-%d")).days
                else:
                    hold_days = 0

                # 뉴스 감성
                news_score = self._get_news_score(code, pos.get("name", ""))

                # 동적 재평가
                target_state = pos.get("target_state")
                if target_state:
                    target_state = engine.daily_reeval(
                        target_state, cp, news_score=news_score
                    )
                    action = target_state.action
                    reason = target_state.reason
                    pos["target_state"] = target_state
                    pos["stop_loss"] = target_state.dynamic_sl
                    pos["take_profit"] = target_state.dynamic_tp
                else:
                    action = ACTION_HOLD
                    reason = "타겟 상태 없음"

                # 최대 보유일 초과
                if hold_days >= max_hold:
                    action = ACTION_FULL_SELL
                    reason = f"최대 보유일 {max_hold}일 도달"

                # 판정 실행
                name = pos.get("name", code)
                icon = {
                    ACTION_HOLD: "🟢", ACTION_ADD: "🔵",
                    ACTION_PARTIAL_SELL: "🟡", ACTION_FULL_SELL: "🔴",
                    ACTION_STOP_LOSS: "⛔",
                }.get(action, "⚪")

                lines.append(
                    f"  {icon} {name}({code}) {pnl:+.1f}% D{hold_days}\n"
                    f"     {action} — {reason}\n"
                    f"     SL:{pos['stop_loss']:,} TP:{pos['take_profit']:,}"
                )

                if action == ACTION_STOP_LOSS:
                    result = self.trader.liquidate_one(code)
                    self._positions.pop(code, None)
                    await self._alert(f"⛔ 동적 손절: {name}({code}) @ {cp:,}")
                elif action == ACTION_FULL_SELL:
                    result = self.trader.liquidate_one(code)
                    self._positions.pop(code, None)
                    await self._alert(f"🔴 동적 전량매도: {name}({code}) @ {cp:,} ({reason})")
                elif action == ACTION_PARTIAL_SELL:
                    # 부분매도: 보유수량의 50% (스마트 지정가)
                    bal = self.trader.fetch_balance()
                    for p in bal.get("positions", []):
                        if p["code"] == code:
                            half = max(1, p["qty"] // 2)
                            self.trader.smart_sell(code, half)
                            await self._alert(f"🟡 부분매도: {name}({code}) {half}주 @ {cp:,}")
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
            # 데이 모드: 전량 청산
            logger.info("장마감 전량 청산")
            await self._alert("🏁 장마감 전량 청산 시작...")
            result = self.trader.liquidate_all()
            self._positions.clear()
            await self._alert(f"{'✅' if result.get('success') else '❌'} {result.get('message')}")
        else:
            # 스윙 모드: 요약만
            logger.info("장마감 스윙 요약")

        # 일일 요약 (공통)
        bal = self.trader.fetch_balance()
        if bal.get("success"):
            pos_lines = []
            for p in bal.get("positions", []):
                pos_lines.append(f"  {p.get('name', p['code'])} {p['qty']}주 {p.get('pnl_pct', 0):+.1f}%")

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
        """Stage 1: 저녁 분석 (16:45) — 5단계 추천 파이프라인

        한국장 마감 + 데이터 수집 완료 후 실행
        릴레이 → 사전감지 → 기술필터 → 뉴스AI → 교차검증
        """
        from datetime import date
        if date.today().weekday() >= 5:
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

        await _send("🌙 저녁 분석 시작 — 5단계 추천 파이프라인...")

        try:
            from data.morning_recommendation import (
                run_evening_recommendation, format_recommendation,
                save_recommendation,
            )
            report = await asyncio.to_thread(run_evening_recommendation)
            save_recommendation(report)

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
                    f"   08:50 최종 확인 → 09:20 자동매수"
                )
        except Exception as e:
            logger.error(f"저녁 분석 실패: {e}")
            await _send(f"❌ 저녁 분석 실패: {e}")

    async def job_us_market_check(self, context):
        """Stage 2: 미국장 체크 (06:30) — 전일 저녁 추천 조정

        미국 S&P500/나스닥/VIX 체크 → 추천 조정/유지
        """
        from datetime import date
        if date.today().weekday() >= 5:
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
                await _send("🇺🇸 미국장 체크: 저녁 추천 없음 — 스킵")
                return

            report = await asyncio.to_thread(run_us_market_check, prev)
            save_recommendation(report)

            msg = format_recommendation(report)
            await _send(msg)

            if report.warning:
                await _send(f"⚠️ 주의: {report.warning}")
            else:
                await _send("✅ 미국장 정상 — 저녁 추천 유지")

        except Exception as e:
            logger.error(f"미국장 체크 실패: {e}")
            await _send(f"❌ 미국장 체크 실패: {e}")

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

    def _run_swing_picker(self):
        """swing_picker 직접 실행"""
        from data.swing_picker import run_picker
        result = run_picker()
        return result.get("candidates", [])

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

        # ETF 제외
        exclude = {"069500", "371160", "102780", "305720", "018880", "011210"}
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
