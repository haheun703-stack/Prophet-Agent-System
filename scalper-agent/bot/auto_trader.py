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
from datetime import datetime, date
from pathlib import Path
from typing import Callable, Optional

logger = logging.getLogger("BH.AutoTrader")

BASE_DIR = Path(__file__).resolve().parent.parent
CANDIDATES_PATH = BASE_DIR / "data_store" / "swing_candidates.json"
RISK_STATE_PATH = BASE_DIR / "data_store" / "risk_state.json"


class AutoTrader:
    """Body Hunter v4 자동매매 - 사전감지 + AI 모니터 통합"""

    def __init__(self, config: dict, trader):
        self.config = config
        self.trader = trader
        self.is_running = False
        self._send_alert: Optional[Callable] = None
        self._positions = {}  # {code: {entry_price, stop_loss, take_profit, target_state, ...}}

        # 진입 감시 대기열: 장 시작 후 실시간으로 관찰 → 조건 충족 시 매수
        # {code: {name, buy_amount, sl, tp, score, prev_close, checks, ...}}
        self._entry_watch = {}

        # 모드: "day" or "swing"
        self.mode = config.get("bot", {}).get("trade_mode", "swing")

        # AI 실시간 모니터
        self._rt_monitor = None

        # Intraday AI Eye (5분 주기 흐름 분석)
        self._eye = None
        self._eye_counter = 0  # 30초 카운터 (10 = 5분)

        # ── 리스크 게이트 (일일손실한도 + MDD) ──
        risk = config.get("risk", {})
        self._daily_loss_limit = risk.get("daily_loss_limit", 500000)
        self._mdd_limit_pct = risk.get("mdd_limit_pct", 4.5)
        self._risk_state = self._load_risk_state()
        self._risk_blocked = False  # True면 신규 매수 차단

        # ── 자동매수 확인 대기열 ──
        self._confirm_auto = config.get("bot", {}).get("confirm_real_order", True)
        self._pending_auto_buys = []  # [{code, name, amount, sl, tp, tp1_quick, score}]

        # ── NIGHTWATCH NXT ──
        self._nxt_positions = {}
        self._nightwatch_report = None

        # ── 시간외 선취매 (Pre-Dawn) ──
        self._predawn_positions = {}  # {code: {name, entry_price, sl, tp, score, ...}}

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
        """대기 중인 자동매수 전부 실행 → 결과 리스트 반환

        분할매수 시: split_done 카운터 여기서 올림 + 포지션 최초 1회만 생성
        """
        results = []
        for item in self._pending_auto_buys:
            code, name = item["code"], item["name"]
            amount = item["amount"]

            result = self.trader.safe_buy(code, amount)
            if result.get("success"):
                # 분할매수: entry_watch 상태 업데이트
                if item.get("_is_split") and code in self._entry_watch:
                    watch = self._entry_watch[code]
                    watch["split_done"] = watch.get("split_done", 0) + 1
                    watch["last_split_check"] = watch.get("checks", 0)
                    done = watch["split_done"]
                    total = watch.get("split_count", 3)
                    logger.info(f"분할매수 체결: {name} {done}/{total}차")
                    # 전부 완료 → 감시 제거
                    if done >= total:
                        self._entry_watch.pop(code, None)

                # 포지션 최초 1회만 생성 (분할매수 시 덮어쓰기 방지)
                if code not in self._positions:
                    price_info = self.trader.fetch_price(code)
                    cp = price_info.get("current_price", 0) if price_info and price_info.get("success") else 0
                    if cp <= 0:
                        # 매수 완료됐으나 가격 조회 실패 → SL 기반 포지션 등록
                        logger.error(f"가격 조회 실패 {code} — SL/TP 기반 포지션 등록")
                        cp = item.get("sl", 0) or item.get("tp", 0)
                        if cp <= 0:
                            cp = 10000  # 최후 fallback
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
                        "high_watermark": cp,
                        "trailing_activated": False,
                        "trailing_sl": 0,
                        "regime": item.get("regime", "NORMAL"),
                        "source": item.get("source", ""),
                    }
                    try:
                        rtm = self._get_rt_monitor()
                        rtm.register_position(code, name, cp, sl, tp)
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
            import json as _json_at
            from datetime import date as _date_at
            picks_path = Path(__file__).parent.parent / "data_store" / "tomorrow_picks.json"
            if picks_path.exists():
                with open(picks_path, "r", encoding="utf-8") as f:
                    picks_data = _json_at.load(f)
                pick_date = picks_data.get("date", "")
                today_str = _date_at.today().strftime("%Y-%m-%d")
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
                                to_data = _json_at.load(f)
                            for t in to_data.get("objects", []):
                                to_map_picks[t.get("code", "")] = t
                    except Exception:
                        pass

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
                                    self.trader.fetch_current_price, code
                                )
                                entry = cp_data.get("price", 0) if cp_data else 0
                            except Exception:
                                entry = 0
                        if entry and not sl:
                            sl = int(entry * 0.965)  # 기본 -3.5%
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

        if not self.is_running:
            lines = ["📋 매수 후보 (자동매매 OFF - 리포트만)"]
            for c in candidates:
                lines.append(
                    f"  {c['name']}({c['code']}) 점수:{c['total_score']:.0f} "
                    f"SL:{c['sl']:,} TP:{c['tp']:,}"
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
            if _rl == "EXTREME":
                usable_cash = int(usable_cash * 0.5)
                await _send("💀 EXTREME 이벤트 → 스윙 예산 50% 축소")
            elif _rl == "HIGH":
                usable_cash = int(usable_cash * 0.7)
                await _send("🔴 HIGH 이벤트 → 스윙 예산 30% 축소")
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
            }
            registered += 1

        lines = [f"👁 장 시작 - {registered}종목 실시간 감시 시작"]
        for code, w in self._entry_watch.items():
            mtm_tag = " [MTM]" if w.get("regime") == "MOMENTUM" else ""
            lines.append(
                f"  📡 {w['name']}({code}){mtm_tag} 점수:{w['score']:.0f} "
                f"금액:{w['buy_amount']:,}원 "
                f"({w['split_count']}분할×{w['split_amount']:,}원)"
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

            # KIS API로 실시간 조회
            try:
                price_info = self.trader.fetch_price(code)
                if not price_info.get("success"):
                    continue

                cp = price_info["current_price"]

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

                # ── 진입 조건 체크 (NORMAL 3/6, MOMENTUM 2/6) ──
                is_momentum = watch.get("regime") == "MOMENTUM"
                conditions_met = 0
                conditions_detail = []

                # 1) 갭업 체크 - "보이지않는 목표가" 기반 판단
                #    고정 5% 거부 X → TP 대비 업사이드가 충분하면 갭업도 매수
                gap_pct = (open_price / prev_close - 1) * 100 if prev_close > 0 else 0
                tp = watch.get("tp", 0)
                upside_to_tp = (tp / cp - 1) * 100 if tp > 0 and cp > 0 else 0

                # 갭업이어도 목표가까지 5% 이상 남으면 → 매수 OK
                # 갭업인데 목표가까지 5% 미만 → 리스크 대비 수익 부족 → 패스
                if gap_pct >= 3.0 and upside_to_tp < 5.0:
                    expired.append(code)
                    await self._alert(
                        f"⛔ 갭업+업사이드 부족: {watch['name']}({code})\n"
                        f"   전일 {prev_close:,} → 시가 {open_price:,} ({gap_pct:+.1f}%)\n"
                        f"   현재 {cp:,} → 목표 {tp:,} (업사이드 {upside_to_tp:+.1f}%)\n"
                        f"   R:R 불리 - 오늘 패스"
                    )
                    continue
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

                # 4) 체결강도 100+ (매수 우위)
                strength = price_info.get("strength", 0)
                if strength >= 100:
                    conditions_met += 1
                    conditions_detail.append(f"체결{strength:.0f}")

                # 5) AI EYE 점수 체크
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
                # MOMENTUM: MACD 스킵 (초기 진입이므로 0선 안 왔을 수 있음)
                if is_momentum:
                    conditions_met += 1
                    conditions_detail.append("MACD스킵(MTM)")
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
                if watch["checks"] % 10 == 0:
                    logger.info(
                        f"진입감시 {watch['name']}: "
                        f"현재{cp:,} 시가{open_price:,} 갭{gap_pct:+.1f}% | "
                        f"조건 {conditions_met}/6 | "
                        f"{' '.join(conditions_detail)}"
                    )

                # ── 진입 조건 충족! → 분할매수 ──
                # MOMENTUM: 2개 / NORMAL: 3개 (연말 4개)
                _now = datetime.now()
                _yearend = (_now.month == 12 and _now.day >= 15) or (_now.month == 1 and _now.day <= 5)
                if is_momentum:
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
                                }
                                # MOMENTUM: 타이트 SL (-3.5%)
                                if watch.get("regime") == "MOMENTUM":
                                    mtm_sl = int(cp * 0.965)
                                    self._positions[code]["stop_loss"] = max(mtm_sl, watch["sl"])
                                try:
                                    rtm = self._get_rt_monitor()
                                    rtm.register_position(code, watch["name"], cp, sl, tp)
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

        # 만료/완료 항목 제거
        for code in expired:
            self._entry_watch.pop(code, None)


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
            if _rl == "EXTREME":
                buy_amount = int(buy_amount * 0.5)
            elif _rl == "HIGH":
                buy_amount = int(buy_amount * 0.7)
        except Exception:
            pass

        # FIX-02: BRAIN 교차 신호 — 관망 차단 + 포지션 캡
        brain_alloc = self._load_brain_allocation()
        if brain_alloc.get("_block_all_buys"):
            await _send(f"🚫 BRAIN 관망모드 — day mode 매수 중단")
            return

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
                cp = price_info.get("current_price", 0) if price_info.get("success") else 0
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
                    actual_qty = 1
                    for p in pre_bal.get("positions", []):
                        if p["code"] == code:
                            actual_qty = p.get("qty", 1)
                            break
                    # 매도 실행
                    result = self.trader.liquidate_one(code)
                    if not result or not result.get("success"):
                        logger.error(f"AI 매도 실패 {code}: {result} — 포지션 유지")
                        continue
                    # 실현 손익 기록
                    pnl_amount = (snap.price - pos["entry_price"]) * actual_qty
                    self.record_realized_loss(pnl_amount)
                    self._positions.pop(code, None)
                    rtm.unregister_position(code)
                    await self._alert(rtm.format_decision_alert(snap))

                elif snap.decision == "PARTIAL_SELL":
                    logger.info(f"AI 부분매도: {code} @ {snap.price:,} ({snap.decision_reason})")
                    bal = self.trader.fetch_balance()
                    for p in bal.get("positions", []):
                        if p["code"] == code:
                            half = max(1, p["qty"] // 2)
                            sell_r = self.trader.smart_sell(code, half)
                            if sell_r and sell_r.get("success"):
                                await self._alert(rtm.format_decision_alert(snap))
                            else:
                                logger.error(f"AI 부분매도 실패 {code}: {sell_r}")
                            break

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
        # Eye→Guardian 리스크 보정: DYING +30, WEAKENING +10, ALIVE 0, BREAKING -10
        EYE_RISK_MAP = {"DYING": 30, "WEAKENING": 10, "ALIVE": 0,
                        "BREAKING": -10, "BOUNCING": 0, "WARMUP": 0}
        eye_adj = EYE_RISK_MAP.get(verdict.verdict, 0)

        # (1) DYING + HIGH confidence → Guardian 즉시 재평가
        if verdict.verdict == "DYING" and verdict.confidence >= 0.70:
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
                    actual_qty = 1
                    for p_item in pre_bal.get("positions", []):
                        if p_item["code"] == code:
                            actual_qty = p_item.get("qty", 1)
                            break
                    result = self.trader.liquidate_one(code)
                    if result and result.get("success"):
                        cp = pos.get("current_price", pos.get("entry_price", 0))
                        pnl = (cp - pos["entry_price"]) * actual_qty
                        self.record_realized_loss(pnl)
                        self._positions.pop(code, None)
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

    async def _job_monitor_fallback(self):
        """AI 모니터 실패 시 폴백: SL + 인트라데이 트레일링 스탑 체크"""
        for code, pos in list(self._positions.items()):
            try:
                price_info = self.trader.fetch_price(code)
                if not price_info.get("success"):
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

                if cp <= effective_sl:
                    # 매도 전 실제 보유수량 조회
                    pre_bal = self.trader.fetch_balance()
                    actual_qty = 1
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
                    self.record_realized_loss(pnl)
                    self._positions.pop(code, None)

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

                elif self.mode == "day" and cp >= pos["take_profit"]:
                    result = self.trader.liquidate_one(code)
                    if not result or not result.get("success"):
                        logger.error(f"TP 매도 실패 {code}: {result} — 포지션 유지")
                        continue
                    self._positions.pop(code, None)
                    gain = cp - entry
                    await self._alert(
                        f"익절\n{name}({code}) @ {cp:,}원\n"
                        f"진입:{entry:,} -> 현재:{cp:,} (+{gain:,})"
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
            for code, pos in self._positions.items():
                if pos.get("regime") == "MOMENTUM":
                    if check_supply_withdrawal(code):
                        momentum_exit_codes.add(code)
                        logger.info(f"[MOMENTUM 수급이탈] {pos.get('name', code)}({code})")
        except Exception as e:
            logger.warning(f"수급 이탈 체크 실패: {e}")

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
                effective_max = 5 if pos.get("source") == "momentum" else max_hold
                if pos.get("regime") == "MOMENTUM":
                    effective_max = min(effective_max, 5)
                if code in reversal_codes:
                    effective_max = min(effective_max, 3)
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
                    result = self.trader.liquidate_one(code)
                    if result and result.get("success"):
                        self._positions.pop(code, None)
                        await self._alert(f"⛔ 동적 손절: {name}({code}) @ {cp:,}")
                    else:
                        logger.error(f"동적 손절 매도 실패 {code}: {result}")
                        await self._alert(f"❌ 손절 매도 실패: {name}({code}) — 수동 확인 필요")
                elif action == ACTION_FULL_SELL:
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
                                    await self._alert(
                                        f"🟡 반분할 익절: {name}({code}) {half}주 @ {cp:,}\n"
                                        f"   나머지 트레일링 전환 ({reason})"
                                )
                                break
                    else:
                        result = self.trader.liquidate_one(code)
                        if not result or not result.get("success"):
                            logger.error(f"동적 전량매도 실패 {code}: {result} — 포지션 유지")
                            continue
                        self._positions.pop(code, None)
                        await self._alert(f"🔴 동적 전량매도: {name}({code}) @ {cp:,} ({reason})")
                elif action == ACTION_ADD:
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
                                await self._alert(
                                    f"🔵 추매 확인 대기: {name}({code})\n"
                                    f"   {reason}\n"
                                    f"   현재 {cp:,}원 ({pnl:+.1f}%)\n"
                                    f"   추매 금액: {add_amount:,}원\n\n"
                                    f"   실행: '자동확인' | 취소: '자동취소'"
                                )
                            else:
                                result = self.trader.safe_buy(code, add_amount)
                                if result.get("success"):
                                    await self._alert(
                                        f"🔵 추매 완료: {name}({code}) @ {cp:,}원\n"
                                        f"   {reason}\n"
                                        f"   추매 금액: {add_amount:,}원"
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
                # preclose 제외하고 개별 청산
                for code, pos in list(self._positions.items()):
                    if code in preclose_codes:
                        continue
                    qty = pos.get("qty", 0)
                    if qty > 0:
                        try:
                            self.trader.liquidate_one(code, qty)
                        except Exception as e:
                            logger.warning(f"EOD 청산 실패 {code}: {e}")
                    self._positions.pop(code, None)
                result = {"success": True, "message": "preclose 제외 청산 완료"}
            else:
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

    async def job_brain_allocation(self, context):
        """16:36 - BRAIN 자본 배분 백업 스케줄 (NIGHTWATCH 실패 대비)"""
        if date.today().weekday() >= 5:
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
                        import json as _json
                        out_path = PL_DIR / f"{date.today().isoformat()}.json"
                        with open(out_path, "w", encoding="utf-8") as f:
                            _json.dump(pl_data, f, ensure_ascii=False, indent=2)
            except Exception as e:
                logger.warning(f"Equal Level 실패: {e}")

            logger.info(f"프리미엄 레벨 {len(results)}종목 완료 (EQ {eq_count}종목 머지)")
        except Exception as e:
            logger.error(f"프리미엄 레벨 실패: {e}")

    async def job_gap_support(self, context):
        """09:05 - 갭 지지/저항 탐지 + PL 머지"""
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
            from strategies.gap_support import run_gap_detection, merge_gap_to_premium_levels
            gap_results = await asyncio.to_thread(run_gap_detection)

            # PL 머지
            merged = 0
            if gap_results:
                try:
                    from strategies.premium_levels import load_premium_levels, PL_DIR
                    import json as _json
                    pl_data = load_premium_levels()
                    for code, gap_info in gap_results.items():
                        if code in pl_data:
                            merge_gap_to_premium_levels(pl_data[code], gap_info)
                            merged += 1
                    if merged > 0:
                        out_path = PL_DIR / f"{date.today().isoformat()}.json"
                        with open(out_path, "w", encoding="utf-8") as f:
                            _json.dump(pl_data, f, ensure_ascii=False, indent=2)
                except Exception as e:
                    logger.warning(f"GAP→PL 머지 실패: {e}")

            up = sum(1 for r in gap_results.values() if r["gap_type"] == "gap_up")
            down = len(gap_results) - up
            logger.info(f"갭 레벨 {len(gap_results)}종목 (갭업:{up} 갭다운:{down} PL머지:{merged})")
        except Exception as e:
            logger.error(f"갭 탐지 실패: {e}")

    async def job_opening_range(self, context):
        """10:05 - OR/IR 확정 + daily_bias 계산"""
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
                import json as _json
                pl_data = load_premium_levels()
                merged = 0
                for code, or_info in results.items():
                    if code in pl_data:
                        merge_or_levels(pl_data[code], or_info)
                        merged += 1
                if merged > 0:
                    out_path = PL_DIR / f"{date.today().isoformat()}.json"
                    with open(out_path, "w", encoding="utf-8") as f:
                        _json.dump(pl_data, f, ensure_ascii=False, indent=2)
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
        if date.today().weekday() >= 5:
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
        if date.today().weekday() >= 5:
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
                    if not hasattr(self, '_nxt_positions'):
                        self._nxt_positions = {}
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
        if date.today().weekday() >= 5:
            return

        nw_cfg = self.config.get("nightwatch", {})
        if not nw_cfg.get("enabled", False):
            return

        if not hasattr(self, '_nxt_positions'):
            self._nxt_positions = {}
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
                curr_price = pi.get("current_price", 0) if pi.get("success") else 0
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

    def _save_nxt_positions(self):
        """NXT 포지션 JSON 저장"""
        path = Path(__file__).resolve().parent.parent / "data_store" / "nxt_positions.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(self._nxt_positions, f, ensure_ascii=False, indent=2)

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
        if date.today().weekday() >= 5:
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
                from datetime import timedelta as _td
                # 내일 이벤트 체크 (선취매는 내일 장에서 보유)
                tomorrow = date.today() + _td(days=1)
                while tomorrow.weekday() >= 5:
                    tomorrow += _td(days=1)
                ev_risk = get_event_risk_for_recommendation(tomorrow)
                risk_level = ev_risk.get("risk_level", "LOW")

                if risk_level == "EXTREME":
                    budget_pct *= 0.3  # 40% → 12%
                    event_warning = f"💀 EXTREME 이벤트 → 예산 70% 축소"
                elif risk_level == "HIGH":
                    budget_pct *= 0.5  # 40% → 20%
                    event_warning = f"🔴 HIGH 이벤트 → 예산 50% 축소"

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

            self._positions[code] = {
                "entry_price": pos.get("entry_price", 0),
                "stop_loss": pos.get("sl", 0),
                "take_profit": pos.get("tp", 0),
                "target_state": "HOLD",
                "entry_date": pos.get("entry_date", ""),
                "source": "predawn",
                "score": pos.get("score", 0),
                "trail_high": pos.get("entry_price", 0),
            }
            merged += 1
            logger.info(
                f"[PREDAWN] 정규 포지션 전환: {pos['name']}({code}) "
                f"@{pos['entry_price']:,}원 SL={pos.get('sl', 0):,} TP={pos.get('tp', 0):,}"
            )

        if merged > 0:
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
