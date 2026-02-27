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
from datetime import datetime
from pathlib import Path
from typing import Callable, Optional

logger = logging.getLogger("BH.AutoTrader")

BASE_DIR = Path(__file__).resolve().parent.parent
CANDIDATES_PATH = BASE_DIR / "data_store" / "swing_candidates.json"


class AutoTrader:
    """Body Hunter v4 자동매매 — 동적 목표가 통합"""

    def __init__(self, config: dict, trader):
        self.config = config
        self.trader = trader
        self.is_running = False
        self._send_alert: Optional[Callable] = None
        self._positions = {}  # {code: {entry_price, stop_loss, take_profit, target_state, ...}}

        # 모드: "day" or "swing"
        self.mode = config.get("bot", {}).get("trade_mode", "swing")

    def start(self, send_alert_func: Callable):
        """자동매매 시작"""
        self.is_running = True
        self._send_alert = send_alert_func
        logger.info(f"자동매매 시작 (모드: {self.mode})")

    def stop(self):
        """자동매매 정지"""
        self.is_running = False
        logger.info("자동매매 정지")

    def _is_market_hours(self) -> bool:
        now = datetime.now()
        if now.weekday() >= 5:
            return False
        h = now.hour * 100 + now.minute
        return 900 <= h <= 1520

    async def _alert(self, text: str):
        if self._send_alert:
            try:
                await self._send_alert(text)
            except Exception as e:
                logger.error(f"알림 전송 실패: {e}")

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
        """스윙 모드 아침 스캔: swing_candidates.json 기반 매수"""
        # swing_candidates.json 로드
        candidates = self._load_swing_candidates()
        if not candidates:
            # 후보가 없으면 swing_picker 실행
            try:
                await _send("스윙 후보 없음 — 7팩터 스캔 실행 중...")
                candidates = await asyncio.to_thread(self._run_swing_picker)
            except Exception as e:
                logger.error(f"스윙 피커 실패: {e}")
                await _send(f"❌ 스윙 피커 실패: {e}")
                return

        if not candidates:
            await _send("스캔 결과: 매수 후보 없음")
            return

        # 리포트 전송
        lines = ["🎯 스윙 매수 후보"]
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
        for c in candidates[:slots]:
            code = c["code"]
            if code in self._positions:
                continue

            result = self.trader.safe_buy(code, buy_amount)
            if result.get("success"):
                bought += 1
                price_info = self.trader.fetch_price(code)
                cp = price_info.get("current_price", c["entry"])

                # 동적 목표가 엔진으로 초기 설정
                target_state = self._init_dynamic_target(code, c["name"], cp)

                self._positions[code] = {
                    "entry_price": cp,
                    "stop_loss": target_state.dynamic_sl if target_state else c["sl"],
                    "take_profit": target_state.dynamic_tp if target_state else c["tp"],
                    "entry_date": datetime.now().strftime("%Y-%m-%d"),
                    "name": c["name"],
                    "target_state": target_state,
                }

                sl = self._positions[code]["stop_loss"]
                tp = self._positions[code]["take_profit"]
                await _send(
                    f"✅ 스윙 매수: {result.get('message')}\n"
                    f"   SL:{sl:,} TP:{tp:,} (동적)"
                )
            else:
                await _send(f"❌ 매수 실패 {code}: {result.get('message')}")

        await _send(f"아침 스캔 완료: {bought}/{min(len(candidates), slots)} 매수")

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
        """포지션 감시 — 손절/익절 체크 (JobQueue 반복 호출)"""
        if not self.is_running:
            return
        if not self._is_market_hours():
            return
        if not self._positions:
            return

        for code, pos in list(self._positions.items()):
            try:
                price_info = self.trader.fetch_price(code)
                if not price_info.get("success"):
                    continue

                cp = price_info["current_price"]

                # 손절
                if cp <= pos["stop_loss"]:
                    logger.info(f"손절 트리거: {code} @ {cp:,} (SL: {pos['stop_loss']:,})")
                    result = self.trader.liquidate_one(code)
                    self._positions.pop(code, None)
                    loss = cp - pos["entry_price"]
                    await self._alert(
                        f"🔴 손절\n{pos.get('name', code)}({code}) @ {cp:,}원\n"
                        f"진입: {pos['entry_price']:,} → 현재: {cp:,} ({loss:+,})"
                    )
                    continue

                # 익절 (데이 모드에서만 TP 고정 매도)
                if self.mode == "day" and cp >= pos["take_profit"]:
                    logger.info(f"익절 트리거: {code} @ {cp:,} (TP: {pos['take_profit']:,})")
                    result = self.trader.liquidate_one(code)
                    self._positions.pop(code, None)
                    gain = cp - pos["entry_price"]
                    await self._alert(
                        f"🟢 익절\n{pos.get('name', code)}({code}) @ {cp:,}원\n"
                        f"진입: {pos['entry_price']:,} → 현재: {cp:,} (+{gain:,})"
                    )

            except Exception as e:
                logger.error(f"감시 실패 {code}: {e}")

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
                    # 부분매도: 보유수량의 50%
                    bal = self.trader.fetch_balance()
                    for p in bal.get("positions", []):
                        if p["code"] == code:
                            half = max(1, p["qty"] // 2)
                            self.trader.sell_market(code, half)
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
