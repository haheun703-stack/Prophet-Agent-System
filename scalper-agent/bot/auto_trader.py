# -*- coding: utf-8 -*-
"""
Body Hunter v3 자동매매 루프
============================
스캔 → 필터 → 매수 → 감시 → 매도 자동화

JobQueue (python-telegram-bot)로 스케줄:
  09:20 → 5D 스캔 후 자동 매수
  매 30초 → 포지션 손절/익절 감시
  15:10 → 전량 청산 + 일일 요약
"""

import asyncio
import logging
from datetime import datetime
from typing import Callable, Optional

logger = logging.getLogger("BH.AutoTrader")


class AutoTrader:
    """Body Hunter v3 자동매매"""

    def __init__(self, config: dict, trader):
        self.config = config
        self.trader = trader
        self.is_running = False
        self._send_alert: Optional[Callable] = None
        self._positions = {}  # {code: {entry_price, stop_loss, take_profit}}

    def start(self, send_alert_func: Callable):
        """자동매매 시작"""
        self.is_running = True
        self._send_alert = send_alert_func
        logger.info("자동매매 시작")

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
        """아침 5D 스캔 → 자동 매수 (JobQueue 호출)"""
        if not self.is_running:
            return

        logger.info("아침 5D 스캔 시작")
        await self._alert("🌅 아침 5D 스캔 시작...")

        try:
            candidates = await asyncio.to_thread(self._scan_and_filter)
        except Exception as e:
            logger.error(f"스캔 실패: {e}")
            await self._alert(f"❌ 스캔 실패: {e}")
            return

        if not candidates:
            await self._alert("스캔 결과: 매수 후보 없음")
            return

        # 매수 후보 리포트
        lines = ["🎯 매수 후보"]
        for f in candidates:
            name = f.score.code
            from bot.kis_trader import CODE_TO_NAME
            name = CODE_TO_NAME.get(f.score.code, f.score.code)
            lines.append(
                f"  {name}({f.score.code}) {f.risk_label} "
                f"5D:{f.stability_grade}({f.stability.stability_score:.0f})"
            )

        await self._alert("\n".join(lines))

        # 자동 매수 실행
        bot_conf = self.config.get("bot", {})
        max_pos = bot_conf.get("max_auto_positions", 3)
        buy_amount = bot_conf.get("auto_buy_amount", 500000)

        bal = self.trader.fetch_balance()
        current_positions = len(bal.get("positions", [])) if bal.get("success") else 0
        slots = max_pos - current_positions

        if slots <= 0:
            await self._alert(f"보유 종목 {current_positions}개 — 추가 매수 불가")
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
                # 현재가 기반 SL/TP 설정
                price_info = self.trader.fetch_price(code)
                cp = price_info.get("current_price", 0)
                if cp > 0:
                    self._positions[code] = {
                        "entry_price": cp,
                        "stop_loss": int(cp * (1 - sl_pct)),
                        "take_profit": int(cp * (1 + tp_pct)),
                    }

                await self._alert(f"✅ 자동 매수: {result.get('message')}")
            else:
                await self._alert(f"❌ 매수 실패 {code}: {result.get('message')}")

        await self._alert(f"아침 스캔 완료: {bought}/{len(candidates[:slots])} 매수")

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
                        f"🔴 손절\n{code} @ {cp:,}원\n"
                        f"진입: {pos['entry_price']:,} → 현재: {cp:,} ({loss:+,})"
                    )
                    continue

                # 익절
                if cp >= pos["take_profit"]:
                    logger.info(f"익절 트리거: {code} @ {cp:,} (TP: {pos['take_profit']:,})")
                    result = self.trader.liquidate_one(code)
                    self._positions.pop(code, None)
                    gain = cp - pos["entry_price"]
                    await self._alert(
                        f"🟢 익절\n{code} @ {cp:,}원\n"
                        f"진입: {pos['entry_price']:,} → 현재: {cp:,} (+{gain:,})"
                    )

            except Exception as e:
                logger.error(f"감시 실패 {code}: {e}")

    async def job_eod_close(self, context):
        """장마감 전 전량 청산 (JobQueue 호출)"""
        if not self.is_running:
            return

        logger.info("장마감 전량 청산")
        await self._alert("🏁 장마감 전량 청산 시작...")

        result = self.trader.liquidate_all()
        self._positions.clear()

        await self._alert(f"{'✅' if result.get('success') else '❌'} {result.get('message')}")

        # 일일 요약
        bal = self.trader.fetch_balance()
        if bal.get("success"):
            await self._alert(
                f"📊 일일 마감\n"
                f"현금: {bal['cash']:,}원\n"
                f"총평가: {bal['total_eval']:,}원\n"
                f"보유: {len(bal['positions'])}종목"
            )

    # ═══════════════════════════════════════
    #  내부 로직
    # ═══════════════════════════════════════

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
