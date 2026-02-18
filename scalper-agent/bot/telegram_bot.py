# -*- coding: utf-8 -*-
"""
Body Hunter v3 텔레그램 봇
===========================
한글 버튼 명령어로 5D 분석 + KIS 실매매 제어

모든 명령어가 한글:
  스캔, 리포트, 분석, 현재잔고, 체결내역, 매수, 매도, 청산, 시작, 정지, 상태 ...
"""

import os
import sys
import asyncio
import logging
from pathlib import Path
from datetime import datetime, time as dtime

from telegram import Update, ReplyKeyboardMarkup, BotCommand
from telegram.ext import (
    Application, CommandHandler, MessageHandler,
    ContextTypes, filters,
)

from bot.kis_trader import KISTrader, resolve_stock, CODE_TO_NAME
from bot.auto_trader import AutoTrader

logger = logging.getLogger("BH.Bot")

# 텔레그램 4096자 제한
TG_MAX = 4096


def _split_message(text: str, limit: int = TG_MAX) -> list:
    """긴 메시지를 텔레그램 제한에 맞게 분할"""
    if len(text) <= limit:
        return [text]
    chunks = []
    current = ""
    for line in text.split("\n"):
        if len(current) + len(line) + 1 > limit - 50:
            chunks.append(current)
            current = line + "\n"
        else:
            current += line + "\n"
    if current.strip():
        chunks.append(current)
    return chunks


# 한글 키보드 레이아웃
MAIN_KEYBOARD = ReplyKeyboardMarkup(
    [
        ["스캔", "ETF", "리포트"],
        ["현재잔고", "체결내역", "포트폴리오"],
        ["시작", "정지", "상태"],
        ["유니버스", "일지", "도움"],
        ["청산", "로그"],
    ],
    resize_keyboard=True,
)

HELP_TEXT = """
🔮 Body Hunter v3 명령어

[분석]
  스캔 — 5D 전종목 수급 스캔
  ETF — ETF 유니버스 스캔
  리포트 — 5D 리포트 전송
  분석 삼성전자 — 개별 종목 분석

[매매]
  매수 삼성전자 10 — 시장가 매수
  매도 삼성전자 — 전량 매도
  청산 — 전종목 청산
  현재잔고 — 계좌 잔고
  체결내역 — 당일 체결
  포트폴리오 — 보유 + 손익

[자동매매]
  시작 — 자동매매 ON
  정지 — 자동매매 OFF

[복기]
  일지 — 오늘 매매 일지
  일지 2026-02-18 — 특정일 일지

[시스템]
  상태 — 봇 상태
  유니버스 — 유니버스 종목 현황
  유니버스갱신 — 시총 1조+ 리빌드
  로그 — 최근 로그
  도움 — 이 메시지
""".strip()


class BodyHunterBot:
    """Body Hunter v3 텔레그램 봇"""

    def __init__(self, config: dict):
        self.config = config
        self.token = os.getenv("TELEGRAM_BOT_TOKEN")
        self.chat_id = os.getenv("TELEGRAM_CHAT_ID")
        self.trader = KISTrader(config)
        self.auto_trader = AutoTrader(config, self.trader)
        self.start_time = datetime.now()
        self._pending_orders = {}  # chat_id → {type, code, qty, name}

    def _is_authorized(self, update: Update) -> bool:
        """본인 채팅 확인"""
        cid = update.effective_chat.id
        ok = str(cid) == str(self.chat_id)
        if not ok:
            logger.warning(f"인증 실패: chat_id={cid}, 허용={self.chat_id}")
        return ok

    # ═══════════════════════════════════════
    #  시스템
    # ═══════════════════════════════════════

    async def cmd_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        logger.info(f"[수신] /start from chat_id={update.effective_chat.id}")
        await update.message.reply_text(
            "🔮 Body Hunter v3\n"
            "아래 버튼으로 명령하세요",
            reply_markup=MAIN_KEYBOARD,
        )

    async def cmd_help(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._is_authorized(update):
            return
        await update.message.reply_text(HELP_TEXT, reply_markup=MAIN_KEYBOARD)

    async def cmd_status(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._is_authorized(update):
            return
        uptime = datetime.now() - self.start_time
        hours = int(uptime.total_seconds() // 3600)
        minutes = int((uptime.total_seconds() % 3600) // 60)

        now = datetime.now()
        market_open = now.replace(hour=9, minute=0, second=0)
        market_close = now.replace(hour=15, minute=20, second=0)
        is_market = market_open <= now <= market_close and now.weekday() < 5

        auto_status = "ON 🟢" if self.auto_trader.is_running else "OFF 🔴"

        lines = [
            "📊 시스템 상태",
            "━" * 25,
            f"가동시간: {hours}시간 {minutes}분",
            f"장상태: {'장중 🟢' if is_market else '장외 🔴'}",
            f"자동매매: {auto_status}",
            f"현재시각: {now.strftime('%H:%M:%S')}",
        ]

        bal = await asyncio.to_thread(self.trader.fetch_balance)
        if bal.get("success"):
            lines.append(f"현금: {bal['cash']:,}원")
            lines.append(f"보유: {len(bal['positions'])}종목")

        await update.message.reply_text("\n".join(lines))

    async def cmd_log(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._is_authorized(update):
            return
        log_dir = Path(__file__).resolve().parent.parent / "logs"
        today = datetime.now().strftime("%Y%m%d")
        log_file = log_dir / f"bot_{today}.log"

        if not log_file.exists():
            await update.message.reply_text("오늘 로그 파일 없음")
            return

        lines = log_file.read_text(encoding="utf-8", errors="ignore").split("\n")
        last_20 = "\n".join(lines[-20:])
        await update.message.reply_text(f"📋 최근 로그\n```\n{last_20}\n```")

    # ═══════════════════════════════════════
    #  분석
    # ═══════════════════════════════════════

    async def cmd_scan(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._is_authorized(update):
            return
        await update.message.reply_text("🔍 5D 수급 스캔 실행중... (30초~1분)")

        def _run():
            from output.send_4d_report import generate_report
            return generate_report()

        try:
            msgs = await asyncio.to_thread(_run)
            for msg in msgs:
                for chunk in _split_message(msg):
                    await update.message.reply_text(chunk)
                    await asyncio.sleep(0.5)
        except Exception as e:
            logger.error(f"스캔 실패: {e}", exc_info=True)
            await update.message.reply_text(f"⚠️ 스캔 실패: {str(e)[:200]}")

    async def cmd_etf_scan(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._is_authorized(update):
            return
        await update.message.reply_text("🔍 ETF 유니버스 스캔중... (2~3분)")

        def _run():
            from etf_scanner import run_scan, format_report
            df = run_scan(min_volume_bill=100, top_n=15)
            if df.empty:
                return "시그널 없음"
            return format_report(df, top_n=15)

        report = await asyncio.to_thread(_run)

        for chunk in _split_message(report):
            await update.message.reply_text(f"```\n{chunk}\n```",
                                            parse_mode="Markdown")
            await asyncio.sleep(0.5)

    async def cmd_report(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._is_authorized(update):
            return
        await update.message.reply_text("📊 5D 리포트 생성 + 전송중...")

        def _run():
            from output.send_4d_report import generate_report
            return generate_report()

        try:
            msgs = await asyncio.to_thread(_run)
            for msg in msgs:
                for chunk in _split_message(msg):
                    await update.message.reply_text(chunk)
                    await asyncio.sleep(0.5)
            await update.message.reply_text(f"✅ 리포트 전송 완료 ({len(msgs)}파트)")
        except Exception as e:
            logger.error(f"리포트 실패: {e}", exc_info=True)
            await update.message.reply_text(f"⚠️ 리포트 실패: {str(e)[:200]}")

    async def cmd_analyze(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._is_authorized(update):
            return
        text = update.message.text.strip()
        parts = text.split()

        if len(parts) < 2:
            await update.message.reply_text("사용법: 분석 삼성전자\n또는: 분석 005930")
            return

        query = parts[1]
        code, name = resolve_stock(query)
        if code is None:
            await update.message.reply_text(f"종목을 찾을 수 없습니다: {query}")
            return

        await update.message.reply_text(f"🔍 {name}({code}) 분석중...")

        def _run():
            from data.supply_analyzer import SupplyAnalyzer
            analyzer = SupplyAnalyzer()
            f = analyzer.analyze_full(code)
            if f is None:
                return None
            return f

        full = await asyncio.to_thread(_run)

        if full is None:
            await update.message.reply_text(f"{name}({code}) 데이터 부족")
            return

        s = full.score
        m = full.momentum
        st = full.stability

        lines = [
            f"🔮 {name} ({code}) 5D 분석",
            "━" * 25,
            f"판정: {full.risk_label}",
            f"3D: {s.grade}({s.total_score:.0f}점)",
            f"4D: {m.signal}({m.momentum_score:.0f}점)",
        ]

        if st:
            lines.append(f"5D: {st.stability_grade}({st.stability_score:.0f}점)")
            lines.append(f"  스윙: {st.volatility_score:.0f} | 유동: {st.liquidity_score:.0f}")
            lines.append(f"  SM강도: {st.intensity_score:.0f} | 신호: {st.alignment_score:.0f}")
            lines.append(f"  ATR: {st.atr_pct:.1f}% | SM비율: {st.smart_money_ratio:+.1f}%")

        # PER/PBR 밸류에이션
        if full.per > 0 or full.pbr > 0:
            per_str = f"{full.per:.1f}" if full.per > 0 else "적자"
            lines.append(f"PER: {per_str} | PBR: {full.pbr:.2f}")
        if full.valuation_warning:
            lines.append(f"⚠️ 밸류: {full.valuation_warning}")

        lines.append("")
        lines.append(f"기관: {m.inst_streak:+d}일 ({m.inst_streak_amount:+.0f}억)")
        lines.append(f"외인변곡: {m.foreign_inflection}")
        lines.append(f"개인역지표: {'O ✅' if m.retail_contrarian else 'X'}")

        price = await asyncio.to_thread(self.trader.fetch_price, code)
        if price.get("success"):
            p = price["current_price"]
            cr = price["change_rate"]
            lines.append(f"\n현재가: {p:,}원 ({cr:+.2f}%)")

        await update.message.reply_text("\n".join(lines))

    # ═══════════════════════════════════════
    #  매매
    # ═══════════════════════════════════════

    async def cmd_balance(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._is_authorized(update):
            return
        await update.message.reply_text("💰 잔고 조회중...")
        bal = await asyncio.to_thread(self.trader.fetch_balance)

        if not bal.get("success"):
            await update.message.reply_text(f"❌ {bal.get('message')}")
            return

        lines = [
            "💰 현재 잔고",
            "━" * 25,
            f"현금: {bal['cash']:,}원",
            f"총평가: {bal['total_eval']:,}원",
            f"보유: {len(bal['positions'])}종목",
        ]

        for p in bal["positions"]:
            sign = "+" if p["pnl_rate"] >= 0 else ""
            lines.append("")
            lines.append(f"📌 {p['name']}({p['code']})")
            lines.append(f"  {p['qty']}주 @ {p['avg_price']:,}원")
            lines.append(f"  현재가: {p['current_price']:,}원")
            lines.append(f"  손익: {p['pnl_amount']:+,}원 ({sign}{p['pnl_rate']:.1f}%)")

        await update.message.reply_text("\n".join(lines))

    async def cmd_executions(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._is_authorized(update):
            return
        await update.message.reply_text("📋 체결내역 조회중...")
        result = await asyncio.to_thread(self.trader.fetch_open_orders)

        if not result.get("success"):
            await update.message.reply_text(f"❌ {result.get('message')}")
            return

        orders = result["orders"]
        if not orders:
            await update.message.reply_text("오늘 미체결 주문 없음")
            return

        lines = ["📋 미체결 주문", "━" * 25]
        for o in orders:
            lines.append(
                f"{o['side']} {o['name']}({o['code']}) "
                f"{o['qty']}주 @ {o['price']:,}원 "
                f"(체결: {o['filled_qty']}주)"
            )

        await update.message.reply_text("\n".join(lines))

    async def cmd_portfolio(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._is_authorized(update):
            return
        await update.message.reply_text("📊 포트폴리오 조회중...")
        bal = await asyncio.to_thread(self.trader.fetch_balance)

        if not bal.get("success"):
            await update.message.reply_text(f"❌ {bal.get('message')}")
            return

        total = bal["total_eval"] or 1
        lines = [
            "📊 포트폴리오",
            "━" * 25,
            f"총평가: {bal['total_eval']:,}원",
            f"현금: {bal['cash']:,}원 ({bal['cash']/total*100:.1f}%)",
        ]

        total_pnl = 0
        for p in bal["positions"]:
            ratio = (p["current_price"] * p["qty"]) / total * 100
            total_pnl += p["pnl_amount"]
            sign = "📈" if p["pnl_rate"] >= 0 else "📉"
            lines.append(
                f"\n{sign} {p['name']} ({ratio:.1f}%)\n"
                f"  {p['qty']}주 | {p['pnl_amount']:+,}원 ({p['pnl_rate']:+.1f}%)"
            )

        lines.append(f"\n총 손익: {total_pnl:+,}원")

        await update.message.reply_text("\n".join(lines))

    async def cmd_buy(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._is_authorized(update):
            return
        text = update.message.text.strip()
        parts = text.split()

        if len(parts) < 3:
            await update.message.reply_text(
                "사용법: 매수 종목 수량\n"
                "예: 매수 삼성전자 10\n"
                "예: 매수 005930 5"
            )
            return

        query = parts[1]
        try:
            qty = int(parts[2])
        except ValueError:
            await update.message.reply_text("수량은 숫자로 입력하세요")
            return

        code, name = resolve_stock(query)
        if code is None:
            await update.message.reply_text(f"종목을 찾을 수 없습니다: {query}")
            return

        confirm = self.config.get("bot", {}).get("confirm_real_order", True)
        if confirm:
            price = await asyncio.to_thread(self.trader.fetch_price, code)
            p = price.get("current_price", 0) if price.get("success") else 0
            est = p * qty

            self._pending_orders[update.effective_chat.id] = {
                "type": "buy", "code": code, "qty": qty, "name": name,
            }

            await update.message.reply_text(
                f"⚠️ 매수 주문 확인\n"
                f"종목: {name}({code})\n"
                f"수량: {qty}주\n"
                f"현재가: {p:,}원\n"
                f"예상금액: {est:,}원\n\n"
                f"실행하려면 '확인' 입력"
            )
            return

        result = await asyncio.to_thread(self.trader.buy_market, code, qty)
        await update.message.reply_text(
            f"{'✅' if result.get('success') else '❌'} {result.get('message')}"
        )

    async def cmd_sell(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._is_authorized(update):
            return
        text = update.message.text.strip()
        parts = text.split()

        if len(parts) < 2:
            await update.message.reply_text("사용법: 매도 종목명\n예: 매도 삼성전자")
            return

        query = parts[1]
        code, name = resolve_stock(query)
        if code is None:
            await update.message.reply_text(f"종목을 찾을 수 없습니다: {query}")
            return

        qty = int(parts[2]) if len(parts) >= 3 else None

        confirm = self.config.get("bot", {}).get("confirm_real_order", True)
        if confirm:
            self._pending_orders[update.effective_chat.id] = {
                "type": "sell", "code": code, "qty": qty, "name": name,
            }
            qty_text = f"{qty}주" if qty else "전량"
            await update.message.reply_text(
                f"⚠️ 매도 주문 확인\n"
                f"종목: {name}({code})\n"
                f"수량: {qty_text}\n\n"
                f"실행하려면 '확인' 입력"
            )
            return

        if qty:
            result = await asyncio.to_thread(self.trader.sell_market, code, qty)
        else:
            result = await asyncio.to_thread(self.trader.liquidate_one, code)

        await update.message.reply_text(
            f"{'✅' if result.get('success') else '❌'} {result.get('message')}"
        )

    async def cmd_confirm(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._is_authorized(update):
            return
        chat_id = update.effective_chat.id
        pending = self._pending_orders.pop(chat_id, None)

        if pending is None:
            await update.message.reply_text("대기 중인 주문이 없습니다")
            return

        await update.message.reply_text("⏳ 주문 실행중...")

        if pending["type"] == "buy":
            result = await asyncio.to_thread(
                self.trader.buy_market, pending["code"], pending["qty"]
            )
        elif pending["type"] == "sell":
            if pending["qty"]:
                result = await asyncio.to_thread(
                    self.trader.sell_market, pending["code"], pending["qty"]
                )
            else:
                result = await asyncio.to_thread(
                    self.trader.liquidate_one, pending["code"]
                )
        elif pending["type"] == "liquidate_all":
            result = await asyncio.to_thread(self.trader.liquidate_all)
        else:
            result = {"success": False, "message": "알 수 없는 주문"}

        await update.message.reply_text(
            f"{'✅' if result.get('success') else '❌'} {result.get('message')}"
        )

    async def cmd_liquidate(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._is_authorized(update):
            return
        confirm = self.config.get("bot", {}).get("confirm_real_order", True)
        if confirm:
            bal = await asyncio.to_thread(self.trader.fetch_balance)
            n = len(bal.get("positions", [])) if bal.get("success") else "?"
            self._pending_orders[update.effective_chat.id] = {"type": "liquidate_all"}
            await update.message.reply_text(
                f"⚠️ 전량 청산 확인\n"
                f"보유 종목: {n}개 전부 시장가 매도\n\n"
                f"실행하려면 '확인' 입력"
            )
            return

        result = await asyncio.to_thread(self.trader.liquidate_all)
        await update.message.reply_text(
            f"{'✅' if result.get('success') else '❌'} {result.get('message')}"
        )

    # ═══════════════════════════════════════
    #  매매 일지
    # ═══════════════════════════════════════

    async def cmd_journal(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """매매 일지 조회"""
        if not self._is_authorized(update):
            return

        text = update.message.text.strip()
        parts = text.split()
        target_date = parts[1] if len(parts) >= 2 else None

        journal = await asyncio.to_thread(self.trader.get_trade_journal, target_date)

        if not journal.get("success"):
            await update.message.reply_text(f"❌ {journal.get('message')}")
            return

        trades = journal["trades"]
        summary = journal["summary"]
        d = journal["date"]

        if not trades:
            await update.message.reply_text(f"📋 {d} 매매 기록 없음")
            return

        lines = [
            f"📋 매매 일지 ({d})",
            "━" * 25,
        ]

        for i, t in enumerate(trades, 1):
            side_icon = "🔴" if t["side"] == "BUY" else "🔵"
            price_str = f" @ {t['price']:,}원" if "price" in t else ""
            amt_str = f" ≈ {t.get('est_amount', 0):,}원" if t.get("est_amount") else ""
            split_str = f" ({t['split']}분할)" if t.get("split", 1) > 1 else ""
            lines.append(
                f"{side_icon} {t['time']} {t['side']} {t['name']}({t['code']}) "
                f"{t['qty']}주{price_str}{amt_str}{split_str}"
            )

        lines.append("")
        lines.append("━" * 25)
        lines.append(f"매수: {summary['buy_count']}건 ({summary['total_buy_amount']:,}원)")
        lines.append(f"매도: {summary['sell_count']}건 ({summary['total_sell_amount']:,}원)")
        net = summary['total_sell_amount'] - summary['total_buy_amount']
        lines.append(f"순매매: {net:+,}원")

        await update.message.reply_text("\n".join(lines))

    # ═══════════════════════════════════════
    #  유니버스
    # ═══════════════════════════════════════

    async def cmd_universe(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """유니버스 현황 조회"""
        if not self._is_authorized(update):
            return
        try:
            from data.universe_builder import load_universe, UNIVERSE_FILE
            uni = load_universe()
            if not uni:
                await update.message.reply_text("유니버스 미생성\n'유니버스갱신' 으로 빌드하세요")
                return

            kospi = sum(1 for v in uni.values() if v["market"] == "KOSPI")
            kosdaq = sum(1 for v in uni.values() if v["market"] == "KOSDAQ")
            top5 = list(uni.items())[:5]
            bottom5 = list(uni.items())[-5:]

            # 파일 수정시간
            import os
            mtime = datetime.fromtimestamp(os.path.getmtime(UNIVERSE_FILE))
            date_str = mtime.strftime("%Y-%m-%d %H:%M")

            top_str = "\n".join(
                f"  {c} {v['name']} ({v['cap_億']:,}억)" if 'cap_億' in v
                else f"  {c} {v['name']} ({v.get('cap_억', 0):,}억)"
                for c, v in top5
            )
            bot_str = "\n".join(
                f"  {c} {v['name']} ({v.get('cap_억', 0):,}억)"
                for c, v in bottom5
            )

            msg = (
                f"📊 유니버스 현황\n"
                f"{'━' * 25}\n"
                f"총 {len(uni)}종목 (KOSPI {kospi} + KOSDAQ {kosdaq})\n"
                f"기준: 시총 1조원 이상\n"
                f"갱신: {date_str}\n\n"
                f"[시총 상위 5]\n{top_str}\n\n"
                f"[시총 하위 5]\n{bot_str}"
            )
            await update.message.reply_text(msg)
        except Exception as e:
            logger.error(f"유니버스 조회 에러: {e}")
            await update.message.reply_text(f"유니버스 조회 실패: {e}")

    async def cmd_universe_rebuild(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """유니버스 재빌드 (시총 1조+)"""
        if not self._is_authorized(update):
            return
        await update.message.reply_text("🔄 유니버스 리빌드중... (1~2분)")
        try:
            from data.universe_builder import build_universe
            uni = await asyncio.to_thread(build_universe, 10000)

            # kis_collector 모듈의 UNIVERSE도 갱신
            import data.kis_collector as kc
            from data.universe_builder import get_universe_dict
            kc.UNIVERSE = get_universe_dict()

            # kis_trader의 NAME_TO_CODE, CODE_TO_NAME도 갱신
            import bot.kis_trader as kt
            kt.NAME_TO_CODE = {info[0]: code for code, info in kc.UNIVERSE.items()}
            kt.CODE_TO_NAME = {code: info[0] for code, info in kc.UNIVERSE.items()}

            kospi = sum(1 for v in uni.values() if v["market"] == "KOSPI")
            kosdaq = sum(1 for v in uni.values() if v["market"] == "KOSDAQ")
            await update.message.reply_text(
                f"✅ 유니버스 갱신 완료\n"
                f"총 {len(uni)}종목 (KOSPI {kospi} + KOSDAQ {kosdaq})\n"
                f"시총 1조원 이상 필터 적용"
            )
        except Exception as e:
            logger.error(f"유니버스 갱신 에러: {e}")
            await update.message.reply_text(f"유니버스 갱신 실패: {e}")

    # ═══════════════════════════════════════
    #  자동매매
    # ═══════════════════════════════════════

    async def cmd_auto_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._is_authorized(update):
            return
        if self.auto_trader.is_running:
            await update.message.reply_text("이미 자동매매 실행중입니다")
            return

        async def _send_alert(text):
            await context.bot.send_message(chat_id=int(self.chat_id), text=text)

        self.auto_trader.start(_send_alert)
        await update.message.reply_text(
            "🟢 자동매매 시작\n"
            "━" * 20 + "\n"
            f"아침 스캔: {self.config.get('bot', {}).get('morning_scan_time', '09:20')}\n"
            f"감시 주기: {self.config.get('bot', {}).get('scan_interval_sec', 30)}초\n"
            f"장마감 청산: {self.config.get('bot', {}).get('eod_close_time', '15:10')}\n"
            f"최대 보유: {self.config.get('bot', {}).get('max_auto_positions', 3)}종목\n"
            f"1회 금액: {self.config.get('bot', {}).get('auto_buy_amount', 500000):,}원"
        )

    async def cmd_auto_stop(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._is_authorized(update):
            return
        self.auto_trader.stop()
        await update.message.reply_text("🔴 자동매매 정지")

    # ═══════════════════════════════════════
    #  봇 빌드 & 실행
    # ═══════════════════════════════════════

    async def _on_startup(self, app: Application):
        """봇 시작 시 키보드 메시지 전송 + 한글 명령어 메뉴 등록"""
        logger.info("봇 초기화 완료 — 시작 메시지 전송")
        try:
            await app.bot.send_message(
                chat_id=int(self.chat_id),
                text="🔮 Body Hunter v3 봇 시작됨\n아래 버튼으로 명령하세요",
                reply_markup=MAIN_KEYBOARD,
            )
        except Exception as e:
            logger.error(f"시작 메시지 전송 실패: {e}")

    async def _fallback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """매칭 안 된 메시지 처리"""
        cid = update.effective_chat.id
        text = update.message.text if update.message else ""
        logger.info(f"[미매칭] chat_id={cid}, text='{text}'")
        await update.message.reply_text(
            "❓ 알 수 없는 명령입니다\n'도움' 버튼을 눌러주세요",
            reply_markup=MAIN_KEYBOARD,
        )

    def build_app(self) -> Application:
        app = Application.builder().token(self.token).build()

        # 시작 시 키보드 전송
        app.post_init = self._on_startup

        # /start — 인증 없이 (최초 접속용)
        app.add_handler(CommandHandler("start", self.cmd_start))

        # 한글 명령어 (정확히 일치) — 인증은 핸들러 내부에서 처리
        exact_commands = {
            r"^도움$": self.cmd_help,
            r"^상태$": self.cmd_status,
            r"^로그$": self.cmd_log,
            r"^스캔$": self.cmd_scan,
            r"^ETF$": self.cmd_etf_scan,
            r"^리포트$": self.cmd_report,
            r"^현재잔고$": self.cmd_balance,
            r"^체결내역$": self.cmd_executions,
            r"^포트폴리오$": self.cmd_portfolio,
            r"^청산$": self.cmd_liquidate,
            r"^일지$": self.cmd_journal,
            r"^유니버스$": self.cmd_universe,
            r"^유니버스갱신$": self.cmd_universe_rebuild,
            r"^시작$": self.cmd_auto_start,
            r"^정지$": self.cmd_auto_stop,
            r"^확인$": self.cmd_confirm,
        }

        for pattern, handler in exact_commands.items():
            app.add_handler(MessageHandler(filters.Regex(pattern), handler))

        # 인자 있는 명령어
        app.add_handler(
            MessageHandler(filters.Regex(r"^분석\s+.+"), self.cmd_analyze)
        )
        app.add_handler(
            MessageHandler(filters.Regex(r"^매수\s+.+"), self.cmd_buy)
        )
        app.add_handler(
            MessageHandler(filters.Regex(r"^매도\s+.+"), self.cmd_sell)
        )
        app.add_handler(
            MessageHandler(filters.Regex(r"^일지\s+.+"), self.cmd_journal)
        )

        # 인자 없는 "분석" → 안내
        app.add_handler(
            MessageHandler(
                filters.Regex(r"^분석$"),
                lambda u, c: u.message.reply_text("사용법: 분석 삼성전자"),
            )
        )

        # catch-all: 매칭 안 된 모든 텍스트
        app.add_handler(
            MessageHandler(filters.TEXT & ~filters.COMMAND, self._fallback)
        )

        # 자동매매 스케줄 등록
        self._setup_jobs(app)

        # 에러 핸들러
        app.add_error_handler(self._error_handler)

        return app

    def _setup_jobs(self, app: Application):
        """자동매매 JobQueue 스케줄 등록"""
        jq = app.job_queue
        if jq is None:
            logger.warning("JobQueue 사용 불가 — 자동매매 스케줄 등록 건너뜀")
            return

        bot_conf = self.config.get("bot", {})

        # 아침 스캔
        scan_time_str = bot_conf.get("morning_scan_time", "09:20")
        h, m = map(int, scan_time_str.split(":"))
        jq.run_daily(self.auto_trader.job_morning_scan, time=dtime(h, m))
        logger.info(f"아침 스캔 등록: {scan_time_str}")

        # 포지션 감시 (30초)
        interval = bot_conf.get("scan_interval_sec", 30)
        jq.run_repeating(self.auto_trader.job_monitor, interval=interval, first=10)
        logger.info(f"포지션 감시 등록: {interval}초")

        # 장마감 청산
        eod_str = bot_conf.get("eod_close_time", "15:10")
        h2, m2 = map(int, eod_str.split(":"))
        jq.run_daily(self.auto_trader.job_eod_close, time=dtime(h2, m2))
        logger.info(f"장마감 청산 등록: {eod_str}")

    async def _error_handler(self, update, context):
        import traceback
        tb = "".join(traceback.format_exception(type(context.error), context.error, context.error.__traceback__))
        logger.error(f"봇 에러: {context.error}\n{tb}")
        if update and update.effective_message:
            try:
                await update.effective_message.reply_text(
                    f"⚠️ 오류 발생: {str(context.error)[:200]}"
                )
            except Exception:
                pass

    def run(self):
        """봇 시작 (blocking)"""
        app = self.build_app()
        logger.info("텔레그램 봇 polling 시작...")
        app.run_polling(allowed_updates=Update.ALL_TYPES)
