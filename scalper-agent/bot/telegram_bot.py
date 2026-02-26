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
        ["스윙스캔", "이상거래", "스캔"],
        ["건전성", "이벤트", "워치리스트"],
        ["현재잔고", "체결내역", "포트폴리오"],
        ["시작", "정지", "상태"],
        ["유니버스", "시나리오", "시그널"],
        ["일지", "로그", "도움"],
        ["청산"],
    ],
    resize_keyboard=True,
)
# 참고: "분석 종목명", "뉴스 종목명", "스윙 종목명"은 키보드 없이 텍스트 입력

HELP_TEXT = """
🔮 Body Hunter v3 명령어

[스윙매매]
  스윙스캔 — 4층 파이프라인 (수급+기술+이상거래→TOP10)
  이상거래 — 이상거래 감지기 (조용한 매집/큰손 포착)
  건전성 — 시장 수급 건전성 진단
  이벤트 — DART+뉴스 이벤트 감지
  스윙 삼성전자 — 개별 종목 스윙 분석
  워치리스트 — 최근 스윙 워치리스트

[분석]
  스캔 — 5D 전종목 수급 스캔
  분석 삼성전자 — 개별 종목 6D 분석
  뉴스 삼성전자 — 뉴스 + Grok 감성분석

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

[시그널]
  시그널 — 일간 1D~4D 시그널 요약
  (자동 16:30 — 전종목 시그널 기록)

[시나리오]
  시나리오 — 매크로 테마 시나리오 목록
  시나리오활성 ID — 테마 ACTIVE 전환
  시나리오대기 ID — 테마 WATCH 전환
  시나리오삭제 ID — 테마 삭제

[데이터]
  분봉수집 — 당일 5분/15분봉 수집 (자동 15:40)
  유니버스 — 유니버스 종목 현황
  유니버스갱신 — 시총 1000억+ 리빌드

[시스템]
  상태 — 봇 상태
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

        await update.message.reply_text(f"🔍 {name}({code}) 6D+뉴스 분석중...")

        def _run():
            from data.supply_analyzer import SupplyAnalyzer
            analyzer = SupplyAnalyzer()
            f = analyzer.analyze_full(code, with_news=True, name=name)
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
            f"🔮 {name} ({code}) 6D 분석",
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

        # 6D 기술건강도
        th = full.tech_health
        if th:
            lines.append(f"6D: {th.tech_grade}({th.tech_score:.0f}점)")
            lines.append(f"  MA: {th.ma_status} | RSI: {th.rsi_value:.0f}({th.rsi_zone})")
            lines.append(f"  MACD: {th.macd_score:.0f}점 | BB: {th.bb_position:.0%}")
            lines.append(f"  거래량: {th.vol_ratio:.1f}x | 교차: {th.cross_score:.0f}점")

        # PER/PBR 밸류에이션
        if full.per > 0 or full.pbr > 0:
            per_str = f"{full.per:.1f}" if full.per > 0 else "적자"
            lines.append(f"PER: {per_str} | PBR: {full.pbr:.2f}")
        if full.valuation_warning:
            lines.append(f"⚠️ 밸류: {full.valuation_warning}")

        # 뉴스 감성분석
        if full.news_score != 0 or full.news_summary:
            emoji = "📈" if full.news_score > 0 else ("📉" if full.news_score < 0 else "📊")
            lines.append(f"\n{emoji} 뉴스: {full.news_score:+.0f}점")
            if full.news_summary:
                lines.append(f"  {full.news_summary}")

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
                f"기준: 시총 1000억 이상\n"
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
                f"시총 1000억 이상 필터 적용"
            )
        except Exception as e:
            logger.error(f"유니버스 갱신 에러: {e}")
            await update.message.reply_text(f"유니버스 갱신 실패: {e}")

    # ═══════════════════════════════════════
    #  분봉 수집
    # ═══════════════════════════════════════

    async def cmd_collect_minutes(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """수동 분봉(5분/15분) 수집"""
        if not self._is_authorized(update):
            return
        await update.message.reply_text("📊 분봉 수집 시작... (전종목 5분+15분, 10~15분 소요)")

        try:
            from data.kis_collector import collect_today_minutes, UNIVERSE
            results = await asyncio.to_thread(collect_today_minutes)

            total = len(UNIVERSE)
            ok = len(results)
            fail = total - ok

            lines = [
                f"✅ 분봉 수집 완료",
                f"   {ok}/{total}종목 성공 ({fail}실패)",
                "",
            ]
            if results:
                # 상위 5개 샘플
                for code, st in list(results.items())[:5]:
                    name = UNIVERSE.get(code, (code,))[0]
                    lines.append(f"  {name}: 5분={st['5min']}봉 15분={st['15min']}봉")
                if ok > 5:
                    lines.append(f"  ... 외 {ok - 5}종목")

            await update.message.reply_text("\n".join(lines))

        except Exception as e:
            logger.error(f"분봉 수집 에러: {e}")
            await update.message.reply_text(f"⚠️ 분봉 수집 실패: {str(e)[:200]}")

    # ═══════════════════════════════════════
    #  시그널
    # ═══════════════════════════════════════

    async def cmd_signal_summary(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """일간 시그널 요약"""
        if not self._is_authorized(update):
            return
        await update.message.reply_text("📋 시그널 요약 조회 중...")

        try:
            from data.signal_analyzer import SignalAnalyzer
            sa = SignalAnalyzer()
            summary = sa.format_daily_summary()

            for chunk in _split_message(summary):
                await update.message.reply_text(chunk)

        except Exception as e:
            logger.error(f"시그널 요약 에러: {e}")
            await update.message.reply_text(f"⚠️ 시그널 요약 실패: {str(e)[:200]}")

    # ═══════════════════════════════════════
    #  뉴스
    # ═══════════════════════════════════════

    async def cmd_news(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """종목 뉴스 + Grok 감성분석"""
        if not self._is_authorized(update):
            return
        text = update.message.text.strip()
        parts = text.split()

        if len(parts) < 2:
            await update.message.reply_text("사용법: 뉴스 삼성전자\n또는: 뉴스 005930")
            return

        query = parts[1]
        code, name = resolve_stock(query)
        if code is None:
            await update.message.reply_text(f"종목을 찾을 수 없습니다: {query}")
            return

        await update.message.reply_text(f"📰 {name}({code}) 뉴스 수집중...")

        def _run():
            from data.news_collector import NewsCollector
            nc = NewsCollector()
            return nc.get_news_score(code, name, use_grok=True)

        result = await asyncio.to_thread(_run)

        headlines = result.get("headlines", [])
        score = result.get("score", 0)
        summary = result.get("summary", "")
        sentiment = result.get("sentiment", "neutral")
        key_factor = result.get("key_factor", "")

        emoji = "📈" if score > 0 else ("📉" if score < 0 else "📊")
        sent_kr = {"positive": "긍정", "negative": "부정", "neutral": "중립"}.get(sentiment, "중립")

        lines = [
            f"📰 {name}({code}) 뉴스분석",
            "━" * 25,
            f"{emoji} 감성점수: {score:+d}점 ({sent_kr})",
        ]
        if summary:
            lines.append(f"요약: {summary}")
        if key_factor:
            lines.append(f"핵심: {key_factor}")

        lines.append(f"\n[최신 헤드라인]")
        if headlines:
            for h in headlines[:5]:
                lines.append(f"  · {h['title']}")
        else:
            lines.append("  뉴스 없음")

        await update.message.reply_text("\n".join(lines))

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
    #  스윙매매 명령
    # ═══════════════════════════════════════

    async def cmd_swing_scan(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """스윙 4층 파이프라인 스캔"""
        if not self._is_authorized(update):
            return
        await update.message.reply_text("📊 스윙 4층 파이프라인 실행중... (2~5분 소요)")

        try:
            from tools.swing_scan import run_pipeline, format_report
            ranked = await asyncio.to_thread(run_pipeline, 10)
            if ranked:
                report = format_report(ranked)
                for chunk in _split_message(report):
                    await update.message.reply_text(chunk)
            else:
                await update.message.reply_text("스윙 스캔 결과 없음 (통과 종목 0개)")
        except Exception as e:
            logger.error(f"스윙스캔 실패: {e}", exc_info=True)
            await update.message.reply_text(f"❌ 스윙스캔 실패: {e}")

    async def cmd_volume_scan(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """이상거래 감지기"""
        if not self._is_authorized(update):
            return
        await update.message.reply_text("🔍 이상거래 감지중... (1~2분 소요)")

        try:
            from data.volume_scanner import scan_universe, save_results, format_results
            results = await asyncio.to_thread(scan_universe, 20)
            if results:
                await asyncio.to_thread(save_results, results)
                report = format_results(results)
                for chunk in _split_message(report):
                    await update.message.reply_text(chunk)
            else:
                await update.message.reply_text("이상거래 감지 없음")
        except Exception as e:
            logger.error(f"이상거래 감지 실패: {e}", exc_info=True)
            await update.message.reply_text(f"❌ 이상거래 감지 실패: {e}")

    async def cmd_swing_analyze(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """개별 종목 스윙 분석 (예: '스윙 삼성전자')"""
        if not self._is_authorized(update):
            return
        text = update.message.text.strip()
        parts = text.split(maxsplit=1)
        if len(parts) < 2:
            await update.message.reply_text("사용법: 스윙 삼성전자")
            return

        query = parts[1].strip()
        code, name = resolve_stock(query)
        if not code:
            await update.message.reply_text(f"'{query}' 종목을 찾을 수 없습니다")
            return

        await update.message.reply_text(f"📊 {name}({code}) 스윙 분석중...")

        try:
            from tools.swing_scan import analyze_single
            cand = await asyncio.to_thread(analyze_single, code)
            if not cand:
                await update.message.reply_text(f"데이터 부족 — 일봉 수집 필요")
                return

            lines = []
            lines.append(f"📊 {cand.name}({cand.code}) 스윙 분석")
            lines.append(f"━━━━━━━━━━━━━━━━━━━")
            lines.append(f"최종: {cand.final_score:.0f}점 [{cand.source}]")
            lines.append(f"")
            lines.append(f"[수급 5D]")
            lines.append(f"  등급: {cand.supply_grade}({cand.supply_score:.0f}) | 4D: {cand.momentum_signal}({cand.momentum_score:.0f})")
            lines.append(f"  에너지: {cand.energy_grade}({cand.energy_score:.0f}) | 판정: {cand.action}")
            lines.append(f"")
            lines.append(f"[기술]")
            lines.append(f"  시그널: {cand.tech_signal}({cand.tech_score:.0f})")
            lines.append(f"  추세: {cand.ema_trend} | RSI: {cand.rsi:.0f} | OBV: {cand.obv_trend}")
            if cand.hist_direction:
                lines.append(f"  히스토그램: {cand.hist_direction} ({cand.hist_strength})")
            lines.append(f"")
            lines.append(f"[매매 레벨]")
            lines.append(f"  종가: {cand.close:,.0f}원 | ATR: {cand.atr_14:,.0f}원")
            lines.append(f"  SL: {cand.swing_sl:,.0f}원({cand.risk_pct:.1f}%) | TP: {cand.swing_tp:,.0f}원")
            if cand.spike_patterns:
                lines.append(f"")
                lines.append(f"[이상거래] {', '.join(cand.spike_patterns)} ({cand.spike_score:.0f}점)")
            if cand.per > 0:
                lines.append(f"")
                lines.append(f"PER: {cand.per:.1f} | PBR: {cand.pbr:.2f}")

            await update.message.reply_text("\n".join(lines))
        except Exception as e:
            logger.error(f"스윙 분석 실패: {e}", exc_info=True)
            await update.message.reply_text(f"❌ 스윙 분석 실패: {e}")

    async def cmd_watchlist(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """워치리스트 조회"""
        if not self._is_authorized(update):
            return
        import json
        wl_path = Path(__file__).resolve().parent.parent / "data_store" / "watchlist.json"
        if not wl_path.exists():
            await update.message.reply_text("워치리스트 없음 — '스윙스캔' 먼저 실행")
            return

        with open(wl_path, "r", encoding="utf-8") as f:
            wl = json.load(f)

        if not wl:
            await update.message.reply_text("워치리스트 비어있음")
            return

        lines = ["📋 스윙 워치리스트"]
        lines.append(f"📅 {wl[0].get('scanned_at', '')}")
        lines.append(f"━━━━━━━━━━━━━━━━━━━")

        for i, w in enumerate(wl, 1):
            lines.append(f"{i}. {w['name']}({w['code']}) — {w['final_score']:.0f}점")
            lines.append(f"   {w['supply_grade']}/{w['momentum']} | {w['tech_signal']} | {w['ema_trend']}")
            if w.get('swing_sl'):
                lines.append(f"   SL:{w['swing_sl']:,.0f} → TP:{w['swing_tp']:,.0f}")

        await update.message.reply_text("\n".join(lines))

    async def cmd_event_scan(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """이벤트 감지기 (DART + 뉴스 테마)"""
        if not self._is_authorized(update):
            return
        await update.message.reply_text("🛰 이벤트 감지중... (1~2분 소요)")

        try:
            from data.event_detector import run_event_scan, format_event_report
            result = await asyncio.to_thread(run_event_scan)
            if result["beneficiaries"]:
                report = format_event_report(result)
                for chunk in _split_message(report):
                    await update.message.reply_text(chunk)
            else:
                await update.message.reply_text("이벤트 감지 없음")
        except Exception as e:
            logger.error(f"이벤트 감지 실패: {e}", exc_info=True)
            await update.message.reply_text(f"❌ 이벤트 감지 실패: {e}")

    # ═══════════════════════════════════════
    #  시나리오 (매크로 테마)
    # ═══════════════════════════════════════

    async def cmd_scenario_list(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """매크로 테마 시나리오 목록"""
        if not self._is_authorized(update):
            return
        try:
            from data.event_detector import get_macro_themes
            themes = get_macro_themes()
            if not themes:
                await update.message.reply_text("시나리오 없음\nmacro_themes.json 미생성")
                return

            lines = ["📋 매크로 테마 시나리오", "━━━━━━━━━━━━━━━━━━━"]
            status_icon = {"ACTIVE": "🟢", "WATCH": "🟡", "ARCHIVE": "⚫"}
            for t in themes:
                icon = status_icon.get(t["status"], "⚪")
                direction = {"POSITIVE": "↑", "NEGATIVE": "↓", "NEUTRAL": "→"}.get(t.get("direction", ""), "?")
                bens = t.get("beneficiaries", [])
                ben_names = ", ".join(b["name"] for b in bens[:3])
                lines.append(f"\n{icon} {t['name']} ({t['status']})")
                lines.append(f"  ID: {t['id']}")
                lines.append(f"  {direction} impact:{t.get('impact',0)} | 키워드: {len(t.get('keywords',[]))}개")
                if ben_names:
                    lines.append(f"  수혜주: {ben_names}")
            lines.append("\n━━━━━━━━━━━━━━━━━━━")
            lines.append("시나리오활성/시나리오대기/시나리오삭제 + ID")
            await update.message.reply_text("\n".join(lines))
        except Exception as e:
            await update.message.reply_text(f"❌ 시나리오 조회 실패: {e}")

    async def cmd_scenario_activate(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """시나리오 ACTIVE 전환"""
        if not self._is_authorized(update):
            return
        theme_id = update.message.text.replace("시나리오활성", "").strip()
        if not theme_id:
            await update.message.reply_text("사용법: 시나리오활성 theme_id")
            return
        from data.event_detector import update_macro_theme_status
        if update_macro_theme_status(theme_id, "ACTIVE"):
            await update.message.reply_text(f"🟢 {theme_id} → ACTIVE 전환 완료")
        else:
            await update.message.reply_text(f"❌ ID '{theme_id}' 찾을 수 없음")

    async def cmd_scenario_watch(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """시나리오 WATCH 전환"""
        if not self._is_authorized(update):
            return
        theme_id = update.message.text.replace("시나리오대기", "").strip()
        if not theme_id:
            await update.message.reply_text("사용법: 시나리오대기 theme_id")
            return
        from data.event_detector import update_macro_theme_status
        if update_macro_theme_status(theme_id, "WATCH"):
            await update.message.reply_text(f"🟡 {theme_id} → WATCH 전환 완료")
        else:
            await update.message.reply_text(f"❌ ID '{theme_id}' 찾을 수 없음")

    async def cmd_scenario_delete(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """시나리오 삭제"""
        if not self._is_authorized(update):
            return
        theme_id = update.message.text.replace("시나리오삭제", "").strip()
        if not theme_id:
            await update.message.reply_text("사용법: 시나리오삭제 theme_id")
            return
        from data.event_detector import remove_macro_theme
        if remove_macro_theme(theme_id):
            await update.message.reply_text(f"🗑 {theme_id} 삭제 완료")
        else:
            await update.message.reply_text(f"❌ ID '{theme_id}' 찾을 수 없음")

    async def cmd_market_health(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """시장 수급 건전성 진단"""
        if not self._is_authorized(update):
            return
        await update.message.reply_text("🛡 시장 건전성 진단중...")

        try:
            from data.market_health import diagnose, format_health_report
            report = await asyncio.to_thread(diagnose)
            msg = format_health_report(report)
            await update.message.reply_text(msg)
        except Exception as e:
            logger.error(f"건전성 진단 실패: {e}", exc_info=True)
            await update.message.reply_text(f"❌ 건전성 진단 실패: {e}")

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
            r"^분봉수집$": self.cmd_collect_minutes,
            r"^시그널$": self.cmd_signal_summary,
            r"^시작$": self.cmd_auto_start,
            r"^정지$": self.cmd_auto_stop,
            r"^확인$": self.cmd_confirm,
            r"^스윙스캔$": self.cmd_swing_scan,
            r"^이상거래$": self.cmd_volume_scan,
            r"^이벤트$": self.cmd_event_scan,
            r"^워치리스트$": self.cmd_watchlist,
            r"^건전성$": self.cmd_market_health,
            r"^시나리오$": self.cmd_scenario_list,
        }

        for pattern, handler in exact_commands.items():
            app.add_handler(MessageHandler(filters.Regex(pattern), handler))

        # 인자 있는 명령어
        app.add_handler(
            MessageHandler(filters.Regex(r"^스윙\s+.+"), self.cmd_swing_analyze)
        )
        app.add_handler(
            MessageHandler(filters.Regex(r"^분석\s+.+"), self.cmd_analyze)
        )
        app.add_handler(
            MessageHandler(filters.Regex(r"^뉴스\s+.+"), self.cmd_news)
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
        # 시나리오 인자 있는 명령어
        app.add_handler(
            MessageHandler(filters.Regex(r"^시나리오활성\s+.+"), self.cmd_scenario_activate)
        )
        app.add_handler(
            MessageHandler(filters.Regex(r"^시나리오대기\s+.+"), self.cmd_scenario_watch)
        )
        app.add_handler(
            MessageHandler(filters.Regex(r"^시나리오삭제\s+.+"), self.cmd_scenario_delete)
        )

        # 인자 없는 "분석" / "뉴스" → 안내
        app.add_handler(
            MessageHandler(
                filters.Regex(r"^분석$"),
                lambda u, c: u.message.reply_text("사용법: 분석 삼성전자"),
            )
        )
        app.add_handler(
            MessageHandler(
                filters.Regex(r"^뉴스$"),
                lambda u, c: u.message.reply_text("사용법: 뉴스 삼성전자"),
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

        # 장마감 후 분봉 수집 (15:40)
        minute_str = bot_conf.get("minute_collect_time", "15:40")
        h3, m3 = map(int, minute_str.split(":"))
        jq.run_daily(self._job_collect_minutes, time=dtime(h3, m3))
        logger.info(f"분봉 수집 등록: {minute_str}")

        # 일봉 + 수급 수집 (16:00)
        daily_str = bot_conf.get("daily_collect_time", "16:00")
        h4, m4 = map(int, daily_str.split(":"))
        jq.run_daily(self._job_collect_daily, time=dtime(h4, m4))
        logger.info(f"일봉 수집 등록: {daily_str}")

        # 체결 스냅샷 폴링 — 장 시작 시 자동 시작 (09:01)
        tick_enabled = self.config.get("schedule", {}).get(
            "tick_collect", {}
        ).get("enabled", True)
        if tick_enabled:
            jq.run_daily(self._job_start_tick_polling, time=dtime(9, 1))
            logger.info("체결 폴링 등록: 09:01 시작 (1분 간격, 장중)")

        # 유니버스 리빌드 (08:30)
        uni_str = bot_conf.get("universe_rebuild_time", "08:30")
        h5, m5 = map(int, uni_str.split(":"))
        jq.run_daily(self._job_rebuild_universe, time=dtime(h5, m5))
        logger.info(f"유니버스 리빌드 등록: {uni_str}")

        # 일간 시그널 기록 (16:30 — 일봉 수집 후)
        jq.run_daily(self._job_record_signals, time=dtime(16, 30))
        logger.info("일간 시그널 기록 등록: 16:30")

    async def _job_start_tick_polling(self, context):
        """장 시작 시 체결 스냅샷 폴링 시작 (백그라운드 스레드)"""
        from datetime import date
        if date.today().weekday() >= 5:
            return

        chat_id = os.getenv("TELEGRAM_CHAT_ID")
        logger.info("체결 폴링 시작 (09:01~15:30, 1분 간격)...")

        try:
            from data.tick_collector import TickCollector
            from data.kis_collector import UNIVERSE

            codes = list(UNIVERSE.keys())
            interval = self.config.get("schedule", {}).get(
                "tick_collect", {}
            ).get("interval_sec", 60)

            tc = TickCollector()
            await context.bot.send_message(
                chat_id=chat_id,
                text=f"📡 체결 폴링 시작: {len(codes)}종목 / {interval}초 간격",
            )

            # 블로킹 루프를 별도 스레드에서 실행
            cycles = await asyncio.to_thread(
                tc.run_market_hours, codes, interval
            )

            await context.bot.send_message(
                chat_id=chat_id,
                text=f"📡 체결 폴링 종료: {cycles}사이클 완료",
            )

        except Exception as e:
            logger.error(f"체결 폴링 에러: {e}")
            await context.bot.send_message(
                chat_id=chat_id,
                text=f"⚠️ 체결 폴링 에러: {str(e)[:200]}",
            )

    async def _job_collect_minutes(self, context):
        """장마감 후 자동 분봉(5분/15분) 수집"""
        from datetime import date
        if date.today().weekday() >= 5:  # 주말 스킵
            return
        logger.info("분봉 자동 수집 시작...")
        chat_id = os.getenv("TELEGRAM_CHAT_ID")

        try:
            from data.kis_collector import collect_today_minutes, UNIVERSE
            results = await asyncio.to_thread(collect_today_minutes)

            msg = (
                f"📊 분봉 수집 완료\n"
                f"  {len(results)}/{len(UNIVERSE)}종목 성공\n"
            )
            if results:
                sample = list(results.items())[:3]
                for code, st in sample:
                    name = UNIVERSE.get(code, (code,))[0]
                    msg += f"  {name}: 5분={st['5min']}봉 15분={st['15min']}봉\n"

            await context.bot.send_message(chat_id=chat_id, text=msg)
            logger.info(f"분봉 수집 완료: {len(results)}종목")

        except Exception as e:
            logger.error(f"분봉 수집 실패: {e}")
            await context.bot.send_message(
                chat_id=chat_id, text=f"⚠️ 분봉 수집 실패: {str(e)[:200]}"
            )

    async def _job_collect_daily(self, context):
        """장마감 후 일봉(KIS) + 수급(pykrx) 데이터 수집"""
        from datetime import date
        if date.today().weekday() >= 5:
            return
        logger.info("일봉+수급 자동 수집 시작...")
        chat_id = os.getenv("TELEGRAM_CHAT_ID")

        # 1. 일봉 (KIS API)
        try:
            from data.kis_collector import collect_daily_kis, UNIVERSE

            results = await asyncio.to_thread(
                collect_daily_kis, list(UNIVERSE.keys()), 24, False
            )

            msg = f"📈 일봉 수집 완료: {len(results)}/{len(UNIVERSE)}종목"
            await context.bot.send_message(chat_id=chat_id, text=msg)
            logger.info(f"일봉 수집 완료: {len(results)}종목")

        except Exception as e:
            logger.error(f"일봉 수집 실패: {e}")
            await context.bot.send_message(
                chat_id=chat_id, text=f"⚠️ 일봉 수집 실패: {str(e)[:200]}"
            )

        # 2. 수급 데이터 (pykrx — 투자자순매수, 외인소진율, 공매도)
        try:
            from data.kis_collector import UNIVERSE
            from data.flow_collector import (
                collect_investor_flow, collect_foreign_exhaustion,
                collect_short_balance, collect_short_volume,
            )
            codes = list(UNIVERSE.keys())

            await context.bot.send_message(
                chat_id=chat_id, text=f"📊 수급 데이터 수집 시작: {len(codes)}종목"
            )

            await asyncio.to_thread(collect_investor_flow, codes, 24, False)
            await asyncio.to_thread(collect_foreign_exhaustion, codes, 24, False)
            await asyncio.to_thread(collect_short_balance, codes, 24, False)
            await asyncio.to_thread(collect_short_volume, codes, 24, False)

            await context.bot.send_message(
                chat_id=chat_id, text="📊 수급 데이터 수집 완료 (투자자+외인+공매도)"
            )
            logger.info("수급 수집 완료")

        except Exception as e:
            logger.error(f"수급 수집 실패: {e}")
            await context.bot.send_message(
                chat_id=chat_id, text=f"⚠️ 수급 수집 실패: {str(e)[:200]}"
            )

    async def _job_rebuild_universe(self, context):
        """장전 유니버스 리빌드 (시총 변동 반영)"""
        from datetime import date
        # 평일만 실행
        if date.today().weekday() >= 5:
            return

        logger.info("유니버스 자동 리빌드 시작...")
        chat_id = os.getenv("TELEGRAM_CHAT_ID")

        try:
            from data.universe_builder import build_universe
            uni = await asyncio.to_thread(build_universe)

            kospi = sum(1 for v in uni.values() if v.get("market") == "KOSPI")
            kosdaq = len(uni) - kospi

            msg = (
                f"🔄 유니버스 리빌드 완료\n"
                f"  총 {len(uni)}종목 (KOSPI {kospi} + KOSDAQ {kosdaq})"
            )
            await context.bot.send_message(chat_id=chat_id, text=msg)
            logger.info(f"유니버스 리빌드 완료: {len(uni)}종목")

        except Exception as e:
            logger.error(f"유니버스 리빌드 실패: {e}")
            await context.bot.send_message(
                chat_id=chat_id, text=f"⚠️ 유니버스 리빌드 실패: {str(e)[:200]}"
            )

    async def _job_record_signals(self, context):
        """일간 1D~4D 시그널 기록 (16:30 — 일봉 수집 완료 후)"""
        from datetime import date
        if date.today().weekday() >= 5:
            return

        logger.info("일간 시그널 기록 시작...")
        chat_id = os.getenv("TELEGRAM_CHAT_ID")

        try:
            from data.signal_analyzer import SignalAnalyzer
            from data.kis_collector import UNIVERSE

            exclude = {"069500", "371160", "102780", "305720"}
            codes = [c for c in UNIVERSE.keys() if c not in exclude]
            names = {c: UNIVERSE[c][0] for c in codes if c in UNIVERSE}

            sa = SignalAnalyzer()
            count = await asyncio.to_thread(sa.record_daily, codes, names)

            summary = sa.format_daily_summary()
            msg = f"📋 일간 시그널 기록 완료: {count}종목\n\n{summary}"

            for chunk in _split_message(msg):
                await context.bot.send_message(chat_id=chat_id, text=chunk)

            logger.info(f"일간 시그널 기록 완료: {count}종목")

        except Exception as e:
            logger.error(f"시그널 기록 실패: {e}")
            await context.bot.send_message(
                chat_id=chat_id, text=f"⚠️ 시그널 기록 실패: {str(e)[:200]}"
            )

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
