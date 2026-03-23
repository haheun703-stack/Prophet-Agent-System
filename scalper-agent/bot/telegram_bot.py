# -*- coding: utf-8 -*-
"""
Body Hunter v5 텔레그램 봇
===========================
한글 버튼 명령어로 5D 분석 + KIS 실매매 제어

모든 명령어가 한글:
  스캔, 리포트, 분석, 현재잔고, 체결내역, 매수, 매도, 청산, 시작, 정지, 상태 ...
"""

import os
import sys
import time
import asyncio
import logging
from pathlib import Path
from datetime import datetime, time as dtime, timezone, timedelta

# 한국 표준시 (UTC+9) - 스케줄러에 반드시 전달
KST = timezone(timedelta(hours=9))

from telegram import Update, ReplyKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, MessageHandler,
    ContextTypes, filters,
)

from bot.kis_trader import KISTrader, resolve_stock, CODE_TO_NAME
from bot.auto_trader import AutoTrader
from bot.bot_logger import log_event, read_today_log, cleanup_old_logs
from data.trading_calendar import is_trading_day, next_trading_day

logger = logging.getLogger("BH.Bot")

# APScheduler 로그 스팸 억제 (장외시간 30초마다 "executed successfully" 제거)
logging.getLogger("apscheduler.executors.default").setLevel(logging.WARNING)
logging.getLogger("apscheduler.scheduler").setLevel(logging.WARNING)

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
        ["추천", "내일", "포트"],
        ["뇌", "상태", "도움"],
    ],
    resize_keyboard=True,
)
# 참고: "분석 종목명", "뉴스 종목명", "스윙 종목명"은 키보드 없이 텍스트 입력

HELP_TEXT = """
Body Hunter v5 명령어

📋 매일 사용
  추천       — 봇 추천 종목 보기
  내일       — 내일 매수 등록 현황
  내일 [종목...]  — 매수 종목 등록
  내일취소    — 매수 등록 초기화
  포트 (ㅍ)   — 포트폴리오 현황
  뇌 (ㅂ)     — BRAIN 상태
  감시 (ㄱ)   — 진입 감시 종목
  페이퍼 (ㅌ)  — PAPER 현황
  이벤트로그 (ㄹ) — 오늘 봇 활동 로그

⚙️ 시스템
  시작/정지  — 자동매매 ON/OFF
  상태      — 봇 상태 + 리스크
  데이터     — 데이터 수집 현황
  위기모드/위기해제 — 위기 모드

💰 매매
  매수 삼성전자 10 — 시장가 매수
  매도 삼성전자 — 전량 매도
  청산 — 전종목 청산
  자동확인/자동취소 — 대기 매수 처리
  확인 — 분할매수 확인

📊 분석
  스캔/스윙스캔/MACD스캔
  사전감지/이상거래/건전성
  이벤트/해외이벤트/종목선정
  뉴스AI/AI모니터
  눈 (ㄴ)    — AI Eye 장중 현황

🔍 개별 종목
  분석 삼성전자 / 스윙 삼성전자
  뉴스 삼성전자 / 국적수급 SK하이닉스

📈 릴레이
  섹터릴레이/그룹릴레이/ETF릴레이
  릴레이종합/로테이션

🌐 JARVIS
  선행지표/스트레스/COT/유동성
  배분현황 — BRAIN 자본 배분

🌙 NXT 야간
  NXT/NXT켜기/NXT끄기/NXT실행
  선취매         — 시간외 선취매 현황

📓 시장 일지
  시장일지 (ㅇ)  — 오늘 시장 일기
  주간          — 주간 보고
  월간          — 월간 보고 + 조정 제안
  적용 1 2     — 제안 적용

📅 이벤트 캘린더
  캘린더         — 오늘 이벤트 리스크
  주간캘린더      — 이번 주 이벤트 브리핑
  DART공시       — DART 주총/주주제안 실시간

📁 기타
  현재잔고/체결내역/일지/시그널
  워치리스트/시나리오/유니버스/로그
""".strip()


class BodyHunterBot:
    """Body Hunter v5 텔레그램 봇"""

    def __init__(self, config: dict):
        self.config = config
        self.token = os.getenv("TELEGRAM_BOT_TOKEN")
        self.chat_id = os.getenv("TELEGRAM_CHAT_ID")
        self.trader = KISTrader(config)
        self.auto_trader = AutoTrader(config, self.trader)
        self.start_time = datetime.now(KST)
        self._pending_orders = {}  # chat_id → {type, code, qty, name}

        # ── 메시지 재설계: 하루 5~7개 ──
        # 각 job이 결과를 여기에 저장 → _send_morning_brief에서 1개로 합침
        self._morning_data = {}
        # 마감 리포트용 데이터 (장중 체결 기록 등)
        self._closing_data = {"trades_today": [], "paper_events": []}
        # 데이터 파이프라인 검증 결과
        self._data_verified = False
        self._verify_result = None

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
            "🔮 Body Hunter v5\n"
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
        now = datetime.now(KST)
        uptime = now - self.start_time.replace(tzinfo=KST) if self.start_time.tzinfo is None else now - self.start_time
        hours = int(uptime.total_seconds() // 3600)
        minutes = int((uptime.total_seconds() % 3600) // 60)

        market_open = now.replace(hour=9, minute=0, second=0)
        market_close = now.replace(hour=15, minute=20, second=0)
        is_market = market_open <= now <= market_close and is_trading_day(now.date())

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

        # 리스크 게이트 상태
        lines.append("━" * 25)
        lines.append(self.auto_trader.get_risk_status())

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
        await update.message.reply_text(f"📋 최근 로그\n{last_20}")

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

        try:
            report = await asyncio.to_thread(_run)
            for chunk in _split_message(report):
                await update.message.reply_text(chunk)
                await asyncio.sleep(0.5)
        except Exception as e:
            logger.error(f"ETF 스캔 실패: {e}", exc_info=True)
            await update.message.reply_text(f"⚠️ ETF 스캔 실패: {str(e)[:200]}")

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

        # 기준선 (SL/TP)
        if full.baseline:
            b = full.baseline
            ic = f" 기관원가:{b.inst_cost:,.0f}" if b.inst_cost > 0 else ""
            lines.append(f"\n📍 기준선 ({b.invalidation_source} 기반)")
            lines.append(f"  SL: {b.invalidation:,.0f}원 | TP1: {b.target_1:,.0f}원 | TP2: {b.target_2:,.0f}원")
            lines.append(f"  리스크: {b.risk_per_share:,.0f}원/주 ({b.atr_pct:.1f}% ATR){ic}")

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

        total = bal["total_eval"] or max(bal.get("cash", 1), 1)
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

    # ═══════════════════════════════════════
    #  보유종목 대시보드 (타 계좌)
    # ═══════════════════════════════════════

    async def cmd_dashboard(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """보유종목 상태판 (수동 호출)"""
        if not self._is_authorized(update):
            return
        await update.message.reply_text("\U0001f4cb 상태판 생성 중...")
        try:
            from bot.portfolio_monitor import generate_dashboard
            text = await asyncio.to_thread(generate_dashboard, self.trader)
            await update.message.reply_text(text)
        except Exception as e:
            logger.error(f"대시보드 생성 실패: {e}")
            await update.message.reply_text(f"\u274c 대시보드 실패: {str(e)[:200]}")

    async def _job_portfolio_dashboard(self, context):
        """보유종목 대시보드 (10:00/13:00/14:30) — Silent: 제거 (명령어로 대체)"""
        # 대시보드는 /포트 명령어로 온디맨드 조회
        pass

    async def _job_portfolio_alert(self, context):
        """보유종목 긴급 알림 (60초) — 급락(-5%) 시에만 전송"""
        now = datetime.now(KST)
        if not is_trading_day(now.date()):
            return
        now_min = now.hour * 60 + now.minute
        if now_min < 540 or now_min >= 930:
            return

        try:
            from bot.portfolio_monitor import check_alerts

            prev = context.bot_data.get("portfolio_alert_states", {})
            alerts, new_states = await asyncio.to_thread(
                check_alerts, self.trader, None, prev
            )
            context.bot_data["portfolio_alert_states"] = new_states

            if alerts:
                # 모든 알림은 log_event에 기록
                for a in alerts:
                    log_event("PORTFOLIO", f"{a.get('name', '')} {a.get('pnl_pct', 0):+.1f}%", a)

                # 긴급 알림: -5% 이상 급락만 텔레그램 전송
                urgent = [a for a in alerts if a.get("pnl_pct", 0) <= -5.0]
                if urgent:
                    chat_id = os.getenv("TELEGRAM_CHAT_ID")
                    lines = ["🚨 [긴급] 보유종목 급락"]
                    for a in urgent:
                        lines.append(
                            f"  {a.get('name', '')} {a.get('pnl_pct', 0):+.1f}%"
                            f" ({a.get('current_price', 0):,}원)"
                        )
                    await context.bot.send_message(
                        chat_id=chat_id, text="\n".join(lines)
                    )
                    logger.info(f"긴급 알림 발송: {len(urgent)}건 급락")
        except Exception as e:
            logger.error(f"보유종목 알림 체크 실패: {e}")

    # ── 전쟁모드 장중 추적 ──────────────────────────────

    def _load_war_targets(self):
        """recommendation.json에서 전쟁모드 추천 종목 로드"""
        import json
        rec_path = Path(__file__).resolve().parent.parent / "data_store" / "recommendation.json"
        if not rec_path.exists():
            return []
        try:
            with open(rec_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            return data.get("stocks", [])
        except Exception as e:
            logger.warning(f"recommendation.json 로드 실패: {e}")
            return []

    async def _job_war_startup(self, context):
        """09:00 전쟁모드 시작 — Silent: log_event만 (모닝브리프에 포함됨)"""
        from datetime import date as _date
        if not is_trading_day():
            return
        targets = self._load_war_targets()
        if not targets:
            logger.info("전쟁모드 종목 없음 — 추적 비활성")
            return
        names = [s.get("name", s.get("code")) for s in targets[:8]]
        log_event("WAR", f"전쟁모드 추적 시작 {len(targets[:8])}종목: {', '.join(names)}")
        logger.info(f"전쟁모드 시작: {len(targets[:8])}종목 (로그만)")

    async def _job_war_tracker(self, context):
        """전쟁모드 추적 — 60초 간격, 급등/급락/SL 알림"""
        from datetime import date as _date
        now = datetime.now(KST)
        if not is_trading_day():
            return
        now_min = now.hour * 60 + now.minute
        if now_min < 540 or now_min >= 935:  # 09:00~15:35 (15:35=935분)
            return

        targets = self._load_war_targets()
        if not targets:
            return

        chat_id = os.getenv("TELEGRAM_CHAT_ID")
        if not chat_id:
            return
        war_state = context.bot_data.setdefault("war_tracker", {})
        alert_sent = war_state.setdefault("alerts", {})
        open_prices = war_state.setdefault("opens", {})
        last_prices = war_state.setdefault("prices", {})

        ok_count = 0
        fail_count = 0

        for idx, s in enumerate(targets[:8]):
            code = s.get("code", "")
            name = s.get("name", code)
            sl = s.get("sl", 0)
            tp = s.get("tp", 0)

            # KIS API rate limit 방지: 종목 간 0.3초 딜레이
            if idx > 0:
                await asyncio.sleep(0.3)

            try:
                p = await asyncio.to_thread(self.trader.fetch_price, code)
                if not p or not p.get("success"):
                    fail_count += 1
                    msg = p.get("message", "") if p else "None"
                    logger.warning(f"전쟁추적 {name}({code}) 가격조회 실패: {msg}")
                    continue

                ok_count += 1
                cp = p["current_price"]
                last_prices[code] = cp

                # 시가: KIS API에서 가져오되, 없으면 현재가 fallback
                if code not in open_prices:
                    open_p_val = p.get("open", 0)
                    open_prices[code] = open_p_val if open_p_val > 0 else cp

                code_alerts = alert_sent.setdefault(code, [])
                open_p = open_prices.get(code, cp)

                # 1) SL 근접 (SL보다 위에 있지만 2% 이내) — log_event만
                if sl > 0 and cp > sl:
                    vs_sl = (cp / sl - 1) * 100
                    if vs_sl <= 2.0 and "SL_NEAR" not in code_alerts:
                        log_event("WAR", f"{name} SL근접 {cp:,} → SL {sl:,} ({vs_sl:+.1f}%)")
                        code_alerts.append("SL_NEAR")

                # 2) SL 이탈 — 긴급 텔레그램 전송
                if sl > 0 and cp <= sl and "SL_BREAK" not in code_alerts:
                    await context.bot.send_message(
                        chat_id=chat_id,
                        text=f"🚨 [긴급] {name} SL 이탈!\n  현재 {cp:,} ≤ SL {sl:,}\n  즉시 대응 필요"
                    )
                    log_event("WAR", f"{name} SL_BREAK {cp:,} ≤ {sl:,}")
                    code_alerts.append("SL_BREAK")

                # 3) 급등/급락 — log_event만 (급락 -5% 이상만 긴급 전송)
                if open_p > 0:
                    day_chg = (cp / open_p - 1) * 100
                    if day_chg >= 3.0 and "SURGE" not in code_alerts:
                        log_event("WAR", f"{name} 급등 {day_chg:+.1f}% ({cp:,}원)")
                        code_alerts.append("SURGE")
                    if day_chg <= -5.0 and "DROP5" not in code_alerts:
                        await context.bot.send_message(
                            chat_id=chat_id,
                            text=f"🚨 [긴급] {name} 급락 {day_chg:+.1f}%\n  현재 {cp:,}원"
                        )
                        log_event("WAR", f"{name} 급락 {day_chg:+.1f}% 긴급알림")
                        code_alerts.append("DROP5")
                    elif day_chg <= -3.0 and "DROP" not in code_alerts:
                        log_event("WAR", f"{name} 하락 {day_chg:+.1f}% ({cp:,}원)")
                        code_alerts.append("DROP")

                # 4) TP 도달 — log_event만
                if tp > 0 and cp >= tp and "TP_HIT" not in code_alerts:
                    log_event("WAR", f"{name} TP도달 {cp:,} ≥ {tp:,}")
                    code_alerts.append("TP_HIT")

            except Exception as e:
                fail_count += 1
                logger.warning(f"전쟁추적 {name} 예외: {e}")

        # 5분마다 로그 (매 60초는 과다)
        if now.minute % 5 == 0:
            logger.info(f"전쟁추적: {ok_count}/{ok_count+fail_count} 조회성공")

    async def _job_war_summary(self, context):
        """전쟁모드 30분 요약 — Silent: log_event만 (/ㅍ로 수동 조회)"""
        from datetime import date as _date
        if not is_trading_day():
            return

        targets = self._load_war_targets()
        if not targets:
            return

        war_state = context.bot_data.get("war_tracker", {})
        last_prices = war_state.get("prices", {})
        open_prices = war_state.get("opens", {})

        data_count = 0
        summary_items = []
        for idx, s in enumerate(targets[:8]):
            code = s.get("code", "")
            name = s.get("name", code)
            tp = s.get("tp", 0)
            cp = last_prices.get(code, 0)

            if cp == 0:
                if idx > 0:
                    await asyncio.sleep(0.3)
                try:
                    p = await asyncio.to_thread(self.trader.fetch_price, code)
                    if p and p.get("success"):
                        cp = p["current_price"]
                        last_prices[code] = cp
                        if code not in open_prices:
                            open_p_val = p.get("open", 0)
                            open_prices[code] = open_p_val if open_p_val > 0 else cp
                except Exception:
                    continue

            if cp == 0:
                continue

            data_count += 1
            open_p = open_prices.get(code, cp)
            day_chg = (cp / open_p - 1) * 100 if open_p > 0 else 0
            vs_tp = (tp / cp - 1) * 100 if tp > 0 and cp > 0 else 0
            summary_items.append(f"{name} {cp:,}원 ({day_chg:+.1f}%) →TP({vs_tp:+.1f}%)")

        log_event("WAR", f"30분 요약 {data_count}종목", summary_items)
        logger.info(f"전쟁추적 요약: {data_count}종목 (로그만)")

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
            # MSG-03/04: 체결/긴급만 텔레그램, 나머지 log_event
            _TG_KEYWORDS = ("✅ 자동 매수", "❌ 매수 실패", "매도 체결", "청산",
                            "리스크 게이트", "BRAIN 관망", "위기 모드", "🚫", "⛔",
                            "📉 트레일링", "손절")
            if any(kw in text for kw in _TG_KEYWORDS):
                # 이모지 가이드: 금지 이모지 → 기능 마커
                out = text.replace("📈", "").replace("📉", "")
                if "✅ 자동 매수" in out:
                    out = out.replace("✅ 자동 매수", "💰 매수 체결")
                elif "⛔ 손절" in out or "⛔ 동적 손절" in out:
                    out = out.replace("⛔ 손절", "🛑 [손절]").replace("⛔ 동적 손절", "🛑 [손절]")
                elif "청산" in out or "매도 체결" in out:
                    if not out.startswith("📤"):
                        out = "📤 " + out
                await context.bot.send_message(chat_id=int(self.chat_id), text=out)
            else:
                cat = "TRADE_BUY" if "매수" in text else "TRADE_SELL" if "매도" in text else "SYSTEM"
                log_event(cat, text[:200])

        self.auto_trader.start(_send_alert)
        bot_conf = self.config.get("bot", {})
        await update.message.reply_text(
            "🟢 자동매매 시작\n"
            "━" * 20 + "\n"
            f"아침 스캔: {bot_conf.get('morning_scan_time', '09:00')}\n"
            f"감시 주기: {bot_conf.get('scan_interval_sec', 30)}초\n"
            f"최대 보유: {bot_conf.get('max_auto_positions', 2)}종목\n"
            f"금액: 잔고 기반 동적 계산\n"
            f"분할: {self.config.get('risk', {}).get('split_count', 3)}회"
        )

    async def cmd_auto_stop(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self._is_authorized(update):
            return
        self.auto_trader.stop()
        await update.message.reply_text("🔴 자동매매 정지")

    async def cmd_auto_confirm(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """대기 중인 자동매수 확인 실행"""
        if not self._is_authorized(update):
            return
        pending = self.auto_trader._pending_auto_buys
        if not pending:
            await update.message.reply_text("대기 중인 자동매수가 없습니다")
            return

        await update.message.reply_text(f"🔄 {len(pending)}건 자동매수 실행 중...")
        results = await asyncio.to_thread(self.auto_trader.execute_pending_auto_buys)
        for r in results:
            if r["success"]:
                await update.message.reply_text(
                    f"✅ {r['name']}({r['code']}) 매수 완료\n"
                    f"   {r['message']}\n"
                    f"   SL:{r['sl']:,} TP:{r['tp']:,}"
                )
            else:
                await update.message.reply_text(
                    f"❌ {r['name']}({r['code']}) 매수 실패\n   {r['message']}"
                )

    async def cmd_auto_cancel(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """대기 중인 자동매수 전체 취소"""
        if not self._is_authorized(update):
            return
        count = self.auto_trader.cancel_pending_auto_buys()
        if count:
            await update.message.reply_text(f"🚫 자동매수 {count}건 취소됨")
        else:
            await update.message.reply_text("대기 중인 자동매수가 없습니다")

    async def cmd_crisis_on(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """위기 모드 활성화 - 모든 매수 차단"""
        if not self._is_authorized(update):
            return
        text = update.message.text.strip()
        reason = text.replace("위기모드", "").strip() or "수동 위기 모드 활성화"
        from data.market_health import set_crisis_mode
        set_crisis_mode(reason)
        # 자동매매도 정지
        self.auto_trader.stop()
        self.auto_trader.cancel_pending_auto_buys()
        await update.message.reply_text(
            f"🚨 위기 모드 활성화\n"
            f"━━━━━━━━━━━━━━━━━━━\n"
            f"사유: {reason}\n"
            f"자동매매: 정지됨\n"
            f"대기 주문: 전부 취소\n"
            f"매수: 완전 차단\n\n"
            f"해제: '위기해제' 입력"
        )

    async def cmd_crisis_off(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """위기 모드 해제"""
        if not self._is_authorized(update):
            return
        from data.market_health import clear_crisis_mode
        cleared = clear_crisis_mode()
        if cleared:
            await update.message.reply_text(
                "✅ 위기 모드 해제\n"
                "매수 차단 해제됨\n"
                "자동매매 재시작: '시작' 입력"
            )
        else:
            await update.message.reply_text("위기 모드가 활성화되어 있지 않습니다")

    # ═══════════════════════════════════════
    #  사전감지 + AI 모니터
    # ═══════════════════════════════════════

    async def cmd_premove_scan(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """사전감지 스캔 - 폭발 직전 종목 포착"""
        if not self._is_authorized(update):
            return
        await update.message.reply_text("사전감지 스캔 실행중... (3~5분 소요)")

        try:
            from data.premove_scanner import scan_premove, format_premove_report
            candidates = await asyncio.to_thread(scan_premove, 5)
            report = format_premove_report(candidates)
            for chunk in _split_message(report):
                await update.message.reply_text(chunk)
        except Exception as e:
            logger.error(f"사전감지 실패: {e}", exc_info=True)
            await update.message.reply_text(f"사전감지 실패: {e}")

    async def cmd_ai_monitor(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """AI 모니터 - 보유종목 실시간 4팩터 분석"""
        if not self._is_authorized(update):
            return

        try:
            from data.realtime_monitor import RealtimeMonitor
            rtm = self.auto_trader._get_rt_monitor()
            positions = rtm.get_positions()

            if not positions:
                await update.message.reply_text("AI 모니터: 등록된 포지션 없음")
                return

            await update.message.reply_text(f"AI 분석 중... ({len(positions)}종목)")
            snapshots = await asyncio.to_thread(rtm.evaluate_all)

            if snapshots:
                report = rtm.format_snapshot_report(snapshots)
                for chunk in _split_message(report):
                    await update.message.reply_text(chunk)
            else:
                await update.message.reply_text("AI 모니터: 스냅샷 수집 실패 (장외시간?)")
        except Exception as e:
            logger.error(f"AI 모니터 실패: {e}", exc_info=True)
            await update.message.reply_text(f"AI 모니터 실패: {e}")

    async def cmd_news_ai(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """뉴스 AI 분석 - 후보 종목 다중소스 뉴스 + Claude AI"""
        if not self._is_authorized(update):
            return

        # 인자가 있으면 특정 종목, 없으면 사전감지 후보
        args = context.args if context.args else []
        await update.message.reply_text("📰 뉴스 AI 분석 중... (1~2분 소요)")

        try:
            from data.news_ai_scanner import scan_news_ai, format_news_ai_report

            if args:
                # 특정 종목
                from bot.kis_trader import resolve_stock
                _code, _name = resolve_stock(args[0])
                if not _code:
                    await update.message.reply_text(f"종목 '{args[0]}' 찾을 수 없음")
                    return
                targets = [{"code": _code, "name": _name}]
            else:
                # 사전감지 후보에서 가져오기
                candidates_path = Path(__file__).parent.parent / "data_store" / "premove_candidates.json"
                if candidates_path.exists():
                    import json
                    with open(candidates_path, "r", encoding="utf-8") as f:
                        targets = json.load(f)[:5]
                else:
                    await update.message.reply_text("사전감지 후보 없음 - 먼저 '사전감지' 실행")
                    return

            results = await asyncio.to_thread(scan_news_ai, targets, True)
            report = format_news_ai_report(results)
            for chunk in _split_message(report):
                await update.message.reply_text(chunk)
        except Exception as e:
            logger.error(f"뉴스AI 실패: {e}", exc_info=True)
            await update.message.reply_text(f"뉴스AI 실패: {e}")

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
                await update.message.reply_text(f"데이터 부족 - 일봉 수집 필요")
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

    async def cmd_swing_watchlist(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """스윙 워치리스트 조회"""
        if not self._is_authorized(update):
            return
        import json
        wl_path = Path(__file__).resolve().parent.parent / "data_store" / "watchlist.json"
        if not wl_path.exists():
            await update.message.reply_text("워치리스트 없음 - '스윙스캔' 먼저 실행")
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
            lines.append(f"{i}. {w['name']}({w['code']}) - {w['final_score']:.0f}점")
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

    async def cmd_global_event(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """해외 이벤트 캘린더 (실적발표 + 경제지표 D-3)"""
        if not self._is_authorized(update):
            return
        await update.message.reply_text("🌍 해외 이벤트 스캔중... (30초~1분)")

        try:
            from data.global_event_calendar import scan_global_events, format_telegram_message
            result = await asyncio.to_thread(scan_global_events)
            msg = format_telegram_message(result)
            for chunk in _split_message(msg):
                await update.message.reply_text(chunk)
        except Exception as e:
            logger.error(f"해외 이벤트 실패: {e}", exc_info=True)
            await update.message.reply_text(f"❌ 해외 이벤트 실패: {e}")

    async def cmd_event_calendar(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """오늘 이벤트 캘린더 (정적 캘린더 기반)"""
        if not self._is_authorized(update):
            return
        try:
            from data.event_calendar import format_daily_telegram
            msg = format_daily_telegram()
            await update.message.reply_text(msg)
        except Exception as e:
            logger.error(f"이벤트 캘린더 실패: {e}", exc_info=True)
            await update.message.reply_text(f"❌ 이벤트 캘린더 실패: {e}")

    async def cmd_weekly_calendar(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """주간 이벤트 브리핑"""
        if not self._is_authorized(update):
            return
        try:
            from data.event_calendar import generate_weekly_briefing, format_weekly_telegram
            briefing = generate_weekly_briefing()
            msg = format_weekly_telegram(briefing)
            for chunk in _split_message(msg):
                await update.message.reply_text(chunk)
        except Exception as e:
            logger.error(f"주간 캘린더 실패: {e}", exc_info=True)
            await update.message.reply_text(f"❌ 주간 캘린더 실패: {e}")

    async def cmd_dart_governance(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """DART 거버넌스 공시 (주총/주주제안/배당/정관변경)"""
        if not self._is_authorized(update):
            return
        await update.message.reply_text("📋 DART 거버넌스 공시 크롤링... (10초)")
        try:
            from data.event_calendar import refresh_dart_events, format_dart_telegram
            events = await asyncio.to_thread(refresh_dart_events, 14)
            msg = format_dart_telegram(events)
            for chunk in _split_message(msg):
                await update.message.reply_text(chunk)
        except Exception as e:
            logger.error(f"DART 공시 실패: {e}", exc_info=True)
            await update.message.reply_text(f"❌ DART 공시 실패: {e}")

    async def cmd_policy_tracker(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """정책 트래커 (대통령실/정부 정책 → 수혜 섹터)"""
        if not self._is_authorized(update):
            return
        await update.message.reply_text("🏛 정책 수집중... (5~10초)")
        try:
            from data.policy_tracker import scan_policy, format_telegram_message
            # 캐시 있으면 캐시 사용, 없으면 새로 스캔
            result = await asyncio.to_thread(scan_policy)
            msg = format_telegram_message(result)
            for chunk in _split_message(msg):
                await update.message.reply_text(chunk)
        except Exception as e:
            logger.error(f"정책 트래커 실패: {e}", exc_info=True)
            await update.message.reply_text(f"❌ 정책 트래커 실패: {e}")

    async def cmd_flowx_vip(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """FLOWX VIP 8-Panel 콘텐츠 생성 + 업로드"""
        if not self._is_authorized(update):
            return
        await update.message.reply_text("📊 FLOWX VIP 8패널 생성중... (3~5초)")
        try:
            from data.flowx_content import generate_vip_content, format_telegram_vip, upload_vip_content
            content = await asyncio.to_thread(generate_vip_content)
            msg = format_telegram_vip(content)
            for chunk in _split_message(msg):
                await update.message.reply_text(chunk, parse_mode="HTML")
            # Supabase 업로드
            ok = await asyncio.to_thread(upload_vip_content, content)
            status = "Supabase 업로드 완료" if ok else "Supabase 미연결"
            await update.message.reply_text(f"✅ VIP 8패널 생성 완료 ({status})")
        except Exception as e:
            logger.error(f"FLOWX VIP 실패: {e}", exc_info=True)
            await update.message.reply_text(f"❌ FLOWX VIP 실패: {e}")

    async def cmd_swing_pick(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """스윙 종목 선정 (6팩터 스코어링 TOP N)"""
        if not self._is_authorized(update):
            return
        await update.message.reply_text("📊 스윙 종목 선정중... (1~2분)")

        try:
            from data.swing_picker import run_picker, format_telegram_message as fmt_swing
            result = await asyncio.to_thread(run_picker)
            msg = fmt_swing(result)
            for chunk in _split_message(msg):
                await update.message.reply_text(chunk)
        except Exception as e:
            logger.error(f"종목 선정 실패: {e}", exc_info=True)
            await update.message.reply_text(f"❌ 종목 선정 실패: {e}")

    async def cmd_macd_scan(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """MACD 제로선 크로스 + 수급폭발 스캔"""
        if not self._is_authorized(update):
            return
        await update.message.reply_text("MACD 제로선 크로스 스캔중... (1~2분)")

        try:
            from strategies.macd_zero_scanner import (
                run_daily_scan,
                format_telegram_message as fmt_macd,
            )
            result = await asyncio.to_thread(run_daily_scan)
            msg = fmt_macd(result)
            for chunk in _split_message(msg):
                await update.message.reply_text(chunk)
        except Exception as e:
            logger.error(f"MACD 스캔 실패: {e}", exc_info=True)
            await update.message.reply_text(f"MACD 스캔 실패: {e}")

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
    #  릴레이 에이전트
    # ═══════════════════════════════════════

    async def cmd_sector_relay(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """섹터 릴레이 스캔"""
        if not self._is_authorized(update):
            return
        await update.message.reply_text("📊 12섹터 릴레이 스캔중... (1~2분)")
        try:
            from data.sector_relay import scan_all_sectors, format_sector_report
            results = await asyncio.to_thread(scan_all_sectors)
            msg = format_sector_report(results)
            for chunk in _split_message(msg):
                await update.message.reply_text(chunk)
        except Exception as e:
            logger.error(f"섹터 릴레이 실패: {e}", exc_info=True)
            await update.message.reply_text(f"❌ 섹터 릴레이 실패: {e}")

    async def cmd_group_relay(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """그룹 릴레이 스캔"""
        if not self._is_authorized(update):
            return
        await update.message.reply_text("🏢 7대 그룹 릴레이 스캔중... (1~2분)")
        try:
            from data.group_relay import scan_all_groups, format_group_report
            results = await asyncio.to_thread(scan_all_groups)
            msg = format_group_report(results)
            for chunk in _split_message(msg):
                await update.message.reply_text(chunk)
        except Exception as e:
            logger.error(f"그룹 릴레이 실패: {e}", exc_info=True)
            await update.message.reply_text(f"❌ 그룹 릴레이 실패: {e}")

    async def cmd_etf_relay(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """ETF 순환 스캔"""
        if not self._is_authorized(update):
            return
        await update.message.reply_text("📈 ETF 순환 스캔중... (1~2분)")
        try:
            from data.etf_relay import scan_all_etfs, format_etf_report
            results = await asyncio.to_thread(scan_all_etfs)
            msg = format_etf_report(results)
            for chunk in _split_message(msg):
                await update.message.reply_text(chunk)
        except Exception as e:
            logger.error(f"ETF 릴레이 실패: {e}", exc_info=True)
            await update.message.reply_text(f"❌ ETF 릴레이 실패: {e}")

    async def cmd_relay_hub(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """릴레이 통합 스캔 (3개 에이전트 교차 검증)"""
        if not self._is_authorized(update):
            return
        await update.message.reply_text("🔄 릴레이 통합 스캔중... (3~5분)\n섹터+그룹+ETF 교차 검증")
        try:
            from data.relay_hub import scan_relay_all, format_relay_report
            report = await asyncio.to_thread(scan_relay_all)
            msg = format_relay_report(report)
            for chunk in _split_message(msg):
                await update.message.reply_text(chunk)
        except Exception as e:
            logger.error(f"릴레이 통합 실패: {e}", exc_info=True)
            await update.message.reply_text(f"❌ 릴레이 통합 실패: {e}")

    async def cmd_recommendation(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """내일 추천 종목 (최신 저장된 리포트 또는 즉시 실행)"""
        if not self._is_authorized(update):
            return
        await update.message.reply_text("📊 내일 추천 종목 조회 중...")
        try:
            from data.morning_recommendation import (
                load_recommendation, format_recommendation,
                run_evening_recommendation, save_recommendation,
            )
            report = load_recommendation()
            if report and report.stocks:
                # Brain 리포트 먼저 전송
                try:
                    from data.market_brain import load_brain_report, format_brain_telegram
                    brain = load_brain_report()
                    if brain:
                        brain_msg = format_brain_telegram(brain)
                        for chunk in _split_message(brain_msg):
                            await update.message.reply_text(chunk)
                except Exception as e:
                    logger.warning(f"Brain 리포트 전송 실패: {e}")
                # Trade Objects R:R 분석 전송
                try:
                    from data.trade_object import load_trade_objects, format_trade_objects_telegram
                    trade_objs = load_trade_objects()
                    if trade_objs:
                        to_msg = format_trade_objects_telegram(trade_objs, paper=True)
                        if to_msg:
                            for chunk in _split_message(to_msg):
                                await update.message.reply_text(chunk)
                except Exception as e:
                    logger.warning(f"Trade Objects 전송 실패: {e}")
                # 기존 상세 리포트 후속 전송
                msg = format_recommendation(report)
                for chunk in _split_message(msg):
                    await update.message.reply_text(chunk)
            else:
                await update.message.reply_text("저장된 추천 없음 - 5단계 분석 즉시 실행 중... (3~5분)")
                report = await asyncio.to_thread(run_evening_recommendation)
                save_recommendation(report)
                # Brain은 save_recommendation 내에서 자동 생성됨
                try:
                    from data.market_brain import load_brain_report, format_brain_telegram
                    brain = load_brain_report()
                    if brain:
                        brain_msg = format_brain_telegram(brain)
                        for chunk in _split_message(brain_msg):
                            await update.message.reply_text(chunk)
                except Exception:
                    pass
                # Trade Objects R:R 분석 전송
                try:
                    from data.trade_object import load_trade_objects, format_trade_objects_telegram
                    trade_objs = load_trade_objects()
                    if trade_objs:
                        to_msg = format_trade_objects_telegram(trade_objs, paper=True)
                        if to_msg:
                            for chunk in _split_message(to_msg):
                                await update.message.reply_text(chunk)
                except Exception:
                    pass
                msg = format_recommendation(report)
                for chunk in _split_message(msg):
                    await update.message.reply_text(chunk)
        except Exception as e:
            logger.error(f"추천 조회 실패: {e}", exc_info=True)
            await update.message.reply_text(f"❌ 추천 조회 실패: {e}")

    # ═══════════════════════════════════════
    #  PAPER 트레이딩 현황
    # ═══════════════════════════════════════

    async def cmd_paper(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """PAPER 트레이딩 현황 조회 + 2주 리뷰 통계"""
        if not self._is_authorized(update):
            return
        try:
            from data.trade_tracker import TradeTracker
            tracker = TradeTracker()
            msg = tracker.get_paper_dashboard(kis=self.trader)
            for chunk in _split_message(msg):
                await update.message.reply_text(chunk)
        except Exception as e:
            await update.message.reply_text(f"❌ PAPER 현황 조회 실패: {e}")

    # ═══════════════════════════════════════
    #  JARVIS BRAIN 자본 배분
    # ═══════════════════════════════════════

    async def cmd_brain(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """BRAIN 자본 배분 현황 조회"""
        if not self._is_authorized(update):
            return
        try:
            import sys
            scalper_dir = Path(__file__).resolve().parent.parent
            project_root = scalper_dir.parent
            if str(project_root) not in sys.path:
                sys.path.insert(0, str(project_root))

            brain_path = project_root / "jarvis" / "data" / "brain_allocation.json"
            if not brain_path.exists():
                await update.message.reply_text(
                    "BRAIN 배분 데이터 없음\n"
                    "NIGHTWATCH 실행 후 자동 생성됩니다"
                )
                return

            import json as _json
            with open(brain_path, "r", encoding="utf-8") as f:
                alloc = _json.load(f)

            from jarvis.brain import format_allocation_report
            msg = format_allocation_report(alloc)

            # 마지막 갱신 시간 표시
            ts = alloc.get("timestamp", "")
            msg += f"\n\n(갱신: {ts})"

            await update.message.reply_text(msg)

        except Exception as e:
            logger.error(f"BRAIN 조회 실패: {e}", exc_info=True)
            await update.message.reply_text(f"BRAIN 조회 실패: {str(e)[:200]}")

    async def cmd_rotation(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """섹터 로테이션 분석 조회"""
        if not self._is_authorized(update):
            return
        try:
            from data.rotation_detector import (
                analyze_rotation, format_rotation_report, get_next_sector_stocks
            )
            rotation = analyze_rotation()
            msg = format_rotation_report(rotation)

            # 다음 섹터 종목 요약 (상위 10개)
            next_stocks = get_next_sector_stocks(rotation)
            staging = {c: i for c, i in next_stocks.items()
                       if i["rotation_source"] in ("staging", "hot_early")}
            if staging:
                msg += "\n\n📌 다음 섹터 주목 종목"
                for code, info in list(staging.items())[:10]:
                    src = info["rotation_source"]
                    tier = info["tier"]
                    msg += f"\n  {info['name']}({code}) [{info['sector']}] {src}/{tier}"

            await update.message.reply_text(msg)
        except Exception as e:
            logger.error(f"로테이션 조회 실패: {e}", exc_info=True)
            await update.message.reply_text(f"로테이션 조회 실패: {str(e)[:200]}")

    async def cmd_leading(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """2D: 레짐 전환 선행지표 조회"""
        if not self._is_authorized(update):
            return
        try:
            import sys
            jarvis_path = str(Path(__file__).resolve().parent.parent.parent / "jarvis")
            if jarvis_path not in sys.path:
                sys.path.insert(0, jarvis_path)
            parent_path = str(Path(__file__).resolve().parent.parent.parent)
            if parent_path not in sys.path:
                sys.path.insert(0, parent_path)

            from jarvis.regime_leading import analyze_leading, format_leading_report
            await update.message.reply_text("선행지표 분석 중... (yfinance 6개 티커)")
            report = analyze_leading()
            msg = format_leading_report(report)
            await update.message.reply_text(msg)
        except Exception as e:
            logger.error(f"선행지표 조회 실패: {e}", exc_info=True)
            await update.message.reply_text(f"선행지표 조회 실패: {str(e)[:200]}")

    async def cmd_stress(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """3D: 크로스에셋 스트레스 지수 조회"""
        if not self._is_authorized(update):
            return
        try:
            import sys
            jarvis_path = str(Path(__file__).resolve().parent.parent.parent / "jarvis")
            if jarvis_path not in sys.path:
                sys.path.insert(0, jarvis_path)
            parent_path = str(Path(__file__).resolve().parent.parent.parent)
            if parent_path not in sys.path:
                sys.path.insert(0, parent_path)

            from jarvis.cross_asset_stress import analyze_stress, format_stress_report
            await update.message.reply_text("크로스에셋 스트레스 분석 중... (yfinance 5개 자산)")
            report = analyze_stress()
            msg = format_stress_report(report)
            await update.message.reply_text(msg)
        except Exception as e:
            logger.error(f"스트레스 조회 실패: {e}", exc_info=True)
            await update.message.reply_text(f"스트레스 조회 실패: {str(e)[:200]}")

    async def cmd_cot(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """4D: COT 스마트머니 포지션 조회"""
        if not self._is_authorized(update):
            return
        try:
            import sys
            parent_path = str(Path(__file__).resolve().parent.parent.parent)
            if parent_path not in sys.path:
                sys.path.insert(0, parent_path)

            from jarvis.cot_smartmoney import analyze_cot, format_cot_report
            await update.message.reply_text("COT 스마트머니 분석 중... (CFTC 데이터 다운로드)")
            report = analyze_cot()
            msg = format_cot_report(report)
            for chunk in _split_message(msg):
                await update.message.reply_text(chunk)
        except Exception as e:
            logger.error(f"COT 조회 실패: {e}", exc_info=True)
            await update.message.reply_text(f"COT 조회 실패: {str(e)[:200]}")

    async def cmd_liquidity(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """5D: 유동성 사이클 (FRED RRP/TGA/M2) 조회"""
        if not self._is_authorized(update):
            return
        try:
            import sys
            parent_path = str(Path(__file__).resolve().parent.parent.parent)
            if parent_path not in sys.path:
                sys.path.insert(0, parent_path)

            from jarvis.liquidity_cycle import analyze_liquidity, format_liquidity_report
            await update.message.reply_text("유동성 사이클 분석 중... (FRED 데이터 다운로드)")
            report = analyze_liquidity()
            msg = format_liquidity_report(report)
            for chunk in _split_message(msg):
                await update.message.reply_text(chunk)
        except Exception as e:
            logger.error(f"유동성 조회 실패: {e}", exc_info=True)
            await update.message.reply_text(f"유동성 조회 실패: {str(e)[:200]}")

    # ═══════════════════════════════════════
    #  NIGHTWATCH NXT 명령어
    # ═══════════════════════════════════════

    async def cmd_nxt(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """NIGHTWATCH 상태 + NXT 포지션 조회"""
        if not self._is_authorized(update):
            return
        try:
            from data.nightwatch import load_nightwatch_report, format_nightwatch_report

            report = load_nightwatch_report()
            if report:
                msg = format_nightwatch_report(report)
            else:
                msg = "NIGHTWATCH 리포트 없음 (아직 미실행)"

            # NXT 포지션
            self.auto_trader._load_nxt_positions()
            nxt_pos = getattr(self.auto_trader, '_nxt_positions', {})
            if nxt_pos:
                msg += "\n\nNXT 보유 포지션:"
                for code, pos in nxt_pos.items():
                    name = pos.get("name", code)
                    entry = pos.get("entry_price", 0)
                    score = pos.get("nw_score", 0)
                    msg += f"\n  {name}({code}) @{entry:,}원 | NW:{score:+.1f}"
            else:
                msg += "\n\nNXT 보유 포지션: 없음"

            nw_cfg = self.config.get("nightwatch", {})
            mode = "알림만" if nw_cfg.get("alert_only", True) else "자동매매"
            msg += f"\n\n모드: {mode}"

            for chunk in _split_message(msg):
                await update.message.reply_text(chunk)

        except Exception as e:
            await update.message.reply_text(f"NXT 조회 실패: {e}")

    async def cmd_nxt_on(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """NXT 자동매매 활성화 (alert_only → False)"""
        if not self._is_authorized(update):
            return
        self.config.setdefault("nightwatch", {})["alert_only"] = False
        await update.message.reply_text("NXT 자동매매 활성화됨\n(시간외 단일가 자동 주문)")

    async def cmd_nxt_off(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """NXT 자동매매 비활성화 (alert_only → True)"""
        if not self._is_authorized(update):
            return
        self.config.setdefault("nightwatch", {})["alert_only"] = True
        await update.message.reply_text("NXT 알림만 모드로 전환\n(자동 주문 중지, 알림은 유지)")

    async def cmd_nxt_run(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """NIGHTWATCH 즉시 실행 (테스트용)"""
        if not self._is_authorized(update):
            return
        await update.message.reply_text("NIGHTWATCH 즉시 실행 중...")
        try:
            from data.nightwatch import run_nightwatch, format_nightwatch_report
            report = await asyncio.to_thread(run_nightwatch)
            msg = format_nightwatch_report(report)
            for chunk in _split_message(msg):
                await update.message.reply_text(chunk)
        except Exception as e:
            await update.message.reply_text(f"NIGHTWATCH 실행 실패: {e}")

    async def cmd_predawn(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """시간외 선취매 현황 조회"""
        if not self._is_authorized(update):
            return
        try:
            from data.nightwatch import (
                select_predawn_targets, load_predawn_positions,
                format_predawn_alert,
            )

            # 1. 현재 선취매 대상 (recommendation.json 기준)
            nw_cfg = self.config.get("nightwatch", {})
            min_score = nw_cfg.get("predawn_min_score", 100)
            max_targets = nw_cfg.get("predawn_max_targets", 3)
            targets = select_predawn_targets(min_score, max_targets)

            # 2. 기 매수된 선취매 포지션
            predawn_pos = load_predawn_positions()

            msg_lines = []
            if predawn_pos:
                msg_lines.append("[ 선취매 보유 포지션 ]")
                for code, pos in predawn_pos.items():
                    pi = self.auto_trader.trader.fetch_price(code)
                    curr = pi.get("current_price", 0) if pi.get("success") else 0
                    entry = pos.get("entry_price", 0)
                    pnl = ((curr - entry) / entry * 100) if entry > 0 and curr > 0 else 0
                    msg_lines.append(
                        f"  {pos['name']}({code})\n"
                        f"  진입: {entry:,}원 → 현재: {curr:,}원 ({pnl:+.1f}%)\n"
                        f"  SL: {pos.get('sl', 0):,} | TP: {pos.get('tp', 0):,}"
                    )
                msg_lines.append("")

            if targets:
                msg = format_predawn_alert(targets)
                msg_lines.append(msg)
            elif not predawn_pos:
                msg_lines.append("선취매 대상 없음 (조건: AAA/AA + FORCE_BUY + 100점+)")

            nw_enabled = nw_cfg.get("enabled", False)
            alert_only = nw_cfg.get("alert_only", True)
            msg_lines.append(f"\n상태: {'ON' if nw_enabled else 'OFF'} | {'알림만' if alert_only else '자동매수'}")

            await update.message.reply_text("\n".join(msg_lines))

        except Exception as e:
            await update.message.reply_text(f"선취매 조회 실패: {e}")

    # ═══════════════════════════════════════
    #  봇 빌드 & 실행
    # ═══════════════════════════════════════

    async def _on_startup(self, app: Application):
        """봇 시작 시 자동매매 자동 시작 + 키보드 전송"""
        logger.info("봇 초기화 완료 - 자동매매 자동 시작")
        cleanup_old_logs(30)
        log_event("SYSTEM", "봇 시작")

        if not self.chat_id:
            logger.error("TELEGRAM_CHAT_ID 미설정 - 시작 메시지/자동매매 비활성")
            return
        chat_id = int(self.chat_id)

        # 자동매매 자동 시작 (auto_trade: true일 때)
        if self.config.get("bot", {}).get("auto_trade", False):
            async def _send_alert(text):
                # MSG-03/04: 체결/긴급만 텔레그램, 나머지 log_event
                _TG_KEYWORDS = ("✅ 자동 매수", "❌ 매수 실패", "매도 체결", "청산",
                                "리스크 게이트", "BRAIN 관망", "위기 모드", "🚫", "⛔",
                                "📉 트레일링", "손절")
                if any(kw in text for kw in _TG_KEYWORDS):
                    # 이모지 가이드: 금지 이모지 → 기능 마커
                    out = text.replace("📈", "").replace("📉", "")
                    if "✅ 자동 매수" in out:
                        out = out.replace("✅ 자동 매수", "💰 매수 체결")
                    elif "⛔ 손절" in out or "⛔ 동적 손절" in out:
                        out = out.replace("⛔ 손절", "🛑 [손절]").replace("⛔ 동적 손절", "🛑 [손절]")
                    elif "청산" in out or "매도 체결" in out:
                        if not out.startswith("📤"):
                            out = "📤 " + out
                    await app.bot.send_message(chat_id=chat_id, text=out)
                else:
                    # 로그에만 기록 — 텔레그램 미전송
                    cat = "TRADE_BUY" if "매수" in text else "TRADE_SELL" if "매도" in text else "SYSTEM"
                    log_event(cat, text[:200])

            self.auto_trader.start(_send_alert)
            now_str = datetime.now().strftime("%H:%M")
            startup_msg = f"🔮 Body Hunter v5 시작 ({now_str}) | 자동매매 ON"
        else:
            now_str = datetime.now().strftime("%H:%M")
            startup_msg = f"🔮 Body Hunter v5 시작 ({now_str}) | 자동매매 OFF"

        try:
            await app.bot.send_message(
                chat_id=chat_id,
                text=startup_msg,
                reply_markup=MAIN_KEYBOARD,
            )
        except Exception as e:
            logger.error(f"시작 메시지 전송 실패: {e}")

    async def _fallback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """매칭 안 된 메시지 처리"""
        if not update.message:
            return
        if not self._is_authorized(update):
            return
        cid = update.effective_chat.id
        text = update.message.text or ""
        logger.info(f"[미매칭] chat_id={cid}, text='{text}'")
        await update.message.reply_text(
            "❓ 알 수 없는 명령입니다\n'도움' 버튼을 눌러주세요",
            reply_markup=MAIN_KEYBOARD,
        )

    def build_app(self) -> Application:
        app = Application.builder().token(self.token).build()

        # 시작 시 키보드 전송
        app.post_init = self._on_startup

        # /start - 인증 없이 (최초 접속용)
        app.add_handler(CommandHandler("start", self.cmd_start))

        # 한글 명령어 (정확히 일치) - 인증은 핸들러 내부에서 처리
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
            r"^상태판$": self.cmd_dashboard,
            r"^청산$": self.cmd_liquidate,
            r"^일지$": self.cmd_journal,
            r"^유니버스$": self.cmd_universe,
            r"^유니버스갱신$": self.cmd_universe_rebuild,
            r"^분봉수집$": self.cmd_collect_minutes,
            r"^시그널$": self.cmd_signal_summary,
            r"^시작$": self.cmd_auto_start,
            r"^정지$": self.cmd_auto_stop,
            r"^확인$": self.cmd_confirm,
            r"^자동확인$": self.cmd_auto_confirm,
            r"^자동취소$": self.cmd_auto_cancel,
            r"^위기모드$": self.cmd_crisis_on,
            r"^위기해제$": self.cmd_crisis_off,
            r"^사전감지$": self.cmd_premove_scan,
            r"^AI모니터$": self.cmd_ai_monitor,
            r"^뉴스AI$": self.cmd_news_ai,
            r"^스윙스캔$": self.cmd_swing_scan,
            r"^이상거래$": self.cmd_volume_scan,
            r"^이벤트$": self.cmd_event_scan,
            r"^워치리스트$": self.cmd_swing_watchlist,
            r"^건전성$": self.cmd_market_health,
            r"^해외이벤트$": self.cmd_global_event,
            r"^종목선정$": self.cmd_swing_pick,
            r"^MACD스캔$": self.cmd_macd_scan,
            r"^시나리오$": self.cmd_scenario_list,
            r"^섹터릴레이$": self.cmd_sector_relay,
            r"^그룹릴레이$": self.cmd_group_relay,
            r"^ETF릴레이$": self.cmd_etf_relay,
            r"^릴레이종합$": self.cmd_relay_hub,
            r"^추천$": self.cmd_recommendation,
            r"^내일추천$": self.cmd_recommendation,
            r"^국적수급$": self.cmd_nationality,
            r"^배분현황$": self.cmd_brain,
            r"^로테이션$": self.cmd_rotation,
            r"^선행지표$": self.cmd_leading,
            r"^스트레스$": self.cmd_stress,
            r"^COT$": self.cmd_cot,
            r"^유동성$": self.cmd_liquidity,
            r"^NXT$": self.cmd_nxt,
            r"^NXT켜기$": self.cmd_nxt_on,
            r"^NXT끄기$": self.cmd_nxt_off,
            r"^NXT실행$": self.cmd_nxt_run,
            r"^선취매$": self.cmd_predawn,
            r"^페이퍼$": self.cmd_paper,
            # ── 온디맨드 명령어 (하루 5~7개 체제) ──
            r"^포트$": self.cmd_port,
            r"^ㅍ$": self.cmd_port,
            r"^뇌$": self.cmd_brain_status,
            r"^감시$": self.cmd_entry_watch,
            r"^ㅂ$": self.cmd_brain_status,
            r"^ㄱ$": self.cmd_entry_watch,
            r"^ㅌ$": self.cmd_paper,
            r"^ㄹ$": self.cmd_event_log,
            r"^이벤트로그$": self.cmd_event_log,
            r"^내일$": self.cmd_tomorrow,
            r"^내일취소$": self.cmd_tomorrow,
            r"^내일확인$": self.cmd_tomorrow,
            # ── AI Eye ──
            r"^눈$": self.cmd_eye_status,
            r"^ㄴ$": self.cmd_eye_status,
            # ── 데이터 검증 ──
            r"^데이터$": self.cmd_data_status,
            # ── 시장 일지 ──
            r"^시장일지$": self.cmd_market_journal,
            r"^ㅇ$": self.cmd_market_journal,
            r"^주간$": self.cmd_weekly_report,
            r"^월간$": self.cmd_monthly_report,
            # ── 이벤트 캘린더 ──
            r"^캘린더$": self.cmd_event_calendar,
            r"^주간캘린더$": self.cmd_weekly_calendar,
            r"^DART공시$": self.cmd_dart_governance,
            # ── 정책 트래커 ──
            r"^정책$": self.cmd_policy_tracker,
            r"^대통령실$": self.cmd_policy_tracker,
            # ── FLOWX VIP 콘텐츠 ──
            r"^VIP$": self.cmd_flowx_vip,
            r"^vip$": self.cmd_flowx_vip,
            r"^FLOWX$": self.cmd_flowx_vip,
            r"^flowx$": self.cmd_flowx_vip,
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

        app.add_handler(
            MessageHandler(filters.Regex(r"^국적수급\s+.+"), self.cmd_nationality)
        )
        app.add_handler(
            MessageHandler(filters.Regex(r"^내일\s+.+"), self.cmd_tomorrow)
        )
        app.add_handler(
            MessageHandler(filters.Regex(r"^적용\s+.+"), self.cmd_apply_proposals)
        )
        app.add_handler(
            MessageHandler(filters.Regex(r"^위기모드\s+.+"), self.cmd_crisis_on)
        )

        # 인자 없는 "분석" / "뉴스" → 안내
        async def _help_분석(u, c):
            await u.message.reply_text("사용법: 분석 삼성전자")

        async def _help_뉴스(u, c):
            await u.message.reply_text("사용법: 뉴스 삼성전자")

        app.add_handler(MessageHandler(filters.Regex(r"^분석$"), _help_분석))
        app.add_handler(MessageHandler(filters.Regex(r"^뉴스$"), _help_뉴스))

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
            logger.warning("JobQueue 사용 불가 - 자동매매 스케줄 등록 건너뜀")
            return

        bot_conf = self.config.get("bot", {})

        # ── 모든 스케줄은 KST (UTC+9) 기준 ──
        def kst_time(h, m):
            return dtime(h, m, tzinfo=KST)

        # 아침 스캔
        scan_time_str = bot_conf.get("morning_scan_time", "09:00")
        h, m = map(int, scan_time_str.split(":"))
        jq.run_daily(self.auto_trader.job_morning_scan, time=kst_time(h, m))
        logger.info(f"아침 스캔 등록: {scan_time_str} KST")

        # 포지션 감시 (30초)
        interval = bot_conf.get("scan_interval_sec", 30)
        jq.run_repeating(self.auto_trader.job_monitor, interval=interval, first=10)
        logger.info(f"포지션 감시 등록: {interval}초")

        # ★ 동적 목표가 재평가 + 트레일링 스탑 (15:00 - 장마감 전)
        reeval_str = bot_conf.get("reeval_time", "15:00")
        h_re, m_re = map(int, reeval_str.split(":"))
        jq.run_daily(self.auto_trader.job_daily_reeval, time=kst_time(h_re, m_re))
        logger.info(f"동적 목표가 재평가 등록: {reeval_str} KST (트레일링스탑+수급판정)")

        # 장마감 청산
        eod_str = bot_conf.get("eod_close_time", "15:10")
        h2, m2 = map(int, eod_str.split(":"))
        jq.run_daily(self.auto_trader.job_eod_close, time=kst_time(h2, m2))
        logger.info(f"장마감 청산 등록: {eod_str} KST")

        # 장마감 후 분봉 수집 (15:40)
        minute_str = bot_conf.get("minute_collect_time", "15:40")
        h3, m3 = map(int, minute_str.split(":"))
        jq.run_daily(self._job_collect_minutes, time=kst_time(h3, m3))
        logger.info(f"분봉 수집 등록: {minute_str} KST")

        # 일봉 + 수급 수집 (16:00)
        daily_str = bot_conf.get("daily_collect_time", "16:00")
        h4, m4 = map(int, daily_str.split(":"))
        jq.run_daily(self._job_collect_daily, time=kst_time(h4, m4))
        logger.info(f"일봉 수집 등록: {daily_str} KST")

        # 체결 스냅샷 폴링 - 장 시작 시 자동 시작 (09:01)
        tick_enabled = self.config.get("schedule", {}).get(
            "tick_collect", {}
        ).get("enabled", True)
        if tick_enabled:
            jq.run_daily(self._job_start_tick_polling, time=kst_time(9, 1))
            logger.info("체결 폴링 등록: 09:01 KST 시작 (1분 간격, 장중)")

        # 유니버스 리빌드 (08:35 - premium_levels 08:30과 충돌 방지)
        uni_str = bot_conf.get("universe_rebuild_time", "08:35")
        h5, m5 = map(int, uni_str.split(":"))
        jq.run_daily(self._job_rebuild_universe, time=kst_time(h5, m5))
        logger.info(f"유니버스 리빌드 등록: {uni_str} KST")

        # ★ 데이터 파이프라인 검증 (16:25 - 일봉 수집 후, 시그널 기록 전)
        jq.run_daily(self._job_verify_data, time=kst_time(16, 25))
        logger.info("데이터 파이프라인 검증 등록: 16:25 KST")

        # 일간 시그널 기록 (16:30 - 일봉 수집 후)
        jq.run_daily(self._job_record_signals, time=kst_time(16, 30))
        logger.info("일간 시그널 기록 등록: 16:30 KST")

        # 일간 학습 리포트 (16:40 - 시그널 기록 후)
        jq.run_daily(self._job_daily_learning, time=kst_time(16, 40))
        logger.info("일간 학습 리포트 등록: 16:40 KST")

        # Position Guardian (08:20 - 장 전 보유종목 수급 진단)
        jq.run_daily(self._job_position_guardian, time=kst_time(8, 20))
        logger.info("Position Guardian 등록: 08:20 KST")

        # 옵션 만기일 알림 (08:10 - D-2/D-1/D-day)
        jq.run_daily(self._job_options_expiry_alert, time=kst_time(8, 10))
        logger.info("옵션 만기일 알림 등록: 08:10 KST")

        # 정책 트래커 (07:55 - 장 전 정부 정책 수집)
        jq.run_daily(self._job_policy_scan, time=kst_time(7, 55))
        logger.info("정책 트래커 등록: 07:55 KST")

        # 해외 이벤트 캘린더 스캔 (08:00 - 장 전 D-3 알림)
        jq.run_daily(self._job_global_event_scan, time=kst_time(8, 0))
        logger.info("해외 이벤트 스캔 등록: 08:00 KST")

        # DART 거버넌스 공시 크롤링 (08:05 1차 / 13:00 2차 / 16:22 3차)
        jq.run_daily(self._job_dart_refresh, time=kst_time(8, 5))
        jq.run_daily(self._job_dart_refresh, time=kst_time(13, 0))
        jq.run_daily(self._job_dart_refresh, time=kst_time(16, 22))
        logger.info("DART 크롤링 등록: 08:05 / 13:00 / 16:22 KST")

        # 스윙 종목 선정 (16:35 - 시그널 기록 후)
        jq.run_daily(self._job_swing_picker, time=kst_time(16, 35))
        logger.info("스윙 종목 선정 등록: 16:35 KST")

        # MACD 제로선 크로스 스캔 (16:55 - 마감 리포트 16:50 이후, 선취매 16:52 충돌방지)
        jq.run_daily(self._job_macd_scan, time=kst_time(16, 55))
        logger.info("MACD 크로스 스캔 등록: 16:55 KST")

        # 사전감지 스캔 (08:50 - 장 시작 전)
        jq.run_daily(self._job_premove_scan, time=kst_time(8, 50))
        logger.info("사전감지 스캔 등록: 08:50 KST")

        # ── 통합 메시지: 하루 5~7개 ──
        # ① 모닝 브리프 (08:55 - 추천 파이프라인 직후)
        jq.run_daily(self._send_morning_brief, time=kst_time(8, 55))
        logger.info("모닝 브리프 등록: 08:55 KST")
        # FLOWX VIP 8패널 콘텐츠 (08:56 - 모닝 브리프 직후)
        jq.run_daily(self._job_flowx_vip_content, time=kst_time(8, 56))
        logger.info("FLOWX VIP 콘텐츠 등록: 08:56 KST")

        # ⑤ 프리클로즈 리포트 (14:50 — 기존 _job_preclose_report 대체)
        # → 이미 14:30 스캔 + 14:50 리포트 등록됨, _send_preclose_brief로 교체
        # ⑥ 일일 마감 리포트 (16:50 - 학습 완료 후)
        jq.run_daily(self._send_daily_closing, time=kst_time(16, 50))
        logger.info("일일 마감 리포트 등록: 16:50 KST")

        # ── 추천 파이프라인 3-Stage ──
        # Stage 1: 저녁 분석 (16:45 - 데이터 수집 완료 후)
        jq.run_daily(self.auto_trader.job_evening_analysis, time=kst_time(16, 45))
        logger.info("저녁 추천 분석 등록: 16:45 KST")

        # Stage 2: 미국장 체크 (06:30 - 다음날 새벽)
        jq.run_daily(self.auto_trader.job_us_market_check, time=kst_time(6, 30))
        logger.info("미국장 체크 등록: 06:30 KST")

        # ── ICT 프리미엄 레벨 (08:30) + 갭 탐지 (09:05) + Opening Range (10:05) ──
        jq.run_daily(self.auto_trader.job_premium_levels, time=kst_time(8, 30))
        logger.info("프리미엄 레벨 등록: 08:30 KST")
        jq.run_daily(self.auto_trader.job_gap_support, time=kst_time(9, 5))
        logger.info("갭 지지/저항 등록: 09:05 KST")
        jq.run_daily(self.auto_trader.job_opening_range, time=kst_time(10, 5))
        logger.info("Opening Range 등록: 10:05 KST")

        # ── 보유종목 대시보드 (장중 3회) + 알림 (60초) ──
        jq.run_daily(self._job_portfolio_dashboard, time=kst_time(10, 0))
        jq.run_daily(self._job_portfolio_dashboard, time=kst_time(13, 0))
        jq.run_daily(self._job_portfolio_dashboard, time=kst_time(14, 30))
        jq.run_repeating(self._job_portfolio_alert, interval=60, first=90)
        logger.info("보유종목 대시보드 등록: 10:00/13:00/14:30 KST + 알림 60초")

        # ── 전쟁모드 장중 추적 (시작알림 + 30분 요약 + 급등/급락 알림) ──
        jq.run_daily(self._job_war_startup, time=kst_time(9, 0))
        jq.run_repeating(self._job_war_tracker, interval=60, first=30)
        jq.run_daily(self._job_war_summary, time=kst_time(9, 30))
        jq.run_daily(self._job_war_summary, time=kst_time(10, 30))
        jq.run_daily(self._job_war_summary, time=kst_time(11, 30))
        jq.run_daily(self._job_war_summary, time=kst_time(13, 30))
        jq.run_daily(self._job_war_summary, time=kst_time(14, 30))
        jq.run_daily(self._job_war_summary, time=kst_time(15, 15))
        logger.info("전쟁모드 추적 등록: 09:00 시작알림 + 60초 감시 + 30분 요약")

        # ── PAPER 트레이딩 자동 추적 ──
        jq.run_daily(self._job_paper_register, time=kst_time(9, 5))
        jq.run_repeating(self._job_paper_check, interval=300, first=600)  # 5분마다
        jq.run_daily(self._job_paper_eod, time=kst_time(15, 25))
        logger.info("PAPER 트레이딩 등록: 09:05 등록 + 5분 체크 + 15:25 EOD")

        # ── 장중 TV 실시간 스캔 (30분마다 유니버스 전체) ──
        jq.run_daily(self._job_intraday_tv_init, time=kst_time(10, 0))
        jq.run_repeating(self._job_intraday_tv_scan, interval=1800, first=3600)
        logger.info("장중 TV 스캔 등록: 10:00 초기화 + 30분 유니버스 스캔")

        # ── 장중 뉴스 감성 스캔 (09:30 / 11:00 / 13:30) ──
        jq.run_daily(self._job_news_sentiment, time=kst_time(9, 30))
        jq.run_daily(self._job_news_sentiment, time=kst_time(11, 0))
        jq.run_daily(self._job_news_sentiment, time=kst_time(13, 30))
        logger.info("장중 뉴스 감성 등록: 09:30 / 11:00 / 13:30 KST")

        # ── 프리클로즈 스캔 (14:30 스캔 + 14:50 리포트) ──
        jq.run_daily(self._job_preclose_scan, time=kst_time(14, 30))
        jq.run_daily(self._send_preclose_brief, time=kst_time(14, 50))
        logger.info("프리클로즈 등록: 14:30 스캔 + 14:50 리포트")

        # ── FLOWX DAYTRADING 일괄 청산 (15:20 - 장 마감 전) ──
        jq.run_daily(self._job_flowx_close_daytrading, time=kst_time(15, 20))
        logger.info("FLOWX DAYTRADING 일괄 청산 등록: 15:20 KST")

        # ── FLOWX sector_universe 수급/거래량 UPDATE (15:40 — 정보봇 15:35 후) ──
        jq.run_daily(self._job_flowx_universe_update, time=kst_time(15, 40))
        logger.info("FLOWX sector_universe UPDATE 등록: 15:40 KST")

        # ── JARVIS BRAIN 자본 배분 (백업용 - NIGHTWATCH 미실행 대비) ──
        jq.run_daily(self.auto_trader.job_brain_allocation, time=kst_time(16, 36))
        logger.info("BRAIN 배분 백업 등록: 16:36 KST")

        # ── NIGHTWATCH NXT 야간매매 ──
        nw_cfg = self.config.get("nightwatch", {})
        if nw_cfg.get("enabled", False):
            collect_str = nw_cfg.get("collect_time", "16:02")
            h_nw1, m_nw1 = map(int, collect_str.split(":"))
            jq.run_daily(self.auto_trader.job_nightwatch_collect, time=kst_time(h_nw1, m_nw1))
            logger.info(f"NIGHTWATCH 수집 등록: {collect_str} KST")

            decide_str = nw_cfg.get("decide_time", "16:35")
            h_nw2, m_nw2 = map(int, decide_str.split(":"))
            jq.run_daily(self.auto_trader.job_nightwatch_decide, time=kst_time(h_nw2, m_nw2))
            logger.info(f"NIGHTWATCH 판단 등록: {decide_str} KST")

            sell_str = nw_cfg.get("morning_sell_time", "08:00")
            h_nw3, m_nw3 = map(int, sell_str.split(":"))
            jq.run_daily(self.auto_trader.job_nxt_morning_sell, time=kst_time(h_nw3, m_nw3))
            logger.info(f"NXT 아침 매도 등록: {sell_str} KST")

            # ── 시간외 선취매 (16:52 — 추천 16:45 완료 후) ──
            predawn_str = nw_cfg.get("predawn_time", "16:52")
            h_pd, m_pd = map(int, predawn_str.split(":"))
            jq.run_daily(self.auto_trader.job_predawn_buy, time=kst_time(h_pd, m_pd))
            logger.info(f"시간외 선취매 등록: {predawn_str} KST")

        # ── OPT-01: 옵션/ETF 심리 시그널 수집 (16:05 — NXT 수집 직후) ──
        jq.run_daily(self._job_collect_options_signal, time=kst_time(16, 5))
        logger.info("옵션 심리 시그널 수집 등록: 16:05 KST")

    async def _job_flowx_universe_update(self, context):
        """FLOWX sector_universe 수급/거래량 UPDATE (15:40 — 정보봇 15:35 후)"""
        from datetime import date
        if not is_trading_day():
            return
        try:
            from data.flowx_universe_updater import update_sector_universe
            import asyncio
            result = await asyncio.to_thread(update_sector_universe)
            kr = result.get("updated_kr", 0)
            us = result.get("updated_us", 0)
            err = result.get("errors", 0)
            logger.info(f"[FLOWX] sector_universe UPDATE: KR={kr} US={us} err={err}")
        except Exception as e:
            logger.error(f"[FLOWX] sector_universe UPDATE 실패: {e}")

    async def _job_collect_options_signal(self, context):
        """OPT-01: ETF 기반 Fear/Greed 시그널 수집"""
        from datetime import date
        if not is_trading_day():
            return
        try:
            from data.options_signal import collect_options_signal
            result = await asyncio.to_thread(collect_options_signal)
            logger.info(f"[OPT] {result.pc_level} ratio={result.fear_greed_ratio:.2f}x nxt_adj={result.nxt_adj:+.1f}")
        except Exception as e:
            logger.error(f"[OPT] 수집 실패: {e}")

    async def _job_start_tick_polling(self, context):
        """장 시작 시 체결 스냅샷 폴링 시작 (백그라운드 스레드)"""
        from datetime import date
        if not is_trading_day():
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
            logger.info(f"체결 폴링 시작: {len(codes)}종목 / {interval}초 간격")

            # 블로킹 루프를 별도 스레드에서 실행
            cycles = await asyncio.to_thread(
                tc.run_market_hours, codes, interval
            )

            logger.info(f"체결 폴링 종료: {cycles}사이클 완료")

        except Exception as e:
            logger.error(f"체결 폴링 에러: {e}")

    async def _job_collect_minutes(self, context):
        """장마감 후 분봉 수집 + 수급 분석 (15:40) — Silent: 로그만"""
        from datetime import date
        if not is_trading_day():
            return
        logger.info("분봉 자동 수집 시작...")
        try:
            from data.kis_collector import collect_today_minutes, UNIVERSE
            results = await asyncio.to_thread(collect_today_minutes)
            log_event("COLLECT", f"분봉 수집 {len(results)}/{len(UNIVERSE)}종목")
            logger.info(f"분봉 수집 완료: {len(results)}/{len(UNIVERSE)}종목 (로그만)")

            try:
                from data.minute_supply_analyzer import analyze_minute_supply
                signals = await asyncio.to_thread(
                    analyze_minute_supply, target_date=None, universe=UNIVERSE, top_n=20,
                )
                logger.info(f"수급 분석 완료: {len(signals or [])}종목 (로그만)")
            except Exception as e:
                logger.error(f"수급 분석 실패: {e}")
        except Exception as e:
            logger.error(f"분봉 수집 실패: {e}")

    async def _job_collect_daily(self, context):
        """장마감 후 일봉+수급 데이터 수집 (16:00) — Silent: 로그만"""
        from datetime import date
        if not is_trading_day():
            return
        logger.info("일봉+수급 자동 수집 시작...")
        t0 = time.time()
        pykrx_cnt = 0
        r1 = []
        ok, fail, sync_cnt = 0, 0, 0

        # 1. 일봉 pykrx
        try:
            from data.universe_builder import collect_daily_pykrx
            from data.kis_collector import UNIVERSE
            codes = list(UNIVERSE.keys())
            pykrx_cnt = await asyncio.to_thread(collect_daily_pykrx, codes, 24, True)
            logger.info(f"pykrx 일봉 수집 완료: {pykrx_cnt}종목")
        except Exception as e:
            logger.error(f"pykrx 일봉 수집 실패: {e}")

        # 2. 수급 데이터
        try:
            from data.kis_collector import UNIVERSE
            from data.flow_collector import (
                collect_investor_flow, collect_foreign_exhaustion,
                collect_short_balance, collect_short_volume,
            )
            codes = list(UNIVERSE.keys())
            r1 = await asyncio.to_thread(collect_investor_flow, codes, 24, True)
            r2 = await asyncio.to_thread(collect_foreign_exhaustion, codes, 24, True)
            r3 = await asyncio.to_thread(collect_short_balance, codes, 24, True)
            r4 = await asyncio.to_thread(collect_short_volume, codes, 24, True)
            logger.info(f"수급 수집 완료: {len(r1)}/{len(r2)}/{len(r3)}/{len(r4)}")
        except Exception as e:
            logger.error(f"수급 수집 실패: {e}")

        # 3. 외국인 국적별 수급
        try:
            nat_codes = self._get_nationality_targets()
            if nat_codes:
                from data.krx_nationality_crawler import afetch_nationality_batch
                date_from = (datetime.now() - timedelta(days=5)).strftime("%Y%m%d")
                date_to = datetime.now().strftime("%Y%m%d")
                nat_results = await afetch_nationality_batch(nat_codes, date_from, date_to)
                nat_ok = sum(1 for df in nat_results.values() if not df.empty)
                logger.info(f"국적별 수급 수집: {nat_ok}/{len(nat_codes)}")
        except Exception as e:
            logger.error(f"국적별 수급 실패: {e}")

        # 4. 수집 기록
        try:
            import json as _json
            collect_info = {
                "date": date.today().strftime("%Y-%m-%d"), "source": "bot",
                "time": datetime.now().strftime("%H:%M:%S"),
                "steps": {"daily": pykrx_cnt, "flow": len(r1) if r1 else 0,
                           "nationality": 8, "parquet": 0, "sync": 0},
            }
            lc_path = Path(__file__).parent.parent / "data_store" / "_last_collect.json"
            with open(lc_path, "w", encoding="utf-8") as f:
                _json.dump(collect_info, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"수집 기록 저장 실패: {e}")

        # 5. Parquet 빌드
        try:
            from data.extend_parquet_data import extend_parquet_all
            ok, fail = await asyncio.to_thread(lambda: extend_parquet_all(codes=None, force=True))
            logger.info(f"Parquet 빌드 완료: ok={ok}, fail={fail}")
        except Exception as e:
            logger.error(f"Parquet 빌드 실패: {e}")

        # 6. stock_data_daily 동기화
        try:
            from collect_all import step5_sync_stock_data_daily
            sync_cnt = await asyncio.to_thread(step5_sync_stock_data_daily)
            logger.info(f"stock_data_daily 동기화 완료: {sync_cnt}종목")
        except Exception as e:
            logger.error(f"stock_data_daily 동기화 실패: {e}")

        # 7. 최종 기록 업데이트
        try:
            collect_info["steps"]["parquet"] = ok
            collect_info["steps"]["sync"] = sync_cnt
            with open(lc_path, "w", encoding="utf-8") as f:
                _json.dump(collect_info, f, ensure_ascii=False, indent=2)
        except Exception:
            pass

        total_elapsed = int(time.time() - t0)
        logger.info(f"데이터 수집 완료 ({total_elapsed}초): 일봉={pykrx_cnt}, 수급={len(r1) if r1 else 0}, parquet={ok}")

    def _get_nationality_targets(self) -> list:
        """국적별 수급 수집 대상 종목 = 추천 + 보유 (중복 제거)"""
        codes = set()

        # 추천 종목
        try:
            import json
            rec_path = Path(__file__).parent.parent / "data_store" / "recommendation.json"
            if rec_path.exists():
                with open(rec_path, "r", encoding="utf-8") as f:
                    rec = json.load(f)
                for s in rec.get("stocks", []):
                    if s.get("code"):
                        codes.add(s["code"])
        except Exception:
            pass

        # 보유 종목 (자동매매 포지션)
        try:
            if self.auto_trader and hasattr(self.auto_trader, "_positions"):
                for code in self.auto_trader._positions.keys():
                    codes.add(code)
        except Exception:
            pass

        return list(codes) if codes else []

    async def cmd_nationality(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """국적수급 - 추천/보유 종목 외국인 국적별 수급 변화 보고"""
        if not self._is_authorized(update):
            return
        text = update.message.text.strip()

        # "국적수급 삼성전자" or "국적수급 005930" → 특정 종목
        parts = text.split()
        if len(parts) >= 2:
            target = parts[1]
            code, name = resolve_stock(target)
            if not code:
                await update.message.reply_text(f"종목 '{target}' 을 찾을 수 없습니다")
                return
            codes = [code]
            code_names = {code: name}
            await update.message.reply_text(f"🌍 {name}({code}) 국적별 수급 분석 중...")
        else:
            codes = self._get_nationality_targets()
            if not codes:
                await update.message.reply_text("추천/보유 종목 없음")
                return
            code_names = {c: CODE_TO_NAME.get(c, c) for c in codes}
            await update.message.reply_text(
                f"🌍 국적별 수급 분석: {', '.join(code_names.values())}"
            )

        try:
            from data.nationality_signal import (
                collect_daily_snapshots, generate_nationality_report,
                score_nationality_batch, _get_latest_data_date,
                _find_prev_trading_day,
            )

            date_new = _get_latest_data_date()

            # 스냅샷 수집 (금일 + 전일)
            await asyncio.to_thread(collect_daily_snapshots, codes, date_new)
            date_old = _find_prev_trading_day(date_new)
            if date_old:
                await asyncio.to_thread(collect_daily_snapshots, codes, date_old)

            # 보고서 생성
            report = await asyncio.to_thread(
                generate_nationality_report, codes, code_names, date_new,
            )
            if report:
                for chunk in _split_message(report):
                    await update.message.reply_text(chunk)

            # 점수 요약
            scores = await asyncio.to_thread(
                score_nationality_batch, codes, date_new,
            )
            if scores:
                score_lines = ["📊 국적별 수급 점수"]
                for code in codes:
                    sc, reason = scores.get(code, (0, ""))
                    name = code_names.get(code, code)
                    score_lines.append(f"  {name}: {sc:+.0f}점 ({reason})")
                await update.message.reply_text("\n".join(score_lines))

        except Exception as e:
            await update.message.reply_text(f"국적수급 실패: {str(e)[:300]}")

    async def _job_rebuild_universe(self, context):
        """장전 유니버스 리빌드 (08:35) — Silent: 로그만"""
        from datetime import date
        if not is_trading_day():
            return

        logger.info("유니버스 자동 리빌드 시작...")
        try:
            from data.universe_builder import build_universe
            uni = await asyncio.to_thread(build_universe)
            log_event("UNIVERSE", f"리빌드 {len(uni)}종목")
            logger.info(f"유니버스 리빌드 완료: {len(uni)}종목 (로그만)")
        except Exception as e:
            logger.error(f"유니버스 리빌드 실패: {e}")

    async def _job_verify_data(self, context):
        """16:25 데이터 파이프라인 검증 — 수집 완료 확인 후 분석 진행"""
        from datetime import date as _date
        if not is_trading_day():
            return
        logger.info("[VER] 데이터 파이프라인 검증 시작...")
        try:
            from data.data_verifier import DataVerifier, format_verify_oneliner
            from bot.bot_logger import log_event

            verifier = DataVerifier()
            result = verifier.verify_all()
            self._verify_result = result
            log_event("VERIFY", f"데이터 검증: {result['status']}", {
                "passed": result["passed"], "total": result["total"],
                "critical_failures": result["critical_failures"],
            })

            if not result["can_proceed"]:
                # CRITICAL 실패 → 재수집 시도
                logger.warning(f"[VER] CRITICAL 실패: {result['critical_failures']} — 재수집 시도")
                await self._retry_failed_collections(result)

                # 재검증
                result2 = DataVerifier().verify_all()
                self._verify_result = result2

                if not result2["can_proceed"]:
                    # 여전히 실패 → 긴급 알림
                    chat_id = os.getenv("TELEGRAM_CHAT_ID")
                    if chat_id:
                        descs = []
                        from data.data_verifier import CHECKLIST
                        for k in result2["critical_failures"]:
                            descs.append(CHECKLIST[k]["description"])
                        await context.bot.send_message(
                            chat_id=chat_id,
                            text=(
                                f"🚨 데이터 수집 실패 — 분석 중단\n"
                                f"   {', '.join(descs)}\n"
                                f"   수동 확인 필요"
                            ),
                        )
                    self._data_verified = False
                    return
                else:
                    logger.info("[VER] 재수집 후 CRITICAL 통과 — 분석 진행")

            self._data_verified = True

            if result.get("status") == "PARTIAL":
                logger.warning(f"[VER] 일부 누락: {result['failed']}건 — 분석 진행")

            logger.info(f"[VER] 검증 완료: {format_verify_oneliner(self._verify_result)}")
        except Exception as e:
            logger.error(f"[VER] 검증 실패 (무시, 진행): {e}", exc_info=True)
            self._data_verified = True  # 검증 자체 실패 시 → 기존대로 진행

    async def _retry_failed_collections(self, result: dict):
        """검증 실패 항목 재수집 시도 (최대 1회)"""
        from data.data_verifier import RETRY_MAP
        retried = set()
        for key, detail in result["details"].items():
            if detail["status"] != "FAIL" or key not in RETRY_MAP:
                continue
            job_name = RETRY_MAP[key]
            if job_name in retried:
                continue
            retried.add(job_name)
            logger.info(f"[VER] 재수집: {key} → {job_name}")
            try:
                # auto_trader의 job인 경우
                if job_name.startswith("job_"):
                    method = getattr(self.auto_trader, job_name, None)
                else:
                    method = getattr(self, job_name, None)
                if method:
                    await asyncio.wait_for(method(None), timeout=120)
            except Exception as e:
                logger.warning(f"[VER] 재수집 실패 {job_name}: {e}")

        # 재수집 후 30초 대기
        if retried:
            logger.info(f"[VER] {len(retried)}개 job 재수집 완료, 30초 대기 후 재검증")
            await asyncio.sleep(30)

    async def _job_record_signals(self, context):
        """일간 시그널 기록 (16:30) — Silent: 로그만"""
        from datetime import date
        if not is_trading_day():
            return
        if not self._data_verified:
            log_event("SKIP", "시그널 기록 — 데이터 미검증")
            logger.warning("시그널 기록 스킵: 데이터 미검증")
            return
        logger.info("일간 시그널 기록 시작...")
        try:
            from data.signal_analyzer import SignalAnalyzer
            from data.kis_collector import UNIVERSE
            exclude = {"069500", "371160", "102780", "305720"}
            codes = [c for c in UNIVERSE.keys() if c not in exclude]
            names = {c: UNIVERSE[c][0] for c in codes if c in UNIVERSE}
            sa = SignalAnalyzer()
            count = await asyncio.to_thread(sa.record_daily, codes, names)
            log_event("SIGNAL", f"일간 시그널 기록 {count}종목")
            logger.info(f"일간 시그널 기록 완료: {count}종목 (로그만)")
        except Exception as e:
            logger.error(f"시그널 기록 실패: {e}")

    # ── Position Guardian (보유종목 수급 진단) ──────────

    async def _job_position_guardian(self, context):
        """보유종목 수급 변곡점 진단 (08:20) — Silent: _morning_data에 저장"""
        from datetime import date
        if not is_trading_day():
            return

        logger.info("Position Guardian 시작...")
        try:
            from data.position_guardian import run as run_guardian
            result = await asyncio.to_thread(run_guardian)
            verdicts = result.get("verdicts", [])
            self._morning_data["guardian"] = verdicts
            exit_cnt = sum(1 for v in verdicts if v.action == "EXIT")
            reduce_cnt = sum(1 for v in verdicts if v.action == "REDUCE")
            log_event("GUARDIAN", f"{len(verdicts)}종목 진단 (EXIT:{exit_cnt} REDUCE:{reduce_cnt})")
            logger.info(f"Guardian 완료: {len(verdicts)}종목 (EXIT:{exit_cnt} REDUCE:{reduce_cnt})")
        except Exception as e:
            logger.error(f"Guardian 실패: {e}")

    # ── 일간 학습 리포트 ──────────────────────────────

    async def _job_daily_learning(self, context):
        """일간 학습 (16:40) — Silent: 결과를 _closing_data에 저장"""
        from datetime import date
        if not is_trading_day():
            return
        logger.info("일간 학습 리포트 시작...")
        try:
            from data.daily_learner import run as run_learner
            result = await asyncio.to_thread(run_learner)
            self._closing_data["learning"] = result
            log_event("LEARNER", f"적중률 {result['verify']['hit_rate']}%", result.get("verify"))
            logger.info(f"일간 학습 완료: 적중률 {result['verify']['hit_rate']}% (마감리포트에 통합)")

            try:
                from data.flowx_signals import run_signal_update
                fx_result = await asyncio.to_thread(run_signal_update)
                logger.info(f"[FLOWX] 시그널 업데이트: {fx_result.get('quant', {})}")
            except Exception as e_fx:
                logger.warning(f"[FLOWX] 시그널 업데이트 실패 (무시): {e_fx}")
        except Exception as e:
            logger.error(f"일간 학습 실패: {e}")

    # ── PAPER 트레이딩 자동 추적 ────────────────────────

    async def _job_paper_register(self, context):
        """09:05 PAPER 종목 자동 등록 — Silent: 로그만"""
        from datetime import date
        if not is_trading_day():
            return
        try:
            from data.trade_tracker import TradeTracker
            tracker = TradeTracker()
            names = tracker.register_paper_from_objects(kis=self.trader)
            if names:
                log_event("PAPER", f"{len(names)}종목 추적 시작: {', '.join(names)}")
                logger.info(f"[PAPER] {len(names)}종목 추적 시작: {', '.join(names)} (로그만)")
        except Exception as e:
            logger.error(f"PAPER 등록 실패: {e}")

    async def _job_paper_check(self, context):
        """장중 5분 — PAPER TP/SL 체크 — Silent: 이벤트를 _closing_data에 저장"""
        now = datetime.now(KST)
        if not is_trading_day(now.date()):
            return
        now_min = now.hour * 60 + now.minute
        if now_min < 545 or now_min >= 920:
            return
        try:
            from data.trade_tracker import TradeTracker
            tracker = TradeTracker()
            events = tracker.check_paper_prices(self.trader)
            if events:
                self._closing_data["paper_events"].extend(events)
                log_event("PAPER", f"TP/SL 이벤트 {len(events)}건", events)
                logger.info(f"[PAPER] TP/SL 이벤트 {len(events)}건 (마감리포트에 통합)")
        except Exception as e:
            logger.warning(f"PAPER 가격체크 실패: {e}")

    async def _job_paper_eod(self, context):
        """15:25 PAPER 시간손절 — Silent: _closing_data에 저장"""
        from datetime import date
        if not is_trading_day():
            return
        try:
            from data.trade_tracker import TradeTracker
            tracker = TradeTracker()
            events = tracker.paper_close_eod(self.trader)
            if events:
                self._closing_data["paper_events"].extend(events)
                log_event("PAPER", f"EOD 시간손절 {len(events)}건", events)
                logger.info(f"PAPER EOD: {len(events)}건 시간손절 (마감리포트에 통합)")
        except Exception as e:
            logger.error(f"PAPER EOD 실패: {e}")

    # ── 장중 TV 실시간 스캔 ────────────────────────────

    # 클래스 레벨 상태 (인스턴스별)
    _tv_avgs: dict = None
    _tv_history = None
    _tv_prev_codes: set = None

    # ── 장중 뉴스 감성 스캔 ──
    async def _job_news_sentiment(self, context):
        """09:30/11:00/13:30 — 보유종목 뉴스 감성 스캔 + Kill-Switch"""
        from datetime import date
        if not is_trading_day():
            return
        try:
            from data.news_sentiment import (
                scan_intraday_news, format_kill_switch_alert,
                format_scan_summary,
            )

            # 보유종목 + 매수 후보 수집
            positions = []
            if hasattr(self, "auto_trader") and self.auto_trader:
                for code, pos in self.auto_trader._positions.items():
                    positions.append({
                        "code": code,
                        "name": pos.get("name", code),
                    })

            # 매수 후보 (프리무브/추천)
            watchlist = []
            pm_path = Path(__file__).resolve().parent.parent / "data_store" / "premove_candidates.json"
            if pm_path.exists():
                try:
                    import json as _json
                    cands = _json.loads(pm_path.read_text(encoding="utf-8"))
                    for c in (cands if isinstance(cands, list) else []):
                        watchlist.append({
                            "code": c.get("code", ""),
                            "name": c.get("name", ""),
                        })
                except Exception:
                    pass

            if not positions and not watchlist:
                return

            report = await asyncio.to_thread(
                scan_intraday_news, positions, watchlist
            )

            # 로그 출력
            logger.info(format_scan_summary(report))

            # Kill-Switch → 텔레그램 긴급 알림
            chat_id = int(self.chat_id) if self.chat_id else None
            for ks in report.kill_switches:
                alert_text = format_kill_switch_alert(ks)
                if chat_id:
                    await context.bot.send_message(
                        chat_id=chat_id, text=alert_text
                    )

            # morning_data에 저장 (프리클로즈 등에서 활용)
            self._morning_data["news_sentiment"] = {
                "scanned_at": report.scanned_at,
                "kill_switches": [
                    {"code": r.code, "name": r.name, "sentiment": r.sentiment}
                    for r in report.kill_switches
                ],
                "warnings": [
                    {"code": r.code, "name": r.name, "sentiment": r.sentiment}
                    for r in report.warnings
                ],
            }

        except Exception as e:
            logger.error(f"[NEWS_SENTIMENT] 장중 스캔 실패: {e}")

    async def _job_intraday_tv_init(self, context):
        """10:00 장중 TV 스캔 초기화 — 20일 평균 거래대금 사전계산"""
        from datetime import date
        if not is_trading_day():
            return
        try:
            from data.trading_value_scanner import (
                precompute_tv_averages, IntradayHistory,
            )
            uni_path = Path(__file__).resolve().parent.parent / "data_store" / "universe.json"
            if not uni_path.exists():
                return
            import json as _json
            with open(uni_path, "r", encoding="utf-8") as f:
                universe = _json.load(f)

            self._tv_avgs = await asyncio.to_thread(
                precompute_tv_averages, universe, 5.0
            )
            self._tv_history = IntradayHistory()
            self._tv_prev_codes = set()
            logger.info(f"[TV Intraday] 초기화 완료: {len(self._tv_avgs)}종목")
        except Exception as e:
            logger.error(f"[TV Intraday] 초기화 실패: {e}")

    async def _job_intraday_tv_scan(self, context):
        """30분 반복 — 장중 TV 스캔 — Silent: 저장만 (프리클로즈에 통합)"""
        from datetime import date
        now = datetime.now()
        if not is_trading_day():
            return
        now_min = now.hour * 60 + now.minute
        if now_min < 600 or now_min > 920:
            return
        if not self._tv_avgs:
            return

        try:
            from data.trading_value_scanner import (
                scan_intraday_universe, save_intraday_results,
            )
            signals = await asyncio.to_thread(
                scan_intraday_universe,
                self.trader, self._tv_avgs, self._tv_history,
                self._tv_prev_codes, 500, 60.0,
            )
            if signals:
                save_intraday_results(signals)
                for sig in signals[:5]:
                    self._tv_prev_codes.add(sig.code)
                logger.info(f"[TV Intraday] {len(signals)}개 신호 저장 (프리클로즈에 통합)")
        except Exception as e:
            logger.error(f"[TV Intraday] 스캔 실패: {e}")

        # FIX-07: 장중 BRAIN 긴급 재평가 (30분 주기 체크)
        try:
            from data.market_brain import emergency_reassess
            result = await asyncio.to_thread(emergency_reassess)
            if result and result.get("triggered"):
                log_event("EMERGENCY", f"BRAIN 긴급 하향 {result['old_pct']}%→{result['new_pct']}%", result)
                chat_id = self.chat_id
                if chat_id:
                    await context.bot.send_message(
                        chat_id=chat_id,
                        text=(
                            f"🚨 [긴급] BRAIN 하향\n"
                            f"  {result['reason']}\n"
                            f"  비중 {result['old_pct']}% → {result['new_pct']}%\n"
                            f"  → 신규 매수 자동 축소/중단"
                        ),
                    )
        except Exception as e:
            logger.error(f"[BRAIN 긴급] 체크 실패: {e}")

    # ── 14:30 프리클로즈 스캔 ────────────────────────────

    async def _job_preclose_scan(self, context):
        """14:30 프리클로즈 스캔 — 장중 데이터 기반 내일 후보 도출"""
        from datetime import date
        if not is_trading_day():
            return
        try:
            from data.preclose_scanner import scan_preclose, save_preclose_results
            candidates = await asyncio.to_thread(
                scan_preclose,
                trader=self.trader,
                min_preclose_score=50.0,
                max_candidates=5,
            )
            if candidates:
                save_preclose_results(candidates)
                logger.info(f"[Preclose] 14:30 스캔 완료: {len(candidates)}종목")
            else:
                logger.info("[Preclose] 14:30 스캔 — 후보 없음")
        except Exception as e:
            logger.error(f"[Preclose] 스캔 실패: {e}")

    async def _job_preclose_report(self, context):
        """14:50 프리클로즈 리포트 — Silent: _send_preclose_brief에 통합됨"""
        # _send_preclose_brief(14:50)가 동일 시각에 통합 메시지 전송
        # 여기서는 log_event만 기록
        from datetime import date
        if not is_trading_day():
            return
        try:
            from data.preclose_scanner import load_preclose_results
            raw = load_preclose_results()
            n = len(raw) if raw else 0
            log_event("PRECLOSE", f"프리클로즈 리포트 {n}종목 (통합메시지에 포함)")
            logger.info(f"[Preclose] 리포트 {n}종목 (프리클로즈 브리프에 통합)")
        except Exception as e:
            logger.error(f"[Preclose] 리포트 실패: {e}")

    # ── FLOWX DAYTRADING 일괄 청산 ────────────────────

    async def _job_flowx_close_daytrading(self, context):
        """15:20 KST - DAYTRADING OPEN 시그널 전량 CLOSED"""
        from datetime import date
        if not is_trading_day():
            return
        try:
            from data.flowx_signals import close_daytrading
            count = await asyncio.to_thread(close_daytrading)
            if count > 0:
                logger.info(f"[FLOWX] DAYTRADING {count}건 일괄 청산")
        except Exception as e:
            logger.warning(f"[FLOWX] DAYTRADING 청산 실패 (무시): {e}")

    # ── 옵션 만기일 알림 ──────────────────────────────

    @staticmethod
    def _get_options_expiry(year: int, month: int):
        """해당 월의 둘째 주 목요일 (옵션/선물 만기일) 계산"""
        import calendar
        # 목요일 = 3 (0=월, 1=화, ..., 3=목)
        cal = calendar.monthcalendar(year, month)
        thursdays = [week[3] for week in cal if week[3] != 0]
        return thursdays[1] if len(thursdays) >= 2 else thursdays[-1]

    @staticmethod
    def _upcoming_expiry_info(today):
        """오늘 기준 가장 가까운 만기일 정보 반환
        Returns: (expiry_date, d_day, is_quarterly)
        """
        from datetime import date, timedelta
        # 분기 만기 (동시만기): 3,6,9,12월
        quarterly_months = {3, 6, 9, 12}
        # 월별 만기: 매월
        y, m = today.year, today.month

        # 이번달~다음달까지 확인
        for offset in range(3):
            cm = m + offset
            cy = y
            if cm > 12:
                cm -= 12
                cy += 1
            exp_day = BodyHunterBot._get_options_expiry(cy, cm)
            exp_date = date(cy, cm, exp_day)
            d_day = (exp_date - today).days
            if d_day >= 0:
                is_quarterly = cm in quarterly_months
                return exp_date, d_day, is_quarterly

        return None, -1, False

    async def _job_options_expiry_alert(self, context):
        """08:10 옵션 만기일 (D-7~D-day) — Silent: _morning_data에 저장"""
        from datetime import date
        today = date.today()
        if not is_trading_day(today):
            return

        exp_date, d_day, is_quarterly = self._upcoming_expiry_info(today)
        if exp_date is None or d_day > 7:
            return

        q_tag = "동시만기일" if is_quarterly else "옵션만기일"
        self._morning_data["options_expiry"] = {
            "d_day": d_day, "tag": q_tag,
            "date": exp_date.strftime("%m/%d"),
            "is_quarterly": is_quarterly,
        }
        log_event("OPTIONS", f"만기일 D-{d_day}: {q_tag}", {"d_day": d_day, "is_quarterly": is_quarterly})
        logger.info(f"옵션 만기일 D-{d_day}: {q_tag} (모닝브리프에 통합)")

    async def _job_flowx_vip_content(self, context):
        """FLOWX VIP 8패널 (08:56) — 모닝브리프 직후 자동 생성 + Supabase 업로드"""
        from datetime import date
        if not is_trading_day():
            return
        try:
            from data.flowx_content import run_vip_content
            content = await asyncio.to_thread(run_vip_content, True)
            n_stocks = len(content.get("panel_1_stocks", []))
            n_accum = len(content.get("panel_5_accumulation", []))
            log_event("FLOWX_VIP", f"8패널 생성+업로드: {n_stocks}종목, 매집{n_accum}건", content.get("summary", {}))
            logger.info(f"FLOWX VIP 8패널 완료: {n_stocks}종목, 매집{n_accum}건")
        except Exception as e:
            logger.error(f"FLOWX VIP 실패: {e}", exc_info=True)

    async def _job_policy_scan(self, context):
        """정책 트래커 (07:55) — Silent: _morning_data에 저장"""
        from datetime import date
        if not is_trading_day():
            return
        try:
            from data.policy_tracker import scan_policy
            result = await asyncio.to_thread(scan_policy)
            self._morning_data["policy"] = result
            n_sectors = len(result.get("matched_sectors", []))
            log_event("POLICY", f"정책 트래커 완료: {n_sectors}개 섹터", result.get("stats", {}))
            logger.info(f"정책 트래커 완료: {n_sectors}개 섹터 (모닝브리프에 통합)")
        except Exception as e:
            logger.error(f"정책 트래커 실패: {e}", exc_info=True)

    async def _job_global_event_scan(self, context):
        """해외 이벤트 캘린더 스캔 (08:00) — Silent: _morning_data에 저장"""
        from datetime import date
        if not is_trading_day():
            return

        logger.info("해외 이벤트 캘린더 스캔 시작...")
        try:
            from data.global_event_calendar import scan_global_events
            result = await asyncio.to_thread(scan_global_events)
            self._morning_data["global_events"] = result
            log_event("GLOBAL_EVENT", "해외 이벤트 스캔 완료", result)
            logger.info("해외 이벤트 스캔 완료 (모닝브리프에 통합)")
        except Exception as e:
            logger.error(f"해외 이벤트 스캔 실패: {e}", exc_info=True)

    async def _job_dart_refresh(self, context):
        """DART 거버넌스 공시 크롤링 (08:05) — Silent: 캐시 저장"""
        from datetime import date
        if not is_trading_day():
            return

        try:
            from data.event_calendar import refresh_dart_events
            events = await asyncio.to_thread(refresh_dart_events, 14)
            log_event("DART_REFRESH", f"DART 거버넌스 {len(events)}건 크롤링", {"count": len(events)})
            logger.info(f"DART 거버넌스 크롤링 완료: {len(events)}건")
        except Exception as e:
            logger.error(f"DART 거버넌스 크롤링 실패: {e}", exc_info=True)

    async def _job_swing_picker(self, context):
        """스윙 종목 선정 (16:35) — Silent: _closing_data에 저장"""
        from datetime import date
        if not is_trading_day():
            return
        logger.info("스윙 종목 선정 시작...")
        try:
            from data.swing_picker import run_picker
            result = await asyncio.to_thread(run_picker)
            self._closing_data["swing"] = result
            n = len(result.get("candidates", []))
            log_event("SIGNAL", f"스윙 종목 선정 {n}종목")
            logger.info(f"스윙 종목 선정 완료: {n}종목 (마감리포트에 통합)")
        except Exception as e:
            logger.error(f"스윙 종목 선정 실패: {e}", exc_info=True)

    async def _job_macd_scan(self, context):
        """MACD 크로스 스캔 (16:50) — Silent: 로그만"""
        from datetime import date
        if not is_trading_day():
            return
        logger.info("MACD 크로스 스캔 시작...")
        try:
            from strategies.macd_zero_scanner import run_daily_scan
            result = await asyncio.to_thread(run_daily_scan)
            p1 = len(result.get("phase1_new", []))
            p2 = len(result.get("phase2_entries", []))
            log_event("MACD", f"신규{p1} 진입{p2}")
            logger.info(f"MACD 스캔 완료: 신규{p1} 진입{p2} (로그만)")
        except Exception as e:
            logger.error(f"MACD 스캔 실패: {e}", exc_info=True)

    async def _job_premove_scan(self, context):
        """사전감지 스캔 (08:50) — Silent: 저장만 (모닝브리프에 반영됨)"""
        from datetime import date
        if not is_trading_day():
            return

        logger.info("사전감지 스캔 시작...")
        try:
            from data.premove_scanner import scan_premove, save_premove_candidates
            candidates = await asyncio.to_thread(scan_premove, 5)
            if candidates:
                await asyncio.to_thread(save_premove_candidates, candidates)
            self._morning_data["premove"] = candidates or []
            log_event("PREMOVE", f"{len(candidates or [])}개 후보 감지", {"count": len(candidates or [])})
            logger.info(f"사전감지 완료: {len(candidates or [])}개 후보 (모닝브리프에 통합)")
        except Exception as e:
            logger.error(f"사전감지 실패: {e}", exc_info=True)

    # ═══════════════════════════════════════════
    #  통합 메시지 시스템 (하루 5~7개)
    # ═══════════════════════════════════════════

    async def _send_morning_brief(self, context):
        """08:55 모닝 브리프 — 하루 시작 전 1개 메시지로 전체 요약

        이 시점에 이미 계산된 것들:
        - 06:30 nightwatch → market_brain
        - 08:00 global_events → _morning_data
        - 08:10 options_expiry → _morning_data
        - 08:20 position_guardian → _morning_data
        - 08:50 premove_scan → _morning_data
        - 08:55 morning_recommendation → recommendation.json
        """
        from datetime import date
        if not is_trading_day():
            return
        chat_id = os.getenv("TELEGRAM_CHAT_ID")
        if not chat_id:
            return

        try:
            # ── 선취매 포지션 → 정규 포지션 전환 (장 시작 전) ──
            try:
                self.auto_trader.merge_predawn_on_open()
            except Exception as e_pd:
                logger.warning(f"선취매 포지션 전환 실패: {e_pd}")

            now = datetime.now()
            dow = ["월", "화", "수", "목", "금", "토", "일"][now.weekday()]
            lines = [
                f"📋 [모닝 브리프] {now.month}/{now.day} ({dow})",
                "━━━━━━━━━━━━━━━━━━━",
            ]

            # ── 매크로 ──
            try:
                import json as _json
                brain_path = Path(__file__).parent.parent / "data_store" / "brain_report.json"
                if brain_path.exists():
                    with open(brain_path, "r", encoding="utf-8") as f:
                        brain = _json.load(f)
                    macro = brain.get("macro_summary", "")
                    mode = brain.get("regime", "표준")
                    contradictions = brain.get("contradictions", 0)
                    lines.append(f"🌍 매크로: {macro}" if macro else f"🌍 BRAIN: {mode}모드")
                    if contradictions:
                        lines.append(f"   모순 {contradictions}개 감지")
            except Exception:
                pass

            # ── 이벤트 (D-day, D-1, D-2만) ──
            events = self._morning_data.get("global_events", {})
            opt = self._morning_data.get("options_expiry")
            event_lines = []
            if opt and opt["d_day"] <= 2:
                event_lines.append(f"{opt['tag']} D-{opt['d_day']} ({opt['date']})")
            if events:
                upcoming = events.get("upcoming", [])
                for ev in upcoming[:2]:
                    if isinstance(ev, dict):
                        event_lines.append(ev.get("title", "")[:40])
                    elif isinstance(ev, str):
                        event_lines.append(ev[:40])
            if event_lines:
                lines.append(f"⚠️ 이벤트: {' | '.join(event_lines)}")

            # ── 이벤트 캘린더 (정적) ──
            try:
                from data.event_calendar import get_event_risk_for_recommendation
                er = get_event_risk_for_recommendation()
                if er.get("events"):
                    risk_lvl = er.get("risk_level", "LOW")
                    _ri = {"EXTREME": "💀", "HIGH": "🔴", "MEDIUM": "🟡", "LOW": "🟢"}.get(risk_lvl, "")
                    ev_names = [f"{e['name']}" for e in er["events"] if e.get("impact") == "HIGH"][:3]
                    lines.append(f"📅 캘린더: {_ri}{risk_lvl} {' '.join(ev_names)}")
            except Exception:
                pass

            # ── 정책 트래커 ──
            policy = self._morning_data.get("policy", {})
            p_matched = policy.get("matched_sectors", [])
            if p_matched:
                p_names = [m["sector"] for m in p_matched[:3]]
                lines.append(f"🏛 정책: {', '.join(p_names)}")

            # ── FLOWX VIP 매집 감지 ──
            try:
                from data.flowx_content import _load_json as _fload
                tv_data = _fload("tv_scanner.json")
                tv_sigs = [s for s in tv_data.get("signals", [])
                           if s.get("pattern", "NORMAL") != "NORMAL"]
                if tv_sigs:
                    top3 = sorted(tv_sigs, key=lambda x: x.get("score", 0), reverse=True)[:3]
                    accum_names = [f"{s['name']}({s['pattern'][:5]})" for s in top3]
                    lines.append(f"🔍 매집감지: {', '.join(accum_names)}")
            except Exception:
                pass

            # ── 보유종목 진단 (Guardian) ──
            verdicts = self._morning_data.get("guardian", [])
            if verdicts:
                lines.append("")
                lines.append("🛡 보유종목:")
                for v in verdicts:
                    name = v.name if hasattr(v, "name") else v.get("name", "")
                    action = v.action if hasattr(v, "action") else v.get("action", "")
                    risk = v.risk_score if hasattr(v, "risk_score") else v.get("risk_score", 0)
                    icon = "✅" if action == "HOLD" else ("🛑" if action == "EXIT" else "⚠️")
                    lines.append(f"   {name} {action} ({risk:.0f}) {icon}")

            # ── 매수 후보 (추천+TradeObject) ──
            try:
                import json as _json
                rec_path = Path(__file__).parent.parent / "data_store" / "recommendation.json"
                to_path = Path(__file__).parent.parent / "data_store" / "trade_objects.json"

                stocks = []
                if rec_path.exists():
                    with open(rec_path, "r", encoding="utf-8") as f:
                        rec = _json.load(f)
                    stocks = rec.get("stocks", [])

                trade_objs = {}
                if to_path.exists():
                    with open(to_path, "r", encoding="utf-8") as f:
                        to_data = _json.load(f)
                    for to in to_data.get("objects", []):
                        trade_objs[to.get("code", "")] = to

                if stocks:
                    lines.append("")
                    accepts = []
                    rejects = []
                    for s in stocks:
                        code = s.get("code", "")
                        name = s.get("name", "")
                        score = s.get("total", s.get("total_score", 0))
                        grade = s.get("grade", "")
                        to = trade_objs.get(code, {})
                        rr = to.get("rr_ratio", 0)
                        verdict = to.get("rr_verdict", "")
                        entry = to.get("entry_price", 0)

                        if verdict == "REJECT":
                            rejects.append(f"{name}(R:R {rr:.1f})")
                        else:
                            tag = "STRONG" if verdict == "STRONG_BUY" else verdict or grade
                            line = f"  {name} {score:.0f}점 {grade} | R:R {rr:.1f} {tag}"
                            if entry > 0:
                                line += f"\n    → 진입 {entry:,} 부근 대기"
                            accepts.append(line)

                    lines.append(f"📊 매수 후보 ({len(accepts)}종목):")
                    for i, a in enumerate(accepts[:3], 1):
                        lines.append(f"   {i}. {a.strip()}")
                    if rejects:
                        lines.append(f"   REJECT: {', '.join(rejects[:3])}")
            except Exception:
                pass

            # ── 가용현금 ──
            try:
                bal = await asyncio.to_thread(self.trader.fetch_balance)
                if bal.get("success"):
                    cash = bal["cash"]
                    total = bal.get("total_eval", cash)
                    ratio = cash / total * 100 if total > 0 else 0
                    lines.append("")
                    lines.append(f"💰 현금 {cash:,}원 ({ratio:.1f}%)")
            except Exception:
                pass

            lines.append("━━━━━━━━━━━━━━━━━━━")

            msg = "\n".join(lines)
            for chunk in _split_message(msg):
                await context.bot.send_message(chat_id=chat_id, text=chunk)
            logger.info("[모닝 브리프] 전송 완료")

            # 다음날을 위해 _morning_data 초기화
            self._morning_data = {}

        except Exception as e:
            logger.error(f"모닝 브리프 실패: {e}", exc_info=True)

    async def _send_preclose_brief(self, context):
        """14:50 프리클로즈 리포트 — 장중 감지 + 내일 후보"""
        from datetime import date
        if not is_trading_day():
            return
        chat_id = os.getenv("TELEGRAM_CHAT_ID")
        if not chat_id:
            return

        try:
            from data.preclose_scanner import (
                load_preclose_results, cross_validate_with_evening,
            )
            from data.trading_value_scanner import load_intraday_results

            lines = [
                "📋 [프리클로즈] 내일 후보",
                "━━━━━━━━━━━━━━━━━━━",
            ]

            # 장중 감지 요약
            intraday = load_intraday_results()
            if intraday:
                lines.append("")
                lines.append("📊 장중 감지:")
                tag_map = {"QUIET_ACCUMULATION": "조용한매집",
                           "GRADUAL_BUILDUP": "점진적증가"}
                for sig in intraday[:3]:
                    pat = tag_map.get(sig.get("pattern", ""), "거래대금폭발")
                    lines.append(
                        f"   {sig.get('name', '')} — {pat}"
                        f" (TV {sig.get('estimated_tv_ratio', 0):.1f}x)"
                    )

            # 프리클로즈 후보 (뉴스 부정 종목 필터)
            raw = load_preclose_results()
            news_rejected = []
            try:
                from data.news_sentiment import get_news_sentiment_for_scoring
                filtered = []
                for r in (raw or []):
                    ns = get_news_sentiment_for_scoring(r.get("code", ""))
                    if ns.get("action") in ("KILL_SWITCH", "WARNING"):
                        news_rejected.append(r.get("name", ""))
                    else:
                        filtered.append(r)
                raw = filtered
            except Exception:
                pass

            if raw:
                lines.append("")
                if news_rejected:
                    lines.append(f"📰 뉴스 악재 제외: {', '.join(news_rejected)}")
                lines.append(f"📊 내일 후보 ({len(raw)}종목):")
                for i, r in enumerate(raw[:3], 1):
                    to = r.get("trade_object", {})
                    rr = to.get("rr_ratio", r.get("rr_ratio", 0))
                    verdict = to.get("rr_verdict", r.get("rr_verdict", ""))
                    entry = to.get("entry_price", 0)
                    tp = to.get("target_price", 0)
                    icon = "✅" if verdict in ("GOOD", "STRONG_BUY") else "⚠️"
                    lines.append(
                        f"  {i}. {r['name']} | R:R {rr:.2f} {icon}"
                    )
                    if entry > 0 and tp > 0:
                        lines.append(f"     진입 {entry:,} → 목표 {tp:,}")

                # 교차검증
                codes = [r["code"] for r in raw]
                overlaps = cross_validate_with_evening(codes)
                if overlaps:
                    lines.append(f"  저녁분석 겹침: {', '.join(overlaps[:3])} ✅")

                lines.append("")
                lines.append("15:00~15:20 진입 대기")
            else:
                lines.append("")
                lines.append("후보 없음 — 저녁분석 대기")

            lines.append("━━━━━━━━━━━━━━━━━━━")

            msg = "\n".join(lines)
            for chunk in _split_message(msg):
                await context.bot.send_message(chat_id=chat_id, text=chunk)
            logger.info("[프리클로즈 리포트] 전송 완료")

        except Exception as e:
            logger.error(f"프리클로즈 리포트 실패: {e}", exc_info=True)

    async def _send_daily_closing(self, context):
        """16:50 일일 마감 리포트 — 하루 결과 1개 메시지

        이 시점에 이미 계산된 것들:
        - 장중 체결 기록 (_closing_data["trades_today"])
        - PAPER 이벤트 (_closing_data["paper_events"])
        - 16:40 daily_learner (_closing_data["learning"])
        - 16:35 swing_picker (_closing_data["swing"])
        - 16:45 job_evening_analysis → recommendation.json
        """
        from datetime import date
        if not is_trading_day():
            return
        chat_id = os.getenv("TELEGRAM_CHAT_ID")
        if not chat_id:
            return

        try:
            now = datetime.now()
            dow = ["월", "화", "수", "목", "금", "토", "일"][now.weekday()]
            lines = [
                f"📊 [마감] {now.month}/{now.day} ({dow})",
                "━━━━━━━━━━━━━━━━━━━",
            ]

            # ── 오늘 실현 P&L ──
            trades = self._closing_data.get("trades_today", [])
            if trades:
                total_pnl = sum(t.get("pnl_krw", 0) for t in trades)
                lines.append("")
                lines.append(f"💰 실현 P&L: {total_pnl:+,}원")
                for t in trades:
                    icon = "✅" if t.get("pnl_pct", 0) > 0 else "🛑"
                    lines.append(
                        f"   {t.get('name', '')} {t.get('pnl_pct', 0):+.1f}%"
                        f" ({t.get('pnl_krw', 0):+,}원) {t.get('reason', '')} {icon}"
                    )

            # ── 포트폴리오 현황 ──
            try:
                bal = await asyncio.to_thread(self.trader.fetch_balance)
                if bal.get("success"):
                    total = bal.get("total_eval", 0)
                    cash = bal.get("cash", 0)
                    pos_cnt = len(bal.get("positions", []))
                    ratio = cash / total * 100 if total > 0 else 0
                    lines.append("")
                    lines.append(f"📊 총자산 {total:,}원 | 보유 {pos_cnt} | 현금 {ratio:.1f}%")

                    # 보유종목 상세
                    for p in bal.get("positions", []):
                        pnl = p.get("pnl_pct", 0)
                        lines.append(
                            f"   {p.get('name', '')} {pnl:+.1f}%"
                            f" D+{p.get('hold_days', '?')}"
                        )
            except Exception:
                pass

            # ── PAPER 결과 ──
            paper_events = self._closing_data.get("paper_events", [])
            if paper_events:
                lines.append("")
                lines.append(f"[PAPER] {len(paper_events)}건:")
                for ev in paper_events[:3]:
                    lines.append(f"  {ev[:60]}" if isinstance(ev, str) else f"  {ev}")

            # ── 내일 준비 ──
            try:
                import json as _json
                rec_path = Path(__file__).parent.parent / "data_store" / "recommendation.json"
                pc_path = Path(__file__).parent.parent / "data_store" / "preclose_results.json"

                lines.append("")
                lines.append("📋 내일:")

                # 프리클로즈 진입
                if pc_path.exists():
                    with open(pc_path, "r", encoding="utf-8") as f:
                        pc = _json.load(f)
                    pc_names = [c.get("name", "") for c in pc.get("candidates", pc) if isinstance(c, dict)]
                    if pc_names:
                        lines.append(f"  프리클로즈: {', '.join(pc_names[:3])}")

                # 저녁 분석 후보
                if rec_path.exists():
                    with open(rec_path, "r", encoding="utf-8") as f:
                        rec = _json.load(f)
                    rec_names = [s.get("name", "") for s in rec.get("stocks", [])[:3]]
                    if rec_names:
                        lines.append(f"  저녁분석: {', '.join(rec_names)}")
            except Exception:
                pass

            # ── 데이터 검증 결과 ──
            if self._verify_result:
                try:
                    from data.data_verifier import format_verify_oneliner
                    lines.append("")
                    lines.append(format_verify_oneliner(self._verify_result))
                except Exception:
                    pass

            # ── 학습 ──
            learning = self._closing_data.get("learning", {})
            if learning:
                verify = learning.get("verify", {})
                hit_rate = verify.get("hit_rate", 0)
                missed = learning.get("missed_gainers", [])
                lines.append("")
                lines.append(f"🧠 적중 {hit_rate}%")
                if missed:
                    top = missed[0] if missed else {}
                    if isinstance(top, dict):
                        lines.append(
                            f"  놓친 급등: {top.get('name', '')} "
                            f"+{top.get('pnl', 0):.1f}%"
                        )

            # ── AI Eye 요약 (EYE-09) ──
            try:
                eye = getattr(self.auto_trader, '_eye', None) if self.auto_trader else None
                if eye and eye._history:
                    lines.append("")
                    lines.append("👁 AI Eye:")
                    for code in list(eye._history.keys()):
                        timeline = eye.format_history_timeline(code)
                        name = eye._names.get(code, code)
                        if timeline:
                            lines.append(f"  {name}: {timeline}")

                    # eye_history.json 저장 (학습/검증용)
                    try:
                        import json as _json
                        from datetime import date as _date
                        hist_dir = Path(__file__).parent.parent / "data_store" / "learning"
                        hist_dir.mkdir(parents=True, exist_ok=True)
                        hist_path = hist_dir / "eye_history.json"
                        existing = {}
                        if hist_path.exists():
                            existing = _json.loads(hist_path.read_text("utf-8"))
                        today_key = _date.today().isoformat()
                        existing[today_key] = {
                            code: eye.get_history(code)
                            for code in eye._history
                        }
                        # 최근 30일만 유지
                        keys = sorted(existing.keys())
                        if len(keys) > 30:
                            for old_key in keys[:-30]:
                                existing.pop(old_key, None)
                        hist_path.write_text(_json.dumps(existing, ensure_ascii=False, indent=2), "utf-8")
                    except Exception as eh:
                        logger.warning(f"eye_history 저장 실패: {eh}")
            except Exception:
                pass

            lines.append("━━━━━━━━━━━━━━━━━━━")

            msg = "\n".join(lines)
            for chunk in _split_message(msg):
                await context.bot.send_message(chat_id=chat_id, text=chunk)
            logger.info("[일일 마감 리포트] 전송 완료")

            # 다음날을 위해 초기화
            self._closing_data = {"trades_today": [], "paper_events": []}
            self._data_verified = False
            self._verify_result = None

        except Exception as e:
            logger.error(f"일일 마감 리포트 실패: {e}", exc_info=True)

    # ═══════════════════════════════════════════
    #  온디맨드 명령어 (내가 보고 싶을 때만)
    # ═══════════════════════════════════════════

    async def cmd_port(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """포트폴리오 현황 (보유종목 + 수익률 + 현금)"""
        if not self._is_authorized(update):
            return
        try:
            from bot.portfolio_monitor import generate_dashboard
            text = await asyncio.to_thread(generate_dashboard, self.trader)
            await update.message.reply_text(text)
        except Exception as e:
            await update.message.reply_text(f"포트폴리오 조회 실패: {e}")

    async def cmd_brain_status(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Market Brain 현재 상태"""
        if not self._is_authorized(update):
            return
        try:
            import json as _json
            brain_path = Path(__file__).parent.parent / "data_store" / "brain_report.json"
            if not brain_path.exists():
                await update.message.reply_text("Brain 데이터 없음")
                return
            with open(brain_path, "r", encoding="utf-8") as f:
                brain = _json.load(f)

            regime = brain.get("regime", "?")
            macro = brain.get("macro_summary", "")
            contradictions = brain.get("contradictions", 0)
            alloc = brain.get("allocation", {})

            lines = [
                f"Market Brain 상태",
                "━━━━━━━━━━━━━━━━━━━",
                f"레짐: {regime}",
                f"매크로: {macro}",
                f"모순: {contradictions}개",
            ]
            if alloc:
                lines.append(f"배분: {alloc}")
            await update.message.reply_text("\n".join(lines))
        except Exception as e:
            await update.message.reply_text(f"Brain 조회 실패: {e}")

    async def cmd_entry_watch(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """현재 진입 감시 중인 종목 목록"""
        if not self._is_authorized(update):
            return
        try:
            watches = self.auto_trader.get_entry_watches() if hasattr(self.auto_trader, "get_entry_watches") else {}
            if not watches:
                await update.message.reply_text("현재 감시 중인 종목 없음")
                return
            lines = ["진입 감시 목록", "━━━━━━━━━━━━━━━━━━━"]
            for code, w in watches.items():
                name = w.get("name", code)
                lines.append(f"  {name} | VWAP {w.get('entry_price', 0):,}")
            await update.message.reply_text("\n".join(lines))
        except Exception as e:
            await update.message.reply_text(f"감시목록 조회 실패: {e}")

    async def cmd_event_log(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """오늘 이벤트 로그 최근 20줄 (bot_logger)"""
        if not self._is_authorized(update):
            return
        text = read_today_log(20)
        await update.message.reply_text(f"이벤트 로그 (최근 20줄)\n━━━━━━━━━━━━━━━━━━━\n{text[-3500:]}")

    async def cmd_tomorrow(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """내일 매수 종목 등록 / 조회 / 취소

        사용법:
            /내일                → 현재 등록 목록 조회
            /내일 GS건설 제일일렉트릭  → 자동 VWAP 진입 등록
            /내일 GS건설 풍산수동      → 풍산은 알림만 (수동)
            /내일 취소            → 목록 초기화
        """
        if not self._is_authorized(update):
            return

        import json as _json
        from bot.kis_trader import resolve_stock

        picks_path = Path(__file__).parent.parent / "data_store" / "tomorrow_picks.json"
        text = update.message.text.strip()
        # "내일 ..." 또는 "내일취소" 또는 "내일확인"
        parts = text.split()
        cmd = parts[0]  # "내일" / "내일취소" / "내일확인"
        args = parts[1:] if len(parts) > 1 else []

        # ── 취소 ("내일취소" 또는 "내일 취소") ──
        if cmd == "내일취소" or (args and args[0] == "취소"):
            if picks_path.exists():
                picks_path.unlink()
            await update.message.reply_text("✅ 내일 매수 목록 초기화")
            return

        # ── 조회 ("내일" 인수 없음 또는 "내일확인") ──
        if not args or cmd == "내일확인":
            if not picks_path.exists():
                await update.message.reply_text(
                    "내일 매수 목록 없음\n"
                    "━━━━━━━━━━━━━━━━━━━\n"
                    "등록: 내일 GS건설 제일일렉트릭\n"
                    "추천 보기: 추천"
                )
                return
            with open(picks_path, "r", encoding="utf-8") as f:
                data = _json.load(f)
            if not data.get("picks"):
                await update.message.reply_text("내일 매수 목록 비어있음")
                return
            lines = [f"📋 내일 매수 목록 ({data.get('date', '?')})"]
            lines.append("━━━━━━━━━━━━━━━━━━━")
            for p in data["picks"]:
                mode_tag = "수동" if p.get("mode") == "manual" else "자동"
                lines.append(f"   {p['name']}({p['code']}) — {mode_tag}")
            lines.append("━━━━━━━━━━━━━━━━━━━")
            lines.append("변경: 내일 종목명 / 취소: 내일취소")
            await update.message.reply_text("\n".join(lines))
            return

        # ── 등록 ──
        picks = []
        not_found = []
        for arg in args:
            # "풍산수동" → name="풍산", mode="manual"
            if arg.endswith("수동"):
                query = arg[:-2]
                mode = "manual"
            else:
                query = arg
                mode = "auto"

            code, name = resolve_stock(query)
            if code:
                picks.append({"code": code, "name": name, "mode": mode})
            else:
                not_found.append(query)

        if not picks and not_found:
            await update.message.reply_text(f"❌ 종목을 찾을 수 없습니다: {', '.join(not_found)}")
            return

        # 날짜: 오늘 15시 이후면 내일, 아니면 오늘
        from datetime import date, timedelta
        now = datetime.now()
        target_date = date.today()
        if now.hour >= 15:
            # 주말+공휴일 건너뛰기
            target_date = next_trading_day(target_date)

        data = {
            "date": target_date.strftime("%Y-%m-%d"),
            "created_at": now.strftime("%Y-%m-%d %H:%M"),
            "picks": picks,
        }
        with open(picks_path, "w", encoding="utf-8") as f:
            _json.dump(data, f, ensure_ascii=False, indent=2)

        # ── 응답: Trade Object R:R 정보 포함 ──
        lines = [f"✅ 내일 매수 등록 ({len(picks)}종목)"]
        lines.append("━━━━━━━━━━━━━━━━━━━")

        # Trade Object 로드
        to_map = {}
        try:
            to_path = Path(__file__).parent.parent / "data_store" / "trade_objects.json"
            if to_path.exists():
                with open(to_path, "r", encoding="utf-8") as f:
                    to_data = _json.load(f)
                for t in to_data.get("objects", []):
                    to_map[t.get("code", "")] = t
        except Exception:
            pass

        for p in picks:
            mode_tag = "수동 (알림만)" if p["mode"] == "manual" else "자동 VWAP 진입"
            to = to_map.get(p["code"], {})
            rr = to.get("rr_ratio", 0)
            verdict = to.get("rr_verdict", "")
            if rr > 0:
                lines.append(f"   {p['name']} — R:R {rr:.2f} {verdict} | {mode_tag}")
            else:
                lines.append(f"   {p['name']} — {mode_tag}")

        if not_found:
            lines.append(f"   ⚠️ 미발견: {', '.join(not_found)}")

        # BRAIN 상태 요약
        try:
            brain_path = Path(__file__).parent.parent / "data_store" / "brain_report.json"
            if brain_path.exists():
                with open(brain_path, "r", encoding="utf-8") as f:
                    brain = _json.load(f)
                regime = brain.get("regime", "?")
                alloc = brain.get("allocation", {})
                pct = alloc.get("position_size_pct", 100) if alloc else 100
                lines.append(f"   BRAIN: {regime} {pct}%")
        except Exception:
            pass

        # 종목당 예상 금액
        try:
            bal = await asyncio.to_thread(self.trader.fetch_balance)
            if bal.get("success"):
                cash = bal["cash"]
                auto_count = sum(1 for p in picks if p["mode"] == "auto")
                if auto_count > 0:
                    per_stock = int(cash * 0.9 / auto_count)
                    lines.append(f"   종목당 ~{per_stock:,}원")
        except Exception:
            pass

        lines.append("━━━━━━━━━━━━━━━━━━━")
        lines.append("변경: 내일 종목명 / 취소: 내일취소")
        lines.append("추천 보기: 추천")
        await update.message.reply_text("\n".join(lines))

    # ── 데이터 검증 핸들러 ────────────────────────

    async def cmd_eye_status(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """AI Eye 장중 현황 — 보유종목 실시간 판정"""
        if not self._is_authorized(update):
            return
        try:
            eye = getattr(self.auto_trader, '_eye', None) if self.auto_trader else None
            if eye is None:
                await update.message.reply_text("AI Eye 아직 미초기화 (장중 자동매매 시작 후 활성)")
                return

            status = eye.get_status()
            if not status:
                await update.message.reply_text("AI Eye 감시 종목 없음")
                return

            now = datetime.now().strftime("%H:%M")
            lines = [f"👁 AI Eye 현황 ({now})", "━━━━━━━━━━━━━━━━━━━"]

            verdict_icon = {
                "ALIVE": "🟢", "BREAKING": "🚀", "BOUNCING": "🔵",
                "WEAKENING": "🟡", "DYING": "🔴", "WARMUP": "⏳",
            }
            for code, info in status.items():
                v = info.get("verdict", "WARMUP")
                icon = verdict_icon.get(v, "⚪")
                price = info.get("latest_price", 0)
                bars = info.get("bars", 0)
                name = info.get("name", code)

                # 타임라인
                timeline = eye.format_history_timeline(code)
                line = f"{icon} {name}: {v} | {price:,}원 ({bars}봉)"
                lines.append(line)
                if timeline:
                    lines.append(f"   {timeline}")

            lines.append("━━━━━━━━━━━━━━━━━━━")
            await update.message.reply_text("\n".join(lines))
        except Exception as e:
            await update.message.reply_text(f"AI Eye 조회 실패: {e}")

    async def cmd_data_status(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """데이터 수집 현황 조회"""
        if not self._is_authorized(update):
            return
        try:
            from data.data_verifier import DataVerifier, format_verify_result
            result = await asyncio.to_thread(DataVerifier().verify_all)
            text = format_verify_result(result)
            for chunk in [text[i:i+TG_MAX] for i in range(0, len(text), TG_MAX)]:
                await update.message.reply_text(chunk)
        except Exception as e:
            await update.message.reply_text(f"데이터 검증 실패: {e}")

    # ── 시장 일지 핸들러 ────────────────────────

    async def cmd_market_journal(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """시장 일지 (오늘)"""
        if not self._is_authorized(update):
            return
        try:
            from data.market_journal import format_daily_journal, DAILY_DIR
            import json as _json
            from datetime import date as _date

            today = _date.today().strftime("%Y-%m-%d")
            path = DAILY_DIR / f"{today}.json"
            if not path.exists():
                await update.message.reply_text(f"오늘({today}) 시장 일지가 아직 없습니다.\n16:45 이후 자동 생성됩니다.")
                return
            journal = _json.loads(path.read_text("utf-8"))
            text = format_daily_journal(journal)
            for chunk in [text[i:i+TG_MAX] for i in range(0, len(text), TG_MAX)]:
                await update.message.reply_text(chunk)
        except Exception as e:
            await update.message.reply_text(f"시장 일지 조회 실패: {e}")

    async def cmd_weekly_report(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """주간 보고"""
        if not self._is_authorized(update):
            return
        try:
            from data.market_journal import generate_weekly_report, format_weekly_report
            report = await asyncio.to_thread(generate_weekly_report)
            text = format_weekly_report(report)
            for chunk in [text[i:i+TG_MAX] for i in range(0, len(text), TG_MAX)]:
                await update.message.reply_text(chunk)
        except Exception as e:
            await update.message.reply_text(f"주간 보고 실패: {e}")

    async def cmd_monthly_report(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """월간 보고 + 파라미터 조정 제안"""
        if not self._is_authorized(update):
            return
        try:
            from data.market_journal import generate_monthly_report, format_monthly_report
            report = await asyncio.to_thread(generate_monthly_report)
            text = format_monthly_report(report)
            # 제안이 있으면 context에 저장 (적용 명령에서 사용)
            proposals = report.get("parameter_proposals", [])
            if proposals:
                context.user_data["journal_proposals"] = proposals
            for chunk in [text[i:i+TG_MAX] for i in range(0, len(text), TG_MAX)]:
                await update.message.reply_text(chunk)
        except Exception as e:
            await update.message.reply_text(f"월간 보고 실패: {e}")

    async def cmd_apply_proposals(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """R:R 조정 제안 적용 ('적용 1 2' 또는 '적용 전체')"""
        if not self._is_authorized(update):
            return
        try:
            from data.market_journal import apply_proposals

            proposals = context.user_data.get("journal_proposals", [])
            if not proposals:
                await update.message.reply_text("적용할 제안이 없습니다. 먼저 '월간' 명령으로 보고를 조회하세요.")
                return

            text = update.message.text.strip()
            args = text.split()[1:]  # "적용" 뒤의 인자

            if "전체" in args:
                indices = list(range(1, len(proposals) + 1))
            else:
                indices = []
                for a in args:
                    try:
                        indices.append(int(a))
                    except ValueError:
                        pass

            if not indices:
                await update.message.reply_text("사용법: 적용 1 2  또는  적용 전체")
                return

            result = await asyncio.to_thread(apply_proposals, indices, proposals)
            applied = result.get("applied", [])
            if applied:
                lines = ["R:R override 적용 완료:"] + applied
                lines.append("\n다음 매매부터 반영됩니다.")
                await update.message.reply_text("\n".join(lines))
            else:
                await update.message.reply_text("적용된 항목이 없습니다. 번호를 확인하세요.")
        except Exception as e:
            await update.message.reply_text(f"제안 적용 실패: {e}")

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
