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
import time
import asyncio
import logging
from pathlib import Path
from datetime import datetime, time as dtime, timezone, timedelta

# 한국 표준시 (UTC+9) - 스케줄러에 반드시 전달
KST = timezone(timedelta(hours=9))

from telegram import Update, ReplyKeyboardMarkup, BotCommand
from telegram.ext import (
    Application, CommandHandler, MessageHandler,
    ContextTypes, filters,
)

from bot.kis_trader import KISTrader, resolve_stock, CODE_TO_NAME
from bot.auto_trader import AutoTrader

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
        ["사전감지", "스윙스캔", "스캔"],
        ["이상거래", "건전성", "이벤트"],
        ["종목선정", "MACD스캔", "워치리스트"],
        ["섹터릴레이", "그룹릴레이", "ETF릴레이"],
        ["릴레이종합", "내일추천", "뉴스AI"],
        ["AI모니터", "해외이벤트", "국적수급"],
        ["시나리오", "현재잔고", "체결내역"],
        ["포트폴리오", "시작", "정지"],
        ["상태", "유니버스", "일지"],
        ["배분현황", "로테이션", "도움"],
        ["청산"],
    ],
    resize_keyboard=True,
)
# 참고: "분석 종목명", "뉴스 종목명", "스윙 종목명"은 키보드 없이 텍스트 입력

HELP_TEXT = """
🔮 Body Hunter v3 명령어

[사전감지 + AI모니터]
  사전감지 - 폭발 직전 종목 포착 (3Gate+10신호)
  AI모니터 - 보유종목 실시간 4팩터 분석
  뉴스AI - Claude AI 뉴스 분석 (전자신문+네이버+DART)

[스윙매매]
  스윙스캔 - 4층 파이프라인 (수급+기술+이상거래→TOP10)
  이상거래 - 이상거래 감지기 (조용한 매집/큰손 포착)
  건전성 - 시장 수급 건전성 진단
  이벤트 - DART+뉴스 이벤트 감지
  해외이벤트 - 해외 실적발표/경제지표 D-3 알림
  종목선정 - 7팩터 스윙 종목 선정 (TOP 10)
  MACD스캔 - MACD 제로선 크로스 + 수급폭발 스캔
  스윙 삼성전자 - 개별 종목 스윙 분석
  워치리스트 - 최근 스윙 워치리스트

[릴레이 에이전트]
  섹터릴레이 - 12섹터 순환 감지 (HOT/RELAY)
  그룹릴레이 - 7대 그룹 순환 감지
  ETF릴레이 - ETF→종목 후행 감지
  릴레이종합 - 3개 에이전트 교차 검증 통합

[내일 추천]
  내일추천 - 5단계 파이프라인 추천 조회/실행
  (자동 16:45 저녁분석 → 06:30 미국장체크 → 08:50 최종확인)

[국적별 수급]
  국적수급 - 추천/보유 종목 외국인 국적별 매매
  국적수급 SK하이닉스 - 특정 종목 국적별 조회

[분석]
  스캔 - 5D 전종목 수급 스캔
  분석 삼성전자 - 개별 종목 6D 분석
  뉴스 삼성전자 - 뉴스 + Grok 감성분석

[매매]
  매수 삼성전자 10 - 시장가 매수
  매도 삼성전자 - 전량 매도
  청산 - 전종목 청산
  현재잔고 - 계좌 잔고
  체결내역 - 당일 체결
  포트폴리오 - 보유 + 손익

[자동매매]
  시작 - 자동매매 ON
  정지 - 자동매매 OFF
  자동확인 - 대기 중 자동매수 실행
  자동취소 - 대기 중 자동매수 취소
  위기모드 [사유] - 매수 완전 차단
  위기해제 - 위기 모드 해제

[JARVIS BRAIN 자본 배분]
  배분현황 - 현재 BRAIN 자본 배분 지시 조회
  로테이션 - 섹터 로테이션 분석 (HOT/STAGING/다음섹터)
  선행지표 - 채권시장 선행지표 (레짐 전환 감지)
  스트레스 - 크로스에셋 상관관계 붕괴 감지
  COT - CFTC 스마트머니 포지션 (주간)
  유동성 - FRED 유동성 사이클 (RRP/TGA/M2)
  (자동: NIGHTWATCH 완료 후 배분 갱신)

[NIGHTWATCH NXT 야간매매]
  NXT - NIGHTWATCH 상태 + NXT 포지션
  NXT실행 - NIGHTWATCH 즉시 실행 (테스트)
  NXT켜기 - NXT 자동매매 ON
  NXT끄기 - NXT 알림만 모드
  (자동 16:00 수집 → 16:35 판단 → 08:00 매도)

[복기]
  일지 - 오늘 매매 일지
  일지 2026-02-18 - 특정일 일지

[시그널]
  시그널 - 일간 1D~4D 시그널 요약
  (자동 16:30 - 전종목 시그널 기록)

[시나리오]
  시나리오 - 매크로 테마 시나리오 목록
  시나리오활성 ID - 테마 ACTIVE 전환
  시나리오대기 ID - 테마 WATCH 전환
  시나리오삭제 ID - 테마 삭제

[보유종목 모니터]
  상태판 - 보유종목 1줄 요약 대시보드
  (자동 10:00, 13:00, 14:30 발송)
  (이상 감지 시 자동 알림 푸시)

[데이터]
  분봉수집 - 당일 5분/15분봉 수집 (자동 15:40)
  유니버스 - 유니버스 종목 현황
  유니버스갱신 - 시총 1000억+ 리빌드

[시스템]
  상태 - 봇 상태
  로그 - 최근 로그
  도움 - 이 메시지
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
        """보유종목 대시보드 정기 발송 (장중 3회)"""
        from datetime import date as _date
        if _date.today().weekday() >= 5:
            return
        chat_id = os.getenv("TELEGRAM_CHAT_ID")
        try:
            from bot.portfolio_monitor import generate_dashboard
            text = await asyncio.to_thread(generate_dashboard, self.trader)
            await context.bot.send_message(chat_id=chat_id, text=text)
            logger.info("보유종목 대시보드 발송 완료")
        except Exception as e:
            logger.error(f"보유종목 대시보드 실패: {e}")

    async def _job_portfolio_alert(self, context):
        """보유종목 이상 감지 (장중 60초 간격)"""
        from datetime import date as _date
        now = datetime.now()
        # 장중만 동작 (09:00~15:30, 주말 제외)
        if _date.today().weekday() >= 5:
            return
        now_min = now.hour * 60 + now.minute
        if now_min < 540 or now_min >= 930:  # 09:00=540, 15:30=930
            return

        try:
            from bot.portfolio_monitor import check_alerts, format_alerts

            # prev_states 보존 (context.bot_data에 저장)
            prev = context.bot_data.get("portfolio_alert_states", {})
            alerts, new_states = await asyncio.to_thread(
                check_alerts, self.trader, None, prev
            )
            context.bot_data["portfolio_alert_states"] = new_states

            if alerts:
                chat_id = os.getenv("TELEGRAM_CHAT_ID")
                msg = format_alerts(alerts)
                await context.bot.send_message(chat_id=chat_id, text=msg)
                logger.info(f"보유종목 알림 발송: {len(alerts)}건")
        except Exception as e:
            logger.error(f"보유종목 알림 체크 실패: {e}")

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
                stock = resolve_stock(args[0])
                if not stock:
                    await update.message.reply_text(f"종목 '{args[0]}' 찾을 수 없음")
                    return
                targets = [{"code": stock["code"], "name": stock["name"]}]
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

    async def cmd_watchlist(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """워치리스트 조회"""
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
                msg = format_recommendation(report)
                for chunk in _split_message(msg):
                    await update.message.reply_text(chunk)
            else:
                await update.message.reply_text("저장된 추천 없음 - 5단계 분석 즉시 실행 중... (3~5분)")
                report = await asyncio.to_thread(run_evening_recommendation)
                save_recommendation(report)
                msg = format_recommendation(report)
                for chunk in _split_message(msg):
                    await update.message.reply_text(chunk)
        except Exception as e:
            logger.error(f"추천 조회 실패: {e}", exc_info=True)
            await update.message.reply_text(f"❌ 추천 조회 실패: {e}")

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

    # ═══════════════════════════════════════
    #  봇 빌드 & 실행
    # ═══════════════════════════════════════

    async def _on_startup(self, app: Application):
        """봇 시작 시 자동매매 자동 시작 + 키보드 전송"""
        logger.info("봇 초기화 완료 - 자동매매 자동 시작")

        chat_id = int(self.chat_id)

        # 자동매매 자동 시작 (auto_trade: true일 때)
        if self.config.get("bot", {}).get("auto_trade", False):
            async def _send_alert(text):
                await app.bot.send_message(chat_id=chat_id, text=text)

            self.auto_trader.start(_send_alert)
            bot_conf = self.config.get("bot", {})
            startup_msg = (
                "🔮 Body Hunter v4 봇 시작\n"
                "━" * 20 + "\n"
                "🟢 자동매매 자동 시작됨\n"
                f"아침 스캔: {bot_conf.get('morning_scan_time', '09:00')}\n"
                f"감시 주기: {bot_conf.get('scan_interval_sec', 30)}초\n"
                f"최대 보유: {bot_conf.get('max_auto_positions', 2)}종목\n"
                f"분할매수: {self.config.get('risk', {}).get('split_count', 3)}회\n"
                f"확인 모드: {'ON' if bot_conf.get('confirm_real_order') else 'OFF'}\n"
                "━" * 20 + "\n"
                "정지: '정지' | 수동 재시작: '시작'"
            )
        else:
            startup_msg = (
                "🔮 Body Hunter v4 봇 시작됨\n"
                "자동매매 OFF - '시작' 입력으로 켜기"
            )

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
            r"^위기모드": self.cmd_crisis_on,
            r"^위기해제$": self.cmd_crisis_off,
            r"^사전감지$": self.cmd_premove_scan,
            r"^AI모니터$": self.cmd_ai_monitor,
            r"^뉴스AI$": self.cmd_news_ai,
            r"^스윙스캔$": self.cmd_swing_scan,
            r"^이상거래$": self.cmd_volume_scan,
            r"^이벤트$": self.cmd_event_scan,
            r"^워치리스트$": self.cmd_watchlist,
            r"^건전성$": self.cmd_market_health,
            r"^해외이벤트$": self.cmd_global_event,
            r"^종목선정$": self.cmd_swing_pick,
            r"^MACD스캔$": self.cmd_macd_scan,
            r"^시나리오$": self.cmd_scenario_list,
            r"^섹터릴레이$": self.cmd_sector_relay,
            r"^그룹릴레이$": self.cmd_group_relay,
            r"^ETF릴레이$": self.cmd_etf_relay,
            r"^릴레이종합$": self.cmd_relay_hub,
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

        # 유니버스 리빌드 (08:30)
        uni_str = bot_conf.get("universe_rebuild_time", "08:30")
        h5, m5 = map(int, uni_str.split(":"))
        jq.run_daily(self._job_rebuild_universe, time=kst_time(h5, m5))
        logger.info(f"유니버스 리빌드 등록: {uni_str} KST")

        # 일간 시그널 기록 (16:30 - 일봉 수집 후)
        jq.run_daily(self._job_record_signals, time=kst_time(16, 30))
        logger.info("일간 시그널 기록 등록: 16:30 KST")

        # 해외 이벤트 캘린더 스캔 (08:00 - 장 전 D-3 알림)
        jq.run_daily(self._job_global_event_scan, time=kst_time(8, 0))
        logger.info("해외 이벤트 스캔 등록: 08:00 KST")

        # 스윙 종목 선정 (16:35 - 시그널 기록 후)
        jq.run_daily(self._job_swing_picker, time=kst_time(16, 35))
        logger.info("스윙 종목 선정 등록: 16:35 KST")

        # MACD 제로선 크로스 스캔 (16:40 - 일봉+수급 수집 후)
        jq.run_daily(self._job_macd_scan, time=kst_time(16, 40))
        logger.info("MACD 크로스 스캔 등록: 16:40 KST")

        # 사전감지 스캔 (08:50 - 장 시작 전)
        jq.run_daily(self._job_premove_scan, time=kst_time(8, 50))
        logger.info("사전감지 스캔 등록: 08:50 KST")

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

        # ── JARVIS BRAIN 자본 배분 (백업용 - NIGHTWATCH 미실행 대비) ──
        jq.run_daily(self.auto_trader.job_brain_allocation, time=kst_time(16, 36))
        logger.info("BRAIN 배분 백업 등록: 16:36 KST")

        # ── NIGHTWATCH NXT 야간매매 ──
        nw_cfg = self.config.get("nightwatch", {})
        if nw_cfg.get("enabled", False):
            collect_str = nw_cfg.get("collect_time", "16:00")
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
        """장마감 후 자동 분봉(5분/15분) 수집 + 수급 분석"""
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
            await context.bot.send_message(chat_id=chat_id, text=msg)
            logger.info(f"분봉 수집 완료: {len(results)}종목")

            # ── 수급 분석 ──
            try:
                from data.minute_supply_analyzer import (
                    analyze_minute_supply,
                    format_supply_report,
                )
                logger.info("분봉 수급 분석 시작...")
                signals = await asyncio.to_thread(
                    analyze_minute_supply,
                    target_date=None,
                    universe=UNIVERSE,
                    top_n=20,
                )
                if signals:
                    report = format_supply_report(signals)
                    await context.bot.send_message(chat_id=chat_id, text=report)
                    logger.info(f"수급 분석 완료: {len(signals)}종목 발송")
                else:
                    await context.bot.send_message(
                        chat_id=chat_id,
                        text="📊 수급 분석: 특이 종목 없음 (vol_ratio < 1.5x)",
                    )
            except Exception as e:
                logger.error(f"수급 분석 실패: {e}")
                await context.bot.send_message(
                    chat_id=chat_id,
                    text=f"⚠️ 수급 분석 실패: {str(e)[:200]}",
                )

        except Exception as e:
            logger.error(f"분봉 수집 실패: {e}")
            await context.bot.send_message(
                chat_id=chat_id, text=f"⚠️ 분봉 수집 실패: {str(e)[:200]}"
            )

    async def _job_collect_daily(self, context):
        """장마감 후 일봉(pykrx) + 수급(pykrx) 데이터 수집

        force=True로 캐시 무시 - 당일 데이터 확실히 갱신
        NOTE: KIS 일봉은 영문 컬럼(open/close)이라 한글 컬럼(시가/종가) CSV를
              덮어쓰는 버그가 있어 제거함. pykrx만 사용.
        """
        from datetime import date
        if date.today().weekday() >= 5:
            return
        logger.info("일봉+수급 자동 수집 시작...")
        chat_id = os.getenv("TELEGRAM_CHAT_ID")
        t0 = time.time()

        # 초기값 (수집 완료 기록용)
        pykrx_cnt = 0
        r1 = []
        ok, fail, sync_cnt = 0, 0, 0

        # 1. 일봉 pykrx (한글 컬럼: 시가/고가/저가/종가/거래량)
        try:
            from data.universe_builder import collect_daily_pykrx
            from data.kis_collector import UNIVERSE

            codes = list(UNIVERSE.keys())
            await context.bot.send_message(
                chat_id=chat_id,
                text=f"📈 일봉+수급 수집 시작: {len(codes)}종목 (force=True)",
            )

            pykrx_cnt = await asyncio.to_thread(
                collect_daily_pykrx, codes, 24, True
            )
            logger.info(f"pykrx 일봉 수집 완료: {pykrx_cnt}종목")

        except Exception as e:
            logger.error(f"pykrx 일봉 수집 실패: {e}")
            await context.bot.send_message(
                chat_id=chat_id, text=f"⚠️ pykrx 일봉 실패: {str(e)[:200]}"
            )

        await context.bot.send_message(
            chat_id=chat_id,
            text=f"📈 일봉 수집 완료: {pykrx_cnt}종목",
        )

        # 2. 수급 데이터 (pykrx - 투자자순매수, 외인소진율, 공매도) force=True
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

            elapsed = int(time.time() - t0)
            cnt = (len(r1), len(r2), len(r3), len(r4))
            await context.bot.send_message(
                chat_id=chat_id,
                text=(
                    f"📊 수급 수집 완료 ({elapsed}초)\n"
                    f"  투자자: {cnt[0]} | 외인소진: {cnt[1]}\n"
                    f"  공매도잔고: {cnt[2]} | 공매도거래량: {cnt[3]}"
                ),
            )
            logger.info(f"수급 수집 완료: {cnt}")

        except Exception as e:
            logger.error(f"수급 수집 실패: {e}")
            await context.bot.send_message(
                chat_id=chat_id, text=f"⚠️ 수급 수집 실패: {str(e)[:200]}"
            )

        # 3. 외국인 국적별 수급 (추천/보유 종목만 - HTTP JSON API)
        try:
            nat_codes = self._get_nationality_targets()
            if nat_codes:
                from data.krx_nationality_crawler import afetch_nationality_batch
                await context.bot.send_message(
                    chat_id=chat_id,
                    text=f"🌍 국적별 수급 수집: {len(nat_codes)}종목...",
                )
                date_from = (datetime.now() - timedelta(days=5)).strftime("%Y%m%d")
                date_to = datetime.now().strftime("%Y%m%d")
                nat_results = await afetch_nationality_batch(
                    nat_codes, date_from, date_to,
                )
                nat_ok = sum(1 for df in nat_results.values() if not df.empty)
                await context.bot.send_message(
                    chat_id=chat_id,
                    text=f"🌍 국적별 수급 완료: {nat_ok}/{len(nat_codes)}종목",
                )
                logger.info(f"국적별 수급 수집: {nat_ok}/{len(nat_codes)}")
        except Exception as e:
            logger.error(f"국적별 수급 실패: {e}")
            await context.bot.send_message(
                chat_id=chat_id, text=f"⚠️ 국적별 수급 실패: {str(e)[:200]}"
            )

        # 4. Parquet 통합 빌드 (CSV → raw parquet → processed parquet)
        try:
            from data.extend_parquet_data import extend_parquet_all
            await context.bot.send_message(
                chat_id=chat_id, text="📦 Parquet 통합 빌드 시작..."
            )
            t1 = time.time()
            ok, fail = await asyncio.to_thread(
                lambda: extend_parquet_all(codes=None, force=True)
            )
            elapsed_pq = int(time.time() - t1)
            await context.bot.send_message(
                chat_id=chat_id,
                text=f"📦 Parquet 빌드 완료 ({elapsed_pq}초): 성공 {ok} / 실패 {fail}",
            )
            logger.info(f"Parquet 빌드 완료: ok={ok}, fail={fail}")
        except Exception as e:
            logger.error(f"Parquet 빌드 실패: {e}")
            await context.bot.send_message(
                chat_id=chat_id, text=f"⚠️ Parquet 빌드 실패: {str(e)[:200]}"
            )

        # 5. stock_data_daily 동기화
        try:
            from collect_all import step5_sync_stock_data_daily
            sync_cnt = await asyncio.to_thread(step5_sync_stock_data_daily)
            logger.info(f"stock_data_daily 동기화 완료: {sync_cnt}종목")
        except Exception as e:
            logger.error(f"stock_data_daily 동기화 실패: {e}")

        # 6. 수집 완료 기록
        try:
            import json as _json
            collect_info = {
                "date": date.today().strftime("%Y-%m-%d"),
                "source": "bot",
                "steps": {
                    "daily": pykrx_cnt,
                    "flow": len(r1) if r1 else 0,
                    "parquet": ok,
                    "sync": sync_cnt,
                },
            }
            lc_path = Path(__file__).parent.parent / "data_store" / "_last_collect.json"
            with open(lc_path, "w", encoding="utf-8") as f:
                _json.dump(collect_info, f, ensure_ascii=False, indent=2)
        except Exception:
            pass

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
            if self.auto_trader and hasattr(self.auto_trader, "positions"):
                for code in self.auto_trader.positions.keys():
                    codes.add(code)
        except Exception:
            pass

        return list(codes) if codes else []

    async def cmd_nationality(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """국적수급 - 추천/보유 종목 외국인 국적별 수급 변화 보고"""
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
        """일간 1D~4D 시그널 기록 (16:30 - 일봉 수집 완료 후)"""
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

    async def _job_global_event_scan(self, context):
        """해외 이벤트 캘린더 스캔 (08:00 - D-3 알림)"""
        from datetime import date
        if date.today().weekday() >= 5:
            return

        logger.info("해외 이벤트 캘린더 스캔 시작...")
        chat_id = os.getenv("TELEGRAM_CHAT_ID")

        try:
            from data.global_event_calendar import scan_global_events, format_telegram_message
            result = await asyncio.to_thread(scan_global_events)
            msg = format_telegram_message(result)

            for chunk in _split_message(msg):
                await context.bot.send_message(chat_id=chat_id, text=chunk)

            logger.info("해외 이벤트 스캔 완료")

        except Exception as e:
            logger.error(f"해외 이벤트 스캔 실패: {e}", exc_info=True)
            await context.bot.send_message(
                chat_id=chat_id, text=f"⚠️ 해외 이벤트 스캔 실패: {str(e)[:200]}"
            )

    async def _job_swing_picker(self, context):
        """스윙 종목 선정 (16:35 - 시그널 기록 후)"""
        from datetime import date
        if date.today().weekday() >= 5:
            return

        logger.info("스윙 종목 선정 시작...")
        chat_id = os.getenv("TELEGRAM_CHAT_ID")

        try:
            from data.swing_picker import run_picker, format_telegram_message as fmt_swing
            result = await asyncio.to_thread(run_picker)
            msg = fmt_swing(result)

            for chunk in _split_message(msg):
                await context.bot.send_message(chat_id=chat_id, text=chunk)

            n = len(result.get("candidates", []))
            logger.info(f"스윙 종목 선정 완료: {n}종목")

        except Exception as e:
            logger.error(f"스윙 종목 선정 실패: {e}", exc_info=True)
            await context.bot.send_message(
                chat_id=chat_id, text=f"⚠️ 스윙 종목 선정 실패: {str(e)[:200]}"
            )

    async def _job_macd_scan(self, context):
        """MACD 제로선 크로스 스캔 (16:40 - 일봉+수급 수집 후)"""
        from datetime import date
        if date.today().weekday() >= 5:
            return

        logger.info("MACD 크로스 스캔 시작...")
        chat_id = os.getenv("TELEGRAM_CHAT_ID")

        try:
            from strategies.macd_zero_scanner import (
                run_daily_scan,
                format_telegram_message as fmt_macd,
            )
            result = await asyncio.to_thread(run_daily_scan)
            msg = fmt_macd(result)

            for chunk in _split_message(msg):
                await context.bot.send_message(chat_id=chat_id, text=chunk)

            p1 = len(result.get("phase1_new", []))
            p2 = len(result.get("phase2_entries", []))
            logger.info(f"MACD 스캔 완료: 신규{p1} 진입{p2}")

        except Exception as e:
            logger.error(f"MACD 스캔 실패: {e}", exc_info=True)
            await context.bot.send_message(
                chat_id=chat_id, text=f"MACD 스캔 실패: {str(e)[:200]}"
            )

    async def _job_premove_scan(self, context):
        """사전감지 스캔 (08:50 - 장 시작 전)"""
        from datetime import date
        if date.today().weekday() >= 5:
            return

        logger.info("사전감지 스캔 시작...")
        chat_id = os.getenv("TELEGRAM_CHAT_ID")

        try:
            from data.premove_scanner import scan_premove, format_premove_report, save_premove_candidates
            candidates = await asyncio.to_thread(scan_premove, 5)
            report = format_premove_report(candidates)

            for chunk in _split_message(report):
                await context.bot.send_message(chat_id=chat_id, text=chunk)

            if candidates:
                await asyncio.to_thread(save_premove_candidates, candidates)

            logger.info(f"사전감지 완료: {len(candidates)}개 후보")

        except Exception as e:
            logger.error(f"사전감지 실패: {e}", exc_info=True)
            await context.bot.send_message(
                chat_id=chat_id, text=f"사전감지 실패: {str(e)[:200]}"
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
