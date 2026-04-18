# -*- coding: utf-8 -*-
"""
VWAP 장중 모니터 — 전쟁→재건 릴레이 + 추천종목
================================================
자동매매 OFF 상태에서 장중 진입 타이밍을 텔레그램으로 알려주는 도구.

실행: python tools/vwap_monitor.py
중지: Ctrl+C

장 시간대별 동작:
  Phase 0 (08:55~09:00): 사전 준비 — 종목 로드, KIS 연결
  Phase 1 (09:00~09:30): 관찰 모드 — VWAP 형성, 변동성 측정 (알림 X)
  Phase 2 (09:30~10:30): 소강 구간 — 풀백 진입 감시 시작
  Phase 3 (10:30~12:00): 본장 — VWAP 기반 눌림목 매수 기회
  Phase 4 (12:00~14:30): 점심장 — 최적 진입 구간 (안정적)
  Phase 5 (14:30~15:30): 마감 — 정리, 최종 리포트
"""

import os
import sys
import json
import time
import logging
import requests
from pathlib import Path
from datetime import datetime, timedelta
from collections import defaultdict
from dotenv import load_dotenv

# ── 프로젝트 경로 설정 ──
BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

load_dotenv(BASE_DIR.parent / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("BH.VWAPMonitor")

# ── 텔레그램 ──
TG_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TG_CHAT = os.getenv("TELEGRAM_CHAT_ID")


def tg_send(text: str):
    """텔레그램 메시지 전송"""
    if not TG_TOKEN or not TG_CHAT:
        logger.warning("텔레그램 설정 없음 — 콘솔 출력만")
        print(text)
        return
    try:
        url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
        requests.post(url, json={"chat_id": TG_CHAT, "text": text}, timeout=10)
    except Exception as e:
        logger.error(f"텔레그램 전송 실패: {e}")


# ═══════════════════════════════════════════
#  VWAP 트래커
# ═══════════════════════════════════════════
class StockTracker:
    """개별 종목 VWAP + 가격 추적"""

    def __init__(self, code, name, entry, entry_agg, sl, tp1, tp2=0, tier="", source=""):
        self.code = code
        self.name = name
        self.entry = entry              # 이상적 진입가
        self.entry_agg = entry_agg      # 공격적 진입가
        self.sl = sl
        self.tp1 = tp1
        self.tp2 = tp2
        self.tier = tier
        self.source = source            # "war_relay" or "pipeline"

        # VWAP 누적
        self.sum_pv = 0.0               # sum(price * volume_delta)
        self.sum_vol = 0                # sum(volume_delta)
        self.vwap = 0.0

        # 가격 히스토리
        self.prev_vol = 0               # 이전 누적거래량
        self.prev_price = 0
        self.open_price = 0             # 시가 (첫 체크)
        self.high = 0
        self.low = 999_999_999
        self.prices = []                # (timestamp, price, volume) 기록

        # 알림 쿨다운 (alert_type -> last_sent_time)
        self.alert_cooldown = {}

    def update(self, current_price, acml_vol, high, low):
        """가격/거래량 업데이트 → VWAP 재계산"""
        now = datetime.now()

        if self.open_price == 0:
            self.open_price = current_price

        self.high = max(self.high, high)
        self.low = min(self.low, low)

        # 거래량 델타 계산
        vol_delta = acml_vol - self.prev_vol if self.prev_vol > 0 else 0
        if vol_delta > 0:
            self.sum_pv += current_price * vol_delta
            self.sum_vol += vol_delta
            self.vwap = self.sum_pv / self.sum_vol

        self.prev_vol = acml_vol
        self.prev_price = current_price
        self.prices.append((now, current_price, acml_vol))

    def can_alert(self, alert_type, cooldown_min=20):
        """쿨다운 체크 — 같은 알림 반복 방지"""
        now = datetime.now()
        last = self.alert_cooldown.get(alert_type)
        if last and (now - last).total_seconds() < cooldown_min * 60:
            return False
        self.alert_cooldown[alert_type] = now
        return True

    @property
    def change_pct(self):
        """시가 대비 등락률"""
        if self.open_price <= 0:
            return 0
        return (self.prev_price / self.open_price - 1) * 100

    @property
    def vwap_gap_pct(self):
        """현재가 vs VWAP 괴리율"""
        if self.vwap <= 0:
            return 0
        return (self.prev_price / self.vwap - 1) * 100

    @property
    def entry_gap_pct(self):
        """현재가 vs 이상적진입 괴리율"""
        if self.entry <= 0:
            return 0
        return (self.prev_price / self.entry - 1) * 100

    @property
    def entry_agg_gap_pct(self):
        """현재가 vs 공격적진입 괴리율"""
        if self.entry_agg <= 0:
            return 0
        return (self.prev_price / self.entry_agg - 1) * 100


# ═══════════════════════════════════════════
#  메인 모니터
# ═══════════════════════════════════════════
class VWAPMonitor:
    """장중 VWAP 모니터링 + 텔레그램 알림"""

    PHASES = {
        "PREPARE":  ("08:50", "09:00", "사전준비"),
        "OBSERVE":  ("09:00", "09:30", "관찰 모드 (VWAP 형성)"),
        "CALM":     ("09:30", "10:30", "소강 구간 (풀백 감시)"),
        "MAIN":     ("10:30", "12:00", "본장 (눌림목 기회)"),
        "LUNCH":    ("12:00", "14:30", "점심장 (최적 진입)"),
        "CLOSE":    ("14:30", "15:30", "마감 (정리)"),
    }

    def __init__(self, poll_sec=60):
        self.poll_sec = poll_sec
        self.trackers: dict[str, StockTracker] = {}
        self.trader = None
        self.advisor = None
        self.current_phase = "PREPARE"
        self.phase_alerted = set()  # 페이즈 전환 알림 중복 방지
        self.last_advisor_run = None  # 마지막 어드바이저 실행 시간

    # ── 종목 로드 ──
    def load_stocks(self):
        """워치리스트 + 추천종목 로드"""
        # 1) 전쟁→재건 릴레이
        wl_path = BASE_DIR / "data_store" / "war_relay_watchlist.json"
        if wl_path.exists():
            with open(wl_path, "r", encoding="utf-8") as f:
                wl = json.load(f)
            for s in wl.get("stocks", []):
                code = s["code"]
                self.trackers[code] = StockTracker(
                    code=code,
                    name=s["name"],
                    entry=s["entry"],
                    entry_agg=s["entry_aggressive"],
                    sl=s["sl"],
                    tp1=s["tp1"],
                    tp2=s.get("tp2", 0),
                    tier=s.get("tier", ""),
                    source="war_relay",
                )
            logger.info(f"전쟁릴레이 {len(wl.get('stocks', []))}종목 로드")

        # 2) 추천 파이프라인 (top 5)
        rec_path = BASE_DIR / "data_store" / "recommendation.json"
        if rec_path.exists():
            with open(rec_path, "r", encoding="utf-8") as f:
                rec = json.load(f)
            for s in rec.get("stocks", [])[:5]:
                code = s["code"]
                if code in self.trackers:
                    continue
                self.trackers[code] = StockTracker(
                    code=code,
                    name=s["name"],
                    entry=s["entry"],
                    entry_agg=s["entry"],  # 파이프라인은 진입가 1개
                    sl=s["sl"],
                    tp1=s["tp"],
                    source="pipeline",
                )
            logger.info(f"파이프라인 추천종목 추가 (총 {len(self.trackers)}종목)")

        logger.info(f"모니터링 대상: {len(self.trackers)}종목")

    # ── KIS 연결 ──
    def init_kis(self):
        """KIS API 트레이더 초기화"""
        from bot.kis_trader import KISTrader
        self.trader = KISTrader()
        logger.info("KIS API 연결 완료")

    # ── 어드바이저 초기화 ──
    def init_advisor(self):
        """포지션 어드바이저 초기화"""
        from strategies.position_advisor import PositionAdvisor
        self.advisor = PositionAdvisor()
        logger.info("포지션 어드바이저 연결 완료")

    # ── 페이즈 판단 ──
    def get_phase(self) -> str:
        now = datetime.now().strftime("%H:%M")
        for phase, (start, end, _) in self.PHASES.items():
            if start <= now < end:
                return phase
        if now >= "15:30":
            return "DONE"
        if now < "08:50":
            return "WAIT"
        return "PREPARE"

    # ── 가격 수집 ──
    def fetch_all_prices(self):
        """전 종목 현재가 조회"""
        for code, tk in self.trackers.items():
            try:
                resp = self.trader.fetch_price(code)
                if not resp.get("success"):
                    continue
                tk.update(
                    current_price=resp["current_price"],
                    acml_vol=resp["volume"],
                    high=resp["high"],
                    low=resp["low"],
                )
            except Exception as e:
                logger.error(f"가격 조회 실패 {code}: {e}")
            time.sleep(0.15)  # KIS API 속도제한

    # ── 알림 체크 ──
    def check_alerts(self):
        """v5.0: 익절/손절 알림만 전송 (VWAP 개별 알림 OFF)"""
        phase = self.current_phase
        if phase in ("PREPARE", "OBSERVE", "DONE", "WAIT"):
            return

        alerts = []

        for code, tk in self.trackers.items():
            cp = tk.prev_price
            if cp <= 0:
                continue

            # ── [손절주의] SL 근접 경고 ──
            if tk.sl > 0:
                sl_gap = (cp / tk.sl - 1) * 100
                if sl_gap < 2.0 and tk.can_alert("SL_NEAR", 30):
                    alerts.append(
                        f"[손절주의]\n"
                        f"{tk.name}({code}) 현재 {cp:,}원\n"
                        f"손절가 {tk.sl:,}원 (거리 {sl_gap:.1f}%)"
                    )

            # ── [익절] TP 도달 ──
            if tk.tp1 > 0 and cp >= tk.tp1 and tk.can_alert("TP_HIT", 60):
                alerts.append(
                    f"[익절]\n"
                    f"{tk.name}({code}) 현재 {cp:,}원 >= 목표 {tk.tp1:,}원\n"
                    f"등락 {tk.change_pct:+.1f}%"
                )

        # 알림 전송
        if alerts:
            msg = f"━━ {datetime.now().strftime('%H:%M')} ━━\n\n" + "\n\n".join(alerts)
            tg_send(msg)
            logger.info(f"알림 {len(alerts)}건 전송")

    # ── 페이즈 전환 알림 ──
    def notify_phase_change(self, new_phase):
        """페이즈 전환 시 현황 요약 + 어드바이저 방향"""
        if new_phase in self.phase_alerted:
            return
        self.phase_alerted.add(new_phase)

        phase_info = self.PHASES.get(new_phase, ("", "", new_phase))

        lines = [f"=== {phase_info[2]} 시작 ({phase_info[0]}~{phase_info[1]}) ===", ""]

        # 진입가 근접 종목 정렬
        near_entry = []
        for code, tk in self.trackers.items():
            if tk.prev_price <= 0:
                continue
            near_entry.append((code, tk, abs(tk.entry_gap_pct)))

        near_entry.sort(key=lambda x: x[2])

        if new_phase == "OBSERVE":
            lines.append(f"모니터링: {len(self.trackers)}종목")
            lines.append("VWAP 형성 중... 알림은 9:30부터")
        else:
            # 어드바이저 분석 (CALM 이후 페이즈에서)
            advisor_results = self.run_advisor_analysis()

            lines.append("-- 진입가 근접 순위 --")
            for code, tk, gap in near_entry[:8]:
                src = "릴" if tk.source == "war_relay" else "파"
                vwap_str = f"V{tk.vwap_gap_pct:+.1f}%" if tk.vwap > 0 else ""
                # 어드바이저 태그
                adv_tag = ""
                ar = advisor_results.get(code)
                if ar:
                    action_short = {"STRONG_BUY": "++", "BUY": "+", "HOLD": "=", "SELL": "-", "STRONG_SELL": "--"}
                    adv_tag = f" [{action_short.get(ar.action, '?')}{ar.score:+.0f}]"
                lines.append(
                    f"[{src}] {tk.name} {tk.prev_price:,}"
                    f" | 진입까지 {tk.entry_gap_pct:+.1f}%"
                    f" | 등락 {tk.change_pct:+.1f}% {vwap_str}{adv_tag}"
                )

            # 페이즈별 어드바이저 전략 가이드
            if new_phase == "CALM" and advisor_results:
                lines.append("")
                lines.append("-- 소강구간 전략 --")
                for code, r in advisor_results.items():
                    if r.action in ("STRONG_BUY", "BUY"):
                        tk = self.trackers.get(code)
                        if tk and abs(tk.entry_gap_pct) < 5:
                            lines.append(f"  주목: {r.name} 진입{tk.entry_gap_pct:+.1f}% + 방향{r.score:+.0f}")

            elif new_phase == "LUNCH" and advisor_results:
                lines.append("")
                lines.append("-- 점심장 최적진입 후보 --")
                buy_ready = [
                    (code, advisor_results[code])
                    for code in advisor_results
                    if advisor_results[code].score > 0 and code in self.trackers
                ]
                buy_ready.sort(key=lambda x: x[1].score, reverse=True)
                for code, r in buy_ready[:5]:
                    tk = self.trackers[code]
                    lines.append(f"  {r.name} 스코어{r.score:+.0f} 진입{tk.entry_gap_pct:+.1f}% ({r.confidence})")

        tg_send("\n".join(lines))

    # ── 어드바이저 분석 실행 ──
    def run_advisor_analysis(self) -> dict:
        """전 종목 어드바이저 분석 → {code: AdvisorResult}"""
        if not self.advisor:
            return {}

        results = {}
        for code, tk in self.trackers.items():
            if tk.prev_price <= 0:
                continue
            try:
                r = self.advisor.analyze(
                    code=code,
                    name=tk.name,
                    intraday_vwap=tk.vwap,
                    intraday_price=tk.prev_price,
                )
                results[code] = r
            except Exception as e:
                logger.error(f"어드바이저 분석 실패 {code}: {e}")

        self.last_advisor_run = datetime.now()
        return results

    # ── 정기 요약 (30분마다) ──
    def send_summary(self):
        """전 종목 현황 요약 + 어드바이저 방향 판단"""
        lines = [f"=== 현황 요약 {datetime.now().strftime('%H:%M')} ===", ""]

        # 어드바이저 분석 실행
        advisor_results = self.run_advisor_analysis()

        # 그룹별 정리
        war_relay = []
        pipeline = []
        for code, tk in self.trackers.items():
            if tk.prev_price <= 0:
                continue
            item = (code, tk)
            if tk.source == "war_relay":
                war_relay.append(item)
            else:
                pipeline.append(item)

        def _format_stock(code, tk, advisor_results):
            arrow = "v" if tk.change_pct < 0 else "^"
            vw = f"V{tk.vwap_gap_pct:+.1f}%" if tk.vwap > 0 else ""
            line = (
                f" {tk.name} {tk.prev_price:,}"
                f" ({tk.change_pct:+.1f}%{arrow})"
                f" 진입 {tk.entry_gap_pct:+.1f}% {vw}"
            )
            # 어드바이저 방향 태그 추가
            ar = advisor_results.get(code)
            if ar:
                action_tag = {"STRONG_BUY": "++", "BUY": "+", "HOLD": "=", "SELL": "-", "STRONG_SELL": "--"}
                tag = action_tag.get(ar.action, "?")
                line += f" [{tag}{ar.score:+.0f}]"
            return line

        if war_relay:
            lines.append("-- 전쟁릴레이 --")
            war_relay.sort(key=lambda x: abs(x[1].entry_gap_pct))
            for code, tk in war_relay:
                lines.append(_format_stock(code, tk, advisor_results))

        if pipeline:
            lines.append("")
            lines.append("-- 파이프라인 --")
            pipeline.sort(key=lambda x: abs(x[1].entry_gap_pct))
            for code, tk in pipeline:
                lines.append(_format_stock(code, tk, advisor_results))

        # 어드바이저 TOP 매수/매도 하이라이트
        if advisor_results:
            buy_candidates = [
                (code, r) for code, r in advisor_results.items()
                if r.action in ("STRONG_BUY", "BUY")
            ]
            sell_candidates = [
                (code, r) for code, r in advisor_results.items()
                if r.action in ("STRONG_SELL", "SELL")
            ]

            if buy_candidates or sell_candidates:
                lines.append("")
                lines.append("-- 어드바이저 시그널 --")
                for code, r in sorted(buy_candidates, key=lambda x: x[1].score, reverse=True):
                    lines.append(f" [매수] {r.name} 스코어{r.score:+.0f} ({r.confidence}) {r.reasons[0] if r.reasons else ''}")
                for code, r in sorted(sell_candidates, key=lambda x: x[1].score):
                    lines.append(f" [매도] {r.name} 스코어{r.score:+.0f} ({r.confidence}) {r.reasons[0] if r.reasons else ''}")

        tg_send("\n".join(lines))

    # ── 최종 리포트 ──
    def send_final_report(self):
        """장 마감 최종 리포트"""
        lines = ["=" * 30, "[ 장 마감 VWAP 리포트 ]", "=" * 30, ""]

        # 진입 기회 있었던 종목
        opportunities = []
        for code, tk in self.trackers.items():
            if tk.low <= 0 or tk.entry <= 0:
                continue
            # 장중 이상적 진입가 이하로 내려갔었는지
            touched_ideal = tk.low <= tk.entry
            touched_agg = tk.low <= tk.entry_agg
            opportunities.append((code, tk, touched_ideal, touched_agg))

        opportunities.sort(key=lambda x: (not x[2], not x[3]))

        lines.append("-- 진입 기회 분석 --")
        for code, tk, t_ideal, t_agg in opportunities:
            status = "** 이상적진입 O **" if t_ideal else ("* 공격적진입 O *" if t_agg else "미도달")
            lines.append(
                f"{tk.name}({code}) [{tk.tier}]"
            )
            lines.append(
                f"  시:{tk.open_price:,} 고:{tk.high:,} 저:{tk.low:,}"
                f" 종:{tk.prev_price:,}"
            )
            lines.append(
                f"  진입 {tk.entry:,} / 공격 {tk.entry_agg:,} → {status}"
            )
            if tk.vwap > 0:
                lines.append(f"  VWAP {tk.vwap:,.0f} | 최종 vs VWAP {tk.vwap_gap_pct:+.1f}%")
            lines.append("")

        # 어드바이저 종합 판단
        advisor_results = self.run_advisor_analysis()
        if advisor_results:
            lines.append("-- 어드바이저 종합 (내일 방향) --")
            sorted_ar = sorted(advisor_results.values(), key=lambda r: r.score, reverse=True)
            for r in sorted_ar:
                action_kr = {"STRONG_BUY": "적극매수", "BUY": "매수검토", "HOLD": "관망", "SELL": "매도검토", "STRONG_SELL": "매도"}
                a = action_kr.get(r.action, r.action)
                lines.append(f"  {r.name} -> {a} ({r.score:+.0f}, {r.confidence})")
            lines.append("")

        lines.append("-- 내일 전략 --")
        lines.append("* 오늘 미도달 종목은 진입가 유지")
        lines.append("* 터치 후 반등 종목은 내일 추격 검토")
        lines.append("* VWAP 하회 마감 종목은 추가 하락 주의")

        # 어드바이저 기반 추가 전략
        if advisor_results:
            strong_buys = [r for r in advisor_results.values() if r.action == "STRONG_BUY"]
            strong_sells = [r for r in advisor_results.values() if r.action in ("STRONG_SELL", "SELL")]
            if strong_buys:
                lines.append(f"* 매수 우선: {', '.join(r.name for r in strong_buys)}")
            if strong_sells:
                lines.append(f"* 진입 보류: {', '.join(r.name for r in strong_sells)}")

        tg_send("\n".join(lines))

    # ═══════════════════════════════════════
    #  메인 루프
    # ═══════════════════════════════════════
    def run(self):
        """메인 실행"""
        logger.info("=== VWAP 모니터 시작 ===")

        # 1. 종목 로드
        self.load_stocks()
        if not self.trackers:
            logger.error("모니터링 대상 종목 없음. 종료.")
            return

        # 2. KIS 연결
        self.init_kis()

        # 2.5. 어드바이저 연결
        try:
            self.init_advisor()
        except Exception as e:
            logger.warning(f"어드바이저 초기화 실패 (VWAP 전용 모드): {e}")

        # 3. 시작 알림
        tg_send(
            f"[VWAP Monitor ON]\n"
            f"모니터링: {len(self.trackers)}종목\n"
            f"알림 시작: 09:30~\n"
            f"폴링: {self.poll_sec}초 간격"
        )

        last_summary = datetime.now()
        cycle = 0

        # 4. 메인 루프
        while True:
            try:
                phase = self.get_phase()

                if phase == "WAIT":
                    logger.info("장 시작 전... 대기")
                    time.sleep(60)
                    continue

                if phase == "DONE":
                    logger.info("장 마감")
                    self.send_final_report()
                    break

                # 페이즈 전환 감지
                if phase != self.current_phase:
                    self.current_phase = phase
                    logger.info(f"=== Phase: {phase} ===")
                    # 첫 가격 조회 후 페이즈 알림
                    self.fetch_all_prices()
                    self.notify_phase_change(phase)

                # 가격 수집
                self.fetch_all_prices()
                cycle += 1

                # 알림 체크 (관찰 모드 이후)
                self.check_alerts()

                # 30분마다 요약
                now = datetime.now()
                if (now - last_summary).total_seconds() >= 1800:
                    self.send_summary()
                    last_summary = now

                # 콘솔 로그 (5분마다)
                if cycle % max(1, 300 // self.poll_sec) == 0:
                    n_near = sum(
                        1 for tk in self.trackers.values()
                        if tk.prev_price > 0 and abs(tk.entry_gap_pct) < 5
                    )
                    logger.info(
                        f"[{phase}] cycle={cycle} | 진입근접(<5%): {n_near}종목"
                    )

                time.sleep(self.poll_sec)

            except KeyboardInterrupt:
                logger.info("사용자 중지")
                self.send_summary()
                break
            except Exception as e:
                logger.error(f"루프 에러: {e}", exc_info=True)
                time.sleep(10)

        logger.info("=== VWAP 모니터 종료 ===")


# ═══════════════════════════════════════════
#  실행
# ═══════════════════════════════════════════
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="VWAP 장중 모니터")
    parser.add_argument("--poll", type=int, default=60, help="폴링 간격 (초, 기본 60)")
    parser.add_argument("--test", action="store_true", help="테스트 모드 (1회 조회)")
    args = parser.parse_args()

    monitor = VWAPMonitor(poll_sec=args.poll)
    monitor.load_stocks()

    if args.test:
        # 테스트: 종목 로드만 확인
        print(f"\n모니터링 대상: {len(monitor.trackers)}종목")
        for code, tk in monitor.trackers.items():
            print(f"  {tk.name}({code}) [{tk.source}] "
                  f"진입={tk.entry:,} 공격={tk.entry_agg:,} SL={tk.sl:,} TP={tk.tp1:,}")
    else:
        monitor.run()
