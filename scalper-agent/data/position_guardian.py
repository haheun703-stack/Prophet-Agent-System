# -*- coding: utf-8 -*-
"""
Position Guardian - 수급 변곡점 기반 포지션 관리자
==================================================
"가격이 떨어졌다"가 아니라 "돈이 빠지고 있다"를 감지한다.

4가지 선행 시그널:
1. SUPPLY_DRAIN  - 외국인/기관 순매수 반전 (국적 크롤러)
2. TV_FADEOUT    - 거래대금 고갈 (TV 스캐너)
3. MOMENTUM_DEATH - 상대강도 소멸 (시장 대비)
4. PRICE_CRACK   - 가격 구조 붕괴 (지지선 이탈)

최종 판단: HOLD / REDUCE / EXIT / TAKE_PROFIT

실행: python data/position_guardian.py
텔레그램: 매일 08:30 자동 전송
"""

import os
import sys
import json
import csv
import logging
import requests
from pathlib import Path
from datetime import datetime, date, timedelta
from dataclasses import dataclass, field, asdict
from typing import Dict, List, Optional, Tuple
from dotenv import load_dotenv

# ── 경로 ──
BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

load_dotenv(BASE_DIR.parent / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("BH.Guardian")

TG_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TG_CHAT = os.getenv("TELEGRAM_CHAT_ID")

STORE_DIR = BASE_DIR / "data_store"
DAILY_DIR = STORE_DIR / "daily"


def tg_send(text: str):
    if not TG_TOKEN or not TG_CHAT:
        print(text)
        return
    try:
        url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
        requests.post(url, json={"chat_id": TG_CHAT, "text": text}, timeout=10)
    except Exception as e:
        logger.error(f"텔레그램 전송 실패: {e}")


# ═══════════════════════════════════════════
#  데이터 클래스
# ═══════════════════════════════════════════
@dataclass
class GuardianSignal:
    """개별 시그널"""
    name: str           # SUPPLY_DRAIN / TV_FADEOUT / MOMENTUM_DEATH / PRICE_CRACK
    score: float        # 0~100 (높을수록 위험)
    severity: str       # SAFE / WARNING / DANGER / CRITICAL
    detail: str         # 설명


@dataclass
class PositionVerdict:
    """종목별 최종 판단"""
    code: str
    name: str
    current_price: int
    change_pct: float
    action: str         # HOLD / REDUCE / EXIT / TAKE_PROFIT
    risk_score: float   # 0~100 종합 위험도
    signals: List[GuardianSignal] = field(default_factory=list)
    key_reason: str = ""
    entry_price: int = 0
    tp1: int = 0
    sl: int = 0
    pnl_pct: float = 0.0


# ═══════════════════════════════════════════
#  Signal 1: 수급 이탈 감지 (SUPPLY_DRAIN)
# ═══════════════════════════════════════════
def _check_supply_drain(code: str, name: str) -> GuardianSignal:
    """외국인/기관 순매수 반전 감지

    데이터 소스:
    - nationality_profiler.predict_tomorrow_flow() → 내일 수급 예측
    - nationality_signal.compare_nationality() → 전일 대비 변화
    - NationalityPower.decay_factor → 시그널 소멸 속도
    """
    score = 0
    details = []

    # A. 수급 예측 (predict_tomorrow_flow)
    try:
        from data.nationality_profiler import predict_tomorrow_flow, calc_nationality_power, collect_daily_series
        from data.nationality_signal import compare_nationality

        # 내일 흐름 예측
        predictions = predict_tomorrow_flow(
            codes=[code], n_days=5,
        )
        if predictions:
            pred = predictions[0]
            sig = pred.get("signal", "NEUTRAL")
            pred_score = pred.get("score", 0)

            if sig == "SELL":
                score += 40
                details.append(f"수급예측: SELL({pred_score:.0f})")
            elif sig == "CAUTION":
                score += 25
                details.append(f"수급예측: CAUTION({pred_score:.0f})")
            elif sig == "NEUTRAL":
                score += 5
            elif sig in ("BUY", "STRONG_BUY"):
                score -= 10  # 긍정적 → 위험도 감소

            # 리스크 경고
            risk = pred.get("risk")
            if risk:
                score += 10
                details.append(f"리스크: {risk}")

        # B. NationalityPower 분석
        daily_data = collect_daily_series([code], n_days=5)
        price_data = _get_price_data(code)
        powers = calc_nationality_power(
            [code], daily_data=daily_data,
            price_data={code: price_data},
            all_codes_data=daily_data, n_days=5,
        )
        np_result = powers.get(code)
        if np_result:
            # decay_factor < 0.7 → 매수세 소멸 중
            if np_result.decay_factor < 0.5:
                score += 20
                details.append(f"매수세 급소멸(decay={np_result.decay_factor:.2f})")
            elif np_result.decay_factor < 0.7:
                score += 10
                details.append(f"매수세 약화(decay={np_result.decay_factor:.2f})")

            # ghost_countries → 핵심 매수국 이탈
            if np_result.ghost_countries:
                ghost_names = np_result.ghost_countries[:3]
                score += 15
                details.append(f"이탈국: {','.join(ghost_names)}")

            # grade 기반
            if np_result.grade == "DANGER":
                score += 15
                details.append("국적파워: DANGER")
            elif np_result.grade == "CAUTION":
                score += 8

        # C. 전일 대비 국적별 변화
        changes = compare_nationality(code)
        if changes:
            inst_chg = sum(c["변화"] for c in changes if c["분류"] == "기관")
            hedge_chg = sum(c["변화"] for c in changes if c["분류"] == "헤지펀드")
            asia_chg = sum(c["변화"] for c in changes if c["분류"] == "아시아")

            # 기관 + 헤지 동반 매도
            if inst_chg < 0 and hedge_chg < 0:
                score += 20
                details.append(f"기관+헤지 동반매도")
            elif inst_chg < 0:
                score += 10
                details.append(f"기관 순매도")

    except Exception as e:
        logger.debug(f"수급 분석 실패({code}): {e}")
        details.append("데이터 부족")

    score = max(0, min(100, score))
    severity = _score_to_severity(score)
    return GuardianSignal(
        name="SUPPLY_DRAIN",
        score=score,
        severity=severity,
        detail=" | ".join(details) if details else "정상",
    )


# ═══════════════════════════════════════════
#  Signal 2: 거래대금 고갈 (TV_FADEOUT)
# ═══════════════════════════════════════════
def _check_tv_fadeout(code: str, name: str) -> GuardianSignal:
    """거래대금이 줄어드는 것 = 관심이 사라지는 것"""
    score = 0
    details = []

    try:
        # TV 스캐너 최신 결과
        tv_path = STORE_DIR / "tv_scanner.json"
        if tv_path.exists():
            tv_data = json.loads(tv_path.read_text("utf-8"))
            signals = tv_data.get("signals", [])
            tv_sig = None
            for s in signals:
                if s.get("code") == code:
                    tv_sig = s
                    break

            if tv_sig:
                tv_ratio = tv_sig.get("tv_ratio", 1.0)
                pattern = tv_sig.get("pattern", "NORMAL")
                tv_trend = tv_sig.get("tv_ratio_5d_trend", 0)

                # tv_ratio 하락 추세
                if tv_trend < -0.3:
                    score += 25
                    details.append(f"거래대금 급감(추세:{tv_trend:.2f})")
                elif tv_trend < -0.1:
                    score += 15
                    details.append(f"거래대금 감소(추세:{tv_trend:.2f})")

                # tv_ratio가 낮으면 관심 부족
                if tv_ratio < 0.5:
                    score += 20
                    details.append(f"거래대금 고갈(x{tv_ratio:.1f})")
                elif tv_ratio < 0.8:
                    score += 10
                    details.append(f"거래대금 부진(x{tv_ratio:.1f})")

                # QUIET_ACC/EXPLOSION 소멸
                if pattern == "NORMAL" and tv_ratio < 1.0:
                    score += 5
            else:
                # TV 스캐너에 안 잡힘 = 거래대금 미미
                score += 10
                details.append("TV 스캐너 미포착")

        # daily CSV에서 최근 거래량 추세 직접 계산
        csv_path = DAILY_DIR / f"{code}.csv"
        if csv_path.exists():
            rows = _read_daily_csv(csv_path, n=20)
            if len(rows) >= 10:
                vol_recent = [r["거래량"] for r in rows[-5:]]
                vol_prev = [r["거래량"] for r in rows[-10:-5]]
                avg_r = sum(vol_recent) / len(vol_recent) if vol_recent else 1
                avg_p = sum(vol_prev) / len(vol_prev) if vol_prev else 1
                vol_ratio = avg_r / avg_p if avg_p > 0 else 1.0

                if vol_ratio < 0.4:
                    score += 20
                    details.append(f"거래량 급감(x{vol_ratio:.2f})")
                elif vol_ratio < 0.6:
                    score += 10
                    details.append(f"거래량 위축(x{vol_ratio:.2f})")

    except Exception as e:
        logger.debug(f"TV 분석 실패({code}): {e}")

    score = max(0, min(100, score))
    severity = _score_to_severity(score)
    return GuardianSignal(
        name="TV_FADEOUT",
        score=score,
        severity=severity,
        detail=" | ".join(details) if details else "정상",
    )


# ═══════════════════════════════════════════
#  Signal 3: 모멘텀 소멸 (MOMENTUM_DEATH)
# ═══════════════════════════════════════════
def _check_momentum_death(code: str, name: str) -> GuardianSignal:
    """시장 대비 상대강도 약화 감지"""
    score = 0
    details = []

    try:
        csv_path = DAILY_DIR / f"{code}.csv"
        # KOSPI 대용: 삼성전자(005930) 또는 KODEX200(069500)
        kospi_path = DAILY_DIR / "069500.csv"
        if not kospi_path.exists():
            kospi_path = DAILY_DIR / "005930.csv"

        stock_rows = _read_daily_csv(csv_path, n=20) if csv_path.exists() else []
        kospi_rows = _read_daily_csv(kospi_path, n=20) if kospi_path.exists() else []

        if len(stock_rows) >= 10 and len(kospi_rows) >= 10:
            # 최근 5일 수익률 vs 시장
            stock_5d = sum(r["등락률"] for r in stock_rows[-5:])
            kospi_5d = sum(r["등락률"] for r in kospi_rows[-5:])
            relative_5d = stock_5d - kospi_5d

            # 최근 10일 수익률 vs 시장
            stock_10d = sum(r["등락률"] for r in stock_rows[-10:])
            kospi_10d = sum(r["등락률"] for r in kospi_rows[-10:])
            relative_10d = stock_10d - kospi_10d

            # 상대강도 하락
            if relative_5d < -5:
                score += 30
                details.append(f"5일 상대강도 급락({relative_5d:+.1f}%)")
            elif relative_5d < -3:
                score += 20
                details.append(f"5일 상대강도 하락({relative_5d:+.1f}%)")
            elif relative_5d < -1:
                score += 10
                details.append(f"5일 상대약세({relative_5d:+.1f}%)")

            if relative_10d < -8:
                score += 20
                details.append(f"10일 상대강도 급락({relative_10d:+.1f}%)")
            elif relative_10d < -5:
                score += 10

            # 연속 하락일 수
            consecutive_down = 0
            for r in reversed(stock_rows[-10:]):
                if r["등락률"] < 0:
                    consecutive_down += 1
                else:
                    break
            if consecutive_down >= 5:
                score += 25
                details.append(f"연속 {consecutive_down}일 하락")
            elif consecutive_down >= 3:
                score += 15
                details.append(f"연속 {consecutive_down}일 하락")

    except Exception as e:
        logger.debug(f"모멘텀 분석 실패({code}): {e}")

    score = max(0, min(100, score))
    severity = _score_to_severity(score)
    return GuardianSignal(
        name="MOMENTUM_DEATH",
        score=score,
        severity=severity,
        detail=" | ".join(details) if details else "정상",
    )


# ═══════════════════════════════════════════
#  Signal 4: 가격 구조 붕괴 (PRICE_CRACK)
# ═══════════════════════════════════════════
def _check_price_crack(code: str, name: str, entry: int = 0, sl: int = 0) -> GuardianSignal:
    """지지선 이탈, 하락 구조 전환 감지"""
    score = 0
    details = []

    try:
        csv_path = DAILY_DIR / f"{code}.csv"
        rows = _read_daily_csv(csv_path, n=20) if csv_path.exists() else []

        if len(rows) >= 5:
            current = rows[-1]["종가"]
            prices = [r["종가"] for r in rows]

            # MA5, MA20 위치
            ma5 = sum(prices[-5:]) / 5
            ma20 = sum(prices[-20:]) / min(20, len(prices))

            # 역배열 (현재 < MA5 < MA20)
            if current < ma5 < ma20:
                score += 20
                details.append("역배열(하락추세)")
            elif current < ma5:
                score += 10

            # 최근 20일 최저가 대비 위치
            low_20d = min(r["저가"] for r in rows)
            high_20d = max(r["고가"] for r in rows)
            price_range = high_20d - low_20d if high_20d > low_20d else 1
            position = (current - low_20d) / price_range

            if position < 0.1:
                score += 25
                details.append(f"20일 최저가 근접({position:.0%})")
            elif position < 0.25:
                score += 15
                details.append(f"20일 하단({position:.0%})")

            # 진입가 대비 손실
            if entry > 0:
                pnl = (current - entry) / entry * 100
                if pnl < -10:
                    score += 25
                    details.append(f"진입가 대비 {pnl:.1f}%")
                elif pnl < -5:
                    score += 15
                    details.append(f"진입가 대비 {pnl:.1f}%")

            # SL 근접
            if sl > 0:
                sl_margin = (current - sl) / current * 100
                if sl_margin < 2:
                    score += 30
                    details.append(f"SL 임박!(여유 {sl_margin:.1f}%)")
                elif sl_margin < 4:
                    score += 15
                    details.append(f"SL 근접(여유 {sl_margin:.1f}%)")

            # 하락폭 확대 패턴 (Lower lows)
            if len(rows) >= 10:
                lows_first = [r["저가"] for r in rows[-10:-5]]
                lows_recent = [r["저가"] for r in rows[-5:]]
                if min(lows_recent) < min(lows_first):
                    score += 10
                    details.append("Lower Low 형성")

    except Exception as e:
        logger.debug(f"가격 분석 실패({code}): {e}")

    score = max(0, min(100, score))
    severity = _score_to_severity(score)
    return GuardianSignal(
        name="PRICE_CRACK",
        score=score,
        severity=severity,
        detail=" | ".join(details) if details else "정상",
    )


# ═══════════════════════════════════════════
#  종합 판단
# ═══════════════════════════════════════════
# 시그널 가중치: 수급이 가장 중요 (선행 지표)
SIGNAL_WEIGHTS = {
    "SUPPLY_DRAIN": 0.35,    # 수급 이탈 (가장 중요 - 원인)
    "TV_FADEOUT": 0.25,      # 거래대금 고갈 (선행)
    "MOMENTUM_DEATH": 0.20,  # 모멘텀 소멸 (동행)
    "PRICE_CRACK": 0.20,     # 가격 구조 (후행이지만 확인용)
}


def evaluate_position(
    code: str, name: str,
    current_price: int = 0, change_pct: float = 0.0,
    entry: int = 0, tp1: int = 0, sl: int = 0,
) -> PositionVerdict:
    """단일 종목 포지션 평가"""
    logger.info(f"[Guardian] {name}({code}) 평가 시작...")

    # 4개 시그널 수집
    sig1 = _check_supply_drain(code, name)
    sig2 = _check_tv_fadeout(code, name)
    sig3 = _check_momentum_death(code, name)
    sig4 = _check_price_crack(code, name, entry, sl)

    signals = [sig1, sig2, sig3, sig4]

    # 가중 평균 위험도
    risk_score = sum(
        s.score * SIGNAL_WEIGHTS.get(s.name, 0.25)
        for s in signals
    )
    risk_score = min(100, risk_score)

    # PNL 계산
    pnl_pct = 0.0
    if entry > 0 and current_price > 0:
        pnl_pct = (current_price - entry) / entry * 100

    # TP1 도달 체크
    if tp1 > 0 and current_price >= tp1:
        action = "TAKE_PROFIT"
        key_reason = f"TP1({tp1:,}) 도달! 절반 익절 권장"
    # 최종 액션 결정
    elif risk_score >= 60:
        action = "EXIT"
        # 가장 위험한 시그널 찾기
        worst = max(signals, key=lambda s: s.score)
        key_reason = f"{worst.name}: {worst.detail}"
    elif risk_score >= 35:
        action = "REDUCE"
        danger_signals = [s for s in signals if s.severity in ("DANGER", "CRITICAL")]
        if danger_signals:
            key_reason = " / ".join(f"{s.name}:{s.detail}" for s in danger_signals[:2])
        else:
            key_reason = "복합 위험 요소 누적"
    else:
        action = "HOLD"
        key_reason = "수급 건전"

    verdict = PositionVerdict(
        code=code,
        name=name,
        current_price=current_price,
        change_pct=change_pct,
        action=action,
        risk_score=round(risk_score, 1),
        signals=signals,
        key_reason=key_reason,
        entry_price=entry,
        tp1=tp1,
        sl=sl,
        pnl_pct=round(pnl_pct, 1),
    )

    logger.info(
        f"  [{action}] risk={risk_score:.0f} | "
        + " | ".join(f"{s.name}:{s.score:.0f}" for s in signals)
    )
    return verdict


# ═══════════════════════════════════════════
#  전체 보유종목 평가
# ═══════════════════════════════════════════
def evaluate_all_holdings() -> List[PositionVerdict]:
    """war_relay_watchlist에서 HOLDING 종목 전체 평가"""
    import time

    wl_path = STORE_DIR / "war_relay_watchlist.json"
    if not wl_path.exists():
        logger.warning("watchlist 없음")
        return []

    wl = json.loads(wl_path.read_text("utf-8"))
    stocks = wl.get("stocks", [])

    # HOLDING 티어 종목만
    holdings = [s for s in stocks if s.get("tier") == "HOLDING"]
    if not holdings:
        logger.warning("HOLDING 종목 없음")
        return []

    # 현재가 조회
    from bot.kis_trader import KISTrader
    trader = KISTrader()

    verdicts = []
    for s in holdings:
        code = s["code"]
        name = s["name"]

        resp = trader.fetch_price(code)
        time.sleep(0.2)

        cp = resp.get("current_price", 0) if resp and resp.get("success") else 0
        chg = resp.get("change_rate", 0) if resp and resp.get("success") else 0

        verdict = evaluate_position(
            code=code, name=name,
            current_price=cp, change_pct=chg,
            entry=s.get("entry", 0),
            tp1=s.get("tp1", 0),
            sl=s.get("sl", 0),
        )
        verdicts.append(verdict)

    # 위험도 내림차순 정렬
    verdicts.sort(key=lambda v: v.risk_score, reverse=True)
    return verdicts


# ═══════════════════════════════════════════
#  텔레그램 리포트
# ═══════════════════════════════════════════
ACTION_ICONS = {
    "EXIT": "🔴",
    "REDUCE": "🟡",
    "HOLD": "🟢",
    "TAKE_PROFIT": "💰",
}


def format_guardian_report(verdicts: List[PositionVerdict]) -> str:
    """텔레그램 리포트 포맷"""
    today = date.today().strftime("%Y-%m-%d")

    lines = [
        "━━━━━━━━━━━━━━━━━━━━━━━━",
        f"🛡 Position Guardian",
        f"📅 {today} 보유종목 수급 진단",
        "━━━━━━━━━━━━━━━━━━━━━━━━",
    ]

    # 요약
    exit_cnt = sum(1 for v in verdicts if v.action == "EXIT")
    reduce_cnt = sum(1 for v in verdicts if v.action == "REDUCE")
    hold_cnt = sum(1 for v in verdicts if v.action == "HOLD")
    tp_cnt = sum(1 for v in verdicts if v.action == "TAKE_PROFIT")

    summary_parts = []
    if tp_cnt:
        summary_parts.append(f"💰익절 {tp_cnt}")
    if exit_cnt:
        summary_parts.append(f"🔴EXIT {exit_cnt}")
    if reduce_cnt:
        summary_parts.append(f"🟡REDUCE {reduce_cnt}")
    if hold_cnt:
        summary_parts.append(f"🟢HOLD {hold_cnt}")

    lines.append(f"\n{' / '.join(summary_parts)}")

    # EXIT 종목 먼저 (가장 긴급)
    for v in verdicts:
        if v.action == "EXIT":
            lines.append(f"\n🔴 EXIT: {v.name}({v.code})")
            lines.append(f"   위험도: {v.risk_score:.0f}/100")
            lines.append(f"   사유: {v.key_reason}")
            if v.pnl_pct != 0:
                lines.append(f"   손익: {v.pnl_pct:+.1f}%")
            # 시그널 상세
            for s in v.signals:
                if s.score >= 30:
                    lines.append(f"   {_severity_icon(s.severity)} {s.name}: {s.detail}")

    # REDUCE 종목
    for v in verdicts:
        if v.action == "REDUCE":
            lines.append(f"\n🟡 REDUCE: {v.name}({v.code})")
            lines.append(f"   위험도: {v.risk_score:.0f}/100 | 손익: {v.pnl_pct:+.1f}%")
            lines.append(f"   사유: {v.key_reason}")

    # TAKE_PROFIT 종목
    for v in verdicts:
        if v.action == "TAKE_PROFIT":
            lines.append(f"\n💰 TAKE PROFIT: {v.name}({v.code})")
            lines.append(f"   TP1({v.tp1:,}) 도달! 절반 익절 권장")
            lines.append(f"   손익: {v.pnl_pct:+.1f}%")

    # HOLD 종목 (간략히)
    hold_stocks = [v for v in verdicts if v.action == "HOLD"]
    if hold_stocks:
        lines.append(f"\n🟢 HOLD ({len(hold_stocks)}종목)")
        lines.append("────────────────────────")
        for v in hold_stocks:
            lines.append(f"  {v.name} risk:{v.risk_score:.0f} ({v.pnl_pct:+.1f}%)")

    lines.append("")
    lines.append("━━━━━━━━━━━━━━━━━━━━━━━━")
    lines.append("Body Hunter | Position Guardian")
    lines.append("수급 변곡점 기반 포지션 관리")

    return "\n".join(lines)


# ═══════════════════════════════════════════
#  유틸리티
# ═══════════════════════════════════════════
def _score_to_severity(score: float) -> str:
    if score >= 60:
        return "CRITICAL"
    elif score >= 40:
        return "DANGER"
    elif score >= 20:
        return "WARNING"
    return "SAFE"


def _severity_icon(severity: str) -> str:
    return {"CRITICAL": "🔴", "DANGER": "🟠", "WARNING": "🟡", "SAFE": "🟢"}.get(severity, "⚪")


def _read_daily_csv(path: Path, n: int = 20) -> list:
    """daily CSV에서 최근 N행 읽기"""
    rows = []
    try:
        with open(path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            all_rows = list(reader)
            for r in all_rows[-n:]:
                rows.append({
                    "날짜": r.get("날짜", ""),
                    "시가": int(float(r.get("시가", 0))),
                    "고가": int(float(r.get("고가", 0))),
                    "저가": int(float(r.get("저가", 0))),
                    "종가": int(float(r.get("종가", 0))),
                    "거래량": int(float(r.get("거래량", 0))),
                    "등락률": float(r.get("등락률", 0)),
                })
    except Exception:
        pass
    return rows


def _get_price_data(code: str) -> dict:
    """daily CSV에서 5일 수익률 계산"""
    csv_path = DAILY_DIR / f"{code}.csv"
    chg_5d = 0.0
    if csv_path.exists():
        rows = _read_daily_csv(csv_path, n=10)
        if len(rows) >= 5:
            c5 = rows[-5]["종가"]
            c0 = rows[-1]["종가"]
            if c5 > 0:
                chg_5d = (c0 / c5 - 1) * 100
    return {"chg_5d": chg_5d}


# ═══════════════════════════════════════════
#  메인
# ═══════════════════════════════════════════
def run():
    """Position Guardian 전체 실행"""
    logger.info("=== Position Guardian 시작 ===")

    verdicts = evaluate_all_holdings()
    if not verdicts:
        logger.warning("평가 대상 없음")
        return {"verdicts": [], "report": ""}

    report = format_guardian_report(verdicts)
    tg_send(report)

    # 결과 저장
    result_path = STORE_DIR / "learning" / "guardian_latest.json"
    result_path.parent.mkdir(parents=True, exist_ok=True)
    result_path.write_text(
        json.dumps(
            {
                "date": date.today().strftime("%Y-%m-%d"),
                "verdicts": [
                    {
                        "code": v.code, "name": v.name,
                        "action": v.action, "risk_score": v.risk_score,
                        "key_reason": v.key_reason, "pnl_pct": v.pnl_pct,
                        "signals": [
                            {"name": s.name, "score": s.score,
                             "severity": s.severity, "detail": s.detail}
                            for s in v.signals
                        ],
                    }
                    for v in verdicts
                ],
            },
            ensure_ascii=False, indent=2,
        ),
        encoding="utf-8",
    )

    logger.info("=== Position Guardian 완료 ===")
    return {"verdicts": verdicts, "report": report}


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Position Guardian - 수급 변곡점 기반 포지션 관리")
    parser.add_argument("--test", action="store_true", help="텔레그램 전송 안함")
    args = parser.parse_args()

    if args.test:
        global TG_TOKEN
        TG_TOKEN = None
        print("[TEST MODE] 텔레그램 전송 OFF")

    run()
