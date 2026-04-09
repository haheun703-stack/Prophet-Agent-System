# -*- coding: utf-8 -*-
"""
미국장 야간 필터 (US Overnight Filter)
=======================================
us_market_collector가 수집한 데이터를 분석하여
다음날 한국장 갭 방향 + 진입 모드를 결정.

입력: data_store/us_market_overnight.json
출력: data_store/us_overnight_result.json + 텔레그램 메시지

모드:
  AGGRESSIVE : 적극 진입. 수급 A+ 빠르게 선점.
  NORMAL     : 기본 필터 그대로. 수급 확인 후 진입.
  DEFENSIVE  : 조건 강화. A+만. 손절 타이트하게.
  HALT       : 진입 금지. 기존 포지션 손절 우선.
"""

import json
import logging
from datetime import date, datetime
from pathlib import Path

from data.us_relay_map import get_relay_picks

logger = logging.getLogger("BH.USFilter")

DATA_DIR = Path(__file__).parent.parent / "data_store"
INPUT_PATH = DATA_DIR / "us_market_overnight.json"
OUTPUT_PATH = DATA_DIR / "us_overnight_result.json"


# ════════════════════════════════════════════════════════════
# 1. 로컬 JSON에서 데이터 로드
# ════════════════════════════════════════════════════════════

def load_us_market() -> dict | None:
    """us_market_overnight.json 로드."""
    if not INPUT_PATH.exists():
        logger.error("us_market_overnight.json 없음 — collect_us_overnight() 먼저 실행")
        return None
    try:
        data = json.loads(INPUT_PATH.read_text("utf-8"))
        # 오늘 날짜 데이터인지 확인
        data_date = data.get("date", "")
        today = date.today().isoformat()
        if data_date != today:
            logger.warning(f"데이터 날짜 불일치: {data_date} ≠ {today} — 오래된 데이터 사용")
        return data
    except Exception as e:
        logger.error(f"us_market_overnight.json 로드 실패: {e}")
        return None


# ════════════════════════════════════════════════════════════
# 2. 갭 방향 예측
# ════════════════════════════════════════════════════════════

def estimate_gap(us: dict) -> dict:
    """
    KOSPI/KOSDAQ 다음날 갭 방향 + 크기 추정.

    핵심 가중치:
      나스닥 등락         35%
      SOXX 등락          25%
      S&P500 등락        15%
      EWY (한국 ETF)     15% — 외국인 한국 바스켓 직접 신호
      KS200 야간선물     10% — 차익거래 직접 힌트
      VIX 조정          -10% (공포↑ → 갭 축소)
    """
    nq   = us.get("nasdaq_change")  or 0
    sox  = us.get("soxx_change")    or 0
    sp   = us.get("sp500_change")   or 0
    ewy  = us.get("ewy_change")     or 0
    ks   = us.get("ks200_change")   or 0
    vix  = us.get("vix")            or 20

    raw = (
        (nq  * 0.35) +
        (sox * 0.25) +
        (sp  * 0.15) +
        (ewy * 0.15) +
        (ks  * 0.10)
    )

    # VIX 조정: 25 이상이면 갭 크기 축소
    vix_penalty = max(0, (vix - 20) * 0.05)
    adjusted = raw * (1 - min(vix_penalty, 0.4))

    if adjusted >= 1.0:
        direction = "GAP_UP"
    elif adjusted <= -1.0:
        direction = "GAP_DOWN"
    else:
        direction = "FLAT"

    return {
        "gap_signal":  direction,
        "gap_est_pct": round(adjusted, 2),
        "gap_raw":     round(raw, 2),
        "vix_penalty": round(vix_penalty, 3),
        "ewy_contrib": round(ewy * 0.15, 2),
        "ks200_contrib": round(ks * 0.10, 2),
    }


def foreign_basket_score(us: dict) -> dict:
    """
    외국인 바스켓 지표(EWY/SSNLF/KS200) 기반 리스크 조정 점수.

    반환:
      - adjustment: 리스크 점수에 더할 값 (음수=완화, 양수=강화)
      - signals: 메시지에 표시할 시그널 리스트
      - ewy_mode_hint: 'BULL' | 'BEAR' | 'NEUTRAL'
    """
    ewy_1d = us.get("ewy_change")     or 0
    ewy_5d = us.get("ewy_change_5d")  or 0
    ks_1d  = us.get("ks200_change")   or 0
    ks_5d  = us.get("ks200_change_5d") or 0

    adjustment = 0
    signals = []

    # ── EWY 1일 (가장 강력한 실시간 신호) ────────────
    if ewy_1d >= 5.0:
        adjustment -= 20  # 리스크 대거 완화
        signals.append(f"🔥 EWY {ewy_1d:+.1f}% 외국인 한국 폭발매수")
    elif ewy_1d >= 2.0:
        adjustment -= 10
        signals.append(f"✅ EWY {ewy_1d:+.1f}% 외국인 한국 매수우세")
    elif ewy_1d >= 0.5:
        adjustment -= 3
    elif ewy_1d <= -3.0:
        adjustment += 20  # 리스크 대거 강화
        signals.append(f"🚨 EWY {ewy_1d:+.1f}% 외국인 한국 대탈출")
    elif ewy_1d <= -1.5:
        adjustment += 10
        signals.append(f"⚠️ EWY {ewy_1d:+.1f}% 외국인 한국 매도")

    # ── EWY 5일 누적 (중기 방향) ─────────────────────
    if ewy_5d >= 8.0:
        adjustment -= 8
        signals.append(f"📈 EWY 5D {ewy_5d:+.1f}% 중기 매수세 강력")
    elif ewy_5d <= -5.0:
        adjustment += 8
        signals.append(f"📉 EWY 5D {ewy_5d:+.1f}% 중기 매도세")

    # ── KS200 야간선물 (차익거래 직접 힌트) ──────────
    if ks_1d >= 2.0:
        adjustment -= 5
        signals.append(f"✅ KS200 야간 {ks_1d:+.1f}% 차익거래 매수")
    elif ks_1d <= -2.0:
        adjustment += 5
        signals.append(f"⚠️ KS200 야간 {ks_1d:+.1f}% 차익거래 매도")

    # ── EWY 모드 힌트 ──────────────────────────────
    if ewy_1d >= 2.0 or ewy_5d >= 5.0:
        ewy_mode_hint = "BULL"
    elif ewy_1d <= -2.0 or ewy_5d <= -4.0:
        ewy_mode_hint = "BEAR"
    else:
        ewy_mode_hint = "NEUTRAL"

    return {
        "adjustment":    adjustment,
        "signals":       signals,
        "ewy_mode_hint": ewy_mode_hint,
        "ewy_1d":        ewy_1d,
        "ewy_5d":        ewy_5d,
        "ks200_1d":      ks_1d,
        "ks200_5d":      ks_5d,
    }


# ════════════════════════════════════════════════════════════
# 3. 진입 모드 결정
# ════════════════════════════════════════════════════════════

def decide_mode(us: dict, gap: dict) -> dict:
    """다음날 단타 진입 모드 결정."""
    vix           = us.get("vix")            or 20
    fear_greed    = us.get("fear_greed")     or 50
    dxy           = us.get("dxy")            or 100
    soxx_change   = us.get("soxx_change")    or 0
    spread_3y_10y = us.get("spread_3y_10y")  or 0
    risk_flags    = us.get("risk_flags")     or []
    gap_pct       = gap["gap_est_pct"]

    # ── 위험 점수 계산 ──────────────────────────────────
    risk_score = 0
    reasons_bad  = []
    reasons_good = []

    # VIX
    if vix >= 35:
        risk_score += 30
        reasons_bad.append(f"VIX {vix:.0f} 패닉구간")
    elif vix >= 25:
        risk_score += 15
        reasons_bad.append(f"VIX {vix:.0f} 주의구간")
    elif vix <= 15:
        risk_score -= 5
        reasons_good.append("VIX 안정")

    # Fear & Greed
    if fear_greed <= 20:
        risk_score += 20
        reasons_bad.append(f"극단공포 ({fear_greed})")
    elif fear_greed >= 75:
        risk_score += 10
        reasons_bad.append(f"극단탐욕 ({fear_greed}) 과열")
    elif 30 <= fear_greed <= 65:
        risk_score -= 5
        reasons_good.append(f"심리 중립 ({fear_greed})")

    # DXY
    if dxy >= 106:
        risk_score += 25
        reasons_bad.append(f"DXY {dxy:.1f} 초강달러 → 외인 대거 이탈 위험")
    elif dxy >= 104:
        risk_score += 12
        reasons_bad.append(f"DXY {dxy:.1f} 강달러 → 외인 이탈 주의")
    elif dxy <= 100:
        risk_score -= 8
        reasons_good.append(f"DXY {dxy:.1f} 약달러 → 외인 유입 우호")

    # SOXX
    if soxx_change <= -4:
        risk_score += 20
        reasons_bad.append(f"SOXX {soxx_change:+.1f}% 급락 → 반도체 급락 예상")
    elif soxx_change <= -2:
        risk_score += 10
        reasons_bad.append(f"SOXX {soxx_change:+.1f}% → 반도체 주의")
    elif soxx_change >= 2:
        risk_score -= 10
        reasons_good.append(f"SOXX {soxx_change:+.1f}% 강세 → 반도체 갭업 기대")

    # 장단기 금리 역전
    if spread_3y_10y < -0.3:
        risk_score += 15
        reasons_bad.append(f"5Y-10Y 금리역전 ({spread_3y_10y:+.3f}%)")
    elif spread_3y_10y < 0:
        risk_score += 5
        reasons_bad.append("장단기 금리역전")

    # 위험 플래그 가산
    risk_score += len(risk_flags) * 5

    # ── 외국인 바스켓 점수 반영 ──────────────────────
    basket = foreign_basket_score(us)
    risk_score += basket["adjustment"]
    if basket["signals"]:
        # EWY/KS200 시그널을 이유 목록에 추가
        for sig in basket["signals"]:
            if "🔥" in sig or "✅" in sig or "📈" in sig:
                reasons_good.append(sig)
            else:
                reasons_bad.append(sig)

    # ── 모드 결정 ──────────────────────────────────────
    risk_score = max(0, risk_score)

    # EWY BULL 힌트가 있으면 AGGRESSIVE 문턱 완화
    ewy_hint = basket["ewy_mode_hint"]
    aggressive_risk_cap = 10 if ewy_hint == "BULL" else 5
    aggressive_gap_min  = 0.3 if ewy_hint == "BULL" else 0.5

    if risk_score >= 60:
        mode = "HALT"
    elif risk_score >= 35:
        mode = "DEFENSIVE"
    elif risk_score <= aggressive_risk_cap and gap_pct >= aggressive_gap_min:
        mode = "AGGRESSIVE"
    elif ewy_hint == "BEAR" and risk_score >= 20:
        # EWY 약세 + 위험 중간 이상 → DEFENSIVE로 격상
        mode = "DEFENSIVE"
    else:
        mode = "NORMAL"

    # ── 섹터 방향 ──────────────────────────────────────
    sector_etf = us.get("sector_etf") or {}
    watch_sectors = []
    avoid_sectors = []

    etf_to_kr = {
        "XLK":  "기술", "XLF":  "금융", "XLE":  "에너지",
        "XLV":  "헬스케어", "XLI":  "산업재", "XLY":  "경기소비재",
        "XLP":  "필수소비재", "XLU":  "유틸리티", "XLB":  "소재",
        "XLRE": "부동산", "XLC":  "커뮤니케이션",
    }
    for ticker, chg in sector_etf.items():
        kr_name = etf_to_kr.get(ticker, ticker)
        if chg >= 1.5:
            watch_sectors.append(kr_name)
        elif chg <= -1.5:
            avoid_sectors.append(kr_name)

    soxx_alert = soxx_change <= -3 or soxx_change >= 3

    return {
        "mode":          mode,
        "risk_score":    risk_score,
        "soxx_alert":    soxx_alert,
        "watch_sectors": watch_sectors[:4],
        "avoid_sectors": avoid_sectors[:4],
        "reasons_bad":   reasons_bad,
        "reasons_good":  reasons_good,
        "foreign_basket": basket,
    }


# ════════════════════════════════════════════════════════════
# 4. 최종 리포트 조립
# ════════════════════════════════════════════════════════════

def build_report(us: dict, gap: dict, mode_data: dict) -> dict:
    """전체 결과 조립."""
    mode       = mode_data["mode"]
    risk_score = mode_data["risk_score"]
    soxx_alert = mode_data["soxx_alert"]

    risk_level = (
        5 if risk_score >= 60 else
        4 if risk_score >= 40 else
        3 if risk_score >= 25 else
        2 if risk_score >= 12 else
        1
    )

    nq   = us.get("nasdaq_change")   or 0
    sox  = us.get("soxx_change")     or 0
    vix  = us.get("vix")             or 20
    gap_ = gap["gap_est_pct"]

    parts = [f"나스닥 {nq:+.1f}%", f"SOXX {sox:+.1f}%"]
    if vix >= 25:
        parts.append(f"VIX {vix:.0f} 주의")

    direction_str = {
        "GAP_UP":   f"→ 한국장 갭업 예상 ({gap_:+.1f}%)",
        "GAP_DOWN": f"→ 한국장 갭다운 예상 ({gap_:+.1f}%)",
        "FLAT":     "→ 한국장 보합 예상",
    }[gap["gap_signal"]]

    reason = " | ".join(parts) + " " + direction_str

    relay_picks = get_relay_picks(us)

    report = {
        "date":          us.get("date", date.today().isoformat()),
        "mode":          mode,
        "gap_signal":    gap["gap_signal"],
        "gap_est_pct":   gap["gap_est_pct"],
        "soxx_alert":    soxx_alert,
        "risk_level":    risk_level,
        "risk_score":    risk_score,
        "watch_sectors": mode_data["watch_sectors"],
        "avoid_sectors": mode_data["avoid_sectors"],
        "reasons_bad":   mode_data["reasons_bad"],
        "reasons_good":  mode_data["reasons_good"],
        "reason":        reason,
        "relay_picks":   relay_picks,
        "nasdaq_change": nq,
        "soxx_change":   sox,
        "vix":           vix,
        "dxy":           us.get("dxy"),
        "us_3y_yield":   us.get("us_3y_yield"),
        "fear_greed":    us.get("fear_greed"),
        "fear_greed_label": us.get("fear_greed_label"),
        "kr_impact":     us.get("kr_impact"),
        "risk_flags":    us.get("risk_flags") or [],
        "foreign_basket": mode_data.get("foreign_basket") or {},
        "ewy_change":    us.get("ewy_change"),
        "ewy_change_5d": us.get("ewy_change_5d"),
        "ks200_change":  us.get("ks200_change"),
        "ks200_change_5d": us.get("ks200_change_5d"),
    }

    return report


# ════════════════════════════════════════════════════════════
# 5. 텔레그램 메시지 생성
# ════════════════════════════════════════════════════════════

def build_telegram_message(report: dict) -> str:
    """단타봇 텔레그램 발송용 메시지."""
    mode_emoji = {
        "AGGRESSIVE": "🟢",
        "NORMAL":     "🔵",
        "DEFENSIVE":  "🟡",
        "HALT":       "🔴",
    }
    gap_emoji = {
        "GAP_UP":   "📈",
        "GAP_DOWN": "📉",
        "FLAT":     "➡️",
    }
    mode    = report["mode"]
    gap_sig = report["gap_signal"]
    gap_pct = report["gap_est_pct"]

    lines = [
        "🇺🇸 미국장 → 한국장 야간 분석",
        "━━━━━━━━━━━━━━━━━━━━",
        "",
        f"{mode_emoji.get(mode, '🔵')} 진입 모드: {mode}",
        f"{gap_emoji.get(gap_sig, '➡️')} 갭 예측: {gap_sig} ({gap_pct:+.1f}%)",
        f"⚡ 위험 레벨: {'⭐' * report['risk_level']} ({report['risk_level']}/5)",
        "",
        "📊 주요 지표",
        f"  나스닥: {report['nasdaq_change']:+.2f}%",
        f"  SOXX:  {report['soxx_change']:+.2f}%" + (" ⚠️" if report['soxx_alert'] else ""),
        f"  VIX:   {report['vix']:.1f}",
    ]

    if report.get("dxy"):
        lines.append(f"  DXY:   {report['dxy']:.1f}")
    if report.get("us_3y_yield"):
        lines.append(f"  5년물: {report['us_3y_yield']:.2f}%")
    if report.get("fear_greed"):
        lines.append(f"  Fear&Greed: {report['fear_greed']} ({report['fear_greed_label']})")

    # 외국인 바스켓 (EWY/KS200)
    ewy_1d = report.get("ewy_change")
    ewy_5d = report.get("ewy_change_5d")
    ks_1d  = report.get("ks200_change")
    if ewy_1d is not None or ks_1d is not None:
        lines.append("")
        lines.append("🌐 외국인 바스켓")
        if ewy_1d is not None:
            lines.append(f"  EWY:   {ewy_1d:+.2f}% (5D {ewy_5d:+.1f}%)")
        if ks_1d is not None:
            lines.append(f"  KS200 야간: {ks_1d:+.2f}%")

    # 주목 섹터
    if report["watch_sectors"]:
        lines.append("")
        lines.append(f"✅ 주목 섹터: {', '.join(report['watch_sectors'])}")
    if report["avoid_sectors"]:
        lines.append(f"🚫 회피 섹터: {', '.join(report['avoid_sectors'])}")

    # 릴레이 종목
    relay = report.get("relay_picks") or []
    if relay:
        lines.append("")
        lines.append("🔗 US→KR 릴레이 후보")
        for r in relay[:4]:
            lines.append(f"  {r['us_ticker']} {r['us_change']:+.1f}% → {r['kr_name']} ({r['kr_code']})")

    # 위험 플래그
    if report["risk_flags"]:
        lines.append("")
        lines.append("⚠️ 위험 플래그")
        for f in report["risk_flags"]:
            lines.append(f"  · {f}")

    # 모드별 행동 지침
    action = {
        "AGGRESSIVE": "→ 적극 진입. 수급 A+ 종목 빠르게 선점",
        "NORMAL":     "→ 기본 필터 그대로. 수급 확인 후 진입",
        "DEFENSIVE":  "→ 조건 강화. 수급 A+만, 손절 타이트하게",
        "HALT":       "→ 진입 금지. 기존 포지션 손절 우선",
    }[mode]
    lines += ["", f"📌 {action}", "━━━━━━━━━━━━━━━━━━━━"]

    return "\n".join(lines)


# ════════════════════════════════════════════════════════════
# 6. 메인 실행
# ════════════════════════════════════════════════════════════

def run() -> dict:
    """메인 실행. 수집 → 분석 → 결과 저장 + 반환."""
    logger.info("=== US Overnight Filter 시작 ===")

    us = load_us_market()
    if not us:
        logger.error("미국장 데이터 없음")
        return {}

    logger.info(f"기준일: {us.get('date')}")

    gap       = estimate_gap(us)
    mode_data = decide_mode(us, gap)
    report    = build_report(us, gap, mode_data)

    logger.info(
        f"모드: {report['mode']}  |  "
        f"갭: {report['gap_signal']} ({report['gap_est_pct']:+.1f}%)  |  "
        f"위험: {report['risk_level']}/5"
    )

    # 결과 저장
    try:
        tmp = OUTPUT_PATH.with_suffix(".tmp")
        tmp.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(OUTPUT_PATH)
    except Exception as e:
        logger.warning(f"결과 저장 실패: {e}")

    return report


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    report = run()
    if report:
        print(build_telegram_message(report))
