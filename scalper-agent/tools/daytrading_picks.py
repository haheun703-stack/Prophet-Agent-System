"""
단타 TOP 픽 (Daytrading Picks v1)
===================================

목적: 내일~3일 안에 오를 확률 높은 종목 5~7개 TOP 리스트
      시나리오 분류 없이 오직 수급 데이터 기반 단일 리스트.

패턴 참조:
- SK텔레콤 (4/2~4/7 조용한 축적 → 4/8 +9.07% → 4/9 +5.39%)
- 후성 (4/8 QUIET_ACC 감지 → 4/9 +15.76%)
- 넥스틸 (4/7 감지 → 4/9 +14.80%)

5단계 복합 필터:
[1] foreign_accumulation_scanner 60점+ (조용한 매집)
[2] 쌍매수 필터: 기관 합류 1일+ (외인+기관 동시)
[3] EWY 수혜 섹터 가산점 (반도체/운송장비/2차전지/금융/통신)
[4] 시총 대형주 가산점 (1조+ = EWY 바스켓 직접 수혜)
[5] 학습 피드백 (insights.json 소스 신뢰도, 있으면)

출력:
- data_store/daytrading_picks.json (기계용)
- 콘솔 + FLOWX 게시용 텍스트 포맷

사용:
    python3 tools/daytrading_picks.py [--top 5] [--save] [--flowx-format]
"""
from __future__ import annotations
import json
import argparse
import sys
import logging
from pathlib import Path
from datetime import datetime, timedelta

# foreign_accumulation_scanner 재사용
sys.path.insert(0, str(Path(__file__).resolve().parent))
from foreign_accumulation_scanner import (
    scan_universe,
    load_universe,
    load_investor_flow,
    LOOKBACK_DAYS,
)

logger = logging.getLogger(__name__)
from sector_etf_map import get_etf_for_stock, aggregate_etf_from_picks


# ─────────────────────────────────────────────
# 경로
# ─────────────────────────────────────────────
BASE = Path(__file__).resolve().parents[1]
OUT_PATH = BASE / "data_store" / "daytrading_picks.json"
INSIGHTS_PATH = BASE / "data_store" / "insights.json"
EWY_INDICATORS_PATH = BASE / "data_store" / "foreign_basket_indicators.json"
US_OVERNIGHT_PATH = BASE / "data_store" / "us_market_overnight.json"


# ─────────────────────────────────────────────
# EWY 수혜 섹터 매핑
# ─────────────────────────────────────────────
# EWY (iShares Korea ETF) 상위 구성 → 한국 섹터 매칭
EWY_CORE_SECTORS = {
    "전기전자": 5,    # 삼성전자, SK하이닉스, 삼성SDI, LG에너지솔루션 (EWY 50%+)
    "운송장비": 4,    # 현대차, 기아, 한화에어로 (EWY 10%+)
    "화학": 3,        # LG화학, 에코프로 (EWY 5%+)
    "금속": 3,        # 포스코, LIG넥스원 (방산 포함)
    "기계장비": 2,    # 두산에너빌리티, 한화오션
    "금융": 2,        # KB금융, 신한지주 (EWY 5%+)
    "통신": 2,        # SK텔레콤, KT
    "건설": 2,        # GS건설, 현대건설 (휴전/재건 수혜)
}

# 시총 구간별 가산점 (EWY 바스켓 수혜도)
MCAP_BONUS = [
    (100000, 5),    # 10조+ (EWY 직접 바스켓)
    (50000, 4),     # 5조+
    (20000, 3),     # 2조+
    (10000, 2),     # 1조+
    (5000, 1),      # 5천억+
]

MIN_FINAL_SCORE = 60.0  # 최종 점수 컷오프

# ETF 브랜드 접두사 — 단타 픽에서 제외 (ETF 섹션 따로 집계하므로)
ETF_BRAND_PREFIXES = (
    "TIGER", "KODEX", "KBSTAR", "ACE", "PLUS", "KOSEF", "HANARO", "SOL",
    "ARIRANG", "TREX", "FOCUS", "HK", "PARA", "마이티", "히어로즈",
    "RISE", "KCGI", "WOORI", "WON", "WON드림",
)


def is_etf(name: str) -> bool:
    """종목명이 ETF 브랜드로 시작하는지."""
    if not name:
        return False
    name_upper = name.strip().upper()
    return any(name_upper.startswith(p.upper()) for p in ETF_BRAND_PREFIXES)


def _calc_wick_from_ohlcv(df, recent_days: int) -> float:
    """OHLCV DataFrame에서 위꼬리 비율 계산 (공통 로직)."""
    if df is None or len(df) < recent_days:
        return -1.0  # 데이터 부족 → 호출자에서 fallback 판단
    wick_ratios = []
    for _, row in df.tail(recent_days).iterrows():
        h, l = float(row["고가"] if "고가" in df.columns else row["High"])
        o = float(row["시가"] if "시가" in df.columns else row["Open"])
        c = float(row["종가"] if "종가" in df.columns else row["Close"])
        rng = h - l
        if rng > 0:
            wick_ratios.append((h - max(o, c)) / rng * 100)
    return round(sum(wick_ratios) / len(wick_ratios), 1) if wick_ratios else 0.0


def calc_upper_wick_pct(code: str, recent_days: int = 3) -> float:
    """최근 N일 위꼬리 비율 평균 계산 (신정재 필터).
    pykrx 우선 → 실패 시 yfinance fallback.

    Returns: 0~100 사이 퍼센트. 에러 시 0.
    """
    # 1차: pykrx
    try:
        from pykrx import stock
        end = datetime.now()
        start = end - timedelta(days=30)
        df = stock.get_market_ohlcv(
            start.strftime("%Y%m%d"), end.strftime("%Y%m%d"), code
        )
        result = _calc_wick_from_ohlcv(df, recent_days)
        if result >= 0:
            return result
    except Exception as e:
        logger.debug(f"위꼬리 pykrx 실패 {code}: {e}")

    # 2차: yfinance fallback
    try:
        import yfinance as yf
        # 한국 종목코드 변환: 005930 → 005930.KS
        suffix = ".KS" if code[0] in "0123" else ".KQ"
        ticker = yf.Ticker(f"{code}{suffix}")
        df = ticker.history(period="1mo")
        if df is not None and len(df) >= recent_days:
            wick_ratios = []
            for _, row in df.tail(recent_days).iterrows():
                h, l = float(row["High"]), float(row["Low"])
                o, c = float(row["Open"]), float(row["Close"])
                rng = h - l
                if rng > 0:
                    wick_ratios.append((h - max(o, c)) / rng * 100)
            if wick_ratios:
                return round(sum(wick_ratios) / len(wick_ratios), 1)
    except Exception as e:
        logger.debug(f"위꼬리 yfinance fallback 실패 {code}: {e}")

    return 0.0


# ─────────────────────────────────────────────
# 트랙 A (대형주) 설정
# ─────────────────────────────────────────────
LARGE_CAP_MIN_MCAP = 20000          # 시총 2조+ 대형주만
LARGE_CAP_QUIET_THRESHOLD = 15.0    # 조용함 ±15% (대형주는 원래 변동 큼)
LARGE_CAP_MIN_DUAL_BUY = 300        # 외인+기관 5일 누적 300억+ (쌍매수 필수)
LARGE_CAP_MIN_FINAL = 55.0          # 대형주 최종 컷오프 (조금 완화)


def scan_large_caps(
    min_mcap: float = LARGE_CAP_MIN_MCAP,
    min_dual_buy: float = LARGE_CAP_MIN_DUAL_BUY,
    quiet_threshold: float = LARGE_CAP_QUIET_THRESHOLD,
) -> list[dict]:
    """
    [트랙 A] 대형주 전용 스캔 — EWY 바스켓 수혜 직접 대상

    기존 scanner와 다른 점:
    - 시총 2조+ 필수
    - 조용함 ±15% (완화)
    - 외인+기관 쌍매수 누적 300억+ (금액 기준)
    - 기관 합류 필수
    """
    uni = load_universe()
    candidates = []

    for code, info in uni.items():
        if not isinstance(info, dict):
            continue
        # ETF 제외 (따로 섹션으로 집계)
        if is_etf(info.get("name", "")):
            continue
        mcap = info.get("cap_억", 0) or 0
        if mcap < min_mcap:
            continue

        flow = load_investor_flow(code)
        if flow is None:
            continue

        try:
            # 5일 외인/기관 누적 (백만원 → 억원)
            foreign_amt = flow["외국인_금액"].astype(float).values / 100
            inst_amt = flow["기관_금액"].astype(float).values / 100
            foreign_total = float(foreign_amt.sum())
            inst_total = float(inst_amt.sum())

            # [필수] 외인+기관 둘 다 양수 (쌍매수)
            if foreign_total <= 0 or inst_total <= 0:
                continue

            dual_total = foreign_total + inst_total
            if dual_total < min_dual_buy:
                continue

            # 주가 변동 (완화된 조용함 필터)
            closes = flow["종가"].astype(float).values
            if closes[0] <= 0:
                continue
            price_chg = ((closes[-1] / closes[0]) - 1) * 100
            if abs(price_chg) > quiet_threshold:
                continue

            # 매수일수
            foreign_buy_days = int((foreign_amt > 0).sum())
            inst_buy_days = int((inst_amt > 0).sum())
            inst_joining = int((inst_amt[-2:] > 0).sum())

            # 대형주 스코어 로직 (쌍매수 금액 우선)
            score = 45.0  # 기본
            # 쌍매수 금액 (최대 +25)
            if dual_total >= 10000:  # 1조+
                score += 25
            elif dual_total >= 5000:  # 5천억+
                score += 20
            elif dual_total >= 2000:  # 2천억+
                score += 15
            elif dual_total >= 1000:  # 1천억+
                score += 10
            else:
                score += 5
            # 매수일수 (최대 +15)
            score += foreign_buy_days * 3
            # 기관 합류 (최대 +10)
            score += inst_joining * 5
            # 시총/외인 비율 (최대 +10)
            ratio = (foreign_total / mcap) * 100
            if ratio >= 0.5:
                score += 10
            elif ratio >= 0.3:
                score += 7
            elif ratio >= 0.15:
                score += 4
            else:
                score += 2

            # 조용함 보너스 (±5% 이내 +5, 이외 0)
            if abs(price_chg) <= 5.0:
                score += 5

            candidates.append({
                "code": code,
                "name": info.get("name", ""),
                "mcap_억": round(mcap, 0),
                "score": round(score, 1),
                "buy_days": foreign_buy_days,
                "inst_buy_days": inst_buy_days,
                "inst_joining": inst_joining,
                "foreign_total_억": round(foreign_total, 1),
                "inst_total_억": round(inst_total, 1),
                "dual_total_억": round(dual_total, 1),
                "ratio_mcap_%": round(ratio, 3),
                "price_change_%": round(price_chg, 2),
                "quiet_score": 0,
                "indiv_out_days": 0,
                "close_start": int(closes[0]),
                "close_end": int(closes[-1]),
                "dates": flow["date"].tolist(),
                "foreign_daily_억": [round(x, 1) for x in foreign_amt.tolist()],
                "inst_daily_억": [round(x, 1) for x in inst_amt.tolist()],
                "track": "large_cap",
            })
        except Exception:
            continue

    candidates.sort(key=lambda x: x["score"], reverse=True)
    return candidates


def load_insights() -> dict:
    """insights.json 로드 (없으면 빈 dict)"""
    if not INSIGHTS_PATH.exists():
        return {}
    try:
        return json.loads(INSIGHTS_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}


def load_ewy_signal() -> dict:
    """EWY/^KS200 시그널 로드 (foreign_basket_indicators.json 우선, us_market_overnight.json 대체)"""
    # 1순위: 오늘 수집한 실시간 지표
    if EWY_INDICATORS_PATH.exists():
        try:
            data = json.loads(EWY_INDICATORS_PATH.read_text(encoding="utf-8"))
            ewy = data.get("EWY", {})
            ks200 = data.get("^KS200", {})
            return {
                "ewy_1d": ewy.get("1d", 0),
                "ewy_5d": ewy.get("5d", 0),
                "ks200_1d": ks200.get("1d", 0),
                "ks200_5d": ks200.get("5d", 0),
                "source": "basket_indicators",
            }
        except Exception:
            pass

    # 2순위: us_market_overnight.json (Phase 1 적용 후)
    if US_OVERNIGHT_PATH.exists():
        try:
            data = json.loads(US_OVERNIGHT_PATH.read_text(encoding="utf-8"))
            return {
                "ewy_1d": data.get("ewy_change", 0),
                "ewy_5d": 0,
                "ks200_1d": data.get("ks200_change", 0),
                "ks200_5d": 0,
                "source": "us_overnight",
            }
        except Exception:
            pass

    return {"ewy_1d": 0, "ewy_5d": 0, "ks200_1d": 0, "ks200_5d": 0, "source": "none"}


def apply_daytrading_filters(
    candidates: list[dict],
    universe: dict,
    ewy_signal: dict,
    insights: dict,
    min_final_score: float = MIN_FINAL_SCORE,
) -> list[dict]:
    """5단계 복합 필터 적용"""
    filtered = []

    # EWY 폭등 보정 (+5% 이상이면 대형주 가산점 강화)
    ewy_boost = ewy_signal.get("ewy_1d", 0) >= 5.0

    # 대형주 트랙은 기본 점수 컷오프를 45로 완화
    base_cutoff = 45.0 if min_final_score < MIN_FINAL_SCORE else 60.0

    for c in candidates:
        code = c["code"]
        info = universe.get(code, {})
        sector = info.get("sector", "-")
        mcap = c.get("mcap_억", 0)

        # [필터 0] ETF 제외 (단타 픽에서 ETF 배제)
        if is_etf(info.get("name", "") or c.get("name", "")):
            continue

        # [필터 1] 기본 점수 컷오프
        if c["score"] < base_cutoff:
            continue

        # [필터 2] 쌍매수 필수 — 기관 합류 1일+
        if c.get("inst_joining", 0) == 0:
            continue

        # [가산 3] EWY 수혜 섹터 보너스
        sector_bonus = EWY_CORE_SECTORS.get(sector, 0)

        # [가산 4] 시총 대형주 보너스
        mcap_bonus = 0
        for threshold, bonus in MCAP_BONUS:
            if mcap >= threshold:
                mcap_bonus = bonus
                break

        # [가산 5] EWY 폭등일 대형주 추가 가산점
        ewy_bonus = 0
        if ewy_boost and mcap >= 10000 and sector in ("전기전자", "운송장비"):
            ewy_bonus = 5

        # [가산 6] 학습 피드백 (insights.json)
        # 예: insights.json에 "foreign_accum" 소스가 최근 승률 70%+면 +3
        learning_bonus = 0
        if insights:
            sources = insights.get("source_accuracy", {})
            foreign_acc = sources.get("foreign_accum", {})
            if foreign_acc.get("recent_winrate", 0) >= 0.7:
                learning_bonus = 3

        # [필터 7] 위꼬리 캔들 페널티 (신정재 필터)
        wick_pct = calc_upper_wick_pct(code)
        wick_pen = 0
        if wick_pct > 30:
            wick_pen = -15
        elif wick_pct > 20:
            wick_pen = -8

        # 최종 점수
        final_score = c["score"] + sector_bonus + mcap_bonus + ewy_bonus + learning_bonus + wick_pen

        if final_score < min_final_score:
            continue

        # 추천 진입/목표가 계산
        close = c.get("close_end", 0)
        entry_low = int(close * 0.985)
        entry_high = int(close * 1.010)
        tp1 = int(close * 1.050)  # +5%
        tp2 = int(close * 1.080)  # +8%
        sl = int(close * 0.965)   # -3.5%

        # 핵심 이유 1줄
        reasons = []
        reasons.append(f"외국인 {c['buy_days']}/{LOOKBACK_DAYS}일 매수 {c['foreign_total_억']:+.0f}억")
        if c.get("inst_joining", 0) >= 1:
            reasons.append(f"기관 합류 {c['inst_joining']}일")
        if sector_bonus >= 4:
            reasons.append(f"EWY 수혜 {sector}")
        if ewy_bonus > 0:
            reasons.append("EWY +10% 대형주 수혜")

        # 섹터 ETF 대안 매핑 (주린이 분산진입용)
        etf_alt = get_etf_for_stock(c.get("name", ""), sector)

        if wick_pen != 0:
            reasons.append(f"위꼬리 {wick_pct:.0f}%({wick_pen:+d})")

        filtered.append({
            **c,
            "sector": sector,
            "sector_bonus": sector_bonus,
            "mcap_bonus": mcap_bonus,
            "ewy_bonus": ewy_bonus,
            "learning_bonus": learning_bonus,
            "wick_pct": wick_pct,
            "wick_pen": wick_pen,
            "final_score": round(final_score, 1),
            "entry_low": entry_low,
            "entry_high": entry_high,
            "tp1": tp1,
            "tp2": tp2,
            "sl": sl,
            "upside_to_tp1_pct": round((tp1 / close - 1) * 100, 1) if close else 0,
            "key_reasons": " + ".join(reasons),
            "etf_alt_code": etf_alt["code"],
            "etf_alt_name": etf_alt["name"],
            "etf_alt_theme": etf_alt["theme"],
        })

    filtered.sort(key=lambda x: x["final_score"], reverse=True)
    return filtered


def _mode_title(mode: str) -> tuple[str, str]:
    """모드별 제목 + 설명 반환."""
    if mode == "preview":
        return (
            "📢 내일 단타 프리뷰",
            "(국장 마감 기준 · 최종 확정은 내일 07:30)",
        )
    else:
        return (
            "🎯 오늘 단타 TOP픽 확정",
            "(미국장 + 외국인 바스켓 반영 완료)",
        )


def format_flowx_post(picks: list[dict], ewy_signal: dict, mode: str = "confirmed") -> str:
    """FLOWX 게시용 포맷 — 깔끔한 리스트 + ETF 대안 + 모드별 제목."""
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    title, subtitle = _mode_title(mode)

    lines = [
        f"{title} · TOP {len(picks)}",
        f"📅 {now} {subtitle}",
        "━━━━━━━━━━━━━━━━━━━━━━━━",
    ]

    # EWY 시그널 (confirmed 모드만)
    if mode == "confirmed":
        ewy_1d = ewy_signal.get("ewy_1d", 0)
        ks200_1d = ewy_signal.get("ks200_1d", 0)
        if abs(ewy_1d) > 0.01 or abs(ks200_1d) > 0.01:
            lines.append(f"🌍 외인 바스켓: EWY {ewy_1d:+.2f}% | KS200 야간 {ks200_1d:+.2f}%")
            if ewy_1d >= 5.0:
                lines.append("🔥 외국인 한국 폭발매수 — 적극 진입 모드")
            elif ewy_1d >= 2.0:
                lines.append("✅ 외국인 한국 매수우세 — 정상 진입")
            elif ewy_1d <= -2.0:
                lines.append("⚠️ 외국인 한국 매도 — 신중 진입")
            lines.append("")

    # 개별 종목
    for i, p in enumerate(picks, 1):
        rank_emoji = ["🥇", "🥈", "🥉", "4️⃣", "5️⃣"][min(i-1, 4)]

        lines.append(f"{rank_emoji} <b>{p['name']}</b> ({p['code']}) · {p.get('sector','-')}")
        lines.append(f"   💰 현재가 {p['close_end']:,}원 · 시총 {int(p['mcap_억']):,}억")
        lines.append(f"   🎯 진입 {p['entry_low']:,}~{p['entry_high']:,} · 목표 {p['tp1']:,} (+{p['upside_to_tp1_pct']:.1f}%)")
        lines.append(f"   📊 {p['key_reasons']}")
        # 🔗 ETF 대안
        etf_code = p.get("etf_alt_code", "")
        etf_name = p.get("etf_alt_name", "")
        etf_theme = p.get("etf_alt_theme", "")
        if etf_code:
            lines.append(f"   🔗 ETF 대안: {etf_name} ({etf_code}) [{etf_theme}]")
        wick_info = f" + 위꼬리 {p.get('wick_pen', 0):+d}" if p.get("wick_pen", 0) != 0 else ""
        lines.append(f"   ⭐ 점수 {p['final_score']:.0f} (기본 {p['score']:.0f} + 섹터 {p['sector_bonus']} + 시총 {p['mcap_bonus']} + EWY {p['ewy_bonus']}{wick_info})")
        lines.append("")

    # 🔗 ETF 집계 섹션 (주린이가 분산진입할 수 있도록)
    etf_agg = aggregate_etf_from_picks(picks)
    if etf_agg:
        lines.append("━━━━━━━━━━━━━━━━━━━━━━━━")
        lines.append("🔗 <b>오늘의 섹터 ETF TOP 3</b> (개별 종목 대신 분산진입)")
        for i, e in enumerate(etf_agg[:3], 1):
            stocks_str = ", ".join(e["stocks"][:3])
            lines.append(
                f"  {i}. {e['name']} ({e['code']}) [{e['theme']}] "
                f"— {e['stock_count']}종목: {stocks_str}"
            )

    lines.append("━━━━━━━━━━━━━━━━━━━━━━━━")
    lines.append("📌 선정: 외국인 선행매집 + 기관 합류 + EWY 바스켓 수혜")
    if mode == "preview":
        lines.append("📌 내일 아침 07:30 미국장 반영 후 최종 확정")
        lines.append("⏰ NXT 야간 매수 가능 (17:00~20:00)")
    else:
        lines.append("⏰ 09:00 개장 ~ 09:30 사이 진입 권장")
    lines.append("⚠️ 투자 책임: 본인")

    return "\n".join(lines)


def format_telegram_message(picks: list[dict], ewy_signal: dict, mode: str = "confirmed") -> str:
    """
    텔레그램 송출용 HTML 메시지 (FLOWX와 거의 동일하지만 약간 간소).
    Telegram parse_mode='HTML'.
    """
    # FLOWX 포맷 재활용 (이미 HTML <b> 포함)
    return format_flowx_post(picks, ewy_signal, mode)


def send_telegram(message: str) -> bool:
    """텔레그램 전송. 실패 시 False 반환."""
    try:
        # 프로젝트 루트의 telegram_bot 모듈 사용
        from bot.telegram_bot import send_telegram_message
        send_telegram_message(message, parse_mode="HTML")
        return True
    except Exception as e:
        print(f"⚠️ 텔레그램 전송 실패(1차): {e}")
        try:
            from bot.telegram_bot import send_message
            send_message(message)
            return True
        except Exception as e2:
            print(f"⚠️ 텔레그램 전송 실패(2차): {e2}")
            return False


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--top-large", type=int, default=3, help="트랙 A 대형주 TOP N (기본 3)")
    parser.add_argument("--top-small", type=int, default=2, help="트랙 B 중소형주 TOP N (기본 2)")
    parser.add_argument("--scan-top", type=int, default=30, help="트랙 B scanner TOP N (기본 30)")
    parser.add_argument("--save", action="store_true", help="JSON 저장")
    parser.add_argument("--flowx-format", action="store_true", help="FLOWX 포맷 출력")
    parser.add_argument(
        "--mode",
        choices=["preview", "confirmed"],
        default="confirmed",
        help="preview(16:45 국장마감 프리뷰) | confirmed(07:30 미국장반영 확정)",
    )
    parser.add_argument("--send-telegram", action="store_true", help="텔레그램 송출")
    args = parser.parse_args()

    print(f"🎯 단타 TOP 픽 시작 (Dual Track · mode={args.mode})")
    print()

    universe = load_universe()
    insights = load_insights()

    # preview 모드: EWY 시그널 무시 (미국장 아직 안 열림)
    if args.mode == "preview":
        ewy_signal = {
            "ewy_1d": 0,
            "ewy_5d": 0,
            "ks200_1d": 0,
            "ks200_5d": 0,
            "source": "preview_no_us",
        }
        print("📢 프리뷰 모드 — 미국장 데이터 무시 (국장 마감 수급만)")
    else:
        ewy_signal = load_ewy_signal()
        print(f"🌍 EWY 시그널: {ewy_signal.get('source','-')} | "
              f"EWY {ewy_signal.get('ewy_1d',0):+.2f}% | KS200 5D {ewy_signal.get('ks200_5d',0):+.2f}%")
    print()

    # ─── 트랙 A: 대형주 (시총 2조+, ±15% 완화) ───
    print("[트랙 A] 대형주 스캔 (시총 2조+, 쌍매수 300억+)...")
    large_candidates = scan_large_caps()
    print(f"  ✅ 대형주 후보: {len(large_candidates)}개")
    large_filtered = apply_daytrading_filters(
        large_candidates, universe, ewy_signal, insights,
        min_final_score=LARGE_CAP_MIN_FINAL,
    )
    picks_a = large_filtered[:args.top_large]
    for p in picks_a:
        p["track"] = "A_대형주"
    print(f"  ✅ 트랙 A 최종: {len(picks_a)}개")

    # ─── 트랙 B: 중소형주 (기존 scanner, ±7% 유지) ───
    print("[트랙 B] 중소형주 선행 매집 스캔 (시총 2천억~50조, ±7%)...")
    small_candidates = scan_universe(top_n=args.scan_top)
    print(f"  ✅ 중소형 후보: {len(small_candidates)}개")
    small_filtered = apply_daytrading_filters(
        small_candidates, universe, ewy_signal, insights,
        min_final_score=MIN_FINAL_SCORE,
    )
    # 트랙 A와 중복 제거
    picks_a_codes = {p["code"] for p in picks_a}
    small_filtered = [p for p in small_filtered if p["code"] not in picks_a_codes]
    picks_b = small_filtered[:args.top_small]
    for p in picks_b:
        p["track"] = "B_중소형주"
    print(f"  ✅ 트랙 B 최종: {len(picks_b)}개")

    # 최종 결합
    picks = picks_a + picks_b
    print(f"  🎯 전체 TOP: {len(picks)}개 (A {len(picks_a)} + B {len(picks_b)})")

    # Step 3: 출력
    print()
    print("=" * 90)
    print(f"🏆 단타 TOP {len(picks)} (Dual Track)")
    print("=" * 90)
    for i, p in enumerate(picks, 1):
        track_label = "🔷대형" if p.get("track","").startswith("A_") else "🟢중소"
        print(f"{i}. {track_label} [{p['final_score']:5.1f}] {p['name']:15} ({p['code']}) "
              f"| {p.get('sector','-'):8} "
              f"| 현재 {p['close_end']:>8,} "
              f"| 진입 {p['entry_low']:>8,}~{p['entry_high']:>8,} "
              f"| 목표 {p['tp1']:>8,}")
        print(f"     └ {p['key_reasons']}")
        inst_total = p.get('inst_total_억', 0)
        dual_total = p.get('dual_total_억', p['foreign_total_억'] + inst_total)
        print(f"     └ 시총 {int(p['mcap_억']):>7,}억 | 외 {p['foreign_total_억']:>+7.1f}+기 {inst_total:>+7.1f}={dual_total:>+7.1f}억 | 기관합류 {p['inst_joining']}일 | 5D {p.get('price_change_%',0):+.1f}%")

    # 저장
    if args.save:
        out = {
            "updated": datetime.now().isoformat(),
            "mode": args.mode,
            "ewy_signal": ewy_signal,
            "config": {
                "scan_top": args.scan_top,
                "top_large": args.top_large,
                "top_small": args.top_small,
                "min_final_score": MIN_FINAL_SCORE,
                "large_cap_min_final": LARGE_CAP_MIN_FINAL,
            },
            "picks": picks,
        }
        OUT_PATH.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"\n💾 저장: {OUT_PATH}")

    # FLOWX 포맷
    if args.flowx_format:
        print()
        print("━" * 80)
        print(f"📤 FLOWX 게시용 포맷 ({args.mode}):")
        print("━" * 80)
        print(format_flowx_post(picks, ewy_signal, mode=args.mode))

    # 텔레그램 송출
    if args.send_telegram:
        print()
        print("📤 텔레그램 송출 중...")
        msg = format_telegram_message(picks, ewy_signal, mode=args.mode)
        ok = send_telegram(msg)
        if ok:
            print("✅ 텔레그램 송출 완료")
        else:
            print("❌ 텔레그램 송출 실패")

    return picks


if __name__ == "__main__":
    main()
