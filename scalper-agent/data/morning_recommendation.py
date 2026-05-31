# -*- coding: utf-8 -*-
"""
내일 추천 종목 파이프라인 (Morning Recommendation)
===================================================
5단계 분석 → 최종 추천 종목 생성

Stage 1: 16:45 저녁분석 (한국장 마감 후, 데이터 수집 완료 후)
  → 릴레이 + 사전감지 + 기술적 필터 + 뉴스AI + 교차검증
  → 1차 추천 텔레그램 리포트

Stage 2: 06:30 미국장 체크 (다음날 새벽)
  → 미국 주요지수 + 선물 체크 → 조정/유지

Stage 3: 08:50 최종 확인
  → 기존 premove_scan 스케줄과 연동
"""

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

# .env 로드 (상위 디렉터리)
from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parent.parent.parent / ".env")

logger = logging.getLogger("BH.Recommend")


@dataclass
class RecommendedStock:
    code: str
    name: str
    close: int = 0
    # 5단계 점수
    relay_score: float = 0.0       # 릴레이 교차검증 점수
    premove_score: float = 0.0     # 사전감지 점수
    tech_score: float = 0.0        # 기술적 점수 (0~5)
    news_score: float = 0.0        # 뉴스AI 점수
    nationality_score: float = 0.0 # 국적별 수급 점수
    nationality_detail: str = ""   # 국적별 수급 상세
    flow_signal: str = ""          # 국적별 행동 시그널 (STRONG_BUY~SELL)
    flow_score: float = 0.0        # 국적별 행동 예측 점수
    flow_detail: str = ""          # 국적별 행동 상세 (근거+리스크)
    cross_count: int = 0           # 교차 등장 횟수
    # 페널티 (soft scoring)
    news_penalty: float = 0.0      # 뉴스 NEGATIVE 페널티
    obv_penalty: float = 0.0       # OBV DOWN 페널티
    relative_penalty: float = 0.0  # 시장대비 약세 페널티
    # 합산
    total_score: float = 0.0
    # 시장 대비
    today_chg: float = 0.0         # 당일 절대 등락률
    relative_str: float = 0.0      # 시장 대비 상대강도
    # 진입 레벨
    entry: int = 0
    sl: int = 0
    tp: int = 0
    sl_source: str = ""
    # 소스 상세
    sources: list = field(default_factory=list)
    tech_detail: str = ""
    news_detail: str = ""
    confidence: str = ""  # HIGH / MED / LOW
    # 7 SECRET 국적 파워
    nat_power: float = 0.0           # calc_nationality_power 점수
    nat_power_grade: str = ""        # POWER_BUY / BUY / NEUTRAL / CAUTION / DANGER
    nat_power_detail: str = ""       # 요약 ("CASCADE+VPD|영국급증|콤보+1.5")
    # MOMENTUM 레짐 감지
    regime: str = "NORMAL"             # "MOMENTUM" or "NORMAL"
    regime_score: float = 0.0          # 0.0 ~ 1.0
    regime_detail: str = ""            # "VOL2.5x+기관5D"
    # 거래대금 폭발 스캐너
    tv_ratio: float = 1.0             # 거래대금 비율 (20일 평균 대비)
    tv_pattern: str = "NORMAL"        # EXPLOSION / QUIET_ACCUMULATION / EARLY_ACCUMULATION / GRADUAL_BUILDUP
    tv_score: float = 0.0             # TV 스캐너 점수 (0~100)
    tv_frgn_joined: bool = False      # 외인합류 매집 (외인 1억+ 순매수)
    # 피보나치 레벨 분석
    fib_adj: float = 0.0              # 피보나치 점수 가감 (-10 ~ +20)
    fib_position: str = ""            # 위치 설명 ("0.618 지지 부근" 등)
    fib_upside_pct: float = 0.0       # 상방 여력 (%)
    fib_downside_pct: float = 0.0     # 하방 지지까지 (%)
    sl_fib: int = 0                   # 피보나치 기반 손절가
    tp_fib: int = 0                   # 피보나치 기반 목표가
    # 명분 게이트 (4단 Stage0, 5/31) — shadow 관측: cutoff 미적용
    thesis_buckets: dict = field(default_factory=dict)  # {재료·수급·기술위치·재무: 정규화}
    thesis_strong: list = field(default_factory=list)   # 강 통과 버킷명
    thesis_pass: bool = True                            # 기본 True = fail-open
    kki_score: float = 0.0
    kki_grade: str = ""


def _aggregate_thesis_buckets(sig: dict) -> tuple:
    """44신호 → 7요소 명분 버킷 정규화 + 통과판정 (4단 Stage0, 5/31 shadow).

    {재료·수급·기술위치} 중 강(정규화≥THESIS_GATE_STRONG_NORM) ≥ THESIS_GATE_MIN_STRONG → pass.
    재무는 modifier(자격박탈 아님). 버킷 = 내부 신호들의 최대 정규화값.
    캡은 코드상 신호 상한(잠정 — backtest 보정 대상). 반환 (buckets, strong, thesis_pass).
    """
    from data.sajang_rules import SAJANG

    def nrm(v, cap):
        return min(1.0, max(0.0, float(v or 0)) / cap) if cap > 0 else 0.0

    재료 = max(nrm(sig.get("tv_direct"), 30), nrm(sig.get("relay_sc"), 45),
              nrm(sig.get("premove_sc"), 30), nrm(sig.get("surge_sc"), 10),
              nrm(sig.get("us_relay_sc"), 10), nrm(sig.get("rotation_bonus"), 12))
    수급 = max(nrm(sig.get("nat_sc"), 50), nrm(sig.get("nat_power_sc"), 30),
              nrm(sig.get("doublebuy_sc"), 18), nrm(sig.get("largecap_sc"), 34),
              nrm(sig.get("fi_sc"), 15), nrm(sig.get("bomb_sc"), 15),
              nrm(sig.get("dual_buy_sc"), 12), nrm(sig.get("invflow_sc"), 15),
              nrm(sig.get("stflow_sc"), 15), nrm(sig.get("etf_flow_sc"), 15))
    기술위치 = max(nrm(sig.get("tech_sc"), 25), nrm(sig.get("trix_sc"), 25),
                nrm(sig.get("fib_adj"), 20))
    재무 = nrm(sig.get("pension_sc"), 15)
    buckets = {"재료": round(재료, 3), "수급": round(수급, 3),
               "기술위치": round(기술위치, 3), "재무": round(재무, 3)}
    strong = [k for k in ("재료", "수급", "기술위치")
              if buckets[k] >= SAJANG.THESIS_GATE_STRONG_NORM]
    thesis_pass = len(strong) >= SAJANG.THESIS_GATE_MIN_STRONG
    return buckets, strong, thesis_pass


@dataclass
class RecommendationReport:
    stage: str = ""  # "evening" / "us_check" / "morning"
    timestamp: str = ""
    market_health: str = ""
    market_change: float = 0.0     # 당일 시장(KOSPI) 등락률
    us_market_note: str = ""
    stocks: list = field(default_factory=list)  # list[RecommendedStock]
    relay_summary: str = ""
    warning: str = ""
    etf_signal: dict = field(default_factory=dict)  # 위기 ETF 시그널
    # NIGHTWATCH 채권 자경단 신호등
    cross_regime: str = ""         # NORMAL / CORRECTION / DIVERGENCE
    cross_regime_detail: str = ""  # "S&P -1.3% + TNX +0.12% → 자경단"
    # 소형주 급등 후보 (Momentum Hunter)
    momentum_stocks: list = field(default_factory=list)
    # 전쟁→재건 섹터릴레이 종목 (War Relay)
    war_relay_stocks: list = field(default_factory=list)
    # 섹터 로테이션 시그널
    rotation_signal: str = ""      # "HOT: 방산(MID 3D) | NEXT: 반도체(스테이징)"
    rotation_detail: list = field(default_factory=list)  # 섹터별 상세
    # TV 클러스터 (그룹/섹터 동시 폭발)
    tv_cluster_info: list = field(default_factory=list)
    # 원자재 릴레이 상황
    commodity_info: dict = field(default_factory=dict)
    # 이벤트 캘린더 리스크
    event_risk: dict = field(default_factory=dict)
    # ETF 추천 (Phase 2)
    etf_recommendations: list = field(default_factory=list)  # list[dict] from RecommendedETF


# ═══════════════════════════════════════
#  Step 1: 릴레이 스캔
# ═══════════════════════════════════════

def _step1_relay_scan() -> dict:
    """릴레이 허브 → 교차검증 종목 + 섹터 상태"""
    try:
        from data.relay_hub import scan_relay_all
        report = scan_relay_all()

        relay_stocks = {}
        for sig in report.unified_signals:
            relay_stocks[sig.code] = {
                "code": sig.code,
                "name": sig.name,
                "close": sig.close,
                "signal_count": sig.signal_count,
                "confidence": sig.confidence,
                "sources": list(sig.sources),
                "change_5d": sig.change_5d,
            }

        # 섹터 요약
        summary_lines = []
        for s in report.hot_sectors:
            summary_lines.append(f"HOT:{s.sector_name}")
        for s in report.relay_sectors:
            summary_lines.append(f"RELAY:{s.sector_name}")
        for g in report.hot_groups:
            summary_lines.append(f"HOT:{g.group_name}")
        for g in report.relay_groups:
            summary_lines.append(f"RELAY:{g.group_name}")

        return {
            "stocks": relay_stocks,
            "summary": " | ".join(summary_lines) if summary_lines else "활성 없음",
            "all_sectors": report.all_sectors,  # 로테이션 분석용 전체 섹터
        }
    except Exception as e:
        logger.error(f"릴레이 스캔 실패: {e}")
        return {"stocks": {}, "summary": f"실패: {e}", "all_sectors": []}


# ═══════════════════════════════════════
#  Step 2: 사전감지 스캔
# ═══════════════════════════════════════

def _step2_premove_scan() -> dict:
    """사전감지 → 폭발 직전 종목"""
    try:
        from data.premove_scanner import scan_premove
        candidates = scan_premove(top_n=10)

        premove_stocks = {}
        for c in candidates:
            premove_stocks[c.code] = {
                "code": c.code,
                "name": c.name,
                "close": c.close,
                "premove_score": c.premove_score,
                "signal_count": c.signal_count,
                "signals": [s["type"] for s in c.signals] if c.signals else [],
                "entry": c.entry,
                "sl": c.sl,
                "tp": c.tp2,
                "tp1_quick": c.tp1_quick,
                "sl_source": c.sl_source,
            }
        return {"stocks": premove_stocks}
    except Exception as e:
        logger.error(f"사전감지 실패: {e}")
        return {"stocks": {}}


# ═══════════════════════════════════════
#  Step 2.5: MACD 제로선 크로스 스캔
# ═══════════════════════════════════════

def _step_macd_zero_scan() -> dict:
    """MACD 음→0선 골든크로스 + 수급/거래량 폭발 종목"""
    try:
        from strategies.macd_zero_scanner import scan_phase1, check_phase2, load_watchlist
        # Phase1: 신규 MACD 크로스 종목
        new_signals = scan_phase1()
        # Phase2: 기존 감시 종목 중 진입 시그널
        watchlist = load_watchlist()
        entries = check_phase2(watchlist)

        macd_stocks = {}
        for s in new_signals[:10]:
            macd_stocks[s["code"]] = {
                "name": s.get("name", s["code"]),
                "source": "macd_zero_phase1",
                "flow_ratio": s.get("flow_ratio", 0),
                "vol_ratio": s.get("vol_ratio", 0),
                "cross_price": s.get("cross_price", 0),
            }
        for e in entries:
            macd_stocks[e["code"]] = {
                "name": e.get("name", e["code"]),
                "source": "macd_zero_phase2",
                "entry": e.get("entry_price", 0),
                "sl": e.get("sl", 0),
                "tp": e.get("tp", 0),
                "drawdown": e.get("drawdown", 0),
            }

        logger.info(f"MACD 0선: Phase1 {len(new_signals)}건, Phase2 진입 {len(entries)}건")
        return macd_stocks
    except Exception as e:
        logger.warning(f"MACD 0선 스캔 실패: {e}")
        return {}


# ═══════════════════════════════════════
#  Step 2.6: TRIX 다이버전스 스캔
# ═══════════════════════════════════════

def _step_trix_divergence_scan() -> dict:
    """TRIX 상승 다이버전스 + ADX 추세 확인 종목"""
    # CTO 피처 플래그 체크
    try:
        from bot.trading_cto import TradingCTO
        _cto_check_path = Path(__file__).resolve().parent.parent / "data_store" / "cto_features.json"
        if _cto_check_path.exists():
            _cto = TradingCTO(config={})
            if not _cto.is_enabled("trix_divergence"):
                logger.info("TRIX: CTO 피처 비활성 - 스킵")
                return {}
    except Exception:
        pass  # CTO 실패 시 기존 동작 유지
    try:
        from strategies.trix_divergence import scan_trix_divergence, load_trix_cache
        # 캐시 확인 (3시간 이내면 재사용)
        cached = load_trix_cache()
        if cached:
            logger.info(f"TRIX 다이버전스: 캐시 {len(cached)}종목 재사용")
            return cached

        # 신규 스캔
        signals = scan_trix_divergence(top_n=10)
        trix_stocks = {}
        for s in signals:
            trix_stocks[s["code"]] = {
                "name": s.get("name", s["code"]),
                "source": "trix_divergence",
                "div_strength": s.get("div_strength", 0),
                "adx": s.get("adx", 0),
                "trix_cross": s.get("trix_cross", "neutral"),
                "composite_score": s.get("composite_score", 0),
            }
        logger.info(f"TRIX 다이버전스: {len(trix_stocks)}종목 감지")
        return trix_stocks
    except Exception as e:
        logger.warning(f"TRIX 다이버전스 스캔 실패: {e}")
        return {}


# ═══════════════════════════════════════
#  Step 2.7: 줍줍 스캔 (낙폭+수급매집)
# ═══════════════════════════════════════

def _step_bargain_scan() -> dict:
    """전쟁전 고점 대비 낙폭 + 기관/외인 진성매집 종목 발굴"""
    try:
        from strategies.bargain_scanner import scan_bargain
        from data.kis_collector import UNIVERSE
        candidates = scan_bargain(universe=UNIVERSE, top_n=10)

        bargain_stocks = {}
        for c in candidates:
            bargain_stocks[c.code] = {
                "name": c.name,
                "close": c.close,
                "source": "bargain",
                "bargain_score": c.bargain_score,
                "drop_pct": c.drop_pct,
                "supply_grade": c.supply_grade,
                "foreign_10d": c.foreign_10d,
                "inst_10d": c.inst_10d,
                "pre_war_high": c.pre_war_high,
            }

        logger.info(f"줍줍 스캔: {len(bargain_stocks)}종목")
        return bargain_stocks
    except Exception as e:
        logger.warning(f"줍줍 스캔 실패: {e}")
        return {}


# ═══════════════════════════════════════
#  Step 2.9: 전쟁→재건 섹터릴레이 워치리스트
# ═══════════════════════════════════════

def _step_war_relay_inject() -> list[dict]:
    """전쟁→재건 섹터릴레이 워치리스트 로드

    data_store/war_relay_watchlist.json에서 사전 분석된 종목을 로드.
    각 종목의 entry/sl/tp는 기술적 분석(Fib/EMA/BB)으로 사전 계산됨.

    Returns: list of war_relay stock dicts (watchlist 원본 + 실시간 가격 업데이트)
    """
    import json
    from pathlib import Path

    watchlist_path = (
        Path(__file__).resolve().parent.parent
        / "data_store" / "war_relay_watchlist.json"
    )
    if not watchlist_path.exists():
        logger.info("전쟁릴레이 워치리스트 없음 - 스킵")
        return []

    try:
        with open(watchlist_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        # enabled=false → 평시 전환 (전쟁릴레이 비활성)
        if not data.get("enabled", True):
            logger.info("전쟁릴레이 비활성 (enabled=false) - 스킵")
            return []

        stocks = data.get("stocks", [])
        if not stocks:
            return []

        # 실시간 종가 업데이트 (CSV 일봉 기준)
        for s in stocks:
            code = s["code"]
            csv_path = (
                Path(__file__).resolve().parent.parent
                / "data_store" / "daily" / f"{code}.csv"
            )
            if csv_path.exists():
                import pandas as pd
                df = pd.read_csv(csv_path)
                if len(df) > 0:
                    s["close"] = int(df.iloc[-1, 4])  # 종가 column
                    s["last_date"] = str(df.iloc[-1, 0])  # 날짜 column

        logger.info(f"전쟁릴레이 워치리스트: {len(stocks)}종목 로드")
        return stocks
    except Exception as e:
        logger.warning(f"전쟁릴레이 워치리스트 로드 실패: {e}")
        return []


# ═══════════════════════════════════════
#  시장 등락률 조회 (상대강도 기준)
# ═══════════════════════════════════════

def _get_market_change_today() -> float:
    """오늘 KOSPI(KODEX200) 등락률 - 상대강도 계산 기준"""
    try:
        from pykrx import stock
        from datetime import datetime, timedelta
        end = datetime.now()
        start = end - timedelta(days=10)
        df = stock.get_market_ohlcv(
            start.strftime("%Y%m%d"), end.strftime("%Y%m%d"), "069500"
        )
        if df is not None and len(df) >= 2:
            chg = (float(df["종가"].iloc[-1]) / float(df["종가"].iloc[-2]) - 1) * 100
            return round(chg, 2)
    except Exception as e:
        logger.warning(f"시장 등락률 조회 실패: {e}")
    return 0.0


# ═══════════════════════════════════════
#  Step 3: 기술적 분석 (Soft Scoring)
# ═══════════════════════════════════════

def _step3_tech_filter(codes_names: list[tuple[str, str]], market_chg: float = 0.0) -> dict:
    """EMA/RSI/MACD/OBV 기술적 점수 계산 + 상대강도 (hard cutoff 제거)

    v2: 절대값 -5% 탈락 → 시장대비 상대강도(relative_str) 계산
        모든 종목이 점수를 받음 (탈락 없음)

    Args:
        market_chg: 당일 시장(KOSPI) 등락률. 상대강도 = 종목등락률 - 시장등락률

    Returns: {code: {"score": 0~5, "detail": "...", "today_chg": -8.2,
                      "relative_str": +2.1, "close": 57600}}
    """
    from pykrx import stock
    from datetime import datetime, timedelta
    import numpy as np

    results = {}
    end = datetime.now()
    start = end - timedelta(days=120)
    start_s = start.strftime("%Y%m%d")
    end_s = end.strftime("%Y%m%d")
    today_s = end.strftime("%Y-%m-%d")

    # 데이터 날짜 검증 플래그
    data_date_ok = None

    for code, name in codes_names:
        try:
            df = stock.get_market_ohlcv(start_s, end_s, code)
            if df is None or len(df) < 60:
                results[code] = {
                    "score": 0, "detail": "데이터부족",
                    "today_chg": 0, "relative_str": 0, "close": 0,
                    "rsi": 50, "obv_dir": "FLAT", "upper_wick_pct": 0,
                }
                continue

            # 데이터 날짜 검증 (첫 종목에서 1회만)
            if data_date_ok is None:
                last_date = str(df.index[-1].date())
                if last_date == today_s:
                    data_date_ok = True
                else:
                    data_date_ok = False
                    logger.warning(f"pykrx 데이터 날짜 불일치: 최신={last_date}, 오늘={today_s}")

            close = df["종가"].astype(float).values
            volume = df["거래량"].astype(float).values

            # 당일 등락률 + 상대강도
            today_chg = 0.0
            if len(close) >= 2:
                today_chg = (close[-1] / close[-2] - 1) * 100
            relative_str = round(today_chg - market_chg, 2)

            # EMA 계산
            def ema(arr, period):
                result = np.zeros_like(arr, dtype=float)
                result[0] = arr[0]
                k = 2.0 / (period + 1)
                for i in range(1, len(arr)):
                    result[i] = arr[i] * k + result[i - 1] * (1 - k)
                return result

            ema5 = ema(close, 5)
            ema20 = ema(close, 20)
            ema60 = ema(close, 60)

            # RSI 14
            deltas = np.diff(close)
            gains = np.where(deltas > 0, deltas, 0)
            losses = np.where(deltas < 0, -deltas, 0)
            avg_gain = np.mean(gains[-14:])
            avg_loss = np.mean(losses[-14:])
            rsi = 100 - (100 / (1 + avg_gain / avg_loss)) if avg_loss > 0 else 100

            # MACD
            ema12 = ema(close, 12)
            ema26 = ema(close, 26)
            macd_line = ema12 - ema26
            macd_signal = ema(macd_line, 9)
            macd_hist = macd_line[-1] - macd_signal[-1]

            # OBV 방향 (5일)
            obv = np.zeros(len(close))
            for i in range(1, len(close)):
                if close[i] > close[i - 1]:
                    obv[i] = obv[i - 1] + volume[i]
                elif close[i] < close[i - 1]:
                    obv[i] = obv[i - 1] - volume[i]
                else:
                    obv[i] = obv[i - 1]

            obv_5d_change = obv[-1] - obv[-5] if len(obv) >= 5 else 0
            obv_dir = "UP" if obv_5d_change > 0 else "DOWN"

            # 점수 계산 (0~5, hard cutoff 없음)
            score = 0.0
            details = []

            # 1. EMA 정배열 (5>20>60)
            if ema5[-1] > ema20[-1] > ema60[-1]:
                score += 1.0
                details.append("정배열")
            elif ema5[-1] < ema20[-1] < ema60[-1]:
                details.append("역배열")

            # 2. EMA20 > EMA60 (중기 추세)
            if ema20[-1] > ema60[-1]:
                score += 1.0
                details.append("EMA20>60")

            # 3. RSI 30~55 (매수 적합 구간)
            if 30 <= rsi <= 55:
                score += 1.0
                details.append(f"RSI{rsi:.0f}")
            elif rsi > 70:
                score -= 0.5
                details.append(f"RSI{rsi:.0f}과열")

            # 4. MACD 양수 또는 히스토그램 양전환
            if macd_hist > 0:
                score += 1.0
                details.append("MACD+")

            # 5. OBV 방향
            if obv_dir == "UP":
                score += 1.0
                details.append("OBV+")
            else:
                score -= 0.5
                details.append("OBV-")

            # 상대강도 표시
            details.append(f"RS{relative_str:+.0f}%")

            # ── 위꼬리 비율 계산 (최근 3일 평균) ──
            upper_wick_avg = 0.0
            try:
                high_arr = df["고가"].astype(float).values
                low_arr = df["저가"].astype(float).values
                open_arr = df["시가"].astype(float).values
                recent_n = min(3, len(close))
                wick_ratios = []
                for i in range(-recent_n, 0):
                    h, l = high_arr[i], low_arr[i]
                    body_high = max(open_arr[i], close[i])
                    rng = h - l
                    if rng > 0:
                        wick_ratios.append((h - body_high) / rng * 100)
                if wick_ratios:
                    upper_wick_avg = round(sum(wick_ratios) / len(wick_ratios), 1)
            except Exception:
                pass

            results[code] = {
                "score": max(0, round(score, 1)),
                "detail": "+".join(details) if details else "N/A",
                "rsi": round(rsi, 1),
                "obv_dir": obv_dir,
                "today_chg": round(today_chg, 1),
                "relative_str": relative_str,
                "close": int(close[-1]),
                "upper_wick_pct": upper_wick_avg,
            }
        except Exception as e:
            logger.warning(f"기술 분석 실패 {name}({code}): {e}")
            results[code] = {
                "score": 0, "detail": f"오류:{e}",
                "today_chg": 0, "relative_str": 0, "close": 0,
                "rsi": 50, "obv_dir": "FLAT", "upper_wick_pct": 0,
            }

    if data_date_ok is False:
        logger.error("pykrx 당일 데이터 미반영 - 추천 정확도 저하 가능")

    return results


# ═══════════════════════════════════════
#  Step 4: 뉴스AI 필터
# ═══════════════════════════════════════

def _step4_news_filter(codes_names: list[tuple[str, str]],
                       max_new_stocks: int = 40) -> dict:
    """뉴스AI로 네거티브 종목 제거 (캐시 활용 + 종목 수 제한)

    Args:
        max_new_stocks: 캐시 미스 시 최대 신규 분석 종목 수

    Returns: {code: {"sentiment": "...", "reason": "...", "score": N}}
    """
    import json
    from datetime import datetime
    from pathlib import Path

    news_map = {}

    # 오늘 캐시 로드 (사전감지에서 이미 분석한 종목)
    today = datetime.now().strftime("%Y%m%d")
    cache_path = Path(__file__).resolve().parent.parent / "data_store" / "news_ai" / f"news_ai_{today}.json"
    cached = {}
    if cache_path.exists():
        try:
            with open(cache_path, "r", encoding="utf-8") as f:
                for item in json.load(f):
                    grade = item.get("news_grade", "NEUTRAL")
                    # news_grade → sentiment 매핑
                    if grade in ("STRONG_NEGATIVE", "NEGATIVE"):
                        sentiment = "NEGATIVE"
                    elif grade in ("STRONG_POSITIVE", "POSITIVE"):
                        sentiment = "POSITIVE"
                    else:
                        sentiment = "NEUTRAL"
                    cached[item["code"]] = {
                        "sentiment": sentiment,
                        "reason": item.get("ai_summary", ""),
                        "score": item.get("news_score", 0),
                    }
            logger.info(f"뉴스AI 캐시: {len(cached)}종목 로드")
        except Exception as e:
            logger.warning(f"뉴스AI 캐시 로드 실패: {e}")

    # 캐시에 있는 종목은 재사용
    need_analysis = []
    for code, name in codes_names:
        if code in cached:
            news_map[code] = cached[code]
        else:
            need_analysis.append((code, name))

    if not need_analysis:
        logger.info(f"뉴스AI: 전종목 캐시 히트 ({len(news_map)}건)")
        return news_map

    # 캐시 미스 종목이 max_new_stocks 초과 시 제한
    if len(need_analysis) > max_new_stocks:
        logger.info(f"뉴스AI: 미분석 {len(need_analysis)}종목 → 상위 {max_new_stocks}종목만 분석")
        need_analysis = need_analysis[:max_new_stocks]

    # 미분석 종목만 뉴스AI 실행
    try:
        from data.news_ai_scanner import scan_news_ai
        targets = [{"code": c, "name": n} for c, n in need_analysis]
        logger.info(f"뉴스AI: {len(need_analysis)}종목 신규 분석 (캐시 {len(news_map)}건)")
        results = scan_news_ai(targets)

        for r in results:
            grade = r.news_grade
            if grade in ("STRONG_NEGATIVE", "NEGATIVE"):
                sentiment = "NEGATIVE"
            elif grade in ("STRONG_POSITIVE", "POSITIVE"):
                sentiment = "POSITIVE"
            else:
                sentiment = "NEUTRAL"
            news_map[r.code] = {
                "sentiment": sentiment,
                "reason": r.ai_summary if hasattr(r, "ai_summary") else "",
                "score": r.news_score if hasattr(r, "news_score") else 0,
            }
    except Exception as e:
        logger.warning(f"뉴스AI 실패: {e} - 미분석 종목 NEUTRAL 처리")

    # 누락 종목 NEUTRAL 처리
    for code, _ in codes_names:
        if code not in news_map:
            news_map[code] = {"sentiment": "NEUTRAL", "reason": "미분석", "score": 0}

    return news_map


# ═══════════════════════════════════════
#  Brain Insights (학습 피드백 루프)
# ═══════════════════════════════════════
_BRAIN_CACHE = {}  # 세션 내 1회만 로드


def _brain_stock_cap(pct: int) -> int:
    """BRAIN 비중(%) → 추천 종목 수 캡 (비표준 pct도 안전 처리, 최소 2)"""
    _CAP = {100: 8, 70: 5, 50: 3, 30: 2, 0: 2}
    cap = _CAP.get(pct)
    if cap is None:
        for k in sorted(_CAP.keys(), reverse=True):
            if pct >= k:
                cap = _CAP[k]
                break
        else:
            cap = 2
    return max(cap, 2)


def _load_brain_insights() -> dict:
    """daily_learner가 생성한 insights.json 로드

    전날 학습 결과가 오늘 추천 스코어링에 반영됩니다.
    """
    if _BRAIN_CACHE:
        return _BRAIN_CACHE

    from pathlib import Path as _PathBrain
    insights_path = _PathBrain(__file__).resolve().parent.parent / "data_store" / "learning" / "insights.json"
    if not insights_path.exists():
        logger.info("[Brain] insights.json 없음 — 학습 데이터 축적 중")
        return {}

    try:
        import json as _json_brain
        data = _json_brain.loads(insights_path.read_text("utf-8"))
        days = data.get("score_adj_applied", 0)
        src_cnt = len(data.get("source_weights", {}))
        sect_cnt = len(data.get("sector_boost", {}))
        logger.info(
            f"[Brain] 인사이트 로드: {days}일 학습, "
            f"{src_cnt}개 소스 가중치, {sect_cnt}개 섹터 부스트"
        )
        _BRAIN_CACHE.update(data)
        return data
    except Exception as e:
        logger.warning(f"[Brain] insights.json 로드 실패: {e}")
        return {}


def _apply_brain_adjustment(
    insights: dict, sources: list, name: str, raw_total: float
) -> float:
    """학습 인사이트 기반 점수 조정

    Returns: 가산/감산 점수 (양수 = 부스트, 음수 = 페널티)
    """
    if not insights:
        return 0.0

    adj = 0.0

    # ── A. 소스 가중치 기반 조정 ──
    # 소스별 학습된 신뢰도에 따라 raw_total에 보정
    sw = insights.get("source_weights", {})
    if sw and sources:
        weight_sum = 0.0
        weight_count = 0
        for src in sources:
            src_key = src.split(":")[0] if ":" in src else src
            if src_key in sw:
                weight_sum += sw[src_key]["weight"]
                weight_count += 1

        if weight_count > 0:
            avg_weight = weight_sum / weight_count
            # 기본 가중치(1.0) 대비 편차를 점수에 반영 (최대 ±15%)
            source_adj = raw_total * (avg_weight - 1.0) * 0.5  # 50% 감쇠
            source_adj = max(-raw_total * 0.15, min(raw_total * 0.15, source_adj))
            adj += source_adj

    # ── B. 누락 섹터 부스트 ──
    # 반복적으로 놓친 섹터에 해당하는 종목 부스트
    sb = insights.get("sector_boost", {})
    if sb:
        for sector, boost_info in sb.items():
            # 종목명에 섹터 키워드가 포함되는지 간단 매칭
            sector_kw = {
                "반도체": ["하이닉스", "삼성전자", "한미반도체", "ISC", "원익", "테스"],
                "2차전지": ["에코프로", "포스코퓨처", "엘앤에프", "LG에너지", "삼성SDI"],
                "방산": ["한화에어로", "현대로템", "LIG넥스원", "한국항공", "풍산"],
                "건설": ["삼성E&A", "현대건설", "GS건설", "DL이앤씨", "대우건설"],
                "전력": ["한전", "HD현대일렉", "일진전기", "제룡전기", "LS일렉"],
                "바이오": ["삼바", "셀트리온", "알테오젠", "리가켐"],
                "금융": ["KB금융", "신한지주", "하나금융", "BNK"],
                "자동차": ["현대차", "기아", "현대모비스"],
                "조선": ["HD한국조선", "삼성중공업", "한화오션"],
                "AI/SW": ["네이버", "카카오", "더존비즈온"],
            }
            if sector in sector_kw:
                if any(kw in name for kw in sector_kw[sector]):
                    adj += boost_info["boost"]
                    break  # 1개 섹터만

    return round(adj, 1)


# ═══════════════════════════════════════
#  TV 클러스터 감지 (그룹/섹터 동시 폭발)
# ═══════════════════════════════════════

# 추가 재벌그룹 (group_relay.py 7대 그룹 외)
_EXTRA_GROUPS = {
    "shinsegae": {
        "name": "신세계",
        "codes": ["004170", "031430", "035510", "139480", "034300", "160600"],
        # 신세계, 신세계인터내셔날, 신세계I&C, 이마트, 신세계건설, 신세계푸드
    },
    "doosan": {
        "name": "두산",
        "codes": ["000150", "131970", "042660", "298040", "336260"],
        # 두산, 두산테스나, 한화오션(구 두산인프라코어), 두산로보틱스, 두산퓨얼셀
    },
    "lotte": {
        "name": "롯데",
        "codes": ["004990", "023530", "004000", "071050", "002270"],
        # 롯데지주, 롯데쇼핑, 롯데정밀화학, 한국금융지주(롯데카드), 롯데제과
    },
    "cj": {
        "name": "CJ",
        "codes": ["001040", "097950", "079160", "000120"],
        # CJ, CJ제일제당, CJ CGV, CJ대한통운
    },
    "ls": {
        "name": "LS",
        "codes": ["006260", "010120", "417200", "112040"],
        # LS, LS ELECTRIC, LS머트리얼즈, LS에코에너지
    },
}


def _detect_tv_clusters(tv_signals: dict, universe: dict) -> dict:
    """동일 그룹/섹터에서 TV 시그널 다발 감지 → 클러스터 보너스

    Returns:
        tv_cluster_map: {code: cluster_bonus_점수}
        cluster_info: [{group_name, codes, patterns}] (텔레그램 표시용)
    """
    if not tv_signals:
        return {}, []

    # 1) 그룹별 매핑: code → group_name
    code_to_group = {}
    try:
        from data.group_relay import GROUPS
        for gkey, gdata in GROUPS.items():
            gname = gdata["name"]
            leader_code = gdata["leader"][0]
            code_to_group[leader_code] = gname
            for aff_code, _ in gdata.get("affiliates", []):
                code_to_group[aff_code] = gname
            for sob_code, _ in gdata.get("sobujan", []):
                code_to_group[sob_code] = gname
    except ImportError:
        pass

    for gkey, gdata in _EXTRA_GROUPS.items():
        gname = gdata["name"]
        for c in gdata["codes"]:
            code_to_group[c] = gname

    # 2) 섹터별 매핑: code → sector
    code_to_sector = {}
    for code, info in universe.items():
        if isinstance(info, dict):
            sec = info.get("sector", "")
            if sec:
                code_to_sector[code] = sec

    # 3) TV 시그널을 그룹/섹터별로 그룹핑
    group_tv = {}   # {group_name: [(code, name, pattern, score, tv_ratio)]}
    sector_tv = {}  # {sector: [(code, name, pattern, score, tv_ratio)]}

    for code, sig in tv_signals.items():
        if isinstance(sig, dict):
            sc = sig.get("score", 0)
            pat = sig.get("pattern", "")
            name = sig.get("name", "")
            tvr = sig.get("tv_ratio", 1.0)
        else:
            sc = getattr(sig, "score", 0)
            pat = getattr(sig, "pattern", "")
            name = getattr(sig, "name", "")
            tvr = getattr(sig, "tv_ratio", 1.0)

        if sc < 60:
            continue

        # EXPLOSION 패턴은 클러스터 보너스 제외 (실측 -1.42% 역효과)
        if pat == "EXPLOSION":
            continue

        entry = (code, name, pat, sc, tvr)

        # 그룹 클러스터
        grp = code_to_group.get(code)
        if grp:
            group_tv.setdefault(grp, []).append(entry)

        # 섹터 클러스터
        sec = code_to_sector.get(code)
        if sec:
            sector_tv.setdefault(sec, []).append(entry)

    # 4) 클러스터 보너스 계산
    tv_cluster_map = {}
    cluster_info = []

    # 그룹 클러스터 (2개+ 시그널)
    for grp_name, entries in group_tv.items():
        if len(entries) < 2:
            continue
        n = len(entries)
        if n >= 4:
            bonus = 15
        elif n >= 3:
            bonus = 12
        else:
            bonus = 8

        # QUIET_ACCUMULATION 다중 동시 보너스 제거 (5/9 학습: QA 80+ 적중 0~9%)
        # qa_count 기반 보너스 폐지

        for code, name, pat, sc, tvr in entries:
            tv_cluster_map[code] = max(tv_cluster_map.get(code, 0), bonus)

        cluster_info.append({
            "type": "group",
            "name": grp_name,
            "count": n,
            "bonus": bonus,
            "members": [(name, pat, tvr) for _, name, pat, _, tvr in entries],
        })

    # 섹터 클러스터 (3개+ 시그널 — 섹터는 종목수 많으니 기준 높임)
    for sec_name, entries in sector_tv.items():
        if len(entries) < 3:
            continue
        n = len(entries)
        if n >= 6:
            bonus = 12
        elif n >= 4:
            bonus = 8
        else:
            bonus = 5

        for code, name, pat, sc, tvr in entries:
            # 그룹 보너스와 중복 시 큰 값만
            tv_cluster_map[code] = max(tv_cluster_map.get(code, 0), bonus)

        cluster_info.append({
            "type": "sector",
            "name": sec_name,
            "count": n,
            "bonus": bonus,
            "members": [(name, pat, tvr) for _, name, pat, _, tvr in entries],
        })

    # ── 클러스터 과열 감지: 3~4개 동시 → 보너스 50% 감소 (실측 -1.56% 역효과) ──
    n_clusters = len(cluster_info)
    if 3 <= n_clusters <= 4:
        for code in tv_cluster_map:
            tv_cluster_map[code] = max(1, int(tv_cluster_map[code] * 0.5))
        logger.info(f"[TV Cluster] 과열 감지: {n_clusters}개 클러스터 → 보너스 50% 감소")

    if cluster_info:
        logger.info(
            f"[TV Cluster] {n_clusters}개 클러스터 감지: "
            + ", ".join(f"{c['name']}({c['count']}종목,+{c['bonus']})" for c in cluster_info)
        )

    return tv_cluster_map, cluster_info


# ═══════════════════════════════════════
#  Step 5: 교차검증 + 최종 랭킹
# ═══════════════════════════════════════

def _step5_cross_validate(
    relay: dict, premove: dict, tech: dict, news: dict,
    macd_result: dict = None,
    nationality: dict = None,
    bargain_result: dict = None,
    market_chg: float = 0.0,
    regime_info: dict = None,
    shock_info: dict = None,
    rotation_stocks: dict = None,
    tv_signals: dict = None,
    tv_cluster_map: dict = None,
    tv_persistence: dict = None,
    trix_result: dict = None,
) -> list[RecommendedStock]:
    """모든 스텝 결과 통합 → Soft Scoring → 최종 랭킹

    v2: Hard gate 제거 → 모든 요소를 점수로 변환
    v3: 줍줍(bargain) 소스 추가 - 낙폭+수급매집 종목
    v4: 거래대금 폭발 소스 추가 - TV 스캐너 교차검증
    v4: CORTEX 체제별 점수 배수 + 충격 섹터 페널티/보너스
    v5: 로테이션 디텍터 - 다음 섹터 종목 보너스 + 반전 섹터 페널티
    v6: TRIX 다이버전스 - 바닥 반전 시그널 보조 점수
    """
    if macd_result is None:
        macd_result = {}
    if nationality is None:
        nationality = {}
    if bargain_result is None:
        bargain_result = {}
    if regime_info is None:
        regime_info = {}
    if shock_info is None:
        shock_info = {}
    if rotation_stocks is None:
        rotation_stocks = {}
    if trix_result is None:
        trix_result = {}

    # ── 학습 인사이트 로드 (피드백 루프) ──
    _brain_insights = _load_brain_insights()

    # CORTEX 체제 — 참고 정보 전용 (점수 차단 X)
    # v4.1: 패닉/충격에서도 추천을 차단하지 않음
    # → 대신 피보나치 레벨로 매수 기회 계산
    regime_multipliers = {
        "NORMAL": 1.0,
        "CAUTION": 1.0,
        "SHOCK": 1.0,
        "PANIC": 1.0,
        "RECOVERY_EARLY": 1.15,      # 줍줍 소폭 부스트
        "RECOVERY_CONFIRMED": 1.1,
    }
    regime = regime_info.get("regime", "NORMAL")
    regime_mult = regime_multipliers.get(regime, 1.0)

    # 충격 섹터 정보
    affected_sectors = shock_info.get("affected_sectors", [])
    opportunity_sectors = shock_info.get("opportunity_sectors", [])

    # ── v4.3: bomb 보너스 맵 (경량 패스 — 7SECRET 스킵, 합류 후 스코어링) ──
    # v4.1: bomb→all_codes→7SECRET 폭증→타임아웃 / v4.2: 교집합만→교집합0=무용지물
    # v4.3: 7SECRET 이전에는 추가 안 함 → 7SECRET 완료 후 bomb-only 합류 (경량 패스)
    _bomb_map = {}  # {code: {bomb_adj, signal, name, ...}}
    try:
        import json as _json_bw
        _bw_path = Path(__file__).resolve().parent.parent / "data_store" / "bomb_watchlist.json"
        if _bw_path.exists():
            _bw_data = _json_bw.loads(_bw_path.read_text(encoding="utf-8"))
            for bw in _bw_data.get("watchlist", []):
                _bomb_map[bw["code"]] = bw
        if _bomb_map:
            logger.info(f"[step5] bomb 맵: {len(_bomb_map)}종목 (v4.3 경량패스)")
    except Exception as _bw_e:
        logger.warning(f"[step5] bomb 맵 로드 실패(무시): {_bw_e}")

    # ── 대량 쌍매수 맵 (C39 결과) ──
    _dual_buy_map = {}  # {code: {total_억, foreign_억, inst_億, ...}}
    try:
        import json as _json_db
        _db_path = Path(__file__).resolve().parent.parent / "data_store" / "massive_dual_buy.json"
        if _db_path.exists():
            _db_data = _json_db.loads(_db_path.read_text(encoding="utf-8"))
            for st in _db_data.get("stocks", []):
                _dual_buy_map[st["code"]] = st
        if _dual_buy_map:
            logger.info(f"[step5] 대량쌍매수 맵: {len(_dual_buy_map)}종목")
    except Exception as _db_e:
        logger.warning(f"[step5] 대량쌍매수 맵 로드 실패(무시): {_db_e}")

    # ── 연속급등 맵 (C39 결과) ──
    _surge_map = {}  # {code: {change_pct, ...}}
    try:
        import json as _json_sg
        _sg_path = Path(__file__).resolve().parent.parent / "data_store" / "consecutive_surge.json"
        if _sg_path.exists():
            _sg_data = _json_sg.loads(_sg_path.read_text(encoding="utf-8"))
            for st in _sg_data.get("stocks", []):
                if st.get("code"):
                    _surge_map[st["code"]] = st
        if _surge_map:
            logger.info(f"[step5] 연속급등 맵: {len(_surge_map)}종목")
    except Exception as _sg_e:
        logger.warning(f"[step5] 연속급등 맵 로드 실패(무시): {_sg_e}")

    # ── 수급 강도 맵 (C35 flow_intensity 결과) ──
    _flow_intensity_map = {}  # {code: {name, quality_score, intensity_pct, ...}}
    try:
        import json as _json_fi
        _fi_path = Path(__file__).resolve().parent.parent / "data_store" / "flow_intensity.json"
        if _fi_path.exists():
            _fi_data = _json_fi.loads(_fi_path.read_text(encoding="utf-8"))
            for st in _fi_data.get("top_stocks", []):
                if st.get("code"):
                    _flow_intensity_map[st["code"]] = st
        if _flow_intensity_map:
            logger.info(f"[step5] 수급강도 맵: {len(_flow_intensity_map)}종목")
    except Exception as _fi_e:
        logger.warning(f"[step5] 수급강도 맵 로드 실패(무시): {_fi_e}")

    # ── 외인 던지기 맵 (FOREIGN_DUMP / FLIP / OVERHEAT) ──
    _foreign_dump_map = {}  # {code: {tag, penalty, detail, ...}}
    try:
        from tools.flow_intelligence import detect_foreign_dump as _detect_fd
        _foreign_dump_map = _detect_fd()
        if _foreign_dump_map:
            logger.info(f"[step5] 외인던지기 맵: {len(_foreign_dump_map)}종목")
    except Exception as _fd_e:
        logger.warning(f"[step5] 외인던지기 맵 로드 실패(무시): {_fd_e}")

    # ── 장전 리스크 블랙리스트 맵 (DART 공시 + 뉴스 센티먼트 + 속보) ──
    _premarket_risk_map = {}
    try:
        from tools.premarket_risk_scanner import scan_premarket_risk
        _premarket_risk_map = scan_premarket_risk()
        if _premarket_risk_map:
            logger.info(f"[step5] 장전리스크: {len(_premarket_risk_map)}종목")
    except Exception as _pm_e:
        logger.warning(f"[step5] 장전리스크 로드 실패(무시): {_pm_e}")

    # ── 투자자 수급 인텔리전스 맵 (외인/기관 TOP 매수/매도) ──
    _investor_flow_map = {}  # {code: FlowScore}
    _investor_flow_mode = "NEUTRAL"
    try:
        from tools.investor_flow_intel import load_investor_flow_intel
        _investor_flow_map, _inv_mkt = load_investor_flow_intel()
        _investor_flow_mode = _inv_mkt.mode
        if _investor_flow_map:
            logger.info(
                f"[step5] 수급인텔: {len(_investor_flow_map)}종목 "
                f"mode={_investor_flow_mode} 외인={_inv_mkt.foreign_total:+,.0f}억"
            )
    except Exception as _if_e:
        logger.warning(f"[step5] 수급인텔 로드 실패(무시): {_if_e}")

    # ── 스텔스 수급 탐지 맵 (매집+다이버전스+신용바닥) ──
    _stealth_flow_map = {}  # {code: StealthSignal}
    try:
        from tools.stealth_flow_detector import scan_stealth_flow
        _stealth_flow_map = scan_stealth_flow()
        if _stealth_flow_map:
            logger.info(f"[step5] 스텔스수급: {len(_stealth_flow_map)}시그널")
    except Exception as _sf_e:
        logger.warning(f"[step5] 스텔스수급 로드 실패(무시): {_sf_e}")

    # ── ETF 자금흐름 부스터 맵 (섹터 유입/유출 → 종목 가감점) ──
    _etf_flow_map = {}  # {code: score}
    _etf_sector_map = {}  # {sector: SectorFlow}
    try:
        from tools.etf_flow_booster import load_etf_flow_booster
        _etf_flow_map, _etf_sector_map = load_etf_flow_booster()
        if _etf_flow_map:
            logger.info(f"[step5] ETF흐름: {len(_etf_flow_map)}종목 {len(_etf_sector_map)}섹터")
    except Exception as _ef_e:
        logger.warning(f"[step5] ETF흐름 로드 실패(무시): {_ef_e}")

    # ── 매크로 리스크 레이더 맵 (시나리오+센티먼트 → 섹터 가감점) ──
    _macro_stock_map = {}  # {code: score}
    _macro_mode = "NEUTRAL"
    try:
        from tools.macro_radar import scan_macro_radar, get_macro_score
        _macro_state = scan_macro_radar()
        _macro_mode = _macro_state.mode
        # 종목별 스코어는 get_macro_score()로 호출
        logger.info(
            f"[step5] 매크로: mode={_macro_mode} "
            f"시나리오={len(_macro_state.active_scenarios)} "
            f"risk={_macro_state.risk_score:+.0f}"
        )
    except Exception as _mr_e:
        logger.warning(f"[step5] 매크로레이더 로드 실패(무시): {_mr_e}")

    # ── 연기금 매수등급 맵 (pension_scan.json → S/A/B/C) ──
    _pension_grade_map = {}  # {code: "S"/"A"/"B"/"C"}
    try:
        import json as _json_pg
        _pg_path = Path(__file__).resolve().parent.parent / "data_store" / "pension_scan.json"
        if _pg_path.exists():
            _pg_data = _json_pg.loads(_pg_path.read_text(encoding="utf-8"))
            for _pg_list in (_pg_data.get("best_stocks", []),
                             _pg_data.get("standby_stocks", [])):
                for st in _pg_list:
                    if st.get("code"):
                        _pension_grade_map[st["code"]] = st.get("pension_grade", "C")
            if _pension_grade_map:
                _pg_s = sum(1 for v in _pension_grade_map.values() if v == "S")
                _pg_a = sum(1 for v in _pension_grade_map.values() if v == "A")
                logger.info(f"[step5] 연기금등급 맵: {len(_pension_grade_map)}종목 (S={_pg_s} A={_pg_a})")
    except Exception as _pg_e:
        logger.warning(f"[step5] 연기금등급 맵 로드 실패(무시): {_pg_e}")

    # ── EWY 비중 변동 맵 (ewy_holdings.json → 비중 변화) ──
    _ewy_change_map = {}  # {code: weight_change}
    _ewy_new_set = set()  # 신규 편입 코드
    _ewy_removed_set = set()  # 편출 코드
    try:
        import json as _json_ew
        _ew_path = Path(__file__).resolve().parent.parent / "data_store" / "ewy_holdings.json"
        if _ew_path.exists():
            _ew_data = _json_ew.loads(_ew_path.read_text(encoding="utf-8"))
            for ch in _ew_data.get("changes", []):
                if ch.get("code"):
                    _ewy_change_map[ch["code"]] = ch.get("weight_change", 0)
            for ne in _ew_data.get("new_entries", []):
                if ne.get("code"):
                    _ewy_new_set.add(ne["code"])
            for rm in _ew_data.get("removed", []):
                if rm.get("code"):
                    _ewy_removed_set.add(rm["code"])
            if _ewy_change_map or _ewy_new_set:
                logger.info(
                    f"[step5] EWY맵: 변동={len(_ewy_change_map)}종목 "
                    f"편입={len(_ewy_new_set)} 편출={len(_ewy_removed_set)}"
                )
    except Exception as _ew_e:
        logger.warning(f"[step5] EWY맵 로드 실패(무시): {_ew_e}")

    # ── ETF 주도주 보너스 맵 (Supabase scalper_etf_leader_picks → TOP 30) ──
    # 차트영웅 영상 로직 기반 ETF Step A→B→C 결과 활용 (5/17 도입)
    _etf_leader_map = {}  # {code: {rank, bonus, tag, cross_inc, ...}}
    try:
        from utils.etf_leader_bonus import load_etf_leader_map as _load_etf_leader
        _etf_leader_map = _load_etf_leader()
        if _etf_leader_map:
            _top10_cnt = sum(1 for v in _etf_leader_map.values() if v["rank"] <= 10)
            logger.info(f"[step5] ETF주도주 맵: {len(_etf_leader_map)}종목 (TOP10={_top10_cnt})")
    except Exception as _el_e:
        logger.warning(f"[step5] ETF주도주 맵 로드 실패(무시): {_el_e}")

    # 모든 종목 코드 수집
    all_codes = set()
    all_codes.update(relay.get("stocks", {}).keys())
    all_codes.update(premove.get("stocks", {}).keys())
    all_codes.update(macd_result.keys())
    # 로테이션 다음 섹터 종목도 후보에 포함 (reversal_exit 제외)
    for code, rot_info in rotation_stocks.items():
        if rot_info.get("rotation_source") != "reversal_exit":
            all_codes.add(code)
    all_codes.update(bargain_result.keys())
    # TV 스캐너 종목도 교차검증 대상에 포함
    if tv_signals:
        all_codes.update(tv_signals.keys())
    # v4.3: bomb 종목은 7SECRET 이전에는 추가하지 않음 (KRX API 호출 방지)
    # → 7SECRET 완료 후 bomb-only 종목을 합류시킴 (경량 패스)
    _bomb_only_codes = set(_bomb_map.keys()) - all_codes  # 기존 후보에 없는 bomb 종목
    _bomb_overlap = len(all_codes & set(_bomb_map.keys()))
    logger.info(f"[step5] bomb: 전체={len(_bomb_map)} / 기존교집합={_bomb_overlap} / bomb전용={len(_bomb_only_codes)}")

    # ── 7 SECRET 국적 파워 (NORMAL 모드) ──
    normal_nat_powers = {}
    norm_price_data = {}
    norm_daily = {}  # FLOWX 업로드용 국적 원시 데이터
    try:
        from data.nationality_profiler import calc_nationality_power, collect_daily_series
        from pathlib import Path as _Path2
        _data_store2 = _Path2(__file__).resolve().parent.parent / "data_store"
        norm_codes = list(all_codes)
        if norm_codes:
            # 5일 수익률 계산
            for c in norm_codes:
                csv_p = _data_store2 / "daily" / f"{c}.csv"
                chg_5d = 0.0
                if csv_p.exists():
                    try:
                        import pandas as _pd2
                        df2 = _pd2.read_csv(csv_p, encoding="utf-8")
                        if len(df2) >= 5:
                            c5 = df2["종가"].iloc[-5]
                            c0 = df2["종가"].iloc[-1]
                            if c5 > 0:
                                chg_5d = (c0 / c5 - 1) * 100
                    except Exception:
                        pass
                norm_price_data[c] = {"chg_5d": chg_5d}
            norm_daily = collect_daily_series(norm_codes, n_days=5)
            normal_nat_powers = calc_nationality_power(
                norm_codes, daily_data=norm_daily, price_data=norm_price_data,
                all_codes_data=norm_daily, n_days=5,
            )
            np_cnt = sum(1 for v in normal_nat_powers.values() if v.score != 0)
            logger.info(f"NORMAL 7SECRET 파워: {np_cnt}/{len(norm_codes)}종목")
    except Exception as e:
        logger.warning(f"NORMAL 7SECRET 파워 실패 (무시): {e}")

    # ── v4.3: bomb-only 종목 합류 (7SECRET 완료 후 → 경량 패스) ──
    # 7SECRET은 기존 후보만 처리됨 → bomb-only 종목은 nat_power_sc=0 (스킵)
    # 대신 bomb_adj(+15/+12) + flow + fib 등으로 점수 확보
    if _bomb_only_codes:
        all_codes.update(_bomb_only_codes)
        logger.info(f"[v4.3] bomb-only {len(_bomb_only_codes)}종목 all_codes 합류 (7SECRET 스킵, 경량 패스)")

    # ── 대량쌍매수/연속급등 종목 합류 (7SECRET 완료 후 → 경량 패스) ──
    _dual_only = set(_dual_buy_map.keys()) - all_codes
    if _dual_only:
        all_codes.update(_dual_only)
        logger.info(f"[step5] dual_buy-only {len(_dual_only)}종목 합류 (경량 패스)")
    _surge_only = set(_surge_map.keys()) - all_codes
    if _surge_only:
        all_codes.update(_surge_only)
        logger.info(f"[step5] surge-only {len(_surge_only)}종목 합류 (경량 패스)")
    _fi_only = set(_flow_intensity_map.keys()) - all_codes
    if _fi_only:
        all_codes.update(_fi_only)
        logger.info(f"[step5] flow_intensity-only {len(_fi_only)}종목 합류 (경량 패스)")

    _fib_cache = {}  # 피보나치 분석 캐시 (코드별 1회만)

    # ── 쌍매수 연속 + 대형주 수급 캐시 (루프 밖에서 1회 로드) ──
    _flow_cache = {}  # {code: {"consec": int, "today_frgn": float, "today_inst": float}}
    _uni_caps = {}    # {code: cap_억}
    try:
        import csv as _csv_mod
        _flow_dir = Path(__file__).resolve().parent.parent / "data_store" / "flow"
        # universe에서 시총 정보 가져오기
        import json as _json_mod
        _uni_path = Path(__file__).resolve().parent.parent / "data_store" / "universe.json"
        _uni_caps = {}
        if _uni_path.exists():
            with open(_uni_path, "r", encoding="utf-8") as _uf:
                _uni_data = _json_mod.load(_uf)
                _uni_caps = {k: v.get("cap_억", 0) for k, v in _uni_data.items()} if isinstance(_uni_data, dict) else {}

        for _fc_code in all_codes:
            _fp = _flow_dir / f"{_fc_code}_investor.csv"
            if not _fp.exists():
                continue
            try:
                with open(_fp, "r", encoding="utf-8-sig") as _ff:
                    reader = _csv_mod.DictReader(_ff)
                    rows = list(reader)
                if len(rows) < 2:
                    continue
                # 최근 5일 역순 탐색 — 연속 쌍매수 일수 카운트
                consec = 0
                today_frgn = 0.0
                today_inst = 0.0
                for idx, row in enumerate(reversed(rows[-5:])):
                    fv = float(row.get("외국인_금액", 0) or 0)
                    iv = float(row.get("기관_금액", 0) or 0)
                    if idx == 0:
                        today_frgn = fv  # 백만원 단위
                        today_inst = iv
                    if fv > 0 and iv > 0:
                        consec += 1
                    else:
                        break  # 연속 끊김
                _flow_cache[_fc_code] = {
                    "consec": consec,
                    "today_frgn": today_frgn,  # 백만원
                    "today_inst": today_inst,   # 백만원
                }
            except Exception:
                pass
        _fc_cnt = sum(1 for v in _flow_cache.values() if v["consec"] >= 2)
        logger.info(f"[수급캐시] {len(_flow_cache)}종목 로드, 쌍매수2일+: {_fc_cnt}종목")
    except Exception as _e:
        logger.warning(f"[수급캐시] 로드 실패 (무시): {_e}")

    # ── 선매집 캐시 (stealth_scan.json → 잠복 종목 보너스) ──
    _stealth_cache = {}  # {code: score}
    try:
        _stealth_path = Path(__file__).resolve().parent.parent / "data_store" / "stealth_scan.json"
        if _stealth_path.exists():
            import json as _json_mod2
            _sd = _json_mod2.loads(_stealth_path.read_text("utf-8"))
            for _s in _sd.get("stealth", []):
                _stealth_cache[_s["code"]] = _s.get("score", 0)
            for _s in _sd.get("moving", []):
                _stealth_cache[_s["code"]] = _s.get("score", 0)
            logger.info(f"[선매집캐시] {len(_stealth_cache)}종목 로드 (잠복+움직임)")
    except Exception as _e2:
        logger.warning(f"[선매집캐시] 로드 실패 (무시): {_e2}")

    # ── US 야간 필터 캐시 (us_overnight_result.json → 진입 모드) ──
    _us_mode = "NORMAL"
    _us_relay_codes = set()
    try:
        _us_path = Path(__file__).resolve().parent.parent / "data_store" / "us_overnight_result.json"
        if _us_path.exists():
            import json as _json_mod3
            _us = _json_mod3.loads(_us_path.read_text("utf-8"))
            _us_mode = _us.get("mode", "NORMAL")
            # 릴레이 종목 코드 추출
            for _rp in _us.get("relay_picks", []):
                _us_relay_codes.add(_rp.get("kr_code", ""))
            logger.info(f"[US모드] {_us_mode} | 릴레이 {len(_us_relay_codes)}종목")
    except Exception as _e3:
        logger.warning(f"[US모드] 로드 실패 (무시): {_e3}")

    candidates = []
    for code in all_codes:
        r_info = relay.get("stocks", {}).get(code, {})
        p_info = premove.get("stocks", {}).get(code, {})
        t_info = tech.get(code, {})
        n_info = news.get(code, {})
        m_info = macd_result.get(code, {})
        b_info = bargain_result.get(code, {})

        # TV-only 종목도 이름 해석 (tv_signals에서 조회)
        _tv_name = ""
        if tv_signals and code in tv_signals:
            _tv_obj = tv_signals[code]
            _tv_name = getattr(_tv_obj, "name", "") or (
                _tv_obj.get("name", "") if isinstance(_tv_obj, dict) else "")
        # v4.3: bomb-only 종목 이름 해석 (bomb_map에서 조회)
        _bomb_name = _bomb_map[code].get("name", "") if code in _bomb_map else ""
        name = (r_info.get("name") or p_info.get("name")
                or m_info.get("name") or b_info.get("name")
                or _tv_name or _bomb_name or code)
        # TV-only 종목도 close 확보 (SL=0, TP=0 방지)
        _tv_close = 0
        if tv_signals and code in tv_signals:
            _tv_c = tv_signals[code]
            _tv_close = (_tv_c.get("close", 0) if isinstance(_tv_c, dict)
                         else getattr(_tv_c, "close", 0))
        close = (t_info.get("close")
                 or r_info.get("close")
                 or p_info.get("close")
                 or b_info.get("close", 0)
                 or _tv_close)
        # v4.3: bomb-only 종목 종가 fallback (daily CSV)
        if not close and code in _bomb_map:
            _bomb_csv = Path(__file__).resolve().parent.parent / "data_store" / "daily" / f"{code}.csv"
            if _bomb_csv.exists():
                try:
                    import pandas as _pd_bomb
                    _df_bomb = _pd_bomb.read_csv(_bomb_csv, encoding="utf-8")
                    if len(_df_bomb) > 0:
                        close = int(_df_bomb["종가"].iloc[-1])
                except Exception:
                    pass

        # ── 양의 점수 (가산) ──────────────────
        # 교차 등장 횟수
        cross = 0
        sources = []
        if code in relay.get("stocks", {}):
            cross += 1
            sources.extend(r_info.get("sources", ["relay"]))
        if code in premove.get("stocks", {}):
            cross += 1
            sources.append("premove")
        if code in macd_result:
            cross += 1
            sources.append(f"macd_zero({m_info.get('source', '')})")
        if code in bargain_result:
            cross += 1
            grade = b_info.get("supply_grade", "")
            sources.append(f"bargain({grade})")

        # TV 스캐너 소스 (거래대금 폭발) — score 70+ = 2소스 취급
        if tv_signals and code in tv_signals:
            _tv_sig = tv_signals[code]
            _tv_sc_cross = _tv_sig.get("score", 0) if isinstance(_tv_sig, dict) else getattr(_tv_sig, "score", 0)
            if _tv_sc_cross >= 70:
                cross += 2  # 강신호 = 2소스 취급
            else:
                cross += 1
            _tv_pat = _tv_sig.get("pattern", "") if isinstance(_tv_sig, dict) else getattr(_tv_sig, "pattern", "")
            sources.append(f"tv:{_tv_pat}({_tv_sc_cross:.0f})")

        # 로테이션 소스 (다음 섹터 종목)
        rot_info = rotation_stocks.get(code, {})
        if rot_info and rot_info.get("rotation_source") != "reversal_exit":
            cross += 1
            rot_src = rot_info["rotation_source"]
            rot_sector = rot_info.get("sector", "")
            sources.append(f"rotation:{rot_src}({rot_sector})")

        relay_sc = min(r_info.get("signal_count", 0) * 15, 45)   # 0~45
        premove_sc = min(p_info.get("premove_score", 0), 100) * 0.3  # 0~30
        tech_sc = t_info.get("score", 0) * 5                      # 0~25
        cross_bonus = cross * 10 if cross >= 2 else 0             # 0~30

        # 국적별 수급 (-30 ~ +50)
        nat_info = nationality.get(code, (0, ""))
        nat_sc = nat_info[0] if isinstance(nat_info, tuple) else 0
        nat_detail = nat_info[1] if isinstance(nat_info, tuple) else ""

        # ── 페널티 (감산, hard cutoff 대신) ──
        # 뉴스 NEGATIVE → -20 (기존: 즉시 탈락)
        news_pen = 0.0
        sentiment = n_info.get("sentiment", "NEUTRAL")
        if sentiment == "NEGATIVE":
            news_pen = -20.0
            logger.debug(f"뉴스 NEGATIVE 페널티: {name}({code}) -20")

        # OBV DOWN → -10 (기존: 즉시 탈락)
        obv_pen = 0.0
        if t_info.get("obv_dir") == "DOWN":
            obv_pen = -10.0

        # 시장대비 상대약세 → 단계적 페널티 (기존: 절대 -5% 탈락)
        rel_pen = 0.0
        relative_str = t_info.get("relative_str", 0)
        today_chg = t_info.get("today_chg", 0)
        if relative_str < -10.0:
            rel_pen = -25.0   # 시장보다 10%p 이상 약세 = 큰 문제
        elif relative_str < -5.0:
            rel_pen = -15.0   # 시장보다 5%p 이상 약세
        elif relative_str < -2.0:
            rel_pen = -5.0    # 시장보다 약간 약세

        # ── 위꼬리 캔들 페널티 (신정재 필터) ──
        wick_pen = 0.0
        _wick_pct = t_info.get("upper_wick_pct", 0)
        if _wick_pct > 30:
            wick_pen = -15.0
        elif _wick_pct > 20:
            wick_pen = -8.0
        if wick_pen != 0:
            sources.append(f"wick({_wick_pct:.0f}%:{wick_pen:+.0f})")

        # 줍줍 점수 (0~30) - bargain_score를 0.3배로 변환
        bargain_sc = min(b_info.get("bargain_score", 0) * 0.3, 30) if b_info else 0

        # ── CORTEX: 충격 섹터 보정 ──────────────
        from data.market_health import get_stock_sector
        sector = get_stock_sector(code)
        shock_pen = 0.0
        opp_bonus = 0.0
        if sector and affected_sectors and sector in affected_sectors:
            shock_pen = -15.0  # 충격 수혜 섹터 (이미 올랐으니 매수 제외)
        if sector and opportunity_sectors and sector in opportunity_sectors:
            opp_bonus = 5.0    # 기회 섹터 (충격 무관 과도 하락)
        if shock_pen != 0:
            sources.append(f"shock:{sector}({shock_pen:+.0f})")
        if opp_bonus != 0:
            sources.append(f"opp:{sector}({opp_bonus:+.0f})")

        # ── 로테이션: 다음 섹터 보너스 / 반전 페널티 ──
        # 4/26 학습: rotation 24건 D+1 +1.38% 적중67% → 수익성 높은 팩터, 가중치 상향
        # hot_early가 핵심 (초기 진입이 수익의 원천)
        rotation_bonus = 0.0
        if rot_info:
            rot_src = rot_info.get("rotation_source", "")
            if rot_src == "hot_early":
                rotation_bonus = 12.0   # HOT 초기 (8→12, 4/26 학습)
            elif rot_src == "staging":
                rotation_bonus = 8.0    # 스테이징 (5→8)
            elif rot_src == "hot_mid":
                rotation_bonus = 0.0    # HOT 중기 → 이미 늦음 (유지)
            elif rot_src == "reversal_exit":
                rotation_bonus = -20.0  # 반전 유지

        # ── OR bias 보정 (ICT Opening Range) ──
        or_bias_adj = 0.0
        _or_bias_label = ""
        try:
            from strategies.opening_range import get_bias_adjustment
            or_bias_adj = get_bias_adjustment(code)
            if or_bias_adj > 0:
                _or_bias_label = "bullish"
            elif or_bias_adj < 0:
                _or_bias_label = "bearish"
        except Exception:
            pass
        if or_bias_adj != 0:
            sources.append(f"or_bias:{_or_bias_label}({or_bias_adj:+.0f})")

        # ── Equal Level 보정 (ICT EQ High/Low) ──
        eq_adj = 0.0
        _eq_reason = ""
        try:
            from strategies.equal_level_detector import get_eq_score_adjustment
            eq_adj, _eq_reason = get_eq_score_adjustment(code, close)
        except Exception:
            pass
        if eq_adj != 0:
            sources.append(f"eq_level:{_eq_reason}({eq_adj:+.0f})")

        # ── 갭 지지/저항 보정 ──
        gap_adj = 0.0
        _gap_reason = ""
        try:
            from strategies.gap_support import get_gap_score_adjustment
            gap_adj, _gap_reason = get_gap_score_adjustment(code, close)
        except Exception:
            pass
        if gap_adj != 0:
            sources.append(f"gap:{_gap_reason}({gap_adj:+.0f})")


        # 7 SECRET 국적 파워
        norm_np = normal_nat_powers.get(code)
        nat_power_sc = norm_np.score if norm_np else 0.0
        if nat_power_sc != 0:
            sources.append(f"7SECRET({norm_np.grade}:{nat_power_sc:+.1f})")

        # ── TV Direct Score (거래대금 강신호 직접 점수) ──
        tv_direct = 0.0
        _tv_ratio = 1.0
        _tv_pattern = "NORMAL"
        _tv_score = 0.0
        if tv_signals and code in tv_signals:
            _tv = tv_signals[code]
            # TVSignal 객체 또는 dict 모두 대응
            _tv_pat = _tv.get("pattern") if isinstance(_tv, dict) else getattr(_tv, "pattern", "NORMAL")
            _tv_sc = _tv.get("score", 0) if isinstance(_tv, dict) else getattr(_tv, "score", 0)
            _tv_r = _tv.get("tv_ratio", 1.0) if isinstance(_tv, dict) else getattr(_tv, "tv_ratio", 1.0)
            _tv_ratio = _tv_r
            _tv_pattern = _tv_pat
            _tv_score = _tv_sc

            # TV 기본 점수 (패턴별 차등)
            # 학습 피드백 (5/9 누적): QA(90)=9%, QA(82)=0%, QA(80)=0%
            # QA 80~90 전 구간 적중률 0~9% → 하드 차단
            # QA 70~79만 초기 매집으로 제한적 활용
            if _tv_pat == "QUIET_ACCUMULATION":
                if _tv_sc >= 80:
                    tv_direct = 0      # 80+ 전면 차단 (적중 0~9%)
                elif _tv_sc >= 70:
                    tv_direct = 8      # 초기 매집만 제한적 (20→8)
                elif _tv_sc >= 60:
                    tv_direct = 12     # 약한 매집 (15→12)
            elif _tv_pat == "EXPLOSION":
                if _tv_sc >= 80:
                    tv_direct = 8      # 이미 폭발 후 → 함정 (30→8)
                elif _tv_sc >= 70:
                    tv_direct = 5      # 함정 위험 (20→5)
                elif _tv_sc >= 60:
                    tv_direct = 3      # 약한 폭발 (12→3)
            elif _tv_pat == "EARLY_ACCUMULATION":
                # 선제 감지 — 아직 확정 아니지만 1일 앞서 포착
                # 백테스트: PANIC(3/25→3/27) 승률 0% → PANIC/SHOCK에서는 무력화
                _cortex = regime_info.get("regime", "NORMAL") if regime_info else "NORMAL"
                if _cortex in ("PANIC", "SHOCK"):
                    tv_direct = 0  # 하락장에서 EA는 노이즈
                elif _tv_sc >= 60:
                    tv_direct = 10     # 정상시장에서만 활용 (12→10 보수적)
                elif _tv_sc >= 45:
                    tv_direct = 6      # (8→6 보수적)
                sources.append(f"tv:EARLY_ACCUMULATION({_tv_sc:.0f})")
            elif _tv_pat == "GRADUAL_BUILDUP":
                if _tv_sc >= 70:
                    tv_direct = 15
                elif _tv_sc >= 60:
                    tv_direct = 10

            # 그룹/섹터 클러스터 보너스
            _cluster_map = tv_cluster_map or {}
            cluster_bonus = _cluster_map.get(code, 0)
            if cluster_bonus > 0:
                tv_direct += cluster_bonus
                sources.append(f"tv_cluster(+{cluster_bonus})")

            # 외인합류 보너스 (백테스트: 외인합류 D+1 +0.97% vs 미합류 -0.61%)
            _frgn_joined = (_tv.get("frgn_joined", False) if isinstance(_tv, dict)
                            else getattr(_tv, "frgn_joined", False))
            _frgn_amt_tv = (_tv.get("frgn_amount", 0) if isinstance(_tv, dict)
                            else getattr(_tv, "frgn_amount", 0))
            if _frgn_joined and _tv_pat == "QUIET_ACCUMULATION":
                tv_direct += 8  # 외인합류 매집 보너스
                sources.append(f"tv_frgn_joined(+8,{_frgn_amt_tv:+.0f}억)")

        # ── TV 잔존 효과 (오늘 TV 미감지지만 과거 강신호 있었던 종목) ──
        if tv_direct == 0 and tv_persistence and code in tv_persistence:
            pinfo = tv_persistence[code]
            p_score = pinfo["persistence_score"]
            p_pattern = pinfo["peak_pattern"]
            p_days = pinfo["days_ago"]
            p_peak = pinfo["peak_score"]
            tv_direct = p_score
            _tv_pattern = p_pattern
            _tv_score = p_peak
            sources.append(f"tv_persist(T-{p_days}:{p_pattern}:{p_peak:.0f})")
            # 연속 모멘텀: T-1 EXPLOSION + 등락률 15%+ → 추가 +10
            if (p_days == 1 and p_pattern == "EXPLOSION"
                    and abs(pinfo.get("change_pct", 0)) >= 15.0):
                tv_direct += 10
                sources.append("tv_momentum(+10)")

        # ── 공매도 잔고 스코어 ──────────────
        short_sc = 0.0
        try:
            from data.short_analyzer import get_short_score, is_short_building_danger
            if is_short_building_danger(code):
                # 숏빌딩 + HIGH 잔고 → Hard Filter (추천 제외)
                logger.info(f"  [SHORT] {name}: 숏빌딩+HIGH → 제외")
                continue
            # ── 장전 리스크 Hard Exclude (상폐 등) ──
            if code in _premarket_risk_map and _premarket_risk_map[code].hard_exclude:
                logger.info(f"  [장전리스크] {name}: {_premarket_risk_map[code].tag} → 제외")
                continue
            short_sc, short_detail = get_short_score(code, name)
            if abs(short_sc) >= 1.0:
                sources.append(f"short:{short_detail}")
        except Exception:
            pass

        # ── TRIX 다이버전스 스코어 ──────────────
        trix_sc = 0.0
        tx = trix_result.get(code, {})
        if tx and tx.get("div_strength", 0) > 0:
            cross += 1  # TRIX = 1 소스
            base = 12
            if tx.get("adx", 0) >= 20:
                base += 6
            if tx.get("trix_cross") == "bullish_cross":
                base += 7
                if tx.get("div_strength", 0) >= 0.7:
                    cross += 1  # 강한 다이버전스 = 추가 소스
            trix_sc = min(base * tx["div_strength"], 25)
            sources.append(f"trix({tx['div_strength']:.2f})")

        # ── 쌍매수 연속 보너스 (학습: 쌍매수 2일+연속 = 가장 확실한 급등 신호) ──
        doublebuy_sc = 0.0
        _fc = _flow_cache.get(code)
        if _fc:
            _consec = _fc["consec"]
            if _consec >= 3:
                doublebuy_sc = 18.0   # 3일+ 연속 쌍매수 → 강력
                cross += 1
                sources.append(f"doublebuy({_consec}D:+18)")
            elif _consec >= 2:
                doublebuy_sc = 12.0   # 2일 연속 쌍매수
                cross += 1
                sources.append(f"doublebuy({_consec}D:+12)")
            elif _consec == 1:
                # 오늘만 쌍매수 — 금액 규모에 따라 차등
                _today_sum = abs(_fc["today_frgn"]) + abs(_fc["today_inst"])
                if _today_sum >= 50000:  # 500억+ (백만원 단위)
                    doublebuy_sc = 8.0
                    sources.append("doublebuy(1D:+8)")
                elif _today_sum >= 10000:  # 100억+
                    doublebuy_sc = 5.0
                    sources.append("doublebuy(1D:+5)")

        # ── 대형주 수급 감지 (학습: 시총1조+ TV미감지 수급 급등 놓침) ──
        # TV 스캐너에 안 잡히는 대형주의 외인/기관 대량매수 감지
        largecap_sc = 0.0
        _cap = _uni_caps.get(code, 0)
        if _cap >= 10000 and _fc and tv_direct == 0:  # 시총 1조+ & TV 미감지
            _frgn_b = abs(_fc["today_frgn"]) / 100  # 억원 변환
            _inst_b = abs(_fc["today_inst"]) / 100
            _is_frgn_buy = _fc["today_frgn"] > 0
            _is_inst_buy = _fc["today_inst"] > 0
            _max_flow = max(_frgn_b if _is_frgn_buy else 0,
                           _inst_b if _is_inst_buy else 0)
            # 매수 금액 규모별 점수
            # 4/26 학습: largecap_flow 16건 D+1 +1.61% 적중69% → 최고 수익 팩터, 가중치 상향
            if _max_flow >= 1000:      # 1000억+ 대량매수
                largecap_sc = 26.0     # (20→26)
            elif _max_flow >= 500:     # 500억+
                largecap_sc = 20.0     # (15→20)
            elif _max_flow >= 100:     # 100억+
                largecap_sc = 12.0     # (8→12)
            elif _max_flow >= 50:      # 50억+
                largecap_sc = 6.0      # (4→6)
            # 쌍매수 배수
            if largecap_sc > 0 and _is_frgn_buy and _is_inst_buy:
                largecap_sc *= 1.3
                largecap_sc = round(largecap_sc, 1)
            if largecap_sc > 0:
                _flow_label = f"F{_frgn_b:+.0f}I{_inst_b:+.0f}"
                sources.append(f"largecap_flow({_flow_label}:+{largecap_sc:.0f})")
                cross += 1

        # ── 선매집 잠복 보너스 (학습: 기관 연속매수 + 가격횡보 = 스프링 장전) ──
        stealth_sc = 0.0
        _stl = _stealth_cache.get(code, 0)
        if _stl >= 100:
            stealth_sc = 15.0   # 100점+ 잠복종목 → 강력 보너스
            sources.append(f"stealth({_stl}:+15)")
            cross += 1
        elif _stl >= 70:
            stealth_sc = 10.0   # 70점+ → 보너스
            sources.append(f"stealth({_stl}:+10)")
        elif _stl >= 50:
            stealth_sc = 5.0    # 50점+ → 약한 보너스
            sources.append(f"stealth({_stl}:+5)")

        # ── US→KR 릴레이 보너스 (미국장 강세 종목의 한국 수혜주) ──
        us_relay_sc = 0.0
        if code in _us_relay_codes:
            us_relay_sc = 10.0
            sources.append("us_relay(+10)")
            cross += 1

        # ── v4.1: 수급폭탄 보너스 (전종목 — B안) ──
        bomb_sc = 0.0
        if code in _bomb_map:
            _bw = _bomb_map[code]
            bomb_sc = float(_bw.get("bomb_adj", 0))  # +15(SECOND_BOMB) or +12(EARLY 2nd)
            sources.append(f"bomb({_bw['signal']}:+{bomb_sc:.0f})")
            cross += 1

        # ── 대량 쌍매수 보너스 (+12) ──
        dual_buy_sc = 0.0
        if code in _dual_buy_map:
            _db = _dual_buy_map[code]
            dual_buy_sc = 12.0
            sources.append(f"dual_buy({_db['total_억']:.0f}억:+12)")
            cross += 1

        # ── 연속급등 보너스 (+10) ──
        surge_sc = 0.0
        if code in _surge_map:
            _sg = _surge_map[code]
            surge_sc = 10.0
            sources.append(f"surge({_sg['change_pct']:+.0f}%:+10)")
            cross += 1

        # ── 수급 강도 보너스 (flow_intensity TOP7 → +10~+15) ──
        fi_sc = 0.0
        if code in _flow_intensity_map:
            _fi = _flow_intensity_map[code]
            _fi_qs = _fi.get("quality_score", 0)
            # 쌍매수이면 +15, 아니면 +10
            fi_sc = 15.0 if _fi.get("dual_buy") else 10.0
            sources.append(f"fi(q{_fi_qs:.0f}:+{fi_sc:.0f})")
            cross += 1

        # ── 외인 던지기 감점 (FOREIGN_DUMP -20 / FLIP -15 / OVERHEAT -25) ──
        fdump_pen = 0.0
        if code in _foreign_dump_map:
            _fd = _foreign_dump_map[code]
            fdump_pen = _fd["penalty"]
            sources.append(f"{_fd['tag']}({fdump_pen:+.0f})")
            if _fd["tag"] == "THEME_OVERHEAT":
                cross = 0  # 과열 종목은 교차검증 무효화

        # ── 장전 리스크 감점 (DART공시/뉴스/속보) ──
        premarket_pen = 0.0
        if code in _premarket_risk_map:
            _pm = _premarket_risk_map[code]
            if not _pm.hard_exclude:
                premarket_pen = _pm.penalty
                sources.append(f"{_pm.tag}({premarket_pen:+.0f})")

        # ── 투자자 수급 인텔리전스 (외인/기관 TOP 매수/매도) ──
        invflow_sc = 0.0
        if code in _investor_flow_map:
            _ifl = _investor_flow_map[code]
            invflow_sc = _ifl.score
            if abs(invflow_sc) >= 1.0:
                sources.append(f"{_ifl.tag}({invflow_sc:+.0f})")
                if invflow_sc > 0:
                    cross += 1  # 외인/기관 매수 = 교차검증 1소스

        # ── 스텔스 수급 보너스 (매집+다이버전스+신용바닥) ──
        stflow_sc = 0.0
        if code in _stealth_flow_map:
            _stl = _stealth_flow_map[code]
            stflow_sc = _stl.score
            sources.append(f"{_stl.tag}(+{stflow_sc:.0f})")
            cross += 1  # 멀티데이 수급 = 교차검증 1소스

        # ── ETF 자금흐름 부스터 (섹터 유입 → 가점, 유출 → 감점) ──
        etf_flow_sc = 0.0
        if code in _etf_flow_map:
            etf_flow_sc = _etf_flow_map[code]
            if abs(etf_flow_sc) >= 3.0:
                sources.append(f"ETF({etf_flow_sc:+.0f})")
                if etf_flow_sc > 0:
                    cross += 1  # 섹터 자금 유입 = 교차검증 1소스

        # ── 매크로 리스크 레이더 (시나리오 기반 섹터 가감점) ──
        macro_sc = 0.0
        macro_tag = ""
        try:
            macro_sc, macro_tag = get_macro_score(code)
        except Exception:
            pass
        if abs(macro_sc) >= 2.0:
            sources.append(f"{macro_tag}({macro_sc:+.0f})" if macro_tag else f"MACRO({macro_sc:+.0f})")

        # ── 연기금 매수등급 보너스 (백테스트: S급 D+5 +1.09% / A급 +0.42%) ──
        pension_sc = 0.0
        _pg = _pension_grade_map.get(code, "")
        if _pg == "S":
            pension_sc = 15.0   # 삼중합류 (연기금+금투+외인+덜오름)
            sources.append("pension(S:+15)")
            cross += 1
        elif _pg == "A":
            pension_sc = 8.0    # 이중합류 (연기금+금투+덜오름)
            sources.append("pension(A:+8)")
        elif _pg == "B":
            pension_sc = 5.0    # 발화대기 (연기금+외인, 금투미진입)
            sources.append("pension(B:+5)")

        # ── EWY 비중 변동 보너스 (패시브 외인 자금 흐름) ──
        # 5/9 학습: ewy(NEW) 80%적중 +6.79% → 최강 시그널, 가중치 대폭 상향
        ewy_sc = 0.0
        if code in _ewy_new_set:
            ewy_sc = 25.0  # 신규 편입 → 패시브 강제매수 (12→25)
            sources.append("ewy(NEW:+25)")
        elif code in _ewy_removed_set:
            ewy_sc = -15.0  # 편출 → 패시브 강제매도 (-10→-15)
            sources.append("ewy(REMOVED:-15)")
        elif code in _ewy_change_map:
            wc = _ewy_change_map[code]
            if wc >= 0.3:
                ewy_sc = 15.0  # LARGE UP (8→15)
                sources.append(f"ewy(+{wc:.2f}%:+15)")
            elif wc >= 0.1:
                ewy_sc = 8.0   # MEDIUM UP (4→8)
                sources.append(f"ewy(+{wc:.2f}%:+8)")
            elif wc <= -0.3:
                ewy_sc = -8.0  # LARGE DOWN (-5→-8)
                sources.append(f"ewy({wc:.2f}%:-8)")

        # ── ETF 주도주 보너스 (Step C TOP 30 합류) ─────
        # TOP 10: +10 + cross / TOP 20: +6 / TOP 30: +3 / 추가 다중 ETF(3+): +3
        etf_leader_sc = 0.0
        if code in _etf_leader_map:
            _el = _etf_leader_map[code]
            etf_leader_sc = _el["bonus"]
            sources.append(_el["tag"])
            if _el.get("cross_inc"):
                cross += 1  # TOP 10만 교차검증 카운트 (강한 시그널)

        # ── 합산 ──────────────────────────────
        raw_total = (relay_sc + premove_sc + tech_sc + bargain_sc + cross_bonus
                     + nat_sc + news_pen + obv_pen + rel_pen
                     + wick_pen       # 위꼬리 캔들 페널티
                     + shock_pen + opp_bonus + rotation_bonus + or_bias_adj
                     + eq_adj + gap_adj
                     + nat_power_sc   # 7 SECRET 파워
                     + tv_direct      # TV 강신호 직접 점수
                     + short_sc       # 공매도 잔고 점수
                     + trix_sc        # TRIX 다이버전스 점수
                     + doublebuy_sc   # 쌍매수 연속 보너스
                     + largecap_sc    # 대형주 수급 감지
                     + stealth_sc     # 선매집 잠복 보너스
                     + us_relay_sc    # US→KR 릴레이 보너스
                     + bomb_sc        # v4.1: 수급폭탄 보너스 (전종목)
                     + dual_buy_sc    # 대량 쌍매수 보너스
                     + surge_sc       # 연속급등 보너스
                     + fi_sc          # 수급 강도 보너스
                     + fdump_pen      # 외인 던지기 감점
                     + premarket_pen  # 장전 리스크 감점
                     + invflow_sc    # 투자자 수급 인텔리전스
                     + stflow_sc     # 스텔스 수급 (매집+다이버전스+신용바닥)
                     + etf_flow_sc   # ETF 자금흐름 부스터
                     + macro_sc      # 매크로 리스크 레이더
                     + pension_sc     # 연기금 매수등급 보너스
                     + ewy_sc         # EWY 비중 변동 보너스
                     + etf_leader_sc) # ETF 주도주 (Step C TOP 30) 보너스

        # ── US 모드 조정 ──────────────────────
        # DEFENSIVE: 전체 점수 20% 감점 (보수적 진입)
        # HALT: 0점 강제 (진입 금지)
        # AGGRESSIVE: 10% 가점 (적극 진입)
        if _us_mode == "HALT":
            raw_total = 0.0
            sources.append("US_HALT(=0)")
        elif _us_mode == "DEFENSIVE":
            raw_total *= 0.8
            sources.append("US_DEF(x0.8)")
        elif _us_mode == "AGGRESSIVE":
            # 4/26 학습: US_AGG 40건 D+1 avg -0.44% → 가산 축소 (x1.1→x1.05)
            raw_total *= 1.05
            sources.append("US_AGG(x1.05)")

        # ── 매크로 모드 보정 ──────────────────────
        # RISK_OFF: 전체 점수 10% 감점 (글로벌 리스크 반영)
        # RISK_ON: 전체 점수 5% 가점
        if _macro_mode == "RISK_OFF" and raw_total > 0:
            raw_total *= 0.90
            sources.append("MACRO_ROFF(x0.9)")
        elif _macro_mode == "RISK_ON" and raw_total > 0:
            raw_total *= 1.05
            sources.append("MACRO_RON(x1.05)")

        # ── 브레인 학습 가중치 적용 ──────────────
        brain_adj = _apply_brain_adjustment(
            _brain_insights, sources, name, raw_total
        )
        if abs(brain_adj) >= 0.5:
            sources.append(f"brain({brain_adj:+.1f})")
        raw_total += brain_adj

        # ── 피보나치 레벨 점수 ──────────────
        _fib = _fib_cache.get(code)
        if _fib is None:
            try:
                from data.fibonacci_analyzer import fib_score_adjustment
                _fib = fib_score_adjustment(code, name)
                _fib_cache[code] = _fib
            except Exception as e:
                logger.debug(f"  [FIB] {name} 분석 실패: {e}")
                _fib = {"fib_adj": 0}
                _fib_cache[code] = _fib
        fib_adj = _fib.get("fib_adj", 0)
        if abs(fib_adj) >= 1:
            sources.append(f"fib({fib_adj:+.0f})")
        raw_total += fib_adj

        # ── 과신호 소프트캡 (2단계 압축) ──
        # 4/26 학습: 100~119점=D+1 +3.33%(최적) / 120+=+0.09%(과대평가)
        # 80~120: 완만 압축(30%) → 100~119 구간 보호
        # 120+: 강한 압축(8%) → 과대평가 억제
        # 예: 110점 → 80 + 30*0.30 = 89 / 150점 → 80 + 40*0.30 + 30*0.08 = 94.4
        if raw_total > 80:
            old_raw = raw_total
            excess = raw_total - 80
            if excess <= 40:  # 80~120 구간
                compressed = excess * 0.30  # 초과분 70% 압축 (0.12→0.30)
            else:  # 120+ 구간
                compressed = 40 * 0.30 + (excess - 40) * 0.08  # 120 초과는 강압축
            raw_total = 80 + compressed
            sources.append(f"softcap(-{old_raw - raw_total:.0f})")
            logger.info(f"  [소프트캡] {name}: {old_raw:.0f}→{raw_total:.0f}")

        # ── 수급 미검증 페널티 (학습: 수급 없는 TV신호 적중률 낮음) ──
        # TV 점수가 높은데 국적/수급 시그널이 없으면 감점
        has_flow = abs(nat_sc) >= 3 or abs(nat_power_sc) >= 5
        if tv_direct >= 15 and not has_flow:
            no_flow_pen = tv_direct * 0.3  # TV 점수의 30% 감점
            raw_total -= no_flow_pen
            sources.append(f"no_flow(-{no_flow_pen:.0f})")

        # CORTEX 체제 배수 적용
        total = raw_total * regime_mult

        # ── TV 시그널 보정 (5/9 누적 학습 반영) ──────────────
        # QA 80+: 적중 0~9% → 하드 차단 (floor 제거, 페널티)
        # QA 70~79: 초기 매집 제한적 활용
        # EXPLOSION 70~74: 25% 적중 → 페널티 유지
        if _tv_pattern == "QUIET_ACCUMULATION" and _tv_score >= 80:
            # ★ QA 80+ 전면 차단: 적중 0~9%, avg -4.8~-6.7%
            _qa_pen = 15.0
            total -= _qa_pen
            sources.append(f"qa_block(-{_qa_pen:.0f},score{_tv_score:.0f})")
            logger.info(f"  [QA 차단] {name}: QUIET_ACC({_tv_score:.0f}) 하드차단 -{_qa_pen}")
        elif _tv_pattern == "EXPLOSION" and 70 <= _tv_score < 75:
            _expl_pen = 8.0
            total -= _expl_pen
            sources.append(f"expl_weak(-{_expl_pen:.0f})")
            logger.info(f"  [TV 약폭발] {name}: EXPLOSION({_tv_score:.0f}) 약신호 페널티 -{_expl_pen}")
        elif _tv_pattern == "EXPLOSION" and _tv_score >= 75:
            pass  # EXPLOSION 75+: 33% 적중 — floor 없음, 자연 점수만

        # 진입/SL/TP: 피보나치 우선 → premove → MACD → bargain → 기본값
        bargain_tp = int(b_info["pre_war_high"] * 0.8) if b_info.get("pre_war_high") else 0
        _fib_sl = _fib.get("sl_fib", 0) if _fib else 0
        _fib_tp = _fib.get("tp_fib", 0) if _fib else 0
        entry = int(p_info.get("entry") or m_info.get("entry") or close)
        from data.sajang_rules import SAJANG
        sl = int(_fib_sl or p_info.get("sl") or m_info.get("sl") or SAJANG.get_normal_sl(close))
        tp = SAJANG.get_take_profit(close)
        sl_source = "FIB" if _fib_sl else p_info.get("sl_source", "ATR")

        # 신뢰도 (교차수 + 기술점수 기반)
        if cross >= 2 and t_info.get("score", 0) >= 3.0:
            confidence = "HIGH"
        elif cross >= 1 and t_info.get("score", 0) >= 2.0:
            confidence = "MED"
        else:
            confidence = "LOW"

        # ★ 4단 브릭4(shadow): 명분 버킷 — cutoff(아래 total_score>0) 미적용, 관측·기록만. fail-open.
        try:
            _tb, _ts, _tp_pass = _aggregate_thesis_buckets({
                "relay_sc": relay_sc, "premove_sc": premove_sc, "tech_sc": tech_sc,
                "nat_sc": nat_sc, "nat_power_sc": nat_power_sc, "tv_direct": tv_direct,
                "doublebuy_sc": doublebuy_sc, "largecap_sc": largecap_sc, "fi_sc": fi_sc,
                "invflow_sc": invflow_sc, "stflow_sc": stflow_sc, "bomb_sc": bomb_sc,
                "dual_buy_sc": dual_buy_sc, "etf_flow_sc": etf_flow_sc, "trix_sc": trix_sc,
                "fib_adj": fib_adj, "surge_sc": surge_sc, "us_relay_sc": us_relay_sc,
                "rotation_bonus": rotation_bonus, "pension_sc": pension_sc,
            })
        except Exception:
            _tb, _ts, _tp_pass = {}, [], True  # fail-open (관측 실패해도 추천 무손상)

        rec = RecommendedStock(
            code=code,
            name=name,
            close=close,
            relay_score=relay_sc,
            premove_score=premove_sc,
            tech_score=tech_sc,
            news_score=n_info.get("score", 0),
            nationality_score=nat_sc,
            nationality_detail=nat_detail,
            cross_count=cross,
            news_penalty=news_pen,
            obv_penalty=obv_pen,
            relative_penalty=rel_pen,
            total_score=round(total, 1),
            today_chg=round(today_chg, 1),
            relative_str=round(relative_str, 1),
            entry=entry,
            sl=sl,
            tp=tp,
            sl_source=sl_source,
            sources=sources,
            tech_detail=t_info.get("detail", ""),
            news_detail=sentiment,
            confidence=confidence,
            nat_power=nat_power_sc,
            nat_power_grade=norm_np.grade if norm_np else "",
            nat_power_detail=norm_np.detail if norm_np else "",
            tv_ratio=_tv_ratio,
            tv_pattern=_tv_pattern,
            tv_score=_tv_score,
            fib_adj=fib_adj,
            fib_position=_fib.get("fib_position", "") if _fib else "",
            fib_upside_pct=_fib.get("upside_pct", 0) if _fib else 0,
            fib_downside_pct=_fib.get("downside_pct", 0) if _fib else 0,
            sl_fib=_fib_sl,
            tp_fib=_fib_tp,
            thesis_buckets=_tb,
            thesis_strong=_ts,
            thesis_pass=_tp_pass,
        )
        candidates.append(rec)

    # 유일한 hard cutoff: 합산 > 0 (페널티가 모든 가산을 초과하면 제거)
    candidates = [c for c in candidates if c.total_score > 0]
    # ★ 4단 브릭4 shadow: 명분 게이트 관측 — cutoff 미적용(위 total_score>0 그대로). 통과분포 로그만.
    try:
        _pass_n = sum(1 for c in candidates if getattr(c, "thesis_pass", True))
        logger.info(f"[명분게이트/shadow] cutoff후 {len(candidates)}건 중 명분통과 {_pass_n}건 / "
                    f"미통과 {len(candidates) - _pass_n}건(약신호더미 후보). cutoff 미적용=관측.")
    except Exception:
        pass

    # 정렬: total_score 내림차순
    candidates.sort(key=lambda x: x.total_score, reverse=True)

    # ── TV 전용 슬롯: 최대 3개 ──
    # TV-only 강신호 종목이 다중소스 종목에 밀려 TOP 8에서 탈락하는 문제 해결
    # v2: 65점, 3슬롯, GRADUAL 포함, 클러스터 소속 우선
    normal_top = candidates[:8]
    top8_codes = {s.code for s in normal_top}

    _cluster_map = tv_cluster_map or {}
    tv_only_picks = []
    for c in candidates[8:]:  # TOP 8 밖의 후보들
        if c.tv_score >= 65 and c.tv_pattern in ("QUIET_ACCUMULATION", "EXPLOSION", "EARLY_ACCUMULATION", "GRADUAL_BUILDUP"):
            tv_only_picks.append(c)

    if tv_only_picks:
        # 클러스터 소속 종목 우선, 그 다음 tv_score 내림차순
        tv_only_picks.sort(
            key=lambda x: (_cluster_map.get(x.code, 0), x.tv_score),
            reverse=True,
        )
        tv_insert = tv_only_picks[:3]
        # 하위 슬롯 교체
        final = normal_top[:8 - len(tv_insert)] + tv_insert
        final.sort(key=lambda x: x.total_score, reverse=True)
        logger.info(
            f"[TV Slot] {len(tv_insert)}종목 TV 전용 삽입: "
            f"{', '.join(f'{s.name}(TV{s.tv_score:.0f})' for s in tv_insert)}"
        )
        return final

    return normal_top


# ═══════════════════════════════════════
#  Step 6: KIS API 교차검증
# ═══════════════════════════════════════

def _step6_kis_verify(stocks: list[RecommendedStock], market_chg: float = 0.0) -> list[RecommendedStock]:
    """KIS API로 최종 후보의 실시간 가격 교차검증

    v2: 절대 -5% 제거 → 가격 교체만 수행 (탈락 판단은 step5 soft scoring에서 완료)
    - pykrx vs KIS 가격 괴리 5% 이상이면 KIS 가격으로 교체
    - close/entry/sl/tp 재계산
    """
    try:
        from bot.kis_trader import KISTrader
        trader = KISTrader()
    except Exception as e:
        logger.warning(f"KIS API 초기화 실패: {e} - 검증 생략")
        return stocks

    # 종목명 fallback용 universe 로드
    _uni_names = {}
    try:
        import json as _j6
        _uni_p = Path(__file__).resolve().parent.parent / "data_store" / "universe.json"
        if _uni_p.exists():
            with open(_uni_p, "r", encoding="utf-8") as _uf6:
                for _c6, _v6 in _j6.load(_uf6).items():
                    _uni_names[_c6] = _v6.get("name", _c6)
    except Exception:
        pass

    verified = []
    for s in stocks:
        try:
            r = trader.fetch_price(s.code)
            if not r or not r.get("success"):
                verified.append(s)
                continue

            kis_price = r["current_price"]
            kis_chg = r["change_rate"]

            # TV-only 등 close=0인 종목 → KIS 가격으로 설정
            if s.close == 0 and kis_price > 0:
                s.close = kis_price
                s.entry = kis_price
                from data.sajang_rules import SAJANG
                s.sl = SAJANG.get_normal_sl(kis_price)
                s.tp = SAJANG.get_take_profit(kis_price)
                logger.info(
                    f"가격 보완: {s.name}({s.code}) close=0 "
                    f"→ KIS {kis_price:,}원 (SL {s.sl:,} / TP {s.tp:,})"
                )
            elif s.close > 0:
                # pykrx vs KIS 가격 괴리 체크 → 가격 교체
                gap_pct = abs(kis_price - s.close) / s.close * 100
                if gap_pct > 5.0:
                    logger.warning(
                        f"가격 괴리: {s.name}({s.code}) "
                        f"pykrx={s.close:,} vs KIS={kis_price:,} ({gap_pct:.1f}%) "
                        f"→ KIS 가격으로 교체"
                    )
                    s.close = kis_price
                    s.entry = kis_price
                    from data.sajang_rules import SAJANG
                    s.sl = SAJANG.get_normal_sl(kis_price)
                    s.tp = SAJANG.get_take_profit(kis_price)

            # KIS 기준 상대강도 업데이트
            s.today_chg = round(kis_chg, 1)
            s.relative_str = round(kis_chg - market_chg, 1)

            # 이름 미해결 종목 보완 (종목코드가 이름인 경우)
            if s.name == s.code:
                resolved = _uni_names.get(s.code, "")
                if resolved:
                    s.name = resolved

            verified.append(s)
        except Exception as e:
            logger.warning(f"KIS 검증 실패 {s.name}: {e}")
            verified.append(s)

    return verified


# ═══════════════════════════════════════
#  메인 파이프라인
# ═══════════════════════════════════════

def run_evening_recommendation() -> RecommendationReport:
    """Stage 1: 저녁 분석 (16:45) - Soft Scoring 파이프라인

    v2 변경점:
    - market_health: diagnose() 사용 + CRITICAL이어도 경고만 (early return 제거)
    - 시장 등락률 조회 → 전 단계에 상대강도 기준 전달
    - 뉴스AI 사전필터 제거: 전종목 분석 (8종목 수준이라 비용 미미)
    - Step5: hard gate 제거 → soft scoring
    """
    import time
    from datetime import datetime

    logger.info("=" * 50)
    logger.info("저녁 추천 파이프라인 시작 (Soft Scoring v2)")
    logger.info("=" * 50)
    t_start = time.time()
    PIPELINE_TOTAL_TIMEOUT = 2100  # 35분 총 타임아웃 (v4.3: 1500→2100, Step3 병렬화+유니버스 2889종목)

    def _check_pipeline_timeout(step_name: str, report_obj, partial_stocks=None):
        """파이프라인 총 경과 시간 체크 — 15분 초과 시 부분 저장 후 리턴"""
        elapsed = time.time() - t_start
        if elapsed < PIPELINE_TOTAL_TIMEOUT:
            return None  # 계속 진행
        logger.warning(
            f"[TIMEOUT] 파이프라인 총 {elapsed:.0f}s 경과 ({step_name} 진입 전) "
            f"— {PIPELINE_TOTAL_TIMEOUT}s 초과, 부분 저장 후 종료"
        )
        report_obj.warning = (
            f"파이프라인 타임아웃 ({elapsed:.0f}s) — {step_name} 이후 단계 스킵"
        )
        if partial_stocks:
            report_obj.stocks = partial_stocks
        try:
            save_recommendation(report_obj)
            logger.info(f"[TIMEOUT] 부분 저장 완료: {len(report_obj.stocks)}종목")
        except Exception as e:
            logger.error(f"[TIMEOUT] 부분 저장 실패: {e}")
        return report_obj

    report = RecommendationReport(
        stage="evening",
        timestamp=datetime.now().strftime("%Y-%m-%d %H:%M"),
    )

    # 0) 시장 건전성 + CORTEX 체제/충격 - CRITICAL이어도 경고만 (종목 추천은 계속)
    regime_info = {}
    shock_info = {}
    try:
        from data.market_health import diagnose, get_position_multiplier
        health = diagnose()
        report.market_health = health.alert_level.upper()
        if health.alert_level == "critical":
            report.warning = "시장 건전성 CRITICAL - 매수 규모 축소 권고"
        # CORTEX 체제/충격 정보 추출
        regime_info = {
            "regime": getattr(health, "regime", "NORMAL"),
            "new_buy": getattr(health, "regime_new_buy", True),
            "capital_use": getattr(health, "position_multiplier", 1.0),
        }
        shock_info = {
            "shock_type": getattr(health, "shock_type", "NONE"),
            "affected_sectors": getattr(health, "affected_sectors", []),
            "opportunity_sectors": getattr(health, "opportunity_sectors", []),
        }
        logger.info(f"[CORTEX] 체제: {regime_info['regime']} | 충격: {shock_info['shock_type']}")
    except Exception as e:
        try:
            from data.market_health import get_position_multiplier
            mult = get_position_multiplier()
            report.market_health = "NORMAL" if mult >= 1.0 else ("WARNING" if mult > 0 else "CRITICAL")
        except Exception:
            report.market_health = "UNKNOWN"
        logger.warning(f"시장건전성 체크 실패: {e}")

    # 0.5) 시장 등락률 (상대강도 기준)
    t0 = time.time()
    market_chg = _get_market_change_today()
    report.market_change = market_chg
    logger.info(f"[시장] KOSPI 등락률: {market_chg:+.2f}% ({time.time()-t0:.0f}s)")

    # Step 1: 릴레이 스캔
    t0 = time.time()
    logger.info("[Step 1/6] 릴레이 스캔...")
    relay_result = _step1_relay_scan()
    report.relay_summary = relay_result.get("summary", "")
    logger.info(f"  → {len(relay_result.get('stocks', {}))}종목 ({time.time()-t0:.0f}s)")

    # Step 1.5: 섹터 로테이션 분석 (히스토리 저장 + 다음 섹터 감지)
    #   Step 1에서 이미 스캔한 all_sectors를 재활용 (중복 pykrx 호출 방지)
    rotation_stocks = {}  # {code: rotation_info}
    t0 = time.time()
    logger.info("[Step 1.5] 섹터 로테이션 분석...")
    try:
        from data.rotation_detector import record_today, analyze_rotation, get_next_sector_stocks
        sector_results = relay_result.get("all_sectors", [])
        if sector_results:
            history = record_today(sector_results)
            rotation = analyze_rotation(history)
            report.rotation_signal = rotation.rotation_signal
            # 상세 정보 저장
            report.rotation_detail = []
            for s in rotation.hot_sectors + rotation.staging_sectors + rotation.cooling_sectors:
                report.rotation_detail.append({
                    "sector": s.sector_name,
                    "phase": s.phase,
                    "hot_days": s.hot_days,
                    "momentum": s.current_momentum,
                    "breadth": s.current_breadth,
                    "signal": s.signal,
                })
            # 다음 섹터 종목 추출
            rotation_stocks = get_next_sector_stocks(rotation)
            next_count = sum(1 for v in rotation_stocks.values()
                            if v["rotation_source"] != "reversal_exit")
            logger.info(f"  → 로테이션: {rotation.rotation_signal}")
            logger.info(f"  → 다음섹터 후보 {next_count}종목 ({time.time()-t0:.0f}s)")
        else:
            logger.warning("  → 섹터 스캔 결과 없음, 로테이션 분석 스킵")
    except Exception as e:
        logger.warning(f"로테이션 분석 실패 (무시): {e}")
        import traceback
        logger.debug(traceback.format_exc())

    # 기술분석 대상 종목 수집 (Step 1.7 HOT 주입에서도 사용)
    all_codes_set = set()

    # Step 1.7: 섹터 모멘텀 분석 (pykrx 전체 시장 직접 조회)
    sector_momentum_report = None
    sector_boost_map = {}  # {code: boost_score}
    t0 = time.time()
    logger.info("[Step 1.7] 섹터 모멘텀 분석 (전체 23개 섹터)...")
    try:
        from data.sector_momentum import analyze_sectors, get_hot_sector_codes
        sector_momentum_report = analyze_sectors()
        if sector_momentum_report:
            hot_sects = [s for s in sector_momentum_report.sectors
                         if isinstance(s, dict) and s.get("phase") in ("HOT", "WARMING")]
            logger.info(f"  → {len(hot_sects)}개 강세 섹터: "
                        + ", ".join(f"{s['sector']}({s['avg_return_1d']:+.1f}%)" for s in hot_sects))
            # HOT 섹터 종목을 자동으로 후보에 주입
            hot_codes = get_hot_sector_codes(top_n=5)
            hot_injected = 0
            for code, boost in hot_codes.items():
                sector_boost_map[code] = boost
                # 부스트 15점 이상인 HOT 섹터 종목은 분석 대상에 추가
                if boost >= 15:
                    # 유니버스에서 이름 조회
                    _uni_info = _universe.get(code, {}) if '_universe' in dir() else {}
                    _nm = _uni_info.get("name", code) if isinstance(_uni_info, dict) else code
                    if (code, _nm) not in all_codes_set:
                        all_codes_set.add((code, _nm))
                        hot_injected += 1
            logger.info(f"  → 섹터부스트 {len(sector_boost_map)}종목, HOT주입 {hot_injected}종목 ({time.time()-t0:.0f}s)")
    except Exception as e:
        logger.warning(f"섹터 모멘텀 분석 실패 (무시): {e}")
        import traceback
        logger.debug(traceback.format_exc())

    # Step 2: 사전감지 스캔
    t0 = time.time()
    logger.info("[Step 2/6] 사전감지 스캔...")
    premove_result = _step2_premove_scan()
    logger.info(f"  → {len(premove_result.get('stocks', {}))}종목 ({time.time()-t0:.0f}s)")

    # 기술분석 대상 종목 수집 (Step 1.7 HOT 주입분 유지)
    for code, info in relay_result.get("stocks", {}).items():
        all_codes_set.add((code, info.get("name", code)))
    for code, info in premove_result.get("stocks", {}).items():
        all_codes_set.add((code, info.get("name", code)))

    # 로테이션 다음 섹터 종목 추가 (reversal_exit 제외)
    for code, info in rotation_stocks.items():
        if info["rotation_source"] != "reversal_exit":
            all_codes_set.add((code, info["name"]))

    # Step 2.5: MACD 제로선 크로스 스캔
    t0 = time.time()
    logger.info("[Step 2.5] MACD 0선 크로스 스캔...")
    macd_result = _step_macd_zero_scan()
    for code, info in macd_result.items():
        if (code, info.get("name", code)) not in all_codes_set:
            all_codes_set.add((code, info.get("name", code)))
    logger.info(f"  → {len(macd_result)}종목 ({time.time()-t0:.0f}s)")

    # Step 2.6: TRIX 다이버전스 스캔
    t0 = time.time()
    logger.info("[Step 2.6] TRIX 다이버전스 스캔...")
    trix_result = _step_trix_divergence_scan()
    for code, info in trix_result.items():
        if (code, info.get("name", code)) not in all_codes_set:
            all_codes_set.add((code, info.get("name", code)))
    logger.info(f"  → {len(trix_result)}종목 ({time.time()-t0:.0f}s)")

    # Step 2.7: 줍줍 스캔 (낙폭+수급매집)
    t0 = time.time()
    logger.info("[Step 2.7] 줍줍 스캔 (낙폭+수급매집)...")
    bargain_result = _step_bargain_scan()
    for code, info in bargain_result.items():
        if (code, info.get("name", code)) not in all_codes_set:
            all_codes_set.add((code, info.get("name", code)))
    logger.info(f"  → {len(bargain_result)}종목 ({time.time()-t0:.0f}s)")

    # Step 2.8: 소형주 모멘텀 스캔 (급등주 포착)
    t0 = time.time()
    logger.info("[Step 2.8] 소형주 모멘텀 스캔...")
    try:
        from strategies.momentum_scanner import scan_momentum
        momentum_result = scan_momentum(top_n=5)
        report.momentum_stocks = [
            {
                "code": c.code, "name": c.name,
                "momentum_score": c.momentum_score,
                "theme": c.theme, "theme_detail": c.theme_detail,
                "volume_spike": c.volume_spike,
                "price_change_5d": c.price_change_5d,
                "entry": c.entry, "sl": c.sl, "tp": c.tp,
                "cap_억": c.cap_억,
            }
            for c in momentum_result
        ]
        logger.info(f"  → {len(momentum_result)}종목 ({time.time()-t0:.0f}s)")
    except Exception as e:
        logger.warning(f"모멘텀 스캔 실패: {e}")

    # Step 2.9: 전쟁→재건 섹터릴레이 워치리스트
    t0 = time.time()
    logger.info("[Step 2.9] 전쟁→재건 릴레이 워치리스트...")
    war_relay_list = _step_war_relay_inject()
    if war_relay_list:
        # 기술분석 대상에 추가 (메인 파이프라인과 동일하게 스코어링)
        for s in war_relay_list:
            all_codes_set.add((s["code"], s["name"]))
        # report에 저장 (auto_trader가 별도로 읽음)
        report.war_relay_stocks = war_relay_list
        logger.info(f"  → {len(war_relay_list)}종목 ({time.time()-t0:.0f}s)")

    if not all_codes_set:
        report.warning = "릴레이+사전감지+MACD+줍줍 결과 0건 - 추천 불가"
        try:
            save_recommendation(report)
        except Exception:
            pass
        return report

    # ── 종목 수 캡 (80개) — Step 3~5 처리 시간 폭증 방지 ──
    PIPELINE_STOCK_CAP = 80
    if len(all_codes_set) > PIPELINE_STOCK_CAP:
        logger.warning(
            f"[CAP] 분석 대상 {len(all_codes_set)}종목 → {PIPELINE_STOCK_CAP}개 제한 "
            f"(relay/premove 우선)"
        )
        # 우선순위: relay > premove > rotation > HOT섹터 > MACD/TRIX/bargain/war
        priority_codes = set()
        # 1순위: relay + premove (핵심)
        for code, info in relay_result.get("stocks", {}).items():
            priority_codes.add(code)
        for code, info in premove_result.get("stocks", {}).items():
            priority_codes.add(code)
        # 2순위: rotation (reversal_exit 제외)
        for code, info in rotation_stocks.items():
            if info.get("rotation_source") != "reversal_exit":
                priority_codes.add(code)

        # 우선 종목 먼저, 나머지는 뒤에
        prioritized = []
        rest = []
        for item in all_codes_set:
            if item[0] in priority_codes:
                prioritized.append(item)
            else:
                rest.append(item)
        all_codes_set = set(prioritized + rest[:PIPELINE_STOCK_CAP - len(prioritized)])
        logger.info(f"[CAP] 최종: {len(all_codes_set)}종목 (우선 {len(prioritized)}, 추가 {len(all_codes_set) - len(prioritized)})")

    codes_names = list(all_codes_set)

    # ── ETF 제외 (ETF는 Step 5d에서 별도 추천) ──
    _ETF_PREFIXES = ('KODEX', 'TIGER', 'RISE', 'ACE', 'SOL', 'PLUS',
                     'HANARO', 'KoAct', 'KIWOOM', 'WON', 'ITF', '1Q', 'TIME')
    _before_etf = len(codes_names)
    codes_names = [(c, n) for c, n in codes_names if not n.startswith(_ETF_PREFIXES)]
    _etf_removed = _before_etf - len(codes_names)
    if _etf_removed:
        logger.info(f"[ETF필터] ETF {_etf_removed}개 제외 (일반추천에서)")

    # ── 타임아웃 가드: Step 3 진입 전 ──
    _timeout_result = _check_pipeline_timeout("Step3(기술분석)", report)
    if _timeout_result:
        return _timeout_result

    # Step 3: 기술 분석 (병렬 청크 — I/O bound pykrx 호출 5배 가속)
    t0 = time.time()
    TECH_WORKERS = 5
    TECH_CHUNK_SIZE = max(1, (len(codes_names) + TECH_WORKERS - 1) // TECH_WORKERS)
    _tech_chunks = [codes_names[i:i+TECH_CHUNK_SIZE] for i in range(0, len(codes_names), TECH_CHUNK_SIZE)]
    logger.info(f"[Step 3/6] 기술 분석 ({len(codes_names)}종목 → {len(_tech_chunks)}청크x{TECH_WORKERS}워커, 시장 {market_chg:+.1f}%)...")
    tech_result = {}
    from concurrent.futures import ThreadPoolExecutor, as_completed
    with ThreadPoolExecutor(max_workers=TECH_WORKERS, thread_name_prefix="tech") as _tech_pool:
        _tech_futs = {_tech_pool.submit(_step3_tech_filter, chunk, market_chg): i
                      for i, chunk in enumerate(_tech_chunks)}
        for _tf in as_completed(_tech_futs):
            try:
                tech_result.update(_tf.result())
            except Exception as _e3:
                logger.warning(f"기술분석 청크 실패: {_e3}")
    logger.info(f"  → 완료 {len(tech_result)}종목 ({time.time()-t0:.0f}s)")

    # ── 타임아웃 가드: Step 4 진입 전 ──
    _timeout_result = _check_pipeline_timeout("Step4(뉴스AI)", report)
    if _timeout_result:
        return _timeout_result

    # Step 4: 뉴스AI - 전종목 분석 (타임아웃 방어)
    t0 = time.time()
    NEWS_AI_TIMEOUT = 120  # 2분 최대 (기존 600s → 타임아웃 빈발로 축소)
    NEWS_AI_MAX_STOCKS = 15  # 캐시 미스 시 최대 15종목만 신규 분석 (기존 40)
    logger.info(f"[Step 4/6] 뉴스AI ({len(codes_names)}종목, timeout={NEWS_AI_TIMEOUT}s, max_new={NEWS_AI_MAX_STOCKS})...")
    try:
        from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout
        with ThreadPoolExecutor(max_workers=1, thread_name_prefix="news_ai") as _executor:
            _future = _executor.submit(_step4_news_filter, codes_names, NEWS_AI_MAX_STOCKS)
            news_result = _future.result(timeout=NEWS_AI_TIMEOUT)
    except FuturesTimeout:
        logger.warning(f"[Step 4] 뉴스AI 타임아웃 ({NEWS_AI_TIMEOUT}s) - 분석된 것만 사용")
        news_result = {}
    except Exception as e_news:
        logger.warning(f"[Step 4] 뉴스AI 실패: {e_news} - 전체 NEUTRAL 처리")
        news_result = {}
    # 누락 종목 NEUTRAL
    for code, name in codes_names:
        if code not in news_result:
            news_result[code] = {"sentiment": "NEUTRAL", "reason": "미분석", "score": 0}
    logger.info(f"  → 완료 ({time.time()-t0:.0f}s, 분석={len(news_result)}건)")

    # ── 타임아웃 가드: Step 5a 진입 전 ──
    _timeout_result = _check_pipeline_timeout("Step5a(수급)", report)
    if _timeout_result:
        return _timeout_result

    # Step 5a: 네이버 수급 검증 (외국인/기관 5일 누적 순매매)
    #   기존 nationality_signal.py는 KRX 국적별 데이터 3종목만 존재 → 사실상 미작동
    #   → 네이버 금융 크롤링으로 대체 (백테스트 PF 1.50과 동일 로직)
    nationality_scores = {}
    SUPPLY_TIMEOUT = 120  # 네이버 수급 최대 2분
    try:
        t_supply = time.time()
        from data.supply_naver import score_supply_batch
        all_code_list = [code for code, _ in codes_names]
        # 종가 딕셔너리 전달 (naver 재조회 줄이기)
        close_prices = {}
        for code, name in codes_names:
            t_info = tech_result.get(code, {})
            if t_info.get("close"):
                close_prices[code] = int(t_info["close"])
        # 타임아웃 방어 (기존에 없었음 → 무한대기 원인)
        from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout
        with ThreadPoolExecutor(max_workers=1, thread_name_prefix="supply") as _sup_exec:
            _sup_future = _sup_exec.submit(
                score_supply_batch, all_code_list, close_prices=close_prices
            )
            try:
                nationality_scores = _sup_future.result(timeout=SUPPLY_TIMEOUT)
            except FuturesTimeout:
                logger.warning(f"[Step 5a] 네이버 수급 타임아웃 ({SUPPLY_TIMEOUT}s) - 스킵")
                nationality_scores = {}
        scored = sum(1 for sc, _ in nationality_scores.values() if sc != 0)
        logger.info(f"[Step 5a] 네이버 수급: {scored}/{len(all_code_list)}종목 점수 반영 ({time.time()-t_supply:.0f}s)")
    except Exception as e:
        logger.warning(f"네이버 수급 점수 실패 (무시): {e}")
        import traceback
        logger.debug(traceback.format_exc())

    # Step 5a.5: 거래대금 폭발 스캔 (전체 유니버스 + 소형주)
    #   최근 2시간 이내 TV 스캔 결과 있으면 재사용 (pykrx 대량 호출 방지)
    tv_signals = {}  # {code: TVSignal dict}
    tv_persistence = {}  # TV 잔존 효과 (outer try 실패 시에도 안전)
    try:
        import json as _json5a5
        t_tv = time.time()
        _tv_cache_path = Path(__file__).resolve().parent.parent / "data_store" / "tv_scanner.json"
        _tv_cache_fresh = False
        if _tv_cache_path.exists():
            try:
                _tv_cache = _json5a5.loads(_tv_cache_path.read_text("utf-8"))
                _tv_scan_date = _tv_cache.get("scan_date", "")
                _today_str = datetime.now().strftime("%Y-%m-%d")
                if _tv_scan_date == _today_str:
                    _tv_cache_fresh = True
                    logger.info(f"[Step 5a.5] TV 스캔 캐시 재사용 (오늘 {_tv_scan_date} 생성)")
                else:
                    # 비거래일이면 마지막 거래일 캐시 재사용 (풀스캔 hang 방지)
                    try:
                        from data.trading_calendar import is_trading_day
                        from datetime import date as _date_tv
                        if not is_trading_day(_date_tv.today()):
                            _tv_cache_fresh = True
                            logger.info(
                                f"[Step 5a.5] 비거래일 — TV 캐시 재사용 "
                                f"(캐시 {_tv_scan_date}, 오늘 {_today_str})"
                            )
                    except Exception:
                        pass
            except Exception:
                pass

        # universe.json은 TV 풀스캔 여부와 무관하게 항상 로드 (클러스터 감지 등에서 필요)
        _uni_path = Path(__file__).resolve().parent.parent / "data_store" / "universe.json"
        try:
            with open(_uni_path, "r", encoding="utf-8") as _uf:
                _universe = _json5a5.load(_uf)
        except Exception:
            _universe = {}

        if _tv_cache_fresh:
            # 캐시에서 로드 (TVSignal namedtuple 대신 dict 사용)
            from types import SimpleNamespace
            for s in _tv_cache.get("signals", []):
                ns = SimpleNamespace(**s)
                if ns.score > 0:
                    tv_signals[ns.code] = ns
        else:
            # 소형주 유니버스 병합 (500억 이하도 TV 스캔 대상)
            _sc_path = Path(__file__).resolve().parent.parent / "data_store" / "universe_smallcap.json"
            if _sc_path.exists():
                try:
                    _sc_uni = _json5a5.loads(_sc_path.read_text("utf-8"))
                    _sc_count = 0
                    for code, info in _sc_uni.items():
                        if code not in _universe:
                            _universe[code] = info
                            _sc_count += 1
                    logger.info(f"  소형주 유니버스 병합: +{_sc_count}종목 → 총 {len(_universe)}")
                except Exception:
                    pass
            # TV 풀스캔에 타임아웃 적용 (hang 방지)
            TV_SCAN_TIMEOUT = 180  # 3분
            from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout
            from data.trading_value_scanner import scan_trading_value, save_tv_results
            with ThreadPoolExecutor(max_workers=1, thread_name_prefix="tv_scan") as _tv_exec:
                _tv_future = _tv_exec.submit(scan_trading_value, _universe, min_tv_billion=10.0)
                try:
                    tv_results = _tv_future.result(timeout=TV_SCAN_TIMEOUT)
                    save_tv_results(tv_results)
                    tv_signals = {s.code: s for s in tv_results if s.score > 0}
                except FuturesTimeout:
                    logger.warning(f"[Step 5a.5] TV 풀스캔 타임아웃 ({TV_SCAN_TIMEOUT}s) - 스킵")
                    tv_signals = {}
        # TV 스캐너에서 잡힌 종목도 교차검증 대상에 추가 (code 기반 dedup)
        # TV 추가 시에도 전체 캡 유지 (100개 — TV 강신호는 별도 여유)
        TV_ADD_CAP = 100
        tv_added = 0
        existing_codes = {code for code, _ in all_codes_set}
        for code, tv_sig in sorted(tv_signals.items(), key=lambda x: x[1].score, reverse=True):
            if len(all_codes_set) >= TV_ADD_CAP:
                logger.info(f"[Step 5a.5] TV 추가 캡 도달 ({TV_ADD_CAP}종목) — 나머지 스킵")
                break
            if tv_sig.score >= 60 and code not in existing_codes:
                all_codes_set.add((code, tv_sig.name))
                codes_names.append((code, tv_sig.name))
                existing_codes.add(code)
                tv_added += 1
        logger.info(f"[Step 5a.5] 거래대금 이상: {len(tv_signals)}종목 ({tv_added}종목 신규추가) ({time.time()-t_tv:.0f}s)")

        # ── TV 잔존 효과 (Signal Persistence) ──
        tv_persistence = {}
        try:
            from data.trading_value_scanner import calc_tv_persistence
            today_tv_codes = set(tv_signals.keys())
            tv_persistence = calc_tv_persistence(today_codes=today_tv_codes)
            # 잔존 종목도 교차검증 대상에 추가
            persist_added = 0
            for pcode, pinfo in tv_persistence.items():
                if pinfo["persistence_score"] >= 10 and pcode not in existing_codes:
                    pname = pinfo.get("name", pcode)
                    all_codes_set.add((pcode, pname))
                    codes_names.append((pcode, pname))
                    existing_codes.add(pcode)
                    persist_added += 1
            if tv_persistence:
                logger.info(f"[Step 5a.6] TV 잔존 시그널: {len(tv_persistence)}종목 ({persist_added}종목 신규추가)")
        except Exception as e:
            logger.warning(f"[TV Persistence] 로드 실패: {e}")

        # TV 신규추가 종목 보충 분석 (tech + news) — 타임아웃 가드 + 병렬화
        if tv_added > 0:
            _tv_elapsed = time.time() - t_start
            tv_new_codes = [(code, tv_signals[code].name) for code, tv_sig in tv_signals.items()
                           if tv_sig.score >= 60 and code not in tech_result]
            if tv_new_codes and _tv_elapsed < PIPELINE_TOTAL_TIMEOUT * 0.65:
                try:
                    _tv_csize = max(1, (len(tv_new_codes) + TECH_WORKERS - 1) // TECH_WORKERS)
                    _tv_chunks = [tv_new_codes[i:i+_tv_csize] for i in range(0, len(tv_new_codes), _tv_csize)]
                    tv_tech = {}
                    with ThreadPoolExecutor(max_workers=TECH_WORKERS, thread_name_prefix="tv_tech") as _tv_pool:
                        _tv_futs = {_tv_pool.submit(_step3_tech_filter, c, market_chg): i
                                    for i, c in enumerate(_tv_chunks)}
                        for _tvf in as_completed(_tv_futs):
                            try:
                                tv_tech.update(_tvf.result())
                            except Exception:
                                pass
                    tech_result.update(tv_tech)
                    logger.info(f"  → TV 보충 기술분석: {len(tv_tech)}종목 ({len(_tv_chunks)}청크 병렬)")
                except Exception as e_tv_tech:
                    logger.warning(f"TV 보충 기술분석 실패: {e_tv_tech}")
            elif tv_new_codes:
                logger.warning(f"[TV 보충] 경과 {_tv_elapsed:.0f}s (타임아웃 65%) — 기술분석 스킵")
            # 뉴스는 NEUTRAL로 채움 (속도 우선)
            if tv_new_codes:
                for code, name in tv_new_codes:
                    if code not in news_result:
                        news_result[code] = {"sentiment": "NEUTRAL", "reason": "TV신규(미분석)", "score": 0}
    except Exception as e:
        logger.warning(f"TV 스캐너 실패 (무시): {e}")
        import traceback
        logger.debug(traceback.format_exc())

    # Step 5a.6: TV 클러스터 감지 (그룹/섹터 동시 폭발)
    tv_cluster_map = {}
    tv_cluster_info = []
    if tv_signals:
        try:
            tv_cluster_map, tv_cluster_info = _detect_tv_clusters(tv_signals, _universe)
        except Exception as e:
            logger.warning(f"TV 클러스터 감지 실패 (무시): {e}")

    # Step 5b: MOMENTUM 레짐 감지 (수급 기반 초기 진입 시그널)
    regime_signals = {}
    try:
        import json as _json5b
        t_regime = time.time()
        from data.regime_detector import detect_regime_batch
        try:
            _universe  # 5a.5에서 이미 로드됨
        except NameError:
            uni_path = Path(__file__).resolve().parent.parent / "data_store" / "universe.json"
            with open(uni_path, "r", encoding="utf-8") as _uf:
                _universe = _json5b.load(_uf)
        # rotation_detail에서 sector breadth 재활용
        sector_breadths = {}
        for rd in report.rotation_detail:
            if isinstance(rd, dict) and "sector" in rd:
                sector_breadths[rd["sector"]] = rd.get("breadth", 0)
        regime_signals = detect_regime_batch(
            [code for code, _ in codes_names], _universe, sector_breadths
        )
        mtm_count = sum(1 for r in regime_signals.values() if r.regime == "MOMENTUM")
        logger.info(f"[Step 5b] 레짐: {mtm_count}/{len(regime_signals)} MOMENTUM ({time.time()-t_regime:.0f}s)")
    except Exception as e:
        logger.warning(f"레짐 감지 실패 (무시): {e}")

    # ── 타임아웃 가드: Step 5 교차검증 진입 전 ──
    _timeout_result = _check_pipeline_timeout("Step5(교차검증)", report)
    if _timeout_result:
        return _timeout_result

    # Step 5: Soft Scoring 교차검증 (로테이션 종목 포함)
    t0 = time.time()
    logger.info("[Step 5/6] Soft Scoring 교차검증...")
    final_stocks = _step5_cross_validate(
        relay_result, premove_result, tech_result, news_result,
        macd_result=macd_result,
        nationality=nationality_scores,
        bargain_result=bargain_result,
        market_chg=market_chg,
        regime_info=regime_info,
        shock_info=shock_info,
        rotation_stocks=rotation_stocks,
        tv_signals=tv_signals,
        tv_cluster_map=tv_cluster_map,
        tv_persistence=tv_persistence,
        trix_result=trix_result,
    )
    logger.info(f"  → {len(final_stocks)}종목 ({time.time()-t0:.0f}s)")

    # 교차검증 후 regime 스탬핑 + MOMENTUM 스코어 부스트
    mtm_boost_count = 0
    for s in final_stocks:
        if s.code in regime_signals:
            r = regime_signals[s.code]
            s.regime = r.regime
            s.regime_score = r.score
            s.regime_detail = f"VOL{r.vol_ratio:.1f}x+TV{r.tv_ratio:.1f}x+기관{r.consec_inst_foreign_days}D"
            # MOMENTUM 종목: total_score에 가산 (TOP 8 진입 확률 증가)
            if r.regime == "MOMENTUM":
                boost = min(r.score * 15, 10)  # score 0.40→+6, 0.55→+8, 최대+10
                s.total_score += boost
                mtm_boost_count += 1
                logger.info(f"  [MTM 부스트] {s.name}: +{boost:.1f}점 → {s.total_score:.1f}")

        # TV 스캐너 스탬핑 (regime과 독립)
        if s.code in tv_signals:
            _tv = tv_signals[s.code]
            s.tv_ratio = _tv.tv_ratio
            s.tv_pattern = _tv.pattern
            s.tv_score = _tv.score
            s.tv_frgn_joined = getattr(_tv, "frgn_joined", False)

    # MOMENTUM 부스트 후 재정렬 (TOP 8에 MOMENTUM 종목 우선 배치)
    if mtm_boost_count > 0:
        final_stocks.sort(key=lambda x: x.total_score, reverse=True)
        logger.info(f"  [MTM] {mtm_boost_count}종목 부스트 → 재정렬 완료")

    # ── Step 5d: 섹터 모멘텀 부스트 ──
    sect_boost_count = 0
    if sector_boost_map:
        for s in final_stocks:
            boost = sector_boost_map.get(s.code, 0)
            if boost != 0:
                s.total_score += boost
                sect_boost_count += 1
                logger.info(f"  [섹터부스트] {s.name}: {boost:+.1f}점 → {s.total_score:.1f}")
        if sect_boost_count > 0:
            final_stocks.sort(key=lambda x: x.total_score, reverse=True)
            logger.info(f"  [섹터] {sect_boost_count}종목 부스트 → 재정렬 완료")

    # 섹터 모멘텀 보고서를 report에 첨부 (save_recommendation에서 사용)
    if sector_momentum_report:
        from dataclasses import asdict as _asdict
        report._sector_momentum = _asdict(sector_momentum_report)

    # ── Step 5d-2: FX→섹터 환율 부스트 (BOND-P1) ──
    try:
        from data.fx_sector_signal import get_fx_all_sector_boosts
        # nightwatch_report.json에서 USDKRW 데이터 로드
        nw_path = BASE_DIR / "data_store" / "nightwatch_report.json"
        if nw_path.exists():
            nw_data = json.loads(nw_path.read_text("utf-8"))
            ri = nw_data.get("raw_indicators", {})
            fx_boosts = get_fx_all_sector_boosts(ri)
            if fx_boosts:
                fx_count = 0
                for s in final_stocks:
                    s_sector = getattr(s, "sector", "")
                    if not s_sector and _universe:
                        ui = _universe.get(s.code, {})
                        s_sector = ui.get("sector", "") if isinstance(ui, dict) else ""
                    fx_adj = fx_boosts.get(s_sector, 0.0)
                    if abs(fx_adj) >= 0.3:
                        s.total_score += fx_adj
                        fx_count += 1
                        logger.debug(f"  [FX부스트] {s.name}({s_sector}): {fx_adj:+.1f}점")
                if fx_count > 0:
                    final_stocks.sort(key=lambda x: x.total_score, reverse=True)
                    logger.info(f"  [FX→섹터] {fx_count}종목 환율 부스트 → 재정렬 완료")
    except Exception as e:
        logger.debug(f"FX 섹터 부스트 실패 (무시): {e}")

    # ── Step 5d-3: 인플레이션 비용 체인 부스트 (BOND-P3) ──
    try:
        from data.inflation_chain import get_inflation_sector_boosts
        nw_path2 = BASE_DIR / "data_store" / "nightwatch_report.json"
        if nw_path2.exists():
            nw_data2 = json.loads(nw_path2.read_text("utf-8"))
            ri2 = nw_data2.get("raw_indicators", {})
            infl_boosts = get_inflation_sector_boosts(ri2)
            if infl_boosts:
                infl_count = 0
                for s in final_stocks:
                    s_sector = getattr(s, "sector", "")
                    if not s_sector and _universe:
                        ui = _universe.get(s.code, {})
                        s_sector = ui.get("sector", "") if isinstance(ui, dict) else ""
                    infl_adj = infl_boosts.get(s_sector, 0.0)
                    if abs(infl_adj) >= 0.5:
                        s.total_score += infl_adj
                        infl_count += 1
                        logger.debug(f"  [인플레체인] {s.name}({s_sector}): {infl_adj:+.1f}점")
                if infl_count > 0:
                    final_stocks.sort(key=lambda x: x.total_score, reverse=True)
                    logger.info(f"  [인플레→섹터] {infl_count}종목 비용체인 부스트 → 재정렬 완료")
    except Exception as e:
        logger.debug(f"인플레 비용체인 부스트 실패 (무시): {e}")

    # ── Step 5d-4: 매크로 전략 섹터 블랙리스트/페널티 ──
    try:
        from data.macro_strategy import get_regime_response, get_sector_score_multiplier, should_avoid_sector
        regime_resp = get_regime_response()
        if regime_resp.regime != "안정":
            avoid_count = 0
            mult_count = 0
            for s in final_stocks[:]:
                s_sector = getattr(s, "sector", "")
                if not s_sector and _universe:
                    ui = _universe.get(s.code, {})
                    s_sector = ui.get("sector", "") if isinstance(ui, dict) else ""
                # 블랙리스트 확인 (스태그플레이션 시 VICTIM 제거)
                avoid, reason = should_avoid_sector(s_sector, regime_resp.regime)
                if avoid:
                    logger.info(f"  [매크로전략] {s.name} 제외: {reason}")
                    final_stocks.remove(s)
                    avoid_count += 1
                    continue
                # 섹터 점수 배수 (VICTIM 감점, BENEFICIARY 가점)
                mult = get_sector_score_multiplier(s_sector, regime_resp.regime)
                if abs(mult - 1.0) > 0.05:
                    old = s.total_score
                    s.total_score = round(s.total_score * mult, 1)
                    mult_count += 1
                    logger.debug(f"  [매크로전략] {s.name}({s_sector}): x{mult:.1f} ({old:.1f}→{s.total_score:.1f})")
            if avoid_count or mult_count:
                final_stocks.sort(key=lambda x: x.total_score, reverse=True)
                logger.info(f"  [매크로전략] {regime_resp.strategy_name}: {avoid_count}종목 제외, {mult_count}종목 배수 적용")
    except Exception as e:
        logger.debug(f"매크로 전략 적용 실패 (무시): {e}")

    # ── Step 5b-2: 섹터 기관 수급 부스트 (TIER2) ──
    try:
        from data.sector_institution_flow import get_sector_flow_boost
        # code_to_sector 빌드 (_universe에서)
        code_to_sector = {}
        if '_universe' in dir():
            for _c, _info in _universe.items():
                if isinstance(_info, dict) and _info.get("sector"):
                    code_to_sector[_c] = _info["sector"]
        inst_boost_count = 0
        for s in final_stocks:
            # 종목의 섹터 조회
            sec = code_to_sector.get(s.code, "")
            if not sec:
                continue
            iboost = get_sector_flow_boost(sec)
            if abs(iboost) >= 1.0:
                s.total_score += iboost
                inst_boost_count += 1
                if abs(iboost) >= 5.0:
                    logger.info(f"  [기관수급] {s.name}: {iboost:+.1f}점 ({sec}) → {s.total_score:.1f}")
        if inst_boost_count > 0:
            final_stocks.sort(key=lambda x: x.total_score, reverse=True)
            logger.info(f"  [기관수급] {inst_boost_count}종목 부스트 → 재정렬 완료")
    except Exception as e:
        logger.warning(f"섹터 기관수급 부스트 실패 (무시): {e}")

    # ── Step 5c: 원자재 릴레이 부스트 ──
    try:
        from data.nightwatch import get_commodity_trend, JARVIS_SECTORS
        commodity = get_commodity_trend(5)
        if commodity:
            # JARVIS 섹터별 수혜 종목 코드 수집
            _commodity_sector_map = {}  # code → (sector_name, boost_reason)
            _active_sectors = []
            # 원자재 조건 → 활성 섹터 결정
            relay = commodity.get("relay")
            active_sigs = commodity.get("active_signals", [])
            if "ng_up" in active_sigs or (commodity.get("ng", {}).get("direction") == "UP"):
                _active_sectors.append(("natural_gas", "NG↑"))
                _active_sectors.append(("shipbuilding", "NG↑LNG선"))  # LNG 공급차질 → 조선 수혜
            if "oil_up" in active_sigs or (commodity.get("oil", {}).get("direction") == "UP"):
                _active_sectors.append(("oil_resource", "OIL↑"))
                _active_sectors.append(("shipbuilding", "OIL↑탱커"))  # [G11] 유가↑ → 조선(탱커) 수혜
            if relay == "silver" or "silver_up" in active_sigs:
                _active_sectors.append(("precious_metals", "은릴레이"))
            if relay == "copper" or "copper_up" in active_sigs:
                _active_sectors.append(("industrial_metals", "구리릴레이"))
                _active_sectors.append(("battery_ev", "구리↑EV"))  # [G10] 구리↑ → 2차전지 수혜

            for sector_key, reason in _active_sectors:
                sector = JARVIS_SECTORS.get(sector_key, {})
                for stock in sector.get("kr_tier1", []) + sector.get("kr_tier2", []):
                    _commodity_sector_map[stock["code"]] = (sector.get("name", ""), reason)

            # 후보 종목에 부스트 적용
            commodity_boost_count = 0
            for s in final_stocks:
                if s.code in _commodity_sector_map:
                    sector_name, reason = _commodity_sector_map[s.code]
                    boost = 10  # tier1/tier2 동일 +10 (릴레이 종목이므로)
                    s.total_score += boost
                    if not hasattr(s, 'sources') or not s.sources:
                        s.sources = []
                    s.sources.append(f"commodity:{reason}({sector_name})")
                    commodity_boost_count += 1
                    logger.info(f"  [원자재 부스트] {s.name}: +{boost}점 ({reason}) → {s.total_score:.1f}")

            if commodity_boost_count > 0:
                final_stocks.sort(key=lambda x: x.total_score, reverse=True)
                logger.info(f"  [원자재] {commodity_boost_count}종목 부스트 → 재정렬 완료")
            elif _active_sectors:
                logger.info(f"  [원자재] 활성 섹터 {[s[1] for s in _active_sectors]} — 추천 후보에 해당 종목 없음")

            # report에 원자재 상황 저장 (텔레그램 표시용)
            report.commodity_info = {
                "relay": relay or "없음",
                "active_sectors": [s[1] for s in _active_sectors],
                "boosted_count": commodity_boost_count,
                "prices": {},
            }
            for key in ("gold", "oil", "ng", "silver", "copper"):
                c_data = commodity.get(key, {})
                if c_data:
                    report.commodity_info["prices"][key] = {
                        "price": c_data.get("price", 0),
                        "change_pct": c_data.get("change_pct", 0),
                        "direction": c_data.get("direction", "FLAT"),
                    }
    except Exception as e:
        logger.warning(f"[원자재 부스트] 실패 (무시): {e}")

    # ── 이벤트 캘린더 리스크 ──
    try:
        from data.event_calendar import get_event_risk_for_recommendation
        event_risk = get_event_risk_for_recommendation()
        report.event_risk = {
            "confluence_score": event_risk["confluence_score"],
            "risk_level": event_risk["risk_level"],
            "warning": event_risk.get("warning", ""),
            "event_count": len(event_risk.get("events", [])),
            "events_summary": [
                f"{e['name']}(D{e.get('days_to', 0):+d})"
                for e in event_risk.get("events", [])
                if e.get("impact") == "HIGH"
            ][:5],
            "seasonality": event_risk.get("seasonality", {}),
        }
        if event_risk["risk_level"] in ("HIGH", "EXTREME"):
            logger.info(f"  [이벤트] {event_risk['risk_level']} — {event_risk.get('warning', '')}")
    except Exception as e:
        logger.warning(f"[이벤트 캘린더] 실패 (무시): {e}")

    # ── Step 5d: ETF 추천 엔진 ──
    try:
        from data.etf_recommender import generate_etf_recommendations, save_etf_recommendations
        etf_recs = generate_etf_recommendations()
        if etf_recs:
            report.etf_recommendations = [r.to_dict() for r in etf_recs]
            save_etf_recommendations(etf_recs)
            logger.info(f"[Step 5d] ETF 추천: {len(etf_recs)}종목")
    except Exception as e:
        logger.warning(f"[ETF 추천] 실패 (무시): {e}")

    # ── 타임아웃 가드: Step 6 진입 전 ──
    _timeout_result = _check_pipeline_timeout("Step6(KIS검증)", report, final_stocks)
    if _timeout_result:
        return _timeout_result

    # Step 6: KIS API 가격 교차검증 (타임아웃 방어)
    KIS_VERIFY_TIMEOUT = 60  # 1분
    t0 = time.time()
    logger.info("[Step 6/6] KIS API 가격 교차검증...")
    try:
        from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout
        with ThreadPoolExecutor(max_workers=1, thread_name_prefix="kis_verify") as _kis_exec:
            _kis_future = _kis_exec.submit(_step6_kis_verify, final_stocks, market_chg)
            final_stocks = _kis_future.result(timeout=KIS_VERIFY_TIMEOUT)
    except FuturesTimeout:
        logger.warning(f"[Step 6] KIS 검증 타임아웃 ({KIS_VERIFY_TIMEOUT}s) - 기존 가격 유지")
    except Exception as e:
        logger.warning(f"[Step 6] KIS 검증 실패: {e} - 기존 가격 유지")
    logger.info(f"  → 최종 {len(final_stocks)}종목 ({time.time()-t0:.0f}s)")

    report.stocks = final_stocks
    report.tv_cluster_info = tv_cluster_info
    elapsed = time.time() - t_start
    logger.info(f"최종 추천: {len(final_stocks)}종목, 모멘텀 {len(report.momentum_stocks)}종목 (전체 {elapsed:.0f}s)")
    if elapsed > PIPELINE_TOTAL_TIMEOUT * 0.8:
        logger.warning(f"[PERF] 파이프라인 {elapsed:.0f}s — 타임아웃({PIPELINE_TOTAL_TIMEOUT}s) 80% 초과, 병목 점검 필요")

    # ── 2단계: 국적별 행동 프로파일러 + 7 SECRET 파워 ──
    try:
        from data.nationality_profiler import predict_tomorrow_flow
        top_codes = [s.code for s in report.stocks]
        top_names = {s.code: s.name for s in report.stocks}
        # price_data 전달 (VPD 계산용) — 일봉 CSV에서 직접 로드
        top_price_data = {}
        _daily_dir = Path(__file__).resolve().parent.parent / "data_store" / "daily"
        for c in top_codes:
            _dp = _daily_dir / f"{c}.csv"
            if _dp.exists():
                try:
                    import pandas as _pd_vpd
                    _df = _pd_vpd.read_csv(_dp, index_col=0, parse_dates=True)
                    if len(_df) >= 2:
                        top_price_data[c] = {
                            "current": int(_df["종가"].iloc[-1]),
                            "prev": int(_df["종가"].iloc[-2]),
                            "volume": int(_df["거래량"].iloc[-1]),
                        }
                except Exception:
                    pass
        flow_preds = predict_tomorrow_flow(
            top_codes, n_days=5, code_names=top_names,
            price_data=top_price_data,
        )
        flow_map = {p["code"]: p for p in flow_preds}

        for s in report.stocks:
            pred = flow_map.get(s.code)
            if pred:
                s.flow_signal = pred["signal"]
                s.flow_score = pred["score"]
                parts = []
                if pred["reason"] != "특이사항 없음":
                    parts.append(pred["reason"])
                if pred.get("risk"):
                    parts.append(f"⚠️{pred['risk']}")
                s.flow_detail = " | ".join(parts) if parts else "중립"
                # 7 SECRET 파워
                np_obj = pred.get("nat_power")
                if np_obj and hasattr(np_obj, "score"):
                    s.nat_power = np_obj.score
                    s.nat_power_grade = np_obj.grade
                    s.nat_power_detail = np_obj.detail

        logger.info(f"2단계 수급 프로파일링 + 7SECRET 완료: {len(flow_preds)}종목")
    except Exception as e:
        logger.warning(f"수급 프로파일링 실패 (무시): {e}")

    # 자동 저장
    try:
        save_recommendation(report)
    except Exception as e:
        logger.warning(f"추천 자동저장 실패: {e}")

    # Market Brain 합성 리포트 생성
    try:
        from data.market_brain import generate_brain_report, save_brain_report
        brain = generate_brain_report()
        save_brain_report(brain)
        logger.info(f"[Market Brain] 비중 {brain.position_size_pct}% | {brain.overall_verdict[:50]}")

        # FIX-01: BRAIN 비중 → 추천 종목 수 캡 (최소 2종목 보장)
        cap = _brain_stock_cap(brain.position_size_pct)
        if len(report.stocks) > cap:
            logger.info(
                f"[BRAIN CAP] {len(report.stocks)}종목 → {cap}종목 "
                f"(비중 {brain.position_size_pct}%)"
            )
            report.stocks = report.stocks[:cap]
            save_recommendation(report)  # 캡 적용된 리스트로 재저장
    except Exception as e:
        logger.warning(f"[Market Brain] 실패 (무시): {e}")

    # Trade Object Layer — 트레이드 설계 자동 생성
    try:
        from data.trade_object import build_trade_objects_from_report, save_trade_objects
        trade_objects = build_trade_objects_from_report(report)
        save_trade_objects(trade_objects)
        accepted = len([t for t in trade_objects if t.rr_verdict != "REJECT"])
        logger.info(f"[TradeObject] {accepted}/{len(trade_objects)} ACCEPT (R:R 통과)")
    except Exception as e:
        logger.warning(f"[TradeObject] 실패 (무시): {e}")

    # FLOWX 업로드 (Supabase short_signals)
    try:
        from data.upload_short import run_flowx_upload
        import json
        rec_path = Path(__file__).resolve().parent.parent / "data_store" / "recommendation.json"
        rec_data = json.loads(rec_path.read_text("utf-8"))
        # 국적 데이터 수집 시도
        _nat_daily = None
        try:
            from data.nationality_profiler import collect_daily_series as _cds
            _top_codes = [s.get("code") for s in rec_data.get("stocks", [])[:10]]
            if _top_codes:
                _nat_daily = _cds(_top_codes, n_days=5)
        except Exception:
            pass
        run_flowx_upload(rec_data, nat_daily_all=_nat_daily)
        logger.info("[FLOWX] 업로드 완료")
    except Exception as e:
        logger.warning(f"[FLOWX] 업로드 실패 (무시): {e}")

    # ── CTO 시그널 추적 (최종 추천 종목) ──
    try:
        from bot.trading_cto import TradingCTO
        _cto_path = Path(__file__).resolve().parent.parent / "data_store" / "cto_features.json"
        if _cto_path.exists():
            _cto = TradingCTO(config={})
            for s in report.stocks:
                top_src = s.sources[0] if isinstance(s.sources, list) and s.sources else "unknown"
                _cto.track_signal(s.code, top_src, "BUY", s.total_score, s.tp)
            logger.info(f"[CTO] {len(report.stocks)}종목 시그널 추적 기록")
    except Exception as e:
        logger.debug(f"[CTO] 시그널 추적 실패 (무시): {e}")

    return report


def _load_us_overnight_cache() -> dict:
    """A11이 생성한 us_market_overnight.json 캐시 로드 (yfinance 대체)."""
    import json
    cache_path = Path(__file__).resolve().parent.parent / "data_store" / "us_market_overnight.json"
    if not cache_path.exists():
        return {}
    try:
        data = json.loads(cache_path.read_text("utf-8"))
        today_str = datetime.now().strftime("%Y-%m-%d")
        if data.get("date", "")[:10] == today_str:
            return data
    except Exception:
        pass
    return {}


def run_us_market_check(prev_report: RecommendationReport) -> RecommendationReport:
    """Stage 2: 미국장 체크 (06:30) - 미국 지수 확인 → 추천 조정

    우선순위: us_market_overnight.json (A11 캐시) → yfinance fallback
    """
    from datetime import datetime

    report = RecommendationReport(
        stage="us_check",
        timestamp=datetime.now().strftime("%Y-%m-%d %H:%M"),
        stocks=prev_report.stocks,  # 이전 추천 유지
        relay_summary=prev_report.relay_summary,
        market_health=prev_report.market_health,
    )

    # ── 1차: A11 캐시에서 미국장 데이터 로드 ──
    us_cache = _load_us_overnight_cache()
    us_notes = []
    sp500_chg = 0.0
    vix_val = 0.0
    tnx_chg = 0.0

    if us_cache:
        logger.info("[US Check] A11 캐시 사용 (yfinance 스킵)")
        sp_chg = us_cache.get("sp500_change", 0.0)
        nq_chg = us_cache.get("nasdaq_change", 0.0)
        vix_val = us_cache.get("vix", 0.0)
        vix_chg = us_cache.get("vix_change", 0.0)
        us_notes.append(f"S&P500: {sp_chg:+.2f}%")
        us_notes.append(f"나스닥: {nq_chg:+.2f}%")
        us_notes.append(f"VIX: {vix_val:.1f} ({vix_chg:+.2f}%)")
        sp500_chg = sp_chg
        # TNX 변동
        tnx_yield = us_cache.get("us_10y_yield", 0.0)
        if tnx_yield:
            tnx_chg = 0.0  # 캐시에 전일 비교 데이터 없으면 0
    else:
        # ── 2차: yfinance fallback (타임아웃 방어) ──
        logger.info("[US Check] A11 캐시 없음 — yfinance fallback")
        try:
            import yfinance as yf
        except ImportError:
            report.us_market_note = "yfinance 미설치"
            return report

        us_indices = {
            "^GSPC": "S&P500",
            "^IXIC": "나스닥",
            "^DJI": "다우",
            "^VIX": "VIX",
        }
        for ticker, name in us_indices.items():
            try:
                data = yf.Ticker(ticker)
                hist = data.history(period="2d", timeout=8)
                if hist is not None and len(hist) >= 2:
                    prev_close = float(hist["Close"].iloc[-2])
                    last_close = float(hist["Close"].iloc[-1])
                    chg = (last_close / prev_close - 1) * 100
                    us_notes.append(f"{name}: {chg:+.2f}%")
                    if ticker == "^GSPC":
                        sp500_chg = chg
                    elif ticker == "^VIX":
                        vix_val = float(hist["Close"].iloc[-1])
            except Exception:
                us_notes.append(f"{name}: 조회실패")

        # TNX (yfinance)
        try:
            tnx = yf.Ticker("^TNX")
            tnx_hist = tnx.history(period="2d", timeout=8)
            if tnx_hist is not None and len(tnx_hist) >= 2:
                tnx_chg = float(tnx_hist["Close"].iloc[-1]) - float(tnx_hist["Close"].iloc[-2])
        except Exception:
            pass

    report.us_market_note = " | ".join(us_notes) if us_notes else "미국장 데이터 없음"

    # VIX 급등 체크
    if vix_val > 30:
        report.warning = f"VIX {vix_val:.1f} - 공포 급등! 매수 규모 축소 권고"
    elif vix_val > 25:
        report.warning = f"VIX {vix_val:.1f} - 경계, 1/2 규모 매수 고려"

    # S&P500 -2% 이상 폭락 시 경고
    if sp500_chg < -2.0:
        report.warning = f"S&P500 {sp500_chg:+.1f}% 폭락! 매수 보류 권고"

    # 3색 신호등 판정
    if sp500_chg < -1.0 and tnx_chg > 0.05:
        cross_regime = "DIVERGENCE"
        cross_detail = (
            f"S&P {sp500_chg:+.1f}% + TNX {tnx_chg:+.2f}%p "
            f"= 채권 자경단 (주식하락+금리상승)"
        )
        report.warning = (
            f"{report.warning} | " if report.warning else ""
        ) + f"NIGHTWATCH: DIVERGENCE - 절대 진입 금지"
    elif sp500_chg < -1.0 and tnx_chg <= 0:
        cross_regime = "CORRECTION"
        cross_detail = (
            f"S&P {sp500_chg:+.1f}% + TNX {tnx_chg:+.2f}%p "
            f"= 일반 리스크오프 (관망)"
        )
        report.warning = (
            f"{report.warning} | " if report.warning else ""
        ) + f"NIGHTWATCH: CORRECTION - 관망 권고"
    else:
        cross_regime = "NORMAL"
        cross_detail = f"S&P {sp500_chg:+.1f}% + TNX {tnx_chg:+.2f}%p = 정상"

    report.cross_regime = cross_regime
    report.cross_regime_detail = cross_detail
    logger.info(f"[NIGHTWATCH] {cross_regime}: {cross_detail}")

    # 위기 ETF 시그널 생성 (인버스/레버리지 추천)
    try:
        from strategies.crisis_etf_signal import generate_signal
        etf_sig = generate_signal()
        report.etf_signal = etf_sig.to_dict()
        if etf_sig.signal != "HOLD":
            etf = etf_sig.recommended_etf
            etf_name = etf.get("name", "") if etf else ""
            etf_code = etf.get("code", "") if etf else ""
            report.warning = (
                f"{report.warning} | " if report.warning else ""
            ) + f"ETF시그널: {etf_sig.signal}({etf_sig.confidence}) → {etf_name}({etf_code})"
    except Exception as e:
        logger.warning(f"ETF 시그널 생성 실패: {e}")

    # 자동 저장
    try:
        save_recommendation(report)
    except Exception as e:
        logger.warning(f"추천 자동저장 실패: {e}")

    return report


def run_morning_confirmation(prev_report: RecommendationReport) -> RecommendationReport:
    """Stage 3: 아침 최종 확인 (08:50)

    이전 추천을 기반으로 최종 확인만 수행.
    실제 매수는 기존 job_morning_scan (09:20)에서 처리.
    """
    from datetime import datetime

    report = RecommendationReport(
        stage="morning",
        timestamp=datetime.now().strftime("%Y-%m-%d %H:%M"),
        stocks=prev_report.stocks,
        relay_summary=prev_report.relay_summary,
        market_health=prev_report.market_health,
        us_market_note=prev_report.us_market_note,
        warning=prev_report.warning,
    )

    # 위기 모드 체크
    try:
        from data.market_health import is_crisis_mode
        crisis, reason = is_crisis_mode()
        if crisis:
            report.warning = f"위기모드 활성: {reason} - 전종목 매수 중단"
            report.stocks = []
    except Exception:
        pass

    return report


# ═══════════════════════════════════════
#  텔레그램 포맷
# ═══════════════════════════════════════

def _get_market_type_tag(code: str) -> str:
    """종목 마켓 타입 태그 — [NXT] 또는 [KRX전용]."""
    nxt_path = Path(__file__).resolve().parent.parent / "data_store" / "nxt_eligible.json"
    if not hasattr(_get_market_type_tag, "_cache"):
        _get_market_type_tag._cache = set()
        if nxt_path.exists():
            try:
                data = json.loads(nxt_path.read_text(encoding="utf-8"))
                _get_market_type_tag._cache = set(data.get("stocks", {}).keys())
            except Exception:
                pass
    return "[NXT]" if code in _get_market_type_tag._cache else "[KRX전용]"


def format_recommendation(report: RecommendationReport, max_budget: int = 0) -> str:
    """텔레그램용 추천 리포트 포맷 (v2: 상대강도 + 페널티 표시)

    max_budget=0이면 매수 수량 표시 안 함 (실제 금액은 장 시작 시 잔고 기반 계산)
    """
    stage_emoji = {"evening": "🌙", "us_check": "🇺🇸", "morning": "🌅"}
    stage_label = {"evening": "저녁 분석", "us_check": "미국장 체크", "morning": "최종 확인"}

    emoji = stage_emoji.get(report.stage, "📊")
    label = stage_label.get(report.stage, report.stage)

    lines = [
        f"{emoji} 내일 추천 종목 [{label}]",
        f"   {report.timestamp}",
        f"=" * 32,
    ]

    if report.market_health:
        health_emoji = {"NORMAL": "🟢", "WARNING": "🟡", "CRITICAL": "🔴"}.get(
            report.market_health, "⚪"
        )
        lines.append(f"{health_emoji} 시장건전성: {report.market_health}")

    # 시장 등락률 표시
    if report.market_change != 0:
        mkt_icon = "📈" if report.market_change > 0 else "📉"
        lines.append(f"{mkt_icon} 시장: KOSPI {report.market_change:+.1f}%")

    if report.relay_summary:
        lines.append(f"🔄 릴레이: {report.relay_summary}")

    if report.us_market_note:
        lines.append(f"🇺🇸 미국: {report.us_market_note}")

    # 원자재 릴레이 상황
    if report.commodity_info and report.commodity_info.get("prices"):
        ci = report.commodity_info
        prices = ci["prices"]
        _labels = {"gold": "금", "silver": "은", "copper": "구리", "oil": "유", "ng": "NG"}
        _icons = {"UP": "▲", "DOWN": "▼", "FLAT": "─"}
        price_parts = []
        for key in ("gold", "silver", "copper", "oil", "ng"):
            if key in prices:
                p = prices[key]
                icon = _icons.get(p["direction"], "─")
                price_parts.append(f"{_labels[key]}{icon}{p['change_pct']:+.1f}%")
        if price_parts:
            lines.append(f"⛏️ 원자재: {' | '.join(price_parts)}")
        relay = ci.get("relay", "없음")
        active = ci.get("active_sectors", [])
        if relay != "없음" or active:
            relay_str = f"릴레이:{relay}" if relay != "없음" else ""
            active_str = " ".join(active) if active else ""
            combined = " | ".join(filter(None, [relay_str, active_str]))
            boosted = ci.get("boosted_count", 0)
            if boosted > 0:
                combined += f" → {boosted}종목 부스트"
            lines.append(f"   {combined}")

    # 이벤트 캘린더 리스크
    if report.event_risk:
        er = report.event_risk
        risk_lvl = er.get("risk_level", "")
        _risk_icon = {"EXTREME": "💀", "HIGH": "🔴", "MEDIUM": "🟡", "LOW": "🟢"}.get(risk_lvl, "")
        confluence = er.get("confluence_score", 0)
        events_summary = er.get("events_summary", [])
        if events_summary or risk_lvl in ("HIGH", "EXTREME"):
            lines.append(f"📅 이벤트: {_risk_icon}{risk_lvl} (c:{confluence:.0f}) {' '.join(events_summary)}")
        season = er.get("seasonality", {})
        if season and season.get("kr_bias"):
            _bias_icon = {"bullish": "📈", "bearish": "📉", "volatile": "⚡", "neutral": "➡️"}.get(season["kr_bias"], "")
            lines.append(f"   시즌: {_bias_icon}{season['kr_bias'].upper()} ({season.get('kr_avg', '')})")

    if report.warning:
        lines.append(f"\n⚠️ {report.warning}")

    # TV 클러스터 알림
    if report.tv_cluster_info:
        for cl in report.tv_cluster_info:
            cl_type = "그룹" if cl["type"] == "group" else "섹터"
            members_str = " | ".join(
                f"{m[0]} TV {m[2]:.1f}x" for m in cl["members"][:5]
            )
            lines.append(
                f"🔥 {cl_type} TV 클러스터: [{cl['name']}] {cl['count']}종목 동시 거래대금 폭발!"
            )
            lines.append(f"   {members_str}")

    lines.append("")

    if not report.stocks:
        lines.append("추천 종목 없음")

    conf_emoji = {"HIGH": "🔴", "MED": "🟡", "LOW": "⚪"}

    for i, s in enumerate(report.stocks[:5], 1):
        ce = conf_emoji.get(s.confidence, "⚪")
        shares = max_budget // s.close if s.close > 0 else 0
        buy_total = shares * s.close if shares > 0 else 0

        # 상대강도 표시
        rs_icon = "💪" if s.relative_str > 2 else ("⚡" if s.relative_str > 0 else "")
        # 마켓 타입 (NXT/KRX전용)
        _mt = _get_market_type_tag(s.code)
        lines.append(
            f"{ce} {i}. {s.name}({s.code}) {_mt} [{s.confidence}] "
            f"총점:{s.total_score:.0f} {rs_icon}"
        )
        lines.append(
            f"   현재: {s.close:,}원 | 등락: {s.today_chg:+.1f}% | RS: {s.relative_str:+.1f}%"
        )
        lines.append(
            f"   SL: {s.sl:,} → TP: {s.tp:,}"
        )
        # 피보나치 레벨 표시
        if getattr(s, "fib_position", ""):
            _fib_parts = [f"📐 {s.fib_position}"]
            if s.fib_upside_pct > 0:
                _fib_parts.append(f"상방+{s.fib_upside_pct:.1f}%")
            if s.fib_downside_pct > 0:
                _fib_parts.append(f"하방-{s.fib_downside_pct:.1f}%")
            lines.append(f"   {' | '.join(_fib_parts)}")

        # 점수 분해 (투명하게)
        score_parts = []
        if s.relay_score > 0:
            score_parts.append(f"릴{s.relay_score:.0f}")
        if s.premove_score > 0:
            score_parts.append(f"사전{s.premove_score:.0f}")
        if s.tech_score > 0:
            score_parts.append(f"기술{s.tech_score:.0f}")
        if s.cross_count >= 2:
            score_parts.append(f"교차+{s.cross_count * 10}")
        if s.nationality_score != 0:
            score_parts.append(f"국적{s.nationality_score:+.0f}")
        # sources에서 추가 가산/감산 파싱 (rotation, opp, shock, or_bias, eq, gap)
        for src in s.sources:
            if src.startswith("rotation:"):
                # rotation:hot_early(방산/국방) → +15
                rot_map = {"hot_early": 15, "staging": 8, "hot_mid": 5, "reversal_exit": -20}
                for key, val in rot_map.items():
                    if f"rotation:{key}" in src:
                        score_parts.append(f"로테{val:+d}")
                        break
            elif src.startswith("opp:"):
                val = src.split("(")[1].rstrip(")")
                score_parts.append(f"기회{val}")
            elif src.startswith("shock:"):
                val = src.split("(")[1].rstrip(")")
                score_parts.append(f"충격{val}")
            elif src.startswith("or_bias:"):
                val = src.split("(")[1].rstrip(")")
                score_parts.append(f"OR{val}")
            elif src.startswith("eq_level:"):
                val = src.split("(")[1].rstrip(")")
                score_parts.append(f"EQ{val}")
            elif src.startswith("gap:"):
                val = src.split("(")[1].rstrip(")")
                score_parts.append(f"갭{val}")
            elif src.startswith("commodity:"):
                reason = src.split(":")[1]
                score_parts.append(f"원자재+10({reason})")

        # 페널티 표시
        penalties = []
        if s.news_penalty < 0:
            penalties.append(f"뉴스{s.news_penalty:.0f}")
        if s.obv_penalty < 0:
            penalties.append(f"OBV{s.obv_penalty:.0f}")
        if s.relative_penalty < 0:
            penalties.append(f"약세{s.relative_penalty:.0f}")

        score_line = "   + ".join(score_parts) if score_parts else "가산없음"
        if penalties:
            score_line += " | " + " ".join(penalties)
        lines.append(f"   {score_line}")

        lines.append(f"   기술: {s.tech_detail}")
        if s.nationality_detail:
            lines.append(f"   🌍 {s.nationality_detail}")
        # 2단계: 국적별 행동 시그널
        flow_sig = getattr(s, "flow_signal", "")
        flow_sc = getattr(s, "flow_score", 0)
        flow_det = getattr(s, "flow_detail", "")
        if flow_sig:
            flow_emoji = {
                "STRONG_BUY": "🔴", "BUY": "🟠",
                "NEUTRAL": "⚪", "CAUTION": "🟡", "SELL": "🔵",
            }.get(flow_sig, "⚪")
            flow_kr = {
                "STRONG_BUY": "강력매수", "BUY": "매수",
                "NEUTRAL": "중립", "CAUTION": "주의", "SELL": "매도",
            }.get(flow_sig, flow_sig)
            lines.append(f"   {flow_emoji} 수급예측: {flow_kr}({flow_sc:+.0f})")
            if flow_det:
                lines.append(f"   └ {flow_det}")
        # 7 SECRET 파워 표시
        np_grade = getattr(s, "nat_power_grade", "")
        np_score = getattr(s, "nat_power", 0)
        np_detail = getattr(s, "nat_power_detail", "")
        if np_grade and np_grade != "NEUTRAL":
            grade_emoji = {"POWER_BUY": "💥", "BUY": "🟢", "CAUTION": "🟡", "DANGER": "🔴"}.get(np_grade, "⚪")
            lines.append(f"   {grade_emoji} 7SECRET: {np_grade}({np_score:+.1f})")
            if np_detail:
                lines.append(f"   └ {np_detail}")
        if shares > 0:
            lines.append(f"   매수: {shares}주 = {buy_total:,}원")
        if s.sources:
            lines.append(f"   출처: {' + '.join(s.sources[:4])}")
        lines.append("")

    # ── ETF 추천 섹션 ──
    if report.etf_recommendations:
        from data.etf_recommender import format_etf_recommendations, RecommendedETF
        etf_recs = []
        for d in report.etf_recommendations:
            etf_recs.append(RecommendedETF(**{
                k: v for k, v in d.items()
                if k in RecommendedETF.__dataclass_fields__
            }))
        etf_text = format_etf_recommendations(etf_recs)
        if etf_text:
            lines.append("=" * 32)
            lines.append(etf_text)

    # ── 모멘텀 종목 섹션 ──
    if report.momentum_stocks:
        lines.append("=" * 32)
        lines.append(f"🚀 소형주 모멘텀 ({len(report.momentum_stocks)}종목)")
        lines.append("")
        for i, m in enumerate(report.momentum_stocks[:5], 1):
            theme = m.get("theme") or m.get("theme_detail") or "미분류"
            score = m.get("momentum_score", 0)
            vol = m.get("volume_spike", 0)
            chg5d = m.get("price_change_5d", 0)
            entry = m.get("entry", 0)
            sl = m.get("sl", 0)
            tp = m.get("tp", 0)
            cap = m.get("cap_억", 0)
            lines.append(
                f"⚡ {i}. {m.get('name','')}({m.get('code','')}) "
                f"[{theme}] {score:.0f}점"
            )
            lines.append(
                f"   거래량 {vol:.1f}배 | 5일 {chg5d:+.1f}% | 시총 {cap}억"
            )
            lines.append(f"   SL: {sl:,} → TP: {tp:,}")
            lines.append("")

    # ── 전쟁→재건 릴레이 종목 섹션 ──
    if report.war_relay_stocks:
        lines.append("=" * 32)
        lines.append(f"🎯 전쟁→재건 릴레이 ({len(report.war_relay_stocks)}종목)")
        lines.append("")
        for i, s in enumerate(report.war_relay_stocks[:8], 1):
            tier = s.get("tier", "")
            entry = s.get("entry", 0)
            entry_agg = s.get("entry_aggressive", 0)
            sl = s.get("sl", 0)
            tp1 = s.get("tp1", 0)
            close = s.get("close", 0)
            rr = s.get("rr", 0)
            gap_pct = ((entry - close) / close * 100) if close > 0 else 0
            lines.append(
                f"🎯 {i}. {s.get('name','')}({s.get('code','')}) [{tier}]"
            )
            lines.append(
                f"   현재: {close:,} | 진입: {entry:,} ({gap_pct:+.1f}%)"
            )
            lines.append(
                f"   SL: {sl:,} → TP: {tp1:,} | R:R 1:{rr}"
            )
            if s.get("reason"):
                lines.append(f"   {s['reason']}")
            lines.append("")

    # ── 금요일 주말 리스크 안내 ──
    from datetime import datetime as _dt
    if _dt.now().weekday() == 4:  # 금요일
        lines.append("=" * 32)
        lines.append(
            "📢 금요일 추천 안내: 주말 동안 해외 이벤트·지정학 "
            "리스크가 반영되지 않습니다. 월요일 갭다운 가능성을 "
            "감안하여 포지션 규모에 유의하세요."
        )

    return "\n".join(lines)


# ═══════════════════════════════════════
#  저장/로드 (stage간 데이터 전달)
# ═══════════════════════════════════════

_STORE_PATH = None

def _get_store_path():
    global _STORE_PATH
    if _STORE_PATH is None:
        from pathlib import Path
        _STORE_PATH = Path(__file__).resolve().parent.parent / "data_store" / "recommendation.json"
    return _STORE_PATH


def _calc_grade(total_score: float, confidence: str, nat_power_grade: str) -> str:
    """total_score → 등급 (AAA~F) — upload_short._score_to_grade와 동일"""
    if total_score >= 85 and confidence == "HIGH" and nat_power_grade in ("POWER_BUY", "BUY"):
        return "AAA"
    elif total_score >= 75 and confidence == "HIGH":
        return "AA"
    elif total_score >= 65 and confidence == "HIGH":
        return "A"
    elif total_score >= 55:
        return "BBB"
    elif total_score >= 45:
        return "BB"
    elif total_score >= 35:
        return "B"
    elif total_score >= 25:
        return "C"
    elif total_score >= 15:
        return "D"
    else:
        return "F"


def _calc_signal_type(grade: str, nat_detail: str, tv_ratio: float,
                      tv_pattern: str = "NORMAL") -> str:
    """grade → signal_type (FORCE_BUY/BUY/WATCH/AVOID) — upload_short와 동일"""
    inst_support = "기OK" in nat_detail or "기+" in nat_detail
    # QA FORCE_BUY 제거 (5/9: QA 80+ 적중 0~9%)
    if grade in ("AAA", "AA", "A") and inst_support and tv_ratio >= 1.5:
        return "FORCE_BUY"
    elif grade in ("AAA", "AA", "A", "BBB") and (inst_support or tv_ratio >= 1.3):
        return "BUY"
    elif grade in ("AAA", "AA", "A", "BBB", "BB"):
        return "WATCH"
    else:
        return "AVOID"


def save_recommendation(report: RecommendationReport):
    """추천 리포트 저장 (stage간 전달용)"""
    import json
    path = _get_store_path()
    path.parent.mkdir(parents=True, exist_ok=True)

    stock_list = []
    for s in report.stocks:
        _grade = _calc_grade(
            s.total_score, s.confidence,
            getattr(s, "nat_power_grade", ""),
        )
        _sig = _calc_signal_type(
            _grade, s.nationality_detail,
            getattr(s, "tv_ratio", 1.0),
            getattr(s, "tv_pattern", "NORMAL"),
        )
        stock_list.append({
            "code": s.code, "name": s.name, "close": s.close,
            "total_score": s.total_score, "confidence": s.confidence,
            "grade": _grade, "signal_type": _sig,
            "entry": s.entry, "sl": s.sl, "tp": s.tp,
            "sl_source": s.sl_source,
            "tech_detail": s.tech_detail, "news_detail": s.news_detail,
            "sources": s.sources, "cross_count": s.cross_count,
            "relay_score": s.relay_score, "premove_score": s.premove_score,
            "tech_score": s.tech_score, "news_score": s.news_score,
            "nationality_score": s.nationality_score,
            "nationality_detail": s.nationality_detail,
            "flow_signal": getattr(s, "flow_signal", ""),
            "flow_score": getattr(s, "flow_score", 0.0),
            "flow_detail": getattr(s, "flow_detail", ""),
            "nat_power": getattr(s, "nat_power", 0.0),
            "nat_power_grade": getattr(s, "nat_power_grade", ""),
            "nat_power_detail": getattr(s, "nat_power_detail", ""),
            "news_penalty": s.news_penalty,
            "obv_penalty": s.obv_penalty,
            "relative_penalty": s.relative_penalty,
            "today_chg": s.today_chg,
            "relative_str": s.relative_str,
            "regime": getattr(s, "regime", "NORMAL"),
            "regime_score": getattr(s, "regime_score", 0.0),
            "regime_detail": getattr(s, "regime_detail", ""),
            "tv_ratio": getattr(s, "tv_ratio", 1.0),
            "tv_pattern": getattr(s, "tv_pattern", "NORMAL"),
            "tv_score": getattr(s, "tv_score", 0.0),
            "fib_adj": getattr(s, "fib_adj", 0.0),
            "fib_position": getattr(s, "fib_position", ""),
            "fib_upside_pct": getattr(s, "fib_upside_pct", 0.0),
            "fib_downside_pct": getattr(s, "fib_downside_pct", 0.0),
            "sl_fib": getattr(s, "sl_fib", 0),
            "tp_fib": getattr(s, "tp_fib", 0),
            # 명분게이트·끼 (4단 shadow 관측 — 6/1~ recommendation.json에 저장)
            "thesis_pass": getattr(s, "thesis_pass", True),
            "thesis_strong": getattr(s, "thesis_strong", []),
            "thesis_buckets": getattr(s, "thesis_buckets", {}),
            "kki_score": getattr(s, "kki_score", 0.0),
            "kki_grade": getattr(s, "kki_grade", ""),
        })

    data = {
        "stage": report.stage,
        "timestamp": report.timestamp,
        "market_health": report.market_health,
        "market_change": report.market_change,
        "us_market_note": report.us_market_note,
        "relay_summary": report.relay_summary,
        "warning": report.warning,
        "cross_regime": report.cross_regime,
        "cross_regime_detail": report.cross_regime_detail,
        "stocks": stock_list,
        "momentum_stocks": report.momentum_stocks,  # 소형주 급등 후보 (dict list)
        "war_relay_stocks": report.war_relay_stocks,  # 전쟁→재건 릴레이 종목
        "rotation_signal": report.rotation_signal,  # 섹터 로테이션 시그널
        "rotation_detail": report.rotation_detail,  # 섹터별 로테이션 상세
        "etf_signal": report.etf_signal,  # 위기 ETF 시그널
        "tv_cluster_info": report.tv_cluster_info,  # TV 클러스터
        "commodity_info": report.commodity_info,  # 원자재 릴레이 상황
        "event_risk": report.event_risk,  # 이벤트 캘린더 리스크
        "etf_recommendations": report.etf_recommendations,  # ETF 추천 (Phase 2)
        "sector_momentum": getattr(report, "_sector_momentum", {}),  # 섹터 모멘텀
    }
    tmp = path.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    tmp.replace(path)
    logger.info(
        f"추천 저장: {path} ({report.stage}, {len(report.stocks)}종목, "
        f"모멘텀 {len(report.momentum_stocks)}종목, "
        f"전쟁릴레이 {len(report.war_relay_stocks)}종목)"
    )


def load_recommendation() -> Optional[RecommendationReport]:
    """저장된 추천 리포트 로드"""
    import json
    path = _get_store_path()
    if not path.exists():
        return None

    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        # 형식 검증: 다른 시스템(quantum 등)이 덮어쓴 경우 감지
        if "recommendations" not in data and "stocks" not in data:
            logger.warning(
                f"recommendation.json 형식 불일치 — 다른 시스템이 덮어쓴 것으로 판단. "
                f"keys={list(data.keys())[:5]}"
            )
            return None

        # "recommendations" 키 사용 시 "stocks"로 변환 (호환)
        if "recommendations" in data and "stocks" not in data:
            data["stocks"] = data["recommendations"]

        report = RecommendationReport(
            stage=data.get("stage", ""),
            timestamp=data.get("timestamp", ""),
            market_health=data.get("market_health", ""),
            market_change=data.get("market_change", 0),
            us_market_note=data.get("us_market_note", ""),
            relay_summary=data.get("relay_summary", ""),
            warning=data.get("warning", ""),
            cross_regime=data.get("cross_regime", ""),
            cross_regime_detail=data.get("cross_regime_detail", ""),
        )
        for sd in data.get("stocks", []):
            report.stocks.append(RecommendedStock(
                code=sd["code"], name=sd["name"], close=sd.get("close", 0),
                total_score=sd.get("total_score", 0),
                confidence=sd.get("confidence", "LOW"),
                entry=sd.get("entry", 0), sl=sd.get("sl", 0), tp=sd.get("tp", 0),
                sl_source=sd.get("sl_source", ""),
                tech_detail=sd.get("tech_detail", ""),
                news_detail=sd.get("news_detail", ""),
                sources=sd.get("sources", []),
                cross_count=sd.get("cross_count", 0),
                relay_score=sd.get("relay_score", 0),
                premove_score=sd.get("premove_score", 0),
                tech_score=sd.get("tech_score", 0),
                news_score=sd.get("news_score", 0),
                nationality_score=sd.get("nationality_score", 0),
                nationality_detail=sd.get("nationality_detail", ""),
                flow_signal=sd.get("flow_signal", ""),
                flow_score=sd.get("flow_score", 0),
                flow_detail=sd.get("flow_detail", ""),
                nat_power=sd.get("nat_power", 0),
                nat_power_grade=sd.get("nat_power_grade", ""),
                nat_power_detail=sd.get("nat_power_detail", ""),
                news_penalty=sd.get("news_penalty", 0),
                obv_penalty=sd.get("obv_penalty", 0),
                relative_penalty=sd.get("relative_penalty", 0),
                today_chg=sd.get("today_chg", 0),
                relative_str=sd.get("relative_str", 0),
                regime=sd.get("regime", "NORMAL"),
                regime_score=sd.get("regime_score", 0.0),
                regime_detail=sd.get("regime_detail", ""),
                tv_ratio=sd.get("tv_ratio", 1.0),
                tv_pattern=sd.get("tv_pattern", "NORMAL"),
                tv_score=sd.get("tv_score", 0.0),
            ))
        # 소형주 모멘텀 후보 로드
        report.momentum_stocks = data.get("momentum_stocks", [])
        # 전쟁→재건 릴레이 종목 로드
        report.war_relay_stocks = data.get("war_relay_stocks", [])
        # 섹터 로테이션 + ETF 시그널 복원
        report.rotation_signal = data.get("rotation_signal", "")
        report.rotation_detail = data.get("rotation_detail", [])
        report.etf_signal = data.get("etf_signal", {})
        report.tv_cluster_info = data.get("tv_cluster_info", [])
        report.commodity_info = data.get("commodity_info", {})
        report.event_risk = data.get("event_risk", {})
        report.etf_recommendations = data.get("etf_recommendations", [])
        return report
    except Exception as e:
        logger.error(f"추천 로드 실패: {e}")
        return None


# ═══════════════════════════════════════
#  전쟁 모드: 낙폭 대형주 랭킹
# ═══════════════════════════════════════

def _get_pre_war_prices(codes: list[str], pre_war_date: str = "2026-02-26") -> dict:
    """전쟁 전 종가 조회 (CSV 일봉 기반)

    Returns: {code: {"pre_war": int, "war_low": int, "current": int}}
    """
    import pandas as pd
    from pathlib import Path

    base = Path(__file__).resolve().parent.parent / "data_store" / "daily"
    results = {}

    for code in codes:
        csv_path = base / f"{code}.csv"
        if not csv_path.exists():
            continue
        try:
            df = pd.read_csv(csv_path, encoding="utf-8")
            df.columns = ["date", "open", "high", "low", "close", "volume", "change"]
            df["date"] = pd.to_datetime(df["date"])
            df = df.sort_values("date")

            # 전쟁 전 종가 (2/24~2/26 중 마지막)
            feb = df[(df["date"] >= "2026-02-24") & (df["date"] <= pre_war_date)]
            pre_war = int(feb.iloc[-1]["close"]) if len(feb) > 0 else 0

            # 전쟁 저점 (3/3~3/5)
            war = df[(df["date"] >= "2026-03-03") & (df["date"] <= "2026-03-05")]
            war_low = int(war["low"].min()) if len(war) > 0 else 0

            # 현재가 (마지막 행)
            current = int(df.iloc[-1]["close"])

            if pre_war > 0 and current > 0:
                results[code] = {
                    "pre_war": pre_war,
                    "war_low": war_low,
                    "current": current,
                }
        except Exception as e:
            logger.debug(f"전쟁전 가격 조회 실패 {code}: {e}")

    return results


def _calc_tech_recovery_scores(codes: list[str]) -> dict:
    """전쟁 모드용 기술 회복 시그널 배치 계산

    각 종목별 0~20점:
      +5: RSI 과매도 탈출 (직전5봉 중 RSI<35 → 현재>40)
      +5: MACD 히스토그램 음→양 전환 (최근 3봉)
      +5: 거래량비율 2x+ (20일 평균 대비)
      +5: 5일 수익률 (후보군 내 상대평가)

    Returns: {code: {"score": float, "rsi": float, "chg_5d": float, "detail": str}}
    """
    import pandas as pd
    import numpy as np
    from pathlib import Path

    base = Path(__file__).resolve().parent.parent / "data_store" / "daily"
    raw_results = {}

    for code in codes:
        csv_path = base / f"{code}.csv"
        if not csv_path.exists():
            continue
        try:
            df = pd.read_csv(csv_path, encoding="utf-8")
            df.columns = ["date", "open", "high", "low", "close", "volume", "change"]
            if len(df) < 25:
                continue

            close = df["close"].astype(float)
            volume = df["volume"].astype(float)

            score = 0.0
            details = []

            # --- RSI 과매도 탈출 ---
            try:
                from data.swing_indicators import calc_rsi
                rsi_series = calc_rsi(close, 14)
                rsi_now = float(rsi_series.iloc[-1])
                rsi_recent_min = float(rsi_series.iloc[-6:-1].min()) if len(rsi_series) >= 6 else rsi_now
                if rsi_recent_min < 35 and rsi_now > 40:
                    score += 5.0
                    details.append(f"RSI탈출({rsi_recent_min:.0f}→{rsi_now:.0f})")
                elif rsi_now < 35:
                    score += 2.0  # 아직 과매도지만 반등 대기
                    details.append(f"RSI과매도({rsi_now:.0f})")
            except Exception:
                rsi_now = 50.0

            # --- MACD 히스토그램 음→양 전환 ---
            try:
                from data.swing_indicators import calc_histogram
                hist_df = calc_histogram(close)
                hist = hist_df["histogram"]
                if len(hist) >= 3:
                    h_prev2 = float(hist.iloc[-3])
                    h_prev1 = float(hist.iloc[-2])
                    h_now = float(hist.iloc[-1])
                    if h_prev2 < 0 and h_now > 0:
                        score += 5.0
                        details.append("MACD전환")
                    elif h_prev1 < 0 and h_now > 0:
                        score += 4.0
                        details.append("MACD전환중")
                    elif h_now > h_prev1 > h_prev2:
                        score += 2.0
                        details.append("MACD개선")
            except Exception:
                pass

            # --- 거래량 급증 (20일 평균 대비) ---
            if len(volume) >= 21:
                vol_avg_20 = float(volume.iloc[-21:-1].mean())
                vol_today = float(volume.iloc[-1])
                if vol_avg_20 > 0:
                    vol_ratio = vol_today / vol_avg_20
                    if vol_ratio >= 2.0:
                        score += 5.0
                        details.append(f"거래량{vol_ratio:.1f}x")
                    elif vol_ratio >= 1.5:
                        score += 2.5
                        details.append(f"거래량{vol_ratio:.1f}x")

            # --- 5일 수익률 (나중에 상대평가) ---
            if len(close) >= 6:
                chg_5d = (float(close.iloc[-1]) / float(close.iloc[-6]) - 1) * 100
            else:
                chg_5d = 0.0

            raw_results[code] = {
                "score": score,  # 0~15 (5일 수익률 5점은 상대평가)
                "rsi": rsi_now,
                "chg_5d": chg_5d,
                "detail": "+".join(details) if details else "",
            }
        except Exception as e:
            logger.debug(f"기술회복 계산 실패 {code}: {e}")

    # --- 5일 수익률 상대평가 (상위 50% → +5점) ---
    if raw_results:
        chg_values = [v["chg_5d"] for v in raw_results.values()]
        median_chg = float(np.median(chg_values)) if chg_values else 0.0
        for code, v in raw_results.items():
            if v["chg_5d"] > median_chg:
                bonus = min(5.0, (v["chg_5d"] - median_chg) * 1.0)  # 최대 5점
                v["score"] += bonus
                if bonus >= 3:
                    v["detail"] += ("|" if v["detail"] else "") + f"5D+{v['chg_5d']:.1f}%"
            v["score"] = min(20.0, v["score"])  # 캡 20점

    return raw_results


def run_war_mode_recommendation() -> RecommendationReport:
    """전쟁 모드: 낙폭 대형주 + 컨센서스 랭킹

    평시 기술분석 대신 단순하고 확실한 전략:
    1. 시총 TOP 50 로드
    2. 전쟁 전 가격 vs 현재가 → 회복 업사이드
    3. 컨센서스 목표가 → 컨센 업사이드
    4. 수급 점수 (외국인/기관 순매수)
    5. 종합 스코어 → TOP 8 추천
    """
    import json
    import time
    from datetime import datetime
    from pathlib import Path

    logger.info("=" * 50)
    logger.info("전쟁 모드 추천 파이프라인 시작 (낙폭 대형주)")
    logger.info("=" * 50)
    t_start = time.time()

    report = RecommendationReport(
        stage="evening",
        timestamp=datetime.now().strftime("%Y-%m-%d %H:%M"),
    )

    # ── 0) 시장 건전성 ──
    try:
        from data.market_health import diagnose
        health = diagnose()
        report.market_health = health.alert_level.upper()
        if hasattr(health, "regime"):
            regime = health.regime
        else:
            regime = "SHOCK"
    except Exception as e:
        report.market_health = "UNKNOWN"
        regime = "SHOCK"
        logger.warning(f"시장건전성 체크 실패: {e}")

    report.warning = f"전쟁 모드 활성 (regime={regime})"

    # ── 1) 시총 TOP 50 로드 ──
    t0 = time.time()
    uni_path = Path(__file__).resolve().parent.parent / "data_store" / "universe.json"
    with open(uni_path, "r", encoding="utf-8") as f:
        universe = json.load(f)

    # 우선주 제외, 시총 정렬
    stocks_by_cap = sorted(
        [(code, info) for code, info in universe.items()
         if not info.get("name", "").endswith("우")],
        key=lambda x: x[1].get("cap_억", 0),
        reverse=True,
    )[:50]

    top_codes = [code for code, _ in stocks_by_cap]
    code_to_info = {code: info for code, info in stocks_by_cap}
    logger.info(f"[Step 1] 시총 TOP 50 로드 ({time.time()-t0:.0f}s)")

    # ── 2) 전쟁 전 가격 조회 ──
    t0 = time.time()
    price_data = _get_pre_war_prices(top_codes)
    logger.info(f"[Step 2] 전쟁전 가격: {len(price_data)}/{len(top_codes)}종목 ({time.time()-t0:.0f}s)")

    # ── 3) 컨센서스 크롤링 ──
    t0 = time.time()
    try:
        from data.consensus_scraper import fetch_consensus_batch
        consensus_data = fetch_consensus_batch(top_codes, delay=0.3, cache_hours=12)
    except Exception as e:
        logger.warning(f"컨센서스 크롤링 실패: {e}")
        consensus_data = {}
    logger.info(f"[Step 3] 컨센서스: {len(consensus_data)}/{len(top_codes)}종목 ({time.time()-t0:.0f}s)")

    # ── 4) 수급 점수 ──
    t0 = time.time()
    supply_scores = {}
    try:
        from data.supply_naver import score_supply_batch
        close_prices = {}
        for code in top_codes:
            _pdi = price_data.get(code)
            if _pdi:
                close_prices[code] = _pdi["current"]
        supply_scores = score_supply_batch(top_codes, close_prices=close_prices)
    except Exception as e:
        logger.warning(f"수급 점수 실패: {e}")
    scored = sum(1 for v in supply_scores.values() if isinstance(v, tuple) and v[0] != 0)
    logger.info(f"[Step 4] 수급: {scored}/{len(top_codes)}종목 ({time.time()-t0:.0f}s)")

    # ── 4.5) 기술 회복 시그널 스캔 ──
    t0 = time.time()
    tech_recovery_scores = _calc_tech_recovery_scores(top_codes)
    tech_count = sum(1 for v in tech_recovery_scores.values() if v["score"] > 0)
    logger.info(f"[Step 4.5] 기술회복: {tech_count}/{len(top_codes)}종목 시그널 ({time.time()-t0:.0f}s)")

    # ── 4.7) 장중이면 실시간 가격으로 업데이트 ──
    now_h = datetime.now().hour
    now_m = datetime.now().minute
    is_market_hours = (9 <= now_h < 15) or (now_h == 15 and now_m <= 30)
    if is_market_hours:
        try:
            from bot.kis_trader import KISTrader
            trader = KISTrader()
            live_updated = 0
            for code in top_codes:
                if code in price_data:
                    p = trader.fetch_price(code)
                    if p and p.get("success") and p.get("current_price", 0) > 0:
                        price_data[code]["current"] = p["current_price"]
                        live_updated += 1
                    time.sleep(0.15)
            logger.info(f"[Step 4.7] 실시간 가격: {live_updated}종목 업데이트")
        except Exception as e:
            logger.warning(f"실시간 가격 실패 (CSV 종가 유지): {e}")

    # ── 4.8) 7 SECRET 국적 파워 분석 ──
    t0 = time.time()
    nat_power_scores = {}
    nat_price_data = {}
    nat_daily = {}  # FLOWX 업로드용 국적 원시 데이터
    try:
        from data.nationality_profiler import calc_nationality_power, collect_daily_series
        from pathlib import Path as _Path
        _data_store = _Path(__file__).resolve().parent.parent / "data_store"
        # price_data를 chg_5d 형태로 변환
        for code in top_codes:
            pd_info = price_data.get(code)
            if pd_info:
                # 5일 수익률: CSV daily에서 계산
                csv_path = _data_store / "daily" / f"{code}.csv"
                chg_5d = 0.0
                if csv_path.exists():
                    try:
                        import pandas as _pd
                        df = _pd.read_csv(csv_path, encoding="utf-8")
                        if len(df) >= 5:
                            c5 = df["종가"].iloc[-5]
                            c0 = df["종가"].iloc[-1]
                            if c5 > 0:
                                chg_5d = (c0 / c5 - 1) * 100
                    except Exception:
                        pass
                nat_price_data[code] = {"chg_5d": chg_5d}
        nat_daily = collect_daily_series(top_codes, n_days=5)
        nat_power_scores = calc_nationality_power(
            top_codes, daily_data=nat_daily, price_data=nat_price_data,
            all_codes_data=nat_daily, n_days=5,
        )
        np_active = sum(1 for v in nat_power_scores.values() if v.score != 0)
        logger.info(f"[Step 4.8] 7SECRET 파워: {np_active}/{len(top_codes)}종목 시그널 ({time.time()-t0:.0f}s)")
    except Exception as e:
        logger.warning(f"[Step 4.8] 7SECRET 파워 실패 (무시): {e}")

    # ── 5) 종합 스코어링 (v3: 기술회복 + 7SECRET 포함) ──
    t0 = time.time()
    candidates = []

    for code in top_codes:
        info = code_to_info.get(code, {})
        name = info.get("name", code)
        pd_info = price_data.get(code)
        cs_info = consensus_data.get(code)

        if not pd_info:
            continue

        current = pd_info["current"]
        pre_war = pd_info["pre_war"]
        war_low = pd_info["war_low"]

        # 이미 전쟁 전 가격을 초과한 종목은 제외 (방산 등)
        if current >= pre_war:
            logger.debug(f"{name}: 이미 회복 완료 ({current:,} >= {pre_war:,})")
            continue

        # 회복 업사이드 (%)
        recovery_upside = (pre_war / current - 1) * 100

        # 컨센서스 업사이드 (%)
        consensus_upside = 0.0
        consensus_target = 0
        if cs_info:
            consensus_target = cs_info["target_price"]
            if consensus_target > current:
                consensus_upside = (consensus_target / current - 1) * 100

        # 전쟁 낙폭 (%) -- 낙폭이 클수록 기회
        war_drop = abs((war_low / pre_war - 1) * 100) if war_low > 0 else 0

        # 현재 회복률 (0~100%) -- 낮을수록 아직 기회
        if pre_war > war_low > 0:
            recovery_rate = (current - war_low) / (pre_war - war_low) * 100
        else:
            recovery_rate = 0

        # 수급 점수 (-25 ~ 0 실제 범위)
        sup_info = supply_scores.get(code, (0, ""))
        supply_sc = sup_info[0] if isinstance(sup_info, tuple) else 0
        supply_detail = sup_info[1] if isinstance(sup_info, tuple) else ""
        # 정규화: -25~0 → 0~25 (0=최악, 25=이탈없음)
        supply_norm = max(0.0, supply_sc + 25.0)

        # 기술 회복 시그널 (0~20)
        tech_info = tech_recovery_scores.get(code, {"score": 0, "rsi": 50, "chg_5d": 0, "detail": ""})
        tech_score = tech_info["score"]
        tech_rsi = tech_info["rsi"]
        tech_chg_5d = tech_info["chg_5d"]
        tech_sig_detail = tech_info["detail"]

        # 유동성 (거래량 기반, 0~10)
        volume = info.get("volume", 0)
        liquidity_sc = min(volume / 1_000_000, 10)  # 100만주 = 1점, 최대 10점

        # 7 SECRET 국적 파워 점수
        np_result = nat_power_scores.get(code)
        nat_power_sc = np_result.score if np_result else 0.0

        # ── 종합 스코어 v3 (기술회복 + 7SECRET 포함) ──
        # 회복25% + 컨센15% + 수급15% + 기술16% + 7SECRET24% + 낙폭5%
        war_score = (
            recovery_upside * 0.25       # 회복 업사이드 (25%)
            + consensus_upside * 0.15    # 컨센서스 업사이드 (15%)
            + supply_norm * 0.15         # 수급 정규화 (15%)
            + tech_score * 0.80          # 기술회복 (16%)
            + nat_power_sc * 0.30        # 7SECRET 파워 (24%)
            + war_drop * 0.05            # 낙폭크기 (5%)
            + liquidity_sc * 0.40        # 유동성
        )

        # 졸업 임박 감점: 70%+ 회복한 종목은 순위 하락
        graduation_tag = ""
        if recovery_rate >= 70:
            war_score -= 10.0
            graduation_tag = "[졸업임박]"
            logger.debug(f"{name}: 회복률 {recovery_rate:.0f}% → 졸업 감점 -10")

        from data.sajang_rules import SAJANG
        sl_base = war_low if war_low > 0 else current
        sl = SAJANG.get_normal_sl(sl_base)
        tp = SAJANG.get_take_profit(current)

        # 신뢰도
        if consensus_upside > 20 and recovery_upside > 15 and supply_sc >= -10:
            confidence = "HIGH"
        elif consensus_upside > 10 or recovery_upside > 20:
            confidence = "MED"
        else:
            confidence = "LOW"

        # 소스 구성
        sources = [f"war_dip({war_drop:.0f}%DOWN)"]
        if recovery_upside > 0:
            sources.append(f"recovery(+{recovery_upside:.0f}%)")
        if consensus_upside > 0:
            sources.append(f"consensus({consensus_target:,}->+{consensus_upside:.0f}%)")
        if supply_sc >= -10:
            sources.append(f"supply(OK)")
        if tech_sig_detail:
            sources.append(f"tech({tech_sig_detail})")

        # 기술 상세 (v2: RSI + 5일수익 포함)
        tech_detail = (
            f"낙폭{war_drop:.0f}%|회복률{recovery_rate:.0f}%|"
            f"5D:{tech_chg_5d:+.1f}%|RSI{tech_rsi:.0f}|"
            f"회복+{recovery_upside:.0f}%|컨센+{consensus_upside:.0f}%"
            f"{graduation_tag}"
        )

        # nat_power 상세
        np_grade = np_result.grade if np_result else ""
        np_detail = np_result.detail if np_result else ""
        if np_result and nat_power_sc != 0:
            sources.append(f"7SECRET({np_grade}:{nat_power_sc:+.1f})")

        rec = RecommendedStock(
            code=code,
            name=name,
            close=current,
            total_score=round(war_score, 1),
            confidence=confidence,
            entry=current,
            sl=sl,
            tp=tp,
            sl_source="war_low" if war_low > 0 else "10%",
            sources=sources,
            tech_detail=tech_detail,
            news_detail=f"recovery_rate={recovery_rate:.0f}%",
            nationality_score=supply_sc,
            nationality_detail=supply_detail,
            cross_count=len(sources),
            nat_power=nat_power_sc,
            nat_power_grade=np_grade,
            nat_power_detail=np_detail,
        )
        candidates.append(rec)

    # 정렬: 종합 스코어 내림차순
    candidates.sort(key=lambda x: x.total_score, reverse=True)

    # 먼저 임시 TOP 선정 (TV 스탬핑 후 TV 슬롯 재배치)
    report.stocks = candidates[:8]

    # 전쟁모드 종목에도 MOMENTUM 레짐 감지 + TV 스캐너 스탬핑
    try:
        import json as _json_war
        from data.regime_detector import detect_regime_batch
        uni_path = Path(__file__).resolve().parent.parent / "data_store" / "universe.json"
        with open(uni_path, "r", encoding="utf-8") as _uf:
            _universe = _json_war.load(_uf)
        war_codes = [s.code for s in report.stocks]
        war_regime = detect_regime_batch(war_codes, _universe)

        # TV 스캐너 결과 로드 (normal 모드에서 이미 저장됨, or 직접 스캔)
        war_tv_signals = {}
        try:
            from data.trading_value_scanner import load_tv_results
            tv_data = load_tv_results()
            war_tv_signals = {s["code"]: s for s in tv_data if s.get("score", 0) > 0}
        except Exception:
            pass

        war_mtm_boost = 0
        for s in report.stocks:
            if s.code in war_regime:
                r = war_regime[s.code]
                s.regime = r.regime
                s.regime_score = r.score
                s.regime_detail = f"VOL{r.vol_ratio:.1f}x+TV{r.tv_ratio:.1f}x+기관{r.consec_inst_foreign_days}D"
                if r.regime == "MOMENTUM":
                    boost = min(r.score * 15, 10)
                    s.total_score += boost
                    war_mtm_boost += 1

            # TV 스탬핑 (전쟁모드)
            if s.code in war_tv_signals:
                tv = war_tv_signals[s.code]
                s.tv_ratio = tv.get("tv_ratio", 1.0)
                s.tv_pattern = tv.get("pattern", "NORMAL")
                s.tv_score = tv.get("score", 0.0)
                s.tv_frgn_joined = tv.get("frgn_joined", False)
                # 전쟁모드 TV 부스트 (최대 +5점)
                if tv.get("pattern") in ("QUIET_ACCUMULATION", "EXPLOSION", "EARLY_ACCUMULATION"):
                    tv_boost = min(tv.get("score", 0) * 0.05, 5)
                    s.total_score += tv_boost

        if war_mtm_boost > 0:
            report.stocks.sort(key=lambda x: x.total_score, reverse=True)
        war_mtm = sum(1 for r in war_regime.values() if r.regime == "MOMENTUM")
        logger.info(f"[전쟁모드] 레짐: {war_mtm}/{len(war_regime)} MOMENTUM ({war_mtm_boost} 부스트)")
    except Exception as e:
        logger.warning(f"[전쟁모드] 레짐 감지 실패 (무시): {e}")

    # ── 전쟁모드 TV 전용 슬롯: 최대 2개 ──
    # v2: TOP 50 밖 TV 강신호도 후보 풀에 주입 (삼성E&A 같은 중형주 커버)
    war_normal_top = report.stocks[:8]
    existing_codes = {c.code for c in candidates}

    # TOP 50 밖 TV 강신호 주입 (QUIET_ACC 75+ / EXPLOSION 75+)
    try:
        if not war_tv_signals:
            from data.trading_value_scanner import load_tv_results
            tv_data = load_tv_results()
            war_tv_signals = {s["code"]: s for s in tv_data if s.get("score", 0) > 0}
    except Exception:
        pass

    tv_injected = 0
    for tv_code, tv_info in war_tv_signals.items():
        if tv_code in existing_codes:
            continue
        tv_pat = tv_info.get("pattern", "NORMAL") if isinstance(tv_info, dict) else getattr(tv_info, "pattern", "NORMAL")
        tv_sc = tv_info.get("score", 0) if isinstance(tv_info, dict) else getattr(tv_info, "score", 0)
        tv_r = tv_info.get("tv_ratio", 1.0) if isinstance(tv_info, dict) else getattr(tv_info, "tv_ratio", 1.0)
        tv_nm = tv_info.get("name", tv_code) if isinstance(tv_info, dict) else getattr(tv_info, "name", tv_code)
        tv_close = tv_info.get("close", 0) if isinstance(tv_info, dict) else getattr(tv_info, "close", 0)

        if tv_pat not in ("QUIET_ACCUMULATION", "EXPLOSION") or tv_sc < 75:
            continue

        # TV 강신호 직접 점수 생성
        tv_direct_sc = 35 if tv_pat == "QUIET_ACCUMULATION" and tv_sc >= 80 else (
            30 if tv_pat == "EXPLOSION" and tv_sc >= 80 else 20)
        cross_sc = 20  # tv_sc >= 75 → cross=2 → bonus 20

        from data.sajang_rules import SAJANG
        injected_rec = RecommendedStock(
            code=tv_code,
            name=tv_nm,
            close=tv_close,
            total_score=round(tv_direct_sc + cross_sc, 1),
            confidence="MED",
            entry=tv_close,
            sl=SAJANG.get_normal_sl(tv_close) if tv_close else 0,
            tp=SAJANG.get_take_profit(tv_close) if tv_close else 0,
            sl_source="SAJANG",
            sources=[f"tv_inject:{tv_pat}({tv_sc:.0f})"],
            tech_detail=f"TV{tv_sc:.0f}|ratio{tv_r:.1f}x",
            news_detail="TV_INJECT",
            tv_ratio=tv_r,
            tv_pattern=tv_pat,
            tv_score=tv_sc,
        )
        candidates.append(injected_rec)
        tv_injected += 1

    if tv_injected > 0:
        logger.info(f"[전쟁모드 TV 주입] TOP50 밖 {tv_injected}종목 후보 풀 추가")

    war_tv_only = []
    for c in candidates[8:]:
        if c.tv_score >= 70 and c.tv_pattern in ("QUIET_ACCUMULATION", "EXPLOSION"):
            war_tv_only.append(c)
    if war_tv_only:
        war_tv_only.sort(key=lambda x: x.tv_score, reverse=True)
        tv_ins = war_tv_only[:2]
        war_final = war_normal_top[:8 - len(tv_ins)] + tv_ins
        war_final.sort(key=lambda x: x.total_score, reverse=True)
        logger.info(f"[전쟁 TV Slot] {len(tv_ins)}종목 삽입: {', '.join(s.name for s in tv_ins)}")
        report.stocks = war_final

    elapsed = time.time() - t_start
    logger.info(f"[전쟁모드] 최종 추천: {len(report.stocks)}종목 ({elapsed:.0f}s)")

    # ── 2단계: 국적별 행동 프로파일러 + 7 SECRET 파워 ──
    try:
        from data.nationality_profiler import predict_tomorrow_flow
        top_codes = [s.code for s in report.stocks]
        top_names = {s.code: s.name for s in report.stocks}
        # price_data 전달 (VPD 계산용)
        top_price_data = {}
        for c in top_codes:
            if c in nat_price_data:
                top_price_data[c] = nat_price_data[c]
        flow_preds = predict_tomorrow_flow(
            top_codes, n_days=5, code_names=top_names,
            price_data=top_price_data,
        )
        flow_map = {p["code"]: p for p in flow_preds}

        for s in report.stocks:
            pred = flow_map.get(s.code)
            if pred:
                s.flow_signal = pred["signal"]
                s.flow_score = pred["score"]
                # 근거 + 리스크 합산
                parts = []
                if pred["reason"] != "특이사항 없음":
                    parts.append(pred["reason"])
                if pred.get("risk"):
                    parts.append(f"⚠️{pred['risk']}")
                s.flow_detail = " | ".join(parts) if parts else "중립"
                # 7 SECRET 파워 (2단계에서 세부 업데이트)
                np_obj = pred.get("nat_power")
                if np_obj and hasattr(np_obj, "score"):
                    s.nat_power = np_obj.score
                    s.nat_power_grade = np_obj.grade
                    s.nat_power_detail = np_obj.detail

        logger.info(f"[전쟁모드] 2단계 수급 프로파일링 + 7SECRET 완료: {len(flow_preds)}종목")
    except Exception as e:
        logger.warning(f"[전쟁모드] 수급 프로파일링 실패 (무시): {e}")

    # ── 전쟁릴레이 워치리스트도 포함 ──
    try:
        war_relay_list = _step_war_relay_inject()
        report.war_relay_stocks = war_relay_list
    except Exception:
        pass

    # 자동 저장
    try:
        save_recommendation(report)
    except Exception as e:
        logger.warning(f"추천 자동저장 실패: {e}")

    # Market Brain 합성 리포트 생성
    try:
        from data.market_brain import generate_brain_report, save_brain_report
        brain = generate_brain_report()
        save_brain_report(brain)
        logger.info(f"[Market Brain] 비중 {brain.position_size_pct}% | {brain.overall_verdict[:50]}")

        # FIX-01: BRAIN 비중 → 추천 종목 수 캡 (최소 2종목 보장)
        cap = _brain_stock_cap(brain.position_size_pct)
        if len(report.stocks) > cap:
            logger.info(
                f"[BRAIN CAP] {len(report.stocks)}종목 → {cap}종목 "
                f"(비중 {brain.position_size_pct}%)"
            )
            report.stocks = report.stocks[:cap]
            save_recommendation(report)  # 캡 적용된 리스트로 재저장
    except Exception as e:
        logger.warning(f"[Market Brain] 실패 (무시): {e}")

    # Trade Object Layer — 트레이드 설계 자동 생성
    try:
        from data.trade_object import build_trade_objects_from_report, save_trade_objects
        trade_objects = build_trade_objects_from_report(report)
        save_trade_objects(trade_objects)
        accepted = len([t for t in trade_objects if t.rr_verdict != "REJECT"])
        logger.info(f"[TradeObject] {accepted}/{len(trade_objects)} ACCEPT (R:R 통과)")
    except Exception as e:
        logger.warning(f"[TradeObject] 실패 (무시): {e}")

    # FLOWX 업로드 (Supabase short_signals)
    try:
        from data.upload_short import run_flowx_upload
        import json
        rec_path = Path(__file__).resolve().parent.parent / "data_store" / "recommendation.json"
        rec_data = json.loads(rec_path.read_text("utf-8"))
        run_flowx_upload(rec_data, nat_daily_all=nat_daily or None)
        logger.info("[FLOWX] 전쟁모드 업로드 완료")
    except Exception as e:
        logger.warning(f"[FLOWX] 전쟁모드 업로드 실패 (무시): {e}")

    return report


def format_war_recommendation(report: RecommendationReport) -> str:
    """전쟁 모드 전용 텔레그램 포맷"""
    lines = [
        "🔴 전쟁 모드 추천 (낙폭 대형주 랭킹)",
        f"   {report.timestamp}",
        "=" * 36,
    ]

    if report.market_health:
        health_emoji = {"NORMAL": "🟢", "WARNING": "🟡", "CRITICAL": "🔴"}.get(
            report.market_health, "⚪"
        )
        lines.append(f"{health_emoji} 시장건전성: {report.market_health}")

    if report.warning:
        lines.append(f"⚠️ {report.warning}")

    lines.append("")

    conf_emoji = {"HIGH": "🔴", "MED": "🟡", "LOW": "⚪"}

    for i, s in enumerate(report.stocks[:8], 1):
        ce = conf_emoji.get(s.confidence, "⚪")
        lines.append(
            f"{ce} {i}. {s.name}({s.code}) [{s.confidence}] "
            f"WAR점수:{s.total_score:.0f}"
        )
        lines.append(f"   현재: {s.close:,}원")
        lines.append(f"   {s.tech_detail}")
        lines.append(f"   SL: {s.sl:,} → TP: {s.tp:,}")
        if s.nationality_detail:
            lines.append(f"   🌍 {s.nationality_detail}")
        # 2단계: 국적별 행동 시그널
        flow_sig = getattr(s, "flow_signal", "")
        flow_sc = getattr(s, "flow_score", 0)
        flow_det = getattr(s, "flow_detail", "")
        if flow_sig:
            flow_emoji = {
                "STRONG_BUY": "🔴", "BUY": "🟠",
                "NEUTRAL": "⚪", "CAUTION": "🟡", "SELL": "🔵",
            }.get(flow_sig, "⚪")
            flow_kr = {
                "STRONG_BUY": "강력매수", "BUY": "매수",
                "NEUTRAL": "중립", "CAUTION": "주의", "SELL": "매도",
            }.get(flow_sig, flow_sig)
            lines.append(f"   {flow_emoji} 수급예측: {flow_kr}({flow_sc:+.0f})")
            if flow_det:
                lines.append(f"   └ {flow_det}")
        # 7 SECRET 파워 표시
        np_grade = getattr(s, "nat_power_grade", "")
        np_score = getattr(s, "nat_power", 0)
        np_detail = getattr(s, "nat_power_detail", "")
        if np_grade and np_grade != "NEUTRAL":
            grade_emoji = {"POWER_BUY": "💥", "BUY": "🟢", "CAUTION": "🟡", "DANGER": "🔴"}.get(np_grade, "⚪")
            lines.append(f"   {grade_emoji} 7SECRET: {np_grade}({np_score:+.1f})")
            if np_detail:
                lines.append(f"   └ {np_detail}")
        if s.sources:
            lines.append(f"   출처: {' + '.join(s.sources[:4])}")
        lines.append("")

    # 전쟁릴레이 포함
    if report.war_relay_stocks:
        lines.append("=" * 36)
        lines.append(f"🎯 전쟁→재건 릴레이 ({len(report.war_relay_stocks)}종목)")
        for i, s in enumerate(report.war_relay_stocks[:5], 1):
            close = s.get("close", 0)
            entry = s.get("entry", 0)
            lines.append(
                f"  {i}. {s.get('name','')}({s.get('code','')}) "
                f"[{s.get('tier','')}] {close:,}원"
            )

    return "\n".join(lines)


# ═══════════════════════════════════════
#  CLI 테스트
# ═══════════════════════════════════════

if __name__ == "__main__":
    import sys
    # CLI 실행 시 scalper-agent/ 를 모듈 경로에 추가
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")

    mode = sys.argv[1] if len(sys.argv) > 1 else "normal"

    if mode == "war":
        print("전쟁 모드 추천 파이프라인 실행...")
        report = run_war_mode_recommendation()
        print(format_war_recommendation(report))
    else:
        print("저녁 추천 파이프라인 실행...")
        report = run_evening_recommendation()
        print(format_recommendation(report))
