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
import os
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
                    "rsi": 50, "obv_dir": "FLAT",
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

            results[code] = {
                "score": max(0, round(score, 1)),
                "detail": "+".join(details) if details else "N/A",
                "rsi": round(rsi, 1),
                "obv_dir": obv_dir,
                "today_chg": round(today_chg, 1),
                "relative_str": relative_str,
                "close": int(close[-1]),
            }
        except Exception as e:
            logger.warning(f"기술 분석 실패 {name}({code}): {e}")
            results[code] = {
                "score": 0, "detail": f"오류:{e}",
                "today_chg": 0, "relative_str": 0, "close": 0,
                "rsi": 50, "obv_dir": "FLAT",
            }

    if data_date_ok is False:
        logger.error("pykrx 당일 데이터 미반영 - 추천 정확도 저하 가능")

    return results


# ═══════════════════════════════════════
#  Step 4: 뉴스AI 필터
# ═══════════════════════════════════════

def _step4_news_filter(codes_names: list[tuple[str, str]]) -> dict:
    """뉴스AI로 네거티브 종목 제거 (캐시 활용)

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
) -> list[RecommendedStock]:
    """모든 스텝 결과 통합 → Soft Scoring → 최종 랭킹

    v2: Hard gate 제거 → 모든 요소를 점수로 변환
    v3: 줍줍(bargain) 소스 추가 - 낙폭+수급매집 종목
    v4: CORTEX 체제별 점수 배수 + 충격 섹터 페널티/보너스
    v5: 로테이션 디텍터 - 다음 섹터 종목 보너스 + 반전 섹터 페널티
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

    # CORTEX 체제별 점수 배수
    regime_multipliers = {
        "NORMAL": 1.0,
        "CAUTION": 0.85,
        "SHOCK": 0.6,
        "PANIC": 0.3,
        "RECOVERY_EARLY": 1.2,      # 줍줍 부스트
        "RECOVERY_CONFIRMED": 1.1,
    }
    regime = regime_info.get("regime", "NORMAL")
    regime_mult = regime_multipliers.get(regime, 1.0)

    # 충격 섹터 정보
    affected_sectors = shock_info.get("affected_sectors", [])
    opportunity_sectors = shock_info.get("opportunity_sectors", [])

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

    # ── 7 SECRET 국적 파워 (NORMAL 모드) ──
    normal_nat_powers = {}
    norm_price_data = {}
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

    candidates = []
    for code in all_codes:
        r_info = relay.get("stocks", {}).get(code, {})
        p_info = premove.get("stocks", {}).get(code, {})
        t_info = tech.get(code, {})
        n_info = news.get(code, {})
        m_info = macd_result.get(code, {})
        b_info = bargain_result.get(code, {})

        name = (r_info.get("name") or p_info.get("name")
                or m_info.get("name") or b_info.get("name", code))
        close = (t_info.get("close")
                 or r_info.get("close")
                 or p_info.get("close")
                 or b_info.get("close", 0))

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
        rotation_bonus = 0.0
        if rot_info:
            rot_src = rot_info.get("rotation_source", "")
            if rot_src == "hot_early":
                rotation_bonus = 15.0   # HOT 초기 소부장 → 확인된 모멘텀, 최우선
            elif rot_src == "staging":
                rotation_bonus = 8.0    # 스테이징 섹터 → 미확인 모멘텀, 보수적
            elif rot_src == "hot_mid":
                rotation_bonus = 5.0    # HOT 중기 → 아직 기회 있음
            elif rot_src == "reversal_exit":
                rotation_bonus = -20.0  # 반전 → 신규 매수 금지 수준

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

        # ── 합산 ──────────────────────────────
        raw_total = (relay_sc + premove_sc + tech_sc + bargain_sc + cross_bonus
                     + nat_sc + news_pen + obv_pen + rel_pen
                     + shock_pen + opp_bonus + rotation_bonus + or_bias_adj
                     + eq_adj + gap_adj
                     + nat_power_sc)  # 7 SECRET 파워
        # CORTEX 체제 배수 적용
        total = raw_total * regime_mult

        # 진입/SL/TP: premove → MACD Phase2 → bargain(고점80%복구) → 간단 계산
        # 줍줍 종목은 고점의 80% 복구를 TP로 설정
        bargain_tp = int(b_info["pre_war_high"] * 0.8) if b_info.get("pre_war_high") else 0
        entry = int(p_info.get("entry") or m_info.get("entry") or close)
        sl = int(p_info.get("sl") or m_info.get("sl") or close * 0.95)
        tp = int(p_info.get("tp") or m_info.get("tp") or bargain_tp or close * 1.10)
        sl_source = p_info.get("sl_source", "ATR")

        # 신뢰도 (교차수 + 기술점수 기반)
        if cross >= 2 and t_info.get("score", 0) >= 3.0:
            confidence = "HIGH"
        elif cross >= 1 and t_info.get("score", 0) >= 2.0:
            confidence = "MED"
        else:
            confidence = "LOW"

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
        )
        candidates.append(rec)

    # 유일한 hard cutoff: 합산 > 0 (페널티가 모든 가산을 초과하면 제거)
    candidates = [c for c in candidates if c.total_score > 0]

    # 정렬: total_score 내림차순
    candidates.sort(key=lambda x: x.total_score, reverse=True)
    return candidates[:8]


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

    verified = []
    for s in stocks:
        try:
            r = trader.fetch_price(s.code)
            if not r.get("success"):
                verified.append(s)
                continue

            kis_price = r["current_price"]
            kis_chg = r["change_rate"]

            # pykrx vs KIS 가격 괴리 체크 → 가격 교체
            if s.close > 0:
                gap_pct = abs(kis_price - s.close) / s.close * 100
                if gap_pct > 5.0:
                    logger.warning(
                        f"가격 괴리: {s.name}({s.code}) "
                        f"pykrx={s.close:,} vs KIS={kis_price:,} ({gap_pct:.1f}%) "
                        f"→ KIS 가격으로 교체"
                    )
                    s.close = kis_price
                    s.entry = kis_price
                    s.sl = int(kis_price * 0.95)
                    s.tp = int(kis_price * 1.10)

            # KIS 기준 상대강도 업데이트
            s.today_chg = round(kis_chg, 1)
            s.relative_str = round(kis_chg - market_chg, 1)

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

    # Step 2: 사전감지 스캔
    t0 = time.time()
    logger.info("[Step 2/6] 사전감지 스캔...")
    premove_result = _step2_premove_scan()
    logger.info(f"  → {len(premove_result.get('stocks', {}))}종목 ({time.time()-t0:.0f}s)")

    # 기술분석 대상 종목 수집
    all_codes_set = set()
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

    codes_names = list(all_codes_set)

    # Step 3: 기술 분석 (market_chg 전달 → 상대강도 계산)
    t0 = time.time()
    logger.info(f"[Step 3/6] 기술 분석 ({len(codes_names)}종목, 시장 {market_chg:+.1f}%)...")
    tech_result = _step3_tech_filter(codes_names, market_chg=market_chg)
    logger.info(f"  → 완료 ({time.time()-t0:.0f}s)")

    # Step 4: 뉴스AI - 전종목 분석 (사전필터 제거)
    t0 = time.time()
    logger.info(f"[Step 4/6] 뉴스AI ({len(codes_names)}종목, 전체)...")
    news_result = _step4_news_filter(codes_names)
    # 누락 종목 NEUTRAL
    for code, name in codes_names:
        if code not in news_result:
            news_result[code] = {"sentiment": "NEUTRAL", "reason": "미분석", "score": 0}
    logger.info(f"  → 완료 ({time.time()-t0:.0f}s)")

    # Step 5a: 네이버 수급 검증 (외국인/기관 5일 누적 순매매)
    #   기존 nationality_signal.py는 KRX 국적별 데이터 3종목만 존재 → 사실상 미작동
    #   → 네이버 금융 크롤링으로 대체 (백테스트 PF 1.50과 동일 로직)
    nationality_scores = {}
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
        nationality_scores = score_supply_batch(
            all_code_list, close_prices=close_prices
        )
        scored = sum(1 for sc, _ in nationality_scores.values() if sc != 0)
        logger.info(f"[Step 5a] 네이버 수급: {scored}/{len(all_code_list)}종목 점수 반영 ({time.time()-t_supply:.0f}s)")
    except Exception as e:
        logger.warning(f"네이버 수급 점수 실패 (무시): {e}")
        import traceback
        logger.debug(traceback.format_exc())

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
    )
    logger.info(f"  → {len(final_stocks)}종목 ({time.time()-t0:.0f}s)")

    # Step 6: KIS API 가격 교차검증
    t0 = time.time()
    logger.info("[Step 6/6] KIS API 가격 교차검증...")
    final_stocks = _step6_kis_verify(final_stocks, market_chg=market_chg)
    logger.info(f"  → 최종 {len(final_stocks)}종목 ({time.time()-t0:.0f}s)")

    report.stocks = final_stocks
    elapsed = time.time() - t_start
    logger.info(f"최종 추천: {len(final_stocks)}종목, 모멘텀 {len(report.momentum_stocks)}종목 (전체 {elapsed:.0f}s)")

    # ── 2단계: 국적별 행동 프로파일러 + 7 SECRET 파워 ──
    try:
        from data.nationality_profiler import predict_tomorrow_flow
        top_codes = [s.code for s in report.stocks]
        top_names = {s.code: s.name for s in report.stocks}
        # price_data 전달 (VPD 계산용)
        top_price_data = {}
        for c in top_codes:
            if c in norm_price_data:
                top_price_data[c] = norm_price_data[c]
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

    return report


def run_us_market_check(prev_report: RecommendationReport) -> RecommendationReport:
    """Stage 2: 미국장 체크 (06:30) - 미국 지수 확인 → 추천 조정"""
    from datetime import datetime
    import yfinance as yf

    report = RecommendationReport(
        stage="us_check",
        timestamp=datetime.now().strftime("%Y-%m-%d %H:%M"),
        stocks=prev_report.stocks,  # 이전 추천 유지
        relay_summary=prev_report.relay_summary,
        market_health=prev_report.market_health,
    )

    # 미국 주요 지수 체크
    us_indices = {
        "^GSPC": "S&P500",
        "^IXIC": "나스닥",
        "^DJI": "다우",
        "^VIX": "VIX",
    }
    us_notes = []

    for ticker, name in us_indices.items():
        try:
            data = yf.Ticker(ticker)
            hist = data.history(period="2d")
            if hist is not None and len(hist) >= 2:
                prev_close = float(hist["Close"].iloc[-2])
                last_close = float(hist["Close"].iloc[-1])
                chg = (last_close / prev_close - 1) * 100
                us_notes.append(f"{name}: {chg:+.2f}%")
        except Exception as e:
            us_notes.append(f"{name}: 조회실패")

    report.us_market_note = " | ".join(us_notes) if us_notes else "미국장 데이터 없음"

    # VIX 급등 체크
    try:
        vix = yf.Ticker("^VIX")
        vix_hist = vix.history(period="2d")
        if vix_hist is not None and len(vix_hist) >= 1:
            vix_val = float(vix_hist["Close"].iloc[-1])
            if vix_val > 30:
                report.warning = f"VIX {vix_val:.1f} - 공포 급등! 매수 규모 축소 권고"
            elif vix_val > 25:
                report.warning = f"VIX {vix_val:.1f} - 경계, 1/2 규모 매수 고려"
    except Exception:
        pass

    # S&P500 -2% 이상 폭락 시 경고
    try:
        sp = yf.Ticker("^GSPC")
        sp_hist = sp.history(period="2d")
        if sp_hist is not None and len(sp_hist) >= 2:
            sp_chg = (float(sp_hist["Close"].iloc[-1]) / float(sp_hist["Close"].iloc[-2]) - 1) * 100
            if sp_chg < -2.0:
                report.warning = f"S&P500 {sp_chg:+.1f}% 폭락! 매수 보류 권고"
    except Exception:
        pass

    # ── NIGHTWATCH 채권 자경단 신호등 (^TNX + S&P500 교차 판정) ──
    sp500_chg = 0.0
    tnx_chg = 0.0
    try:
        sp = yf.Ticker("^GSPC")
        sp_hist = sp.history(period="2d")
        if sp_hist is not None and len(sp_hist) >= 2:
            sp500_chg = (float(sp_hist["Close"].iloc[-1]) / float(sp_hist["Close"].iloc[-2]) - 1) * 100
    except Exception:
        pass

    try:
        tnx = yf.Ticker("^TNX")
        tnx_hist = tnx.history(period="2d")
        if tnx_hist is not None and len(tnx_hist) >= 2:
            # TNX는 금리(%) 자체이므로 변동폭(bp 아닌 %p)으로 계산
            tnx_prev = float(tnx_hist["Close"].iloc[-2])
            tnx_last = float(tnx_hist["Close"].iloc[-1])
            tnx_chg = tnx_last - tnx_prev  # %p 변동 (예: 4.20→4.32 = +0.12)
    except Exception:
        pass

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
        from strategies.crisis_etf_signal import generate_signal, format_signal_telegram
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

    if report.warning:
        lines.append(f"\n⚠️ {report.warning}")

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
        lines.append(
            f"{ce} {i}. {s.name}({s.code}) [{s.confidence}] "
            f"총점:{s.total_score:.0f} {rs_icon}"
        )
        lines.append(
            f"   현재: {s.close:,}원 | 등락: {s.today_chg:+.1f}% | RS: {s.relative_str:+.1f}%"
        )
        lines.append(
            f"   SL: {s.sl:,} → TP: {s.tp:,}"
        )

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


def save_recommendation(report: RecommendationReport):
    """추천 리포트 저장 (stage간 전달용)"""
    import json
    path = _get_store_path()
    path.parent.mkdir(parents=True, exist_ok=True)

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
        "stocks": [
            {
                "code": s.code, "name": s.name, "close": s.close,
                "total_score": s.total_score, "confidence": s.confidence,
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
            }
            for s in report.stocks
        ],
        "momentum_stocks": report.momentum_stocks,  # 소형주 급등 후보 (dict list)
        "war_relay_stocks": report.war_relay_stocks,  # 전쟁→재건 릴레이 종목
        "rotation_signal": report.rotation_signal,  # 섹터 로테이션 시그널
        "rotation_detail": report.rotation_detail,  # 섹터별 로테이션 상세
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
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
            ))
        # 소형주 모멘텀 후보 로드
        report.momentum_stocks = data.get("momentum_stocks", [])
        # 전쟁→재건 릴레이 종목 로드
        report.war_relay_stocks = data.get("war_relay_stocks", [])
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
            pd = price_data.get(code)
            if pd:
                close_prices[code] = pd["current"]
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

        # SL/TP 계산
        sl = int(war_low * 0.97) if war_low > 0 else int(current * 0.90)
        tp = consensus_target if consensus_target > current else pre_war

        # 신뢰도
        if consensus_upside > 20 and recovery_upside > 15 and supply_sc >= 0:
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
        if supply_sc >= 0:
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
    report.stocks = candidates[:8]

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
