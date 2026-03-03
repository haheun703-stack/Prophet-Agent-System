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
from typing import Optional

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
    cross_count: int = 0           # 교차 등장 횟수
    # 합산
    total_score: float = 0.0
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


@dataclass
class RecommendationReport:
    stage: str = ""  # "evening" / "us_check" / "morning"
    timestamp: str = ""
    market_health: str = ""
    us_market_note: str = ""
    stocks: list = field(default_factory=list)  # list[RecommendedStock]
    relay_summary: str = ""
    warning: str = ""


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
        }
    except Exception as e:
        logger.error(f"릴레이 스캔 실패: {e}")
        return {"stocks": {}, "summary": f"실패: {e}"}


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
#  Step 3: 기술적 분석 필터
# ═══════════════════════════════════════

def _step3_tech_filter(codes_names: list[tuple[str, str]]) -> dict:
    """EMA/RSI/MACD/OBV 기술적 점수 계산

    Returns: {code: {"score": 0~5, "detail": "정배열+OBV UP+RSI42"}}
    """
    from pykrx import stock
    from datetime import datetime, timedelta
    import numpy as np

    results = {}
    end = datetime.now()
    start = end - timedelta(days=120)
    start_s = start.strftime("%Y%m%d")
    end_s = end.strftime("%Y%m%d")

    for code, name in codes_names:
        try:
            df = stock.get_market_ohlcv(start_s, end_s, code)
            if df is None or len(df) < 60:
                results[code] = {"score": 0, "detail": "데이터부족"}
                continue

            close = df["종가"].astype(float).values
            volume = df["거래량"].astype(float).values

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

            # 점수 계산 (0~5)
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
                details.append("OBV UP")
            else:
                score -= 0.5
                details.append("OBV DOWN")

            results[code] = {
                "score": max(0, round(score, 1)),
                "detail": "+".join(details) if details else "N/A",
                "rsi": round(rsi, 1),
                "obv_dir": obv_dir,
            }
        except Exception as e:
            logger.warning(f"기술 분석 실패 {name}({code}): {e}")
            results[code] = {"score": 0, "detail": f"오류:{e}"}

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
        logger.warning(f"뉴스AI 실패: {e} — 미분석 종목 NEUTRAL 처리")

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
) -> list[RecommendedStock]:
    """모든 스텝 결과 통합 → 교차검증 → 최종 랭킹"""
    if macd_result is None:
        macd_result = {}

    # 모든 종목 코드 수집
    all_codes = set()
    all_codes.update(relay.get("stocks", {}).keys())
    all_codes.update(premove.get("stocks", {}).keys())
    all_codes.update(macd_result.keys())

    candidates = []
    for code in all_codes:
        r_info = relay.get("stocks", {}).get(code, {})
        p_info = premove.get("stocks", {}).get(code, {})
        t_info = tech.get(code, {})
        n_info = news.get(code, {})

        # 뉴스 NEGATIVE → 제거
        if n_info.get("sentiment") == "NEGATIVE":
            logger.info(f"뉴스 NEGATIVE 제거: {r_info.get('name', p_info.get('name', code))}")
            continue

        # OBV DOWN → 제거 (돈 빠지는 중)
        if t_info.get("obv_dir") == "DOWN":
            logger.info(f"OBV DOWN 제거: {r_info.get('name', p_info.get('name', code))}")
            continue

        # 기술 점수 2.0 미만 → 제거
        if t_info.get("score", 0) < 2.0:
            continue

        name = r_info.get("name") or p_info.get("name", code)
        close = r_info.get("close") or p_info.get("close", 0)

        # 교차 등장 횟수 (MACD 0선 포함)
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
            sources.append(f"macd_zero({macd_result[code].get('source', '')})")

        # 점수 합산 (가중치)
        relay_sc = min(r_info.get("signal_count", 0) * 15, 45)  # 0~45
        premove_sc = min(p_info.get("premove_score", 0), 100) * 0.3  # 0~30
        tech_sc = t_info.get("score", 0) * 5  # 0~25
        cross_bonus = cross * 10 if cross >= 2 else 0

        total = relay_sc + premove_sc + tech_sc + cross_bonus

        # MACD Phase2 진입 시그널 정보
        m_info = macd_result.get(code, {})

        # 진입/SL/TP: premove → MACD Phase2 → 간단 계산 (우선순위)
        entry = int(p_info.get("entry") or m_info.get("entry") or close)
        sl = int(p_info.get("sl") or m_info.get("sl") or close * 0.95)
        tp = int(p_info.get("tp") or m_info.get("tp") or close * 1.10)
        sl_source = p_info.get("sl_source", "ATR")

        # 신뢰도
        if cross >= 2 and t_info.get("score", 0) >= 3.5:
            confidence = "HIGH"
        elif cross >= 1 and t_info.get("score", 0) >= 2.5:
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
            cross_count=cross,
            total_score=round(total, 1),
            entry=entry,
            sl=sl,
            tp=tp,
            sl_source=sl_source,
            sources=sources,
            tech_detail=t_info.get("detail", ""),
            news_detail=n_info.get("sentiment", "NEUTRAL"),
            confidence=confidence,
        )
        candidates.append(rec)

    # 정렬: total_score 내림차순
    candidates.sort(key=lambda x: x.total_score, reverse=True)
    return candidates[:8]  # 최대 8개


# ═══════════════════════════════════════
#  메인 파이프라인
# ═══════════════════════════════════════

def run_evening_recommendation() -> RecommendationReport:
    """Stage 1: 저녁 분석 (16:45) — 5단계 전체 실행 + MACD 0선

    최적화: 기술필터 통과 종목만 뉴스AI 분석 (API 비용/시간 절약)
    캐시: 오늘 이미 분석한 종목은 재사용
    """
    import time
    from datetime import datetime

    logger.info("=" * 50)
    logger.info("저녁 추천 파이프라인 시작 (5단계)")
    logger.info("=" * 50)
    t_start = time.time()

    report = RecommendationReport(
        stage="evening",
        timestamp=datetime.now().strftime("%Y-%m-%d %H:%M"),
    )

    # 0) 시장 건전성
    try:
        from data.market_health import check_market_health
        health = check_market_health()
        report.market_health = health.get("level", "UNKNOWN")
        if report.market_health == "CRITICAL":
            report.warning = "시장 건전성 CRITICAL — 매수 자제 권고"
            return report
    except Exception as e:
        report.market_health = f"체크실패: {e}"

    # Step 1: 릴레이 스캔
    t0 = time.time()
    logger.info("[Step 1/5] 릴레이 스캔...")
    relay_result = _step1_relay_scan()
    report.relay_summary = relay_result.get("summary", "")
    logger.info(f"  → {len(relay_result.get('stocks', {}))}종목 ({time.time()-t0:.0f}s)")

    # Step 2: 사전감지 스캔 (내부에 뉴스AI 포함 — 오래 걸림)
    t0 = time.time()
    logger.info("[Step 2/5] 사전감지 스캔...")
    premove_result = _step2_premove_scan()
    logger.info(f"  → {len(premove_result.get('stocks', {}))}종목 ({time.time()-t0:.0f}s)")

    # 기술분석 대상 종목 수집
    all_codes = set()
    for code, info in relay_result.get("stocks", {}).items():
        all_codes.add((code, info.get("name", code)))
    for code, info in premove_result.get("stocks", {}).items():
        all_codes.add((code, info.get("name", code)))

    if not all_codes:
        report.warning = "릴레이+사전감지 결과 0건 — 추천 불가"
        return report

    # Step 2.5: MACD 제로선 크로스 스캔 (추가 소스)
    t0 = time.time()
    logger.info("[Step 2.5] MACD 0선 크로스 스캔...")
    macd_result = _step_macd_zero_scan()
    for code, info in macd_result.items():
        if (code, info.get("name", code)) not in all_codes:
            all_codes.add((code, info.get("name", code)))
    logger.info(f"  → {len(macd_result)}종목 ({time.time()-t0:.0f}s)")

    codes_names = list(all_codes)

    # Step 3: 기술적 필터
    t0 = time.time()
    logger.info(f"[Step 3/5] 기술 분석 ({len(codes_names)}종목)...")
    tech_result = _step3_tech_filter(codes_names)
    logger.info(f"  → 완료 ({time.time()-t0:.0f}s)")

    # Step 4: 뉴스AI (기술필터 통과 종목만 — API 비용/시간 절약)
    # 기술 점수 2.0+ AND OBV UP인 종목만 분석 (나머지는 Step5에서 걸러짐)
    t0 = time.time()
    filtered_for_news = [
        (code, name) for code, name in codes_names
        if tech_result.get(code, {}).get("score", 0) >= 2.0
        and tech_result.get(code, {}).get("obv_dir") != "DOWN"
    ]
    logger.info(f"[Step 4/5] 뉴스AI ({len(filtered_for_news)}/{len(codes_names)}종목, 기술필터 통과분만)...")
    news_result = _step4_news_filter(filtered_for_news)
    # 뉴스AI 미분석 종목은 NEUTRAL로 채움
    for code, name in codes_names:
        if code not in news_result:
            news_result[code] = {"sentiment": "NEUTRAL", "reason": "기술필터미통과", "score": 0}
    logger.info(f"  → 완료 ({time.time()-t0:.0f}s)")

    # Step 5: 교차검증 (MACD 0선 소스 포함)
    t0 = time.time()
    logger.info("[Step 5/5] 교차검증 + 최종 랭킹...")
    final_stocks = _step5_cross_validate(
        relay_result, premove_result, tech_result, news_result,
        macd_result=macd_result,
    )

    report.stocks = final_stocks
    elapsed = time.time() - t_start
    logger.info(f"최종 추천: {len(final_stocks)}종목 (전체 {elapsed:.0f}s)")

    return report


def run_us_market_check(prev_report: RecommendationReport) -> RecommendationReport:
    """Stage 2: 미국장 체크 (06:30) — 미국 지수 확인 → 추천 조정"""
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
                report.warning = f"VIX {vix_val:.1f} — 공포 급등! 매수 규모 축소 권고"
            elif vix_val > 25:
                report.warning = f"VIX {vix_val:.1f} — 경계, 1/2 규모 매수 고려"
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
            report.warning = f"위기모드 활성: {reason} — 전종목 매수 중단"
            report.stocks = []
    except Exception:
        pass

    return report


# ═══════════════════════════════════════
#  텔레그램 포맷
# ═══════════════════════════════════════

def format_recommendation(report: RecommendationReport, max_budget: int = 0) -> str:
    """텔레그램용 추천 리포트 포맷

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

    if report.relay_summary:
        lines.append(f"🔄 릴레이: {report.relay_summary}")

    if report.us_market_note:
        lines.append(f"🇺🇸 미국: {report.us_market_note}")

    if report.warning:
        lines.append(f"\n⚠️ {report.warning}")

    lines.append("")

    if not report.stocks:
        lines.append("추천 종목 없음")
        return "\n".join(lines)

    conf_emoji = {"HIGH": "🔴", "MED": "🟡", "LOW": "⚪"}

    for i, s in enumerate(report.stocks[:5], 1):
        ce = conf_emoji.get(s.confidence, "⚪")
        shares = max_budget // s.close if s.close > 0 else 0
        buy_total = shares * s.close if shares > 0 else 0

        lines.append(
            f"{ce} {i}. {s.name}({s.code}) [{s.confidence}] "
            f"총점:{s.total_score:.0f}"
        )
        lines.append(
            f"   현재: {s.close:,}원 | SL: {s.sl:,} → TP: {s.tp:,}"
        )
        lines.append(
            f"   기술: {s.tech_detail} | 뉴스: {s.news_detail}"
        )
        if shares > 0:
            lines.append(f"   매수: {shares}주 = {buy_total:,}원")
        if s.sources:
            lines.append(f"   출처: {' + '.join(s.sources[:4])}")
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
        "us_market_note": report.us_market_note,
        "relay_summary": report.relay_summary,
        "warning": report.warning,
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
            }
            for s in report.stocks
        ],
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    logger.info(f"추천 저장: {path} ({report.stage}, {len(report.stocks)}종목)")


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
            us_market_note=data.get("us_market_note", ""),
            relay_summary=data.get("relay_summary", ""),
            warning=data.get("warning", ""),
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
            ))
        return report
    except Exception as e:
        logger.error(f"추천 로드 실패: {e}")
        return None


# ═══════════════════════════════════════
#  CLI 테스트
# ═══════════════════════════════════════

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    print("저녁 추천 파이프라인 실행...")
    report = run_evening_recommendation()
    print(format_recommendation(report))
