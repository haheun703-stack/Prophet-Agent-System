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
    nationality_score: float = 0.0 # 국적별 수급 점수
    nationality_detail: str = ""   # 국적별 수급 상세
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
#  시장 등락률 조회 (상대강도 기준)
# ═══════════════════════════════════════

def _get_market_change_today() -> float:
    """오늘 KOSPI(KODEX200) 등락률 — 상대강도 계산 기준"""
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
        logger.error("pykrx 당일 데이터 미반영 — 추천 정확도 저하 가능")

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
    nationality: dict = None,
    bargain_result: dict = None,
    market_chg: float = 0.0,
    regime_info: dict = None,
    shock_info: dict = None,
) -> list[RecommendedStock]:
    """모든 스텝 결과 통합 → Soft Scoring → 최종 랭킹

    v2: Hard gate 제거 → 모든 요소를 점수로 변환
    v3: 줍줍(bargain) 소스 추가 — 낙폭+수급매집 종목
    v4: CORTEX 체제별 점수 배수 + 충격 섹터 페널티/보너스
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
    all_codes.update(bargain_result.keys())

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

        # 줍줍 점수 (0~30) — bargain_score를 0.3배로 변환
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

        # ── 합산 ──────────────────────────────
        raw_total = (relay_sc + premove_sc + tech_sc + bargain_sc + cross_bonus
                     + nat_sc + news_pen + obv_pen + rel_pen
                     + shock_pen + opp_bonus)
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
        logger.warning(f"KIS API 초기화 실패: {e} — 검증 생략")
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
    """Stage 1: 저녁 분석 (16:45) — Soft Scoring 파이프라인

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

    # 0) 시장 건전성 + CORTEX 체제/충격 — CRITICAL이어도 경고만 (종목 추천은 계속)
    regime_info = {}
    shock_info = {}
    try:
        from data.market_health import diagnose, get_position_multiplier
        health = diagnose()
        report.market_health = health.alert_level.upper()
        if health.alert_level == "critical":
            report.warning = "시장 건전성 CRITICAL — 매수 규모 축소 권고"
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

    if not all_codes_set:
        report.warning = "릴레이+사전감지+MACD+줍줍 결과 0건 — 추천 불가"
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

    # Step 4: 뉴스AI — 전종목 분석 (사전필터 제거)
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

    # Step 5: Soft Scoring 교차검증
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

    # 자동 저장
    try:
        save_recommendation(report)
    except Exception as e:
        logger.warning(f"추천 자동저장 실패: {e}")

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
        ) + f"NIGHTWATCH: DIVERGENCE — 절대 진입 금지"
    elif sp500_chg < -1.0 and tnx_chg <= 0:
        cross_regime = "CORRECTION"
        cross_detail = (
            f"S&P {sp500_chg:+.1f}% + TNX {tnx_chg:+.2f}%p "
            f"= 일반 리스크오프 (관망)"
        )
        report.warning = (
            f"{report.warning} | " if report.warning else ""
        ) + f"NIGHTWATCH: CORRECTION — 관망 권고"
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
            report.warning = f"위기모드 활성: {reason} — 전종목 매수 중단"
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
        return "\n".join(lines)

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
                "news_penalty": s.news_penalty,
                "obv_penalty": s.obv_penalty,
                "relative_penalty": s.relative_penalty,
                "today_chg": s.today_chg,
                "relative_str": s.relative_str,
            }
            for s in report.stocks
        ],
        "momentum_stocks": report.momentum_stocks,  # 소형주 급등 후보 (dict list)
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    logger.info(f"추천 저장: {path} ({report.stage}, {len(report.stocks)}종목, 모멘텀 {len(report.momentum_stocks)}종목)")


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
                news_penalty=sd.get("news_penalty", 0),
                obv_penalty=sd.get("obv_penalty", 0),
                relative_penalty=sd.get("relative_penalty", 0),
                today_chg=sd.get("today_chg", 0),
                relative_str=sd.get("relative_str", 0),
            ))
        # 소형주 모멘텀 후보 로드
        report.momentum_stocks = data.get("momentum_stocks", [])
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
