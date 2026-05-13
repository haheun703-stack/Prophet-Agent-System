# -*- coding: utf-8 -*-
"""
섹터 모멘텀 분석기 (Sector Momentum Analyzer)
==============================================
전체 23개 섹터의 실시간 모멘텀을 Naver Finance API로 감지.
rotation_detector.py가 릴레이에 의존하는 반면,
이 모듈은 독립적으로 전체 시장 섹터를 분석한다.

핵심:
1. Naver Finance API로 KOSPI+KOSDAQ 전체 종목 1일 등락률 일괄 수집
2. universe.json 섹터별 그룹핑 → 섹터 평균 수익률
3. 섹터 가속도 감지 (1D vs 5D 대비 급변)
4. morning_recommendation에 섹터 부스트 점수 공급

Usage:
    python data/sector_momentum.py          # 오늘 섹터 모멘텀 분석
    python data/sector_momentum.py --boost  # 종목별 부스트 점수 출력
"""

import json
import logging
import sys
import time
from collections import defaultdict
from pathlib import Path
from datetime import datetime, timedelta
from dataclasses import dataclass, field, asdict
from typing import Dict, List, Optional, Tuple

# shared 모듈 경로
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

logger = logging.getLogger("BH.SectorMomentum")

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data_store"
UNIVERSE_PATH = DATA_DIR / "universe.json"
SECTOR_MOMENTUM_PATH = DATA_DIR / "sector_momentum.json"
SECTOR_HISTORY_DIR = DATA_DIR / "sector_momentum_history"
THEME_MOMENTUM_PATH = DATA_DIR / "theme_momentum.json"

_NAVER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}


@dataclass
class SectorScore:
    """개별 섹터 모멘텀 점수"""
    sector: str
    stock_count: int = 0
    avg_return_1d: float = 0.0
    avg_return_3d: float = 0.0
    avg_return_5d: float = 0.0
    breadth_1d: float = 0.0       # 오늘 상승 종목 비율 (0~1)
    breadth_3d: float = 0.0       # 3일 양봉 비율
    acceleration: float = 0.0     # 가속도 = 1d - 5d_avg (급변 감지)
    volume_surge: float = 0.0     # 거래량 폭증 비율 (평소 대비)
    rank: int = 0                 # 전체 섹터 중 순위 (1=최강)
    phase: str = "NEUTRAL"        # HOT / WARMING / NEUTRAL / COOLING / COLD
    boost_score: float = 0.0      # 추천 엔진용 부스트 점수
    top_movers: list = field(default_factory=list)  # [(code, name, chg_1d)]


@dataclass
class SectorMomentumReport:
    """전체 섹터 모멘텀 보고서"""
    timestamp: str = ""
    market_return_1d: float = 0.0
    sectors: list = field(default_factory=list)  # List[SectorScore]
    hot_sectors: list = field(default_factory=list)
    cold_sectors: list = field(default_factory=list)
    rotation_signal: str = ""


# ═══════════════════════════════════════════════════════
#  유니버스 섹터 그룹핑
# ═══════════════════════════════════════════════════════

def _load_universe_sectors() -> Dict[str, List[dict]]:
    """universe.json → {sector: [{code, name, cap_억}]}"""
    if not UNIVERSE_PATH.exists():
        logger.warning("universe.json 없음")
        return {}
    uni = json.loads(UNIVERSE_PATH.read_text("utf-8"))
    sectors: Dict[str, List[dict]] = {}
    for code, info in uni.items():
        if not isinstance(info, dict):
            continue
        sector = info.get("sector", "기타")
        if sector not in sectors:
            sectors[sector] = []
        sectors[sector].append({
            "code": code,
            "name": info.get("name", code),
            "cap": info.get("cap_억", 0),
        })
    return sectors


# ═══════════════════════════════════════════════════════
#  Naver Finance API — 전체 종목 시세 수집
# ═══════════════════════════════════════════════════════

def _parse_number(s) -> float:
    """'191,900' → 191900.0, '-' → 0.0"""
    if isinstance(s, (int, float)):
        return float(s)
    if isinstance(s, str):
        cleaned = s.replace(",", "").strip()
        if not cleaned or cleaned == "-":
            return 0.0
        try:
            return float(cleaned)
        except ValueError:
            return 0.0
    return 0.0


def _fetch_naver_all(market: str, page_size: int = 100) -> List[dict]:
    """Naver Finance API로 전체 종목 시세 수집 (KOSPI/KOSDAQ)"""
    import requests
    all_stocks = []
    page = 1
    while True:
        try:
            url = (
                f"https://m.stock.naver.com/api/stocks/marketValue/"
                f"{market}?page={page}&pageSize={page_size}"
            )
            r = requests.get(url, headers=_NAVER_HEADERS, timeout=10)
            if r.status_code != 200:
                break
            data = r.json()
            stocks = data.get("stocks", [])
            if not stocks:
                break
            all_stocks.extend(stocks)
            total = data.get("totalCount", 0)
            if len(all_stocks) >= total:
                break
            page += 1
            time.sleep(0.15)
        except Exception as e:
            logger.warning(f"Naver {market} page {page} 실패: {e}")
            break
    return all_stocks


def _fetch_market_returns(n_days: int = 5) -> Dict[str, Dict]:
    """Naver Finance API로 전체 시장 종목별 등락률 수집

    Returns: {code: {"chg_1d": float, "close": float, "volume": float, "market_cap": float}}
    """
    result: Dict[str, Dict] = {}

    for market in ["KOSPI", "KOSDAQ"]:
        try:
            stocks = _fetch_naver_all(market)
            logger.info(f"  Naver {market}: {len(stocks)}종목 수집")
            skip_count = 0
            for s in stocks:
                code = s.get("itemCode", "")
                if not code or len(code) != 6:
                    continue
                try:
                    chg_1d = _parse_number(s.get("fluctuationsRatio", 0))
                    close = _parse_number(s.get("closePrice", 0))
                    volume = _parse_number(s.get("accumulatedTradingVolume", 0))
                    market_cap = _parse_number(s.get("marketValue", 0))  # 억원
                    result[code] = {
                        "chg_1d": chg_1d,
                        "close": close,
                        "volume": volume,
                        "market_cap": market_cap,
                    }
                except Exception:
                    skip_count += 1
            if skip_count:
                logger.debug(f"  {market}: {skip_count}종목 파싱 스킵")
        except Exception as e:
            logger.warning(f"Naver {market} 수집 실패: {e}")

    # 3d/5d 수익률 보완: 섹터 모멘텀 히스토리에서 계산
    _enrich_multi_day_from_history(result)

    return result


def _enrich_multi_day_from_history(result: Dict[str, Dict]):
    """과거 섹터 모멘텀 히스토리에서 3d/5d 추정 (개별 종목이 아닌 섹터 레벨)

    히스토리가 없으면 스킵 — 1d만으로도 섹터 감지는 충분히 가능
    """
    if not SECTOR_HISTORY_DIR.exists():
        return
    try:
        hist_files = sorted(SECTOR_HISTORY_DIR.glob("*.json"), reverse=True)
        if len(hist_files) < 2:
            return
        # 3일전/5일전 히스토리가 있으면 섹터별 누적 수익률 추정
        # 각 종목의 chg_3d/chg_5d는 히스토리 sum으로 근사
        day_returns: Dict[str, List[float]] = {}  # {code: [day1_chg, day2_chg, ...]}
        for hf in hist_files[:5]:
            try:
                hdata = json.loads(hf.read_text("utf-8"))
                for sector_info in hdata.get("sectors", []):
                    for mover in sector_info.get("top_movers", []):
                        code = mover.get("code", "")
                        chg = mover.get("chg_1d", 0)
                        if code:
                            if code not in day_returns:
                                day_returns[code] = []
                            day_returns[code].append(chg)
            except Exception:
                continue
        # 근사: 히스토리에서 찾은 종목만 3d/5d 설정
        for code, daily in day_returns.items():
            if code in result:
                if len(daily) >= 3:
                    result[code]["chg_3d"] = round(sum(daily[:3]), 2)
                if len(daily) >= 5:
                    result[code]["chg_5d"] = round(sum(daily[:5]), 2)
    except Exception as e:
        logger.debug(f"히스토리 보완 스킵: {e}")


# ═══════════════════════════════════════════════════════
#  섹터 모멘텀 계산
# ═══════════════════════════════════════════════════════

def analyze_sectors(market_returns: Optional[Dict] = None) -> SectorMomentumReport:
    """전체 섹터 모멘텀 분석

    Args:
        market_returns: 미리 조회한 종목별 수익률 (없으면 자동 조회)

    Returns: SectorMomentumReport
    """
    if market_returns is None:
        logger.info("Naver Finance 전체 시장 데이터 조회 중...")
        t0 = time.time()
        market_returns = _fetch_market_returns(n_days=5)
        logger.info(f"  → {len(market_returns)}종목 조회 ({time.time()-t0:.1f}s)")

    sectors = _load_universe_sectors()
    if not sectors:
        return SectorMomentumReport(timestamp=datetime.now().strftime("%Y-%m-%d %H:%M"))

    # 전체 시장 평균 (시장 대비 상대강도 계산용)
    all_1d = [v.get("chg_1d", 0) for v in market_returns.values() if "chg_1d" in v]
    market_avg_1d = sum(all_1d) / len(all_1d) if all_1d else 0

    sector_scores: List[SectorScore] = []

    for sector_name, stocks in sectors.items():
        codes = [s["code"] for s in stocks]
        matched = [(c, market_returns[c]) for c in codes if c in market_returns]

        if len(matched) < 3:
            continue

        # 섹터 평균 수익률
        returns_1d = [m["chg_1d"] for _, m in matched if "chg_1d" in m]
        returns_3d = [m.get("chg_3d", 0) for _, m in matched]
        returns_5d = [m.get("chg_5d", 0) for _, m in matched]

        avg_1d = sum(returns_1d) / len(returns_1d) if returns_1d else 0
        avg_3d = sum(returns_3d) / len(returns_3d) if returns_3d else 0
        avg_5d = sum(returns_5d) / len(returns_5d) if returns_5d else 0

        # 브레드쓰 (상승 종목 비율)
        positive_1d = sum(1 for r in returns_1d if r > 0)
        breadth_1d = positive_1d / len(returns_1d) if returns_1d else 0

        positive_3d = sum(1 for _, m in matched if m.get("chg_3d", 0) > 0)
        breadth_3d = positive_3d / len(matched) if matched else 0

        # 가속도: 오늘 수익률 - 5일 일평균
        daily_avg_5d = avg_5d / 5 if avg_5d else 0
        acceleration = avg_1d - daily_avg_5d

        # 거래량 서지 (상위 10% 종목 거래량 증가 비율)
        vol_surge = 0.0  # TODO: 추후 20일 평균 대비 계산

        # TOP 모버 (오늘 수익률 상위 5)
        movers = sorted(
            [(c, m.get("chg_1d", 0)) for c, m in matched],
            key=lambda x: x[1], reverse=True
        )
        top_movers = []
        for c, chg in movers[:5]:
            name = next((s["name"] for s in stocks if s["code"] == c), c)
            top_movers.append({"code": c, "name": name, "chg_1d": round(chg, 2)})

        # 페이즈 판단
        phase = _determine_phase(avg_1d, avg_3d, avg_5d, breadth_1d, acceleration)

        # 부스트 점수 계산
        boost = _calc_boost_score(avg_1d, avg_3d, breadth_1d, acceleration, phase)

        ss = SectorScore(
            sector=sector_name,
            stock_count=len(matched),
            avg_return_1d=round(avg_1d, 2),
            avg_return_3d=round(avg_3d, 2),
            avg_return_5d=round(avg_5d, 2),
            breadth_1d=round(breadth_1d, 3),
            breadth_3d=round(breadth_3d, 3),
            acceleration=round(acceleration, 2),
            volume_surge=round(vol_surge, 2),
            phase=phase,
            boost_score=round(boost, 1),
            top_movers=top_movers,
        )
        sector_scores.append(ss)

    # 순위 매기기 (1d 수익률 기준)
    sector_scores.sort(key=lambda s: s.avg_return_1d, reverse=True)
    for i, ss in enumerate(sector_scores):
        ss.rank = i + 1

    # HOT/COLD 분류
    hot = [s.sector for s in sector_scores if s.phase in ("HOT", "WARMING")]
    cold = [s.sector for s in sector_scores if s.phase in ("COLD", "COOLING")]

    # 로테이션 시그널
    rot_parts = []
    for s in sector_scores:
        if s.phase == "HOT":
            rot_parts.append(f"HOT:{s.sector}({s.avg_return_1d:+.1f}%)")
        elif s.phase == "WARMING":
            rot_parts.append(f"WARMING:{s.sector}({s.avg_return_1d:+.1f}%)")
    rotation_signal = " | ".join(rot_parts) if rot_parts else "특이 섹터 없음"

    report = SectorMomentumReport(
        timestamp=datetime.now().strftime("%Y-%m-%d %H:%M"),
        market_return_1d=round(market_avg_1d, 2),
        sectors=[asdict(s) for s in sector_scores],
        hot_sectors=hot,
        cold_sectors=cold,
        rotation_signal=rotation_signal,
    )

    # 결과 저장
    _save_report(report)

    return report


def _determine_phase(avg_1d, avg_3d, avg_5d, breadth_1d, acceleration) -> str:
    """섹터 페이즈 판단"""
    # HOT: 오늘 +2% 이상 & 브레드쓰 60%+
    if avg_1d >= 2.0 and breadth_1d >= 0.6:
        return "HOT"
    # HOT: 오늘 +1.5% & 3일 +3% & 브레드쓰 55%+
    if avg_1d >= 1.5 and avg_3d >= 3.0 and breadth_1d >= 0.55:
        return "HOT"
    # WARMING: 오늘 +1% & 가속도 양수 & 브레드쓰 50%+
    if avg_1d >= 1.0 and acceleration > 0.3 and breadth_1d >= 0.50:
        return "WARMING"
    # WARMING: 3일 연속 양봉 비율 높음
    if avg_1d >= 0.5 and avg_3d >= 2.0 and breadth_1d >= 0.55:
        return "WARMING"
    # COOLING: 오늘 음수 & 5일은 양수 (꺾이는 중)
    if avg_1d < -0.5 and avg_5d > 2.0:
        return "COOLING"
    # COLD: 오늘 -1% 이하 & 브레드쓰 40% 미만
    if avg_1d <= -1.0 and breadth_1d < 0.4:
        return "COLD"
    return "NEUTRAL"


def _calc_boost_score(avg_1d, avg_3d, breadth_1d, acceleration, phase) -> float:
    """추천 엔진용 부스트 점수 계산

    HOT: +15~25점, WARMING: +8~15점, NEUTRAL: 0, COOLING: -5~-10, COLD: -10~-15
    """
    if phase == "HOT":
        base = 15.0
        # 수익률 비례 추가 (최대 +10)
        extra = min(10.0, avg_1d * 3)
        # 브레드쓰 보너스
        breadth_bonus = (breadth_1d - 0.5) * 10 if breadth_1d > 0.5 else 0
        return base + extra + breadth_bonus

    elif phase == "WARMING":
        base = 8.0
        extra = min(7.0, avg_1d * 2)
        accel_bonus = min(5.0, acceleration * 2) if acceleration > 0 else 0
        return base + extra + accel_bonus

    elif phase == "COOLING":
        return max(-10.0, avg_1d * 3)

    elif phase == "COLD":
        return max(-15.0, -10.0 + avg_1d * 2)

    return 0.0


def _save_report(report: SectorMomentumReport):
    """분석 결과 저장"""
    try:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        tmp = SECTOR_MOMENTUM_PATH.with_suffix(".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(asdict(report), f, ensure_ascii=False, indent=2)
        tmp.replace(SECTOR_MOMENTUM_PATH)
        logger.info(f"섹터 모멘텀 저장: {SECTOR_MOMENTUM_PATH}")

        # 히스토리 누적
        SECTOR_HISTORY_DIR.mkdir(parents=True, exist_ok=True)
        today = datetime.now().strftime("%Y-%m-%d")
        hist_path = SECTOR_HISTORY_DIR / f"{today}.json"
        with open(hist_path, "w", encoding="utf-8") as f:
            json.dump(asdict(report), f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.error(f"섹터 모멘텀 저장 실패: {e}")


# ═══════════════════════════════════════════════════════
#  추천 엔진용 인터페이스
# ═══════════════════════════════════════════════════════

def get_sector_boost(code: str, sector: str = "") -> float:
    """종목의 섹터 부스트 점수 반환 (캐시된 결과 사용)"""
    if not SECTOR_MOMENTUM_PATH.exists():
        return 0.0
    try:
        data = json.loads(SECTOR_MOMENTUM_PATH.read_text("utf-8"))
        # sector가 지정 안 되면 universe에서 찾기
        if not sector:
            uni = json.loads(UNIVERSE_PATH.read_text("utf-8")) if UNIVERSE_PATH.exists() else {}
            info = uni.get(code, {})
            sector = info.get("sector", "") if isinstance(info, dict) else ""

        for s in data.get("sectors", []):
            if s.get("sector") == sector:
                return s.get("boost_score", 0.0)
    except Exception:
        pass
    return 0.0


def get_hot_sector_codes(top_n: int = 3) -> Dict[str, float]:
    """HOT/WARMING 섹터 종목 코드 + 부스트 점수 반환

    Returns: {code: boost_score}
    """
    if not SECTOR_MOMENTUM_PATH.exists():
        return {}
    try:
        data = json.loads(SECTOR_MOMENTUM_PATH.read_text("utf-8"))
        uni = json.loads(UNIVERSE_PATH.read_text("utf-8")) if UNIVERSE_PATH.exists() else {}

        hot_sectors = []
        for s in data.get("sectors", []):
            if s.get("phase") in ("HOT", "WARMING"):
                hot_sectors.append(s)
        hot_sectors.sort(key=lambda s: s.get("boost_score", 0), reverse=True)

        result = {}
        for s in hot_sectors[:top_n]:
            sector_name = s["sector"]
            boost = s.get("boost_score", 0)
            for code, info in uni.items():
                if isinstance(info, dict) and info.get("sector") == sector_name:
                    result[code] = boost
        return result
    except Exception:
        return {}


# ═══════════════════════════════════════════════════════
#  테마별 모멘텀 분석 (theme_map 302개 테마 기준)
# ═══════════════════════════════════════════════════════

def analyze_theme_momentum(market_returns: Optional[Dict] = None,
                           min_stocks: int = 3) -> dict:
    """302개 KIS 테마 기준 모멘텀 분석.

    기존 analyze_sectors()가 KRX 23개 섹터를 분석하는 반면,
    이 함수는 theme_map의 302개 테마 단위로 같은 분석을 수행한다.
    (예: "MLCC" 테마 → 삼성전기, LG이노텍, 대덕전자, 기가비스 평균 수익률)

    Args:
        market_returns: Naver API 종목별 등락률 (없으면 자동 조회)
        min_stocks: 최소 매칭 종목수 (기본 3)

    Returns:
        {"timestamp", "market_return_1d", "total_themes",
         "themes": [SectorScore-dict], "hot_themes", "cold_themes",
         "rotation_signal"}
    """
    from shared.theme_service import get_all_themes, is_noise_theme

    if market_returns is None:
        logger.info("Naver Finance 전체 시장 데이터 조회 중 (테마 모멘텀)...")
        t0 = time.time()
        market_returns = _fetch_market_returns(n_days=5)
        logger.info(f"  → {len(market_returns)}종목 조회 ({time.time()-t0:.1f}s)")

    all_themes = get_all_themes()
    if not all_themes:
        logger.warning("[테마모멘텀] theme_map 비어있음")
        return {"timestamp": datetime.now().strftime("%Y-%m-%d %H:%M"),
                "themes": [], "hot_themes": [], "cold_themes": []}

    # 전체 시장 평균
    all_1d = [v.get("chg_1d", 0) for v in market_returns.values() if "chg_1d" in v]
    market_avg_1d = sum(all_1d) / len(all_1d) if all_1d else 0

    # universe.json에서 종목명 매핑
    uni = {}
    if UNIVERSE_PATH.exists():
        try:
            uni = json.loads(UNIVERSE_PATH.read_text("utf-8"))
        except Exception:
            pass

    theme_scores: List[SectorScore] = []

    for theme_code, theme_info in all_themes.items():
        tname = theme_info.get("name", theme_code)
        if is_noise_theme(tname):
            continue

        codes = theme_info.get("codes", [])
        matched = [(c, market_returns[c]) for c in codes if c in market_returns]

        if len(matched) < min_stocks:
            continue

        # 테마 평균 수익률
        returns_1d = [m["chg_1d"] for _, m in matched if "chg_1d" in m]
        returns_3d = [m.get("chg_3d", 0) for _, m in matched]
        returns_5d = [m.get("chg_5d", 0) for _, m in matched]

        avg_1d = sum(returns_1d) / len(returns_1d) if returns_1d else 0
        avg_3d = sum(returns_3d) / len(returns_3d) if returns_3d else 0
        avg_5d = sum(returns_5d) / len(returns_5d) if returns_5d else 0

        # 브레드쓰 (상승 종목 비율)
        positive_1d = sum(1 for r in returns_1d if r > 0)
        breadth_1d = positive_1d / len(returns_1d) if returns_1d else 0

        positive_3d = sum(1 for _, m in matched if m.get("chg_3d", 0) > 0)
        breadth_3d = positive_3d / len(matched) if matched else 0

        # 가속도
        daily_avg_5d = avg_5d / 5 if avg_5d else 0
        acceleration = avg_1d - daily_avg_5d

        # TOP 모버
        movers = sorted(
            [(c, m.get("chg_1d", 0)) for c, m in matched],
            key=lambda x: x[1], reverse=True
        )
        top_movers = []
        for c, chg in movers[:5]:
            name = c
            info = uni.get(c, {})
            if isinstance(info, dict):
                name = info.get("name", c)
            top_movers.append({"code": c, "name": name, "chg_1d": round(chg, 2)})

        phase = _determine_phase(avg_1d, avg_3d, avg_5d, breadth_1d, acceleration)
        boost = _calc_boost_score(avg_1d, avg_3d, breadth_1d, acceleration, phase)

        ss = SectorScore(
            sector=tname,
            stock_count=len(matched),
            avg_return_1d=round(avg_1d, 2),
            avg_return_3d=round(avg_3d, 2),
            avg_return_5d=round(avg_5d, 2),
            breadth_1d=round(breadth_1d, 3),
            breadth_3d=round(breadth_3d, 3),
            acceleration=round(acceleration, 2),
            phase=phase,
            boost_score=round(boost, 1),
            top_movers=top_movers,
        )
        theme_scores.append(ss)

    # 순위
    theme_scores.sort(key=lambda s: s.avg_return_1d, reverse=True)
    for i, ss in enumerate(theme_scores):
        ss.rank = i + 1

    hot = [s.sector for s in theme_scores if s.phase in ("HOT", "WARMING")]
    cold = [s.sector for s in theme_scores if s.phase in ("COLD", "COOLING")]

    rot_parts = []
    for s in theme_scores:
        if s.phase == "HOT":
            rot_parts.append(f"HOT:{s.sector}({s.avg_return_1d:+.1f}%)")
        elif s.phase == "WARMING":
            rot_parts.append(f"WARMING:{s.sector}({s.avg_return_1d:+.1f}%)")
    rotation_signal = " | ".join(rot_parts[:10]) if rot_parts else "특이 테마 없음"

    result = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "market_return_1d": round(market_avg_1d, 2),
        "total_themes": len(theme_scores),
        "themes": [asdict(s) for s in theme_scores],
        "hot_themes": hot,
        "cold_themes": cold,
        "rotation_signal": rotation_signal,
    }

    # 저장
    try:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        tmp = THEME_MOMENTUM_PATH.with_suffix(".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        tmp.replace(THEME_MOMENTUM_PATH)
        logger.info(
            f"[테마모멘텀] 저장: {THEME_MOMENTUM_PATH.name} "
            f"({len(theme_scores)}테마, HOT {len(hot)}, COLD {len(cold)})"
        )
    except Exception as e:
        logger.error(f"[테마모멘텀] 저장 실패: {e}")

    return result


def get_theme_momentum_boost(theme_name: str) -> float:
    """특정 테마의 모멘텀 부스트 점수 반환.

    Returns: -15.0 ~ +25.0
    """
    if not THEME_MOMENTUM_PATH.exists():
        return 0.0
    try:
        data = json.loads(THEME_MOMENTUM_PATH.read_text("utf-8"))
        for s in data.get("themes", []):
            if s.get("sector") == theme_name:
                return s.get("boost_score", 0.0)
        return 0.0
    except Exception:
        return 0.0


def get_hot_theme_codes(top_n: int = 5) -> Dict[str, float]:
    """HOT/WARMING 테마 종목 코드 + 부스트 점수 반환.

    Returns: {code: boost_score}
    """
    if not THEME_MOMENTUM_PATH.exists():
        return {}
    try:
        data = json.loads(THEME_MOMENTUM_PATH.read_text("utf-8"))
        hot_themes = [
            s for s in data.get("themes", [])
            if s.get("phase") in ("HOT", "WARMING")
        ]
        hot_themes.sort(key=lambda s: s.get("boost_score", 0), reverse=True)

        result = {}
        for s in hot_themes[:top_n]:
            boost = s.get("boost_score", 0)
            for m in s.get("top_movers", []):
                code = m.get("code", "")
                if code and code not in result:
                    result[code] = boost
        return result
    except Exception:
        return {}


# ═══════════════════════════════════════════════════════
#  텔레그램 리포트
# ═══════════════════════════════════════════════════════

def format_telegram_report(report: SectorMomentumReport) -> str:
    """텔레그램용 섹터 모멘텀 보고서"""
    lines = [
        f"📊 섹터 모멘텀 ({report.timestamp})",
        f"시장 평균: {report.market_return_1d:+.2f}%",
        "",
    ]

    # HOT 섹터
    hot = [s for s in report.sectors if isinstance(s, dict) and s.get("phase") in ("HOT", "WARMING")]
    if hot:
        lines.append("🔥 강세 섹터:")
        for s in hot:
            phase_icon = "🔴" if s["phase"] == "HOT" else "🟠"
            movers = s.get("top_movers", [])[:3]
            mover_str = ", ".join(f"{m['name']}{m['chg_1d']:+.1f}%" for m in movers)
            lines.append(
                f"  {phase_icon} {s['sector']} {s['avg_return_1d']:+.1f}% "
                f"(BR {s['breadth_1d']:.0%}) [{mover_str}]"
            )
        lines.append("")

    # COLD 섹터
    cold = [s for s in report.sectors if isinstance(s, dict) and s.get("phase") in ("COLD", "COOLING")]
    if cold:
        lines.append("🧊 약세 섹터:")
        for s in cold[:3]:
            lines.append(f"  ❄️ {s['sector']} {s['avg_return_1d']:+.1f}% (BR {s['breadth_1d']:.0%})")
        lines.append("")

    # 전체 순위 (TOP 5 + BOTTOM 3)
    all_sectors = sorted(report.sectors, key=lambda s: s.get("avg_return_1d", 0) if isinstance(s, dict) else 0, reverse=True)
    lines.append("📈 섹터 순위:")
    for i, s in enumerate(all_sectors[:7]):
        if isinstance(s, dict):
            lines.append(f"  {i+1}. {s['sector']:6s} {s['avg_return_1d']:+.1f}% | BR {s['breadth_1d']:.0%} | {s['phase']}")
    lines.append("  ...")
    for s in all_sectors[-3:]:
        if isinstance(s, dict):
            lines.append(f"  {s.get('rank','-')}. {s['sector']:6s} {s['avg_return_1d']:+.1f}% | BR {s['breadth_1d']:.0%} | {s['phase']}")

    if report.rotation_signal:
        lines.append(f"\n🔄 로테이션: {report.rotation_signal}")

    return "\n".join(lines)


# ═══════════════════════════════════════════════════════
#  CLI
# ═══════════════════════════════════════════════════════

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(message)s")

    if "--theme" in sys.argv:
        # 테마별 모멘텀 분석
        result = analyze_theme_momentum()
        themes = result.get("themes", [])
        hot = [t for t in themes if t.get("phase") in ("HOT", "WARMING")]
        cold = [t for t in themes if t.get("phase") in ("COLD", "COOLING")]

        print(f"\n=== 테마 모멘텀 ({result.get('total_themes', 0)}개 테마) ===")
        print(f"시장 평균: {result.get('market_return_1d', 0):+.2f}%\n")

        if hot:
            print("🔥 HOT/WARMING 테마:")
            for t in hot[:10]:
                movers = t.get("top_movers", [])[:3]
                mover_str = ", ".join(f"{m['name']}" for m in movers)
                print(f"  {t['phase']:8s} {t['sector']:12s} "
                      f"{t['avg_return_1d']:+.1f}% BR {t['breadth_1d']:.0%} "
                      f"[{mover_str}]")
        if cold:
            print("\n🧊 COLD/COOLING 테마:")
            for t in cold[:5]:
                print(f"  {t['phase']:8s} {t['sector']:12s} "
                      f"{t['avg_return_1d']:+.1f}% BR {t['breadth_1d']:.0%}")
    elif "--boost" in sys.argv:
        report = analyze_sectors()
        print("\n=== 종목별 섹터 부스트 점수 ===")
        hot_codes = get_hot_sector_codes(top_n=5)
        for code, boost in sorted(hot_codes.items(), key=lambda x: -x[1])[:20]:
            print(f"  {code}: +{boost:.1f}점")
    else:
        report = analyze_sectors()
        print(format_telegram_report(report))
