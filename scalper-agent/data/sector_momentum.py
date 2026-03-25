# -*- coding: utf-8 -*-
"""
섹터 모멘텀 분석기 (Sector Momentum Analyzer)
==============================================
전체 23개 섹터의 실시간 모멘텀을 pykrx 직접 조회로 감지.
rotation_detector.py가 릴레이에 의존하는 반면,
이 모듈은 독립적으로 전체 시장 섹터를 분석한다.

핵심:
1. pykrx get_market_price_change로 전체 종목 등락률 일괄 조회 (1회 호출)
2. universe.json 섹터별 그룹핑 → 섹터 평균 수익률
3. 섹터 가속도 감지 (1D vs 5D 대비 급변)
4. morning_recommendation에 섹터 부스트 점수 공급

Usage:
    python data/sector_momentum.py          # 오늘 섹터 모멘텀 분석
    python data/sector_momentum.py --boost  # 종목별 부스트 점수 출력
"""

import json
import logging
import time
from pathlib import Path
from datetime import datetime, timedelta
from dataclasses import dataclass, field, asdict
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger("BH.SectorMomentum")

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data_store"
UNIVERSE_PATH = DATA_DIR / "universe.json"
SECTOR_MOMENTUM_PATH = DATA_DIR / "sector_momentum.json"
SECTOR_HISTORY_DIR = DATA_DIR / "sector_momentum_history"


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
#  pykrx 일괄 조회 — 핵심 데이터 수집
# ═══════════════════════════════════════════════════════

def _fetch_market_returns(n_days: int = 5) -> Dict[str, Dict]:
    """pykrx로 전체 시장 종목별 등락률 수집

    Returns: {code: {"chg_1d": float, "chg_3d": float, "chg_5d": float, "vol_ratio": float}}
    """
    from pykrx import stock
    import warnings
    warnings.filterwarnings("ignore")

    end = datetime.now()
    # 거래일 여유 확보 (공휴일 감안 +7일)
    start = end - timedelta(days=n_days + 10)
    end_s = end.strftime("%Y%m%d")
    start_s = start.strftime("%Y%m%d")

    all_data: Dict[str, Dict] = {}

    for market in ["KOSPI", "KOSDAQ"]:
        try:
            # 전체 종목 기간 등락률 (1회 호출로 전체 종목 커버)
            df = stock.get_market_price_change(start_s, end_s, market=market)
            if df is None or len(df) == 0:
                continue

            for ticker in df.index:
                code = str(ticker)
                row = df.loc[ticker]
                # 컬럼: 시가, 종가, 변동폭, 등락률, 거래량, 거래대금
                chg_total = float(row.get("등락률", 0))
                close = float(row.get("종가", 0))
                open_p = float(row.get("시가", 0))
                volume = float(row.get("거래량", 0))

                all_data[code] = {
                    "chg_period": chg_total,
                    "close": close,
                    "volume": volume,
                }
        except Exception as e:
            logger.warning(f"pykrx {market} 조회 실패: {e}")

    # 1일/3일/5일 개별 조회 (get_market_price_change는 기간 합산이므로)
    # 1일 등락률은 별도 조회
    result: Dict[str, Dict] = {}
    try:
        for market in ["KOSPI", "KOSDAQ"]:
            df_today = stock.get_market_ohlcv_by_ticker(end_s, market=market)
            if df_today is None or len(df_today) == 0:
                # 오늘 데이터 없으면 어제 시도
                yesterday = (end - timedelta(days=1)).strftime("%Y%m%d")
                df_today = stock.get_market_ohlcv_by_ticker(yesterday, market=market)
            if df_today is None or len(df_today) == 0:
                continue

            for ticker in df_today.index:
                code = str(ticker)
                row = df_today.loc[ticker]
                chg_1d = float(row.get("등락률", 0))
                vol = float(row.get("거래량", 0))
                close = float(row.get("종가", 0))
                result[code] = {
                    "chg_1d": chg_1d,
                    "close": close,
                    "volume": vol,
                }
            time.sleep(0.3)
    except Exception as e:
        logger.warning(f"1일 등락률 조회 실패: {e}")

    # 3일/5일은 기간 price_change로 계산
    for label, days_back in [("chg_3d", 3), ("chg_5d", 5)]:
        try:
            s = (end - timedelta(days=days_back + 5)).strftime("%Y%m%d")
            for market in ["KOSPI", "KOSDAQ"]:
                df_nd = stock.get_market_price_change(s, end_s, market=market)
                if df_nd is None or len(df_nd) == 0:
                    continue
                for ticker in df_nd.index:
                    code = str(ticker)
                    if code in result:
                        result[code][label] = float(df_nd.loc[ticker].get("등락률", 0))
                    else:
                        result[code] = {
                            "chg_1d": 0,
                            label: float(df_nd.loc[ticker].get("등락률", 0)),
                            "close": float(df_nd.loc[ticker].get("종가", 0)),
                            "volume": 0,
                        }
                time.sleep(0.3)
        except Exception as e:
            logger.warning(f"{label} 조회 실패: {e}")

    return result


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
        logger.info("pykrx 전체 시장 데이터 조회 중...")
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
    import sys
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(message)s")

    report = analyze_sectors()

    if "--boost" in sys.argv:
        print("\n=== 종목별 섹터 부스트 점수 ===")
        hot_codes = get_hot_sector_codes(top_n=5)
        for code, boost in sorted(hot_codes.items(), key=lambda x: -x[1])[:20]:
            print(f"  {code}: +{boost:.1f}점")
    else:
        print(format_telegram_report(report))
