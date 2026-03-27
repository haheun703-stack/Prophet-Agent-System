# -*- coding: utf-8 -*-
"""
ETF 유니버스 정의 + 데이터 수집
================================
3카테고리 ~30종목:
  A. 방향성 (인버스/레버리지/지수)
  B. 원자재 (금/은/원유/구리)
  C. 섹터/테마 (방산/반도체/2차전지 등)

수집: pykrx → data_store/etf_daily/{code}.csv
"""

import json
import logging
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from pathlib import Path

logger = logging.getLogger(__name__)

DATA_DIR = Path(__file__).resolve().parent.parent / "data_store"
ETF_DAILY_DIR = DATA_DIR / "etf_daily"

# ═════════════════════════════════════════════════════════════
# ETF 유니버스 정의
# ═════════════════════════════════════════════════════════════

ETF_UNIVERSE = {
    # ── A. 방향성 ETF (인버스/레버리지/지수) ──
    "directional": {
        "069500": {"name": "KODEX 200", "type": "index", "mult": 1, "underlying": "KOSPI200"},
        "229200": {"name": "KODEX 코스닥150", "type": "index", "mult": 1, "underlying": "KOSDAQ150"},
        "122630": {"name": "KODEX 레버리지", "type": "leverage", "mult": 2, "underlying": "KOSPI200"},
        "233740": {"name": "KODEX 코스닥150레버리지", "type": "leverage", "mult": 2, "underlying": "KOSDAQ150"},
        "114800": {"name": "KODEX 인버스", "type": "inverse", "mult": -1, "underlying": "KOSPI200"},
        "252670": {"name": "KODEX 200선물인버스2X", "type": "inverse", "mult": -2, "underlying": "KOSPI200"},
    },

    # ── B. 원자재 ETF ──
    "commodity": {
        "132030": {"name": "KODEX 골드선물(H)", "type": "commodity", "underlying": "GC=F", "sector": "gold"},
        "411060": {"name": "ACE KRX금현물", "type": "commodity", "underlying": "gold_spot", "sector": "gold"},
        "261220": {"name": "KODEX WTI원유선물(H)", "type": "commodity", "underlying": "CL=F", "sector": "oil"},
        "130680": {"name": "TIGER 원유선물Enhanced(H)", "type": "commodity", "underlying": "CL=F", "sector": "oil"},
        "139320": {"name": "TIGER 금속선물(H)", "type": "commodity", "underlying": "HG=F", "sector": "metals"},
    },

    # ── C. 섹터/테마 ETF ──
    "sector": {
        "449450": {"name": "PLUS K방산", "type": "sector", "sector_key": "space_defense"},
        "091160": {"name": "KODEX 반도체", "type": "sector", "sector_key": "semiconductor"},
        "305720": {"name": "KODEX 2차전지산업", "type": "sector", "sector_key": "battery_ev"},
        "244580": {"name": "KODEX 바이오", "type": "sector", "sector_key": "bio_health"},
        "117700": {"name": "KODEX 건설", "type": "sector", "sector_key": "construction"},
        "139230": {"name": "TIGER 조선TOP10", "type": "sector", "sector_key": "shipbuilding"},
        "091180": {"name": "KODEX 자동차", "type": "sector", "sector_key": "auto"},
        "266370": {"name": "KODEX IT", "type": "sector", "sector_key": "it_sw"},
        "140700": {"name": "KODEX 보험", "type": "sector", "sector_key": "finance"},
        "102780": {"name": "KODEX 삼성그룹", "type": "sector", "sector_key": "samsung"},
    },
}

# ── 원자재 릴레이 → ETF 매핑 ──
COMMODITY_TO_ETF = {
    "gold_up":   ["132030", "411060"],
    "oil_up":    ["261220", "130680"],
    "silver_up": ["139320"],
    "copper_up": ["139320"],
    "ng_up":     [],  # NG 전용 ETF 없음 → 개별주 릴레이만
}


def get_all_etf_codes() -> list[str]:
    """전체 ETF 유니버스 코드 목록"""
    codes = []
    for cat in ETF_UNIVERSE.values():
        codes.extend(cat.keys())
    return codes


def get_etf_info(code: str) -> dict | None:
    """ETF 코드로 정보 조회"""
    for cat_name, cat in ETF_UNIVERSE.items():
        if code in cat:
            info = cat[code].copy()
            info["category"] = cat_name
            return info
    return None


def get_etfs_by_category(category: str) -> dict:
    """카테고리별 ETF 목록"""
    return ETF_UNIVERSE.get(category, {})


def get_commodity_etfs_for_signal(signal: str) -> list[dict]:
    """원자재 릴레이 시그널 → 해당 ETF 정보 리스트"""
    codes = COMMODITY_TO_ETF.get(signal, [])
    result = []
    for code in codes:
        info = get_etf_info(code)
        if info:
            info["code"] = code
            result.append(info)
    return result


# ═════════════════════════════════════════════════════════════
# ETF 데이터 수집
# ═════════════════════════════════════════════════════════════

def collect_etf_daily(codes: list[str] = None, months: int = 12, force: bool = False) -> int:
    """pykrx로 ETF 일봉 수집 → data_store/etf_daily/{code}.csv

    Args:
        codes: 수집할 ETF 코드 목록. None이면 전체 유니버스.
        months: 수집 기간 (월)
        force: True면 기존 데이터 무시하고 재수집

    Returns: 성공 수집 종목 수
    """
    from pykrx import stock
    import pandas as pd

    ETF_DAILY_DIR.mkdir(parents=True, exist_ok=True)

    if codes is None:
        codes = get_all_etf_codes()

    end_dt = datetime.now().strftime("%Y%m%d")
    start_dt = (datetime.now() - timedelta(days=months * 30)).strftime("%Y%m%d")
    today_str = datetime.now().strftime("%Y-%m-%d")

    collected = 0
    for code in codes:
        csv_path = ETF_DAILY_DIR / f"{code}.csv"

        # 이미 오늘 수집했으면 스킵 (force가 아닐 때)
        if not force and csv_path.exists():
            try:
                df_exist = pd.read_csv(csv_path, index_col=0)
                if len(df_exist) > 0 and str(df_exist.index[-1])[:10] >= today_str:
                    collected += 1
                    continue
            except Exception:
                pass

        try:
            # ETF도 KRX 상장이므로 get_market_ohlcv_by_date 사용
            df = stock.get_market_ohlcv_by_date(start_dt, end_dt, code)
            if df is None or df.empty:
                logger.warning(f"[ETF] {code} 데이터 없음")
                continue

            # 컬럼 표준화 (pykrx 한글 컬럼 → 영문)
            col_map = {}
            for c in df.columns:
                cs = str(c)
                if "시가" in cs:
                    col_map[c] = "open"
                elif "고가" in cs:
                    col_map[c] = "high"
                elif "저가" in cs:
                    col_map[c] = "low"
                elif "종가" in cs:
                    col_map[c] = "close"
                elif "거래량" in cs:
                    col_map[c] = "volume"
            if col_map:
                df = df.rename(columns=col_map)

            needed = ["open", "high", "low", "close", "volume"]
            available = [c for c in needed if c in df.columns]
            if len(available) < 4:
                logger.warning(f"[ETF] {code} 필수 컬럼 부족: {df.columns.tolist()}")
                continue

            df = df[available]

            # 기존 데이터와 병합
            if csv_path.exists() and not force:
                try:
                    df_old = pd.read_csv(csv_path, index_col=0, parse_dates=True)
                    combined = pd.concat([df_old, df])
                    combined = combined[~combined.index.duplicated(keep="last")]
                    combined = combined.sort_index()
                    df = combined
                except Exception:
                    pass

            # 원자적 쓰기
            tmp_path = csv_path.with_suffix(".tmp")
            df.to_csv(tmp_path)
            tmp_path.replace(csv_path)
            collected += 1

            # pykrx rate limit 방지
            time.sleep(0.3)

        except Exception as e:
            logger.warning(f"[ETF] {code} 수집 실패: {e}")
            time.sleep(0.5)

    logger.info(f"[ETF] 일봉 수집 완료: {collected}/{len(codes)}종목")
    return collected


def get_etf_snapshot(code: str) -> dict | None:
    """ETF 실시간 스냅샷 (종가/NAV/거래량)

    Returns: {close, volume, change_pct} or None
    """
    import pandas as pd

    csv_path = ETF_DAILY_DIR / f"{code}.csv"
    if not csv_path.exists():
        return None

    try:
        df = pd.read_csv(csv_path, index_col=0)
        if df.empty:
            return None

        last = df.iloc[-1]
        prev = df.iloc[-2] if len(df) > 1 else last

        close = int(last.get("close", 0))
        prev_close = int(prev.get("close", 0))
        change_pct = ((close - prev_close) / prev_close * 100) if prev_close else 0

        return {
            "code": code,
            "close": close,
            "volume": int(last.get("volume", 0)),
            "change_pct": round(change_pct, 2),
            "date": str(df.index[-1])[:10],
        }
    except Exception as e:
        logger.warning(f"[ETF] {code} 스냅샷 실패: {e}")
        return None


def get_etf_atr(code: str, period: int = 14) -> float | None:
    """ETF ATR (Average True Range) 계산 — SL/TP 산정에 사용"""
    import pandas as pd

    csv_path = ETF_DAILY_DIR / f"{code}.csv"
    if not csv_path.exists():
        return None

    try:
        df = pd.read_csv(csv_path, index_col=0)
        if len(df) < period + 1:
            return None

        df = df.tail(period + 1)
        highs = df["high"].values
        lows = df["low"].values
        closes = df["close"].values

        trs = []
        for i in range(1, len(df)):
            tr = max(
                highs[i] - lows[i],
                abs(highs[i] - closes[i - 1]),
                abs(lows[i] - closes[i - 1]),
            )
            trs.append(tr)

        return sum(trs) / len(trs) if trs else None
    except Exception as e:
        logger.warning(f"[ETF] {code} ATR 계산 실패: {e}")
        return None
