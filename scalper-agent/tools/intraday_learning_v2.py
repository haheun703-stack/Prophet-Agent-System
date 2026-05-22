# -*- coding: utf-8 -*-
"""장중 실시간 학습 v2 — 풀세트 D 12종 데이터 통합 (사장님 5/22 09:35 명령).

사장님 발화 (5/22 09:15):
> "5분봉으로 장중에 잘보고 학습 / +10% 이상, 상한가까지 가는 종목들
>  패턴, 수급, 체결강도, 호가 등등 진짜 제대로 학습"

12종 데이터:
  ① 5분봉 OHLCV  ② 수급(외인/기관)  ③ 체결강도  ④ 호가 10단계
  ⑤ 거래원/창구  ⑥ VWAP  ⑦ 상한가 직전 패턴
  ⑧ 섹터 동조  ⑨ 공시/뉴스
  ⑩ 매물대  ⑪ ATR  ⑫ 시간대별 패턴

작동 방식 (intraday_learning.py v1 후속):
  - VPS cron: 매 30분 (09:00~15:30) 평일 (기존 cron 유지)
  - 저장: data_store/learning_v2_YYYYMMDD/{type}_HHMM.json
  - 동적 발굴: 시장 전체 +5%+ 급등주 + 보유 종목

영구 메모리:
  [[project_5_22_evening_learning_fix]] — 풀세트 D
  [[feedback_self_monitoring_realtime_5_22]] — 단타봇 자율 모니터링
"""
import argparse
import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

try:
    from zoneinfo import ZoneInfo
    KST = ZoneInfo("Asia/Seoul")
except ImportError:
    KST = timezone.utc

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))

from dotenv import load_dotenv
load_dotenv(_ROOT.parent / ".env")

logger = logging.getLogger("BH.LearnerV2")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)


# ── 동적 발굴 임계값 ─────────────────────────
GAINER_MIN_PCT = 5.0          # +5%+ 급등주 (사장님 명령 "+10%" 보다 보수적으로 포함)
LIMIT_UP_THRESHOLD_PCT = 25.0  # +25%+ 상한가 임박
SCAN_TOP_N = 200               # universe 스캔 상위
MIN_MARKET_CAP_BILLION = 100   # 시총 100억+ 필터


def _data_dir() -> Path:
    today = datetime.now(KST).strftime("%Y%m%d")
    d = _ROOT / "data_store" / f"learning_v2_{today}"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _now_hhmm() -> str:
    return datetime.now(KST).strftime("%H%M")


# ═════════════════════════════════════════════
# 1) 동적 발굴 — 시장 전체 +5%+ 종목 스캔
# ═════════════════════════════════════════════
def discover_market_gainers(trader, universe: Dict[str, Dict]) -> List[Dict]:
    """+5%+ 급등주 + 상한가 임박 동적 발굴.

    Returns:
        [{code, name, sector, change_rate, current_price, category}]
        category: "limit_up_imminent" / "gainer"
    """
    gainers = []
    scanned = 0

    for code, info in list(universe.items())[:SCAN_TOP_N]:
        if not isinstance(info, dict):
            continue
        scanned += 1
        try:
            p = trader.fetch_price(code)
            if not p or not p.get("success"):
                continue

            ch = float(p.get("change_rate", 0))
            if ch < GAINER_MIN_PCT:
                continue

            category = "limit_up_imminent" if ch >= LIMIT_UP_THRESHOLD_PCT else "gainer"
            gainers.append({
                "code": code,
                "name": info.get("name", code),
                "sector": info.get("sector", ""),
                "change_rate": ch,
                "current_price": p.get("current_price", 0),
                "volume": p.get("volume", 0),
                "category": category,
            })
        except Exception as e:
            logger.debug(f"가격 조회 실패 {code}: {e}")

    logger.info(f"동적 발굴: {len(gainers)}종목 (+{GAINER_MIN_PCT}%+) / 스캔 {scanned}")
    return gainers


# ═════════════════════════════════════════════
# 2) 핵심 API 수집 — 5분봉 / 호가 / 거래원
# ═════════════════════════════════════════════
def collect_five_min_bars(trader, code: str, n_bars: int = 30) -> List[Dict]:
    """5분봉 OHLCV 수집 (KIS inquire_time_itemchartprice).

    Returns:
        [{time, open, high, low, close, volume}] 시간순 (최근 n_bars개)

    Note:
        KIS 분봉 API 호출. 단타봇 fetch_minute_chart 메서드 호출.
        구현 실패 시 빈 리스트 반환 (best-effort).
    """
    try:
        if hasattr(trader, "fetch_minute_chart"):
            bars = trader.fetch_minute_chart(code, minutes=5, n=n_bars)
            return bars or []
        # fallback: 단타봇에 5분봉 메서드 없으면 현재가 1개만 반환
        p = trader.fetch_price(code)
        if p and p.get("success"):
            return [{
                "time": _now_hhmm(),
                "open": p.get("open", 0),
                "high": p.get("high", 0),
                "low": p.get("low", 0),
                "close": p.get("current_price", 0),
                "volume": p.get("volume", 0),
            }]
    except Exception as e:
        logger.warning(f"5분봉 수집 실패 {code}: {e}")
    return []


def collect_orderbook(trader, code: str) -> Optional[Dict]:
    """호가 10단계 수집 (KIS inquire_asking_price_exp_ccn).

    Returns:
        {asks: [{price, qty} × 10], bids: [{price, qty} × 10],
         total_ask_qty, total_bid_qty, bid_ask_ratio}
    """
    try:
        if hasattr(trader, "fetch_orderbook"):
            ob = trader.fetch_orderbook(code)
            if ob and ob.get("success"):
                return ob
    except Exception as e:
        logger.warning(f"호가 수집 실패 {code}: {e}")
    return None


def collect_broker_flow(trader, code: str) -> Optional[Dict]:
    """거래원/창구 매매 수집 (KIS 거래원 API).

    외국계 (모건/JP모건/CS/UBS) vs 국내 (삼성/미래/한투/NH) 매수 추적.

    Returns:
        {foreign_brokers: [...], domestic_brokers: [...], top_buyer, top_seller}
    """
    try:
        if hasattr(trader, "fetch_broker_flow"):
            bf = trader.fetch_broker_flow(code)
            if bf and bf.get("success"):
                return bf
    except Exception as e:
        logger.warning(f"거래원 수집 실패 {code}: {e}")
    return None


# ═════════════════════════════════════════════
# 3) Supabase 연동 — 수급 / 공시/뉴스
# ═════════════════════════════════════════════
def collect_supply_demand(code: str) -> Optional[Dict]:
    """정보봇 intelligence_supply_demand 5분봉 외인/기관 수급."""
    try:
        from utils.supabase_sql import query_one
        sql = """
            SELECT * FROM intelligence_supply_demand
            WHERE ticker = %s AND date = CURRENT_DATE
            ORDER BY created_at DESC LIMIT 1
        """
        row = query_one(sql, (code,))
        return row
    except Exception as e:
        logger.warning(f"수급 조회 실패 {code}: {e}")
    return None


def collect_news_trigger(code: str) -> Optional[Dict]:
    """오늘 공시/뉴스 (정보봇 BAD_NEWS / GOOD_NEWS / DART)."""
    try:
        from utils.supabase_sql import query
        # 정보봇 뉴스 + DART 공시 (있다면)
        sql = """
            SELECT 'news' AS source, headline, sentiment, created_at
            FROM intelligence_news
            WHERE ticker = %s AND DATE(created_at) = CURRENT_DATE
            ORDER BY created_at DESC LIMIT 5
        """
        rows = query(sql, (code,))
        return {"items": rows or [], "count": len(rows or [])}
    except Exception as e:
        logger.debug(f"뉴스 조회 실패 {code}: {e}")
    return None


# ═════════════════════════════════════════════
# 4) 파생 데이터 (5분봉에서 계산)
# ═════════════════════════════════════════════
def derive_vwap(bars: Sequence[Dict]) -> Optional[float]:
    """VWAP (Volume Weighted Average Price) — 5분봉 누적."""
    if not bars:
        return None
    total_pv = sum(((b["high"] + b["low"] + b["close"]) / 3.0) * b["volume"] for b in bars)
    total_v = sum(b["volume"] for b in bars)
    if total_v <= 0:
        return None
    return round(total_pv / total_v, 2)


def derive_atr(bars: Sequence[Dict], period: int = 14) -> Optional[float]:
    """ATR (Average True Range) — 5분봉 변동성 지표."""
    if len(bars) < period + 1:
        return None
    trs = []
    for i in range(1, len(bars)):
        prev_close = bars[i - 1]["close"]
        hi = bars[i]["high"]
        lo = bars[i]["low"]
        tr = max(hi - lo, abs(hi - prev_close), abs(lo - prev_close))
        trs.append(tr)
    if len(trs) < period:
        return None
    return round(sum(trs[-period:]) / period, 2)


def derive_volume_profile(bars: Sequence[Dict], buckets: int = 10) -> Dict:
    """매물대 — 가격대별 거래량 분포 → 저항/지지 추정."""
    if not bars:
        return {}
    prices = [b["close"] for b in bars]
    lo, hi = min(prices), max(prices)
    if hi - lo <= 0:
        return {}
    step = (hi - lo) / buckets
    profile = [{"price_lo": lo + i * step, "price_hi": lo + (i + 1) * step, "volume": 0}
               for i in range(buckets)]
    for b in bars:
        idx = min(int((b["close"] - lo) / step), buckets - 1)
        profile[idx]["volume"] += b["volume"]
    # 매물대 TOP 3 (저항/지지)
    top = sorted(profile, key=lambda x: -x["volume"])[:3]
    return {"profile": profile, "top_3_resistance": top}


def derive_limit_up_journey(bars: Sequence[Dict], base_price: float) -> Dict:
    """상한가 직전 패턴 — +5/+10/+15/+20/상한가 각 단계 도달 시간."""
    if not bars or base_price <= 0:
        return {}
    journey = {}
    thresholds = [5, 10, 15, 20, 29.5]  # 상한가 ~+29.5%
    for th in thresholds:
        target = base_price * (1 + th / 100)
        for b in bars:
            if b["high"] >= target:
                journey[f"reached_+{th}%"] = b.get("time", "?")
                break
    journey["max_reached_pct"] = round((max(b["high"] for b in bars) / base_price - 1) * 100, 2)
    return journey


def derive_time_pattern(bars: Sequence[Dict]) -> Dict:
    """시간대별 패턴 — 09:00~9:30 / 13:00~14:30 / 14:30~15:30 거래량/변동."""
    buckets = {
        "morning": {"vol": 0, "range": 0.0, "candles": 0},   # ~09:30
        "midday":  {"vol": 0, "range": 0.0, "candles": 0},   # 09:30~14:30
        "closing": {"vol": 0, "range": 0.0, "candles": 0},   # 14:30~
    }
    for b in bars:
        t = str(b.get("time", "")).zfill(4)
        if not t.isdigit():
            continue
        hhmm = int(t)
        if hhmm < 930:
            bucket = "morning"
        elif hhmm < 1430:
            bucket = "midday"
        else:
            bucket = "closing"
        buckets[bucket]["vol"] += b["volume"]
        buckets[bucket]["range"] += b["high"] - b["low"]
        buckets[bucket]["candles"] += 1
    return buckets


# ═════════════════════════════════════════════
# 5) 섹터 동조 (기존 sector_concurrent_surge.json 활용)
# ═════════════════════════════════════════════
def derive_sector_concurrent(sector: str) -> Optional[Dict]:
    """같은 섹터 N종목 동시 급등 강도."""
    try:
        path = _ROOT / "data_store" / "sector_concurrent_surge.json"
        if not path.exists():
            return None
        data = json.loads(path.read_text(encoding="utf-8"))
        sectors = data.get("sectors", {}) if isinstance(data, dict) else {}
        return sectors.get(sector)
    except Exception as e:
        logger.debug(f"섹터 동조 조회 실패 {sector}: {e}")
    return None


# ═════════════════════════════════════════════
# 6) 체결강도 (기존 flow_intensity.json 활용)
# ═════════════════════════════════════════════
def collect_flow_intensity(code: str) -> Optional[Dict]:
    """체결강도 — data_store/flow_intensity.json 에서 종목별 조회."""
    try:
        path = _ROOT / "data_store" / "flow_intensity.json"
        if not path.exists():
            return None
        data = json.loads(path.read_text(encoding="utf-8"))
        items = data.get("items", []) if isinstance(data, dict) else []
        for item in items:
            if str(item.get("code")) == code:
                return item
    except Exception as e:
        logger.debug(f"체결강도 조회 실패 {code}: {e}")
    return None


# ═════════════════════════════════════════════
# 7) 통합 스냅샷 — 12종 데이터 한 번에 수집
# ═════════════════════════════════════════════
def snapshot_full(trader, targets: List[Dict]) -> Dict:
    """타겟 종목들의 12종 데이터 통합 스냅샷.

    Args:
        trader: KISTrader 인스턴스
        targets: [{code, name, sector, ...}] 발굴된 종목 리스트

    Returns:
        {timestamp, time_kst, snapshots: {code: {12종 데이터}}}
    """
    now = datetime.now(KST)
    snapshot = {
        "timestamp": now.isoformat(),
        "time_kst": now.strftime("%H:%M:%S"),
        "snapshots": {},
    }

    for tgt in targets:
        code = tgt["code"]
        name = tgt.get("name", code)
        sector = tgt.get("sector", "")
        base_price = tgt.get("current_price", 0)

        try:
            bars = collect_five_min_bars(trader, code, n_bars=30)
            orderbook = collect_orderbook(trader, code)
            broker = collect_broker_flow(trader, code)
            supply = collect_supply_demand(code)
            news = collect_news_trigger(code)
            flow = collect_flow_intensity(code)

            snapshot["snapshots"][code] = {
                "name": name,
                "sector": sector,
                "base_info": tgt,
                # ① 5분봉
                "five_min_bars": bars,
                # ② 수급
                "supply_demand": supply,
                # ③ 체결강도
                "flow_intensity": flow,
                # ④ 호가
                "orderbook": orderbook,
                # ⑤ 거래원
                "broker_flow": broker,
                # ⑥ VWAP (5분봉 파생)
                "vwap": derive_vwap(bars),
                # ⑦ 상한가 직전 패턴 (5분봉 파생)
                "limit_up_journey": derive_limit_up_journey(bars, base_price),
                # ⑧ 섹터 동조
                "sector_concurrent": derive_sector_concurrent(sector),
                # ⑨ 공시/뉴스
                "news_trigger": news,
                # ⑩ 매물대 (5분봉 파생)
                "volume_profile": derive_volume_profile(bars),
                # ⑪ ATR (5분봉 파생)
                "atr": derive_atr(bars),
                # ⑫ 시간대별 (5분봉 파생)
                "time_pattern": derive_time_pattern(bars),
            }
        except Exception as e:
            logger.error(f"스냅샷 실패 {code} {name}: {e}")

    return snapshot


# ═════════════════════════════════════════════
# 8) 메인 실행 (cron 호출)
# ═════════════════════════════════════════════
def main(once: bool = True) -> int:
    """매 30분 cron 호출 — 12종 데이터 통합 수집."""
    try:
        from bot.kis_trader import KISTrader
    except Exception as e:
        logger.error(f"KISTrader 로드 실패: {e}")
        return 1

    trader = KISTrader()

    # universe 로드
    universe_path = _ROOT / "data_store" / "universe.json"
    if not universe_path.exists():
        logger.warning("universe.json 없음 — 동적 발굴 스킵")
        universe = {}
    else:
        universe = json.loads(universe_path.read_text(encoding="utf-8"))

    # 1) 동적 발굴
    gainers = discover_market_gainers(trader, universe)

    # 2) 보유 종목 추가
    try:
        b = trader.fetch_balance()
        for p in b.get("positions", []):
            code = str(p.get("code", ""))
            if not code or any(g["code"] == code for g in gainers):
                continue
            gainers.append({
                "code": code,
                "name": p.get("name", code),
                "sector": "",
                "change_rate": p.get("pnl_rate", 0),
                "current_price": p.get("current_price", 0),
                "category": "holding",
            })
    except Exception as e:
        logger.warning(f"보유 종목 추가 실패: {e}")

    # 3) 12종 데이터 통합 스냅샷
    snapshot = snapshot_full(trader, gainers)

    # 4) 저장
    hhmm = _now_hhmm()
    out_path = _data_dir() / f"snapshot_v2_{hhmm}.json"
    out_path.write_text(json.dumps(snapshot, ensure_ascii=False, indent=2, default=str),
                        encoding="utf-8")
    logger.info(f"학습 v2 스냅샷 저장: {out_path.name} ({len(gainers)}종목)")

    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="장중 학습 v2 (풀세트 D)")
    parser.add_argument("--once", action="store_true", help="1회 실행 (cron 호출)")
    args = parser.parse_args()
    sys.exit(main(once=args.once))
