"""15:05 프리클로즈 상한가 임박 알림.

장 마감 전(15:05) surge_universe + watchlist + history 종목을 KIS API로 스캔하여
+25%+ 상승 중인 종목을 감지. 상한가 이력(2회+) 교차검증 후 Telegram 전송.

스케줄: 15:05 KST (bomb_buy_alert 15:00 이후 5분)
소요: ~15초 (286종목 × 0.05s)
"""
import json
import logging
import time
import asyncio
from pathlib import Path
from datetime import datetime, timedelta
from collections import defaultdict

logger = logging.getLogger("BH.PreCloseLimitUp")

SCALPER_DIR = Path(__file__).resolve().parent.parent
DATA_STORE = SCALPER_DIR / "data_store"
LIMIT_UP_DIR = DATA_STORE / "limit_up"

# 상한가 임박 판단 기준
# Why: 25% 이상은 거의 다 상한가 도달/근접(+29.9%~+30%) → 매수호가 잠겨 매수 불가.
# 18%로 낮춰서 매수 가능한 강세 종목(+18~28%) 포함. 15:30 마감 전 진입 목적.
DEFAULT_THRESHOLD_PCT = 18.0


def _build_scan_universe() -> list[dict]:
    """3개 소스에서 스캔 대상 유니버스 구축 (중복 제거).

    Sources:
      1. surge_universe.json  — 최근 급등 종목
      2. watchlist.json       — 상한가 워치리스트
      3. history.json         — 상한가 이력 2회+ 종목

    Returns:
        [{"code": "...", "name": "..."}, ...]
    """
    universe = {}  # code → name

    # 1) surge_universe
    su_path = LIMIT_UP_DIR / "surge_universe.json"
    if su_path.exists():
        try:
            su = json.loads(su_path.read_text(encoding="utf-8"))
            for code, info in su.get("stocks", {}).items():
                universe[code] = info.get("name", code)
        except Exception as e:
            logger.warning(f"surge_universe 로드 실패: {e}")

    # 2) watchlist
    wl_path = LIMIT_UP_DIR / "watchlist.json"
    if wl_path.exists():
        try:
            wl = json.loads(wl_path.read_text(encoding="utf-8"))
            for item in wl.get("items", []):
                code = item.get("code", "")
                if code:
                    universe[code] = item.get("name", code)
        except Exception as e:
            logger.warning(f"watchlist 로드 실패: {e}")

    # 3) history (상한가 이력 2회+ 종목만)
    hist_path = LIMIT_UP_DIR / "history.json"
    if hist_path.exists():
        try:
            hist = json.loads(hist_path.read_text(encoding="utf-8"))
            code_counts = defaultdict(list)
            for ev in hist.get("events", []):
                code_counts[ev["code"]].append(ev)
            for code, events in code_counts.items():
                if len(events) >= 2:
                    universe[code] = events[0].get("name", code)
        except Exception as e:
            logger.warning(f"history 로드 실패: {e}")

    result = [{"code": c, "name": n} for c, n in universe.items()]
    logger.info(f"[프리클로즈] 스캔 유니버스: {len(result)}종목")
    return result


def _build_limit_history_map() -> dict:
    """history.json에서 종목별 상한가 이력 맵 구축.

    Returns:
        {code: {"limit_count": N, "last_date": "YYYY-MM-DD", "is_qualified": bool}}
    """
    hist_path = LIMIT_UP_DIR / "history.json"
    if not hist_path.exists():
        return {}

    try:
        hist = json.loads(hist_path.read_text(encoding="utf-8"))
    except Exception:
        return {}

    code_events = defaultdict(list)
    for ev in hist.get("events", []):
        code_events[ev["code"]].append(ev.get("limit_date", ""))

    cutoff = (datetime.now() - timedelta(days=15)).strftime("%Y-%m-%d")
    result = {}
    for code, dates in code_events.items():
        dates_sorted = sorted(dates, reverse=True)
        last_date = dates_sorted[0] if dates_sorted else ""
        count = len(dates_sorted)
        is_qualified = count >= 2 and last_date >= cutoff
        result[code] = {
            "limit_count": count,
            "last_date": last_date,
            "is_qualified": is_qualified,
        }
    return result


def _scan_prices(kis_trader, universe: list[dict], threshold: float) -> list[dict]:
    """KIS API로 유니버스 종목 현재가 조회 (동기, blocking).

    Args:
        kis_trader: KISTrader 인스턴스
        universe: [{"code", "name"}, ...]
        threshold: 등락률 필터 기준 (%)

    Returns:
        threshold 이상 종목 리스트
    """
    near_limit = []
    fail_count = 0

    for stock in universe:
        try:
            price = kis_trader.fetch_price(stock["code"])
            if not price.get("success"):
                fail_count += 1
                continue

            change_rate = price.get("change_rate", 0)
            if change_rate >= threshold:
                near_limit.append({
                    "code": stock["code"],
                    "name": stock["name"],
                    "current_price": price.get("current_price", 0),
                    "change_rate": change_rate,
                    "volume": price.get("volume", 0),
                    "high": price.get("high", 0),
                    "open": price.get("open", 0),
                })
        except Exception as e:
            fail_count += 1
            logger.debug(f"fetch_price 실패 {stock['code']}: {e}")

        time.sleep(0.05)

    if fail_count > len(universe) * 0.1:
        logger.warning(f"[프리클로즈] API 실패 {fail_count}/{len(universe)} (10%+)")

    return near_limit


def _format_message(stocks: list[dict], history_map: dict) -> str:
    """텔레그램 메시지 포맷팅.

    이력 보유(2회+) 종목과 미보유 종목을 시각적으로 분리.
    """
    if not stocks:
        return ""

    now = datetime.now()
    today_str = now.strftime("%m/%d(%a)")
    time_str = now.strftime("%H:%M")

    lines = []
    lines.append("━━━━━━━━━━━━━━━━━━━")
    lines.append(f"  [15시] 상한가 임박 알림")
    lines.append(f"  {today_str} {time_str}")
    lines.append("━━━━━━━━━━━━━━━━━━━")

    # 이력 보유 vs 미보유 분리
    qualified = []
    unqualified = []
    for s in stocks:
        hist = history_map.get(s["code"], {})
        s["limit_count"] = hist.get("limit_count", 0)
        s["last_date"] = hist.get("last_date", "")
        s["is_qualified"] = hist.get("is_qualified", False)
        if s["is_qualified"]:
            qualified.append(s)
        else:
            unqualified.append(s)

    idx = 1

    if qualified:
        lines.append("")
        lines.append("★ 이력 보유 (매수 검토)")
        for s in qualified:
            vol_k = s["volume"] / 1000
            last_short = s["last_date"][5:] if s["last_date"] else "?"
            lines.append(
                f"{idx}. {s['name']}({s['code']}) "
                f"{s['current_price']:,}원 +{s['change_rate']:.1f}%"
            )
            lines.append(
                f"   상한가 이력 {s['limit_count']}회 | 최근 {last_short}"
            )
            lines.append(
                f"   거래량 {vol_k:,.0f}K | 시가 {s['open']:,}원"
            )
            idx += 1

    if unqualified:
        lines.append("")
        lines.append("━━━ 이력 없음 (참고) ━━━")
        for s in unqualified:
            vol_k = s["volume"] / 1000
            count_label = f"{s['limit_count']}회" if s["limit_count"] else "없음"
            lines.append(
                f"{idx}. {s['name']}({s['code']}) "
                f"{s['current_price']:,}원 +{s['change_rate']:.1f}%"
            )
            lines.append(
                f"   상한가 이력 {count_label} | "
                f"거래량 {vol_k:,.0f}K"
            )
            idx += 1

    lines.append("")
    lines.append("━━━━━━━━━━━━━━━━━━━")
    lines.append("전략: 이력 2회+ 상한가 임박 →")
    lines.append("15:30 직전 시장가 매수 검토")
    lines.append("━━━━━━━━━━━━━━━━━━━")

    return "\n".join(lines)


async def generate_preclose_limitup_alert(
    kis_trader, threshold: float = DEFAULT_THRESHOLD_PCT, top_n: int = 10
) -> str:
    """15시 프리클로즈 상한가 임박 알림 생성.

    Args:
        kis_trader: KISTrader 인스턴스
        threshold: 등락률 임계값 (기본 25%)
        top_n: 최대 표시 종목 수

    Returns:
        텔레그램 전송용 텍스트 (종목 없으면 빈 문자열)
    """
    # 1. 유니버스 + 이력맵 구축 (I/O 경량, 메인스레드 OK)
    universe = _build_scan_universe()
    if not universe:
        logger.warning("[프리클로즈] 스캔 유니버스 비어있음")
        return ""

    history_map = _build_limit_history_map()

    # 2. KIS API 스캔 (blocking → thread)
    near_limit = await asyncio.to_thread(
        _scan_prices, kis_trader, universe, threshold
    )

    if not near_limit:
        logger.info(f"[프리클로즈] +{threshold}%+ 종목 없음 ({len(universe)}종목 스캔)")
        return ""

    # 3. 정렬: 이력보유 우선 → change_rate 내림차순
    for s in near_limit:
        hist = history_map.get(s["code"], {})
        s["_qualified"] = hist.get("is_qualified", False)
    near_limit.sort(key=lambda x: (-int(x["_qualified"]), -x["change_rate"]))

    # 4. 메시지 생성
    top_stocks = near_limit[:top_n]
    msg = _format_message(top_stocks, history_map)

    logger.info(
        f"[프리클로즈] {len(near_limit)}종목 감지 "
        f"(이력보유 {sum(1 for s in near_limit if s['_qualified'])}종목)"
    )
    return msg
