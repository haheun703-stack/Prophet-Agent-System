# -*- coding: utf-8 -*-
"""
단타 TOP픽 일일 성적표
=======================
당일 confirmed TOP 5 → 시가/종가 수익률 계산 + 주간/월간 누적.

스케줄: G7 16:30 (장마감 후)
데이터: daytrading_picks.json (confirmed) + pykrx OHLCV
출력: Supabase intelligence_daytrading_performance + 텔레그램
"""

import json
import logging
from datetime import date, datetime, timedelta
from pathlib import Path

logger = logging.getLogger("BH.DaytradingPerf")

DATA_DIR = Path(__file__).resolve().parent.parent / "data_store"
PICKS_PATH = DATA_DIR / "daytrading_picks.json"


def load_today_confirmed_picks() -> list:
    """당일 confirmed 픽 로드 (daytrading_picks.json)."""
    if not PICKS_PATH.exists():
        logger.warning(f"daytrading_picks.json 없음: {PICKS_PATH}")
        return []

    try:
        raw = json.loads(PICKS_PATH.read_text(encoding="utf-8"))
    except Exception as e:
        logger.error(f"picks JSON 파싱 실패: {e}")
        return []

    mode = raw.get("mode", "")
    if mode != "confirmed":
        logger.warning(f"picks mode={mode} — confirmed가 아님, 스킵")
        return []

    # 날짜 검증: updated가 오늘인지
    updated = raw.get("updated", "")
    today_str = date.today().isoformat()
    if updated and not updated.startswith(today_str):
        logger.warning(f"picks 날짜 불일치: updated={updated[:10]}, today={today_str}")
        return []

    return raw.get("picks", [])


def fetch_today_ohlcv(codes: list[str], target_date: str = None) -> dict:
    """pykrx로 당일 OHLCV 조회.

    Returns: {code: {"open": int, "close": int, "high": int, "low": int, "volume": int}}
    """
    from pykrx import stock as pykrx_stock

    if not target_date:
        target_date = date.today().strftime("%Y%m%d")

    result = {}
    for code in codes:
        try:
            df = pykrx_stock.get_market_ohlcv(target_date, target_date, code)
            if df.empty:
                logger.warning(f"OHLCV 없음: {code} ({target_date})")
                continue
            row = df.iloc[-1]
            result[code] = {
                "open": int(row.get("시가", 0)),
                "close": int(row.get("종가", 0)),
                "high": int(row.get("고가", 0)),
                "low": int(row.get("저가", 0)),
                "volume": int(row.get("거래량", 0)),
            }
        except Exception as e:
            logger.warning(f"OHLCV 조회 실패 {code}: {e}")
    return result


def calculate_daily_performance(picks: list, ohlcv: dict) -> list:
    """각 종목별 시가→종가 수익률 계산.

    Returns: [{"rank": 1, "code": "...", "name": "...", "score": float,
               "open_price": int, "close_price": int, "return_pct": float, ...}]
    """
    results = []
    for i, p in enumerate(picks, 1):
        code = p.get("code", "")
        prices = ohlcv.get(code)
        if not prices or prices["open"] <= 0:
            logger.warning(f"가격 없음: {code} {p.get('name')} — 스킵")
            continue

        open_p = prices["open"]
        close_p = prices["close"]
        ret_pct = round((close_p - open_p) / open_p * 100, 2)

        results.append({
            "rank": i,
            "code": code,
            "name": p.get("name", ""),
            "sector": p.get("sector", ""),
            "score": p.get("final_score", 0),
            "open_price": open_p,
            "close_price": close_p,
            "high_price": prices.get("high", 0),
            "low_price": prices.get("low", 0),
            "return_pct": ret_pct,
            "volume": prices.get("volume", 0),
        })

    return results


def fetch_cumulative_from_supabase(lookback_days: int = 30) -> list:
    """Supabase에서 과거 성적표 조회 (주간/월간 누적 계산용).

    Returns: [{"date": "2026-04-09", "avg_return": -1.2, "items": [...]}]
    """
    try:
        from data.upload_swing import _get_client
        client = _get_client()
        if not client:
            return []

        since = (date.today() - timedelta(days=lookback_days)).isoformat()
        resp = client.table("intelligence_daytrading_performance") \
            .select("date,avg_return,items") \
            .gte("date", since) \
            .order("date", desc=False) \
            .execute()

        return resp.data or []
    except Exception as e:
        logger.warning(f"누적 성적 조회 실패: {e}")
        return []


def calculate_cumulative(history: list, today_avg: float) -> dict:
    """주간/월간 누적 수익률 계산.

    Returns: {"weekly_return": float, "monthly_return": float,
              "weekly_days": int, "monthly_days": int,
              "weekly_wins": int, "monthly_wins": int}
    """
    today_str = date.today().isoformat()
    week_ago = (date.today() - timedelta(days=7)).isoformat()
    month_ago = (date.today() - timedelta(days=30)).isoformat()

    # 과거 데이터 + 오늘 합산
    all_returns = []
    for h in history:
        if h["date"] != today_str:  # 중복 방지
            all_returns.append({
                "date": h["date"],
                "avg_return": h.get("avg_return", 0) or 0,
            })
    all_returns.append({"date": today_str, "avg_return": today_avg})

    # 주간
    weekly = [r for r in all_returns if r["date"] >= week_ago]
    weekly_sum = sum(r["avg_return"] for r in weekly)
    weekly_wins = sum(1 for r in weekly if r["avg_return"] > 0)

    # 월간
    monthly = [r for r in all_returns if r["date"] >= month_ago]
    monthly_sum = sum(r["avg_return"] for r in monthly)
    monthly_wins = sum(1 for r in monthly if r["avg_return"] > 0)

    return {
        "weekly_return": round(weekly_sum, 2),
        "weekly_days": len(weekly),
        "weekly_wins": weekly_wins,
        "monthly_return": round(monthly_sum, 2),
        "monthly_days": len(monthly),
        "monthly_wins": monthly_wins,
    }


def format_performance_telegram(
    items: list, cumulative: dict, target_date: str = None
) -> str:
    """텔레그램용 성적표 메시지 포맷."""
    if not target_date:
        target_date = date.today().strftime("%m/%d")

    lines = [
        f"📊 <b>단타 TOP 5 성적표</b> ({target_date})",
        "",
    ]

    if not items:
        lines.append("⚠️ 오늘 성적 데이터 없음")
        return "\n".join(lines)

    total_ret = 0
    rank_emoji = ["🥇", "🥈", "🥉", "4️⃣", "5️⃣"]

    for it in items:
        r = it["return_pct"]
        total_ret += r
        sign = "+" if r >= 0 else ""
        emoji = rank_emoji[min(it["rank"] - 1, 4)]
        arrow = "🔴" if r < 0 else ("🟢" if r > 0 else "⚪")
        lines.append(
            f"{emoji} {it['name']} · {arrow} {sign}{r:.2f}%"
            f" ({it['open_price']:,}→{it['close_price']:,})"
        )

    avg_ret = round(total_ret / len(items), 2)
    sign = "+" if avg_ret >= 0 else ""
    lines.append("")
    lines.append(f"📈 <b>오늘 평균: {sign}{avg_ret:.2f}%</b>")
    lines.append("")

    # 주간/월간 누적
    wr = cumulative.get("weekly_return", 0)
    wd = cumulative.get("weekly_days", 0)
    ww = cumulative.get("weekly_wins", 0)
    mr = cumulative.get("monthly_return", 0)
    md = cumulative.get("monthly_days", 0)
    mw = cumulative.get("monthly_wins", 0)

    ws = "+" if wr >= 0 else ""
    ms = "+" if mr >= 0 else ""
    lines.append(f"📅 주간: {ws}{wr:.2f}% ({ww}승/{wd - ww}패, {wd}일)")
    lines.append(f"📆 월간: {ms}{mr:.2f}% ({mw}승/{md - mw}패, {md}일)")

    return "\n".join(lines)


def build_performance_report(target_date_str: str = None) -> dict | None:
    """일일 성적표 전체 빌드.

    Returns: {
        "date": "2026-04-10",
        "items": [...],
        "avg_return": float,
        "cumulative": {...},
        "telegram_msg": str,
    } or None
    """
    picks = load_today_confirmed_picks()
    if not picks:
        logger.warning("오늘 confirmed 픽 없음 — 성적표 생성 불가")
        return None

    # TOP 5만
    picks = picks[:5]
    codes = [p["code"] for p in picks]

    if not target_date_str:
        target_date_str = date.today().strftime("%Y%m%d")

    ohlcv = fetch_today_ohlcv(codes, target_date_str)
    if not ohlcv:
        logger.error("OHLCV 조회 실패 — 성적표 생성 불가")
        return None

    items = calculate_daily_performance(picks, ohlcv)
    if not items:
        logger.error("성적 계산 결과 없음")
        return None

    avg_ret = round(sum(it["return_pct"] for it in items) / len(items), 2)

    # 과거 데이터에서 누적 계산
    history = fetch_cumulative_from_supabase(lookback_days=30)
    cumulative = calculate_cumulative(history, avg_ret)

    msg = format_performance_telegram(items, cumulative)

    return {
        "date": date.today().isoformat(),
        "items": items,
        "avg_return": avg_ret,
        "best_pick": max(items, key=lambda x: x["return_pct"])["name"] if items else "",
        "worst_pick": min(items, key=lambda x: x["return_pct"])["name"] if items else "",
        "cumulative": cumulative,
        "telegram_msg": msg,
    }


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO)
    result = build_performance_report()
    if result:
        print(result["telegram_msg"])
        print(f"\n--- avg: {result['avg_return']}% ---")
    else:
        print("성적표 생성 실패")
        sys.exit(1)
