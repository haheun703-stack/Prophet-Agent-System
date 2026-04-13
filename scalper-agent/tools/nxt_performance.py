# -*- coding: utf-8 -*-
"""
NXT 야간매수 TOP 5 + 성적표
============================
C32: nightwatch → NXT TOP 5 추출 → 진입가 기록 → FLOWX/텔레그램 발행
C33: 어제 NXT TOP 5 → 오늘 종가 기준 수익률 → 주간/월간 누적

스케줄: G7 16:30 Stage 3
"""

import json
import logging
from datetime import date, datetime, timedelta
from pathlib import Path

logger = logging.getLogger("BH.NxtPerf")

DATA_DIR = Path(__file__).resolve().parent.parent / "data_store"
NW_REPORT_PATH = DATA_DIR / "nightwatch_report.json"
NXT_PICKS_PATH = DATA_DIR / "nxt_top5_picks.json"
UNIVERSE_PATH = DATA_DIR / "universe.json"

# ETF 브랜드 접두사 (시간외 단일가 불가)
_ETF_PREFIXES = (
    "TIGER", "KODEX", "KBSTAR", "ACE", "PLUS", "KOSEF", "HANARO", "SOL",
    "ARIRANG", "TREX", "FOCUS", "HK", "PARA", "마이티", "히어로즈",
    "RISE", "KCGI", "WOORI", "WON", "WON드림",
)


_universe_cache: dict | None = None


def _load_universe() -> dict:
    """universe.json 캐시 로드."""
    global _universe_cache
    if _universe_cache is not None:
        return _universe_cache
    try:
        if UNIVERSE_PATH.exists():
            _universe_cache = json.loads(UNIVERSE_PATH.read_text(encoding="utf-8"))
            return _universe_cache
    except Exception:
        pass
    _universe_cache = {}
    return _universe_cache


def _is_afterhours_eligible(code: str, name: str) -> bool:
    """시간외 매매 가능 여부 판별.

    NXT 대체거래소 통합 매수 적용 (2025.03~):
    - NXT 등록 종목 → NXT 시간외 (15:30~20:00)
    - NXT 미등록 종목 → KRX 시간외 단일가 (15:40~16:00)
    - ETF → LP 부재로 시간외 체결 불가 (제외)
    """
    # ETF 브랜드 체크 (유일한 제외 사유)
    if name:
        n = name.strip().upper()
        if any(n.startswith(p.upper()) for p in _ETF_PREFIXES):
            return False

    return True


# ═══════════════════════════════════════════════════
# Part A: NXT TOP 5 추출 + 발행 (C32)
# ═══════════════════════════════════════════════════

def extract_nxt_top5(target_date: str = None) -> dict | None:
    """nightwatch_report.json에서 시간외매매 가능 종목 중 supply_score 상위 5개 추출.

    NXT 대체거래소 통합: KOSPI/KOSDAQ 모두 가능, ETF만 제외

    Returns: {
        "date": "2026-04-10",
        "nxt_score": 4.0,
        "nxt_signal": "🟢 매수 고려",
        "picks": [{"rank":1, "code":"005290", "name":"동진쎄미켐",
                    "sector":"반도체", "supply_score":80, "entry_price":35000}, ...]
    }
    """
    if not NW_REPORT_PATH.exists():
        logger.warning(f"nightwatch_report.json 없음: {NW_REPORT_PATH}")
        return None

    try:
        nw = json.loads(NW_REPORT_PATH.read_text(encoding="utf-8"))
    except Exception as e:
        logger.error(f"nightwatch_report 파싱 실패: {e}")
        return None

    targets = nw.get("nxt_targets", [])
    if not targets:
        logger.warning("nxt_targets 비어있음")
        return None

    # 시간외매매 적격성 필터 (ETF 제외, KOSPI/KOSDAQ 모두 NXT 대체거래소로 매수 가능)
    eligible = []
    skipped = []
    for t in targets:
        code = t.get("code", "")
        name = t.get("name", "")
        if _is_afterhours_eligible(code, name):
            eligible.append(t)
        else:
            skipped.append(f"{name}({code})")

    if skipped:
        logger.info(f"시간외매매 부적격 제외: {', '.join(skipped)}")

    if not eligible:
        logger.warning("시간외매매 가능 종목 없음")
        return None

    # supply_score 내림차순 → 상위 5개
    sorted_targets = sorted(eligible, key=lambda x: x.get("supply_score", 0), reverse=True)
    top5 = sorted_targets[:5]

    # 진입가 = 당일 종가 (pykrx)
    if not target_date:
        target_date = date.today().strftime("%Y%m%d")

    from pykrx import stock as pykrx_stock

    picks = []
    for i, t in enumerate(top5, 1):
        code = t.get("code", "")
        entry_price = 0
        try:
            df = pykrx_stock.get_market_ohlcv(target_date, target_date, code)
            if not df.empty:
                entry_price = int(df.iloc[-1].get("종가", 0))
        except Exception as e:
            logger.warning(f"종가 조회 실패 {code}: {e}")

        if entry_price <= 0:
            logger.warning(f"진입가 0원: {code} {t.get('name')} — 스킵")
            continue

        # universe.json에서 최신 종목명 우선 사용
        uni = _load_universe()
        uni_name = ""
        if isinstance(uni, dict):
            uni_info = uni.get(code, {})
            if isinstance(uni_info, dict):
                uni_name = uni_info.get("name", "")

        picks.append({
            "rank": i,
            "code": code,
            "name": uni_name or t.get("name", ""),
            "sector": t.get("sector", "").replace("💾 ", "").replace("⚡ ", "")
                      .replace("🏗 ", "").replace("🔋 ", "").replace("🛡 ", ""),
            "sector_raw": t.get("sector", ""),
            "supply_score": t.get("supply_score", 0),
            "tier": t.get("tier", 0),
            "entry_price": entry_price,
            "foreign_flow_warning": t.get("foreign_flow_warning", ""),
        })

    if not picks:
        logger.warning("유효한 NXT TOP 5 없음 (진입가 조회 실패)")
        return None

    result = {
        "date": date.today().isoformat(),
        "updated": datetime.now().isoformat(),
        "nxt_score": nw.get("total_score", 0),
        "nxt_signal": f"{nw.get('signal', '🟡')} {nw.get('signal_text', '관망')}",
        "recommended_sectors": nw.get("recommended_sectors", [])[:3],
        "picks": picks,
    }

    # 로컬 저장
    try:
        NXT_PICKS_PATH.write_text(
            json.dumps(result, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        logger.info(f"NXT TOP 5 저장: {NXT_PICKS_PATH}")
    except Exception as e:
        logger.warning(f"NXT TOP 5 로컬 저장 실패: {e}")

    return result


def format_nxt_top5_telegram(picks_data: dict) -> str:
    """NXT TOP 5 발행 텔레그램 메시지."""
    if not picks_data:
        return ""

    d = picks_data.get("date", "")
    date_str = d[5:].replace("-", "/") if d else datetime.now().strftime("%m/%d")
    score = picks_data.get("nxt_score", 0)
    signal = picks_data.get("nxt_signal", "관망")
    picks = picks_data.get("picks", [])

    lines = [
        f"🌙 <b>NXT 야간매수 TOP 5</b> ({date_str})",
        f"종합: {score:+.1f}점 {signal}",
        "",
    ]

    rank_emoji = ["🥇", "🥈", "🥉", "4️⃣", "5️⃣"]
    for p in picks:
        emoji = rank_emoji[min(p["rank"] - 1, 4)]
        warning = p.get("foreign_flow_warning", "")
        warn_tag = ""
        if warning == "쌍매도":
            warn_tag = " 🔴쌍매도"
        elif warning == "외국인대량이탈":
            warn_tag = " ⚠️외국인대량이탈"
        elif warning == "외국인이탈":
            warn_tag = " ⚠️외국인이탈"
        lines.append(
            f"{emoji} <b>{p['name']}</b> · {p['sector']} "
            f"· 수급{p['supply_score']} · {p['entry_price']:,}원{warn_tag}"
        )

    # 섹터 요약
    sectors = picks_data.get("recommended_sectors", [])
    if sectors:
        lines.append("")
        lines.append("📡 " + " / ".join(s[:10] for s in sectors[:3]))

    lines.append("")
    lines.append("⏰ NXT 시간외 매수 (15:30~20:00)")

    # 금요일 경고
    if date.today().weekday() == 4:  # 금요일
        lines.append("⚠️ 금→월 보유 (주말 리스크)")

    return "\n".join(lines)


# ═══════════════════════════════════════════════════
# Part B: NXT 성적표 (C33)
# ═══════════════════════════════════════════════════

def load_yesterday_nxt_picks() -> dict | None:
    """Supabase에서 가장 최근 NXT TOP 5 로드 (어제 or 금요일).

    Returns: picks_data dict or None
    """
    try:
        from data.upload_swing import _get_client
        client = _get_client()
        if not client:
            return None

        # 최근 7일 내 가장 마지막 NXT 픽
        since = (date.today() - timedelta(days=7)).isoformat()
        resp = client.table("intelligence_nxt_picks") \
            .select("*") \
            .gte("date", since) \
            .lt("date", date.today().isoformat()) \
            .order("date", desc=True) \
            .limit(1) \
            .execute()

        if resp.data:
            row = resp.data[0]
            return {
                "date": row["date"],
                "nxt_score": row.get("nxt_score", 0),
                "nxt_signal": row.get("signal", ""),
                "picks": row.get("picks", []),
            }
        return None
    except Exception as e:
        logger.warning(f"어제 NXT 픽 Supabase 조회 실패: {e}")
        # fallback: 로컬 파일
        return _load_nxt_picks_local()


def _load_nxt_picks_local() -> dict | None:
    """로컬 nxt_top5_picks.json 폴백."""
    if not NXT_PICKS_PATH.exists():
        return None
    try:
        data = json.loads(NXT_PICKS_PATH.read_text(encoding="utf-8"))
        pick_date = data.get("date", "")
        today = date.today().isoformat()
        if pick_date >= today:
            # 오늘 이후 데이터는 아직 어제 것이 아님
            return None
        return data
    except Exception:
        return None


def build_nxt_performance_report() -> dict | None:
    """어제 NXT TOP 5의 오늘 종가 기준 성적표.

    Returns: {
        "pick_date": "2026-04-10",
        "result_date": "2026-04-11",
        "items": [...],
        "avg_return": float,
        "cumulative": {...},
        "telegram_msg": str,
    }
    """
    yesterday_picks = load_yesterday_nxt_picks()
    if not yesterday_picks:
        logger.warning("어제 NXT 픽 없음 — 성적표 생성 불가")
        return None

    picks = yesterday_picks.get("picks", [])
    if not picks:
        logger.warning("어제 NXT 픽 비어있음")
        return None

    picks = picks[:5]
    codes = [p.get("code", "") for p in picks if p.get("code")]

    # 오늘 종가 조회
    from pykrx import stock as pykrx_stock
    today_str = date.today().strftime("%Y%m%d")

    items = []
    for p in picks:
        code = p.get("code", "")
        entry = p.get("entry_price", 0)
        if not code or entry <= 0:
            continue

        try:
            df = pykrx_stock.get_market_ohlcv(today_str, today_str, code)
            if df.empty:
                logger.warning(f"오늘 OHLCV 없음: {code}")
                continue
            row = df.iloc[-1]
            close = int(row.get("종가", 0))
            if close <= 0:
                continue
        except Exception as e:
            logger.warning(f"OHLCV 실패 {code}: {e}")
            continue

        ret_pct = round((close - entry) / entry * 100, 2)
        items.append({
            "rank": p.get("rank", 0),
            "code": code,
            "name": p.get("name", ""),
            "sector": p.get("sector", ""),
            "supply_score": p.get("supply_score", 0),
            "entry_price": entry,
            "close_price": close,
            "return_pct": ret_pct,
        })

    if not items:
        logger.warning("NXT 성적 계산 결과 없음")
        return None

    avg_ret = round(sum(it["return_pct"] for it in items) / len(items), 2)

    # 주간/월간 누적
    history = _fetch_nxt_cumulative(lookback_days=30)
    cumulative = _calculate_cumulative(history, avg_ret)

    pick_date = yesterday_picks.get("date", "")
    msg = format_nxt_performance_telegram(items, cumulative, pick_date)

    return {
        "pick_date": pick_date,
        "result_date": date.today().isoformat(),
        "items": items,
        "avg_return": avg_ret,
        "best_pick": max(items, key=lambda x: x["return_pct"])["name"] if items else "",
        "worst_pick": min(items, key=lambda x: x["return_pct"])["name"] if items else "",
        "cumulative": cumulative,
        "telegram_msg": msg,
    }


def _fetch_nxt_cumulative(lookback_days: int = 30) -> list:
    """Supabase에서 과거 NXT 성적 조회."""
    try:
        from data.upload_swing import _get_client
        client = _get_client()
        if not client:
            return []

        since = (date.today() - timedelta(days=lookback_days)).isoformat()
        resp = client.table("intelligence_nxt_performance") \
            .select("pick_date,avg_return") \
            .gte("pick_date", since) \
            .order("pick_date", desc=False) \
            .execute()

        return [{"date": r["pick_date"], "avg_return": r.get("avg_return", 0) or 0}
                for r in (resp.data or [])]
    except Exception as e:
        logger.warning(f"NXT 누적 조회 실패: {e}")
        return []


def _calculate_cumulative(history: list, today_avg: float) -> dict:
    """주간/월간 누적 수익률."""
    today_str = date.today().isoformat()
    week_ago = (date.today() - timedelta(days=7)).isoformat()
    month_ago = (date.today() - timedelta(days=30)).isoformat()

    all_returns = [r for r in history if r["date"] != today_str]
    all_returns.append({"date": today_str, "avg_return": today_avg})

    weekly = [r for r in all_returns if r["date"] >= week_ago]
    monthly = [r for r in all_returns if r["date"] >= month_ago]

    return {
        "weekly_return": round(sum(r["avg_return"] for r in weekly), 2),
        "weekly_days": len(weekly),
        "weekly_wins": sum(1 for r in weekly if r["avg_return"] > 0),
        "monthly_return": round(sum(r["avg_return"] for r in monthly), 2),
        "monthly_days": len(monthly),
        "monthly_wins": sum(1 for r in monthly if r["avg_return"] > 0),
    }


def format_nxt_performance_telegram(
    items: list, cumulative: dict, pick_date: str = ""
) -> str:
    """NXT 성적표 텔레그램 메시지."""
    date_str = pick_date[5:].replace("-", "/") if pick_date else "?"

    lines = [
        f"📊 <b>NXT 야간매수 성적표</b> ({date_str} 추천분)",
        "",
    ]

    if not items:
        lines.append("⚠️ 성적 데이터 없음")
        return "\n".join(lines)

    rank_emoji = ["🥇", "🥈", "🥉", "4️⃣", "5️⃣"]
    total_ret = 0

    for it in items:
        r = it["return_pct"]
        total_ret += r
        sign = "+" if r >= 0 else ""
        emoji = rank_emoji[min(it["rank"] - 1, 4)]
        arrow = "🔴" if r < 0 else ("🟢" if r > 0 else "⚪")
        lines.append(
            f"{emoji} {it['name']} · {arrow} {sign}{r:.2f}%"
            f" ({it['entry_price']:,}→{it['close_price']:,})"
        )

    avg_ret = round(total_ret / len(items), 2)
    sign = "+" if avg_ret >= 0 else ""
    lines.append("")
    lines.append(f"📈 <b>평균: {sign}{avg_ret:.2f}%</b>")

    wr = cumulative.get("weekly_return", 0)
    wd = cumulative.get("weekly_days", 0)
    ww = cumulative.get("weekly_wins", 0)
    mr = cumulative.get("monthly_return", 0)
    md = cumulative.get("monthly_days", 0)
    mw = cumulative.get("monthly_wins", 0)

    lines.append("")
    ws = "+" if wr >= 0 else ""
    ms = "+" if mr >= 0 else ""
    lines.append(f"📅 주간: {ws}{wr:.2f}% ({ww}승/{wd - ww}패, {wd}일)")
    lines.append(f"📆 월간: {ms}{mr:.2f}% ({mw}승/{md - mw}패, {md}일)")

    return "\n".join(lines)


# ═══════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════

if __name__ == "__main__":
    import argparse
    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument("--extract", action="store_true", help="NXT TOP 5 추출")
    parser.add_argument("--report", action="store_true", help="어제 NXT 성적표")
    args = parser.parse_args()

    if args.extract:
        result = extract_nxt_top5()
        if result:
            msg = format_nxt_top5_telegram(result)
            print(msg)
            print(f"\n--- {len(result['picks'])}종목 저장 완료 ---")
        else:
            print("NXT TOP 5 추출 실패")

    elif args.report:
        report = build_nxt_performance_report()
        if report:
            print(report["telegram_msg"])
            print(f"\n--- avg: {report['avg_return']}% ---")
        else:
            print("NXT 성적표 생성 실패")

    else:
        parser.print_help()
