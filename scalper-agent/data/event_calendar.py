# -*- coding: utf-8 -*-
"""
Event Calendar — 정적 이벤트 캘린더 + Confluence 스코어링
==========================================================
Layer 1: 정적 캘린더 (EVENT_CALENDAR_2026.json)
Layer 3: Impact Scoring — 이벤트 겹침 정도 → 변동성 예측
Layer 4: Confluence Detection — 동시 이벤트 → 리스크 레벨

기존 global_event_calendar.py (Perplexity 실시간)와 별개.
이 모듈은 사전 등록된 정기 이벤트를 빠르게 조회하는 역할.

사용:
    python -m data.event_calendar                # 오늘 이벤트
    python -m data.event_calendar --week         # 이번 주 브리핑
    python -m data.event_calendar --date 2026-04-29  # 특정 날짜
"""

import json
import logging
import sys
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger("BH.EventCalendar")

BASE_DIR = Path(__file__).resolve().parent.parent
CALENDAR_JSON = BASE_DIR.parent / "EVENT_CALENDAR_2026.json"

# ═══════════════════════════════════════════════════
#  1. JSON 로드 + 캐시
# ═══════════════════════════════════════════════════

_calendar_cache: Optional[Dict] = None
_cache_mtime: float = 0


def _load_calendar() -> Dict:
    """EVENT_CALENDAR_2026.json 로드 (변경시만 재로드)"""
    global _calendar_cache, _cache_mtime
    if not CALENDAR_JSON.exists():
        logger.warning(f"캘린더 파일 없음: {CALENDAR_JSON}")
        return {}
    mtime = CALENDAR_JSON.stat().st_mtime
    if _calendar_cache is not None and mtime == _cache_mtime:
        return _calendar_cache
    with open(CALENDAR_JSON, "r", encoding="utf-8") as f:
        _calendar_cache = json.load(f)
    _cache_mtime = mtime
    return _calendar_cache


def _parse_date(s: str) -> Optional[date]:
    """YYYY-MM-DD 문자열 → date"""
    try:
        return datetime.strptime(s[:10], "%Y-%m-%d").date()
    except (ValueError, TypeError):
        return None


def _parse_date_range(s: str) -> Tuple[Optional[date], Optional[date]]:
    """'2026-03-17 ~ 2026-03-18' → (start, end)"""
    parts = s.split("~")
    if len(parts) == 2:
        return _parse_date(parts[0].strip()), _parse_date(parts[1].strip())
    d = _parse_date(s.strip())
    return d, d


# ═══════════════════════════════════════════════════
#  2. 특정 날짜 기준 이벤트 조회
# ═══════════════════════════════════════════════════

def get_events_for_date(target: date = None) -> List[Dict]:
    """특정 날짜에 영향을 주는 모든 이벤트 반환

    각 이벤트의 window (before/after) 기준으로, target이 해당 구간에 포함되면 반환.
    즉 FOMC가 4/29인데 window before=-2이면, 4/27~4/30 사이에 target이 있으면 반환.
    """
    if target is None:
        target = date.today()
    cal = _load_calendar()
    if not cal:
        return []

    events = []

    # ── 중앙은행 ──
    for bank_key, bank in cal.get("central_banks", {}).items():
        window = bank.get("window", {"before": -2, "after": 1})
        w_before = abs(window.get("before", -2))
        w_after = window.get("after", 1)
        impact = bank.get("impact", "MEDIUM")
        vol_mult = bank.get("volatility_multiplier", 1.0)

        meetings_key = None
        for k in bank:
            if "meetings" in k.lower() or "monetary" in k.lower():
                meetings_key = k
                break
        if not meetings_key:
            continue

        for meeting in bank[meetings_key]:
            if meeting.get("status") == "completed":
                continue
            # 날짜 파싱
            dr = meeting.get("date_range", meeting.get("date", ""))
            m_start, m_end = _parse_date_range(dr)
            if not m_start:
                d = meeting.get("date")
                if d:
                    m_start = _parse_date(d)
                    m_end = m_start
            if not m_start:
                continue

            # window 체크
            win_start = m_start - timedelta(days=w_before)
            win_end = (m_end or m_start) + timedelta(days=w_after)
            if win_start <= target <= win_end:
                days_to = (m_start - target).days
                events.append({
                    "category": "central_bank",
                    "name": bank_key,
                    "full_name": bank.get("full_name", bank_key),
                    "country": bank.get("country", ""),
                    "date": str(m_start),
                    "date_end": str(m_end) if m_end != m_start else None,
                    "days_to": days_to,
                    "impact": impact,
                    "volatility_multiplier": vol_mult,
                    "has_SEP": meeting.get("has_SEP", False),
                    "has_outlook": meeting.get("has_outlook", False),
                    "notes": meeting.get("notes", bank.get("notes", "")),
                })

    # ── 인덱스 리밸런싱 ──
    for idx_key, idx in cal.get("index_rebalancing", {}).items():
        impact = idx.get("impact", "MEDIUM")
        vol_mult = idx.get("volatility_multiplier", 1.5)
        window = idx.get("window", {"before": -5, "after": 1})
        w_before = abs(window.get("before", -5))
        w_after = window.get("after", 1)

        reviews = idx.get("reviews_2026", [])
        if isinstance(reviews, dict):
            reviews = [reviews]  # FTSE annual
        for rev in reviews:
            if rev.get("status") == "completed":
                continue
            eff_date = _parse_date(rev.get("effective_date", ""))
            ann_date = _parse_date(rev.get("announcement_date", rev.get("announcement_approx", "")))
            if not eff_date:
                continue
            ref_date = ann_date or eff_date
            win_start = ref_date - timedelta(days=w_before)
            win_end = eff_date + timedelta(days=w_after)
            if win_start <= target <= win_end:
                days_to = (eff_date - target).days
                events.append({
                    "category": "index_rebalancing",
                    "name": idx_key,
                    "date": str(eff_date),
                    "announcement": str(ann_date) if ann_date else None,
                    "days_to": days_to,
                    "impact": impact,
                    "volatility_multiplier": vol_mult,
                    "notes": rev.get("notes", idx.get("notes", "")),
                })

    # ── 옵션 만기 ──
    for opx_key, opx in cal.get("options_expiration", {}).items():
        impact = opx.get("impact", "MEDIUM")
        vol_mult = opx.get("volatility_multiplier", 1.3)

        dates_list = opx.get("dates_2026", [])
        for item in dates_list:
            if isinstance(item, str):
                d = _parse_date(item)
                quarterly = False
                label = opx_key
            else:
                d = _parse_date(item.get("date", ""))
                quarterly = item.get("quarterly", False)
                label = item.get("label", opx_key)
            if not d:
                continue
            # 만기일 당일 + 전일만 체크
            if target == d or target == d - timedelta(days=1):
                events.append({
                    "category": "options_expiration",
                    "name": label,
                    "date": str(d),
                    "days_to": (d - target).days,
                    "impact": "HIGH" if quarterly else impact,
                    "volatility_multiplier": vol_mult if quarterly else vol_mult * 0.8,
                    "quarterly": quarterly,
                    "notes": opx.get("notes", ""),
                })

    # ── 어닝 시즌 ──
    for country, info in cal.get("earnings_seasons", {}).items():
        impact = info.get("impact", "MEDIUM")
        for season in info.get("seasons_2026", []):
            # 기간 체크
            period = season.get("period", "")
            prelim = season.get("preliminary", "")
            if period:
                start, end = _parse_date_range(period)
            elif prelim:
                start, end = _parse_date_range(prelim)
            else:
                continue
            if not start or not end:
                continue
            if start <= target <= end:
                # peak week 체크
                in_peak = False
                for pw in season.get("peak_weeks", []):
                    ps, pe = _parse_date_range(pw)
                    if ps and pe and ps <= target <= pe:
                        in_peak = True
                        break
                events.append({
                    "category": "earnings_season",
                    "name": f"{country}_earnings",
                    "country": country,
                    "quarter": season.get("quarter", ""),
                    "date": str(start),
                    "date_end": str(end),
                    "in_peak_week": in_peak,
                    "impact": impact,
                    "volatility_multiplier": 1.5 if in_peak else 1.2,
                    "notes": season.get("notes", info.get("notes", "")),
                })

    # ── 한국 주총/배당 ──
    corp = cal.get("korea_corporate_events", {})
    agm = corp.get("annual_shareholders_meetings", {})
    if agm:
        agm_impact = agm.get("impact", "HIGH")
        agm_window = agm.get("window", {"before": -5, "after": 2})
        w_before = abs(agm_window.get("before", -5))
        w_after = agm_window.get("after", 2)
        for peak in agm.get("peak_dates_2026", []):
            d = _parse_date(peak.get("date", ""))
            if not d:
                continue
            win_start = d - timedelta(days=w_before)
            win_end = d + timedelta(days=w_after)
            if win_start <= target <= win_end:
                events.append({
                    "category": "korea_agm",
                    "name": "주총",
                    "date": str(d),
                    "days_to": (d - target).days,
                    "impact": agm_impact,
                    "volatility_multiplier": agm.get("volatility_multiplier", 1.4),
                    "notes": peak.get("notes", ""),
                })

    div = corp.get("dividend_calendar", {})
    if div:
        for key_date in div.get("key_dates_2026", []):
            period = key_date.get("period", "")
            start, end = _parse_date_range(period)
            if start and end and start <= target <= end:
                events.append({
                    "category": "korea_dividend",
                    "name": "배당기준일",
                    "date": str(start),
                    "date_end": str(end),
                    "days_to": 0,
                    "impact": div.get("impact", "MEDIUM"),
                    "volatility_multiplier": 1.2,
                    "notes": key_date.get("notes", ""),
                })

    # ── 글로벌 컨퍼런스 ──
    for conf_key, conf in cal.get("global_conferences", {}).items():
        if "events_2026" in conf:
            for ev in conf["events_2026"]:
                dr = ev.get("date_range", ev.get("date", ev.get("date_approx", "")))
                if "TBD" in str(dr):
                    continue
                start, end = _parse_date_range(dr)
                if not start:
                    continue
                if not end:
                    end = start
                # 컨퍼런스 기간 + 전후 1일
                if (start - timedelta(days=1)) <= target <= (end + timedelta(days=1)):
                    events.append({
                        "category": "conference",
                        "name": ev.get("name", conf_key),
                        "date": str(start),
                        "date_end": str(end) if end != start else None,
                        "days_to": (start - target).days,
                        "impact": conf.get("impact", "MEDIUM"),
                        "volatility_multiplier": conf.get("volatility_multiplier", 1.2),
                        "notes": conf.get("notes", ""),
                    })
        else:
            # 단일 이벤트 (davos, jackson_hole)
            dr = conf.get("date_range", conf.get("date", ""))
            if not dr:
                continue
            start, end = _parse_date_range(dr)
            if not start:
                continue
            if not end:
                end = start
            if (start - timedelta(days=1)) <= target <= (end + timedelta(days=1)):
                events.append({
                    "category": "conference",
                    "name": conf.get("name", conf_key),
                    "date": str(start),
                    "date_end": str(end) if end != start else None,
                    "days_to": (start - target).days,
                    "impact": conf.get("impact", "MEDIUM"),
                    "volatility_multiplier": conf.get("volatility_multiplier", 1.2),
                    "notes": conf.get("notes", ""),
                })

    return events


# ═══════════════════════════════════════════════════
#  3. Confluence 스코어 — 이벤트 겹침 위험도
# ═══════════════════════════════════════════════════

IMPACT_WEIGHT = {"HIGH": 3.0, "MEDIUM": 1.5, "LOW": 0.5}


def calc_confluence_score(events: List[Dict]) -> float:
    """이벤트 목록 → Confluence 스코어 (0~100)

    가중치: HIGH 이벤트 3점, MEDIUM 1.5점, LOW 0.5점
    변동성 배수도 곱함.
    최대 100점 cap.
    """
    if not events:
        return 0.0

    score = 0.0
    for ev in events:
        impact = ev.get("impact", "MEDIUM")
        base = IMPACT_WEIGHT.get(impact, 1.0)
        vol_mult = ev.get("volatility_multiplier", 1.0)
        # 당일 이벤트면 가중치 1.5배
        days_to = abs(ev.get("days_to", 0))
        proximity = 1.5 if days_to == 0 else 1.2 if days_to == 1 else 1.0
        score += base * vol_mult * proximity

    # 이벤트 수 시너지 (3개+ 겹치면 추가)
    n = len(events)
    if n >= 4:
        score *= 1.5
    elif n >= 3:
        score *= 1.3
    elif n >= 2:
        score *= 1.1

    return min(round(score, 1), 100.0)


def get_risk_level(confluence_score: float) -> str:
    """Confluence 스코어 → 리스크 레벨"""
    if confluence_score >= 60:
        return "EXTREME"
    elif confluence_score >= 35:
        return "HIGH"
    elif confluence_score >= 15:
        return "MEDIUM"
    else:
        return "LOW"


# ═══════════════════════════════════════════════════
#  4. 시즌성 (월별 바이어스)
# ═══════════════════════════════════════════════════

def get_seasonality(target: date = None) -> Dict:
    """해당 월의 시즌성 바이어스"""
    if target is None:
        target = date.today()
    cal = _load_calendar()
    month_key = f"{target.month:02d}"

    sp = cal.get("seasonality_patterns", {}).get("monthly_bias", {})
    kr = sp.get("KR_KOSPI", {}).get(month_key, {})
    us = sp.get("US_SPX", {}).get(month_key, {})

    return {
        "month": target.month,
        "kr_bias": kr.get("bias", "neutral"),
        "kr_avg": kr.get("avg", "0%"),
        "kr_note": kr.get("note", ""),
        "us_bias": us.get("bias", "neutral"),
        "us_avg": us.get("avg", "0%"),
        "us_note": us.get("note", ""),
    }


# ═══════════════════════════════════════════════════
#  5. 주간 브리핑 생성
# ═══════════════════════════════════════════════════

def generate_weekly_briefing(target: date = None) -> Dict:
    """해당 주(월~금) 이벤트 종합 브리핑

    Returns:
        {
            "week_start": "2026-03-16",
            "week_end": "2026-03-20",
            "events_by_day": {날짜: [이벤트]},
            "all_events": [전체 이벤트],
            "confluence_score": float,
            "risk_level": "LOW~EXTREME",
            "seasonality": {...},
            "key_message": "한줄 요약",
            "sectors_to_watch": ["섹터"],
        }
    """
    if target is None:
        target = date.today()

    # 해당 주 월요일~금요일
    weekday = target.weekday()  # 0=월
    monday = target - timedelta(days=weekday)
    friday = monday + timedelta(days=4)

    events_by_day: Dict[str, List] = {}
    all_events: List[Dict] = []
    seen = set()

    for i in range(5):
        d = monday + timedelta(days=i)
        day_events = get_events_for_date(d)
        events_by_day[str(d)] = day_events
        for ev in day_events:
            key = (ev["category"], ev["name"], ev["date"])
            if key not in seen:
                seen.add(key)
                all_events.append(ev)

    confluence = calc_confluence_score(all_events)
    risk = get_risk_level(confluence)
    season = get_seasonality(target)

    # 주목 섹터 추출
    sectors = set()
    for ev in all_events:
        cat = ev.get("category", "")
        if cat == "central_bank":
            sectors.add("금융")
            if ev.get("country") == "US":
                sectors.add("반도체")  # FOMC → 성장주 영향
        elif cat == "index_rebalancing":
            sectors.add("대형주")
        elif cat == "korea_agm":
            sectors.add("지배구조")
        elif cat == "earnings_season":
            if ev.get("country") == "US":
                sectors.add("반도체")
                sectors.add("2차전지")
        elif cat == "options_expiration":
            sectors.add("선물옵션")

    # 핵심 메시지
    if risk == "EXTREME":
        key_msg = f"이벤트 겹침 EXTREME ({len(all_events)}건) — 변동성 최대 주간"
    elif risk == "HIGH":
        high_events = [e["name"] for e in all_events if e.get("impact") == "HIGH"]
        key_msg = f"HIGH 이벤트 {len(high_events)}건: {'+'.join(high_events[:3])}"
    elif risk == "MEDIUM":
        key_msg = f"이벤트 {len(all_events)}건 — 적정 변동성"
    else:
        key_msg = f"이벤트 한산 — 종목 장세 예상"

    return {
        "week_start": str(monday),
        "week_end": str(friday),
        "events_by_day": events_by_day,
        "all_events": all_events,
        "confluence_score": confluence,
        "risk_level": risk,
        "seasonality": season,
        "key_message": key_msg,
        "sectors_to_watch": sorted(sectors),
    }


# ═══════════════════════════════════════════════════
#  6. 종목별 이벤트 보너스 (morning_recommendation 연동)
# ═══════════════════════════════════════════════════

# 이벤트 시즌별 수혜 섹터 매핑
_EVENT_SECTOR_BOOST = {
    "central_bank": {
        "US": ["금융", "반도체", "성장주"],
        "KR": ["금융", "은행"],
        "JP": ["반도체", "자동차부품"],
    },
    "earnings_season": {
        "US": ["반도체", "2차전지", "IT부품"],
        "KR": ["반도체", "자동차", "화학"],
    },
    "index_rebalancing": {
        "MSCI": ["대형주", "외국인선호"],
        "KOSPI200": ["대형주", "ETF구성"],
    },
    "korea_agm": {
        "default": ["지배구조", "행동주의"],
    },
}


def get_event_risk_for_recommendation(target: date = None) -> Dict:
    """추천 파이프라인용 — 오늘 이벤트 리스크 요약

    Returns:
        {
            "confluence_score": float,
            "risk_level": str,
            "events": [이벤트],
            "warning": str,  # HIGH 이상이면 경고 메시지
            "seasonality": {...},
        }
    """
    if target is None:
        target = date.today()

    events = get_events_for_date(target)
    confluence = calc_confluence_score(events)
    risk = get_risk_level(confluence)
    season = get_seasonality(target)

    warning = ""
    if risk in ("HIGH", "EXTREME"):
        event_names = [f"{e['name']}(D{e.get('days_to', 0):+d})" for e in events if e.get("impact") == "HIGH"]
        warning = f"HIGH이벤트: {', '.join(event_names)}" if event_names else ""

    return {
        "confluence_score": confluence,
        "risk_level": risk,
        "events": events,
        "warning": warning,
        "seasonality": season,
    }


# ═══════════════════════════════════════════════════
#  7. 텔레그램 포맷
# ═══════════════════════════════════════════════════

_RISK_EMOJI = {
    "LOW": "🟢",
    "MEDIUM": "🟡",
    "HIGH": "🔴",
    "EXTREME": "💀",
}

_IMPACT_EMOJI = {
    "HIGH": "🔴",
    "MEDIUM": "🟡",
    "LOW": "⚪",
}

_CAT_EMOJI = {
    "central_bank": "🏦",
    "index_rebalancing": "📊",
    "options_expiration": "📉",
    "earnings_season": "📈",
    "korea_agm": "🏢",
    "korea_dividend": "💰",
    "conference": "🎤",
}


def format_daily_telegram(target: date = None) -> str:
    """오늘 이벤트 → 텔레그램 메시지"""
    if target is None:
        target = date.today()

    events = get_events_for_date(target)
    confluence = calc_confluence_score(events)
    risk = get_risk_level(confluence)
    risk_emoji = _RISK_EMOJI.get(risk, "")
    season = get_seasonality(target)

    lines = [
        "━" * 24,
        f"📅 이벤트 캘린더 {target.strftime('%Y-%m-%d')} ({['월','화','수','목','금','토','일'][target.weekday()]})",
        f"리스크: {risk_emoji} {risk} (confluence: {confluence:.0f})",
        "━" * 24,
    ]

    if not events:
        lines.append("  이벤트 없음 — 종목 장세")
    else:
        for ev in sorted(events, key=lambda x: -IMPACT_WEIGHT.get(x.get("impact", ""), 0)):
            cat_emoji = _CAT_EMOJI.get(ev["category"], "📌")
            imp_emoji = _IMPACT_EMOJI.get(ev.get("impact", ""), "⚪")
            days_to = ev.get("days_to", 0)
            d_str = f"D{days_to:+d}" if days_to != 0 else "D-day"
            notes = ev.get("notes", "")
            if len(notes) > 40:
                notes = notes[:37] + "..."
            lines.append(f"  {cat_emoji}{imp_emoji} {ev['name']} [{d_str}]")
            if notes:
                lines.append(f"     {notes}")

    # 시즌성
    bias_emoji = {"bullish": "📈", "bearish": "📉", "volatile": "⚡", "neutral": "➡️"}
    kr_b = bias_emoji.get(season["kr_bias"], "")
    lines.append("")
    lines.append(f"📊 시즌성: {kr_b} {season['kr_bias'].upper()} ({season['kr_avg']}) {season['kr_note']}")

    lines.append("━" * 24)
    return "\n".join(lines)


def format_weekly_telegram(briefing: Dict = None) -> str:
    """주간 브리핑 → 텔레그램 메시지"""
    if briefing is None:
        briefing = generate_weekly_briefing()

    risk = briefing["risk_level"]
    risk_emoji = _RISK_EMOJI.get(risk, "")
    confluence = briefing["confluence_score"]
    season = briefing.get("seasonality", {})

    lines = [
        "━" * 24,
        f"📅 주간 이벤트 브리핑",
        f"   {briefing['week_start']} ~ {briefing['week_end']}",
        f"   리스크: {risk_emoji} {risk} (confluence: {confluence:.0f})",
        "━" * 24,
    ]

    # 일별 이벤트
    for day_str, day_events in briefing["events_by_day"].items():
        d = _parse_date(day_str)
        if not d:
            continue
        day_name = ['월', '화', '수', '목', '금'][d.weekday()] if d.weekday() < 5 else '?'
        if day_events:
            lines.append(f"\n{day_str[5:]} ({day_name}) ─ {len(day_events)}건")
            for ev in day_events:
                imp = _IMPACT_EMOJI.get(ev.get("impact", ""), "⚪")
                cat = _CAT_EMOJI.get(ev["category"], "📌")
                lines.append(f"  {cat}{imp} {ev['name']}")
        else:
            lines.append(f"\n{day_str[5:]} ({day_name}) ─ 이벤트 없음")

    # 주목 섹터
    sectors = briefing.get("sectors_to_watch", [])
    if sectors:
        lines.append(f"\n🏭 주목 섹터: {', '.join(sectors)}")

    # 시즌성
    if season:
        bias_emoji = {"bullish": "📈", "bearish": "📉", "volatile": "⚡", "neutral": "➡️"}
        kr_b = bias_emoji.get(season.get("kr_bias", ""), "")
        lines.append(f"📊 시즌성: {kr_b} {season.get('kr_bias', '').upper()} ({season.get('kr_avg', '')})")

    # 핵심 메시지
    lines.append("")
    lines.append(f"💡 {briefing['key_message']}")
    lines.append("━" * 24)
    lines.append("Prophet | Event Calendar v1.0")

    return "\n".join(lines)


# ═══════════════════════════════════════════════════
#  CLI
# ═══════════════════════════════════════════════════

if __name__ == "__main__":
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
    logging.basicConfig(level=logging.INFO)

    args = sys.argv[1:]
    target = date.today()

    if "--date" in args:
        idx = args.index("--date")
        if idx + 1 < len(args):
            target = _parse_date(args[idx + 1]) or target

    if "--week" in args:
        briefing = generate_weekly_briefing(target)
        print(format_weekly_telegram(briefing))
        print(f"\n총 이벤트: {len(briefing['all_events'])}건")
        print(f"Confluence: {briefing['confluence_score']:.1f}")
        print(f"리스크: {briefing['risk_level']}")
    else:
        # 일일
        print(format_daily_telegram(target))
        events = get_events_for_date(target)
        print(f"\n감지 이벤트: {len(events)}건")
        for ev in events:
            print(f"  [{ev['category']}] {ev['name']} D{ev.get('days_to', 0):+d} {ev.get('impact', '')}")

        # 추천용 리스크
        risk_info = get_event_risk_for_recommendation(target)
        print(f"\n추천용 리스크: {risk_info['risk_level']} (confluence: {risk_info['confluence_score']:.1f})")
        if risk_info["warning"]:
            print(f"  경고: {risk_info['warning']}")
