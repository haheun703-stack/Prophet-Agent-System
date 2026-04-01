"""
FLOWX 모닝 브리핑 — Supabase 업로드 + 텔레그램 듀얼 발송
============================================================
8레이어 데이터를 기존 data_store 파일에서 수집 → 브리핑 JSON 생성
→ Supabase morning_briefings 테이블 upsert
→ 텔레그램 듀얼 발송 (개인=전체 / FLOWX=요약)

Usage:
  python data/flowx_briefing.py --test        # 생성만 (업로드/텔레그램 X)
  python data/flowx_briefing.py --upload       # Supabase 업로드
  python data/flowx_briefing.py --upload --tg  # Supabase + 텔레그램
"""
import os
import sys
import json
import logging
import argparse
import requests
from datetime import date, datetime
from pathlib import Path

logger = logging.getLogger("flowx_briefing")

# ── 경로 설정 ────────────────────────────────────────
_DATA_STORE = Path(__file__).resolve().parent.parent / "data_store"
_ENV_PATH = Path(__file__).resolve().parent.parent.parent / ".env"


# ── Supabase 클라이언트 (upload_short.py 패턴 재사용) ──
_supabase = None


def _get_client():
    """Supabase 클라이언트 lazy 초기화"""
    global _supabase
    if _supabase is not None:
        return _supabase

    from dotenv import load_dotenv
    load_dotenv(_ENV_PATH)

    url = os.environ.get("SUPABASE_URL", "")
    key = os.environ.get("SUPABASE_KEY", "")
    if not url or not key:
        logger.error("SUPABASE_URL / SUPABASE_KEY 미설정 (.env 확인)")
        return None

    from supabase import create_client
    _supabase = create_client(url, key)
    logger.info(f"Supabase 연결: {url[:40]}...")
    return _supabase


# ── JSON 로더 ─────────────────────────────────────────

def _load_json(filename: str) -> dict:
    """data_store 내 JSON 파일 로드 (없으면 빈 dict)"""
    path = _DATA_STORE / filename
    if not path.exists():
        logger.warning(f"{filename} 없음")
        return {}
    try:
        return json.loads(path.read_text("utf-8"))
    except Exception as e:
        logger.warning(f"{filename} 로드 실패: {e}")
        return {}


# ── 변환 헬퍼 ─────────────────────────────────────────

def _map_market_status(market_health: str, cross_regime: str = "") -> str:
    """market_health + cross_regime → BULL/BEAR/NEUTRAL/CAUTION"""
    if market_health == "CRITICAL" or cross_regime == "DIVERGENCE":
        return "BEAR"
    if market_health == "WARNING" or cross_regime == "CORRECTION":
        return "CAUTION"
    # NORMAL 상태에서 추가 판별
    return "NEUTRAL"


def _build_kr_summary(rec: dict) -> str:
    """한국 시장 요약 텍스트 생성"""
    parts = []
    mh = rec.get("market_health", "NORMAL")
    mc = rec.get("market_change", 0)
    if mc != 0:
        parts.append(f"KOSPI {mc:+.1f}%")

    # 레짐
    regime = rec.get("cross_regime", "NORMAL")
    if regime != "NORMAL":
        parts.append(f"체제: {regime}")

    # HOT 섹터
    relay = rec.get("relay_summary", "")
    if relay:
        hots = [s.strip().replace("HOT:", "") for s in relay.split("|") if "HOT" in s]
        if hots:
            parts.append(f"HOT: {', '.join(hots)}")

    # 경고
    warning = rec.get("warning", "")
    if warning:
        parts.append(f"경고: {warning}")

    return " | ".join(parts) if parts else "시장 정보 수집 중"


def _extract_hot_sectors(rec: dict) -> list:
    """추천 리포트에서 HOT 섹터 리스트 추출"""
    relay = rec.get("relay_summary", "")
    sectors = []
    for part in relay.split("|"):
        part = part.strip()
        if "HOT" in part:
            sector = part.replace("HOT:", "").strip()
            if sector and sector not in sectors:
                sectors.append(sector)
    return sectors


def _resolve_name(code: str, raw_name: str) -> str:
    """코드가 이름으로 들어온 경우 universe.json → pykrx 순으로 해석"""
    if raw_name and not raw_name.isdigit():
        return raw_name
    # universe.json fallback
    try:
        uni_path = _DATA_STORE / "universe.json"
        if uni_path.exists():
            import json as _jrn
            with open(uni_path, "r", encoding="utf-8") as _f:
                uni = _jrn.load(_f)
            name = uni.get(code, {}).get("name", "")
            if name:
                return name
    except Exception:
        pass
    try:
        from pykrx import stock
        resolved = stock.get_market_ticker_name(code)
        if resolved:
            return resolved
    except Exception:
        pass
    return raw_name or code


def _extract_top_stocks(rec: dict, max_n: int = 5) -> list:
    """추천 종목 상위 N개 → FLOWX 형식"""
    from data.upload_short import _score_to_grade, _determine_signal_type

    stocks = rec.get("stocks", [])
    result = []
    for s in stocks[:max_n]:
        grade = _score_to_grade(
            s.get("total_score", 0),
            s.get("confidence", ""),
            s.get("nat_power_grade", ""),
        )
        tv_ratio = s.get("tv_ratio", 1.0) or 1.0
        inst_support = "기OK" in s.get("nationality_detail", "")
        signal_type = _determine_signal_type(
            grade, inst_support, tv_ratio,
            s.get("tv_pattern", "NORMAL"),
        )
        name = _resolve_name(s["code"], s["name"])
        result.append({
            "code": s["code"],
            "name": name,
            "score": round(s.get("total_score", 0), 1),
            "grade": grade,
            "signal_type": signal_type,
            "confidence": s.get("confidence", ""),
            "regime": s.get("regime", "NORMAL"),
        })
    return result


def _extract_news_picks(rec: dict) -> list:
    """추천 종목의 뉴스 시그널 추출 → news_picks"""
    picks = []
    for s in rec.get("stocks", []):
        nd = s.get("news_detail", "")
        if nd and nd != "뉴스 없음":
            picks.append({
                "ticker": s["code"],
                "name": s["name"],
                "title": nd[:100],  # 100자 제한
            })
    return picks[:5]  # 최대 5건


def _extract_events(events: dict) -> list:
    """global_events.json → 브리핑용 이벤트 리스트"""
    result = []
    for ev in events.get("economic", []):
        result.append({
            "date": ev.get("date", ""),
            "event": ev.get("event", ""),
            "impact": ev.get("impact", ""),
            "direction": ev.get("direction", ""),
            "kr_sectors": ev.get("kr_sectors", []),
        })
    for ev in events.get("earnings", []):
        result.append({
            "date": ev.get("date", ""),
            "event": f"{ev.get('company', '')} 실적발표",
            "impact": ev.get("impact", "MEDIUM"),
            "direction": ev.get("direction", "NEUTRAL"),
            "kr_sectors": ev.get("kr_sectors", []),
        })
    return result[:10]  # 최대 10건


def _extract_guardian_alerts(guardian: dict) -> list:
    """guardian_latest.json → EXIT/REDUCE 알림"""
    alerts = []
    for v in guardian.get("verdicts", []):
        if v.get("action") in ("EXIT", "REDUCE"):
            alerts.append({
                "code": v.get("code", ""),
                "name": v.get("name", ""),
                "action": v.get("action", ""),
                "risk_score": v.get("risk_score", 0),
                "key_reason": v.get("key_reason", "")[:100],
            })
    return alerts


def _extract_raw_indicators(nw: dict) -> dict:
    """nightwatch_report.json → 주요 해외지표 dict"""
    raw = nw.get("raw_indicators", {})
    result = {}
    mapping = {
        "sp500": "ES", "nasdaq": "NQ", "vix": "VIX",
        "tnx": "TNX", "usdkrw": "USDKRW", "oil": "CL", "gold": "GOLD",
    }
    for key, raw_key in mapping.items():
        ind = raw.get(raw_key, {})
        if ind:
            result[key] = {
                "value": ind.get("value", 0),
                "change_pct": round(ind.get("change_pct", 0), 2),
            }
    return result


# ══════════════════════════════════════════
#  메인: 브리핑 생성
# ══════════════════════════════════════════

def _extract_policy_content(policy: dict) -> dict:
    """policy_latest.json → 정책 콘텐츠"""
    matched = policy.get("matched_sectors", [])
    if not matched:
        return {}
    sectors = []
    for ms in matched[:5]:
        stocks = [s[1] for s in ms.get("kr_stocks", [])[:3]]
        sectors.append({
            "sector": ms["sector"],
            "keywords": ms.get("keywords_found", [])[:3],
            "article_count": ms.get("article_count", 0),
            "relevance": ms.get("relevance", 0),
            "top_stocks": stocks,
        })
    return {
        "summary": policy.get("summary", ""),
        "sectors": sectors,
        "total_articles": len(policy.get("articles", [])),
    }


def _extract_macro_indicators(events: dict) -> dict:
    """global_events.json → 매크로 지표 (BOK + Alpha Vantage)"""
    macro = events.get("macro_indicators", {})
    gm = events.get("global_markets", {})
    result = {}
    if "base_rate" in macro:
        result["base_rate"] = macro["base_rate"]["value"]
    if "usd_krw" in macro:
        result["usd_krw"] = macro["usd_krw"]["value"]
    elif "usd_krw" in gm:
        result["usd_krw"] = gm["usd_krw"]["value"]
    if "cpi_yoy" in macro:
        result["cpi_yoy"] = macro["cpi_yoy"]["value"]
    if "wti" in gm:
        result["wti"] = gm["wti"]["value"]
    if "natural_gas" in gm:
        result["natural_gas"] = gm["natural_gas"]["value"]
    return result


def generate_morning_briefing() -> dict:
    """기존 data_store 파일에서 10레이어 브리핑 데이터 수집 → dict 반환

    Layer 1: 시장 체제 (market_health.json + recommendation.json)
    Layer 2: 해외 지표 (nightwatch_report.json)
    Layer 3: 섹터 로테이션 (recommendation.relay_summary)
    Layer 4: 이벤트 경보 (global_events.json)
    Layer 5: 추천 종목 (recommendation.json stocks)
    Layer 6: 뉴스 시그널 (recommendation.json news_detail)
    Layer 7: 포지션 가디언 (guardian_latest.json)
    Layer 8: 원시 지표 (nightwatch raw_indicators)
    Layer 9: 정책 트래커 (policy_latest.json)
    Layer 10: 매크로 경제지표 (global_events → BOK/AV)
    """
    rec = _load_json("recommendation.json")
    nw = _load_json("nightwatch_report.json")
    events = _load_json("global_events.json")
    guardian = _load_json("learning/guardian_latest.json")
    policy = _load_json("policy_latest.json")

    market_status = _map_market_status(
        rec.get("market_health", "NORMAL"),
        rec.get("cross_regime", ""),
    )

    # KOSPI/KOSDAQ 종가 추출
    raw_ind = _extract_raw_indicators(nw)
    kospi_close = None
    kosdaq_close = None
    # nightwatch에서 KOSPI/KOSDAQ 종가 시도
    for key in ("kospi", "KOSPI"):
        v = nw.get(key, {})
        if isinstance(v, dict) and v.get("close"):
            kospi_close = v["close"]
    for key in ("kosdaq", "KOSDAQ"):
        v = nw.get(key, {})
        if isinstance(v, dict) and v.get("close"):
            kosdaq_close = v["close"]

    briefing = {
        "date": str(date.today()),
        "market_status": market_status,
        "kospi_close": kospi_close,
        "kosdaq_close": kosdaq_close,
        "us_summary": rec.get("us_market_note", ""),
        "kr_summary": _build_kr_summary(rec),
        "cross_regime": rec.get("cross_regime", "NORMAL"),
        "cross_regime_detail": rec.get("cross_regime_detail", ""),
        "warning": rec.get("warning", ""),
        "news_picks": _extract_news_picks(rec),
        "sector_focus": _extract_hot_sectors(rec),
        "global_events": _extract_events(events),
        "top_stocks": _extract_top_stocks(rec),
        "guardian_alerts": _extract_guardian_alerts(guardian),
        "raw_indicators": raw_ind,
        # Layer 9-10: 정책 + 매크로
        "policy_content": _extract_policy_content(policy),
        "macro_indicators": _extract_macro_indicators(events),
    }

    n_policy = len(briefing.get("policy_content", {}).get("sectors", []))
    n_macro = len(briefing.get("macro_indicators", {}))
    logger.info(
        f"[FLOWX] 브리핑 생성: {briefing['date']} | "
        f"상태={market_status} | 종목={len(briefing['top_stocks'])} | "
        f"이벤트={len(briefing['global_events'])} | "
        f"가디언={len(briefing['guardian_alerts'])} | "
        f"정책={n_policy}섹터 | 매크로={n_macro}지표"
    )
    return briefing


# ══════════════════════════════════════════
#  Supabase 업로드
# ══════════════════════════════════════════

def upload_morning_briefing(briefing: dict) -> bool:
    """morning_briefings 테이블에 upsert (같은 date 있으면 update)"""
    client = _get_client()
    if not client:
        return False

    try:
        # full_report: 개인 텔레그램용 전체 본문 (FLOWX에는 비공개)
        full_report = _format_full_briefing(briefing)

        # Supabase morning_briefings 실제 컬럼에 맞춤
        # (warning, global_events, top_stocks, guardian_alerts,
        #  raw_indicators, policy_content, macro_indicators → 미존재, full_report에 통합)
        row = {
            "date": briefing["date"],
            "market_status": briefing["market_status"],
            "kospi_close": briefing.get("kospi_close"),
            "kosdaq_close": briefing.get("kosdaq_close"),
            "us_summary": briefing.get("us_summary", ""),
            "kr_summary": briefing.get("kr_summary", ""),
            "news_picks": json.dumps(briefing.get("news_picks", []), ensure_ascii=False),
            "sector_focus": json.dumps(briefing.get("sector_focus", []), ensure_ascii=False),
            "full_report": full_report,
        }

        client.table("morning_briefings").upsert(
            [row], on_conflict="date"
        ).execute()

        logger.info(f"[FLOWX] 모닝 브리핑 업로드 완료: {briefing['date']}")
        return True
    except Exception as e:
        logger.error(f"[FLOWX] 모닝 브리핑 업로드 실패: {e}")
        return False


# ══════════════════════════════════════════
#  텔레그램 발송
# ══════════════════════════════════════════

def _tg_send(text: str, chat_id: str = ""):
    """텔레그램 메시지 발송"""
    from dotenv import load_dotenv
    load_dotenv(_ENV_PATH)

    token = os.environ.get("TG_TOKEN", "") or os.environ.get("TELEGRAM_TOKEN", "")
    if not chat_id:
        chat_id = os.environ.get("TELEGRAM_CHAT_ID", "")
    if not token or not chat_id:
        logger.warning("텔레그램 토큰/채팅ID 미설정")
        return

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    try:
        requests.post(url, json={
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }, timeout=10)
    except Exception as e:
        logger.warning(f"텔레그램 발송 실패: {e}")


def _format_full_briefing(b: dict) -> str:
    """전체 브리핑 텍스트 (개인채널용)"""
    lines = []
    lines.append(f"<b>FLOWX 모닝 브리핑 {b['date']}</b>")
    lines.append("")

    # 시장 상태
    status_emoji = {"BULL": "🟢", "BEAR": "🔴", "CAUTION": "🟡", "NEUTRAL": "⚪"}
    lines.append(f"{status_emoji.get(b['market_status'], '⚪')} 시장: <b>{b['market_status']}</b>")

    # 미국 지표
    if b.get("us_summary"):
        lines.append(f"🇺🇸 {b['us_summary']}")

    # 한국 요약
    if b.get("kr_summary"):
        lines.append(f"🇰🇷 {b['kr_summary']}")

    # 체제 경고
    if b.get("warning"):
        lines.append(f"⚠️ {b['warning']}")

    # 해외 지표
    ri = b.get("raw_indicators", {})
    if ri:
        lines.append("")
        lines.append("<b>해외 지표</b>")
        for key, label in [("sp500", "S&P500"), ("nasdaq", "나스닥"),
                           ("vix", "VIX"), ("tnx", "미국10Y"),
                           ("usdkrw", "원/달러"), ("oil", "WTI"), ("gold", "금")]:
            ind = ri.get(key, {})
            if ind:
                val = ind.get("value", 0)
                chg = ind.get("change_pct", 0)
                arrow = "+" if chg > 0 else ""
                lines.append(f"  {label}: {val:,.1f} ({arrow}{chg:.1f}%)")

    # HOT 섹터
    sf = b.get("sector_focus", [])
    if sf:
        lines.append("")
        lines.append(f"🔥 HOT 섹터: {', '.join(sf)}")

    # 이벤트
    evts = b.get("global_events", [])
    if evts:
        lines.append("")
        lines.append("<b>주요 이벤트</b>")
        for ev in evts[:5]:
            impact_emoji = {"HIGH": "🔴", "MEDIUM": "🟡", "LOW": "🟢"}.get(ev.get("impact"), "⚪")
            lines.append(f"  {impact_emoji} {ev.get('date', '')} {ev.get('event', '')}")

    # 추천 종목
    stocks = b.get("top_stocks", [])
    if stocks:
        lines.append("")
        lines.append("<b>AI 추천 TOP</b>")
        for i, s in enumerate(stocks, 1):
            lines.append(
                f"  {i}. {s['name']}({s['code']}) [{s['grade']}] "
                f"점수 {s['score']} {s['signal_type']}"
            )

    # 포지션 가디언
    alerts = b.get("guardian_alerts", [])
    if alerts:
        lines.append("")
        lines.append("<b>포지션 경고</b>")
        for a in alerts:
            icon = "🚨" if a["action"] == "EXIT" else "⚠️"
            lines.append(f"  {icon} {a['name']} → {a['action']} (리스크 {a['risk_score']:.0f})")

    # 매크로 지표 (BOK + Alpha Vantage)
    macro = b.get("macro_indicators", {})
    if macro:
        lines.append("")
        lines.append("<b>매크로 지표</b>")
        parts = []
        if "base_rate" in macro:
            parts.append(f"기준금리 {macro['base_rate']}%")
        if "usd_krw" in macro:
            parts.append(f"원/달러 {macro['usd_krw']:,.0f}")
        if "cpi_yoy" in macro:
            parts.append(f"CPI {macro['cpi_yoy']}%")
        if parts:
            lines.append(f"  {' | '.join(parts)}")
        parts2 = []
        if "wti" in macro:
            parts2.append(f"WTI ${macro['wti']:.2f}")
        if "natural_gas" in macro:
            parts2.append(f"NG ${macro['natural_gas']:.2f}")
        if parts2:
            lines.append(f"  {' | '.join(parts2)}")

    # 정책 트래커
    pc = b.get("policy_content", {})
    if pc and pc.get("sectors"):
        lines.append("")
        lines.append("<b>정책 수혜 섹터</b>")
        for ps in pc["sectors"][:3]:
            stocks = ", ".join(ps.get("top_stocks", [])[:3])
            lines.append(f"  🏛 {ps['sector']}: {stocks}")

    return "\n".join(lines)


def _format_summary_briefing(b: dict) -> str:
    """요약 브리핑 텍스트 (FLOWX 공개채널용 — 상위 3뉴스 + 시장상태만)"""
    lines = []
    status_emoji = {"BULL": "🟢", "BEAR": "🔴", "CAUTION": "🟡", "NEUTRAL": "⚪"}
    lines.append(f"<b>FLOWX 모닝 브리핑 {b['date']}</b>")
    lines.append(f"{status_emoji.get(b['market_status'], '⚪')} 시장: <b>{b['market_status']}</b>")

    if b.get("us_summary"):
        lines.append(f"🇺🇸 {b['us_summary']}")
    if b.get("kr_summary"):
        lines.append(f"🇰🇷 {b['kr_summary']}")

    # HOT 섹터
    sf = b.get("sector_focus", [])
    if sf:
        lines.append(f"🔥 HOT: {', '.join(sf)}")

    # 주요 이벤트 3건만
    evts = b.get("global_events", [])
    if evts:
        lines.append("")
        for ev in evts[:3]:
            lines.append(f"📌 {ev.get('date', '')} {ev.get('event', '')}")

    # 정책 수혜 (공개용 — 섹터명만)
    pc = b.get("policy_content", {})
    if pc and pc.get("sectors"):
        sector_names = [ps["sector"] for ps in pc["sectors"][:3]]
        lines.append(f"🏛 정책: {', '.join(sector_names)}")

    # 추천 종목 — 블러 처리 (종목명만, 상세 없음)
    stocks = b.get("top_stocks", [])
    if stocks:
        lines.append("")
        lines.append(f"AI 추천 {len(stocks)}종목 대기 중...")
        lines.append("👉 FLOWX에서 전체 확인 → flowx.kr")

    return "\n".join(lines)


def send_briefing_telegram(briefing: dict):
    """텔레그램 듀얼 발송"""
    from dotenv import load_dotenv
    load_dotenv(_ENV_PATH)

    # 개인채널: 전체 브리핑
    full_msg = _format_full_briefing(briefing)
    _tg_send(full_msg)

    # FLOWX 공개채널: 요약본 (FLOWX_CHAT_ID가 있는 경우만)
    flowx_chat = os.environ.get("FLOWX_CHAT_ID", "")
    if flowx_chat:
        summary_msg = _format_summary_briefing(briefing)
        _tg_send(summary_msg, chat_id=flowx_chat)
        logger.info("[FLOWX] 공개채널 요약 발송 완료")
    else:
        logger.info("[FLOWX] FLOWX_CHAT_ID 미설정 — 공개채널 미발송")


# ══════════════════════════════════════════
#  통합 파이프라인
# ══════════════════════════════════════════

def run_morning_briefing(upload: bool = True, telegram: bool = False) -> dict:
    """원클릭 파이프라인: 생성 → 업로드 → 텔레그램

    Args:
        upload: Supabase 업로드 여부
        telegram: 텔레그램 발송 여부

    Returns:
        생성된 브리핑 dict
    """
    briefing = generate_morning_briefing()

    if upload:
        success = upload_morning_briefing(briefing)
        if not success:
            logger.warning("[FLOWX] Supabase 업로드 실패 — 테이블이 없을 수 있습니다")

    if telegram:
        send_briefing_telegram(briefing)

    return briefing


# ══════════════════════════════════════════
#  CLI
# ══════════════════════════════════════════

def _print_briefing(b: dict):
    """콘솔 출력"""
    print(f"\n{'='*60}")
    print(f"  FLOWX 모닝 브리핑 - {b['date']}")
    print(f"{'='*60}")
    print(f"  시장 상태: {b['market_status']}")
    print(f"  미국: {b.get('us_summary', '-')}")
    print(f"  한국: {b.get('kr_summary', '-')}")
    print(f"  체제: {b.get('cross_regime', 'NORMAL')} - {b.get('cross_regime_detail', '')}")
    if b.get("warning"):
        print(f"  경고: {b['warning']}")
    print()

    # 해외 지표
    ri = b.get("raw_indicators", {})
    if ri:
        print("  [해외 지표]")
        for key, label in [("sp500", "S&P500"), ("nasdaq", "나스닥"),
                           ("vix", "VIX"), ("oil", "WTI"), ("gold", "금"),
                           ("usdkrw", "원/달러"), ("tnx", "미10Y")]:
            ind = ri.get(key, {})
            if ind:
                print(f"    {label:>8}: {ind['value']:>10,.1f} ({ind['change_pct']:+.1f}%)")
        print()

    # HOT 섹터
    sf = b.get("sector_focus", [])
    if sf:
        print(f"  [HOT 섹터] {', '.join(sf)}")
        print()

    # 이벤트
    evts = b.get("global_events", [])
    if evts:
        print("  [주요 이벤트]")
        for ev in evts[:5]:
            print(f"    {ev['impact']:>6} {ev['date']} {ev['event']}")
        print()

    # 추천 종목
    stocks = b.get("top_stocks", [])
    if stocks:
        print("  [AI 추천 TOP]")
        for i, s in enumerate(stocks, 1):
            print(f"    {i}. {s['name']:>12}({s['code']}) [{s['grade']:>3}] "
                  f"점수 {s['score']:>5.1f} {s['signal_type']}")
        print()

    # 포지션 가디언
    alerts = b.get("guardian_alerts", [])
    if alerts:
        print("  [포지션 경고]")
        for a in alerts:
            print(f"    {a['action']:>6} {a['name']}({a['code']}) "
                  f"리스크 {a['risk_score']:.0f}")
        print()

    print(f"{'='*60}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    parser = argparse.ArgumentParser(description="FLOWX 모닝 브리핑")
    parser.add_argument("--upload", action="store_true", help="Supabase 업로드")
    parser.add_argument("--tg", action="store_true", help="텔레그램 발송")
    parser.add_argument("--test", action="store_true", help="테스트 모드 (생성만)")
    args = parser.parse_args()

    # PYTHONPATH 설정
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

    briefing = generate_morning_briefing()
    _print_briefing(briefing)

    if args.test:
        print("[TEST] 생성만 완료 (업로드/텔레그램 미발송)")
    else:
        if args.upload:
            ok = upload_morning_briefing(briefing)
            print(f"Supabase 업로드: {'성공' if ok else '실패'}")
        if args.tg:
            send_briefing_telegram(briefing)
            print("텔레그램 발송 완료")
        if not args.upload and not args.tg:
            print("[INFO] --upload 또는 --tg 옵션 사용하세요")
