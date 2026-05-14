# -*- coding: utf-8 -*-
"""정보봇 컨텍스트 조회 + 진입 차단 판단 (단타봇 측 통합 모듈).

정보봇이 제공하는 4가지 컨텍스트를 단타봇 자동매매에 활용:
- 외국인/기관 streak (intelligence_supply_demand)
- 섹터별 수급 (sector_investor_flow)
- ETF 수급 (etf_investor_flow)
- 프로그램매매 KOSPI vs KOSDAQ 비대칭 (program_trading)

활용 패턴:
1. entry_filter — 진입 직전 차단 (외국인 6일 매도 + 섹터 매크로)
2. watchlist 확장 — ETF 수혜 섹터 종목 자동 추가
3. morning_state 확장 — 06:00 모닝 컨텍스트 주입

캐시: 1시간 (운영 중 봇이 같은 데이터 N번 호출 방지)
"""
import logging
import os
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional

# shared 모듈 path 추가 (단타봇 외부 디렉토리)
_BODYHUNTER_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_BODYHUNTER_ROOT) not in sys.path:
    sys.path.insert(0, str(_BODYHUNTER_ROOT))

logger = logging.getLogger("BH.JgisContext")

# 5분 캐시 (장중 변화 빠른 데이터)
_CACHE: Dict[str, tuple] = {}  # key -> (data, expires_at)
_CACHE_TTL = 300  # 5분


def _cache_get(key: str):
    entry = _CACHE.get(key)
    if entry and entry[1] > time.time():
        return entry[0]
    return None


def _cache_set(key: str, data, ttl: int = _CACHE_TTL):
    _CACHE[key] = (data, time.time() + ttl)


def _get_client():
    try:
        from shared.supabase_client import get_client
        return get_client()
    except Exception as e:
        logger.warning(f"[JGIS] supabase 클라이언트 실패: {e}")
        return None


# ─────────────────────────────────────────────
# 1) 외국인/기관 전체 streak
# ─────────────────────────────────────────────
def get_supply_streak(date: Optional[str] = None) -> Optional[dict]:
    """전일 외국인/기관 연속 매수/매도 streak.

    Returns:
        {date, foreign_streak, inst_streak, foreign_net, institution_net,
         foreign_trend, institution_trend, summary}
        streak 음수 = 연속 매도, 양수 = 연속 매수
    """
    cache_key = f"streak:{date or 'latest'}"
    cached = _cache_get(cache_key)
    if cached:
        return cached

    c = _get_client()
    if not c:
        return None
    try:
        q = c.table("intelligence_supply_demand").select("*")
        if date:
            q = q.eq("date", date)
        else:
            q = q.order("date", desc=True)
        r = q.limit(1).execute()
        if not r.data:
            return None
        _cache_set(cache_key, r.data[0])
        return r.data[0]
    except Exception as e:
        logger.warning(f"[JGIS] streak 조회 실패: {e}")
        return None


# ─────────────────────────────────────────────
# 2) 섹터별 수급
# ─────────────────────────────────────────────
def get_sector_supply(sector: str, date: Optional[str] = None) -> Optional[dict]:
    """특정 섹터의 외국인/기관 일별 순매수.

    sector: '반도체', '2차전지', '바이오' 등 한글
    Returns:
        {date, sector, foreign_net_amt, inst_net_amt, top_foreign_sell, ...}
    """
    cache_key = f"sector:{sector}:{date or 'latest'}"
    cached = _cache_get(cache_key)
    if cached:
        return cached

    c = _get_client()
    if not c:
        return None
    try:
        q = c.table("sector_investor_flow").select("*").eq("sector", sector)
        if date:
            q = q.eq("date", date)
        else:
            q = q.order("date", desc=True)
        r = q.limit(1).execute()
        if not r.data:
            return None
        _cache_set(cache_key, r.data[0])
        return r.data[0]
    except Exception as e:
        logger.warning(f"[JGIS] sector {sector} 조회 실패: {e}")
        return None


def get_all_sectors(date: Optional[str] = None) -> List[dict]:
    """모든 섹터 수급 (TOP 외인/기관 매수 섹터 분석용)."""
    cache_key = f"sectors_all:{date or 'latest'}"
    cached = _cache_get(cache_key)
    if cached:
        return cached

    c = _get_client()
    if not c:
        return []
    try:
        q = c.table("sector_investor_flow").select("*")
        if date:
            q = q.eq("date", date)
        else:
            # 최신 날짜만
            latest_q = c.table("sector_investor_flow").select("date").order("date", desc=True).limit(1).execute()
            if latest_q.data:
                q = q.eq("date", latest_q.data[0]["date"])
        r = q.execute()
        _cache_set(cache_key, r.data or [])
        return r.data or []
    except Exception as e:
        logger.warning(f"[JGIS] all sectors 실패: {e}")
        return []


# ─────────────────────────────────────────────
# 3) ETF 수급 → 수혜 섹터 추출
# ─────────────────────────────────────────────
def get_etf_top_inflow(date: Optional[str] = None, top_n: int = 5) -> List[dict]:
    """외국인+기관 순매수 상위 ETF TOP N (다음날 수혜 섹터 추정용)."""
    cache_key = f"etf_top:{date or 'latest'}:{top_n}"
    cached = _cache_get(cache_key)
    if cached:
        return cached

    c = _get_client()
    if not c:
        return []
    try:
        q = c.table("etf_investor_flow").select("*")
        if date:
            q = q.eq("date", date)
        else:
            latest_q = c.table("etf_investor_flow").select("date").order("date", desc=True).limit(1).execute()
            if latest_q.data:
                q = q.eq("date", latest_q.data[0]["date"])
        r = q.execute()
        items = r.data or []
        # 외인+기관 순매수 합계로 정렬
        for it in items:
            it["combined_net"] = (it.get("foreign_net_amt", 0) or 0) + (it.get("institution_net_amt", 0) or 0)
        items.sort(key=lambda x: x["combined_net"], reverse=True)
        result = items[:top_n]
        _cache_set(cache_key, result)
        return result
    except Exception as e:
        logger.warning(f"[JGIS] ETF top inflow 실패: {e}")
        return []


# ─────────────────────────────────────────────
# 4) 프로그램매매 KOSPI vs KOSDAQ 비대칭
# ─────────────────────────────────────────────
def get_program_asymmetry(date: Optional[str] = None) -> Optional[dict]:
    """KOSPI vs KOSDAQ 프로그램매매 비대칭 판단.

    Returns:
        {date, kospi_net, kosdaq_net, asymmetry: 'KOSPI_FAVOR' | 'KOSDAQ_FAVOR' | 'NEUTRAL'}
    """
    cache_key = f"program:{date or 'latest'}"
    cached = _cache_get(cache_key)
    if cached:
        return cached

    c = _get_client()
    if not c:
        return None
    try:
        q = c.table("program_trading").select("*")
        if date:
            q = q.eq("date", date)
        else:
            latest_q = c.table("program_trading").select("date").order("date", desc=True).limit(1).execute()
            if latest_q.data:
                q = q.eq("date", latest_q.data[0]["date"])
        r = q.execute()
        items = r.data or []
        kospi = next((i for i in items if i.get("market") == "KOSPI"), None)
        kosdaq = next((i for i in items if i.get("market") == "KOSDAQ"), None)
        if not (kospi and kosdaq):
            return None
        kn = kospi.get("total_net_amt", 0) or 0
        dn = kosdaq.get("total_net_amt", 0) or 0
        if kn > 0 and dn < -100000:  # KOSPI +, KOSDAQ -1000억+
            asymmetry = "KOSPI_FAVOR"
        elif dn > 0 and kn < -100000:
            asymmetry = "KOSDAQ_FAVOR"
        else:
            asymmetry = "NEUTRAL"
        data = {
            "date": kospi.get("date"),
            "kospi_net": kn,
            "kosdaq_net": dn,
            "asymmetry": asymmetry,
        }
        _cache_set(cache_key, data)
        return data
    except Exception as e:
        logger.warning(f"[JGIS] program asymmetry 실패: {e}")
        return None


# ─────────────────────────────────────────────
# 5) 진입 차단 판단 (통합 entry_filter)
# ─────────────────────────────────────────────
def check_entry_blocked(code: str, sector: Optional[str] = None,
                        market: Optional[str] = None) -> dict:
    """진입 직전 차단 판단 — 정보봇 4가지 신호 통합 평가.

    Args:
        code: 종목코드
        sector: 섹터 ('반도체', '2차전지' 등 한글)
        market: 'KOSPI' or 'KOSDAQ'

    Returns:
        {blocked: bool, reasons: [str], warnings: [str]}
        blocked=True면 매수 진입 차단 권장
        warnings는 차단 안 하지만 주의사항 (분할/SL 타이트)
    """
    reasons = []
    warnings = []

    # 1) 외국인 6일 연속 매도 → 보수적 (시나리오 1)
    streak = get_supply_streak()
    if streak:
        f_streak = streak.get("foreign_streak", 0) or 0
        i_streak = streak.get("inst_streak", 0) or 0
        if f_streak <= -6:
            reasons.append(f"외국인 {abs(f_streak)}일 연속 매도")
        elif f_streak <= -3:
            warnings.append(f"외국인 {abs(f_streak)}일 매도 (보수적 진입)")
        if i_streak <= -5:
            warnings.append(f"기관 {abs(i_streak)}일 매도")

    # 2) 섹터 매크로 — 반도체 등 쌍끌이 매도면 차단
    if sector:
        sec = get_sector_supply(sector)
        if sec:
            f_amt = sec.get("foreign_net_amt", 0) or 0
            i_amt = sec.get("inst_net_amt", 0) or 0
            # 외인+기관 쌍끌이 매도 (각 -1000억+)
            if f_amt < -100000 and i_amt < -100000:
                reasons.append(f"{sector} 외인+기관 쌍끌이 매도")
            elif f_amt < -50000:
                warnings.append(f"{sector} 외인 매도 우세")

    # 3) 프로그램매매 비대칭 — KOSDAQ 회피 (시나리오 2)
    if market:
        prog = get_program_asymmetry()
        if prog:
            asy = prog.get("asymmetry", "NEUTRAL")
            if market == "KOSDAQ" and asy == "KOSPI_FAVOR":
                warnings.append(f"프로그램 KOSPI 우세 (KOSDAQ {prog.get('kosdaq_net', 0):+,}억)")
            elif market == "KOSPI" and asy == "KOSDAQ_FAVOR":
                warnings.append(f"프로그램 KOSDAQ 우세")

    # 4) ETF 시장 방향성 — '강한 하락' 시 매수 차단 (5/15 신규 100개 ETF)
    etf_summary = get_etf_summary()
    if etf_summary:
        direction = etf_summary.get("direction", "")
        if direction in ("강한 하락",):
            reasons.append(f"ETF 시장 방향 '{direction}'")
        elif direction == "하락":
            warnings.append(f"ETF 시장 방향 '하락' (보수적)")

    # 5) 섹터 ETF 쌍끌이매도 → 매수 차단
    if sector:
        etf_judgment = get_etf_sector_judgment(sector)
        if etf_judgment == "쌍끌이매도":
            reasons.append(f"{sector} ETF 쌍끌이매도")
        elif etf_judgment and "매도" in etf_judgment:
            warnings.append(f"{sector} ETF {etf_judgment}")

    # 6) 그룹주 ETF 매도 → 그룹사 회피 (삼성/현대차)
    # code 매핑: 005930(삼성전자), 010140(삼성중공업), 207940(삼성바이오), 005380(현대차), 012330(현대모비스) 등
    SAMSUNG_CODES = {"005930", "009150", "006400", "028260", "032830",
                     "000810", "018260", "207940", "010140", "207940"}
    HYUNDAI_CODES = {"005380", "012330", "086280", "001120", "011210"}
    group_judgment = None
    group_label = None
    if code in SAMSUNG_CODES:
        group_judgment = get_etf_sector_judgment("삼성그룹")
        group_label = "삼성그룹"
    elif code in HYUNDAI_CODES:
        group_judgment = get_etf_sector_judgment("현대차그룹")
        group_label = "현대차그룹"
    if group_judgment == "쌍끌이매도":
        reasons.append(f"{group_label} ETF 쌍끌이매도")
    elif group_judgment and "매도" in str(group_judgment):
        warnings.append(f"{group_label} ETF {group_judgment}")

    return {
        "blocked": len(reasons) > 0,
        "reasons": reasons,
        "warnings": warnings,
    }


# ─────────────────────────────────────────────
# 5.5) ETF 시장 요약 (etf_investor_summary — 5/15 신규 100개 확장)
# ─────────────────────────────────────────────
def get_etf_summary(date: Optional[str] = None) -> Optional[dict]:
    """etf_investor_summary 조회 — 시장 방향 + 다음날 수혜 후보.

    Returns:
        {date, direction, inst_direction_amt, top_sectors, beneficiaries}
        direction: '강한 상승' / '상승' / '혼조' / '하락' / '강한 하락'
    """
    cache_key = f"etf_summary:{date or 'latest'}"
    cached = _cache_get(cache_key)
    if cached:
        return cached

    c = _get_client()
    if not c:
        return None
    try:
        q = c.table("etf_investor_summary").select("*")
        if date:
            q = q.eq("date", date)
        else:
            q = q.order("date", desc=True)
        r = q.limit(1).execute()
        if not r.data:
            return None
        _cache_set(cache_key, r.data[0])
        return r.data[0]
    except Exception as e:
        logger.warning(f"[JGIS] etf_summary 조회 실패: {e}")
        return None


def get_etf_beneficiaries(date: Optional[str] = None) -> List[dict]:
    """내일 수혜 후보 종목 리스트 (etf_investor_summary.beneficiaries JSONB).

    Returns:
        [{ticker, name, sector, reason, foreign_amt, inst_amt}, ...]
    """
    summary = get_etf_summary(date)
    if not summary:
        return []
    bs = summary.get("beneficiaries", [])
    if isinstance(bs, str):
        import json
        try:
            bs = json.loads(bs)
        except Exception:
            return []
    return bs or []


def get_etf_sector_judgment(sector_class: str, date: Optional[str] = None) -> Optional[str]:
    """특정 섹터의 ETF 수급 판정 (etf_investor_summary.top_sectors).

    sector_class: '반도체', '2차전지', '바이오', '자동차', '삼성그룹' 등
    Returns:
        '쌍끌이매수' / '외인주도매수' / '기관주도매수' / '쌍끌이매도' / ... 또는 None
    """
    summary = get_etf_summary(date)
    if not summary:
        return None
    top = summary.get("top_sectors", [])
    if isinstance(top, str):
        import json
        try:
            top = json.loads(top)
        except Exception:
            return None
    for s in (top or []):
        if s.get("sector") == sector_class or sector_class in str(s.get("sector", "")):
            return s.get("judgment")
    return None


# ─────────────────────────────────────────────
# 6) 모닝 컨텍스트 저장 (#2 watchlist + #3 morning_state 통합)
# ─────────────────────────────────────────────
def build_morning_context() -> dict:
    """정보봇 4가지 컨텍스트를 한 번에 수집 → 단타봇 모닝 분석용.

    Returns:
        {date, foreign_streak, inst_streak, summary,
         top_sell_sectors: [...], top_buy_sectors: [...],
         etf_top_inflow: [...],
         program: {asymmetry, kospi_net, kosdaq_net},
         generated_at}
    """
    import json
    from datetime import datetime

    streak = get_supply_streak() or {}
    sectors = get_all_sectors()
    etf_top = get_etf_top_inflow(top_n=10)
    program = get_program_asymmetry() or {}
    etf_summary = get_etf_summary() or {}
    etf_beneficiaries = get_etf_beneficiaries() or []

    # 섹터 정렬: 외인+기관 합산 매수 TOP / 매도 TOP
    for s in sectors:
        s["combined_net"] = (s.get("foreign_net_amt", 0) or 0) + (s.get("inst_net_amt", 0) or 0)
    sectors_sorted = sorted(sectors, key=lambda x: x["combined_net"], reverse=True)
    top_buy = sectors_sorted[:5]
    top_sell = sectors_sorted[-5:][::-1]  # 매도 우세 TOP 5 (역순)

    return {
        "date": streak.get("date"),
        "foreign_streak": streak.get("foreign_streak"),
        "inst_streak": streak.get("inst_streak"),
        "foreign_trend": streak.get("foreign_trend"),
        "institution_trend": streak.get("institution_trend"),
        "summary": streak.get("summary"),
        "top_buy_sectors": [
            {"sector": s["sector"], "combined_net": s["combined_net"],
             "foreign_net": s.get("foreign_net_amt", 0),
             "inst_net": s.get("inst_net_amt", 0),
             "top_foreign_buy": s.get("top_foreign_buy")}
            for s in top_buy
        ],
        "top_sell_sectors": [
            {"sector": s["sector"], "combined_net": s["combined_net"],
             "foreign_net": s.get("foreign_net_amt", 0),
             "inst_net": s.get("inst_net_amt", 0),
             "top_foreign_sell": s.get("top_foreign_sell")}
            for s in top_sell
        ],
        "etf_top_inflow": [
            {"ticker": e.get("ticker"), "name": e.get("name"),
             "sector": e.get("sector"), "category": e.get("category"),
             "combined_net": e.get("combined_net", 0),
             "foreign_streak": e.get("foreign_streak", 0)}
            for e in etf_top
        ],
        "program": {
            "asymmetry": program.get("asymmetry"),
            "kospi_net": program.get("kospi_net"),
            "kosdaq_net": program.get("kosdaq_net"),
        },
        "etf_market": {
            "direction": etf_summary.get("direction"),
            "inst_direction_amt": etf_summary.get("inst_direction_amt"),
            "top_sectors": etf_summary.get("top_sectors", []),
        },
        "etf_beneficiaries": etf_beneficiaries,
        "generated_at": datetime.now().isoformat(),
    }


def save_morning_context(out_path: Optional[Path] = None) -> dict:
    """모닝 컨텍스트를 jgis_morning_context.json에 저장.

    Returns:
        저장된 dict (호출자가 텔레그램 알림용으로 활용 가능)
    """
    import json
    from datetime import datetime

    ctx = build_morning_context()
    if out_path is None:
        out_path = Path(__file__).resolve().parent.parent / "data_store" / "jgis_morning_context.json"

    # atomic write
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = out_path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(ctx, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, out_path)
    logger.info(f"[JGIS] morning_context 저장: {out_path.name} — "
                f"streak {ctx.get('foreign_streak')} / "
                f"buy {len(ctx.get('top_buy_sectors', []))} sectors / "
                f"ETF {len(ctx.get('etf_top_inflow', []))}")
    return ctx


def refresh_etf_watchlist(out_path: Optional[Path] = None) -> dict:
    """ETF 수혜 종목 자동 추출 → jgis_etf_watchlist.json 저장 (5/15 16:35 매일).

    etf_investor_summary.beneficiaries 조회 → 다음날 단타봇 진입 후보로 활용.

    Returns:
        {date, direction, beneficiaries_count, beneficiaries: [...], saved_path}
    """
    import json
    from datetime import datetime

    summary = get_etf_summary() or {}
    benef = get_etf_beneficiaries()

    if out_path is None:
        out_path = Path(__file__).resolve().parent.parent / "data_store" / "jgis_etf_watchlist.json"

    data = {
        "date": summary.get("date"),
        "direction": summary.get("direction"),
        "inst_direction_amt": summary.get("inst_direction_amt"),
        "beneficiaries_count": len(benef),
        "beneficiaries": benef,
        "generated_at": datetime.now().isoformat(),
    }

    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = out_path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, out_path)
    logger.info(f"[JGIS] ETF watchlist 저장: {out_path.name} — "
                f"direction={data.get('direction')} / 수혜 {len(benef)}종목")
    data["saved_path"] = str(out_path)
    return data


def format_etf_watchlist_telegram(data: dict) -> str:
    """ETF watchlist 텔레그램 알림 포맷 (16:35)."""
    lines = ["💎 정보봇 ETF 수급 분석 (내일 매수 후보)"]
    lines.append(f"기준일: {data.get('date', '?')}")
    direction = data.get("direction", "?")
    d_emoji = {"강한 상승": "🟢🟢", "상승": "🟢", "혼조": "⚪",
               "하락": "🔴", "강한 하락": "🔴🔴"}.get(direction, "⚪")
    lines.append(f"{d_emoji} 시장 방향: {direction}")
    inst_amt = data.get("inst_direction_amt")
    if inst_amt is not None:
        lines.append(f"  기관 레버-인버스: {inst_amt:+,}")

    benef = data.get("beneficiaries", [])[:10]
    if benef:
        lines.append(f"\n📌 내일 수혜 후보 TOP {len(benef)}:")
        for b in benef:
            ticker = b.get("ticker", "?")
            name = b.get("name", "?")
            reason = b.get("reason", "")
            lines.append(f"  • {name}({ticker}) — {reason}")

    return "\n".join(lines)


def format_morning_context_telegram(ctx: dict) -> str:
    """모닝 브리프 텔레그램용 포맷."""
    lines = ["📊 정보봇 모닝 컨텍스트", f"기준일: {ctx.get('date', '?')}"]
    fs = ctx.get("foreign_streak")
    iss = ctx.get("inst_streak")
    if fs is not None:
        f_emoji = "🔴" if fs < -3 else ("🟡" if fs < 0 else "🟢")
        i_emoji = "🔴" if iss < -3 else ("🟡" if iss < 0 else "🟢")
        lines.append(f"{f_emoji} 외국인 streak: {fs:+d}일 / {i_emoji} 기관: {iss:+d}일")
    sm = ctx.get("summary")
    if sm:
        lines.append(f"  {sm[:80]}")

    # 프로그램 매매
    prog = ctx.get("program", {})
    if prog.get("asymmetry"):
        lines.append(f"\n📈 프로그램매매: {prog['asymmetry']}")
        lines.append(f"  KOSPI {prog.get('kospi_net', 0):+,} / KOSDAQ {prog.get('kosdaq_net', 0):+,}")

    # 매수 섹터 TOP 3
    buy = ctx.get("top_buy_sectors", [])[:3]
    if buy:
        lines.append("\n🟢 외인+기관 매수 섹터 TOP 3:")
        for s in buy:
            lines.append(f"  {s['sector']}: {s['combined_net']:+,}")

    # 매도 섹터 TOP 3
    sell = ctx.get("top_sell_sectors", [])[:3]
    if sell:
        lines.append("\n🔴 매도 섹터 TOP 3 (매수 자제):")
        for s in sell:
            lines.append(f"  {s['sector']}: {s['combined_net']:+,}")

    # ETF TOP 5
    etf = ctx.get("etf_top_inflow", [])[:5]
    if etf:
        lines.append("\n💎 ETF 순매수 TOP 5 (수혜 섹터):")
        for e in etf:
            lines.append(f"  {e['name']} ({e['sector']}) {e['combined_net']:+,}")

    return "\n".join(lines)


# ─────────────────────────────────────────────
# CLI 검증
# ─────────────────────────────────────────────
if __name__ == "__main__":
    import sys
    import os
    from pathlib import Path
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parent.parent.parent / ".env")

    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")

    print("=== 정보봇 컨텍스트 검증 ===\n")

    # 1) 외국인 streak
    s = get_supply_streak()
    if s:
        print(f"[1] 외국인 streak: {s.get('foreign_streak')}일 / 기관: {s.get('inst_streak')}일")
        print(f"    Summary: {s.get('summary', '')[:100]}")

    # 2) 섹터 (반도체)
    sec = get_sector_supply("반도체")
    if sec:
        print(f"\n[2] 반도체 — 외인 {sec.get('foreign_net_amt'):+,} / 기관 {sec.get('inst_net_amt'):+,}")

    # 3) ETF TOP 5
    etf = get_etf_top_inflow(top_n=5)
    print(f"\n[3] ETF 순매수 TOP 5:")
    for i, e in enumerate(etf, 1):
        print(f"    {i}. {e['name']}({e['ticker']}) {e['sector']} 합산 {e['combined_net']:+,}")

    # 4) 프로그램매매 비대칭
    pa = get_program_asymmetry()
    if pa:
        print(f"\n[4] 프로그램매매: {pa['asymmetry']} | KOSPI {pa['kospi_net']:+,} / KOSDAQ {pa['kosdaq_net']:+,}")

    # 5) check_entry_blocked 예시
    print(f"\n[5] 진입 차단 판단 예시:")
    for code, sector, market in [
        ("005930", "반도체", "KOSPI"),
        ("032580", "기타", "KOSDAQ"),
    ]:
        b = check_entry_blocked(code, sector, market)
        status = "❌ BLOCKED" if b["blocked"] else "✅ OK"
        print(f"    {code} ({sector}, {market}): {status}")
        for r in b["reasons"]:
            print(f"       [REJECT] {r}")
        for w in b["warnings"]:
            print(f"       [WARN] {w}")
