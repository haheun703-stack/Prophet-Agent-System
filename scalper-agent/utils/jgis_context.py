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

    return {
        "blocked": len(reasons) > 0,
        "reasons": reasons,
        "warnings": warnings,
    }


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
