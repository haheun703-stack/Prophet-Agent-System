# -*- coding: utf-8 -*-
"""ETF 주도주 보너스 맵 — morning_recommendation 점수 가산용.

Supabase `scalper_etf_leader_picks` 테이블에서 가장 최근 base_date의
TOP 30 주도주 데이터를 로드하여, 종목코드 → 보너스 점수 매핑 반환.

보너스 공식 (보수적 첫 도입):
  - TOP 10 (rank 1~10):   +10점 + cross += 1 (강한 시그널)
  - TOP 20 (rank 11~20):  +6점
  - TOP 30 (rank 21~30):  +3점
  - 추가: etf_count >= 3:  +3점 (다중 ETF 합류 — 강한 신뢰)
  → 최대 +13점 (TOP 10 + 다중 ETF)

dual_buy(+12), surge(+10)와 비교해 적정 수준.
실측 누적되면 향후 조정.

5/17 일요일 구축. AUDIT_BACKLOG §1 "morning_recommendation 보너스 통합" 항목.
"""
import logging
from typing import Dict, Optional, Tuple

logger = logging.getLogger(__name__)

# ── 보너스 파라미터 ────────────────────────────────────
_BONUS_TOP10 = 10
_BONUS_TOP20 = 6
_BONUS_TOP30 = 3
_BONUS_MULTI_ETF = 3      # etf_count >= 3 추가
_MULTI_ETF_THRESHOLD = 3
_CROSS_RANK_THRESHOLD = 10  # TOP 10만 cross += 1


def _calc_bonus(rank: int, etf_count: int) -> Tuple[float, str]:
    """rank + etf_count → (보너스 점수, 사유 태그).

    Returns:
        (bonus_score, tag) — tag 예: "etf_lead(TOP10/4ETF:+13)"
    """
    if rank <= 10:
        base = _BONUS_TOP10
        rank_tag = "TOP10"
    elif rank <= 20:
        base = _BONUS_TOP20
        rank_tag = "TOP20"
    elif rank <= 30:
        base = _BONUS_TOP30
        rank_tag = "TOP30"
    else:
        return 0.0, ""

    multi = _BONUS_MULTI_ETF if etf_count >= _MULTI_ETF_THRESHOLD else 0
    total = base + multi
    etf_info = f"/{etf_count}ETF" if etf_count >= _MULTI_ETF_THRESHOLD else ""
    return float(total), f"etf_lead({rank_tag}{etf_info}:+{total})"


def load_etf_leader_map(max_age_days: int = 35) -> Dict[str, dict]:
    """Supabase scalper_etf_leader_picks에서 최신 base_date TOP 30 조회.

    Args:
        max_age_days: 데이터 신선도 한계 (기본 35일 — 월 1회 갱신 가정)

    Returns:
        {
            "042700": {
                "rank": 1, "leader_score": 100.67, "etf_count": 4,
                "top_etf_name": "TIGER AI반도체핵심공정",
                "bonus": 13.0, "tag": "etf_lead(TOP10/4ETF:+13)",
                "cross_inc": True, "base_date": "2026-05-15"
            },
            ...
        }
        조회 실패 또는 데이터 오래됨 → 빈 dict (호출자가 safe하게 무시)
    """
    try:
        from utils.supabase_sql import query, query_one
    except ImportError as e:
        logger.warning(f"supabase_sql import 실패 — ETF 주도주 보너스 비활성: {e}")
        return {}

    try:
        # 1) 최신 base_date 조회
        row = query_one(
            "SELECT MAX(base_date) AS d FROM scalper_etf_leader_picks"
        )
        if not row or not row.get("d"):
            logger.info("[etf_leader] DB에 데이터 없음 — 보너스 비활성")
            return {}
        latest = row["d"]

        # 2) 신선도 검증
        from datetime import date, timedelta
        cutoff = date.today() - timedelta(days=max_age_days)
        if latest < cutoff:
            logger.warning(
                f"[etf_leader] 최신 데이터 {latest} 가 {max_age_days}일 초과 — 보너스 비활성. "
                f"`tools/refresh_etf_leaders.py` 재실행 필요"
            )
            return {}

        # 3) TOP 30 일괄 로드
        rows = query(
            """
            SELECT rank, stock_ticker, stock_name, leader_score, etf_count, top_etf_name, base_date
            FROM scalper_etf_leader_picks
            WHERE base_date = %s
            ORDER BY rank
            """,
            (latest,),
        )

        result: Dict[str, dict] = {}
        for r in rows:
            rank = r["rank"]
            etf_count = r["etf_count"]
            bonus, tag = _calc_bonus(rank, etf_count)
            if bonus == 0.0:
                continue
            result[r["stock_ticker"]] = {
                "rank": rank,
                "leader_score": float(r["leader_score"]),
                "etf_count": etf_count,
                "top_etf_name": r["top_etf_name"],
                "bonus": bonus,
                "tag": tag,
                "cross_inc": rank <= _CROSS_RANK_THRESHOLD,
                "base_date": str(latest),
            }
        logger.info(
            f"[etf_leader] 보너스 맵 로드: {len(result)}종목 (base_date={latest}, "
            f"TOP10={sum(1 for v in result.values() if v['rank']<=10)})"
        )
        return result

    except Exception as e:
        logger.warning(f"[etf_leader] 로드 실패 (무시): {type(e).__name__}: {e}")
        return {}


def get_etf_leader_bonus(ticker: str, leader_map: Optional[Dict] = None) -> Optional[dict]:
    """단일 종목 보너스 조회 (테스트용 / 직접 호출용).

    Args:
        ticker: 종목코드
        leader_map: 미리 로드한 맵 (None이면 새로 로드)

    Returns:
        보너스 dict 또는 None
    """
    if leader_map is None:
        leader_map = load_etf_leader_map()
    return leader_map.get(ticker)


if __name__ == "__main__":
    import sys, io
    from pathlib import Path as _Path
    # 직접 실행 시 sys.path에 scalper-agent 추가 (utils.supabase_sql 해석)
    sys.path.insert(0, str(_Path(__file__).resolve().parent.parent))
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

    print(f"\n=== ETF 주도주 보너스 맵 테스트 ===\n")
    m = load_etf_leader_map()
    print(f"\n로드된 종목 수: {len(m)}")

    # TOP 5 미리보기
    if m:
        sorted_picks = sorted(m.values(), key=lambda x: x["rank"])[:5]
        print(f"\n[TOP 5 보너스 적용 결과]")
        for p in sorted_picks:
            print(
                f"  {p['rank']:>2}위 [{[k for k,v in m.items() if v==p][0]}] "
                f"leader_score={p['leader_score']:.2f} | "
                f"etf_count={p['etf_count']} | {p['tag']} | "
                f"cross_inc={p['cross_inc']}"
            )

    # 단위 검증
    print(f"\n[단위 검증]")
    test_cases = [
        ("042700", "한미반도체 (TOP 1, 4 ETF)"),
        ("068270", "셀트리온 (TOP 2, 5 ETF)"),
        ("353200", "대덕전자 (TOP 30, 3 ETF)"),
        ("999999", "존재 X (None 반환 예상)"),
    ]
    for code, desc in test_cases:
        b = get_etf_leader_bonus(code, m)
        if b:
            print(f"  ✓ {desc}: bonus={b['bonus']:+.0f}점 / {b['tag']}")
        else:
            print(f"  · {desc}: None")
