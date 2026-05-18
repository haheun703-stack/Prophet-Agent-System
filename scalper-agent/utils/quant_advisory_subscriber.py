# -*- coding: utf-8 -*-
"""퀀트봇 형 advisory 구독 클라이언트 (5/18 형 v2 가이드 반영)

형 → 동생: quant_bot_advisory 테이블 SELECT — msg_type 4종:
- MORNING_BRIEFING (매일 07:00) — 게이트 ① 매크로 최우선
- ADVICE (11:00 / 13:30 / 15:00) — 게이트 ① 시간대 보정
- SNAPSHOT (09:30~15:50 10분 간격) — 게이트 ④ 인버스 매크로
- LEADING (형 수동 긴급) — 게이트 ③/⑤ 즉시 차단

동생 측 사용 패턴:
  from utils.quant_advisory_subscriber import fetch_latest_advisory, fetch_today_advisories

  # 검증 모드 v3 매수 직전에 호출 (08:55)
  adv = fetch_latest_advisory()
  if adv and adv['market_regime'] in ('CAUTION', 'BEARISH'):
      ... # 진입 자제 또는 STRONG 등급만 매수

운영:
  python utils/quant_advisory_subscriber.py        # 오늘 모든 advisory 조회
  python utils/quant_advisory_subscriber.py --id 1 # 특정 id 조회 (검증용)
"""
import argparse
import sys
from pathlib import Path
from typing import Dict, List, Optional

# 프로젝트 루트
_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))

from dotenv import load_dotenv
load_dotenv(_ROOT.parent / ".env")

from utils.supabase_sql import query  # noqa: E402


def fetch_latest_advisory(
    target_bot: str = "scalper",
    msg_type: Optional[str] = None,
) -> Optional[Dict]:
    """오늘 가장 최근 advisory 조회 (단타봇 매수 직전 호출 권장).

    Args:
        target_bot: 'scalper' (단타봇용)
        msg_type: 'MORNING_BRIEFING' | 'ADVICE' | 'SNAPSHOT' | 'LEADING' | None (전체)

    Examples:
        # 게이트 ① 매크로 (09:00 진입 전)
        mb = fetch_latest_advisory(msg_type='MORNING_BRIEFING')

        # 게이트 ④ 인버스 매크로 (장중 갱신)
        snap = fetch_latest_advisory(msg_type='SNAPSHOT')

        # 형 긴급 LEADING 체크
        urgent = fetch_latest_advisory(msg_type='LEADING')
    """
    sql = """
        SELECT * FROM quant_bot_advisory
        WHERE advisory_date = CURRENT_DATE AND target_bot = %s
    """
    params: tuple = (target_bot,)
    if msg_type:
        sql += " AND msg_type = %s"
        params = (target_bot, msg_type)
    sql += " ORDER BY advisory_time DESC LIMIT 1"

    rows = query(sql, params)
    if not rows:
        return None
    return _row_to_dict(rows[0], include_all=True)


def fetch_today_advisories(
    target_bot: str = "scalper",
    msg_type: Optional[str] = None,
) -> List[Dict]:
    """오늘 모든 advisory 조회 (저녁 종합 분석용)."""
    sql = """
        SELECT * FROM quant_bot_advisory
        WHERE advisory_date = CURRENT_DATE AND target_bot = %s
    """
    params: tuple = (target_bot,)
    if msg_type:
        sql += " AND msg_type = %s"
        params = (target_bot, msg_type)
    sql += " ORDER BY advisory_time ASC"

    rows = query(sql, params)
    return [_row_to_dict(r, include_all=True) for r in rows]


# 형 v2 가이드 코드 호환 별칭 (today_all)
today_all = fetch_today_advisories


def fetch_advisory_by_id(adv_id: int) -> Optional[Dict]:
    """특정 id 조회 (검증/디버그용)."""
    rows = query(
        """
        SELECT id, advisory_date, advisory_time, msg_type, market_regime,
               market_strength_avg, inverse_etf_strength,
               title, body, related_tickers, alert_codes, reasoning, target_bot
        FROM quant_bot_advisory
        WHERE id = %s
        """,
        (adv_id,),
    )
    if not rows:
        return None
    return _row_to_dict(rows[0], include_target=True)


def _row_to_dict(row, include_target: bool = False, include_all: bool = False) -> Dict:
    """RealDictCursor row(dict) → 표준화 dict 변환.

    include_all=True: 형 v2 가이드의 모든 필드 노출 (risk_level / severity / 등)
    """
    base = {
        "id": row["id"],
        "advisory_date": str(row["advisory_date"]),
        "advisory_time": str(row["advisory_time"]),
        "msg_type": row["msg_type"],
        "market_regime": row.get("market_regime"),
        "market_strength_avg": float(row["market_strength_avg"]) if row.get("market_strength_avg") is not None else None,
        "inverse_etf_strength": float(row["inverse_etf_strength"]) if row.get("inverse_etf_strength") is not None else None,
        "title": row.get("title"),
        "body": row.get("body"),
        "related_tickers": row.get("related_tickers") or [],
        "alert_codes": row.get("alert_codes") or [],
        "reasoning": row.get("reasoning"),
    }
    if include_target or include_all:
        base["target_bot"] = row.get("target_bot")
    if include_all:
        # 형 v2 가이드의 추가 필드들 (전체 dict로 반환)
        for k, v in row.items():
            if k not in base:
                base[k] = v
    return base


def print_advisory(adv: Dict) -> None:
    """advisory 사람이 읽기 좋게 출력."""
    print(f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    print(f"🤖 퀀트봇 형 Advisory #{adv['id']}")
    print(f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    print(f"📅 {adv['advisory_date']}  ⏰ {adv['advisory_time']}")
    print(f"📋 Type: {adv['msg_type']}  Regime: {adv['market_regime']}")
    if adv['market_strength_avg'] is not None:
        print(f"📊 시장 강도: {adv['market_strength_avg']:.2f}  인버스 강도: {adv['inverse_etf_strength']:.2f}")
    print()
    print(f"📌 {adv['title']}")
    print()
    if adv['body']:
        print(f"{adv['body']}")
        print()
    if adv['related_tickers']:
        print(f"🎯 종목: {', '.join(adv['related_tickers'])}")
    if adv['alert_codes']:
        print(f"🚨 Alert: {', '.join(adv['alert_codes'])}")
    if adv['reasoning']:
        print(f"💡 Reasoning: {adv['reasoning']}")
    print(f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="퀀트봇 형 advisory 구독 클라이언트")
    parser.add_argument("--id", type=int, help="특정 id 조회")
    parser.add_argument("--all", action="store_true", help="오늘 모든 advisory")
    args = parser.parse_args()

    if args.id:
        adv = fetch_advisory_by_id(args.id)
        if adv:
            print_advisory(adv)
        else:
            print(f"advisory id={args.id} 없음")
    elif args.all:
        advs = fetch_today_advisories()
        print(f"=== 오늘 advisory {len(advs)}건 ===\n")
        for adv in advs:
            print_advisory(adv)
            print()
    else:
        # 기본: 최신 1건
        adv = fetch_latest_advisory()
        if adv:
            print_advisory(adv)
        else:
            print("오늘 advisory 없음")
