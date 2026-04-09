# -*- coding: utf-8 -*-
"""
단타 TOP픽 Supabase 업로드
==========================
daytrading_picks.json (Hybrid 2단 발행: preview/confirmed) 을
Supabase intelligence_daytrading_picks 테이블에 저장.

FLOWX 웹 대시보드 → "오늘의 단타 TOP픽" 패널에 표시됨.

모드 구분:
- preview  : 전일 저녁(16:45) 국장 마감 기준 — NXT 야간매수용
- confirmed: 당일 아침(07:35) 미국장 + EWY 반영 — 09:00 진입용

테이블:
    (date, mode) 복합 PK — 같은 날 preview/confirmed 둘 다 저장 가능
"""

import logging
from datetime import date, datetime

logger = logging.getLogger("BH.DaytradingPicks")


def upload_daytrading_picks(out: dict) -> bool:
    """daytrading_picks.json dict을 Supabase에 업로드.

    Args:
        out: {
          "updated": iso, "mode": "preview"|"confirmed",
          "ewy_signal": {...}, "picks": [...]
        }
    """
    if not out or not isinstance(out, dict):
        logger.warning("빈 out — 업로드 스킵")
        return False

    picks = out.get("picks") or []
    mode = out.get("mode", "confirmed")

    try:
        from data.upload_swing import _get_client
        client = _get_client()
        if not client:
            logger.error("Supabase 클라이언트 없음")
            return False

        # 당일 날짜 (오늘 장 기준)
        today = date.today().isoformat()

        ewy = out.get("ewy_signal") or {}

        # 개별 pick을 직렬화 가능한 형태로 정리
        picks_json = []
        for p in picks:
            picks_json.append({
                "rank": picks.index(p) + 1,
                "code": p.get("code"),
                "name": p.get("name"),
                "sector": p.get("sector"),
                "track": p.get("track"),
                "mcap_억": p.get("mcap_억"),
                "close": p.get("close_end"),
                "entry_low": p.get("entry_low"),
                "entry_high": p.get("entry_high"),
                "tp1": p.get("tp1"),
                "tp2": p.get("tp2"),
                "sl": p.get("sl"),
                "upside_to_tp1_pct": p.get("upside_to_tp1_pct"),
                "final_score": p.get("final_score"),
                "score": p.get("score"),
                "sector_bonus": p.get("sector_bonus"),
                "mcap_bonus": p.get("mcap_bonus"),
                "ewy_bonus": p.get("ewy_bonus"),
                "key_reasons": p.get("key_reasons"),
                "foreign_total_억": p.get("foreign_total_억"),
                "inst_total_억": p.get("inst_total_억"),
                "dual_total_억": p.get("dual_total_억"),
                "buy_days": p.get("buy_days"),
                "inst_joining": p.get("inst_joining"),
                "price_change_%": p.get("price_change_%"),
                "etf_alt_code": p.get("etf_alt_code"),
                "etf_alt_name": p.get("etf_alt_name"),
                "etf_alt_theme": p.get("etf_alt_theme"),
            })

        row = {
            "date": today,
            "mode": mode,
            "updated_at": out.get("updated") or datetime.now().isoformat(),
            "picks_count": len(picks),
            "track_a_count": sum(1 for p in picks if p.get("track", "").startswith("A_")),
            "track_b_count": sum(1 for p in picks if p.get("track", "").startswith("B_")),
            "ewy_1d": ewy.get("ewy_1d"),
            "ewy_5d": ewy.get("ewy_5d"),
            "ks200_1d": ewy.get("ks200_1d"),
            "ks200_5d": ewy.get("ks200_5d"),
            "ewy_source": ewy.get("source"),
            "picks": picks_json,  # JSONB
        }

        client.table("intelligence_daytrading_picks") \
            .upsert(row, on_conflict="date,mode") \
            .execute()

        logger.info(
            f"✅ 단타 픽 업로드 완료: {today} · mode={mode} · "
            f"{len(picks)}종목 (A {row['track_a_count']} + B {row['track_b_count']})"
        )
        return True

    except Exception as e:
        err = str(e)
        if "does not exist" in err:
            logger.warning(
                "intelligence_daytrading_picks 테이블 없음 — "
                "sql/intelligence_daytrading_picks_migration.sql 실행 필요"
            )
        else:
            logger.error(f"단타 픽 업로드 실패: {e}")
        return False
