# -*- coding: utf-8 -*-
"""
단타 TOP 5 성적표 Supabase 업로드
=================================
intelligence_daytrading_performance 테이블에 일일 성적 저장.
FLOWX 웹 대시보드 → "TOP 5 성적표" 패널에 표시됨.
"""

import logging
from datetime import datetime

logger = logging.getLogger("BH.DaytradingPerf")


def upload_daytrading_performance(report: dict) -> bool:
    """성적표 report dict를 Supabase에 업로드.

    Args:
        report: build_performance_report()의 반환값
            {"date", "items", "avg_return", "best_pick", "worst_pick",
             "cumulative": {"weekly_return", ...}}
    """
    if not report or not isinstance(report, dict):
        logger.warning("빈 report — 업로드 스킵")
        return False

    try:
        from data.upload_swing import _get_client
        client = _get_client()
        if not client:
            logger.error("Supabase 클라이언트 없음")
            return False

        cum = report.get("cumulative", {})
        items = report.get("items", [])

        # items를 직렬화 가능한 형태로 정리
        items_json = []
        for it in items:
            items_json.append({
                "rank": it.get("rank"),
                "code": it.get("code"),
                "name": it.get("name"),
                "sector": it.get("sector"),
                "score": it.get("score"),
                "open_price": it.get("open_price"),
                "close_price": it.get("close_price"),
                "high_price": it.get("high_price"),
                "low_price": it.get("low_price"),
                "return_pct": it.get("return_pct"),
                "volume": it.get("volume"),
            })

        row = {
            "date": report["date"],
            "avg_return": report.get("avg_return", 0),
            "best_pick": report.get("best_pick", ""),
            "worst_pick": report.get("worst_pick", ""),
            "weekly_return": cum.get("weekly_return", 0),
            "weekly_days": cum.get("weekly_days", 0),
            "weekly_wins": cum.get("weekly_wins", 0),
            "monthly_return": cum.get("monthly_return", 0),
            "monthly_days": cum.get("monthly_days", 0),
            "monthly_wins": cum.get("monthly_wins", 0),
            "items": items_json,
        }

        client.table("intelligence_daytrading_performance") \
            .upsert(row, on_conflict="date") \
            .execute()

        logger.info(
            f"성적표 업로드 완료: {report['date']} · "
            f"평균 {report.get('avg_return', 0):+.2f}% · "
            f"{len(items)}종목"
        )
        return True

    except Exception as e:
        err = str(e)
        if "does not exist" in err:
            logger.warning(
                "intelligence_daytrading_performance 테이블 없음 — "
                "sql/intelligence_daytrading_performance_migration.sql 실행 필요"
            )
        else:
            logger.error(f"성적표 업로드 실패: {e}")
        return False
