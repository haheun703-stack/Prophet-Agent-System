# -*- coding: utf-8 -*-
"""
NXT 야간매수 TOP 5 Supabase 업로드
===================================
intelligence_nxt_picks: NXT TOP 5 추천 저장 (C32)
intelligence_nxt_performance: NXT 성적표 저장 (C33)
"""

import logging

logger = logging.getLogger("BH.NxtPerf")


def upload_nxt_picks(picks_data: dict) -> bool:
    """NXT TOP 5 추천을 Supabase에 저장.

    Args:
        picks_data: extract_nxt_top5() 반환값
    """
    if not picks_data:
        return False

    try:
        from data.upload_swing import _get_client
        client = _get_client()
        if not client:
            return False

        picks = picks_data.get("picks", [])
        picks_json = []
        for p in picks:
            picks_json.append({
                "rank": p.get("rank"),
                "code": p.get("code"),
                "name": p.get("name"),
                "sector": p.get("sector"),
                "supply_score": p.get("supply_score"),
                "entry_price": p.get("entry_price"),
                "foreign_flow_warning": p.get("foreign_flow_warning", ""),
            })

        row = {
            "date": picks_data["date"],
            "nxt_score": picks_data.get("nxt_score", 0),
            "signal": picks_data.get("nxt_signal", ""),
            "sectors": picks_data.get("recommended_sectors", []),
            "picks": picks_json,
        }

        client.table("intelligence_nxt_picks") \
            .upsert(row, on_conflict="date") \
            .execute()

        logger.info(f"NXT 픽 업로드 완료: {picks_data['date']} · {len(picks)}종목")
        return True

    except Exception as e:
        logger.error(f"NXT 픽 업로드 실패: {e}")
        return False


def upload_accumulation_radar(radar_data: dict) -> bool:
    """매집 레이더 데이터를 Supabase에 저장.

    NXT TOP5 바로 아래, 매매 타임라인 위에 표시.
    외인 3일+ 매집 중이나 아직 안 오른 "미발화" 종목.

    Args:
        radar_data: {
            "date": "2026-04-20",
            "stocks": [{"code","name","frgn_days","accel_b","chg5","tag","last_dual"}, ...]
        }
    """
    if not radar_data or not radar_data.get("stocks"):
        return False

    try:
        from data.upload_swing import _get_client
        client = _get_client()
        if not client:
            return False

        stocks_json = []
        for s in radar_data["stocks"]:
            stocks_json.append({
                "code": s.get("code", ""),
                "name": s.get("name", ""),
                "frgn_days": s.get("frgn_days", 0),
                "accel_b": round(s.get("accel_b", 0), 1),
                "chg5": round(s.get("chg5", 0), 1),
                "tag": s.get("tag", ""),
                "last_dual": s.get("last_dual", False),
                "supply_score": s.get("supply_score", 0),
                "combined_supply": round(s.get("combined_supply", 0), 1),
            })

        row = {
            "date": radar_data["date"],
            "stocks": stocks_json,
        }

        client.table("intelligence_accumulation_radar") \
            .upsert(row, on_conflict="date") \
            .execute()

        logger.info(f"매집 레이더 업로드 완료: {radar_data['date']} · {len(stocks_json)}종목")
        return True

    except Exception as e:
        logger.error(f"매집 레이더 업로드 실패: {e}")
        return False


def upload_nxt_performance(report: dict) -> bool:
    """NXT 성적표를 Supabase에 저장.

    Args:
        report: build_nxt_performance_report() 반환값
    """
    if not report:
        return False

    try:
        from data.upload_swing import _get_client
        client = _get_client()
        if not client:
            return False

        cum = report.get("cumulative", {})
        items = report.get("items", [])

        items_json = []
        for it in items:
            items_json.append({
                "rank": it.get("rank"),
                "code": it.get("code"),
                "name": it.get("name"),
                "sector": it.get("sector"),
                "supply_score": it.get("supply_score"),
                "entry_price": it.get("entry_price"),
                "close_price": it.get("close_price"),
                "return_pct": it.get("return_pct"),
            })

        row = {
            "pick_date": report["pick_date"],
            "result_date": report["result_date"],
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

        client.table("intelligence_nxt_performance") \
            .upsert(row, on_conflict="pick_date") \
            .execute()

        logger.info(
            f"NXT 성적표 업로드 완료: {report['pick_date']} → "
            f"{report['result_date']} · 평균 {report.get('avg_return', 0):+.2f}%"
        )
        return True

    except Exception as e:
        logger.error(f"NXT 성적표 업로드 실패: {e}")
        return False
