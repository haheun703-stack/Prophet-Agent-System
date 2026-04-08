# -*- coding: utf-8 -*-
"""
미국장 분석 결과 Supabase 업로드
================================
us_overnight_filter 결과를 daytrading_us_overnight 테이블에 저장.
FLOWX 대시보드 → "오늘의 모드" 패널에 표시됨.
"""

import logging
from datetime import date

logger = logging.getLogger("BH.USUpload")


def upload_us_overnight(report: dict) -> bool:
    """분석 결과를 Supabase daytrading_us_overnight에 업로드."""
    if not report:
        logger.error("빈 리포트 — 업로드 스킵")
        return False

    try:
        from data.upload_swing import _get_client
        client = _get_client()
        if not client:
            logger.error("Supabase 클라이언트 없음")
            return False

        row = {
            "date":          report.get("date", date.today().isoformat()),
            "mode":          report.get("mode"),
            "gap_signal":    report.get("gap_signal"),
            "gap_est_pct":   report.get("gap_est_pct"),
            "soxx_alert":    report.get("soxx_alert"),
            "risk_level":    report.get("risk_level"),
            "risk_score":    report.get("risk_score"),
            "watch_sectors": report.get("watch_sectors") or [],
            "avoid_sectors": report.get("avoid_sectors") or [],
            "relay_picks":   report.get("relay_picks")   or [],
            "reasons_bad":   report.get("reasons_bad")   or [],
            "reasons_good":  report.get("reasons_good")  or [],
            "reason":        report.get("reason"),
            "nasdaq_change": report.get("nasdaq_change"),
            "soxx_change":   report.get("soxx_change"),
            "vix":           report.get("vix"),
            "dxy":           report.get("dxy"),
            "us_3y_yield":   report.get("us_3y_yield"),
            "fear_greed":    report.get("fear_greed"),
            "fear_greed_label": report.get("fear_greed_label"),
            "kr_impact":     report.get("kr_impact"),
            "risk_flags":    report.get("risk_flags") or [],
        }

        client.table("daytrading_us_overnight") \
            .upsert(row, on_conflict="date") \
            .execute()

        logger.info(f"✅ US 업로드 완료: {row['date']} | 모드: {row['mode']}")
        return True

    except Exception as e:
        err_str = str(e)
        if "does not exist" in err_str:
            logger.warning(
                "daytrading_us_overnight 테이블 없음 — "
                "Supabase에서 CREATE TABLE 먼저 실행하세요"
            )
        else:
            logger.error(f"US 업로드 실패: {e}")
        return False
