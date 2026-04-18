# -*- coding: utf-8 -*-
"""수급 강도 TOP → Supabase 업로드.

FLOWX 스윙시스템 "수급 인텔리전스" 패널에 표시.
테이블: intelligence_flow_intensity (date PK, upsert)

COO C35 이후 호출:
  from data.upload_flow_intensity import upload_flow_intensity
  upload_flow_intensity(data)
"""
import logging
from datetime import datetime

logger = logging.getLogger("BH.FlowIntensity")


def upload_flow_intensity(data: dict) -> bool:
    """수급 강도 데이터를 Supabase에 업로드.

    Args:
        data: generate_flow_intensity_data() 반환값
              {"date", "total_scanned", "top_stocks", "dual_buy_count", "overheat_count"}
    """
    if not data or not data.get("top_stocks"):
        logger.warning("빈 데이터 — 업로드 스킵")
        return False

    try:
        from data.upload_swing import _get_client
        client = _get_client()
        if not client:
            logger.error("Supabase 클라이언트 없음")
            return False

        row = {
            "date": data["date"],
            "updated_at": datetime.now().isoformat(),
            "total_scanned": data.get("total_scanned", 0),
            "top_stocks": data["top_stocks"],
            "dual_buy_count": data.get("dual_buy_count", 0),
            "overheat_count": data.get("overheat_count", 0),
        }

        client.table("intelligence_flow_intensity") \
            .upsert(row, on_conflict="date") \
            .execute()

        top1 = data["top_stocks"][0] if data["top_stocks"] else {}
        logger.info(
            f"수급강도 업로드 완료: {data['date']} · "
            f"TOP1={top1.get('name', '?')}({top1.get('intensity_pct', 0)}%) · "
            f"{len(data['top_stocks'])}종목 "
            f"(쌍매수{data.get('dual_buy_count', 0)} 과열{data.get('overheat_count', 0)})"
        )
        return True

    except Exception as e:
        err = str(e)
        if "does not exist" in err:
            logger.warning(
                "intelligence_flow_intensity 테이블 없음 — "
                "sql/intelligence_flow_intensity_migration.sql 실행 필요"
            )
        else:
            logger.error(f"수급강도 업로드 실패: {e}")
        return False
