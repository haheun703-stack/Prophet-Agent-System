# -*- coding: utf-8 -*-
"""기관 연속매수 초기 감지 → Supabase 업로드.

FLOWX 스윙시스템 "기관 매집 레이더" 패널에 표시.
테이블: intelligence_inst_accumulation (date PK, upsert)

COO G7 이후 호출:
  from data.inst_accumulation_scan import scan_inst_accumulation
  from data.upload_inst_accumulation import upload_inst_accumulation
  data = scan_inst_accumulation()
  upload_inst_accumulation(data)
"""
import logging
from datetime import datetime

logger = logging.getLogger("BH.InstAccUpload")


def upload_inst_accumulation(data: dict) -> bool:
    """기관 연속매수 데이터를 Supabase에 업로드.

    Args:
        data: scan_inst_accumulation() 반환값
              {"date", "total_count", "early_count", "running_count",
               "early_stocks", "running_stocks"}
    Returns:
        bool: 성공 여부
    """
    if not data or (not data.get("early_stocks") and not data.get("running_stocks")):
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
            "total_count": data.get("total_count", 0),
            "early_count": data.get("early_count", 0),
            "running_count": data.get("running_count", 0),
            "early_stocks": data.get("early_stocks", []),
            "running_stocks": data.get("running_stocks", []),
        }

        client.table("intelligence_inst_accumulation") \
            .upsert(row, on_conflict="date") \
            .execute()

        top1 = data["early_stocks"][0] if data["early_stocks"] else {}
        logger.info(
            f"기관매집 업로드 완료: {data['date']} · "
            f"TOP1={top1.get('name', '?')}({top1.get('inst_consec', 0)}일) · "
            f"초기 {data.get('early_count', 0)}종목 / "
            f"진행 {data.get('running_count', 0)}종목"
        )
        return True

    except Exception as e:
        err = str(e)
        if "does not exist" in err:
            logger.warning(
                "테이블 intelligence_inst_accumulation 없음 — "
                "Supabase에서 테이블 생성 필요"
            )
        else:
            logger.error(f"기관매집 업로드 실패: {e}")
        return False
