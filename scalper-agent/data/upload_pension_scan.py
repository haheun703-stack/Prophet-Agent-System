# -*- coding: utf-8 -*-
"""연기금+금투 합류 스캔 → Supabase 업로드.

FLOWX "연기금 매집 타이밍" 패널에 표시.
테이블: intelligence_pension_scan (date PK, upsert)

COO G7 Stage4 C41에서 호출:
  from data.pension_finance_scan import scan_pension_finance
  from data.upload_pension_scan import upload_pension_scan
  result = scan_pension_finance()
  upload_pension_scan(result)
"""
import logging
from datetime import datetime

logger = logging.getLogger("BH.PensionUpload")


def upload_pension_scan(data: dict) -> bool:
    """연기금+금투 스캔 결과를 Supabase에 업로드.

    Args:
        data: scan_pension_finance() 반환값
    Returns:
        bool: 성공 여부
    """
    if not data or (not data.get("best_stocks") and not data.get("standby_stocks")):
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
            "best_count": data.get("best_count", 0),
            "best_fresh_count": data.get("best_fresh_count", 0),
            "standby_count": data.get("standby_count", 0),
            "best_stocks": data.get("best_stocks", []),
            "best_fresh": data.get("best_fresh", []),
            "standby_stocks": data.get("standby_stocks", []),
        }

        client.table("intelligence_pension_scan") \
            .upsert(row, on_conflict="date") \
            .execute()

        top1 = data["best_stocks"][0] if data["best_stocks"] else {}
        logger.info(
            f"연기금스캔 업로드 완료: {data['date']} · "
            f"TOP1={top1.get('name', '?')}({top1.get('pension_consec', 0)}d) · "
            f"핵심 {data.get('best_count', 0)}종목 / "
            f"대기 {data.get('standby_count', 0)}종목"
        )
        return True

    except Exception as e:
        err = str(e)
        if "does not exist" in err:
            logger.warning(
                "테이블 intelligence_pension_scan 없음 — "
                "Supabase에서 테이블 생성 필요"
            )
        else:
            logger.error(f"연기금스캔 업로드 실패: {e}")
        return False
