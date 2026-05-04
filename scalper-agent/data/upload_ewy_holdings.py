# -*- coding: utf-8 -*-
"""EWY 보유종목 변동 → Supabase 업로드.

FLOWX "EWY 펀드 보유종목 변동" 패널에 표시.
테이블: intelligence_ewy_holdings (date PK, upsert)

COO G1 A11B:
  from data.ewy_holdings_collector import collect_ewy_holdings
  from data.upload_ewy_holdings import upload_ewy_holdings
  result = collect_ewy_holdings()
  upload_ewy_holdings(result)
"""
import logging
from datetime import datetime

logger = logging.getLogger("BH.EWYUpload")


def upload_ewy_holdings(data: dict) -> bool:
    """EWY 보유종목 결과를 Supabase에 업로드.

    Args:
        data: collect_ewy_holdings() 반환값
    Returns:
        bool: 성공 여부
    """
    if not data or not data.get("top20"):
        logger.warning("[EWY Upload] 빈 데이터 — 업로드 스킵")
        return False

    try:
        from data.upload_swing import _get_client

        client = _get_client()
        if not client:
            logger.error("[EWY Upload] Supabase 클라이언트 없음")
            return False

        row = {
            "date": data["date"],
            "updated_at": datetime.now().isoformat(),
            "as_of": data.get("as_of", ""),
            "total_stocks": data.get("total_stocks", 0),
            "top20": data.get("top20", []),
            "changes": data.get("changes", []),
            "new_entries": data.get("new_entries", []),
            "removed": data.get("removed", []),
            "summary": data.get("summary", ""),
        }

        client.table("intelligence_ewy_holdings") \
            .upsert(row, on_conflict="date") \
            .execute()

        n_changes = len(data.get("changes", []))
        n_new = len(data.get("new_entries", []))
        n_removed = len(data.get("removed", []))
        top1 = data["top20"][0] if data["top20"] else {}

        logger.info(
            f"[EWY Upload] 완료: {data['date']} · "
            f"기준={data.get('as_of', '?')} · "
            f"TOP1={top1.get('name', '?')}({top1.get('weight', 0):.2f}%) · "
            f"{data.get('total_stocks', 0)}종목 · "
            f"변동 {n_changes} / 편입 {n_new} / 편출 {n_removed}"
        )
        return True

    except Exception as e:
        err = str(e)
        if "does not exist" in err:
            logger.warning(
                "[EWY Upload] 테이블 intelligence_ewy_holdings 없음 — "
                "Supabase에서 테이블 생성 필요"
            )
        else:
            logger.error(f"[EWY Upload] 업로드 실패: {e}")
        return False
