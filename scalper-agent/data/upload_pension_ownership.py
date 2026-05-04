# -*- coding: utf-8 -*-
"""연기금 지분 추적기 → Supabase 업로드.

FLOWX "연기금 지분 추적기" 패널에 표시.
테이블: intelligence_pension_ownership (date PK, upsert)

COO G7 Stage4 C42에서 호출:
  from data.pension_ownership_tracker import track_pension_ownership
  from data.upload_pension_ownership import upload_pension_ownership
  result = track_pension_ownership()
  upload_pension_ownership(result)
"""
import logging
from datetime import datetime

logger = logging.getLogger("BH.PensionOwnershipUpload")


def upload_pension_ownership(data: dict) -> bool:
    """연기금 지분 추적 결과를 Supabase에 업로드.

    Args:
        data: track_pension_ownership() 반환값
    Returns:
        bool: 성공 여부
    """
    if not data or not data.get("stocks"):
        logger.warning("빈 데이터 — 업로드 스킵")
        return False

    try:
        from data.upload_swing import _get_client
        client = _get_client()
        if not client:
            logger.error("Supabase 클라이언트 없음")
            return False

        summary = data.get("summary", {})

        row = {
            "date": data["date"],
            "updated_at": datetime.now().isoformat(),
            "total_tracked": data.get("total_tracked", 0),
            "accumulating_count": summary.get("accumulating_count", 0),
            "distributing_count": summary.get("distributing_count", 0),
            "avg_correlation": summary.get("avg_correlation", 0),
            "strong_positive_corr": summary.get("strong_positive_corr", 0),
            "strong_negative_corr": summary.get("strong_negative_corr", 0),
            "top_accumulating": summary.get("top_accumulating", []),
            "top_distributing": summary.get("top_distributing", []),
            "stocks": data.get("stocks", []),
        }

        client.table("intelligence_pension_ownership") \
            .upsert(row, on_conflict="date") \
            .execute()

        acc = summary.get("accumulating_count", 0)
        dist = summary.get("distributing_count", 0)
        corr = summary.get("avg_correlation", 0)
        top = summary.get("top_accumulating", [{}])
        top1 = top[0] if top else {}

        logger.info(
            f"연기금보유추적 업로드: {data['date']} · "
            f"{data.get('total_tracked', 0)}종목 · "
            f"매집 {acc}/매도 {dist} · "
            f"상관 {corr:.3f} · "
            f"TOP={top1.get('name', '?')}({top1.get('net_10d', 0):+.0f}억)"
        )
        return True

    except Exception as e:
        err = str(e)
        if "does not exist" in err:
            logger.warning(
                "intelligence_pension_ownership 테이블 없음 — "
                "Supabase에서 테이블 생성 필요"
            )
        else:
            logger.error(f"연기금보유추적 업로드 실패: {e}")
        return False
