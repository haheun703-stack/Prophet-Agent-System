# -*- coding: utf-8 -*-
"""매집 합류 시그널 → Supabase 업로드.

FLOWX "매집 합류 시그널" 패널에 표시.
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

        # 등급별 카운트 (프론트엔드 요약 표시용)
        all_stocks = data.get("best_stocks", []) + data.get("standby_stocks", [])
        grade_counts = {"S": 0, "A": 0, "B": 0, "C": 0}
        for st in all_stocks:
            g = st.get("pension_grade", "C")
            grade_counts[g] = grade_counts.get(g, 0) + 1

        row = {
            "date": data["date"],
            "updated_at": datetime.now().isoformat(),
            "total_count": data.get("total_count", 0),
            "best_count": data.get("best_count", 0),
            "best_fresh_count": data.get("best_fresh_count", 0),
            "standby_count": data.get("standby_count", 0),
            "grade_s": grade_counts["S"],
            "grade_a": grade_counts["A"],
            "grade_b": grade_counts["B"],
            "best_stocks": data.get("best_stocks", []),
            "best_fresh": data.get("best_fresh", []),
            "standby_stocks": data.get("standby_stocks", []),
            "ranked_stocks": data.get("ranked_stocks", []),
        }

        try:
            client.table("intelligence_pension_scan") \
                .upsert(row, on_conflict="date") \
                .execute()
        except Exception as upsert_err:
            # grade_s/a/b 컬럼 미존재 시 컬럼 제거 후 재시도
            if "grade_" in str(upsert_err):
                logger.warning("grade 컬럼 미존재 — 등급 필드 제외 후 재시도")
                row.pop("grade_s", None)
                row.pop("grade_a", None)
                row.pop("grade_b", None)
                client.table("intelligence_pension_scan") \
                    .upsert(row, on_conflict="date") \
                    .execute()
            else:
                raise

        ranked = data.get("ranked_stocks", [])
        best = data.get("best_stocks", [])
        top1 = ranked[0] if ranked else (best[0] if best else {})
        logger.info(
            f"연기금스캔 업로드 완료: {data.get('date', '?')} · "
            f"TOP1={top1.get('name', '?')}({top1.get('pension_score', 0)}점) · "
            f"등급 S={grade_counts['S']} A={grade_counts['A']} B={grade_counts['B']} · "
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
