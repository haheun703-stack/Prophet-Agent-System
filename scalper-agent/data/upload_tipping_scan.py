# -*- coding: utf-8 -*-
"""수급 임계점 스캔 결과 → Supabase 업로드.

테이블: intelligence_tipping_scan
스케줄: G7 Stage 4, C42 (stealth_scan 직후)
"""

import json
import logging
from datetime import datetime
from pathlib import Path

logger = logging.getLogger("BH.UploadTipping")

STORE_DIR = Path(__file__).resolve().parent.parent / "data_store"


def upload_tipping_scan(scan_data: dict = None) -> bool:
    """수급 임계점 스캔 결과를 Supabase에 업로드.

    Args:
        scan_data: scan_tipping_point() 반환값. None이면 JSON 파일에서 로드.

    Returns:
        True = 성공, False = 실패 (전일 데이터 유지)
    """
    from data.upload_swing import _get_client

    # 데이터 로드
    if not scan_data:
        scan_path = STORE_DIR / "tipping_scan.json"
        if not scan_path.exists():
            logger.warning("tipping_scan.json 없음 — 업로드 스킵")
            return False
        try:
            scan_data = json.loads(scan_path.read_text("utf-8"))
        except Exception as e:
            logger.error(f"tipping_scan.json 파싱 실패: {e}")
            return False

    if not scan_data or scan_data.get("error"):
        logger.warning("임계점 데이터 비어있음 — 업로드 스킵")
        return False

    client = _get_client()
    if not client:
        return False

    today = datetime.now().strftime("%Y-%m-%d")
    coiled = scan_data.get("coiled", [])
    warming = scan_data.get("warming", [])
    launched = scan_data.get("launched", [])

    row = {
        "date": today,
        "total_scanned": scan_data.get("total_scanned", 0),
        "coiled_count": len(coiled),
        "warming_count": len(warming),
        "launched_count": len(launched),
        "coiled": coiled[:30],
        "warming": warming[:15],
        "launched": launched[:10],
    }

    try:
        client.table("intelligence_tipping_scan") \
            .upsert(row, on_conflict="date") \
            .execute()
        logger.info(
            f"[임계점] 업로드 완료: {today} — "
            f"코일 {len(coiled)} / 점화 {len(warming)} / 이륙 {len(launched)}"
        )
        return True
    except Exception as e:
        err = str(e)
        if "does not exist" in err or "Could not find" in err:
            logger.warning(
                "intelligence_tipping_scan 테이블 없음 — "
                "sql/intelligence_tipping_scan_migration.sql 실행 필요"
            )
        else:
            logger.error(f"[임계점] 업로드 실패 (전일 유지): {e}")
        return False
