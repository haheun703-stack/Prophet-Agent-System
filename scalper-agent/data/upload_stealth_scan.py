# -*- coding: utf-8 -*-
"""
선매집 탐지 Supabase 업로드
===========================
intelligence_stealth_scan 테이블에 일일 선매집 탐지 결과 저장.
FLOWX 웹 대시보드 → "선매집 탐지" 패널에 표시됨.

데이터 소스: data_store/stealth_scan.json (C28에서 생성)
"""

import json
import logging
from datetime import datetime
from pathlib import Path

logger = logging.getLogger("BH.StealthUpload")

STORE_DIR = Path(__file__).resolve().parent.parent / "data_store"


def upload_stealth_scan(scan_data: dict = None) -> bool:
    """선매집 탐지 결과를 Supabase에 업로드.

    Args:
        scan_data: scan_stealth_accumulation()의 반환값.
                   None이면 stealth_scan.json에서 로드.
    """
    # 데이터 로드
    if not scan_data:
        scan_path = STORE_DIR / "stealth_scan.json"
        if not scan_path.exists():
            logger.warning("stealth_scan.json 없음 — 업로드 스킵")
            return False
        try:
            scan_data = json.loads(scan_path.read_text("utf-8"))
        except Exception as e:
            logger.error(f"stealth_scan.json 파싱 실패: {e}")
            return False

    if not scan_data or scan_data.get("error"):
        logger.warning("선매집 데이터 비어있음 — 업로드 스킵")
        return False

    try:
        from data.upload_swing import _get_client
        client = _get_client()
        if not client:
            logger.error("Supabase 클라이언트 없음")
            return False

        today = datetime.now().strftime("%Y-%m-%d")
        summary = scan_data.get("summary", {})

        # 잠복 종목 (상위 20개) — JSONB
        stealth_items = []
        for s in scan_data.get("stealth", [])[:20]:
            stealth_items.append({
                "code": s.get("code"),
                "name": s.get("name"),
                "sector": s.get("sector"),
                "score": s.get("score"),
                "pattern": s.get("pattern"),
                "dual_buy": s.get("dual_buy", False),
                "inst_consec": s.get("inst_consec"),
                "frgn_consec": s.get("frgn_consec"),
                "inst_avg": s.get("inst_avg"),
                "frgn_avg": s.get("frgn_avg"),
                "chg_5d": s.get("chg_5d"),
                "close": s.get("close"),
                "cap": s.get("cap"),
            })

        # 움직임 종목 (상위 10개) — JSONB
        moving_items = []
        for m in scan_data.get("moving", [])[:10]:
            moving_items.append({
                "code": m.get("code"),
                "name": m.get("name"),
                "sector": m.get("sector"),
                "score": m.get("score"),
                "pattern": m.get("pattern"),
                "dual_buy": m.get("dual_buy", False),
                "chg_5d": m.get("chg_5d"),
                "close": m.get("close"),
            })

        # 이미상승 종목 (상위 5개) — JSONB
        surged_items = []
        for g in scan_data.get("surged", [])[:5]:
            surged_items.append({
                "code": g.get("code"),
                "name": g.get("name"),
                "score": g.get("score"),
                "pattern": g.get("pattern"),
                "chg_5d": g.get("chg_5d"),
            })

        row = {
            "date": today,
            "total_scanned": scan_data.get("total_scanned", 0),
            "stealth_count": summary.get("stealth_count", 0),
            "moving_count": summary.get("moving_count", 0),
            "surged_count": summary.get("surged_count", 0),
            "stealth_items": stealth_items,
            "moving_items": moving_items,
            "surged_items": surged_items,
            "top_stealth_names": summary.get("top_stealth", []),
        }

        client.table("intelligence_stealth_scan") \
            .upsert(row, on_conflict="date") \
            .execute()

        logger.info(
            f"선매집 업로드 완료: {today} · "
            f"잠복 {summary.get('stealth_count', 0)} / "
            f"움직임 {summary.get('moving_count', 0)} / "
            f"이미상승 {summary.get('surged_count', 0)}"
        )
        return True

    except Exception as e:
        err = str(e)
        if "does not exist" in err:
            logger.warning(
                "intelligence_stealth_scan 테이블 없음 — "
                "sql/intelligence_stealth_scan_migration.sql 실행 필요"
            )
        else:
            logger.error(f"선매집 업로드 실패: {e}")
        return False
