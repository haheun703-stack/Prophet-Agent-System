# -*- coding: utf-8 -*-
"""
수급 사이클 감지기 Supabase 업로드
==================================
intelligence_cycle_scan 테이블에 일일 사이클 감지 결과 저장.
FLOWX 웹 대시보드 → "사이클 감지기" 패널에 표시됨.

데이터 소스: data_store/cycle_scan.json (C34에서 생성)
"""

import json
import logging
from datetime import datetime
from pathlib import Path

logger = logging.getLogger("BH.CycleUpload")

STORE_DIR = Path(__file__).resolve().parent.parent / "data_store"


def upload_cycle_scan(scan_results: list = None) -> bool:
    """사이클 감지 결과를 Supabase에 업로드.

    Args:
        scan_results: run_cycle_scan()의 반환값 (CycleResult 리스트).
                      None이면 cycle_scan.json에서 로드.
    """
    # 데이터 로드
    raw_data = None
    if not scan_results:
        scan_path = STORE_DIR / "cycle_scan.json"
        if not scan_path.exists():
            logger.warning("cycle_scan.json 없음 — 업로드 스킵")
            return False
        try:
            raw_data = json.loads(scan_path.read_text("utf-8"))
            scan_results = raw_data.get("results", [])
        except Exception as e:
            logger.error(f"cycle_scan.json 파싱 실패: {e}")
            return False

    if not scan_results:
        logger.warning("사이클 데이터 비어있음 — 업로드 스킵")
        return False

    try:
        from data.upload_swing import _get_client
        client = _get_client()
        if not client:
            logger.error("Supabase 클라이언트 없음")
            return False

        today = datetime.now().strftime("%Y-%m-%d")

        # 위상별 분류
        surge_items = []
        acc_items = []
        warning_items = []

        for r in scan_results:
            # CycleResult dataclass → dict (이미 dict일 수도 있음)
            item = r if isinstance(r, dict) else r.__dict__ if hasattr(r, '__dict__') else {}

            phase = item.get("phase", "NEUTRAL")
            compact = {
                "code": item.get("code"),
                "name": item.get("name"),
                "phase": phase,
                "phase_kr": item.get("phase_kr", ""),
                "score": item.get("score", 0),
                "latest_close": item.get("latest_close", 0),
                "change_pct": round(item.get("change_pct", 0), 2),
                "cap_억": item.get("cap_억", 0),
                "market": item.get("market", ""),
                "summary": item.get("summary", ""),
                "signals": _compact_signals(item.get("signals", [])),
            }

            if phase == "SURGE":
                surge_items.append(compact)
            elif phase == "ACCUMULATION":
                acc_items.append(compact)
            elif phase in ("DISTRIBUTION", "PEAK_WARN"):
                warning_items.append(compact)

        # 위상별 카운트
        phase_counts = {}
        for r in scan_results:
            p = r.get("phase") if isinstance(r, dict) else getattr(r, "phase", "NEUTRAL")
            phase_counts[p] = phase_counts.get(p, 0) + 1

        row = {
            "date": today,
            "total_scanned": len(scan_results),
            "surge_count": len(surge_items),
            "accumulate_count": len(acc_items),
            "reversal_count": phase_counts.get("REVERSAL", 0),
            "neutral_count": phase_counts.get("NEUTRAL", 0),
            "distribute_count": phase_counts.get("DISTRIBUTION", 0),
            "peak_warn_count": phase_counts.get("PEAK_WARN", 0),
            "surge_items": surge_items[:15],
            "accumulate_items": acc_items[:15],
            "warning_items": warning_items[:10],
            "top_surge_names": [s["name"] for s in surge_items[:5]],
            "phase_summary": {
                "total": len(scan_results),
                **phase_counts,
            },
        }

        client.table("intelligence_cycle_scan") \
            .upsert(row, on_conflict="date") \
            .execute()

        logger.info(
            f"사이클 업로드 완료: {today} · "
            f"급등임박 {len(surge_items)} / "
            f"매집 {len(acc_items)} / "
            f"경고 {len(warning_items)}"
        )
        return True

    except Exception as e:
        err = str(e)
        if "does not exist" in err:
            logger.warning(
                "intelligence_cycle_scan 테이블 없음 — "
                "sql/intelligence_cycle_scan_migration.sql 실행 필요"
            )
        else:
            logger.error(f"사이클 업로드 실패: {e}")
        return False


def _compact_signals(signals: list) -> list:
    """신호 리스트를 Supabase JSONB에 맞게 압축."""
    result = []
    for s in signals:
        if isinstance(s, dict):
            result.append({
                "name": s.get("name", ""),
                "name_kr": s.get("name_kr", ""),
                "score": s.get("score", 0),
                "detail": s.get("detail", ""),
                "days": s.get("days", 0),
            })
        elif hasattr(s, "name"):
            result.append({
                "name": s.name,
                "name_kr": s.name_kr,
                "score": s.score,
                "detail": s.detail,
                "days": s.days,
            })
    return result
