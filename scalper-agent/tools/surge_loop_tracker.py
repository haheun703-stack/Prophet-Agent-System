# -*- coding: utf-8 -*-
"""5/19 D-Day 관망 모드 — 15분 간격 백그라운드 폴링.

11:30 ~ 15:35 KST 동안 매 15분마다 라이브 스냅샷 저장.
장 마감 후 surge_aggregator.py로 일괄 분석.
"""
from __future__ import annotations

import sys
import time
import logging
from datetime import datetime, time as dtime
from pathlib import Path

if sys.platform == "win32":
    for _s in ("stdout", "stderr"):
        s = getattr(sys, _s, None)
        if s and hasattr(s, "reconfigure"):
            try:
                s.reconfigure(encoding="utf-8", errors="replace")
            except Exception:
                pass

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from tools.live_surge_tracker import snapshot_now  # type: ignore

POLL_INTERVAL_SEC = 15 * 60  # 15분
END_TIME = dtime(15, 35)     # 장 마감 + 5분

logger = logging.getLogger("BH.SurgeLoop")


def run_loop():
    logger.info(f"폴링 시작 — 매 {POLL_INTERVAL_SEC//60}분, ~ {END_TIME.strftime('%H:%M')}")
    count = 0
    while True:
        now = datetime.now()
        if now.time() > END_TIME:
            logger.info(f"폴링 종료 (now={now.strftime('%H:%M')}) | 총 {count}회 스냅샷")
            return
        try:
            snap = snapshot_now(top_n=30)
            flu = snap.get("fluctuation_top", [])
            top3 = [f"{x['name']}({x['change_rate']:+.1f}%)" for x in flu[:3]]
            count += 1
            logger.info(f"[{now.strftime('%H:%M')}] 스냅샷 #{count} 저장 | TOP3: {' / '.join(top3)}")
        except Exception as e:
            logger.error(f"스냅샷 실패: {e}")
        time.sleep(POLL_INTERVAL_SEC)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(message)s",
        datefmt="%H:%M:%S",
    )
    run_loop()
