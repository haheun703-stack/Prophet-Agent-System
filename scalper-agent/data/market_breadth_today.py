# -*- coding: utf-8 -*-
"""오늘 시장 폭(breadth) 상태 조회 헬퍼 (6/27 신설) — NXT 페이퍼 진입 게이트용.

배경:
  - paper_3type ledger forward 검증: BROAD_UP일 때 진입 승률 28→55%(VPS 205표본).
  - NXT 페이퍼 진입(trading_coo _job_nxt_paper_register)이 BRAIN 레짐 게이트만 있어
    시장 취약(breadth 약세)을 충분히 못 잡음(-12.51%). breadth로 보강.

획득 우선순위(가벼움→독립):
  1. 오늘 paper_3type ledger market_context.breadth_state (이미 계산됨·csv 1개·가벼움)
  2. 없으면 market_breadth 직접 계산(전종목 일봉·무거움·독립)

★ read-only: 일봉/ledger csv 읽기만. 매매/주문/SAJANG/picks 0 접촉.
  breadth_state 정의(market_breadth_6_8): 상승종목 ≥60% BROAD_UP / ≤40% BROAD_DOWN / 그외 MIXED / 데이터부재 None.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path

logger = logging.getLogger("BH.BreadthToday")

ROOT = Path(__file__).resolve().parent.parent          # scalper-agent
PROJECT_ROOT = ROOT.parent                             # Prophet_Agent_System
LEDGER_DIR = ROOT / "data_store" / "paper_3type"
DAILY_DIR = PROJECT_ROOT / "stock_data_daily"


def get_today_breadth_state(asof: str | None = None):
    """오늘 시장 breadth_state 조회. 반환: (breadth_state, breadth_pct).

    조회 실패/데이터부재 시 (None, None) → 호출측에서 게이트 미작동(보수적).
    """
    if asof is None:
        asof = datetime.now().strftime("%Y-%m-%d")

    # 1) 오늘 ledger market_context (가벼움 — 이미 nightly ④에서 계산됨)
    led = LEDGER_DIR / f"ledger_{asof}.json"
    if led.exists():
        try:
            ctx = json.loads(led.read_text(encoding="utf-8")).get("market_context", {}) or {}
            state = ctx.get("breadth_state")
            if state:
                return state, ctx.get("breadth_pct")
        except Exception as e:  # noqa: BLE001
            logger.warning(f"[breadth_today] ledger 읽기 실패({e}) — 직접계산 fallback")

    # 2) 직접 계산 (전종목 일봉 — 무거움·독립)
    try:
        import sys
        tools = str(ROOT / "tools")
        if tools not in sys.path:
            sys.path.insert(0, tools)
        from market_breadth_6_8 import market_breadth  # noqa: E402
        from price_structure_features_6_8 import find_index_asof  # noqa: E402
        from leader_prospective_scan_6_2 import load_daily  # noqa: E402

        if not DAILY_DIR.exists():
            return None, None
        c2p = {p.stem: str(p) for p in DAILY_DIR.glob("*.csv")}
        if not c2p:
            return None, None
        ctx = market_breadth(c2p, asof, load_daily, find_index_asof)
        return ctx.get("breadth_state"), ctx.get("breadth_pct")
    except Exception as e:  # noqa: BLE001
        logger.warning(f"[breadth_today] 직접계산 실패({e}) — None 반환")
        return None, None


if __name__ == "__main__":
    import sys
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    s, p = get_today_breadth_state()
    print(f"오늘 breadth_state={s} breadth_pct={p}")
