# -*- coding: utf-8 -*-
"""페이퍼 전용 포트폴리오 시뮬 (6/27 신설) — 봇 OFF여도 매일 도는 가상 매매 루프.

배경:
  - 봇 OFF(AUTO_TRADE_DISABLED)로 실제 paper_portfolio 매매가 5/29부터 정지.
  - paper_3type ledger(봇 무관·매일 nightly ④ 생성)로 가상 포트폴리오를 복리로 굴려
    breadth 게이트 효과를 "실계좌처럼" 누적수익률·MDD로 검증한다.
  - "관측만"이 아니라 현행(baseline) vs breadth게이트 두 가상계좌를 나란히 굴려 비교.

★ 안전: read-only(ledger 읽기만)·실주문0·라이브0·picks불변·SAJANG 매도헬퍼 미접촉(룰값만 참조).
손절 근사: MAE ≤ -TRAILING_PCT → 손절(-TRAILING), 아니면 forward_d3 보유 청산.
  (일봉 OHLC 정밀 트레일링/재진입은 다음 정밀화 — MVP는 ledger forward+MAE 근사.)
사용법: python -m data.paper_sim_portfolio
"""
from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path

from data.paper_rule_shadow import _cohorts
from data.sajang_rules import SAJANG

logger = logging.getLogger("BH.PaperSim")

OUT_PATH = Path(__file__).resolve().parent.parent / "data_store" / "paper_sim_portfolio.json"

TOP_K = 7              # 매일 진입 종목 수(단타봇 09:15 5 + 14:50 2 가정)
INVEST_RATIO = 0.7     # 자본 투입 비율(SAJANG 30% 현금보유 룰 반영)


def _stock_pnl(c) -> float:
    """종목 트레일링 손절 근사 pnl(%). SAJANG 충실:
    +TRAIL 고점 도달 → 고점-TRAIL 익절(이익 락인) / 그 전 MAE≤-TRAIL → 진입가-TRAIL 손절 / else d3 만기."""
    trail = SAJANG.TRAILING_PCT
    mfe = c.get("MFE")
    mae = c.get("MAE")
    if mfe is not None and mfe >= trail:
        return round(mfe - trail, 2)       # 고점 트레일링 익절(이익 구간)
    if mae is not None and mae <= -trail:
        return -trail                      # 진입가 -TRAIL 손절
    d3 = c.get("forward_d3")
    return d3 if d3 is not None else 0.0    # 보유 만기 청산(d3)


def simulate(cohorts: dict, gate_fn, top_k: int = TOP_K,
             invest_ratio: float = INVEST_RATIO) -> dict:
    """가상 포트폴리오 복리 시뮬. gate_fn(c)=True인 후보만 진입."""
    cash = 1.0
    peak = 1.0
    mdd = 0.0
    win_days = n_days = 0
    trades = wins = 0
    for date in sorted(cohorts):
        picks = [c for c in cohorts[date] if gate_fn(c)]
        if not picks:
            continue
        sel = picks[:top_k]
        pnls = [_stock_pnl(c) for c in sel]
        for p in pnls:
            trades += 1
            if p > 0:
                wins += 1
        day_ret = (sum(pnls) / len(pnls)) / 100.0 * invest_ratio
        cash *= (1 + day_ret)
        n_days += 1
        if day_ret > 0:
            win_days += 1
        peak = max(peak, cash)
        mdd = min(mdd, cash / peak - 1)
    return {
        "final_equity": round(cash, 4),
        "total_return_pct": round((cash - 1) * 100, 2),
        "mdd_pct": round(mdd * 100, 2),
        "n_days": n_days,
        "win_day_pct": round(100 * win_days / n_days, 0) if n_days else None,
        "trades": trades,
        "win_pct": round(100 * wins / trades, 0) if trades else None,
    }


def build_paper_sim(save: bool = True) -> dict:
    """현행(baseline) vs breadth게이트 두 가상 포트폴리오 복리 비교."""
    cohorts = _cohorts()
    if not cohorts:
        logger.info("[paper_sim] 코호트 없음 — skip")
        return {"cohorts": 0}

    base = simulate(cohorts, lambda c: True)
    gated = simulate(cohorts, lambda c: c.get("_breadth_state") == "BROAD_UP")

    out = {
        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "cohorts": len(cohorts),
        "config": {"top_k": TOP_K, "invest_ratio": INVEST_RATIO,
                   "stop_model": f"MAE<=-{SAJANG.TRAILING_PCT}%→손절 else forward_d3"},
        "baseline": base,
        "breadth_gated": gated,
        "note": "페이퍼 전용 가상 포트폴리오(record-only·봇무관·매일). 실주문0·라이브0. "
                "★절대수익 해석금지 — MFE익절 낙관편향+미투입현금 미반영으로 낙관됨. "
                "baseline vs breadth_gated 상대비교 전용(절대수익=생존편향·상대만 신뢰). "
                "MAE/MFE 손절근사 MVP(정밀 OHLC 트레일링·재진입은 다음 정밀화).",
    }
    if save:
        try:
            OUT_PATH.write_text(json.dumps(out, ensure_ascii=False, indent=1), encoding="utf-8")
        except Exception as e:  # noqa: BLE001
            logger.warning(f"[paper_sim] 저장 실패: {e}")

    logger.info(
        f"[paper_sim] baseline {base['total_return_pct']}%(MDD{base['mdd_pct']}·승률{base['win_pct']}%) "
        f"vs breadth {gated['total_return_pct']}%(MDD{gated['mdd_pct']}·승률{gated['win_pct']}%)"
    )
    return out


if __name__ == "__main__":
    import sys
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    r = build_paper_sim()
    if r.get("cohorts"):
        b, g = r["baseline"], r["breadth_gated"]
        print(f"\n=== 페이퍼 전용 가상 포트폴리오 ({r['cohorts']}일) ===")
        print(f"현행:       누적 {b['total_return_pct']:+}% | MDD {b['mdd_pct']}% | 승률 {b['win_pct']}% | {b['trades']}거래")
        print(f"breadth게이트: 누적 {g['total_return_pct']:+}% | MDD {g['mdd_pct']}% | 승률 {g['win_pct']}% | {g['trades']}거래")
        print("\n★ record-only·봇무관·실주문0. MAE 손절근사 MVP.")
