# -*- coding: utf-8 -*-
"""
KIS 실계좌 잔고 확인 스크립트
==============================
월요일 장전(08:30~08:55) 실행하여 실제 평가액 확인 후 봇 시작.

사용법:
    python tools/check_balance.py
"""

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))  # tools/manual/ → scalper-agent

import json
from pathlib import Path
from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parent.parent.parent.parent / ".env")  # tools/manual/ → repo root

from bot.kis_trader import KISTrader

RISK_STATE_PATH = Path(__file__).resolve().parent.parent.parent / "data_store" / "risk_state.json"  # tools/manual/ → scalper-agent


def main():
    print("=" * 50)
    print("  KIS 실계좌 잔고 확인")
    print("=" * 50)

    # 1) KIS API 잔고 조회
    trader = KISTrader()
    bal = trader.fetch_balance()

    if not bal.get("success"):
        print(f"\n[ERROR] 잔고 조회 실패: {bal.get('message')}")
        print("  -> KIS API 키/토큰 확인 필요")
        return

    cash = bal["cash"]
    total_eval = bal["total_eval"]
    positions = bal["positions"]

    print(f"\n  현금:       {cash:>12,}원")
    print(f"  총 평가액:  {total_eval:>12,}원")
    print(f"  보유종목:   {len(positions)}종목")

    if positions:
        print(f"\n  {'종목명':<12s} {'수량':>6s} {'평균가':>10s} {'현재가':>10s} {'손익':>10s} {'수익률':>7s}")
        print("  " + "-" * 60)
        for p in positions:
            print(f"  {p['name']:<12s} {p['qty']:>6d} {p['avg_price']:>10,} {p['current_price']:>10,} {p['pnl_amount']:>+10,} {p['pnl_rate']:>+6.1f}%")

    # 2) risk_state.json 현재 상태
    print(f"\n{'=' * 50}")
    print("  risk_state.json 상태")
    print("=" * 50)

    if RISK_STATE_PATH.exists():
        with open(RISK_STATE_PATH, "r", encoding="utf-8") as f:
            rs = json.load(f)
        peak = rs.get("peak_equity", 0)
        mdd = rs.get("current_mdd_pct", 0)
        blocked = rs.get("blocked_reason", "")
        print(f"  peak_equity:    {peak:>12,}원")
        print(f"  current_mdd:    {mdd:>11.1f}%")
        print(f"  blocked_reason: {blocked or '(없음)'}")

        # 3) 판정
        print(f"\n{'=' * 50}")
        print("  판정")
        print("=" * 50)

        if peak == 0:
            print(f"  [OK] peak_equity=0 (리셋 상태)")
            print(f"       -> 첫 잔고 조회에서 새 peak={total_eval:,}원 설정 예정")
            print(f"       -> MDD 게이트 정상 작동 예상")
        elif blocked:
            print(f"  [BLOCKED] MDD 블록 활성!")
            print(f"       peak={peak:,} vs 현재={total_eval:,}")
            if peak > 0:
                real_mdd = (peak - total_eval) / peak * 100
                print(f"       실제 MDD: -{real_mdd:.1f}%")
                if real_mdd < 4.5:
                    print(f"       -> 실제 MDD < 4.5% — risk_state 리셋 필요!")
                else:
                    print(f"       -> 실제 MDD >= 4.5% — 진짜 MDD 초과! 수동 확인 필요")
        else:
            if total_eval > 0 and peak > 0:
                real_mdd = (peak - total_eval) / peak * 100
                print(f"  [OK] MDD: -{real_mdd:.1f}% (한도 -4.5%)")
            else:
                print(f"  [OK] 정상")
    else:
        print("  risk_state.json 없음 — 자동 생성 예정")

    print()


if __name__ == "__main__":
    main()
