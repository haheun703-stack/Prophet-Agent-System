# -*- coding: utf-8 -*-
"""시장 레짐 게이트 일일 러너 (7/4 신설).

모듈: data/market_regime_gate.py — 전일(D) 마감 데이터 → 다음 거래일 레짐 라벨(record-only).
nightly(VPS 18:00) ⑯ 클러스터 수확 뒤 ⑰로 실행 — ④ ledger(당일 breadth) 생성 이후여야 함.
applies_to 멱등(같은 날 재실행 시 교체) — 1회 호출이면 충분. 자가검증 충전 포함.

★ record-only: market_regime*.json 기록만. 매수/매도/picks/SAJANG/주문 0접촉.
★ NO_GO(전일 breadth≤0.45)만 검증된 회피 신호 — GO는 상승예측 아님(7/4 레짐검증).
스케줄러 보호 — 어떤 예외도 exit 0.

실행: python tools/run_market_regime_daily.py [--asof YYYY-MM-DD]
"""
import sys
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent
if str(BASE) not in sys.path:
    sys.path.insert(0, str(BASE))
try:
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")
except Exception:
    pass

from data.market_regime_gate import build_regime  # noqa: E402


def run(asof=None) -> dict:
    r = build_regime(asof=asof)
    lt = r.get("latest", {})
    v = r.get("validation", {})
    rates = {k: vv.get("down_rate_pct") for k, vv in (v.get("by_regime") or {}).items()}
    print(
        f"[market_regime] {lt.get('based_on')} → {lt.get('applies_to')} "
        f"레짐={lt.get('regime')} breadth={(lt.get('breadth') or {}).get('breadth_pct')} "
        f"/ 자가검증 {v.get('n_scored')}건 익일DOWN율 {rates} "
        f"(record-only·매수 무접촉·NO_GO만 검증신호)"
    )
    return r


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--asof", default=None)
    a = ap.parse_args()
    try:
        run(asof=a.asof)
    except Exception as e:
        print(f"[market_regime] 치명 예외(무시): {e}")
    sys.exit(0)
