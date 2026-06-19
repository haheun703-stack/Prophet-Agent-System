# -*- coding: utf-8 -*-
"""Paper 학습 루프 자동 적재 러너 (6/19 신설).

모듈: data/paper_learning.py — paper 3type forward 누적 집계 학습DB.
nightly(VPS 18:00) ⑤ paper forward 충전 직후 실행(forward 채워진 뒤 학습).
매일 전체 ledger 재집계라 멱등 — 1회 호출이면 충분.

★ read-only 학습(주문/매도/picks/SAJANG 무접촉) + 학습DB json 적재만. 룰 자동변경 0.
스케줄러 보호 — 어떤 예외도 exit 0.

실행: python tools/run_paper_learning_daily.py
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

from data.paper_learning import build_paper_learning, OUT_PATH  # noqa: E402


def run() -> dict:
    r = build_paper_learning()
    n = r.get("total_samples", 0)
    ov = r.get("overall", {})
    print(
        f"[paper_learning] 누적 {n}표본 학습 — d1={ov.get('d1')} d3={ov.get('d3')} "
        f"승률={ov.get('win_d1_pct')}% / 인사이트 {len(r.get('insights', []))}건 "
        f"-> {OUT_PATH.name} (read-only·룰변경0)"
    )
    return r


if __name__ == "__main__":
    try:
        run()
    except Exception as e:
        print(f"[paper_learning] 치명 예외(무시): {e}")
    sys.exit(0)
