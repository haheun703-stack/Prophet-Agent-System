# -*- coding: utf-8 -*-
"""Paper 규칙 적용 연습장부 러너 (6/19 신설).

모듈: data/paper_rule_shadow.py — 학습 발견 규칙을 가상 적용해 현행 vs 규칙적용 forward 비교.
nightly(VPS 18:00) ⑤-2 학습 직후 실행(학습 결과 반영된 뒤 규칙 검증).
매일 전체 재집계라 멱등.

★ read-only(주문/매도/실제 picks 무접촉) + 연습 결과 json 적재만. 실제 선정 변경 0.
스케줄러 보호 — 어떤 예외도 exit 0.

실행: python tools/run_paper_rule_shadow_daily.py
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

from data.paper_rule_shadow import build_rule_shadow, OUT_PATH  # noqa: E402


def run() -> dict:
    r = build_rule_shadow()
    rules = r.get("rules", {})
    msg = " / ".join(
        f"{n}: d3 Δ{v['pooled_delta']['d3']} 승률 Δ{v['pooled_delta']['win_d1_pct']}%p"
        for n, v in rules.items()
    )
    print(f"[rule_shadow] {r.get('cohorts',0)}코호트·{r.get('total_picks',0)}표본 — {msg} "
          f"-> {OUT_PATH.name} (연습 전용·실선정변경0)")
    return r


if __name__ == "__main__":
    try:
        run()
    except Exception as e:
        print(f"[rule_shadow] 치명 예외(무시): {e}")
    sys.exit(0)
