# -*- coding: utf-8 -*-
"""재진입 룰 shadow 시뮬레이션 자동 적재 러너 (6/18 설계 구현).

설계: docs/02-design/reentry_rule_5_31.md / 모듈: data/reentry_shadow.py
봇 OFF 유지 중 nightly(VPS 18:00) / OS 스케줄러가 호출한다. early_variant_shadow 직후 실행
(early 후보가 먼저 있어야 재진입 시뮬 가능).

build_reentry_shadow()는 early_variant_shadow.json 후보 전체를 순회 + 멱등(이미 resolved면 skip,
bar 부족=pending은 다음 실행이 충전)이라 per-date 캐치업 불필요 — 1회 호출이면 충분.

★실매수/매도 로직/picks/tier/SAJANG 매도헬퍼/order path 무접촉 — read-only 관측만.★
스케줄러 보호 — 어떤 예외도 exit 0.

실행: python tools/run_reentry_shadow_daily.py
"""
import sys
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent
if str(BASE) not in sys.path:
    sys.path.insert(0, str(BASE))

# Windows cp949 콘솔/리다이렉트에서 한글·em-dash 출력 크래시 방지
try:
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")
except Exception:
    pass

from data.reentry_shadow import build_reentry_shadow, SHADOW_OUT  # noqa: E402


def run() -> dict:
    """early 후보 전체 재진입 시뮬 멱등 적재 + 요약 출력."""
    res = build_reentry_shadow()
    print(
        f"[reentry_runner] 재진입 shadow 적재 — "
        f"resolved {res.get('resolved', 0)} / pending(bar부족) {res.get('pending', 0)} "
        f"-> {SHADOW_OUT.name} (봇OFF·매수/매도 무접촉·관측 전용)"
    )
    return res


if __name__ == "__main__":
    try:
        run()
    except Exception as e:  # 스케줄러 보호 — 어떤 예외도 삼킴
        print(f"[reentry_runner] 치명 예외(무시): {e}")
    sys.exit(0)
