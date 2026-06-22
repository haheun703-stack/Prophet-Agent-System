# -*- coding: utf-8 -*-
"""테마 relay-aware shadow 자동 적재 러너 (6/22 구현).

설계: docs/02-design/theme_relay_aware_selection.md / 모듈: data/theme_relay_shadow.py
봇 OFF 유지 중 nightly(VPS 18:00)/OS 스케줄러가 호출. record-only 관측.

build_shadow(asof)는 (theme,date) 멱등 → 최근 N거래일 백필로 relay 히스토리(바통 역전 판정) 확보.
★실매수/매도/picks/tier/SAJANG/order 무접촉 — read-only 관측만. 어떤 예외도 exit 0(스케줄러 보호).★

실행: python tools/run_theme_relay_shadow_daily.py
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

from data.theme_relay_shadow import build_shadow, DAILY_DIR, SHADOW_OUT  # noqa: E402

BACKFILL_DAYS = 12   # relay 역전 판정 위해 최근 거래일 히스토리 확보(멱등이라 재실행 안전)


def _recent_dates() -> list:
    """삼성전자(005930) 일봉에서 최근 거래일 BACKFILL_DAYS개."""
    f = DAILY_DIR / "005930.csv"
    if not f.exists():
        return []
    ds = []
    for ln in f.read_text(encoding="utf-8").splitlines():
        p = ln.split(",")
        if len(p) >= 6 and p[0][:4].isdigit():
            ds.append(p[0])
    return sorted(ds)[-BACKFILL_DAYS:]


def run() -> dict:
    dates = _recent_dates()
    total = 0
    for asof in dates:
        try:
            r = build_shadow(asof)
            total += r.get("built", 0)
        except Exception as e:
            print(f"[theme_relay_runner] {asof} skip(무시): {e}")
    print(f"[theme_relay_runner] 최근 {len(dates)}거래일 적재 {total}건 -> {SHADOW_OUT.name} "
          f"(봇OFF·매수/매도 무접촉·관측 전용)")
    return {"dates": len(dates), "built": total}


if __name__ == "__main__":
    try:
        run()
    except Exception as e:
        print(f"[theme_relay_runner] 치명 예외(무시): {e}")
    sys.exit(0)
