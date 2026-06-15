# -*- coding: utf-8 -*-
"""Early variant shadow 자동 적재 러너 (6/16 설계 구현 — strict/early 병렬 관측).

설계: docs/02-design/early_entry_shadow_6_15.md
봇 OFF 유지 중 nightly(VPS 18:00) / OS 스케줄러가 호출한다. sector_reversal 러너와 동일 규격:
self-heal 캐치업 — 디바이스 시작일(=가장 이른 기록일)~오늘 미기록 거래일만 채움 + forward 갱신.
★시작일 이전(과거)은 prepend 금지 — 관측 범위 고정.★ 최초 가동(기록 0건)이면 '오늘'만 적재.

★실매수 로직/picks/tier/SAJANG 매도헬퍼/order path 무접촉 — read-only 관측만.★
멱등: 이미 기록된 (date,code,variant)는 build 자체가 보존 merge. 스케줄러 보호 — 어떤 예외도 exit 0.

실행: python tools/run_early_variant_shadow_daily.py [--max-days 60]
"""
import argparse
import json
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent
if str(BASE) not in sys.path:
    sys.path.insert(0, str(BASE))

# Windows cp949 콘솔/리다이렉트에서 한글·em-dash 출력 크래시 방지 (vbs는 PYTHONIOENCODING=utf-8도 세팅)
try:
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")
except Exception:
    pass

from data.early_variant_shadow import build_early_shadow, update_early_forward, SHADOW_OUT  # noqa: E402
from data.trading_calendar import is_trading_day  # noqa: E402


def _recorded_dates() -> set:
    """이미 shadow json에 적재된 날짜 집합 (없으면 빈 set)."""
    if not SHADOW_OUT.exists():
        return set()
    try:
        data = json.loads(SHADOW_OUT.read_text(encoding="utf-8"))
        return {r["date"] for r in data.get("records", []) if r.get("date")}
    except Exception:
        return set()


def run(max_days: int = 60) -> dict:
    """디바이스 시작일(가장 이른 기록일)~오늘 사이 미기록 거래일만 채움 + forward 갱신.

    과거(시작일 이전) prepend 금지. 기록 0건이면 '오늘'만. max_days = 폭주 안전 바운드.
    """
    today = date.today()
    done = _recorded_dates()
    if done:
        try:
            anchor = min(datetime.strptime(x, "%Y-%m-%d").date() for x in done)
        except Exception:
            anchor = today
    else:
        anchor = today  # 최초 가동 = 오늘만 (과거 prepend 금지)
    if (today - anchor).days > max_days:  # 시작일 비정상 과거 → 폭주 방지
        anchor = today - timedelta(days=max_days)

    built = []
    d = anchor
    while d <= today:
        if is_trading_day(d):
            ds = d.strftime("%Y-%m-%d")
            if ds not in done:
                try:
                    r = build_early_shadow(ds)
                    built.append([ds, r.get("strict", 0), r.get("early", 0)])
                except Exception as e:
                    print(f"[early_runner] build {ds} 실패(무시): {e}")
        d += timedelta(days=1)

    filled = 0
    try:
        filled = update_early_forward()
    except Exception as e:
        print(f"[early_runner] update_early_forward 실패(무시): {e}")

    print(
        f"[early_runner] {today.strftime('%Y-%m-%d')} 적재 — "
        f"신규 {len(built)}일 [date,strict,early]={built} / forward {filled}건 / 기존 {len(done)}일 "
        f"(봇OFF·매수 무접촉·관측 전용)"
    )
    return {"today": today.strftime("%Y-%m-%d"), "built": built,
            "forward_filled": filled, "already_recorded": len(done)}


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="early variant shadow 자동 적재(self-heal)")
    ap.add_argument("--max-days", type=int, default=60,
                    help="시작일~오늘 폭주 안전 바운드 (기본 60)")
    args = ap.parse_args()
    try:
        run(args.max_days)
    except Exception as e:  # 스케줄러 보호 — 어떤 예외도 삼킴
        print(f"[early_runner] 치명 예외(무시): {e}")
    sys.exit(0)
