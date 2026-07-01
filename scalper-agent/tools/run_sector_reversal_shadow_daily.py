# -*- coding: utf-8 -*-
"""Sector reversal shadow 자동 적재 러너 (6/9 사장님: "스케줄표에 맞게 알아서 자동 적재").

봇 OFF 유지 중 OS 스케줄러(매일 15:50 + 로그온 캐치업)가 호출한다.
self-heal 캐치업: 디바이스 시작일(=가장 이른 기록일)부터 오늘까지의 미기록 거래일만
pykrx as_of로 채운다 → update_forward 1회. 노트북 1대가 15:50에 꺼져 있어도 다음에
켜질 때 누락분을 자동으로 메운다. ★시작일 이전(과거)은 prepend 하지 않는다 — 관측 범위 고정.★
최초 가동(기록 0건)이면 '오늘'만 적재한다.

★실매수 로직/HOT 판정/tier/SAJANG/order path 무접촉 — read-only 관측만.★
멱등: 이미 기록된 날은 skip(불필요한 pykrx 호출 회피). build_shadow 자체도 idempotent merge.
스케줄러가 깨지지 않게 어떤 예외에도 exit 0.

실행: python tools/run_sector_reversal_shadow_daily.py [--max-days 60]
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

from data.sector_reversal_shadow import build_shadow, update_forward, SHADOW_OUT  # noqa: E402
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


def _loaded_dates() -> set:
    """★ 7/1 전체검수 H3 — 005930 일봉에 실제 적재된 날짜 집합. ① fill 실패/미적재 시
    shadow가 stale 데이터로 today를 기록→멱등키 고착되는 STALE 오염 방지(미적재일은 기록 skip)."""
    f = BASE / "data_store" / "daily" / "005930.csv"
    if not f.exists():
        return set()
    try:
        lines = f.read_text(encoding="utf-8").strip().splitlines()[1:]
        return {ln.split(",")[0] for ln in lines if ln}
    except Exception:
        return set()


def run(max_days: int = 60) -> dict:
    """디바이스 시작일(가장 이른 기록일)~오늘 사이 미기록 거래일만 채움 + forward 갱신.

    과거(시작일 이전) prepend 금지. 기록 0건이면 '오늘'만. max_days = 폭주 안전 바운드.
    """
    today = date.today()
    done = _recorded_dates()
    loaded = _loaded_dates()  # ★ 7/1 검수 H3: fill 성공(일봉 적재) 전제 — 미적재일 기록 skip
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
            if ds not in done and ds in loaded:  # ★ H3: 일봉 미적재일은 기록 skip(멱등키 고착 방지)
                try:
                    r = build_shadow(ds)
                    built.append([ds, len(r.get("records", []))])
                except Exception as e:
                    print(f"[shadow_runner] build {ds} 실패(무시): {e}")
        d += timedelta(days=1)

    filled = 0
    try:
        filled = update_forward()
    except Exception as e:
        print(f"[shadow_runner] update_forward 실패(무시): {e}")

    print(
        f"[shadow_runner] {today.strftime('%Y-%m-%d')} 적재 — "
        f"신규 {len(built)}일 {built} / forward {filled}건 / 기존 {len(done)}일 "
        f"(봇OFF·매수 무접촉·관측 전용)"
    )
    return {"today": today.strftime("%Y-%m-%d"), "built": built,
            "forward_filled": filled, "already_recorded": len(done)}


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="sector reversal shadow 자동 적재(self-heal)")
    ap.add_argument("--max-days", type=int, default=60,
                    help="시작일~오늘 폭주 안전 바운드 (기본 60)")
    args = ap.parse_args()
    try:
        run(args.max_days)
    except Exception as e:  # 스케줄러 보호 — 어떤 예외도 삼킴
        print(f"[shadow_runner] 치명 예외(무시): {e}")
    sys.exit(0)
