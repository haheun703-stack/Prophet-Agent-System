# -*- coding: utf-8 -*-
"""명분+맥점 스캐너 자동 러너 (6/24 사장님 지시) — record-only.

실시간 순위(ranking_scanner) 갱신 → 명분+맥점 분석(catalyst_scanner) 한 번에.
장중/마감후 아무 때나 실행하면 그 시점 "명분 있는 끼 종목"을 발굴한다.

★ record-only: 매수/매도/picks/SAJANG/order 0 접촉. 봇 OFF·실주문 0.
   is_trading_day 가드. 어떤 예외도 exit 0 (스케줄러 보호).

실행: python tools/run_catalyst_scanner_daily.py [--top N]
"""
import sys
from pathlib import Path
from datetime import date

BASE = Path(__file__).resolve().parent.parent
if str(BASE) not in sys.path:
    sys.path.insert(0, str(BASE))
try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

from data.trading_calendar import is_trading_day  # noqa: E402


def run(top_n: int = 30) -> int:
    if not is_trading_day(date.today()):
        print("[catalyst] 휴장일 — skip (exit 0)")
        return 0
    # 1) 실시간 순위 갱신 (KIS — 상한가/급등/거래량)
    try:
        from tools.ranking_scanner import scan_surge
        scan_surge()
        print("[catalyst] ranking_scan 갱신 완료")
    except Exception as e:
        print(f"[catalyst] ranking_scan 갱신 실패(기존 사용): {e}")
    # 2) 명분+맥점 분석
    try:
        from tools.catalyst_scanner import scan_catalyst, format_report
        r = scan_catalyst(top_n=top_n)
        print(format_report(r))
        cand = [it for it in r["items"] if str(it.get("verdict", "")).startswith("★")]
        print(f"\n[catalyst] ★관측후보(명분+맥점) {len(cand)}종목 / 전체 {r['count']}종목 "
              f"— record-only·매수 무접촉")
    except Exception as e:
        print(f"[catalyst] ERROR(무시): {e}")
    return 0


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--top", type=int, default=30)
    args = ap.parse_args()
    try:
        sys.exit(run(top_n=args.top))
    except Exception as e:  # noqa: BLE001
        print(f"[catalyst] 최상위 예외(무시): {e}")
        sys.exit(0)
