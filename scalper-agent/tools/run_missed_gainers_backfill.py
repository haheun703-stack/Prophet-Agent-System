# -*- coding: utf-8 -*-
"""missed_gainers backfill 러너 (nightly ②-2).

일봉이 늦게 적재된 거래일의 missed_gainers를 자동 재생성한다(self-heal).
★ 6/22 STALE 사고 재발 방지: 일봉이 KRX 지연 등으로 당일 저녁 미적재되면
   missed_gainers가 빈 결과로 미생성 → 다운스트림(C35 패턴스캔·bomb_watchlist)
   FileNotFoundError 연쇄 + 영구 공백. 일봉이 채워진 뒤 이 러너가 복구한다.

배선: run_nightly_pipeline.py ②-2 (① fill 4-way → ② step6 sync 뒤, 일봉 채운 직후).

원칙:
  - is_trading_day 가드: 휴장일이면 skip (exit 0)
  - 어떤 예외도 exit 0 (nightly 비차단)
  - 멱등: 이미 있는 파일은 건드리지 않음 (daily_learner.backfill_missed_gainers)
  - 매수/매도 무접촉 — 데이터 위생 전용
"""
import sys
from pathlib import Path
from datetime import date

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from data.trading_calendar import is_trading_day  # noqa: E402


def main() -> int:
    if not is_trading_day(date.today()):
        print("[MissedGainersBackfill] 휴장일 — skip (exit 0)")
        return 0
    try:
        from data.daily_learner import backfill_missed_gainers
        r = backfill_missed_gainers(lookback_days=10)
        print(
            f"[MissedGainersBackfill] created={len(r['created'])} "
            f"existing={r['existing']} skipped={len(r['skipped'])} "
            f"created_days={r['created']}"
        )
    except Exception as e:  # noqa: BLE001
        print(f"[MissedGainersBackfill] ERROR(무시): {e}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
