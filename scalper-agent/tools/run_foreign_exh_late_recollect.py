# -*- coding: utf-8 -*-
"""외국인 소진율(foreign_exh) 늦은-게시 재수집 러너 (6/24 사장님 지시) — record-only.

[배경] nightly(VPS 18:00) ⑥ "수급 4종"은 18:09경 네이버 frgn 소진율을 수집하는데,
네이버 frgn 소진율은 거래일 당일 "저녁에 점진 게시"된다 → 18:09 시점엔 수십 종목만
게시돼 대부분이 당일분을 못 받는다(6/24 점검 실측: 44/3510). 19:0x엔 게시 완료되어
재수집 시 44→2494(유니버스 96.7%) 채워짐을 확인.

[해결] 이 러너를 더 늦은 시각(cron 19:40 평일)에 한 번 더 돌려 당일 소진율을 채운다.
네이버 frgn만 재조회(KIS·매매·주문 무관) → flow/*_foreign_exh.csv 갱신만. 네이버 frgn은
거래일 행만 제공하므로 휴장 ghost가 원천 차단된다(collect_foreign_exhaustion 주석 참조).
nightly ⑥와 동일 함수라 동작 검증됨. 18:00 nightly는 18:25경 종료 → 동시쓰기 없음.

★ record-only: 매수/매도/picks/SAJANG/order 0 접촉. 봇 OFF·실주문 0.
   is_trading_day 가드. 어떤 예외도 exit 0 (스케줄러 보호).

실행: python tools/run_foreign_exh_late_recollect.py
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


def run() -> int:
    if not is_trading_day(date.today()):
        print("[foreign_exh_late] 휴장일 — skip (exit 0)")
        return 0
    try:
        from collect_all import get_universe_codes
        from data.flow_collector import collect_foreign_exhaustion
    except Exception as e:  # noqa: BLE001 — import 실패도 스케줄러 보호
        print(f"[foreign_exh_late] import 실패(무시): {e}")
        return 0

    try:
        codes = get_universe_codes()
        if not codes:
            print("[foreign_exh_late] 유니버스 비어있음 — skip")
            return 0
        res = collect_foreign_exhaustion(codes)
        # 당일분 적재 종목 수 카운트 (검증용 로그 — 매매 무관)
        today_str = date.today().strftime("%Y-%m-%d")
        filled = 0
        for df in res.values():
            try:
                if df is not None and len(df) and any(
                    str(i)[:10] == today_str for i in df.index
                ):
                    filled += 1
            except Exception:
                continue
        print(
            f"[foreign_exh_late] {today_str} 소진율 재수집 — "
            f"유니버스 {len(codes)} / 수집 {len(res)} / 당일적재 {filled} "
            f"(record-only·매수 무접촉·봇 OFF)"
        )
    except Exception as e:  # noqa: BLE001
        print(f"[foreign_exh_late] ERROR(무시): {e}")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(run())
    except Exception as e:  # noqa: BLE001
        print(f"[foreign_exh_late] 최상위 예외(무시): {e}")
        sys.exit(0)
