"""6/4 일봉 incremental 보충 — pykrx(KRX 로그인 전환) 막힘 대응.

배경:
  - pykrx 일봉 경로가 KRX 로그인 전환으로 차단됨(KRX_ID/PW 요구) → 6/4 일봉 수신 불가.
  - KIS 일봉 API는 정상 작동(종목수급도 KIS로 6/4 수신됨).
  - collect_daily_kis 는 덮어쓰기(merge X)라 작은 months로 쓰면 과거가 잘림 → 직접 merge.

설계(안전):
  - KIS 일봉 최근 N일치만 fetch → 기존 daily csv와 병합.
  - keep='first' = 기존 날짜 100% 보존, 신규 거래일만 추가(과거 무손상).
  - 등락률은 신규 거래일이 청크 중간이라 직전 거래일 대비 정상 계산.
  - 원자적 저장(.tmp → replace).
  - SAJANG/매매 무관, 봇 OFF 유지. data_store/daily 보충 전용. read 후 step6 sync 별도.
"""
import sys
import time
import argparse
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from data.kis_collector import _get_broker, _parse_daily_data  # noqa: E402

DAILY = Path(__file__).resolve().parent.parent / "data_store" / "daily"


def fill(codes, recent_days=20, sleep=0.1, log_every=200):
    broker = _get_broker()
    today = datetime.now().strftime("%Y%m%d")
    start = (datetime.now() - timedelta(days=recent_days)).strftime("%Y%m%d")
    ok = added = nochange = failed = 0
    n = len(codes)
    for i, code in enumerate(codes):
        f = DAILY / f"{code}.csv"
        try:
            resp = broker.fetch_ohlcv_domestic(
                symbol=code, timeframe="D", start_day=start, end_day=today
            )
            rows = resp.get("output2", []) if isinstance(resp, dict) else []
            df_new = _parse_daily_data(rows) if rows else None
            if df_new is None or df_new.empty:
                failed += 1
                continue
            df_new.index.name = "날짜"

            if f.exists():
                df_old = pd.read_csv(f, index_col=0, parse_dates=True)
                df_old.index.name = "날짜"
                old_last = df_old.index.max() if len(df_old) else None
                combined = pd.concat([df_old, df_new])
                # keep='first' → 기존(old) 우선 보존, 신규 거래일만 추가
                combined = combined[~combined.index.duplicated(keep="first")].sort_index()
            else:
                old_last = None
                combined = df_new.sort_index()

            new_last = combined.index.max()
            tmp = f.with_suffix(".tmp")
            combined.to_csv(tmp)
            tmp.replace(f)
            ok += 1
            if old_last is None or new_last > old_last:
                added += 1
            else:
                nochange += 1
            time.sleep(sleep)
        except Exception:
            failed += 1
        if (i + 1) % log_every == 0:
            print(
                f"  진행 {i+1}/{n} | ok={ok} added={added} nochange={nochange} failed={failed}",
                flush=True,
            )
    print(
        f"완료: ok={ok} added={added} nochange={nochange} failed={failed} (총 {n})",
        flush=True,
    )
    return ok, added, nochange, failed


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--codes", default="", help="쉼표구분 종목코드. 없으면 전체 유니버스+daily보유분")
    ap.add_argument("--recent-days", type=int, default=20)
    args = ap.parse_args()

    if args.codes:
        codes = [c.strip() for c in args.codes.split(",") if c.strip()]
    else:
        from collect_all import get_universe_codes
        uni = set(get_universe_codes())
        existing = {p.stem for p in DAILY.glob("*.csv") if not p.stem.startswith("_")}
        codes = sorted(uni | existing)

    print(f"대상: {len(codes)}종목 (recent_days={args.recent_days})", flush=True)
    fill(codes, recent_days=args.recent_days)
