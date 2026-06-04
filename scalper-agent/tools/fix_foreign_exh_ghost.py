"""foreign_exh 휴장 ghost 행 제거 — 데이터 복구 (코드 fix 아님).

배경:
  collect_foreign_exhaustion 은 현재가 스냅샷 API(inquire-price) + datetime.now() 로
  날짜를 찍음(KIS 응답의 거래일 미사용). 휴장일/장전에 수집되면
  "오늘 날짜 + 직전거래일 값" = ghost 행이 쌓임.
  - 6/3(선거 휴장) ghost: 휴장일인데 행 존재 → 제거 대상
  - 6/4(거래일) stale: 장전 수집으로 값이 6/2 → force 재수집으로 self-heal(별도)

이 스크립트:
  전 foreign_exh csv에서 is_trading_day 아닌 행(휴장 ghost)만 제거.
  market_investor_collector 의 self-heal 패턴과 동일. 원자적 저장.
  거래일 stale(6/4 등)은 건드리지 않음 → force 재수집이 담당.

근본 코드 fix(휴장가드 + 종가확정 가드)는 지시서대로 후속.
봇 OFF·매매 무관·SAJANG 무변경.
"""
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from data.trading_calendar import is_trading_day  # noqa: E402

FLOW = Path(__file__).resolve().parent.parent / "data_store" / "flow"


def main():
    files = removed = touched = errs = 0
    removed_dates = {}
    for f in FLOW.glob("*_foreign_exh.csv"):
        files += 1
        try:
            df = pd.read_csv(f, index_col=0, parse_dates=True)
        except Exception:
            errs += 1
            continue
        if df.empty:
            continue
        mask = [is_trading_day(ix.date()) for ix in df.index]
        if all(mask):
            continue
        # 제거되는 휴장일 집계
        for keep, ix in zip(mask, df.index):
            if not keep:
                k = str(ix.date())
                removed_dates[k] = removed_dates.get(k, 0) + 1
        df2 = df[mask]
        tmp = f.with_suffix(".tmp")
        df2.to_csv(tmp)
        tmp.replace(f)
        removed += (len(df) - len(df2))
        touched += 1
    print(f"스캔 {files}파일 | 정리 {touched}파일 | ghost행 제거 {removed} | 오류 {errs}")
    if removed_dates:
        print("제거된 휴장일 분포:")
        for d in sorted(removed_dates):
            print(f"  {d}: {removed_dates[d]}행")


if __name__ == "__main__":
    main()
