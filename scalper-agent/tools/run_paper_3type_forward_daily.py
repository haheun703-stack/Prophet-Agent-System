# -*- coding: utf-8 -*-
"""3-Type paper ledger forward 자동 충전 러너 (6/15 신설 — H-3 dead code 해소).

배경(진단 6/12~6/15):
  - data/paper_3type_ledger.py:update_paper_3type_forward 가 정의만 되고 **호출처 0**
    (= H-3 dead code). 그 결과 6/4~6/12 전 코호트의 forward_d1/d3/d5·MFE/MAE 가 None →
    6/12 1차 판정(A/B/C 수익 비교) 불가의 근본원인.
  - shadow는 run_sector_reversal_shadow_daily.py 가 nightly ③에서 build+forward를 돌리는데
    paper는 ④에서 build(생성)만 하고 forward 충전 단계가 없었다.

이 러너(= nightly ⑤로 배선):
  - data_store/paper_3type/ledger_YYYY-MM-DD.json (정규 본장부만, _bc_only 등 변형 제외)을
    최근 max_days 이내로 훑어 각 날짜에 update_paper_3type_forward(date) 호출.
  - update_paper_3type_forward 자체가 멱등(forward_d5 채워지면 skip) → 매일 돌려도 안전.
  - forward는 '과거' 코호트 대상(오늘 코호트는 미래 일봉 부재라 None 유지가 정상).

안전(★ run_sector_reversal_shadow_daily.py 와 동일 규격 ★):
  - 봇 OFF·매수 무접촉·실주문 0·KIS 주문 0·SAJANG 무참조 — ledger 관측 필드만 충전.
  - picks(후보 선정)·summary·market_context·rejects 불변 (update_paper_3type_forward 보장).
  - 스케줄러 보호: 어떤 예외에도 exit 0. 단일 날짜 실패는 격리(다음 날짜 진행).

실행: python tools/run_paper_3type_forward_daily.py [--max-days 30]
"""
import argparse
import re
import sys
from datetime import date, datetime, timedelta
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

from data.paper_3type_ledger import LEDGER_DIR, update_paper_3type_forward  # noqa: E402

# 정규 본장부만: ledger_2026-06-12.json (O) / ledger_2026-06-12_bc_only.json (X)
_LEDGER_RE = re.compile(r"^ledger_(\d{4}-\d{2}-\d{2})\.json$")


def _ledger_dates() -> list:
    """LEDGER_DIR의 정규 본장부 날짜 목록(거래일순). 변형(suffix) 파일 제외."""
    if not LEDGER_DIR.exists():
        return []
    out = []
    for f in LEDGER_DIR.iterdir():
        m = _LEDGER_RE.match(f.name)
        if m:
            out.append(m.group(1))
    return sorted(out)


def run(max_days: int = 30) -> dict:
    """최근 max_days 이내 본장부를 훑어 forward 충전. 반환=요약 dict.

    max_days = 폭주 안전 바운드(오늘 기준). forward_d5는 5거래일이면 채워지므로
    기본 30일이면 미완성 코호트를 충분히 커버. 멱등이라 과거 재방문 비용은 skip 1회.
    """
    today = date.today()
    cutoff = today - timedelta(days=max_days)
    dates = _ledger_dates()

    targeted, total_filled = [], 0
    for ds in dates:
        try:
            d = datetime.strptime(ds, "%Y-%m-%d").date()
        except Exception:
            continue
        if d < cutoff:           # 폭주 방지 — 너무 과거는 skip
            continue
        try:
            n = update_paper_3type_forward(ds)   # 멱등 · picks 불변 · 매매 무접촉
            if n:
                targeted.append([ds, n])
                total_filled += n
        except Exception as e:   # 단일 날짜 실패 격리
            print(f"[paper_fwd] {ds} 충전 실패(무시): {e}")

    print(
        f"[paper_fwd] {today.strftime('%Y-%m-%d')} forward 충전 — "
        f"대상 {len(dates)}개 장부 / 갱신 {len(targeted)}일 {targeted} / "
        f"총 {total_filled}건 (봇OFF·매수 무접촉·관측 전용)"
    )
    return {"today": today.strftime("%Y-%m-%d"), "ledgers": len(dates),
            "updated": targeted, "total_filled": total_filled}


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="3-Type paper ledger forward 자동 충전(멱등)")
    ap.add_argument("--max-days", type=int, default=30,
                    help="오늘 기준 폭주 안전 바운드 (기본 30)")
    args = ap.parse_args()
    try:
        run(args.max_days)
    except Exception as e:  # 스케줄러 보호 — 어떤 예외도 삼킴
        print(f"[paper_fwd] 치명 예외(무시): {e}")
    sys.exit(0)
