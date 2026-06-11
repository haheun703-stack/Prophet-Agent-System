"""F1 외인 장중 잠정 로거 — 야간 forward 충전 + preflight '찍힘' 검증 (6/11 신설).

야간 파이프라인(18:00) ⑧단계로 호출. fill(①) 후라 일봉 최신 → 전일 F1의 forward_d1(오늘 종가) 충전.
preflight = 사장님 단서② "연결됨이 아니라 찍힘" — 오늘 F1 ledger가 실제 찍혔는지 자동 판정(경보).

안전: read-only(ledger 계산만)·매매 무접촉·어떤 예외도 exit 0.
"""
import sys
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


def main():
    from data.foreign_f1_intraday_logger import update_forward, check_f1_ledger_today, finalize
    total = 0
    for d in range(0, 7):  # 최근 7일 F1의 finalize(백업)+forward 멱등 충전(채울 것만)
        asof = (date.today() - timedelta(days=d)).isoformat()
        try:
            finalize(asof)  # 봇 16:05 finalize 누락 시 백업(멱등, is_final이면 skip)
        except Exception as e:  # noqa: BLE001
            print(f"  F1 finalize {asof} 실패(무시): {str(e)[:80]}")
        try:
            total += update_forward(asof)
        except Exception as e:  # noqa: BLE001
            print(f"  F1 forward {asof} 실패(무시): {str(e)[:80]}")
    r = check_f1_ledger_today()
    status = "OK 찍힘" if r["ok"] else f"⚠경보: {r['reason']}"
    print(f"F1 forward 충전 {total}건 | preflight {status} "
          f"(records={r['records']} foreign={r['with_foreign']} selected_at={r['selected_at']})")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:  # noqa: BLE001 — 최상위 가드
        print(f"F1 nightly 예외(무시): {str(e)[:150]}")
        sys.exit(0)
