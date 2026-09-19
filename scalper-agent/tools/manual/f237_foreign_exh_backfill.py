# -*- coding: utf-8 -*-
"""[F-237] 외국인소진율 결손 소급 백필 — 일회성 수동 도구 (9/19).

[사고] 네이버가 `finance.naver.com/item/frgn.naver` 를 폐지(302)해 2026-09-11~09-18
**6영업일** 동안 2,531종 전건 수집 실패. 원본 계수로 해당 날짜 행 0건 확인.

[복구 원리] 신 원천(`m.stock.naver.com/api/stock/{code}/trend`)은 한 번에 **10거래일**을
돌려준다. 즉 평시 수집 경로를 그대로 한 번 돌리면 결손 6일이 함께 채워진다 —
별도 백필 로직이 필요 없고, 새로 만들지 않는 편이 안전하다(경로가 둘이면 둘 다 틀릴 수 있다).

[이 도구가 따로 있는 이유] 정규 러너 `tools/run_foreign_exh_late_recollect.py` 는
`is_trading_day()` 가드가 있어 **휴장일에는 skip** 한다. 사고 복구를 다음 거래일까지
미루지 않기 위해 같은 함수를 가드 없이 한 번 호출한다.
★가드를 제거하는 게 아니라 **우회하지 않는 별도 입구**를 만드는 것 — 정규 러너는 불변.

★ record-only: 매수/매도/picks/SAJANG/order 0 접촉. 봇 OFF·실주문 0. KRX 무접촉.

실행:
    python3.11 -X utf8 tools/manual/f237_foreign_exh_backfill.py --dry-run
    python3.11 -X utf8 tools/manual/f237_foreign_exh_backfill.py --apply
"""
import argparse
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent.parent
if str(BASE) not in sys.path:
    sys.path.insert(0, str(BASE))
try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

KST = timezone(timedelta(hours=9))
# 결손일 — 9/19 원본 계수로 확정(해당 날짜 행 0건)
MISSING = ["2026-09-11", "2026-09-14", "2026-09-15", "2026-09-16", "2026-09-17", "2026-09-18"]


def _count(flow_dir: Path, dates) -> dict:
    """결손일별 실제 보유 행 수를 **원본 파일에서 직접** 센다(검증기 라벨 불신)."""
    want = set(dates)
    out = {d: 0 for d in dates}
    for f in flow_dir.glob("*_foreign_exh.csv"):
        try:
            lines = f.read_text(encoding="utf-8", errors="ignore").splitlines()
        except Exception:
            continue
        for ln in lines[-30:]:
            d = ln.split(",", 1)[0].strip()
            if d in want:
                out[d] += 1
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="실제 수집 실행")
    ap.add_argument("--dry-run", action="store_true", help="현재 결손 상태만 보고")
    ap.add_argument("--limit", type=int, default=0, help="상위 N종목만(리허설용)")
    args = ap.parse_args()
    if not args.apply and not args.dry_run:
        ap.error("--apply 또는 --dry-run 중 하나를 지정하십시오")

    from collect_all import get_universe_codes
    from data.flow_collector import collect_foreign_exhaustion, FLOW_DIR

    now = datetime.now(KST)
    print(f"[F-237] 백필 — 지금 {now:%Y-%m-%d(%a) %H:%M:%S} KST")
    print(f"        오늘 거래일 여부와 무관하게 1회 수집합니다(사고 복구).")

    before = _count(FLOW_DIR, MISSING)
    print("\n[수집 전] 결손일별 행 수")
    for d in MISSING:
        print(f"  {d}  {before[d]:6d}")

    if args.dry_run:
        print("\n--dry-run — 수집하지 않고 종료")
        return 0

    codes = get_universe_codes()
    if args.limit:
        codes = codes[:args.limit]
    print(f"\n[수집] 유니버스 {len(codes)}종 — 평시와 **같은 함수**로 1회 실행")
    collect_foreign_exhaustion(codes)

    after = _count(FLOW_DIR, MISSING)
    print("\n[수집 후] 결손일별 행 수 (증분)")
    ok = True
    for d in MISSING:
        delta = after[d] - before[d]
        mark = "✅" if after[d] > 0 else "🚨"
        if after[d] == 0:
            ok = False
        print(f"  {mark} {d}  {before[d]:6d} → {after[d]:6d}  ({delta:+d})")

    print("\n" + ("✅ 결손 6일 전부 적재됨" if ok else "🚨 아직 0건인 날짜가 있습니다 — 원인 확인 필요"))
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
