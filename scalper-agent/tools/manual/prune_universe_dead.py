# -*- coding: utf-8 -*-
"""universe.json 죽은 종목(상폐·장기정지) 정리 — 운영 CLI (7/17 [F-1]).

배경: universe.json은 6/22 박제 후 갱신 도구가 없어 죽은 종목이 누적(~3.6%).
랜덤 표본 검증(daily_ohlcv)이 이를 밟아 오탐(7/15 종가 ⚠7/10 사례). daily CSV의
마지막 행 날짜가 컷라인보다 오래된 종목만 제거하는 **보수적 prune** 도구.

컷라인 기본 28일: 단기 거래정지(재개 가능·예: 더존비즈온 1~4주 정지)는 유지하고
상폐·장기정지 확정군만 제거. 7/17 실측: 4주+ 65종목(5/26 정지 클러스터 등).

한계(각주 F-13): 신규상장 추가·거래재개 종목 재편입은 KIS 마스터 결합이 필요한
별도 파이프 — 이 도구는 제거만 한다. 제거분은 백업+로그로 영구 보존(복원 가능).

★ 안전: 기본 dry-run(읽기 전용). --apply 시에만 백업 생성 후 원자쓰기.
  매수/매도/SAJANG/picks/주문 0접촉. KRX 무접촉(판정 근거=기존 daily CSV뿐).
★ 실행 위치 = VPS (데이터 메인). 로컬(노트북)에서 --apply하면 다음 VPSSync가
  VPS의 미정리본으로 되돌린다(단방향 미러) → win32 --apply는 코드로 차단.

실행: python tools/manual/prune_universe_dead.py             # dry-run
      python tools/manual/prune_universe_dead.py --apply     # 백업 후 실제 정리
      python tools/manual/prune_universe_dead.py --cutoff-days 60
"""
import argparse
import json
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent.parent  # scalper-agent/
if str(BASE) not in sys.path:
    sys.path.insert(0, str(BASE))

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

DS = BASE / "data_store"
UNI_PATH = DS / "universe.json"
DAILY_DIR = DS / "daily"
PRUNE_LOG = DS / "universe_prune_log.json"


def _last_date(code: str):
    p = DAILY_DIR / f"{code}.csv"
    if not p.exists():
        return None
    try:
        lines = p.read_text(encoding="utf-8").strip().splitlines()
        return lines[-1].split(",")[0][:10] if len(lines) > 1 else None
    except Exception:  # noqa: BLE001
        return None


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="백업 후 실제 정리 (기본 dry-run)")
    ap.add_argument("--cutoff-days", type=int, default=28,
                    help="전체 최신 거래일 기준 stale 허용 일수 (기본 28 — 단기정지 유지)")
    args = ap.parse_args()

    if args.apply and sys.platform == "win32":
        print("[prune] 로컬(노트북)은 미러 — --apply는 VPS에서만 (VPSSync가 되돌림). 중단")
        return 1

    uni = json.loads(UNI_PATH.read_text(encoding="utf-8"))
    lasts = {c: _last_date(c) for c in uni}
    latest = max((d for d in lasts.values() if d), default=None)
    if not latest:
        print("[prune] daily 데이터 없음 — 중단")
        return 1
    cutoff = (date.fromisoformat(latest) - timedelta(days=args.cutoff_days)).isoformat()
    dead = sorted(c for c, d in lasts.items() if d is None or d < cutoff)

    print(f"[prune] 유니버스 {len(uni)} / 최신 거래일 {latest} / 컷라인 {cutoff} "
          f"(cutoff-days={args.cutoff_days})")
    print(f"[prune] 제거 대상 {len(dead)}종목 → 잔존 {len(uni) - len(dead)}종목")
    for c in dead:
        print(f"  {c} {lasts[c] or '(daily 없음)'} {uni[c].get('name', '')}")

    if not args.apply:
        print("[prune] dry-run — 변경 없음 (--apply로 실행)")
        return 0

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")   # 초 단위 — 같은 날 재실행이 이전 백업을 덮지 않게
    backup = DS / f"universe_backup_{stamp}.json"
    backup.write_text(json.dumps(uni, ensure_ascii=False, indent=2), encoding="utf-8")

    dead_set = set(dead)
    pruned = {c: v for c, v in uni.items() if c not in dead_set}
    tmp = UNI_PATH.with_suffix(".tmp")
    # indent=2 = 빌더와 동일 — git 추적 파일이라 reindent 노이즈 없이 제거 라인만 diff
    tmp.write_text(json.dumps(pruned, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(UNI_PATH)

    # 제거 이력 append (복원·감사용) — 본 작업(원자쓰기) 성공 후이므로 로그 실패가 크래시로 보이면 안 됨
    try:
        try:
            log = json.loads(PRUNE_LOG.read_text(encoding="utf-8"))
            if not isinstance(log, list):
                log = []
        except Exception:  # noqa: BLE001
            log = []
        log.append({"date": stamp, "cutoff": cutoff, "removed": {c: {"last": lasts[c],
                    "name": uni[c].get("name", "")} for c in dead},
                    "backup": backup.name, "before": len(uni), "after": len(pruned)})
        PRUNE_LOG.write_text(json.dumps(log, ensure_ascii=False, indent=1), encoding="utf-8")
    except Exception as e:  # noqa: BLE001
        print(f"[prune] ⚠ 감사 로그 기록 실패(정리 자체는 완료·백업 존재): {e}")

    print(f"[prune] ✅ 적용 완료 — {len(uni)}→{len(pruned)} / 백업 {backup.name} / "
          f"로그 {PRUNE_LOG.name}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
