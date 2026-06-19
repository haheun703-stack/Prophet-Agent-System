# -*- coding: utf-8 -*-
"""기관 매집 합류 시그널 — 스캔 + 업로드 러너 (KRX-free, 6/19 신설).

설계: data/institution_accum_scan.py (KIS 기관계 기반, 연기금/금투 KRX 박제 대체)
nightly(VPS 18:00) / 수동 호출. 봇 OFF여도 돈다(데이터 잡).

intelligence_pension_scan 테이블에 fresh date로 upsert → 웹봇 health STALE 해소.
★ source='kis_institution_total'(proxy) — 프론트가 "기관계"로 라벨·구분.
★ read-only 스캔(주문/매도 무접촉) + 리포트 테이블 업서트만. 매매경로 0.
스케줄러 보호 — 어떤 예외도 exit 0.

실행: python tools/run_institution_accum_scan.py
"""
import sys
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent
if str(BASE) not in sys.path:
    sys.path.insert(0, str(BASE))
try:
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")
except Exception:
    pass

from data.institution_accum_scan import scan_institution_accum  # noqa: E402
from data.upload_pension_scan import upload_pension_scan  # noqa: E402


def run() -> dict:
    result = scan_institution_accum()
    if not result:
        print("[inst_accum] 스캔 결과 없음 — 업로드 skip")
        return {"ok": False}
    uploaded = upload_pension_scan(result)
    print(
        f"[inst_accum] {result.get('date')} (KRX-free·source={result.get('source')}) "
        f"핵심 {result.get('best_count')} / 대기 {result.get('standby_count')} "
        f"/ 업로드 {'OK' if uploaded else 'FAIL'}"
    )
    return {"ok": uploaded, "date": result.get("date"), "best": result.get("best_count")}


if __name__ == "__main__":
    try:
        run()
    except Exception as e:
        print(f"[inst_accum] 치명 예외(무시): {e}")
    sys.exit(0)
