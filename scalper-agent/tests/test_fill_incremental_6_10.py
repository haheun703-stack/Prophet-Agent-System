"""classify_fetch_window 셀프테스트 — 안전망 손상복구 분류 검증 (네트워크 0).

6/10 fill 증분화(안전망+병렬)의 분류기를 격리 검증.
가짜 CSV를 tmp 디렉토리에 주입 → 정상/누락/손상/신규/UPTODATE/역전/중복 각 분류 확인.
asof=2026-06-09(거래일, ref=6/9). 005930 손상 교훈 = CORRUPT 강제 20일복구 입증.
"""
import sys
import tempfile
from pathlib import Path
from datetime import date

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import tools.fill_daily_kis_incremental as M  # noqa: E402
from tools.fill_daily_kis_incremental import (  # noqa: E402
    classify_fetch_window, FULL_DAYS, NORMAL_DAYS,
)

ASOF = date(2026, 6, 9)  # 거래일, ref=6/9 (직전거래일=6/8)
HDR = "날짜,시가,고가,저가,종가,거래량,등락률\n"
ROW = "{d},100,110,90,105,1000,1.5\n"


def _write(d, code, body):
    f = d / f"{code}.csv"
    f.write_text(body, encoding="utf-8")
    return f


def run():
    passed = failed = 0
    fails = []

    def check(name, got_reason, got_days, exp_reason, exp_days):
        nonlocal passed, failed
        ok = (got_reason == exp_reason) and (got_days == exp_days)
        if ok:
            passed += 1
        else:
            failed += 1
            fails.append(f"{name}: got=({got_reason},{got_days}) exp=({exp_reason},{exp_days})")
        flag = "✓" if ok else "✗"
        print(f"  {flag} {name}: ({got_reason}, {got_days}d)")

    with tempfile.TemporaryDirectory() as td:
        d = Path(td)

        # 1. NORMAL — 정상, last_dt=6/8 (gap 1거래일)
        _write(d, "T_NORMAL", HDR + ROW.format(d="2026-06-04") + ROW.format(d="2026-06-05")
               + ROW.format(d="2026-06-08"))
        rd, rs = classify_fetch_window("T_NORMAL", d, ASOF)
        check("NORMAL(gap1)", rs, rd, "NORMAL", NORMAL_DAYS)

        # 2. UPTODATE — last_dt=6/9 (= ref)
        _write(d, "T_UPTODATE", HDR + ROW.format(d="2026-06-05") + ROW.format(d="2026-06-08")
               + ROW.format(d="2026-06-09"))
        rd, rs = classify_fetch_window("T_UPTODATE", d, ASOF)
        check("UPTODATE(=ref)", rs, rd, "UPTODATE", NORMAL_DAYS)

        # 3. STALE — last_dt=5/22 (gap > 5거래일)
        _write(d, "T_STALE", HDR + ROW.format(d="2026-05-20") + ROW.format(d="2026-05-21")
               + ROW.format(d="2026-05-22"))
        rd, rs = classify_fetch_window("T_STALE", d, ASOF)
        check("STALE(gap>5)", rs, rd, "STALE", FULL_DAYS)

        # 4. NEW — 파일 없음
        rd, rs = classify_fetch_window("T_NOFILE", d, ASOF)
        check("NEW(no file)", rs, rd, "NEW", FULL_DAYS)

        # 5. CORRUPT — 헤더만(행0)
        _write(d, "T_EMPTY", HDR)
        rd, rs = classify_fetch_window("T_EMPTY", d, ASOF)
        check("CORRUPT(empty)", rs, rd, "CORRUPT", FULL_DAYS)

        # 6. CORRUPT — 행 1개 (< MIN_ROWS=2)
        _write(d, "T_ONEROW", HDR + ROW.format(d="2026-06-08"))
        rd, rs = classify_fetch_window("T_ONEROW", d, ASOF)
        check("CORRUPT(1row)", rs, rd, "CORRUPT", FULL_DAYS)

        # 7. CORRUPT — 날짜 역전 (내림차순)
        _write(d, "T_REVERSE", HDR + ROW.format(d="2026-06-08") + ROW.format(d="2026-06-05")
               + ROW.format(d="2026-06-04"))
        rd, rs = classify_fetch_window("T_REVERSE", d, ASOF)
        check("CORRUPT(reverse)", rs, rd, "CORRUPT", FULL_DAYS)

        # 8. CORRUPT — 날짜 중복
        _write(d, "T_DUP", HDR + ROW.format(d="2026-06-05") + ROW.format(d="2026-06-08")
               + ROW.format(d="2026-06-08"))
        rd, rs = classify_fetch_window("T_DUP", d, ASOF)
        check("CORRUPT(dup)", rs, rd, "CORRUPT", FULL_DAYS)

        # 9. CORRUPT — 깨진 내용 (읽기 실패 유도)
        _write(d, "T_GARBAGE", "@@@broken\x00\x01not,a,valid\ncsv at all\n")
        rd, rs = classify_fetch_window("T_GARBAGE", d, ASOF)
        # 깨진 csv는 read_csv가 통과할 수도 있으나 날짜 파싱/구조 검증에서 CORRUPT
        ok9 = (rs == "CORRUPT" and rd == FULL_DAYS)
        check("CORRUPT(garbage)", rs, rd, "CORRUPT", FULL_DAYS)

        # 10. CORRUPT — 날짜 파싱 불가(잘못된 날짜 문자열)
        _write(d, "T_BADDATE", HDR + "notadate,100,110,90,105,1000,1.5\n"
               + ROW.format(d="2026-06-08"))
        rd, rs = classify_fetch_window("T_BADDATE", d, ASOF)
        check("CORRUPT(baddate)", rs, rd, "CORRUPT", FULL_DAYS)

    print(f"\n[분류] 결과: {passed} PASS / {failed} FAIL (총 {passed+failed})")
    if fails:
        print("FAIL 상세:")
        for f in fails:
            print(f"  - {f}")
    return failed == 0


# ============================================================
#  M-5 회귀 — merge/keep=first/NaT복구/등락률재계산/verify (broker stub, 네트워크 0)
# ============================================================
class StubBroker:
    """fetch_ohlcv_domestic 모킹 — {code: [kis_row]} 반환."""
    def __init__(self, rows_map):
        self.rows_map = rows_map

    def fetch_ohlcv_domestic(self, symbol, timeframe="D", start_day="", end_day=""):
        return {"output2": self.rows_map.get(symbol, [])}


def _kis_row(d, o, h, lo, c, vol=1000):
    return {"stck_bsop_date": d, "stck_oprc": o, "stck_hgpr": h,
            "stck_lwpr": lo, "stck_clpr": c, "acml_vol": vol}


def run_merge_regression():
    passed = failed = 0
    fails = []

    def ck(name, cond):
        nonlocal passed, failed
        if cond:
            passed += 1
            print(f"  ✓ {name}")
        else:
            failed += 1
            fails.append(name)
            print(f"  ✗ {name}")

    with tempfile.TemporaryDirectory() as td:
        d = Path(td)
        orig_daily = M.DAILY
        M.DAILY = d  # 전역 DAILY를 임시디렉토리로(실 data_store 무접촉)
        try:
            # ── 케이스 A: keep='first' 과거보존 + 신규행 등락률 재계산 ──
            # 기존: 6/4,6/5,6/8(종가200)
            _write(d, "R1", HDR + "2026-06-04,100,110,90,190,1000,1.0\n"
                   + "2026-06-05,100,110,90,195,1000,2.6\n"
                   + "2026-06-08,100,110,90,200,1000,2.5\n")
            # stub: 6/8(종가999 충돌) + 6/9(종가210)
            stub = StubBroker({"R1": [
                _kis_row("20260608", 100, 110, 90, 999),   # keep=first로 무시돼야
                _kis_row("20260609", 200, 215, 195, 210),
            ]})
            r = M._fetch_one(stub, "R1", date(2026, 6, 9), "20260609")
            ck("A1 merge status=ok", r["status"] == "ok")
            df = M._read_daily_indexed(d / "R1.csv")
            ck("A2 keep=first 과거보존(6/8종가=200, 999아님)",
               int(df.loc["2026-06-08", "종가"]) == 200)
            ck("A3 신규 6/9 추가됨", df.index.max().date() == date(2026, 6, 9))
            ck("A4 신규행 등락률 재계산(6/9=(210-200)/200*100=5.0)",
               abs(float(df.loc["2026-06-09", "등락률"]) - 5.0) < 0.01)
            ck("A5 행수 보존+1 (3→4)", len(df) == 4)

            # ── 케이스 B: NaT 손상행 복구 (CORRUPT 종목) ──
            # 기존: 정상 6/5 + 손상(baddate) + 정상 6/8
            _write(d, "R2", HDR + "2026-06-05,100,110,90,150,1000,1.0\n"
                   + "notadate,100,110,90,160,1000,1.0\n"
                   + "2026-06-08,100,110,90,170,1000,1.0\n")
            stub2 = StubBroker({"R2": [
                _kis_row("20260605", 100, 110, 90, 150),
                _kis_row("20260608", 100, 110, 90, 170),
                _kis_row("20260609", 100, 110, 90, 180),
            ]})
            r2 = M._fetch_one(stub2, "R2", date(2026, 6, 9), "20260609", force_full=True)
            ck("B1 복구 status=ok", r2["status"] == "ok")
            df2 = M._read_daily_indexed(d / "R2.csv")
            ck("B2 NaT 손상행 제거됨(인덱스 NaT 0)", not df2.index.isna().any())
            ck("B3 정렬 단조증가", df2.index.is_monotonic_increasing)
            ck("B4 6/9 채워짐", df2.index.max().date() == date(2026, 6, 9))

            # ── 케이스 C: verify_coverage 분류 ──
            # R3 정상(last=6/9=ref) / R4 미래ghost(6/30) / R5 과거(6/5)
            _write(d, "R3", HDR + "2026-06-08,1,1,1,1,1,0\n2026-06-09,1,1,1,1,1,0\n")
            _write(d, "R4", HDR + "2026-06-08,1,1,1,1,1,0\n2026-06-30,1,1,1,1,1,0\n")
            _write(d, "R5", HDR + "2026-06-04,1,1,1,1,1,0\n2026-06-05,1,1,1,1,1,0\n")
            missing, corrupt = M.verify_coverage(["R3", "R4", "R5"], date(2026, 6, 9))
            mset = {c for c, _ in missing}
            cset = {c for c, _ in corrupt}
            ck("C1 R3 정상(missing/corrupt 둘다 아님)", "R3" not in mset and "R3" not in cset)
            ck("C2 R4 미래ghost→corrupt", "R4" in cset)
            ck("C3 R5 과거→missing", "R5" in mset)
        finally:
            M.DAILY = orig_daily

    print(f"\n[회귀] 결과: {passed} PASS / {failed} FAIL (총 {passed+failed})")
    if fails:
        print("FAIL 상세:")
        for f in fails:
            print(f"  - {f}")
    return failed == 0


if __name__ == "__main__":
    print("=== 분류 셀프테스트 ===")
    ok1 = run()
    print("\n=== merge/회귀 셀프테스트 (broker stub) ===")
    ok2 = run_merge_regression()
    sys.exit(0 if (ok1 and ok2) else 1)
