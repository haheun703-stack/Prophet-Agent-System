"""[마감후 실행] fill 증분(7일) vs 전체(20일) 회귀 — 결과 동일성 검증.

★격리★: 실 data_store/daily 무접촉. 샘플 종목을 임시디렉토리 사본으로 복사 후
  거기서 M.DAILY를 패치해 증분 모드와 전체20일 모드를 각각 돌려 비교.

검증: 증분(NORMAL 7일)으로도 전체(20일)와 동일하게 최신일/마지막종가가 채워지는가.
  (keep='first' merge라 과거 동일 전제, 차이는 '신규일을 빠짐없이 받는가')

사용: python tools/fill_regression_6_10.py [--sample 80]
  ★마감 후(일봉 확정) 실행★. 장중엔 미확정봉이라 의미 약함.
"""
import sys
import shutil
import tempfile
import argparse
from pathlib import Path
from datetime import date, datetime

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import tools.fill_daily_kis_incremental as M  # noqa: E402
from tools.fill_daily_kis_incremental import (  # noqa: E402
    _get_broker, _read_daily_indexed, _run_pool,
)


def _snapshot(codes, daily_dir):
    snap = {}
    for c in codes:
        f = Path(daily_dir) / f"{c}.csv"
        if not f.exists():
            snap[c] = None
            continue
        df = _read_daily_indexed(f)
        df = df[~df.index.isna()]
        if not len(df):
            snap[c] = ("EMPTY", None, 0)
            continue
        snap[c] = (df.index.max().date(), float(df.iloc[-1]["종가"]), len(df))
    return snap


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sample", type=int, default=80)
    ap.add_argument("--parallel", type=int, default=4)
    args = ap.parse_args()

    real_daily = M.DAILY
    codes = sorted(
        p.stem for p in real_daily.glob("*.csv") if not p.stem.startswith("_")
    )[:args.sample]
    broker = _get_broker()
    today = datetime.now().strftime("%Y%m%d")
    asof = date.today()
    print(f"회귀 대상 {len(codes)}종목 | asof={asof} | parallel={args.parallel}")

    with tempfile.TemporaryDirectory() as t_inc, tempfile.TemporaryDirectory() as t_full:
        inc_dir, full_dir = Path(t_inc), Path(t_full)
        # 두 사본 모두 6/9 상태(현재 실 daily)로 복사
        for c in codes:
            f = real_daily / f"{c}.csv"
            if f.exists():
                shutil.copy2(f, inc_dir / f"{c}.csv")
                shutil.copy2(f, full_dir / f"{c}.csv")

        try:
            # ── 증분 모드 (사본 inc_dir) ──
            M.DAILY = inc_dir
            _run_pool(codes, broker, asof, today, args.parallel, force_full=False)
            snap_inc = _snapshot(codes, inc_dir)

            # ── 전체20일 모드 (사본 full_dir) ──
            M.DAILY = full_dir
            _run_pool(codes, broker, asof, today, args.parallel, force_full=True)
            snap_full = _snapshot(codes, full_dir)
        finally:
            M.DAILY = real_daily  # 실 daily 경로 복원(무접촉 보장)

    # ── 비교 ──
    diff_last = diff_close = both_none = 0
    samples = []
    for c in codes:
        a, b = snap_inc[c], snap_full[c]
        if a is None and b is None:
            both_none += 1
            continue
        if a is None or b is None:
            diff_last += 1
            samples.append(f"{c}: 증분={a} 전체={b}")
            continue
        if a[0] != b[0]:
            diff_last += 1
            samples.append(f"{c} 최신일: 증분={a[0]} 전체={b[0]}")
        elif a[1] != b[1]:
            diff_close += 1
            samples.append(f"{c} 종가: 증분={a[1]} 전체={b[1]}")

    print(f"\n=== 회귀 결과 ({len(codes)}종목) ===")
    print(f"  둘다 데이터없음(상폐 등): {both_none}")
    print(f"  최신일 불일치: {diff_last}")
    print(f"  종가 불일치:   {diff_close}")
    if samples:
        print("  불일치 샘플:")
        for s in samples[:15]:
            print(f"    - {s}")
    ok = (diff_last == 0 and diff_close == 0)
    print(f"\n  {'PASS — 증분 == 전체20일 (증분으로 누락 없음)' if ok else 'FAIL — 증분이 전체와 다름(누락 위험)'}")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
