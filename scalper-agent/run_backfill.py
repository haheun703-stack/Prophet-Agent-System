# -*- coding: utf-8 -*-
"""
전종목 백필 스크립트
===================
1단계: 유니버스 빌드 (시총 3000억+ 우선주 포함)
2단계: pykrx 데이터 수집 (일봉 + 수급 + 공매도, 11개월)
3단계: 1D~4D 시그널 백필 (25년 3월 ~ 현재)

실행:
  cd scalper-agent
  python run_backfill.py
  python run_backfill.py --signal-only   # 데이터 이미 있으면 시그널만
  python run_backfill.py --stock 005930  # 개별 종목 테스트
"""
import sys
import io
import os
import logging
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
os.chdir(str(Path(__file__).resolve().parent))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(message)s",
)
logger = logging.getLogger("Backfill")


def main():
    import argparse
    parser = argparse.ArgumentParser(description="전종목 백필")
    parser.add_argument("--signal-only", action="store_true", help="시그널 백필만 (데이터 수집 건너뜀)")
    parser.add_argument("--stock", type=str, help="개별 종목 테스트")
    parser.add_argument("--start", type=str, default="20250301", help="백필 시작일")
    parser.add_argument("--end", type=str, default=datetime.now().strftime("%Y%m%d"), help="백필 종료일")
    parser.add_argument("--months", type=int, default=24, help="pykrx 수집 기간 (월)")
    parser.add_argument("--min-cap", type=int, default=3000, help="최소 시총 (억원)")
    args = parser.parse_args()

    # ── 개별 종목 테스트 ──
    if args.stock:
        from data.signal_analyzer import SignalAnalyzer
        from data.universe_builder import load_universe

        uni = load_universe()
        name = uni.get(args.stock, {}).get("name", args.stock)

        sa = SignalAnalyzer()
        rec = sa.analyze_stock(args.stock, name=name)
        if rec:
            print(sa.format_report(rec))
        else:
            print(f"{args.stock}: 데이터 부족 (일봉 60일 이상 필요)")
            print("먼저 데이터 수집: python run_backfill.py")
        return

    # ── 1단계: 유니버스 빌드 ──
    if not args.signal_only:
        print("=" * 60)
        print("  [1/3] 유니버스 빌드")
        print("=" * 60)
        from data.universe_builder import build_universe
        universe = build_universe(min_cap_억=args.min_cap)
        codes = list(universe.keys())
        print(f"  유니버스: {len(codes)}종목\n")

        # ── 2단계: pykrx 데이터 수집 ──
        print("=" * 60)
        print(f"  [2/3] pykrx 데이터 수집 ({args.months}개월)")
        print("=" * 60)
        from data.universe_builder import collect_daily_pykrx
        from data.flow_collector import (
            collect_investor_flow,
            collect_foreign_exhaustion,
            collect_short_balance,
            collect_short_volume,
        )

        print(f"\n  [2-1] 일봉 OHLCV ({len(codes)}종목)...")
        collect_daily_pykrx(codes, months=args.months, force=False)

        print(f"\n  [2-2] 투자자별 순매수...")
        collect_investor_flow(codes, months=args.months, force=False)

        print(f"\n  [2-3] 외국인 소진율...")
        collect_foreign_exhaustion(codes, months=args.months, force=False)

        print(f"\n  [2-4] 공매도 잔고...")
        collect_short_balance(codes, months=args.months, force=False)

        print(f"\n  [2-5] 공매도 거래량...")
        collect_short_volume(codes, months=args.months, force=False)

        print(f"\n  데이터 수집 완료\n")
    else:
        from data.universe_builder import load_universe, get_universe_dict
        uni = load_universe()
        if not uni:
            print("유니버스 없음. --signal-only 없이 실행하세요.")
            return
        codes = list(uni.keys())

    # ── 3단계: 시그널 백필 ──
    print("=" * 60)
    print(f"  [3/3] 1D~4D 시그널 백필 ({args.start} ~ {args.end})")
    print("=" * 60)

    from data.signal_analyzer import SignalAnalyzer
    from data.universe_builder import load_universe

    uni = load_universe()
    exclude = {"069500", "371160", "102780", "305720"}
    codes = [c for c in uni.keys() if c not in exclude]
    names = {c: uni[c].get("name", c) for c in codes}

    sa = SignalAnalyzer()
    results = sa.backfill(codes, args.start, args.end, names=names)

    # ── 완료 요약 ──
    print("\n" + "=" * 60)
    print("  백필 완료 요약")
    print("=" * 60)
    total_records = sum(results.values())
    print(f"  종목 수: {len(codes)}")
    print(f"  거래일 수: {len(results)}")
    print(f"  총 레코드: {total_records:,}")
    if results:
        dates = sorted(results.keys())
        print(f"  기간: {dates[0]} ~ {dates[-1]}")

    # 최신 요약 출력
    print("\n" + sa.format_daily_summary())
    print("=" * 60)


if __name__ == "__main__":
    main()
