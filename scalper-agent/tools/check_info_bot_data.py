# -*- coding: utf-8 -*-
"""정보봇 daily_limit_up_history 활용 — 5일선 회귀 단타 진입 후보 스캐너.

가이드 §10 즉시 활용 예시 구현.
"내년 매매할 종목 미리 발굴" 패턴 (차트영웅) + 5일선 회귀 진입 자동화.

흐름:
  1) 정보봇 daily_limit_up_history (상한가/급등 기록) 조회
  2) 상한가 깬 종목(is_active=false) + 깬 지 1-3일 + 5일선 ±5% 회귀
  3) 거래량 비율 내림차순 TOP 5

조건 해설:
  - is_active=false: 상한가에서 빠져나옴 (가격이 떨어짐)
  - days_since_break BETWEEN 1 AND 3: 빠져나온 지 1-3일 (너무 오래되면 패턴 깨짐)
  - ABS(ma5_distance_pct) <= 5: 현재가가 5일선 ±5% 이내 (눌림목 진입 타점)
  - vol_ratio DESC: 거래량 살아있는 순

사용:
  python tools/check_info_bot_data.py
  python tools/check_info_bot_data.py --top 10 --max-distance 7
"""
import sys
import io
import argparse
import logging
from datetime import datetime
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from utils.supabase_sql import query, query_one, ping

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger("InfoBotScan")


def find_ma5_pullback_candidates(
    top_n: int = 5,
    max_ma5_distance: float = 5.0,
    days_since_break_min: int = 1,
    days_since_break_max: int = 3,
):
    """5일선 회귀 진입 후보 검색.

    Returns:
        list of dict: 진입 후보 (점수 내림차순)
    """
    sql = """
        SELECT
            ticker,
            name,
            date,
            ma5_distance_pct,
            vol_ratio,
            days_since_break,
            is_active
        FROM daily_limit_up_history
        WHERE date = (SELECT MAX(date) FROM daily_limit_up_history)
          AND is_active = false
          AND days_since_break BETWEEN %s AND %s
          AND ABS(ma5_distance_pct) <= %s
        ORDER BY vol_ratio DESC
        LIMIT %s
    """
    return query(sql, (days_since_break_min, days_since_break_max, max_ma5_distance, top_n))


def get_recent_signals_summary():
    """정보봇 daily_limit_up_history 메타 정보."""
    return query_one("""
        SELECT
            MAX(date) AS last_date,
            COUNT(*) FILTER (WHERE date >= CURRENT_DATE - INTERVAL '7 days') AS week_total,
            COUNT(*) FILTER (WHERE date = (SELECT MAX(date) FROM daily_limit_up_history)) AS today_total,
            COUNT(*) FILTER (WHERE is_active = true AND date = (SELECT MAX(date) FROM daily_limit_up_history)) AS today_active
        FROM daily_limit_up_history
    """)


def main():
    parser = argparse.ArgumentParser(description="정보봇 데이터 활용 5일선 회귀 스캐너")
    parser.add_argument("--top", type=int, default=5, help="상위 N개 (기본 5)")
    parser.add_argument("--max-distance", type=float, default=5.0,
                        help="5일선 ±% 임계 (기본 5.0)")
    parser.add_argument("--days-min", type=int, default=1, help="상한가 깬 후 최소일 (기본 1)")
    parser.add_argument("--days-max", type=int, default=3, help="상한가 깬 후 최대일 (기본 3)")
    args = parser.parse_args()

    started = datetime.now()
    print(f"\n{'='*70}")
    print(f"  📊 정보봇 데이터 활용 — 5일선 회귀 단타 진입 후보")
    print(f"  {started.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*70}\n")

    # 1) 연결 확인
    print("  → Supabase 연결 확인...")
    health = ping()
    if not health["ok"]:
        print(f"  ✗ 연결 실패: {health['error']}")
        print(f"\n  대처: .env의 DATABASE_URL 확인 (가이드 §2)")
        sys.exit(1)
    print(f"  ✓ {health['version'][:50]}")

    # 2) 정보봇 데이터 상태
    print("\n  → 정보봇 daily_limit_up_history 메타...")
    try:
        summary = get_recent_signals_summary()
        if summary:
            print(f"  - 최신 데이터일: {summary['last_date']}")
            print(f"  - 최근 7일 전체: {summary['week_total']}건")
            print(f"  - 당일 전체: {summary['today_total']}건")
            print(f"  - 당일 active(아직 상한가): {summary['today_active']}건")
    except Exception as e:
        print(f"  ⚠ 메타 조회 실패: {e}")
        print(f"  (정보봇이 테이블 권한 또는 schema 확인 필요)")
        sys.exit(2)

    # 3) 후보 발굴
    print(f"\n  → 5일선 ±{args.max_distance}% 회귀 후보 (상한가 깬 후 {args.days_min}~{args.days_max}일)...")
    try:
        candidates = find_ma5_pullback_candidates(
            top_n=args.top,
            max_ma5_distance=args.max_distance,
            days_since_break_min=args.days_min,
            days_since_break_max=args.days_max,
        )
    except Exception as e:
        print(f"  ✗ 쿼리 실패: {e}")
        sys.exit(3)

    if not candidates:
        print(f"  (조건 충족 후보 없음 — 임계 완화 시도: --max-distance 10)")
        return

    print(f"\n{'='*70}")
    print(f"  🎯 TOP {len(candidates)} — 5일선 회귀 진입 후보")
    print(f"{'='*70}")
    print(f"  {'순위':<4}{'코드':<8}{'종목명':<20}{'5MA거리':>10}{'거래량비':>10}{'깬일':>6}")
    print(f"  {'-'*70}")

    for i, c in enumerate(candidates, 1):
        dist = c.get("ma5_distance_pct") or 0
        vol = c.get("vol_ratio") or 0
        days = c.get("days_since_break") or 0
        print(
            f"  {i:<4}"
            f"{str(c.get('ticker', ''))[:6]:<8}"
            f"{str(c.get('name', ''))[:18]:<20s}"
            f"{dist:>+9.2f}%"
            f"{vol:>9.2f}x"
            f"{days:>6}"
        )

    print(f"\n{'='*70}")
    print(f"  💡 활용: 5일선 ±{args.max_distance}% 이내 진입 → 차트영웅/영상 매매법 자동화")
    print(f"  ⏱  소요: {(datetime.now() - started).total_seconds():.1f}초")


if __name__ == "__main__":
    main()
