# -*- coding: utf-8 -*-
"""★ 5/24 사장님 영구 룰 검증 ★ 5대 미흡 영역 + 13-A/B/C/D/E 종합 백테스트.

사장님 영구 룰 [[feedback_verify_external_knowledge]]:
> "남의 말만 듣지 말고 우리가 검증을 해봐야된다"

사장님 질문 (5/24 (일) 19:00):
> "1일 20% 수익률을 가져갈수 있겠니?"

목적:
    단타봇이 제안한 5대 미흡 영역이 실제로 1일 +20% 수익률에 기여하는지 검증.

전략 비교 (일봉 기반 시뮬레이션 — 정밀 5분봉은 추후):
    Baseline:  09:15 시초가 매수 → 종가 매도 (단순)
    A: + 13-A/B/C/D/E 트레일링 (-10% 동적 폭)
    B: + 08:30 갭상승 필터 (시초가/전일종가 >= 1.05)
    C: + 부분 익절 (+5% 50% 익절 + 나머지 트레일링)
    종합: A + B + C

표본:
    daily_limit_up_history 6개월 (상한가 2회+ active 종목) — 사장님 5/22 명령 표본

데이터:
    KIS API fetch_daily_chart 일봉 OHLCV
"""
import argparse
import json
import logging
import statistics
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))

from dotenv import load_dotenv
load_dotenv(_ROOT.parent / ".env")

logger = logging.getLogger("BH.Backtest5Strats")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s: %(message)s")


# ── 표본 수집 (backtest_elliott_fib의 active_limit_up 재사용) ──
def collect_sample(limit: int = 50) -> List[Dict]:
    """6개월 내 상한가 2회+ active 종목."""
    try:
        from utils.supabase_sql import query
        sql = """
            SELECT ticker, name,
                   COUNT(*) AS n_active,
                   MAX(date) AS latest_date
            FROM daily_limit_up_history
            WHERE date >= CURRENT_DATE - INTERVAL '180 days'
            GROUP BY ticker, name
            HAVING COUNT(*) >= 2
            ORDER BY COUNT(*) DESC, MAX(date) DESC
            LIMIT %s
        """
        rows = query(sql, (limit,))
        return [{"code": r["ticker"], "name": r["name"],
                 "n_active": r["n_active"], "latest_date": str(r["latest_date"])}
                for r in (rows or [])]
    except Exception as e:
        logger.error(f"표본 수집 실패: {e}")
        return []


def fetch_daily_bars(trader, code: str) -> List[Dict]:
    """KIS 일봉 200영업일."""
    try:
        bars = trader.fetch_daily_chart(code)
        return bars or []
    except Exception as e:
        logger.warning(f"{code} 일봉 실패: {e}")
        return []


# ═════════════════════════════════════════════
# 전략 시뮬레이션 (일봉 기반)
# ═════════════════════════════════════════════
def simulate_strategies(bars: List[Dict], surge_pct_threshold: float = 10.0) -> List[Dict]:
    """각 일봉에 대해 5대 전략 시뮬레이션.

    조건: 해당 일이 +threshold%+ 상승 (단타봇 매수 대상)
    매수 시점: 그 날 시초가 또는 그 전날 종가
    매도 시점: 그 날 종가 또는 다음날

    Returns:
        각 일별 시뮬레이션 결과 리스트
    """
    results = []
    for i in range(1, len(bars) - 1):
        prev = bars[i - 1]
        today = bars[i]

        prev_close = prev.get("close", 0)
        op = today.get("open", 0)
        hi = today.get("high", 0)
        lo = today.get("low", 0)
        cl = today.get("close", 0)
        vol = today.get("volume", 0)

        if not all([prev_close, op, hi, lo, cl]):
            continue

        # 단타 대상 필터: 시초가 또는 종가가 +threshold%+ 상승
        change_pct_open_to_close = (cl / op - 1) * 100
        change_pct_prev_to_high = (hi / prev_close - 1) * 100

        # 큰 변동성 일만 표본 (단타 대상 가정)
        if change_pct_prev_to_high < surge_pct_threshold:
            continue

        # ── 전략별 매수가 / 매도가 ──

        # Baseline: 09:15 시초가 매수 → 종가 매도 (단순)
        baseline_buy = op
        baseline_sell = cl
        baseline_pnl = (baseline_sell / baseline_buy - 1) * 100

        # A: 13-A/B/C/D/E 트레일링 (시초가 매수 → max(고가-10%, 종가) 매도)
        # 강한 추세 +20%+ 도달 시 -10%, 그 외 -5% 가정 (보수적 평균)
        hi_pct = (hi / op - 1) * 100
        if hi_pct >= 20:
            trail_pct = 0.10
        elif hi_pct >= 10:
            trail_pct = 0.07
        elif hi_pct >= 5:
            trail_pct = 0.05
        else:
            trail_pct = 0.03
        a_sell = max(int(hi * (1 - trail_pct)), cl)
        a_pnl = (a_sell / op - 1) * 100

        # B: + 08:30 갭상승 필터 (시초가/전일종가 >= 1.05 시만 진입)
        gap_pct = (op / prev_close - 1) * 100
        b_eligible = gap_pct >= 5.0
        b_pnl = a_pnl if b_eligible else 0  # 진입 안 하면 0

        # C: 부분 익절 + 트레일링 (+5% 50% + 나머지 트레일링)
        if hi_pct >= 5.0:
            # 50% +5%에 익절 + 50% 트레일링
            c_partial_pnl = 0.5 * 5.0
            c_trail_pnl = 0.5 * a_pnl
            c_pnl = c_partial_pnl + c_trail_pnl
        else:
            c_pnl = a_pnl   # +5% 미달 → 전량 a 동일

        # 종합: A + B 필터 + C 부분 익절
        if b_eligible:
            if hi_pct >= 5.0:
                combined_pnl = 0.5 * 5.0 + 0.5 * a_pnl
            else:
                combined_pnl = a_pnl
        else:
            combined_pnl = 0

        results.append({
            "date": today.get("date", ""),
            "prev_close": prev_close,
            "open": op,
            "high": hi,
            "low": lo,
            "close": cl,
            "gap_pct": round(gap_pct, 2),
            "open_to_high_pct": round(hi_pct, 2),
            "open_to_close_pct": round(change_pct_open_to_close, 2),
            "prev_to_high_pct": round(change_pct_prev_to_high, 2),
            "baseline_pnl": round(baseline_pnl, 2),
            "a_trailing_pnl": round(a_pnl, 2),
            "b_gap_filter_pnl": round(b_pnl, 2),
            "b_eligible": b_eligible,
            "c_partial_pnl": round(c_pnl, 2),
            "combined_pnl": round(combined_pnl, 2),
        })
    return results


def aggregate_stats(all_results: List[Dict]) -> Dict:
    """전략별 통계 — 평균 / 중앙값 / 표준편차 / 최대 / 진입률."""
    if not all_results:
        return {}
    strategies = {
        "baseline": [r["baseline_pnl"] for r in all_results],
        "A_trailing": [r["a_trailing_pnl"] for r in all_results],
        "B_gap_filter": [r["b_gap_filter_pnl"] for r in all_results if r["b_eligible"]],
        "C_partial_exit": [r["c_partial_pnl"] for r in all_results],
        "combined": [r["combined_pnl"] for r in all_results if r["b_eligible"]],
    }
    stats = {}
    for name, pnls in strategies.items():
        if not pnls:
            stats[name] = {"n": 0}
            continue
        winning = [p for p in pnls if p > 0]
        stats[name] = {
            "n": len(pnls),
            "mean_pnl_pct": round(statistics.mean(pnls), 2),
            "median_pnl_pct": round(statistics.median(pnls), 2),
            "stdev_pnl_pct": round(statistics.stdev(pnls), 2) if len(pnls) >= 2 else 0,
            "max_pnl_pct": round(max(pnls), 2),
            "min_pnl_pct": round(min(pnls), 2),
            "win_rate_pct": round(len(winning) / len(pnls) * 100, 2),
            "positive_count": len(winning),
        }
    # B 필터 통과율
    b_pass = sum(1 for r in all_results if r["b_eligible"])
    stats["b_filter_pass_rate"] = round(b_pass / len(all_results) * 100, 2)
    stats["total_surge_events"] = len(all_results)
    return stats


def run(limit: int = 30, surge_threshold: float = 10.0, delay: float = 0.3) -> Dict:
    """전체 백테스트 흐름."""
    from bot.kis_trader import KISTrader
    trader = KISTrader()

    logger.info(f"표본 수집 시작 (limit={limit})...")
    samples = collect_sample(limit=limit)
    if not samples:
        logger.error("표본 0건")
        return {}
    logger.info(f"표본 {len(samples)}건 (상한가 2회+ active)")

    all_results = []
    for i, s in enumerate(samples, 1):
        code = s["code"]
        bars = fetch_daily_bars(trader, code)
        if len(bars) < 30:
            logger.warning(f"[{i}/{len(samples)}] {code} {s['name']}: 일봉 부족 ({len(bars)})")
            continue

        sim = simulate_strategies(bars, surge_pct_threshold=surge_threshold)
        for r in sim:
            r["code"] = code
            r["name"] = s["name"]
        all_results.extend(sim)

        if i % 5 == 0:
            logger.info(f"진행 {i}/{len(samples)} — 누적 {len(all_results)}건")
        time.sleep(delay)

    stats = aggregate_stats(all_results)
    return {
        "samples": len(samples),
        "total_surge_events": len(all_results),
        "surge_threshold_pct": surge_threshold,
        "stats": stats,
        "sample_events": all_results[:5],   # 처음 5건 sample
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=30, help="표본 종목 수")
    parser.add_argument("--surge-threshold", type=float, default=10.0,
                        help="단타 대상 필터: 전일종가 대비 고가 상승률 %")
    parser.add_argument("--delay", type=float, default=0.3, help="KIS API 대기 (rate limit)")
    parser.add_argument("--output", type=str,
                        default="data_store/backtest_5_strategies_5_24.json",
                        help="결과 저장 경로")
    args = parser.parse_args()

    result = run(limit=args.limit, surge_threshold=args.surge_threshold, delay=args.delay)
    if not result:
        print("백테스트 실패 — 표본 0건")
        sys.exit(1)

    out_path = _ROOT / args.output
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(result, ensure_ascii=False, indent=2, default=str),
                          encoding="utf-8")
    print(f"\n✅ 백테스트 완료 → {out_path}")
    print(f"표본 {result['samples']}건 / 단타 이벤트 {result['total_surge_events']}건")
    print(f"\n=== 전략별 통계 (단타 대상 +{args.surge_threshold}%+ 일자) ===")
    for name, s in result["stats"].items():
        if isinstance(s, dict) and "mean_pnl_pct" in s:
            print(f"  {name}: 평균 +{s['mean_pnl_pct']}% / 승률 {s['win_rate_pct']}% / "
                  f"n={s['n']} / max +{s['max_pnl_pct']}%")
    print(f"\nB 필터 통과율: {result['stats'].get('b_filter_pass_rate', 0)}%")
