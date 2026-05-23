# -*- coding: utf-8 -*-
"""상한가 엔진 ↔ 엘리어트 sweet spot 결합 검증 백테스트 (사장님 5/23 토 직접 명령).

사장님 의도:
  "원래 상한가 엔진이 주력. 1달간 상한가 친 종목 × 시그널/수급/포지션/재상한가
   + 엘리어트 sweet 결합 가능? 데이터로 비교 보고."

검증 목표:
  ① 상한가 종목의 D-1~D-14 시점에서 sweet 36~46% 진입 비율
  ② sweet 진입 → 상한가 평균 일수 (사전 포지셔닝 가능성)
  ③ 4파 phase 비율 (정형 엘리어트 패턴 상한가 비율)
  ④ 재상한가 사이클 (같은 종목 N일 후 2차 상한가)
  ⑤ 수급 등급 × sweet 동시 = 최강 룰

결과 시나리오:
  A. 강결합 (sweet 진입 ≥50%) → enrich_with_elliott + surge_likelihood
  B. 부분결합 (20~50%) → cross-validate 보너스
  C. 별개 운영 (<20%) → 두 시스템 분리

영구 메모리:
  - [[project_elliott_sweet_spot_5_23]] sweet 36~46% 발견
  - [[surge_master_20260519]] 상한가 엔진 v3.0
  - [[feedback_verify_external_knowledge]] 데이터 검증
"""
import argparse
import json
import logging
import statistics
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))

from dotenv import load_dotenv
load_dotenv(_ROOT.parent / ".env")

from bot.elliott_wave import (
    _identify_waves,
    evaluate_fib_zone,
    detect_elliott_pattern,
)

logger = logging.getLogger("BH.SurgeElliott")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)

# ── sweet zone (5/23 토 fine-grained 결정) ──
SWEET_LO, SWEET_HI = 36.0, 46.0


def collect_surge_samples(days_back: int = 30, limit: int = 200) -> List[Dict]:
    """최근 N일 상한가 종목 수집 (daily_limit_up_history).

    Returns:
        [{ticker, name, date, vol_ratio, consecutive_days, peak_close, is_active}, ...]
    """
    try:
        from utils.supabase_sql import query
        sql = """
            SELECT ticker, name, date,
                   COALESCE(vol_ratio, 0) AS vol_ratio,
                   COALESCE(consecutive_days, 1) AS consecutive_days,
                   COALESCE(peak_close, 0) AS peak_close,
                   COALESCE(is_active, false) AS is_active
            FROM daily_limit_up_history
            WHERE date >= CURRENT_DATE - INTERVAL '%s days'
            ORDER BY date DESC, vol_ratio DESC
            LIMIT %s
        """
        rows = query(sql, (days_back, limit))
        return [dict(r) for r in (rows or [])]
    except Exception as e:
        logger.error(f"상한가 표본 수집 실패: {e}")
        return []


def find_d_day_index(bars: Sequence[Dict], surge_date: str) -> Optional[int]:
    """bars(시간순) 중 surge_date에 해당하는 인덱스 (없으면 None)."""
    target = str(surge_date)
    for i, b in enumerate(bars):
        bd = str(b.get("date", ""))
        if bd == target or bd.replace("-", "") == target.replace("-", ""):
            return i
    return None


def measure_pre_surge_elliott(bars: Sequence[Dict], d_day_idx: int,
                              lookback_days: Sequence[int] = (1, 3, 5, 7, 10, 14)) -> Dict:
    """D-Day 직전 시점들에서 elliott sweet 진입 여부 측정.

    Args:
        bars: 일봉 시간순 전체
        d_day_idx: 상한가 D-Day 인덱스
        lookback_days: 평가할 D-N 시점 리스트

    Returns:
        {
          "D-1": {fib_pct, zone, phase, in_sweet, w4_idx_offset},
          "D-3": {...},
          ...
          "best_entry": {day_offset, fib_pct, zone}  # sweet 진입한 최근 시점
        }
    """
    result = {}
    best_entry = None

    for n in lookback_days:
        end_idx = d_day_idx - n + 1  # D-N 시점까지 데이터 (D-N도 포함)
        if end_idx < 20:  # 최소 일봉 20개 필요
            result[f"D-{n}"] = {"error": "insufficient_bars"}
            continue

        window = bars[:end_idx]
        try:
            res = detect_elliott_pattern(window)
            fib_pct = res.fib_retracement_pct if res.fib_retracement_pct is not None else 0
            in_sweet = SWEET_LO <= fib_pct < SWEET_HI
            entry = {
                "fib_pct": round(fib_pct, 2),
                "zone": res.fib_zone_name,
                "signal": res.fib_zone_signal,
                "phase": res.phase,
                "in_sweet": in_sweet,
                "buy_signal": res.buy_signal,
                "non_overlap": res.non_overlap_check,
            }
            result[f"D-{n}"] = entry

            # 가장 가까운 sweet 진입 기록
            if in_sweet and best_entry is None:
                best_entry = {"day_offset": n, **entry}
        except Exception as e:
            result[f"D-{n}"] = {"error": str(e)[:50]}

    result["best_entry"] = best_entry
    return result


def find_next_surge(bars: Sequence[Dict], d_day_idx: int,
                    surge_pct: float = 25.0, max_search_days: int = 30) -> Optional[int]:
    """D-Day 이후 다음 상한가(+25%+) 시점 — 재상한가 사이클 측정.

    Returns:
        N (D-Day로부터 N일 후 재상한가) or None (없음)
    """
    if d_day_idx + 1 >= len(bars):
        return None

    end_search = min(d_day_idx + max_search_days + 1, len(bars))
    for offset, b in enumerate(bars[d_day_idx + 1:end_search], start=1):
        prev_close = bars[d_day_idx + offset - 1].get("close", 0)
        cur_high = b.get("high", 0)
        if prev_close > 0 and cur_high > 0:
            change_pct = (cur_high - prev_close) / prev_close * 100
            if change_pct >= surge_pct:
                return offset
    return None


# ── 메인 백테스트 ─────────────────────────────────
def run_correlation_backtest(days_back: int = 30, limit: int = 200,
                             delay: float = 0.3) -> Dict:
    from bot.kis_trader import KISTrader
    trader = KISTrader()

    samples = collect_surge_samples(days_back=days_back, limit=limit)
    if not samples:
        logger.error("상한가 표본 0건")
        return {}

    logger.info(f"상한가 표본 수집: {len(samples)}건 (최근 {days_back}일)")

    results = []
    sweet_entry_counts = defaultdict(int)  # day_offset → count
    sweet_total_by_n = defaultdict(int)    # D-N → 측정 성공 건수
    phase_4_count = 0
    phase_5_count = 0
    next_surge_days = []
    consecutive_surge_distribution = defaultdict(int)

    for i, s in enumerate(samples, 1):
        code = s.get("ticker", s.get("code", ""))
        name = s.get("name", "")
        surge_date = str(s.get("date", ""))

        try:
            bars = trader.fetch_daily_chart(code)
            if not bars or len(bars) < 30:
                logger.debug(f"[{i}/{len(samples)}] {code}: 일봉 부족 ({len(bars or [])}봉)")
                continue

            # D-Day 인덱스 찾기
            d_idx = find_d_day_index(bars, surge_date)
            if d_idx is None or d_idx < 20:
                logger.debug(f"[{i}/{len(samples)}] {code} {surge_date}: D-Day 인덱스 X")
                continue

            # D-1~D-14 sweet 측정
            pre = measure_pre_surge_elliott(bars, d_idx)

            # 4파/5파 phase 비율
            d1 = pre.get("D-1", {})
            if d1.get("phase") == "wave_4_pullback":
                phase_4_count += 1
            elif d1.get("phase") == "wave_5_start":
                phase_5_count += 1

            # sweet 진입 카운트 (D-N별)
            for n in [1, 3, 5, 7, 10, 14]:
                key = f"D-{n}"
                if key in pre and "error" not in pre[key]:
                    sweet_total_by_n[n] += 1
                    if pre[key].get("in_sweet"):
                        sweet_entry_counts[n] += 1

            # 재상한가 사이클
            next_n = find_next_surge(bars, d_idx)
            if next_n is not None:
                next_surge_days.append(next_n)

            consec = s.get("consecutive_days", 1)
            consecutive_surge_distribution[consec] += 1

            results.append({
                "ticker": code,
                "name": name,
                "surge_date": surge_date,
                "consecutive_days": consec,
                "vol_ratio": s.get("vol_ratio", 0),
                "is_active": s.get("is_active", False),
                "pre_surge_elliott": pre,
                "best_entry_offset": pre.get("best_entry", {}).get("day_offset") if pre.get("best_entry") else None,
                "next_surge_days": next_n,
            })

            if i % 10 == 0:
                sw1 = sweet_entry_counts[1] / max(sweet_total_by_n[1], 1) * 100
                logger.info(f"진행 {i}/{len(samples)} — sweet D-1 {sw1:.1f}% / 재상한가 {len(next_surge_days)}건")

            time.sleep(delay)
        except Exception as e:
            logger.error(f"{code} 실패: {e}")

    # 통계
    valid_count = len(results)
    if valid_count == 0:
        return {"error": "유효 측정 0건"}

    sweet_rates = {}
    for n in [1, 3, 5, 7, 10, 14]:
        tot = sweet_total_by_n[n]
        ent = sweet_entry_counts[n]
        sweet_rates[f"D-{n}"] = {
            "measured": tot,
            "sweet_entry": ent,
            "rate_pct": round(ent / tot * 100, 1) if tot else 0,
        }

    # 시나리오 자동 판정 (D-1~D-7 평균 sweet 진입 비율 기준)
    d_1_to_7 = [sweet_rates[f"D-{n}"]["rate_pct"] for n in [1, 3, 5, 7] if sweet_rates[f"D-{n}"]["measured"] > 0]
    avg_sweet_rate = round(statistics.mean(d_1_to_7), 1) if d_1_to_7 else 0

    if avg_sweet_rate >= 50:
        scenario = "A_strong_combine"
    elif avg_sweet_rate >= 20:
        scenario = "B_partial_combine"
    else:
        scenario = "C_separate"

    best_entries = [r["best_entry_offset"] for r in results if r["best_entry_offset"] is not None]

    return {
        "total_samples": len(samples),
        "valid_measurements": valid_count,
        "phase_4_pullback_count": phase_4_count,
        "phase_5_start_count": phase_5_count,
        "phase_4_rate": round(phase_4_count / valid_count * 100, 1),
        "phase_5_rate": round(phase_5_count / valid_count * 100, 1),
        "sweet_entry_rates_by_d_n": sweet_rates,
        "avg_sweet_rate_d1_to_d7": avg_sweet_rate,
        "scenario": scenario,
        "best_entry_offset_stats": {
            "count": len(best_entries),
            "mean": round(statistics.mean(best_entries), 1) if best_entries else 0,
            "median": round(statistics.median(best_entries), 1) if best_entries else 0,
        },
        "next_surge_cycle_stats": {
            "count": len(next_surge_days),
            "mean_days": round(statistics.mean(next_surge_days), 1) if next_surge_days else 0,
            "median_days": round(statistics.median(next_surge_days), 1) if next_surge_days else 0,
            "rate_pct": round(len(next_surge_days) / valid_count * 100, 1),
        },
        "consecutive_surge_distribution": dict(consecutive_surge_distribution),
        "raw_results": results[:30],
    }


def main():
    parser = argparse.ArgumentParser(description="상한가 × elliott sweet 결합 검증")
    parser.add_argument("--days-back", type=int, default=30, help="최근 N일 상한가 (기본 30)")
    parser.add_argument("--limit", type=int, default=200, help="표본 상한 (기본 200)")
    parser.add_argument("--delay", type=float, default=0.3, help="KIS API delay")
    parser.add_argument("--save", action="store_true", help="JSON 저장")
    args = parser.parse_args()

    print(f"\n=== 상한가 ↔ elliott sweet 결합 검증 (최근 {args.days_back}일 / 표본 {args.limit}건) ===\n")
    r = run_correlation_backtest(days_back=args.days_back, limit=args.limit, delay=args.delay)

    if r.get("error"):
        print(f"❌ {r['error']}")
        return 1

    print(f"\n=== 결과 요약 ===")
    print(f"총 표본: {r['total_samples']}건 / 유효 측정: {r['valid_measurements']}건")
    print(f"\n[★ 핵심: D-N 시점에서 sweet 36-46% zone 진입 비율 ★]")
    for d_n, c in r["sweet_entry_rates_by_d_n"].items():
        bar = "█" * int(c["rate_pct"] / 3)
        print(f"  {d_n:<6} 측정 {c['measured']:>3}건 / sweet 진입 {c['sweet_entry']:>3}건 / "
              f"{c['rate_pct']:>5}% {bar}")

    print(f"\n[★★★ 시나리오 자동 판정 ★★★]")
    print(f"  D-1~D-7 평균 sweet 진입 비율: {r['avg_sweet_rate_d1_to_d7']}%")
    s = r["scenario"]
    if s == "A_strong_combine":
        print(f"  → ★★★ 시나리오 A 강결합 (≥50%) ★★★ — enrich_with_elliott + surge_likelihood 즉시 적용 가능")
    elif s == "B_partial_combine":
        print(f"  → ★★ 시나리오 B 부분결합 (20~50%) ★★ — cross-validate 보너스 룰 추가")
    else:
        print(f"  → ★ 시나리오 C 별개 운영 (<20%) ★ — 두 시스템 분리 유지")

    print(f"\n[4파/5파 phase 비율 (D-1 시점)]")
    print(f"  wave_4_pullback: {r['phase_4_pullback_count']}건 ({r['phase_4_rate']}%)")
    print(f"  wave_5_start  : {r['phase_5_start_count']}건 ({r['phase_5_rate']}%)")

    print(f"\n[★ Sweet 진입 → 상한가 최적 D-N (몇일 전 진입이 가장 유효한가) ★]")
    be = r["best_entry_offset_stats"]
    print(f"  표본 {be['count']}건 / 평균 D-{be['mean']} / 중앙값 D-{be['median']}")

    print(f"\n[★ 재상한가 사이클 (상한가 후 N일 만에 다시 +25%+) ★]")
    ns = r["next_surge_cycle_stats"]
    print(f"  재상한가 발생: {ns['count']}건 ({ns['rate_pct']}%)")
    print(f"  평균 {ns['mean_days']}일 / 중앙값 {ns['median_days']}일")

    print(f"\n[연속 상한가 분포]")
    for consec, cnt in sorted(r["consecutive_surge_distribution"].items()):
        print(f"  {consec}회차: {cnt}건")

    if args.save:
        out_path = _ROOT / "data_store" / "backtest" / "surge_elliott_correlation.json"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(r, ensure_ascii=False, indent=2, default=str),
                            encoding="utf-8")
        print(f"\n💾 저장: {out_path}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
