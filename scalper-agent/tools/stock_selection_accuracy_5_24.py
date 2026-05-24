# -*- coding: utf-8 -*-
"""★ 5/24 사장님 영구 룰 ★ 종목 선정 정확도 측정 (1일 +20% 가능성 진짜 답).

사장님 영구 룰 [[feedback_stock_selection_80_percent]]:
> "종목선정만 잘해도 이미 80%로는 먹고 들어가잖아"

검증 목표:
    포트폴리오 시뮬 결과 +27.88%는 사후 매수 가정 (data leakage).
    진짜 1일 수익률 = +27.88% × 종목 선정 정확도.

분석 A: continuation_score 사전 예측력
    history.json 613 events의 continuation_score 분포 →
    상위 7건 vs 하위 7건의 max_gain_after 비교 → 점수의 변별력

분석 B: 실전 매매 vs 상한가 일치율
    trade_journal (단타봇 실제 매수) vs limit_up_history (그날 상한가)
    매수 종목이 그날 상한가 도달 비율 = 진짜 cover rate

분석 C: asset_pool 17개 소스별 잠재 cover
    history.json sector 분포 vs asset_pool 소스 종목 분포
"""
import argparse
import json
import logging
import statistics
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Dict, List

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))

from dotenv import load_dotenv
load_dotenv(_ROOT.parent / ".env")

logger = logging.getLogger("BH.SelectionAccuracy")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s: %(message)s")

HISTORY_PATH = _ROOT / "data_store" / "limit_up" / "history.json"


def load_events() -> List[Dict]:
    data = json.loads(HISTORY_PATH.read_text(encoding="utf-8"))
    return data.get("events", [])


# ═════════════════════════════════════════════
# 분석 A: continuation_score 사전 예측력
# ═════════════════════════════════════════════
def analyze_continuation_score(events: List[Dict]) -> Dict:
    """continuation_score 분포 + 변별력."""
    # 일별 그룹
    by_date = defaultdict(list)
    for e in events:
        by_date[e["limit_date"]].append(e)

    # 일별 상위 7건 vs 하위 7건 vs 임의 7건의 max_gain_after 비교
    top_gains = []
    bot_gains = []
    random_gains = []

    import random
    rng = random.Random(42)

    for date, day_evts in by_date.items():
        if len(day_evts) < 7:
            continue
        # 상위 7건 (continuation_score 높은 순)
        top = sorted(day_evts, key=lambda x: -x.get("continuation_score", 0))[:7]
        top_avg = statistics.mean(e.get("max_gain_after", 0) or 0 for e in top)
        top_gains.append(top_avg)

        # 하위 7건
        bot = sorted(day_evts, key=lambda x: x.get("continuation_score", 0))[:7]
        bot_avg = statistics.mean(e.get("max_gain_after", 0) or 0 for e in bot)
        bot_gains.append(bot_avg)

        # 임의 7건 (baseline)
        rand = rng.sample(day_evts, 7)
        rand_avg = statistics.mean(e.get("max_gain_after", 0) or 0 for e in rand)
        random_gains.append(rand_avg)

    # 전체 score 분포
    all_scores = [e.get("continuation_score", 0) for e in events]
    all_gains = [e.get("max_gain_after", 0) or 0 for e in events]

    # 점수 구간별 평균 gain
    bins = {"0-30": [], "30-50": [], "50-70": [], "70-90+": []}
    for e in events:
        s = e.get("continuation_score", 0)
        g = e.get("max_gain_after", 0) or 0
        if s < 30:
            bins["0-30"].append(g)
        elif s < 50:
            bins["30-50"].append(g)
        elif s < 70:
            bins["50-70"].append(g)
        else:
            bins["70-90+"].append(g)

    bin_stats = {}
    for label, gains in bins.items():
        if gains:
            bin_stats[label] = {
                "n": len(gains),
                "mean_max_gain_pct": round(statistics.mean(gains), 2),
                "median_max_gain_pct": round(statistics.median(gains), 2),
            }

    return {
        "score_distribution": {
            "min": min(all_scores),
            "max": max(all_scores),
            "mean": round(statistics.mean(all_scores), 2),
            "median": round(statistics.median(all_scores), 2),
            "stdev": round(statistics.stdev(all_scores), 2) if len(all_scores) >= 2 else 0,
        },
        "top7_vs_bottom7_vs_random7": {
            "top7_mean_gain_pct": round(statistics.mean(top_gains), 2) if top_gains else 0,
            "bottom7_mean_gain_pct": round(statistics.mean(bot_gains), 2) if bot_gains else 0,
            "random7_mean_gain_pct": round(statistics.mean(random_gains), 2) if random_gains else 0,
            "top7_advantage_vs_random_pct": round(
                statistics.mean(top_gains) - statistics.mean(random_gains), 2
            ) if top_gains and random_gains else 0,
            "n_days_compared": len(top_gains),
        },
        "score_bin_max_gain": bin_stats,
    }


# ═════════════════════════════════════════════
# 분석 B: 실전 매매 vs 상한가 일치율
# ═════════════════════════════════════════════
def analyze_trade_journal_vs_limit_up(events: List[Dict],
                                         lookback_days: int = 30) -> Dict:
    """실전 매매 (trade_journal) vs 그날 상한가 종목 일치율."""
    # event 종목 set (날짜별)
    surge_by_date = defaultdict(set)
    for e in events:
        surge_by_date[e["limit_date"]].add(e["code"])

    try:
        from utils.supabase_sql import query
        sql = """
            SELECT event_date, stock_ticker, stock_name, source, signal_tags
            FROM scalper_trade_journal
            WHERE event_type = 'buy'
              AND event_date >= CURRENT_DATE - INTERVAL '%s days'
            ORDER BY event_date DESC
        """
        rows = query(sql, (lookback_days,))
    except Exception as e:
        logger.error(f"trade_journal 쿼리 실패: {e}")
        return {"error": str(e)}

    if not rows:
        return {"error": "trade_journal 데이터 0건"}

    buys_by_date = defaultdict(list)
    for r in rows:
        d = str(r["event_date"])
        buys_by_date[d].append({
            "code": r["stock_ticker"],
            "name": r["stock_name"],
            "source": r.get("source"),
        })

    # 각 매수일에 단타봇 매수 종목 중 그날 상한가 도달 비율
    cover_results = []
    for date, buys in sorted(buys_by_date.items()):
        surge_codes = surge_by_date.get(date, set())
        if not surge_codes:
            continue
        buy_codes = set(b["code"] for b in buys)
        cover = buy_codes & surge_codes
        cover_results.append({
            "date": date,
            "n_buys": len(buy_codes),
            "n_surge": len(surge_codes),
            "n_cover": len(cover),
            "cover_rate_pct": round(len(cover) / len(buy_codes) * 100, 2) if buy_codes else 0,
            "cover_codes": list(cover),
        })

    if not cover_results:
        return {"info": "겹치는 일자 0건 (단타봇 매수 vs 상한가)",
                 "lookback_days": lookback_days,
                 "buy_dates": sorted(buys_by_date.keys()),
                 "surge_dates_in_range": [d for d in surge_by_date if d in buys_by_date]}

    total_buys = sum(r["n_buys"] for r in cover_results)
    total_cover = sum(r["n_cover"] for r in cover_results)

    return {
        "lookback_days": lookback_days,
        "n_buy_dates": len(buys_by_date),
        "n_overlap_dates": len(cover_results),
        "total_buys": total_buys,
        "total_cover": total_cover,
        "overall_cover_rate_pct": round(total_cover / total_buys * 100, 2) if total_buys else 0,
        "per_date": cover_results,
    }


# ═════════════════════════════════════════════
# 분석 C: 섹터 분포 + asset_pool 소스 잠재 cover
# ═════════════════════════════════════════════
def analyze_sector_distribution(events: List[Dict]) -> Dict:
    """상한가 종목의 섹터 분포 — 단타봇이 어떤 섹터에 강해야 하나."""
    sector_count = Counter()
    sector_gain = defaultdict(list)
    for e in events:
        sec = e.get("sector") or "기타"
        sector_count[sec] += 1
        sector_gain[sec].append(e.get("max_gain_after", 0) or 0)

    top_sectors = []
    for sec, cnt in sector_count.most_common(15):
        gains = sector_gain[sec]
        top_sectors.append({
            "sector": sec,
            "n_surge_events": cnt,
            "share_pct": round(cnt / len(events) * 100, 2),
            "mean_max_gain_pct": round(statistics.mean(gains), 2) if gains else 0,
        })
    return {
        "total_unique_sectors": len(sector_count),
        "top_15_sectors_by_surge_count": top_sectors,
    }


# ═════════════════════════════════════════════
# 종합 + 실전 추정
# ═════════════════════════════════════════════
def compute_realistic_estimates(theory_max_pct: float,
                                  selection_accuracy_estimates: List[float]) -> Dict:
    """이론 최대 × 추정 정확도 → 실전 추정 수익률."""
    estimates = {}
    for acc in selection_accuracy_estimates:
        estimates[f"acc_{int(acc*100)}%"] = round(theory_max_pct * acc, 2)
    return {
        "theory_max_pct": theory_max_pct,
        "accuracy_to_pnl_mapping": estimates,
        "70eok_target_daily_pct": 2.8,
        "required_accuracy_for_70eok_pct": round(2.8 / theory_max_pct * 100, 2)
            if theory_max_pct > 0 else 0,
        "target_20pct_required_accuracy_pct": round(20 / theory_max_pct * 100, 2)
            if theory_max_pct > 0 else 0,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--lookback", type=int, default=30,
                        help="trade_journal 분석 영업일")
    parser.add_argument("--output",
                        default="data_store/selection_accuracy_5_24.json")
    args = parser.parse_args()

    events = load_events()
    logger.info(f"events {len(events)}건 로드")

    print("\n" + "=" * 70)
    print("★ 종목 선정 정확도 측정 ★")
    print("=" * 70)

    # A: continuation_score 변별력
    print("\n[A] continuation_score 사전 예측력 분석")
    a = analyze_continuation_score(events)
    print(f"  점수 분포: min={a['score_distribution']['min']} / "
          f"mean={a['score_distribution']['mean']} / "
          f"max={a['score_distribution']['max']} / "
          f"stdev={a['score_distribution']['stdev']}")
    t = a["top7_vs_bottom7_vs_random7"]
    print(f"  TOP 7 평균 max_gain: +{t['top7_mean_gain_pct']}%")
    print(f"  BOTTOM 7 평균: +{t['bottom7_mean_gain_pct']}%")
    print(f"  RANDOM 7 평균: +{t['random7_mean_gain_pct']}%")
    print(f"  TOP vs RANDOM 우세: +{t['top7_advantage_vs_random_pct']}%p")
    print(f"  점수 구간별 평균 max_gain:")
    for bin_label, stats in a["score_bin_max_gain"].items():
        print(f"    {bin_label}: n={stats['n']} / "
              f"평균 +{stats['mean_max_gain_pct']}% / "
              f"median +{stats['median_max_gain_pct']}%")

    # B: 실전 매매 일치율
    print(f"\n[B] 실전 매매 (trade_journal {args.lookback}일) vs 상한가 일치율")
    b = analyze_trade_journal_vs_limit_up(events, lookback_days=args.lookback)
    if "error" in b:
        print(f"  ⚠ {b['error']}")
    elif "info" in b:
        print(f"  ⚠ {b['info']}")
        print(f"  단타봇 매수 일자: {b.get('buy_dates', [])[-5:]}")
    else:
        print(f"  매수 일자: {b['n_buy_dates']} / 겹치는 일자: {b['n_overlap_dates']}")
        print(f"  총 매수 종목: {b['total_buys']} / 그 중 상한가 도달: {b['total_cover']}")
        print(f"  ★ 종합 cover rate: {b['overall_cover_rate_pct']}% ★")
        print(f"  일별 상세 (최근 5일):")
        for r in b["per_date"][-5:]:
            print(f"    {r['date']}: 매수 {r['n_buys']} / 상한가 {r['n_surge']} / "
                  f"cover {r['n_cover']} ({r['cover_rate_pct']}%)")

    # C: 섹터 분포
    print(f"\n[C] 상한가 종목 섹터 분포 (TOP 10)")
    c = analyze_sector_distribution(events)
    for s in c["top_15_sectors_by_surge_count"][:10]:
        print(f"  {s['sector']:12s} : {s['n_surge_events']:3d}건 "
              f"({s['share_pct']:5.2f}%) / 평균 max +{s['mean_max_gain_pct']:.1f}%")

    # 종합 실전 추정
    theory_max = 27.88   # 직전 시뮬 결과
    print(f"\n[종합] 실전 1일 수익률 = 이론 최대 (+{theory_max}%) × 정확도")
    est = compute_realistic_estimates(theory_max, [1.0, 0.8, 0.5, 0.3, 0.1])
    for acc, pnl in est["accuracy_to_pnl_mapping"].items():
        print(f"  {acc}: +{pnl}%")
    print(f"\n  70억 미션 일평균 +2.8% 달성 필요 정확도: {est['required_accuracy_for_70eok_pct']}%")
    print(f"  1일 +20% 달성 필요 정확도: {est['target_20pct_required_accuracy_pct']}%")

    # 저장
    out_path = _ROOT / args.output
    out_path.parent.mkdir(parents=True, exist_ok=True)
    result = {
        "A_continuation_score": a,
        "B_trade_journal_match": b,
        "C_sector_distribution": c,
        "realistic_estimates": est,
    }
    out_path.write_text(json.dumps(result, ensure_ascii=False, indent=2, default=str),
                          encoding="utf-8")
    print(f"\n✅ 결과 저장 → {out_path}")
