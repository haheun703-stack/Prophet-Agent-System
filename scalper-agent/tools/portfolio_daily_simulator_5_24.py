# -*- coding: utf-8 -*-
"""★ 5/24 사장님 1번 검증 ★ 포트폴리오 단위 일별 시뮬레이션.

사장님 질문 (5/24 19:00):
> "1일 +20% 가능성 — 어떻게 검증하는데?"

검증 7단계 중 ① 가장 중요:
    매일 09:00 → asset_pool 재구성 (그날 단타봇이 추천했을 종목 7종)
              → 7종 × 일별 결과 합산 = 포트폴리오 일일 수익률
    6개월 일별 수익률 분포 → 평균 / 표준편차 / +20% 일 비율

데이터 소스:
    data_store/limit_up/history.json — 613 events / 59일 (2026-02-06 ~ 2026-05-07)
    각 event: limit_date, limit_close, pullback_entry, continuation_score, post_days
    KIS 일봉 OHLC — 정밀 트레일링 시뮬용 (보강)

매수 시나리오:
    Mode 1: pullback - D+1 pullback_entry 매수 (단타봇 5/23 commit d597e4e 옵션)
    Mode 2: open - D+1 시초가 매수 (현재 기본 09:15 cron)

매도 시뮬:
    13-A/B/C/D/E 트레일링:
        +3~5%: -3% / +5~10%: -5% / +10~20%: -7% / +20%+: -10%
        limit_up_phase (+25%/+29%): -12%/-15%
    포지션 보유 한도: 최대 5영업일 (D+1 ~ D+5)
"""
import argparse
import json
import logging
import statistics
import sys
import time
from collections import defaultdict, Counter
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))

from dotenv import load_dotenv
load_dotenv(_ROOT.parent / ".env")

logger = logging.getLogger("BH.PortfolioSim")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s: %(message)s")


HISTORY_PATH = _ROOT / "data_store" / "limit_up" / "history.json"


# ═════════════════════════════════════════════
# 트레일링 시뮬 (13-A/B/C/D/E 헬퍼 mirror)
# ═════════════════════════════════════════════
def compute_trail_pct(hwm_pct: float, is_momentum: bool = False) -> float:
    """auto_trader._compute_dynamic_trailing_sl 13-B + 13-E mirror."""
    if hwm_pct >= 29.0:
        return 0.15
    if hwm_pct >= 25.0:
        return 0.12
    if hwm_pct >= 20.0:
        return 0.10
    if hwm_pct >= 10.0:
        return 0.07
    if hwm_pct >= 5.0:
        return 0.05
    return 0.02 if is_momentum else 0.03


def simulate_trailing_with_ohlc(
    entry_price: int,
    post_bars: List[Dict],
    max_hold_days: int = 5,
    sl_pct: float = 0.03,
) -> Dict:
    """OHLC 일봉 기반 정밀 트레일링 시뮬레이션.

    매 일자에 대해:
      1. high 갱신 → hwm 갱신
      2. trail_pct 동적 (hwm_pct 기준)
      3. trail_sl 갱신 (ratchet)
      4. low 가 trail_sl 닿으면 매도 (해당 일 청산)

    Returns:
        {
            "exited": True/False,
            "exit_day": D+N (1-indexed) or None,
            "exit_price": int,
            "exit_reason": "trail"/"sl"/"max_hold",
            "pnl_pct": float,
            "hwm_pct": float,
            "limit_up_phase_reached": bool,
        }
    """
    if not post_bars or entry_price <= 0:
        return {"exited": False, "pnl_pct": 0, "exit_reason": "no_data"}

    hwm = entry_price
    trail_sl = int(entry_price * (1 - sl_pct))   # 초기 -3% SL
    trailing_activated = False
    limit_up_phase = False

    for day_idx, bar in enumerate(post_bars[:max_hold_days], start=1):
        hi = bar.get("high", 0)
        lo = bar.get("low", 0)
        cl = bar.get("close", 0)
        op = bar.get("open", 0)
        if not (hi and lo and cl):
            continue

        # 1. high 갱신
        hwm = max(hwm, hi)
        hwm_pct = (hwm / entry_price - 1) * 100
        if hwm_pct >= 25.0:
            limit_up_phase = True

        # 2. 활성화 임계값 (+3%)
        if hwm_pct >= 3.0:
            trailing_activated = True

        # 3. trail_pct 동적
        if trailing_activated:
            trail_pct = compute_trail_pct(hwm_pct)
            # 13-E ratchet
            if limit_up_phase and trail_pct < 0.10:
                trail_pct = 0.10
            new_trail_sl = int(hwm * (1 - trail_pct))
            # 본전 보장
            if hwm > entry_price * 1.03:
                new_trail_sl = max(new_trail_sl, entry_price)
            trail_sl = max(trail_sl, new_trail_sl)

        # 4. 매도 체크: low <= trail_sl
        if lo <= trail_sl:
            # 매도 가격 = trail_sl (보수적; 실제는 그 사이 어딘가)
            exit_price = trail_sl
            return {
                "exited": True,
                "exit_day": day_idx,
                "exit_price": exit_price,
                "exit_reason": "trail" if trailing_activated else "sl",
                "pnl_pct": round((exit_price / entry_price - 1) * 100, 2),
                "hwm_pct": round(hwm_pct, 2),
                "limit_up_phase_reached": limit_up_phase,
            }

    # max_hold 도달 - 마지막 종가 매도
    last_bar = post_bars[min(max_hold_days, len(post_bars)) - 1]
    exit_price = last_bar.get("close", entry_price)
    return {
        "exited": True,
        "exit_day": min(max_hold_days, len(post_bars)),
        "exit_price": exit_price,
        "exit_reason": "max_hold",
        "pnl_pct": round((exit_price / entry_price - 1) * 100, 2),
        "hwm_pct": round((hwm / entry_price - 1) * 100, 2),
        "limit_up_phase_reached": limit_up_phase,
    }


# ═════════════════════════════════════════════
# KIS 일봉 보강 + bar index 매핑
# ═════════════════════════════════════════════
def fetch_post_bars_ohlc(trader, code: str, limit_date: str,
                          max_days: int = 10) -> List[Dict]:
    """limit_date 다음 영업일부터 max_days 일봉 OHLC 반환.

    Args:
        limit_date: "YYYY-MM-DD" 또는 "YYYYMMDD"
    """
    try:
        bars = trader.fetch_daily_chart(code) or []
    except Exception as e:
        logger.debug(f"{code} 일봉 fetch 실패: {e}")
        return []

    # limit_date 정규화
    if "-" in limit_date:
        ld = limit_date.replace("-", "")
    else:
        ld = limit_date

    # bars는 보통 최신 → 과거 순서. date 키 형식 확인 필요
    # KIS 보통 'date' 필드에 'YYYYMMDD' 형태
    d_idx = None
    for i, b in enumerate(bars):
        bdate = str(b.get("date", "")).replace("-", "")
        if bdate == ld:
            d_idx = i
            break
    if d_idx is None:
        return []

    # bars[d_idx] = D-Day
    # 과거 순 정렬 가정: bars[d_idx-1] = D+1, bars[d_idx-N] = D+N
    # 최신 순 정렬 가정: bars[d_idx+1] = D-1 (반대) → 확인 필요
    # KIS 일반: 최신 = bars[0] 또는 bars[-1]
    if d_idx == 0:
        # 최신이 D-Day → post 데이터 없음
        return []
    if d_idx >= len(bars) - 1:
        # 가장 오래된이 D-Day → post = bars[d_idx-1::-1] 등
        pass

    # 안전: 정렬 순서 명시 확인
    # 첫 두 개 bar 의 date 비교
    if len(bars) >= 2:
        d0 = str(bars[0].get("date", "")).replace("-", "")
        d1 = str(bars[1].get("date", "")).replace("-", "")
        if d0 > d1:
            # 내림차순 (최신→과거)
            post_bars = bars[max(0, d_idx - max_days):d_idx]
            post_bars = list(reversed(post_bars))   # D+1 → D+N 순
        else:
            # 오름차순 (과거→최신)
            post_bars = bars[d_idx + 1:d_idx + 1 + max_days]
    else:
        return []

    # 표준화 — high/low/close/open/volume 키 통일
    out = []
    for b in post_bars:
        out.append({
            "date": b.get("date"),
            "open": int(b.get("open", 0)) if b.get("open") else 0,
            "high": int(b.get("high", 0)) if b.get("high") else 0,
            "low": int(b.get("low", 0)) if b.get("low") else 0,
            "close": int(b.get("close", 0)) if b.get("close") else 0,
            "volume": int(b.get("volume", 0)) if b.get("volume") else 0,
        })
    return out


# ═════════════════════════════════════════════
# 포트폴리오 일별 시뮬레이션
# ═════════════════════════════════════════════
def run_portfolio_simulation(top_k: int = 7, max_hold_days: int = 5,
                              entry_mode: str = "pullback",
                              delay: float = 0.3) -> Dict:
    """전체 포트폴리오 단위 시뮬레이션.

    Args:
        top_k: 매일 매수 종목 수 (단타봇 7종 가정: 09:15 5 + 14:50 2)
        max_hold_days: 종목당 최대 보유 영업일
        entry_mode: "pullback" (D+1 pullback_entry) / "open" (D+1 시초가)
    """
    data = json.loads(HISTORY_PATH.read_text(encoding="utf-8"))
    events = data.get("events", [])
    logger.info(f"events 로드: {len(events)}건")

    # KIS trader
    from bot.kis_trader import KISTrader
    trader = KISTrader()

    # 종목별 일봉 캐시 (중복 호출 방지)
    daily_cache: Dict[str, List[Dict]] = {}
    unique_codes = list(set(e["code"] for e in events))
    logger.info(f"unique 종목: {len(unique_codes)}개 — KIS 일봉 호출 시작")

    for i, code in enumerate(unique_codes, 1):
        try:
            bars = trader.fetch_daily_chart(code) or []
            daily_cache[code] = bars
            if i % 20 == 0:
                logger.info(f"일봉 fetch {i}/{len(unique_codes)}")
            time.sleep(delay)
        except Exception as e:
            logger.warning(f"{code} 일봉 실패: {e}")
            daily_cache[code] = []

    # 각 event 시뮬레이션
    logger.info("event 별 트레일링 시뮬 시작")
    for e in events:
        bars = daily_cache.get(e["code"], [])
        if not bars:
            e["sim_result"] = {"exited": False, "pnl_pct": 0, "exit_reason": "no_bars"}
            continue

        ld = e["limit_date"].replace("-", "")
        # D-Day index
        d_idx = None
        for i, b in enumerate(bars):
            if str(b.get("date", "")).replace("-", "") == ld:
                d_idx = i
                break
        if d_idx is None:
            e["sim_result"] = {"exited": False, "pnl_pct": 0, "exit_reason": "no_d_day"}
            continue

        # post_bars 추출 (정렬 자동 감지)
        if len(bars) < 2:
            e["sim_result"] = {"exited": False, "pnl_pct": 0, "exit_reason": "short_bars"}
            continue
        d0 = str(bars[0].get("date", "")).replace("-", "")
        d1 = str(bars[1].get("date", "")).replace("-", "")
        if d0 > d1:
            post_bars_raw = bars[max(0, d_idx - max_hold_days):d_idx]
            post_bars_raw = list(reversed(post_bars_raw))
        else:
            post_bars_raw = bars[d_idx + 1:d_idx + 1 + max_hold_days]

        post_bars = [{
            "date": b.get("date"),
            "open": int(b.get("open") or 0),
            "high": int(b.get("high") or 0),
            "low": int(b.get("low") or 0),
            "close": int(b.get("close") or 0),
            "volume": int(b.get("volume") or 0),
        } for b in post_bars_raw]

        if not post_bars or not post_bars[0]["open"]:
            e["sim_result"] = {"exited": False, "pnl_pct": 0, "exit_reason": "no_post"}
            continue

        # 매수가 결정
        if entry_mode == "pullback":
            entry = e.get("pullback_entry", post_bars[0]["open"]) or post_bars[0]["open"]
        else:   # open
            entry = post_bars[0]["open"]

        # 시뮬
        sim = simulate_trailing_with_ohlc(entry, post_bars[1:], max_hold_days=max_hold_days)
        sim["entry_price"] = entry
        sim["entry_mode"] = entry_mode
        e["sim_result"] = sim

    # ── 포트폴리오 일별 시뮬 ──
    # 각 limit_date(D)에 해당 종목들이 추천됐다고 가정 → 다음 영업일(D+1) 매수
    events_by_date = defaultdict(list)
    for e in events:
        events_by_date[e["limit_date"]].append(e)

    portfolio_results = []
    for date, day_events in sorted(events_by_date.items()):
        # 단타봇 선정: continuation_score 상위 top_k (정확도 시뮬)
        selected = sorted(day_events,
                           key=lambda x: -x.get("continuation_score", 0))[:top_k]
        actual_pnls = []
        for ev in selected:
            sr = ev.get("sim_result", {})
            if sr.get("exited") and sr.get("pnl_pct") is not None:
                actual_pnls.append(sr["pnl_pct"])

        if not actual_pnls:
            continue
        portfolio_pnl = sum(actual_pnls) / len(actual_pnls)   # 1주씩 균등 가중

        portfolio_results.append({
            "date": date,
            "n_candidates": len(day_events),
            "n_selected": len(selected),
            "n_simulated": len(actual_pnls),
            "individual_pnls": [round(p, 2) for p in actual_pnls],
            "portfolio_pnl_pct": round(portfolio_pnl, 2),
            "best_pick_pnl": round(max(actual_pnls), 2),
            "worst_pick_pnl": round(min(actual_pnls), 2),
        })

    return {
        "config": {
            "top_k": top_k,
            "max_hold_days": max_hold_days,
            "entry_mode": entry_mode,
            "total_events": len(events),
            "unique_codes": len(unique_codes),
        },
        "portfolio_daily": portfolio_results,
    }


# ═════════════════════════════════════════════
# 결과 분석
# ═════════════════════════════════════════════
def analyze_distribution(portfolio_results: List[Dict]) -> Dict:
    """일별 수익률 분포 분석."""
    if not portfolio_results:
        return {}

    pnls = [r["portfolio_pnl_pct"] for r in portfolio_results]
    bests = [r["best_pick_pnl"] for r in portfolio_results]
    worsts = [r["worst_pick_pnl"] for r in portfolio_results]

    # 분포 구간
    plus_20 = sum(1 for p in pnls if p >= 20)
    plus_10 = sum(1 for p in pnls if p >= 10)
    plus_5 = sum(1 for p in pnls if p >= 5)
    plus_2_8 = sum(1 for p in pnls if p >= 2.8)   # 사장님 70억 미션 일평균
    positive = sum(1 for p in pnls if p > 0)
    negative = sum(1 for p in pnls if p < 0)

    n = len(pnls)

    return {
        "n_days": n,
        "stats": {
            "mean_pct": round(statistics.mean(pnls), 2),
            "median_pct": round(statistics.median(pnls), 2),
            "stdev_pct": round(statistics.stdev(pnls), 2) if n >= 2 else 0,
            "min_pct": round(min(pnls), 2),
            "max_pct": round(max(pnls), 2),
        },
        "best_pick_stats": {
            "mean": round(statistics.mean(bests), 2),
            "max": round(max(bests), 2),
        },
        "worst_pick_stats": {
            "mean": round(statistics.mean(worsts), 2),
            "min": round(min(worsts), 2),
        },
        "distribution": {
            "plus_20_days": plus_20,
            "plus_20_rate_pct": round(plus_20 / n * 100, 2),
            "plus_10_days": plus_10,
            "plus_10_rate_pct": round(plus_10 / n * 100, 2),
            "plus_5_days": plus_5,
            "plus_5_rate_pct": round(plus_5 / n * 100, 2),
            "plus_2_8_days": plus_2_8,
            "plus_2_8_rate_pct": round(plus_2_8 / n * 100, 2),   # 사장님 70억 일평균
            "positive_days": positive,
            "win_rate_pct": round(positive / n * 100, 2),
            "negative_days": negative,
        },
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--top-k", type=int, default=7)
    parser.add_argument("--max-hold", type=int, default=5)
    parser.add_argument("--entry-mode", default="pullback",
                        choices=["pullback", "open"])
    parser.add_argument("--delay", type=float, default=0.25)
    parser.add_argument("--output",
                        default="data_store/portfolio_daily_sim_5_24.json")
    args = parser.parse_args()

    result = run_portfolio_simulation(
        top_k=args.top_k, max_hold_days=args.max_hold,
        entry_mode=args.entry_mode, delay=args.delay,
    )
    analysis = analyze_distribution(result.get("portfolio_daily", []))
    result["analysis"] = analysis

    out_path = _ROOT / args.output
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(result, ensure_ascii=False, indent=2, default=str),
                          encoding="utf-8")

    print(f"\n✅ 시뮬 완료 → {out_path}")
    print(f"\n=== Config ===")
    for k, v in result["config"].items():
        print(f"  {k}: {v}")
    print(f"\n=== 분포 분석 (n={analysis.get('n_days', 0)}일) ===")
    s = analysis.get("stats", {})
    print(f"  평균: +{s.get('mean_pct')}% / 중앙값: +{s.get('median_pct')}%")
    print(f"  표준편차: {s.get('stdev_pct')}% / min {s.get('min_pct')}% ~ max +{s.get('max_pct')}%")
    d = analysis.get("distribution", {})
    print(f"\n=== 일별 수익률 분포 ===")
    print(f"  +20% 이상: {d.get('plus_20_days')}일 ({d.get('plus_20_rate_pct')}%)")
    print(f"  +10% 이상: {d.get('plus_10_days')}일 ({d.get('plus_10_rate_pct')}%)")
    print(f"  + 5% 이상: {d.get('plus_5_days')}일 ({d.get('plus_5_rate_pct')}%)")
    print(f"  +2.8% 이상 (70억 일평균): {d.get('plus_2_8_days')}일 ({d.get('plus_2_8_rate_pct')}%)")
    print(f"  승률 (+0%+): {d.get('win_rate_pct')}% ({d.get('positive_days')}일)")
    print(f"  손실일: {d.get('negative_days')}일")
    print(f"\n=== Best/Worst Pick ===")
    bp = analysis.get("best_pick_stats", {})
    wp = analysis.get("worst_pick_stats", {})
    print(f"  Best pick 평균: +{bp.get('mean')}% / max +{bp.get('max')}%")
    print(f"  Worst pick 평균: {wp.get('mean')}% / min {wp.get('min')}%")
