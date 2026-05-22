# -*- coding: utf-8 -*-
"""엘리어트 38.2% 피보 비율 백테스트 (사장님 5/22 16:30 비판 응답).

사장님 영구 룰 [[feedback_verify_external_knowledge]]:
> "남의 말만 듣지 말고 우리가 검증을 해봐야된다. 얼마의 수치가 맞는건지"

목적:
  - 사장님 자료 (trading factory 영상) "피보 38.2%" 무비판 수용 사고 → 실데이터 검증
  - 과거 +20%+ 종목 50건+ 표본 → 4파 저점 분포 측정
  - 38.2% vs 다른 비율 (23.6 / 50 / 61.8) 통계 비교
  - 최적 비율 결정 → elliott_wave.py 파라미터 갱신

데이터 소스:
  - 정보봇 Supabase: daily_limit_up_history (+20%+ 상한가 종목)
  - KIS API: fetch_daily_chart (일봉 200영업일)
  - 일봉 단위 1파/2파/3파/4파 패턴 분석

영구 메모리: [[project_5_22_evening_learning_fix]] TODO ⑯
"""
import argparse
import json
import logging
import statistics
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))

from dotenv import load_dotenv
load_dotenv(_ROOT.parent / ".env")

from bot.elliott_wave import _identify_waves

logger = logging.getLogger("BH.BacktestFib")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)


# ── 표본 수집 ─────────────────────────────────────
def collect_sample_codes(limit: int = 100, source: str = "surge_patterns") -> List[Dict]:
    """표본 수집 — 5/22 단타봇 자율 변경 (상승 추세 종목으로).

    Args:
        source:
          "active_limit_up" — daily_limit_up_history 2회+ active 종목 (사장님 5/22 명령)
          "surge_patterns"  — jarvis_learning/surge_patterns/*.json (최근 +5%+ 누적)
          "limit_up"        — daily_limit_up_history (이전 표본, 부적합 입증)
    """
    if source == "universe":
        # 사장님 5/22 명령 옵션 C: 더 큰 universe 표본 (상한가 외 종목 추가)
        # 필터: 시총 100억~5,000억 (중소형 변동성) + 거래량 활발
        try:
            uni_path = _ROOT / "data_store" / "universe.json"
            if not uni_path.exists():
                logger.error("universe.json 없음")
                return []
            u = json.loads(uni_path.read_text(encoding="utf-8"))
            candidates = []
            for code, info in u.items():
                if not isinstance(info, dict):
                    continue
                cap = info.get("cap_억", 0)
                vol = info.get("volume", 0)
                # 필터: 시총 100~5,000억 + 거래량 10만+
                if 100 <= cap <= 5000 and vol >= 100_000:
                    candidates.append({
                        "code": code,
                        "name": info.get("name", code),
                        "sector": info.get("sector", ""),
                        "cap": cap,
                        "volume": vol,
                    })
            # 거래량 기준 정렬 (활발한 종목 우선)
            candidates.sort(key=lambda x: -x["volume"])
            logger.info(f"universe 필터링: 2596 → {len(candidates)}건 → 상위 {limit}건")
            return candidates[:limit]
        except Exception as e:
            logger.error(f"universe 표본 실패: {e}")
            return []

    if source == "active_limit_up":
        # 사장님 5/22 명령: 상한가 2번+ 친 종목 (6개월 백테스트용)
        try:
            from utils.supabase_sql import query
            sql = """
                SELECT ticker, name,
                       COUNT(*) AS n_active,
                       MAX(date) AS latest,
                       MAX(consecutive_days) AS max_consec,
                       MAX(peak_close) AS peak
                FROM daily_limit_up_history
                WHERE date >= CURRENT_DATE - INTERVAL '180 days'
                GROUP BY ticker, name
                HAVING COUNT(*) >= 2
                ORDER BY COUNT(*) DESC, MAX(date) DESC
                LIMIT %s
            """
            rows = query(sql, (limit,))
            return [{"code": r["ticker"], "name": r["name"],
                     "n_active": r["n_active"], "latest": r["latest"]}
                    for r in (rows or [])]
        except Exception as e:
            logger.error(f"active_limit_up 표본 실패: {e}")
            return []

    if source == "surge_patterns":
        codes_seen = set()
        results = []
        sp_dir = _ROOT / "data_store" / "jarvis_learning" / "surge_patterns"
        if not sp_dir.exists():
            logger.error(f"{sp_dir} 폴더 없음")
            return []
        # 최신 파일부터 수집
        for fp in sorted(sp_dir.glob("*.json"), reverse=True):
            try:
                data = json.loads(fp.read_text(encoding="utf-8"))
                for s in data.get("stocks", []):
                    code = s.get("code")
                    if code and code not in codes_seen:
                        codes_seen.add(code)
                        results.append({
                            "code": code,
                            "name": s.get("name", code),
                            "surge_date": fp.stem,  # YYYYMMDD
                            "max_pct": s.get("max_pct", 0),
                        })
                        if len(results) >= limit:
                            break
                if len(results) >= limit:
                    break
            except Exception as e:
                logger.warning(f"{fp.name} 로드 실패: {e}")
        return results

    # 폴백: limit_up_history (이전 표본 — 부적합 입증)
    try:
        from utils.supabase_sql import query
        sql = """
            SELECT ticker, name, date
            FROM daily_limit_up_history
            WHERE date >= CURRENT_DATE - INTERVAL '90 days'
            ORDER BY date DESC, vol_ratio DESC
            LIMIT %s
        """
        rows = query(sql, (limit,))
        return [{"code": r["ticker"], "name": r["name"], "limit_up_date": r["date"]}
                for r in (rows or [])]
    except Exception as e:
        logger.error(f"표본 수집 실패: {e}")
        return []


# ── 4파 저점 피보 % 측정 ──────────────────────────
def measure_fib_retracement(bars: Sequence[Dict]) -> Optional[Dict]:
    """1파~4파 저점 패턴 감지 + 피보 % 계산.

    Args:
        bars: 일봉 OHLCV 리스트 (시간순)

    Returns:
        {w1_high, w2_low, w3_high, w4_low, wave_3_length,
         fib_retracement_pct, succeeded_to_w5}
        None: 패턴 미감지
    """
    if len(bars) < 20:
        return None

    w1, w2, w3, w4 = _identify_waves(bars, lookback=3)
    if w1 is None or w3 is None or w4 is None:
        return None

    w1h = bars[w1]["high"]
    w2l = bars[w2]["low"] if w2 is not None else 0
    w3h = bars[w3]["high"]
    w4l = bars[w4]["low"]

    # 3파 길이 (보수적 — w2 저점 → w3 고점)
    wave_3_length = w3h - w2l
    if wave_3_length <= 0:
        return None

    # 피보 되돌림 % — 4파 저점이 3파 길이의 몇 % 되돌림인가
    fib_retracement_pct = (w3h - w4l) / wave_3_length * 100.0

    # 5파 진입 성공 여부 — w4 이후 캔들 중 w3 고점 돌파 여부
    succeeded_to_w5 = False
    max_high_after_w4 = 0
    for b in bars[w4 + 1:]:
        if b["high"] > max_high_after_w4:
            max_high_after_w4 = b["high"]
        if b["high"] > w3h:
            succeeded_to_w5 = True
            break

    # 4파 저점 vs 1파 고점 (파동 중첩 금지 룰)
    non_overlap = w4l > w1h

    return {
        "w1_high": w1h, "w2_low": w2l, "w3_high": w3h, "w4_low": w4l,
        "wave_3_length": wave_3_length,
        "fib_retracement_pct": round(fib_retracement_pct, 2),
        "succeeded_to_w5": succeeded_to_w5,
        "max_high_after_w4": max_high_after_w4,
        "non_overlap_check": non_overlap,
        "max_gain_after_w4_pct": round((max_high_after_w4 / w4l - 1) * 100, 2) if w4l > 0 else 0,
    }


# ── 메인 백테스트 ─────────────────────────────────
def run_backtest(limit: int = 50, delay: float = 0.3,
                 source: str = "surge_patterns",
                 window_size: int = 60, window_step: int = 20) -> Dict:
    """전체 백테스트 흐름.

    Args:
        limit: 표본 종목 수
        delay: KIS API 호출 간 대기 (rate limit)
        source: 표본 소스 (active_limit_up / surge_patterns / limit_up)
        window_size: 슬라이딩 윈도우 크기 (일봉 봉수, 기본 60 ≈ 3개월)
        window_step: 슬라이딩 스텝 (기본 20)
    """
    from bot.kis_trader import KISTrader
    trader = KISTrader()

    # 1) 표본 수집
    samples = collect_sample_codes(limit=limit, source=source)
    if not samples:
        logger.error(f"표본 0건 — source={source}")
        return {}
    logger.info(f"표본 수집 ({source}): {len(samples)}건")

    # 2) 각 종목 일봉 끌어와 슬라이딩 윈도우로 4파 분석
    measurements = []
    for i, s in enumerate(samples, 1):
        code = s["code"]
        try:
            bars = trader.fetch_daily_chart(code)
            if not bars or len(bars) < window_size:
                logger.warning(f"[{i}/{len(samples)}] {code} {s['name']}: 일봉 부족 ({len(bars or [])}개)")
                continue

            # 슬라이딩 윈도우 — 한 종목에서 여러 4파 사이클 탐지
            n = len(bars)
            patterns_found = 0
            for start in range(0, n - window_size + 1, window_step):
                window = bars[start:start + window_size]
                m = measure_fib_retracement(window)
                if m is None:
                    continue
                m["code"] = code
                m["name"] = s["name"]
                m["window_start"] = window[0].get("date", "")
                m["window_end"] = window[-1].get("date", "")
                measurements.append(m)
                patterns_found += 1

            if i % 5 == 0:
                logger.info(f"진행 {i}/{len(samples)} — 누적 측정 {len(measurements)}건")

            time.sleep(delay)  # rate limit
        except Exception as e:
            logger.error(f"{code} 실패: {e}")

    # 3) 통계 분석
    if not measurements:
        return {"error": "측정 0건"}

    pcts = [m["fib_retracement_pct"] for m in measurements]
    succeeded = [m for m in measurements if m["succeeded_to_w5"]]
    succeeded_pcts = [m["fib_retracement_pct"] for m in succeeded]

    result = {
        "total_samples": len(samples),
        "valid_measurements": len(measurements),
        "succeeded_w5": len(succeeded),
        "stats_all": {
            "mean": round(statistics.mean(pcts), 2),
            "median": round(statistics.median(pcts), 2),
            "stdev": round(statistics.stdev(pcts), 2) if len(pcts) >= 2 else 0,
            "min": min(pcts),
            "max": max(pcts),
        },
        "stats_succeeded": {
            "mean": round(statistics.mean(succeeded_pcts), 2) if succeeded_pcts else 0,
            "median": round(statistics.median(succeeded_pcts), 2) if succeeded_pcts else 0,
            "stdev": round(statistics.stdev(succeeded_pcts), 2) if len(succeeded_pcts) >= 2 else 0,
        },
        "bucket_distribution": _bucket_distribution(pcts),
        "comparison_with_382": _compare_to_382(measurements),
        "raw_measurements": measurements[:20],  # 샘플만
    }

    return result


def _bucket_distribution(pcts: List[float]) -> Dict:
    """피보 % 구간별 분포."""
    buckets = {
        "0~23.6%": 0, "23.6~38.2%": 0, "38.2~50%": 0,
        "50~61.8%": 0, "61.8~78.6%": 0, "78.6%+": 0,
    }
    for p in pcts:
        if p < 23.6: buckets["0~23.6%"] += 1
        elif p < 38.2: buckets["23.6~38.2%"] += 1
        elif p < 50.0: buckets["38.2~50%"] += 1
        elif p < 61.8: buckets["50~61.8%"] += 1
        elif p < 78.6: buckets["61.8~78.6%"] += 1
        else: buckets["78.6%+"] += 1
    return buckets


def _compare_to_382(measurements: List[Dict]) -> Dict:
    """38.2% ± tolerance 구간 진입 종목의 5파 성공률.

    여러 tolerance 비교:
      - ±2.5% / ±5% / ±10% / ±15%
    """
    result = {}
    for tol in [2.5, 5.0, 10.0, 15.0]:
        in_zone = [m for m in measurements
                   if 38.2 - tol <= m["fib_retracement_pct"] <= 38.2 + tol]
        succeeded = [m for m in in_zone if m["succeeded_to_w5"]]
        result[f"±{tol}%"] = {
            "in_zone_count": len(in_zone),
            "succeeded": len(succeeded),
            "success_rate": round(len(succeeded) / len(in_zone) * 100, 1) if in_zone else 0,
        }
    return result


# ── 메인 실행 ─────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="엘리어트 38.2% 피보 백테스트")
    parser.add_argument("--limit", type=int, default=50, help="표본 종목 수 (기본 50)")
    parser.add_argument("--delay", type=float, default=0.3, help="KIS API 대기 (초)")
    parser.add_argument("--source", type=str, default="surge_patterns",
                        choices=["universe", "active_limit_up", "surge_patterns", "limit_up"],
                        help="표본 소스 (universe=옵션 C 대규모 / active_limit_up=상한가 2번+)")
    parser.add_argument("--window", type=int, default=60, help="슬라이딩 윈도우 봉수 (기본 60)")
    parser.add_argument("--step", type=int, default=20, help="슬라이딩 스텝 (기본 20)")
    parser.add_argument("--save", action="store_true", help="JSON 저장")
    args = parser.parse_args()

    print(f"\n=== 엘리어트 38.2% 백테스트 (source={args.source} / 표본 {args.limit}건 / 윈도우 {args.window}봉) ===\n")
    result = run_backtest(limit=args.limit, delay=args.delay,
                          source=args.source, window_size=args.window, window_step=args.step)

    if result.get("error"):
        print(f"❌ {result['error']}")
        return 1

    print(f"\n=== 결과 요약 ===")
    print(f"총 표본: {result['total_samples']}건")
    print(f"4파 패턴 측정 성공: {result['valid_measurements']}건")
    print(f"5파 진입 성공: {result['succeeded_w5']}건")
    print(f"\n[전체 측정 통계]")
    s = result["stats_all"]
    print(f"  평균: {s['mean']}% / 중앙값: {s['median']}% / 표준편차: {s['stdev']}%")
    print(f"  범위: {s['min']}% ~ {s['max']}%")
    print(f"\n[5파 성공 종목만 통계]")
    sw = result["stats_succeeded"]
    print(f"  평균: {sw['mean']}% / 중앙값: {sw['median']}% / 표준편차: {sw['stdev']}%")
    print(f"\n[피보 % 구간 분포]")
    for k, v in result["bucket_distribution"].items():
        bar = "█" * v
        print(f"  {k:<14} {v:>3}건 {bar}")
    print(f"\n[38.2% ± tolerance 5파 성공률]")
    for tol, c in result["comparison_with_382"].items():
        print(f"  {tol:<6}: {c['in_zone_count']:>3}건 / 성공 {c['succeeded']:>3}건 / 성공률 {c['success_rate']}%")

    if args.save:
        out_path = _ROOT / "data_store" / "backtest" / "elliott_fib_382.json"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(result, ensure_ascii=False, indent=2, default=str),
                            encoding="utf-8")
        print(f"\n💾 저장: {out_path}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
