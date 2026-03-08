"""ICT Equal High/Low 탐지 모듈

매일 08:30 premium_levels 직후 실행:
- 최근 60거래일 일봉 데이터로 Equal High / Equal Low 클러스터링
- 허용 오차: 5만원 이상 0.3%, 1만원 이하 0.5%, 그 사이 선형 보간
- 터치 2회 = normal, 3회+ = strong
- 손절/타겟 후보 + 매수 신뢰도 보정값 제공

데이터 소스: data_store/daily/ (일봉 CSV)
결과: data_store/equal_levels/{date}.json
"""
import json
import logging
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

logger = logging.getLogger("BH.EqualLevel")

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data_store"
DAILY_DIR = DATA_DIR / "daily"
EQ_DIR = DATA_DIR / "equal_levels"


# ═══════════════════════════════════════
#  허용 오차 (가격대별 차등)
# ═══════════════════════════════════════

def _get_tolerance_pct(price: float) -> float:
    """가격대별 허용 오차 반환
    - 5만원 이상: 0.3%
    - 1만원 이하: 0.5%
    - 그 사이: 선형 보간
    """
    if price >= 50000:
        return 0.3
    elif price <= 10000:
        return 0.5
    else:
        # 10000~50000 선형 보간: 0.5% → 0.3%
        ratio = (price - 10000) / (50000 - 10000)
        return 0.5 - ratio * 0.2


# ═══════════════════════════════════════
#  일봉 로드
# ═══════════════════════════════════════

def _load_daily(code: str) -> Optional[pd.DataFrame]:
    """일봉 CSV 로드 (한글/영문 헤더 자동 감지)"""
    fpath = DAILY_DIR / f"{code}.csv"
    if not fpath.exists():
        return None
    try:
        df = pd.read_csv(fpath)
        col_map = {"날짜": "date", "시가": "open", "고가": "high",
                    "저가": "low", "종가": "close", "거래량": "volume"}
        if "날짜" in df.columns:
            df = df.rename(columns=col_map)
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"])
            df = df.set_index("date")
        else:
            df.index = pd.to_datetime(df.index)

        if df.index.tz is not None:
            df.index = df.index.tz_convert("Asia/Seoul").tz_localize(None)

        df = df.sort_index()
        for col in ["open", "high", "low", "close", "volume"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        return df
    except Exception as e:
        logger.debug(f"{code} daily 로드 실패: {e}")
        return None


# ═══════════════════════════════════════
#  클러스터링 기반 Equal Level 탐지
# ═══════════════════════════════════════

def _cluster_prices(prices: list[tuple[float, str]], tol_pct: float) -> list[dict]:
    """근접 가격을 클러스터링하여 Equal Level 후보 추출

    Args:
        prices: [(price, date_str), ...] - 정렬 불필요
        tol_pct: 허용 오차 비율 (0.3 → 0.3%)

    Returns:
        [{price: 대표가, touches: 횟수, dates: [날짜들]}]
        touches >= 2인 것만 반환
    """
    if len(prices) < 2:
        return []

    # 가격순 정렬
    sorted_prices = sorted(prices, key=lambda x: x[0])
    clusters = []
    used = [False] * len(sorted_prices)

    for i in range(len(sorted_prices)):
        if used[i]:
            continue

        cluster_prices = [sorted_prices[i][0]]
        cluster_dates = [sorted_prices[i][1]]
        used[i] = True

        # i 기준으로 근접한 것들 그룹핑
        ref_price = sorted_prices[i][0]
        threshold = ref_price * tol_pct / 100.0

        for j in range(i + 1, len(sorted_prices)):
            if used[j]:
                continue
            if abs(sorted_prices[j][0] - ref_price) <= threshold:
                cluster_prices.append(sorted_prices[j][0])
                cluster_dates.append(sorted_prices[j][1])
                used[j] = True
            elif sorted_prices[j][0] - ref_price > threshold:
                # 정렬되어 있으므로 이후 더 먼 가격은 패스
                break

        if len(cluster_prices) >= 2:
            # 대표 가격 = 중앙값 (정수 변환)
            rep_price = int(np.median(cluster_prices))
            clusters.append({
                "price": rep_price,
                "touches": len(cluster_prices),
                "dates": cluster_dates,
                "strength": "strong" if len(cluster_prices) >= 3 else "normal",
            })

    return clusters


def detect_equal_levels(
    code: str,
    target_date: date = None,
    lookback: int = 60,
    tol_pct: float = None,
) -> Optional[dict]:
    """단일 종목 Equal High/Low 탐지

    Args:
        code: 종목 코드
        target_date: 기준 날짜
        lookback: 룩백 거래일 수
        tol_pct: 허용 오차 (None이면 가격대별 자동)

    Returns:
        {code, date, equal_highs: [...], equal_lows: [...],
         current_close, tolerance_pct}
    """
    if target_date is None:
        target_date = date.today()

    df = _load_daily(code)
    if df is None or len(df) < 10:
        return None

    # target_date 이전 데이터만, 최근 lookback일
    before = df[df.index.date < target_date]
    if len(before) < lookback // 2:
        return None
    window = before.tail(lookback)

    if len(window) < 5:
        return None

    # 현재가 (마지막 종가)
    current_close = int(window["close"].iloc[-1])

    # 가격대별 허용 오차
    if tol_pct is None:
        tol_pct = _get_tolerance_pct(current_close)

    # 고점 / 저점 수집
    highs = []
    lows = []
    for idx, row in window.iterrows():
        d_str = idx.strftime("%m-%d")
        highs.append((row["high"], d_str))
        lows.append((row["low"], d_str))

    # 클러스터링
    equal_highs = _cluster_prices(highs, tol_pct)
    equal_lows = _cluster_prices(lows, tol_pct)

    # 가격순 정렬 (고점은 높은 순, 저점은 낮은 순)
    equal_highs.sort(key=lambda x: x["price"], reverse=True)
    equal_lows.sort(key=lambda x: x["price"])

    return {
        "code": code,
        "date": target_date.isoformat(),
        "equal_highs": equal_highs,
        "equal_lows": equal_lows,
        "current_close": current_close,
        "tolerance_pct": tol_pct,
        "lookback": lookback,
    }


# ═══════════════════════════════════════
#  일괄 실행 + 저장
# ═══════════════════════════════════════

def run_equal_levels(
    codes: list[str] = None,
    target_date: date = None,
    lookback: int = 60,
) -> dict:
    """전 종목 Equal Level 탐지 + JSON 저장"""
    if target_date is None:
        target_date = date.today()

    if codes is None:
        codes = _get_universe_codes()

    results = {}
    for code in codes:
        r = detect_equal_levels(code, target_date, lookback)
        if r and (r["equal_highs"] or r["equal_lows"]):
            results[code] = r

    # 저장
    EQ_DIR.mkdir(parents=True, exist_ok=True)
    out_path = EQ_DIR / f"{target_date.isoformat()}.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    logger.info(f"[EQ] {len(results)}종목 Equal Level 저장: {out_path}")

    return results


def load_equal_levels(target_date: date = None) -> dict:
    """저장된 Equal Level 로드"""
    if target_date is None:
        target_date = date.today()
    fpath = EQ_DIR / f"{target_date.isoformat()}.json"
    if not fpath.exists():
        return {}
    try:
        with open(fpath, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


# ═══════════════════════════════════════
#  스윙봇 연동: 점수 보정
# ═══════════════════════════════════════

def get_eq_score_adjustment(code: str, current_price: int,
                            target_date: date = None) -> tuple[float, str]:
    """매수 시그널 신뢰도 보정값 반환

    - Equal Low 하방이탈 후 반등 중 (EQ Low < current < EQ Low * 1.02):
      normal → +5점, strong → +8점
    - Equal High 근접 (1% 이내):
      → -3점

    Returns:
        (score_adjustment, reason_str)
    """
    data = load_equal_levels(target_date)
    info = data.get(code)
    if not info:
        return 0.0, ""

    adj = 0.0
    reasons = []

    # Equal Low 반등 체크: 현재가가 EQ Low 위에 있되, 2% 이내
    for el in info.get("equal_lows", []):
        eq_price = el["price"]
        if eq_price <= 0:
            continue
        dist_pct = (current_price - eq_price) / eq_price * 100

        # EQ Low 바로 위에서 반등 중 (0% ~ +2%)
        if 0 <= dist_pct <= 2.0:
            bonus = 8.0 if el["strength"] == "strong" else 5.0
            if bonus > adj:
                adj = bonus
                touches = el["touches"]
                reasons = [f"EQ Low {eq_price:,} 반등(x{touches})"]
            break  # 가장 가까운 것 1개만

    # Equal High 근접 체크: 현재가 기준 EQ High까지 1% 이내
    for eh in info.get("equal_highs", []):
        eq_price = eh["price"]
        if eq_price <= 0:
            continue
        dist_pct = (eq_price - current_price) / current_price * 100

        if 0 < dist_pct <= 1.0:
            adj -= 3.0
            touches = eh["touches"]
            reasons.append(f"EQ High {eq_price:,} 근접(x{touches})")
            break

    return adj, " / ".join(reasons)


def get_eq_target_levels(code: str, current_price: int,
                         target_date: date = None) -> dict:
    """Equal Level 기반 손절/타겟 후보

    Returns:
        {eq_sl: Equal Low (이탈 시 추가하락),
         eq_tp: Equal High (상방 타겟),
         eq_sl_touches, eq_tp_touches, eq_sl_strength, eq_tp_strength}
    """
    data = load_equal_levels(target_date)
    info = data.get(code)
    if not info:
        return {}

    result = {}

    # 가장 가까운 하방 Equal Low → 손절 후보
    for el in info.get("equal_lows", []):
        if el["price"] < current_price:
            result["eq_sl"] = el["price"]
            result["eq_sl_touches"] = el["touches"]
            result["eq_sl_strength"] = el["strength"]
            result["eq_sl_dist_pct"] = round(
                (el["price"] - current_price) / current_price * 100, 2
            )
            break

    # 가장 가까운 상방 Equal High → 익절 타겟
    for eh in sorted(info.get("equal_highs", []), key=lambda x: x["price"]):
        if eh["price"] > current_price:
            result["eq_tp"] = eh["price"]
            result["eq_tp_touches"] = eh["touches"]
            result["eq_tp_strength"] = eh["strength"]
            result["eq_tp_dist_pct"] = round(
                (eh["price"] - current_price) / current_price * 100, 2
            )
            break

    return result


# ═══════════════════════════════════════
#  프리미엄 레벨 통합
# ═══════════════════════════════════════

def merge_eq_to_premium_levels(pl: dict, eq_data: dict) -> dict:
    """Equal Level을 프리미엄 레벨에 병합

    Args:
        pl: premium_levels dict (종목 단위)
        eq_data: equal_level_detector dict (종목 단위)

    Returns:
        pl (수정된)
    """
    if not eq_data:
        return pl

    existing_prices = {lv["price"] for lv in pl.get("levels", [])}

    for eh in eq_data.get("equal_highs", []):
        price = eh["price"]
        if price not in existing_prices:
            pl["levels"].append({
                "name": f"EQ고 x{eh['touches']}",
                "price": price,
                "type": "resistance",
                "eq_strength": eh["strength"],
            })
            existing_prices.add(price)

    for el in eq_data.get("equal_lows", []):
        price = el["price"]
        if price not in existing_prices:
            pl["levels"].append({
                "name": f"EQ저 x{el['touches']}",
                "price": price,
                "type": "support",
                "eq_strength": el["strength"],
            })
            existing_prices.add(price)

    return pl


def format_telegram_eq(code: str, name: str, current_price: int,
                       target_date: date = None) -> str:
    """텔레그램 EQ 레벨 1줄 포맷

    예: 한국콜마 72,400 | 상방: EQ High 75,200(+3.9%) x2 | 하방: EQ Low 68,500(-5.4%) x3 strong
    """
    data = load_equal_levels(target_date)
    info = data.get(code)
    if not info:
        return ""

    parts = []

    # 가장 가까운 상방 EQ High
    for eh in sorted(info.get("equal_highs", []), key=lambda x: x["price"]):
        if eh["price"] > current_price:
            dist = (eh["price"] - current_price) / current_price * 100
            strength_tag = " strong" if eh["strength"] == "strong" else ""
            parts.append(
                f"EQ High {eh['price']:,}({dist:+.1f}%) x{eh['touches']}{strength_tag}"
            )
            break

    # 가장 가까운 하방 EQ Low
    for el in sorted(info.get("equal_lows", []),
                     key=lambda x: x["price"], reverse=True):
        if el["price"] < current_price:
            dist = (el["price"] - current_price) / current_price * 100
            strength_tag = " strong" if el["strength"] == "strong" else ""
            parts.append(
                f"EQ Low {el['price']:,}({dist:+.1f}%) x{el['touches']}{strength_tag}"
            )
            break

    if not parts:
        return ""

    return f"{name} {current_price:,} | " + " | ".join(parts)


# ═══════════════════════════════════════
#  백테스트: EQ Level 이탈 후 반전율
# ═══════════════════════════════════════

def backtest_eq_reversal(
    n_days: int = 60,
    lookback: int = 60,
    tol_pct: float = None,
    reversal_days: int = 5,
    reversal_pct: float = 2.0,
    sample_codes: list[str] = None,
) -> dict:
    """Equal Level 이탈 후 반전율 백테스트

    - Equal Low 이탈 → 5거래일 내 +2% 회복 = 반전
    - Equal High 돌파 → 5거래일 내 -2% 하락 = 반전

    Args:
        n_days: 테스트 기간 (거래일 수)
        lookback: EQ Level 탐지 룩백 일수
        tol_pct: 허용 오차 (None=가격대별 자동)
        reversal_days: 반전 판정 윈도우
        reversal_pct: 반전 기준 (%)
        sample_codes: 테스트 종목 (None=시총 상위 50)

    Returns:
        {total, eq_low_break: {total, reversals, rate},
         eq_high_break: {total, reversals, rate},
         strong_vs_normal: {strong: {total, reversals, rate}, normal: {...}}}
    """
    if sample_codes is None:
        uni_path = DATA_DIR / "universe.json"
        if uni_path.exists():
            with open(uni_path, "r", encoding="utf-8") as f:
                uni = json.load(f)
            sorted_codes = sorted(
                uni.keys(),
                key=lambda c: uni[c].get("cap_億", 0),
                reverse=True,
            )
            sample_codes = sorted_codes[:50]
        else:
            sample_codes = []

    eq_low_stats = {"total": 0, "reversals": 0}
    eq_high_stats = {"total": 0, "reversals": 0}
    strength_stats = {
        "strong": {"total": 0, "reversals": 0},
        "normal": {"total": 0, "reversals": 0},
    }

    for code in sample_codes:
        df = _load_daily(code)
        if df is None or len(df) < lookback + n_days:
            continue

        # 테스트 가능한 날짜 범위
        all_dates = df.index.date
        unique_dates = sorted(set(all_dates))

        # 마지막 n_days 거래일을 테스트 대상으로
        if len(unique_dates) < lookback + reversal_days + 1:
            continue

        test_dates = unique_dates[-(n_days + reversal_days):-reversal_days]

        for d in test_dates:
            # d 시점 기준 EQ Level 계산
            eq = detect_equal_levels(code, d, lookback, tol_pct)
            if not eq:
                continue

            # d 당일 + 이후 reversal_days일 데이터
            future = df[df.index.date >= d]
            if len(future) < reversal_days + 1:
                continue

            today_row = future.iloc[0]
            future_window = future.iloc[1:reversal_days + 1]
            if len(future_window) == 0:
                continue

            today_close = today_row["close"]
            today_low = today_row["low"]
            today_high = today_row["high"]

            # Equal Low 이탈 체크: 당일 저가가 EQ Low 아래
            for el in eq.get("equal_lows", []):
                eq_price = el["price"]
                if eq_price <= 0:
                    continue
                if today_low <= eq_price:
                    # 이탈 발생
                    eq_low_stats["total"] += 1
                    strength_stats[el["strength"]]["total"] += 1

                    # 반전: 이후 reversal_days 내 이탈가 대비 +reversal_pct% 회복
                    max_recovery = future_window["high"].max()
                    if (max_recovery - eq_price) / eq_price * 100 >= reversal_pct:
                        eq_low_stats["reversals"] += 1
                        strength_stats[el["strength"]]["reversals"] += 1
                    break  # 종목당 날짜당 1개만

            # Equal High 돌파 체크: 당일 고가가 EQ High 위
            for eh in eq.get("equal_highs", []):
                eq_price = eh["price"]
                if eq_price <= 0:
                    continue
                if today_high >= eq_price:
                    # 돌파 발생
                    eq_high_stats["total"] += 1
                    strength_stats[eh["strength"]]["total"] += 1

                    # 반전: 이후 reversal_days 내 돌파가 대비 -reversal_pct% 하락
                    min_after = future_window["low"].min()
                    if (eq_price - min_after) / eq_price * 100 >= reversal_pct:
                        eq_high_stats["reversals"] += 1
                        strength_stats[eh["strength"]]["reversals"] += 1
                    break

    # 비율 계산
    def _calc_rate(d):
        t = d["total"]
        d["rate"] = round(d["reversals"] / t * 100, 1) if t > 0 else 0.0
        return d

    _calc_rate(eq_low_stats)
    _calc_rate(eq_high_stats)
    _calc_rate(strength_stats["strong"])
    _calc_rate(strength_stats["normal"])

    total = eq_low_stats["total"] + eq_high_stats["total"]
    total_rev = eq_low_stats["reversals"] + eq_high_stats["reversals"]

    return {
        "n_days": n_days,
        "n_stocks": len(sample_codes),
        "lookback": lookback,
        "tol_pct": tol_pct or "auto",
        "reversal_days": reversal_days,
        "reversal_pct": reversal_pct,
        "total": total,
        "reversals": total_rev,
        "overall_rate": round(total_rev / total * 100, 1) if total > 0 else 0.0,
        "eq_low_break": eq_low_stats,
        "eq_high_break": eq_high_stats,
        "strong_vs_normal": strength_stats,
    }


def backtest_param_sweep(sample_codes: list[str] = None) -> dict:
    """허용 오차 (0.2/0.3/0.5%) × 룩백 (20/40/60일) 파라미터 스위프

    Returns:
        {(tol, lookback): {탐지건수, 반전율, ...}}
    """
    if sample_codes is None:
        uni_path = DATA_DIR / "universe.json"
        if uni_path.exists():
            with open(uni_path, "r", encoding="utf-8") as f:
                uni = json.load(f)
            sorted_codes = sorted(
                uni.keys(),
                key=lambda c: uni[c].get("cap_億", 0),
                reverse=True,
            )
            sample_codes = sorted_codes[:30]
        else:
            sample_codes = []

    results = {}
    for tol in [0.2, 0.3, 0.5]:
        for lb in [20, 40, 60]:
            logger.info(f"[EQ Sweep] tol={tol}%, lookback={lb}일 ...")
            r = backtest_eq_reversal(
                n_days=40, lookback=lb, tol_pct=tol,
                sample_codes=sample_codes,
            )
            key = f"tol{tol}_lb{lb}"
            results[key] = {
                "tolerance": tol,
                "lookback": lb,
                "total": r["total"],
                "overall_rate": r["overall_rate"],
                "eq_low_rate": r["eq_low_break"]["rate"],
                "eq_high_rate": r["eq_high_break"]["rate"],
                "strong_rate": r["strong_vs_normal"]["strong"]["rate"],
                "normal_rate": r["strong_vs_normal"]["normal"]["rate"],
            }

    return results


def _get_universe_codes() -> list[str]:
    """유니버스 전 종목 코드"""
    codes = []
    if DAILY_DIR.exists():
        for f in DAILY_DIR.glob("*.csv"):
            codes.append(f.stem)
    return sorted(codes)


# ═══════════════════════════════════════
#  CLI
# ═══════════════════════════════════════

if __name__ == "__main__":
    import sys
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")

    if len(sys.argv) > 1 and sys.argv[1] == "backtest":
        print("EQ Level 반전율 백테스트 실행...")
        result = backtest_eq_reversal()
        print(f"\n=== Equal Level 반전율 검증 ===")
        print(f"기간: {result['n_days']}거래일 x {result['n_stocks']}종목")
        print(f"룩백: {result['lookback']}일, 허용오차: {result['tol_pct']}")
        print(f"전체: {result['total']}건 → {result['reversals']}건 반전 = {result['overall_rate']}%")
        print(f"  EQ Low 이탈: {result['eq_low_break']['total']}건 → {result['eq_low_break']['rate']}%")
        print(f"  EQ High 돌파: {result['eq_high_break']['total']}건 → {result['eq_high_break']['rate']}%")
        print(f"  strong(3+): {result['strong_vs_normal']['strong']['total']}건 → {result['strong_vs_normal']['strong']['rate']}%")
        print(f"  normal(2): {result['strong_vs_normal']['normal']['total']}건 → {result['strong_vs_normal']['normal']['rate']}%")

    elif len(sys.argv) > 1 and sys.argv[1] == "sweep":
        print("파라미터 스위프 실행 (tol × lookback)...")
        results = backtest_param_sweep()
        print(f"\n=== 파라미터 스위프 결과 ===")
        print(f"{'설정':>15} {'건수':>6} {'전체%':>7} {'EQLow%':>7} {'EQHigh%':>8} {'strong%':>8} {'normal%':>8}")
        print("-" * 70)
        for key, r in sorted(results.items()):
            label = f"tol{r['tolerance']}%_lb{r['lookback']}d"
            print(f"{label:>15} {r['total']:>6} {r['overall_rate']:>6.1f}% "
                  f"{r['eq_low_rate']:>6.1f}% {r['eq_high_rate']:>7.1f}% "
                  f"{r['strong_rate']:>7.1f}% {r['normal_rate']:>7.1f}%")

    elif len(sys.argv) > 1 and sys.argv[1] == "detect":
        # 특정 종목 탐지
        code = sys.argv[2] if len(sys.argv) > 2 else "005930"
        result = detect_equal_levels(code)
        if result:
            print(f"\n=== {code} Equal Levels ===")
            print(f"현재가: {result['current_close']:,} (허용오차: {result['tolerance_pct']}%)")
            print(f"Equal Highs:")
            for eh in result["equal_highs"]:
                print(f"  {eh['price']:>8,} x{eh['touches']} [{eh['strength']}] {eh['dates']}")
            print(f"Equal Lows:")
            for el in result["equal_lows"]:
                print(f"  {el['price']:>8,} x{el['touches']} [{el['strength']}] {el['dates']}")
        else:
            print(f"{code}: 데이터 없음 또는 EQ Level 없음")

    else:
        print("Equal Level 일괄 탐지...")
        results = run_equal_levels()
        total_highs = sum(len(r["equal_highs"]) for r in results.values())
        total_lows = sum(len(r["equal_lows"]) for r in results.values())
        strong_count = sum(
            1 for r in results.values()
            for eq in r["equal_highs"] + r["equal_lows"]
            if eq["strength"] == "strong"
        )
        print(f"결과: {len(results)}종목 - EQ High:{total_highs} EQ Low:{total_lows} (strong:{strong_count})")
