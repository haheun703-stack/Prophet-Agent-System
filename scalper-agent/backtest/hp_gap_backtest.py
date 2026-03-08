"""ICT Phase 3-1 - HP Gap 백테스트

HP Gap 정의:
- 갭업 + 당일 고가가 전일 고가 돌파 = HP Gap Up (상방 유동성 흡수)
- 갭다운 + 당일 저가가 전일 저가 이탈 = HP Gap Down (하방 유동성 흡수)
- 일반 갭 = 갭 발생했으나 프리미엄 레벨 미흡수

반전 테스트:
- 갭 형성 후 5거래일 내 갭 영역 되돌림 발생 시
- HP Gap Up: 갭 하단에서 반등 +1% = 지지 성공
- HP Gap Down: 갭 상단에서 하락 -1% = 저항 성공
"""
import json
import logging
import sys
import io
from datetime import date
from pathlib import Path

import pandas as pd
import numpy as np

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
logger = logging.getLogger("BH.HPGapBacktest")

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data_store"
DAILY_DIR = DATA_DIR / "daily"


def _load_daily(code: str) -> pd.DataFrame | None:
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
        df = df.sort_index()
        for col in ["open", "high", "low", "close", "volume"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        return df
    except Exception:
        return None


def _get_sample_codes(n: int = 50) -> list[str]:
    uni_path = DATA_DIR / "universe.json"
    if uni_path.exists():
        with open(uni_path, "r", encoding="utf-8") as f:
            uni = json.load(f)
        sorted_codes = sorted(
            uni.keys(),
            key=lambda c: uni[c].get("cap_億", 0),
            reverse=True,
        )
        return sorted_codes[:n]
    return []


def _classify_gap_size(gap_pct: float) -> str:
    """갭 크기 분류"""
    abs_gap = abs(gap_pct)
    if abs_gap < 1.0:
        return "0.5~1%"
    elif abs_gap < 2.0:
        return "1~2%"
    else:
        return "2%+"


def run_backtest(
    n_days: int = 120,
    gap_threshold: float = 0.5,
    reversal_window: int = 5,
    support_pct: float = 1.0,
    sample_codes: list[str] = None,
) -> dict:
    """HP Gap 백테스트 실행

    Args:
        n_days: 테스트 기간 (거래일)
        gap_threshold: 갭 판정 기준 (%)
        reversal_window: 되돌림 판정 윈도우 (거래일)
        support_pct: 지지/저항 성공 기준 (%)
        sample_codes: 테스트 종목

    Returns:
        {hp_gap_up, hp_gap_down, normal_gap_up, normal_gap_down,
         by_size: {size: {hp: {...}, normal: {...}}}}
    """
    if sample_codes is None:
        sample_codes = _get_sample_codes(50)

    # 카테고리별 통계
    stats = {
        "hp_gap_up": {"total": 0, "retraced": 0, "success": 0},
        "hp_gap_down": {"total": 0, "retraced": 0, "success": 0},
        "normal_gap_up": {"total": 0, "retraced": 0, "success": 0},
        "normal_gap_down": {"total": 0, "retraced": 0, "success": 0},
    }

    # 갭 크기별 세분화
    by_size = {}
    for size_label in ["0.5~1%", "1~2%", "2%+"]:
        by_size[size_label] = {
            "hp": {"total": 0, "retraced": 0, "success": 0},
            "normal": {"total": 0, "retraced": 0, "success": 0},
        }

    processed = 0

    for code in sample_codes:
        df = _load_daily(code)
        if df is None or len(df) < n_days + reversal_window + 5:
            continue

        # 최근 n_days + reversal_window 범위
        df = df.tail(n_days + reversal_window + 1)
        dates = df.index.tolist()

        for i in range(1, len(dates) - reversal_window):
            prev = df.loc[dates[i - 1]]
            today = df.loc[dates[i]]
            future = df.iloc[i + 1: i + 1 + reversal_window]

            if len(future) == 0:
                continue

            prev_close = prev["close"]
            prev_high = prev["high"]
            prev_low = prev["low"]
            today_open = today["open"]
            today_high = today["high"]
            today_low = today["low"]

            if prev_close <= 0 or today_open <= 0:
                continue

            gap_pct = (today_open - prev_close) / prev_close * 100

            # 갭 판정
            if abs(gap_pct) < gap_threshold:
                continue

            is_gap_up = gap_pct > 0
            size_label = _classify_gap_size(gap_pct)

            if is_gap_up:
                # 갭업: 갭 영역 = [prev_close, today_open]
                gap_bottom = prev_close
                gap_top = today_open

                # HP 판정: 당일 고가가 전일 고가를 돌파
                is_hp = today_high > prev_high

                cat = "hp_gap_up" if is_hp else "normal_gap_up"
                size_cat = "hp" if is_hp else "normal"

                stats[cat]["total"] += 1
                by_size[size_label][size_cat]["total"] += 1

                # 되돌림: 이후 5일 내 저가가 갭 영역 진입 (gap_top 아래)
                retraced = False
                support_success = False

                for j in range(len(future)):
                    row = future.iloc[j]
                    if row["low"] <= gap_top:
                        retraced = True
                        # 지지 성공: 갭 하단(gap_bottom) 터치 후 반등 +support_pct%
                        touch_price = max(row["low"], gap_bottom)
                        # 이후 남은 기간에서 반등 확인
                        remaining = future.iloc[j:]
                        if len(remaining) > 0:
                            max_after = remaining["high"].max()
                            if (max_after - touch_price) / touch_price * 100 >= support_pct:
                                support_success = True
                        break

                if retraced:
                    stats[cat]["retraced"] += 1
                    by_size[size_label][size_cat]["retraced"] += 1
                    if support_success:
                        stats[cat]["success"] += 1
                        by_size[size_label][size_cat]["success"] += 1

            else:
                # 갭다운: 갭 영역 = [today_open, prev_close]
                gap_bottom = today_open
                gap_top = prev_close

                # HP 판정: 당일 저가가 전일 저가를 이탈
                is_hp = today_low < prev_low

                cat = "hp_gap_down" if is_hp else "normal_gap_down"
                size_cat = "hp" if is_hp else "normal"

                stats[cat]["total"] += 1
                by_size[size_label][size_cat]["total"] += 1

                # 되돌림: 이후 5일 내 고가가 갭 영역 진입 (gap_bottom 위)
                retraced = False
                resist_success = False

                for j in range(len(future)):
                    row = future.iloc[j]
                    if row["high"] >= gap_bottom:
                        retraced = True
                        # 저항 성공: 갭 상단(gap_top) 터치 후 하락 -support_pct%
                        touch_price = min(row["high"], gap_top)
                        remaining = future.iloc[j:]
                        if len(remaining) > 0:
                            min_after = remaining["low"].min()
                            if (touch_price - min_after) / touch_price * 100 >= support_pct:
                                resist_success = True
                        break

                if retraced:
                    stats[cat]["retraced"] += 1
                    by_size[size_label][size_cat]["retraced"] += 1
                    if resist_success:
                        stats[cat]["success"] += 1
                        by_size[size_label][size_cat]["success"] += 1

        processed += 1

    # 비율 계산
    def _calc(d):
        d["retrace_rate"] = round(d["retraced"] / d["total"] * 100, 1) if d["total"] > 0 else 0.0
        d["success_rate"] = round(d["success"] / d["retraced"] * 100, 1) if d["retraced"] > 0 else 0.0
        return d

    for k in stats:
        _calc(stats[k])

    for size_label in by_size:
        _calc(by_size[size_label]["hp"])
        _calc(by_size[size_label]["normal"])

    # HP vs 일반 차이
    hp_up_rate = stats["hp_gap_up"]["success_rate"]
    normal_up_rate = stats["normal_gap_up"]["success_rate"]
    hp_down_rate = stats["hp_gap_down"]["success_rate"]
    normal_down_rate = stats["normal_gap_down"]["success_rate"]

    diff_up = hp_up_rate - normal_up_rate
    diff_down = hp_down_rate - normal_down_rate
    avg_diff = (diff_up + diff_down) / 2

    return {
        "n_days": n_days,
        "n_stocks": processed,
        "gap_threshold": gap_threshold,
        "reversal_window": reversal_window,
        "support_pct": support_pct,
        "stats": stats,
        "by_size": by_size,
        "diff_up": round(diff_up, 1),
        "diff_down": round(diff_down, 1),
        "avg_diff": round(avg_diff, 1),
        "verdict": "구현 진행" if avg_diff >= 10.0 else "폐기",
    }


if __name__ == "__main__":
    print("=" * 60)
    print("  ICT Phase 3-1 - HP Gap 백테스트")
    print("=" * 60)

    result = run_backtest()

    stats = result["stats"]
    print(f"\n기간: {result['n_days']}거래일 x {result['n_stocks']}종목")
    print(f"갭 기준: ±{result['gap_threshold']}%, 되돌림 윈도우: {result['reversal_window']}일")
    print(f"지지/저항 성공 기준: ±{result['support_pct']}%")

    print(f"\n{'갭 유형':<16} | {'건수':>5} | {'되돌림':>6} | {'성공':>5} | {'되돌림률':>7} | {'성공률':>6}")
    print("-" * 65)
    for cat in ["hp_gap_up", "hp_gap_down", "normal_gap_up", "normal_gap_down"]:
        s = stats[cat]
        label = {
            "hp_gap_up": "HP Gap Up",
            "hp_gap_down": "HP Gap Down",
            "normal_gap_up": "일반 Gap Up",
            "normal_gap_down": "일반 Gap Down",
        }[cat]
        print(f"{label:<16} | {s['total']:>5} | {s['retraced']:>6} | {s['success']:>5} | "
              f"{s['retrace_rate']:>6.1f}% | {s['success_rate']:>5.1f}%")

    print(f"\nHP vs 일반 차이:")
    print(f"  Gap Up:   {result['diff_up']:+.1f}%p")
    print(f"  Gap Down: {result['diff_down']:+.1f}%p")
    print(f"  평균:     {result['avg_diff']:+.1f}%p")

    print(f"\n=== 갭 크기별 세분화 ===")
    print(f"{'크기':<8} | {'HP건수':>6} {'HP성공률':>7} | {'일반건수':>7} {'일반성공률':>8} | {'차이':>6}")
    print("-" * 60)
    for size_label in ["0.5~1%", "1~2%", "2%+"]:
        hp = result["by_size"][size_label]["hp"]
        nm = result["by_size"][size_label]["normal"]
        diff = hp["success_rate"] - nm["success_rate"]
        print(f"{size_label:<8} | {hp['total']:>6} {hp['success_rate']:>6.1f}% | "
              f"{nm['total']:>7} {nm['success_rate']:>7.1f}% | {diff:>+5.1f}%p")

    print(f"\n{'=' * 60}")
    print(f"  판정: {result['verdict']}")
    print(f"  (기준: HP vs 일반 평균 차이 ≥ 10%p)")
    print(f"{'=' * 60}")
