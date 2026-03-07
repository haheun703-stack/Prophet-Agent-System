"""ICT 프리미엄 유동성 레벨 모듈

매일 08:30 자동 실행:
- 전일 고/저, 전주 고/저(직전 완성 주봉), 전월 고/저(직전 완성 월봉)
- OR/IR 값은 10:00 이후 병합 (opening_range.py에서 공급)
- 현재가 대비 각 레벨까지 거리(%) + 가장 가까운 상방/하방 레벨

데이터 소스: 일봉 CSV (data_store/daily/)
결과: data_store/premium_levels/{date}.json
"""
import json
import logging
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd

logger = logging.getLogger("BH.PremiumLevels")

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data_store"
DAILY_DIR = DATA_DIR / "daily"
PL_DIR = DATA_DIR / "premium_levels"


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
        # 한글 헤더 처리
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
        # 숫자 변환
        for col in ["open", "high", "low", "close", "volume"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        return df
    except Exception as e:
        logger.debug(f"{code} daily 로드 실패: {e}")
        return None


# ═══════════════════════════════════════
#  레벨 계산
# ═══════════════════════════════════════

def calc_premium_levels(code: str, target_date: date = None) -> Optional[dict]:
    """단일 종목 프리미엄 유동성 레벨 계산

    Returns:
        {code, date, prev_day_high/low, prev_week_high/low,
         prev_month_high/low, levels: [{name, price, type}]}
    """
    if target_date is None:
        target_date = date.today()

    df = _load_daily(code)
    if df is None or len(df) < 5:
        return None

    # target_date 이전 데이터만 사용
    before = df[df.index.date < target_date]
    if len(before) < 2:
        return None

    # ── 전일 고/저 ──
    prev_day = before.iloc[-1]
    prev_day_high = int(prev_day["high"])
    prev_day_low = int(prev_day["low"])
    prev_day_close = int(prev_day["close"])

    # ── 전주 고/저 (직전 완성 주봉) ──
    # target_date가 속한 주의 이전 주
    target_weekday = target_date.weekday()  # 0=Mon
    this_week_start = target_date - timedelta(days=target_weekday)
    prev_week_end = this_week_start - timedelta(days=1)
    prev_week_start = prev_week_end - timedelta(days=prev_week_end.weekday())

    pw_data = before[(before.index.date >= prev_week_start) &
                     (before.index.date <= prev_week_end)]
    if len(pw_data) > 0:
        prev_week_high = int(pw_data["high"].max())
        prev_week_low = int(pw_data["low"].min())
    else:
        # 직전 주 데이터 없으면 최근 5일
        last5 = before.tail(5)
        prev_week_high = int(last5["high"].max())
        prev_week_low = int(last5["low"].min())

    # ── 전월 고/저 (직전 완성 월봉) ──
    if target_date.month == 1:
        pm_year, pm_month = target_date.year - 1, 12
    else:
        pm_year, pm_month = target_date.year, target_date.month - 1

    pm_data = before[(before.index.year == pm_year) &
                     (before.index.month == pm_month)]
    if len(pm_data) > 0:
        prev_month_high = int(pm_data["high"].max())
        prev_month_low = int(pm_data["low"].min())
    else:
        # 전월 데이터 없으면 최근 20일
        last20 = before.tail(20)
        prev_month_high = int(last20["high"].max())
        prev_month_low = int(last20["low"].min())

    # 레벨 리스트 조합
    levels = [
        {"name": "전일고", "price": prev_day_high, "type": "resistance"},
        {"name": "전일저", "price": prev_day_low, "type": "support"},
        {"name": "전주고", "price": prev_week_high, "type": "resistance"},
        {"name": "전주저", "price": prev_week_low, "type": "support"},
        {"name": "전월고", "price": prev_month_high, "type": "resistance"},
        {"name": "전월저", "price": prev_month_low, "type": "support"},
    ]

    # 중복 제거 (동일 가격이면 더 큰 범위 우선)
    seen = set()
    unique_levels = []
    for lv in levels:
        if lv["price"] not in seen:
            unique_levels.append(lv)
            seen.add(lv["price"])

    return {
        "code": code,
        "date": target_date.isoformat(),
        "prev_day_high": prev_day_high,
        "prev_day_low": prev_day_low,
        "prev_day_close": prev_day_close,
        "prev_week_high": prev_week_high,
        "prev_week_low": prev_week_low,
        "prev_month_high": prev_month_high,
        "prev_month_low": prev_month_low,
        "levels": unique_levels,
    }


def enrich_with_current_price(pl: dict, current_price: int) -> dict:
    """현재가 대비 각 레벨 거리(%) 계산 + 가장 가까운 상방/하방"""
    if not pl or current_price <= 0:
        return pl

    for lv in pl.get("levels", []):
        dist = (lv["price"] - current_price) / current_price * 100
        lv["distance_pct"] = round(dist, 2)

    # 가장 가까운 상방/하방
    above = [lv for lv in pl["levels"] if lv["price"] > current_price]
    below = [lv for lv in pl["levels"] if lv["price"] < current_price]

    pl["nearest_resistance"] = min(above, key=lambda x: x["price"]) if above else None
    pl["nearest_support"] = max(below, key=lambda x: x["price"]) if below else None
    pl["current_price"] = current_price

    return pl


def merge_or_levels(pl: dict, or_data: dict) -> dict:
    """OR/IR 데이터를 프리미엄 레벨에 병합"""
    if not or_data:
        return pl

    or_levels = []
    if or_data.get("or_high"):
        or_levels.append({"name": "OR고", "price": or_data["or_high"], "type": "resistance"})
    if or_data.get("or_low"):
        or_levels.append({"name": "OR저", "price": or_data["or_low"], "type": "support"})
    if or_data.get("ir_high") and or_data["ir_high"] != or_data.get("or_high"):
        or_levels.append({"name": "IR고", "price": or_data["ir_high"], "type": "resistance"})
    if or_data.get("ir_low") and or_data["ir_low"] != or_data.get("or_low"):
        or_levels.append({"name": "IR저", "price": or_data["ir_low"], "type": "support"})

    # 기존 레벨에 추가 (중복 제거)
    existing_prices = {lv["price"] for lv in pl.get("levels", [])}
    for lv in or_levels:
        if lv["price"] not in existing_prices:
            pl["levels"].append(lv)
            existing_prices.add(lv["price"])

    pl["daily_bias"] = or_data.get("daily_bias", "neutral")
    pl["bias_time"] = or_data.get("bias_time")
    return pl


# ═══════════════════════════════════════
#  일괄 실행
# ═══════════════════════════════════════

def run_premium_levels(
    codes: list[str] = None,
    target_date: date = None,
) -> dict:
    """전 종목 프리미엄 레벨 계산 + JSON 저장"""
    if target_date is None:
        target_date = date.today()

    if codes is None:
        codes = _get_universe_codes()

    results = {}
    for code in codes:
        r = calc_premium_levels(code, target_date)
        if r:
            results[code] = r

    # 저장
    PL_DIR.mkdir(parents=True, exist_ok=True)
    out_path = PL_DIR / f"{target_date.isoformat()}.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    logger.info(f"[PL] {len(results)}종목 프리미엄 레벨 저장: {out_path}")

    return results


def load_premium_levels(target_date: date = None) -> dict:
    """저장된 프리미엄 레벨 로드"""
    if target_date is None:
        target_date = date.today()
    fpath = PL_DIR / f"{target_date.isoformat()}.json"
    if not fpath.exists():
        return {}
    try:
        with open(fpath, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def get_target_levels(code: str, current_price: int, target_date: date = None) -> dict:
    """매수 시 타겟/손절 제안

    Returns:
        {tp_1st: 가장 가까운 상방 레벨,
         sl_level: 가장 가까운 하방 레벨 아래,
         tp_name, sl_name}
    """
    data = load_premium_levels(target_date)
    pl = data.get(code)
    if not pl:
        return {}

    pl = enrich_with_current_price(pl, current_price)

    result = {}
    nr = pl.get("nearest_resistance")
    ns = pl.get("nearest_support")

    if nr:
        result["tp_1st"] = nr["price"]
        result["tp_name"] = nr["name"]
        result["tp_dist_pct"] = nr["distance_pct"]

    if ns:
        # 손절은 하방 레벨보다 0.5% 아래
        sl_price = int(ns["price"] * 0.995)
        result["sl_level"] = sl_price
        result["sl_name"] = ns["name"]
        result["sl_dist_pct"] = round(
            (sl_price - current_price) / current_price * 100, 2
        )

    return result


def format_telegram_levels(code: str, name: str, current_price: int,
                           target_date: date = None) -> str:
    """텔레그램 리포트용 프리미엄 레벨 1줄 포맷

    예: 한국콜마 72,400 | 상방: 전일고 73,800(+1.9%) | 하방: OR저 71,800(-0.8%)
    """
    data = load_premium_levels(target_date)
    pl = data.get(code)
    if not pl:
        return ""

    pl = enrich_with_current_price(pl, current_price)

    nr = pl.get("nearest_resistance")
    ns = pl.get("nearest_support")

    parts = [f"{name} {current_price:,}"]

    if nr:
        parts.append(f"상방: {nr['name']} {nr['price']:,}({nr['distance_pct']:+.1f}%)")
    if ns:
        parts.append(f"하방: {ns['name']} {ns['price']:,}({ns['distance_pct']:+.1f}%)")

    bias = pl.get("daily_bias")
    if bias and bias != "neutral":
        emoji = "🟢" if bias == "bullish" else "🔴"
        parts.append(f"{emoji}{bias}")

    return " | ".join(parts)


def _get_universe_codes() -> list[str]:
    """유니버스 전 종목 코드 (일봉 파일 있는 것)"""
    codes = []
    if DAILY_DIR.exists():
        for f in DAILY_DIR.glob("*.csv"):
            codes.append(f.stem)
    return sorted(codes)


# ═══════════════════════════════════════
#  백테스트: 프리미엄 레벨 반전율 검증
# ═══════════════════════════════════════

def backtest_reversal_rate(n_days: int = 20, sample_codes: list[str] = None,
                           touch_pct: float = 0.3, reversal_pct: float = 1.0) -> dict:
    """프리미엄 레벨 ±touch_pct% 이내 터치 후 반전(반대 방향 reversal_pct% 이상 이동) 비율

    Returns:
        {total_touches, reversals, reversal_rate, by_level: {...}}
    """
    if sample_codes is None:
        uni_path = DATA_DIR / "universe.json"
        if uni_path.exists():
            with open(uni_path, "r", encoding="utf-8") as f:
                uni = json.load(f)
            sorted_codes = sorted(uni.keys(),
                                  key=lambda c: uni[c].get("cap_억", 0),
                                  reverse=True)
            sample_codes = sorted_codes[:50]
        else:
            sample_codes = []

    try:
        from .opening_range import _load_intraday, MIN5_DIR
    except ImportError:
        from strategies.opening_range import _load_intraday, MIN5_DIR

    # 거래일 목록
    all_dates = set()
    for code in sample_codes[:5]:
        fpath = MIN5_DIR / f"{code}.csv"
        if fpath.exists():
            try:
                df = pd.read_csv(fpath, index_col=0, parse_dates=True)
                all_dates.update(df.index.date)
            except Exception:
                pass

    sorted_dates = sorted(all_dates)[-n_days:]

    total_touches = 0
    reversals = 0
    by_level = {}

    for code in sample_codes:
        for d in sorted_dates:
            pl = calc_premium_levels(code, d)
            if not pl:
                continue

            df = _load_intraday(code, d)
            if df is None or len(df) < 10:
                continue

            for lv in pl["levels"]:
                level_price = lv["price"]
                level_name = lv["name"]
                if level_price <= 0:
                    continue

                if level_name not in by_level:
                    by_level[level_name] = {"touches": 0, "reversals": 0}

                # 터치 판정: 장중 가격이 level ±touch_pct% 이내
                touch_range = level_price * touch_pct / 100
                touched_idx = None
                for i, (idx, row) in enumerate(df.iterrows()):
                    if abs(row["close"] - level_price) <= touch_range:
                        touched_idx = i
                        break

                if touched_idx is None:
                    continue

                total_touches += 1
                by_level[level_name]["touches"] += 1

                # 반전 판정: 터치 후 나머지 봉에서 반대 방향 reversal_pct% 이동
                remaining = df.iloc[touched_idx + 1:]
                if len(remaining) == 0:
                    continue

                touch_price = df.iloc[touched_idx]["close"]
                is_resistance = lv["type"] == "resistance"

                if is_resistance:
                    # 저항: 터치 후 하락하면 반전
                    min_after = remaining["low"].min()
                    if (touch_price - min_after) / touch_price * 100 >= reversal_pct:
                        reversals += 1
                        by_level[level_name]["reversals"] += 1
                else:
                    # 지지: 터치 후 상승하면 반전
                    max_after = remaining["high"].max()
                    if (max_after - touch_price) / touch_price * 100 >= reversal_pct:
                        reversals += 1
                        by_level[level_name]["reversals"] += 1

    reversal_rate = round(reversals / total_touches * 100, 1) if total_touches > 0 else 0
    for k in by_level:
        t = by_level[k]["touches"]
        by_level[k]["rate"] = round(by_level[k]["reversals"] / t * 100, 1) if t > 0 else 0

    return {
        "n_days": len(sorted_dates),
        "n_stocks": len(sample_codes),
        "total_touches": total_touches,
        "reversals": reversals,
        "reversal_rate": reversal_rate,
        "by_level": by_level,
    }


# ═══════════════════════════════════════
#  CLI
# ═══════════════════════════════════════

if __name__ == "__main__":
    import sys
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")

    sys.path.insert(0, str(BASE_DIR))

    if len(sys.argv) > 1 and sys.argv[1] == "backtest":
        print("프리미엄 레벨 반전율 백테스트 실행...")
        result = backtest_reversal_rate()
        print(f"\n=== 프리미엄 레벨 반전율 검증 ===")
        print(f"기간: {result.get('n_days', 0)}거래일 x {result.get('n_stocks', 0)}종목")
        print(f"전체: {result['total_touches']}건 터치 → {result['reversals']}건 반전 = {result['reversal_rate']}%")
        for name, info in sorted(result.get("by_level", {}).items(),
                                  key=lambda x: x[1]["touches"], reverse=True):
            print(f"  {name:>6}: {info['touches']:>5}건 터치 → {info['rate']:>5.1f}% 반전")
    else:
        print("프리미엄 레벨 계산 실행...")
        results = run_premium_levels()
        print(f"결과: {len(results)}종목 프리미엄 레벨 저장 완료")
