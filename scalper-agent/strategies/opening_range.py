"""ICT Opening Range / Initial Range 모듈

매일 장중 자동 실행:
- OR: 09:00~09:30 고/저
- IR: 09:00~10:00 고/저
- daily_bias: 현재가가 OR 고점 돌파(bullish) / 저점 이탈(bearish) / 그 사이(neutral)

데이터 소스: 기존 수집된 분봉 데이터 (1min/5min CSV)
결과: data_store/daily/{date}_opening_range.json
"""
import json
import logging
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd

logger = logging.getLogger("BH.OpeningRange")

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data_store"
MIN1_DIR = DATA_DIR / "1min"
MIN5_DIR = DATA_DIR / "5min"
DAILY_DIR = DATA_DIR / "daily"
OR_DIR = DATA_DIR / "opening_range"


# ═══════════════════════════════════════
#  핵심 계산
# ═══════════════════════════════════════

def _load_intraday(code: str, target_date: date) -> Optional[pd.DataFrame]:
    """분봉 데이터 로드 (1min 우선, 없으면 5min 폴백)"""
    for src_dir, label in [(MIN1_DIR, "1min"), (MIN5_DIR, "5min")]:
        fpath = src_dir / f"{code}.csv"
        if not fpath.exists():
            continue
        try:
            df = pd.read_csv(fpath, index_col=0, parse_dates=True)
            if df.index.tz is not None:
                df.index = df.index.tz_convert("Asia/Seoul").tz_localize(None)
            day_df = df[df.index.date == target_date]
            if len(day_df) > 0:
                return day_df
        except Exception as e:
            logger.debug(f"{code} {label} 로드 실패: {e}")
    return None


def calc_opening_range(code: str, target_date: date) -> Optional[dict]:
    """단일 종목 OR/IR 계산

    Returns:
        {code, date, or_high, or_low, ir_high, ir_low,
         daily_bias, bias_time, close} or None
    """
    df = _load_intraday(code, target_date)
    if df is None or len(df) == 0:
        return None

    # OR: 09:00 ~ 09:30
    or_mask = (df.index.time >= pd.Timestamp("09:00").time()) & \
              (df.index.time < pd.Timestamp("09:30").time())
    or_df = df[or_mask]

    # IR: 09:00 ~ 10:00
    ir_mask = (df.index.time >= pd.Timestamp("09:00").time()) & \
              (df.index.time < pd.Timestamp("10:00").time())
    ir_df = df[ir_mask]

    if len(or_df) == 0:
        return None

    or_high = int(or_df["high"].max())
    or_low = int(or_df["low"].min())
    ir_high = int(ir_df["high"].max()) if len(ir_df) > 0 else or_high
    ir_low = int(ir_df["low"].min()) if len(ir_df) > 0 else or_low

    # daily_bias: 10:00 이후 데이터에서 판정
    post_ir = df[df.index.time >= pd.Timestamp("10:00").time()]
    if len(post_ir) == 0:
        # 10:00 전이면 IR 구간 마지막 가격으로 판단
        last_price = int(ir_df["close"].iloc[-1])
        bias_time = None
    else:
        last_price = int(post_ir["close"].iloc[-1])
        bias_time = None

    # bias 판정
    daily_bias = "neutral"
    if last_price > or_high:
        daily_bias = "bullish"
    elif last_price < or_low:
        daily_bias = "bearish"

    # bias 전환 시점 탐색 (10:00 이후 최초 돌파/이탈 시각)
    if len(post_ir) > 0 and daily_bias != "neutral":
        for idx, row in post_ir.iterrows():
            price = row["close"]
            if daily_bias == "bullish" and price > or_high:
                bias_time = idx.strftime("%H:%M")
                break
            elif daily_bias == "bearish" and price < or_low:
                bias_time = idx.strftime("%H:%M")
                break

    # 종가 (마지막 봉)
    close = int(df["close"].iloc[-1])

    return {
        "code": code,
        "date": target_date.isoformat(),
        "or_high": or_high,
        "or_low": or_low,
        "ir_high": ir_high,
        "ir_low": ir_low,
        "daily_bias": daily_bias,
        "bias_time": bias_time,
        "close": close,
    }


# ═══════════════════════════════════════
#  일괄 실행
# ═══════════════════════════════════════

def run_opening_range(
    codes: list[str] = None,
    target_date: date = None,
) -> dict:
    """전 종목 OR/IR 계산 + JSON 저장

    Args:
        codes: 종목 코드 리스트 (None이면 보유+매수후보)
        target_date: 대상 날짜 (None이면 오늘)

    Returns:
        {code: {or_high, or_low, ir_high, ir_low, daily_bias, ...}}
    """
    if target_date is None:
        target_date = date.today()

    if codes is None:
        codes = _get_target_codes()

    results = {}
    for code in codes:
        r = calc_opening_range(code, target_date)
        if r:
            results[code] = r

    # 저장
    OR_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OR_DIR / f"{target_date.isoformat()}_opening_range.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    logger.info(f"[OR] {len(results)}종목 OR/IR 저장: {out_path}")

    return results


def load_opening_range(target_date: date = None) -> dict:
    """저장된 OR/IR 데이터 로드"""
    if target_date is None:
        target_date = date.today()
    fpath = OR_DIR / f"{target_date.isoformat()}_opening_range.json"
    if not fpath.exists():
        return {}
    try:
        with open(fpath, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def get_bias_adjustment(code: str, target_date: date = None) -> float:
    """매수 시그널 신뢰도 조정값 반환

    - bullish: +3점
    - bearish: -5점
    - neutral / 데이터 없음: 0점
    """
    data = load_opening_range(target_date)
    info = data.get(code)
    if not info:
        return 0.0
    bias = info.get("daily_bias", "neutral")
    if bias == "bullish":
        return 3.0
    elif bias == "bearish":
        return -5.0
    return 0.0


def _get_target_codes() -> list[str]:
    """보유 종목 + 추천 후보 코드 목록"""
    codes = set()

    # 추천 종목
    rec_path = DATA_DIR / "recommendation.json"
    if rec_path.exists():
        try:
            with open(rec_path, "r", encoding="utf-8") as f:
                rec = json.load(f)
            for s in rec.get("stocks", []):
                codes.add(s.get("code", ""))
            for s in rec.get("momentum_stocks", []):
                codes.add(s.get("code", ""))
        except Exception:
            pass

    # 유니버스 전체 (분봉 파일이 있는 것만)
    if MIN1_DIR.exists():
        for f in MIN1_DIR.glob("*.csv"):
            codes.add(f.stem)
    elif MIN5_DIR.exists():
        for f in MIN5_DIR.glob("*.csv"):
            codes.add(f.stem)

    codes.discard("")
    return sorted(codes)


# ═══════════════════════════════════════
#  백테스트: bias 일치율 검증
# ═══════════════════════════════════════

def backtest_bias_accuracy(n_days: int = 20, sample_codes: list[str] = None) -> dict:
    """과거 N거래일 OR bias(10:00 시점) vs 실제 종가 방향 일치율 검증

    핵심: bias는 10:00 첫 봉 가격으로 확정 → 종가 방향과 비교
    (종가로 bias를 계산하면 순환논증이 되므로 반드시 10:00 시점 기준)

    Returns:
        {total, match, accuracy, by_bias: {bullish: {total, match}, ...}}
    """
    if sample_codes is None:
        uni_path = DATA_DIR / "universe.json"
        if uni_path.exists():
            with open(uni_path, "r", encoding="utf-8") as f:
                uni = json.load(f)
            sorted_codes = sorted(uni.keys(),
                                  key=lambda c: uni[c].get("cap_億", uni[c].get("cap_억", 0)),
                                  reverse=True)
            sample_codes = sorted_codes[:50]
        else:
            sample_codes = []

    # 거래일 목록 수집 (5min 데이터 기준)
    all_dates = set()
    for code in sample_codes[:5]:
        fpath = MIN5_DIR / f"{code}.csv"
        if fpath.exists():
            try:
                df = pd.read_csv(fpath, index_col=0, parse_dates=True)
                all_dates.update(df.index.date)
            except Exception:
                pass

    if not all_dates:
        return {"error": "분봉 데이터 없음"}

    sorted_dates = sorted(all_dates)[-n_days:]

    total = 0
    match = 0
    by_bias = {"bullish": {"total": 0, "match": 0},
               "bearish": {"total": 0, "match": 0},
               "neutral": {"total": 0, "match": 0}}

    for code in sample_codes:
        for d in sorted_dates:
            df = _load_intraday(code, d)
            if df is None or len(df) < 10:
                continue

            # OR 구간 (09:00~09:30) 고/저
            or_mask = (df.index.time >= pd.Timestamp("09:00").time()) & \
                      (df.index.time < pd.Timestamp("09:30").time())
            or_df = df[or_mask]
            if len(or_df) == 0:
                continue

            or_high = or_df["high"].max()
            or_low = or_df["low"].min()

            # 10:00 시점 가격으로 bias 확정 (첫 10:00 봉 close)
            t10 = df[df.index.time >= pd.Timestamp("10:00").time()]
            if len(t10) == 0:
                continue
            price_at_10 = t10["close"].iloc[0]

            if price_at_10 > or_high:
                bias = "bullish"
            elif price_at_10 < or_low:
                bias = "bearish"
            else:
                bias = "neutral"

            # 종가 방향 (시가 대비)
            close_price = df["close"].iloc[-1]
            open_price = df["open"].iloc[0]
            if open_price == 0:
                continue

            pct = (close_price - open_price) / open_price * 100
            if pct > 0.1:
                actual_dir = "bullish"
            elif pct < -0.1:
                actual_dir = "bearish"
            else:
                actual_dir = "neutral"

            total += 1
            by_bias[bias]["total"] += 1

            if bias == actual_dir:
                match += 1
                by_bias[bias]["match"] += 1

    accuracy = round(match / total * 100, 1) if total > 0 else 0
    for k in by_bias:
        t = by_bias[k]["total"]
        by_bias[k]["accuracy"] = round(by_bias[k]["match"] / t * 100, 1) if t > 0 else 0

    return {
        "n_days": len(sorted_dates),
        "n_stocks": len(sample_codes),
        "total": total,
        "match": match,
        "accuracy": accuracy,
        "by_bias": by_bias,
    }


# ═══════════════════════════════════════
#  CLI
# ═══════════════════════════════════════

if __name__ == "__main__":
    import sys
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")

    if len(sys.argv) > 1 and sys.argv[1] == "backtest":
        print("OR/IR bias 백테스트 실행...")
        result = backtest_bias_accuracy()
        print(f"\n=== OR Bias 일치율 검증 ===")
        print(f"기간: {result.get('n_days', 0)}거래일 x {result.get('n_stocks', 0)}종목")
        print(f"전체: {result['total']}건 중 {result['match']}건 일치 = {result['accuracy']}%")
        for bias, info in result.get("by_bias", {}).items():
            print(f"  {bias:>8}: {info['total']:>5}건 → {info['accuracy']:>5.1f}%")
    else:
        print("Opening Range 일괄 계산...")
        results = run_opening_range()
        bullish = sum(1 for r in results.values() if r["daily_bias"] == "bullish")
        bearish = sum(1 for r in results.values() if r["daily_bias"] == "bearish")
        neutral = sum(1 for r in results.values() if r["daily_bias"] == "neutral")
        print(f"결과: {len(results)}종목 - bullish:{bullish} bearish:{bearish} neutral:{neutral}")
