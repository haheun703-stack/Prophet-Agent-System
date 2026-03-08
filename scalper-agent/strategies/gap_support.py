"""단순 갭 지지/저항 탐지 모듈

매일 09:05 (시가 확정 후) 자동 실행:
- 당일 시가 vs 전일 종가 갭이 ±0.5% 이상이면 갭 영역 마킹
- 갭업: 갭 영역 [전일종가, 당일시가] → 하방 되돌림 시 지지 존
- 갭다운: 갭 영역 [당일시가, 전일종가] → 상방 되돌림 시 저항 존
- HP 여부 구분 없이 동일 처리 (백테스트: 지지 82%, 저항 75%)

데이터 소스: KIS API 당일 시가 + 전일 종가 (일봉 CSV)
결과: data_store/gap_levels/{date}.json
"""
import json
import logging
from datetime import date
from pathlib import Path
from typing import Optional

import pandas as pd

logger = logging.getLogger("BH.GapSupport")

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data_store"
DAILY_DIR = DATA_DIR / "daily"
GAP_DIR = DATA_DIR / "gap_levels"


# ═══════════════════════════════════════
#  일봉 로드
# ═══════════════════════════════════════

def _load_daily(code: str) -> Optional[pd.DataFrame]:
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
    except Exception as e:
        logger.debug(f"{code} daily 로드 실패: {e}")
        return None


# ═══════════════════════════════════════
#  갭 탐지
# ═══════════════════════════════════════

def detect_gap(
    code: str,
    today_open: int = None,
    target_date: date = None,
    gap_threshold: float = 0.5,
) -> Optional[dict]:
    """단일 종목 갭 탐지

    Args:
        code: 종목 코드
        today_open: 당일 시가 (None이면 일봉에서 추출)
        target_date: 기준 날짜
        gap_threshold: 갭 판정 기준 (%)

    Returns:
        {code, date, gap_type, gap_pct, gap_zone: {top, bottom},
         strength} or None
    """
    if target_date is None:
        target_date = date.today()

    df = _load_daily(code)
    if df is None or len(df) < 2:
        return None

    # 전일 데이터
    before = df[df.index.date < target_date]
    if len(before) == 0:
        return None
    prev_day = before.iloc[-1]
    prev_close = int(prev_day["close"])

    if prev_close <= 0:
        return None

    # 당일 시가
    if today_open is None:
        today_data = df[df.index.date == target_date]
        if len(today_data) == 0:
            return None
        today_open = int(today_data.iloc[0]["open"])

    if today_open <= 0:
        return None

    # 갭 계산
    gap_pct = (today_open - prev_close) / prev_close * 100

    if abs(gap_pct) < gap_threshold:
        return None

    if gap_pct > 0:
        gap_type = "gap_up"
        gap_zone = {"top": today_open, "bottom": prev_close}
    else:
        gap_type = "gap_down"
        gap_zone = {"top": prev_close, "bottom": today_open}

    # 강도: 1% 미만 = normal, 1% 이상 = strong
    strength = "strong" if abs(gap_pct) >= 1.0 else "normal"

    return {
        "code": code,
        "date": target_date.isoformat(),
        "gap_type": gap_type,
        "gap_pct": round(gap_pct, 2),
        "gap_zone": gap_zone,
        "prev_close": prev_close,
        "today_open": today_open,
        "strength": strength,
    }


def detect_gap_realtime(
    code: str,
    today_open: int,
    prev_close: int,
    target_date: date = None,
    gap_threshold: float = 0.5,
) -> Optional[dict]:
    """실시간 갭 탐지 (일봉 파일 불필요, 가격 직접 입력)

    장 시작 직후 KIS API에서 시가/전일종가를 받아 직접 판정.
    """
    if target_date is None:
        target_date = date.today()

    if prev_close <= 0 or today_open <= 0:
        return None

    gap_pct = (today_open - prev_close) / prev_close * 100

    if abs(gap_pct) < gap_threshold:
        return None

    if gap_pct > 0:
        gap_type = "gap_up"
        gap_zone = {"top": today_open, "bottom": prev_close}
    else:
        gap_type = "gap_down"
        gap_zone = {"top": prev_close, "bottom": today_open}

    strength = "strong" if abs(gap_pct) >= 1.0 else "normal"

    return {
        "code": code,
        "date": target_date.isoformat(),
        "gap_type": gap_type,
        "gap_pct": round(gap_pct, 2),
        "gap_zone": gap_zone,
        "prev_close": prev_close,
        "today_open": today_open,
        "strength": strength,
    }


# ═══════════════════════════════════════
#  일괄 실행 + 저장
# ═══════════════════════════════════════

def run_gap_detection(
    codes: list[str] = None,
    target_date: date = None,
) -> dict:
    """전 종목 갭 탐지 + JSON 저장 (일봉 기반, 장전 실행용)"""
    if target_date is None:
        target_date = date.today()

    if codes is None:
        codes = _get_universe_codes()

    results = {}
    for code in codes:
        r = detect_gap(code, target_date=target_date)
        if r:
            results[code] = r

    # 저장
    GAP_DIR.mkdir(parents=True, exist_ok=True)
    out_path = GAP_DIR / f"{target_date.isoformat()}.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    logger.info(f"[GAP] {len(results)}종목 갭 탐지 저장: {out_path}")

    return results


def load_gap_levels(target_date: date = None) -> dict:
    """저장된 갭 데이터 로드"""
    if target_date is None:
        target_date = date.today()
    fpath = GAP_DIR / f"{target_date.isoformat()}.json"
    if not fpath.exists():
        return {}
    try:
        with open(fpath, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


# ═══════════════════════════════════════
#  스윙봇 연동
# ═══════════════════════════════════════

def get_gap_score_adjustment(code: str, current_price: int,
                             target_date: date = None) -> tuple[float, str]:
    """갭 기반 매수 신뢰도 보정

    - 갭업 종목: 현재가가 갭 영역 안에 있으면 지지 존 → +3점 (strong +5점)
    - 갭다운 종목: 현재가가 갭 영역 안에 있으면 저항 존 → -3점
    """
    data = load_gap_levels(target_date)
    info = data.get(code)
    if not info:
        return 0.0, ""

    zone = info.get("gap_zone", {})
    top = zone.get("top", 0)
    bottom = zone.get("bottom", 0)

    if top <= 0 or bottom <= 0:
        return 0.0, ""

    # 현재가가 갭 영역 내 또는 근접(0.5% 이내)에 있는지
    zone_range = top - bottom
    margin = max(zone_range * 0.5, current_price * 0.005)

    in_zone = (bottom - margin) <= current_price <= (top + margin)
    if not in_zone:
        return 0.0, ""

    gap_type = info.get("gap_type", "")
    strength = info.get("strength", "normal")
    gap_pct = info.get("gap_pct", 0)

    if gap_type == "gap_up":
        # 갭업 영역 = 지지 존 (되돌림 시 반등 기대)
        bonus = 5.0 if strength == "strong" else 3.0
        reason = f"GAP 지지 {bottom:,}~{top:,}({gap_pct:+.1f}%)"
        return bonus, reason
    elif gap_type == "gap_down":
        # 갭다운 영역 = 저항 존 (되돌림 시 저항 기대)
        reason = f"GAP 저항 {bottom:,}~{top:,}({gap_pct:+.1f}%)"
        return -3.0, reason

    return 0.0, ""


def get_gap_target_levels(code: str, current_price: int,
                          target_date: date = None) -> dict:
    """갭 기반 손절/타겟 후보

    Returns:
        {gap_sl: 갭 하단 (이탈 시 지지 실패),
         gap_tp: 갭 상단 (저항 존),
         gap_type, gap_strength}
    """
    data = load_gap_levels(target_date)
    info = data.get(code)
    if not info:
        return {}

    zone = info.get("gap_zone", {})
    top = zone.get("top", 0)
    bottom = zone.get("bottom", 0)

    if top <= 0 or bottom <= 0:
        return {}

    result = {
        "gap_type": info.get("gap_type", ""),
        "gap_strength": info.get("strength", "normal"),
        "gap_pct": info.get("gap_pct", 0),
    }

    if info["gap_type"] == "gap_up":
        # 갭업: 하단 = 지지선 (이탈 시 손절), 상단 = 돌파 대기
        result["gap_support"] = bottom
        result["gap_support_dist_pct"] = round(
            (bottom - current_price) / current_price * 100, 2
        )
    elif info["gap_type"] == "gap_down":
        # 갭다운: 상단 = 저항선 (접근 시 익절)
        result["gap_resist"] = top
        result["gap_resist_dist_pct"] = round(
            (top - current_price) / current_price * 100, 2
        )

    return result


# ═══════════════════════════════════════
#  프리미엄 레벨 통합
# ═══════════════════════════════════════

def merge_gap_to_premium_levels(pl: dict, gap_data: dict) -> dict:
    """갭 영역을 프리미엄 레벨에 병합

    Args:
        pl: premium_levels dict (종목 단위)
        gap_data: gap_support dict (종목 단위)
    """
    if not gap_data:
        return pl

    zone = gap_data.get("gap_zone", {})
    top = zone.get("top", 0)
    bottom = zone.get("bottom", 0)
    gap_type = gap_data.get("gap_type", "")
    strength = gap_data.get("strength", "normal")

    if top <= 0 or bottom <= 0:
        return pl

    existing_prices = {lv["price"] for lv in pl.get("levels", [])}

    strength_tag = " S" if strength == "strong" else ""

    if gap_type == "gap_up":
        # 갭 하단 = 지지
        if bottom not in existing_prices:
            pl["levels"].append({
                "name": f"GAP지지{strength_tag}",
                "price": bottom,
                "type": "support",
            })
            existing_prices.add(bottom)
        # 갭 상단 = 위에서 올라간 시작점 (참고용)
        if top not in existing_prices:
            pl["levels"].append({
                "name": f"GAP상단{strength_tag}",
                "price": top,
                "type": "resistance",
            })
            existing_prices.add(top)
    elif gap_type == "gap_down":
        # 갭 상단 = 저항
        if top not in existing_prices:
            pl["levels"].append({
                "name": f"GAP저항{strength_tag}",
                "price": top,
                "type": "resistance",
            })
            existing_prices.add(top)
        # 갭 하단 = 아래로 내려간 시작점 (참고용)
        if bottom not in existing_prices:
            pl["levels"].append({
                "name": f"GAP하단{strength_tag}",
                "price": bottom,
                "type": "support",
            })
            existing_prices.add(bottom)

    return pl


def format_telegram_gap(code: str, name: str, current_price: int,
                        target_date: date = None) -> str:
    """텔레그램 갭 레벨 1줄 포맷

    예: 한국콜마 72,400 | GAP 지지 71,200~72,400(+1.2%) strong
    """
    data = load_gap_levels(target_date)
    info = data.get(code)
    if not info:
        return ""

    zone = info.get("gap_zone", {})
    top = zone.get("top", 0)
    bottom = zone.get("bottom", 0)
    gap_type = info.get("gap_type", "")
    gap_pct = info.get("gap_pct", 0)
    strength = info.get("strength", "normal")

    if top <= 0 or bottom <= 0:
        return ""

    strength_tag = " strong" if strength == "strong" else ""
    type_label = "지지" if gap_type == "gap_up" else "저항"

    return (
        f"{name} {current_price:,} | "
        f"GAP {type_label} {bottom:,}~{top:,}({gap_pct:+.1f}%){strength_tag}"
    )


def _get_universe_codes() -> list[str]:
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

    if len(sys.argv) > 1 and sys.argv[1] == "detect":
        code = sys.argv[2] if len(sys.argv) > 2 else "005930"
        # 최근 5거래일 갭 확인
        df = _load_daily(code)
        if df is not None and len(df) >= 6:
            last5 = df.tail(6)
            dates = last5.index.tolist()
            print(f"\n=== {code} 최근 5일 갭 ===")
            for i in range(1, len(dates)):
                prev_close = int(last5.loc[dates[i-1], "close"])
                today_open = int(last5.loc[dates[i], "open"])
                gap_pct = (today_open - prev_close) / prev_close * 100
                d_str = dates[i].strftime("%m-%d")
                if abs(gap_pct) >= 0.5:
                    gap_type = "UP" if gap_pct > 0 else "DN"
                    print(f"  {d_str}: {gap_type} {gap_pct:+.1f}% "
                          f"(전일종:{prev_close:,} → 시가:{today_open:,})")
                else:
                    print(f"  {d_str}: 갭 없음 ({gap_pct:+.2f}%)")
    else:
        print("갭 레벨 일괄 탐지...")
        results = run_gap_detection()
        up = sum(1 for r in results.values() if r["gap_type"] == "gap_up")
        down = sum(1 for r in results.values() if r["gap_type"] == "gap_down")
        strong = sum(1 for r in results.values() if r["strength"] == "strong")
        print(f"결과: {len(results)}종목 - 갭업:{up} 갭다운:{down} (strong:{strong})")
