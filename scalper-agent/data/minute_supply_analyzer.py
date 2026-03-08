# -*- coding: utf-8 -*-
"""
분봉 수급 분석기 (Minute Supply Analyzer)
=========================================
5분봉 데이터 수집 완료 후 전 종목 수급 분석:
  1. 거래량 비율: 당일 평균 거래량 / 20일 평균 거래량
  2. 장 후반 가속도: 14~15시 거래량 / 09~10시 거래량
  3. OBV 방향: 양봉+거래량 누적 vs 음봉+거래량 누적
  4. 에너지(가격×거래량): 종가 기준 거래대금 크기

분류:
  - 수급 폭발 (3x+): 평소의 3배 이상 거래량
  - 수급 강함 (2x+): 평소의 2배 이상
  - 수급 유입 (1.5x+): 평소의 1.5배 이상
  - 그 외는 무시
"""

import logging
from pathlib import Path
from datetime import date, datetime
from dataclasses import dataclass
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)

DATA_DIR = Path(__file__).resolve().parent.parent / "data_store"
FIVEMIN_DIR = DATA_DIR / "5min"


@dataclass
class SupplySignal:
    code: str
    name: str
    close: float
    change_pct: float           # 당일 등락률 (%)
    vol_ratio: float            # 당일 평균 거래량 / 20일 평균 거래량
    afternoon_accel: float      # 후반(14~15시) / 전반(09~10시) 거래량 비율
    obv_direction: str          # "UP" / "DOWN" / "FLAT"
    energy: float               # 종가 × 당일 총 거래량 (거래대금, 억원)
    category: str               # "폭발" / "강함" / "유입"
    today_volume: int           # 당일 총 거래량
    avg_20d_volume: int         # 20일 평균 일 거래량


def analyze_minute_supply(target_date: Optional[date] = None,
                          universe: Optional[dict] = None,
                          top_n: int = 20) -> list[SupplySignal]:
    """전 종목 5분봉 수급 분석.

    Args:
        target_date: 분석 대상일 (None이면 가장 최근 거래일)
        universe: {code: (name, ...)} 유니버스 dict
        top_n: 상위 N종목만 반환

    Returns:
        SupplySignal 리스트 (vol_ratio 내림차순)
    """
    if not FIVEMIN_DIR.exists():
        logger.warning(f"5분봉 디렉토리 없음: {FIVEMIN_DIR}")
        return []

    # 유니버스 이름 매핑
    name_map = {}
    if universe:
        for code, info in universe.items():
            if isinstance(info, (list, tuple)) and len(info) > 0:
                name_map[code] = info[0]
            elif isinstance(info, dict):
                name_map[code] = info.get("name", code)

    csv_files = list(FIVEMIN_DIR.glob("*.csv"))
    if not csv_files:
        logger.warning("5분봉 CSV 파일 없음")
        return []

    signals = []
    errors = 0

    for csv_path in csv_files:
        code = csv_path.stem
        if code.startswith("_"):
            continue

        try:
            df = pd.read_csv(csv_path, index_col=0, parse_dates=True)
            if df.empty or len(df) < 20:
                continue

            # 날짜 인덱스 확인
            if not isinstance(df.index, pd.DatetimeIndex):
                df.index = pd.to_datetime(df.index)

            # 대상일 결정
            available_dates = df.index.normalize().unique().sort_values()
            if target_date:
                td = pd.Timestamp(target_date)
            else:
                td = available_dates[-1]

            # 당일 데이터
            today_mask = df.index.normalize() == td
            df_today = df[today_mask]
            if len(df_today) < 5:  # 최소 5봉 이상
                continue

            # 20일 평균 거래량 (당일 제외 최근 20거래일)
            past_dates = available_dates[available_dates < td]
            if len(past_dates) < 3:  # 최소 3일 데이터
                continue

            past_20 = past_dates[-20:]  # 최대 20일
            past_mask = df.index.normalize().isin(past_20)
            df_past = df[past_mask]

            if df_past.empty or df_past["volume"].sum() == 0:
                continue

            # === 1. 거래량 비율 ===
            today_total_vol = df_today["volume"].sum()
            # 20일간 일 평균 거래량 (총 거래량 / 일수)
            n_past_days = len(past_20)
            past_daily_avg = df_past["volume"].sum() / n_past_days if n_past_days > 0 else 1
            vol_ratio = today_total_vol / past_daily_avg if past_daily_avg > 0 else 0

            if vol_ratio < 1.5:  # 1.5배 미만은 무시
                continue

            # === 2. 후반 가속도 ===
            morning = df_today.between_time("09:00", "10:00")
            afternoon = df_today.between_time("14:00", "15:30")
            morning_vol = morning["volume"].sum() if len(morning) > 0 else 1
            afternoon_vol = afternoon["volume"].sum() if len(afternoon) > 0 else 0
            afternoon_accel = afternoon_vol / morning_vol if morning_vol > 0 else 0

            # === 3. OBV 방향 ===
            price_changes = df_today["close"].diff()
            obv = 0
            for i in range(1, len(df_today)):
                pc = price_changes.iloc[i]
                vol = df_today["volume"].iloc[i]
                if pc > 0:
                    obv += vol
                elif pc < 0:
                    obv -= vol
            # OBV 방향: 당일 전체 기준
            if obv > today_total_vol * 0.1:
                obv_dir = "UP"
            elif obv < -today_total_vol * 0.1:
                obv_dir = "DOWN"
            else:
                obv_dir = "FLAT"

            # === 4. 에너지 (거래대금 억원) ===
            close_price = df_today["close"].iloc[-1]
            energy = (close_price * today_total_vol) / 1e8  # 억원

            # === 5. 등락률 ===
            open_price = df_today["open"].iloc[0]
            change_pct = ((close_price - open_price) / open_price * 100) if open_price > 0 else 0

            # === 분류 ===
            if vol_ratio >= 3.0:
                category = "폭발"
            elif vol_ratio >= 2.0:
                category = "강함"
            else:
                category = "유입"

            name = name_map.get(code, code)

            signals.append(SupplySignal(
                code=code,
                name=name,
                close=close_price,
                change_pct=round(change_pct, 2),
                vol_ratio=round(vol_ratio, 2),
                afternoon_accel=round(afternoon_accel, 2),
                obv_direction=obv_dir,
                energy=round(energy, 1),
                category=category,
                today_volume=int(today_total_vol),
                avg_20d_volume=int(past_daily_avg),
            ))

        except Exception as e:
            errors += 1
            if errors <= 3:
                logger.debug(f"분석 실패 {code}: {e}")

    # 정렬: vol_ratio 내림차순
    signals.sort(key=lambda s: s.vol_ratio, reverse=True)

    logger.info(f"분봉 수급 분석 완료: {len(signals)}종목 필터 통과 (에러 {errors}건)")

    return signals[:top_n]


def format_supply_report(signals: list[SupplySignal], date_str: str = "") -> str:
    """텔레그램용 수급 분석 리포트 생성."""
    if not date_str:
        date_str = date.today().strftime("%m/%d")

    lines = [f"📊 분봉 수급 분석 ({date_str})", "=" * 28, ""]

    # 카테고리별 분류
    explosive = [s for s in signals if s.category == "폭발"]
    strong = [s for s in signals if s.category == "강함"]
    inflow = [s for s in signals if s.category == "유입"]

    obv_emoji = {"UP": "▲", "DOWN": "▼", "FLAT": "─"}
    accel_emoji = lambda a: "🚀" if a >= 1.5 else ("📈" if a >= 1.0 else "📉")

    if explosive:
        lines.append(f"🔴 수급 폭발 (3x+) - {len(explosive)}종목")
        for s in explosive[:7]:
            oe = obv_emoji.get(s.obv_direction, "─")
            ae = accel_emoji(s.afternoon_accel)
            lines.append(
                f"  {s.name}({s.code}) {s.close:,.0f}원 {s.change_pct:+.1f}%"
            )
            lines.append(
                f"    Vol {s.vol_ratio:.1f}x | OBV{oe} | 후반{ae}{s.afternoon_accel:.1f}x"
                f" | {s.energy:.0f}억"
            )
        lines.append("")

    if strong:
        lines.append(f"🟡 수급 강함 (2x+) - {len(strong)}종목")
        for s in strong[:7]:
            oe = obv_emoji.get(s.obv_direction, "─")
            ae = accel_emoji(s.afternoon_accel)
            lines.append(
                f"  {s.name}({s.code}) {s.close:,.0f}원 {s.change_pct:+.1f}%"
            )
            lines.append(
                f"    Vol {s.vol_ratio:.1f}x | OBV{oe} | 후반{ae}{s.afternoon_accel:.1f}x"
                f" | {s.energy:.0f}억"
            )
        lines.append("")

    if inflow:
        lines.append(f"🟢 수급 유입 (1.5x+) - {len(inflow)}종목")
        for s in inflow[:6]:
            oe = obv_emoji.get(s.obv_direction, "─")
            lines.append(
                f"  {s.name}({s.code}) {s.close:,.0f}원 {s.change_pct:+.1f}%"
                f" Vol {s.vol_ratio:.1f}x OBV{oe}"
            )
        lines.append("")

    # 요약 통계
    if signals:
        up_obv = sum(1 for s in signals if s.obv_direction == "UP")
        dn_obv = sum(1 for s in signals if s.obv_direction == "DOWN")
        accel_hi = sum(1 for s in signals if s.afternoon_accel >= 1.5)
        lines.append(
            f"📈 요약: OBV상승 {up_obv}종목 / OBV하락 {dn_obv}종목"
            f" | 후반가속 {accel_hi}종목"
        )
    else:
        lines.append("⬜ 수급 특이 종목 없음")

    return "\n".join(lines)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    try:
        from data.kis_collector import UNIVERSE
    except Exception:
        UNIVERSE = None
    signals = analyze_minute_supply(universe=UNIVERSE)
    print(format_supply_report(signals))
