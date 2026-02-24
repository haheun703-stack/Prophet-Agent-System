# -*- coding: utf-8 -*-
"""
이상거래 감지기 — Volume Spike Scanner
========================================
+30% 폭등 1~3일 전 나타나는 이상 거래량 패턴 5종을 감지한다.

패턴:
  1. VOLUME_SPIKE      — 거래량 2.5배 이상 급증
  2. QUIET_ACCUMULATION — 거래량 3배+ 인데 가격변동 3% 미만 (조용한 매집)
  3. OBV_BREAKOUT      — OBV 신고가 but 주가는 아님 (돈이 먼저 들어옴)
  4. MULTI_DAY_ACCUM   — 3일 연속 거래량 증가 + 주가 상승
  5. BIG_MONEY_INFLOW  — 거래대금 5배 이상 급증 (큰손 진입)

사용법:
  python -m data.volume_scanner              # 전체 유니버스 스캔
  python -m data.volume_scanner --top 20     # 상위 20개만 출력
  python -m data.volume_scanner --telegram   # 텔레그램 알림 전송
"""

import sys
import io
import json
import logging
from pathlib import Path
from datetime import datetime

import numpy as np
import pandas as pd

from data.indicator_calc import IndicatorCalc

logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data_store"
DAILY_DIR = DATA_DIR / "daily"
RESULT_DIR = DATA_DIR / "scan_results"


# ─── 패턴 감지 엔진 ─────────────────────────────────

def detect_patterns(df: pd.DataFrame, code: str, name: str) -> dict:
    """일봉 데이터에서 5가지 이상거래 패턴을 감지한다.

    Args:
        df: 일봉 DataFrame (컬럼: 시가/고가/저가/종가/거래량 또는 open/high/low/close/volume)
        code: 종목코드
        name: 종목명

    Returns:
        패턴 감지 결과 dict (patterns, spike_score 등)
    """
    # 컬럼 표준화
    col_map = {
        "시가": "open", "고가": "high", "저가": "low",
        "종가": "close", "거래량": "volume", "거래대금": "trade_value",
    }
    df = df.rename(columns=col_map)

    required = ["open", "high", "low", "close", "volume"]
    for c in required:
        if c not in df.columns:
            return {"code": code, "name": name, "patterns": [], "spike_score": 0}

    if len(df) < 25:
        return {"code": code, "name": name, "patterns": [], "spike_score": 0}

    close = df["close"].astype(float)
    volume = df["volume"].astype(float)
    high = df["high"].astype(float)
    low = df["low"].astype(float)

    # 거래대금 (없으면 종가*거래량으로 근사)
    if "trade_value" in df.columns:
        trade_val = df["trade_value"].astype(float)
    else:
        trade_val = close * volume

    # 20일 평균 거래량/거래대금
    vol_avg_20 = volume.rolling(20).mean()
    val_avg_20 = trade_val.rolling(20).mean()

    latest_vol = volume.iloc[-1]
    latest_val = trade_val.iloc[-1]
    latest_close = close.iloc[-1]
    prev_close = close.iloc[-2] if len(close) > 1 else latest_close
    price_change = abs(latest_close / prev_close - 1) * 100 if prev_close > 0 else 0

    avg_vol = vol_avg_20.iloc[-1] if not pd.isna(vol_avg_20.iloc[-1]) else 1
    avg_val = val_avg_20.iloc[-1] if not pd.isna(val_avg_20.iloc[-1]) else 1

    vol_ratio = latest_vol / max(avg_vol, 1)
    val_ratio = latest_val / max(avg_val, 1)

    patterns = []
    score = 0

    # ─── 패턴 1: VOLUME_SPIKE (거래량 2.5배+) ───
    if vol_ratio >= 2.5:
        pts = min(30, int((vol_ratio - 2.5) * 10) + 15)
        patterns.append({
            "type": "VOLUME_SPIKE",
            "description": f"거래량 {vol_ratio:.1f}배 급증",
            "score": pts,
        })
        score += pts

    # ─── 패턴 2: QUIET_ACCUMULATION (3배+ 거래량 but 가격 3% 미만) ───
    if vol_ratio >= 3.0 and price_change <= 3.0:
        pts = min(40, int((vol_ratio - 3.0) * 8) + 25)
        patterns.append({
            "type": "QUIET_ACCUMULATION",
            "description": f"조용한 매집 — 거래량 {vol_ratio:.1f}배, 가격변동 {price_change:.1f}%",
            "score": pts,
        })
        score += pts

    # ─── 패턴 3: OBV_BREAKOUT (OBV 신고가, 주가는 아님) ───
    obv_series = IndicatorCalc.obv(close, volume)
    obv_20_high = obv_series.tail(20).max()
    price_20_high = close.tail(20).max()

    obv_at_high = obv_series.iloc[-1] >= obv_20_high * 0.98
    price_not_high = close.iloc[-1] < price_20_high * 0.97

    if obv_at_high and price_not_high:
        pts = 20
        patterns.append({
            "type": "OBV_BREAKOUT",
            "description": f"OBV 신고가 (주가 미돌파) — 자금 선행 유입",
            "score": pts,
        })
        score += pts

    # ─── 패턴 4: MULTI_DAY_ACCUM (3일 연속 거래량 증가 + 주가 상승) ───
    if len(volume) >= 4:
        vol_3d = volume.iloc[-3:]
        close_3d = close.iloc[-3:]
        vol_increasing = all(vol_3d.diff().dropna() > 0)
        price_rising = close_3d.iloc[-1] > close_3d.iloc[0]

        if vol_increasing and price_rising:
            pts = 20
            patterns.append({
                "type": "MULTI_DAY_ACCUM",
                "description": f"3일 연속 거래량 증가 + 주가 상승",
                "score": pts,
            })
            score += pts

    # ─── 패턴 5: BIG_MONEY_INFLOW (거래대금 5배+) ───
    if val_ratio >= 5.0:
        pts = min(35, int((val_ratio - 5.0) * 5) + 20)
        patterns.append({
            "type": "BIG_MONEY_INFLOW",
            "description": f"거래대금 {val_ratio:.1f}배 급증 — 큰손 진입",
            "score": pts,
        })
        score += pts

    # 최종 점수 (100 cap)
    spike_score = min(100, score)

    return {
        "code": code,
        "name": name,
        "patterns": patterns,
        "spike_score": spike_score,
        "vol_ratio": round(vol_ratio, 2),
        "val_ratio": round(val_ratio, 2),
        "price_change": round(price_change, 2),
        "close": int(latest_close),
        "volume": int(latest_vol),
        "obv_trend": IndicatorCalc.obv_trend(close, volume),
        "detected_at": datetime.now().strftime("%Y-%m-%d %H:%M"),
    }


# ─── 스캐너 ─────────────────────────────────────────

def scan_universe(top_n: int = 30) -> list:
    """유니버스 전체를 스캔하여 이상거래 종목 추출

    Returns:
        spike_score 내림차순 정렬된 결과 리스트
    """
    from data.universe_builder import load_universe

    universe = load_universe()
    if not universe:
        logger.error("유니버스 없음 — python -m data.universe_builder --build-only 실행 필요")
        return []

    print(f"\n🔍 이상거래 감지기 — {len(universe)}종목 스캔")
    print("=" * 60)

    results = []
    scanned = 0
    skipped = 0

    for code, info in universe.items():
        name = info.get("name", code) if isinstance(info, dict) else info[0]
        daily_file = DAILY_DIR / f"{code}.csv"

        if not daily_file.exists():
            skipped += 1
            continue

        try:
            df = pd.read_csv(daily_file, index_col=0, parse_dates=True)
            if len(df) < 25:
                skipped += 1
                continue

            result = detect_patterns(df, code, name)
            if result["patterns"]:
                results.append(result)

            scanned += 1

        except Exception as e:
            logger.warning(f"  스캔 실패 {code} {name}: {e}")
            skipped += 1

    # 점수 내림차순
    results.sort(key=lambda x: -x["spike_score"])
    results = results[:top_n]

    print(f"  스캔: {scanned}종목 | 건너뜀: {skipped}종목")
    print(f"  이상거래 감지: {len(results)}종목")

    return results


def save_results(results: list) -> Path:
    """스캔 결과 저장"""
    RESULT_DIR.mkdir(parents=True, exist_ok=True)

    today = datetime.now().strftime("%Y%m%d")

    # 전체 결과
    full_path = RESULT_DIR / f"volume_spikes_{today}.json"
    with open(full_path, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    # extra_universe.json — QUIET_ACCUMULATION 종목만 (파이프라인 연동용)
    quiet_stocks = [
        r for r in results
        if any(p["type"] == "QUIET_ACCUMULATION" for p in r["patterns"])
    ]
    extra_path = DATA_DIR / "extra_universe.json"
    extra = {
        r["code"]: {
            "name": r["name"],
            "source": "VOL_SPIKE",
            "spike_score": r["spike_score"],
            "patterns": [p["type"] for p in r["patterns"]],
        }
        for r in quiet_stocks
    }
    with open(extra_path, "w", encoding="utf-8") as f:
        json.dump(extra, f, ensure_ascii=False, indent=2)

    print(f"\n  💾 전체 결과: {full_path}")
    print(f"  💾 조용한 매집: {extra_path} ({len(extra)}종목)")

    return full_path


# ─── 출력 ─────────────────────────────────────────

def format_results(results: list) -> str:
    """결과를 텔레그램/콘솔용 텍스트로 포맷"""
    if not results:
        return "이상거래 감지 없음"

    lines = []
    lines.append(f"🔍 이상거래 감지 리포트")
    lines.append(f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    lines.append(f"━━━━━━━━━━━━━━━━━━")

    for i, r in enumerate(results[:15], 1):
        # 패턴 아이콘
        pattern_icons = []
        for p in r["patterns"]:
            icons = {
                "VOLUME_SPIKE": "📊",
                "QUIET_ACCUMULATION": "🤫",
                "OBV_BREAKOUT": "💰",
                "MULTI_DAY_ACCUM": "📈",
                "BIG_MONEY_INFLOW": "🐋",
            }
            pattern_icons.append(icons.get(p["type"], "⚡"))

        icon_str = "".join(pattern_icons)
        lines.append(f"\n{i}. {r['name']}({r['code']}) {icon_str}")
        lines.append(f"   점수: {r['spike_score']}점 | 종가: {r['close']:,}원")
        lines.append(f"   거래량: {r['vol_ratio']}배 | 가격변동: {r['price_change']}%")

        for p in r["patterns"]:
            lines.append(f"   → {p['description']}")

    lines.append(f"\n━━━━━━━━━━━━━━━━━━")
    lines.append(f"🤫 = 조용한매집 | 🐋 = 큰손 | 💰 = OBV선행")

    return "\n".join(lines)


def print_results(results: list):
    """콘솔 출력"""
    print(format_results(results))


# ─── CLI ─────────────────────────────────────────

if __name__ == "__main__":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
    logging.basicConfig(level=logging.WARNING)

    import argparse
    parser = argparse.ArgumentParser(description="이상거래 감지기")
    parser.add_argument("--top", type=int, default=30, help="상위 N개 (기본 30)")
    parser.add_argument("--telegram", action="store_true", help="텔레그램 알림")
    args = parser.parse_args()

    results = scan_universe(top_n=args.top)
    save_results(results)
    print_results(results)

    if args.telegram and results:
        try:
            from bot.telegram_bot import send_message
            msg = format_results(results)
            send_message(msg)
            print("\n📨 텔레그램 전송 완료")
        except Exception as e:
            print(f"\n⚠️ 텔레그램 전송 실패: {e}")
