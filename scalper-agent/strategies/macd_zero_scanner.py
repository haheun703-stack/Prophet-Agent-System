# -*- coding: utf-8 -*-
"""
MACD 제로선 크로스 전략 스캐너
==============================
백테스트 검증 결과 (1535종목 × 478일):
  E2: MACD음→0 크로스 + 수급+거래량 폭발 → 49.5% WR, 1.37R, +1.26% avg

2단계 프로세스:
  Phase1 (감시) — MACD 음→0선 골든크로스 + 수급 폭발 → 감시목록 등록
  Phase2 (진입) — 감시 종목이 고점 대비 -3%~-15% 조정 → 진입 시그널

스케줄:
  16:35 → Phase1 스캔 (장마감 후 일봉 확정)
  09:30 → Phase2 체크 (장중 조정 감지)
"""

import json
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

logger = logging.getLogger("BH.MACDZero")

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data_store"
DAILY_DIR = DATA_DIR / "daily"
FLOW_DIR = DATA_DIR / "flow"
WATCHLIST_PATH = DATA_DIR / "macd_watchlist.json"


# ═══════════════════════════════════════════
#  MACD 계산
# ═══════════════════════════════════════════

def _calc_macd(close: np.ndarray, fast=12, slow=26, signal=9):
    s = pd.Series(close)
    ema_f = s.ewm(span=fast, adjust=False).mean()
    ema_s = s.ewm(span=slow, adjust=False).mean()
    macd = ema_f - ema_s
    sig = macd.ewm(span=signal, adjust=False).mean()
    return macd.values, sig.values, (macd - sig).values


# ═══════════════════════════════════════════
#  Phase1: 감시 종목 스캔
# ═══════════════════════════════════════════

def scan_phase1(codes: List[str] = None) -> List[Dict]:
    """
    MACD 음→0선 골든크로스 + 수급/거래량 폭발 종목 스캔.
    장 마감 후(16:35) 실행.

    Returns: [{code, name, sector, cross_date, macd_ratio, flow_ratio,
               vol_ratio, peak_price, cross_price}, ...]
    """
    # 유니버스
    uni_path = DATA_DIR / "universe.json"
    if not uni_path.exists():
        return []
    with open(uni_path, "r", encoding="utf-8") as f:
        universe = json.load(f)

    if codes is None:
        codes = [c for c in universe.keys()
                 if c[-1] not in {"5", "7", "8", "9", "K", "L"}]

    results = []

    for code in codes:
        daily_path = DAILY_DIR / f"{code}.csv"
        if not daily_path.exists():
            continue

        try:
            df = pd.read_csv(daily_path, index_col=0, parse_dates=True)
        except Exception:
            continue

        if len(df) < 40:
            continue

        close = df["종가"].values.astype(float)
        high = df["고가"].values.astype(float)
        low = df["저가"].values.astype(float)
        volume = df["거래량"].values.astype(float)
        dates = df.index

        macd, sig, hist = _calc_macd(close)
        atr_14 = pd.Series(high - low).rolling(14).mean().values
        vol_ma20 = pd.Series(volume).rolling(20).mean().values

        # 최근 봉(오늘) 기준 체크
        i = len(close) - 1
        if i < 35:
            continue

        atr_val = atr_14[i] if atr_14[i] > 0 else 1

        # ── 조건1: MACD 0선 근처 (ATR의 30% 이내) ──
        macd_ratio = abs(macd[i]) / atr_val
        if macd_ratio > 0.3:
            continue

        # ── 조건2: 골든크로스 (최근 3일 내 발생) ──
        cross_found = False
        cross_day = i
        for d in range(i, max(i - 3, 0), -1):
            if d > 0 and hist[d - 1] <= 0 and hist[d] > 0:
                cross_found = True
                cross_day = d
                break
        if not cross_found:
            continue

        # ── 조건3: MACD가 아래에서 올라오는 중 ──
        if not (macd[i] > macd[max(i - 5, 0)]):
            continue
        # 5일전 MACD가 음수였어야 함
        if macd[max(i - 5, 0)] >= 0:
            continue

        # ── 조건4: 수급 폭발 ──
        flow_path = FLOW_DIR / f"{code}_investor.csv"
        if not flow_path.exists():
            continue
        try:
            ff = pd.read_csv(flow_path, index_col=0, parse_dates=True)
            ff["combo"] = ff["기관_수량"].astype(float) + ff["외국인_수량"].astype(float)
            flow = ff["combo"].reindex(df.index).fillna(0).values
        except Exception:
            continue

        recent_5_flow = flow[i - 4:i + 1].sum()
        avg_20_flow = np.mean(np.abs(flow[max(i - 24, 0):i - 4]))
        if avg_20_flow <= 0 or recent_5_flow <= 0:
            continue
        flow_ratio = recent_5_flow / avg_20_flow
        if flow_ratio < 2.0:
            continue

        # ── 조건5: 거래량 폭발 ──
        recent_5_vol = volume[i - 4:i + 1].mean()
        avg_20_vol = vol_ma20[max(i - 5, 0)]
        if np.isnan(avg_20_vol) or avg_20_vol <= 0:
            continue
        vol_ratio = recent_5_vol / avg_20_vol
        if vol_ratio < 1.5:
            continue

        # ── 거래대금 필터 (10억+) ──
        avg_value = (volume[-5:] * close[-5:]).mean()
        if avg_value < 1_000_000_000:
            continue

        info = universe.get(code, {})
        name = info.get("name", code) if isinstance(info, dict) else code
        sector = info.get("sector", "기타") if isinstance(info, dict) else "기타"

        results.append({
            "code": code,
            "name": name,
            "sector": sector,
            "cross_date": str(dates[cross_day].date()),
            "cross_price": int(close[cross_day]),
            "current_price": int(close[i]),
            "peak_price": int(max(close[cross_day:i + 1])),
            "macd_ratio": round(macd_ratio, 3),
            "flow_ratio": round(flow_ratio, 1),
            "vol_ratio": round(vol_ratio, 1),
        })

    results.sort(key=lambda x: -x["flow_ratio"])
    return results


# ═══════════════════════════════════════════
#  Phase2: 조정 진입 시그널 체크
# ═══════════════════════════════════════════

def check_phase2(watchlist: List[Dict] = None) -> List[Dict]:
    """
    감시 종목의 조정 진입 여부 체크.
    장중(09:30~) 또는 장 마감 후 실행.

    Returns: [{...watchlist_item, drawdown, entry_price, sl, tp, status}, ...]
    """
    if watchlist is None:
        watchlist = load_watchlist()

    if not watchlist:
        return []

    signals = []
    today = datetime.now().date()

    for item in watchlist:
        code = item["code"]
        daily_path = DAILY_DIR / f"{code}.csv"
        if not daily_path.exists():
            continue

        try:
            df = pd.read_csv(daily_path, index_col=0, parse_dates=True)
        except Exception:
            continue

        if len(df) < 20:
            continue

        close = df["종가"].values.astype(float)
        high = df["고가"].values.astype(float)
        low = df["저가"].values.astype(float)

        current = close[-1]

        # 크로스 이후 고점 갱신
        cross_date = item.get("cross_date", "")
        if cross_date:
            mask = df.index >= pd.Timestamp(cross_date)
            if mask.any():
                peak = float(high[mask].max())
            else:
                peak = float(item.get("peak_price", current))
        else:
            peak = float(item.get("peak_price", current))

        drawdown = (current / peak - 1) * 100

        # ATR 기반 SL/TP
        tr = np.maximum(high[-14:] - low[-14:], 0)
        atr = float(tr.mean())

        # 감시 시작일로부터 15영업일 초과 → 만료
        try:
            cross_dt = datetime.strptime(cross_date, "%Y-%m-%d").date()
            days_elapsed = (today - cross_dt).days
        except Exception:
            days_elapsed = 0

        status = "감시중"

        if days_elapsed > 25:
            status = "만료"
        elif -15 < drawdown < -3:
            # 조정 진입 구간!
            status = "진입"
            entry = int(current)
            sl = int(entry - atr * 1.0)
            tp = int(entry + atr * 2.0)  # 2R

            signals.append({
                **item,
                "status": status,
                "drawdown": round(drawdown, 2),
                "peak_since_cross": int(peak),
                "entry_price": entry,
                "sl": sl,
                "tp": tp,
                "days_elapsed": days_elapsed,
            })
        elif drawdown < -15:
            status = "급락_제외"
        else:
            # 아직 조정 안 옴 — 계속 감시
            pass

        # 감시 중인 종목도 기록 (상태 업데이트용)
        if status != "진입":
            item["status"] = status
            item["peak_since_cross"] = int(peak)
            item["drawdown"] = round(drawdown, 2)
            item["days_elapsed"] = days_elapsed

    return signals


# ═══════════════════════════════════════════
#  감시 목록 저장/로드
# ═══════════════════════════════════════════

def load_watchlist() -> List[Dict]:
    if not WATCHLIST_PATH.exists():
        return []
    with open(WATCHLIST_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data.get("watchlist", [])


def save_watchlist(watchlist: List[Dict], new_phase1: List[Dict] = None):
    """감시 목록 저장 (신규 추가 + 기존 유지)"""
    existing_codes = {w["code"] for w in watchlist}

    # 신규 Phase1 종목 추가 (중복 방지)
    if new_phase1:
        for item in new_phase1:
            if item["code"] not in existing_codes:
                item["status"] = "감시중"
                item["added_at"] = datetime.now().isoformat()
                watchlist.append(item)
                existing_codes.add(item["code"])

    # 만료/급락 제거
    watchlist = [w for w in watchlist
                 if w.get("status") not in ("만료", "급락_제외", "진입완료")]

    data = {
        "updated_at": datetime.now().isoformat(),
        "count": len(watchlist),
        "watchlist": watchlist,
    }

    with open(WATCHLIST_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    return watchlist


# ═══════════════════════════════════════════
#  통합 실행
# ═══════════════════════════════════════════

def run_daily_scan() -> Dict:
    """
    매일 장 마감 후 실행:
      1) Phase1: 신규 MACD 크로스 종목 스캔
      2) Phase2: 기존 감시 종목 조정 진입 체크
      3) 감시 목록 갱신

    Returns: {phase1_new, phase2_entries, watchlist_count}
    """
    print("=" * 50)
    print("  MACD 제로선 크로스 스캐너")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 50)

    # Phase1: 신규 크로스 스캔
    print("\n[Phase1] MACD 음→0선 골든크로스 스캔...")
    new_signals = scan_phase1()
    print(f"  → 신규 감시 후보: {len(new_signals)}종목")

    # 기존 감시 목록 로드
    watchlist = load_watchlist()
    print(f"  기존 감시 종목: {len(watchlist)}개")

    # Phase2: 조정 진입 체크
    print("\n[Phase2] 조정 진입 시그널 체크...")
    entries = check_phase2(watchlist)
    print(f"  → 진입 시그널: {len(entries)}종목")

    # 감시 목록 갱신
    watchlist = save_watchlist(watchlist, new_signals)
    print(f"  갱신 후 감시 종목: {len(watchlist)}개")

    result = {
        "scan_time": datetime.now().isoformat(),
        "phase1_new": new_signals,
        "phase2_entries": entries,
        "watchlist_count": len(watchlist),
    }

    # 결과 요약 출력
    if new_signals:
        print(f"\n  [신규 감시 종목]")
        for s in new_signals[:10]:
            print(f"    {s['name']:12s}({s['code']}) | "
                  f"수급x{s['flow_ratio']:.0f} 거래량x{s['vol_ratio']:.1f} | "
                  f"MACD비율:{s['macd_ratio']:.3f}")

    if entries:
        print(f"\n  [진입 시그널]")
        for e in entries:
            print(f"    {e['name']:12s}({e['code']}) | "
                  f"고점 대비 {e['drawdown']:+.1f}% | "
                  f"진입:{e['entry_price']:,} SL:{e['sl']:,} TP:{e['tp']:,}")

    return result


# ═══════════════════════════════════════════
#  텔레그램 메시지 포맷
# ═══════════════════════════════════════════

def format_telegram_message(result: Dict) -> str:
    """스캔 결과 → 텔레그램 메시지"""
    lines = [
        "━" * 22,
        "MACD 제로선 크로스 스캐너",
        f"{result.get('scan_time', '')[:16]}",
        "━" * 22,
    ]

    # Phase1: 신규 감시 종목
    new_sigs = result.get("phase1_new", [])
    lines.append(f"\n[Phase1] 신규 감시 등록: {len(new_sigs)}종목")

    if new_sigs:
        for s in new_sigs[:8]:
            lines.append(
                f"  {s['name']}({s['code']})"
            )
            lines.append(
                f"    수급x{s['flow_ratio']:.0f} | "
                f"거래량x{s['vol_ratio']:.1f} | "
                f"현재가:{s['current_price']:,}"
            )

    # Phase2: 진입 시그널
    entries = result.get("phase2_entries", [])
    lines.append(f"\n[Phase2] 조정 진입 시그널: {len(entries)}종목")

    if entries:
        for e in entries:
            lines.append(
                f"  {e['name']}({e['code']}) 조정:{e['drawdown']:+.1f}%"
            )
            lines.append(
                f"    진입:{e['entry_price']:,} "
                f"SL:{e['sl']:,} TP:{e['tp']:,}"
            )

    # 감시 목록 현황
    lines.append(f"\n감시 목록: {result.get('watchlist_count', 0)}종목")

    if not new_sigs and not entries:
        lines.append("오늘은 시그널 없음")

    lines.append("━" * 22)
    return "\n".join(lines)


if __name__ == "__main__":
    import sys
    sys.stdout.reconfigure(encoding="utf-8")
    result = run_daily_scan()
    print("\n" + format_telegram_message(result))
