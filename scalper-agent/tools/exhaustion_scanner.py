# -*- coding: utf-8 -*-
"""
미래에셋증권우 패턴 스캐너 v2 — 진짜 매물소진 → 반등 시작 종목
================================================================
v1 문제: 거래량만 줄면 다 잡음 → 관심 없는 횡보 종목 대량 혼입
v2 핵심 변경:
  1. 스토캐스틱 필수: %K > %D 크로스 or %K 바닥반전 (20 이하에서 올라옴)
  2. 거래량 반등 시작: 오늘 vol > 어제 vol (살아나는 신호)
  3. 하락 추세 제외: 20D 하락 > -15% 제거 (진짜 하락중인 건 매물소진 아님)
  4. 일평균 거래대금 필터: 최소 10억원 (유동성 없으면 무의미)
  5. 수급 데이터 연동: 기관/외인 순매수 확인 (있으면)
"""
import sys, io, os, json
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
os.chdir(os.path.join(os.path.dirname(__file__), ".."))

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime

DATA_DIR = Path("data_store")
DAILY_DIR = DATA_DIR / "daily"
FLOW_DIR = DATA_DIR / "flow"
SHORT_DIR = DATA_DIR / "short"

with open(DATA_DIR / "universe.json", "r", encoding="utf-8") as f:
    uni = json.load(f)

today = datetime.now().strftime("%Y-%m-%d")
print("=" * 95)
print("  매물소진 스캐너 v2 — 스토캐스틱 반전 + 거래량 살아남 + 수급 확인")
print(f"  {today} 기준")
print("=" * 95)


def calc_stochastic(high, low, close, k_period=14, d_period=3):
    """스토캐스틱 %K, %D 계산"""
    lowest = low.rolling(window=k_period).min()
    highest = high.rolling(window=k_period).max()
    denom = (highest - lowest).replace(0, np.nan)
    k = 100 * (close - lowest) / denom
    d = k.rolling(window=d_period).mean()
    return k, d


def load_flow(code):
    """수급 데이터 로드"""
    path = FLOW_DIR / f"{code}_investor.csv"
    if not path.exists():
        return None
    try:
        df = pd.read_csv(path, index_col=0, parse_dates=True)
        return df
    except Exception:
        return None


def load_short(code):
    """공매도 데이터 로드"""
    path = SHORT_DIR / f"{code}_short_bal.csv"
    if not path.exists():
        return None
    try:
        df = pd.read_csv(path, index_col=0, parse_dates=True)
        return df
    except Exception:
        return None


results = []
total = 0
skipped = {"data_short": 0, "no_volume": 0, "vol_too_low": 0, "no_exhaustion": 0,
           "falling_too_much": 0, "sto_not_turning": 0, "vol_not_reviving": 0,
           "low_score": 0, "low_liquidity": 0}

for csv_path in sorted(DAILY_DIR.glob("*.csv")):
    code = csv_path.stem
    if code not in uni:
        continue

    try:
        df = pd.read_csv(csv_path, index_col=0, parse_dates=True)
        col_map = {
            "시가": "open", "고가": "high", "저가": "low",
            "종가": "close", "거래량": "volume", "등락률": "change_pct",
        }
        df.rename(columns=col_map, inplace=True)

        if len(df) < 60:
            skipped["data_short"] += 1
            continue

        total += 1
        close = df["close"].astype(float)
        volume = df["volume"].astype(float)
        high = df["high"].astype(float)
        low = df["low"].astype(float)
        opn = df["open"].astype(float) if "open" in df.columns else close

        cur_price = float(close.iloc[-1])
        cur_vol = float(volume.iloc[-1])
        prev_vol = float(volume.iloc[-2])

        if cur_price <= 0:
            skipped["no_volume"] += 1
            continue

        # ── 일평균 거래대금 필터 (최소 10억원) ──
        avg_turnover_20d = float((close.iloc[-20:] * volume.iloc[-20:]).mean())
        if avg_turnover_20d < 10_0000_0000:  # 10억원
            skipped["low_liquidity"] += 1
            continue

        # ── MA ──
        ma5 = close.rolling(5).mean()
        ma20 = close.rolling(20).mean()
        ma60 = close.rolling(60).mean()
        vol_ma20 = volume.rolling(20).mean()

        cur_ma5 = float(ma5.iloc[-1])
        cur_ma20 = float(ma20.iloc[-1])
        cur_ma60 = float(ma60.iloc[-1])
        cur_vol_ma20 = float(vol_ma20.iloc[-1])

        if cur_vol_ma20 <= 0:
            skipped["no_volume"] += 1
            continue

        # ── 1단계: 매물소진 확인 (최근 3~7일 중 거래량 < MA20*0.5) ──
        recent_vols = volume.iloc[-7:]
        vol_ratios = recent_vols / cur_vol_ma20
        low_vol_days = int((vol_ratios < 0.5).sum())
        avg_vol_ratio = float(volume.iloc[-5:].mean() / cur_vol_ma20)

        if low_vol_days < 3:
            skipped["no_exhaustion"] += 1
            continue

        # ── 2단계: 하락 추세 제거 ──
        price_20d_ago = float(close.iloc[-20])
        change_20d = (cur_price - price_20d_ago) / price_20d_ago * 100
        if change_20d < -15:
            skipped["falling_too_much"] += 1
            continue

        # ── 3단계: 스토캐스틱 반전 확인 (핵심!) ──
        stoch_k, stoch_d = calc_stochastic(high, low, close)
        cur_k = float(stoch_k.iloc[-1]) if not pd.isna(stoch_k.iloc[-1]) else 50
        cur_d = float(stoch_d.iloc[-1]) if not pd.isna(stoch_d.iloc[-1]) else 50
        prev_k = float(stoch_k.iloc[-2]) if not pd.isna(stoch_k.iloc[-2]) else 50
        prev_d = float(stoch_d.iloc[-2]) if not pd.isna(stoch_d.iloc[-2]) else 50
        prev2_k = float(stoch_k.iloc[-3]) if not pd.isna(stoch_k.iloc[-3]) else 50

        # 스토캐스틱 상태 판정
        sto_golden = prev_k <= prev_d and cur_k > cur_d  # %K가 %D 상향돌파
        sto_turning_up = cur_k > prev_k and prev_k <= prev2_k  # %K 바닥 찍고 올라옴
        sto_oversold = cur_k < 30 or cur_d < 30  # 과매도 영역
        sto_rising = cur_k > prev_k  # 최소한 %K가 올라가는 중

        # 스토캐스틱 조건: 골든크로스 or (과매도+반전) or (올라가는중+낮은영역)
        sto_ok = (
            sto_golden or
            (sto_oversold and sto_turning_up) or
            (sto_rising and cur_k < 50)
        )

        if not sto_ok:
            skipped["sto_not_turning"] += 1
            continue

        # ── 4단계: 거래량 살아남 확인 ──
        # 최근 2일 중 하루라도 전일 대비 거래량 증가 or 오늘 > MA20*0.3
        vol_reviving = (
            cur_vol > prev_vol * 1.1 or  # 오늘 > 어제 +10%
            cur_vol > cur_vol_ma20 * 0.8 or  # 평균 80% 이상 회복
            (cur_vol > prev_vol and cur_vol > float(volume.iloc[-3]))  # 2일 연속 증가
        )

        # 가격도 양봉이면 보너스 (거래량+가격 동반)
        price_up_today = cur_price > float(opn.iloc[-1])

        # ── 추가 지표 ──
        # RSI
        delta = close.diff()
        gain = delta.where(delta > 0, 0.0)
        loss_s = (-delta).where(delta < 0, 0.0)
        avg_gain = gain.ewm(alpha=1 / 14, min_periods=14).mean()
        avg_loss = loss_s.ewm(alpha=1 / 14, min_periods=14).mean()
        rs = avg_gain / avg_loss.replace(0, np.nan)
        rsi = 100 - (100 / (1 + rs))
        cur_rsi = float(rsi.iloc[-1]) if not pd.isna(rsi.iloc[-1]) else 50

        # MACD
        ema12 = close.ewm(span=12, adjust=False).mean()
        ema26 = close.ewm(span=26, adjust=False).mean()
        macd_line = ema12 - ema26
        signal_line = macd_line.ewm(span=9, adjust=False).mean()
        hist = macd_line - signal_line
        hist_now = float(hist.iloc[-1]) if not pd.isna(hist.iloc[-1]) else 0
        hist_prev = float(hist.iloc[-2]) if not pd.isna(hist.iloc[-2]) else 0
        macd_improving = hist_now > hist_prev
        macd_golden = (float(macd_line.iloc[-2]) <= float(signal_line.iloc[-2]) and
                       float(macd_line.iloc[-1]) > float(signal_line.iloc[-1]))

        # 가격 횡보
        price_range_5d = (close.iloc[-5:].max() - close.iloc[-5:].min()) / cur_price * 100
        price_vs_ma20 = (cur_price - cur_ma20) / cur_ma20 * 100

        # 볼린저 하단 근처?
        bb_mid = close.rolling(20).mean()
        bb_std = close.rolling(20).std()
        bb_lower = bb_mid - 2 * bb_std
        bb_upper = bb_mid + 2 * bb_std
        bb_pos = (cur_price - float(bb_lower.iloc[-1])) / (float(bb_upper.iloc[-1]) - float(bb_lower.iloc[-1])) if float(bb_upper.iloc[-1]) != float(bb_lower.iloc[-1]) else 0.5

        # 수급 확인
        inst_net_5d = 0.0
        foreign_net_5d = 0.0
        smart_score = 0
        flow_df = load_flow(code)
        if flow_df is not None and len(flow_df) >= 5:
            if "기관_금액" in flow_df.columns:
                inst_net_5d = round(float(flow_df["기관_금액"].iloc[-5:].sum()) / 1e8, 1)
            if "외국인_금액" in flow_df.columns:
                foreign_net_5d = round(float(flow_df["외국인_금액"].iloc[-5:].sum()) / 1e8, 1)

        short_chg = 0.0
        short_df = load_short(code)
        if short_df is not None and "비중" in short_df.columns and len(short_df) >= 5:
            short_chg = round(float(short_df["비중"].iloc[-1]) - float(short_df["비중"].iloc[-5]), 3)

        # ══════════ 점수 체계 (v2) ══════════
        score = 0
        signals = []

        # 스토캐스틱 (핵심 — 최대 30점)
        if sto_golden and cur_k < 30:
            score += 30
            signals.append(f"STO골든({cur_k:.0f}/{cur_d:.0f})")
        elif sto_golden:
            score += 25
            signals.append(f"STO골든({cur_k:.0f}/{cur_d:.0f})")
        elif sto_turning_up and sto_oversold:
            score += 20
            signals.append(f"STO반전({cur_k:.0f})")
        elif sto_rising and cur_k < 40:
            score += 15
            signals.append(f"STO상승({cur_k:.0f})")
        else:
            score += 10
            signals.append(f"STO({cur_k:.0f})")

        # 거래량 반등 (최대 25점)
        today_vol_ratio = cur_vol / cur_vol_ma20
        if vol_reviving and today_vol_ratio >= 1.5:
            score += 25
            signals.append(f"거래량폭증{today_vol_ratio:.1f}x!")
        elif vol_reviving and price_up_today:
            score += 20
            signals.append(f"거래량+양봉")
        elif vol_reviving:
            score += 15
            signals.append(f"거래량살아남")
        else:
            score += 5  # 거래량 아직 안 살아남 — 감점

        # 매물소진 강도 (최대 15점)
        if low_vol_days >= 5 and avg_vol_ratio < 0.3:
            score += 15
            signals.append(f"극고갈{low_vol_days}일VR{avg_vol_ratio:.1f}x")
        elif low_vol_days >= 4:
            score += 12
            signals.append(f"고갈{low_vol_days}일")
        else:
            score += 8
            signals.append(f"소진{low_vol_days}일")

        # MACD (최대 15점)
        if macd_golden:
            score += 15
            signals.append("MACD골든!")
        elif macd_improving and hist_now > 0:
            score += 12
            signals.append("MACD양전환")
        elif macd_improving:
            score += 8
            signals.append("MACD수렴")

        # RSI 위치 (최대 10점)
        if 25 <= cur_rsi <= 40:
            score += 10
            signals.append(f"RSI{cur_rsi:.0f}과매도")
        elif 40 < cur_rsi <= 50:
            score += 5

        # MA 배열 (최대 10점)
        if cur_ma5 > cur_ma20 > cur_ma60:
            score += 10
            signals.append("정배열")
        elif cur_ma5 > cur_ma20:
            score += 5
            signals.append("부분정배열")

        # 수급 (최대 15점)
        smart_net = inst_net_5d + foreign_net_5d
        if inst_net_5d > 10 and foreign_net_5d > 10:
            score += 15
            signals.append(f"쌍끌이+{smart_net:.0f}억")
        elif smart_net > 30:
            score += 12
            signals.append(f"스마트+{smart_net:.0f}억")
        elif smart_net > 0:
            score += 5
        elif smart_net < -30:
            score -= 5
            signals.append(f"수급유출{smart_net:.0f}억")

        # 공매도 감소 (보너스 5점)
        if short_chg < -0.05:
            score += 5
            signals.append("공매도감소")

        # 볼린저 하단 근처 (보너스 5점)
        if bb_pos < 0.3:
            score += 5
            signals.append(f"BB하단{bb_pos:.0%}")

        # ── 최종 필터 ──
        if score < 50:
            skipped["low_score"] += 1
            continue

        name = uni[code]["name"]
        cap = uni[code]["cap_억"]
        sub_sector = uni[code].get("sub_sector", uni[code].get("sector", "기타"))

        results.append({
            "code": code,
            "name": name,
            "cap": cap,
            "sector": sub_sector,
            "score": score,
            "price": int(cur_price),
            "change_20d": round(change_20d, 1),
            "vol_ratio": round(avg_vol_ratio, 2),
            "low_vol_days": low_vol_days,
            "price_range": round(price_range_5d, 1),
            "rsi": round(cur_rsi, 1),
            "sto_k": round(cur_k, 1),
            "sto_d": round(cur_d, 1),
            "sto_status": ("골든" if sto_golden else ("반전" if sto_turning_up else "상승")),
            "macd_ok": macd_improving,
            "macd_golden": macd_golden,
            "ma_status": ("정배열" if cur_ma5 > cur_ma20 > cur_ma60
                          else ("부분" if cur_ma5 > cur_ma20 else "역배열")),
            "price_vs_ma20": round(price_vs_ma20, 1),
            "vol_reviving": vol_reviving,
            "today_vr": round(today_vol_ratio, 2),
            "bb_pos": round(bb_pos, 2),
            "inst_5d": inst_net_5d,
            "foreign_5d": foreign_net_5d,
            "smart_net": round(smart_net, 1),
            "short_chg": short_chg,
            "turnover_억": round(avg_turnover_20d / 1e8, 0),
            "signals": " | ".join(signals),
        })
    except Exception:
        continue

results.sort(key=lambda x: -x["score"])

print(f"\n  분석: {total}종목 → 통과: {len(results)}종목")
print(f"  필터링: 유동성부족={skipped['low_liquidity']}, 매물소진없음={skipped['no_exhaustion']}, "
      f"하락과다={skipped['falling_too_much']}, STO미반전={skipped['sto_not_turning']}, "
      f"점수미달={skipped['low_score']}\n")

if results:
    print(f"{'#':>2} {'종목':>14} {'코드':>8} {'점수':>4}"
          f" {'종가':>10} {'20D%':>6} {'VR':>5} {'고갈':>4}"
          f" {'STO':>10} {'RSI':>5} {'MA':>5} {'수급':>8} {'시그널'}")
    print("-" * 130)

    for i, r in enumerate(results[:40], 1):
        marker = ""
        if r["score"] >= 85:
            marker = " ★★★"
        elif r["score"] >= 70:
            marker = " ★★"
        elif r["score"] >= 55:
            marker = " ★"

        sto_str = f"{r['sto_k']:.0f}/{r['sto_d']:.0f}{r['sto_status']}"
        smart_str = f"{r['smart_net']:+.0f}억" if r["smart_net"] != 0 else "-"

        print(
            f"{i:>2} {r['name']:>14} {r['code']:>8} {r['score']:>4}"
            f" {r['price']:>10,} {r['change_20d']:>+6.1f}%"
            f" {r['vol_ratio']:>5.2f} {r['low_vol_days']:>2}일"
            f" {sto_str:>10} {r['rsi']:>5.1f} {r['ma_status']:>5}"
            f" {smart_str:>8}"
            f" {r['signals']}{marker}"
        )

    if len(results) > 40:
        print(f"  ... 외 {len(results) - 40}종목")

    # ═══ 카테고리별 정리 ═══
    tier1 = [r for r in results if r["score"] >= 85]
    tier2 = [r for r in results if 70 <= r["score"] < 85]
    tier3 = [r for r in results if 55 <= r["score"] < 70]

    print()
    print("=" * 95)
    print(f"  ★★★ 반등 시작 확인 (85+): {len(tier1)}종목")
    print("=" * 95)
    for r in tier1:
        vol_mark = "거래량폭증!" if r["today_vr"] >= 1.5 else ("살아남" if r["vol_reviving"] else "")
        macd_mark = "MACD골든!" if r["macd_golden"] else ""
        smart_mark = f"수급+{r['smart_net']:.0f}억" if r["smart_net"] > 10 else ""
        marks = " / ".join(filter(None, [vol_mark, macd_mark, smart_mark]))
        print(
            f"  {r['name']}({r['code']}) {r['price']:,}원 점수:{r['score']}"
            f" | STO:{r['sto_k']:.0f}/{r['sto_d']:.0f}({r['sto_status']})"
            f" | VR:{r['vol_ratio']:.2f}x→오늘{r['today_vr']:.1f}x"
            f" | RSI:{r['rsi']:.0f} | {r['ma_status']}"
            f" | 거래대금:{r['turnover_억']:.0f}억/일"
        )
        if marks:
            print(f"    → {marks}")
        print(f"    시그널: {r['signals']}")

    print()
    print(f"  ★★ 반등 유력 (70~84): {len(tier2)}종목")
    print("-" * 95)
    for r in tier2:
        print(
            f"  {r['name']}({r['code']}) {r['price']:,}원 점수:{r['score']}"
            f" STO:{r['sto_k']:.0f}/{r['sto_d']:.0f}({r['sto_status']})"
            f" VR:{r['vol_ratio']:.2f}x RSI:{r['rsi']:.0f}"
            f" {r['ma_status']} 수급:{r['smart_net']:+.0f}억"
        )

    print()
    print(f"  ★ 관찰 (55~69): {len(tier3)}종목")
    print("=" * 95)

    # ═══════════════════════════════════════════════════
    # 섹터별 분류
    # ═══════════════════════════════════════════════════
    from collections import defaultdict
    sector_groups = defaultdict(list)
    for r in results:
        sector_groups[r["sector"]].append(r)

    # 섹터를 평균 점수 높은 순으로 정렬
    sector_avg = {s: sum(r["score"] for r in lst) / len(lst) for s, lst in sector_groups.items()}
    sorted_sectors = sorted(sector_groups.keys(), key=lambda s: -sector_avg[s])

    print()
    print("=" * 100)
    print("  ■ 섹터별 매물소진 종목 분류")
    print("=" * 100)
    print(f"\n  {'섹터':12s} {'종목수':>4} {'평균점수':>6} {'대표종목'}")
    print("-" * 100)

    for sect in sorted_sectors:
        lst = sorted(sector_groups[sect], key=lambda x: -x["score"])
        avg = sector_avg[sect]
        top3 = ", ".join(f"{r['name']}({r['score']})" for r in lst[:3])
        marker = ""
        if avg >= 85:
            marker = " 🔥"
        elif avg >= 75:
            marker = " ⚡"
        print(f"  {sect:12s} {len(lst):>4}개 {avg:>6.1f} {top3}{marker}")

    # 섹터 상세 (종목 3개 이상인 섹터)
    hot_sectors = [(s, sector_groups[s]) for s in sorted_sectors if len(sector_groups[s]) >= 2]
    if hot_sectors:
        print()
        print("=" * 100)
        print("  ■ 주요 섹터 상세 (2종목 이상)")
        print("=" * 100)
        for sect, lst in hot_sectors:
            lst_sorted = sorted(lst, key=lambda x: -x["score"])
            total_smart = sum(r["smart_net"] for r in lst)
            avg_score = sum(r["score"] for r in lst) / len(lst)
            print(f"\n  ▶ {sect} ({len(lst)}종목) — 평균점수:{avg_score:.0f} | 섹터합산수급:{total_smart:+,.0f}억")
            for r in lst_sorted:
                star = "★★★" if r["score"] >= 85 else ("★★" if r["score"] >= 70 else "★")
                print(
                    f"    {star} {r['name']:14s}({r['code']}) {r['price']:>10,}원"
                    f" 점수:{r['score']:>3}"
                    f" STO:{r['sto_k']:.0f}/{r['sto_d']:.0f}"
                    f" 수급:{r['smart_net']:+.0f}억"
                    f" {r['signals']}"
                )
else:
    print("  조건 충족 종목 없음 — 필터가 매우 엄격합니다.")
    print("  (스토캐스틱 반전 + 거래량 살아남 + 매물소진 3일+ 동시 충족 필요)")
