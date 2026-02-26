"""
MACD 제로선 크로스 + 수급 폭발 후 조정→급등 패턴 백테스트
==========================================================
전략:
  1. MACD(12,26,9)가 0선 근처(-2~+2)에서
  2. MACD선이 시그널선을 상향 돌파 (골든크로스)
  3. 이때 수급(기관+외국인) 폭발 여부도 체크
  4. 진입 후 20일간 수익률 추적

추가 검증:
  - 수급 폭발(최근 5일 순매수 > 20일 평균의 3배) 후
  - 조정(5~15% 하락) → 반등 패턴 빈도
"""
import sys, os, json
import pandas as pd
import numpy as np
from pathlib import Path
from collections import defaultdict

sys.stdout.reconfigure(encoding="utf-8")

BASE = Path(__file__).resolve().parent.parent
DAILY_DIR = BASE / "data_store" / "daily"
FLOW_DIR = BASE / "data_store" / "flow"


def calc_macd(close, fast=12, slow=26, signal=9):
    """MACD, Signal, Histogram 계산"""
    ema_fast = pd.Series(close).ewm(span=fast, adjust=False).mean()
    ema_slow = pd.Series(close).ewm(span=slow, adjust=False).mean()
    macd = ema_fast - ema_slow
    sig = macd.ewm(span=signal, adjust=False).mean()
    hist = macd - sig
    return macd.values, sig.values, hist.values


def load_flow(code):
    """수급(기관+외국인 순매수수량) 로드"""
    path = FLOW_DIR / f"{code}_investor.csv"
    if not path.exists():
        return None
    df = pd.read_csv(path, index_col=0, parse_dates=True)
    df["combo"] = df["기관_수량"].astype(float) + df["외국인_수량"].astype(float)
    return df


def run_backtest():
    # 유니버스 로드
    uni_path = BASE / "data_store" / "universe.json"
    with open(uni_path, "r", encoding="utf-8") as f:
        universe = json.load(f)

    codes = [c for c in universe.keys() if c[-1] not in {"5","7","8","9","K","L"}]
    print(f"대상 종목: {len(codes)}개")

    # ── 결과 저장 ──
    # 전략 A: MACD 제로선 골든크로스
    signals_a = []
    # 전략 B: A + 수급 폭발 동반
    signals_b = []
    # 패턴 C: 수급 폭발 → 조정 → 반등
    pattern_c = []

    for idx, code in enumerate(codes):
        if (idx + 1) % 200 == 0:
            print(f"  처리 중 {idx+1}/{len(codes)}...", flush=True)

        daily_path = DAILY_DIR / f"{code}.csv"
        if not daily_path.exists():
            continue

        try:
            df = pd.read_csv(daily_path, index_col=0, parse_dates=True)
        except Exception:
            continue

        if len(df) < 60:
            continue

        close = df["종가"].values.astype(float)
        high = df["고가"].values.astype(float)
        low = df["저가"].values.astype(float)
        dates = df.index

        macd, sig, hist = calc_macd(close)

        # 수급 데이터
        flow_df = load_flow(code)
        flow_aligned = None
        if flow_df is not None:
            common_idx = df.index.intersection(flow_df.index)
            if len(common_idx) > 30:
                flow_aligned = flow_df.loc[common_idx, "combo"].reindex(df.index).fillna(0).values

        # ── 전략 A: MACD 제로선 골든크로스 스캔 ──
        # MACD 정규화: ATR 대비 비율로 "0선 근처" 판단
        atr_14 = pd.Series(high - low).rolling(14).mean().values

        for i in range(35, len(close) - 20):
            # 조건1: MACD가 0선 근처 (ATR의 ±10% 이내)
            atr_val = atr_14[i] if atr_14[i] > 0 else 1
            macd_ratio = abs(macd[i]) / atr_val

            if macd_ratio > 0.5:  # ATR 대비 50% 넘으면 0선 근처 아님
                continue

            # 조건2: 골든크로스 (MACD가 시그널 상향돌파)
            if not (hist[i-1] <= 0 and hist[i] > 0):
                continue

            # 조건3: MACD선이 상승 추세 (직전 3일 중 상승)
            if not (macd[i] > macd[i-3]):
                continue

            # ── 진입 ──
            entry_price = close[i]
            entry_date = dates[i]

            # 이후 20일 수익률
            future = close[i+1:i+21]
            max_gain = (future.max() / entry_price - 1) * 100
            max_loss = (future.min() / entry_price - 1) * 100
            ret_5d = (close[min(i+5, len(close)-1)] / entry_price - 1) * 100
            ret_10d = (close[min(i+10, len(close)-1)] / entry_price - 1) * 100
            ret_20d = (close[min(i+20, len(close)-1)] / entry_price - 1) * 100

            name = universe.get(code, {})
            name = name.get("name", code) if isinstance(name, dict) else code

            sig_data = {
                "code": code, "name": name,
                "date": str(entry_date.date()),
                "entry": entry_price,
                "macd_ratio": round(macd_ratio, 3),
                "ret_5d": round(ret_5d, 2),
                "ret_10d": round(ret_10d, 2),
                "ret_20d": round(ret_20d, 2),
                "max_gain": round(max_gain, 2),
                "max_loss": round(max_loss, 2),
            }

            signals_a.append(sig_data)

            # ── 전략 B: 수급 폭발 동반 체크 ──
            if flow_aligned is not None:
                recent_5 = flow_aligned[i-4:i+1].sum()
                avg_20 = np.mean(np.abs(flow_aligned[i-24:i-4])) if i >= 24 else 0

                if avg_20 > 0 and recent_5 > 0 and recent_5 > avg_20 * 3:
                    sig_data_b = sig_data.copy()
                    sig_data_b["flow_ratio"] = round(recent_5 / avg_20, 1)
                    signals_b.append(sig_data_b)

        # ── 패턴 C: 수급 폭발 → 조정 → 반등 ──
        if flow_aligned is not None:
            for i in range(30, len(close) - 25):
                avg_20 = np.mean(np.abs(flow_aligned[i-24:i-4])) if i >= 24 else 0
                recent_5 = flow_aligned[i-4:i+1].sum()

                # 수급 폭발 탐지: 5일 순매수 > 20일 절대평균의 5배
                if avg_20 <= 0 or recent_5 <= 0 or recent_5 < avg_20 * 5:
                    continue

                # 폭발 시점 이후 20일간 추적
                peak_price = close[i]
                found_correction = False
                correction_idx = None

                for j in range(i+1, min(i+16, len(close))):
                    drawdown = (close[j] / peak_price - 1) * 100
                    if close[j] > peak_price:
                        peak_price = close[j]

                    # 고점 대비 5~20% 조정
                    if -20 < drawdown < -3:
                        found_correction = True
                        correction_idx = j
                        break

                if not found_correction or correction_idx is None:
                    continue

                # 조정 이후 10일간 반등 추적
                correction_price = close[correction_idx]
                if correction_idx + 10 >= len(close):
                    continue

                rebound = close[correction_idx+1:correction_idx+11]
                rebound_max = (rebound.max() / correction_price - 1) * 100
                rebound_5d = (close[correction_idx+5] / correction_price - 1) * 100

                name = universe.get(code, {})
                name = name.get("name", code) if isinstance(name, dict) else code

                pattern_c.append({
                    "code": code, "name": name,
                    "explosion_date": str(dates[i].date()),
                    "correction_date": str(dates[correction_idx].date()),
                    "drawdown": round((close[correction_idx]/close[i]-1)*100, 2),
                    "rebound_5d": round(rebound_5d, 2),
                    "rebound_max10d": round(rebound_max, 2),
                    "flow_ratio": round(recent_5/avg_20, 1),
                })

    # ═══════════════════════════════════════════
    # 결과 분석
    # ═══════════════════════════════════════════
    print("\n" + "=" * 60)
    print("  MACD 제로선 골든크로스 백테스트 결과")
    print("=" * 60)

    # ── 전략 A: MACD 제로선 크로스 ──
    print(f"\n[A] MACD 제로선 골든크로스 (전체)")
    print(f"    시그널 수: {len(signals_a)}")
    if signals_a:
        df_a = pd.DataFrame(signals_a)
        print(f"    5일 수익률  평균: {df_a['ret_5d'].mean():+.2f}%  중앙: {df_a['ret_5d'].median():+.2f}%")
        print(f"    10일 수익률 평균: {df_a['ret_10d'].mean():+.2f}%  중앙: {df_a['ret_10d'].median():+.2f}%")
        print(f"    20일 수익률 평균: {df_a['ret_20d'].mean():+.2f}%  중앙: {df_a['ret_20d'].median():+.2f}%")
        print(f"    최대 이익 평균: {df_a['max_gain'].mean():+.2f}%")
        print(f"    최대 손실 평균: {df_a['max_loss'].mean():+.2f}%")

        # 승률 (5일 기준 양수)
        wr_5 = (df_a['ret_5d'] > 0).mean() * 100
        wr_10 = (df_a['ret_10d'] > 0).mean() * 100
        wr_20 = (df_a['ret_20d'] > 0).mean() * 100
        print(f"    승률: 5일={wr_5:.1f}% | 10일={wr_10:.1f}% | 20일={wr_20:.1f}%")

        # 수익률 구간 분포
        bins = [-999, -10, -5, -2, 0, 2, 5, 10, 999]
        labels = ["<-10%", "-10~-5%", "-5~-2%", "-2~0%", "0~2%", "2~5%", "5~10%", ">10%"]
        df_a["bin_10d"] = pd.cut(df_a["ret_10d"], bins=bins, labels=labels)
        print(f"\n    10일 수익률 분포:")
        for label in labels:
            cnt = (df_a["bin_10d"] == label).sum()
            pct = cnt / len(df_a) * 100
            bar = "#" * int(pct / 2)
            print(f"      {label:>8s}: {cnt:4d} ({pct:5.1f}%) {bar}")

    # ── 전략 B: MACD 크로스 + 수급 폭발 ──
    print(f"\n[B] MACD 제로선 크로스 + 수급 폭발 동반")
    print(f"    시그널 수: {len(signals_b)}")
    if signals_b:
        df_b = pd.DataFrame(signals_b)
        print(f"    5일 수익률  평균: {df_b['ret_5d'].mean():+.2f}%  중앙: {df_b['ret_5d'].median():+.2f}%")
        print(f"    10일 수익률 평균: {df_b['ret_10d'].mean():+.2f}%  중앙: {df_b['ret_10d'].median():+.2f}%")
        print(f"    20일 수익률 평균: {df_b['ret_20d'].mean():+.2f}%  중앙: {df_b['ret_20d'].median():+.2f}%")
        print(f"    최대 이익 평균: {df_b['max_gain'].mean():+.2f}%")
        print(f"    최대 손실 평균: {df_b['max_loss'].mean():+.2f}%")

        wr_5 = (df_b['ret_5d'] > 0).mean() * 100
        wr_10 = (df_b['ret_10d'] > 0).mean() * 100
        wr_20 = (df_b['ret_20d'] > 0).mean() * 100
        print(f"    승률: 5일={wr_5:.1f}% | 10일={wr_10:.1f}% | 20일={wr_20:.1f}%")

        # A대비 비교
        if signals_a:
            df_a2 = pd.DataFrame(signals_a)
            diff_10 = df_b['ret_10d'].mean() - df_a2['ret_10d'].mean()
            print(f"\n    → A 대비 10일 수익률 차이: {diff_10:+.2f}%p ({'수급이 도움됨' if diff_10 > 0 else '수급 무관'})")

    # ── 패턴 C: 수급 폭발 → 조정 → 반등 ──
    print(f"\n[C] 수급 폭발 → 조정(-3~-20%) → 반등 패턴")
    print(f"    발견 패턴 수: {len(pattern_c)}")
    if pattern_c:
        df_c = pd.DataFrame(pattern_c)
        print(f"    조정 폭 평균: {df_c['drawdown'].mean():.2f}%")
        print(f"    반등 5일 평균: {df_c['rebound_5d'].mean():+.2f}%  중앙: {df_c['rebound_5d'].median():+.2f}%")
        print(f"    반등 10일 최대 평균: {df_c['rebound_max10d'].mean():+.2f}%")

        # 반등 성공률
        rebound_wr = (df_c['rebound_5d'] > 0).mean() * 100
        strong_rebound = (df_c['rebound_max10d'] > 5).mean() * 100
        print(f"    반등 승률(5일>0%): {rebound_wr:.1f}%")
        print(f"    강한 반등(10일 max>5%): {strong_rebound:.1f}%")

        # 수급 폭발 강도별 반등
        df_c["flow_cat"] = pd.cut(df_c["flow_ratio"], bins=[0,5,10,20,999],
                                   labels=["5~10x","10~20x","20~50x",">50x"])
        print(f"\n    수급 폭발 강도별 반등(5일):")
        for cat in ["5~10x","10~20x","20~50x",">50x"]:
            sub = df_c[df_c["flow_cat"] == cat]
            if len(sub) > 0:
                print(f"      {cat:>6s}: {len(sub):4d}건 | 반등 {sub['rebound_5d'].mean():+.2f}% | 승률 {(sub['rebound_5d']>0).mean()*100:.0f}%")

        # TOP 사례
        print(f"\n    강한 반등 TOP 10:")
        top = df_c.nlargest(10, "rebound_max10d")
        for _, r in top.iterrows():
            print(f"      {r['name']:12s} | 폭발:{r['explosion_date']} 조정:{r['drawdown']:+.1f}% → 반등:{r['rebound_max10d']:+.1f}% | 수급x{r['flow_ratio']:.0f}")

    # ═══════════════════════════════════════════
    # 결론
    # ═══════════════════════════════════════════
    print("\n" + "=" * 60)
    print("  결론")
    print("=" * 60)

    if signals_a and signals_b:
        df_a3 = pd.DataFrame(signals_a)
        df_b3 = pd.DataFrame(signals_b)
        a_wr = (df_a3['ret_10d'] > 0).mean() * 100
        b_wr = (df_b3['ret_10d'] > 0).mean() * 100

        print(f"  A) MACD 제로선 크로스만: {len(signals_a)}건, 10일 승률 {a_wr:.1f}%")
        print(f"  B) + 수급 폭발 동반:     {len(signals_b)}건, 10일 승률 {b_wr:.1f}%")

    if pattern_c:
        df_c2 = pd.DataFrame(pattern_c)
        c_wr = (df_c2['rebound_5d'] > 0).mean() * 100
        print(f"  C) 수급폭발→조정→반등:   {len(pattern_c)}건, 5일 반등 승률 {c_wr:.1f}%")

    print()
    return signals_a, signals_b, pattern_c


if __name__ == "__main__":
    run_backtest()
