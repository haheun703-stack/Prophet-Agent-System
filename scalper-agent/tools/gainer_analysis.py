# -*- coding: utf-8 -*-
"""금일 5%이상 상승 종목 — 전일 시그널 역추적 분석"""
import sys, io, json, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
os.chdir(os.path.join(os.path.dirname(__file__), ".."))

import pandas as pd
import numpy as np
from collections import Counter, defaultdict

with open('data_store/today_gainers.json', 'r', encoding='utf-8') as f:
    gainers = json.load(f)

with open('data_store/universe.json', 'r', encoding='utf-8') as f:
    uni = json.load(f)

results = []
no_data = 0

for g in gainers:
    code = g['code']
    daily_path = f'data_store/daily/{code}.csv'
    if not os.path.exists(daily_path):
        no_data += 1
        g['signals'] = ['데이터없음']
        results.append(g)
        continue

    df = pd.read_csv(daily_path)
    if len(df) < 20:
        no_data += 1
        g['signals'] = ['데이터부족']
        results.append(g)
        continue

    df = df.sort_values('날짜').reset_index(drop=True)

    # 오늘 데이터 제외 (전일까지만)
    if df['날짜'].iloc[-1] == '2026-02-19':
        df = df.iloc[:-1]

    if len(df) < 20:
        g['signals'] = ['데이터부족']
        results.append(g)
        continue

    close = df['종가'].values.astype(float)
    high = df['고가'].values.astype(float)
    low = df['저가'].values.astype(float)
    volume = df['거래량'].values.astype(float)
    opn = df['시가'].values.astype(float)

    signals = []

    # === 1. Stochastic (14,3,3) ===
    k_period = 14
    if len(close) >= k_period + 6:
        k_raw = []
        for i in range(k_period - 1, len(close)):
            h14 = max(high[i-k_period+1:i+1])
            l14 = min(low[i-k_period+1:i+1])
            if h14 != l14:
                k_raw.append((close[i] - l14) / (h14 - l14) * 100)
            else:
                k_raw.append(50)

        k_slow = pd.Series(k_raw).rolling(3).mean().values
        d_vals = pd.Series(k_slow).rolling(3).mean().values

        k_now = k_slow[-1] if not np.isnan(k_slow[-1]) else 50
        d_now = d_vals[-1] if not np.isnan(d_vals[-1]) else 50
        k_prev = k_slow[-2] if not np.isnan(k_slow[-2]) else 50
        d_prev = d_vals[-2] if not np.isnan(d_vals[-2]) else 50

        g['sto_k'] = round(k_now, 1)
        g['sto_d'] = round(d_now, 1)

        if k_now > d_now and k_prev <= d_prev:
            signals.append('STO골든')
        elif k_now > d_now:
            signals.append('STO상향')

        if k_now < 20:
            signals.append('STO과매도')
        elif k_prev < 20 and k_now >= 20:
            signals.append('STO과매도탈출')

        if k_now > 80:
            signals.append('STO과매수')

    # === 2. RSI(14) ===
    if len(close) >= 15:
        deltas = np.diff(close[-15:])
        gains = np.where(deltas > 0, deltas, 0)
        losses = np.where(deltas < 0, -deltas, 0)
        avg_gain = gains.mean()
        avg_loss = losses.mean()
        if avg_loss > 0:
            rs = avg_gain / avg_loss
            rsi = 100 - 100 / (1 + rs)
        else:
            rsi = 100
        g['rsi'] = round(rsi, 1)

        if rsi < 30:
            signals.append('RSI과매도(<30)')
        elif rsi < 40:
            signals.append('RSI약세(30~40)')

    # === 3. 이동평균 ===
    if len(close) >= 60:
        ma5 = close[-5:].mean()
        ma20 = close[-20:].mean()
        ma60 = close[-60:].mean()
        c = close[-1]

        if ma5 > ma20 > ma60:
            signals.append('정배열')
        elif ma5 < ma20 < ma60:
            signals.append('역배열')
        else:
            signals.append('배열혼조')

        if close[-2] < ma20 and close[-1] >= ma20:
            signals.append('20일선돌파')
        elif c < ma20 * 0.98:
            signals.append('20일선하회')

        if close[-2] < ma60 and close[-1] >= ma60:
            signals.append('60일선돌파')

    # === 4. 거래량 분석 ===
    if len(volume) >= 20:
        vol_avg20 = volume[-20:].mean()
        vol_yday = volume[-1]
        vr = vol_yday / vol_avg20 if vol_avg20 > 0 else 0
        g['vol_ratio'] = round(vr, 2)

        if vr < 0.3:
            signals.append('극저거래량(<0.3x)')
        elif vr < 0.5:
            signals.append('저거래량(<0.5x)')
        elif vr > 3:
            signals.append('거래량대폭증(>3x)')
        elif vr > 1.5:
            signals.append('거래량증가(>1.5x)')

        # 매물소진 패턴: 최근 5일 평균 < 20일 평균의 50%
        if len(volume) >= 5:
            vol_5d_avg = volume[-5:].mean()
            if vol_5d_avg < vol_avg20 * 0.5:
                signals.append('매물소진패턴')

    # === 5. MACD ===
    if len(close) >= 35:
        ema12 = pd.Series(close).ewm(span=12).mean().values
        ema26 = pd.Series(close).ewm(span=26).mean().values
        macd = ema12 - ema26
        sig_line = pd.Series(macd).ewm(span=9).mean().values

        if macd[-1] > sig_line[-1] and macd[-2] <= sig_line[-2]:
            signals.append('MACD골든')
        elif macd[-1] > sig_line[-1]:
            signals.append('MACD양전환')
        elif abs(macd[-1] - sig_line[-1]) / (abs(sig_line[-1]) + 1) < 0.1:
            signals.append('MACD수렴')

        if macd[-2] < 0 and macd[-1] > 0:
            signals.append('MACD제로돌파')

    # === 6. 볼린저밴드 ===
    if len(close) >= 20:
        ma20_bb = close[-20:].mean()
        std20 = close[-20:].std()
        bb_upper = ma20_bb + 2 * std20
        bb_lower = ma20_bb - 2 * std20

        c = close[-1]
        if bb_upper != bb_lower:
            bb_pct = (c - bb_lower) / (bb_upper - bb_lower) * 100
        else:
            bb_pct = 50
        g['bb_pct'] = round(bb_pct, 1)

        if bb_pct < 5:
            signals.append('BB하단이탈')
        elif bb_pct < 20:
            signals.append('BB하단근접')

        bb_width = (bb_upper - bb_lower) / ma20_bb * 100 if ma20_bb > 0 else 0
        if bb_width < 8:
            signals.append('BB스퀴즈')

    # === 7. 낙폭 (20일 고점 대비) ===
    if len(close) >= 20:
        high20 = max(close[-20:])
        chg20 = (close[-1] / high20 - 1) * 100
        g['drop_20d'] = round(chg20, 1)

        if chg20 < -25:
            signals.append('20일낙폭>25%')
        elif chg20 < -15:
            signals.append('20일낙폭>15%')
        elif chg20 < -10:
            signals.append('20일낙폭>10%')

    # === 8. 캔들 패턴 ===
    if len(df) >= 3:
        c1, o1 = close[-1], opn[-1]
        c2, o2 = close[-2], opn[-2]
        c3, o3 = close[-3], opn[-3]

        if c1 > o1:
            signals.append('전일양봉')
        else:
            signals.append('전일음봉')

        # 연속 음봉
        neg = 0
        for i in range(len(close)-1, max(len(close)-8, -1), -1):
            if close[i] < opn[i]:
                neg += 1
            else:
                break
        if neg >= 3:
            signals.append(f'연속음봉{neg}일')

        # 도지/망치형
        body = abs(c1 - o1)
        total_range = high[-1] - low[-1]
        if total_range > 0:
            lower_shadow = min(c1, o1) - low[-1]
            if body < total_range * 0.2 and lower_shadow > total_range * 0.5:
                signals.append('망치형캔들')
            elif body < total_range * 0.15:
                signals.append('도지캔들')

    # === 9. 수급 데이터 ===
    inv_path = f'data_store/investor/{code}.csv'
    if os.path.exists(inv_path):
        inv_df = pd.read_csv(inv_path)
        if len(inv_df) >= 5:
            inv_df = inv_df.sort_values('날짜' if '날짜' in inv_df.columns else inv_df.columns[0])
            recent = inv_df.tail(5)
            inst_5d = 0
            foreign_5d = 0
            for col in recent.columns:
                if '기관' in col and '합계' in col:
                    inst_5d = recent[col].sum()
                elif '외국인' in col and '합계' in col:
                    foreign_5d = recent[col].sum()

            net_5d = inst_5d + foreign_5d
            g['net_supply_5d'] = round(net_5d / 1e8, 0)

            if inst_5d > 0 and foreign_5d > 0:
                signals.append('쌍끌이매수')
            elif inst_5d > 0:
                signals.append('기관순매수')
            elif foreign_5d > 0:
                signals.append('외인순매수')
            else:
                signals.append('수급유출')

    # === 10. 공매도 ===
    short_path = f'data_store/short_balance/{code}.csv'
    if os.path.exists(short_path):
        sdf = pd.read_csv(short_path)
        if len(sdf) >= 5:
            sdf = sdf.sort_values(sdf.columns[0])
            bal_col = [c for c in sdf.columns if '잔고' in c and '금액' in c]
            if bal_col:
                vals = sdf[bal_col[0]].tail(5).values
                if len(vals) >= 2 and vals[-2] > 0:
                    short_chg = (vals[-1] / vals[-2] - 1) * 100
                    if short_chg < -10:
                        signals.append('공매도급감')
                    elif short_chg < -5:
                        signals.append('공매도감소')

    if not signals:
        signals.append('특이시그널없음')

    g['signals'] = signals
    g['signal_count'] = len([s for s in signals if s not in ('전일양봉','전일음봉','배열혼조','STO상향','MACD양전환')])
    results.append(g)

# Save
with open('data_store/today_gainers_analyzed.json', 'w', encoding='utf-8') as f:
    json.dump(results, f, ensure_ascii=False, indent=2)

# ═══ REPORT ═══
print("=" * 100)
print("  금일(2026-02-19) 5%이상 상승종목 — 전일 시그널 역추적 분석")
print("=" * 100)
print(f"  분석: {len(results)}종목 (데이터없음: {no_data})")

# 시그널 빈도 분석
all_signals = []
for r in results:
    all_signals.extend(r['signals'])
sig_counter = Counter(all_signals)

print(f"\n{'=' * 100}")
print("  ■ 시그널 빈도 TOP (전일 기준, 오늘 5%+상승 종목들이 공통으로 가진 시그널)")
print("=" * 100)
for sig, cnt in sig_counter.most_common(30):
    pct = cnt / len(results) * 100
    bar = "█" * int(pct / 2)
    print(f"  {sig:22s}: {cnt:>3}종목 ({pct:5.1f}%) {bar}")

# 섹터별 집계
print(f"\n{'=' * 100}")
print("  ■ 섹터별 5%+ 상승 종목 수")
print("=" * 100)
sector_counts = Counter(r['sector'] for r in results)
for sec, cnt in sector_counts.most_common(30):
    stocks = [r for r in results if r['sector'] == sec]
    avg_chg = np.mean([r['change'] for r in stocks])
    names = ', '.join(f"{r['name']}(+{r['change']:.0f}%)" for r in sorted(stocks, key=lambda x: -x['change'])[:4])
    print(f"  {sec:14s}: {cnt:>3}종목 평균+{avg_chg:.1f}% | {names}")

# 상승률 상위 종목 상세
print(f"\n{'=' * 100}")
print("  ■ 상승률 TOP 50 — 전일 시그널 상세")
print("=" * 100)
results_sorted = sorted(results, key=lambda x: -x['change'])

print(f"  {'#':>3} {'종목':12s} {'섹터':10s} {'등락':>6s} {'STO':>8s} {'RSI':>5s} {'VR':>5s} {'BB%':>5s} {'낙폭':>5s} {'수급5일':>8s} 주요시그널")
print("-" * 130)

for i, r in enumerate(results_sorted[:50], 1):
    sto = f"{r.get('sto_k','-')}/{r.get('sto_d','-')}"
    rsi = f"{r.get('rsi','-')}"
    vr = f"{r.get('vol_ratio','-')}x"
    bb = f"{r.get('bb_pct','-')}"
    drop = f"{r.get('drop_20d','-')}%"
    supply = f"{r.get('net_supply_5d','-')}억" if r.get('net_supply_5d') is not None else "-"
    key_sigs = [s for s in r['signals'] if s not in ('전일양봉','전일음봉','STO상향','MACD양전환','배열혼조')]
    sig_str = ' | '.join(key_sigs[:6])
    print(f"  {i:>3} {r['name']:12s} {r['sector']:10s} +{r['change']:5.1f}% {sto:>8s} {rsi:>5s} {vr:>5s} {bb:>5s} {drop:>6s} {supply:>8s} {sig_str}")

# 핵심 패턴 분석
print(f"\n{'=' * 100}")
print("  ■ 핵심 패턴 분석 — 어떤 시그널 조합이 급등을 예고했나?")
print("=" * 100)

# 시그널 조합 빈도
combo_counter = Counter()
for r in results:
    key_sigs = sorted(set(s for s in r['signals']
        if s not in ('전일양봉','전일음봉','STO상향','MACD양전환','배열혼조','데이터없음','데이터부족','특이시그널없음')))
    for i in range(len(key_sigs)):
        for j in range(i+1, len(key_sigs)):
            combo_counter[(key_sigs[i], key_sigs[j])] += 1

print(f"\n  ▶ 가장 많이 동시에 나타난 시그널 조합:")
for (s1, s2), cnt in combo_counter.most_common(15):
    print(f"    {s1} + {s2}: {cnt}종목")

# STO 과매도/과매도탈출에서 상승한 종목
sto_oversold = [r for r in results if 'STO과매도' in r['signals'] or 'STO과매도탈출' in r['signals']]
print(f"\n  ▶ STO 과매도/과매도탈출 → 급등: {len(sto_oversold)}종목")
for r in sorted(sto_oversold, key=lambda x: -x['change'])[:10]:
    print(f"    {r['name']:14s} +{r['change']:5.1f}% STO:{r.get('sto_k','-')}/{r.get('sto_d','-')} {r['sector']}")

# 매물소진 패턴 → 급등
exhaust = [r for r in results if '매물소진패턴' in r['signals']]
print(f"\n  ▶ 매물소진패턴 → 급등: {len(exhaust)}종목")
for r in sorted(exhaust, key=lambda x: -x['change'])[:10]:
    print(f"    {r['name']:14s} +{r['change']:5.1f}% VR:{r.get('vol_ratio','-')}x {r['sector']}")

# BB하단 → 급등
bb_lower = [r for r in results if 'BB하단이탈' in r['signals'] or 'BB하단근접' in r['signals']]
print(f"\n  ▶ BB하단이탈/근접 → 급등: {len(bb_lower)}종목")
for r in sorted(bb_lower, key=lambda x: -x['change'])[:10]:
    print(f"    {r['name']:14s} +{r['change']:5.1f}% BB:{r.get('bb_pct','-')}% {r['sector']}")

# 역배열에서 급등
reverse = [r for r in results if '역배열' in r['signals']]
print(f"\n  ▶ 역배열(하락추세)에서 급등: {len(reverse)}종목")

# 정배열에서 급등
aligned = [r for r in results if '정배열' in r['signals']]
print(f"  ▶ 정배열(상승추세)에서 급등: {len(aligned)}종목")

# 쌍끌이 매수
dual = [r for r in results if '쌍끌이매수' in r['signals']]
print(f"\n  ▶ 기관+외인 쌍끌이매수 → 급등: {len(dual)}종목")
for r in sorted(dual, key=lambda x: -x['change'])[:10]:
    print(f"    {r['name']:14s} +{r['change']:5.1f}% 수급:{r.get('net_supply_5d',0):>+,.0f}억 {r['sector']}")

# 시그널 많은 종목 = 예측 가능했던 종목
print(f"\n{'=' * 100}")
print("  ■ 시그널 밀집도 TOP 20 (사전에 가장 많은 시그널이 겹쳤던 종목)")
print("=" * 100)
by_sig_count = sorted(results, key=lambda x: -x.get('signal_count', 0))
for i, r in enumerate(by_sig_count[:20], 1):
    key_sigs = [s for s in r['signals'] if s not in ('전일양봉','전일음봉','STO상향','MACD양전환','배열혼조')]
    print(f"  {i:>2}. {r['name']:14s} +{r['change']:5.1f}% ({r['sector']:10s}) 시그널{len(key_sigs)}개: {' | '.join(key_sigs[:8])}")

print(f"\n{'=' * 100}")
print("  분석 완료")
print("=" * 100)
