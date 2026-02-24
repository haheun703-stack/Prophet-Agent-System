# -*- coding: utf-8 -*-
"""
시그널 패턴 백테스트 v2 — 스토캐스틱 중심 세분화
매일 전일 시그널 스캔 → 다음날/3일/5일 수익률 → 패턴별 승률/평균수익
"""
import sys, io, json, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
os.chdir(os.path.join(os.path.dirname(__file__), ".."))

import pandas as pd
import numpy as np
from collections import defaultdict, Counter


def compute_stochastic(close, high, low, k_period=14, slow_k=3, slow_d=3):
    """스토캐스틱 (K slow, D) 전체 시계열 반환"""
    if len(close) < k_period + slow_k + slow_d:
        return None, None
    k_raw = []
    for i in range(k_period - 1, len(close)):
        h14 = max(high[i-k_period+1:i+1])
        l14 = min(low[i-k_period+1:i+1])
        k_raw.append((close[i] - l14) / (h14 - l14) * 100 if h14 != l14 else 50)
    k_slow = pd.Series(k_raw).rolling(slow_k).mean().values
    d_vals = pd.Series(k_slow).rolling(slow_d).mean().values
    return k_slow, d_vals


def compute_signals(close, high, low, volume, opn):
    """전일 데이터 기준 시그널 계산 — 스토캐스틱 세분화"""
    signals = []
    sto_detail = {}

    # ═══ 1. Stochastic (14,3,3) — 핵심 세분화 ═══
    k_slow, d_vals = compute_stochastic(close, high, low)
    if k_slow is not None and len(k_slow) >= 4:
        k_now = k_slow[-1] if not np.isnan(k_slow[-1]) else 50
        d_now = d_vals[-1] if not np.isnan(d_vals[-1]) else 50
        k_prev = k_slow[-2] if not np.isnan(k_slow[-2]) else 50
        d_prev = d_vals[-2] if not np.isnan(d_vals[-2]) else 50
        k_prev2 = k_slow[-3] if not np.isnan(k_slow[-3]) else 50

        sto_detail = {'k': k_now, 'd': d_now}

        # ── 골든크로스 (K > D 크로스) ──
        golden = k_now > d_now and k_prev <= d_prev

        # ── 골든크로스 위치별 세분화 ──
        if golden:
            if k_now < 20:
                signals.append('STO골든_과매도(0~20)')    # 바닥 골든
            elif k_now < 40:
                signals.append('STO골든_저구간(20~40)')   # 반등초기 골든
            elif k_now < 60:
                signals.append('STO골든_중립(40~60)')     # 중간 골든
            elif k_now < 80:
                signals.append('STO골든_고구간(60~80)')   # 추세중 골든
            else:
                signals.append('STO골든_과매수(80~)')     # 과매수 골든
            signals.append('STO골든')  # 공통 태그

        # ── K>D 상태 (골든 아닌 일반 상향) ──
        elif k_now > d_now:
            signals.append('STO상향')

        # ── 데드크로스 ──
        dead = k_now < d_now and k_prev >= d_prev
        if dead:
            if k_now > 80:
                signals.append('STO데드_과매수(80~)')
            elif k_now > 50:
                signals.append('STO데드_고구간')
            else:
                signals.append('STO데드_저구간')

        # ── K값 상승 전환 (K 하락→상승) ──
        if k_now > k_prev and k_prev <= k_prev2:
            signals.append('STO_K상승전환')
            if k_now < 20:
                signals.append('STO_K상승전환_과매도')
            elif k_now < 40:
                signals.append('STO_K상승전환_저구간')

        # ── K값 하락 전환 ──
        if k_now < k_prev and k_prev >= k_prev2:
            signals.append('STO_K하락전환')

        # ── 영역별 ──
        if k_now < 10:
            signals.append('STO극과매도(<10)')
        elif k_now < 20:
            signals.append('STO과매도(10~20)')
        elif k_now < 30:
            signals.append('STO저구간(20~30)')
        elif k_now < 50:
            signals.append('STO중하(30~50)')
        elif k_now < 70:
            signals.append('STO중상(50~70)')
        elif k_now < 80:
            signals.append('STO고구간(70~80)')
        elif k_now < 90:
            signals.append('STO과매수(80~90)')
        else:
            signals.append('STO극과매수(90~)')

        # ── 과매도 탈출 ──
        if k_prev < 20 and k_now >= 20:
            signals.append('STO과매도탈출')
        if k_prev < 30 and k_now >= 30:
            signals.append('STO30돌파')

        # ── K-D 스프레드 (모멘텀 강도) ──
        spread = k_now - d_now
        if spread > 20:
            signals.append('STO스프레드>20')
        elif spread > 10:
            signals.append('STO스프레드>10')

        # ── K 기울기 (2일간 변화량) ──
        k_slope = k_now - k_prev
        if k_slope > 15:
            signals.append('STO급등(K+15이상)')
        elif k_slope > 8:
            signals.append('STO상승(K+8이상)')

        # ── STO 다이버전스 (가격 신저가 but STO 높아짐) ──
        if len(close) >= 10 and len(k_slow) >= 10:
            price_low5 = min(close[-5:])
            price_low_prev = min(close[-10:-5])
            k_low5 = min(k_slow[-5:]) if not any(np.isnan(k_slow[-5:])) else 50
            k_low_prev = min(k_slow[-10:-5]) if not any(np.isnan(k_slow[-10:-5])) else 50
            if price_low5 < price_low_prev and k_low5 > k_low_prev:
                signals.append('STO상승다이버전스')

    # ═══ 2. RSI ═══
    if len(close) >= 15:
        deltas = np.diff(close[-15:])
        gains = np.where(deltas > 0, deltas, 0)
        losses = np.where(deltas < 0, -deltas, 0)
        avg_gain, avg_loss = gains.mean(), losses.mean()
        rsi = 100 - 100 / (1 + avg_gain / avg_loss) if avg_loss > 0 else 100
        if rsi < 30:
            signals.append('RSI과매도')
        elif rsi < 40:
            signals.append('RSI약세')

    # ═══ 3. 이동평균 ═══
    if len(close) >= 60:
        ma5, ma20, ma60 = close[-5:].mean(), close[-20:].mean(), close[-60:].mean()
        if ma5 > ma20 > ma60:
            signals.append('정배열')
        elif ma5 < ma20 < ma60:
            signals.append('역배열')
        if close[-2] < ma20 and close[-1] >= ma20:
            signals.append('20일선돌파')

    # ═══ 4. 거래량 ═══
    if len(volume) >= 20:
        vol_avg20 = volume[-20:].mean()
        vr = volume[-1] / vol_avg20 if vol_avg20 > 0 else 0
        if vr < 0.3:
            signals.append('극저거래량')
        elif vr < 0.5:
            signals.append('저거래량')
        elif vr > 3:
            signals.append('거래량대폭증')
        elif vr > 1.5:
            signals.append('거래량증가')
        if len(volume) >= 5:
            if volume[-5:].mean() < vol_avg20 * 0.5:
                signals.append('매물소진')

    # ═══ 5. MACD ═══
    if len(close) >= 35:
        ema12 = pd.Series(close).ewm(span=12).mean().values
        ema26 = pd.Series(close).ewm(span=26).mean().values
        macd = ema12 - ema26
        sig = pd.Series(macd).ewm(span=9).mean().values
        if macd[-1] > sig[-1] and macd[-2] <= sig[-2]:
            signals.append('MACD골든')
        if abs(macd[-1] - sig[-1]) / (abs(sig[-1]) + 1) < 0.1:
            signals.append('MACD수렴')

    # ═══ 6. BB ═══
    if len(close) >= 20:
        ma20_bb = close[-20:].mean()
        std20 = close[-20:].std()
        bb_lower = ma20_bb - 2 * std20
        bb_upper = ma20_bb + 2 * std20
        bb_pct = (close[-1] - bb_lower) / (bb_upper - bb_lower) * 100 if bb_upper != bb_lower else 50
        if bb_pct < 5:
            signals.append('BB하단이탈')
        elif bb_pct < 20:
            signals.append('BB하단근접')

    # ═══ 7. 낙폭 ═══
    if len(close) >= 20:
        chg = (close[-1] / max(close[-20:]) - 1) * 100
        if chg < -25:
            signals.append('20일낙폭>25%')
        elif chg < -15:
            signals.append('20일낙폭>15%')
        elif chg < -10:
            signals.append('20일낙폭>10%')

    # ═══ 8. 캔들 ═══
    if len(close) >= 3:
        body = abs(close[-1] - opn[-1])
        tr = high[-1] - low[-1]
        if tr > 0:
            lower_shadow = min(close[-1], opn[-1]) - low[-1]
            if body < tr * 0.2 and lower_shadow > tr * 0.5:
                signals.append('망치형')
        neg = 0
        for i in range(len(close)-1, max(len(close)-8, -1), -1):
            if close[i] < opn[i]:
                neg += 1
            else:
                break
        if neg >= 3:
            signals.append('연속음봉')

    return signals, sto_detail


def run_backtest():
    import glob as gl

    csv_files = gl.glob('data_store/daily/*.csv')
    with open('data_store/universe.json', 'r', encoding='utf-8') as f:
        uni = json.load(f)

    print(f"백테스트 대상: {len(csv_files)}종목")

    signal_returns_1d = defaultdict(list)
    signal_returns_3d = defaultdict(list)
    signal_returns_5d = defaultdict(list)
    combo_returns = defaultdict(list)

    # ── 스토캐스틱 특화 패턴 ──
    sto_patterns = {
        'P1: STO골든_과매도+매물소진':        {'1d': [], '3d': [], '5d': []},
        'P2: STO골든_저구간+정배열':          {'1d': [], '3d': [], '5d': []},
        'P3: STO골든_중립+정배열+거래량↑':    {'1d': [], '3d': [], '5d': []},
        'P4: STO과매도탈출+거래량폭증':       {'1d': [], '3d': [], '5d': []},
        'P5: STO_K상승전환_과매도+매물소진':   {'1d': [], '3d': [], '5d': []},
        'P6: STO상승다이버전스+매물소진':       {'1d': [], '3d': [], '5d': []},
        'P7: STO골든+MACD골든(더블골든)':    {'1d': [], '3d': [], '5d': []},
        'P8: STO극과매도+BB하단+연속음봉':    {'1d': [], '3d': [], '5d': []},
        'P9: STO과매수+정배열+거래량↑':       {'1d': [], '3d': [], '5d': []},
        'P10: STO30돌파+정배열':             {'1d': [], '3d': [], '5d': []},
        'P11: STO급등+정배열':               {'1d': [], '3d': [], '5d': []},
        'P12: STO골든_어디든+정배열':         {'1d': [], '3d': [], '5d': []},
    }
    random_returns = {'1d': [], '3d': [], '5d': []}

    processed = 0
    total_signals = 0

    for fpath in csv_files:
        code = os.path.basename(fpath).replace('.csv', '')
        if code not in uni:
            continue
        df = pd.read_csv(fpath)
        if len(df) < 70:
            continue
        df = df.sort_values('날짜').reset_index(drop=True)
        close = df['종가'].values.astype(float)
        high_arr = df['고가'].values.astype(float)
        low_arr = df['저가'].values.astype(float)
        vol_arr = df['거래량'].values.astype(float)
        opn_arr = df['시가'].values.astype(float)
        processed += 1

        scan_start = max(60, len(df) - 150)
        scan_end = len(df) - 1

        for idx in range(scan_start, scan_end):
            c = close[:idx+1]
            h = high_arr[:idx+1]
            l = low_arr[:idx+1]
            v = vol_arr[:idx+1]
            o = opn_arr[:idx+1]

            sigs, sto = compute_signals(c, h, l, v, o)
            if not sigs:
                continue

            ret_1d = (close[idx+1] / close[idx] - 1) * 100
            ret_3d = (close[min(idx+3, len(close)-1)] / close[idx] - 1) * 100 if idx+3 < len(close) else None
            ret_5d = (close[min(idx+5, len(close)-1)] / close[idx] - 1) * 100 if idx+5 < len(close) else None

            random_returns['1d'].append(ret_1d)
            if ret_3d is not None:
                random_returns['3d'].append(ret_3d)
            if ret_5d is not None:
                random_returns['5d'].append(ret_5d)
            total_signals += 1

            for s in sigs:
                signal_returns_1d[s].append(ret_1d)
                if ret_3d is not None:
                    signal_returns_3d[s].append(ret_3d)
                if ret_5d is not None:
                    signal_returns_5d[s].append(ret_5d)

            sig_set = set(sigs)

            def add_pattern(name):
                sto_patterns[name]['1d'].append(ret_1d)
                if ret_3d is not None:
                    sto_patterns[name]['3d'].append(ret_3d)
                if ret_5d is not None:
                    sto_patterns[name]['5d'].append(ret_5d)

            # P1: STO골든 과매도 + 매물소진
            if 'STO골든_과매도(0~20)' in sig_set and '매물소진' in sig_set:
                add_pattern('P1: STO골든_과매도+매물소진')

            # P2: STO골든 저구간 + 정배열
            if 'STO골든_저구간(20~40)' in sig_set and '정배열' in sig_set:
                add_pattern('P2: STO골든_저구간+정배열')

            # P3: STO골든 중립 + 정배열 + 거래량↑
            if 'STO골든_중립(40~60)' in sig_set and '정배열' in sig_set and \
               ('거래량증가' in sig_set or '거래량대폭증' in sig_set):
                add_pattern('P3: STO골든_중립+정배열+거래량↑')

            # P4: STO과매도탈출 + 거래량폭증
            if 'STO과매도탈출' in sig_set and '거래량대폭증' in sig_set:
                add_pattern('P4: STO과매도탈출+거래량폭증')

            # P5: K상승전환 과매도 + 매물소진
            if 'STO_K상승전환_과매도' in sig_set and '매물소진' in sig_set:
                add_pattern('P5: STO_K상승전환_과매도+매물소진')

            # P6: 상승다이버전스 + 매물소진
            if 'STO상승다이버전스' in sig_set and '매물소진' in sig_set:
                add_pattern('P6: STO상승다이버전스+매물소진')

            # P7: STO골든 + MACD골든 (더블 골든크로스)
            if 'STO골든' in sig_set and 'MACD골든' in sig_set:
                add_pattern('P7: STO골든+MACD골든(더블골든)')

            # P8: 극과매도 + BB하단 + 연속음봉
            if 'STO극과매도(<10)' in sig_set and \
               ('BB하단이탈' in sig_set or 'BB하단근접' in sig_set) and '연속음봉' in sig_set:
                add_pattern('P8: STO극과매도+BB하단+연속음봉')

            # P9: STO과매수 + 정배열 + 거래량↑
            if ('STO과매수(80~90)' in sig_set or 'STO극과매수(90~)' in sig_set) and \
               '정배열' in sig_set and ('거래량증가' in sig_set or '거래량대폭증' in sig_set):
                add_pattern('P9: STO과매수+정배열+거래량↑')

            # P10: STO30돌파 + 정배열
            if 'STO30돌파' in sig_set and '정배열' in sig_set:
                add_pattern('P10: STO30돌파+정배열')

            # P11: STO급등 + 정배열
            if 'STO급등(K+15이상)' in sig_set and '정배열' in sig_set:
                add_pattern('P11: STO급등+정배열')

            # P12: STO골든 어디든 + 정배열
            if 'STO골든' in sig_set and '정배열' in sig_set:
                add_pattern('P12: STO골든_어디든+정배열')

            # 2-시그널 조합
            key_sigs = sorted(s for s in sigs if s not in ('STO상향',))
            for i in range(len(key_sigs)):
                for j in range(i+1, min(i+4, len(key_sigs))):
                    combo_returns[(key_sigs[i], key_sigs[j])].append(ret_1d)

    # ═══════════════════════════ REPORT ═══════════════════════════
    print("=" * 120)
    print("  시그널 패턴 백테스트 v2 — 스토캐스틱 중심 세분화")
    print("=" * 120)
    print(f"  분석: {processed}종목, {total_signals:,}건")
    base_1d = np.mean(random_returns['1d'])
    base_wr = sum(1 for r in random_returns['1d'] if r > 0) / len(random_returns['1d']) * 100
    print(f"  기준선: 1일 {base_1d:+.2f}% 승률 {base_wr:.1f}%")

    # ── SECTION 1: 스토캐스틱 영역별 수익률 ──
    print(f"\n{'=' * 120}")
    print("  ■ [1] 스토캐스틱 영역별 다음날 수익률")
    print("=" * 120)
    sto_zones = [
        'STO극과매도(<10)', 'STO과매도(10~20)', 'STO저구간(20~30)',
        'STO중하(30~50)', 'STO중상(50~70)', 'STO고구간(70~80)',
        'STO과매수(80~90)', 'STO극과매수(90~)'
    ]
    print(f"  {'영역':22s} {'건수':>7s} {'1일평균':>8s} {'승률':>6s} {'3일':>8s} {'5일':>8s} {'5%+':>6s}")
    print("-" * 90)
    for z in sto_zones:
        if z in signal_returns_1d and len(signal_returns_1d[z]) >= 30:
            r = signal_returns_1d[z]
            avg1 = np.mean(r)
            win = sum(1 for x in r if x > 0) / len(r) * 100
            big = sum(1 for x in r if x >= 5) / len(r) * 100
            avg3 = np.mean(signal_returns_3d[z]) if signal_returns_3d[z] else 0
            avg5 = np.mean(signal_returns_5d[z]) if signal_returns_5d[z] else 0
            marker = " ◀ BEST" if avg1 == max(np.mean(signal_returns_1d[zz]) for zz in sto_zones if zz in signal_returns_1d and len(signal_returns_1d[zz]) >= 30) else ""
            print(f"  {z:22s} {len(r):>6,}건 {avg1:>+7.2f}% {win:>5.1f}% {avg3:>+7.2f}% {avg5:>+7.2f}% {big:>5.1f}%{marker}")

    # ── SECTION 2: STO 골든크로스 위치별 ──
    print(f"\n{'=' * 120}")
    print("  ■ [2] STO 골든크로스 — 어디서 발생하느냐에 따른 수익률 차이")
    print("=" * 120)
    golden_zones = [
        'STO골든_과매도(0~20)', 'STO골든_저구간(20~40)',
        'STO골든_중립(40~60)', 'STO골든_고구간(60~80)', 'STO골든_과매수(80~)'
    ]
    print(f"  {'골든위치':24s} {'건수':>7s} {'1일':>8s} {'승률':>6s} {'3일':>8s} {'5일':>8s} {'5%+':>6s}")
    print("-" * 90)
    for z in golden_zones:
        if z in signal_returns_1d and len(signal_returns_1d[z]) >= 20:
            r = signal_returns_1d[z]
            avg1 = np.mean(r)
            win = sum(1 for x in r if x > 0) / len(r) * 100
            big = sum(1 for x in r if x >= 5) / len(r) * 100
            avg3 = np.mean(signal_returns_3d[z]) if signal_returns_3d[z] else 0
            avg5 = np.mean(signal_returns_5d[z]) if signal_returns_5d[z] else 0
            print(f"  {z:24s} {len(r):>6,}건 {avg1:>+7.2f}% {win:>5.1f}% {avg3:>+7.2f}% {avg5:>+7.2f}% {big:>5.1f}%")

    # ── SECTION 3: STO 행동 시그널 ──
    print(f"\n{'=' * 120}")
    print("  ■ [3] STO 행동 시그널별 수익률")
    print("=" * 120)
    sto_actions = [
        'STO골든', 'STO과매도탈출', 'STO30돌파',
        'STO_K상승전환', 'STO_K상승전환_과매도', 'STO_K상승전환_저구간',
        'STO_K하락전환', 'STO상승다이버전스',
        'STO스프레드>20', 'STO스프레드>10',
        'STO급등(K+15이상)', 'STO상승(K+8이상)',
        'STO데드_과매수(80~)', 'STO데드_고구간', 'STO데드_저구간',
    ]
    print(f"  {'시그널':26s} {'건수':>7s} {'1일':>8s} {'승률':>6s} {'3일':>8s} {'5일':>8s} {'5%+':>6s}")
    print("-" * 100)
    for z in sto_actions:
        if z in signal_returns_1d and len(signal_returns_1d[z]) >= 30:
            r = signal_returns_1d[z]
            avg1 = np.mean(r)
            win = sum(1 for x in r if x > 0) / len(r) * 100
            big = sum(1 for x in r if x >= 5) / len(r) * 100
            avg3 = np.mean(signal_returns_3d[z]) if signal_returns_3d[z] else 0
            avg5 = np.mean(signal_returns_5d[z]) if signal_returns_5d[z] else 0
            print(f"  {z:26s} {len(r):>6,}건 {avg1:>+7.2f}% {win:>5.1f}% {avg3:>+7.2f}% {avg5:>+7.2f}% {big:>5.1f}%")

    # ── SECTION 4: STO 중심 복합 패턴 ──
    print(f"\n{'=' * 120}")
    print("  ■ [4] STO 중심 복합 패턴 — 핵심 전략")
    print("=" * 120)
    print(f"  {'패턴':40s} {'건수':>7s} {'1일':>8s} {'승률':>6s} {'3일':>8s} {'5일':>8s} {'5%+':>7s} {'vs기준':>7s}")
    print("-" * 120)

    for name, rets in sorted(sto_patterns.items(), key=lambda x: -np.mean(x[1]['1d']) if x[1]['1d'] else 0):
        if not rets['1d']:
            continue
        r1 = rets['1d']
        avg1 = np.mean(r1)
        win = sum(1 for x in r1 if x > 0) / len(r1) * 100
        big = sum(1 for x in r1 if x >= 5) / len(r1) * 100
        avg3 = np.mean(rets['3d']) if rets['3d'] else 0
        avg5 = np.mean(rets['5d']) if rets['5d'] else 0
        ratio = avg1 / base_1d if base_1d != 0 else 0
        print(f"  {name:40s} {len(r1):>6,}건 {avg1:>+7.2f}% {win:>5.1f}% {avg3:>+7.2f}% {avg5:>+7.2f}% {big:>6.1f}% {ratio:>6.1f}x")

    # ── SECTION 5: 기타 시그널별 (비STO) ──
    print(f"\n{'=' * 120}")
    print("  ■ [5] 기타 시그널별 수익률 (보조참고)")
    print("=" * 120)
    other_sigs = [
        '정배열', '역배열', '매물소진', '저거래량', '극저거래량',
        '거래량증가', '거래량대폭증', 'MACD골든', 'MACD수렴',
        'BB하단이탈', 'BB하단근접', '20일낙폭>10%', '20일낙폭>15%', '20일낙폭>25%',
        '20일선돌파', '연속음봉', '망치형', 'RSI과매도', 'RSI약세',
    ]
    print(f"  {'시그널':18s} {'건수':>7s} {'1일':>8s} {'승률':>6s} {'3일':>8s} {'5일':>8s} {'5%+':>6s}")
    print("-" * 80)
    for z in other_sigs:
        if z in signal_returns_1d and len(signal_returns_1d[z]) >= 30:
            r = signal_returns_1d[z]
            avg1 = np.mean(r)
            win = sum(1 for x in r if x > 0) / len(r) * 100
            big = sum(1 for x in r if x >= 5) / len(r) * 100
            avg3 = np.mean(signal_returns_3d[z]) if signal_returns_3d[z] else 0
            avg5 = np.mean(signal_returns_5d[z]) if signal_returns_5d[z] else 0
            print(f"  {z:18s} {len(r):>6,}건 {avg1:>+7.2f}% {win:>5.1f}% {avg3:>+7.2f}% {avg5:>+7.2f}% {big:>5.1f}%")

    # ── SECTION 6: STO 포함 2-시그널 조합 TOP ──
    print(f"\n{'=' * 120}")
    print("  ■ [6] STO 포함 2-시그널 조합 TOP 25 (최소 50건)")
    print("=" * 120)
    print(f"  {'조합':46s} {'건수':>7s} {'1일':>8s} {'승률':>6s} {'5%+':>6s}")
    print("-" * 90)

    sto_combos = []
    for (s1, s2), rets in combo_returns.items():
        if len(rets) < 50:
            continue
        if not ('STO' in s1 or 'STO' in s2):
            continue
        avg = np.mean(rets)
        win = sum(1 for r in rets if r > 0) / len(rets) * 100
        big = sum(1 for r in rets if r >= 5) / len(rets) * 100
        sto_combos.append((f"{s1} + {s2}", len(rets), avg, win, big))

    sto_combos.sort(key=lambda x: -x[2])
    for name, cnt, avg, win, big in sto_combos[:25]:
        print(f"  {name:46s} {cnt:>6,}건 {avg:>+7.2f}% {win:>5.1f}% {big:>5.1f}%")

    print(f"\n  ▶ STO 포함 조합 WORST 10")
    print("-" * 90)
    for name, cnt, avg, win, big in sto_combos[-10:]:
        print(f"  {name:46s} {cnt:>6,}건 {avg:>+7.2f}% {win:>5.1f}% {big:>5.1f}%")

    # ── SECTION 7: 수익분포 ──
    best_pattern = max(sto_patterns.items(), key=lambda x: np.mean(x[1]['1d']) if x[1]['1d'] else -999)
    print(f"\n{'=' * 120}")
    print(f"  ■ [7] 최고 패턴 [{best_pattern[0]}] 수익 분포")
    print("=" * 120)
    if best_pattern[1]['1d']:
        r1 = best_pattern[1]['1d']
        bins = [(-99, -5), (-5, -3), (-3, -1), (-1, 0), (0, 1), (1, 3), (3, 5), (5, 10), (10, 99)]
        for lo, hi in bins:
            cnt = sum(1 for r in r1 if lo <= r < hi)
            pct = cnt / len(r1) * 100
            bar = "█" * int(pct)
            label = f"{hi:+d}%이하" if lo == -99 else (f"{lo:+d}%이상" if hi == 99 else f"{lo:+d}~{hi:+d}%")
            print(f"  {label:>12s}: {cnt:>5}건 ({pct:5.1f}%) {bar}")

    print(f"\n{'=' * 120}")
    print("  백테스트 완료")
    print("=" * 120)


if __name__ == "__main__":
    run_backtest()
