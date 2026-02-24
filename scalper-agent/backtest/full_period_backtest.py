# -*- coding: utf-8 -*-
"""
전기간 백테스트 (2025-03 ~ 현재) — STO 인사이트 적용 스캐너 v3
매일 장마감 시그널 스캔 → 다음날 시가 매수 → 1일/3일/5일 수익 측정
"""
import sys, io, json, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
os.chdir(os.path.join(os.path.dirname(__file__), ".."))

import pandas as pd
import numpy as np
from collections import defaultdict
import glob as gl


def compute_stochastic(close, high, low, k_period=14, slow_k=3, slow_d=3):
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


def score_stock(close, high, low, volume, opn):
    """
    스캐너 v3 스코어링 — STO 인사이트 적용
    Returns: (score, signals_list, details_dict) or None
    """
    if len(close) < 65:
        return None

    score = 0
    signals = []
    details = {}

    # ═══ 1. 스토캐스틱 (핵심 지표) ═══
    k_slow, d_vals = compute_stochastic(close, high, low)
    if k_slow is None:
        return None

    k_now = k_slow[-1] if not np.isnan(k_slow[-1]) else 50
    d_now = d_vals[-1] if not np.isnan(d_vals[-1]) else 50
    k_prev = k_slow[-2] if not np.isnan(k_slow[-2]) else 50
    d_prev = d_vals[-2] if not np.isnan(d_vals[-2]) else 50
    k_prev2 = k_slow[-3] if not np.isnan(k_slow[-3]) else 50

    details['sto_k'] = round(k_now, 1)
    details['sto_d'] = round(d_now, 1)

    golden = k_now > d_now and k_prev <= d_prev
    k_rising = k_now > k_prev and k_prev <= k_prev2
    spread = k_now - d_now

    # STO 영역 가중치 (백테스트 결과: 90+가 최강)
    if k_now >= 90:
        score += 12
        signals.append('STO극과매수(90+)')
    elif k_now >= 80:
        score += 8
        signals.append('STO과매수(80~90)')
    elif k_now >= 50:
        score += 5
    elif k_now >= 20:
        score += 2
    else:
        score += 3  # 과매도는 반등 기대

    # STO 골든크로스 위치별 (중립 40~60이 best)
    if golden:
        if 40 <= k_now < 60:
            score += 15
            signals.append('STO골든_중립★')
        elif 20 <= k_now < 40:
            score += 12
            signals.append('STO골든_저구간')
        elif k_now < 20:
            score += 10
            signals.append('STO골든_과매도')
        elif k_now >= 80:
            score += 8
            signals.append('STO골든_과매수')
        else:
            score += 10
            signals.append('STO골든_고구간')

    # STO 스프레드 (K-D) — 최강 시그널
    if spread > 20:
        score += 15
        signals.append(f'STO스프레드>20({spread:.0f})')
    elif spread > 10:
        score += 8
        signals.append('STO스프레드>10')

    # K값 상승전환
    if k_rising:
        score += 5
        signals.append('STO_K상승전환')
        if k_now < 20:
            score += 3
            signals.append('STO_K반등_과매도')

    # STO 급등 (K +15 이상)
    k_slope = k_now - k_prev
    if k_slope > 15:
        score += 10
        signals.append('STO급등')
    elif k_slope > 8:
        score += 5

    # 과매도 탈출
    if k_prev < 20 and k_now >= 20:
        score += 8
        signals.append('STO과매도탈출')

    # STO 다이버전스
    if len(k_slow) >= 10:
        try:
            valid_k = [x for x in k_slow[-5:] if not np.isnan(x)]
            valid_k_prev = [x for x in k_slow[-10:-5] if not np.isnan(x)]
            if valid_k and valid_k_prev:
                price_low5 = min(close[-5:])
                price_low_prev = min(close[-10:-5])
                k_low5 = min(valid_k)
                k_low_prev = min(valid_k_prev)
                if price_low5 < price_low_prev and k_low5 > k_low_prev:
                    score += 8
                    signals.append('STO상승다이버전스')
        except:
            pass

    # ═══ 2. 거래량 분석 ═══
    vol_avg20 = volume[-20:].mean()
    vol_now = volume[-1]
    vr = vol_now / vol_avg20 if vol_avg20 > 0 else 0
    details['vr'] = round(vr, 2)

    # 매물소진 패턴
    vol_5d = volume[-5:].mean()
    is_exhaust = vol_5d < vol_avg20 * 0.5

    if vr > 3:
        score += 10
        signals.append('거래량대폭증')
        # STO과매도탈출 + 거래량폭증 = P4 (4.0x)
        if 'STO과매도탈출' in signals:
            score += 10
            signals.append('★P4:과매도탈출+폭증')
    elif vr > 1.5:
        score += 6
        signals.append('거래량증가')
    elif vr < 0.3:
        signals.append('극저거래량')
        if k_now >= 90:
            # STO극과매수 + 극저거래량 = 최강 (3.29%)
            score += 15
            signals.append('★극과매수+극저거래량')
        elif is_exhaust:
            score += 5
    elif vr < 0.5:
        signals.append('저거래량')

    if is_exhaust:
        score += 5
        signals.append('매물소진')

    # ═══ 3. 이동평균 ═══
    ma5, ma20, ma60 = close[-5:].mean(), close[-20:].mean(), close[-60:].mean()
    aligned = ma5 > ma20 > ma60

    if aligned:
        score += 8
        signals.append('정배열')
    elif ma5 < ma20 < ma60:
        signals.append('역배열')

    # STO과매수 + 정배열 + 거래량↑ = P9 (2.3x)
    if k_now >= 80 and aligned and vr > 1.5:
        score += 10
        signals.append('★P9:과매수+정배열+거래량↑')

    # STO골든_중립 + 정배열 + 거래량↑ = P3 (1.9x, 5일+3.38%)
    if golden and 40 <= k_now < 60 and aligned and vr > 1.5:
        score += 8
        signals.append('★P3:골든중립+정배열+거래량↑')

    # ═══ 4. MACD ═══
    if len(close) >= 35:
        ema12 = pd.Series(close).ewm(span=12).mean().values
        ema26 = pd.Series(close).ewm(span=26).mean().values
        macd = ema12 - ema26
        sig = pd.Series(macd).ewm(span=9).mean().values
        macd_golden = macd[-1] > sig[-1] and macd[-2] <= sig[-2]

        if macd_golden:
            score += 8
            signals.append('MACD골든')
            # 더블 골든 (STO + MACD)
            if golden:
                score += 5
                signals.append('★더블골든')

    # ═══ 5. 볼린저밴드 ═══
    ma20_bb = close[-20:].mean()
    std20 = close[-20:].std()
    bb_lower = ma20_bb - 2 * std20
    bb_upper = ma20_bb + 2 * std20
    bb_pct = (close[-1] - bb_lower) / (bb_upper - bb_lower) * 100 if bb_upper != bb_lower else 50
    details['bb_pct'] = round(bb_pct, 1)

    if bb_pct < 5:
        score += 5
        signals.append('BB하단이탈')
    elif bb_pct < 20:
        score += 3
        signals.append('BB하단근접')

    # ═══ 6. 낙폭 ═══
    high20 = max(close[-20:])
    drop20 = (close[-1] / high20 - 1) * 100
    details['drop20'] = round(drop20, 1)

    # 20일낙폭>15% + 정배열 = +0.98% (좋음)
    if drop20 < -15 and aligned:
        score += 8
        signals.append('눌림목(낙폭15%+정배열)')
    elif drop20 < -15:
        score += 3
        signals.append('낙폭>15%')

    # ═══ 7. 연속음봉 ═══
    neg = 0
    for i in range(len(close)-1, max(len(close)-8, -1), -1):
        if close[i] < opn[i]:
            neg += 1
        else:
            break
    if neg >= 3:
        signals.append(f'연속음봉{neg}일')
        # STO극과매도 + BB하단 + 연속음봉 = P8 (바닥)
        if k_now < 10 and bb_pct < 20:
            score += 5
            signals.append('★P8:극과매도+BB하단+연속음봉')

    # ═══ 최소 점수 필터 ═══
    # 최소 거래대금 필터 (일 10억 이상)
    avg_turnover = (close[-20:] * volume[-20:]).mean()
    if avg_turnover < 10e8:
        return None

    details['score'] = score
    details['signals'] = signals
    return (score, signals, details)


def run_full_backtest():
    csv_files = gl.glob('data_store/daily/*.csv')
    with open('data_store/universe.json', 'r', encoding='utf-8') as f:
        uni = json.load(f)

    # 모든 종목 데이터 로드
    all_data = {}
    for fpath in csv_files:
        code = os.path.basename(fpath).replace('.csv', '')
        if code not in uni:
            continue
        df = pd.read_csv(fpath).sort_values('날짜').reset_index(drop=True)
        if len(df) < 70:
            continue
        all_data[code] = df

    print(f"로드: {len(all_data)}종목")

    # 날짜 목록 (공통)
    sample = list(all_data.values())[0]
    all_dates = sample['날짜'].tolist()

    # 2025-06-01 이후부터 스캔 시작 (60일 lookback 필요)
    scan_start_date = '2025-06-01'
    start_idx = next((i for i, d in enumerate(all_dates) if d >= scan_start_date), 60)

    print(f"스캔기간: {all_dates[start_idx]} ~ {all_dates[-2]} ({len(all_dates) - start_idx - 1}일)")

    # 결과 저장
    daily_results = []  # 매일 스캔 결과
    all_trades = []     # 전체 매매 기록

    # 점수 구간별
    tier_returns = {
        'S(90+)': {'1d': [], '3d': [], '5d': []},
        'A(70~89)': {'1d': [], '3d': [], '5d': []},
        'B(50~69)': {'1d': [], '3d': [], '5d': []},
        'C(30~49)': {'1d': [], '3d': [], '5d': []},
        'ALL': {'1d': [], '3d': [], '5d': []},
    }

    # 패턴별
    pattern_returns = defaultdict(lambda: {'1d': [], '3d': [], '5d': []})

    for day_idx in range(start_idx, len(all_dates) - 1):
        date = all_dates[day_idx]
        next_date = all_dates[day_idx + 1] if day_idx + 1 < len(all_dates) else None
        date_3d = all_dates[min(day_idx + 3, len(all_dates) - 1)]
        date_5d = all_dates[min(day_idx + 5, len(all_dates) - 1)]

        day_picks = []

        for code, df in all_data.items():
            # 이 종목의 해당 날짜 인덱스 찾기
            date_mask = df['날짜'] <= date
            if date_mask.sum() < 65:
                continue

            idx = date_mask.sum() - 1
            c = df['종가'].values[:idx+1].astype(float)
            h = df['고가'].values[:idx+1].astype(float)
            l = df['저가'].values[:idx+1].astype(float)
            v = df['거래량'].values[:idx+1].astype(float)
            o = df['시가'].values[:idx+1].astype(float)

            result = score_stock(c, h, l, v, o)
            if result is None:
                continue

            sc, sigs, det = result
            if sc < 30:  # 최소 점수 기준
                continue

            # 다음날 수익률 계산
            close_today = c[-1]
            future = df[df['날짜'] > date]
            if len(future) == 0:
                continue

            ret_1d = (future.iloc[0]['종가'] / close_today - 1) * 100
            ret_3d = (future.iloc[min(2, len(future)-1)]['종가'] / close_today - 1) * 100 if len(future) >= 1 else None
            ret_5d = (future.iloc[min(4, len(future)-1)]['종가'] / close_today - 1) * 100 if len(future) >= 3 else None

            trade = {
                'date': date,
                'code': code,
                'name': uni[code]['name'],
                'sector': uni[code].get('sub_sector', ''),
                'score': sc,
                'ret_1d': ret_1d,
                'ret_3d': ret_3d,
                'ret_5d': ret_5d,
                'signals': sigs,
                'sto_k': det.get('sto_k', 0),
            }
            day_picks.append(trade)
            all_trades.append(trade)

            # 티어별 분류
            tier = 'S(90+)' if sc >= 90 else 'A(70~89)' if sc >= 70 else 'B(50~69)' if sc >= 50 else 'C(30~49)'
            tier_returns[tier]['1d'].append(ret_1d)
            if ret_3d is not None:
                tier_returns[tier]['3d'].append(ret_3d)
            if ret_5d is not None:
                tier_returns[tier]['5d'].append(ret_5d)

            tier_returns['ALL']['1d'].append(ret_1d)
            if ret_3d is not None:
                tier_returns['ALL']['3d'].append(ret_3d)
            if ret_5d is not None:
                tier_returns['ALL']['5d'].append(ret_5d)

            # 특수 패턴별
            for sig in sigs:
                if sig.startswith('★'):
                    pattern_returns[sig]['1d'].append(ret_1d)
                    if ret_3d is not None:
                        pattern_returns[sig]['3d'].append(ret_3d)
                    if ret_5d is not None:
                        pattern_returns[sig]['5d'].append(ret_5d)

        if day_picks:
            top = sorted(day_picks, key=lambda x: -x['score'])[:5]
            daily_results.append({
                'date': date,
                'total_picks': len(day_picks),
                'avg_score': np.mean([p['score'] for p in day_picks]),
                'top5': top,
            })

    # ═══════════════════════ REPORT ═══════════════════════
    print("\n" + "=" * 120)
    print("  전기간 백테스트 — STO 인사이트 적용 스캐너 v3")
    print(f"  기간: {all_dates[start_idx]} ~ {all_dates[-2]}")
    print("=" * 120)
    print(f"  총 매매: {len(all_trades):,}건, 일평균 {len(all_trades)/max(len(daily_results),1):.0f}종목 선정")

    # ── 티어별 성과 ──
    print(f"\n{'=' * 120}")
    print("  ■ [1] 점수 티어별 수익률")
    print("=" * 120)
    print(f"  {'티어':12s} {'건수':>8s} {'1일평균':>8s} {'1일승률':>8s} {'3일평균':>8s} {'5일평균':>8s} {'5%+':>7s} {'10%+':>7s} {'손실5%+':>7s}")
    print("-" * 110)

    for tier_name in ['S(90+)', 'A(70~89)', 'B(50~69)', 'C(30~49)', 'ALL']:
        r = tier_returns[tier_name]
        if not r['1d']:
            continue
        r1 = r['1d']
        avg1 = np.mean(r1)
        med1 = np.median(r1)
        win = sum(1 for x in r1 if x > 0) / len(r1) * 100
        big5 = sum(1 for x in r1 if x >= 5) / len(r1) * 100
        big10 = sum(1 for x in r1 if x >= 10) / len(r1) * 100
        loss5 = sum(1 for x in r1 if x <= -5) / len(r1) * 100
        avg3 = np.mean(r['3d']) if r['3d'] else 0
        avg5 = np.mean(r['5d']) if r['5d'] else 0
        print(f"  {tier_name:12s} {len(r1):>7,}건 {avg1:>+7.2f}% {win:>7.1f}% {avg3:>+7.2f}% {avg5:>+7.2f}% {big5:>6.1f}% {big10:>6.1f}% {loss5:>6.1f}%")

    # ── 특수 패턴별 성과 ──
    print(f"\n{'=' * 120}")
    print("  ■ [2] ★특수패턴별 수익률")
    print("=" * 120)
    print(f"  {'패턴':30s} {'건수':>7s} {'1일':>8s} {'승률':>7s} {'3일':>8s} {'5일':>8s} {'5%+':>6s}")
    print("-" * 90)

    for pname, rets in sorted(pattern_returns.items(), key=lambda x: -np.mean(x[1]['1d']) if x[1]['1d'] else 0):
        if not rets['1d']:
            continue
        r1 = rets['1d']
        avg1 = np.mean(r1)
        win = sum(1 for x in r1 if x > 0) / len(r1) * 100
        avg3 = np.mean(rets['3d']) if rets['3d'] else 0
        avg5 = np.mean(rets['5d']) if rets['5d'] else 0
        big = sum(1 for x in r1 if x >= 5) / len(r1) * 100
        print(f"  {pname:30s} {len(r1):>6,}건 {avg1:>+7.2f}% {win:>6.1f}% {avg3:>+7.2f}% {avg5:>+7.2f}% {big:>5.1f}%")

    # ── 월별 성과 ──
    print(f"\n{'=' * 120}")
    print("  ■ [3] 월별 성과 (전체 선정 종목)")
    print("=" * 120)
    monthly = defaultdict(list)
    for t in all_trades:
        ym = t['date'][:7]
        monthly[ym].append(t['ret_1d'])

    print(f"  {'월':10s} {'매매수':>7s} {'1일평균':>8s} {'승률':>7s} {'5%+':>6s} {'누적수익':>10s}")
    print("-" * 70)
    cum = 0
    for ym in sorted(monthly.keys()):
        r = monthly[ym]
        avg1 = np.mean(r)
        win = sum(1 for x in r if x > 0) / len(r) * 100
        big = sum(1 for x in r if x >= 5) / len(r) * 100
        cum += avg1 * len(r) / 100
        print(f"  {ym:10s} {len(r):>6,}건 {avg1:>+7.2f}% {win:>6.1f}% {big:>5.1f}%  {cum:>+9.1f}%")

    # ── 포트폴리오 시뮬레이션 (매일 TOP5 균등매수 → 1일 보유) ──
    print(f"\n{'=' * 120}")
    print("  ■ [4] 포트폴리오 시뮬: 매일 TOP5 균등매수 → 1일 보유")
    print("=" * 120)

    capital = 10000  # 1천만원 기준 (단위: 만원)
    equity_curve = [capital]
    daily_pnl = []
    win_days = 0
    total_days = 0

    for dr in daily_results:
        top5 = dr['top5'][:5]
        if not top5:
            continue
        # 각 종목에 균등 배분
        rets = [t['ret_1d'] for t in top5]
        day_ret = np.mean(rets) / 100
        capital *= (1 + day_ret)
        equity_curve.append(capital)
        daily_pnl.append(day_ret * 100)
        total_days += 1
        if day_ret > 0:
            win_days += 1

    total_ret = (equity_curve[-1] / equity_curve[0] - 1) * 100
    max_dd = 0
    peak = equity_curve[0]
    for eq in equity_curve:
        if eq > peak:
            peak = eq
        dd = (eq / peak - 1) * 100
        if dd < max_dd:
            max_dd = dd

    print(f"  초기자본: 1,000만원")
    print(f"  최종자본: {equity_curve[-1]/100:,.0f}만원")
    print(f"  총수익률: {total_ret:+.1f}%")
    print(f"  거래일수: {total_days}일")
    print(f"  일승률:   {win_days/total_days*100:.1f}% ({win_days}/{total_days})")
    print(f"  일평균:   {np.mean(daily_pnl):+.2f}%")
    print(f"  최대낙폭:  {max_dd:.1f}%")
    if daily_pnl:
        sharpe = np.mean(daily_pnl) / np.std(daily_pnl) * np.sqrt(220) if np.std(daily_pnl) > 0 else 0
        print(f"  샤프비율:  {sharpe:.2f}")

    # ── TOP5 시뮬 월별 ──
    print(f"\n  ▶ TOP5 포트폴리오 월별 수익:")
    monthly_port = defaultdict(list)
    for i, dr in enumerate(daily_results):
        top5 = dr['top5'][:5]
        if not top5:
            continue
        ym = dr['date'][:7]
        monthly_port[ym].append(np.mean([t['ret_1d'] for t in top5]))

    print(f"  {'월':10s} {'거래일':>6s} {'월수익':>8s} {'승률':>7s}")
    print("  " + "-" * 40)
    for ym in sorted(monthly_port.keys()):
        r = monthly_port[ym]
        month_ret = sum(r)
        win = sum(1 for x in r if x > 0) / len(r) * 100
        print(f"  {ym:10s} {len(r):>5}일 {month_ret:>+7.1f}% {win:>6.1f}%")

    # ── 최고/최악 거래 ──
    print(f"\n{'=' * 120}")
    print("  ■ [5] 최고 수익 TOP 20")
    print("=" * 120)
    best = sorted(all_trades, key=lambda x: -x['ret_1d'])[:20]
    for i, t in enumerate(best, 1):
        sigs = [s for s in t['signals'] if s.startswith('★') or 'STO' in s or '골든' in s][:4]
        print(f"  {i:>2}. {t['date']} {t['name']:12s} ({t['sector']:8s}) 점수:{t['score']:>3} "
              f"1일:{t['ret_1d']:>+6.1f}% STO:{t['sto_k']:.0f} {' | '.join(sigs)}")

    print(f"\n  ▶ 최악 손실 TOP 10")
    worst = sorted(all_trades, key=lambda x: x['ret_1d'])[:10]
    for i, t in enumerate(worst, 1):
        print(f"  {i:>2}. {t['date']} {t['name']:12s} ({t['sector']:8s}) 점수:{t['score']:>3} "
              f"1일:{t['ret_1d']:>+6.1f}% STO:{t['sto_k']:.0f}")

    # ── 섹터별 성과 ──
    print(f"\n{'=' * 120}")
    print("  ■ [6] 섹터별 스캔 성과 TOP 15")
    print("=" * 120)
    sector_rets = defaultdict(list)
    for t in all_trades:
        sector_rets[t['sector']].append(t['ret_1d'])

    sector_stats = []
    for sec, rets in sector_rets.items():
        if len(rets) < 20:
            continue
        sector_stats.append((sec, len(rets), np.mean(rets),
                            sum(1 for r in rets if r > 0) / len(rets) * 100))
    sector_stats.sort(key=lambda x: -x[2])
    print(f"  {'섹터':14s} {'건수':>7s} {'1일평균':>8s} {'승률':>7s}")
    print("-" * 50)
    for sec, cnt, avg, win in sector_stats[:15]:
        print(f"  {sec:14s} {cnt:>6,}건 {avg:>+7.2f}% {win:>6.1f}%")

    print(f"\n  ▶ WORST 5")
    for sec, cnt, avg, win in sector_stats[-5:]:
        print(f"  {sec:14s} {cnt:>6,}건 {avg:>+7.2f}% {win:>6.1f}%")

    print(f"\n{'=' * 120}")
    print("  백테스트 완료")
    print("=" * 120)

    # Save trades
    with open('data_store/backtest_trades.json', 'w', encoding='utf-8') as f:
        json.dump(all_trades[:1000], f, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
    run_full_backtest()
