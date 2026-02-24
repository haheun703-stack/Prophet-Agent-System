# -*- coding: utf-8 -*-
"""
최적화 백테스트 — 손절(-5%) + 최소 점수 컷 탐색
점수 컷: 30~80 (5단위), TOP N: 3/5/7/10
손절: 장중 저가가 진입가 대비 -5% 이하면 -5%로 확정
"""
import sys, io, json, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
os.chdir(os.path.join(os.path.dirname(__file__), ".."))

import pandas as pd
import numpy as np
from collections import defaultdict
import glob as gl

# score_stock 재사용
from backtest.full_period_backtest import compute_stochastic, score_stock


STOP_LOSS = -5.0  # -5% 손절


def load_data():
    csv_files = gl.glob('data_store/daily/*.csv')
    with open('data_store/universe.json', 'r', encoding='utf-8') as f:
        uni = json.load(f)

    all_data = {}
    for fpath in csv_files:
        code = os.path.basename(fpath).replace('.csv', '')
        if code not in uni:
            continue
        df = pd.read_csv(fpath).sort_values('날짜').reset_index(drop=True)
        if len(df) < 70:
            continue
        all_data[code] = df

    return all_data, uni


def scan_all_days(all_data, uni):
    """모든 날짜에 대해 스캔 결과를 미리 계산 (한 번만)"""
    sample = list(all_data.values())[0]
    all_dates = sample['날짜'].tolist()

    scan_start_date = '2025-06-01'
    start_idx = next((i for i, d in enumerate(all_dates) if d >= scan_start_date), 60)

    print(f"로드: {len(all_data)}종목")
    print(f"스캔기간: {all_dates[start_idx]} ~ {all_dates[-2]}")

    # 전체 스캔 결과 저장: {date: [(code, score, signals, sto_k, ret_1d_raw, ret_1d_sl, ...)]}
    daily_scans = {}

    for day_idx in range(start_idx, len(all_dates) - 1):
        date = all_dates[day_idx]
        picks = []

        for code, df in all_data.items():
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

            # 다음날 수익률 (손절 적용 / 미적용)
            entry_price = c[-1]  # 당일 종가 매수
            future = df[df['날짜'] > date]
            if len(future) == 0:
                continue

            next_row = future.iloc[0]
            next_low = float(next_row['저가'])
            next_close = float(next_row['종가'])

            # 손절 체크: 다음날 저가가 진입가 * (1 + SL%) 이하?
            sl_price = entry_price * (1 + STOP_LOSS / 100)
            if next_low <= sl_price:
                ret_1d_sl = STOP_LOSS  # 손절 확정
            else:
                ret_1d_sl = (next_close / entry_price - 1) * 100

            ret_1d_raw = (next_close / entry_price - 1) * 100

            # 3일/5일 수익 (손절 적용)
            ret_3d_sl = ret_1d_sl  # 초기값
            ret_5d_sl = ret_1d_sl
            stopped = next_low <= sl_price  # 1일차에 이미 손절?

            if not stopped and len(future) >= 2:
                # 2~3일차 확인
                for fi in range(1, min(3, len(future))):
                    row = future.iloc[fi]
                    low_price = float(row['저가'])
                    if low_price <= sl_price:
                        ret_3d_sl = STOP_LOSS
                        stopped = True
                        break
                if not stopped:
                    ret_3d_sl = (float(future.iloc[min(2, len(future)-1)]['종가']) / entry_price - 1) * 100

            if not stopped and len(future) >= 3:
                for fi in range(min(3, len(future)), min(5, len(future))):
                    row = future.iloc[fi]
                    low_price = float(row['저가'])
                    if low_price <= sl_price:
                        ret_5d_sl = STOP_LOSS
                        stopped = True
                        break
                if not stopped:
                    ret_5d_sl = (float(future.iloc[min(4, len(future)-1)]['종가']) / entry_price - 1) * 100
            elif stopped:
                ret_5d_sl = ret_3d_sl  # 이미 손절

            picks.append({
                'code': code,
                'name': uni[code]['name'],
                'sector': uni[code].get('sub_sector', ''),
                'score': sc,
                'signals': sigs,
                'sto_k': det.get('sto_k', 0),
                'ret_1d_raw': ret_1d_raw,
                'ret_1d_sl': ret_1d_sl,
                'ret_3d_sl': ret_3d_sl,
                'ret_5d_sl': ret_5d_sl,
                'stopped': next_low <= sl_price,
            })

        if picks:
            daily_scans[date] = picks

    return daily_scans, all_dates[start_idx:]


def simulate_portfolio(daily_scans, dates, min_score, top_n):
    """주어진 min_score와 top_n으로 포트폴리오 시뮬"""
    capital = 10000.0
    equity = [capital]
    daily_rets = []
    win_days = 0
    total_days = 0
    total_trades = 0
    sl_count = 0
    all_rets = []

    for date in dates:
        if date not in daily_scans:
            continue
        picks = daily_scans[date]
        filtered = [p for p in picks if p['score'] >= min_score]
        if not filtered:
            continue

        # 점수 높은 순 TOP N 선택
        top = sorted(filtered, key=lambda x: -x['score'])[:top_n]
        rets = [t['ret_1d_sl'] for t in top]
        day_ret = np.mean(rets) / 100

        capital *= (1 + day_ret)
        equity.append(capital)
        daily_rets.append(day_ret * 100)
        total_days += 1
        total_trades += len(top)
        sl_count += sum(1 for t in top if t['stopped'])
        all_rets.extend(rets)

        if day_ret > 0:
            win_days += 1

    if total_days == 0:
        return None

    total_ret = (equity[-1] / equity[0] - 1) * 100

    # 최대낙폭
    max_dd = 0
    peak = equity[0]
    for eq in equity:
        if eq > peak:
            peak = eq
        dd = (eq / peak - 1) * 100
        if dd < max_dd:
            max_dd = dd

    sharpe = np.mean(daily_rets) / np.std(daily_rets) * np.sqrt(220) if np.std(daily_rets) > 0 else 0

    # 월별 수익
    monthly_rets = defaultdict(list)
    day_i = 0
    for date in dates:
        if date not in daily_scans:
            continue
        picks = daily_scans[date]
        filtered = [p for p in picks if p['score'] >= min_score]
        if not filtered:
            continue
        top = sorted(filtered, key=lambda x: -x['score'])[:top_n]
        ym = date[:7]
        monthly_rets[ym].append(np.mean([t['ret_1d_sl'] for t in top]))

    # 연속 손실 월 수
    monthly_total = {}
    for ym, rets in sorted(monthly_rets.items()):
        monthly_total[ym] = sum(rets)

    max_consec_loss_months = 0
    consec = 0
    for ym in sorted(monthly_total.keys()):
        if monthly_total[ym] < 0:
            consec += 1
            max_consec_loss_months = max(max_consec_loss_months, consec)
        else:
            consec = 0

    return {
        'total_ret': total_ret,
        'max_dd': max_dd,
        'sharpe': sharpe,
        'win_rate': win_days / total_days * 100,
        'daily_avg': np.mean(daily_rets),
        'total_days': total_days,
        'total_trades': total_trades,
        'sl_count': sl_count,
        'sl_rate': sl_count / total_trades * 100 if total_trades > 0 else 0,
        'big5_rate': sum(1 for r in all_rets if r >= 5) / len(all_rets) * 100 if all_rets else 0,
        'loss5_rate': sum(1 for r in all_rets if r <= -5) / len(all_rets) * 100 if all_rets else 0,
        'monthly': monthly_total,
        'max_consec_loss': max_consec_loss_months,
        'equity': equity,
    }


def run_optimization():
    all_data, uni = load_data()
    daily_scans, dates = scan_all_days(all_data, uni)

    print(f"\n스캔 완료: {len(daily_scans)}거래일\n")

    # ═══════════════════════════════════════════════════════
    #  1. 점수 컷 × TOP N 그리드 서치
    # ═══════════════════════════════════════════════════════
    score_cuts = [30, 35, 40, 45, 50, 55, 60, 65, 70, 80]
    top_ns = [3, 5, 7, 10]

    print("=" * 130)
    print("  ■ [1] 그리드 서치: 최소 점수 × TOP N — 손절 -5% 적용")
    print("=" * 130)
    print(f"  {'점수컷':>6s} {'TOP':>4s} {'총수익':>8s} {'MDD':>7s} {'샤프':>6s} {'승률':>6s} {'일평균':>7s} {'거래일':>6s} "
          f"{'건수':>7s} {'손절률':>6s} {'5%+':>5s} {'연속손실월':>8s}")
    print("-" * 130)

    results = {}
    best_key = None
    best_score = -999

    for min_sc in score_cuts:
        for top_n in top_ns:
            r = simulate_portfolio(daily_scans, dates, min_sc, top_n)
            if r is None:
                continue

            key = (min_sc, top_n)
            results[key] = r

            # 종합 점수: 수익률/MDD 비율 + 샤프 - 손절률 패널티
            calmar = r['total_ret'] / abs(r['max_dd']) if r['max_dd'] != 0 else 0
            composite = calmar * 0.4 + r['sharpe'] * 0.3 + r['win_rate'] * 0.01 - r['sl_rate'] * 0.02
            if composite > best_score:
                best_score = composite
                best_key = key

            mark = " ★" if key == best_key else ""
            print(f"  {min_sc:>5}점 {top_n:>3}개 {r['total_ret']:>+7.1f}% {r['max_dd']:>+6.1f}% {r['sharpe']:>5.2f} "
                  f"{r['win_rate']:>5.1f}% {r['daily_avg']:>+6.2f}% {r['total_days']:>5}일 "
                  f"{r['total_trades']:>6,}건 {r['sl_rate']:>5.1f}% {r['big5_rate']:>4.1f}% {r['max_consec_loss']:>5}개월{mark}")

    # ═══════════════════════════════════════════════════════
    #  2. BEST 조합 상세 리포트
    # ═══════════════════════════════════════════════════════
    print(f"\n{'=' * 130}")
    print(f"  ■ [2] ★ BEST 조합: 점수 {best_key[0]}점 이상 / TOP {best_key[1]}개 / 손절 -5%")
    print(f"{'=' * 130}")

    br = results[best_key]
    print(f"  총수익률:    {br['total_ret']:+.1f}%")
    print(f"  최대낙폭:    {br['max_dd']:.1f}%")
    print(f"  수익/MDD:    {br['total_ret']/abs(br['max_dd']):.2f}x" if br['max_dd'] != 0 else "  수익/MDD:    inf")
    print(f"  샤프비율:    {br['sharpe']:.2f}")
    print(f"  일승률:      {br['win_rate']:.1f}% ({int(br['win_rate']*br['total_days']/100)}/{br['total_days']}일)")
    print(f"  일평균수익:  {br['daily_avg']:+.2f}%")
    print(f"  총매매:      {br['total_trades']:,}건")
    print(f"  손절발동:    {br['sl_count']:,}건 ({br['sl_rate']:.1f}%)")
    print(f"  5%+비율:     {br['big5_rate']:.1f}%")
    print(f"  -5%이하:     {br['loss5_rate']:.1f}% (손절로 제한)")

    print(f"\n  ▶ 월별 수익:")
    print(f"  {'월':10s} {'수익':>8s}")
    print("  " + "-" * 25)
    cum = 0
    for ym in sorted(br['monthly'].keys()):
        mret = br['monthly'][ym]
        cum += mret
        mark = " ◀ 손실" if mret < 0 else ""
        print(f"  {ym:10s} {mret:>+7.1f}%  (누적 {cum:>+7.1f}%){mark}")

    # ═══════════════════════════════════════════════════════
    #  3. 손절 유무 비교
    # ═══════════════════════════════════════════════════════
    print(f"\n{'=' * 130}")
    print(f"  ■ [3] 손절 효과 비교 (BEST 점수 {best_key[0]}점, TOP {best_key[1]})")
    print(f"{'=' * 130}")

    # 손절 없는 버전 재계산
    min_sc_best, top_n_best = best_key

    cap_sl = 10000.0
    cap_no = 10000.0
    eq_sl = [cap_sl]
    eq_no = [cap_no]

    for date in dates:
        if date not in daily_scans:
            continue
        picks = daily_scans[date]
        filtered = [p for p in picks if p['score'] >= min_sc_best]
        if not filtered:
            continue
        top = sorted(filtered, key=lambda x: -x['score'])[:top_n_best]

        # 손절 적용
        ret_sl = np.mean([t['ret_1d_sl'] for t in top]) / 100
        cap_sl *= (1 + ret_sl)
        eq_sl.append(cap_sl)

        # 손절 미적용
        ret_no = np.mean([t['ret_1d_raw'] for t in top]) / 100
        cap_no *= (1 + ret_no)
        eq_no.append(cap_no)

    # MDD 비교
    def calc_mdd(eq):
        mdd = 0
        peak = eq[0]
        for v in eq:
            if v > peak: peak = v
            dd = (v / peak - 1) * 100
            if dd < mdd: mdd = dd
        return mdd

    ret_sl = (eq_sl[-1] / eq_sl[0] - 1) * 100
    ret_no = (eq_no[-1] / eq_no[0] - 1) * 100
    mdd_sl = calc_mdd(eq_sl)
    mdd_no = calc_mdd(eq_no)

    print(f"  {'':12s} {'총수익':>8s} {'MDD':>8s} {'수익/MDD':>8s}")
    print("  " + "-" * 45)
    print(f"  {'손절 없음':12s} {ret_no:>+7.1f}% {mdd_no:>+7.1f}% {ret_no/abs(mdd_no):>7.2f}x" if mdd_no != 0 else "")
    print(f"  {'손절 -5%':12s} {ret_sl:>+7.1f}% {mdd_sl:>+7.1f}% {ret_sl/abs(mdd_sl):>7.2f}x" if mdd_sl != 0 else "")
    saved = mdd_no - mdd_sl
    print(f"  → 손절로 MDD {saved:+.1f}%p 개선")

    # ═══════════════════════════════════════════════════════
    #  4. 점수대별 손절 발동률 분석
    # ═══════════════════════════════════════════════════════
    print(f"\n{'=' * 130}")
    print(f"  ■ [4] 점수대별 손절 발동률 & 수익")
    print(f"{'=' * 130}")

    score_buckets = [(30, 39), (40, 49), (50, 59), (60, 69), (70, 79), (80, 100)]
    print(f"  {'점수대':10s} {'건수':>7s} {'손절률':>7s} {'평균(손절)':>10s} {'평균(무손절)':>10s} {'차이':>7s}")
    print("  " + "-" * 65)

    for lo, hi in score_buckets:
        all_sl = []
        all_no = []
        sl_cnt = 0
        total = 0
        for date_picks in daily_scans.values():
            for p in date_picks:
                if lo <= p['score'] <= hi:
                    all_sl.append(p['ret_1d_sl'])
                    all_no.append(p['ret_1d_raw'])
                    total += 1
                    if p['stopped']:
                        sl_cnt += 1
        if total == 0:
            continue
        avg_sl = np.mean(all_sl)
        avg_no = np.mean(all_no)
        print(f"  {lo}~{hi}점 {total:>7,}건 {sl_cnt/total*100:>6.1f}% {avg_sl:>+9.2f}% {avg_no:>+9.2f}% {avg_sl-avg_no:>+6.2f}%")

    # ═══════════════════════════════════════════════════════
    #  5. TOP 3/5/7 비교 (BEST 점수 고정)
    # ═══════════════════════════════════════════════════════
    print(f"\n{'=' * 130}")
    print(f"  ■ [5] TOP N 비교 (점수 {best_key[0]}점+, 손절 -5%)")
    print(f"{'=' * 130}")
    print(f"  {'TOP N':>6s} {'총수익':>8s} {'MDD':>7s} {'수익/MDD':>8s} {'샤프':>6s} {'승률':>6s} {'건수':>7s} {'손절률':>6s}")
    print("  " + "-" * 65)

    for n in [1, 2, 3, 5, 7, 10]:
        r = simulate_portfolio(daily_scans, dates, best_key[0], n)
        if r is None:
            continue
        calmar = r['total_ret'] / abs(r['max_dd']) if r['max_dd'] != 0 else 0
        mark = " ★" if n == best_key[1] else ""
        print(f"  TOP{n:>2} {r['total_ret']:>+7.1f}% {r['max_dd']:>+6.1f}% {calmar:>7.2f}x {r['sharpe']:>5.2f} "
              f"{r['win_rate']:>5.1f}% {r['total_trades']:>6,}건 {r['sl_rate']:>5.1f}%{mark}")

    # ═══════════════════════════════════════════════════════
    #  최종 추천
    # ═══════════════════════════════════════════════════════
    print(f"\n{'=' * 130}")
    print(f"  ■ 최종 추천 설정")
    print(f"{'=' * 130}")
    print(f"  최소 점수:  {best_key[0]}점")
    print(f"  종목 수:    TOP {best_key[1]}개")
    print(f"  손절:       -5%")
    print(f"  예상 수익:  {br['total_ret']:+.1f}% (8개월)")
    print(f"  예상 MDD:   {br['max_dd']:.1f}%")
    print(f"  손절 발동:  {br['sl_rate']:.1f}%")
    print(f"{'=' * 130}")


if __name__ == "__main__":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
    run_optimization()
