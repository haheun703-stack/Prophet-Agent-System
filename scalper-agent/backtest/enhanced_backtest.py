"""
강화 백테스트 v3 — 8가지 개선 적용
════════════════════════════════════
v2 (5가지):
1. BUY만 진입 (STRONG_BUY 제외 — 승률 48% vs 33%)
2. 눌림목 진입 — 종가 진입 → 다음 2일 내 저가가 종가*0.98 이하면 그 가격으로 진입
3. 분할 익절 — TP1에서 50% 청산 + 나머지 트레일링(SL=진입가, TP2까지)
4. 보유일 컷오프 5일 (10일 → 5일)
5. 사전감지 필터 — premove 신호 2개 이상인 종목만 진입

v3 추가 (3가지):
6. INST_COST SL 제거 — 25% 승률, 유일한 마이너스 EV SL 유형
7. 눌림목 진입 비활성화 — 종가 진입이 +6.36% vs 눌림목 +2.49%
8. RSI 필터 — RSI 35~65 범위만 진입 (과열/과매도 제외)

+ v1/v2 대비 비교 결과 출력
"""
import sys, os, warnings
warnings.filterwarnings("ignore")
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), "../../.env"))

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from data.supply_analyzer import SupplyAnalyzer
from data.universe_builder import load_universe

# ── 설정 ──
HOLD_DAYS = 5          # 개선4: 5일 컷오프
TOP_N = 30
PULLBACK_ENABLED = True   # 눌림목 유지 (품질 필터 역할)
PULLBACK_DAYS = 2
PULLBACK_DEPTH = 0.98
MIN_PREMOVE_SIGNALS = 3  # 개선5: 최소 사전감지 신호 (2→3 강화)
EXCLUDE_SL_SOURCES = {"INST_COST"}  # 개선6: INST_COST SL만 제거 (25%승률)
RSI_FILTER = False       # 개선8: 비활성화 (42건 제거 → 과도한 필터)
RSI_MIN = 25
RSI_MAX = 75

def get_trading_days_from_cache(analyzer, lookback=60):
    for code in list(analyzer._cache_daily.keys())[:5]:
        df = analyzer._cache_daily[code]
        if df is not None and len(df) > 0:
            if 'date' in df.columns:
                dates = sorted(pd.to_datetime(df['date']).dt.strftime("%Y-%m-%d").unique())
            else:
                dates = sorted(set(pd.to_datetime(df.index).strftime("%Y-%m-%d").tolist()))
            return dates[-lookback:] if len(dates) > lookback else dates
    return []


def count_premove_signals(full, day_df):
    """사전감지 신호 개수 (간이 버전 — 백테스트용)"""
    signals = 0
    momentum = full.momentum

    # 1. 기관 연속매수 5일+
    if momentum.inst_streak >= 5:
        signals += 1

    # 2. 수급가속 30%+
    if momentum.supply_accel > 30:
        signals += 1

    # 3. 개인 역지표
    if momentum.retail_contrarian:
        signals += 1

    # 4. 외인소진율 변곡
    if momentum.foreign_inflection == "UP_TURN":
        signals += 1

    # 5. 거래량 건조
    try:
        close_col = "close" if "close" in day_df.columns else "종가"
        vol_col = "volume" if "volume" in day_df.columns else "거래량"
        if vol_col in day_df.columns and len(day_df) >= 25:
            vol = day_df[vol_col].astype(float)
            ma20_vol = vol.rolling(20).mean()
            recent_3d_vol = vol.iloc[-3:].mean()
            ma20_val = float(ma20_vol.iloc[-1])
            if ma20_val > 0 and recent_3d_vol < ma20_val * 0.5:
                prices = day_df[close_col].astype(float)
                price_range_3d = (prices.iloc[-3:].max() - prices.iloc[-3:].min()) / prices.iloc[-1] * 100
                if price_range_3d < 3:
                    signals += 1
    except:
        pass

    # 6. 기술적 정렬 (EMA+RSI+MACD)
    try:
        from data.swing_indicators import analyze_stock as swing_analyze
        tech = swing_analyze(day_df.copy())
        ema_trend = tech.get("ema_trend", "UNKNOWN")
        rsi_val = tech.get("rsi", 50)
        hist = tech.get("histogram", {})
        hist_triggered = hist.get("triggered", False) if isinstance(hist, dict) else False
        hist_dir = hist.get("direction", None) if isinstance(hist, dict) else None
        if ema_trend == "BULLISH" and 35 <= rsi_val <= 55 and hist_triggered and hist_dir == "BUY":
            signals += 1
    except:
        pass

    return signals


def find_pullback_entry(df_future, scan_close, pullback_days=2, pullback_depth=0.98):
    """눌림목 진입가 찾기

    다음 pullback_days일 내에 저가가 scan_close * pullback_depth 이하로 내려오면
    그 가격을 진입가로 사용. 못 찾으면 None.
    """
    target = scan_close * pullback_depth

    for i, (_, row) in enumerate(df_future.head(pullback_days).iterrows()):
        low = row.get('low', row.get('저가', 0))
        if low <= 0:
            continue
        if low <= target:
            # 눌림목 진입: 목표가 or 저가 중 높은 값 (실제론 지정가 대기)
            entry = max(target, low)
            return entry, i
    return None, None


def forward_check_enhanced(analyzer, code, entry_date, baseline, full,
                           hold_days=5, pullback_enabled=False,
                           pullback_days=2, pullback_depth=0.98):
    """
    강화 포워드 체크:
    - 눌림목 진입 (2일 대기)
    - 분할 익절 (TP1에서 50%, 나머지 트레일링)
    - 5일 컷오프
    """
    if code not in analyzer._cache_daily:
        return None

    df = analyzer._cache_daily[code].copy()
    df = df.reset_index()
    date_col = df.columns[0]
    df['_date'] = pd.to_datetime(df[date_col]).dt.strftime("%Y-%m-%d")

    # entry_date 이후 데이터
    future_all = df[df['_date'] > entry_date]
    if len(future_all) < 3:
        return None

    scan_close = baseline.close
    sl = baseline.invalidation
    tp1 = baseline.target_1
    tp2 = baseline.target_2

    # ── 개선2/7: 눌림목 진입 (v3: 비활성화 가능) ──
    pullback_entry = None
    if pullback_enabled:
        pullback_entry, pullback_day = find_pullback_entry(
            future_all, scan_close, pullback_days, pullback_depth
        )

    if pullback_entry is not None:
        entry = pullback_entry
        trade_start_idx = pullback_day + 1
    else:
        entry = scan_close
        trade_start_idx = 0

    # 진입 후 데이터
    future = future_all.iloc[trade_start_idx:trade_start_idx + hold_days]
    if len(future) == 0:
        return None

    entry_actual_date = future_all.iloc[trade_start_idx if trade_start_idx < len(future_all) else 0]['_date']

    result = {
        'code': code,
        'entry_date': entry_actual_date,
        'scan_date': entry_date,
        'entry_price': entry,
        'sl': sl,
        'tp1': tp1,
        'tp2': tp2,
        'risk': baseline.risk_per_share,
        'sl_source': baseline.invalidation_source,
        'pullback_entry': pullback_entry is not None,
        'outcome': 'HOLD',
        'exit_price': 0,
        'exit_date': '',
        'exit_day': 0,
        'pnl_pct': 0,
        'r_multiple': 0,
    }

    # ── 개선3: 분할 익절 시뮬레이션 ──
    # 50% 포지션A (TP1에서 청산) + 50% 포지션B (TP2 or 트레일링)
    pos_a_closed = False
    pos_a_pnl = 0
    pos_b_sl = sl  # B 포지션 SL (TP1 히트 후 진입가로 이동)
    high_since_entry = entry

    for i, (_, row) in enumerate(future.iterrows()):
        low = row.get('low', row.get('저가', 0))
        high = row.get('high', row.get('고가', 0))
        close = row.get('close', row.get('종가', 0))
        date = row['_date']

        if low <= 0 or high <= 0:
            continue

        high_since_entry = max(high_since_entry, high)

        # SL 체크 (A 또는 B 포지션)
        if not pos_a_closed:
            # A+B 둘 다 SL 히트
            if low <= sl:
                result['outcome'] = 'SL_HIT'
                result['exit_price'] = sl
                result['exit_date'] = date
                result['exit_day'] = i + 1
                result['pnl_pct'] = (sl - entry) / entry * 100
                result['r_multiple'] = -1.0
                return result
        else:
            # A는 이미 TP1에서 청산됨, B만 남음
            if low <= pos_b_sl:
                # B 포지션 SL 히트 (본절 or 트레일링)
                pos_b_pnl = (pos_b_sl - entry) / entry * 100
                combined_pnl = pos_a_pnl * 0.5 + pos_b_pnl * 0.5
                result['outcome'] = 'SPLIT_TP1_SL'
                result['exit_price'] = pos_b_sl
                result['exit_date'] = date
                result['exit_day'] = i + 1
                result['pnl_pct'] = combined_pnl
                result['r_multiple'] = combined_pnl / abs((sl - entry) / entry * 100) if entry > sl else 0
                return result

        # TP1 히트 (아직 A 포지션 남아있을 때)
        if not pos_a_closed and high >= tp1:
            pos_a_closed = True
            pos_a_pnl = (tp1 - entry) / entry * 100
            pos_b_sl = entry  # B 포지션 SL을 진입가로 이동 (본절)

        # TP2 히트 (B 포지션)
        if pos_a_closed and high >= tp2:
            pos_b_pnl = (tp2 - entry) / entry * 100
            combined_pnl = pos_a_pnl * 0.5 + pos_b_pnl * 0.5
            result['outcome'] = 'SPLIT_TP1_TP2'
            result['exit_price'] = tp2
            result['exit_date'] = date
            result['exit_day'] = i + 1
            result['pnl_pct'] = combined_pnl
            result['r_multiple'] = combined_pnl / abs((sl - entry) / entry * 100) if entry > sl else 0
            return result

        # 트레일링 (B 포지션, TP1 히트 후)
        if pos_a_closed:
            pnl_pct = (high_since_entry - entry) / entry * 100
            if pnl_pct >= 10:
                trail_sl = int(high_since_entry * 0.97)  # 3% 트레일링
                if trail_sl > pos_b_sl:
                    pos_b_sl = trail_sl

    # ── 개선4: 5일 컷오프 — 만기 종료 ──
    last_row = future.iloc[-1]
    last_close = last_row.get('close', last_row.get('종가', entry))
    last_date = last_row['_date']

    if pos_a_closed:
        # A는 TP1에서 익절, B는 만기 종가 청산
        pos_b_pnl = (last_close - entry) / entry * 100
        combined_pnl = pos_a_pnl * 0.5 + pos_b_pnl * 0.5
        result['outcome'] = 'SPLIT_TP1_EXP'
        result['pnl_pct'] = combined_pnl
    else:
        # A+B 둘 다 만기 종가 청산
        result['outcome'] = 'EXPIRED'
        result['pnl_pct'] = (last_close - entry) / entry * 100

    result['exit_price'] = last_close
    result['exit_date'] = last_date
    result['exit_day'] = len(future)
    result['r_multiple'] = result['pnl_pct'] / abs((sl - entry) / entry * 100) if entry > sl else 0
    return result


def run_backtest():
    print("=" * 70)
    print("  강화 백테스트 v3 — 8가지 개선 적용")
    print("=" * 70)
    print()
    print("  [v2] BUY만 / 분할익절 / 5일컷오프 / 사전감지2+")
    print("  [v3-6] INST_COST SL 제거 (25%승률 → 마이너스EV)")
    print("  [v3-7] 눌림목 비활성화 (종가진입 +6.36% > 눌림 +2.49%)")
    print("  [v3-8] RSI 35~65 필터 (과열/과매도 제외)")
    print()

    print(f"보유기간: 최대 {HOLD_DAYS}일")

    universe = load_universe()
    codes = list(universe.keys())[:500]
    print(f"유니버스: {len(codes)}개")

    analyzer = SupplyAnalyzer()

    print("데이터 프리로드 중...")
    for code in codes[:10]:
        try:
            analyzer.analyze_full(code)
        except:
            pass

    trading_days = get_trading_days_from_cache(analyzer, lookback=60)
    if len(trading_days) < 30:
        print(f"거래일 부족: {len(trading_days)}일")
        return

    scan_indices = [-25, -20, -15, -10]
    scan_dates = [trading_days[idx] for idx in scan_indices if abs(idx) < len(trading_days)]
    print(f"\n스캔 시점: {scan_dates}")

    all_results = []
    filter_stats = {'total_buy': 0, 'premove_pass': 0, 'premove_fail': 0}

    for scan_idx, scan_date in enumerate(scan_dates):
        print(f"\n{'─'*60}")
        print(f"[{scan_idx+1}/{len(scan_dates)}] 스캔일: {scan_date}")
        print(f"{'─'*60}")

        scored = []
        for i, code in enumerate(codes):
            if (i+1) % 100 == 0:
                print(f"  분석 중... {i+1}/{len(codes)}")
            try:
                full = analyzer.analyze_full(code, as_of=scan_date)
                if not full or not full.baseline:
                    continue

                # ── 개선1: BUY만 (STRONG_BUY 제외) ──
                if full.action != 'BUY':
                    continue

                filter_stats['total_buy'] += 1
                name = universe.get(code, {}).get('name', code)

                # ── 개선5: 사전감지 신호 필터 ──
                day_df = analyzer._cache_daily.get(code)
                if day_df is not None and len(day_df) > 20:
                    if scan_date:
                        day_df_filtered = day_df[day_df.index <= pd.Timestamp(scan_date)]
                    else:
                        day_df_filtered = day_df

                    sig_count = count_premove_signals(full, day_df_filtered)
                    if sig_count < MIN_PREMOVE_SIGNALS:
                        filter_stats['premove_fail'] += 1
                        continue
                    filter_stats['premove_pass'] += 1

                    # ── 개선6: INST_COST SL 제거 ──
                    if full.baseline.invalidation_source in EXCLUDE_SL_SOURCES:
                        filter_stats.setdefault('sl_excluded', 0)
                        filter_stats['sl_excluded'] += 1
                        continue

                    # ── 개선8: RSI 필터 (35~65) ──
                    if RSI_FILTER:
                        try:
                            close_col = "close" if "close" in day_df_filtered.columns else "종가"
                            prices = day_df_filtered[close_col].astype(float)
                            if len(prices) >= 15:
                                delta = prices.diff()
                                gain = delta.clip(lower=0).rolling(14).mean()
                                loss = (-delta.clip(upper=0)).rolling(14).mean()
                                rs = gain / loss.replace(0, np.nan)
                                rsi = 100 - (100 / (1 + rs))
                                rsi_val = float(rsi.iloc[-1])
                                if not (RSI_MIN <= rsi_val <= RSI_MAX):
                                    filter_stats.setdefault('rsi_excluded', 0)
                                    filter_stats['rsi_excluded'] += 1
                                    continue
                        except:
                            pass  # RSI 계산 실패 시 통과

                else:
                    filter_stats['premove_fail'] += 1
                    continue

                scored.append((code, name, full, sig_count))
            except Exception:
                continue

        print(f"  BUY + 사전감지 {MIN_PREMOVE_SIGNALS}+: {len(scored)}개")

        if not scored:
            continue

        scored.sort(key=lambda x: x[2].composite_score, reverse=True)
        top = scored[:TOP_N]

        for code, name, f, sig_cnt in top:
            result = forward_check_enhanced(
                analyzer, code, scan_date, f.baseline, f,
                hold_days=HOLD_DAYS,
                pullback_enabled=PULLBACK_ENABLED,
                pullback_days=PULLBACK_DAYS,
                pullback_depth=PULLBACK_DEPTH,
            )
            if result:
                result['name'] = name
                result['action'] = f.action
                result['score'] = f.composite_score
                result['premove_signals'] = sig_cnt
                all_results.append(result)

    # ── 결과 분석 ──
    if not all_results:
        print("\n결과 없음!")
        return

    df = pd.DataFrame(all_results)
    total = len(df)

    print(f"\n{'='*70}")
    print(f"  강화 백테스트 결과 (총 {total}건)")
    print(f"{'='*70}")

    # 필터 통계
    print(f"\n필터 통계:")
    print(f"  BUY 후보: {filter_stats['total_buy']}건")
    print(f"  사전감지 통과: {filter_stats['premove_pass']}건")
    print(f"  사전감지 탈락: {filter_stats['premove_fail']}건")
    print(f"  INST_COST SL 제거: {filter_stats.get('sl_excluded', 0)}건")
    print(f"  RSI 범위 제외: {filter_stats.get('rsi_excluded', 0)}건")

    # 히트 분포
    outcomes = df['outcome'].value_counts()
    print(f"\n히트 분포:")
    for outcome in ['SPLIT_TP1_TP2', 'SPLIT_TP1_SL', 'SPLIT_TP1_EXP', 'SL_HIT', 'EXPIRED']:
        cnt = outcomes.get(outcome, 0)
        if cnt > 0:
            avg_pnl = df[df['outcome'] == outcome]['pnl_pct'].mean()
            print(f"  {outcome}: {cnt}건 ({cnt/total*100:.1f}%) 평균 {avg_pnl:+.2f}%")

    # 승률
    winners = df[df['pnl_pct'] > 0]
    losers = df[df['pnl_pct'] < 0]
    win_rate = len(winners) / total * 100 if total > 0 else 0
    print(f"\n승률: {win_rate:.1f}% ({len(winners)}/{total})")

    # 수익률
    avg_pnl = df['pnl_pct'].mean()
    med_pnl = df['pnl_pct'].median()
    avg_r = df['r_multiple'].mean()
    win_pnl = winners['pnl_pct'].mean() if len(winners) > 0 else 0
    loss_pnl = losers['pnl_pct'].mean() if len(losers) > 0 else 0

    print(f"\n수익률:")
    print(f"  평균: {avg_pnl:+.2f}% | 중앙값: {med_pnl:+.2f}%")
    print(f"  평균 R배수: {avg_r:+.2f}R")
    print(f"  평균 이익: {win_pnl:+.2f}% | 평균 손실: {loss_pnl:+.2f}%")

    # 보유일
    avg_days = df['exit_day'].mean()
    print(f"\n평균 보유일: {avg_days:.1f}일")

    # 눌림목 진입 비율
    pullback_cnt = df['pullback_entry'].sum()
    print(f"\n눌림목 진입: {pullback_cnt}건 ({pullback_cnt/total*100:.1f}%)")
    if pullback_cnt > 0:
        pb_pnl = df[df['pullback_entry']]['pnl_pct'].mean()
        no_pb_pnl = df[~df['pullback_entry']]['pnl_pct'].mean()
        print(f"  눌림목 진입 평균: {pb_pnl:+.2f}% | 종가 진입 평균: {no_pb_pnl:+.2f}%")

    # SL 근거별
    print(f"\nSL 근거별 성과:")
    for src in df['sl_source'].unique():
        sub = df[df['sl_source'] == src]
        wr = len(sub[sub['pnl_pct'] > 0]) / len(sub) * 100
        print(f"  {src}: 승률 {wr:.0f}% | 평균 {sub['pnl_pct'].mean():+.2f}% | {len(sub)}건")

    # 사전감지 신호별
    print(f"\n사전감지 신호 수별 성과:")
    for sig in sorted(df['premove_signals'].unique()):
        sub = df[df['premove_signals'] == sig]
        wr = len(sub[sub['pnl_pct'] > 0]) / len(sub) * 100
        print(f"  {sig}개 신호: 승률 {wr:.0f}% | 평균 {sub['pnl_pct'].mean():+.2f}% | {len(sub)}건")

    # 상세 결과
    print(f"\n{'─'*100}")
    print(f"{'종목':>12} {'스캔일':>12} {'진입일':>12} {'진입가':>10} {'결과':>16} "
          f"{'청산가':>10} {'수익률':>8} {'R배수':>6} {'일':>3} {'신호':>3} {'눌림':>4}")
    print(f"{'─'*100}")

    for _, r in df.sort_values('pnl_pct', ascending=False).iterrows():
        emoji = {
            'SPLIT_TP1_TP2': 'TT',
            'SPLIT_TP1_SL': 'TS',
            'SPLIT_TP1_EXP': 'TE',
            'SL_HIT': 'SL',
            'EXPIRED': 'EX',
        }
        pb = "Y" if r.get('pullback_entry', False) else "N"
        print(f"{r['name']:>12} {r['scan_date']:>12} {r['entry_date']:>12} "
              f"{r['entry_price']:>10,.0f} {emoji.get(r['outcome'],'??'):>2} {r['outcome']:<13} "
              f"{r['exit_price']:>10,.0f} {r['pnl_pct']:>+7.2f}% {r['r_multiple']:>+5.2f}R "
              f"{r['exit_day']:>3}  {r.get('premove_signals',0):>2}  {pb:>3}")

    # ── v1 vs v2 vs v3 비교 ──
    print(f"\n{'='*80}")
    print(f"  v1(기존) vs v2(5개선) vs v3(8개선) 비교")
    print(f"{'='*80}")
    print(f"  {'지표':>16} {'v1(기존)':>12} {'v2(5개선)':>12} {'v3(8개선)':>12} {'v2→v3':>10}")
    print(f"  {'─'*62}")
    print(f"  {'승률':>16} {'36.0%':>12} {'48.6%':>12} {f'{win_rate:.1f}%':>12} {f'{win_rate-48.6:+.1f}%':>10}")
    print(f"  {'기대값(EV)':>16} {'+1.43%':>12} {'+3.46%':>12} {f'{avg_pnl:+.2f}%':>12} {f'{avg_pnl-3.46:+.2f}%':>10}")
    print(f"  {'평균 R배수':>16} {'+0.30R':>12} {'+1.45R':>12} {f'{avg_r:+.2f}R':>12} {f'{avg_r-1.45:+.2f}R':>10}")
    print(f"  {'평균 보유일':>16} {'3.6일':>12} {'3.0일':>12} {f'{avg_days:.1f}일':>12} {f'{avg_days-3.0:+.1f}일':>10}")
    print(f"  {'총 건수':>16} {'100건':>12} {'35건':>12} {f'{total}건':>12}")
    print(f"{'='*80}")

    ev = avg_pnl
    if ev > 3:
        print(f"  기대값 {ev:+.2f}% — 실전 투입 가능 수준")
    elif ev > 0:
        print(f"  기대값 {ev:+.2f}% — 양수지만 추가 개선 필요")
    else:
        print(f"  기대값 {ev:+.2f}% — 개선 실패, 다른 접근 필요")
    print(f"{'='*70}")


if __name__ == "__main__":
    run_backtest()
