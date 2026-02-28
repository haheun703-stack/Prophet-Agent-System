"""
BaselineLevels 백테스트
─────────────────────────
과거 시점에서 6D 스캔 → STRONG_BUY/BUY 종목 추출
→ 이후 N일 동안 TP1/TP2 히트 vs SL 히트 검증
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
SCAN_DATES = []  # 자동 계산
HOLD_DAYS = 10   # 최대 보유일
TOP_N = 30       # 각 스캔일 상위 N개만 (속도)

def get_trading_days_from_cache(analyzer, lookback=60):
    """캐시된 일봉 데이터에서 실제 거래일 추출"""
    for code in list(analyzer._cache_daily.keys())[:5]:
        df = analyzer._cache_daily[code]
        if df is not None and len(df) > 0:
            # index가 날짜이거나 date 컬럼이 있거나
            if 'date' in df.columns:
                dates = sorted(pd.to_datetime(df['date']).dt.strftime("%Y-%m-%d").unique())
            else:
                dates = sorted(pd.to_datetime(df.index).strftime("%Y-%m-%d").tolist())
                # 중복 제거
                dates = sorted(set(dates))
            return dates[-lookback:] if len(dates) > lookback else dates
    return []

def forward_check(analyzer, code, entry_date, baseline, hold_days=10):
    """
    entry_date 이후 hold_days 동안 TP1/TP2/SL 히트 여부 체크
    Returns: dict with result
    """
    if code not in analyzer._cache_daily:
        return None

    df = analyzer._cache_daily[code].copy()
    # index를 date 컬럼으로 변환
    df = df.reset_index()
    # 첫 번째 컬럼이 날짜
    date_col = df.columns[0]
    df['_date'] = pd.to_datetime(df[date_col]).dt.strftime("%Y-%m-%d")

    # entry_date 이후 데이터만
    future = df[df['_date'] > entry_date].head(hold_days)
    if len(future) == 0:
        return None

    sl = baseline.invalidation
    tp1 = baseline.target_1
    tp2 = baseline.target_2
    entry = baseline.close

    result = {
        'code': code,
        'entry_date': entry_date,
        'entry_price': entry,
        'sl': sl,
        'tp1': tp1,
        'tp2': tp2,
        'risk': baseline.risk_per_share,
        'sl_source': baseline.invalidation_source,
        'outcome': 'HOLD',
        'exit_price': 0,
        'exit_date': '',
        'exit_day': 0,
        'pnl_pct': 0,
        'r_multiple': 0,
    }

    for i, (_, row) in enumerate(future.iterrows()):
        low = row.get('low', row.get('저가', 0))
        high = row.get('high', row.get('고가', 0))
        close = row.get('close', row.get('종가', 0))
        date = row['_date']

        if low <= 0 or high <= 0:
            continue

        # SL 히트 체크 (저가가 SL 이하)
        if low <= sl:
            result['outcome'] = 'SL_HIT'
            result['exit_price'] = sl
            result['exit_date'] = date
            result['exit_day'] = i + 1
            result['pnl_pct'] = (sl - entry) / entry * 100
            result['r_multiple'] = -1.0
            return result

        # TP2 히트 체크 (고가가 TP2 이상)
        if high >= tp2:
            result['outcome'] = 'TP2_HIT'
            result['exit_price'] = tp2
            result['exit_date'] = date
            result['exit_day'] = i + 1
            result['pnl_pct'] = (tp2 - entry) / entry * 100
            result['r_multiple'] = (tp2 - entry) / (entry - sl) if entry > sl else 0
            return result

        # TP1 히트 체크 (고가가 TP1 이상)
        if high >= tp1:
            result['outcome'] = 'TP1_HIT'
            result['exit_price'] = tp1
            result['exit_date'] = date
            result['exit_day'] = i + 1
            result['pnl_pct'] = (tp1 - entry) / entry * 100
            result['r_multiple'] = (tp1 - entry) / (entry - sl) if entry > sl else 0
            return result

    # 만기 종료 — 마지막 종가로 청산
    last_row = future.iloc[-1]
    last_close = last_row.get('close', last_row.get('종가', entry))
    last_date = last_row['_date']
    result['outcome'] = 'EXPIRED'
    result['exit_price'] = last_close
    result['exit_date'] = last_date
    result['exit_day'] = len(future)
    result['pnl_pct'] = (last_close - entry) / entry * 100
    result['r_multiple'] = (last_close - entry) / (entry - sl) if entry > sl else 0
    return result


def run_backtest():
    print("=" * 70)
    print("  6D BaselineLevels 백테스트")
    print("=" * 70)

    print(f"보유기간: 최대 {HOLD_DAYS}일")
    print(f"유니버스 로딩 중...")

    # 유니버스 로딩
    universe = load_universe()
    codes = list(universe.keys())[:500]  # 상위 500개
    print(f"유니버스: {len(codes)}개")

    # 분석기 초기화 + 데이터 프리로드
    analyzer = SupplyAnalyzer()

    # 일단 몇 종목 로드해서 거래일 추출
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

    # 스캔 날짜: 5일 간격으로 4개 시점
    scan_indices = [-25, -20, -15, -10]
    scan_dates = []
    for idx in scan_indices:
        if abs(idx) < len(trading_days):
            scan_dates.append(trading_days[idx])

    print(f"\n스캔 시점: {scan_dates}")

    all_results = []

    for scan_idx, scan_date in enumerate(scan_dates):
        print(f"\n{'─'*60}")
        print(f"[{scan_idx+1}/{len(scan_dates)}] 스캔일: {scan_date}")
        print(f"{'─'*60}")

        # 전종목 스캔
        scored = []
        for i, code in enumerate(codes):
            if (i+1) % 100 == 0:
                print(f"  분석 중... {i+1}/{len(codes)}")
            try:
                full = analyzer.analyze_full(code, as_of=scan_date)
                if full and full.action in ('STRONG_BUY', 'BUY') and full.baseline:
                    name = universe.get(code, {}).get('name', code)
                    scored.append((code, name, full))
            except Exception:
                continue

        print(f"  STRONG_BUY/BUY: {len(scored)}개")

        if not scored:
            continue

        # 종합점수 순 정렬
        scored.sort(key=lambda x: x[2].composite_score, reverse=True)
        top = scored[:TOP_N]

        # 포워드 체크
        for code, name, f in top:
            result = forward_check(analyzer, code, scan_date, f.baseline, HOLD_DAYS)
            if result:
                result['name'] = name
                result['action'] = f.action
                result['score'] = f.composite_score
                all_results.append(result)

    # ── 결과 분석 ──
    if not all_results:
        print("\n결과 없음!")
        return

    df = pd.DataFrame(all_results)

    print(f"\n{'='*70}")
    print(f"  백테스트 결과 (총 {len(df)}건)")
    print(f"{'='*70}")

    # 전체 통계
    total = len(df)
    tp2_hit = len(df[df['outcome'] == 'TP2_HIT'])
    tp1_hit = len(df[df['outcome'] == 'TP1_HIT'])
    sl_hit = len(df[df['outcome'] == 'SL_HIT'])
    expired = len(df[df['outcome'] == 'EXPIRED'])

    win = tp1_hit + tp2_hit
    expired_win = len(df[(df['outcome'] == 'EXPIRED') & (df['pnl_pct'] > 0)])

    print(f"\n📊 히트 분포:")
    print(f"  TP2 히트: {tp2_hit}건 ({tp2_hit/total*100:.1f}%)")
    print(f"  TP1 히트: {tp1_hit}건 ({tp1_hit/total*100:.1f}%)")
    print(f"  SL 히트:  {sl_hit}건 ({sl_hit/total*100:.1f}%)")
    print(f"  만기종료: {expired}건 ({expired/total*100:.1f}%) [이익:{expired_win} 손실:{expired-expired_win}]")

    win_rate = (win / total * 100) if total > 0 else 0
    print(f"\n📈 승률 (TP 히트 기준): {win_rate:.1f}% ({win}/{total})")

    # 수익률 통계
    avg_pnl = df['pnl_pct'].mean()
    med_pnl = df['pnl_pct'].median()
    avg_r = df['r_multiple'].mean()

    win_pnl = df[df['pnl_pct'] > 0]['pnl_pct'].mean() if len(df[df['pnl_pct'] > 0]) > 0 else 0
    loss_pnl = df[df['pnl_pct'] < 0]['pnl_pct'].mean() if len(df[df['pnl_pct'] < 0]) > 0 else 0

    print(f"\n💰 수익률:")
    print(f"  평균: {avg_pnl:+.2f}% | 중앙값: {med_pnl:+.2f}%")
    print(f"  평균 R배수: {avg_r:+.2f}R")
    print(f"  평균 이익: {win_pnl:+.2f}% | 평균 손실: {loss_pnl:+.2f}%")

    # 평균 보유일
    avg_days = df['exit_day'].mean()
    print(f"\n⏱️ 평균 보유일: {avg_days:.1f}일")

    # STRONG_BUY vs BUY 비교
    sb = df[df['action'] == 'STRONG_BUY']
    buy = df[df['action'] == 'BUY']

    if len(sb) > 0 and len(buy) > 0:
        print(f"\n📊 STRONG_BUY vs BUY:")
        sb_wr = len(sb[sb['pnl_pct'] > 0]) / len(sb) * 100
        buy_wr = len(buy[buy['pnl_pct'] > 0]) / len(buy) * 100
        print(f"  STRONG_BUY: 승률 {sb_wr:.0f}% | 평균 {sb['pnl_pct'].mean():+.2f}% | {len(sb)}건")
        print(f"  BUY:        승률 {buy_wr:.0f}% | 평균 {buy['pnl_pct'].mean():+.2f}% | {len(buy)}건")

    # SL 근거별 비교
    print(f"\n📊 SL 근거별 성과:")
    for src in df['sl_source'].unique():
        sub = df[df['sl_source'] == src]
        wr = len(sub[sub['pnl_pct'] > 0]) / len(sub) * 100
        print(f"  {src}: 승률 {wr:.0f}% | 평균 {sub['pnl_pct'].mean():+.2f}% | {len(sub)}건")

    # 상세 결과
    print(f"\n{'─'*90}")
    print(f"{'종목':>12} {'판정':>12} {'진입일':>12} {'진입가':>10} {'결과':>10} "
          f"{'청산가':>10} {'수익률':>8} {'R배수':>6} {'보유일':>4}")
    print(f"{'─'*90}")

    for _, r in df.sort_values('pnl_pct', ascending=False).iterrows():
        emoji = {'TP2_HIT': '🎯', 'TP1_HIT': '✅', 'SL_HIT': '❌', 'EXPIRED': '⏰', 'HOLD': '⏳'}
        print(f"{r['name']:>12} {r['action']:>12} {r['entry_date']:>12} "
              f"{r['entry_price']:>10,.0f} {emoji.get(r['outcome'],'')}{r['outcome']:>8} "
              f"{r['exit_price']:>10,.0f} {r['pnl_pct']:>+7.2f}% {r['r_multiple']:>+5.2f}R "
              f"{r['exit_day']:>3}일")

    # 기대값 계산
    if total > 0:
        ev = avg_pnl  # 건당 평균 수익률 = 기대값
        print(f"\n{'='*70}")
        print(f"  📊 기대값(EV): 건당 {ev:+.2f}%")
        if ev > 0:
            print(f"  ✅ 양의 기대값 → 이 전략은 장기적으로 수익 가능")
        else:
            print(f"  ⚠️ 음의 기대값 → 전략 개선 필요")
        print(f"{'='*70}")


if __name__ == "__main__":
    run_backtest()
