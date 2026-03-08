"""
외국인 누적 순매수 → 수익률 백테스트
기간: 2025-12-01 ~ 2026-02-27 (정상장)
"""
import sys, io, os
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

import pandas as pd
import numpy as np
from pathlib import Path
from collections import defaultdict
import warnings
warnings.filterwarnings('ignore')

# ─── 경로 설정 ───
BASE = Path(r"D:\Prophet_Agent_System_예언자\scalper-agent\data_store")
FLOW_DIR = BASE / "flow"
DAILY_DIR = BASE / "daily"

START_DATE = "2025-12-01"
END_DATE = "2026-02-27"
ROLLING_WINDOW = 5  # 5일 누적
FORWARD_DAYS = [5, 10, 20]  # 수익률 측정 기간

def load_investor_csv(code):
    """투자자 수급 CSV 로드"""
    path = FLOW_DIR / f"{code}_investor.csv"
    if not path.exists():
        return None
    try:
        df = pd.read_csv(path, index_col=0, parse_dates=True)
        # 컬럼명 정리
        df.columns = [c.strip() for c in df.columns]
        return df
    except Exception:
        return None

def load_daily_csv(code):
    """일봉 CSV 로드"""
    path = DAILY_DIR / f"{code}.csv"
    if not path.exists():
        return None
    try:
        df = pd.read_csv(path, index_col=0, parse_dates=True)
        df.columns = [c.strip() for c in df.columns]
        return df
    except Exception:
        return None

def get_all_codes():
    """flow/ 폴더에서 전체 종목코드 추출"""
    codes = set()
    for f in FLOW_DIR.glob("*_investor.csv"):
        code = f.stem.replace("_investor", "")
        # daily csv도 있는지 확인
        if (DAILY_DIR / f"{code}.csv").exists():
            codes.add(code)
    return sorted(codes)

def compute_signals_and_returns(code):
    """
    한 종목에 대해:
    - 외국인 5일 누적 순매수 금액 계산
    - 기관 5일 누적 순매수 금액 계산
    - 이후 5/10/20일 수익률 계산

    Returns: list of dict (각 시그널 시점)
    """
    inv_df = load_investor_csv(code)
    daily_df = load_daily_csv(code)

    if inv_df is None or daily_df is None:
        return []

    # 날짜 범위 필터 (시그널 생성 기간)
    inv_df = inv_df.sort_index()
    daily_df = daily_df.sort_index()

    # 외국인_금액, 기관_금액 컬럼 확인
    if '외국인_금액' not in inv_df.columns or '기관_금액' not in inv_df.columns:
        return []

    # 숫자 변환 (혹시 문자열인 경우)
    inv_df['외국인_금액'] = pd.to_numeric(inv_df['외국인_금액'], errors='coerce').fillna(0)
    inv_df['기관_금액'] = pd.to_numeric(inv_df['기관_금액'], errors='coerce').fillna(0)

    # 5일 누적 순매수
    inv_df['외국인_5일누적'] = inv_df['외국인_금액'].rolling(ROLLING_WINDOW, min_periods=ROLLING_WINDOW).sum()
    inv_df['기관_5일누적'] = inv_df['기관_금액'].rolling(ROLLING_WINDOW, min_periods=ROLLING_WINDOW).sum()

    # 종가 컬럼
    if '종가' not in daily_df.columns:
        return []

    daily_df['종가'] = pd.to_numeric(daily_df['종가'], errors='coerce')

    # 시그널 기간 필터
    mask = (inv_df.index >= START_DATE) & (inv_df.index <= END_DATE)
    signal_df = inv_df.loc[mask].dropna(subset=['외국인_5일누적'])

    results = []
    for date, row in signal_df.iterrows():
        foreign_5d = row['외국인_5일누적']
        inst_5d = row['기관_5일누적']

        # 당일 종가 가져오기
        if date not in daily_df.index:
            continue
        entry_price = daily_df.loc[date, '종가']
        if pd.isna(entry_price) or entry_price <= 0:
            continue

        # 이후 N일 수익률 계산
        future_dates = daily_df.index[daily_df.index > date]

        ret_dict = {'code': code, 'date': date,
                     'foreign_5d': foreign_5d, 'inst_5d': inst_5d,
                     'entry_price': entry_price}

        for n in FORWARD_DAYS:
            if len(future_dates) >= n:
                future_price = daily_df.loc[future_dates[n-1], '종가']
                if pd.notna(future_price) and future_price > 0:
                    ret_dict[f'ret_{n}d'] = (future_price - entry_price) / entry_price * 100
                else:
                    ret_dict[f'ret_{n}d'] = np.nan
            else:
                ret_dict[f'ret_{n}d'] = np.nan

        results.append(ret_dict)

    return results

def categorize_signal(row):
    """시그널 분류"""
    foreign_buy = row['foreign_5d'] > 0
    foreign_sell = row['foreign_5d'] < 0
    inst_buy = row['inst_5d'] > 0
    inst_sell = row['inst_5d'] < 0

    if foreign_buy and inst_buy:
        return 'foreign+inst_buy'      # 외인+기관 동시 순매수
    elif foreign_buy and inst_sell:
        return 'foreign_only_buy'       # 외인 단독 순매수 (기관 매도)
    elif foreign_sell:
        return 'foreign_sell'           # 외인 순매도
    else:
        return 'other'

def main():
    print("=" * 80)
    print("  외국인 누적 순매수 → 수익률 백테스트")
    print(f"  기간: {START_DATE} ~ {END_DATE}")
    print(f"  5일 누적 기준, 이후 {FORWARD_DAYS}일 수익률 측정")
    print("=" * 80)

    all_codes = get_all_codes()
    print(f"\n전체 유니버스: {len(all_codes)}종목 (flow + daily 교집합)")

    # 샘플 30종목 선택 - 시가총액 큰 대표 종목들
    # 실제로는 모든 종목에서 시그널을 모으고, 종목수 기준 샘플링
    # → 더 나은 방법: 모든 종목 처리 후 시그널 랜덤 샘플링

    # 대형주 우선 (코드 기준 대표적 대형주들)
    priority_codes = [
        '005930',  # 삼성전자
        '000660',  # SK하이닉스
        '005380',  # 현대차
        '035420',  # NAVER
        '035720',  # 카카오
        '006400',  # 삼성SDI
        '051910',  # LG화학
        '003670',  # 포스코퓨처엠
        '105560',  # KB금융
        '055550',  # 신한지주
        '005490',  # POSCO홀딩스
        '000270',  # 기아
        '068270',  # 셀트리온
        '207940',  # 삼성바이오로직스
        '012330',  # 현대모비스
        '066570',  # LG전자
        '003550',  # LG
        '086790',  # 하나금융지주
        '096770',  # SK이노베이션
        '034730',  # SK
        '010130',  # 고려아연
        '009150',  # 삼성전기
        '028260',  # 삼성물산
        '017670',  # SK텔레콤
        '032830',  # 삼성생명
        '003490',  # 대한항공
        '015760',  # 한국전력
        '033780',  # KT&G
        '003570',  # SNT다이내믹스
        '103140',  # 풍산
    ]

    # 실존하는 코드만 필터링
    sample_codes = [c for c in priority_codes if c in all_codes]
    # 부족하면 추가
    if len(sample_codes) < 30:
        remaining = [c for c in all_codes if c not in sample_codes]
        np.random.seed(42)
        extra = list(np.random.choice(remaining, min(30 - len(sample_codes), len(remaining)), replace=False))
        sample_codes.extend(extra)

    sample_codes = sample_codes[:30]
    print(f"샘플 종목 수: {len(sample_codes)}")

    # 모든 시그널 수집
    all_signals = []
    processed = 0
    for code in sample_codes:
        signals = compute_signals_and_returns(code)
        all_signals.extend(signals)
        processed += 1

    print(f"처리 완료: {processed}종목, 총 시그널: {len(all_signals)}개")

    if not all_signals:
        print("시그널이 없습니다!")
        return

    df = pd.DataFrame(all_signals)
    df['category'] = df.apply(categorize_signal, axis=1)

    print(f"\n시그널 분포:")
    for cat, cnt in df['category'].value_counts().items():
        print(f"  {cat}: {cnt}개")

    # ─── 핵심 분석 ───
    categories = {
        'foreign+inst_buy': '외인+기관 동시 순매수',
        'foreign_only_buy': '외인 단독 순매수 (기관 매도)',
        'foreign_sell': '외인 순매도 (비교군)',
    }

    print("\n" + "=" * 80)
    print("  [핵심 결과] 카테고리별 수익률 비교")
    print("=" * 80)

    summary_rows = []

    for cat_key, cat_name in categories.items():
        subset = df[df['category'] == cat_key]
        if len(subset) == 0:
            print(f"\n  {cat_name}: 시그널 없음")
            continue

        print(f"\n{'─' * 60}")
        print(f"  {cat_name} (시그널 {len(subset)}개, {subset['code'].nunique()}종목)")
        print(f"{'─' * 60}")

        for n in FORWARD_DAYS:
            col = f'ret_{n}d'
            valid = subset[col].dropna()
            if len(valid) == 0:
                continue

            avg_ret = valid.mean()
            med_ret = valid.median()
            win_rate = (valid > 0).sum() / len(valid) * 100
            std_ret = valid.std()
            max_ret = valid.max()
            min_ret = valid.min()

            print(f"\n  [{n}일 후 수익률]")
            print(f"    평균: {avg_ret:+.2f}% | 중앙값: {med_ret:+.2f}%")
            print(f"    승률: {win_rate:.1f}% ({(valid > 0).sum()}/{len(valid)})")
            print(f"    표준편차: {std_ret:.2f}% | 최대: {max_ret:+.2f}% | 최소: {min_ret:+.2f}%")

            summary_rows.append({
                'category': cat_name,
                'period': f'{n}일',
                'signals': len(valid),
                'avg_ret': avg_ret,
                'median_ret': med_ret,
                'win_rate': win_rate,
                'std': std_ret,
            })

    # ─── 요약 테이블 ───
    print("\n\n" + "=" * 80)
    print("  [요약 테이블]")
    print("=" * 80)

    summary_df = pd.DataFrame(summary_rows)
    if not summary_df.empty:
        print(f"\n{'카테고리':<30} {'기간':>5} {'시그널':>6} {'평균수익률':>10} {'중앙값':>8} {'승률':>8}")
        print("─" * 75)
        for _, row in summary_df.iterrows():
            print(f"{row['category']:<30} {row['period']:>5} {row['signals']:>6} "
                  f"{row['avg_ret']:>+9.2f}% {row['median_ret']:>+7.2f}% {row['win_rate']:>7.1f}%")

    # ─── 외인 순매수 vs 순매도 직접 비교 ───
    print("\n\n" + "=" * 80)
    print("  [외인 순매수 vs 순매도 직접 비교]")
    print("=" * 80)

    buy_all = df[df['foreign_5d'] > 0]
    sell_all = df[df['foreign_5d'] < 0]

    print(f"\n{'구분':<20} ", end='')
    for n in FORWARD_DAYS:
        print(f"{'%d일 평균' % n:>12} {'승률':>8}", end='')
    print()
    print("─" * 65)

    for label, subset in [('외인 순매수', buy_all), ('외인 순매도', sell_all)]:
        print(f"{label:<20} ", end='')
        for n in FORWARD_DAYS:
            col = f'ret_{n}d'
            valid = subset[col].dropna()
            if len(valid) > 0:
                avg = valid.mean()
                wr = (valid > 0).sum() / len(valid) * 100
                print(f"{avg:>+11.2f}% {wr:>7.1f}%", end='')
            else:
                print(f"{'N/A':>12} {'N/A':>8}", end='')
        print()

    # ─── 종목별 상세 (상위 5개, 외인+기관 순매수 중 20일 수익률 기준) ───
    print("\n\n" + "=" * 80)
    print("  [종목별 상세: 외인+기관 동시순매수 → 20일 수익률 TOP 5]")
    print("=" * 80)

    both_buy = df[df['category'] == 'foreign+inst_buy'].copy()
    if not both_buy.empty and 'ret_20d' in both_buy.columns:
        top5 = both_buy.nlargest(5, 'ret_20d')
        for _, row in top5.iterrows():
            print(f"\n  종목: {row['code']} | 시그널일: {row['date'].strftime('%Y-%m-%d')}")
            print(f"  진입가: {row['entry_price']:,.0f}원")
            print(f"  외인5일: {row['foreign_5d']:+,.0f} | 기관5일: {row['inst_5d']:+,.0f}")
            for n in FORWARD_DAYS:
                col = f'ret_{n}d'
                if pd.notna(row[col]):
                    print(f"  {n}일 수익률: {row[col]:+.2f}%")

    print("\n\n" + "=" * 80)
    print("  [종목별 상세: 외인+기관 동시순매수 → 20일 수익률 WORST 5]")
    print("=" * 80)

    if not both_buy.empty and 'ret_20d' in both_buy.columns:
        worst5 = both_buy.nsmallest(5, 'ret_20d')
        for _, row in worst5.iterrows():
            print(f"\n  종목: {row['code']} | 시그널일: {row['date'].strftime('%Y-%m-%d')}")
            print(f"  진입가: {row['entry_price']:,.0f}원")
            print(f"  외인5일: {row['foreign_5d']:+,.0f} | 기관5일: {row['inst_5d']:+,.0f}")
            for n in FORWARD_DAYS:
                col = f'ret_{n}d'
                if pd.notna(row[col]):
                    print(f"  {n}일 수익률: {row[col]:+.2f}%")

    # ─── 추가: 전체 유니버스 (1567종목) 빠른 스캔 ───
    print("\n\n" + "=" * 80)
    print("  [보너스] 전체 유니버스 빠른 스캔 (통계만)")
    print("=" * 80)

    all_signals_full = []
    count = 0
    for code in all_codes:
        signals = compute_signals_and_returns(code)
        all_signals_full.extend(signals)
        count += 1
        if count % 200 == 0:
            print(f"  ... {count}/{len(all_codes)} 처리 중")

    print(f"  전체 처리 완료: {count}종목, 총 시그널: {len(all_signals_full)}개")

    if all_signals_full:
        df_full = pd.DataFrame(all_signals_full)
        df_full['category'] = df_full.apply(categorize_signal, axis=1)

        print(f"\n{'카테고리':<30} {'기간':>5} {'시그널':>7} {'평균수익률':>10} {'중앙값':>8} {'승률':>8}")
        print("─" * 80)

        for cat_key, cat_name in categories.items():
            subset = df_full[df_full['category'] == cat_key]
            for n in FORWARD_DAYS:
                col = f'ret_{n}d'
                valid = subset[col].dropna()
                if len(valid) > 0:
                    avg = valid.mean()
                    med = valid.median()
                    wr = (valid > 0).sum() / len(valid) * 100
                    print(f"{cat_name:<30} {n:>3}일 {len(valid):>7} "
                          f"{avg:>+9.2f}% {med:>+7.2f}% {wr:>7.1f}%")

    print("\n\n  백테스트 완료!")
    print("=" * 80)


if __name__ == '__main__':
    main()
