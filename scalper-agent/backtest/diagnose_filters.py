"""
필터 병목 진단: 어디서 거래 기회가 걸러지는지 분석
"""
import sys
import io
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
from backtest.real_data_backtest import download_5min_data, TOP_STOCKS
from data.csv_loader import CSVLoader
from strategies.daily_filter import DailyFilter

def diagnose():
    print("=" * 60)
    print("  필터 병목 진단")
    print("=" * 60)

    etf_df, stock_data = download_5min_data("1mo")
    etf_df["date"] = etf_df.index.date
    trading_days = sorted(etf_df["date"].unique())

    code_to_name = {code: name for code, (name, _) in TOP_STOCKS.items()}

    # 일봉 필터
    csv_loader = CSVLoader()
    daily_filter = DailyFilter()
    daily_whitelist = {}
    for code in stock_data.keys():
        df_daily = csv_loader.load(code)
        if df_daily is not None and len(df_daily) >= 60:
            name = code_to_name.get(code, code)
            score = daily_filter.score_stock(df_daily, code, name)
            if score and score.total_score >= 50:
                daily_whitelist[code] = (score.total_score, score.grade)

    print(f"\n일봉 필터: {len(stock_data)}종목 → {len(daily_whitelist)}종목 통과")
    rejected_daily = set(stock_data.keys()) - set(daily_whitelist.keys())
    for code in rejected_daily:
        name = code_to_name.get(code, code)
        df_daily = csv_loader.load(code)
        if df_daily is not None and len(df_daily) >= 60:
            score = daily_filter.score_stock(df_daily, code, name)
            if score:
                print(f"  탈락: {code}({name}) {score.total_score:.0f}점 [{score.grade}]")

    print(f"\n{'='*60}")
    print(f"  일별 필터 진단")
    print(f"{'='*60}")

    etf_pass = 0
    etf_fail = 0
    candidate_counts = []

    for day_date in trading_days:
        etf_day = etf_df[etf_df["date"] == day_date]
        if len(etf_day) < 10:
            continue

        first_bars = etf_day.iloc[:4]
        etf_open = float(first_bars.iloc[0]["open"])
        etf_close = float(first_bars.iloc[-1]["close"])
        etf_change = (etf_close - etf_open) / etf_open * 100

        if etf_change <= 0.15:
            etf_fail += 1
            print(f"\n  {day_date} ETF변화: {etf_change:+.3f}% → 탈락 (<=0.15%)")
            continue

        etf_pass += 1
        print(f"\n  {day_date} ETF변화: {etf_change:+.3f}% → 통과")

        # 종목 스캔
        total_stocks = 0
        pass_whitelist = 0
        pass_data = 0
        pass_positive = 0
        pass_relstr = 0
        pass_volume = 0

        for code, df in stock_data.items():
            total_stocks += 1

            if code not in daily_whitelist:
                continue
            pass_whitelist += 1

            day_df = df[df.index.date == day_date]
            if len(day_df) < 10:
                continue
            pass_data += 1

            stock_first = day_df.iloc[:4]
            s_open = float(stock_first.iloc[0]["open"])
            s_close = float(stock_first.iloc[-1]["close"])
            if s_open == 0:
                continue
            s_change = (s_close - s_open) / s_open * 100

            if s_change <= 0:
                continue
            pass_positive += 1

            rel_str = abs(s_change) / abs(etf_change) if abs(etf_change) > 0 else 0
            name = code_to_name.get(code, code)

            if rel_str < 1.5:
                print(f"    {code}({name}) 변화:{s_change:+.2f}% 상대강도:{rel_str:.2f} → 탈락 (<1.5)")
                continue
            pass_relstr += 1

            recent_vol = float(stock_first["volume"].mean())
            if recent_vol < 1000:
                print(f"    {code}({name}) 거래량:{recent_vol:.0f} → 탈락 (<1000)")
                continue
            pass_volume += 1

            print(f"    + {code}({name}) 변화:{s_change:+.2f}% 상대강도:{rel_str:.2f} 거래량:{recent_vol:.0f} → 후보!")

        print(f"    요약: 전체{total_stocks} → 화이트{pass_whitelist} → 데이터{pass_data} → 양봉{pass_positive} → 상대강도{pass_relstr} → 거래량{pass_volume}")
        candidate_counts.append(pass_volume)

    print(f"\n{'='*60}")
    print(f"  종합 진단")
    print(f"{'='*60}")
    print(f"  총 거래일: {len(trading_days)}")
    print(f"  ETF 통과: {etf_pass}일 / 탈락: {etf_fail}일")
    print(f"  ETF 통과율: {etf_pass/len(trading_days)*100:.1f}%")
    if candidate_counts:
        print(f"  ETF 통과일 평균 후보: {sum(candidate_counts)/len(candidate_counts):.1f}종목")
    print(f"{'='*60}")


if __name__ == "__main__":
    diagnose()
