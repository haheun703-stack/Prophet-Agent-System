# -*- coding: utf-8 -*-
"""투자자동향 리포트 — 시장/섹터/종목별 기관·외인·개인 순매매"""
import sys, io, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

from pykrx import stock
from datetime import datetime, timedelta
import pandas as pd

today_str = datetime.now().strftime("%Y%m%d")
# 최근 영업일 기준 (주말 보정)
dt = datetime.now()
if dt.weekday() >= 5:
    dt -= timedelta(days=(dt.weekday() - 4))
    today_str = dt.strftime("%Y%m%d")

from5d = (dt - timedelta(days=10)).strftime("%Y%m%d")
from20d = (dt - timedelta(days=35)).strftime("%Y%m%d")

today_disp = dt.strftime("%Y-%m-%d")

print("=" * 100)
print("  투자자동향 리포트")
print(f"  {today_disp} 기준")
print("=" * 100)

# ═══ 1. 종목별 순매수 TOP — 각 투자자 유형별 ═══
investors = {
    "외국인합계": "외국인",
    "기관합계": "기관",
    "개인": "개인",
    "연기금": "연기금",
    "금융투자": "금융투자(증권사)",
}

print("\n" + "=" * 100)
print("  ■ 오늘 투자자별 순매수 TOP 10 (KOSPI)")
print("=" * 100)

for inv_code, inv_name in investors.items():
    try:
        df = stock.get_market_net_purchases_of_equities(
            today_str, today_str, market="KOSPI", investor=inv_code
        )
        if len(df) > 0:
            print(f"\n  ▶ {inv_name} 순매수 TOP 10")
            for i, (ticker, row) in enumerate(df.head(10).iterrows(), 1):
                amt = row.get("순매수거래대금", 0)
                name = row.get("종목명", ticker)
                print(f"    {i:>2}. {name:16s} {amt/1e8:>+10,.0f}억원")

            print(f"\n  ▶ {inv_name} 순매도 TOP 10")
            for i, (ticker, row) in enumerate(df.tail(10).iloc[::-1].iterrows(), 1):
                amt = row.get("순매수거래대금", 0)
                name = row.get("종목명", ticker)
                print(f"    {i:>2}. {name:16s} {amt/1e8:>+10,.0f}억원")
    except Exception as e:
        print(f"  {inv_name}: 데이터 없음 ({e})")

# ═══ 2. KOSDAQ도 ═══
print("\n" + "=" * 100)
print("  ■ 오늘 투자자별 순매수 TOP 10 (KOSDAQ)")
print("=" * 100)

for inv_code, inv_name in investors.items():
    try:
        df = stock.get_market_net_purchases_of_equities(
            today_str, today_str, market="KOSDAQ", investor=inv_code
        )
        if len(df) > 0:
            print(f"\n  ▶ {inv_name} 순매수 TOP 10")
            for i, (ticker, row) in enumerate(df.head(10).iterrows(), 1):
                amt = row.get("순매수거래대금", 0)
                name = row.get("종목명", ticker)
                print(f"    {i:>2}. {name:16s} {amt/1e8:>+10,.0f}억원")

            print(f"\n  ▶ {inv_name} 순매도 TOP 10")
            for i, (ticker, row) in enumerate(df.tail(10).iloc[::-1].iterrows(), 1):
                amt = row.get("순매수거래대금", 0)
                name = row.get("종목명", ticker)
                print(f"    {i:>2}. {name:16s} {amt/1e8:>+10,.0f}억원")
    except Exception as e:
        print(f"  {inv_name}: 데이터 없음 ({e})")

# ═══ 3. 5일 누적 순매수 TOP ═══
print("\n" + "=" * 100)
print("  ■ 5일 누적 순매수 TOP 15 (KOSPI)")
print("=" * 100)

for inv_code, inv_name in [("외국인합계", "외국인"), ("기관합계", "기관")]:
    try:
        df = stock.get_market_net_purchases_of_equities(
            from5d, today_str, market="KOSPI", investor=inv_code
        )
        if len(df) > 0:
            print(f"\n  ▶ {inv_name} 5일 순매수 TOP 15")
            for i, (ticker, row) in enumerate(df.head(15).iterrows(), 1):
                amt = row.get("순매수거래대금", 0)
                name = row.get("종목명", ticker)
                print(f"    {i:>2}. {name:16s} {amt/1e8:>+10,.0f}억원")
    except Exception as e:
        print(f"  {inv_name} 5일: 데이터 없음 ({e})")

print("\n" + "=" * 100)
print("  ■ 5일 누적 순매수 TOP 15 (KOSDAQ)")
print("=" * 100)

for inv_code, inv_name in [("외국인합계", "외국인"), ("기관합계", "기관")]:
    try:
        df = stock.get_market_net_purchases_of_equities(
            from5d, today_str, market="KOSDAQ", investor=inv_code
        )
        if len(df) > 0:
            print(f"\n  ▶ {inv_name} 5일 순매수 TOP 15")
            for i, (ticker, row) in enumerate(df.head(15).iterrows(), 1):
                amt = row.get("순매수거래대금", 0)
                name = row.get("종목명", ticker)
                print(f"    {i:>2}. {name:16s} {amt/1e8:>+10,.0f}억원")
    except Exception as e:
        print(f"  {inv_name} 5일: 데이터 없음 ({e})")

# ═══ 4. 업종별 투자자 동향 (주요 업종) ═══
print("\n" + "=" * 100)
print("  ■ 주요 업종별 투자자 동향 (오늘)")
print("=" * 100)

sector_indices = {
    "1001": "코스피전체",
    "1024": "증권",
    "1013": "전기전자",
    "1008": "화학",
    "1009": "제약",
    "1015": "운송장비",
    "1012": "기계장비",
    "1021": "금융",
    "1019": "운송창고",
    "1018": "건설",
    "1011": "금속",
}

print(f"\n  {'업종':10s} {'기관':>12s} {'외국인':>12s} {'개인':>12s} {'금융투자':>12s} {'연기금':>12s}")
print("-" * 80)

for idx, name in sector_indices.items():
    try:
        df = stock.get_market_trading_value_by_date(
            today_str, today_str, ticker=idx, on="순매수", detail=True
        )
        if len(df) > 0:
            row = df.iloc[-1]
            cols = df.columns.tolist()
            # Try to find the right column names
            inst = 0
            foreign = 0
            indiv = 0
            finvest = 0
            pension = 0

            for col in cols:
                if "기관" in col:
                    inst = row[col]
                elif "외국인" in col and "기타" not in col:
                    foreign = row[col]
                elif "개인" in col:
                    indiv = row[col]
                elif "금융투자" in col:
                    finvest = row[col]
                elif "연기금" in col:
                    pension = row[col]

            print(
                f"  {name:10s}"
                f" {inst/1e8:>+12,.0f}"
                f" {foreign/1e8:>+12,.0f}"
                f" {indiv/1e8:>+12,.0f}"
                f" {finvest/1e8:>+12,.0f}"
                f" {pension/1e8:>+12,.0f}"
            )
    except Exception as e:
        pass

print(f"\n  (단위: 억원)")

print("\n" + "=" * 100)
print("  리포트 완료")
print("=" * 100)
