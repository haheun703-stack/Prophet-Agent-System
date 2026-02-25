# -*- coding: utf-8 -*-
"""전종목 데이터 수집 스크립트"""
import sys, os, time
os.environ["PYTHONIOENCODING"] = "utf-8"
sys.path.insert(0, "D:/Prophet_Agent_System_예언자/scalper-agent")
os.chdir("D:/Prophet_Agent_System_예언자/scalper-agent")

import logging
logging.basicConfig(level=logging.WARNING)

from data.universe_builder import load_universe, collect_daily_pykrx
from data.flow_collector import (
    collect_investor_flow,
    collect_foreign_exhaustion,
    collect_short_balance,
    collect_short_volume,
)

uni = load_universe()
codes = list(uni.keys())
print(f"유니버스: {len(codes)}개 종목\n")

t0 = time.time()

# 1. 일봉
print("[1/5] 일봉 OHLCV 수집...")
collect_daily_pykrx(codes, months=24, force=False)
print(f"  경과: {time.time()-t0:.0f}초\n")

# 2. 투자자별 순매수
print("[2/5] 투자자별 순매수 (기관/외인)...")
collect_investor_flow(codes, months=24, force=False)
print(f"  경과: {time.time()-t0:.0f}초\n")

# 3. 외국인 소진율
print("[3/5] 외국인 소진율...")
collect_foreign_exhaustion(codes, months=24, force=False)
print(f"  경과: {time.time()-t0:.0f}초\n")

# 4. 공매도 잔고
print("[4/5] 공매도 잔고...")
collect_short_balance(codes, months=24, force=False)
print(f"  경과: {time.time()-t0:.0f}초\n")

# 5. 공매도 거래량
print("[5/5] 공매도 거래량...")
collect_short_volume(codes, months=24, force=False)

total = time.time() - t0
print(f"\n{'='*60}")
print(f"  ✅ 전체 수집 완료: {len(codes)}종목, {total:.0f}초 소요")
print(f"{'='*60}")
