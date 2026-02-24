# -*- coding: utf-8 -*-
"""TOP 20 매물소진 종목 — 1D~4D 상세 분석"""
import sys, io, os, json
sys.path.insert(0, 'd:/Prophet_Agent_System_예언자/scalper-agent')
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
os.chdir('d:/Prophet_Agent_System_예언자/scalper-agent')

import logging
logging.basicConfig(level=logging.WARNING)

from data.signal_analyzer import SignalAnalyzer
from data.universe_builder import load_universe

uni = load_universe()
sa = SignalAnalyzer()

# TOP 20 exhaustion stocks (from scanner)
top_codes = [
    '047920', '298830', '294570', '466100',
    '067630', '138610', '328130', '388720', '455900',
    '000650', '226950', '249420', '454910', '462860',
    '049070', '053030', '101360', '101930', '462350',
]
# 에스엔시스 0008Z0 도 시도
top_codes.insert(0, '0008Z0')

# +정배열 종목 추가 (쿠콘/루닛/에코앤드림 등 + 미래에셋증권우 비교)
extra_codes = ['006805']  # 미래에셋증권우
top_codes.extend(extra_codes)

print("=" * 100)
print("  매물소진 TOP 20+α — 1D~4D 상세 분석")
print("  2026-02-19 기준")
print("=" * 100)

results = []

for code in top_codes:
    if code not in uni:
        print(f"\n  {code}: 유니버스에 없음 (스킵)")
        continue
    name = uni[code]['name']
    try:
        rec = sa.analyze_stock(code, name=name)
        if rec:
            results.append(rec)
            report = sa.format_report(rec)
            print(report)
        else:
            print(f"\n  {name}({code}): 데이터 부족")
    except Exception as e:
        print(f"\n  {name}({code}): 에러 - {e}")

# ── 종합 순위 ──
print("\n\n" + "=" * 100)
print("  종합 순위 — 매물소진 + 1D~4D 통합")
print("=" * 100)

# Sort by confidence
results.sort(key=lambda x: -x.get('confidence', 0))

print(f"\n{'#':>2} {'종목':>14} {'코드':>8} {'시그널':>12} {'확신도':>4}"
      f" {'1D패턴':>16} {'2D소진':>8} {'3D수급':>4}"
      f" {'RSI':>5} {'MACD':>10} {'BB':>5} {'키시그널'}")
print("-" * 130)

for i, r in enumerate(results, 1):
    marker = ""
    conf = r.get('confidence', 0)
    if conf >= 75:
        marker = " ★★★"
    elif conf >= 60:
        marker = " ★★"
    elif conf >= 50:
        marker = " ★"

    print(
        f"{i:>2} {r.get('name',''):>14} {r.get('code',''):>8}"
        f" {r.get('signal',''):>12} {conf:>4}"
        f" {r.get('candle_pattern',''):>16} {r.get('vol_exhaustion',''):>8}"
        f" {r.get('supply_grade',''):>4}"
        f" {r.get('rsi14',0):>5.1f} {r.get('macd_cross',''):>10}"
        f" {r.get('bb_position',0):>5.3f}"
        f" {r.get('key_signal','')}{marker}"
    )

# ── 미래에셋증권우 패턴 매칭 점수 ──
print("\n\n" + "=" * 100)
print("  미래에셋증권우 패턴 유사도 분석")
print("=" * 100)
print("  조건: 매물소진(VR<0.5) + 횡보 + RSI30~50 + MACD수렴 + MA근접")
print()

for r in results:
    match_score = 0
    match_reasons = []

    # 매물소진
    if r.get('vol_exhaustion') in ('yes', 'possible'):
        match_score += 25
        match_reasons.append("매물소진O")
    elif r.get('vol_ratio_ma20', 1) < 0.5:
        match_score += 20
        match_reasons.append(f"VR:{r.get('vol_ratio_ma20',0):.2f}")

    # RSI 과매도 접근
    rsi = r.get('rsi14', 50)
    if 30 <= rsi <= 45:
        match_score += 25
        match_reasons.append(f"RSI:{rsi:.0f}(과매도접근)")
    elif 45 < rsi <= 55:
        match_score += 15
        match_reasons.append(f"RSI:{rsi:.0f}(중립)")

    # MACD 수렴/골든크로스
    if r.get('macd_cross') == 'golden_cross':
        match_score += 25
        match_reasons.append("MACD골든크로스!")
    elif r.get('macd_hist', 0) > 0:
        match_score += 15
        match_reasons.append("MACD양전환")

    # MA 배열
    ma_st = r.get('ma_status', '')
    if ma_st in ('perfect_bull', 'bull'):
        match_score += 15
        match_reasons.append("정배열")
    elif ma_st == 'neutral_up':
        match_score += 10
        match_reasons.append("MA20위")

    # 수급
    grade = r.get('supply_grade', 'D')
    if grade in ('A+', 'A'):
        match_score += 10
        match_reasons.append(f"수급{grade}")
    elif grade == 'B':
        match_score += 5
        match_reasons.append("수급B")

    marker = ""
    if match_score >= 70:
        marker = " ★★★ 반등 임박!"
    elif match_score >= 55:
        marker = " ★★ 관찰"
    elif match_score >= 40:
        marker = " ★ 대기"

    print(f"  {r.get('name',''):>14}({r.get('code','')}) "
          f"패턴매칭: {match_score}/100 — "
          f"{' | '.join(match_reasons)}{marker}")
