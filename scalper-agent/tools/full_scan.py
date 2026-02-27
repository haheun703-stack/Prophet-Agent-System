# -*- coding: utf-8 -*-
"""전종목 6D 스캔"""
import sys, os
os.environ["PYTHONIOENCODING"] = "utf-8"
sys.path.insert(0, 'D:/Prophet_Agent_System_예언자/scalper-agent')
os.chdir('D:/Prophet_Agent_System_예언자/scalper-agent')

from dotenv import load_dotenv
load_dotenv('D:/Prophet_Agent_System_예언자/.env')

from data.supply_analyzer import SupplyAnalyzer
from data.universe_builder import load_universe, get_universe_dict

# 새 유니버스 사용
UNIVERSE = get_universe_dict()
analyzer = SupplyAnalyzer()

# ETF 제외
exclude = {'069500', '371160', '102780', '305720'}
codes = [c for c in UNIVERSE.keys() if c not in exclude]
print(f'전체 종목 수: {len(codes)}개\n', flush=True)

fulls = analyzer.scan_all_full(codes)

order = {'STRONG_BUY': 0, 'BUY': 1, 'ENTER': 2, 'WATCH': 3, 'HOLD': 4, 'SKIP': 5}
fulls.sort(key=lambda f: (order.get(f.action, 6), -f.score.total_score))

print(f'분석 완료: {len(fulls)}개\n')

header = f"{'#':>2} {'종목':>12} {'판정':>24} {'3D':>10} {'4D':>14} {'5D에너지':>18} {'6D기술':>10} {'기관':>16} {'외인':>10} {'SM%':>6} {'ATR%':>5} {'PER':>7} {'PBR':>5}"
print(header)
print('=' * 170)

for i, f in enumerate(fulls, 1):
    s = f.score
    m = f.momentum
    st = f.stability
    raw = UNIVERSE.get(s.code, s.code)
    name = raw[0] if isinstance(raw, tuple) else raw

    if st:
        energy = f"{st.stability_grade}({st.stability_score:.0f})"
        atr = f"{st.atr_pct:.1f}"
        sm = f"{st.smart_money_ratio:+.1f}"
    else:
        energy = "N/A"
        atr = "0.0"
        sm = "0.0"

    # 6D 기술건강도
    th = f.tech_health
    tech_str = f"{th.tech_grade}({th.tech_score:.0f})" if th else "N/A"

    label = f"{f.risk_label}"
    d3 = f"{s.grade}({s.total_score:.0f})"
    d4 = f"{m.signal}({m.momentum_score:.0f})"
    inst = f"{m.inst_streak:+d}일({m.inst_streak_amount:+.0f}억)"

    # PER/PBR 표시
    per_str = f"{f.per:.1f}" if f.per > 0 else "적자"
    pbr_str = f"{f.pbr:.2f}" if f.pbr > 0 else "-"

    marker = ''
    if f.action == 'STRONG_BUY':
        marker = ' ★★★'
    elif f.action == 'BUY':
        marker = ' ★★'
    elif f.action == 'ENTER':
        marker = ' ★'

    print(f"{i:>2} {name:>12} {label:>24} {d3:>10} {d4:>14} {energy:>18} {tech_str:>10} {inst:>16} {m.foreign_inflection:>10} {sm:>6}% {atr:>5}% {per_str:>7} {pbr_str:>5}{marker}")

print('=' * 170)

# 요약
buy_list = [f for f in fulls if f.action in ('STRONG_BUY', 'BUY')]
enter_list = [f for f in fulls if f.action == 'ENTER']
print(f"\n★ STRONG_BUY/BUY: {len(buy_list)}개")
def _name(code):
    raw = UNIVERSE.get(code, code)
    return raw[0] if isinstance(raw, tuple) else raw

for f in buy_list:
    name = _name(f.score.code)
    st = f.stability
    th = f.tech_health
    energy = f"{st.stability_grade}({st.stability_score:.0f})" if st else "N/A"
    tech = f"6D:{th.tech_grade}({th.tech_score:.0f})" if th else "6D:N/A"
    per_str = f"PER:{f.per:.1f}" if f.per > 0 else "PER:적자"
    warn = f" ⚠{f.valuation_warning}" if f.valuation_warning else ""
    print(f"  → {name}({f.score.code}) {f.risk_label} | 3D:{f.score.grade}({f.score.total_score:.0f}) 4D:{f.momentum.signal}({f.momentum.momentum_score:.0f}) 5D:{energy} {tech} | 기관:{f.momentum.inst_streak:+d}일({f.momentum.inst_streak_amount:+.0f}억) | {per_str}{warn}")
    if f.baseline:
        b = f.baseline
        ic = f"기관원가:{b.inst_cost:,.0f}" if b.inst_cost > 0 else ""
        print(f"    📍 진입:{b.close:,.0f} SL:{b.invalidation:,.0f}({b.invalidation_source}) TP1:{b.target_1:,.0f} TP2:{b.target_2:,.0f} | {ic}")

print(f"\n★ ENTER (진입대기): {len(enter_list)}개")
for f in enter_list:
    name = _name(f.score.code)
    st = f.stability
    th = f.tech_health
    energy = f"{st.stability_grade}({st.stability_score:.0f})" if st else "N/A"
    tech = f"6D:{th.tech_grade}({th.tech_score:.0f})" if th else "6D:N/A"
    per_str = f"PER:{f.per:.1f}" if f.per > 0 else "PER:적자"
    warn = f" ⚠{f.valuation_warning}" if f.valuation_warning else ""
    print(f"  → {name}({f.score.code}) {f.risk_label} | 3D:{f.score.grade}({f.score.total_score:.0f}) 4D:{f.momentum.signal}({f.momentum.momentum_score:.0f}) 5D:{energy} {tech} | {per_str}{warn}")
    if f.baseline:
        b = f.baseline
        ic = f"기관원가:{b.inst_cost:,.0f}" if b.inst_cost > 0 else ""
        print(f"    📍 진입:{b.close:,.0f} SL:{b.invalidation:,.0f}({b.invalidation_source}) TP1:{b.target_1:,.0f} TP2:{b.target_2:,.0f} | {ic}")
