# -*- coding: utf-8 -*-
"""점화 족적 추출 — 승자들이 '터진 그 날'의 공통 신호 (사장님 5/31 핵심 지시).

목적: 과거 승자(끝물)를 묘사가 아니라, **점화일(기지에서 터진 날)의 족적**을 뽑아
'지금 그 단계에 있는 종목을 찾는 무기(스캐너)'의 설계도를 만든다.

점화일 정의(기지→폭발, 끝물 아님):
  최근 50거래일 중 첫 날 i where: 당일등락 ≥ IGNITE_GAIN AND 거래량배수 ≥ IGNITE_VOLX
  AND 직전 위치 pos20 ≤ BASE_POS (전일 종가가 직전20일 range 하단~중단 = 아직 안 extended).
출력: 점화일 신호 + 점화 후 총수익(이 신호가 실제로 돈이 됐나) + 점화일 끼.
★ 한계: 일봉, 생존편향(이미 오른 10종목). 패턴 추출용 — 무기는 별도 백테스트 필요.
"""
from __future__ import annotations
import csv
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PROJ = ROOT.parent
sys.path.insert(0, str(ROOT))
from data.limit_up_scanner import LimitUpStock, score_kki, count_surge_limit_days
from data.sajang_rules import SAJANG

DAILY = PROJ / "stock_data_daily"
LOOK = 50
IGNITE_GAIN = 0.10   # 점화일 최소 등락
IGNITE_VOLX = 2.5    # 거래량 배수
BASE_POS = 0.6       # 직전 위치(기지) 상한

STOCKS = [("삼성전기", "009150"), ("LG이노텍", "011070"), ("삼화콘덴서", "001820"),
          ("삼성전기우", "009155"), ("LG전자", "066570"), ("현대오토에버", "307950"),
          ("피델릭스", "032580"), ("에스에이엠티", "031330"), ("나무기술", "242040")]


def _load(name, code):
    for p in DAILY.glob(f"{name}_{code}.csv"):
        rows = []
        with open(p, encoding="utf-8") as f:
            for r in csv.DictReader(f):
                try:
                    rows.append((r.get("날짜") or r.get("") or "", float(r["open"]), float(r["high"]),
                                 float(r["low"]), float(r["close"]), float(r["volume"])))
                except (ValueError, KeyError):
                    continue
        return rows
    return []


def _find_ignition(rows):
    n = len(rows)
    start = max(21, n - LOOK)
    for i in range(start, n):
        c, pc = rows[i][4], rows[i - 1][4]
        if pc <= 0:
            continue
        gain = c / pc - 1
        vavg = sum(rows[j][5] for j in range(i - 20, i)) / 20
        volx = rows[i][5] / vavg if vavg > 0 else 0
        lows = [rows[j][3] for j in range(i - 20, i)]
        highs = [rows[j][2] for j in range(i - 20, i)]
        rng = max(highs) - min(lows)
        pos20 = (pc - min(lows)) / rng if rng > 0 else 0.5   # 전일 위치(기지 여부)
        if gain >= IGNITE_GAIN and volx >= IGNITE_VOLX and pos20 <= BASE_POS:
            return i, gain, volx, pos20
    return None, 0, 0, 0


def main():
    uni = {}
    try:
        uni = json.load(open(ROOT / "data_store" / "universe.json", encoding="utf-8"))
    except Exception:
        pass

    print(f"점화 기준: 등락≥{IGNITE_GAIN*100:.0f}% & 거래량≥{IGNITE_VOLX}x & 직전위치≤{BASE_POS*100:.0f}%(기지)\n")
    print(f'{"종목":<14}{"점화일":>11}{"점화등락":>8}{"거래량":>7}{"기지위치":>8}{"점화후수익":>10}{"점화후일":>7}{"점화일끼":>16}')
    gains, volxs, poss = [], [], []
    for name, code in STOCKS:
        rows = _load(name, code)
        if len(rows) < 25:
            print(f"{name:<14} 데이터부족")
            continue
        i, gain, volx, pos20 = _find_ignition(rows)
        if i is None:
            print(f"{name:<14} {'점화일 미검출(이미 추세중/조건밖)':>40}")
            continue
        ig = rows[i]
        run = rows[-1][4] / ig[4] - 1
        days = len(rows) - 1 - i
        # 점화일 끼 (그 시점 데이터로)
        cap = uni.get(code, {}).get("cap_억", 0)
        closes_to_i = [r[4] for r in rows[:i + 1]]
        seg = rows[max(0, i - 14):i + 1]
        trs = [max(seg[k][2] - seg[k][3], abs(seg[k][2] - seg[k - 1][4]), abs(seg[k][3] - seg[k - 1][4]))
               for k in range(1, len(seg))]
        atr = (sum(trs) / len(trs)) / ig[4] * 100 if trs and ig[4] > 0 else 0
        surge, limit = count_surge_limit_days(closes_to_i)
        vavg = sum(rows[j][5] for j in range(i - 20, i)) / 20
        st = LimitUpStock(code=code, name=name, close=int(ig[4]), volume_ratio=round(volx, 2),
                          trading_value_억=round(ig[4] * ig[5] / 1e8, 1),
                          turnover_pct=round((ig[4] * ig[5] / 1e8) / cap * 100, 2) if cap else 0,
                          close_strength=round((ig[4] - ig[3]) / (ig[2] - ig[3]), 3) if ig[2] > ig[3] else 0.5,
                          consecutive_limit=0)
        kki = score_kki(st, atr, surge, limit)
        gains.append(gain * 100); volxs.append(volx); poss.append(pos20 * 100)
        print(f'{name:<14}{ig[0]:>11}{gain*100:>+7.0f}%{volx:>6.1f}x{pos20*100:>7.0f}%{run*100:>+9.0f}%{days:>6}일'
              f'  {kki:.0f}={SAJANG.kki_grade(kki)}')

    if gains:
        n = len(gains)
        print(f"\n── 공통 점화 족적 (검출 {n}종목 평균) ──")
        print(f"  점화 등락 {sum(gains)/n:+.0f}% | 거래량 {sum(volxs)/n:.1f}x | 기지위치 {sum(poss)/n:.0f}%")
        print(f"  → 무기 1차 기준: 기지(위치≤{BASE_POS*100:.0f}%)에서 +{IGNITE_GAIN*100:.0f}%↑ & 거래량 {IGNITE_VOLX}x↑ 돌파 + 끼 HUNTABLE↑")
    print("\n★ 이게 '무기(점화 스캐너)'의 설계도. 다음 = 이 기준으로 오늘 유니버스 스캔 + 백테스트(점화일 진입 시 실제 수익).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
