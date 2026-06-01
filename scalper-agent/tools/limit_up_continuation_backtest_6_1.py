# -*- coding: utf-8 -*-
"""상한가 엔진 적중 백테스트 — "잘 맞췄어"를 숫자로 (6/1, 사장님 관찰 검증).

사장님 6/1: "상한가 엔진으로 뽑은 종목들이 생각보다 잘 맞췄어."
→ 감을 숫자로. 엔진 논지(limit_up_scanner:5 / continuation_tracker 상수)를 그대로 측정:
   **어제 종가 +SIGNAL%↑ (강세/상한가) → 익일 +TARGET%↑ 도달하나?**
   (LIMIT_UP_PCT_THRESHOLD=15 / CONTINUATION_PCT_TRIGGER=10 — 엔진 실제값)

★ 단타봇 호흡(D→D+1) 측정 — 묻어두기/트레일과 무관, 순수 단타 선정 적중.
★ 정직 2단: ①"익일 +10% 터치"(엔진 주장, 낙관) ②**갭 먹고 D+1 시가 매수 시 실제 손익**(현실).
   익일은 갭상승으로 시작 → 시가에 사면 +10%까지 여유가 줄어듦. 그게 진짜 단타 수익.

데이터: stock_data_daily 일봉. `--selftest`(데이터 불필요) / `--data DIR`.
한계: 일봉(장중 경로 모름 — 터치=고점 도달 가정), 생존편향, 비용 0.48%/leg. 상대·분포로 해석.
"""
from __future__ import annotations
import argparse
import csv
import glob
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
DAILY_DEFAULT = ROOT / "stock_data_daily"

SIGNAL_PCT = 15.0     # 어제 종가 상승% (엔진 LIMIT_UP_PCT_THRESHOLD)
TARGET_PCT = 10.0     # 익일 도달 목표% from D종가 (엔진 CONTINUATION_PCT_TRIGGER)
COST_LEG = 0.0048     # 왕복 비용(매수+매도 슬립+세금)


def _load(path):
    rows = []
    try:
        with open(path, encoding="utf-8-sig") as f:   # BOM 방어 (#2 교훈)
            for r in csv.DictReader(f):
                try:
                    o = float(r["open"]); h = float(r["high"])
                    l = float(r["low"]); c = float(r["close"]); v = float(r["volume"])
                except (ValueError, KeyError):
                    continue
                if o > 0 and c > 0:
                    rows.append((o, h, l, c, v))
    except Exception:
        return []
    return rows


def _signals(rows):
    """어제 +SIGNAL%↑ 마감한 날 D의 인덱스 (익일 D+1 존재해야)."""
    out = []
    for i in range(1, len(rows) - 1):
        pc = rows[i - 1][3]
        if pc > 0 and (rows[i][3] / pc - 1) * 100 >= SIGNAL_PCT:
            out.append(i)
    return out


def _eval(rows, i):
    """신호일 D(i) → 익일 D+1 측정. D종가 기준 + 시가매수 현실 손익."""
    d_close = rows[i][3]
    o1, h1, l1, c1, _ = rows[i + 1]
    target = d_close * (1 + TARGET_PCT / 100)

    touched = h1 >= target                       # ① 엔진 적중: 익일 +10% 터치
    gap = (o1 / d_close - 1) * 100                # 익일 시가 갭%
    close_ret = (c1 / d_close - 1) * 100          # D종가→D+1종가 보유

    # ② 현실 단타: D+1 시가 매수 → 목표가 진입 위 + 도달이면 목표청산, 아니면 종가청산 (비용 차감)
    if h1 >= target and target >= o1:
        trade = (target / o1 - 1) - COST_LEG      # 목표를 앞으로 잡음
        hit_tradeable = True
    else:
        trade = (c1 / o1 - 1) - COST_LEG          # 종가 청산 (목표가 진입 아래=갭으로 지나침, or 미도달)
        hit_tradeable = False                     # 시가매수로 +10% 목표를 못 잡음
    return {"touched": touched, "gap": gap, "close_ret": close_ret,
            "trade": trade * 100, "hit_tradeable": hit_tradeable,
            "gap_ate": (h1 >= target) and (o1 > target)}  # 터치했으나 갭이 목표 먹음


def _report(evals):
    n = len(evals)
    if n == 0:
        print("신호 0건"); return 0
    touched = sum(1 for e in evals if e["touched"])
    avg_gap = sum(e["gap"] for e in evals) / n
    avg_close = sum(e["close_ret"] for e in evals) / n
    avg_trade = sum(e["trade"] for e in evals) / n
    wins = [e["trade"] for e in evals if e["trade"] > 0]
    losses = [e["trade"] for e in evals if e["trade"] <= 0]
    wr = len(wins) / n * 100
    payoff = (sum(wins) / len(wins)) / abs(sum(losses) / len(losses)) if wins and losses else float("nan")

    print(f"상한가 엔진 적중 백테스트 — 어제 +{SIGNAL_PCT:.0f}%↑ → 익일 +{TARGET_PCT:.0f}%")
    print(f"신호 {n:,}건 (비용 {COST_LEG*100:.2f}%/leg)\n")
    print("① 엔진 주장 (낙관 — 익일 고점이 +10% 터치):")
    print(f"   터치 적중률 {touched/n*100:.1f}%  ← 사장님 '잘 맞췄어'가 이것일 것\n")
    gap_ate = sum(1 for e in evals if e.get("gap_ate"))
    print("② 현실 단타 (D+1 시가 매수 — 갭 먹고 진입):")
    print(f"   익일 평균 갭: {avg_gap:+.2f}%  (이만큼 먹고 시작 → +10%까지 여유 축소)")
    print(f"   갭이 +10% 목표를 먹어 시가매수로 못 잡은 비율: {gap_ate/n*100:.1f}%")
    print(f"   D종가→D+1종가 보유: {avg_close:+.2f}%")
    print(f"   시가매수 실현(목표/종가청산, 비용후): {avg_trade:+.2f}%  승률 {wr:.0f}%  손익비 {payoff:.2f}")
    print()
    print("해석: ①터치율 높아도 ②갭이 크면 시가매수 실현은 깎임(갭이 적중을 먹음).")
    print("      손익비>1 & 시가매수 실현>0 이어야 단타 엣지 실재. 그게 사장님 관찰의 진짜 검증.")
    print("★ 일봉·생존편향 한계 → 분포·상대·손익비로만 신뢰(절대수익 X).")
    return 0


def _selftest() -> int:
    """합성 — 신호탐지·갭·적중·현실손익 로직 검증(데이터 불필요)."""
    # 종목X: idx2서 +20%(신호) → idx3 갭+5% 시가, 고점 +12%(목표터치), 종가 +8%
    # entry=D종가 100*1.20=120. D+1: open=126(+5%),high=134.4(+12%),close=129.6(+8%),low=125
    rows = [(100, 100, 100, 100, 1000),     # idx0
            (100, 100, 100, 100, 1000),     # idx1 (prev)
            (100, 121, 100, 120, 3000),     # idx2 신호일 +20%
            (126, 134.4, 125, 129.6, 2000), # idx3 D+1
            (130, 131, 129, 130, 1500)]     # idx4 (꼬리 — D+1 존재 보장용)
    sig = _signals(rows)
    checks = []
    checks.append(("신호 1건(idx2)", sig == [2]))
    e = _eval(rows, 2)
    target = 120 * 1.10  # 132
    checks.append(("익일 +10% 터치 인식", e["touched"] is True))          # high 134.4 >= 132
    checks.append(("갭 +5% 계산", abs(e["gap"] - 5.0) < 1e-6))            # 126/120-1
    # 현실: open120*1.05=126, target132>=126 & high134.4>=132 → 목표청산 132/126-1=4.76% -0.48%
    expect_trade = (132 / 126 - 1) * 100 - COST_LEG * 100
    checks.append(("시가매수 목표청산 손익", abs(e["trade"] - expect_trade) < 1e-6))
    # 갭이 목표 초과한 케이스: open이 target 위면 못 잡음 → 종가청산
    rows2 = [(100,100,100,100,1000),(100,100,100,100,1000),
             (100,121,100,120,3000),(140,145,138,142,2000),(142,143,141,142,1000)]
    e2 = _eval(rows2, 2)  # open140 > target132 → hit_tradeable False, trade=종가청산
    checks.append(("시가>목표면 못잡음", e2["hit_tradeable"] is False and e2["touched"] is True))

    ok = sum(1 for _, c in checks if c)
    for name, c in checks:
        print(f"  [{'PASS' if c else 'FAIL'}] {name}")
    print(f"\n셀프테스트: {ok}/{len(checks)} PASS")
    return 0 if ok == len(checks) else 1


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", default=str(DAILY_DEFAULT))
    ap.add_argument("--selftest", action="store_true")
    args = ap.parse_args()
    if args.selftest:
        return _selftest()
    files = glob.glob(str(Path(args.data) / "*.csv"))
    if not files:
        print(f"[!] 데이터 없음: {args.data} (노트북 stock_data_daily). 로직검증=--selftest", file=sys.stderr)
        return 2
    evals = []
    for f in files:
        rows = _load(f)
        if len(rows) >= 3:
            evals.extend(_eval(rows, i) for i in _signals(rows))
    return _report(evals)


if __name__ == "__main__":
    raise SystemExit(main())
