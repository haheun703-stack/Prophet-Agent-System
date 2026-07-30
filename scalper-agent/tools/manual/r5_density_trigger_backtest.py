# -*- coding: utf-8 -*-
"""R-5 신호밀도 도달 트리거 백테스트 — F-26 차기 단일가설 (7/30 사전등록·오늘 마지막).

규칙: 당일 v2 신호가 시간순 누적 K건 나온 뒤(K건은 버림), K+1번째부터를
cap5 슬롯 후보로 삼는다. look-ahead 0 — 장중 누적 카운트만으로 실행 가능.
- 신호 <K건인 날 = 자동 관망 (해부 진단: 신호 적은 날이 나쁜 날)
- 신호 많은 날 = 초반 역선택 구간(순위 1~2 −0.87/−1.41) 스킵

정직 공시: 기계적으로 7/28 '앞N스킵'과 동일 규칙. 당시 페이퍼 41건 기각 →
백테스트 2,406건 + 7/30 해부 진단(8+ 순위 +0.295)을 근거로 정식 재검정.
K 그리드는 결과 확인 전 고정: {2,3,5,8,12,20} (인접컷용).

규약: ⑲ 미러링(r1/r4 도구와 동일 축) · 진입=신호 다음 관측행 실측가(즉시 진입
— 7/30 확정: 엣지는 즉시 진입에서만 삶) · 청산=_simulate_exit · OK일만 · net=COST_RT 차감.
판정 지표: cap5(K스킵 후 entry_time순 5건/일) — S-1 스코어보드 미러.
읽기전용. 실행(VPS): python3.11 -X utf8 r5_density_trigger_backtest.py
"""
import argparse
import json
import sys
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent.parent
for p in (BASE, BASE / "tools"):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

import playbook_shadow as pb  # noqa: E402

SIG_PCT, CUTOFF, STR_MIN, CHASE, COST = (pb.SIGNAL_PCT, pb.REFINED_CUTOFF,
                                         pb.REFINED_STRENGTH, pb.CHASE_MAX,
                                         pb.COST_RT)
K_GRID = [2, 3, 5, 8, 12, 20]   # 사전 고정 (결과 확인 전)
CAP_PER_DAY = 5


def _stats(trades):
    rets = [t["net"] for t in trades]
    if not rets:
        return {"n": 0}
    wins = sum(1 for r in rets if r > 0)
    return {"n": len(rets), "win_pct": round(100 * wins / len(rets), 1),
            "avg_net": round(sum(rets) / len(rets), 3),
            "sum_net": round(sum(rets), 1)}


def _split3(trades, days_sorted):
    n = len(days_sorted)
    cuts = [set(days_sorted[:n // 3]), set(days_sorted[n // 3:2 * n // 3]),
            set(days_sorted[2 * n // 3:])]
    return [_stats([t for t in trades if t["date"] in c]) for c in cuts]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="/tmp/r5_out.json")
    args = ap.parse_args()

    all_days = sorted(d.name for d in pb.TICKS.iterdir()
                      if d.is_dir() and pb._is_trading_day_dir(d.name))
    ok_days = [d for d in all_days
               if (h := pb.ticks_health(d)) and h["verdict"] == "OK"]
    print(f"[r5] 유효(OK) {len(ok_days)}일 / 대상 {len(all_days)}일")

    by_day = {}
    for day in ok_days:
        for f in (pb.TICKS / day).glob("*.csv"):
            rows = pb._read_ticks(day, f.stem)
            if len(rows) < 3:
                continue
            sig = next((i for i, (t, px, cr, st) in enumerate(rows)
                        if cr >= SIG_PCT and t <= CUTOFF), None)
            if sig is None or sig + 1 >= len(rows):
                continue
            t_, px_, cr_, st_ = rows[sig]
            if not (st_ >= STR_MIN and cr_ < CHASE):
                continue
            entry = rows[sig + 1][1]
            ret, why, _ = pb._simulate_exit(rows, sig + 1, entry)
            if ret is None:
                continue
            by_day.setdefault(day, []).append(
                {"date": day, "sig_t": t_, "e_t": rows[sig + 1][0],
                 "net": ret - COST})

    report = {"days_ok": len(ok_days), "k_grid": K_GRID, "variants": {}}
    print(f"\n{'K':>4} {'거래일':>5} {'건수':>5} {'승률':>6} {'avg_net':>8} "
          f"{'sum_net':>9} | 일평균 sum_net")
    for K in [0] + K_GRID:                     # K=0 = baseline cap5 앵커
        sel = []
        days_traded = 0
        for day in sorted(by_day):
            ts = sorted(by_day[day], key=lambda x: x["sig_t"])[K:]   # 앞 K건 버림
            if not ts:
                continue
            picks = sorted(ts, key=lambda x: x["e_t"])[:CAP_PER_DAY]
            if picks:
                days_traded += 1
                sel.extend(picks)
        s = _stats(sel)
        per_day = round(s.get("sum_net", 0) / max(days_traded, 1), 2)
        name = "cap5_K0(앵커)" if K == 0 else f"cap5_K{K}"
        report["variants"][name] = {"stats": s, "days_traded": days_traded,
                                    "per_day_sum": per_day,
                                    "split3": _split3(sel, ok_days)}
        if s.get("n"):
            print(f"{K:>4} {days_traded:>5} {s['n']:>5} {s['win_pct']:>5.1f}% "
                  f"{s['avg_net']:>8.3f} {s['sum_net']:>9.1f} | {per_day:+.2f}")
        else:
            print(f"{K:>4} 표본 0")

    print("\n[3분할 WF] (avg_net per 구간)")
    for name, v in report["variants"].items():
        sp = v["split3"]
        vals = [f"{x.get('avg_net', '—'):>7}" if x.get("n") else "      —" for x in sp]
        print(f"  {name:<14} {' '.join(vals)}")

    Path(args.out).write_text(json.dumps(report, ensure_ascii=False, indent=1),
                              encoding="utf-8")
    print(f"\n산출: {args.out}")


if __name__ == "__main__":
    main()
