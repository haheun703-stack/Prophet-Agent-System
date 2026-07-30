# -*- coding: utf-8 -*-
"""F-21 확장 판정자료 — 상한 구조(슬롯 수) 자금 재활용 시뮬 (7/30).

배경: 7/28~30 7규칙 기각 수렴 결론 = "전신호 +0.173인데 어떤 '5건 고르기'도
마이너스 → 엣지는 폭에 있다". 남은 질문 = 폭을 실제 자금으로 담을 수 있는가.

시뮬 (선택규칙 0·look-ahead 0):
- 사장님 자금룰 유지: 현금 30% 보유 · 가용 70%를 N슬롯 균등 분할(split_cash 동형)
- 슬롯 점유 [entry_time, exit_time) — 청산되면 슬롯 반환, 다음 신호 선착순 재사용
- 신호가 왔는데 빈 슬롯 없으면 스킵(실제와 동일)
- 일별 P&L% = Σ net%×(0.70/N) — 복리 없음(일 독립·보수)
- N 그리드: {5, 10, 20, 30, 50}

읽기전용. 실행(VPS): python3.11 -X utf8 f21_cap_structure_sim.py
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
N_GRID = [5, 10, 20, 30, 50]


def _sim_exit_with_time(rows, ei, entry):
    ret, why, ex_t = pb._simulate_exit(rows, ei, entry)
    if ret is None:
        return None
    return ret, why, ex_t


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="/tmp/f21_out.json")
    args = ap.parse_args()

    all_days = sorted(d.name for d in pb.TICKS.iterdir()
                      if d.is_dir() and pb._is_trading_day_dir(d.name))
    ok_days = [d for d in all_days
               if (h := pb.ticks_health(d)) and h["verdict"] == "OK"]
    print(f"[f21] 유효(OK) {len(ok_days)}일")

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
            r = _sim_exit_with_time(rows, sig + 1, rows[sig + 1][1])
            if r is None:
                continue
            ret, why, ex_t = r
            by_day.setdefault(day, []).append(
                {"e_t": rows[sig + 1][0], "x_t": ex_t or "15:30:00",
                 "net": ret - COST})

    report = {"days_ok": len(ok_days), "n_grid": N_GRID, "variants": {}}
    print(f"\n{'N슬롯':>5} {'체결':>6} {'스킵':>6} {'승률':>6} {'건당net':>8} "
          f"{'일평균P&L%':>10} {'누적P&L%':>9} {'흑자일':>7}")
    for N in N_GRID:
        w = 0.70 / N                      # 슬롯당 총자본 대비 비중
        day_pnl = {}
        taken = skipped = 0
        rets = []
        for day in sorted(by_day):
            slots = []                    # 점유 중인 exit_time 목록
            pnl = 0.0
            for t in sorted(by_day[day], key=lambda x: x["e_t"]):
                slots = [x for x in slots if x > t["e_t"]]   # 청산분 반환
                if len(slots) < N:
                    slots.append(t["x_t"])
                    pnl += t["net"] * w
                    rets.append(t["net"])
                    taken += 1
                else:
                    skipped += 1
            day_pnl[day] = pnl
        vals = list(day_pnl.values())
        pos_days = sum(1 for v in vals if v > 0)
        win = 100 * sum(1 for r in rets if r > 0) / max(len(rets), 1)
        avg_daily = sum(vals) / max(len(vals), 1)
        report["variants"][f"N{N}"] = {
            "taken": taken, "skipped": skipped, "win_pct": round(win, 1),
            "avg_net_per_trade": round(sum(rets) / max(len(rets), 1), 3),
            "avg_daily_pnl_pct": round(avg_daily, 3),
            "sum_pnl_pct": round(sum(vals), 1),
            "pos_days": pos_days, "days": len(vals),
            "day_pnl": {k: round(v, 3) for k, v in day_pnl.items()}}
        print(f"{N:>5} {taken:>6} {skipped:>6} {win:>5.1f}% "
              f"{sum(rets)/max(len(rets),1):>8.3f} {avg_daily:>10.3f} "
              f"{sum(vals):>9.1f} {pos_days:>4}/{len(vals)}")

    # 3분할 WF (일평균 P&L% per 구간)
    print("\n[3분할 WF] (일평균 P&L% per 구간)")
    n = len(ok_days)
    cuts = [ok_days[:n // 3], ok_days[n // 3:2 * n // 3], ok_days[2 * n // 3:]]
    for N in N_GRID:
        dp = report["variants"][f"N{N}"]["day_pnl"]
        segs = []
        for c in cuts:
            v = [dp[d] for d in c if d in dp]
            segs.append(f"{sum(v)/len(v):>7.3f}" if v else "      —")
        print(f"  N{N:<4} {' '.join(segs)}")

    Path(args.out).write_text(json.dumps(report, ensure_ascii=False, indent=1),
                              encoding="utf-8")
    print(f"\n산출: {args.out}")


if __name__ == "__main__":
    main()
