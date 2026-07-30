# -*- coding: utf-8 -*-
"""R-4 되돌림 후 재돌파 진입 백테스트 — F-26 차기 단일가설 (7/30 사전등록).

가설: R-1(되돌림에서 매수=떨어지는 칼)은 역선택 심화로 기각(7/30).
정반대 방향 — 신호 후 p% 눌림이 나온 뒤 **신호가를 회복(reclaim)하는 순간** 진입.
"털고 다시 가는 놈"만 사는 구조 = 재진입 reclaim 컨셉(6/22)과 동형.

규약 (⑲ 미러링 — r1_pullback_entry_backtest와 동일 축):
- 신호: v2 정제판 (+5% 첫 도달·10:00前·강도 200+·추격<8%)
- 진입: 신호 후 관측가가 신호가×(1-p%) 이하로 눌림 확인(j행) → 이후 관측가가
        신호가 이상 회복한 첫 행(m행)에서 **m행 실측가** 진입(스톱매수 체결 보수 근사
        — 실측가 ≥ 신호가라 항상 같거나 불리한 체결). 진입 시각 14:30 이전만
        (SIGNAL_CUTOFF 재사용 — 청산 활주로 확보·튜닝 아님).
- 청산: ⑲ _simulate_exit 그대로 (SL 우선→TP→EOD)
- 유효일: ticks_health OK만

분해 진단 (선별 효과 vs 타이밍 효과):
- R-4 체결 표본과 동일 (day,code)의 baseline 성적을 병기 — R-4가 이기면
  "타이밍 기여", R-4 표본의 baseline이 이미 좋으면 "선별 기여".

과적합 3중 차단: ①p 그리드 인접컷 ②기간 3분할 WF ③비정제 대조군(p=2.0).

읽기전용. 실행(VPS): python3.11 -X utf8 r4_reclaim_entry_backtest.py --out /tmp/r4_out.json
"""
import argparse
import json
import sys
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent.parent   # scalper-agent/
for p in (BASE, BASE / "tools"):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

import playbook_shadow as pb  # noqa: E402

SIG_PCT = pb.SIGNAL_PCT
CUTOFF = pb.REFINED_CUTOFF
STR_MIN = pb.REFINED_STRENGTH
CHASE = pb.CHASE_MAX
COST = pb.COST_RT
ENTRY_CUTOFF = pb.SIGNAL_CUTOFF      # 14:30 — 진입(재돌파) 마감선

P_GRID = [1.0, 1.5, 2.0, 2.5, 3.0]   # 눌림 깊이 %
CAP_PER_DAY = 5


def _signal_index(rows):
    return next((i for i, (t, px, cr, st) in enumerate(rows)
                 if cr >= SIG_PCT and t <= CUTOFF), None)


def _is_v2(rows, sig):
    t, px, cr, st = rows[sig]
    return st >= STR_MIN and cr < CHASE


def _entry_reclaim(rows, sig, p):
    """눌림 -p% 확인 후 신호가 회복 첫 관측행 — (index, 실측 진입가, 시각)."""
    sig_px = rows[sig][1]
    dip = sig_px * (1 - p / 100.0)
    j = next((k for k in range(sig + 1, len(rows)) if rows[k][1] <= dip), None)
    if j is None:
        return None
    for m in range(j + 1, len(rows)):
        if rows[m][1] >= sig_px:
            if rows[m][0] > ENTRY_CUTOFF:
                return None
            return m, rows[m][1], rows[m][0]
    return None


def _trade(rows, ei, entry_px, entry_t, day, code, sig):
    ret, why, _ = pb._simulate_exit(rows, ei, entry_px)
    if ret is None:
        return None
    return {"date": day, "code": code, "signal_time": rows[sig][0],
            "entry_time": entry_t, "ret": round(ret, 2), "why": why}


def _stats(trades):
    rets = [t["ret"] for t in trades]
    if not rets:
        return {"n": 0}
    wins = sum(1 for r in rets if r > 0)
    avg = sum(rets) / len(rets)
    return {"n": len(rets), "win_pct": round(100 * wins / len(rets), 1),
            "avg_gross": round(avg, 3), "avg_net": round(avg - COST, 3),
            "sum_net": round(sum(r - COST for r in rets), 1)}


def _cap5(trades):
    by_day = {}
    for t in trades:
        by_day.setdefault(t["date"], []).append(t)
    sel = []
    for day in sorted(by_day):
        sel.extend(sorted(by_day[day], key=lambda x: x["entry_time"])[:CAP_PER_DAY])
    return _stats(sel)


def _split3(trades, days_sorted):
    n = len(days_sorted)
    cuts = [set(days_sorted[:n // 3]), set(days_sorted[n // 3:2 * n // 3]),
            set(days_sorted[2 * n // 3:])]
    return [_stats([t for t in trades if t["date"] in c]) for c in cuts]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="/tmp/r4_out.json")
    args = ap.parse_args()

    all_days = sorted(d.name for d in pb.TICKS.iterdir()
                      if d.is_dir() and pb._is_trading_day_dir(d.name))
    ok_days, dropped = [], []
    for d in all_days:
        h = pb.ticks_health(d)
        (ok_days if h and h["verdict"] == "OK" else dropped).append(d)
    print(f"[r4] 대상 {len(all_days)}일 → 유효(OK) {len(ok_days)}일 · 제외 {len(dropped)}일")

    baseline = []                          # v2 전 신호 선착순(앵커·분해 진단용)
    base_by_key = {}                       # (day,code) → baseline ret
    variants = {f"r4_p{p}": [] for p in P_GRID}
    ctrl = []                              # 비정제 신호 p=2.0 대조

    for day in ok_days:
        for f in (pb.TICKS / day).glob("*.csv"):
            rows = pb._read_ticks(day, f.stem)
            if len(rows) < 3:
                continue
            sig = _signal_index(rows)
            if sig is None:
                continue
            code = f.stem
            v2 = _is_v2(rows, sig)

            if sig + 1 < len(rows):
                tr = _trade(rows, sig + 1, rows[sig + 1][1], rows[sig + 1][0],
                            day, code, sig)
                if tr and v2:
                    baseline.append(tr)
                    base_by_key[(day, code)] = tr["ret"]

            if v2:
                for p_ in P_GRID:
                    er = _entry_reclaim(rows, sig, p_)
                    if er:
                        tr = _trade(rows, er[0], er[1], er[2], day, code, sig)
                        if tr:
                            variants[f"r4_p{p_}"].append(tr)
            else:
                er = _entry_reclaim(rows, sig, 2.0)
                if er:
                    tr = _trade(rows, er[0], er[1], er[2], day, code, sig)
                    if tr:
                        ctrl.append(tr)

    report = {"days_ok": len(ok_days), "days_dropped": dropped,
              "params": {"p_grid": P_GRID, "entry_cutoff": ENTRY_CUTOFF,
                         "cost_rt": COST, "cap": CAP_PER_DAY},
              "variants": {}, "baseline": _stats(baseline),
              "baseline_cap5": _cap5(baseline), "control_p2": _stats(ctrl)}

    b = report["baseline"]
    print(f"\nbaseline(v2 선착순 앵커): {b['n']}건 승{b['win_pct']}% "
          f"net {b['avg_net']:+.3f} | cap5 {report['baseline_cap5'].get('avg_net'):+.3f}")

    print(f"\n{'변형':<10} {'건수':>5} {'승률':>6} {'avg_net':>8} {'sum_net':>9} "
          f"{'동표본 baseline':>14} | cap5: {'건':>4} {'승률':>6} {'avg_net':>8}")
    for name, trades in variants.items():
        s, c = _stats(trades), _cap5(trades)
        # 분해 진단 — 같은 (day,code) 신호를 선착순으로 샀다면?
        subset_base = [base_by_key[(t["date"], t["code"])] for t in trades
                       if (t["date"], t["code"]) in base_by_key]
        sb_avg = (round(sum(subset_base) / len(subset_base) - COST, 3)
                  if subset_base else None)
        report["variants"][name] = {"all": s, "cap5": c,
                                    "subset_baseline_avg_net": sb_avg,
                                    "split3": _split3(trades, ok_days)}
        if s.get("n"):
            print(f"{name:<10} {s['n']:>5} {s['win_pct']:>5.1f}% {s['avg_net']:>8.3f} "
                  f"{s['sum_net']:>9.1f} {str(sb_avg):>14} | "
                  f"{c.get('n', 0):>6} {c.get('win_pct', 0):>5.1f}% "
                  f"{c.get('avg_net', 0):>8.3f}")
        else:
            print(f"{name:<10} 표본 0")

    print("\n[3분할 WF] (avg_net per 구간)")
    for name in report["variants"]:
        sp = report["variants"][name]["split3"]
        vals = [f"{x.get('avg_net', '—'):>7}" if x.get("n") else "      —" for x in sp]
        print(f"  {name:<10} {' '.join(vals)}")

    c = report["control_p2"]
    if c.get("n"):
        print(f"\n[대조군 — 비정제 신호 p=2.0] {c['n']}건 승{c['win_pct']}% net {c['avg_net']:+.3f}")

    Path(args.out).write_text(json.dumps(report, ensure_ascii=False, indent=1),
                              encoding="utf-8")
    print(f"\n산출: {args.out}")


if __name__ == "__main__":
    main()
