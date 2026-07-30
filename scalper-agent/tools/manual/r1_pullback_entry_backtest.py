# -*- coding: utf-8 -*-
"""R-1 되돌림 후 진입 백테스트 — S-1 실행규칙 재설계 1순위 (7/28 사장님 결정·F-26).

문제(7/28 규명): v2 전 신호 건당 +엣지인데 "먼저 나온 것부터 산다"(선착순)가
역선택으로 파괴. 오늘 축 = "언제 사는가": 신호 즉시가 아니라 되돌림을 기다렸다 진입.

규약 (⑲ playbook_shadow 미러링 — 새 시뮬 규약 작성 금지·7/16 교훈):
- 신호: v2 정제판 = 장중 등락률 +5% 첫 도달행 · 10:00前 · 체결강도 200+ · 추격<8%
- 진입 (변형별):
  * baseline  : 신호 다음 관측행 실측가 (⑲ 그대로 — 대조 기준선)
  * R-1 k%    : 신호행 가격 × (1-k%) 지정가 — 이후 관측행이 임계 이하로 처음
                내려온 시점 체결. 체결가 = 임계가(지정가 체결·보수: 실측가가 더
                낮아도 임계가로 잡음). 체결행 자체가 SL 이하면 그 행 실측가로 즉시 SL
                (체결 후 같은 관측창에서 추가 하락한 경로 — 낙관 차단).
  * R-3 N분   : 신호 후 N분 지나 첫 관측행 실측가 (시장가 지연 진입)
- 청산: ⑲ _simulate_exit 그대로 (SL 우선 → TP → EOD·경로 가정 0)
- 유효일: ticks_health() OK만 (BROKEN/TRUNCATED 제외 — 제외 일수 명시 출력)
- 비용: COST_RT 0.2%p/건 왕복 차감 = net (의사결정 축)

과적합 3중 차단 (7/28 규약 유지):
1) 인접컷: k 그리드 이웃 부호 일치 확인
2) 기간 3분할: OK일 시간순 3등분 각각 net 부호
3) 대조군: 비정제 신호(강도<200 or 10시後)에 같은 R-1 적용 — 리프트 문맥 확인

F-28 동시 산출: 일별 v2 신호수 ↔ 당일 baseline 건당 net 상관 (순위상관·3분위)

읽기전용 — ticks/원장 어떤 파일도 쓰지 않음. 산출물은 --out JSON뿐.
실행(VPS): python3.11 -X utf8 r1_pullback_entry_backtest.py --out /tmp/r1_out.json
"""
import argparse
import json
import sys
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent.parent   # scalper-agent/
for p in (BASE, BASE / "tools"):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

import playbook_shadow as pb  # noqa: E402  — ⑲ 규약 단일진실 재사용

SIG_PCT = pb.SIGNAL_PCT              # +5%
CUTOFF = pb.REFINED_CUTOFF           # 10:00:00
STR_MIN = pb.REFINED_STRENGTH        # 200
CHASE = pb.CHASE_MAX                 # 8.0
COST = pb.COST_RT                    # 0.2
TP, SL = pb.TP_PCT, pb.SL_PCT

K_GRID = [1.0, 1.5, 2.0, 2.5, 3.0, 4.0]     # R-1 되돌림 폭 (인접컷용 촘촘 그리드)
N_GRID = [10, 20, 30]                        # R-3 지연 분
CAP_PER_DAY = 5                              # S-1 판정 지표와 동일 상한


def _signal_index(rows):
    """v2 신호행 index — +5% 첫 도달·10시前. (강도·추격은 호출부에서 분리 판정)"""
    return next((i for i, (t, px, cr, st) in enumerate(rows)
                 if cr >= SIG_PCT and t <= CUTOFF), None)


def _is_v2(rows, sig):
    t, px, cr, st = rows[sig]
    return st >= STR_MIN and cr < CHASE


def _entry_baseline(rows, sig):
    if sig + 1 >= len(rows):
        return None
    return sig + 1, rows[sig + 1][1], rows[sig + 1][0]


def _entry_pullback(rows, sig, k):
    thresh = rows[sig][1] * (1 - k / 100.0)
    for j in range(sig + 1, len(rows)):
        if rows[j][1] <= thresh:
            return j, thresh, rows[j][0]     # 지정가 체결 = 임계가(보수)
    return None


def _entry_delay(rows, sig, n_min):
    st = rows[sig][0]
    h, m, s = int(st[:2]), int(st[3:5]), int(st[6:8])
    tot = h * 3600 + m * 60 + s + n_min * 60
    tgt = f"{tot // 3600:02d}:{(tot % 3600) // 60:02d}:{tot % 60:02d}"
    for j in range(sig + 1, len(rows)):
        if rows[j][0] >= tgt:
            return j, rows[j][1], rows[j][0]
    return None


def _trade(rows, ei, entry_px, entry_t, day, code, sig):
    # 체결행 자체가 이미 SL 이하로 내려간 경우 — 그 행 실측가 즉시 SL(낙관 차단)
    if rows[ei][1] <= entry_px * (1 + SL / 100):
        ret = (rows[ei][1] / entry_px - 1) * 100
        return {"date": day, "code": code, "signal_time": rows[sig][0],
                "entry_time": entry_t, "ret": round(ret, 2), "why": "SL"}
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
    """일별 entry_time 시간순 앞 5건 체결(실행 가능 상한) — S-1 판정 지표 미러."""
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
    return [_stats([t for t in trades if t["date"] in c])
            for c in cuts]


def _rank_corr(xs, ys):
    """스피어만 순위상관 (동률=평균순위 근사 없이 단순순위 — 참고 지표)."""
    if len(xs) < 3:
        return None
    def ranks(v):
        order = sorted(range(len(v)), key=lambda i: v[i])
        r = [0] * len(v)
        for rank, i in enumerate(order):
            r[i] = rank
        return r
    rx, ry = ranks(xs), ranks(ys)
    n = len(xs)
    d2 = sum((a - b) ** 2 for a, b in zip(rx, ry))
    return round(1 - 6 * d2 / (n * (n ** 2 - 1)), 3)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="/tmp/r1_out.json")
    ap.add_argument("--days-limit", type=int, default=0, help="최근 N일만(0=전체)")
    args = ap.parse_args()

    all_days = sorted(d.name for d in pb.TICKS.iterdir()
                      if d.is_dir() and pb._is_trading_day_dir(d.name))
    if args.days_limit:
        all_days = all_days[-args.days_limit:]
    ok_days, dropped = [], []
    for d in all_days:
        h = pb.ticks_health(d)
        (ok_days if h and h["verdict"] == "OK" else dropped).append(d)
    print(f"[r1] 대상 {len(all_days)}일 → 유효(OK) {len(ok_days)}일 · 제외 {len(dropped)}일"
          f" (BROKEN/TRUNCATED — 무표본 가드)")

    variants = {"baseline": []}
    for k in K_GRID:
        variants[f"r1_k{k}"] = []
    for n in N_GRID:
        variants[f"r3_n{n}"] = []
    control = {"baseline": [], "r1_best_ctrl": []}   # 비정제 대조(문맥 확인)
    day_sig_count = {}

    for day in ok_days:
        ddir = pb.TICKS / day
        n_v2 = 0
        for f in ddir.glob("*.csv"):
            rows = pb._read_ticks(day, f.stem)
            if len(rows) < 3:
                continue
            sig = _signal_index(rows)
            if sig is None:
                continue
            code = f.stem
            v2 = _is_v2(rows, sig)
            if v2:
                n_v2 += 1
            tgt = variants if v2 else None

            eb = _entry_baseline(rows, sig)
            if eb:
                tr = _trade(rows, eb[0], eb[1], eb[2], day, code, sig)
                if tr:
                    (variants["baseline"] if v2 else control["baseline"]).append(tr)
            if v2:
                for k in K_GRID:
                    ep = _entry_pullback(rows, sig, k)
                    if ep:
                        tr = _trade(rows, ep[0], ep[1], ep[2], day, code, sig)
                        if tr:
                            variants[f"r1_k{k}"].append(tr)
                for n in N_GRID:
                    ed = _entry_delay(rows, sig, n)
                    if ed:
                        tr = _trade(rows, ed[0], ed[1], ed[2], day, code, sig)
                        if tr:
                            variants[f"r3_n{n}"].append(tr)
            else:
                ep = _entry_pullback(rows, sig, 2.0)   # 대조군은 대표 k=2.0 고정
                if ep:
                    tr = _trade(rows, ep[0], ep[1], ep[2], day, code, sig)
                    if tr:
                        control["r1_best_ctrl"].append(tr)
        day_sig_count[day] = n_v2

    report = {"days_total": len(all_days), "days_ok": len(ok_days),
              "days_dropped": dropped, "params": {
                  "tp": TP, "sl": SL, "cutoff": CUTOFF, "strength": STR_MIN,
                  "chase": CHASE, "cost_rt": COST, "cap": CAP_PER_DAY},
              "variants": {}, "control": {}, "f28": {}}

    print(f"\n{'변형':<12} {'건수':>6} {'승률':>6} {'avg_g':>7} {'avg_net':>8} "
          f"{'sum_net':>9} | cap5: {'건':>4} {'승률':>6} {'avg_net':>8} {'sum_net':>9}")
    for name, trades in variants.items():
        s, c = _stats(trades), _cap5(trades)
        report["variants"][name] = {"all": s, "cap5": c,
                                    "split3": _split3(trades, ok_days)}
        if s.get("n"):
            print(f"{name:<12} {s['n']:>6} {s['win_pct']:>5.1f}% {s['avg_gross']:>7.3f} "
                  f"{s['avg_net']:>8.3f} {s['sum_net']:>9.1f} | "
                  f"{c.get('n', 0):>6} {c.get('win_pct', 0):>5.1f}% "
                  f"{c.get('avg_net', 0):>8.3f} {c.get('sum_net', 0):>9.1f}")
        else:
            print(f"{name:<12} 표본 0")

    for name, trades in control.items():
        report["control"][name] = _stats(trades)

    # 기간 3분할 출력
    print("\n[3분할 WF] (avg_net per 구간)")
    for name in report["variants"]:
        sp = report["variants"][name]["split3"]
        vals = [f"{x.get('avg_net', '—'):>7}" if x.get("n") else "      —" for x in sp]
        print(f"  {name:<12} {' '.join(vals)}")

    # F-28: 신호밀도 ↔ 당일 성적
    days_with = [d for d in ok_days if day_sig_count.get(d, 0) > 0]
    xs, ys = [], []
    by_day_base = {}
    for t in variants["baseline"]:
        by_day_base.setdefault(t["date"], []).append(t["ret"] - COST)
    for d in days_with:
        if d in by_day_base:
            xs.append(day_sig_count[d])
            ys.append(sum(by_day_base[d]) / len(by_day_base[d]))
    rho = _rank_corr(xs, ys)
    terc = sorted(zip(xs, ys))
    n3 = len(terc) // 3
    buckets = [terc[:n3], terc[n3:2 * n3], terc[2 * n3:]] if n3 else []
    f28 = {"days": len(xs), "spearman": rho,
           "terciles": [{"days": len(b),
                         "sig_range": [b[0][0], b[-1][0]] if b else None,
                         "avg_net_per_trade": round(sum(y for _, y in b) / len(b), 3)
                         if b else None} for b in buckets]}
    report["f28"] = {**f28, "day_detail": {d: {"n_sig": day_sig_count[d],
                     "avg_net": round(sum(by_day_base[d]) / len(by_day_base[d]), 3)}
                     for d in days_with if d in by_day_base}}
    print(f"\n[F-28] 신호밀도↔당일 건당 net: {len(xs)}일 · 스피어만 {rho}")
    for i, b in enumerate(f28["terciles"]):
        print(f"  {i + 1}분위(신호 {b['sig_range']}): {b['days']}일 "
              f"건당 net {b['avg_net_per_trade']}")

    Path(args.out).write_text(json.dumps(report, ensure_ascii=False, indent=1),
                              encoding="utf-8")
    print(f"\n산출: {args.out}")


if __name__ == "__main__":
    main()
