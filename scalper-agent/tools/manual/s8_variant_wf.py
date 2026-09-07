# -*- coding: utf-8 -*-
"""[S-8 재검] 외인/기관 수급 조건 변형의 walk-forward 리프트 재측정 (read-only).

★ 왜 만들었나 (9/7 사장님 "순차적으로 작업 진행하자" ②)
  [S-8]("외인 3일 순매수 ∩ 기관 3일 순매도")은 8/21 walk-forward A/B 두 구간 리프트 +0.41/+0.42로 등재됐다.
  관측 8일(8/21~9/1)에서 리프트 **−0.29**(양수일 3/8)인데, 등재 시 `control`로 지정한
  **대조군(외인 3일 순매수 단독)은 +0.18(양수일 7/8)** — 기관 매도 조건이 리프트를 깎는 방향이다.
  8일치로 규칙을 바꾸면 in-sample 튜닝이다(8/21 게이트 기각 교훈). 그래서 **같은 구간 규약(A/B)** 에
  관측 구간 C를 더해 대조군·동반매수·기관 단독 변형을 **정식으로** 다시 잰다.

★ 방법론 — `tools/manual/lift_screen.py`(8/21 [T-1]) 그대로
  1) 동일일 대조군: 그날 유니버스 전체 T+3 평균을 빼서 날 효과 제거
  2) look-ahead 차단: 피처는 D 종가까지, 진입은 D+1 시가, 청산 D+3 종가
  3) 구간: A 3/18~5/29 · B 6/1~8/11 (등재 시 구간 그대로) · C 8/12~forward 완성 최종일 (관측 구간)
  4) 판정선: walk-forward 통과 = A·B 같은 부호 + 양쪽 |t|≥1.0 (lift_screen --wf 규약) ·
     후보 승격은 전 구간 t>2 (T_PASS) — 낮은 문턱은 소표본 노이즈를 전략으로 승격시킨다(7/28)
  5) cap5: 하루 5건(코드순 — s8_observe와 동일 규약·왕복 비용 0.2%p) 건당 net — 실행 규칙 층
  6) 통과 0건이면 "0건"이라고 보고한다 — 억지 후보 금지

★ 안전: read-only. 주문·매수·매도·picks·SAJANG·대장(strategy_deadlines.json) 무접촉. 파일 쓰기 없음(--json 지정 시에만).
★ 이 도구는 판정기가 아니다 — [S-8] 10/2 판정은 대장 criteria(두 관문·150건)로 별도.

실행(VPS):  cd ~/bodyhunter/scalper-agent && venv/bin/python3.11 -X utf8 tools/manual/s8_variant_wf.py
"""
from __future__ import annotations

import argparse
import json
import math
import statistics as st
import sys
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent.parent   # scalper-agent/
if str(BASE) not in sys.path:
    sys.path.insert(0, str(BASE))

from tools.manual.lift_screen import load_all, fwd, _streak, _f   # noqa: E402  정본 재사용(로컬 복제 금지 [F-37])

HORIZON = 3
COST = 0.2          # 왕복 비용 %p (s8_observe·⑲-3 규약)
CAP = 5
T_PASS = 2.0
MIN_MKT = 100       # 그날 시장 대조군 최소 종목 수 (lift_screen 규약)

WINDOWS = {
    "A": ("2026-03-18", "2026-05-29"),   # 등재 시 A
    "B": ("2026-06-01", "2026-08-11"),   # 등재 시 B
    "C": ("2026-08-12", None),           # 관측 구간 (None = forward 완성 최종일까지)
}


def _inst_today(S, d, sign):
    row = S["inv"].get(d)
    v = _f(row, "기관_수량") if row else None
    return None if v is None else (v * sign > 0)


RULES = {
    "S-8 외인3매수 ∩ 기관3매도":     lambda S, d: _streak(S, d, "외국인_수량", 3, 1) and _streak(S, d, "기관_수량", 3, -1),
    "대조군 외인3매수 단독":          lambda S, d: _streak(S, d, "외국인_수량", 3, 1),
    "외인3매수 ∩ 기관3매수(동반)":   lambda S, d: _streak(S, d, "외국인_수량", 3, 1) and _streak(S, d, "기관_수량", 3, 1),
    "외인3매수 ∩ 기관 당일매도":     lambda S, d: _streak(S, d, "외국인_수량", 3, 1) and (_inst_today(S, d, -1) is True),
    "외인3매수 ∩ 기관 당일매수":     lambda S, d: _streak(S, d, "외국인_수량", 3, 1) and (_inst_today(S, d, 1) is True),
    "기관3매도 단독":                 lambda S, d: _streak(S, d, "기관_수량", 3, -1),
    "기관3매수 단독":                 lambda S, d: _streak(S, d, "기관_수량", 3, 1),
}


def evaluate(data: dict, days: list[str]) -> dict:
    """규칙별 {lift, t, days, n, pos_days, cap_n, cap_avg, cap_sum}. lift_screen.screen()과 같은 집계."""
    fwd_cache = {}
    for d in days:
        row = {}
        for c, S in data.items():
            v = fwd(S, d, HORIZON)
            if v is not None:
                row[c] = v
        fwd_cache[d] = row
    out = {}
    for name, fn in RULES.items():
        lifts, npick, cap = [], 0, []
        for d in days:
            mk = fwd_cache.get(d) or {}
            if len(mk) < MIN_MKT:
                continue
            dmean = st.mean(mk.values())
            sel = []
            for c, v in mk.items():
                try:
                    hit = fn(data[c], d)
                except Exception:                    # noqa: BLE001
                    hit = None
                if hit:
                    sel.append(c)
            if not sel:
                continue
            lifts.append(st.mean(mk[c] for c in sel) - dmean)
            npick += len(sel)
            cap += [mk[c] - COST for c in sorted(sel)[:CAP]]
        if len(lifts) < 2:
            out[name] = None
            continue
        m = st.mean(lifts)
        sd = st.pstdev(lifts)
        t = m / (sd / math.sqrt(len(lifts))) if sd > 0 else 0.0
        out[name] = {
            "lift": m, "t": t, "days": len(lifts), "n": npick,
            "pos_days": sum(1 for x in lifts if x > 0),
            "cap_n": len(cap), "cap_avg": (sum(cap) / len(cap)) if cap else None,
            "cap_sum": sum(cap) if cap else None,
        }
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description="[S-8 재검] 수급 조건 변형 walk-forward (read-only)")
    ap.add_argument("--json", default=None, help="결과 JSON 저장 경로 (지정 시에만 씀)")
    args = ap.parse_args()

    from data.data_verifier import _all_universe_codes
    codes = _all_universe_codes()
    print(f"[S-8 재검] 유니버스 {len(codes)}종 로드 중…", flush=True)
    data = load_all(codes)
    ref = max(data.values(), key=lambda S: len(S["bars"]))
    all_days = [b[0] for b in ref["bars"]]
    last_ok = all_days[-(HORIZON + 1)]           # forward 완성 최종일 (D+3 종가 존재)
    print(f"     일봉 보유 {len(data)}종 · forward 완성 최종일 {last_ok}\n", flush=True)

    results = {}
    for w, (s0, s1) in WINDOWS.items():
        s1 = s1 or last_ok
        days = [d for d in all_days if s0 <= d <= min(s1, last_ok)]
        print(f"[{w}] {days[0] if days else '-'} ~ {days[-1] if days else '-'} ({len(days)}일)", flush=True)
        results[w] = evaluate(data, days) if days else {}

    print()
    hdr = f"{'규칙':30s}" + "".join(f" | {w} 리프트  t   +일  cap5/건" for w in WINDOWS) + "  | 판정"
    print(hdr)
    print("─" * len(hdr))
    for name in RULES:
        cells, ra, rb = [], results["A"].get(name), results["B"].get(name)
        for w in WINDOWS:
            r = results[w].get(name)
            if not r:
                cells.append(f" | {'—':>7s} {'—':>5s} {'—':>5s} {'—':>8s}")
                continue
            cap = f"{r['cap_avg']:+.2f}" if r["cap_avg"] is not None else "—"
            cells.append(f" | {r['lift']:+6.2f}%p {r['t']:+5.2f} {r['pos_days']:>2d}/{r['days']:<2d} {cap:>7s}")
        if ra and rb:
            same = (ra["lift"] > 0) == (rb["lift"] > 0)
            both = min(abs(ra["t"]), abs(rb["t"])) >= 1.0
            wf = "✅WF일관" if same and both else ("⚠방향만" if same else "❌비일관")
            strong = "★t>2" if min(ra["t"], rb["t"]) > T_PASS else ""
        else:
            wf, strong = "판정불가", ""
        print(f"{name:30s}" + "".join(cells) + f"  | {wf} {strong}")
    print()
    print("★ WF일관 = A·B 같은 부호 + 양쪽 |t|≥1.0 (8/21 규약) · ★t>2 = 양 구간 모두 t>2 (후보 승격선)")
    print("★ C = 관측 구간(8/12~) — 표본이 작아 참고만. 판정은 대장 criteria(두 관문·150건)로 10/2에.")
    print("★ cap5/건 = 하루 코드순 5건 net(비용 0.2%p) — 실행 규칙 층. 리프트(+)여도 cap5(−)면 '실행 규칙 미해결'.")

    if args.json:
        Path(args.json).write_text(json.dumps(results, ensure_ascii=False, indent=1), encoding="utf-8")
        print(f"\n💾 {args.json}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
