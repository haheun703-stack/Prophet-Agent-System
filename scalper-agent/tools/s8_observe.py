# -*- coding: utf-8 -*-
"""[S-8] 외인 매수 ∩ 기관 매도 — 관측치 계산 (단일진실).

★ 왜 만들었나 (8/29)
  8/21에 [S-8]을 등재하면서 *"일봉 종가 신호라 사후 재구성 100% 가능하니 관측 인프라 추가 0"*
  이라고 판단했다. 그런데 그 결과 **8일간 아무 기록도 남지 않았고**, 아무도 보고 있지 않았다.
  10/2 판정까지 6주를 그렇게 두면 **그 6주가 통째로 blind**다.
  "사후에 계산할 수 있다"와 "지금 보고 있다"는 다르다 — 7/17 사장님 대장의 취지는 후자다.

★ 설계
  - **새 cron·새 스텝 0** — `strategy_deadline_check`(이미 nightly ⑲-4 + 08:30 등록)가 호출한다.
  - 비용 실측 **3.5초**(일봉 2.7 + 수급 0.9, 2,531종). 체커 예산 안.
  - **graceful** — 실패하면 None을 돌려주고 호출자는 기존 동작 유지(nightly 차단 금지).
  - **정직** — forward 미완성일은 '측정 불가'로 세고 미달로 접지 않는다([F-106] 계열).

★ 판정 기준 (data/strategy_deadlines.json [S-8] criteria와 동일)
  ① 전 신호 T+3 리프트 > 0 (시장 동일가중·동일일 대조)
  ② 하루 5건 규칙의 건당 net > 0
  최소 표본 150건. 둘 다 통과해야 생존.

★ 안전: read-only. 주문·매수·매도·picks·SAJANG 무접촉.
"""
from __future__ import annotations

import csv
import statistics as st
import sys
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent
if str(BASE) not in sys.path:
    sys.path.insert(0, str(BASE))

START = "2026-08-21"      # [S-8] 등재일
CAP = 5                   # 일일 상한 (사장님 7/23 확정)
COST = 0.2                # 왕복 비용 %p (⑲-3 장부와 동일 규약)
HORIZON = 3               # T+3 (대장 criteria)
MIN_SAMPLE = 150          # 대장 min_sample


def _load():
    from data.data_verifier import DAILY_DIR, _all_universe_codes
    store = DAILY_DIR.parent
    bars, idx, inv = {}, {}, {}
    for c in _all_universe_codes():
        p = DAILY_DIR / f"{c}.csv"
        if not p.exists():
            continue
        rows = []
        try:
            with open(p, encoding="utf-8") as f:
                for r in csv.DictReader(f):
                    try:
                        rows.append((r["날짜"][:10], float(r["시가"]), float(r["종가"])))
                    except (KeyError, TypeError, ValueError):
                        continue
        except OSError:
            continue
        if len(rows) < 30:
            continue
        bars[c] = rows
        idx[c] = {d: i for i, (d, *_) in enumerate(rows)}
        m = {}
        fp = store / "flow" / f"{c}_investor.csv"
        if fp.exists():
            try:
                with open(fp, encoding="utf-8") as f:
                    for r in csv.DictReader(f):
                        if r.get("date"):
                            m[r["date"][:10]] = r
            except OSError:
                pass
        inv[c] = m
    return bars, idx, inv


def _num(row, key):
    try:
        v = row.get(key)
        return float(v) if v not in (None, "") else None
    except (AttributeError, TypeError, ValueError):
        return None


def _streak(bars, idx, inv, c, d, key, n, sign):
    i = idx[c].get(d)
    if i is None or i < n:
        return False
    for j in range(n):
        v = _num(inv[c].get(bars[c][i - j][0]) or {}, key)
        if v is None or v * sign <= 0:
            return False
    return True


def _fwd(bars, idx, c, d, k):
    i = idx[c].get(d)
    if i is None or i + k >= len(bars[c]):
        return None
    entry = bars[c][i + 1][1]          # D+1 시가 (look-ahead 차단)
    return (bars[c][i + k][2] - entry) / entry * 100 if entry > 0 else None


def observe() -> dict | None:
    """[S-8] 등재 후 누적 관측. 실패 시 None(호출자는 기존 동작 유지)."""
    try:
        bars, idx, inv = _load()
        if not bars:
            return None
        ref = max(bars.values(), key=len)
        days = [b[0] for b in ref if b[0] >= START]
        if not days:
            return None

        total_sig = 0
        lifts, cap_nets = [], []
        done, pending = 0, 0
        for d in days:
            sel = [c for c in bars
                   if _streak(bars, idx, inv, c, d, "외국인_수량", 3, 1)
                   and _streak(bars, idx, inv, c, d, "기관_수량", 3, -1)]
            total_sig += len(sel)
            sig_r = [x for x in (_fwd(bars, idx, c, d, HORIZON) for c in sel) if x is not None]
            mkt_r = [x for x in (_fwd(bars, idx, c, d, HORIZON) for c in bars) if x is not None]
            if not sig_r or not mkt_r:
                pending += 1               # forward 미완성 — 측정 불가(미달 아님)
                continue
            done += 1
            lifts.append(st.mean(sig_r) - st.mean(mkt_r))
            for c in sorted(sel)[:CAP]:
                v = _fwd(bars, idx, c, d, HORIZON)
                if v is not None:
                    cap_nets.append(v - COST)

        return {
            "start": START,
            "obs_days": len(days),
            "done_days": done,
            "pending_days": pending,
            "signals": total_sig,
            "lift": round(st.mean(lifts), 3) if lifts else None,
            "cap_n": len(cap_nets),
            "cap_sum": round(sum(cap_nets), 2) if cap_nets else None,
            "cap_avg": round(sum(cap_nets) / len(cap_nets), 3) if cap_nets else None,
            "min_sample": MIN_SAMPLE,
        }
    except Exception:                      # noqa: BLE001 — nightly 차단 금지
        return None


def summary_line(o: dict | None) -> str:
    """스코어보드 한 줄. o가 None이면 그 사실을 그대로 말한다(조용한 성공 위장 금지)."""
    if not o:
        return "🔭 [S-8] 관측: 계산 실패 — 수동 확인 필요"
    if not o["signals"]:
        return f"🔭 [S-8] 관측({o['start']}~): 신호 0건 — 신호 소멸 여부 확인 필요"
    lift = f"{o['lift']:+.2f}%p" if o["lift"] is not None else "미완성"
    if o["cap_n"]:
        cap = f"{o['cap_n']}건 {o['cap_sum']:+.2f}%p(건당 {o['cap_avg']:+.3f})"
    else:
        cap = "표본 0"
    gate = "표본 부족" if o["cap_n"] < o["min_sample"] else "표본 충족"
    return (f"🔭 [S-8] 관측({o['start']}~): 신호 {o['signals']}건 · 완성 {o['done_days']}일"
            f"(미완성 {o['pending_days']}) · ①리프트 {lift} · ②cap5 {cap} "
            f"· {gate}({o['cap_n']}/{o['min_sample']})")


if __name__ == "__main__":
    import json
    r = observe()
    print(summary_line(r))
    print(json.dumps(r, ensure_ascii=False, indent=1))
