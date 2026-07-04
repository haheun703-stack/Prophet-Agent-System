# -*- coding: utf-8 -*-
"""market_breadth — 6/8 3묶음③ 시장 폭넓이 + 거래대금 집중 (시장 컨텍스트, read-only).

사장님 6/7: "지수만 보면 착시(삼성전자 몇 개가 지수 끌어올림)." → 전 종목 breadth가 체감장을
정확히 본다. 코스피/코스닥 지수 일봉 시계열이 없으므로(ETF만 존재, 프록시는 부정확) 지수 MA 대신
전 종목 폭넓이로 시장 환경을 기록 — 이게 오히려 더 실전적(사장님 1번 우선순위 의도와 일치).

★ per-candidate 아님 — ledger market_context 1블록(그날 시장 상태, 후보마다 안 붙음). hard gate 0.
  6/12에 B/C 성과를 "장이 받쳐준 날 vs 약한 날"로 해석하는 좌표축. 매수조건 미개입.
★ read-only: 일봉 csv만 읽음. 실주문 0 / 주문함수 0 / SAJANG·scheduler 미참조.
★ 지수 MA20/60/120 = 지수 일봉 데이터 확보 후 6/12+ (현재 보류).
"""
from __future__ import annotations

import os
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
DAILY_DIR = ROOT / "stock_data_daily"

LIMIT_PCT = 29.0       # 상한가/하한가 근사 (한국 ±30%, 종가 ±29%↑로 카운트)
STRONG_PCT = 3.0       # ±3% 강세/약세 종목
YEAR_N = 252           # 52주 신고가/신저가


def market_breadth(c2p, asof, load_daily, find_index_asof):
    """전 종목 일봉으로 그날 시장 폭넓이 + 거래대금 집중. 반환 dict(시장 컨텍스트).
    load_daily(path)·find_index_asof(d, asof)는 호출처에서 주입(순수·테스트 용이)."""
    up = down = flat = 0
    up3 = down3 = lu = ld = nh = nl = 0
    turnovers = []                          # (code, name, 거래대금_억)
    scanned = 0
    for code, p in c2p.items():
        try:
            d = load_daily(p)
        except Exception:                   # noqa: BLE001 - 깨진 csv는 건너뜀(시장집계, 개별 무관)
            continue
        i = find_index_asof(d, asof)
        if i is None or i < 1:
            continue
        c, pc = d[i][4], d[i - 1][4]
        if c <= 0 or pc <= 0:
            continue
        scanned += 1
        chg = (c / pc - 1) * 100
        if chg > 0.05:
            up += 1
        elif chg < -0.05:
            down += 1
        else:
            flat += 1
        if chg >= STRONG_PCT:
            up3 += 1
        if chg <= -STRONG_PCT:
            down3 += 1
        if chg >= LIMIT_PCT:
            lu += 1
        if chg <= -LIMIT_PCT:
            ld += 1
        win = d[max(0, i - YEAR_N + 1):i + 1]
        if c >= max(x[2] for x in win):
            nh += 1
        if c <= min(x[3] for x in win):
            nl += 1
        name = os.path.basename(p)[:-4].rsplit("_", 1)[0]
        turnovers.append((code, name, c * d[i][5] / 1e8))
    total = up + down + flat
    breadth_pct = (up / total) if total else None
    adr = (up / down) if down else None
    turnovers.sort(key=lambda x: -x[2])
    total_turn = sum(t[2] for t in turnovers)
    top = turnovers[:10]
    top10_share = (sum(t[2] for t in top) / total_turn) if total_turn else None
    state = ("BROAD_UP" if breadth_pct is not None and breadth_pct >= 0.6
             else "BROAD_DOWN" if breadth_pct is not None and breadth_pct <= 0.4
             else "MIXED" if breadth_pct is not None else None)
    return {
        "asof": asof, "scanned": scanned,
        "advancers": up, "decliners": down, "unchanged": flat,
        "adv_dec_ratio": round(adr, 2) if adr else None,
        "breadth_pct": round(breadth_pct, 3) if breadth_pct is not None else None,
        "breadth_state": state,
        "up_3pct": up3, "down_3pct": down3, "limit_up": lu, "limit_down": ld,
        "new_high_52w": nh, "new_low_52w": nl,
        "turnover_top10": [{"code": cd, "name": nm, "억": round(t, 1)} for cd, nm, t in top],
        "turnover_top10_share": round(top10_share, 3) if top10_share is not None else None,
        "total_turnover_억": round(total_turn, 1),
    }


# ─────────────────────────────── selftest (합성 주입, 네트워크 0) ───────────────────────────────
def selftest():
    ok = []
    synth = {
        "AAA": [("2026-06-03", 100, 100, 100, 100, 1000), ("2026-06-04", 100, 106, 100, 105, 2000)],  # +5%
        "BBB": [("2026-06-03", 100, 100, 100, 100, 1000), ("2026-06-04", 100, 100, 94, 95, 1000)],     # -5%
        "CCC": [("2026-06-03", 100, 100, 100, 100, 1000), ("2026-06-04", 100, 131, 100, 130, 5000)],   # +30%
        "DDD": [("2026-06-03", 100, 100, 100, 100, 1000), ("2026-06-04", 100, 101, 99, 100, 1000)],     # flat
    }
    fake_load = lambda p: synth[p]                                            # noqa: E731
    fake_idx = lambda d, a: next((j for j in range(len(d) - 1, -1, -1) if d[j][0] <= a), None)  # noqa: E731
    c2p = {k: k for k in synth}
    r = market_breadth(c2p, "2026-06-04", fake_load, fake_idx)
    ok.append(("T1 advancers=2(AAA,CCC) decliners=1(BBB) flat=1(DDD)",
               r["advancers"] == 2 and r["decliners"] == 1 and r["unchanged"] == 1))
    ok.append(("T2 up3=2 down3=1 limit_up=1(CCC +30%)",
               r["up_3pct"] == 2 and r["down_3pct"] == 1 and r["limit_up"] == 1))
    ok.append(("T3 breadth_pct=0.5 → MIXED", r["breadth_pct"] == 0.5 and r["breadth_state"] == "MIXED"))
    ok.append(("T4 거래대금 top1=CCC(최대)", r["turnover_top10"][0]["code"] == "CCC"))
    ok.append(("T5 top10_share=1.0(4종목<10)", r["turnover_top10_share"] == 1.0))
    # 빈 c2p → 안전(None)
    r0 = market_breadth({}, "2026-06-04", fake_load, fake_idx)
    ok.append(("T6 빈 시장 → scanned 0 · breadth None",
               r0["scanned"] == 0 and r0["breadth_pct"] is None and r0["breadth_state"] is None))
    # broad up: 3/4 상승
    synth2 = dict(synth); synth2["DDD"] = [("2026-06-03", 100, 100, 100, 100, 1), ("2026-06-04", 100, 105, 100, 104, 1)]
    r2 = market_breadth({k: k for k in synth2}, "2026-06-04", lambda p: synth2[p], fake_idx)
    ok.append(("T7 3/4 상승 → BROAD_UP", r2["advancers"] == 3 and r2["breadth_state"] == "BROAD_UP"))
    print("market_breadth 셀프테스트:")
    for nme, p in ok:
        print(f"  [{'PASS' if p else 'FAIL'}] {nme}")
    return all(p for _, p in ok)


if __name__ == "__main__":
    import sys
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    sys.exit(0 if selftest() else 1)
