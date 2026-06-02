# -*- coding: utf-8 -*-
"""leader_supply_enrich — 수급 결합 #1 첫 테스트 (6/2, read-only).

전수 스캔/정밀화: 기계적 leader-ride median 음수(꼬리복권) → 선택(수급)이 진짜 레버 가설.
이 도구: leader 후보를 ★외인/기관 수급★으로 분류 → median이 양으로 끌리는가 검증.

데이터: data_store/flow/{code}_investor.csv (외인/기관/개인/기타법인 금액, 2026-02-04~06-02 78일).
  ★ walk-forward: 수급은 T0 종가 EOD 보고 → 진입(T0종가)시 T-4..T0 누적만 사용(미래 미사용).
  ★ 한계: 외인/기관 '4주체'만 = ★세력(금융투자) 라인은 키움 opt10059 영역(미보유)★.
    내 5/31 발견(수급동반<수급없음, 정보시차)이 4주체 crude 부호 탓일 수 있음 → 금액·magnitude로 재검.

★ 순수 read-only: 실주문 0 / SAJANG·scheduler·systemd 무변경 / 봇 OFF.
사용: python tools/leader_supply_enrich_6_2.py [--exit ma10] [--limit N] [--selftest]
"""
from __future__ import annotations

import argparse
import csv
import glob
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
SA = Path(__file__).resolve().parent.parent
TOOLS = Path(__file__).resolve().parent
DAILY_DIR = ROOT / "stock_data_daily"
FLOW_DIR = SA / "data_store" / "flow"
sys.path.insert(0, str(SA)); sys.path.insert(0, str(TOOLS))

from data.sajang_rules import SAJANG  # noqa: E402
from leader_prospective_scan_6_2 import detect, load_daily, fwd_metrics, _is_excluded, STEADY_HIGH_N  # noqa: E402
from leader_exit_refine_6_2 import exit_variant  # noqa: E402

SUP_LOOKBACK = 5          # T-4..T0 누적 수급
LEADER_MFE = 20.0


def load_flow(code):
    """{date: (frgn_amt, inst_amt)} — 외국인_금액(idx6)/기관_금액(idx7)."""
    p = FLOW_DIR / f"{code}_investor.csv"
    if not p.exists():
        return None
    out = {}
    with open(p, encoding="utf-8") as f:
        rd = list(csv.reader(f))
    for r in rd[1:]:
        try:
            d = r[0][:10].replace(".", "-").replace("/", "-")
            out[d] = (float(r[6]), float(r[7]))
        except (ValueError, IndexError):
            pass
    return out


def supply_class(flow, dates, i):
    """T-4..T0 누적 외인/기관 부호 → 클래스 + magnitude."""
    fc = ic = 0.0
    got = 0
    for k in range(max(0, i - SUP_LOOKBACK + 1), i + 1):
        d = dates[k]
        if d in flow:
            fc += flow[d][0]; ic += flow[d][1]; got += 1
    if got == 0:
        return None, 0.0
    if fc > 0 and ic > 0:
        cls = "dual_buy"
    elif fc > 0 or ic > 0:
        cls = "single_buy"
    elif fc < 0 and ic < 0:
        cls = "dual_sell"
    else:
        cls = "neutral"
    return cls, fc + ic


def _median(x):
    if not x:
        return 0.0
    s = sorted(x); n = len(s)
    return round(s[n // 2] if n % 2 else (s[n // 2 - 1] + s[n // 2]) / 2, 2)


def _avg(x):
    return round(sum(x) / len(x), 2) if x else 0.0


def run(exit_kind, limit):
    dpaths = {}
    for f in glob.glob(str(DAILY_DIR / "*.csv")):
        dpaths[os.path.basename(f)[:-4].split("_")[-1]] = f
    flow_codes = [os.path.basename(p)[:-len("_investor.csv")]
                  for p in glob.glob(str(FLOW_DIR / "*_investor.csv"))]
    if limit:
        flow_codes = flow_codes[:limit]

    # 수급클래스 × 점화타입 집계
    acc = {}
    overall = {}
    for code in flow_codes:
        dp = dpaths.get(code) or dpaths.get(code.zfill(6))
        if not dp:
            continue
        name = os.path.basename(dp)[:-4].rsplit("_", 1)[0]
        if _is_excluded(name):
            continue
        d = load_daily(dp)
        if len(d) < STEADY_HIGH_N + 25:
            continue
        flow = load_flow(code)
        if not flow:
            continue
        dates = [x[0] for x in d]
        for i in range(STEADY_HIGH_N, len(d) - 21):
            if dates[i] not in flow:           # 수급 있는 기간만
                continue
            t = detect(d, i)
            if not t:
                continue
            cls, mag = supply_class(flow, dates, i)
            if cls is None:
                continue
            fm = fwd_metrics(d, i)
            if not fm:
                continue
            net, hold = exit_variant(d, i, exit_kind)
            lead = 1 if fm["mfe"] >= LEADER_MFE else 0
            for key in ((t, cls), (t, "ALL"), ("ALL", cls), ("ALL", "ALL")):
                a = acc.setdefault(key, {"net": [], "lead": 0, "n": 0})
                a["net"].append(net); a["lead"] += lead; a["n"] += 1
    return acc


def selftest():
    ok = []
    flow = {"2026-02-10": (100.0, 50.0), "2026-02-11": (-20.0, 10.0)}
    dates = ["2026-02-09", "2026-02-10", "2026-02-11"]
    c, m = supply_class(flow, dates, 2)        # T-1..T1: 외 80, 기 60 둘다+ → dual_buy
    ok.append(("T1 dual_buy", c == "dual_buy" and m == 140.0))
    flow2 = {"2026-02-11": (-30.0, -10.0)}
    c2, _ = supply_class(flow2, ["x", "y", "2026-02-11"], 2)
    ok.append(("T2 dual_sell", c2 == "dual_sell"))
    ok.append(("T3 median", _median([3, 1, 2]) == 2))
    ok.append(("T4 flow none", supply_class({}, ["a"], 0) == (None, 0.0)))
    print("supply enrich 셀프테스트:")
    for n, p in ok:
        print(f"  [{'PASS' if p else 'FAIL'}] {n}")
    return all(p for _, p in ok)


def main():
    sys.stdout.reconfigure(encoding="utf-8")
    ap = argparse.ArgumentParser()
    ap.add_argument("--exit", default="ma10", help="청산 변형(ma10/swinglow5 등, leader_exit_refine)")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--selftest", action="store_true")
    a = ap.parse_args()
    if a.selftest:
        sys.exit(0 if selftest() else 1)

    acc = run(a.exit, a.limit)
    print("=" * 92)
    print(f"leader_supply_enrich — 수급결합 #1 (외인/기관 4주체, 2026-02~06, 청산={a.exit})")
    print(f"★ 질문: 수급이 음수 median을 양으로 끌어올리는가  /  ★세력(금융투자)=키움 미보유, 4주체 crude")
    print(f"★ 증빙: SAJANG.AUTO_TRADE_DISABLED={SAJANG.AUTO_TRADE_DISABLED} / 실주문 0 / SAJANG·scheduler·systemd 무변경")
    print("=" * 92)
    for t in ("SURGE", "STEADY", "ALL"):
        print(f"\n■ {t}")
        print(f"  {'수급클래스':<14}{'건수':>6}{'median%':>9}{'mean%':>8}{'precision%':>11}{'승률%':>7}")
        for cls in ("dual_buy", "single_buy", "neutral", "dual_sell", "ALL"):
            a2 = acc.get((t, cls))
            if not a2 or a2["n"] == 0:
                continue
            nets = a2["net"]
            prec = 100 * a2["lead"] / a2["n"]
            wr = 100 * sum(1 for x in nets if x > 0) / len(nets)
            star = " ★median+" if _median(nets) > 0 else ""
            print(f"  {cls:<14}{a2['n']:>6}{_median(nets):>9}{_avg(nets):>8}{prec:>11.0f}{wr:>7.0f}{star}")
    print("\n★★ 판정: 어떤 수급클래스든 median 양전환되면 = 수급이 레버(키움 세력 추가시 더↑ 기대).")
    print("   전부 음수면 = 외인/기관 4주체로는 부족 → ★세력(금융투자)=키움 opt10059 필요★ (5/31 수급동반<없음 재확인).")
    print("   정직: 생존편향·4주체 crude·일봉. 절대치 X, 클래스간 상대·median 부호만.")


if __name__ == "__main__":
    main()
