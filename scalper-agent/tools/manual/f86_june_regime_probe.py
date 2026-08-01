"""[F-86] 6월 규명 — S-1 엣지가 6월에만 존재한 이유를 찾는다 (read-only).

■ 배경 ([F-83] 8/1)
  baseline 건당 net 월별: 4월 +0.316 / 5월 -0.399 / **6월 +1.601(승률 71.2%)**
  / 7월 +0.002. 전체 2,666건 +0.241%p는 **6월 하나가 만든 수치**다.
  6월은 KOSPI -3.55% / KOSDAQ -12.75% 하락장이었으므로 시장 베타도 아니다.
  → 6월을 설명하는 '사전 관측 가능한' 조건이 있는가? 없으면 S-1은 우연이다.

■ ★기각 조건 (실행 前 고정 — 8/1 사장님 승인)
  레짐 축은 이미 두 번 기각됐다([F-28] 신호밀도 스피어만 0.145 /
  7/10 레짐 게이트는 PB-A에 역효과). 세 번째 시도는 사후 해석이 되기 가장
  쉬운 자리이므로 아래를 **먼저** 못 박고 들어간다.
    1. 변수는 **장전 또는 신호 시점에 관측 가능**해야 한다.
       → 전부 D-1 값으로 정의. 당일 종가·마감 집계 사용 시 자동 기각.
    2. 후보 목록을 **실행 前에 고정**하고, 돌린 뒤 새 변수를 추가하지 않는다.
    3. 6월에서 얻은 임계를 **4·5·7월에 적용**해 부호가 유지돼야 한다
       (in-sample만 좋으면 기각).
    4. 3분위에서 **단조성**을 확인한다. 6월만 튀는 이분법이면 기각.

■ 후보 변수 (실행 前 고정·이후 추가 금지)
    V1 시장 변동성   : D-1 전 종목 등락률 표준편차
    V2 급등 종목 수  : D-1 등락률 >= 15% 종목 수
    V3 체결강도      : D-1 v2 신호들의 신호시점 강도 중앙값
    V4 시장 방향     : D-1 전 종목 등락률 중앙값
    V5 ticks 품질    : 당일 usable_pct  ※예측 변수가 아니라 **무효화 검사**
                      (6월 성과가 데이터 품질 차이에서 온 착시인지 확인)
  ⚠ 원안의 '섹터 집중도'는 **실행 前 제외**한다 — sector_history.json이
    2026-07-01부터 31일치뿐이라 4·5·6월 out-of-sample(조건 3)이 불가능하다.
    성적을 보고 뺀 것이 아니라 데이터 부재로 뺀 것임을 명시한다.

■ 단일진실: 신호·진입·청산 판정은 r1_pullback_entry_backtest에서 import.
■ 불변식: read-only·매매 무접촉·봇 OFF.

실행(VPS): python3.11 -X utf8 tools/manual/f86_june_regime_probe.py
"""
from __future__ import annotations

import argparse
import csv
import statistics as st
import sys
from pathlib import Path

_HERE = Path(__file__).resolve().parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))

import r1_pullback_entry_backtest as r1  # noqa: E402  판정 단일진실

pb = r1.pb
DS = Path(__file__).resolve().parents[2] / "data_store"
SURGE = 15.0        # V2 급등 기준(%) — 실행 前 고정


def _daily_market() -> dict[str, dict]:
    """daily CSV 1회 스캔 → {YYYYMMDD: {std, surge, med}}  (V1·V2·V4)."""
    by_date: dict[str, list[float]] = {}
    ddir = DS / "daily"
    for f in ddir.glob("*.csv"):
        try:
            with open(f, encoding="utf-8-sig", errors="replace") as fh:
                for row in csv.DictReader(fh):
                    d = (row.get("날짜") or "").strip().replace("-", "")
                    if len(d) != 8:
                        continue
                    try:
                        cr = float(row.get("등락률") or 0)
                    except ValueError:
                        continue
                    by_date.setdefault(d, []).append(cr)
        except OSError:
            continue
    out = {}
    for d, v in by_date.items():
        if len(v) < 50:
            continue
        out[d] = {"std": st.pstdev(v), "surge": sum(1 for x in v if x >= SURGE),
                  "med": st.median(v), "n": len(v)}
    return out


def _day_ticks(day: str) -> tuple[list[float], list[float]]:
    """그날 v2 신호의 (net 리스트, 신호시점 강도 리스트) — ticks 1회 순회."""
    ddir = pb.TICKS / day
    if not ddir.is_dir():
        return [], []
    nets, strs = [], []
    for f in ddir.glob("*.csv"):
        rows = pb._read_ticks(day, f.stem)
        if len(rows) < 3:
            continue
        sig = r1._signal_index(rows)
        if sig is None or not r1._is_v2(rows, sig):
            continue
        strs.append(rows[sig][3])
        eb = r1._entry_baseline(rows, sig)
        if not eb:
            continue
        tr = r1._trade(rows, eb[0], eb[1], eb[2], day, f.stem, sig)
        if tr:
            nets.append(tr["ret"] - pb.COST_RT)
    return nets, strs


def _tercile(pairs: list[tuple[float, float, int]], name: str) -> None:
    """(x, 건당net, 건수) → 3분위 단조성 검사 (조건 4)."""
    if len(pairs) < 9:
        print(f"  {name:<12} 표본 {len(pairs)}일 — 3분위 불가")
        return
    s = sorted(pairs, key=lambda p: p[0])
    k = len(s) // 3
    parts = [s[:k], s[k:2 * k], s[2 * k:]]
    outs = []
    for p in parts:
        tot_n = sum(n for _, _, n in p)
        tot_w = sum(a * n for _, a, n in p)
        outs.append(tot_w / tot_n if tot_n else 0)
    mono = (outs[0] < outs[1] < outs[2]) or (outs[0] > outs[1] > outs[2])
    rng = f"{s[0][0]:.2f}~{s[-1][0]:.2f}"
    print(f"  {name:<12} [{rng:>16}] "
          f"{outs[0]:+7.3f} → {outs[1]:+7.3f} → {outs[2]:+7.3f}  "
          f"{'✅단조' if mono else '❌비단조(조건4 기각)'}")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.parse_args()

    print("=" * 78)
    print("[F-86] 6월 규명 — 사전 고정 기각조건 4개 / 변수 5개(섹터 제외)")
    print("=" * 78)

    mkt = _daily_market()
    all_days = sorted(d.name for d in pb.TICKS.iterdir()
                      if d.is_dir() and pb._is_trading_day_dir(d.name))
    ok_days = [d for d in all_days
               if (h := pb.ticks_health(d)) and h["verdict"] == "OK"]
    mkt_days = sorted(mkt)

    rec = {}
    for d in ok_days:
        nets, strs = _day_ticks(d)
        if not nets:
            continue
        prev = [x for x in mkt_days if x < d]
        if not prev:
            continue
        p = prev[-1]                       # D-1 = 직전 거래일
        h = pb.ticks_health(d) or {}
        rec[d] = {"n": len(nets), "avg": sum(nets) / len(nets),
                  "V1": mkt[p]["std"], "V2": mkt[p]["surge"],
                  "V4": mkt[p]["med"], "V5": h.get("usable_pct", 0.0),
                  "_str": (st.median(strs) if strs else None), "prev": p}
    # V3 = D-1의 신호강도 중앙값 (당일 것이 아니라 직전 유효일 것)
    for d in rec:
        prevs = [x for x in sorted(rec) if x < d]
        rec[d]["V3"] = rec[prevs[-1]]["_str"] if prevs else None

    print(f"  유효일 {len(ok_days)}일 중 분석 대상 {len(rec)}일\n")

    # ── 6월 vs 나머지 ────────────────────────────────────────
    print("── 월별 변수 평균 (D-1 관측값) " + "─" * 45)
    print(f"  {'월':>7} {'일수':>4} {'건당net':>9} │ {'V1변동성':>9} "
          f"{'V2급등수':>8} {'V3강도':>8} {'V4방향':>8} {'V5품질':>7}")
    months = sorted({d[:6] for d in rec})
    for m in months:
        ds = [d for d in rec if d.startswith(m)]
        tot_n = sum(rec[d]["n"] for d in ds)
        w = sum(rec[d]["avg"] * rec[d]["n"] for d in ds) / tot_n if tot_n else 0
        v3 = [rec[d]["V3"] for d in ds if rec[d]["V3"] is not None]
        print(f"  {m:>7} {len(ds):>4} {w:+8.3f}%p │ "
              f"{st.mean([rec[d]['V1'] for d in ds]):>9.2f} "
              f"{st.mean([rec[d]['V2'] for d in ds]):>8.1f} "
              f"{(st.mean(v3) if v3 else 0):>8.1f} "
              f"{st.mean([rec[d]['V4'] for d in ds]):>+8.2f} "
              f"{st.mean([rec[d]['V5'] for d in ds]):>6.1f}%")

    # ── 조건 4: 3분위 단조성 ─────────────────────────────────
    print("\n── 조건4 검사: 3분위 단조성 (전 기간) " + "─" * 38)
    for v in ("V1", "V2", "V3", "V4", "V5"):
        pairs = [(rec[d][v], rec[d]["avg"], rec[d]["n"])
                 for d in rec if rec[d].get(v) is not None]
        _tercile(pairs, v)

    # ── 조건 3: out-of-sample ────────────────────────────────
    print("\n── ★조건3 검사: 6월 임계 → 4·5·7월 적용 " + "─" * 34)
    jun = [d for d in rec if d.startswith("202606")]
    oth = [d for d in rec if not d.startswith("202606")]
    if not jun or not oth:
        print("  6월 또는 대조군 표본 부족 — 검사 불가")
        return 0
    for v in ("V1", "V2", "V3", "V4"):
        jv = [rec[d][v] for d in jun if rec[d].get(v) is not None]
        if not jv:
            continue
        lo, hi = min(jv), max(jv)
        sel = [d for d in oth
               if rec[d].get(v) is not None and lo <= rec[d][v] <= hi]
        if not sel:
            print(f"  {v}: 6월 범위 [{lo:.2f},{hi:.2f}] — 대조군 해당일 0 → 검사불가")
            continue
        tn = sum(rec[d]["n"] for d in sel)
        tw = sum(rec[d]["avg"] * rec[d]["n"] for d in sel) / tn if tn else 0
        base_n = sum(rec[d]["n"] for d in oth)
        base = sum(rec[d]["avg"] * rec[d]["n"] for d in oth) / base_n
        ok = "✅부호유지" if tw > 0 else "❌기각"
        print(f"  {v}: 6월범위[{lo:>7.2f},{hi:>7.2f}] → 대조군 {len(sel)}일 "
              f"{tn}건 {tw:+.3f}%p (대조군 전체 {base:+.3f}%p) {ok}")

    print("\n  ※ 관측 전용. 조건 3·4를 모두 통과하지 못하면 6월은 '설명 불가'이며,")
    print("    그 경우 S-1은 8/29에 폐기가 정직한 결론이다(사전 선언).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
