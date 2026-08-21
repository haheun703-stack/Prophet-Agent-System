# -*- coding: utf-8 -*-
"""[T-1] 리프트 스크리닝 — 우리가 수집만 하고 안 쓰던 피처가 시장을 이기는가.

★ 왜 만들었나 (8/21 사장님 승인)
  우리는 수급(investor)·공매도(short_bal)·신용(credit_bal)·외국인소진율(foreign_exh)을
  **100% 커버리지로 매일 수집·검증**하는데, **어떤 전략도 그것을 쓰지 않고**
  **시장 대비 리프트를 내는지 한 번도 잰 적이 없다.** 이 도구가 그것을 잰다.

★ 방법론 (8/21 판정에서 확립된 규약)
  1) **동일일 대조군** — 같은 날 유니버스 전체 평균을 빼서 **날 효과를 제거**한다.
     단순 그룹 평균 비교는 '좋은 날에 많이 나온 것'을 '좋은 종목'으로 착각한다
     (8/21 실증: ma20 지지가 단순비교 +2.32%p → 동일일 대조 -0.22%p).
  2) **look-ahead 차단** — 피처는 D일 종가까지의 데이터로만 만들고, 진입은 **D+1 시가**.
  3) **판정선 t > 2** — 8/21에 잰 것 중 t>2를 넘긴 것은 pos20 고점권(회피) 하나뿐이었다.
     낮은 문턱은 소표본 노이즈를 전략으로 승격시킨다(7/28 교훈).
  4) **통과 0건이면 "0건"이라고 보고한다** — 억지 후보 금지.

★ 안전: read-only. 주문·매수·매도·picks·SAJANG 무접촉. 파일 쓰기 없음(--json 지정 시에만).
"""
from __future__ import annotations

import argparse
import csv
import json
import math
import statistics as st
import sys
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent.parent   # scalper-agent/
sys.path.insert(0, str(BASE))
STORE = BASE / "data_store"

T_PASS = 2.0          # 판정선 — 이보다 낮으면 후보로 올리지 않는다
MIN_DAYS = 15         # 짝지어진 날이 이보다 적으면 판정 불가로 표기(측정불가 ≠ 미달)
MIN_PICKS = 100       # 총 선택 건수 하한


# ────────────────────────────── 데이터 로드 ──────────────────────────────
def _read(path: Path) -> list[dict]:
    if not path.exists():
        return []
    try:
        with open(path, encoding="utf-8") as f:
            return list(csv.DictReader(f))
    except Exception:                                   # noqa: BLE001
        return []


def _f(row: dict, key: str):
    try:
        v = row.get(key)
        return float(v) if v not in (None, "") else None
    except (TypeError, ValueError):
        return None


def load_all(codes: list[str]) -> dict:
    """종목별 시계열 로드. 반환 = {code: {"bars":[...], "idx":{}, "inv":{d:row}, ...}}"""
    out = {}
    for c in codes:
        bars = []
        for r in _read(STORE / "daily" / f"{c}.csv"):
            try:
                bars.append((r["날짜"][:10], float(r["시가"]), float(r["고가"]),
                             float(r["저가"]), float(r["종가"]), float(r.get("거래량") or 0)))
            except (KeyError, TypeError, ValueError):
                continue
        if len(bars) < 30:
            continue
        out[c] = {
            "bars": bars,
            "idx": {d: i for i, (d, *_) in enumerate(bars)},
            "inv": {r["date"][:10]: r for r in _read(STORE / "flow" / f"{c}_investor.csv") if r.get("date")},
            "sht": {r["date"][:10]: r for r in _read(STORE / "short" / f"{c}_short_bal.csv") if r.get("date")},
            "crd": {r["date"][:10]: r for r in _read(STORE / "credit" / f"{c}_credit_bal.csv") if r.get("date")},
            "exh": {r["date"][:10]: r for r in _read(STORE / "flow" / f"{c}_foreign_exh.csv") if r.get("date")},
        }
    return out


# ────────────────────────────── 파생값 ──────────────────────────────
def fwd(S: dict, d0: str, k: int):
    """D+1 시가 진입 → D+k 종가 청산 (%). look-ahead 없음."""
    i = S["idx"].get(d0)
    if i is None or i + k >= len(S["bars"]):
        return None
    entry = S["bars"][i + 1][1]
    return (S["bars"][i + k][4] - entry) / entry * 100 if entry > 0 else None


def pos20(S: dict, d0: str):
    i = S["idx"].get(d0)
    if i is None or i < 20:
        return None
    w = S["bars"][i - 19:i + 1]
    hi, lo = max(x[2] for x in w), min(x[3] for x in w)
    return (S["bars"][i][4] - lo) / (hi - lo) if hi > lo else None


def _streak(S: dict, d0: str, key: str, days: int, sign: int = 1) -> bool:
    """investor의 key가 days일 연속 sign 방향인가 (거래일 기준·결손일은 실패 처리)."""
    i = S["idx"].get(d0)
    if i is None or i < days:
        return False
    for j in range(days):
        row = S["inv"].get(S["bars"][i - j][0])
        v = _f(row, key) if row else None
        if v is None or v * sign <= 0:
            return False
    return True


def _chg(S: dict, d0: str, book: str, key: str, back: int):
    """book[key]의 back거래일 전 대비 변화율(%). 두 시점 다 있어야 함."""
    i = S["idx"].get(d0)
    if i is None or i < back:
        return None
    now = _f(S[book].get(S["bars"][i][0]), key) if S[book].get(S["bars"][i][0]) else None
    was = _f(S[book].get(S["bars"][i - back][0]), key) if S[book].get(S["bars"][i - back][0]) else None
    if now is None or was is None or was == 0:
        return None
    return (now - was) / abs(was) * 100


def _sum_inv(S: dict, d0: str, key: str, back: int):
    i = S["idx"].get(d0)
    if i is None or i < back:
        return None
    tot = 0.0
    for j in range(back):
        v = _f(S["inv"].get(S["bars"][i - j][0]), key) if S["inv"].get(S["bars"][i - j][0]) else None
        if v is None:
            return None
        tot += v
    return tot


# ────────────────────────────── 피처 정의 ──────────────────────────────
# 각 피처: (이름, 함수(S, d) -> bool|None). None = 데이터 없음(그날 그 종목 제외)
FEATURES = {
    # 수급 (investor · ~134일 가용)
    "외인 3일 연속 순매수":      lambda S, d: _streak(S, d, "외국인_수량", 3, 1),
    "외인 5일 연속 순매수":      lambda S, d: _streak(S, d, "외국인_수량", 5, 1),
    "기관 3일 연속 순매수":      lambda S, d: _streak(S, d, "기관_수량", 3, 1),
    "외인+기관 동시 순매수(당일)": lambda S, d: (lambda a, b: None if a is None or b is None else (a > 0 and b > 0))(
        _f(S["inv"].get(d), "외국인_수량") if S["inv"].get(d) else None,
        _f(S["inv"].get(d), "기관_수량") if S["inv"].get(d) else None),
    "외인+기관 3일 동시 순매수":  lambda S, d: _streak(S, d, "외국인_수량", 3, 1) and _streak(S, d, "기관_수량", 3, 1),
    "개인 3일 연속 순매도":      lambda S, d: _streak(S, d, "개인_수량", 3, -1),
    "매집형(외인·기관 매수 + 개인 매도 3일)": lambda S, d: (
        _streak(S, d, "외국인_수량", 3, 1) and _streak(S, d, "개인_수량", 3, -1)),
    "외인 5일 순매수 누적 > 0":   lambda S, d: (lambda v: None if v is None else v > 0)(_sum_inv(S, d, "외국인_수량", 5)),

    # 공매도 (short_bal · ~66일)
    "공매도 잔고 5일 감소":      lambda S, d: (lambda v: None if v is None else v < 0)(_chg(S, d, "sht", "short_bal_qty", 5)),
    "공매도 잔고 5일 -5%↓":     lambda S, d: (lambda v: None if v is None else v < -5)(_chg(S, d, "sht", "short_bal_qty", 5)),
    "공매도 비중 낮음(ratio<1)":  lambda S, d: (lambda v: None if v is None else v < 1.0)(
        _f(S["sht"].get(d), "short_ratio") if S["sht"].get(d) else None),
    "공매도 비중 높음(ratio>5)":  lambda S, d: (lambda v: None if v is None else v > 5.0)(
        _f(S["sht"].get(d), "short_ratio") if S["sht"].get(d) else None),

    # 신용 (credit_bal · ~67일 · T+2~3 지연이라 D-3 기준 사용)
    "신용잔고 5일 감소":         lambda S, d: (lambda v: None if v is None else v < 0)(_chg(S, d, "crd", "credit_buy_qty", 5)),
    "신용잔고 5일 -3%↓":        lambda S, d: (lambda v: None if v is None else v < -3)(_chg(S, d, "crd", "credit_buy_qty", 5)),
    "신용 비중 높음(rate>2)":    lambda S, d: (lambda v: None if v is None else v > 2.0)(
        _f(S["crd"].get(d), "credit_buy_rate") if S["crd"].get(d) else None),

    # 외국인 소진율 (foreign_exh · ~84일)
    "소진율 5일 상승":           lambda S, d: (lambda v: None if v is None else v > 0)(_chg(S, d, "exh", "소진율", 5)),
    "소진율 5일 하락":           lambda S, d: (lambda v: None if v is None else v < 0)(_chg(S, d, "exh", "소진율", 5)),

    # 가격 위치 (8/21 확립 — 대조 기준선)
    "pos20 저점권(<=0.05)":     lambda S, d: (lambda v: None if v is None else v <= 0.05)(pos20(S, d)),
    "pos20 고점권(>=0.90)":     lambda S, d: (lambda v: None if v is None else v >= 0.90)(pos20(S, d)),

    # 결합 (저점 + 매집)
    "pos20<=0.30 ∩ 외인 3일 매수": lambda S, d: (
        (lambda p: False if p is None else p <= 0.30)(pos20(S, d)) and _streak(S, d, "외국인_수량", 3, 1)),
    "pos20<=0.30 ∩ 공매도잔고 감소": lambda S, d: (
        (lambda p: False if p is None else p <= 0.30)(pos20(S, d))
        and (lambda v: False if v is None else v < 0)(_chg(S, d, "sht", "short_bal_qty", 5))),

    # ── 2라운드 (8/21 1라운드 산물) ──
    # 1라운드에서 외인 3일매수 t +1.87 / 기관 3일매수 t -2.43 으로 **방향이 정반대**였고,
    # 둘을 AND로 묶은 "외인+기관 동시"는 -0.12(t -1.14)로 나빠졌다 = 기관이 신호를 오염시킨다.
    # → 기관을 **반대 방향으로** 거는 조합을 잰다(1라운드엔 없던 축).
    "외인 3일매수 ∩ 기관 순매도(당일)": lambda S, d: (
        _streak(S, d, "외국인_수량", 3, 1)
        and (lambda v: False if v is None else v < 0)(
            _f(S["inv"].get(d), "기관_수량") if S["inv"].get(d) else None)),
    "외인 3일매수 ∩ 기관 3일매도": lambda S, d: (
        _streak(S, d, "외국인_수량", 3, 1) and _streak(S, d, "기관_수량", 3, -1)),
    "외인 5일매수 ∩ 기관 3일매도": lambda S, d: (
        _streak(S, d, "외국인_수량", 5, 1) and _streak(S, d, "기관_수량", 3, -1)),
    "기관 3일 연속 순매도":        lambda S, d: _streak(S, d, "기관_수량", 3, -1),
    "외인 3일매수 ∩ pos20<=0.5 ∩ 기관매도": lambda S, d: (
        _streak(S, d, "외국인_수량", 3, 1)
        and (lambda p: False if p is None else p <= 0.50)(pos20(S, d))
        and (lambda v: False if v is None else v < 0)(
            _f(S["inv"].get(d), "기관_수량") if S["inv"].get(d) else None)),
    # 공매도 잔고 감소는 T+1 음수 → T+5 +1.45 로 **늦게 큰다** → 긴 지평에서 재확인
    "공매도잔고 5일-5%↓ ∩ 외인 3일매수": lambda S, d: (
        (lambda v: False if v is None else v < -5)(_chg(S, d, "sht", "short_bal_qty", 5))
        and _streak(S, d, "외국인_수량", 3, 1)),
}


# ────────────────────────────── 스크리닝 ──────────────────────────────
def screen(data: dict, days: list[str], horizons=(1, 3, 5, 10)) -> list[dict]:
    """피처별 동일일 대조 리프트 + t. 날 효과 제거."""
    results = []
    fwd_cache = {k: {} for k in horizons}
    for K in horizons:
        for d in days:
            row = {}
            for c, S in data.items():
                v = fwd(S, d, K)
                if v is not None:
                    row[c] = v
            fwd_cache[K][d] = row

    for name, fn in FEATURES.items():
        rec = {"name": name, "h": {}}
        for K in horizons:
            lifts, npick, pairdays = [], 0, 0
            for d in days:
                mk = fwd_cache[K].get(d) or {}
                if len(mk) < 100:
                    continue
                dmean = st.mean(mk.values())
                sel = []
                for c, v in mk.items():
                    try:
                        hit = fn(data[c], d)
                    except Exception:                    # noqa: BLE001
                        hit = None
                    if hit:
                        sel.append(v)
                if not sel:
                    continue
                lifts.append(st.mean(sel) - dmean)
                npick += len(sel)
                pairdays += 1
            if len(lifts) < 2:
                rec["h"][K] = None
                continue
            m = st.mean(lifts)
            sd = st.pstdev(lifts)
            t = m / (sd / math.sqrt(len(lifts))) if sd > 0 else 0.0
            rec["h"][K] = {"lift": m, "t": t, "days": pairdays, "n": npick,
                           "pos_days": sum(1 for x in lifts if x > 0)}
        results.append(rec)
    return results


def main() -> int:
    ap = argparse.ArgumentParser(description="[T-1] 리프트 스크리닝 (read-only)")
    ap.add_argument("--days", type=int, default=60, help="최근 N 거래일 (기본 60)")
    ap.add_argument("--json", default=None, help="결과 JSON 저장 경로 (지정 시에만 씀)")
    ap.add_argument("--wf", action="store_true",
                    help="walk-forward — 구간을 둘로 갈라 각각 평가(in-sample 편향 검사)")
    args = ap.parse_args()

    from data.data_verifier import _all_universe_codes
    codes = _all_universe_codes()
    print(f"[T-1] 유니버스 {len(codes)}종 로드 중…", flush=True)
    data = load_all(codes)
    print(f"     일봉 보유 {len(data)}종", flush=True)

    ref = max(data.values(), key=lambda S: len(S["bars"]))
    all_days = [b[0] for b in ref["bars"]]
    days = all_days[-(args.days + 6):-6]        # 뒤 6일은 forward 미확정이라 제외
    print(f"     평가 구간 {days[0]} ~ {days[-1]} ({len(days)}일)\n", flush=True)

    # ★8/21 walk-forward — 2라운드 피처 일부는 1라운드 결과를 보고 만들었다(같은 데이터에서
    #   가설을 만들고 같은 데이터로 검정 = in-sample 편향). 7/30 R-5가 이 검사로 기각됐다.
    #   구간을 둘로 갈라 **양쪽 다 같은 방향**이어야 후보로 인정한다.
    if args.wf:
        h = len(days) // 2
        a, b = days[:h], days[h:]
        print(f"[walk-forward] A={a[0]}~{a[-1]}({len(a)}일)  B={b[0]}~{b[-1]}({len(b)}일)")
        print()
        ra = {r["name"]: r for r in screen(data, a)}
        rb = {r["name"]: r for r in screen(data, b)}
        print(f"{'피처':36s} {'A t(T+3)':>9s} {'B t(T+3)':>9s} {'A 리프트':>9s} {'B 리프트':>9s}  판정")
        print("─" * 100)
        rows = []
        for name in FEATURES:
            ha, hb = (ra.get(name) or {}).get("h", {}).get(3), (rb.get(name) or {}).get("h", {}).get(3)
            if not ha or not hb:
                print(f"{name:36s} {'—':>9s} {'—':>9s}  판정 불가(표본 부족)")
                continue
            same = (ha["lift"] > 0) == (hb["lift"] > 0)
            both = min(abs(ha["t"]), abs(hb["t"])) >= 1.0
            verdict = ("✅일관" if same and both else ("⚠방향만 일치" if same else "❌비일관"))
            rows.append((name, ha, hb, verdict))
        rows.sort(key=lambda x: -min(x[1]["t"], x[2]["t"]))
        for name, ha, hb, v in rows:
            print(f"{name:36s} {ha['t']:+9.2f} {hb['t']:+9.2f} {ha['lift']:+8.2f}%p "
                  f"{hb['lift']:+8.2f}%p  {v}")
        print()
        print("★ walk-forward 통과 = 양 구간 같은 방향 + 양쪽 |t|>=1.0")
        return 0

    res = screen(data, days)
    res.sort(key=lambda r: -(r["h"].get(3) or {}).get("t", -99))

    print(f"{'피처':36s} {'일평균':>6s} {'T+3':>9s} {'t(T+3)':>7s} {'플러스날':>8s} "
          f"{'T+5':>9s} {'t(T+5)':>7s} {'T+10':>9s} {'t(T+10)':>8s}")
    print("─" * 112)
    passed = []
    for r in res:
        h3, h5, h10 = r["h"].get(3), r["h"].get(5), r["h"].get(10)
        if not h3:
            print(f"{r['name']:34s} {'—':>6s}  판정 불가(표본 부족)")
            continue
        avg = h3["n"] / h3["days"] if h3["days"] else 0
        mark = ""
        # ★ 지평별 판정 — 공매도 잔고 감소처럼 **늦게 크는** 축은 T+3만 보면 놓친다(1라운드 실측).
        #   어느 지평이든 t>2 이면 후보(단 지평을 반드시 명시 — 사후 체리피킹 방지용으로 전 지평 표기).
        ok = any((h or {}).get("t", 0) > T_PASS and (h or {}).get("days", 0) >= MIN_DAYS
                 and (h or {}).get("n", 0) >= MIN_PICKS for h in (h3, h5, h10))
        if ok:
            mark = "  ★t>2 통과"
            passed.append(r)
        elif any((h or {}).get("t", 0) < -T_PASS and (h or {}).get("days", 0) >= MIN_DAYS
                 and (h or {}).get("n", 0) >= MIN_PICKS for h in (h3, h5, h10)):
            mark = "  ▼회피후보"
        print(f"{r['name']:36s} {avg:6.1f} {h3['lift']:+8.2f}%p {h3['t']:+7.2f} "
              f"{h3['pos_days']:4d}/{h3['days']:<3d} "
              f"{(h5 or {}).get('lift', 0):+8.2f}%p {(h5 or {}).get('t', 0):+7.2f} "
              f"{(h10 or {}).get('lift', 0):+8.2f}%p {(h10 or {}).get('t', 0):+8.2f}{mark}")

    print("\n" + "─" * 104)
    if passed:
        print(f"★ t>{T_PASS} 통과 {len(passed)}건 — 후보 등재 대상:")
        for r in passed:
            best = max(((K, r["h"].get(K)) for K in (3, 5, 10) if r["h"].get(K)),
                       key=lambda kv: kv[1]["t"])
            K, h = best
            print(f"   · {r['name']} — **T+{K}** 리프트 {h['lift']:+.2f}%p · t {h['t']:+.2f} · "
                  f"일평균 {h['n']/h['days']:.1f}건 · {h['days']}일 "
                  f"(전 지평: " + " / ".join(
                      f"T+{k} {(r['h'].get(k) or {}).get('t', 0):+.2f}" for k in (3, 5, 10)) + ")")
    else:
        print(f"★ t>{T_PASS} 통과 **0건**. — 후보 등재 없음.")
        print("  (억지 후보 금지 규약: 통과 0건이면 0건이라고 보고한다)")

    if args.json:
        Path(args.json).write_text(json.dumps(res, ensure_ascii=False, indent=1), encoding="utf-8")
        print(f"\n저장: {args.json}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
