# -*- coding: utf-8 -*-
"""재진입 비용 민감도 — paper go/no-go 게이트 (6/1 단타봇 라이브 숙제 #1).

사장님 6/1 결정: "라이브 숙제 분석". paper 전 #1 치명 질문 =
  "STOP_REENTER의 +엣지는 추가 세그먼트(재진입=추가 왕복비용)으로 산 것.
   실 비용이 얼마까지 오르면 그 우위가 사라지나(break-even cost)?"

방법(breakout_reentry_backtest_5_31.py 로직 계승, 비용 분리):
  - 각 돌파 이벤트마다 전략별 **gross PnL(비용0)** + **세그먼트 수(=왕복 횟수)** 측정.
  - net(c) = gross - 세그먼트수 × c   (c=레그당 왕복비용). 재시뮬 없이 비용 그리드 스윕.
  - break-even: avg_re_net(c)=avg_on_net(c) 푸는 c* (해석해). c < c* 면 REENTER 우위.
      REENTER vs ONCE  : c*_once = (E[re_gross]-E[on_gross]) / (E[re_seg]-E[on_seg])
      REENTER vs HOLD  : c*_hold = (E[re_gross]-E[hold_gross]) / (E[re_seg]-1)
  - 끼 등급별로도 산출(EXPLOSIVE는 엣지 큼 → 비용 헤드룸 큼? 검증).

데이터: stock_data_daily(3885종목 일봉). `--data DIR` 로 합성 fixture 검증 가능.
★ 한계 계승: 일봉 해상도(장중 -3% 정밀X)·생존편향(절대수익 과대)·종가체결 가정.
  → **절대 break-even 숫자보다 "현 비용 0.48% 대비 헤드룸 배수"를 상대지표로 해석.**

사용:
  python tools/reentry_cost_sensitivity_6_1.py                 # 실데이터(노트북/VPS)
  python tools/reentry_cost_sensitivity_6_1.py --data tmp/fix  # 합성 검증
  python tools/reentry_cost_sensitivity_6_1.py --selftest      # 로직 자기검증(데이터 불필요)
"""
from __future__ import annotations
import argparse
import csv
import glob
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
DAILY_DEFAULT = ROOT / "stock_data_daily"

UP_THRESH = 0.10      # 돌파일 종가상승 +10% (5_31 동일)
VOL_X = 2.0           # 거래량 MA20 대비 2배
TRAIL = 0.03          # 고점 -3%
HOLD_K = 5            # 보유 윈도우(거래일)
LIVE_COST = 0.0048    # 현재 가정 레그당 왕복비용(슬립+세금) — 헤드룸 기준점
COST_GRID = (0.0030, 0.0040, 0.0048, 0.0060, 0.0080, 0.0100, 0.0120)


def _load(path):
    """CSV → (rows, dates). rows=(o,h,l,c,v) 인덱스 동일, dates=parallel 날짜str.

    날짜는 OOS(기간 분할) 전용 — OHLCV 계산엔 미사용(rows 구조 불변).
    """
    rows = []
    dates = []
    try:
        with open(path, encoding="utf-8-sig") as f:
            for r in csv.DictReader(f):
                try:
                    o = float(r["open"]); h = float(r["high"])
                    l = float(r["low"]); c = float(r["close"]); v = float(r["volume"])
                except (ValueError, KeyError):
                    continue
                if o > 0 and c > 0:
                    rows.append((o, h, l, c, v))
                    # 날짜 컬럼명 파일마다 상이(날짜/date/Date/빈헤더) → 다중 키 + 첫 컬럼 폴백
                    dval = (r.get("날짜") or r.get("date") or r.get("Date")
                            or r.get("") or next(iter(r.values()), ""))
                    dates.append(str(dval or ""))
    except Exception:
        return [], []
    return rows, dates


def _events(rows):
    """돌파 진입 인덱스 (종가매수). 종가상승≥UP_THRESH & 거래량≥VOL_X×MA20."""
    n = len(rows)
    ev = []
    for i in range(60, n - 1):
        prev_c = rows[i - 1][3]
        c = rows[i][3]
        if prev_c <= 0:
            continue
        up = c / prev_c - 1
        vma = sum(rows[j][4] for j in range(i - 20, i)) / 20
        if vma <= 0:
            continue
        if up >= UP_THRESH and rows[i][4] >= VOL_X * vma:
            ev.append(i)
    return ev


def _kki_proxy(rows, e):
    """끼 프록시 = 진입 직전 과거60일 +15%↑ 급등일수 (Stage1 F2)."""
    cnt = 0
    for j in range(max(1, e - 60), e):
        pc = rows[j - 1][3]
        if pc > 0 and rows[j][3] / pc - 1 >= 0.15:
            cnt += 1
    return cnt


def _sim_gross(rows, e, hold_k=HOLD_K):
    """돌파 e(종가매수)~K일 — 전략별 **gross PnL%(비용0)** + 세그먼트 수.

    세그먼트 = 매수→매도 1왕복(비용 1회 부과 단위). 비용은 사후 net()에서 차감.
    """
    entry = rows[e][3]
    end = min(e + hold_k, len(rows) - 1)
    days = list(range(e + 1, end + 1))
    if not days:
        return None

    hold_gross = (rows[end][3] / entry - 1) * 100      # HOLD: 1세그먼트(K일 보유 후 종가청산)
    mdd = min((rows[d][2] / entry - 1) for d in days) * 100

    def run(reenter: bool):
        gross = 0.0
        seg = 0
        in_pos = True
        p_in = entry
        hw = entry
        last_stop = entry * (1 - TRAIL)
        for d in days:
            o, h, l, c, v = rows[d]
            if in_pos:
                hw = max(hw, h)
                stop = hw * (1 - TRAIL)
                last_stop = stop
                if l <= stop:                       # 손절(저가가 트레일 닿음)
                    gross += (stop / p_in - 1)
                    seg += 1
                    in_pos = False
            else:
                if reenter and c >= last_stop:      # 종가가 직전손절선 회복 → 재진입
                    in_pos = True
                    p_in = c
                    hw = c
        if in_pos:                                   # 마지막날 종가 청산
            gross += (rows[end][3] / p_in - 1)
            seg += 1
        return gross * 100, seg

    re_g, re_s = run(True)
    on_g, on_s = run(False)
    return {"hold_g": hold_gross, "hold_s": 1,
            "re_g": re_g, "re_s": re_s,
            "on_g": on_g, "on_s": on_s, "mdd": mdd}


def _net(gross, seg, cost_frac):
    """net% = gross% - 세그먼트수 × (레그당왕복비용 fraction × 100)."""
    return gross - seg * cost_frac * 100


def _summarize(res):
    """이벤트 결과 리스트 → 전략별 평균 gross/세그먼트. (None 제외 가정)"""
    n = len(res)
    def m(k):
        return sum(r[k] for r in res) / n
    return {
        "n": n,
        "re_g": m("re_g"), "re_s": m("re_s"),
        "on_g": m("on_g"), "on_s": m("on_s"),
        "hold_g": m("hold_g"), "hold_s": 1.0,
    }


def _breakeven(num, denom):
    """break-even 비용 fraction. denom(세그먼트차)≤0이면 None(우위 비용무관/없음)."""
    if denom <= 0:
        return None
    return (num / 100) / denom        # num은 %p, denom은 세그먼트차 → fraction


def _headroom_line(ev_subset, label):
    """이벤트 부분집합 → vs ONCE break-even 헤드룸 한 줄. OOS 출력용."""
    res = [r for r in (_sim_gross(rows, e, HOLD_K) for rows, e, *_ in ev_subset) if r]
    if not res:
        return f"  {label:>10}: 이벤트 0건"
    s = _summarize(res)
    be = _breakeven(s["re_g"] - s["on_g"], s["re_s"] - s["on_s"])
    if be is None:
        return f"  {label:>10}: {s['n']:>6,}건  BE vs ONCE=비용무관(∞ 헤드룸)"
    return f"  {label:>10}: {s['n']:>6,}건  BE vs ONCE={be*100:.2f}%/leg ({be/LIVE_COST:.1f}배 헤드룸)"


def _report(ev_all):
    res = [r for r in (_sim_gross(rows, e, HOLD_K) for rows, e, *_ in ev_all) if r]
    s = _summarize(res)
    print(f"돌파 이벤트 {s['n']:,}건 (종가상승≥{UP_THRESH*100:.0f}% & 거래량≥{VOL_X}×MA20, 보유{HOLD_K}일)\n")

    print("[A] 평균 세그먼트(왕복 횟수) — 재진입이 비용을 얼마나 더 쓰나:")
    print(f"  STOP_REENTER {s['re_s']:.2f}세그  |  STOP_ONCE {s['on_s']:.2f}세그  |  HOLD 1.00세그")
    print(f"  → REENTER가 ONCE보다 평균 {s['re_s']-s['on_s']:+.2f}세그 추가(=추가 왕복비용)\n")

    print("[B] 비용 스윕 — 레그당 왕복비용별 평균 net 수익%:")
    print(f'{"비용/leg":>9}{"HOLD":>11}{"STOP_REENTER":>15}{"STOP_ONCE":>13}{"1위":>14}')
    for c in COST_GRID:
        h = _net(s["hold_g"], s["hold_s"], c)
        re_ = _net(s["re_g"], s["re_s"], c)
        on = _net(s["on_g"], s["on_s"], c)
        best = max([("HOLD", h), ("REENTER", re_), ("ONCE", on)], key=lambda x: x[1])[0]
        flag = " ←현재" if abs(c - LIVE_COST) < 1e-9 else ""
        print(f'{c*100:>7.2f}%{h:>+10.2f}%{re_:>+14.2f}%{on:>+12.2f}%{best:>12}{flag}')
    print()

    be_once = _breakeven(s["re_g"] - s["on_g"], s["re_s"] - s["on_s"])
    be_hold = _breakeven(s["re_g"] - s["hold_g"], s["re_s"] - s["hold_s"])
    print("[C] ★ break-even 비용 (REENTER 우위가 사라지는 레그당 왕복비용):")
    def fmt_be(be, who):
        if be is None:
            return f"  vs {who}: 세그먼트차≤0 → REENTER가 비용 더 안 씀(우위 비용무관)"
        mult = be / LIVE_COST
        verdict = "합격(헤드룸 충분)" if mult >= 2.0 else ("주의(2배 미만)" if mult >= 1.0 else "위험(현비용서 이미 열위)")
        return f"  vs {who}: {be*100:.2f}%/leg 까지 우위 (현 0.48% 대비 {mult:.1f}배 헤드룸) → {verdict}"
    print(fmt_be(be_once, "STOP_ONCE"))
    print(fmt_be(be_hold, "HOLD"))
    print()

    print("[D] 끼 등급별 break-even (高끼=엣지 큼 → 헤드룸 큰가?):")
    print(f'{"끼등급":>16}{"이벤트":>8}{"re세그":>8}{"on세그":>8}{"BE vs ONCE":>13}{"헤드룸":>9}')
    buckets = [("SLUGGISH 끼0", lambda k: k == 0), ("MODERATE 끼1~2", lambda k: 1 <= k <= 2),
               ("HUNTABLE 끼3~4", lambda k: 3 <= k <= 4), ("EXPLOSIVE 끼5+", lambda k: k >= 5)]
    for label, cond in buckets:
        seg_res = [r for r in (_sim_gross(rows, e, HOLD_K) for rows, e, k, *_ in ev_all if cond(k)) if r]
        if not seg_res:
            print(f'{label:>16}{0:>8}'); continue
        sb = _summarize(seg_res)
        be = _breakeven(sb["re_g"] - sb["on_g"], sb["re_s"] - sb["on_s"])
        if be is None:
            print(f'{label:>16}{sb["n"]:>8,}{sb["re_s"]:>8.2f}{sb["on_s"]:>8.2f}{"비용무관":>13}{"∞":>9}')
        else:
            print(f'{label:>16}{sb["n"]:>8,}{sb["re_s"]:>8.2f}{sb["on_s"]:>8.2f}{be*100:>11.2f}%{be/LIVE_COST:>8.1f}x')
    print()

    # [E] OOS — 이벤트 발생일 기준 전/후반 분할 (과적합·레짐의존 점검)
    dated = [t for t in ev_all if len(t) >= 4 and t[3]]
    print("[E] OOS 기간분할 (이벤트일 중앙값으로 전/후반) — 헤드룸이 양 기간 유지되나:")
    if len(dated) < 2:
        print("  날짜 정보 부족 → OOS 생략")
    else:
        ds = sorted(dated, key=lambda t: t[3])
        mid = len(ds) // 2
        cut = ds[mid][3]
        early = [t for t in ds if t[3] < cut]
        late = [t for t in ds if t[3] >= cut]
        # 경계 동일날짜 쏠림 방지: 한쪽이 비면 인덱스 절반으로 강제 분할
        if not early or not late:
            early, late = ds[:mid], ds[mid:]
        print(_headroom_line(early, f"전반(<{cut})"))
        print(_headroom_line(late, f"후반(≥{cut})"))
        print("  → 양 기간 모두 헤드룸 ≥1배면 레짐 비의존(견고). 한쪽만이면 과적합 의심.")
    print()

    print("해석: [C] vs ONCE 헤드룸 ≥2배 & [E] 양기간 견고면 재진입 엣지가 비용에 강건 → paper 신호.")
    print("      <1배거나 한 기간만 우위면 재진입 라이브 보류(설계 재검·비용 정밀화).")
    print("★ 일봉·생존편향 한계 → 절대 BE보다 '0.48% 대비 배수'를 상대지표로만 신뢰.")
    return 0


def _make_fixture(dirpath: Path):
    """합성 fixture 생성 — 로직 검증용. 알려진 패턴 2종목.

    A: 돌파 후 트레일 2 cycle(털기→회복재진입 ×2) → REENTER 3세그(재진입 검증).
    B: 돌파 후 -3% 털고 회복 못함(재진입 안 됨, ONCE/REENTER 동일=1세그먼트).
    """
    dirpath.mkdir(parents=True, exist_ok=True)
    hdr = "date,open,high,low,close,volume\n"

    # A는 전반(2026-03), B는 후반(2026-10) 날짜 → OOS 분할 검증 가능. (OHLCV 계산엔 날짜 미사용)
    _date = {"v": "2026-03-01"}

    def line(o, h, l, c, v):
        return f"{_date['v']},{o},{h},{l},{c},{v}\n"

    # 62일 워밍업(평탄, 끼0) + 돌파 + 시나리오. 거래량 MA20=100, 돌파일 300(≥2x).
    # _events는 인덱스 60부터(끼 60일 산출) → 워밍업 ≥62 필요. 돌파일=인덱스62.
    def warmup():
        return [line(100, 101, 99, 100, 100) for _ in range(62)]

    # 종목A(전반): idx62 돌파(+12%, vol300) 종가112 → +1일 고점120후 저가 손절터치 → 종가회복 → 상승
    a = warmup()
    a.append(line(100, 112, 100, 112, 300))   # 돌파일(e): close 112 = +12%
    a.append(line(112, 120, 115, 118, 200))   # 고점120 trail=116.4, 저가115<=116.4 손절→ 종가118>=116.4 회복(재진입)
    a.append(line(118, 126, 117, 125, 180))   # 상승 (재진입분 이익)
    a.append(line(125, 130, 124, 129, 150))
    a.append(line(129, 133, 128, 132, 140))
    a.append(line(132, 135, 131, 134, 130))   # 보유끝(K=5) 종가청산

    # 종목B(후반): 돌파 후 손절만, 회복 못함(재진입 X)
    _date["v"] = "2026-10-01"
    b = warmup()
    b.append(line(100, 112, 100, 112, 300))   # 돌파
    b.append(line(112, 115, 108, 109, 200))   # 고점115 trail=111.55, 저가108 손절 → 종가109<111.55 회복못함
    b.append(line(109, 110, 105, 106, 180))
    b.append(line(106, 108, 104, 105, 150))
    b.append(line(105, 107, 103, 104, 140))
    b.append(line(104, 106, 102, 103, 130))

    (dirpath / "A.csv").write_text(hdr + "".join(a), encoding="utf-8")
    (dirpath / "B.csv").write_text(hdr + "".join(b), encoding="utf-8")


def _selftest():
    """데이터 불필요 — 합성 fixture로 핵심 로직(세그먼트/비용단조/BE) 검증."""
    import tempfile, shutil
    tmp = Path(tempfile.mkdtemp(prefix="reentry_cost_"))
    try:
        _make_fixture(tmp)
        ev_all = []
        for f in sorted(glob.glob(str(tmp / "*.csv"))):
            rows, dates = _load(f)
            if len(rows) < 30:
                continue
            for e in _events(rows):
                ev_all.append((rows, e, _kki_proxy(rows, e), dates[e]))
        assert len(ev_all) == 2, f"이벤트 2건 기대(A,B), 실제 {len(ev_all)}"

        results = [_sim_gross(rows, e, HOLD_K) for rows, e, k, d in ev_all]
        a = next(r for r in results if r["re_s"] > r["on_s"])   # 재진입 발생 = A
        b = next(r for r in results if r["re_s"] == r["on_s"])  # 재진입 없음 = B

        checks = []
        # 1) A: 트레일이 2 cycle → 재진입 2회 = REENTER 3세그, ONCE는 첫 손절뿐 1세그
        checks.append(("A 재진입 세그먼트 re=3,on=1", a["re_s"] == 3 and a["on_s"] == 1))
        # 2) B: 회복 못함 → 재진입 없음 → re_s == on_s == 1
        checks.append(("B 재진입없음 re=on=1", b["re_s"] == 1 and b["on_s"] == 1))
        # 3) HOLD은 항상 1세그
        checks.append(("HOLD 1세그", a["hold_s"] == 1 and b["hold_s"] == 1))
        # 4) 비용 단조: 비용↑ → net↓ (모든 전략)
        mono = all(_net(a["re_g"], a["re_s"], COST_GRID[i]) >= _net(a["re_g"], a["re_s"], COST_GRID[i+1])
                   for i in range(len(COST_GRID)-1))
        checks.append(("비용↑→net↓ 단조", mono))
        # 5) A: REENTER가 ONCE보다 추가세그(2개↑) → BE 유한·양수(gross_re>gross_on 설계)
        be = _breakeven(a["re_g"] - a["on_g"], a["re_s"] - a["on_s"])
        checks.append(("A BE 유한·양수", be is not None and be > 0))
        # 6) B: 세그먼트차 0 → BE None(비용무관)
        be_b = _breakeven(b["re_g"] - b["on_g"], b["re_s"] - b["on_s"])
        checks.append(("B BE None(세그먼트차0)", be_b is None))
        # 7) net 공식 정합: gross에서 (실세그먼트수)×cost×100 정확히 차감
        manual = a["re_g"] - a["re_s"] * LIVE_COST * 100
        checks.append(("net 공식 정합", abs(_net(a["re_g"], a["re_s"], LIVE_COST) - manual) < 1e-9))
        # 8) OOS 날짜 파싱·분할: A(2026-03)=전반, B(2026-10)=후반 정확 분리
        dates_seen = sorted(t[3] for t in ev_all)
        checks.append(("OOS 날짜 파싱(A전·B후)", dates_seen == ["2026-03-01", "2026-10-01"]))

        ok = sum(1 for _, c in checks if c)
        for name, c in checks:
            print(f"  [{'PASS' if c else 'FAIL'}] {name}")
        print(f"\n셀프테스트 결과: {ok}/{len(checks)} PASS")
        print(f"  (검증값 A: re_g={a['re_g']:.2f}% on_g={a['on_g']:.2f}% BE={be*100:.2f}%/leg)")
        return 0 if ok == len(checks) else 1
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", default=str(DAILY_DEFAULT), help="일봉 CSV 디렉토리")
    ap.add_argument("--selftest", action="store_true", help="합성 로직 자기검증(데이터 불필요)")
    args = ap.parse_args()

    if args.selftest:
        return _selftest()

    files = glob.glob(str(Path(args.data) / "*.csv"))
    if not files:
        print(f"[!] 데이터 없음: {args.data}\n"
              f"    실데이터는 노트북/VPS의 stock_data_daily 에 있음.\n"
              f"    로직 검증은 `--selftest` 로 가능(데이터 불필요).", file=sys.stderr)
        return 2
    ev_all = []
    for f in files:
        rows, dates = _load(f)
        if len(rows) < 30:
            continue
        for e in _events(rows):
            ev_all.append((rows, e, _kki_proxy(rows, e), dates[e]))
    if not ev_all:
        print("[!] 돌파 이벤트 0건", file=sys.stderr)
        return 2
    return _report(ev_all)


if __name__ == "__main__":
    raise SystemExit(main())
