# -*- coding: utf-8 -*-
"""[F-155] 장중 점화(_intraday_ignition) 재현 백테스트 — read-only.

■ 왜 이걸 파는가
  8/1 [F-84] 결론: **선착순 실행 규칙을 완벽히 고쳐도 개선 여지는 8%뿐이고,
  손실의 92%는 "그날 후보 전체가 마이너스"에서 나온다** → 자원을 '고르는 순서'에서
  **'후보에 무엇을 넣는가'**로 옮겨야 한다.
  8/1 [F-87] 처방: 명분↔v2 교집합 0의 원인은 시간축이고, **"명분을 장중에 알아야 한다"**.

  그런데 8/10 실측에서 드러난 것 —
  `catalyst_scanner._intraday_ignition` 은 **6/26에 이미 구현돼 있었고**,
  `scan_catalyst` 도 `_is_today_loaded` 로 장중/마감후를 분기한다. 그런데
  **catalyst_archive 29일 870건이 전부 `intraday: False`** = 45일간 단 한 번도
  장중 경로가 돈 적이 없다. 스케줄이 nightly 18:28 하나뿐이고 그 시각엔 항상
  오늘봉이 적재돼 있어 **일봉 경로만** 탄다.
  → [F-147]과 같은 모양: **"장중 탐지가 있다" ≠ "장중 탐지가 돈다".**

■ 이 도구가 하는 일 (cron 없이 검증한다)
  과거 ticks 로 장중을 재생해 `_intraday_ignition` 을 그대로 먹인다.
    ① 하루 몇 종목이 잡히는가 (실행 가능한 규모인가)
    ② v2 신호와 얼마나 겹치는가 — [F-87] 의 교집합 질문을 **look-ahead 없이**
    ③ 점화 시점 진입 성과 — [F-84] 의 "후보에 무엇을 넣는가"에 대한 첫 실측
    ④ 사장님 자금 룰(상한 5건/일) 적용 후 성과 — [F-32] 판정 4층

■ look-ahead 없음 (중요)
  점화 판정 입력은 ①D-1 까지의 일봉 ②시각 t 까지 관측된 당일 등락률·누적거래량뿐이다.
  둘 다 시각 t 에 실제로 알 수 있다. catalyst 의 **명분(뉴스) 등급은 쓰지 않는다** —
  그건 18:28 산출이라 look-ahead 이기 때문이다([F-87] 경고 준수).
  즉 여기서 재는 것은 **'명분 있는 끼'의 '끼(점화)' 축 단독**이다.

■ 단일진실 (로컬 복제 금지 — [F-37] 교훈)
  점화 판정 = catalyst_scanner._intraday_ignition (import)
  v2 신호   = r1_pullback_entry_backtest._signal_index/_is_v2 (import)
  청산·상수 = playbook_shadow._simulate_exit / TP_PCT·SL_PCT·COST_RT (import)
  상한 5건  = r1.CAP_PER_DAY (import)
  ★ 백테스트 정확성을 위해 `catalyst_scanner._rows` 만 **D-1 까지로 잘라 주입**한다
    (판정 로직은 손대지 않는다 — 원본 함수가 그대로 판단한다).

■ 방법론 주의 (정직 표기)
  `_intraday_ignition` 의 vol_ratio 는 **누적 거래량 ÷ 20일 일간 평균**이다.
  장중 이른 시각일수록 분자가 덜 쌓여 2.0배 문턱이 구조적으로 더 어렵다.
  이건 결함이 아니라 **운영 코드와 같은 성질**이라 그대로 재현했다(운영에서도
  ranking_scan 의 장중 누적 거래량을 쓴다). 시각별 민감도를 같이 출력한다.

■ 불변식: read-only · 매매 무접촉 · 봇 OFF · 파일 쓰기 0.

실행(VPS):
  python3.11 -X utf8 tools/manual/f155_intraday_ignition_backtest.py
  python3.11 -X utf8 tools/manual/f155_intraday_ignition_backtest.py --at 09:30:00
"""
from __future__ import annotations

import argparse
import contextlib
import csv
import sys
from collections import defaultdict
from pathlib import Path

_HERE = Path(__file__).resolve().parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))
_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import r1_pullback_entry_backtest as r1        # noqa: E402  v2 신호 판정 단일진실
from tools import catalyst_scanner as cs       # noqa: E402  점화 판정 단일진실

pb = r1.pb
CAP = r1.CAP_PER_DAY
COST = pb.COST_RT

# 관측 시각 그리드 — v2 정제판이 10시 前만 보므로 같은 창으로 맞춘다.
AT_GRID = ["09:15:00", "09:30:00", "09:45:00", "10:00:00"]


# ── 판독 (pb._read_ticks 는 volume 을 버리므로 여기서만 별도 판독) ──────────
def _read_ticks_vol(day: str, code: str) -> list:
    """ticks → [(time, change_rate, cum_volume)] · price>0 만 · 시간순.

    ★pb._read_ticks 를 대체하는 게 아니라 **보완**이다(가격·강도는 pb 것을 쓴다).
      pb 튜플에 volume 이 없어서 이 축만 따로 읽는다 — 판정식은 건드리지 않는다.
    """
    p = pb.TICKS / day / f"{code}.csv"
    if not p.exists():
        return []
    out = []
    try:
        with p.open(encoding="utf-8") as f:
            for row in csv.DictReader(f):
                try:
                    t = str(row.get("time", ""))
                    px = float(row.get("price") or 0)
                    cr = float(row.get("change_rate") or 0)
                    vol = float(row.get("volume") or 0)
                except (ValueError, TypeError):
                    continue
                if t and px > 0:
                    out.append((t, cr, vol))
    except Exception:  # noqa: BLE001
        return []
    out.sort(key=lambda x: x[0])
    return out


def _daily_rows_before(code: str, day: str) -> list:
    """일봉 rows 중 **day 이전만** (백테스트 look-ahead 차단).

    cs._rows 와 같은 포맷 [(date, close, high, low, volume)] 을 돌려준다.
    """
    iso = f"{day[:4]}-{day[4:6]}-{day[6:]}"
    return [r for r in cs._rows(code) if str(r[0])[:10] < iso]


@contextlib.contextmanager
def _sliced_rows(day: str):
    """cs._rows 를 'day 이전만' 버전으로 임시 교체.

    ★판정 함수(_intraday_ignition)는 원본 그대로 두고 **입력만** 과거로 자른다.
      로직을 복제하면 [F-37](로컬 복제가 정본과 조용히 갈림)이 재발한다.
    """
    orig = cs._rows
    cache: dict[str, list] = {}

    def patched(code: str):
        if code not in cache:
            cache[code] = [r for r in orig(code) if str(r[0])[:10] < f"{day[:4]}-{day[4:6]}-{day[6:]}"]
        return cache[code]

    cs._rows = patched
    try:
        yield
    finally:
        cs._rows = orig


def _obs_at(rows_vol: list, at: str):
    """시각 at 시점의 마지막 관측 (t <= at) → (t, change_rate, cum_volume)."""
    best = None
    for t, cr, vol in rows_vol:
        if t <= at:
            best = (t, cr, vol)
        else:
            break
    return best


def _valid_days() -> list:
    days = []
    for d in sorted(p.name for p in pb.TICKS.iterdir() if p.is_dir()):
        if not (len(d) == 8 and d.isdigit()):
            continue
        try:
            h = pb.ticks_health(d)
        except Exception:  # noqa: BLE001
            continue
        verdict = h.get("verdict") if isinstance(h, dict) else h
        if verdict == "OK":
            days.append(d)
    return days


def _codes(day: str) -> list:
    d = pb.TICKS / day
    return sorted(f.stem for f in d.glob("*.csv")) if d.is_dir() else []


def _stats(rets: list) -> dict:
    if not rets:
        return {"n": 0, "win_pct": 0.0, "avg_net": 0.0, "sum_net": 0.0}
    wins = sum(1 for r in rets if r > 0)
    avg = sum(rets) / len(rets)
    return {
        "n": len(rets),
        "win_pct": round(100 * wins / len(rets), 1),
        "avg_net": round(avg - COST, 3),
        "sum_net": round(sum(r - COST for r in rets), 2),
    }


def run(at: str, days: list, verbose: bool = False) -> dict:
    ig_by_day: dict[str, list] = defaultdict(list)
    v2_by_day: dict[str, set] = defaultdict(set)
    universe_by_day: dict[str, int] = {}
    ig_trades, v2_trades, both_trades = [], [], []

    for day in days:
        codes = _codes(day)
        universe_by_day[day] = len(codes)
        with _sliced_rows(day):
            for code in codes:
                rows = pb._read_ticks(day, code)
                if not rows:
                    continue

                # ── v2 신호 (단일진실) ──
                sig = r1._signal_index(rows)
                if sig is not None and r1._is_v2(rows, sig):
                    v2_by_day[day].add(code)

                # ── 장중 점화 (단일진실·입력만 과거로 슬라이스) ──
                rv = _read_ticks_vol(day, code)
                obs = _obs_at(rv, at)
                if obs is None:
                    continue
                t_obs, cr_obs, vol_obs = obs
                try:
                    is_ig, vol_ratio = cs._intraday_ignition(code, cr_obs, vol_obs)
                except Exception:  # noqa: BLE001
                    continue
                if not is_ig:
                    continue

                ig_by_day[day].append({"code": code, "t": t_obs, "chg": cr_obs,
                                       "vol_ratio": vol_ratio})

    # ── 성과 (진입 = 점화 관측 이후 첫 행, 청산 = pb 정본) ──
    for day, igs in ig_by_day.items():
        igs.sort(key=lambda x: x["t"])           # 시간순 = 실행 가능한 순서
        for rank, ig in enumerate(igs):
            rows = pb._read_ticks(day, ig["code"])
            ei = next((i for i, r in enumerate(rows) if r[0] > ig["t"]), None)
            if ei is None:
                continue
            entry = rows[ei][1]
            ret, why, _ = pb._simulate_exit(rows, ei, entry)
            if ret is None:
                continue
            rec = {"date": day, "code": ig["code"], "t": ig["t"], "ret": round(ret, 2),
                   "why": why, "rank": rank, "vol_ratio": ig["vol_ratio"]}
            ig_trades.append(rec)
            if ig["code"] in v2_by_day[day]:
                both_trades.append(rec)

    # v2 baseline (같은 표본 기간·같은 청산 규약)
    for day in days:
        for code in sorted(v2_by_day[day]):
            rows = pb._read_ticks(day, code)
            sig = r1._signal_index(rows)
            if sig is None:
                continue
            e = r1._entry_baseline(rows, sig)
            if not e:
                continue
            ei, entry, et = e
            tr = r1._trade(rows, ei, entry, et, day, code, sig)
            if tr:
                v2_trades.append(tr)

    # ── 교집합 — 무작위 기대 대비 ([F-87] 방법) ──
    n_ig = sum(len(v) for v in ig_by_day.values())
    n_v2 = sum(len(v) for v in v2_by_day.values())
    inter = sum(len({x["code"] for x in ig_by_day[d]} & v2_by_day[d]) for d in days)
    exp = sum(len(ig_by_day[d]) * len(v2_by_day[d]) / max(1, universe_by_day[d]) for d in days)

    # ── 상한 5건/일 (사장님 자금 룰 — [F-32] 4층) ──
    cap_rets = []
    by_day = defaultdict(list)
    for t in ig_trades:
        by_day[t["date"]].append(t)
    for day, ts in by_day.items():
        ts.sort(key=lambda x: x["t"])
        cap_rets += [x["ret"] for x in ts[:CAP]]

    return {
        "at": at, "days": len(days),
        "ig_total": n_ig, "v2_total": n_v2,
        "ig_per_day": round(n_ig / max(1, len(days)), 2),
        "intersect": inter, "expected": round(exp, 1),
        "lift": round(inter / exp, 2) if exp > 0 else None,
        "ig_all": _stats([t["ret"] for t in ig_trades]),
        "ig_cap5": _stats(cap_rets),
        "v2_baseline": _stats([t["ret"] for t in v2_trades]),
        "both": _stats([t["ret"] for t in both_trades]),
        "_ig_trades": ig_trades,
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--at", help="단일 관측 시각 (기본: 그리드 전체)")
    ap.add_argument("--days", type=int, default=0, help="최근 N 유효일만")
    args = ap.parse_args()

    days = _valid_days()
    if args.days:
        days = days[-args.days:]
    if not days:
        print("유효일 0 — ticks_health OK 인 날이 없다")
        return 1

    print("=" * 96)
    print(f"[F-155] 장중 점화 재현 백테스트 — 유효일 {len(days)}일 "
          f"({days[0]}~{days[-1]}) · read-only · look-ahead 없음")
    print(f"  점화 판정 = catalyst_scanner._intraday_ignition (원본 import·입력만 D-1 슬라이스)")
    print(f"  휴면<{cs.IGNITE_DORMANT_MAX}% · 거래량>={cs.IGNITE_VOL_X}x · "
          f"당일 {cs.IGNITE_SURGE_MIN}~{cs.IGNITE_CHASE_MAX}%")
    print("=" * 96)

    grid = [args.at] if args.at else AT_GRID
    for at in grid:
        r = run(at, days)
        print(f"\n■ 관측 시각 {at}")
        print(f"  점화 {r['ig_total']}건 ({r['ig_per_day']}/일) · v2 {r['v2_total']}건")
        print(f"  교집합 {r['intersect']}건 / 무작위 기대 {r['expected']} "
              f"→ lift {r['lift']}  (1.0=무관·>1=같은 종목을 본다)")
        for label, key in (("점화 전건", "ig_all"), (f"점화 상한{CAP}건/일", "ig_cap5"),
                           ("v2 baseline", "v2_baseline"), ("점화∩v2", "both")):
            s = r[key]
            if s["n"] == 0:
                print(f"    {label:16s} : 표본 0")
                continue
            print(f"    {label:16s} : {s['n']:5d}건 승{s['win_pct']:5.1f}% "
                  f"건당 net {s['avg_net']:+.3f}%p  누적 {s['sum_net']:+.2f}%p")

    print("\n" + "=" * 96)
    print("★ 판정 인용 시 4층 통과 명시 의무([F-32]) — 위 '상한5건/일' 행이 실행 가능 수치다.")
    print("★ 명분(뉴스) 축은 여기 없다 — 18:28 산출이라 look-ahead. '끼(점화)' 단독 측정이다.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
