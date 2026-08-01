"""[F-83] 백테스트 ↔ 실측 페이퍼 장부 갭 규명 (read-only).

■ 문제 (8/1 발견)
  같은 전략(PB-A v2)인데 두 수치의 부호가 반대다.
    · 백테스트 58 유효일 2,406건 = 건당 **+0.173%p**   ([F-26] 앵커)
    · 실측 장부 12 유효일  821건 = 건당 **-0.050%p**   (8/1 lynch_concentration)
  [F-26]의 3일치 결론(R-1·R-3·R-4·R-5 기각, "엣지는 폭에 있다")이 전부
  "신호엔 엣지가 있다"는 백테스트 앵커 위에 서 있다. 앵커가 실측에서 재현되지
  않으면 그 위의 결론도 재검토 대상이다 → 8/29 판정 전 필수 규명.

■ 방법 — 교란요인을 하나씩 벗긴다
  갭의 후보 원인은 셋이고, 셋을 섞으면 아무것도 못 가른다.
    ① 표본 기간   : 58일 vs 12일 (나머지 46일이 플러스를 만들었을 수 있다)
    ② 러너 커버리지: 실시간 러너가 백테스트 신호의 일부만 포착([F-60])
    ③ CB 필터     : 장부는 CB 적용 後 수치이고 잘린 건의 손익은 미보존([F-57])
  → 본 도구는 **①을 제거**한다. 백테스트를 장부와 **완전히 같은 12일**로 잘라
    재실행하고, 남은 차이를 ②(건수)와 ③(skipped_cb)로 분해해 제시한다.

■ 판정 분기 (실행 후 이 표로 읽을 것)
    · 백테스트도 12일에서 마이너스  → 원인 ① 표본 효과. 앵커 +0.173은
      "58일 중 특정 구간"의 성질이며 일반화 불가 → [F-26] 결론 재검토 필요.
    · 백테스트는 12일에서도 플러스  → 원인 ②/③. 신호에는 엣지가 있으나
      러너가 못 잡거나 CB가 잘라낸다 → 실행 인프라 문제로 축이 이동.

■ 단일진실 (F-37 재발 차단)
  신호 판정·진입·청산은 r1_pullback_entry_backtest에서 **import**한다
  (_signal_index/_is_v2/_entry_baseline/_trade). 로직을 복제하면 이 도구의
  결론이 기존 판정과 다른 규약을 말하게 된다.
  유효일·장부 수치는 observe_v2_paper에서 import.

■ 불변식: read-only(ticks·장부 어느 파일도 쓰지 않음)·매매 무접촉·봇 OFF.

실행(VPS): python3.11 -X utf8 tools/manual/f83_backtest_ledger_gap.py
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

_HERE = Path(__file__).resolve().parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))

import r1_pullback_entry_backtest as r1  # noqa: E402  판정 로직 단일진실

pb = r1.pb
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from observe_v2_paper import _load_ledger  # noqa: E402


def _ledger_valid_days(led: dict) -> dict:
    """장부 유효일 → {day: {n, net, skipped_cb}}  (규약은 _cum과 동일)."""
    out = {}
    for d in (led.get("days") or {}).values():
        s = d.get("summary") or {}
        if (s.get("ticks_health") or {}).get("verdict") != "OK":
            continue
        if len(d.get("trades") or []) != (s.get("n") or 0):
            continue  # TORN — 증거 훼손일 제외
        out[d.get("date", "?")] = {
            "n": s.get("n") or 0,
            "net": s.get("sum_net") or 0.0,
            "skipped_cb": s.get("skipped_cb") or 0,
        }
    return out


def _backtest_day(day: str) -> list[dict]:
    """해당 일자의 v2 baseline 체결 목록 — r1과 동일 규약."""
    ddir = pb.TICKS / day
    if not ddir.is_dir():
        return []
    trades = []
    for f in ddir.glob("*.csv"):
        rows = pb._read_ticks(day, f.stem)
        if len(rows) < 3:
            continue
        sig = r1._signal_index(rows)
        if sig is None or not r1._is_v2(rows, sig):
            continue
        eb = r1._entry_baseline(rows, sig)
        if not eb:
            continue
        tr = r1._trade(rows, eb[0], eb[1], eb[2], day, f.stem, sig)
        if tr:
            trades.append(tr)
    return trades


def _timeline() -> int:
    """★엣지 열화 검사 — 전 유효일을 월별로 갈라 baseline 건당 net 추이를 본다.

    F-83 1차 결과가 '표본 효과'로 나왔다면 남는 질문은 하나다:
    앵커 +0.173%p를 만든 나머지 날들이 **과거**에 몰려 있는가?
    몰려 있다면 이 신호의 엣지는 열화(decay) 중이며, 8/29 판정은
    '표본 부족'이 아니라 '수명 종료'로 읽어야 한다.
    """
    all_days = sorted(d.name for d in pb.TICKS.iterdir()
                      if d.is_dir() and pb._is_trading_day_dir(d.name))
    ok_days = [d for d in all_days
               if (h := pb.ticks_health(d)) and h["verdict"] == "OK"]
    print("=" * 78)
    print(f"[F-83-T] 엣지 열화 검사 — 유효 {len(ok_days)}일 / 전체 {len(all_days)}일")
    print("=" * 78)
    # ticks 재판독은 비싸다 — 하루 1회만 읽고 seq/buckets를 함께 채운다.
    buckets: dict[str, list[float]] = {}
    seq: list[tuple[str, float]] = []
    for day in ok_days:
        nets = [t["ret"] - pb.COST_RT for t in _backtest_day(day)]
        if not nets:
            continue
        buckets.setdefault(day[:6], []).extend(nets)
        seq.extend((day, x) for x in nets)

    print(f"  {'월':>8} {'건수':>7} {'건당net':>11} {'승률':>7}")
    for mon in sorted(buckets):
        v = buckets[mon]
        wr = 100 * sum(1 for x in v if x > 0) / len(v)
        print(f"  {mon:>8} {len(v):>7} {sum(v)/len(v):+10.3f}%p {wr:>6.1f}%")

    allv = [x for _, x in seq]
    # 시간 3분할 — 월별 표본 불균형을 보정해 '건수 균등'으로 자른다
    if seq:
        third = len(seq) // 3
        parts = [seq[:third], seq[third:2 * third], seq[2 * third:]]
        print(f"\n  ── 시간 3분할 (건수 균등·총 {len(seq)}건) ──")
        for i, p in enumerate(parts, 1):
            if not p:
                continue
            v = [x for _, x in p]
            print(f"   {i}구간 {p[0][0]}~{p[-1][0]} {len(v):>5}건 "
                  f"{sum(v)/len(v):+.3f}%p")
    print(f"\n  전체 {len(allv)}건 건당 "
          f"{(sum(allv)/len(allv) if allv else 0):+.3f}%p  "
          f"(≈ [F-26] 앵커 재현 확인용)")
    print("  ※ 무제한 체결 가정 — 자금·시간 제약 통과 前 수치(4층 규약·[F-32]).")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="", help="JSON 산출 경로(선택)")
    ap.add_argument("--timeline", action="store_true",
                    help="전 유효일 월별/3분할 열화 검사")
    args = ap.parse_args()
    if args.timeline:
        return _timeline()

    led_days = _ledger_valid_days(_load_ledger())
    if not led_days:
        print("[F-83] 장부 유효일 0 — 비교 불가.")
        return 1
    days = sorted(led_days)
    print("=" * 78)
    print(f"[F-83] 백테스트 ↔ 장부 갭 — 동일 {len(days)}일로 표본 효과 제거")
    print("=" * 78)

    print(f"  {'일자':>10} {'백테건':>6} {'백테net/건':>11} │ "
          f"{'장부건':>6} {'장부net/건':>11} {'CB컷':>5} │ {'커버리지':>8}")
    bt_n = bt_sum = 0
    lg_n = lg_sum = skip = 0
    rows_out = []
    for day in days:
        trs = _backtest_day(day)
        n_b = len(trs)
        # 백테스트 net = ret - 왕복비용(장부 규약과 동일: pb.COST_RT)
        s_b = sum(t["ret"] - pb.COST_RT for t in trs)
        lg = led_days[day]
        bt_n += n_b
        bt_sum += s_b
        lg_n += lg["n"]
        lg_sum += lg["net"]
        skip += lg["skipped_cb"]
        cov = (lg["n"] + lg["skipped_cb"]) / n_b * 100 if n_b else 0
        print(f"  {day:>10} {n_b:>6} {(s_b/n_b if n_b else 0):+10.3f}%p │ "
              f"{lg['n']:>6} {(lg['net']/lg['n'] if lg['n'] else 0):+10.3f}%p "
              f"{lg['skipped_cb']:>5} │ {cov:>7.1f}%")
        rows_out.append({"date": day, "bt_n": n_b, "bt_sum": round(s_b, 2),
                         "lg_n": lg["n"], "lg_sum": round(lg["net"], 2),
                         "skipped_cb": lg["skipped_cb"]})

    print("─" * 78)
    bt_avg = bt_sum / bt_n if bt_n else 0
    lg_avg = lg_sum / lg_n if lg_n else 0
    print(f"  {'합계':>10} {bt_n:>6} {bt_avg:+10.3f}%p │ "
          f"{lg_n:>6} {lg_avg:+10.3f}%p {skip:>5} │ "
          f"{((lg_n + skip) / bt_n * 100) if bt_n else 0:>7.1f}%")

    print("\n── 판정 " + "─" * 69)
    print(f"  · 백테스트(같은 {len(days)}일·CB 없음) 건당 {bt_avg:+.3f}%p / {bt_n}건")
    print(f"  · 실측 장부(CB 적용 후)        건당 {lg_avg:+.3f}%p / {lg_n}건 "
          f"(+ CB로 잘린 {skip}건은 손익 미보존)")
    if bt_avg > 0 and lg_avg < 0:
        print("  ▶ 표본 효과 아님. 같은 날에도 백테는 +, 실측은 −.")
        print("    → 원인은 ②러너 커버리지 또는 ③CB/체결 로직. 실행 인프라 축으로 이동.")
        print(f"    커버리지 {((lg_n + skip) / bt_n * 100) if bt_n else 0:.1f}% "
              f"— 100%에서 멀수록 ② 비중이 크다.")
    elif bt_avg < 0:
        print("  ▶ ★표본 효과. 같은 12일에서는 백테스트도 마이너스다.")
        print("    → 앵커 '+0.173%p'는 58일 중 특정 구간의 성질이며 일반화 불가.")
        print("    → [F-26] '신호엔 엣지가 있다' 전제와 그 위의 결론 전부 재검토 대상.")
    else:
        print("  ▶ 두 수치가 같은 방향 — 갭 없음. F-83 종결 후보.")
    print("  ※ 관측 전용. 라이브 flip 근거로 단독 인용 금지.")

    if args.out:
        Path(args.out).write_text(json.dumps(
            {"days": rows_out, "bt_n": bt_n, "bt_avg": round(bt_avg, 4),
             "lg_n": lg_n, "lg_avg": round(lg_avg, 4), "skipped_cb": skip},
            ensure_ascii=False, indent=1), encoding="utf-8")
        print(f"\n[out] {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
