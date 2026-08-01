"""수익/손실 집중도 진단 — 피터 린치 《투자 이야기》 p.155·p.260 가설 검증 (read-only).

■ 검증 대상 가설 (8/1 사장님 지시 — 책 적용 1단계)
  린치: "1980년대 5년 상승기 증시 개장 1,276일 중 수익이 집중 발생한 기간은 단 40일.
        그 40일 빠지면 연 26.3% → 4.3%"(p.155) / "40년 중 최고 40개월 빠지면
        11.5% → 2.7%"(p.260)
  → 우리 판정 지표(S-1 ⑲-3 페이퍼)도 소수 날에 손익이 몰려 있는가?

■ ★린치를 그대로 쓰면 안 되는 지점 (단타봇 정직 해석)
  린치의 표본은 '장기 상승장'이라 best-days 제거만 봐도 논지가 성립한다.
  우리 스코어보드는 순누적이 **마이너스**다. best만 제거하면 "빼니 더 나빠졌다"는
  당연한 산술이 나올 뿐 진단이 안 된다. → best/worst **양방향**으로 제거해
  "이 전략이 소수 대박일에 업혀 있는가 / 소수 참사일에 눌려 있는가"를 가른다.

■ 단일진실 (F-37 재발 차단)
  CAP_VIEW_N·MIN_JUDGE_N·NOTIONAL_KRW·유효일 판정은 전부 observe_v2_paper에서 import.
  로컬 복제 금지 — 상한이 5→3으로 바뀌면 이 도구도 함께 따라가야 한다.
  실행 말미에 정본 `_cum()`과 합계를 대조해 불일치 시 즉시 실패시킨다(조용한 괴리 차단).

■ 불변식: read-only(장부 미기록)·매매 무접촉·봇 OFF.

사용: python3.11 -X utf8 tools/manual/lynch_concentration.py [--top 5] [--json]
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from observe_v2_paper import (  # noqa: E402  (경로 주입 후 import)
    CAP_VIEW_N,
    MIN_JUDGE_N,
    NOTIONAL_KRW,
    _cum,
    _load_ledger,
)


def _valid_day_rows(led: dict) -> list[dict]:
    """유효일만 뽑아 일별 cap 손익을 계산한다.

    ★유효일 판정은 observe_v2_paper._cum(:238-265)과 **동일 규약**이어야 한다:
      verdict == "OK" AND len(trades) == summary.n  (불일치 = TORN → 제외)
    규약이 갈리면 이 도구의 결론이 스코어보드와 다른 모집단을 말하게 된다
    ([F-57] '라벨과 모집단 불일치'와 같은 실패 계열).
    """
    rows = []
    for d in (led.get("days") or {}).values():
        s = d.get("summary") or {}
        verdict = (s.get("ticks_health") or {}).get("verdict")
        trades = d.get("trades") or []
        if verdict != "OK":
            continue
        if len(trades) != (s.get("n") or 0):
            continue  # TORN — 증거 훼손일
        cap = trades[:CAP_VIEW_N]
        rows.append({
            "date": d.get("date", "?"),
            "n": len(cap),
            "net": round(sum((t.get("net") or 0) for t in cap), 4),
            "wins": sum(1 for t in cap if (t.get("net") or 0) > 0),
            "trades": cap,
            # ── 날 효과 분해용 (그날 장부 수록 전체) ──
            # ⚠ [F-57]: summary.n/sum_net은 '전 신호'가 아니라 **CB 적용 후** 수치다.
            #   라벨을 '전 신호'로 쓰면 모집단이 다른 두 숫자를 비교하게 된다.
            #   여기서는 'CB 적용 후 그날 전체'라는 같은 모집단 안에서만 비교한다.
            "all_n": s.get("n") or 0,
            "all_net": s.get("sum_net") or 0.0,
        })
    rows.sort(key=lambda r: r["date"])
    return rows


def _pct_to_krw(pct: float) -> int:
    return int(round(pct, 2) / 100 * NOTIONAL_KRW)


def _strip(rows: list[dict], k: int, worst: bool) -> tuple[float, int]:
    """손익 상위(worst=False) 또는 하위(worst=True) k일을 제거한 뒤의 (순누적, 건수)."""
    if k <= 0:
        return sum(r["net"] for r in rows), sum(r["n"] for r in rows)
    ordered = sorted(rows, key=lambda r: r["net"], reverse=not worst)
    kept = ordered[k:]
    return sum(r["net"] for r in kept), sum(r["n"] for r in kept)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--top", type=int, default=5, help="제거 시뮬 최대 일수")
    ap.add_argument("--json", action="store_true", help="기계 판독용 JSON 출력")
    args = ap.parse_args()

    led = _load_ledger()
    rows = _valid_day_rows(led)
    cum = _cum(led)

    if not rows:
        print("[lynch] 유효일 0 — 판정 불가(수집장애 또는 미정산). 종료.")
        return 0

    total = sum(r["net"] for r in rows)
    total_n = sum(r["n"] for r in rows)

    # ── 자기검증: 정본 _cum과 합계가 어긋나면 즉시 실패(조용한 괴리 차단) ──
    ref = cum.get("valid_cap_sum")
    if ref is None or abs(round(total, 2) - ref) > 0.01:
        print(f"[lynch] ❌ 정본 불일치 — 재계산 {round(total, 2)} vs _cum {ref}")
        print("        유효일 규약이 갈렸다. 판정 인용 금지, 즉시 규명 필요.")
        return 1

    plus = [r for r in rows if r["net"] > 0]
    minus = [r for r in rows if r["net"] < 0]
    sum_plus = sum(r["net"] for r in plus)
    sum_minus = sum(r["net"] for r in minus)

    if args.json:
        print(json.dumps({
            "valid_days": len(rows), "cap": CAP_VIEW_N, "n": total_n,
            "sum_net": round(total, 2), "sum_plus": round(sum_plus, 2),
            "sum_minus": round(sum_minus, 2),
            "days": [{"date": r["date"], "net": r["net"], "n": r["n"]} for r in rows],
        }, ensure_ascii=False, indent=1))
        return 0

    print("=" * 72)
    print(f"수익/손실 집중도 진단 (린치 p.155·p.260 가설) — 상한 {CAP_VIEW_N}건/일")
    print("=" * 72)
    print(f"유효일 {len(rows)}일 · {total_n}건 · 순누적 {total:+.2f}%p "
          f"({_pct_to_krw(total):,}원)")
    if total_n < MIN_JUDGE_N:
        print(f"  ⚠ 표본 {total_n}건 < {MIN_JUDGE_N}건 — 판정 유예 권고(무표본과 구분)")
    print(f"플러스일 {len(plus)}일 합 {sum_plus:+.2f}%p / "
          f"마이너스일 {len(minus)}일 합 {sum_minus:+.2f}%p")

    print("\n── 일별 손익 (내림차순) " + "─" * 47)
    for r in sorted(rows, key=lambda x: x["net"], reverse=True):
        share = (r["net"] / sum_plus * 100) if (r["net"] > 0 and sum_plus) else (
            (r["net"] / sum_minus * 100) if (r["net"] < 0 and sum_minus) else 0)
        bar = "+" if r["net"] > 0 else "-"
        print(f"  {r['date']}  {r['net']:+8.2f}%p  {r['n']}건 "
              f"승{r['wins']}  {bar}측 기여 {abs(share):5.1f}%")

    print("\n── ★린치 검증: 상위 K일 제거 (수익 집중도) " + "─" * 28)
    print(f"  {'K':>2}  {'순누적':>10}  {'건수':>5}   변화")
    for k in range(0, args.top + 1):
        net, n = _strip(rows, k, worst=False)
        delta = net - total
        print(f"  {k:>2}  {net:+9.2f}%p  {n:>4}건   {delta:+.2f}%p")

    print("\n── 대조: 하위 K일 제거 (손실 집중도) " + "─" * 34)
    print(f"  {'K':>2}  {'순누적':>10}  {'건수':>5}   변화")
    flip_at = None
    for k in range(0, args.top + 1):
        net, n = _strip(rows, k, worst=True)
        delta = net - total
        mark = ""
        if flip_at is None and net > 0:
            flip_at = k
            mark = "  ← 여기서 플러스 전환"
        print(f"  {k:>2}  {net:+9.2f}%p  {n:>4}건   {delta:+.2f}%p{mark}")

    # ── 건별 집중도 (린치 p.169·p.215 '10개 중 3개' 축) ──
    all_tr = [t for r in rows for t in r["trades"]]
    all_tr.sort(key=lambda t: (t.get("net") or 0), reverse=True)
    tp = [t for t in all_tr if (t.get("net") or 0) > 0]
    tm = [t for t in all_tr if (t.get("net") or 0) < 0]
    stp = sum((t.get("net") or 0) for t in tp)
    stm = sum((t.get("net") or 0) for t in tm)
    print("\n── 건별 집중도 (p.169 '10개 중 3개가 나머지를 만회') " + "─" * 18)
    print(f"  플러스 {len(tp)}건 합 {stp:+.2f}%p / 마이너스 {len(tm)}건 합 {stm:+.2f}%p")
    for q in (1, 3, 5, 10):
        if len(all_tr) >= q:
            top_q = sum((t.get("net") or 0) for t in all_tr[:q])
            bot_q = sum((t.get("net") or 0) for t in all_tr[-q:])
            sh = (top_q / stp * 100) if stp else 0
            print(f"  상위 {q:>2}건 {top_q:+7.2f}%p (플러스의 {sh:5.1f}%) │ "
                  f"하위 {q:>2}건 {bot_q:+7.2f}%p")

    # ── ★날 효과 vs 선택 효과 분해 ────────────────────────────────
    # 린치 p.259-261: "하락장은 예측할 수 없다". 그러므로 '나쁜 날을 피하자'는
    # 처방이 될 수 없다. 대신 나쁜 날의 손실이 **날 때문인지 고르기 때문인지**를
    # 가른다. 같은 날 안에서 (그날 전체 평균)과 (상한 N건 평균)을 비교하므로
    # 날 효과는 상쇄되고 선택 규칙의 기여만 남는다.
    print("\n── ★분해: 날 효과 vs 선택 효과 " + "─" * 39)
    print(f"  {'일자':>10} {'전체건':>5} {'전체평균':>9} {'상한평균':>9} "
          f"{'선택리프트':>10}")
    lifts = []
    cf_total = 0.0          # 반사실: 상한 N건이 '그날 평균 수준'만 뽑았다면
    for r in sorted(rows, key=lambda x: x["net"]):
        if not r["all_n"] or not r["n"]:
            continue
        all_avg = r["all_net"] / r["all_n"]
        cap_avg = r["net"] / r["n"]
        lift = cap_avg - all_avg
        lifts.append(lift)
        cf_total += all_avg * r["n"]
        print(f"  {r['date']:>10} {r['all_n']:>5} {all_avg:+8.3f}%p "
              f"{cap_avg:+8.3f}%p {lift:+9.3f}%p")
    if lifts:
        neg = sum(1 for x in lifts if x < 0)
        print(f"\n  선택 리프트 평균 {sum(lifts)/len(lifts):+.3f}%p/건 · "
              f"음(-)인 날 {neg}/{len(lifts)}일")
        print(f"  반사실 순누적(선택이 중립이었다면) {cf_total:+.2f}%p "
              f"vs 실제 {total:+.2f}%p → 선택 규칙 비용 {total - cf_total:+.2f}%p")
        # ★상한을 벗기면(=폭을 넓히면) 무엇이 남는가 — [F-21] 상한 구조 직결.
        #   ⚠ 이 합계는 '무제한 자금·무제한 체결' 가정이다. 7/30 F-21 슬롯 시뮬에서
        #   자금 제약을 넣으면 절반만 담긴다는 것이 이미 확인됐다(4층 규약·[F-32]).
        #   따라서 아래 숫자는 '신호 자체의 방향'을 보는 용도로만 인용할 것.
        a_n = sum(r["all_n"] for r in rows)
        a_net = sum(r["all_net"] for r in rows)
        pos_days = sum(1 for r in rows if r["all_n"] and r["all_net"] > 0)
        print(f"  [무제한 가정] 그날 전체 합산 {a_n}건 {a_net:+.2f}%p "
              f"(건당 {a_net/a_n if a_n else 0:+.3f}%p) · 전체가 플러스인 날 "
              f"{pos_days}/{len(rows)}일")

    print("\n── 판정 " + "─" * 63)
    if flip_at is not None:
        print(f"  · 최악 {flip_at}일만 빼면 순누적이 플러스로 전환된다.")
        print("    → 손실이 소수 날에 집중. '그 날'의 정체 규명이 다음 과제"
              "(레짐·급락·SL 해상도).")
    else:
        print(f"  · 하위 {args.top}일을 빼도 플러스 전환 실패 "
              f"→ 손실이 전 구간에 고르게 퍼져 있다. 구조적 미달 방향.")
    top1 = max(rows, key=lambda r: r["net"])
    print(f"  · 최대 수익일 {top1['date']} {top1['net']:+.2f}%p "
          f"= 플러스 총합의 {(top1['net']/sum_plus*100 if sum_plus else 0):.1f}%")
    print("  ※ 이 수치는 관측 전용. 라이브 flip 근거로 단독 인용 금지(관측 없이 flip 금지).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
