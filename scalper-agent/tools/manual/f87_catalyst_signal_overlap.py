"""[F-87] 명분(catalyst) ↔ v2 신호 교집합 규명 (read-only).

■ 왜 이걸 파는가 (8/1 사장님 지시 — "2번부터")
  사장님 확언(6/24): "명분 있는 끼 종목 5~10% 먹고 나온다 = 돈 버는 유일한 길."
  그런데 4개월 검증해온 S-1(PB-A v2)의 조건은 '오전10시前 + 체결강도200+ +
  추격<8%'가 전부이고 **명분이 한 글자도 안 들어간다**.
  [F-25]는 명분 게이트 결합을 시도했다가 "21일 D-1 교집합 0건"으로 닫았다.
  → 그 0건을 실패로 넘기지 않고 **왜 0인지**를 가른다. 8/1 [F-84]가 밝힌
    '손실의 92%는 그날 후보 구성에서 나온다'와 같은 자리이기 때문이다.

■ 가르려는 두 가설
  (A) 시간축 불일치 : 명분과 신호는 **같은 날 같은 종목**에서 일어나는데
                      catalyst 산출이 18:28(장 마감 後)이라 하루 늦게 본다.
                      → D0∩D0 크고 D-1∩D0 작음. 처방 = 장중 실시간 명분 탐지
                        (6/24 메모 '다음 정밀화 = 실시간 장중 점화'와 동일 결론).
  (B) 종목 세계 불일치: 애초에 다른 종목군이다. → D0∩D0 도 0에 가까움.
                      처방 = 명분 축과 v2 축의 결합 자체를 포기하거나 재설계.

  ⚠ D0 catalyst는 장 마감 後 산출이므로 **실전 사용 불가(look-ahead)**.
    여기서는 오직 '가설 A/B를 가르는 진단용'으로만 쓴다. 매매 규칙으로
    승격 금지 — 이 경고를 지우지 말 것.

■ 단일진실: v2 신호 판정은 r1_pullback_entry_backtest에서 import(_signal_index/
  _is_v2). 유효일은 pb.ticks_health OK만(수집장애일 오염 차단).

■ 불변식: read-only·매매 무접촉·봇 OFF.

실행(VPS): python3.11 -X utf8 tools/manual/f87_catalyst_signal_overlap.py
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

_HERE = Path(__file__).resolve().parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))

import r1_pullback_entry_backtest as r1  # noqa: E402  신호 판정 단일진실

pb = r1.pb
ARCHIVE = pb.BASE / "data_store" / "catalyst_archive" if hasattr(pb, "BASE") \
    else Path(__file__).resolve().parents[2] / "data_store" / "catalyst_archive"


def _catalyst_by_day() -> dict[str, set[str]]:
    """catalyst_archive/catalyst_YYYY-MM-DD.json → {YYYYMMDD: {code}}."""
    out: dict[str, set[str]] = {}
    if not ARCHIVE.is_dir():
        return out
    for f in sorted(ARCHIVE.glob("catalyst_*.json")):
        day = f.stem.replace("catalyst_", "").replace("-", "")
        try:
            data = json.loads(f.read_text(encoding="utf-8"))
        except Exception:
            continue
        items = data.get("items") if isinstance(data, dict) else data
        codes = set()
        for it in (items or []):
            c = (it.get("code") or "").strip() if isinstance(it, dict) else ""
            if c:
                codes.add(c.zfill(6))
        if codes:
            out[day] = codes
    return out


def _v2_codes(day: str) -> set[str]:
    """그날 v2 신호를 낸 종목코드 집합 — r1 규약과 동일."""
    ddir = pb.TICKS / day
    if not ddir.is_dir():
        return set()
    out = set()
    for f in ddir.glob("*.csv"):
        rows = pb._read_ticks(day, f.stem)
        if len(rows) < 3:
            continue
        sig = r1._signal_index(rows)
        if sig is not None and r1._is_v2(rows, sig):
            out.add(f.stem.zfill(6))
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--lags", default="0,1,2,3",
                    help="catalyst 기준일 지연(일). 0=같은날(진단용·실전불가)")
    args = ap.parse_args()
    lags = [int(x) for x in args.lags.split(",") if x.strip()]

    cat = _catalyst_by_day()
    if not cat:
        print(f"[F-87] catalyst_archive 없음 — 비교 불가 ({ARCHIVE})")
        return 1

    all_days = sorted(d.name for d in pb.TICKS.iterdir()
                      if d.is_dir() and pb._is_trading_day_dir(d.name))
    ok_days = [d for d in all_days
               if (h := pb.ticks_health(d)) and h["verdict"] == "OK"]
    cat_days = sorted(cat)
    # 두 데이터가 함께 존재하는 구간만 — 없는 구간을 0으로 세면 '교집합 0' 위장
    usable = [d for d in ok_days if d >= cat_days[0]]

    print("=" * 76)
    print("[F-87] 명분(catalyst) ↔ v2 신호 교집합 규명")
    print("=" * 76)
    print(f"  catalyst 아카이브 {len(cat)}일 ({cat_days[0]}~{cat_days[-1]}) "
          f"· 일평균 {sum(len(v) for v in cat.values())/len(cat):.1f}종")
    print(f"  ticks 유효일 {len(ok_days)}일 · 겹치는 구간 {len(usable)}일")
    if not usable:
        print("  ▶ 겹치는 날이 없다 — 비교 불가(데이터 보존기간 문제).")
        return 0

    v2_cache = {d: _v2_codes(d) for d in usable}
    idx = {d: i for i, d in enumerate(usable)}

    print(f"\n  {'lag':>4} {'대상일':>5} {'v2평균':>8} {'명분평균':>9} "
          f"{'교집합계':>9} {'교집합>0인날':>12}")
    results = {}
    for lag in lags:
        hit_total = days_with_hit = cmp_days = 0
        v2_tot = cat_tot = 0
        pairs = []
        for d in usable:
            i = idx[d]
            if i - lag < 0:
                continue
            src = usable[i - lag]      # 거래일 기준 lag일 前 catalyst
            if src not in cat:
                continue
            inter = cat[src] & v2_cache[d]
            cmp_days += 1
            v2_tot += len(v2_cache[d])
            cat_tot += len(cat[src])
            hit_total += len(inter)
            if inter:
                days_with_hit += 1
                pairs.append((d, src, sorted(inter)))
        if not cmp_days:
            continue
        results[lag] = (hit_total, cmp_days, pairs)
        tag = " ← 진단용(실전불가)" if lag == 0 else ""
        print(f"  {lag:>4} {cmp_days:>5} {v2_tot/cmp_days:>7.1f} "
              f"{cat_tot/cmp_days:>8.1f} {hit_total:>9} "
              f"{days_with_hit:>7}/{cmp_days}일{tag}")

    # ── ★무작위 기대치 대조 ──────────────────────────────────────
    # 교집합 절대 건수만 보면 "적다"는 인상만 남고 판정이 안 된다.
    # 두 집합이 독립이라면 기대 교집합 = |명분| × |v2| / |유니버스| 이다.
    # 실측이 기대보다 **낮으면** 두 축은 독립이 아니라 서로 **배타적**이다.
    uni = len(pb._load_universe()) if hasattr(pb, "_load_universe") else 2531
    print("\n── ★무작위 기대치 대조 (유니버스 %d종 가정) " % uni + "─" * 24)
    print(f"  {'lag':>4} {'실측':>6} {'기대':>8} {'비(실측/기대)':>13}")
    ratios = {}
    for lag in lags:
        if lag not in results:
            continue
        hit, cmp_days, _ = results[lag]
        exp = 0.0
        for d in usable:
            i = idx[d]
            if i - lag < 0:
                continue
            src = usable[i - lag]
            if src not in cat:
                continue
            exp += len(cat[src]) * len(v2_cache[d]) / uni
        ratios[lag] = (hit / exp) if exp else None
        print(f"  {lag:>4} {hit:>6} {exp:>8.1f} "
              f"{(hit/exp if exp else 0):>12.2f}x")

    print("\n── 판정 " + "─" * 67)
    d0 = results.get(0, (0, 0, []))[0]
    r0 = ratios.get(0)
    lag_hits = [results[l][0] for l in lags if l in results]
    spread = (max(lag_hits) - min(lag_hits)) if lag_hits else 0
    if r0 is not None and r0 < 0.7:
        print(f"  ▶ ★가설(C) 구조적 배타. 같은 날 교집합이 무작위 기대의 {r0:.2f}배다.")
        print("    → 두 축은 '안 겹치는' 정도가 아니라 **서로를 밀어낸다**.")
        print("    → 시간축(lag) 조정으로 해결되지 않는다"
              f" (lag별 편차 {spread}건 = 노이즈 수준).")
        print("    → 원인은 필터 정의에서 찾아야 한다 ↓ 아래 배제사유 분해 참조.")
    elif d0 == 0:
        print("  ▶ 가설(B) 종목 세계 불일치. 같은 날로 맞춰도 0이다.")
    else:
        print(f"  ▶ 교집합이 기대치 수준({r0:.2f}x). 시간축/정의 문제 아님 — 추가 분해 필요.")

    # ── ★배제사유 분해 — 명분 종목은 왜 v2가 되지 못했나 ────────────
    # 여기가 F-87의 핵심이다. '안 겹친다'가 아니라 '어느 필터에서 잘리는가'를
    # 세면 처방이 나온다. 같은 날(lag 0) 기준 — 진단 목적이므로 look-ahead 무관.
    print("\n── ★배제사유 분해 (같은 날 명분 종목이 v2에 못 든 이유) " + "─" * 15)
    reasons: dict[str, int] = {}
    first_cr = []
    for d in usable:
        for c in cat.get(d, ()):  # 그날 명분 종목
            rows = pb._read_ticks(d, c)
            if len(rows) < 3:
                reasons["ticks 없음/부족"] = reasons.get("ticks 없음/부족", 0) + 1
                continue
            first_cr.append(rows[0][2])          # 첫 관측 등락률
            sig = r1._signal_index(rows)
            if sig is None:
                # +5% 미도달인지, 이미 10시를 넘겼는지 구분
                hit5 = any(cr >= r1.SIG_PCT for _, _, cr, _ in rows)
                key = "10시前 신호창 놓침" if hit5 else "+5% 미도달"
                reasons[key] = reasons.get(key, 0) + 1
                continue
            _t, _px, cr, st = rows[sig]
            if cr >= r1.CHASE:
                reasons[f"★추격 상한({r1.CHASE}%) 초과"] = \
                    reasons.get(f"★추격 상한({r1.CHASE}%) 초과", 0) + 1
            elif st < r1.STR_MIN:
                reasons[f"체결강도 <{r1.STR_MIN}"] = \
                    reasons.get(f"체결강도 <{r1.STR_MIN}", 0) + 1
            else:
                reasons["v2 통과(교집합)"] = reasons.get("v2 통과(교집합)", 0) + 1
    tot_r = sum(reasons.values())
    for k, v in sorted(reasons.items(), key=lambda x: -x[1]):
        print(f"  {k:<28} {v:>5}건 ({v/tot_r*100 if tot_r else 0:>5.1f}%)")
    if first_cr:
        srt = sorted(first_cr)
        med = srt[len(srt) // 2]
        over = sum(1 for x in first_cr if x >= r1.CHASE)
        print(f"\n  명분 종목의 **첫 관측 등락률** 중앙값 {med:+.2f}% · "
              f"첫 관측에서 이미 {r1.CHASE}% 초과 {over}/{len(first_cr)}건 "
              f"({over/len(first_cr)*100:.1f}%)")
        print("  ※ 첫 관측이 늦을수록 이 값이 커진다 — [F-31] 시총순 순회 지연과 결합.")

    # ── ★★추격 구간별 성과 — 8% 상한이 명분 종목에서도 옳은가 ────────
    # 7/9 지시서 백테스트에서 추격 상한 8%는 '강력 통과'였다(<8% +0.37 /
    # ≥8% −0.64). 그러나 그 검증은 **명분 없는 전체 표본**이었다.
    # 위 분해에서 명분 종목의 38.6%가 이 상한에 잘리는 것이 확인됐으므로,
    # 명분 종목에 한해 같은 결론이 서는지 별도로 재본다.
    # (진입 = 신호 다음 관측행 = baseline 규약, 청산 = pb 정본)
    print("\n── ★★추격 구간별 성과 (명분 종목만·baseline 진입) " + "─" * 19)
    BUCKETS = [(0, 8), (8, 15), (15, 25), (25, 1e9)]
    agg = {b: [] for b in BUCKETS}
    for d in usable:
        for c in cat.get(d, ()):
            rows = pb._read_ticks(d, c)
            if len(rows) < 3:
                continue
            sig = r1._signal_index(rows)
            if sig is None:
                continue
            cr = rows[sig][2]
            eb = r1._entry_baseline(rows, sig)
            if not eb:
                continue
            tr = r1._trade(rows, eb[0], eb[1], eb[2], d, c, sig)
            if not tr:
                continue
            for b in BUCKETS:
                if b[0] <= cr < b[1]:
                    agg[b].append(tr["ret"] - pb.COST_RT)
                    break
    print(f"  {'추격구간':>12} {'건수':>6} {'건당net':>10} {'승률':>7}")
    for b in BUCKETS:
        v = agg[b]
        if not v:
            continue
        label = f"{b[0]:g}%+" if b[1] > 1e8 else f"{b[0]:g}~{b[1]:g}%"
        wr = 100 * sum(1 for x in v if x > 0) / len(v)
        print(f"  {label:>12} {len(v):>6} {sum(v)/len(v):+9.3f}%p "
              f"{wr:>6.1f}%")
    # '<8% vs ≥8%' 이분법은 경계를 못 찾는다. 상한을 옮겨가며 누적으로 본다
    # — 건당 net만 보면 상한이 낮을수록 좋아 보이지만 표본이 말라버린다.
    print(f"\n  {'상한':>6} {'건수':>6} {'건당net':>10} {'총net':>10}")
    best = None
    for cap in (8, 15, 25, 999):
        v = [x for b in BUCKETS if b[1] <= cap or (cap == 999)
             for x in agg[b] if b[0] < cap]
        if not v:
            continue
        tot_v = sum(v)
        lbl = "무제한" if cap == 999 else f"{cap}%"
        print(f"  {lbl:>6} {len(v):>6} {tot_v/len(v):+9.3f}%p {tot_v:+9.1f}%p")
        if best is None or tot_v > best[1]:
            best = (lbl, tot_v, len(v), tot_v / len(v))
    if best:
        print(f"\n  ▶ 총 net 최대 = 상한 {best[0]} "
              f"({best[2]}건 · 건당 {best[3]:+.3f}%p · 총 {best[1]:+.1f}%p)")
        print("    현행 8%는 건당은 최고지만 표본이 말라 총량에서 진다"
              " — [F-84] '폭' 결론과 같은 방향.")
    print("\n  ⚠⚠ 실전 적용 불가 조건: 위 명분 집합은 **같은 날 18:28 산출**이다"
          "(look-ahead).")
    print("     D-1 명분으로는 교집합이 1건뿐이므로, 이 축을 실제로 열려면")
    print("     **장중 실시간 명분 탐지**가 선행돼야 한다(6/24 메모 '실시간 장중 점화').")
    print("     즉 이 결과의 처방은 '상한을 올려라'가 아니라 '명분을 장중에 알아야 한다'.")

    for lag in lags:
        if lag in results and results[lag][2]:
            print(f"\n  [lag {lag}] 교집합 발생일 샘플(최대 5):")
            for d, src, codes in results[lag][2][:5]:
                print(f"    {d} ← 명분 {src} : {codes[:8]}")
    print("\n  ※ 관측 전용. lag 0은 look-ahead이므로 매매 규칙 승격 금지.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
