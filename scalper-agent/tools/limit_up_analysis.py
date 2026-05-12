# -*- coding: utf-8 -*-
"""
상한가 후속 성과 분석 -성공 vs 실패 패턴 비교
================================================
history.json (613건) + signal_history.json (수급 데이터) 결합 분석

목적:
  1. 10%+ 도달 종목 vs 미도달 종목의 패턴 차이
  2. 과열도/수급등급/continuation_score 별 성과
  3. 시가총액/섹터별 성과
  4. 현재 필터 로직(overheat>150, survival_ok, flow_grade) 검증
"""
import json
import sys
import io
from pathlib import Path
from collections import defaultdict

# Windows cp949 인코딩 에러 방지
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

BASE = Path(__file__).resolve().parents[1]
HISTORY = BASE / "data_store" / "limit_up" / "history.json"
SIGNAL_HISTORY = BASE / "data_store" / "limit_up" / "signal_history.json"
WATCHLIST = BASE / "data_store" / "limit_up" / "watchlist.json"


def load():
    with open(HISTORY, "r", encoding="utf-8") as f:
        hist = json.load(f)
    events = hist.get("events", [])

    # signal_history + watchlist에서 수급 데이터 로드 (code+date / code로 매칭)
    flow_map = {}
    if SIGNAL_HISTORY.exists():
        with open(SIGNAL_HISTORY, "r", encoding="utf-8") as f:
            sig_hist = json.load(f)
        for sig in sig_hist.get("signals", []):
            # code+date 키
            key_dated = f"{sig['code']}_{sig.get('signal_date', '')}"
            flow_map[key_dated] = sig
            # code만으로도 매칭 (fallback)
            flow_map[sig['code']] = sig

    # watchlist에서도 수급 데이터 보충
    if WATCHLIST.exists():
        with open(WATCHLIST, "r", encoding="utf-8") as f:
            wl = json.load(f)
        for item in wl.get("items", []):
            code = item.get("code", "")
            if code and code not in flow_map:
                flow_map[code] = item
            key_dated = f"{code}_{item.get('signal_date', '')}"
            if key_dated not in flow_map:
                flow_map[key_dated] = item

    return events, flow_map, hist.get("summary", {})


def analyze():
    events, flow_map, summary = load()
    total = len(events)

    print(f"{'=' * 70}")
    print(f"  상한가 후속 성과 분석 ({total}건)")
    print(f"  기간: {summary.get('period_days', '?')}일")
    print(f"{'=' * 70}")

    # ── 기본 통계 ──
    winners = [e for e in events if e.get("reached_10pct")]
    losers = [e for e in events if not e.get("reached_10pct")]
    print(f"\n[기본 통계]")
    print(f"  10%+ 도달: {len(winners)}건 ({len(winners)/total*100:.1f}%)")
    print(f"  미도달:    {len(losers)}건 ({len(losers)/total*100:.1f}%)")
    print(f"  전체 평균 max_gain: {sum(e.get('max_gain_after',0) for e in events)/total:+.1f}%")
    print(f"  전체 평균 max_drop: {sum(e.get('max_drop_after',0) for e in events)/total:+.1f}%")
    if winners:
        print(f"  성공 평균 max_gain: {sum(e.get('max_gain_after',0) for e in winners)/len(winners):+.1f}%")
    if losers:
        print(f"  실패 평균 max_drop: {sum(e.get('max_drop_after',0) for e in losers)/len(losers):+.1f}%")

    # ── 1. continuation_score 구간별 ──
    print(f"\n{'─' * 70}")
    print(f"[1] Continuation Score 구간별 성과")
    print(f"  {'구간':>10s}  {'건수':>5s}  {'10%+':>6s}  {'승률':>6s}  {'평균gain':>9s}  {'평균drop':>9s}")
    score_bins = [(0, 20), (20, 40), (40, 60), (60, 80), (80, 101)]
    for lo, hi in score_bins:
        grp = [e for e in events if lo <= e.get("continuation_score", 0) < hi]
        if not grp:
            continue
        wins = sum(1 for e in grp if e.get("reached_10pct"))
        avg_gain = sum(e.get("max_gain_after", 0) for e in grp) / len(grp)
        avg_drop = sum(e.get("max_drop_after", 0) for e in grp) / len(grp)
        label = f"{lo}~{hi-1}" if hi < 101 else f"{lo}~100"
        print(f"  {label:>10s}  {len(grp):>5d}  {wins:>6d}  {wins/len(grp)*100:>5.1f}%  {avg_gain:>+8.1f}%  {avg_drop:>+8.1f}%")

    # ── 2. 시가총액 구간별 ──
    print(f"\n{'─' * 70}")
    print(f"[2] 시가총액 구간별 성과")
    print(f"  {'구간':>12s}  {'건수':>5s}  {'10%+':>6s}  {'승률':>6s}  {'평균gain':>9s}  {'평균drop':>9s}")
    cap_bins = [(0, 500), (500, 1000), (1000, 3000), (3000, 10000), (10000, 999999)]
    cap_labels = ["~500억", "500~1천억", "1천~3천억", "3천~1만억", "1만억+"]
    for (lo, hi), label in zip(cap_bins, cap_labels):
        grp = [e for e in events if lo <= e.get("cap_억", 0) < hi]
        if not grp:
            continue
        wins = sum(1 for e in grp if e.get("reached_10pct"))
        avg_gain = sum(e.get("max_gain_after", 0) for e in grp) / len(grp)
        avg_drop = sum(e.get("max_drop_after", 0) for e in grp) / len(grp)
        print(f"  {label:>12s}  {len(grp):>5d}  {wins:>6d}  {wins/len(grp)*100:>5.1f}%  {avg_gain:>+8.1f}%  {avg_drop:>+8.1f}%")

    # ── 3. 섹터별 ──
    print(f"\n{'─' * 70}")
    print(f"[3] 섹터별 성과 (5건 이상)")
    sector_stats = defaultdict(list)
    for e in events:
        sector_stats[e.get("sector", "기타")].append(e)

    print(f"  {'섹터':>12s}  {'건수':>5s}  {'10%+':>6s}  {'승률':>6s}  {'평균gain':>9s}  {'평균drop':>9s}")
    for sector, grp in sorted(sector_stats.items(), key=lambda x: -len(x[1])):
        if len(grp) < 5:
            continue
        wins = sum(1 for e in grp if e.get("reached_10pct"))
        avg_gain = sum(e.get("max_gain_after", 0) for e in grp) / len(grp)
        avg_drop = sum(e.get("max_drop_after", 0) for e in grp) / len(grp)
        print(f"  {sector:>12s}  {len(grp):>5d}  {wins:>6d}  {wins/len(grp)*100:>5.1f}%  {avg_gain:>+8.1f}%  {avg_drop:>+8.1f}%")

    # ── 4. 상한가 당일 등락률(change_pct) 구간별 ──
    print(f"\n{'─' * 70}")
    print(f"[4] 상한가 당일 등락률 구간별")
    print(f"  {'구간':>12s}  {'건수':>5s}  {'10%+':>6s}  {'승률':>6s}  {'평균gain':>9s}")
    chg_bins = [(10, 20), (20, 25), (25, 30), (30, 31)]
    chg_labels = ["10~20%", "20~25%", "25~30%", "30%(상한)"]
    for (lo, hi), label in zip(chg_bins, chg_labels):
        grp = [e for e in events if lo <= e.get("change_pct", 0) < hi]
        if not grp:
            continue
        wins = sum(1 for e in grp if e.get("reached_10pct"))
        avg_gain = sum(e.get("max_gain_after", 0) for e in grp) / len(grp)
        print(f"  {label:>12s}  {len(grp):>5d}  {wins:>6d}  {wins/len(grp)*100:>5.1f}%  {avg_gain:>+8.1f}%")

    # ── 5. D+1 거래량 비율 vs 성과 ──
    print(f"\n{'─' * 70}")
    print(f"[5] D+1 거래량 비율 vs 성과")
    d1_events = []
    for e in events:
        pds = e.get("post_days", [])
        if pds and len(pds) > 0:
            d1_vol = pds[0].get("vol_ratio", 0)
            d1_events.append((e, d1_vol))

    print(f"  {'거래량비율':>12s}  {'건수':>5s}  {'10%+':>6s}  {'승률':>6s}  {'평균gain':>9s}")
    vol_bins = [(0, 1), (1, 3), (3, 10), (10, 50), (50, 9999)]
    vol_labels = ["<1x", "1~3x", "3~10x", "10~50x", "50x+"]
    for (lo, hi), label in zip(vol_bins, vol_labels):
        grp = [(e, v) for e, v in d1_events if lo <= v < hi]
        if not grp:
            continue
        wins = sum(1 for e, v in grp if e.get("reached_10pct"))
        avg_gain = sum(e.get("max_gain_after", 0) for e, v in grp) / len(grp)
        print(f"  {label:>12s}  {len(grp):>5d}  {wins:>6d}  {wins/len(grp)*100:>5.1f}%  {avg_gain:>+8.1f}%")

    # ── 6. 수급 데이터 분석 (signal_history 매칭) ──
    matched = 0
    flow_events = []
    for e in events:
        key_dated = f"{e['code']}_{e.get('limit_date', '')}"
        key_code = e['code']
        sig = flow_map.get(key_dated) or flow_map.get(key_code)
        if sig and sig.get("flow_grade"):
            flow_events.append((e, sig))
            matched += 1

    print(f"\n{'─' * 70}")
    print(f"[6] 수급 데이터 분석 (매칭: {matched}/{total}건)")

    if flow_events:
        # 6a. flow_grade별
        print(f"\n  [6a] 수급등급별 성과")
        print(f"  {'등급':>8s}  {'건수':>5s}  {'10%+':>6s}  {'승률':>6s}  {'평균gain':>9s}  {'평균drop':>9s}")
        grade_stats = defaultdict(list)
        for e, sig in flow_events:
            grade = sig.get("flow_grade", "?")
            grade_stats[grade].append(e)

        for grade in ["A", "B", "C", "D", "?"]:
            grp = grade_stats.get(grade, [])
            if not grp:
                continue
            wins = sum(1 for e in grp if e.get("reached_10pct"))
            avg_gain = sum(e.get("max_gain_after", 0) for e in grp) / len(grp)
            avg_drop = sum(e.get("max_drop_after", 0) for e in grp) / len(grp)
            print(f"  {grade:>8s}  {len(grp):>5d}  {wins:>6d}  {wins/len(grp)*100:>5.1f}%  {avg_gain:>+8.1f}%  {avg_drop:>+8.1f}%")

        # 6b. survival_ok별
        print(f"\n  [6b] 수급생존 여부별 성과")
        print(f"  {'생존':>8s}  {'건수':>5s}  {'10%+':>6s}  {'승률':>6s}  {'평균gain':>9s}  {'평균drop':>9s}")
        for survival_val, label in [(True, "True"), (False, "False")]:
            grp = [e for e, sig in flow_events if sig.get("survival_ok") is survival_val]
            if not grp:
                continue
            wins = sum(1 for e in grp if e.get("reached_10pct"))
            avg_gain = sum(e.get("max_gain_after", 0) for e in grp) / len(grp)
            avg_drop = sum(e.get("max_drop_after", 0) for e in grp) / len(grp)
            print(f"  {label:>8s}  {len(grp):>5d}  {wins:>6d}  {wins/len(grp)*100:>5.1f}%  {avg_gain:>+8.1f}%  {avg_drop:>+8.1f}%")

        # 6c. overheat_pct 구간별
        print(f"\n  [6c] 과열도(overheat_pct) 구간별 성과")
        print(f"  {'과열도':>12s}  {'건수':>5s}  {'10%+':>6s}  {'승률':>6s}  {'평균gain':>9s}  {'평균drop':>9s}")
        oh_bins = [(0, 30), (30, 60), (60, 100), (100, 150), (150, 200), (200, 9999)]
        oh_labels = ["0~30%", "30~60%", "60~100%", "100~150%", "150~200%", "200%+"]
        for (lo, hi), label in zip(oh_bins, oh_labels):
            grp = [(e, sig) for e, sig in flow_events
                   if lo <= (sig.get("overheat_pct", 0) or 0) < hi]
            if not grp:
                continue
            wins = sum(1 for e, s in grp if e.get("reached_10pct"))
            avg_gain = sum(e.get("max_gain_after", 0) for e, s in grp) / len(grp)
            avg_drop = sum(e.get("max_drop_after", 0) for e, s in grp) / len(grp)
            print(f"  {label:>12s}  {len(grp):>5d}  {wins:>6d}  {wins/len(grp)*100:>5.1f}%  {avg_gain:>+8.1f}%  {avg_drop:>+8.1f}%")

        # 6d. dual_buy (외인+기관 쌍매수) 여부별
        print(f"\n  [6d] 외인+기관 쌍매수(dual_buy) 여부별 성과")
        print(f"  {'쌍매수':>8s}  {'건수':>5s}  {'10%+':>6s}  {'승률':>6s}  {'평균gain':>9s}  {'평균drop':>9s}")
        for dual_val, label in [(True, "True"), (False, "False")]:
            grp = [e for e, sig in flow_events if sig.get("dual_buy") is dual_val]
            if not grp:
                continue
            wins = sum(1 for e in grp if e.get("reached_10pct"))
            avg_gain = sum(e.get("max_gain_after", 0) for e in grp) / len(grp)
            avg_drop = sum(e.get("max_drop_after", 0) for e in grp) / len(grp)
            print(f"  {label:>8s}  {len(grp):>5d}  {wins:>6d}  {wins/len(grp)*100:>5.1f}%  {avg_gain:>+8.1f}%  {avg_drop:>+8.1f}%")

        # 6e. sell_exhaustion 구간별
        print(f"\n  [6e] 매도소진율(sell_exhaustion) 구간별")
        print(f"  {'소진율':>12s}  {'건수':>5s}  {'10%+':>6s}  {'승률':>6s}  {'평균gain':>9s}")
        se_bins = [(0, 0.5), (0.5, 1.0), (1.0, 2.0), (2.0, 5.0), (5.0, 999)]
        se_labels = ["<0.5", "0.5~1.0", "1.0~2.0", "2.0~5.0", "5.0+"]
        for (lo, hi), label in zip(se_bins, se_labels):
            grp = [(e, sig) for e, sig in flow_events
                   if lo <= (sig.get("sell_exhaustion", 0) or 0) < hi]
            if not grp:
                continue
            wins = sum(1 for e, s in grp if e.get("reached_10pct"))
            avg_gain = sum(e.get("max_gain_after", 0) for e, s in grp) / len(grp)
            print(f"  {label:>12s}  {len(grp):>5d}  {wins:>6d}  {wins/len(grp)*100:>5.1f}%  {avg_gain:>+8.1f}%")

    # ── 7. 현재 필터 시뮬레이션 ──
    print(f"\n{'─' * 70}")
    print(f"[7] 필터 시뮬레이션 -현재 로직 적용 시")
    if flow_events:
        # 현재 필터: overheat <= 150, survival_ok != False, flow_grade not in C/D
        passed = []
        filtered_out = []
        for e, sig in flow_events:
            oh = sig.get("overheat_pct", 0) or 0
            surv = sig.get("survival_ok")
            fg = sig.get("flow_grade", "")
            if oh > 150 or surv is False or fg in ("C", "D"):
                filtered_out.append((e, sig))
            else:
                passed.append((e, sig))

        print(f"\n  통과: {len(passed)}건 | 필터링: {len(filtered_out)}건")

        if passed:
            p_wins = sum(1 for e, s in passed if e.get("reached_10pct"))
            p_gain = sum(e.get("max_gain_after", 0) for e, s in passed) / len(passed)
            p_drop = sum(e.get("max_drop_after", 0) for e, s in passed) / len(passed)
            print(f"  통과 그룹: 승률 {p_wins/len(passed)*100:.1f}% | 평균gain {p_gain:+.1f}% | 평균drop {p_drop:+.1f}%")

        if filtered_out:
            f_wins = sum(1 for e, s in filtered_out if e.get("reached_10pct"))
            f_gain = sum(e.get("max_gain_after", 0) for e, s in filtered_out) / len(filtered_out)
            f_drop = sum(e.get("max_drop_after", 0) for e, s in filtered_out) / len(filtered_out)
            print(f"  필터링 그룹: 승률 {f_wins/len(filtered_out)*100:.1f}% | 평균gain {f_gain:+.1f}% | 평균drop {f_drop:+.1f}%")

        # 필터링된 종목 상세
        print(f"\n  필터링된 종목 상세:")
        for e, sig in filtered_out:
            oh = sig.get("overheat_pct", 0) or 0
            surv = sig.get("survival_ok")
            fg = sig.get("flow_grade", "")
            reasons = []
            if oh > 150:
                reasons.append(f"과열{oh:.0f}%")
            if surv is False:
                reasons.append("생존실패")
            if fg in ("C", "D"):
                reasons.append(f"수급{fg}")
            result = "O" if e.get("reached_10pct") else "X"
            print(f"    {e['name']:12s} {e.get('limit_date','')} [{result}] "
                  f"gain:{e.get('max_gain_after',0):+.1f}% drop:{e.get('max_drop_after',0):+.1f}% "
                  f"→ {', '.join(reasons)}")
    else:
        print("  수급 데이터 매칭 없음 -history.json만으로 분석")

    # ── 8. 결론: continuation_score 기반 필터 효과 ──
    print(f"\n{'─' * 70}")
    print(f"[8] Continuation Score 60+ 필터 효과")
    high_score = [e for e in events if e.get("continuation_score", 0) >= 60]
    low_score = [e for e in events if e.get("continuation_score", 0) < 60]
    if high_score:
        hs_wins = sum(1 for e in high_score if e.get("reached_10pct"))
        hs_gain = sum(e.get("max_gain_after", 0) for e in high_score) / len(high_score)
        hs_drop = sum(e.get("max_drop_after", 0) for e in high_score) / len(high_score)
        print(f"  Score 60+: {len(high_score)}건 | 승률 {hs_wins/len(high_score)*100:.1f}% | gain {hs_gain:+.1f}% | drop {hs_drop:+.1f}%")
    if low_score:
        ls_wins = sum(1 for e in low_score if e.get("reached_10pct"))
        ls_gain = sum(e.get("max_gain_after", 0) for e in low_score) / len(low_score)
        ls_drop = sum(e.get("max_drop_after", 0) for e in low_score) / len(low_score)
        print(f"  Score <60: {len(low_score)}건 | 승률 {ls_wins/len(low_score)*100:.1f}% | gain {ls_gain:+.1f}% | drop {ls_drop:+.1f}%")

    print(f"\n{'=' * 70}")
    print(f"  분석 완료")
    print(f"{'=' * 70}")


if __name__ == "__main__":
    analyze()
