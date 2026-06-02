# -*- coding: utf-8 -*-
"""매매스타일 비교 — 물타기(분할매수) vs 손절+추격, 수익금(절대₩) 기준 (6/2 사장님 질문).

사장님 6/2: "오늘 같은 조정장(지수는 괜찮아도 개별종목은 박살)에서,
  ① 떨어질 때 -X%마다 분할매수 → 천장-3~5% 트레일 매도 (물타기형)
  ② 떨어지면 -3~5% 칼손절, 오를 때 추격 → 천장 트레일 매도 (손절+추격형)
  둘 중 수익금(수익률X)이 좋은 건?" → 변형 3개씩 전부 측정.

★ 핵심: 수익률 아니라 **수익금(절대₩)**. 캠페인당 동일 작업자본 B로, 누가 더 번 ₩인가.
  물타기는 평단↓로 같은 B가 더 많은 수량 → 회복하면 큰 수익금 but 안 돌아오면 큰 손실.

캠페인(조정에 물린 종목) = 종가가 직전 20일 고점 대비 -DROP%↓ 진입한 날(개별종목 박살 = 조정장 프록시).

스타일1(물타기, 약할때 사모음) 3변형:
  S1a -3/-6/-9% 3분할 균등, 손절X
  S1b -5/-10/-15% 피라미드(1:2:3 비중), 손절X
  S1c S1a + 평단 -12% 넘으면 전량손절(안전판, 리버모어 'never average a loss' 절충)
스타일2(손절+추격, 지는놈 자르고 이기는놈 따라) 3변형 (진입=상승전환 확인):
  S2a 최근10일 고가 돌파시 진입
  S2b 최근저점 +3% 반등시 진입
  S2c 종가가 20일이평 상향돌파시 진입
  공통: 진입 -STOP% 손절, 재진입 허용, 천장 -TRAIL% 매도

공통: 천장 -TRAIL% 트레일 매도 / 캠페인 MAX일 타임아웃 종가청산 / 자본 B / 비용 COST/leg.
데이터: stock_data_daily 일봉. --selftest(데이터 불요) / --data DIR.
한계(어제 교훈): 일봉(장중 순서·트레일 휩쏘 과소)·생존편향·단일 trail값 → 절대₩ 과대,
  ★두 스타일 '상대 수익금 비교'가 신뢰선★. 비용·세금 COST 반영.
"""
from __future__ import annotations
import argparse
import csv
import glob
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
DAILY_DEFAULT = ROOT / "stock_data_daily"

LOOKBACK = 20            # 직전 고점 산출 윈도
DROP = 0.10              # 20일고점 대비 -10%↓ = 조정 진입(캠페인 트리거)
TRAIL = 0.04             # 천장 -4% 트레일 매도 (사장님 3~5% 중앙; --trail로 변경)
STOP = 0.04              # 스타일2 손절 -4% / S1c는 평단 -12%
S1C_MAXLOSS = 0.12       # S1c 평단대비 최대손실 컷
CAMP_MAX = 60            # 캠페인 최대 보유(거래일)
CAPITAL = 10_000_000     # 캠페인당 작업자본 B (수익금 ₩ 기준)
COST = 0.0048            # 왕복 비용(매수+매도 슬립+세금)


def _load(path):
    """일봉 CSV → rows[(o,h,l,c,v)]. utf-8-sig+영문 OHLCV (어제 헤더교훈)."""
    rows = []
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
    except Exception:
        return []
    return rows


def _campaigns(rows):
    """조정 진입일 인덱스 + 직전고점 H. 종가가 20일고점 -DROP%↓ & 전일은 비조정(START만).
    겹침 방지: 직전 캠페인 종료(+CAMP_MAX) 이후만 새 캠페인."""
    out = []
    n = len(rows)
    last_end = -1
    for i in range(LOOKBACK + 1, n - 2):       # +1: i-1 윈도 언더플로우 방지
        if i <= last_end:
            continue
        H = max(rows[j][1] for j in range(i - LOOKBACK, i))     # 직전 20일 고가
        Hp = max(rows[j][1] for j in range(i - 1 - LOOKBACK, i - 1))
        in_corr = rows[i][3] <= H * (1 - DROP)
        prev_corr = rows[i - 1][3] <= Hp * (1 - DROP)
        if in_corr and not prev_corr:
            out.append((i, H))
            last_end = i + CAMP_MAX
    return out


# ───────────────────────── 스타일1 (물타기) ─────────────────────────

def _style1(rows, ci, H, variant):
    """물타기. 떨어질 때 분할매수+버티기, 회복(천장) 후에만 -TRAIL% 트레일 매도.
    S1c만 평단 -S1C_MAXLOSS 안전손절. variant a/b/c. 반환 (수익금₩, 1, 투입₩)."""
    n = len(rows)
    end = min(ci + CAMP_MAX, n - 1)
    if variant == "b":
        levels = [(0.05, 1), (0.10, 2), (0.15, 3)]    # (-%, 비중) 피라미드
    else:  # a, c
        levels = [(0.03, 1), (0.06, 1), (0.09, 1)]    # 균등
    wsum = sum(w for _, w in levels)
    legs = []   # (price, cash)
    li = 0
    peak = 0.0
    for d in range(ci, end + 1):
        o, h, l, c, v = rows[d]
        # 1) 분할 체결 (레벨 터치)
        while li < len(levels):
            pct, w = levels[li]
            lvl = H * (1 - pct)
            if l <= lvl:
                legs.append((lvl, CAPITAL * w / wsum))
                if peak == 0.0:
                    peak = c            # 첫 진입일 종가=초기 peak
                li += 1
            else:
                break
        if not legs:
            continue
        spent = sum(cash for _, cash in legs)
        sh = sum(cash / p for p, cash in legs)
        avg = spent / sh
        peak = max(peak, h)
        # 2) S1c 안전손절: 평단 -12% (S1a/b는 없음 — 버티기)
        if variant == "c" and l <= avg * (1 - S1C_MAXLOSS):
            px = avg * (1 - S1C_MAXLOSS)
            return sh * px - spent - spent * COST, 1, spent
        # 3) 트레일: ★+TRAIL% 이상 회복(천장)한 뒤에만 무장★ = 버티다 천장-TRAIL 매도
        #    avg×(1+TRAIL): 진입일 고가가 평단 살짝 넘는 인공발동 방지 = 진짜 천장만
        if peak >= avg * (1 + TRAIL) and l <= peak * (1 - TRAIL):
            px = peak * (1 - TRAIL)
            return sh * px - spent - spent * COST, 1, spent
    # 4) 타임아웃 종가청산 (안 돌아오면 큰 손실 = 물타기 꼬리위험)
    if not legs:
        return 0.0, 0, 0.0
    spent = sum(cash for _, cash in legs)
    sh = sum(cash / p for p, cash in legs)
    return sh * rows[end][3] - spent - spent * COST, 1, spent


# ───────────────────────── 스타일2 (손절+추격) ─────────────────────────

def _chase_entry(rows, d, variant, lo_since):
    """d일에 추격 진입 신호? a=10일고가돌파 b=저점+3% c=20일이평상향돌파."""
    o, h, l, c, v = rows[d]
    if variant == "a":
        if d < 10:
            return None
        hi10 = max(rows[j][1] for j in range(d - 10, d))
        return hi10 if h >= hi10 else None         # 돌파레벨 체결
    if variant == "b":
        if lo_since <= 0:
            return None
        lvl = lo_since * 1.03
        return lvl if h >= lvl else None
    if variant == "c":
        if d < 20:
            return None
        ma = sum(rows[j][3] for j in range(d - 20, d)) / 20
        pc = rows[d - 1][3]
        return c if (pc < ma and c >= ma) else None    # 상향돌파 종가진입
    return None


def _style2(rows, ci, H, variant):
    """손절+추격. 캠페인 내 추격진입→ -STOP 손절 or 천장트레일. 재진입 허용.
    반환 (수익금합₩, 라운드트립수, 누적투입₩=B기준 회전)."""
    n = len(rows)
    end = min(ci + CAMP_MAX, n - 1)
    total = 0.0
    trips = 0
    turnover = 0.0
    d = ci
    lo_since = rows[ci][3]
    while d <= end:
        o, h, l, c, v = rows[d]
        lo_since = min(lo_since, l)
        lvl = _chase_entry(rows, d, variant, lo_since)
        if lvl is None:
            d += 1
            continue
        # 진입
        entry = lvl
        sh = CAPITAL / entry
        turnover += CAPITAL
        trips += 1
        peak = entry
        exited = False
        e = d + 1
        while e <= end:
            o2, h2, l2, c2, v2 = rows[e]
            peak = max(peak, h2)
            cut = entry * (1 - STOP)
            trl = peak * (1 - TRAIL)
            sl = max(cut, trl)              # 손절선·트레일선 중 높은쪽(둘다 위로 래칫)
            if l2 <= sl:
                total += sh * sl - CAPITAL - CAPITAL * COST
                exited = True
                d = e + 1
                lo_since = l2
                break
            e += 1
        if not exited:
            total += sh * rows[end][3] - CAPITAL - CAPITAL * COST
            break
    return total, trips, turnover


# ───────────────────────── 집계·리포트 ─────────────────────────

CONFIGS = [
    ("S1a 물타기 -3/6/9 균등·손절X", "1", "a"),
    ("S1b 물타기 -5/10/15 피라미드·손절X", "1", "b"),
    ("S1c 물타기 -3/6/9 +최대-12%손절", "1", "c"),
    ("S2a 손절+추격 10일고가돌파", "2", "a"),
    ("S2b 손절+추격 저점+3%반등", "2", "b"),
    ("S2c 손절+추격 20일이평회복", "2", "c"),
]


def _run_one(rows, ci, H, style, variant):
    if style == "1":
        return _style1(rows, ci, H, variant)
    return _style2(rows, ci, H, variant)


def _report(agg, n_camp):
    if n_camp == 0:
        print("캠페인 0건"); return 0
    print(f"매매스타일 비교 — 조정 캠페인 {n_camp:,}건 (자본 {CAPITAL:,}₩/캠페인, "
          f"trail -{TRAIL*100:.0f}% / drop -{DROP*100:.0f}% / 비용 {COST*100:.2f}%/leg)\n")
    print(f"  {'전략':<32}{'평균수익금₩':>14}{'총수익금₩':>16}{'승률':>7}{'평균라운드':>9}")
    rows_sorted = sorted(agg.items(), key=lambda kv: -kv[1]["sum"])
    for label, a in rows_sorted:
        nn = a["n"]
        if nn == 0:
            continue
        avg = a["sum"] / nn
        wr = a["wins"] / nn * 100
        rt = a["trips"] / nn
        print(f"  {label:<30}{avg:>14,.0f}{a['sum']:>16,.0f}{wr:>6.0f}%{rt:>9.2f}")
    print()
    best = rows_sorted[0]
    print(f"★ 수익금 1위: {best[0]} (평균 {best[1]['sum']/best[1]['n']:,.0f}₩/캠페인)")
    print("해석: 물타기(S1)는 평단↓로 회복시 큰₩ but 안돌아오면 큰손실(S1c가 그 꼬리 자름).")
    print("      손절+추격(S2)는 작은손실 통제+확인된 상승만 → 라운드수↑(회전).")
    print("★ 일봉·생존편향·단일trail → 절대₩ 과대, 두 스타일 '상대 수익금'만 신뢰. (--trail로 민감도)")
    return 0


def _run_real(data_dir):
    files = glob.glob(str(Path(data_dir) / "*.csv"))
    if not files:
        print(f"[!] 데이터 없음: {data_dir} (노트북 stock_data_daily). 로직검증=--selftest", file=sys.stderr)
        return 2
    agg = {label: {"n": 0, "sum": 0.0, "wins": 0, "trips": 0} for label, _, _ in CONFIGS}
    n_camp = 0
    for f in files:
        rows = _load(f)
        if len(rows) < LOOKBACK + 5:
            continue
        camps = _campaigns(rows)
        for ci, H in camps:
            n_camp += 1
            for label, style, variant in CONFIGS:
                g, trips, _ = _run_one(rows, ci, H, style, variant)
                a = agg[label]
                a["n"] += 1
                a["sum"] += g
                a["wins"] += 1 if g > 0 else 0
                a["trips"] += trips
    return _report(agg, n_camp)


# ───────────────────────── 셀프테스트 ─────────────────────────

def _mk(seq):
    """[(o,h,l,c)] → rows[(o,h,l,c,v)]."""
    return [(float(o), float(h), float(l), float(c), 1000.0) for o, h, l, c in seq]


def _selftest() -> int:
    checks = []
    base = [(100, 100, 100, 100)] * 22       # 평탄 22일(루프 LOOKBACK+1=21 통과 보장)

    # 1) 조정 캠페인 트리거: day22 종가88 = 20일고가100 -12% → 트리거, 전일 비조정
    rows = _mk(base + [(95, 95, 88, 88)] + [(90, 90, 90, 90)] * 5)
    checks.append(("조정 캠페인 트리거(day22,H100)", _campaigns(rows) == [(22, 100.0)]))

    # 2) S1a: H=100 레벨97/94/91. day22 저가88→3분할 체결(평단94). 익일 회복130 → 천장-4% 매도
    rows2 = _mk(base + [(95, 95, 88, 90)] + [(90, 130, 90, 128)] * 5)
    g1, t1, sp1 = _style1(rows2, 22, 100.0, "a")
    checks.append(("S1a 분할체결+회복 수익금>0", g1 > 0 and sp1 > 0))

    # 3) ★버티기★: 진입 직후 약간 하락(평단 위 회복 전)엔 트레일 미발동 — 안 잘림
    rows_hold = _mk(base + [(95, 95, 88, 90)] + [(90, 92, 89, 91)] * 3 + [(91, 130, 91, 128)] * 3)
    gH, _, _ = _style1(rows_hold, 22, 100.0, "a")
    checks.append(("S1a 회복전 눌림 안잘리고 버팀→회복수익", gH > 0))

    # 4) S1c 안전손절: 지속 폭락(회복X) → S1c는 평단-12% 컷, S1a는 타임아웃까지 더 잃음
    rows3 = _mk(base + [(95, 95, 88, 90)] + [(85, 85, 70, 72)] * 8)
    g1c, _, _ = _style1(rows3, 22, 100.0, "c")
    g1a, _, _ = _style1(rows3, 22, 100.0, "a")
    checks.append(("S1c 지속폭락서 S1a보다 덜잃음", g1c > g1a))

    # 5) S2b: 저점+3% 반등 진입(trips>0)
    rows4 = _mk(base + [(95, 95, 80, 82)] + [(82, 86, 82, 85)] + [(85, 100, 85, 98)] * 5)
    _, t2, _ = _style2(rows4, 22, 100.0, "b")
    checks.append(("S2b 추격진입 발생", t2 > 0))

    # 6) S2a: 10일고가 돌파 진입(trips>0)
    rows5 = _mk(base + [(95, 110, 95, 108)] + [(108, 108, 100, 101)] * 3)
    _, t2a, _ = _style2(rows5, 22, 100.0, "a")
    checks.append(("S2a 돌파진입 발생", t2a > 0))

    # 7) S2 손절 동작: 돌파진입 후 -STOP% 깨고 회복없으면 손실 컷(수익금<0 가능)
    rows7 = _mk(base + [(95, 110, 95, 108)] + [(108, 108, 95, 96)] * 4)  # 돌파후 -STOP깨고 하락
    g7, t7, _ = _style2(rows7, 22, 100.0, "a")
    checks.append(("S2a 진입+손절 라운드 발생", t7 > 0))

    ok = sum(1 for _, c in checks if c)
    for name, c in checks:
        print(f"  [{'PASS' if c else 'FAIL'}] {name}")
    print(f"\n셀프테스트: {ok}/{len(checks)} PASS")
    return 0 if ok == len(checks) else 1


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="매매스타일 비교: 물타기 vs 손절+추격 (수익금)")
    ap.add_argument("--data", default=str(DAILY_DEFAULT))
    ap.add_argument("--selftest", action="store_true")
    ap.add_argument("--trail", type=float, default=None, help="트레일% (예 0.03/0.05)")
    args = ap.parse_args(argv)
    if args.trail:
        global TRAIL
        TRAIL = args.trail
    if args.selftest:
        return _selftest()
    return _run_real(args.data)


if __name__ == "__main__":
    raise SystemExit(main())
