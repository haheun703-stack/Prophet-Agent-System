# -*- coding: utf-8 -*-
"""휴면 종목의 첫 급등/상한가 → 이후 '계속 가는 놈 vs 죽는 놈'을 수급·패턴이 가르나 (사장님 5/31).

사장님 핵심 질문: "멀쩡하고 반응 없던 놈이 갑자기 +10%↑ 급등/상한가. 그 이후 패턴·수급·뭔가를 찾아야."
= 3번째 상한가(끝물)가 아니라 **휴면→첫 점화** 후 연속성을 가르는 신호 탐색.

데이터: OHLCV(stock_data_daily 10년) + ★수급(data_store/flow/{code}_investor.csv = 외인/기관 금액, 단 ~2026-02~05 4개월)★.
  → 수급 검증은 최근 4개월(현 초강세장 regime, 사장님이 원한 창)만 가능. code+date 조인.

이벤트(휴면→첫 급등): 직전20일 최대 일간상승<10%(반응없음) & 기지(pos20≤60%) & T0 종가상승≥SURGE & flow 존재(T0,T+1).
연속자(CONTINUER): T+1~T+5 고점이 T0종가 대비 ≥+10% 추가 상승(또 갔다). 페이더: 아님.
비교: 종가강도/거래량 + ★외인·기관 순매수(T0, T+1=수급 잔류)★ → 무엇이 연속을 가르나.
★ 한계: 수급 4개월(표본·regime 한정)/일봉/생존편향(상폐부재)/금액 단위 미정→부호·상대만. 비용 미반영(패턴분석).
"""
from __future__ import annotations
import csv, glob, sys
from pathlib import Path
import numpy as np

ROOT = Path(__file__).resolve().parent.parent
PROJ = ROOT.parent
DAILY = PROJ / "stock_data_daily"
FLOW = ROOT / "data_store" / "flow"

SURGE = 0.15      # 첫 급등 임계 (사장님 "+10%↑~상한가" → 강한급등 15%)
DORMANT_MAX = 0.10  # 직전20일 최대 일간상승 < 10% = 휴면
BASE_POS = 0.60
CONT = 0.10       # 연속자 = T+1~T+5 고점 +10%↑ 추가
PRE = 20
POST = 5


def _load_daily(p):
    rows = []
    try:
        with open(p, encoding="utf-8") as f:
            rd = csv.reader(f)
            header = next(rd, None)
            for r in rd:
                try:
                    dt = r[0]; o = float(r[1]); h = float(r[2]); l = float(r[3]); c = float(r[4]); v = float(r[5])
                except (ValueError, IndexError):
                    continue
                if o > 0 and c > 0:
                    rows.append((dt, o, h, l, c, v))
    except Exception:
        return []
    return rows


def _load_flow(code):
    """{date: (외인금액, 기관금액)}"""
    p = FLOW / f"{code}_investor.csv"
    if not p.exists():
        return {}
    out = {}
    try:
        with open(p, encoding="utf-8") as f:
            for r in csv.DictReader(f):
                try:
                    out[r["date"]] = (float(r.get("외국인_금액", 0) or 0), float(r.get("기관_금액", 0) or 0))
                except (ValueError, KeyError):
                    continue
    except Exception:
        return {}
    return out


def main():
    # code -> daily path
    dmap = {}
    for p in glob.glob(str(DAILY / "*.csv")):
        code = Path(p).stem.rsplit("_", 1)[-1]
        dmap[code] = p

    cont, fade = [], []   # 각 원소: dict(features)
    n_ev = 0; n_limit = 0
    flow_codes = [Path(p).stem.replace("_investor", "") for p in glob.glob(str(FLOW / "*_investor.csv"))]

    for code in flow_codes:
        if code not in dmap:
            continue
        rows = _load_daily(dmap[code])
        if len(rows) < PRE + POST + 2:
            continue
        flow = _load_flow(code)
        if not flow:
            continue
        n = len(rows)
        for i in range(PRE, n - POST):
            dt0 = rows[i][0]
            if dt0 not in flow:      # 수급 있는 기간만 (최근 4개월)
                continue
            pc = rows[i - 1][4]
            if pc <= 0:
                continue
            up = rows[i][4] / pc - 1
            if up < SURGE:
                continue
            # 휴면: 직전20일 최대 일간상승 < 10%
            prior_max = 0.0
            for j in range(i - PRE, i):
                ppc = rows[j - 1][4]
                if ppc > 0:
                    prior_max = max(prior_max, rows[j][4] / ppc - 1)
            if prior_max >= DORMANT_MAX:
                continue
            # 기지(pos20)
            lows = [rows[j][3] for j in range(i - 20, i)]; highs = [rows[j][2] for j in range(i - 20, i)]
            band = max(highs) - min(lows)
            pos20 = (pc - min(lows)) / band if band > 0 else 0.5
            if pos20 > BASE_POS:
                continue
            # T+1 수급 존재 확인
            dt1 = rows[i + 1][0]
            if dt1 not in flow:
                continue
            n_ev += 1
            if up >= 0.25:
                n_limit += 1
            c0 = rows[i][4]; h0 = rows[i][2]; l0 = rows[i][3]
            cs = (c0 - l0) / (h0 - l0) if h0 > l0 else 0.5
            vma = sum(rows[j][5] for j in range(i - 20, i)) / 20
            volx = rows[i][5] / vma if vma > 0 else 0
            fwd_max = max(rows[i + t][2] for t in range(1, POST + 1)) / c0 - 1
            f0 = flow[dt0]; f1 = flow[dt1]
            feat = {
                "cs": cs, "volx": volx, "up": up,
                "frgn0": 1 if f0[0] > 0 else 0, "inst0": 1 if f0[1] > 0 else 0,
                "frgn1": 1 if f1[0] > 0 else 0, "inst1": 1 if f1[1] > 0 else 0,
                "smart1": 1 if (f1[0] > 0 or f1[1] > 0) else 0,   # 익일 외인 or 기관 순매수 = 수급 잔류
                "t1ret": rows[i + 1][4] / c0 - 1,
            }
            (cont if fwd_max >= CONT else fade).append(feat)

    nc, nf = len(cont), len(fade)
    tot = nc + nf
    if tot == 0:
        print("이벤트 0 (수급기간 휴면-첫급등 없음)"); return 1
    print(f"휴면→첫 급등(≥{SURGE:.0%}) 이벤트 {tot:,}건 (상한가급 ≥25%: {n_limit}건) | 수급기간 ~2026-02~05")
    print(f"연속자(T+1~5 고점 +{CONT:.0%}↑): {nc:,} ({nc/tot*100:.0f}%) / 페이더: {nf:,} ({nf/tot*100:.0f}%)\n")

    def avg(lst, k):
        return np.mean([x[k] for x in lst]) if lst else 0

    print(f'{"피처":<22}{"연속자":>10}{"페이더":>10}{"차이":>9}')
    rows_out = [
        ("종가강도(0~1)", "cs", "%.2f"),
        ("거래량배수", "volx", "%.1fx"),
        ("당일상승%", "up", "%.0f%%"),
        ("★외인 순매수 T0", "frgn0", "%.0f%%"),
        ("★기관 순매수 T0", "inst0", "%.0f%%"),
        ("★외인 순매수 T+1", "frgn1", "%.0f%%"),
        ("★기관 순매수 T+1", "inst1", "%.0f%%"),
        ("★★수급잔류 T+1(외or기)", "smart1", "%.0f%%"),
        ("익일 T+1 수익%", "t1ret", "%.1f%%"),
    ]
    for label, k, fmt in rows_out:
        a, b = avg(cont, k), avg(fade, k)
        if "%%" in fmt and k not in ("up", "t1ret"):
            a, b = a * 100, b * 100; ds = f"{a-b:+.0f}%p"
            print(f'{label:<22}{a:>9.0f}%{b:>9.0f}%{ds:>9}')
        elif k in ("up", "t1ret"):
            a, b = a * 100, b * 100
            print(f'{label:<22}{a:>+8.1f}%{b:>+8.1f}%{a-b:>+8.1f}p')
        else:
            print(f'{label:<22}{a:>9.2f}{b:>9.2f}{a-b:>+8.2f}')

    # ★ 핵심: 수급잔류 유무별 연속률
    print("\n★ 수급잔류(T+1 외인 or 기관 순매수) 유무별 연속률:")
    allf = cont + fade
    smart = [x for x in allf if x["smart1"] == 1]; nosmart = [x for x in allf if x["smart1"] == 0]
    for lab, seg in (("수급잔류 O", smart), ("수급잔류 X", nosmart)):
        if seg:
            cr = np.mean([1 if (x in cont) else 0 for x in seg]) * 100  # 비효율적이나 소규모
            # 재계산: seg 중 연속자 비율
        # 정확 계산
    cont_set = set(id(x) for x in cont)
    for lab, seg in (("수급잔류 O", smart), ("수급잔류 X", nosmart)):
        if not seg:
            print(f"  {lab}: 0건"); continue
        cr = sum(1 for x in seg if id(x) in cont_set) / len(seg) * 100
        print(f"  {lab}: {len(seg):,}건 → 연속률 {cr:.0f}%")
    # 종가강도 강/약
    strong = [x for x in allf if x["cs"] >= 0.8]; weak = [x for x in allf if x["cs"] < 0.5]
    for lab, seg in (("종가강도≥0.8(끝까지 밀어)", strong), ("종가강도<0.5(위꼬리)", weak)):
        if not seg:
            print(f"  {lab}: 0건"); continue
        cr = sum(1 for x in seg if id(x) in cont_set) / len(seg) * 100
        print(f"  {lab}: {len(seg):,}건 → 연속률 {cr:.0f}%")

    print("\n★ 해석: 연속자 vs 페이더 차이 큰 피처 = 첫 급등 후 '갈 놈' 판별 신호. 특히 수급잔류·종가강도가 가르면 = 사장님 직관 입증.")
    print("★ 한계: 수급 4개월(현 초강세장·표본한정)/일봉/생존편향/금액 부호만(단위無)/비용 미반영.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
