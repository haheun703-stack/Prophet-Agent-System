# -*- coding: utf-8 -*-
"""D0 intraday sector reversal shadow label (6/9 사장님 지시).

목적: 기존 5일 섹터 강도(HOT)는 급락 후 V자 반전을 하루 늦게 잡는다.
당일(D0) 반전 신호(거래대금/상대강도/breadth)를 ★별도 shadow 라벨로만★ 기록해
6/12에 "금융(5일 HOT 선정) vs 반도체(shadow 미선정)" forward 수익을 비교한다.

★★★ 안전 불변식 (사장님 6/9) ★★★
- 실매수 로직 변경 0 / HOT 섹터 판정 변경 0 / 후보 tier 변경 0
- sector_relay는 read-only로만 사용 (scan_all_sectors 호출, 아무것도 mutate 안 함)
- 출력은 별도 파일(sector_reversal_shadow_{date}.json) — ledger/picks 무접촉
- forward_d3/d5/verdict_6_12 는 placeholder(None) — 6/12에 별도 채움

실행: python tools/sector_reversal_shadow_6_9.py --asof 2026-06-09
"""
import argparse
import json
import os
import sys
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE))  # tools/ 실행 시 루트 모듈(data.*) 접근
DAILY_DIR = BASE / "data_store" / "daily"
OUT_DIR = BASE / "data_store"


def _daily_turnover(code: str, asof: str) -> float:
    """일봉 csv에서 asof 당일 거래대금(거래량×종가). 없으면 0. (read-only)"""
    f = DAILY_DIR / f"{code}.csv"
    if not f.exists():
        return 0.0
    try:
        last = f.read_text(encoding="utf-8").strip().split("\n")[-1].split(",")
        if last[0] == asof:
            close = float(last[4]); vol = float(last[5])  # 헤더: 날짜,시가,고가,저가,종가,거래량,등락률
            return close * vol
    except Exception:
        pass
    return 0.0


def build_shadow(asof: str) -> dict:
    from data.sector_relay import scan_all_sectors  # read-only

    sectors = scan_all_sectors(asof)
    rows = []
    for st in sectors:
        if not getattr(st, "stocks", None):
            continue
        d0 = sum(s.change_1d for s in st.stocks) / len(st.stocks)
        turnover = sum(_daily_turnover(s.code, asof) for s in st.stocks)
        rows.append({
            "sector": st.sector_name,
            "d0_return": round(d0, 2),
            "breadth_1d": round(st.breadth, 2),
            "momentum_5d": round(st.momentum_5d, 2),
            "status_5d": st.status,                       # 기존 HOT/WARMING/... (참고)
            "selected_by_5d_hot": st.status == "HOT",     # 기존 5일 HOT 선정 여부
            "n_stocks": len(st.stocks),
            "_turnover": turnover,
        })

    if not rows:
        return {"asof": asof, "note": "no sectors", "sectors": []}

    # market proxy = 섹터 유니버스 d0_return 평균 (코스피 지수 일봉 부재 → 근사, caveat)
    market_return = round(sum(r["d0_return"] for r in rows) / len(rows), 2)
    # turnover_rank (거래대금 내림차순, 1=최대)
    for i, r in enumerate(sorted(rows, key=lambda x: -x["_turnover"])):
        r["turnover_rank"] = i + 1
    for r in rows:
        r["market_return"] = market_return
        r["relative_strength"] = round(r["d0_return"] - market_return, 2)
        r.pop("_turnover", None)
        r["forward_d3"] = None       # 6/12 채움
        r["forward_d5"] = None       # 6/12 채움
        r["verdict_6_12"] = None     # 6/12 채움 (5일HOT vs shadow 비교)

    rows.sort(key=lambda r: -r["relative_strength"])
    return {
        "asof": asof,
        "role": "SHADOW_LABEL_ONLY",
        "note": "D0 intraday sector reversal. 매수로직/HOT판정/tier 무접촉. 6/12 forward 검증용.",
        "market_return": market_return,
        "market_return_proxy": "sector_universe_d0_mean (코스피 지수 일봉 부재 근사)",
        "sectors": rows,
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--asof", required=True)
    args = ap.parse_args()
    result = build_shadow(args.asof)
    out = OUT_DIR / f"sector_reversal_shadow_{args.asof}.json"
    out.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    # 요약 출력
    print(f"[sector_reversal_shadow] asof={args.asof} market_return={result.get('market_return')}")
    print(f"  저장: {out}")
    print("  --- 상대강도 상위 (d0_return / RS / breadth / 5d_HOT / turnover_rank) ---")
    for r in result.get("sectors", [])[:8]:
        flag = "★HOT" if r["selected_by_5d_hot"] else "   "
        print(f"  {flag} {r['sector']:<12} d0={r['d0_return']:+.1f}% RS={r['relative_strength']:+.1f} "
              f"breadth={r['breadth_1d']} 5d={r['status_5d']} turn#{r['turnover_rank']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
