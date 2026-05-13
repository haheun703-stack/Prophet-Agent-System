# -*- coding: utf-8 -*-
"""
진입 후보 스캔 - MOMENTUM v3 기준 (국적 팩터 추가)
시총 2000억+ 확대 + 싱가포르/케이맨 국적 파워 반영
"""

import json
import sys
import os
import numpy as np
import pandas as pd
from pathlib import Path
from collections import defaultdict

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

DATA_DIR = Path(__file__).resolve().parent.parent / "data_store"
DAILY_DIR = DATA_DIR / "daily"
FLOW_DIR = DATA_DIR / "flow"
NAT_DIR = DATA_DIR / "nationality"

MOMENTUM_THRESHOLD = 0.40
MIN_CAP = 2000  # v3: 5000 -> 2000 (중소형 헤지펀드 종목 포착)

# 국적 분류
_HEDGE_COUNTRIES = {"케이맨 제도", "영국령 버진아일랜드", "버뮤다"}
_ASIA_ACTIVE = {"싱가포르", "홍콩"}


def _load_nationality(code):
    """최신 국적 스냅샷 로드"""
    if not NAT_DIR.exists():
        return {}
    snapshots = sorted(NAT_DIR.glob(f"{code}_2*.csv"), reverse=True)
    if snapshots:
        try:
            df = pd.read_csv(snapshots[0], encoding="utf-8-sig")
            return dict(zip(df["국가명"], df["거래량"]))
        except Exception:
            pass
    profile = NAT_DIR / f"nationality_{code}.csv"
    if profile.exists():
        try:
            df = pd.read_csv(profile, encoding="utf-8-sig")
            return dict(zip(df["국가명"], df["거래량"]))
        except Exception:
            pass
    return {}


def _calc_nat_ratio(nat_data):
    """싱+케 합산 비중"""
    if not nat_data:
        return 0.0, 0.0, 0.0
    total = sum(v for v in nat_data.values() if v > 0)
    if total <= 0:
        return 0.0, 0.0, 0.0
    hedge = sum(nat_data.get(c, 0) for c in _HEDGE_COUNTRIES)
    asia = sum(nat_data.get(c, 0) for c in _ASIA_ACTIVE)
    return (hedge + asia) / total, hedge / total, asia / total


def scan():
    universe = json.load(open(DATA_DIR / "universe.json", "r", encoding="utf-8"))

    # 시총 2000억+ 필터 (v3: 5000 -> 2000)
    big_codes = [c for c in universe if universe[c].get("cap_억", 0) >= MIN_CAP]
    print(f"시총 {MIN_CAP}억+ 종목: {len(big_codes)}개")

    # 데이터 프리로드
    daily_cache = {}
    flow_cache = {}
    for code in big_codes:
        dp = DAILY_DIR / f"{code}.csv"
        fp = FLOW_DIR / f"{code}_investor.csv"
        try:
            if dp.exists():
                daily_cache[code] = pd.read_csv(dp, index_col=0, parse_dates=True).sort_index()
        except Exception:
            pass
        try:
            if fp.exists():
                flow_cache[code] = pd.read_csv(fp, index_col=0, parse_dates=True).sort_index()
        except Exception:
            pass

    print(f"일봉 로드: {len(daily_cache)}종목, 수급 로드: {len(flow_cache)}종목")

    # 섹터별 피어 플로우 캐시
    sector_peer_cache = {}
    sector_stocks = defaultdict(list)
    for code in big_codes:
        sector = universe[code].get("sector", "")
        if sector:
            cap = universe[code].get("cap_억", 0)
            sector_stocks[sector].append((code, cap))

    for sector, stocks in sector_stocks.items():
        stocks.sort(key=lambda x: x[1], reverse=True)
        top30 = stocks[:30]
        pos_count = 0
        total_count = 0
        for sc, _ in top30:
            flow = flow_cache.get(sc)
            if flow is None or len(flow) < 1:
                continue
            total_count += 1
            last_row = flow.iloc[-1]
            inst = last_row.get("기관_금액", 0) or 0
            frgn = last_row.get("외국인_금액", 0) or 0
            if (inst + frgn) > 0:
                pos_count += 1
        ratio = pos_count / total_count if total_count > 0 else 0
        sector_peer_cache[sector] = ratio

    # MOMENTUM 스캔
    momentum_stocks = []

    for code in big_codes:
        daily = daily_cache.get(code)
        flow = flow_cache.get(code)
        if daily is None or len(daily) < 21:
            continue

        # (A) 거래량 폭증
        latest_vol = daily["거래량"].iloc[-1]
        avg_20d = daily["거래량"].iloc[-21:-1].mean()
        vol_ratio = latest_vol / avg_20d if avg_20d > 0 else 1.0
        vol_score = 0.25 if vol_ratio >= 3.0 else (0.15 if vol_ratio >= 2.0 else (0.08 if vol_ratio >= 1.5 else 0.0))

        # (B) 기관+외인 연속매수
        consec = 0
        if flow is not None and len(flow) > 0:
            for j in range(len(flow) - 1, -1, -1):
                row = flow.iloc[j]
                inst = row.get("기관_금액", 0) or 0
                frgn = row.get("외국인_금액", 0) or 0
                if (inst + frgn) > 0:
                    consec += 1
                else:
                    break
        consec_score = 0.30 if consec >= 5 else (0.25 if consec >= 3 else (0.15 if consec >= 2 else 0.0))

        # (C) 섹터 동반상승
        sector = universe[code].get("sector", "")
        peer_ratio = sector_peer_cache.get(sector, 0)
        breadth_score = 0.20 if peer_ratio >= 0.30 else (0.15 if peer_ratio >= 0.15 else (0.08 if peer_ratio >= 0.10 else 0.0))

        # (D) 기관 프로그램 매수
        prog_score = 0.0
        if flow is not None and len(flow) >= 5:
            recent5 = flow.iloc[-5:]
            pos_days = sum(1 for _, r in recent5.iterrows() if (r.get("기관_금액", 0) or 0) > 0)
            prog_score = 0.15 if pos_days >= 4 else (0.10 if pos_days >= 3 else 0.0)

        # (E) 조용한 매집
        quiet_score = 0.0
        if flow is not None and len(flow) >= 5 and len(daily) >= 16:
            recent_f5 = flow.iloc[-5:]
            inst_total = sum((r.get("기관_금액", 0) or 0) for _, r in recent_f5.iterrows())
            if inst_total > 0:
                vol_r5 = daily["거래량"].iloc[-5:].mean()
                vol_e10 = daily["거래량"].iloc[-15:-5].mean()
                vol_ch = vol_r5 / vol_e10 if vol_e10 > 0 else 1
                close5 = daily["종가"].iloc[-5:]
                pr = (close5.max() - close5.min()) / close5.mean() * 100
                if vol_ch <= 1.3 and pr <= 3.0:
                    quiet_score = 0.10

        # (F) 섹터 피어 부스트
        peer_score = 0.0
        if peer_ratio >= 0.60:
            peer_score = 0.15
        elif peer_ratio >= 0.45:
            peer_score = 0.10
        elif peer_ratio >= 0.35:
            peer_score = 0.05

        # (G) 외국인 국적 파워
        nat_data = _load_nationality(code)
        combined_pct, hedge_pct, asia_pct = _calc_nat_ratio(nat_data)
        nat_score = 0.0
        if combined_pct >= 0.35:
            nat_score = 0.20
        elif combined_pct >= 0.25:
            nat_score = 0.12
        elif combined_pct >= 0.15:
            nat_score = 0.05

        total = vol_score + consec_score + breadth_score + prog_score + quiet_score + peer_score + nat_score

        if total >= MOMENTUM_THRESHOLD:
            name = universe[code].get("name", code)
            cap = universe[code].get("cap_億", universe[code].get("cap_억", 0))

            # 최근 5일 수익률
            ret_5d = 0
            if len(daily) >= 6:
                ret_5d = (daily["종가"].iloc[-1] / daily["종가"].iloc[-6] - 1) * 100

            # 최근 등락률
            ret_1d = daily.get("등락률", pd.Series([0])).iloc[-1] if "등락률" in daily.columns else 0

            # 수급 상세
            inst_5d = 0
            frgn_5d = 0
            if flow is not None and len(flow) >= 5:
                for _, r in flow.iloc[-5:].iterrows():
                    inst_5d += (r.get("기관_금액", 0) or 0)
                    frgn_5d += (r.get("외국인_금액", 0) or 0)

            close = int(daily["종가"].iloc[-1])

            momentum_stocks.append({
                "code": code,
                "name": name,
                "sector": sector,
                "cap_억": cap,
                "close": close,
                "score": round(total, 3),
                "vol_ratio": round(vol_ratio, 1),
                "consec": consec,
                "ret_1d": round(ret_1d, 2),
                "ret_5d": round(ret_5d, 2),
                "inst_5d": inst_5d,
                "frgn_5d": frgn_5d,
                "nat_combined": round(combined_pct * 100, 1),
                "factors": {
                    "vol": vol_score,
                    "consec": consec_score,
                    "breadth": breadth_score,
                    "prog": prog_score,
                    "quiet": quiet_score,
                    "peer": peer_score,
                    "nat": nat_score,
                },
            })

    momentum_stocks.sort(key=lambda x: x["score"], reverse=True)

    print(f"\n{'=' * 110}")
    print(f"  MOMENTUM v3 스캔 결과 (시총 {MIN_CAP}억+ | 국적 팩터 포함)")
    print(f"  THRESHOLD >= {MOMENTUM_THRESHOLD} | 감지: {len(momentum_stocks)}종목")
    print(f"{'=' * 110}")

    # TOP 종목 상세
    for i, s in enumerate(momentum_stocks[:30], 1):
        f = s["factors"]
        factors_str = ""
        if f["vol"] > 0: factors_str += f"VOL{s['vol_ratio']}x "
        if f["consec"] > 0: factors_str += f"연속{s['consec']}D "
        if f["breadth"] > 0: factors_str += f"섹터 "
        if f["prog"] > 0: factors_str += f"프로그 "
        if f["quiet"] > 0: factors_str += f"매집 "
        if f["peer"] > 0: factors_str += f"피어 "
        if f.get("nat", 0) > 0: factors_str += f"국적{s.get('nat_combined',0):.0f}% "

        # SL/TP 제안
        sl = int(s["close"] * 0.965)  # -3.5%
        tp = int(s["close"] * 1.10)   # +10%

        print(f"  {i:>2}. {s['name']:>14}({s['code']}) {s['close']:>10,}원  "
              f"score={s['score']:.2f}  5일{s['ret_5d']:+.1f}% 금일{s['ret_1d']:+.1f}%  "
              f"기관5D={s['inst_5d']:+,.0f} 외인5D={s['frgn_5d']:+,.0f}  "
              f"[{factors_str.strip()}]")
        if i <= 15:
            print(f"      SL={sl:,}원(-3.5%) TP={tp:,}원(+10%) | {s['sector']}")

    # 스코어별 분포
    print(f"\n  ── 스코어 분포 ──")
    for threshold in [0.70, 0.60, 0.55, 0.50, 0.45, 0.40]:
        cnt = len([s for s in momentum_stocks if s["score"] >= threshold])
        print(f"  score >= {threshold}: {cnt}종목")

    # 섹터별 분포
    print(f"\n  ── 섹터별 MOMENTUM 분포 ──")
    sector_counts = defaultdict(list)
    for s in momentum_stocks:
        sector_counts[s["sector"]].append(s)
    for sector, stocks in sorted(sector_counts.items(), key=lambda x: len(x[1]), reverse=True):
        if len(stocks) >= 2:
            names = ", ".join(s["name"] for s in stocks[:5])
            avg_score = np.mean([s["score"] for s in stocks])
            print(f"    {sector:>12}: {len(stocks)}종목 (avg={avg_score:.2f}) — {names}")

    # 연속매수 TOP
    print(f"\n  ── 연속 기관+외인 매수 TOP 10 ──")
    by_consec = sorted(momentum_stocks, key=lambda x: x["consec"], reverse=True)
    for s in by_consec[:10]:
        print(f"    {s['name']:>14}: {s['consec']}일 연속매수  score={s['score']:.2f}  "
              f"기관5D={s['inst_5d']:+,.0f} 외인5D={s['frgn_5d']:+,.0f}")

    # 추천 JSON 저장
    top_candidates = momentum_stocks[:15]
    output = {
        "scan_date": "2026-03-14",
        "target_date": "2026-03-16",
        "threshold": MOMENTUM_THRESHOLD,
        "total_detected": len(momentum_stocks),
        "candidates": top_candidates,
    }
    out_path = DATA_DIR / "monday_candidates.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    print(f"\n  저장: {out_path}")
    print(f"{'=' * 110}")


if __name__ == "__main__":
    scan()
