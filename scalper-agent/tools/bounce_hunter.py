# -*- coding: utf-8 -*-
"""
바운스 헌터 - 낙폭 반등 후보 스크리너
======================================
RFHIC +12%, 미래에셋증권 +10% 패턴 재현 목표

패턴: 최근 고점 대비 크게 하락 + 펀더멘탈 건재 → 5일 내 10~20% 반등

스크리닝 기준:
  1) 20일 고점 대비 -10% 이상 하락 (눌림 확인)
  2) 60일 저점 대비 아직 바닥권 (반등 여력)
  3) 거래량 급증 (세력 유입 신호)
  4) 컨센서스 업사이드 15%+ (펀더멘탈 OK)
  5) 외국인 or 기관 최근 순매수 전환 (수급 전환)
  6) 시총 1,000억+ (유동성)
"""

import json
import logging
import sys
import time
from pathlib import Path
from datetime import datetime

import numpy as np
import pandas as pd

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

from dotenv import load_dotenv
load_dotenv(BASE_DIR.parent / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("BounceHunter")

DATA_DIR = BASE_DIR / "data_store"
DAILY_DIR = DATA_DIR / "daily"
FLOW_DIR = DATA_DIR / "flow"
UNIVERSE_PATH = DATA_DIR / "universe.json"


def load_daily(code: str) -> pd.DataFrame | None:
    """일봉 CSV 로드"""
    path = DAILY_DIR / f"{code}.csv"
    if not path.exists():
        return None
    try:
        df = pd.read_csv(path, index_col=0, parse_dates=True)
        if len(df) < 20:
            return None
        return df
    except Exception:
        return None


def load_flow(code: str) -> pd.DataFrame | None:
    """수급 CSV 로드"""
    path = FLOW_DIR / f"{code}_investor.csv"
    if not path.exists():
        return None
    try:
        df = pd.read_csv(path, index_col=0, parse_dates=True)
        if len(df) < 5:
            return None
        return df
    except Exception:
        return None


def analyze_bounce_potential(code: str, info: dict) -> dict | None:
    """낙폭 반등 가능성 분석"""
    df = load_daily(code)
    if df is None:
        return None

    close = df["종가"].values.astype(float)
    volume = df["거래량"].values.astype(float)

    if len(close) < 60:
        return None

    current = close[-1]
    if current <= 0:
        return None

    # ── 1. 낙폭 계산 ──
    high_20 = close[-20:].max()        # 20일 고점
    high_60 = close[-60:].max()        # 60일 고점
    low_20 = close[-20:].min()         # 20일 저점
    low_5 = close[-5:].min()           # 5일 저점

    drop_from_20h = (current / high_20 - 1) * 100   # 20일 고점 대비 하락률
    drop_from_60h = (current / high_60 - 1) * 100   # 60일 고점 대비 하락률
    bounce_from_low = (current / low_5 - 1) * 100   # 5일 저점 대비 반등

    # 필터: 20일 고점 대비 -8% 이상 하락해야 함
    if drop_from_20h > -8:
        return None

    # 이미 많이 반등했으면 제외 (5일 저점 대비 +8% 이상이면 이미 타이밍 놓침)
    if bounce_from_low > 8:
        return None

    # ── 2. 거래량 분석 ──
    avg_vol_5 = volume[-5:].mean()
    avg_vol_20 = volume[-20:].mean()
    vol_ratio = avg_vol_5 / avg_vol_20 if avg_vol_20 > 0 else 1.0

    # ── 3. 기술적 위치 ──
    ma5 = close[-5:].mean()
    ma20 = close[-20:].mean()
    ma60 = close[-60:].mean()

    # RSI 14
    changes = np.diff(close[-15:])
    gains = np.where(changes > 0, changes, 0)
    losses = np.where(changes < 0, -changes, 0)
    avg_gain = gains.mean()
    avg_loss = losses.mean()
    if avg_loss > 0:
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
    else:
        rsi = 100

    # ── 4. 수급 분석 ──
    flow = load_flow(code)
    frgn_5d = 0
    inst_5d = 0
    frgn_turning = False  # 순매도→순매수 전환
    inst_turning = False

    if flow is not None and len(flow) >= 5:
        recent = flow.tail(5)
        frgn_cols = [c for c in flow.columns if "외국인" in c and "금액" in c]
        inst_cols = [c for c in flow.columns if "기관" in c and "금액" in c]

        if frgn_cols:
            fvals = recent[frgn_cols[0]].values
            frgn_5d = int(fvals.sum())
            # 전환 감지: 앞 3일 음수 → 뒤 2일 양수
            if len(fvals) >= 5:
                if fvals[-2:].sum() > 0 and fvals[:3].sum() < 0:
                    frgn_turning = True

        if inst_cols:
            ivals = recent[inst_cols[0]].values
            inst_5d = int(ivals.sum())
            if len(ivals) >= 5:
                if ivals[-2:].sum() > 0 and ivals[:3].sum() < 0:
                    inst_turning = True

    # ── 5. 종합 스코어링 ──
    score = 0

    # 낙폭 점수 (30점): -8%=0, -25%=30
    drop_score = min(max((-drop_from_20h - 8) / 17 * 30, 0), 30)
    score += drop_score

    # RSI 과매도 (20점): RSI 30이하=20, 50이상=0
    rsi_score = min(max((50 - rsi) / 20 * 20, 0), 20)
    score += rsi_score

    # 거래량 급증 (15점): 1.5배+=15
    vol_score = min(max((vol_ratio - 1.0) / 1.5 * 15, 0), 15)
    score += vol_score

    # 수급 전환 (20점)
    supply_score = 0
    if frgn_turning:
        supply_score += 10
    if inst_turning:
        supply_score += 10
    if frgn_5d > 0:
        supply_score += 5
    if inst_5d > 0:
        supply_score += 5
    supply_score = min(supply_score, 20)
    score += supply_score

    # 60일선 위 (5점) — 장기 추세 건재
    if current > ma60:
        score += 5

    # 시총 보너스 (5점)
    cap = info.get("cap_억", 0)
    if cap > 10000:
        score += 5
    elif cap > 5000:
        score += 3

    # 밸류 보너스 (5점)
    pbr = info.get("pbr", 99)
    if 0 < pbr < 1.0:
        score += 5
    elif 0 < pbr < 1.5:
        score += 2

    return {
        "code": code,
        "name": info.get("name", ""),
        "sector": info.get("sector", ""),
        "market": info.get("market", ""),
        "current_price": int(current),
        "high_20": int(high_20),
        "high_60": int(high_60),
        "drop_from_20h": round(drop_from_20h, 1),
        "drop_from_60h": round(drop_from_60h, 1),
        "bounce_from_low": round(bounce_from_low, 1),
        "rsi": round(rsi, 1),
        "vol_ratio": round(vol_ratio, 2),
        "ma5": int(ma5),
        "ma20": int(ma20),
        "ma60": int(ma60),
        "above_ma60": current > ma60,
        "frgn_5d": frgn_5d,
        "inst_5d": inst_5d,
        "frgn_turning": frgn_turning,
        "inst_turning": inst_turning,
        "per": info.get("per", 0),
        "pbr": info.get("pbr", 0),
        "cap_억": cap,
        "score": round(score, 1),
        "score_detail": {
            "drop": round(drop_score, 1),
            "rsi": round(rsi_score, 1),
            "volume": round(vol_score, 1),
            "supply": round(supply_score, 1),
        },
        # 반등 목표 (20일 고점의 90% 회복)
        "target_bounce": int(high_20 * 0.92),
        "bounce_upside": round((high_20 * 0.92 / current - 1) * 100, 1),
    }


def main():
    t0 = time.time()

    # 유니버스 로드
    with open(UNIVERSE_PATH, "r", encoding="utf-8") as f:
        universe = json.load(f)

    logger.info(f"유니버스 {len(universe)}종목 스캔 시작...")

    # daily CSV가 있는 종목만 대상
    results = []
    scanned = 0
    for code, info in universe.items():
        name = info.get("name", "")
        cap = info.get("cap_억", 0)

        # 기본 필터
        if name.endswith("우") or name.endswith("우B"):
            continue
        if cap < 1000:  # 시총 1000억 미만 제외
            continue

        result = analyze_bounce_potential(code, info)
        if result:
            results.append(result)

        scanned += 1
        if scanned % 200 == 0:
            logger.info(f"  스캔 진행: {scanned}종목 ({len(results)}건 후보)")

    logger.info(f"스캔 완료: {scanned}종목 -> {len(results)}건 바운스 후보")

    # 컨센서스 검증 (TOP 30만)
    results.sort(key=lambda x: x["score"], reverse=True)
    top = results[:30]

    logger.info(f"TOP {len(top)}종목 컨센서스 검증...")
    from data.consensus_scraper import fetch_consensus

    for r in top:
        cons = fetch_consensus(r["code"], use_cache=True, cache_hours=24)
        if cons:
            r["target_price"] = cons.get("target_price", 0)
            r["analyst_count"] = cons.get("analyst_count", 0)
            if r["current_price"] > 0 and r["target_price"] > 0:
                r["cons_upside"] = round(
                    (r["target_price"] / r["current_price"] - 1) * 100, 1
                )
            else:
                r["cons_upside"] = 0
        else:
            r["target_price"] = 0
            r["analyst_count"] = 0
            r["cons_upside"] = 0

    # 최종 정렬: score + 컨센 업사이드 가중
    for r in top:
        r["final_score"] = r["score"] + min(r.get("cons_upside", 0) / 3, 15)

    top.sort(key=lambda x: x["final_score"], reverse=True)

    # ── 출력 ──
    print("\n" + "=" * 80)
    print("  BOUNCE HUNTER - 낙폭 반등 후보 TOP 15")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M')} | RFHIC/미래에셋증권 패턴 재현")
    print("=" * 80)

    INVEST = 10000000  # 1000만원
    best_picks = top[:15]

    for i, r in enumerate(best_picks, 1):
        # 수급 아이콘
        supply_icon = ""
        if r["frgn_turning"]:
            supply_icon += "F전환"
        if r["inst_turning"]:
            supply_icon += " I전환"
        if r["frgn_5d"] > 0 and r["inst_5d"] > 0:
            supply_icon += " 쌍순매수"
        elif r["frgn_5d"] > 0:
            supply_icon += " F순매수"
        elif r["inst_5d"] > 0:
            supply_icon += " I순매수"

        if not supply_icon:
            supply_icon = "순매도중"

        # 투자 시뮬
        shares = int((INVEST / 3) / r["current_price"])
        profit_bounce = int(shares * (r["target_bounce"] - r["current_price"]))

        print(f"\n  #{i} {r['name']} ({r['code']}) | {r['sector']} | {r['market']}")
        print(f"  {'─' * 55}")
        print(f"  종가: {r['current_price']:,}원 | 20일고점: {r['high_20']:,}원 | 낙폭: {r['drop_from_20h']:+.1f}%")
        print(f"  RSI: {r['rsi']:.0f} | 거래량비: {r['vol_ratio']:.1f}x | 60일선: {'위' if r['above_ma60'] else '아래'}")
        print(f"  PER: {r['per']:.1f} | PBR: {r['pbr']:.2f} | 시총: {r['cap_억']:,}억")
        print(f"  수급: {supply_icon} | 외인5일: {r['frgn_5d']/1e8:+.1f}억 | 기관5일: {r['inst_5d']/1e8:+.1f}억")

        if r.get("target_price", 0) > 0:
            print(f"  컨센: {r['target_price']:,}원 ({r['cons_upside']:+.1f}%) | 애널: {r['analyst_count']}명")

        print(f"  반등 목표(고점90%): {r['target_bounce']:,}원 ({r['bounce_upside']:+.1f}%)")
        print(f"  333만원 투자시: {shares}주 -> 반등 수익 {profit_bounce:+,}원 ({r['bounce_upside']:+.1f}%)")
        print(f"  SCORE: {r['final_score']:.1f} (낙폭:{r['score_detail']['drop']:.0f} RSI:{r['score_detail']['rsi']:.0f} "
              f"거래량:{r['score_detail']['volume']:.0f} 수급:{r['score_detail']['supply']:.0f})")

    # ── 전투 계획 ──
    top3 = best_picks[:3]
    print(f"\n\n{'=' * 80}")
    print(f"  1,000만원 5일 전투 계획 (바운스 전략)")
    print(f"{'=' * 80}\n")

    total_profit = 0
    for i, r in enumerate(top3, 1):
        alloc = INVEST // 3
        shares = int(alloc / r["current_price"])
        actual = shares * r["current_price"]
        profit = int(shares * (r["target_bounce"] - r["current_price"]))
        sl_price = int(r["current_price"] * 0.95)
        loss = int(shares * (sl_price - r["current_price"]))
        total_profit += profit

        print(f"  #{i} {r['name']}")
        print(f"     매수: {r['current_price']:,}원 x {shares}주 = {actual:,}원")
        print(f"     TP: {r['target_bounce']:,}원 ({r['bounce_upside']:+.1f}%) -> +{profit:,}원")
        print(f"     SL: {sl_price:,}원 (-5%) -> {loss:,}원")
        print()

    avg_upside = sum(r["bounce_upside"] for r in top3) / 3
    total_loss = sum(int(INVEST // 3 // r["current_price"] * r["current_price"] * -0.05) for r in top3)
    mid = total_profit * 2 // 3 + total_loss // 3

    print(f"  [BEST]  3종목 TP:  +{total_profit:,}원 (평균 {avg_upside:+.1f}%)")
    print(f"  [MID]   2승 1패:  +{mid:,}원")
    print(f"  [WORST] 3종목 SL: {total_loss:,}원 (-5%)")

    print(f"""
  -- SL 규칙 --
  - 매수가 대비 -5% 도달 시 즉시 손절 (고점 대비 이미 -8%이상 빠진 종목이므로 SL 타이트하게)
  - 최대 손실: {total_loss:,}원
  - 수익 +5% 도달 시 → SL을 본전으로 올림 (무손실 보장)
  - 수익 +10% 도달 시 → 절반 익절 + 나머지 트레일링 -3%
""")

    # 저장
    output = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "strategy": "bounce_hunter",
        "pattern": "RFHIC/미래에셋증권 낙폭반등 패턴",
        "count": len(best_picks),
        "candidates": best_picks,
    }
    out_path = DATA_DIR / "bounce_candidates.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2, default=str)
    logger.info(f"결과 저장: {out_path}")

    elapsed = int(time.time() - t0)
    m, s = divmod(elapsed, 60)
    logger.info(f"총 소요: {m}분 {s}초")


if __name__ == "__main__":
    main()
