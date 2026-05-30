# -*- coding: utf-8 -*-
"""
바운스 헌터 - 낙폭 반등 후보 스크리너
======================================
RFHIC +12%, 미래에셋증권 +10% 패턴 재현 목표

패턴: 최근 고점 대비 크게 하락 + 펀더멘탈 건재 → 5일 내 10~20% 반등

스크리닝 기준:
  1) 20일 고점 대비 -8% 이상 하락 (눌림 확인)
  2) 5일 저점 대비 반등 8% 미만 (아직 타이밍 유효)
  3) 거래량 변화 (급증 시 가산점)
  4) 기관 패턴 등급 S/A/B/C/D (핵심 — 40점/100)
  5) 컨센서스 업사이드 (최종 스코어 가중)
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

    # ── 4. 수급 분석 (기관 패턴 등급 체계) ──
    # 통계적 근거: 기관 대량매수 후 20일 승률 64%, 평균 +8.9%
    flow = load_flow(code)
    frgn_5d = 0
    inst_5d = 0
    indi_5d = 0
    frgn_turning = False   # 순매도→순매수 전환
    inst_turning = False
    inst_streak = 0        # 기관 연속 매수일수
    inst_big_buy = False   # 기관 대량매수 (상위 5%)
    absorb_pattern = False # 흡수 패턴 (기관매수 + 외인매도)
    inst_accel = False     # 기관 가속 (최근 2일 > 이전 3일)

    if flow is not None and len(flow) >= 5:
        recent = flow.tail(5)
        frgn_cols = [c for c in flow.columns if "외국인" in c and "금액" in c]
        inst_cols = [c for c in flow.columns if "기관" in c and "금액" in c]
        indi_cols = [c for c in flow.columns if "개인" in c and "금액" in c]

        if frgn_cols:
            fvals = recent[frgn_cols[0]].values
            frgn_5d = int(fvals.sum())
            if len(fvals) >= 5:
                if fvals[-2:].sum() > 0 and fvals[:3].sum() < 0:
                    frgn_turning = True

        if inst_cols:
            ivals = recent[inst_cols[0]].values
            inst_5d = int(ivals.sum())
            if len(ivals) >= 5:
                if ivals[-2:].sum() > 0 and ivals[:3].sum() < 0:
                    inst_turning = True

            # 기관 연속 매수일수 (전체 데이터에서 역순)
            all_inst = flow[inst_cols[0]].values
            for v in reversed(all_inst):
                if v > 0:
                    inst_streak += 1
                else:
                    break

            # 기관 대량매수 여부 (최근 3일 중 상위 5% 해당)
            # NOTE: 2026-01-16 전후로 flow 데이터 단위 변경(원→백만원)
            #       혼합 방지를 위해 최근 40일만 사용
            recent_40 = flow[inst_cols[0]].tail(40)
            if len(recent_40) >= 20:
                threshold_95 = recent_40.quantile(0.95)
                last3 = flow[inst_cols[0]].tail(3)
                if threshold_95 > 0 and (last3 >= threshold_95).any():
                    inst_big_buy = True

            # 기관 가속: 최근 2일 평균 > 이전 3일 평균 & 양수
            if len(flow) >= 5:
                recent2 = flow[inst_cols[0]].tail(2).mean()
                prev3 = flow[inst_cols[0]].tail(5).head(3).mean()
                if recent2 > prev3 and recent2 > 0:
                    inst_accel = True

        if indi_cols:
            indi_5d = int(recent[indi_cols[0]].sum())

        # 흡수 패턴: 최근 3일 기관 순매수 + 외인 순매도
        if inst_cols and frgn_cols:
            last3_inst = flow[inst_cols[0]].tail(3).sum()
            last3_frgn = flow[frgn_cols[0]].tail(3).sum()
            if last3_inst > 0 and last3_frgn < 0:
                absorb_pattern = True

    # ── 기관 패턴 등급 (S/A/B/C/D) ──
    # S: 기관 3일+ 연속 + 외인 흡수 (최고 시그널 — 승률 64%, +8.9%)
    # A: 기관 대량매수 or 3일+ 연속
    # B: 기관 순매수 + 흡수
    # C: 기관 or 외인 순매수
    # D: 기관+외인 쌍매도
    inst_grade = "D"
    inst_grade_score = 0

    if inst_streak >= 3 and absorb_pattern:
        inst_grade = "S"
        inst_grade_score = 35
    elif inst_big_buy and absorb_pattern:
        inst_grade = "A"
        inst_grade_score = 28
    elif inst_streak >= 3:
        inst_grade = "A"
        inst_grade_score = 25
    elif inst_big_buy:
        inst_grade = "B"
        inst_grade_score = 18
    elif inst_5d > 0 and absorb_pattern:
        inst_grade = "B"
        inst_grade_score = 15
    elif inst_5d > 0:
        inst_grade = "C"
        inst_grade_score = 8
    elif frgn_5d > 0:
        inst_grade = "C"
        inst_grade_score = 5
    # else: D, 0점

    # 가속 보너스
    if inst_accel and inst_grade_score > 0:
        inst_grade_score += 5

    # ── 5. 종합 스코어링 ──
    score = 0

    # 낙폭 점수 (25점): -8%=0, -25%=25
    drop_score = min(max((-drop_from_20h - 8) / 17 * 25, 0), 25)
    score += drop_score

    # RSI 과매도 (15점): RSI 30이하=15, 50이상=0
    rsi_score = min(max((50 - rsi) / 20 * 15, 0), 15)
    score += rsi_score

    # 거래량 급증 (10점): 1.5배+=10
    vol_score = min(max((vol_ratio - 1.0) / 1.5 * 10, 0), 10)
    score += vol_score

    # 기관 패턴 등급 점수 (40점) — 핵심 팩터
    score += inst_grade_score

    # 60일선 위 (5점) — 장기 추세 건재
    ma60_score = 5 if current > ma60 else 0
    score += ma60_score

    # 시총 보너스 (3점)
    cap = info.get("cap_억", 0)
    if cap > 10000:
        cap_score = 3
    elif cap > 5000:
        cap_score = 2
    else:
        cap_score = 0
    score += cap_score

    # 밸류 보너스 (2점)
    pbr = info.get("pbr", 99)
    if 0 < pbr < 1.0:
        value_score = 2
    elif 0 < pbr < 1.5:
        value_score = 1
    else:
        value_score = 0
    score += value_score

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
        "above_ma60": bool(current > ma60),
        "frgn_5d": frgn_5d,
        "inst_5d": inst_5d,
        "indi_5d": indi_5d,
        "frgn_turning": frgn_turning,
        "inst_turning": inst_turning,
        "inst_streak": inst_streak,
        "inst_big_buy": inst_big_buy,
        "absorb_pattern": absorb_pattern,
        "inst_accel": inst_accel,
        "inst_grade": inst_grade,
        "per": info.get("per", 0),
        "pbr": info.get("pbr", 0),
        "cap_억": cap,
        "score": round(score, 1),
        "score_detail": {
            "drop": round(drop_score, 1),
            "rsi": round(rsi_score, 1),
            "volume": round(vol_score, 1),
            "inst_pattern": round(inst_grade_score, 1),
            "ma60": ma60_score,
            "cap": cap_score,
            "value": value_score,
        },
        # 반등 목표 (20일 고점의 92% 회복)
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

    grade_emoji = {"S": "★", "A": "◆", "B": "●", "C": "○", "D": "✕"}

    for i, r in enumerate(best_picks, 1):
        # 기관 등급
        ig = r.get("inst_grade", "D")
        ge = grade_emoji.get(ig, "?")

        # 수급 상세 텍스트
        supply_parts = []
        streak = r.get("inst_streak", 0)
        if streak >= 2:
            supply_parts.append(f"기관{streak}연속")
        if r.get("absorb_pattern"):
            supply_parts.append("흡수패턴")
        if r.get("inst_big_buy"):
            supply_parts.append("대량매수")
        if r.get("inst_accel"):
            supply_parts.append("가속")
        if r.get("frgn_turning"):
            supply_parts.append("외인전환")
        if not supply_parts:
            if r.get("frgn_5d", 0) > 0:
                supply_parts.append("외인순매수")
            elif r.get("inst_5d", 0) > 0:
                supply_parts.append("기관순매수")
            else:
                supply_parts.append("쌍매도")
        supply_icon = " + ".join(supply_parts)

        # 투자 시뮬
        shares = int((INVEST / 3) / r["current_price"])
        profit_bounce = int(shares * (r["target_bounce"] - r["current_price"]))

        print(f"\n  #{i} {ge}{ig} {r['name']} ({r['code']}) | {r['sector']} | {r['market']}")
        print(f"  {'─' * 60}")
        print(f"  종가: {r['current_price']:,}원 | 20일고점: {r['high_20']:,}원 | 낙폭: {r['drop_from_20h']:+.1f}%")
        print(f"  RSI: {r['rsi']:.0f} | 거래량비: {r['vol_ratio']:.1f}x | 60일선: {'위' if r['above_ma60'] else '아래'}")
        print(f"  PER: {r['per']:.1f} | PBR: {r['pbr']:.2f} | 시총: {r['cap_억']:,}억")
        print(f"  기관등급: {ge}{ig} | {supply_icon}")
        # NOTE: flow 데이터 2026-01-16부터 백만원 단위 → /100 = 억원
        print(f"  외인5일: {r['frgn_5d']/100:+.1f}억 | 기관5일: {r['inst_5d']/100:+.1f}억 | 개인5일: {r.get('indi_5d',0)/100:+.1f}억")

        if r.get("target_price", 0) > 0:
            print(f"  컨센: {r['target_price']:,}원 ({r['cons_upside']:+.1f}%) | 애널: {r['analyst_count']}명")

        print(f"  반등 목표(고점92%): {r['target_bounce']:,}원 ({r['bounce_upside']:+.1f}%)")
        print(f"  333만원 투자시: {shares}주 -> 반등 수익 {profit_bounce:+,}원 ({r['bounce_upside']:+.1f}%)")
        sd = r['score_detail']
        print(f"  SCORE: {r['final_score']:.1f} (낙폭:{sd['drop']:.0f} RSI:{sd['rsi']:.0f} "
              f"거래량:{sd['volume']:.0f} 기관패턴:{sd['inst_pattern']:.0f})")

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
        from data.sajang_rules import SAJANG
        sl_price = SAJANG.get_normal_sl(r["current_price"])
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
