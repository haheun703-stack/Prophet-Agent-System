# -*- coding: utf-8 -*-
"""
숨은보석 스크리너 (Hidden Gems Screener)
==========================================
전체 유니버스(~1,567종목)를 돌려서 저평가 + 컨센서스 업사이드 + 수급 양호한
숨겨진 종목을 찾아내는 독립 실행 스크립트.

스크리닝 기준:
  Phase 1 (빠른 필터): universe.json 기반
    - PBR < 1.5 (저평가 영역)
    - PER > 0 & < 20 (흑자 + 적정가치)
    - 시가총액 500억 ~ 100,000억 (유동성 + 숨은보석 범위)
    - 우선주 제외

  Phase 2 (컨센서스 검증): WiseReport/FnGuide
    - 컨센서스 업사이드 20%+
    - 애널리스트 2명 이상

  Phase 3 (수급 검증): 네이버 금융
    - 외국인/기관 5일 누적 순매수

  Phase 4 (종합 스코어링):
    - 컨센업사이드(35%) + PBR할인(20%) + PER할인(15%) + 수급(20%) + 애널수(10%)

사용법:
  cd scalper-agent
  python tools/hidden_gems_screener.py           # 전체 스크리닝
  python tools/hidden_gems_screener.py --top 10   # TOP 10만
  python tools/hidden_gems_screener.py --skip-supply  # 수급 조회 생략 (빠르게)
"""

import json
import logging
import os
import sys
import time
import argparse
from pathlib import Path
from datetime import datetime

# 프로젝트 루트 설정
BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

from dotenv import load_dotenv
load_dotenv(BASE_DIR.parent / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("HiddenGems")

DATA_DIR = BASE_DIR / "data_store"
UNIVERSE_PATH = DATA_DIR / "universe.json"
CONSENSUS_PATH = DATA_DIR / "consensus.json"
RECOMMENDATION_PATH = DATA_DIR / "recommendation.json"
OUTPUT_PATH = DATA_DIR / "hidden_gems.json"


# ═══════════════════════════════════════════════════════
#  Phase 1: 유니버스 기본 필터
# ═══════════════════════════════════════════════════════

def phase1_universe_filter() -> list[dict]:
    """PBR/PER/시총 기준 1차 필터"""
    with open(UNIVERSE_PATH, "r", encoding="utf-8") as f:
        universe = json.load(f)

    # 이미 추천된 종목 코드 로드 (제외 대상)
    excluded_codes = set()
    if RECOMMENDATION_PATH.exists():
        try:
            rec = json.load(open(RECOMMENDATION_PATH, "r", encoding="utf-8"))
            for s in rec.get("stocks", []):
                excluded_codes.add(s.get("code", ""))
        except Exception:
            pass

    candidates = []
    skipped = {"pref": 0, "per": 0, "pbr": 0, "cap": 0, "excluded": 0}

    for code, info in universe.items():
        name = info.get("name", "")

        # 우선주 제외
        if name.endswith("우") or name.endswith("우B") or "우선" in name:
            skipped["pref"] += 1
            continue

        # 이미 추천 종목 제외
        if code in excluded_codes:
            skipped["excluded"] += 1
            continue

        per = info.get("per", 0)
        pbr = info.get("pbr", 0)
        cap = info.get("cap_억", 0)

        # PER 필터: 흑자 + 저PER (0 < PER < 20)
        if per <= 0 or per > 20:
            skipped["per"] += 1
            continue

        # PBR 필터: < 1.5 (본질가치 대비 저평가)
        if pbr <= 0 or pbr > 1.5:
            skipped["pbr"] += 1
            continue

        # 시총 필터: 500억 ~ 100,000억
        if cap < 500 or cap > 100000:
            skipped["cap"] += 1
            continue

        candidates.append({
            "code": code,
            "name": name,
            "market": info.get("market", ""),
            "sector": info.get("sector", ""),
            "per": per,
            "pbr": pbr,
            "cap_억": cap,
            "volume": info.get("volume", 0),
        })

    logger.info(
        f"Phase 1 완료: {len(universe)}종목 -> {len(candidates)}종목 통과 "
        f"(우선주:{skipped['pref']} PER:{skipped['per']} PBR:{skipped['pbr']} "
        f"시총:{skipped['cap']} 이미추천:{skipped['excluded']})"
    )
    return candidates


# ═══════════════════════════════════════════════════════
#  Phase 2: 컨센서스 검증
# ═══════════════════════════════════════════════════════

def phase2_consensus_check(candidates: list[dict]) -> list[dict]:
    """컨센서스 목표가 업사이드 20%+ 필터"""
    from data.consensus_scraper import fetch_consensus

    passed = []
    no_consensus = 0
    low_upside = 0
    low_analyst = 0
    errors = 0

    total = len(candidates)
    for i, c in enumerate(candidates):
        code = c["code"]
        if (i + 1) % 50 == 0:
            logger.info(f"  컨센서스 조회 진행: {i+1}/{total} ({len(passed)}건 통과)")

        try:
            cons = fetch_consensus(code, use_cache=True, cache_hours=24)
        except Exception as e:
            errors += 1
            continue

        if not cons:
            no_consensus += 1
            continue

        target_price = cons.get("target_price", 0)
        analyst_count = cons.get("analyst_count", 0)
        con_per = cons.get("per", 0)

        # 애널리스트 2명 이상
        if analyst_count < 2:
            low_analyst += 1
            continue

        # 현재가 추정: PER * EPS (또는 캐시에서)
        eps = cons.get("eps", 0)
        if eps > 0 and con_per > 0:
            est_price = int(con_per * eps)
        else:
            # universe PER로 역산 시도
            est_price = 0

        if target_price <= 0:
            no_consensus += 1
            continue

        # 업사이드 계산 (est_price가 있으면 사용, 없으면 일단 포함)
        if est_price > 0:
            upside = (target_price / est_price - 1) * 100
        else:
            upside = 30.0  # 추정 불가 → 일단 포함

        if upside < 20:
            low_upside += 1
            continue

        c["target_price"] = target_price
        c["analyst_count"] = analyst_count
        c["consensus_per"] = con_per
        c["consensus_eps"] = eps
        c["est_price"] = est_price
        c["consensus_upside"] = round(upside, 1)
        passed.append(c)

    logger.info(
        f"Phase 2 완료: {total}종목 -> {len(passed)}종목 통과 "
        f"(컨센없음:{no_consensus} 업사이드부족:{low_upside} "
        f"애널부족:{low_analyst} 에러:{errors})"
    )
    return passed


# ═══════════════════════════════════════════════════════
#  Phase 3: 수급 검증 (네이버 금융)
# ═══════════════════════════════════════════════════════

def phase3_supply_check(candidates: list[dict]) -> list[dict]:
    """외국인/기관 5일 누적 순매수 조회"""
    from data.supply_naver import fetch_naver_supply

    total = len(candidates)
    for i, c in enumerate(candidates):
        code = c["code"]
        if (i + 1) % 20 == 0:
            logger.info(f"  수급 조회 진행: {i+1}/{total}")

        try:
            supply = fetch_naver_supply(code, days=5)
        except Exception:
            c["frgn_5d"] = 0
            c["inst_5d"] = 0
            c["supply_score"] = 0
            continue

        if not supply:
            c["frgn_5d"] = 0
            c["inst_5d"] = 0
            c["supply_score"] = 0
            continue

        frgn_sum = sum(d.get("frgn_net", 0) for d in supply)
        inst_sum = sum(d.get("inst_net", 0) for d in supply)

        c["frgn_5d"] = frgn_sum
        c["inst_5d"] = inst_sum

        # 수급 점수 (0~30)
        score = 0
        if frgn_sum > 0:
            score += min(frgn_sum / 1e8, 15)
        if inst_sum > 0:
            score += min(inst_sum / 1e8, 15)
        # 쌍순매수 보너스
        if frgn_sum > 0 and inst_sum > 0:
            score += 5

        c["supply_score"] = round(score, 1)

    logger.info(f"Phase 3 완료: {total}종목 수급 조회")
    return candidates


# ═══════════════════════════════════════════════════════
#  Phase 4: 종합 스코어링
# ═══════════════════════════════════════════════════════

def phase4_scoring(candidates: list[dict], top_n: int = 20) -> list[dict]:
    """
    종합 점수 = 컨센업사이드(35%) + PBR할인(20%) + PER할인(15%) + 수급(20%) + 애널수(10%)
    """
    for c in candidates:
        # 1) 컨센서스 업사이드 점수 (0~35) — 업사이드 20%=0, 100%=35
        upside = c.get("consensus_upside", 0)
        consensus_score = min((upside - 20) / 80 * 35, 35) if upside > 20 else 0

        # 2) PBR 할인 점수 (0~20) — PBR 1.5=0, 0.3=20
        pbr = c.get("pbr", 1.5)
        pbr_score = min((1.5 - pbr) / 1.2 * 20, 20) if pbr < 1.5 else 0

        # 3) PER 할인 점수 (0~15) — PER 20=0, 3=15
        per = c.get("per", 20)
        per_score = min((20 - per) / 17 * 15, 15) if per < 20 else 0

        # 4) 수급 점수 (0~20) — supply_score 0=0, 35=20
        supply = c.get("supply_score", 0)
        supply_score = min(supply / 35 * 20, 20)

        # 5) 애널리스트 커버리지 (0~10) — 2명=2, 10명+=10
        analyst = c.get("analyst_count", 0)
        analyst_score = min(analyst, 10)

        total = consensus_score + pbr_score + per_score + supply_score + analyst_score
        c["score_detail"] = {
            "consensus": round(consensus_score, 1),
            "pbr": round(pbr_score, 1),
            "per": round(per_score, 1),
            "supply": round(supply_score, 1),
            "analyst": round(analyst_score, 1),
        }
        c["total_score"] = round(total, 1)

    # 정렬
    candidates.sort(key=lambda x: x["total_score"], reverse=True)

    logger.info(f"Phase 4 완료: TOP {min(top_n, len(candidates))}개 선정")
    return candidates[:top_n]


# ═══════════════════════════════════════════════════════
#  Phase 5: KIS API 현재가 조회 (TOP N)
# ═══════════════════════════════════════════════════════

def phase5_price_check(gems: list[dict]) -> list[dict]:
    """TOP 종목의 실시간 현재가 조회"""
    try:
        from bot.kis_trader import KISTrader
        trader = KISTrader()
    except Exception as e:
        logger.warning(f"KIS 트레이더 초기화 실패: {e} — 가격 조회 생략")
        return gems

    for c in gems:
        try:
            p = trader.fetch_price(c["code"])
            if p and p.get("success"):
                c["current_price"] = p["current_price"]
                c["day_change"] = p.get("change_pct", 0)
                # 실제 업사이드 재계산
                if c["current_price"] > 0 and c.get("target_price", 0) > 0:
                    c["real_upside"] = round(
                        (c["target_price"] / c["current_price"] - 1) * 100, 1
                    )
            time.sleep(0.15)
        except Exception as e:
            logger.debug(f"{c['name']} 가격조회 실패: {e}")

    return gems


# ═══════════════════════════════════════════════════════
#  출력 + 저장
# ═══════════════════════════════════════════════════════

def print_report(gems: list[dict]):
    """콘솔 보고서 출력"""
    print("\n" + "=" * 80)
    print("  HIDDEN GEMS REPORT - Body Hunter v4")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M')} | {len(gems)}종목")
    print("=" * 80)

    for i, g in enumerate(gems, 1):
        name = g["name"]
        code = g["code"]
        sector = g.get("sector", "")
        per = g.get("per", 0)
        pbr = g.get("pbr", 0)
        cap = g.get("cap_억", 0)
        score = g.get("total_score", 0)

        # 가격 정보
        price = g.get("current_price", 0)
        tp = g.get("target_price", 0)
        real_up = g.get("real_upside", g.get("consensus_upside", 0))
        day_chg = g.get("day_change", 0)

        # 수급
        frgn = g.get("frgn_5d", 0)
        inst = g.get("inst_5d", 0)
        analyst = g.get("analyst_count", 0)

        # 스코어 상세
        sd = g.get("score_detail", {})

        print(f"\n{'─' * 60}")
        print(f"  #{i} {name} ({code}) | {sector} | {g.get('market','')}")
        print(f"  {'─' * 56}")

        if price > 0:
            print(f"  현재가: {price:,}원 ({day_chg:+.1f}%) | 목표가: {tp:,}원 | 업사이드: {real_up:+.1f}%")
        else:
            print(f"  목표가: {tp:,}원 | 추정업사이드: {real_up:+.1f}%")

        print(f"  PER: {per:.1f} | PBR: {pbr:.2f} | 시총: {cap:,}억 | 애널: {analyst}명")

        # 수급
        frgn_억 = frgn / 1e8 if frgn else 0
        inst_억 = inst / 1e8 if inst else 0
        frgn_icon = "+" if frgn > 0 else ""
        inst_icon = "+" if inst > 0 else ""
        print(f"  5일수급 | 외인: {frgn_icon}{frgn_억:.1f}억 | 기관: {inst_icon}{inst_억:.1f}억")

        # 종합 점수
        print(f"  SCORE: {score:.1f}/100 "
              f"(컨센:{sd.get('consensus',0)} PBR:{sd.get('pbr',0)} "
              f"PER:{sd.get('per',0)} 수급:{sd.get('supply',0)} 애널:{sd.get('analyst',0)})")

    print(f"\n{'=' * 80}")
    print(f"  스크리닝 기준: PBR<1.5 + PER<20 + 시총500~100,000억 + 컨센업사이드20%+")
    print(f"  수급: 네이버금융 외국인/기관 5일 누적")
    print(f"  주의: 컨센서스 목표가는 참고용. 반드시 개별 종목 분석 후 투자 결정!")
    print("=" * 80 + "\n")


def save_results(gems: list[dict]):
    """결과 JSON 저장"""
    output = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "criteria": {
            "pbr_max": 1.5,
            "per_range": "0~20",
            "cap_range": "500~100,000억",
            "consensus_upside_min": "20%",
            "analyst_min": 2,
        },
        "count": len(gems),
        "gems": gems,
    }
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    logger.info(f"결과 저장: {OUTPUT_PATH}")


# ═══════════════════════════════════════════════════════
#  메인
# ═══════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="Hidden Gems Screener")
    parser.add_argument("--top", type=int, default=20, help="상위 N개 (기본 20)")
    parser.add_argument("--skip-supply", action="store_true", help="수급 조회 생략 (빠르게)")
    parser.add_argument("--skip-price", action="store_true", help="현재가 조회 생략")
    args = parser.parse_args()

    t0 = time.time()

    # Phase 1: 기본 필터
    logger.info("=== Phase 1: 유니버스 기본 필터 ===")
    candidates = phase1_universe_filter()
    if not candidates:
        logger.error("Phase 1 결과 0건 — 종료")
        return

    # Phase 2: 컨센서스 검증
    logger.info("=== Phase 2: 컨센서스 검증 ===")
    candidates = phase2_consensus_check(candidates)
    if not candidates:
        logger.error("Phase 2 결과 0건 — 컨센서스 커버리지 종목 없음")
        return

    # Phase 3: 수급 검증
    if not args.skip_supply:
        logger.info("=== Phase 3: 수급 검증 (네이버) ===")
        candidates = phase3_supply_check(candidates)
    else:
        logger.info("=== Phase 3: 수급 조회 SKIP ===")
        for c in candidates:
            c["supply_score"] = 0

    # Phase 4: 종합 스코어링
    logger.info("=== Phase 4: 종합 스코어링 ===")
    top_gems = phase4_scoring(candidates, top_n=args.top)

    # Phase 5: 현재가 조회
    if not args.skip_price:
        logger.info("=== Phase 5: KIS 현재가 조회 ===")
        top_gems = phase5_price_check(top_gems)

    # 보고서 + 저장
    print_report(top_gems)
    save_results(top_gems)

    elapsed = int(time.time() - t0)
    m, s = divmod(elapsed, 60)
    logger.info(f"총 소요: {m}분 {s}초")


if __name__ == "__main__":
    main()
