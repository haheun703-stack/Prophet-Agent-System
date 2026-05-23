# -*- coding: utf-8 -*-
"""asset_pool × elliott_wave 통합 모듈 (사장님 5/22 20:00 G 통합 명령).

asset_pool_loader의 후보 종목들에 elliott 4파 분석 + metadata 가중치 적용.

호출 흐름 (5/26 D-Day 09:15):
    1. trading_coo._job_asset_pool_scan() → auto_trader.asset_pool_scan_and_buy()
    2. asset_pool_loader.get_diversified_candidates() → 분산 5종 후보
    3. ★ enrich_with_elliott(candidates, trader) ← 이 모듈 ★
    4. 각 후보별:
       - KIS fetch_daily_chart (일봉 200영업일)
       - detect_elliott_pattern (4파 분석)
       - boost_with_metadata (시총/섹터 가중치)
    5. score에 elliott_boost 합산 + 점수순 재정렬

영구 메모리:
  - [[feedback_verify_external_knowledge]] — 외부 자료 → 데이터 검증
  - [[project_5_22_evening_learning_fix]] — 풀세트 D + 엘리어트
"""
import logging
import time
from pathlib import Path
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


def _cap_band(cap_억: float) -> str:
    """시총 구간 분류 (백테스트 매트릭스 검증 — universe 200종)."""
    if cap_억 < 500:
        return "소형(100-500억)"
    elif cap_억 < 1000:
        return "중소형(500-1000억)"
    elif cap_억 < 5000:
        return "중대형(1000-5000억)"
    else:
        return "대형(5000억+)"


def _load_universe() -> Dict:
    """universe.json 로드 (cap/sector metadata)."""
    try:
        import json
        path = Path(__file__).resolve().parent.parent / "data_store" / "universe.json"
        if not path.exists():
            logger.warning("universe.json 없음 — metadata 가중치 적용 X")
            return {}
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        logger.warning(f"universe.json 로드 실패: {e}")
        return {}


def enrich_with_elliott(
    candidates: List[Dict],
    trader,
    delay: float = 0.3,
    daily_bars_min: int = 20,
    log_each: bool = True,
) -> List[Dict]:
    """후보 종목에 elliott 점수 + metadata 가중치 적용 + 재정렬.

    Args:
        candidates: [{code, name, score, sources, ...}] 후보 리스트
        trader: KISTrader 인스턴스 (fetch_daily_chart 호출용)
        delay: KIS API 호출 간 대기 (rate limit, 기본 0.3초)
        daily_bars_min: 최소 일봉 개수 (미달 시 skip)
        log_each: 각 종목 결과 로그 출력

    Returns:
        elliott_boost / elliott_zone / elliott_signal 필드 추가된 candidates,
        score 합산 후 점수순 재정렬.

    Note:
        - candidates의 score 필드를 elliott_boost만큼 가산
        - 4파 미감지 또는 일봉 부족 시 elliott_boost=0 (변화 X)
        - 5/22 백테스트 검증된 zone × cap × sector 가중치 적용
    """
    if not candidates:
        return []

    # 지연 import (circular 회피)
    try:
        from bot.elliott_wave import detect_elliott_pattern, boost_with_metadata
    except Exception as e:
        logger.error(f"elliott_wave import 실패: {e}")
        return candidates

    universe = _load_universe()

    enriched = []
    for c in candidates:
        code = c.get("code")
        if not code:
            enriched.append(c)
            continue

        # 기본값 (실패 케이스 대비)
        c.setdefault("elliott_boost", 0)
        c.setdefault("elliott_zone", "unknown")
        c.setdefault("elliott_signal", "NO_DATA")

        try:
            bars = trader.fetch_daily_chart(code)
            if not bars or len(bars) < daily_bars_min:
                if log_each:
                    logger.debug(f"[elliott_enrich] {code}: 일봉 부족 ({len(bars or [])}봉)")
                enriched.append(c)
                continue

            result = detect_elliott_pattern(bars)

            # metadata
            info = universe.get(code, {}) if isinstance(universe, dict) else {}
            cap = info.get("cap_억", 0) if isinstance(info, dict) else 0
            cap_band = _cap_band(cap)
            sector = info.get("sector", "") if isinstance(info, dict) else ""

            boost = boost_with_metadata(result, cap_band=cap_band, sector=sector)
            c["elliott_boost"] = boost
            c["elliott_zone"] = result.fib_zone_name
            c["elliott_signal"] = result.fib_zone_signal
            c["elliott_fib_pct"] = result.fib_retracement_pct
            c["cap_band"] = cap_band
            c["sector_for_elliott"] = sector

            # score 합산 (기존 score + elliott_boost)
            old_score = c.get("score", 0)
            c["score"] = old_score + boost
            c["score_breakdown"] = {
                "base": old_score,
                "elliott_boost": boost,
                "total": c["score"],
            }

            if log_each:
                logger.info(
                    f"[elliott_enrich] {code} {c.get('name', '?')[:8]:<8} "
                    f"zone={result.fib_zone_name:<18} signal={result.fib_zone_signal:<12} "
                    f"cap={cap_band[:10]:<10} sector={sector[:6]:<6} "
                    f"boost={boost:+d} → score {old_score} → {c['score']}"
                )

            if delay > 0:
                time.sleep(delay)
        except Exception as e:
            logger.warning(f"[elliott_enrich] {code} 실패 (무시): {e}")

        enriched.append(c)

    # 점수 순 재정렬
    enriched.sort(key=lambda x: -x.get("score", 0))
    return enriched


def merge_and_cross_validate_jgis(
    dantabot_candidates: List[Dict],
    jgis_top_limit: int = 10,
) -> List[Dict]:
    """단타봇 자체 candidates × 정보봇 STRONG_BUY 교차 검증 (사장님 5/22 21:00 명령).

    ★ 사장님 원칙 ★: "정보봇도 다 믿지 마라. 단타봇 자체와 비교해서 확인"

    교차 검증 4가지 시나리오:
      1. 단타봇 + 정보봇 둘 다 시그널: +50 보너스 (★ 강한 매수)
      2. 단타봇 단독: 그대로 (자체 시그널 신뢰)
      3. 정보봇 단독: +20 합류 (보수 — 단타봇 발굴 X but jgis 검증)
      4. 정보봇 단독 + 후속 elliott AVOID: 회피 (cross_block 플래그)

    Args:
        dantabot_candidates: get_diversified_candidates 결과 (단타봇 자체)
        jgis_top_limit: 정보봇 STRONG_BUY 상위 N건

    Returns:
        합집합 candidates with:
          - cross_validated: bool (단타봇+정보봇 동시)
          - cross_status: "both" / "dantabot_only" / "jgis_only" / "jgis_blocked"
          - cross_bonus: +50 / 0 / +20 / 0
          - jgis_signal: {sniper, smart_money, supply, composite}
    """
    # 단타봇 candidates code set
    dantabot_codes = {c.get("code") for c in dantabot_candidates}

    # 정보봇 STRONG_BUY 가져오기
    jgis_codes = set()
    jgis_sigs_by_code = {}
    try:
        from bot.jgis_signal_consumer import top_strong_buy_signals
        tops = top_strong_buy_signals(limit=jgis_top_limit)
        for sig in tops:
            jgis_codes.add(sig.code)
            jgis_sigs_by_code[sig.code] = {
                "sniper_signal": sig.sniper_signal,
                "sniper_score": sig.sniper_score,
                "smart_money_signal": sig.smart_money_signal,
                "smart_money_score": sig.smart_money_score,
                "supply_grade": sig.supply_grade,
                "supply_score": sig.supply_score,
                "composite": sig.composite_score,
                "name": sig.name,
                "sector": sig.sector,
            }
    except Exception as e:
        logger.warning(f"[cross_validate] jgis 조회 실패: {e}")

    if not jgis_codes:
        # 정보봇 시그널 없음 → 단타봇 그대로
        for c in dantabot_candidates:
            c["cross_status"] = "dantabot_only"
            c["cross_validated"] = False
            c["cross_bonus"] = 0
        return dantabot_candidates

    merged = []
    overlap = dantabot_codes & jgis_codes  # 1. 둘 다 시그널

    # 단타봇 candidates 처리
    for c in dantabot_candidates:
        code = c.get("code")
        if code in overlap:
            # ★ 시나리오 1: 단타봇 + 정보봇 둘 다 → 강한 매수 ★
            c["cross_status"] = "both"
            c["cross_validated"] = True
            c["cross_bonus"] = 50
            c["score"] = c.get("score", 0) + 50
            c["jgis_signal"] = jgis_sigs_by_code[code]
            logger.info(
                f"[cross_validate] ★ DOUBLE ★ {code} {c.get('name', '')[:8]} "
                f"단타봇+정보봇 동시 → +50 (jgis={jgis_sigs_by_code[code]['sniper_signal']})"
            )
        else:
            # 시나리오 2: 단타봇 단독 → 그대로
            c["cross_status"] = "dantabot_only"
            c["cross_validated"] = False
            c["cross_bonus"] = 0
        merged.append(c)

    # 정보봇 단독 종목 합류 (시나리오 3)
    jgis_only_codes = jgis_codes - dantabot_codes
    for code in jgis_only_codes:
        sig = jgis_sigs_by_code[code]
        merged.append({
            "code": code,
            "name": sig["name"],
            "score": int(sig["composite"]) + 20,  # composite + 20 보너스
            "sources": ["jgis_strong_buy"],
            "category": "jgis_priority",
            "cross_status": "jgis_only",
            "cross_validated": False,
            "cross_bonus": 20,
            "jgis_signal": sig,
        })
        logger.info(
            f"[cross_validate] JGIS_ONLY {code} {sig['name'][:8]} "
            f"정보봇 단독 → +20 (단타봇 X / jgis_composite={sig['composite']})"
        )

    return merged


def apply_elliott_cross_block(candidates: List[Dict]) -> List[Dict]:
    """elliott zone × jgis 시그널 교차 검증 차단 (시나리오 4).

    정보봇 단독 종목이 elliott AVOID/NO_ENTRY zone이면 cross_block:
      - jgis가 STRONG_BUY 말하지만 elliott 분석 결과 추세 종료 의심
      - 단타봇 elliott 신뢰 (자체 분석 우선)
      - score를 큰 폭 차감 (사실상 매수 차단)

    enrich_with_elliott 호출 후에 호출.
    """
    blocked = 0
    for c in candidates:
        cross_status = c.get("cross_status", "")
        elliott_signal = c.get("elliott_signal", "")
        if cross_status == "jgis_only" and elliott_signal in ("AVOID", "NO_ENTRY"):
            old_score = c.get("score", 0)
            c["score"] = old_score - 100  # 사실상 매수 차단
            c["cross_status"] = "jgis_blocked"
            c["cross_block_reason"] = f"jgis STRONG_BUY but elliott {elliott_signal}"
            blocked += 1
            logger.warning(
                f"[cross_block] {c['code']} {c.get('name', '')[:8]} — "
                f"jgis STRONG_BUY but elliott {elliott_signal} → 매수 차단 (-100)"
            )
    if blocked > 0:
        logger.info(f"[cross_block] {blocked}건 차단 — 단타봇 elliott 우선")
        candidates.sort(key=lambda x: -x.get("score", 0))
    return candidates


def apply_korean_surge_pattern_bonus(
    candidates: List[Dict],
    log_each: bool = True,
) -> List[Dict]:
    """★ 5/23 토 사장님 결정 B ★ 한국형 V자 반등 zone 보너스.

    surge_elliott_correlation 발견:
      - 상한가 직전 70%가 korean_low/trend_caution/trend_end 영역
      - 외부 책 'sweet 38.2%' = 한국 상한가 직전 ≠ (6.1% 평균)
      - 한국형 = 깊은 되돌림 후 V자 반등 (62-100%+)

    zone × consec 매트릭스 (raw 30건):
      - korean_low (62-80%) × 2회차: 8건 ← V자 반등 패턴
      - trend_caution (80-100%) × 2회차: 6건 ← 강력 추세
      - trend_end (100%+) × 3회차: 2건 (SK증권우 fib 122%) ← 진짜 강세

    적용 룰:
      - korean_low + consec >= 2 → +10 (V_RECOVERY)
      - trend_caution + consec >= 2 → +15 (STRONG_PUSH)
      - trend_end + consec >= 3 → +20 (BREAKOUT — 진짜 추세)
      - 그 외 → 0

    Args:
        candidates: enrich_with_elliott + apply_surge_recurrence_bonus 적용 후 후보
                    elliott_zone + surge_recurrence_consec 필드 필요
        log_each: 각 종목 로그 출력

    Returns:
        보너스 적용된 candidates (점수 정렬 X — 호출자에서 정렬)

    Note:
        - surge_recurrence_consec이 없으면 0회로 간주 (보너스 X)
        - elliott_zone이 'unknown'이면 보너스 X
        - sweet (fib_38_safe) 종목은 이미 boost_with_metadata에서 +25 받음
          이 함수는 외부 책 sweet 아닌 한국형 zone만 추가 보너스
    """
    if not candidates:
        return candidates

    bonus_applied = 0
    pattern_dist = {"V_RECOVERY": 0, "STRONG_PUSH": 0, "BREAKOUT": 0, "NONE": 0}

    for c in candidates:
        zone = c.get("elliott_zone", "")
        consec = c.get("surge_recurrence_consec", 0) or 0

        bonus = 0
        pattern = "NONE"

        if zone == "korean_low" and consec >= 2:
            bonus = 10
            pattern = "V_RECOVERY"
        elif zone == "trend_caution" and consec >= 2:
            bonus = 15
            pattern = "STRONG_PUSH"
        elif zone == "trend_end" and consec >= 3:
            bonus = 20
            pattern = "BREAKOUT"

        c["korean_pattern"] = pattern
        c["korean_pattern_bonus"] = bonus

        if bonus > 0:
            old_score = c.get("score", 0)
            c["score"] = old_score + bonus
            bonus_applied += 1
            pattern_dist[pattern] += 1
            if log_each:
                logger.info(
                    f"[korean_surge] {pattern} {c['code']} {c.get('name','')[:8]} "
                    f"zone={zone} consec={consec} → +{bonus} "
                    f"(score {old_score} → {c['score']})"
                )
        else:
            pattern_dist["NONE"] += 1

    logger.info(
        f"[korean_surge] 적용 — 총 {len(candidates)}종 / 보너스 {bonus_applied}건 / "
        f"분포: V_RECOVERY {pattern_dist['V_RECOVERY']} / "
        f"STRONG_PUSH {pattern_dist['STRONG_PUSH']} / "
        f"BREAKOUT {pattern_dist['BREAKOUT']}"
    )

    # 점수 순 재정렬
    candidates.sort(key=lambda x: -x.get("score", 0))
    return candidates


def apply_surge_recurrence_bonus(
    candidates: List[Dict],
    days_back: int = 2,
    base_bonus: int = 20,
    consecutive_bonus: int = 5,
) -> List[Dict]:
    """★ 5/23 토 사장님 결정 A-2 ★ 재상한가 사이클 watchlist 보너스.

    5/23 백테스트 발견 (surge_elliott_correlation):
      - 1개월 상한가의 10.6%가 평균 2일 만에 다시 +25%+ 폭발
      - 연속 상한가 분포: 2회차 85% / 3회차 15%
      - 즉 "어제 상한가 친 종목 → 오늘 또 가능성 ★"

    동작:
      - daily_limit_up_history WHERE date >= (CURRENT_DATE - days_back)
      - 후보에 이미 있으면 score + base_bonus + consecutive_bonus × (consec-1)
      - 후보에 없으면 새 후보 추가 (score = 50 + base_bonus + consec_bonus)
      - sources에 "surge_recurrence" 태그 추가
      - cross_status 유지 (jgis/elliott과 독립 룰)

    Args:
        candidates: enrich_with_elliott 결과 (또는 그 전 단계)
        days_back: 며칠 전까지 상한가 (기본 2일 — 5/23 평균 2일)
        base_bonus: 기본 보너스 (기본 +20)
        consecutive_bonus: 연속 횟수당 추가 (기본 +5)

    Returns:
        보너스 적용된 candidates (점수 정렬 X — 호출자에서 정렬)
    """
    if not candidates and days_back <= 0:
        return candidates

    # 어제~그저께 상한가 종목 조회
    surge_codes = {}  # code → {consecutive, date}
    try:
        from utils.supabase_sql import query
        sql = """
            SELECT ticker, name, date, COALESCE(consecutive_days, 1) AS consec
            FROM daily_limit_up_history
            WHERE date >= CURRENT_DATE - INTERVAL '%s days'
              AND date < CURRENT_DATE
            ORDER BY consec DESC, date DESC
        """
        rows = query(sql, (days_back,))
        for r in (rows or []):
            code = r.get("ticker", "")
            if code:
                # 가장 최근 + 가장 높은 consec 우선
                if code not in surge_codes or r.get("consec", 1) > surge_codes[code]["consec"]:
                    surge_codes[code] = {
                        "consec": r.get("consec", 1),
                        "date": str(r.get("date", "")),
                        "name": r.get("name", ""),
                    }
    except Exception as e:
        logger.warning(f"[surge_recurrence] DB 조회 실패: {e}")
        return candidates

    if not surge_codes:
        logger.info(f"[surge_recurrence] 최근 {days_back}일 상한가 0건 — 보너스 X")
        return candidates

    # 기존 후보 코드 set
    existing = {c.get("code"): i for i, c in enumerate(candidates)}

    bonus_applied = 0
    new_added = 0

    # 기존 후보에 보너스 적용
    for code, info in surge_codes.items():
        consec = info["consec"]
        bonus = base_bonus + consecutive_bonus * (consec - 1)

        if code in existing:
            idx = existing[code]
            c = candidates[idx]
            old_score = c.get("score", 0)
            c["score"] = old_score + bonus
            c["surge_recurrence_bonus"] = bonus
            c["surge_recurrence_consec"] = consec
            c["surge_recurrence_date"] = info["date"]
            srcs = c.get("sources", [])
            if "surge_recurrence" not in srcs:
                srcs.append("surge_recurrence")
                c["sources"] = srcs
            bonus_applied += 1
            logger.info(
                f"[surge_recurrence] BONUS {code} {info['name'][:8]} "
                f"consec={consec} → +{bonus} (score {old_score} → {c['score']})"
            )
        else:
            # 새 후보 추가 (base 50 + bonus)
            new_score = 50 + bonus
            candidates.append({
                "code": code,
                "name": info["name"],
                "score": new_score,
                "sources": ["surge_recurrence"],
                "category": "surge_recurrence",
                "surge_recurrence_bonus": bonus,
                "surge_recurrence_consec": consec,
                "surge_recurrence_date": info["date"],
                "cross_status": "surge_recurrence",
                "cross_validated": False,
                "cross_bonus": 0,
            })
            new_added += 1
            logger.info(
                f"[surge_recurrence] NEW {code} {info['name'][:8]} "
                f"consec={consec} → score {new_score}"
            )

    logger.info(
        f"[surge_recurrence] 적용 완료 — DB {len(surge_codes)}건 / "
        f"기존 +bonus {bonus_applied}건 / 신규 {new_added}건"
    )

    # 점수 순 재정렬
    candidates.sort(key=lambda x: -x.get("score", 0))
    return candidates


def summarize_elliott_distribution(candidates: List[Dict]) -> Dict:
    """elliott 적용 후보 분포 요약 (보고용)."""
    if not candidates:
        return {"total": 0, "zone_distribution": {}, "avg_boost": 0}

    zone_count = {}
    boosts = []
    for c in candidates:
        z = c.get("elliott_zone", "unknown")
        zone_count[z] = zone_count.get(z, 0) + 1
        b = c.get("elliott_boost", 0)
        if isinstance(b, (int, float)):
            boosts.append(b)

    summary = {
        "total": len(candidates),
        "zone_distribution": zone_count,
        "avg_boost": round(sum(boosts) / len(boosts), 1) if boosts else 0,
        "max_boost": max(boosts) if boosts else 0,
        "min_boost": min(boosts) if boosts else 0,
        "top_candidate": {
            "code": candidates[0].get("code"),
            "name": candidates[0].get("name"),
            "score": candidates[0].get("score"),
            "elliott_zone": candidates[0].get("elliott_zone"),
        } if candidates else None,
    }
    return summary


if __name__ == "__main__":
    import json as _json

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
    print("=== asset_pool_elliott 데모 (Mock trader) ===\n")

    # Mock trader
    class MockTrader:
        def fetch_daily_chart(self, code):
            # 데모: 20봉 단순 패턴
            return [
                {"date": f"2026-05-{i:02d}", "open": 100 + i, "high": 100 + i + 2,
                 "low": 100 + i - 1, "close": 100 + i + 1, "volume": 1000}
                for i in range(1, 21)
            ]

    candidates = [
        {"code": "005930", "name": "삼성전자", "score": 100, "sources": ["limit_up"]},
        {"code": "204320", "name": "HL만도", "score": 80, "sources": ["nxt"]},
    ]
    enriched = enrich_with_elliott(candidates, MockTrader(), delay=0, log_each=True)
    print("\n=== 결과 ===")
    for c in enriched:
        print(_json.dumps(c, ensure_ascii=False, default=str)[:200])
    print("\n=== 요약 ===")
    print(_json.dumps(summarize_elliott_distribution(enriched), ensure_ascii=False, indent=2))
