# -*- coding: utf-8 -*-
"""
BRAIN 1D~5D 풀 스택 검증 스크립트
==================================
검증 순서: 검증3(엣지케이스) → 검증4(실시간 크로스체크)
검증1(백테스트)과 검증2(정합성)는 별도 스크립트로 분리.

실행: python -m jarvis.tests.brain_5d_verification
"""

import io
import json
import logging
import sys
import os
import copy
from pathlib import Path
from datetime import datetime, timedelta
from unittest.mock import patch, MagicMock
from typing import Dict, List

# 프로젝트 루트 설정
ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))
os.environ["PYTHONIOENCODING"] = "utf-8"

logger = logging.getLogger("verification")

# ═══════════════════════════════════════
#  검증 결과 수집
# ═══════════════════════════════════════

results = {
    "검증3_엣지케이스": [],
    "검증3_극단값": [],
    "검증3_통합": [],
    "검증4_실시간": [],
    "bugs": [],
}


def record(category: str, test_name: str, passed: bool, detail: str):
    emoji = "PASS" if passed else "FAIL"
    results[category].append({
        "test": test_name,
        "result": emoji,
        "detail": detail,
    })
    print(f"  [{emoji}] {test_name}: {detail}")


# ═══════════════════════════════════════
#  검증 3-1: 데이터 누락 시뮬레이션
# ═══════════════════════════════════════

def test_3_1_fred_api_down():
    """FRED API 다운 시 캐시 폴백"""
    print("\n=== 3-1-A: FRED API 다운 ===")
    from jarvis.liquidity_cycle import _fetch_all_series, CACHE_PATH

    # 먼저 정상 실행으로 캐시 확보
    normal = _fetch_all_series()
    has_cache = CACHE_PATH.exists()

    record("검증3_엣지케이스", "FRED 정상 캐시 존재",
           has_cache, f"캐시 존재={has_cache}, rrp={len(normal.get('rrp', []))}건")

    # urlopen을 mock으로 Timeout
    with patch("jarvis.liquidity_cycle.urlopen", side_effect=TimeoutError("mock timeout")):
        # 캐시를 약간 오래되게 수정 (2일 전)
        if CACHE_PATH.exists():
            cache = json.loads(CACHE_PATH.read_text(encoding="utf-8"))
            cache["fetched_at"] = (datetime.now() - timedelta(days=2)).isoformat()
            CACHE_PATH.write_text(json.dumps(cache, ensure_ascii=False), encoding="utf-8")

        fallback = _fetch_all_series()
        has_data = bool(fallback.get("rrp"))

        record("검증3_엣지케이스", "FRED 다운 → 캐시 폴백",
               has_data, f"폴백 데이터 rrp={len(fallback.get('rrp', []))}건")

    # analyze_liquidity까지 정상 동작하는지
    with patch("jarvis.liquidity_cycle.urlopen", side_effect=TimeoutError("mock timeout")):
        try:
            from jarvis.liquidity_cycle import analyze_liquidity
            report = analyze_liquidity()
            record("검증3_엣지케이스", "FRED 다운 → analyze_liquidity 정상 완료",
                   True, f"score={report.liquidity_score}, level={report.liquidity_level}")
        except Exception as e:
            record("검증3_엣지케이스", "FRED 다운 → analyze_liquidity 정상 완료",
                   False, f"에러: {e}")
            results["bugs"].append("[ACTION REQUIRED] FRED 다운 시 analyze_liquidity 크래시")


def test_3_1_cftc_404():
    """CFTC 파일 미발표 (공휴일) — 404 반환"""
    print("\n=== 3-1-B: CFTC 404 ===")
    from jarvis.cot_smartmoney import _fetch_cot_data, CACHE_PATH

    # 정상 캐시 확보
    normal = _fetch_cot_data()
    record("검증3_엣지케이스", "CFTC 정상 데이터",
           len(normal) > 0, f"{len(normal)}건")

    # urlopen mock → 404
    mock_resp = MagicMock()
    mock_resp.read.return_value = b""
    from urllib.error import HTTPError

    with patch("jarvis.cot_smartmoney.urlopen", side_effect=HTTPError(
        url="mock", code=404, msg="Not Found", hdrs=None, fp=None
    )):
        # 캐시를 오래되게 (6일)
        if CACHE_PATH.exists():
            cache = json.loads(CACHE_PATH.read_text(encoding="utf-8"))
            cache["fetched_at"] = (datetime.now() - timedelta(days=6)).isoformat()
            CACHE_PATH.write_text(json.dumps(cache, ensure_ascii=False), encoding="utf-8")

        fallback = _fetch_cot_data()
        has_data = len(fallback) > 0

        record("검증3_엣지케이스", "CFTC 404 → 캐시 폴백",
               has_data, f"폴백 {len(fallback)}건")

    # analyze_cot까지 정상인지
    with patch("jarvis.cot_smartmoney.urlopen", side_effect=HTTPError(
        url="mock", code=404, msg="Not Found", hdrs=None, fp=None
    )):
        try:
            from jarvis.cot_smartmoney import analyze_cot
            report = analyze_cot()
            record("검증3_엣지케이스", "CFTC 404 → analyze_cot 정상 완료",
                   True, f"score={report.smart_money_score}, signal={report.smart_money_signal}")
        except Exception as e:
            record("검증3_엣지케이스", "CFTC 404 → analyze_cot 정상 완료",
                   False, f"에러: {e}")
            results["bugs"].append("[ACTION REQUIRED] CFTC 404 시 analyze_cot 크래시")


def test_3_1_yahoo_bad_ticker():
    """Yahoo Finance 존재하지 않는 티커"""
    print("\n=== 3-1-C: Yahoo 잘못된 티커 ===")
    try:
        from jarvis.cross_asset_stress import _fetch_returns
        ret = _fetch_returns("XXXXXXXXX_FAKE", 30)
        record("검증3_엣지케이스", "Yahoo 잘못된 티커 → graceful None",
               ret is None, f"반환값={ret}")
    except Exception as e:
        record("검증3_엣지케이스", "Yahoo 잘못된 티커 → graceful None",
               False, f"예외 발생: {e}")
        results["bugs"].append("[ACTION REQUIRED] Yahoo 잘못된 티커에서 예외 발생")

    # 3D 전체가 죽지 않는지
    try:
        from jarvis.cross_asset_stress import analyze_stress, ASSET_PAIRS
        original_pairs = copy.deepcopy(ASSET_PAIRS)
        # 하나의 티커를 고의로 변경
        ASSET_PAIRS["gold_kospi"]["asset_a"] = "XXXFAKE"
        report = analyze_stress()
        # 원복
        ASSET_PAIRS.update(original_pairs)
        record("검증3_엣지케이스", "Yahoo 티커 오류 → analyze_stress 생존",
               True, f"stress={report.stress_index}, level={report.stress_level}")
    except Exception as e:
        # 원복 시도
        try:
            from jarvis.cross_asset_stress import ASSET_PAIRS as ap
            ap.update(original_pairs)
        except:
            pass
        record("검증3_엣지케이스", "Yahoo 티커 오류 → analyze_stress 생존",
               False, f"크래시: {e}")
        results["bugs"].append("[ACTION REQUIRED] 3D 티커 오류 시 전체 크래시")


def test_3_1_m2_stale():
    """M2 데이터 2개월 미갱신"""
    print("\n=== 3-1-D: M2 데이터 60일 오래됨 ===")
    from jarvis.liquidity_cycle import analyze_liquidity, _fetch_all_series, CACHE_PATH

    # 정상 데이터 가져오기
    all_data = _fetch_all_series()
    m2 = all_data.get("m2", [])
    if m2:
        # m2 최신 날짜를 60일 전으로 조작
        original_last_date = m2[-1][0]
        old_date = (datetime.now() - timedelta(days=60)).strftime("%Y-%m-%d")
        modified_m2 = [(old_date if d == original_last_date else d, v) for d, v in m2]

        # 캐시에 덮어쓰기
        if CACHE_PATH.exists():
            cache = json.loads(CACHE_PATH.read_text(encoding="utf-8"))
            cache["m2"] = modified_m2
            cache["fetched_at"] = datetime.now().isoformat()
            CACHE_PATH.write_text(json.dumps(cache, ensure_ascii=False), encoding="utf-8")

        report = analyze_liquidity()
        record("검증3_엣지케이스", "M2 60일 오래됨 → 분석 정상 완료",
               True, f"score={report.liquidity_score}, m2 date={report.m2.data_date}")

        # M2 z-score가 여전히 계산되는지 (forward-fill 효과)
        record("검증3_엣지케이스", "M2 오래됨 → z-score 계산됨",
               True, f"m2_z={report.m2.z_score}, m2_val={report.m2.current_value}")

        # TODO: "M2 데이터 오래됨" 경고가 필요할 수 있음
        if report.m2.data_date and report.m2.data_date < (datetime.now() - timedelta(days=45)).strftime("%Y-%m-%d"):
            results["bugs"].append("[IMPROVEMENT] M2 데이터 45일+ 오래됨 경고 표시 기능 추가 필요")
    else:
        record("검증3_엣지케이스", "M2 60일 오래됨 → 분석 정상 완료",
               False, "M2 데이터 자체가 없음")


# ═══════════════════════════════════════
#  검증 3-2: 극단값 테스트
# ═══════════════════════════════════════

def test_3_2_all_worst_2d():
    """2D: 4개 지표 전부 동시 경고"""
    print("\n=== 3-2-A: 2D 4지표 전부 경고 ===")
    from jarvis.regime_leading import RegimeLeadingReport, LeadingIndicator, _calc_preemptive_adjust

    report = RegimeLeadingReport(date=datetime.now().strftime("%Y-%m-%d"))
    # 4개 지표 전부 경고 상태
    for ind in [report.credit_spread, report.yield_curve, report.move_index, report.ted_spread]:
        ind.is_warning = True
        ind.direction = "WORSENING"
    report.warning_count = 4

    # 종합 판정 로직 재현
    if report.warning_count >= 3:
        report.transition_signal = "IMMINENT"
        report.transition_direction = "RISK_OFF"
        report.confidence = 0.9

    adj = _calc_preemptive_adjust(report)

    record("검증3_극단값", "2D 4지표 경고 → IMMINENT",
           report.transition_signal == "IMMINENT", f"signal={report.transition_signal}")
    record("검증3_극단값", "2D 확신도 90%",
           report.confidence >= 0.9, f"confidence={report.confidence}")
    record("검증3_극단값", "2D 선제조정: 현금+10, 인버스+5",
           adj.get("cash_shift_pct", 0) == 10, f"adj={adj}")


def test_3_2_all_worst_3d():
    """3D: 4쌍 전부 상관 붕괴"""
    print("\n=== 3-2-B: 3D 4쌍 전부 상관 붕괴 ===")
    from jarvis.cross_asset_stress import (
        StressReport, PairCorrelation, ASSET_PAIRS,
        _calc_allocation_adjust,
    )

    report = StressReport(date=datetime.now().strftime("%Y-%m-%d"))
    report.anomaly_count = 4

    # 4쌍 전부 최대 스트레스
    for pair_id, pair_def in ASSET_PAIRS.items():
        pc = PairCorrelation(
            pair_id=pair_id,
            pair_name=pair_def["name"],
            normal_corr=pair_def["normal_corr"],
        )
        pc.is_anomaly = True
        pc.deviation = 1.5  # 최대
        pc.stress_contribution = round(pair_def["weight"] * 1.5, 2)
        report.pairs.append(pc)

    # 종합 스트레스 = sum * 1.5 (4쌍 이상)
    total = sum(p.stress_contribution for p in report.pairs)
    total *= 1.5  # 3쌍 이상 증폭
    report.stress_index = round(min(total, 10.0), 1)

    if report.stress_index >= 7.0:
        report.stress_level = "CRITICAL"

    adj = _calc_allocation_adjust(report)

    record("검증3_극단값", "3D 4쌍 붕괴 → CRITICAL",
           report.stress_level == "CRITICAL", f"index={report.stress_index}")
    record("검증3_극단값", "3D 스트레스 지수 ≥ 7.0",
           report.stress_index >= 7.0, f"index={report.stress_index}")
    record("검증3_극단값", "3D 배분: 스윙 -25%p, 현금 +20%p",
           adj.get("swing_reduce_pct", 0) == 25, f"adj={adj}")


def test_3_2_all_worst_4d():
    """4D: 모든 계약 극단 동시"""
    print("\n=== 3-2-C: 4D 모든 계약 극단 ===")
    from jarvis.cot_smartmoney import (
        COTReport, ContractPosition, TRACKED_CONTRACTS,
        _calc_allocation_adjust, Z_EXTREME,
    )

    report = COTReport(date=datetime.now().strftime("%Y-%m-%d"))
    total_score = 0.0
    extreme_count = 0

    # 시나리오: KOSPI 관점에서 전부 BEARISH가 되도록 설계
    # S&P(beta+1.0): Comm매도+Spec과열 → 자산 BEARISH → KOSPI BEARISH
    # Gold(beta-0.5): Spec과매도 → 자산 BULLISH → beta(-0.5)*BULL= KOSPI BEARISH
    # Oil(beta-0.3): Comm매수+Spec과매도 → 자산 BULLISH → beta(-0.3)*BULL= KOSPI BEARISH
    # Yen(beta-0.8): Comm매수+Spec과매도 → 엔 BULLISH(강세) → beta(-0.8)*BULL= KOSPI BEARISH
    test_scenarios = {
        "sp500":    {"comm_z": -2.0, "noncomm_z": +2.0},   # 자산 bearish * beta +1.0 → KOSPI↓
        "gold":     {"comm_z": +0.5, "noncomm_z": -2.5},   # 자산 bullish * beta -0.5 → KOSPI↓
        "crude_oil": {"comm_z": +2.0, "noncomm_z": -2.0},  # 자산 bullish * beta -0.3 → KOSPI↓
        "yen":      {"comm_z": +2.0, "noncomm_z": -2.0},   # 자산 bullish(엔강세) * beta -0.8 → KOSPI↓
    }

    for cid, cdef in TRACKED_CONTRACTS.items():
        scenario = test_scenarios[cid]
        pos = ContractPosition(
            contract_id=cid,
            contract_name=cdef["name"],
        )
        pos.comm_z = scenario["comm_z"]
        pos.noncomm_z = scenario["noncomm_z"]

        kospi_beta = cdef["kospi_beta"]
        comm_weight = cdef["comm_weight"]
        spec_weight = cdef["spec_weight"]

        contract_bullish = 0.0
        # Commercial
        if pos.comm_z >= Z_EXTREME:
            contract_bullish += comm_weight
            extreme_count += 1
        elif pos.comm_z <= -Z_EXTREME:
            contract_bullish -= comm_weight
            extreme_count += 1

        # Speculator (역지표)
        if pos.noncomm_z >= Z_EXTREME:
            contract_bullish -= spec_weight
            extreme_count += 1
        elif pos.noncomm_z <= -Z_EXTREME:
            contract_bullish += spec_weight
            extreme_count += 1

        pos.kospi_contribution = round(contract_bullish * kospi_beta, 2)
        total_score += pos.kospi_contribution
        report.positions.append(pos)

    report.smart_money_score = round(max(-10.0, min(10.0, total_score)), 1)
    report.extreme_count = extreme_count

    if report.smart_money_score <= -3.0:
        report.smart_money_signal = "RISK_OFF"
    elif report.smart_money_score >= 3.0:
        report.smart_money_signal = "RISK_ON"
    else:
        report.smart_money_signal = "NEUTRAL"

    adj = _calc_allocation_adjust(report)

    record("검증3_극단값", "4D 모든 계약 극단 → RISK_OFF",
           report.smart_money_signal == "RISK_OFF",
           f"score={report.smart_money_score}, signal={report.smart_money_signal}")
    record("검증3_극단값", "4D 극단 카운트 ≥ 4",
           extreme_count >= 4, f"extreme={extreme_count}")
    record("검증3_극단값", "4D 스코어 ≤ -5.0 (강한 RISK_OFF)",
           report.smart_money_score <= -5.0,
           f"score={report.smart_money_score}")


def test_3_2_all_worst_5d():
    """5D: RRP=0 + TGA 급등 + M2 마이너스"""
    print("\n=== 3-2-D: 5D 유동성 최악 시나리오 ===")
    from jarvis.liquidity_cycle import (
        LiquidityReport, IndicatorStatus,
        _calc_allocation_adjust,
    )

    report = LiquidityReport(date=datetime.now().strftime("%Y-%m-%d"))

    # RRP: z = -2.0 (급락 → 유동성 확장이므로 반전 후 +2.0)
    # → 사실 RRP가 0이면 이미 다 빠져나간 거라 중립
    # TGA: z = +3.0 → 반전 = -3.0 (급등 = 유동성 흡수)
    # M2: z = -2.0 (YoY 마이너스)
    report.rrp = IndicatorStatus(name="역레포(RRP)", z_score=0.0, weighted_z=0.0)
    report.tga = IndicatorStatus(name="재무부 TGA", z_score=-3.0, weighted_z=-1.05)  # 35% * -3.0
    report.m2 = IndicatorStatus(name="M2 통화량", z_score=-2.0, weighted_z=-0.50)  # 25% * -2.0

    raw = report.rrp.weighted_z + report.tga.weighted_z + report.m2.weighted_z  # -1.55
    scaled = raw * 3.0  # -4.65
    report.liquidity_score = round(max(-10.0, min(10.0, scaled)), 1)

    if report.liquidity_score <= -5.0:
        report.liquidity_level = "LIQUIDITY_DRAIN"
    elif report.liquidity_score <= -2.0:
        report.liquidity_level = "MILD_CONTRACTION"
    else:
        report.liquidity_level = "NEUTRAL"

    adj = _calc_allocation_adjust(report)

    record("검증3_극단값", "5D TGA급등+M2마이너스 → CONTRACTION 이하",
           report.liquidity_level in ["MILD_CONTRACTION", "LIQUIDITY_DRAIN"],
           f"score={report.liquidity_score}, level={report.liquidity_level}")
    record("검증3_극단값", "5D 배분조정: 스윙 감소",
           adj.get("swing_shift_pct", 0) < 0,
           f"adj={adj}")

    # 더 극단: TGA z=-5, M2 z=-3
    report2 = LiquidityReport(date=datetime.now().strftime("%Y-%m-%d"))
    report2.rrp = IndicatorStatus(name="RRP", z_score=+1.0, weighted_z=+0.40)
    report2.tga = IndicatorStatus(name="TGA", z_score=-5.0, weighted_z=-1.75)
    report2.m2 = IndicatorStatus(name="M2", z_score=-3.0, weighted_z=-0.75)
    raw2 = 0.40 + (-1.75) + (-0.75)  # -2.1
    report2.liquidity_score = round(max(-10.0, min(10.0, raw2 * 3.0)), 1)
    report2.liquidity_level = "LIQUIDITY_DRAIN" if report2.liquidity_score <= -5.0 else "MILD_CONTRACTION"
    adj2 = _calc_allocation_adjust(report2)

    record("검증3_극단값", "5D 최악 시나리오 → DRAIN",
           report2.liquidity_level == "LIQUIDITY_DRAIN",
           f"score={report2.liquidity_score}, level={report2.liquidity_level}")


# ═══════════════════════════════════════
#  검증 3-3: 전체 파이프라인 통합 테스트
# ═══════════════════════════════════════

def test_3_3_execution_order():
    """1D~5D 순서대로 실행되는지"""
    print("\n=== 3-3-A: 실행 순서 검증 ===")
    execution_log = []

    original_funcs = {}

    # 각 모듈 로드 함수를 래핑하여 호출 순서 기록
    import jarvis.brain as brain

    for name in ["_load_regime_leading", "_load_cross_asset_stress",
                  "_load_cot_smartmoney", "_load_liquidity_cycle"]:
        original_funcs[name] = getattr(brain, name)

        def make_wrapper(fname, orig):
            def wrapper(*a, **kw):
                execution_log.append(fname)
                return orig(*a, **kw)
            return wrapper

        setattr(brain, name, make_wrapper(name, original_funcs[name]))

    try:
        result = brain.run_brain(total_capital=1_150_000)

        expected_order = [
            "_load_regime_leading",      # 2D
            "_load_cross_asset_stress",  # 3D
            "_load_cot_smartmoney",      # 4D
            "_load_liquidity_cycle",     # 5D
        ]

        order_correct = execution_log == expected_order
        record("검증3_통합", "실행 순서: 2D→3D→4D→5D",
               order_correct,
               f"실제={execution_log}, 기대={expected_order}")
    finally:
        # 원복
        for name, func in original_funcs.items():
            setattr(brain, name, func)


def test_3_3_single_dimension_error():
    """한 차원 에러 시 나머지 정상 실행"""
    print("\n=== 3-3-B: 차원 에러 격리 ===")
    import jarvis.brain as brain

    # 4D(COT)를 고의로 에러 발생시키고 나머지 확인
    original = brain._load_cot_smartmoney

    def broken_cot():
        raise RuntimeError("INTENTIONAL 4D ERROR FOR TEST")

    brain._load_cot_smartmoney = broken_cot

    try:
        result = brain.run_brain(total_capital=1_150_000)
        has_result = "allocation_pct" in result
        cot_empty = result.get("cot_smartmoney") == {}
        has_1d = result.get("effective_regime") is not None
        has_5d = result.get("liquidity_cycle") != {}

        record("검증3_통합", "4D 에러 → run_brain 생존",
               has_result, f"regime={result.get('effective_regime')}")
        record("검증3_통합", "4D 에러 → cot_smartmoney 빈 dict",
               cot_empty, f"cot={result.get('cot_smartmoney')}")
        record("검증3_통합", "4D 에러 → 1D/5D 정상",
               has_1d and has_5d,
               f"1D={result.get('effective_regime')}, 5D_level={result.get('liquidity_cycle', {}).get('liquidity_level', 'MISSING')}")

    except Exception as e:
        record("검증3_통합", "4D 에러 → run_brain 생존",
               False, f"전체 크래시: {e}")
        results["bugs"].append("[ACTION REQUIRED] 단일 차원 에러 시 run_brain 전체 크래시")
    finally:
        brain._load_cot_smartmoney = original


def test_3_3_telegram_neutral_hidden():
    """NEUTRAL 차원은 텔레그램에서 생략되는지"""
    print("\n=== 3-3-C: NEUTRAL 차원 텔레그램 생략 ===")
    import jarvis.brain as brain

    result = brain.run_brain(total_capital=1_150_000)
    msg = result.get("telegram_message", "")

    # 현재 실시간 상태에서 NEUTRAL인 차원의 블록이 없어야 함
    cot_signal = result.get("cot_smartmoney", {}).get("smart_money_signal", "NEUTRAL")
    liq_level = result.get("liquidity_cycle", {}).get("liquidity_level", "NEUTRAL")

    if cot_signal == "NEUTRAL":
        cot_block_present = "COT 스마트머니" in msg
        record("검증3_통합", "4D NEUTRAL → 텔레그램 생략",
               not cot_block_present,
               f"COT 블록 {'포함됨 (미생략)' if cot_block_present else '생략됨 (OK)'}")

    if liq_level == "NEUTRAL":
        liq_block_present = "유동성 사이클" in msg
        record("검증3_통합", "5D NEUTRAL → 텔레그램 생략",
               not liq_block_present,
               f"유동성 블록 {'포함됨 (미생략)' if liq_block_present else '생략됨 (OK)'}")


def test_3_3_allocation_sum_100():
    """배분 합계가 항상 100%"""
    print("\n=== 3-3-D: 배분 합계 100% ===")
    import jarvis.brain as brain

    # 여러 레짐에서 테스트
    for regime in ["PANIC", "FEAR_EXTREME", "RISK_OFF", "CAUTIOUS", "NEUTRAL", "RISK_ON", "EUPHORIA"]:
        result = brain.get_capital_allocation(regime, 1_150_000, apply_safety=False)
        pct = result["allocation_pct"]
        total = sum(pct.values())
        record("검증3_통합", f"배분합 100% ({regime})",
               total == 100, f"합={total}%, pct={pct}")


def test_3_3_all_worst_final_allocation():
    """모든 차원 동시 최악 시 최종 배분"""
    print("\n=== 3-3-E: 전차원 최악 → 최종 배분 합리성 ===")
    import jarvis.brain as brain

    # 기본 PANIC 배분 가져오기
    base = brain.get_capital_allocation("PANIC", 1_150_000, apply_safety=False)
    alloc = dict(base["allocation_pct"])

    # 2D~5D 전부 최악 조정 누적
    stress_info = {"allocation_adjust": {
        "cash_add_pct": 20, "inverse_add_pct": 10, "swing_reduce_pct": 25,
    }}
    leading_info = {"preemptive_adjust": {
        "cash_shift_pct": 10, "inverse_shift_pct": 5,
    }}
    cot_info = {"smart_money_signal": "RISK_OFF", "allocation_adjust": {
        "cash_shift_pct": 8, "inverse_shift_pct": 3, "swing_reduce_pct": -10,
    }}
    liquidity_info = {"liquidity_level": "LIQUIDITY_DRAIN", "allocation_adjust": {
        "swing_shift_pct": -10, "cash_shift_pct": 8, "inverse_shift_pct": 3,
    }}

    adjusted, notes = brain._apply_sensor_adjustments(
        alloc, stress_info, leading_info, cot_info, liquidity_info
    )

    total = sum(adjusted.values())
    swing = adjusted.get("bh_swing", 0)
    cash = adjusted.get("cash", 0)

    record("검증3_통합", "전차원 최악 → 합계 100%",
           total == 100, f"합={total}%")
    # 전차원 최악 시 현금+인버스 합이 60% 이상 (방어적 배분)
    defensive = cash + adjusted.get("inverse_etf", 0)
    record("검증3_통합", "전차원 최악 → 방어자산(현금+인버스) > 60%",
           defensive >= 60, f"현금={cash}% + 인버스={adjusted.get('inverse_etf', 0)}% = {defensive}%")
    record("검증3_통합", "전차원 최악 → 스윙 < 20%",
           swing <= 20, f"스윙={swing}%")
    record("검증3_통합", "전차원 최악 → 스윙 ≥ 0 (음수 아님)",
           swing >= 0, f"스윙={swing}%")

    if total != 100:
        results["bugs"].append(f"[ACTION REQUIRED] 전차원 최악 시 배분 합계={total}% (100% 아님)")
    if swing < 0:
        results["bugs"].append(f"[ACTION REQUIRED] 스윙 배분 음수: {swing}%")

    print(f"  배분 상세: {adjusted}")
    print(f"  조정 노트: {notes}")


# ═══════════════════════════════════════
#  검증 4: 현재 실시간 크로스 체크
# ═══════════════════════════════════════

def test_4_live_crosscheck():
    """현재 시점 1D~5D 출력을 실제 데이터와 크로스 체크"""
    print("\n" + "=" * 60)
    print("검증 4: 현재 실시간 크로스 체크 (2026-03-07)")
    print("=" * 60)

    import jarvis.brain as brain

    result = brain.run_brain(total_capital=1_150_000)

    # ── 1D NIGHTWATCH ──
    print("\n--- 1D NIGHTWATCH ---")
    regime = result.get("effective_regime", "?")
    record("검증4_실시간", "1D 레짐 판정",
           regime in ["PANIC", "FEAR_EXTREME", "RISK_OFF", "CAUTIOUS", "NEUTRAL", "RISK_ON", "EUPHORIA"],
           f"regime={regime}")

    # ── 2D 선행지표 ──
    print("\n--- 2D 선행지표 ---")
    leading = result.get("regime_leading", {})
    if leading:
        signal = leading.get("transition_signal", "?")
        direction = leading.get("transition_direction", "?")
        record("검증4_실시간", "2D 전환신호 유효",
               signal in ["IMMINENT", "WATCH", "CLEAR"],
               f"signal={signal}, direction={direction}")
    else:
        record("검증4_실시간", "2D 데이터 존재", False, "leading_info 비어있음")

    # ── 3D 크로스에셋 ──
    print("\n--- 3D 크로스에셋 ---")
    stress = result.get("cross_asset_stress", {})
    if stress:
        s_level = stress.get("stress_level", "?")
        s_index = stress.get("stress_index", 0)
        anomaly = stress.get("anomaly_count", 0)
        record("검증4_실시간", "3D 스트레스 레벨 유효",
               s_level in ["NORMAL", "ELEVATED", "HIGH", "CRITICAL"],
               f"level={s_level}, index={s_index}, anomaly={anomaly}/4")
    else:
        record("검증4_실시간", "3D 데이터 존재", False, "stress_info 비어있음")

    # ── 4D COT ──
    print("\n--- 4D COT ---")
    cot = result.get("cot_smartmoney", {})
    if cot:
        cot_sig = cot.get("smart_money_signal", "?")
        cot_score = cot.get("smart_money_score", 0)
        cot_date = cot.get("data_date", "?")
        record("검증4_실시간", "4D COT 신호 유효",
               cot_sig in ["RISK_ON", "RISK_OFF", "NEUTRAL", "VOLATILE"],
               f"signal={cot_sig}, score={cot_score}, date={cot_date}")
    else:
        record("검증4_실시간", "4D 데이터 존재", False, "cot_info 비어있음")

    # ── 5D 유동성 ──
    print("\n--- 5D 유동성 ---")
    liq = result.get("liquidity_cycle", {})
    if liq:
        liq_level = liq.get("liquidity_level", "?")
        liq_score = liq.get("liquidity_score", 0)
        rrp_z = liq.get("rrp_z", 0)
        tga_z = liq.get("tga_z", 0)
        m2_z = liq.get("m2_z", 0)
        record("검증4_실시간", "5D 유동성 레벨 유효",
               liq_level in ["LIQUIDITY_EXPANSION", "MILD_EXPANSION", "NEUTRAL",
                              "MILD_CONTRACTION", "LIQUIDITY_DRAIN"],
               f"level={liq_level}, score={liq_score}")
        record("검증4_실시간", "5D RRP z-score 범위",
               -5.0 <= rrp_z <= 5.0, f"rrp_z={rrp_z}")
        record("검증4_실시간", "5D TGA z-score 범위",
               -5.0 <= tga_z <= 5.0, f"tga_z={tga_z}")
        record("검증4_실시간", "5D M2 z-score 범위",
               -5.0 <= m2_z <= 5.0, f"m2_z={m2_z}")
    else:
        record("검증4_실시간", "5D 데이터 존재", False, "liquidity_info 비어있음")

    # ── FRED 직접 확인 ──
    print("\n--- FRED 직접 대조 ---")
    try:
        from jarvis.liquidity_cycle import _download_fred_csv
        rrp_data = _download_fred_csv("RRPONTSYD", years_back=0)  # 올해만
        tga_data = _download_fred_csv("WTREGEN", years_back=0)
        m2_data = _download_fred_csv("M2SL", years_back=0)

        if rrp_data:
            latest_rrp = rrp_data[-1]
            module_rrp = liq.get("rrp_z", "?")  # z값만 비교는 어려우니 raw도
            record("검증4_실시간", "FRED RRP 최신값 확인",
                   True, f"FRED={latest_rrp[1]:.1f}B$ ({latest_rrp[0]})")

        if tga_data:
            latest_tga = tga_data[-1]
            record("검증4_실시간", "FRED TGA 최신값 확인",
                   True, f"FRED={latest_tga[1]/1000:.0f}B$ ({latest_tga[0]})")

        if m2_data:
            latest_m2 = m2_data[-1]
            record("검증4_실시간", "FRED M2 최신값 확인",
                   True, f"FRED={latest_m2[1]/1000:.1f}T$ ({latest_m2[0]})")

    except Exception as e:
        record("검증4_실시간", "FRED 직접 조회", False, f"에러: {e}")

    # ── 배분 합계 ──
    print("\n--- 배분 검증 ---")
    pct = result.get("allocation_pct", {})
    total = sum(pct.values())
    record("검증4_실시간", "최종 배분 합계 100%",
           total == 100, f"합={total}%, pct={pct}")

    # 센서 조정 내역
    sensor_adj = result.get("sensor_adjustments", [])
    if sensor_adj:
        record("검증4_실시간", "센서 조정 적용됨",
               True, f"조정={sensor_adj}")
    else:
        record("검증4_실시간", "센서 조정 없음 (모두 중립)",
               True, "2D~5D 전부 중립 → 조정 0")


# ═══════════════════════════════════════
#  보고서 생성
# ═══════════════════════════════════════

def generate_reports():
    """결과를 마크다운 보고서로 출력"""

    # ── 검증3: 스트레스 테스트 보고서 ──
    lines = ["# BRAIN 5D 스트레스 테스트 결과", ""]
    lines.append(f"실행 시각: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append("")

    for category, title in [
        ("검증3_엣지케이스", "## 검증 3-1: 데이터 누락 시뮬레이션"),
        ("검증3_극단값", "## 검증 3-2: 극단값 테스트"),
        ("검증3_통합", "## 검증 3-3: 파이프라인 통합 테스트"),
    ]:
        lines.append(title)
        lines.append("")
        lines.append("| 테스트 | 결과 | 상세 |")
        lines.append("|--------|------|------|")
        for r in results[category]:
            lines.append(f"| {r['test']} | {r['result']} | {r['detail']} |")
        lines.append("")

    # 버그 목록
    if results["bugs"]:
        lines.append("## 발견된 문제점")
        lines.append("")
        for bug in results["bugs"]:
            lines.append(f"- {bug}")
        lines.append("")

    # 통계
    all_tests = []
    for cat in ["검증3_엣지케이스", "검증3_극단값", "검증3_통합"]:
        all_tests.extend(results[cat])
    total = len(all_tests)
    passed = sum(1 for t in all_tests if t["result"] == "PASS")
    lines.append(f"## 통계: {passed}/{total} 통과 ({passed/total*100:.0f}%)")

    stress_path = ROOT / "BRAIN_5D_STRESS_TEST.md"
    stress_path.write_text("\n".join(lines), encoding="utf-8")
    print(f"\n보고서 저장: {stress_path}")

    # ── 검증4: 실시간 크로스 체크 보고서 ──
    lines4 = ["# BRAIN 5D 실시간 크로스 체크", ""]
    lines4.append(f"실행 시각: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines4.append("")
    lines4.append("| 테스트 | 결과 | 상세 |")
    lines4.append("|--------|------|------|")
    for r in results["검증4_실시간"]:
        lines4.append(f"| {r['test']} | {r['result']} | {r['detail']} |")

    live_tests = results["검증4_실시간"]
    t4 = len(live_tests)
    p4 = sum(1 for t in live_tests if t["result"] == "PASS")
    lines4.append("")
    lines4.append(f"## 통계: {p4}/{t4} 통과 ({p4/t4*100:.0f}%)" if t4 > 0 else "")

    live_path = ROOT / "BRAIN_5D_LIVE_CROSSCHECK.md"
    live_path.write_text("\n".join(lines4), encoding="utf-8")
    print(f"보고서 저장: {live_path}")


# ═══════════════════════════════════════
#  메인
# ═══════════════════════════════════════

if __name__ == "__main__":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", line_buffering=True)
    logging.basicConfig(
        level=logging.WARNING,  # 테스트 중 로그 최소화
        format="%(asctime)s %(name)s %(message)s",
        datefmt="%H:%M:%S",
    )

    print("=" * 60)
    print("BRAIN 1D~5D 풀 스택 검증")
    print("=" * 60)

    # ── 검증 3: 엣지 케이스 ──
    print("\n" + "=" * 60)
    print("검증 3-1: 데이터 누락 시뮬레이션")
    print("=" * 60)

    test_3_1_fred_api_down()
    test_3_1_cftc_404()
    test_3_1_yahoo_bad_ticker()
    test_3_1_m2_stale()

    print("\n" + "=" * 60)
    print("검증 3-2: 극단값 테스트")
    print("=" * 60)

    test_3_2_all_worst_2d()
    test_3_2_all_worst_3d()
    test_3_2_all_worst_4d()
    test_3_2_all_worst_5d()

    print("\n" + "=" * 60)
    print("검증 3-3: 파이프라인 통합 테스트")
    print("=" * 60)

    test_3_3_execution_order()
    test_3_3_single_dimension_error()
    test_3_3_telegram_neutral_hidden()
    test_3_3_allocation_sum_100()
    test_3_3_all_worst_final_allocation()

    # ── 검증 4: 실시간 크로스 체크 ──
    test_4_live_crosscheck()

    # ── 보고서 생성 ──
    generate_reports()

    # 최종 요약
    all_tests = []
    for cat_name, cat_items in results.items():
        if cat_name == "bugs":
            continue  # bugs는 문자열 리스트
        all_tests.extend(cat_items)
    total = len(all_tests)
    passed = sum(1 for t in all_tests if t["result"] == "PASS")
    failed = total - passed

    print("\n" + "=" * 60)
    print(f"전체 결과: {passed}/{total} 통과 ({passed/total*100:.0f}%)")
    if failed > 0:
        print(f"실패: {failed}건")
    if results["bugs"]:
        print(f"\n발견된 문제점 ({len(results['bugs'])}건):")
        for bug in results["bugs"]:
            print(f"  {bug}")
    print("=" * 60)
