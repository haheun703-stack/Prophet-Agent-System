# -*- coding: utf-8 -*-
"""
T3 수급 패턴 감지기
====================
2026-04-16 작성

목적:
- 급등주(+3% 이상)의 수급 구조를 자동 분류
- 쌍매수/외인 단독/기관 단독/기타법인 매수/기타법인 매도/개인 주도/수급 이탈 7종 분류
- 5D/10D/20D 누적 수급 제공
- 11주체 스키마 호환 (현재 4주체, OpenAPI 승인 후 자동 확장)

입력:
- data_store/learning/missed_gainers/{YYYY-MM-DD}.json
- 또는 코드 리스트 직접 전달

출력:
- data_store/learning/pattern_scan/{YYYY-MM-DD}.json
- 콘솔 표 출력

사용:
    python tools/supply_pattern_detector.py              # 오늘 missed_gainers 분석
    python tools/supply_pattern_detector.py --date 2026-04-16
    python tools/supply_pattern_detector.py --codes 077360,009580,356680
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

import pandas as pd

# ── 한국시간 (VPS UTC 대응) ──
KST = timezone(timedelta(hours=9))

# ── KIS API 단위 전환일 (2026-01-16 이전 pykrx=원, 이후 KIS=백만원) ──
# flow_collector.py가 KIS로 전환된 시점. 이 날짜 이전 데이터는 pykrx 잔재(원 단위).
UNIT_CHANGE_DATE = pd.Timestamp("2026-01-16")

# ── 프로젝트 경로 ──
SCALPER_DIR = Path(__file__).resolve().parent.parent
REPO_DIR = SCALPER_DIR.parent

sys.path.insert(0, str(SCALPER_DIR))
from data.supply_flow_schema import (  # noqa: E402
    ENTITY_COLUMNS,
    ENTITY_KR,
    AVAILABLE_ENTITIES,
    to_canonical,
)

# ── 경로 ──
FLOW_DIR = SCALPER_DIR / "data_store" / "flow"
MISSED_DIR = SCALPER_DIR / "data_store" / "learning" / "missed_gainers"
OUTPUT_DIR = SCALPER_DIR / "data_store" / "learning" / "pattern_scan"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ── 패턴 임계값 (억원 단위) — 대형주 절대값 fallback ──
# ※ 실제 감지는 _dynamic_thr(시총)로 동적 계산 (아래 THR는 시총 미확인시 fallback)
THR = {
    "dual_foreign": 50.0,      # 쌍매수: 외인 최소
    "dual_inst": 30.0,         # 쌍매수: 기관 최소
    "solo_main": 50.0,         # 단독: 주도 주체 최소
    "solo_other": 30.0,        # 단독: 보조 주체 최대
    "retail_strong": 30.0,     # 개인주도: 개인 최소 매수
    "retail_netral": 20.0,     # 개인주도: 외+기관 합 허용 폭
    "other_corp_load": 20.0,   # 기타법인 매수
    "other_corp_dump": 20.0,   # 기타법인 매도 (절대값)
    "outflow_foreign": 30.0,   # 수급이탈: 외인 순매도 최소
    "outflow_inst": 20.0,      # 수급이탈: 기관 순매도 최소
    "surge_pct": 10.0,         # 수급이탈 판정용 가격 급등 기준
    "individual_dump": 30.0,   # 쌍매수: 개인 순매도 최소 (절대값)
}


# ── 시총 기반 동적 임계값 ──
# 75만 행 통계분석 결과 (2,431종목):
#   - 급등주(+10%+) 시총대비 외인매수: 소형 P50=0.292%, 중형 P50=0.313%, 대형 P50=0.242%
#   - 0.3% = 전 규모 중앙값 수준 → "의미있는 매수"의 최소 기준
#   - 대칭도(|외인+개인|/|외인|) P50=0.271, 양호 기준 ≤0.5 (62.3% 해당)
#   - 폭탄 당일 추격매수 = 손실 (D+5 -1.28%) → 눌림목 재매수만 유효
#   - 최적 조합: 대칭0.5+눌림-3%+기타법인 → D+3 +1.88% 승률53%
def _dynamic_thr(cap_b: float) -> dict:
    """시총 기반 동적 임계값 (억원 단위).

    cap_b: 시총 (억원). 0이면 절대값 fallback(THR) 반환.
    """
    if cap_b <= 0:
        return THR

    # 핵심: 시총의 0.3% = 의미있는 수급 이동 (75만 행 검증)
    bomb = cap_b * 0.003
    bomb = max(bomb, 2.0)     # 최소 2억 (시총 ~667억 이하)
    bomb = min(bomb, 500.0)   # 최대 500억 (시총 ~16.7조+ 초대형주)

    return {
        "dual_foreign": bomb,                  # 쌍매수: 외인 0.3%+
        "dual_inst": max(bomb * 0.6, 1.5),     # 쌍매수: 기관 0.18%+
        "solo_main": bomb,                      # 단독: 주도 0.3%+
        "solo_other": max(bomb * 0.6, 1.5),    # 단독: 보조 최대 0.18%
        "retail_strong": max(bomb * 0.6, 1.5),
        "retail_netral": max(bomb * 0.4, 1.0),
        "other_corp_load": max(bomb * 0.4, 1.0),
        "other_corp_dump": max(bomb * 0.4, 1.0),
        "outflow_foreign": max(bomb * 0.6, 1.5),
        "outflow_inst": max(bomb * 0.4, 1.0),
        "surge_pct": 10.0,                     # 고정
        "individual_dump": max(bomb * 0.6, 1.5),
    }


def _calc_symmetry(foreign: float, individual: float) -> Optional[float]:
    """외인-개인 대칭도. |외인+개인|/|외인|. 0=완벽대칭, ≤0.5 양호(62.3%)."""
    if abs(foreign) < 0.1:
        return None
    return round(abs(foreign + individual) / abs(foreign), 3)

# ── 패턴 정의 ──
PATTERN_DESC = {
    "DUAL_SURGE": "쌍매수 폭발 (외인+기관 동반)",
    "FOREIGN_SOLO": "외국인 단독 매수",
    "INST_SOLO": "기관 단독 매수",
    "OTHER_CORP_LOAD": "기타법인 대량 매수 (M&A/자사주 의심)",
    "OTHER_CORP_DUMP": "기타법인 대량 매도 (자사주 처분/내부자 이탈 의심)",
    "RETAIL_LED": "개인 주도 (단타성)",
    "OUTFLOW": "수급 이탈 (외인/기관 매도에도 급등)",
    "MIXED": "혼조 (명확한 주도 세력 없음)",
}

PATTERN_SCORE = {
    "DUAL_SURGE": 95,
    "FOREIGN_SOLO": 80,
    "INST_SOLO": 75,
    "OTHER_CORP_LOAD": 65,   # 공시 확인 필요
    "OTHER_CORP_DUMP": 25,   # 위험
    "RETAIL_LED": 40,        # 단타성
    "OUTFLOW": 15,           # 고점 신호
    "MIXED": 50,
}

PATTERN_GRADE = {
    "DUAL_SURGE": "최강",
    "FOREIGN_SOLO": "강함",
    "INST_SOLO": "중강",
    "OTHER_CORP_LOAD": "특수",
    "OTHER_CORP_DUMP": "위험",
    "RETAIL_LED": "약함",
    "OUTFLOW": "위험",
    "MIXED": "보통",
}


@dataclass
class PatternResult:
    code: str
    name: str
    pattern: str
    description: str
    score: int
    grade: str
    change_rate: float
    close: float
    volume: int
    entities_today: Dict[str, Optional[float]] = field(default_factory=dict)
    entities_5d: Dict[str, Optional[float]] = field(default_factory=dict)
    entities_10d: Dict[str, Optional[float]] = field(default_factory=dict)
    entities_20d: Dict[str, Optional[float]] = field(default_factory=dict)
    warnings: List[str] = field(default_factory=list)
    # C3M-T3 연동: 시장 맥락 보조점수 (시장 역풍시 감쇠)
    market_context: Dict[str, Optional[float]] = field(default_factory=dict)
    market_adj: int = 0            # 시장맥락 점수 조정 (-15 ~ +10)
    final_score: int = 0           # = score + market_adj (0~100 clip)
    # v2 추가: 시총비율 기반 (75만 행 통계 분석)
    cap_b: float = 0.0                           # 시총 (억원)
    symmetry: Optional[float] = None             # 외인-개인 대칭도 (≤0.5 양호)
    bomb_reentry: Dict = field(default_factory=dict)  # 멀티데이 폭탄→재매수 신호
    classified_at: str = ""


# ============================================================
#  C3M-T3 연동: 시장 맥락 로더 + 점수 조정
# ============================================================

_UNIVERSE_CACHE: Optional[Dict[str, dict]] = None  # code -> {"market": str, "cap": float}


def _load_universe():
    """universe.json 로드 (시장+시총 통합 캐시)."""
    global _UNIVERSE_CACHE
    if _UNIVERSE_CACHE is not None:
        return
    try:
        uni_path = SCALPER_DIR / "data_store" / "universe.json"
        uni = json.loads(uni_path.read_text(encoding="utf-8"))
        _UNIVERSE_CACHE = {}
        for k, v in uni.items():
            market = "kospi" if v.get("market") == "KOSPI" else "kosdaq"
            cap = 0.0
            for vk, vv in v.items():
                if vk.startswith("cap_") and isinstance(vv, (int, float)):
                    cap = float(vv)
                    break
            _UNIVERSE_CACHE[k] = {"market": market, "cap": cap, "name": v.get("name", k)}
    except Exception:
        _UNIVERSE_CACHE = {}


def _get_market_for_code(code: str) -> str:
    """종목 코드 → 시장(kospi/kosdaq) 판별."""
    _load_universe()
    return (_UNIVERSE_CACHE or {}).get(code, {}).get("market", "kospi")


_KRX_MKTCAP_CACHE: Optional[Dict[str, float]] = None  # code -> 억원


def _get_cap_for_code(code: str) -> float:
    """종목 코드 → 시총 (억원). KRX API 일별 시총 우선, fallback=universe.json."""
    global _KRX_MKTCAP_CACHE
    # 1) KRX API 캐시 (당일 실시간 시총)
    if _KRX_MKTCAP_CACHE is None:
        try:
            from data.krx_openapi_client import get_mktcap_dict
            _KRX_MKTCAP_CACHE = get_mktcap_dict()
        except Exception:
            _KRX_MKTCAP_CACHE = {}
    if code in _KRX_MKTCAP_CACHE and _KRX_MKTCAP_CACHE[code] > 0:
        return _KRX_MKTCAP_CACHE[code]
    # 2) fallback: universe.json
    _load_universe()
    return (_UNIVERSE_CACHE or {}).get(code, {}).get("cap", 0.0)


def _load_market_context(market: str = "kospi") -> Dict[str, Optional[float]]:
    """C3M 데이터(market_{kospi,kosdaq}.csv)에서 최근 5일 주체별 누적 (억원)."""
    try:
        sys.path.insert(0, str(SCALPER_DIR))
        from data.market_flow_collector import get_recent_entity_sum
        ctx = {}
        for entity in ("foreign", "individual", "institution",
                       "finance_invest", "trust", "pension",
                       "private_equity", "other_corp"):
            v = get_recent_entity_sum(market, entity, days=5)
            ctx[f"{entity}_5d"] = round(v, 1) if v is not None else None
        return ctx
    except Exception:
        return {}


# ── 시장별 임계값 (억원) ──
# KOSDAQ은 KOSPI 대비 약 1/3~1/10 규모 (2026-04-16 실측: 외인 KOSPI +13,485 vs KOSDAQ -1,809)
MARKET_THR = {
    "kospi": {
        "bull_flow": 3000,      # bull/bear: 외+기 각 절대값
        "foreign_big": 5000,    # 외인 단독 대량
        "inst_big": 5000,       # 기관 동반/역풍
        "pension_accum": 3000,  # 연기금 매집
        "trust_outflow": 3000,  # 투신 이탈
        "retail_extreme": 10000,# 개인쏠림 임계
        "flow_sum_bear": 5000,  # 외+기 합 매도
        "other_corp_trend": 5000,
    },
    "kosdaq": {
        # KOSDAQ 규모 1/3 기준 조정 (너무 민감하지 않도록)
        "bull_flow": 1000,
        "foreign_big": 1500,
        "inst_big": 1500,
        "pension_accum": 800,
        "trust_outflow": 1000,
        "retail_extreme": 3000,
        "flow_sum_bear": 1500,
        "other_corp_trend": 1500,
    },
}


def _apply_market_context(
    pattern: str,
    entities_today: Dict[str, Optional[float]],
    market_ctx: Dict[str, Optional[float]],
    change_rate: float,
    market: str = "kospi",
) -> Tuple[int, List[str]]:
    """시장 맥락 기반 점수 조정.

    Args:
        market: "kospi" or "kosdaq" — 시장별 임계값 분기

    Returns: (adj, notes)
      - adj: -15 ~ +10 점수 가감
      - notes: 경고/주석 문자열 리스트
    """
    if not market_ctx:
        return 0, []

    thr = MARKET_THR.get(market, MARKET_THR["kospi"])
    mkt_tag = f"[{market.upper()}]"

    adj = 0
    notes: List[str] = []

    # 시장 수급 방향 판정 (외인+기관 5일 합)
    mkt_f = market_ctx.get("foreign_5d") or 0
    mkt_i = market_ctx.get("institution_5d") or 0
    mkt_pension = market_ctx.get("pension_5d") or 0
    mkt_trust = market_ctx.get("trust_5d") or 0
    mkt_flow_sum = mkt_f + mkt_i

    mkt_bull = mkt_f >= thr["bull_flow"] and mkt_i >= thr["bull_flow"]
    mkt_bear = mkt_f <= -thr["bull_flow"] and mkt_i <= -thr["bull_flow"]

    # 1) FOREIGN_SOLO / DUAL_SURGE — 시장 역풍시 감쇠
    if pattern in ("FOREIGN_SOLO", "DUAL_SURGE"):
        if mkt_bull:
            adj += 5
            notes.append(f"{mkt_tag}시장순풍(외{mkt_f:+.0f}/기{mkt_i:+.0f}억)(+5)")
        elif mkt_bear:
            adj -= 10
            notes.append(f"{mkt_tag}시장역풍(외{mkt_f:+.0f}/기{mkt_i:+.0f}억)(-10)")
        elif mkt_f <= -thr["foreign_big"]:
            adj -= 5
            notes.append(f"{mkt_tag}시장외인매도(외{mkt_f:+.0f}억)(-5)")

    # 2) INST_SOLO — 기관 시장맥락 체크
    elif pattern == "INST_SOLO":
        if mkt_i >= thr["inst_big"]:
            adj += 5
            notes.append(f"{mkt_tag}시장기관동반(기{mkt_i:+.0f}억)(+5)")
        elif mkt_i <= -thr["inst_big"]:
            adj -= 8
            notes.append(f"{mkt_tag}시장기관매도속 단독매수(기{mkt_i:+.0f}억)(-8)")
        # 연기금 장기매집 순풍
        if mkt_pension >= thr["pension_accum"]:
            adj += 3
            notes.append(f"{mkt_tag}연기금매집(+3)")

    # 3) OUTFLOW — 시장도 동반 매도면 "시장 전체 조정"
    elif pattern == "OUTFLOW":
        if mkt_bear:
            notes.append(f"{mkt_tag}시장동반매도(외{mkt_f:+.0f}/기{mkt_i:+.0f}억) — 전체 조정 국면")
            adj += 3

    # 4) OTHER_CORP_LOAD — 시장 기타법인 매집 추세면 섹터 트렌드
    elif pattern == "OTHER_CORP_LOAD":
        mkt_oc = market_ctx.get("other_corp_5d") or 0
        if mkt_oc >= thr["other_corp_trend"]:
            notes.append(f"{mkt_tag}시장기타법인동반매집(+{mkt_oc:.0f}억) — 자사주 트렌드")
            adj += 3

    # 5) RETAIL_LED — 시장 개인쏠림이면 과열 경고
    elif pattern == "RETAIL_LED":
        mkt_p = market_ctx.get("individual_5d") or 0
        if mkt_p >= thr["retail_extreme"] and mkt_flow_sum <= -thr["flow_sum_bear"]:
            adj -= 5
            notes.append(f"{mkt_tag}시장전체 개인쏠림 — 과열국면(-5)")

    # 6) 공통: 투신(개인간접) 대량 이탈시 단타성 패턴 추가 감점
    if mkt_trust <= -thr["trust_outflow"] and pattern in ("RETAIL_LED", "MIXED"):
        adj -= 2
        notes.append(f"{mkt_tag}투신이탈(-2)")

    # 범위 제한
    adj = max(-15, min(10, adj))
    return adj, notes


# ============================================================
#  멀티데이 외인폭탄 → 눌림목 → 재매수 감지
# ============================================================
# 75만 행 통계 결과:
#   당일 추격매수 = 손실 (D+5 -1.28%)
#   눌림 -3%+ 후 재매수 = D+3 +1.88% (대칭0.5+기타법인)
#   눌림 -8%+ 후 재매수 = D+5 +3.18% (최강)
#   기타법인 동반 = +1.17%p 엣지
#   재매수 타이밍: P25=2일, P50=3일, P75=4일

def _detect_bomb_reentry(
    df: Optional[pd.DataFrame],
    cap_b: float,
    ref_date: Optional[str] = None,
    lookback: int = 15,
) -> Dict:
    """외인 폭탄 → 눌림목 → 재매수 멀티데이 패턴 감지.

    Args:
        df: canonical flow DataFrame (억원 단위, index=date)
        cap_b: 시총 (억원)
        ref_date: 기준일 (None이면 최근)
        lookback: 검색 기간 (일)

    Returns:
        dict with signal: NONE/BOMB_TODAY/SECOND_BOMB/EARLY/PULLBACK_WAIT/REENTRY/STRONG_REENTRY/EXPIRED/QUIET_ACCUM
    """
    empty = {"signal": "NONE"}
    if df is None or df.empty or cap_b <= 0:
        return empty

    bomb_thr = max(cap_b * 0.003, 2.0)
    bomb_thr = min(bomb_thr, 500.0)   # Fix1: 초대형주 캡 (삼성SDI 38조→500억 vs 이전 1139억)

    if ref_date:
        recent = df.loc[df.index <= ref_date].tail(lookback)
    else:
        recent = df.tail(lookback)

    if len(recent) < 3:
        return empty

    # 가장 최근의 유효한 폭탄일 탐색 (역순)
    for i in range(len(recent) - 1, -1, -1):
        row = recent.iloc[i]
        f_val = float(row.get("foreign", 0) or 0)
        p_val = float(row.get("individual", 0) or 0)

        if f_val < bomb_thr:
            continue

        # 대칭도 체크
        sym = abs(f_val + p_val) / abs(f_val) if abs(f_val) > 0 else 999

        bomb_date = recent.index[i]
        days_after = len(recent) - 1 - i
        bomb_pct = round(f_val / cap_b * 100, 3)

        base = {
            "bomb_date": str(bomb_date)[:10],
            "bomb_foreign_pct": bomb_pct,
            "bomb_foreign_abs": round(f_val, 1),
            "symmetry": round(sym, 3),
            "symmetry_ok": sym <= 0.5,
            "days_since_bomb": days_after,
        }

        # (A) 당일 폭탄
        if days_after == 0:
            # Fix2: 2차 폭탄 검사 — 이전에 또 다른 폭탄이 있었나?
            # 빅텍: 4/10 폭탄→눌림→4/16 재폭발 = SECOND_BOMB (최강 매수신호)
            for j in range(i - 1, max(i - lookback, -1), -1):
                prev_row = recent.iloc[j]
                prev_f = float(prev_row.get("foreign", 0) or 0)
                if prev_f >= bomb_thr:
                    gap = i - j
                    prev_date = recent.index[j]
                    return {**base,
                            "signal": "SECOND_BOMB",
                            "prev_bomb_date": str(prev_date)[:10],
                            "prev_bomb_foreign": round(prev_f, 1),
                            "gap_days": gap,
                            "note": f"2차+ 폭탄 — {gap}거래일 전 1차 폭탄 후 재폭발 (최강 매수신호)"}

            # 이전 폭탄 없음 → 순수 1차 폭탄 (추격 금지)
            return {**base,
                    "signal": "BOMB_TODAY",
                    "note": "당일 1차 폭탄 — 추격매수 금지 (D+5 -1.28%)"}

        # (B) 폭탄 직후 (1일) — 아직 이른감
        if days_after == 1:
            # M-2: 이전 폭탄 검사 — 2차 폭탄 후 EARLY는 더 강한 신호
            for j in range(i - 1, max(i - lookback, -1), -1):
                prev_row = recent.iloc[j]
                prev_f = float(prev_row.get("foreign", 0) or 0)
                if prev_f >= bomb_thr:
                    gap = i - j
                    prev_date = recent.index[j]
                    return {**base,
                            "signal": "EARLY",
                            "prev_bomb_date": str(prev_date)[:10],
                            "prev_bomb_foreign": round(prev_f, 1),
                            "gap_days": gap,
                            "is_second_bomb": True,
                            "note": f"2차 폭탄 직후 D+1 — {gap}일 전 1차 후 재폭발 (내일 재매수 주시)"}
            return {**base,
                    "signal": "EARLY",
                    "is_second_bomb": False,
                    "note": "1차 폭탄 직후 — 눌림목 대기 (최적 2-4일)"}

        # (C) 유효 기간 내 (2~7일)
        if 2 <= days_after <= 7:
            today_row = recent.iloc[-1]
            today_f = float(today_row.get("foreign", 0) or 0)
            today_oc = float(today_row.get("other_corp", 0) or 0)

            # 폭탄 후 외인 순매수 합
            after_slice = recent.iloc[i + 1:]
            after_f_sum = sum(float(r.get("foreign", 0) or 0) for _, r in after_slice.iterrows())

            base["reentry_today_foreign"] = round(today_f, 1)
            base["other_corp_coop"] = today_oc > 0
            base["after_bomb_foreign_sum"] = round(after_f_sum, 1)

            if today_f > 0 and sym <= 0.5:
                # 재매수 감지!
                if today_oc > 0:
                    return {**base,
                            "signal": "STRONG_REENTRY",
                            "expected_edge_d3": 1.88,
                            "expected_winrate_d3": 53.0,
                            "note": "재매수+기타법인동반 (D+3 +1.88% 승률53%)"}
                else:
                    return {**base,
                            "signal": "REENTRY",
                            "expected_edge_d3": 0.93,
                            "expected_winrate_d3": 49.9,
                            "note": "재매수 감지 (D+3 +0.93% 승률50%)"}
            elif today_f <= 0 and sym <= 0.5:
                return {**base,
                        "signal": "PULLBACK_WAIT",
                        "note": f"눌림목 대기 D+{days_after} — 외인 재매수 시작 시 진입"}
            else:
                # 대칭도 미달이지만 폭탄은 있음
                return {**base,
                        "signal": "WEAK_BOMB",
                        "note": "대칭도 미달(개인매도 부족) — 신뢰도 낮음"}

        # (D) 유효기간 초과
        if days_after > 7:
            return {**base,
                    "signal": "EXPIRED",
                    "note": f"폭탄 {days_after}일 경과 — 유효기간 초과"}

    # ── Fix3: 폭탄은 없지만 5D 저강도 연속 외인매수 ──
    # 예: 후성(6.7조) 5D+67억, 덕산테코피아(8539억) 5D+21.7억
    accum_thr = min(bomb_thr * 0.3, 50.0)  # 폭탄의 30% 또는 50억 중 작은 값
    accum_thr = max(accum_thr, 1.0)        # 최소 1억

    if ref_date:
        tail5 = df.loc[df.index <= ref_date].tail(5)
    else:
        tail5 = df.tail(5)

    if len(tail5) >= 3:
        f_sum = 0.0
        pos_days = 0
        for _, row in tail5.iterrows():
            fv = float(row.get("foreign", 0) or 0)
            f_sum += fv
            if fv > 0:
                pos_days += 1

        if f_sum >= accum_thr and pos_days >= 4:
            f_pct = round(f_sum / cap_b * 100, 3) if cap_b > 0 else 0
            return {
                "signal": "QUIET_ACCUM",
                "foreign_5d_sum": round(f_sum, 1),
                "foreign_5d_pct": f_pct,
                "positive_days": pos_days,
                "total_days": len(tail5),
                "note": f"저강도 연속매수 5D(외인+{f_sum:.1f}억, {pos_days}/{len(tail5)}일 매수)",
            }

    return empty


def _csv_to_canonical_df(csv_path: Path) -> Optional[pd.DataFrame]:
    """
    CSV 파일(백만원 단위) → canonical 영문 ID DataFrame (억원 단위).

    Returns:
        DataFrame with index=date, columns=ENTITY_COLUMNS (값 in 억원)
        실패시 None.
    """
    if not csv_path.exists():
        return None
    try:
        df = pd.read_csv(csv_path, index_col=0, parse_dates=True)
    except Exception:
        return None
    if df.empty:
        return None

    # 각 row를 canonical로 변환
    canonical_rows = []
    for idx, row in df.iterrows():
        c = to_canonical(row.to_dict())
        canonical_rows.append(c)

    out = pd.DataFrame(canonical_rows, index=df.index)
    # C-1 수정 (2026-04-16): 날짜 기반 단위 분기 (기존 magnitude 휴리스틱 버그 해결)
    #   - UNIT_CHANGE_DATE 이전: pykrx 잔재 = 원 단위 → /1e8
    #   - UNIT_CHANGE_DATE 이후: KIS API = 백만원 단위 → /100
    #   - 날짜 불명시: magnitude fallback (abs>=1e7 → 원, else 백만원)
    def _convert_unit(v, row_date=None):
        if v is None or v != v:  # None or NaN
            return None
        if row_date is not None:
            try:
                if pd.Timestamp(row_date) >= UNIT_CHANGE_DATE:
                    return v / 100.0   # 백만원 → 억원
                return v / 1e8         # 원 → 억원
            except Exception:
                pass
        # Fallback (날짜 파싱 실패 시)
        if abs(v) >= 1e7:
            return v / 1e8
        return v / 100.0

    for col in ENTITY_COLUMNS:
        if col in out.columns:
            out[col] = [_convert_unit(v, idx) for v, idx in zip(out[col], out.index)]
    # 정렬
    out = out.sort_index()
    return out


def _calc_cumulative(df: pd.DataFrame, days: int, ref_date: Optional[str] = None) -> Dict[str, Optional[float]]:
    """
    최근 N일 누적 (ref_date 포함 역방향).
    None 값은 스킵.
    """
    if df is None or df.empty:
        return {k: None for k in ENTITY_COLUMNS}

    if ref_date:
        sub = df.loc[df.index <= ref_date].tail(days)
    else:
        sub = df.tail(days)

    out = {}
    for col in ENTITY_COLUMNS:
        if col in sub.columns:
            vals = sub[col].dropna()
            out[col] = float(vals.sum()) if len(vals) > 0 else None
        else:
            out[col] = None
    return out


def _today_row(df: pd.DataFrame, ref_date: Optional[str] = None) -> Dict[str, Optional[float]]:
    """ref_date 당일 수급 (default: 최근 일자)."""
    if df is None or df.empty:
        return {k: None for k in ENTITY_COLUMNS}
    if ref_date:
        candidates = df.loc[df.index <= ref_date]
        if candidates.empty:
            return {k: None for k in ENTITY_COLUMNS}
        last = candidates.iloc[-1]
    else:
        last = df.iloc[-1]
    return {col: (float(last[col]) if col in last.index and pd.notna(last[col]) else None)
            for col in ENTITY_COLUMNS}


def classify_pattern(
    entities_today: Dict[str, Optional[float]],
    change_rate: float,
    thr: Optional[dict] = None,
) -> Tuple[str, List[str]]:
    """
    오늘 수급(억원) + 등락률 → 패턴 분류.

    Args:
        thr: 동적 임계값 dict (_dynamic_thr 결과). None이면 글로벌 THR 사용.

    우선순위:
      1. DUAL_SURGE (외+기관 쌍매수, 개인 대량 매도)
      2. OUTFLOW (외/기관 대량 매도인데 급등)
      3. OTHER_CORP_DUMP (기타법인 대량 매도)
      4. OTHER_CORP_LOAD (기타법인 대량 매수)
      5. FOREIGN_SOLO
      6. INST_SOLO
      7. RETAIL_LED
      8. MIXED
    """
    t = thr or THR

    f = entities_today.get("foreign") or 0.0
    i = entities_today.get("institution") or 0.0
    p = entities_today.get("individual") or 0.0
    oc = entities_today.get("other_corp") or 0.0

    warnings: List[str] = []

    # 1. DUAL_SURGE — 외 + 기관 쌍매수 + 개인 대량 매도
    if f >= t["dual_foreign"] and i >= t["dual_inst"] and p <= -t["individual_dump"]:
        if oc <= -t["other_corp_dump"]:
            warnings.append(
                f"쌍매수지만 기타법인 {oc:+.1f}억 매도 병행 — 내부자 물량 소화 의심"
            )
        return "DUAL_SURGE", warnings

    # 2. OUTFLOW — 외/기관 대량 매도인데 급등
    if f <= -t["outflow_foreign"] and i <= -t["outflow_inst"] and change_rate >= t["surge_pct"]:
        warnings.append(f"수급 이탈에도 +{change_rate:.1f}% 급등 — 고점 가능성")
        return "OUTFLOW", warnings

    # 3. OTHER_CORP_DUMP
    if oc <= -t["other_corp_dump"]:
        warnings.append(f"기타법인 {oc:+.1f}억 매도 — 자사주 처분/대주주 매도 의심")
        warnings.append("※ 기타법인은 역산값 — 공시/거래원 확인 필요")
        return "OTHER_CORP_DUMP", warnings

    # 4. OTHER_CORP_LOAD
    if oc >= t["other_corp_load"]:
        warnings.append(f"기타법인 {oc:+.1f}억 매수 — M&A/자사주 취득 가능성")
        warnings.append("※ 기타법인은 역산값 — 공시/거래원 확인 필요")
        return "OTHER_CORP_LOAD", warnings

    # 5. FOREIGN_SOLO
    if f >= t["solo_main"] and i < t["solo_other"]:
        return "FOREIGN_SOLO", warnings

    # 6. INST_SOLO
    if i >= t["solo_main"] and f < t["solo_other"]:
        return "INST_SOLO", warnings

    # 7. DUAL_WEAK fallback — 외+기 모두 매수지만 DUAL_SURGE 개인조건 미달
    if f >= t["dual_inst"] and i >= t["dual_inst"]:
        if f >= i:
            warnings.append(f"외인 우세 쌍매수 (외 {f:+.1f}억 / 기 {i:+.1f}억)")
            return "FOREIGN_SOLO", warnings
        else:
            warnings.append(f"기관 우세 쌍매수 (외 {f:+.1f}억 / 기 {i:+.1f}억)")
            return "INST_SOLO", warnings

    # 8. RETAIL_LED
    if abs(f + i) <= t["retail_netral"] and p >= t["retail_strong"]:
        warnings.append("개인 주도 단타 — 변동성 주의")
        return "RETAIL_LED", warnings

    # 9. MIXED
    return "MIXED", warnings


def analyze_code(
    code: str,
    name: str,
    change_rate: float,
    close: float,
    volume: int,
    ref_date: Optional[str] = None,
) -> PatternResult:
    """단일 코드 패턴 분석 (시총비율 기반 동적 임계값 v2)."""
    csv_path = FLOW_DIR / f"{code}_investor.csv"
    df = _csv_to_canonical_df(csv_path)

    entities_today = _today_row(df, ref_date) if df is not None else {k: None for k in ENTITY_COLUMNS}
    entities_5d = _calc_cumulative(df, 5, ref_date) if df is not None else {}
    entities_10d = _calc_cumulative(df, 10, ref_date) if df is not None else {}
    entities_20d = _calc_cumulative(df, 20, ref_date) if df is not None else {}

    # ── 시총 기반 동적 임계값 (v2 핵심) ──
    cap_b = _get_cap_for_code(code)
    dyn_thr = _dynamic_thr(cap_b)

    pattern, warnings = classify_pattern(entities_today, change_rate, thr=dyn_thr)

    if df is None:
        warnings.insert(0, f"CSV 없음: {csv_path.name}")
    else:
        if all(v is None for v in entities_today.values()):
            warnings.insert(0, "수급 데이터 전체 누락 — 패턴 분류 신뢰 불가")

    # ── 대칭도 계산 ──
    f_val = entities_today.get("foreign") or 0.0
    p_val = entities_today.get("individual") or 0.0
    symmetry = _calc_symmetry(f_val, p_val)

    # ── 멀티데이 폭탄→재매수 감지 ──
    bomb_reentry = _detect_bomb_reentry(df, cap_b, ref_date)
    bomb_sig = bomb_reentry.get("signal", "NONE")
    if bomb_sig == "SECOND_BOMB":
        gap = bomb_reentry.get("gap_days", "?")
        warnings.append(f"2차+ 폭탄 — {gap}일 전 1차 후 재폭발 (최강 매수신호)")
    elif bomb_sig == "EARLY":
        is_2nd = bomb_reentry.get("is_second_bomb", False)
        if is_2nd:
            gap = bomb_reentry.get("gap_days", "?")
            warnings.append(f"2차 폭탄 D+1 — {gap}일 전 1차 후 재폭발 직후 (TP3%/SL-2% 승률56%)")
        else:
            warnings.append(f"1차 폭탄 D+1 — 눌림목 대기 추천 (최적 진입 2-4일 후)")
    elif bomb_sig == "BOMB_TODAY":
        warnings.append(f"외인폭탄 당일 (1차) — 추격매수 금지 (통계: D+5 -1.28%)")
    elif bomb_sig == "STRONG_REENTRY":
        warnings.append(f"외인폭탄 재매수+기타법인 (D+3 +1.88% 승률53%)")
    elif bomb_sig == "REENTRY":
        warnings.append(f"외인폭탄 재매수 감지 (D+3 +0.93%)")
    elif bomb_sig == "PULLBACK_WAIT":
        warnings.append(f"외인폭탄 D+{bomb_reentry.get('days_since_bomb', '?')} — 눌림 대기 중")
    elif bomb_sig == "QUIET_ACCUM":
        warnings.append(f"저강도 연속매수 {bomb_reentry.get('note', '')}")

    # ── C3M-T3 시장맥락 보조점수 ──
    mkt = _get_market_for_code(code)
    market_ctx = _load_market_context(mkt)
    market_adj, ctx_notes = _apply_market_context(
        pattern, entities_today, market_ctx, change_rate, market=mkt,
    )
    warnings.extend(ctx_notes)

    # ── 최종 점수 (폭탄 재매수 보너스 포함) ──
    base_score = PATTERN_SCORE[pattern]
    bomb_adj = 0
    if bomb_sig == "SECOND_BOMB":
        bomb_adj = 15   # 2차 폭탄 = 최강 매수신호
    elif bomb_sig == "EARLY" and bomb_reentry.get("is_second_bomb", False):
        bomb_adj = 12   # 2차 폭탄 직후 D+1 (통계: TP3%/SL-2% +0.77%, 승률56%)
    elif bomb_sig == "EARLY":
        bomb_adj = 3    # 1차 폭탄 D+1 — 아직 눌림 대기 필요
    elif bomb_sig == "STRONG_REENTRY":
        bomb_adj = 10
    elif bomb_sig == "REENTRY":
        bomb_adj = 5
    elif bomb_sig == "QUIET_ACCUM":
        bomb_adj = 5    # 저강도 연속매수 = 관심
    elif bomb_sig == "BOMB_TODAY":
        bomb_adj = -10  # 1차 폭탄 추격 감점

    final_score = max(0, min(100, base_score + market_adj + bomb_adj))

    return PatternResult(
        code=code,
        name=name,
        pattern=pattern,
        description=PATTERN_DESC[pattern],
        score=base_score,
        grade=PATTERN_GRADE[pattern],
        change_rate=change_rate,
        close=close,
        volume=volume,
        entities_today=entities_today,
        entities_5d=entities_5d,
        entities_10d=entities_10d,
        entities_20d=entities_20d,
        warnings=warnings,
        market_context=market_ctx,
        market_adj=market_adj + bomb_adj,
        final_score=final_score,
        cap_b=cap_b,
        symmetry=symmetry,
        bomb_reentry=bomb_reentry,
        classified_at=datetime.now().isoformat(timespec="seconds"),
    )


def _safe_num(v, default: float = 0.0) -> float:
    """H-4: NaN/None/타입 오류 방어 float 변환."""
    if v is None:
        return default
    try:
        f = float(v)
        if f != f or f in (float("inf"), float("-inf")):  # NaN/inf
            return default
        return f
    except (ValueError, TypeError):
        return default


def analyze_missed_gainers(date: str) -> List[PatternResult]:
    """오늘(date)의 missed_gainers 전체 분석."""
    mg_path = MISSED_DIR / f"{date}.json"
    if not mg_path.exists():
        raise FileNotFoundError(f"missed_gainers 파일 없음: {mg_path}")

    data = json.loads(mg_path.read_text(encoding="utf-8"))
    items = data.get("items", [])

    results = []
    skipped = 0
    for it in items:
        # H-5: code가 필수. 없으면 스킵 (KeyError 대신 warn)
        code = (it.get("code") or "").strip()
        if not code:
            skipped += 1
            continue
        # H-4: NaN/type 방어
        r = analyze_code(
            code=code,
            name=it.get("name") or code,
            change_rate=_safe_num(it.get("change_rate")),
            close=_safe_num(it.get("close")),
            volume=int(_safe_num(it.get("volume"))),
            ref_date=date,
        )
        results.append(r)
    if skipped:
        print(f"[경고] missed_gainers code 누락 {skipped}건 스킵")
    return results


def print_summary(results: List[PatternResult]) -> None:
    """콘솔 표 출력 (한글)."""
    # 패턴별 집계
    pattern_count: Dict[str, int] = {}
    for r in results:
        pattern_count[r.pattern] = pattern_count.get(r.pattern, 0) + 1

    print()
    print("=" * 80)
    print(f"수급 패턴 분류 결과 — 총 {len(results)}종목")
    print("=" * 80)
    print()

    print("[패턴별 집계]")
    for pat, cnt in sorted(pattern_count.items(), key=lambda x: -PATTERN_SCORE.get(x[0], 0)):
        print(f"  {PATTERN_DESC[pat]:30s} : {cnt}종목")
    print()

    # 종목별 상세
    print("[종목별 상세]")
    print(f"{'종목':18s} {'시총':>7s} {'등락률':>7s} {'외인':>9s} {'기관':>9s} {'개인':>9s}  {'점수':>11s}  {'패턴':16s} {'폭탄신호':12s}")
    print("-" * 130)
    # final_score 순 정렬 (시장맥락+폭탄 반영)
    results_sorted = sorted(results, key=lambda r: (-r.final_score, -r.change_rate))
    for r in results_sorted:
        f = r.entities_today.get("foreign")
        i = r.entities_today.get("institution")
        p = r.entities_today.get("individual")
        f_s = f"{f:+.1f}억" if f is not None else "    N/A"
        i_s = f"{i:+.1f}억" if i is not None else "    N/A"
        p_s = f"{p:+.1f}억" if p is not None else "    N/A"
        name_disp = r.name[:10]
        # 시총 표시
        if r.cap_b >= 10000:
            cap_s = f"{r.cap_b / 10000:.1f}조"
        elif r.cap_b > 0:
            cap_s = f"{r.cap_b:,.0f}억"
        else:
            cap_s = "N/A"
        # 점수 (조정 포함)
        if r.market_adj:
            sc_s = f"{r.final_score:>3d}({r.score}{r.market_adj:+d})"
        else:
            sc_s = f"{r.final_score:>3d}       "
        # 폭탄 재매수 신호
        bomb_sig = r.bomb_reentry.get("signal", "NONE")
        bomb_s = bomb_sig if bomb_sig != "NONE" else ""
        print(f"{name_disp:18s} {cap_s:>7s} {r.change_rate:+6.2f}% {f_s:>9s} {i_s:>9s} {p_s:>9s}  {sc_s:>11s}  {PATTERN_DESC[r.pattern][:16]:16s} {bomb_s:12s}")

    # 대칭도 양호 종목 (≤0.5)
    sym_good = [r for r in results_sorted if r.symmetry is not None and r.symmetry <= 0.5
                and r.entities_today.get("foreign", 0) and (r.entities_today.get("foreign") or 0) > 0]
    if sym_good:
        print()
        print("[대칭도 양호 종목] (외인매수 = 개인매도 대칭, ≤0.5)")
        for r in sym_good[:10]:
            f_v = r.entities_today.get("foreign", 0)
            p_v = r.entities_today.get("individual", 0)
            bomb_s = r.bomb_reentry.get("signal", "")
            print(f"  {r.name[:10]:12s} 대칭도={r.symmetry:.3f} 외인{f_v:+.1f}/개인{p_v:+.1f}억"
                  + (f" [{bomb_s}]" if bomb_s and bomb_s != "NONE" else ""))

    print()
    print("[경고 발생 종목]")
    warn_count = 0
    for r in results_sorted:
        if r.warnings:
            warn_count += 1
            print(f"  [{r.code} {r.name}] {r.pattern}")
            for w in r.warnings:
                print(f"     └ {w}")
    if warn_count == 0:
        print("  (없음)")
    print()


def save_results(results: List[PatternResult], date: str) -> Path:
    """JSON 저장."""
    out_path = OUTPUT_DIR / f"{date}.json"

    # 패턴별 분리
    by_pattern: Dict[str, List[dict]] = {}
    for r in results:
        d = asdict(r)
        by_pattern.setdefault(r.pattern, []).append(d)

    output = {
        "date": date,
        "count": len(results),
        "schema_version": "supply_flow_v3_second_bomb",
        "available_entities": AVAILABLE_ENTITIES,
        "pending_entities": [c for c in ENTITY_COLUMNS if c not in AVAILABLE_ENTITIES],
        "summary": {
            pat: {"count": len(lst), "description": PATTERN_DESC[pat]}
            for pat, lst in by_pattern.items()
        },
        # final_score 기준 정렬 (시장맥락 반영)
        "items": [asdict(r) for r in sorted(results, key=lambda r: (-r.final_score, -r.change_rate))],
        "by_pattern": by_pattern,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
    }
    out_path.write_text(
        json.dumps(output, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return out_path


def generate_bomb_watchlist(
    ref_date: Optional[str] = None,
    top_n: int = 5,
) -> List[Dict]:
    """bomb_watchlist 생성 — SECOND_BOMB/EARLY(2nd) TOP-N 선별.

    v4: 백테스트 결과 반영 — 무필터 전종목 투입 = 실패(29.2%),
    TOP-N 선별 + 점수 랭킹이 핵심.

    스코어링:
      - SECOND_BOMB: base 15점
      - EARLY(is_second_bomb): base 12점
      - 시총 3000억+: +3점 (유동성)
      - gap_days 2~5일: +2점 (최적 갭)
      - bomb_foreign_pct 0.5%+: +3점 (강한 매수)

    Args:
        ref_date: 기준일 (None=오늘)
        top_n: 상위 N개 선별 (기본 5)

    Returns:
        [{code, name, signal, score, bomb_adj, gap_days, cap_b, ...}]
        + data_store/bomb_watchlist.json 저장
    """
    _load_universe()
    universe = _UNIVERSE_CACHE or {}
    candidates = []

    for code in universe:
        csv_path = FLOW_DIR / f"{code}_investor.csv"
        df = _csv_to_canonical_df(csv_path)
        if df is None or df.empty:
            continue

        cap_b = _get_cap_for_code(code)
        if cap_b <= 0:
            continue

        result = _detect_bomb_reentry(df, cap_b, ref_date=ref_date)
        sig = result.get("signal", "NONE")

        if sig == "SECOND_BOMB":
            bomb_adj = 15
        elif sig == "EARLY" and result.get("is_second_bomb", False):
            bomb_adj = 12
        else:
            continue

        # 스코어링
        rank_score = bomb_adj
        if cap_b >= 3000:
            rank_score += 3  # 유동성 보너스
        gap = result.get("gap_days", 0)
        if 2 <= gap <= 5:
            rank_score += 2  # 최적 갭
        bfp = result.get("bomb_foreign_pct", 0)
        if bfp >= 0.5:
            rank_score += 3  # 강한 매수

        name = universe.get(code, {}).get("name", code) if isinstance(universe.get(code), dict) else code
        candidates.append({
            "code": code,
            "name": name,
            "signal": sig,
            "is_second_bomb": True,
            "bomb_adj": bomb_adj,
            "rank_score": rank_score,
            "gap_days": gap,
            "cap_b": round(cap_b, 0),
            "bomb_foreign_pct": bfp,
            "bomb_date": result.get("bomb_date", ""),
            "track": "B",
        })

    # TOP-N 정렬: rank_score 내림 → 시총 내림
    candidates.sort(key=lambda x: (-x["rank_score"], -x["cap_b"]))
    watchlist = candidates[:top_n]

    # JSON 저장
    output = {
        "date": ref_date or datetime.now().strftime("%Y-%m-%d"),
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "total_candidates": len(candidates),
        "top_n": top_n,
        "watchlist": watchlist,
    }
    out_path = SCALPER_DIR / "data_store" / "bomb_watchlist.json"
    try:
        out_path.write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8")
        logger.info("bomb_watchlist 저장: %d/%d종목 → %s", len(watchlist), len(candidates), out_path)
    except Exception as e:
        logger.error("bomb_watchlist 저장 실패: %s", e)

    return watchlist


def main():
    ap = argparse.ArgumentParser(description="T3 수급 패턴 감지기")
    ap.add_argument("--date", default=None, help="기준일 YYYY-MM-DD (기본: 오늘)")
    ap.add_argument("--codes", default=None, help="직접 지정 (콤마 구분, 예: 077360,009580)")
    ap.add_argument("--no-save", action="store_true", help="파일 저장 생략")
    args = ap.parse_args()

    date = args.date or datetime.now().strftime("%Y-%m-%d")

    if args.codes:
        # 코드 직접 지정
        codes = [c.strip() for c in args.codes.split(",") if c.strip()]
        # missed_gainers에서 이름/등락률 보조 조회
        mg_items = {}
        mg_path = MISSED_DIR / f"{date}.json"
        if mg_path.exists():
            mg_data = json.loads(mg_path.read_text(encoding="utf-8"))
            for it in mg_data.get("items", []):
                mg_items[it["code"]] = it

        results = []
        for code in codes:
            it = mg_items.get(code, {})
            r = analyze_code(
                code=code,
                name=it.get("name", code),
                change_rate=float(it.get("change_rate", 0.0)),
                close=float(it.get("close", 0.0)),
                volume=int(it.get("volume", 0)),
                ref_date=date,
            )
            results.append(r)
    else:
        # missed_gainers 전체
        results = analyze_missed_gainers(date)

    print_summary(results)

    if not args.no_save:
        out_path = save_results(results, date)
        print(f"[저장] {out_path}")


if __name__ == "__main__":
    main()
