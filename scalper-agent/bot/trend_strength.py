# -*- coding: utf-8 -*-
"""★ 5/24 사장님 5/22 13-C 명령 ★ 추세 강도 측정 — 학습 데이터 연동.

사장님 결정 [project_5_22_evening_learning_fix]:
    - 5분봉 연속 양봉 카운트
    - 거래량 폭증 (직전 30분 2배+)
    - 체결강도 > 100 + 외인 매수
    - VWAP 상단 유지

목적:
    매도 결정 (트레일링 폭 / 자비스 약화 트리거) 시 추세 강도 점수 반영.
    강한 추세 = 일시 조정 견디기 / 약한 추세 = 빠른 익절.

설계:
    - intraday_learning_v2 데이터 수집 함수 재사용 (collect_five_min_bars/derive_vwap/collect_flow_intensity)
    - 4가지 factor → 점수 0~10
    - 등급 매핑: WEAK(<3) / MID(3~5) / STRONG(6~7) / EXPLOSIVE(8+)
"""
import logging
import sys
import time
from pathlib import Path
from typing import Dict, Optional

logger = logging.getLogger("BH.TrendStrength")

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))


# 등급 임계값
GRADE_THRESHOLDS = {
    "EXPLOSIVE": 8,
    "STRONG":    6,
    "MID":       3,
    "WEAK":      0,
}


def _grade_from_score(score: float) -> str:
    if score >= GRADE_THRESHOLDS["EXPLOSIVE"]:
        return "EXPLOSIVE"
    if score >= GRADE_THRESHOLDS["STRONG"]:
        return "STRONG"
    if score >= GRADE_THRESHOLDS["MID"]:
        return "MID"
    return "WEAK"


def compute_trend_strength(trader, code: str, current_price: int) -> Dict:
    """추세 강도 점수 계산 (4-factor 0~10).

    Args:
        trader: KIS trader (fetch_minute_chart 보유)
        code: 종목코드
        current_price: 현재가 (VWAP 비교용)

    Returns:
        {
            "score": float (0~10),
            "grade": "WEAK"/"MID"/"STRONG"/"EXPLOSIVE"/"UNKNOWN",
            "factors": {
                "bullish_candles": int (직전 6봉 중 양봉 수),
                "volume_ratio": float (직전 6봉 합 / 그 이전 6봉 합),
                "vwap_diff_pct": float (현재가 vs VWAP 차이 %),
                "flow_intensity": float (체결강도),
            },
            "ts": int (epoch — 캐시용)
        }
    """
    try:
        from tools.intraday_learning_v2 import (
            collect_five_min_bars, derive_vwap, collect_flow_intensity,
        )
    except Exception as e:
        logger.warning(f"[trend_strength] import 실패: {e}")
        return {"score": 0, "grade": "UNKNOWN", "factors": {}, "ts": int(time.time())}

    bars = collect_five_min_bars(trader, code, n_bars=12)   # 1시간 (12 × 5분)
    if not bars or len(bars) < 6:
        return {"score": 0, "grade": "UNKNOWN", "factors": {"reason": "no_bars"}, "ts": int(time.time())}

    # ── Factor 1: 직전 6봉 양봉 카운트 (0~3점) ──
    recent = bars[-6:]
    bullish = sum(1 for b in recent if b.get("close", 0) > b.get("open", 0))
    bullish_score = min(3, bullish // 2)   # 6=3, 4~5=2, 2~3=1, 0~1=0

    # ── Factor 2: 거래량 폭증 (0~3점) ──
    recent_vol = sum(b.get("volume", 0) for b in bars[-6:])
    prev_vol = sum(b.get("volume", 0) for b in bars[-12:-6]) if len(bars) >= 12 else recent_vol
    vol_ratio = (recent_vol / prev_vol) if prev_vol > 0 else 1.0
    if vol_ratio >= 3.0:
        vol_score = 3
    elif vol_ratio >= 2.0:
        vol_score = 2
    elif vol_ratio >= 1.5:
        vol_score = 1
    else:
        vol_score = 0

    # ── Factor 3: VWAP 상단 유지 (0~2점) ──
    vwap = derive_vwap(bars)
    vwap_score = 0
    vwap_diff_pct = 0.0
    if vwap and current_price > 0:
        vwap_diff_pct = (current_price - vwap) / vwap * 100
        if vwap_diff_pct >= 2.0:
            vwap_score = 2
        elif vwap_diff_pct >= 0.0:
            vwap_score = 1
        # 음수면 0점 (VWAP 하단)

    # ── Factor 4: 체결강도 (0~2점) ──
    # ★ 5/24 검수 fix ★ 데이터 없으면 0점 (이전: 기본 100 → 가짜 1점)
    flow_intensity = 0.0
    flow_score = 0
    try:
        flow = collect_flow_intensity(code)
        if flow:
            raw = flow.get("intensity")
            if raw is None:
                raw = flow.get("flow_intensity")
            if raw is None:
                raw = flow.get("strength")
            if raw is not None:
                flow_intensity = float(raw)
                if flow_intensity >= 150:
                    flow_score = 2
                elif flow_intensity >= 100:
                    flow_score = 1
            # raw 가 None 이면 flow_score=0 유지 (가짜 점수 X)
    except Exception:
        pass

    total = bullish_score + vol_score + vwap_score + flow_score

    return {
        "score": float(total),
        "grade": _grade_from_score(total),
        "factors": {
            "bullish_candles": int(bullish),
            "volume_ratio": round(vol_ratio, 2),
            "vwap_diff_pct": round(vwap_diff_pct, 2),
            "flow_intensity": round(flow_intensity, 1),
        },
        "ts": int(time.time()),
    }


def is_strong_trend(strength: Dict) -> bool:
    """STRONG 이상 (조정 인내 적용 기준)."""
    return strength.get("grade") in ("STRONG", "EXPLOSIVE")


def is_weak_trend(strength: Dict) -> bool:
    """WEAK (빠른 익절 기준)."""
    return strength.get("grade") == "WEAK"


def is_genuine_weakening(strength: Dict, prev_strength: Optional[Dict] = None) -> bool:
    """진짜 약화 vs 일시 조정 구분 (13-D 보조용).

    조건 (사장님 5/22 13-D):
        - VWAP 하단 (vwap_diff_pct < 0)
        - 거래량 감소 (volume_ratio < 1.0)
        - 체결강도 < 100

    이 3개 동시 만족 = 진짜 약화. 일부만이면 일시 조정.

    ★ 5/24 검수 fix ★ factors 없거나 기본값만 있으면 보수적으로 True 반환
    (data 부재 = 안전 결정 불가 → 차단 안 발동 = 매도 진행 = 안전 default)
    """
    f = strength.get("factors", {})
    if not f or strength.get("grade") == "UNKNOWN":
        # 데이터 부재 → 보수적으로 진짜 약화 처리 → 13-D 차단 안 됨
        return True
    # 명시적 값 있는 경우만 검증 (기본값 fallback 시 None 처리)
    vwap_diff = f.get("vwap_diff_pct")
    vol_ratio = f.get("volume_ratio")
    flow_int = f.get("flow_intensity")
    if vwap_diff is None or vol_ratio is None or flow_int is None:
        return True   # 데이터 부재 = 보수적
    return (vwap_diff < 0) and (vol_ratio < 1.0) and (flow_int < 100)
