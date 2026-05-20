# -*- coding: utf-8 -*-
"""trade_style_decider — 자비스 자율 보유 스타일 결정 (단타 vs 스윙).

[사장님 5/20 21:00 비전]
> "위에서 내가 준것처럼 길게 끌고 갈껀지. 단타로 칠건지를 선정해야되...
>  그건 장에 움직임에 따라 다르니까... 그래서 너 ai 에게 부탁하고 지시를 하는거잖아"

= 매수 직전 자비스가 "이 종목 + 이 시장 = 단타 or 스윙" 자율 결정

스타일 분류:
  - SCALP_1H (1시간 이내 익절): 시초가 급등 모멘텀, 거래량 폭증
  - DAY_TRADE (당일 청산): 일반 단타 — 15:25 자동 청산
  - SWING_2D~10D (2~10일 보유): 추세 형성, 큰형 BULLISH, 강한 테마
  - LONG_HOLD (사장님 명령 시): 분기 실적 등 fundamental — 사장님이 sl_disabled 명시

결정 입력:
  1. brain_state regime (큰형 advisory)
  2. 종목 sector / 테마 강도 (theme_universe + sector_concurrent_surge)
  3. 거래량 추세 (5일 평균 vs 오늘)
  4. surge_pattern_learner 학습 통계 (이 섹터/패턴의 평균 보유 적정 일수)
  5. 매수 시점 (시초가 5분 vs 11시 vs 오후)

[자비스 활용]
  auto_trader.asset_pool_scan_and_buy() 매수 직전 호출 →
  → style 결정 → SL/TP/보유일 설정
"""
import json
import logging
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Dict, Optional

try:
    from zoneinfo import ZoneInfo
    KST = ZoneInfo("Asia/Seoul")
except ImportError:
    KST = None

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))

logger = logging.getLogger(__name__)


# 스타일 정의 (SL/TP/max_hold_days)
STYLE_PROFILES = {
    "SCALP_1H": {"sl_pct": -2.0, "tp_pct": 3.0, "max_hold_minutes": 60, "max_hold_days": 0},
    "DAY_TRADE": {"sl_pct": -3.0, "tp_pct": 5.0, "max_hold_minutes": 360, "max_hold_days": 1},
    "SWING_2D": {"sl_pct": -5.0, "tp_pct": 10.0, "max_hold_days": 2},
    "SWING_5D": {"sl_pct": -7.0, "tp_pct": 15.0, "max_hold_days": 5},
    "SWING_10D": {"sl_pct": -10.0, "tp_pct": 25.0, "max_hold_days": 10},
    "LONG_HOLD": {"sl_pct": None, "tp_pct": None, "max_hold_days": 60},  # 사장님 명시만
}


def _load_brain_state() -> Optional[Dict]:
    path = _ROOT / "data_store" / "brain_state.json"
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _load_theme_subtheme(code: str) -> Optional[Dict]:
    """막내 theme_universe에서 종목의 서브테마 + weight 조회."""
    import os
    paths = [
        "/home/ubuntu/jgis/data_store/theme_universe.json",
        str(_ROOT / "data_store" / "theme_universe.json"),
    ]
    for p in paths:
        if not os.path.exists(p):
            continue
        try:
            data = json.loads(open(p, encoding="utf-8").read())
            for theme in data.get("themes", []):
                for sub in theme.get("subthemes", []):
                    for stock in sub.get("stocks", []):
                        if stock.get("code") == code:
                            return {
                                "theme": theme.get("theme_id"),
                                "subtheme": sub.get("id"),
                                "weight": sub.get("weight", 0),
                            }
        except Exception:
            pass
    return None


def _get_sector_surge_count(code: str) -> int:
    """오늘 같은 섹터에 +10%+ 동조 종목 몇 개 있는지."""
    path = _ROOT / "data_store" / "sector_concurrent_surge.json"
    if not path.exists():
        return 0
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        # 데이터 구조에 따라 추출 (구조 모르므로 best-effort)
        if isinstance(data, dict):
            for sector_name, items in data.items():
                if isinstance(items, list):
                    codes = [str(x.get("code", "") if isinstance(x, dict) else x) for x in items]
                    if code in codes:
                        return len(codes)
        return 0
    except Exception:
        return 0


def decide_trade_style(
    code: str,
    name: str = "",
    change_pct: float = 0.0,
    volume_ratio: float = 1.0,
    source_signals: Optional[list] = None,
    current_time: Optional[datetime] = None,
) -> Dict:
    """매수 직전 자비스 자율 결정.

    Args:
        code: 종목 코드
        change_pct: 현재 등락률 (%)
        volume_ratio: 거래량 비율 (오늘 / 5일 평균)
        source_signals: asset_pool_loader가 발굴한 시그널 소스 리스트
        current_time: 결정 시각 (None=현재)

    Returns:
        {
            "style": "DAY_TRADE" | "SWING_2D" | ...,
            "sl_pct": -3.0,
            "tp_pct": 5.0,
            "max_hold_days": 1,
            "reason": "...",
            "confidence": "HIGH/MED/LOW",
        }
    """
    if current_time is None:
        current_time = datetime.now(KST) if KST else datetime.now()
    source_signals = source_signals or []

    # 1) 큰형 regime
    brain = _load_brain_state() or {}
    regime = (brain.get("regime") or "NEUTRAL").upper()
    allocation = brain.get("allocation_pct", 50)

    # 2) 테마 정보
    theme_info = _load_theme_subtheme(code) or {}
    theme_weight = theme_info.get("weight", 0)
    subtheme = theme_info.get("subtheme", "")

    # 3) 섹터 동조 강도
    sector_concurrent = _get_sector_surge_count(code)

    # 4) 시간대 (시초가/오전/오후)
    hour = current_time.hour
    is_morning = 9 <= hour < 11
    is_lunch_after = 11 <= hour < 14
    is_late = 14 <= hour < 15

    # ─── 결정 로직 ───────────────────────────────────────
    reasons = []

    # PANIC → 매수 자체 안 함 (이건 호출 전에 차단되지만 safety)
    if regime == "PANIC":
        return {
            "style": "DAY_TRADE", "sl_pct": -2.0, "tp_pct": 2.0,
            "max_hold_days": 0, "max_hold_minutes": 60,
            "reason": "PANIC regime — 최단 보유, 작은 SL/TP",
            "confidence": "LOW",
        }

    # 1) 강한 테마(진단키트 weight 30+) + 큰형 BULLISH + 섹터 동조 3+ → SWING_5D
    if theme_weight >= 25 and regime in ("BULLISH", "NEUTRAL") and sector_concurrent >= 3:
        reasons.append(f"강한 테마 [{subtheme}] weight={theme_weight}")
        reasons.append(f"섹터 동조 {sector_concurrent}종")
        reasons.append(f"regime={regime}")
        profile = STYLE_PROFILES["SWING_5D"]
        return {
            "style": "SWING_5D",
            "sl_pct": profile["sl_pct"], "tp_pct": profile["tp_pct"],
            "max_hold_days": profile["max_hold_days"],
            "reason": " + ".join(reasons),
            "confidence": "HIGH",
        }

    # 2) 시초가 강한 모멘텀 (+20%+ + 거래량 5배+) → SCALP_1H (단발 익절)
    if is_morning and change_pct >= 20 and volume_ratio >= 5:
        reasons.append(f"시초가 모멘텀 +{change_pct:.1f}% / 거래량 {volume_ratio:.1f}x")
        profile = STYLE_PROFILES["SCALP_1H"]
        return {
            "style": "SCALP_1H",
            "sl_pct": profile["sl_pct"], "tp_pct": profile["tp_pct"],
            "max_hold_minutes": profile["max_hold_minutes"],
            "max_hold_days": 0,
            "reason": " + ".join(reasons),
            "confidence": "MED",
        }

    # 3) BEARISH regime + 단발 시그널 → DAY_TRADE (15:25 청산)
    if regime == "BEARISH":
        reasons.append(f"BEARISH regime → 당일 청산")
        profile = STYLE_PROFILES["DAY_TRADE"]
        return {
            "style": "DAY_TRADE",
            "sl_pct": profile["sl_pct"], "tp_pct": profile["tp_pct"],
            "max_hold_days": profile["max_hold_days"],
            "reason": " + ".join(reasons),
            "confidence": "MED",
        }

    # 4) limit_up_trigger + 외인_매집 동시 시그널 → SWING_2D
    has_limit = any("limit_up" in s for s in source_signals)
    has_accum = any("accumulation" in s or "foreign" in s for s in source_signals)
    if has_limit and has_accum:
        reasons.append("상한가 + 매집 동시 시그널 → 추세 형성")
        profile = STYLE_PROFILES["SWING_2D"]
        return {
            "style": "SWING_2D",
            "sl_pct": profile["sl_pct"], "tp_pct": profile["tp_pct"],
            "max_hold_days": profile["max_hold_days"],
            "reason": " + ".join(reasons),
            "confidence": "HIGH",
        }

    # 5) 오후 늦은 시간 진입 (14시+) → 다음날 갭 노려서 SWING_2D
    if is_late and change_pct >= 5:
        reasons.append(f"오후 {hour}시 +{change_pct:.1f}% → 다음날 갭 노림")
        profile = STYLE_PROFILES["SWING_2D"]
        return {
            "style": "SWING_2D",
            "sl_pct": profile["sl_pct"], "tp_pct": profile["tp_pct"],
            "max_hold_days": profile["max_hold_days"],
            "reason": " + ".join(reasons),
            "confidence": "MED",
        }

    # default — DAY_TRADE (가장 안전)
    profile = STYLE_PROFILES["DAY_TRADE"]
    return {
        "style": "DAY_TRADE",
        "sl_pct": profile["sl_pct"], "tp_pct": profile["tp_pct"],
        "max_hold_days": profile["max_hold_days"],
        "reason": "기본 = 당일 청산 (특별 시그널 없음)",
        "confidence": "LOW",
    }


if __name__ == "__main__":
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
    logging.basicConfig(level=logging.INFO)

    # 테스트 케이스 — 5/20 학습 종목들
    test_cases = [
        ("142280", "녹십자엠에스", 29.84, 10.0, ["limit_up_trigger", "theme:diagnostic_kit"]),
        ("011000", "진원생명과학", 19.62, 8.5, ["theme:diagnostic_kit", "short_cover"]),
        ("215790", "이노인스트루먼트", 29.80, 12.0, ["kis_market_top"]),
        ("066570", "LG전자", 1.5, 1.2, ["limit_up_trigger", "oneshot_stealth"]),
    ]
    print("=== trade_style_decider 테스트 (5/20 사례) ===")
    for code, name, chg, vol, sigs in test_cases:
        result = decide_trade_style(code, name, change_pct=chg, volume_ratio=vol,
                                      source_signals=sigs)
        print(f"\n[{code}] {name} +{chg}% (vol {vol}x, sigs={sigs}):")
        print(f"  → {result['style']} (SL {result['sl_pct']}%, TP {result['tp_pct']}%, "
              f"max_hold={result.get('max_hold_days', '?')}d, conf={result['confidence']})")
        print(f"  사유: {result['reason']}")
