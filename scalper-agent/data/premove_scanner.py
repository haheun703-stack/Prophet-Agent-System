"""
사전감지 스캐너 (Pre-Move Scanner)
───────────────────────────────────
3-Gate 필터 + 10개 사전감지 신호 조합
"오르기 직전" 종목만 최대 5개 추출

기존 14개 함수를 오케스트레이션 — 새 감지 로직 없음
"""
import sys, os, logging, json, warnings
warnings.filterwarnings("ignore")
from dataclasses import dataclass, field
from typing import List, Optional, Tuple
from pathlib import Path
from datetime import datetime, timedelta

import pandas as pd
import numpy as np

# CLI 직접 실행 시 경로 보정
if __name__ == "__main__":
    sys.path.insert(0, str(Path(__file__).parent.parent))
    from dotenv import load_dotenv
    load_dotenv(str(Path(__file__).parent.parent.parent / ".env"))

from data.supply_analyzer import SupplyAnalyzer, SupplyFull
from data.volume_scanner import detect_patterns as vol_detect_patterns
from data.swing_indicators import analyze_stock as swing_analyze
from data.universe_builder import load_universe

logger = logging.getLogger(__name__)

# ── 신호 점수 테이블 ──
SIGNAL_POINTS = {
    "QUIET_ACCUMULATION": 20,
    "OBV_BREAKOUT": 15,
    "MULTI_DAY_ACCUM": 12,
    "BIG_MONEY_INFLOW": 15,
    "INST_STREAK": 20,
    "SUPPLY_ACCEL": 12,
    "RETAIL_CONTRARIAN": 15,
    "FOREIGN_INFLECTION": 12,
    "VOL_EXHAUSTION": 18,
    "TECH_ALIGNMENT": 15,
}

DATA_STORE = Path(__file__).parent.parent / "data_store"


@dataclass
class PreMoveCandidate:
    """사전감지 후보 종목"""
    code: str
    name: str
    premove_score: float          # 사전감지 점수 (0~100)
    signal_count: int             # 발동 신호 수
    signals: list                 # [{type, description, points}]

    # 수급 컨텍스트
    supply_grade: str             # A+/A
    momentum_signal: str          # ACC/STEADY/DEC
    energy_grade: str             # EXPLOSIVE/HUNTABLE/...
    composite_score: float        # 6D 종합점수

    # 개선된 기준선
    entry: float
    sl: float
    tp1_quick: float              # 퀵 TP (~5-7%)
    tp2: float                    # 2R
    sl_source: str
    risk_pct: float               # SL 거리 %

    # 추가 컨텍스트
    close: float
    vol_ratio: float              # 당일 거래량 / MA20
    rsi: float
    inst_streak: int
    inst_streak_amount: float     # 억원


def detect_premove_signals(code: str, name: str,
                           supply_full: SupplyFull,
                           day_df: pd.DataFrame) -> List[dict]:
    """10개 사전감지 신호 체크 — 기존 함수 재사용

    Returns: [{type, description, points}]
    """
    signals = []
    momentum = supply_full.momentum

    # ── 신호 1~4: 거래량 패턴 (volume_scanner) ──
    try:
        vol_result = vol_detect_patterns(day_df.copy(), code, name)
        for p in vol_result.get("patterns", []):
            ptype = p.get("type", "")
            if ptype in SIGNAL_POINTS:
                signals.append({
                    "type": ptype,
                    "description": p.get("description", ptype),
                    "points": SIGNAL_POINTS[ptype],
                })
    except Exception as e:
        logger.debug(f"vol_detect_patterns failed for {code}: {e}")

    # ── 신호 5: 기관 연속매수 5일+ ──
    if momentum.inst_streak >= 5:
        signals.append({
            "type": "INST_STREAK",
            "description": f"기관 {momentum.inst_streak}일 연속매수 ({momentum.inst_streak_amount:+.0f}억)",
            "points": SIGNAL_POINTS["INST_STREAK"],
        })

    # ── 신호 6: 수급가속 30%+ ──
    if momentum.supply_accel > 30:
        signals.append({
            "type": "SUPPLY_ACCEL",
            "description": f"수급가속 {momentum.supply_accel:+.0f}%",
            "points": SIGNAL_POINTS["SUPPLY_ACCEL"],
        })

    # ── 신호 7: 개인 역지표 ──
    if momentum.retail_contrarian:
        signals.append({
            "type": "RETAIL_CONTRARIAN",
            "description": f"개인 {momentum.retail_net_5d:+.0f}억 vs 스마트 {momentum.smart_net_5d:+.0f}억",
            "points": SIGNAL_POINTS["RETAIL_CONTRARIAN"],
        })

    # ── 신호 8: 외인소진율 변곡 ──
    if momentum.foreign_inflection == "UP_TURN":
        signals.append({
            "type": "FOREIGN_INFLECTION",
            "description": f"외인 소진율 상향변곡 (가속 {momentum.foreign_exh_accel:+.2f})",
            "points": SIGNAL_POINTS["FOREIGN_INFLECTION"],
        })

    # ── 신호 9: 거래량 건조 (직접 계산) ──
    try:
        close_col = "close" if "close" in day_df.columns else "종가"
        vol_col = "volume" if "volume" in day_df.columns else "거래량"
        if vol_col in day_df.columns and len(day_df) >= 25:
            vol = day_df[vol_col].astype(float)
            ma20_vol = vol.rolling(20).mean()
            recent_3d_vol = vol.iloc[-3:].mean()
            ma20_val = float(ma20_vol.iloc[-1])
            if ma20_val > 0 and recent_3d_vol < ma20_val * 0.5:
                # 가격 변동폭 체크
                prices = day_df[close_col].astype(float)
                price_range_3d = (prices.iloc[-3:].max() - prices.iloc[-3:].min()) / prices.iloc[-1] * 100
                if price_range_3d < 3:
                    signals.append({
                        "type": "VOL_EXHAUSTION",
                        "description": f"거래량 건조 (3일평균/MA20={recent_3d_vol/ma20_val:.1%}, 변동{price_range_3d:.1f}%)",
                        "points": SIGNAL_POINTS["VOL_EXHAUSTION"],
                    })
    except Exception as e:
        logger.debug(f"vol_exhaustion check failed for {code}: {e}")

    # ── 신호 10: 기술적 정렬 (EMA상향 + RSI 35~55 + MACD 색전환) ──
    try:
        tech = swing_analyze(day_df.copy())
        ema_trend = tech.get("ema_trend", "UNKNOWN")
        rsi_val = tech.get("rsi", 50)
        hist = tech.get("histogram", {})
        hist_triggered = hist.get("triggered", False) if isinstance(hist, dict) else False
        hist_dir = hist.get("direction", None) if isinstance(hist, dict) else None

        if (ema_trend == "BULLISH" and 35 <= rsi_val <= 55
                and hist_triggered and hist_dir == "BUY"):
            signals.append({
                "type": "TECH_ALIGNMENT",
                "description": f"EMA↑ + RSI({rsi_val:.0f}) + MACD 색전환",
                "points": SIGNAL_POINTS["TECH_ALIGNMENT"],
            })
    except Exception as e:
        logger.debug(f"tech alignment check failed for {code}: {e}")

    return signals


def calc_premove_score(signals: list, supply_full: SupplyFull) -> float:
    """사전감지 점수 계산 (0~100)

    = 신호점수(50%) + 수급종합점수(30%) + 신호개수 보너스(20%)
    """
    signal_pts = sum(s["points"] for s in signals)
    signal_pts_norm = min(100, signal_pts)

    supply_component = supply_full.composite_score  # 이미 0~100

    count_bonus = min(len(signals), 6) / 6 * 100

    total = signal_pts_norm * 0.50 + supply_component * 0.30 + count_bonus * 0.20
    return round(total, 1)


def scan_premove(top_n: int = 5, as_of: str = None) -> List[PreMoveCandidate]:
    """사전감지 스캔 — 3-Gate 파이프라인

    Gate 1: 수급 퀄리티 (A+/A)
    Gate 2: 에너지 + 모멘텀 (EXPLOSIVE/HUNTABLE + ACC)
    Gate 3: 과열 방지 (10D < 25%, 전일 < 10%)

    Returns: 최대 top_n개 PreMoveCandidate
    """
    print("=" * 60)
    print("  🔮 사전감지 스캐너 (Pre-Move Scanner)")
    print("=" * 60)

    # 유니버스
    universe = load_universe()
    codes = list(universe.keys())
    print(f"유니버스: {len(codes)}개")

    # 분석기
    analyzer = SupplyAnalyzer()

    gate1_pass = []
    gate2_pass = []
    gate3_pass = []
    candidates = []

    for i, code in enumerate(codes):
        if (i + 1) % 200 == 0:
            print(f"  스캔 중... {i+1}/{len(codes)}")

        try:
            full = analyzer.analyze_full(code, as_of=as_of)
            if not full:
                continue
        except Exception:
            continue

        name = universe.get(code, {}).get("name", code)

        # ── GATE 1: 수급 퀄리티 ──
        grade = full.score.grade
        if grade not in ("A+", "A"):
            continue
        gate1_pass.append(code)

        # ── GATE 2: 에너지 + 모멘텀 ──
        stability = full.stability
        if not stability:
            continue
        energy = stability.stability_grade
        if energy not in ("EXPLOSIVE", "HUNTABLE"):
            continue
        mom_signal = full.momentum.signal
        if mom_signal != "ACC":
            continue
        gate2_pass.append(code)

        # ── GATE 3: 과열 방지 ──
        day_df = analyzer._cache_daily.get(code)
        if day_df is None or len(day_df) < 15:
            continue

        if as_of:
            day_df = day_df[day_df.index <= pd.Timestamp(as_of)]

        close_col = "close" if "close" in day_df.columns else "종가"
        prices = day_df[close_col].astype(float)
        if len(prices) < 11:
            continue

        ret_10d = (prices.iloc[-1] - prices.iloc[-11]) / prices.iloc[-11] * 100
        ret_1d = (prices.iloc[-1] - prices.iloc[-2]) / prices.iloc[-2] * 100 if len(prices) >= 2 else 0

        if ret_10d > 25 or ret_1d > 10:
            continue
        gate3_pass.append(code)

        # ── 사전감지 신호 ──
        signals = detect_premove_signals(code, name, full, day_df)
        if len(signals) < 3:
            continue

        # ── 스코어링 ──
        score = calc_premove_score(signals, full)
        if score < 50:
            continue

        # ── 기준선 ──
        baseline = full.baseline
        if not baseline:
            continue

        sl_pct = (baseline.invalidation - baseline.close) / baseline.close * 100

        # RSI
        rsi = 50.0
        try:
            tech = swing_analyze(day_df.copy())
            rsi = tech.get("rsi", 50.0)
        except:
            pass

        # 거래량 비율
        vol_col = "volume" if "volume" in day_df.columns else "거래량"
        vol_ratio = 1.0
        try:
            vol = day_df[vol_col].astype(float)
            ma20 = vol.rolling(20).mean().iloc[-1]
            if ma20 > 0:
                vol_ratio = float(vol.iloc[-1] / ma20)
        except:
            pass

        candidates.append(PreMoveCandidate(
            code=code,
            name=name,
            premove_score=score,
            signal_count=len(signals),
            signals=signals,
            supply_grade=grade,
            momentum_signal=mom_signal,
            energy_grade=energy,
            composite_score=full.composite_score,
            entry=baseline.close,
            sl=baseline.invalidation,
            tp1_quick=baseline.target_1_quick,
            tp2=baseline.target_1,  # 2R
            sl_source=baseline.invalidation_source,
            risk_pct=sl_pct,
            close=baseline.close,
            vol_ratio=vol_ratio,
            rsi=rsi,
            inst_streak=full.momentum.inst_streak,
            inst_streak_amount=full.momentum.inst_streak_amount,
        ))

    print(f"\n필터 통과: G1={len(gate1_pass)} → G2={len(gate2_pass)} → G3={len(gate3_pass)} → 신호3+={len(candidates)}")

    # 점수 순 정렬
    candidates.sort(key=lambda c: c.premove_score, reverse=True)
    result = candidates[:top_n]

    print(f"최종 후보: {len(result)}개\n")
    return result


def format_premove_report(candidates: List[PreMoveCandidate]) -> str:
    """텔레그램용 리포트 포맷"""
    if not candidates:
        return "🔮 사전감지: 조건 충족 종목 없음"

    lines = [
        "🔮 사전감지 스캔 결과",
        f"━━━━━━━━━━━━━━━━━━━━",
    ]

    for i, c in enumerate(candidates, 1):
        lines.append(f"\n#{i} {c.name}({c.code})")
        lines.append(f"  점수: {c.premove_score:.0f}/100 | 신호: {c.signal_count}개")
        lines.append(f"  수급: {c.supply_grade} | 모멘텀: {c.momentum_signal} | 에너지: {c.energy_grade}")
        lines.append(f"  종가: {c.close:,.0f}원 | RSI: {c.rsi:.0f} | Vol: {c.vol_ratio:.1f}x")

        # SL/TP
        lines.append(f"  SL: {c.sl:,.0f}({c.sl_source},{c.risk_pct:+.1f}%)")
        lines.append(f"  퀵TP: {c.tp1_quick:,.0f}({(c.tp1_quick-c.close)/c.close*100:+.1f}%)"
                      f" | TP(2R): {c.tp2:,.0f}({(c.tp2-c.close)/c.close*100:+.1f}%)")

        # 기관
        if c.inst_streak != 0:
            lines.append(f"  기관: {c.inst_streak:+d}일 ({c.inst_streak_amount:+.0f}억)")

        # 신호 목록
        sig_names = [s["type"].replace("_", " ").title() for s in c.signals]
        lines.append(f"  신호: {' | '.join(sig_names)}")

    lines.append(f"\n{datetime.now().strftime('%Y-%m-%d %H:%M')}")
    return "\n".join(lines)


def save_premove_candidates(candidates: List[PreMoveCandidate]) -> Path:
    """premove_candidates.json 저장"""
    DATA_STORE.mkdir(exist_ok=True)
    path = DATA_STORE / "premove_candidates.json"
    data = []
    for c in candidates:
        data.append({
            "code": c.code,
            "name": c.name,
            "premove_score": c.premove_score,
            "signal_count": c.signal_count,
            "entry": c.entry,
            "sl": c.sl,
            "tp1_quick": c.tp1_quick,
            "tp2": c.tp2,
            "sl_source": c.sl_source,
        })
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    return path


# ── CLI ──
if __name__ == "__main__":
    candidates = scan_premove(top_n=5)
    print(format_premove_report(candidates))

    if candidates:
        path = save_premove_candidates(candidates)
        print(f"\n저장: {path}")
