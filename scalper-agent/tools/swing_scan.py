# -*- coding: utf-8 -*-
"""
스윙매매 종합 스크리너 - 4층 파이프라인
==========================================
매일 20:00 실행. 전종목 → 수급(5D) + 기술(OBV/EMA/RSI/히스토그램) + 이상거래 → TOP 10

파이프라인:
  L1: 유니버스 (시총 1000억+, ~1500종목)
  L2: 수급 필터 (5D 분석 → A+/A/B 이상)
  L3: 기술 분석 (OBV 비토 → EMA → RSI → 히스토그램)
  L4: 이상거래 보너스 (조용한 매집, 큰손 진입)
  → 최종 스코어 = 수급(40%) + 기술(40%) + 이상거래(20%)

사용법:
  python -m tools.swing_scan                    # 전체 스캔
  python -m tools.swing_scan --top 5            # TOP 5만
  python -m tools.swing_scan --telegram         # 텔레그램 전송
  python -m tools.swing_scan --code 005930      # 개별 종목 분석
"""

import sys
import os
import io
import json
import logging
from pathlib import Path
from datetime import datetime
from dataclasses import dataclass, field
from typing import List, Optional

import pandas as pd

# 경로 설정
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
os.chdir(os.path.join(os.path.dirname(__file__), '..'))

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '..', '.env'))

logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data_store"
DAILY_DIR = DATA_DIR / "daily"
RESULT_DIR = DATA_DIR / "scan_results"


# ═══════════════════════════════════════════════════
#  스윙 후보 데이터 구조
# ═══════════════════════════════════════════════════

@dataclass
class SwingCandidate:
    """스윙매매 후보 종목"""
    code: str
    name: str

    # L2: 수급 점수
    supply_grade: str = ""          # A+/A/B/C/D
    supply_score: float = 0         # 3D 수급 0~100
    momentum_signal: str = ""       # ACC/STEADY/DEC
    momentum_score: float = 0       # 4D 모멘텀 0~100
    energy_grade: str = ""          # 5D 에너지 등급
    energy_score: float = 0         # 5D 에너지 0~100
    action: str = ""                # STRONG_BUY/BUY/ENTER/WATCH/SKIP

    # L3: 기술 점수
    tech_signal: str = ""           # STRONG_BUY/BUY/WATCH/NO_ENTRY
    tech_score: float = 0           # 기술 종합 0~100
    ema_trend: str = ""             # BULLISH/BEARISH/SIDEWAYS
    rsi: float = 0                  # RSI 14
    obv_trend: str = ""             # UP/DOWN/FLAT
    hist_direction: str = ""        # BUY/SELL
    hist_strength: str = ""         # STRONG/NORMAL/WEAK

    # L4: 이상거래
    spike_score: float = 0          # 이상거래 점수 0~100
    spike_patterns: list = field(default_factory=list)

    # 매매 레벨
    close: float = 0
    atr_14: float = 0
    swing_sl: float = 0             # 손절 (1.5 ATR)
    swing_tp: float = 0             # 목표 (3.0 ATR, 2R)
    risk_pct: float = 0             # 손절 대비 리스크 %
    rr_ratio: float = 2.0           # Risk/Reward

    # PER/PBR
    per: float = 0
    pbr: float = 0
    val_warning: str = ""

    # 이벤트
    event_score: float = 0          # 이벤트 점수 0~100
    event_types: list = field(default_factory=list)  # TREASURY_BUY, THEME:반도체 등

    # 최종 점수
    final_score: float = 0
    source: str = ""                # SUPPLY/TECH/SPIKE/ALL

    def calc_final_score(self):
        """최종 점수 = 수급(35%) + 기술(35%) + 이벤트(15%) + 이상거래(15%)"""
        # 수급 점수 정규화 (3D + 4D + 5D 평균)
        supply_norm = (self.supply_score * 0.4 + self.momentum_score * 0.3 + self.energy_score * 0.3)
        self.final_score = round(
            supply_norm * 0.35 + self.tech_score * 0.35
            + self.event_score * 0.15 + self.spike_score * 0.15,
            1
        )
        # 소스 판정
        sources = []
        if supply_norm >= 60:
            sources.append("수급")
        if self.tech_score >= 60:
            sources.append("기술")
        if self.event_score >= 30:
            sources.append("이벤트")
        if self.spike_score >= 30:
            sources.append("이상")
        self.source = "+".join(sources) if sources else "-"

        # 리스크 %
        if self.close > 0 and self.swing_sl > 0:
            self.risk_pct = round((1 - self.swing_sl / self.close) * 100, 1)


# ═══════════════════════════════════════════════════
#  L2: 수급 분석
# ═══════════════════════════════════════════════════

def run_supply_analysis(codes: list, universe: dict) -> dict:
    """5D 수급 분석 실행

    Returns: {code: SwingCandidate}
    """
    from data.supply_analyzer import SupplyAnalyzer

    analyzer = SupplyAnalyzer()
    results = {}

    print(f"\n[L2] 수급 분석 - {len(codes)}종목...")

    fulls = analyzer.scan_all_full(codes)

    for f in fulls:
        code = f.score.code
        raw = universe.get(code, code)
        name = raw[0] if isinstance(raw, tuple) else (raw.get("name", code) if isinstance(raw, dict) else code)

        c = SwingCandidate(code=code, name=name)
        c.supply_grade = f.score.grade
        c.supply_score = f.score.total_score
        c.momentum_signal = f.momentum.signal
        c.momentum_score = f.momentum.momentum_score
        c.action = f.action
        c.per = f.per
        c.pbr = f.pbr
        c.val_warning = f.valuation_warning or ""

        if f.stability:
            c.energy_grade = f.stability.stability_grade
            c.energy_score = f.stability.stability_score

        results[code] = c

    # 수급 A 이상 or ACC 만 통과
    passed = {
        code: c for code, c in results.items()
        if c.action in ("STRONG_BUY", "BUY", "ENTER", "WATCH")
    }

    print(f"  전체: {len(results)} | 통과: {len(passed)} (STRONG_BUY~WATCH)")

    return passed


# ═══════════════════════════════════════════════════
#  L3: 기술 분석
# ═══════════════════════════════════════════════════

def run_tech_analysis(candidates: dict) -> dict:
    """스윙 기술 분석 (OBV → EMA → RSI → 히스토그램)

    Returns: {code: SwingCandidate} (OBV 비토된 종목 제외)
    """
    from data.swing_indicators import analyze_stock

    print(f"\n[L3] 기술 분석 - {len(candidates)}종목...")

    passed = {}
    vetoed = 0

    for code, cand in candidates.items():
        daily_file = DAILY_DIR / f"{code}.csv"
        if not daily_file.exists():
            continue

        try:
            df = pd.read_csv(daily_file, index_col=0, parse_dates=True)
            if len(df) < 65:
                continue

            result = analyze_stock(df)

            if result["signal"] == "NO_DATA":
                continue

            cand.tech_signal = result["signal"]
            cand.tech_score = result["score"]
            cand.ema_trend = result.get("ema_trend", "")
            cand.rsi = result.get("rsi", 0)
            cand.obv_trend = result.get("obv_trend", "")
            cand.hist_direction = result.get("histogram_direction", "")
            cand.hist_strength = result.get("histogram_strength", "")
            cand.close = result.get("close", 0)
            cand.atr_14 = result.get("atr_14", 0)
            cand.swing_sl = result.get("swing_sl", 0)
            cand.swing_tp = result.get("swing_tp", 0)

            # OBV 비토 → 제외
            if result["signal"] == "NO_ENTRY" and cand.obv_trend == "DOWN":
                vetoed += 1
                continue

            passed[code] = cand

        except Exception as e:
            logger.warning(f"  기술분석 실패 {code}: {e}")

    print(f"  통과: {len(passed)} | OBV 비토: {vetoed}")

    return passed


# ═══════════════════════════════════════════════════
#  L3.5: 이벤트 보너스 (DART + 뉴스)
# ═══════════════════════════════════════════════════

def apply_event_bonus(candidates: dict) -> dict:
    """이벤트 감지 결과 반영 (events.json)"""
    print(f"\n[L3.5] 이벤트 보너스 적용...")

    event_file = DATA_DIR / "events.json"
    if not event_file.exists():
        print("  events.json 없음 - 건너뜀 (이벤트 스캔 먼저 실행)")
        return candidates

    with open(event_file, "r", encoding="utf-8") as f:
        event_data = json.load(f)

    beneficiaries = {b["ticker"]: b for b in event_data.get("beneficiaries", [])}

    if not beneficiaries:
        print("  수혜주 0개")
        return candidates

    bonus_count = 0
    for code, cand in candidates.items():
        if code in beneficiaries:
            b = beneficiaries[code]
            # 이벤트 점수 정규화 (0~100 스케일)
            cand.event_score = min(100, b["total_score"])
            cand.event_types = b.get("events", [])
            bonus_count += 1

    # 이벤트 수혜주인데 수급 필터에 없는 종목도 추가
    added = 0
    for ticker, b in beneficiaries.items():
        if ticker not in candidates and b["direction"] == "POSITIVE" and b["total_score"] >= 60:
            # 기술 분석 실행
            daily_file = DAILY_DIR / f"{ticker}.csv"
            if not daily_file.exists():
                continue

            try:
                from data.swing_indicators import analyze_stock
                df = pd.read_csv(daily_file, index_col=0, parse_dates=True)
                if len(df) < 65:
                    continue
                result = analyze_stock(df)
                if result["signal"] in ("NO_ENTRY", "NO_DATA"):
                    continue

                cand = SwingCandidate(code=ticker, name=b["name"])
                cand.tech_signal = result["signal"]
                cand.tech_score = result["score"]
                cand.ema_trend = result.get("ema_trend", "")
                cand.rsi = result.get("rsi", 0)
                cand.obv_trend = result.get("obv_trend", "")
                cand.close = result.get("close", 0)
                cand.atr_14 = result.get("atr_14", 0)
                cand.swing_sl = result.get("swing_sl", 0)
                cand.swing_tp = result.get("swing_tp", 0)
                cand.event_score = min(100, b["total_score"])
                cand.event_types = b.get("events", [])
                candidates[ticker] = cand
                added += 1
            except Exception:
                pass

    print(f"  이벤트 보너스: {bonus_count}종목 | 이벤트 추가: {added}종목")
    return candidates


# ═══════════════════════════════════════════════════
#  L4: 이상거래 보너스
# ═══════════════════════════════════════════════════

def apply_spike_bonus(candidates: dict) -> dict:
    """이상거래 감지 결과 반영 (extra_universe.json + volume_spikes)"""
    print(f"\n[L4] 이상거래 보너스 적용...")

    # extra_universe.json 로드
    extra_file = DATA_DIR / "extra_universe.json"
    extra = {}
    if extra_file.exists():
        with open(extra_file, "r", encoding="utf-8") as f:
            extra = json.load(f)
        print(f"  조용한 매집 종목: {len(extra)}개")

    # 최신 volume_spikes 파일 로드
    spike_file = None
    if RESULT_DIR.exists():
        spike_files = sorted(RESULT_DIR.glob("volume_spikes_*.json"), reverse=True)
        if spike_files:
            spike_file = spike_files[0]

    spike_data = {}
    if spike_file:
        with open(spike_file, "r", encoding="utf-8") as f:
            spikes = json.load(f)
        spike_data = {s["code"]: s for s in spikes}
        print(f"  이상거래 결과: {len(spike_data)}개 ({spike_file.name})")

    # 보너스 적용
    bonus_count = 0
    for code, cand in candidates.items():
        if code in spike_data:
            s = spike_data[code]
            cand.spike_score = s["spike_score"]
            cand.spike_patterns = [p["type"] for p in s["patterns"]]
            bonus_count += 1
        elif code in extra:
            cand.spike_score = extra[code].get("spike_score", 30)
            cand.spike_patterns = extra[code].get("patterns", ["QUIET_ACCUMULATION"])
            bonus_count += 1

    print(f"  보너스 적용: {bonus_count}종목")

    # 이상거래 감지됐지만 기존 수급/기술에 없는 종목도 추가
    added = 0
    for code, info in extra.items():
        if code not in candidates:
            name = info.get("name", code)
            # 기술 분석 실행
            daily_file = DAILY_DIR / f"{code}.csv"
            if not daily_file.exists():
                continue

            try:
                from data.swing_indicators import analyze_stock
                df = pd.read_csv(daily_file, index_col=0, parse_dates=True)
                if len(df) < 65:
                    continue

                result = analyze_stock(df)
                if result["signal"] in ("NO_ENTRY", "NO_DATA"):
                    continue

                cand = SwingCandidate(code=code, name=name)
                cand.tech_signal = result["signal"]
                cand.tech_score = result["score"]
                cand.ema_trend = result.get("ema_trend", "")
                cand.rsi = result.get("rsi", 0)
                cand.obv_trend = result.get("obv_trend", "")
                cand.close = result.get("close", 0)
                cand.atr_14 = result.get("atr_14", 0)
                cand.swing_sl = result.get("swing_sl", 0)
                cand.swing_tp = result.get("swing_tp", 0)
                cand.spike_score = info.get("spike_score", 30)
                cand.spike_patterns = info.get("patterns", [])
                candidates[code] = cand
                added += 1
            except Exception:
                pass

    if added:
        print(f"  이상거래 추가: {added}종목 (수급 필터 바이패스)")

    return candidates


# ═══════════════════════════════════════════════════
#  최종 스코어링 & 출력
# ═══════════════════════════════════════════════════

def rank_candidates(candidates: dict, top_n: int = 10) -> List[SwingCandidate]:
    """최종 점수 산출 + 정렬"""
    for cand in candidates.values():
        cand.calc_final_score()

    ranked = sorted(candidates.values(), key=lambda c: -c.final_score)
    return ranked[:top_n]


def format_report(ranked: List[SwingCandidate], pos_mult: float = 1.0) -> str:
    """텔레그램/콘솔용 리포트 포맷"""
    from data.market_health import get_position_multiplier
    if pos_mult == 1.0:
        pos_mult = get_position_multiplier()

    lines = []
    lines.append(f"📊 스윙 스크리너 리포트")
    lines.append(f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M')}")

    # 건전성 배수 표시
    if pos_mult < 1.0:
        health_icon = "🚨" if pos_mult <= 0 else "⚠️"
        lines.append(f"{health_icon} 포지션 배수: {pos_mult*100:.0f}%")
    else:
        lines.append(f"✅ 시장 건전성 정상")
    lines.append(f"━━━━━━━━━━━━━━━━━━━")

    for i, c in enumerate(ranked, 1):
        # 수급 등급 아이콘
        supply_icon = {"STRONG_BUY": "🔥", "BUY": "💪", "ENTER": "👍", "WATCH": "👀"}.get(c.action, "-")
        # 기술 시그널 아이콘
        tech_icon = {"STRONG_BUY": "🟢", "BUY": "🟢", "WATCH": "🟡", "NO_ENTRY": "🔴"}.get(c.tech_signal, "⚪")
        # 이상거래 아이콘
        spike_icons = {
            "QUIET_ACCUMULATION": "🤫", "VOLUME_SPIKE": "📊",
            "OBV_BREAKOUT": "💰", "BIG_MONEY_INFLOW": "🐋",
            "MULTI_DAY_ACCUM": "📈",
        }
        spike_str = "".join(spike_icons.get(p, "") for p in c.spike_patterns)

        lines.append(f"\n{i}. {c.name}({c.code}) - {c.final_score:.0f}점")
        lines.append(f"   수급: {c.supply_grade}/{c.momentum_signal} {supply_icon} | 기술: {c.tech_signal} {tech_icon}")
        lines.append(f"   추세: {c.ema_trend} | RSI: {c.rsi:.0f} | OBV: {c.obv_trend}")

        if c.hist_direction:
            lines.append(f"   히스토그램: {c.hist_direction} ({c.hist_strength})")

        if c.event_types:
            evt_str = ", ".join(c.event_types[:3])
            lines.append(f"   이벤트: {evt_str} ({c.event_score:.0f}점)")

        if c.spike_patterns:
            lines.append(f"   이상거래: {spike_str} ({c.spike_score:.0f}점)")

        if c.swing_sl > 0:
            lines.append(f"   ▸ 진입: {c.close:,.0f}원 | SL: {c.swing_sl:,.0f}원({c.risk_pct:.1f}%) | TP: {c.swing_tp:,.0f}원")

        if c.val_warning:
            lines.append(f"   ⚠️ {c.val_warning} (PER:{c.per:.1f})")

        lines.append(f"   소스: [{c.source}]")

    lines.append(f"\n━━━━━━━━━━━━━━━━━━━")
    lines.append(f"🔥=수급강 🟢=기술BUY 🤫=매집 🐋=큰손")

    return "\n".join(lines)


def save_watchlist(ranked: List[SwingCandidate]) -> Path:
    """TOP 종목 watchlist.json 저장 (다음날 VWAP 모니터링용)"""
    RESULT_DIR.mkdir(parents=True, exist_ok=True)

    from data.market_health import get_position_multiplier
    pos_mult = get_position_multiplier()

    watchlist = []
    for c in ranked[:10]:
        watchlist.append({
            "code": c.code,
            "name": c.name,
            "final_score": c.final_score,
            "supply_grade": c.supply_grade,
            "momentum": c.momentum_signal,
            "action": c.action,
            "tech_signal": c.tech_signal,
            "tech_score": c.tech_score,
            "ema_trend": c.ema_trend,
            "rsi": round(c.rsi, 1),
            "obv_trend": c.obv_trend,
            "close": c.close,
            "swing_sl": c.swing_sl,
            "swing_tp": c.swing_tp,
            "risk_pct": c.risk_pct,
            "spike_patterns": c.spike_patterns,
            "source": c.source,
            "position_multiplier": pos_mult,
            "scanned_at": datetime.now().strftime("%Y-%m-%d %H:%M"),
        })

    path = DATA_DIR / "watchlist.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(watchlist, f, ensure_ascii=False, indent=2)

    # 날짜별 아카이브
    today = datetime.now().strftime("%Y%m%d")
    archive_path = RESULT_DIR / f"watchlist_{today}.json"
    with open(archive_path, "w", encoding="utf-8") as f:
        json.dump(watchlist, f, ensure_ascii=False, indent=2)

    print(f"\n  💾 watchlist.json 저장 ({len(watchlist)}종목)")
    return path


# ═══════════════════════════════════════════════════
#  개별 종목 분석
# ═══════════════════════════════════════════════════

def analyze_single(code: str) -> Optional[SwingCandidate]:
    """개별 종목 상세 분석"""
    from data.supply_analyzer import SupplyAnalyzer
    from data.swing_indicators import analyze_stock
    from data.universe_builder import load_universe

    universe = load_universe()
    raw = universe.get(code, {})
    name = raw.get("name", code) if isinstance(raw, dict) else (raw[0] if isinstance(raw, tuple) else code)

    cand = SwingCandidate(code=code, name=name)

    # 수급 분석
    analyzer = SupplyAnalyzer()
    full = analyzer.analyze_full(code)
    if full:
        cand.supply_grade = full.score.grade
        cand.supply_score = full.score.total_score
        cand.momentum_signal = full.momentum.signal
        cand.momentum_score = full.momentum.momentum_score
        cand.action = full.action
        cand.per = full.per
        cand.pbr = full.pbr
        if full.stability:
            cand.energy_grade = full.stability.stability_grade
            cand.energy_score = full.stability.stability_score

    # 기술 분석
    daily_file = DAILY_DIR / f"{code}.csv"
    if daily_file.exists():
        df = pd.read_csv(daily_file, index_col=0, parse_dates=True)
        result = analyze_stock(df)
        cand.tech_signal = result.get("signal", "")
        cand.tech_score = result.get("score", 0)
        cand.ema_trend = result.get("ema_trend", "")
        cand.rsi = result.get("rsi", 0)
        cand.obv_trend = result.get("obv_trend", "")
        cand.hist_direction = result.get("histogram_direction", "")
        cand.hist_strength = result.get("histogram_strength", "")
        cand.close = result.get("close", 0)
        cand.atr_14 = result.get("atr_14", 0)
        cand.swing_sl = result.get("swing_sl", 0)
        cand.swing_tp = result.get("swing_tp", 0)

        # 이상거래
        from data.volume_scanner import detect_patterns
        spike = detect_patterns(df, code, name)
        if spike["patterns"]:
            cand.spike_score = spike["spike_score"]
            cand.spike_patterns = [p["type"] for p in spike["patterns"]]

    cand.calc_final_score()
    return cand


def print_single(cand: SwingCandidate):
    """개별 종목 상세 출력"""
    print(f"\n{'='*50}")
    print(f"  {cand.name} ({cand.code}) - 스윙 분석")
    print(f"{'='*50}")
    print(f"\n  📊 최종 점수: {cand.final_score:.0f}점 [{cand.source}]")
    print(f"\n  ━━ 수급 (5D) ━━")
    print(f"  3D 등급: {cand.supply_grade} ({cand.supply_score:.0f}점)")
    print(f"  4D 모멘텀: {cand.momentum_signal} ({cand.momentum_score:.0f}점)")
    print(f"  5D 에너지: {cand.energy_grade} ({cand.energy_score:.0f}점)")
    print(f"  판정: {cand.action}")
    print(f"\n  ━━ 기술 ━━")
    print(f"  시그널: {cand.tech_signal} ({cand.tech_score:.0f}점)")
    print(f"  EMA 추세: {cand.ema_trend}")
    print(f"  RSI: {cand.rsi:.1f}")
    print(f"  OBV: {cand.obv_trend}")
    print(f"  히스토그램: {cand.hist_direction} ({cand.hist_strength})")
    print(f"\n  ━━ 매매 레벨 ━━")
    print(f"  종가: {cand.close:,.0f}원")
    print(f"  ATR(14): {cand.atr_14:,.0f}원")
    print(f"  손절(SL): {cand.swing_sl:,.0f}원 ({cand.risk_pct:.1f}%)")
    print(f"  목표(TP): {cand.swing_tp:,.0f}원 (2R)")
    if cand.spike_patterns:
        print(f"\n  ━━ 이상거래 ━━")
        print(f"  패턴: {', '.join(cand.spike_patterns)}")
        print(f"  점수: {cand.spike_score:.0f}")
    if cand.per > 0:
        print(f"\n  PER: {cand.per:.1f} | PBR: {cand.pbr:.2f}")
    if cand.val_warning:
        print(f"  ⚠️ {cand.val_warning}")
    print()


# ═══════════════════════════════════════════════════
#  메인 파이프라인
# ═══════════════════════════════════════════════════

def run_pipeline(top_n: int = 10) -> List[SwingCandidate]:
    """4층 파이프라인 실행"""
    from data.universe_builder import load_universe, get_universe_dict
    from data.market_health import get_position_multiplier

    print()
    print("═" * 60)
    print("  📊 스윙 스크리너 - 4층 파이프라인")
    print(f"  📅 {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("═" * 60)

    # L0: 시장 건전성 게이트
    pos_mult = get_position_multiplier()
    if pos_mult <= 0:
        print(f"\n🚨 시장 건전성 CRITICAL - 신규 진입 금지 (배수: {pos_mult})")
        print("  → 건전성 진단: python -m data.market_health")
        return []
    elif pos_mult < 1.0:
        print(f"\n⚠️ 시장 건전성 WARNING - 포지션 {pos_mult*100:.0f}% 축소 권장")
    else:
        print(f"\n✅ 시장 건전성 NORMAL - 풀사이즈 진입 가능")

    # L1: 유니버스
    universe = get_universe_dict()
    if not universe:
        print("  ❌ 유니버스 없음 - python -m data.universe_builder --build-only 실행")
        return []

    exclude = {'069500', '371160', '102780', '305720'}  # ETF
    codes = [c for c in universe.keys() if c not in exclude]
    print(f"\n[L1] 유니버스: {len(codes)}종목")

    # L2: 수급 분석
    candidates = run_supply_analysis(codes, universe)

    if not candidates:
        print("  ❌ 수급 통과 종목 없음")
        return []

    # L3: 기술 분석
    candidates = run_tech_analysis(candidates)

    if not candidates:
        print("  ❌ 기술 분석 통과 종목 없음")
        return []

    # L3.5: 이벤트 보너스 (DART + 뉴스)
    candidates = apply_event_bonus(candidates)

    # L4: 이상거래 보너스
    candidates = apply_spike_bonus(candidates)

    # 최종 랭킹
    ranked = rank_candidates(candidates, top_n)

    # 결과 출력
    print(f"\n{'='*60}")
    report = format_report(ranked, pos_mult)
    print(report)

    # 저장
    save_watchlist(ranked)

    print(f"\n{'='*60}")
    print(f"  ✅ 스윙 스크리너 완료 - TOP {len(ranked)}종목")
    print(f"{'='*60}")

    return ranked


# ═══════════════════════════════════════════════════
#  CLI
# ═══════════════════════════════════════════════════

if __name__ == "__main__":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
    logging.basicConfig(level=logging.WARNING)

    import argparse
    parser = argparse.ArgumentParser(description="스윙 스크리너")
    parser.add_argument("--top", type=int, default=10, help="상위 N개")
    parser.add_argument("--telegram", action="store_true", help="텔레그램 전송")
    parser.add_argument("--code", type=str, help="개별 종목 분석")
    args = parser.parse_args()

    if args.code:
        cand = analyze_single(args.code)
        if cand:
            print_single(cand)
    else:
        ranked = run_pipeline(top_n=args.top)

        if args.telegram and ranked:
            try:
                from bot.telegram_bot import send_message
                msg = format_report(ranked)
                send_message(msg)
                print("\n📨 텔레그램 전송 완료")
            except Exception as e:
                print(f"\n⚠️ 텔레그램 전송 실패: {e}")
