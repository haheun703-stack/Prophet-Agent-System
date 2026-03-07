# -*- coding: utf-8 -*-
"""
JARVIS BRAIN — 자본 배분 엔진 (Phase 1: 지시서만)
═══════════════════════════════════════════════════
NIGHTWATCH 매크로 레짐 판정 → 전략별 자본 배분 비율 계산 → JSON 저장

실매매 실행 안 함. 각 전략봇(v10.3, 그룹ETF, 단타봇)이
brain_allocation.json을 읽어서 자기 배분 한도를 조절하는 구조.

사용:
  python -m jarvis.brain                    # NIGHTWATCH 연동 + 배분 계산
  python -m jarvis.brain --regime PANIC     # 수동 레짐 지정
  python -m jarvis.brain --capital 5000000  # 총 자금 지정
"""

import json
import logging
import sys
from pathlib import Path
from datetime import datetime, date
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, asdict

logger = logging.getLogger("jarvis.brain")

# ── 경로 ─────────────────────────────────────────
ROOT = Path(__file__).resolve().parent
PROJECT_ROOT = ROOT.parent
CONFIG_DIR = ROOT / "config"
DATA_DIR = ROOT / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

ALLOCATION_TABLE_PATH = CONFIG_DIR / "allocation_table.json"
ALLOCATION_PATH = DATA_DIR / "brain_allocation.json"
HISTORY_PATH = DATA_DIR / "brain_history.json"

# scalper-agent NIGHTWATCH 경로
NIGHTWATCH_REPORT_PATH = PROJECT_ROOT / "scalper-agent" / "data_store" / "nightwatch_report.json"

# ── 전략 키 (배분 대상) ──────────────────────────
STRATEGY_KEYS = [
    "v10_swing",     # v10.3 84종목 스윙
    "group_etf",     # 그룹ETF 순환매 (현대차3+삼성4)
    "gold_etf",      # 132030 KODEX 골드선물
    "inverse_etf",   # 252670 KODEX 200선물인버스2X
    "small_cap",     # 단타봇/Prophet Agent 소형주
    "cash",          # 현금 보유
]

STRATEGY_LABELS = {
    "v10_swing":    "v10.3 스윙",
    "group_etf":    "그룹ETF",
    "gold_etf":     "금 ETF",
    "inverse_etf":  "인버스",
    "small_cap":    "소형주",
    "cash":         "현금",
}

# ── NIGHTWATCH 점수 → BRAIN 레짐 매핑 ────────────
# NIGHTWATCH total_score: -10 ~ +10
# BRAIN regime: 7단계
REGIME_THRESHOLDS = [
    # (하한, 상한, 레짐코드)  — 순서 중요: 먼저 매칭되면 리턴
    (-10.0, -7.0,  "PANIC"),
    (-7.0,  -5.0,  "FEAR_EXTREME"),
    (-5.0,  -2.0,  "RISK_OFF"),
    (-2.0,  +2.0,  "CAUTIOUS"),
    (+2.0,  +5.0,  "NEUTRAL"),
    (+5.0,  +7.0,  "RISK_ON"),
    (+7.0,  +10.1, "EUPHORIA"),
]


# ═══════════════════════════════════════════════════
#  배분 테이블 로드
# ═══════════════════════════════════════════════════

def load_allocation_table() -> Dict:
    """allocation_table.json 로드 (외부 파일 → 튜닝 용이)"""
    if not ALLOCATION_TABLE_PATH.exists():
        raise FileNotFoundError(f"배분 테이블 없음: {ALLOCATION_TABLE_PATH}")
    with open(ALLOCATION_TABLE_PATH, "r", encoding="utf-8") as f:
        table = json.load(f)
    # _comment 등 메타 키 제거
    return {k: v for k, v in table.items() if not k.startswith("_")}


def _validate_table(table: Dict):
    """배분 테이블 검증: 각 레짐의 합이 100%인지 확인"""
    for regime, alloc in table.items():
        total = sum(alloc.get(k, 0) for k in STRATEGY_KEYS)
        assert total == 100, (
            f"배분 합계 오류: {regime} = {total}% (100% 아님). "
            f"allocation_table.json 확인 필요."
        )


# ═══════════════════════════════════════════════════
#  NIGHTWATCH → 레짐 매핑
# ═══════════════════════════════════════════════════

def nightwatch_score_to_regime(score: float) -> str:
    """NIGHTWATCH total_score → BRAIN 레짐 코드

    매핑:
      ≤-7  PANIC | -7~-5 FEAR_EXTREME | -5~-2 RISK_OFF
      -2~+2 CAUTIOUS | +2~+5 NEUTRAL | +5~+7 RISK_ON | ≥+7 EUPHORIA
    """
    score = max(-10.0, min(10.0, score))
    for low, high, regime in REGIME_THRESHOLDS:
        if low <= score < high:
            return regime
    return "CAUTIOUS"  # fallback


def load_nightwatch_regime() -> Tuple[str, float, str]:
    """NIGHTWATCH 리포트에서 레짐 판정

    Returns: (regime_code, nightwatch_score, signal_text)
    """
    if not NIGHTWATCH_REPORT_PATH.exists():
        logger.warning(f"NIGHTWATCH 리포트 없음: {NIGHTWATCH_REPORT_PATH}")
        return "CAUTIOUS", 0.0, "리포트없음"

    try:
        with open(NIGHTWATCH_REPORT_PATH, "r", encoding="utf-8") as f:
            report = json.load(f)
        score = report.get("total_score", 0.0)
        signal_text = report.get("signal_text", "")
        regime = nightwatch_score_to_regime(score)
        return regime, score, signal_text
    except Exception as e:
        logger.error(f"NIGHTWATCH 리포트 파싱 실패: {e}")
        return "CAUTIOUS", 0.0, f"파싱에러:{e}"


# ═══════════════════════════════════════════════════
#  안전장치
# ═══════════════════════════════════════════════════

MAX_DAILY_CHANGE = 20  # 하루 최대 배분 변경폭 (±20%p)
WHIPSAW_CONFIRM_DAYS = 2  # 레짐 변경 확인 일수


def load_history() -> List[Dict]:
    """brain_history.json 로드"""
    if not HISTORY_PATH.exists():
        return []
    try:
        with open(HISTORY_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return []


def save_history(history: List[Dict]):
    """brain_history.json 저장 (최근 90일만 유지)"""
    # 최근 90개만 유지
    history = history[-90:]
    with open(HISTORY_PATH, "w", encoding="utf-8") as f:
        json.dump(history, f, ensure_ascii=False, indent=2)


def check_whipsaw(new_regime: str, history: List[Dict]) -> Tuple[bool, str]:
    """whipsaw 방지: 2일 연속 같은 레짐이어야 전환 허용

    Returns: (should_switch, reason)
    """
    if not history:
        return True, "첫 실행"

    last = history[-1]
    last_regime = last.get("regime", "")
    last_effective = last.get("effective_regime", "")

    # 이미 같은 레짐이면 유지
    if new_regime == last_effective:
        return True, "레짐 유지"

    # 새 레짐이 2일 연속인지 확인
    recent_regimes = [h.get("regime") for h in history[-WHIPSAW_CONFIRM_DAYS:]]
    if len(recent_regimes) >= WHIPSAW_CONFIRM_DAYS and all(r == new_regime for r in recent_regimes):
        return True, f"{WHIPSAW_CONFIRM_DAYS}일 연속 확인 → 전환"

    # PANIC은 예외: 즉시 전환 (위기 대응은 늦으면 안 됨)
    if new_regime == "PANIC":
        return True, "PANIC 즉시 전환"

    return False, f"whipsaw 방지: {new_regime} 1일차 (2일 확인 필요)"


def apply_rate_limit(
    new_alloc: Dict[str, int],
    prev_alloc: Dict[str, int],
) -> Dict[str, int]:
    """하루 최대 ±20%p 변경 제한

    제한 적용 후 합계가 100%가 되도록 현금으로 조정
    """
    if not prev_alloc:
        return new_alloc

    limited = {}
    for key in STRATEGY_KEYS:
        if key == "cash":
            continue
        new_val = new_alloc.get(key, 0)
        prev_val = prev_alloc.get(key, 0)
        diff = new_val - prev_val
        if abs(diff) > MAX_DAILY_CHANGE:
            limited[key] = prev_val + (MAX_DAILY_CHANGE if diff > 0 else -MAX_DAILY_CHANGE)
        else:
            limited[key] = new_val

    # 현금으로 100% 맞추기
    non_cash = sum(limited.get(k, 0) for k in STRATEGY_KEYS if k != "cash")
    limited["cash"] = 100 - non_cash
    # 현금이 음수면 비례 축소
    if limited["cash"] < 0:
        excess = -limited["cash"]
        for key in STRATEGY_KEYS:
            if key != "cash" and limited[key] > 0:
                reduction = min(limited[key], excess * limited[key] / non_cash)
                limited[key] -= int(reduction)
        limited["cash"] = 100 - sum(limited.get(k, 0) for k in STRATEGY_KEYS if k != "cash")

    return limited


# ═══════════════════════════════════════════════════
#  핵심: 자본 배분 계산
# ═══════════════════════════════════════════════════

def get_capital_allocation(
    regime_code: str,
    total_capital: int,
    apply_safety: bool = True,
) -> Dict:
    """레짐 코드 → 전략별 배분 금액 반환

    Args:
        regime_code: "PANIC"~"EUPHORIA"
        total_capital: 총 운용자금 (원)
        apply_safety: whipsaw/rate limit 적용 여부

    Returns:
        {
            "regime": str,
            "effective_regime": str,  # whipsaw 적용 후 실제 레짐
            "total_capital": int,
            "allocation_pct": {key: pct, ...},
            "allocation_krw": {key: 금액, ...},
            "safety_note": str,
            "timestamp": str,
        }
    """
    table = load_allocation_table()
    _validate_table(table)

    if regime_code not in table:
        logger.warning(f"알 수 없는 레짐: {regime_code} → CAUTIOUS 대체")
        regime_code = "CAUTIOUS"

    # 안전장치 적용
    history = load_history()
    effective_regime = regime_code
    safety_note = ""

    if apply_safety:
        should_switch, reason = check_whipsaw(regime_code, history)
        if not should_switch:
            # 이전 effective_regime 유지
            effective_regime = history[-1].get("effective_regime", "CAUTIOUS") if history else "CAUTIOUS"
            safety_note = reason
            logger.info(f"[BRAIN] whipsaw 방지: {regime_code} → {effective_regime} 유지 ({reason})")
        else:
            safety_note = reason

    # 배분 비율 가져오기
    alloc_pct = {k: table[effective_regime].get(k, 0) for k in STRATEGY_KEYS}

    # rate limit 적용
    if apply_safety and history:
        prev_pct = history[-1].get("allocation_pct", {})
        if prev_pct and effective_regime != history[-1].get("effective_regime"):
            alloc_pct = apply_rate_limit(alloc_pct, prev_pct)
            if alloc_pct != {k: table[effective_regime].get(k, 0) for k in STRATEGY_KEYS}:
                safety_note += " | rate limit 적용"

    # 금액 계산
    alloc_krw = {}
    for key in STRATEGY_KEYS:
        pct = alloc_pct.get(key, 0)
        alloc_krw[key] = int(total_capital * pct / 100)

    result = {
        "regime": regime_code,
        "effective_regime": effective_regime,
        "severity": table[effective_regime].get("severity", 0),
        "description": table[effective_regime].get("description", ""),
        "total_capital": total_capital,
        "allocation_pct": alloc_pct,
        "allocation_krw": alloc_krw,
        "safety_note": safety_note,
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }

    return result


# ═══════════════════════════════════════════════════
#  저장
# ═══════════════════════════════════════════════════

def save_allocation(result: Dict):
    """brain_allocation.json 저장 (다른 봇이 읽는 지시서)"""
    with open(ALLOCATION_PATH, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    logger.info(f"[BRAIN] 배분 저장: {ALLOCATION_PATH}")


def append_history(result: Dict):
    """brain_history.json에 이력 추가"""
    history = load_history()
    entry = {
        "date": date.today().isoformat(),
        "regime": result["regime"],
        "effective_regime": result["effective_regime"],
        "severity": result["severity"],
        "allocation_pct": result["allocation_pct"],
        "safety_note": result["safety_note"],
        "timestamp": result["timestamp"],
    }
    history.append(entry)
    save_history(history)
    logger.info(f"[BRAIN] 이력 추가: {len(history)}건")


# ═══════════════════════════════════════════════════
#  텔레그램 브리핑 포맷
# ═══════════════════════════════════════════════════

def format_allocation_report(
    result: Dict,
    nightwatch_score: float = 0.0,
    nightwatch_signal: str = "",
) -> str:
    """텔레그램용 배분 현황 블록"""
    pct = result["allocation_pct"]
    krw = result["allocation_krw"]
    regime = result["effective_regime"]
    desc = result["description"]
    severity = result["severity"]

    # 평시 기준 (NEUTRAL)
    table = load_allocation_table()
    neutral_pct = {k: table.get("NEUTRAL", {}).get(k, 0) for k in STRATEGY_KEYS}

    def _bar(p: int) -> str:
        filled = max(0, p // 10)
        return chr(0x2588) * filled + chr(0x2591) * (10 - filled)

    def _arrow(curr: int, normal: int) -> str:
        if curr > normal + 5:
            return "UP"
        elif curr < normal - 5:
            return "DN"
        elif curr == 0 and normal > 0:
            return "X"
        elif curr > 0 and normal == 0:
            return "NEW"
        else:
            return " "

    lines = [
        "",
        "BRAIN 자본 배분 지시",
        "=" * 28,
    ]

    if result["regime"] != result["effective_regime"]:
        lines.append(
            f"!! 판정: {result['regime']} -> "
            f"whipsaw 방지 -> {regime} 유지"
        )
        lines.append("")

    for key in STRATEGY_KEYS:
        p = pct.get(key, 0)
        n = neutral_pct.get(key, 0)
        label = STRATEGY_LABELS.get(key, key)
        arrow = _arrow(p, n)
        amount = krw.get(key, 0)
        if amount >= 10000:
            amt_str = f"{amount/10000:.0f}만"
        else:
            amt_str = f"{amount:,}"
        lines.append(
            f"{label:<8} {_bar(p)} {p:>3}% {arrow:>3} ({amt_str}원)"
        )

    lines.extend([
        "=" * 28,
        f"레짐: {regime} (severity {severity})",
        f"설명: {desc}",
    ])

    if result.get("safety_note"):
        lines.append(f"안전: {result['safety_note']}")

    return "\n".join(lines)


# ═══════════════════════════════════════════════════
#  메인 실행
# ═══════════════════════════════════════════════════

def _load_rotation_info() -> Dict:
    """섹터 로테이션 분석 결과 로드 (추천 파이프라인에서 저장한 것)"""
    try:
        import sys
        scalper_path = str(PROJECT_ROOT / "scalper-agent")
        if scalper_path not in sys.path:
            sys.path.insert(0, scalper_path)
        from data.rotation_detector import analyze_rotation, format_rotation_report
        rotation = analyze_rotation()
        return {
            "rotation_signal": rotation.rotation_signal,
            "hot_sectors": [
                {"sector": s.sector_name, "phase": s.phase,
                 "hot_days": s.hot_days, "signal": s.signal}
                for s in rotation.hot_sectors
            ],
            "staging_sectors": [
                {"sector": s.sector_name, "momentum": s.current_momentum,
                 "breadth": s.current_breadth, "signal": s.signal}
                for s in rotation.staging_sectors
            ],
            "cooling_sectors": [
                {"sector": s.sector_name, "phase": s.phase,
                 "hot_days": s.hot_days, "signal": s.signal}
                for s in rotation.cooling_sectors
            ],
            "telegram_block": format_rotation_report(rotation),
        }
    except Exception as e:
        logger.warning(f"[BRAIN] 로테이션 정보 로드 실패: {e}")
        return {}


def run_brain(
    regime_override: str = None,
    total_capital: int = 1_150_000,
) -> Dict:
    """BRAIN 전체 실행

    1. NIGHTWATCH 점수 읽기 (또는 수동 레짐)
    2. 레짐 판정
    3. 자본 배분 계산 (안전장치 포함)
    4. 섹터 로테이션 분석 로드
    5. JSON 저장
    6. 텔레그램 메시지 생성

    Args:
        regime_override: 수동 레짐 ("PANIC" 등, None이면 NIGHTWATCH 자동)
        total_capital: 총 운용자금

    Returns:
        배분 결과 dict
    """
    logger.info("[BRAIN] 실행 시작")

    # 1. 레짐 판정
    if regime_override:
        regime = regime_override.upper()
        nw_score = 0.0
        nw_signal = f"수동:{regime}"
        logger.info(f"[BRAIN] 수동 레짐: {regime}")
    else:
        regime, nw_score, nw_signal = load_nightwatch_regime()
        logger.info(f"[BRAIN] NIGHTWATCH: {nw_score:+.1f} ({nw_signal}) → {regime}")

    # 2. 자본 배분 계산
    result = get_capital_allocation(regime, total_capital)

    # 3. 섹터 로테이션 분석 로드
    rotation_info = _load_rotation_info()
    result["rotation"] = rotation_info

    # 4. 저장
    save_allocation(result)
    append_history(result)

    # 5. 텔레그램 메시지 (배분 + 로테이션)
    telegram_msg = format_allocation_report(result, nw_score, nw_signal)
    if rotation_info.get("rotation_signal"):
        telegram_msg += "\n\n" + rotation_info.get("telegram_block", "")
    result["telegram_message"] = telegram_msg

    logger.info(
        f"[BRAIN] 완료: {result['effective_regime']} | "
        f"스윙 {result['allocation_pct']['v10_swing']}% "
        f"금 {result['allocation_pct']['gold_etf']}% "
        f"인버스 {result['allocation_pct']['inverse_etf']}% "
        f"현금 {result['allocation_pct']['cash']}%"
    )
    if rotation_info.get("rotation_signal"):
        logger.info(f"[BRAIN] 로테이션: {rotation_info['rotation_signal']}")

    return result


# ═══════════════════════════════════════════════════
#  CLI
# ═══════════════════════════════════════════════════

if __name__ == "__main__":
    import argparse
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", line_buffering=True)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(message)s",
        datefmt="%H:%M:%S",
    )

    parser = argparse.ArgumentParser(description="JARVIS BRAIN 자본 배분 엔진")
    parser.add_argument("--regime", default=None,
                        choices=["PANIC", "FEAR_EXTREME", "RISK_OFF", "CAUTIOUS",
                                 "NEUTRAL", "RISK_ON", "EUPHORIA"],
                        help="수동 레짐 (생략 시 NIGHTWATCH 자동)")
    parser.add_argument("--capital", type=int, default=1_150_000,
                        help="총 운용자금 (원)")
    parser.add_argument("--no-safety", action="store_true",
                        help="안전장치 비활성화 (whipsaw/rate limit)")
    args = parser.parse_args()

    if args.no_safety:
        # 안전장치 없이 직접 계산
        if args.regime:
            regime = args.regime
        else:
            regime, _, _ = load_nightwatch_regime()
        result = get_capital_allocation(regime, args.capital, apply_safety=False)
        save_allocation(result)
        msg = format_allocation_report(result)
        print(msg)
    else:
        result = run_brain(
            regime_override=args.regime,
            total_capital=args.capital,
        )
        print(result["telegram_message"])
