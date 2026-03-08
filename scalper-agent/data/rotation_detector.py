# -*- coding: utf-8 -*-
"""
섹터 로테이션 디텍터 (Sector Rotation Detector)
================================================
12개 섹터의 다일간 히스토리를 추적하여 로테이션 감지.

핵심 기능:
1. 섹터 히스토리 누적 (매일 저녁 scan 결과 저장)
2. 로테이션 페이즈 감지 (EARLY → MID → LATE → REVERSAL)
3. 스테이징 섹터 감지 (COLD→WARMING 전환 중인 섹터)
4. 다음 섹터 예측 (현재 HOT 뒤에 바톤 이어받을 후보)

로테이션 페이즈 정의:
  EARLY  - HOT 1~2일차, 리더만 급등, 소부장 미반응
  MID    - HOT 3~5일차, 소부장까지 확산, 거래량 유지
  LATE   - HOT 6일+, 브레드쓰 하락, 거래량 감소
  REVERSAL - 모멘텀 꺾임, 다음 섹터로 전환 시작

스테이징 섹터 조건:
  - 최근 3일 중 COLD→WARMING 전환 (상태 개선)
  - 또는 모멘텀 연속 2일 개선 (저점 탈출)
  - 브레드쓰 상승세 (상승 종목 비율 증가)
"""

import json
import logging
from pathlib import Path
from datetime import datetime, timedelta
from dataclasses import dataclass, field, asdict
from typing import Optional

logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data_store"
HISTORY_PATH = DATA_DIR / "sector_history.json"

# 로테이션 페이즈
PHASE_EARLY = "EARLY"        # 초기 (1~2일)
PHASE_MID = "MID"            # 중기 (3~5일)
PHASE_LATE = "LATE"          # 후기 (6일+)
PHASE_REVERSAL = "REVERSAL"  # 반전 (모멘텀 꺾임)
PHASE_STAGING = "STAGING"    # 스테이징 (다음 섹터 후보)
PHASE_DORMANT = "DORMANT"    # 비활성 (COLD/COOLING)


@dataclass
class SectorSnapshot:
    """하루치 섹터 스냅샷"""
    sector_id: str
    sector_name: str
    date: str
    status: str           # HOT/WARMING/RELAY/COOLING/COLD
    momentum_5d: float = 0.0
    vol_ratio: float = 0.0
    breadth: float = 0.0    # 상승 종목 비율
    leader_avg_5d: float = 0.0
    sobujan_avg_5d: float = 0.0
    relay_gap: float = 0.0


@dataclass
class RotationPhase:
    """섹터 로테이션 페이즈"""
    sector_id: str
    sector_name: str
    phase: str              # EARLY/MID/LATE/REVERSAL/STAGING/DORMANT
    hot_days: int = 0       # HOT/WARMING 연속일수
    momentum_trend: str = ""  # UP/FLAT/DOWN (최근 3일 모멘텀 추세)
    breadth_trend: str = ""   # UP/FLAT/DOWN (최근 3일 브레드쓰 추세)
    current_status: str = ""  # 오늘 섹터 상태
    current_momentum: float = 0.0
    current_breadth: float = 0.0
    signal: str = ""        # 로테이션 시그널 텍스트
    relay_candidates: list = field(default_factory=list)  # 릴레이 후보 종목


@dataclass
class RotationReport:
    """전체 로테이션 분석 결과"""
    date: str
    hot_sectors: list = field(default_factory=list)       # RotationPhase (현재 HOT)
    staging_sectors: list = field(default_factory=list)    # RotationPhase (다음 후보)
    cooling_sectors: list = field(default_factory=list)    # RotationPhase (쿨링 중)
    dormant_sectors: list = field(default_factory=list)    # RotationPhase (비활성)
    rotation_signal: str = ""  # 전체 로테이션 시그널 요약


# ═══════════════════════════════════════
#  섹터 히스토리 저장/로드
# ═══════════════════════════════════════

def load_history() -> dict:
    """sector_history.json 로드 - {date: {sector_id: snapshot_dict}}"""
    if HISTORY_PATH.exists():
        try:
            data = json.loads(HISTORY_PATH.read_text(encoding="utf-8"))
            return data
        except Exception as e:
            logger.warning(f"히스토리 로드 실패: {e}")
    return {}


def save_history(history: dict):
    """sector_history.json 저장 (최근 30일만 유지)"""
    # 30일 이전 데이터 정리
    cutoff = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
    cleaned = {d: v for d, v in history.items() if d >= cutoff}

    HISTORY_PATH.write_text(
        json.dumps(cleaned, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    logger.info(f"섹터 히스토리 저장: {len(cleaned)}일치 → {HISTORY_PATH}")


def record_today(sector_results: list) -> dict:
    """오늘의 섹터 스캔 결과를 히스토리에 추가

    Args:
        sector_results: scan_all_sectors()의 반환값 (list[SectorStatus])
    Returns:
        업데이트된 히스토리
    """
    today = datetime.now().strftime("%Y-%m-%d")
    history = load_history()

    today_data = {}
    for ss in sector_results:
        today_data[ss.sector_id] = {
            "sector_name": ss.sector_name,
            "status": ss.status,
            "momentum_5d": ss.momentum_5d,
            "vol_ratio": ss.vol_ratio,
            "breadth": ss.breadth,
            "leader_avg_5d": ss.leader_avg_5d,
            "sobujan_avg_5d": ss.sobujan_avg_5d,
            "relay_gap": ss.relay_gap,
        }

    history[today] = today_data
    save_history(history)
    return history


# ═══════════════════════════════════════
#  로테이션 분석 엔진
# ═══════════════════════════════════════

def _get_recent_days(history: dict, n: int = 5) -> list[str]:
    """최근 N일의 날짜 리스트 (내림차순 → 최신이 [0])"""
    dates = sorted(history.keys(), reverse=True)
    return dates[:n]


def _calc_trend(values: list[float]) -> str:
    """최근 값들로 추세 판단 (최신→과거 순서)
    values[0]=오늘, values[1]=어제, values[2]=그저께
    """
    if len(values) < 2:
        return "FLAT"
    # 최근 2일 기준
    diff = values[0] - values[1]
    if len(values) >= 3:
        diff2 = values[1] - values[2]
        avg_diff = (diff + diff2) / 2
    else:
        avg_diff = diff

    if avg_diff > 0.5:
        return "UP"
    elif avg_diff < -0.5:
        return "DOWN"
    return "FLAT"


def _count_hot_streak(history: dict, sector_id: str) -> int:
    """HOT/WARMING 연속일수 (오늘부터 거슬러 올라감)"""
    dates = sorted(history.keys(), reverse=True)
    streak = 0
    for d in dates:
        day_data = history[d].get(sector_id, {})
        status = day_data.get("status", "COLD")
        if status in ("HOT", "WARMING", "RELAY"):
            streak += 1
        else:
            break
    return streak


def _detect_phase(
    sector_id: str,
    sector_name: str,
    history: dict,
    hot_days: int,
    momentum_trend: str,
    breadth_trend: str,
    current: dict,
) -> RotationPhase:
    """개별 섹터의 로테이션 페이즈 결정"""
    status = current.get("status", "COLD")
    momentum = current.get("momentum_5d", 0)
    breadth = current.get("breadth", 0)

    phase = PHASE_DORMANT
    signal = ""

    if status in ("HOT", "WARMING"):
        if hot_days <= 2:
            phase = PHASE_EARLY
            signal = f"초기 진입({hot_days}일차) - 소부장 기회"
        elif hot_days <= 5:
            if breadth_trend == "DOWN" or momentum_trend == "DOWN":
                phase = PHASE_LATE
                signal = f"후반({hot_days}일차) - 모멘텀/확산 둔화, 진입 주의"
            else:
                phase = PHASE_MID
                signal = f"중기({hot_days}일차) - 확산 진행 중"
        else:
            if momentum_trend == "DOWN":
                phase = PHASE_REVERSAL
                signal = f"반전({hot_days}일차) - 매도 고려, 다음 섹터 주시"
            else:
                phase = PHASE_LATE
                signal = f"후반({hot_days}일차) - 장기 지속, 신규진입 비추"

    elif status == "RELAY":
        # 릴레이 = 아직 소부장 기회 있음
        phase = PHASE_MID
        signal = f"릴레이({hot_days}일차) - 소부장 캐치업 구간"

    elif status == "COOLING":
        if hot_days > 0:
            # 실제로 HOT이었다가 쿨링 → 진짜 반전
            phase = PHASE_REVERSAL
            signal = f"반전({hot_days}일 활성 후 쿨링) - 포지션 정리"
        else:
            # 한번도 HOT 아니었는데 모멘텀만 마이너스 → 비활성
            phase = PHASE_DORMANT
            signal = ""

    else:  # COLD
        # 스테이징 감지: 최근 3일간 모멘텀 개선 중인지 확인
        # 단, 폭락 섹터(-5% 이하)는 스테이징 제외 (아직 저점 탈출 안 함)
        if momentum > -5.0:
            dates = _get_recent_days(history, 3)
            if len(dates) >= 2:
                momentums = []
                for d in dates:
                    day_data = history[d].get(sector_id, {})
                    momentums.append(day_data.get("momentum_5d", 0))

                if len(momentums) >= 2 and momentums[0] > momentums[-1] + 1.0:
                    phase = PHASE_STAGING
                    signal = f"웜업 중 - 모멘텀 {momentums[-1]:+.1f}→{momentums[0]:+.1f}% 개선"
                elif momentum > -1.0 and breadth >= 0.4:
                    phase = PHASE_STAGING
                    signal = f"스테이징 - 모멘텀 {momentum:+.1f}%, 브레드쓰 {breadth:.0%}"

    return RotationPhase(
        sector_id=sector_id,
        sector_name=sector_name,
        phase=phase,
        hot_days=hot_days,
        momentum_trend=momentum_trend,
        breadth_trend=breadth_trend,
        current_status=status,
        current_momentum=momentum,
        current_breadth=breadth,
        signal=signal,
    )


def analyze_rotation(history: dict = None) -> RotationReport:
    """전체 섹터 로테이션 분석

    Args:
        history: 섹터 히스토리 dict (None이면 파일에서 로드)
    Returns:
        RotationReport
    """
    if history is None:
        history = load_history()

    today = datetime.now().strftime("%Y-%m-%d")
    dates = _get_recent_days(history, 5)

    if not dates:
        return RotationReport(date=today, rotation_signal="히스토리 없음 - 첫 스캔 필요")

    latest_date = dates[0]
    today_data = history.get(latest_date, {})

    from data.sector_relay import SECTORS

    report = RotationReport(date=latest_date)

    for sector_id, sector_def in SECTORS.items():
        sector_name = sector_def["name"]
        current = today_data.get(sector_id, {})

        if not current:
            continue

        # 연속 HOT 일수
        hot_days = _count_hot_streak(history, sector_id)

        # 최근 3일 모멘텀/브레드쓰 추세
        momentums = []
        breadths = []
        for d in dates[:3]:
            day_data = history[d].get(sector_id, {})
            momentums.append(day_data.get("momentum_5d", 0))
            breadths.append(day_data.get("breadth", 0))

        momentum_trend = _calc_trend(momentums)
        breadth_trend = _calc_trend(breadths)

        # 페이즈 결정
        phase = _detect_phase(
            sector_id, sector_name, history,
            hot_days, momentum_trend, breadth_trend, current,
        )

        # 카테고리별 분류
        if phase.phase in (PHASE_EARLY, PHASE_MID):
            report.hot_sectors.append(phase)
        elif phase.phase == PHASE_STAGING:
            report.staging_sectors.append(phase)
        elif phase.phase in (PHASE_LATE, PHASE_REVERSAL):
            report.cooling_sectors.append(phase)
        else:
            report.dormant_sectors.append(phase)

    # 전체 로테이션 시그널 생성
    signals = []
    for s in report.hot_sectors:
        signals.append(f"HOT: {s.sector_name}({s.phase} {s.hot_days}D)")
    for s in report.staging_sectors:
        signals.append(f"NEXT: {s.sector_name}(스테이징)")
    for s in report.cooling_sectors:
        if s.phase == PHASE_REVERSAL:
            signals.append(f"EXIT: {s.sector_name}(반전)")

    report.rotation_signal = " | ".join(signals) if signals else "활성 로테이션 없음"

    # 정렬: hot_sectors는 hot_days 오름차순 (진입 초기가 먼저)
    report.hot_sectors.sort(key=lambda x: x.hot_days)
    # staging은 모멘텀 기준 내림차순
    report.staging_sectors.sort(key=lambda x: x.current_momentum, reverse=True)

    return report


def get_next_sector_stocks(rotation: RotationReport = None) -> dict:
    """로테이션 분석 → 다음 섹터의 투자 대상 종목 추출

    Returns:
        {code: {"name": str, "sector": str, "phase": str, "tier": str, "signal": str}}
    """
    if rotation is None:
        rotation = analyze_rotation()

    from data.sector_relay import SECTORS

    next_stocks = {}

    # 1) HOT EARLY/MID 섹터의 소부장 (아직 안 오른 종목)
    for phase in rotation.hot_sectors:
        if phase.phase in (PHASE_EARLY, PHASE_MID):
            sector_def = SECTORS.get(phase.sector_id, {})
            # 소부장 → mid → leaders 순서 (캐치업 기대)
            for tier_name, tier_key in [("sobujan", "sobujan"), ("mid", "mid")]:
                for code, name in sector_def.get(tier_key, []):
                    next_stocks[code] = {
                        "name": name,
                        "sector": phase.sector_name,
                        "sector_id": phase.sector_id,
                        "phase": phase.phase,
                        "tier": tier_name,
                        "hot_days": phase.hot_days,
                        "signal": phase.signal,
                        "rotation_source": f"hot_{phase.phase.lower()}",
                    }

    # 2) STAGING 섹터의 리더 + mid (웜업 시작 종목)
    for phase in rotation.staging_sectors:
        sector_def = SECTORS.get(phase.sector_id, {})
        for tier_name, tier_key in [("leader", "leaders"), ("mid", "mid")]:
            for code, name in sector_def.get(tier_key, []):
                if code not in next_stocks:
                    next_stocks[code] = {
                        "name": name,
                        "sector": phase.sector_name,
                        "sector_id": phase.sector_id,
                        "phase": PHASE_STAGING,
                        "tier": tier_name,
                        "hot_days": 0,
                        "signal": phase.signal,
                        "rotation_source": "staging",
                    }

    # 3) REVERSAL 섹터 → 제외 마킹 (기존 포지션 정리 대상)
    # 별도 dict가 아니라 rotation_source에 "reversal"로 표시
    for phase in rotation.cooling_sectors:
        if phase.phase == PHASE_REVERSAL:
            sector_def = SECTORS.get(phase.sector_id, {})
            for tier_key in ["leaders", "mid", "sobujan"]:
                for code, name in sector_def.get(tier_key, []):
                    if code in next_stocks:
                        # 이미 다른 소스로 들어있으면 스킵
                        continue
                    next_stocks[code] = {
                        "name": name,
                        "sector": phase.sector_name,
                        "sector_id": phase.sector_id,
                        "phase": PHASE_REVERSAL,
                        "tier": tier_key.replace("leaders", "leader"),
                        "hot_days": phase.hot_days,
                        "signal": phase.signal,
                        "rotation_source": "reversal_exit",
                    }

    return next_stocks


def format_rotation_report(report: RotationReport) -> str:
    """텔레그램용 로테이션 리포트"""
    phase_emoji = {
        PHASE_EARLY: "🟢",
        PHASE_MID: "🟡",
        PHASE_LATE: "🟠",
        PHASE_REVERSAL: "🔴",
        PHASE_STAGING: "🔵",
        PHASE_DORMANT: "⬜",
    }

    lines = [
        "🔄 섹터 로테이션 분석",
        f"📅 {report.date}",
        "",
        f"💡 {report.rotation_signal}",
        "",
    ]

    if report.hot_sectors:
        lines.append("── 🔥 활성 섹터 ──")
        for s in report.hot_sectors:
            emoji = phase_emoji.get(s.phase, "⬜")
            lines.append(
                f"{emoji} {s.sector_name} [{s.phase}] {s.hot_days}일차"
                f"  {s.current_momentum:+.1f}%"
            )
            lines.append(f"   📊 모멘텀{s.momentum_trend} 확산{s.breadth_trend}")
            if s.signal:
                lines.append(f"   → {s.signal}")
        lines.append("")

    if report.staging_sectors:
        lines.append("── 🔵 다음 섹터 후보 ──")
        for s in report.staging_sectors:
            lines.append(
                f"🔵 {s.sector_name} [{s.phase}]"
                f"  {s.current_momentum:+.1f}% 확산:{s.current_breadth:.0%}"
            )
            if s.signal:
                lines.append(f"   → {s.signal}")
        lines.append("")

    if report.cooling_sectors:
        lines.append("── 🟠 쿨링/반전 ──")
        for s in report.cooling_sectors:
            emoji = phase_emoji.get(s.phase, "⬜")
            lines.append(
                f"{emoji} {s.sector_name} [{s.phase}] {s.hot_days}D"
                f"  {s.current_momentum:+.1f}%"
            )
            if s.signal:
                lines.append(f"   → {s.signal}")
        lines.append("")

    # 비활성 섹터 한줄 요약
    if report.dormant_sectors:
        dormant_names = [s.sector_name for s in report.dormant_sectors]
        lines.append(f"⬜ 비활성: {', '.join(dormant_names)}")

    return "\n".join(lines)


# ═══════════════════════════════════════
#  CLI 테스트
# ═══════════════════════════════════════

if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO)

    if "--scan" in sys.argv:
        # 섹터 스캔 + 히스토리 저장 + 로테이션 분석
        print("섹터 스캔 + 히스토리 저장...")
        from data.sector_relay import scan_all_sectors
        sectors = scan_all_sectors()
        history = record_today(sectors)
        print(f"히스토리: {len(history)}일치 저장됨")

        print("\n로테이션 분석...")
        rotation = analyze_rotation(history)
        print(format_rotation_report(rotation))

        print("\n다음 섹터 종목:")
        next_stocks = get_next_sector_stocks(rotation)
        for code, info in next_stocks.items():
            print(f"  {info['name']}({code}) [{info['sector']}] {info['phase']} {info['tier']} - {info['signal']}")
    else:
        # 기존 히스토리로 분석만
        print("로테이션 분석 (기존 히스토리)...")
        rotation = analyze_rotation()
        print(format_rotation_report(rotation))

        next_stocks = get_next_sector_stocks(rotation)
        if next_stocks:
            print(f"\n다음 섹터 종목: {len(next_stocks)}개")
            for code, info in list(next_stocks.items())[:10]:
                src = info['rotation_source']
                print(f"  {info['name']}({code}) [{info['sector']}] {src}/{info['tier']}")
