# -*- coding: utf-8 -*-
"""
보유 종목 보호 정책 (manual_sync 면제)

목적:
    수동 동기화로 등록된 종목(예: 5/16 잔고 동기화의 5종목)을
    AI 점수 청산 / Eye+Guardian EXIT / 트레일링 동적 조정 / EXIT_ONLY 모드 등
    "회복 시나리오를 무시하는 자동 청산 경로"에서 면제하기 위한 공통 인터페이스.

사용 패턴:
    from bot.position_protection import is_protected_position

    if is_protected_position(code):
        return  # 사장님 "회복 시나리오 신뢰" 의지 보존

배경:
    - 5/16 5종목 잔고 동기화 시 SL -25% / TP +10% 결정
    - 평균 -13.4% 손실 상태에서 회복 기다림 의지
    - 정보봇 P0-8 (DANGER 신규 차단)은 매수만 영향 → 5종목 무관
    - 향후 정보봇이 트레일링/EXIT_ONLY 추가 시 이 함수로 자동 면제

관련 면제 가드 (이미 적용됨):
    - data/realtime_monitor.py _decide(): AI 점수 청산 면제 (commit 2430e1f)
    - bot/auto_trader.py _eye_guardian_action(): EXIT/REDUCE 면제 (commit a2c592d)

향후 면제 필요 경로 (정보봇 가이드 추가 적용 시):
    - 가이드 3: 트레일링 스탑 동적 조정 (DANGER 시 -0.5%)
    - 가이드 5: 일일 시작 시 EXIT_ONLY 모드
"""
import json
import logging
from pathlib import Path
from typing import Optional, Set, Tuple

logger = logging.getLogger(__name__)

POSITIONS_PATH = Path(__file__).resolve().parent.parent / "data_store" / "positions.json"

# 보호 정책 prefix — 이걸로 시작하는 source는 자동 청산 면제
PROTECTED_SOURCE_PREFIXES: Tuple[str, ...] = (
    "manual_sync",     # 잔고 수동 동기화 (회복 시나리오 신뢰)
)


def is_protected_position(code: str) -> bool:
    """code가 수동 동기화 보호 종목인지 판정.

    Args:
        code: 종목코드 (6자리)

    Returns:
        True = 자동 청산 면제 대상 (SL/TP 외 모든 청산 트리거 우회)
        False = 일반 종목 (기존 청산 로직 정상 적용)

    Examples:
        >>> # 트레일링 SL 동적 조정 적용 시
        >>> if is_protected_position(code):
        ...     continue  # 트레일링 면제
        >>> # 일반 종목은 트레일링 적용
    """
    if not POSITIONS_PATH.exists():
        return False
    try:
        positions = json.loads(POSITIONS_PATH.read_text("utf-8"))
    except (json.JSONDecodeError, OSError) as e:
        logger.warning("[position_protection] positions.json 로드 실패: %s", e)
        return False

    pos = positions.get(code)
    if not pos:
        return False

    source = pos.get("source", "")
    return source.startswith(PROTECTED_SOURCE_PREFIXES)


def get_protected_codes() -> Set[str]:
    """현재 보호 대상 종목코드 전체 집합 반환.

    사용:
        protected = get_protected_codes()
        for code in all_positions:
            if code in protected:
                continue  # 면제
    """
    if not POSITIONS_PATH.exists():
        return set()
    try:
        positions = json.loads(POSITIONS_PATH.read_text("utf-8"))
    except (json.JSONDecodeError, OSError) as e:
        logger.warning("[position_protection] positions.json 로드 실패: %s", e)
        return set()

    return {
        code for code, pos in positions.items()
        if pos.get("source", "").startswith(PROTECTED_SOURCE_PREFIXES)
    }


def get_protection_reason(code: str) -> Optional[str]:
    """code의 보호 사유 (source 값) 반환. 보호 대상 아니면 None.

    로깅/디버깅 용도.
    """
    if not POSITIONS_PATH.exists():
        return None
    try:
        positions = json.loads(POSITIONS_PATH.read_text("utf-8"))
    except (json.JSONDecodeError, OSError):
        return None

    pos = positions.get(code)
    if not pos:
        return None

    source = pos.get("source", "")
    if source.startswith(PROTECTED_SOURCE_PREFIXES):
        return source
    return None
