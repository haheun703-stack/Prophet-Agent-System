# -*- coding: utf-8 -*-
"""
한국 주식시장 거래일 캘린더
============================
주말 + 공휴일 + 대체휴일 + 임시공휴일 통합 관리
모든 스케줄러/추천/매매 모듈에서 이 모듈을 사용할 것

사용법:
    from data.trading_calendar import is_trading_day, last_trading_day, next_trading_day
"""

from datetime import date, timedelta
from typing import Optional
import logging

logger = logging.getLogger("BH.Calendar")

# ═══════════════════════════════════════════════════════
# 2026 한국 공휴일 (KRX 휴장일)
# ═══════════════════════════════════════════════════════
KR_HOLIDAYS_2026 = {
    # 1월
    date(2026, 1, 1),   # 신정
    date(2026, 1, 16),  # 설날 연휴 (금)
    date(2026, 1, 17),  # 설날 (토) → 대체휴일 1/19(월)
    date(2026, 1, 18),  # 설날 연휴 (일)
    date(2026, 1, 19),  # 설날 대체휴일 (월)
    # 3월
    date(2026, 3, 1),   # 삼일절 (일) → 대체휴일 3/2(월)
    date(2026, 3, 2),   # 삼일절 대체휴일 (월)
    # 5월
    date(2026, 5, 1),   # 근로자의 날 (금) — KRX 휴장
    date(2026, 5, 5),   # 어린이날 (화)
    date(2026, 5, 24),  # 부처님오신날 (일) → 대체휴일 5/25(월)
    date(2026, 5, 25),  # 부처님오신날 대체휴일 (월)
    # 6월
    date(2026, 6, 3),   # 제9회 전국동시지방선거 — 임시공휴일·KRX 휴장
    date(2026, 6, 6),   # 현충일 (토)
    # 8월
    date(2026, 8, 15),  # 광복절 (토)
    date(2026, 8, 17),  # 광복절 대체휴일 (월)
    # 9월
    date(2026, 9, 24),  # 추석 연휴 (목)
    date(2026, 9, 25),  # 추석 (금)
    date(2026, 9, 26),  # 추석 연휴 (토)
    # 10월
    date(2026, 10, 3),  # 개천절 (토)
    date(2026, 10, 5),  # 개천절 대체휴일 (월)
    date(2026, 10, 9),  # 한글날 (금)
    # 12월
    date(2026, 12, 25), # 크리스마스 (금)
    date(2026, 12, 31), # 연말 — KRX 보통 정상 (필요시 추가)
}

# 2025 공휴일 (연말 참조용)
KR_HOLIDAYS_2025 = {
    date(2025, 1, 1),
    date(2025, 1, 28), date(2025, 1, 29), date(2025, 1, 30),
    date(2025, 3, 1),
    date(2025, 5, 1), date(2025, 5, 5), date(2025, 5, 6),
    date(2025, 6, 6),
    date(2025, 8, 15),
    date(2025, 10, 3), date(2025, 10, 6), date(2025, 10, 7), date(2025, 10, 8), date(2025, 10, 9),
    date(2025, 12, 25),
}

# 전체 공휴일 합침
ALL_HOLIDAYS = KR_HOLIDAYS_2025 | KR_HOLIDAYS_2026


def is_holiday(d: date) -> bool:
    """공휴일인지 확인"""
    return d in ALL_HOLIDAYS


def is_trading_day(d: Optional[date] = None) -> bool:
    """거래일인지 확인 (주말 + 공휴일 모두 체크)

    Args:
        d: 확인할 날짜. None이면 오늘
    Returns:
        True if 거래일
    """
    if d is None:
        d = date.today()
    # 주말
    if d.weekday() >= 5:
        return False
    # 공휴일
    if d in ALL_HOLIDAYS:
        return False
    return True


def last_trading_day(d: Optional[date] = None) -> date:
    """직전 거래일 (d 포함하지 않음)

    Args:
        d: 기준 날짜. None이면 오늘
    Returns:
        가장 최근 거래일
    """
    if d is None:
        d = date.today()
    d = d - timedelta(days=1)
    while not is_trading_day(d):
        d = d - timedelta(days=1)
    return d


def next_trading_day(d: Optional[date] = None) -> date:
    """다음 거래일 (d 포함하지 않음)

    Args:
        d: 기준 날짜. None이면 오늘
    Returns:
        다음 거래일
    """
    if d is None:
        d = date.today()
    d = d + timedelta(days=1)
    while not is_trading_day(d):
        d = d + timedelta(days=1)
    return d


def days_until_next_trading(d: Optional[date] = None) -> int:
    """다음 거래일까지 남은 일수

    주말: 금요일 → 월요일 = 3일
    추석 연휴: 수요일 → 다음주 월요일 = 5일
    """
    if d is None:
        d = date.today()
    nxt = next_trading_day(d)
    return (nxt - d).days


def is_long_break(d: Optional[date] = None, threshold: int = 3) -> bool:
    """장기 휴장인지 (연휴 등)

    Args:
        d: 기준 날짜
        threshold: 이 일수 이상이면 장기 휴장
    """
    return days_until_next_trading(d) >= threshold


def get_trading_days_between(start: date, end: date) -> list:
    """두 날짜 사이의 거래일 목록"""
    days = []
    d = start
    while d <= end:
        if is_trading_day(d):
            days.append(d)
        d += timedelta(days=1)
    return days


def describe_today() -> str:
    """오늘 상태 설명 (로그/텔레그램용)"""
    today = date.today()
    dow = ["월", "화", "수", "목", "금", "토", "일"][today.weekday()]

    if is_trading_day(today):
        return f"{today.strftime('%m/%d')}({dow}) 거래일"

    if today.weekday() >= 5:
        reason = "주말"
    elif today in ALL_HOLIDAYS:
        reason = "공휴일"
    else:
        reason = "휴장"

    nxt = next_trading_day(today)
    nxt_dow = ["월", "화", "수", "목", "금", "토", "일"][nxt.weekday()]
    return f"{today.strftime('%m/%d')}({dow}) {reason} — 다음 거래일: {nxt.strftime('%m/%d')}({nxt_dow})"
