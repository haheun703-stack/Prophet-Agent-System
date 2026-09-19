# -*- coding: utf-8 -*-
"""거래일 캘린더 — 공휴일·대체공휴일 고정 (9/19 신설).

[왜 신설했나] 이 파일은 7/17 제헌절 유령신호 사고의 **원천**이고, 9/19에는 단타봇이
**9/28(월)을 대체공휴일로 오판**해 두 번(9/7·9/19) HIGH로 올렸다. 넣었다면 정상 거래일에
`is_trading_day` 가드가 걸린 잡이 전부 skip 되어 수집·점검·알림이 통째로 죽었다
— 7/17의 **정반대 방향** 사고. 그런데 이 파일에는 **전용 테스트가 0건**이었다.

[규칙이 종류별로 다르다 — 이것을 몰라서 틀렸다]
  · 설날·추석 연휴 → **일요일**과 겹칠 때만 대체
  · 그 밖의 공휴일(삼일절·부처님오신날·광복절·개천절 등) → 토·일 **모두** 대체

★날짜를 추측해 넣지 않는다. 아래 값은 달력·공시 원본 확인분이다.
"""
from datetime import date

from data.trading_calendar import is_trading_day, calendar_covers


def test_chuseok_2026_has_no_substitute_holiday():
    """★9/28(월)은 **거래일**이다 — 단타봇이 두 번 틀린 자리.

    추석 연휴 9/24(목)·9/25(금)·9/26(토)는 **일요일과 겹치지 않으므로** 대체공휴일이 없다.
    이 단언이 빨개지면 누군가 또 9/28을 휴일로 넣은 것이다.
    """
    assert is_trading_day(date(2026, 9, 24)) is False   # 추석 연휴(목)
    assert is_trading_day(date(2026, 9, 25)) is False   # 추석(금)
    assert is_trading_day(date(2026, 9, 26)) is False   # 추석 연휴(토)
    assert is_trading_day(date(2026, 9, 27)) is False   # 일요일
    assert is_trading_day(date(2026, 9, 28)) is True, \
        "9/28(월)은 대체공휴일이 아니다 — 넣으면 정상 거래일에 수집·점검·알림이 전부 skip된다"
    assert is_trading_day(date(2026, 9, 29)) is True


def test_seollal_2026_has_substitute_because_sunday_overlaps():
    """대조군 — 설날은 연휴가 **일요일(1/18)**과 겹쳐서 1/19 대체가 맞다.

    9/28 오판의 근거였던 *"같은 규칙을 5번 적용했다"* 는 패턴 추론이 왜 틀렸는지를
    이 테스트와 위 테스트가 **나란히** 보여준다.
    """
    assert is_trading_day(date(2026, 1, 16)) is False   # 설날 연휴(금)
    assert is_trading_day(date(2026, 1, 17)) is False   # 설날(토)
    assert is_trading_day(date(2026, 1, 18)) is False   # 일요일
    assert is_trading_day(date(2026, 1, 19)) is False, "설날은 일요일 겹침 → 1/19 대체가 있어야 한다"
    assert is_trading_day(date(2026, 1, 20)) is True


def test_non_lunar_holidays_substitute_on_saturday_too():
    """설날·추석이 **아닌** 공휴일은 토요일도 대체 — 8/15·10/3."""
    assert is_trading_day(date(2026, 8, 15)) is False    # 광복절(토)
    assert is_trading_day(date(2026, 8, 17)) is False    # 대체(월)
    assert is_trading_day(date(2026, 10, 3)) is False    # 개천절(토)
    assert is_trading_day(date(2026, 10, 5)) is False    # 대체(월)


def test_constitution_day_still_closed():
    """7/17 제헌절 — 사고 재발 방지 고정(휴장)."""
    assert is_trading_day(date(2026, 7, 17)) is False
    assert is_trading_day(date(2026, 7, 16)) is True
    assert is_trading_day(date(2026, 7, 20)) is True


def test_plain_weekday_and_weekend():
    assert is_trading_day(date(2026, 9, 21)) is True     # 월
    assert is_trading_day(date(2026, 9, 18)) is True     # 금
    assert is_trading_day(date(2026, 9, 19)) is False    # 토
    assert is_trading_day(date(2026, 9, 20)) is False    # 일


def test_uncovered_year_is_visible():
    """[F-114] 미등재 연도는 **조용히 '주말만 휴장'으로 퇴화**하면 안 된다."""
    assert calendar_covers(date(2026, 1, 1)) is True
    assert calendar_covers(date(2027, 1, 1)) is False, \
        "2027 미등재가 covers=True 로 보이면 신정·설날에 거래일로 오판한다"
