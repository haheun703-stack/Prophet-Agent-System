# -*- coding: utf-8 -*-
"""
정보봇 위험감지 SDK Wrapper for bodyhunter (단타봇)

목적:
    bodyhunter 자동매수 사이클(execute_pending_auto_buys) 진입 전 위험감지.
    퀀트봇과 달리 단타봇은 사용자 승인 단계가 없으므로 자동 차단 정책이 더 보수적.

단타봇 정책 (퀀트봇 대비):
    - bot_name = "daytrading"
    - DANGER 구간: 신규 매수 자동 차단 (퀀트봇은 ×0.4 축소)
    - CRISIS 구간: 모든 매수 차단 + 청산 검토
    - MSCI 차단 종목: 자동 거부

호출 패턴:
    # auto_trader.check_risk_gate() 안에서:
    blocked, reason = check_market_risk_blocked()
    if blocked: return False, reason

    # execute_pending_auto_buys() 안에서:
    if is_msci_blacklisted(ticker):
        skip_this_ticker()
"""
from __future__ import annotations

import importlib.util
import logging
import os
from datetime import date
from typing import Optional

logger = logging.getLogger(__name__)

# ★8/29 [F-171] 외인소진율 판정 근거의 최대 허용 나이(달력일).
#   소진율은 평일 매일 수집되고 재수집(19:40)까지 있어 정상이면 1~3일이다.
#   휴장 연휴를 감안해 10일. 실측(8/29): 이 임계로 2,482종 통과 / 49종 stale.
EXHAUSTION_MAX_AGE_DAYS = 10

# 정보봇 SDK importlib 로드 (bodyhunter src/ 와 충돌 회피)
_JGIS_SDK_PATH = "/home/ubuntu/jgis/src/infrastructure/adapters/risk_gate_client.py"
RiskGateClient = None  # type: ignore
_SDK_AVAILABLE = False

try:
    if os.path.exists(_JGIS_SDK_PATH):
        _spec = importlib.util.spec_from_file_location(
            "jgis_risk_gate_client", _JGIS_SDK_PATH
        )
        _module = importlib.util.module_from_spec(_spec)
        _spec.loader.exec_module(_module)
        RiskGateClient = _module.RiskGateClient
        _SDK_AVAILABLE = True
    else:
        logger.warning("[risk_gate_helper] 정보봇 SDK 파일 없음: %s", _JGIS_SDK_PATH)
except Exception as e:
    logger.warning("[risk_gate_helper] 정보봇 SDK 로드 실패: %s — 위험감지 비활성", e)


_client_singleton: Optional["RiskGateClient"] = None


def _get_client() -> Optional["RiskGateClient"]:
    """RiskGateClient 싱글톤 (10분 캐시) — bot_name='daytrading'"""
    global _client_singleton
    if not _SDK_AVAILABLE:
        return None
    if _client_singleton is not None:
        return _client_singleton
    url = os.environ.get("SUPABASE_URL", "")
    key = os.environ.get("SUPABASE_KEY") or os.environ.get("SUPABASE_ANON_KEY", "")
    if not url or not key:
        logger.warning("[risk_gate_helper] SUPABASE 환경변수 없음 — 위험감지 비활성")
        return None
    try:
        _client_singleton = RiskGateClient(url, key, bot_name="daytrading", cache_minutes=10)
        return _client_singleton
    except Exception as e:
        logger.warning("[risk_gate_helper] 초기화 실패: %s", e)
        return None


def check_market_risk_blocked() -> tuple[bool, str]:
    """시장 위험에 의한 신규 매수 차단 여부

    단타봇 정책:
      - DANGER (70~85): 차단 (신규 매수 X)
      - CRISIS (85+):   차단 + 보유 청산 검토
      - WARNING 이하:   통과 (다른 위험 게이트가 sizing 등으로 처리)

    Returns:
        (blocked, reason)
        - blocked=True: 매수 차단해야 함
        - blocked=False: 정보봇 차단 안 함 (기존 게이트로 진행)
    """
    client = _get_client()
    if client is None:
        return False, ""  # SDK 미가용 시 차단 안 함 (운영 지속)

    try:
        if client.should_block_new_entry():
            level_kr = client.get_current_level_kr()
            status = client.get_full_status()
            score = status.get("total_score", 0) if status else 0
            signals = client.get_key_signals()
            top_signal = signals[0] if signals else ""
            return True, f"정보봇 위험감지 {level_kr} ({score}점) — {top_signal}"
        return False, ""
    except Exception as e:
        logger.warning("[risk_gate_helper] 시장 위험 체크 실패: %s", e)
        return False, ""


def is_msci_blacklisted(ticker: str) -> bool:
    """ticker가 정보봇 MSCI 차단목록에 있는가?"""
    client = _get_client()
    if client is None:
        return False
    try:
        return client.is_msci_blacklisted(ticker)
    except Exception as e:
        logger.warning("[risk_gate_helper] MSCI 차단 체크 실패: %s", e)
        return False


def get_risk_info_brief() -> str:
    """단타봇 텔레그램 메시지용 짧은 위험 요약 (1~2줄)

    Returns:
        텍스트 (위험 정보 없으면 빈 문자열)
    """
    client = _get_client()
    if client is None:
        return ""
    try:
        status = client.get_full_status()
        if not status:
            return ""
        level = status.get("level", "NORMAL")
        if level == "NORMAL":
            return ""  # 정상은 표시 안 함
        level_kr = status.get("level_kr", "")
        score = status.get("total_score", 0)
        emoji = {
            "CAUTION": "🟡", "WARNING": "🟠",
            "DANGER": "🔴", "CRISIS": "⚠️",
        }.get(level, "")
        signals = client.get_key_signals()
        top = signals[0] if signals else ""
        return f"{emoji} 정보봇 {level_kr} {score}점 | {top}"
    except Exception:
        return ""


def get_position_multiplier(default: float = 1.0) -> float:
    """매수금액 자동 조절 계수 (단타봇 정책)

    NORMAL: 1.0 / CAUTION: 0.7 / WARNING: 0.5 / DANGER/CRISIS: 0.0 (차단)
    """
    client = _get_client()
    if client is None:
        return default
    try:
        return client.get_position_multiplier()
    except Exception:
        return default


def is_foreign_exhaustion_blocked(code: str) -> bool:
    """종목코드의 외인소진율을 캐시에서 로드 → 위험구간일 때 임계값 초과 시 차단

    5/15 폭락에서 외인소진율 높은 종목이 패닉셀 (유진테크 36% → -19%, 티씨케이 64% → -12%).
    정보봇 위험점수가 위험 구간일 때 외인소진율 50%+ 종목을 자동 차단.

    Args:
        code: 종목코드 (6자리)

    Returns:
        True = 차단해야 함 (NORMAL 구간이면 항상 False)
    """
    client = _get_client()
    if client is None:
        return False
    try:
        from data.flow_collector import load_foreign_exhaustion
        df = load_foreign_exhaustion(code)
        if df is None or len(df) == 0:
            return False

        # ★8/29 [F-171] — 여기는 **날짜를 보지 않고** 마지막 행을 그대로 읽고 있었다.
        #   소진율 파일이 몇 달 전에서 멈춘 종목이 있어도 그 옛 숫자로 오늘 매수를 판정한다.
        #   ★실측(8/29·2,531종): 최신(8/28) 2,422종(95.7%) / **한 달+ 묵은 것 49종(1.9%)**.
        #     그 49종의 소진율은 전부 0~16%라 차단 임계(50%)에 안 걸린다 →
        #     **오늘 시점의 실제 오차단은 0종**이다. 즉 급한 사고는 없다.
        #   ★그러나 위험 방향은 반대다 — 묵은 데이터가 *낮은* 값을 보여주면
        #     **차단해야 할 종목을 통과시킨다**(안전장치가 조용히 무력화). 49종이 전부 그 형태다.
        #   ★처방은 **'보이게 하되 동작 불변'**([F-164] 규약): stale이면 차단하지 않고(현행 유지)
        #     경고를 남긴다. 지금 차단으로 바꾸면 49종이 영구 매수불가가 되는데, 그 부작용을
        #     정당화할 실측 근거(오차단 0종)가 없다. **관측 후 승격**이 순서다.
        _stale_days = None
        try:
            _last = df.index[-1]
            _lastd = _last.date() if hasattr(_last, "date") else None
            if _lastd is not None:
                _stale_days = (date.today() - _lastd).days
        except Exception:  # noqa: BLE001
            _stale_days = None

        rate = float(df["소진율"].iloc[-1])
        if _stale_days is not None and _stale_days > EXHAUSTION_MAX_AGE_DAYS:
            # 판정은 그대로 내되(동작 불변) 근거가 낡았다는 사실을 남긴다.
            logger.warning(
                "[risk_gate_helper][F-171] 외인소진율 STALE %s: 마지막 %d일 전 "
                "(소진율 %.2f%%·임계 밖이면 통과) — 안전장치가 낡은 값으로 판정 중",
                code, _stale_days, rate)
        return client.is_blacklisted_for_exhaustion(rate)
    except Exception as e:
        logger.warning("[risk_gate_helper] 외인소진율 체크 실패 %s: %s", code, e)
        return False


# 정보봇 가이드 7번: CRISIS 구간 인버스 ETF 알림 (자동매수 X, 사장님 수동 결정용)
INVERSE_ETF_BY_INDEX: dict[str, dict[str, str]] = {
    "KOSPI":  {"code": "114800", "name": "KODEX 인버스"},
    "KOSDAQ": {"code": "251340", "name": "KODEX 코스닥150 인버스"},
}


def should_trigger_inverse_alert() -> bool:
    """현재 시장이 CRISIS 구간이고 외국인 매도 폭증 상태인지 판정.

    조건:
      - 위험점수 등급 = CRISIS (85+)
      - 외국인 매도 점수 >= 20 (가이드 임계값)

    Returns:
        True = 인버스 ETF 권유 알림 송출해야 함
    """
    client = _get_client()
    if client is None:
        return False
    try:
        if client.get_current_level() != "CRISIS":
            return False
        status = client.get_full_status()
        if not status:
            return False
        foreign_flow_score = status.get("foreign_flow_score", 0)
        return foreign_flow_score >= 20
    except Exception as e:
        logger.warning("[risk_gate_helper] 인버스 알림 판정 실패: %s", e)
        return False


def get_inverse_alert_message() -> str:
    """CRISIS 구간 인버스 ETF 권유 텔레그램 메시지.

    Returns:
        텍스트 (CRISIS 아니면 빈 문자열)
    """
    client = _get_client()
    if client is None:
        return ""
    try:
        status = client.get_full_status()
        if not status:
            return ""
        score = status.get("total_score", 0)
        signals = client.get_key_signals()
        top_signal = signals[0] if signals else "외국인 매도 폭증"
        kospi = INVERSE_ETF_BY_INDEX["KOSPI"]
        kosdaq = INVERSE_ETF_BY_INDEX["KOSDAQ"]
        return (
            f"⚠️ 시장 위기 감지 — 인버스 ETF 매수 검토 권유\n"
            f"   위험점수: {score}점 (CRISIS)\n"
            f"   주요 신호: {top_signal}\n"
            f"   {kospi['name']}({kospi['code']}) / {kosdaq['name']}({kosdaq['code']})\n"
            f"   ※ 권장 비중: 계좌 10% (사장님 수동 매수)"
        )
    except Exception:
        return ""
