# -*- coding: utf-8 -*-
"""KIS NX(KRX+NXT 통합 시장) + 시간대 분기 헬퍼.

Why: 단타봇 기존 fetch_price는 'J'(KRX 정규장)만 사용 → NXT 거래량 누락.
자동매매 진입 평가 시 NXT 비중 큰 종목은 진짜 수급 못 파악.
이 모듈은 신규 추가 — 기존 호출은 안 건드리고 옵션 메서드만 제공.

Usage:
    from utils.kis_nxt_kit import fetch_nx_price, get_session, is_yangmaesoo_5d

    # 1. NX 통합 시세
    r = fetch_nx_price('005930')
    # → {'success': True, 'current_price': 296000, 'volume': 39282453 (KRX+NXT 합산), ...}

    # 2. 시간대 분기
    sess = get_session()
    # → 'KRX_REGULAR' / 'NXT_PREMARKET' / 'NXT_AFTERMARKET' / 'GLOBAL_ETF' / ...

    # 3. 양매수 5일 판정 (자동매매 신호)
    yang = is_yangmaesoo_5d(investor_5d_rows)
    # → {'is_yangmaesoo': True, 'days_dual_buy': 4, ...}

검증된 KIS TR_ID:
- FHKST01010100: 주식현재가 시세 (NX 지원)

비검증 영역 (다음 Phase):
- 매매 주문 NXT 시장 지정 방법
- WebSocket H0STCNT0 실시간 체결 (Phase 3)
- NXT 미참여 종목 fallback 정책
"""
from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, time as dtime
from pathlib import Path
from typing import Optional

import requests

logger = logging.getLogger("BH.KisNxtKit")

_BASE_URL = "https://openapi.koreainvestment.com:9443"
_TOKEN_CACHE = Path(__file__).resolve().parent.parent / "data_store" / ".nxt_kit_token.json"


# ─────────────────────────────────────────────
# 토큰 캐시 (24시간 — KIS access_token 표준)
# ─────────────────────────────────────────────
def _get_access_token() -> Optional[str]:
    """KIS REST API 접근 토큰 (24h 캐시, 자동 갱신)."""
    # 캐시 hit
    if _TOKEN_CACHE.exists():
        try:
            cache = json.loads(_TOKEN_CACHE.read_text())
            if cache.get("expires_at", 0) > time.time() + 60:
                return cache.get("access_token")
        except Exception:
            pass

    # 새 토큰 발급
    app_key = os.getenv("KIS_APP_KEY")
    app_secret = os.getenv("KIS_APP_SECRET")
    if not (app_key and app_secret):
        logger.warning("[NxtKit] KIS_APP_KEY/SECRET 환경변수 없음")
        return None

    try:
        r = requests.post(
            f"{_BASE_URL}/oauth2/tokenP",
            headers={"content-type": "application/json"},
            json={
                "grant_type": "client_credentials",
                "appkey": app_key,
                "appsecret": app_secret,
            },
            timeout=10,
        )
        data = r.json()
        token = data.get("access_token")
        expires_in = int(data.get("expires_in", 86400))
        if not token:
            # M8 fix: 응답 body 전체 로그 X (access_token 노출 위험)
            logger.warning(f"[NxtKit] 토큰 발급 실패: {data.get('error_description') or data.get('msg1') or '?'}")
            return None

        _TOKEN_CACHE.parent.mkdir(parents=True, exist_ok=True)
        _TOKEN_CACHE.write_text(json.dumps({
            "access_token": token,
            "expires_at": time.time() + expires_in - 60,
        }))
        # H2 fix: 토큰 파일 0o600 (소유자 읽기만)
        try:
            os.chmod(_TOKEN_CACHE, 0o600)
        except Exception:
            pass
        return token
    except Exception as e:
        logger.warning(f"[NxtKit] 토큰 발급 실패: {e}")
        return None


# ─────────────────────────────────────────────
# NX 시장 시세 (KRX + NXT 통합)
# ─────────────────────────────────────────────
def fetch_nx_price(code: str) -> dict:
    """NX 시장 통합 현재가/거래량 조회.

    Args:
        code: 6자리 종목코드 (예: '005930')

    Returns:
        success=True 시:
            {success, current_price, change_rate, volume, high, low, open}
            volume은 KRX + NXT 합산 거래량.
        실패 시:
            {success: False, message}

    Note:
        - NXT 미참여 종목 (KOSDAQ 일부)은 stck_prpr=0 응답 → success: False
        - 호출 측에서 success 체크 후 fallback (fetch_price KRX-only) 권장
    """
    token = _get_access_token()
    if not token:
        return {"success": False, "message": "토큰 발급 실패"}

    try:
        r = requests.get(
            f"{_BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-price",
            headers={
                "authorization": f"Bearer {token}",
                "appkey": os.getenv("KIS_APP_KEY", ""),
                "appsecret": os.getenv("KIS_APP_SECRET", ""),
                "tr_id": "FHKST01010100",
                "custtype": "P",
            },
            params={
                "FID_COND_MRKT_DIV_CODE": "NX",
                "FID_INPUT_ISCD": code,
            },
            timeout=5,
        )
        data = r.json()
        if data.get("rt_cd") != "0":
            return {"success": False, "message": data.get("msg1", "?")}

        o = data.get("output", {}) or {}
        cp = int(o.get("stck_prpr", 0) or 0)
        if cp == 0:
            return {"success": False, "message": "NX 응답 0원 (NXT 미참여 가능성)"}

        return {
            "success": True,
            "current_price": cp,
            "change_rate": float(o.get("prdy_ctrt", 0) or 0),
            "volume": int(o.get("acml_vol", 0) or 0),
            "high": int(o.get("stck_hgpr", 0) or 0),
            "low": int(o.get("stck_lwpr", 0) or 0),
            "open": int(o.get("stck_oprc", 0) or 0),
            "market": "NX",
        }
    except Exception as e:
        return {"success": False, "message": f"{type(e).__name__}: {e}"}


# ─────────────────────────────────────────────
# WebSocket approval_key (Phase 3 사용 준비)
# 자동 발급 — 한투 개발자센터 UI 액션 불필요
# ─────────────────────────────────────────────
_APPROVAL_CACHE = Path(__file__).resolve().parent.parent / "data_store" / ".nxt_kit_approval.json"


def get_approval_key(force_refresh: bool = False) -> Optional[str]:
    """WebSocket 실시간 시세용 approval_key 발급 (24h 캐시).

    KIS API의 /oauth2/Approval 엔드포인트가 자동으로 신청+발급 한 번에 처리.
    한투 개발자센터 UI에서 별도 신청 필요 없음.

    Args:
        force_refresh: True면 캐시 무시하고 새로 발급

    Returns:
        approval_key 문자열 (실패 시 None)
    """
    # 캐시 hit
    if not force_refresh and _APPROVAL_CACHE.exists():
        try:
            cache = json.loads(_APPROVAL_CACHE.read_text())
            if cache.get("expires_at", 0) > time.time() + 60:
                return cache.get("approval_key")
        except Exception:
            pass

    app_key = os.getenv("KIS_APP_KEY")
    app_secret = os.getenv("KIS_APP_SECRET")
    if not (app_key and app_secret):
        logger.warning("[NxtKit] approval_key 발급 — KIS 키 환경변수 없음")
        return None

    try:
        r = requests.post(
            f"{_BASE_URL}/oauth2/Approval",
            headers={"content-type": "application/json"},
            json={
                "grant_type": "client_credentials",
                "appkey": app_key,
                "secretkey": app_secret,  # 주의: tokenP는 appsecret, Approval은 secretkey
            },
            timeout=10,
        )
        data = r.json()
        key = data.get("approval_key")
        if not key:
            # M8 fix: 응답 body 전체 로그 X
            logger.warning(f"[NxtKit] approval_key 실패: {data.get('error_description') or data.get('msg1') or '?'}")
            return None

        _APPROVAL_CACHE.parent.mkdir(parents=True, exist_ok=True)
        _APPROVAL_CACHE.write_text(json.dumps({
            "approval_key": key,
            "expires_at": time.time() + 86400 - 60,  # 24h - 1min
        }))
        # H2 fix: approval_key 파일 0o600 (소유자 읽기만)
        try:
            os.chmod(_APPROVAL_CACHE, 0o600)
        except Exception:
            pass
        logger.info("[NxtKit] approval_key 발급/갱신 완료")
        return key
    except Exception as e:
        logger.warning(f"[NxtKit] approval_key 발급 실패: {e}")
        return None


# ─────────────────────────────────────────────
# 시간대 분기
# ─────────────────────────────────────────────
def get_session() -> str:
    """현재 시간대 → 데이터 소스 추천.

    Returns:
        'NXT_PREMARKET' (08:00~08:50)
        'KRX_REGULAR'   (08:55~15:30)
        'NXT_AFTERMARKET' (15:30~20:00)
        'GLOBAL_ETF'    (20:00~08:00 또는 새벽)
        'WEEKEND_GLOBAL' (토/일)
        'IDLE'          (그 외 빈 구간)
    """
    n = datetime.now()
    if n.weekday() >= 5:
        return "WEEKEND_GLOBAL"
    t = n.time()
    if dtime(8, 0) <= t < dtime(8, 50):
        return "NXT_PREMARKET"
    if dtime(8, 55) <= t < dtime(15, 30):
        return "KRX_REGULAR"
    if dtime(15, 30) <= t < dtime(20, 0):
        return "NXT_AFTERMARKET"
    if t >= dtime(20, 0) or t < dtime(8, 0):
        return "GLOBAL_ETF"
    return "IDLE"


# ─────────────────────────────────────────────
# 양매수 5일 판정 (자동매매 신호용)
# ─────────────────────────────────────────────
def is_yangmaesoo_5d(investor_5d_rows: list) -> dict:
    """5일 양매수(외인+기관 동시 매수) 판정.

    Args:
        investor_5d_rows: 최근 5일 수급 row 리스트
            각 row: {date, foreign_qty, inst_qty, ...}

    Returns:
        {is_yangmaesoo: bool,    # 3일+ 양매수면 True
         days_dual_buy: int,     # 둘 다 +인 날 수
         foreign_5d_sum: int,
         inst_5d_sum: int}
    """
    if not investor_5d_rows:
        return {
            "is_yangmaesoo": False,
            "days_dual_buy": 0,
            "foreign_5d_sum": 0,
            "inst_5d_sum": 0,
        }

    def _i(v):
        try:
            return int(v or 0)
        except (TypeError, ValueError):
            return 0

    f_sum = sum(_i(r.get("foreign_qty", r.get("foreign", 0))) for r in investor_5d_rows)
    i_sum = sum(_i(r.get("inst_qty", r.get("inst", 0))) for r in investor_5d_rows)
    dual = sum(
        1 for r in investor_5d_rows
        if _i(r.get("foreign_qty", r.get("foreign", 0))) > 0
        and _i(r.get("inst_qty", r.get("inst", 0))) > 0
    )
    return {
        "is_yangmaesoo": (f_sum > 0 and i_sum > 0 and dual >= 3),
        "days_dual_buy": dual,
        "foreign_5d_sum": f_sum,
        "inst_5d_sum": i_sum,
    }


# ─────────────────────────────────────────────
# CLI 검증
# ─────────────────────────────────────────────
if __name__ == "__main__":
    import sys
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parent.parent.parent / ".env")

    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")

    print("=== KIS NX 통합 시세 검증 ===")
    print(f"현재 세션: {get_session()}")
    print()

    for code, name in [("005930", "삼성전자"), ("000660", "SK하이닉스")]:
        r = fetch_nx_price(code)
        if r["success"]:
            print(f"{name}({code}) NX: 현재가 {r['current_price']:,}원 "
                  f"({r['change_rate']:+.2f}%) 거래량 {r['volume']:,}")
        else:
            print(f"{name}({code}) NX: 실패 — {r['message']}")

    # WebSocket approval_key 자동 발급 검증 (Phase 3 준비)
    print()
    print("=== WebSocket approval_key 자동 발급 검증 ===")
    ak = get_approval_key()
    if ak:
        print(f"approval_key OK ({ak[:20]}...) — Phase 3 WebSocket 즉시 사용 가능")
    else:
        print("approval_key 발급 실패")
