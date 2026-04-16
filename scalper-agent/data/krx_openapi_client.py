# -*- coding: utf-8 -*-
"""
KRX OpenAPI 클라이언트 (https://openapi.krx.co.kr)
====================================================
2026-04-16 작성 (T3 감지기 11주체 데이터 수집용)

현재 상태:
- AUTH_KEY 발급 완료 (.env: KRX_OPEN_API_KEY)
- 콘텐츠별 서비스 이용 신청 **대기 중**
  → 승인 전: 401 Unauthorized 반환
  → 승인 후: 이 모듈 활성화하여 11주체 일별 수급 자동 수집

API 엔드포인트 (openapi.krx.co.kr/svc):
- /apis/sto/stk_bydd_trd          : 유가 일별매매정보
- /apis/sto/ksq_bydd_trd          : 코스닥 일별매매정보
- /apis/sto/stk_invr_bydd_trd     : 유가 투자자별 일별거래 (11주체) ★
- /apis/sto/ksq_invr_bydd_trd     : 코스닥 투자자별 일별거래 (11주체) ★
- /apis/idx/kospi_dd_trd          : KOSPI 지수 일별
- /apis/idx/kosdaq_dd_trd         : KOSDAQ 지수 일별

헤더 규격:
- AUTH_KEY: {발급키}
- Content-Type: application/json

응답 규격:
- JSON { "OutBlock_1": [ {...}, {...} ] }
"""

from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

try:
    from dotenv import load_dotenv
    ENV_PATH = Path(__file__).resolve().parent.parent.parent / ".env"
    load_dotenv(ENV_PATH)
except ImportError:
    pass

try:
    import requests
except ImportError:
    requests = None


BASE_URL = "https://data-dbg.krx.co.kr"

# 서비스 경로 매핑
# 확인된 엔드포인트(401 = 경로 유효, 이용신청 승인 대기 중)
# 미확인 엔드포인트(404 = 경로 오류, KRX 문서에서 정확한 경로 재확인 필요)
ENDPOINTS = {
    # [확인됨 - 승인대기]
    "stk_bydd_trd": "/svc/apis/sto/stk_bydd_trd",          # 유가 일별매매정보
    "ksq_bydd_trd": "/svc/apis/sto/ksq_bydd_trd",          # 코스닥 일별매매정보
    "kospi_dd_trd": "/svc/apis/idx/kospi_dd_trd",          # KOSPI 지수
    "kosdaq_dd_trd": "/svc/apis/idx/kosdaq_dd_trd",        # KOSDAQ 지수
    # [TODO - 경로 재확인 필요]
    # 투자자별 11주체 세부 수급은 KRX OpenAPI 공식 제공 여부가 불확실함.
    # 만약 OpenAPI 미제공이면 정보데이터시스템 웹 크롤링 또는 유료 Data Marketplace 대안 검토.
    # "stk_invr_bydd": "/svc/apis/sto/stk_invr_bydd_trd",  # 404 (경로 오류)
    # "ksq_invr_bydd": "/svc/apis/sto/ksq_invr_bydd_trd",  # 404 (경로 오류)
}


class KRXOpenAPIError(Exception):
    pass


class KRXOpenAPIClient:
    """
    KRX OpenAPI 클라이언트.

    Usage:
        c = KRXOpenAPIClient()  # .env KRX_OPEN_API_KEY 자동 로드
        data = c.fetch_stk_invr_daily("20260416")
    """

    def __init__(self, auth_key: Optional[str] = None, timeout: int = 15):
        self.auth_key = auth_key or os.getenv("KRX_OPEN_API_KEY", "").strip()
        if not self.auth_key:
            raise KRXOpenAPIError("KRX_OPEN_API_KEY 환경변수가 없습니다.")
        self.timeout = timeout
        self._headers = {
            "AUTH_KEY": self.auth_key,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    # ────────────────────────────────────────
    # 내부 공통
    # ────────────────────────────────────────
    def _request(
        self,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        retries: int = 3,
    ) -> Dict[str, Any]:
        if requests is None:
            raise KRXOpenAPIError("requests 라이브러리가 설치되지 않았습니다.")
        url = BASE_URL + path
        last_err = None
        for attempt in range(retries):
            try:
                resp = requests.get(
                    url,
                    headers=self._headers,
                    params=params or {},
                    timeout=self.timeout,
                )
                if resp.status_code == 200:
                    try:
                        return resp.json()
                    except json.JSONDecodeError as e:
                        raise KRXOpenAPIError(
                            f"JSON 디코딩 실패: {e} / body={resp.text[:200]}"
                        )
                if resp.status_code == 401:
                    raise KRXOpenAPIError(
                        f"401 Unauthorized — 콘텐츠 이용신청 승인 대기 중일 수 있습니다. "
                        f"endpoint={path}"
                    )
                if resp.status_code == 429:
                    # 레이트 리밋 → 지수 백오프
                    wait = 2 ** attempt
                    time.sleep(wait)
                    continue
                if resp.status_code >= 500:
                    wait = 2 ** attempt
                    time.sleep(wait)
                    continue
                raise KRXOpenAPIError(
                    f"HTTP {resp.status_code}: {resp.text[:200]}"
                )
            except requests.exceptions.RequestException as e:
                last_err = e
                time.sleep(2 ** attempt)
        raise KRXOpenAPIError(f"요청 실패 (retries={retries}): {last_err}")

    # ────────────────────────────────────────
    # 엔드포인트 래퍼
    # ────────────────────────────────────────
    def fetch_stk_bydd_trd(self, basDd: str) -> List[Dict[str, Any]]:
        """유가증권 시장 일별매매정보 (종목별)."""
        data = self._request(ENDPOINTS["stk_bydd_trd"], params={"basDd": basDd})
        return data.get("OutBlock_1", []) or []

    def fetch_ksq_bydd_trd(self, basDd: str) -> List[Dict[str, Any]]:
        """코스닥 일별매매정보."""
        data = self._request(ENDPOINTS["ksq_bydd_trd"], params={"basDd": basDd})
        return data.get("OutBlock_1", []) or []

    def fetch_stk_investor_daily(self, basDd: str) -> List[Dict[str, Any]]:
        """
        유가증권 시장 투자자별 일별거래 (11주체). ★ 핵심 (미확인) ★

        ※ 주의: OpenAPI 경로가 확정되지 않음.
        KRX openapi.krx.co.kr MyPage에서 정확한 경로 확인 후 ENDPOINTS 등록 필요.
        """
        raise KRXOpenAPIError(
            "투자자별 11주체 엔드포인트 경로 미확정 — KRX OpenAPI 문서 재확인 필요"
        )

    def fetch_ksq_investor_daily(self, basDd: str) -> List[Dict[str, Any]]:
        """코스닥 투자자별 일별거래 (11주체). 미확인."""
        raise KRXOpenAPIError(
            "투자자별 11주체 엔드포인트 경로 미확정 — KRX OpenAPI 문서 재확인 필요"
        )

    def fetch_kospi_index_daily(self, basDd: str) -> List[Dict[str, Any]]:
        data = self._request(ENDPOINTS["kospi_dd_trd"], params={"basDd": basDd})
        return data.get("OutBlock_1", []) or []

    def fetch_kosdaq_index_daily(self, basDd: str) -> List[Dict[str, Any]]:
        data = self._request(ENDPOINTS["kosdaq_dd_trd"], params={"basDd": basDd})
        return data.get("OutBlock_1", []) or []

    # ────────────────────────────────────────
    # 헬스체크
    # ────────────────────────────────────────
    def health_check(self) -> Dict[str, Any]:
        """
        각 엔드포인트 승인 여부 확인 (401/200/500 카운트).
        이용신청 승인되는 엔드포인트부터 200 반환.
        """
        import datetime as _dt
        yesterday = (_dt.datetime.now() - _dt.timedelta(days=1)).strftime("%Y%m%d")

        results = {}
        for name, path in ENDPOINTS.items():
            try:
                data = self._request(path, params={"basDd": yesterday}, retries=1)
                rows = data.get("OutBlock_1", [])
                results[name] = {
                    "status": "OK",
                    "rows": len(rows),
                    "sample": rows[0] if rows else None,
                }
            except KRXOpenAPIError as e:
                msg = str(e)
                if "401" in msg:
                    results[name] = {"status": "PENDING_APPROVAL", "error": msg[:80]}
                else:
                    results[name] = {"status": "ERROR", "error": msg[:120]}
        return results


# ────────────────────────────────────────
# 테스트 CLI
# ────────────────────────────────────────
def main():
    import argparse
    ap = argparse.ArgumentParser(description="KRX OpenAPI 클라이언트 테스트")
    ap.add_argument("--health", action="store_true", help="전 엔드포인트 헬스체크")
    ap.add_argument("--date", default=None, help="YYYYMMDD (기본: 어제)")
    ap.add_argument("--endpoint", default=None, help="특정 엔드포인트만 호출")
    args = ap.parse_args()

    try:
        c = KRXOpenAPIClient()
        print(f"[KRX OpenAPI] AUTH_KEY 로드 완료 (길이={len(c.auth_key)})")
    except KRXOpenAPIError as e:
        print(f"[ERROR] {e}")
        return

    if args.health or args.endpoint is None:
        print("\n=== 엔드포인트 헬스체크 ===")
        results = c.health_check()
        for name, info in results.items():
            status = info["status"]
            mark = "[OK]" if status == "OK" else "[대기]" if status == "PENDING_APPROVAL" else "[에러]"
            print(f"  {mark:6s} {name:20s} {info.get('error', '')[:80]}")

            if status == "OK":
                rows = info.get("rows", 0)
                sample = info.get("sample")
                print(f"         rows={rows}, sample_keys={list(sample.keys())[:8] if sample else []}")
        return

    # 특정 엔드포인트
    import datetime as _dt
    date = args.date or (_dt.datetime.now() - _dt.timedelta(days=1)).strftime("%Y%m%d")
    func_map = {
        "stk_bydd": c.fetch_stk_bydd_trd,
        "ksq_bydd": c.fetch_ksq_bydd_trd,
        "stk_invr": c.fetch_stk_investor_daily,
        "ksq_invr": c.fetch_ksq_investor_daily,
        "kospi_idx": c.fetch_kospi_index_daily,
        "kosdaq_idx": c.fetch_kosdaq_index_daily,
    }
    f = func_map.get(args.endpoint)
    if f is None:
        print(f"알 수 없는 엔드포인트: {args.endpoint}")
        print(f"사용 가능: {list(func_map.keys())}")
        return
    try:
        rows = f(date)
        print(f"[{args.endpoint}] date={date}, rows={len(rows)}")
        if rows:
            print(f"첫 행: {json.dumps(rows[0], ensure_ascii=False, indent=2)}")
    except KRXOpenAPIError as e:
        print(f"[ERROR] {e}")


if __name__ == "__main__":
    main()
