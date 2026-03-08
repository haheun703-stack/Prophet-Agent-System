"""
수급 데이터 수집기 - 외국인/기관/공매도/소진율

데이터 소스: KIS API (투자자수급, 외국인소진율) + 캐시 (공매도)
pykrx 수급 API 전면 깨짐 → KIS API로 대체 (2026-03-04)

수집 항목:
  1순위: 외국인/기관 순매수 (금액+수량) - KIS API FHKST01010900
  1순위: 외국인 소진율 - KIS 현재가 API hts_frgn_ehrt
  2순위: 공매도 잔고/거래량 - 캐시 반환 (pykrx 깨짐)

사용법:
  python -m data.flow_collector
"""

import os
import sys
import time
import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

import pandas as pd
import numpy as np
import requests as _requests

logger = logging.getLogger(__name__)

DATA_DIR = Path(__file__).resolve().parent.parent / "data_store"
FLOW_DIR = DATA_DIR / "flow"          # 수급 데이터
SHORT_DIR = DATA_DIR / "short"        # 공매도 데이터
NAT_DIR = DATA_DIR / "nationality"    # 외국인 국적별 데이터


def _ensure_dirs():
    FLOW_DIR.mkdir(parents=True, exist_ok=True)
    SHORT_DIR.mkdir(parents=True, exist_ok=True)
    NAT_DIR.mkdir(parents=True, exist_ok=True)


# ============================================================
#  KIS API 싱글톤 세션 (346종목 수집 최적화)
# ============================================================

def _get_kis_session() -> Tuple[str, dict]:
    """KIS API 토큰+헤더 1회 생성, 전 종목에 재사용

    Returns: (base_url, headers_template)
    """
    from dotenv import load_dotenv
    load_dotenv()
    import mojito

    broker = mojito.KoreaInvestment(
        api_key=os.getenv("KIS_APP_KEY"),
        api_secret=os.getenv("KIS_APP_SECRET"),
        acc_no=os.getenv("KIS_ACC_NO"),
        mock=False,
    )

    token = broker.access_token
    if token.startswith("Bearer "):
        token = token.replace("Bearer ", "")

    base_url = "https://openapi.koreainvestment.com:9443"
    headers = {
        "content-type": "application/json; charset=utf-8",
        "authorization": f"Bearer {token}",
        "appkey": os.getenv("KIS_APP_KEY"),
        "appsecret": os.getenv("KIS_APP_SECRET"),
        "custtype": "P",
    }

    return base_url, headers


# ============================================================
#  1순위: 투자자별 순매수 (외국인/기관) - KIS API
# ============================================================

def collect_investor_flow(
    codes: List[str],
    months: int = 24,
    force: bool = False,
) -> Dict[str, pd.DataFrame]:
    """투자자별 순매수 금액+수량 수집 (KIS API, pykrx 깨짐 대체 2026-03-04)

    KIS API tr_id=FHKST01010900 - 30일치 일별 투자자 매매동향
    컬럼: 기관_금액, 개인_금액, 외국인_금액, 기관_수량, 개인_수량, 외국인_수량

    Returns: {code: DataFrame(date index)}
    """
    _ensure_dirs()

    # 캐시로 커버되지 않는 종목 확인
    results = {}
    need_fetch = []
    for code in codes:
        cache_file = FLOW_DIR / f"{code}_investor.csv"
        if not force and cache_file.exists():
            cached = pd.read_csv(cache_file, index_col=0, parse_dates=True)
            if len(cached) > 0:
                days_old = (datetime.now() - cached.index[-1].to_pydatetime().replace(tzinfo=None)).days
                if days_old <= 3:
                    results[code] = cached
                    continue
        need_fetch.append(code)

    if not need_fetch:
        print(f"  투자자별 수급: 전체 캐시 히트 ({len(results)}종목)")
        return results

    # KIS 세션 1회 생성
    print(f"  투자자 수급: {len(need_fetch)}종목 KIS API 수집 시작...")
    base_url, headers = _get_kis_session()
    headers["tr_id"] = "FHKST01010900"

    fetched = 0
    failed = 0
    for i, code in enumerate(need_fetch):
        cache_file = FLOW_DIR / f"{code}_investor.csv"

        if (i + 1) % 50 == 0 or i == 0:
            print(f"    [{i+1}/{len(need_fetch)}] {code}...")

        try:
            df = _fetch_investor_api(base_url, headers, code)
            if df is not None and len(df) > 0:
                # 기존 캐시에 병합 (30일 이상 축적)
                if cache_file.exists():
                    old = pd.read_csv(cache_file, index_col=0, parse_dates=True)
                    df = pd.concat([old, df])
                    df = df[~df.index.duplicated(keep="last")]
                    df = df.sort_index()
                df.to_csv(cache_file)
                results[code] = df
                fetched += 1

            time.sleep(0.12)  # KIS API 속도 제한 (초당 ~8건)

        except Exception as e:
            logger.warning(f"투자자별 수급 수집 실패 {code}: {e}")
            failed += 1
            continue

    print(f"  투자자별 수급 완료: 신규{fetched} + 캐시{len(results)-fetched} = {len(results)}종목 (실패{failed})")
    return results


def _fetch_investor_api(base_url: str, headers: dict, code: str) -> Optional[pd.DataFrame]:
    """KIS API로 투자자별 매매동향 30일치 조회 (세션 재사용)"""
    try:
        params = {
            "FID_COND_MRKT_DIV_CODE": "J",
            "FID_INPUT_ISCD": code,
        }

        resp = _requests.get(
            f"{base_url}/uapi/domestic-stock/v1/quotations/inquire-investor",
            headers=headers, params=params, timeout=10,
        )
        data = resp.json()

        if data.get("rt_cd") != "0":
            logger.warning(f"KIS 투자자 API 실패 {code}: {data.get('msg1', '')}")
            return None

        output = data.get("output", [])
        if not output:
            return None

        def _safe_int(val, default=0):
            if not val and val != 0:
                return default
            try:
                return int(val)
            except (ValueError, TypeError):
                return default

        rows = []
        for item in output:
            date_str = item.get("stck_bsop_date", "")
            if not date_str:
                continue
            rows.append({
                "date": pd.Timestamp(f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"),
                "종가": _safe_int(item.get("stck_clpr")),
                "전일대비": _safe_int(item.get("prdy_vrss")),
                "외국인_수량": _safe_int(item.get("frgn_ntby_qty")),
                "기관_수량": _safe_int(item.get("orgn_ntby_qty")),
                "개인_수량": _safe_int(item.get("prsn_ntby_qty")),
                "외국인_금액": _safe_int(item.get("frgn_ntby_tr_pbmn")),
                "기관_금액": _safe_int(item.get("orgn_ntby_tr_pbmn")),
                "개인_금액": _safe_int(item.get("prsn_ntby_tr_pbmn")),
            })

        if not rows:
            return None

        df = pd.DataFrame(rows).set_index("date").sort_index()
        return df

    except Exception as e:
        logger.warning(f"KIS 투자자 수집 실패 {code}: {e}")
        return None


# ============================================================
#  1순위: 외국인 소진율 (KIS 현재가 API)
# ============================================================

def collect_foreign_exhaustion(
    codes: List[str],
    months: int = 24,
    force: bool = False,
) -> Dict[str, pd.DataFrame]:
    """외국인 보유비율(소진율) 수집 - KIS 현재가 API

    pykrx get_exhaustion_rates 깨짐 → KIS 현재가에서 hts_frgn_ehrt 필드 사용
    일별 추이 대신 현재 보유비율 + 투자자수급 외국인_수량으로 추이 보완

    컬럼: 소진율(%), 보유수량, 종가

    Returns: {code: DataFrame(date index)}
    """
    _ensure_dirs()

    # 캐시 확인
    results = {}
    need_fetch = []
    for code in codes:
        cache_file = FLOW_DIR / f"{code}_foreign_exh.csv"
        if not force and cache_file.exists():
            cached = pd.read_csv(cache_file, index_col=0, parse_dates=True)
            if len(cached) > 0:
                days_old = (datetime.now() - cached.index[-1].to_pydatetime().replace(tzinfo=None)).days
                if days_old <= 3:
                    results[code] = cached
                    continue
        need_fetch.append(code)

    if not need_fetch:
        print(f"  외국인 소진율: 전체 캐시 히트 ({len(results)}종목)")
        return results

    # KIS 세션 1회 생성
    print(f"  외국인 소진율: {len(need_fetch)}종목 KIS API 수집 시작...")
    base_url, headers = _get_kis_session()
    headers["tr_id"] = "FHKST01010100"

    fetched = 0
    failed = 0
    today = pd.Timestamp(datetime.now().strftime("%Y-%m-%d"))

    for i, code in enumerate(need_fetch):
        cache_file = FLOW_DIR / f"{code}_foreign_exh.csv"

        if (i + 1) % 50 == 0 or i == 0:
            print(f"    [{i+1}/{len(need_fetch)}] {code}...")

        try:
            row = _fetch_foreign_rate_api(base_url, headers, code)
            if row is None:
                failed += 1
                continue

            new_row = pd.DataFrame([row], index=pd.DatetimeIndex([today], name="date"))

            # 기존 캐시에 병합
            if cache_file.exists():
                old = pd.read_csv(cache_file, index_col=0, parse_dates=True)
                df = pd.concat([old, new_row])
                df = df[~df.index.duplicated(keep="last")]
                df = df.sort_index()
            else:
                df = new_row

            df.to_csv(cache_file)
            results[code] = df
            fetched += 1

            time.sleep(0.12)

        except Exception as e:
            logger.warning(f"외국인 소진율 수집 실패 {code}: {e}")
            failed += 1
            continue

    print(f"  외국인 소진율 완료: 신규{fetched} + 캐시{len(results)-fetched} = {len(results)}종목 (실패{failed})")
    return results


def _fetch_foreign_rate_api(base_url: str, headers: dict, code: str) -> Optional[dict]:
    """KIS 현재가 API에서 외국인 보유비율 조회 (세션 재사용)"""
    try:
        params = {
            "FID_COND_MRKT_DIV_CODE": "J",
            "FID_INPUT_ISCD": code,
        }

        resp = _requests.get(
            f"{base_url}/uapi/domestic-stock/v1/quotations/inquire-price",
            headers=headers, params=params, timeout=10,
        )
        data = resp.json()

        if data.get("rt_cd") != "0":
            return None

        out = data.get("output", {})
        ehrt = out.get("hts_frgn_ehrt", "0")
        hldn = out.get("frgn_hldn_qty", "0")
        prpr = out.get("stck_prpr", "0")
        return {
            "소진율": float(ehrt) if ehrt else 0.0,
            "보유수량": int(hldn) if hldn else 0,
            "종가": int(prpr) if prpr else 0,
        }

    except Exception as e:
        logger.warning(f"KIS 외국인 보유비율 조회 실패 {code}: {e}")
        return None


# ============================================================
#  2순위: 공매도 잔고 (pykrx - 현재 깨짐, 캐시 반환 모드)
# ============================================================

def collect_short_balance(
    codes: List[str],
    months: int = 24,
    force: bool = False,
) -> Dict[str, pd.DataFrame]:
    """공매도 잔고 수집

    주의: pykrx 공매도 API 깨짐 (2026-03 기준)
    - 캐시 있으면 캐시 반환
    - 신규 수집 시도 → 실패시 skip (전체 수집 안 멈춤)

    Returns: {code: DataFrame(date index)}
    """
    _ensure_dirs()

    results = {}
    cache_only = 0
    for i, code in enumerate(codes):
        cache_file = SHORT_DIR / f"{code}_short_bal.csv"

        # 캐시 있으면 무조건 반환 (pykrx 깨져서 갱신 불가)
        if cache_file.exists():
            cached = pd.read_csv(cache_file, index_col=0, parse_dates=True)
            if len(cached) > 0:
                results[code] = cached
                cache_only += 1
                continue

    if cache_only > 0:
        print(f"  공매도 잔고: 캐시 {cache_only}종목 반환 (pykrx API 깨짐, 신규수집 불가)")
    else:
        print(f"  공매도 잔고: 캐시 없음 (pykrx API 깨짐)")
    return results


# ============================================================
#  2순위: 공매도 거래량 (pykrx - 현재 깨짐, 캐시 반환 모드)
# ============================================================

def collect_short_volume(
    codes: List[str],
    months: int = 24,
    force: bool = False,
) -> Dict[str, pd.DataFrame]:
    """공매도 거래량/거래대금 수집

    주의: pykrx 공매도 API 깨짐 (2026-03 기준)
    - 캐시 있으면 캐시 반환
    - 신규 수집 시도 → 실패시 skip

    Returns: {code: DataFrame(date index)}
    """
    _ensure_dirs()

    results = {}
    cache_only = 0
    for i, code in enumerate(codes):
        cache_file = SHORT_DIR / f"{code}_short_vol.csv"

        if cache_file.exists():
            cached = pd.read_csv(cache_file, index_col=0, parse_dates=True)
            if len(cached) > 0:
                results[code] = cached
                cache_only += 1
                continue

    if cache_only > 0:
        print(f"  공매도 거래량: 캐시 {cache_only}종목 반환 (pykrx API 깨짐, 신규수집 불가)")
    else:
        print(f"  공매도 거래량: 캐시 없음 (pykrx API 깨짐)")
    return results


# ============================================================
#  통합 수집
# ============================================================

def collect_all_flow(
    codes: List[str] = None,
    months: int = 24,
    force: bool = False,
):
    """전체 수급 데이터 수집"""
    if codes is None:
        from data.kis_collector import UNIVERSE
        codes = list(UNIVERSE.keys())

    print("=" * 60)
    print("  수급 데이터 수집기 (KIS API + 캐시)")
    print(f"  종목: {len(codes)}개 | 기간: {months}개월")
    print("=" * 60)

    # 1. 투자자별 순매수
    print(f"\n[1/4] 투자자별 순매수 (외국인/기관)...")
    investor = collect_investor_flow(codes, months, force)

    # 2. 외국인 소진율
    print(f"\n[2/4] 외국인 소진율...")
    foreign_exh = collect_foreign_exhaustion(codes, months, force)

    # 3. 공매도 잔고
    print(f"\n[3/4] 공매도 잔고...")
    short_bal = collect_short_balance(codes, months, force)

    # 4. 공매도 거래량
    print(f"\n[4/4] 공매도 거래량...")
    short_vol = collect_short_volume(codes, months, force)

    print(f"\n{'='*60}")
    print(f"  수급 데이터 수집 완료")
    print(f"  투자자별 수급: {len(investor)}종목")
    print(f"  외국인 소진율: {len(foreign_exh)}종목")
    print(f"  공매도 잔고:   {len(short_bal)}종목")
    print(f"  공매도 거래량: {len(short_vol)}종목")
    print(f"{'='*60}")

    return {
        "investor": investor,
        "foreign_exhaustion": foreign_exh,
        "short_balance": short_bal,
        "short_volume": short_vol,
    }


# ============================================================
#  5순위: 외국인 국적별 매매 (KRX HARD053 Playwright)
# ============================================================

def collect_nationality(
    codes: List[str],
    months: int = 24,
    force: bool = False,
) -> Dict[str, pd.DataFrame]:
    """외국인 국적별 거래량 수집 (KRX HARD053 JSON API)

    하이브리드: Playwright(네이버 로그인) + HTTP(JSON API 데이터).
    전체 유니버스(346종목) 대신 추천/보유 종목만 대상으로 할 것.

    캐시: data_store/nationality/{code}.csv - 당일 캐시 있으면 스킵
    쿠키 만료 시 빈 dict 반환 (에러 로그만, 전체 파이프라인 안 멈춤)

    Returns: {code: DataFrame(국가명, 거래규모)}
    """
    _ensure_dirs()

    # 캐시 확인
    results = {}
    need_fetch = []
    today_str = datetime.now().strftime("%Y%m%d")

    for code in codes:
        cache_file = NAT_DIR / f"nationality_{code}.csv"
        if not force and cache_file.exists():
            try:
                cached = pd.read_csv(cache_file, encoding="utf-8-sig")
                # 파일 수정일이 오늘이면 캐시 히트
                mtime = datetime.fromtimestamp(cache_file.stat().st_mtime)
                if mtime.strftime("%Y%m%d") == today_str and len(cached) > 0:
                    results[code] = cached
                    continue
            except Exception:
                pass
        need_fetch.append(code)

    if not need_fetch:
        print(f"  국적별 수급: 전체 캐시 히트 ({len(results)}종목)")
        return results

    # HTTP JSON API 배치 수집
    print(f"  국적별 수급: {len(need_fetch)}종목 KRX HARD053 수집...")
    try:
        from data.krx_nationality_crawler import fetch_nationality_batch
        date_from = (datetime.now() - timedelta(days=5)).strftime("%Y%m%d")
        date_to = today_str

        fetched = fetch_nationality_batch(need_fetch, date_from, date_to)
        for code, df in fetched.items():
            if not df.empty:
                results[code] = df

        ok = sum(1 for df in fetched.values() if not df.empty)
        fail = len(need_fetch) - ok
        print(f"  국적별 수급 완료: 신규{ok} + 캐시{len(results)-ok} = {len(results)}종목 (실패{fail})")

    except Exception as e:
        logger.error(f"국적별 수급 크롤링 실패: {e}")
        print(f"  국적별 수급 실패: {e}")

    return results


def load_nationality(code: str) -> Optional[pd.DataFrame]:
    """캐시된 국적별 매매 데이터 로드"""
    path = NAT_DIR / f"nationality_{code}.csv"
    if path.exists():
        return pd.read_csv(path, encoding="utf-8-sig")
    return None


# ============================================================
#  빠른 조회 유틸
# ============================================================

def load_investor_flow(code: str) -> Optional[pd.DataFrame]:
    """캐시된 투자자별 수급 로드"""
    path = FLOW_DIR / f"{code}_investor.csv"
    if path.exists():
        return pd.read_csv(path, index_col=0, parse_dates=True)
    return None


def load_foreign_exhaustion(code: str) -> Optional[pd.DataFrame]:
    """캐시된 외국인 소진율 로드"""
    path = FLOW_DIR / f"{code}_foreign_exh.csv"
    if path.exists():
        return pd.read_csv(path, index_col=0, parse_dates=True)
    return None


def load_short_balance(code: str) -> Optional[pd.DataFrame]:
    """캐시된 공매도 잔고 로드"""
    path = SHORT_DIR / f"{code}_short_bal.csv"
    if path.exists():
        return pd.read_csv(path, index_col=0, parse_dates=True)
    return None


# ============================================================
#  CLI
# ============================================================

if __name__ == "__main__":
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
    logging.basicConfig(level=logging.WARNING)

    # 전체 유니버스 수집
    collect_all_flow(months=24, force=False)
