# -*- coding: utf-8 -*-
"""
시장별 투자자 수급 수집기 (KOSPI/KOSDAQ 11주체)
=================================================
2026-04-17 작성

데이터 소스: KIS API FHPTJ04040000 (inquire-investor-daily-by-market)
제공 주체: 11주체 (외인/개인/기관종합/금융투자/투신/사모/은행/보험/기타금융/연기금/기타법인)
단위: 백만원 (원본)

활용:
- BRAIN 시장분석 (11주체 세부 자금 흐름)
- T3 패턴 감지기 보조 (시장 전체 맥락)
- FLOWX VIP 콘텐츠 (스마트머니 추적)

파일: data_store/flow/market_{kospi|kosdaq}.csv (30일 롤링)

사용:
    python -m data.market_flow_collector            # 오늘 1일치
    python -m data.market_flow_collector --days 30  # 30일 백필
"""
from __future__ import annotations

import logging
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Optional, Tuple

import pandas as pd
import requests as _requests

logger = logging.getLogger(__name__)

# ── 경로 ──
SCALPER_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = SCALPER_DIR / "data_store"
FLOW_DIR = DATA_DIR / "flow"

# ── 한국시간 ──
KST = timezone(timedelta(hours=9))

# ── 시장 설정 ──
MARKETS = {
    "kospi":  {"code": "0001", "iscd_1": "KSP", "file": "market_kospi.csv"},
    "kosdaq": {"code": "1001", "iscd_1": "KSQ", "file": "market_kosdaq.csv"},
}

# ── KIS output 필드 → 영문 ID 매핑 (11주체) ──
# 단위: 백만원 → 저장시 그대로 (읽을 때 /100 = 억원)
FIELD_MAP = {
    # 외국인 (종합)
    "frgn_ntby_tr_pbmn":    "foreign",
    "frgn_reg_ntby_pbmn":   "foreign_reg",
    "frgn_nreg_ntby_pbmn":  "foreign_nreg",
    # 개인
    "prsn_ntby_tr_pbmn":    "individual",
    # 기관 세부 (7주체)
    "orgn_ntby_tr_pbmn":    "institution",
    "scrt_ntby_tr_pbmn":    "finance_invest",   # 금융투자(증권)
    "ivtr_ntby_tr_pbmn":    "trust",            # 투신
    "pe_fund_ntby_tr_pbmn": "private_equity",   # 사모펀드
    "bank_ntby_tr_pbmn":    "bank",
    "insu_ntby_tr_pbmn":    "insurance",
    "mrbn_ntby_tr_pbmn":    "other_finance",    # 기타금융
    "fund_ntby_tr_pbmn":    "pension",          # 연기금
    # 기타법인 / 기타단체
    "etc_corp_ntby_tr_pbmn": "other_corp",
    "etc_orgt_ntby_tr_pbmn": "other_orgt",
    # 지수 정보 (BRAIN 활용)
    "bstp_nmix_prpr":       "idx_close",
    "bstp_nmix_oprc":       "idx_open",
    "bstp_nmix_hgpr":       "idx_high",
    "bstp_nmix_lwpr":       "idx_low",
    "bstp_nmix_prdy_ctrt":  "idx_chg_pct",
    "bstp_nmix_prdy_vrss":  "idx_chg",
}

SAVE_COLUMNS = [
    # 지수
    "idx_close", "idx_open", "idx_high", "idx_low", "idx_chg_pct", "idx_chg",
    # 주요 4주체
    "foreign", "individual", "institution", "other_corp",
    # 외인 세부
    "foreign_reg", "foreign_nreg",
    # 기관 세부 (7주체)
    "finance_invest", "trust", "private_equity",
    "bank", "insurance", "other_finance", "pension",
    # 기타단체
    "other_orgt",
]


def _ensure_dirs():
    FLOW_DIR.mkdir(parents=True, exist_ok=True)


def _safe_num(v, default=0, as_float: bool = False):
    """int 기본, as_float=True면 float 반환 (지수값용)."""
    if v is None or v == "":
        return default
    try:
        f = float(v)
        if as_float:
            return f
        return int(f)
    except (ValueError, TypeError):
        return default


# 지수값(소수점 유지) 필드
_FLOAT_FIELDS = {
    "idx_close", "idx_open", "idx_high", "idx_low",
    "idx_chg_pct", "idx_chg",
}


def _fetch_market_day(
    base_url: str,
    headers: dict,
    market_code: str,
    iscd_1: str,
    date_yyyymmdd: str,
) -> Optional[Dict]:
    """특정 시장(KOSPI/KOSDAQ) 1일치 11주체 수급 조회.

    Returns: {date, idx_close, foreign, ..., pension, other_corp} or None
    """
    params = {
        "FID_COND_MRKT_DIV_CODE": "U",
        "FID_INPUT_ISCD": market_code,
        "FID_INPUT_DATE_1": date_yyyymmdd,
        "FID_INPUT_DATE_2": date_yyyymmdd,
        "FID_PERIOD_DIV_CODE": "D",
        "FID_INPUT_ISCD_1": iscd_1,
        "FID_INPUT_ISCD_2": market_code,
    }
    hdr = headers.copy()
    hdr["tr_id"] = "FHPTJ04040000"
    try:
        resp = _requests.get(
            f"{base_url}/uapi/domestic-stock/v1/quotations/inquire-investor-daily-by-market",
            headers=hdr, params=params, timeout=10,
        )
        data = resp.json()
        if data.get("rt_cd") != "0":
            logger.warning(
                f"[MARKET_FLOW] {market_code} {date_yyyymmdd} API 실패: {data.get('msg1')}"
            )
            return None
        output = data.get("output", [])
        if not output:
            return None
        item = output[0]
        # 날짜 확인 (API가 가끔 이전일 반환)
        api_date = item.get("stck_bsop_date", "")
        if api_date != date_yyyymmdd:
            logger.debug(
                f"[MARKET_FLOW] {market_code} 요청일({date_yyyymmdd}) ≠ 응답일({api_date}) — 휴장일 가능"
            )
            return None
        # 필드 매핑 (지수는 float 유지)
        row = {"date": pd.Timestamp(f"{api_date[:4]}-{api_date[4:6]}-{api_date[6:8]}")}
        for kis_key, our_key in FIELD_MAP.items():
            row[our_key] = _safe_num(
                item.get(kis_key),
                as_float=(our_key in _FLOAT_FIELDS),
            )
        return row
    except Exception as e:
        logger.warning(f"[MARKET_FLOW] {market_code} {date_yyyymmdd} 수집 예외: {e}")
        return None


def _get_kis_session() -> Optional[Tuple[str, dict]]:
    """flow_collector와 동일 패턴 — 3회 재시도 + 토큰 검증."""
    _scalper_dir = str(SCALPER_DIR)
    for attempt in range(3):
        try:
            from dotenv import load_dotenv
            load_dotenv()
            import mojito

            if attempt > 0:
                token_path = Path(_scalper_dir) / "token.dat"
                if token_path.exists():
                    token_path.unlink()
                    logger.info(f"[MARKET_FLOW] stale token 삭제 (attempt={attempt+1})")
                time.sleep(3)

            original_cwd = os.getcwd()
            try:
                os.chdir(_scalper_dir)
                broker = mojito.KoreaInvestment(
                    api_key=os.getenv("KIS_APP_KEY"),
                    api_secret=os.getenv("KIS_APP_SECRET"),
                    acc_no=os.getenv("KIS_ACC_NO"),
                    mock=False,
                )
            finally:
                os.chdir(original_cwd)

            token = broker.access_token
            if token and token.startswith("Bearer "):
                token = token.replace("Bearer ", "")
            if not token or len(token) < 10:
                logger.warning(f"[MARKET_FLOW] 토큰 무효 — 재시도")
                continue

            base_url = "https://openapi.koreainvestment.com:9443"
            headers = {
                "content-type": "application/json; charset=utf-8",
                "authorization": f"Bearer {token}",
                "appkey": os.getenv("KIS_APP_KEY"),
                "appsecret": os.getenv("KIS_APP_SECRET"),
                "custtype": "P",
            }
            return base_url, headers
        except Exception as e:
            if attempt < 2:
                logger.warning(f"[MARKET_FLOW] 세션 생성 실패 ({attempt+1}차): {e}")
            else:
                logger.critical(f"[MARKET_FLOW] 세션 생성 3차 실패: {e}")
    return None


def collect_market_flow(
    days: int = 1,
    session: Optional[Tuple[str, dict]] = None,
    force: bool = False,
) -> Dict[str, pd.DataFrame]:
    """KOSPI/KOSDAQ 각각 최근 N일 시장 수급 수집.

    Args:
        days: 수집 일수 (1=오늘만, 30=백필)
        session: 외부 KIS 세션 재사용
        force: True면 캐시 무시하고 재수집

    Returns: {"kospi": DataFrame, "kosdaq": DataFrame}
             columns = SAVE_COLUMNS (단위: 백만원, 지수는 float)
    """
    _ensure_dirs()

    if session is None:
        session = _get_kis_session()
    if session is None:
        logger.error("[MARKET_FLOW] KIS 세션 없음 — 수집 스킵")
        return {}
    base_url, headers = session[0], session[1]

    # KST 기준 오늘부터 역순으로 N일
    now_kst = datetime.now(KST).replace(tzinfo=None)

    results: Dict[str, pd.DataFrame] = {}
    for mname, cfg in MARKETS.items():
        cache_file = FLOW_DIR / cfg["file"]
        existing = None
        if cache_file.exists() and not force:
            try:
                existing = pd.read_csv(cache_file, index_col=0, parse_dates=True)
            except Exception:
                existing = None

        # 수집할 날짜 목록
        collect_dates = []
        for i in range(days):
            d = now_kst - timedelta(days=i)
            # 주말 스킵 (휴장일은 API가 빈 응답 반환 → 자동 처리)
            if d.weekday() >= 5:  # 토(5)/일(6)
                continue
            d_str = d.strftime("%Y%m%d")
            # 이미 있으면 스킵 (force=False)
            if existing is not None and not existing.empty:
                if pd.Timestamp(d.date()) in existing.index:
                    continue
            collect_dates.append(d_str)

        new_rows = []
        for d_str in collect_dates:
            row = _fetch_market_day(
                base_url, headers, cfg["code"], cfg["iscd_1"], d_str
            )
            if row:
                new_rows.append(row)
            time.sleep(0.12)  # 속도 제한

        if not new_rows and existing is None:
            logger.info(f"[MARKET_FLOW] {mname}: 수집 0건 (기존 캐시 없음)")
            continue

        # DataFrame 생성 + 병합
        if new_rows:
            df_new = pd.DataFrame(new_rows).set_index("date")
        else:
            df_new = pd.DataFrame()

        if existing is not None and not existing.empty:
            merged = pd.concat([existing, df_new])
            merged = merged[~merged.index.duplicated(keep="last")].sort_index()
        else:
            merged = df_new.sort_index()

        # 저장 컬럼만 남기기 (순서 보정)
        for col in SAVE_COLUMNS:
            if col not in merged.columns:
                merged[col] = None
        merged = merged[SAVE_COLUMNS]

        # 30일 롤링 (넘으면 오래된 것 버림)
        if len(merged) > 60:  # 여유 60일 유지
            merged = merged.tail(60)

        merged.to_csv(cache_file)
        logger.info(
            f"[MARKET_FLOW] {mname}: 신규 {len(new_rows)}건, 총 {len(merged)}건 → {cache_file.name}"
        )
        results[mname] = merged

    return results


# ============================================================
#  편의 함수: 최근 N일 특정 주체 합계 (억원)
# ============================================================

def get_recent_entity_sum(
    market: str,
    entity: str,
    days: int = 5,
) -> Optional[float]:
    """market_{kospi|kosdaq}.csv에서 최근 N일 특정 주체 누적값 조회 (억원).

    Args:
        market: "kospi" or "kosdaq"
        entity: 영문 주체 ID (예: "foreign", "pension")
        days: 누적 일수

    Returns: 억원 단위 float 또는 None
    """
    if market not in MARKETS:
        return None
    cache_file = FLOW_DIR / MARKETS[market]["file"]
    if not cache_file.exists():
        return None
    try:
        df = pd.read_csv(cache_file, index_col=0, parse_dates=True)
        if entity not in df.columns:
            return None
        vals = df[entity].dropna().tail(days)
        if len(vals) == 0:
            return None
        # 백만원 → 억원
        return float(vals.sum()) / 100.0
    except Exception:
        return None


if __name__ == "__main__":
    import argparse
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=1, help="수집 일수")
    ap.add_argument("--force", action="store_true")
    args = ap.parse_args()

    print(f"[MARKET_FLOW] 수집 시작 — days={args.days} force={args.force}")
    out = collect_market_flow(days=args.days, force=args.force)
    for mname, df in out.items():
        print(f"\n=== {mname.upper()} (최근 5일) ===")
        if df.empty:
            print("(데이터 없음)")
            continue
        # 요약 출력 (억원 환산)
        show = df.tail(5).copy()
        for c in ["foreign", "individual", "institution", "other_corp",
                  "finance_invest", "trust", "private_equity",
                  "bank", "insurance", "other_finance", "pension"]:
            if c in show.columns:
                show[c] = (show[c].astype(float) / 100).round(1)
        cols = ["idx_close", "idx_chg_pct",
                "foreign", "individual", "institution", "other_corp",
                "finance_invest", "pension"]
        cols = [c for c in cols if c in show.columns]
        print(show[cols].to_string())
