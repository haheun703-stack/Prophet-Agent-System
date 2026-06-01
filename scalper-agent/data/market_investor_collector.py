# -*- coding: utf-8 -*-
"""시장별 투자자 11주체 세부 수급 수집기 (사장님 5/31 — 버린 데이터 줍기).

배경: 종목별 수급(flow_collector, KIS FHKST01010900)은 외국인/기관계/개인/기타법인 4주체뿐.
  KIS FHPTJ04040000(시장별)은 raw에 11주체 세부(금융투자/투신/사모/보험/은행/종금/기금/
  기타외국인=미등록)를 다 주는데, 기존 코드(KISOpenAPI_260320/api/kis_api.py:334)는
  외국인/기관계/개인 3개만 파싱하고 나머지를 전부 버렸음 → 세력(금융투자) 족적이 소멸.
  사장님·퀀트봇 지적 + raw 직접확인으로 입증(2026-05-29 기관계의 81%가 금융투자 단독주도).

이 수집기: KIS FHPTJ04040000으로 코스피+코스닥 11주체 세부 순매수(금액)를 저장.
  → 시장 regime/명분 보강("오늘 세력=금융투자가 시장 전체로 사는가").

★ 매매 무관(조회/수집 전용, 매도 경로 무접촉). SAJANG 매매룰 미사용.
★ 종목별 11주체 세부는 이 TR로 안 됨(시장 전체만) → 키움 opt10059 필요(별도, 계좌).

저장: data_store/flow_market/{kospi|kosdaq}_investor.csv
사용: python -m data.market_investor_collector --days 10
"""
import time
import logging
from pathlib import Path
from datetime import datetime, timedelta

import pandas as pd
import requests as _requests

logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent
MKT_DIR = ROOT / "data_store" / "flow_market"

# market 이름 → (FID_INPUT_ISCD 업종코드, FID_INPUT_ISCD_1 시장구분)
MARKETS = {"kospi": ("0001", "KSP"), "kosdaq": ("1001", "KSQ")}

# KIS FHPTJ04040000 raw 필드(순매수 금액 _tr_pbmn) → 한글 컬럼 (11주체 세부)
FIELD_MAP = {
    "frgn_ntby_tr_pbmn": "외국인_금액",        # 외국인 전체
    "frgn_reg_ntby_pbmn": "등록외국인_금액",   # 등록 외국인
    "frgn_nreg_ntby_pbmn": "기타외국인_금액",  # 미등록 = 기타외국인(핫머니)
    "prsn_ntby_tr_pbmn": "개인_금액",
    "orgn_ntby_tr_pbmn": "기관계_금액",        # 기관 합계(세부의 합)
    "scrt_ntby_tr_pbmn": "금융투자_금액",      # 증권사 자기매매 = 세력/시장조성
    "ivtr_ntby_tr_pbmn": "투신_금액",
    "pe_fund_ntby_tr_pbmn": "사모_금액",
    "bank_ntby_tr_pbmn": "은행_금액",
    "insu_ntby_tr_pbmn": "보험_금액",
    "mrbn_ntby_tr_pbmn": "종금_금액",
    "fund_ntby_tr_pbmn": "기금_금액",
    # 주의: etc_ntby_tr_pbmn = 기타단체+기타법인 합(=기타법인과 중복) → 제거. 아래 둘로 분해 저장.
    "etc_orgt_ntby_tr_pbmn": "기타단체_금액",
    "etc_corp_ntby_tr_pbmn": "기타법인_금액",
}

# 기관계(orgn) = 금융투자+투신+사모+은행+보험+종금+기금 (7세부, raw에서 정확히 일치 확인). 정합성 검증용.
_INST_PARTS = ["금융투자_금액", "투신_금액", "사모_금액", "은행_금액",
               "보험_금액", "종금_금액", "기금_금액"]


def _to_int(v):
    try:
        return int(v)
    except (ValueError, TypeError):
        return 0


def _is_trading(ds: str) -> bool:
    """YYYYMMDD 문자열이 거래일인지 (주말·공휴일 제외). 파싱 실패 시 보수적 통과.

    ★ 5/31 단타봇 발견: KIS FHPTJ04040000은 휴일/주말에 빈응답이 아니라 직전 거래일
      데이터를 그대로 복제 반환 → 휴일 행이 stale 복제로 쌓임. is_trading_day로 차단.
    """
    try:
        from data.trading_calendar import is_trading_day
        return is_trading_day(datetime.strptime(ds, "%Y%m%d").date())
    except Exception:
        return True


def _fetch_market_day(base_url, headers, market_code, sub_code, date_str):
    """FHPTJ04040000 하루치 시장 세부 수급. 실패 시 None."""
    url = f"{base_url}/uapi/domestic-stock/v1/quotations/inquire-investor-daily-by-market"
    params = {
        "FID_COND_MRKT_DIV_CODE": "U",
        "FID_INPUT_ISCD": market_code,
        "FID_INPUT_DATE_1": date_str,
        "FID_INPUT_DATE_2": date_str,
        "FID_PERIOD_DIV_CODE": "D",
        "FID_INPUT_ISCD_1": sub_code,
        "FID_INPUT_ISCD_2": market_code,
    }
    r = _requests.get(url, headers=headers, params=params, timeout=15)
    if r.status_code != 200:
        return None
    j = r.json()
    if j.get("rt_cd") != "0":
        return None
    out = j.get("output") or []
    if not out:
        return None
    row = out[0]
    rec = {"date": date_str, "지수종가": row.get("bstp_nmix_prpr", "")}
    for raw_k, ko in FIELD_MAP.items():
        rec[ko] = _to_int(row.get(raw_k, 0))
    return rec


def collect_market_investor(days: int = 1, session=None) -> dict:
    """코스피+코스닥 11주체 세부 수급을 최근 `days` 캘린더일 백필 수집.

    Args:
        days: 오늘 포함 최근 며칠(휴일은 API가 빈 응답 → 자동 skip).
        session: (base_url, headers). None이면 flow_collector._get_kis_session() 재사용.

    Returns: {market: DataFrame}
    """
    MKT_DIR.mkdir(parents=True, exist_ok=True)

    if session is None:
        from data.flow_collector import _get_kis_session
        session = _get_kis_session()
    if session is None:
        logger.error("[MKT] KIS 세션 없음 — 시장 세부 수급 수집 스킵")
        return {}
    base_url, headers = session[0], session[1].copy()
    headers["tr_id"] = "FHPTJ04040000"

    end = datetime.now()
    dates = sorted((end - timedelta(days=i)).strftime("%Y%m%d") for i in range(max(1, days)))
    # ★ 거래일만 수집 (휴일/주말은 KIS가 직전거래일 stale 복제 반환 → 차단)
    dates = [d for d in dates if _is_trading(d)]
    if not dates:
        logger.info("[MKT] 수집 대상 거래일 없음(휴장) — skip")
        return {}

    out = {}
    for mkt, (mcode, scode) in MARKETS.items():
        recs = []
        for ds in dates:
            try:
                rec = _fetch_market_day(base_url, headers, mcode, scode, ds)
                if rec:
                    recs.append(rec)
            except Exception as e:
                logger.warning(f"[MKT] {mkt} {ds} 수집 실패: {e}")
            time.sleep(0.12)
        if not recs:
            logger.warning(f"[MKT] {mkt}: 수집 0건")
            continue
        df_new = pd.DataFrame(recs).set_index("date")
        fp = MKT_DIR / f"{mkt}_investor.csv"
        if fp.exists():
            old = pd.read_csv(fp, index_col=0)
            old.index = old.index.astype(str)
            # ★ 기존 stale(휴일 복제) 행 self-heal: 거래일 행만 유지
            old = old[[_is_trading(str(ix)) for ix in old.index]]
            df = pd.concat([old, df_new])
            df = df[~df.index.duplicated(keep="last")].sort_index()
        else:
            df = df_new.sort_index()
        df.to_csv(fp, encoding="utf-8-sig")
        out[mkt] = df
        print(f"  {mkt}: 신규 {len(df_new)}일 / 누적 {len(df)}일 -> {fp.name}")
    return out


def _summary(out: dict):
    """수집 직후 정합성 + 세부 분해 요약 (기관계 = 세부합 확인)."""
    for mkt, df in out.items():
        if not len(df):
            continue
        last = df.iloc[-1]
        inst = last.get("기관계_금액", 0)
        parts_sum = sum(_to_int(last.get(c, 0)) for c in _INST_PARTS)
        scrt = _to_int(last.get("금융투자_금액", 0))
        share = (scrt / inst * 100) if inst else 0
        print(f"\n[{mkt}] {df.index[-1]} 기관계={inst:+,} (세부합={parts_sum:+,} 정합={'OK' if abs(inst-parts_sum)<=abs(inst)*0.02+1 else 'DIFF'})")
        print(f"   금융투자(세력)={scrt:+,} ({share:.0f}%) 투신={_to_int(last.get('투신_금액',0)):+,} "
              f"보험={_to_int(last.get('보험_금액',0)):+,} 기금={_to_int(last.get('기금_금액',0)):+,}")
        print(f"   외국인 등록={_to_int(last.get('등록외국인_금액',0)):+,} vs 기타외국인(핫머니)={_to_int(last.get('기타외국인_금액',0)):+,}")


if __name__ == "__main__":
    import argparse
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=1, help="최근 며칠 백필(휴일 자동 skip)")
    a = ap.parse_args()
    result = collect_market_investor(days=a.days)
    _summary(result)
