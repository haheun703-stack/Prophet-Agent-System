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
from datetime import datetime, timedelta, time as dt_time, timezone
from typing import Dict, List, Optional, Tuple

# VPS UTC 대응 KST
KST = timezone(timedelta(hours=9))

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

def _get_kis_session() -> Optional[Tuple[str, dict]]:
    """KIS API 토큰+헤더 1회 생성, 전 종목에 재사용

    Returns: (base_url, headers_template) 또는 None (실패 시)

    토큰 만료 시 token.dat 삭제 → mojito 강제 재발급 (3회 재시도).
    """
    _scalper_dir = str(Path(__file__).resolve().parent.parent)

    for attempt in range(3):
        try:
            from dotenv import load_dotenv
            load_dotenv()
            import mojito
            import requests as _req

            # 재시도 시 stale token.dat 삭제 → mojito가 새 토큰 발급
            if attempt > 0:
                token_path = Path(_scalper_dir) / "token.dat"
                if token_path.exists():
                    token_path.unlink()
                    logger.info(f"[H3] stale token.dat 삭제 (attempt={attempt+1})")
                time.sleep(3)

            # mojito는 CWD의 token.dat 사용 → CWD를 scalper-agent로 고정
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

            # H3: 토큰 즉시 검증 — None/빈문자열/짧은 토큰 거부
            if not token or len(token) < 10:
                logger.warning(f"[H3] KIS 토큰 무효 (len={len(token) if token else 0}) — 재발급 시도")
                continue

            base_url = "https://openapi.koreainvestment.com:9443"
            headers = {
                "content-type": "application/json; charset=utf-8",
                "authorization": f"Bearer {token}",
                "appkey": os.getenv("KIS_APP_KEY"),
                "appsecret": os.getenv("KIS_APP_SECRET"),
                "custtype": "P",
            }

            # H3: 토큰 유효성 API 테스트 (삼성전자 현재가 1회 조회)
            test_h = headers.copy()
            test_h["tr_id"] = "FHKST01010100"
            resp = _req.get(
                f"{base_url}/uapi/domestic-stock/v1/quotations/inquire-price",
                headers=test_h,
                params={"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": "005930"},
                timeout=5,
            )
            if resp.status_code != 200 or resp.json().get("rt_cd") != "0":
                rt_msg = resp.json().get("msg1", "unknown")
                logger.warning(f"[H3] KIS 토큰 검증 실패 (rt_cd≠0): {rt_msg} — 재발급 시도")
                time.sleep(2)
                continue

            logger.info(f"[H3] KIS 토큰 검증 성공 (attempt={attempt+1})")
            return base_url, headers

        except Exception as e:
            if attempt < 2:
                logger.warning(f"[KIS] 세션 생성 실패 ({attempt+1}차): {e} — 재시도")
            else:
                logger.critical(f"[KIS] 세션 생성 3차 최종 실패: {e}")
                _tg_alert_kis_failure(e)

    return None


def _tg_alert_kis_failure(error):
    """KIS 토큰 3차 실패 시 텔레그램 긴급 알림."""
    try:
        from dotenv import load_dotenv
        # scalper-agent 상위 루트 .env 로드 (텔레그램 토큰 위치)
        root_env = Path(__file__).resolve().parent.parent.parent / ".env"
        load_dotenv(root_env, override=False)
        token = os.getenv("TELEGRAM_BOT_TOKEN")
        chat_id = os.getenv("TELEGRAM_CHAT_ID")
        if not token or not chat_id:
            logger.warning("[TG] TELEGRAM_BOT_TOKEN 또는 CHAT_ID 미설정 — 알림 불가")
            return
        text = (
            "[ALERT] KIS 토큰 사망 — 세션 생성 3차 최종 실패\n"
            f"오류: {error}\n"
            "→ 수급 수집 전면 중단 상태\n"
            "→ token.dat 삭제 후 재발급 필요"
        )
        _requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": text},
            timeout=5,
        )
    except Exception:
        pass


# ============================================================
#  1순위: 투자자별 순매수 (외국인/기관) - KIS API
# ============================================================

def collect_investor_flow(
    codes: List[str],
    months: int = 24,
    force: bool = False,
    session: Optional[Tuple[str, dict]] = None,
) -> Dict[str, pd.DataFrame]:
    """투자자별 순매수 금액+수량 수집 (KIS API, pykrx 깨짐 대체 2026-03-04)

    KIS API tr_id=FHKST01010900 - 30일치 일별 투자자 매매동향
    컬럼: 기관_금액, 개인_금액, 외국인_금액, 기관_수량, 개인_수량, 외국인_수량

    Args:
        session: H2 — 외부에서 전달받은 (base_url, headers). None이면 내부 생성.

    Returns: {code: DataFrame(date index)}
    """
    _ensure_dirs()

    # H-3 수정: 장중 캐시 오염 방지.
    #   - 장마감 후(16:00 KST): 오늘 데이터 캐시 = 확정값 → 캐시 신뢰
    #   - 장중/장전: 캐시 mtime 5분 이내만 신뢰, 아니면 재수집
    #   - VPS UTC 대응: KST 기준 시간으로 판정
    now_kst = datetime.now(KST).replace(tzinfo=None)
    today_str = now_kst.strftime("%Y-%m-%d")
    # KST 16:00 이후 = 장마감 후
    is_market_closed = now_kst.time() >= dt_time(16, 0)
    CACHE_FRESH_SEC = 300  # 장중 캐시 신선도 5분

    results = {}
    need_fetch = []
    for code in codes:
        cache_file = FLOW_DIR / f"{code}_investor.csv"
        if not force and cache_file.exists():
            cached = pd.read_csv(cache_file, index_col=0, parse_dates=True)
            if len(cached) > 0:
                last_date = cached.index[-1].strftime("%Y-%m-%d")
                if last_date == today_str:
                    # 장마감 후 → 확정값, 캐시 신뢰
                    # 장중/장전 → 5분 이내 수집한 캐시만 신뢰
                    if is_market_closed:
                        results[code] = cached
                        continue
                    mtime_age = time.time() - cache_file.stat().st_mtime
                    if mtime_age < CACHE_FRESH_SEC:
                        results[code] = cached
                        continue
        need_fetch.append(code)

    cache_count = len(results)
    if not need_fetch:
        print(f"  투자자별 수급: 전체 캐시 히트 ({cache_count}종목, 오늘 수집 완료)")
        return results

    # H2: 외부 세션 우선 사용, 없으면 내부 생성
    est_sec = len(need_fetch) * 0.15  # 예상 소요시간
    print(f"  투자자 수급: {len(need_fetch)}종목 KIS API 수집 시작 "
          f"(캐시{cache_count}, 예상 {est_sec:.0f}초)...")
    if session is None:
        session = _get_kis_session()
    if session is None:
        logger.error("[FLOW] KIS 세션 없음 — 투자자 수급 수집 스킵")
        return results
    base_url, headers = session[0], session[1].copy()
    headers["tr_id"] = "FHKST01010900"

    fetched = 0
    failed = 0
    t_start = time.time()
    for i, code in enumerate(need_fetch):
        cache_file = FLOW_DIR / f"{code}_investor.csv"

        if (i + 1) % 200 == 0 or i == 0:
            elapsed = time.time() - t_start
            remain = (elapsed / max(i, 1)) * (len(need_fetch) - i)
            print(f"    [{i+1}/{len(need_fetch)}] 수집중... "
                  f"(성공{fetched} 실패{failed} 잔여{remain:.0f}초)")

        try:
            df = _fetch_investor_api(base_url, headers, code)
            if df is not None and len(df) > 0:
                # 기존 캐시에 병합 (30일 이상 축적)
                if cache_file.exists():
                    old = pd.read_csv(cache_file, index_col=0, parse_dates=True)
                    # 기존 CSV에 기타법인 없으면 역산으로 채움
                    if "기타법인_금액" not in old.columns and all(
                        c in old.columns for c in ("외국인_금액", "기관_금액", "개인_금액")
                    ):
                        old["기타법인_금액"] = -(old["외국인_금액"] + old["기관_금액"] + old["개인_금액"])
                    if "기타법인_수량" not in old.columns and all(
                        c in old.columns for c in ("외국인_수량", "기관_수량", "개인_수량")
                    ):
                        old["기타법인_수량"] = -(old["외국인_수량"] + old["기관_수량"] + old["개인_수량"])
                    # 구버전 컬럼명 정리 (기타_금액 → 기타법인_금액)
                    if "기타_금액" in old.columns:
                        if "기타법인_금액" not in old.columns:
                            old.rename(columns={"기타_금액": "기타법인_금액"}, inplace=True)
                        else:
                            old.drop(columns=["기타_금액"], inplace=True, errors="ignore")
                    # NaN 행 백필 (컬럼 있지만 값 비어있는 경우)
                    if "기타법인_금액" in old.columns:
                        mask = old["기타법인_금액"].isna()
                        if mask.any() and all(c in old.columns for c in ("외국인_금액", "기관_금액", "개인_금액")):
                            old.loc[mask, "기타법인_금액"] = -(old.loc[mask, "외국인_금액"] + old.loc[mask, "기관_금액"] + old.loc[mask, "개인_금액"])
                    if "기타법인_수량" in old.columns:
                        mask = old["기타법인_수량"].isna()
                        if mask.any() and all(c in old.columns for c in ("외국인_수량", "기관_수량", "개인_수량")):
                            old.loc[mask, "기타법인_수량"] = -(old.loc[mask, "외국인_수량"] + old.loc[mask, "기관_수량"] + old.loc[mask, "개인_수량"])
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

    elapsed = time.time() - t_start
    total = len(results)
    coverage = total / len(codes) * 100 if codes else 0
    print(f"  투자자별 수급 완료: 신규{fetched} + 캐시{cache_count} = "
          f"{total}종목/{len(codes)} ({coverage:.1f}%) "
          f"실패{failed} | {elapsed:.0f}초")
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
            f_amt = _safe_int(item.get("frgn_ntby_tr_pbmn"))
            i_amt = _safe_int(item.get("orgn_ntby_tr_pbmn"))
            p_amt = _safe_int(item.get("prsn_ntby_tr_pbmn"))
            f_qty = _safe_int(item.get("frgn_ntby_qty"))
            i_qty = _safe_int(item.get("orgn_ntby_qty"))
            p_qty = _safe_int(item.get("prsn_ntby_qty"))
            # 기타법인 = -(외인+기관+개인) — 기타법인/자사주/계열사 등
            etc_amt = -(f_amt + i_amt + p_amt)
            etc_qty = -(f_qty + i_qty + p_qty)
            rows.append({
                "date": pd.Timestamp(f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"),
                "종가": _safe_int(item.get("stck_clpr")),
                "전일대비": _safe_int(item.get("prdy_vrss")),
                "외국인_수량": f_qty,
                "기관_수량": i_qty,
                "개인_수량": p_qty,
                "기타법인_수량": etc_qty,
                "외국인_금액": f_amt,
                "기관_금액": i_amt,
                "개인_금액": p_amt,
                "기타법인_금액": etc_amt,
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
    session: Optional[Tuple[str, dict]] = None,
) -> Dict[str, pd.DataFrame]:
    """외국인 보유비율(소진율) 수집 - KIS 현재가 API

    pykrx get_exhaustion_rates 깨짐 → KIS 현재가에서 hts_frgn_ehrt 필드 사용
    일별 추이 대신 현재 보유비율 + 투자자수급 외국인_수량으로 추이 보완

    컬럼: 소진율(%), 보유수량, 종가

    Args:
        session: H2 — 외부에서 전달받은 (base_url, headers). None이면 내부 생성.

    Returns: {code: DataFrame(date index)}
    """
    _ensure_dirs()

    # 캐시: 오늘 날짜 데이터 있으면 스킵, 없으면 재수집
    today_str = datetime.now().strftime("%Y-%m-%d")
    results = {}
    need_fetch = []
    for code in codes:
        cache_file = FLOW_DIR / f"{code}_foreign_exh.csv"
        if not force and cache_file.exists():
            if cache_file.stat().st_size == 0:
                cache_file.unlink()
            else:
                try:
                    cached = pd.read_csv(cache_file, index_col=0, parse_dates=True)
                except Exception:
                    cache_file.unlink()
                    need_fetch.append(code)
                    continue
                if len(cached) > 0:
                    last_date = cached.index[-1].strftime("%Y-%m-%d")
                    if last_date == today_str:
                        results[code] = cached
                        continue
        need_fetch.append(code)

    cache_count = len(results)
    if not need_fetch:
        print(f"  외국인 소진율: 전체 캐시 히트 ({cache_count}종목, 오늘 수집 완료)")
        return results

    # H2: 외부 세션 우선 사용, 없으면 내부 생성
    print(f"  외국인 소진율: {len(need_fetch)}종목 KIS API 수집 시작 (캐시{cache_count})...")
    if session is None:
        session = _get_kis_session()
    if session is None:
        logger.error("[FLOW] KIS 세션 없음 — 외국인 소진율 수집 스킵")
        return results
    base_url, headers = session[0], session[1].copy()
    headers["tr_id"] = "FHKST01010100"

    fetched = 0
    failed = 0
    today = pd.Timestamp(datetime.now().strftime("%Y-%m-%d"))

    for i, code in enumerate(need_fetch):
        cache_file = FLOW_DIR / f"{code}_foreign_exh.csv"

        if (i + 1) % 200 == 0 or i == 0:
            print(f"    [{i+1}/{len(need_fetch)}] 수집중... (성공{fetched} 실패{failed})")

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

    total = len(results)
    coverage = total / len(codes) * 100 if codes else 0
    print(f"  외국인 소진율 완료: 신규{fetched} + 캐시{cache_count} = "
          f"{total}종목/{len(codes)} ({coverage:.1f}%) 실패{failed}")
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

def _try_pykrx_short_balance(codes: List[str], months: int) -> Dict[str, pd.DataFrame]:
    """pykrx 공매도 잔고 수집 시도 (깨져 있을 수 있음, 안전하게 실패)

    pykrx API가 복구되면 자동으로 다시 수집됨.
    Returns: {code: DataFrame} — 실패 시 빈 dict
    """
    try:
        from pykrx import stock
        from datetime import date, timedelta

        today = date.today()
        end_date = today.strftime("%Y%m%d")
        start_date = (today - timedelta(days=months * 30)).strftime("%Y%m%d")

        # 삼성전자 1건으로 API 상태 확인 (probe)
        probe = stock.get_shorting_balance_by_date(
            (today - timedelta(days=10)).strftime("%Y%m%d"),
            end_date, "005930",
        )
        if probe.empty:
            logger.info("[SHORT] pykrx 공매도 API 여전히 비정상 — 캐시 모드 유지")
            return {}

        logger.info("[SHORT] pykrx 공매도 API 복구 감지! 수집 시작...")
        results = {}
        fetched = 0
        for code in codes[:50]:  # 최대 50종목 (속도 제한)
            try:
                df = stock.get_shorting_balance_by_date(start_date, end_date, code)
                if not df.empty:
                    results[code] = df
                    fetched += 1
                time.sleep(0.5)
            except Exception:
                continue

        if fetched > 0:
            logger.info(f"[SHORT] pykrx 공매도 수집 성공: {fetched}종목")
            # 캐시 저장
            for code, df in results.items():
                cache_file = SHORT_DIR / f"{code}_short_bal.csv"
                df.to_csv(cache_file)
        return results

    except Exception as e:
        logger.debug(f"[SHORT] pykrx 시도 실패: {e}")
        return {}


def collect_short_balance(
    codes: List[str],
    months: int = 24,
    force: bool = False,
) -> Dict[str, pd.DataFrame]:
    """공매도 잔고 수집 — KRX 데이터 제공 중단 (2026-04~)

    KRX에서 공매도 잔고 데이터를 더 이상 제공하지 않으므로
    즉시 빈 결과를 반환합니다.
    """
    logger.info("[공매도] KRX 데이터 제공 중단 — 수집 스킵")
    return {}


# ============================================================
#  2순위: 공매도 거래량 (pykrx - 현재 깨짐, 캐시 반환 모드)
# ============================================================

def collect_short_volume(
    codes: List[str],
    months: int = 24,
    force: bool = False,
) -> Dict[str, pd.DataFrame]:
    """공매도 거래량/거래대금 수집 — KRX 데이터 제공 중단 (2026-04~)"""
    logger.info("[공매도] KRX 데이터 제공 중단 — 수집 스킵")
    return {}


# ============================================================
#  통합 수집
# ============================================================

def collect_all_flow(
    codes: List[str] = None,
    months: int = 24,
    force: bool = False,
):
    """전체 수급 데이터 수집 (DC-02: 투자자+소진율 병렬)"""
    from concurrent.futures import ThreadPoolExecutor

    if codes is None:
        from data.kis_collector import UNIVERSE
        codes = list(UNIVERSE.keys())

    print("=" * 60)
    print("  수급 데이터 수집기 (KIS API + 캐시)")
    print(f"  종목: {len(codes)}개 | 기간: {months}개월")
    print("=" * 60)

    # H2: 세션 2개를 진입 시 한 번에 생성 (각 스레드가 독립 headers 사용)
    print(f"\n[0/4] KIS 세션 사전 생성...")
    session1 = _get_kis_session()
    session2 = _get_kis_session() if session1 else None
    if session1:
        logger.info(f"[H2] KIS 세션 2개 사전 생성 완료")
    else:
        logger.warning(f"[H2] KIS 세션 생성 실패 — 수급 수집 제한적")

    # 1+2. 투자자별 순매수 + 외국인 소진율 (동시 실행)
    # 서로 다른 KIS API tr_id 사용 → 독립적 headers.copy()로 병렬 안전
    print(f"\n[1+2/4] 투자자 수급 + 외국인 소진율 (병렬)...")
    t0 = time.time()
    investor = {}
    foreign_exh = {}

    with ThreadPoolExecutor(max_workers=2) as executor:
        f_inv = executor.submit(collect_investor_flow, codes, months, force, session=session1)
        f_fex = executor.submit(collect_foreign_exhaustion, codes, months, force, session=session2)
        # C3: 개별 try/except — 한쪽 실패해도 다른 쪽 결과 보존
        try:
            investor = f_inv.result()
        except Exception as e:
            logger.error(f"[C3] 투자자 수급 스레드 실패: {e}")
            investor = {}
        try:
            foreign_exh = f_fex.result()
        except Exception as e:
            logger.error(f"[C3] 외국인 소진율 스레드 실패: {e}")
            foreign_exh = {}

    inv_status = f"{len(investor)}종목" if investor else "실패"
    fex_status = f"{len(foreign_exh)}종목" if foreign_exh else "실패"
    print(f"  → 투자자({inv_status}) + 소진율({fex_status}) 병렬 완료: {int(time.time()-t0)}초")

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

    result = {
        "investor": investor,
        "foreign_exhaustion": foreign_exh,
        "short_balance": short_bal,
        "short_volume": short_vol,
    }

    # 수집 완료 마커 기록 → AUTO-RECOVERY 검증용
    _write_flow_marker(result, total_codes=len(codes))

    return result


def _write_flow_marker(result: dict, total_codes: int = 0):
    """수급 수집 완료 마커 파일 기록 (AUTO-RECOVERY 검증용)."""
    import json
    from datetime import date
    inv_count = len(result.get("investor", {}))
    fex_count = len(result.get("foreign_exhaustion", {}))
    coverage = inv_count / total_codes * 100 if total_codes > 0 else 0
    marker = {
        "date": date.today().strftime("%Y-%m-%d"),
        "investor": inv_count,
        "foreign_exhaustion": fex_count,
        "short_balance": len(result.get("short_balance", {})),
        "short_volume": len(result.get("short_volume", {})),
        "total_codes": total_codes,
        "coverage_pct": round(coverage, 1),
    }
    marker_path = FLOW_DIR / "_last_update.json"
    try:
        with open(marker_path, "w", encoding="utf-8") as f:
            json.dump(marker, f, ensure_ascii=False, indent=2)
        logger.info(f"[FLOW] 마커 기록: investor={inv_count}/{total_codes} "
                     f"({coverage:.1f}%), foreign_exh={fex_count}")
    except Exception as e:
        logger.warning(f"[FLOW] 마커 기록 실패: {e}")


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
