"""
수급 데이터 수집기 - 외국인/기관/공매도/소진율

데이터 소스: KIS API (투자자수급) + 네이버 일별 frgn (외국인소진율) + 캐시 (공매도)
pykrx 수급 API 전면 깨짐 → KIS API로 대체 (2026-03-04)

수집 항목:
  1순위: 외국인/기관 순매수 (금액+수량) - KIS API FHKST01010900
  1순위: 외국인 소진율 - 네이버 frgn 거래일별 보유율/보유주수
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
from io import StringIO
from typing import Dict, List, Optional, Tuple

# VPS UTC 대응 KST
KST = timezone(timedelta(hours=9))

import pandas as pd
import numpy as np
import requests as _requests

logger = logging.getLogger(__name__)

DATA_DIR = Path(__file__).resolve().parent.parent / "data_store"
FLOW_DIR = DATA_DIR / "flow"          # 수급 데이터
SHORT_DIR = DATA_DIR / "short"        # 공매도 데이터 (KIS 일별추이로 채움 2026-06-27)
CREDIT_DIR = DATA_DIR / "credit"      # 신용잔고 데이터 (KIS 일별추이 2026-06-27 신설)
NAT_DIR = DATA_DIR / "nationality"    # 외국인 국적별 데이터
# ★9/19 [F-237] 구 frgn 페이지는 **폐지**됐다(302 → stock.naver.com).
#   9/11부터 2,531종 전건 실패·6영업일 결손. 상수는 남겨 둔다 — 음성대조 테스트가
#   "구 경로는 더 이상 표를 주지 않는다"를 고정하는 데 쓰고, 이력의 근거이기도 하다.
NAVER_FRGN_URL = "https://finance.naver.com/item/frgn.naver"
# 신 원천: 같은 데이터의 JSON 판. 대조 실측(9/19) = 소진율 807쌍·종가 130쌍 **100% 일치**,
#   전수 2,531종 중 2,491 성공(1.5분). 즉 '다른 데이터로 갈아탄 것'이 아니라 **같은 자**다.
NAVER_TREND_URL = "https://m.stock.naver.com/api/stock/{code}/trend"
# ★9/19 [F-237·D-1] **자가복구 창**. 파라미터 없이 부르면 10거래일만 온다 —
#   이번 사고가 6영업일이었으니 여유가 **단 4일**이었고, 일주일만 늦게 잡았으면
#   정규 수집 경로로는 영영 못 채웠다(그리고 그 사실을 알려줄 알림도 없다).
#   구 HTML 경로는 `page=1`로 20행을 받았으므로 10행은 **사고 대응력의 후퇴**이기도 하다.
#   라이브 실측(9/19): pageSize 20·30·60 정상(전건 소진율 유효) / 100은 HTTP 400.
#   30 = 약 6주 = 이번 사고 길이의 5배 여유. 응답만 커지고 요청 수는 그대로다.
NAVER_TREND_PAGE_SIZE = 30
NAVER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0 Safari/537.36"
    ),
    "Referer": "https://finance.naver.com/",
}
NAVER_API_HEADERS = {
    "User-Agent": NAVER_HEADERS["User-Agent"],
    "Referer": "https://m.stock.naver.com/",
}


def _ensure_dirs():
    FLOW_DIR.mkdir(parents=True, exist_ok=True)
    SHORT_DIR.mkdir(parents=True, exist_ok=True)
    CREDIT_DIR.mkdir(parents=True, exist_ok=True)
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
            # 0바이트/손상 캐시 방어(foreign_exh 534-541과 동일 원리) — 무가드 read_csv는
            # EmptyDataError/파싱실패를 상위로 전파해 investor 스레드 전체(~2600종목) 유실(7/14 검수 M).
            if cache_file.stat().st_size == 0:
                cache_file.unlink()
            else:
                try:
                    cached = pd.read_csv(cache_file, index_col=0, parse_dates=True)
                    last_date = cached.index[-1].strftime("%Y-%m-%d") if len(cached) > 0 else None
                except Exception:
                    cache_file.unlink()
                    last_date = None
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
#  1순위: 외국인 소진율 (네이버 frgn 일별)
# ============================================================

def _safe_int(value, default: int = 0) -> int:
    if value is None:
        return default
    text = str(value).strip()
    if not text or text.lower() == "nan":
        return default
    cleaned = (
        text.replace(",", "")
        .replace("%", "")
        .replace("+", "")
        .replace("−", "-")
        .replace(" ", "")
    )
    try:
        return int(float(cleaned))
    except (TypeError, ValueError):
        return default


def _safe_float(value, default: float = 0.0) -> float:
    if value is None:
        return default
    text = str(value).strip()
    if not text or text.lower() == "nan":
        return default
    cleaned = (
        text.replace(",", "")
        .replace("%", "")
        .replace("+", "")
        .replace("−", "-")
        .replace(" ", "")
    )
    try:
        return float(cleaned)
    except (TypeError, ValueError):
        return default


def _normalize_frgn_columns(df: pd.DataFrame) -> pd.DataFrame:
    """네이버 read_html 결과의 MultiIndex 컬럼을 검색 가능한 문자열로 정리."""
    out = df.copy()
    columns = []
    for col in out.columns:
        if isinstance(col, tuple):
            parts = [str(p).strip() for p in col if str(p).strip() and not str(p).startswith("Unnamed")]
            uniq = []
            for part in parts:
                if part not in uniq:
                    uniq.append(part)
            columns.append(" ".join(uniq) if uniq else "")
        else:
            columns.append(str(col).strip())
    out.columns = columns
    return out


def _find_col(columns: List[str], *keywords: str) -> Optional[str]:
    for col in columns:
        if all(keyword in col for keyword in keywords):
            return col
    return None


def _parse_naver_date(value) -> Optional[str]:
    try:
        return datetime.strptime(str(value).strip(), "%Y.%m.%d").strftime("%Y-%m-%d")
    except (TypeError, ValueError):
        return None


def _parse_naver_frgn_html(code: str, html: str) -> List[dict]:
    """네이버 frgn HTML에서 거래일별 소진율 행을 추출."""
    try:
        tables = pd.read_html(StringIO(html))
    except (ImportError, ValueError) as e:
        logger.debug("네이버 frgn HTML 파싱 실패 %s: %s", code, e)
        return []

    rows = []
    for table in tables:
        df = _normalize_frgn_columns(table).dropna(how="all")
        columns = [str(c).strip() for c in df.columns]
        date_col = _find_col(columns, "날짜")
        close_col = _find_col(columns, "종가")
        holding_col = _find_col(columns, "보유주식수") or _find_col(columns, "보유주수")
        ratio_col = _find_col(columns, "보유율")
        if not all([date_col, close_col, holding_col, ratio_col]):
            continue

        for _, row in df.iterrows():
            date_str = _parse_naver_date(row.get(date_col))
            if not date_str:
                continue
            rate = _safe_float(row.get(ratio_col))
            holding = _safe_int(row.get(holding_col))
            close = _safe_int(row.get(close_col))
            if rate <= 0 and holding <= 0:
                continue
            rows.append({
                "date": date_str,
                "소진율": rate,
                "보유수량": holding,
                "종가": close,
            })

    dedup = {row["date"]: row for row in rows}
    return sorted(dedup.values(), key=lambda x: x["date"], reverse=True)


def _parse_ratio_or_none(value) -> Optional[float]:
    """소진율 문자열 → float. **숫자가 아니면 None**(0.0 아님).

    ★9/19 [F-237] — `_safe_float`는 파싱 실패를 0.0으로 돌려준다. 이 채널에서 그건
    치명적인데, **0.00은 '외국인 보유가 없다'는 실제 값**이기 때문이다. 신 원천은
    일부 종목(실측 7종)에 `'-'`를 주므로 둘을 섞으면 '미제공'이 '보유 0'으로 둔갑한다.
    8/11 교훈(라벨이 아니라 값)의 같은 얼굴 — 그래서 별도 함수로 None을 살려 둔다.
    """
    if value is None:
        return None
    text = str(value).strip()
    if not text or text.lower() == "nan":
        return None
    cleaned = (
        text.replace(",", "").replace("%", "").replace("+", "")
        .replace("−", "-").replace(" ", "")
    )
    if cleaned in ("", "-", "--"):
        return None
    try:
        return float(cleaned)
    except (TypeError, ValueError):
        return None


def _parse_close_or_none(value) -> Optional[int]:
    """종가 문자열 → int. 숫자가 아니면 **None**(0 아님) — [F-237·D-5]."""
    v = _parse_ratio_or_none(value)
    if v is None:
        return None
    try:
        return int(v)
    except (TypeError, ValueError, OverflowError):
        return None


def _parse_naver_trend_json(code: str, payload, stats: Optional[dict] = None) -> List[dict]:
    """신 네이버 trend JSON → 거래일별 소진율 행 (최신순).

    반환 스키마는 구 HTML 파서와 **동일**하다(date/소진율/보유수량/종가) — 호출부와
    캐시 CSV를 한 줄도 바꾸지 않기 위해서다.

    ★`보유수량`은 신 원천에 **없다**(integration·basic·price 전부 확인). 0을 채우지
    않고 `None`(→ CSV 공란)으로 둔다. 0을 쓰면 '실제 보유 0'과 구분이 사라진다.
    실소비처는 `spacex_watchlist`의 리포트 표시 1곳뿐이고 매매 경로(매수 게이트
    `is_foreign_exhaustion_blocked`)는 소진율만 읽는다 — 9/19 호출부 전수 확인.
    """
    if not isinstance(payload, list):
        # ★9/19 [F-237·D-2] 여기서 조용히 빠지면 **이 fix의 목적 자체가 미달**이다.
        #   네이버가 `[...]` → `{"trend":[...]}` 로 감싸는 흔한 스키마 변경을 하면
        #   2,531종 전건 실패인데 로그는 `실패2531 [HTTP오류0/...]` 로 찍혀
        #   9/11 사고 때와 **글자 그대로 같은 정보량**이 된다.
        if stats is not None:
            stats["schema_error"] = stats.get("schema_error", 0) + 1
        return []

    rows = []
    no_ratio_rows = 0
    for item in payload:
        if not isinstance(item, dict):
            continue
        bizdate = str(item.get("bizdate") or "").strip()
        if len(bizdate) != 8 or not bizdate.isdigit():
            continue
        rate = _parse_ratio_or_none(item.get("foreignerHoldRatio"))
        if rate is None:
            # '-' 종목(실측 7종). 이 채널의 목적이 소진율이므로 행을 만들지 않는다.
            # 실패가 아니라 '값 미제공'이라 호출부에서 별도로 센다.
            # ★[F-237·D-4] **종목 단위**로 센다. 행 단위로 세면 한 종목이 30행이라
            #   로그의 `소진율미제공`이 옆 카운터(종목 단위)의 30배로 찍혀 오해를 만든다
            #   ([F-178] 분모 병기 교훈).
            no_ratio_rows += 1
            continue
        rows.append({
            "date": f"{bizdate[:4]}-{bizdate[4:6]}-{bizdate[6:]}",
            "소진율": rate,
            "보유수량": None,      # 신 원천 미제공 — 0으로 위장하지 않는다
            # ★[F-237·D-5] 종가도 같은 원칙. `_safe_int`는 결측을 **0**으로 돌려주는데,
            #   병합이 셀 단위(combine_first)라 0은 '값'으로 취급돼 **기존 캐시의 실제
            #   종가를 0으로 덮는다**(결측이었다면 지켰을 값). 소진율에만 이 원칙을
            #   세우고 바로 옆 줄에서 깨뜨리고 있었다.
            "종가": _parse_close_or_none(item.get("closePrice")),
        })

    if stats is not None and no_ratio_rows and not rows:
        # 이 종목은 전 기간 소진율이 '-' 였다 = 값 미제공 **종목** 1건
        stats["no_ratio"] = stats.get("no_ratio", 0) + 1

    dedup = {row["date"]: row for row in rows}
    return sorted(dedup.values(), key=lambda x: x["date"], reverse=True)


def _drop_empty_exh_rows(df: pd.DataFrame) -> pd.DataFrame:
    """소진율 캐시의 빈 행 제거 — 소진율·보유수량이 **둘 다 결측**인 행만 버린다.

    ★9/19 [F-237] 판정을 `> 0` 에서 `결측 아님` 으로 바꿨다. 이유 둘:
      ① 신 원천은 보유수량을 주지 않는다(공란) → 옛 OR 조건은 한쪽 날개가 꺾인 채
         돌아가고, **소진율 0.00인 84종(전수 실측 3.37%)**이 매일 행을 못 쌓아 빈다.
      ② 0.00은 오염이 아니라 **'외국인 보유가 없다'는 사실**이다.
    ★기존 행 삭제 위험 0 — 옛 규칙이 남긴 행은 전부 두 컬럼이 숫자라 새 규칙에서도
      반드시 보존된다([F-170] 계열 삭제 사고 원천 차단).

    ★★별도 함수인 이유 = **테스트가 이 코드를 직접 부르게** 하기 위해서다. 초안에서는
      이 식을 테스트 파일에 복제해 두고 *"구현이 바뀌면 같이 깨진다"* 고 적었는데
      정반대였다 — 프로덕션을 옛 규칙으로 되돌려도 복제 테스트 3건이 전부 통과했다
      (9/19 Tier-1 검수가 실측으로 적발). 8/6 [F-89]와 같은 얼굴.
    """
    keep_cols = [c for c in ("소진율", "보유수량") if c in df.columns]
    if not keep_cols:
        return df
    mask = df[keep_cols[0]].notna()
    for c in keep_cols[1:]:
        mask = mask | df[c].notna()
    return df[mask]


def _today_kst_ts() -> pd.Timestamp:
    """오늘(KST) 자정 Timestamp — foreign_exh ghost 컷 기준일([F-170]).

    별도 함수인 이유 = **테스트에서 기준일을 갈아끼울 수 있어야** 한다. 이 값이 컷의
    유일한 기준이므로, 고정 날짜 픽스처로 '미래 행은 지우고 과거 행은 안 지운다'를
    양방향으로 고정할 수 있어야 한다."""
    return pd.Timestamp(datetime.now(KST).replace(tzinfo=None).date())


def _fetch_foreign_rates_naver(code: str,
                               http_session: Optional[_requests.Session] = None,
                               stats: Optional[dict] = None) -> List[dict]:
    """네이버 frgn 일별 페이지의 **전 거래일** 외국인 보유율 행 (최신순).

    ★8/13 [F-170] — 이 페이지를 20일치 파싱해 놓고 최신 1행만 쓰고 버리던 것이
    사고의 절반이었다. 나머지 절반은 `collect_foreign_exhaustion`의 ghost 컷이
    파일 **전체 이력**에 걸린다는 것. 둘이 만나면 네이버가 최신일을 한 번이라도
    이전 날짜로 준 종목은 그 뒤 행이 지워지고 되채울 경로가 없다.
    파싱 결과를 그대로 돌려주는 것이 유일한 복구 경로이며 HTTP 추가 비용은 0이다.
    """
    sess = http_session or _requests.Session()
    try:
        # ★9/19 [F-237] 구 frgn HTML 페이지 폐지 → 신 trend JSON.
        #   `allow_redirects=False`를 쓰지 않는다 — 신 엔드포인트는 리다이렉트하지 않고,
        #   혹시 또 바뀌면 조용히 빈 리스트가 아니라 **파싱 실패로 시끄럽게** 나야 한다.
        resp = sess.get(
            NAVER_TREND_URL.format(code=code),
            params={"pageSize": NAVER_TREND_PAGE_SIZE},
            headers=NAVER_API_HEADERS,
            timeout=10,
        )
        resp.raise_for_status()
        payload = resp.json()
        if stats is not None and isinstance(payload, list) and not payload:
            # 상장폐지·거래정지 종목(전수 실측 40종). **실패가 아니다** — 이것을
            # 실패로 세면 매일 "실패 47"이 뜨고, 반복되는 숫자는 배경이 된다.
            # ★[F-237·D-3] `not payload` 만으로는 truthy 한 dict/str 이 빠져나가
            #   스키마 변경이 이 분기로 잘못 들어왔다. list 임을 먼저 확인한다.
            stats["no_rows"] = stats.get("no_rows", 0) + 1
        return _parse_naver_trend_json(code, payload, stats=stats)
    except Exception as e:
        if stats is not None:
            stats["http_error"] = stats.get("http_error", 0) + 1
        logger.warning(f"네이버 외국인 보유비율 조회 실패 {code}: {e}")
        return []


def _fetch_foreign_rate_naver(code: str, http_session: Optional[_requests.Session] = None) -> Optional[dict]:
    """네이버 frgn 일별 페이지에서 최신 거래일 1행 (하위호환 — `_fetch_foreign_rate_api` 전용).

    ★신규 코드는 `_fetch_foreign_rates_naver`(복수)를 쓸 것. 이 함수의 '1행만' 성질이
    [F-170]의 원인이었으므로 수집 경로에서는 더 이상 쓰지 않는다."""
    rows = _fetch_foreign_rates_naver(code, http_session=http_session)
    return rows[0] if rows else None


def collect_foreign_exhaustion(
    codes: List[str],
    months: int = 24,
    force: bool = False,
    session: Optional[Tuple[str, dict]] = None,
) -> Dict[str, pd.DataFrame]:
    """외국인 보유비율(소진율) 수집 - 네이버 frgn 거래일별 페이지

    KIS 현재가 스냅샷은 장전/휴장일에 today ghost를 만들 수 있어 사용하지 않는다.
    네이버 frgn은 거래일 행만 제공하므로 휴장 ghost가 원천 차단된다.

    컬럼: 소진율(%), 보유수량, 종가

    Args:
        session: 하위호환 인자. 네이버 수집에서는 사용하지 않는다.

    Returns: {code: DataFrame(date index)}
    """
    _ensure_dirs()

    # 네이버 frgn의 최신 거래일 행을 매번 확인한다. 기존 KIS 스냅샷 캐시에 today ghost가
    # 남아 있을 수 있으므로 "오늘 날짜가 있으면 스킵"하지 않는다.
    results = {}
    need_fetch = []
    existing_cache_count = 0
    for code in codes:
        cache_file = FLOW_DIR / f"{code}_foreign_exh.csv"
        if cache_file.exists():
            if cache_file.stat().st_size == 0:
                cache_file.unlink()
            else:
                try:
                    pd.read_csv(cache_file, index_col=0, parse_dates=True)
                    existing_cache_count += 1
                except Exception:
                    cache_file.unlink()
        need_fetch.append(code)

    if not need_fetch:
        print("  외국인 소진율: 수집 대상 없음")
        return results

    print(
        f"  외국인 소진율: {len(need_fetch)}종목 네이버 frgn 일별 수집 시작 "
        f"(기존캐시{existing_cache_count}, 거래일 재확인)..."
    )

    fetched = 0
    failed = 0
    stale_skipped = 0
    # ★[F-170] ghost 컷이 실제로 몇 행을 지웠는지 **반드시 보이게** 센다.
    # 이 사고가 오래 안 보인 이유가 "조용히 지웠다"는 것 하나였다.
    ghost_dropped = 0
    # ★9/19 [F-237] 실패를 한 덩어리로 세면 원인이 안 보인다. 9/11~9/18 사고 때
    #   로그는 매일 `실패2531`만 찍었고, 그 숫자만으로는 '네트워크'인지 '원천 폐지'인지
    #   알 수 없었다. 종류별로 센다.
    fetch_stats: dict = {}
    http_session = _requests.Session()

    for i, code in enumerate(need_fetch):
        cache_file = FLOW_DIR / f"{code}_foreign_exh.csv"

        if (i + 1) % 200 == 0 or i == 0:
            print(f"    [{i+1}/{len(need_fetch)}] 수집중... (성공{fetched} 실패{failed})")

        try:
            # ★[F-237·D-3] 주석은 "상폐·정지는 실패가 아니다"라고 선언해 놓고
            #   구현은 전부 `failed` 로 세고 있었다 — 문서가 구현보다 낙관적인
            #   [F-110]/[F-187] 계열. `failed` 는 이 로그에서만 쓰이므로
            #   **진짜 실패(HTTP·스키마)만** 세고 정상 부재(상폐·정지·소진율 '-')는
            #   내역에만 남긴다. 판정은 **카운터 증분**으로 — 별도 상태를 끼워 넣으면
            #   그 상태를 아무도 안 세팅하는 사고가 난다(초안이 실제로 그랬다).
            absent_before = (fetch_stats.get("no_rows", 0)
                             + fetch_stats.get("no_ratio", 0))
            rows = _fetch_foreign_rates_naver(code, http_session=http_session,
                                              stats=fetch_stats)
            if not rows:
                absent_after = (fetch_stats.get("no_rows", 0)
                                + fetch_stats.get("no_ratio", 0))
                if absent_after == absent_before:
                    failed += 1          # 정상 부재로 분류되지 않았다 = 진짜 실패
                continue

            # ★[F-170] 페이지가 준 거래일을 **전부** 병합한다. 최신 1행만 쓰면 아래
            # ghost 컷이 지운 중간 날짜를 영영 되채울 수 없어 이력에 구멍이 남는다
            # (8/13 실측: 9거래일 226건). 전부 병합하면 다음 수집에서 자가복구된다.
            recs = [dict(r) for r in rows]
            dates = [pd.Timestamp(r.pop("date")) for r in recs]
            new_rows = pd.DataFrame(
                recs, index=pd.DatetimeIndex(dates, name="date")
            ).sort_index()

            # 기존 캐시에 병합 — 같은 날짜는 네이버(신규)가 이긴다.
            # ★★9/19 [F-237] `concat + duplicated(keep="last")`를 **쓰면 안 된다**.
            #   신 원천은 보유수량을 주지 않으므로 신규 행의 그 칸은 결측인데, keep="last"는
            #   행 단위로 통째 교체한다 → 같은 날짜의 **기존 보유수량 실값이 결측으로 덮인다**.
            #   신 원천이 매번 10거래일을 주므로 수집 때마다 3,510파일 × 최근 10행이
            #   조용히 지워지고, 신 원천에 그 값이 없으니 **되채울 경로가 0**이다.
            #   = 8/13 [F-170]과 같은 얼굴(조용한 삭제)을, 그 사고를 고친 코드 위에서
            #     다시 만드는 것. 9/19 검수에서 재현해 잡았다.
            #   ★올바른 규칙 = **셀 단위**: 신규에 값이 있으면 신규가 이기고,
            #     신규가 결측이면 기존 값을 지킨다(`combine_first`).
            if cache_file.exists():
                old = pd.read_csv(cache_file, index_col=0, parse_dates=True)
                df = new_rows.combine_first(old)
                # combine_first 는 컬럼 순서를 알파벳순으로 바꾼다 — CSV 헤더를 고정한다.
                cols = [c for c in old.columns if c in df.columns]
                cols += [c for c in df.columns if c not in cols]
                df = df[cols].sort_index()
            else:
                df = new_rows

            # ★[F-170] ghost 컷 기준을 '네이버가 준 최신일' → **오늘(KST)** 로 바꿨다.
            # 옛 `df.index <= row_date` 는 네이버가 한 번이라도 스테일 응답을 주면 그
            # 사이의 **멀쩡한 최근 행을 삭제**했다(8/11 −6 실측 · 소형주는 지워지면
            # 네이버가 과거 보유주수를 0으로 덮으므로 **영구 복구 불가**).
            # 원래 목적이던 "죽은 KIS 스냅샷 경로가 남긴 today ghost 제거"는
            # `_fetch_foreign_rate_kis_snapshot` **호출자 0건**이라 이미 사문이고,
            # 미래 일자만 자르면 목적(있을 수 없는 행 제거)은 지키면서 정상 행은 못 지운다.
            # ★거래일 캘린더로 자르지 않는 이유 = 캘린더가 틀린 적이 있다(7/17 제헌절).
            #   캘린더 버그가 데이터 삭제로 번지는 경로를 만들지 않는다.
            before_cut = len(df)
            df = df[df.index <= _today_kst_ts()]
            ghost_dropped += before_cut - len(df)

            # 빈/오염 행 방어: 소진율 또는 보유수량에 **값이 있는** 거래일만 보존.
            # ★9/19 [F-237] 판정을 `> 0`에서 `결측 아님`으로 바꿨다. 이유 둘:
            #   ① 신 원천은 보유수량을 주지 않는다(공란) → 옛 OR 조건은 한쪽 날개가
            #      꺾인 채로 돌아가고, **소진율 0.00인 84종(전수 실측 3.37%)**이
            #      매일 행을 못 쌓아 조용히 빈다.
            #   ② 0.00은 오염이 아니라 **'외국인 보유가 없다'는 사실**이다. 값을 값으로
            #      취급하지 않은 것이 원래 틀렸다.
            #   ★기존 행 삭제 위험 0 — 옛 규칙이 남긴 행은 전부 두 컬럼이 숫자라
            #     새 규칙에서도 반드시 보존된다([F-170] 계열 삭제 사고 원천 차단).
            before = len(df)
            df = _drop_empty_exh_rows(df)
            stale_skipped += before - len(df)


            # ★9/19 [F-237] 보유수량을 nullable 정수로 고정한다.
            #   신 원천이 이 칸을 주지 않아 결측이 섞이면 pandas 가 컬럼을 float 로 올려
            #   CSV 표기가 `2736456466` → `2736456466.0` 으로 **전 파일에서** 바뀐다.
            #   값은 같지만(2^53 미만이라 정밀도 손실 없음) 이 저장소가 실제로 쓰는
            #   검증 기법 하나가 죽는다 — 8/13·8/21 삭제 사고를 잡아낸 것이 바로
            #   **재수집 전후 바이트 재현** 대조였다. 표기를 지켜 그 기법을 살려 둔다.
            if "보유수량" in df.columns:
                try:
                    df["보유수량"] = df["보유수량"].astype("Int64")
                except (TypeError, ValueError):
                    pass   # 예기치 못한 값이면 표기만 포기하고 데이터는 그대로 둔다
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
    detail = (f" [HTTP오류{fetch_stats.get('http_error', 0)}"
              f"/스키마이상{fetch_stats.get('schema_error', 0)}"
              f"/상폐·정지{fetch_stats.get('no_rows', 0)}"
              f"/소진율미제공{fetch_stats.get('no_ratio', 0)}]"
              "  (실패=HTTP·스키마만·단위 모두 종목)")
    print(f"  외국인 소진율 완료: 수집{fetched}, 기존캐시{existing_cache_count}, 저장"
          f"{total}종목/{len(codes)} ({coverage:.1f}%) 실패{failed}{detail} 정리{stale_skipped}"
          f" ghost컷{ghost_dropped}")
    if ghost_dropped:
        # 미래 일자 행은 정상 경로로는 생길 수 없다 — 0이 아니면 새 유입 경로가 생긴 것.
        logger.warning("[F-170] foreign_exh ghost 컷 %d행 삭제 — 미래 일자 행 유입 경로 확인 필요",
                       ghost_dropped)
    return results


def _fetch_foreign_rate_api(base_url: str, headers: dict, code: str) -> Optional[dict]:
    """하위호환 wrapper: 외인소진율은 네이버 거래일별 페이지를 사용."""
    row = _fetch_foreign_rate_naver(code)
    if row:
        row = dict(row)
        row.pop("date", None)
    return row


def _fetch_foreign_rate_kis_snapshot(base_url: str, headers: dict, code: str) -> Optional[dict]:
    """KIS 현재가 API에서 외국인 보유비율 조회 (legacy snapshot fallback only)."""
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
#  공매도/신용 일별추이 — KIS API (2026-06-27, pykrx/KRX 중단 대체)
#  변동성 장 급락 선행지표. record-only 적재(매매 무접촉).
# ============================================================

def _kis_daily_to_df(rows: list) -> Optional[pd.DataFrame]:
    """KIS 일별추이 dict 리스트 → date index DataFrame(yyyymmdd→datetime)."""
    if not rows:
        return None
    df = pd.DataFrame(rows)
    if "date" not in df.columns:
        return None
    df["date"] = pd.to_datetime(df["date"], format="%Y%m%d", errors="coerce")
    df = df.dropna(subset=["date"]).set_index("date")
    return df if len(df) else None


def _collect_kis_daily(
    codes: List[str],
    cache_dir: Path,
    suffix: str,
    fetch_fn,
    label: str,
    force: bool = False,
) -> Dict[str, pd.DataFrame]:
    """KIS 종목별 일별추이 공통 수집 — 증분 캐시 병합(중복 keep=last, sort).

    cache_dir/{code}_{suffix}.csv 에 저장. fetch_fn(code) -> {success, data:[{date,...}]}.
    """
    _ensure_dirs()
    results = {}
    fetched = failed = 0
    for i, code in enumerate(codes):
        if (i + 1) % 500 == 0 or i == 0:
            print(f"    [{label}] [{i+1}/{len(codes)}] 수집중... (성공{fetched} 실패{failed})")
        cache_file = cache_dir / f"{code}_{suffix}.csv"
        try:
            res = fetch_fn(code)
            if not res or not res.get("success"):
                failed += 1
                continue
            df_new = _kis_daily_to_df(res.get("data", []))
            if df_new is None:
                failed += 1
                continue
            if cache_file.exists():
                try:
                    old = pd.read_csv(cache_file, index_col=0, parse_dates=True)
                    # 구 스키마(pykrx 공매도 잔재 등) 컬럼 불일치 시 → KIS 신규 스키마로 교체
                    if set(old.columns) == set(df_new.columns):
                        df = pd.concat([old, df_new])
                        df = df[~df.index.duplicated(keep="last")].sort_index()
                    else:
                        df = df_new.sort_index()
                except Exception:
                    df = df_new.sort_index()
            else:
                df = df_new.sort_index()
            df.to_csv(cache_file)
            results[code] = df
            fetched += 1
            time.sleep(0.12)
        except Exception as e:
            logger.warning(f"[{label}] {code} 수집 실패: {e}")
            failed += 1
            continue
    coverage = len(results) / len(codes) * 100 if codes else 0
    print(f"  {label} 완료: 수집{fetched} 실패{failed} 저장{len(results)}/{len(codes)} ({coverage:.1f}%)")
    return results


def collect_short_sale(
    codes: List[str],
    months: int = 24,
    force: bool = False,
) -> Dict[str, pd.DataFrame]:
    """공매도 일별추이 수집 — KIS API (FHPST04830000, pykrx 중단 대체 2026-06-27).

    저장: data_store/short/{code}_short_bal.csv (date index)
          — data_verifier DC-04가 기대하는 경로/형식과 동일(자동 GREEN화).
    컬럼: close, short_qty, short_amt, short_ratio, avg_price
    record-only(매매 무접촉).
    """
    from bot.kis_trader import KISTrader
    trader = KISTrader()
    return _collect_kis_daily(
        codes, SHORT_DIR, "short_bal",
        trader.fetch_daily_short_sale, "공매도", force,
    )


def collect_credit_balance(
    codes: List[str],
    months: int = 24,
    force: bool = False,
) -> Dict[str, pd.DataFrame]:
    """신용잔고 일별추이 수집 — KIS API (FHPST04760000, 2026-06-27 신설).

    저장: data_store/credit/{code}_credit_bal.csv (date index)
    컬럼: close, credit_buy_qty, credit_buy_amt, credit_buy_rate, credit_sell_qty
    record-only(매매 무접촉).
    """
    from bot.kis_trader import KISTrader
    trader = KISTrader()
    return _collect_kis_daily(
        codes, CREDIT_DIR, "credit_bal",
        trader.fetch_daily_credit_balance, "신용", force,
    )


# ============================================================
#  [DEAD stub] 구 pykrx 공매도/거래량 (KRX 중단 2026-04~) — collect_short_sale(KIS)로 대체.
#  ※ _try_pykrx_short_balance(고아·pykrx probe 함수)는 6/30 제거(호출처 0 + KRX 무접촉 룰).
#  아래 두 stub은 구 수집 스크립트(collect_all.py/run_backfill/universe_builder 등)가
#  아직 import하므로 no-op({})로 유지 — 진짜 공매도는 collect_short_sale(KIS)이 담당.
# ============================================================

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
    print("  수급 데이터 수집기 (KIS 투자자 + 네이버 frgn 소진율 + 캐시)")
    print(f"  종목: {len(codes)}개 | 기간: {months}개월")
    print("=" * 60)

    # KIS는 투자자 수급에만 사용한다. 외국인 소진율은 네이버 거래일별 frgn을 사용한다.
    print(f"\n[0/4] KIS 세션 사전 생성...")
    session1 = _get_kis_session()
    if session1:
        logger.info(f"[H2] KIS 세션 사전 생성 완료")
    else:
        logger.warning(f"[H2] KIS 세션 생성 실패 — 수급 수집 제한적")

    # 1+2. 투자자별 순매수 + 외국인 소진율 (동시 실행)
    # 투자자 수급은 KIS, 외국인 소진율은 네이버 frgn이므로 병렬 안전
    print(f"\n[1+2/4] 투자자 수급 + 외국인 소진율 (병렬)...")
    t0 = time.time()
    investor = {}
    foreign_exh = {}

    with ThreadPoolExecutor(max_workers=2) as executor:
        f_inv = executor.submit(collect_investor_flow, codes, months, force, session=session1)
        f_fex = executor.submit(collect_foreign_exhaustion, codes, months, force)
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

    # 3+4. 공매도 — 진짜 KIS 공매도(collect_short_sale)는 nightly ⑬가 별도 수집(659s).
    # 이 경로는 투자자 수급 복구 전용(_recover_investor_flow·5분 타임아웃)이라 공매도 제외.
    # (이전: collect_short_balance/volume = KRX중단 no-op {} → 빈 dict 직접·동작 무변경)
    short_bal = {}
    short_vol = {}

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


def _compute_real_data_status(inv_dict: dict) -> Tuple[int, Optional[str]]:
    """placeholder(수급 전부 0인 trailing 행) 제외한 실데이터 종목수·최신일 계산 (순수).

    investor.csv는 개장 전 placeholder 행(수급 전부 0)이 미리 생겨, 마커의
    date(=수집 실행일)가 '데이터 최신일'로 오해되는 문제가 있다. 이 함수는
    placeholder를 걷어낸 '실제 수급 데이터의 종목수와 최신일'을 산출해
    마커를 정직화한다(date와 분리). supply_analyzer의 trim 로직을 단일 진실로 재사용.

    Returns: (real_count, data_through)  — data_through는 'YYYY-MM-DD' 또는 None
    """
    from data.supply_analyzer import _trim_trailing_placeholder_rows
    real_count = 0
    data_through = None
    for _df in (inv_dict or {}).values():
        trimmed = _trim_trailing_placeholder_rows(_df)
        if trimmed is None or len(trimmed) == 0:
            continue
        real_count += 1
        try:
            last = str(trimmed.index[-1].date())
        except AttributeError:
            last = str(trimmed.index[-1])
        if data_through is None or last > data_through:
            data_through = last
    return real_count, data_through


def _write_flow_marker(result: dict, total_codes: int = 0):
    """수급 수집 완료 마커 파일 기록 (AUTO-RECOVERY 검증용).

    필드 의미 (6/22 정직화):
      date          = 수집 잡 실행일 (freshness 체크 = '오늘 수집이 돌았나')
      data_through  = 실수급 데이터 최신일 (placeholder 제외) ★ '언제까지의 데이터인가'
      investor      = 처리 종목수 (placeholder 포함)
      investor_real = 실데이터 종목수 (placeholder 제외) ★
    """
    import json
    from datetime import date
    inv_dict = result.get("investor", {})
    inv_count = len(inv_dict)
    fex_count = len(result.get("foreign_exhaustion", {}))
    coverage = inv_count / total_codes * 100 if total_codes > 0 else 0
    try:
        real_count, data_through = _compute_real_data_status(inv_dict)
    except Exception as e:
        logger.warning(f"[FLOW] 실데이터 상태 계산 실패(마커는 기록 진행): {e}")
        real_count, data_through = 0, None
    marker = {
        "date": date.today().strftime("%Y-%m-%d"),
        "data_through": data_through,
        "investor": inv_count,
        "investor_real": real_count,
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
                     f"({coverage:.1f}%, 실데이터 {real_count}종목 through {data_through}), "
                     f"foreign_exh={fex_count}")
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
