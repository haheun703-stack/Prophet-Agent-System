"""
Parquet 통합 데이터 관리 - OHLCV + 수급 합본

기존 CSV 분산 저장 → 종목별 parquet 1개로 통합
pykrx 수급 빈값 → KIS API fallback 자동 교체

구조:
  data_store/raw/{code}.parquet      - 원본 (OHLCV + 수급)
  data_store/processed/{code}.parquet - 가공 (기술지표 추가)

컬럼:
  시가, 고가, 저가, 종가, 거래량, 등락률           ← OHLCV (pykrx)
  외국인합계, 기관합계, 개인                          ← 수급 수량 (KIS API)
  외국인_금액, 기관_금액, 개인_금액                    ← 수급 금액 (KIS API)
  외국인_소진율                                       ← KIS 현재가 API

사용법:
  python -m data.extend_parquet_data                 # 전체 유니버스 마이그레이션+수집
  python -m data.extend_parquet_data --codes 003570  # 특정 종목만
  python -m data.extend_parquet_data --force          # 캐시 무시 강제 갱신
"""

import os
import sys
import time
import logging
from pathlib import Path
from datetime import datetime
from typing import List, Optional, Tuple

import pandas as pd
import numpy as np
import requests as _requests

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

logger = logging.getLogger(__name__)

DATA_DIR = Path(__file__).resolve().parent.parent / "data_store"
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"
DAILY_DIR = DATA_DIR / "daily"       # 기존 CSV (마이그레이션 소스)
FLOW_DIR = DATA_DIR / "flow"         # 기존 CSV (마이그레이션 소스)
SHORT_DIR = DATA_DIR / "short"       # 기존 CSV (마이그레이션 소스)


def _ensure_dirs():
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)


# ============================================================
#  KIS API 세션 (싱글톤 - 전 종목 재사용)
# ============================================================

def _get_kis_session() -> Optional[Tuple[str, dict]]:
    """KIS API 토큰+헤더 1회 생성 (실패 시 None)"""
    for attempt in range(2):
        try:
            from dotenv import load_dotenv
            load_dotenv()
            import mojito
            import requests as _req

            broker = mojito.KoreaInvestment(
                api_key=os.getenv("KIS_APP_KEY"),
                api_secret=os.getenv("KIS_APP_SECRET"),
                acc_no=os.getenv("KIS_ACC_NO"),
                mock=False,
            )
            token = broker.access_token
            if token and token.startswith("Bearer "):
                token = token.replace("Bearer ", "")

            # H3: 토큰 즉시 검증 — None/빈문자열/짧은 토큰 거부
            if not token or len(token) < 10:
                logger.warning(f"[H3] KIS 토큰 무효 (len={len(token) if token else 0}) — 재발급 시도")
                time.sleep(2)
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
                logger.warning(f"[H3] KIS 토큰 검증 실패: {rt_msg} — 재발급 시도")
                time.sleep(2)
                continue

            logger.info(f"[H3] KIS 토큰 검증 성공 (attempt={attempt+1})")
            return base_url, headers
        except Exception as e:
            if attempt == 0:
                logger.warning(f"[KIS] 세션 생성 실패: {e} — 재시도")
                time.sleep(2)
            else:
                logger.critical(f"[KIS] 세션 재생성도 실패: {e}")
    return None


# ============================================================
#  KIS API 호출: 투자자 수급 (30일)
# ============================================================

def _fetch_investor_kis(base_url: str, headers: dict, code: str) -> Optional[pd.DataFrame]:
    """KIS API FHKST01010900 - 30일치 투자자별 매매동향"""
    try:
        h = headers.copy()
        h["tr_id"] = "FHKST01010900"
        params = {"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": code}

        resp = _requests.get(
            f"{base_url}/uapi/domestic-stock/v1/quotations/inquire-investor",
            headers=h, params=params, timeout=10,
        )
        data = resp.json()
        if data.get("rt_cd") != "0":
            return None

        output = data.get("output", [])
        if not output:
            return None

        def _safe_int(val, default=0):
            """빈 문자열/None → 0 변환"""
            if not val and val != 0:
                return default
            try:
                return int(val)
            except (ValueError, TypeError):
                return default

        rows = []
        for item in output:
            ds = item.get("stck_bsop_date", "")
            if not ds:
                continue
            rows.append({
                "날짜": pd.Timestamp(f"{ds[:4]}-{ds[4:6]}-{ds[6:8]}"),
                "외국인합계": _safe_int(item.get("frgn_ntby_qty")),
                "기관합계": _safe_int(item.get("orgn_ntby_qty")),
                "개인": _safe_int(item.get("prsn_ntby_qty")),
                "외국인_금액": _safe_int(item.get("frgn_ntby_tr_pbmn")),
                "기관_금액": _safe_int(item.get("orgn_ntby_tr_pbmn")),
                "개인_금액": _safe_int(item.get("prsn_ntby_tr_pbmn")),
            })

        if not rows:
            return None
        return pd.DataFrame(rows).set_index("날짜").sort_index()

    except Exception as e:
        logger.warning(f"KIS 투자자 수집 실패 {code}: {e}")
        return None


# ============================================================
#  KIS API 호출: 외국인 소진율 (현재가)
# ============================================================

def _fetch_foreign_rate_kis(base_url: str, headers: dict, code: str) -> Optional[dict]:
    """KIS 현재가 API - 외국인 보유비율"""
    try:
        h = headers.copy()
        h["tr_id"] = "FHKST01010100"
        params = {"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": code}

        resp = _requests.get(
            f"{base_url}/uapi/domestic-stock/v1/quotations/inquire-price",
            headers=h, params=params, timeout=10,
        )
        data = resp.json()
        if data.get("rt_cd") != "0":
            return None

        out = data.get("output", {})
        ehrt = out.get("hts_frgn_ehrt", "0")
        return {
            "외국인_소진율": float(ehrt) if ehrt else 0.0,
        }

    except Exception as e:
        logger.warning(f"KIS 외국인 소진율 실패 {code}: {e}")
        return None


# ============================================================
#  1단계: 기존 CSV → raw parquet 마이그레이션
# ============================================================

def migrate_csv_to_parquet(code: str) -> Optional[pd.DataFrame]:
    """기존 daily CSV + flow CSV → 하나의 DataFrame 합치기"""
    raw_file = RAW_DIR / f"{code}.parquet"

    # 1) OHLCV (daily CSV)
    daily_csv = DAILY_DIR / f"{code}.csv"
    if not daily_csv.exists():
        return None

    df = pd.read_csv(daily_csv, index_col=0, parse_dates=True)
    if len(df) == 0:
        return None

    # 컬럼 표준화
    expected = ["시가", "고가", "저가", "종가", "거래량", "등락률"]
    if len(df.columns) == 6:
        df.columns = expected

    # 2) 투자자 수급 (flow CSV) - 있으면 merge
    inv_csv = FLOW_DIR / f"{code}_investor.csv"
    if inv_csv.exists():
        inv = pd.read_csv(inv_csv, index_col=0, parse_dates=True)
        # 컬럼 매핑: 기존 CSV의 다양한 컬럼명 → 표준화
        col_map = {}
        for c in inv.columns:
            if "외국인" in c and "수량" in c:
                col_map[c] = "외국인합계"
            elif "기관" in c and "수량" in c:
                col_map[c] = "기관합계"
            elif "개인" in c and "수량" in c:
                col_map[c] = "개인"
            elif "외국인" in c and "금액" in c:
                col_map[c] = "외국인_금액"
            elif "기관" in c and "금액" in c:
                col_map[c] = "기관_금액"
            elif "개인" in c and "금액" in c:
                col_map[c] = "개인_금액"

        if col_map:
            inv_mapped = inv.rename(columns=col_map)
            supply_cols = ["외국인합계", "기관합계", "개인", "외국인_금액", "기관_금액", "개인_금액"]
            available = [c for c in supply_cols if c in inv_mapped.columns]
            if available:
                df = df.join(inv_mapped[available], how="left")

    # 3) 외국인 소진율 (flow CSV)
    exh_csv = FLOW_DIR / f"{code}_foreign_exh.csv"
    if exh_csv.exists():
        exh = pd.read_csv(exh_csv, index_col=0, parse_dates=True)
        if "소진율" in exh.columns:
            df = df.join(exh[["소진율"]].rename(columns={"소진율": "외국인_소진율"}), how="left")

    # 수급 컬럼 없으면 0으로 초기화
    for col in ["외국인합계", "기관합계", "개인", "외국인_금액", "기관_금액", "개인_금액", "외국인_소진율"]:
        if col not in df.columns:
            df[col] = 0

    return df


# ============================================================
#  2단계: KIS API로 수급 0값 채우기 (pykrx fallback)
# ============================================================

def fill_supply_from_kis(
    df: pd.DataFrame,
    code: str,
    base_url: str,
    headers: dict,
) -> pd.DataFrame:
    """parquet DataFrame에서 수급 컬럼이 0인 행을 KIS API로 채움

    KIS API는 최근 30일만 → 30일 이내 0값만 교체
    """
    # 투자자 수급 채우기
    inv_df = _fetch_investor_kis(base_url, headers, code)
    if inv_df is not None and len(inv_df) > 0:
        for date_idx in inv_df.index:
            if date_idx in df.index:
                row = inv_df.loc[date_idx]
                # 0이거나 NaN인 수급 컬럼만 교체
                for col in ["외국인합계", "기관합계", "개인", "외국인_금액", "기관_금액", "개인_금액"]:
                    if col in row.index and col in df.columns:
                        current = df.at[date_idx, col]
                        if pd.isna(current) or current == 0:
                            df.at[date_idx, col] = row[col]

    # 외국인 소진율 - 오늘 날짜 행에만 최신값 기록
    frgn = _fetch_foreign_rate_kis(base_url, headers, code)
    if frgn:
        today = pd.Timestamp(datetime.now().strftime("%Y-%m-%d"))
        if today in df.index and "외국인_소진율" in df.columns:
            df.at[today, "외국인_소진율"] = frgn["외국인_소진율"]
        # 오늘 행이 없어도 가장 최근 행에 기록
        elif len(df) > 0 and "외국인_소진율" in df.columns:
            last_date = df.index[-1]
            if pd.isna(df.at[last_date, "외국인_소진율"]) or df.at[last_date, "외국인_소진율"] == 0:
                df.at[last_date, "외국인_소진율"] = frgn["외국인_소진율"]

    return df


# ============================================================
#  3단계: processed parquet 생성 (기술지표 추가)
# ============================================================

def build_processed(df: pd.DataFrame) -> pd.DataFrame:
    """raw → processed: 기술지표 추가"""
    if len(df) < 20:
        return df

    close = df["종가"].astype(float)
    volume = df["거래량"].astype(float)

    # EMA
    df["EMA20"] = close.ewm(span=20, adjust=False).mean()
    df["EMA60"] = close.ewm(span=60, adjust=False).mean()

    # RSI 14
    delta = close.diff()
    gain = delta.where(delta > 0, 0).rolling(14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
    rs = gain / loss.replace(0, np.nan)
    df["RSI14"] = 100 - (100 / (1 + rs))

    # MACD
    ema12 = close.ewm(span=12, adjust=False).mean()
    ema26 = close.ewm(span=26, adjust=False).mean()
    df["MACD"] = ema12 - ema26
    df["MACD_signal"] = df["MACD"].ewm(span=9, adjust=False).mean()
    df["MACD_hist"] = df["MACD"] - df["MACD_signal"]

    # OBV
    obv = pd.Series(0.0, index=df.index)
    for j in range(1, len(df)):
        if close.iloc[j] > close.iloc[j - 1]:
            obv.iloc[j] = obv.iloc[j - 1] + volume.iloc[j]
        elif close.iloc[j] < close.iloc[j - 1]:
            obv.iloc[j] = obv.iloc[j - 1] - volume.iloc[j]
        else:
            obv.iloc[j] = obv.iloc[j - 1]
    df["OBV"] = obv

    # 볼린저 밴드
    sma20 = close.rolling(20).mean()
    std20 = close.rolling(20).std()
    df["BB_upper"] = sma20 + 2 * std20
    df["BB_lower"] = sma20 - 2 * std20

    # 수급 누적 (외국인/기관)
    if "외국인합계" in df.columns:
        df["외국인_누적5"] = df["외국인합계"].rolling(5).sum()
        df["기관_누적5"] = df["기관합계"].rolling(5).sum()

    return df


# ============================================================
#  메인: 전체 유니버스 parquet 빌드
# ============================================================

def _build_single_parquet(
    code: str,
    base_url: Optional[str] = None,
    headers: Optional[dict] = None,
    skip_kis_fill: bool = False,
) -> str:
    """단일 종목 parquet 빌드 (병렬 실행용)

    Returns: 'ok' | 'fail' | 'skip'
    """
    try:
        df = migrate_csv_to_parquet(code)
        if df is None or len(df) < 20:
            return "fail"

        if not skip_kis_fill and base_url and headers:
            df = fill_supply_from_kis(df, code, base_url, headers)
            time.sleep(0.12)

        # ── C2: 원자적 쓰기 (tmp → rename) ──
        raw_path = RAW_DIR / f"{code}.parquet"
        raw_tmp = RAW_DIR / f"{code}.parquet.tmp"
        df.to_parquet(raw_tmp)
        raw_tmp.replace(raw_path)

        proc = build_processed(df.copy())
        proc_path = PROCESSED_DIR / f"{code}.parquet"
        proc_tmp = PROCESSED_DIR / f"{code}.parquet.tmp"
        proc.to_parquet(proc_tmp)
        proc_tmp.replace(proc_path)

        return "ok"
    except Exception as e:
        # C2: 크래시 시 .tmp 잔여 파일 정리
        for tmp in [RAW_DIR / f"{code}.parquet.tmp",
                    PROCESSED_DIR / f"{code}.parquet.tmp"]:
            try:
                tmp.unlink(missing_ok=True)
            except Exception:
                pass
        logger.warning(f"parquet 빌드 실패 {code}: {e}")
        return "fail"


def extend_parquet_all(
    codes: List[str] = None,
    force: bool = False,
    skip_kis_fill: bool = False,
    n_workers: int = 4,
):
    """전체 유니버스 parquet 빌드/갱신 (DC-03: 병렬화)

    1) 기존 CSV → raw parquet 마이그레이션
    2) KIS API로 수급 0값 채우기 (skip_kis_fill=True면 스킵)
    3) processed parquet 생성 (기술지표 추가)

    Args:
        skip_kis_fill: True면 KIS API 재호출 안 함
            flow_collector 직후 호출 시 True → 98분→5분
        n_workers: 병렬 워커 수 (skip_kis_fill=True일 때만 적용)
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    _ensure_dirs()

    # ── C2: 이전 크래시에서 남은 .tmp 파일 정리 ──
    for d in [RAW_DIR, PROCESSED_DIR]:
        for tmp_file in d.glob("*.parquet.tmp"):
            try:
                tmp_file.unlink()
                logger.info(f"[C2] 잔여 tmp 삭제: {tmp_file.name}")
            except Exception:
                pass

    if codes is None:
        from data.kis_collector import UNIVERSE
        codes = list(UNIVERSE.keys())

    print("=" * 60)
    mode = "로컬전용" if skip_kis_fill else "KIS수급포함"
    print(f"  Parquet 통합 빌드 - {len(codes)}종목 ({mode})")
    print("=" * 60)

    # 캐시 확인: 이미 최신 parquet이면 skip
    need_update = []
    cached = 0
    for code in codes:
        raw_file = RAW_DIR / f"{code}.parquet"
        if not force and raw_file.exists():
            try:
                existing = pd.read_parquet(raw_file)
                if len(existing) > 0:
                    last = existing.index[-1]
                    if hasattr(last, 'to_pydatetime'):
                        last = last.to_pydatetime().replace(tzinfo=None)
                    days_old = (datetime.now() - last).days
                    if days_old <= 3:
                        cached += 1
                        continue
            except Exception:
                pass
        need_update.append(code)

    if not need_update:
        print(f"  전체 캐시 히트 ({cached}종목). 갱신 불필요.")
        return cached, 0

    print(f"  갱신 필요: {len(need_update)}종목 (캐시: {cached}종목)")

    base_url, headers = None, None
    if not skip_kis_fill:
        print(f"\n[1/3] KIS API 세션 생성...")
        session = _get_kis_session()
        if session is None:
            logger.warning("[PARQUET] KIS 세션 실패 — 로컬전용 모드 전환")
            skip_kis_fill = True
        else:
            base_url, headers = session
            print(f"  세션 OK")

    t0 = time.time()

    if skip_kis_fill:
        # 병렬 빌드 (KIS 호출 없으므로 rate limit 걱정 없음)
        print(f"\n[2/3] Raw parquet 병렬 빌드 ({n_workers} workers)...")
        built = 0
        failed = 0

        with ThreadPoolExecutor(max_workers=n_workers) as executor:
            futures = {
                executor.submit(_build_single_parquet, code, None, None, True): code
                for code in need_update
            }
            done_count = 0
            # C3: as_completed + 개별 try/except — 하나 실패해도 나머지 계속
            for future in as_completed(futures):
                code = futures[future]
                try:
                    result = future.result()
                except Exception as e:
                    logger.warning(f"[C3] parquet 스레드 실패 {code}: {e}")
                    result = "fail"
                done_count += 1
                if result == "ok":
                    built += 1
                else:
                    failed += 1
                if done_count % 200 == 0:
                    print(f"    [{done_count}/{len(need_update)}] 진행중...")
    else:
        # 순차 빌드 (KIS API rate limit 존재)
        built = 0
        failed = 0
        print(f"\n[2/3] Raw parquet 빌드 + KIS 수급 주입...")
        for i, code in enumerate(need_update):
            if (i + 1) % 50 == 0 or i == 0:
                print(f"    [{i+1}/{len(need_update)}] {code}...")

            result = _build_single_parquet(code, base_url, headers, False)
            if result == "ok":
                built += 1
            else:
                failed += 1

    elapsed = int(time.time() - t0)
    total = built + failed
    fail_pct = f"{failed/total*100:.1f}%" if total > 0 else "0%"
    print(f"\n[3/3] 완료! ({elapsed}초)")
    print(f"={'='*60}")
    print(f"  빌드 성공: {built}종목")
    print(f"  캐시 유지: {cached}종목")
    print(f"  실패:      {failed}종목 ({fail_pct})")
    print(f"  합계:      {built + cached}종목")
    print(f"={'='*60}")
    logger.info(f"Parquet 빌드: {built} 성공, {failed} 실패 ({fail_pct})")

    return built + cached, failed


# ============================================================
#  유틸: parquet 로드
# ============================================================

def load_daily(code: str) -> Optional[pd.DataFrame]:
    """일봉 로드 - parquet 우선, CSV 폴백

    전체 시스템에서 일봉 데이터 읽을 때 이 함수 사용.
    processed parquet → raw parquet → daily CSV 순으로 시도.
    """
    # 1) processed parquet (기술지표 포함)
    proc_path = PROCESSED_DIR / f"{code}.parquet"
    if proc_path.exists():
        try:
            return pd.read_parquet(proc_path)
        except Exception:
            pass

    # 2) raw parquet
    raw_path = RAW_DIR / f"{code}.parquet"
    if raw_path.exists():
        try:
            return pd.read_parquet(raw_path)
        except Exception:
            pass

    # 3) CSV 폴백 (기존 호환)
    csv_path = DAILY_DIR / f"{code}.csv"
    if csv_path.exists():
        return pd.read_csv(csv_path, index_col=0, parse_dates=True)

    return None


def load_raw(code: str) -> Optional[pd.DataFrame]:
    """raw parquet 로드"""
    path = RAW_DIR / f"{code}.parquet"
    if path.exists():
        return pd.read_parquet(path)
    return None


def load_processed(code: str) -> Optional[pd.DataFrame]:
    """processed parquet 로드"""
    path = PROCESSED_DIR / f"{code}.parquet"
    if path.exists():
        return pd.read_parquet(path)
    return None


# ============================================================
#  CLI
# ============================================================

if __name__ == "__main__":
    import argparse
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
    logging.basicConfig(level=logging.WARNING)

    parser = argparse.ArgumentParser(description="Parquet 통합 데이터 빌드")
    parser.add_argument("--codes", nargs="*", help="특정 종목만 (예: 003570 103140)")
    parser.add_argument("--force", action="store_true", help="캐시 무시 강제 갱신")
    args = parser.parse_args()

    extend_parquet_all(codes=args.codes, force=args.force)
