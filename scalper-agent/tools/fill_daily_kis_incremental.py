"""일봉 incremental 보충 — 안전망(손상복구) + 병렬 fetch (6/10 재설계).

배경:
  - pykrx 일봉 경로가 KRX 로그인 전환으로 차단(KRX_ID/PW 요구) → KIS 일봉 API로 대체.
  - collect_daily_kis 는 덮어쓰기(merge X)라 직접 merge(keep='first').

6/10 재설계 (측정 기반):
  - ★측정 확정★ recent_days 축소(20→5)는 시간 단축 0 — KIS 일봉 API는 요청당
    고정 latency ~338ms 지배(응답 행수 무관). 병목 = 종목당 API 1회 × 3651 = ~20분.
  - ★진짜 단축 = 병렬 fetch★ — 직렬은 latency 때문에 초당 ~3건(rate limit 초당 12의 1/4).
    4-way 병렬이면 초당 ~12건 → 20분 → ~5분.
  - ★안전망(손상복구)★ — recent_days 일괄(20일) 대신 종목별 분류:
      NEW/STALE/CORRUPT → 20일 복구 / NORMAL → 증분 / UPTODATE → 스킵.
      날짜역전·행부족·중복·읽기실패 자동 탐지 → 20일 강제 복구(005930 손상 교훈).
  - ★완료 후 검증 step★ — 누락/손상 잔존 0 확인 + 자동 2차 fallback(그 종목만 20일 재시도).

thread-safety(실측 검증 — 가정 아님):
  - mojito 토큰은 __init__에서 1회 발급/캐시 로드 → broker 1개 공유(재발급 race 없음, 5분<24h).
  - ★mojito koreainvestment.py 전체 grep: `self.session`/`= requests.Session` 0건 — 모든 fetch가
    모듈레벨 `requests.get(url,...)` 사용(공유 Session 가변상태 없음). self.access_token만 공유(read-only).★
  - 각 종목 = 독립 CSV 파일(thread별 고유 .tmp) → 파일 write race 없음.
  - 실측(6/10 dry 100종목): 1/4/8-way 크래시 0·ok 정상. 8-way fail+1 = rate limit(초당~12) 신호
    → 4-way 권장(종목당~139ms=초당~7건, rate 안전 마진). 직렬 23.7분 → 4-way 8.5분.
  - ★parallel=4 확정(6/10 사장님)★: 목적=누락0·중복0·손상복구정상·shadow forward 입력 신뢰성
    (속도 최적화 아님). 오늘 데이터가 sector_reversal_shadow·KODEX hedge·gate forward 검증에
    물려 rate limit 누락이 뒤 검증을 흐림. 6-way+는 본작업 제외 — 별도 rate-limit 실험으로 분리.

안전 불변식: keep='first' = 과거 100% 보존(덮어쓰기 금지). SAJANG/매매 무관. 봇 OFF.
"""
import sys
import time
import argparse
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, date
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from data.kis_collector import _get_broker, _parse_daily_data  # noqa: E402
from data.trading_calendar import (  # noqa: E402
    is_trading_day,
    last_trading_day,
    get_trading_days_between,
)

DAILY = Path(__file__).resolve().parent.parent / "data_store" / "daily"


def _read_daily_indexed(f):
    """daily csv → DataFrame. index=DatetimeIndex(ISO8601). 손상 날짜는 NaT.

    format 명시로 dateutil fallback 경고/속도저하 제거(3651종목 누적).
    실 daily는 'YYYY-MM-DD' ISO 저장(kis_collector pd.Timestamp).
    """
    df = pd.read_csv(f, index_col=0)
    df.index = pd.to_datetime(df.index, format="ISO8601", errors="coerce")
    return df


# ── 분류 파라미터 ──
FULL_DAYS = 20        # 복구/신규/누락 (캘린더 일수)
NORMAL_DAYS = 7       # 정상 증분 (캘린더 7일 ≈ 거래일 5개; gap≤5 커버)
MIN_ROWS = 2          # 이하면 CORRUPT
NORMAL_GAP_MAX = 5    # 직전 확정 거래일까지 거래일 gap 이 이하면 NORMAL


# ============================================================
#  안전망 ① — 종목별 fetch window 분류 (순수함수, 네트워크 0)
# ============================================================
def classify_fetch_window(code, daily_dir, asof_date):
    """(recent_days, reason). reason ∈ NEW/NORMAL/STALE/CORRUPT/UPTODATE.

    CORRUPT = 읽기실패·행부족·날짜파싱불가·날짜역전·중복 → 20일 강제 복구.
    """
    f = Path(daily_dir) / f"{code}.csv"
    if not f.exists():
        return FULL_DAYS, "NEW"
    try:
        df = _read_daily_indexed(f)
    except Exception:
        return FULL_DAYS, "CORRUPT"
    if df is None or len(df) < MIN_ROWS:
        return FULL_DAYS, "CORRUPT"
    idx = df.index
    # 날짜 파싱 실패 (NaT 혼입)
    if idx.isna().any():
        return FULL_DAYS, "CORRUPT"
    # 날짜 역전(내림차순/비정렬) 또는 중복
    if (not idx.is_monotonic_increasing) or idx.duplicated().any():
        return FULL_DAYS, "CORRUPT"
    try:
        last_dt = idx.max().date()
    except Exception:
        return FULL_DAYS, "CORRUPT"
    # asof 기준 마지막 확정 거래일
    ref = asof_date if is_trading_day(asof_date) else last_trading_day(asof_date)
    if last_dt > ref:
        return FULL_DAYS, "CORRUPT"       # 미래 ghost row (foreign_exh ghost 전례) → 복구
    if last_dt == ref:
        return NORMAL_DAYS, "UPTODATE"    # 이미 최신 → fetch 스킵
    # gap = last_dt(제외) ~ ref(포함) 사이 거래일 수
    gap = sum(1 for d in get_trading_days_between(last_dt, ref) if d > last_dt)
    if gap <= NORMAL_GAP_MAX:
        return NORMAL_DAYS, "NORMAL"
    return FULL_DAYS, "STALE"


# ============================================================
#  병렬 worker — 단일 종목 (classify → fetch → merge → atomic write)
# ============================================================
def _fetch_one(broker, code, asof_date, today_str, force_full=False, measure_only=False):
    """단일 종목 처리. dict 반환: code/reason/status/added.

    status ∈ ok / skip / fail. force_full=True → UPTODATE/NORMAL 무시하고 20일(회귀·2차).
    measure_only=True → fetch만, 디스크 쓰기 X (장중 안전 측정용).
    """
    recent_days, reason = classify_fetch_window(code, DAILY, asof_date)
    if force_full:
        recent_days, reason = FULL_DAYS, (reason if reason != "UPTODATE" else "RETRY")
    elif reason == "UPTODATE":
        return {"code": code, "reason": reason, "status": "skip", "added": 0}

    start = (datetime.now() - timedelta(days=recent_days)).strftime("%Y%m%d")
    tmp = None
    try:
        resp = broker.fetch_ohlcv_domestic(
            symbol=code, timeframe="D", start_day=start, end_day=today_str
        )
        rows = resp.get("output2", []) if isinstance(resp, dict) else []
        df_new = _parse_daily_data(rows) if rows else None
        if df_new is None or df_new.empty:
            return {"code": code, "reason": reason, "status": "fail", "added": 0}
        df_new.index.name = "날짜"

        if measure_only:
            return {"code": code, "reason": reason, "status": "ok", "added": 0,
                    "rows": len(rows)}

        f = DAILY / f"{code}.csv"
        if f.exists():
            df_old = _read_daily_indexed(f)
            # 손상행(NaT) 제거 → CORRUPT 복구 시 손상날짜 버리고 df_new로 채움
            df_old = df_old[~df_old.index.isna()]
            df_old.index.name = "날짜"
            old_last = df_old.index.max() if len(df_old) else None
            # 컬럼 스키마 정합 (drift 시 NaN 컬럼/정렬 변형 방지)
            if list(df_new.columns) != list(df_old.columns):
                df_new = df_new.reindex(columns=df_old.columns)
            new_dates = df_new.index.difference(df_old.index)  # df_old에 없던 신규일
            combined = pd.concat([df_old, df_new])
            # keep='first' → 기존(old) 우선 보존, 신규 거래일만 추가
            combined = combined[~combined.index.duplicated(keep="first")].sort_index()
            # ★신규 추가 행만★ 등락률 재계산(7일 윈도우 경계 왜곡 교정, 과거 행 불변)
            if len(new_dates) and "등락률" in combined.columns and "종가" in combined.columns:
                full_pct = combined["종가"].pct_change() * 100
                common_new = combined.index.intersection(new_dates)
                combined.loc[common_new, "등락률"] = full_pct.loc[common_new]
        else:
            old_last = None
            combined = df_new.sort_index()

        new_last = combined.index.max()
        # thread별 고유 tmp (동일 종목 동시처리 없으나 재실행 겹침 방어)
        tmp = f.with_suffix(f".tmp{threading.get_ident()}")
        combined.to_csv(tmp)
        tmp.replace(f)
        added = 1 if (old_last is None or new_last > old_last) else 0
        return {"code": code, "reason": reason, "status": "ok", "added": added}
    except Exception as e:  # noqa: BLE001
        if tmp is not None:  # to_csv 성공 후 replace 실패 시 tmp 잔존 방지
            try:
                tmp.unlink(missing_ok=True)
            except Exception:
                pass
        return {"code": code, "reason": reason, "status": "fail", "added": 0,
                "err": str(e)[:60]}


def _run_pool(codes, broker, asof_date, today_str, parallel, force_full=False,
              measure_only=False, sleep_per_call=0.0, log_every=500):
    """ThreadPoolExecutor 실행 → 결과 리스트 + 집계."""
    results = []
    counts = {"ok": 0, "skip": 0, "fail": 0}
    reason_counts = {}
    added = 0
    n = len(codes)
    done = 0

    def task(c):
        r = _fetch_one(broker, c, asof_date, today_str, force_full, measure_only)
        if sleep_per_call:
            time.sleep(sleep_per_call)
        return r

    with ThreadPoolExecutor(max_workers=parallel) as ex:
        futures = [ex.submit(task, c) for c in codes]
        for fut in as_completed(futures):
            r = fut.result()
            results.append(r)
            counts[r["status"]] = counts.get(r["status"], 0) + 1
            reason_counts[r["reason"]] = reason_counts.get(r["reason"], 0) + 1
            added += r.get("added", 0)
            done += 1
            if done % log_every == 0:
                print(f"  진행 {done}/{n} | ok={counts['ok']} skip={counts['skip']} "
                      f"fail={counts['fail']} added={added}", flush=True)
    return results, counts, reason_counts, added


# ============================================================
#  안전망 ② — 완료 후 커버리지 검증 (누락/손상 잔존)
# ============================================================
def verify_coverage(codes, asof_date):
    """각 종목 최신일이 ref(직전 확정 거래일)와 일치하는지.

    Returns: (missing, corrupt) — 둘 다 [(code, detail)] 리스트.
      missing = last_dt < ref (아직 안 채워짐; 단 상폐/거래정지는 정상 잔차일 수 있음)
      corrupt = 구조 손상 잔존(읽기실패·행부족·역전·중복·미래 ghost)

    ★한계(H-2)★: last_dt(끝단) 기준만 검사 — 시계열 내부 hole(중간 거래일 누락)은
      미탐지. classify가 last_dt만 보고 NORMAL 7일 fetch하므로 7일 이전 내부 hole은
      이 step으로 복구 안 됨(끝단 손상 005930류는 잡음). 내부 hole 전수검사는 별도 숙제.
    """
    ref = asof_date if is_trading_day(asof_date) else last_trading_day(asof_date)
    missing, corrupt = [], []
    for code in codes:
        f = DAILY / f"{code}.csv"
        if not f.exists():
            missing.append((code, "NO_FILE"))
            continue
        try:
            df = _read_daily_indexed(f)
        except Exception:
            corrupt.append((code, "READ_FAIL"))
            continue
        idx = df.index
        if (len(df) < MIN_ROWS or idx.isna().any()
                or (not idx.is_monotonic_increasing) or idx.duplicated().any()):
            corrupt.append((code, "STRUCT"))
            continue
        last_dt = idx.max().date()
        if last_dt > ref:
            corrupt.append((code, f"FUTURE:{last_dt}"))   # 미래 ghost row → 복구 대상
        elif last_dt < ref:
            missing.append((code, str(last_dt)))
    return missing, corrupt


# ============================================================
#  메인 fill — 안전망 + 병렬 + 검증 + 자동 2차 fallback
# ============================================================
def fill(codes, asof_date=None, parallel=4, verify=True, sleep_per_call=0.0,
         measure_only=False, log_every=500):
    if asof_date is None:
        asof_date = date.today()
    broker = _get_broker()  # 토큰 1회 발급/로드 (병렬 전)
    today_str = datetime.now().strftime("%Y%m%d")
    n = len(codes)

    mode = "측정(쓰기X)" if measure_only else "실저장"
    ref = asof_date if is_trading_day(asof_date) else last_trading_day(asof_date)
    print(f"대상: {n}종목 | parallel={parallel} | asof={asof_date} "
          f"(ref거래일={ref}) | {mode}", flush=True)

    t0 = time.time()
    results, counts, reason_counts, added = _run_pool(
        codes, broker, asof_date, today_str, parallel,
        measure_only=measure_only, sleep_per_call=sleep_per_call, log_every=log_every)
    elapsed = time.time() - t0

    print(f"\n1차 완료 ({elapsed:.1f}s): ok={counts['ok']} skip={counts['skip']} "
          f"fail={counts['fail']} added={added}", flush=True)
    print(f"  분류: " + " ".join(f"{k}={v}" for k, v in sorted(reason_counts.items())),
          flush=True)

    if measure_only:
        print("  (측정 모드 — 검증/저장 생략)", flush=True)
        return {"elapsed": elapsed, "counts": counts, "reason_counts": reason_counts}

    if not verify:
        return {"elapsed": elapsed, "counts": counts, "reason_counts": reason_counts}

    # ── 검증 step ──
    missing, corrupt = verify_coverage(codes, asof_date)
    print(f"\n검증: missing={len(missing)} corrupt={len(corrupt)}", flush=True)

    # ── 자동 2차 fallback (missing+corrupt 종목만 20일 강제 복구) ──
    retry_codes = [c for c, _ in missing] + [c for c, _ in corrupt]
    if retry_codes:
        print(f"  2차 fallback: {len(retry_codes)}종목 20일 강제 복구...", flush=True)
        t1 = time.time()
        r2, c2, rc2, a2 = _run_pool(
            retry_codes, broker, asof_date, today_str, parallel,
            force_full=True, sleep_per_call=sleep_per_call, log_every=200)
        print(f"  2차 완료 ({time.time()-t1:.1f}s): ok={c2['ok']} fail={c2['fail']} added={a2}",
              flush=True)
        # 2차 후 재검증
        missing, corrupt = verify_coverage(codes, asof_date)
        print(f"  2차 후 재검증: missing={len(missing)} corrupt={len(corrupt)}", flush=True)

    # 잔존 보고 (상폐/거래정지/ETN/스팩은 정상 잔차일 수 있음 — 회귀로 기준선 비교)
    if corrupt:
        print(f"  ⚠ 손상 잔존 {len(corrupt)}종목(샘플): "
              f"{[c for c, _ in corrupt[:10]]}", flush=True)
    if missing:
        print(f"  ⚠ 미충전 잔존 {len(missing)}종목(상폐/정지 가능, 샘플): "
              f"{[(c, d) for c, d in missing[:10]]}", flush=True)

    return {
        "elapsed": elapsed, "counts": counts, "reason_counts": reason_counts,
        "missing": missing, "corrupt": corrupt,
    }


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--codes", default="", help="쉼표구분 종목코드. 없으면 전체 유니버스+daily보유분")
    ap.add_argument("--asof", default="", help="기준일 YYYY-MM-DD. 없으면 오늘")
    ap.add_argument("--parallel", type=int, default=4, help="동시 fetch 수 (rate limit 초당~12 → 4 적정)")
    ap.add_argument("--no-verify", action="store_true", help="완료 후 검증 step 생략")
    ap.add_argument("--measure", action="store_true", help="측정 모드 (fetch만, 디스크 쓰기 X)")
    ap.add_argument("--sleep", type=float, default=0.0, help="호출당 추가 sleep(rate 마진)")
    args = ap.parse_args()

    if args.codes:
        codes = [c.strip() for c in args.codes.split(",") if c.strip()]
    else:
        from collect_all import get_universe_codes
        uni = set(get_universe_codes())
        existing = {p.stem for p in DAILY.glob("*.csv") if not p.stem.startswith("_")}
        codes = sorted(uni | existing)

    asof = date.fromisoformat(args.asof) if args.asof else None
    fill(codes, asof_date=asof, parallel=args.parallel, verify=not args.no_verify,
         measure_only=args.measure, sleep_per_call=args.sleep)
