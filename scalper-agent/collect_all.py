# -*- coding: utf-8 -*-
"""
독립 데이터 수집 스크립트 - 봇 무관하게 매일 자동 실행
=======================================================
봇이 꺼져 있어도 Windows 작업 스케줄러로 매일 16:10에 실행.

수집 항목:
  1. pykrx 일봉 OHLCV (data_store/daily/)
  2. 수급 데이터 - 투자자/외인소진/공매도잔고/공매도거래량 (data_store/flow/, short/)
  2b. ETF 일봉 OHLCV (data_store/etf_daily/) — ~30종목
  3. 국적별 외국인 수급 (data_store/nationality/) - 추천+보유 종목
  4. Parquet 통합 빌드 (data_store/raw/, processed/)
  5. SpaceX 관련주 일일 모니터링 (data_store/spacex_report.json)
  6. stock_data_daily 동기화 (data_store/daily → stock_data_daily)
  7. 데이터 건강성 검증 (open=0 등 이상 데이터 감지)

자동화 안정성 (v2.0):
  - Step-level 체크포인트 → 크래시 시 중단 지점부터 재개 (--resume)
  - 데이터 건강성 자동 검증 (open=0, 커버리지 부족 등)
  - 텔레그램 수집 요약 알림 (완료/실패 자동 발송)
  - 진행률 중간 알림 (주요 Step 완료 시)

사용법:
  python collect_all.py                  # 전체 수집 (당일 기준)
  python collect_all.py --force          # 캐시 무시 강제 수집
  python collect_all.py --resume         # 크래시 지점부터 재개
  python collect_all.py --sync-only      # stock_data_daily 동기화만
"""

import os
import sys
import time
import json
import logging
import argparse
from pathlib import Path
from datetime import datetime, date

# Windows cp949 → UTF-8 강제 (이모지 출력 에러 방지)
if sys.platform == "win32":
    for stream in ("stdout", "stderr"):
        s = getattr(sys, stream, None)
        if s and hasattr(s, "reconfigure"):
            try:
                s.reconfigure(encoding="utf-8", errors="replace")
            except Exception:
                pass

from data.trading_calendar import is_trading_day

# 프로젝트 루트 설정
SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))

# .env 로드
from dotenv import load_dotenv
env_path = SCRIPT_DIR.parent / ".env"
if env_path.exists():
    load_dotenv(env_path)

# 로깅
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(
            SCRIPT_DIR / "logs" / f"collect_{date.today().strftime('%Y%m%d')}.log",
            encoding="utf-8",
        ),
    ],
)
logger = logging.getLogger("Collector")

# 경로
DATA_DIR = SCRIPT_DIR / "data_store"
DAILY_DIR = DATA_DIR / "daily"
STOCK_DATA_DAILY = SCRIPT_DIR.parent / "stock_data_daily"

# 수급 우선수집 기준 (시총 억 단위)
FLOW_PRIORITY_CAP = 1500


def get_universe_codes() -> list:
    """유니버스 종목 코드 리스트 반환"""
    try:
        from data.universe_builder import get_universe_dict
        u = get_universe_dict()
        if u:
            return list(u.keys())
    except Exception as e:
        logger.warning(f"유니버스 로드 실패: {e}")

    # fallback: data_store/daily 폴더의 기존 CSV 파일들
    codes = []
    if DAILY_DIR.exists():
        for f in DAILY_DIR.glob("*.csv"):
            codes.append(f.stem)
    logger.info(f"Fallback: daily 폴더에서 {len(codes)}종목 로드")
    return codes


def get_flow_priority_codes(all_codes: list) -> tuple:
    """수급 우선수집 대상 선별 → (priority_list, remaining_list)

    Phase A (우선): 시총 FLOW_PRIORITY_CAP 이상 + 추천/보유/감시/상한가 이력
    Phase B (잔여): 나머지 소형주 → 메인 파이프라인 비차단 백그라운드 수집
    """
    priority = set()

    # 1) 시총 기준 필터 (universe.json 직접 로드)
    uni_path = DATA_DIR / "universe.json"
    try:
        with open(uni_path, "r", encoding="utf-8") as f:
            uni = json.load(f)
        for code, info in uni.items():
            if isinstance(info, dict) and info.get("cap_억", 0) >= FLOW_PRIORITY_CAP:
                priority.add(code)
        logger.debug(f"[FlowPriority] 시총 {FLOW_PRIORITY_CAP}억+: {len(priority)}종목")
    except Exception:
        pass

    # 2) 추천/보유/감시 종목
    special_files = {
        "recommendation.json": lambda d: [s.get("code", "") for s in d.get("stocks", [])],
        "watchlist.json": lambda d: [
            (i.get("code", "") if isinstance(i, dict) else "")
            for i in (d if isinstance(d, list) else [])
        ],
    }
    for fname, extractor in special_files.items():
        fp = DATA_DIR / fname
        if fp.exists():
            try:
                with open(fp, "r", encoding="utf-8") as f:
                    data = json.load(f)
                codes_from = [c for c in extractor(data) if c]
                priority.update(codes_from)
            except Exception:
                pass

    # 보유 종목 (봇 포지션)
    for pos_file in ["swing_candidates.json", "nxt_positions.json"]:
        fp = DATA_DIR / pos_file
        if fp.exists():
            try:
                with open(fp, "r", encoding="utf-8") as f:
                    data = json.load(f)
                if isinstance(data, dict):
                    if "candidates" in data and isinstance(data["candidates"], dict):
                        priority.update(k for k in data["candidates"] if k[:1].isdigit())
                    else:
                        priority.update(k for k in data if k[:1].isdigit())
                elif isinstance(data, list):
                    for item in data:
                        if isinstance(item, dict) and "code" in item:
                            priority.add(item["code"])
            except Exception:
                pass

    # 3) 상한가 엔진 감시/이력 종목
    lu_dir = DATA_DIR / "limit_up"
    for lu_file in ["watchlist.json", "candidates.json", "signals.json"]:
        fp = lu_dir / lu_file
        if fp.exists():
            try:
                with open(fp, "r", encoding="utf-8") as f:
                    data = json.load(f)
                if isinstance(data, dict):
                    # watchlist: items 안에 리스트
                    items = data.get("items", data.get("signals", []))
                    if isinstance(items, list):
                        for item in items:
                            if isinstance(item, dict) and "code" in item:
                                priority.add(item["code"])
                    # candidates: code 키 직접
                    for k in data:
                        if isinstance(k, str) and k[:1].isdigit() and len(k) == 6:
                            priority.add(k)
            except Exception:
                pass

    # 4) 핵심 종목 항상 포함
    priority.update(["005930", "000660", "003570", "103140"])

    # 5) SpaceX Tier 1~2
    try:
        from data.spacex_watchlist import SPACEX_STOCKS
        priority.update(c for c, m in SPACEX_STOCKS.items() if m.get("tier", 9) <= 2)
    except Exception:
        pass

    # all_codes 순서 유지하며 분리
    priority_list = [c for c in all_codes if c in priority]
    remaining_list = [c for c in all_codes if c not in priority]

    logger.info(
        f"[FlowPriority] 우선 {len(priority_list)}종목 "
        f"(시총{FLOW_PRIORITY_CAP}억+ {sum(1 for c in priority_list if c in priority)}개 "
        f"+ 추천/감시/상한가) | 잔여 {len(remaining_list)}종목"
    )
    return priority_list, remaining_list


def step1_daily_ohlcv(codes: list, force: bool = False):
    """1단계: pykrx 일봉 수집"""
    logger.info(f"[1/6] 일봉 수집 시작: {len(codes)}종목 (force={force})")
    t0 = time.time()
    try:
        from data.universe_builder import collect_daily_pykrx
        cnt = collect_daily_pykrx(codes, months=24, force=force)
        logger.info(f"[1/6] 일봉 완료: {cnt}종목 ({int(time.time()-t0)}초)")
        return cnt
    except Exception as e:
        logger.error(f"[1/6] 일봉 실패: {e}")
        return 0


def step2_supply_demand(codes: list, force: bool = False):
    """2단계: 수급 데이터 (투자자/외인소진/공매도)"""
    logger.info(f"[2/6] 수급 수집 시작: {len(codes)}종목")
    t0 = time.time()
    results = {}
    try:
        from data.flow_collector import (
            collect_investor_flow,
            collect_foreign_exhaustion,
            collect_short_balance,
            collect_short_volume,
            _write_flow_marker,
        )
        inv = collect_investor_flow(codes, 24, force)
        fex = collect_foreign_exhaustion(codes, 24, force)
        sbal = collect_short_balance(codes, 24, force)
        svol = collect_short_volume(codes, 24, force)
        results["investor"] = len(inv)
        results["foreign_exh"] = len(fex)
        results["short_bal"] = len(sbal)
        results["short_vol"] = len(svol)
        # AUTO-RECOVERY 검증용 마커 기록
        _write_flow_marker({
            "investor": inv, "foreign_exhaustion": fex,
            "short_balance": sbal, "short_volume": svol,
        })
        logger.info(f"[2/6] 수급 완료: {results} ({int(time.time()-t0)}초)")
    except Exception as e:
        logger.error(f"[2/6] 수급 실패: {e}")
    return results


def step2b_etf_ohlcv(force: bool = False):
    """2b단계: ETF 일봉 수집 (pykrx → data_store/etf_daily/)"""
    logger.info("[2b] ETF 일봉 수집 시작...")
    t0 = time.time()
    try:
        from data.etf_universe import collect_etf_daily
        cnt = collect_etf_daily(months=12, force=force)
        logger.info(f"[2b] ETF 일봉 완료: {cnt}종목 ({int(time.time()-t0)}초)")
        return cnt
    except Exception as e:
        logger.error(f"[2b] ETF 일봉 실패: {e}")
        return 0


def _get_top200_codes() -> list:
    """시총 상위 200종목 코드 반환 (universe.json cap_억 기준)"""
    try:
        uni_path = DATA_DIR / "universe.json"
        if not uni_path.exists():
            return []
        with open(uni_path, "r", encoding="utf-8") as f:
            uni = json.load(f)
        items = []
        for code, info in uni.items():
            if isinstance(info, dict):
                cap = info.get("cap_억", 0) or 0
            else:
                cap = 0
            items.append((code, cap))
        items.sort(key=lambda x: x[1], reverse=True)
        return [code for code, _ in items[:200]]
    except Exception as e:
        logger.warning(f"TOP200 로드 실패: {e}")
        return []


def _run_nationality_fetch(nat_codes: list, snap_date: str):
    """국적별 배치 크롤링 실행 + 스냅샷 저장. (ok, snap_saved) 반환."""
    import asyncio
    from data.krx_nationality_crawler import afetch_nationality_batch
    from data.nationality_signal import save_daily_snapshot

    try:
        results = asyncio.run(afetch_nationality_batch(nat_codes, snap_date, snap_date))
    except RuntimeError:
        loop = asyncio.new_event_loop()
        try:
            results = loop.run_until_complete(
                afetch_nationality_batch(nat_codes, snap_date, snap_date))
        finally:
            loop.close()

    ok = 0
    snap_saved = 0
    for code, df in results.items():
        if df is not None and not df.empty:
            ok += 1
            try:
                snap_data = {}
                if "국가명" in df.columns and "거래량" in df.columns:
                    for _, row in df.iterrows():
                        country = str(row.get("국가명", "")).strip()
                        vol = int(row.get("거래량", 0))
                        if country:
                            snap_data[country] = snap_data.get(country, 0) + vol
                if snap_data:
                    save_daily_snapshot(code, snap_date, snap_data)
                    snap_saved += 1
            except Exception as e:
                logger.debug(f"스냅샷 저장 실패 {code}: {e}")
    return ok, snap_saved


def step3_nationality(force: bool = False):
    """3단계: 국적별 외국인 수급 (TOP200 + 추천 + 보유 + 감시 종목)

    수집 대상:
      1) 시총 TOP200 (universe.json cap_억 기준)
      2) 추천 종목 (recommendation.json)
      3) 보유 종목 (swing_candidates, nxt_positions)
      4) 감시 종목 (watchlist.json)
      5) 핵심 종목 (삼전/하닉/LG화학/풍산)

    데이터 흐름:
      - 1차: afetch_nationality_batch() → 5일 집계 (nationality_{code}.csv)
      - 2차: 단일일 스냅샷 ({code}_{date}.csv) — 동일 세션 재사용
    """
    logger.info("[3/6] 국적별 수급 수집...")
    t0 = time.time()
    nat_codes = set()

    # 1) 시총 TOP200
    top200 = _get_top200_codes()
    nat_codes.update(top200)
    logger.info(f"[3/6] TOP200: {len(top200)}종목")

    # 2) 추천 종목
    rec_path = DATA_DIR / "recommendation.json"
    if rec_path.exists():
        try:
            with open(rec_path, "r", encoding="utf-8") as f:
                rec = json.load(f)
            for s in rec.get("stocks", []):
                c = s.get("code", "")
                if c:
                    nat_codes.add(c)
        except Exception:
            pass

    # 3) 보유 종목 (봇 포지션)
    for pos_file in ["swing_candidates.json", "nxt_positions.json"]:
        fp = DATA_DIR / pos_file
        if fp.exists():
            try:
                with open(fp, "r", encoding="utf-8") as f:
                    data = json.load(f)
                if isinstance(data, dict):
                    # candidates 키 안의 실제 종목코드만 추출 (메타키 제외)
                    if "candidates" in data and isinstance(data["candidates"], dict):
                        nat_codes.update(
                            k for k in data["candidates"].keys()
                            if k[:1].isdigit()
                        )
                    else:
                        nat_codes.update(
                            k for k in data.keys() if k[:1].isdigit()
                        )
                elif isinstance(data, list):
                    for item in data:
                        if isinstance(item, dict) and "code" in item:
                            nat_codes.add(item["code"])
            except Exception:
                pass

    # 4) 감시 종목 (watchlist)
    wl_path = DATA_DIR / "watchlist.json"
    if wl_path.exists():
        try:
            with open(wl_path, "r", encoding="utf-8") as f:
                wl = json.load(f)
            for item in wl:
                c = item.get("code", "") if isinstance(item, dict) else ""
                if c:
                    nat_codes.add(c)
        except Exception:
            pass

    # 5) 핵심 종목 항상 포함
    for code in ["005930", "000660", "003570", "103140"]:
        nat_codes.add(code)

    # 6) SpaceX 관련주 (Tier 1~2) 항상 포함
    try:
        from data.spacex_watchlist import SPACEX_STOCKS
        sx_codes = [c for c, m in SPACEX_STOCKS.items() if m.get("tier", 9) <= 2]
        nat_codes.update(sx_codes)
        logger.info(f"[3/6] SpaceX Tier1~2: {len(sx_codes)}종목 추가")
    except Exception:
        pass

    if not nat_codes:
        logger.info("[3/6] 국적별: 대상 종목 없음, 스킵")
        return 0

    nat_codes = list(nat_codes)
    logger.info(f"[3/6] 국적별 대상: {len(nat_codes)}종목 (TOP200+추천+보유+감시)")

    try:
        from data.nationality_signal import _get_latest_data_date

        snap_date = _get_latest_data_date()

        # 1차: 단일일 fetch → 스냅샷 직접 저장 (collect_daily_snapshots 대체)
        # 세션 1회 생성, 모든 종목에 재사용 → 세션 만료 문제 방지
        ok, snap_saved = _run_nationality_fetch(nat_codes, snap_date)

        # 0건 폴백: snap_date 데이터 미반영 → 전거래일로 재시도
        if ok == 0 and len(nat_codes) > 0:
            from data.trading_calendar import last_trading_day
            from datetime import datetime as _dt
            fallback = last_trading_day(
                _dt.strptime(snap_date, "%Y%m%d").date()
            ).strftime("%Y%m%d")
            if fallback != snap_date:
                logger.warning(
                    f"[3/6] 국적별 0건 → 전거래일({fallback}) 폴백 재시도")
                ok, snap_saved = _run_nationality_fetch(nat_codes, fallback)
                snap_date = fallback

        elapsed = int(time.time() - t0)
        logger.info(f"[3/6] 국적별 완료: {ok}/{len(nat_codes)} | 스냅샷: {snap_saved} ({snap_date}) | {elapsed}초")

        # 재발방지: 스냅샷 0건이면 명시적 경고
        if snap_saved == 0 and len(nat_codes) > 0:
            logger.warning(f"[3/6] ⚠ 국적별 스냅샷 0건! KRX 세션 또는 API 장애 의심")

        return ok
    except Exception as e:
        logger.error(f"[3/6] 국적별 실패: {e}")
        return 0


def step4_parquet_build():
    """4단계: Parquet 통합 빌드 (DC-03: KIS 스킵 + 병렬)"""
    logger.info("[4/6] Parquet 빌드 (로컬전용+병렬)...")
    t0 = time.time()
    try:
        from data.extend_parquet_data import extend_parquet_all
        # flow_collector 직후 → KIS 재호출 불필요, 병렬 빌드
        ok, fail = extend_parquet_all(codes=None, force=True,
                                       skip_kis_fill=True, n_workers=4)
        logger.info(f"[4/6] Parquet 완료: 성공 {ok} / 실패 {fail} ({int(time.time()-t0)}초)")
        return ok
    except Exception as e:
        logger.error(f"[4/6] Parquet 실패: {e}")
        return 0


def step5b_limit_up_scan():
    """5b단계: 상한가 스캐너 (당일 상한가/급등 + 순차급등 패턴)"""
    logger.info("[5b] 상한가 스캐너 실행...")
    t0 = time.time()
    try:
        from data.limit_up_scanner import (
            scan_limit_up, scan_sequential_push, score_continuation,
            analyze_short_proxy, LIMIT_UP_DIR, LIMIT_UP_CANDIDATES,
        )
        from dataclasses import asdict

        uni_path = DATA_DIR / "universe.json"
        if not uni_path.exists():
            logger.warning("[5b] universe.json 없음, 스킵")
            return 0

        with open(uni_path, "r", encoding="utf-8") as f:
            universe = json.load(f)

        # 상한가/급등 스캔
        limit_stocks = scan_limit_up(universe=universe)
        for s in limit_stocks:
            sp = getattr(s, '_short_proxy', None)
            s.score = score_continuation(s, short_proxy=sp)
        limit_stocks.sort(key=lambda x: -x.score)

        # 순차급등 패턴 스캔
        seq_stocks = scan_sequential_push(universe=universe)

        # 저장
        LIMIT_UP_DIR.mkdir(parents=True, exist_ok=True)
        candidates = {
            "scanned_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "limit_up": [asdict(s) for s in limit_stocks],
            "sequential_push": [asdict(s) for s in seq_stocks[:30]],
        }
        with open(LIMIT_UP_CANDIDATES, "w", encoding="utf-8") as f:
            json.dump(candidates, f, ensure_ascii=False, indent=2, default=str)

        cnt = len(limit_stocks) + len(seq_stocks)
        logger.info(
            f"[5b] 상한가 스캔 완료: 상한가 {len(limit_stocks)}건 + "
            f"순차급등 {len(seq_stocks)}건 ({int(time.time()-t0)}초)"
        )
        return cnt
    except Exception as e:
        logger.error(f"[5b] 상한가 스캔 실패: {e}")
        return 0


def step5c_limit_up_engine():
    """5c단계: 상한가 눌림목 엔진 (시그널 감지 + 감시풀 관리)"""
    logger.info("[5c] 상한가 눌림목 엔진 실행...")
    t0 = time.time()
    try:
        from data.limit_up_engine import run_daily
        result = run_daily(send_telegram=True)
        new_sigs = result.get("new_signals", 0)
        triggered = result.get("triggered", 0)
        watch_cnt = result.get("watchlist_count", 0)
        logger.info(
            f"[5c] 엔진 완료: 신규시그널 {new_sigs}건 | "
            f"트리거 {triggered}건 | 감시풀 {watch_cnt}건 "
            f"({int(time.time()-t0)}초)"
        )
        return result
    except Exception as e:
        logger.error(f"[5c] 상한가 엔진 실패: {e}")
        return {"error": str(e)}


def step5d_limit_up_paper():
    """5d단계: 상한가 페이퍼 트레이딩 (시그널 → 가상매매)"""
    logger.info("[5d] 상한가 페이퍼 트레이딩 실행...")
    t0 = time.time()
    try:
        from data.limit_up_paper_trader import run_paper_trading
        result = run_paper_trading(send_telegram=True)
        entries = result.get("new_entries", 0)
        closed = result.get("closed", 0)
        active = result.get("active", 0)
        logger.info(
            f"[5d] 페이퍼 완료: 진입 {entries}건 | "
            f"청산 {closed}건 | 보유 {active}건 "
            f"({int(time.time()-t0)}초)"
        )
        return result
    except Exception as e:
        logger.error(f"[5d] 페이퍼 트레이딩 실패: {e}")
        return {"error": str(e)}


def step5e_limit_up_upload():
    """5e단계: 상한가 시그널 + 성적표 FLOWX 업로드"""
    logger.info("[5e] 상한가 FLOWX 업로드...")
    t0 = time.time()
    try:
        from data.upload_limit_up import upload_all
        result = upload_all()
        sig_ok = result.get("signals", False)
        perf_ok = result.get("performance", False)
        logger.info(
            f"[5e] 업로드 완료: 시그널 {'OK' if sig_ok else 'FAIL'} | "
            f"성적 {'OK' if perf_ok else 'FAIL'} ({int(time.time()-t0)}초)"
        )
        return result
    except Exception as e:
        logger.error(f"[5e] FLOWX 업로드 실패: {e}")
        return {"error": str(e)}


def step5f_tipping_point():
    """5f단계: 수급 임계점 스캔 + FLOWX 업로드"""
    logger.info("[5f] 수급 임계점 스캔...")
    t0 = time.time()
    try:
        from data.tipping_point_scanner import scan_tipping_point
        scan = scan_tipping_point()
        coiled = len(scan.get("coiled", []))
        warming = len(scan.get("warming", []))
        logger.info(
            f"[5f] 스캔 완료: 코일 {coiled} / 점화 {warming} "
            f"({int(time.time()-t0)}초)"
        )

        # Supabase 업로드
        from data.upload_tipping_scan import upload_tipping_scan
        ok = upload_tipping_scan(scan)
        logger.info(f"[5f] 업로드 {'OK' if ok else 'FAIL'}")
        return {"coiled": coiled, "warming": warming, "uploaded": ok}
    except Exception as e:
        logger.error(f"[5f] 수급 임계점 실패: {e}")
        return {"error": str(e)}


def step5_spacex_report():
    """5단계: SpaceX 관련주 일일 모니터링 리포트"""
    logger.info("[5/6] SpaceX 관련주 리포트 생성...")
    t0 = time.time()
    try:
        from data.spacex_watchlist import generate_spacex_report
        report = generate_spacex_report()
        cnt = len(report.get("stocks", []))
        avg = report.get("summary", {}).get("avg_change", 0)
        logger.info(f"[5/6] SpaceX 리포트 완료: {cnt}종목, 평균 {avg:+.2f}% ({int(time.time()-t0)}초)")
        return cnt
    except Exception as e:
        logger.error(f"[5/6] SpaceX 리포트 실패: {e}")
        return 0


def step8_us_overnight():
    """8단계: 미국장 야간 데이터 수집 (us_market_overnight.json)

    5/19 점검 결과: A11 _job_us_overnight_filter가 G1에서 실패 시
    logger.warning만 찍고 묵음 통과 → us_market_overnight.json이 5/17 이후 stale.
    여기 collect_all 본 사이클에 등록하여 매 거래일 안정적으로 갱신한다.

    출력: us_market_overnight.json + us_overnight_result.json
    """
    logger.info("[8/9] 미국장 야간 수집...")
    t0 = time.time()
    try:
        from data.us_market_collector import collect_us_overnight
        from data.us_overnight_filter import run as run_us_filter

        us_data = collect_us_overnight()
        if not us_data:
            logger.warning("[8] US 데이터 수집 빈 결과")
            return {"us_overnight": "NO_DATA"}

        report = run_us_filter()
        mode = (report or {}).get("mode", "UNKNOWN")
        gap = (report or {}).get("gap_est_pct", 0.0)
        logger.info(
            f"[8/9] US 야간 수집 완료: mode={mode}, gap={gap:+.2f}% ({int(time.time()-t0)}초)"
        )
        return {"us_overnight": "OK", "us_mode": mode, "gap_pct": gap}
    except Exception as e:
        logger.error(f"[8] US 야간 수집 실패: {e}")
        return {"us_overnight": f"ERROR: {e}"}


def step9_volume_scan():
    """9단계: 거래량 이상거래 스캔 (data_store/scan_results/)

    5/19 점검 결과: volume_scanner가 3/4 이후 미가동 — scan_results 폴더
    일별 갱신 멈춤. collect_all 본 사이클에 등록해 매 거래일 자동 생성.

    출력: scan_results/volume_spikes_YYYYMMDD.json + extra_universe.json
    """
    logger.info("[9/10] 거래량 이상거래 스캔...")
    t0 = time.time()
    try:
        from data.volume_scanner import scan_universe, save_results

        results = scan_universe(top_n=30)
        if not results:
            logger.warning("[9] Volume scan: 결과 0건 (감지 패턴 없음)")
            return {"volume_scan": 0}
        save_results(results)
        logger.info(
            f"[9/10] Volume scan 완료: {len(results)}건 ({int(time.time()-t0)}초)"
        )
        return {"volume_scan": len(results)}
    except Exception as e:
        logger.error(f"[9] Volume scan 실패: {e}")
        return {"volume_scan": f"ERROR: {e}"}


def step10_gainer_analysis():
    """10단계: 금일 5%+ 상승종목 시그널 역추적 분석 (today_gainers_analyzed.json)

    5/19 점검 결과: tools/gainer_analysis.py가 2/19 이후 미가동.
    정보봇 P0 today_gainers.json 공급 정상 + P1 ±30% suspicious 필터링 완료 후,
    단타봇 측 책임인 분석 단계를 collect_all에 등록해 매 거래일 자동 실행.

    구현 메모:
      - gainer_analysis.py가 top-level 스크립트 (함수 0개) + os.chdir 부작용
        → subprocess.run으로 격리 실행 (import 부작용 차단)
      - today_gainers.json 부재 시 그레이스풀 스킵 (정보봇 sync 지연 대응)
    """
    logger.info("[10/10] Gainer 시그널 역추적 분석...")
    t0 = time.time()
    try:
        import subprocess

        # 정보봇 P0 산출물 존재 확인 (sync 지연 시 스킵)
        gainers_path = DATA_DIR / "today_gainers.json"
        if not gainers_path.exists():
            logger.warning("[10] today_gainers.json 부재 — 정보봇 sync 대기 후 스킵")
            return {"gainer_analysis": "SKIP_NO_INPUT"}

        # 입력 파일 신선도 체크 (24h 초과 시 stale 경고)
        age_h = (time.time() - gainers_path.stat().st_mtime) / 3600
        if age_h > 36:
            logger.warning(
                f"[10] today_gainers.json stale: {age_h:.1f}h 전 — 분석 스킵"
            )
            return {"gainer_analysis": f"SKIP_STALE_{age_h:.0f}h"}

        # subprocess 호출 (별도 프로세스로 격리 — os.chdir 부작용 차단)
        # 5/19 자아성찰: Windows에서 stderr가 cp949로 떨어지는 문제 방지
        # → PYTHONIOENCODING=utf-8 환경변수로 자식 프로세스 stdout/stderr 인코딩 강제
        child_env = dict(os.environ)
        child_env["PYTHONIOENCODING"] = "utf-8"

        result = subprocess.run(
            [sys.executable, "tools/gainer_analysis.py"],
            cwd=str(SCRIPT_DIR),
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=300,
            env=child_env,
        )

        if result.returncode != 0:
            logger.error(
                f"[10] gainer_analysis exit={result.returncode} | "
                f"stderr: {result.stderr[:300]}"
            )
            return {"gainer_analysis": f"ERROR_RC{result.returncode}"}

        # 산출물 검증
        analyzed_path = DATA_DIR / "today_gainers_analyzed.json"
        if analyzed_path.exists():
            with open(analyzed_path, "r", encoding="utf-8") as f:
                analyzed = json.load(f)
            cnt = len(analyzed) if isinstance(analyzed, list) else 0
            logger.info(
                f"[10/10] Gainer 분석 완료: {cnt}종목 ({int(time.time()-t0)}초)"
            )
            return {"gainer_analysis": "OK", "analyzed_count": cnt}
        else:
            logger.warning("[10] today_gainers_analyzed.json 산출물 미생성")
            return {"gainer_analysis": "NO_OUTPUT"}
    except subprocess.TimeoutExpired:
        logger.error("[10] gainer_analysis timeout (5분 초과)")
        return {"gainer_analysis": "TIMEOUT"}
    except Exception as e:
        logger.error(f"[10] Gainer 분석 실패: {e}")
        return {"gainer_analysis": f"ERROR: {e}"}


def step6_sync_stock_data_daily():
    """6단계: data_store/daily → stock_data_daily 동기화

    stock_data_daily 폴더는 이름_코드.csv 형식으로 저장됨.
    data_store/daily의 pykrx 데이터를 stock_data_daily 형식에 맞게 병합.
    """
    logger.info("[6/6] stock_data_daily 동기화...")
    if not STOCK_DATA_DAILY.exists():
        logger.warning(f"[6/6] {STOCK_DATA_DAILY} 폴더 없음, 스킵")
        return 0

    import pandas as pd

    # 유니버스에서 이름 매핑
    name_map = {}
    try:
        from data.universe_builder import get_universe_dict
        u = get_universe_dict()
        if u:
            for code, info in u.items():
                if isinstance(info, (list, tuple)) and len(info) > 0:
                    name_map[code] = info[0]
                elif isinstance(info, dict):
                    name_map[code] = info.get("name", code)
    except Exception:
        pass

    # stock_data_daily에 있는 기존 파일들에서 이름 매핑 보완
    for f in STOCK_DATA_DAILY.glob("*_*.csv"):
        parts = f.stem.rsplit("_", 1)
        if len(parts) == 2:
            name, code = parts
            if code not in name_map:
                name_map[code] = name

    synced = 0
    for src in DAILY_DIR.glob("*.csv"):
        code = src.stem
        if code.startswith("_"):
            continue

        name = name_map.get(code, code)
        dst = STOCK_DATA_DAILY / f"{name}_{code}.csv"

        try:
            # pykrx 데이터 읽기 (인덱스=날짜, 컬럼: 시가/고가/저가/종가/거래량/등락률)
            df_new = pd.read_csv(src, index_col=0, parse_dates=True)
            if df_new.empty:
                continue

            # 컬럼 표준화 → stock_data_daily 형식 (open,high,low,close,volume)
            col_map = {}
            for c in df_new.columns:
                cl = c.lower().strip()
                if cl in ("시가", "open"):
                    col_map[c] = "open"
                elif cl in ("고가", "high"):
                    col_map[c] = "high"
                elif cl in ("저가", "low"):
                    col_map[c] = "low"
                elif cl in ("종가", "close"):
                    col_map[c] = "close"
                elif cl in ("거래량", "volume"):
                    col_map[c] = "volume"
            df_new = df_new.rename(columns=col_map)

            needed = ["open", "high", "low", "close", "volume"]
            if not all(c in df_new.columns for c in needed):
                continue

            df_new = df_new[needed]

            if dst.exists():
                df_old = pd.read_csv(dst, index_col=0, parse_dates=True)
                # 기존 데이터의 컬럼도 표준화
                old_map = {}
                for c in df_old.columns:
                    cl = c.lower().strip()
                    if cl in ("시가", "open"):
                        old_map[c] = "open"
                    elif cl in ("고가", "high"):
                        old_map[c] = "high"
                    elif cl in ("저가", "low"):
                        old_map[c] = "low"
                    elif cl in ("종가", "close"):
                        old_map[c] = "close"
                    elif cl in ("거래량", "volume"):
                        old_map[c] = "volume"
                if old_map:
                    df_old = df_old.rename(columns=old_map)

                if all(c in df_old.columns for c in needed):
                    df_old = df_old[needed]
                    # 새 데이터만 추가 (중복 날짜 제거)
                    combined = pd.concat([df_old, df_new])
                    combined = combined[~combined.index.duplicated(keep="last")]
                    combined = combined.sort_index()
                    df_new = combined

            tmp_dst = dst.with_suffix(".tmp")
            df_new.to_csv(tmp_dst)
            tmp_dst.replace(dst)
            synced += 1
        except Exception as e:
            if synced < 3:  # 처음 몇 개만 로깅
                logger.debug(f"동기화 실패 {code}: {e}")

    logger.info(f"[6/6] stock_data_daily 동기화 완료: {synced}종목")
    return synced


def _check_bot_already_collected() -> bool:
    """봇이 오늘 이미 수집했는지 확인 (_last_collect.json)"""
    lc_path = DATA_DIR / "_last_collect.json"
    if not lc_path.exists():
        return False
    try:
        with open(lc_path, "r", encoding="utf-8") as f:
            info = json.load(f)
        return info.get("date") == date.today().strftime("%Y-%m-%d")
    except Exception:
        return False


def _save_collect_result(steps: dict):
    """수집 완료 기록"""
    lc_path = DATA_DIR / "_last_collect.json"
    try:
        info = {
            "date": date.today().strftime("%Y-%m-%d"),
            "source": "scheduler",
            "time": datetime.now().strftime("%H:%M:%S"),
            "steps": steps,
        }
        with open(lc_path, "w", encoding="utf-8") as f:
            json.dump(info, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.warning(f"수집 기록 저장 실패: {e}")


# ═══════════════════════════════════════════════════════
#  자동화 안정성 v2.0: 체크포인트 / 건강성검증 / 텔레그램
# ═══════════════════════════════════════════════════════

CHECKPOINT_PATH = DATA_DIR / "_collect_checkpoint.json"

# Step 실행 순서 (체크포인트 기준)
STEP_ORDER = [
    "step1_2",            # 일봉 + 수급 + ETF (동시)
    "step3",              # 국적별
    "step4",              # Parquet
    "step5_spacex",       # SpaceX
    "step5b",             # 상한가 스캐너
    "step5c",             # 상한가 엔진
    "step5d",             # 페이퍼 트레이딩
    "step5e",             # FLOWX 업로드
    "step5f",             # 수급 임계점
    "step6",              # sync
    "step8_us_overnight", # 미국장 야간 (5/19 점검 추가)
    "step9_volume_scan",  # 거래량 이상거래 (5/19 점검 추가)
    "step10_gainer_analysis",  # 5%+ 상승 시그널 역추적 (5/19 점검 추가)
    "step7",              # 건강성 검증
]


def _save_checkpoint(step: str, result, elapsed: int = 0):
    """Step 완료 시 체크포인트 저장 — 크래시 복구용"""
    try:
        cp = {}
        if CHECKPOINT_PATH.exists():
            with open(CHECKPOINT_PATH, "r", encoding="utf-8") as f:
                cp = json.load(f)
        # 날짜가 다르면 초기화
        if cp.get("date") != date.today().strftime("%Y-%m-%d"):
            cp = {"date": date.today().strftime("%Y-%m-%d"), "completed": {}}
        cp["completed"][step] = {
            "result": str(result)[:200],
            "time": datetime.now().strftime("%H:%M:%S"),
            "elapsed": elapsed,
        }
        cp["last_step"] = step
        with open(CHECKPOINT_PATH, "w", encoding="utf-8") as f:
            json.dump(cp, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.warning(f"체크포인트 저장 실패: {e}")


def _get_resume_step() -> str | None:
    """크래시 복구: 오늘 마지막 완료 Step 다음부터 재개할 Step 반환"""
    if not CHECKPOINT_PATH.exists():
        return None
    try:
        with open(CHECKPOINT_PATH, "r", encoding="utf-8") as f:
            cp = json.load(f)
        if cp.get("date") != date.today().strftime("%Y-%m-%d"):
            return None
        last = cp.get("last_step")
        if not last or last not in STEP_ORDER:
            return None
        idx = STEP_ORDER.index(last)
        if idx + 1 >= len(STEP_ORDER):
            return None  # 모든 Step 완료됨
        return STEP_ORDER[idx + 1]
    except Exception:
        return None


def _should_skip(step: str, resume_from: str | None) -> bool:
    """resume 모드에서 이미 완료된 Step은 스킵"""
    if resume_from is None:
        return False
    if step not in STEP_ORDER or resume_from not in STEP_ORDER:
        return False
    return STEP_ORDER.index(step) < STEP_ORDER.index(resume_from)


def step7_health_check(results: dict) -> dict:
    """7단계: 수집 데이터 건강성 검증

    검증 항목:
      1. 일봉 OHLCV 커버리지 (universe 대비)
      2. 시가=0 이상 데이터 감지
      3. 수급 데이터 커버리지
      4. 최신 날짜 정합성
    """
    logger.info("[7/7] 데이터 건강성 검증...")
    t0 = time.time()
    health = {"issues": [], "warnings": [], "ok": []}

    today_str = date.today().strftime("%Y-%m-%d")

    # 1) 일봉 커버리지 체크
    try:
        daily_files = list(DAILY_DIR.glob("*.csv"))
        daily_count = len(daily_files)
        if daily_count < 2500:
            health["issues"].append(f"일봉 파일 부족: {daily_count}개 (기대: 2900+)")
        else:
            health["ok"].append(f"일봉: {daily_count}종목")
    except Exception as e:
        health["issues"].append(f"일봉 검증 실패: {e}")

    # 2) 시가=0 이상 데이터 감지
    try:
        open_zero_codes = []
        sample_count = 0
        for f in DAILY_DIR.glob("*.csv"):
            try:
                with open(f, "r", encoding="utf-8") as fp:
                    lines = fp.readlines()
                if len(lines) < 2:
                    continue
                last = lines[-1].strip().split(",")
                if last[0] == today_str and len(last) >= 5:
                    sample_count += 1
                    if float(last[1]) == 0 and float(last[4]) > 0:
                        open_zero_codes.append(f.stem)
            except Exception:
                continue
        if open_zero_codes:
            health["warnings"].append(
                f"시가=0 이상: {len(open_zero_codes)}종목 "
                f"(예: {', '.join(open_zero_codes[:5])})"
            )
        if sample_count > 0:
            health["ok"].append(f"오늘 데이터: {sample_count}종목 검증")
    except Exception as e:
        health["warnings"].append(f"시가=0 검증 실패: {e}")

    # 3) 수급 커버리지 체크
    try:
        flow_dir = DATA_DIR / "flow"
        inv_files = list(flow_dir.glob("*_investor.csv"))
        exh_files = list(flow_dir.glob("*_foreign_exh.csv"))
        flow_result = results.get("flow", {})
        inv_count = flow_result.get("investor", 0) if isinstance(flow_result, dict) else 0
        exh_count = flow_result.get("foreign_exh", 0) if isinstance(flow_result, dict) else 0
        if inv_count < 2000:
            health["warnings"].append(f"수급(investor) 부족: {inv_count}종목")
        else:
            health["ok"].append(f"수급: investor {inv_count} / exh {exh_count}")
    except Exception as e:
        health["warnings"].append(f"수급 검증 실패: {e}")

    # 4) Parquet 빌드 체크
    pq_count = results.get("parquet", 0)
    if isinstance(pq_count, int) and pq_count < 20:
        health["warnings"].append(f"Parquet 빌드 부족: {pq_count}건")

    # 5) US 야간 데이터 신선도 (5/19 점검 추가)
    try:
        us_path = DATA_DIR / "us_market_overnight.json"
        if not us_path.exists():
            health["issues"].append("us_market_overnight.json 부재")
        else:
            age_h = (time.time() - us_path.stat().st_mtime) / 3600
            if age_h > 36:
                health["issues"].append(
                    f"us_market_overnight stale: {age_h:.1f}h"
                )
            else:
                us_res = results.get("us_overnight", {})
                mode = us_res.get("us_mode") if isinstance(us_res, dict) else "?"
                health["ok"].append(f"US 야간: {age_h:.1f}h 전 (mode={mode})")
    except Exception as e:
        health["warnings"].append(f"US 야간 검증 실패: {e}")

    # 6) Volume scan 결과 (5/19 점검 추가)
    try:
        today_compact = date.today().strftime("%Y%m%d")
        scan_path = DATA_DIR / "scan_results" / f"volume_spikes_{today_compact}.json"
        if not scan_path.exists():
            health["warnings"].append(
                f"volume_spikes_{today_compact}.json 부재"
            )
        else:
            vs_res = results.get("volume_scan", {})
            cnt = vs_res.get("volume_scan", 0) if isinstance(vs_res, dict) else 0
            health["ok"].append(f"Volume scan: {cnt}건")
    except Exception as e:
        health["warnings"].append(f"Volume scan 검증 실패: {e}")

    # 7) Gainer analyzed (5/19 점검 추가)
    #    today_gainers.json은 정보봇 산출물 → 부재해도 단타봇 책임 아님 (WARN)
    #    today_gainers_analyzed.json은 단타봇 책임 → 부재면 CRITICAL
    try:
        gainers_path = DATA_DIR / "today_gainers.json"
        analyzed_path = DATA_DIR / "today_gainers_analyzed.json"

        if not gainers_path.exists():
            health["warnings"].append(
                "today_gainers.json 부재 (정보봇 sync 대기 — 단타봇 책임 아님)"
            )
        else:
            gainers_age_h = (time.time() - gainers_path.stat().st_mtime) / 3600
            if gainers_age_h > 36:
                health["warnings"].append(
                    f"today_gainers.json stale: {gainers_age_h:.1f}h (정보봇 sync 확인 필요)"
                )

            if not analyzed_path.exists():
                health["issues"].append(
                    "today_gainers_analyzed.json 부재 (step10 실패)"
                )
            else:
                analyzed_age_h = (time.time() - analyzed_path.stat().st_mtime) / 3600
                if analyzed_age_h > 36:
                    health["issues"].append(
                        f"today_gainers_analyzed.json stale: {analyzed_age_h:.1f}h"
                    )
                else:
                    ga_res = results.get("gainer_analysis", {})
                    cnt = ga_res.get("analyzed_count", 0) if isinstance(ga_res, dict) else 0
                    health["ok"].append(f"Gainer 분석: {cnt}종목")
    except Exception as e:
        health["warnings"].append(f"Gainer analyzed 검증 실패: {e}")

    elapsed = int(time.time() - t0)
    health["elapsed"] = elapsed

    # 건강성 요약 로깅
    if health["issues"]:
        for issue in health["issues"]:
            logger.error(f"[HEALTH] CRITICAL: {issue}")
    if health["warnings"]:
        for warn in health["warnings"]:
            logger.warning(f"[HEALTH] WARNING: {warn}")
    for ok in health["ok"]:
        logger.info(f"[HEALTH] OK: {ok}")
    logger.info(f"[7/7] 건강성 검증 완료 ({elapsed}초)")

    return health


def _send_collect_telegram(results: dict, timings: dict, health: dict):
    """수집 완료 텔레그램 요약 발송"""
    try:
        from output.telegram_alert import TelegramAlert
        tg = TelegramAlert()
        if not tg.enabled:
            logger.info("[Telegram 미설정] 수집 요약 생략")
            return

        total = timings.get("total", 0)
        daily = results.get("daily", 0)
        flow = results.get("flow", {})
        inv = flow.get("investor", 0) if isinstance(flow, dict) else 0
        exh = flow.get("foreign_exh", 0) if isinstance(flow, dict) else 0
        etf = results.get("etf_daily", 0)
        nat = results.get("nationality", 0)
        pq = results.get("parquet", 0)
        lu = results.get("limit_up", 0)

        # 상한가 엔진 결과
        engine = results.get("limit_up_engine", {})
        eng_sigs = engine.get("new_signals", 0) if isinstance(engine, dict) else 0
        eng_trig = engine.get("triggered", 0) if isinstance(engine, dict) else 0

        # 건강성 요약
        issues = health.get("issues", [])
        warnings = health.get("warnings", [])
        status = "FAIL" if issues else ("WARN" if warnings else "OK")
        emoji = {"OK": "V", "WARN": "!", "FAIL": "X"}.get(status, "?")

        lines = [
            f"[{emoji}] 데이터 수집 완료 ({total//60}분{total%60}초)",
            f"일봉: {daily} | ETF: {etf}",
            f"수급: inv {inv} / exh {exh}",
            f"국적별: {nat} | Parquet: {pq}",
            f"상한가: 스캔 {lu} | 시그널 {eng_sigs} | 트리거 {eng_trig}",
        ]

        # 건강성 이슈
        if issues:
            lines.append(f"-- CRITICAL --")
            for i in issues[:3]:
                lines.append(f"  {i}")
        if warnings:
            lines.append(f"-- WARNING --")
            for w in warnings[:3]:
                lines.append(f"  {w}")
        if not issues and not warnings:
            lines.append("건강성: ALL OK")

        msg = "\n".join(lines)
        tg._send(msg)
        logger.info("[Telegram] 수집 요약 발송 완료")
    except Exception as e:
        logger.warning(f"텔레그램 수집 요약 실패: {e}")


def _send_step_telegram(step_name: str, detail: str = ""):
    """주요 Step 완료 시 진행률 알림"""
    try:
        from output.telegram_alert import TelegramAlert
        tg = TelegramAlert()
        if not tg.enabled:
            return
        msg = f"[수집] {step_name} 완료"
        if detail:
            msg += f" | {detail}"
        tg._send(msg)
    except Exception:
        pass  # 진행률 알림 실패는 무시


def main():
    parser = argparse.ArgumentParser(description="독립 데이터 수집기")
    parser.add_argument("--force", action="store_true", help="캐시 무시 강제 수집")
    parser.add_argument("--resume", action="store_true", help="크래시 지점부터 재개")
    parser.add_argument("--sync-only", action="store_true", help="stock_data_daily 동기화만")
    args = parser.parse_args()

    # logs 디렉토리 보장
    (SCRIPT_DIR / "logs").mkdir(exist_ok=True)

    # 주말/공휴일 체크
    if not is_trading_day():
        logger.info("휴장일 - 수집 스킵")
        return

    logger.info(f"{'='*60}")
    logger.info(f"데이터 수집 시작: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"{'='*60}")
    t_start = time.time()

    if args.sync_only:
        logger.info("sync_only 모드 — 현재 미지원 (step5 삭제됨)")
        return

    # 봇이 오늘 이미 수집했는지 체크
    bot_done = _check_bot_already_collected()
    if bot_done:
        logger.info("봇이 이미 오늘 수집 완료 → parquet만 실행")
        pq = step4_parquet_build()
        _save_collect_result({"parquet": pq, "skipped": "bot_done"})
        elapsed = int(time.time() - t_start)
        logger.info(f"보완 수집 완료: {elapsed}초")
        return

    # 크래시 복구: --resume 시 마지막 체크포인트 다음 Step부터 재개
    resume_from = None
    if args.resume:
        resume_from = _get_resume_step()
        if resume_from:
            logger.info(f"[RESUME] 크래시 복구: {resume_from} 부터 재개")
            _send_step_telegram("RESUME", f"{resume_from} 부터 재개")
        else:
            logger.info("[RESUME] 체크포인트 없음 → 전체 수집")

    codes = get_universe_codes()
    if not codes:
        logger.error("유니버스 종목이 없습니다!")
        return

    logger.info(f"유니버스: {len(codes)}종목")

    results = {}
    timings = {}
    import threading
    from concurrent.futures import ThreadPoolExecutor

    # 수급 2단계 분할: 우선 종목 / 잔여 종목
    priority_codes, remaining_codes = get_flow_priority_codes(codes)

    # ── Step 1+2A+2b: 일봉 + 우선수급 + ETF 동시 실행 ──
    bg_flow_thread = None  # 잔여 수급 백그라운드 스레드
    if not _should_skip("step1_2", resume_from):
        logger.info(
            f"[1+2A+2b] 일봉(전체) + 수급(우선 {len(priority_codes)}종목) + ETF 동시 시작..."
        )
        t12 = time.time()
        with ThreadPoolExecutor(max_workers=3) as executor:
            f1 = executor.submit(step1_daily_ohlcv, codes, args.force)
            f2a = executor.submit(step2_supply_demand, priority_codes, args.force)
            f2b = executor.submit(step2b_etf_ohlcv, args.force)
            # C3: 개별 try/except — 한쪽 실패해도 다른 쪽 결과 보존
            try:
                results["daily"] = f1.result()
            except Exception as e:
                logger.error(f"[C3] Step1 일봉 스레드 실패: {e}")
                results["daily"] = 0
            try:
                results["flow"] = f2a.result()
            except Exception as e:
                logger.error(f"[C3] Step2A 수급(우선) 스레드 실패: {e}")
                results["flow"] = {}
            try:
                results["etf_daily"] = f2b.result()
            except Exception as e:
                logger.error(f"[C3] Step2b ETF 스레드 실패: {e}")
                results["etf_daily"] = 0
        timings["step1_2"] = int(time.time() - t12)
        logger.info(
            f"[1+2A+2b] 일봉+수급(우선)+ETF 완료: {timings['step1_2']}초 "
            f"(우선 {len(priority_codes)}종목)"
        )
        _save_checkpoint("step1_2", results.get("daily", 0), timings["step1_2"])
        _send_step_telegram(
            "일봉+수급(우선)+ETF",
            f"일봉 {results.get('daily',0)} | "
            f"수급(우선 {len(priority_codes)}) {results.get('flow',{})} | "
            f"ETF {results.get('etf_daily',0)} | "
            f"{timings['step1_2']}초"
        )

        # ── Step 2B: 잔여 수급 백그라운드 수집 (메인 파이프라인 비차단) ──
        if remaining_codes:
            logger.info(
                f"[2B] 잔여 수급 백그라운드 시작: {len(remaining_codes)}종목"
            )
            bg_flow_thread = threading.Thread(
                target=step2_supply_demand,
                args=(remaining_codes, args.force),
                name="flow-bg",
                daemon=False,
            )
            bg_flow_thread.start()
    else:
        logger.info("[SKIP] Step 1+2+2b (이미 완료)")

    # ── Step 3: 국적별 ──
    if not _should_skip("step3", resume_from):
        t3 = time.time()
        results["nationality"] = step3_nationality(args.force)
        timings["step3"] = int(time.time() - t3)
        _save_checkpoint("step3", results["nationality"], timings["step3"])
    else:
        logger.info("[SKIP] Step 3 국적별 (이미 완료)")

    # ── Step 4: Parquet ──
    if not _should_skip("step4", resume_from):
        t4 = time.time()
        results["parquet"] = step4_parquet_build()
        timings["step4"] = int(time.time() - t4)
        _save_checkpoint("step4", results["parquet"], timings["step4"])
    else:
        logger.info("[SKIP] Step 4 Parquet (이미 완료)")

    # ── Step 5: SpaceX ──
    if not _should_skip("step5_spacex", resume_from):
        t5sx = time.time()
        results["spacex"] = step5_spacex_report()
        timings["step5_spacex"] = int(time.time() - t5sx)
        _save_checkpoint("step5_spacex", results["spacex"], timings["step5_spacex"])
    else:
        logger.info("[SKIP] Step 5 SpaceX (이미 완료)")

    # ── Step 5b: 상한가 스캐너 ──
    if not _should_skip("step5b", resume_from):
        t5b = time.time()
        results["limit_up"] = step5b_limit_up_scan()
        timings["step5b"] = int(time.time() - t5b)
        _save_checkpoint("step5b", results["limit_up"], timings["step5b"])
    else:
        logger.info("[SKIP] Step 5b 상한가 스캐너 (이미 완료)")

    # ── Step 5c: 상한가 엔진 ──
    if not _should_skip("step5c", resume_from):
        t5c = time.time()
        results["limit_up_engine"] = step5c_limit_up_engine()
        timings["step5c"] = int(time.time() - t5c)
        _save_checkpoint("step5c", results.get("limit_up_engine", {}), timings["step5c"])
    else:
        logger.info("[SKIP] Step 5c 상한가 엔진 (이미 완료)")

    # ── Step 5d: 페이퍼 트레이딩 ──
    if not _should_skip("step5d", resume_from):
        t5d = time.time()
        results["limit_up_paper"] = step5d_limit_up_paper()
        timings["step5d"] = int(time.time() - t5d)
        _save_checkpoint("step5d", results.get("limit_up_paper", {}), timings["step5d"])
    else:
        logger.info("[SKIP] Step 5d 페이퍼 (이미 완료)")

    # ── Step 5e: FLOWX 업로드 ──
    if not _should_skip("step5e", resume_from):
        t5e = time.time()
        results["limit_up_upload"] = step5e_limit_up_upload()
        timings["step5e"] = int(time.time() - t5e)
        _save_checkpoint("step5e", results.get("limit_up_upload", {}), timings["step5e"])
    else:
        logger.info("[SKIP] Step 5e FLOWX 업로드 (이미 완료)")

    # ── Step 5f: 수급 임계점 ──
    if not _should_skip("step5f", resume_from):
        t5f = time.time()
        results["tipping_point"] = step5f_tipping_point()
        timings["step5f"] = int(time.time() - t5f)
        _save_checkpoint("step5f", results.get("tipping_point", {}), timings["step5f"])
    else:
        logger.info("[SKIP] Step 5f 수급 임계점 (이미 완료)")

    # ── Step 6: stock_data_daily 동기화 ──
    if not _should_skip("step6", resume_from):
        t6 = time.time()
        results["sync"] = step6_sync_stock_data_daily()
        timings["step6"] = int(time.time() - t6)
        _save_checkpoint("step6", results["sync"], timings["step6"])
    else:
        logger.info("[SKIP] Step 6 sync (이미 완료)")

    # ── Step 8: 미국장 야간 (5/19 점검 추가) ──
    if not _should_skip("step8_us_overnight", resume_from):
        t8 = time.time()
        results["us_overnight"] = step8_us_overnight()
        timings["step8_us_overnight"] = int(time.time() - t8)
        _save_checkpoint(
            "step8_us_overnight",
            results["us_overnight"],
            timings["step8_us_overnight"],
        )
    else:
        logger.info("[SKIP] Step 8 US 야간 (이미 완료)")

    # ── Step 9: 거래량 이상거래 스캔 (5/19 점검 추가) ──
    if not _should_skip("step9_volume_scan", resume_from):
        t9 = time.time()
        results["volume_scan"] = step9_volume_scan()
        timings["step9_volume_scan"] = int(time.time() - t9)
        _save_checkpoint(
            "step9_volume_scan",
            results["volume_scan"],
            timings["step9_volume_scan"],
        )
    else:
        logger.info("[SKIP] Step 9 Volume scan (이미 완료)")

    # ── Step 10: Gainer 시그널 역추적 분석 (5/19 점검 추가) ──
    if not _should_skip("step10_gainer_analysis", resume_from):
        t10 = time.time()
        results["gainer_analysis"] = step10_gainer_analysis()
        timings["step10_gainer_analysis"] = int(time.time() - t10)
        _save_checkpoint(
            "step10_gainer_analysis",
            results["gainer_analysis"],
            timings["step10_gainer_analysis"],
        )
    else:
        logger.info("[SKIP] Step 10 Gainer analysis (이미 완료)")

    # ── Step 7: 데이터 건강성 검증 (NEW) ──
    t7 = time.time()
    health = step7_health_check(results)
    timings["step7"] = int(time.time() - t7)
    results["health"] = health
    _save_checkpoint("step7", len(health.get("issues", [])), timings["step7"])

    # DC-08: 수집 시간 로깅
    timings["total"] = int(time.time() - t_start)

    # 수집 완료 기록 (타이밍 포함)
    results["timings"] = timings
    _save_collect_result(results)

    # 텔레그램 수집 완료 요약 알림
    _send_collect_telegram(results, timings, health)

    elapsed = timings["total"]
    logger.info(f"{'='*60}")
    logger.info(f"메인 파이프라인 완료: {elapsed}초 ({elapsed//60}분)")
    logger.info(f"  Step1+2A: {timings.get('step1_2',0)}초 | Step3: {timings.get('step3',0)}초 | "
                f"Step4: {timings.get('step4',0)}초 | Step5-SpaceX: {timings.get('step5_spacex',0)}초 | "
                f"Step5b-LimitUp: {timings.get('step5b',0)}초 | "
                f"Step5c-Engine: {timings.get('step5c',0)}초 | "
                f"Step5d-Paper: {timings.get('step5d',0)}초 | "
                f"Step5e-Upload: {timings.get('step5e',0)}초 | "
                f"Step5f-Tipping: {timings.get('step5f',0)}초 | "
                f"Step6: {timings.get('step6',0)}초 | Step7-Health: {timings.get('step7',0)}초")
    logger.info(f"{'='*60}")

    # ── Step 2B 대기: 잔여 수급 백그라운드 완료 대기 ──
    if bg_flow_thread is not None and bg_flow_thread.is_alive():
        logger.info(f"[2B] 잔여 수급 백그라운드 완료 대기 중...")
        bg_flow_thread.join()
        logger.info(f"[2B] 잔여 수급 백그라운드 완료 (총 {int(time.time()-t_start)}초)")

    # 체크포인트 정리 (전체 완료 시)
    if CHECKPOINT_PATH.exists():
        try:
            CHECKPOINT_PATH.unlink()
        except Exception:
            pass


if __name__ == "__main__":
    main()
