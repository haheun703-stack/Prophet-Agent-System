# -*- coding: utf-8 -*-
"""
독립 데이터 수집 스크립트 — 봇 무관하게 매일 자동 실행
=======================================================
봇이 꺼져 있어도 Windows 작업 스케줄러로 매일 16:10에 실행.

수집 항목:
  1. pykrx 일봉 OHLCV (data_store/daily/)
  2. 수급 데이터 — 투자자/외인소진/공매도잔고/공매도거래량 (data_store/flow/, short/)
  3. 국적별 외국인 수급 (data_store/nationality/) — 추천+보유 종목
  4. Parquet 통합 빌드 (data_store/raw/, processed/)
  5. stock_data_daily 동기화 (data_store/daily → stock_data_daily)

사용법:
  python collect_all.py                  # 전체 수집 (당일 기준)
  python collect_all.py --force          # 캐시 무시 강제 수집
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


def step1_daily_ohlcv(codes: list, force: bool = False):
    """1단계: pykrx 일봉 수집"""
    logger.info(f"[1/5] 일봉 수집 시작: {len(codes)}종목 (force={force})")
    t0 = time.time()
    try:
        from data.universe_builder import collect_daily_pykrx
        cnt = collect_daily_pykrx(codes, months=24, force=force)
        logger.info(f"[1/5] 일봉 완료: {cnt}종목 ({int(time.time()-t0)}초)")
        return cnt
    except Exception as e:
        logger.error(f"[1/5] 일봉 실패: {e}")
        return 0


def step2_supply_demand(codes: list, force: bool = False):
    """2단계: 수급 데이터 (투자자/외인소진/공매도)"""
    logger.info(f"[2/5] 수급 수집 시작: {len(codes)}종목")
    t0 = time.time()
    results = {}
    try:
        from data.flow_collector import (
            collect_investor_flow,
            collect_foreign_exhaustion,
            collect_short_balance,
            collect_short_volume,
        )
        results["investor"] = len(collect_investor_flow(codes, 24, force))
        results["foreign_exh"] = len(collect_foreign_exhaustion(codes, 24, force))
        results["short_bal"] = len(collect_short_balance(codes, 24, force))
        results["short_vol"] = len(collect_short_volume(codes, 24, force))
        logger.info(f"[2/5] 수급 완료: {results} ({int(time.time()-t0)}초)")
    except Exception as e:
        logger.error(f"[2/5] 수급 실패: {e}")
    return results


def step3_nationality(force: bool = False):
    """3단계: 국적별 외국인 수급 (추천+보유 종목)"""
    logger.info("[3/5] 국적별 수급 수집...")
    t0 = time.time()
    nat_codes = set()

    # 추천 종목
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

    # 보유 종목 (봇 포지션)
    for pos_file in ["swing_candidates.json", "nxt_positions.json"]:
        fp = DATA_DIR / pos_file
        if fp.exists():
            try:
                with open(fp, "r", encoding="utf-8") as f:
                    data = json.load(f)
                if isinstance(data, dict):
                    nat_codes.update(data.keys())
                elif isinstance(data, list):
                    for item in data:
                        if isinstance(item, dict) and "code" in item:
                            nat_codes.add(item["code"])
            except Exception:
                pass

    # 핵심 종목 항상 포함
    for code in ["005930", "000660", "003570", "103140"]:
        nat_codes.add(code)

    if not nat_codes:
        logger.info("[3/5] 국적별: 대상 종목 없음, 스킵")
        return 0

    nat_codes = list(nat_codes)
    logger.info(f"[3/5] 국적별 대상: {len(nat_codes)}종목")

    try:
        import asyncio
        from data.krx_nationality_crawler import afetch_nationality_batch
        from datetime import timedelta

        date_from = (datetime.now() - timedelta(days=5)).strftime("%Y%m%d")
        date_to = datetime.now().strftime("%Y%m%d")

        results = asyncio.run(afetch_nationality_batch(nat_codes, date_from, date_to))
        ok = sum(1 for df in results.values() if not df.empty)
        logger.info(f"[3/5] 국적별 완료: {ok}/{len(nat_codes)} ({int(time.time()-t0)}초)")
        return ok
    except Exception as e:
        logger.error(f"[3/5] 국적별 실패: {e}")
        return 0


def step4_parquet_build():
    """4단계: Parquet 통합 빌드"""
    logger.info("[4/5] Parquet 빌드...")
    t0 = time.time()
    try:
        from data.extend_parquet_data import extend_parquet_all
        ok, fail = extend_parquet_all(codes=None, force=True)
        logger.info(f"[4/5] Parquet 완료: 성공 {ok} / 실패 {fail} ({int(time.time()-t0)}초)")
        return ok
    except Exception as e:
        logger.error(f"[4/5] Parquet 실패: {e}")
        return 0


def step5_sync_stock_data_daily():
    """5단계: data_store/daily → stock_data_daily 동기화

    stock_data_daily 폴더는 이름_코드.csv 형식으로 저장됨.
    data_store/daily의 pykrx 데이터를 stock_data_daily 형식에 맞게 병합.
    """
    logger.info("[5/5] stock_data_daily 동기화...")
    if not STOCK_DATA_DAILY.exists():
        logger.warning(f"[5/5] {STOCK_DATA_DAILY} 폴더 없음, 스킵")
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

            df_new.to_csv(dst)
            synced += 1
        except Exception as e:
            if synced < 3:  # 처음 몇 개만 로깅
                logger.debug(f"동기화 실패 {code}: {e}")

    logger.info(f"[5/5] stock_data_daily 동기화 완료: {synced}종목")
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


def main():
    parser = argparse.ArgumentParser(description="독립 데이터 수집기")
    parser.add_argument("--force", action="store_true", help="캐시 무시 강제 수집")
    parser.add_argument("--sync-only", action="store_true", help="stock_data_daily 동기화만")
    args = parser.parse_args()

    # logs 디렉토리 보장
    (SCRIPT_DIR / "logs").mkdir(exist_ok=True)

    # 주말 체크
    if date.today().weekday() >= 5:
        logger.info("주말 — 수집 스킵")
        return

    logger.info(f"{'='*60}")
    logger.info(f"데이터 수집 시작: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"{'='*60}")
    t_start = time.time()

    if args.sync_only:
        step5_sync_stock_data_daily()
        return

    # 봇이 오늘 이미 수집했는지 체크
    bot_done = _check_bot_already_collected()
    if bot_done:
        logger.info("봇이 이미 오늘 수집 완료 → parquet+sync만 실행")
        pq = step4_parquet_build()
        sync = step5_sync_stock_data_daily()
        _save_collect_result({"parquet": pq, "sync": sync, "skipped": "bot_done"})
        elapsed = int(time.time() - t_start)
        logger.info(f"보완 수집 완료: {elapsed}초")
        return

    codes = get_universe_codes()
    if not codes:
        logger.error("유니버스 종목이 없습니다!")
        return

    logger.info(f"유니버스: {len(codes)}종목")

    results = {}

    # 1. 일봉
    results["daily"] = step1_daily_ohlcv(codes, args.force)

    # 2. 수급
    results["flow"] = step2_supply_demand(codes, args.force)

    # 3. 국적별
    results["nationality"] = step3_nationality(args.force)

    # 4. Parquet
    results["parquet"] = step4_parquet_build()

    # 5. stock_data_daily 동기화
    results["sync"] = step5_sync_stock_data_daily()

    # 수집 완료 기록
    _save_collect_result(results)

    elapsed = int(time.time() - t_start)
    logger.info(f"{'='*60}")
    logger.info(f"전체 수집 완료: {elapsed}초 ({elapsed//60}분)")
    logger.info(f"{'='*60}")


if __name__ == "__main__":
    main()
