# -*- coding: utf-8 -*-
"""순위/시세 스냅샷 수집기 (6/27 신설) — KIS 순위 6종 record-only 날짜별 적재.

배경:
  - KIS 순위 API(등락률/거래량/체결강도/상하한가/외인기관순매수/야간선물)는
    bot/kis_trader.py 에 함수가 이미 있으나, tools/ranking_scanner.py 가
    ranking_scan.json 1개에 덮어쓸 뿐 날짜별 시계열로는 안 쌓였다.
  - 변동성 장에서 끼·수급·급락 신호의 시계열 관측을 위해 매일 1회 스냅샷을 누적.

안전(불변식):
  - 봇 OFF·실주문0·매도무손상·picks불변·SAJANG무접촉. 조회(fetch_*) 함수만 호출.
  - record-only: data_store/ranking_snapshots/{kind}.csv 누적(매매 연결 없음).
  - 휴장일 skip(is_trading_day, RULE-002). CSV-001(date yyyy-mm-dd + 중복제거 keep=last + sort).

사용법: python -m data.ranking_snapshot_collector
"""
import sys
import time
import logging
from pathlib import Path
from datetime import date

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd  # noqa: E402

from data.trading_calendar import is_trading_day  # noqa: E402

logger = logging.getLogger(__name__)

DATA_DIR = ROOT / "data_store"
SNAP_DIR = DATA_DIR / "ranking_snapshots"

SLEEP_SEC = 0.3  # KIS rate-limit (tools/ranking_scanner.py 패턴)


def _ensure_dirs():
    SNAP_DIR.mkdir(parents=True, exist_ok=True)


def _append_snapshot(kind: str, rows: list, today: str, dedup_extra=None) -> int:
    """rows(list[dict])에 date 부여 후 {kind}.csv 에 누적(중복 제거 keep=last).

    dedup_extra: 중복 판정 subset 추가 컬럼(기본 ["code"]).
    Returns: 이번에 추가된 행 수(0이면 빈 결과).
    """
    if not rows:
        logger.warning(f"[RANKSNAP] {kind}: 빈 결과 — skip")
        return 0
    df_new = pd.DataFrame(rows)
    df_new.insert(0, "date", today)

    cache = SNAP_DIR / f"{kind}.csv"
    if cache.exists():
        try:
            old = pd.read_csv(cache, dtype={"code": str})
            df = pd.concat([old, df_new], ignore_index=True)
        except Exception as e:
            logger.warning(f"[RANKSNAP] {kind} 기존 csv 읽기 실패({e}) — 새로 생성")
            df = df_new
    else:
        df = df_new

    subset = ["date"] + (dedup_extra if dedup_extra else ["code"])
    subset = [c for c in subset if c in df.columns]
    df = df.drop_duplicates(subset=subset, keep="last")
    df = df.sort_values("date").reset_index(drop=True)
    df.to_csv(cache, index=False, encoding="utf-8-sig")
    return len(df_new)


def collect_ranking_snapshots(top_n: int = 50) -> dict:
    """KIS 순위 6종 스냅샷을 날짜별 csv로 record-only 적재.

    record-only — 어떤 매수/매도/주문 함수도 호출하지 않는다(조회 전용).
    """
    today_d = date.today()
    if not is_trading_day(today_d):
        logger.info("[RANKSNAP] 휴장일 — skip (exit 0)")
        return {"status": "skip_holiday"}

    _ensure_dirs()
    today = today_d.isoformat()

    from bot.kis_trader import KISTrader
    trader = KISTrader()

    saved = {}

    def _ranked(kind, fetch_fn):
        try:
            rows = fetch_fn()
            for i, r in enumerate(rows):
                r["rank"] = i + 1
            saved[kind] = _append_snapshot(kind, rows, today)
        except Exception as e:  # noqa: BLE001 — 종류별 격리
            logger.error(f"[RANKSNAP] {kind} 실패: {e}")
            saved[kind] = 0
        time.sleep(SLEEP_SEC)

    # 1~4) 단순 순위형 (등락률 / 거래량 / 체결강도 / 상하한가)
    _ranked("fluctuation", lambda: trader.fetch_ranking_fluctuation(top_n=top_n))
    _ranked("volume", lambda: trader.fetch_ranking_volume(top_n=top_n))
    _ranked("strength", lambda: trader.fetch_ranking_strength(top_n=top_n))
    _ranked("uplowprice", lambda: trader.fetch_uplowprice(price_cls="0", div_cls="0"))

    # 5) 외인/기관 순매수 (외인매수 + 기관매수, category 구분)
    try:
        all_rows = []
        for cat, tgt in [("foreign_buy", "1"), ("inst_buy", "2")]:
            part = trader.fetch_foreign_inst_total(target=tgt, sort_cls="0")
            for i, r in enumerate(part):
                r["rank"] = i + 1
                r["category"] = cat
            all_rows.extend(part)
            time.sleep(SLEEP_SEC)
        saved["foreign_inst"] = _append_snapshot(
            "foreign_inst", all_rows, today, dedup_extra=["category", "code"]
        )
    except Exception as e:  # noqa: BLE001
        logger.error(f"[RANKSNAP] foreign_inst 실패: {e}")
        saved["foreign_inst"] = 0

    # 6) KOSPI200 야간선물 (dict 1개 → 1행/일, 장외시간이면 skip)
    try:
        nf = trader.fetch_night_futures_price()
        if nf and nf.get("available"):
            saved["night_futures"] = _append_snapshot("night_futures", [nf], today)
        else:
            reason = nf.get("reason", "none") if nf else "none"
            logger.info(f"[RANKSNAP] night_futures 장외/없음: {reason}")
            saved["night_futures"] = 0
    except Exception as e:  # noqa: BLE001
        logger.error(f"[RANKSNAP] night_futures 실패: {e}")
        saved["night_futures"] = 0

    logger.info(f"[RANKSNAP] 완료 {today}: {saved}")
    return {"status": "ok", "date": today, "saved": saved}


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    result = collect_ranking_snapshots()
    print(result)
