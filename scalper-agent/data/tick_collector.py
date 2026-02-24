# -*- coding: utf-8 -*-
"""
KIS API 체결 스냅샷 수집기 — 전종목 1분 폴링

매분 354종목 REST API 호출 → CSV 누적 저장
- 시세 API (FHKST01010100): 현재가, 전일대비, 등락률, 누적거래량
- 체결 API (FHKST01010300): 체결강도, 체결량
- 호가 API (FHKST01010200): 매도호가1, 매수호가1

저장: data_store/ticks/YYYYMMDD/{code}.csv
"""

import os
import time
import logging
import requests
from pathlib import Path
from datetime import datetime, date
from typing import Dict, Optional

import pandas as pd

logger = logging.getLogger("BH.TickCollector")

DATA_DIR = Path(__file__).resolve().parent.parent / "data_store" / "ticks"


def _ensure_dir(today: str):
    d = DATA_DIR / today
    d.mkdir(parents=True, exist_ok=True)
    return d


class TickCollector:
    """전종목 체결 스냅샷 1분 폴링 수집기"""

    # 유저 요청 9개 필드
    COLUMNS = [
        "time",          # 체결시간 (HH:MM:SS)
        "price",         # 현재가
        "change",        # 전일대비
        "change_rate",   # 등락률 (%)
        "ask1",          # 매도호가1
        "bid1",          # 매수호가1
        "strength",      # 체결강도
        "volume",        # 누적거래량
        "tick_volume",   # 체결량 (구간거래량)
    ]

    def __init__(self):
        self._broker = None
        self._prev_volume: Dict[str, int] = {}  # 이전 거래량 (체결량 계산용)

    def _get_broker(self):
        if self._broker is not None:
            return self._broker
        from dotenv import load_dotenv
        load_dotenv()
        import mojito
        self._broker = mojito.KoreaInvestment(
            api_key=os.getenv("KIS_APP_KEY"),
            api_secret=os.getenv("KIS_APP_SECRET"),
            acc_no=os.getenv("KIS_ACC_NO"),
            mock=False,
        )
        return self._broker

    def _fetch_snapshot(self, code: str) -> Optional[dict]:
        """1종목 체결 스냅샷 (시세+체결+호가 3 API 조합)

        API 호출 순서:
        1. 시세 (FHKST01010100): 현재가, 전일대비, 등락률, 거래량
        2. 체결 (FHKST01010300): 체결강도, 체결량
        3. 호가 (FHKST01010200): 매도호가1, 매수호가1
        """
        broker = self._get_broker()
        base = broker.base_url
        common_headers = {
            "content-type": "application/json; charset=utf-8",
            "authorization": broker.access_token,
            "appKey": broker.api_key,
            "appSecret": broker.api_secret,
        }
        common_params = {
            "fid_cond_mrkt_div_code": "J",
            "fid_input_iscd": code,
        }

        now_str = datetime.now().strftime("%H:%M:%S")
        row = {"time": now_str}

        try:
            # 1) 시세 — 현재가, 전일대비, 등락률, 거래량
            h1 = {**common_headers, "tr_id": "FHKST01010100"}
            r1 = requests.get(
                f"{base}/uapi/domestic-stock/v1/quotations/inquire-price",
                headers=h1, params=common_params, timeout=5,
            )
            d1 = r1.json().get("output", {})

            price = int(d1.get("stck_prpr", 0))
            change = int(d1.get("prdy_vrss", 0))
            # 하락이면 음수 처리
            sign = d1.get("prdy_vrss_sign", "0")
            if sign in ("5", "4"):  # 하한/하락
                change = -abs(change)
            change_rate = float(d1.get("prdy_ctrt", 0))
            volume = int(d1.get("acml_vol", 0))

            row["price"] = price
            row["change"] = change
            row["change_rate"] = change_rate
            row["volume"] = volume

            # 체결량 = 현재 거래량 - 이전 거래량
            prev_vol = self._prev_volume.get(code, 0)
            tick_vol = volume - prev_vol if prev_vol > 0 else 0
            self._prev_volume[code] = volume
            row["tick_volume"] = tick_vol

            time.sleep(0.05)

            # 2) 체결 — 체결강도
            h2 = {**common_headers, "tr_id": "FHKST01010300"}
            r2 = requests.get(
                f"{base}/uapi/domestic-stock/v1/quotations/inquire-ccnl",
                headers=h2, params=common_params, timeout=5,
            )
            d2_list = r2.json().get("output", [])
            if d2_list:
                row["strength"] = float(d2_list[0].get("tday_rltv", 0))
            else:
                row["strength"] = 0.0

            time.sleep(0.05)

            # 3) 호가 — 매도호가1, 매수호가1
            h3 = {**common_headers, "tr_id": "FHKST01010200"}
            r3 = requests.get(
                f"{base}/uapi/domestic-stock/v1/quotations/inquire-asking-price-exp-ccn",
                headers=h3, params=common_params, timeout=5,
            )
            d3 = r3.json().get("output1", {})
            row["ask1"] = int(d3.get("askp1", 0))
            row["bid1"] = int(d3.get("bidp1", 0))

            return row

        except Exception as e:
            logger.warning(f"[{code}] 스냅샷 실패: {e}")
            return None

    def poll_once(self, codes: list) -> int:
        """전종목 1회 폴링 → CSV append

        Returns: 성공 종목 수
        """
        today = date.today().strftime("%Y%m%d")
        save_dir = _ensure_dir(today)

        ok = 0
        for i, code in enumerate(codes):
            row = self._fetch_snapshot(code)
            if row is None:
                continue

            # CSV에 append
            csv_path = save_dir / f"{code}.csv"
            write_header = not csv_path.exists()

            with open(csv_path, "a", encoding="utf-8") as f:
                if write_header:
                    f.write(",".join(self.COLUMNS) + "\n")
                vals = [str(row.get(c, "")) for c in self.COLUMNS]
                f.write(",".join(vals) + "\n")

            ok += 1
            time.sleep(0.05)  # 종목 간 대기 (rate limit)

        return ok

    def run_market_hours(self, codes: list, interval_sec: int = 60):
        """장중 반복 폴링 (09:01 ~ 15:30)

        Args:
            codes: 종목코드 리스트
            interval_sec: 폴링 간격 (초), 기본 60초
        """
        logger.info(f"체결 폴링 시작: {len(codes)}종목, {interval_sec}초 간격")

        cycle = 0
        while True:
            now = datetime.now()
            t = now.strftime("%H%M")

            # 장 시작 전이면 대기
            if t < "0901":
                time.sleep(30)
                continue

            # 장 마감 후 종료
            if t > "1530":
                logger.info("장 마감 — 체결 폴링 종료")
                break

            cycle += 1
            start = time.time()
            ok = self.poll_once(codes)
            elapsed = time.time() - start

            if cycle % 10 == 0:
                logger.info(
                    f"체결 폴링 #{cycle}: {ok}/{len(codes)}종목 ({elapsed:.0f}초)"
                )

            # 다음 사이클까지 대기
            wait = max(0, interval_sec - elapsed)
            if wait > 0:
                time.sleep(wait)

        # 수집 통계
        today = date.today().strftime("%Y%m%d")
        save_dir = DATA_DIR / today
        if save_dir.exists():
            csvs = list(save_dir.glob("*.csv"))
            total_rows = 0
            for csv in csvs:
                with open(csv, "r") as f:
                    total_rows += sum(1 for _ in f) - 1  # header 제외
            total_mb = sum(f.stat().st_size for f in csvs) / 1024 / 1024
            logger.info(
                f"체결 수집 완료: {len(csvs)}종목, {total_rows:,}행, {total_mb:.1f}MB"
            )

        return cycle


if __name__ == "__main__":
    import sys
    import io
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")

    from data.kis_collector import UNIVERSE

    if "--test" in sys.argv:
        # 5종목 1회 테스트
        test_codes = list(UNIVERSE.keys())[:5]
        tc = TickCollector()
        ok = tc.poll_once(test_codes)
        print(f"테스트 완료: {ok}/{len(test_codes)}종목")

        today = date.today().strftime("%Y%m%d")
        for code in test_codes:
            csv = DATA_DIR / today / f"{code}.csv"
            if csv.exists():
                df = pd.read_csv(csv)
                name = UNIVERSE.get(code, (code,))[0]
                print(f"  {name}: {df.iloc[-1].to_dict()}")
    else:
        # 전종목 장중 폴링
        codes = list(UNIVERSE.keys())
        tc = TickCollector()
        tc.run_market_hours(codes, interval_sec=60)
