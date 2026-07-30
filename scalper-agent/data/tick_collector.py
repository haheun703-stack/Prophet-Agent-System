# -*- coding: utf-8 -*-
"""
KIS API 체결 스냅샷 수집기 - 전종목 1분 폴링

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


def _safe_int(v, default=0):
    """KIS는 정수 필드에 "55710.28" 같은 소수 문자열을 줄 때가 있다(5/19 사장님 fix·
    realtime_monitor와 동일 패턴). 파싱 실패를 '네트워크 예외'로 오진하지 않기 위한 가드."""
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return default


def _safe_float(v, default=0.0):
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def _refused(resp, j) -> bool:
    """KIS 거부 판정 — 레이트리밋·토큰만료는 HTTP 200 + rt_cd!="0"로 온다.
    rt_cd 부재(게이트웨이 오류 바디)도 거부로 취급(레포 관례 str(rt_cd)!="0" 엄격 판정)."""
    if getattr(resp, "status_code", 200) != 200:
        return True
    return str(j.get("rt_cd")) != "0"


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

    # 장애 escalation 임계 (7/20 사고 fix — 자기증폭 차단 + 토큰만료 복구)
    FAIL_BACKOFF_AT = 50      # 연속 실패 n건마다 백오프
    FAIL_BACKOFF_SEC = 10     # 백오프 길이
    FAIL_RESET_AT = 150       # 연속 실패 n건 = 토큰/세션 계열 의심 → broker 재생성 1회

    def __init__(self):
        self._broker = None
        self._prev_volume: Dict[str, int] = {}  # 이전 거래량 (체결량 계산용)
        self._last_api_error: Optional[str] = None   # 사이클 요약 로그용(7/20 사고 fix)
        self._api_fail: int = 0                      # hard 거부(행 미기록) 종목 수
        self._api_soft: int = 0                      # soft 거부(필드 결손 기록) 종목 수 — M1

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
        # ★ broker 획득은 try 밖이라 예외가 poll_once까지 전파 → 폴링 스레드 사망.
        # 원래는 세션 1회 생성이라 사실상 안전했으나, 7/20 fix로 장애 시 재생성(토큰 재발급)을
        # 하게 되면서 '재발급 실패 = 루프 사망' 경로가 생겼다 → 여기서 격리(자체 모의검증 발견).
        try:
            broker = self._get_broker()
            base = broker.base_url
            common_headers = {
                "content-type": "application/json; charset=utf-8",
                "authorization": broker.access_token,
                "appKey": broker.api_key,
                "appSecret": broker.api_secret,
            }
        except Exception as e:  # noqa: BLE001
            self._broker = None          # 다음 종목에서 재시도
            self._last_api_error = f"broker 준비 실패: {str(e)[:40]}"
            self._api_fail += 1
            logger.warning(f"[tick] broker 준비 실패 — 다음 종목에서 재시도: {str(e)[:80]}")
            return None
        common_params = {
            "fid_cond_mrkt_div_code": "J",
            "fid_input_iscd": code,
        }

        now_str = datetime.now().strftime("%H:%M:%S")
        row = {"time": now_str}

        try:
            # 1) 시세 - 현재가, 전일대비, 등락률, 거래량
            h1 = {**common_headers, "tr_id": "FHKST01010100"}
            r1 = requests.get(
                f"{base}/uapi/domestic-stock/v1/quotations/inquire-price",
                headers=h1, params=common_params, timeout=5,
            )
            # ★ 7/13·7/20 사고 fix — KIS는 레이트리밋·토큰만료를 HTTP 200 + rt_cd!=0 으로 준다.
            # 예외가 안 나므로 기존 코드는 output 결손을 price=0으로 기록했고, poll_once는
            # 이를 '성공'으로 집계했다(오늘 로그 "2530/2531 성공"인데 전 종목 0). 조용한 오염 =
            # ⑲/⑲-3 판정 증거가 통째로 무의미해지는 사고 → 거부는 거부로 인식하고 행을 쓰지 않는다.
            j1 = r1.json()
            if _refused(r1, j1):
                # r1 거부 = price 없음 → 행 자체 불성립(hard fail·행 미기록)
                self._last_api_error = f"r1 {j1.get('msg_cd', '')} {str(j1.get('msg1', ''))[:40]}".strip()
                self._api_fail += 1
                return None
            d1 = j1.get("output") or {}

            price = _safe_int(d1.get("stck_prpr"))
            if price <= 0:
                # output 결손/휴장 스냅샷 — 0 기록은 신호·청산 시뮬을 오염시킨다(가격 0 행 금지)
                self._last_api_error = self._last_api_error or "output 결손(price=0)"
                self._api_fail += 1
                return None
            change = _safe_int(d1.get("prdy_vrss"))
            # 하락이면 음수 처리
            sign = d1.get("prdy_vrss_sign", "0")
            if sign in ("5", "4"):  # 하한/하락
                change = -abs(change)
            change_rate = _safe_float(d1.get("prdy_ctrt"))
            volume = _safe_int(d1.get("acml_vol"))

            row["price"] = price
            row["change"] = change
            row["change_rate"] = change_rate
            row["volume"] = volume

            # 체결량 = 현재 거래량 - 이전 거래량
            prev_vol = self._prev_volume.get(code, 0)
            tick_vol = volume - prev_vol if prev_vol > 0 else 0
            row["tick_volume"] = tick_vol

            time.sleep(0.05)

            # ★ M1 fix (Tier1 검수 반영 재설계) — r2/r3 거부는 soft-fail:
            # 행을 버리지 않고 해당 필드만 공란("")으로 기록한다.
            #  · 무가드(종전)의 문제: 거부인데 strength=0.0 기록 = '멀쩡해 보이는 오염행'
            #    (v2 신호필터 원천 오염·7/20 사고 자매).
            #  · 행 폐기(1차 설계)의 문제: ticks CSV에서 ask1/bid1을 읽는 소비자는 0인데
            #    (유일 판독기 playbook_shadow._read_ticks = time/price/change_rate/strength만)
            #    모두가 쓰는 price 관측까지 버려져 스로틀 오후 집중 시 멀쩡한 날이
            #    TRUNCATED로 판정 유효일에서 통째 탈락(반대 방향 과교정).
            #  · 공란은 판독기에서 0으로 강제되어(신호 false-negative만 발생·오진입 없음)
            #    보수적이며, 원본 CSV에선 정상 0.0과 구분돼 감사 가능.
            # soft는 백오프 미발동 — 실제 스로틀 스톰이면 r1도 거부돼 hard 경로가 건다.
            soft = False

            # 2) 체결 - 체결강도
            h2 = {**common_headers, "tr_id": "FHKST01010300"}
            r2 = requests.get(
                f"{base}/uapi/domestic-stock/v1/quotations/inquire-ccnl",
                headers=h2, params=common_params, timeout=5,
            )
            j2 = r2.json()
            if _refused(r2, j2):
                self._last_api_error = f"r2 {j2.get('msg_cd', '')} {str(j2.get('msg1', ''))[:40]}".strip()
                row["strength"] = ""     # 미상 — 정상 0.0(무체결)과 구분해 기록
                soft = True
            else:
                d2_list = j2.get("output") or []
                if isinstance(d2_list, list) and d2_list:
                    row["strength"] = _safe_float(d2_list[0].get("tday_rltv"))
                else:
                    row["strength"] = 0.0   # rt_cd=0 + 빈 output(장초 무체결 등) = 정상

            time.sleep(0.05)

            # 3) 호가 - 매도호가1, 매수호가1
            h3 = {**common_headers, "tr_id": "FHKST01010200"}
            r3 = requests.get(
                f"{base}/uapi/domestic-stock/v1/quotations/inquire-asking-price-exp-ccn",
                headers=h3, params=common_params, timeout=5,
            )
            j3 = r3.json()
            if _refused(r3, j3):
                self._last_api_error = f"r3 {j3.get('msg_cd', '')} {str(j3.get('msg1', ''))[:40]}".strip()
                row["ask1"] = ""
                row["bid1"] = ""
                soft = True
            else:
                d3 = j3.get("output1") or {}
                # ask1/bid1=0 자체는 상·하한가에서 정상이라 값 가드는 없음(rt_cd 거부만 구분)
                row["ask1"] = _safe_int(d3.get("askp1"))
                row["bid1"] = _safe_int(d3.get("bidp1"))

            if soft:
                self._api_soft += 1
            self._prev_volume[code] = volume
            return row

        except Exception as e:
            # 네트워크 예외(timeout/conn)도 실패로 집계 — 안 하면 전종목 timeout 시 사이클
            # 요약이 침묵(0/2500인데 경고 없음)한다(7/21 검수 관점2 D·"조용한 실패 금지").
            self._api_fail += 1
            self._last_api_error = f"네트워크 예외: {str(e)[:40]}"
            logger.warning(f"[{code}] 스냅샷 실패: {e}")
            return None

    def poll_once(self, codes: list) -> int:
        """전종목 1회 폴링 → CSV append

        Returns: 성공 종목 수
        """
        today = date.today().strftime("%Y%m%d")
        save_dir = _ensure_dir(today)

        ok = 0
        self._api_fail = 0
        self._api_soft = 0
        self._last_api_error = None
        streak = 0          # 연속 실패 — 레이트리밋/토큰만료 escalation 판단
        reset_done = False  # 사이클당 broker 재발급 1회 (토큰 만료 복구)
        for i, code in enumerate(codes):
            row = self._fetch_snapshot(code)
            if row is None:
                # ★ 실패 경로도 성공 경로와 동일 페이싱. early return이 _fetch_snapshot의
                # sleep 2회를 건너뛰므로, 여기서 쉬지 않으면 장애 중 요청 속도가 오히려
                # ~3배로 뛰어 레이트리밋을 자기증폭시킨다(7/20 fix 1차본의 결함).
                time.sleep(0.05)
                streak += 1
                # 연속 실패 FAIL_BACKOFF_AT건'마다' 백오프(50·100·150…) — 기존 (50,100)만
                # 발동은 151+ 무백오프 질주를 남겼다(7/21 검수 관점2 C).
                if streak % self.FAIL_BACKOFF_AT == 0:
                    logger.warning(f"[tick] 연속 실패 {streak}건 — {self.FAIL_BACKOFF_SEC}s 백오프 "
                                   f"(사유: {self._last_api_error or '미상'})")
                    time.sleep(self.FAIL_BACKOFF_SEC)
                if streak >= self.FAIL_RESET_AT and not reset_done:
                    # 토큰 만료 계열이면 재발급 없이는 하루 종일 0 수집 — 1회 재생성 시도
                    logger.error(f"[tick] 연속 실패 {streak}건 — broker 재생성(토큰 재발급) 시도")
                    self._broker = None
                    reset_done = True
                continue
            streak = 0

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

        # 사이클 건강도 가시화 — 조용한 실패 금지(7/20 사고: 전 종목 0인데 '성공' 집계)
        # hard=행 미기록(r1 거부·price 결손·예외) / soft=필드 결손 기록(r2/r3 거부 — M1)
        if self._api_fail or self._api_soft:
            share = 100 * self._api_fail / max(len(codes), 1)
            level = logger.error if share >= 50 else logger.warning
            level(f"[tick] API 거부 hard {self._api_fail}(행 미기록) / "
                  f"soft {self._api_soft}(strength·호가 공란 기록) / {len(codes)}종목 "
                  f"— 최근 사유: {self._last_api_error or '미상'}")
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
                logger.info("장 마감 - 체결 폴링 종료")
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
                with open(csv, "r", encoding="utf-8") as f:
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
