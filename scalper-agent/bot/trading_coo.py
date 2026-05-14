# -*- coding: utf-8 -*-
"""
Trading COO (Chief Operating Officer) — 매매봇 운영 총괄 시스템
=================================================================
텔레그램 스케줄러 위에 올리는 래퍼 레이어.
기존 모듈을 절대 수정하지 않고, 실행 순서 보장 + 예외 격리 + 상태 기록.

7-Group 구조:
  G1 MORNING_PREP   — 06:30~08:30 (데이터 검증, 추천 생성, 브레인)
  A15 동시호가 스캐너 — 08:30~08:53 (예상체결가 모니터링, 갭 알림)
  G2 MORNING_LAUNCH  — 08:55~09:05 (선취매 머지, 자동매매 ON)
  G3 INTRADAY_INIT   — 09:00~09:35 (Eye/MACD/TV 초기화)
  G4 INTRADAY_LOOP   — 09:35~15:10 (30초 루프, 포지션 감시)
  G5 MARKET_CLOSE    — 15:10~16:00 (청산, 수집, DART)
  G6 DATA_PIPELINE   — 16:00~17:30 (일봉, 수급, parquet, 유니버스)
  G7 EVENING_BRAIN   — 16:35~17:00 (학습, 브레인, 인사이트, NXT)

STEP 3-1: 기본 클래스 스켈레톤
  - run_job_safe / run_parallel / save_state / load_state
  - save_run_log / get_status_summary

STEP 3-2: G1 MORNING_PREP 그룹 실행 함수
  - run_job_safe_async / run_parallel_async (async 래퍼)
  - run_g1() — 10개 잡 병렬 실행
  - save_morning_state() — _morning_data 영속화
STEP 3-3: G2 MORNING_LAUNCH 그룹 실행 함수
  - run_g2() — 순차 실행 (G1 대기 → predawn → brief → scan → flowx)
  - _fallback_a12() — 모닝스캔 실패 시 recommendation.json 직접 로드
STEP 3-4: G3 INTRADAY_INIT 그룹 실행 함수
  - run_g3() — 5개 잡 병렬 (B1/B2/B4/B5/B6) + B6 실패 플래그
STEP 3-5: G4 INTRADAY_LOOP 그룹
  - G4_JOBS 레지스트리 (8개 반복 잡 정의)
  - run_g4_setup() — 등록 + 감시 시작
  - check_monitor_health() — 5분마다 B3 생존 확인 (positions.json + 로그 mtime)
  - _fallback_b3() — 모니터 크래시 시 재호출 + 진입 차단
STEP 3-6: G5 MARKET_CLOSE 그룹 실행 함수
  - run_g5() — 순차+병렬 (reeval → eod_close → flowx+paper → 분봉수집)
  - _fallback_b15() — EOD 청산 실패 시 미청산 종목 경고
STEP 3-7: G6 DATA_PIPELINE 그룹 실행 함수
  - run_g6() — 순차체인 (flowx→수집→병렬→검증) + g6_mode 결정
  - _fallback_collect_daily() — 5분 대기 재시도 → 전일 데이터 폴백
  - _fallback_verify_data() — 재검증 → DEGRADED 모드
STEP 3-8: G7 EVENING_BRAIN 그룹 실행 함수
  - run_g7() — 3-Stage (신호+스윙+브레인 → 학습→★분석 → 클로징+선취매+MACD+TRIX+국적+헬스)
  - _fallback_evening_analysis() — 3분 재시도 → 전일 recommendation.json 유지
STEP 3-9: setup_schedule() + telegram_bot.py 연결
  - setup_schedule(jq) — G1~G7 시간별 등록
  - telegram_bot.py: COO 초기화 + /coo 명령어
"""

import asyncio
import json
import logging
import os
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, time as dtime, timezone, timedelta
from enum import Enum
from pathlib import Path
from typing import Callable, Coroutine, Dict, List, Optional, Tuple

logger = logging.getLogger("BH.COO")

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_STORE = BASE_DIR / "data_store"

# ── 상태 파일 ──
COO_STATE_PATH = DATA_STORE / "coo_state.json"
COO_RUN_LOG_PATH = DATA_STORE / "coo_run_log.json"
MORNING_STATE_PATH = DATA_STORE / "morning_state.json"


class GroupStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    DONE = "done"
    PARTIAL = "partial"   # 일부 잡 실패
    FAILED = "failed"     # 전체 실패
    SKIPPED = "skipped"   # 비거래일 등


class JobResult:
    """단일 잡 실행 결과"""
    __slots__ = ("name", "success", "elapsed", "error", "timestamp")

    def __init__(self, name: str, success: bool, elapsed: float,
                 error: Optional[str] = None):
        self.name = name
        self.success = success
        self.elapsed = elapsed
        self.error = error
        self.timestamp = datetime.now().strftime("%H:%M:%S")

    def to_dict(self) -> dict:
        d = {
            "name": self.name,
            "success": self.success,
            "elapsed_sec": round(self.elapsed, 2),
            "timestamp": self.timestamp,
        }
        if self.error:
            d["error"] = self.error[:200]  # 에러 메시지 200자 제한
        return d


class TradingCOO:
    """매매봇 운영 총괄 — 실행 순서 보장 + 예외 격리 + 상태 기록"""

    def __init__(self, bot=None, auto_trader=None, cfo=None, cto=None):
        self._lock = threading.Lock()
        self.bot = bot              # TelegramBot 인스턴스
        self.auto_trader = auto_trader  # AutoTrader 인스턴스
        self.cfo = cfo              # TradingCFO 인스턴스
        self.cto = cto              # TradingCTO 인스턴스

        # G3 B6 TV 초기화 플래그 (B9 TV스캔 스킵 판단용)
        self._tv_init_ok = True
        # G4 B3 모니터 마지막 실행 시간 (헬스체크용)
        self._last_monitor_run = datetime.now()
        self._b3_fallback_count = 0  # B3 연속 실패 카운터 (무한루프 방지)
        # G6 데이터 파이프라인 모드 (NORMAL/STALE/DEGRADED)
        self._g6_mode = "NORMAL"
        # C13 추천 파이프라인 실행 중 플래그 (AUTO-RECOVERY 2중 실행 방지)
        self._c13_recommendation_running = False

        # 그룹별 상태
        self.group_status: Dict[str, str] = {
            f"G{i}": GroupStatus.PENDING
            for i in range(1, 8)
        }

        # 오늘 실행 로그 (그룹 → 잡 결과 리스트)
        self.run_log: Dict[str, List[dict]] = {}

        # 오늘 날짜 (자정 넘기면 리셋)
        self.today = datetime.now().strftime("%Y-%m-%d")

        # 이전 상태 복원
        self.load_state()
        logger.info("[COO] TradingCOO 초기화 완료")

    # ─────────────────────────────────────────────
    # 백그라운드 태스크 예외 콜백
    # ─────────────────────────────────────────────
    @staticmethod
    def _bg_task_done_cb(task: asyncio.Task) -> None:
        """fire-and-forget 백그라운드 태스크의 예외를 로깅."""
        if task.cancelled():
            logger.warning(f"[COO] 백그라운드 태스크 취소됨: {task.get_name()}")
            return
        exc = task.exception()
        if exc:
            logger.error(f"[COO] 백그라운드 태스크 예외 ({task.get_name()}): {exc}", exc_info=exc)

    # ─────────────────────────────────────────────
    # run_job_safe: 단일 잡 래퍼
    # ─────────────────────────────────────────────
    def run_job_safe(
        self,
        name: str,
        func: Callable,
        *args,
        timeout: Optional[float] = None,
        **kwargs,
    ) -> JobResult:
        """단일 잡을 try/except + 타임아웃 + 소요시간 측정으로 실행.

        Args:
            name: 잡 이름 (예: "data_verify")
            func: 실행할 함수
            timeout: 초 단위 타임아웃 (None이면 무제한)
        Returns:
            JobResult
        """
        start = time.monotonic()
        try:
            if timeout:
                # 타임아웃이 필요한 경우 별도 스레드에서 실행
                result_holder = [None]
                error_holder = [None]

                def _target():
                    try:
                        result_holder[0] = func(*args, **kwargs)
                    except Exception as e:
                        error_holder[0] = e

                t = threading.Thread(target=_target, daemon=True)
                t.start()
                t.join(timeout=timeout)

                if t.is_alive():
                    elapsed = time.monotonic() - start
                    logger.warning(f"[COO] {name} 타임아웃 ({timeout}초)")
                    return JobResult(name, False, elapsed,
                                    f"TIMEOUT ({timeout}s)")

                if error_holder[0]:
                    elapsed = time.monotonic() - start
                    err = str(error_holder[0])
                    logger.error(f"[COO] {name} 실패: {err}")
                    return JobResult(name, False, elapsed, err)
            else:
                func(*args, **kwargs)

            elapsed = time.monotonic() - start
            logger.info(f"[COO] {name} 완료 ({elapsed:.1f}초)")
            return JobResult(name, True, elapsed)

        except Exception as e:
            elapsed = time.monotonic() - start
            err = str(e)
            logger.error(f"[COO] {name} 예외: {err}")
            return JobResult(name, False, elapsed, err)

    # ─────────────────────────────────────────────
    # run_parallel: 병렬 실행 래퍼
    # ─────────────────────────────────────────────
    def run_parallel(
        self,
        jobs: List[Tuple[str, Callable]],
        max_workers: int = 3,
        timeout_per_job: Optional[float] = None,
    ) -> List[JobResult]:
        """여러 잡을 병렬 실행, 개별 예외 격리.

        Args:
            jobs: [(name, func), ...] 리스트
            max_workers: 동시 실행 수
            timeout_per_job: 잡별 타임아웃 (초)
        Returns:
            JobResult 리스트 (완료 순서)
        """
        results: List[JobResult] = []

        if not jobs:
            return results

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {}
            for name, func in jobs:
                f = executor.submit(self.run_job_safe, name, func,
                                    timeout=timeout_per_job)
                futures[f] = name

            for future in as_completed(futures):
                job_name = futures[future]
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    # run_job_safe 자체가 예외를 내는 경우 (거의 불가능)
                    logger.error(f"[COO] run_parallel 예외 {job_name}: {e}")
                    results.append(JobResult(job_name, False, 0, str(e)))

        ok = sum(1 for r in results if r.success)
        fail = len(results) - ok
        logger.info(f"[COO] 병렬 완료: {ok}성공 / {fail}실패 "
                     f"(총 {len(results)}개)")
        return results

    # ─────────────────────────────────────────────
    # 그룹 상태 업데이트
    # ─────────────────────────────────────────────
    def update_group(self, group: str, results: List[JobResult]):
        """그룹 실행 결과에 따라 상태 업데이트 + 로그 저장.

        Args:
            group: "G1" ~ "G7"
            results: 해당 그룹의 JobResult 리스트
        """
        with self._lock:
            all_ok = all(r.success for r in results)
            any_ok = any(r.success for r in results)

            if all_ok:
                self.group_status[group] = GroupStatus.DONE
            elif any_ok:
                self.group_status[group] = GroupStatus.PARTIAL
            else:
                self.group_status[group] = GroupStatus.FAILED

            self.run_log[group] = [r.to_dict() for r in results]

        self.save_state()
        self.save_run_log()

    # ─────────────────────────────────────────────
    # save_state / load_state — coo_state.json
    # ─────────────────────────────────────────────
    def save_state(self):
        """그룹 상태를 coo_state.json에 원자적으로 저장."""
        self.today = datetime.now().strftime("%Y-%m-%d")
        state = {
            "date": self.today,
            "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "groups": dict(self.group_status),
        }
        tmp = COO_STATE_PATH.with_suffix(".json.tmp")
        try:
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(state, f, ensure_ascii=False, indent=2)
            tmp.rename(COO_STATE_PATH)
        except Exception as e:
            logger.error(f"[COO] save_state 실패: {e}")
            try:
                tmp.unlink(missing_ok=True)
            except Exception:
                pass

    def load_state(self):
        """coo_state.json에서 오늘 상태 복원. 날짜 다르면 리셋."""
        if not COO_STATE_PATH.exists():
            return

        try:
            with open(COO_STATE_PATH, "r", encoding="utf-8") as f:
                state = json.load(f)

            if state.get("date") != self.today:
                logger.info("[COO] 날짜 변경 → 상태 리셋")
                return  # 새 날이면 pending 상태 유지

            saved_groups = state.get("groups", {})
            for g in self.group_status:
                if g in saved_groups:
                    self.group_status[g] = saved_groups[g]

            logger.info(f"[COO] 상태 복원: {self.group_status}")
        except Exception as e:
            logger.warning(f"[COO] load_state 실패: {e}")

    # ─────────────────────────────────────────────
    # save_run_log — coo_run_log.json
    # ─────────────────────────────────────────────
    def save_run_log(self):
        """실행 로그를 coo_run_log.json에 원자적으로 저장."""
        # 자정 넘김 방지: 저장 시점의 실제 날짜 사용
        self.today = datetime.now().strftime("%Y-%m-%d")
        log_data = {
            "date": self.today,
            "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "groups": self.run_log,
        }
        tmp = COO_RUN_LOG_PATH.with_suffix(".json.tmp")
        try:
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(log_data, f, ensure_ascii=False, indent=2)
            tmp.rename(COO_RUN_LOG_PATH)
        except Exception as e:
            logger.error(f"[COO] save_run_log 실패: {e}")
            try:
                tmp.unlink(missing_ok=True)
            except Exception:
                pass

    # ─────────────────────────────────────────────
    # get_status_summary — /coo 텔레그램 명령어용
    # ─────────────────────────────────────────────
    def get_status_summary(self) -> str:
        """현재 COO 상태를 텔레그램 /coo 명령어용 텍스트로 반환."""
        group_names = {
            "G1": "MORNING_PREP",
            "G2": "MORNING_LAUNCH",
            "G3": "INTRADAY_INIT",
            "G4": "INTRADAY_LOOP",
            "G5": "MARKET_CLOSE",
            "G6": "DATA_PIPELINE",
            "G7": "EVENING_BRAIN",
        }

        status_icons = {
            GroupStatus.PENDING: "⏳",
            GroupStatus.RUNNING: "🔄",
            GroupStatus.DONE: "✅",
            GroupStatus.PARTIAL: "⚠️",
            GroupStatus.FAILED: "❌",
            GroupStatus.SKIPPED: "⏭️",
        }

        lines = [f"📋 COO 운영 현황 ({self.today})", ""]

        for g in [f"G{i}" for i in range(1, 8)]:
            status = self.group_status.get(g, GroupStatus.PENDING)
            icon = status_icons.get(status, "❓")
            name = group_names.get(g, g)

            # 실행 로그에서 소요시간/실패 집계
            group_jobs = self.run_log.get(g, [])
            if group_jobs:
                total_sec = sum(j.get("elapsed_sec", 0) for j in group_jobs)
                fails = [j["name"] for j in group_jobs if not j.get("success")]
                detail = f" ({total_sec:.0f}초"
                if fails:
                    detail += f", 실패: {','.join(fails)}"
                detail += ")"
            else:
                detail = ""

            lines.append(f"{icon} {g} {name}{detail}")

        # 전체 요약
        done = sum(1 for s in self.group_status.values()
                   if s in (GroupStatus.DONE, GroupStatus.SKIPPED))
        total = len(self.group_status)
        lines.append("")
        lines.append(f"진행: {done}/{total} 그룹 완료")

        return "\n".join(lines)

    # ═════════════════════════════════════════════
    # STEP 3-2: Async 래퍼 + G1 MORNING_PREP
    # ═════════════════════════════════════════════

    # ─────────────────────────────────────────────
    # run_job_safe_async: async 단일 잡 래퍼
    # ─────────────────────────────────────────────
    async def run_job_safe_async(
        self,
        name: str,
        coro: Coroutine,
        timeout: Optional[float] = None,
        retry_factory: Optional[Callable] = None,
    ) -> JobResult:
        """async 잡을 try/except + asyncio.wait_for 타임아웃으로 실행.

        타임아웃/예외 발생 시 retry_factory가 있으면 1회 자동 재시도.
        재시도 시 타임아웃을 1.5배로 확장.

        Args:
            name: 잡 이름
            coro: 실행할 코루틴 (이미 생성된 상태)
            timeout: 초 단위 타임아웃 (None이면 무제한)
            retry_factory: 재시도용 코루틴 팩토리 (없으면 재시도 안 함)
        Returns:
            JobResult
        """
        start = time.monotonic()
        try:
            if timeout:
                await asyncio.wait_for(coro, timeout=timeout)
            else:
                await coro

            elapsed = time.monotonic() - start
            logger.info(f"[COO] {name} 완료 ({elapsed:.1f}초)")
            return JobResult(name, True, elapsed)

        except asyncio.TimeoutError:
            elapsed = time.monotonic() - start
            if retry_factory:
                logger.warning(
                    f"[COO] {name} 타임아웃 ({timeout}초) → 1회 재시도 "
                    f"(확장 {int(timeout * 1.5)}초)"
                )
                return await self._retry_once(
                    name, retry_factory, int(timeout * 1.5), start
                )
            logger.warning(f"[COO] {name} 타임아웃 ({timeout}초)")
            return JobResult(name, False, elapsed, f"TIMEOUT ({timeout}s)")

        except Exception as e:
            elapsed = time.monotonic() - start
            err = str(e)
            if retry_factory:
                logger.warning(
                    f"[COO] {name} 예외 ({err}) → 1회 재시도"
                )
                return await self._retry_once(
                    name, retry_factory, timeout, start
                )
            logger.error(f"[COO] {name} 예외: {err}")
            return JobResult(name, False, elapsed, err)

    async def _retry_once(
        self,
        name: str,
        factory: Callable,
        timeout: Optional[float],
        original_start: float,
    ) -> JobResult:
        """1회 재시도 실행."""
        try:
            coro2 = factory()
            if timeout:
                await asyncio.wait_for(coro2, timeout=timeout)
            else:
                await coro2
            elapsed = time.monotonic() - original_start
            logger.info(f"[COO] {name} 재시도 성공 ({elapsed:.1f}초)")
            return JobResult(name, True, elapsed)
        except asyncio.TimeoutError:
            elapsed = time.monotonic() - original_start
            logger.error(f"[COO] {name} 재시도도 타임아웃 ({timeout}초)")
            return JobResult(name, False, elapsed, f"RETRY_TIMEOUT ({timeout}s)")
        except Exception as e2:
            elapsed = time.monotonic() - original_start
            logger.error(f"[COO] {name} 재시도 실패: {e2}")
            return JobResult(name, False, elapsed, f"RETRY_FAIL: {e2}")

    # ─────────────────────────────────────────────
    # run_parallel_async: async 병렬 실행 래퍼
    # ─────────────────────────────────────────────
    async def run_parallel_async(
        self,
        jobs: list,
        timeout_per_job: Optional[float] = None,
    ) -> List[JobResult]:
        """async 잡들을 asyncio.gather로 병렬 실행, 개별 예외 격리.

        Args:
            jobs: [(name, coroutine), ...] 또는
                  [(name, coroutine, retry_factory), ...] 리스트
                  retry_factory가 있으면 타임아웃/예외 시 1회 자동 재시도
            timeout_per_job: 잡별 타임아웃 (초)
        Returns:
            JobResult 리스트
        """
        if not jobs:
            return []

        tasks = []
        for job in jobs:
            if len(job) == 3:
                name, coro, factory = job
                tasks.append(self.run_job_safe_async(
                    name, coro, timeout=timeout_per_job,
                    retry_factory=factory,
                ))
            else:
                name, coro = job
                tasks.append(self.run_job_safe_async(
                    name, coro, timeout=timeout_per_job,
                ))
        results = await asyncio.gather(*tasks)

        ok = sum(1 for r in results if r.success)
        fail = len(results) - ok
        logger.info(f"[COO] async 병렬 완료: {ok}성공 / {fail}실패 "
                     f"(총 {len(results)}개)")
        return list(results)

    # ─────────────────────────────────────────────
    # run_g1: G1 MORNING_PREP (08:00~08:50)
    # ─────────────────────────────────────────────
    async def run_g1(self, context=None):
        """G1 MORNING_PREP — 12개 잡 병렬 실행.

        전부 non-critical → 개별 실패해도 계속 진행.
        결과를 update_group + morning_state.json 영속화.

        잡 목록 (A1~A12 + A5P):
          A1  job_us_market_check    — 미국시장 체크 (AutoTrader)
          A2  _job_policy_scan       — 정책 트래커 (TelegramBot)
          A3  _job_global_event_scan — 해외 이벤트 (TelegramBot)
          A4  job_nxt_morning_sell   — NXT 아침 매도 (AutoTrader)
          A5  _job_dart_refresh      — DART 공시 (TelegramBot)
          A5P _job_nxt_paper_morning_close — NXT Paper 아침 청산 (COO)
          A6  _job_options_expiry_alert — 옵션 만기 (TelegramBot)
          A7  _job_position_guardian — 포지션 가디언 (TelegramBot)
          A8  job_premium_levels     — 프리미엄 레벨 (AutoTrader)
          A9  _job_rebuild_universe  — 유니버스 리빌드 (TelegramBot)
          A10 _job_premove_scan      — 사전감지 (TelegramBot)
          A11 _job_us_overnight_filter — 미국장 야간 필터 (COO)
          A11B ewy_holdings_collector — EWY 보유종목 수집+업로드 (A11 후 순차)
          A12 _job_daytrading_picks(confirmed) — 단타 TOP픽 확정 (A11 후 순차, 07:35)
        """
        logger.info("[COO] ═══ G1 MORNING_PREP 시작 ═══")
        self.group_status["G1"] = GroupStatus.RUNNING
        self.save_state()

        # ── A0: 전일 데이터 백필 (병렬 전 순차 실행) ──
        # 16:25 verify는 flow 수집 중 통과하지만 C34 등 HIGH 잡이 실패할 수 있음.
        # A1~A12(A11 미국장 필터, A10 premove 등)가 전일 cycle_scan/sector_history를
        # 읽기 전에 반드시 백필이 완료돼야 함.
        try:
            a0_result = await asyncio.wait_for(
                self._job_morning_backfill(context), timeout=900
            )
            logger.info(f"[COO] A0 백필 결과: {a0_result}")
        except asyncio.TimeoutError:
            logger.warning("[COO] A0 백필 타임아웃 (15분) — 계속 진행")
        except Exception as e:
            logger.warning(f"[COO] A0 백필 에러: {e}")

        # 잡 → 코루틴 매핑 (self.bot / self.auto_trader에서 참조)
        jobs: List[Tuple[str, Coroutine]] = []

        # AutoTrader 잡 (3개)
        if self.auto_trader:
            jobs.extend([
                ("A1_us_market_check",
                 self.auto_trader.job_us_market_check(context)),
                ("A4_nxt_morning_sell",
                 self.auto_trader.job_nxt_morning_sell(context)),
                ("A8_premium_levels",
                 self.auto_trader.job_premium_levels(context)),
            ])
        else:
            logger.warning("[COO] auto_trader 미연결 — A1/A4/A8 스킵")

        # NXT Paper 아침 청산 (COO 자체 잡)
        jobs.append((
            "A5P_nxt_paper_morning_close",
            self._job_nxt_paper_morning_close(context),
        ))

        # 미국장 야간 필터 (COO 자체 잡)
        jobs.append((
            "A11_us_overnight_filter",
            self._job_us_overnight_filter(context),
        ))

        # TelegramBot 잡 (7개)
        if self.bot:
            jobs.extend([
                ("A2_policy_scan",
                 self.bot._job_policy_scan(context)),
                ("A3_global_event_scan",
                 self.bot._job_global_event_scan(context)),
                ("A5_dart_refresh",
                 self.bot._job_dart_refresh(context)),
                ("A6_options_expiry_alert",
                 self.bot._job_options_expiry_alert(context)),
                ("A7_position_guardian",
                 self.bot._job_position_guardian(context)),
                ("A9_rebuild_universe",
                 self.bot._job_rebuild_universe(context)),
                ("A10_premove_scan",
                 self.bot._job_premove_scan(context)),
            ])
        else:
            logger.warning("[COO] bot 미연결 — A2~A10 스킵")

        if not jobs:
            logger.error("[COO] G1: 실행할 잡 없음 (bot/auto_trader 미연결)")
            self.group_status["G1"] = GroupStatus.FAILED
            self.save_state()
            return []

        # 12개 잡 병렬 실행 — 개별 타임아웃 300초 (5분)
        results = await self.run_parallel_async(jobs, timeout_per_job=300)

        # ── A11B: EWY 보유종목 수집 + 업로드 (A11 후 순차) ──
        try:
            from data.ewy_holdings_collector import collect_ewy_holdings
            from data.upload_ewy_holdings import upload_ewy_holdings

            ewy_result = await asyncio.to_thread(collect_ewy_holdings)
            if ewy_result:
                await asyncio.to_thread(upload_ewy_holdings, ewy_result)
                logger.info(
                    f"[COO] A11B EWY 수집 완료: "
                    f"{ewy_result.get('total_stocks', 0)}종목"
                )
        except Exception as e:
            logger.warning(f"[COO] A11B EWY 수집 실패 (무시): {e}")

        # ── A12: 단타 TOP픽 확정 (미국장 반영 · A11 후 순차) ──
        # A11이 EWY/KS200 데이터를 us_market_overnight.json에 저장 후 실행
        try:
            a12 = await self.run_job_safe_async(
                "A12_daytrading_picks_confirmed",
                self._job_daytrading_picks(context, mode="confirmed"),
                timeout=300,
            )
            results.append(a12)
        except Exception as e:
            logger.warning(f"[COO] A12 단타 확정 실패 (무시): {e}")

        # 그룹 상태 업데이트 + 로그 저장
        self.update_group("G1", results)

        # _morning_data 영속화
        self.save_morning_state()

        logger.info("[COO] ═══ G1 MORNING_PREP 완료 ═══")
        return results

    # ─────────────────────────────────────────────
    # save_morning_state — morning_state.json
    # ─────────────────────────────────────────────
    def save_morning_state(self):
        """bot._morning_data를 morning_state.json에 원자적으로 영속화.

        봇 재시작 시 G1 결과를 복원할 수 있도록 백업.
        """
        if not self.bot or not hasattr(self.bot, "_morning_data"):
            return

        morning_data = self.bot._morning_data
        if not morning_data:
            return

        state = {
            "date": self.today,
            "saved_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "data": {},
        }

        # _morning_data의 각 키를 JSON 직렬화 가능한 형태로 저장
        for key, value in morning_data.items():
            try:
                # JSON 직렬화 테스트 (직렬화 불가능한 객체 방어)
                json.dumps(value, ensure_ascii=False, default=str)
                state["data"][key] = value
            except (TypeError, ValueError):
                state["data"][key] = str(value)[:500]

        tmp = MORNING_STATE_PATH.with_suffix(".json.tmp")
        try:
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(state, f, ensure_ascii=False, indent=2, default=str)
            tmp.rename(MORNING_STATE_PATH)
            logger.info(f"[COO] morning_state 저장 완료 "
                         f"(키: {list(state['data'].keys())})")
        except Exception as e:
            logger.error(f"[COO] save_morning_state 실패: {e}")
            try:
                tmp.unlink(missing_ok=True)
            except Exception:
                pass

    # ═════════════════════════════════════════════
    # STEP 3-3: G2 MORNING_LAUNCH (08:55~09:05)
    # ═════════════════════════════════════════════

    async def run_g2(self, context=None):
        """G2 MORNING_LAUNCH — 순차 실행 (의존 체인).

        순서:
          1. G1 완료 확인 (최대 2분 대기, 미완료면 강제 진행)
          2. A13 merge_predawn_on_open (non-critical, sync)
          3. A11 ★ _send_morning_brief (critical)
          4. A12 ★ job_morning_scan (critical + FALLBACK)
          5. A14 _job_flowx_vip_content (non-critical)
        """
        logger.info("[COO] ═══ G2 MORNING_LAUNCH 시작 ═══")
        self.group_status["G2"] = GroupStatus.RUNNING
        self.save_state()

        results: List[JobResult] = []

        # ── 1) G1 완료 대기 (최대 2분) ──
        g1_status = self.group_status.get("G1", GroupStatus.PENDING)
        if g1_status not in (GroupStatus.DONE, GroupStatus.PARTIAL,
                             GroupStatus.SKIPPED):
            logger.warning("[COO] G1 미완료 — 최대 120초 대기")
            waited = 0
            while waited < 120:
                await asyncio.sleep(5)
                waited += 5
                g1_status = self.group_status.get("G1", GroupStatus.PENDING)
                if g1_status in (GroupStatus.DONE, GroupStatus.PARTIAL,
                                 GroupStatus.SKIPPED):
                    break
            if g1_status not in (GroupStatus.DONE, GroupStatus.PARTIAL,
                                 GroupStatus.SKIPPED):
                logger.warning("[COO] G1 2분 초과 — 강제 진행")

        # ── 2) A13 merge_predawn_on_open (non-critical, sync) ──
        if self.auto_trader:
            r = await self.run_job_safe_async(
                "A13_merge_predawn",
                asyncio.to_thread(self.auto_trader.merge_predawn_on_open),
                timeout=60,
            )
            results.append(r)
            if not r.success:
                logger.warning("[COO] A13 실패 — 선취매 없으면 무영향, 계속 진행")
        else:
            logger.warning("[COO] auto_trader 미연결 — A13 스킵")

        # ── 3) A11 ★ _send_morning_brief (critical) ──
        if self.bot:
            r = await self.run_job_safe_async(
                "A11_morning_brief",
                self.bot._send_morning_brief(context),
                timeout=120,
            )
            results.append(r)
            if not r.success:
                logger.error("[COO] A11 모닝브리프 실패 — 텔레그램 미전송, "
                             "A12는 계속 진행")
        else:
            logger.warning("[COO] bot 미연결 — A11 스킵")

        # ── 4) A12 ★ job_morning_scan (critical + FALLBACK) ──
        a12_ok = False
        if self.auto_trader:
            r = await self.run_job_safe_async(
                "A12_morning_scan",
                self.auto_trader.job_morning_scan(context),
                timeout=300,
            )
            results.append(r)
            a12_ok = r.success

            if not a12_ok:
                logger.error("[COO] A12 모닝스캔 실패 — FALLBACK 실행")
                a12_ok = await self._fallback_a12(context)
        else:
            logger.warning("[COO] auto_trader 미연결 — A12 스킵")

        # ── 5) A14 _job_flowx_vip_content (non-critical) ──
        if self.bot:
            r = await self.run_job_safe_async(
                "A14_flowx_vip",
                self.bot._job_flowx_vip_content(context),
                timeout=120,
            )
            results.append(r)
            if not r.success:
                logger.warning("[COO] A14 FLOWX VIP 실패 — 스킵")
        else:
            logger.warning("[COO] bot 미연결 — A14 스킵")

        # ── 그룹 상태 업데이트 ──
        self.update_group("G2", results)

        logger.info("[COO] ═══ G2 MORNING_LAUNCH 완료 ═══")
        return results

    # ─────────────────────────────────────────────
    # FALLBACK-A12: 모닝스캔 실패 시 복구
    # ─────────────────────────────────────────────
    async def _fallback_a12(self, context=None) -> bool:
        """A12 job_morning_scan 실패 시 폴백.

        a. recommendation.json 직접 로드 시도
        b. 그것도 실패 → _entry_watch = {} (빈 감시)
        c. 텔레그램 경고 발송

        Returns:
            True면 부분 복구 성공, False면 완전 실패
        """
        recovered = False
        rec_path = DATA_STORE / "recommendation.json"

        # (a) recommendation.json 직접 로드
        try:
            if rec_path.exists():
                with open(rec_path, "r", encoding="utf-8") as f:
                    rec_data = json.load(f)

                if rec_data and isinstance(rec_data, dict):
                    logger.info(f"[COO] FALLBACK-A12: recommendation.json "
                                f"직접 로드 성공 ({len(rec_data)}개 키)")
                    recovered = True
        except Exception as e:
            logger.error(f"[COO] FALLBACK-A12: recommendation.json "
                         f"로드 실패: {e}")

        # (b) 완전 실패 → 빈 감시
        if not recovered:
            logger.warning("[COO] FALLBACK-A12: 빈 감시 모드 — "
                           "자동매수 비활성")
            if self.auto_trader and hasattr(self.auto_trader, "_entry_watch"):
                self.auto_trader._entry_watch = {}

        # (c) 텔레그램 경고
        try:
            alert_fn = getattr(self.auto_trader, "_send_alert", None)
            if alert_fn:
                msg = ("⚠️ 모닝스캔 실패\n"
                       f"{'recommendation.json 직접 로드 성공' if recovered else '자동매수 비활성'}\n"
                       "수동 확인 필요")
                await asyncio.to_thread(alert_fn, msg)
        except Exception as e:
            logger.warning(f"[COO] FALLBACK-A12 텔레그램 경고 실패: {e}")

        return recovered

    # ═════════════════════════════════════════════
    # STEP 3-4: G3 INTRADAY_INIT (09:00~09:35)
    # ═════════════════════════════════════════════

    async def run_g3(self, context=None):
        """G3 INTRADAY_INIT — 5개 잡 병렬 실행.

        전부 non-critical → 개별 실패해도 G4 진행.
        B6 실패 시 tv_init_ok=False 기록 (이후 B9 TV스캔 스킵 판단용).

        잡 목록 (B1~B6 + B5R):
          B1  _job_war_startup       — 전쟁모드 시작 (TelegramBot)
          B2  _job_start_tick_polling — 체결 폴링 시작 (TelegramBot)
          B4  job_gap_support        — 갭 지지/저항 (AutoTrader)
          B5  _job_paper_register    — PAPER 등록 (TelegramBot)
          B5R _job_nxt_paper_reprice — NXT Paper 시가 재정산 (COO)
          B6  _job_intraday_tv_init  — TV 스캔 초기화 (TelegramBot)
        """
        logger.info("[COO] ═══ G3 INTRADAY_INIT 시작 ═══")
        self.group_status["G3"] = GroupStatus.RUNNING
        self.save_state()

        # ── 1) G2 완료 대기 (최대 1분) ──
        g2_status = self.group_status.get("G2", GroupStatus.PENDING)
        if g2_status not in (GroupStatus.DONE, GroupStatus.PARTIAL,
                             GroupStatus.SKIPPED):
            logger.warning("[COO] G2 미완료 — 최대 60초 대기")
            waited = 0
            while waited < 60:
                await asyncio.sleep(5)
                waited += 5
                g2_status = self.group_status.get("G2", GroupStatus.PENDING)
                if g2_status in (GroupStatus.DONE, GroupStatus.PARTIAL,
                                 GroupStatus.SKIPPED):
                    break
            if g2_status not in (GroupStatus.DONE, GroupStatus.PARTIAL,
                                 GroupStatus.SKIPPED):
                logger.warning("[COO] G2 1분 초과 — 강제 진행")

        # ── 2) 5개 잡 병렬 실행 ──
        jobs: List[Tuple[str, Coroutine]] = []
        pre_results = []

        # TelegramBot 잡 (3개 병렬 + B2 백그라운드)
        if self.bot:
            jobs.extend([
                ("B1_war_startup",
                 self.bot._job_war_startup(context)),
                ("B5_paper_register",
                 self.bot._job_paper_register(context)),
                ("B6_tv_init",
                 self.bot._job_intraday_tv_init(context)),
            ])
            # B2 tick_polling: 장중 6시간 연속 실행 → 백그라운드 task
            _b2_task = asyncio.create_task(self.bot._job_start_tick_polling(context))
            _b2_task.add_done_callback(self._bg_task_done_cb)
            pre_results.append(JobResult("B2_tick_polling", True, 0))
            logger.info("[COO] B2 tick_polling 백그라운드 시작")
        else:
            logger.warning("[COO] bot 미연결 — B1/B2/B5/B6 스킵")

        # NXT Paper 시가 재정산 (A5P 08:00 폴백 → 09:00 실시가 반영)
        jobs.append((
            "B5R_nxt_paper_reprice",
            self._job_nxt_paper_reprice(context),
        ))

        # AutoTrader 잡 (1개)
        if self.auto_trader:
            jobs.append(
                ("B4_gap_support",
                 self.auto_trader.job_gap_support(context)),
            )
        else:
            logger.warning("[COO] auto_trader 미연결 — B4 스킵")

        if not jobs:
            logger.error("[COO] G3: 실행할 잡 없음")
            self.group_status["G3"] = GroupStatus.FAILED
            self.save_state()
            return []

        # 개별 타임아웃 120초
        results = pre_results + await self.run_parallel_async(jobs, timeout_per_job=120)

        # ── 3) B6 실패 여부 기록 ──
        b6_results = [r for r in results if r.name == "B6_tv_init"]
        if b6_results and not b6_results[0].success:
            logger.warning("[COO] B6 TV초기화 실패 — "
                           "이후 B9 TV스캔 비활성 예정")
            self._tv_init_ok = False
        else:
            self._tv_init_ok = True

        # ── 그룹 상태 업데이트 ──
        self.update_group("G3", results)

        logger.info("[COO] ═══ G3 INTRADAY_INIT 완료 ═══")
        return results

    # ═════════════════════════════════════════════
    # STEP 3-5: G4 INTRADAY_LOOP (09:35~15:10)
    # ═════════════════════════════════════════════

    # G4는 기존 스케줄러가 반복 실행 → COO는 등록 + 감시만

    # ── 반복 잡 레지스트리 (실행은 기존 스케줄러) ──
    G4_JOBS = {
        "B3":  {"name": "job_monitor",        "interval": "30s",  "critical": True,
                "source": "auto_trader"},
        "B7":  {"name": "_job_news_sentiment", "interval": "3x/day", "critical": False,
                "source": "bot"},
        "B8":  {"name": "_job_war_tracker",    "interval": "60s",  "critical": False,
                "source": "bot"},
        "B9":  {"name": "_job_intraday_tv_scan", "interval": "30min", "critical": False,
                "source": "bot", "requires": "tv_init_ok"},
        "B10": {"name": "job_opening_range",   "interval": "once",  "critical": False,
                "source": "auto_trader"},
        "B11": {"name": "_job_dart_refresh",   "interval": "once",  "critical": False,
                "source": "bot"},
        "B12": {"name": "_job_preclose_scan",  "interval": "once",  "critical": False,
                "source": "bot"},
        "B13": {"name": "_send_preclose_brief", "interval": "once", "critical": False,
                "source": "bot"},
        "B14": {"name": "_job_algo_detect",     "interval": "60s",  "critical": False,
                "source": "bot"},
    }

    async def run_g4_setup(self, context=None):
        """G4 INTRADAY_LOOP 셋업 — 반복 잡 등록 + 감시 시작.

        G4는 다른 그룹과 달리 COO가 직접 실행하지 않음.
        기존 스케줄러(JobQueue)가 반복 실행하는 걸 COO가 감시.
        """
        logger.info("[COO] ═══ G4 INTRADAY_LOOP 셋업 ═══")
        self.group_status["G4"] = GroupStatus.RUNNING
        self._last_monitor_run = datetime.now()
        self.save_state()

        # B9 TV스캔: G3에서 B6 실패했으면 스킵 경고
        if not self._tv_init_ok:
            logger.warning("[COO] G4: B9 TV스캔 비활성 "
                           "(B6 TV초기화 실패)")

        logger.info(f"[COO] G4: 반복 잡 {len(self.G4_JOBS)}개 등록 완료 — "
                     "모니터 헬스체크 5분 간격 시작")
        return True

    async def check_monitor_health(self, context=None):
        """B3 job_monitor 생존 확인 (5분마다 실행).

        기존 코드 수정 없이 수동 감지:
          1. positions.json mtime 확인 (포지션 변경 시 갱신)
          2. 오늘 봇 로그 파일 mtime 확인 (log_event 호출 시 갱신)
          3. 두 파일 모두 5분+ 정체 → 모니터 크래시로 판단

        장중(09:05~15:10)에만 동작.
        """
        now = datetime.now()

        # 장중 아닌 시간은 패스
        market_start = now.replace(hour=9, minute=5, second=0)
        market_end = now.replace(hour=15, minute=10, second=0)
        if not (market_start <= now <= market_end):
            return

        stale_threshold = 300  # 5분

        # ── 감지 1: positions.json mtime ──
        positions_path = DATA_STORE / "positions.json"
        positions_age = float("inf")
        try:
            if positions_path.exists():
                mtime = os.path.getmtime(positions_path)
                positions_age = time.time() - mtime
        except Exception:
            pass

        # ── 감지 2: 오늘 봇 로그 mtime ──
        log_dir = BASE_DIR / "logs"
        today_log = log_dir / f"{now.strftime('%Y-%m-%d')}_bot.log"
        log_age = float("inf")
        try:
            if today_log.exists():
                mtime = os.path.getmtime(today_log)
                log_age = time.time() - mtime
        except Exception:
            pass

        # ── 판정: 둘 다 5분+ 정체 → 모니터 크래시 ──
        if positions_age < stale_threshold or log_age < stale_threshold:
            # 최소 하나가 최근 활동 있음 → 정상
            self._last_monitor_run = now
            self._b3_fallback_count = 0
            return

        # 연속 3회 이상 실패 시 더 이상 FALLBACK 시도 안 함 (로그 노이즈 방지)
        if self._b3_fallback_count >= 3:
            return  # 이미 경고 완료, 추가 시도 무의미

        # 둘 다 5분+ 정체
        self._b3_fallback_count += 1
        elapsed_min = min(positions_age, log_age) / 60
        logger.warning(f"[COO] B3 모니터 {elapsed_min:.1f}분 미응답 — "
                       f"FALLBACK-B3 실행 ({self._b3_fallback_count}/3)")
        await self._fallback_b3(context)

    async def _fallback_b3(self, context=None):
        """B3 job_monitor 크래시 시 폴백.

        a. job_monitor() 강제 재호출 시도
        b. 실패 → 신규 진입 차단 (_risk_blocked = True)
        c. 텔레그램 경고
        """
        recovered = False

        # (a) job_monitor 강제 재호출
        if self.auto_trader:
            try:
                r = await self.run_job_safe_async(
                    "B3_monitor_recovery",
                    self.auto_trader.job_monitor(context),
                    timeout=60,
                )
                if r.success:
                    logger.info("[COO] FALLBACK-B3: job_monitor 강제 재호출 성공")
                    self._last_monitor_run = datetime.now()
                    recovered = True
            except Exception as e:
                logger.error(f"[COO] FALLBACK-B3: 재호출 실패: {e}")

        # (b) 실패 → 신규 진입 차단
        if not recovered:
            logger.error("[COO] FALLBACK-B3: 모니터 복구 실패 — "
                         "신규 진입 차단")
            if self.auto_trader:
                self.auto_trader._risk_blocked = True

        # (c) 텔레그램 경고
        try:
            alert_fn = getattr(self.auto_trader, "_send_alert", None)
            if alert_fn:
                status = "강제 재호출 성공" if recovered else "신규 진입 차단 중"
                msg = f"🚨 B3 모니터 크래시 감지\n{status}\n수동 확인 필요"
                await asyncio.to_thread(alert_fn, msg)
        except Exception as e:
            logger.warning(f"[COO] FALLBACK-B3 텔레그램 경고 실패: {e}")

        return recovered

    # ═════════════════════════════════════════════
    # STEP 3-6: G5 MARKET_CLOSE (15:10~16:00)
    # ═════════════════════════════════════════════

    async def run_g5(self, context=None):
        """G5 MARKET_CLOSE — 순차 + 부분 병렬.

        순서:
          1. B14 job_daily_reeval (non-critical) — 동적 목표가 재평가
          2. B15 ★ job_eod_close (critical) — 장마감 청산 + FALLBACK
          3. B16+B17 병렬 (non-critical) — FLOWX 마감 + PAPER EOD
          4. C1 _job_collect_minutes (non-critical) — 분봉 수집
        """
        logger.info("[COO] ═══ G5 MARKET_CLOSE 시작 ═══")
        self.group_status["G5"] = GroupStatus.RUNNING
        self.group_status["G4"] = GroupStatus.DONE  # G4 장중 루프 종료
        self.save_state()

        results: List[JobResult] = []

        # ── 1) B14 job_daily_reeval (non-critical) ──
        if self.auto_trader:
            r = await self.run_job_safe_async(
                "B14_daily_reeval",
                self.auto_trader.job_daily_reeval(context),
                timeout=300,
            )
            results.append(r)
            if not r.success:
                logger.warning("[COO] B14 재평가 실패 — "
                               "기본 SL/TP로 B15 청산 진행")
        else:
            logger.warning("[COO] auto_trader 미연결 — B14 스킵")

        # ── 2) B15 ★ job_eod_close (critical) ──
        b15_ok = False
        if self.auto_trader:
            r = await self.run_job_safe_async(
                "B15_eod_close",
                self.auto_trader.job_eod_close(context),
                timeout=300,
            )
            results.append(r)
            b15_ok = r.success

            if not b15_ok:
                logger.error("[COO] B15 EOD 청산 실패 — FALLBACK 실행")
                await self._fallback_b15(context)
        else:
            logger.warning("[COO] auto_trader 미연결 — B15 스킵")

        # ── 3) B16 + B17 병렬 (non-critical) ──
        parallel_jobs: List[Tuple[str, Coroutine]] = []
        if self.bot:
            parallel_jobs.append(
                ("B16_flowx_close",
                 self.bot._job_flowx_close_daytrading(context)))
            parallel_jobs.append(
                ("B17_paper_eod",
                 self.bot._job_paper_eod(context)))

        if parallel_jobs:
            par_results = await self.run_parallel_async(
                parallel_jobs, timeout_per_job=120)
            results.extend(par_results)

        # ── 4) C1 _job_collect_minutes — 비활성화 (2026-04-04) ──
        # 장중 tick_polling이 실시간 처리, 저장 분봉은 검증 시 항상 FAIL → 낭비
        # 실매매 전환 시 필요하면 재활성화
        logger.info("[COO] C1 분봉 수집 — 비활성화 상태 (스킵)")

        # ── 그룹 상태 업데이트 ──
        self.update_group("G5", results)

        logger.info("[COO] ═══ G5 MARKET_CLOSE 완료 ═══")
        return results

    # ─────────────────────────────────────────────
    # FALLBACK-B15: EOD 청산 실패 시 복구
    # ─────────────────────────────────────────────
    async def _fallback_b15(self, context=None):
        """B15 job_eod_close 실패 시 폴백.

        a. 미청산 종목 리스트 수집
        b. 텔레그램 경고: "🚨 EOD 청산 실패 — 미청산 종목: {리스트}"
        c. 수동 청산 필요 알림
        """
        unclosed = []

        # (a) 미청산 종목 수집
        if self.auto_trader and hasattr(self.auto_trader, "_positions"):
            positions = self.auto_trader._positions or {}
            unclosed = list(positions.keys())
            logger.error(f"[COO] FALLBACK-B15: 미청산 종목 {len(unclosed)}개: "
                         f"{unclosed}")

        # (b) + (c) 텔레그램 경고
        try:
            alert_fn = getattr(self.auto_trader, "_send_alert", None)
            if alert_fn:
                if unclosed:
                    codes_str = ", ".join(unclosed[:10])
                    if len(unclosed) > 10:
                        codes_str += f" 외 {len(unclosed)-10}개"
                    msg = (f"🚨 EOD 청산 실패\n"
                           f"미청산 종목: {codes_str}\n"
                           f"수동 청산 필요!")
                else:
                    msg = ("🚨 EOD 청산 실패\n"
                           "포지션 확인 불가\n"
                           "수동 확인 필요!")
                await asyncio.to_thread(alert_fn, msg)
        except Exception as e:
            logger.warning(f"[COO] FALLBACK-B15 텔레그램 경고 실패: {e}")

    # ═════════════════════════════════════════════
    # STEP 3-7: G6 DATA_PIPELINE (16:00~17:30)
    # ═════════════════════════════════════════════

    async def run_g6(self, context=None):
        """G6 DATA_PIPELINE — 순차 체인 (장마감 데이터 수집의 핵심).

        순서:
          1. G5 완료 대기 (최대 5분)
          2. C2 _job_flowx_universe_update (non-critical)
          3. C3 ★ _job_collect_daily (CRITICAL + FALLBACK)
          4. C5+C6+C5Z 병렬 (non-critical, Z-score 사전 계산 포함)
          5. C7 ★ _job_verify_data (CRITICAL + FALLBACK)
          6. g6_mode 결정: NORMAL / STALE / DEGRADED
        """
        logger.info("[COO] ═══ G6 DATA_PIPELINE 시작 ═══")
        self.group_status["G6"] = GroupStatus.RUNNING
        self._g6_mode = "NORMAL"  # 기본값
        self.save_state()

        results: List[JobResult] = []

        # ── 1) G5 완료 대기 (최대 5분) ──
        g5_status = self.group_status.get("G5", GroupStatus.PENDING)
        if g5_status not in (GroupStatus.DONE, GroupStatus.PARTIAL,
                             GroupStatus.SKIPPED):
            logger.warning("[COO] G5 미완료 — 최대 300초 대기")
            waited = 0
            while waited < 300:
                await asyncio.sleep(10)
                waited += 10
                g5_status = self.group_status.get("G5", GroupStatus.PENDING)
                if g5_status in (GroupStatus.DONE, GroupStatus.PARTIAL,
                                 GroupStatus.SKIPPED):
                    break
            if g5_status not in (GroupStatus.DONE, GroupStatus.PARTIAL,
                                 GroupStatus.SKIPPED):
                logger.warning("[COO] G5 5분 초과 — 강제 진행")

        # ── 2) C2 + C4E 선행 병렬 (NXT 사전 수집은 C3 대기 없이 즉시) ──
        early_jobs = []
        if self.bot:
            early_jobs.append((
                "C2_flowx_universe",
                self.bot._job_flowx_universe_update(context)))
        # C4E: NXT 사전 수집 (C3 일봉수집과 독립 — 핵심!)
        early_jobs.append((
            "C4E_nxt_early_collect",
            self._job_nxt_early_collect(context)))

        if early_jobs:
            early_results = await self.run_parallel_async(
                early_jobs, timeout_per_job=300)
            results.extend(early_results)

        # ── 3) C3 ★ _job_collect_daily (CRITICAL!) ──
        c3_ok = False
        if self.bot:
            r = await self.run_job_safe_async(
                "C3_collect_daily",
                self.bot._job_collect_daily(context),
                timeout=5400,  # 90분
            )
            results.append(r)
            c3_ok = r.success

            if not c3_ok:
                logger.error("[COO] C3 일봉수집 실패 — FALLBACK 실행")
                c3_ok = await self._fallback_collect_daily(context)
        else:
            logger.warning("[COO] bot 미연결 — C3 스킵")

        # ── 4) C5 + C6 + C5Z 병렬 (C4는 C4E로 대체됨) ──
        parallel_jobs: List[Tuple[str, Coroutine]] = []
        if self.bot:
            parallel_jobs.append(
                ("C5_options_signal",
                 self.bot._job_collect_options_signal(context)))
            parallel_jobs.append(
                ("C6_dart_refresh",
                 self.bot._job_dart_refresh(context)))
        # C5Z: 수급 Z-score 사전 계산 (BRAIN이 캐시 즉시 사용)
        parallel_jobs.append((
            "C5Z_flow_zscore",
            self._job_flow_zscore_precalc(context)))
        # C5M: 매크로 기준선 수집 (1일 1회, 2-Layer 판단 활성화)
        parallel_jobs.append((
            "C5M_macro_baseline",
            self._job_macro_baseline(context)))
        # C3M: 시장별 11주체 수급 (KIS FHPTJ04040000) — BRAIN/T3 보조자료
        parallel_jobs.append((
            "C3M_market_flow",
            self._job_market_flow_collect(context)))
        # C5T: TV 스캐너 독립 갱신 (C7 검증 전 tv_scanner.json 날짜 보장)
        parallel_jobs.append((
            "C5T_tv_scanner",
            self._job_tv_scanner_refresh(context)))

        if parallel_jobs:
            par_results = await self.run_parallel_async(
                parallel_jobs, timeout_per_job=300)
            results.extend(par_results)

        # ── 5) C7 ★ _job_verify_data (CRITICAL!) ──
        c7_ok = False
        if self.bot:
            r = await self.run_job_safe_async(
                "C7_verify_data",
                self.bot._job_verify_data(context),
                timeout=600,
            )
            results.append(r)
            c7_ok = r.success

            if not c7_ok:
                logger.error("[COO] C7 데이터 검증 실패 — FALLBACK 실행")
                c7_ok = await self._fallback_verify_data(context)
        else:
            logger.warning("[COO] bot 미연결 — C7 스킵")

        # ── 5.5) C7L limit_up_engine — 다음날 15:05 프리클로즈 알림용 데이터 갱신 ──
        r = await self.run_job_safe_async(
            "C7L_limit_up_engine",
            self._job_limit_up_engine(context),
            timeout=600,
        )
        results.append(r)
        if not r.success:
            logger.warning("[COO] C7L 실패 — 다음날 15:05 프리클로즈 알림 영향 가능")

        # ── 6) g6_mode 최종 결정 ──
        if c3_ok and c7_ok:
            self._g6_mode = "NORMAL"
        elif not c3_ok:
            self._g6_mode = "STALE"
        else:
            self._g6_mode = "DEGRADED"

        logger.info(f"[COO] G6 모드: {self._g6_mode}")

        # ── 그룹 상태 업데이트 ──
        self.update_group("G6", results)

        logger.info("[COO] ═══ G6 DATA_PIPELINE 완료 ═══")
        return results

    # ─────────────────────────────────────────────
    # FALLBACK-C3: 일봉수집 실패 시 복구
    # ─────────────────────────────────────────────
    async def _fallback_collect_daily(self, context=None) -> bool:
        """C3 _job_collect_daily 실패 시 폴백.

        a. 5분 대기 → C3 재시도 (1회)
        b. 재시도도 실패 → 전일 Parquet/CSV 재사용
        c. g6_mode = "STALE"
        d. 텔레그램 경고
        """
        # (a) 5분 대기 후 재시도
        logger.info("[COO] FALLBACK-C3: 5분 대기 후 재시도...")
        await asyncio.sleep(300)

        recovered = False
        if self.bot:
            r = await self.run_job_safe_async(
                "C3_collect_daily_retry",
                self.bot._job_collect_daily(context),
                timeout=5400,
            )
            if r.success:
                logger.info("[COO] FALLBACK-C3: 재시도 성공!")
                recovered = True

        # (b) 재시도 실패 → 전일 데이터 재사용 (파일 이미 존재하므로 자동)
        if not recovered:
            logger.warning("[COO] FALLBACK-C3: 재시도 실패 — "
                           "전일 Parquet/CSV 기반 운용")
            self._g6_mode = "STALE"

        # (d) 텔레그램 경고 — 4/8 비활성화 (알림 축소)
        logger.info(f"[COO] C3 일봉수집 {'성공' if recovered else '실패'} (텔레그램 OFF)")

        return recovered

    # ─────────────────────────────────────────────
    # FALLBACK-C7: 데이터 검증 실패 시 복구
    # ─────────────────────────────────────────────
    async def _fallback_verify_data(self, context=None) -> bool:
        """C7 _job_verify_data 실패 시 폴백.

        a. 검증 재시도 (1회)
        b. 여전히 실패 → g6_mode = "DEGRADED"
        c. 텔레그램 경고: 선취매 비활성 알림
        """
        recovered = False

        # (a) 검증 재시도
        if self.bot:
            logger.info("[COO] FALLBACK-C7: 데이터 검증 재시도...")
            r = await self.run_job_safe_async(
                "C7_verify_data_retry",
                self.bot._job_verify_data(context),
                timeout=600,
            )
            if r.success:
                logger.info("[COO] FALLBACK-C7: 재검증 성공!")
                recovered = True

        # (b) 여전히 실패 → DEGRADED
        if not recovered:
            logger.error("[COO] FALLBACK-C7: 재검증 실패 — "
                         "DEGRADED 모드, 선취매 비활성")
            self._g6_mode = "DEGRADED"

        # (c) 텔레그램 경고 — 4/8 비활성화 (알림 축소)
        logger.info(f"[COO] C7 데이터검증 {'성공' if recovered else '실패→DEGRADED'} (텔레그램 OFF)")

        return recovered

    # ═════════════════════════════════════════════
    # G7 EVENING_BRAIN — 이브닝 브레인 + 선취매
    # ═════════════════════════════════════════════
    async def run_g7(self, context=None):
        """G7 EVENING_BRAIN 실행.

        3-Stage 실행:
        - Stage 1: C8+C10+C11 병렬 (신호기록 / 스윙선정 / 브레인배분)
        - Stage 2: C12→C13 순차 (일간학습 → ★이브닝분석 CRITICAL)
        - Stage 3: C14~C29 병렬 (클로징/선취매/MACD/TRIX/국적/헬스/스윙/수급/ETF/퀀트대시/CTO정확도/NXT Paper/Paper리포트/선매집/주목박스)

        g6_mode 분기:
        - NORMAL: 전체 실행
        - STALE: C15(선취매) 경고 포함 실행
        - DEGRADED: C15 스킵 + 경고
        """
        logger.info("[COO] ═══ G7 EVENING_BRAIN 시작 ═══")
        self.group_status["G7"] = GroupStatus.RUNNING
        results = []

        # ── G6 대기 (10분 max) ──
        g6_wait_start = datetime.now()
        while self.group_status.get("G6") not in (
            GroupStatus.DONE, GroupStatus.PARTIAL, GroupStatus.FAILED
        ):
            elapsed = (datetime.now() - g6_wait_start).total_seconds()
            if elapsed > 600:
                logger.warning("[COO] G7: G6 대기 타임아웃 (10분) — 강제 진행")
                break
            await asyncio.sleep(5)

        logger.info(f"[COO] G7 g6_mode={self._g6_mode}")

        # ── KIS 토큰 사전 갱신 (C12/C13 KIS API 호출 전 토큰 확보) ──
        try:
            if self.auto_trader and hasattr(self.auto_trader, 'trader'):
                await asyncio.to_thread(
                    self.auto_trader.trader.ensure_fresh_token
                )
                logger.info("[COO] G7: KIS 토큰 사전 갱신 완료")
            else:
                logger.warning("[COO] G7: auto_trader 미연결 — KIS 토큰 갱신 스킵")
        except Exception as e:
            logger.warning(f"[COO] G7: KIS 토큰 갱신 실패 (무시): {e}")

        # ── 매크로 기준선 갱신 (1일 1회, 캐시 히트 시 스킵) ──
        try:
            from data.macro_baseline import fetch_all_baselines
            await asyncio.to_thread(fetch_all_baselines)
            logger.info("[COO] G7: 매크로 기준선 갱신 완료")
        except Exception as e:
            logger.warning(f"[COO] G7: 기준선 갱신 실패 (무시): {e}")

        # ── 전쟁/휴전 시그널 갱신 (1일 1회) ──
        try:
            from data.war_signal import analyze_war_signal, format_war_signal_alert
            ws = await asyncio.to_thread(analyze_war_signal)
            logger.info(f"[COO] G7: 전쟁 시그널 — {ws.level_label} ({ws.signals_met}/4)")
            # CEASEFIRE_LIKELY 이상이면 텔레그램 알림 — 4/8 비활성화 (알림 축소)
            # alert_msg = format_war_signal_alert(ws)
            # if alert_msg and hasattr(self, '_bot') and self._bot:
            #     await self._bot.send_message(alert_msg)
        except Exception as e:
            logger.warning(f"[COO] G7: 전쟁 시그널 갱신 실패 (무시): {e}")

        # ── g6_mode 경고 ──
        if self._g6_mode == "STALE":
            logger.warning("[COO] G7: STALE 데이터 — 선취매 주의")
        elif self._g6_mode == "DEGRADED":
            logger.warning("[COO] G7: DEGRADED — 선취매 스킵")

        # ── Stage 1: C8 + C10 + C11 병렬 (600s, 재시도 1회) ──
        logger.info("[COO] G7 Stage 1: C8+C10+C11 병렬 (재시도 지원)")
        stage1_jobs = []

        if self.bot:
            stage1_jobs.append((
                "C8_record_signals",
                self.bot._job_record_signals(context),
                lambda: self.bot._job_record_signals(context),
            ))
            stage1_jobs.append((
                "C10_swing_picker",
                self.bot._job_swing_picker(context),
                lambda: self.bot._job_swing_picker(context),
            ))

        if self.auto_trader:
            stage1_jobs.append((
                "C11_brain_allocation",
                self.auto_trader.job_brain_allocation(context),
            ))

        if stage1_jobs:
            s1 = await self.run_parallel_async(stage1_jobs, timeout_per_job=600)
            results.extend(s1)

        # ── Stage 2: C12 → C13 순차 ──
        logger.info("[COO] G7 Stage 2: C12→C13 순차")

        # C12: 일간 학습
        if self.bot:
            r12 = await self.run_job_safe_async(
                "C12_daily_learning",
                self.bot._job_daily_learning(context),
                timeout=600,
            )
            results.append(r12)

        # C35: T3 수급 패턴 감지기 — 오늘 급등주 전수 패턴 분류
        # C12 missed_gainers 생성 직후 실행. 가벼운 잡 (<30초).
        r35 = await self.run_job_safe_async(
            "C35_pattern_scan",
            self._job_supply_pattern_scan(context),
            timeout=120,
        )
        results.append(r35)

        # C13: ★ 이브닝 분석 (CRITICAL)
        # 추천 파이프라인: Step1~6 합계 ~25분
        # Step5 Soft Scoring이 300+종목 처리 시 ~20분 소요 (TV잔존+거래대금이상 종목 추가)
        # 4/8: C13을 백그라운드로 분리 — Stage 3과 병렬 실행 (블로킹 방지)
        c13_ok = False
        c13_task = None
        if self.auto_trader:
            async def _run_c13_with_fallback():
                """C13 실행 + 실패 시 FALLBACK (백그라운드)."""
                self._c13_recommendation_running = True
                try:
                    r = await self.run_job_safe_async(
                        "C13_evening_analysis",
                        self.auto_trader.job_evening_analysis(context),
                        timeout=2700,
                    )
                    if r.success:
                        return r, True
                    # FALLBACK: 1분 대기 후 재시도 (기존 3분→1분 단축)
                    logger.error("[COO] ★ C13 이브닝분석 실패 — FALLBACK-C13 가동 (백그라운드)")
                    await asyncio.sleep(60)
                    r2 = await self.run_job_safe_async(
                        "C13_evening_analysis_retry",
                        self.auto_trader.job_evening_analysis(context),
                        timeout=1800,
                    )
                    if r2.success:
                        logger.info("[COO] FALLBACK-C13: 재시도 성공!")
                    else:
                        logger.warning("[COO] FALLBACK-C13: 재시도 실패 — 전일 recommendation.json 유지")
                        # 텔레그램 경고 — 4/8 비활성화 (알림 축소)
                        logger.warning("[COO] C13 이브닝분석 실패 (텔레그램 OFF)")
                    return r2, r2.success
                finally:
                    self._c13_recommendation_running = False
            c13_task = asyncio.create_task(_run_c13_with_fallback())

        # ── Stage 3: C14~C27 병렬 — C13과 동시 진행 ──
        logger.info("[COO] G7 Stage 3: C14~C29 병렬 (C13과 동시 진행)")
        stage3_jobs = []

        # C14: 클로징 브리프 — 4/8 비활성화 (알림 축소)
        # if self.bot:
        #     stage3_jobs.append((
        #         "C14_closing_brief",
        #         self.bot._send_daily_closing(context),
        #     ))
        logger.info("[COO] C14 클로징 브리프 (텔레그램 OFF)")

        # C15: 선취매 — DEGRADED면 스킵
        if self._g6_mode != "DEGRADED" and self.auto_trader:
            stage3_jobs.append((
                "C15_predawn_buy",
                self.auto_trader.job_predawn_buy(context),
            ))
        elif self._g6_mode == "DEGRADED":
            logger.warning("[COO] C15 선취매 스킵 (DEGRADED 모드)")
            results.append(JobResult(
                name="C15_predawn_buy",
                success=False,
                elapsed=0.0,
                error="SKIPPED: DEGRADED mode",
                timestamp=datetime.now().isoformat(),
            ))

        # C16: MACD 스캔
        if self.bot:
            stage3_jobs.append((
                "C16_macd_scan",
                self.bot._job_macd_scan(context),
            ))

        # C17: 국적 차트 (재시도 지원 — TOP200 KRX 크롤링으로 600초 초과 빈발)
        if self.bot:
            stage3_jobs.append((
                "C17_nationality_charts",
                self.bot._job_nationality_charts(context),
                lambda: self.bot._job_nationality_charts(context),
            ))

        # C18: 파이프라인 헬스
        if self.bot:
            stage3_jobs.append((
                "C18_pipeline_health",
                self.bot._job_pipeline_health(context),
            ))

        # C19: FLOWX 스윙 VIP 업로드 (non-critical)
        stage3_jobs.append((
            "C19_flowx_swing",
            self._job_flowx_swing_upload(context),
        ))

        # C20: 섹터 기관 수급 분석 (TIER2)
        if self.bot:
            stage3_jobs.append((
                "C20_sector_flow",
                self.bot._job_sector_flow(context),
            ))

        # C20b: 테마별 수급 + 모멘텀 (302개 KIS 테마)
        stage3_jobs.append((
            "C20b_theme_flow_momentum",
            self._job_theme_flow_momentum(context),
        ))

        # C21: ETF 투자자별 수급 분석 (TIER2 Phase 2)
        if self.bot:
            stage3_jobs.append((
                "C21_etf_flow",
                self.bot._job_etf_flow(context),
            ))

        # C22: → Stage 3 이후 순차 실행 (C20/C21 완료 대기 필요)

        # C23: TRIX 다이버전스 사전 스캔 (다음날 모닝추천 캐시)
        stage3_jobs.append((
            "C23_trix_prescan",
            self._job_trix_prescan(context),
        ))

        # C24: CTO 시그널 정확도 업데이트
        if self.cto:
            stage3_jobs.append((
                "C24_cto_accuracy",
                self._job_cto_accuracy_update(context),
            ))

        # C25: 국적 수급 X-ray Supabase 업로드 (독립 경로)
        stage3_jobs.append((
            "C25_nationality_xray",
            self._job_nationality_xray_upload(context),
        ))

        # C26: NXT Paper Trading 등록
        stage3_jobs.append((
            "C26_nxt_paper_register",
            self._job_nxt_paper_register(context),
        ))

        # C27: Paper Trading 일일 성적표
        stage3_jobs.append((
            "C27_paper_daily_report",
            self._job_paper_daily_report(context),
        ))

        # C28: 기관 선매집 탐지 스캔
        stage3_jobs.append((
            "C28_stealth_scan",
            self._job_stealth_scan(context),
        ))

        # C29: 주목 종목 박스 생성 + Supabase 업로드
        stage3_jobs.append((
            "C29_watchbox",
            self._job_watchbox(context),
        ))

        # C30: 단타 TOP픽 프리뷰 (16:45 · 국장 마감 기준)
        # 미국장 미개장 → EWY 시그널 0, NXT 야간매수용
        # 07:30 G1 A12에서 미국장 반영 확정 재발행
        stage3_jobs.append((
            "C30_daytrading_picks_preview",
            self._job_daytrading_picks(context, mode="preview"),
        ))

        # C31: 단타 TOP 5 일일 성적표 (시가→종가 수익률 + 주간/월간 누적)
        stage3_jobs.append((
            "C31_daytrading_performance",
            self._job_daytrading_performance(context),
        ))

        # C34: 수급 사이클 감지기 스캔 — 4세력 기반 위상 판정
        stage3_jobs.append((
            "C34_cycle_detector",
            self._job_cycle_scan(context),
        ))

        # C32/C33: Stage 3 이후 순차 실행 (nightwatch_report 갱신 대기 필요)
        # C26이 nightwatch_decide 완료를 보장한 뒤에 C32가 읽어야 정확한 score 반영

        if stage3_jobs:
            # C17 국적차트 TOP200 생성+업로드, C19 FLOWX 스윙 등 무거운 작업 포함
            s3 = await self.run_parallel_async(stage3_jobs, timeout_per_job=600)
            results.extend(s3)

        # ── C13 백그라운드 태스크 합류 (최대 60초 대기) ──
        if c13_task is not None:
            try:
                c13_result, c13_ok = await asyncio.wait_for(c13_task, timeout=60)
                results.append(c13_result)
                logger.info(f"[COO] C13 백그라운드 합류: {'성공' if c13_ok else '실패'}")
            except asyncio.TimeoutError:
                logger.warning("[COO] C13 백그라운드 아직 진행 중 — 대기 포기, Stage 3 이후 작업 계속")
            except Exception as e:
                logger.warning(f"[COO] C13 백그라운드 합류 에러: {e}")

        # ── Stage 4: C32 + C33 순차 (C26 nightwatch 대기 완료 후) ──
        logger.info("[COO] G7 Stage 4: C32+C33 (nightwatch 갱신 보장 후)")
        stage4_jobs = [
            ("C32_nxt_top5_publish", self._job_nxt_top5_publish(context)),
            ("C33_nxt_performance", self._job_nxt_performance(context)),
            ("C36_accumulation_radar", self._job_accumulation_radar(context)),
            ("C37_oneshot_stealth", self._job_oneshot_stealth(context)),
            ("C38_foreign_flow", self._job_foreign_flow_upload(context)),
            ("C39_massive_dual_alert", self._job_massive_dual_buy_alert(context)),
            ("C40_inst_accumulation", self._job_inst_accumulation(context)),
            ("C41_pension_scan", self._job_pension_scan(context)),
            ("C42_pension_ownership", self._job_pension_ownership(context)),
        ]
        s4 = await self.run_parallel_async(stage4_jobs, timeout_per_job=300)
        results.extend(s4)

        # ── C22: 퀀트 대시보드 업로드 — 재활성화 (2026-04-10) ──
        try:
            import time as _time
            _c22_t0 = _time.time()
            c22_result = await asyncio.wait_for(
                self._job_quant_dashboard_upload(context), timeout=300
            )
            results.append(JobResult("C22_quant_dashboard", True, _time.time() - _c22_t0))
        except Exception as e:
            logger.warning(f"[C22] 퀀트 대시보드 실패 (무시): {e}")
            results.append(JobResult("C22_quant_dashboard", False, 0.0, str(e)))

        # ── brain_report.json 갱신 safeguard ──
        # C13 실패 시에도 brain_report는 반드시 오늘 날짜로 갱신
        try:
            await self._ensure_brain_report_today()
        except Exception as e:
            logger.warning(f"[COO] brain_report safeguard 실패: {e}")

        # ── 그룹 상태 업데이트 ──
        self.update_group("G7", results)

        # ── 전체 파이프라인 일일 리포트 — 4/8 비활성화 (알림 축소) ──
        # try:
        #     await self._send_daily_pipeline_report(context)
        # except Exception as e:
        #     logger.warning(f"[COO] 일일 파이프라인 리포트 발송 실패: {e}")
        logger.info("[COO] 일일 파이프라인 리포트 (텔레그램 OFF)")

        # ── G7 후 데이터 재검증 (G6 시점 미생성 데이터 포함) ──
        try:
            if self.bot:
                r = await asyncio.wait_for(
                    self.bot._job_verify_data(context), timeout=120
                )
                logger.info("[COO] G7 후 데이터 재검증 완료")
        except Exception as e:
            logger.warning(f"[COO] G7 후 재검증 실패 (무시): {e}")

        # ── AUTO-RECOVERY: 핵심 파일 검증 + 실패 시 개별 복구 ──
        try:
            await self._post_g7_auto_recovery(context)
        except Exception as e:
            logger.error(f"[COO] AUTO-RECOVERY 실행 실패: {e}")

        logger.info("[COO] ═══ G7 EVENING_BRAIN 완료 ═══")
        return results

    # ─────────────────────────────────────────────
    # 일일 파이프라인 리포트 (G7 완료 시 자동 발송)
    # ─────────────────────────────────────────────
    async def _send_daily_pipeline_report(self, context):
        """G1~G7 전체 Stage 결과 요약 → 텔레그램 자동 발송."""
        ok = 0
        fail = 0
        timeout = 0
        fail_names = []

        for gname, jobs in self.run_log.items():
            for j in jobs:
                s = j.get("success", False)
                t = j.get("elapsed_sec", 0)
                nm = j.get("name", "?")
                if s:
                    ok += 1
                elif not s and isinstance(t, (int, float)) and t >= 290:
                    timeout += 1
                    fail_names.append(f"  {nm} (TIMEOUT {t:.0f}s)")
                else:
                    fail += 1
                    fail_names.append(f"  {nm} (FAIL)")

        total = ok + fail + timeout
        icon = "✅" if fail == 0 and timeout == 0 else "⚠️" if timeout > 0 else "❌"

        lines = [
            f"{icon} COO 일일 파이프라인 리포트",
            f"날짜: {self.today}",
            f"총 {total}개 Stage: {ok} OK / {fail} FAIL / {timeout} TIMEOUT",
        ]
        if fail_names:
            lines.append("")
            lines.append("문제 Stage:")
            lines.extend(fail_names)

        msg = "\n".join(lines)

        # 텔레그램 발송
        if self.bot and hasattr(self.bot, "chat_id") and self.bot.chat_id:
            try:
                await context.bot.send_message(
                    chat_id=self.bot.chat_id, text=msg)
            except Exception as e:
                logger.warning(f"[COO] 리포트 텔레그램 발송 실패: {e}")

        logger.info(f"[COO] 일일 리포트: {ok}OK/{fail}FAIL/{timeout}TIMEOUT")

    # ─────────────────────────────────────────────
    # C19: FLOWX VIP 스윙 업로드
    # ─────────────────────────────────────────────
    async def _job_flowx_swing_upload(self, context=None) -> dict:
        """FLOWX VIP 스윙 페이지 데이터 생성 + Supabase 업로드"""
        try:
            from data.upload_swing import run_flowx_swing_upload
            success = run_flowx_swing_upload()
            return {"flowx_swing": "OK" if success else "UPLOAD_FAIL"}
        except Exception as e:
            logger.warning(f"[C19] FLOWX 스윙 업로드 실패 (무시): {e}")
            return {"flowx_swing": f"ERROR: {e}"}

    # ─────────────────────────────────────────────
    # C22: FLOWX 퀀트 대시보드 통합 업로드
    # ─────────────────────────────────────────────
    async def _job_theme_flow_momentum(self, context=None) -> dict:
        """302개 KIS 테마별 수급 + 모멘텀 분석 → JSON 저장.

        C20b: theme_flow.json + theme_momentum.json 생성.
        C22 quant_dashboard_upload 에서 Supabase로 업로드.
        """
        results = {}
        try:
            from data.sector_institution_flow import analyze_theme_flow
            await asyncio.to_thread(analyze_theme_flow)
            results["theme_flow"] = "OK"
        except Exception as e:
            logger.warning(f"[C20b] theme_flow 실패: {e}")
            results["theme_flow"] = f"ERROR: {e}"
        try:
            from data.sector_momentum import analyze_theme_momentum
            await asyncio.to_thread(analyze_theme_momentum)
            results["theme_momentum"] = "OK"
        except Exception as e:
            logger.warning(f"[C20b] theme_momentum 실패: {e}")
            results["theme_momentum"] = f"ERROR: {e}"
        logger.info(f"[C20b] 테마 수급/모멘텀 완료: {results}")
        return results

    async def _job_quant_dashboard_upload(self, context=None) -> dict:
        """7개 퀀트 대시보드 테이블 → Supabase 업로드"""
        try:
            from data.upload_quant_dashboard import run_quant_dashboard_upload
            results = run_quant_dashboard_upload()
            ok_count = sum(1 for v in results.values() if v == "OK")
            return {"quant_dashboard": f"{ok_count}/5 OK", "detail": results}
        except Exception as e:
            logger.warning(f"[C22] 퀀트 대시보드 업로드 실패 (무시): {e}")
            return {"quant_dashboard": f"ERROR: {e}"}

    # ─────────────────────────────────────────────
    # C5Z: 수급 Z-score 사전 계산
    # ─────────────────────────────────────────────
    async def _job_flow_zscore_precalc(self, context=None) -> dict:
        """pykrx 투자자별 수급 데이터 → 20일 Z-score → 캐시 저장.

        G6 Step 4에서 C5+C6과 병렬 실행.
        G7 C11(BRAIN)이 캐시를 즉시 재사용하여 pykrx 재호출 불필요.
        """
        try:
            from data.flow_zscore import calc_market_zscore, save_flow_zscore

            result = await asyncio.to_thread(calc_market_zscore)
            if result.get("combined"):
                save_flow_zscore(result)
                signal = result.get("signal", "")
                brain_adj = result.get("brain_adj", 0.0)
                logger.info(
                    f"[C5Z] Z-score 사전 계산 완료: adj={brain_adj:+.1f}, "
                    f"signal={signal[:40]}")
                return {
                    "flow_zscore": "OK",
                    "brain_adj": brain_adj,
                    "signal": signal,
                }
            logger.warning("[C5Z] Z-score 계산 결과 없음 (빈 데이터)")
            return {"flow_zscore": "EMPTY"}
        except Exception as e:
            logger.warning(f"[C5Z] Z-score 사전 계산 실패 (무시): {e}")
            return {"flow_zscore": f"ERROR: {e}"}

    async def _job_tv_scanner_refresh(self, context=None) -> dict:
        """C5T: TV 스캐너 독립 갱신 — G6에서 C3 완료 후 실행.

        C13(이브닝분석)이 G7 백그라운드에서 TV 스캔을 하지만,
        C7 데이터 검증이 먼저 실행되어 날짜 불일치 FAIL 발생.
        G6 병렬 단계에서 독립 스캔하여 C7 검증 전 갱신 보장.
        """
        try:
            from data.trading_value_scanner import scan_trading_value, save_tv_results, enrich_signals_with_flow
            import json
            from pathlib import Path

            store = Path(__file__).resolve().parent.parent / "data_store"

            # 캐시 체크: 이미 오늘 스캔 완료면 스킵
            tv_path = store / "tv_scanner.json"
            if tv_path.exists():
                from datetime import datetime
                today_str = datetime.now().strftime("%Y-%m-%d")
                try:
                    cached = json.loads(tv_path.read_text("utf-8"))
                    if cached.get("scan_date") == today_str:
                        logger.info("[C5T] TV 스캐너 이미 오늘 갱신 완료 — 스킵")
                        return {"tv_scanner": "CACHED"}
                except Exception:
                    pass

            # 유니버스 로드
            uni_path = store / "universe.json"
            if not uni_path.exists():
                return {"tv_scanner": "NO_UNIVERSE"}
            universe = json.loads(uni_path.read_text("utf-8"))

            # TV 풀스캔 (타임아웃 180초)
            from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout
            with ThreadPoolExecutor(max_workers=1, thread_name_prefix="c5t_tv") as ex:
                future = ex.submit(scan_trading_value, universe, min_tv_billion=10.0)
                try:
                    signals = future.result(timeout=180)
                    enrich_signals_with_flow(signals)
                    save_tv_results(signals)
                    frgn_cnt = sum(1 for s in signals if s.frgn_joined)
                    logger.info(f"[C5T] TV 스캐너 갱신 완료: {len(signals)}개 시그널 (외인합류 {frgn_cnt})")
                    return {"tv_scanner": "OK", "signals": len(signals), "frgn_joined": frgn_cnt}
                except FuturesTimeout:
                    logger.warning("[C5T] TV 스캐너 타임아웃 (180s)")
                    return {"tv_scanner": "TIMEOUT"}
        except Exception as e:
            logger.warning(f"[C5T] TV 스캐너 갱신 실패 (무시): {e}")
            return {"tv_scanner": f"ERROR: {e}"}

    # ─────────────────────────────────────────────
    # C35: T3 수급 패턴 감지기 (4주체 기반, 11주체 확장 가능)
    # ─────────────────────────────────────────────
    async def _job_supply_pattern_scan(self, context=None) -> dict:
        """C35: 오늘 급등주 수급 패턴 전수 분류.

        G7 Stage 2 C12 직후 실행 (missed_gainers 생성 이후).
        쌍매수/외인단독/기관단독/기타법인매수·매도/개인주도/수급이탈 7종 분류.
        11주체 호환 스키마 사용 — KRX OpenAPI 승인 시 자동 확장.

        산출물: data_store/learning/pattern_scan/{YYYY-MM-DD}.json
        """
        try:
            from tools.supply_pattern_detector import (
                analyze_missed_gainers,
                save_results,
                PATTERN_DESC,
                PATTERN_SCORE,
            )
            from datetime import datetime as _dt, timezone as _tz, timedelta as _td

            # M-2 수정: VPS UTC 대응 — KST 기준 날짜 사용
            _KST = _tz(_td(hours=9))
            date_str = _dt.now(_KST).strftime("%Y-%m-%d")

            # H-2 수정: flow_collector(C3) 결과 확인 — STALE/실패 시 패턴 분류 신뢰 불가
            try:
                flow_dir = Path(__file__).resolve().parent.parent / "data_store" / "flow"
                # 오늘 수집된 _investor.csv 파일 존재 여부 체크 (10개 이상이면 정상)
                today_ts = _dt.now(_KST).replace(tzinfo=None).timestamp()
                recent = [f for f in flow_dir.glob("*_investor.csv")
                          if (today_ts - f.stat().st_mtime) < 7200]  # 2시간 이내
                if len(recent) < 10:
                    logger.warning(
                        f"[C35] flow_collector 최근 수집 {len(recent)}건 < 10 — "
                        f"STALE 가능성. 패턴 스캔 스킵"
                    )
                    return {"pattern_scan": "SKIP", "reason": "flow STALE", "recent": len(recent)}
            except Exception as _e:
                logger.warning(f"[C35] flow STALE 체크 실패(무시): {_e}")

            # 동기 함수를 스레드로 실행 (CSV 읽기 다수)
            results = await asyncio.to_thread(analyze_missed_gainers, date_str)

            if not results:
                logger.info("[C35] 패턴 스캔: 대상 없음 (missed_gainers 비어있음)")
                return {"pattern_scan": "EMPTY", "count": 0}

            # 패턴별 집계
            counts: dict = {}
            for r in results:
                counts[r.pattern] = counts.get(r.pattern, 0) + 1

            # 파일 저장
            out_path = await asyncio.to_thread(save_results, results, date_str)

            # 주요 패턴 요약 로그
            strong_patterns = ["DUAL_SURGE", "FOREIGN_SOLO", "INST_SOLO"]
            strong_cnt = sum(counts.get(p, 0) for p in strong_patterns)
            warn_patterns = ["OTHER_CORP_DUMP", "OUTFLOW"]
            warn_cnt = sum(counts.get(p, 0) for p in warn_patterns)

            logger.info(
                f"[C35] 수급 패턴 스캔 완료 — {len(results)}종목, "
                f"강신호 {strong_cnt}개, 위험 {warn_cnt}개 → {out_path.name}"
            )

            # ── v4.2: bomb_watchlist 전체 저장 (morning_rec 보너스 맵용) ──
            # v4: top_n=10 → v4.2: top_n=9999 (전체 저장, morning_rec에서 교집합만 사용)
            bomb_count = 0
            try:
                from tools.supply_pattern_detector import generate_bomb_watchlist
                bomb_list = await asyncio.to_thread(
                    generate_bomb_watchlist, date_str, 9999
                )
                bomb_count = len(bomb_list)
                if bomb_list:
                    bomb_names = ", ".join(b["name"] for b in bomb_list[:3])
                    logger.info(f"[C35] bomb_watchlist: {bomb_count}종목 — {bomb_names}")
            except Exception as _be:
                logger.warning(f"[C35] bomb_watchlist 생성 실패(무시): {_be}")

            # ── v5.0: 수급 강도 TOP 생성 + Supabase 업로드 ──
            intensity_count = 0
            try:
                from tools.flow_intelligence import generate_flow_intensity_data
                from data.upload_flow_intensity import upload_flow_intensity
                intensity_data = await asyncio.to_thread(
                    generate_flow_intensity_data, 7, 2000
                )
                _top_stocks = (intensity_data or {}).get("top_stocks") or []
                if _top_stocks:
                    intensity_count = len(_top_stocks)
                    await asyncio.to_thread(upload_flow_intensity, intensity_data)
                    # 로컬 저장 → morning_recommendation 후보 소스로 사용
                    _fi_path = DATA_STORE / "flow_intensity.json"
                    _tmp_fi = _fi_path.with_suffix(".tmp")
                    _tmp_fi.write_text(
                        json.dumps(intensity_data, ensure_ascii=False, indent=2),
                        encoding="utf-8",
                    )
                    _tmp_fi.replace(_fi_path)  # 원자적 교체
                    top1 = _top_stocks[0]
                    logger.info(
                        f"[C35] 수급강도 TOP{intensity_count} → {_fi_path.name} 저장: "
                        f"1위 {top1['name']}({top1['intensity_pct']}%)"
                    )
            except Exception as _ie:
                logger.warning(f"[C35] 수급강도 생성/업로드 실패(무시): {_ie}")

            return {
                "pattern_scan": "OK",
                "count": len(results),
                "strong": strong_cnt,
                "warn": warn_cnt,
                "counts": counts,
                "path": str(out_path),
                "bomb_watchlist": bomb_count,
                "flow_intensity": intensity_count,
            }
        except FileNotFoundError as e:
            logger.warning(f"[C35] missed_gainers 파일 없음: {e}")
            return {"pattern_scan": "SKIP", "reason": "missed_gainers missing"}
        except Exception as e:
            logger.warning(f"[C35] 수급 패턴 스캔 실패 (무시): {e}")
            return {"pattern_scan": f"ERROR: {e}"}

    async def _job_market_flow_collect(self, context=None) -> dict:
        """C3M: 시장별 11주체 수급 수집 (KOSPI/KOSDAQ) — KIS FHPTJ04040000.

        G6 장마감 후 1회 실행. 외인/개인/기관종합 + 기관 7주체(금융투자/투신/
        사모/은행/보험/기타금융/연기금) + 기타법인 = 11주체.
        data_store/flow/market_{kospi|kosdaq}.csv 저장 (60일 롤링).
        """
        try:
            from data.market_flow_collector import collect_market_flow
            out = await asyncio.to_thread(collect_market_flow, 1)
            if not out:
                return {"market_flow": "EMPTY"}
            info = {k: len(v) for k, v in out.items() if v is not None}
            logger.info(f"[C3M] 시장 수급 수집 완료 — {info}")
            return {"market_flow": "OK", "markets": info}
        except Exception as e:
            logger.warning(f"[C3M] 시장 수급 수집 실패 (무시): {e}")
            return {"market_flow": f"ERROR: {e}"}

    async def _job_macro_baseline(self, context=None) -> dict:
        """C5M: 매크로 기준선 수집 (yfinance 6지표 20MA/60MA).

        G6 Step 4에서 병렬 실행.
        macro_baseline.json에 캐시 → inflation_chain/fx_sector/bond_yield의
        2-Layer 판단 활성화.
        """
        try:
            from data.macro_baseline import fetch_all_baselines

            baselines = await asyncio.to_thread(fetch_all_baselines)
            if baselines and baselines.timestamp:
                logger.info(f"[C5M] 매크로 기준선 수집 완료 ({baselines.timestamp})")
                return {"macro_baseline": "OK", "timestamp": baselines.timestamp}
            return {"macro_baseline": "EMPTY"}
        except Exception as e:
            logger.warning(f"[C5M] 매크로 기준선 수집 실패 (무시): {e}")
            return {"macro_baseline": f"ERROR: {e}"}

    # ─────────────────────────────────────────────
    # C7L: limit_up_engine 일일 스캔 (매일 G6에서 자동 실행)
    # ─────────────────────────────────────────────
    async def _job_limit_up_engine(self, context=None) -> dict:
        """C7L: 상한가 눌림목 엔진 일일 스캔 — 다음날 15:05 프리클로즈 알림용 데이터 갱신.

        Why: 5/14에 발견 — limit_up_engine 자동 실행 메커니즘 부재. 매일 15:05
        프리클로즈 알림이 limit_up/ 디렉토리의 surge_universe/watchlist/history
        파일을 읽어 종목 추천하는데, 그 데이터를 만드는 스크립트가 cron/COO 어디에도
        등록 안 되어있어서 5/12~5/14 동안 디렉토리 자체가 빈 상태였음.

        G6 데이터 수집 직후 1회 실행하여 다음날 15:05 알림이 정상 동작하도록 보장.
        """
        try:
            from data.limit_up_engine import run_daily

            result = await asyncio.to_thread(run_daily, True)
            new_signals = result.get("new_signals", 0) if isinstance(result, dict) else 0
            triggered = result.get("triggered", 0) if isinstance(result, dict) else 0
            logger.info(
                f"[C7L] limit_up_engine 완료 — "
                f"신규 시그널 {new_signals}건, 트리거 {triggered}건"
            )
            return {"limit_up_engine": "OK", "new_signals": new_signals,
                    "triggered": triggered}
        except Exception as e:
            logger.warning(f"[C7L] limit_up_engine 실패 (다음날 15:05 알림 영향 가능): {e}")
            return {"limit_up_engine": f"ERROR: {e}"}

    # ─────────────────────────────────────────────
    # C23: TRIX 다이버전스 사전 스캔
    # ─────────────────────────────────────────────
    async def _job_trix_prescan(self, context=None) -> dict:
        """전체 유니버스 TRIX 다이버전스 스캔 → 캐시 저장.

        G7 Stage 3에서 C16(MACD)과 병렬 실행.
        다음날 모닝추천 Step 2.6에서 캐시(3시간 TTL) 즉시 재사용.
        """
        try:
            from strategies.trix_divergence import (
                scan_trix_divergence, save_trix_cache,
            )

            signals = await asyncio.to_thread(scan_trix_divergence, None, 10)
            if signals:
                # 캐시 형식: {code: {info}}
                cache_data = {}
                for s in signals:
                    cache_data[s["code"]] = {
                        "name": s.get("name", s["code"]),
                        "source": "trix_divergence",
                        "div_strength": s.get("div_strength", 0),
                        "adx": s.get("adx", 0),
                        "trix_cross": s.get("trix_cross", "neutral"),
                        "composite_score": s.get("composite_score", 0),
                    }
                save_trix_cache(cache_data)
                logger.info(
                    f"[C23] TRIX 사전 스캔 완료: {len(signals)}종목 감지")
                return {
                    "trix_prescan": "OK",
                    "count": len(signals),
                    "top": [s.get("name", s["code"]) for s in signals[:3]],
                }
            logger.info("[C23] TRIX 다이버전스 감지 종목 없음")
            return {"trix_prescan": "EMPTY"}
        except Exception as e:
            logger.warning(f"[C23] TRIX 사전 스캔 실패 (무시): {e}")
            return {"trix_prescan": f"ERROR: {e}"}

    # ─────────────────────────────────────────────
    # C24: CTO 시그널 정확도 업데이트
    # ─────────────────────────────────────────────
    async def _job_cto_accuracy_update(self, context=None) -> dict:
        """CTO 시그널 소스별 정확도 계산 (recommendation_history 기반)."""
        try:
            result = await asyncio.to_thread(self.cto.calc_signal_accuracy)
            top_sources = sorted(
                result.items(), key=lambda x: -x[1].get("accuracy_pct", 0)
            )[:3]
            summary = ", ".join(
                f"{s[0]}={s[1]['accuracy_pct']:.0f}%" for s in top_sources
            )
            logger.info(f"[C24] CTO 정확도 업데이트: {len(result)}소스 ({summary})")
            return {"cto_accuracy": "OK", "sources": len(result)}
        except Exception as e:
            logger.warning(f"[C24] CTO 정확도 업데이트 실패 (무시): {e}")
            return {"cto_accuracy": f"ERROR: {e}"}

    async def _job_nationality_xray_upload(self, context=None) -> dict:
        """C25: 국적 수급 X-ray Supabase 독립 업로드.

        morning_recommendation 경로와 무관하게 G7에서 항상 실행.
        nationality CSV 데이터가 있는 전 종목의 외국인 국적별 수급을 업로드.
        실패 시 AUTO-RECOVERY에서 재시도.
        """
        try:
            from data.upload_short import upload_nationality_flows
            ok = await asyncio.to_thread(upload_nationality_flows)
            if ok:
                logger.info("[C25] 국적 X-ray 업로드 완료")
                return {"nationality_xray": "OK"}
            else:
                logger.warning("[C25] 국적 X-ray 업로드 실패 (반환 False)")
                return {"nationality_xray": "FAILED"}
        except Exception as e:
            logger.warning(f"[C25] 국적 X-ray 업로드 실패 (무시): {e}")
            return {"nationality_xray": f"ERROR: {e}"}

    # ─────────────────────────────────────────────
    # A5P: NXT Paper 아침 청산 (G1)
    # ─────────────────────────────────────────────
    async def _job_nxt_paper_morning_close(self, context=None) -> dict:
        """NXT + 단타 TOP픽 paper 포지션 → 익일 시가 기준 청산."""
        try:
            from data.paper_portfolio import PaperPortfolio
            portfolio = PaperPortfolio()
            closed = []
            today = datetime.now().strftime("%Y-%m-%d")

            # NXT + daytrading_pick 모두 time_stop_days=1 → 익일 아침 청산
            _day_trade_sources = ("nxt", "daytrading_pick")

            for code in list(portfolio.positions.keys()):
                pos = portfolio.positions[code]
                if pos.get("source") not in _day_trade_sources:
                    continue
                # 전일 등록 → 익일 아침 청산
                if pos.get("entry_date", "") >= today:
                    continue  # 오늘 등록된 건 스킵

                # 시가 조회 (08:00 장전 → stck_oprc=0 → current_price 폴백)
                open_price = pos["entry_price"]
                if self.auto_trader and hasattr(self.auto_trader, 'trader'):
                    try:
                        p = await asyncio.to_thread(
                            self.auto_trader.trader.fetch_price, code
                        )
                        if p.get("success"):
                            if p.get("open", 0) > 0:
                                open_price = p["open"]
                            elif p.get("current_price", 0) > 0:
                                open_price = p["current_price"]
                    except Exception:
                        pass

                sell_reason = "NXT_MORNING_SELL" if pos.get("source") == "nxt" else "TOP_MORNING_SELL"
                result = portfolio.close_position(code, open_price, sell_reason)
                if result:
                    closed.append(f"{pos['name']} {result['pnl_pct']:+.1f}%")

            if closed:
                logger.info(f"[A5P] NXT Paper 아침 청산: {', '.join(closed)}")
            return {"nxt_paper_morning_close": len(closed)}
        except Exception as e:
            logger.warning(f"[A5P] NXT Paper 아침 청산 실패 (무시): {e}")
            return {"nxt_paper_morning_close": f"ERROR: {e}"}

    # ─────────────────────────────────────────────
    # B5R: NXT Paper 시가 재정산 (G3 09:00+)
    # ─────────────────────────────────────────────
    async def _job_nxt_paper_reprice(self, context=None) -> dict:
        """A5P(08:00) 폴백 가격 → 장 개시 후 실제 시가로 재정산."""
        try:
            from data.paper_portfolio import PaperPortfolio
            portfolio = PaperPortfolio()

            kis = None
            if self.auto_trader and hasattr(self.auto_trader, 'trader'):
                kis = self.auto_trader.trader
            elif self.bot and hasattr(self.bot, 'trader'):
                kis = self.bot.trader

            if not kis:
                return {"nxt_paper_reprice": "KIS 미연결"}

            updated = await asyncio.to_thread(
                portfolio.reprice_nxt_morning_sells, kis
            )
            if updated:
                logger.info(
                    f"[B5R] NXT Paper 시가 재정산 {len(updated)}건: "
                    + ", ".join(updated)
                )
            return {"nxt_paper_reprice": len(updated)}
        except Exception as e:
            logger.warning(f"[B5R] NXT Paper 시가 재정산 실패: {e}")
            return {"nxt_paper_reprice": f"ERROR: {e}"}

    # ─────────────────────────────────────────────
    # C26: NXT Paper Trading 등록 (G7 Stage 3)
    # ─────────────────────────────────────────────
    async def _job_nxt_paper_register(self, context=None) -> dict:
        """nightwatch_report.json에서 NXT 추천 → PaperPortfolio 등록."""
        try:
            from data.paper_portfolio import PaperPortfolio
            report_path = DATA_STORE / "nightwatch_report.json"

            today = datetime.now().strftime("%Y-%m-%d")

            # nightwatch_decide(16:35)와 경쟁 상태 방어: 최대 5회 x 30초 = 150초
            report = None
            max_attempts = 5
            for attempt in range(max_attempts):
                if not report_path.exists():
                    if attempt < max_attempts - 1:
                        logger.info(f"[C26] nightwatch_report.json 없음 — 30초 후 재시도 ({attempt+1}/{max_attempts})")
                        await asyncio.sleep(30)
                        continue
                    logger.info("[C26] nightwatch_report.json 없음 — 스킵")
                    return {"nxt_paper_register": "NO_REPORT"}

                with open(report_path, "r", encoding="utf-8") as f:
                    report = json.load(f)

                # date 필드 우선, 없으면 timestamp에서 추출
                report_date = report.get("date", "")
                if not report_date:
                    ts = report.get("timestamp", "")
                    report_date = ts[:10] if len(ts) >= 10 else ""

                if report_date == today:
                    break  # 오늘 리포트 확인 완료

                if attempt < max_attempts - 1:
                    logger.info(f"[C26] 리포트 날짜 불일치 ({report_date} ≠ {today}) "
                                f"— nightwatch 완료 대기 60초 ({attempt+1}/{max_attempts})")
                    await asyncio.sleep(60)
                else:
                    logger.info(f"[C26] 리포트 날짜 불일치 ({report_date} ≠ {today}) "
                                f"— {max_attempts}회 재시도 후에도 stale, 스킵")
                    return {"nxt_paper_register": "STALE_REPORT"}

            if not report:
                return {"nxt_paper_register": "NO_REPORT"}

            signal = report.get("signal", report.get("verdict", ""))
            signal_text = report.get("signal_text", "")
            total_score = report.get("total_score", 0)
            logger.info(f"[C26] NXT signal={signal} ({signal_text}), score={total_score}")

            targets = report.get("nxt_targets", [])
            if not isinstance(targets, list):
                logger.warning(f"[C26] nxt_targets 타입 이상: {type(targets)} — 스킵")
                return {"nxt_paper_register": "INVALID_TARGETS"}
            if not targets:
                logger.info("[C26] NXT 추천 종목 없음")
                return {"nxt_paper_register": "NO_TARGETS"}

            # ── BRAIN allocation 준수 ──
            brain_regime = "표준"
            brain_pct = 70
            max_nxt_positions = 5  # 기본값
            try:
                alloc_path = DATA_STORE / "brain_allocation.json"
                if alloc_path.exists():
                    alloc = json.loads(alloc_path.read_text(encoding="utf-8"))
                    brain_regime = alloc.get("effective_regime", "표준")
                    brain_pct = alloc.get("position_size_pct", 70)
            except Exception:
                pass

            # BRAIN 레짐별 NXT Paper 최대 포지션 수
            if brain_regime == "관망" or brain_pct == 0:
                logger.info("[C26] BRAIN 관망 → NXT Paper 매수 금지")
                return {"nxt_paper_register": "BRAIN_HOLD", "regime": brain_regime}
            elif brain_regime == "최소" or brain_pct <= 30:
                max_nxt_positions = 2
            elif brain_regime == "방어" or brain_pct <= 50:
                max_nxt_positions = 3
            else:
                max_nxt_positions = 5

            # NXT 점수(total_score)가 진입 금지(🔴) 수준이면 축소
            if total_score is not None and total_score <= -3:
                orig = max_nxt_positions
                max_nxt_positions = min(max_nxt_positions, 2)
                logger.info(f"[C26] NXT 진입금지 ({total_score:+.1f}) → 최대 {orig}→{max_nxt_positions}종목")

            logger.info(f"[C26] BRAIN {brain_regime}({brain_pct}%) → NXT 최대 {max_nxt_positions}종목")

            portfolio = PaperPortfolio()
            registered = []

            # 기존 NXT 포지션 수 확인
            existing_nxt = sum(
                1 for pos in portfolio.positions.values()
                if pos.get("source", "").startswith("nxt")
            )
            remaining_slots = max(0, max_nxt_positions - existing_nxt)
            if remaining_slots <= 0:
                logger.info(f"[C26] NXT 슬롯 소진 (기존 {existing_nxt}/{max_nxt_positions}) — 스킵")
                return {"nxt_paper_register": 0, "reason": "SLOT_FULL",
                        "existing": existing_nxt, "max": max_nxt_positions}

            # supply_score 상위순 정렬 후 슬롯 수만큼만 등록
            targets_sorted = sorted(targets, key=lambda x: -x.get("supply_score", 0))

            for t in targets_sorted:
                if len(registered) >= remaining_slots:
                    break

                code = t.get("code", "")
                name = t.get("name", code)
                if not code or code in portfolio.positions:
                    continue

                # 현재가 (장마감 가격) 조회
                entry = 0
                if self.auto_trader and hasattr(self.auto_trader, 'trader'):
                    try:
                        p = await asyncio.to_thread(
                            self.auto_trader.trader.fetch_price, code
                        )
                        if p.get("success") and p.get("current_price", 0) > 0:
                            entry = p["current_price"]
                    except Exception:
                        pass

                if entry <= 0:
                    continue

                # 포지션 크기: BRAIN allocation 반영
                alloc_ratio = brain_pct / 100.0
                shares = max(1, int(portfolio.cash * 0.3 * alloc_ratio / entry))

                # TP/SL: NXT 기본값 (+3% / -2.5%)
                tp = int(entry * 1.03)
                sl = int(entry * 0.975)

                ok = portfolio.open_position(
                    code, name, entry, shares, "nxt", tp, sl, time_stop_days=1,
                )
                if ok:
                    registered.append(name)

            if registered:
                logger.info(f"[C26] NXT Paper 등록: {', '.join(registered)} "
                            f"({brain_regime} {brain_pct}%, {len(registered)}/{max_nxt_positions})")
            return {"nxt_paper_register": len(registered), "names": registered,
                    "brain_regime": brain_regime, "max_slots": max_nxt_positions}
        except Exception as e:
            logger.warning(f"[C26] NXT Paper 등록 실패 (무시): {e}")
            return {"nxt_paper_register": f"ERROR: {e}"}

    # ─────────────────────────────────────────────
    # C27: Paper Trading 일일 성적표 (G7 Stage 3)
    # ─────────────────────────────────────────────
    async def _job_paper_daily_report(self, context=None) -> dict:
        """일일 Paper Trading 성적표 → 텔레그램 발송."""
        try:
            from data.paper_portfolio import PaperPortfolio
            portfolio = PaperPortfolio()

            # MTM 갱신
            if self.auto_trader and hasattr(self.auto_trader, 'trader'):
                await asyncio.to_thread(
                    portfolio.mark_to_market, self.auto_trader.trader
                )

            # 일일 스냅샷 저장
            snapshot = portfolio.record_daily_snapshot()

            # 일일 성적표 텔레그램 발송 — 4/8 비활성화 (알림 축소, 웹에서 확인)
            report = portfolio.get_daily_report()
            logger.info(f"[C27] Paper Trading 일일 성적표 생성 (텔레그램 OFF)")

            # 7일차 이상이면 주간 종합 리포트 추가 발송
            day_count = portfolio.get_day_count()
            if day_count >= 7 and day_count % 7 == 0:
                two_week = portfolio.format_two_week_report()
                alert_fn = getattr(self.auto_trader, "_send_alert", None) if self.auto_trader else None
                if alert_fn:
                    await asyncio.to_thread(alert_fn, two_week)
                    logger.info(f"[C27] Paper Trading Week{day_count//7} 종합 리포트 발송")

            return {
                "paper_daily_report": "OK",
                "day": day_count,
                "total_value": snapshot.get("total_value", 0),
            }
        except Exception as e:
            logger.warning(f"[C27] Paper Trading 리포트 실패 (무시): {e}")
            return {"paper_daily_report": f"ERROR: {e}"}

    async def _job_stealth_scan(self, context=None) -> dict:
        """C28: 기관 선매집 탐지 스캔 → stealth_scan.json + Supabase 업로드."""
        try:
            from data.stealth_scanner import scan_stealth_accumulation

            result = await asyncio.to_thread(scan_stealth_accumulation)
            summary = result.get("summary", {})
            stealth_count = summary.get("stealth_count", 0)

            # Supabase 업로드 (FLOWX 대시보드용)
            upload_ok = False
            try:
                from data.upload_stealth_scan import upload_stealth_scan
                upload_ok = await asyncio.to_thread(upload_stealth_scan, result)
            except Exception as ue:
                logger.warning(f"[C28] Supabase 업로드 실패 (무시): {ue}")

            logger.info(
                f"[C28] 선매집 스캔 완료: "
                f"잠복 {stealth_count} / 움직임 {summary.get('moving_count', 0)} / "
                f"이미상승 {summary.get('surged_count', 0)} / "
                f"Supabase {'OK' if upload_ok else 'SKIP'}"
            )
            return {"stealth_scan": "OK", "stealth_count": stealth_count, "supabase": upload_ok}
        except Exception as e:
            logger.warning(f"[C28] 선매집 스캔 실패 (무시): {e}")
            return {"stealth_scan": f"ERROR: {e}"}

    async def _job_cycle_scan(self, context=None) -> dict:
        """C34: 수급 사이클 감지기 스캔 → cycle_scan.json + Supabase 업로드 + 텔레그램 알림."""
        try:
            from analysis.cycle_detector import run_cycle_scan, format_cycle_telegram

            results = await asyncio.to_thread(run_cycle_scan, 3000, 20)

            surge_cnt = sum(1 for r in results if r.phase == "SURGE")
            acc_cnt = sum(1 for r in results if r.phase == "ACCUMULATION")
            warn_cnt = sum(1 for r in results
                          if r.phase in ("PEAK_WARN", "DISTRIBUTION"))

            # Supabase 업로드
            try:
                from data.upload_cycle_scan import upload_cycle_scan
                await asyncio.to_thread(upload_cycle_scan, results)
            except Exception as ue:
                logger.warning(f"[C34] 사이클 Supabase 업로드 실패 (무시): {ue}")

            # 급등임박 3종목+ 시 텔레그램 알림
            if surge_cnt >= 3:
                msg = format_cycle_telegram(results, title="수급 사이클 자동 스캔")
                alert_fn = getattr(self.auto_trader, "_send_alert", None) if self.auto_trader else None
                if alert_fn:
                    try:
                        await asyncio.to_thread(alert_fn, msg)
                    except Exception:
                        pass

            logger.info(
                f"[C34] 수급 사이클 스캔 완료: "
                f"급등임박 {surge_cnt} / 매집 {acc_cnt} / 경고 {warn_cnt}"
            )
            return {"cycle_scan": "OK", "surge": surge_cnt,
                    "accumulation": acc_cnt, "warnings": warn_cnt}
        except Exception as e:
            logger.warning(f"[C34] 수급 사이클 스캔 실패 (무시): {e}")
            return {"cycle_scan": f"ERROR: {e}"}

    async def _job_morning_backfill(self, context=None) -> dict:
        """A0: G1 시작 시 전일 데이터 누락 자동 백필.

        16:25 verify는 flow 수집 중이라 통과하지만, 이후 수집 오류로 C34 등
        HIGH 우선순위 잡이 실패하는 경우가 많음.
        아침 06:30에 전일 기준 verify_all() → FAIL/PARTIAL이면 RETRY_MAP으로 재실행.

        Step 1: sector_history 과거 구멍 자동 백필 (rotation_detector.backfill_missing_dates)
        Step 2: verify_all() 실행
        Step 3: FAIL/PARTIAL 항목 중 RETRY_MAP에 매핑된 잡 재실행
        Step 4: 재검증 → 잔여 실패시 텔레그램 경고
        """
        from datetime import date as _date, timedelta as _td
        try:
            # ── Step 1: sector_history 과거 구멍 자동 백필 ──
            sector_fill_result = {"filled": [], "skipped": [], "total": 0}
            try:
                from data.rotation_detector import backfill_missing_dates
                sector_fill_result = await asyncio.to_thread(
                    backfill_missing_dates, 10
                )
                if sector_fill_result.get("filled"):
                    logger.info(
                        f"[A0] sector_history 구멍 복구: "
                        f"{sector_fill_result['filled']}"
                    )
                if sector_fill_result.get("skipped"):
                    logger.warning(
                        f"[A0] sector_history 백필 실패: "
                        f"{sector_fill_result['skipped']}"
                    )
            except Exception as e:
                logger.warning(f"[A0] sector_history 백필 예외: {e}")

            # ── Step 2: verify_all ──
            from data.data_verifier import DataVerifier, RETRY_MAP

            today = _date.today().isoformat()
            verifier = DataVerifier(today=today)
            result = await asyncio.to_thread(verifier.verify_all)

            status = result.get("status", "UNKNOWN")
            passed = result.get("passed", 0)
            total = result.get("total", 0)
            logger.info(f"[A0] 전일 데이터 검증: {status} {passed}/{total}")

            # ── Step 3: FAIL/PARTIAL 항목 재실행 ──
            # PARTIAL도 포함 (consensus lazy-cache는 제외)
            retried = set()
            skipped_failures = []
            _retry_statuses = ("FAIL", "PARTIAL")
            for key, detail in result.get("details", {}).items():
                if detail.get("status") not in _retry_statuses:
                    continue
                # consensus는 lazy-cache라 재시도 안함
                if key == "consensus":
                    continue
                job_name = RETRY_MAP.get(key)
                if not job_name:
                    skipped_failures.append(key)
                    continue
                if job_name in retried:
                    continue
                retried.add(job_name)
                logger.info(f"[A0] 백필 시도: {key}({detail.get('status')}) → {job_name}")
                try:
                    method = getattr(self, job_name, None)
                    if method is None and self.bot:
                        method = getattr(self.bot, job_name, None)
                    if method is None and self.auto_trader:
                        method = getattr(self.auto_trader, job_name, None)
                    if method:
                        await asyncio.wait_for(method(context), timeout=180)
                    else:
                        logger.warning(f"[A0] 백필 메서드 미발견: {job_name}")
                except Exception as e:
                    logger.warning(f"[A0] 백필 실패 {job_name}: {e}")

            # ── Step 4: 재검증 + 경고 ──
            if retried or sector_fill_result.get("filled"):
                result2 = await asyncio.to_thread(DataVerifier(today=today).verify_all)
                still_fail = [k for k, d in result2.get("details", {}).items()
                              if d.get("status") == "FAIL" and k in RETRY_MAP]
                logger.info(
                    f"[A0] 백필 완료: sector구멍 {len(sector_fill_result.get('filled', []))}일 "
                    f"/ 잡 {len(retried)}개 재실행 / 잔여 FAIL: {still_fail}"
                )

                if still_fail:
                    alert_fn = getattr(self.auto_trader, "_send_alert", None) \
                               if self.auto_trader else None
                    if alert_fn:
                        try:
                            msg = (
                                "⚠️ 아침 데이터 백필 경고\n"
                                f"   재시도 후에도 FAIL: {', '.join(still_fail)}\n"
                                f"   수동 확인 필요"
                            )
                            await asyncio.to_thread(alert_fn, msg)
                        except Exception:
                            pass

                return {"morning_backfill": "OK",
                        "sector_filled": sector_fill_result.get("filled", []),
                        "retried": len(retried),
                        "remaining_fail": still_fail}

            return {"morning_backfill": "OK",
                    "sector_filled": [],
                    "retried": 0,
                    "skipped": skipped_failures}
        except Exception as e:
            logger.warning(f"[A0] 아침 백필 실패 (무시): {e}")
            return {"morning_backfill": f"ERROR: {e}"}

    async def _job_watchbox(self, context=None) -> dict:
        """C29: 주목 종목 박스 생성 → watchbox.json + Supabase 업로드 + 텔레그램 알림."""
        try:
            from data.watchbox import build_watchbox, format_watchbox_telegram, upload_watchbox_supabase

            result = await asyncio.to_thread(build_watchbox)
            total = result.get("total_count", 0)

            # Supabase 업로드 (실패해도 계속)
            try:
                await asyncio.to_thread(upload_watchbox_supabase, result)
            except Exception as e:
                logger.warning(f"[C29] Supabase 업로드 실패 (무시): {e}")

            # 텔레그램 알림
            if total > 0:
                alert_fn = getattr(self.auto_trader, "_send_alert", None) if self.auto_trader else None
                if alert_fn:
                    msg = format_watchbox_telegram(result)
                    await asyncio.to_thread(alert_fn, msg)

            logger.info(f"[C29] 주목 종목 박스 생성 완료 ({total}종목)")
            return {"watchbox": "OK", "count": total}
        except Exception as e:
            logger.warning(f"[C29] 주목 종목 박스 실패 (무시): {e}")
            return {"watchbox": f"ERROR: {e}"}

    async def _job_us_overnight_filter(self, context=None) -> dict:
        """A11: 미국장 야간 필터 — US 데이터 수집 → 갭 예측 → 진입 모드 결정."""
        try:
            from data.us_market_collector import collect_us_overnight
            from data.us_overnight_filter import run as run_us_filter
            from data.us_overnight_filter import build_telegram_message
            from data.upload_daytrading_us import upload_us_overnight

            # 1) US 데이터 수집
            us_data = await asyncio.to_thread(collect_us_overnight)
            if not us_data:
                logger.warning("[A11] US 데이터 수집 실패")
                return {"us_filter": "NO_DATA"}

            # 2) 필터 실행 (수집된 데이터 기반)
            report = await asyncio.to_thread(run_us_filter)
            if not report:
                logger.warning("[A11] US 필터 실행 실패")
                return {"us_filter": "FILTER_FAIL"}

            mode = report.get("mode", "NORMAL")

            # 3) 텔레그램 알림
            msg = build_telegram_message(report)
            sent = False
            if context and self.bot and getattr(self.bot, "chat_id", None):
                try:
                    await context.bot.send_message(
                        chat_id=self.bot.chat_id, text=msg)
                    sent = True
                except Exception as te:
                    logger.warning(f"[A11] context.bot 송출 실패: {te}")
            if not sent:
                alert_fn = getattr(self.auto_trader, "_send_alert", None) if self.auto_trader else None
                if alert_fn:
                    await asyncio.to_thread(alert_fn, msg)

            # 4) Supabase 업로드 (실패해도 무시)
            try:
                await asyncio.to_thread(upload_us_overnight, report)
            except Exception as ue:
                logger.warning(f"[A11] US 업로드 실패 (무시): {ue}")

            logger.info(
                f"[A11] US 야간 필터 완료: 모드={mode} | "
                f"갭={report.get('gap_signal')} ({report.get('gap_est_pct', 0):+.1f}%) | "
                f"위험={report.get('risk_level', 0)}/5"
            )
            return {"us_filter": "OK", "us_mode": mode}
        except Exception as e:
            logger.warning(f"[A11] US 야간 필터 실패 (무시): {e}")
            return {"us_filter": f"ERROR: {e}"}

    async def _job_auction_scan(self, context=None) -> dict:
        """A15: 동시호가 스캐너 — 08:30~08:53 예상체결가 모니터링 + 텔레그램 알림.

        recommendation.json 추천 종목의 예상체결가/체결량을 30초 간격으로 스캔.
        갭업/갭다운/거래폭발 감지 시 텔레그램 경고.
        08:53 최종 서머리 전송 (G2 08:55 직전).
        """
        from tools.trading_calendar import is_trading_day
        if not is_trading_day():
            logger.info("[A15] 비거래일 — 동시호가 스캐너 스킵")
            return {"auction_scan": "SKIP_NON_TRADING"}

        try:
            from engine.auction_scanner import run_auction_scanner

            # KIS 트레이더 확보
            kis = None
            if self.auto_trader:
                kis = getattr(self.auto_trader, "kis", None)
            if not kis:
                logger.warning("[A15] KIS 트레이더 미연결 — 스킵")
                return {"auction_scan": "NO_KIS"}

            # 텔레그램 전송 함수 생성
            async def send_fn(text: str):
                sent = False
                if context and self.bot and getattr(self.bot, "chat_id", None):
                    try:
                        await context.bot.send_message(
                            chat_id=self.bot.chat_id, text=text)
                        sent = True
                    except Exception as te:
                        logger.warning(f"[A15] context.bot 실패: {te}")
                if not sent:
                    alert_fn = getattr(self.auto_trader, "_send_alert", None) if self.auto_trader else None
                    if alert_fn:
                        if asyncio.iscoroutinefunction(alert_fn):
                            await alert_fn(text)
                        else:
                            await asyncio.to_thread(alert_fn, text)

            # 타임아웃: 최대 25분 (08:30→08:55 사이 안전 마진)
            result = await asyncio.wait_for(
                run_auction_scanner(
                    kis=kis,
                    send_fn=send_fn,
                    interval=30,
                ),
                timeout=1500,  # 25분
            )
            logger.info(f"[A15] 동시호가 스캐너 완료: {result}")
            return {"auction_scan": "OK", **result}
        except asyncio.TimeoutError:
            logger.warning("[A15] 동시호가 스캐너 타임아웃 (25분)")
            return {"auction_scan": "TIMEOUT"}
        except Exception as e:
            logger.warning(f"[A15] 동시호가 스캐너 실패 (무시): {e}")
            return {"auction_scan": f"ERROR: {e}"}

    async def _job_daytrading_picks(self, context=None, mode: str = "confirmed") -> dict:
        """단타 TOP픽 Hybrid 발행.

        mode="preview"   : G7 16:45 국장마감 프리뷰 (미국장 무시, NXT 야간매수용)
        mode="confirmed" : G1 07:35 미국장 반영 확정 (EWY 바스켓 반영, 09:00 진입용)

        실행: scalper-agent/tools/daytrading_picks.py
        출력: data_store/daytrading_picks.json + FLOWX 포맷 + 텔레그램
        """
        try:
            from tools.daytrading_picks import (
                scan_large_caps,
                scan_universe,
                apply_daytrading_filters,
                format_flowx_post,
                format_telegram_message,
                load_insights,
                load_ewy_signal,
                load_universe,
                LARGE_CAP_MIN_FINAL,
                MIN_FINAL_SCORE,
                OUT_PATH,
            )

            universe = load_universe()
            insights = load_insights()

            # preview: EWY 시그널 무시 (미국장 미개장)
            if mode == "preview":
                ewy_signal = {
                    "ewy_1d": 0,
                    "ewy_5d": 0,
                    "ks200_1d": 0,
                    "ks200_5d": 0,
                    "source": "preview_no_us",
                }
            else:
                ewy_signal = await asyncio.to_thread(load_ewy_signal)

            # 트랙 A (대형주)
            large_cands = await asyncio.to_thread(scan_large_caps)
            large_filtered = apply_daytrading_filters(
                large_cands, universe, ewy_signal, insights,
                min_final_score=LARGE_CAP_MIN_FINAL,
            )
            picks_a = large_filtered[:3]
            for p in picks_a:
                p["track"] = "A_대형주"

            # 트랙 B (중소형주)
            small_cands = await asyncio.to_thread(scan_universe, 30)
            small_filtered = apply_daytrading_filters(
                small_cands, universe, ewy_signal, insights,
                min_final_score=MIN_FINAL_SCORE,
            )
            a_codes = {p["code"] for p in picks_a}
            small_filtered = [p for p in small_filtered if p["code"] not in a_codes]
            picks_b = small_filtered[:2]
            for p in picks_b:
                p["track"] = "B_중소형주"

            picks = picks_a + picks_b

            # JSON 저장
            out = {
                "updated": datetime.now().isoformat(),
                "mode": mode,
                "ewy_signal": ewy_signal,
                "picks": picks,
            }
            try:
                OUT_PATH.write_text(
                    json.dumps(out, ensure_ascii=False, indent=2),
                    encoding="utf-8",
                )
            except Exception as se:
                logger.warning(f"[DAYTRADING:{mode}] JSON 저장 실패 (무시): {se}")

            # Supabase 업로드 (있으면)
            try:
                from data.upload_daytrading_picks import upload_daytrading_picks
                await asyncio.to_thread(upload_daytrading_picks, out)
            except ImportError:
                pass  # 업로더 미존재 — 무시
            except Exception as ue:
                logger.warning(f"[DAYTRADING:{mode}] Supabase 업로드 실패 (무시): {ue}")

            # 텔레그램 송출
            if picks:
                msg = format_telegram_message(picks, ewy_signal, mode=mode)
                sent = False
                # 1차: context.bot 직접 사용 (G1 시점에도 동작)
                if context and self.bot and getattr(self.bot, "chat_id", None):
                    try:
                        await context.bot.send_message(
                            chat_id=self.bot.chat_id, text=msg)
                        sent = True
                    except Exception as te:
                        logger.warning(f"[DAYTRADING:{mode}] context.bot 송출 실패: {te}")
                # 2차: auto_trader._send_alert fallback
                if not sent:
                    alert_fn = getattr(self.auto_trader, "_send_alert", None) if self.auto_trader else None
                    if alert_fn:
                        await asyncio.to_thread(alert_fn, msg)
                        sent = True
                if sent:
                    logger.info(f"[DAYTRADING:{mode}] 텔레그램 송출 완료 ({len(picks)}종목)")
                else:
                    logger.warning(f"[DAYTRADING:{mode}] 텔레그램 송출 실패 — bot/auto_trader 모두 불가")
            else:
                logger.warning(f"[DAYTRADING:{mode}] 픽 0개 — 텔레그램 스킵")

            return {
                "daytrading": "OK",
                "mode": mode,
                "count": len(picks),
                "track_a": len(picks_a),
                "track_b": len(picks_b),
            }
        except Exception as e:
            logger.warning(f"[DAYTRADING:{mode}] 실패 (무시): {e}")
            return {"daytrading": f"ERROR: {e}", "mode": mode}

    async def _job_daytrading_performance(self, context=None) -> dict:
        """C31: 단타 TOP 5 일일 성적표 (시가→종가 수익률 + 주간/월간 누적).

        G7 Stage 3에서 실행. confirmed TOP 5의 당일 시가→종가 수익률 계산,
        Supabase 업로드, 텔레그램 발송.
        """
        try:
            from tools.daytrading_performance import build_performance_report
            from data.upload_daytrading_performance import upload_daytrading_performance

            report = await asyncio.to_thread(build_performance_report)
            if not report:
                logger.warning("[C31] 성적표 생성 불가 (confirmed 픽 없음 or OHLCV 실패)")
                return {"performance": "SKIP", "reason": "no data"}

            # Supabase 업로드
            try:
                await asyncio.to_thread(upload_daytrading_performance, report)
            except Exception as ue:
                logger.warning(f"[C31] Supabase 업로드 실패 (무시): {ue}")

            # 텔레그램 송출
            msg = report.get("telegram_msg", "")
            if msg:
                sent = False
                if context and self.bot and getattr(self.bot, "chat_id", None):
                    try:
                        await context.bot.send_message(
                            chat_id=self.bot.chat_id, text=msg)
                        sent = True
                    except Exception as te:
                        logger.warning(f"[C31] context.bot 송출 실패: {te}")
                if not sent:
                    alert_fn = getattr(self.auto_trader, "_send_alert", None) if self.auto_trader else None
                    if alert_fn:
                        await asyncio.to_thread(alert_fn, msg)
                        sent = True
                if sent:
                    logger.info(f"[C31] 성적표 텔레그램 송출 완료 (평균 {report['avg_return']:+.2f}%)")

            return {
                "performance": "OK",
                "avg_return": report["avg_return"],
                "best": report.get("best_pick", ""),
                "worst": report.get("worst_pick", ""),
            }
        except Exception as e:
            logger.warning(f"[C31] 성적표 실패 (무시): {e}")
            return {"performance": f"ERROR: {e}"}

    async def _job_nxt_top5_publish(self, context=None) -> dict:
        """C32: NXT 야간매수 TOP 5 추출 + 발행.

        nightwatch_report.json → supply_score TOP 5 → 진입가 기록 →
        Supabase + 텔레그램 발행.
        """
        try:
            from tools.nxt_performance import extract_nxt_top5, format_nxt_top5_telegram
            from data.upload_nxt_performance import upload_nxt_picks

            picks_data = await asyncio.to_thread(extract_nxt_top5)
            if not picks_data:
                logger.warning("[C32] NXT TOP 5 추출 불가 (nightwatch 없음 or 종가 실패)")
                return {"nxt_top5": "SKIP"}

            # Supabase 업로드 (intelligence_nxt_picks)
            try:
                await asyncio.to_thread(upload_nxt_picks, picks_data)
            except Exception as ue:
                logger.warning(f"[C32] Supabase 업로드 실패 (무시): {ue}")

            # quant_nxt_picks 상세 스코어링 업로드
            try:
                from data.upload_nxt_performance import upload_quant_nxt_picks
                await asyncio.to_thread(upload_quant_nxt_picks)
            except Exception as qe:
                logger.warning(f"[C32] quant_nxt_picks 업로드 실패 (무시): {qe}")

            # 텔레그램 송출
            msg = format_nxt_top5_telegram(picks_data)
            if msg:
                sent = False
                if context and self.bot and getattr(self.bot, "chat_id", None):
                    try:
                        await context.bot.send_message(
                            chat_id=self.bot.chat_id, text=msg)
                        sent = True
                    except Exception as te:
                        logger.warning(f"[C32] context.bot 송출 실패: {te}")
                if not sent:
                    alert_fn = getattr(self.auto_trader, "_send_alert", None) if self.auto_trader else None
                    if alert_fn:
                        await asyncio.to_thread(alert_fn, msg)
                        sent = True
                if sent:
                    logger.info(f"[C32] NXT TOP 5 텔레그램 발행 ({len(picks_data['picks'])}종목)")

            return {"nxt_top5": "OK", "count": len(picks_data["picks"])}
        except Exception as e:
            logger.warning(f"[C32] NXT TOP 5 실패 (무시): {e}")
            return {"nxt_top5": f"ERROR: {e}"}

    async def _job_accumulation_radar(self, context=None) -> dict:
        """C36: 매집 레이더 — 외인 매집 초기 미발화 종목 FLOWX 업로드.

        nightwatch._inject_early_accumulation_targets()로 감지된 종목을
        intelligence_accumulation_radar 테이블에 업로드.
        NXT TOP5 바로 아래, 매매 타임라인 위에 표시.
        """
        try:
            from data.nightwatch import _inject_early_accumulation_targets
            from data.upload_nxt_performance import upload_accumulation_radar
            from datetime import date

            # 감지기 실행
            nxt_targets = []
            injected = await asyncio.to_thread(
                _inject_early_accumulation_targets, nxt_targets
            )
            if not nxt_targets:
                logger.info("[C36] 매집 레이더 종목 없음")
                return {"accumulation_radar": "SKIP", "count": 0}

            # 섹터/테마 태그 생성 (쌍매수가 최강 신호 → 최우선)
            for t in nxt_targets:
                if not t.get("tag"):
                    if t.get("last_dual"):
                        t["tag"] = "쌍매수"
                    elif t.get("chg5", 0) < -3:
                        t["tag"] = "바닥매집"
                    elif t.get("accel_b", 0) > 30:
                        t["tag"] = "가속전환"
                    else:
                        t["tag"] = "외인매집"

            radar_data = {
                "date": date.today().isoformat(),
                "stocks": nxt_targets,
            }

            # 로컬 캐시 저장 (장중 수급 리포트에서 교차 체크용)
            cache_path = Path(__file__).resolve().parent.parent / "data_store" / "accumulation_radar.json"
            cache_path.write_text(json.dumps(radar_data, ensure_ascii=False, indent=2), encoding="utf-8")

            # Supabase 업로드
            await asyncio.to_thread(upload_accumulation_radar, radar_data)

            # 텔레그램 알림 (간단 리스트)
            lines = ["🔍 매집 레이더 — 외인 조용한 매집 감지"]
            lines.append("아직 안 올랐지만 외인이 3일+ 매집 중\n")
            for t in nxt_targets[:5]:
                tag = t.get("tag", "")
                dual_mark = "🔥" if t.get("last_dual") and "쌍" not in tag else ""
                lines.append(
                    f"  {t.get('name','?')} | 외인{t.get('frgn_days',0)}일 "
                    f"가속{t.get('accel_b',0):+.0f}억 | "
                    f"5일{t.get('chg5',0):+.1f}% | [{tag}]{dual_mark}"
                )
            msg = "\n".join(lines)

            sent = False
            if context and self.bot and getattr(self.bot, "chat_id", None):
                try:
                    await context.bot.send_message(
                        chat_id=self.bot.chat_id, text=msg)
                    sent = True
                except Exception as e:
                    logger.debug(f"[C36] 텔레그램 전송 실패: {e}")
            if not sent:
                alert_fn = getattr(self.auto_trader, "_send_alert", None) if self.auto_trader else None
                if alert_fn:
                    await asyncio.to_thread(alert_fn, msg)

            logger.info(f"[C36] 매집 레이더 발행 완료 ({len(nxt_targets)}종목)")
            return {"accumulation_radar": "OK", "count": len(nxt_targets)}

        except Exception as e:
            logger.warning(f"[C36] 매집 레이더 실패 (무시): {e}")
            return {"accumulation_radar": f"ERROR: {e}"}

    async def _job_oneshot_stealth(self, context=None) -> dict:
        """C37: 원샷 쌍매수 잠복 감지 → oneshot_stealth.json + 텔레그램 알림.

        최근 7일 내 외인+기관 동시 대량매수(200억+) 후
        아직 주가가 크게 움직이지 않은 잠복 종목 포착.
        """
        try:
            from data.oneshot_detector import scan_oneshot_stealth, format_oneshot_alert

            result = await asyncio.to_thread(scan_oneshot_stealth)
            stealth_count = result.get("summary", {}).get("stealth_count", 0)

            if stealth_count == 0:
                logger.info("[C37] 원샷 잠복 종목 없음")
                return {"oneshot_stealth": "SKIP", "count": 0}

            # Supabase 업로드 (FLOWX 대시보드 — NXT 아래, 매집 레이더 위)
            upload_ok = False
            try:
                from data.upload_nxt_performance import upload_oneshot_stealth
                upload_ok = await asyncio.to_thread(upload_oneshot_stealth, result)
            except Exception as ue:
                logger.warning(f"[C37] Supabase 업로드 실패 (무시): {ue}")

            # 텔레그램 알림
            msg = format_oneshot_alert(result)
            sent = False
            if context and self.bot and getattr(self.bot, "chat_id", None):
                try:
                    await context.bot.send_message(
                        chat_id=self.bot.chat_id, text=msg)
                    sent = True
                except Exception:
                    pass
            if not sent:
                alert_fn = getattr(self.auto_trader, "_send_alert", None) if self.auto_trader else None
                if alert_fn:
                    await asyncio.to_thread(alert_fn, msg)

            logger.info(
                f"[C37] 원샷 잠복 감지 완료 (잠복 {stealth_count}건, "
                f"Supabase {'OK' if upload_ok else 'SKIP'})"
            )
            return {"oneshot_stealth": "OK", "count": stealth_count, "supabase": upload_ok}

        except Exception as e:
            logger.warning(f"[C37] 원샷 잠복 감지 실패 (무시): {e}")
            return {"oneshot_stealth": f"ERROR: {e}"}

    async def _job_foreign_flow_upload(self, context=None) -> dict:
        """C38: 외국인 돈 흐름 Supabase 업로드.

        시총 2천억+ 전 유니버스 5일 롤링 외인/기관 수급 →
        intelligence_foreign_flow + intelligence_foreign_flow_sector upsert.
        """
        try:
            from tools.upload_foreign_flow import analyze, upload_to_supabase

            stock_rows, sector_rows, last_date = await asyncio.to_thread(analyze)
            if not stock_rows:
                logger.info("[C38] foreign_flow: 감지 종목 없음")
                return {"foreign_flow": "EMPTY"}

            ok = await asyncio.to_thread(upload_to_supabase, stock_rows, sector_rows)
            logger.info(
                f"[C38] foreign_flow 업로드 {'완료' if ok else '실패'} — "
                f"{len(stock_rows)}종목, {len(sector_rows)}섹터 ({last_date})"
            )
            return {"foreign_flow": "OK" if ok else "FAIL",
                    "stocks": len(stock_rows), "sectors": len(sector_rows)}

        except Exception as e:
            logger.warning(f"[C38] foreign_flow 업로드 실패 (무시): {e}")
            return {"foreign_flow": f"ERROR: {e}"}

    async def _job_massive_dual_buy_alert(self, context=None) -> dict:
        """C39: 대량 쌍매수 + 연속급등 감지 → JSON 저장 + 텔레그램 알림.

        매일 16:40 G7에서 실행.
        - detect_massive_dual_buy(): 외인+기관 합산 100억+ 쌍매수
        - detect_consecutive_surge(): 전일 +20% 급등 종목 (연속상한가 후보)
        → 결과를 massive_dual_buy.json / consecutive_surge.json 저장
        → 텔레그램 알림 (상위 10종목)
        """
        try:
            from tools.flow_intelligence import (
                detect_massive_dual_buy, detect_consecutive_surge,
            )

            dual = await asyncio.to_thread(detect_massive_dual_buy)
            surge = await asyncio.to_thread(detect_consecutive_surge)

            # JSON 저장
            dual_path = DATA_STORE / "massive_dual_buy.json"
            surge_path = DATA_STORE / "consecutive_surge.json"

            if dual:
                dual_path.write_text(
                    json.dumps(dual, ensure_ascii=False, indent=2),
                    encoding="utf-8",
                )
            if surge:
                surge_path.write_text(
                    json.dumps(surge, ensure_ascii=False, indent=2),
                    encoding="utf-8",
                )

            # 텔레그램 알림 구성
            lines = []
            if dual and dual.get("stocks"):
                top = dual["stocks"][:10]
                lines.append(f"[대량 쌍매수] {dual.get('date','?')} — {dual.get('detected',0)}종목 감지")
                lines.append("")
                for s in top:
                    lines.append(
                        f"#{s.get('rank','?')} {s.get('name','?')} "
                        f"외{s.get('foreign_억',0):+.0f} 기{s.get('inst_億',0):+.0f} "
                        f"합{s.get('total_億',0):.0f}억 {s.get('pct_1d',0):+.1f}%"
                    )

            if surge and surge.get("stocks"):
                lines.append("")
                lines.append(f"[연속급등 후보] {surge.get('date','?')} +20%↑ — {surge.get('count',0)}종목")
                for s in surge["stocks"]:
                    lines.append(
                        f"  {s.get('name','?')} {s.get('change_pct',0):+.1f}%"
                    )

            if lines:
                msg = "\n".join(lines)
                sent = False
                if context and self.bot and getattr(self.bot, "chat_id", None):
                    try:
                        await context.bot.send_message(
                            chat_id=self.bot.chat_id, text=msg,
                        )
                        sent = True
                    except Exception as e:
                        logger.debug(f"[C39] 텔레그램 직접 전송 실패: {e}")
                if not sent:
                    alert_fn = getattr(self.auto_trader, "_send_alert", None) if self.auto_trader else None
                    if alert_fn:
                        await asyncio.to_thread(alert_fn, msg)
                logger.info(f"[C39] 대량쌍매수 {dual.get('detected',0)}종목 "
                            f"+ 연속급등 {surge.get('count',0)}종목 알림 발송")

            return {
                "massive_dual": dual.get("detected", 0) if dual else 0,
                "consecutive_surge": surge.get("count", 0) if surge else 0,
            }

        except Exception as e:
            logger.warning(f"[C39] 대량쌍매수/연속급등 감지 실패 (무시): {e}")
            return {"massive_dual": 0, "consecutive_surge": 0}

    async def _job_inst_accumulation(self, context=None) -> dict:
        """C40: 기관 연속매수 초기 감지 → FLOWX 업로드.

        시총 1000억+ 종목 중 기관 3일+ 연속 순매수 종목을 감지,
        아직 급등 전(5일<15%)인 초기 매집 종목을 Supabase에 업로드.
        """
        try:
            from data.inst_accumulation_scan import scan_inst_accumulation
            from data.upload_inst_accumulation import upload_inst_accumulation

            result = await asyncio.to_thread(scan_inst_accumulation)
            if not result or (
                not result.get("early_stocks") and not result.get("running_stocks")
            ):
                logger.warning("[C40] 기관 연속매수 감지 종목 없음")
                return {"inst_accumulation": "SKIP"}

            ok = await asyncio.to_thread(upload_inst_accumulation, result)

            # 텔레그램 알림 (TOP 5 초기 매집)
            early = result.get("early_stocks", [])[:5]
            if early:
                lines = [f"[기관매집 레이더] 초기 {result.get('early_count', 0)}종목"]
                for s in early:
                    tag = f" *{s.get('tag','')}" if s.get("tag") else ""
                    lines.append(
                        f"  {s.get('name','?')} 기관{s.get('inst_consec',0)}d "
                        f"+{s.get('inst_cum',0):.0f}억 "
                        f"5d{s.get('ret5',0):+.1f}%{tag}"
                    )
                msg = "\n".join(lines)
                sent = False
                if context and self.bot and getattr(self.bot, "chat_id", None):
                    try:
                        await context.bot.send_message(
                            chat_id=self.bot.chat_id, text=msg,
                        )
                        sent = True
                    except Exception as e:
                        logger.debug(f"[C40] 텔레그램 직접 전송 실패: {e}")
                if not sent:
                    alert_fn = (
                        getattr(self.auto_trader, "_send_alert", None)
                        if self.auto_trader else None
                    )
                    if alert_fn:
                        await asyncio.to_thread(alert_fn, msg)

            logger.info(
                f"[C40] 기관매집 {'완료' if ok else '실패'} — "
                f"초기 {result.get('early_count', 0)}종목 / "
                f"진행 {result.get('running_count', 0)}종목"
            )
            return {"inst_accumulation": result.get("total_count", 0)}

        except Exception as e:
            logger.warning(f"[C40] 기관 연속매수 감지 실패 (무시): {e}")
            return {"inst_accumulation": f"ERROR: {e}"}

    async def _job_pension_scan(self, context=None, _is_retry=False) -> dict:
        """C41: 연기금+금투 합류 타이밍 스캔 → FLOWX 업로드.

        연기금 7일+(10일 윈도우 빈도 기반) 종목 중 금투가 합류한 종목 감지.
        pension_score 기반 TOP 랭킹 + Supabase 업로드.
        백테스트: D+5 +1.59%, 외인 방향 무관.

        quant_investor_extra.json 갱신(~17:32)보다 먼저 실행되면
        데이터가 전날이므로, 35분 후 1회 재시도를 예약한다.
        """
        try:
            from data.pension_finance_scan import scan_pension_finance
            from data.upload_pension_scan import upload_pension_scan
            from datetime import date as _date

            result = await asyncio.to_thread(scan_pension_finance)
            if not result or (
                not result.get("best_stocks") and not result.get("standby_stocks")
            ):
                logger.warning("[C41] 연기금+금투 스캔 종목 없음")
                return {"pension_scan": "SKIP"}

            # 날짜 검증: quant 데이터가 오늘인지 확인
            today_str = _date.today().strftime("%Y-%m-%d")
            data_date = result.get("date", "")
            if data_date != today_str and not _is_retry:
                logger.warning(
                    f"[C41] quant 미갱신 ({data_date} ≠ {today_str}) "
                    f"— 업로드 스킵, 35분 후 재시도 예약"
                )
                asyncio.create_task(self._pension_scan_delayed_retry(context))
                return {"pension_scan": f"STALE({data_date})"}

            ok = await asyncio.to_thread(upload_pension_scan, result)

            # 텔레그램 알림 (수급 강도 TOP 5)
            ranked = result.get("ranked_stocks", [])
            if ranked:
                lines = [f"[매집 합류 시그널] 수급TOP (D+5 +1.6%)"]
                for i, s in enumerate(ranked[:5], 1):
                    joined_tag = (
                        "오늘" if s.get("fi_joined") == "TODAY"
                        else "어제" if s.get("fi_joined") == "YESTERDAY"
                        else "대기"
                    )
                    p_cum = s.get('pension_cum', 0) or 0
                    f_today = s.get('fi_today', 0) or 0
                    lines.append(
                        f"  {i}. {s.get('name','?')} {s.get('pension_score', 0)}점 "
                        f"연{s.get('pension_buy_days', 0)}d "
                        f"누적{p_cum:+.0f}억 "
                        f"금투{f_today:+.0f}억 "
                        f"[{joined_tag}]"
                    )
                standby_count = result.get("standby_count", 0)
                if standby_count:
                    lines.append(f"  +대기 {standby_count}종목")
                msg = "\n".join(lines)
                sent = False
                if context and self.bot and getattr(self.bot, "chat_id", None):
                    try:
                        await context.bot.send_message(
                            chat_id=self.bot.chat_id, text=msg,
                        )
                        sent = True
                    except Exception as e:
                        logger.debug(f"[C41] 텔레그램 전송 실패: {e}")
                if not sent:
                    alert_fn = (
                        getattr(self.auto_trader, "_send_alert", None)
                        if self.auto_trader else None
                    )
                    if alert_fn:
                        await asyncio.to_thread(alert_fn, msg)

            logger.info(
                f"[C41] 연기금스캔 {'완료' if ok else '실패'} — "
                f"핵심 {result.get('best_count', 0)}종목 / "
                f"대기 {result.get('standby_count', 0)}종목"
            )
            return {"pension_scan": result.get("total_count", 0)}

        except Exception as e:
            logger.warning(f"[C41] 연기금+금투 스캔 실패 (무시): {e}")
            return {"pension_scan": f"ERROR: {e}"}

    async def _pension_scan_delayed_retry(self, context=None):
        """C41 지연 재시도 — quant_investor_extra 갱신(~17:32) 대기 후 1회 재실행."""
        try:
            await asyncio.sleep(35 * 60)  # 35분 대기
            logger.info("[C41] 지연 재시도 시작 (quant 갱신 대기 완료)")
            result = await self._job_pension_scan(context, _is_retry=True)
            logger.info(f"[C41] 지연 재시도 결과: {result}")
        except Exception as e:
            logger.warning(f"[C41] 지연 재시도 실패: {e}")

    async def _job_pension_ownership(self, context=None) -> dict:
        """C42: 연기금 지분 추적기 — 국민연금 보유 추이 vs 주가 상관분석.

        NPS_BASE_OWNERSHIP(DART 공시) + quant_investor_extra(매일 순매수)
        → 연기금 매수 추이 vs 주가 상관계수 → FLOWX 업로드.
        """
        try:
            from data.pension_ownership_tracker import track_pension_ownership
            from data.upload_pension_ownership import upload_pension_ownership

            result = await asyncio.to_thread(track_pension_ownership)
            if not result or not result.get("stocks"):
                logger.warning("[C42] 연기금보유추적 종목 없음")
                return {"pension_ownership": "SKIP"}

            ok = await asyncio.to_thread(upload_pension_ownership, result)
            summary = result.get("summary", {})
            logger.info(
                f"[C42] 연기금보유추적 {'완료' if ok else '실패'} — "
                f"{result.get('total_tracked', 0)}종목 · "
                f"매집 {summary.get('accumulating_count', 0)} / "
                f"매도 {summary.get('distributing_count', 0)} · "
                f"상관 {summary.get('avg_correlation', 0):.3f}"
            )
            return {"pension_ownership": result.get("total_tracked", 0)}

        except Exception as e:
            logger.warning(f"[C42] 연기금보유추적 실패 (무시): {e}")
            return {"pension_ownership": f"ERROR: {e}"}

    async def _job_nxt_performance(self, context=None) -> dict:
        """C33: 어제 NXT TOP 5 성적표.

        어제 NXT 추천 → 오늘 종가 기준 수익률 → 주간/월간 누적 →
        Supabase + 텔레그램 발행.
        """
        try:
            from tools.nxt_performance import build_nxt_performance_report
            from data.upload_nxt_performance import upload_nxt_performance

            report = await asyncio.to_thread(build_nxt_performance_report)
            if not report:
                logger.warning("[C33] NXT 성적표 불가 (어제 픽 없음 or OHLCV 실패)")
                return {"nxt_perf": "SKIP"}

            # Supabase 업로드
            try:
                await asyncio.to_thread(upload_nxt_performance, report)
            except Exception as ue:
                logger.warning(f"[C33] Supabase 업로드 실패 (무시): {ue}")

            # 텔레그램 송출
            msg = report.get("telegram_msg", "")
            if msg:
                sent = False
                if context and self.bot and getattr(self.bot, "chat_id", None):
                    try:
                        await context.bot.send_message(
                            chat_id=self.bot.chat_id, text=msg)
                        sent = True
                    except Exception as te:
                        logger.warning(f"[C33] context.bot 송출 실패: {te}")
                if not sent:
                    alert_fn = getattr(self.auto_trader, "_send_alert", None) if self.auto_trader else None
                    if alert_fn:
                        await asyncio.to_thread(alert_fn, msg)
                        sent = True
                if sent:
                    logger.info(f"[C33] NXT 성적표 텔레그램 발행 (평균 {report['avg_return']:+.2f}%)")

            return {
                "nxt_perf": "OK",
                "pick_date": report["pick_date"],
                "avg_return": report["avg_return"],
            }
        except Exception as e:
            logger.warning(f"[C33] NXT 성적표 실패 (무시): {e}")
            return {"nxt_perf": f"ERROR: {e}"}

    async def _job_nxt_early_collect(self, context=None) -> dict:
        """C4E: NXT 사전 데이터 수집 + 예비 알림 발송.

        G6에서 C3(일봉수집 90분)과 독립 병렬로 실행.
        stages 1~4를 캐시에 저장하여 16:35 run_nightwatch() 가속.
        NQ 데이터 수집 성공 시 즉시 예비 알림 텔레그램 발송.
        """
        try:
            from data.nightwatch import collect_nxt_early_data, format_nxt_pre_alert

            data = await asyncio.to_thread(collect_nxt_early_data)
            logger.info(f"[C4E] NXT 사전 수집 완료: {data.get('collected_at', '')}")

            # NQ 데이터 있으면 예비 알림 발송
            nq_pct = data.get("macro_conditions", {}).get("nasdaq_pct")
            if nq_pct is not None and self.auto_trader:
                msg = format_nxt_pre_alert(data)
                alert_fn = getattr(self.auto_trader, "_send_alert", None)
                if alert_fn and msg:
                    await asyncio.to_thread(alert_fn, msg)
                    logger.info("[C4E] NXT 예비 알림 발송 완료")

            return {"nxt_early": "OK", "nasdaq_pct": nq_pct}
        except Exception as e:
            logger.warning(f"[C4E] NXT 사전 수집 실패 (무시): {e}")
            return {"nxt_early": f"ERROR: {e}"}

    # ─────────────────────────────────────────────
    # FALLBACK-C13: 이브닝 분석 실패 시 복구
    # ─────────────────────────────────────────────
    async def _fallback_evening_analysis(self, context=None) -> bool:
        """C13 job_evening_analysis 실패 시 폴백.

        a. 3분 대기 → C13 재시도 (1회)
        b. 재시도 실패 → 전일 recommendation.json 유지
        c. 텔레그램 경고: 내일 추천 정확도 저하 알림
        """
        # (a) 3분 대기 후 재시도
        logger.info("[COO] FALLBACK-C13: 3분 대기 후 재시도...")
        await asyncio.sleep(180)

        recovered = False
        if self.auto_trader:
            r = await self.run_job_safe_async(
                "C13_evening_analysis_retry",
                self.auto_trader.job_evening_analysis(context),
                timeout=2700,
            )
            if r.success:
                logger.info("[COO] FALLBACK-C13: 재시도 성공!")
                recovered = True

        # (b) 재시도 실패 → 전일 recommendation.json 유지 (파일 덮어쓰지 않으므로 자동)
        if not recovered:
            logger.warning("[COO] FALLBACK-C13: 재시도 실패 — "
                           "전일 recommendation.json 유지")

        # (c) 텔레그램 경고
        try:
            alert_fn = getattr(self.auto_trader, "_send_alert", None)
            if alert_fn:
                if recovered:
                    msg = "⚠️ 이브닝분석: 재시도 성공"
                else:
                    msg = ("🚨 이브닝분석 실패\n"
                           "전일 recommendation.json 유지\n"
                           "내일 추천 정확도 저하 가능")
                await asyncio.to_thread(alert_fn, msg)
        except Exception as e:
            logger.warning(f"[COO] FALLBACK-C13 텔레그램 경고 실패: {e}")

        return recovered

    # ─────────────────────────────────────────────
    # brain_report.json 갱신 safeguard
    # ─────────────────────────────────────────────
    async def _ensure_brain_report_today(self):
        """C13 실패 시에도 brain_report.json이 오늘 날짜로 갱신되도록 보장.

        brain_report는 추천 파이프라인(C13) 내부에서 생성되므로
        C13 TIMEOUT 시 brain_report도 미갱신됨.
        이 safeguard가 G7 종료 직전에 체크하여 독립 생성.
        """
        import json
        from pathlib import Path

        brain_path = Path(__file__).parent.parent / "data_store" / "brain_report.json"

        # 오늘 날짜 확인
        needs_update = True
        if brain_path.exists():
            try:
                with open(brain_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                if data.get("date") == self.today:
                    needs_update = False
            except Exception:
                pass

        if not needs_update:
            logger.info("[COO] brain_report.json 이미 오늘 날짜 — 스킵")
            return

        # 독립 생성
        logger.info("[COO] brain_report.json 미갱신 감지 — 독립 생성 시작")
        try:
            from data.market_brain import generate_brain_report, save_brain_report
            brain = await asyncio.to_thread(generate_brain_report)
            await asyncio.to_thread(save_brain_report, brain)
            logger.info(f"[COO] brain_report.json 독립 생성 완료: "
                        f"{brain.overall_verdict} | 비중 {brain.position_size_pct}%")
        except Exception as e:
            logger.error(f"[COO] brain_report.json 독립 생성 실패: {e}")

    # ─────────────────────────────────────────────
    # G7 자동복구 시스템 (AUTO-RECOVERY)
    # ─────────────────────────────────────────────
    # 개별 복구 타임아웃 (초) — 5분이면 충분, 그 이상은 실패 처리
    _RECOVERY_TIMEOUT = 300
    # 전체 AUTO-RECOVERY 총 타임아웃 (초) — 10분 초과 시 나머지 스킵
    _RECOVERY_TOTAL_TIMEOUT = 600

    async def _post_g7_auto_recovery(self, context=None):
        """G7 완료 후 핵심 데이터 파일 검증 + 실패 항목 자동 복구.

        검증 대상 6개 (핵심 데이터만):
        1. recommendation.json — 오늘 날짜 (C13)
        2. brain_report.json   — 오늘 날짜 (C13 내부)
        3. sector_flow.json    — 오늘 날짜 (C20)
        4. etf_flow.json       — 오늘 날짜 (C21)
        5. sector_momentum.json — 오늘 날짜 (C20 내부)
        6. investor_flow       — 오늘 날짜

        복구 전략:
        - 개별 생성 함수를 직접 호출 (C13 전체 재실행 안 함)
        - 개별 타임아웃 5분 / 전체 타임아웃 10분
        - 병렬 실행으로 속도 향상
        """
        import json
        from pathlib import Path
        import time

        logger.info("[COO] ══ AUTO-RECOVERY 시작 ══")
        recovery_start = time.monotonic()

        data_dir = Path(__file__).parent.parent / "data_store"
        today = self.today  # "YYYY-MM-DD"

        # ── 검증 대상 정의 ──
        checks = [
            {
                "name": "recommendation.json",
                "path": data_dir / "recommendation.json",
                "date_key": "date",
                "recover": self._recover_recommendation,
            },
            {
                "name": "brain_report.json",
                "path": data_dir / "brain_report.json",
                "date_key": "date",
                "recover": self._recover_brain_report,
            },
            {
                "name": "sector_flow.json",
                "path": data_dir / "sector_flow.json",
                "date_key": "date",
                "recover": self._recover_sector_flow,
            },
            {
                "name": "etf_flow.json",
                "path": data_dir / "etf_flow.json",
                "date_key": "date",
                "recover": self._recover_etf_flow,
            },
            {
                "name": "sector_momentum.json",
                "path": data_dir / "sector_momentum.json",
                "date_key": "date",
                "recover": self._recover_sector_momentum,
            },
            {
                "name": "investor_flow",
                "path": data_dir / "flow" / "_last_update.json",
                "date_key": "date",
                "recover": self._recover_investor_flow,
            },
            {
                "name": "nationality_xray",
                "path": data_dir / "nationality" / "_last_upload.json",
                "date_key": "date",
                "recover": self._recover_nationality_flows,
            },
        ]

        # ── 1단계: 검증 ──
        stale = []
        fresh = []
        for chk in checks:
            ok = self._check_freshness(chk["path"], today, chk["date_key"])
            if ok:
                fresh.append(chk["name"])
            else:
                stale.append(chk)

        logger.info(f"[AUTO-RECOVERY] 검증: {len(fresh)}개 정상 / "
                     f"{len(stale)}개 미갱신")

        if not stale:
            logger.info("[AUTO-RECOVERY] 전체 정상 — 복구 불필요")
            await self._send_recovery_alert(
                context, fresh, [], [], "✅ 전체 정상")
            return

        # ── C13 2중 실행 방지: 추천 파이프라인 진행 중이면 recommendation 복구 스킵 ──
        if self._c13_recommendation_running:
            before_len = len(stale)
            stale = [chk for chk in stale if chk["name"] != "recommendation.json"]
            if len(stale) < before_len:
                logger.info("[AUTO-RECOVERY] recommendation 스킵 (C13 FALLBACK 진행 중)")
                fresh.append("recommendation.json (C13 진행중)")
            if not stale:
                logger.info("[AUTO-RECOVERY] C13 제외 후 복구 대상 없음")
                await self._send_recovery_alert(
                    context, fresh, [], [], "✅ 전체 정상 (C13 진행중)")
                return

        # ── 2단계: 병렬 복구 (개별 5분 / 전체 10분 타임아웃) ──
        async def _run_one(chk):
            """개별 복구 실행 + 타임아웃 + 재검증."""
            name = chk["name"]
            logger.info(f"[AUTO-RECOVERY] {name} 복구 시도...")
            try:
                await asyncio.wait_for(
                    chk["recover"](),
                    timeout=self._RECOVERY_TIMEOUT,
                )
                if self._check_freshness(
                        chk["path"], today, chk["date_key"]):
                    logger.info(f"[AUTO-RECOVERY] {name} 복구 성공 ✅")
                    return ("ok", name)
                else:
                    logger.warning(
                        f"[AUTO-RECOVERY] {name} 복구 실행됐지만 "
                        f"여전히 미갱신 ❌")
                    return ("fail", name)
            except asyncio.TimeoutError:
                logger.error(
                    f"[AUTO-RECOVERY] {name} 타임아웃 "
                    f"({self._RECOVERY_TIMEOUT}초) ❌")
                return ("fail", name)
            except Exception as e:
                logger.error(f"[AUTO-RECOVERY] {name} 복구 실패: {e}")
                return ("fail", name)

        try:
            results = await asyncio.wait_for(
                asyncio.gather(*[_run_one(chk) for chk in stale],
                               return_exceptions=True),
                timeout=self._RECOVERY_TOTAL_TIMEOUT,
            )
        except asyncio.TimeoutError:
            elapsed = time.monotonic() - recovery_start
            logger.error(
                f"[AUTO-RECOVERY] 전체 타임아웃 "
                f"({self._RECOVERY_TOTAL_TIMEOUT}초, "
                f"경과 {elapsed:.0f}초) — 나머지 스킵")
            results = []

        recovered = []
        failed = []
        for r in results:
            if isinstance(r, Exception):
                failed.append("(예외)")
            elif isinstance(r, tuple):
                if r[0] == "ok":
                    recovered.append(r[1])
                else:
                    failed.append(r[1])

        # 전체 타임아웃으로 결과 못 받은 항목 → 실패 처리
        reported = set(recovered) | set(failed)
        for chk in stale:
            if chk["name"] not in reported:
                failed.append(chk["name"])

        # ── 3단계: 결과 리포트 ──
        elapsed = time.monotonic() - recovery_start
        if failed:
            status = (f"⚠️ {len(recovered)}복구 / {len(failed)}실패 "
                      f"({elapsed:.0f}초)")
        else:
            status = f"✅ {len(recovered)}건 전량 복구 ({elapsed:.0f}초)"

        logger.info(f"[AUTO-RECOVERY] 결과: {status}")
        await self._send_recovery_alert(
            context, fresh, recovered, failed, status)

    def _check_freshness(self, path, today: str,
                         date_key: str | None) -> bool:
        """파일이 오늘 날짜인지 검증.

        investor_flow 마커: 수집률 90% 미만이면 미갱신으로 판정 → 재수집 트리거.
        """
        import json
        try:
            if not path.exists():
                return False

            if date_key:
                # JSON 내부 date 필드 확인
                with open(path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                file_date = str(data.get(date_key, ""))
                if file_date == today:
                    # investor_flow 마커: 수집률 90% 미만이면 재수집 필요
                    coverage = data.get("coverage_pct", 100)
                    if coverage < 90:
                        logger.info(f"[AUTO-RECOVERY] {path.name}: "
                                     f"수집률 {coverage}% < 90% → 재수집 필요")
                        return False
                    return True
                # fallback: timestamp 필드에서 날짜 추출
                # (recommendation.json 등 date 키 없이 timestamp만 있는 경우)
                ts = data.get("timestamp", "")
                if ts and str(ts)[:10] == today:
                    return True
                return False
            else:
                # mtime 기반 (당일 수정 여부)
                import os
                from datetime import datetime
                mtime = os.path.getmtime(path)
                file_date = datetime.fromtimestamp(mtime).strftime(
                    "%Y-%m-%d")
                return file_date == today
        except Exception:
            return False

    async def _send_recovery_alert(self, context, fresh: list,
                                   recovered: list, failed: list,
                                   status: str):
        """자동복구 결과 텔레그램 발송."""
        lines = [
            f"🔄 AUTO-RECOVERY 리포트",
            f"날짜: {self.today}",
            f"상태: {status}",
            f"",
            f"정상: {len(fresh)}개",
        ]
        if recovered:
            lines.append(f"복구 성공: {', '.join(recovered)}")
        if failed:
            lines.append(f"❌ 복구 실패: {', '.join(failed)}")
            lines.append(f"→ 수동 확인 필요!")

        msg = "\n".join(lines)

        if self.bot and hasattr(self.bot, "chat_id") and self.bot.chat_id:
            try:
                await context.bot.send_message(
                    chat_id=self.bot.chat_id, text=msg)
            except Exception as e:
                logger.warning(
                    f"[AUTO-RECOVERY] 텔레그램 발송 실패: {e}")

        logger.info(f"[AUTO-RECOVERY] 리포트 발송 완료")

    # ── 개별 복구 함수 ──

    async def _recover_recommendation(self):
        """recommendation.json 독립 복구 — 전체 이브닝 추천 파이프라인.

        외부에서 _RECOVERY_TIMEOUT(5분)으로 타임아웃 제어.
        """
        from data.morning_recommendation import run_evening_recommendation
        await asyncio.to_thread(run_evening_recommendation)

    async def _recover_brain_report(self):
        """brain_report.json 독립 복구."""
        from data.market_brain import generate_brain_report, save_brain_report
        brain = await asyncio.to_thread(generate_brain_report)
        await asyncio.to_thread(save_brain_report, brain)

    async def _recover_sector_flow(self):
        """sector_flow.json 독립 복구 — 분석+저장 내장."""
        from data.sector_institution_flow import analyze_sector_flow
        await asyncio.to_thread(analyze_sector_flow)

    async def _recover_etf_flow(self):
        """etf_flow.json 독립 복구 — 분석+저장 내장."""
        from data.etf_fund_flow import analyze_etf_flow
        await asyncio.to_thread(analyze_etf_flow)

    async def _recover_sector_momentum(self):
        """sector_momentum.json 독립 복구 — 분석+저장 내장."""
        from data.sector_momentum import analyze_sectors
        await asyncio.to_thread(analyze_sectors)

    async def _recover_investor_flow(self):
        """investor_flow 독립 복구 — KIS 토큰 재발급 + 수급 재수집.

        flow_collector에 token.dat 자동 삭제 + 3회 재시도 로직이 있으므로
        토큰 만료 시에도 자동 복구됨.
        외부에서 _RECOVERY_TIMEOUT(5분)으로 타임아웃 제어.
        """
        from data.flow_collector import collect_all_flow
        await asyncio.to_thread(collect_all_flow)

    async def _recover_nationality_flows(self):
        """nationality_flows Supabase 업로드 독립 복구.

        nationality CSV 데이터가 존재하면 Supabase에 재업로드.
        성공 시 _last_upload.json 마커 파일 갱신.
        """
        from data.upload_short import upload_nationality_flows
        await asyncio.to_thread(upload_nationality_flows)

    # ═════════════════════════════════════════════
    # STEP 3-9: setup_schedule() — JobQueue 등록
    # ═════════════════════════════════════════════
    def setup_schedule(self, jq):
        """COO 그룹 스케줄을 JobQueue에 등록.

        기존 스케줄은 그대로 유지! COO는 래퍼이지 대체가 아님.
        STEP 4에서 중복 정리 예정.
        """
        KST = timezone(timedelta(hours=9))

        def kst_time(h, m):
            return dtime(h, m, tzinfo=KST)

        # ── G1 MORNING_PREP (06:30) ──
        jq.run_daily(self.run_g1, time=kst_time(6, 30))
        logger.info("[COO] G1 MORNING_PREP 등록: 06:30 KST")

        # ── A15 동시호가 스캐너 (08:30) ──
        jq.run_daily(self._job_auction_scan, time=kst_time(8, 30))
        logger.info("[COO] A15 동시호가 스캐너 등록: 08:30 KST")

        # ── G2 MORNING_LAUNCH (08:55) ──
        jq.run_daily(self.run_g2, time=kst_time(8, 55))
        logger.info("[COO] G2 MORNING_LAUNCH 등록: 08:55 KST")

        # ── G3 INTRADAY_INIT (09:00) ──
        jq.run_daily(self.run_g3, time=kst_time(9, 0))
        logger.info("[COO] G3 INTRADAY_INIT 등록: 09:00 KST")

        # ── G4 INTRADAY_LOOP (09:00) ──
        jq.run_daily(self.run_g4_setup, time=kst_time(9, 0))
        logger.info("[COO] G4 INTRADAY_LOOP 등록: 09:00 KST")

        # ── 모니터 헬스체크 (5분 반복) ──
        jq.run_repeating(self.check_monitor_health, interval=300, first=600)
        logger.info("[COO] 모니터 헬스체크 등록: 300초 반복")

        # ── G5 MARKET_CLOSE (15:10) ──
        jq.run_daily(self.run_g5, time=kst_time(15, 10))
        logger.info("[COO] G5 MARKET_CLOSE 등록: 15:10 KST")

        # ── G6 DATA_PIPELINE (15:40) ──
        jq.run_daily(self.run_g6, time=kst_time(15, 40))
        logger.info("[COO] G6 DATA_PIPELINE 등록: 15:40 KST")

        # ── G7 EVENING_BRAIN (16:30) ──
        jq.run_daily(self.run_g7, time=kst_time(16, 30))
        logger.info("[COO] G7 EVENING_BRAIN 등록: 16:30 KST")

        # ── G7 자동복구: G6 done + G7 pending + 현재 16:35 이후 → 60초 후 재실행 ──
        now_kst = datetime.now(KST)
        g6_done = self.group_status.get("G6") in (
            GroupStatus.DONE, GroupStatus.PARTIAL)
        g7_pending = self.group_status.get("G7") == GroupStatus.PENDING
        past_g7_time = (now_kst.hour > 16 or
                        (now_kst.hour == 16 and now_kst.minute >= 35))
        if g6_done and g7_pending and past_g7_time:
            logger.warning(
                "[COO] ⚠️ G7 미완료 감지 (G6=done, G7=pending) "
                "— 60초 후 자동복구 실행")
            jq.run_once(self.run_g7, when=60)

        logger.info("[COO] ═══ 전체 스케줄 등록 완료 (G1~G7) ═══")
