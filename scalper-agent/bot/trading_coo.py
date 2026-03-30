# -*- coding: utf-8 -*-
"""
Trading COO (Chief Operating Officer) — 매매봇 운영 총괄 시스템
=================================================================
텔레그램 스케줄러 위에 올리는 래퍼 레이어.
기존 모듈을 절대 수정하지 않고, 실행 순서 보장 + 예외 격리 + 상태 기록.

7-Group 구조:
  G1 MORNING_PREP   — 08:00~08:50 (데이터 검증, 추천 생성, 브레인)
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
from typing import Any, Callable, Coroutine, Dict, List, Optional, Tuple

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
        # G6 데이터 파이프라인 모드 (NORMAL/STALE/DEGRADED)
        self._g6_mode = "NORMAL"

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
    ) -> JobResult:
        """async 잡을 try/except + asyncio.wait_for 타임아웃으로 실행.

        Args:
            name: 잡 이름
            coro: 실행할 코루틴 (이미 생성된 상태)
            timeout: 초 단위 타임아웃 (None이면 무제한)
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
            logger.warning(f"[COO] {name} 타임아웃 ({timeout}초)")
            return JobResult(name, False, elapsed, f"TIMEOUT ({timeout}s)")

        except Exception as e:
            elapsed = time.monotonic() - start
            err = str(e)
            logger.error(f"[COO] {name} 예외: {err}")
            return JobResult(name, False, elapsed, err)

    # ─────────────────────────────────────────────
    # run_parallel_async: async 병렬 실행 래퍼
    # ─────────────────────────────────────────────
    async def run_parallel_async(
        self,
        jobs: List[Tuple[str, Coroutine]],
        timeout_per_job: Optional[float] = None,
    ) -> List[JobResult]:
        """async 잡들을 asyncio.gather로 병렬 실행, 개별 예외 격리.

        Args:
            jobs: [(name, coroutine), ...] 리스트
            timeout_per_job: 잡별 타임아웃 (초)
        Returns:
            JobResult 리스트
        """
        if not jobs:
            return []

        tasks = [
            self.run_job_safe_async(name, coro, timeout=timeout_per_job)
            for name, coro in jobs
        ]
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
        """G1 MORNING_PREP — 10개 잡 병렬 실행.

        전부 non-critical → 개별 실패해도 계속 진행.
        결과를 update_group + morning_state.json 영속화.

        잡 목록 (A1~A10):
          A1  job_us_market_check    — 미국시장 체크 (AutoTrader)
          A2  _job_policy_scan       — 정책 트래커 (TelegramBot)
          A3  _job_global_event_scan — 해외 이벤트 (TelegramBot)
          A4  job_nxt_morning_sell   — NXT 아침 매도 (AutoTrader)
          A5  _job_dart_refresh      — DART 공시 (TelegramBot)
          A6  _job_options_expiry_alert — 옵션 만기 (TelegramBot)
          A7  _job_position_guardian — 포지션 가디언 (TelegramBot)
          A8  job_premium_levels     — 프리미엄 레벨 (AutoTrader)
          A9  _job_rebuild_universe  — 유니버스 리빌드 (TelegramBot)
          A10 _job_premove_scan      — 사전감지 (TelegramBot)
        """
        logger.info("[COO] ═══ G1 MORNING_PREP 시작 ═══")
        self.group_status["G1"] = GroupStatus.RUNNING
        self.save_state()

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

        # 10개 잡 병렬 실행 — 개별 타임아웃 300초 (5분)
        results = await self.run_parallel_async(jobs, timeout_per_job=300)

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

        잡 목록 (B1~B6):
          B1 _job_war_startup       — 전쟁모드 시작 (TelegramBot)
          B2 _job_start_tick_polling — 체결 폴링 시작 (TelegramBot)
          B4 job_gap_support        — 갭 지지/저항 (AutoTrader)
          B5 _job_paper_register    — PAPER 등록 (TelegramBot)
          B6 _job_intraday_tv_init  — TV 스캔 초기화 (TelegramBot)
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

        # TelegramBot 잡 (4개)
        if self.bot:
            jobs.extend([
                ("B1_war_startup",
                 self.bot._job_war_startup(context)),
                ("B2_tick_polling",
                 self.bot._job_start_tick_polling(context)),
                ("B5_paper_register",
                 self.bot._job_paper_register(context)),
                ("B6_tv_init",
                 self.bot._job_intraday_tv_init(context)),
            ])
        else:
            logger.warning("[COO] bot 미연결 — B1/B2/B5/B6 스킵")

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
        results = await self.run_parallel_async(jobs, timeout_per_job=120)

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
            return

        # 둘 다 5분+ 정체
        elapsed_min = min(positions_age, log_age) / 60
        logger.error(f"[COO] B3 모니터 {elapsed_min:.1f}분 미응답 — "
                     "FALLBACK-B3 실행")
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

        # ── 4) C1 _job_collect_minutes (non-critical) ──
        if self.bot:
            r = await self.run_job_safe_async(
                "C1_collect_minutes",
                self.bot._job_collect_minutes(context),
                timeout=600,
            )
            results.append(r)
            if not r.success:
                logger.warning("[COO] C1 분봉 수집 실패 — 스킵")
        else:
            logger.warning("[COO] bot 미연결 — C1 스킵")

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

        # (d) 텔레그램 경고
        try:
            alert_fn = getattr(self.auto_trader, "_send_alert", None)
            if alert_fn:
                if recovered:
                    msg = "⚠️ 일봉수집: 재시도 성공"
                else:
                    msg = ("⚠️ 일봉수집 실패\n"
                           "전일 데이터 기반 운용\n"
                           "내일 추천 정확도 저하 가능")
                await asyncio.to_thread(alert_fn, msg)
        except Exception as e:
            logger.warning(f"[COO] FALLBACK-C3 텔레그램 경고 실패: {e}")

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

        # (c) 텔레그램 경고
        try:
            alert_fn = getattr(self.auto_trader, "_send_alert", None)
            if alert_fn:
                if recovered:
                    msg = "⚠️ 데이터 검증: 재시도 성공"
                else:
                    msg = ("🚨 데이터 검증 실패\n"
                           "DEGRADED 모드 — 선취매 비활성\n"
                           "수동 확인 필요")
                await asyncio.to_thread(alert_fn, msg)
        except Exception as e:
            logger.warning(f"[COO] FALLBACK-C7 텔레그램 경고 실패: {e}")

        return recovered

    # ═════════════════════════════════════════════
    # G7 EVENING_BRAIN — 이브닝 브레인 + 선취매
    # ═════════════════════════════════════════════
    async def run_g7(self, context=None):
        """G7 EVENING_BRAIN 실행.

        3-Stage 실행:
        - Stage 1: C8+C10+C11 병렬 (신호기록 / 스윙선정 / 브레인배분)
        - Stage 2: C12→C13 순차 (일간학습 → ★이브닝분석 CRITICAL)
        - Stage 3: C14~C24 병렬 (클로징/선취매/MACD/TRIX/국적/헬스/스윙/수급/ETF/퀀트대시/CTO정확도)

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
            # CEASEFIRE_LIKELY 이상이면 텔레그램 알림
            alert_msg = format_war_signal_alert(ws)
            if alert_msg and hasattr(self, '_bot') and self._bot:
                await self._bot.send_message(alert_msg)
        except Exception as e:
            logger.warning(f"[COO] G7: 전쟁 시그널 갱신 실패 (무시): {e}")

        # ── g6_mode 경고 ──
        if self._g6_mode == "STALE":
            logger.warning("[COO] G7: STALE 데이터 — 선취매 주의")
        elif self._g6_mode == "DEGRADED":
            logger.warning("[COO] G7: DEGRADED — 선취매 스킵")

        # ── Stage 1: C8 + C10 + C11 병렬 (300s) ──
        logger.info("[COO] G7 Stage 1: C8+C10+C11 병렬")
        stage1_jobs = []

        if self.bot:
            stage1_jobs.append((
                "C8_record_signals",
                self.bot._job_record_signals(context),
            ))
            stage1_jobs.append((
                "C10_swing_picker",
                self.bot._job_swing_picker(context),
            ))

        if self.auto_trader:
            stage1_jobs.append((
                "C11_brain_allocation",
                self.auto_trader.job_brain_allocation(context),
            ))

        if stage1_jobs:
            s1 = await self.run_parallel_async(stage1_jobs, timeout_per_job=300)
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

        # C13: ★ 이브닝 분석 (CRITICAL)
        c13_ok = False
        if self.auto_trader:
            r13 = await self.run_job_safe_async(
                "C13_evening_analysis",
                self.auto_trader.job_evening_analysis(context),
                timeout=600,
            )
            results.append(r13)
            c13_ok = r13.success

            if not c13_ok:
                logger.error("[COO] ★ C13 이브닝분석 실패 — FALLBACK-C13 가동")
                recovered = await self._fallback_evening_analysis(context)
                if recovered:
                    c13_ok = True

        # ── Stage 3: C14~C23 병렬 (300s) ──
        logger.info("[COO] G7 Stage 3: C14~C23 병렬")
        stage3_jobs = []

        # C14: 클로징 브리프
        if self.bot:
            stage3_jobs.append((
                "C14_closing_brief",
                self.bot._send_daily_closing(context),
            ))

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

        # C17: 국적 차트
        if self.bot:
            stage3_jobs.append((
                "C17_nationality_charts",
                self.bot._job_nationality_charts(context),
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

        # C21: ETF 투자자별 수급 분석 (TIER2 Phase 2)
        if self.bot:
            stage3_jobs.append((
                "C21_etf_flow",
                self.bot._job_etf_flow(context),
            ))

        # C22: FLOWX 퀀트 대시보드 통합 업로드 (5개 테이블)
        stage3_jobs.append((
            "C22_quant_dashboard",
            self._job_quant_dashboard_upload(context),
        ))

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

        if stage3_jobs:
            s3 = await self.run_parallel_async(stage3_jobs, timeout_per_job=300)
            results.extend(s3)

        # ── 그룹 상태 업데이트 ──
        self.update_group("G7", results)

        logger.info("[COO] ═══ G7 EVENING_BRAIN 완료 ═══")
        return results

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
    async def _job_quant_dashboard_upload(self, context=None) -> dict:
        """5개 퀀트 대시보드 테이블 → Supabase 업로드"""
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
            import asyncio
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

    async def _job_macro_baseline(self, context=None) -> dict:
        """C5M: 매크로 기준선 수집 (yfinance 6지표 20MA/60MA).

        G6 Step 4에서 병렬 실행.
        macro_baseline.json에 캐시 → inflation_chain/fx_sector/bond_yield의
        2-Layer 판단 활성화.
        """
        try:
            import asyncio
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
    # C23: TRIX 다이버전스 사전 스캔
    # ─────────────────────────────────────────────
    async def _job_trix_prescan(self, context=None) -> dict:
        """전체 유니버스 TRIX 다이버전스 스캔 → 캐시 저장.

        G7 Stage 3에서 C16(MACD)과 병렬 실행.
        다음날 모닝추천 Step 2.6에서 캐시(3시간 TTL) 즉시 재사용.
        """
        try:
            import asyncio
            from strategies.trix_divergence import (
                scan_trix_divergence, save_trix_cache,
            )

            signals = await asyncio.to_thread(scan_trix_divergence, 10)
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
            import asyncio
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

    async def _job_nxt_early_collect(self, context=None) -> dict:
        """C4E: NXT 사전 데이터 수집 + 예비 알림 발송.

        G6에서 C3(일봉수집 90분)과 독립 병렬로 실행.
        stages 1~4를 캐시에 저장하여 16:35 run_nightwatch() 가속.
        NQ 데이터 수집 성공 시 즉시 예비 알림 텔레그램 발송.
        """
        try:
            from data.nightwatch import collect_nxt_early_data, format_nxt_pre_alert
            import asyncio

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
                timeout=600,
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
        now_kst = datetime.now(timezone(timedelta(hours=9)))
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
