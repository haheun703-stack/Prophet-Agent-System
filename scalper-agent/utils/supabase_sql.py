# -*- coding: utf-8 -*-
"""Supabase SQL 직접 연결 클라이언트 (psycopg2 기반) v1.1.

정보봇 5/17 발행 가이드 v1에 따른 표준 구현.
4시스템(정보봇/단타봇/퀀트봇/웹봇) 공통 DATABASE_URL 공유 패턴.

사용법:
    from utils.supabase_sql import query, query_one, get_conn

    # 1) 다건 조회 (RealDictCursor → dict 리스트)
    rows = query("SELECT * FROM daily_limit_up_history WHERE date = CURRENT_DATE LIMIT 5")

    # 2) 단건 조회
    row = query_one("SELECT MAX(date) AS max_date FROM daily_limit_up_history")

    # 3) 직접 컨텍스트 사용 (INSERT, scalper_* prefix만)
    execute("INSERT INTO scalper_picks (ticker, score) VALUES (%s, %s)", (t, s))

협업 룰 (DB role 분리 전 단기 가드):
  - 정보봇 60+ 테이블 → 단타봇은 read-only (SELECT만)
  - 단타봇 자체 데이터 → scalper_* prefix만 사용
  - 다른 봇 테이블 INSERT/UPDATE/DELETE 절대 금지

⚠️ 중요: 정공법은 Supabase DB role 분리(scalper_readonly + scalper_writer).
   이 가드는 단기 안전망일 뿐 — AUDIT_BACKLOG.md §1 TODO 참조.

보안:
  - DATABASE_URL은 .env에서만 로드, 절대 git 커밋 금지
  - Pool max=3 (단타봇 보수적 설정, Free tier 60 conn 안전 마진)
  - statement_timeout 5초 (장중 Pool 정지 사고 방지)
  - threading.Lock으로 race condition 차단
  - SQL 주석/CTE/multi-statement 차단 (C-1 단기 가드)

v1.1 변경 (5/17 EVALUATOR FAIL → 수정):
  - C-1: SQL 가드 강화 (WITH/--/* /;/대소문자 정규화)
  - H-2: statement_timeout 5000ms
  - H-3: ping() SELECT 1로 단순화 (DB 버전 노출 제거)
  - H-4: threading.Lock으로 Pool 초기화 race 차단
  - L-1: atexit으로 close_pool 자동 등록
  - M-4: 에러 메시지 URL 노출 15자로 축소
"""
import atexit
import logging
import os
import re
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, List, Optional

import psycopg2
from psycopg2 import pool
from psycopg2.extras import RealDictCursor

logger = logging.getLogger(__name__)

# ── .env 자동 로드 ────────────────────────────────────
_ENV_PATH = Path(__file__).resolve().parent.parent.parent / ".env"
if _ENV_PATH.exists():
    from dotenv import load_dotenv
    load_dotenv(_ENV_PATH)

# ── Pool 설정 ─────────────────────────────────────────
_POOL: Optional[pool.SimpleConnectionPool] = None
_POOL_LOCK = threading.Lock()
_POOL_MIN = 1
_POOL_MAX = 3                    # 단타봇 보수적 (장중 동시 호출 대비)
_CONNECT_TIMEOUT_SEC = 10
_STATEMENT_TIMEOUT_MS = 5000     # 5초 (장중 Pool 정지 방지)

# ── SQL 가드 패턴 (C-1) ──────────────────────────────
# 위험 패턴: 주석, CTE 시작, 세미콜론(multi-statement), 대소문자 우회
_DANGEROUS_PREFIX = re.compile(r"^(\s|--|/\*|\bwith\b)", re.IGNORECASE)
_MULTI_STATEMENT = re.compile(r";\s*\S")  # ; 뒤에 비공백 (multi-statement)
_WRITE_OPS = ("INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "TRUNCATE",
              "GRANT", "REVOKE", "CREATE", "RENAME", "COPY")


def _get_database_url() -> str:
    """DATABASE_URL을 .env에서 로드 (검증 포함)."""
    url = os.getenv("DATABASE_URL", "").strip()
    if not url:
        raise RuntimeError(
            "DATABASE_URL 미설정. .env에 다음 형식으로 추가하세요:\n"
            "  DATABASE_URL=postgresql://postgres:[PASSWORD]@db.<project_ref>.supabase.co:5432/postgres\n"
            "정보봇 .env의 DATABASE_URL을 그대로 복사하면 됩니다."
        )
    if not url.startswith(("postgresql://", "postgres://")):
        raise RuntimeError(f"DATABASE_URL 형식 오류 (postgresql:// 시작 필요): {url[:15]}...")
    if url.startswith("postgres://"):
        logger.warning("postgres:// scheme deprecated — postgresql://로 자동 치환")
        url = "postgresql://" + url[len("postgres://"):]
    return url


def _init_pool() -> pool.SimpleConnectionPool:
    """Pool 지연 초기화 (lock + double-check, 멀티스레드 안전)."""
    global _POOL
    if _POOL is None:
        with _POOL_LOCK:
            if _POOL is None:  # double-check
                _POOL = pool.SimpleConnectionPool(
                    _POOL_MIN,
                    _POOL_MAX,
                    dsn=_get_database_url(),
                    cursor_factory=RealDictCursor,
                    connect_timeout=_CONNECT_TIMEOUT_SEC,
                    options=f"-c statement_timeout={_STATEMENT_TIMEOUT_MS}",
                )
                logger.info(
                    f"Supabase pool 초기화 완료 (min={_POOL_MIN}, max={_POOL_MAX}, "
                    f"timeout={_STATEMENT_TIMEOUT_MS}ms)"
                )
    return _POOL


@contextmanager
def get_conn(readonly: bool = False):
    """Pool에서 연결 획득 → 컨텍스트 끝나면 자동 반환.

    Args:
        readonly: True면 commit 생략 (SELECT 전용 — 트랜잭션 오버헤드 감소)

    중요: 이 함수가 반환한 conn에 `with conn:` 사용 금지 (psycopg2 commit 중복).
          오직 `with conn.cursor() as cur:` 만 사용.
    """
    pool_inst = _init_pool()
    conn = pool_inst.getconn()
    try:
        yield conn
        if not readonly:
            conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        pool_inst.putconn(conn)


def _check_sql_safety(sql: str) -> None:
    """SQL 가드 — C-1 우회 패턴 차단 (단기 안전망).

    차단 패턴:
      - SQL 주석 prefix (-- /*)
      - CTE 시작 (WITH ... UPDATE/INSERT/DELETE)
      - Multi-statement (; 뒤 비공백)
      - 정보봇 테이블 직접 변경

    Raises:
        PermissionError: 위반 시
    """
    if not sql or not sql.strip():
        return

    stripped = sql.strip()

    # 1) Multi-statement 차단 (마지막 ; 제외)
    cleaned = stripped.rstrip(";").rstrip()
    if _MULTI_STATEMENT.search(cleaned):
        raise PermissionError(
            "Multi-statement 금지 — 세미콜론으로 여러 쿼리 분리 시도 차단"
        )

    # 2) 위험 prefix 차단 (주석/CTE)
    if _DANGEROUS_PREFIX.match(stripped):
        # 단, "WITH RECURSIVE ... SELECT" 같은 read-only CTE는 허용
        if stripped.lower().startswith("with"):
            # CTE 내부에 write op 존재 여부 검증
            sql_upper = stripped.upper()
            for op in _WRITE_OPS:
                if re.search(rf"\b{op}\b", sql_upper):
                    raise PermissionError(
                        f"CTE 내부에서 쓰기 작업({op}) 시도 차단 — "
                        f"DB role 분리 필요 (AUDIT_BACKLOG §1 C-2)"
                    )
            return  # read-only CTE는 OK
        raise PermissionError(
            "SQL 주석 prefix 금지 — 가드 우회 시도 차단"
        )


def _check_write_target(sql: str) -> None:
    """쓰기 작업의 대상 테이블이 scalper_* prefix인지 검증.

    예외:
      - SELECT는 검증 안 함 (read-only)
      - scalper_*, public.scalper_* 만 허용
    """
    sql_upper = sql.strip().upper()
    op_match = None
    for op in _WRITE_OPS:
        if sql_upper.startswith(op + " ") or sql_upper.startswith(op + "\t"):
            op_match = op
            break

    if not op_match:
        return  # SELECT 등 read-only

    # 테이블 이름 추출
    target = None
    if op_match in ("INSERT",):
        m = re.search(r"INTO\s+([^\s\(]+)", sql_upper)
        target = m.group(1) if m else None
    elif op_match in ("UPDATE",):
        m = re.search(r"UPDATE\s+([^\s]+)", sql_upper)
        target = m.group(1) if m else None
    elif op_match in ("DELETE",):
        m = re.search(r"FROM\s+([^\s]+)", sql_upper)
        target = m.group(1) if m else None
    elif op_match in ("TRUNCATE", "DROP", "ALTER", "RENAME"):
        # DDL은 보수적으로 모두 차단
        raise PermissionError(
            f"{op_match} 금지 — 스키마 변경은 마이그레이션 트랙으로만 수행"
        )

    if not target:
        raise PermissionError(f"{op_match} 대상 테이블 파악 불가 — 차단")

    target_lower = target.lower().strip('"').strip("'").lstrip("public.")
    if not target_lower.startswith("scalper_"):
        raise PermissionError(
            f"단타봇은 scalper_* prefix 테이블만 변경 가능. "
            f"감지된 대상: {target_lower} (협업 룰 §5 위반)"
        )


def query(sql: str, params: Optional[tuple] = None) -> List[Dict[str, Any]]:
    """SELECT 다건 조회 → dict 리스트 반환."""
    _check_sql_safety(sql)
    with get_conn(readonly=True) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return [dict(r) for r in cur.fetchall()]


def query_one(sql: str, params: Optional[tuple] = None) -> Optional[Dict[str, Any]]:
    """SELECT 단건 조회 → dict 또는 None."""
    _check_sql_safety(sql)
    with get_conn(readonly=True) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            row = cur.fetchone()
            return dict(row) if row else None


def execute(sql: str, params: Optional[tuple] = None) -> int:
    """INSERT/UPDATE/DELETE 실행 → 영향받은 행 수.

    단타봇 자체 테이블(scalper_*)에만 사용 — 정보봇 테이블 절대 변경 금지.
    """
    _check_sql_safety(sql)
    _check_write_target(sql)

    with get_conn(readonly=False) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.rowcount


def ping() -> Dict[str, Any]:
    """연결 헬스체크 — SELECT 1만 (DB 버전 노출 제거 H-3)."""
    try:
        row = query_one("SELECT 1 AS ok")
        return {"ok": True, "result": row.get("ok") == 1 if row else False}
    except Exception as e:
        return {"ok": False, "error": str(e)[:200]}  # 에러 메시지도 200자 제한


def check_schema(table: str, required_columns: List[str]) -> Dict[str, Any]:
    """테이블 스키마 검증 (M-2 — 정보봇 변경 시 silent fail 방지).

    Args:
        table: 테이블 이름 (예: 'daily_limit_up_history')
        required_columns: 필수 컬럼 리스트

    Returns:
        {"ok": bool, "missing": list, "found": list}
    """
    rows = query(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = %s
        """,
        (table,),
    )
    found = {r["column_name"] for r in rows}
    missing = [c for c in required_columns if c not in found]
    return {
        "ok": len(missing) == 0,
        "missing": missing,
        "found": sorted(found),
        "table_exists": len(found) > 0,
    }


def close_pool():
    """Pool 종료 (atexit 자동 등록)."""
    global _POOL
    if _POOL is not None:
        try:
            _POOL.closeall()
        except Exception as e:
            logger.warning(f"Pool 종료 중 오류 (무시): {e}")
        _POOL = None
        logger.info("Supabase pool 종료")


# ── 프로세스 종료 시 자동 정리 (L-1) ──
atexit.register(close_pool)


# ── 직접 실행: 헬스체크 ──
if __name__ == "__main__":
    import sys, io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
    logging.basicConfig(level=logging.INFO)

    print("\n=== Supabase 연결 헬스체크 v1.1 ===")
    result = ping()
    if result["ok"]:
        print(f"  ✓ 연결 성공 (result={result.get('result')})")

        # 정보봇 테이블 샘플 조회
        try:
            count = query_one(
                "SELECT COUNT(*) AS n FROM daily_limit_up_history "
                "WHERE date >= CURRENT_DATE - INTERVAL '7 days'"
            )
            print(f"  ✓ 정보봇 daily_limit_up_history 7일 데이터: {count['n']}건")
        except Exception as e:
            print(f"  ⚠ 정보봇 테이블 조회 실패: {e}")

        # 스키마 검증
        schema = check_schema(
            "daily_limit_up_history",
            ["ticker", "name", "date", "ma5_distance_pct", "vol_ratio",
             "days_since_break", "is_active"],
        )
        if schema["ok"]:
            print(f"  ✓ 스키마 검증 통과 ({len(schema['found'])}개 컬럼)")
        else:
            print(f"  ✗ 스키마 미스매치 — 누락 컬럼: {schema['missing']}")

        # 가드 자체 테스트
        print(f"\n  [가드 자체 테스트]")
        guard_tests = [
            ("정상 SELECT", "SELECT 1", False),
            ("정상 scalper_* INSERT", "INSERT INTO scalper_picks (a) VALUES (1)", False),
            ("정보봇 UPDATE 차단", "UPDATE daily_limit_up_history SET x=1 WHERE 1=1", True),
            ("주석 우회 차단", "/* */ UPDATE daily_limit_up_history SET x=1", True),
            ("CTE 우회 차단", "WITH y AS (SELECT 1) UPDATE daily_limit_up_history SET x=1", True),
            ("Multi-statement 차단", "INSERT INTO scalper_picks VALUES (1); DROP TABLE daily_limit_up_history", True),
            ("DDL 차단", "TRUNCATE TABLE scalper_picks", True),
        ]
        for desc, sql, should_block in guard_tests:
            try:
                _check_sql_safety(sql)
                _check_write_target(sql)
                blocked = False
            except PermissionError as e:
                blocked = True
            status = "✓" if (blocked == should_block) else "✗"
            mark = "차단됨" if blocked else "통과"
            expected = "차단 예상" if should_block else "통과 예상"
            print(f"    {status} {desc}: {mark} ({expected})")

    else:
        print(f"  ✗ 연결 실패: {result['error']}")
        sys.exit(1)
