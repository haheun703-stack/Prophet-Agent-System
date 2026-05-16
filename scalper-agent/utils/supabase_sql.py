# -*- coding: utf-8 -*-
"""Supabase SQL 직접 연결 클라이언트 (psycopg2 기반).

정보봇 5/17 발행 가이드 v1에 따른 표준 구현.
4시스템(정보봇/단타봇/퀀트봇/웹봇) 공통 DATABASE_URL 공유 패턴.

사용법:
    from utils.supabase_sql import query, query_one, get_conn

    # 1) 다건 조회 (RealDictCursor → dict 리스트)
    rows = query("SELECT * FROM daily_limit_up_history WHERE date = CURRENT_DATE LIMIT 5")
    for r in rows:
        print(r["ticker"], r["name"])

    # 2) 단건 조회
    row = query_one("SELECT MAX(date) AS max_date FROM daily_limit_up_history")

    # 3) 직접 컨텍스트 사용 (트랜잭션/INSERT)
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO scalper_picks (...) VALUES (...)")

협업 룰:
  - 정보봇 60+ 테이블 → 단타봇은 read-only
  - 단타봇 자체 데이터 → scalper_* prefix만 사용
  - 다른 봇 테이블 INSERT/UPDATE 절대 금지

보안:
  - DATABASE_URL은 .env에서만 로드, 절대 git 커밋 금지
  - Pool max=3 (단타봇 보수적 설정, Free tier 60 conn 안전 마진)
"""
import os
import logging
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
_POOL_MIN = 1
_POOL_MAX = 3  # 단타봇 보수적 설정 (장중 동시 호출 대비)


def _get_database_url() -> str:
    """DATABASE_URL을 .env에서 로드 (검증 포함)."""
    url = os.getenv("DATABASE_URL", "").strip()
    if not url:
        raise RuntimeError(
            "DATABASE_URL 미설정. .env에 다음 형식으로 추가하세요:\n"
            "  DATABASE_URL=postgresql://postgres:[PASSWORD]@db.<project_ref>.supabase.co:5432/postgres\n"
            "정보봇 .env의 DATABASE_URL을 그대로 복사하면 됩니다."
        )
    if not url.startswith("postgresql://") and not url.startswith("postgres://"):
        raise RuntimeError(f"DATABASE_URL 형식 오류 (postgresql:// 시작 필요): {url[:30]}...")
    return url


def _init_pool() -> pool.SimpleConnectionPool:
    """Pool 지연 초기화 (첫 호출 시)."""
    global _POOL
    if _POOL is None:
        _POOL = pool.SimpleConnectionPool(
            _POOL_MIN,
            _POOL_MAX,
            dsn=_get_database_url(),
            cursor_factory=RealDictCursor,
            connect_timeout=10,
        )
        logger.info(f"Supabase pool 초기화 완료 (min={_POOL_MIN}, max={_POOL_MAX})")
    return _POOL


@contextmanager
def get_conn():
    """Pool에서 연결 획득 → 컨텍스트 끝나면 자동 반환."""
    pool_inst = _init_pool()
    conn = pool_inst.getconn()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        pool_inst.putconn(conn)


def query(sql: str, params: Optional[tuple] = None) -> List[Dict[str, Any]]:
    """SELECT 다건 조회 → dict 리스트 반환."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return [dict(r) for r in cur.fetchall()]


def query_one(sql: str, params: Optional[tuple] = None) -> Optional[Dict[str, Any]]:
    """SELECT 단건 조회 → dict 또는 None."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            row = cur.fetchone()
            return dict(row) if row else None


def execute(sql: str, params: Optional[tuple] = None) -> int:
    """INSERT/UPDATE/DELETE 실행 → 영향받은 행 수.

    단타봇 자체 테이블(scalper_*)에만 사용 — 정보봇 테이블 절대 변경 금지.
    """
    # 안전 가드: scalper_* prefix 검증
    sql_upper = sql.strip().upper()
    if sql_upper.startswith(("INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "TRUNCATE")):
        # 테이블 이름 추출 시도 (간단 휴리스틱)
        target = ""
        if "INTO" in sql_upper:
            target = sql_upper.split("INTO")[1].strip().split()[0]
        elif "UPDATE" in sql_upper:
            target = sql_upper.split("UPDATE")[1].strip().split()[0]
        elif "FROM" in sql_upper and "DELETE" in sql_upper:
            target = sql_upper.split("FROM")[1].strip().split()[0]

        target_lower = target.lower().strip('"').strip("'")
        if target_lower and not target_lower.startswith(("scalper_", "public.scalper_")):
            raise PermissionError(
                f"단타봇은 scalper_* prefix 테이블만 변경 가능. "
                f"감지된 대상: {target_lower} (협업 룰 §5 위반)"
            )

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.rowcount


def ping() -> Dict[str, Any]:
    """연결 헬스체크."""
    try:
        row = query_one("SELECT version() AS version, current_timestamp AS now")
        return {"ok": True, "version": row["version"][:60], "now": str(row["now"])}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def close_pool():
    """Pool 종료 (프로세스 종료 시 호출)."""
    global _POOL
    if _POOL is not None:
        _POOL.closeall()
        _POOL = None
        logger.info("Supabase pool 종료")


# ── 직접 실행: 헬스체크 ──
if __name__ == "__main__":
    import sys, io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
    logging.basicConfig(level=logging.INFO)

    print("\n=== Supabase 연결 헬스체크 ===")
    result = ping()
    if result["ok"]:
        print(f"  ✓ 연결 성공")
        print(f"  버전: {result['version']}")
        print(f"  시각: {result['now']}")

        # 정보봇 테이블 샘플 조회
        try:
            count = query_one("SELECT COUNT(*) AS n FROM daily_limit_up_history WHERE date >= CURRENT_DATE - INTERVAL '7 days'")
            print(f"  ✓ 정보봇 daily_limit_up_history 7일 데이터: {count['n']}건")
        except Exception as e:
            print(f"  ⚠ 정보봇 테이블 조회 실패: {e}")

    else:
        print(f"  ✗ 연결 실패: {result['error']}")
        sys.exit(1)
