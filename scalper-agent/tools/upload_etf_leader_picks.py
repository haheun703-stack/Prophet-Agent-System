# -*- coding: utf-8 -*-
"""ETF Step C-5 — leader_stocks_<date>.json → Supabase scalper_etf_leader_picks 적재.

흐름:
  1) 테이블 스키마 검증 (check_schema)
     - 없으면 마이그레이션 안내 + exit (또는 --create-table 플래그 시 자동 생성)
  2) DELETE WHERE base_date = ? (idempotent — 재실행 안전)
  3) INSERT 일괄 (TOP 30)
  4) 검증: COUNT(*) WHERE base_date = ? → 30 확인

사용:
  python tools/upload_etf_leader_picks.py 20260515
  python tools/upload_etf_leader_picks.py 20260515 --create-table  # 1회성 DDL
"""
import sys
import io
import json
import logging
from datetime import datetime
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT / "scalper-agent"))

from utils.supabase_sql import (  # noqa: E402
    query,
    query_one,
    execute,
    check_schema,
    get_conn,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("UploadETFLeader")

DATA_DIR = ROOT / "scalper-agent" / "data_store" / "etf_backtest"
SQL_PATH = ROOT / "scalper-agent" / "sql" / "scalper_etf_leader_picks_migration.sql"

REQUIRED_COLUMNS = [
    "base_date", "stock_ticker", "rank", "stock_name", "leader_score",
    "etf_count", "top_weight_pct", "top_etf_ticker", "top_etf_name",
    "top_etf_score", "included_etfs", "created_at", "updated_at",
]


def parse_base_date(s: str) -> str:
    """YYYYMMDD → YYYY-MM-DD (Postgres DATE 형식)."""
    if len(s) != 8 or not s.isdigit():
        raise ValueError(f"base_date 형식 오류 (YYYYMMDD): {s}")
    return f"{s[:4]}-{s[4:6]}-{s[6:]}"


def load_leader_stocks(date_str: str) -> dict:
    path = DATA_DIR / f"leader_stocks_{date_str}.json"
    if not path.exists():
        raise FileNotFoundError(
            f"leader_stocks 파일 없음: {path}\n"
            f"먼저 'python tools/collect_etf_holdings.py {date_str}' 실행 필요."
        )
    return json.loads(path.read_text(encoding="utf-8"))


def create_table_via_raw_conn():
    """1회성 DDL 직접 실행 — 가드 우회 (마이그레이션 트랙 명시 의도).

    supabase_sql.execute는 DDL을 차단하므로 raw cursor 사용.
    """
    if not SQL_PATH.exists():
        raise FileNotFoundError(f"DDL 파일 없음: {SQL_PATH}")
    ddl = SQL_PATH.read_text(encoding="utf-8")
    logger.info(f"DDL 실행 중 (사장님 명시 의도, --create-table)...")
    with get_conn(readonly=False) as conn:
        with conn.cursor() as cur:
            cur.execute(ddl)
    logger.info("DDL 실행 완료. 스키마 재검증 중...")


def upload(date_str: str, create_table: bool = False):
    base_date_iso = parse_base_date(date_str)
    print(f"\n{'='*70}\n  ETF Step C-5 — Supabase 적재 (base_date={base_date_iso})\n{'='*70}\n")

    # 1) 스키마 검증
    schema = check_schema("scalper_etf_leader_picks", REQUIRED_COLUMNS)
    if not schema["table_exists"]:
        if create_table:
            create_table_via_raw_conn()
            schema = check_schema("scalper_etf_leader_picks", REQUIRED_COLUMNS)
            if not schema["ok"]:
                logger.error(f"DDL 후에도 스키마 미스매치: missing={schema['missing']}")
                sys.exit(1)
        else:
            print(
                f"  ⚠  테이블 'scalper_etf_leader_picks'가 없습니다.\n\n"
                f"  방법 A) Supabase 대시보드 → SQL Editor에서 실행:\n"
                f"     파일: {SQL_PATH}\n\n"
                f"  방법 B) --create-table 플래그로 자동 생성:\n"
                f"     python tools/upload_etf_leader_picks.py {date_str} --create-table\n"
            )
            sys.exit(2)
    elif not schema["ok"]:
        logger.error(f"스키마 컬럼 누락: {schema['missing']}")
        sys.exit(1)
    else:
        logger.info(f"스키마 OK ({len(schema['found'])}개 컬럼)")

    # 2) leader_stocks 로드
    data = load_leader_stocks(date_str)
    top_stocks = data.get("top_stocks", [])
    if not top_stocks:
        logger.error("top_stocks가 비어있음")
        sys.exit(1)
    logger.info(f"leader_stocks 로드: {len(top_stocks)}개 종목")

    # 3) 멱등 처리 — 기존 base_date 삭제
    deleted = execute(
        "DELETE FROM scalper_etf_leader_picks WHERE base_date = %s",
        (base_date_iso,),
    )
    if deleted > 0:
        logger.info(f"기존 {base_date_iso} 데이터 {deleted}행 삭제 (재실행 멱등)")

    # 4) 일괄 INSERT
    inserted = 0
    for i, s in enumerate(top_stocks, 1):
        # included_etfs에서 top_etf 정보 추출
        included = s.get("included_etfs", [])
        top_etf = max(included, key=lambda e: e.get("weight_pct", 0), default={})

        execute(
            """
            INSERT INTO scalper_etf_leader_picks (
                base_date, stock_ticker, rank, stock_name, leader_score,
                etf_count, top_weight_pct, top_etf_ticker, top_etf_name,
                top_etf_score, included_etfs
            ) VALUES (
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s::jsonb
            )
            """,
            (
                base_date_iso,
                s["stock_ticker"],
                i,
                s["stock_name"],
                s["leader_score"],
                s["etf_count"],
                s["top_weight"],
                top_etf.get("etf_ticker"),
                top_etf.get("etf_name", s.get("top_etf_name")),
                top_etf.get("etf_score", s.get("top_etf_score")),
                json.dumps(included, ensure_ascii=False),
            ),
        )
        inserted += 1

    logger.info(f"INSERT 완료: {inserted}행 적재")

    # 5) 검증
    cnt = query_one(
        "SELECT COUNT(*) AS n FROM scalper_etf_leader_picks WHERE base_date = %s",
        (base_date_iso,),
    )
    db_count = cnt["n"] if cnt else 0
    print(f"\n  ✓ 적재 검증: DB {db_count}행 (예상 {inserted}행) — {'PASS' if db_count == inserted else 'FAIL'}")

    # TOP 5 미리보기
    rows = query(
        """
        SELECT rank, stock_ticker, stock_name, leader_score, etf_count, top_etf_name
        FROM scalper_etf_leader_picks
        WHERE base_date = %s
        ORDER BY rank
        LIMIT 5
        """,
        (base_date_iso,),
    )
    print(f"\n  [TOP 5 미리보기]")
    for r in rows:
        print(
            f"    {r['rank']:>2}. [{r['stock_ticker']}] {r['stock_name']:<20s} "
            f"점수 {r['leader_score']:>6.2f} ETF {r['etf_count']}개  ({r['top_etf_name']})"
        )

    return db_count == inserted


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python tools/upload_etf_leader_picks.py YYYYMMDD [--create-table]")
        sys.exit(1)
    date_str = sys.argv[1]
    create_table = "--create-table" in sys.argv
    ok = upload(date_str, create_table=create_table)
    sys.exit(0 if ok else 1)
