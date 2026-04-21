# -*- coding: utf-8 -*-
"""
투자자 수급 DB 조회 래퍼 (investor_daily.db)
=============================================
퀀트봇 DB(원 단위, net_val 컬럼)를 단타봇 컨벤션(억원, net_buy)으로 변환.

DB 위치: data_store/investor_daily.db (퀀트봇 symlink)
스키마:  date TEXT, ticker TEXT, name TEXT, investor TEXT,
         sell_vol INT, buy_vol INT, net_vol INT,
         sell_val INT, buy_val INT, net_val INT  (원 단위)

사용법:
    from data.investor_db import InvestorDB
    db = InvestorDB()
    # 삼성전자 연기금 최근 20일
    rows = db.get_flow("005930", "연기금", days=20)
    # 4/20 연기금 순매수 TOP10
    top = db.get_top("연기금", "20260420", top_n=10)
"""

import logging
import os
import sqlite3
from pathlib import Path
from typing import Optional

logger = logging.getLogger("BH.InvestorDB")

DB_PATH = Path(__file__).parent.parent / "data_store" / "investor_daily.db"

# 투자자 유형 상수
INVESTOR_PENSION = "연기금"
INVESTOR_INSTITUTION = "기관합계"
INVESTOR_FOREIGN = "외국인"
INVESTOR_INDIVIDUAL = "개인"
INVESTOR_CORP = "기타법인"

ALL_INVESTORS = [
    INVESTOR_PENSION, INVESTOR_INSTITUTION, INVESTOR_FOREIGN,
    INVESTOR_INDIVIDUAL, INVESTOR_CORP,
]


class InvestorDB:
    """investor_daily.db 조회 래퍼 — 모든 금액 억원 반환."""

    def __init__(self, db_path: Optional[str] = None):
        self._db_path = str(db_path or DB_PATH)
        if not os.path.exists(self._db_path):
            logger.warning(f"investor_daily.db 없음: {self._db_path}")

    def _conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._db_path)
        conn.row_factory = sqlite3.Row
        return conn

    # ─── 단건 조회 ───────────────────────────────────────────

    def get_net_buy(self, ticker: str, investor: str, date: str) -> float:
        """특정 종목/투자자/날짜의 순매수액 (억원). 없으면 0."""
        try:
            conn = self._conn()
            cur = conn.execute(
                "SELECT net_val FROM investor_daily "
                "WHERE ticker=? AND investor=? AND date=?",
                (ticker, investor, date),
            )
            row = cur.fetchone()
            conn.close()
            return round(row["net_val"] / 1e8, 2) if row else 0.0
        except Exception as e:
            logger.error(f"get_net_buy 실패: {e}")
            return 0.0

    # ─── 시계열 조회 ─────────────────────────────────────────

    def get_flow(
        self, ticker: str, investor: str, days: int = 30
    ) -> list[dict]:
        """특정 종목/투자자의 최근 N거래일 수급.

        Returns:
            [{"date": "20260420", "net_buy": -181.5, "buy_val": 300.2, "sell_val": 481.7}, ...]
            금액 단위: 억원, 최신일 먼저
        """
        try:
            conn = self._conn()
            cur = conn.execute(
                "SELECT date, buy_val, sell_val, net_val FROM investor_daily "
                "WHERE ticker=? AND investor=? "
                "ORDER BY date DESC LIMIT ?",
                (ticker, investor, days),
            )
            rows = [
                {
                    "date": r["date"],
                    "net_buy": round(r["net_val"] / 1e8, 2),
                    "buy_val": round(r["buy_val"] / 1e8, 2),
                    "sell_val": round(r["sell_val"] / 1e8, 2),
                }
                for r in cur.fetchall()
            ]
            conn.close()
            return rows
        except Exception as e:
            logger.error(f"get_flow 실패 ({ticker}/{investor}): {e}")
            return []

    def get_all_investors(self, ticker: str, date: str) -> dict[str, float]:
        """특정 종목/날짜의 전 투자자 순매수 (억원).

        Returns:
            {"연기금": -181.5, "외국인": 50.2, "기관합계": -30.1, ...}
        """
        try:
            conn = self._conn()
            cur = conn.execute(
                "SELECT investor, net_val FROM investor_daily "
                "WHERE ticker=? AND date=?",
                (ticker, date),
            )
            result = {r["investor"]: round(r["net_val"] / 1e8, 2) for r in cur.fetchall()}
            conn.close()
            return result
        except Exception as e:
            logger.error(f"get_all_investors 실패: {e}")
            return {}

    # ─── 순위 조회 ───────────────────────────────────────────

    def get_top(
        self, investor: str, date: str, top_n: int = 10, ascending: bool = False
    ) -> list[dict]:
        """특정 투자자/날짜의 순매수(또는 순매도) 상위 종목.

        Args:
            ascending: True면 순매도 상위 (net_buy 작은 순)

        Returns:
            [{"ticker": "005930", "name": "삼성전자", "net_buy": -181.5}, ...]
        """
        order = "ASC" if ascending else "DESC"
        try:
            conn = self._conn()
            cur = conn.execute(
                f"SELECT ticker, name, net_val FROM investor_daily "
                f"WHERE investor=? AND date=? "
                f"ORDER BY net_val {order} LIMIT ?",
                (investor, date, top_n),
            )
            rows = [
                {
                    "ticker": r["ticker"],
                    "name": r["name"],
                    "net_buy": round(r["net_val"] / 1e8, 2),
                }
                for r in cur.fetchall()
            ]
            conn.close()
            return rows
        except Exception as e:
            logger.error(f"get_top 실패: {e}")
            return []

    # ─── 연속 매수/매도 분석 ─────────────────────────────────

    def get_consecutive_buy_days(
        self, ticker: str, investor: str, lookback: int = 20
    ) -> int:
        """최근 연속 순매수 일수 (0이면 매도 전환)."""
        rows = self.get_flow(ticker, investor, days=lookback)
        count = 0
        for r in rows:  # 최신일부터
            if r["net_buy"] > 0:
                count += 1
            else:
                break
        return count

    # ─── 유틸 ────────────────────────────────────────────────

    def get_latest_date(self, investor: Optional[str] = None) -> str:
        """DB에 저장된 최신 거래일."""
        try:
            conn = self._conn()
            if investor:
                cur = conn.execute(
                    "SELECT MAX(date) FROM investor_daily WHERE investor=?",
                    (investor,),
                )
            else:
                cur = conn.execute("SELECT MAX(date) FROM investor_daily")
            row = cur.fetchone()
            conn.close()
            return row[0] if row else ""
        except Exception as e:
            logger.error(f"get_latest_date 실패: {e}")
            return ""

    def get_available_dates(self, investor: str, limit: int = 10) -> list[str]:
        """해당 투자자의 최근 N거래일 목록."""
        try:
            conn = self._conn()
            cur = conn.execute(
                "SELECT DISTINCT date FROM investor_daily "
                "WHERE investor=? ORDER BY date DESC LIMIT ?",
                (investor, limit),
            )
            dates = [r["date"] for r in cur.fetchall()]
            conn.close()
            return dates
        except Exception as e:
            logger.error(f"get_available_dates 실패: {e}")
            return []

    def is_available(self) -> bool:
        """DB 파일 존재 + 테이블 접근 가능 여부."""
        if not os.path.exists(self._db_path):
            return False
        try:
            conn = self._conn()
            conn.execute("SELECT 1 FROM investor_daily LIMIT 1")
            conn.close()
            return True
        except Exception:
            return False
