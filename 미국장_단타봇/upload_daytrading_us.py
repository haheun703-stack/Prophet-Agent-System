"""
============================================
FLOWX 단타봇 — 미국장 분석 결과 Supabase 업로드
파일명: upload_daytrading_us.py
============================================

단타봇의 미국장 야간 분석 결과를 Supabase에 저장.
FLOWX 대시보드 → "오늘의 모드" 패널에 표시됨.

테이블: daytrading_us_overnight
============================================
"""

import os
import json
import logging
from datetime import date

from supabase import create_client
from dotenv import load_dotenv
from us_overnight_filter import run as run_filter

load_dotenv()
log = logging.getLogger("upload_daytrading_us")

supabase = create_client(
    os.environ["SUPABASE_URL"],
    os.environ["SUPABASE_KEY"]
)

# ── Supabase 테이블 SQL (최초 1회 실행) ─────────────────────
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS daytrading_us_overnight (
  id            BIGSERIAL PRIMARY KEY,
  date          DATE NOT NULL UNIQUE,
  mode          TEXT,           -- AGGRESSIVE/NORMAL/DEFENSIVE/HALT
  gap_signal    TEXT,           -- GAP_UP/FLAT/GAP_DOWN
  gap_est_pct   NUMERIC,
  soxx_alert    BOOLEAN,
  risk_level    INT,            -- 1~5
  risk_score    INT,
  watch_sectors JSONB DEFAULT '[]',
  avoid_sectors JSONB DEFAULT '[]',
  relay_picks   JSONB DEFAULT '[]',
  reasons_bad   JSONB DEFAULT '[]',
  reasons_good  JSONB DEFAULT '[]',
  reason        TEXT,
  nasdaq_change NUMERIC,
  soxx_change   NUMERIC,
  vix           NUMERIC,
  dxy           NUMERIC,
  us_3y_yield   NUMERIC,
  fear_greed    INT,
  fear_greed_label TEXT,
  kr_impact     TEXT,
  risk_flags    JSONB DEFAULT '[]',
  created_at    TIMESTAMPTZ DEFAULT NOW()
);

-- RLS
ALTER TABLE daytrading_us_overnight ENABLE ROW LEVEL SECURITY;
CREATE POLICY "public read"  ON daytrading_us_overnight FOR SELECT USING (true);
CREATE POLICY "service write" ON daytrading_us_overnight FOR ALL USING (auth.role() = 'service_role');
"""


def upload(report: dict) -> bool:
    """분석 결과 업로드."""
    if not report:
        log.error("빈 리포트 — 업로드 스킵")
        return False
    try:
        row = {
            "date":          report.get("date", date.today().isoformat()),
            "mode":          report.get("mode"),
            "gap_signal":    report.get("gap_signal"),
            "gap_est_pct":   report.get("gap_est_pct"),
            "soxx_alert":    report.get("soxx_alert"),
            "risk_level":    report.get("risk_level"),
            "risk_score":    report.get("risk_score"),
            "watch_sectors": report.get("watch_sectors") or [],
            "avoid_sectors": report.get("avoid_sectors") or [],
            "relay_picks":   report.get("relay_picks")   or [],
            "reasons_bad":   report.get("reasons_bad")   or [],
            "reasons_good":  report.get("reasons_good")  or [],
            "reason":        report.get("reason"),
            "nasdaq_change": report.get("nasdaq_change"),
            "soxx_change":   report.get("soxx_change"),
            "vix":           report.get("vix"),
            "dxy":           report.get("dxy"),
            "us_3y_yield":   report.get("us_3y_yield"),
            "fear_greed":    report.get("fear_greed"),
            "fear_greed_label": report.get("fear_greed_label"),
            "kr_impact":     report.get("kr_impact"),
            "risk_flags":    report.get("risk_flags")    or [],
        }
        supabase.table("daytrading_us_overnight") \
            .upsert(row, on_conflict="date") \
            .execute()
        log.info(f"✅ 업로드 완료: {row['date']} | 모드: {row['mode']}")
        return True
    except Exception as e:
        log.error(f"업로드 실패: {e}")
        return False


def main():
    log.info("단타봇 미국장 분석 + 업로드 시작")
    report = run_filter()
    if report:
        upload(report)


if __name__ == "__main__":
    main()
