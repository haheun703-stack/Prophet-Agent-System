-- ============================================
-- scoreboard: FLOWX 시그널 성과 스코어보드
-- 생성일: 2026-04-22
-- 출처: flowx_signals.py aggregate_scoreboard()
-- 목적: 30D/60D/90D/ALL 기간별 봇 성과 집계
-- PK: (bot_type, period)
-- ============================================
CREATE TABLE IF NOT EXISTS scoreboard (
    bot_type        TEXT NOT NULL,              -- 'QUANT', 'DAYTRADING', 'ALL'
    period          TEXT NOT NULL,              -- '30D', '60D', '90D', 'ALL'
    total_signals   INT DEFAULT 0,
    win_count       INT DEFAULT 0,
    lose_count      INT DEFAULT 0,
    win_rate        REAL DEFAULT 0,             -- 승률 (%)
    avg_return_pct  REAL DEFAULT 0,             -- 평균 수익률 (%)
    avg_win_pct     REAL DEFAULT 0,             -- 평균 익절 수익률 (%)
    avg_lose_pct    REAL DEFAULT 0,             -- 평균 손절 수익률 (%)
    best_signal     JSONB,                      -- {ticker, name, return_pct, date}
    worst_signal    JSONB,                      -- {ticker, name, return_pct, date}
    calculated_at   TEXT,                       -- ISO datetime
    created_at      TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (bot_type, period)
);

-- RLS (PK가 bot_type 선행이므로 별도 인덱스 불필요)
ALTER TABLE scoreboard ENABLE ROW LEVEL SECURITY;
DO $$ BEGIN
    CREATE POLICY "Public read" ON scoreboard
        FOR SELECT USING (true);
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;
DO $$ BEGIN
    CREATE POLICY "Service write" ON scoreboard
        FOR ALL USING (true) WITH CHECK (true);
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

-- 검증
SELECT 'scoreboard' AS tbl, count(*) AS rows FROM scoreboard;
