-- intelligence_daytrading_performance: 단타 TOP 5 일일 성적표
-- 매일 G7(16:30) 발행 — 시가→종가 수익률 + 주간/월간 누적
CREATE TABLE IF NOT EXISTS intelligence_daytrading_performance (
    date        DATE PRIMARY KEY,
    avg_return  REAL,                    -- 당일 평균 수익률(%)
    best_pick   TEXT,                    -- 최고 수익 종목
    worst_pick  TEXT,                    -- 최저 수익 종목
    weekly_return  REAL DEFAULT 0,       -- 주간 누적 수익률(%)
    weekly_days    INT DEFAULT 0,
    weekly_wins    INT DEFAULT 0,
    monthly_return REAL DEFAULT 0,       -- 월간 누적 수익률(%)
    monthly_days   INT DEFAULT 0,
    monthly_wins   INT DEFAULT 0,
    items       JSONB,                   -- [{rank, code, name, score, open, close, return_pct, ...}]
    created_at  TIMESTAMPTZ DEFAULT now()
);

-- RLS
ALTER TABLE intelligence_daytrading_performance ENABLE ROW LEVEL SECURITY;

CREATE POLICY "daytrading_perf_public_read"
    ON intelligence_daytrading_performance
    FOR SELECT
    USING (true);

CREATE POLICY "daytrading_perf_service_write"
    ON intelligence_daytrading_performance
    FOR ALL
    USING (auth.role() = 'service_role');
