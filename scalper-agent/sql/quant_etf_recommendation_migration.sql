-- =============================================
-- quant_etf_recommendation: ETF 추천 (방향성/원자재/섹터)
-- 스케줄: G7 Stage3 (~16:45)
-- 설명: 최대 3개 ETF 추천 (카테고리별 1개) + ATR 기반 진입/SL/TP
-- =============================================

CREATE TABLE IF NOT EXISTS quant_etf_recommendation (
    date            DATE NOT NULL,
    picks           JSONB NOT NULL DEFAULT '[]',
    -- picks 배열 항목: {
    --   code: TEXT,             -- ETF 코드
    --   name: TEXT,             -- ETF명
    --   category: TEXT,         -- directional/commodity/sector
    --   etf_type: TEXT,         -- index/leverage/inverse/commodity/sector
    --   signal: TEXT,           -- BUY/SELL/HOLD
    --   confidence: TEXT,       -- HIGH/MEDIUM/LOW
    --   score: REAL,            -- 0~100
    --   entry: INT,             -- 진입가
    --   sl: INT,                -- 손절가
    --   tp: INT,                -- 목표가
    --   risk_pct: REAL,         -- SL 손실률(%)
    --   reason: TEXT,           -- 추천 사유
    --   holding_days: INT       -- 최대 보유일
    -- }
    pick_count      INT DEFAULT 0,
    has_directional BOOLEAN DEFAULT FALSE,
    has_commodity   BOOLEAN DEFAULT FALSE,
    has_sector      BOOLEAN DEFAULT FALSE,
    created_at      TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (date)
);

CREATE INDEX IF NOT EXISTS idx_quant_etf_recommendation_date
    ON quant_etf_recommendation(date DESC);

ALTER TABLE quant_etf_recommendation ENABLE ROW LEVEL SECURITY;

DO $$ BEGIN
    CREATE POLICY "Public read" ON quant_etf_recommendation
        FOR SELECT USING (true);
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

DO $$ BEGIN
    CREATE POLICY "Service write" ON quant_etf_recommendation
        FOR ALL USING (true) WITH CHECK (true);
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

SELECT 'quant_etf_recommendation' AS tbl, count(*) FROM quant_etf_recommendation;
