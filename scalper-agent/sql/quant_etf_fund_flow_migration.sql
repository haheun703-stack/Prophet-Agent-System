-- =============================================
-- quant_etf_fund_flow: ETF 투자자별 수급 분석
-- 스케줄: G7 Stage3 (~16:45)
-- 설명: 21개 ETF 기관·외인·개인 수급 + 시장방향 + 안전자산 판단
-- =============================================

CREATE TABLE IF NOT EXISTS quant_etf_fund_flow (
    date                DATE NOT NULL,
    etfs                JSONB NOT NULL DEFAULT '[]',
    -- etfs 배열 항목: {
    --   code: TEXT,             -- ETF 종목코드
    --   name: TEXT,             -- ETF명
    --   alias: TEXT,            -- 주린이용 (코스피 추종)
    --   group: TEXT,            -- directional/commodity/sector
    --   inst_1d: REAL,          -- 기관 당일(억)
    --   inst_3d: REAL,
    --   inst_consecutive: INT,
    --   foreign_1d: REAL,
    --   foreign_3d: REAL,
    --   foreign_consecutive: INT,
    --   retail_1d: REAL,
    --   signal: TEXT,           -- 기관매수/기관매도/외인매수/중립
    --   signal_desc: TEXT,      -- 주린이용
    --   strength: INT,          -- 0~100
    --   change_pct: REAL        -- 등락률(%)
    -- }
    market_direction    TEXT DEFAULT 'NEUTRAL',  -- BULLISH/BEARISH/NEUTRAL
    market_direction_desc TEXT DEFAULT '',        -- 주린이용 설명
    inverse_warning     BOOLEAN DEFAULT FALSE,   -- 인버스 기관매수 경고
    hot_sector_etfs     JSONB DEFAULT '[]',      -- 기관 집중 섹터 ETF명
    safe_haven_signal   TEXT DEFAULT 'NEUTRAL',  -- RISK_ON/RISK_OFF/NEUTRAL
    safe_haven_desc     TEXT DEFAULT '',
    brain_defense_score REAL DEFAULT 0,          -- -10~+10
    created_at          TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (date)
);

CREATE INDEX IF NOT EXISTS idx_quant_etf_fund_flow_date
    ON quant_etf_fund_flow(date DESC);

ALTER TABLE quant_etf_fund_flow ENABLE ROW LEVEL SECURITY;

DO $$ BEGIN
    CREATE POLICY "Public read" ON quant_etf_fund_flow
        FOR SELECT USING (true);
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

DO $$ BEGIN
    CREATE POLICY "Service write" ON quant_etf_fund_flow
        FOR ALL USING (true) WITH CHECK (true);
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

SELECT 'quant_etf_fund_flow' AS tbl, count(*) FROM quant_etf_fund_flow;
