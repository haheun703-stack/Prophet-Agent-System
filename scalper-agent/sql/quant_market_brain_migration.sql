-- =============================================
-- quant_market_brain: BRAIN 시장판단 (7.5Phase 종합)
-- 스케줄: G7 Stage3 (~16:45)
-- 설명: 매크로·원자재·섹터·수급·리스크 6단계 판단 + 투자비중
-- =============================================

CREATE TABLE IF NOT EXISTS quant_market_brain (
    date                DATE NOT NULL,
    -- 종합 판단
    overall_verdict     TEXT DEFAULT '',          -- 오늘의 판단 (한글 서술)
    position_size_pct   INT DEFAULT 70,           -- 투자비중 (0~100%)
    position_size_reason TEXT DEFAULT '',          -- 비중 이유
    -- Phase 1: 매크로
    macro_direction     TEXT DEFAULT 'NEUTRAL',   -- STRONG_BULL~STRONG_BEAR
    macro_narrative     TEXT DEFAULT '',           -- 주린이용 매크로 설명
    vix                 REAL DEFAULT 0,
    nasdaq_chg          REAL DEFAULT 0,            -- NASDAQ 변동률(%)
    usdkrw              REAL DEFAULT 0,
    usdkrw_chg          REAL DEFAULT 0,
    gold_chg            REAL DEFAULT 0,
    -- Phase 2: 원자재
    commodity_relay     TEXT DEFAULT 'NONE',       -- 릴레이 단계
    commodity_narrative TEXT DEFAULT '',
    -- Phase 3: 섹터
    hot_sectors         JSONB DEFAULT '[]',
    next_sectors        JSONB DEFAULT '[]',
    cooling_sectors     JSONB DEFAULT '[]',
    sector_narrative    TEXT DEFAULT '',
    -- Phase 4: 수급
    dominant_buyer      TEXT DEFAULT '',            -- 기관/외인/개인
    flow_narrative      TEXT DEFAULT '',
    -- Phase 5: 리스크
    risk_level          TEXT DEFAULT 'LOW',         -- LOW/MEDIUM/HIGH/EXTREME
    risk_score          REAL DEFAULT 0,
    risk_narrative      TEXT DEFAULT '',
    -- Phase 6: 종목 서술
    stock_narratives    JSONB DEFAULT '[]',
    -- stock_narratives: [{
    --   code, name, total_score, grade,
    --   why_narrative, risk_flag, macro_alignment
    -- }]
    created_at          TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (date)
);

CREATE INDEX IF NOT EXISTS idx_quant_market_brain_date
    ON quant_market_brain(date DESC);

ALTER TABLE quant_market_brain ENABLE ROW LEVEL SECURITY;

DO $$ BEGIN
    CREATE POLICY "Public read" ON quant_market_brain
        FOR SELECT USING (true);
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

DO $$ BEGIN
    CREATE POLICY "Service write" ON quant_market_brain
        FOR ALL USING (true) WITH CHECK (true);
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

SELECT 'quant_market_brain' AS tbl, count(*) FROM quant_market_brain;
