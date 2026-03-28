-- =============================================
-- quant_sector_flow: 섹터별 기관/외국인 수급 흐름
-- 스케줄: G7 Stage3 (~16:45)
-- 설명: 23개 섹터 기관·외국인 순매수 + 연속매수일 + 합의매수 판단
-- =============================================

CREATE TABLE IF NOT EXISTS quant_sector_flow (
    date            DATE NOT NULL,
    sectors         JSONB NOT NULL DEFAULT '[]',
    -- sectors 배열 항목: {
    --   sector: TEXT,           -- 섹터 원본명
    --   alias: TEXT,            -- 주린이용 (반도체/전자)
    --   inst_1d: REAL,          -- 기관 당일 순매수(억)
    --   inst_3d: REAL,          -- 기관 3일 합계
    --   inst_5d: REAL,          -- 기관 5일 합계
    --   inst_consecutive: INT,  -- 기관 연속 순매수일
    --   foreign_1d: REAL,       -- 외인 당일
    --   foreign_3d: REAL,       -- 외인 3일
    --   foreign_5d: REAL,       -- 외인 5일
    --   foreign_consecutive: INT,
    --   agreement: TEXT,        -- 합의매수/합의매도/의견분열/중립
    --   agreement_desc: TEXT,   -- 주린이용 설명
    --   boost_score: REAL       -- 추천 보정 점수
    -- }
    top_inflow      JSONB DEFAULT '[]',     -- 매수 집중 TOP3 섹터명
    top_outflow     JSONB DEFAULT '[]',     -- 이탈 TOP3 섹터명
    signal          TEXT DEFAULT '',         -- 한줄 요약
    total_sectors   INT DEFAULT 0,
    created_at      TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (date)
);

CREATE INDEX IF NOT EXISTS idx_quant_sector_flow_date
    ON quant_sector_flow(date DESC);

ALTER TABLE quant_sector_flow ENABLE ROW LEVEL SECURITY;

DO $$ BEGIN
    CREATE POLICY "Public read" ON quant_sector_flow
        FOR SELECT USING (true);
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

DO $$ BEGIN
    CREATE POLICY "Service write" ON quant_sector_flow
        FOR ALL USING (true) WITH CHECK (true);
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

SELECT 'quant_sector_flow' AS tbl, count(*) FROM quant_sector_flow;
