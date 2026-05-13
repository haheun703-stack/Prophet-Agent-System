-- =============================================
-- quant_theme_flow: 테마별 기관/외국인 수급 흐름
-- 스케줄: G7 Stage3 (~16:45)
-- 설명: KIS 302개 테마 기준 기관·외국인 순매수 집계
--       기존 quant_sector_flow(23개 KRX 섹터)의 세분화 버전
-- =============================================

CREATE TABLE IF NOT EXISTS quant_theme_flow (
    date            DATE NOT NULL,
    themes          JSONB NOT NULL DEFAULT '[]',
    -- themes 배열 항목: {
    --   테마: TEXT,               -- 테마명 (예: MLCC, 2차전지, HBM)
    --   종목수: INT,              -- flow 데이터 있는 종목수
    --   테마전체: INT,            -- 테마 전체 종목수
    --   기관_당일: REAL,          -- 기관 당일 순매수(억)
    --   기관_3일: REAL,           -- 기관 3일 합계
    --   기관_5일: REAL,           -- 기관 5일 합계
    --   기관_연속일: INT,         -- 기관 연속 순매수일
    --   외인_당일: REAL,          -- 외인 당일
    --   외인_3일: REAL,           -- 외인 3일
    --   외인_5일: REAL,           -- 외인 5일
    --   외인_연속일: INT,
    --   판단: TEXT,               -- 합의매수/합의매도/의견분열/기관매집/외인매집/중립
    --   설명: TEXT,               -- 주린이용 설명
    --   보정점수: REAL            -- 추천 보정 점수 (-15 ~ +15)
    -- }
    top_inflow      JSONB DEFAULT '[]',     -- 자금 유입 TOP5 테마명
    top_outflow     JSONB DEFAULT '[]',     -- 자금 이탈 TOP5 테마명
    signal          TEXT DEFAULT '',         -- 한줄 요약
    total_themes    INT DEFAULT 0,
    created_at      TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (date)
);

CREATE INDEX IF NOT EXISTS idx_quant_theme_flow_date
    ON quant_theme_flow(date DESC);

ALTER TABLE quant_theme_flow ENABLE ROW LEVEL SECURITY;

DO $$ BEGIN
    CREATE POLICY "Public read" ON quant_theme_flow
        FOR SELECT USING (true);
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

DO $$ BEGIN
    CREATE POLICY "Service write" ON quant_theme_flow
        FOR ALL USING (true) WITH CHECK (true);
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

SELECT 'quant_theme_flow' AS tbl, count(*) FROM quant_theme_flow;
