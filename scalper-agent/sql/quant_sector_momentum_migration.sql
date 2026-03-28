-- =============================================
-- quant_sector_momentum: 섹터 모멘텀 (HOT/WARMING/COLD)
-- 스케줄: G7 Stage3 (~16:45)
-- 설명: 23개 섹터 수익률·가속도·상승비율 + 시장 전체 수익률
-- =============================================

CREATE TABLE IF NOT EXISTS quant_sector_momentum (
    date                DATE NOT NULL,
    market_return_1d    REAL DEFAULT 0,          -- 시장 전체 1일 수익률(%)
    sectors             JSONB NOT NULL DEFAULT '[]',
    -- sectors 배열 항목: {
    --   sector: TEXT,           -- 섹터명
    --   phase: TEXT,            -- HOT/WARMING/NEUTRAL/COOLING/COLD
    --   rank: INT,              -- 순위 (1=최강)
    --   avg_return_1d: REAL,    -- 당일 평균 수익률(%)
    --   avg_return_3d: REAL,    -- 3일
    --   avg_return_5d: REAL,    -- 5일
    --   breadth_1d: REAL,       -- 상승 종목 비율 (0~1)
    --   acceleration: REAL,     -- 가속도 (급변 감지)
    --   volume_surge: REAL,     -- 거래량 폭증 비율
    --   boost_score: REAL,      -- 추천 부스트 점수
    --   top_movers: JSONB       -- [{code, name, chg_1d}]
    -- }
    hot_sectors         JSONB DEFAULT '[]',      -- HOT 섹터명 목록
    cold_sectors        JSONB DEFAULT '[]',      -- COLD 섹터명 목록
    rotation_signal     TEXT DEFAULT '',          -- 로테이션 요약
    created_at          TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (date)
);

CREATE INDEX IF NOT EXISTS idx_quant_sector_momentum_date
    ON quant_sector_momentum(date DESC);

ALTER TABLE quant_sector_momentum ENABLE ROW LEVEL SECURITY;

DO $$ BEGIN
    CREATE POLICY "Public read" ON quant_sector_momentum
        FOR SELECT USING (true);
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

DO $$ BEGIN
    CREATE POLICY "Service write" ON quant_sector_momentum
        FOR ALL USING (true) WITH CHECK (true);
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

SELECT 'quant_sector_momentum' AS tbl, count(*) FROM quant_sector_momentum;
