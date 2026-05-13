-- =============================================
-- quant_theme_momentum: 테마별 모멘텀 (HOT/COLD)
-- 스케줄: G7 Stage3 (~16:45)
-- 설명: KIS 302개 테마 기준 HOT/COLD 모멘텀 분석
--       기존 quant_sector_momentum(23개 KRX 섹터)의 세분화 버전
-- =============================================

CREATE TABLE IF NOT EXISTS quant_theme_momentum (
    date             DATE NOT NULL,
    market_return_1d REAL DEFAULT 0,         -- 시장 전체 당일 수익률(%)
    themes           JSONB NOT NULL DEFAULT '[]',
    -- themes 배열 항목: {
    --   테마: TEXT,               -- 테마명 (예: MLCC, 2차전지, HBM)
    --   상태: TEXT,               -- HOT/WARMING/NEUTRAL/COOLING/COLD
    --   순위: INT,                -- 전체 테마 중 순위 (1=최강)
    --   종목수: INT,              -- 매칭 종목수
    --   당일수익률: REAL,         -- 테마 평균 당일 수익률(%)
    --   3일수익률: REAL,          -- 테마 평균 3일 수익률(%)
    --   5일수익률: REAL,          -- 테마 평균 5일 수익률(%)
    --   상승비율: REAL,           -- 오늘 상승 종목 비율 (0~1)
    --   가속도: REAL,             -- 1D - 5D일평균 (급변 감지)
    --   부스트: REAL,             -- 추천 엔진용 부스트 점수
    --   주도주: JSONB             -- [{code, name, chg_1d}] TOP 3
    -- }
    hot_themes       JSONB DEFAULT '[]',     -- HOT/WARMING 테마명 목록
    cold_themes      JSONB DEFAULT '[]',     -- COLD/COOLING 테마명 목록
    rotation_signal  TEXT DEFAULT '',         -- 테마 로테이션 요약
    total_themes     INT DEFAULT 0,
    created_at       TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (date)
);

CREATE INDEX IF NOT EXISTS idx_quant_theme_momentum_date
    ON quant_theme_momentum(date DESC);

ALTER TABLE quant_theme_momentum ENABLE ROW LEVEL SECURITY;

DO $$ BEGIN
    CREATE POLICY "Public read" ON quant_theme_momentum
        FOR SELECT USING (true);
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

DO $$ BEGIN
    CREATE POLICY "Service write" ON quant_theme_momentum
        FOR ALL USING (true) WITH CHECK (true);
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

SELECT 'quant_theme_momentum' AS tbl, count(*) FROM quant_theme_momentum;
