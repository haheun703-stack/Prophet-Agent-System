-- ============================================================
-- intelligence_foreign_flow : 외국인 돈 흐름 추적
-- 생성일: 2026-04-09
-- 출처: foreign_accumulation_scanner v1 확장
-- 목적: SK텔레콤 패턴 / 외국인이 지금 담고 있는 종목 실시간 추적
-- ============================================================

-- ─────────────────────────────────────────────────────────
-- 1) 종목별 외국인/기관 수급 (5일 롤링)
-- ─────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS intelligence_foreign_flow (
    date DATE NOT NULL,
    code TEXT NOT NULL,
    name TEXT NOT NULL,
    sector TEXT,
    mcap_억 BIGINT,

    -- 당일 (4/9 기준)
    foreign_today_억 REAL,
    inst_today_억 REAL,
    close INTEGER,

    -- 5일 누적
    foreign_5d_억 REAL,
    inst_5d_억 REAL,
    foreign_buy_days INTEGER,      -- 5일 중 외인 매수일수
    inst_buy_days INTEGER,          -- 5일 중 기관 매수일수

    -- 플래그
    is_dual_buy BOOLEAN DEFAULT false,  -- 외인+기관 쌍매수 (3일+)
    is_quiet_accum BOOLEAN DEFAULT false, -- 조용한 축적 (SK텔레콤 패턴)

    -- 랭킹
    rank_single INTEGER,  -- 당일 외인 순매수 순위
    rank_cumul INTEGER,   -- 5일 누적 외인 순위
    rank_dual INTEGER,    -- 쌍매수 순위

    -- 가격 변동
    price_change_5d_pct REAL,  -- 5일 주가 변동률

    created_at TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (date, code)
);

-- 인덱스
CREATE INDEX IF NOT EXISTS idx_foreign_flow_date
    ON intelligence_foreign_flow(date DESC);
CREATE INDEX IF NOT EXISTS idx_foreign_flow_rank_single
    ON intelligence_foreign_flow(date DESC, rank_single ASC);
CREATE INDEX IF NOT EXISTS idx_foreign_flow_rank_cumul
    ON intelligence_foreign_flow(date DESC, rank_cumul ASC);
CREATE INDEX IF NOT EXISTS idx_foreign_flow_rank_dual
    ON intelligence_foreign_flow(date DESC, rank_dual ASC);
CREATE INDEX IF NOT EXISTS idx_foreign_flow_sector
    ON intelligence_foreign_flow(date DESC, sector);

-- RLS
ALTER TABLE intelligence_foreign_flow ENABLE ROW LEVEL SECURITY;
DO $$ BEGIN
    CREATE POLICY "Public read" ON intelligence_foreign_flow
        FOR SELECT USING (true);
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;
DO $$ BEGIN
    CREATE POLICY "Service write" ON intelligence_foreign_flow
        FOR ALL USING (true) WITH CHECK (true);
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;


-- ─────────────────────────────────────────────────────────
-- 2) 섹터별 외국인/기관 돈 흐름 랭킹
-- ─────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS intelligence_foreign_flow_sector (
    date DATE NOT NULL,
    sector TEXT NOT NULL,

    foreign_5d_억 REAL,
    inst_5d_억 REAL,
    stock_count INTEGER,           -- 해당 섹터에서 매수 감지된 종목수
    rank INTEGER,                  -- 섹터 순위

    top_stocks JSONB,              -- [{"name":"SK하이닉스","foreign_5d":15144,"inst_5d":11825}, ...]

    created_at TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (date, sector)
);

CREATE INDEX IF NOT EXISTS idx_foreign_flow_sector_date
    ON intelligence_foreign_flow_sector(date DESC, rank ASC);

ALTER TABLE intelligence_foreign_flow_sector ENABLE ROW LEVEL SECURITY;
DO $$ BEGIN
    CREATE POLICY "Public read" ON intelligence_foreign_flow_sector
        FOR SELECT USING (true);
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;
DO $$ BEGIN
    CREATE POLICY "Service write" ON intelligence_foreign_flow_sector
        FOR ALL USING (true) WITH CHECK (true);
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;


-- ─────────────────────────────────────────────────────────
-- 검증 쿼리
-- ─────────────────────────────────────────────────────────
SELECT 'intelligence_foreign_flow' AS tbl, count(*) AS rows FROM intelligence_foreign_flow;
SELECT 'intelligence_foreign_flow_sector' AS tbl, count(*) AS rows FROM intelligence_foreign_flow_sector;
