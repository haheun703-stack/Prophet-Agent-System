-- intelligence_ewy_holdings: EWY 보유종목 변동 추적
-- iShares MSCI South Korea ETF (EWY) 보유종목 비중 변화 → 패시브 외인 자금 흐름 선행 감지

CREATE TABLE IF NOT EXISTS intelligence_ewy_holdings (
    date DATE PRIMARY KEY,
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    as_of TEXT,                          -- iShares 기준일 (CSV 내 날짜)
    total_stocks INT DEFAULT 0,          -- 보유 종목 수
    top20 JSONB NOT NULL DEFAULT '[]'::jsonb,    -- 상위 20종목 배열
    changes JSONB NOT NULL DEFAULT '[]'::jsonb,  -- 전일 대비 변동 배열
    new_entries JSONB NOT NULL DEFAULT '[]'::jsonb,  -- 신규 편입 종목
    removed JSONB NOT NULL DEFAULT '[]'::jsonb,      -- 편출 종목
    summary TEXT DEFAULT ''              -- 요약 텍스트 (FLOWX 표시용)
);

ALTER TABLE intelligence_ewy_holdings ENABLE ROW LEVEL SECURITY;

CREATE POLICY "ewy_public_read"
    ON intelligence_ewy_holdings FOR SELECT USING (true);

CREATE POLICY "ewy_service_write"
    ON intelligence_ewy_holdings FOR ALL USING (auth.role() = 'service_role');
