-- intelligence_flow_intensity: 수급 강도 TOP 종목 (FLOWX 스윙시스템 패널)
-- 매일 C35 이후 생성, 시총 대비 수급 유입 비율로 랭킹
CREATE TABLE IF NOT EXISTS intelligence_flow_intensity (
    date        DATE NOT NULL,
    updated_at  TIMESTAMPTZ DEFAULT now(),
    -- 메타
    total_scanned   INT,          -- 스캔 대상 종목 수
    -- TOP 종목 리스트 (JSONB 배열)
    -- 각 항목: {rank, code, name, market_type, intensity_pct,
    --           foreign_3d_억, inst_3d_억, total_3d_억, cap_억,
    --           close, pct_5d, dual_buy, consec_foreign, consec_inst,
    --           is_overheated}
    top_stocks  JSONB NOT NULL DEFAULT '[]',
    -- 요약 통계
    dual_buy_count  INT DEFAULT 0,
    overheat_count  INT DEFAULT 0,
    PRIMARY KEY (date)
);

-- RLS
ALTER TABLE intelligence_flow_intensity ENABLE ROW LEVEL SECURITY;
CREATE POLICY IF NOT EXISTS "flow_intensity_read"
    ON intelligence_flow_intensity FOR SELECT USING (true);
CREATE POLICY IF NOT EXISTS "flow_intensity_write"
    ON intelligence_flow_intensity FOR ALL USING (true);
