-- intelligence_stealth_scan: 선매집 탐지 일일 결과
-- FLOWX 웹 대시보드 "선매집 탐지" 패널 데이터

CREATE TABLE IF NOT EXISTS intelligence_stealth_scan (
    date          DATE PRIMARY KEY,
    total_scanned INTEGER DEFAULT 0,
    stealth_count INTEGER DEFAULT 0,
    moving_count  INTEGER DEFAULT 0,
    surged_count  INTEGER DEFAULT 0,
    stealth_items JSONB DEFAULT '[]'::jsonb,
    moving_items  JSONB DEFAULT '[]'::jsonb,
    surged_items  JSONB DEFAULT '[]'::jsonb,
    top_stealth_names JSONB DEFAULT '[]'::jsonb,
    created_at    TIMESTAMPTZ DEFAULT now()
);

-- RLS 설정
ALTER TABLE intelligence_stealth_scan ENABLE ROW LEVEL SECURITY;

CREATE POLICY "선매집 공개읽기"
    ON intelligence_stealth_scan FOR SELECT
    USING (true);

CREATE POLICY "선매집 서비스쓰기"
    ON intelligence_stealth_scan FOR ALL
    USING (auth.role() = 'service_role');

-- 인덱스
CREATE INDEX IF NOT EXISTS idx_stealth_scan_date
    ON intelligence_stealth_scan (date DESC);

COMMENT ON TABLE intelligence_stealth_scan IS '기관/외인 선매집 탐지 일일 결과 (잠복/움직임/이미상승)';
