-- 수급 임계점 스캐너 (Supply Tipping Point Scanner)
-- 6자 투자자 수급 통합 분석 → "한 번만 들어오면 오르는" 코일 종목 감지

CREATE TABLE IF NOT EXISTS intelligence_tipping_scan (
    date DATE PRIMARY KEY,
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    total_scanned INTEGER DEFAULT 0,
    coiled_count INTEGER DEFAULT 0,
    warming_count INTEGER DEFAULT 0,
    launched_count INTEGER DEFAULT 0,
    coiled JSONB DEFAULT '[]'::jsonb,
    warming JSONB DEFAULT '[]'::jsonb,
    launched JSONB DEFAULT '[]'::jsonb
);

-- RLS 정책
ALTER TABLE intelligence_tipping_scan ENABLE ROW LEVEL SECURITY;
CREATE POLICY "Allow public read" ON intelligence_tipping_scan
    FOR SELECT USING (true);

COMMENT ON TABLE intelligence_tipping_scan IS '수급 임계점 스캐너 — 6자 수급 통합 분석으로 코일 상태 종목 감지';
COMMENT ON COLUMN intelligence_tipping_scan.coiled IS '코일 상태 (가격 ±5%, 스프링 장전) 최대 30건';
COMMENT ON COLUMN intelligence_tipping_scan.warming IS '점화 상태 (+5~10% 초기 움직임) 최대 15건';
COMMENT ON COLUMN intelligence_tipping_scan.launched IS '이륙 상태 (+10~15% 진행 중) 최대 10건';
