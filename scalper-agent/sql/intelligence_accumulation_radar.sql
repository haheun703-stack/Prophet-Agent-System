-- intelligence_accumulation_radar: 매집 레이더 (C36)
-- 외인 3일+ 매집 중이나 아직 안 오른 "미발화" 종목

CREATE TABLE IF NOT EXISTS intelligence_accumulation_radar (
    date DATE PRIMARY KEY,
    stocks JSONB NOT NULL DEFAULT '[]'::jsonb
);

ALTER TABLE intelligence_accumulation_radar ENABLE ROW LEVEL SECURITY;

CREATE POLICY "radar_public_read"
    ON intelligence_accumulation_radar FOR SELECT USING (true);

CREATE POLICY "radar_service_write"
    ON intelligence_accumulation_radar FOR ALL USING (auth.role() = 'service_role');
