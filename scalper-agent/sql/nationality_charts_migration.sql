-- ═══════════════════════════════════════════════════════════
-- nationality_charts
-- ═══════════════════════════════════════════════════════════
-- 외국인 국적별 수급 픽토그램 차트 (Supabase Storage 이미지 URL + 메타데이터)
-- FLOWX 웹 대시보드의 "국적별 수급 X-ray" 패널에 표시됨.
--
-- 복합 PK (date, code) — 같은 날 같은 종목은 1개만
-- nationality_pictogram.py → upload_chart_to_supabase()에서 upsert
-- ═══════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS nationality_charts (
    date            DATE NOT NULL,
    code            TEXT NOT NULL,
    name            TEXT NOT NULL,
    image_url       TEXT NOT NULL,
    nat_score       NUMERIC(6,2) DEFAULT 0,
    nat_grade       TEXT DEFAULT '',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (date, code)
);

CREATE INDEX IF NOT EXISTS idx_nationality_charts_date
    ON nationality_charts (date DESC);

CREATE INDEX IF NOT EXISTS idx_nationality_charts_code
    ON nationality_charts (code, date DESC);

-- ═══════════════════════════════════════════════════════════
-- RLS (Row Level Security)
-- ═══════════════════════════════════════════════════════════
ALTER TABLE nationality_charts ENABLE ROW LEVEL SECURITY;

-- anon/authenticated 읽기 허용 (웹 대시보드)
DROP POLICY IF EXISTS "read_nationality_charts" ON nationality_charts;
CREATE POLICY "read_nationality_charts"
    ON nationality_charts
    FOR SELECT
    TO anon, authenticated
    USING (true);

-- service_role upsert 허용 (봇 업로드)
DROP POLICY IF EXISTS "upsert_nationality_charts" ON nationality_charts;
CREATE POLICY "upsert_nationality_charts"
    ON nationality_charts
    FOR ALL
    TO service_role
    USING (true)
    WITH CHECK (true);

-- ═══════════════════════════════════════════════════════════
-- Storage Bucket (수동 생성 필요 시 참고)
-- ═══════════════════════════════════════════════════════════
-- INSERT INTO storage.buckets (id, name, public)
-- VALUES ('nationality-charts', 'nationality-charts', true)
-- ON CONFLICT DO NOTHING;

-- ═══════════════════════════════════════════════════════════
-- Comment
-- ═══════════════════════════════════════════════════════════
COMMENT ON TABLE nationality_charts IS
    '외국인 국적별 수급 픽토그램 차트. nationality_pictogram.py에서 일일 자동 생성+업로드. FLOWX 웹 대시보드 국적 X-ray 패널.';
COMMENT ON COLUMN nationality_charts.code IS '종목코드 (6자리)';
COMMENT ON COLUMN nationality_charts.image_url IS 'Supabase Storage public URL (nationality-charts/{date}/{code}.png)';
COMMENT ON COLUMN nationality_charts.nat_score IS '국적 수급 점수 (nationality_signal.py 기반)';
COMMENT ON COLUMN nationality_charts.nat_grade IS '국적 수급 등급/사유';
