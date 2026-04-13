-- ═══════════════════════════════════════════════════════════
-- vip_content
-- ═══════════════════════════════════════════════════════════
-- FLOWX VIP 8-Panel 콘텐츠 (일일 1행)
-- flowx_content.py → upload_vip_content()에서 upsert
-- 패널 구성:
--   1. 종목 리스트    2. 수급 분석    3. 추천 사유
--   4. 국적별 수급    5. 매집 감지    6. 릴레이 맵
--   7. 적중률         8. EXIT 알림
-- ═══════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS vip_content (
    date                DATE NOT NULL PRIMARY KEY,
    generated_at        TEXT,
    panel_1_stocks      JSONB DEFAULT '[]'::jsonb,
    panel_2_supply      JSONB DEFAULT '{}'::jsonb,
    panel_3_reasons     JSONB DEFAULT '[]'::jsonb,
    panel_4_nationality JSONB DEFAULT '[]'::jsonb,
    panel_5_accumulation JSONB DEFAULT '[]'::jsonb,
    panel_6_relay       JSONB DEFAULT '[]'::jsonb,
    panel_7_accuracy    JSONB DEFAULT '{}'::jsonb,
    panel_8_alerts      JSONB DEFAULT '[]'::jsonb,
    summary             JSONB DEFAULT '{}'::jsonb,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_vip_content_date
    ON vip_content (date DESC);

-- ═══════════════════════════════════════════════════════════
-- RLS (Row Level Security)
-- ═══════════════════════════════════════════════════════════
ALTER TABLE vip_content ENABLE ROW LEVEL SECURITY;

-- anon/authenticated 읽기 허용 (웹 대시보드)
DROP POLICY IF EXISTS "read_vip_content" ON vip_content;
CREATE POLICY "read_vip_content"
    ON vip_content
    FOR SELECT
    TO anon, authenticated
    USING (true);

-- service_role upsert 허용 (봇 업로드)
DROP POLICY IF EXISTS "upsert_vip_content" ON vip_content;
CREATE POLICY "upsert_vip_content"
    ON vip_content
    FOR ALL
    TO service_role
    USING (true)
    WITH CHECK (true);

-- ═══════════════════════════════════════════════════════════
-- Comment
-- ═══════════════════════════════════════════════════════════
COMMENT ON TABLE vip_content IS
    'FLOWX VIP 8-Panel 콘텐츠. flowx_content.py에서 일일 자동 생성+업로드. 날짜별 1행.';
COMMENT ON COLUMN vip_content.panel_1_stocks IS '추천 종목 리스트 (code, name, score, grade, theme 등)';
COMMENT ON COLUMN vip_content.panel_2_supply IS '수급 분석 (기관/외인 매수/매도 상위)';
COMMENT ON COLUMN vip_content.panel_3_reasons IS '종목별 추천 사유 (AI분석 근거)';
COMMENT ON COLUMN vip_content.panel_4_nationality IS '외국인 국적별 수급 요약';
COMMENT ON COLUMN vip_content.panel_5_accumulation IS '매집 감지 종목 (거래대금 폭발/조용한 매집)';
COMMENT ON COLUMN vip_content.panel_6_relay IS '섹터 릴레이 맵 (HOT/RELAY/COLD)';
COMMENT ON COLUMN vip_content.panel_7_accuracy IS '적중률 통계 (주간/월간/전체)';
COMMENT ON COLUMN vip_content.panel_8_alerts IS 'EXIT/REDUCE 알림 목록';
COMMENT ON COLUMN vip_content.summary IS '요약 메타데이터 (종목수, 매집감지수, 알림수 등)';
