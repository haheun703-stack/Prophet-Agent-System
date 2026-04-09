-- ═══════════════════════════════════════════════════════════
-- intelligence_daytrading_picks
-- ═══════════════════════════════════════════════════════════
-- 단타 TOP픽 Hybrid 2단 발행 (preview 16:45 + confirmed 07:35)
-- FLOWX 웹 대시보드의 "오늘의 단타 TOP픽" 패널에 표시됨.
--
-- 복합 PK (date, mode) — 같은 날 preview/confirmed 둘 다 저장
-- ═══════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS intelligence_daytrading_picks (
    date            DATE NOT NULL,
    mode            TEXT NOT NULL CHECK (mode IN ('preview', 'confirmed')),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- 집계
    picks_count     INT  NOT NULL DEFAULT 0,
    track_a_count   INT  NOT NULL DEFAULT 0,  -- 대형주
    track_b_count   INT  NOT NULL DEFAULT 0,  -- 중소형주

    -- 외국인 바스켓 시그널 (preview 모드는 NULL 또는 0)
    ewy_1d          NUMERIC(6,2),
    ewy_5d          NUMERIC(6,2),
    ks200_1d        NUMERIC(6,2),
    ks200_5d        NUMERIC(6,2),
    ewy_source      TEXT,

    -- 종목 리스트 (rank/code/name/sector/track/mcap/close/
    --              entry_low/entry_high/tp1/tp2/sl/upside/final_score/
    --              score/sector_bonus/mcap_bonus/ewy_bonus/key_reasons/
    --              foreign_total/inst_total/dual_total/buy_days/inst_joining/
    --              price_change/etf_alt_code/etf_alt_name/etf_alt_theme)
    picks           JSONB NOT NULL DEFAULT '[]',

    PRIMARY KEY (date, mode)
);

CREATE INDEX IF NOT EXISTS idx_daytrading_picks_date
    ON intelligence_daytrading_picks (date DESC);

CREATE INDEX IF NOT EXISTS idx_daytrading_picks_mode
    ON intelligence_daytrading_picks (mode, date DESC);

-- ═══════════════════════════════════════════════════════════
-- RLS (Row Level Security)
-- ═══════════════════════════════════════════════════════════
ALTER TABLE intelligence_daytrading_picks ENABLE ROW LEVEL SECURITY;

-- anon/authenticated 읽기 허용 (웹 대시보드)
DROP POLICY IF EXISTS "read_daytrading_picks" ON intelligence_daytrading_picks;
CREATE POLICY "read_daytrading_picks"
    ON intelligence_daytrading_picks
    FOR SELECT
    TO anon, authenticated
    USING (true);

-- service_role upsert 허용 (봇 업로드)
DROP POLICY IF EXISTS "upsert_daytrading_picks" ON intelligence_daytrading_picks;
CREATE POLICY "upsert_daytrading_picks"
    ON intelligence_daytrading_picks
    FOR ALL
    TO service_role
    USING (true)
    WITH CHECK (true);

-- ═══════════════════════════════════════════════════════════
-- Comment
-- ═══════════════════════════════════════════════════════════
COMMENT ON TABLE intelligence_daytrading_picks IS
    '단타 TOP픽 Hybrid 2단 발행 (preview 16:45 + confirmed 07:35). FLOWX 웹 대시보드 단타 패널.';
COMMENT ON COLUMN intelligence_daytrading_picks.mode IS
    'preview: 전일 16:45 국장마감 기준 (NXT 야간매수용) / confirmed: 당일 07:35 미국장 반영 (09:00 진입용)';
COMMENT ON COLUMN intelligence_daytrading_picks.picks IS
    'JSONB 배열. 각 항목: rank/code/name/sector/track/mcap/close/entry_low/entry_high/tp1/tp2/sl/upside/final_score/key_reasons/etf_alt_code/etf_alt_name/etf_alt_theme';
