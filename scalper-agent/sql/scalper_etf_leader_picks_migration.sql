-- ═══════════════════════════════════════════════════════════
-- scalper_etf_leader_picks
-- ═══════════════════════════════════════════════════════════
-- 단타봇 ETF 주도주 발굴 파이프라인 결과 (Step A→B→C).
-- 차트영웅 영상 로직 기반: 다년 박스권 돌파 + 눌림목 ETF의
-- 비중 TOP 10 종목을 점수화하여 TOP 30 주도주 산출.
--
-- 데이터 소스:
--   - leader_picks_<date>.json (Step B: TOP 20 ETF + 차트 점수)
--   - holdings_<date>/{ticker}.json (Step C: 네이버 etfAnalysis 비중)
--   - leader_stocks_<date>.json (Step C-4: 점수화 결과)
--
-- 복합 PK (base_date, stock_ticker) — 같은 날 같은 종목 중복 방지
-- 매월/매분기 재실행하여 base_date별로 누적
-- ═══════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS scalper_etf_leader_picks (
    base_date           DATE NOT NULL,
    stock_ticker        TEXT NOT NULL,
    rank                INT  NOT NULL,
    stock_name          TEXT NOT NULL,
    leader_score        NUMERIC(8,3) NOT NULL,
    etf_count           INT  NOT NULL,
    top_weight_pct      NUMERIC(6,2) NOT NULL,
    top_etf_ticker      TEXT,
    top_etf_name        TEXT,
    top_etf_score       INT,

    -- 포함 ETF 상세 (배열): [{etf_ticker, etf_name, etf_score, weight_pct, seq, contribution}]
    included_etfs       JSONB NOT NULL DEFAULT '[]',

    -- 메타
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (base_date, stock_ticker)
);

CREATE INDEX IF NOT EXISTS idx_scalper_etf_leader_picks_date
    ON scalper_etf_leader_picks (base_date DESC);

CREATE INDEX IF NOT EXISTS idx_scalper_etf_leader_picks_rank
    ON scalper_etf_leader_picks (base_date DESC, rank ASC);

CREATE INDEX IF NOT EXISTS idx_scalper_etf_leader_picks_score
    ON scalper_etf_leader_picks (leader_score DESC);

CREATE INDEX IF NOT EXISTS idx_scalper_etf_leader_picks_etf_count
    ON scalper_etf_leader_picks (etf_count DESC, leader_score DESC)
    WHERE etf_count >= 3;

-- ═══════════════════════════════════════════════════════════
-- RLS (Row Level Security)
-- ═══════════════════════════════════════════════════════════
ALTER TABLE scalper_etf_leader_picks ENABLE ROW LEVEL SECURITY;

-- anon/authenticated 읽기 허용 (FLOWX 웹 대시보드)
DROP POLICY IF EXISTS "read_scalper_etf_leader_picks" ON scalper_etf_leader_picks;
CREATE POLICY "read_scalper_etf_leader_picks"
    ON scalper_etf_leader_picks
    FOR SELECT
    TO anon, authenticated
    USING (true);

-- service_role upsert 허용 (단타봇 업로드)
DROP POLICY IF EXISTS "upsert_scalper_etf_leader_picks" ON scalper_etf_leader_picks;
CREATE POLICY "upsert_scalper_etf_leader_picks"
    ON scalper_etf_leader_picks
    FOR ALL
    TO service_role
    USING (true)
    WITH CHECK (true);

-- 향후 scalper_writer role 분리 후 (AUDIT_BACKLOG §1 C-2):
-- GRANT SELECT, INSERT, UPDATE, DELETE ON scalper_etf_leader_picks TO scalper_writer;
-- GRANT SELECT ON scalper_etf_leader_picks TO scalper_readonly;

-- ═══════════════════════════════════════════════════════════
-- Comments
-- ═══════════════════════════════════════════════════════════
COMMENT ON TABLE scalper_etf_leader_picks IS
    '단타봇 ETF 주도주 발굴 결과 (차트영웅 패턴 + 네이버 etfAnalysis 비중 통합). base_date별 TOP 30 누적.';
COMMENT ON COLUMN scalper_etf_leader_picks.leader_score IS
    '∑(ETF점수 × 비중% × rank_factor) — rank_factor: TOP3=1.2x, TOP5=1.1x, 그 외 1.0x';
COMMENT ON COLUMN scalper_etf_leader_picks.etf_count IS
    '이 종목을 비중 TOP 10에 포함한 ETF 개수 (3개 이상 = 강한 주도주 후보)';
COMMENT ON COLUMN scalper_etf_leader_picks.top_weight_pct IS
    '이 종목이 가장 큰 비중으로 들어간 ETF에서의 비중 (%)';
COMMENT ON COLUMN scalper_etf_leader_picks.included_etfs IS
    'JSONB 배열. 각 항목: {etf_ticker, etf_name, etf_score, weight_pct, seq, contribution}';
