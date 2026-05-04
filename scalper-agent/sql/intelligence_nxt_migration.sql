-- ============================================
-- intelligence_nxt_picks: NXT 야간매수 TOP 5 추천
-- 매일 G7(16:40) C32 발행
-- ============================================
CREATE TABLE IF NOT EXISTS intelligence_nxt_picks (
    date        DATE PRIMARY KEY,
    mode        TEXT DEFAULT 'confirmed', -- 'confirmed' (16:35 최종 확정)
    nxt_score   REAL,                    -- nightwatch 종합 점수 (-10~+10)
    signal      TEXT,                    -- "🟢 매수 고려" 등
    sectors     JSONB,                   -- 추천 섹터 TOP 3
    picks       JSONB,                   -- [{rank, code, name, sector, supply_score, entry_price}]
    created_at  TIMESTAMPTZ DEFAULT now()
);

-- ※ 기존 테이블에 mode 컬럼 추가 (이미 존재하면 무시)
ALTER TABLE intelligence_nxt_picks
ADD COLUMN IF NOT EXISTS mode TEXT DEFAULT 'confirmed';

-- 기존 데이터 mode 채우기
UPDATE intelligence_nxt_picks SET mode = 'confirmed' WHERE mode IS NULL;

ALTER TABLE intelligence_nxt_picks ENABLE ROW LEVEL SECURITY;

CREATE POLICY "nxt_picks_public_read"
    ON intelligence_nxt_picks FOR SELECT USING (true);

CREATE POLICY "nxt_picks_service_write"
    ON intelligence_nxt_picks FOR ALL USING (auth.role() = 'service_role');


-- ============================================
-- intelligence_nxt_performance: NXT 성적표
-- 매일 G7(16:30) C33 발행 — 어제 NXT의 오늘 결과
-- ============================================
CREATE TABLE IF NOT EXISTS intelligence_nxt_performance (
    pick_date      DATE PRIMARY KEY,        -- 추천일 (어제)
    result_date    DATE,                    -- 결과일 (오늘)
    avg_return     REAL,                    -- 평균 수익률(%)
    best_pick      TEXT,
    worst_pick     TEXT,
    weekly_return  REAL DEFAULT 0,
    weekly_days    INT DEFAULT 0,
    weekly_wins    INT DEFAULT 0,
    monthly_return REAL DEFAULT 0,
    monthly_days   INT DEFAULT 0,
    monthly_wins   INT DEFAULT 0,
    items          JSONB,                   -- [{rank, code, name, entry_price, close_price, return_pct}]
    created_at     TIMESTAMPTZ DEFAULT now()
);

ALTER TABLE intelligence_nxt_performance ENABLE ROW LEVEL SECURITY;

CREATE POLICY "nxt_perf_public_read"
    ON intelligence_nxt_performance FOR SELECT USING (true);

CREATE POLICY "nxt_perf_service_write"
    ON intelligence_nxt_performance FOR ALL USING (auth.role() = 'service_role');
