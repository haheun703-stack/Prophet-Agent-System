-- ═══════════════════════════════════════════════════════════
-- scalper_verification_log
-- ═══════════════════════════════════════════════════════════
-- 검증 모드 (1주 실전 수익률 검증) — 일자별 종목별 매수/청산 기록.
-- 사장님 제안 2026-05-17: morning_recommendation 전체 1주씩 사서 1~2일 정산.
--
-- 영구 기록 목적:
--   1. 시그널 가중치 분석 (etf_lead/dual_buy/pension/bomb 등 적중률)
--   2. 본격 진입 시점 결정 (2일 평균 PnL > +0.5% 임계)
--   3. 향후 백테스트 ground truth (실측 슬리피지/체결 포함)
--
-- 복합 PK (trade_date, stock_ticker) — 같은 날 같은 종목 중복 방지
-- ═══════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS scalper_verification_log (
    trade_date          DATE NOT NULL,
    stock_ticker        TEXT NOT NULL,
    stock_name          TEXT NOT NULL,

    -- 매수
    buy_price           NUMERIC(12,2) NOT NULL,
    buy_time            TIMESTAMPTZ NOT NULL,

    -- 청산 (당일 15:25 강제 청산 시점)
    sell_price          NUMERIC(12,2),
    sell_time           TIMESTAMPTZ,
    sell_reason         TEXT,  -- '1525_close' / 'overheat' / 'manual' 등

    -- 결과
    pnl_pct             NUMERIC(8,3),  -- 수익률 %

    -- 시그널 컨텍스트 (적중률 분석용)
    signal_tags         TEXT,           -- 예: "etf_lead(TOP10/4ETF:+13) dual_buy(...) pension(S:+15)"
    final_score         NUMERIC(8,2),  -- morning_recommendation 최종 점수

    -- 메타
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (trade_date, stock_ticker)
);

CREATE INDEX IF NOT EXISTS idx_scalper_verification_log_date
    ON scalper_verification_log (trade_date DESC);

CREATE INDEX IF NOT EXISTS idx_scalper_verification_log_pnl
    ON scalper_verification_log (trade_date DESC, pnl_pct DESC NULLS LAST);

CREATE INDEX IF NOT EXISTS idx_scalper_verification_log_score
    ON scalper_verification_log (final_score DESC);

-- ═══════════════════════════════════════════════════════════
-- RLS
-- ═══════════════════════════════════════════════════════════
ALTER TABLE scalper_verification_log ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "read_scalper_verification_log" ON scalper_verification_log;
CREATE POLICY "read_scalper_verification_log"
    ON scalper_verification_log
    FOR SELECT
    TO anon, authenticated
    USING (true);

DROP POLICY IF EXISTS "upsert_scalper_verification_log" ON scalper_verification_log;
CREATE POLICY "upsert_scalper_verification_log"
    ON scalper_verification_log
    FOR ALL
    TO service_role
    USING (true)
    WITH CHECK (true);

-- ═══════════════════════════════════════════════════════════
-- Comments
-- ═══════════════════════════════════════════════════════════
COMMENT ON TABLE scalper_verification_log IS
    '검증 모드 1주 실전 매수/청산 기록 — 시그널 가중치 분석 + 본격 진입 결정 ground truth.';
COMMENT ON COLUMN scalper_verification_log.signal_tags IS
    '매수 시점의 시그널 소스 텍스트 (공백 구분). 예: "etf_lead(TOP10/4ETF:+13) dual_buy(...) pension(S:+15)"';
COMMENT ON COLUMN scalper_verification_log.sell_reason IS
    '1525_close: 15:25 자동 청산 / overheat: 외인 던지기 감지 / manual: 수동 청산';
COMMENT ON COLUMN scalper_verification_log.pnl_pct IS
    '(sell_price - buy_price) / buy_price * 100 — 수수료/세금 차감 전';
