-- ═══════════════════════════════════════════════════════════
-- scalper_trade_journal
-- ═══════════════════════════════════════════════════════════
-- 단타봇 실전매매 일지 — timeline 형식 모든 매매 이벤트 영구 기록.
-- 사장님 요청 2026-05-17: FLOWX 대시보드 마지막 탭 "실전매매 결과" 페이지의 데이터 소스.
--
-- 기록 범위:
--   - 5/18~5/19 검증 모드 (1주씩 매수)
--   - 5/20+ 본격 실전 매매 (auto_trader 정상 모드)
--   - NXT 자동매매 (5/21+)
--   - 수동 매매 (사장님 직접 / NXT 야간 등)
--
-- 그래프 데이터 소스:
--   1. Equity Curve (누적 수익률 곡선)
--   2. 시그널 적중률 도넛 (signal_tags 분석)
--   3. 일별 PnL + 드로다운 (event_date GROUP BY)
--   4. TOP/Bottom 종목 (stock_ticker GROUP BY)
--   5. Timeline 일지 (event_time DESC LIMIT 50)
--
-- BIGSERIAL id PK + (event_date, stock_ticker, event_type) 인덱스
-- ═══════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS scalper_trade_journal (
    id                  BIGSERIAL PRIMARY KEY,

    -- 이벤트 시점
    event_time          TIMESTAMPTZ NOT NULL,
    event_date          DATE NOT NULL,         -- 일별 집계 인덱스용 (event_time::date)

    -- 이벤트 종류
    event_type          TEXT NOT NULL,         -- 'buy', 'sell_tp', 'sell_sl', 'sell_close', 'partial_sell', 'manual_buy', 'manual_sell'
    source              TEXT NOT NULL,         -- 'morning_rec', 'verification', 'nxt_auto', 'predawn', 'manual', 'guardian', 'eye'

    -- 종목 정보
    stock_ticker        TEXT NOT NULL,
    stock_name          TEXT NOT NULL,

    -- 체결 정보
    qty                 INT NOT NULL,
    price               NUMERIC(12,2) NOT NULL,
    total_amount        NUMERIC(14,2),         -- qty * price

    -- 매도 시 PnL (매수 이벤트에는 NULL)
    matched_buy_price   NUMERIC(12,2),         -- 평균 매수가 (FIFO 또는 평단)
    pnl_pct             NUMERIC(8,3),
    pnl_amount          NUMERIC(14,2),

    -- 시그널 컨텍스트 (매수 시점 — 적중률 분석)
    signal_tags         TEXT,                  -- "etf_lead(TOP10/4ETF:+13) dual_buy(...) pension(S:+15)"
    final_score         NUMERIC(8,2),

    -- 메타
    order_no            TEXT,                   -- KIS 주문 번호 (감사/추적)
    note                TEXT,                   -- 특이 사유 (예: "외인 OVERHEAT 강제청산")
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ═══════════════════════════════════════════════════════════
-- 인덱스 (그래프별 쿼리 최적화)
-- ═══════════════════════════════════════════════════════════

-- Timeline 정렬 (대시보드 매매 일지)
CREATE INDEX IF NOT EXISTS idx_scalper_trade_journal_time
    ON scalper_trade_journal (event_time DESC);

-- 일별 집계 (Equity Curve, 일별 PnL 막대)
CREATE INDEX IF NOT EXISTS idx_scalper_trade_journal_date
    ON scalper_trade_journal (event_date DESC, event_type);

-- 종목별 집계 (TOP/Bottom 종목)
CREATE INDEX IF NOT EXISTS idx_scalper_trade_journal_stock
    ON scalper_trade_journal (stock_ticker, event_date DESC);

-- source 필터 (검증 vs 실전 분리 view)
CREATE INDEX IF NOT EXISTS idx_scalper_trade_journal_source
    ON scalper_trade_journal (source, event_date DESC);

-- PnL 분석 (시그널 적중률 — 매도 이벤트만)
CREATE INDEX IF NOT EXISTS idx_scalper_trade_journal_pnl
    ON scalper_trade_journal (event_date DESC, pnl_pct DESC NULLS LAST)
    WHERE event_type LIKE 'sell%';

-- ═══════════════════════════════════════════════════════════
-- RLS
-- ═══════════════════════════════════════════════════════════
ALTER TABLE scalper_trade_journal ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "read_scalper_trade_journal" ON scalper_trade_journal;
CREATE POLICY "read_scalper_trade_journal"
    ON scalper_trade_journal
    FOR SELECT
    TO anon, authenticated
    USING (true);

DROP POLICY IF EXISTS "upsert_scalper_trade_journal" ON scalper_trade_journal;
CREATE POLICY "upsert_scalper_trade_journal"
    ON scalper_trade_journal
    FOR ALL
    TO service_role
    USING (true)
    WITH CHECK (true);

-- ═══════════════════════════════════════════════════════════
-- Comments
-- ═══════════════════════════════════════════════════════════
COMMENT ON TABLE scalper_trade_journal IS
    '단타봇 실전매매 일지 — FLOWX 대시보드 실전매매 결과 페이지 데이터 소스. 5/18~ 모든 매매 이벤트 timeline.';
COMMENT ON COLUMN scalper_trade_journal.event_type IS
    'buy/sell_tp/sell_sl/sell_close(15:30)/partial_sell/manual_buy/manual_sell';
COMMENT ON COLUMN scalper_trade_journal.source IS
    'morning_rec(09:00 자동) / verification(검증 모드) / nxt_auto(NXT 자동) / predawn(선취매) / manual(수동) / guardian(position_guardian) / eye(AI EYE)';
COMMENT ON COLUMN scalper_trade_journal.signal_tags IS
    '매수 이벤트 시점의 시그널 소스 텍스트 (공백 구분). 적중률 분석용. 예: "etf_lead(TOP10/4ETF:+13) dual_buy(+12)"';
COMMENT ON COLUMN scalper_trade_journal.pnl_pct IS
    '매도 이벤트만 — (price - matched_buy_price) / matched_buy_price * 100, 수수료/세금 차감 전';

-- ═══════════════════════════════════════════════════════════
-- 분석용 View (그래프 직접 쿼리에 활용)
-- ═══════════════════════════════════════════════════════════

-- 일별 PnL 집계 (Equity Curve + 일별 막대)
CREATE OR REPLACE VIEW scalper_trade_daily_pnl AS
SELECT
    event_date,
    COUNT(*) FILTER (WHERE event_type = 'buy') AS buys,
    COUNT(*) FILTER (WHERE event_type LIKE 'sell%') AS sells,
    SUM(pnl_amount) FILTER (WHERE event_type LIKE 'sell%') AS daily_pnl_amount,
    AVG(pnl_pct) FILTER (WHERE event_type LIKE 'sell%') AS daily_avg_pnl_pct,
    COUNT(*) FILTER (WHERE event_type LIKE 'sell%' AND pnl_pct > 0) AS wins,
    COUNT(*) FILTER (WHERE event_type LIKE 'sell%' AND pnl_pct <= 0) AS losses
FROM scalper_trade_journal
GROUP BY event_date
ORDER BY event_date DESC;

COMMENT ON VIEW scalper_trade_daily_pnl IS
    'Equity Curve + 일별 PnL 막대 차트용 일별 집계 (모든 source 통합)';

-- 종목별 누적 PnL (TOP/Bottom 차트)
CREATE OR REPLACE VIEW scalper_trade_stock_pnl AS
SELECT
    stock_ticker,
    stock_name,
    COUNT(*) FILTER (WHERE event_type = 'buy') AS trade_count,
    SUM(pnl_amount) FILTER (WHERE event_type LIKE 'sell%') AS total_pnl_amount,
    AVG(pnl_pct) FILTER (WHERE event_type LIKE 'sell%') AS avg_pnl_pct,
    COUNT(*) FILTER (WHERE event_type LIKE 'sell%' AND pnl_pct > 0) AS wins,
    COUNT(*) FILTER (WHERE event_type LIKE 'sell%' AND pnl_pct <= 0) AS losses
FROM scalper_trade_journal
GROUP BY stock_ticker, stock_name
ORDER BY total_pnl_amount DESC NULLS LAST;

COMMENT ON VIEW scalper_trade_stock_pnl IS
    'TOP/Bottom 종목 차트용 — 종목별 누적 PnL + 매매 횟수';
