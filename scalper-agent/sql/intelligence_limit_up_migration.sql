-- intelligence_limit_up_signals: 상한가 엔진 일일 시그널
-- 매일 G5c(16:20) 발행 — 트리거 시그널 + 감시풀
CREATE TABLE IF NOT EXISTS intelligence_limit_up_signals (
    date           DATE PRIMARY KEY,
    scanned_at     TIMESTAMPTZ,
    total_scanned  INT DEFAULT 0,          -- 스캔 대상 종목수
    triggered      JSONB,                  -- [{code, name, entry_type, entry_price, tp_price, limit_count, overheat_pct, ...}]
    triggered_count INT DEFAULT 0,
    watchlist      JSONB,                  -- [{code, name, signal_close, monitor_until, ...}]
    watchlist_count INT DEFAULT 0,
    created_at     TIMESTAMPTZ DEFAULT now()
);

-- intelligence_limit_up_performance: 상한가 페이퍼 트레이딩 성적표
-- 매일 G5d(16:25) 발행 — 포지션별 수익률 + 누적 통계
CREATE TABLE IF NOT EXISTS intelligence_limit_up_performance (
    date           DATE PRIMARY KEY,
    -- 당일 액션
    new_entries    INT DEFAULT 0,
    closed_count   INT DEFAULT 0,
    active_count   INT DEFAULT 0,
    -- 보유 포지션 상세
    positions      JSONB,                  -- [{code, name, entry_price, current_price, pnl_pct, hold_days, tp_price, ...}]
    -- 당일 청산 상세
    closed_trades  JSONB,                  -- [{code, name, entry_price, exit_price, pnl_pct, reason, hold_days}]
    -- 누적 통계
    total_trades   INT DEFAULT 0,
    win_rate       REAL DEFAULT 0,         -- 승률(%)
    avg_pnl        REAL DEFAULT 0,         -- 평균 수익률(%)
    cum_return     REAL DEFAULT 0,         -- 누적 수익률(%)
    mdd            REAL DEFAULT 0,         -- 최대 낙폭(%)
    -- 전략별 통계
    strategy1_trades INT DEFAULT 0,        -- 즉시진입 거래수
    strategy1_wins   INT DEFAULT 0,
    strategy2_trades INT DEFAULT 0,        -- 눌림목 거래수
    strategy2_wins   INT DEFAULT 0,
    -- 자금
    cash           INT DEFAULT 0,
    total_value    INT DEFAULT 0,
    created_at     TIMESTAMPTZ DEFAULT now()
);

-- RLS
ALTER TABLE intelligence_limit_up_signals ENABLE ROW LEVEL SECURITY;
ALTER TABLE intelligence_limit_up_performance ENABLE ROW LEVEL SECURITY;

CREATE POLICY "limit_up_signals_public_read"
    ON intelligence_limit_up_signals
    FOR SELECT USING (true);

CREATE POLICY "limit_up_signals_service_write"
    ON intelligence_limit_up_signals
    FOR ALL USING (auth.role() = 'service_role');

CREATE POLICY "limit_up_perf_public_read"
    ON intelligence_limit_up_performance
    FOR SELECT USING (true);

CREATE POLICY "limit_up_perf_service_write"
    ON intelligence_limit_up_performance
    FOR ALL USING (auth.role() = 'service_role');
