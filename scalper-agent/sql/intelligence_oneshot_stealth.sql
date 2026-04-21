-- intelligence_oneshot_stealth: 원샷 쌍매수 잠복 감지기 (C37)
-- 최근 7일 내 외인+기관 200억+ 동시매수 후 아직 ±7% 잠복 종목
-- NXT TOP5 아래, 매집 레이더 위에 표시

CREATE TABLE IF NOT EXISTS intelligence_oneshot_stealth (
    date DATE PRIMARY KEY,
    lookback_days INTEGER NOT NULL DEFAULT 7,
    min_dual_buy INTEGER NOT NULL DEFAULT 200,
    stealth_count INTEGER NOT NULL DEFAULT 0,
    gone_count INTEGER NOT NULL DEFAULT 0,
    failed_count INTEGER NOT NULL DEFAULT 0,
    stocks JSONB NOT NULL DEFAULT '[]'::jsonb
);

ALTER TABLE intelligence_oneshot_stealth ENABLE ROW LEVEL SECURITY;

CREATE POLICY "oneshot_public_read"
    ON intelligence_oneshot_stealth FOR SELECT USING (true);

CREATE POLICY "oneshot_service_write"
    ON intelligence_oneshot_stealth FOR ALL USING (auth.role() = 'service_role');
