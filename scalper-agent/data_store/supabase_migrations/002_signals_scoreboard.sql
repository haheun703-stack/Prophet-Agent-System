-- FLOWX Phase C STEP 2+3: signals + scoreboard 테이블
-- Supabase Dashboard > SQL Editor 에서 실행

-- ═══════════════════════════════════════
--  signals (퀀트 + 단타 공용)
-- ═══════════════════════════════════════

CREATE TABLE IF NOT EXISTS signals (
  id uuid DEFAULT gen_random_uuid() PRIMARY KEY,
  bot_type text NOT NULL CHECK (bot_type IN ('QUANT', 'DAYTRADING')),
  ticker text NOT NULL,
  ticker_name text NOT NULL,
  signal_type text NOT NULL CHECK (signal_type IN ('BUY', 'SELL')),
  grade text CHECK (grade IN ('AAA', 'AA', 'A', 'BBB', 'BB', 'B', 'C', 'D', 'F')),
  score integer CHECK (score BETWEEN 0 AND 100),
  entry_price integer NOT NULL,
  target_price integer,
  stop_price integer,
  current_price integer,
  return_pct numeric(6,2) DEFAULT 0,
  max_return_pct numeric(6,2) DEFAULT 0,
  status text NOT NULL DEFAULT 'OPEN'
    CHECK (status IN ('OPEN', 'CLOSED', 'STOPPED')),
  signal_date date NOT NULL DEFAULT CURRENT_DATE,
  close_date date,
  close_reason text CHECK (close_reason IN (
    'TARGET_HIT', 'STOP_HIT', 'TIME_LIMIT', 'MANUAL', 'DAILY_CLOSE'
  )),
  multiplier numeric(3,1) DEFAULT 1.0,
  memo text,
  sources jsonb DEFAULT '[]'::jsonb,
  created_at timestamptz DEFAULT now(),
  updated_at timestamptz DEFAULT now(),
  UNIQUE(bot_type, ticker, signal_date)
);

-- 인덱스
CREATE INDEX IF NOT EXISTS idx_signals_status ON signals(status);
CREATE INDEX IF NOT EXISTS idx_signals_bot_type ON signals(bot_type);
CREATE INDEX IF NOT EXISTS idx_signals_date ON signals(signal_date DESC);
CREATE INDEX IF NOT EXISTS idx_signals_ticker ON signals(ticker);

-- updated_at 자동 갱신 트리거
CREATE OR REPLACE FUNCTION update_updated_at()
RETURNS TRIGGER AS $$
BEGIN NEW.updated_at = now(); RETURN NEW; END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER signals_updated_at
  BEFORE UPDATE ON signals
  FOR EACH ROW EXECUTE FUNCTION update_updated_at();

-- RLS
ALTER TABLE signals ENABLE ROW LEVEL SECURITY;
CREATE POLICY "Service role full access" ON signals
  FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "Public read access" ON signals
  FOR SELECT USING (true);

-- ═══════════════════════════════════════
--  scoreboard (집계 결과)
-- ═══════════════════════════════════════

CREATE TABLE IF NOT EXISTS scoreboard (
  id uuid DEFAULT gen_random_uuid() PRIMARY KEY,
  bot_type text NOT NULL CHECK (bot_type IN ('QUANT', 'DAYTRADING', 'ALL')),
  period text NOT NULL CHECK (period IN ('30D', '60D', '90D', 'ALL')),
  total_signals integer DEFAULT 0,
  win_count integer DEFAULT 0,
  lose_count integer DEFAULT 0,
  win_rate numeric(5,2) DEFAULT 0,
  avg_return_pct numeric(6,2) DEFAULT 0,
  avg_win_pct numeric(6,2) DEFAULT 0,
  avg_lose_pct numeric(6,2) DEFAULT 0,
  best_signal jsonb DEFAULT '{}'::jsonb,
  worst_signal jsonb DEFAULT '{}'::jsonb,
  recent_closed jsonb DEFAULT '[]'::jsonb,
  calculated_at timestamptz DEFAULT now(),
  UNIQUE(bot_type, period)
);

-- RLS
ALTER TABLE scoreboard ENABLE ROW LEVEL SECURITY;
CREATE POLICY "Service role full access" ON scoreboard
  FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "Public read access" ON scoreboard
  FOR SELECT USING (true);
