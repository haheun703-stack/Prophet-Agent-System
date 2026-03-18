-- FLOWX Phase C STEP 2: signals + scoreboard 테이블
-- Supabase Dashboard > SQL Editor 에서 실행

CREATE TABLE IF NOT EXISTS signals (
  id uuid DEFAULT gen_random_uuid() PRIMARY KEY,
  bot_type text NOT NULL DEFAULT 'QUANT',
  ticker text NOT NULL,
  ticker_name text NOT NULL,
  signal_type text NOT NULL,
  grade text,
  score integer DEFAULT 0,
  entry_price integer NOT NULL,
  target_price integer DEFAULT 0,
  stop_loss integer DEFAULT 0,
  current_price integer DEFAULT 0,
  return_pct numeric(6,2) DEFAULT 0,
  status text NOT NULL DEFAULT 'OPEN',
  signal_date date NOT NULL,
  close_date date,
  sources jsonb DEFAULT '[]'::jsonb,
  created_at timestamptz DEFAULT now(),
  UNIQUE(bot_type, ticker, signal_date)
);

CREATE INDEX IF NOT EXISTS idx_signals_status ON signals(status, bot_type);
CREATE INDEX IF NOT EXISTS idx_signals_date ON signals(signal_date DESC);

CREATE TABLE IF NOT EXISTS scoreboard (
  id uuid DEFAULT gen_random_uuid() PRIMARY KEY,
  bot_type text NOT NULL,
  period_days integer NOT NULL,
  total_signals integer DEFAULT 0,
  hit_count integer DEFAULT 0,
  hit_rate numeric(5,2) DEFAULT 0,
  avg_return_pct numeric(6,2) DEFAULT 0,
  best_signal jsonb DEFAULT '{}'::jsonb,
  worst_signal jsonb DEFAULT '{}'::jsonb,
  recent_closed jsonb DEFAULT '[]'::jsonb,
  updated_at timestamptz DEFAULT now(),
  UNIQUE(bot_type, period_days)
);

-- RLS 정책
ALTER TABLE signals ENABLE ROW LEVEL SECURITY;
CREATE POLICY "Service role full access" ON signals
  FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "Public read access" ON signals
  FOR SELECT USING (true);

ALTER TABLE scoreboard ENABLE ROW LEVEL SECURITY;
CREATE POLICY "Service role full access" ON scoreboard
  FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "Public read access" ON scoreboard
  FOR SELECT USING (true);
