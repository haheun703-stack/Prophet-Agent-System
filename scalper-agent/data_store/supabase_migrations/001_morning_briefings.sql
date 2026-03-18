-- FLOWX Phase C STEP 1: morning_briefings 테이블
-- Supabase Dashboard > SQL Editor 에서 실행

CREATE TABLE IF NOT EXISTS morning_briefings (
  id uuid DEFAULT gen_random_uuid() PRIMARY KEY,
  date date NOT NULL UNIQUE,
  market_status text NOT NULL DEFAULT 'NEUTRAL',
  kospi_close numeric(10,2),
  kosdaq_close numeric(10,2),
  us_summary text,
  kr_summary text,
  cross_regime text,
  cross_regime_detail text,
  warning text,
  news_picks jsonb DEFAULT '[]'::jsonb,
  sector_focus jsonb DEFAULT '[]'::jsonb,
  global_events jsonb DEFAULT '[]'::jsonb,
  top_stocks jsonb DEFAULT '[]'::jsonb,
  guardian_alerts jsonb DEFAULT '[]'::jsonb,
  raw_indicators jsonb DEFAULT '{}'::jsonb,
  full_report text,
  created_at timestamptz DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_briefings_date ON morning_briefings(date DESC);

-- RLS
ALTER TABLE morning_briefings ENABLE ROW LEVEL SECURITY;
CREATE POLICY "Service role full access" ON morning_briefings
  FOR ALL USING (true) WITH CHECK (true);
CREATE POLICY "Public read access" ON morning_briefings
  FOR SELECT USING (true);
