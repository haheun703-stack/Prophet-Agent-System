-- ═══════════════════════════════════════════════════
--  FLOWX 스윙시스템 대시보드 테이블
--  페이지: /swing
--  업데이트: 매일 16:40 (G7 완료 후)
-- ═══════════════════════════════════════════════════

CREATE TABLE dashboard_swing (
  id BIGSERIAL PRIMARY KEY,
  date DATE NOT NULL,

  -- ── BRAIN 판단 ──
  brain_verdict TEXT NOT NULL DEFAULT '관망',       -- 공격/표준/방어/관망
  brain_pct INTEGER NOT NULL DEFAULT 0,             -- 0~100 (포지션 비중)
  brain_reason TEXT DEFAULT '',
  regime TEXT DEFAULT 'NORMAL',                     -- MOMENTUM/NORMAL/CAUTION/PANIC
  regime_severity INTEGER DEFAULT 0,                -- 0~5
  regime_desc TEXT DEFAULT '',                      -- "전면 방어 — 현금+금+인버스만"

  -- ── 자산 배분 (%) ──
  alloc_swing INTEGER DEFAULT 0,                    -- BH 스윙
  alloc_gold_etf INTEGER DEFAULT 0,                 -- 금 ETF
  alloc_inverse INTEGER DEFAULT 0,                  -- 인버스 ETF
  alloc_group_etf INTEGER DEFAULT 0,                -- 그룹 ETF
  alloc_small_cap INTEGER DEFAULT 0,                -- 소형주
  alloc_cash INTEGER DEFAULT 100,                   -- 현금

  -- ── 추천 종목 (JSONB) ──
  -- picks[]: {code, name, grade, score, entry_price, target_price, stop_price,
  --           hold_days, conviction, catalyst, rr_ratio, regime, supply_score,
  --           tv_pattern, nat_power_grade}
  picks JSONB DEFAULT '[]'::jsonb,

  -- etf_picks[]: {code, name, category, signal, entry, sl, tp, reason, holding_days}
  etf_picks JSONB DEFAULT '[]'::jsonb,

  -- watchlist[]: {code, name, reason, trigger, grade, score}
  watchlist JSONB DEFAULT '[]'::jsonb,

  -- ── NXT 야간매매 ──
  nxt_signal TEXT DEFAULT '',                       -- ☠️/🔴/🟡/🟢/🟢🟢
  nxt_signal_text TEXT DEFAULT '',                   -- "인버스 강력" / "매수" / "관망"
  nxt_score REAL DEFAULT 0,                         -- -10 ~ +10
  nxt_reason TEXT DEFAULT '',

  -- nxt_targets[]: {code, name, sector, tier, priority, supply_score, is_etf}
  nxt_targets JSONB DEFAULT '[]'::jsonb,

  -- ── 시장 지표 ──
  vix REAL DEFAULT 0,
  nasdaq_pct REAL DEFAULT 0,
  usdkrw REAL DEFAULT 0,
  oil_pct REAL DEFAULT 0,
  gold_pct REAL DEFAULT 0,
  silver_pct REAL DEFAULT 0,

  -- ── 분석 보고서 ──
  -- analysis: {macro_summary, commodity_summary, sector_summary, flow_summary, risk_summary}
  analysis JSONB DEFAULT '{}'::jsonb,

  -- portfolio: {win_rate, total_trades, current_picks, brain_cash_ratio}
  portfolio JSONB DEFAULT '{}'::jsonb,

  -- ── 센서 데이터 ──
  smart_money_score REAL DEFAULT 0,                 -- COT 스마트머니 (-10 ~ +10)
  smart_money_signal TEXT DEFAULT '',               -- RISK_ON / RISK_OFF / NEUTRAL
  stress_index REAL DEFAULT 0,                      -- 크로스에셋 스트레스 (0~10)
  stress_level TEXT DEFAULT '',                     -- LOW / NORMAL / ELEVATED / HIGH
  rotation_signal TEXT DEFAULT '',                  -- "HOT: 2차전지/EV | NEXT: 방산"
  liquidity_score REAL DEFAULT 0,                   -- 유동성 사이클 (-10 ~ +10)

  -- ── 메타 ──
  market_comment TEXT DEFAULT '',
  generated_at TIMESTAMPTZ DEFAULT NOW(),
  created_at TIMESTAMPTZ DEFAULT NOW(),

  CONSTRAINT dashboard_swing_date_unique UNIQUE (date)
);

-- 인덱스
CREATE INDEX idx_dashboard_swing_date ON dashboard_swing(date DESC);

-- RLS (Row Level Security) — 읽기 전용 공개
ALTER TABLE dashboard_swing ENABLE ROW LEVEL SECURITY;
CREATE POLICY "Public read" ON dashboard_swing FOR SELECT USING (true);
