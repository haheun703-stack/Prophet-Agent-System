-- intelligence_inst_accumulation: 기관 연속매수 초기 감지 (FLOWX 기관매집 레이더)
-- 매일 G7 수급 수집 후 생성, 기관 3일+ 연속 순매수 종목 감지
CREATE TABLE IF NOT EXISTS intelligence_inst_accumulation (
    date            DATE NOT NULL,
    updated_at      TIMESTAMPTZ DEFAULT now(),
    -- 요약
    total_count     INT DEFAULT 0,      -- 전체 감지 종목 수
    early_count     INT DEFAULT 0,      -- 아직 급등 전 (5일 수익 < 15%)
    running_count   INT DEFAULT 0,      -- 이미 진행 중 (5일 수익 >= 15%)
    -- 종목 리스트 (JSONB 배열)
    -- 각 항목: {code, name, sector, cap, inst_consec, inst_cum,
    --           frgn_consec, ret5, ret10, today_inst, today_frgn,
    --           close, tag, last_date}
    early_stocks    JSONB NOT NULL DEFAULT '[]',
    running_stocks  JSONB NOT NULL DEFAULT '[]',
    PRIMARY KEY (date)
);

-- RLS
ALTER TABLE intelligence_inst_accumulation ENABLE ROW LEVEL SECURITY;
CREATE POLICY IF NOT EXISTS "inst_acc_read"
    ON intelligence_inst_accumulation FOR SELECT USING (true);
CREATE POLICY IF NOT EXISTS "inst_acc_write"
    ON intelligence_inst_accumulation FOR ALL USING (true);
