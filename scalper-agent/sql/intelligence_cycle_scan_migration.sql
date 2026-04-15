-- intelligence_cycle_scan: 수급 사이클 감지기 일일 결과
-- FLOWX 웹 대시보드 "사이클 감지기" 패널 데이터
-- 4세력(외인/기관/개인/기타법인) 기반 사이클 위상 판정

CREATE TABLE IF NOT EXISTS intelligence_cycle_scan (
    date              DATE PRIMARY KEY,
    total_scanned     INTEGER DEFAULT 0,
    surge_count       INTEGER DEFAULT 0,     -- 급등임박
    accumulate_count  INTEGER DEFAULT 0,     -- 매집
    reversal_count    INTEGER DEFAULT 0,     -- 전환
    neutral_count     INTEGER DEFAULT 0,     -- 중립
    distribute_count  INTEGER DEFAULT 0,     -- 물량분배
    peak_warn_count   INTEGER DEFAULT 0,     -- 고점경고
    surge_items       JSONB DEFAULT '[]'::jsonb,      -- 급등임박 종목 리스트
    accumulate_items  JSONB DEFAULT '[]'::jsonb,      -- 매집 종목 리스트
    warning_items     JSONB DEFAULT '[]'::jsonb,      -- 경고(분배+고점) 종목 리스트
    top_surge_names   JSONB DEFAULT '[]'::jsonb,      -- 급등임박 종목명 요약
    phase_summary     JSONB DEFAULT '{}'::jsonb,       -- 위상별 통계 요약
    created_at        TIMESTAMPTZ DEFAULT now()
);

-- RLS 설정
ALTER TABLE intelligence_cycle_scan ENABLE ROW LEVEL SECURITY;

CREATE POLICY "사이클감지_공개읽기"
    ON intelligence_cycle_scan FOR SELECT
    USING (true);

CREATE POLICY "사이클감지_서비스쓰기"
    ON intelligence_cycle_scan FOR ALL
    USING (auth.role() = 'service_role');

-- 인덱스
CREATE INDEX IF NOT EXISTS idx_cycle_scan_date
    ON intelligence_cycle_scan (date DESC);

COMMENT ON TABLE intelligence_cycle_scan IS '4세력 수급 사이클 감지기 일일 결과 (급등임박/매집/전환/분배/고점경고)';

-- v2: reversal_items 컬럼 추가 (전환 위상 종목 상세)
ALTER TABLE intelligence_cycle_scan
    ADD COLUMN IF NOT EXISTS reversal_items JSONB DEFAULT '[]'::jsonb;
