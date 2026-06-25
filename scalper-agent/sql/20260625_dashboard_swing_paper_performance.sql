-- ============================================================
-- 한국스윙(dashboard_swing) 페이퍼 매매 누적수익률 컬럼 추가
-- 6/25 사장님 지시: '시장판단&전략' 오른쪽에 페이퍼 누적수익률 매일 기록 (퀀트봇 방식)
-- ============================================================
-- 멱등(IF NOT EXISTS) · nullable · 무손상.
-- 컬럼 추가 즉시 단타봇 다음 upload_dashboard_swing()부터 paper_performance 자동 적재.
-- /api/swing-dashboard 가 select('*') 라 API 무수정으로 자동 포함.
--
-- paper_performance JSONB 구조 (둘 다 나란히 — 미실현 숨기지 않음):
--   {
--     "date", "start_date", "initial_cash",
--     "equity_return_pct",   -- 기준1: 전체자산 누적(미실현 보유손실 포함·현실)
--     "mdd_pct",
--     "realized_sum_pct",    -- 기준2: 청산손익합(판 것만·realized)
--     "closed_total", "closed_wins", "win_rate",
--     "today_closed", "today_realized_pnl", "open_positions",
--     "equity_curve": [{"date","equity_pct"}, ...],  -- 최근 30 스냅샷
--     "note"
--   }
-- ============================================================

ALTER TABLE dashboard_swing
    ADD COLUMN IF NOT EXISTS paper_performance JSONB;

COMMENT ON COLUMN dashboard_swing.paper_performance IS
    '단타봇 페이퍼 매매 누적수익률 — 전체자산(equity_return_pct·미실현포함) + 청산손익합(realized_sum_pct·판것만) 두 기준. record-only·실매수 신호 아님 (6/25 사장님)';
