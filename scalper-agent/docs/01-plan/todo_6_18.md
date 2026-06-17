# 단타봇 내일(6/18 목) TODO — 6/17 전체검수 후속

> 작성: 단타봇 6/17(수) 마감. 우선순위 = 정보봇 기준 "효력 있는(가동 즉시 위험) 것 먼저 / unfreeze 의존은 나중".
> 안전 전제: 봇 OFF·실주문 0·picks 불변·매도 무손상. 모든 코드 변경은 4-Tier 검수 의무.
> 근거 메모리: project_full_audit_6_17 · project_limit_up_hardstop_sajang_6_17 · project_early_entry_shadow_impl_6_16

---

## A. 가동전 P0 — 가동 즉시 위험 (먼저)

### A-1. jarvis +10% 고정 TP → SAJANG 단일진실 (최우선)
- **위치**: `bot/jarvis_decision.py:250` (`pnl>=10 → ACTION_TAKE_PROFIT`) + L234~246 시총별 SL(-10/-5/-3%) SAJANG 미경유 하드코딩
- **실태**: dead 아님 — `trading_coo.py:5165 run_repeating(_job_jarvis_decision, interval=300)` = 5분주기 라이브 배선. 현재 3중게이트(config `dry_run:true` + `AUTO_TRADE_DISABLED=True` + `_order_gate`)로 차단 중.
- **위험**: 셋 다 해제 동시 시 +10% 고정TP 실발동 → 트레일링-only 영구룰 위반(5/26 삼화콘덴서 계열).
- **fix 안**: ① +10% TP를 `SAJANG.get_take_profit`(=0) 경유로 / 시총SL을 `SAJANG.NORMAL_SL_PCT` 경유로, 또는 ② jarvis 매도액션 영구 dry_run lock. → 사장님 결정 1건 필요(jarvis 자체 폐기 vs SAJANG 경유 존속).
- 4-Tier 후 commit.

### A-2. RealtimeMonitor TP=0 즉시매도 트랩
- **위치**: `data/realtime_monitor.py:447` (`if price >= pos.current_tp` — current_tp=0이면 `price>=0` 항상 참 → 즉시 FULL_SELL)
- **미확인(먼저 규명)**: `_restore_positions_to_rt_monitor`(메모리상 auto_trader.py:6084)가 재시작 시 tp=0으로 RT monitor에 등록하는지 직접 추적. 등록되면 봇 ON 즉시 전량매도 위험.
- 현재 차단 = `_job_monitor` 킬스위치 한 겹 + `_is_sell_protected` + `_order_gate`. 한 겹 의존이라 가동전 정밀 점검 1순위.
- fix = current_tp=0 가드(`if tp>0 and price>=tp`) 추가 검토 → 4-Tier.

---

## B. 가동전 P0 — unfreeze 의존 (나중)

### B-1. 상한가 +25% 즉시 분할(룰7) 라이브 연결
- `should_trigger_split`(즉시 절반+29%) = 현재 test만. auto_trader는 `should_trigger_eod_split`(룰B EOD 15:25, auto_trader.py:5152)만 라이브.
- 장중 즉시 룰7 미실현. 봇 가동 시 룰7 미동작 → intraday monitor loop에 호출 추가 필요. unfreeze 후.

### B-2. dynamic_target engine 규명/정리
- `strategies/dynamic_target.py` import+instantiate(auto_trader.py:4567 job_realtime_eval)만, `re_evaluate`/`adjust_tp` 라이브 호출 여부 불명확(부분 dead).
- 낮에 실제 호출 경로 추적 → 살아있으면 SL/TP 계산이 SAJANG 경유인지 확인, dead면 정리.
- (참고: dynamic_tp +5% block `elif False` auto_trader.py:4431 = 죽음 확정 / quick_exit config `enabled:false` = 이중방어. 둘은 inert 유지)

---

## C. 진행중 관측 (자동 누적 — 코드 작업 아님)

### C-1. early variant shadow 2주 관측 (VPS 자동)
- VPS nightly 18:00 ③-2가 매일 자동 누적 중(6/16~). 노트북 꺼도 VPS 24/7이라 계속됨.
- 현재 통합 시계열(6/4~6/17, VPS 정본). 만기 ~6/30경 → 사장님 flip 결정(early를 C타입 추격 대체로 매수 연결?).
- 관측 지표: strict vs early forward 우위 / pos20 단조성(미확정·역설 구간 있음) / 거래량 구간 / would_stop 빈도.
- ★ **caveat(판정 시 필수 반영)**: `would_stop_day` = D0종가 대비 -3% 저가 도달 프록시 ≠ 고점 트레일링 -3%. 트레일링 손절빈도로 오독하면 과대평가.
- ★ 관측 없이 flip 금지(5/31·5/26 사고 교훈).
- 분석 시: 노트북은 sync 제외라 drift → **VPS에서 scp pull 먼저**.

### C-2. ma gate 승격 후보
- 이평선(20일선 봉우리돌파·정배열) 매수 게이트 승격 후보. 6/12 코호트 forward 관측 후 결정. (project_ma_gate_upgrade_candidate_6_13)

### C-3. 매매일지
- 코호트 D+1 부분판정 매매일지 양식(docs/01-plan/daily_trade_journal_format_6_13.md)으로 작성.

---

## D. 슬러지 정리 (낮음·위험 0)
- engine/risk/구 strategies(trading_engine·portfolio·order_manager·risk_manager·position_sizer·ma_crossover·volume_spike 등) = test만 import, 구 PyQt 아키텍처 잔재. `/deprecated` 이동 or 명시 주석. git history 보존.

---

## 안전 체크(매 작업 전후)
- 봇 OFF 3중게이트(AUTO_TRADE_DISABLED·MORNING_AUTO_BUY_DISABLED·kill_switch) 유지
- 코드 변경 = 4-Tier 의무(code-analyzer→grep/AST→회귀→Codex) + pre-commit RULE
- SAJANG 단일진실(하드코딩 금지) / picks 불변 / 매도 무손상
- ★ source='manual_president'는 매도 보호 비대상(의도적) — 봇 가동 시 자동매도 대상임을 사장님께 재확인
