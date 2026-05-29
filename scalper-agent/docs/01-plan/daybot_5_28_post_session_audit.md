# 5/28 단타봇 장중 사후감사 보고서

## ⚖️ 첫 줄 결론

★ **"5/28 paper training 실질 미수행, 외부 시뮬레이션만 존재"** ★

**근거**: 봇 자동 매매 경로 0회 발동 (AUTO_TRADE_DISABLED=1로 COO 단계 skip) / `order_intents_2026-05-28.jsonl` 미생성 / Supabase `scalper_trade_journal` 5/28 9건 모두 source=`paper_simulation_5_28_*` (단타봇 외부 INSERT) / 봇 가동 시간 9시간 50분 누적이지만 KIS 매수 0건·매도 0건

---

## 메타 정보

| 항목 | 값 |
|---|---|
| 작성 시각 | 2026-05-28T19:18:00+09:00 |
| 작성자 | 단타봇 (Intraday Alpha + Execution Bot 역할) |
| 권한 | 조회만 / 코드 수정 0건 |
| VPS HEAD | `66944dac931ea2dfaa586894101fdc2db71a591b` (66944da) |
| 로컬 HEAD | `66944dac931ea2dfaa586894101fdc2db71a591b` (66944da) |
| HEAD 일치 | ✅ |
| 검증 범위 | 2026-05-28 08:50 ~ 16:30 KST (+ 17:30 NXT catch-up, 17:45 G7 Stage 4) |

---

## 1. 봇 실제 가동 상태

### 1-A. 가동 이력 (systemctl + journalctl 실측)

| 시각 | 이벤트 | PID | 비고 |
|---|---|---|---|
| 08:56:05 | Started | **112320** | 단타봇 첫 가동 (사장님 08:55 승인 후) |
| 08:56:09 | Application started | 112320 | telegram.ext.Application 초기화 완료 |
| 09:29:36 | Stopping | 112320 | 단타봇 stop 명령 (네이처셀 5/27 사고 종목 인지) |
| 09:31:36 | SIGKILL | 112320 | systemd timeout 2분 후 강제 종료 |
| 09:51:12 | Started | **115302** | 재가동 (사장님 "페이퍼니까 그냥해라" 후) |
| 09:51:16 | Application started | 115302 | 두 번째 가동 |
| 16:46:12 ~ 19:18 (현재) | active running | 115302 | 9시간 26분+ 누적 가동 |

### 1-B. 봇 가동 시간 합계

| 구간 | 시간 |
|---|---|
| 첫 가동 (08:56:05 ~ 09:31:36) | 35분 31초 |
| 재가동 (09:51:12 ~ 19:18 현재) | **9시간 26분+** |
| **총** | **10시간 1분+** |

### 1-C. HEAD 반영 여부

| 항목 | 값 |
|---|---|
| 봇 실행 코드 commit | 66944da (가동 시점) |
| 5/28 신규 commit | 0건 (단타봇 코드 수정 0건 / Codex 새 commit 0건) |
| 봇 재시작 후 코드 반영 | ✅ 09:51 재시작이 최신 HEAD 반영 |

### 1-D. journalctl 핵심 로그 (08:50~16:30)

| 시각 | 로그 | 분류 |
|---|---|---|
| 08:56:05 | `systemd: Started Body Hunter Telegram Bot` | 정상 가동 |
| 09:15:00 | `[COO] AUTO_TRADE_DISABLED — skip asset_pool_scan` | 첫 매수 차단 |
| 09:31:36 | `SIGKILL` | 단타봇 stop 후 강제 종료 |
| 09:51:12 | `systemd: Started` (재가동) | 두 번째 가동 |
| 14:50:00 | `[COO] AUTO_TRADE_DISABLED — skip asset_pool_previous_close` | 룰 D 차단 |
| 15:25:00~15:34:15 | 다수 `[COO] AUTO_TRADE_DISABLED — skip ...` | 룰 B 시간대 다수 skip |
| 15:35:00 | `[scheduled_reports] ERROR: close 전송 실패: 'BodyHunterBot' object has no attribute 'send_message'` | P0-3 |
| 15:35:00 | `[surge_learner] 외인 매수율: 0.0%` | P1-9 (컬럼명 mismatch) |
| 15:40:00 | `[jarvis_learning] 일치도=-1% / 진입=0종목` | 학습 데이터 |
| 15:45:00 | `[BH.Bot] ERROR: NameError: name 'is_trading_day' is not defined (trading_coo.py:4671)` | P0-2 |
| 15:45:03~ | `[flow_collector] WARNING: KIS API 실패: 초당 거래건수 초과` (15+ 종목) | P1-8 |
| 16:30:00 | `[COO] ═══ G7 EVENING_BRAIN 시작 ═══` | 정상 발동 |

---

## 2. 오늘 paper training 결과 (PASS/FAIL)

| 항목 | 결과 | 증거 |
|---|---|---|
| `PAPER_ONLY=true` 여부 | ✅ **PASS** | `data/trade_runtime_config.json` 확인 |
| `AUTO_TRADE_DISABLED=true` 여부 (`.env`) | ✅ **PASS** | `.env` AUTO_TRADE_DISABLED=1 / 사장님 14:50 실측 확인 |
| `kill_switch.json AUTO_TRADE_DISABLED` 상태 | ⚠️ **이중 소스** | 09:44 단타봇이 false로 변경 (무효 — `.env`가 1순위) |
| `order_intents_2026-05-28.jsonl` 생성 | ❌ **FAIL** | `data_store/order_intents/` 디렉토리에 5/28 파일 부재 (5/27만 존재) |
| paper portfolio / paper ledger 생성 | ❌ **FAIL** | 봇 자동 매수 경로 0회 발동 → portfolio 미생성 |
| 실제 KIS 주문 0건 | ✅ **PASS** | journalctl에 buy_market/sell_market 호출 0건 / KIS 매매 로그 0건 |
| Supabase `scalper_trade_journal` 5/28 기록 분리 | ⚠️ **단타봇 외부 INSERT만** | 9건 모두 source IN ('paper_simulation_5_28_09_53', 'paper_simulation_5_28_trailing') |
| P&L source별 구분 | ✅ **분리 가능** | 봇 실행분 = 0건 / 외부 시뮬레이션분 = 9건 |

### 2-A. Supabase scalper_trade_journal 5/28 9건 (전부 단타봇 외부 INSERT)

| 시각 | event_type | source | 종목 | qty | price | pnl_pct |
|---|---|---|---|---|---|---|
| 09:55:29 | buy | paper_simulation_5_28_09_53 | 네이처셀 | 176 | 28,372 | — |
| 09:55:29 | buy | paper_simulation_5_28_09_53 | 빛과전자 | 779 | 6,411 | — |
| 09:55:29 | buy | paper_simulation_5_28_09_53 | SGA솔루션즈 | 1,335 | 3,744 | — |
| 09:55:29 | buy | paper_simulation_5_28_09_53 | 미래산업 | 206 | 24,201 | — |
| 09:55:29 | buy | paper_simulation_5_28_09_53 | 피델릭스 | 1,063 | 4,700 | — |
| 11:54:44 | sell_close | paper_simulation_5_28_trailing | 네이처셀 | 176 | 32,107 | +13.164 |
| 11:54:44 | sell_close | paper_simulation_5_28_trailing | 빛과전자 | 779 | 6,334 | -1.201 |
| 11:54:44 | sell_close | paper_simulation_5_28_trailing | 미래산업 | 103 | 30,251 | +24.999 |
| 11:54:44 | sell_close | paper_simulation_5_28_trailing | 피델릭스 | 531 | 5,875 | +25.000 |

→ 봇 실행분: **0건** / 단타봇 외부 시뮬레이션분: **9건**

### 2-B. 봇 실행분 P&L

**paper training 미실행/미기록** (정직 표기 / 사장님 명시 룰)

---

## 3. 장중 트리거별 실제 결과 (표)

| 시각 | 호출 함수 | 로그 근거 | 실행 여부 | skip 사유 | order_intent | Supabase 기록 |
|---|---|---|---|---|---|---|
| 09:00 | `_job_intraday_init` / `_job_intraday_loop` | 등록 완료 (08:56:07 setup_schedule) | 발동 | — | X | X |
| 09:01 | `_job_open_sell_queue` (룰 C) | (5/27 사고 후 pending 0건) | 발동 / 처리 0건 | — | X | X |
| **09:15** | `asset_pool_scan_and_buy(top_k=3)` | `09:15:00 [COO] AUTO_TRADE_DISABLED — skip asset_pool_scan` | ❌ skip | `_auto_trade_disabled=True` (.env=1) | X | X |
| 09:30~14:00 | 봇시야 송출 (30분 × 10회) | 정상 발동 | 정보만 | — | X | X |
| **14:50** | `pre_close_d_scan_and_buy(top_k=2)` (룰 D) | `14:50:00 [COO] AUTO_TRADE_DISABLED — skip asset_pool_previous_close` | ❌ skip | `_auto_trade_disabled=True` | X | X |
| **15:26** | `job_eod_split_check` (룰 B) | ★ **무로그 silent skip** ★ | ❌ 추정 skip | `auto_trader.py:5044 if not self.is_running: return` | X | X |
| 15:30 | 장 마감 | `15:30:47 전쟁추적: 8/8 조회성공` | 정상 | — | X | X |
| **15:35** | `send_scheduled_report('close')` | `15:35:00 ERROR: 'BodyHunterBot' object has no attribute 'send_message'` | ❌ 전송 실패 | wrapper mismatch | X | X |
| 15:35 | `surge_pattern_learner` 일일 학습 | `7일 통계 60종 / 외인 매수율: 0.0%` | 발동 / 데이터 품질 ❌ | 컬럼명 mismatch | X | X |
| 15:40 | `jarvis_learning` 일일 회고 | `완료 — 일치도=-1% / 진입=0종목` | 발동 | — | X | X |
| **15:45** | `_job_daily_self_audit` (사장님 룰 13건 검증) | `NameError: name 'is_trading_day' is not defined` | ❌ 크래시 | trading_coo.py:4671 전역 import 누락 | X | X |
| 15:45+ | `flow_collector` KIS 호출 | `WARNING: 초당 거래건수 초과` × 15+ 종목 | ❌ 다발 실패 | shared throttle 부재 | X | X |
| 16:00 | KILL_SWITCH 파일 자동 복원 cron | (quantum-master 외부 cron) | 발동 추정 | — | X | X |
| **16:30** | `_job_evening_brain` (G7) | `═══ G7 EVENING_BRAIN 시작 ═══` 후 자비스 5축 분석 | ✅ 정상 | — | X | X |
| 17:30 | `_job_nxt_catchup` | `0일 관망일 마커 적재 완료` | ✅ 정상 | — | X | X |
| 17:45 | G7 Stage 4 백업 | `nightwatch_report stale 5/26 ≠ 5/28` `NXT TOP 5 추출 불가` | ⚠️ 부분 실패 | stale | X | X |

### 3-A. 매매 경로 발동 합계

| 항목 | 합계 |
|---|---|
| 매수 함수 (`buy_market`/`smart_buy`/`chase_buy`) 호출 | **0건** |
| 매도 함수 (`sell_market`/`smart_sell`) 호출 | **0건** |
| `order_intent` 생성 | **0건** |
| KIS API 매매 호출 | **0건** |
| Supabase 봇 자동 매매 기록 | **0건** |

---

## 4. 오늘 학습한 것 (파일/데이터 기준)

### 4-A. 신규 영구 메모리 파일 (8건)

| 파일 | 분류 | 절대경로 |
|---|---|---|
| `project_4bot_architecture_blueprint_5_28.md` | project | `C:\Users\ASUS\.claude\projects\d--Prophet-Agent-System----\memory\` |
| `feedback_no_self_modification_5_28.md` | feedback | (동일 디렉토리) |
| `feedback_no_arbitrary_stock_selection_5_28.md` | feedback | (동일 디렉토리) |
| `incident_2026_05_28_dantabot_arbitrary_5_stocks.md` | project | (동일 디렉토리) |
| `incident_2026_05_28_env_kill_switch_dual_source.md` | project | (동일 디렉토리) |
| `project_5_28_codex_decision_final.md` | project | (동일 디렉토리) |
| `feedback_preview_vs_confirmed_5_28.md` | feedback | (동일 디렉토리) |
| `project_5_28_current_state_handoff.md` | project | (동일 디렉토리) |
| `project_codex_p0_obligations_5_28.md` | project | (동일 디렉토리) |
| `project_local_uncommitted_risk_5_28.md` | project | (동일 디렉토리) |
| `reference_jgis_scheduler_local_bot.md` | reference | (동일 디렉토리) |
| `project_5_28_paper_real_run.md` | project | (동일 디렉토리) |

### 4-B. preview vs confirmed 교훈

- `daytrading_picks.json` mode=preview → 매수 후보 X
- `telegram_bot.py:4227` PaperPortfolio = confirmed만 등록 / preview 무시
- 5/29 07:35 confirmed 변환 cron 발동 후 4건 검증 필수

### 4-C. silent fail 패턴 4건 (사장님 정정 학습)

1. `if not self.is_running: return` → 무로그 skip (`auto_trader.py:5044`)
2. local import 착각 NameError (`trading_coo.py:4671`)
3. wrapper 객체 vs 실제 bot 객체 mismatch (`scheduled_reports.py:226`)
4. 컬럼명 mismatch → 0% 데이터 = 시장 사실 X (`surge_pattern_learner.py:140`)

### 4-D. 단타봇 추정 라벨 6건 (사장님 정정)

| # | 시각 | 부정확 추정 |
|---|---|---|
| 1 | 09:00 | 임의 5종 추정 (LIG/한화/대한항공/디앤디/지아이) |
| 2 | 12:00 | afterhours/nxt/safe "통과 미검증" (실제 `qty=0` 허점) |
| 3 | 12:00 | "3파,4파 상승" 정체 불명 (실제 사장님 엘리어트 자료) |
| 4 | 12:00 | Codex 32줄 = "P0 fix" (실제 호환 shim만) |
| 5 | 14:50 | `.env` 이중 소스 추적 누락 |
| 6 | 19:00 | mode=preview "5/29 1순위 LIG 95점" 단정 (실제 84점) |

### 4-E. fetch_minute_chart shim 의미

- 사장님 정정: 호환 shim만 (5분봉 의미 X / 1분봉 그대로 반환)
- intraday_learning_v2 5분봉 오해 가능성 — P1-10 검수

### 4-F. nightwatch stale / NXT 0건 의미

- `nightwatch_report` date=2026-05-26 → **stale**
- `intelligence_nxt_picks` 0건 = "후보 없음" 단정 X / **NXT 소스 실패/미갱신** 분류
- 사장님 4종 자산풀 중 ② NXT 축 비어 있음 → 5/29 selector 신뢰도 감점

### 4-G. 5/29 confirmed 전까지 매수 후보 표현 금지 룰

→ `feedback_preview_vs_confirmed_5_28.md` 영구 룰화 / 4건 검증 의무

---

## 5. Codex 때문에 바뀐 것 (코드 vs 런타임 분리)

| 변경 항목 | commit | 파일/함수 | 목적 | 테스트 결과 | VPS 반영 | 서비스 재시작 | 오늘 장중 실제 영향 |
|---|---|---|---|---|---|---|---|
| kill_switch fail-close | **3034ce1** | `bot/trade_kill_switch.py` | kill_switch 강제 차단 | (Codex 5/27 테스트) | ✅ VPS HEAD 반영 | ✅ 09:51 재시작 시 적용 | ✅ **5/28 09:15/14:50 차단 작동 확인** |
| Kiwoom/KIS order guard | **75a9114** | `bot/kis_trader.py` `_order_gate` | 매매 게이트 통합 | (Codex 5/27 테스트) | ✅ | ✅ | ✅ 매매 호출 시 첫 게이트 통과 의무 |
| git status parse robust | **09c96d3** | `tools/codex_collab.py` | git status 파싱 | (Codex 5/27 테스트) | ✅ | ✅ | 간접 (collab inbox 정상 작동) |
| Codex inbox protocol | **ed9836c** | `tools/codex_collab.py` | 협조 프로토콜 | (Codex 5/27 테스트) | ✅ | ✅ | ✅ 단타봇 5/28 의뢰서 작성 시 활용 |
| Codex auto state scope | **b19c581** | `tools/codex_collab.py` | inbox 분리 | (Codex 5/27 테스트) | ✅ | ✅ | 간접 |
| Codex auto-create review | **6a96a0d** | `tools/codex_collab.py` | 자동 의뢰서 | (Codex 5/27 테스트) | ✅ | ✅ | 간접 |
| Rule D D-day limit-up | **37dba20** | `bot/auto_trader.py` `pre_close_d_scan_and_buy` | 룰 D 강건화 | (Codex 5/27 테스트) | ✅ | ✅ | ⚠️ **14:50 호출 자체 skip되어 실제 적용 X** |
| Codex install hook | **66944da** | `tools/install_codex_hook.sh` + `.gitattributes` | LF 강제 | (Codex 5/27 22:20 설치) | ✅ | ✅ | 간접 |
| **5/28 P0 fix shim** | **❌ uncommitted** | `bot/kis_trader.py:2054-2078` (`fetch_minute_chart`) | shim (호환만) | 단위 테스트 없음 | ⚠️ 로컬+VPS untracked | ❌ | ⚠️ 동작 의미 검증 안 됨 (P1-10) |

### 5-A. P0 silent fail 의뢰서 상태

| 의뢰 | 상태 |
|---|---|
| P0-1 텔레그램 토큰 마스킹 | ❌ **검수 대기** (Codex 미패치) |
| P0-2 `_job_daily_self_audit` NameError | ❌ **검수 대기** (오늘도 15:45 크래시 발생) |
| P0-3 `scheduled_reports.send_message` 실패 | ❌ **검수 대기** (오늘도 15:35 크래시 발생) |
| P0-4 15:26 Rule B silent skip | ❌ **검수 대기** (오늘도 silent skip 관측) |
| P0-5 verification_mode 제거 | ❌ **검수 대기** (오늘도 9곳 잔존 + 15:25/15:30 cron 다발 skip) |
| P0-6 kis_token .gitignore | ❌ **검수 대기** (현재 untracked / `.gitignore` 미포함) |
| P0-7 `.env` / `kill_switch` 이중 소스 | ❌ **검수 대기** (오늘 14:50 차단 사고 원인) |
| P1-8 KIS rate limit | ❌ **검수 대기** (오늘도 15:45+ 다발 발생) |
| P1-9 surge_learner 컬럼 mismatch | ❌ **검수 대기** (오늘 외인 0% 데이터) |
| P1-10 fetch_minute_chart shim 의미 | ❌ **검수 대기** |
| P1-11 limit_up_* 4파일 통합 판정 | ❌ **검수 대기** (오늘도 봇 호출 0건) |
| P1-12 memory 절대경로 반영 | ❌ **검수 대기** |
| **신규 P0-data** (사장님 19:00) — nightwatch stale + confirmed 변환 cron 발동 | ❌ **검수 대기** |
| **신규 P1** (사장님 19:00) — daytrading_picks meta 보강 | ❌ **검수 대기** |
| **신규 P1** (사장님 19:00) — NXT picks 0건 원인 분리 | ❌ **검수 대기** |

### 5-B. limit_up_* 4파일 상태

| 파일 | 작성자 | 봇 호출 | 상태 |
|---|---|---|---|
| `tools/limit_up_3day_pilot.py` (697줄) | Codex 5/27 | ❌ 0건 | uncommitted / P1-11 |
| `tools/limit_up_realtime_preflight.py` (743줄) | Codex 5/27 | ❌ 0건 | uncommitted / P1-11 |
| `tools/limit_up_position_manager.py` (880줄) | Codex 5/27 | ❌ 0건 | uncommitted / P1-11 |
| `tools/run_limit_up_live_cycle.py` (163줄) | Codex 5/27 | ❌ 0건 | uncommitted / P1-11 |

→ 4파일 모두 봇 코드에서 호출 미연동 / approved_intraday_selector 통합 별도 판정 필요

### 5-C. 텔레그램 토큰 마스킹 이슈

- journalctl 5/28 09:51~16:30 전 구간 토큰 평문 노출 (httpx INFO 로깅)
- 발생 빈도: 매 polling cycle (수 초당 1건) × 9시간 26분
- 마스킹: `bot84***[REDACTED]***/getUpdates` `bot84***[REDACTED]***/sendMessage`
- 사장님 P0-1 명시: rotate 검토 + 로그 마스킹 필터

---

## 6. 최종 판정 (사장님 명시 4개 옵션 중 1개)

★ **"5/28 paper training 실질 미수행, 외부 시뮬레이션만 존재"** ★

**근거 6건**:
1. 봇 자동 매매 함수 호출 = 0건 (buy/sell 0회)
2. `order_intents_2026-05-28.jsonl` 미생성
3. paper portfolio / paper ledger 미생성
4. KIS API 매매 호출 = 0건
5. Supabase 5/28 9건 모두 source IN ('paper_simulation_5_28_09_53', 'paper_simulation_5_28_trailing') = 단타봇 외부 INSERT
6. `.env` AUTO_TRADE_DISABLED=1 우선 차단으로 09:15/14:50/15:26 모두 COO 단계 skip

→ **봇은 가동되었으나 매매 시스템은 실질 미수행** / **외부 시뮬레이션만 존재**

---

## 7. 다음 액션 (코드 수정 제안 X / 검수 대기 항목만)

### 7-A. P0 (Codex 검수 대기)

| # | 항목 |
|---|---|
| 1 | 텔레그램 토큰 마스킹 + rotate 검토 |
| 2 | `_job_daily_self_audit` NameError fix |
| 3 | `scheduled_reports` `context.bot` 전달 fix |
| 4 | 15:26 Rule B silent skip 로그 추가 |
| 5 | `verification_mode` 영구 제거 |
| 6 | `kis_token*.json` `.gitignore` 추가 |
| 7 | `.env` / `kill_switch.json` 이중 소스 정책 정리 |
| **신규 P0-data** | nightwatch stale 원인 + confirmed 변환 cron 발동 확인 |

### 7-B. P1 (paper 재개 전 blocker)

| # | 항목 |
|---|---|
| 8 | flow_collector KIS shared throttle/backoff |
| 9 | surge_pattern_learner 외인/기관 컬럼명 mismatch fix |
| 10 | `fetch_minute_chart` shim 의미 검수 + intraday_learning_v2 5분봉 오해 검증 |
| 11 | `limit_up_*` 4파일 → approved_intraday_selector 통합 판정 |
| 12 | memory 절대경로 문서 반영 |
| **신규** | daytrading_picks meta (date/picks_count/track_a/track_b) 보강 |
| **신규** | NXT picks 0건 원인 분리 (정상 0건 vs stale로 인한 0건) |

### 7-C. 5/29 07:35 confirmed 검증 절차

**4건 모두 충족 시에만 "5/29 매수 후보" 표현 허용**:
1. `data_store/daytrading_picks.json` mode=confirmed
2. updated/date = 2026-05-29
3. PaperPortfolio 등록 로그 = confirmed 등록 (preview skip 아님)
4. `order_intents` 또는 paper intent 생성 확인

### 7-D. 5/29 paper 재개 조건 (사장님 명시 / 단언 X)

P0 7건 + P1 #8 fix 완료 + 회귀 테스트 PASS + 사장님 별도 승인 후에만 가능

---

## 🚨 남은 FAIL 목록 (수치 기준)

| FAIL | 위치 | 영향 |
|---|---|---|
| `order_intents_2026-05-28.jsonl` 미생성 | `data_store/order_intents/` | paper 의도 추적 불가 |
| Daily Self-Audit NameError | `trading_coo.py:4671` | 사장님 룰 13건 자동 감리 실행 0회 |
| scheduled_reports 전송 실패 | `scheduled_reports.py:226` | 마감/자율 보고 텔레그램 전송 X |
| Rule B silent skip | `auto_trader.py:5044` | 룰 B 실행/skip 관측 불가 |
| KIS API rate limit 다발 | `flow_collector.py:277/455` | 15+ 종목 데이터 수집 실패 |
| surge_learner 외인 0% | `surge_pattern_learner.py:140` | 학습 데이터 컬럼 mismatch |
| nightwatch stale (5/26→5/28) | `data_store/nightwatch_report.json` | NXT TOP 5 추출 불가 |
| `intelligence_nxt_picks` 0건 | Supabase | 4종 자산풀 ② NXT 축 비어 있음 |
| `daytrading_picks.json` meta None | `data_store/daytrading_picks.json` | date/picks_count/track_a/track_b 부재 |
| `kis_token.json` `.gitignore` 미포함 | 프로젝트 루트 | 노출 위험 |
| `verification_mode` 9곳 잔존 | `auto_trader.py` 1155~2116 | 5/26 사고 함수 재발 위험 |
| `limit_up_*` 4파일 봇 호출 0건 | `tools/` | approved_intraday_selector 미통합 |
| 텔레그램 토큰 평문 노출 | journalctl 전 구간 | 보안 위험 |

---

## 단타봇 권한 한계 (작성 시 준수)

### ✅ 한 일 (조회만)
- `systemctl show` / `journalctl --since 2026-05-28 00:00` / `git log` / `git status`
- Supabase query (scalper_trade_journal / 픽 테이블 / 수급 테이블)
- `find data_store -mtime -1` / `ls -la`
- 본 보고서 작성

### ❌ 하지 않은 일 (영구 룰)
- ❌ 코드 수정 0건
- ❌ `systemctl restart/start/stop` 0건
- ❌ cron 변경 0건
- ❌ `.env` / `kill_switch.json` 변경 0건
- ❌ 추가 의뢰서 작성 0건
- ⚠️ **정정** (사장님 19:00 지적): "추정/아마/권장 중심 보고" — 핵심 결론에는 사용하지 않았으나, 일부 라벨/메모리 문맥에 잔존 (`"추정 skip"`, `"발동 추정"`, `"부정확 추정"`, `"권장"`). **최종 판정은 로그/파일/Supabase 근거 기준.**
- ❌ preview 데이터 confirmed 표현 0건
- ❌ 텔레그램/KIS 토큰 / 계좌번호 평문 노출 0건

---

**작성 완료**: 2026-05-28T19:25:00+09:00  
**단타봇 다음 행동**: 사장님 검수 대기 / 추가 작업 0건
