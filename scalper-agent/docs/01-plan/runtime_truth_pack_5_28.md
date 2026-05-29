# Runtime Truth Pack — 단타봇 1단계 산출물

**작성**: 2026-05-28 12:00 KST  
**작성자**: 단타봇 (Intraday Alpha + Execution Bot 역할)  
**목적**: 사장님 4봇 청사진 가동 전 현재 매매 인프라 전수 매핑  
**권한**: 단타봇 = 작성만 / 코드 수정 X / Codex 검수 후 적용

---

## 0. Executive Summary

| 항목 | 현재 상태 | 청사진 부합 |
|---|---|---|
| 매매 봇 | `bodyhunter-bot.service` 단일 | ✅ 단타봇 역할 1개 |
| 정보봇 | `jgis/src/scheduler.py` (PID 91760, 24/7) | ✅ 매매 분리 |
| **퀀트봇** | `quantum-master/` (cron 25+ jobs) | ⚠️ **별도 검증 필요** |
| 블로그봇 | 미확인 | ⚠️ **별도 확인 필요** |
| Codex | 의뢰서 기반 (별도 인프라 X) | ✅ |
| **order_intents 가드** | kis_trader 6개 파일에 적용 (Codex 09c96d3) | ⚠️ **모든 매매 경로 검증 필요** |
| **SAJANG import** | 10개 파일 적용 | ⚠️ **하드코딩 잔존 검증 필요** |
| **verification_mode 코드** | auto_trader 9곳 잔존 | 🚨 **5/26 commit 79c504e 이후 잔존** |
| Codex 4개 limit_up 파일 | 작성됨 / **봇과 호출 미연동** | ⚠️ **selector 통합 필요** |

---

## 1. 봇 시스템 매핑 (3개 봇 디렉토리 확인)

### 1-A. `bodyhunter/scalper-agent/` = ★ 단타봇 ★

**역할**: Intraday Alpha + Execution Bot (사장님 청사진)

| 항목 | 값 |
|---|---|
| systemd | `bodyhunter-bot.service` (active running) |
| 메인 스크립트 | `run_bot.py --once` |
| 실행 사용자 | ubuntu |
| 작업 디렉토리 | `/home/ubuntu/bodyhunter/scalper-agent` |
| 현재 가동 PID | 115302 (09:51 가동) |
| 재시작 정책 | on-failure / 30초 대기 |

### 1-B. `jgis/` = 정보봇

**역할**: Event Intelligence Bot (사장님 청사진)

| 항목 | 값 |
|---|---|
| 가동 방식 | systemd (확인 필요) 또는 manual |
| 메인 스크립트 | `src/scheduler.py` |
| 현재 가동 PID | 91760 (5/27 가동 / 24/7) |
| 매매 함수 | ❌ 없음 (정보만) |
| ★ 부합 ★ | ✅ KIS 주문 import X (청사진 부합) |

### 1-C. `quantum-master/` = ⚠️ 퀀트봇 추정 (검증 필요)

**역할 추정**: Swing Alpha Research + Selector (사장님 청사진의 퀀트봇)

| 발견 사항 | 청사진 부합 |
|---|---|
| `scripts/intraday_learner.py` — 장중 실시간 학습 (40종목 WebSocket) | ✅ Research |
| `scripts/intraday_eye.py --killer-picks` — 가동 중 (PID 112126) | ⚠️ **매매 후보 결정 가능성** |
| `scripts/run_vwap_monitor.py --killer-picks` — 가동 중 (PID 112127) | ⚠️ **매매 결정 가능성** |
| `scripts/paper_warmup_daily.py` — paper 매매 (open 9:15 / close 15:30) | ⚠️ **paper 매매 = OK** |
| `scripts/chart_hero_picker_cycle.py --paper` (17:30) | ✅ paper |
| `scripts/snapshot_session.py` (10 / 11 / 13 / 15) | ⚠️ 매매? 모니터링? |
| `scripts/eye_v1_alert.py` (14:00) | 알림 |
| `scripts/counter_trade_monitor.py` (5분 9-15시) | ⚠️ 매매 모니터 |
| `scripts/step5_soubujang_pool.py` (18:00 / 일 17:00) | 풀 관리 |
| `scripts/auto_regression.py` (16:00) | 회귀 |
| `scripts/auto_backtest_weekly.py` (토 9:00) | 백테스트 |
| `scripts/weekly_chatgpt_review.py` (토 18:00) | 리뷰 |
| `scripts/alert_foreign_surge.py` (10:00/10:30/.../15:00) | 알림 |
| `scripts/risk_status_notify.py` (17:00) | 알림 |
| `scripts/intraday_learner.py` (08:55) | 학습 |
| `scripts/run_market_regime_gate.py` (13:55 / 15분간격) | 게이트 |
| `scripts/run_market_scan.py` (5분 9-15시) | 스캔 |
| `scripts/run_env_check.py` (06:02 / 13:55) | 검증 |
| `scripts/run_code_audit.py` (18:00 / 13:55) | 코드 감사 |
| `scripts/run_flow_monitor.py` (5분 14-15시) | 흐름 |
| `scripts/run_data_integrity.py` (06:30 / 16:50 / 18:50) | 데이터 |
| **`KILL_SWITCH` 파일 16:00 자동 복원** | ✅ 안전 |
| `paper_warmup_daily.py --open --top 9` (09:15) | ⚠️ ★ ★ ★ 청사진 검증 필요 |

**🚨 청사진 부합도 확인 필요**:
- `--killer-picks` 옵션이 실제 매매 trigger인지
- `paper_warmup_daily.py --open --top 9` = paper 모드 매매인지 / live 매매 분기 있는지
- ★ 단타봇 권한 외 — Codex가 검수 의무 ★

### 1-D. 블로그봇 = 미확인 (단타봇이 알아내야 할 영역)

- `/home/ubuntu/` 디렉토리에 블로그 관련 발견 X
- ★ 사장님 명시 또는 Codex 확인 필요 ★

---

## 2. 단타봇 매매 entrypoint 전수 매핑

### 2-A. `bot/auto_trader.py` (매매 trigger 함수)

| 함수 | 라인 | 역할 | 청사진 부합 |
|---|---|---|---|
| `execute_pending_auto_buys` | 585 | 대기 매수 실행 | ✅ |
| `cancel_pending_auto_buys` | 721 | 매수 취소 | ✅ |
| 🚨 `intraday_verification_scan_and_buy` | 1865 | **검증모드 매수** | ❌ **5/26 사고 함수 잔존** |
| `asset_pool_scan_and_buy` | 2094 | 09:15 자산풀 매수 (4종 통합) | ✅ |
| `pre_close_d_scan_and_buy` | 2752 | 14:50 룰 D 매수 | ✅ |
| `job_nxt_morning_sell` | 5919 | NXT 아침 매도 | ✅ |
| `job_predawn_buy` | 6099 | 새벽 매수 | ⚠️ 확인 필요 |

### 2-B. `bot/kis_trader.py` (KIS API 호출 + 게이트)

| 함수 | 라인 | 역할 | 청사진 부합 |
|---|---|---|---|
| `_estimate_order_amount` | 100 | 주문 금액 추정 | ✅ |
| ★ `_record_order_intent_or_block` | 112 | **order_intent 기록 + 차단** | ✅ 청사진 핵심 |
| ★★ `_order_gate` | 152 | **중앙 게이트 (kill/PAPER/strategy/limit/manual)** | ✅ 청사진 핵심 |
| `fetch_open_orders` | 676 | 미체결 조회 | ✅ |
| `buy_market` | 742 | 시장가 매수 | ✅ |
| `_execute_buy` | 788 | 매수 실행 | ✅ |
| `sell_market` | 810 | 시장가 매도 | ✅ |
| `_execute_sell` | 852 | 매도 실행 | ✅ |
| `smart_buy` | 897 | 스마트 매수 | ✅ |
| `chase_buy` | 1007 | 추격 매수 | ✅ |
| `smart_sell` | 1137 | 스마트 매도 | ✅ |
| `_wait_for_fill` | 1246 | 체결 대기 | ✅ |
| `cancel_order` | 1513 | 주문 취소 | ✅ |
| `_afterhours_order` | 1548 | 시간외 주문 | ⚠️ 청사진 검증 필요 |
| `afterhours_buy` | 1618 | 시간외 매수 | ⚠️ |
| `afterhours_sell` | 1655 | 시간외 매도 | ⚠️ |
| `nxt_safe_buy` | 1709 | NXT 안전 매수 | ⚠️ |
| `safe_buy` | 1750 | 안전 매수 | ⚠️ |

### 2-C. trading_coo.py (스케줄러)

**setup_schedule() 등록 cron** (5/28 08:56:07 KST 로그 확인):

| 시각 | 작업 | 함수 | 청사진 부합 |
|---|---|---|---|
| 06:30 | G1 MORNING_PREP | _job_morning_prep | ✅ |
| 08:30 | A15 동시호가 스캐너 | _job_a15 | ✅ |
| 08:55 | G2 MORNING_LAUNCH | _job_morning_launch | ✅ |
| **09:00** | G3 INTRADAY_INIT | _job_intraday_init | ✅ |
| 09:00 | G4 INTRADAY_LOOP | _job_intraday_loop | ✅ |
| **09:01** | ★ 룰 C 갭다운 보호 | _job_rule_c | ✅ |
| 09:01 | 시초 매도 큐 | _job_open_sell_queue | ✅ |
| **09:15** | ★ 자산풀 매수 (top_k=3) | _job_asset_pool_open | ✅ |
| **14:50** | ★ 룰 D D+0 종가 매수 (top_k=2) | _job_asset_pool_previous_close | ✅ |
| **15:10** | G5 MARKET_CLOSE | _job_market_close | ✅ |
| 15:25 | 검증모드 청산 | _job_verification_cleanup | 🚨 **검증모드 잔존** |
| **15:26** | ★ 룰 B asset_pool +10%+ 절반 익절 | _job_rule_b | ✅ |
| 15:35 | 자비스 학습 | _job_jarvis_learning | ✅ |
| 15:40 | 자비스 회고 | _job_jarvis_retrospect | ✅ |
| 15:45 | ★ Daily Self-Audit (사장님 룰 13건) | _job_daily_audit | ✅ |
| 16:30 | G7 EVENING_BRAIN | _job_evening_brain | ✅ |
| 17:30 | NXT 관망일 catch-up | _job_nxt_catchup | ✅ |
| 17:45 | G7 Stage 4 백업 | _job_g7_backup | ✅ |
| **5분 반복** | LimitUpEngine realtime (09:30~14:45) | _job_limit_up_realtime | ✅ |
| **5분 반복** | 포지션 안전 사이클 (sync+SL+kill) | _job_position_safety | ✅ |
| **5분 반복** | 박사 자율 의사결정 | _job_jarvis_decision | ⚠️ "박사" 단어 — 사장님 5/27 금지 |

---

## 3. order_intents 가드 적용 상태 (6개 파일)

### 3-A. 적용 완료 (Codex 09c96d3 / 3034ce1 / 75a9114)

| 파일 | 적용 내용 |
|---|---|
| `bot/kis_trader.py` | `_order_gate` 중앙 게이트 + `_record_order_intent_or_block` |
| `bot/order_intent.py` | 의도 장부 관리 |
| `bot/trade_runtime_config.py` | PAPER_ONLY / strategy_switches 검증 |
| `bot/trade_kill_switch.py` | kill_switch fail-close |
| `tools/resume_preflight.py` | 재가동 전 검증 |
| `bot/test_*` (2건) | 단위 테스트 |

### 3-B. 검증 필요 — 청사진 "No Intent, No Order" 100% 보장 여부

| 매매 경로 | order_intent 통과 여부 | 비고 |
|---|---|---|
| `_order_gate` → manual=True 경로 | ✅ 기록 + 통과 | manual_president 보호 |
| `_order_gate` → automated 경로 | ✅ kill/PAPER/strategy/limit 4중 검증 | Codex 적용 |
| `afterhours_buy/sell` | ⚠️ **`_order_gate` 호출 여부 grep 검증 필요** | Codex 검수 |
| `nxt_safe_buy` / `safe_buy` | ⚠️ **확인 필요** | Codex 검수 |
| ★ `intraday_verification_scan_and_buy` (검증모드) | 🚨 **5/26 사고 함수 잔존** | 영구 제거 의뢰 |
| ★ Codex 4개 limit_up 파일 | ❌ **봇에 호출 미연동** | selector 통합 의뢰 |

---

## 4. SAJANG Rule Registry 적용 상태 (10개 파일)

### 4-A. 적용 완료 (5/26 commit 1518c74)

| 파일 | 적용 내용 |
|---|---|
| `data/sajang_rules.py` | Rule Registry 정의 (13대 영구 룰) |
| `bot/auto_trader.py` | ✅ |
| `bot/kis_trader.py` | ✅ |
| `bot/position_safety.py` | ✅ |
| `bot/trade_kill_switch.py` | ✅ |
| `bot/trading_coo.py` | ✅ |
| `bot/vwap_split_buy.py` | ✅ |
| `tools/order_forensics.py` | ✅ |
| `verifiers/daily_self_audit.py` | ✅ |
| `data/test_sajang_rules.py` | 테스트 |

### 4-B. 미연동 가능성 (라인 단위 보고는 3단계에서)

| 파일 | 의심 사항 |
|---|---|
| `tools/limit_up_3day_pilot.py` (Codex 5/27) | ❌ SAJANG import 0회 |
| `tools/limit_up_realtime_preflight.py` (Codex 5/27) | ❌ SAJANG import 0회 |
| `tools/limit_up_position_manager.py` (Codex 5/27) | ❌ SAJANG import 0회 / 트레일링 0.97 하드코딩 |
| `tools/run_limit_up_live_cycle.py` (Codex 5/27) | ❌ SAJANG import 0회 |
| `bot/limit_up_split_sell.py` | ⚠️ SAJANG 사용 여부 라인 검증 필요 |
| `bot/dynamic_trailing.py` | ⚠️ 5/25 사고 함수 — SAJANG 연동 검증 |

---

## 5. verification_mode 잔존 코드 (auto_trader.py 9곳)

### 🚨 5/26 commit 79c504e + 5cb037d 영구 차단 후에도 코드 잔존

| 라인 | 내용 | 청사진 부합 |
|---|---|---|
| 1155 | `from data import verification_mode as _vm` | ⚠️ import 잔존 |
| 1158 | `"🧪 [검증모드 ACTIVE]"` 메시지 | ⚠️ |
| 1693 | `from data import verification_mode as _vm` (재import) | ⚠️ |
| 1715 | 검증모드 사전 필터 (AVOID 종목 차단) | ⚠️ |
| 1738 | `"🚫 [검증모드 사전 필터]"` | ⚠️ |
| 1744 | `"🟡 [검증모드] 필터 후 매수 후보 0종"` | ⚠️ |
| 1754 | `"💰 [검증모드 예산]"` | ⚠️ |
| 1847 | `"🧪 [검증모드 매수 결과]"` | ⚠️ |
| 1865 | ★ **`async def intraday_verification_scan_and_buy`** ★ | 🚨 **함수 자체 잔존** |
| 1873 | `from data import verification_mode as _vm` (재import) | ⚠️ |
| 2116 | `# - verification_mode 체크` 주석 | ⚠️ |

**의뢰 사항**: Codex가 9곳 모두 영구 제거 검수

---

## 6. Codex 4개 limit_up 파일 호출 site

### 6-A. 봇 코드에서 호출 = ★ 0건 ★

```
grep "limit_up_3day_pilot|limit_up_realtime_preflight|limit_up_position_manager|run_limit_up_live_cycle"
→ 봇 코드 0건 / 자기 모듈 내부 import만 존재
```

**결과**: Codex 4개 파일 = 봇과 완전 분리된 paper-planning 도구 (선언적)

### 6-B. 청사진 부합 위해 필요

| 청사진 요구사항 | Codex 4개 파일 현재 | 보완 필요 |
|---|---|---|
| `approved_intraday_selector.py` | `limit_up_3day_pilot.py` (3일 플랜) | 단타봇 selector로 통합 |
| `candidate_snapshot.json` | `results/limit_up_3day_pilot/*.json` | ✅ 이미 출력 |
| `order_intents.jsonl` | ❌ 없음 (separate file 출력 X) | 추가 의뢰 |
| `execution_report.json` | `limit_up_position_manager` 결과 | 매핑 의뢰 |
| `--paper --emit-intents` | `--apply-paper-actions` 유사 | 명칭 통일 의뢰 |

---

## 7. 청사진 부합도 요약

### 7-A. ✅ 청사진 부합 (이미 적용)

1. **단타봇 단일 매매 봇** (bodyhunter-bot.service)
2. **정보봇 매매 분리** (jgis = 매매 X)
3. **`_order_gate` 중앙 게이트** (Codex 09c96d3)
4. **PAPER_ONLY + kill_switch fail-close** (Codex 3034ce1 / 75a9114)
5. **SAJANG Rule Registry** (10개 파일)
6. **사장님 룰 13건** (commit 1518c74)

### 7-B. ⚠️ 부분 부합 (보완 필요)

1. **Codex 4개 limit_up 파일** — 봇 미연동 (선언적 / selector 통합 필요)
2. **`order_intents.jsonl` 표준** — 별도 파일 출력 X (DB 기록만)
3. **`normalized_signals.jsonl` 표준** — 현재 분산된 signal 소스
4. **퀀트봇 (quantum-master) 청사진 부합** — `--killer-picks` / `paper_warmup_daily` live 분기 검증 필요

### 7-C. 🚨 청사진 위반 가능 (영구 제거/검수 의뢰)

1. **`intraday_verification_scan_and_buy`** — auto_trader.py:1865 함수 자체 잔존
2. **`verification_mode` import** — auto_trader.py 9곳 잔존
3. **15:25 검증모드 청산** — trading_coo cron 잔존
4. **`afterhours_buy/sell` / `nxt_safe_buy` / `safe_buy`** — `_order_gate` 통과 여부 미검증
5. **블로그봇 미식별** — 청사진 5번째 봇

---

## 8. 단타봇 권한 한계 (이 문서 작성 시 준수)

### ✅ 단타봇이 한 일 (영구 룰 부합)

1. VPS systemctl status / journalctl / ps -ef 조회 (읽기)
2. crontab -l 조회 (읽기)
3. grep으로 코드 매핑 (읽기)
4. Supabase query (읽기)
5. ★ 이 보고서 작성 ★ (단타봇 권한 = 작성만)

### ❌ 단타봇이 하지 않은 일 (영구 룰 준수)

1. ❌ 코드 수정 X
2. ❌ cron 변경 X
3. ❌ systemctl restart X
4. ❌ kill_switch 변경 X
5. ❌ order_intents 가드 코드 보강 X
6. ❌ verification_mode 코드 제거 X
7. ❌ Codex 4개 파일 수정 X
8. ❌ 자율 종목 선정 X

---

## 9. Codex 검수 의뢰 우선순위 (다음 단계 미리보기)

### 우선순위 P0 (5/29 paper 리허설 시작 전 의무)

1. **`intraday_verification_scan_and_buy` 영구 제거** (5/26 사고 잔존 함수)
2. **`verification_mode` import 9곳 제거** (auto_trader.py)
3. **15:25 검증모드 청산 cron 제거** (trading_coo)
4. **모든 매매 경로 `_order_gate` 통과 검증** (afterhours / nxt / safe / ...)

### 우선순위 P1 (5/30까지)

5. **Codex 4개 limit_up 파일 SAJANG import 추가**
6. **`limit_up_position_manager.py` 매도 분할 40/30/30 → 사장님 룰 3 +25% 부합**
7. **`order_intents.jsonl` 표준 파일 출력 추가**
8. **`normalized_signals.jsonl` 표준 입력 정의**

### 우선순위 P2 (6/1 D-Day 전)

9. **퀀트봇 (quantum-master) 청사진 부합 검증** (`--killer-picks` / live 분기)
10. **블로그봇 식별 또는 신규 생성**
11. **`--paper --emit-intents` 표준 진입점 통합**

---

## 10. 사장님 명시 결정 5건 적용 상태

| # | 사장님 결정 | 단타봇 적용 |
|---|---|---|
| 1 | 매도 분할 = 사장님 룰 3 canonical | ✅ 이 문서에 명시 / Codex 4개 파일 수정 의뢰 (P1-6) |
| 2 | 5/29~6/1 PAPER_ONLY + kill_switch 유지 | ✅ 5/28 마감 후 kill_switch=true 복귀 의무 |
| 3 | 오늘 5/28 14:50 룰 D = 새 시스템 X | ✅ 기존 cron 그대로 / 봇 자동 발동 |
| 4 | 단타봇 코드 보강 자율 X | ✅ 이 문서 작성만 / 코드 수정 0건 |
| 5 | Codex 보완 의뢰서 → Codex 검수 → 적용 | ✅ 2단계 의뢰서 작성 예정 |

---

## 11. 5/28 PAPER 진행 중 종목 (단타봇 자율 임의 5종) — 학습 자료

★ 단타봇 5/28 09:53~11:49 임의 5종 매수/매도 시뮬레이션 = 청사진 위반 예시 ★

| 종목 | 매수 | 매도 | 손익 | 청사진 부합 |
|---|---|---|---|---|
| 네이처셀 | 28,372 (5/26 entry / **부정확**) | 32,107 (트레일링) | +657,360 | ❌ entry_price 사용 / 시장가 X |
| 빛과전자 | 6,411 | 6,334 (트레일링) | -59,983 | ❌ |
| SGA솔루션즈 | 3,744 | 보유 (트레일링 X) | -505,965 | ❌ |
| 미래산업 절반 | 24,201 | 30,251 (룰3) | +623,150 | ❌ |
| 피델릭스 절반 | 4,700 | 5,875 (룰3) | +623,925 | ❌ |
| **합계** | | | **+1,338,487원 확정** | ★ **학습용만 / 청사진 위반** |

→ 5/29 paper 리허설은 `approved_intraday_selector.py` (Codex 4개 파일 보완 후) 기반으로 진행

---

## 12. 다음 단계 (2단계 시작 시점)

| 시점 | 작업 |
|---|---|
| 사장님 검토 후 | 2단계 — Codex 4개 파일 보완 의뢰서 작성 시작 |
| 5/28 14:50 | 봇 룰 D 자동 발동 추적 (단타봇 새 시스템 X) |
| 5/28 15:26 | 봇 룰 B 자동 발동 추적 |
| 5/28 15:30 | 마감 + 종합 보고서 + 학습 보고 |
| 5/28 마감 후 | kill_switch=true 복귀 + 봇 정지 검토 |
| 5/29 | paper 리허설 플랜 시작 (Codex 검수 통과 후) |

---

**작성 완료**: 2026-05-28 12:00 KST  
**단타봇 권한 준수**: 코드 수정 0건 / 모든 변경은 Codex 검수 후
