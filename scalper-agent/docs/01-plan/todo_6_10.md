# 6/10 (수) TODO — 6/9 세션 인계서

> 사장님 "못다한 부분 내일 todo에 저장하고 바로 진행" (6/9 밤).
> 다음 세션/단타봇은 이거 먼저 읽고 1순위부터 바로 착수.

---

## ✅ 어제(6/9) 완료 — 손대지 말 것 (검증 끝)
- **전체검수**: 안전 불변식 전부 PASS (봇 OFF 3중·주문 단일통로·신규 관측코드 read-only·매도 무손상·게이트 8/8). code-analyzer 2 + Explore 1 + 단타봇 직접 결판.
- **open gate 테마 가점 silent no-op fix** (commit 950b056): 후보 dict에 sector 키 없어 가점이 항상 무효였음 → 후보 code로 universe.json 한글 sector 역참조. `_adj` 0→3 입증. 4-Tier 통과.
- **sector_reversal_shadow OS 스케줄러 자동 적재** (commit d5ac5a0): `BodyHunter_ShadowDaily` 매일 15:50 + StartWhenAvailable(꺼져있으면 켤 때 따라잡기). 봇 OFF 유지. 4-Tier 통과.

---

## 🎯 오늘(6/10) 바로 진행

### 1순위 ★ fill 증분화 (기존 계획 todo_6_10_fill_incremental.md 그대로)
- 적응형: 정상 5일 증분 + 누락/CSV손상/날짜역전 종목만 20일 복구 fallback.
- 완료 후 누락 0 검증 + 4-Tier (★회귀 = 증분 == 전체 20일, keep=first, 005930 손상 교훈★).
- ★**fill이 선결**: 끝나야 step6 sync + shadow forward 충전됨.★ 순서 = fill → step6 → (shadow는 15:50 자동).

### 2순위 shadow 자동 적재 관측 (자동이라 손 안 댐, 확인만)
- 15:50 `BodyHunter_ShadowDaily` 자동 실행 → 6/10 D0 기록 확인 (`logs/shadow_20260610.log`).
- fill 후 `update_forward`가 6/9 record의 forward_d1 채우는지 확인.
- 누적 6/9~6/12 → **6/12 판정** (아래).

### 3순위 open gate _adj 확인
- ★봇 09:15 open 경로가 실행돼야 `[open_gate] 테마 가점/감점 _adj N종` 로그가 나옴. **봇 OFF면 안 나옴.**★
- 코드 fix는 950b056로 완료(시뮬 _adj=3 입증). 실작동 로그는 봇 가동/드라이런 시 확인.

---

## ⏸️ 보류 — 봇 가동 직전 별도 4-Tier 일괄 (오늘 손대지 말 것)
- **룰4 +25% 장중 분할 배선**: `should_trigger_split` 프로덕션 호출 0건(데모/test만). 매도 behavior 변경 = 사장님 승인 + 4-Tier. (룰 B 15:26 EOD는 이미 연결·라이브)
- **M-1 이중경로**: 봇 켜면 OS Task(15:50) + 봇 내부 `_job_sector_reversal_shadow`(15:50) 동시 실행 → 멱등이라 손상0이나 둘 중 하나 비활성(OS task disable or 봇 job skip) 필요.
- **6/3 P0 7건**: 상한가분할·dedupe·주문ledger·실계좌·jarvis TP·데이터게이트·AUD-8 dynamic_tp→SAJANG 통일.

---

## 📊 6/12 판정 (HOT_5D vs REVERSAL_D0)
- REVERSAL_D0가 HOT_5D보다 D+3/D+5 우위 + MAE 과도X + breadth/거래대금 유지 + ETF/외인 동행 → **다음주 실전 보조점수 +5** (승인 + 4-Tier). 미충족 시 5일 HOT 유지·shadow 기각.
- ★검증 없이 당일 추격 금지(5/31 blow-off 교훈).★

---

**안전 불변식 (항상)**: 봇 OFF · 실주문 0 · 게이트 8/8 · 매도 무손상 · SAJANG 단일진실 · No Intent No Order.
**git**: d5ac5a0 동기화.
