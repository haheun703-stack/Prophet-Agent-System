# 6/11 (목) TODO — 6/10 세션 인계서

> 사장님 "못다한 일 내일 todo 저장, 내일 '업무시작하자' 하면 바로 진행" (6/10 밤).
> 다음 세션/단타봇은 이거 먼저 읽고 1순위부터 바로 착수.

---

## ✅ 어제(6/10) 완료 — 손대지 말 것 (검증 끝)
- **fill 증분화** (안전망 손상복구 + 병렬 4-way, commit 1b5044a) — rate 0·회귀 PASS·정상종목 누락 0·corrupt 0.
- **6/10 기초데이터 전체 업데이트** — 일봉3464·투자자수급·11주체·stock_data_daily·국적별(T+1). missing 187=거래정지/상폐/ETN.
- **정보봇 SSoT 교차검증** (commit ca253fd) — 단타봇 코스피 -4.52% = 정보봇 정정값 정확 일치. 정정전 +3.29%/+4.42% = 정보봇 6/9누락(yfinance hist 결손)→6/8 전일 오참조 규명. +7.63%=제3소스(단타봇 출처 아님).
- ★**야간 통합 자동화 VPS 메인 전환**★ (commit 28c9679 배포) — 사장님 "노트북 끄면 자동화 멈춤" 지적 → VPS 24/7로.
  - VPS git 배포 d45b644→28c9679 (런타임/kill_switch 보존·봇 재시작 없음).
  - **VPS cron `0 18 * * 1-5 run_nightly_pipeline.py`** (fill→step6→shadow→paper→수급→11주체→국적별).
  - VPS 통합 **7/7 실측 PASS**(32분) + step6 폴더 fix(stock_data_daily 3539).
  - 노트북 task 정리: NightlyPipeline/ShadowDaily **disable**, VPSSync **19:00 재활성**(VPS→노트북 수신).
  - shadow/paper는 sync 대상 아님 = 노트북 6/9 shadow 보존됨(확인).

---

## 🎯 오늘(6/11) 할 일

### 1순위 ★ VPS shadow 6/9 기록 합치기 (6/12 판정 직결)
- **문제**: VPS shadow가 6/10부터 시작 → **6/9(반도체/AI REVERSAL_D0 d0+10.9%·RS+7.69 / 금융 HOT_5D / 바이오 REVERSAL_D0 3건 + forward)가 VPS엔 없음.**
- 6/12 판정(6/9~6/12 forward 비교)은 6/9가 핵심인데 VPS 누락 → **노트북 sector_reversal_shadow.json(6/9~6/10)을 VPS로 합쳐야** 판정 완전.
- 방법: 노트북 shadow json → VPS `/home/ubuntu/bodyhunter/scalper-agent/data_store/sector_reversal_shadow.json` 병합(VPS 6/10과 멱등 merge, forward 안 깨지게). 봇 무관·관측 데이터.
- ★검증: VPS shadow 6/9부터 누적 + 6/9 forward(금융-1.52·반도체-5.21·바이오-2.44) 보존 확인★.

### 2순위 봇 16:03 구버전 중복수집 정리
- VPS bodyhunter 봇(run_bot.py --once systemd active)이 16:03 구버전 collect_all 수집 → 18:00 통합과 중복(18:00이 덮어 현재 무해, but 자원낭비 + 정보봇 참조 시 구버전 위험).
- run_bot 내부 스케줄러 확인 → 16:03 수집잡 비활성. ★봇 재시작은 안전윈도(08~09 / 20시+ / 23:30~06)★.

### 3순위 (시각) 6/11 18:00 VPS 첫 자동실행 + 19:00 sync 확인
- VPS cron 18:00 통합 7단계 자동 → `~/bodyhunter/logs/nightly.log`. **②step6도 폴더 있으니 정상 동기화되는지**(어제 fix).
- 노트북 19:00 VPSSync 수신 정상 — VPS 통합 32분(paper events 850s 변수)이라 19:00이 충분한지. fill 신규면 더 길 수 있음 → 늦으면 sync 시각 조정.

---

## 📊 6/12 판정 (HOT_5D vs REVERSAL_D0)
- 6/9~6/12 forward 비교. **1순위(6/9 합치기) 선결.**
- REVERSAL_D0가 HOT_5D보다 D+3/D+5 우위 + MAE 과도X + breadth/거래대금 유지 + ETF/외인 동행 → 다음주 보조점수 +5 (승인 + 4-Tier). 미충족 시 5일 HOT 유지.
- ★검증 없이 당일 추격 금지 (5/31 blow-off 교훈)★.

---

## ⏸️ 봇 가동 직전 별도 4-Tier 일괄 (오늘 손대지 말 것)
- 룰4 +25% 장중 분할 배선 / M-1 이중경로 / 6/3 P0 7건 / AUD-8 dynamic_tp→SAJANG / fill 2차 fallback 지연 추가.

## 🔭 향후 검토 (검증 후, 당장 X)
- **60/120 이평선 배열을 관측 레이어로** — 사장님 6/7 약점("60/120 이평·지수추세를 매수타이밍에 안 엮음") + 6/10 이동평균선 강의 검증. shadow 검증 후 반영(추세추종 부분유효, 단 단타 시간축과 다름).

---

**안전 불변식**: 봇 OFF · 실주문 0 · 게이트 8/8 · 매도 무손상 · SAJANG 단일진실 · No Intent No Order.
**git**: 28c9679 / **VPS**: 28c9679 (동기화). 노트북=VPS 메인 + 19:00 sync 수신.
**작성**: 2026-06-10 밤
