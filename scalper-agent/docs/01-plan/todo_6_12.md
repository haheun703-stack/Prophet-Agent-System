# 6/12 (금) TODO — 6/11 세션 인계서

> 6/12 = ★판정일(shadow 6/9~12 forward) + F1 첫 가동일★. "업무시작하자" 하면 이 순서로.

---

## ✅ 어제(6/11) 완료 — 손대지 말 것

- **VPS shadow 6/9 병합** — dry-run→승인→병합(2→5건), 검증 7/7. 6/12 판정 데이터 = VPS 단일진실.
- **16:03 규명 → A.현상태유지(사장님)** — 정체=G6 DATA_PIPELINE(15:40~16:06 일봉수집). G6→G7 의존이라 비활성 불가. *비고였던 "봇 옛코드"는 저녁 F1 배포 재시작으로 자연 해소됨.*
- **Fable5 외부조언 항목별 판정** — `docs/03-analysis/external_advice_vs_impl_6_11.md` (이미있음/새무기/충돌/E2 3-way 설계).
- **F1 외인 장중 잠정 로거** (ccaca41) — 구현+4-Tier 전부 PASS + **VPS 배포 완료(20:01, HEAD 5b61c2e, PID 91563, 검증 6/6)**. 6/12 11:00 첫 가동.
- **VPSSync 보강** (954d06e) — flow_market(11주체)+foreign_f1 sync 추가, 11주체 6/11 수신 실측.
- **pension_scan 규명** (75e514d+5b61c2e) — 단타봇 잡 무죄(업로드 3일 200 OK·date=6/9 고정 upsert가 원인). 진범=**pykrx KRX 로그인 의무화**(실호출 0행 확정) → 정보봇 DB 6/9 멈춤. 부활경로=KRX 계정→.env KRX_ID/PW. 퀀트봇 인계서 푸시.
- **VPS 18:00 nightly 첫 자동실행 7/7 성공**(25분) + 19:00 VPSSync 첫 자동(Result 0).

---

## 🎯 오늘(6/12) 할 일

### 1순위 ★ 5/21 사후분석 (판정 전 마감 — E2 3-way 가설 직접증거)
- 5/20~21 VPS 로그/ledger에서 대한전선·SKC·제룡전기·산일전기 "4일 도달" **청산 시점 손익**(매수가 대비) 발굴.
- +1.5%(=+0.5R) 이상이었나? → 이상이면 "조건부 E2면 5/21 사고 안 났다" 직접 증거 = 3-way ③ 강화.
- read-only. 대조표 「5/21 사후분석」 섹션에 결과 기입.

### 2순위 ★★ 6/12 판정 — sector shadow HOT_5D vs REVERSAL_D0 (사장님과 함께)
- 데이터: **VPS** `data_store/sector_reversal_shadow.json` (6/9 3건은 forward_d3가 오늘 종가로 충전됨 — 18:00 nightly 후 완전).
- 기준(6/9 사장님): REVERSAL_D0가 HOT_5D보다 ①D+3/D+5 우위 ②MAE 과도X ③breadth/거래대금 유지 ④ETF/외인 동행 → 다음주 보조점수 +5(승인+4-Tier). 미충족=5일HOT 유지.
- ★검증 없이 당일 추격 금지(5/31)★. paper 3-Type 1차 판정(6/4~6/12, KEEP/DROP/TUNE/ESCALATE)도 같은 날 — 6/8 첫 평일장부 라벨(지형도)과 forward 연결 확인.

### 3순위 ★ F1 첫 가동 관측 (자동 — 확인만)
- 11:00 select(+3%↑상위30+paper후보) → 13:00/14:30/15:30 스냅샷 → 16:05 finalize. VPS `data_store/foreign_f1/f1_2026-06-12.json`.
- ★데이터 전제 최종판정 = preflight `with_foreign` 수치★ (18:00 nightly ⑧ 로그 또는 수동 `python tools/run_f1_forward_preflight.py`).
  - with_foreign > 0 = 장중 외인 잠정 충전됨 → F1 성립 ✅
  - = 0 → KIS TR이 장중 당일행 미제공 → 스냅샷 시각/TR 교정 필요(INCOMPLETE라 손실 0).
- 11:00 잡 소요시간도 관찰(M2: 종목수×4스냅샷 API 직렬).

### 4순위 (시각) 18:00 nightly **8단계** 첫 실행 — ⑧ F1 forward+preflight 포함 확인.

---

## ⏸️ 대기/사장님 결정
- **KRX 비밀번호 변경** (★6/11 20:10 정정: 계정/키는 .env에 이미 있음. 진범=KRX 비번 만료 "패스워드 변경 필요"★) — KRX 사이트에서 비번 변경 → VPS .env 2곳(quantum-master·bodyhunter) `KRX_PW` 갱신(새 비번 주시면 단타봇이 갱신 대행) → 정보봇 투자자 데이터 자동 부활+백필. 비번 만료는 주기 재발 예정 = 퀀트봇 가드 권장.
- E2 3-way·⑤프로그램차익분리 — 6/12 판정 후 순서대로(대조표 승격조건 참조).
- 봇 가동 직전 4-Tier 일괄(P0 7건+AUD-8 등)은 그대로 보류.

---

**안전 불변식**: 봇 OFF(env 1단 차단) · 실주문 0 · 게이트 8/8 · 매도 무손상 · SAJANG 단일진실.
**git**: 5b61c2e = VPS 5b61c2e (동기화·봇 PID 91563 최신코드). **작성**: 2026-06-11 20:05
