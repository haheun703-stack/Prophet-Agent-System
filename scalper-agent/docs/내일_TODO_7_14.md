# 📋 내일(7/14 화) TODO — 7/13 마감 시 사장님 지시로 저장

> 작성: 2026-07-13(월) 21시경 · 봇 OFF(실매매)·실주문 0 유지 중
> 오늘 완료분은 git 기록(cb9d612·93eb39f). 아래는 **내일 이어갈 남은 과제**.

---

## ✅ 오늘(7/13) 완료 (참고)
- **기초데이터 점검**: KIS 4채널(daily·investor·flow_market·foreign_exh) 전량 7/13 종가 최신. investor gap 5종목 = 상폐·정지(daily·investor·short 3채널 동반 정지가 증거) = 진짜 수집실패 0건.
- **DataVerifier 오탐 fix** (`cb9d612` + `93eb39f`): investor_flow가 6/22 박제 `universe.json`의 죽은 종목을 랜덤으로 밟아 뜨던 PARTIAL 오탐 차단. daily 교차기준으로 활성종목만 표본(`_load_active_codes`). 4-Tier 전부 PASS·VPS 실측 30회 랜덤 전부 PASS(15/19→16/19). 로컬=origin=VPS 동기화.
- **OBSERVE v2 첫 실전 가동 확인**: cron 정상 동작 → 폭락장(삼성 -10.7%·하이닉스 -15.4%)에서 intent 17건 기록 → 저녁 ⑲-2 대조 **17/17 일치 100% PASS**(실주문 0).

---

## 🔷 내일 TODO

### 1. ★ 사장님 결정 대기 — "봇 OFF" 표현 정정 여부
- **발견(7/13)**: CLAUDE.md·메모리가 반복하는 "봇 OFF"는 엄밀히 부정확.
  - 실제: `bodyhunter-bot.service` **active running**(매일 06:00~·페이퍼 리허설).
  - `PAPER_ONLY: true` + `runtime_mode: paper_rehearsal` → 실주문은 확실히 0(오늘 실주문 전송 로그 0건).
  - order_intents 매일 생성(오늘 18건) = 페이퍼 시뮬레이션.
  - 5/28 사장님 "그냥해라" 명령으로 재적용된 상태 — 오늘 변화 아님(파일 7/7 수정).
  - kill_switch.json이 git상 상시 dirty(HEAD=true, 런타임=false) = 런타임 상태파일 특성.
- **판단**: 문서 표현을 "봇 OFF" → **"실매매 OFF·페이퍼 리허설 가동(실주문 0)"**로 정정할지 사장님 결정.
- ★ kill_switch·config는 사장님 매매결정 영역 — 단타봇 임의 변경 절대 금지.

### 2. daily_ohlcv 동일 오탐 취약점 (별도 설계 필요)
- investor와 같은 패턴: 랜덤 샘플이 죽은 종목(daily stale) 밟으면 PARTIAL 오탐 가능.
- investor는 daily라는 교차기준이 있어 고쳤지만, **daily 자신은 상위 교차기준이 없음**.
- 근본 해결안 후보: ① universe.json 활성 갱신(TODO 3과 연동) ② 거래정지 종목 리스트 확보 ③ 임계 완화.
- 우선순위 중 — daily_ohlcv는 죽은종목 비율 높아(3555 중 204 stale ≈5.7%) investor보다 오탐 빈도 높을 수 있음.

### 3. universe.json 6/22 박제 갱신 검토
- 현재 `universe.json` = 6/22 09:36 생성 후 미갱신 → 상폐·정지 종목 누적(오늘 5개 확인).
- **주의**: 매수/선정 로직이 이 파일을 참조하는지 grep 먼저(매매 영향 시 사장님 승인 필요·큰 변경).
- 검증기만 쓰면 갱신 안전. 매매가 쓰면 별도 판단.

### 4. 유니버스 밖 잔재 격리
- `flow/` 폴더에 유니버스 밖 파일 893개(ETN·스팩 201 + 상폐·ETF 692) = 검증 노이즈원.
- 삭제 아닌 **격리 이동**(7/7 교훈). 수집 대상 아님 확인 후.

### 5. VPS 루트 `_tmp_*.py` 슬러지 정리
- `_tmp_check_data.py` 등 다수 임시 스크립트(무해). 격리 정리.

### 6. OBSERVE v2 로드맵 계속 (기존)
- **7/14 대조 확인**: 오늘 17건 intent의 질(체결강도·추격<8 필터 동작) 검토.
- 대조 통과 누적 시 **7/15경 소액 라이브 사장님 결정**(안전핀: 일일 -6%p CB·섹터쿨다운·추격<8·킬스위치 SAJANG 등재 후).
- ★ 관측 없이 flip 금지 — 라이브 전환은 언제나 사장님 결정.

---

**불변식**: 봇 실매매 OFF·실주문 0·매도 무손상·picks 불변·SAJANG 단일진실·관측 없이 flip 금지.
