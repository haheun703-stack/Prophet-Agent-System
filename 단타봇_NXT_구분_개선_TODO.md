# 단타봇 vs NXT 구분 명확화 작업 TODO

> **작성**: 2026-04-16 저녁 세션 중단 시점
> **상태**: 조사 완료, 개선 방향 선택 대기 중
> **재개 시**: 이 문서 읽고 퐝가님이 A/B/C/D 중 선택 → 바로 구현

---

## 🎯 문제 진단

퐝가님 말씀: "단타봇 krx에 들어갈 종목, nxt에 들어갈 종목 구분이 불명확"

### 원인 분석 결과

**4종류 메시지가 각자 따로 텔레그램에 나가면서 뒤섞임:**

| 메시지 | 시각 | 모듈 | 매매처 명시 |
|--------|------|------|------------|
| 단타 TOP Preview | 16:45 (C30) | `tools/daytrading_picks.py` | ⚠️ **"NXT 야간 매수 가능 (17:00~20:00)"** ← 혼선 원인 |
| 단타 TOP Confirmed | 07:35 (A12) | `tools/daytrading_picks.py` | "09:00 개장~09:30 진입 권장" |
| NXT TOP 5 | 16:30 (C32) | `tools/nxt_performance.py` | "NXT 시간외 매수 (15:30~20:00)" |
| NXT Nightwatch 예비 | 15:55 (C4E) | `data/nightwatch.py` | "매수 고려" |

### 핵심 혼선 포인트

1. **단타봇 preview가 "NXT 야간 매수 가능"이라고 말함** ([daytrading_picks.py:527](scalper-agent/tools/daytrading_picks.py#L527))
   → 단타봇 종목인데 NXT로 사라는 건지 KRX 정규장으로 사라는 건지 애매

2. **NXT TOP 5도 따로 나옴** → 같은 종목이 겹쳐도 메시지가 분리됨 → 어디서 사야 할지 혼선

3. **단타봇과 NXT가 독립 실행** → 겹침 방지 로직 없음

---

## 📊 단타봇 vs NXT 로직 비교 (조사 완료)

| 구분 | 단타봇 (daytrading_picks) | NXT (nightwatch) |
|------|--------------------------|------------------|
| **매매 경로** | KRX 정규장 09:00 시가 | NXT 시간외 15:30~20:00 |
| **보유 기간** | 1~3일 | 1일 (익일 08:00 매도) |
| **핵심 스캐너** | `foreign_accumulation_scanner` (5일 조용한 축적) | `nightwatch.py supply_score` (매크로+당일 수급) |
| **시그널** | 외인 5일 매집 + 쌍매수 300억+ | NASDAQ/FX/VIX/금/공매도/뉴스 종합 |
| **선별 관점** | **미래 3일** (선행 매집) | **야간 1일** (NASDAQ 반영) |
| **ETF 포함** | ❌ 제외 | ✅ 포함 (골드 ETF 등) |
| **출력 파일** | `data_store/daytrading_picks.json` | `data_store/nightwatch_report.json` + `nxt_top5_picks.json` |

### 구체 데이터 (4/16 기준)

**오늘 NXT TOP 8:**
- tier2 동진쎄미켐(005290) 수급78 · 반도체
- tier2 삼성전기(009150) 수급75 · 반도체
- tier1 HD현대일렉트릭(267260) 수급30 · 전력
- tier2 LS에코에너지(229640) 수급30 · 전력
- tier2 풍산홀딩스(005810) 수급40 · 금/은
- tier2 KZ정밀(036560) 수급35 · 금/은
- tier1 KODEX 골드선물H(132030) 수급100 · 금 ETF
- tier1 ACE KRX금현물(411060) 수급100 · 금 ETF

**어제(4/15) 단타봇 Confirmed 4종목:**
- SK하이닉스(000660) A_대형주
- 부산엔터프라이즈(034020) A_대형주
- 이수앱시스(007660) A_대형주
- KG스틸(001390) B_중소형주

→ **우연히 겹치지 않았을 뿐**, 선별 기준이 비슷한 수급 로직이라 언제든 겹칠 수 있음.

---

## ⚠️ 구조적 문제 3가지

### 1. universe.json에 NXT 등록 플래그 없음
```
현재 필드: name, market, suffix, mkt_code, sector, volume, per, pbr, cap_억
                                                    ↑ nxt_eligible 같은 컬럼 없음
```
현재 `_is_afterhours_eligible()`는 **ETF만 제외**하고 모든 일반주를 NXT 가능으로 가정.
→ 실제 NXT 미등록 종목을 추천할 위험.

### 2. 겹침 방지 로직 없음
단타봇과 NXT가 독립 실행 → 한 종목이 양쪽에 나오면 퐝가님 혼선.

### 3. 단타봇 preview 메시지가 "NXT 야간 매수 가능"이라 말함
[`tools/daytrading_picks.py:527`](scalper-agent/tools/daytrading_picks.py#L527)
→ 단타봇 = KRX 정규장 대상인데 NXT를 섞어서 말함.

---

## 🛠️ 개선 선택지 (퐝가님 선택 필요)

### A. **최소 수정** — 단타봇 preview에서 "NXT 야간 매수 가능" 문구만 제거
- 수정 파일: `tools/daytrading_picks.py:527`
- 효과: 단타봇 = **KRX 정규장 전용**으로 명확화
- 작업량: 5분

### B. **겹침 방지** — NXT에 뽑힌 종목은 단타봇에서 자동 제외 (NXT 우선)
- 수정 파일: `bot/trading_coo.py _job_daytrading_picks` 부분 또는 `tools/daytrading_picks.py apply_daytrading_filters`
- 로직: `nightwatch_report.json`의 nxt_targets 코드 세트 로드 → 단타봇 필터에서 제외
- 작업량: 30분

### C. **통합 메시지** — 16:50경 "오늘의 매매 가이드" 하나로 통합
- 구조:
  ```
  📢 오늘의 매매 가이드 (4/16)
  🌙 야간 NXT 매수 (15:30~20:00): [동진쎄미켐, 삼성전기, ...]
  ☀️ 내일 KRX 정규장 매수 (09:00 시가): [A, B, C]
  ⚠️ 겹친 종목 없음 / 겹친 종목 D는 NXT 우선
  ```
- 신규 모듈 작성 필요
- 작업량: 반나절

### D. **NXT 등록 플래그 데이터 수집** — universe.json에 `nxt_eligible` 필드 추가
- 실제 NXT 등록 종목 리스트 크롤링 (KRX 공시)
- 작업량: 며칠 (데이터 검증 포함)

---

## 🎯 제안 우선순위: **A → B → C**

- **A**: 즉시 가능 (문구 한 줄). 혼선 80% 해소.
- **B**: 겹침 원천 봉쇄. 단타봇과 NXT의 역할 분리 명확.
- **C**: 사용자 경험 완성. 하나의 메시지로 당일 계획 파악.
- **D**: 중장기 — 데이터 인프라 작업이라 별개 트랙.

---

## 🔑 재개 시 첫 행동

1. 이 MD 읽기
2. 퐝가님 선택 확인 (A/B/C/D 또는 조합)
3. 선택된 방향으로 바로 코드 수정 시작
4. 3역할 프로세스(Planner→Generator→Evaluator)로 진행

---

## 📎 관련 파일

- `scalper-agent/tools/daytrading_picks.py` — 단타봇 본체 (691줄)
- `scalper-agent/tools/nxt_performance.py` — NXT TOP 5 발행 (513줄)
- `scalper-agent/data/nightwatch.py` — NXT nightwatch (3186줄)
- `scalper-agent/bot/trading_coo.py` — 스케줄러 (COO G1/G6/G7)
- `scalper-agent/data_store/daytrading_picks.json` — 단타봇 출력
- `scalper-agent/data_store/nightwatch_report.json` — NXT 출력
- `scalper-agent/data_store/nxt_top5_picks.json` — NXT TOP 5 출력

---

## 📝 이번 세션 커밋 이력 (참고)

- `35afe52` feat: 놓친 급등주 텔레그램 제외 + 날짜별 파일 축적 (직전 작업)
- `1235352` docs: 사이클 감지기 FLOWX 지시서 v2
- `6df8684` fix: 사이클 감지기 검수 6건 수정
- `c492405` feat: 사이클 감지기 급등 세분화 — 지속/원샷 분류

---

**마지막 세션 상태**: 퐝가님 "잠깐만 노트북 껏다가 다시 켜면 진행 바로하자 md에 저장해줘"
→ 이 MD 저장 완료. 재개 대기.
