# T3 — 수급 패턴 감지기 확장 Planner 문서

**작성일**: 2026-04-16
**역할**: PLANNER (코드 작성 금지, 무엇을 만들지만 정의)
**작업 규모**: 대형 (풀세트 3역할 적용)

---

## 배경

### 문제
퐝가님 원 질문:
> "오늘 급등주들 수급 현황 다시 보자. 이거 우리 패턴을 무조건 잡아내야 된다.
> 외국인, 개인, 기관, 금융투자, 투신, 사모펀드, 은행, 보험, 기타금융, 연기금, 기타 — 이 모든 수급들의 누적까지.
> 그래야 기초데이터 근거로 패턴 파악이 가능하잖아."

### 현실 체크 (2026-04-16 기준)
| 항목 | 상태 |
|------|------|
| 저장 CSV 주체 수 | **4개** (외국인/기관/개인/기타법인) |
| 필요 주체 수 | **11개** (외국인/개인/금융투자/투신/사모펀드/은행/보험/기타금융/연기금/기타법인/기관종합) |
| pykrx 11주체 함수 | **장애** (get_market_net_purchases_of_equities Empty) |
| KRX 정보시스템 직접 호출 | **차단** (400 LOGOUT) |
| KRX OpenAPI | AUTH_KEY 발급 완료, **서비스 이용신청 대기** |

### 결론
**지금 당장 가능한 것**: 4주체 기반 패턴 감지기 먼저 구축
**확보 후 확장**: 11주체 스키마 호환 설계 → OpenAPI 승인 시 플러그인 방식 전환

---

## P0 (최우선) — 지금 즉시 실행

### P0-1: 11주체 호환 스키마 선설계
**목적**: 나중에 11주체 데이터 확보 시 기존 코드 수정 없이 플러그인 가능한 스키마 정의.

**산출물**: `scalper-agent/schemas/supply_flow_schema.py`
```python
ENTITY_COLUMNS = [
    # 4주체 (현재 수집 중)
    "foreign", "institution", "individual", "other_corp",
    # 7주체 (KRX OpenAPI 확보 후)
    "finance_invest",   # 금융투자 (증권사 자기매매)
    "trust",            # 투신 (자산운용)
    "private_equity",   # 사모펀드
    "bank",             # 은행
    "insurance",        # 보험
    "other_finance",    # 기타금융
    "pension",          # 연기금등
]

ENTITY_KR = {
    "foreign": "외국인",
    "institution": "기관종합",
    "individual": "개인",
    "other_corp": "기타법인",
    "finance_invest": "금융투자",
    "trust": "투신",
    "private_equity": "사모펀드",
    "bank": "은행",
    "insurance": "보험",
    "other_finance": "기타금융",
    "pension": "연기금등",
}
```

**DoD**: T3 감지기가 이 스키마를 참조하고, 없는 컬럼은 None 처리.

---

### P0-2: 수급 패턴 6종 분류 로직
**목적**: 오늘 급등주를 수급 패턴별로 자동 분류.

**분류 알고리즘**:

| # | 패턴명 | 조건 | 신호 |
|---|--------|------|------|
| 1 | **쌍매수 폭발** (DUAL_SURGE) | 외국인 +100억+ AND 기관 +50억+ AND 개인 -100억 | 최강 (세력 흡수) |
| 2 | **외국인 단독** (FOREIGN_SOLO) | 외국인 +50억+ AND 기관 <30억 AND 개인 -50억 | 강함 (해외자금) |
| 3 | **기관 단독** (INST_SOLO) | 기관 +50억+ AND 외국인 <30억 AND 개인 -50억 | 중강 (국내자금) |
| 4 | **M&A/자사주 의심** (OTHER_CORP_LOAD) | 기타법인 +20억+ AND 거래량 평균 3배+ | 특수 (공시 확인 필요) |
| 5 | **개인 주도** (RETAIL_LED) | 외국인+기관 합 <10억 AND 개인 +50억+ | 약함 (단타성) |
| 6 | **수급 이탈** (OUTFLOW) | 외국인 <-30억 AND 기관 <-20억 AND 가격 급등 | 위험 (고점 신호) |

**적용 파일**: `scalper-agent/tools/supply_pattern_detector.py` (신규)

**입력**: 코드 리스트 + 오늘 날짜
**출력**: JSON `{code: {pattern: "DUAL_SURGE", score: 95, entities: {...}, warning: []}}`

**누적 계산**:
- 5D 누적: `df.tail(5).sum()`
- 10D 누적: `df.tail(10).sum()`
- 20D 누적: `df.tail(20).sum()`
- **4주체만 현재 계산, 11주체는 데이터 있을 때 자동 포함**

---

### P0-3: 오늘 급등주 TOP 30 실데이터 분류
**목적**: P0-2 감지기를 `data_store/learning/missed_gainers/2026-04-16.json` 에 적용.

**산출물**:
- 콘솔: 패턴별 종목 리스트 출력
- 파일: `data_store/learning/pattern_scan/2026-04-16.json`

**DoD 검증**:
- [ ] 덕산하이메탈 +29.9% → DUAL_SURGE 분류되어야 함 (외 +65.7억, 기 +62.0억)
- [ ] 무림P&P +29.9% → OUTFLOW 분류되어야 함 (외 -13.9억인데 상한가)
- [ ] 엑스게이트 +30.0% → OTHER_CORP_LOAD 의심 (기타법인 -29.1억, 이 경우 매도 패턴)
- [ ] 30종목 전부 패턴 배정 완료

---

### P0-4: KRX OpenAPI 클라이언트 모듈 준비
**목적**: 이용신청 승인되는 즉시 수집 시작 가능하게 미리 구축.

**산출물**: `scalper-agent/data/krx_openapi_client.py`

**기능**:
- `KRXOpenAPIClient(auth_key)` — AUTH_KEY 헤더 인증
- `fetch_stock_daily_trade(basDd)` — 주식 일별 매매정보
- `fetch_investor_daily_by_stock(basDd, isin)` — 종목별 투자자별 (서비스 확인 후)
- 재시도 로직 (429/500 → 지수 백오프)
- 결과 캐싱

**승인 대기 상태에서도 모듈은 즉시 임포트 가능** (__main__에 --test 모드로 키 작동 확인).

---

### P0-5: 자동화 배선
**목적**: 매일 장마감 후 자동 실행.

**배선 위치**: `bot/trading_coo.py` COO G5 그룹 (16:00~16:40)

**스케줄**:
```
16:05 (G5) → supply_pattern_detector.py --universe-scan
              → 오늘 +3%↑ 종목 전수 패턴 분류
              → data_store/learning/pattern_scan/{date}.json 저장
16:06 (G5) → 쌍매수 폭발/외국인 단독 패턴 → 텔레그램 알림 (선택)
```

**DoD**: 내일(4/17) 16:05 자동 실행 확인.

---

## P1 — 차순위 (T3 완성 후)

### P1-1: 텔레그램 수급 패턴 일일 리포트
- 발송 시각: 16:45 (C30 단타봇 preview 이전)
- 내용: 오늘 쌍매수 폭발 TOP 5, OUTFLOW 위험 종목 TOP 5

### P1-2: 패턴 persistence (잔존 효과)
- 3일 전 DUAL_SURGE 종목 → 오늘 가격 추적
- 성공률 백테스트

### P1-3: 11주체 확보 후 확장
- OpenAPI 승인 즉시 `krx_openapi_client.py` 활성화
- 스키마 11주체 전환
- 패턴 분류 추가 (예: PENSION_ACCUMULATE — 연기금 꾸준 매수)

---

## 3역할 체크포인트

### Generator 완료 기준
- [ ] P0-1 스키마 파일 생성
- [ ] P0-2 감지기 코드 작성 + 단위 테스트
- [ ] P0-3 오늘 급등주 30종목 실제 분류 출력
- [ ] P0-4 OpenAPI 클라이언트 작성 + --test 모드 확인
- [ ] P0-5 COO G5 배선 + 내일 실행 스케줄 확인

### Evaluator 검수 기준 (FAIL 조건)
- Critical: 덕산하이메탈/무림P&P/엑스게이트 중 1개라도 오분류
- Critical: 30종목 중 패턴 미배정 존재
- Critical: 스키마가 11주체 확장 불가
- High: KRX OpenAPI 클라이언트 import 실패
- High: COO G5 배선 시 기존 스케줄 충돌
- Medium: 누적 계산 5D/10D/20D 중 하나라도 오류

### PASS 기준
- 위 Critical/High 0건
- Medium 2건 이하
- 오늘 급등주 30종목 전부 의미있는 패턴 배정

---

## 승인 요청 (내부 자동 승인)

지시사항: "a안 부터 순차적으로 진행 + 자동 auto + 강력하게 실수 없이"

→ PLANNER 단계 완료, GENERATOR 단계 자동 진입.
