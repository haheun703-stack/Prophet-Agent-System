# FLOWX 매집 합류 시그널 패널 지시서

## 개요
연기금이 3~5일 연속 매수 중인 종목에 **금융투자(증권사)가 합류한 타이밍**을 감지하여 표시.
백테스트 결과: D+5 평균 **+1.59%**, 외인 방향 무관 (매도해도 +1.58%, 매수해도 +1.60%).
핵심 인사이트: 연기금의 느린 분할매수에 금투의 단기 자금이 올라타는 시점이 최적 매수 타이밍.

## 배치 위치
```
스윙 시스템 페이지 내:
┌─────────────────────────────────┐
│  기관 매집 레이더 (C40)          │
├─────────────────────────────────┤
│  ★ 매집 합류 시그널 (C41, NEW)   │  ← 여기
├─────────────────────────────────┤
│  피보나치 눌림목 (기존)           │
└─────────────────────────────────┘
```

## 데이터 소스

### Supabase 테이블: `intelligence_pension_scan`

#### 테이블 생성 SQL
```sql
CREATE TABLE intelligence_pension_scan (
    date DATE PRIMARY KEY,
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    total_count INTEGER DEFAULT 0,
    best_count INTEGER DEFAULT 0,
    best_fresh_count INTEGER DEFAULT 0,
    standby_count INTEGER DEFAULT 0,
    best_stocks JSONB DEFAULT '[]'::jsonb,
    best_fresh JSONB DEFAULT '[]'::jsonb,
    standby_stocks JSONB DEFAULT '[]'::jsonb
);

-- RLS 정책
ALTER TABLE intelligence_pension_scan ENABLE ROW LEVEL SECURITY;
CREATE POLICY "Allow public read" ON intelligence_pension_scan
    FOR SELECT USING (true);
```

#### 컬럼 설명
| 컬럼 | 타입 | 설명 |
|------|------|------|
| date | DATE PK | 감지일 (YYYY-MM-DD) |
| updated_at | TIMESTAMPTZ | 업데이트 시각 |
| total_count | INTEGER | best + standby 합계 |
| best_count | INTEGER | 핵심후보 종목 수 (연기금 + 금투 합류) |
| best_fresh_count | INTEGER | 핵심후보 중 아직 덜 오른 것 (5d < 5%) |
| standby_count | INTEGER | 대기 종목 수 (연기금만, 금투 미합류) |
| best_stocks | JSONB | 핵심후보 배열 (최대 20종목) |
| best_fresh | JSONB | 덜 오른 핵심후보 (최대 15종목, best_stocks의 부분집합) |
| standby_stocks | JSONB | 대기 배열 (최대 15종목) |

### JSONB 종목 구조 (best_stocks / best_fresh / standby_stocks 공통)
```json
{
    "code": "005930",
    "name": "삼성전자",
    "sector": "전기전자",
    "cap": 3500000,
    "pension_consec": 5,
    "pension_cum": 1351.2,
    "fi_today": 1632.0,
    "fi_3d": 2100.5,
    "fi_joined": "TODAY",
    "ret5": 2.1,
    "close": 58300
}
```

### 필드 설명
| 필드 | 의미 | 표시 방법 |
|------|------|-----------|
| code | 종목코드 (6자리) | 내부 식별용 |
| name | 종목명 | 메인 텍스트 |
| sector | 업종 | 서브 텍스트 (회색) |
| cap | 시가총액 (억) | "시총 3.5조" (cap >= 10000이면 조 단위) |
| pension_consec | 연기금 연속매수 일수 (3~5) | "연기금 5일째" |
| pension_cum | 연기금 누적 순매수(억) | "+1,351억 누적" |
| fi_today | 금투 오늘 순매수(억) | "+1,632억" |
| fi_3d | 금투 최근 3일 합계(억) | 참고용 |
| fi_joined | 금투 합류 시점 | 뱃지 (아래 참조) |
| ret5 | 5일 등락률(%) | "+2.1%" 또는 "-0.2%" |
| close | 현재가 (원) | "58,300원" |

## UI 디자인 가이드

> **참고 레이아웃**: 퀀트봇 "발화 섹터 종목" 패널과 동일한 **섹터별 그룹 + 테이블** 방식 적용.
> 카드형 나열 대신, 섹터로 묶어서 테이블로 보여줘야 32종목도 한눈에 비교 가능.

### 페이지 헤더
```
매집 합류 시그널 — 연기금 3-5일 매수 → 금투 합류 시 D+5 +1.6%
2026-04-28 기준
```

### 3-탭 필터
```
[전체 32] [미발화 8] [대기 7]
```
- **전체**: best_stocks 전체 (합류 완료)
- **미발화**: best_fresh (합류 완료 + 5d < 5% — 아직 덜 오른 것, 가장 중요)
- **대기**: standby_stocks (연기금만, 금투 미합류)

### 섹터별 그룹 레이아웃 (핵심)

각 섹터를 카드로 묶고, 내부는 테이블 형태:

```
┌─ 전기전자 ──────────────────────────────────────────────────────────┐
│ 연기금5d: +1,351억  금투5d: +1,632억                                 │
│                                                                     │
│ 종목        종가       연기금   누적       금투오늘    합류    5d수익   │
│ ─────────────────────────────────────────────────────────────────── │
│ 삼성전자    222,000    5d    +1,351억   +1,632억   오늘   +2.1%  ★  │
│ ISC        237,500    3d      +48억      -10억   대기   -1.2%  ★  │
└─────────────────────────────────────────────────────────────────────┘

┌─ 운송장비 ──────────────────────────────────────────────────────────┐
│ 연기금5d: +474억  금투5d: +59억                                      │
│                                                                     │
│ 종목        종가       연기금   누적       금투오늘    합류    5d수익   │
│ ─────────────────────────────────────────────────────────────────── │
│ 현대로템    244,500    5d     +294억     +41억    오늘   +2.9%  ★  │
│ HD현대     280,500    4d     +180억     +18억    오늘   +6.0%     │
└─────────────────────────────────────────────────────────────────────┘

┌─ 금속 ─────────────────────────────────────────────────────────────┐
│ ...                                                                 │
└─────────────────────────────────────────────────────────────────────┘
```

### 섹터 헤더 구성
- **섹터명** (굵게)
- **연기금5d / 금투5d**: 해당 섹터 내 종목들의 연기금/금투 누적 합산 (억)
- 프론트엔드에서 sector별 그룹핑 + 합산 계산

### 그룹핑 방법 (프론트엔드)
```javascript
// best_stocks를 sector별로 그룹핑
const grouped = {};
for (const stock of data.best_stocks) {
  const sector = stock.sector || '기타';
  if (!grouped[sector]) grouped[sector] = [];
  grouped[sector].push(stock);
}
// 섹터별 pension_cum 합계 내림차순 정렬
const sortedSectors = Object.entries(grouped)
  .map(([sector, stocks]) => ({
    sector,
    stocks,
    totalPension: stocks.reduce((s, x) => s + x.pension_cum, 0),
    totalFi: stocks.reduce((s, x) => s + x.fi_today, 0),
  }))
  .sort((a, b) => b.totalPension - a.totalPension);
```

### 테이블 컬럼 정의
| 컬럼 | 데이터 | 정렬 | 비고 |
|------|--------|------|------|
| 종목 | name + code (회색) | - | 종목명 굵게 |
| 종가 | close (천 단위 콤마) | - | 오른쪽 정렬 |
| 연기금 | pension_consec + "d" | - | 5d = 빨강, 3d = 파랑 |
| 누적 | pension_cum (억) | pension_cum DESC | +/- 부호, 양수=빨강 |
| 금투오늘 | fi_today (억) | - | +/- 부호, 100억+ 굵게 |
| 합류 | fi_joined 뱃지 | - | 아래 뱃지 참조 |
| 5d수익 | ret5 (%) | - | 색상 규칙 적용 |
| 미발화 | ret5 < 5% 이면 ★ | - | 초록색 별 |

### 합류 뱃지
| fi_joined | 표시 | 색상 |
|-----------|------|------|
| TODAY | 오늘 | 빨간 배경 + 흰 글자 (강조) |
| YESTERDAY | 어제 | 주황 배경 + 흰 글자 |
| (빈값) | 대기 | 회색 배경 (standby 탭에서만) |

### 미발화(★) 행 강조
- **ret5 < 0%**: 행 배경 연한 초록 + ★ 진한 초록 (최고 기회)
- **ret5 0~3%**: 행 배경 연한 초록 + ★ 초록
- **ret5 3~5%**: ★만 표시, 배경 없음
- **ret5 >= 5%**: ★ 없음, 일반 행

### 대기 탭 레이아웃
대기 종목도 동일한 섹터별 테이블 형태. 단, "금투오늘" 컬럼 대신 "금투 미합류" 텍스트:
```
┌─ 전자부품 ──────────────────────────────────────────────┐
│ 종목        종가       연기금   누적       상태     5d수익 │
│ 삼화콘덴서   26,300     4d     +48억    대기중    -0.9% ★│
└─────────────────────────────────────────────────────────┘
```

### 섹터 카드 정렬
1. 핵심후보(전체/미발화): 섹터 내 pension_cum 합계 내림차순
2. 대기: 동일

### 빈 상태
```
전체/미발화: "오늘은 연기금+금투 합류 종목이 없습니다"
대기: "연기금 연속매수 종목이 없습니다"
```

## 데이터 쿼리

```javascript
// 최신 1건
const { data } = await supabase
  .from('intelligence_pension_scan')
  .select('*')
  .order('date', { ascending: false })
  .limit(1)
  .single();

// 핵심후보 (pension_cum 내림차순 정렬 — 이미 서버에서 정렬됨)
const bestStocks = data.best_stocks;

// 덜 오른 핵심후보만 별도 표시 (이미 서버에서 분리됨)
const bestFresh = data.best_fresh;

// 대기 리스트
const standbyStocks = data.standby_stocks;

// 카운트
const bestCount = data.best_count;
const standbyCount = data.standby_count;
```

## 갱신 주기
- **매일 16:35~16:40** (G7 파이프라인 Stage 4, C41)
- 기관 매집 레이더(C40) 직후 실행
- 주말/공휴일엔 마지막 거래일 데이터 유지

## 데이터 없는 날 처리
- 테이블에 해당 날짜 row 없음 → "오늘은 감지된 종목이 없습니다"
- best_stocks + standby_stocks 모두 빈 배열 → 동일 처리

## 참고: 감지 로직 요약 (백엔드)
1. `quant_investor_extra.json` 읽기 (퀀트봇 pykrx, 매일 17:28 갱신)
2. 시총 1,000억+ / ETF 제외 필터
3. 연기금 연속 순매수 3~5일 카운트
4. 금투(금융투자) 오늘/어제 순매수 여부로 합류 판별
5. 5일 수익률 계산 (flow CSV 종가 기준)
6. 핵심후보: 합류 완료 (TODAY/YESTERDAY) → pension_cum 내림차순
7. 대기: 연기금만 매수중 + 5d 수익률 < 5% → pension_cum 내림차순

## 백테스트 근거
| 조건 | D+1 | D+3 | D+5 | 승률 |
|------|-----|-----|-----|------|
| 연기금 3d+ + 금투 매수 | +0.42% | +1.09% | +1.59% | 54.7% |
| + 외인 매도 | +0.40% | +1.08% | +1.58% | 54.5% |
| + 외인 매수 | +0.44% | +1.11% | +1.60% | 54.9% |
| 연기금만 (금투X) | +0.15% | +0.38% | +0.52% | 51.2% |
- **핵심**: 연기금만으론 약함(+0.52%), 금투 합류가 트리거(+1.59%)
- **외인 무관**: 외인 방향은 수익에 영향 없음
- 1,714종목 x 249거래일 백테스트
