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

### 섹션 헤더
```
🤝 매집 합류 시그널 — 연기금 + 금투 합류 타이밍
```
- 기관 매집 레이더와 **같은 톤** (기관 수급 계열)
- 서브 타이틀: "연기금 3-5일 매수 → 금투 합류 시 D+5 +1.6%"

### 2-탭 구조
```
[핵심후보 (12)] [대기 (7)]
```
- **핵심후보 탭**: best_stocks (연기금 + 금투 합류 완료)
- **대기 탭**: standby_stocks (연기금만 매수중, 금투 아직)

### 핵심후보 카드 (각 종목)
```
┌──────────────────────────────────────────────────┐
│ [오늘합류]  삼성전자         전기전자   시총 3.5조   │
│ 연기금 5일째 · 누적 +1,351억                       │
│ 금투 오늘 +1,632억 · 5d +2.1%                     │
└──────────────────────────────────────────────────┘
```

### fi_joined 뱃지
| fi_joined | 뱃지 텍스트 | 색상 | 의미 |
|-----------|------------|------|------|
| TODAY | "오늘합류" | 빨간색/핫핑크 (강조) | 금투가 오늘 처음 매수 |
| YESTERDAY | "어제합류" | 주황색 | 금투가 어제 매수 시작 |

### 대기 카드 (standby — 금투 미합류)
```
┌──────────────────────────────────────────────────┐
│ [대기중]  HD현대일렉        전기전자   시총 1.2조   │
│ 연기금 4일째 · 누적 +294억 · 5d -0.2%             │
│ 금투 미합류 — 진입 시 알림 예정                     │
└──────────────────────────────────────────────────┘
```
- 대기 뱃지: 회색/파란색 "대기중"
- 금투 미합류 텍스트 표시

### 핵심 시각 강조 규칙
1. **ret5 < 0%** → "미발화" 강조 (초록 텍스트, 가장 좋은 기회)
2. **ret5 0~3%** → 일반 표시
3. **ret5 3~5%** → 약간 회색 (이미 일부 반영)
4. **pension_consec == 5** → 연기금 일수 빨간색 (오래 쌓임 = 큰 포지션)
5. **pension_consec == 3** → 초기 감지, 파란색
6. **fi_today > 100억** → 금투 금액 굵게 강조

### 정렬
- 핵심후보: pension_cum 내림차순 (연기금 누적 많은 순)
- 대기: pension_cum 내림차순

### 빈 상태
각 탭에 해당 종목이 0개일 때:
```
핵심후보: "오늘은 연기금+금투 합류 종목이 없습니다"
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
