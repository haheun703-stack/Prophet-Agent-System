# FLOWX 선매집 탐지 패널 지시서

## Supabase 테이블

**`intelligence_stealth_scan`** — 일일 선매집 탐지 결과

| 컬럼 | 타입 | 설명 |
|------|------|------|
| date | DATE (PK) | 거래일 |
| total_scanned | INTEGER | 스캔 종목수 (시총 3,000억+) |
| stealth_count | INTEGER | 잠복 종목수 (가격 ±5%) |
| moving_count | INTEGER | 움직임 종목수 (+5~10%) |
| surged_count | INTEGER | 이미상승 종목수 (+10%+) |
| stealth_items | JSONB | 잠복 종목 상위 20개 |
| moving_items | JSONB | 움직임 종목 상위 10개 |
| surged_items | JSONB | 이미상승 종목 상위 5개 |
| top_stealth_names | JSONB | 잠복 TOP 5 이름 배열 |

## 업데이트 주기

- **매일 16:30~17:00** (G7 Stage3 C28)
- 장 마감 후 당일 수급 데이터 반영

## JSONB 구조

### stealth_items (잠복 — 핵심 종목)
```json
[
  {
    "code": "005930",
    "name": "삼성전자",
    "sector": "반도체",
    "score": 120,
    "pattern": "쌍매수5D",
    "dual_buy": true,
    "inst_consec": 5,
    "frgn_consec": 5,
    "inst_avg": 350,
    "frgn_avg": 500,
    "chg_5d": -1.2,
    "close": 167200,
    "cap": 9980
  }
]
```

### moving_items (움직임)
```json
[
  {
    "code": "000660",
    "name": "SK하이닉스",
    "sector": "반도체",
    "score": 95,
    "pattern": "외인7D",
    "dual_buy": false,
    "chg_5d": 7.3,
    "close": 807000
  }
]
```

## 점수 체계 (최대 140점)

| 항목 | 최대 | 기준 |
|------|------|------|
| 연속 매수일수 | 50점 | min(일수, 10) x 5 |
| 일평균 순매수액 | 40점 | 5억+→40, 2억+→30, 1억+→20 |
| 쌍매수(기관+외인) | 30점 | 기관+외인 동시 연속매수 |
| 잠복 보너스 | 20점 | 가격 ±5% 이내 (스프링 장전) |

## 패턴 라벨

| 패턴 | 의미 | 예시 |
|------|------|------|
| 쌍매수3D | 기관+외인 3일 동시매수 | 가장 강한 신호 |
| 기관5D | 기관 5일 연속매수 | 기관 주도 |
| 외인7D | 외인 7일 연속매수 | 외인 주도 |

## UI 권장 레이아웃

### 메인 요약 바
```
스캔 280종목 | 잠복 23 | 움직임 8 | 이미상승 3
```

### 잠복 종목 리스트 (카드 또는 테이블)
- 점수 100+: 빨간 뱃지
- 점수 70+: 주황 뱃지
- 점수 50+: 노란 뱃지
- 쌍매수: 번개 아이콘
- 정렬: 점수 내림차순

### 각 종목 카드
```
[빨간뱃지] [번개] 삼성전자 · 반도체
점수 120 | 쌍매수5D
기관 +350M/일 | 외인 +500M/일
5일 등락: -1.2% | 현재가 167,200
```

### 움직임 섹션
- 잠복보다 작게 표시
- "초기 반응 시작" 라벨

### 히스토리 차트 (선택)
- 최근 30일 잠복/움직임/이미상승 추이 (date별 count)
- `SELECT date, stealth_count, moving_count, surged_count FROM intelligence_stealth_scan ORDER BY date DESC LIMIT 30`

## Supabase 쿼리 예시

### 오늘 선매집 결과
```js
const { data } = await supabase
  .from('intelligence_stealth_scan')
  .select('*')
  .order('date', { ascending: false })
  .limit(1)
  .single()
```

### 최근 7일 추이
```js
const { data } = await supabase
  .from('intelligence_stealth_scan')
  .select('date, stealth_count, moving_count, surged_count, top_stealth_names')
  .order('date', { ascending: false })
  .limit(7)
```
