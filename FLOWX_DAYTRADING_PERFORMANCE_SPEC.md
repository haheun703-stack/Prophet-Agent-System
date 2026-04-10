# FLOWX 단타 TOP 5 성적표 패널 지시서

## 데이터 소스
- **테이블**: `intelligence_daytrading_performance`
- **PK**: `date` (DATE)
- **갱신 주기**: 매 거래일 16:30 (G7 자동 발행)

## 테이블 스키마
| 컬럼 | 타입 | 설명 |
|------|------|------|
| date | DATE PK | 거래일 |
| avg_return | REAL | 당일 평균 수익률(%) |
| best_pick | TEXT | 최고 수익 종목명 |
| worst_pick | TEXT | 최저 수익 종목명 |
| weekly_return | REAL | 주간 누적 수익률(%) |
| weekly_days | INT | 주간 거래일수 |
| weekly_wins | INT | 주간 양수일 |
| monthly_return | REAL | 월간 누적 수익률(%) |
| monthly_days | INT | 월간 거래일수 |
| monthly_wins | INT | 월간 양수일 |
| items | JSONB | 종목별 상세 (아래 참조) |
| created_at | TIMESTAMPTZ | 생성 시각 |

### items JSONB 구조
```json
[
  {
    "rank": 1,
    "code": "000660",
    "name": "SK하이닉스",
    "sector": "반도체",
    "score": 93.0,
    "open_price": 198000,
    "close_price": 210000,
    "high_price": 212000,
    "low_price": 196000,
    "return_pct": 6.06,
    "volume": 5230000
  }
]
```

## 웹 대시보드 구현

### 1. 오늘의 성적표 카드
- **최신 1건** 조회: `SELECT * FROM intelligence_daytrading_performance ORDER BY date DESC LIMIT 1`
- **표시 항목**:
  - 날짜 + 평균 수익률 (크게, 색상: 양수=녹색, 음수=빨강)
  - 종목별 리스트: 순위 | 종목명 | 시가→종가 | 수익률(%)
  - 최고 수익: {best_pick}, 최저: {worst_pick}

### 2. 주간/월간 누적 배지
- `weekly_return` / `monthly_return` 표시
- 승률 표시: `weekly_wins / weekly_days`, `monthly_wins / monthly_days`
- 색상: 양수=녹색, 음수=빨강

### 3. 수익률 차트 (최근 20거래일)
- **쿼리**: `SELECT date, avg_return FROM intelligence_daytrading_performance ORDER BY date DESC LIMIT 20`
- **차트**: 바 차트 (양수=녹색, 음수=빨강)
- **누적선**: 일별 avg_return 합산 라인 오버레이

### 4. 기존 TOP 5 픽과 연동
- `intelligence_daytrading_picks` (mode=confirmed) → 오늘의 추천
- `intelligence_daytrading_performance` → 오늘의 실적
- 두 테이블을 date로 JOIN하여 "추천 → 결과" 흐름 표현

## 디자인 참고
- 기존 FLOWX 퀀트 대시보드 스타일 유지
- 모바일 우선 레이아웃 (텔레그램 웹앱 내 표시)
- 다크 테마 기본
