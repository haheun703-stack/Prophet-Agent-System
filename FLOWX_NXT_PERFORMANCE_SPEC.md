# FLOWX NXT 야간매수 TOP 5 + 성적표 패널 지시서

## 데이터 소스 (2개 테이블)

### 1. intelligence_nxt_picks — NXT TOP 5 추천
| 컬럼 | 타입 | 설명 |
|------|------|------|
| date | DATE PK | 추천일 |
| nxt_score | REAL | nightwatch 종합 점수 (-10~+10) |
| signal | TEXT | "🟢 매수 고려" 등 |
| sectors | JSONB | 추천 섹터 TOP 3 |
| picks | JSONB | 종목별 상세 (아래 참조) |

#### picks JSONB 구조
```json
[
  {
    "rank": 1,
    "code": "005290",
    "name": "동진쎄미켐",
    "sector": "반도체",
    "supply_score": 80,
    "entry_price": 35000
  }
]
```

### 2. intelligence_nxt_performance — NXT 성적표
| 컬럼 | 타입 | 설명 |
|------|------|------|
| pick_date | DATE PK | 추천일 |
| result_date | DATE | 결과 확인일 |
| avg_return | REAL | 평균 수익률(%) |
| best_pick | TEXT | 최고 수익 종목 |
| worst_pick | TEXT | 최저 수익 종목 |
| weekly_return | REAL | 주간 누적(%) |
| weekly_days | INT | 주간 거래일수 |
| weekly_wins | INT | 주간 승수 |
| monthly_return | REAL | 월간 누적(%) |
| monthly_days | INT | 월간 거래일수 |
| monthly_wins | INT | 월간 승수 |
| items | JSONB | 종목별 결과 |

#### items JSONB 구조
```json
[
  {
    "rank": 1,
    "code": "005290",
    "name": "동진쎄미켐",
    "sector": "반도체",
    "supply_score": 80,
    "entry_price": 35000,
    "close_price": 36099,
    "return_pct": 3.14
  }
]
```

## 웹 대시보드 구현

### 1. NXT TOP 5 추천 카드
- 최신 1건: `SELECT * FROM intelligence_nxt_picks ORDER BY date DESC LIMIT 1`
- nightwatch 종합 점수 + 신호 표시
- 종목별: 순위 | 종목명 | 섹터 | 수급점수 | 진입가

### 2. NXT 성적표 카드
- 최신 1건: `SELECT * FROM intelligence_nxt_performance ORDER BY pick_date DESC LIMIT 1`
- 평균 수익률 (크게, 색상: 양수=녹색, 음수=빨강)
- 종목별: 진입가→종가 | 수익률(%)

### 3. 주간/월간 배지
- weekly_return / monthly_return 크게 표시
- 승률: wins/days

### 4. 수익률 차트 (최근 20거래일)
- `SELECT pick_date, avg_return FROM intelligence_nxt_performance ORDER BY pick_date DESC LIMIT 20`
- 바 차트 + 누적선 오버레이

### 5. TODAY vs NXT 비교 테이블
- 왼쪽: TODAY 단타 TOP 5 (intelligence_daytrading_performance)
- 오른쪽: NXT 야간매수 TOP 5 (intelligence_nxt_performance)
- 동일 날짜 기준 수익률 비교
