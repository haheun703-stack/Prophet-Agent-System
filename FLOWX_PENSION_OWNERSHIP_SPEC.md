# FLOWX 연기금 지분 추적기 (Pension Ownership Tracker)

## 패널 목적
국민연금(NPS) 지분율 5%+ 공시 종목의 매일 연기금 매수/매도 추이를 추적하고,
**연기금 매수 ↔ 주가 변화 상관관계**를 실시간 분석합니다.

## Supabase 테이블
`intelligence_pension_ownership` (date PK, upsert)

### 컬럼 정의
```sql
CREATE TABLE intelligence_pension_ownership (
  date              DATE PRIMARY KEY,
  updated_at        TIMESTAMPTZ DEFAULT now(),
  total_tracked     INTEGER DEFAULT 0,
  accumulating_count INTEGER DEFAULT 0,
  distributing_count INTEGER DEFAULT 0,
  avg_correlation   FLOAT DEFAULT 0,
  strong_positive_corr INTEGER DEFAULT 0,
  strong_negative_corr INTEGER DEFAULT 0,
  top_accumulating  JSONB,
  top_distributing  JSONB,
  stocks            JSONB
);
```

### stocks JSONB 구조
```json
[
  {
    "code": "010120",
    "name": "LS ELECTRIC",
    "base_ownership_pct": 8.79,
    "base_date": "2025-03",
    "cap_b": 417000,
    "pension_net_10d": -631.3,
    "pension_net_5d": -468.0,
    "pension_buy_days": 2,
    "pension_sell_days": 8,
    "pension_total_net": -631.3,
    "pension_flow_trend": "DISTRIBUTING",
    "price_change_pct": 50.9,
    "correlation": -0.789,
    "sector": "기타"
  }
]
```

## 웹봇 표시 규칙

### 1. 요약 카드
```
연기금 지분 추적기 | 5/4(월)
━━━━━━━━━━━━━━━━━━
추적 77종목 | 매집 18 | 매도 37 | 보합 22
상관계수 평균: -0.023 (약한 음의 상관)
양의상관 15종목 | 음의상관 17종목
```

### 2. 핵심 인사이트 (매일 자동 생성)
상관계수 평균에 따라 메시지 변경:
- `avg > 0.3`: "연기금 매수 = 주가 상승 일치도 높음"
- `-0.3 < avg < 0.3`: "연기금 매수와 주가 방향성 약함 (독립적)"
- `avg < -0.3`: "연기금은 역방향 — 오르는 종목을 매도 중 (차익실현)"

### 3. 매집 중 TOP 5 테이블
```
종목         지분율   10일순매수    주가변화   상관
한화엔진     10.60%  +554.7억   +62.1%  +0.905
삼성전자      7.26%  +665.0억    +2.8%  +0.112
코스맥스     13.36%  +247.3억    +0.0%  -0.039
한국콜마     13.48%  +228.9억    +1.9%  +0.541
DL이앤씨    11.68%  +226.0억    +3.7%  +0.251
```

### 4. 매도 중 TOP 5 테이블 (주의 종목)
```
종목         지분율   10일순매수    주가변화   상관
SK하이닉스    7.55% -1387.9억   +10.3%  -0.863
삼성전기     10.62% -1024.9억   +22.4%  -0.757
LS일렉트릭    8.79%  -631.3억   +50.9%  -0.789
효성중공업    12.83%  -516.0억   +30.0%  -0.501
현대모비스     9.06%  -515.2억    +1.2%  +0.149
```

### 5. 상관관계 해석 뱃지
| 상관계수 | 뱃지 | 의미 |
|---------|------|------|
| ≥ +0.7 | 🟢 강한 동행 | 연기금 사면 주가도 오름 |
| +0.3~0.7 | 🟡 약한 동행 | 어느정도 일치 |
| -0.3~+0.3 | ⚪ 무관 | 연기금 매매와 주가 무관 |
| -0.7~-0.3 | 🟠 약한 역행 | 연기금 팔 때 오히려 오름 |
| ≤ -0.7 | 🔴 강한 역행 | 연기금 대량 매도 중 급등 |

### 6. trend별 아이콘
- `ACCUMULATING`: 📈 매집
- `DISTRIBUTING`: 📉 매도
- `FLAT`: ➡️ 보합
- `NO_DATA`: ❓ 없음

### 7. 일별 추이 차트 (stocks 배열 활용)
- X축: 날짜 (date 컬럼)
- Y축 좌: accumulating_count / distributing_count (막대)
- Y축 우: avg_correlation (선)
- 여러 날짜의 데이터를 조회하여 추이 표시

## 업데이트 주기
- COO G7 Stage4 C42에서 매일 1회 실행 (16:30~17:00 경)
- quant_investor_extra.json 갱신 후 실행

## 데이터 출처
- 지분율(%): DART 공시 기준 (분기별 수동 업데이트)
- 일별 순매수: quant_investor_extra.json (VPS 매일 갱신)
- 종가: flow/*_investor.csv
