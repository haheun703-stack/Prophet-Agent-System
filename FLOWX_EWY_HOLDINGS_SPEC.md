# FLOWX EWY 펀드 보유종목 추적기 지시서

## 개요
iShares MSCI South Korea ETF (EWY)의 보유종목 비중 변화를 추적하여
**외국인 패시브 자금 흐름을 선행 감지**하는 패널.
EWY는 한국 주식에 투자하는 최대 해외 ETF로, 비중 변화 = 외인 매수/매도 압력 예측의 핵심 지표.

## 핵심 가치
- **비중 증가 종목** = 패시브 외인 매수 유입 예정 → 선취매 후보
- **비중 감소 종목** = 패시브 외인 매도 압력 → 매수 회피
- **신규 편입** = 강제 매수 발생 → 단기 수급 폭탄
- **편출** = 강제 매도 → 급락 리스크

## 배치 위치
```
[퀀트봇 탭]
┌─────────────────────────────────┐
│  EWY 펀드 보유종목 변동 (NEW)    │  ← 여기
├─────────────────────────────────┤
│  기존 퀀트봇 패널들              │
└─────────────────────────────────┘
```
- **담당**: 퀀트봇 (단타봇 아님)
- **갱신 주기**: 매 영업일 08:00 (장 시작 전)

## 데이터 소스

### 원본: iShares 공식 CSV
- **URL**: `https://www.ishares.com/us/products/239681/ishares-msci-south-korea-etf/1467271812596.ajax?fileType=csv&fileName=EWY_holdings&dataType=fund`
- **갱신**: 매 영업일 (미국 기준 전일 종가 반영)
- **종목 수**: ~100개 (현금성 자산 포함)
- **Ticker 형식**: 6자리 숫자 (KRX 표준코드) → universe.json과 동일

### CSV 컬럼 구조
| 컬럼 | 설명 | 예시 |
|------|------|------|
| Ticker | KRX 종목코드 | 000660 |
| Name | 영문 종목명 | SK HYNIX INC |
| Sector | GICS 섹터 | Information Technology |
| Asset Class | 자산분류 | Equity |
| Market Value | 시장가치(USD) | 2,345,678,901 |
| Weight (%) | 비중 | 22.78 |
| Notional Value | 명목가치 | 2,345,678,901 |
| Quantity | 보유수량 | 12,345,678 |
| Price | 가격(원) | 189,800 |
| Location | 국가 | South Korea |
| Exchange | 거래소 | Korea Exchange |
| Currency | 통화 | KRW |

### Supabase 테이블: `intelligence_ewy_holdings`
| 컬럼 | 타입 | 설명 |
|------|------|------|
| date | DATE PK | 기준일 |
| as_of | TEXT | iShares 기준일 (CSV 내 날짜) |
| total_stocks | INT | 보유 종목 수 |
| top20 | JSONB | 상위 20종목 배열 |
| changes | JSONB | 전일 대비 변동 배열 |
| new_entries | JSONB | 신규 편입 종목 |
| removed | JSONB | 편출 종목 |
| summary | TEXT | 요약 텍스트 (FLOWX 표시용) |

### top20 JSONB 구조
```json
[
  {
    "rank": 1,
    "code": "000660",
    "name": "SK하이닉스",
    "name_en": "SK HYNIX INC",
    "weight": 22.78,
    "weight_prev": 22.15,
    "weight_change": 0.63,
    "quantity": 12345678,
    "sector": "Information Technology",
    "sector_kr": "전기전자",
    "signal": "UP"
  }
]
```

### changes JSONB 구조
```json
[
  {
    "code": "000660",
    "name": "SK하이닉스",
    "weight": 22.78,
    "weight_prev": 22.15,
    "weight_change": 0.63,
    "direction": "UP",
    "magnitude": "LARGE"
  }
]
```
- direction: `UP` / `DOWN` / `STABLE` (변동 0.05% 미만)
- magnitude: `LARGE` (0.3%+) / `MEDIUM` (0.1~0.3%) / `SMALL` (0.05~0.1%)

### new_entries / removed JSONB 구조
```json
[
  {
    "code": "012345",
    "name": "신규편입종목",
    "weight": 0.45,
    "sector": "Industrials",
    "impact": "패시브 강제매수 예상"
  }
]
```

## FLOWX 표시 형식

### 카드 레이아웃
```
┌──────────────────────────────────────────────┐
│  EWY 펀드 보유종목 변동                        │
│  기준: 2026-05-04  |  보유 98종목              │
├──────────────────────────────────────────────┤
│                                              │
│  비중 TOP 5                                   │
│  1. SK하이닉스    22.78% (+0.63%) ▲           │
│  2. 삼성전자      22.43% (-0.32%) ▼           │
│  3. SK스퀘어       2.79% (+0.15%) ▲           │
│  4. 현대차         2.41% (+0.08%)             │
│  5. KB금융         2.00% (-0.02%)             │
│                                              │
│  주요 변동 (비중 0.1%+ 변화)                   │
│  ▲ 두산에너빌리티  1.96% (+0.31%)  신규TOP20   │
│  ▲ SK하이닉스     22.78% (+0.63%)             │
│  ▼ LG에너지솔루션  0.85% (-0.22%)             │
│  ▼ 삼성전자       22.43% (-0.32%)             │
│                                              │
│  신규 편입: 없음                               │
│  편출: 없음                                   │
│                                              │
│  요약: SK하이닉스 비중 최대, 삼성전자 소폭 감소. │
│  반도체 섹터 비중 46%로 역대 최고.              │
└──────────────────────────────────────────────┘
```

### 변동 표시 규칙
| 변동 | 색상/아이콘 | 기준 |
|------|-----------|------|
| 비중 증가 0.3%+ | 빨간 ▲ (강한 매수) | LARGE UP |
| 비중 증가 0.1~0.3% | 주황 ▲ | MEDIUM UP |
| 비중 변동 0.1% 미만 | 회색 (표시 안함) | STABLE |
| 비중 감소 0.1~0.3% | 파랑 ▼ | MEDIUM DOWN |
| 비중 감소 0.3%+ | 진파랑 ▼ (강한 매도) | LARGE DOWN |
| 신규 편입 | 금색 별 | NEW |
| 편출 | 회색 X | REMOVED |

## 단타봇 연동

### 모닝 추천 반영 (morning_recommendation.py)
```
Step 5에서 EWY 비중 변동 보너스:
- 비중 LARGE UP (+0.3%+) 종목: +8점
- 비중 MEDIUM UP (+0.1~0.3%) 종목: +4점
- 비중 LARGE DOWN (-0.3%+) 종목: -5점
- 신규 편입: +12점
- 편출: -10점
```

### NXT 야간매수 반영 (nightwatch.py)
```
EWY TOP20 종목이 NXT 추천에 있으면:
- 비중 1%+ 대형주: +5점 (패시브 매수 지속)
- 비중 증가 중: +3점 추가
```

## 수집 파이프라인

### 단타봇 (수집 담당)
```
[매일 07:30] ewy_holdings_collector.py
  1. iShares CSV 다운로드
  2. 파싱 → 한국주식만 필터 (KRW 현금 제외)
  3. universe.json과 매칭 → 한글 종목명 보정
  4. 전일 데이터와 비교 → 변동/편입/편출 감지
  5. data_store/ewy_holdings.json 저장
  6. Supabase intelligence_ewy_holdings 업로드
```

### 퀀트봇 (표시 담당)
```
Supabase intelligence_ewy_holdings 테이블에서 읽어서
FLOWX 퀀트봇 탭에 카드 형태로 표시
```

## 파일 구조
```
scalper-agent/
  data/
    ewy_holdings_collector.py   ← 신규: CSV 수집 + 변동 분석
    upload_ewy_holdings.py      ← 신규: Supabase 업로드
  data_store/
    ewy_holdings.json           ← 오늘 보유종목
    ewy_holdings_prev.json      ← 전일 보유종목 (비교용)
```

## COO 배선
```
G1 (미국장 마감 후) — A11 이후
  A11B: ewy_holdings_collector → upload_ewy_holdings
```

## 주의사항
- iShares CSV는 **미국 영업일 기준** 갱신 → 한국 공휴일에도 미국 개장이면 갱신됨
- CSV 첫 몇 줄은 메타데이터 (펀드명, 기준일 등) → 파싱 시 스킵 필요
- `KRW` (현금), `CASH` 등 비주식 항목 필터 필요
- Ticker가 없는 행 (선물/옵션 등) 스킵
- 비중 합계가 100%를 약간 초과할 수 있음 (레버리지/현금 포함)
