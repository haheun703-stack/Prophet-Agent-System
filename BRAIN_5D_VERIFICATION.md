# BRAIN 1D~5D 풀 스택 검증 지시서

## 목적

BRAIN 5개 차원(1D NIGHTWATCH ~ 5D 유동성 사이클)이 실제 위기 상황에서 제대로 작동하는지 검증한다. **과거 실제 이벤트**를 재현해서 각 차원이 적시에 경고를 발생시키는지 확인하고, 미탐지·오탐지를 식별한다.

---

## 검증 1: 과거 위기 이벤트 백테스트

아래 5개 이벤트에 대해 각 차원별 시그널을 **사후적으로** 계산하라. FRED/Yahoo Finance/CFTC에서 해당 기간 데이터를 받아서, 기존 모듈의 계산 로직을 그대로 적용한다.

### 이벤트 목록

| # | 이벤트 | 날짜 | 검증 구간 (시작~종료) | 핵심 확인 포인트 |
|---|--------|------|-----------------------|------------------|
| 1 | 엔 캐리 청산 쇼크 | 2024-07-31 ~ 08-05 | 2024-06-01 ~ 2024-08-31 | 4D Yen Spec 극단 → 청산 직전 감지 여부 |
| 2 | SVB 은행 위기 | 2023-03-10 | 2023-01-01 ~ 2023-04-30 | 2D 신용스프레드 + MOVE 급등 선행 감지 여부 |
| 3 | 코로나 폭락 | 2020-02-20 ~ 03-23 | 2020-01-01 ~ 2020-04-30 | 3D 크로스에셋 상관 붕괴 감지 여부 |
| 4 | 2022 긴축 사이클 | 2022-01-01 ~ 10-13 | 2021-10-01 ~ 2023-01-31 | 5D 유동성 스코어 하락 전환 시점 |
| 5 | 2024 하반기 랠리 | 2024-08 ~ 2024-12 | 2024-06-01 ~ 2024-12-31 | 5D 유동성 팽창 + 4D COT 리스크온 전환 |

### 각 이벤트별 출력 형식

```
=== 이벤트 #1: 엔 캐리 청산 쇼크 ===

[1D NIGHTWATCH]
- 위기 D-30: 점수 XX → 레짐 YYYYY
- 위기 D-14: 점수 XX → 레짐 YYYYY
- 위기 D-7:  점수 XX → 레짐 YYYYY
- 위기 D-1:  점수 XX → 레짐 YYYYY
- 위기 당일: 점수 XX → 레짐 YYYYY
- 최초 경고일: YYYY-MM-DD (위기 대비 D-N)

[2D 채권 선행지표]
- 신용 스프레드 경고 최초 발동일: YYYY-MM-DD (D-N)
- MOVE 경고 최초 발동일: YYYY-MM-DD (D-N)
- 수익률곡선 경고 최초 발동일: YYYY-MM-DD (D-N)
- TED 경고 최초 발동일: YYYY-MM-DD (D-N)
- IMMINENT 판정 최초일: YYYY-MM-DD (D-N)

[3D 크로스에셋 스트레스]
- 스트레스 지수 추이: D-30(X.X) → D-14(X.X) → D-7(X.X) → D-1(X.X) → 당일(X.X)
- 상관 붕괴 쌍 수: N/4
- CRITICAL 최초 도달일: YYYY-MM-DD (D-N)

[4D COT 스마트머니]
- S&P Comm z-score 추이 (주간): [리스트]
- Gold Spec z-score 추이 (주간): [리스트]
- Yen Spec z-score 추이 (주간): [리스트]
- Oil Comm z-score 추이 (주간): [리스트]
- smart_money_score 추이: [리스트]
- RISK_OFF 최초 트리거일: YYYY-MM-DD (D-N)

[5D 유동성 사이클]
- RRP 추이 (월간): [리스트]
- TGA 추이 (월간): [리스트]
- M2 YoY 추이 (월간): [리스트]
- 유동성 스코어 추이: [리스트]
- DRAIN/CONTRACTION 최초 전환일: YYYY-MM-DD (D-N)

[종합 판정]
- 가장 먼저 경고한 차원: XD (D-N)
- 가장 늦게 경고한 차원: XD (D-N)
- 미탐지 차원: 없음 / XD (사유: ...)
- 선행일수 평균: N일
```

---

## 검증 2: 차원 간 정합성 테스트

과거 데이터에서 아래 조건을 확인하라:

### 2-1. 방향 일치성

| 검증 항목 | 기대 결과 |
|-----------|-----------|
| 3D CRITICAL 발동 시, 1D가 같은 주 내에 RISK_OFF 도달 | 90% 이상 |
| 2D IMMINENT 발동 후, 1D 레짐이 7일 내 한 단계 악화 | 70% 이상 |
| 4D RISK_OFF 발동 시, 5D가 같은 월에 CONTRACTION 이하 | 60% 이상 |
| 5D EXPANSION 구간에서 1D가 RISK_ON 비율 | 50% 이상 |

### 2-2. 모순 검출

아래는 비정상적 조합이다. 발생 빈도를 세고, 실제 발생한 경우 원인을 분석하라:

- 3D CRITICAL인데 2D CLEAR → 상관 붕괴가 있는데 채권이 안 움직인 경우?
- 5D EXPANSION인데 4D RISK_OFF → 유동성은 풀리는데 기관이 빠지는 경우?
- 1D RISK_ON인데 3D HIGH 이상 → 주식은 오르는데 상관 붕괴?

각 모순 조합이 실제 발생하면 "정상적 모순인지(예: 시차)" vs "버그인지" 판별하라.

---

## 검증 3: 엣지 케이스 & 장애 내성

### 3-1. 데이터 누락 시뮬레이션

각 모듈에 대해 아래 시나리오를 실행하라:

| 시나리오 | 테스트 방법 | 기대 동작 |
|----------|-------------|-----------|
| FRED API 다운 | requests.get을 mock으로 Timeout 발생 | 마지막 캐시값 사용, 에러 로그 남김, 스코어 계산 중단 아님 |
| CFTC 파일 미발표 (공휴일) | deafut.txt URL 404 반환 mock | 직전 주 데이터 유지, 텔레그램에 "데이터 미갱신" 표시 |
| Yahoo Finance 티커 변경/상폐 | 존재하지 않는 티커로 교체 | fallback 티커 사용 or graceful skip |
| M2 데이터 2개월 미갱신 | M2 최신값 날짜를 60일 전으로 조작 | forward-fill 하되 "M2 데이터 오래됨" 경고 |

### 3-2. 극단값 테스트

각 모듈에 아래 인위적 데이터를 주입하고 출력을 확인:

```python
# 2D: 4개 지표 전부 동시 경고
test_regime_leading(
    credit_spread_expanding=True,  # 3일 연속
    yield_curve_inverting=True,
    move_spiking=True,
    ted_spiking=True
)
# 기대: 확신도 100%, IMMINENT → BRAIN이 방어 배분 최대치 적용

# 3D: 4쌍 전부 상관 붕괴
test_cross_asset_stress(
    gold_kospi_corr=+0.5,
    usd_kospi_corr=+0.5,
    bond_kospi_corr=+0.5,
    oil_kospi_corr=+0.5
)
# 기대: 스트레스 10/10, CRITICAL, 1.5배 증폭, BRAIN 현금 최대

# 4D: 모든 계약 극단 동시
test_cot_smartmoney(
    sp_comm_zscore=-3.0,
    gold_spec_zscore=+3.0,
    yen_spec_zscore=-3.0,
    oil_comm_zscore=-3.0
)
# 기대: smart_money_score -10 (최대 RISK_OFF)

# 5D: RRP 0 + TGA 급등 + M2 마이너스
test_liquidity_cycle(
    rrp_change_pct=-90,
    tga_change_pct=+200,
    m2_yoy=-2.0
)
# 기대: 유동성 스코어 최저, LIQUIDITY_DRAIN
```

### 3-3. 전체 파이프라인 통합 테스트

`run_brain()`을 호출했을 때:

1. 1D~5D가 **순서대로** 실행되는지 (센서 적용 순서: 3D→2D→4D→5D)
2. 한 차원이 에러 나도 나머지 차원은 정상 실행되는지
3. 텔레그램 리포트에 경고 차원만 표시되고, NEUTRAL 차원은 생략되는지
4. 배분 조정값이 누적 적용될 때 합계가 100%를 초과하지 않는지
5. 모든 차원 동시 최악 시 최종 배분이 합리적인지 (현금 > 60%, 스윙 < 20%)

---

## 검증 4: 현재 실시간 크로스 체크

현재(2026-03-07) 각 차원 출력이 시장 상황과 정합하는지 수동 확인:

| 확인 항목 | 방법 |
|-----------|------|
| 1D CAUTIOUS(-1.0) | 오늘 KOSPI 종가/VIX/환율과 비교해서 CAUTIOUS가 합리적인가? |
| 2D CLEAR | HYG/LQD 비율, TNX-IRX, TLT 변동성을 직접 조회해서 정말 이상 없는지? |
| 3D NORMAL(0.0) | Gold↔KOSPI +0.20 외에 나머지 3쌍이 정말 정상 범위인지? |
| 4D NEUTRAL(-2.6) | CFTC 최신 보고서와 파싱 결과가 일치하는지? 숫자 검증 |
| 5D NEUTRAL(-0.9) | FRED에서 RRP/TGA/M2 직접 조회, 모듈 출력값과 일치하는지? |

---

## 출력물

1. **`BRAIN_5D_BACKTEST_RESULTS.md`** — 5개 이벤트 백테스트 결과 전체
2. **`BRAIN_5D_CONSISTENCY_REPORT.md`** — 차원 간 정합성 + 모순 분석
3. **`BRAIN_5D_STRESS_TEST.md`** — 엣지 케이스 + 극단값 테스트 결과
4. **`BRAIN_5D_LIVE_CROSSCHECK.md`** — 현재 실시간 데이터 수동 검증
5. **발견된 버그/개선점 목록** — 각 보고서 말미에 `[ACTION REQUIRED]` 태그로 표시

---

## 우선순위

검증 3 (엣지 케이스) → 검증 4 (실시간 크로스 체크) → 검증 1 (백테스트) → 검증 2 (정합성)

사유: 엣지 케이스에서 파이프라인이 죽으면 나머지가 의미 없고, 실시간 숫자가 틀리면 백테스트 결과도 못 믿는다. 기초 신뢰성 먼저 확보하고 역사적 검증으로 간다.
