# FLOWX 스윙시스템 페이지 스펙

> **프로젝트**: Body Hunter v4 — 한국 주식 단타봇(자동매매 시스템)
> **대상**: 웹봇 (프론트엔드 개발용)
> **작성일**: 2026-03-29
> **데이터 소스**: Supabase (2개 테이블)

---

## 1. 페이지 개요

FLOWX 스윙시스템 페이지는 **BRAIN(시장분석 AI)이 자동으로 결정한 자산배분 + 종목 추천**을 보여주는 대시보드입니다.

**핵심 가치**: 사용자는 매일 이 페이지만 보면 "오늘 스윙 포지션을 잡을지/관망할지/방어할지"를 바로 알 수 있음.

---

## 2. Supabase 테이블 2개

### 2-1. `dashboard_swing` (메인 대시보드)

**PK**: `date` (YYYY-MM-DD)
**갱신 주기**: 매일 07:30 KST (COO G7 파이프라인)

| 컬럼 | 타입 | 설명 | 예시 |
|------|------|------|------|
| `date` | text | 날짜 | "2026-03-29" |
| `brain_verdict` | text | AI 판정 | "공격" / "표준" / "방어" / "관망" |
| `brain_pct` | int | BRAIN 점수 (0~100) | 72 |
| `brain_reason` | text | 판정 근거 (300자) | "표준(72) — 매크로중립(+10) \| ..." |
| `regime` | text | 시장 체제 | "MOMENTUM" / "NORMAL" / "관망" |
| `regime_severity` | int | 체제 강도 | 0~100 |
| `regime_desc` | text | 체제 설명 | "모멘텀 장세" |
| **자산배분 (%)** | | | |
| `alloc_swing` | int | 스윙 비중 | 60 |
| `alloc_gold_etf` | int | 금ETF 비중 | 10 |
| `alloc_inverse` | int | 인버스 비중 | 0 |
| `alloc_group_etf` | int | 그룹ETF 비중 | 10 |
| `alloc_small_cap` | int | 소형주 비중 | 10 |
| `alloc_cash` | int | 현금 비중 | 10 |
| **추천 종목** | | | |
| `picks` | JSONB | 스윙 종목 리스트 | (아래 상세) |
| `etf_picks` | JSONB | ETF 추천 리스트 | (아래 상세) |
| `watchlist` | JSONB | 관망모드 반등감시 | (아래 상세) |
| **NXT 야간매매** | | | |
| `nxt_signal` | text | NXT 시그널 이모지 | "BUY" / "SELL" / "☠️" |
| `nxt_signal_text` | text | 시그널 텍스트 | "매수 유망" / "인버스 강력" |
| `nxt_score` | int | NXT 점수 (-10~+10) | 5 |
| `nxt_reason` | text | NXT 근거 | "VIX 20 + 한국(+2.5)" |
| `nxt_targets` | JSONB | NXT 대상 종목 | (아래 상세) |
| **시장 지표** | | | |
| `vix` | float | VIX 지수 | 27.59 |
| `nasdaq_pct` | float | 나스닥 등락률(%) | 0.329 |
| `usdkrw` | float | 원/달러 환율 | 1507.28 |
| `oil_pct` | float | 유가 등락률(%) | 0.455 |
| `gold_pct` | float | 금 등락률(%) | 1.684 |
| `silver_pct` | float | 은 등락률(%) | 3.19 |
| **분석** | | | |
| `analysis` | JSONB | 5대 분석 요약 | (아래 상세) |
| `portfolio` | JSONB | 포트폴리오 성과 | (아래 상세) |
| **센서 (추후 확장)** | | | |
| `smart_money_score` | int | 스마트머니 점수 | 0~100 |
| `smart_money_signal` | text | 스마트머니 시그널 | |
| `stress_index` | int | 스트레스 지수 | 0~100 |
| `stress_level` | text | 스트레스 레벨 | |
| `rotation_signal` | text | 로테이션 시그널 | |
| `liquidity_score` | int | 유동성 점수 | 0~100 |
| `market_comment` | text | 한줄 코멘트 | "방향 불명확 — 현금이 포지션입니다" |
| `generated_at` | timestamp | 생성 시각 | |

### 2-2. `swing_signals` (상세 분석)

**PK**: `date` (YYYY-MM-DD)
**갱신 주기**: `dashboard_swing`과 동시

| 컬럼 | 타입 | 설명 |
|------|------|------|
| `date` | text | 날짜 |
| `brain_verdict` | text | AI 판정 |
| `brain_pct` | int | BRAIN 점수 |
| `brain_reason` | text | 판정 근거 |
| `min_grade_applied` | text | 적용 최소 등급 (AAA/AA/A/NONE) |
| `market_comment` | text | 한줄 코멘트 |
| `picks` | JSONB | 종목 리스트 |
| `etf_picks` | JSONB | ETF 리스트 |
| `portfolio` | JSONB | 포트폴리오 성과 |
| `analysis` | JSONB | 5대 분석 |
| `watchlist` | JSONB | 반등감시 종목 |

---

## 3. JSONB 필드 상세

### 3-1. `picks` (스윙 종목)

```json
[
  {
    "code": "005930",
    "name": "삼성전자",
    "grade": "AAA",
    "score": 87.5,
    "sector": "반도체",
    "rr_ratio": 2.3,
    "rr_verdict": "GOOD",
    "entry_price": 68500,
    "target_price": 72000,
    "stop_price": 66000,
    "hold_days": 3,
    "conviction": "HIGH",
    "catalyst": "trix_divergence + tv_quiet_acc + 모멘텀",
    "regime": "MOMENTUM",
    "tv_pattern": "QUIET_ACCUMULATION",
    "news_sentiment": "POSITIVE",
    "tech_score": 85,
    "supply_signal": "BUY",
    "nat_power_grade": "POWER_BUY"
  }
]
```

**UI 표현 팁:**

- `upside_pct` = `(target_price - entry_price) / entry_price * 100` 으로 프론트에서 계산
- `conviction`: HIGH=녹색, MEDIUM=파랑, LOW=회색
- `rr_verdict`: EXCELLENT(2.5+) / GOOD(1.5+) / FAIR(1.0+) / POOR(<1.0)
- `supply_signal`: BUY=녹색, SELL=빨강, NEUTRAL=회색

### 3-2. `etf_picks` (ETF 추천)

```json
[
  {
    "code": "069500",
    "name": "KODEX 200",
    "category": "시장대표",
    "signal": "BUY",
    "entry": 35000,
    "sl": 33500,
    "tp": 37000,
    "reason": "저평가 + 수급 유입",
    "holding_days": 5
  }
]
```

### 3-3. `watchlist` (관망모드 반등감시)

```json
[
  {
    "code": "004250",
    "name": "NPC",
    "sector": "화학",
    "grade": "BB",
    "score": 53.6,
    "reason": "반등 감시 — 모멘텀 레짐, 외국인 수급 양호",
    "trigger": "종가 4,015원 돌파 + 거래량 증가 시 진입 검토"
  }
]
```

### 3-4. `nxt_targets` (NXT 야간매매 대상)

```json
[
  {
    "code": "005930",
    "name": "삼성전자",
    "sector": "반도체",
    "tier": 1,
    "priority": 1,
    "supply_score": 82,
    "is_etf": false
  }
]
```

### 3-5. `analysis` (5대 분석 요약)

```json
{
  "flow_summary": "외국인 순매수 3일차, 기관 소폭 매도",
  "risk_summary": "VIX 27.6 (EXTREME), 원/달러 1507",
  "macro_summary": "미국 금리 동결 기조, 한국 수출 호조",
  "sector_summary": "반도체 HOT, 2차전지 WARMING, 바이오 COLD",
  "commodity_summary": "금 +1.7%, 유가 +0.5%, 은 +3.2%"
}
```

### 3-6. `portfolio` (포트폴리오 성과)

```json
{
  "win_rate": 65.2,
  "brain_pct": 72,
  "total_trades": 15,
  "current_picks": 3,
  "brain_cash_ratio": 28
}
```

---

## 4. BRAIN 역방향 필터 (자동 리스크 관리)

| brain_pct | 판정 | 최소 등급 | 최대 종목수 | 자산배분 |
|-----------|------|----------|-----------|---------|
| 80~100 | 공격 | A 이상 | 5 | 스윙 70%+ |
| 60~79 | 표준 | AA 이상 | 3 | 스윙 50~70% |
| 40~59 | 방어 | AAA만 | 2 | 스윙 30~50%, 금/인버스 증가 |
| 0~39 | 관망 | 없음 (0종목) | 0 | 현금 80~100%, watchlist만 표시 |

**UI 표현 가이드:**
- `공격`: 녹색 계열, 활성적 느낌
- `표준`: 파란색 계열, 안정적 느낌
- `방어`: 노란/주황 계열, 주의 느낌
- `관망`: 빨간/회색 계열, 경고 느낌

---

## 5. 페이지 구성 제안 (5섹션)

### Section A: 헤더 — BRAIN 판정 카드
```
┌──────────────────────────────────┐
│  [공격/표준/방어/관망]  BRAIN 72점  │
│  "표준모드 — AA등급 이상 3종목 추천"  │
│  2026-03-29 07:30 갱신              │
└──────────────────────────────────┘
```
- `brain_verdict` + `brain_pct` + `brain_reason` (한줄)
- `market_comment` 서브텍스트

### Section B: 자산배분 파이차트
```
┌──────────────────────────────────┐
│  🥧 자산배분                      │
│  스윙 60% | 금ETF 10% | 현금 30%  │
│  [파이차트 시각화]                 │
└──────────────────────────────────┘
```
- `alloc_swing`, `alloc_gold_etf`, `alloc_inverse`, `alloc_group_etf`, `alloc_small_cap`, `alloc_cash`
- 합계 항상 100%

### Section C: 추천 종목 테이블
```
┌──────────────────────────────────────────────┐
│  📊 스윙 추천 (AA 이상)                        │
│  ┌──────┬────────┬─────┬──────┬─────┬──────┐ │
│  │ 종목 │ 등급   │ 점수 │ 목표가│ SL  │ R:R │  │
│  ├──────┼────────┼─────┼──────┼─────┼──────┤ │
│  │삼성전│  AAA   │ 87  │72,000│66,000│ 2.3 │  │
│  └──────┴────────┴─────┴──────┴─────┴──────┘ │
│                                              │
│  📈 ETF 추천                                  │
│  [etf_picks 테이블]                            │
│                                              │
│  👁️ 반등감시 (관망모드시)                      │
│  [watchlist 테이블 — trigger 조건 포함]         │
└──────────────────────────────────────────────┘
```
- `picks` → 메인 종목 테이블
- `etf_picks` → ETF 추천 (별도 섹션)
- `watchlist` → 관망모드일 때만 표시 (trigger 포함)
- picks=0이면 "관망모드 — 종목 추천 없음" 표시

### Section D: NXT 야간매매 카드
```
┌──────────────────────────────────┐
│  🌙 NXT 야간매매                  │
│  시그널: BUY (+7.5)               │
│  "VIX 20 + 한국(+2.5) → 매수 유망" │
│  대상: [nxt_targets 리스트]        │
└──────────────────────────────────┘
```
- `nxt_signal` + `nxt_score` + `nxt_reason`
- `nxt_targets` 종목 리스트

### Section E: 시장 센서 + 글로벌 지표
```
┌──────────────────────────────────┐
│  🌍 글로벌 지표                   │
│  VIX 27.6 | NQ +0.3% | $/W 1507 │
│  유가 +0.5% | 금 +1.7% | 은 +3.2%│
│                                  │
│  📡 5대 분석                      │
│  [analysis 요약 — 5줄]            │
└──────────────────────────────────┘
```
- 시장지표 6개: `vix`, `nasdaq_pct`, `usdkrw`, `oil_pct`, `gold_pct`, `silver_pct`
- `analysis` 5개 요약문

---

## 6. API 쿼리 예시 (Supabase JS)

```javascript
// 오늘의 스윙 대시보드
const { data } = await supabase
  .from('dashboard_swing')
  .select('*')
  .eq('date', '2026-03-29')
  .single()

// 최근 7일 히스토리 (차트용)
const { data: history } = await supabase
  .from('dashboard_swing')
  .select('date, brain_pct, brain_verdict, alloc_swing, alloc_cash, vix, nasdaq_pct')
  .order('date', { ascending: false })
  .limit(7)

// 최근 추천 종목이 있는 날만
const { data: withPicks } = await supabase
  .from('swing_signals')
  .select('date, brain_verdict, picks, etf_picks')
  .not('picks', 'eq', '[]')
  .order('date', { ascending: false })
  .limit(5)
```

---

## 7. 참고사항

- **갱신 시간**: 매일 07:30 KST (장 시작 전). 주말/공휴일에도 갱신되지만, 데이터는 마지막 거래일 기준
- **비거래일**: brain_pct=0, picks=[], watchlist만 존재할 수 있음
- **alloc 합계**: 항상 100%. 관망모드면 alloc_cash=100
- **picks 정렬**: score 내림차순 (가장 높은 점수가 첫번째)
- **NXT 야간매매**: 16:35 판단 → 다음날 아침까지 유효. 장중에는 참고용
- **센서 필드** (smart_money, stress 등): 현재 0 — 향후 TIER2에서 활성화 예정
