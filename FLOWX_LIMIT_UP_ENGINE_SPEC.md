# FLOWX 웹봇 지시서 — 상한가 엔진 3섹션 패널

> **프로젝트**: Body Hunter v4 — 한국 주식 단타봇(자동매매 시스템)
> KIS API(한국투자증권) 실계좌 매매, 텔레그램 봇 UI, 7-Group COO 파이프라인,
> BRAIN 시장분석, NXT 야간매매, FLOWX VIP 콘텐츠를 포함합니다.
> CFO(재무)/CTO(기술)/COO(운영) 3-Officer 구조로 운영됩니다.

## 개요

상한가(+30%) 및 10%+ 급등 패턴을 감지하고 **눌림목 + 수급 확인 후 분할매수**하는 자동 시그널 엔진.
백테스트 확정 파라미터(100% 승률, 37건, 6개월) 기반으로 **페이퍼 트레이딩 → 실매매** 순차 전환 중.

**v3 핵심 변경**:
- 수급 분석: 외국인/기관 5일 순매수 + 소진율 → 매집중/이탈중/중립 등급
- 분할매수 3단계: 1차 50% / 2차 30% / 3차 20% 구체적 가격 제시
- "어떻게 사야 하는지"까지 안내하는 실전형 시그널

**품질 유니버스 필터 (v2)**:
전략2(눌림목 대기)는 **EWY 바스켓 80종목 + FLOWX 섹터유니버스 130종목 = 175종목** 내에서만 허용.
10%+ 급등해도 품질 유니버스에 없는 잡주는 자동 제외됨.

**3섹션 순서**:
1. 시장판단 & 전략 (기존 BRAIN)
2. 상한가 엔진 (신규 — 이 지시서)
3. 매집합류 (기존 기관 매집 레이더)

---

## 배치 위치 (3-Tier 분산 배치)

> **참고**: `FLOWX_SWING_UNIFIED_SPEC.md` v2의 3-Tier 구조에 따라 상한가 엔진 콘텐츠는
> 더 이상 독립 3섹션이 아니라 **Tier 1 / Tier 2 / Tier 3에 분산** 배치됩니다.

```
TIER 1: 오늘의 핵심 (ACTION)
┌─────────────────────────────────────────────────┐
│ 1-B. 오늘의 추천 — 4탭 통합                       │
│   [스윙 3] [단타TOP 7] [NXT야간 5] [상한가 2]     │  ← triggered 시그널 + 감시풀(접힘)
│                                 ▲ 이 탭          │
└─────────────────────────────────────────────────┘

TIER 2: 시장 현황 (CONTEXT)
┌─────────────────────────────────────────────────┐
│ 2-C. AI 시장판단 (BRAIN Quant)                    │  ← quant_market_brain
│   리스크 등급 + 투자비중 + HOT/COLD 섹터          │
└─────────────────────────────────────────────────┘

TIER 3: 심층 분석 (DEEP DIVE, 기본 접힘)
┌─────────────────────────────────────────────────┐
│ 3-A. 성적표 — [단타] [NXT] [상한가Paper] 탭       │  ← intelligence_limit_up_performance
│                              ▲ 이 탭             │
└─────────────────────────────────────────────────┘
```

### 분산 배치 상세

| Tier | 섹션 | 상한가 엔진 콘텐츠 | 데이터 소스 |
|------|------|------------------|-----------|
| **1-B** | 오늘의 추천 [상한가] 탭 | triggered 시그널 카드 + 감시풀(접힘) | `intelligence_limit_up_signals.triggered / watchlist` |
| **2-C** | AI 시장판단 | 시장 리스크 등급 (상한가 진입 판단 참고) | `quant_market_brain` |
| **3-A** | 성적표 [상한가Paper] 탭 | Paper Trading 요약 + 포지션 + 청산 | `intelligence_limit_up_performance` |

> **UI 디자인 가이드**(아래)의 카드/테이블 레이아웃은 그대로 유지.
> 다만 독립 패널이 아니라 탭 내부 컨텐츠로 렌더링됨.

---

## 데이터 소스

### 테이블 1: `intelligence_limit_up_signals`

일일 시그널 + 감시풀.

| 컬럼 | 타입 | 설명 |
|------|------|------|
| date | DATE PK | 스캔 날짜 |
| scanned_at | TIMESTAMPTZ | 스캔 시각 |
| total_scanned | INT | 스캔 대상 종목수 |
| triggered | JSONB | 트리거된 시그널 배열 |
| triggered_count | INT | 트리거 건수 |
| watchlist | JSONB | 감시중 종목 배열 |
| watchlist_count | INT | 감시풀 건수 |

#### triggered JSONB 구조 (각 항목)
```json
{
  "code": "011000",
  "name": "진원생명과학",
  "entry_type": "next_day",
  "signal_date": "2026-05-09",
  "signal_close": 1129.0,
  "entry_price": 1095,
  "entry_low": 1095,
  "entry_high": 1129,
  "tp_price": 1204,
  "limit_count": 3,
  "overheat_pct": 44.2,
  "origin_price": 783.0,
  "sector": "기타",
  "market_cap": 790,
  "volume_ratio": 0.91,
  "flow_foreign_5d": -120,
  "flow_inst_5d": 85,
  "flow_grade": "중립",
  "foreign_exh_rate": 0.57,
  "split_plan": [
    {"tranche": 1, "pct": 50, "price": 1095, "label": "1차 (시가-3%)"},
    {"tranche": 2, "pct": 30, "price": 1129, "label": "2차 (시가)"},
    {"tranche": 3, "pct": 20, "price": 1049, "label": "3차 (눌림-7%)"}
  ],
  "reasons": ["상한가 +29.9% (3회차)", "원점대비 +44%", "상한가마감", "수급: 중립", "외인5일 -120백만"]
}
```

#### 필드 설명 — triggered
| 필드 | 의미 | 표시 방법 |
|------|------|-----------|
| code | 종목코드 | 내부 식별용 |
| name | 종목명 | 메인 텍스트 |
| entry_type | 진입 유형 | `next_day` → "분할매수" 뱃지 / `pullback` → "눌림목진입" 뱃지 |
| signal_close | 시그널일 종가 | 현재가 표시 |
| entry_price | 추천 진입가 | 진입가 표시 |
| entry_low / entry_high | 진입 범위 | "1,095 ~ 1,129" |
| tp_price | 목표가 (+10%) | 목표가 표시 |
| limit_count | 상한가 회차 | "3회차 상한가" |
| overheat_pct | 원점 대비 과열도 | "+44%" |
| origin_price | 첫 상한가 원점가 | 참고용 |
| market_cap | 시총 (억원) | "790억" |
| volume_ratio | 거래량 비율 | 참고용 (1.0 이상 = 평소 이상) |
| **flow_foreign_5d** | **5일 외국인 순매수 (백만원)** | **양수=녹색 "+120백만" / 음수=빨강 "-120백만"** |
| **flow_inst_5d** | **5일 기관 순매수 (백만원)** | **양수=녹색 / 음수=빨강** |
| **flow_grade** | **수급 등급** | **"매집중"=녹색뱃지 / "이탈중"=빨강뱃지 / "중립"=회색뱃지** |
| **foreign_exh_rate** | **외국인 소진율 (%)** | **"소진율 0.57%"** |
| **split_plan** | **분할매수 계획 (3단계)** | **아래 분할매수 UI 참고** |
| reasons | 시그널 사유 | 태그 뱃지로 표시 |

#### watchlist JSONB 구조 (각 항목)

```json
{
  "code": "012330",
  "name": "현대모비스",
  "signal_close": 509000,
  "signal_date": "2026-05-09",
  "monitor_until": "2026-05-16",
  "entry_type": "pullback",
  "sector": "운송장비",
  "market_cap": 240000,
  "flow_foreign_5d": 250,
  "flow_inst_5d": -80,
  "flow_grade": "중립",
  "foreign_exh_rate": 43.23
}
```

---

### 테이블 2: `intelligence_limit_up_performance`

페이퍼 트레이딩 일일 성적.

| 컬럼 | 타입 | 설명 |
|------|------|------|
| date | DATE PK | 기록 날짜 |
| new_entries | INT | 당일 신규 진입 건수 |
| closed_count | INT | 당일 청산 건수 |
| active_count | INT | 현재 보유 건수 |
| positions | JSONB | 보유 포지션 상세 배열 |
| closed_trades | JSONB | 당일 청산 상세 배열 |
| total_trades | INT | 누적 총 거래 수 |
| win_rate | REAL | 누적 승률 (%) |
| avg_pnl | REAL | 평균 수익률 (%) |
| cum_return | REAL | 누적 수익률 (%) |
| mdd | REAL | 최대 낙폭 (%) |
| strategy1_trades | INT | 전략1(즉시진입) 거래수 |
| strategy1_wins | INT | 전략1 승수 |
| strategy2_trades | INT | 전략2(눌림목) 거래수 |
| strategy2_wins | INT | 전략2 승수 |
| cash | INT | 잔여 현금 (원) |
| total_value | INT | 포트폴리오 평가액 (원) |

#### positions JSONB 구조
```json
{
  "code": "011000",
  "name": "진원생명과학",
  "entry_price": 1095,
  "current_price": 1095,
  "pnl_pct": 0.0,
  "hold_days": 0,
  "tp_price": 1204,
  "entry_date": "2026-05-09",
  "shares": 1858
}
```

#### closed_trades JSONB 구조
```json
{
  "code": "429270",
  "name": "시지트로닉스",
  "entry_price": 10010,
  "exit_price": 11100,
  "pnl_pct": 10.9,
  "pnl_krw": 176580,
  "reason": "TARGET",
  "hold_days": 3
}
```

---

## UI 디자인 가이드

### 전체 레이아웃

```
┌─────────────────────────────────────────────────┐
│ ⚡ 상한가 엔진 · 2026-05-09 · Paper Trading     │
│ ────────────────────────────────────────────────│
│                                                 │
│ [시그널 섹션]                                    │
│ ┌─────────────────────────────────────────────┐ │
│ │ 🔴 분할매수  진원생명과학 (011000)            │ │
│ │ 3회차 상한가 · 원점 +44% · 시총 790억        │ │
│ │                                             │ │
│ │ 📊 수급: 중립  외인 -120백만  기관 +85백만    │ │
│ │    소진율 0.57%                              │ │
│ │                                             │ │
│ │ 💰 분할매수 계획                              │ │
│ │   1차 (시가-3%)    1,095원  50%             │ │
│ │   2차 (시가)       1,129원  30%             │ │
│ │   3차 (눌림-7%)    1,049원  20%             │ │
│ │                                             │ │
│ │ 목표: 1,204 (+10%) · 만기 20영업일           │ │
│ └─────────────────────────────────────────────┘ │
│                                                 │
│ [감시풀] 3건 눌림목+수급 대기 중 (품질유니버스)    │
│                                                 │
│ ────────────────────────────────────────────────│
│                                                 │
│ [Paper Trading 성적표]                           │
│ ┌──────────┬──────────┬──────────┬────────────┐ │
│ │ 가상자금   │ 누적수익  │ 승률     │ MDD        │ │
│ │ 1,000만원  │ +1.75%   │ ---%     │ -5.1%      │ │
│ └──────────┴──────────┴──────────┴────────────┘ │
│                                                 │
│ [보유 포지션]                                    │
│  진원생명과학  1,095 → 1,095 (±0.0%) D+0/20    │
│  시지트로닉스  10,010 → 10,010 (±0.0%) D+0/20  │
│                                                 │
│ * 가상매매 — 실제 주문 없음 · TP +10% · 만기 20일 │
└─────────────────────────────────────────────────┘
```

### 섹션 1: 시그널 헤더

```
⚡ 상한가 엔진 · 2026-05-09 16:20
```
- 아이콘: ⚡ (번개)
- 날짜 + 스캔 시각 (`scanned_at`)
- 우측에 `Paper Trading` 뱃지 (실매매 전환 전까지)

### 섹션 2: 트리거 시그널 카드

`triggered` 배열 순회. 각 카드:

```
┌──────────────────────────────────────────┐
│ [분할매수]  진원생명과학 (011000)          │
│                                          │
│ 3회차 상한가 · 원점+44% · 시총 790억      │
│                                          │
│ 📊 수급: 중립  외인-120백만  기관+85백만   │
│    소진율 0.57%                           │
│                                          │
│ 💰 분할매수 계획                           │
│   1차 (시가-3%)    1,095원  50%          │
│   2차 (시가)       1,129원  30%          │
│   3차 (눌림-7%)    1,049원  20%          │
│                                          │
│ 목표: 1,204 (+10%)                       │
│                                          │
│ 상한가 +29.9% · 원점대비 +44% · 상한가마감 │
└──────────────────────────────────────────┘
```

#### 뱃지 색상

| entry_type | 뱃지 텍스트 | 색상 |
|------------|-----------|------|
| `next_day` | 분할매수 | 빨간색 (High conviction) |
| `pullback` | 눌림목진입 | 노란색 (감시 중) |

#### 카드 하이라이트

- `limit_count >= 3` → 카드 테두리 강조 (골드/빨강)
- `overheat_pct >= 200` → "과열주의" 경고 표시 (주황)
- `overheat_pct < 100` → "적정 구간" 표시 (녹색)

#### 수급 표시 (v3 신규)

`flow_grade` 값에 따라 수급 섹션 색상:

| flow_grade | 텍스트 | 색상 | 의미 |
|------------|--------|------|------|
| `매집중` | 📈 매집중 | 녹색 배경 | 외인/기관 순매수 우위 (진입 유리) |
| `이탈중` | 📉 이탈중 | 빨강 배경 | 외인/기관 순매도 우위 (주의) |
| `중립` | ➡️ 중립 | 회색 배경 | 혼조세 |
| `미확인` | ❓ 미확인 | 회색 배경 | 수급 데이터 없음 |

- `flow_foreign_5d` / `flow_inst_5d`: 양수 → 녹색, 음수 → 빨강
- `foreign_exh_rate`: 30% 이상 → "외국인 관심 종목" 표시

#### 분할매수 계획 표시 (v3 신규)

`split_plan` 배열을 테이블로 표시:

```
💰 분할매수 계획
┌────────────────┬──────────┬──────┐
│ 단계            │ 가격     │ 비중 │
├────────────────┼──────────┼──────┤
│ 1차 (시가-3%)   │ 1,095원  │ 50% │
│ 2차 (시가)      │ 1,129원  │ 30% │
│ 3차 (눌림-7%)   │ 1,049원  │ 20% │
└────────────────┴──────────┴──────┘
```

- `tranche` 1번 → 굵은 글씨 (최우선 진입)
- `pct` → 가로 프로그레스바로 표시 가능

#### reasons 태그

`reasons` 배열의 각 항목을 작은 뱃지로 표시:

- "상한가" 포함 → 빨간 뱃지
- "원점" 포함 → 파란 뱃지
- "상한가마감" → 회색 뱃지
- "수급: 매집중" → 녹색 뱃지
- "수급: 이탈중" → 빨강 뱃지
- "외인5일" / "기관5일" → 파란 뱃지

### 섹션 3: 감시풀 (접기/펼치기)

`watchlist` 배열. 기본 접힘 상태. 헤더만 표시:

```
📋 감시풀 3건 — 눌림목+수급 대기 중 (품질유니버스 한정)    [▼]
```

펼치면 리스트:

```
  현대모비스(012330)       509,000원  감시~05/16  수급:중립  [EWY·자동차]
  레인보우로보틱스(277810)  784,000원  감시~05/16  수급:중립  [로봇]
  로보티즈(108490)         334,500원  감시~05/16  수급:중립  [로봇]
```

- `monitor_until` 날짜가 오늘이면 "만기" 표시 (빨강)
- 종목수가 0이면 섹션 숨김

### 섹션 4: Paper Trading 성적표

```
━━ Paper Trading 성적표 ━━━━━━━━━━━━━━━━
```

#### 4-1. 요약 카드 (4칸 가로 배치)

```
┌──────────┬──────────┬──────────┬────────────┐
│ 가상자금   │ 누적수익  │ 승률     │ MDD        │
│ 1,000만원  │ +1.75%   │ 0%       │ -5.1%      │
│            │ (연환산)  │ (0/0건)  │            │
└──────────┴──────────┴──────────┴────────────┘
```

필드 매핑:
| 카드 | 소스 필드 | 표시 |
|------|----------|------|
| 가상자금 | `total_value` | `10,175,370` → "1,017만원" |
| 누적수익 | `cum_return` | "+1.75%" 양수=녹색, 음수=빨강 |
| 승률 | `win_rate` | "67%" + `total_trades`건 |
| MDD | `mdd` | "-5.1%" (항상 빨강 톤) |

#### 4-2. 전략별 성적 (옵션)

```
전략1 (즉시진입): 2건 · 승률 ---%
전략2 (눌림목):   0건 · 승률 ---%
```

필드: `strategy1_trades`, `strategy1_wins`, `strategy2_trades`, `strategy2_wins`

#### 4-3. 보유 포지션 테이블

`positions` 배열:

```
종목명          진입가      현재가     수익률    보유일
진원생명과학    1,095      1,095     ±0.0%    D+0/20
시지트로닉스    10,010     10,010    ±0.0%    D+0/20
```

- `pnl_pct > 0` → 녹색 / `< 0` → 빨강
- `hold_days >= 15` → 보유일 주황색 (만기 임박)
- TP까지 남은 % 프로그레스바 (옵션):
  - `progress = (current_price - entry_price) / (tp_price - entry_price) * 100`

#### 4-4. 당일 청산 (있을 때만)

`closed_trades` 배열:

```
[TARGET] 시지트로닉스  10,010 → 11,100  +10.9%  D+3  +176,580원
```

- reason별 색상:
  - `TARGET` → 녹색 (목표 도달)
  - `TIME_STOP` → 회색 (만기 청산)

#### 4-5. 푸터

```
* 가상매매 — 실제 주문 없음
* TP +10% · SL 없음 · 만기 20영업일
* 백테스트 기준: 100% 승률 (37건, 6개월)
```

### 빈 상태 처리

**시그널 0건:**
```
⚡ 상한가 엔진 · 2026-05-12
오늘은 상한가 시그널이 없습니다
```

**포지션 0건:**
```
Paper Trading 성적표
보유 포지션 없음 — 시그널 대기 중
```

---

## 데이터 쿼리 예시

### 오늘 시그널 (최신 1건)
```javascript
const { data: signals } = await supabase
  .from('intelligence_limit_up_signals')
  .select('*')
  .order('date', { ascending: false })
  .limit(1)
  .single();

// triggered 배열 표시
const triggered = signals.triggered || [];
const watchlist = signals.watchlist || [];
```

### 오늘 성적 (최신 1건)
```javascript
const { data: perf } = await supabase
  .from('intelligence_limit_up_performance')
  .select('*')
  .order('date', { ascending: false })
  .limit(1)
  .single();

// 보유 포지션
const positions = perf.positions || [];
// 당일 청산
const closedToday = perf.closed_trades || [];
```

### 수익률 추이 차트 (30일)
```javascript
const { data: history } = await supabase
  .from('intelligence_limit_up_performance')
  .select('date, cum_return, total_value, win_rate, mdd, active_count')
  .order('date', { ascending: true })
  .gte('date', thirtyDaysAgo);

// cum_return으로 라인 차트
// total_value로 자금 추이 차트
```

### 전략별 성적 비교
```javascript
const { data } = await supabase
  .from('intelligence_limit_up_performance')
  .select('date, strategy1_trades, strategy1_wins, strategy2_trades, strategy2_wins')
  .order('date', { ascending: false })
  .limit(1)
  .single();

const s1WinRate = data.strategy1_trades > 0
  ? (data.strategy1_wins / data.strategy1_trades * 100).toFixed(1)
  : '---';
```

---

## 스케줄

| 시각 | 스텝 | 설명 |
|------|------|------|
| 16:10~ | step5b | 상한가/급등 스캔 → candidates.json |
| 16:15~ | step5c | 눌림목 엔진 → signals.json + watchlist.json |
| 16:18~ | step5d | 페이퍼 트레이딩 → PaperPortfolio 가상매수/매도 |
| 16:20~ | step5e | FLOWX 업로드 → Supabase 2개 테이블 |

모두 `collect_all.py` 파이프라인 내 순차 실행.
주말/공휴일엔 `is_trading_day()` 체크로 자동 스킵.

---

## 백테스트 근거 (참고)

### 2층 전략 구조

| 전략 | 조건 | 백테스트 결과 (6개월) |
|------|------|---------------------|
| 전략1 (분할매수) | 3회차+ 상한가 · 원점+300% 미만 · 10일 이내 · 수급확인 · 분할매수 3단계 | 100% 승률 · 37건 · MDD -20% |
| 전략2 (눌림목) | 10%+ 급등(품질유니버스) → 5일 감시 → -10% 눌림+수급확인 → 분할매수 | 92% 승률 · 눌림목 진입 |

### 확정 파라미터
- TP: +10% (확정)
- SL: 없음 (만기 청산)
- 만기: 20영업일
- 과열 필터: 원점 대비 +300% 미만
- 최소 주가: 1,000원 이상
- 급등 기준: 10%+ (품질유니버스 필터와 결합)
- 감시 기간: 5영업일 (10%급등→연속급등→눌림 패턴 대응)

### 품질 유니버스 (175종목)

전략2 종목 선별 필터. 3개 레이어 통합:

| Layer | 소스 | 종목수 | 내용 |
|-------|------|--------|------|
| 1 | EWY 바스켓 | 80 | iShares MSCI South Korea ETF 실제 보유 종목 |
| 2 | FLOWX 섹터유니버스 | ~95 | 13섹터 핵심 KR 종목 (tier 1-2) |
| 3 | 추가 섹터 보완 | ~20 | 2차전지, 전력장비, 화장품, 통신, 사이버보안 등 |

대상 섹터: 반도체, 조선, 방산, 건설, 바이오, 금융, 자동차, 로봇,
에너지(원전/신재생), 게임, 엔터, 해운/항공, 식품, 2차전지,
전력장비/변압기, 철강/금속, 석유/화학, 화장품/뷰티, 통신/IT, 사이버보안, 우주항공

**적용 방식**:

- 전략1 (3번째+ 상한가): 유니버스 필터 미적용 (워낙 희귀, 100% 승률) + 수급확인 + 분할매수
- 전략2 (10%+ 급등 눌림목): 175종목 내에서만 허용 + 눌림+수급확인 → 분할매수

---

## 엣지 케이스

1. **시그널 0건**: `triggered_count = 0` → "오늘은 시그널이 없습니다" 표시, 감시풀만 표시
2. **감시풀 만 있음**: `triggered_count = 0, watchlist_count > 0` → 감시풀 자동 펼침
3. **포지션 0건 + 청산 0건**: 성적표 요약 카드만 표시 (보유/청산 섹션 숨김)
4. **장 휴장일**: 해당 날짜 row 없음 → "장 휴장" 표시, 마지막 거래일 데이터 유지
5. **Paper → 실매매 전환 시**: 뱃지를 `Paper Trading` → `Live Trading` 변경, 푸터 문구 변경
6. **MDD가 -20% 초과**: 경고 배너 표시 — "전략 재검토 필요"

---

## 데이터 로딩 (Wave 1 / Wave 2)

> 3-Tier 분산 배치에 따라 상한가 엔진 데이터는 **두 웨이브**에 나뉘어 로드됩니다.
> 전체 Wave 로딩은 `FLOWX_SWING_UNIFIED_SPEC.md` "데이터 로딩 전략" 섹션 참고.

### Wave 1 — Tier 1 + 2 즉시 로드 (페이지 초기)

상한가 시그널은 Tier 1-B [상한가] 탭에 필요하므로 Wave 1에 포함:

```javascript
// Wave 1 중 상한가 관련 쿼리 (다른 테이블과 Promise.all로 병렬)
const { data: signals } = await supabase
  .from('intelligence_limit_up_signals')
  .select('*')
  .order('date', { ascending: false })
  .limit(1)
  .single();

// triggered 배열 → 1-B [상한가] 탭 시그널 카드
const triggered = signals.triggered || [];
// watchlist 배열 → 1-B [상한가] 탭 감시풀 (접힘)
const watchlist = signals.watchlist || [];
```

### Wave 2 — Tier 3 lazy load (사용자가 펼칠 때)

Paper Trading 성적표는 Tier 3-A [상한가Paper] 탭에 배치, 사용자가 펼칠 때 로드:

```javascript
// Wave 2 — 3-A 성적표 탭 펼칠 때 로드
const { data: perf } = await supabase
  .from('intelligence_limit_up_performance')
  .select('*')
  .order('date', { ascending: false })
  .limit(1)
  .single();

// 보유 포지션
const positions = perf.positions || [];
// 당일 청산
const closedToday = perf.closed_trades || [];
```

> **참고**: `quant_market_brain`(2-C)과 `intelligence_inst_accumulation`(3-C)은
> 각각의 Tier에서 별도 로드됩니다. 상한가 엔진 지시서에서는 다루지 않습니다.

---

## 변경 이력

- **2026-05-10** — v3.1 3-Tier 분산 배치 반영
  - **배치 위치**: 독립 3섹션 패널 → Tier 1-B/2-C/3-A 분산 배치로 변경
  - triggered 시그널 → 1-B [상한가] 탭, Paper Trading → 3-A [상한가Paper] 탭
  - **쿼리**: "3섹션 통합 쿼리" → Wave 1/2 분리 로딩으로 변경
  - UI 디자인 가이드·데이터 구조·백테스트 근거 등 나머지는 변경 없음
  - `FLOWX_SWING_UNIFIED_SPEC.md` v2 3-Tier 구조에 정합

- **2026-05-10** — v3 수급 분석 + 분할매수
  - **수급 분석**: 외국인/기관 5일 순매수(백만원) + 소진율 → 매집중/이탈중/중립 등급
  - **분할매수 3단계**: 1차 50% / 2차 30% / 3차 20% — 구체적 가격 제시
  - 전략1(상한가): "즉시진입" → "분할매수" 뱃지 변경, 수급+분할 정보 포함
  - 전략2(눌림목): 눌림목 트리거 시 수급 재분석 + 분할매수 계획 자동 생성
  - JSONB 신규 필드: `flow_foreign_5d`, `flow_inst_5d`, `flow_grade`, `foreign_exh_rate`, `split_plan`
  - 감시풀에도 수급등급 표시 (flow_grade)
  - 텔레그램 알림: 수급 + 분할매수 계획 포함
  - UI 카드: 수급 섹션(색상별) + 분할매수 테이블 추가

- **2026-05-08** — v2 품질유니버스 필터
  - 급등 기준: 15% → **10%** (품질유니버스 필터와 결합하여 잡주 제외)
  - 감시 기간: 3영업일 → **5영업일** (10%급등→연속급등→눌림 패턴 대응)
  - `quality_universe.py` 신규: EWY 80 + 섹터유니버스 95 + 추가섹터 20 = **175종목**
  - 전략2 종목 선별: 2,274종목 무차별 스캔 → 175종목 품질유니버스 내에서만 허용
  - 기존 감시풀 잡주 자동 정리 (21건 → 3건)
  - 섹터 정보 reasons에 추가 (EWY바스켓 여부, 섹터 테마명)

- **2026-05-09** — v1 초안
  - 상한가 엔진 파이프라인: step5b~5e (collect_all.py)
  - Supabase: `intelligence_limit_up_signals` + `intelligence_limit_up_performance`
  - 백엔드 모듈: `limit_up_engine.py` (시그널) + `limit_up_paper_trader.py` (페이퍼) + `upload_limit_up.py` (업로드)
  - 3섹션 배치: BRAIN → 상한가 엔진 → 매집합류
