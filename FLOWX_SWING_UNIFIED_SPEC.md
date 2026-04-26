# FLOWX 스윙시스템 페이지 — 통합 구현 지시서

> **이 문서 하나로 스윙시스템 페이지 전체를 구현할 수 있습니다.**
> 기존 개별 SPEC 15개를 통합·정리한 최종본입니다.

## 프로젝트 소개
이 프로젝트는 **Body Hunter v4** — 한국 주식 단타봇(자동매매 시스템)입니다.
KIS API(한국투자증권) 실계좌 매매, 텔레그램 봇 UI, 7-Group COO 파이프라인,
BRAIN 시장분석, NXT 야간매매, FLOWX VIP 콘텐츠를 포함합니다.
CFO(재무)/CTO(기술)/COO(운영) 3-Officer 구조로 운영됩니다.

---

## 0. 기술 스택

| 항목 | 사용 기술 |
|------|----------|
| 프론트엔드 | Next.js (App Router) + TypeScript + Tailwind CSS |
| 배포 | Vercel |
| DB | Supabase (PostgreSQL) |
| 차트 | Recharts (기본) + D3.js (Treemap/고급) |
| 디자인 | 다크 테마 기본, 모바일 우선 |

### 차트 색상 토큰
```typescript
const CHART_COLORS = {
  up: '#E24B4A',       // 빨강 (한국 상승 컨벤션)
  down: '#378ADD',      // 파랑 (한국 하락 컨벤션)
  neutral: '#888780',
  green: '#22c55e',
  yellow: '#eab308',
  red: '#ef4444',
  orange: '#ea580c',
  bg: 'transparent',
  grid: 'rgba(136, 135, 128, 0.15)',
};
```

---

## 1. Supabase 테이블 일람

총 **9개 테이블** — 모두 이미 생성 완료, 매일 자동 갱신 중.

| # | 테이블 | PK | 최신 데이터 | 갱신 시각 | 용도 |
|---|--------|-----|-----------|----------|------|
| 1 | `dashboard_swing` | date | 2026-04-26 | 07:30 + 16:45 | **메인 대시보드** (48 컬럼) |
| 2 | `swing_signals` | date | 2026-04-26 | 07:30 | 상세 분석 (legacy, 선택) |
| 3 | `intelligence_daytrading_picks` | (date, mode) | 2026-04-26 | 16:45/07:35 | 단타 TOP픽 |
| 4 | `intelligence_daytrading_performance` | date | 2026-04-24 | 16:30 | 단타 성적표 |
| 5 | `intelligence_nxt_picks` | date | 2026-04-24 | 16:45 | NXT TOP5 추천 |
| 6 | `intelligence_nxt_performance` | pick_date | 2026-04-23 | 16:30 | NXT 성적표 |
| 7 | `intelligence_accumulation_radar` | date | 2026-04-26 | 16:35 | 매집 레이더 |
| 8 | `intelligence_cycle_scan` | date | 2026-04-26 | 16:30 | 수급 사이클 감지기 |
| 9 | `intelligence_stealth_scan` | date | 2026-04-26 | 16:40 | 기관 선매집 탐지 (별도 테이블) |

### RLS 정책 (전 테이블 공통)
- `anon` → SELECT만 가능 (읽기 전용)
- `service_role` → ALL (백엔드 쓰기용)
- 프론트엔드에서는 **anon key**로 읽기만 하면 됩니다.

---

## 2. 페이지 전체 레이아웃

```
┌─────────────────────────────────────────────────┐
│  💲 A. 달러-환율 모니터 (fx_monitor)             │ ← 최상단
├─────────────────────────────────────────────────┤
│  🧠 B. BRAIN 판정 + 자산배분                     │
├─────────────────────────────────────────────────┤
│  📊 C. 글로벌 지표 + 5대 분석                    │
├─────────────────────────────────────────────────┤
│  📈 D. 스윙 추천 종목 + ETF 추천                 │
├─────────────────────────────────────────────────┤
│  🔎 E. 야간 매매 판단 근거 (nxt_rationale)       │
├─────────────────────────────────────────────────┤
│  🌙 F. NXT 야간매매 + NXT 성적표                 │
├─────────────────────────────────────────────────┤
│  🎯 G. 단타 TOP픽 + 단타 성적표                  │
├─────────────────────────────────────────────────┤
│  📐 H. 3탭 퀀트 패널                             │
│  [전체 피보나치] [대형주 피보나치] [섹터 로테이션]  │
├─────────────────────────────────────────────────┤
│  🔍 I. 수급 사이클 감지기                        │
├─────────────────────────────────────────────────┤
│  🕵️ J. 기관 선매집 탐지                          │
├─────────────────────────────────────────────────┤
│  🔍 K. 매집 레이더                               │
└─────────────────────────────────────────────────┘
```

---

## 3. 각 패널 상세

---

### A. 달러-환율 모니터 (`dashboard_swing.fx_monitor`)

**위치**: 페이지 최상단
**데이터**: `dashboard_swing.fx_monitor` (JSONB)

#### 데이터 구조
```typescript
interface FxMonitor {
  timestamp: string;       // "2026-04-05 17:53"
  dxy: {
    value: number;         // 100.03
    prev: number;          // 99.65
    chg_1d: number;        // +0.38 (%)
    ma5: number;           // 5일 이평선
    ma20: number;          // 20일 이평선
    trend: string;         // "약세" | "강세" | "횡보"
  };
  usdkrw: {
    value: number;         // 1510.5
    prev: number;
    chg_1d: number;
    ma5: number;
    ma20: number;
    trend: string;         // "원강세" | "원약세" | "횡보"
  };
  vix_structure: {
    vix: number;           // 23.87
    vix3m: number;         // 24.72
    ratio: number;         // 0.966
    structure: string;     // "CONTANGO" | "BACKWARDATION"
    label: string;         // "정상(안전)" | "역전(패닉)"
  };
  correlation: {
    matches: number;       // 12
    total: number;         // 14
    pct: number;           // 86
    label: string;         // "최근 14일 중 12일 역상관 (86%)"
  };
  foreign_flow: {
    proxy: string;         // "삼성전자"
    today_억: number;      // 966
    sum_3d_억: number;     // -1720
    streak: number;        // 1
    direction: string;     // "매수" | "매도"
    signal: string;        // "순매수전환"
    signal_color: string;  // "GREEN" | "YELLOW" | "RED"
  };
  verdict: {
    text: string;          // "외국인 유입 가능"
    color: string;         // "GREEN" | "YELLOW" | "RED"
    bullish: number;
    bearish: number;
    score: number;
  };
}
```

#### UI: 가로 4칸 카드
```
┌──────────────┐ ┌──────────────┐ ┌──────────────┐ ┌──────────────┐
│  DXY 달러     │ │  USD/KRW     │ │  VIX 구조     │ │  외국인 흐름   │
│  100.03      │ │  1,510.5원   │ │  23.87       │ │  🟢순매수전환  │
│  ▼약세       │ │  ▼원강세     │ │  CONTANGO    │ │  +966억      │
│  5일: 100.1  │ │  5일: 1,510  │ │  VIX3M: 24.7 │ │  3일: -1,720 │
└──────────────┘ └──────────────┘ └──────────────┘ └──────────────┘

📊 환율↔KOSPI 상관: 86% (최근 14일 중 12일 역상관)
[████████████████████░░░░] 86%
```

#### 추세 화살표 규칙
| 필드 | 값 | 화살표 | 색상 | 의미 |
|------|-----|--------|------|------|
| dxy.trend | "약세" | ▼ | 초록 | 달러 약세 = 긍정 |
| dxy.trend | "강세" | ▲ | 빨강 | 달러 강세 = 부정 |
| usdkrw.trend | "원강세" | ▼ | 초록 | 환율 하락 = 긍정 |
| usdkrw.trend | "원약세" | ▲ | 빨강 | 환율 상승 = 부정 |

#### verdict 배지
| color | 텍스트 예 | 배경색 |
|-------|----------|--------|
| GREEN | "외국인 유입 강력" | #22c55e |
| YELLOW | "중립 (관망)" | #eab308 |
| RED | "외국인 유출 경고" | #ef4444 |

#### 빈 데이터
`fx_monitor`가 null이거나 `dxy` 필드 없음 → "데이터 수집 중입니다. 16:45 이후 갱신됩니다."

---

### B. BRAIN 판정 + 자산배분 (`dashboard_swing`)

#### 헤더 카드
```
┌──────────────────────────────────────┐
│  [공격/표준/방어/관망]  BRAIN 72점     │
│  "표준모드 — AA등급 이상 3종목 추천"    │
│  2026-04-26 07:30 갱신                │
└──────────────────────────────────────┘
```

**필드**: `brain_verdict` + `brain_pct` + `brain_reason` + `market_comment`

**추가 필드**:
- `brain_raw_pct` — BRAIN 원본 점수 (체제 캡 적용 전)
- `brain_capped_pct` — 체제 캡 적용 후 점수 (= `brain_pct`와 동일)
- `regime_cap_reason` — 캡 사유 (예: `"비용상승→50%캡"`, 빈 문자열이면 캡 없음)
- `regime_desc` — 체제 설명 텍스트

**캡 표시**: `regime_cap_reason`이 비어있지 않으면 → `"BRAIN 72점 (원본 100점, 비용상승→50%캡)"` 형태로 표시

| brain_verdict | 배경색 | 의미 |
|---------------|--------|------|
| 공격 | 녹색 | 적극 매수 |
| 표준 | 파랑 | 일반 매수 |
| 방어 | 노랑 | 축소 운영 |
| 관망 | 빨강/회색 | 현금 대기 |

#### 자산배분 파이차트
```
alloc_swing(스윙) + alloc_gold_etf(금ETF) + alloc_inverse(인버스)
+ alloc_group_etf(그룹ETF) + alloc_small_cap(소형주) + alloc_cash(현금) = 100%
```

**추가 필드**: `regime` (시장 체제), `regime_severity` (체제 강도)

#### 모델 포트폴리오 (`portfolio` JSONB)
```typescript
interface Portfolio {
  current_picks: number;   // 현재 추천 종목 수
  brain_pct: number;       // BRAIN 점수
  brain_cash_ratio: number;// 현금 비율
  total_trades: number;    // 총 거래 수
  win_rate: number;        // 승률 %
}
```
- `total_trades > 0`이면 "승률 {win_rate}% ({total_trades}건)" 표시

#### 센서 필드 (향후 활성화)
아래 필드는 현재 값이 0 또는 빈 문자열이지만, 향후 활성화될 예정:
- `smart_money_score` / `smart_money_signal` — 스마트머니 센서
- `stress_index` / `stress_level` — 스트레스 센서
- `rotation_signal` — 로테이션 신호
- `liquidity_score` — 유동성 점수

→ 값이 0이 아닐 때만 UI에 표시하면 됩니다.

---

### C. 글로벌 지표 + 5대 분석 (`dashboard_swing`)

#### 시장지표 6개
| 필드 | 표시명 | 포맷 |
|------|--------|------|
| vix | VIX | 18.92 |
| nasdaq_pct | 나스닥 | -0.31% |
| usdkrw | 환율 | 1,480.7 |
| oil_pct | 유가 | +2.2% |
| gold_pct | 금 | -0.1% |
| silver_pct | 은 | -2.4% |

#### 5대 분석 (`analysis` JSONB)
```json
{
  "flow_summary": "외국인 순매수 3일차, 기관 소폭 매도",
  "risk_summary": "VIX 27.6 (EXTREME), 원/달러 1507",
  "macro_summary": "미국 금리 동결 기조, 한국 수출 호조",
  "sector_summary": "반도체 HOT, 2차전지 WARMING, 바이오 COLD",
  "commodity_summary": "금 +1.7%, 유가 +0.5%, 은 +3.2%"
}
```

---

### D. 스윙 추천 종목 (`dashboard_swing.picks` + `etf_picks` + `watchlist`)

#### picks (스윙 종목) JSONB 배열
```typescript
interface SwingPick {
  code: string;          // "005930"
  name: string;          // "삼성전자"
  grade: string;         // "AAA" | "AA" | "A" | "BBB" | ...
  score: number;         // 87.5
  sector: string;
  rr_ratio: number;      // 2.3
  rr_verdict: string;    // "EXCELLENT" | "GOOD" | "FAIR" | "POOR"
  entry_price: number;
  target_price: number;
  stop_price: number;
  hold_days: number;
  conviction: string;    // "HIGH" | "MEDIUM" | "LOW"
  catalyst: string;
  regime: string;
  tv_pattern: string;
  news_sentiment: string;
  tech_score: number;
  supply_signal: string; // "BUY" | "SELL" | "NEUTRAL"
  nat_power_grade: string;
}
```

| conviction | 색상 |
|------------|------|
| HIGH | 녹색 |
| MEDIUM | 파랑 |
| LOW | 회색 |

#### etf_picks (ETF 추천) JSONB 배열
```json
[{"code":"069500","name":"KODEX 200","category":"시장대표","signal":"BUY",
  "entry":35000,"sl":33500,"tp":37000,"reason":"저평가+수급유입","holding_days":5}]
```

#### watchlist (반등감시 — 관망모드 시)
```json
[{"code":"004250","name":"NPC","grade":"BB","score":53.6,
  "reason":"반등 감시 — 모멘텀 레짐","trigger":"종가 4,015원 돌파 시"}]
```
- `brain_verdict === "관망"`일 때만 이 섹션 표시

---

### E. 야간 매매 판단 근거 (`dashboard_swing.nxt_rationale`)

**위치**: NXT 추천 종목 바로 위

#### 데이터 구조
```typescript
interface NxtRationale {
  timestamp: string;          // "2026-04-04 23:13:10"
  verdict: string;            // "적극 매수" | "조건부 매수" | "경계" | "회피"
  green: number;              // 안전 신호 수 (0~7)
  yellow: number;
  red: number;
  total: number;              // 7
  indicators: Indicator[];    // 7개 지표 상세
}

interface Indicator {
  key: string;         // "move" | "vix_term" | "cu_au" | "jpy_carry" | "vvix" | "credit_spread" | "btc"
  name: string;        // "MOVE 채권공포"
  signal: string;      // "GREEN" | "YELLOW" | "RED"
  signal_label: string;// "안전" | "경계" | "위험"
  detail: string;      // 수치 요약 (그대로 표시)
}
```

#### UI
```
┌─────────────────────────────────────────────┐
│  🔎 야간 매매 판단 근거                      │
│  종합: [적극 매수]                           │
│  🟢×5  🟡×1  🔴×1  (7개 지표 중)            │
│  기준: 2026-04-04 23:13                      │
├──────────────────┬────────┬──────────────────┤
│ MOVE 채권공포     │ 🟢안전 │ 81.78            │
│ VIX 기간구조      │ 🟢안전 │ VIX 23.87/VIX3M 24.72 │
│ 구리/금 비율      │ 🟢안전 │ 구리 -1.1% vs 금 -2.8% │
│ 엔 캐리트레이드   │ 🟢안전 │ JPY 159.63 (ON)  │
│ VVIX 스마트머니   │ 🟢안전 │ 115.33           │
│ 신용스프레드      │ 🔴위험 │ HYG +0.24%       │
│ BTC 야간심리      │ 🟡경계 │ $67,198 (+0.4%)  │
└──────────────────┴────────┴──────────────────┘
```

#### verdict 배지 색상
| verdict | 색상 |
|---------|------|
| 적극 매수 | #22c55e (초록) |
| 조건부 매수 | #3b82f6 (파랑) |
| 경계 | #eab308 (노랑) |
| 회피 | #ef4444 (빨강) |

#### 7개 지표 툴팁 설명
| 지표 | 설명 |
|------|------|
| MOVE 채권공포 | 미국 채권 변동성. 100↓ 안전, 120↑ 위험 |
| VIX 기간구조 | 단기 vs 장기. CONTANGO=정상, BACKWARDATION=패닉 |
| 구리/금 비율 | 구리 강세=경기 회복, 금 강세=불안 |
| 엔 캐리트레이드 | 엔 약세(ON)=위험자산 선호, 강세(OFF)=안전자산 선호 |
| VVIX 스마트머니 | VIX의 VIX. MA20 대비 높으면 기관 헤지 중 |
| 신용스프레드 | HYG(하이일드) 강세=리스크온, LQD 강세=리스크오프 |
| BTC 야간심리 | 비트코인 24시간 변동. 야간 투자심리 바로미터 |

---

### F. NXT 야간매매 (`dashboard_swing` + `intelligence_nxt_*`)

#### F-1. NXT 시그널 카드 (`dashboard_swing`)
```
🌙 NXT 야간매매  시그널: BUY (+7.5)
"VIX 20 + 한국(+2.5) → 매수 유망"
대상: [nxt_targets 리스트]
```
**필드**: `nxt_signal`, `nxt_score`(-10~+10), `nxt_signal_text`, `nxt_reason`, `nxt_targets`(JSONB)

#### F-2. NXT TOP5 추천 (`intelligence_nxt_picks`)
```sql
SELECT * FROM intelligence_nxt_picks ORDER BY date DESC LIMIT 1;
```
```typescript
interface NxtPick {
  rank: number;
  code: string;
  name: string;
  sector: string;
  supply_score: number;
  entry_price: number;
}
```

#### F-3. NXT 성적표 (`intelligence_nxt_performance`)
```sql
SELECT * FROM intelligence_nxt_performance ORDER BY pick_date DESC LIMIT 1;
```

| 컬럼 | 설명 |
|------|------|
| pick_date (PK) | 추천일 |
| result_date | 결과 확인일 |
| avg_return | 평균 수익률(%) |
| best_pick / worst_pick | 최고/최저 종목명 |
| weekly_return / monthly_return | 주간/월간 누적(%) |
| items (JSONB) | 종목별 결과 배열 |

**차트**: 최근 20거래일 바 차트 (양수=녹색, 음수=빨강) + 누적선 오버레이

---

### G. 단타 TOP픽 (`intelligence_daytrading_picks` + `_performance`)

#### G-1. 단타 TOP픽 — Hybrid 2단 발행

| 발행 시각 | mode | 데이터 기준 | 대상 |
|-----------|------|------------|------|
| 16:45 KST | `preview` | 국장 마감 수급만 | NXT 야간매수자 |
| 07:35 KST | `confirmed` | 미국장 + EWY 반영 | 정규장 09:00 진입자 |

**쿼리** (confirmed 우선):
```sql
SELECT * FROM intelligence_daytrading_picks
WHERE date = CURRENT_DATE
ORDER BY CASE mode WHEN 'confirmed' THEN 0 ELSE 1 END, updated_at DESC
LIMIT 1;
```

#### picks JSONB 구조
```typescript
interface DaytradingPick {
  rank: number;           // 1~7
  code: string;
  name: string;
  sector: string;
  track: string;          // "A_대형주" | "B_중소형주"
  mcap_억: number;
  close: number;
  entry_low: number;
  entry_high: number;
  tp1: number;
  tp2: number;
  sl: number;
  upside_to_tp1_pct: number;
  final_score: number;
  key_reasons: string;
  foreign_total_억: number;
  inst_total_억: number;
  buy_days: number;
  etf_alt_code: string;   // ETF 대안 코드
  etf_alt_name: string;   // ETF 대안 명칭
  etf_alt_theme: string;
}
```

#### UI
```
🎯 단타 TOP픽  [confirmed] · 2026-04-09 07:35
🌍 EWY +10.13% · KS200 -1.93% · 🔥 외국인 한국 폭발매수

┌────────────────────────────────────┐
│ 🥇 삼성전자 (005930) · 전기전자     │
│ 💰 78,500원 · 시총 350,000억        │
│ 🎯 진입 77,322~79,285               │
│ 🎯 목표 82,425 (+5.0%)              │
│ 📊 외국인 4/5일 매수 +1,200억        │
│ 🔗 KODEX 반도체 (091160) [반도체]    │
│ ⭐ 85.5점                           │
└────────────────────────────────────┘
```

- **mode 뱃지**: preview → `📢 프리뷰`, confirmed → `✅ 확정`
- **트랙 필터**: 전체 | 🔷 대형주 (Track A) | 🟢 중소형주 (Track B)
- **rank 1~3**: 🥇🥈🥉

#### G-2. 단타 성적표 (`intelligence_daytrading_performance`)
```sql
SELECT * FROM intelligence_daytrading_performance ORDER BY date DESC LIMIT 1;
```

| 컬럼 | 설명 |
|------|------|
| date (PK) | 거래일 |
| avg_return | 당일 평균 수익률(%) |
| best_pick / worst_pick | 최고/최저 종목 |
| weekly_return / monthly_return | 주간/월간 누적(%) |
| items (JSONB) | 종목별 상세 (rank, code, name, return_pct 등) |

**차트**: 최근 20거래일 바 차트 + 누적선

#### G-3. TODAY vs NXT 비교 테이블
- 왼쪽: TODAY 단타 TOP 5 (daytrading_performance)
- 오른쪽: NXT 야간매수 TOP 5 (nxt_performance)
- 동일 날짜 기준 수익률 비교

---

### H. 3탭 퀀트 패널 (`dashboard_swing`)

**기존 6탭(BRAIN판단/섹터수급/모멘텀/로테이션/ETF수급/ETF추천) 삭제** → 아래 3탭 교체.

```
┌─────────────────────────────────────────────┐
│ [전체 피보나치] [대형주 피보나치] [섹터 로테이션] │
├─────────────────────────────────────────────┤
│           선택된 탭의 콘텐츠                   │
└─────────────────────────────────────────────┘
```

#### H-1. 전체 피보나치 눌림목 (`fib_stocks`)

**데이터**: `dashboard_swing.fib_stocks` (JSONB 배열, 최대 50종목)

```typescript
interface FibStock {
  code: string;           // "000250"
  name: string;           // "삼천당제약"
  sector: string;         // "제약"
  cap: number;            // 260613 (시총, 억원)
  price: number;          // 609000 (현재가)
  w52h: number;           // 1233000 (52주 고점)
  w52l: number;           // 127600 (52주 저점)
  drop: number;           // -50.6 (52주 고점 대비 하락률 %)
  fib_zone: string;       // "DEEP" | "MID" | "MILD" | "SHALLOW"
  fib_zone_label: string; // "50%+ 하락 (바닥 매수 구간)"
  fib_382: number;        // 38.2% 되돌림 가격
  fib_500: number;        // 50% 되돌림 가격
  fib_618: number;        // 61.8% 되돌림 가격
  fib_status: string;     // "38.2% 아래 (깊은 하락)"
  target: number;         // 목표가
  upside: number;         // 상승여력 %
  per: number;
  pbr: number;
  frgn: number;           // 외국인 보유율 %
}
```

**fib_zone별 그룹핑** (서버에서 이미 DEEP→SHALLOW 순 정렬):

| fib_zone | 아이콘 | 배경색 | 텍스트 | 의미 |
|----------|--------|--------|--------|------|
| DEEP | 🔴 | #FEF2F2 | #DC2626 | 50%+ 하락, 바닥 매수 |
| MID | 🟠 | #FFF7ED | #EA580C | 40~50% 하락, 중간 눌림 |
| MILD | 🟡 | #FFFBEB | #CA8A04 | 30~40% 하락, 1차 눌림 |
| SHALLOW | 🟢 | #F7FEE7 | #65A30D | 15~30% 하락, 얕은 조정 |

**컬럼 포맷**:
| 필드 | 표시명 | 포맷 |
|------|--------|------|
| name | 종목 | 굵게 |
| sector | 섹터 | 텍스트 |
| drop | 하락률 | `-38.8%` (빨강) |
| fib_status | 피보나치 위치 | `38.2% 아래` |
| upside | 상승여력 | `+39.0%` (초록) |
| per | PER | `15.2` |
| frgn | 외국인 | `33.1%` |
| cap | 시총 | cap >= 10000 → `X.X조`, else → `X,XXX억` |

**정렬**: 기본 fib_zone별 그룹핑 + 하락률순 (서버정렬). 토글: 상승여력순/PER순/외국인순

**피보나치 게이지 (선택, 종목 클릭 시)**:
```
52주 저점 ━━━━ 38.2% ━━━━ 50% ━━━━ 61.8% ━━━━ 52주 고점
                            ▲ 현재 위치
```

#### H-2. 대형주 피보나치 (`fib_leaders`)

**데이터**: `dashboard_swing.fib_leaders` (JSONB 배열, 30종목)

```typescript
interface FibLeader {
  code: string;        // "005930"
  name: string;        // "삼성전자"
  sector: string;
  cap: number;         // 10560634 (억원)
  price: number;       // 206000
  w52h: number;        // 223000
  w52l: number;        // 52900
  w52h_date: string;   // "20260227"
  drop: number;        // -7.62
  fib_382: number;
  fib_500: number;
  fib_618: number;
  fib_status: string;  // "고점 근접" | "38.2% 아래 (깊은 하락)" | ...
  fib_zone?: string;   // 서버에서 drop 기반 자동 계산 (NEAR_HIGH 추가)
  per: number;
  pbr: number;
}
```

**핵심 차이점**: 시총 TOP 30 **무조건 표시** (하락률 무관, 삼성전자도 포함)

**테이블** (그룹핑 없이 시총 순):
```
| # | 종목     | 섹터     | 시총     | 현재가     | 하락률  | 피보나치 위치     | PER  |
|---|---------|---------|---------|----------|--------|----------------|------|
| 1 | 삼성전자  | 전기전자 | 105.6조 | 206,000  | -7.6%  | 고점 근접        | 40.4 |
```

**fib_status 색상**:
| fib_status | 색상 |
|------------|------|
| 고점 근접 | 🟢 초록 |
| 61.8% 위 (회복 중) | 🟢 연초록 |
| 50%~61.8% 사이 | 🟡 노랑 |
| 38.2%~50% 사이 | 🟠 주황 |
| 38.2% 아래 (깊은 하락) | 🔴 빨강 |

**fib_zone 없으면 프론트에서 자동 계산**:
```typescript
function getFibZone(drop: number): string {
  const d = Math.abs(drop);
  if (d >= 50) return "DEEP";
  if (d >= 40) return "MID";
  if (d >= 30) return "MILD";
  if (d >= 15) return "SHALLOW";
  return "NEAR_HIGH";
}
```

#### H-3. 섹터 로테이션 맵 (`sector_rotation`)

**데이터**: `dashboard_swing.sector_rotation` (JSONB)

```typescript
interface SectorRotation {
  timestamp: string;       // "2026-04-10 16:40"
  total_sectors: number;
  total_stocks: number;
  sectors: Sector[];       // 점수순 정렬
}

interface Sector {
  sector: string;          // "2차전지"
  count: number;           // 종목 수
  total_score: number;     // 종합 점수
  momentum: number;        // 모멘텀 점수
  flow_score: number;      // 수급 점수 (±30 상한)
  dual_bonus: number;      // 쌍매수 보너스
  avg_chg: number;         // 평균 등락률 %
  avg_drop: number;        // 평균 하락률 %
  avg_upside: number;      // 평균 상승여력 %
  net_flow_억: number;     // 순수급 (억원)
  dual_buy_3d: number;     // 3일 쌍매수 종목 수
  up_count: number;        // 상승 종목 수
  down_count: number;
  deep: number;            // 각 fib_zone별 종목 수
  mid: number;
  mild: number;
  shallow: number;
  cap_조: number;          // 섹터 시총 (조원)
  cap_億: number;          // 섹터 시총 (억원, 원본값)
  warning: string;         // "" 또는 "개인 주도 상승 (수급 미확인)"
  stage: string;           // "선도" | "추격" | "대기" | "후발"
  stage_num: number;       // 1~4
  stage_color: string;     // "GREEN" | "YELLOW" | "RED"
}
```

**stage별 그룹핑**:
| stage | 조건 | 색상 | 배경 |
|-------|------|------|------|
| 선도 | total_score 50+ | #16A34A | #F0FDF4 |
| 추격 | 20~49 | #22C55E | #F0FDF4 |
| 대기 | 0~19 | #EAB308 | #FEFCE8 |
| 후발 | 0 미만 | #DC2626 | #FEF2F2 |

**섹터 클릭 시 상세** (선택):
```
2차전지 (15종목 · 18.5조)
  평균 하락: -38.2%  |  상승여력: +55.1%
  피보나치 분포: 🔴 DEEP 4 │ 🟠 MID 3 │ 🟡 MILD 5 │ 🟢 SHALLOW 3
  점수: 모멘텀 +25.0 | 수급 +17.3 | 쌍매수 +30.0 = 72.3
```

---

### I. 수급 사이클 감지기 (`intelligence_cycle_scan`)

```sql
SELECT * FROM intelligence_cycle_scan ORDER BY date DESC LIMIT 1;
```

#### 6가지 사이클 위상
| 위상 | 한국어 | 아이콘 | 색상 | 의미 |
|------|--------|--------|------|------|
| SURGE | 급등임박 | 🔥 | 빨강/금색 | 매수 검토 대상 |
| ACCUMULATION | 매집 | 📦 | 주황 | 관심 종목 등록 |
| REVERSAL | 전환 | 🔄 | 초록 | 바닥 전환 징후 |
| NEUTRAL | 중립 | ⚪ | 회색 | 특별 신호 없음 |
| DISTRIBUTION | 물량분배 | 📤 | 보라 | 세력 → 개인 떠넘기기 |
| PEAK_WARN | 고점경고 | ⚠️ | 검정/빨강 | 하락 전조 |

#### 데이터 구조
```typescript
interface CycleScan {
  date: string;
  total_scanned: number;
  surge_count: number;
  accumulate_count: number;
  reversal_count: number;
  neutral_count: number;
  distribute_count: number;
  peak_warn_count: number;
  surge_items: CycleStock[];       // 급등임박
  accumulate_items: CycleStock[];  // 매집
  reversal_items: CycleStock[];    // 전환
  warning_items: CycleStock[];     // 고점경고
  top_surge_names: string[];       // 급등임박 종목명 TOP (요약용)
  phase_summary: Record<string, number>;
}

interface CycleStock {
  code: string;
  name: string;
  phase: string;       // "SURGE" 등
  phase_kr: string;    // "급등임박"
  score: number;       // -100 ~ +100
  latest_close: number;
  change_pct: number;
  cap_억: number;
  market: string;      // "KOSPI" | "KOSDAQ"
  summary: string;     // 한줄 요약
  surge_type?: string; // "지속" | "원샷" (SURGE만)
  signals: Signal[];
}

interface Signal {
  name: string;        // "twin_buy"
  name_kr: string;     // "쌍매수"
  score: number;
  detail: string;
  days: number;
}
```

#### 8가지 감지 신호
| name | 한국어 | 아이콘 | 점수 | 색상 |
|------|--------|--------|------|------|
| twin_buy | 쌍매수 | ⚡ | +25 | 금색 |
| twin_sell | 쌍매도 | 🔻 | -20 | 빨강 |
| retail_sacrifice | 개인바침 | 🎯 | +20 | 보라 |
| stealth_acc | 기타매집 | 🕵️ | +15 | 남색 |
| stealth_exit | 기타이탈 | ⛔ | -20 | 검정 |
| force_reversal | 세력전환 | 🔄 | +20 | 초록 |
| retail_trap | 개인함정 | 🪤 | -15 | 회색 |
| triple_buy | 3세력매수 | 👑 | +10 | 금색 |

#### UI: 4개 섹션
```
🔍 수급 사이클 감지기  |  총 40종목 스캔
🔥 급등임박 5  |  📦 매집 12  |  🔄 전환 3  |  ⚠️ 경고 5

── 🔥 급등임박 (surge_items) ──
┌──────────────────────────────────────────────────┐
│ 🔥 [지속] 현대차  [KOSPI]  +100점  508,000원      │
│ ⚡ 쌍매수 (+25) — 외인+기관 3일 중 2일 동시 매수     │
│ 🕵️ 기타매집 (+30) — 기타법인 7일 연속 +926억        │
└──────────────────────────────────────────────────┘

── ⚠️ 경고 (warning_items) ──
┌──────────────────────────────────────────────────┐
│ ⚠️ 예시종목  [KOSDAQ]  -45점  50,000원  +8.5%     │
│ ⛔ 기타이탈 (-20) — 기타법인 4일 연속 매도 -180억    │
│ 🪤 개인함정 (-15) — 개인만 매수, 3세력 매도          │
└──────────────────────────────────────────────────┘
```

**surge_type 뱃지**: [지속]=초록(추가 상승 가능), [원샷⚡]=빨강(추격 위험)

#### 점수별 색상
| 점수 | 스타일 |
|------|--------|
| +80↑ | 금색 테두리 + 빨강 |
| +50~79 | 주황 |
| +20~49 | 노랑 |
| -19~+19 | 회색 |
| -50↓ | 빨강 테두리 + 검정 |

---

### J. 기관 선매집 탐지 (`dashboard_swing.stealth_stocks`)

```typescript
interface StealthData {
  timestamp: string;
  stealth: StealthStock[];  // 잠복 종목 (가격 ±5%, 아직 안 움직임)
  moving: StealthStock[];   // 움직임 종목 (이미 5~10% 상승)
  summary: {
    total_detected: number;
    stealth_count: number;
    moving_count: number;
    surged_count: number;      // 이미 상승한 종목 수
    top_stealth: string[];
  };
}

interface StealthStock {
  code: string;
  name: string;
  sector: string;
  score: number;       // 0~140
  pattern: string;     // "쌍매수2D" | "기관5D" | "외인4D"
  dual_buy: boolean;   // 기관+외인 동시
  inst_avg: number;    // 기관 일평균 순매수 (백만원) → /100 = 억
  frgn_avg: number;    // 외인 일평균 순매수 (백만원) → /100 = 억
  chg_5d: number;      // 5일 등락률 %
  close: number;
  cap: number;         // 시총 (억원)
  category: string;    // "잠복" | "움직임"
}
```

#### UI: 2탭
- **탭 1: 잠복 🔍** — 배경 연보라/남색 (비밀스러운 느낌)
- **탭 2: 움직임 🚀** — 배경 연파란

```
기관 선매집 탐지  |  잠복 50종목  |  움직임 12종목

┌─────────────────────────────────────────┐
│ ⚡ LG에너지솔루션  [전기전자]   125점    │
│ 쌍매수2D  │  I+361억  F+191억           │
│ 현재가 408,500원  │  5일 +0.4%          │
│ 시총 95.6조                              │
└─────────────────────────────────────────┘
```

#### 패턴 뱃지 색상
| 패턴 | 색상 | 의미 |
|------|------|------|
| 쌍매수 | 보라/금색 | 기관+외인 동시 (가장 강력) |
| 기관 | 빨강 | 기관만 매수 |
| 외인 | 파랑 | 외인만 매수 |

- `dual_buy = true` → ⚡ 아이콘 + 금색 테두리
- `inst_avg / 100` = 억원 → "I+361억"

#### J 대안 데이터: `intelligence_stealth_scan` (별도 테이블)

`dashboard_swing.stealth_stocks`가 비어있을 경우, 이 테이블에서 직접 조회 가능:

```sql
SELECT * FROM intelligence_stealth_scan ORDER BY date DESC LIMIT 1;
```

| 컬럼 | 타입 | 설명 |
|------|------|------|
| date (PK) | DATE | 날짜 |
| total_scanned | INT | 총 스캔 종목 수 |
| stealth_count | INT | 잠복 종목 수 |
| moving_count | INT | 움직임 종목 수 |
| surged_count | INT | 이미 상승 종목 수 |
| stealth_items | JSONB | 잠복 종목 상세 (최대 20개) |
| moving_items | JSONB | 움직임 종목 상세 (최대 10개) |
| surged_items | JSONB | 이미 상승 종목 상세 (최대 5개) |
| top_stealth_names | JSONB | 잠복 TOP 종목명 배열 |

`stealth_items` 각 항목은 `StealthStock` 인터페이스와 동일 + `inst_consec`(기관 연속일), `frgn_consec`(외인 연속일) 추가.

---

### K. 매집 레이더 (`intelligence_accumulation_radar`)

```sql
SELECT * FROM intelligence_accumulation_radar ORDER BY date DESC LIMIT 1;
```

```typescript
interface AccStock {
  code: string;
  name: string;
  frgn_days: number;       // 외인 연속 순매수 일수
  accel_b: number;         // 최근 2일 평균 외인 순매수(억)
  chg5: number;            // 5일 등락률 %
  tag: string;             // "쌍매수" | "가속전환" | "바닥매집" | "외인매집"
  last_dual: boolean;
  supply_score: number;    // 정렬 기준
  combined_supply: number;
}
```

#### UI: 리스트형 (카드 X)
```
🔍 매집 레이더 — 외인 조용한 매집 감지

┌──────────────────────────────────────────────┐
│ [쌍매수]  세아제강지주                          │
│ 외인 4일 매집 · +16억/일 가속 · 5일 -8.0%      │
└──────────────────────────────────────────────┘
```

#### 태그 뱃지 색상
| tag | 색상 | 의미 |
|-----|------|------|
| 쌍매수 | 빨강/핫핑크 | 외인+기관 동시 (최강) |
| 가속전환 | 주황 | 매수 가속 중 |
| 바닥매집 | 파랑 | 조용히 매집 |
| 외인매집 | 하늘색 | 외인 단독 |

- `chg5` 마이너스 → "미발화" 강조 (초록)
- `chg5` +5% 이상 → "이미 출발" (회색 처리)
- `last_dual = true` → 🔥 아이콘

---

## 4. 공통 규칙

### 4-1. 시총 표시 변환
```typescript
function formatCap(cap_억: number): string {
  if (cap_억 >= 10000) return `${(cap_억 / 10000).toFixed(1)}조`;
  return `${cap_억.toLocaleString()}억`;
}
```

### 4-2. 빈 데이터 처리
모든 패널에서 해당 필드가 `null`, `[]`, `{}` 또는 빈 배열이면:
```
[패널 제목]
데이터가 없습니다. 다음 갱신 시 업데이트됩니다.
```

### 4-3. 반응형
- **데스크톱**: 기본 레이아웃
- **모바일**: 가로 카드 → 세로 스택, 4칸 카드 → 2x2 그리드
- 텔레그램 웹앱 내에서도 표시 가능해야 함

### 4-4. 날짜 표시
- 모든 timestamp는 **한국 시간(KST)** 기준
- `dashboard_swing`은 `generated_at` (UTC) → KST 변환

### 4-5. 색상 규칙 요약
| 의미 | 색상 코드 |
|------|----------|
| 상승/양수/긍정/안전 (GREEN) | #22c55e |
| 경계/주의 (YELLOW) | #eab308 |
| 하락/음수/위험 (RED) | #ef4444 |
| 주황 | #ea580c |
| 파랑 | #3b82f6 |
| 회색/중립 | #9ca3af |

### 4-6. 피보나치 레벨 설명 (툴팁)
| 레벨 | 의미 |
|------|------|
| 38.2% | 가장 약한 되돌림. 여기서 반등 = 강한 추세 |
| 50% | 중간 되돌림. 가장 많이 사용되는 지지선 |
| 61.8% | 황금비율. 여기를 깨면 추세 전환 가능 |
| 현재가 < 38.2% | 깊은 눌림, 바닥 매수 후보 |
| 현재가 > 61.8% | 상승 추세 복귀 중 |
| 고점 근접 | 52주 고점의 90% 이상 |

---

## 5. Supabase API 쿼리 모음

### 메인 대시보드 (오늘)
```javascript
const { data } = await supabase
  .from('dashboard_swing')
  .select('*')
  .order('date', { ascending: false })
  .limit(1)
  .single();
```

### 히스토리 (최근 7일)
```javascript
const { data } = await supabase
  .from('dashboard_swing')
  .select('date, brain_pct, brain_verdict, alloc_swing, alloc_cash, vix')
  .order('date', { ascending: false })
  .limit(7);
```

### 단타 TOP픽 (confirmed 우선)
```javascript
const { data } = await supabase
  .from('intelligence_daytrading_picks')
  .select('*')
  .eq('date', today)
  .order('mode', { ascending: true }) // confirmed < preview
  .limit(1)
  .single();
```

### 수급 사이클 (최신)
```javascript
const { data } = await supabase
  .from('intelligence_cycle_scan')
  .select('*')
  .order('date', { ascending: false })
  .limit(1)
  .single();
```

### 매집 레이더 (최신)
```javascript
const { data } = await supabase
  .from('intelligence_accumulation_radar')
  .select('date, stocks')
  .order('date', { ascending: false })
  .limit(1)
  .single();
```

### 선매집 탐지 (최신, J 패널 대안)
```javascript
const { data } = await supabase
  .from('intelligence_stealth_scan')
  .select('*')
  .order('date', { ascending: false })
  .limit(1)
  .single();
```

### 성적표 차트 (최근 20일)
```javascript
const { data: dayPerf } = await supabase
  .from('intelligence_daytrading_performance')
  .select('date, avg_return')
  .order('date', { ascending: false })
  .limit(20);

const { data: nxtPerf } = await supabase
  .from('intelligence_nxt_performance')
  .select('pick_date, avg_return')
  .order('pick_date', { ascending: false })
  .limit(20);
```

---

## 6. 데이터 갱신 스케줄

| 시각 (KST) | 테이블 | 내용 |
|------------|--------|------|
| 07:30~07:50 | dashboard_swing | 메인 대시보드 (BRAIN + 추천 + 글로벌지표) |
| 07:35~07:50 | intelligence_daytrading_picks (confirmed) | 단타 TOP픽 확정본 |
| 16:30~16:50 | dashboard_swing 갱신 | fib_stocks, fib_leaders, sector_rotation, fx_monitor, nxt_rationale, stealth_stocks 추가 |
| 16:30~16:50 | intelligence_cycle_scan | 수급 사이클 감지기 |
| 16:30~16:50 | intelligence_accumulation_radar | 매집 레이더 |
| 16:30~16:50 | intelligence_daytrading_performance | 단타 성적표 |
| 16:45~17:00 | intelligence_daytrading_picks (preview) | 단타 TOP픽 프리뷰 |
| 16:45~17:00 | intelligence_nxt_picks | NXT TOP5 추천 |
| 16:45~17:00 | intelligence_nxt_performance | NXT 성적표 |

---

## 7. 체크리스트

### 필수 (P0)
- [ ] Supabase 연결 (anon key 읽기)
- [ ] 메인 대시보드 쿼리 (dashboard_swing 최신 1건)
- [ ] A. 달러-환율 모니터 4칸 카드
- [ ] B. BRAIN 판정 헤더 + 자산배분 파이차트
- [ ] C. 글로벌 지표 6개 + 5대 분석
- [ ] D. 스윙 추천 종목 테이블 + ETF 추천
- [ ] H. 3탭 퀀트 (피보나치/대형주/섹터로테이션)
- [ ] 빈 데이터 안내 메시지 (전 패널)

### 높음 (P1)
- [ ] E. 채권자경단 (야간 매매 판단 근거) 7개 지표
- [ ] F. NXT 시그널 + TOP5 + 성적표
- [ ] G. 단타 TOP픽 + 성적표 + 차트
- [ ] I. 수급 사이클 감지기 (4섹션)
- [ ] J. 기관 선매집 탐지 (잠복/움직임 2탭)

### 중간 (P2)
- [ ] K. 매집 레이더 리스트
- [ ] G-3. TODAY vs NXT 비교 테이블
- [ ] 각 탭 정렬 토글 (피보나치/대형주/섹터)
- [ ] 피보나치 게이지 바 (인라인)
- [ ] 섹터 상세 팝업 (피보나치 분포 + 점수 구성)
- [ ] 성적표 바 차트 + 누적선

### 낮음 (P3)
- [ ] 7일 히스토리 (BRAIN/자산배분 변화 추이)
- [ ] 반응형 최적화 (텔레그램 웹앱)
- [ ] 섹터 히트맵 Treemap (D3.js)

---

## 8. 참고: 사용자 설명 텍스트 모음 (UI에 삽입)

### 피보나치 눌림목
> 52주 고점 대비 크게 하락한 우량주를 피보나치 되돌림 기준으로 분류합니다.
> 깊이 빠진 종목일수록 반등 시 큰 수익이 가능하지만, 하락 이유를 반드시 확인하세요.

### 수급 사이클 감지기
> 외인·기관·개인·기타법인 4세력의 수급 흐름을 분석합니다.
> 🔥 급등임박 = 세력이 모았고, 개인이 바쳤고, 터질 준비 완료
> 📦 매집 = 조용히 모으는 중, 아직 때가 아님
> ⚠️ 고점경고 = 세력은 빠지고 개인만 남음, 탈출 검토

### 기관 선매집 탐지
> 기관과 외국인이 조용히 매수하고 있지만, 주가는 아직 움직이지 않은 종목입니다.
> 뉴스나 촉매가 나오면 급등할 가능성이 높은 "장전된 스프링" 종목입니다.
> 쌍매수(⚡) = 기관+외인 동시 매수로, 가장 강력한 신호입니다.

### 매집 레이더
> 외국인이 3일 이상 조용히 매집 중이나 아직 주가가 안 오른 "미발화" 종목을 감지합니다.
> 가온전선 패턴(4일 매집 → +42%)의 초기 진입 시점을 포착합니다.

---

> **끝. 이 문서를 웹봇에 전달하고 "이거 보고 스윙시스템 페이지 구현해"라고 하면 됩니다.**
