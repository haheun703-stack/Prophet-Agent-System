# FLOWX 스윙시스템 페이지 — 통합 구현 지시서 v2

> **이 문서 하나로 스윙시스템 페이지 전체를 구현할 수 있습니다.**
> 기존 A~K 평면 나열 → **3-Tier 정보 계층** 구조로 전면 개편.
> v1 대비 변경: 15개 섹션 뒤죽박죽 → "뭘 사라 / 시장 상황 / 심층 분석" 3단계 분리.

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

총 **14개 테이블** — 모두 이미 생성 완료, 매일 자동 갱신 중.

### Wave 1 (Tier 1 + 2, 즉시 로드)

| # | 테이블 | PK | 갱신 시각 | 용도 |
|---|--------|-----|----------|------|
| 1 | `dashboard_swing` | date | 07:30+16:45 | 메인 대시보드 (48 컬럼) |
| 2 | `intelligence_daytrading_picks` | (date, mode) | 16:45/07:35 | 단타 TOP픽 |
| 3 | `intelligence_nxt_picks` | date | 16:45 | NXT TOP5 추천 |
| 4 | `intelligence_limit_up_signals` | date | 16:20 | 상한가 엔진 시그널 |
| 5 | `intelligence_pension_scan` | date | 16:35 | 매집 합류 시그널 |
| 6 | `intelligence_accumulation_radar` | date | 16:35 | 매집 레이더 |
| 7 | `quant_market_brain` | date | 16:45 | AI 시장판단 |

### Wave 2 (Tier 3, 사용자가 펼칠 때 lazy load)

| # | 테이블 | PK | 갱신 시각 | 용도 |
|---|--------|-----|----------|------|
| 8 | `intelligence_daytrading_performance` | date | 16:30 | 단타 성적표 |
| 9 | `intelligence_nxt_performance` | pick_date | 16:30 | NXT 성적표 |
| 10 | `intelligence_limit_up_performance` | date | 16:20 | 상한가 Paper Trading 성적 |
| 11 | `intelligence_cycle_scan` | date | 16:30 | 수급 사이클 감지기 |
| 12 | `intelligence_stealth_scan` | date | 16:40 | 기관 선매집 탐지 |
| 13 | `swing_signals` | date | 07:30 | 상세 분석 (legacy) |

### 퀀트 대시보드 (Tier 3-B 탭 내 lazy load)

| # | 테이블 | PK | 갱신 시각 | 용도 |
|---|--------|-----|----------|------|
| 14 | `quant_sector_flow` | date | 16:45 | 섹터 수급 흐름 |
| 15 | `quant_etf_fund_flow` | date | 16:45 | ETF 투자자 수급 |
| 16 | `quant_sector_momentum` | date | 16:45 | 섹터 모멘텀 |
| 17 | `quant_etf_recommendation` | date | 16:45 | ETF 추천 |

### RLS 정책 (전 테이블 공통)
- `anon` → SELECT만 가능 (읽기 전용)
- `service_role` → ALL (백엔드 쓰기용)
- 프론트엔드에서는 **anon key**로 읽기만 하면 됩니다.

---

## 2. 페이지 전체 레이아웃 — 3-Tier 구조

### 설계 원칙

| Tier | 이름 | 사용자 질문 | 기본 상태 |
|------|------|-----------|----------|
| **TIER 1** | 오늘의 핵심 | "오늘 뭘 사지?" | **항상 펼침**, 최상단 |
| **TIER 2** | 시장 현황 | "시장 괜찮나?" | **펼침**, 스크롤 아래 |
| **TIER 3** | 심층 분석 | "자세히 파볼까" | **접힘**, 탭 구성 |

### 시각적 계층 구분

| Tier | 왼쪽 보더 | 라벨 색상 | 라벨 텍스트 |
|------|----------|----------|-----------|
| TIER 1 | 4px solid `#22c55e` | 초록 | `오늘의 핵심` |
| TIER 2 | 4px solid `#3b82f6` | 파랑 | `시장 현황` |
| TIER 3 | 4px solid `#9ca3af` | 회색 | `심층 분석` |

각 Tier 그룹 시작에 소형 라벨 표시:
```tsx
<div className="text-xs font-bold uppercase tracking-wider text-green-500 mb-2">
  오늘의 핵심
</div>
```

### 전체 레이아웃

```
┌──────────────────────────────────────────────────────────┐
│ ── TIER 1: 오늘의 핵심 ──────────────────────────────── │
│                                                          │
│ ┌─ 1-A. BRAIN 커맨드바 ─────────────────────────────┐   │
│ │ [표준] BRAIN 72점  "AA등급 이상 3종목 추천"  ◯파이 │   │
│ └───────────────────────────────────────────────────┘   │
│                                                          │
│ ┌─ 1-B. 오늘의 추천 ───────────────────────────────┐   │
│ │ [스윙 3] [단타TOP 7] [NXT야간 5] [상한가 2]       │   │
│ │ ─────────────────────────────────────────────      │   │
│ │ (선택된 탭의 종목 카드들)                           │   │
│ │ + ETF추천 / 감시풀 / watchlist (탭별 부속)          │   │
│ └───────────────────────────────────────────────────┘   │
│                                                          │
│ ┌─ 1-C. 매집합류 포착 (3건) ─────────── [더보기 >] ─┐   │
│ │ [쌍매수] 세아제강  [가속] LG에너지  [합류] 현대차  │   │
│ └───────────────────────────────────────────────────┘   │
│                                                          │
│ ── TIER 2: 시장 현황 ──────────────────────────────── │
│                                                          │
│ ┌─ 2-A. 시장 지표 ──────────────────────────────────┐   │
│ │ [DXY][USD/KRW][VIX][외인흐름][나스닥][금/유가]     │   │
│ │ 환율↔KOSPI 상관: 86%  ████████████░░░░              │   │
│ └───────────────────────────────────────────────────┘   │
│                                                          │
│ ┌─ 2-B. 시장 분석 ──────────────────────────────────┐   │
│ │ 수급/리스크/매크로/섹터/원자재 5줄 요약             │   │
│ │ ▶ 야간 판단 근거 (7지표)                [펼치기 ▼] │   │
│ └───────────────────────────────────────────────────┘   │
│                                                          │
│ ┌─ 2-C. AI 시장판단 (BRAIN Quant) ──────────────────┐   │
│ │ [MEDIUM] 투자비중 70%  HOT: 반도체  COLD: 바이오   │   │
│ └───────────────────────────────────────────────────┘   │
│                                                          │
│ ── TIER 3: 심층 분석 ──────────────────────────────── │
│                                                          │
│ ┌─ 3-A. 성적표 (수익률 추적) ──────────── [펼치기 ▼] ┐   │
│ └───────────────────────────────────────────────────┘   │
│ ┌─ 3-B. 퀀트 분석 도구 ───────────────── [펼치기 ▼] ┐   │
│ └───────────────────────────────────────────────────┘   │
│ ┌─ 3-C. 수급 심층 분석 ───────────────── [펼치기 ▼] ┐   │
│ └───────────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────────────┘
```

---

## 3. 각 패널 상세

---

### 1-A. BRAIN 커맨드바 (히어로)

**위치**: TIER 1 최상단
**데이터**: `dashboard_swing` — `brain_verdict`, `brain_pct`, `brain_reason`, `market_comment`, `brain_raw_pct`, `brain_capped_pct`, `regime_cap_reason`, `regime_desc`, `alloc_*`, `portfolio`

#### 레이아웃
```
┌──────────────────────────────────────────────────────────┐
│  [표준]  BRAIN 72점                    ┌──────────┐      │
│  "표준모드 — AA등급 이상 3종목 추천"   │  ◯ 파이  │      │
│  2026-05-10 07:30 갱신                 │  차트    │      │
│  체제: 모멘텀 장세                     └──────────┘      │
└──────────────────────────────────────────────────────────┘
```

- **좌측**: verdict 뱃지(크게) + 점수 + reason 텍스트 + 갱신시각 + 체제 설명
- **우측**: 자산배분 파이차트 (인라인, 소형)
- 배경색: verdict에 따라 그라데이션 (공격=녹색, 표준=파랑, 방어=노랑, 관망=빨강)

| brain_verdict | 배경색 | 의미 |
|---------------|--------|------|
| 공격 | 녹색 | 적극 매수 |
| 표준 | 파랑 | 일반 매수 |
| 방어 | 노랑 | 축소 운영 |
| 관망 | 빨강/회색 | 현금 대기 |

**캡 표시**: `regime_cap_reason`이 비어있지 않으면 → `"BRAIN 72점 (원본 100점, 비용상승→50%캡)"` 형태로 표시

#### 자산배분 파이차트
```
alloc_swing(스윙) + alloc_gold_etf(금ETF) + alloc_inverse(인버스)
+ alloc_group_etf(그룹ETF) + alloc_small_cap(소형주) + alloc_cash(현금) = 100%
```

**추가 필드**: `regime` (시장 체제), `regime_severity` (체제 강도)

#### 모델 포트폴리오 (`portfolio` JSONB)
```typescript
interface Portfolio {
  current_picks: number;
  brain_pct: number;
  brain_cash_ratio: number;
  total_trades: number;
  win_rate: number;
}
```
- `total_trades > 0`이면 "승률 {win_rate}% ({total_trades}건)" 표시

#### 센서 필드 (향후 활성화)
아래 필드는 현재 값이 0 또는 빈 문자열이지만, 향후 활성화될 예정:
- `smart_money_score` / `smart_money_signal`
- `stress_index` / `stress_level`
- `rotation_signal`
- `liquidity_score`
→ 값이 0이 아닐 때만 UI에 표시

---

### 1-B. 오늘의 추천 (4탭 통합 패널)

**위치**: TIER 1, BRAIN 커맨드바 바로 아래
**핵심 변경**: 기존 4개 별도 섹션(D, G-1, F-2, 상한가)을 하나의 탭 패널로 통합

#### 탭 구성

```
┌──────────────────────────────────────────────────────────┐
│  오늘의 추천                                              │
│  [스윙 추천 3] [단타 TOP 7] [NXT 야간 5] [상한가 엔진 2]  │
├──────────────────────────────────────────────────────────┤
│                                                          │
│  (선택된 탭의 카드들)                                     │
│                                                          │
└──────────────────────────────────────────────────────────┘
```

- 각 탭에 건수 뱃지 표시 (예: "단타 TOP 7")
- 건수 0인 탭: 회색 비활성
- 탭 높이 변화 시 `min-height` 설정으로 레이아웃 점프 방지

#### 탭 자동 선택 로직
```typescript
function getDefaultTab(data): PickTab {
  if (data.brain_verdict === "관망") return 'nxt';
  const hour = new Date().getHours();
  if (hour >= 17 || hour < 9) return 'nxt';
  if (data.limitUpSignals?.triggered_count > 0) return 'limitup';
  return 'swing';
}
```

---

#### 1-B-1. [스윙 추천] 탭

**데이터**: `dashboard_swing.picks` + `dashboard_swing.etf_picks` + `dashboard_swing.watchlist`

##### picks (스윙 종목) JSONB 배열
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

##### etf_picks (ETF 추천) JSONB 배열
```json
[{"code":"069500","name":"KODEX 200","category":"시장대표","signal":"BUY",
  "entry":35000,"sl":33500,"tp":37000,"reason":"저평가+수급유입","holding_days":5}]
```
→ 스윙 탭 하단에 "ETF 추천" 서브섹션으로 표시

##### watchlist (반등감시 — 관망모드 시)
```json
[{"code":"004250","name":"NPC","grade":"BB","score":53.6,
  "reason":"반등 감시 — 모멘텀 레짐","trigger":"종가 4,015원 돌파 시"}]
```
- `brain_verdict === "관망"`일 때만 이 서브섹션 표시

---

#### 1-B-2. [단타 TOP] 탭

**데이터**: `intelligence_daytrading_picks`

##### Hybrid 2단 발행

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

##### picks JSONB 구조
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
  etf_alt_code: string;
  etf_alt_name: string;
  etf_alt_theme: string;
}
```

##### UI 카드
```
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

- **mode 뱃지**: preview → `프리뷰`, confirmed → `확정`
- **트랙 필터**: 전체 | 대형주 (Track A) | 중소형주 (Track B)
- **rank 1~3**: 메달 표시

---

#### 1-B-3. [NXT 야간] 탭

**데이터**: `dashboard_swing.nxt_signal`, `nxt_score`, `nxt_signal_text`, `nxt_reason`, `nxt_targets` + `intelligence_nxt_picks`

##### NXT 시그널 카드 (탭 상단)
```
🌙 NXT 야간매매  시그널: BUY (+7.5)
"VIX 20 + 한국(+2.5) → 매수 유망"
대상: [nxt_targets 리스트]
```

##### NXT TOP5 추천 (`intelligence_nxt_picks`)
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

---

#### 1-B-4. [상한가 엔진] 탭

**데이터**: `intelligence_limit_up_signals`

> 상세 데이터 구조 및 UI 가이드는 `FLOWX_LIMIT_UP_ENGINE_SPEC.md` 참조

##### 핵심 요소
- `triggered` 배열: 시그널 카드 (수급등급 + 분할매수 계획)
- `watchlist` 배열: 감시풀 (기본 접힘)
- `Paper Trading` 뱃지 (실매매 전환 전까지)

##### triggered 카드 요약
```
┌────────────────────────────────────────┐
│ [분할매수]  진원생명과학 (011000)        │
│ 3회차 상한가 · 원점+44% · 시총 790억   │
│ 📊 수급: 중립  외인-120백만  기관+85백만 │
│ 💰 분할매수: 1차 1,095(50%) 2차 1,129(30%) 3차 1,049(20%) │
│ 목표: 1,204 (+10%)                     │
└────────────────────────────────────────┘
```

##### 감시풀 (접기/펼치기)
```
📋 감시풀 3건 — 눌림목+수급 대기 중    [▼]
```

##### 빈 상태
```
⚡ 상한가 엔진 · 2026-05-12
오늘은 상한가 시그널이 없습니다
```

---

### 1-C. 매집합류 포착 (Quick Strip)

**위치**: TIER 1 하단, 가로 한 줄
**데이터**: `intelligence_pension_scan.best_fresh` (TOP 3) + `intelligence_accumulation_radar.stocks` (TOP 2)

#### 레이아웃
```
┌──────────────────────────────────────────────────────────┐
│  매집 합류 포착 (3건)                              [더보기]│
│  [쌍매수] 세아제강  [가속] LG에너지  [합류] 현대차         │
└──────────────────────────────────────────────────────────┘
```

- pension_scan의 `best_fresh` + accumulation_radar의 `stocks`에서 종목코드 중복 제거
- 상위 3~5개만 가로 뱃지로 표시
- `[더보기]` 클릭 → 3-C 수급 심층 분석 자동 펼침 + 스크롤

#### 태그 뱃지 색상

| tag | 색상 | 출처 |
|-----|------|------|
| 쌍매수 | 빨강/핫핑크 | accumulation_radar |
| 가속전환 | 주황 | accumulation_radar |
| 바닥매집 | 파랑 | accumulation_radar |
| 외인매집 | 하늘색 | accumulation_radar |
| 합류 | 보라 | pension_scan (연기금+금투 합류) |

- 5일 등락률이 마이너스 → "미발화" 강조 표시 (초록)

---

### 2-A. 시장 지표 (FX + 글로벌 통합)

**위치**: TIER 2 최상단
**데이터**: `dashboard_swing.fx_monitor` + `dashboard_swing`의 시장지표 필드

#### 기존 A (FX 모니터) + C (글로벌 지표)를 6칸 카드 행으로 통합

```
┌──────────────────────────────────────────────────────────┐
│  시장 지표                                                │
│  ┌──────┐ ┌──────┐ ┌──────┐ ┌──────┐ ┌──────┐ ┌──────┐ │
│  │ DXY  │ │USD/KR│ │ VIX  │ │외인   │ │나스닥│ │금/유가│ │
│  │100.03│ │1,510 │ │23.87 │ │+966억 │ │-0.3% │ │-0.1% │ │
│  │▼약세 │ │▼원강 │ │CONTAN│ │🟢전환 │ │      │ │      │ │
│  └──────┘ └──────┘ └──────┘ └──────┘ └──────┘ └──────┘ │
│                                                          │
│  📊 환율↔KOSPI 상관: 86% (최근 14일 중 12일 역상관)       │
│  [████████████████████░░░░] 86%                           │
└──────────────────────────────────────────────────────────┘
```

#### 카드 1~3: FX 모니터 (`dashboard_swing.fx_monitor`)

```typescript
interface FxMonitor {
  timestamp: string;
  dxy: { value: number; prev: number; chg_1d: number; ma5: number; ma20: number; trend: string; };
  usdkrw: { value: number; prev: number; chg_1d: number; ma5: number; ma20: number; trend: string; };
  vix_structure: { vix: number; vix3m: number; ratio: number; structure: string; label: string; };
  correlation: { matches: number; total: number; pct: number; label: string; };
  foreign_flow: { proxy: string; today_억: number; sum_3d_억: number; streak: number; direction: string; signal: string; signal_color: string; };
  verdict: { text: string; color: string; bullish: number; bearish: number; score: number; };
}
```

#### 카드 4: 외인 흐름 (`fx_monitor.foreign_flow`)
#### 카드 5~6: 글로벌 지표 (`dashboard_swing`)

| 필드 | 표시명 | 포맷 |
|------|--------|------|
| nasdaq_pct | 나스닥 | -0.31% |
| oil_pct + gold_pct | 금/유가 | 금 -0.1% / 유가 +2.2% |

#### verdict 배지 (카드 행 하단)
| color | 텍스트 예 | 배경색 |
|-------|----------|--------|
| GREEN | "외국인 유입 강력" | #22c55e |
| YELLOW | "중립 (관망)" | #eab308 |
| RED | "외국인 유출 경고" | #ef4444 |

#### 추세 화살표 규칙
| 필드 | 값 | 화살표 | 색상 |
|------|-----|--------|------|
| dxy.trend | "약세" | ▼ | 초록 |
| dxy.trend | "강세" | ▲ | 빨강 |
| usdkrw.trend | "원강세" | ▼ | 초록 |
| usdkrw.trend | "원약세" | ▲ | 빨강 |

#### 빈 데이터
`fx_monitor`가 null → "데이터 수집 중입니다. 16:45 이후 갱신됩니다."

---

### 2-B. 시장 분석 (5대 분석 + NXT Rationale)

**위치**: TIER 2, 시장 지표 아래
**데이터**: `dashboard_swing.analysis` + `dashboard_swing.nxt_rationale`

#### 5대 분석 (`analysis` JSONB) — 기본 펼침
```
┌──────────────────────────────────────────────────────────┐
│  시장 분석                                                │
│                                                          │
│  수급: 외국인 순매수 3일차, 기관 소폭 매도                  │
│  리스크: VIX 27.6 (EXTREME), 원/달러 1507                 │
│  매크로: 미국 금리 동결 기조, 한국 수출 호조                 │
│  섹터: 반도체 HOT, 2차전지 WARMING, 바이오 COLD            │
│  원자재: 금 +1.7%, 유가 +0.5%, 은 +3.2%                   │
│                                                          │
│  ▶ 야간 매매 판단 근거 (7개 지표)               [펼치기 ▼] │
│  (접힌 상태: 종합 [적극 매수] 🟢×5  🟡×1  🔴×1)            │
└──────────────────────────────────────────────────────────┘
```

#### 야간 매매 판단 근거 (`nxt_rationale`) — 기본 **접힘**

접힌 상태에서 한 줄 요약만 표시: `종합: [적극 매수] 🟢×5  🟡×1  🔴×1`

펼치면 7개 지표 테이블:

```typescript
interface NxtRationale {
  timestamp: string;
  verdict: string;       // "적극 매수" | "조건부 매수" | "경계" | "회피"
  green: number;
  yellow: number;
  red: number;
  total: number;
  indicators: Indicator[];
}

interface Indicator {
  key: string;
  name: string;
  signal: string;        // "GREEN" | "YELLOW" | "RED"
  signal_label: string;
  detail: string;
}
```

```
┌──────────────────┬────────┬──────────────────┐
│ MOVE 채권공포     │ 🟢안전 │ 81.78            │
│ VIX 기간구조      │ 🟢안전 │ VIX 23.87/VIX3M 24.72 │
│ 구리/금 비율      │ 🟢안전 │ 구리 -1.1% vs 금 -2.8% │
│ 엔 캐리트레이드   │ 🟢안전 │ JPY 159.63 (ON)  │
│ VVIX 스마트머니   │ 🟢안전 │ 115.33           │
│ 신용스프레드      │ 🔴위험 │ HYG +0.24%       │
│ BTC 야간심리      │ 🟡경계 │ $67,198 (+0.4%)  │
└──────────────────┴────────┴──────────────────┘
```

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
| VIX 기간구조 | CONTANGO=정상, BACKWARDATION=패닉 |
| 구리/금 비율 | 구리 강세=경기 회복, 금 강세=불안 |
| 엔 캐리트레이드 | 엔 약세(ON)=위험자산 선호, 강세(OFF)=안전자산 선호 |
| VVIX 스마트머니 | VIX의 VIX. MA20 대비 높으면 기관 헤지 중 |
| 신용스프레드 | HYG 강세=리스크온, LQD 강세=리스크오프 |
| BTC 야간심리 | 비트코인 24시간 변동. 야간 투자심리 바로미터 |

---

### 2-C. AI 시장판단 (BRAIN Quant)

**위치**: TIER 2 최하단
**데이터**: `quant_market_brain`

```sql
SELECT * FROM quant_market_brain ORDER BY date DESC LIMIT 1;
```

#### 레이아웃
```
┌──────────────────────────────────────────────────────────┐
│  AI 시장판단 (BRAIN Quant)                                │
│  [MEDIUM RISK]  투자비중 권장: 70%                         │
│  "미국 금리 동결 기조 속 반도체 수출 호조, 2차전지 재편..." │
│  HOT: 반도체, 조선   NEXT: 자동차   COLD: 바이오           │
│  주도 매수: 외국인  매크로: BULLISH                         │
└──────────────────────────────────────────────────────────┘
```

#### 데이터 구조 (주요 필드)
| 필드 | 타입 | 표시 |
|------|------|------|
| overall_verdict | TEXT | 종합 판단 서술 |
| position_size_pct | INT | 투자비중 권장 (0~100%) |
| macro_direction | TEXT | STRONG_BULL~STRONG_BEAR |
| risk_level | TEXT | LOW/MEDIUM/HIGH/EXTREME |
| risk_score | REAL | 리스크 점수 |
| hot_sectors | JSONB | HOT 섹터명 |
| next_sectors | JSONB | NEXT 섹터명 |
| cooling_sectors | JSONB | COLD 섹터명 |
| dominant_buyer | TEXT | 주도 매수 주체 |

#### risk_level 뱃지
| risk_level | 색상 | 배경 |
|------------|------|------|
| LOW | #22c55e | 연녹 |
| MEDIUM | #eab308 | 연노랑 |
| HIGH | #ef4444 | 연빨강 |
| EXTREME | #dc2626 | 진빨강 + 테두리 |

---

### 3-A. 성적표 — 수익률 추적 (접힘)

**위치**: TIER 3, 기본 **접힘**
**내부 탭**: [단타 성적] [NXT 성적] [상한가 Paper]

접힌 상태에서 한 줄 요약:
```
▶ 성적표 (수익률 추적)  단타 +2.1% 승률67% | NXT +1.3% | 상한가 +1.75%   [펼치기 ▼]
```

---

#### 3-A-1. [단타 성적] 탭

**데이터**: `intelligence_daytrading_performance`

```sql
SELECT * FROM intelligence_daytrading_performance ORDER BY date DESC LIMIT 1;
```

| 컬럼 | 설명 |
|------|------|
| date (PK) | 거래일 |
| avg_return | 당일 평균 수익률(%) |
| best_pick / worst_pick | 최고/최저 종목 |
| weekly_return / monthly_return | 주간/월간 누적(%) |
| items (JSONB) | 종목별 상세 |

**차트**: 최근 20거래일 바 차트 (양수=녹색, 음수=빨강) + 누적선 오버레이

**TODAY vs NXT 비교 테이블**:
- 왼쪽: TODAY 단타 TOP 5 (daytrading_performance)
- 오른쪽: NXT 야간매수 TOP 5 (nxt_performance)
- 동일 날짜 기준 수익률 비교

---

#### 3-A-2. [NXT 성적] 탭

**데이터**: `intelligence_nxt_performance`

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

**차트**: 최근 20거래일 바 차트 + 누적선 오버레이

---

#### 3-A-3. [상한가 Paper] 탭

**데이터**: `intelligence_limit_up_performance`

> 상세 UI 가이드는 `FLOWX_LIMIT_UP_ENGINE_SPEC.md` 섹션 4 참조

요약 카드 (4칸 가로):
```
┌──────────┬──────────┬──────────┬────────────┐
│ 가상자금   │ 누적수익  │ 승률     │ MDD        │
│ 1,000만원  │ +1.75%   │ ---%     │ -5.1%      │
└──────────┴──────────┴──────────┴────────────┘
```

+ 보유 포지션 테이블 + 당일 청산 내역

---

### 3-B. 퀀트 분석 도구 (접힘)

**위치**: TIER 3, 기본 **접힘**
**내부 탭**: [전체 피보나치] [대형주 피보나치] [섹터 로테이션] [섹터 수급] [ETF 수급] [섹터 모멘텀]

접힌 상태에서 한 줄 요약:
```
▶ 퀀트 분석 도구  피보나치 50종목 | 섹터 선도 5개 | ETF 3건   [펼치기 ▼]
```

---

#### 3-B-1. [전체 피보나치] 탭

**데이터**: `dashboard_swing.fib_stocks` (JSONB 배열, 최대 50종목)

```typescript
interface FibStock {
  code: string;
  name: string;
  sector: string;
  cap: number;            // 시총 (억원)
  price: number;
  w52h: number;
  w52l: number;
  drop: number;           // 52주 고점 대비 하락률 %
  fib_zone: string;       // "DEEP" | "MID" | "MILD" | "SHALLOW"
  fib_zone_label: string;
  fib_382: number;
  fib_500: number;
  fib_618: number;
  fib_status: string;
  target: number;
  upside: number;
  per: number;
  pbr: number;
  frgn: number;
}
```

**fib_zone별 그룹핑**:

| fib_zone | 배경색 | 텍스트색 | 의미 |
|----------|--------|---------|------|
| DEEP | #FEF2F2 | #DC2626 | 50%+ 하락, 바닥 매수 |
| MID | #FFF7ED | #EA580C | 40~50% 하락, 중간 눌림 |
| MILD | #FFFBEB | #CA8A04 | 30~40% 하락, 1차 눌림 |
| SHALLOW | #F7FEE7 | #65A30D | 15~30% 하락, 얕은 조정 |

**정렬**: 기본 fib_zone별 그룹핑 + 하락률순. 토글: 상승여력순/PER순/외국인순

---

#### 3-B-2. [대형주 피보나치] 탭

**데이터**: `dashboard_swing.fib_leaders` (JSONB 배열, 30종목)

시총 TOP 30 **무조건 표시** (하락률 무관).

```typescript
interface FibLeader {
  code: string;
  name: string;
  sector: string;
  cap: number;
  price: number;
  w52h: number;
  w52l: number;
  w52h_date: string;
  drop: number;
  fib_382: number;
  fib_500: number;
  fib_618: number;
  fib_status: string;
  fib_zone?: string;
  per: number;
  pbr: number;
}
```

**fib_status 색상**:
| fib_status | 색상 |
|------------|------|
| 고점 근접 | 초록 |
| 61.8% 위 (회복 중) | 연초록 |
| 50%~61.8% 사이 | 노랑 |
| 38.2%~50% 사이 | 주황 |
| 38.2% 아래 (깊은 하락) | 빨강 |

---

#### 3-B-3. [섹터 로테이션] 탭

**데이터**: `dashboard_swing.sector_rotation` (JSONB)

```typescript
interface SectorRotation {
  timestamp: string;
  total_sectors: number;
  total_stocks: number;
  sectors: Sector[];
}

interface Sector {
  sector: string;
  count: number;
  total_score: number;
  momentum: number;
  flow_score: number;
  dual_bonus: number;
  avg_chg: number;
  avg_drop: number;
  avg_upside: number;
  net_flow_억: number;
  dual_buy_3d: number;
  up_count: number;
  down_count: number;
  deep: number;
  mid: number;
  mild: number;
  shallow: number;
  cap_조: number;
  cap_億: number;
  warning: string;
  stage: string;       // "선도" | "추격" | "대기" | "후발"
  stage_num: number;
  stage_color: string;
}
```

**stage별 그룹핑**:
| stage | 조건 | 색상 | 배경 |
|-------|------|------|------|
| 선도 | total_score 50+ | #16A34A | #F0FDF4 |
| 추격 | 20~49 | #22C55E | #F0FDF4 |
| 대기 | 0~19 | #EAB308 | #FEFCE8 |
| 후발 | 0 미만 | #DC2626 | #FEF2F2 |

---

#### 3-B-4. [섹터 수급] 탭

**데이터**: `quant_sector_flow`

23개 섹터 기관·외국인 순매수 + 연속일 + 합의매수 판단.

| 컬럼 | 설명 |
|------|------|
| sectors (JSONB) | 배열: {섹터, 기관_당일~연속일, 외인_당일~연속일, 판단, 설명, 보정점수} |
| top_inflow | 매수 집중 TOP3 섹터명 |
| top_outflow | 이탈 TOP3 섹터명 |
| signal | 한줄 요약 |

---

#### 3-B-5. [ETF 수급] 탭

**데이터**: `quant_etf_fund_flow`

21개 ETF 기관·외인·개인 수급 + 시장방향 + 인버스경고 + 안전자산.

| 컬럼 | 설명 |
|------|------|
| etfs (JSONB) | 배열: {종목코드, ETF명, 분류, 기관/외인/개인 수급, 시그널, 강도, 등락률} |
| market_direction | BULLISH/BEARISH/NEUTRAL |
| inverse_warning | 인버스 기관매수 경고 |
| safe_haven_signal | RISK_ON/RISK_OFF/NEUTRAL |

---

#### 3-B-6. [섹터 모멘텀] 탭

**데이터**: `quant_sector_momentum`

23개 섹터 HOT/COLD 상태 + 수익률 + 가속도 + 주도주.

| 컬럼 | 설명 |
|------|------|
| sectors (JSONB) | 배열: {섹터, 상태, 순위, 당일/3일/5일수익률, 상승비율, 가속도, 거래량폭증, 주도주} |
| hot_sectors | HOT 섹터명 목록 |
| cold_sectors | COLD 섹터명 목록 |
| rotation_signal | 로테이션 요약 |

---

### 3-C. 수급 심층 분석 (접힘)

**위치**: TIER 3, 기본 **접힘**
**내부 탭**: [수급 사이클 감지기] [기관 선매집 탐지] [매집 레이더]

접힌 상태에서 한 줄 요약:
```
▶ 수급 심층 분석  급등임박 5건 | 잠복 50건 | 매집 12건   [펼치기 ▼]
```

**1-C [더보기]에서 연결**: 클릭 시 이 섹션 자동 펼침 + 스크롤

---

#### 3-C-1. [수급 사이클 감지기] 탭

**데이터**: `intelligence_cycle_scan`

```sql
SELECT * FROM intelligence_cycle_scan ORDER BY date DESC LIMIT 1;
```

##### 6가지 사이클 위상
| 위상 | 한국어 | 색상 | 의미 |
|------|--------|------|------|
| SURGE | 급등임박 | 빨강/금색 | 매수 검토 대상 |
| ACCUMULATION | 매집 | 주황 | 관심 종목 등록 |
| REVERSAL | 전환 | 초록 | 바닥 전환 징후 |
| NEUTRAL | 중립 | 회색 | 특별 신호 없음 |
| DISTRIBUTION | 물량분배 | 보라 | 세력→개인 떠넘기기 |
| PEAK_WARN | 고점경고 | 검정/빨강 | 하락 전조 |

##### 데이터 구조
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
  surge_items: CycleStock[];
  accumulate_items: CycleStock[];
  reversal_items: CycleStock[];
  warning_items: CycleStock[];
  top_surge_names: string[];
  phase_summary: Record<string, number>;
}

interface CycleStock {
  code: string;
  name: string;
  phase: string;
  phase_kr: string;
  score: number;        // -100 ~ +100
  latest_close: number;
  change_pct: number;
  cap_억: number;
  market: string;
  summary: string;
  surge_type?: string;  // "지속" | "원샷"
  signals: Signal[];
}

interface Signal {
  name: string;
  name_kr: string;
  score: number;
  detail: string;
  days: number;
}
```

##### 8가지 감지 신호
| name | 한국어 | 점수 | 색상 |
|------|--------|------|------|
| twin_buy | 쌍매수 | +25 | 금색 |
| twin_sell | 쌍매도 | -20 | 빨강 |
| retail_sacrifice | 개인바침 | +20 | 보라 |
| stealth_acc | 기타매집 | +15 | 남색 |
| stealth_exit | 기타이탈 | -20 | 검정 |
| force_reversal | 세력전환 | +20 | 초록 |
| retail_trap | 개인함정 | -15 | 회색 |
| triple_buy | 3세력매수 | +10 | 금색 |

##### UI: 4개 서브섹션
```
🔍 수급 사이클 감지기  |  총 40종목 스캔
🔥 급등임박 5  |  📦 매집 12  |  🔄 전환 3  |  ⚠️ 경고 5

── 🔥 급등임박 (surge_items) ──
┌──────────────────────────────────────────────────┐
│ 🔥 [지속] 현대차  [KOSPI]  +100점  508,000원      │
│ ⚡ 쌍매수 (+25) — 외인+기관 3일 중 2일 동시 매수     │
│ 🕵️ 기타매집 (+30) — 기타법인 7일 연속 +926억        │
└──────────────────────────────────────────────────┘
```

- `surge_type` 뱃지: [지속]=초록, [원샷]=빨강

##### 점수별 색상
| 점수 | 스타일 |
|------|--------|
| +80↑ | 금색 테두리 + 빨강 |
| +50~79 | 주황 |
| +20~49 | 노랑 |
| -19~+19 | 회색 |
| -50↓ | 빨강 테두리 + 검정 |

---

#### 3-C-2. [기관 선매집 탐지] 탭

**데이터**: `dashboard_swing.stealth_stocks` 또는 `intelligence_stealth_scan`

```typescript
interface StealthData {
  timestamp: string;
  stealth: StealthStock[];  // 잠복 종목
  moving: StealthStock[];   // 움직임 종목
  summary: {
    total_detected: number;
    stealth_count: number;
    moving_count: number;
    surged_count: number;
    top_stealth: string[];
  };
}

interface StealthStock {
  code: string;
  name: string;
  sector: string;
  score: number;       // 0~140
  pattern: string;     // "쌍매수2D" | "기관5D" | "외인4D"
  dual_buy: boolean;
  inst_avg: number;    // 기관 일평균 순매수 (백만원) → /100 = 억
  frgn_avg: number;
  chg_5d: number;
  close: number;
  cap: number;
  category: string;
}
```

##### UI: 2 서브탭
- **잠복** — 배경 연보라/남색
- **움직임** — 배경 연파란

```
기관 선매집 탐지  |  잠복 50종목  |  움직임 12종목

┌─────────────────────────────────────────┐
│ ⚡ LG에너지솔루션  [전기전자]   125점    │
│ 쌍매수2D  │  I+361억  F+191억           │
│ 현재가 408,500원  │  5일 +0.4%          │
└─────────────────────────────────────────┘
```

##### 패턴 뱃지
| 패턴 | 색상 | 의미 |
|------|------|------|
| 쌍매수 | 보라/금색 | 기관+외인 동시 (가장 강력) |
| 기관 | 빨강 | 기관만 매수 |
| 외인 | 파랑 | 외인만 매수 |

- `dual_buy = true` → 금색 테두리
- `inst_avg / 100` = 억원 → "I+361억"

##### 대안 데이터: `intelligence_stealth_scan`
`dashboard_swing.stealth_stocks`가 비어있을 경우 직접 조회:
```sql
SELECT * FROM intelligence_stealth_scan ORDER BY date DESC LIMIT 1;
```

---

#### 3-C-3. [매집 레이더] 탭

**데이터**: `intelligence_accumulation_radar`

```sql
SELECT * FROM intelligence_accumulation_radar ORDER BY date DESC LIMIT 1;
```

```typescript
interface AccStock {
  code: string;
  name: string;
  frgn_days: number;
  accel_b: number;
  chg5: number;
  tag: string;        // "쌍매수" | "가속전환" | "바닥매집" | "외인매집"
  last_dual: boolean;
  supply_score: number;
  combined_supply: number;
}
```

##### UI: 리스트형
```
🔍 매집 레이더 — 외인 조용한 매집 감지

┌──────────────────────────────────────────────┐
│ [쌍매수]  세아제강지주                          │
│ 외인 4일 매집 · +16억/일 가속 · 5일 -8.0%      │
└──────────────────────────────────────────────┘
```

##### 태그 뱃지 색상
| tag | 색상 |
|-----|------|
| 쌍매수 | 빨강/핫핑크 |
| 가속전환 | 주황 |
| 바닥매집 | 파랑 |
| 외인매집 | 하늘색 |

- `chg5` 마이너스 → "미발화" 강조 (초록)
- `chg5` +5% 이상 → "이미 출발" (회색 처리)
- `last_dual = true` → 아이콘 강조

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

| 구간 | 처리 |
|------|------|
| 데스크톱 (1024px+) | 기본 레이아웃 |
| 태블릿 (768~1023px) | 6칸 카드 → 3×2 그리드, 탭 패널 유지 |
| 모바일 (<768px) | 6칸 카드 → 2×3 그리드, 추천 카드 1열 스택 |

텔레그램 웹앱 내에서도 표시 가능해야 함.

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

### 4-6. CollapsibleSection 컴포넌트

TIER 3의 접힘/펼침에 사용:

```typescript
interface CollapsibleSectionProps {
  title: string;           // "성적표 (수익률 추적)"
  summaryLine: string;     // 접힌 상태 한 줄 요약
  defaultOpen: boolean;    // TIER 3은 false
  tier: 1 | 2 | 3;        // 좌측 보더 색상 결정
  badge?: string;          // 우측 건수 뱃지
  children: React.ReactNode;
}
```

- 헤더: 제목 + 건수뱃지 + 펼치기/접기 chevron
- Tier별 좌측 보더 색상 적용
- 부드러운 `max-height` 트랜지션 애니메이션
- 접힌 상태: `summaryLine` 한 줄만 표시

### 4-7. 피보나치 레벨 설명 (툴팁)
| 레벨 | 의미 |
|------|------|
| 38.2% | 가장 약한 되돌림. 반등 = 강한 추세 |
| 50% | 중간 되돌림. 가장 많이 사용되는 지지선 |
| 61.8% | 황금비율. 여기를 깨면 추세 전환 가능 |
| 현재가 < 38.2% | 깊은 눌림, 바닥 매수 후보 |
| 현재가 > 61.8% | 상승 추세 복귀 중 |

---

## 5. Supabase API 쿼리 모음

### Wave 1 — 즉시 로드 (Tier 1 + 2)

```javascript
// 6개 병렬 쿼리로 TIER 1 + 2 한 번에 로드
const [dashRes, dtPicksRes, nxtPicksRes, luSigRes, pensionRes, radarRes, brainRes] =
  await Promise.all([
    // 1. 메인 대시보드
    supabase.from('dashboard_swing')
      .select('*').order('date', { ascending: false }).limit(1).single(),
    // 2. 단타 TOP픽 (confirmed 우선)
    supabase.from('intelligence_daytrading_picks')
      .select('*').eq('date', today).order('mode', { ascending: true }).limit(1).single(),
    // 3. NXT TOP5
    supabase.from('intelligence_nxt_picks')
      .select('*').order('date', { ascending: false }).limit(1).single(),
    // 4. 상한가 시그널
    supabase.from('intelligence_limit_up_signals')
      .select('*').order('date', { ascending: false }).limit(1).single(),
    // 5. 매집 합류
    supabase.from('intelligence_pension_scan')
      .select('best_fresh, best_count, best_fresh_count')
      .order('date', { ascending: false }).limit(1).single(),
    // 6. 매집 레이더 (Quick Strip용 TOP 3)
    supabase.from('intelligence_accumulation_radar')
      .select('date, stocks').order('date', { ascending: false }).limit(1).single(),
    // 7. AI 시장판단
    supabase.from('quant_market_brain')
      .select('*').order('date', { ascending: false }).limit(1).single(),
  ]);
```

### Wave 2 — Tier 3 lazy load (사용자가 펼칠 때)

```javascript
// 성적표 (3-A)
const loadPerformance = async () => {
  const [dtPerf, nxtPerf, luPerf] = await Promise.all([
    supabase.from('intelligence_daytrading_performance')
      .select('*').order('date', { ascending: false }).limit(20),
    supabase.from('intelligence_nxt_performance')
      .select('*').order('pick_date', { ascending: false }).limit(20),
    supabase.from('intelligence_limit_up_performance')
      .select('*').order('date', { ascending: false }).limit(1).single(),
  ]);
  return { dtPerf, nxtPerf, luPerf };
};

// 수급 심층 (3-C)
const loadSupplyDeep = async () => {
  const [cycleScan, stealthScan] = await Promise.all([
    supabase.from('intelligence_cycle_scan')
      .select('*').order('date', { ascending: false }).limit(1).single(),
    supabase.from('intelligence_stealth_scan')
      .select('*').order('date', { ascending: false }).limit(1).single(),
  ]);
  return { cycleScan, stealthScan };
};

// 퀀트 분석 (3-B) — 필요한 탭만
const loadQuant = async (tab: string) => {
  const tableMap = {
    sector_flow: 'quant_sector_flow',
    etf_flow: 'quant_etf_fund_flow',
    momentum: 'quant_sector_momentum',
  };
  return supabase.from(tableMap[tab])
    .select('*').order('date', { ascending: false }).limit(1).single();
};
```

### 히스토리 (최근 7일)
```javascript
const { data } = await supabase
  .from('dashboard_swing')
  .select('date, brain_pct, brain_verdict, alloc_swing, alloc_cash, vix')
  .order('date', { ascending: false })
  .limit(7);
```

---

## 6. 데이터 갱신 스케줄

| 시각 (KST) | 테이블 | 내용 |
|------------|--------|------|
| 07:30~07:50 | dashboard_swing | 메인 대시보드 (BRAIN + 추천 + 글로벌지표) |
| 07:35~07:50 | intelligence_daytrading_picks (confirmed) | 단타 TOP픽 확정본 |
| 16:10~16:20 | intelligence_limit_up_signals | 상한가 엔진 시그널 |
| 16:10~16:20 | intelligence_limit_up_performance | 상한가 Paper Trading |
| 16:30~16:50 | dashboard_swing 갱신 | fib, sector_rotation, fx_monitor, nxt_rationale, stealth 추가 |
| 16:30~16:50 | intelligence_cycle_scan | 수급 사이클 감지기 |
| 16:30~16:50 | intelligence_accumulation_radar | 매집 레이더 |
| 16:30~16:50 | intelligence_daytrading_performance | 단타 성적표 |
| 16:35 | intelligence_pension_scan | 매집 합류 시그널 |
| 16:40 | intelligence_stealth_scan | 기관 선매집 탐지 |
| 16:45~17:00 | intelligence_daytrading_picks (preview) | 단타 TOP픽 프리뷰 |
| 16:45~17:00 | intelligence_nxt_picks | NXT TOP5 추천 |
| 16:45~17:00 | intelligence_nxt_performance | NXT 성적표 |
| 16:45 | quant_market_brain | AI 시장판단 |
| 16:45 | quant_sector_flow | 섹터 수급 흐름 |
| 16:45 | quant_etf_fund_flow | ETF 투자자 수급 |
| 16:45 | quant_sector_momentum | 섹터 모멘텀 |
| 16:45 | quant_etf_recommendation | ETF 추천 |

---

## 7. 기존→신규 섹션 매핑 전체표

| 기존 섹션 | 이름 | → 신규 위치 | 비고 |
|----------|------|-----------|------|
| A | 달러-환율 모니터 | **2-A** 시장 지표 (통합) | C 글로벌지표와 합침 |
| B | BRAIN 판정 + 자산배분 | **1-A** BRAIN 커맨드바 | 히어로로 승격 |
| C-지표 | 글로벌 지표 6개 | **2-A** 시장 지표 (통합) | A FX모니터와 합침 |
| C-분석 | 5대 분석 | **2-B** 시장 분석 | 텍스트 요약 |
| D | 스윙 추천 + ETF | **1-B** [스윙] 탭 | 4탭 통합 |
| E | NXT Rationale 7지표 | **2-B** 접힘 서브섹션 | 기본 접힘 |
| F-1,2 | NXT 시그널 + TOP5 | **1-B** [NXT야간] 탭 | 4탭 통합 |
| F-3 | NXT 성적표 | **3-A** [NXT] 탭 | 접힘 |
| G-1 | 단타 TOP픽 | **1-B** [단타TOP] 탭 | 4탭 통합 |
| G-2 | 단타 성적표 | **3-A** [단타] 탭 | 접힘 |
| G-3 | TODAY vs NXT 비교 | **3-A** [단타] 탭 내부 | 접힘 |
| H | 3탭 퀀트 패널 | **3-B** 탭 1~3 | 접힘 |
| I | 수급 사이클 감지기 | **3-C** [수급사이클] 탭 | 접힘 |
| J | 기관 선매집 탐지 | **3-C** [기관선매집] 탭 | 접힘 |
| K | 매집 레이더 | **1-C** 스트립(Top3) + **3-C** [매집레이더] 탭 | 이중 노출 |
| 상한가 시그널 | triggered | **1-B** [상한가] 탭 | 4탭 통합 |
| 상한가 PT | Paper Trading 성적 | **3-A** [상한가Paper] 탭 | 접힘 |
| BRAIN Quant | quant_market_brain | **2-C** AI 시장판단 | 별도 카드 |
| 매집합류 | pension_scan | **1-C** 매집합류 스트립 | Top3~5만 |
| 퀀트 섹터수급 | quant_sector_flow | **3-B** [섹터 수급] 탭 | 접힘, lazy |
| 퀀트 ETF수급 | quant_etf_fund_flow | **3-B** [ETF 수급] 탭 | 접힘, lazy |
| 퀀트 모멘텀 | quant_sector_momentum | **3-B** [섹터 모멘텀] 탭 | 접힘, lazy |
| 퀀트 ETF추천 | quant_etf_recommendation | **1-B** [스윙] 탭 ETF 서브섹션 참고 | 통합 |

---

## 8. 체크리스트

### 필수 (P0) — TIER 1
- [ ] 1-A. BRAIN 커맨드바 (verdict + 점수 + 파이차트)
- [ ] 1-B. 4탭 통합 추천 패널 (스윙/단타/NXT/상한가)
- [ ] 1-B 탭 자동 선택 로직
- [ ] 1-C. 매집합류 Quick Strip
- [ ] CollapsibleSection 컴포넌트
- [ ] Tier 라벨 + 좌측 보더 시각적 구분
- [ ] 빈 데이터 안내 메시지 (전 패널)

### 높음 (P1) — TIER 2
- [ ] 2-A. 시장 지표 6칸 통합 카드 (FX + 글로벌)
- [ ] 2-B. 5대 분석 + NXT Rationale 접힘
- [ ] 2-C. AI 시장판단 (quant_market_brain)
- [ ] Wave 1/2 데이터 로딩 분리

### 중간 (P2) — TIER 3
- [ ] 3-A. 성적표 3탭 (단타/NXT/상한가Paper)
- [ ] 3-B. 퀀트 분석 6탭 (피보/대형주/섹터/수급/ETF/모멘텀)
- [ ] 3-C. 수급 심층 3탭 (사이클/선매집/레이더)
- [ ] 1-C → 3-C 더보기 연결 (자동 펼침 + 스크롤)
- [ ] 성적표 바 차트 + 누적선

### 낮음 (P3)
- [ ] 7일 히스토리 (BRAIN/자산배분 변화 추이)
- [ ] 반응형 최적화 (텔레그램 웹앱)
- [ ] 섹터 히트맵 Treemap (D3.js)
- [ ] 피보나치 게이지 바 (인라인)
- [ ] 섹터 상세 팝업

---

## 9. 참고: 사용자 설명 텍스트 모음 (UI에 삽입)

### 피보나치 눌림목
> 52주 고점 대비 크게 하락한 우량주를 피보나치 되돌림 기준으로 분류합니다.
> 깊이 빠진 종목일수록 반등 시 큰 수익이 가능하지만, 하락 이유를 반드시 확인하세요.

### 수급 사이클 감지기
> 외인·기관·개인·기타법인 4세력의 수급 흐름을 분석합니다.
> 급등임박 = 세력이 모았고, 개인이 바쳤고, 터질 준비 완료
> 매집 = 조용히 모으는 중, 아직 때가 아님
> 고점경고 = 세력은 빠지고 개인만 남음, 탈출 검토

### 기관 선매집 탐지
> 기관과 외국인이 조용히 매수하고 있지만, 주가는 아직 움직이지 않은 종목입니다.
> 뉴스나 촉매가 나오면 급등할 가능성이 높은 "장전된 스프링" 종목입니다.
> 쌍매수 = 기관+외인 동시 매수로, 가장 강력한 신호입니다.

### 매집 레이더
> 외국인이 3일 이상 조용히 매집 중이나 아직 주가가 안 오른 "미발화" 종목을 감지합니다.
> 가온전선 패턴(4일 매집 → +42%)의 초기 진입 시점을 포착합니다.

---

## 10. 변경 이력

- **2026-05-10** — v2 3-Tier 구조 전면 개편
  - 기존 A~K 11개 평면 나열 → **TIER 1(오늘의 핵심) / TIER 2(시장 현황) / TIER 3(심층 분석)** 3단 계층 구조
  - 1-B "오늘의 추천": 스윙(D) + 단타(G-1) + NXT(F-2) + 상한가 엔진을 **4탭 통합 패널**로 합침
  - 1-C "매집합류 포착": 매집레이더(K) + pension_scan의 TOP 3~5를 가로 Quick Strip으로 표시
  - 2-A "시장 지표": FX 모니터(A) + 글로벌 지표(C)를 **6칸 통합 카드 행**으로 합침
  - 2-B "시장 분석": 5대 분석(C) + NXT Rationale(E)를 통합, NXT Rationale **기본 접힘**
  - 2-C "AI 시장판단": quant_market_brain 별도 카드 신설
  - 3-A "성적표": 단타(G-2) + NXT(F-3) + 상한가 Paper Trading을 **3탭 접힘 컨테이너**로 통합
  - 3-B "퀀트 분석 도구": 기존 H 3탭 + 퀀트 대시보드 5테이블을 **6탭 접힘 컨테이너**로 통합
  - 3-C "수급 심층 분석": 수급사이클(I) + 기관선매집(J) + 매집레이더(K)를 **3탭 접힘 컨테이너**로 통합
  - Wave 1/2 데이터 로딩 전략: Tier 1+2 즉시 로드, Tier 3 lazy load
  - CollapsibleSection 컴포넌트 명세 추가
  - Tier별 시각적 구분 (좌측 보더: 초록/파랑/회색)

- **이전 버전** — v1 (A~K 평면 나열)

---

> **끝. 이 문서를 웹봇에 전달하고 "이거 보고 스윙시스템 페이지 구현해"라고 하면 됩니다.**
> **상한가 엔진의 상세 UI(분할매수/수급 카드 등)는 `FLOWX_LIMIT_UP_ENGINE_SPEC.md` 참조.**
