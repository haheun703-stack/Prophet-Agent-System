# FLOWX 데이터 시각화 스펙문서
## Version 1.0 | 2026.03.21 | 정보봇 → 웹봇 전달용

---

## 1. 프로젝트 개요

FlowX(flowx.kr) 대시보드에 데이터 시각화를 추가하여, 일반 개인 구독자가 **0.5초 안에 시장 상태를 직관적으로 파악**할 수 있게 한다.

### 현재 상태
- **완료**: 대시보드 8패널 UI, 시장요약 페이지, 섹터 히트맵(텍스트), 수급 X-Ray, 검색 기능
- **미완료**: 차트/다이어그램 시각화 (현재 숫자+텍스트만 표시)
- **데이터 파이프라인**: 정보봇/퀀트봇/단타봇 → Supabase → FlowX (주말 외 매일 자동 갱신)

### 기술 스택
- **프론트엔드**: Next.js (App Router) + TypeScript + Tailwind CSS
- **배포**: Vercel
- **DB**: Supabase (PostgreSQL)
- **차트 라이브러리**: Recharts (기본) + D3.js (고급)

---

## 2. 차트 라이브러리 전략

### 이중 스택 원칙
| 라이브러리 | 용도 | 설치 |
|-----------|------|------|
| **Recharts** | Line, Bar, Area, Pie/Donut, Radar, Gauge, Sparkline | `npm install recharts` |
| **D3.js** | Treemap, Sankey, Chord diagram | `npm install d3` |

### 왜 이중 스택인가
- Recharts: React 네이티브, 선언형, 빠른 개발. 기본 차트에 최적.
- D3.js: Treemap/Sankey/Chord는 Recharts로 표현 불가. D3만 보조로 사용.
- 절대 금지: Chart.js (React와 궁합 나쁨), Plotly (번들 사이즈 과다)

### 공통 디자인 토큰
```typescript
// lib/chart-tokens.ts
export const CHART_COLORS = {
  // 상승/하락
  up: '#E24B4A',      // 빨강 (한국 상승 컨벤션)
  down: '#378ADD',     // 파랑 (한국 하락 컨벤션)
  neutral: '#888780',  // 회색
  
  // 봇별 테마
  info: '#1D9E75',     // 정보봇 (teal)
  short: '#D85A30',    // 단타봇 (coral)  
  quant: '#7F77DD',    // 퀀트봇 (purple)
  
  // 섹터 히트맵 그라데이션
  heatmap: {
    strongUp: '#E24B4A',
    mildUp: '#F09595',
    flat: '#B4B2A9',
    mildDown: '#85B7EB',
    strongDown: '#378ADD',
  },
  
  // 차트 배경
  bg: 'transparent',
  grid: 'rgba(136, 135, 128, 0.15)',
  tooltip: 'rgba(44, 44, 42, 0.95)',
};

export const CHART_FONT = {
  family: "'Pretendard', -apple-system, sans-serif",
  size: { xs: 10, sm: 12, md: 14, lg: 16 },
};
```

---

## 3. 봇별 시각화 상세 스펙

---

### 3-A. 정보봇 (JGIS) — 6개 시각화

#### 3-A-1. 섹터 히트맵 (Treemap)
- **패널**: `/market` 페이지 메인, `/dashboard` 섹터 모멘텀
- **라이브러리**: D3.js (`d3-hierarchy`, `d3-scale`)
- **데이터 소스**: `sector_heatmap` 테이블
- **무료/유료**: 무료 (미끼용)

```typescript
// 데이터 스키마
interface SectorHeatmapData {
  sector: string;        // "반도체", "2차전지", "바이오"...
  change_pct: number;    // -5.2 ~ +8.1
  market_cap: number;    // 시총 합계 (사각형 크기 결정)
  top_stock: string;     // "삼성전자" (마우스오버 표시)
  top_stock_change: number;
}
```

```
구현 포인트:
- 사각형 크기 = 섹터 시총 합계 (비례)
- 사각형 색상 = 등락률 (-3%이하 진한파랑 ~ +3%이상 진한빨강)
- 마우스오버 → 상위 3종목 + 등락률 툴팁
- 클릭 → 해당 섹터 상세 페이지 이동
- 반응형: 모바일에서는 세로 스택 리스트로 폴백
```

**컴포넌트 파일**: `components/charts/SectorTreemap.tsx`

---

#### 3-A-2. 4축 게이지 (Gauge Chart)
- **패널**: `/dashboard` 장세 판단 영역
- **라이브러리**: Recharts (PieChart 기반 커스텀)
- **데이터 소스**: `market_briefing` 테이블의 `four_axis_score`
- **무료/유료**: 무료 (종합 점수만) / 유료 (4축 개별)

```typescript
interface FourAxisGauge {
  overall: 'AAA' | 'AA' | 'A' | 'BBB' | 'BB' | 'B' | 'CCC' | 'F';
  axes: {
    liquidity: number;    // 0~100 유동성
    sentiment: number;    // 0~100 심리
    supply: number;       // 0~100 수급
    technical: number;    // 0~100 기술적
  };
}
```

```
구현 포인트:
- 반원형 게이지 (180도)
- 바늘이 현재 점수 위치 가리킴
- 색상 구간: 0-30 파랑(위험) / 30-50 회색(중립) / 50-70 연두 / 70-100 빨강(과열)
- 등급 텍스트(AAA~F) 게이지 중앙에 크게 표시
- 무료: 종합 게이지 1개만 / 유료: 4축 각각 미니 게이지
```

**컴포넌트 파일**: `components/charts/GaugeChart.tsx`

---

#### 3-A-3. 외인/기관 수급 바 (Diverging Bar Chart)
- **패널**: `/market` 외인/기관 순매수 TOP 5
- **라이브러리**: Recharts (BarChart)
- **데이터 소스**: `sector_heatmap` + 종목별 수급 데이터
- **무료/유료**: 무료 (TOP 3) / 유료 (TOP 10 + 금액)

```typescript
interface SupplyDemandBar {
  name: string;           // 종목명
  foreign_net: number;    // 외인 순매수 (억원, 음수=매도)
  institution_net: number; // 기관 순매수
}
```

```
구현 포인트:
- 수평 바 차트, 0 기준선 좌우로 확장
- 왼쪽(매도) = 파랑, 오른쪽(매수) = 빨강
- 외인 바 + 기관 바 나란히 (grouped)
- 절대값 기준 정렬 (가장 큰 수급부터)
- 금액 라벨: 무료는 상대 크기만, 유료는 실제 억원 표시
```

**컴포넌트 파일**: `components/charts/DivergingBarChart.tsx`

---

#### 3-A-4. 시장 온도계 (Bullet Chart)
- **패널**: `/dashboard` 모닝 브리핑 영역
- **라이브러리**: Recharts (커스텀 BarChart)
- **데이터 소스**: `market_briefing` 테이블의 `fear_greed_index`
- **무료/유료**: 무료

```typescript
interface MarketThermometer {
  score: number;          // 0~100
  label: '극도공포' | '공포' | '중립' | '탐욕' | '극도탐욕';
  prev_score: number;     // 전일 점수 (비교용)
}
```

```
구현 포인트:
- 수평 바 1개, 배경에 5구간 색상 밴드
- 현재 위치에 마커(▼) 표시
- 전일 위치에 점선 마커 (변화 방향 인지)
- 라벨: "공포 32 → 탐욕 58 (+26)" 형태
```

**컴포넌트 파일**: `components/charts/BulletChart.tsx`

---

#### 3-A-5. 주요 지수 스파크라인 (Sparkline)
- **패널**: `/market` 상단, `/dashboard` 글로벌 티커 바
- **라이브러리**: Recharts (LineChart, 축 제거)
- **데이터 소스**: `market_briefing` 테이블의 `index_data`
- **무료/유료**: 무료

```typescript
interface SparklineData {
  index_name: string;     // "KOSPI", "KOSDAQ", "S&P500", "NASDAQ"
  current: number;
  change_pct: number;
  history_20d: number[];  // 최근 20거래일 종가 배열
}
```

```
구현 포인트:
- 높이 40px, 너비 120px의 미니 라인차트
- 축/그리드/라벨 전부 제거 (순수 라인만)
- 상승=빨강 라인, 하락=파랑 라인
- 마지막 점에 작은 원(dot) 표시
- 옆에 현재가 + 등락률 텍스트
```

**컴포넌트 파일**: `components/charts/Sparkline.tsx`

---

#### 3-A-6. 시장 종합 레이더 (Radar Chart)
- **패널**: `/dashboard` 장세 판단 하단
- **라이브러리**: Recharts (RadarChart)
- **데이터 소스**: `market_briefing` 테이블
- **무료/유료**: 유료 (SIGNAL 이상)

```typescript
interface MarketRadar {
  axes: {
    name: string;    // "유동성", "심리", "수급", "기술적", "밸류", "모멘텀"
    value: number;   // 0~100
    prev: number;    // 전일 (겹쳐서 변화 표시)
  }[];
}
```

```
구현 포인트:
- 6축 레이더
- 오늘 = 실선 + 면적 채우기 (teal, opacity 0.3)
- 전일 = 점선 (비교용)
- 각 꼭짓점에 점수 표시
```

**컴포넌트 파일**: `components/charts/MarketRadar.tsx`

---

### 3-B. 단타봇 — 4개 시각화

#### 3-B-1. 세력 포착 버블 차트 (Bubble Chart)
- **패널**: `/dashboard` 세력 포착
- **라이브러리**: Recharts (ScatterChart)
- **데이터 소스**: `short_signals` 테이블
- **무료/유료**: 유료 (SIGNAL 이상)

```typescript
interface BubbleData {
  name: string;           // 종목명
  volume_ratio: number;   // 평균 대비 거래량 배율 (x축)
  score: number;          // 종합 점수 (y축)
  market_cap: number;     // 시총 (버블 크기)
  grade: 'S' | 'A' | 'B' | 'C';
}
```

```
구현 포인트:
- x축: 거래량 배율 (1x ~ 10x+)
- y축: 종합 점수 (0~100)
- 버블 크기: 시총 비례
- 버블 색상: S=빨강, A=주황, B=회색, C=연회색
- 우상단 = "강력 매수 시그널" 영역 표시 (배경 하이라이트)
- 마우스오버 → 종목명, 점수, 외인/기관 수급, 진입가, 손절가
```

**컴포넌트 파일**: `components/charts/BubbleChart.tsx`

---

#### 3-B-2. 국가별 자금 흐름 (Sankey Diagram)
- **패널**: `/dashboard` 중국자금 흐름 (확장)
- **라이브러리**: D3.js (`d3-sankey`)
- **데이터 소스**: `short_signals` + 국가별 외인 수급
- **무료/유료**: 유료 (PRO 이상)

```typescript
interface SankeyFlowData {
  nodes: { id: string; label: string }[];  // "미국", "유럽", "중국", "반도체", "2차전지"...
  links: { source: string; target: string; value: number }[];  // 순매수 금액
}
```

```
구현 포인트:
- 왼쪽 노드: 국가 (미국, 유럽, 중국, 기타)
- 오른쪽 노드: 섹터 (반도체, 2차전지, 바이오...)
- 링크 두께: 순매수 금액 비례
- 링크 색상: 순매수=빨강, 순매도=파랑
- 노드 클릭 → 해당 국가/섹터 상세
```

**컴포넌트 파일**: `components/charts/SankeyFlow.tsx`

---

#### 3-B-3. AI 추천 랭킹 리스트 (Ranked List)
- **패널**: `/dashboard` AI 추천
- **라이브러리**: Recharts (BarChart 가로) + 커스텀 CSS
- **데이터 소스**: `short_signals` 테이블
- **무료/유료**: 무료 (TOP 3, 점수만) / 유료 (TOP 10 + 상세)

```typescript
interface RankedItem {
  rank: number;
  name: string;
  ticker: string;
  score: number;          // 0~100
  grade: 'S' | 'A' | 'B' | 'C';
  entry_price?: number;   // 유료만
  stop_loss?: number;     // 유료만
  target_price?: number;  // 유료만
  foreign_net?: string;   // "미국 +120억" 유료만
}
```

```
구현 포인트:
- 수평 바: 점수 비례 (0~100)
- 등급 뱃지: S=빨강, A=주황, B=초록, C=회색
- 무료: 종목명 + 점수 바만 / 유료: 진입가/손절가/목표가 표시
- 리스트 하단에 "SIGNAL 구독하면 전체 목록 확인" CTA
```

**컴포넌트 파일**: `components/charts/RankedList.tsx`

---

#### 3-B-4. 스나이퍼 워치 타임라인 (Timeline)
- **패널**: `/dashboard` 스나이퍼 워치
- **라이브러리**: Recharts (AreaChart) + 커스텀 마커
- **데이터 소스**: `short_signals` 테이블
- **무료/유료**: 유료 (VIP)

```typescript
interface SniperTimeline {
  name: string;
  entry_date: string;
  entry_price: number;
  current_price: number;
  target_date: string;    // 예상 D+3~5
  target_price: number;
  stop_loss: number;
  status: 'active' | 'profit' | 'loss' | 'pending';
}
```

```
구현 포인트:
- 5일 캔들 미니차트 + 진입/목표/손절 수평선
- 진입점(▲) 녹색 마커, 목표(★) 빨강, 손절(✕) 파랑
- 현재 포지션 하이라이트 (수익=초록 영역, 손실=빨강 영역)
- 카드형으로 종목별 나열 (최대 5개)
```

**컴포넌트 파일**: `components/charts/SniperTimeline.tsx`

---

### 3-C. 퀀트봇 — 4개 시각화

#### 3-C-1. ETF 포트폴리오 도넛 (Donut Chart)
- **패널**: `/dashboard` ETF 시그널
- **라이브러리**: Recharts (PieChart, innerRadius)
- **데이터 소스**: `etf_signals` 테이블
- **무료/유료**: 유료 (PRO 이상)

```typescript
interface PortfolioDonut {
  sectors: {
    name: string;       // "반도체 ETF", "2차전지 ETF"...
    weight: number;     // 0~100%
    change_pct: number;
    etf_ticker: string;
  }[];
  total_return: number;  // 총 수익률
}
```

```
구현 포인트:
- 도넛 중앙: 총 수익률 숫자 (크게)
- 섹터별 색상 구분 (최대 8색)
- 마우스오버 → 섹터명, 비중%, ETF 티커, 수익률
- 도넛 바깥에 범례 (오른쪽)
```

**컴포넌트 파일**: `components/charts/PortfolioDonut.tsx`

---

#### 3-C-2. 누적 수익률 라인 (Line Chart)
- **패널**: `/dashboard` 퀀트봇 성과 (Month 3 이후)
- **라이브러리**: Recharts (AreaChart)
- **데이터 소스**: `paper_trades` 테이블 (누적 집계)
- **무료/유료**: 무료 (신뢰 구축용)

```typescript
interface PerformanceLine {
  dates: string[];        // "2026-01-02", "2026-01-03"...
  cumulative_return: number[];  // 누적 수익률 %
  benchmark: number[];    // KOSPI 벤치마크
  drawdown: number[];     // MDD 음수 값
  stats: {
    total_return: number;
    win_rate: number;
    profit_factor: number;
    max_drawdown: number;
  };
}
```

```
구현 포인트:
- 메인 라인: 퀀트봇 누적수익률 (teal, 면적 채우기)
- 서브 라인: KOSPI 벤치마크 (회색 점선)
- 하단 서브차트: Drawdown 바 (빨강, 아래 방향)
- 우측 상단: PF, 승률, MDD 텍스트 뱃지
- 이 차트가 "진짜 돈 버는 시스템" 증명의 핵심
```

**컴포넌트 파일**: `components/charts/PerformanceLine.tsx`

---

#### 3-C-3. 섹터 로테이션 코드 (Chord Diagram)
- **패널**: `/dashboard` 섹터 모멘텀
- **라이브러리**: D3.js (`d3-chord`)
- **데이터 소스**: `etf_signals` 테이블 (섹터 간 자금이동)
- **무료/유료**: 유료 (PRO 이상)

```typescript
interface ChordData {
  sectors: string[];      // ["반도체", "2차전지", "바이오", "금융", "에너지"]
  matrix: number[][];     // 섹터 간 자금이동 매트릭스
}
```

```
구현 포인트:
- 원형 레이아웃: 각 섹터 = 호(arc)
- 호의 크기: 해당 섹터 전체 자금 흐름 비례
- 리본(ribbon): 섹터 A에서 B로 이동한 자금
- 리본 색상: 유출 섹터 색상 상속
- 마우스오버 → 특정 섹터 관련 리본만 하이라이트
- ⚠️ 구현 난이도 높음 — Phase C에서 진행
```

**컴포넌트 파일**: `components/charts/SectorChord.tsx`

---

#### 3-C-4. 중국자금 흐름 추이 (Stacked Area Chart)
- **패널**: `/dashboard` 중국자금 흐름
- **라이브러리**: Recharts (AreaChart, stackId)
- **데이터 소스**: `china_flow` 테이블
- **무료/유료**: 유료 (PRO 이상)

```typescript
interface ChinaFlowData {
  dates: string[];
  northbound: number[];   // 북향 자금 (억위안)
  southbound: number[];   // 남향 자금
  net: number[];          // 순유입
  sectors: {
    name: string;
    values: number[];     // 시간별 섹터 유입량
  }[];
}
```

```
구현 포인트:
- 메인: 섹터별 스택 영역 차트 (최대 5개 섹터)
- 서브 라인: 순유입 총액 (점선)
- x축: 최근 20거래일
- 양수=위쪽(유입), 음수=아래쪽(유출)
- 색상: 섹터별 고유 색상
```

**컴포넌트 파일**: `components/charts/ChinaFlowArea.tsx`

---

## 4. 구현 우선순위 (Phase별)

### Phase A — Week 1~2 (정보봇 + 섹터맵 듀얼뷰, 무료 미끼)
| 순서 | 컴포넌트 | 난이도 | 임팩트 |
|------|----------|--------|--------|
| 1 | `SectorSwimlane.tsx` | ★★☆ | ⭐ 킬러 피처 View A |
| 2 | `SectorNetwork.tsx` | ★★★ | ⭐ 킬러 피처 View B (Canvas) |
| 3 | `SectorMapView.tsx` | ★☆☆ | 듀얼 뷰 래퍼/토글 |
| 4 | `Sparkline.tsx` | ★☆☆ | 글로벌 티커에 즉시 적용 |
| 5 | `BulletChart.tsx` | ★☆☆ | 모닝 브리핑 시각화 |
| 6 | `DivergingBarChart.tsx` | ★★☆ | 수급 데이터 직관화 |
| 7 | `GaugeChart.tsx` | ★★☆ | 장세 판단 핵심 |
| 8 | `SectorTreemap.tsx` | ★★★ | D3 필요, 메인 비주얼 |

### Phase B — Week 3~4 (단타봇, 유료 전환)
| 순서 | 컴포넌트 | 난이도 | 임팩트 |
|------|----------|--------|--------|
| 6 | `RankedList.tsx` | ★☆☆ | AI 추천 시각화 |
| 7 | `BubbleChart.tsx` | ★★☆ | 세력 포착 직관화 |
| 8 | `SniperTimeline.tsx` | ★★☆ | 타임라인 시각화 |
| 9 | `SankeyFlow.tsx` | ★★★ | D3 필요, 자금 흐름 |

### Phase C — Month 2~3 (퀀트봇, 신뢰 구축)
| 순서 | 컴포넌트 | 난이도 | 임팩트 |
|------|----------|--------|--------|
| 10 | `PerformanceLine.tsx` | ★★☆ | 수익률 증명 (핵심) |
| 11 | `PortfolioDonut.tsx` | ★☆☆ | ETF 비중 표시 |
| 12 | `ChinaFlowArea.tsx` | ★★☆ | 중국자금 추이 |
| 13 | `SectorChord.tsx` | ★★★ | D3 고급, 마지막 |
| 14 | `MarketRadar.tsx` | ★★☆ | 종합 레이더 |

---

## 5. 파일 구조

```
src/
├── components/
│   └── charts/
│       ├── index.ts                 // 전체 export
│       ├── chart-tokens.ts          // 색상/폰트/티어 토큰
│       ├── ChartContainer.tsx       // 공통 래퍼 (로딩/에러/반응형/블러)
│       │
│       ├── // === Phase A (정보봇 + 섹터맵) ===
│       ├── SectorMapView.tsx        // ⭐ 듀얼 뷰 래퍼 (토글 UI)
│       ├── SectorSwimlane.tsx       // ⭐ View A: 스윔레인+커넥션
│       ├── SectorNetwork.tsx        // ⭐ View B: 네트워크 그래프 (PRO)
│       ├── Sparkline.tsx
│       ├── BulletChart.tsx
│       ├── DivergingBarChart.tsx
│       ├── GaugeChart.tsx
│       ├── SectorTreemap.tsx        // D3
│       ├── MarketRadar.tsx
│       │
│       ├── // === Phase B (단타봇) ===
│       ├── RankedList.tsx
│       ├── BubbleChart.tsx
│       ├── SniperTimeline.tsx
│       ├── SankeyFlow.tsx           // D3
│       │
│       └── // === Phase C (퀀트봇) ===
│           ├── PerformanceLine.tsx
│           ├── PortfolioDonut.tsx
│           ├── ChinaFlowArea.tsx
│           └── SectorChord.tsx      // D3
│
├── hooks/
│   ├── useChartData.ts             // Supabase 실시간 구독 훅
│   └── useSectorData.ts            // 섹터 데이터 + links 훅
│
├── lib/
│   └── chart-tokens.ts
│
└── app/
    └── sectors/
        ├── page.tsx                 // 섹터 그리드 (13개 카드)
        └── [key]/
            └── page.tsx             // 듀얼 뷰 (스윔레인+네트워크)
```

---

## 6. 무료/유료 분기 로직

```typescript
// components/charts/ChartContainer.tsx
interface ChartContainerProps {
  tier: 'free' | 'signal' | 'pro' | 'vip';
  requiredTier: 'free' | 'signal' | 'pro' | 'vip';
  children: React.ReactNode;
}

// 사용 예시:
<ChartContainer tier={userTier} requiredTier="signal">
  <BubbleChart data={data} />
</ChartContainer>

// requiredTier 미달 시 → 블러 + "SIGNAL 구독으로 해금" 오버레이
```

### 티어별 해금 차트
| 차트 | FREE | SIGNAL ₩9,900 | PRO ₩25,000 | VIP ₩50,000 |
|------|------|---------------|-------------|-------------|
| SectorSwimlane (5★~2★) | 이름만 | ✅ | ✅ | ✅ |
| SectorSwimlane (1★) | ❌ | ❌ | ✅ | ✅ |
| SectorSwimlane 커넥션 | 1단계만 | ✅ | ✅ | ✅ |
| SectorNetwork (PRO뷰) | ❌ 🔒 | ❌ 🔒 | ✅ | ✅ |
| Sparkline | ✅ | ✅ | ✅ | ✅ |
| BulletChart | ✅ | ✅ | ✅ | ✅ |
| DivergingBar (TOP3) | ✅ | ✅ | ✅ | ✅ |
| DivergingBar (TOP10) | ❌ | ✅ | ✅ | ✅ |
| GaugeChart (종합) | ✅ | ✅ | ✅ | ✅ |
| GaugeChart (4축) | ❌ | ✅ | ✅ | ✅ |
| SectorTreemap | ✅ | ✅ | ✅ | ✅ |
| MarketRadar | ❌ | ✅ | ✅ | ✅ |
| RankedList (TOP3) | ✅ | ✅ | ✅ | ✅ |
| RankedList (전체) | ❌ | ✅ | ✅ | ✅ |
| BubbleChart | ❌ | ✅ | ✅ | ✅ |
| SankeyFlow | ❌ | ❌ | ✅ | ✅ |
| PortfolioDonut | ❌ | ❌ | ✅ | ✅ |
| PerformanceLine | ✅ | ✅ | ✅ | ✅ |
| ChinaFlowArea | ❌ | ❌ | ✅ | ✅ |
| SectorChord | ❌ | ❌ | ✅ | ✅ |
| SniperTimeline | ❌ | ❌ | ❌ | ✅ |

---

## 7. Supabase 연동

```typescript
// hooks/useChartData.ts
import { createClient } from '@supabase/supabase-js';

const supabase = createClient(
  process.env.NEXT_PUBLIC_SUPABASE_URL!,
  process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY!
);

// 실시간 구독 (장중 자동 갱신)
export function useRealtimeChart(table: string) {
  const [data, setData] = useState(null);
  
  useEffect(() => {
    // 초기 로드
    supabase.from(table).select('*').order('created_at', { ascending: false }).limit(1)
      .then(({ data }) => setData(data?.[0]));
    
    // 실시간 구독
    const channel = supabase
      .channel(`${table}_changes`)
      .on('postgres_changes', { event: 'INSERT', schema: 'public', table }, 
        (payload) => setData(payload.new))
      .subscribe();
    
    return () => { supabase.removeChannel(channel); };
  }, [table]);
  
  return data;
}
```

---

## 8. 섹터 공급망 시각화 — DUAL VIEW (KILLER FEATURE)

### 개요
FlowX의 킬러 피처. 트레이딩뷰에 없는 기능.
**2가지 뷰를 유저 티어에 따라 제공:**
- **View A: 스윔레인 플로우차트 + 커넥션** → FREE/SIGNAL 기본 뷰
- **View B: 네트워크 그래프** → PRO/VIP 전용 고급 뷰

"NVIDIA가 올랐으니 SK하이닉스도 오를까?"의 판단을 한눈에.
개별 기업 간 실제 공급망 관계(HBM 납품, 장비 공급, 파운드리 등)까지 시각화.

> 참고: datavizproject.com의 "스윔레인 플로우차트" + "네트워크 다이어그램" 유형 기반

### 섹터 목록 (13개)
1. 반도체  2. 조선  3. 방산  4. 건설  5. 바이오
6. 금융  7. 자동차  8. 로봇  9. 에너지  10. 게임
11. 엔터  12. 유통  13. 식품

### 데이터 소스
- **초기 시드**: `FLOWX_SECTOR_UNIVERSE.json` (13섹터 259종목)
- **공급망 연결 데이터**: `links` 배열 (from → to + 관계 라벨)
- **일일 갱신**: 정보봇이 매일 장마감 후 change_pct, volume_ratio, foreign_net 업데이트
- **Supabase 테이블**: `sector_universe` + `sector_links` (스키마 하단 참조)

### 공통 데이터 인터페이스

```typescript
// === 종목 노드 ===
interface StockNode {
  name: string;
  ticker: string;
  market: 'US' | 'KR' | 'EU' | 'JP' | 'CN' | 'UK';
  change_pct: number;
  volume_ratio: number;
  foreign_net: number;
  institution_net: number;
}

// === 티어 데이터 ===
interface TierData {
  tier: 1 | 2 | 3 | 4 | 5;
  label: string;
  sub: string;
  stocks: StockNode[];
}

// === 공급망 연결 (⭐ 핵심 데이터) ===
interface SupplyLink {
  from: string;        // 종목명 (예: "NVIDIA")
  to: string[];        // 연결 대상들 (예: ["TSMC","SK하이닉스","한미반도체"])
  rel: string;         // 관계 라벨 (예: "HBM/파운드리")
}

// === 섹터 전체 ===
interface SectorData {
  key: string;
  name: string;
  tiers: TierData[];
  links: SupplyLink[];  // ⭐ 개별 기업 간 연결 관계
}
```

### 티어별 색상 토큰

```typescript
export const TIER_COLORS = {
  5: { bg: '#EEEDFE', border: '#AFA9EC', text: '#3C3489', badge: '#534AB7' },
  4: { bg: '#E6F1FB', border: '#85B7EB', text: '#0C447C', badge: '#185FA5' },
  3: { bg: '#E1F5EE', border: '#5DCAA5', text: '#085041', badge: '#0F6E56' },
  2: { bg: '#FAEEDA', border: '#FAC775', text: '#633806', badge: '#854F0B' },
  1: { bg: '#FAECE7', border: '#F0997B', text: '#712B13', badge: '#993C1D' },
};

export const CONNECTION_COLOR = '#7F77DD';  // 보라 — 커넥션 라인 색상
```

---

### 8-A. 스윔레인 플로우차트 + 커넥션 (기본 뷰)

**파일**: `components/charts/SectorSwimlane.tsx`
**대상**: FREE / SIGNAL / PRO / VIP (모든 유저)
**라이브러리**: 순수 CSS + HTML (D3 불필요)

**핵심 레이아웃 — 스윔레인 플로우차트:**
```
┌──────────────────────────────────────────────────────┐
│ [반도체 | 방산 | 조선 | 바이오 | ...] 섹터 탭         │
│                                                       │
│ ┌─────┬──────────────────────────────────────────┐   │
│ │5★   │ [SOXX] [SMH]                             │   │
│ │ETF  │                                           │   │
│ ├─────┼───────────── ▼ benchmark ────────────────┤   │
│ │4★   │ [NVIDIA] [TSMC] [AMD] [ASML] [Broadcom] │   │
│ │Glbl │ [Micron] [Lam Res.] [App.Mat.] ...       │   │
│ ├─────┼───────────── ▼ supply chain ─────────────┤   │
│ │3★   │ [Marvell] [ON Semi] [Entegris] ...       │   │
│ │Supp │                                           │   │
│ ├─────┼───────────── ▼ 한국시장 ─────────────────┤   │
│ │2★KR │ [SK하이닉스] [삼성전자] [DB하이텍]        │   │
│ │대형 │                                           │   │
│ ├─────┼───────────── ▼ 소부장 ───────────────────┤   │
│ │1★KR │ [주성엔지] [한미반도체] [원익IPS] ...     │   │
│ │소부장│                                           │   │
│ └─────┴──────────────────────────────────────────┘   │
│                                                       │
│  💡 종목 클릭 시 → 보라색 커넥션 라인 + 관계 뱃지     │
└──────────────────────────────────────────────────────┘
```

**구현 포인트:**

1. **레인 구조**
   - 왼쪽: 레인 라벨 영역 (width: 100px, 별점+이름+설명+종목수)
   - 오른쪽: 레인 바디 (flex-wrap, 종목 카드들)
   - 레인 배경: 티어별 고유색 (투명도 20~30%)
   - 레인 사이: ▼ 화살표 + 흐름 라벨 ("↓ supply chain", "↓ 한국시장")

2. **종목 카드** (스윔레인 내 노드)
   - 배경: 티어 고유 bg + 반투명 흰색 overlay
   - 종목명: font-weight 500, 10px, 티어 text 색상
   - 티커: font-mono, 8px, opacity 0.55
   - 등락률: 상승 #E24B4A(빨강), 하락 #378ADD(파랑), 9px bold
   - 호버: translateY(-1px), 미세 box-shadow
   - min-width: 58px, padding: 4px 7px

3. **⭐ 커넥션 인터랙션 (핵심 차별화)**
   - 종목 클릭 → SVG overlay에 **보라색 베지에 곡선** 그리기
   - 연결된 종목만 하이라이트 (나머지 dim opacity 0.12)
   - 연결된 종목 카드 위에 **관계 뱃지** 표시 ("HBM 납품", "장비 공급")
   - 뱃지: bg #7F77DD, color white, font-size 7px, border-radius 6px
   - 빈 공간 클릭 → 하이라이트 해제, 전체 복원
   - SVG 커넥션 라인: stroke #7F77DD, dasharray 4 3, opacity 0.7
   - 선택된 종목: 시작점에 원형 dot (r=4, fill #534AB7)
   - 연결 대상: 끝점에 원형 dot (r=3, fill #7F77DD)

4. **커넥션 라인 계산 로직**
```typescript
// SVG overlay에서 베지에 곡선 그리기
function drawConnection(fromEl: HTMLElement, toEl: HTMLElement, container: HTMLElement) {
  const rect = container.getBoundingClientRect();
  const fromR = fromEl.getBoundingClientRect();
  const toR = toEl.getBoundingClientRect();
  
  const sx = fromR.left - rect.left + fromR.width / 2;
  const sy = fromR.top - rect.top + fromR.height / 2;
  const ex = toR.left - rect.left + toR.width / 2;
  const ey = toR.top - rect.top + toR.height / 2;
  const midY = (sy + ey) / 2;
  
  return `M${sx} ${sy} C${sx} ${midY} ${ex} ${midY} ${ex} ${ey}`;
}
```

**반도체 섹터 연결 데이터 예시 (links 배열):**
```json
[
  {"from": "SOXX", "to": ["NVIDIA","TSMC","AMD","Intel","Broadcom"], "rel": "ETF 구성"},
  {"from": "NVIDIA", "to": ["TSMC","SK하이닉스","한미반도체"], "rel": "HBM/파운드리"},
  {"from": "TSMC", "to": ["ASML","App.Mat.","Lam Res.","주성엔지"], "rel": "장비 공급"},
  {"from": "ASML", "to": ["Tokyo El.","한화비전"], "rel": "광학/노광"},
  {"from": "AMD", "to": ["TSMC","SK하이닉스"], "rel": "파운드리/HBM"},
  {"from": "App.Mat.", "to": ["주성엔지","원익IPS","유진테크"], "rel": "장비 공급"},
  {"from": "Lam Res.", "to": ["주성엔지","피에스케이"], "rel": "식각/세정"},
  {"from": "KLA", "to": ["리노공업","ISC"], "rel": "검사/테스트"},
  {"from": "SK하이닉스", "to": ["한미반도체","원익IPS","유진테크","테크윙"], "rel": "장비/패키징"},
  {"from": "삼성전자", "to": ["주성엔지","한미반도체","피에스케이","ISC"], "rel": "장비/패키징"},
  {"from": "DB하이텍", "to": ["주성엔지","유진테크"], "rel": "파운드리"},
  {"from": "리노공업", "to": ["ISC","테크윙"], "rel": "테스트 소켓"}
]
```

---

### 8-B. 네트워크 그래프 (PRO/VIP 전용 뷰)

**파일**: `components/charts/SectorNetwork.tsx`
**대상**: PRO / VIP 전용
**라이브러리**: Canvas 2D (D3 불필요, 순수 Canvas API)

**핵심 레이아웃 — 자유 배치 네트워크:**
```
┌─────────────────────────────────────────────┐
│                                              │
│        ○SOXX  ○SMH                          │
│       ╱   ╲                                  │
│    ◉NVIDIA ──── ◉TSMC ──── ○ASML           │
│     ╱│╲          │╲         │               │
│  ○AMD │  ○Micron │ ◉삼성전자 │              │
│       │          │          │               │
│     ◉SK하이닉스  ○App.Mat. ○한화비전        │
│      ╱│╲         │                          │
│  ○한미 ○원익 ○유진  ○주성엔지              │
│                                              │
│  노드 크기 = 연결 수 (영향력)               │
│  드래그로 노드 이동 가능                     │
│  호버 시 연결된 종목만 하이라이트            │
└─────────────────────────────────────────────┘
```

**구현 포인트:**

1. **Canvas 렌더링 (requestAnimationFrame 루프)**
   - 배경: 투명 (호스트 UI 배경 사용)
   - 60fps 렌더링 루프
   - Canvas 크기: width=컨테이너폭, height=480px
   - Retina 대응: canvas.width = offsetWidth * 2, ctx.scale(2,2)

2. **노드 (기업)**
   - 원형, 크기 = max(8, 연결수 × 3 + 6)px radius
   - 외곽선: 티어 색상 (TIER_COLORS[tier].badge)
   - 채우기: 티어 배경색 (TIER_COLORS[tier].bg)
   - 텍스트: 종목명, 노드 중앙에 표시
   - 작은 노드(r<16): font-size 8px, 큰 노드: 10px
   - 허브 노드(연결 5개+): 외곽선 두께 2.5px, 나머지 1.5px

3. **엣지 (연결선)**
   - 기본 상태: 회색(#bbb), 0.7px, opacity 0.25, 곡선(quadraticCurveTo)
   - 호버 상태: 연결된 엣지만 보라색(#7F77DD), 1.8px, opacity 0.8
   - 비연결 엣지: 회색, 0.5px, opacity 0.15

4. **호버 인터랙션**
   - 마우스 위치에서 가장 가까운 노드 탐색
   - 해당 노드 + 연결 노드만 opacity 1.0
   - 나머지 노드: opacity 0.12 (dim)
   - 연결 엣지만 보라색 하이라이트
   - 호버 노드 위에 툴팁: "NVIDIA (8 connections)"

5. **드래그 인터랙션**
   - mousedown on node → 드래그 시작
   - mousemove → 노드 위치 업데이트 (실시간)
   - mouseup → 드래그 종료
   - cursor: grab(기본), pointer(노드 위), grabbing(드래그 중)

6. **클릭 인터랙션**
   - 노드 클릭 → sendPrompt('종목명 종목 분석해줘')

7. **다크모드 대응**
```typescript
const dark = matchMedia('(prefers-color-scheme: dark)').matches;
const textColor = dark ? '#c2c0b6' : '#2c2c2a';
const edgeDefault = dark ? '#666' : '#bbb';
const dimEdge = dark ? '#444' : '#ddd';
```

8. **초기 노드 배치**
   - 5★: 좌상단 (x: 60~120, y: 40~120)
   - 4★: 중앙 상단 (x: 180~500, y: 40~210)
   - 3★: 우상단 (x: 480~600, y: 40~200)
   - 2★: 중앙 하단 (x: 250~550, y: 260~340)
   - 1★: 하단 (x: 120~620, y: 340~450)
   - 드래그로 유저가 자유롭게 재배치 가능

---

### 8-C. 듀얼 뷰 전환 UI

**파일**: `components/charts/SectorMapView.tsx` (래퍼 컴포넌트)

```typescript
interface SectorMapViewProps {
  sectorKey: string;
  userTier: 'free' | 'signal' | 'pro' | 'vip';
}

export default function SectorMapView({ sectorKey, userTier }: SectorMapViewProps) {
  const [view, setView] = useState<'swimlane' | 'network'>('swimlane');
  const canNetwork = userTier === 'pro' || userTier === 'vip';
  
  return (
    <>
      {/* 뷰 전환 토글 */}
      <div className="flex gap-2 mb-3">
        <button 
          onClick={() => setView('swimlane')}
          className={view === 'swimlane' ? 'active' : ''}
        >
          Flowchart
        </button>
        <button 
          onClick={() => setView('network')}
          className={canNetwork ? '' : 'locked'}
          disabled={!canNetwork}
        >
          Network {!canNetwork && '🔒 PRO'}
        </button>
      </div>
      
      {/* 뷰 렌더링 */}
      {view === 'swimlane' ? (
        <SectorSwimlane sectorKey={sectorKey} userTier={userTier} />
      ) : (
        <SectorNetwork sectorKey={sectorKey} />
      )}
    </>
  );
}
```

**뷰 전환 버튼 스타일:**
- 활성: bg secondary, border-primary, font-weight 500
- 비활성: bg transparent, border-tertiary
- 잠금(PRO 미달): opacity 0.5, cursor not-allowed, 🔒 아이콘 + "PRO" 뱃지

---

### 8-D. 무료/유료 분기

| 기능 | FREE (₩0) | SIGNAL (₩9,900) | PRO (₩25,000) | VIP (₩50,000) |
|------|-----------|-----------------|---------------|---------------|
| **스윔레인 뷰** | ✅ | ✅ | ✅ | ✅ |
| 5★ ETF | ✅ 전체 | ✅ | ✅ | ✅ |
| 4★ Global | 이름만 (등락률 블러) | ✅ 전체 | ✅ | ✅ |
| 3★ Suppliers | ❌ 블러 + CTA | ✅ 전체 | ✅ | ✅ |
| 2★ KR Majors | 이름만 (등락률 블러) | ✅ 전체 | ✅ | ✅ |
| 1★ KR Parts | ❌ 블러 + CTA | ❌ 블러 + CTA | ✅ 전체 | ✅ |
| 커넥션 클릭 | 1단계만 (from→to) | ✅ 전체 | ✅ 전체 | ✅ 전체 |
| **네트워크 뷰** | ❌ 🔒 | ❌ 🔒 | ✅ | ✅ |
| 노드 드래그 | - | - | ✅ | ✅ |
| 수급 데이터 | ❌ | 등락률만 | 전체 | 전체+푸시알림 |

**블러 처리:**
```typescript
<div style={{ filter: 'blur(6px)', pointerEvents: 'none' }}>
  {/* 잠긴 티어의 종목 카드들 */}
</div>
<div className="absolute inset-0 flex items-center justify-center">
  <button className="bg-purple-600 text-white px-4 py-2 rounded-lg">
    🔓 SIGNAL 구독으로 해금
  </button>
</div>
```

---

### 8-E. Supabase 테이블: sector_links

```sql
CREATE TABLE sector_links (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  sector_key text NOT NULL,
  from_stock text NOT NULL,        -- "NVIDIA"
  to_stock text NOT NULL,          -- "SK하이닉스"
  relation text NOT NULL,          -- "HBM/파운드리"
  strength integer DEFAULT 1,       -- 관계 강도 (1~5, 선 굵기에 반영)
  updated_at timestamptz DEFAULT now()
);

CREATE INDEX idx_links_sector ON sector_links(sector_key);
CREATE INDEX idx_links_from ON sector_links(from_stock);
```

---

### 8-F. 페이지 배치
- `/dashboard` → 사이드바에 "섹터맵" 메뉴 추가
- `/sectors` → 13개 섹터 그리드 (카드형, 각 카드에 미니 스윔레인 프리뷰)
- `/sectors/[key]` → 듀얼 뷰 전체 (스윔레인 기본 + 네트워크 토글)
- `/market` → 섹터 히트맵 Treemap 클릭 시 해당 `/sectors/[key]`로 연결

### 8-G. 반응형 전략
| 뷰포트 | 스윔레인 | 네트워크 |
|--------|---------|---------|
| Desktop (1024px+) | 풀 레인, 커넥션 SVG | Canvas 풀 사이즈 |
| Tablet (768~1023px) | 카드 축소 (min-width 54px) | Canvas height 400px |
| Mobile (< 768px) | 가로 스크롤, 드래그 탐색 | 세로 스크롤, 드래그 비활성 |

### 8-H. 성능 최적화
- 13섹터 259종목이지만 한 번에 1섹터만 렌더링 (탭 전환)
- 스윔레인: React.memo로 종목 카드 리렌더 방지
- 네트워크: Canvas requestAnimationFrame (DOM 조작 없음, 가장 빠름)
- Supabase 실시간 구독: 활성 섹터만 subscribe
- 커넥션 SVG: 클릭 시에만 생성, 해제 시 remove

---

## 9. 참고 레퍼런스

- **DataVizProject**: https://datavizproject.com — 160+ 시각화 유형 카탈로그
- **Recharts 문서**: https://recharts.org — React 차트 공식 문서
- **D3 Treemap**: https://d3js.org/d3-hierarchy/treemap
- **D3 Sankey**: https://github.com/d3/d3-sankey
- **D3 Chord**: https://d3js.org/d3-chord
- **섹터 유니버스 시드**: `FLOWX_SECTOR_UNIVERSE.json` (함께 전달)

---

## 10. 웹봇 작업 지시

> 이 문서를 받은 웹봇(Claude Code)은 Phase A부터 순서대로 작업한다.
> 
> **섹터맵 듀얼뷰가 최우선 — 아래 순서로 진행:**
> 1. `SectorSwimlane.tsx` — 스윔레인 레인 레이아웃 + 종목 카드
> 2. 스윔레인에 커넥션 기능 추가 — SVG overlay + 클릭 인터랙션
> 3. `SectorNetwork.tsx` — Canvas 네트워크 그래프 (PRO 전용)
> 4. `SectorMapView.tsx` — 듀얼 뷰 토글 래퍼 + 유저 티어 분기
> 5. `sector_links` 테이블 생성 + 시드 INSERT 스크립트
> 
> **공급망 연결 데이터(links)가 핵심.** 
> `FLOWX_SECTOR_UNIVERSE.json`의 종목 데이터 + 별도 links 배열 모두 Supabase에 넣기.
> links는 `sector_links` 테이블에 from_stock/to_stock/relation으로 저장.
> 
> 각 컴포넌트는 독립적으로 동작하며, `ChartContainer`로 감싸서 무료/유료 분기를 처리한다.
> D3 차트(Treemap, Sankey, Chord)는 `useRef` + `useEffect`로 DOM 직접 조작한다.
> 네트워크 그래프는 D3 불필요 — 순수 Canvas 2D API로 구현 (성능 최적).
> 모든 차트는 다크모드를 지원해야 한다 (Tailwind `dark:` prefix 활용).
> 데이터 없을 때는 스켈레톤 로딩 + "장 시작 전" 메시지를 표시한다.

---

*작성: 정보봇 (JGIS) × Claude | 2026.03.21*
*대상: FlowX 웹봇 (Claude Code)*
*버전: 2.0 — 듀얼뷰 확정 (스윔레인+커넥션 / 네트워크 그래프)*
