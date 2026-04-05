# FlowX 섹터 로테이션 맵 — 프론트엔드 지시서

> **날짜**: 2026-04-05
> **발신**: 단타봇 (Prophet Agent System)
> **수신**: 웹봇 (FlowX 프론트엔드)
> **위치**: 스윙 분석 페이지 — 기존 퀀트 대시보드(6개 탭) 아래 또는 대체

---

## 개요

섹터별 **자금 흐름 방향**을 한눈에 보여주는 로테이션 맵입니다.
피보나치 눌림목 + 기관/외인 수급 + 직전 거래일 모멘텀을 종합하여
각 섹터의 **로테이션 단계**(선도/추격/대기/후발)를 판정합니다.

---

## Supabase 스키마

### 컬럼 추가 SQL
```sql
ALTER TABLE dashboard_swing
ADD COLUMN IF NOT EXISTS sector_rotation JSONB DEFAULT '{}'::jsonb;
```

### 데이터 구조 (TypeScript)

```typescript
interface SectorRotation {
  timestamp: string;        // "2026-04-05 19:36"
  total_sectors: number;    // 17
  total_stocks: number;     // 279
  sectors: SectorItem[];    // 점수순 정렬 (내림차순)
}

interface SectorItem {
  sector: string;           // "전기전자", "운송장비", "화학", ...
  count: number;            // 해당 섹터 종목 수
  total_score: number;      // 종합 점수 (momentum + flow_score + dual_bonus)
  momentum: number;         // 직전 거래일 평균 등락률 × 10
  flow_score: number;       // 기관+외인 3일 수급 점수 (±30 상한)
  dual_bonus: number;       // 기관+외인 동시 순매수 종목 수 × 10
  avg_chg: number;          // 직전 거래일 평균 등락률 (%)
  avg_drop: number;         // 52주 고점 대비 평균 하락률 (%, 음수)
  avg_upside: number;       // 피보나치 기준 평균 상승여력 (%)
  net_flow_억: number;      // 기관+외인 3일 합산 순매수 (억 단위)
  dual_buy_3d: number;      // 쌍매수(기관+외인 동시 순매수) 종목 수
  up_count: number;         // 직전 거래일 상승 종목 수
  down_count: number;       // 직전 거래일 하락 종목 수
  deep: number;             // DEEP zone 종목 수 (50%+ 하락)
  mid: number;              // MID zone 종목 수 (40~50% 하락)
  mild: number;             // MILD zone 종목 수 (30~40% 하락)
  shallow: number;          // SHALLOW zone 종목 수 (15~30% 하락)
  cap_조: number;           // 섹터 총 시총 (조 단위, 1만억 이상)
  cap_억: number;           // 섹터 총 시총 (억 단위, 1만억 미만)
  stage: string;            // "선도" | "추격" | "대기" | "후발"
  stage_num: number;        // 1=선도, 2=추격, 3=대기, 4=후발
  stage_color: string;      // "GREEN" | "YELLOW" | "RED"
  warning: string;          // "" 또는 "개인 주도 상승 (수급 미확인)"
}
```

---

## UI 디자인

### 패널 제목
**섹터 로테이션 맵** — 부제: "자금 흐름 예측 · 피보나치 + 수급 + 모멘텀"

### 레이아웃

각 섹터를 **카드 형태**로 표시합니다. 점수순(total_score 내림차순) 정렬은 백엔드에서 이미 완료되어 있습니다.

#### 섹터 카드 구성

```
┌─────────────────────────────────────────────┐
│ [선도] 전기전자                    +153.6점  │
│─────────────────────────────────────────────│
│  모멘텀 +13.6  │  수급 +30.0  │  쌍매수 110  │
│─────────────────────────────────────────────│
│  3일 수급: +18,752억  │  쌍매수 11종목        │
│  평균 하락률: -38.2%   │  상승여력: +52.3%     │
│  종목 41개 (↑23 ↓18)  │  DEEP 8 MID 12 ...   │
│─────────────────────────────────────────────│
│  ⚠️ (warning이 있으면 여기 표시)              │
└─────────────────────────────────────────────┘
```

### 로테이션 단계별 색상

| stage | stage_color | 배경색 | 테두리 | 의미 |
|-------|------------|--------|--------|------|
| 선도 | GREEN | `#0d9488` 계열 (teal-600) | `border-teal-500` | 자금 유입 활발 |
| 추격 | GREEN | `#65a30d` 계열 (lime-600) | `border-lime-500` | 유입 시작 |
| 대기 | YELLOW | `#ca8a04` 계열 (yellow-600) | `border-yellow-500` | 관망 |
| 후발 | RED | `#dc2626` 계열 (red-600) | `border-red-500` | 자금 이탈 |

### 카드 상단 뱃지

stage별 뱃지를 카드 좌측 상단에 표시:
- **선도**: `bg-teal-500/20 text-teal-300` — "선도"
- **추격**: `bg-lime-500/20 text-lime-300` — "추격"
- **대기**: `bg-yellow-500/20 text-yellow-300` — "대기"
- **후발**: `bg-red-500/20 text-red-300` — "후발"

### 점수 표시

total_score를 카드 우측 상단에 크게 표시:
- 양수: `text-teal-400` (예: `+153.6`)
- 음수: `text-red-400` (예: `-16.1`)

### 점수 구성 바

3개 점수(momentum, flow_score, dual_bonus)를 수평 바로 시각화:
- **모멘텀**: 파란색 계열 — 직전 거래일 시장 반응
- **수급**: 초록/빨강 — 기관+외인 자금 방향
- **쌍매수**: 금색 — 기관+외인 동시 매수 보너스

### 수급 금액 포맷
- `net_flow_억` 양수: `+18,752억` (초록)
- `net_flow_억` 음수: `-1,001억` (빨강)

### 피보나치 존 분포

DEEP / MID / MILD / SHALLOW 종목 수를 작은 가로 바 차트로:
- DEEP: `bg-red-500` (진한 빨강)
- MID: `bg-orange-500`
- MILD: `bg-yellow-500`
- SHALLOW: `bg-green-500`

### Warning 표시
`warning`이 빈 문자열이 아니면 카드 하단에 경고 표시:
- `bg-amber-500/20 text-amber-300`
- 아이콘 + 텍스트 (예: "개인 주도 상승 (수급 미확인)")

### 상승/하락 종목 비율
`up_count` / `down_count`를 작은 원형 또는 bar로 표시:
- 상승: 초록 `↑23`
- 하락: 빨강 `↓18`

---

## 반응형 레이아웃

| 화면 | 카드 배치 |
|------|----------|
| Desktop (1280+) | 3열 그리드 |
| Tablet (768~1279) | 2열 그리드 |
| Mobile (~767) | 1열 리스트 |

---

## 데이터 갱신 주기

- C19 스케줄 (16:45)에 `upload_dashboard_swing()` 실행 시 자동 갱신
- `sector_rotation`이 `{}` (빈 객체)이면 패널 자체를 숨김 처리

---

## 하위 호환

- `sector_rotation` 필드가 없는 기존 row → 패널 숨김
- `sectors` 배열이 비어있으면 → "데이터 수집 중" 표시

---

## 실제 데이터 예시 (2026-04-05)

```json
{
  "timestamp": "2026-04-05 19:36",
  "total_sectors": 17,
  "total_stocks": 279,
  "sectors": [
    {
      "sector": "전기전자",
      "count": 41,
      "total_score": 153.6,
      "momentum": 13.6,
      "flow_score": 30.0,
      "dual_bonus": 110,
      "avg_chg": 1.36,
      "avg_drop": -38.2,
      "avg_upside": 52.3,
      "net_flow_억": 18752,
      "dual_buy_3d": 11,
      "up_count": 23,
      "down_count": 18,
      "deep": 8,
      "mid": 12,
      "mild": 14,
      "shallow": 7,
      "cap_조": 425.3,
      "cap_억": 0,
      "stage": "선도",
      "stage_num": 1,
      "stage_color": "GREEN",
      "warning": ""
    },
    {
      "sector": "건설",
      "count": 6,
      "total_score": 45.7,
      "momentum": 35.0,
      "flow_score": -9.3,
      "dual_bonus": 20,
      "avg_chg": 3.5,
      "avg_drop": -35.0,
      "avg_upside": 48.0,
      "net_flow_억": -927,
      "dual_buy_3d": 2,
      "up_count": 5,
      "down_count": 1,
      "deep": 1,
      "mid": 2,
      "mild": 2,
      "shallow": 1,
      "cap_조": 8.5,
      "cap_억": 0,
      "stage": "추격",
      "stage_num": 2,
      "stage_color": "GREEN",
      "warning": "개인 주도 상승 (수급 미확인)"
    }
  ]
}
```

---

## 체크리스트

- [ ] Supabase에 `sector_rotation` JSONB 컬럼 추가
- [ ] `SectorRotationPanel` 컴포넌트 생성
- [ ] stage별 뱃지 + 색상 적용
- [ ] 점수 구성 바 (momentum/flow/dual) 시각화
- [ ] 수급 금액 포맷 (쉼표, 억 단위)
- [ ] 피보나치 존 분포 바 차트
- [ ] Warning 표시
- [ ] 반응형 레이아웃 (3열/2열/1열)
- [ ] 빈 데이터 → 패널 숨김 처리
