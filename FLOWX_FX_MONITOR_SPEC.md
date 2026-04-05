# FLOWX 스윙페이지 — 달러-환율 모니터 (외국인 자금 흐름 신호) 지시서

## 프로젝트 소개
이 프로젝트는 **Body Hunter v4** — 한국 주식 단타봇(자동매매 시스템)입니다.
KIS API(한국투자증권) 실계좌 매매, 텔레그램 봇 UI, 7-Group COO 파이프라인,
BRAIN 시장분석, NXT 야간매매, FLOWX VIP 콘텐츠를 포함합니다.

---

## 1. 사전 작업 (Supabase SQL Editor)

```sql
ALTER TABLE dashboard_swing
ADD COLUMN IF NOT EXISTS fx_monitor JSONB DEFAULT '{}'::jsonb;
COMMENT ON COLUMN dashboard_swing.fx_monitor IS '달러-환율 모니터 — DXY/USD-KRW/VIX/외국인 자금 흐름';
```

---

## 2. fx_monitor 데이터 구조

```typescript
interface FxMonitor {
  timestamp: string;           // "2026-04-05 17:53"

  dxy: {
    value: number;             // 100.03 (달러인덱스)
    prev: number;              // 99.65 (전일)
    chg_1d: number;            // +0.38 (전일대비 %)
    ma5: number;               // 100.06 (5일 이평선)
    ma20: number;              // 99.64 (20일 이평선)
    trend: string;             // "약세" | "강세" | "횡보"
  };

  usdkrw: {
    value: number;             // 1510.5 (원/달러)
    prev: number;              // 1509.2
    chg_1d: number;            // +0.09%
    ma5: number;               // 1510.3
    ma20: number;              // 1495.4
    trend: string;             // "원강세" | "원약세" | "횡보"
  };

  vix_structure: {
    vix: number;               // 23.87
    vix3m: number;             // 24.72
    ratio: number;             // 0.966
    structure: string;         // "CONTANGO" | "BACKWARDATION"
    label: string;             // "정상(안전)" | "역전(패닉)"
  };

  correlation: {
    matches: number;           // 12 (역상관 일치 일수)
    total: number;             // 14 (전체 비교 일수)
    pct: number;               // 86 (적중률 %)
    label: string;             // "최근 14일 중 12일 역상관 (86%)"
  };

  foreign_flow: {
    proxy: string;             // "삼성전자" (대용 지표)
    today: number;             // 96570 (백만원)
    today_억: number;          // 966
    sum_3d: number;            // -172004 (3일 합계, 백만원)
    sum_3d_억: number;         // -1720
    streak: number;            // 1 (연속 일수)
    direction: string;         // "매수" | "매도"
    signal: string;            // "순매수전환" | "3일연속매수" | "5일연속매도" 등
    signal_color: string;      // "GREEN" | "YELLOW" | "RED"
  };

  verdict: {
    text: string;              // "외국인 유입 가능"
    color: string;             // "GREEN" | "YELLOW" | "RED"
    bullish: number;           // 3 (유입 방향 점수)
    bearish: number;           // 0 (유출 방향 점수)
    score: number;             // 3 (bullish - bearish)
  };
}
```

### 실제 데이터 예시 (4/5 기준)

```json
{
  "timestamp": "2026-04-05 17:53",
  "dxy": {"value": 100.03, "prev": 99.65, "chg_1d": 0.38, "ma5": 100.06, "ma20": 99.64, "trend": "횡보"},
  "usdkrw": {"value": 1510.5, "prev": 1509.2, "chg_1d": 0.09, "ma5": 1510.3, "ma20": 1495.4, "trend": "횡보"},
  "vix_structure": {"vix": 23.87, "vix3m": 24.72, "ratio": 0.966, "structure": "CONTANGO", "label": "정상(안전)"},
  "correlation": {"matches": 12, "total": 14, "pct": 86, "label": "최근 14일 중 12일 역상관 (86%)"},
  "foreign_flow": {"proxy": "삼성전자", "today_억": 966, "sum_3d_억": -1720, "streak": 1, "direction": "매수", "signal": "순매수전환", "signal_color": "GREEN"},
  "verdict": {"text": "외국인 유입 가능", "color": "GREEN", "bullish": 3, "bearish": 0, "score": 3}
}
```

---

## 3. 프론트엔드 배치 위치

**스윙페이지 최상단** — 모든 콘텐츠 위에 배치.

```
━━━━━━━━━━━━━━━━━━━━━━━━━━━
💲 달러-환율 모니터                    ← 최상단 (신규)
━━━━━━━━━━━━━━━━━━━━━━━━━━━

[... BRAIN 시장판단 ...]
[... 자산배분 ...]
[... 피보나치 눌림목 ...]
[... 추천 종목 ...]
```

---

## 4. UI 디자인 가이드

### 4-1. 헤더 + 종합 판정

```
┌─────────────────────────────────────────────────────────────┐
│  💲 달러-환율 모니터 — 외국인 자금 흐름 신호                     │
│                                                              │
│  종합: [외국인 유입 가능]          유입 +3 / 유출 0              │
│  기준: 2026-04-05 17:53                                       │
└─────────────────────────────────────────────────────────────┘
```

- **verdict.color**에 따라 배지 색상:
  - `GREEN` → 초록 배경 (#22c55e) — "외국인 유입 강력" 또는 "외국인 유입 가능"
  - `YELLOW` → 노랑 배경 (#eab308) — "중립 (관망)" 또는 "외국인 유출 우려"
  - `RED` → 빨강 배경 (#ef4444) — "외국인 유출 경고"

### 4-2. 4개 카드 레이아웃

가로 4칸 카드로 핵심 지표 표시:

```
┌──────────────┐ ┌──────────────┐ ┌──────────────┐ ┌──────────────┐
│  DXY 달러     │ │  USD/KRW     │ │  VIX 구조     │ │  외국인 흐름   │
│              │ │              │ │              │ │              │
│  100.03      │ │  1,510.5원   │ │  23.87       │ │  🟢순매수전환  │
│  ▼약세       │ │  ▼원강세     │ │  CONTANGO    │ │  +966억      │
│              │ │              │ │              │ │              │
│  5일: 100.1  │ │  5일: 1,510  │ │  VIX3M: 24.7 │ │  3일: -1,720 │
│  20일: 99.6  │ │  20일: 1,495 │ │  비율: 0.966  │ │  1일째 매수   │
└──────────────┘ └──────────────┘ └──────────────┘ └──────────────┘
```

### 4-3. 각 카드 상세

#### DXY 달러인덱스
- **주 수치**: `dxy.value` (큰 폰트)
- **추세 화살표**: `dxy.trend` → "약세"=▼초록, "강세"=▲빨강, "횡보"=→회색
- **이평선**: `dxy.ma5` / `dxy.ma20`
- **전일대비**: `dxy.chg_1d` (%)

#### USD/KRW 환율
- **주 수치**: `usdkrw.value` + "원" (큰 폰트)
- **추세 화살표**: "원강세"=▼초록, "원약세"=▲빨강, "횡보"=→회색
  - 원강세 = 환율 하락 = 좋은 신호 → **초록색**
- **이평선**: `usdkrw.ma5` / `usdkrw.ma20`

#### VIX 구조
- **주 수치**: `vix_structure.vix`
- **구조**: `vix_structure.structure`
  - CONTANGO → 초록 배지 "정상(안전)"
  - BACKWARDATION → 빨강 배지 "역전(패닉)"
- **부가**: `vix_structure.vix3m`, `vix_structure.ratio`

#### 외국인 흐름
- **신호**: `foreign_flow.signal` (큰 텍스트)
- **신호 색상**: `foreign_flow.signal_color` (GREEN/YELLOW/RED)
- **당일 금액**: `foreign_flow.today_억` + "억"
- **3일 합계**: `foreign_flow.sum_3d_억` + "억"
- **연속**: `foreign_flow.streak` + "일째 " + `foreign_flow.direction`
- **주석**: "(삼성전자 기준)" 작은 텍스트

### 4-4. 상관관계 바

카드 아래에 얇은 바로 표시:

```
📊 환율↔KOSPI 상관: 86% (최근 14일 중 12일 역상관)
[████████████████████░░░░] 86%
```

- `correlation.pct`를 진행 바로 표시
- 80%+ → 초록, 60~80% → 노랑, 60% 미만 → 빨강

### 4-5. 추세 화살표 규칙

| 필드 | 조건 | 화살표 | 색상 | 의미 |
|------|------|--------|------|------|
| dxy.trend | "약세" | ▼ | #22c55e (초록) | 달러 약세 = 원강세 = 긍정 |
| dxy.trend | "강세" | ▲ | #ef4444 (빨강) | 달러 강세 = 원약세 = 부정 |
| dxy.trend | "횡보" | → | #9ca3af (회색) | 중립 |
| usdkrw.trend | "원강세" | ▼ | #22c55e | 환율 하락 = 긍정 |
| usdkrw.trend | "원약세" | ▲ | #ef4444 | 환율 상승 = 부정 |
| usdkrw.trend | "횡보" | → | #9ca3af | 중립 |

**핵심**: DXY ▼(약세) + 환율 ▼(원강세) = 모두 초록 = 외국인 유입 신호

### 4-6. 데이터 없을 때

`fx_monitor`가 `null`, `{}`, 또는 `dxy` 필드가 없으면:

```
┌─────────────────────────────────────────────┐
│  💲 달러-환율 모니터                           │
│                                              │
│  데이터 수집 중입니다. 16:45 이후 갱신됩니다.   │
└─────────────────────────────────────────────┘
```

---

## 5. 데이터 흐름

```
16:45  upload_swing.py → run_flowx_swing_upload()
  └─ _build_fx_monitor()
       ├─ yfinance: DXY, USD/KRW, VIX, VIX3M, KOSPI
       ├─ CSV: 삼성전자 외국인 순매수 (data_store/flow/005930_investor.csv)
       ├─ 환율↔KOSPI 15일 상관관계 계산
       └─ 종합 판정 (외국인 유입/유출 점수)
  └─ upload_dashboard_swing(data)
       └─ Supabase dashboard_swing.fx_monitor에 업로드
```

---

## 6. 종합 판정 점수 체계

| 지표 | 조건 | 유입 점수 | 유출 점수 |
|------|------|-----------|-----------|
| DXY | 약세 | +2 | - |
| DXY | 강세 | - | +2 |
| 환율 | 원강세 | +2 | - |
| 환율 | 원약세 | - | +2 |
| VIX | CONTANGO | +1 | - |
| VIX | BACKWARDATION | - | +2 |
| 외국인 | GREEN (매수) | +2 | - |
| 외국인 | RED (매도) | - | +2 |

**최종 score = bullish - bearish**

| score | 판정 | 색상 |
|-------|------|------|
| 4+ | 외국인 유입 강력 | GREEN |
| 2~3 | 외국인 유입 가능 | GREEN |
| 0~1 | 중립 (관망) | YELLOW |
| -1~-2 | 외국인 유출 우려 | YELLOW |
| -3 이하 | 외국인 유출 경고 | RED |

---

## 7. 각 지표 설명 (툴팁용)

| 지표 | VIP 회원용 설명 |
|------|----------------|
| DXY 달러인덱스 | 미국 달러의 세계적 강약. 100 이하=약세, 103+=강세. 달러 약세 시 신흥국(한국) 자금 유입 |
| USD/KRW | 원/달러 환율. 하락=원강세=외국인이 한국 주식 사기 유리. 상승=원약세=외국인 이탈 |
| VIX CONTANGO | VIX < VIX3M이면 정상(CONTANGO). VIX > VIX3M이면 패닉(BACKWARDATION=기관 헤지 급증) |
| 상관관계 | 최근 15일간 환율↔KOSPI 역상관 적중률. 80%+이면 환율이 KOSPI 방향의 강력한 선행지표 |
| 외국인 흐름 | 삼성전자 외국인 순매수 추이 (시장 전체 대용). 순매수전환=긍정 신호, 연속매도=부정 신호 |

---

## 8. 주의사항

1. **trend 필드는 한국어** — "약세", "원강세" 등 그대로 표시
2. **signal_color는 영어** (GREEN/YELLOW/RED) — 프론트에서 색상 매핑
3. **today_억, sum_3d_억 사용** — 백만원 단위(today, sum_3d)는 내부용
4. **timestamp는 한국 시간(KST)** 기준
5. **모바일에서는 4칸 → 2x2 그리드**로 반응형
