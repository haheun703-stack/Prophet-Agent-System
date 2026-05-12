# FLOWX 스윙페이지 — NXT 추천 근거 (채권자경단 v2) 표현 지시서

## 프로젝트 소개
이 프로젝트는 **Body Hunter v4** — 한국 주식 단타봇(자동매매 시스템)입니다.
KIS API(한국투자증권) 실계좌 매매, 텔레그램 봇 UI, 7-Group COO 파이프라인,
BRAIN 시장분석, NXT 야간매매, FLOWX VIP 콘텐츠를 포함합니다.

---

## 1. 사전 작업 (Supabase SQL Editor)

```sql
ALTER TABLE dashboard_swing
ADD COLUMN IF NOT EXISTS nxt_rationale JSONB DEFAULT '{}'::jsonb;
COMMENT ON COLUMN dashboard_swing.nxt_rationale IS '채권자경단 v2 — 7대 글로벌 리스크 신호 (NXT 추천 근거)';
```

이 SQL을 Supabase Dashboard → SQL Editor에서 실행해주세요.

---

## 2. nxt_rationale 데이터 구조

```typescript
interface NxtRationale {
  timestamp: string;           // "2026-04-04 23:13:10"
  verdict: string;             // "적극 매수" | "조건부 매수" | "경계" | "회피"
  green: number;               // 안전 신호 수 (0~7)
  yellow: number;              // 경계 신호 수
  red: number;                 // 위험 신호 수
  total: number;               // 전체 지표 수 (7)
  indicators: Indicator[];     // 7개 지표 상세
}

interface Indicator {
  key: string;          // "move" | "vix_term" | "cu_au" | "jpy_carry" | "vvix" | "credit_spread" | "btc"
  name: string;         // 한국어 이름 (예: "MOVE 채권공포")
  signal: string;       // "GREEN" | "YELLOW" | "RED"
  signal_label: string; // "안전" | "경계" | "위험"
  detail: string;       // 수치 요약 (예: "VIX 23.87 / VIX3M 24.72 (CONTANGO)")
}
```

### 실제 데이터 예시 (4/3 기준)

```json
{
  "timestamp": "2026-04-04 23:13:10",
  "verdict": "적극 매수",
  "green": 5,
  "yellow": 1,
  "red": 1,
  "total": 7,
  "indicators": [
    {"key": "move", "name": "MOVE 채권공포", "signal": "GREEN", "signal_label": "안전", "detail": "81.78"},
    {"key": "vix_term", "name": "VIX 기간구조", "signal": "GREEN", "signal_label": "안전", "detail": "VIX 23.87 / VIX3M 24.72 (CONTANGO)"},
    {"key": "cu_au", "name": "구리/금 비율", "signal": "GREEN", "signal_label": "안전", "detail": "구리 -1.1% vs 금 -2.8%"},
    {"key": "jpy_carry", "name": "엔 캐리트레이드", "signal": "GREEN", "signal_label": "안전", "detail": "JPY 159.63 (ON)"},
    {"key": "vvix", "name": "VVIX 스마트머니", "signal": "GREEN", "signal_label": "안전", "detail": "115.33 (MA20 대비 -6.6%)"},
    {"key": "credit_spread", "name": "신용스프레드", "signal": "RED", "signal_label": "위험", "detail": "HYG +0.24% vs LQD +0.42%"},
    {"key": "btc", "name": "BTC 야간심리", "signal": "YELLOW", "signal_label": "경계", "detail": "$67,198 (+0.4%)"}
  ]
}
```

---

## 3. 프론트엔드 배치 위치

스윙페이지에서 **"추천 종목 - 야간 매매(NXT)"** 섹션 **바로 위에** 배치합니다.

```
[... BRAIN 분석 ...]
[... 추천 종목 - 주간 매매 ...]
[... ETF 추천 ...]

━━━━━━━━━━━━━━━━━━━━━━━━━━━
🔎 야간 매매 판단 근거 (채권자경단 v2)    ← 여기
━━━━━━━━━━━━━━━━━━━━━━━━━━━

[... 추천 종목 - 야간 매매(NXT) ...]
```

---

## 4. UI 디자인 가이드

### 4-1. 헤더 (종합 판정)

```
┌─────────────────────────────────────────────┐
│  🔎 야간 매매 판단 근거                      │
│                                              │
│  ██████████  종합: [적극 매수]               │
│  🟢×5  🟡×1  🔴×1  (7개 지표 중)            │
│                                              │
│  기준: 2026-04-04 23:13                      │
└─────────────────────────────────────────────┘
```

- **verdict** 값에 따라 배지 색상:
  - `적극 매수` → 초록 배경 (#22c55e)
  - `조건부 매수` → 파랑 배경 (#3b82f6)
  - `경계` → 노랑 배경 (#eab308)
  - `회피` → 빨강 배경 (#ef4444)

### 4-2. 7개 지표 테이블/카드

각 indicator를 카드 또는 테이블 행으로 표시:

```
┌──────────────────┬────────┬──────────────────────────────┐
│ 지표              │ 판정   │ 상세                          │
├──────────────────┼────────┼──────────────────────────────┤
│ MOVE 채권공포     │ 🟢안전 │ 81.78                        │
│ VIX 기간구조      │ 🟢안전 │ VIX 23.87 / VIX3M 24.72     │
│ 구리/금 비율      │ 🟢안전 │ 구리 -1.1% vs 금 -2.8%       │
│ 엔 캐리트레이드   │ 🟢안전 │ JPY 159.63 (ON)              │
│ VVIX 스마트머니   │ 🟢안전 │ 115.33 (MA20 대비 -6.6%)     │
│ 신용스프레드      │ 🔴위험 │ HYG +0.24% vs LQD +0.42%    │
│ BTC 야간심리      │ 🟡경계 │ $67,198 (+0.4%)              │
└──────────────────┴────────┴──────────────────────────────┘
```

### 4-3. 신호 색상 규칙

| signal | 색상 코드 | 아이콘 | 텍스트 |
|--------|-----------|--------|--------|
| GREEN  | #22c55e   | 🟢    | 안전   |
| YELLOW | #eab308   | 🟡    | 경계   |
| RED    | #ef4444   | 🔴    | 위험   |

### 4-4. 데이터 없을 때 (fallback)

`nxt_rationale`이 `null`, `{}`, 또는 `indicators`가 빈 배열이면:

```
┌─────────────────────────────────────────────┐
│  🔎 야간 매매 판단 근거                      │
│                                              │
│  데이터 수집 중입니다. 16:35 이후 갱신됩니다.  │
└─────────────────────────────────────────────┘
```

---

## 5. 데이터 흐름 (자동화)

```
16:35  nightwatch.py run_nightwatch()
  ├─ [1단~5단] 기존 NXT 분석
  ├─ [5.5단] collect_bond_vigilante_v2()  ← 신규
  │    └─ yfinance로 7개 지표 수집
  │    └─ GREEN/YELLOW/RED 판정
  │    └─ 종합 verdict 산출
  └─ save_nightwatch_report(report, bond_vigilante=bv)
       └─ nightwatch_report.json에 "bond_vigilante" 필드 저장

16:45  upload_swing.py run_flowx_swing_upload()
  ├─ generate_swing_page_data() → picks, etf_picks 생성
  └─ upload_dashboard_swing(data)
       └─ _build_nxt_rationale(nxt) → JSONB 변환
       └─ Supabase dashboard_swing.nxt_rationale에 업로드
```

**주기**: 매일 장 마감 후 16:35~16:50 사이 자동 갱신

---

## 6. 주의사항

1. **signal 필드는 영어("GREEN"/"YELLOW"/"RED")**, signal_label이 한국어("안전"/"경계"/"위험")
2. **indicators 배열 순서는 고정** (MOVE → VIX기간구조 → 구리금 → 엔캐리 → VVIX → 신용스프레드 → BTC)
3. **detail 문자열은 그대로 표시** — 프론트에서 파싱하지 말 것
4. **verdict가 "수집실패"이면** fallback UI 표시
5. **timestamp는 한국 시간(KST)** 기준

---

## 7. 각 지표 의미 설명 (툴팁용)

| 지표 | 설명 (VIP 회원용) |
|------|-------------------|
| MOVE 채권공포 | 미국 채권 시장 변동성. 100 이하 안전, 120 이상 위험 |
| VIX 기간구조 | 단기(VIX) vs 장기(VIX3M). CONTANGO=정상, BACKWARDATION=패닉 |
| 구리/금 비율 | 구리(경기) vs 금(안전). 구리 강세=경기 회복, 금 강세=불안 |
| 엔 캐리트레이드 | 엔화 약세(ON)=위험자산 선호, 엔화 강세(OFF)=안전자산 선호 |
| VVIX 스마트머니 | VIX의 VIX. MA20 대비 높으면=기관이 헤지 중, 낮으면=안심 |
| 신용스프레드 | HYG(하이일드) vs LQD(투자등급). HYG 강세=리스크온, LQD 강세=리스크오프 |
| BTC 야간심리 | 비트코인 24시간 변동. 야간 투자심리의 바로미터 |
