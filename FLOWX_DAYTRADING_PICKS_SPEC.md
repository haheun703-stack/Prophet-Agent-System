# FLOWX 웹봇 지시서 — 단타 TOP픽 Hybrid 2단 패널

> **프로젝트**: Body Hunter v4 — 한국 주식 단타봇(자동매매 시스템)
> KIS API(한국투자증권) 실계좌 매매, 텔레그램 봇 UI, 7-Group COO 파이프라인,
> BRAIN 시장분석, NXT 야간매매, FLOWX VIP 콘텐츠를 포함합니다.
> CFO(재무)/CTO(기술)/COO(운영) 3-Officer 구조로 운영됩니다.

## 배경

기존 단타 TOP픽은 일회성 발행이었으나, **외국인 야간 수급(EWY)** 을 반영하려면 미국장이 열린 뒤(08:00 KST 이후)여야 의미가 있습니다. 반면, **NXT 야간 매수자**는 16:35~20:00 사이에 매수 가능하므로 국장 마감 직후(16:45)에도 한 번 발행이 필요합니다.

→ **Hybrid 2단 발행** 도입:

| 발행 시각 | 모드 | 데이터 기준 | 대상 |
|-----------|------|------------|------|
| **16:45 KST** | `preview` | 국장 마감 수급만 (미국장 X) | NXT 야간매수자 |
| **07:35 KST** | `confirmed` | 미국장 + EWY 바스켓 반영 | 정규장 09:00 진입자 |

같은 날 prev/conf 두 발행이 모두 저장되며, 웹 대시보드에서는 `confirmed` 우선, 없으면 `preview` 표시.

---

## 데이터 소스

### Supabase 테이블
```
intelligence_daytrading_picks
  PK: (date, mode)  -- mode ∈ ('preview', 'confirmed')
```

### 주요 컬럼
- `date` — DATE
- `mode` — 'preview' | 'confirmed'
- `updated_at` — TIMESTAMPTZ (UTC)
- `picks_count`, `track_a_count`, `track_b_count` — INT
- `ewy_1d`, `ewy_5d`, `ks200_1d`, `ks200_5d` — NUMERIC (preview 모드는 NULL/0)
- `ewy_source` — TEXT
- `picks` — **JSONB 배열** (종목 리스트)

### picks JSONB 구조 (각 항목)
```json
{
  "rank": 1,
  "code": "005930",
  "name": "삼성전자",
  "sector": "전기전자",
  "track": "A_대형주",           // "A_대형주" | "B_중소형주"
  "mcap_억": 350000,
  "close": 78500,
  "entry_low": 77322,
  "entry_high": 79285,
  "tp1": 82425,
  "tp2": 84780,
  "sl": 75750,
  "upside_to_tp1_pct": 5.0,
  "final_score": 85.5,
  "score": 70.0,
  "sector_bonus": 5,
  "mcap_bonus": 5,
  "ewy_bonus": 5,
  "key_reasons": "외국인 4/5일 매수 +1,200억 + 기관 합류 2일 + EWY 수혜 전기전자",
  "foreign_total_억": 1200.5,
  "inst_total_억": 850.2,
  "dual_total_억": 2050.7,
  "buy_days": 4,
  "inst_joining": 2,
  "price_change_%": 3.5,
  "etf_alt_code": "091160",
  "etf_alt_name": "KODEX 반도체",
  "etf_alt_theme": "반도체"
}
```

---

## UI 요구사항

### 위치
FLOWX 스윙시스템 페이지 (기존 스윙 패널 위 또는 별도 탭)

### 패널 구성 (권장)

#### 1. 헤더
```
🎯 단타 TOP픽  [confirmed] · 2026-04-09 07:35
🌍 EWY +10.13% · KS200 -1.93% · 🔥 외국인 한국 폭발매수
```

- mode가 `preview`면 `📢 프리뷰 · 국장 마감 기준` 뱃지
- mode가 `confirmed`면 `✅ 확정 · 미국장 반영` 뱃지
- EWY 섹션은 `confirmed` 모드에서만 표시
- EWY ≥ +5% → 🔥 / ≥ +2% → ✅ / ≤ -2% → ⚠️

#### 2. 트랙 필터 탭
- **전체** | **🔷 대형주 (Track A)** | **🟢 중소형주 (Track B)**

#### 3. 카드 리스트 (rank 순)
각 카드 구성:
```
┌────────────────────────────────────┐
│ 🥇 삼성전자 (005930) · 전기전자     │
│ 💰 78,500원 · 시총 350,000억        │
│ 🎯 진입 77,322~79,285                │
│ 🎯 목표 82,425 (+5.0%)              │
│ 📊 외국인 4/5일 매수 +1,200억      │
│    + 기관 합류 2일                  │
│ 🔗 KODEX 반도체 (091160) [반도체]  │
│ ⭐ 85.5점                           │
└────────────────────────────────────┘
```

- rank 1~3: 🥇🥈🥉, 4~7: 숫자 뱃지
- track A는 청색 테두리, track B는 녹색 테두리
- 진입가 → 목표가 → SL은 작은 박스로
- 🔗 ETF 대안은 **subtle**하게 (작은 배지)

#### 4. 하단 ETF 집계 섹션
```
🔗 오늘의 섹터 ETF TOP 3 (개별 종목 대신 분산진입)
  1. KODEX 반도체 (091160) [반도체] — 3종목: SK하이닉스, 삼성전기, 삼성SDI
  2. PLUS K방산 (449450) [방산] — 2종목: 한화에어로, LIG넥스원
  3. TIGER 조선TOP10 (139230) [조선] — 1종목: 대한해운
```

개별 pick의 `etf_alt_code`를 기준으로 프론트에서 집계하면 됩니다.

#### 5. 푸터
- `preview`: `⏰ 09:00 KRX 정규장 진입 (NXT 종목은 별도 메시지) · 내일 07:35 확정 재발행`
- `confirmed`: `⏰ 09:00 개장 ~ 09:30 진입 권장`

---

## 데이터 쿼리 예시

### 오늘의 최신 발행 (confirmed 우선)
```sql
SELECT *
FROM intelligence_daytrading_picks
WHERE date = CURRENT_DATE
ORDER BY
  CASE mode WHEN 'confirmed' THEN 0 ELSE 1 END,
  updated_at DESC
LIMIT 1;
```

### 7일 히스토리 (confirmed만)
```sql
SELECT date, mode, picks_count, ewy_1d, picks
FROM intelligence_daytrading_picks
WHERE mode = 'confirmed'
  AND date >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY date DESC;
```

### 특정 종목의 히스토리 (track record)
```sql
SELECT date, mode, (p.value->>'rank')::int AS rank,
       (p.value->>'final_score')::numeric AS score
FROM intelligence_daytrading_picks t,
     jsonb_array_elements(t.picks) p
WHERE p.value->>'code' = '005930'
  AND date >= CURRENT_DATE - INTERVAL '30 days'
ORDER BY date DESC;
```

---

## 스케줄

- **16:45 KST** — COO G7 Stage 3 `C30_daytrading_picks_preview`
  - G6(C3 일봉수집)/C28(stealth_scan) 완료 후 실행
  - 실제 실행 시각은 16:30~16:50 범위
- **07:35 KST** — COO G1 `A12_daytrading_picks_confirmed`
  - G1 병렬 잡(A1~A11) 완료 후 순차 실행
  - A11(미국장 야간 필터)이 EWY/KS200 데이터 생성 후 진행
  - 실제 실행 시각은 07:35~07:50 범위

두 시각 모두 텔레그램으로 전송(퐝가님 계정)되며, Supabase 업로드는 실패해도 텔레그램은 송출됨.

---

## 엣지 케이스

1. **pick 0개**: 필터 통과 종목 없음 → Supabase에는 `picks_count=0`으로 저장, 텔레그램 스킵
2. **preview 발행 후 confirmed 실패**: confirmed가 실패해도 preview는 유지 → 프론트는 `confirmed` 없으면 `preview` 표시
3. **장 휴장일**: G1/G7이 자동 스킵되므로 해당 날짜 row 없음 → 프론트는 "오늘은 장이 닫힌 날입니다" 표시
4. **EWY 데이터 없음** (미국장 미개장 등): `ewy_source='none'`, 경고 배지 표시

---

## 변경 이력

- **2026-04-09** — v1 초안 (Hybrid 2단 발행 설계, ETF 대안 추가)
  - COO 배선: G1 A12(confirmed) + G7 C30(preview)
  - Supabase: `intelligence_daytrading_picks (date, mode) PK`
  - 단타 발행 스크립트: `scalper-agent/tools/daytrading_picks.py --mode {preview|confirmed}`
  - ETF 매핑: `scalper-agent/tools/sector_etf_map.py`
