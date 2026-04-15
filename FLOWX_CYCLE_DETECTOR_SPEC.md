# FLOWX 수급 사이클 감지기 패널 — 웹봇 지시서

## 개요
스윙시스템 페이지에 **수급 사이클 감지기** 탭 추가.
4세력(외인/기관/개인/기타법인) 수급 흐름을 분석해서
각 종목의 사이클 위상(급등임박/매집/전환/분배/고점경고)을 판정.

## 탭 순서
```
대시보드 → 사이클 감지기 → 단타 TOP픽 → 선매집 탐지
```
기존 "대시보드" 바로 다음, "단타 TOP픽" 앞에 위치.

## 데이터 소스
**테이블**: `intelligence_cycle_scan`
**쿼리**: `SELECT * FROM intelligence_cycle_scan ORDER BY date DESC LIMIT 1`

## 데이터 구조
```json
{
  "date": "2026-04-15",
  "total_scanned": 40,
  "surge_count": 5,
  "accumulate_count": 12,
  "reversal_count": 3,
  "neutral_count": 15,
  "distribute_count": 3,
  "peak_warn_count": 2,
  "surge_items": [
    {
      "code": "005380",
      "name": "현대차",
      "phase": "SURGE",
      "phase_kr": "급등임박",
      "score": 100,
      "latest_close": 508000,
      "change_pct": 3.4,
      "cap_억": 465000,
      "market": "KOSPI",
      "summary": "현대차: [급등임박(지속)] 쌍매수 + 쌍매도 + 개인바침 + 기타매집 + 기관전환 + 3세력매수",
      "surge_type": "지속",
      "signals": [
        {"name": "twin_buy", "name_kr": "쌍매수", "score": 38, "detail": "최근3일 중 2일 쌍매수", "days": 2},
        {"name": "stealth_acc", "name_kr": "기타매집", "score": 33, "detail": "기타법인 7일 연속 매수 (합산+926억)", "days": 7}
      ]
    }
  ],
  "accumulate_items": [
    {
      "code": "000660",
      "name": "SK하이닉스",
      "phase": "ACCUMULATION",
      "phase_kr": "매집",
      "score": 45,
      "latest_close": 195000,
      "change_pct": -0.5,
      "cap_억": 1340000,
      "market": "KOSPI",
      "summary": "기관 5일 연속 매수",
      "signals": [
        {"name": "stealth_acc", "name_kr": "기타매집", "score": 15, "detail": "기타법인 5일 연속 +340억", "days": 5}
      ]
    }
  ],
  "reversal_items": [
    {
      "code": "066570",
      "name": "LG전자",
      "phase": "REVERSAL",
      "phase_kr": "전환",
      "score": 20,
      "latest_close": 85000,
      "change_pct": 1.2,
      "cap_억": 139000,
      "market": "KOSPI",
      "summary": "LG전자: [전환] 기관전환",
      "surge_type": "",
      "signals": [
        {"name": "force_reversal", "name_kr": "기관전환", "score": 20, "detail": "기관 매도(-80)→매수(+120) 전환", "days": 0}
      ]
    }
  ],
  "warning_items": [
    {
      "code": "012345",
      "name": "예시종목",
      "phase": "PEAK_WARN",
      "phase_kr": "고점경고",
      "score": -45,
      "latest_close": 50000,
      "change_pct": 8.5,
      "cap_억": 12000,
      "market": "KOSDAQ",
      "summary": "기타법인 대량 이탈 + 개인만 매수",
      "signals": [
        {"name": "stealth_exit", "name_kr": "기타이탈", "score": -20, "detail": "기타법인 4일 연속 매도 -180억", "days": 4},
        {"name": "retail_trap", "name_kr": "개인함정", "score": -15, "detail": "개인만 매수, 3세력 매도", "days": 3}
      ]
    }
  ],
  "top_surge_names": ["현대차", "LG에너지솔루션", "삼성전자"],
  "phase_summary": {
    "total": 40,
    "SURGE": 5,
    "ACCUMULATION": 12,
    "REVERSAL": 3,
    "NEUTRAL": 15,
    "DISTRIBUTION": 3,
    "PEAK_WARN": 2
  }
}
```

## 6가지 사이클 위상 정의

| 위상 | 한국어 | 의미 | 색상 | 아이콘 |
|------|--------|------|------|--------|
| SURGE | 급등임박 | 쌍매수+개인바침/기타매집 → 폭등 직전 | 빨강/금색 | 🔥 |
| ACCUMULATION | 매집 | 세력이 조용히 모으는 중 | 주황 | 📦 |
| REVERSAL | 전환 | 하락→상승 바닥 전환 징후 | 초록 | 🔄 |
| NEUTRAL | 중립 | 특별 신호 없음 | 회색 | ⚪ |
| DISTRIBUTION | 물량분배 | 세력→개인 떠넘기기 진행 | 보라 | 📤 |
| PEAK_WARN | 고점경고 | 기타법인 이탈+개인만 매수 = 하락 전조 | 검정/빨강 | ⚠️ |

## 급등 세분화: surge_type (v2)

SURGE(급등임박) 종목을 "지속"과 "원샷"으로 구분.
**surge_type 필드는 SURGE 위상에서만 값이 있고, 나머지 위상은 빈 문자열.**

| surge_type | 의미 | 조건 | 뱃지 색상 |
|------------|------|------|-----------|
| 지속 | 3일+ 연속매집 후 상승 → 추가 상승 가능 | acc_days >= 3 | 초록 |
| 원샷 | 하루 몰빵 급등 → 추격 위험 | change >= 15% & acc <= 2일, 또는 change >= 10% & acc <= 1일 | 빨강 + ⚡ |

### 카드 표시 예시
```
🔥 [지속] 현대차  +100점  508,000원  +3.4%
   → 3일+ 연속매집, 추가 상승 가능

🔥 [원샷⚡] 대한전선  +55점  41,250원  +28.3%
   → 하루 몰빵 급등, 추격 위험
```

### 정렬 우선순위
같은 SURGE 내에서: **지속 > 원샷** 순서 (지속이 더 가치 있음)

## 전환 위상: reversal_items (v2)

REVERSAL(전환) 위상 종목을 별도 섹션으로 표시.
**섹션 순서**: 급등임박 → 전환 → 매집 → 경고

| 필드 | 설명 |
|------|------|
| `reversal_items` | JSONB 배열, 최대 10개. 바닥 전환 징후 종목 |

### 빈 데이터
- `reversal_items` 빈 배열 또는 null: "전환 감지 종목 없음"

## 8가지 감지 신호 (signals 배열)

| name | name_kr | 의미 | 점수 | 색상 |
|------|---------|------|------|------|
| twin_buy | 쌍매수 | 외인+기관 동시 매수 | +25 | 금색 |
| twin_sell | 쌍매도 | 외인+기관 동시 매도 | -20 | 빨강 |
| retail_sacrifice | 개인바침 | 개인 대량 매도(세력 매수 중) | +20 | 보라 |
| stealth_acc | 기타매집 | 기타법인 지속 매수 (숨은 세력) | +15 | 남색 |
| stealth_exit | 기타이탈 | 기타법인 대량 이탈 | -20 | 검정 |
| force_reversal | 세력전환 | 매도→매수 전환 감지 | +20 | 초록 |
| retail_trap | 개인함정 | 개인만 매수, 3세력 전부 매도 | -15 | 회색 |
| triple_buy | 3세력매수 | 외인+기관+기타법인 동시매수 | +10 | 금색 |

## UI 레이아웃

### 헤더 요약 (상단 배너)
```
🔍 수급 사이클 감지기  |  총 40종목 스캔
🔥 급등임박 5  |  📦 매집 12  |  🔄 전환 3  |  ⚠️ 경고 5
마지막 스캔: 2026-04-15
```

### 위상 분포 차트 (선택)
파이차트 또는 도넛차트로 6위상 분포 표시.
`phase_summary` 데이터 사용.

### 4개 섹션/탭

**섹션 1: 🔥 급등임박 (surge_items)** — 빨강/금색 테마
- 가장 중요한 섹션 — 매수 검토 대상
- 기본 펼침 상태
- surge_type별 뱃지: [지속]=초록, [원샷⚡]=빨강

**섹션 2: 🔄 전환 (reversal_items)** — 초록 테마
- 바닥 전환 징후 종목 — 관찰 대상
- 기본 접힘 가능

**섹션 3: 📦 매집 (accumulate_items)** — 주황 테마
- 관심 종목 등록 대상 (아직 진입 이름)
- 기본 접힘 가능

**섹션 4: ⚠️ 경고 (warning_items)** — 빨강/검정 테마
- 보유 중이면 청산 검토, 미보유면 매수 금지
- 기본 접힘 가능

### 종목 카드 레이아웃
```
┌──────────────────────────────────────────────────┐
│ 🔥 현대차  [KOSPI]   급등임박   +100점           │
│──────────────────────────────────────────────────│
│ 감지 신호:                                        │
│  ⚡ 쌍매수 (+25) — 외인+기관 5일 중 3일 동시 매수   │
│  🕵️ 기타매집 (+30) — 기타법인 7일 연속 +926억       │
│──────────────────────────────────────────────────│
│ 현재가 218,000원  │  전일비 +2.3%  │  시총 46.5조   │
│ 한줄 요약: 기타법인 7일 연속 매수(926억) + 쌍매수    │
└──────────────────────────────────────────────────┘
```

### 경고 종목 카드 (다른 디자인)
```
┌──────────────────────────────────────────────────┐
│ ⚠️ 예시종목  [KOSDAQ]   고점경고   -45점          │
│──────────────────────────────────────────────────│
│ 위험 신호:                                        │
│  ⛔ 기타이탈 (-20) — 기타법인 4일 연속 매도 -180억   │
│  🪤 개인함정 (-15) — 개인만 매수, 3세력 매도          │
│──────────────────────────────────────────────────│
│ 현재가 50,000원  │  전일비 +8.5%  │  시총 1.2조     │
│ 한줄 요약: 기타법인 대량 이탈 + 개인만 매수           │
└──────────────────────────────────────────────────┘
```

## 필드 설명

| 필드 | 설명 | 표시 |
|------|------|------|
| `name` | 종목명 | 메인 텍스트 |
| `phase` | 영문 위상 | 내부 분류용 |
| `phase_kr` | 한국어 위상 | 뱃지 표시 |
| `score` | 종합 점수 (-100 ~ +100) | 점수 표시 (양수=녹색, 음수=빨간) |
| `latest_close` | 현재가 | 가격 포맷 (천 단위 콤마) |
| `change_pct` | 전일 대비 등락률(%) | +녹색/-빨간 |
| `cap_억` | 시총(억원) | 조/억 자동변환 |
| `market` | 시장 (KOSPI/KOSDAQ) | 뱃지 |
| `summary` | 한줄 요약 | 카드 하단 텍스트 |
| `signals` | 감지 신호 배열 | 카드 중앙 신호 리스트 |

## 신호별 아이콘 매핑

| signal name | 아이콘 | 배경색 |
|-------------|--------|--------|
| twin_buy | ⚡ | 금색/보라 |
| twin_sell | 🔻 | 빨강 |
| retail_sacrifice | 🎯 | 보라 |
| stealth_acc | 🕵️ | 남색 |
| stealth_exit | ⛔ | 검정 |
| force_reversal | 🔄 | 초록 |
| retail_trap | 🪤 | 회색 |
| triple_buy | 👑 | 금색 |

## 점수별 색상/스타일

| 점수 범위 | 색상 | 의미 |
|-----------|------|------|
| +80 이상 | 금색 테두리 + 빨강 | 최강 신호 |
| +50 ~ +79 | 주황 | 강한 신호 |
| +20 ~ +49 | 노란 | 보통 |
| -19 ~ +19 | 회색 | 중립 |
| -20 ~ -49 | 연보라 | 주의 |
| -50 이하 | 빨강 테두리 + 검정 | 위험 |

## 시총(cap_억) 표시 변환
- 10,000억 이상 → "X.X조" (예: 465,000 → "46.5조")
- 1,000억 이상 → "X,XXX억" (예: 12,000 → "1.2조")
- 그 외 → "XXX억"

## 정렬
- 급등임박(surge): `score` 내림차순 (높은 점수 먼저)
- 매집(accumulate): `score` 내림차순
- 경고(warning): `score` 오름차순 (가장 위험한 것 먼저, 음수가 큰 순)

## 빈 데이터 처리
- 테이블에 데이터 없음: "사이클 감지 데이터 없음 (장마감 후 갱신)"
- `surge_items` 빈 배열: "급등임박 종목 없음"
- `accumulate_items` 빈 배열: "매집 감지 종목 없음"
- `warning_items` 빈 배열: "경고 종목 없음 — 시장 안정"

## 갱신 주기
- 매일 G7 Stage3 (16:30~17:00) 자동 갱신
- 텔레그램 `/사이클` 명령으로 수동 스캔 가능

## 핵심 컨셉 (사용자 설명 텍스트)
> 외인·기관·개인·기타법인 4세력의 수급 흐름을 분석해서
> 각 종목이 사이클의 어느 단계에 있는지 판정합니다.
>
> 🔥 **급등임박** = 세력이 모았고, 개인이 바쳤고, 터질 준비 완료
> 📦 **매집** = 조용히 모으는 중. 아직 때가 아님
> ⚠️ **고점경고** = 세력은 빠지고 개인만 남음. 탈출 검토
>
> **가장 강한 신호**: 쌍매수(⚡) + 기타법인 매집(🕵️) + 개인 바침(🎯) 동시 발생

## SQL 테이블 생성 (Supabase SQL Editor)
```sql
-- scalper-agent/sql/intelligence_cycle_scan_migration.sql 참조
CREATE TABLE IF NOT EXISTS intelligence_cycle_scan (
    date              DATE PRIMARY KEY,
    total_scanned     INTEGER DEFAULT 0,
    surge_count       INTEGER DEFAULT 0,
    accumulate_count  INTEGER DEFAULT 0,
    reversal_count    INTEGER DEFAULT 0,
    neutral_count     INTEGER DEFAULT 0,
    distribute_count  INTEGER DEFAULT 0,
    peak_warn_count   INTEGER DEFAULT 0,
    surge_items       JSONB DEFAULT '[]'::jsonb,
    accumulate_items  JSONB DEFAULT '[]'::jsonb,
    reversal_items    JSONB DEFAULT '[]'::jsonb,
    warning_items     JSONB DEFAULT '[]'::jsonb,
    top_surge_names   JSONB DEFAULT '[]'::jsonb,
    phase_summary     JSONB DEFAULT '{}'::jsonb,
    created_at        TIMESTAMPTZ DEFAULT now()
);

ALTER TABLE intelligence_cycle_scan ENABLE ROW LEVEL SECURITY;

CREATE POLICY "사이클감지_공개읽기"
    ON intelligence_cycle_scan FOR SELECT USING (true);

CREATE POLICY "사이클감지_서비스쓰기"
    ON intelligence_cycle_scan FOR ALL
    USING (auth.role() = 'service_role');

CREATE INDEX IF NOT EXISTS idx_cycle_scan_date
    ON intelligence_cycle_scan (date DESC);
```
