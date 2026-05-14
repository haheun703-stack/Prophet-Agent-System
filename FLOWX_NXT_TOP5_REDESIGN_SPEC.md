# FLOWX 웹봇 지시서 — NXT 야간 관심종목 TOP 5 패널 재디자인

> **프로젝트**: Body Hunter v4 — 한국 주식 단타봇(자동매매 시스템)
> KIS API(한국투자증권) 실계좌 매매, 텔레그램 봇 UI, 7-Group COO 파이프라인,
> BRAIN 시장분석, NXT 야간매매, FLOWX VIP 콘텐츠를 포함합니다.
> CFO(재무)/CTO(기술)/COO(운영) 3-Officer 구조로 운영됩니다.

> **요청자**: 단타봇 (Prophet Agent System)
> **요청일**: 2026-05-14
> **대상 페이지**: 한국 스윙 페이지 — `NXT 야간매수 TOP 5` 패널 + 그 아래 `NXT 추천 성적표`

---

## 배경

사장님이 5/14 화면 캡처를 보고 4가지 변경 요청:

1. **제목 변경** — "NXT 야간매수 TOP 5"는 매수 강제 인상이 강함. **"관심종목"**으로 톤다운하여 자동매수 강제 인상 제거
2. **일별 수익률 그래프** — 현재 점(dot)으로만 표시되어 시각적 흐름 파악 어려움. **시연성 있는 라인/막대 차트**로 교체
3. **글씨 크기 키우기** — 종목명/수치 가독성 개선
4. **색 진하게** — 등급/수익률 색상 채도 강화

데이터 자체는 단타봇이 매일 정상 공급 중 (`intelligence_nxt_picks` + `intelligence_nxt_performance`). 디자인/텍스트 레이어만 변경 필요.

---

## 변경 사항 명세

### 1. 제목 변경 (텍스트만)

| 위치 | 현재 | 변경 |
|------|------|------|
| 상단 패널 헤더 | `🍌 NXT 야간매수 TOP 5` | `🍌 NXT 야간 관심종목 TOP 5` |
| 상태 배지 | `🟢 매수 고려` | `🟢 관심 추적` (유지하되 워딩 톤다운) |

이유: "매수"라는 강한 행위 동사 제거. 사장님이 단순 참고용 종목 추적 목적임을 명확히.

### 2. 일별 수익률 그래프 — 점(scatter) → **라인+면적 차트**로 교체

**현재 문제**:
- 점선(dotted scatter)으로만 표시되어 추세 파악 어려움
- 양수/음수 누적 구분 시각적으로 약함

**변경 명세**:

```
[권장 차트 타입]
- 메인: 라인 차트 (line) + 라인 하부 면적(area) 옅게 채움
- 보조: 일별 수익률 막대 (양수 초록 / 음수 빨강) — 라인 차트와 동일 X축에 겹쳐 표시 (combo)
- 0선: 회색 점선으로 항상 표시

[색상]
- 양수 라인/면적: emerald-600 (#059669) — 현재보다 진하게
- 음수 라인/면적: rose-600  (#e11d48)
- 누적 라인: indigo-700 (#4338ca) 굵게 (stroke-width 2.5)
- 0선: gray-400 dashed

[축]
- Y축 좌측: 일별 수익률 (%) — 영역 -5% ~ +10% 자동 fit
- Y축 우측: 누적 수익률 (%) — 0% 부터 시작, 자동 fit
- X축: pick_date (MM-DD 포맷) — 최근 20거래일

[마커]
- 각 데이터 포인트에 작은 원 (radius 3) + hover 시 4로 확대
- hover tooltip: 날짜 + 일별 수익률 + 누적 수익률
```

**라이브러리**: recharts, victory, chart.js, ECharts 중 현재 FLOWX 프로젝트에서 사용 중인 것으로.

### 3. 글씨 크기 키우기

| 요소 | 현재 추정 | 변경 |
|------|----------|------|
| 패널 제목 | text-lg (18px) | **text-2xl (24px)** + font-bold |
| 종목명 (SK이터닉스 등) | text-base (16px) | **text-lg (18px)** + font-semibold |
| 종목코드 (475150 등) | text-xs (12px) | **text-sm (14px)** |
| 진입가 (51,400원) | text-base | **text-xl (20px)** + font-bold |
| 수급 점수 (130점 등) | text-sm | **text-base (16px)** + font-bold |
| 성적표 큰 숫자 (+1.56% 등) | text-3xl | **text-4xl (36px)** + font-extrabold |
| 일별/주간/월간 라벨 | text-sm | **text-base** + font-medium |

### 4. 색 진하게

| 요소 | 현재 | 변경 |
|------|------|------|
| 양수 수익률 텍스트 | green-500 | **emerald-700** (#047857) |
| 음수 수익률 텍스트 | red-500 | **rose-700** (#be123c) |
| 등급 뱃지 (S/A/B) | 옅은 배경 + 흐린 텍스트 | **진한 배경 + 흰 텍스트** (S=violet-700, A=emerald-700, B=amber-600) |
| 메달 아이콘 (🏅) 색 | 기본 | **gold/silver/bronze 명확 구분** |
| "매집초기(미발화)" 같은 보조 라벨 | gray-500 | **gray-700 + font-medium** |
| 점수 막대 (NXT 점수 +4.8) | 옅은 초록 | **emerald-600 → emerald-500 그라데이션** |

---

## 데이터 소스 (참고용 — 변경 없음)

### Supabase 테이블

```
intelligence_nxt_picks
  PK: date
  컬럼: date, picks (JSONB array), top_sectors (JSONB), score_total, generated_at
  picks 항목: {rank, code, name, sector, supply_score, entry_price, phase, ...}

intelligence_nxt_performance
  PK: pick_date
  컬럼: pick_date, result_date, avg_return, best_pick, worst_pick,
        weekly_return, weekly_days, weekly_wins,
        monthly_return, monthly_days, monthly_wins,
        items (JSONB array), created_at
  items 항목: {code, name, ret_pct}
```

### 일별 수익률 그래프 데이터 매핑

```
X축: intelligence_nxt_performance.pick_date (최근 20개)
양수/음수 막대: avg_return (양수면 emerald, 음수면 rose)
누적 라인: avg_return cumulative sum
```

---

## 기대 결과 (DoD)

1. [ ] 패널 제목 `NXT 야간 관심종목 TOP 5`로 변경됨
2. [ ] 상태 배지 `매수 고려` → `관심 추적`
3. [ ] 일별 수익률 그래프가 라인+면적 + 막대 콤보 차트로 표시됨
4. [ ] 0선이 dashed로 항상 표시됨
5. [ ] hover 시 tooltip이 날짜 + 일별 + 누적 수익률 표시
6. [ ] 글씨 크기 명세대로 적용 (제목 text-2xl, 종목명 text-lg 등)
7. [ ] 색상 명세대로 적용 (양수 emerald-700, 음수 rose-700)
8. [ ] 모바일 반응형 깨지지 않음

---

## 구현 후 단타봇에게 회신 요청

웹봇 구현 완료 후 다음 정보 회신:
1. 변경된 컴포넌트 파일 경로
2. 스크린샷 (5/14 데이터 기준)
3. 사용한 차트 라이브러리
4. 미구현/연기 항목 있으면 사유

회신 받은 후 단타봇에서:
- 데이터 누락(예: items JSONB 변형 필요) 발견 시 supabase 컬럼 추가 또는 upload_nxt_performance.py 수정
- 다음 패널(매집 합류, 수급 임계점, 수급추적 등) 동일 패턴으로 일괄 재디자인 가능
