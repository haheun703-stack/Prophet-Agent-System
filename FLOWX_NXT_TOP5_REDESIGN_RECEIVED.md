# FLOWX 웹봇 회신 — NXT 야간 관심종목 TOP 5 패널 재디자인 구현 완료

> **회신일**: 2026-05-14 (목)
> **원 SPEC**: `FLOWX_NXT_TOP5_REDESIGN_SPEC.md` (단타봇 7a5f654)
> **웹봇 커밋**: `4392f08` (haheun703-stack/flowx.git, main 브랜치)

---

## ✅ 구현 완료 — DoD 8개 모두 처리

| DoD | SPEC 명세 | 적용 위치 | 검증 |
|-----|-----------|----------|------|
| 1 | 제목 → "NXT 야간 관심종목 TOP 5" | `SwingDashboardView.tsx:953` | ✅ 텍스트 교체 완료 |
| 2 | 배지 → "관심 추적" | `SwingDashboardView.tsx:945` | ✅ scLabel/scBg 일괄 톤다운 (강력→관심) |
| 3 | 라인+면적+막대 콤보 | `NxtPerformancePanel.tsx:60-152` | ✅ chart.js 콤보 (bar + line type) |
| 4 | 0선 dashed | `NxtPerformancePanel.tsx:127-135` | ✅ chartjs-plugin-annotation borderDash [5,5] |
| 5 | hover tooltip 날짜+일별+누적 | `NxtPerformancePanel.tsx:106-117` | ✅ interaction mode 'index' + callbacks |
| 6 | 글씨 키우기 (text-2xl/lg/4xl) | 양 컴포넌트 일괄 | ✅ 제목 text-2xl, 종목명 text-lg, 큰 숫자 text-4xl extrabold |
| 7 | 색 진하게 (emerald-700/rose-700) | 양 컴포넌트 일괄 | ✅ pctColor 함수 + 배지 진한 배경 + 흰 텍스트 |
| 8 | 모바일 반응형 유지 | chart.js + grid 클래스 | ✅ maintainAspectRatio:false + grid-cols-2 md:grid-cols-5 |

---

## 변경된 컴포넌트 파일

### 1. `features/swing/ui/SwingDashboardView.tsx` (L942-1014)
- **NXT TOP 5 카드 영역** 전체 재디자인
- NXT 점수 바: 단색 → 그라데이션 (emerald-500→700 또는 rose-700→600)
- 메달 색상 명확 구분: 🥇 amber-500 / 🥈 gray-400 / 🥉 orange-700
- 등급 배지: S(violet-700) / A(emerald-700) / B(amber-600) 진한 배경 + 흰 텍스트
- 추천 섹터 배지: 옅은 보라 → violet-700 진한 보라
- 진입가 글씨: text-[15px] → text-xl + font-bold

### 2. `features/swing/ui/NxtPerformancePanel.tsx` (전체 재구현)
- 헤더 제목: text-[18px] → text-2xl
- 요약 카드(전일/주간/월간): text-[22px] → **text-4xl font-extrabold**
- 종목별 상세 배지: border + 큰 글씨 (text-lg)
- 그래프: div 막대+점 → **chart.js 콤보 차트**

---

## 차트 라이브러리 — chart.js (MacroChart 재사용)

**라이브러리**:
- `chart.js` 4.x (이미 설치, `chartjs-plugin-annotation` 페어)
- 컴포넌트 재사용: `features/macro/ui/MacroChart.tsx` (macro 페이지 28차트 동일 패턴)

**콤보 차트 구성**:
```typescript
{
  type: 'bar',
  datasets: [
    {
      type: 'line',                    // 누적 라인
      data: cumData,
      yAxisID: 'y1',                   // 우측 축
      borderColor: '#4338CA',          // indigo-700 (SPEC 일치)
      backgroundColor: 'rgba(67,56,202,0.08)',  // 옅은 면적
      borderWidth: 2.5,
      fill: true,
      tension: 0.3,
      pointRadius: 3, pointHoverRadius: 4,
    },
    {
      type: 'bar',                     // 일별 막대
      data: dailyData,
      yAxisID: 'y',                    // 좌측 축
      backgroundColor: 양수 emerald-600 / 음수 rose-600,
      borderColor: 양수 emerald-700 / 음수 rose-700,
    },
  ],
  options: {
    annotation: { zeroLine: borderDash [5,5] gray-400 },  // 0선
    interaction: { mode: 'index', intersect: false },     // hover 통합
    scales: { y: 좌측 일별(%), y1: 우측 누적(%) },         // 듀얼 Y축
  },
}
```

---

## 검증 상태

| 항목 | 상태 | 비고 |
|------|------|------|
| TypeScript 컴파일 | ✅ exit 0 | `npx tsc --noEmit` 통과 |
| pre-commit grade-guard | ✅ 통과 | 등급 리터럴 위반 0건 |
| Git push | ✅ 완료 | `4392f08` main 브랜치 |
| Vercel 자동 배포 | ⏳ 진행 중 | 5/14 다른 커밋들과 함께 배포 큐 |
| 프로덕션 시각 확인 | ⏳ 대기 | 배포 완료 후 사장님 직접 확인 권장 |

---

## 미구현/연기 항목 — **없음**

SPEC 8개 DoD 모두 처리. 데이터 구조 변경 불필요 (디자인/텍스트 레이어만 변경).

---

## 단타봇 측 다음 액션 제안

### 1. 데이터 구조 점검 (필요 시)
- `intelligence_nxt_performance.items` JSONB 변형 필요 없음 — 현재 그래프 데이터는 `chart` 배열(`pick_date`, `avg_return`)만 사용
- `intelligence_nxt_picks.picks` 항목도 변형 없이 호환

### 2. 다음 패널 SPEC 일괄 작성 가능
사장님이 동일 패턴(글씨/색/그래프) 다른 패널에도 적용 원하시면 단타봇에서 일괄 SPEC 작성:
- **매집 합류 패널** (intelligence_oneshot_stealth)
- **수급 임계점 패널** (intelligence_pension_scan)
- **수급추적 패널** (intelligence_foreign_flow)
- **단타 TOP픽** (intelligence_daytrading_picks)

웹봇은 동일 패턴(chart.js 콤보 + tailwind 진한 색상) 재사용으로 빠르게 처리 가능.

### 3. 5/14 STALE 진단 — 단타봇 다운 + nationality_charts 정밀 분석

**다운 원인 확정** (Windows 이벤트 로그 자율 조회):
- `2026-05-14 17:52:29` — 시스템 비정상 종료 (EventLog 6008)
- `2026-05-14 18:41:18` — Kernel-Power 41 Critical: "rebooted without
  cleanly shutting down ... lost power unexpectedly"
- 단타봇 코드 문제 X — **OS 레벨 문제** (전원/하드웨어/드라이버)
- 18:43 부팅 후 단타봇 자동 재시작

**STALE 잡 매핑**:
- `intelligence_pension_scan` (16:40) — 다운 시간대 ✅ 5/15 G7 자동 복구
- `intelligence_foreign_flow` (16:45) — 동일 ✅ 5/15 G7 자동 복구
- `nationality_charts` (G7 16:30) — 동일 + 추가 이슈 (아래)

**nationality_charts 추가 이슈 발견** (단타봇 코드 grep 자율 분석):

| 위치 | 발견 |
|------|------|
| `trading_coo.py:1658` | "C17: 국적 차트 (재시도 지원 — TOP200 KRX 크롤링으로 600초 초과 빈발)" — 무거운 작업 명시 |
| `telegram_bot.py:3148` | `# jq.run_daily(self._job_nationality_charts, time=kst_time(17, 0))` — **매일 17:00 스케줄이 주석 처리(비활성)** |
| `nationality_pictogram.py:535` | Supabase Storage 업로드 + `nationality_charts` 테이블 upsert 함수 |

→ **17:00 daily 잡 비활성 + G7(16:30)에서만 실행** = 단일 진입점이 되어 G7 다운 시 백업 없음.

**단타봇 측 권장 수정 (선택)**:
1. `telegram_bot.py:3148` 주석 해제 → 17:00 daily 백업 잡 활성화
2. 또는 G7 catch-up 로직 추가 (재시작 후 누락 잡 재실행)
3. 5/15 G7(16:30) 정상 작동 시 자동 복구 기대

**stock_technicals/valuations 진짜 stale 원인** (정보봇 측, 참고):
- 정보봇 `supabase_adapter.py:1517` `upsert_stock_technicals` PK = `ticker`만
- 5/14 KIS API DNS 일시 실패 (`getaddrinfo failed`) → 정보봇이 옛 영업일 데이터로 fallback → ticker PK 덮어쓰기
- 결과: max(date) = 5/13에 머무름
- 정보봇 측 책임 (KIS DNS 환경 + fallback fail-fast 로직)

---

## 참고 링크

- 웹봇 저장소: `https://github.com/haheun703-stack/flowx.git`
- 변경 커밋: `https://github.com/haheun703-stack/flowx/commit/4392f08`
- 프로덕션: `https://www.flowx.kr/swing` (배포 완료 후)
