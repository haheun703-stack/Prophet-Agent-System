/**
 * 메인 탭 네비게이션 6탭 재구성 지시
 *
 * 기존 4탭:
 *   [시장판단 & 전략] [매집 합류 시그널] [매매포인트] [수급추적]
 *
 * 변경 → 6탭:
 *   [시장판단 & 전략] [매매포인트] [상한가 엔진] [매집 합류 시그널] [수급 임계점] [수급추적]
 *
 * ──────────────────────────────────────────────
 * 순서 근거 (사용자 의사결정 흐름):
 *
 *   ① 시장판단 & 전략  — "시장 괜찮나?" (CONTEXT)
 *   ② 매매포인트        — "오늘 뭘 사지?" (ACTION)
 *   ③ 상한가 엔진       — "급등 후보는?" (AGGRESSIVE ACTION)
 *   ④ 매집 합류 시그널  — "누가 사고있나?" (SIGNAL)
 *   ⑤ 수급 임계점       — "코일 상태는?" (ANALYSIS)
 *   ⑥ 수급추적          — "수급 디테일" (DATA)
 *
 * ──────────────────────────────────────────────
 */

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// PATCH 1: 탭 상수 변경
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

// 기존:
// const MAIN_TABS = ["시장판단 & 전략", "매집 합류 시그널", "매매포인트", "수급추적"];

// 변경:
const MAIN_TABS = [
  "시장판단 & 전략",
  "매매포인트",
  "상한가 엔진",
  "매집 합류 시그널",
  "수급 임계점",
  "수급추적",
] as const;

type MainTab = typeof MAIN_TABS[number];

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// PATCH 2: import 추가
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

// 기존 import 근처에 추가:
import LimitUpEnginePanel from "@/features/swing/ui/LimitUpEnginePanel";
import TippingPointPanel from "@/features/swing/ui/TippingPointPanel";

// ※ 상한가 엔진 패널이 아직 독립 컴포넌트가 아니라면,
//   기존 SwingDashboardView 내의 상한가 엔진 섹션을
//   LimitUpEnginePanel.tsx로 분리 필요.

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// PATCH 3: 탭 콘텐츠 렌더링 분기
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

// 기존 탭 분기에 2개 추가:

/*
  {activeTab === "시장판단 & 전략" && <MarketStrategySection />}
  {activeTab === "매매포인트" && <TradePointsSection />}
  {activeTab === "상한가 엔진" && <LimitUpEnginePanel />}
  {activeTab === "매집 합류 시그널" && <AccumulationSignalSection />}
  {activeTab === "수급 임계점" && <TippingPointPanel />}
  {activeTab === "수급추적" && <SupplyTrackingSection />}
*/

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// PATCH 4: 상한가 엔진 — 기존 섹션에서 분리
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

// 상한가 엔진이 기존에 다른 탭(시장판단 또는 매매포인트) 내부에
// 서브섹션으로 들어가 있다면, 해당 부분을 제거하고
// "상한가 엔진" 탭에서만 렌더되도록 이동.
//
// 상한가 엔진 패널이 이미 LimitUpEnginePanel.tsx로 존재한다면
// import만 하면 됨.
// 아직 없다면 기존 상한가 관련 JSX를 LimitUpEnginePanel.tsx로 추출.

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// PATCH 5: 수급 임계점 — 기존 3-C 탭에서 제거
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

// 이전 패치(SwingDashboardView_tipping_patch.tsx)에서
// Tier 3-C 수급심층 4번째 탭으로 넣었는데,
// 이제 메인 탭으로 독립했으므로 3-C 내부에서 제거.
//
// 3-C 수급심층 탭은 기존 3탭으로 복원:
//   [수급사이클] [기관선매집] [매집레이더]

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// 참고: 탭 바 스타일 (6탭이므로 폰트 축소)
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

// 6탭은 모바일에서 좁을 수 있으므로:
// - fontSize: 13 → 12
// - padding: "8px 12px" → "8px 8px"
// - 또는 가로 스크롤: overflowX: "auto", whiteSpace: "nowrap"
//
// 추천: 모바일에서 가로 스크롤 허용

/*
<div style={{
  display: "flex",
  gap: 0,
  overflowX: "auto",
  WebkitOverflowScrolling: "touch",
  scrollbarWidth: "none",
}}>
  {MAIN_TABS.map((tab) => (
    <button
      key={tab}
      onClick={() => setActiveTab(tab)}
      style={{
        flex: "0 0 auto",
        padding: "10px 14px",
        fontSize: 13,
        fontWeight: activeTab === tab ? 700 : 500,
        color: activeTab === tab ? "#fff" : "#9ca3af",
        backgroundColor: activeTab === tab ? "#22c55e" : "transparent",
        border: "none",
        borderRadius: activeTab === tab ? 8 : 0,
        cursor: "pointer",
        whiteSpace: "nowrap",
        transition: "all 0.2s",
      }}
    >
      {tab}
    </button>
  ))}
</div>
*/
