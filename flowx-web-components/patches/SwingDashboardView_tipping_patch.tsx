/**
 * SwingDashboardView.tsx 패치 지시
 *
 * 수급 임계점 패널을 Tier 3-C 수급 심층 분석에 4번째 탭으로 추가.
 * 기존 [수급사이클] [기관선매집] [매집레이더] 다음에 [임계점] 탭 추가.
 *
 * ──────────────────────────────────────────────────
 * 적용 방법: 아래 3개 블록을 SwingDashboardView.tsx에 적용
 * ──────────────────────────────────────────────────
 */

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// PATCH 1: import 추가 (파일 상단, 다른 import 근처)
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

import TippingPointPanel from "@/components/intelligence/TippingPointPanel";

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// PATCH 2: Tier 3-C 탭 목록에 "임계점" 추가
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

// 기존 코드 (예시):
//   const SUPPLY_TABS = ["수급사이클", "기관선매집", "매집레이더"];
// 변경 후:
//   const SUPPLY_TABS = ["수급사이클", "기관선매집", "매집레이더", "임계점"];

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// PATCH 3: 탭 렌더링 분기에 임계점 패널 추가
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

// 기존 Tier 3-C 수급 심층 분석 CollapsibleSection 내부,
// 매집레이더 패널 렌더 다음에 추가:

/*
  {activeSupplyTab === "임계점" && <TippingPointPanel />}
*/

// ──────────────────────────────────────────────────
// 전체 Tier 3-C 렌더링 예시 (참고용)
// ──────────────────────────────────────────────────

export function SupplyDeepDiveSection() {
  // 이 코드는 기존 3-C CollapsibleSection 안에 들어갑니다.
  // 아래는 탭 분기 전체 예시:

  const SUPPLY_TABS = ["수급사이클", "기관선매집", "매집레이더", "임계점"] as const;
  type SupplyTab = typeof SUPPLY_TABS[number];

  const [activeTab, setActiveTab] = useState<SupplyTab>("수급사이클");

  return (
    <>
      {/* 탭 바 */}
      <div style={{ display: "flex", gap: 4, marginBottom: 16 }}>
        {SUPPLY_TABS.map((tab) => (
          <button
            key={tab}
            onClick={() => setActiveTab(tab)}
            style={{
              flex: 1,
              padding: "8px 0",
              border: `1px solid ${activeTab === tab ? "#9ca3af" : "#1a2035"}`,
              borderRadius: 6,
              backgroundColor: activeTab === tab ? "#9ca3af18" : "transparent",
              color: activeTab === tab ? "#e8f0fa" : "#5a6a80",
              fontWeight: activeTab === tab ? 700 : 500,
              fontSize: 13,
              cursor: "pointer",
            }}
          >
            {tab}
          </button>
        ))}
      </div>

      {/* 탭 콘텐츠 */}
      {activeTab === "수급사이클" && <CycleScanPanel />}
      {activeTab === "기관선매집" && <StealthScannerPanel />}
      {activeTab === "매집레이더" && <AccumulationRadarPanel />}
      {activeTab === "임계점" && <TippingPointPanel />}
    </>
  );
}

// ──────────────────────────────────────────────────
// 주의사항
// ──────────────────────────────────────────────────
// 1. TippingPointPanel은 자체적으로 /api/intelligence/tipping-scan을 fetch합니다.
// 2. Wave 2 lazy load: CollapsibleSection이 펼쳐질 때만 렌더되므로
//    TippingPointPanel의 useEffect는 탭 활성화 시에만 실행됩니다.
// 3. CycleScanPanel, StealthScannerPanel, AccumulationRadarPanel은
//    기존 import를 그대로 유지합니다.
// 4. route.ts는 flowx-web-components/api/intelligence/tipping-scan/route.ts에
//    이미 생성되어 있습니다.
