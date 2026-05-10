/**
 * 수급 임계점 스캐너 패널
 * Tier 3-C [임계점] 탭
 *
 * 코일/점화/이륙 3탭 + 종목카드(5대 지표 바) + 최근 3일 수급 테이블
 */
"use client";

import { useState, useEffect, useMemo } from "react";

/* ── Theme (FLOWX 공통) ─────────────────────────── */
const T = {
  bg: "#06080e",
  panel: "#0c1018",
  border: "#1a2035",
  borderHi: "#2a3555",
  text: "#c8d6e5",
  textDim: "#5a6a80",
  textBright: "#e8f0fa",
  accent: "#00e5ff",
  danger: "#ff3b5c",
  warn: "#ffa726",
  safe: "#00e676",
  blue: "#448aff",
  gold: "#ffd740",
};

/* ── Types ───────────────────────────────────────── */
interface RecentDay {
  date: string;
  frgn: number;
  inst: number;
  pension: number;
  fi: number;
  indi: number;
  close: number;
}

interface TippingStock {
  code: string;
  name: string;
  sector: string;
  market: string;
  cap: number;
  close: number;
  chg_5d: number;
  phase: "코일" | "점화" | "이륙";
  tipping_score: number;
  accum_intensity: number;
  accum_score: number;
  sell_exhaustion: number;
  sell_score: number;
  vol_compression: number;
  vol_score: number;
  cross_count: number;
  today_buyers: string[];
  cross_score: number;
  continuation_prob: number;
  cont_score: number;
  smart_consec: number;
  frgn_consec: number;
  inst_consec: number;
  pension_consec: number;
  dual_buy: boolean;
  pattern: string;
  price_sensitivity: number;
  recent_3d: RecentDay[];
}

interface TippingScanData {
  date: string;
  total_scanned: number;
  coiled_count: number;
  warming_count: number;
  launched_count: number;
  coiled: TippingStock[];
  warming: TippingStock[];
  launched: TippingStock[];
}

type PhaseTab = "코일" | "점화" | "이륙";

/* ── Phase config ────────────────────────────────── */
const PHASE_CFG: Record<PhaseTab, { color: string; label: string; emptyMsg: string }> = {
  코일: { color: T.safe, label: "코일", emptyMsg: "오늘은 임계점 도달 종목이 없습니다" },
  점화: { color: T.warn, label: "점화", emptyMsg: "점화 단계 종목이 없습니다" },
  이륙: { color: T.blue, label: "이륙", emptyMsg: "이륙 단계 종목이 없습니다" },
};

/* ── Helpers ──────────────────────────────────────── */
const fmt = (n: number) => n.toLocaleString("ko-KR");
const fmtCap = (cap: number) => (cap >= 10000 ? `${(cap / 10000).toFixed(1)}조` : `${fmt(cap)}억`);
const sign = (n: number) => (n > 0 ? `+${n}` : `${n}`);

function scoreColor(score: number): string {
  if (score >= 70) return T.danger;
  if (score >= 50) return T.warn;
  return T.gold;
}

function cardBorder(score: number): string {
  if (score >= 70) return T.gold;
  if (score >= 50) return T.warn;
  return T.border;
}

/* ── Sub-components ──────────────────────────────── */

function PhaseBadge({ phase }: { phase: PhaseTab }) {
  const cfg = PHASE_CFG[phase];
  return (
    <span
      style={{
        display: "inline-block",
        padding: "2px 10px",
        borderRadius: 4,
        fontSize: 12,
        fontWeight: 700,
        color: "#fff",
        backgroundColor: cfg.color,
      }}
    >
      {cfg.label}
    </span>
  );
}

function IndicatorBar({
  label,
  value,
  score,
  maxScore,
  color,
  suffix,
}: {
  label: string;
  value: string;
  score: number;
  maxScore: number;
  color: string;
  suffix?: string;
}) {
  const pct = Math.min((score / maxScore) * 100, 100);
  return (
    <div style={{ flex: "1 1 160px", minWidth: 140 }}>
      <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 3 }}>
        <span style={{ fontSize: 11, color: T.textDim }}>
          {label} <span style={{ color: T.textBright, fontWeight: 600 }}>{value}{suffix || ""}</span>
        </span>
        <span style={{ fontSize: 10, color: T.textDim }}>{score}/{maxScore}</span>
      </div>
      <div style={{ height: 6, borderRadius: 3, backgroundColor: T.border, overflow: "hidden" }}>
        <div
          style={{
            height: "100%",
            width: `${pct}%`,
            borderRadius: 3,
            backgroundColor: color,
            transition: "width 0.4s ease",
          }}
        />
      </div>
    </div>
  );
}

function FlowCell({ val }: { val: number }) {
  if (val === 0) return <td style={{ ...cellStyle, color: T.textDim }}>—</td>;
  const color = val > 0 ? T.danger : T.safe;
  return (
    <td style={{ ...cellStyle, color, fontWeight: val !== 0 ? 600 : 400 }}>
      {sign(val)}
    </td>
  );
}

const cellStyle: React.CSSProperties = {
  padding: "4px 6px",
  fontSize: 11,
  textAlign: "right",
  whiteSpace: "nowrap",
  borderBottom: `1px solid ${T.border}`,
};

const thStyle: React.CSSProperties = {
  ...cellStyle,
  color: T.textDim,
  fontWeight: 500,
  textAlign: "right",
  position: "sticky" as const,
  top: 0,
  backgroundColor: T.panel,
};

/* ── Stock Card ──────────────────────────────────── */

function StockCard({ stock, expanded, onToggle }: { stock: TippingStock; expanded: boolean; onToggle: () => void }) {
  const border = cardBorder(stock.tipping_score);

  // 지표별 색상
  const accumColor = stock.accum_score >= 20 ? T.danger : stock.accum_score >= 10 ? T.warn : T.textDim;
  const sellColor = stock.sell_exhaustion <= 0.5 ? T.safe : stock.sell_exhaustion <= 1.0 ? T.warn : T.danger;
  const volColor = stock.vol_compression <= 0.7 ? T.blue : stock.vol_compression <= 1.0 ? T.accent : T.textDim;
  const crossColor = stock.cross_count >= 3 ? T.danger : stock.cross_count >= 2 ? T.warn : T.textDim;
  const contColor = stock.continuation_prob >= 55 ? T.safe : stock.continuation_prob >= 45 ? T.warn : T.textDim;

  return (
    <div
      style={{
        backgroundColor: T.panel,
        border: `1px solid ${border}`,
        borderRadius: 8,
        padding: 16,
        marginBottom: 12,
        boxShadow: stock.tipping_score >= 70 ? `0 0 12px ${T.gold}22` : undefined,
      }}
    >
      {/* Header */}
      <div
        style={{ display: "flex", justifyContent: "space-between", alignItems: "center", cursor: "pointer" }}
        onClick={onToggle}
      >
        <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
          <PhaseBadge phase={stock.phase} />
          <span style={{ fontWeight: 700, fontSize: 15, color: T.textBright }}>{stock.name}</span>
          <span style={{ fontSize: 12, color: T.textDim }}>({stock.code})</span>
        </div>
        <span
          style={{
            fontSize: 20,
            fontWeight: 800,
            color: scoreColor(stock.tipping_score),
          }}
        >
          {stock.tipping_score}점
        </span>
      </div>

      {/* Sub-header */}
      <div style={{ fontSize: 12, color: T.textDim, marginTop: 4 }}>
        {stock.sector} · {stock.market} · 시총 {fmtCap(stock.cap)} · 종가 {fmt(stock.close)}
        <span style={{ marginLeft: 12, color: stock.chg_5d >= 0 ? T.danger : T.safe }}>
          5D {stock.chg_5d >= 0 ? "+" : ""}{stock.chg_5d.toFixed(1)}%
        </span>
      </div>

      {/* 5-indicator bars */}
      <div style={{ display: "flex", flexWrap: "wrap", gap: 12, marginTop: 14 }}>
        <IndicatorBar label="매집" value={stock.accum_intensity.toFixed(1)} suffix="x" score={stock.accum_score} maxScore={30} color={accumColor} />
        <IndicatorBar label="소진" value={stock.sell_exhaustion.toFixed(2)} score={stock.sell_score} maxScore={20} color={sellColor} />
        <IndicatorBar label="압축" value={stock.vol_compression.toFixed(2)} score={stock.vol_score} maxScore={15} color={volColor} />
        <IndicatorBar label="합류" value={`${stock.cross_count}자`} score={stock.cross_score} maxScore={15} color={crossColor} />
        <IndicatorBar label="지속" value={`${stock.continuation_prob.toFixed(0)}%`} score={stock.cont_score} maxScore={20} color={contColor} />
      </div>

      {/* Pattern + today buyers */}
      <div style={{ fontSize: 12, color: T.textDim, marginTop: 10 }}>
        패턴: <span style={{ color: T.accent }}>{stock.pattern}</span>
        {stock.today_buyers.length > 0 && (
          <>
            {" · 오늘 매수: "}
            <span style={{ color: T.textBright }}>{stock.today_buyers.join(", ")}</span>
          </>
        )}
        {stock.dual_buy && (
          <span style={{ marginLeft: 8, color: T.danger, fontWeight: 700 }}>🔥 쌍매수</span>
        )}
      </div>

      {/* Expandable: 최근 3일 수급 */}
      {expanded && stock.recent_3d && stock.recent_3d.length > 0 && (
        <div style={{ marginTop: 12, overflowX: "auto" }}>
          <div style={{ fontSize: 11, color: T.textDim, marginBottom: 4 }}>── 최근 3일 수급 ──</div>
          <table style={{ width: "100%", borderCollapse: "collapse" }}>
            <thead>
              <tr>
                <th style={{ ...thStyle, textAlign: "left" }}>날짜</th>
                <th style={thStyle}>종가</th>
                <th style={thStyle}>외인</th>
                <th style={thStyle}>기관</th>
                <th style={thStyle}>연기금</th>
                <th style={thStyle}>금투</th>
                <th style={thStyle}>개인</th>
              </tr>
            </thead>
            <tbody>
              {stock.recent_3d.map((d) => (
                <tr key={d.date}>
                  <td style={{ ...cellStyle, textAlign: "left", color: T.textDim }}>{d.date.slice(5)}</td>
                  <td style={{ ...cellStyle, color: T.textBright }}>{fmt(d.close)}</td>
                  <FlowCell val={d.frgn} />
                  <FlowCell val={d.inst} />
                  <FlowCell val={d.pension} />
                  <FlowCell val={d.fi} />
                  <FlowCell val={d.indi} />
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {/* Expand toggle hint */}
      <div
        onClick={onToggle}
        style={{
          textAlign: "center",
          cursor: "pointer",
          fontSize: 11,
          color: T.textDim,
          marginTop: 6,
          userSelect: "none",
        }}
      >
        {expanded ? "▲ 접기" : "▼ 3일 수급 보기"}
      </div>
    </div>
  );
}

/* ── Main Panel ──────────────────────────────────── */

export default function TippingPointPanel() {
  const [data, setData] = useState<TippingScanData | null>(null);
  const [loading, setLoading] = useState(true);
  const [activeTab, setActiveTab] = useState<PhaseTab>("코일");
  const [expandedCode, setExpandedCode] = useState<string | null>(null);

  useEffect(() => {
    fetch("/api/intelligence/tipping-scan")
      .then((r) => r.json())
      .then((d) => {
        setData(d);
        // 자동 탭 선택: 코일 없으면 점화, 점화 없으면 이륙
        if ((!d.coiled || d.coiled.length === 0) && d.warming?.length > 0) setActiveTab("점화");
        else if ((!d.coiled || d.coiled.length === 0) && (!d.warming || d.warming.length === 0)) setActiveTab("이륙");
      })
      .catch(() => setData(null))
      .finally(() => setLoading(false));
  }, []);

  const stocks = useMemo<TippingStock[]>(() => {
    if (!data) return [];
    if (activeTab === "코일") return data.coiled || [];
    if (activeTab === "점화") return data.warming || [];
    return data.launched || [];
  }, [data, activeTab]);

  const counts = useMemo(() => ({
    코일: data?.coiled_count ?? 0,
    점화: data?.warming_count ?? 0,
    이륙: data?.launched_count ?? 0,
  }), [data]);

  if (loading) {
    return (
      <div style={{ padding: 24, color: T.textDim, textAlign: "center" }}>
        수급 임계점 데이터 로딩 중...
      </div>
    );
  }

  if (!data || !data.date) {
    return (
      <div style={{ padding: 24, color: T.textDim, textAlign: "center" }}>
        임계점 데이터가 없습니다. 파이프라인 가동 후 표시됩니다.
      </div>
    );
  }

  return (
    <div>
      {/* Header */}
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 12 }}>
        <div>
          <span style={{ fontSize: 16, fontWeight: 700, color: T.textBright }}>수급 임계점 스캐너</span>
          <span style={{ fontSize: 12, color: T.textDim, marginLeft: 8 }}>
            {data.date} · {data.total_scanned}종목 분석
          </span>
        </div>
        <div style={{ fontSize: 11, color: T.textDim }}>
          코일 {counts.코일} · 점화 {counts.점화} · 이륙 {counts.이륙}
        </div>
      </div>

      {/* Tab bar */}
      <div style={{ display: "flex", gap: 4, marginBottom: 16 }}>
        {(["코일", "점화", "이륙"] as PhaseTab[]).map((tab) => {
          const active = tab === activeTab;
          const cfg = PHASE_CFG[tab];
          return (
            <button
              key={tab}
              onClick={() => setActiveTab(tab)}
              style={{
                flex: 1,
                padding: "8px 0",
                border: `1px solid ${active ? cfg.color : T.border}`,
                borderRadius: 6,
                backgroundColor: active ? `${cfg.color}18` : "transparent",
                color: active ? cfg.color : T.textDim,
                fontWeight: active ? 700 : 500,
                fontSize: 13,
                cursor: "pointer",
                transition: "all 0.2s",
              }}
            >
              {cfg.label} {counts[tab]}
            </button>
          );
        })}
      </div>

      {/* Stock list */}
      {stocks.length === 0 ? (
        <div style={{ padding: 40, textAlign: "center", color: T.textDim, fontSize: 14 }}>
          {PHASE_CFG[activeTab].emptyMsg}
        </div>
      ) : (
        <div>
          {stocks.map((s) => (
            <StockCard
              key={s.code}
              stock={s}
              expanded={expandedCode === s.code}
              onToggle={() => setExpandedCode(expandedCode === s.code ? null : s.code)}
            />
          ))}
        </div>
      )}

      {/* Footer disclaimer */}
      <div style={{ fontSize: 10, color: T.textDim, marginTop: 12, textAlign: "center" }}>
        임계점 = 확률적 상태이며 매수 추천이 아닙니다. BRAIN 시장판단과 교차 확인 필수.
      </div>
    </div>
  );
}
