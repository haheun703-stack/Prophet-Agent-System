# -*- coding: utf-8 -*-
"""5/19 장 마감 후 집계 — 시간대별 스냅샷 → 패턴 추출.

사용:
  python -X utf8 tools/surge_aggregator.py
"""
from __future__ import annotations

import sys
import json
import re
from pathlib import Path
from datetime import datetime
from collections import defaultdict

if sys.platform == "win32":
    for _s in ("stdout", "stderr"):
        s = getattr(sys, _s, None)
        if s and hasattr(s, "reconfigure"):
            try:
                s.reconfigure(encoding="utf-8", errors="replace")
            except Exception:
                pass

ROOT = Path(__file__).resolve().parent.parent
SNAP_DIR = ROOT / "data_store" / "surge_learning"
OUT_PATH = ROOT / "docs" / f"surge_report_{datetime.now().strftime('%Y%m%d')}.md"


def load_snapshots(date_str: str = None) -> list[dict]:
    """오늘 또는 지정일 스냅샷 로드 (시간순)."""
    date_str = date_str or datetime.now().strftime("%Y%m%d")
    pattern = re.compile(rf"^snap_{date_str}_\d{{4}}\.json$")
    files = sorted([f for f in SNAP_DIR.iterdir() if pattern.match(f.name)])
    snaps = []
    for f in files:
        try:
            snaps.append(json.loads(f.read_text(encoding="utf-8")))
        except Exception as e:
            print(f"로드 실패 {f.name}: {e}")
    return snaps


def aggregate(snaps: list[dict]) -> dict:
    """스냅샷 집계 — 시간대별 +5/+10/+상한가 종목 추적."""
    # 종목별 시간대 등락률 트래킹
    code_trace: dict[str, list[tuple[str, float, int]]] = defaultdict(list)  # code → [(time, change, vol)]
    name_map: dict[str, str] = {}

    surge_events_5 = []   # +5%+ 등장 첫 시각
    surge_events_10 = []  # +10%+ 등장 첫 시각
    limit_events = []     # +29%+ 등장 첫 시각
    seen_5: set[str] = set()
    seen_10: set[str] = set()
    seen_limit: set[str] = set()

    for snap in snaps:
        ts = snap["timestamp"]
        hh_mm = ts.split(" ")[1][:5] if " " in ts else ts[:5]
        for x in snap.get("fluctuation_top", []):
            code = x.get("code", "")
            name = x.get("name", "")
            ch = x.get("change_rate", 0)
            vol = x.get("volume", 0)
            if not code:
                continue
            name_map[code] = name
            code_trace[code].append((hh_mm, ch, vol))

            if ch >= 5 and code not in seen_5:
                seen_5.add(code); surge_events_5.append((hh_mm, code, name, ch))
            if ch >= 10 and code not in seen_10:
                seen_10.add(code); surge_events_10.append((hh_mm, code, name, ch))
            if ch >= 29 and code not in seen_limit:
                seen_limit.add(code); limit_events.append((hh_mm, code, name, ch))

    return {
        "snapshots_count": len(snaps),
        "time_range": (snaps[0]["timestamp"] if snaps else "", snaps[-1]["timestamp"] if snaps else ""),
        "code_trace": dict(code_trace),
        "name_map": name_map,
        "surge_5_first_appearance": surge_events_5,
        "surge_10_first_appearance": surge_events_10,
        "limit_first_appearance": limit_events,
        "unique_5p_plus": len(seen_5),
        "unique_10p_plus": len(seen_10),
        "unique_limit": len(seen_limit),
    }


def write_report(agg: dict, snaps: list[dict]):
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    L = []
    L.append(f"# 5/19 D-Day 급등주 집계 보고서")
    L.append(f"")
    L.append(f"**생성**: {datetime.now().isoformat()}")
    L.append(f"**스냅샷 수**: {agg['snapshots_count']}")
    L.append(f"**시간 범위**: {agg['time_range'][0]} ~ {agg['time_range'][1]}")
    L.append(f"")
    L.append(f"## 요약")
    L.append(f"- **상한가 (+29%+)**: {agg['unique_limit']}건")
    L.append(f"- **+10%+ 급등**: {agg['unique_10p_plus']}건")
    L.append(f"- **+5%+**: {agg['unique_5p_plus']}건")
    L.append(f"")

    L.append(f"## 1. 상한가 종목 첫 등장 시각")
    L.append(f"")
    L.append("| 시각 | 코드 | 종목명 | 등락률 |")
    L.append("|------|------|--------|--------|")
    for hh_mm, code, name, ch in agg["limit_first_appearance"]:
        L.append(f"| {hh_mm} | {code} | {name} | +{ch:.2f}% |")
    L.append("")

    L.append(f"## 2. +10%+ 종목 첫 등장 시각 (TOP 30)")
    L.append(f"")
    L.append("| 시각 | 코드 | 종목명 | 등락률 |")
    L.append("|------|------|--------|--------|")
    for hh_mm, code, name, ch in agg["surge_10_first_appearance"][:30]:
        L.append(f"| {hh_mm} | {code} | {name} | +{ch:.2f}% |")
    L.append("")

    L.append(f"## 3. 종목별 시간대 등락 궤적 (상한가 도달 종목만)")
    L.append(f"")
    for code in [c for _, c, _, _ in agg["limit_first_appearance"]]:
        trace = agg["code_trace"].get(code, [])
        name = agg["name_map"].get(code, "?")
        L.append(f"### {name} ({code})")
        L.append("")
        L.append("| 시각 | 등락률 | 거래량 |")
        L.append("|------|--------|--------|")
        for hh_mm, ch, vol in trace:
            L.append(f"| {hh_mm} | +{ch:.2f}% | {vol:,} |")
        L.append("")

    OUT_PATH.write_text("\n".join(L), encoding="utf-8")
    print(f"보고서 저장: {OUT_PATH}")


if __name__ == "__main__":
    snaps = load_snapshots()
    print(f"스냅샷 {len(snaps)}개 로드")
    if not snaps:
        print("스냅샷 없음 — surge_loop_tracker.py 가동 필요")
        sys.exit(0)
    agg = aggregate(snaps)
    print(f"상한가: {agg['unique_limit']} / +10%+: {agg['unique_10p_plus']} / +5%+: {agg['unique_5p_plus']}")
    write_report(agg, snaps)
