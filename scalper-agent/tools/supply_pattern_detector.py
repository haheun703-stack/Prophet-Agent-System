# -*- coding: utf-8 -*-
"""
T3 수급 패턴 감지기
====================
2026-04-16 작성

목적:
- 급등주(+3% 이상)의 수급 구조를 자동 분류
- 쌍매수/외인 단독/기관 단독/기타법인 매수/기타법인 매도/개인 주도/수급 이탈 7종 분류
- 5D/10D/20D 누적 수급 제공
- 11주체 스키마 호환 (현재 4주체, OpenAPI 승인 후 자동 확장)

입력:
- data_store/learning/missed_gainers/{YYYY-MM-DD}.json
- 또는 코드 리스트 직접 전달

출력:
- data_store/learning/pattern_scan/{YYYY-MM-DD}.json
- 콘솔 표 출력

사용:
    python tools/supply_pattern_detector.py              # 오늘 missed_gainers 분석
    python tools/supply_pattern_detector.py --date 2026-04-16
    python tools/supply_pattern_detector.py --codes 077360,009580,356680
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd

# ── 프로젝트 경로 ──
SCALPER_DIR = Path(__file__).resolve().parent.parent
REPO_DIR = SCALPER_DIR.parent

sys.path.insert(0, str(SCALPER_DIR))
from data.supply_flow_schema import (  # noqa: E402
    ENTITY_COLUMNS,
    ENTITY_KR,
    AVAILABLE_ENTITIES,
    to_canonical,
)

# ── 경로 ──
FLOW_DIR = SCALPER_DIR / "data_store" / "flow"
MISSED_DIR = SCALPER_DIR / "data_store" / "learning" / "missed_gainers"
OUTPUT_DIR = SCALPER_DIR / "data_store" / "learning" / "pattern_scan"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ── 패턴 임계값 (억원 단위) ──
# 실데이터 기반 튜닝 (2026-04-16 덕산/무림/엑스게이트 검증)
THR = {
    "dual_foreign": 50.0,      # 쌍매수: 외인 최소
    "dual_inst": 30.0,         # 쌍매수: 기관 최소
    "solo_main": 50.0,         # 단독: 주도 주체 최소
    "solo_other": 30.0,        # 단독: 보조 주체 최대
    "retail_strong": 30.0,     # 개인주도: 개인 최소 매수
    "retail_netral": 20.0,     # 개인주도: 외+기관 합 허용 폭
    "other_corp_load": 20.0,   # 기타법인 매수
    "other_corp_dump": 20.0,   # 기타법인 매도 (절대값)
    "outflow_foreign": 30.0,   # 수급이탈: 외인 순매도 최소
    "outflow_inst": 20.0,      # 수급이탈: 기관 순매도 최소
    "surge_pct": 10.0,         # 수급이탈 판정용 가격 급등 기준
    "individual_dump": 30.0,   # 쌍매수: 개인 순매도 최소 (절대값)
}

# ── 패턴 정의 ──
PATTERN_DESC = {
    "DUAL_SURGE": "쌍매수 폭발 (외인+기관 동반)",
    "FOREIGN_SOLO": "외국인 단독 매수",
    "INST_SOLO": "기관 단독 매수",
    "OTHER_CORP_LOAD": "기타법인 대량 매수 (M&A/자사주 의심)",
    "OTHER_CORP_DUMP": "기타법인 대량 매도 (자사주 처분/내부자 이탈 의심)",
    "RETAIL_LED": "개인 주도 (단타성)",
    "OUTFLOW": "수급 이탈 (외인/기관 매도에도 급등)",
    "MIXED": "혼조 (명확한 주도 세력 없음)",
}

PATTERN_SCORE = {
    "DUAL_SURGE": 95,
    "FOREIGN_SOLO": 80,
    "INST_SOLO": 75,
    "OTHER_CORP_LOAD": 65,   # 공시 확인 필요
    "OTHER_CORP_DUMP": 25,   # 위험
    "RETAIL_LED": 40,        # 단타성
    "OUTFLOW": 15,           # 고점 신호
    "MIXED": 50,
}

PATTERN_GRADE = {
    "DUAL_SURGE": "최강",
    "FOREIGN_SOLO": "강함",
    "INST_SOLO": "중강",
    "OTHER_CORP_LOAD": "특수",
    "OTHER_CORP_DUMP": "위험",
    "RETAIL_LED": "약함",
    "OUTFLOW": "위험",
    "MIXED": "보통",
}


@dataclass
class PatternResult:
    code: str
    name: str
    pattern: str
    description: str
    score: int
    grade: str
    change_rate: float
    close: float
    volume: int
    entities_today: Dict[str, Optional[float]] = field(default_factory=dict)
    entities_5d: Dict[str, Optional[float]] = field(default_factory=dict)
    entities_10d: Dict[str, Optional[float]] = field(default_factory=dict)
    entities_20d: Dict[str, Optional[float]] = field(default_factory=dict)
    warnings: List[str] = field(default_factory=list)
    classified_at: str = ""


def _csv_to_canonical_df(csv_path: Path) -> Optional[pd.DataFrame]:
    """
    CSV 파일(백만원 단위) → canonical 영문 ID DataFrame (억원 단위).

    Returns:
        DataFrame with index=date, columns=ENTITY_COLUMNS (값 in 억원)
        실패시 None.
    """
    if not csv_path.exists():
        return None
    try:
        df = pd.read_csv(csv_path, index_col=0, parse_dates=True)
    except Exception:
        return None
    if df.empty:
        return None

    # 각 row를 canonical로 변환
    canonical_rows = []
    for idx, row in df.iterrows():
        c = to_canonical(row.to_dict())
        canonical_rows.append(c)

    out = pd.DataFrame(canonical_rows, index=df.index)
    # 단위 자동 감지 (2026-04-16 버그 수정):
    #   CSV가 과거(~2024 초)엔 원 단위(예: 76525320), 최근엔 백만원 단위(예: 6569)로
    #   혼재 저장된 것을 발견. 절대값 기준 임계로 자동 분기:
    #     - abs >= 1e7 → 원 단위 → /1e8 (억원)
    #     - abs <  1e7 → 백만원 단위 → /100 (억원)
    def _convert_unit(v):
        if v is None:
            return None
        if v != v:  # NaN
            return None
        if abs(v) >= 1e7:
            return v / 1e8
        return v / 100.0

    for col in ENTITY_COLUMNS:
        if col in out.columns:
            out[col] = out[col].apply(_convert_unit)
    # 정렬
    out = out.sort_index()
    return out


def _calc_cumulative(df: pd.DataFrame, days: int, ref_date: Optional[str] = None) -> Dict[str, Optional[float]]:
    """
    최근 N일 누적 (ref_date 포함 역방향).
    None 값은 스킵.
    """
    if df is None or df.empty:
        return {k: None for k in ENTITY_COLUMNS}

    if ref_date:
        sub = df.loc[df.index <= ref_date].tail(days)
    else:
        sub = df.tail(days)

    out = {}
    for col in ENTITY_COLUMNS:
        if col in sub.columns:
            vals = sub[col].dropna()
            out[col] = float(vals.sum()) if len(vals) > 0 else None
        else:
            out[col] = None
    return out


def _today_row(df: pd.DataFrame, ref_date: Optional[str] = None) -> Dict[str, Optional[float]]:
    """ref_date 당일 수급 (default: 최근 일자)."""
    if df is None or df.empty:
        return {k: None for k in ENTITY_COLUMNS}
    if ref_date:
        candidates = df.loc[df.index <= ref_date]
        if candidates.empty:
            return {k: None for k in ENTITY_COLUMNS}
        last = candidates.iloc[-1]
    else:
        last = df.iloc[-1]
    return {col: (float(last[col]) if col in last.index and pd.notna(last[col]) else None)
            for col in ENTITY_COLUMNS}


def classify_pattern(
    entities_today: Dict[str, Optional[float]],
    change_rate: float,
) -> Tuple[str, List[str]]:
    """
    오늘 수급(억원) + 등락률 → 패턴 분류.

    우선순위:
      1. DUAL_SURGE (외+기관 쌍매수, 개인 대량 매도)
      2. OUTFLOW (외/기관 대량 매도인데 급등)
      3. OTHER_CORP_DUMP (기타법인 대량 매도)
      4. OTHER_CORP_LOAD (기타법인 대량 매수)
      5. FOREIGN_SOLO
      6. INST_SOLO
      7. RETAIL_LED
      8. MIXED
    """
    f = entities_today.get("foreign") or 0.0
    i = entities_today.get("institution") or 0.0
    p = entities_today.get("individual") or 0.0
    oc = entities_today.get("other_corp") or 0.0

    warnings: List[str] = []

    # 1. DUAL_SURGE — 외 +50억 AND 기 +30억 AND 개 -30억 이상 매도
    if f >= THR["dual_foreign"] and i >= THR["dual_inst"] and p <= -THR["individual_dump"]:
        # M-1: DUAL_SURGE인데 기타법인 대량 매도 병행 시 경고 추가
        if oc <= -THR["other_corp_dump"]:
            warnings.append(
                f"쌍매수지만 기타법인 {oc:+.1f}억 매도 병행 — 내부자 물량 소화 의심"
            )
        return "DUAL_SURGE", warnings

    # 2. OUTFLOW — 외 -30억 AND 기 -20억 AND 급등 10%+
    if f <= -THR["outflow_foreign"] and i <= -THR["outflow_inst"] and change_rate >= THR["surge_pct"]:
        warnings.append(f"수급 이탈에도 +{change_rate:.1f}% 급등 — 고점 가능성")
        return "OUTFLOW", warnings

    # 3. OTHER_CORP_DUMP — 기타법인 -20억 이하 (매도)
    if oc <= -THR["other_corp_dump"]:
        warnings.append(f"기타법인 {oc:+.1f}억 매도 — 자사주 처분/대주주 매도 의심")
        return "OTHER_CORP_DUMP", warnings

    # 4. OTHER_CORP_LOAD — 기타법인 +20억 이상 (매수)
    if oc >= THR["other_corp_load"]:
        warnings.append(f"기타법인 {oc:+.1f}억 매수 — M&A/자사주 취득 가능성")
        return "OTHER_CORP_LOAD", warnings

    # 5. FOREIGN_SOLO — 외 +50억 AND (기 <30억 OR 외인 우세)
    # H-1/H-3 수정: 외인+기관 모두 매수지만 DUAL_SURGE 개인조건 미달시 우세 주체로 SOLO 승격
    if f >= THR["solo_main"] and i < THR["solo_other"]:
        return "FOREIGN_SOLO", warnings

    # 6. INST_SOLO — 기 +50억 AND 외 <30억
    if i >= THR["solo_main"] and f < THR["solo_other"]:
        return "INST_SOLO", warnings

    # 7. DUAL_WEAK fallback — 외+기 모두 매수인데 개인조건 미달시 우세 주체로 분류
    # 예: 코나아이(외+30.8/기+97.8/개-128.3) → INST_SOLO (기관 우세)
    # 예: 필옵틱스(외+49/기+14.2/개-61.2) → FOREIGN_SOLO (외인 우세)
    if f >= THR["dual_inst"] and i >= THR["dual_inst"]:
        if f >= i:
            warnings.append(f"외인 우세 쌍매수 (외 {f:+.1f}억 / 기 {i:+.1f}억)")
            return "FOREIGN_SOLO", warnings
        else:
            warnings.append(f"기관 우세 쌍매수 (외 {f:+.1f}억 / 기 {i:+.1f}억)")
            return "INST_SOLO", warnings

    # 8. RETAIL_LED — 외+기 합 |20억| 이내 AND 개인 +30억
    if abs(f + i) <= THR["retail_netral"] and p >= THR["retail_strong"]:
        warnings.append("개인 주도 단타 — 변동성 주의")
        return "RETAIL_LED", warnings

    # 9. MIXED
    return "MIXED", warnings


def analyze_code(
    code: str,
    name: str,
    change_rate: float,
    close: float,
    volume: int,
    ref_date: Optional[str] = None,
) -> PatternResult:
    """단일 코드 패턴 분석."""
    csv_path = FLOW_DIR / f"{code}_investor.csv"
    df = _csv_to_canonical_df(csv_path)

    entities_today = _today_row(df, ref_date) if df is not None else {k: None for k in ENTITY_COLUMNS}
    entities_5d = _calc_cumulative(df, 5, ref_date) if df is not None else {}
    entities_10d = _calc_cumulative(df, 10, ref_date) if df is not None else {}
    entities_20d = _calc_cumulative(df, 20, ref_date) if df is not None else {}

    pattern, warnings = classify_pattern(entities_today, change_rate)

    if df is None:
        warnings.insert(0, f"CSV 없음: {csv_path.name}")

    return PatternResult(
        code=code,
        name=name,
        pattern=pattern,
        description=PATTERN_DESC[pattern],
        score=PATTERN_SCORE[pattern],
        grade=PATTERN_GRADE[pattern],
        change_rate=change_rate,
        close=close,
        volume=volume,
        entities_today=entities_today,
        entities_5d=entities_5d,
        entities_10d=entities_10d,
        entities_20d=entities_20d,
        warnings=warnings,
        classified_at=datetime.now().isoformat(timespec="seconds"),
    )


def analyze_missed_gainers(date: str) -> List[PatternResult]:
    """오늘(date)의 missed_gainers 전체 분석."""
    mg_path = MISSED_DIR / f"{date}.json"
    if not mg_path.exists():
        raise FileNotFoundError(f"missed_gainers 파일 없음: {mg_path}")

    data = json.loads(mg_path.read_text(encoding="utf-8"))
    items = data.get("items", [])

    results = []
    for it in items:
        r = analyze_code(
            code=it["code"],
            name=it["name"],
            change_rate=float(it["change_rate"]),
            close=float(it["close"]),
            volume=int(it["volume"]),
            ref_date=date,
        )
        results.append(r)
    return results


def print_summary(results: List[PatternResult]) -> None:
    """콘솔 표 출력 (한글)."""
    # 패턴별 집계
    pattern_count: Dict[str, int] = {}
    for r in results:
        pattern_count[r.pattern] = pattern_count.get(r.pattern, 0) + 1

    print()
    print("=" * 80)
    print(f"수급 패턴 분류 결과 — 총 {len(results)}종목")
    print("=" * 80)
    print()

    print("[패턴별 집계]")
    for pat, cnt in sorted(pattern_count.items(), key=lambda x: -PATTERN_SCORE.get(x[0], 0)):
        print(f"  {PATTERN_DESC[pat]:30s} : {cnt}종목")
    print()

    # 종목별 상세
    print("[종목별 상세]")
    print(f"{'종목':18s} {'등락률':>7s} {'외인':>9s} {'기관':>9s} {'개인':>9s} {'기타법인':>10s}  {'패턴':20s}")
    print("-" * 100)
    # 점수 순 정렬
    results_sorted = sorted(results, key=lambda r: (-r.score, -r.change_rate))
    for r in results_sorted:
        f = r.entities_today.get("foreign")
        i = r.entities_today.get("institution")
        p = r.entities_today.get("individual")
        oc = r.entities_today.get("other_corp")
        f_s = f"{f:+.1f}억" if f is not None else "    N/A"
        i_s = f"{i:+.1f}억" if i is not None else "    N/A"
        p_s = f"{p:+.1f}억" if p is not None else "    N/A"
        oc_s = f"{oc:+.1f}억" if oc is not None else "    N/A"
        name_disp = r.name[:10]
        print(f"{name_disp:18s} {r.change_rate:+6.2f}% {f_s:>9s} {i_s:>9s} {p_s:>9s} {oc_s:>10s}  {PATTERN_DESC[r.pattern][:20]:20s}")

    print()
    print("[경고 발생 종목]")
    warn_count = 0
    for r in results_sorted:
        if r.warnings:
            warn_count += 1
            print(f"  [{r.code} {r.name}] {r.pattern}")
            for w in r.warnings:
                print(f"     └ {w}")
    if warn_count == 0:
        print("  (없음)")
    print()


def save_results(results: List[PatternResult], date: str) -> Path:
    """JSON 저장."""
    out_path = OUTPUT_DIR / f"{date}.json"

    # 패턴별 분리
    by_pattern: Dict[str, List[dict]] = {}
    for r in results:
        d = asdict(r)
        by_pattern.setdefault(r.pattern, []).append(d)

    output = {
        "date": date,
        "count": len(results),
        "schema_version": "supply_flow_v1_4entity",
        "available_entities": AVAILABLE_ENTITIES,
        "pending_entities": [c for c in ENTITY_COLUMNS if c not in AVAILABLE_ENTITIES],
        "summary": {
            pat: {"count": len(lst), "description": PATTERN_DESC[pat]}
            for pat, lst in by_pattern.items()
        },
        "items": [asdict(r) for r in sorted(results, key=lambda r: (-r.score, -r.change_rate))],
        "by_pattern": by_pattern,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
    }
    out_path.write_text(
        json.dumps(output, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return out_path


def main():
    ap = argparse.ArgumentParser(description="T3 수급 패턴 감지기")
    ap.add_argument("--date", default=None, help="기준일 YYYY-MM-DD (기본: 오늘)")
    ap.add_argument("--codes", default=None, help="직접 지정 (콤마 구분, 예: 077360,009580)")
    ap.add_argument("--no-save", action="store_true", help="파일 저장 생략")
    args = ap.parse_args()

    date = args.date or datetime.now().strftime("%Y-%m-%d")

    if args.codes:
        # 코드 직접 지정
        codes = [c.strip() for c in args.codes.split(",") if c.strip()]
        # missed_gainers에서 이름/등락률 보조 조회
        mg_items = {}
        mg_path = MISSED_DIR / f"{date}.json"
        if mg_path.exists():
            mg_data = json.loads(mg_path.read_text(encoding="utf-8"))
            for it in mg_data.get("items", []):
                mg_items[it["code"]] = it

        results = []
        for code in codes:
            it = mg_items.get(code, {})
            r = analyze_code(
                code=code,
                name=it.get("name", code),
                change_rate=float(it.get("change_rate", 0.0)),
                close=float(it.get("close", 0.0)),
                volume=int(it.get("volume", 0)),
                ref_date=date,
            )
            results.append(r)
    else:
        # missed_gainers 전체
        results = analyze_missed_gainers(date)

    print_summary(results)

    if not args.no_save:
        out_path = save_results(results, date)
        print(f"[저장] {out_path}")


if __name__ == "__main__":
    main()
