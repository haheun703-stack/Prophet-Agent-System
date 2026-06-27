# -*- coding: utf-8 -*-
"""Paper 규칙 적용 연습장부 (what-if shadow) — 6/19 신설, read-only.

사장님 6/19: "배운 거(오답노트)를 실제 종목선정에 반영하되, 실전 전에 연습으로 한번 검증하자."
이 모듈 = paper 학습(paper_learning)에서 나온 규칙을 **가상 적용**해, 코호트별로
  "현행 선정 vs 규칙적용 선정"의 forward를 평행 비교·누적한다.

★★★ 안전 불변식 ★★★
- read-only: paper_3type ledger 읽기만. 실제 picks·주문·매도·SAJANG 0 접촉.
- record-only: 연습 결과 json만 기록. 실제 종목선정 변경 0 ("이렇게 골랐으면 어땠나"만 기록).
- 검증 후에만 사장님 승인 → 실선정 적용(라이브). 이 장부는 그 승인의 근거자료일 뿐.
- 관측 없이 flip 금지(5/31·5/26). 표본·레짐 다양성(강세장 포함) 확보 후 판단.

규칙(확장 가능): paper_learning 발견에 근거. 현재 = RELAY 레짐 제외(승률 14% 최악).
  ※ MAE는 진입 후 사후값이라 직접 게이트 불가 → 규칙에서 제외(진입시점 대리지표 역추적은 별도).
"""
from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path

from data.paper_learning import _flatten_ledger, _avg, LEDGER_DIR, _dedup_by_stock
from data.paper_gate_shock import (
    keep_short_ratio_below, keep_short_surge_below, keep_credit_below,
)

logger = logging.getLogger("BH.PaperRuleShadow")


def _tc(c):
    """candidate → (ticker, virtual_entry_date) 추출 헬퍼."""
    return c.get("ticker"), c.get("virtual_entry_date")

SCALPER_DIR = Path(__file__).resolve().parent.parent
OUT_PATH = SCALPER_DIR / "data_store" / "paper_rule_shadow.json"

# ── 규칙 정의 (paper_learning 발견 근거, keep=True면 그 종목 유지) ──
# 확장: 새 발견이 검증되면 여기에 추가(예: ma20 이탈 제외 등). 각 규칙은 독립 평행 검증.
RULES = [
    {
        "name": "no_relay",
        "desc": "RELAY 레짐 제외 (학습: RELAY 승률 14% < WARMING 42%)",
        "keep": lambda c: c.get("market_regime") != "RELAY",
    },
    # ── 급락 선행 게이트 (6/27 — ★소급검증 결과 "기각": d3 -2.75%p 역효과·반증기록용 유지) ──
    # 6/22 폭탄(295310 공매도12%·389500 5.4%)이 공매도 과열로 보였으나 selection bias
    # (대형주가 원래 공매도비중 높음·삼성 7.51%). 전체 17종목 소급 시 오히려 역효과.
    # 반증 기록 가치로 유지(다른 임계/표본 재검증 참조). ★진짜 변별자 = 아래 breadth(시장 폭).
    {
        "name": "gate_short_r8",
        "desc": "공매도 거래비중 ≥8% 제외 (진입 직전)",
        "keep": lambda c: keep_short_ratio_below(*_tc(c), 8.0),
    },
    {
        "name": "gate_short_surge2",
        "desc": "공매도 비중 5일평균比 2배+ 급증 제외",
        "keep": lambda c: keep_short_surge_below(*_tc(c), 2.0),
    },
    {
        "name": "gate_credit_r5",
        "desc": "신용융자 잔고율 ≥5% 제외 (반대매매 리스크)",
        "keep": lambda c: keep_credit_below(*_tc(c), 5.0),
    },
    {
        "name": "gate_combo",
        "desc": "공매도비중≥8% or 급증2배+ or 신용≥5% 통합 회피",
        "keep": lambda c: (
            keep_short_ratio_below(*_tc(c), 8.0)
            and keep_short_surge_below(*_tc(c), 2.0)
            and keep_credit_below(*_tc(c), 5.0)
        ),
    },
    # ── 시장 폭(breadth) 게이트 (6/27 — 공매도 게이트 기각 후 진짜 변별자 발견) ──
    # 학습: BROAD_UP d3 +1.58%(손절터치52%) vs 빈값 d3 -5.80%(손절터치93%). 차이 +7.4%p.
    {
        "name": "gate_breadth_up_only",
        "desc": "breadth=BROAD_UP 일 때만 진입 (시장 폭넓은 상승)",
        "keep": lambda c: c.get("_breadth_state") == "BROAD_UP",
    },
    {
        "name": "gate_breadth_ex_empty",
        "desc": "breadth 빈값/BROAD_DOWN 제외 (약세장 회피)",
        "keep": lambda c: c.get("_breadth_state") not in ("", None, "BROAD_DOWN"),
    },
]


def _cohorts() -> dict:
    """date → resolved picks(forward_d1 채워짐·오염표본 제외). paper_learning과 동일 기준."""
    out = {}
    if not LEDGER_DIR.exists():
        return out
    for f in sorted(LEDGER_DIR.glob("ledger_2026-*.json")):
        if "_bc_only" in f.name:  # M1(6/27 fix): 정식 6/12 ledger와 date 동일 → silent 덮어쓰기 방지
            continue
        try:
            led = json.loads(f.read_text(encoding="utf-8"))
        except Exception:
            continue
        date = led.get("date", f.stem.replace("ledger_", ""))
        picks = [c for c in _flatten_ledger(led)
                 if c.get("forward_d1") is not None and not c.get("forward_base_mismatch")]
        # B·C 동시선정 종목 1표본 (⑤-2 paper_learning과 동일 정책) — 같은 종목 1포지션이라
        # 포트폴리오 성과·규칙 delta가 중복 가중되지 않도록 (date,ticker) dedup.
        picks = _dedup_by_stock(picks)
        if picks:
            out[date] = picks
    return out


def _port(picks: list) -> dict:
    """포트폴리오 성과: n·평균 d1/d3/d5·승률(d1>0)·MAE 손절터치율(≤-3%)."""
    d1 = [c.get("forward_d1") for c in picks]
    d1v = [x for x in d1 if x is not None]
    win = round(100 * len([x for x in d1v if x > 0]) / len(d1v), 0) if d1v else None
    maes = [c.get("MAE") for c in picks if c.get("MAE") is not None]
    touch = round(100 * len([m for m in maes if m <= -3.0]) / len(maes), 0) if maes else None
    return {
        "n": len(picks),
        "d1": _avg(d1), "d3": _avg([c.get("forward_d3") for c in picks]),
        "d5": _avg([c.get("forward_d5") for c in picks]),
        "win_d1_pct": win, "mae_touch_pct": touch,
    }


def _delta(gated: dict, base: dict, key: str):
    a, b = gated.get(key), base.get(key)
    return round(a - b, 2) if (a is not None and b is not None) else None


def build_rule_shadow(save: bool = True) -> dict:
    """규칙별 현행 vs 적용 forward 평행 비교·누적 (코호트별 + 전체 pooled)."""
    cohorts = _cohorts()
    if not cohorts:
        logger.info("[rule_shadow] 코호트 없음 — skip")
        return {"cohorts": 0}

    all_picks = [c for picks in cohorts.values() for c in picks]
    results = {}
    for rule in RULES:
        keep = rule["keep"]
        # 코호트별 delta (현행 vs 규칙적용)
        per_cohort = []
        for date, picks in sorted(cohorts.items()):
            base = _port(picks)
            gated = _port([c for c in picks if keep(c)])
            per_cohort.append({
                "date": date, "base_n": base["n"], "gated_n": gated["n"],
                "removed": base["n"] - gated["n"],
                "d1_delta": _delta(gated, base, "d1"),
                "d3_delta": _delta(gated, base, "d3"),
                "win_delta": _delta(gated, base, "win_d1_pct"),
            })
        # 전체 pooled
        base_all = _port(all_picks)
        gated_all = _port([c for c in all_picks if keep(c)])
        results[rule["name"]] = {
            "desc": rule["desc"],
            "pooled_baseline": base_all,
            "pooled_gated": gated_all,
            "pooled_delta": {
                "d1": _delta(gated_all, base_all, "d1"),
                "d3": _delta(gated_all, base_all, "d3"),
                "d5": _delta(gated_all, base_all, "d5"),
                "win_d1_pct": _delta(gated_all, base_all, "win_d1_pct"),
                "mae_touch_pct": _delta(gated_all, base_all, "mae_touch_pct"),
            },
            "removed_total": base_all["n"] - gated_all["n"],
            "per_cohort": per_cohort,
        }

    out = {
        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "cohorts": len(cohorts),
        "total_picks": len(all_picks),
        "rules": results,
        "note": "규칙 가상적용 연습장부(관측 전용). 실제 종목선정 변경 0. 검증→사장님 승인→라이브.",
    }
    if save:
        try:
            OUT_PATH.write_text(json.dumps(out, ensure_ascii=False, indent=1), encoding="utf-8")
        except Exception as e:
            logger.warning(f"[rule_shadow] 저장 실패: {e}")

    for name, r in results.items():
        pd = r["pooled_delta"]
        logger.info(f"[rule_shadow] {name}: 제거 {r['removed_total']}종목 / "
                    f"d3 {r['pooled_baseline']['d3']}→{r['pooled_gated']['d3']} (Δ{pd['d3']}) "
                    f"승률 Δ{pd['win_d1_pct']}%p -> {OUT_PATH.name}")
    return out


if __name__ == "__main__":
    import sys
    sys.stdout.reconfigure(encoding="utf-8")
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    r = build_rule_shadow()
    if r.get("cohorts"):
        print(f"\n=== 규칙 적용 연습장부 ({r['cohorts']}코호트·{r['total_picks']}표본) ===")
        def _f(x):  # None 안전 포맷(+부호)
            return f"{x:+}" if isinstance(x, (int, float)) else "n/a"
        for name, rr in r["rules"].items():
            b, g, d = rr["pooled_baseline"], rr["pooled_gated"], rr["pooled_delta"]
            print(f"\n[{name}] {rr['desc']}")
            print(f"  현행:   n={b['n']} d1={b['d1']} d3={b['d3']} 승률={b['win_d1_pct']}% 손절터치={b['mae_touch_pct']}%")
            print(f"  규칙적용: n={g['n']} d1={g['d1']} d3={g['d3']} 승률={g['win_d1_pct']}% 손절터치={g['mae_touch_pct']}%")
            print(f"  → 개선: d3 {_f(d['d3'])}%p · 승률 {_f(d['win_d1_pct'])}%p · 손절터치 {_f(d['mae_touch_pct'])}%p ({rr['removed_total']}종목 제거)")
        print("\n★ 연습 전용 — 실제 선정 변경 0. 검증 쌓이면 사장님 승인 후 라이브.")
