# -*- coding: utf-8 -*-
"""Paper 학습 루프 — paper 3type forward를 구조적으로 집계·누적 (6/19 신설, read-only).

사장님 6/19 지시: "paper에서 배운 걸 저장하고 추가 학습해야지, 관찰하고 끝이면 안 된다."
배경: paper 3type는 매일 picks+forward를 저장만 하고, 그걸 받아 "어떤 조건이 수익을 냈나"
  집계·누적하는 학습 루프가 없었음. 매일 교훈이 흩어진 보고서로 날아가고 복리로 안 쌓임.

이 모듈: paper_3type/ledger_*.json 전체를 매일 재집계 → 축(시그널타입·섹터·레짐·ma지지·
  MAE대역·눌림깊이)별 forward 성과를 누적 학습DB(paper_learning.json)로 적재 + 인사이트 surface.

★★★ 안전 불변식 ★★★
- read-only: ledger json 읽기만. 주문/매수/매도/picks/recommendation/SAJANG 매도헬퍼 0 접촉.
- record-only: 학습DB json만 기록. 룰 자동변경 0 (학습→룰은 단타봇 제안→사장님 승인→shadow→live).
- 관측 없이 flip 금지 (5/31·5/26 사고). 이 학습기는 "사람이 보는 재료"를 체계화할 뿐, 자동 적용 X.

집계 축: type(A/B/C) · market_regime(HOT/WARMING/RELAY) · breadth_state · sector · ma20지지 ·
  MAE대역(-3% 터치 여부=재진입 근거) · 눌림깊이대역.
"""
from __future__ import annotations

import json
import logging
import re
from datetime import datetime
from pathlib import Path

logger = logging.getLogger("BH.PaperLearning")

SCALPER_DIR = Path(__file__).resolve().parent.parent
DATA_STORE = SCALPER_DIR / "data_store"
LEDGER_DIR = DATA_STORE / "paper_3type"
OUT_PATH = DATA_STORE / "paper_learning.json"

_MA20_RE = re.compile(r"ma20지지:(True|False)")
_PULLBACK_RE = re.compile(r"pullback\s+(-?\d+\.?\d*)%")


def _flatten_ledger(ledger: dict) -> list:
    """ledger candidates(dict A/B/C) → 평탄화 + breadth_state 부착."""
    breadth = (ledger.get("market_context") or {}).get("breadth_state", "")
    out = []
    cands = ledger.get("candidates") or {}
    iters = cands.values() if isinstance(cands, dict) else [cands]
    for v in iters:
        if isinstance(v, list):
            for c in v:
                if isinstance(c, dict):
                    c = dict(c)
                    c["_breadth_state"] = breadth
                    out.append(c)
    return out


def _load_all_picks() -> list:
    """모든 paper_3type ledger의 picks 평탄화 (forward_d1 채워진 것만 = resolved)."""
    picks = []
    if not LEDGER_DIR.exists():
        return picks
    for f in sorted(LEDGER_DIR.glob("ledger_2026-*.json")):
        try:
            led = json.loads(f.read_text(encoding="utf-8"))
        except Exception:
            continue
        for c in _flatten_ledger(led):
            # resolved만 학습 + base 어긋난 오염표본(forward_base_mismatch) 제외(데이터 무결성)
            if c.get("forward_d1") is not None and not c.get("forward_base_mismatch"):
                picks.append(c)
    return picks


def _dedup_by_stock(picks: list) -> list:
    """(date, ticker) 중복 제거 — 같은 종목이 B·C 등 여러 type에 동시 선정될 때 1표본만 카운트.

    배경(6/22): 6/12 24 candidate 중 8종목이 B·C 동시선정 → 고유 16종목인데 표본 24로 부풀려져,
      섹터·레짐·MAE대역 같은 '종목 단위' 집계에서 그 종목 가중치가 2배가 되는 왜곡 발생.
    정책: type 우선순위(A>B>C)로 첫 1개만 유지(결정적). forward/MAE는 종목 고유라 어느 type을
      남겨도 결과 통계 동일, entry_reason 파생(ma20·pullback)만 우선 type 기준으로 결정.
    ※ by_type(시나리오 단위) 집계는 원본을 써야 하므로 여기서 dedup하지 않음(호출부에서 분리).
    """
    order = {"A": 0, "B": 1, "C": 2}
    seen = {}
    for c in sorted(picks, key=lambda x: (str(x.get("date")), str(x.get("ticker")),
                                          order.get(x.get("type"), 9))):
        # 정렬 키와 dict 키를 동일 표현(str)으로 통일 — 향후 ticker 타입 변동에도 결정성 견고(L-1)
        seen.setdefault((str(c.get("date")), str(c.get("ticker"))), c)
    return list(seen.values())


def _ma20_support(entry_reason) -> str:
    if isinstance(entry_reason, str):
        m = _MA20_RE.search(entry_reason)
        if m:
            return "지지" if m.group(1) == "True" else "이탈"
    return "미상"


def _pullback_band(entry_reason) -> str:
    if isinstance(entry_reason, str):
        m = _PULLBACK_RE.search(entry_reason)
        if m:
            try:
                p = abs(float(m.group(1)))
                if p < 5:
                    return "얕음(<5%)"
                if p < 10:
                    return "중간(5~10%)"
                return "깊음(10%+)"
            except ValueError:
                pass
    return "미상"


def _mae_band(mae) -> str:
    """MAE -3% 터치 여부 = -3% 손절 가정 시 잘렸나 (재진입 근거)."""
    if mae is None:
        return "미상"
    return "손절터치(MAE≤-3%)" if mae <= -3.0 else "버팀(MAE>-3%)"


def _avg(xs):
    xs = [x for x in xs if x is not None]
    return round(sum(xs) / len(xs), 2) if xs else None


def _stats(items: list) -> dict:
    """그룹 통계: n · avg d1/d3/d5 · MFE/MAE · 승률(d1>0)."""
    d1 = [c.get("forward_d1") for c in items]
    d3 = [c.get("forward_d3") for c in items]
    d5 = [c.get("forward_d5") for c in items]
    d1v = [x for x in d1 if x is not None]
    win = round(100 * len([x for x in d1v if x > 0]) / len(d1v), 0) if d1v else None
    return {
        "n": len(items),
        "d1": _avg(d1), "d3": _avg(d3), "d5": _avg(d5),
        "mfe": _avg([c.get("MFE") for c in items]),
        "mae": _avg([c.get("MAE") for c in items]),
        "win_d1_pct": win,
        "n_d5": len(d5) - d5.count(None),
    }


def _group_by(picks: list, keyfn) -> dict:
    g = {}
    for c in picks:
        k = keyfn(c) or "미상"
        g.setdefault(k, []).append(c)
    return {k: _stats(v) for k, v in sorted(g.items(), key=lambda kv: -len(kv[1]))}


def _insights(agg: dict) -> list:
    """의사결정용 비교 인사이트 surface (사람이 보는 재료)."""
    out = []
    ma = agg.get("by_ma20_support", {})
    if "지지" in ma and "이탈" in ma and ma["지지"]["d3"] is not None and ma["이탈"]["d3"] is not None:
        diff = round(ma["지지"]["d3"] - ma["이탈"]["d3"], 2)
        out.append(f"ma20 지지 vs 이탈 d3: {ma['지지']['d3']} vs {ma['이탈']['d3']} (지지 {diff:+}%p)")
    mb = agg.get("by_mae_band", {})
    if "손절터치(MAE≤-3%)" in mb:
        s = mb["손절터치(MAE≤-3%)"]
        out.append(f"-3% 손절터치 종목 {s['n']}개 d3={s['d3']} d5={s['d5']} → "
                   f"터치율 높으면 재진입 필수(would_stop 일치)")
    bt = agg.get("by_type", {})
    for t in ("A", "B", "C"):
        if t in bt and bt[t]["n"] >= 3:
            out.append(f"[{t}타입] n={bt[t]['n']} d1={bt[t]['d1']} d3={bt[t]['d3']} 승률={bt[t]['win_d1_pct']}%")
    return out


def build_paper_learning(save: bool = True) -> dict:
    """paper 3type forward 누적 집계 → 학습DB. 매일 전체 재집계(forward 채워질수록 표본↑)."""
    picks_raw = _load_all_picks()
    picks = _dedup_by_stock(picks_raw)  # 종목 단위 집계용(중복 제거) — by_type만 원본 사용
    if not picks:
        logger.info("[paper_learning] resolved picks 없음 — skip")
        return {"total_samples": 0}

    agg = {
        "by_type": _group_by(picks_raw, lambda c: c.get("type")),  # 시나리오 단위 = 원본(그룹 내 중복 없음)
        "by_market_regime": _group_by(picks, lambda c: c.get("market_regime")),
        "by_breadth_state": _group_by(picks, lambda c: c.get("_breadth_state")),
        "by_sector": _group_by(picks, lambda c: c.get("sector")),
        "by_ma20_support": _group_by(picks, lambda c: _ma20_support(c.get("entry_reason"))),
        "by_mae_band": _group_by(picks, lambda c: _mae_band(c.get("MAE"))),
        "by_pullback_band": _group_by(picks, lambda c: _pullback_band(c.get("entry_reason"))),
    }
    result = {
        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "total_samples": len(picks),
        "total_samples_raw": len(picks_raw),
        "dedup_removed": len(picks_raw) - len(picks),
        "overall": _stats(picks),
        **agg,
        "insights": _insights(agg),
        "note": "paper 3type forward 누적 학습DB(관측 전용). 종목 단위 집계는 (date,ticker) 중복제거, "
                "by_type만 원본(시나리오 단위). 룰 자동변경 X — 단타봇 제안→사장님 승인→shadow→live.",
    }
    if save:
        try:
            OUT_PATH.write_text(json.dumps(result, ensure_ascii=False, indent=1), encoding="utf-8")
        except Exception as e:
            logger.warning(f"[paper_learning] 저장 실패: {e}")

    ov = result["overall"]
    logger.info(f"[paper_learning] 누적 {len(picks)}표본(원본 {len(picks_raw)}, 중복 "
                f"{len(picks_raw) - len(picks)} 제거) 학습 — 전체 d1={ov['d1']} d3={ov['d3']} "
                f"승률={ov['win_d1_pct']}% / 인사이트 {len(result['insights'])}건 -> {OUT_PATH.name}")
    return result


if __name__ == "__main__":
    import sys
    sys.stdout.reconfigure(encoding="utf-8")
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    r = build_paper_learning()
    if r.get("total_samples"):
        print(f"\n=== Paper 누적 학습DB ({r['total_samples']}표본"
              f", 원본 {r.get('total_samples_raw')} 중 중복 {r.get('dedup_removed')} 제거) ===")
        ov = r["overall"]
        print(f"전체: d1={ov['d1']} d3={ov['d3']} d5={ov['d5']} 승률={ov['win_d1_pct']}% (d5표본 {ov['n_d5']})")
        print("\n[ma20 지지여부]")
        for k, s in r["by_ma20_support"].items():
            print(f"  {k:6s} n={s['n']:3d} d1={s['d1']} d3={s['d3']} d5={s['d5']} 승률={s['win_d1_pct']}%")
        print("\n[MAE 대역 = 재진입 근거]")
        for k, s in r["by_mae_band"].items():
            print(f"  {k:18s} n={s['n']:3d} d1={s['d1']} d3={s['d3']} d5={s['d5']}")
        print("\n[시장 레짐]")
        for k, s in r["by_market_regime"].items():
            print(f"  {k:10s} n={s['n']:3d} d1={s['d1']} d3={s['d3']} 승률={s['win_d1_pct']}%")
        print("\n★ 인사이트:")
        for ins in r["insights"]:
            print(f"  - {ins}")
