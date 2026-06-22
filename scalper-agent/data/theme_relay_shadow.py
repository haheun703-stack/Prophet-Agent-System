# -*- coding: utf-8 -*-
"""테마 relay-aware 종목선정 shadow (6/22 구현, record-only·paper shadow).

설계: docs/02-design/theme_relay_aware_selection.md / registry: data/theme_relay_registry.json
사장님·Codex 6/22: 수동 theme registry + manual seed + 시총 fallback + paper shadow 시작.

★★★ 안전 불변식 ★★★
- read-only: daily csv + registry json + early_variant_shadow.json 만. 네트워크 0.
- record-only: theme_relay_shadow.json 만 기록. 매수/매도/picks/recommendation/SAJANG/order_intent 0 접촉.
- ★ 고정 룰 금지: 소부장/대장주 우선 고정 X. relay 방향은 박지 않고 강도로 매일 관측(relay_leader_group). ★
- 멱등: (theme, date) 키. 미해결 forward는 None 유지(다음 실행 충전).
- 봇 OFF·실주문 0. ON/OFF 플래그 없음(생성기는 관측만).

기록 필드: theme_strength / leaders_strength / supply_strength / intra_theme_group_strength /
  relay_leader_group(+leader_reversal) / early_candidates(주도 group 초입) / rotation_watch / forward(d1/d3/d5).
"""
from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional

logger = logging.getLogger("BH.ThemeRelayShadow")

SA = Path(__file__).resolve().parent.parent
DAILY_DIR = SA / "data_store" / "daily"
REGISTRY = SA / "data" / "theme_relay_registry.json"
SHADOW_OUT = SA / "data_store" / "theme_relay_shadow.json"
EARLY_SHADOW = SA / "data_store" / "early_variant_shadow.json"

STRENGTH_WINDOW = 5     # 모멘텀 측정 거래일
POS20_WINDOW = 20       # 20일 범위 내 위치(초입 판정)
EARLY_POS20_MAX = 0.5   # pos20 < 0.5 = 하단부(초입 후보)


# ───────────────────────── 데이터 (read-only) ─────────────────────────
def _load_daily(code: str, daily_dir: Path = None) -> dict:
    """code → {date: (close, high, low)}."""
    daily_dir = daily_dir or DAILY_DIR
    f = daily_dir / f"{str(code).zfill(6)}.csv"
    if not f.exists():
        return {}
    out = {}
    for ln in f.read_text(encoding="utf-8").splitlines():
        p = ln.split(",")
        if len(p) < 6 or not p[0][:4].isdigit():
            continue
        try:
            out[p[0]] = (float(p[4]), float(p[2]), float(p[3]))
        except Exception:
            pass
    return out


def _dates_upto(rows: dict, asof: str) -> list:
    return sorted(d for d in rows if d <= asof)


def _ret_n(rows: dict, asof: str, n: int) -> Optional[float]:
    """asof 종가 / n거래일 전 종가 - 1 (%). 데이터 부족 시 None (순수함수)."""
    ds = _dates_upto(rows, asof)
    if len(ds) < n + 1:
        return None
    c_now = rows[ds[-1]][0]
    c_then = rows[ds[-1 - n]][0]
    if c_then <= 0:
        return None
    return round((c_now / c_then - 1) * 100, 2)


def _pos20(rows: dict, asof: str) -> Optional[float]:
    """20일 범위 내 현재 위치 0~1 (0=저점, 1=고점). 초입=낮음. 순수함수."""
    ds = _dates_upto(rows, asof)
    if len(ds) < 2:
        return None
    win = ds[-POS20_WINDOW:]
    lo = min(rows[d][2] for d in win)
    hi = max(rows[d][1] for d in win)
    c = rows[ds[-1]][0]
    if hi <= lo:
        return None
    return round((c - lo) / (hi - lo), 3)


def _fwd(rows: dict, asof: str, k: int) -> Optional[float]:
    """asof → asof+k거래일 종가 수익률(%). 미래 부족 시 None."""
    ds = sorted(rows)
    if asof not in ds:
        return None
    i = ds.index(asof)
    if i + k >= len(ds):
        return None
    c0 = rows[asof][0]
    if c0 <= 0:
        return None
    return round((rows[ds[i + k]][0] / c0 - 1) * 100, 2)


# ───────────────────────── 그룹 강도 ─────────────────────────
def _group_strength(codes: list, asof: str, daily_dir: Path = None, n: int = STRENGTH_WINDOW):
    """그룹 평균 n거래일 수익률 + 표본수 (이름 아닌 code 기준)."""
    vals = []
    for c in codes:
        r = _ret_n(_load_daily(c, daily_dir), asof, n)
        if r is not None:
            vals.append(r)
    return (round(sum(vals) / len(vals), 2) if vals else None), len(vals)


def _codes(group: list) -> list:
    return [m["code"] for m in group if m.get("code")]


# ───────────────────────── 저장 (멱등) ─────────────────────────
def _load_out(path: Path = None) -> dict:
    path = path or SHADOW_OUT
    if path.exists():
        try:
            d = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(d, dict) and isinstance(d.get("records"), list):
                return d
        except Exception:
            pass
    return {"records": [], "note": "테마 relay-aware shadow (관측 전용·매수/매도 무접촉)"}


def _save_out(data: dict, path: Path = None) -> None:
    path = path or SHADOW_OUT
    data["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def _prior_leader(data: dict, theme: str, asof: str) -> Optional[str]:
    """직전(가장 최근 < asof) 기록의 relay_leader_group (바통 역전 판정용)."""
    prev = [r for r in data["records"] if r["theme"] == theme and r["date"] < asof]
    if not prev:
        return None
    return sorted(prev, key=lambda r: r["date"])[-1].get("relay_leader_group")


# ───────────────────────── 빌드 (관측) ─────────────────────────
def build_shadow(asof: str, registry_path: Path = None, daily_dir: Path = None,
                 out_path: Path = None, save: bool = True) -> dict:
    """registry 각 테마 → 강도·주도그룹·초입후보·바통 관측 → theme_relay_shadow.json 멱등 적재.

    ★ record-only: 매수/매도/picks/SAJANG/order 무접촉. relay 방향 하드코딩 X(강도로 관측). ★
    """
    registry_path = registry_path or REGISTRY
    reg = json.loads(registry_path.read_text(encoding="utf-8"))
    data = _load_out(out_path)
    idx = {(r["theme"], r["date"]): k for k, r in enumerate(data["records"])}

    built = 0
    for theme, g in reg.get("themes", {}).items():
        lead_codes = _codes(g.get("leaders", []))
        supp_codes = _codes(g.get("supply_chain", []))
        all_codes = lead_codes + supp_codes
        if not all_codes:
            continue
        ls, ln = _group_strength(lead_codes, asof, daily_dir)
        ss, sn = _group_strength(supp_codes, asof, daily_dir)
        ts, tn = _group_strength(all_codes, asof, daily_dir)
        if ts is None:
            continue   # 데이터 없음 → skip(다음 실행)

        # 주도 group (강도로 결정 — 고정 X). 둘 다 있으면 비교, 하나만 있으면 그쪽.
        if ls is not None and ss is not None:
            leader = "LEADERS" if ls >= ss else "SUPPLY_CHAIN"
            intra = round(ls - ss, 2)
        elif ls is not None:
            leader, intra = "LEADERS", None
        elif ss is not None:
            leader, intra = "SUPPLY_CHAIN", None
        else:
            leader, intra = None, None

        prior = _prior_leader(data, theme, asof)
        reversal = bool(prior and leader and prior != leader)

        # 약세테마 게이트 = '주도 group' 강도로 판정 (전체평균 X). 바통 넘어갈 때 평균이
        # 빠지는 group에 끌려 음수가 되는 함정 회피(반도체 6/19: 평균 -1.03이나 대장주 +25.46).
        lead_strength = ls if leader == "LEADERS" else (ss if leader == "SUPPLY_CHAIN" else ts)

        # 초입 후보 = 주도 group에서 pos20 < EARLY_POS20_MAX (하단부)
        lead_members = g.get("leaders", []) if leader == "LEADERS" else \
            (g.get("supply_chain", []) if leader == "SUPPLY_CHAIN" else [])
        early = []
        for m in lead_members:
            rows = _load_daily(m["code"], daily_dir)
            p20 = _pos20(rows, asof)
            r5 = _ret_n(rows, asof, STRENGTH_WINDOW)
            if p20 is not None and p20 < EARLY_POS20_MAX:
                early.append({"code": m["code"], "name": m.get("name", m["code"]),
                              "pos20": p20, "ret5": r5})
        early.sort(key=lambda x: x["pos20"])

        # forward = 주도 group 평균 (관측용)
        def grp_fwd(codes, k):
            vs = [_fwd(_load_daily(c, daily_dir), asof, k) for c in codes]
            vs = [v for v in vs if v is not None]
            return round(sum(vs) / len(vs), 2) if vs else None
        lead_set = lead_codes if leader == "LEADERS" else supp_codes if leader == "SUPPLY_CHAIN" else all_codes

        rec = {
            "theme": theme, "date": asof,
            "theme_strength": ts, "n_theme": tn,
            "leaders_strength": ls, "n_leaders": ln,
            "supply_strength": ss, "n_supply": sn,
            "intra_theme_group_strength": intra,
            "relay_leader_group": leader,
            "leader_reversal": reversal,
            "weak_theme": bool(lead_strength is not None and lead_strength < 0),  # 주도group 강도<0=약세(회피)
            "early_candidates": early,
            "rotation_watch": reversal,
            "fwd_leader_d1": grp_fwd(lead_set, 1),
            "fwd_leader_d3": grp_fwd(lead_set, 3),
            "fwd_leader_d5": grp_fwd(lead_set, 5),
        }
        key = (theme, asof)
        if key in idx:
            data["records"][idx[key]] = rec
        else:
            data["records"].append(rec)
            idx[key] = len(data["records"]) - 1
        built += 1

    if save and built:
        _save_out(data, out_path)
    logger.info(f"[theme_relay/shadow] {asof} 테마 {built}건 기록 (record-only·매수 무접촉)")
    return {"asof": asof, "built": built}


# ───────────────────────── selftest (합성·네트워크 0) ─────────────────────────
def _synth(closes):
    """종가 리스트 → {date:(c,h,l)} (h=c*1.01, l=c*0.99)."""
    out = {}
    for i, c in enumerate(closes):
        out[f"2026-06-{i+1:02d}"] = (c, c * 1.01, c * 0.99)
    return out


def selftest() -> bool:
    ok = []
    rows = _synth([100, 102, 104, 106, 108, 110])   # 5일 +상승
    ok.append(("_ret_n 5일", _ret_n(rows, "2026-06-06", 5) == round((110/100-1)*100, 2)))
    ok.append(("_ret_n 데이터부족 None", _ret_n(rows, "2026-06-02", 5) is None))
    # pos20: 마지막이 고점 → 1.0 근처
    p = _pos20(rows, "2026-06-06")
    ok.append(("_pos20 고점=높음", p is not None and p > 0.9))
    rows2 = _synth([110, 108, 106, 104, 102, 100])  # 하락 → 마지막이 저점
    p2 = _pos20(rows2, "2026-06-06")
    ok.append(("_pos20 저점=낮음(초입)", p2 is not None and p2 < 0.1))
    ok.append(("_fwd 미래있음", _fwd(rows, "2026-06-03", 2) is not None))
    ok.append(("_fwd 미래없음 None", _fwd(rows, "2026-06-06", 2) is None))
    # 그룹 강도(합성 daily_dir 없이 _ret_n 직접) — 로직만
    leader = "LEADERS" if 5.0 >= 3.0 else "SUPPLY_CHAIN"
    ok.append(("주도판정 강도순", leader == "LEADERS"))
    leader2 = "LEADERS" if 1.0 >= 4.0 else "SUPPLY_CHAIN"
    ok.append(("주도판정 소부장우위", leader2 == "SUPPLY_CHAIN"))
    # registry 로드 + group 분류
    try:
        reg = json.loads(REGISTRY.read_text(encoding="utf-8"))
        ok.append(("registry 5테마", len(reg.get("themes", {})) == 5))
        ok.append(("반도체 leaders=대표주(삼성전자 in)",
                   "005930" in _codes(reg["themes"]["반도체"]["leaders"])))
        ok.append(("한미반도체=소부장(role)",
                   "042700" in _codes(reg["themes"]["반도체"]["supply_chain"])))
    except Exception as e:
        ok.append((f"registry 로드 실패: {e}", False))
    passed = sum(1 for _, v in ok if v)
    print(f"\n[theme_relay_shadow selftest] PASS {passed}/{len(ok)}")
    for name, v in ok:
        if not v:
            print("  [FAIL]", name)
    return passed == len(ok)


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    if "--selftest" in sys.argv:
        sys.exit(0 if selftest() else 1)
    asof = "2026-06-19"
    for a in sys.argv[1:]:
        if not a.startswith("--"):
            asof = a
    res = build_shadow(asof)
    print(f"[theme_relay shadow] {res}")
    print("★ record-only. 매수/매도/주문 무접촉. 관측 없이 flip 금지.")
