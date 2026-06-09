# -*- coding: utf-8 -*-
"""Market open regime builder.

Pure read-only logic for three consumers:
- quant bot: sector/tier re-ranking
- scalper bot: D1 open entry gate
- info bot: explanation/digest

No order path, no SAJANG mutation, no network calls.
Inputs are existing data_store JSON files.
"""

import json
import logging
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger("BH.MarketOpenRegime")

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data_store"

RAW_OUT = DATA_DIR / "market_open_raw.json"
QUANT_OUT = DATA_DIR / "quant_market_regime.json"
SCALPER_OUT = DATA_DIR / "scalper_open_gate.json"
INFO_OUT = DATA_DIR / "info_market_open_digest.json"


ETF_THEME_BY_CODE = {
    "069500": ("kospi", "KOSPI200", "directional"),
    "122630": ("kospi", "KOSPI200_LEVERAGE", "directional"),
    "229200": ("kosdaq", "KOSDAQ150", "directional"),
    "233740": ("kosdaq", "KOSDAQ150_LEVERAGE", "directional"),
    "114800": ("inverse", "KOSPI_INVERSE", "risk_off"),
    "252670": ("inverse", "KOSPI_INVERSE_2X", "risk_off"),
    "091160": ("semiconductor", "SEMICONDUCTOR", "sector"),
    "266370": ("it", "IT", "sector"),
    "244580": ("bio", "BIO", "sector"),
    "305720": ("battery", "BATTERY", "sector"),
    "449450": ("defense", "DEFENSE", "sector"),
    "139230": ("shipbuilding", "SHIPBUILDING", "sector"),
    "091180": ("auto", "AUTO", "sector"),
    "117700": ("construction", "CONSTRUCTION", "sector"),
    "140700": ("insurance", "INSURANCE", "sector"),
    "102780": ("samsung", "SAMSUNG_GROUP", "sector"),
    "132030": ("gold", "GOLD", "commodity"),
    "411060": ("gold", "GOLD_SPOT", "commodity"),
    "261220": ("oil", "OIL", "commodity"),
    "130680": ("oil", "OIL", "commodity"),
    "139320": ("metals", "METALS", "commodity"),
}

SECTOR_KEYWORDS = {
    "semiconductor": ("semiconductor", "semi", "chip", "hbm", "memory", "반도체"),
    "bio": ("bio", "health", "pharma", "바이오", "제약"),
    "battery": ("battery", "2차", "전지", "ev"),
    "shipbuilding": ("ship", "조선", "lng"),
    "defense": ("defense", "방산", "aero"),
    "auto": ("auto", "car", "자동차"),
    "it": ("it", "software", "platform", "전기전자", "전자"),
    "construction": ("construction", "건설"),
    "insurance": ("insurance", "보험"),
    "gold": ("gold", "금선물", "금현물", "귀금속"),
    "oil": ("oil", "energy", "원유"),
    "metals": ("metal", "steel", "금속", "철강"),
}


@dataclass
class ThemeScore:
    key: str
    label: str
    score: float = 0.0
    sources: List[str] = field(default_factory=list)
    inst_1d: float = 0.0
    foreign_1d: float = 0.0
    change_pct: float = 0.0


def _load_json(path: Path, default: Any) -> Any:
    try:
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        logger.warning("[market_open] load failed %s: %s", path.name, exc)
    return default


def _save_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def _num(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _clip(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


def _score_etf_item(item: dict) -> float:
    """Score one ETF flow item on a roughly -100..+100 scale."""
    inst_1d = _num(item.get("inst_1d"))
    inst_3d = _num(item.get("inst_3d"))
    inst_5d = _num(item.get("inst_5d"))
    foreign_1d = _num(item.get("foreign_1d"))
    foreign_3d = _num(item.get("foreign_3d"))
    inst_consec = _num(item.get("inst_consecutive"))
    foreign_consec = _num(item.get("foreign_consecutive"))
    change_pct = _num(item.get("change_pct"))
    strength = _num(item.get("strength"))

    score = 0.0
    score += _clip(inst_1d / 20.0, -35, 35)
    score += _clip(inst_3d / 80.0, -20, 20)
    score += _clip(inst_5d / 150.0, -15, 15)
    score += _clip(foreign_1d / 15.0, -15, 15)
    score += _clip(foreign_3d / 60.0, -10, 10)
    score += _clip(inst_consec * 4.0, -16, 16)
    score += _clip(foreign_consec * 2.5, -10, 10)
    score += _clip(change_pct * 1.5, -12, 12)
    score += _clip(strength / 10.0, 0, 10)
    return round(_clip(score, -100, 100), 2)


def _add_theme(scores: Dict[str, ThemeScore], key: str, label: str, score: float,
               source: str, inst_1d: float = 0.0, foreign_1d: float = 0.0,
               change_pct: float = 0.0) -> None:
    current = scores.get(key)
    if current is None:
        current = ThemeScore(key=key, label=label)
        scores[key] = current
    current.score += score
    current.sources.append(source)
    current.inst_1d += inst_1d
    current.foreign_1d += foreign_1d
    current.change_pct = max(current.change_pct, change_pct)


def _theme_from_text(text: str) -> Optional[str]:
    lowered = str(text or "").lower()
    for key, words in SECTOR_KEYWORDS.items():
        if any(word.lower() in lowered for word in words):
            return key
    return None


def _date_ordinal(value: Any) -> Optional[int]:
    try:
        if not value:
            return None
        return datetime.strptime(str(value)[:10], "%Y-%m-%d").date().toordinal()
    except (TypeError, ValueError):
        return None


def _freshness_status(data_dates: dict) -> dict:
    """Detect stale/mixed-source packets.

    US overnight and domestic data do not always share the same calendar date,
    but a gap larger than three days is operationally stale.
    """
    warnings = []
    etf_ord = _date_ordinal(data_dates.get("etf_flow") or data_dates.get("brain"))
    us_ord = _date_ordinal(data_dates.get("us"))
    if etf_ord and us_ord and abs(etf_ord - us_ord) > 3:
        warnings.append(
            f"us_overnight_stale: us={data_dates.get('us')} domestic={data_dates.get('etf_flow')}"
        )
    if not data_dates.get("etf_flow"):
        warnings.append("missing_etf_flow_date")
    if not data_dates.get("us"):
        warnings.append("missing_us_overnight_date")
    return {
        "ok": not warnings,
        "warnings": warnings,
    }


def build_market_raw(data_dir: Path = DATA_DIR) -> dict:
    """Load existing raw signals into one packet."""
    us = _load_json(data_dir / "us_market_overnight.json", {})
    us_result = _load_json(data_dir / "us_overnight_result.json", {})
    etf_flow = _load_json(data_dir / "etf_flow.json", {})
    sector_flow = _load_json(data_dir / "sector_flow.json", {})
    sector_momentum = _load_json(data_dir / "sector_momentum.json", {})
    market_health = _load_json(data_dir / "market_health.json", {})
    brain_report = _load_json(data_dir / "brain_report.json", {})

    data_dates = {
        "us": us.get("date"),
        "us_result": us_result.get("date"),
        "etf_flow": etf_flow.get("data_date"),
        "sector_flow": sector_flow.get("data_date"),
        "brain": brain_report.get("date"),
    }

    return {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "data_dates": data_dates,
        "freshness": _freshness_status(data_dates),
        "us_overnight": us,
        "us_overnight_result": us_result,
        "etf_flow": etf_flow,
        "sector_flow": sector_flow,
        "sector_momentum": sector_momentum,
        "market_health": market_health,
        "brain_report": brain_report,
    }


def _build_etf_theme_scores(raw: dict) -> Dict[str, ThemeScore]:
    scores: Dict[str, ThemeScore] = {}
    for item in raw.get("etf_flow", {}).get("items", []) or []:
        code = str(item.get("code", ""))
        theme_key, label, kind = ETF_THEME_BY_CODE.get(
            code,
            (str(item.get("group") or "unknown"), str(item.get("name") or code), "unknown"),
        )
        score = _score_etf_item(item)
        if kind == "risk_off":
            score *= -1.0
        _add_theme(
            scores,
            theme_key,
            label,
            score,
            "domestic_etf",
            _num(item.get("inst_1d")),
            _num(item.get("foreign_1d")),
            _num(item.get("change_pct")),
        )
    return scores


def _apply_sector_raw_scores(raw: dict, scores: Dict[str, ThemeScore]) -> None:
    for item in raw.get("sector_flow", {}).get("sectors", []) or []:
        key = _theme_from_text(item.get("sector", "")) or _theme_from_text(item.get("alias", ""))
        if not key:
            continue
        inst_1d = _num(item.get("inst_1d"))
        foreign_1d = _num(item.get("foreign_1d"))
        boost = _num(item.get("boost_score"))
        flow_score = _clip(inst_1d / 80.0, -15, 15) + _clip(foreign_1d / 80.0, -10, 10) + boost
        _add_theme(scores, key, key.upper(), flow_score, "sector_flow", inst_1d, foreign_1d)

    for item in raw.get("sector_momentum", {}).get("sectors", []) or []:
        key = _theme_from_text(item.get("sector", ""))
        if not key:
            continue
        momentum = _num(item.get("avg_return_1d"))
        breadth = _num(item.get("breadth_1d"))
        boost = _num(item.get("boost_score"))
        mom_score = _clip(momentum * 2.0, -15, 15) + _clip((breadth - 0.5) * 20.0, -8, 8) + _clip(boost / 3.0, -5, 10)
        _add_theme(scores, key, key.upper(), mom_score, "sector_momentum", change_pct=momentum)


def build_quant_regime(raw: dict) -> dict:
    """Quant bot interpretation: sector weights and tier re-ranking hints."""
    scores = _build_etf_theme_scores(raw)
    _apply_sector_raw_scores(raw, scores)

    themes = sorted(scores.values(), key=lambda x: x.score, reverse=True)
    leading = [asdict(t) for t in themes if t.score >= 20][:5]
    avoid = [asdict(t) for t in sorted(scores.values(), key=lambda x: x.score) if t.score <= -15][:5]

    kospi = scores.get("kospi", ThemeScore("kospi", "KOSPI")).score
    kosdaq = scores.get("kosdaq", ThemeScore("kosdaq", "KOSDAQ")).score
    inverse = scores.get("inverse", ThemeScore("inverse", "INVERSE")).score

    if inverse <= -25:
        market_bias = "RISK_OFF"
    elif kosdaq - kospi >= 12:
        market_bias = "KOSDAQ"
    elif kospi - kosdaq >= 12:
        market_bias = "KOSPI"
    elif max(kospi, kosdaq) >= 25:
        market_bias = "BROAD_RISK_ON"
    else:
        market_bias = "NEUTRAL"

    etf_dominant = bool(
        (max(kospi, kosdaq) >= 25)
        or any(t.score >= 30 for t in themes if t.key not in ("kospi", "kosdaq"))
    )

    sector_weights = {
        t.key: round(_clip(1.0 + t.score / 100.0, 0.55, 1.45), 3)
        for t in themes
    }

    return {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "market_bias": market_bias,
        "etf_dominant": etf_dominant,
        "leading_themes": leading,
        "avoid_themes": avoid,
        "sector_weights": sector_weights,
        "theme_scores": [asdict(t) for t in themes],
        "tier_policy": {
            "core_rule": "CORE should align with leading_themes unless stock-specific evidence is exceptional.",
            "control_recheck_rule": "CONTROL candidates aligned with leading_themes require WATCH recheck.",
            "avoid_rule": "Avoid-themes receive strong demotion for CORE and D1 open entries.",
        },
    }


def score_quant_candidate(candidate: dict, quant_regime: dict) -> dict:
    """Score one candidate for quant tier adjustment.

    Expected candidate keys are flexible: code/name/sector/tier/etf_codes.
    This helper is intentionally side-effect free so quant bot can call it
    during CORE/WATCH/CONTROL sorting.
    """
    sector = str(candidate.get("sector") or candidate.get("theme") or "")
    tier = str(candidate.get("tier") or "").upper()
    theme = candidate.get("theme_key") or _theme_from_text(sector) or "unknown"
    weights = quant_regime.get("sector_weights") or {}
    weight = _num(weights.get(theme), 1.0)
    leading_keys = {x.get("key") for x in quant_regime.get("leading_themes", [])}
    avoid_keys = {x.get("key") for x in quant_regime.get("avoid_themes", [])}
    aligned = theme in leading_keys or weight >= 1.20

    score = 0.0
    reasons = []
    if aligned:
        score += 25
        reasons.append("market_theme_aligned")
    if theme in avoid_keys:
        score -= 35
        reasons.append("avoid_theme")
    score += (weight - 1.0) * 100.0

    action = "KEEP"
    if tier == "CONTROL" and aligned and score >= 20:
        action = "RECHECK_CONTROL_TO_WATCH"
    elif tier == "CORE" and theme in avoid_keys:
        action = "DEMOTE_CORE_RECHECK"
    elif tier == "CORE" and not aligned and weight < 1.15:
        action = "CORE_WEAK_ALIGNMENT_RECHECK"
    elif score >= 35:
        action = "PROMOTE"
    elif score <= -25:
        action = "DEMOTE"

    return {
        "code": candidate.get("code", ""),
        "name": candidate.get("name", ""),
        "input_tier": tier,
        "theme_key": theme,
        "sector_weight": round(weight, 3),
        "etf_exposure_score": round(score, 1),
        "tier_action": action,
        "reasons": reasons,
    }


def _overnight_korea_score(raw: dict, quant_regime: dict) -> Tuple[int, List[str]]:
    us = raw.get("us_overnight") or {}
    us_result = raw.get("us_overnight_result") or {}
    health = raw.get("market_health") or {}

    ewy = _num(us.get("ewy_change"))
    ewy_5d = _num(us.get("ewy_change_5d"))
    soxx = _num(us.get("soxx_change"))
    nasdaq = _num(us.get("nasdaq_change"))
    sp500 = _num(us.get("sp500_change"))
    ks200 = _num(us.get("ks200_change"))
    vix = _num(us.get("vix"), 20)
    dxy = _num(us.get("dxy"), 100)
    gap_est = _num(us_result.get("gap_est_pct"))
    decline_ratio = _num(health.get("decline_ratio"))

    score = 50.0
    reasons = []
    freshness = raw.get("freshness") or {}
    if not freshness.get("ok", True):
        score -= 30
        reasons.append("stale_or_mixed_raw_data")

    score += _clip(ewy * 5.0, -25, 25)
    score += _clip(ewy_5d * 1.2, -12, 12)
    score += _clip(soxx * 3.0, -18, 18)
    score += _clip(nasdaq * 3.0, -12, 12)
    score += _clip(sp500 * 2.0, -8, 8)
    score += _clip(ks200 * 3.0, -15, 15)
    score += _clip(gap_est * 4.0, -10, 10)

    if vix >= 25:
        score -= 15
        reasons.append("vix_caution")
    elif vix <= 17:
        score += 4

    if dxy >= 106:
        score -= 12
        reasons.append("strong_dollar")
    elif dxy <= 101:
        score += 5

    if quant_regime.get("market_bias") in ("KOSDAQ", "KOSPI", "BROAD_RISK_ON"):
        score += 8
        reasons.append("domestic_etf_support")
    if quant_regime.get("market_bias") == "RISK_OFF":
        score -= 18
        reasons.append("domestic_inverse_pressure")

    if decline_ratio >= 0.75 and ewy >= 1.0 and soxx >= 0.5:
        score += 8
        reasons.append("panic_recovery_setup")

    if ewy >= 2.0:
        reasons.append("ewy_bull")
    if soxx >= 2.0:
        reasons.append("soxx_bull")
    if ewy <= -2.0:
        reasons.append("ewy_bear")
    if soxx <= -2.0:
        reasons.append("soxx_bear")

    if not freshness.get("ok", True):
        score = min(score, 55)

    return int(round(_clip(score, 0, 100))), reasons


def build_scalper_open_gate(raw: dict, quant_regime: dict) -> dict:
    """Scalper bot interpretation: whether D1 open entry is allowed."""
    score, reasons = _overnight_korea_score(raw, quant_regime)
    health = raw.get("market_health") or {}
    regime = str(health.get("regime") or "")
    decline_ratio = _num(health.get("decline_ratio"))

    if score >= 72:
        gate = "ALLOW_D1_OPEN"
        top_k_delta = 1
        position_scale = 1.0
    elif score >= 58:
        gate = "SELECTIVE_D1_OPEN"
        top_k_delta = 0
        position_scale = 0.8
    elif score >= 44:
        gate = "WATCH_ONLY"
        top_k_delta = -1
        position_scale = 0.5
    else:
        gate = "BLOCK_NEW_BUY"
        top_k_delta = -99
        position_scale = 0.0

    panic_recovery = bool(decline_ratio >= 0.70 and score >= 58)
    if panic_recovery:
        reasons.append("panic_recovery_candidate")

    return {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "gate": gate,
        "overnight_korea_score": score,
        "top_k_delta": top_k_delta,
        "position_scale": position_scale,
        "panic_recovery": panic_recovery,
        "previous_market_regime": regime,
        "decline_ratio": decline_ratio,
        "preferred_themes": [x.get("key") for x in quant_regime.get("leading_themes", [])[:3]],
        "avoid_themes": [x.get("key") for x in quant_regime.get("avoid_themes", [])[:3]],
        "reasons": reasons,
        "freshness": raw.get("freshness") or {},
        "usage": {
            "allow_order": "This is a gate only. Order modules must still pass SAJANG/account/order-path checks.",
            "d1_open_rule": "Use D1 open only when gate is ALLOW_D1_OPEN or SELECTIVE_D1_OPEN.",
        },
    }


def build_info_digest(raw: dict, quant_regime: dict, scalper_gate: dict) -> dict:
    """Info bot interpretation: explain regime without owning trade decisions."""
    us = raw.get("us_overnight") or {}
    us_result = raw.get("us_overnight_result") or {}
    leading = quant_regime.get("leading_themes", [])[:3]
    avoid = quant_regime.get("avoid_themes", [])[:3]

    lines = [
        f"market_bias={quant_regime.get('market_bias')}",
        f"etf_dominant={quant_regime.get('etf_dominant')}",
        f"freshness_ok={(raw.get('freshness') or {}).get('ok')}",
        f"scalper_gate={scalper_gate.get('gate')} score={scalper_gate.get('overnight_korea_score')}",
        f"EWY={_num(us.get('ewy_change')):+.2f}% 5D={_num(us.get('ewy_change_5d')):+.2f}%",
        f"SOXX={_num(us.get('soxx_change')):+.2f}% NASDAQ={_num(us.get('nasdaq_change')):+.2f}%",
        f"gap_est={_num(us_result.get('gap_est_pct')):+.2f}%",
    ]
    if leading:
        lines.append("leading=" + ",".join(x.get("key", "") for x in leading))
    if avoid:
        lines.append("avoid=" + ",".join(x.get("key", "") for x in avoid))

    return {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "role": "INFO_DIGEST_ONLY",
        "summary": " | ".join(lines),
        "facts": {
            "freshness": raw.get("freshness") or {},
            "market_bias": quant_regime.get("market_bias"),
            "etf_dominant": quant_regime.get("etf_dominant"),
            "scalper_gate": scalper_gate.get("gate"),
            "overnight_korea_score": scalper_gate.get("overnight_korea_score"),
            "leading_themes": leading,
            "avoid_themes": avoid,
        },
        "handoff": {
            "quant_bot": "Use quant_market_regime.json for CORE/WATCH/CONTROL re-ranking.",
            "scalper_bot": "Use scalper_open_gate.json for D1 open gate and top_k adjustment.",
            "info_bot": "Report facts and freshness; do not become the trade decision owner.",
        },
    }


def build_all(data_dir: Path = DATA_DIR, save: bool = True) -> dict:
    raw = build_market_raw(data_dir)
    quant = build_quant_regime(raw)
    scalper = build_scalper_open_gate(raw, quant)
    info = build_info_digest(raw, quant, scalper)

    result = {
        "raw": raw,
        "quant_regime": quant,
        "scalper_open_gate": scalper,
        "info_digest": info,
    }
    if save:
        _save_json(data_dir / RAW_OUT.name, raw)
        _save_json(data_dir / QUANT_OUT.name, quant)
        _save_json(data_dir / SCALPER_OUT.name, scalper)
        _save_json(data_dir / INFO_OUT.name, info)
    return result


def format_brief(result: dict) -> str:
    quant = result.get("quant_regime", {})
    scalper = result.get("scalper_open_gate", {})
    info = result.get("info_digest", {})
    leading = ", ".join(x.get("key", "") for x in quant.get("leading_themes", [])[:3]) or "none"
    avoid = ", ".join(x.get("key", "") for x in quant.get("avoid_themes", [])[:3]) or "none"
    return "\n".join([
        "[MARKET_OPEN_REGIME]",
        f"market_bias: {quant.get('market_bias')} | etf_dominant: {quant.get('etf_dominant')}",
        f"leading: {leading}",
        f"avoid: {avoid}",
        f"scalper_gate: {scalper.get('gate')} | overnight_score: {scalper.get('overnight_korea_score')}",
        f"info: {info.get('summary', '')}",
    ])


# ── Consumer-side read helpers (scalper bot D1 open gate) ──
# Pure read-only. No order path, no SAJANG mutation, no network.

def load_scalper_open_gate(data_dir: Path = DATA_DIR) -> Optional[dict]:
    """Read scalper_open_gate.json for the scalper bot D1 open filter.

    Returns the gate dict, or None when the file is missing or unreadable so the
    caller keeps its legacy behavior (gate is an additive filter, not a hard
    dependency). Callers must still apply freshness/BLOCK rules themselves.
    """
    path = data_dir / SCALPER_OUT.name
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        logger.warning("[market_open] scalper gate load failed %s: %s", path.name, exc)
        return None


def theme_of_sector(text: str) -> Optional[str]:
    """Public wrapper mapping a sector/label string to a theme key (or None).

    Used by the scalper bot to match candidate sectors against
    preferred_themes / avoid_themes. Returns None when no theme keyword matches
    (caller then skips bonus/penalty for that candidate).
    """
    return _theme_from_text(text)


def decide_open_gate(gate: Optional[dict], base_top_k: int) -> dict:
    """Pure decision for the scalper D1 open filter (no side effects).

    Translates a scalper_open_gate dict (or None) + the base top_k into an
    actionable decision. The scalper bot uses this BEFORE candidate selection.
    It only adjusts top_k and exposes preferred/avoid themes — it never touches
    SAJANG money rules, order quantity, or the order path.

    Rules (사장님 6/9):
      - gate is None (file missing/unreadable) → keep legacy behavior (allow, base_top_k).
      - BLOCK_NEW_BUY → block new buys (allow=False, top_k=0).
      - freshness.ok=false → demote ALLOW/SELECTIVE to WATCH_ONLY and forbid the
        positive top_k_delta (aggressive entry blocked). top_k may only shrink.
      - WATCH_ONLY → top_k += top_k_delta (typically -1); allow=False if it hits 0.
      - ALLOW/SELECTIVE (fresh) → top_k += top_k_delta (+1 / +0).
      - position_scale is intentionally ignored here (SAJANG money rule untouched).

    Returns dict: allow, top_k, gate, preferred, avoid, fresh_ok, reason.
    """
    if not gate:
        return {
            "allow": True, "top_k": int(base_top_k), "gate": "NO_GATE",
            "preferred": [], "avoid": [], "fresh_ok": True,
            "reason": "no_gate_file_legacy",
        }

    gname = str(gate.get("gate") or "")
    fresh_ok = bool((gate.get("freshness") or {}).get("ok", False))
    delta = int(gate.get("top_k_delta", 0) or 0)
    preferred = list(gate.get("preferred_themes") or [])
    avoid = list(gate.get("avoid_themes") or [])

    if gname == "BLOCK_NEW_BUY":
        return {
            "allow": False, "top_k": 0, "gate": "BLOCK_NEW_BUY",
            "preferred": preferred, "avoid": avoid, "fresh_ok": fresh_ok,
            "reason": "block_new_buy",
        }

    reason = gname.lower()
    if not fresh_ok:
        delta = min(delta, 0)  # forbid positive delta (no aggressive entry)
        if gname in ("ALLOW_D1_OPEN", "SELECTIVE_D1_OPEN"):
            gname = "WATCH_ONLY"
        reason = "stale_demoted_watch_only"

    new_top_k = max(0, int(base_top_k) + delta)
    allow = new_top_k > 0
    if not allow:
        reason = "watch_only_top_k_zero"

    return {
        "allow": allow, "top_k": new_top_k, "gate": gname,
        "preferred": preferred, "avoid": avoid, "fresh_ok": fresh_ok,
        "reason": reason,
    }


def summarize_open_gate_health(gate: Optional[dict], base_top_k: int = 3) -> dict:
    """Build a compact 09:10 health payload for the D1 open gate.

    This is intentionally side-effect free. The scheduler job can log or send
    the returned text, while tests can assert the same decision contract used
    by the 09:15 entry path.
    """
    decision = decide_open_gate(gate, base_top_k)
    freshness = (gate or {}).get("freshness") or {}
    warnings = list(freshness.get("warnings") or [])
    generated_at = (gate or {}).get("generated_at")
    score = (gate or {}).get("overnight_korea_score")
    position_scale = (gate or {}).get("position_scale")

    needs_attention = (
        decision["gate"] == "NO_GATE"
        or not decision["fresh_ok"]
        or bool(warnings)
        or not decision["allow"]
    )

    text_lines = [
        "[MARKET_OPEN_GATE 09:10]",
        f"gate={decision['gate']} fresh_ok={decision['fresh_ok']} allow={decision['allow']}",
        f"top_k={int(base_top_k)}->{decision['top_k']} score={score}",
        "preferred=" + ",".join(decision["preferred"] or []) +
        " | avoid=" + ",".join(decision["avoid"] or []),
        f"reason={decision['reason']}",
    ]
    if generated_at:
        text_lines.append(f"generated_at={generated_at}")
    if warnings:
        text_lines.append("warnings=" + " / ".join(str(w) for w in warnings))
    if position_scale is not None:
        text_lines.append(f"position_scale_ignored={position_scale}")
    if decision["gate"] == "NO_GATE":
        text_lines.append("gate unavailable, legacy open entry used")

    return {
        "decision": decision,
        "freshness": freshness,
        "warnings": warnings,
        "generated_at": generated_at,
        "overnight_korea_score": score,
        "position_scale_ignored": position_scale,
        "needs_attention": needs_attention,
        "text": "\n".join(text_lines),
    }
