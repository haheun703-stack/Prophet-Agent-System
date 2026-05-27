# -*- coding: utf-8 -*-
"""Rule D D-day guard for limit-up signals.

The guard is intentionally pure and small so the 5/22 -> 5/27 failure mode can
be tested without touching KIS or the async trading loop.
"""

from dataclasses import dataclass
from datetime import date
from pathlib import Path
import json
from typing import Any, Optional


@dataclass(frozen=True)
class RuleDSignalInfo:
    code: str
    signal_date: date
    signal_close_price: float
    name: str = ""
    source: str = "limit_up_pullback"


@dataclass(frozen=True)
class RuleDDecision:
    allowed: bool
    reason: str
    d_plus: Optional[int] = None
    cumulative_pct: Optional[float] = None
    priority_boost: int = 0


def _parse_date(value: Any) -> Optional[date]:
    if isinstance(value, date):
        return value
    if not value:
        return None
    try:
        return date.fromisoformat(str(value)[:10])
    except Exception:
        return None


def _trading_d_plus(signal_date: date, today: date) -> int:
    if today < signal_date:
        return -1
    try:
        from data.trading_calendar import get_trading_days_between

        days = get_trading_days_between(signal_date, today)
        if days and days[0] == signal_date:
            return max(0, len(days) - 1)
        return len(days)
    except Exception:
        return (today - signal_date).days


def normalize_signal_item(item: dict[str, Any]) -> Optional[RuleDSignalInfo]:
    code = str(item.get("code") or item.get("ticker") or "").zfill(6)
    if not code or code == "000000":
        return None
    signal_date = _parse_date(item.get("signal_date") or item.get("date"))
    if signal_date is None:
        return None
    close_raw = (
        item.get("signal_close_price")
        or item.get("signal_close")
        or item.get("close")
        or item.get("entry_price")
    )
    try:
        signal_close_price = float(close_raw or 0)
    except Exception:
        signal_close_price = 0.0
    return RuleDSignalInfo(
        code=code,
        name=str(item.get("name") or ""),
        signal_date=signal_date,
        signal_close_price=signal_close_price,
        source=str(item.get("source") or item.get("entry_type") or "limit_up_pullback"),
    )


def load_signal_map(path: Path | None = None) -> dict[str, RuleDSignalInfo]:
    if path is None:
        root = Path(__file__).resolve().parent.parent
        path = root / "data_store" / "limit_up" / "watchlist.json"
    if not path.exists():
        return {}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    items = raw if isinstance(raw, list) else raw.get("items", [])
    signal_map: dict[str, RuleDSignalInfo] = {}
    for item in items:
        if not isinstance(item, dict):
            continue
        status = str(item.get("status") or "")
        if status and status not in {"monitoring", "triggered", "entered"}:
            continue
        info = normalize_signal_item(item)
        if info is not None:
            signal_map[info.code] = info
    return signal_map


def evaluate_rule_d_signal(
    signal: RuleDSignalInfo,
    current_price: float,
    today: date | None = None,
) -> RuleDDecision:
    today = today or date.today()
    d_plus = _trading_d_plus(signal.signal_date, today)
    if d_plus < 0:
        return RuleDDecision(False, "signal date is in the future", d_plus=d_plus)

    cumulative_pct: Optional[float] = None
    if signal.signal_close_price > 0 and current_price > 0:
        cumulative_pct = (float(current_price) / signal.signal_close_price - 1.0) * 100.0
        if cumulative_pct >= 20.0 - 1e-9:
            return RuleDDecision(
                False,
                f"cumulative +{cumulative_pct:.1f}% >= +20.0% peak-chase block",
                d_plus=d_plus,
                cumulative_pct=cumulative_pct,
            )

    if d_plus == 0:
        return RuleDDecision(
            True,
            "D+0 priority entry",
            d_plus=d_plus,
            cumulative_pct=cumulative_pct,
            priority_boost=30,
        )

    if d_plus == 1:
        if cumulative_pct is not None and cumulative_pct > 5.0:
            return RuleDDecision(
                False,
                f"D+1 +{cumulative_pct:.1f}% > +5.0% block",
                d_plus=d_plus,
                cumulative_pct=cumulative_pct,
            )
        return RuleDDecision(
            True,
            "D+1 within +5% allowed",
            d_plus=d_plus,
            cumulative_pct=cumulative_pct,
            priority_boost=15,
        )

    return RuleDDecision(
        False,
        f"D+{d_plus} late entry block",
        d_plus=d_plus,
        cumulative_pct=cumulative_pct,
    )


def evaluate_rule_d_item(
    item: dict[str, Any],
    current_price: float,
    today: date | None = None,
) -> RuleDDecision:
    signal = normalize_signal_item(item)
    if signal is None:
        return RuleDDecision(True, "no limit-up signal metadata")
    return evaluate_rule_d_signal(signal, current_price=current_price, today=today)
