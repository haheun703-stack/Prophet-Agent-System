# -*- coding: utf-8 -*-
"""Run the limit-up discovery -> plan -> preflight -> position cycle.

This is still a read-only/paper-planning workflow. It refreshes the limit-up
engine outputs, rebuilds the D+0/D+1~D+3 plan, and then writes the realtime
preflight report. It never calls an order method.

Usage:
    python tools/run_limit_up_live_cycle.py
    python tools/run_limit_up_live_cycle.py --offline
    python tools/run_limit_up_live_cycle.py --skip-engine --offline
    python tools/run_limit_up_live_cycle.py --skip-engine --manage-positions
"""
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any


ROOT = Path(__file__).resolve().parent.parent

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.limit_up_3day_pilot import build_plans, save_outputs as save_plan_outputs  # noqa: E402
from tools.limit_up_realtime_preflight import build_preflight, save_outputs as save_preflight_outputs  # noqa: E402
from tools.limit_up_position_manager import (  # noqa: E402
    build_position_report,
    save_outputs as save_position_outputs,
)


def _run_engine() -> int:
    cmd = [sys.executable, "-m", "data.limit_up_engine", "--no-telegram"]
    print("step 1: refreshing limit-up engine")
    print("command:", " ".join(cmd))
    proc = subprocess.run(cmd, cwd=str(ROOT), check=False)
    return int(proc.returncode)


def _build_plan(args: argparse.Namespace) -> tuple[dict[str, Any], Path, Path, Path]:
    print("step 2: building 3-day paper plan")
    summary = build_plans(
        budget=max(1, args.budget),
        max_positions=max(1, args.max_positions),
        min_score=args.min_score,
        allow_stale=False,
    )
    json_path, md_path, csv_path = save_plan_outputs(summary)
    return summary, json_path, md_path, csv_path


def _build_preflight(args: argparse.Namespace) -> tuple[dict[str, Any], Path, Path, Path]:
    print("step 3: building realtime preflight")
    pf_args = SimpleNamespace(
        budget=max(1, args.budget),
        max_positions=max(1, args.max_positions),
        min_score=args.min_score,
        top=args.top,
        include_watch=args.include_watch,
        allow_stale=False,
        offline=args.offline,
        min_volume=args.min_volume,
        min_strength=args.min_strength,
        max_chase_premium_pct=args.max_chase_premium_pct,
        max_vwap_gap_pct=args.max_vwap_gap_pct,
    )
    summary = build_preflight(pf_args)
    json_path, md_path, csv_path = save_preflight_outputs(summary)
    return summary, json_path, md_path, csv_path


def _build_position_report(args: argparse.Namespace) -> tuple[dict[str, Any], Path, Path, Path]:
    print("step 4: managing D+0~D+3 positions")
    pm_args = SimpleNamespace(
        offline=args.offline,
        record_entry=None,
        name="",
        qty=0,
        entry_price=0,
        entry_date="",
        source="cycle",
        note="",
        paper_enter_ready=args.paper_enter_ready,
        entry_ratio_pct=args.entry_ratio_pct,
        apply_paper_actions=args.apply_paper_actions,
    )
    summary = build_position_report(pm_args)
    json_path, md_path, csv_path = save_position_outputs(summary)
    return summary, json_path, md_path, csv_path


def main() -> int:
    parser = argparse.ArgumentParser(description="Run read-only limit-up live cycle")
    parser.add_argument("--budget", type=int, default=3_000_000)
    parser.add_argument("--max-positions", type=int, default=2)
    parser.add_argument("--min-score", type=float, default=55.0)
    parser.add_argument("--top", type=int, default=5)
    parser.add_argument("--include-watch", action="store_true")
    parser.add_argument("--offline", action="store_true", help="Skip live KIS reads in the preflight step.")
    parser.add_argument("--skip-engine", action="store_true", help="Use existing limit-up engine files.")
    parser.add_argument("--min-volume", type=int, default=5_000)
    parser.add_argument("--min-strength", type=float, default=100.0)
    parser.add_argument("--max-chase-premium-pct", type=float, default=12.0)
    parser.add_argument("--max-vwap-gap-pct", type=float, default=3.5)
    parser.add_argument(
        "--manage-positions",
        action="store_true",
        help="Also run the read-only D+0~D+3 position manager.",
    )
    parser.add_argument(
        "--paper-enter-ready",
        action="store_true",
        help="Create paper ledger entries from latest ENTRY_READY preflight decisions.",
    )
    parser.add_argument("--entry-ratio-pct", type=int, default=50)
    parser.add_argument(
        "--apply-paper-actions",
        action="store_true",
        help="Apply recommended paper sells to the local ledger. Never places real orders.",
    )
    args = parser.parse_args()

    if not args.skip_engine:
        rc = _run_engine()
        if rc != 0:
            print(f"limit-up engine failed with exit code {rc}")
            return rc
    else:
        print("step 1: skipped limit-up engine refresh")

    plan, plan_json, plan_md, plan_csv = _build_plan(args)
    preflight, pf_json, pf_md, pf_csv = _build_preflight(args)
    position = None
    pos_json = pos_md = pos_csv = None
    if args.manage_positions or args.paper_enter_ready or args.apply_paper_actions:
        position, pos_json, pos_md, pos_csv = _build_position_report(args)

    print()
    print("limit-up live cycle complete")
    print(f"plan selected: {plan['selected_count']} / {plan['raw_candidates']}")
    print(f"preflight decisions: {preflight['decision_counts']}")
    print(f"plan report: {plan_md}")
    print(f"plan action sheet: {plan_csv}")
    print(f"preflight report: {pf_md}")
    print(f"preflight action sheet: {pf_csv}")
    if position is not None:
        print(f"position actions: {position['action_counts']}")
        print(f"position report: {pos_md}")
        print(f"position action sheet: {pos_csv}")
    print(f"plan summary: {plan_json}")
    print(f"preflight summary: {pf_json}")
    if position is not None:
        print(f"position summary: {pos_json}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
