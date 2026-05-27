# -*- coding: utf-8 -*-
"""
Safe restart preflight for next-week live trading.

No orders are sent. The tool checks kill switch, runtime mode, strategy
switches, local git state, KIS order gates, optional open orders, and manual
holding protection.
"""

import argparse
import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = ROOT.parent
RESULT_DIR = ROOT / "results" / "resume_preflight"
sys.path.insert(0, str(ROOT))


def _run(cmd: list[str], cwd: Path = REPO_ROOT, timeout: int = 10) -> tuple[int, str]:
    try:
        res = subprocess.run(
            cmd,
            cwd=str(cwd),
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=timeout,
        )
        return res.returncode, (res.stdout or res.stderr).strip()
    except Exception as exc:
        return 99, str(exc)


def _check(name: str, ok: bool, detail: str, severity: str = "CRITICAL") -> dict[str, Any]:
    return {"name": name, "ok": bool(ok), "detail": detail, "severity": severity}


def _service_state_hint() -> str:
    return "Check VPS services separately: bodyhunter-bot, quantum-scheduler, jgis-scheduler must stay inactive until launch."


def _is_runtime_worktree_line(line: str) -> bool:
    path = line[3:] if len(line) > 3 else line
    path = path.replace("\\", "/")
    runtime_prefixes = (
        "scalper-agent/data_store/",
        "scalper-agent/logs/",
        "scalper-agent/results/",
        "logs/",
    )
    return path.startswith(runtime_prefixes)


def collect_preflight(expect: str, live_kis: bool, test_code: str) -> dict[str, Any]:
    from bot.kis_trader import KISTrader
    from bot.manual_position_protection import audit_kis_vs_memory, auto_sell_block_reason
    from bot.order_intent import count_allowed_auto_buys_today, sum_allowed_auto_buy_amount_today
    from bot.trade_kill_switch import KILL_SWITCH_PATH, is_auto_trade_disabled
    from bot.trade_runtime_config import load_runtime_config

    checks: list[dict[str, Any]] = []
    rc, head = _run(["git", "rev-parse", "--short", "HEAD"])
    checks.append(_check("git_head", rc == 0, head, "HIGH"))

    rc, status = _run(["git", "status", "--short"])
    dirty_tracked = [
        line for line in status.splitlines()
        if line and not line.startswith("??") and not _is_runtime_worktree_line(line)
    ]
    checks.append(
        _check(
            "tracked_worktree_clean",
            not dirty_tracked,
            "clean" if not dirty_tracked else "\n".join(dirty_tracked[:20]),
            "HIGH",
        )
    )

    kill_on = is_auto_trade_disabled()
    runtime_cfg = load_runtime_config()
    paper_only = bool(runtime_cfg.get("PAPER_ONLY"))
    checks.append(_check("kill_switch_file", KILL_SWITCH_PATH.exists(), str(KILL_SWITCH_PATH), "CRITICAL"))
    checks.append(_check("runtime_config_file", (ROOT / "data" / "trade_runtime_config.json").exists(), "trade_runtime_config.json", "CRITICAL"))

    if expect == "off":
        checks.append(_check("expect_off_kill_switch", kill_on, f"AUTO_TRADE_DISABLED={kill_on}", "CRITICAL"))
    elif expect == "paper":
        checks.append(_check("expect_paper_only", paper_only, f"PAPER_ONLY={paper_only}", "CRITICAL"))
    elif expect == "limited-live":
        checks.append(_check("expect_live_kill_switch_off", not kill_on, f"AUTO_TRADE_DISABLED={kill_on}", "CRITICAL"))
        checks.append(_check("expect_live_paper_off", not paper_only, f"PAPER_ONLY={paper_only}", "CRITICAL"))

    trader = KISTrader()
    buy_gate = trader._order_gate("BUY", test_code, 1, source="resume_preflight")
    sell_gate = trader._order_gate("SELL", test_code, 1, source="resume_preflight")

    if expect in {"off", "paper"}:
        checks.append(_check("auto_buy_gate_blocks", bool(buy_gate and buy_gate.get("blocked")), str(buy_gate), "CRITICAL"))
        checks.append(_check("auto_sell_gate_blocks", bool(sell_gate and sell_gate.get("blocked")), str(sell_gate), "CRITICAL"))
    else:
        checks.append(_check("auto_buy_gate_result", buy_gate is None, str(buy_gate), "HIGH"))
        protect_reason = auto_sell_block_reason(test_code)
        checks.append(_check("manual_protection_probe", protect_reason is not None, str(protect_reason), "MEDIUM"))

    manual_gate = trader._order_gate("BUY", test_code, 1, manual=True, source="resume_preflight_manual")
    checks.append(_check("manual_order_gate_allowed", manual_gate is None, str(manual_gate), "HIGH"))

    kis_summary: dict[str, Any] = {"enabled": bool(live_kis)}
    if live_kis:
        try:
            open_orders = trader.fetch_open_orders()
            orders = open_orders.get("orders", []) if open_orders.get("success") else []
            kis_summary["open_orders"] = orders
            checks.append(_check("kis_open_orders_empty", len(orders) == 0, f"{len(orders)} open orders", "CRITICAL"))
        except Exception as exc:
            checks.append(_check("kis_open_orders_check", False, str(exc), "CRITICAL"))
        try:
            balance = trader.fetch_balance()
            positions = balance.get("positions", []) if balance.get("success") else []
            audit = audit_kis_vs_memory(positions)
            kis_summary["manual_protection_audit"] = audit
            checks.append(
                _check(
                    "manual_unknown_holdings_protected",
                    len(audit.get("unknown_to_memory", [])) == len([
                        p for p in audit.get("protected", [])
                        if p.get("code") in set(audit.get("unknown_to_memory", []))
                    ]),
                    json.dumps(audit, ensure_ascii=False),
                    "HIGH",
                )
            )
        except Exception as exc:
            checks.append(_check("manual_protection_audit", False, str(exc), "HIGH"))

    intent_summary = {
        "allowed_auto_buys_today": count_allowed_auto_buys_today(),
        "allowed_auto_buy_amount_today": sum_allowed_auto_buy_amount_today(),
    }
    checks.append(_check("vps_service_hint", True, _service_state_hint(), "INFO"))

    failed = [c for c in checks if not c["ok"] and c["severity"] in {"CRITICAL", "HIGH"}]
    return {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "expect": expect,
        "git_head": head,
        "kill_switch": kill_on,
        "runtime_config": runtime_cfg,
        "intent_summary": intent_summary,
        "kis_summary": kis_summary,
        "checks": checks,
        "status": "PASS" if not failed else "FAIL",
    }


def write_report(result: dict[str, Any]) -> dict[str, str]:
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    json_path = RESULT_DIR / f"resume_preflight_{stamp}.json"
    md_path = RESULT_DIR / f"resume_preflight_{stamp}.md"
    latest_path = RESULT_DIR / "resume_preflight_latest.md"
    json_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")

    lines = [
        "# Resume Preflight",
        "",
        f"- status: {result['status']}",
        f"- generated_at: {result['generated_at']}",
        f"- expect: {result['expect']}",
        f"- git_head: {result['git_head']}",
        f"- kill_switch: {result['kill_switch']}",
        "",
        "## Checks",
    ]
    for check in result["checks"]:
        mark = "PASS" if check["ok"] else "FAIL"
        lines.append(f"- {mark} [{check['severity']}] {check['name']}: {check['detail']}")
    lines.extend(
        [
            "",
            "## Runtime",
            "```json",
            json.dumps(result["runtime_config"], ensure_ascii=False, indent=2),
            "```",
        ]
    )
    md = "\n".join(lines) + "\n"
    md_path.write_text(md, encoding="utf-8")
    latest_path.write_text(md, encoding="utf-8")
    return {"json": str(json_path), "md": str(md_path), "latest": str(latest_path)}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Safe restart preflight")
    parser.add_argument("--expect", choices=["off", "paper", "limited-live"], default="off")
    parser.add_argument("--live-kis", action="store_true", help="query KIS open orders and balance")
    parser.add_argument("--test-code", default="005930")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    result = collect_preflight(args.expect, args.live_kis, args.test_code.zfill(6))
    paths = write_report(result)
    print("resume preflight complete")
    print(f"status: {result['status']}")
    print(f"expect: {args.expect}")
    print(f"latest report: {paths['latest']}")
    return 0 if result["status"] == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
