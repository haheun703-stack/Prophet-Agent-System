# -*- coding: utf-8 -*-
"""Deterministic static audit for order-path safety.

This tool intentionally does not import project modules. It only reads source
files, parses Python AST, and applies grep-like checks. Runtime trading state,
KIS, Telegram, cron, systemd, and environment files are never touched.
"""
from __future__ import annotations

import argparse
import ast
import json
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any


TOOL_NAME = "audit_order_paths.py"
TOOL_VERSION = "2"
SCALPER_ROOT = Path(__file__).resolve().parents[1]
SCAN_DIRS = ("bot", "engine", "data", "api", "tools")
GATE_REQUIRED_CHECKS = (
    "CHECK-1_paper_open_close_no_intent",
    "CHECK-2_tradetracker_no_intent",
    "CHECK-4_gate_write_enforce",
    "CHECK-5_order_gate_coverage",
    "CHECK-6_fixed_tp_sl_violation",
    "CHECK-7_intent_forensic_fields",
    "CHECK-8_cash30_single_source",
)
GATE_REQUIRED_NUMBERS = [1, 2, 4, 5, 6, 7, 8]
ORDER_PATH_MODULES = {
    "bot/auto_trader.py",
    "bot/telegram_bot.py",
    "bot/trading_coo.py",
    "bot/kis_trader.py",
    "engine/paper_portfolio.py",
    "engine/limit_up_paper_trader.py",
    "data/trade_object.py",
    "data/trade_tracker.py",
    "data/limit_up_engine.py",
}
CHECK8_REAL_EXTRA_MODULES = {"bot/trading_cfo.py"}
FIXED_MULTIPLIERS = (1.03, 1.05, 1.10, 0.95, 0.97, 0.975, 0.93)
TP_SL_TARGET_NAMES = {
    "tp",
    "sl",
    "take_profit",
    "stop_loss",
    "target_price",
    "tp_price",
    "sl_price",
    "reversal_tp",
}
TP_SL_DICT_KEYS = {
    "tp",
    "sl",
    "take_profit",
    "stop_loss",
    "target_price",
    "tp_price",
    "sl_price",
    "exit_rules",
    "tp_5pct",
    "tp_10pct",
    "sl_15pct",
}


@dataclass(frozen=True)
class SourceFile:
    path: Path
    rel: str
    text: str
    lines: list[str]
    tree: ast.AST


def _read_source(path: Path) -> SourceFile:
    text = path.read_text(encoding="utf-8", errors="replace")
    return SourceFile(
        path=path,
        rel=path.relative_to(SCALPER_ROOT).as_posix(),
        text=text,
        lines=text.splitlines(),
        tree=ast.parse(text, filename=str(path)),
    )


def _iter_python_files() -> list[Path]:
    files: list[Path] = []
    for root_name in SCAN_DIRS:
        root = SCALPER_ROOT / root_name
        if not root.exists():
            continue
        for path in root.rglob("*.py"):
            parts = set(path.parts)
            if "__pycache__" in parts:
                continue
            if path.name.startswith("test_") or path.name.endswith("_test.py"):
                continue
            files.append(path)
    return sorted(files)


def _load_sources() -> list[SourceFile]:
    sources: list[SourceFile] = []
    for path in _iter_python_files():
        try:
            sources.append(_read_source(path))
        except SyntaxError as exc:
            raise RuntimeError(f"AST parse failed: {path}: {exc}") from exc
    return sources


def _run_git(args: list[str]) -> str:
    try:
        res = subprocess.run(
            ["git", "-C", str(SCALPER_ROOT), *args],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=8,
        )
        out = (res.stdout or res.stderr).strip()
        return out or "unknown"
    except Exception as exc:
        return f"git_error:{exc}"


def _call_name(node: ast.AST) -> str:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return node.attr
    return ""


def _expr_text(node: ast.AST) -> str:
    try:
        return ast.unparse(node)
    except Exception:
        return ""


def _node_contains_name(node: ast.AST, name: str) -> bool:
    return any(isinstance(child, ast.Name) and child.id == name for child in ast.walk(node))


def _numeric_constant(node: ast.AST) -> float | None:
    if (
        isinstance(node, ast.Constant)
        and isinstance(node.value, (int, float))
        and not isinstance(node.value, bool)
    ):
        return float(node.value)
    if (
        isinstance(node, ast.UnaryOp)
        and isinstance(node.op, ast.USub)
        and isinstance(node.operand, ast.Constant)
        and isinstance(node.operand.value, (int, float))
        and not isinstance(node.operand.value, bool)
    ):
        return -float(node.operand.value)
    return None


def _close_float(value: float, candidates: tuple[float, ...] = FIXED_MULTIPLIERS) -> bool:
    return any(abs(value - candidate) < 0.000001 for candidate in candidates)


def _name_is_tp_sl(name: str) -> bool:
    lower = name.lower()
    if lower in TP_SL_TARGET_NAMES:
        return True
    if lower.startswith("tp_") or lower.startswith("sl_"):
        return True
    if lower.endswith("_tp") or lower.endswith("_sl"):
        return True
    if "_tp_" in lower or "_sl_" in lower:
        return True
    if lower in {"tp_pct", "sl_pct", "target_pct", "stop_pct"}:
        return True
    if lower.endswith("_tp_pct") or lower.endswith("_sl_pct"):
        return True
    if lower.endswith("tp_pct") or lower.endswith("sl_pct"):
        return True
    return False


def _name_is_tp_sl_constant(name: str) -> bool:
    lower = name.lower()
    upper = name.upper()
    if upper in {"TP_PCT", "SL_PCT", "TARGET_PCT", "STOP_PCT"}:
        return True
    if lower.endswith("_pct") and (
        lower.startswith("tp")
        or lower.startswith("sl")
        or lower.startswith("target")
        or lower.startswith("stop")
        or "_tp" in lower
        or "_sl" in lower
    ):
        return True
    if "_tp_" in lower or "_sl_" in lower:
        return True
    return False


def _dict_key_name(node: ast.AST | None) -> str:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.Name):
        return node.id
    return ""


def _target_names(target: ast.AST) -> list[str]:
    if isinstance(target, ast.Name):
        return [target.id]
    if isinstance(target, ast.Attribute):
        return [target.attr]
    if isinstance(target, ast.Subscript):
        return _target_names(target.value)
    if isinstance(target, (ast.Tuple, ast.List)):
        names: list[str] = []
        for elt in target.elts:
            names.extend(_target_names(elt))
        return names
    return []


def _path_is_sim_learning(rel: str) -> bool:
    name = Path(rel).name.lower()
    lower = rel.lower()
    return (
        "backtest" in lower
        or name == "trade_learner.py"
        or "_sim" in lower
        or "what_if" in lower
    )


def _path_is_dashboard_upload(rel: str) -> bool:
    return Path(rel).name.lower().startswith("upload_")


def _function_source(source: SourceFile, node: ast.AST) -> str:
    start = getattr(node, "lineno", 1)
    end = getattr(node, "end_lineno", start)
    return "\n".join(source.lines[start - 1:end])


def _has_call_named(node: ast.AST, names: set[str]) -> bool:
    for child in ast.walk(node):
        if isinstance(child, ast.Call) and _call_name(child.func) in names:
            return True
    return False


def _contains_intent_before(source: SourceFile, func: ast.AST, line: int) -> bool:
    for child in ast.walk(func):
        if isinstance(child, ast.Call) and getattr(child, "lineno", 0) < line:
            if _call_name(child.func) in {"record_order_intent", "_record_order_intent_or_block"}:
                return True
            if any(kw.arg == "intent_id" for kw in child.keywords):
                return True
    return False


def _call_has_intent_kw(call: ast.Call) -> bool:
    return any(kw.arg == "intent_id" for kw in call.keywords)


def _kw_constant(call: ast.Call, name: str) -> Any:
    for kw in call.keywords:
        if kw.arg == name and isinstance(kw.value, ast.Constant):
            return kw.value.value
    return None


class PaperCallVisitor(ast.NodeVisitor):
    def __init__(self, source: SourceFile) -> None:
        self.source = source
        self.stack: list[ast.AST] = []
        self.calls: dict[str, list[dict[str, Any]]] = {"open": [], "close": []}

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self.stack.append(node)
        self.generic_visit(node)
        self.stack.pop()

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        self.visit_FunctionDef(node)  # type: ignore[arg-type]

    def visit_Call(self, node: ast.Call) -> None:
        if isinstance(node.func, ast.Attribute) and node.func.attr in {
            "open_position",
            "close_position",
        }:
            func = self.stack[-1] if self.stack else node
            kind = "open" if node.func.attr == "open_position" else "close"
            self.calls[kind].append({
                "file": self.source.rel,
                "line": node.lineno,
                "function": getattr(func, "name", "<module>"),
                "object": _expr_text(node.func.value),
                "has_intent_before": (
                    _call_has_intent_kw(node)
                    or _contains_intent_before(self.source, func, node.lineno)
                ),
            })
        self.generic_visit(node)


def check_1_paper_open_close(sources: list[SourceFile]) -> dict[str, Any]:
    found = {"open": [], "close": []}
    for source in sources:
        visitor = PaperCallVisitor(source)
        visitor.visit(source.tree)
        found["open"].extend(visitor.calls["open"])
        found["close"].extend(visitor.calls["close"])
    missing = {
        "open": [row for row in found["open"] if not row["has_intent_before"]],
        "close": [row for row in found["close"] if not row["has_intent_before"]],
    }
    return {
        "expected": {
            "missing_intent_open_v6": 4,
            "missing_intent_close_v6": 9,
            "pass_missing_intent_total": 0,
        },
        "found": found,
        "missing_intent": missing,
        "verdict": "PASS" if not missing["open"] and not missing["close"] else "FAIL",
    }


def check_2_tradetracker() -> dict[str, Any]:
    target_names = {
        "register",
        "activate",
        "close",
        "register_paper_from_objects",
        "check_paper_prices",
        "paper_close_eod",
        "register_paper_preclose",
    }
    path = SCALPER_ROOT / "data" / "trade_tracker.py"
    source = _read_source(path)
    methods: list[dict[str, Any]] = []
    for node in ast.walk(source.tree):
        if isinstance(node, ast.FunctionDef) and node.name in target_names:
            methods.append({
                "name": node.name,
                "line": node.lineno,
                "has_intent": _has_call_named(
                    node, {"record_order_intent", "_record_order_intent_or_block"}
                ),
            })
    methods.sort(key=lambda row: row["line"])
    missing = [row for row in methods if not row["has_intent"]]
    return {
        "expected": {"methods": sorted(target_names), "pass_missing_intent": 0},
        "found": {"methods": methods, "missing_intent_count": len(missing)},
        "verdict": "PASS" if not missing and len(methods) == len(target_names) else "FAIL",
    }


def _is_sajang_auto_disabled(node: ast.AST) -> bool:
    return (
        isinstance(node, ast.Attribute)
        and node.attr == "AUTO_TRADE_DISABLED"
        and isinstance(node.value, ast.Name)
        and node.value.id == "SAJANG"
    )


def _is_auto_disabled_test(node: ast.AST) -> bool:
    if isinstance(node, ast.Call):
        name = _call_name(node.func)
        return name in {"_auto_trade_disabled", "is_auto_trade_disabled"}
    if _is_sajang_auto_disabled(node):
        return True
    if isinstance(node, ast.BoolOp):
        return any(_is_auto_disabled_test(value) for value in node.values)
    if isinstance(node, ast.UnaryOp):
        return _is_auto_disabled_test(node.operand)
    if isinstance(node, ast.Compare):
        return _is_auto_disabled_test(node.left) or any(
            _is_auto_disabled_test(comp) for comp in node.comparators
        )
    return False


def _body_has_return(body: list[ast.stmt]) -> bool:
    for stmt in body:
        if isinstance(stmt, ast.Return):
            return True
        if isinstance(stmt, (ast.If, ast.For, ast.While, ast.Try, ast.With)):
            if any(isinstance(child, ast.Return) for child in ast.walk(stmt)):
                return True
    return False


class EarlyReturnVisitor(ast.NodeVisitor):
    def __init__(self, source: SourceFile) -> None:
        self.source = source
        self.stack: list[ast.AST] = []
        self.rows: list[dict[str, Any]] = []

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self.stack.append(node)
        self.generic_visit(node)
        self.stack.pop()

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        self.visit_FunctionDef(node)  # type: ignore[arg-type]

    def visit_If(self, node: ast.If) -> None:
        if _is_auto_disabled_test(node.test) and _body_has_return(node.body):
            func = self.stack[-1] if self.stack else node
            has_intent = _contains_intent_before(self.source, func, node.lineno)
            if not has_intent:
                self.rows.append({
                    "file": self.source.rel,
                    "line": node.lineno,
                    "function": getattr(func, "name", "<module>"),
                    "test": _expr_text(node.test),
                })
        self.generic_visit(node)


def check_3_caller_preblock(sources: list[SourceFile]) -> dict[str, Any]:
    target_files = {
        "bot/auto_trader.py",
        "bot/telegram_bot.py",
        "bot/vwap_split_buy.py",
    }
    rows: list[dict[str, Any]] = []
    for source in sources:
        if source.rel not in target_files:
            continue
        visitor = EarlyReturnVisitor(source)
        visitor.visit(source.tree)
        rows.extend(visitor.rows)

    auto_rows = [row for row in rows if row["file"] == "bot/auto_trader.py"]
    sajang_rows = [row for row in auto_rows if "SAJANG.AUTO_TRADE_DISABLED" in row["test"]]
    counts_by_file: dict[str, int] = {}
    for row in rows:
        counts_by_file[row["file"]] = counts_by_file.get(row["file"], 0) + 1
    expected_total = 19
    return {
        "expected": {
            "auto_trader_total_v6": 17,
            "auto_trader_sajang_branch_v6": 1,
            "kis_total_v6": expected_total,
        },
        "found": {
            "auto_trader_total": len(auto_rows),
            "auto_trader_sajang_branch": len(sajang_rows),
            "kis_total": len(rows),
            "by_file": counts_by_file,
            "lines": rows,
        },
        "verdict": "PASS" if len(rows) == expected_total else "WARN",
    }


def _call_has_blocked_propagation(source: SourceFile, line: int) -> bool:
    window = "\n".join(source.lines[line - 1: min(len(source.lines), line + 18)])
    has_assignment = re.search(
        r"\bblocked\s*=\s*self\._record_order_intent_or_block\s*\(", window
    )
    has_return = re.search(r"if\s+blocked\s*:\s*\n\s+return\s+blocked", window)
    return bool(has_assignment and has_return)


def check_4_gate_write_enforce() -> dict[str, Any]:
    source = _read_source(SCALPER_ROOT / "bot" / "kis_trader.py")
    target_func = None
    for node in ast.walk(source.tree):
        if isinstance(node, ast.FunctionDef) and node.name == "_order_gate":
            target_func = node
            break
    branches = {
        "allowed": {"found": False, "line": None, "propagates_block": False},
        "runtime_block": {"found": False, "line": None, "propagates_block": False},
        "protected": {"found": False, "line": None, "propagates_block": False},
        "disabled": {"found": False, "line": None, "propagates_block": False},
    }
    if target_func is not None:
        for call in ast.walk(target_func):
            if not isinstance(call, ast.Call):
                continue
            if _call_name(call.func) != "_record_order_intent_or_block":
                continue
            reason = _kw_constant(call, "reason")
            allowed = _kw_constant(call, "allowed")
            label = None
            if reason == "AUTO_ORDER_ALLOWED":
                label = "allowed"
            elif reason == "MANUAL_POSITION_PROTECTED":
                label = "protected"
            elif reason == "AUTO_TRADE_DISABLED":
                label = "disabled"
            elif allowed is False and reason is None:
                text = "\n".join(source.lines[call.lineno - 1: call.lineno + 12])
                if "runtime_block" in text:
                    label = "runtime_block"
            if label:
                branches[label] = {
                    "found": True,
                    "line": call.lineno,
                    "propagates_block": _call_has_blocked_propagation(source, call.lineno),
                }
    return {
        "expected": {"pass_all_branches_propagate_block": True},
        "found": branches,
        "verdict": "PASS" if all(row["propagates_block"] for row in branches.values()) else "FAIL",
    }


def check_5_order_gate_coverage() -> dict[str, Any]:
    source = _read_source(SCALPER_ROOT / "bot" / "kis_trader.py")
    methods_by_name = {
        node.name: node
        for node in ast.walk(source.tree)
        if isinstance(node, ast.FunctionDef)
    }
    targets = {
        "buy_market": "_execute_buy",
        "smart_buy": "smart_buy",
        "chase_buy": "chase_buy",
        "smart_sell": "smart_sell",
        "safe_buy": "safe_buy",
        "nxt_safe_buy": "nxt_safe_buy",
        "_afterhours_order": "_afterhours_order",
    }
    methods: list[dict[str, Any]] = []
    for display, actual in targets.items():
        node = methods_by_name.get(actual)
        body = _function_source(source, node) if node is not None else ""
        has_gate = "_order_gate(" in body
        methods.append({
            "name": display,
            "actual_method": actual,
            "line": getattr(node, "lineno", None),
            "has_order_gate": has_gate,
        })
    return {
        "expected": {"all_target_methods_have_order_gate": True},
        "found": {"methods": methods},
        "verdict": "PASS" if all(row["has_order_gate"] for row in methods) else "FAIL",
    }


def _line_uses_sajang(lines: list[str], line_no: int) -> bool:
    start = max(1, line_no - 2)
    end = min(len(lines), line_no + 2)
    window = "\n".join(lines[start - 1:end])
    return "SAJANG" in window and (
        "get_take_profit" in window
        or "get_stop_loss" in window
        or "TRAILING" in window
        or "CASH_RESERVE" in window
    )


def _expr_contains_sajang_helper(node: ast.AST) -> bool:
    text = _expr_text(node)
    return "SAJANG." in text and (
        "get_take_profit" in text
        or "get_trailing_stop" in text
        or "get_normal_sl" in text
        or "get_stop_loss" in text
    )


def _expr_fixed_multiplier_reason(node: ast.AST) -> str | None:
    for child in ast.walk(node):
        if isinstance(child, ast.BinOp) and isinstance(child.op, ast.Mult):
            left_const = _numeric_constant(child.left)
            right_const = _numeric_constant(child.right)
            if left_const is not None and _close_float(abs(left_const)):
                return f"fixed_multiplier:{left_const:g}"
            if right_const is not None and _close_float(abs(right_const)):
                return f"fixed_multiplier:{right_const:g}"
        if isinstance(child, ast.Name) and _name_is_tp_sl(child.id):
            upper = child.id.upper()
            if "PCT" in upper or upper in {"TP_PCT", "SL_PCT", "TARGET_PCT", "STOP_PCT"}:
                return f"named_constant:{child.id}"
    return None


def _node_has_order_sink(node: ast.AST, names: set[str]) -> bool:
    if not names:
        return False
    sink_call_names = {
        "open_position",
        "close_position",
        "register",
        "activate",
        "close",
        "register_paper_from_objects",
        "check_paper_prices",
        "paper_close_eod",
        "register_paper_preclose",
        "buy_market",
        "smart_buy",
        "chase_buy",
        "smart_sell",
        "safe_buy",
        "nxt_safe_buy",
        "_afterhours_order",
    }
    constructor_sink_names = {"RecommendedStock"}
    for child in ast.walk(node):
        if isinstance(child, ast.Call):
            call_name = _call_name(child.func)
            if call_name in sink_call_names:
                for arg in child.args:
                    if any(_node_contains_name(arg, name) for name in names):
                        return True
                for kw in child.keywords:
                    if any(_node_contains_name(kw.value, name) for name in names):
                        return True
            if call_name in constructor_sink_names:
                for kw in child.keywords:
                    if kw.arg and _name_is_tp_sl(kw.arg):
                        if any(_node_contains_name(kw.value, name) for name in names):
                            return True
        if isinstance(child, ast.Dict):
            for key, value in zip(child.keys, child.values):
                key_name = _dict_key_name(key)
                if key_name == "exit_rules":
                    if any(_node_contains_name(value, name) for name in names):
                        return True
    return False


def _is_entry_band_check6(source: SourceFile, func: ast.AST | None, names: set[str], line: int) -> bool:
    func_name = getattr(func, "name", "") if func is not None else ""
    if source.rel != "data/limit_up_engine.py":
        return False
    if func_name == "_make_split_plan" and names == {"price"}:
        return True
    if func_name == "scan_new_signals":
        text = source.lines[line - 1].strip() if 1 <= line <= len(source.lines) else ""
        if names == {"signal"} and "WatchItem(" in text:
            return True
        if names <= {"entry_price", "entry_low", "entry_high", "price"}:
            return True
    if func_name == "check_pullback_entries" and names <= {"entry_price", "entry_low", "entry_high"}:
        return True
    return False


def _classify_check6(source: SourceFile, func: ast.AST | None, names: set[str], line: int) -> tuple[str, str]:
    if _is_entry_band_check6(source, func, names, line):
        return "ENTRY_BAND", "limit-up entry-band price planning, not TP/SL exit"
    if _path_is_sim_learning(source.rel):
        return "SIM_LEARNING", "module excluded as simulation/learning/backtest"
    if _path_is_dashboard_upload(source.rel):
        return "DASHBOARD_UPLOAD", "upload module separated from order gate"
    if source.rel in ORDER_PATH_MODULES:
        return "ORDER_PATH", "order-path module allowlist"
    if func is not None and _node_has_order_sink(func, names):
        return "ORDER_PATH", "matched variable flows into order-path sink"
    return "UNCERTAIN", "no order-path module or sink proven"


class TpSlMatchVisitor(ast.NodeVisitor):
    def __init__(self, source: SourceFile) -> None:
        self.source = source
        self.stack: list[ast.AST] = []
        self.matches: list[dict[str, Any]] = []
        self._seen_sites: set[tuple[str, int, str, str]] = set()

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self.stack.append(node)
        self.generic_visit(node)
        self.stack.pop()

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        self.visit_FunctionDef(node)  # type: ignore[arg-type]

    def _record(
        self,
        node: ast.AST,
        kind: str,
        reason: str,
        names: list[str],
        expr: ast.AST,
    ) -> None:
        line = getattr(node, "lineno", None)
        if line is None:
            return
        if _line_uses_sajang(self.source.lines, line) or _expr_contains_sajang_helper(expr):
            return
        func = self.stack[-1] if self.stack else None
        name_set = set(names)
        classification, classification_reason = _classify_check6(self.source, func, name_set, line)
        site_key = f"{self.source.rel}:{line}:{','.join(sorted(name_set)) or kind}:{reason}"
        dedupe_key = (self.source.rel, line, ",".join(sorted(name_set)), reason)
        if dedupe_key in self._seen_sites:
            return
        self._seen_sites.add(dedupe_key)
        self.matches.append({
            "file": self.source.rel,
            "line": line,
            "function": getattr(func, "name", "<module>") if func is not None else "<module>",
            "kind": kind,
            "reason": reason,
            "names": sorted(name_set),
            "classification": classification,
            "classification_reason": classification_reason,
            "site_key": site_key,
            "text": self.source.lines[line - 1].strip()[:180],
        })

    def visit_Assign(self, node: ast.Assign) -> None:
        names: list[str] = []
        for target in node.targets:
            names.extend(_target_names(target))
        target_has_tp_sl = any(_name_is_tp_sl(name) for name in names)
        target_has_tp_sl_const = any(_name_is_tp_sl_constant(name) for name in names)
        value_number = _numeric_constant(node.value)
        reason = _expr_fixed_multiplier_reason(node.value)
        if target_has_tp_sl_const and value_number is not None and value_number != 0:
            reason = reason or f"named_tp_sl_constant:{value_number:g}"
        if target_has_tp_sl and reason:
            self._record(node, "tp_sl_assignment", reason, names, node.value)
        elif reason and self.source.rel in ORDER_PATH_MODULES:
            self._record(node, "order_path_multiplier_assignment", reason, names, node.value)
        self.generic_visit(node)

    def visit_AnnAssign(self, node: ast.AnnAssign) -> None:
        if node.value is not None:
            names = _target_names(node.target)
            target_has_tp_sl = any(_name_is_tp_sl(name) for name in names)
            target_has_tp_sl_const = any(_name_is_tp_sl_constant(name) for name in names)
            value_number = _numeric_constant(node.value)
            reason = _expr_fixed_multiplier_reason(node.value)
            if target_has_tp_sl_const and value_number is not None and value_number != 0:
                reason = reason or f"named_tp_sl_constant:{value_number:g}"
            if target_has_tp_sl and reason:
                self._record(node, "tp_sl_assignment", reason, names, node.value)
            elif reason and self.source.rel in ORDER_PATH_MODULES:
                self._record(node, "order_path_multiplier_assignment", reason, names, node.value)
        self.generic_visit(node)

    def visit_Dict(self, node: ast.Dict) -> None:
        if self.source.rel in ORDER_PATH_MODULES:
            for key, value in zip(node.keys, node.values):
                reason = _expr_fixed_multiplier_reason(value)
                if not reason:
                    continue
                key_name = _dict_key_name(key)
                names = [key_name] if key_name else []
                self._record(value, "order_path_dict_multiplier", reason, names, value)
        self.generic_visit(node)


def check_6_fixed_tp_sl(sources: list[SourceFile]) -> dict[str, Any]:
    matches: list[dict[str, Any]] = []
    for source in sources:
        if source.rel == "tools/audit_order_paths.py" or source.rel == "data/sajang_rules.py":
            continue
        visitor = TpSlMatchVisitor(source)
        visitor.visit(source.tree)
        matches.extend(visitor.matches)
    order_path_real = [row for row in matches if row["classification"] == "ORDER_PATH"]
    entry_band = [row for row in matches if row["classification"] == "ENTRY_BAND"]
    sim_learning = [row for row in matches if row["classification"] == "SIM_LEARNING"]
    dashboard_upload = [row for row in matches if row["classification"] == "DASHBOARD_UPLOAD"]
    uncertain = [row for row in matches if row["classification"] == "UNCERTAIN"]
    unique_sites = {row["site_key"] for row in matches}
    unique_lines = {(row["file"], row["line"]) for row in matches}
    return {
        "expected": {
            "order_path_real_must_include": [
                "bot/auto_trader.py:882",
                "bot/auto_trader.py:4607",
                "bot/telegram_bot.py:4259-4260",
                "bot/trading_coo.py:2664-2665",
                "data/trade_object.py:181",
                "data/trade_tracker.py:482",
                "data/limit_up_engine.py:81/416/429/431",
                "data/limit_up_scanner.py:925",
                "data/morning_recommendation.py:4281",
                "engine/limit_up_paper_trader.py:43-44",
            ],
            "excluded_examples": [
                "data/trade_learner.py:41-42 => SIM_LEARNING",
                "data/upload_swing.py:303-304 => DASHBOARD_UPLOAD",
            ],
            "pass_order_path_real": 0,
        },
        "found": {
            "site_count": len(unique_sites),
            "line_count": len(unique_lines),
            "order_path_real": order_path_real,
            "entry_band_excluded": entry_band,
            "sim_learning_excluded": sim_learning,
            "dashboard_upload": dashboard_upload,
            "uncertain": uncertain,
        },
        "verdict": "PASS" if not order_path_real else "FAIL",
    }


def check_7_intent_forensic_fields() -> dict[str, Any]:
    source = _read_source(SCALPER_ROOT / "bot" / "order_intent.py")
    keys: set[str] = set()
    for node in ast.walk(source.tree):
        if not isinstance(node, ast.FunctionDef) or node.name != "record_order_intent":
            continue
        for child in ast.walk(node):
            if not isinstance(child, ast.Assign):
                continue
            if not any(isinstance(t, ast.Name) and t.id == "record" for t in child.targets):
                continue
            if isinstance(child.value, ast.Dict):
                for key in child.value.keys:
                    if isinstance(key, ast.Constant) and isinstance(key.value, str):
                        keys.add(key.value)
    required = ["order_no", "rt_cd", "filled_qty", "avg_fill_price"]
    missing = [key for key in required if key not in keys]
    return {
        "expected": {"required_keys": required},
        "found": {"record_keys": sorted(keys), "missing": missing},
        "verdict": "PASS" if not missing else "FAIL",
    }


class CashRuleVisitor(ast.NodeVisitor):
    def __init__(self, source: SourceFile) -> None:
        self.source = source
        self.stack: list[ast.AST] = []
        self.rows: list[dict[str, Any]] = []

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self.stack.append(node)
        self.generic_visit(node)
        self.stack.pop()

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        self.visit_FunctionDef(node)  # type: ignore[arg-type]

    def visit_Call(self, node: ast.Call) -> None:
        if isinstance(node.func, ast.Attribute) and node.func.attr == "get":
            if (
                len(node.args) >= 2
                and isinstance(node.args[0], ast.Constant)
                and node.args[0].value == "min_cash_ratio"
                and isinstance(node.args[1], ast.Constant)
                and isinstance(node.args[1].value, (int, float))
            ):
                func = self.stack[-1] if self.stack else node
                self.rows.append({
                    "file": self.source.rel,
                    "line": node.lineno,
                    "function": getattr(func, "name", "<module>"),
                    "kind": "min_cash_ratio_fallback",
                    "value": node.args[1].value,
                    "text": self.source.lines[node.lineno - 1].strip()[:180],
                })
        self.generic_visit(node)

    def visit_Assign(self, node: ast.Assign) -> None:
        if isinstance(node.value, ast.Constant) and node.value.value == 0.10:
            for target in node.targets:
                name = ""
                if isinstance(target, ast.Name):
                    name = target.id
                elif isinstance(target, ast.Attribute):
                    name = target.attr
                if "CASH_RESERVE" in name:
                    func = self.stack[-1] if self.stack else node
                    self.rows.append({
                        "file": self.source.rel,
                        "line": node.lineno,
                        "function": getattr(func, "name", "<module>"),
                        "kind": "cash_reserve_constant",
                        "value": node.value.value,
                        "text": self.source.lines[node.lineno - 1].strip()[:180],
                    })
        self.generic_visit(node)


def _classify_check8(source: SourceFile) -> tuple[str, str]:
    if _path_is_sim_learning(source.rel):
        return "SIM_LEARNING", "module excluded as simulation/learning/backtest"
    if _path_is_dashboard_upload(source.rel):
        return "DASHBOARD_UPLOAD", "upload module separated from order gate"
    if source.rel in ORDER_PATH_MODULES or source.rel in CHECK8_REAL_EXTRA_MODULES:
        return "ORDER_PATH", "cash rule affects order/budget path"
    return "UNCERTAIN", "cash rule relevance not proven"


def check_8_cash30_single_source(sources: list[SourceFile]) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    for source in sources:
        if source.rel == "tools/audit_order_paths.py" or source.rel == "data/sajang_rules.py":
            continue
        visitor = CashRuleVisitor(source)
        visitor.visit(source.tree)
        classification, reason = _classify_check8(source)
        for row in visitor.rows:
            row["classification"] = classification
            row["classification_reason"] = reason
            row["site_key"] = f"{row['file']}:{row['line']}:{row['kind']}"
            rows.append(row)
    order_path_real = [row for row in rows if row["classification"] == "ORDER_PATH"]
    excluded = [row for row in rows if row["classification"] != "ORDER_PATH"]
    return {
        "expected": {
            "order_path_real": [
                "bot/kis_trader.py:1803",
                "bot/trading_cfo.py:106",
                "data/trade_object.py:31",
            ],
            "single_truth": "SAJANG.CASH_RESERVE_PCT",
        },
        "found": {
            "order_path_real": order_path_real,
            "excluded": excluded,
            "count": len(rows),
            "real_count": len(order_path_real),
        },
        "verdict": "PASS" if not order_path_real else "FAIL",
    }


def _issue_ids(report: dict[str, Any]) -> set[str]:
    checks = report.get("checks", {})
    ids: set[str] = set()
    check1 = checks.get("CHECK-1_paper_open_close_no_intent", {})
    missing1 = check1.get("missing_intent", {})
    if isinstance(missing1, dict):
        for kind, rows in missing1.items():
            if isinstance(rows, list):
                for row in rows:
                    ids.add(f"CHECK-1:{kind}:{row.get('file')}:{row.get('line')}")
    check2 = checks.get("CHECK-2_tradetracker_no_intent", {})
    found2 = check2.get("found", {})
    if isinstance(found2, dict):
        for row in found2.get("methods", []) or []:
            if not row.get("has_intent"):
                ids.add(f"CHECK-2:{row.get('name')}:{row.get('line')}")
    check4 = checks.get("CHECK-4_gate_write_enforce", {})
    found4 = check4.get("found", {})
    if isinstance(found4, dict):
        for name, row in found4.items():
            if isinstance(row, dict) and not row.get("propagates_block"):
                ids.add(f"CHECK-4:{name}:{row.get('line')}")
    check5 = checks.get("CHECK-5_order_gate_coverage", {})
    found5 = check5.get("found", {})
    if isinstance(found5, dict):
        for row in found5.get("methods", []) or []:
            if not row.get("has_order_gate"):
                ids.add(f"CHECK-5:{row.get('name')}:{row.get('line')}")
    check6 = checks.get("CHECK-6_fixed_tp_sl_violation", {})
    found6 = check6.get("found", {})
    if isinstance(found6, dict):
        rows6 = found6.get("order_path_real", []) or found6.get("violations", []) or []
        for row in rows6:
            ids.add(f"CHECK-6:{row.get('file')}:{row.get('line')}")
    check7 = checks.get("CHECK-7_intent_forensic_fields", {})
    found7 = check7.get("found", {})
    if isinstance(found7, dict):
        for key in found7.get("missing", []) or []:
            ids.add(f"CHECK-7:{key}")
    check8 = checks.get("CHECK-8_cash30_single_source", {})
    found8 = check8.get("found", {})
    if isinstance(found8, dict):
        rows8 = found8.get("order_path_real", []) or found8.get("non_sajang_sites", []) or []
        for row in rows8:
            ids.add(f"CHECK-8:{row.get('file')}:{row.get('line')}")
    return ids


def compare_baseline(baseline: dict[str, Any], current: dict[str, Any]) -> dict[str, Any]:
    old_ids = _issue_ids(baseline)
    new_ids = _issue_ids(current)
    added = sorted(new_ids - old_ids)
    resolved = sorted(old_ids - new_ids)
    return {
        "baseline_repo_head": baseline.get("repo_head"),
        "current_repo_head": current.get("repo_head"),
        "new_violation_count": len(added),
        "resolved_violation_count": len(resolved),
        "new_violations": added,
        "resolved_violations": resolved,
    }


def _load_json_file(path: Path) -> dict[str, Any]:
    last_error: Exception | None = None
    for encoding in ("utf-8-sig", "utf-16"):
        try:
            return json.loads(path.read_text(encoding=encoding))
        except Exception as exc:
            last_error = exc
    raise RuntimeError(f"failed to load JSON baseline {path}: {last_error}")


def run_audit() -> dict[str, Any]:
    sources = _load_sources()
    checks = {
        "CHECK-1_paper_open_close_no_intent": check_1_paper_open_close(sources),
        "CHECK-2_tradetracker_no_intent": check_2_tradetracker(),
        "CHECK-3_caller_preblock": check_3_caller_preblock(sources),
        "CHECK-4_gate_write_enforce": check_4_gate_write_enforce(),
        "CHECK-5_order_gate_coverage": check_5_order_gate_coverage(),
        "CHECK-6_fixed_tp_sl_violation": check_6_fixed_tp_sl(sources),
        "CHECK-7_intent_forensic_fields": check_7_intent_forensic_fields(),
        "CHECK-8_cash30_single_source": check_8_cash30_single_source(sources),
    }
    fail_count = sum(1 for check in checks.values() if check.get("verdict") == "FAIL")
    gate_pass = all(checks[name].get("verdict") == "PASS" for name in GATE_REQUIRED_CHECKS)
    gate = {
        "required": GATE_REQUIRED_NUMBERS,
        "check3_monitor": checks["CHECK-3_caller_preblock"]["found"]["kis_total"],
        "overall": "PASS" if gate_pass else "FAIL",
    }
    return {
        "tool": TOOL_NAME,
        "version": TOOL_VERSION,
        "generated_by": "deterministic_static_analysis",
        "repo_head": _run_git(["rev-parse", "HEAD"]),
        "repo_status_short": _run_git(["status", "--short", "--", "."]),
        "scanned_root": str(SCALPER_ROOT),
        "checks": checks,
        "overall": "PASS" if fail_count == 0 else "FAIL",
        "fail_count": fail_count,
        "gate": gate,
    }


def print_human(report: dict[str, Any]) -> None:
    print(f"{TOOL_NAME} v{TOOL_VERSION}")
    print(f"repo_head: {report['repo_head']}")
    print(
        f"overall: {report['overall']} fail_count={report['fail_count']} "
        f"gate={report['gate']['overall']}"
    )
    print("")
    for name, check in report["checks"].items():
        print(f"{name}: {check.get('verdict')}")
        found = check.get("found", {})
        if isinstance(found, dict):
            if "count" in found:
                print(f"  count: {found['count']}")
            if "site_count" in found:
                print(f"  site_count: {found['site_count']} line_count={found.get('line_count')}")
                print(f"  order_path_real: {len(found.get('order_path_real', []))}")
            if "missing_intent_count" in found:
                print(f"  missing_intent_count: {found['missing_intent_count']}")
            if "kis_total" in found:
                print(
                    "  caller_preblock: "
                    f"auto_trader={found.get('auto_trader_total')} "
                    f"sajang={found.get('auto_trader_sajang_branch')} "
                    f"kis_total={found.get('kis_total')}"
                )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Static audit for scalper order paths")
    parser.add_argument("--json", action="store_true", help="print JSON evidence bundle")
    parser.add_argument("--gate", action="store_true", help="print gate verdict and exit by gate")
    parser.add_argument("--baseline", help="compare current JSON result against a previous result")
    args = parser.parse_args(argv)

    try:
        report = run_audit()
        if args.baseline:
            baseline_path = Path(args.baseline)
            baseline = _load_json_file(baseline_path)
            report["baseline_diff"] = compare_baseline(baseline, report)
    except Exception as exc:
        error_report = {
            "tool": TOOL_NAME,
            "version": TOOL_VERSION,
            "overall": "ERROR",
            "error": str(exc),
        }
        if args.json:
            print(json.dumps(error_report, indent=2, sort_keys=True))
        else:
            print(f"{TOOL_NAME}: ERROR: {exc}", file=sys.stderr)
        return 2

    if args.json:
        print(json.dumps(report, indent=2, sort_keys=True))
    elif args.gate:
        print(report["gate"]["overall"])
    else:
        print_human(report)

    if args.gate:
        return 0 if report["gate"]["overall"] == "PASS" else 1
    return 0 if report["overall"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
