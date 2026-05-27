# -*- coding: utf-8 -*-
"""
Read-only order forensics.

This tool exists for one rule: do not guess who placed an order.
It collects account executions from KIS when available and local bot evidence
from logs/files. It never places, cancels, or modifies orders.

Examples:
    python tools/order_forensics.py --date 2026-05-27 --codes 007390 009190
    python tools/order_forensics.py --date 2026-05-27 --codes 007390 009190 --offline
"""

import argparse
import csv
import json
import os
import re
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any


SCALPER_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = SCALPER_ROOT.parent
RESULT_DIR = SCALPER_ROOT / "results" / "order_forensics"
DEFAULT_TERMS = [
    "\ub124\uc774\ucc98\uc140",
    "\ub300\uc591\uae08\uc18d",
    "safe_buy",
    "buy_market",
    "sell_market",
    "ORDER",
    "order",
    "blocked",
    "AUTO_TRADE_DISABLED",
]

sys.path.insert(0, str(SCALPER_ROOT))


def _json_default(obj: Any) -> str:
    return str(obj)


def _safe_int(value: Any) -> int:
    try:
        if value is None or value == "":
            return 0
        return int(float(str(value).replace(",", "")))
    except Exception:
        return 0


def _today() -> str:
    return datetime.now().strftime("%Y-%m-%d")


def _yyyymmdd(date_text: str) -> str:
    return date_text.replace("-", "")


def _run_git(args: list[str]) -> str:
    try:
        res = subprocess.run(
            ["git", "-C", str(REPO_ROOT), *args],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=8,
        )
        return (res.stdout or res.stderr).strip()
    except Exception as exc:
        return f"git_error: {exc}"


def collect_runtime_state() -> dict[str, Any]:
    state: dict[str, Any] = {
        "repo_root": str(REPO_ROOT),
        "scalper_root": str(SCALPER_ROOT),
        "git_head": _run_git(["rev-parse", "--short", "HEAD"]),
        "git_status_short": _run_git(["status", "--short"]),
    }
    try:
        from data.sajang_rules import SAJANG

        state["sajang_flags"] = {
            "AUTO_TRADE_DISABLED": bool(getattr(SAJANG, "AUTO_TRADE_DISABLED", False)),
            "MORNING_AUTO_BUY_DISABLED": bool(
                getattr(SAJANG, "MORNING_AUTO_BUY_DISABLED", False)
            ),
        }
    except Exception as exc:
        state["sajang_flags_error"] = str(exc)
    return state


def _candidate_text_files(scan_all: bool, date_text: str) -> list[Path]:
    date_compact = _yyyymmdd(date_text)
    roots = [
        SCALPER_ROOT / "logs",
        REPO_ROOT / "logs",
        SCALPER_ROOT / "data",
        SCALPER_ROOT / "data_store",
        SCALPER_ROOT / "results",
    ]
    suffixes = {".log", ".txt", ".json", ".jsonl", ".md", ".csv"}
    skip_dirs = {
        "__pycache__",
        ".pytest_cache",
        "daily",
        "15min",
        "1min",
        "5min",
        "minute5",
        "minute",
        "minutes",
        "ohlcv",
        "cache",
        "equal_levels",
        "etf_backtest",
        "etf_daily",
        "flow",
        "gap_levels",
        "jarvis_learning",
        "krx_cache",
        "learning",
        "nationality",
        "news",
        "news_ai",
        "news_sentiment",
        "opening_range",
        "premium_levels",
        "processed",
        "raw",
        "scan_results",
        "sector_momentum_history",
        "short",
        "signals",
        "supabase_migrations",
        "surge_learning",
        "ticks",
        "tv_history",
        "verifiers",
        "order_forensics",
    }
    files: list[Path] = []
    for root in roots:
        if not root.exists():
            continue
        for dirpath, dirnames, filenames in os.walk(root):
            dirnames[:] = [d for d in dirnames if d not in skip_dirs]
            for filename in filenames:
                path = Path(dirpath) / filename
                if path.suffix.lower() not in suffixes:
                    continue
                name = path.name
                if scan_all or date_text in name or date_compact in name:
                    files.append(path)
    return sorted(set(files))


def scan_local_evidence(
    date_text: str,
    codes: list[str],
    terms: list[str],
    scan_all: bool,
    max_matches: int,
    max_file_bytes: int,
) -> list[dict[str, Any]]:
    patterns = [p for p in [*DEFAULT_TERMS, *codes, *terms] if p]
    regex_parts: list[str] = []
    for pattern in sorted(set(patterns), key=len, reverse=True):
        escaped = re.escape(pattern)
        if re.fullmatch(r"\d{6}", pattern):
            regex_parts.append(rf"(?<!\d){escaped}(?!\d)")
        else:
            regex_parts.append(escaped)
    regex = re.compile("|".join(regex_parts), re.I)
    matches: list[dict[str, Any]] = []
    for path in _candidate_text_files(scan_all, date_text):
        try:
            size = path.stat().st_size
            if size > max_file_bytes:
                matches.append(
                    {
                        "file": str(path),
                        "skipped": f"large file ({size} bytes > {max_file_bytes})",
                    }
                )
                if len(matches) >= max_matches:
                    return matches
                continue
            with path.open("r", encoding="utf-8", errors="replace") as fh:
                for lineno, line in enumerate(fh, start=1):
                    if not regex.search(line):
                        continue
                    matches.append(
                        {
                            "file": str(path),
                            "line": lineno,
                            "text": line.rstrip("\n")[:800],
                        }
                    )
                    if len(matches) >= max_matches:
                        return matches
        except Exception as exc:
            matches.append({"file": str(path), "error": str(exc)})
            if len(matches) >= max_matches:
                return matches
    return matches


def _load_env() -> None:
    try:
        from dotenv import load_dotenv

        load_dotenv(REPO_ROOT / ".env", override=False)
        load_dotenv(SCALPER_ROOT / ".env", override=False)
    except Exception:
        pass


def _auth_headers(broker: Any, tr_id: str, token_override: str | None = None) -> dict[str, str]:
    token = token_override or getattr(broker, "access_token", "")
    if token and not str(token).startswith("Bearer "):
        token = f"Bearer {token}"
    return {
        "Content-Type": "application/json; charset=utf-8",
        "authorization": str(token),
        "appkey": str(getattr(broker, "api_key", os.getenv("KIS_APP_KEY", ""))),
        "appsecret": str(getattr(broker, "api_secret", os.getenv("KIS_APP_SECRET", ""))),
        "tr_id": tr_id,
        "custtype": "P",
    }


def _fresh_access_token(base_url: str, app_key: str, app_secret: str) -> str | None:
    try:
        import requests

        resp = requests.post(
            f"{base_url}/oauth2/tokenP",
            json={
                "grant_type": "client_credentials",
                "appkey": app_key,
                "appsecret": app_secret,
            },
            headers={"Content-Type": "application/json; charset=utf-8"},
            timeout=10,
        )
        payload = resp.json()
        return payload.get("access_token")
    except Exception:
        return None


def _account_parts(broker: Any) -> tuple[str, str]:
    acc_no = str(getattr(broker, "acc_no", "") or os.getenv("KIS_ACC_NO", ""))
    acc_clean = acc_no.replace("-", "").replace(" ", "")
    if len(acc_clean) < 10:
        raise RuntimeError(f"KIS account number is invalid: {acc_no!r}")
    return acc_clean[:8], acc_clean[8:]


def _normalize_execution(row: dict[str, Any]) -> dict[str, Any]:
    side_code = str(row.get("sll_buy_dvsn_cd") or row.get("SLL_BUY_DVSN_CD") or "")
    side_name = str(row.get("sll_buy_dvsn_name") or row.get("trad_dvsn_name") or "")
    if not side_name:
        if side_code == "02":
            side_name = "BUY"
        elif side_code == "01":
            side_name = "SELL"
        else:
            side_name = side_code or "UNKNOWN"
    return {
        "order_date": row.get("ord_dt") or row.get("stck_bsop_date") or row.get("ORD_DT"),
        "order_time": row.get("ord_tmd") or row.get("ORD_TMD"),
        "order_no": row.get("odno") or row.get("ODNO"),
        "code": row.get("pdno") or row.get("PDNO"),
        "name": row.get("prdt_name") or row.get("PRDT_NAME"),
        "side": side_name,
        "side_code": side_code,
        "order_qty": _safe_int(row.get("ord_qty") or row.get("ORD_QTY")),
        "filled_qty": _safe_int(row.get("tot_ccld_qty") or row.get("ccld_qty")),
        "order_price": _safe_int(row.get("ord_unpr") or row.get("ORD_UNPR")),
        "avg_fill_price": _safe_int(row.get("avg_prvs") or row.get("avg_prvs_pric")),
        "raw": row,
    }


def fetch_kis_executions(date_text: str, codes: list[str]) -> dict[str, Any]:
    _load_env()
    import requests
    from bot.kis_trader import KISTrader

    trader = KISTrader()
    broker = trader._get_broker(force_refresh=True)
    cano, product = _account_parts(broker)
    base_url = getattr(broker, "base_url", "https://openapi.koreainvestment.com:9443")
    app_key = str(getattr(broker, "api_key", os.getenv("KIS_APP_KEY", "")))
    app_secret = str(getattr(broker, "api_secret", os.getenv("KIS_APP_SECRET", "")))
    fresh_token = _fresh_access_token(base_url, app_key, app_secret)
    url = f"{base_url}/uapi/domestic-stock/v1/trading/inquire-daily-ccld"
    ymd = _yyyymmdd(date_text)
    query_codes = codes or [""]
    attempts: list[dict[str, Any]] = []
    executions: list[dict[str, Any]] = []

    for code in query_codes:
        params = {
            "CANO": cano,
            "ACNT_PRDT_CD": product,
            "INQR_STRT_DT": ymd,
            "INQR_END_DT": ymd,
            "SLL_BUY_DVSN_CD": "00",
            "INQR_DVSN": "00",
            "PDNO": code,
            "CCLD_DVSN": "00",
            "ORD_GNO_BRNO": "",
            "ODNO": "",
            "INQR_DVSN_3": "00",
            "INQR_DVSN_1": "",
            "CTX_AREA_FK100": "",
            "CTX_AREA_NK100": "",
        }
        try:
            resp = requests.get(
                url,
                headers=_auth_headers(broker, "TTTC8001R", fresh_token),
                params=params,
                timeout=10,
            )
            try:
                payload = resp.json()
            except Exception:
                payload = {"raw_text": resp.text}
            if isinstance(payload, dict) and payload.get("msg_cd") == "EGW00123":
                broker = trader._get_broker(force_refresh=True)
                fresh_token = _fresh_access_token(base_url, app_key, app_secret)
                resp = requests.get(
                    url,
                    headers=_auth_headers(broker, "TTTC8001R", fresh_token),
                    params=params,
                    timeout=10,
                )
                try:
                    payload = resp.json()
                except Exception:
                    payload = {"raw_text": resp.text}
            attempts.append(
                {
                    "code": code,
                    "status_code": resp.status_code,
                    "rt_cd": payload.get("rt_cd") if isinstance(payload, dict) else None,
                    "msg_cd": payload.get("msg_cd") if isinstance(payload, dict) else None,
                    "msg1": payload.get("msg1") if isinstance(payload, dict) else None,
                    "raw": payload,
                }
            )
            if isinstance(payload, dict) and payload.get("rt_cd") == "0":
                rows = payload.get("output1") or payload.get("output") or []
                if isinstance(rows, dict):
                    rows = [rows]
                for row in rows:
                    if isinstance(row, dict):
                        normalized = _normalize_execution(row)
                        if not codes or normalized.get("code") in codes:
                            executions.append(normalized)
        except Exception as exc:
            attempts.append({"code": code, "error": str(exc)})

    return {"executions": executions, "attempts": attempts}


def write_outputs(
    date_text: str,
    runtime: dict[str, Any],
    kis: dict[str, Any] | None,
    local_matches: list[dict[str, Any]],
) -> dict[str, str]:
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    json_path = RESULT_DIR / f"order_forensics_{stamp}.json"
    csv_path = RESULT_DIR / f"order_forensics_{stamp}.csv"
    md_path = RESULT_DIR / f"order_forensics_{stamp}.md"
    latest_path = RESULT_DIR / "order_forensics_latest.md"

    payload = {
        "date": date_text,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "runtime": runtime,
        "kis": kis,
        "local_matches": local_matches,
        "rule": "No source attribution without KIS execution evidence plus route/log evidence.",
    }
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=_json_default), encoding="utf-8")

    executions = (kis or {}).get("executions", [])
    with csv_path.open("w", encoding="utf-8-sig", newline="") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=[
                "order_date",
                "order_time",
                "order_no",
                "code",
                "name",
                "side",
                "side_code",
                "order_qty",
                "filled_qty",
                "order_price",
                "avg_fill_price",
            ],
        )
        writer.writeheader()
        for row in executions:
            writer.writerow({k: row.get(k, "") for k in writer.fieldnames})

    md_lines = [
        "# Order Forensics",
        "",
        f"- date: {date_text}",
        f"- generated_at: {payload['generated_at']}",
        f"- git_head: {runtime.get('git_head')}",
        f"- AUTO_TRADE_DISABLED: {runtime.get('sajang_flags', {}).get('AUTO_TRADE_DISABLED')}",
        "",
        "## KIS executions",
    ]
    if kis is None:
        md_lines.append("- offline mode: KIS execution query skipped")
    elif executions:
        for row in executions:
            md_lines.append(
                "- {time} {side} {name}({code}) qty={qty} avg={avg} order_no={odno}".format(
                    time=row.get("order_time") or "?",
                    side=row.get("side") or "?",
                    name=row.get("name") or "?",
                    code=row.get("code") or "?",
                    qty=row.get("filled_qty") or row.get("order_qty") or 0,
                    avg=row.get("avg_fill_price") or row.get("order_price") or 0,
                    odno=row.get("order_no") or "?",
                )
            )
    else:
        md_lines.append("- no executions returned by KIS for the requested filter")
        for attempt in (kis or {}).get("attempts", [])[:6]:
            md_lines.append(
                f"- attempt code={attempt.get('code') or '(all)'} "
                f"rt_cd={attempt.get('rt_cd')} msg={attempt.get('msg1') or attempt.get('error')}"
            )

    md_lines.extend(["", "## Local evidence matches"])
    if local_matches:
        for item in local_matches[:80]:
            if "error" in item:
                md_lines.append(f"- {item.get('file')}: ERROR {item.get('error')}")
            elif "skipped" in item:
                md_lines.append(f"- {item.get('file')}: SKIPPED {item.get('skipped')}")
            else:
                md_lines.append(
                    f"- {item.get('file')}:{item.get('line')} | {item.get('text')}"
                )
    else:
        md_lines.append("- no matching local log/file lines found")

    md_lines.extend(
        [
            "",
            "## Attribution rule",
            "- KIS executions prove account activity only.",
            "- Bot/manual/source attribution requires matching route evidence from logs or order metadata.",
            "- If KIS and local logs disagree, treat the result as inconclusive until deployment state is checked.",
        ]
    )
    md_text = "\n".join(md_lines) + "\n"
    md_path.write_text(md_text, encoding="utf-8")
    latest_path.write_text(md_text, encoding="utf-8")
    return {
        "json": str(json_path),
        "csv": str(csv_path),
        "md": str(md_path),
        "latest": str(latest_path),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Read-only KIS/local order forensics")
    parser.add_argument("--date", default=_today(), help="trade date, e.g. 2026-05-27")
    parser.add_argument("--codes", nargs="*", default=[], help="stock codes to filter")
    parser.add_argument("--terms", nargs="*", default=[], help="extra log search terms")
    parser.add_argument("--offline", action="store_true", help="skip KIS API query")
    parser.add_argument("--scan-all", action="store_true", help="scan all local evidence files, not only dated files")
    parser.add_argument("--max-matches", type=int, default=200, help="max local evidence lines")
    parser.add_argument("--max-file-bytes", type=int, default=3_000_000, help="skip larger local files")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    codes = [str(c).zfill(6) for c in args.codes]
    runtime = collect_runtime_state()
    kis = None
    if not args.offline:
        kis = fetch_kis_executions(args.date, codes)
    local_matches = scan_local_evidence(
        args.date,
        codes,
        args.terms,
        scan_all=args.scan_all,
        max_matches=max(1, args.max_matches),
        max_file_bytes=max(1, args.max_file_bytes),
    )
    paths = write_outputs(args.date, runtime, kis, local_matches)
    print("order forensics complete")
    print(f"date: {args.date}")
    print(f"git_head: {runtime.get('git_head')}")
    if kis is None:
        print("kis: skipped (offline)")
    else:
        print(f"kis executions: {len(kis.get('executions', []))}")
    print(f"local matches: {len(local_matches)}")
    print(f"latest report: {paths['latest']}")
    print(f"csv: {paths['csv']}")
    print(f"json: {paths['json']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
