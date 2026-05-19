# -*- coding: utf-8 -*-
"""Reporter — 5명(나 제외 4명) 검수 결과 통합 보고.

사용처:
1. 일 1회 자동 (장 마감 후 16:00) — 텔레그램 일일 종합 보고
2. 텔레그램 /검수 명령 → 즉시 종합 실행 + 보고
3. 봇 시작 직후 — 5명 모두 실행 후 한 번에 보고

사장님이 매번 안 챙겨도 되도록 — 정상이면 정상이라고, 이상 있으면 정리해서.
"""
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))

from verifiers._common import send_telegram, save_state, load_state, make_result


def run_all() -> Dict:
    """4명 검수 에이전트 실행 → 결과 통합."""
    from verifiers import env_checker, data_integrity, flow_monitor, code_auditor

    results = {}
    for name, mod in [
        ("env", env_checker),
        ("data", data_integrity),
        ("flow", flow_monitor),
        ("code", code_auditor),
    ]:
        try:
            results[name] = mod.run()
        except Exception as e:
            results[name] = {"verifier": name, "ok": False, "summary": f"실행 실패: {e}"}

    all_ok = all(r.get("ok") for r in results.values())
    issue_count = sum(0 if r.get("ok") else 1 for r in results.values())

    if all_ok:
        summary = "✅ Verifier 팀 4명 전원 정상"
    else:
        summary = f"🚨 Verifier 팀 {issue_count}/4명 이상"

    final = make_result("reporter", all_ok, summary, {
        "env": results["env"].get("summary"),
        "data": results["data"].get("summary"),
        "flow": results["flow"].get("summary"),
        "code": results["code"].get("summary"),
    })
    save_state("reporter_last_run", final)
    return final


def send_summary_telegram(send_even_if_ok: bool = True) -> bool:
    """4명 결과 종합 → 텔레그램. send_even_if_ok=False면 이상 시만."""
    r = run_all()
    lines = [
        "📋 Verifier 팀 검수 결과",
        f"  · 시각: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"  · 종합: {r['summary']}",
        "",
    ]
    details = r["details"]
    for key, label in [
        ("env", "EnvChecker"),
        ("data", "DataIntegrity"),
        ("flow", "FlowMonitor"),
        ("code", "CodeAuditor"),
    ]:
        lines.append(f"• {label}: {details.get(key, 'N/A')}")

    # 이상 있는 항목은 details 추가
    if not r["ok"]:
        lines.append("")
        lines.append("[상세] 각 verifier 텔레그램 알림 별도 전송됨")

    if send_even_if_ok or not r["ok"]:
        return send_telegram("\n".join(lines))
    return False


if __name__ == "__main__":
    arg = sys.argv[1] if len(sys.argv) > 1 else "run"
    if arg == "send":
        send_summary_telegram(send_even_if_ok=True)
    else:
        r = run_all()
        print(json.dumps(r, ensure_ascii=False, indent=2, default=str))
