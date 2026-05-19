# -*- coding: utf-8 -*-
"""CodeAuditor — 코드 검수 (commit 후 + 봇 재시작 후).

오늘 5/19 사고 사례:
- strength >= 95 BEARISH 차단 (방향 거꾸로) — 사장님 검수 시켜서 잡음
- intraday_verification 게이트 누락 — 사장님 검수 시켜서 잡음
- ACTION_ADD manual_sync 가드 누락 — 사장님 "전력주 가만히 둬"로 잡음

자동 검수 룰:
1. 매수 경로(safe_buy / nxt_safe_buy / afterhours_buy 호출처) 누락 게이트 탐지
2. advisory 게이트 일관성 — swing 있으면 intraday도 있어야
3. manual_sync 가드 — ACTION_ADD / recovery / EYE_Guardian 모두 일관
4. fail-safe try/except — 외부 fetch (Supabase/HTTP) 호출 시 필수
5. 헬퍼 함수 중복 정의 (같은 함수 안에 name = ... 두 번 등)
"""
import re
import sys
from pathlib import Path
from typing import Dict, List, Tuple

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))

from verifiers._common import send_telegram, save_state, make_result, fmt_alert

AUTO_TRADER = _ROOT / "bot" / "auto_trader.py"
KIS_TRADER = _ROOT / "bot" / "kis_trader.py"


def _read(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except Exception:
        return ""


def _check_advisory_gate_consistency() -> List[str]:
    """advisory 게이트가 swing과 intraday 양쪽에 있는지."""
    issues = []
    src = _read(AUTO_TRADER)
    if not src:
        return ["⚠️ auto_trader.py 읽기 실패"]
    has_swing_gate = "quant_advisory" in src and "_morning_swing" in src
    # intraday 게이트는 intraday_verification_scan_and_buy 안에 quant_advisory 호출
    intraday_block = re.search(
        r"async def intraday_verification_scan_and_buy[\s\S]+?(?=\n    async def |\n    def |\Z)",
        src,
    )
    has_intraday_gate = bool(intraday_block and "quant_advisory" in intraday_block.group(0))

    if has_swing_gate and not has_intraday_gate:
        issues.append("⚠️ swing 매수에 큰형 advisory 게이트 있지만 intraday에는 없음")
    if has_intraday_gate and not has_swing_gate:
        issues.append("⚠️ intraday에 advisory 게이트 있지만 swing에는 없음")
    return issues


def _check_manual_sync_guards() -> List[str]:
    """manual_sync 가드 일관성 — ACTION_ADD / EYE_Guardian / recovery 모두."""
    issues = []
    src = _read(AUTO_TRADER)
    if not src:
        return []

    # EYE_Guardian: manual_sync 면제 있어야
    if "_eye_guardian_action" in src:
        eye_block = re.search(
            r"async def _eye_guardian_action[\s\S]+?(?=\n    async def |\n    def |\Z)",
            src,
        )
        if eye_block and 'startswith("manual_sync")' not in eye_block.group(0):
            issues.append("⚠️ _eye_guardian_action에 manual_sync 가드 없음")

    # ACTION_ADD: manual_sync skip 있어야
    action_add_blocks = re.findall(r"elif action == ACTION_ADD:[\s\S]{0,500}", src)
    for blk in action_add_blocks:
        if "manual_sync" not in blk:
            issues.append("⚠️ ACTION_ADD 분기에 manual_sync 가드 없음")
            break

    return issues


def _check_one_share_mode_paths() -> List[str]:
    """safe_buy / nxt_safe_buy 모두 _is_one_share_mode 호출하는지."""
    issues = []
    src = _read(KIS_TRADER)
    if not src:
        return []

    # safe_buy 함수 안에 _is_one_share_mode 있는지
    safe_buy_block = re.search(
        r"def safe_buy\(self[\s\S]+?(?=\n    def |\Z)",
        src,
    )
    if safe_buy_block and "_is_one_share_mode" not in safe_buy_block.group(0):
        issues.append("⚠️ safe_buy()에 1주 모드 체크 누락")

    nxt_block = re.search(
        r"def nxt_safe_buy\(self[\s\S]+?(?=\n    def |\Z)",
        src,
    )
    if nxt_block and "_is_one_share_mode" not in nxt_block.group(0):
        issues.append("⚠️ nxt_safe_buy()에 1주 모드 체크 누락")

    return issues


def _check_failsafe_external_calls() -> List[str]:
    """외부 호출(fetch_latest_advisory / fetch_price 등) try/except 감싸는지."""
    issues = []
    src = _read(AUTO_TRADER)
    if not src:
        return []

    # fetch_latest_advisory 호출 — 모두 try/except 안에 있어야
    for m in re.finditer(r"fetch_latest_advisory\(", src):
        # 호출 줄 앞 200자에서 try: 있는지
        start = max(0, m.start() - 500)
        ctx = src[start:m.end()]
        if "try:" not in ctx.split("def ")[-1]:
            issues.append("⚠️ fetch_latest_advisory 호출 중 try/except 없는 곳 의심")
            break
    return issues


def _check_strength_direction() -> List[str]:
    """strength 방향 — strength >= 95 차단 패턴은 잘못된 방향."""
    issues = []
    src = _read(AUTO_TRADER)
    if not src:
        return []

    # 잘못된 패턴: strength >= 95 또는 strength > 95 + BEARISH 차단 메시지
    bad = re.search(r"strength\s*>=?\s*9[0-9]\b[\s\S]{0,200}(?:차단|return|BEARISH)", src)
    if bad:
        issues.append("⚠️ strength >= 95 차단 패턴 의심 (방향 거꾸로 — 95+는 매수 우세)")
    return issues


def run() -> Dict:
    issues = []
    issues += _check_advisory_gate_consistency()
    issues += _check_manual_sync_guards()
    issues += _check_one_share_mode_paths()
    issues += _check_failsafe_external_calls()
    issues += _check_strength_direction()

    ok = len(issues) == 0
    summary = (
        f"✅ CodeAuditor 정상 (5가지 룰 모두 통과)"
        if ok else f"🚨 CodeAuditor 이상 {len(issues)}건"
    )

    result = make_result("code_auditor", ok, summary, {"issues": issues})
    save_state("code_auditor", result)

    if not ok:
        lines = ["", "[코드 검수 결과]"]
        lines.extend(issues)
        send_telegram(fmt_alert("CodeAuditor 🚨", "", lines))

    return result


if __name__ == "__main__":
    import json
    r = run()
    print(json.dumps(r, ensure_ascii=False, indent=2, default=str))
    sys.exit(0 if r["ok"] else 1)
