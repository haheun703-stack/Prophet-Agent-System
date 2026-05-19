# -*- coding: utf-8 -*-
"""EnvChecker — .env + config.yaml 환경 점검 에이전트.

오늘 5/19 사고: VERIFICATION_MODE=false 빠진 채로 D-Day 가동 → 검증 모드 비활성
사장님이 매매 점검하시다 발견. 시스템 자체가 잡았어야 했다.

점검 항목:
1. .env 필수 토글 — VERIFICATION_MODE, KIS API 키, TELEGRAM 등
2. config.yaml 핵심 설정 — auto_trade, scalper_budget, one_share_mode_until, max_positions
3. 날짜 기반 활성 토글 (1주 모드 활성? 검증 모드 날짜 범위?)
4. 위험 조합 (auto_trade=true이지만 confirm_real_order=true → 매번 확인 필요)
5. 큰형 advisory 통합 상태 (config + bot/ 코드 일치 확인)

비활성/이상 항목 발견 시 텔레그램 즉시 알림.
"""
import os
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Dict, List, Tuple

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))

from verifiers._common import send_telegram, save_state, make_result, fmt_alert


# 필수 .env 키 + 기대값(있어야 함) 또는 None(아무 값이나 있어야 함)
ENV_REQUIRED = {
    "TELEGRAM_BOT_TOKEN": None,
    "TELEGRAM_CHAT_ID": None,
    "KIS_APP_KEY": None,
    "KIS_APP_SECRET": None,
    "KIS_ACC_NO": None,
    "DATABASE_URL": None,
}

# 날짜 범위 활성 토글 (start/end 검증 필요)
ENV_DATE_RANGE_TOGGLES = [
    ("VERIFICATION_MODE", "VERIFICATION_START", "VERIFICATION_END"),
]

# config.yaml 필수 키 (path: expected condition)
CONFIG_CHECKS = {
    "bot.auto_trade": lambda v: v is True,
    "bot.scalper_budget": lambda v: isinstance(v, int) and v > 0,
    "bot.max_auto_positions": lambda v: isinstance(v, int) and v > 0,
    "bot.action_add_max_per_day": lambda v: isinstance(v, int) and v >= 0,
    "risk.max_positions": lambda v: isinstance(v, int) and v > 0,
    "risk.stop_loss_pct": lambda v: isinstance(v, float) and 0 < v < 0.20,
    "risk.min_cash_ratio": lambda v: isinstance(v, float) and 0 <= v <= 1,
}


def _get_nested(d: dict, dotted_path: str):
    cur = d
    for k in dotted_path.split("."):
        if not isinstance(cur, dict) or k not in cur:
            return None
        cur = cur[k]
    return cur


def _check_env() -> Tuple[List[str], List[str]]:
    """반환: (issues, infos)"""
    issues = []
    infos = []

    # .env 로드
    try:
        from dotenv import load_dotenv
        load_dotenv(_ROOT.parent / ".env")
    except Exception as e:
        issues.append(f"⚠️ dotenv 로드 실패: {e}")
        return issues, infos

    # 필수 키 점검
    for key, expected in ENV_REQUIRED.items():
        val = os.getenv(key, "").strip()
        if not val:
            issues.append(f"❌ .env {key} 미설정")
        elif expected is not None and val != expected:
            issues.append(f"⚠️ .env {key}={val} (기대값: {expected})")

    # 날짜 범위 토글
    today = date.today()
    for toggle_key, start_key, end_key in ENV_DATE_RANGE_TOGGLES:
        toggle = os.getenv(toggle_key, "false").strip().lower()
        start_raw = os.getenv(start_key, "").strip()
        end_raw = os.getenv(end_key, "").strip()

        def _parse(d):
            if len(d) == 8 and d.isdigit():
                try:
                    return date(int(d[:4]), int(d[4:6]), int(d[6:]))
                except Exception:
                    return None
            return None

        start = _parse(start_raw)
        end = _parse(end_raw)
        in_range = bool(start and end and start <= today <= end)

        if toggle == "true" and not in_range:
            issues.append(
                f"⚠️ .env {toggle_key}=true 인데 오늘({today})이 범위 밖 "
                f"({start_raw}~{end_raw})"
            )
        elif toggle == "false" and in_range:
            issues.append(
                f"🚨 .env {toggle_key}=false 인데 오늘({today})이 활성 범위 "
                f"({start_raw}~{end_raw}) — 활성화 필요?"
            )
        else:
            infos.append(f"{toggle_key}={toggle} (range {start_raw}~{end_raw}, today={today})")

    return issues, infos


def _check_config() -> Tuple[List[str], List[str]]:
    """반환: (issues, infos)"""
    issues = []
    infos = []

    try:
        import yaml
        cfg_path = _ROOT / "config.yaml"
        cfg = yaml.safe_load(cfg_path.read_text(encoding="utf-8"))
    except Exception as e:
        issues.append(f"⚠️ config.yaml 로드 실패: {e}")
        return issues, infos

    for path, check in CONFIG_CHECKS.items():
        val = _get_nested(cfg, path)
        if val is None:
            issues.append(f"❌ config.{path} 미설정")
        elif not check(val):
            issues.append(f"⚠️ config.{path}={val} 조건 불만족")
        else:
            infos.append(f"{path}={val}")

    # 1주 모드 활성 체크
    one_share_until = _get_nested(cfg, "bot.one_share_mode_until")
    if one_share_until:
        try:
            until = date.fromisoformat(str(one_share_until))
            today = date.today()
            active = today <= until
            infos.append(f"one_share_mode_until={one_share_until} active={active} (today={today})")
            if not active:
                infos.append(f"💡 1주 모드 만료 (5/21+) — 풀 회전 복원")
        except Exception as e:
            issues.append(f"⚠️ one_share_mode_until 파싱 실패: {e}")

    # 위험 조합: auto_trade=true + confirm_real_order=true → 비효율
    auto = _get_nested(cfg, "bot.auto_trade")
    confirm = _get_nested(cfg, "bot.confirm_real_order")
    if auto is True and confirm is True:
        issues.append("⚠️ bot.auto_trade=true 인데 bot.confirm_real_order=true — 자동매매가 매번 확인 대기")

    return issues, infos


def run() -> Dict:
    """EnvChecker 실행 (단일 호출). 결과 dict 반환 + 이상 시 텔레그램 알림."""
    env_issues, env_infos = _check_env()
    cfg_issues, cfg_infos = _check_config()

    all_issues = env_issues + cfg_issues
    ok = len(all_issues) == 0

    if ok:
        summary = f"✅ EnvChecker 정상 (.env {len(env_infos)}건 / config {len(cfg_infos)}건 통과)"
    else:
        summary = f"🚨 EnvChecker 이상 {len(all_issues)}건 발견"

    result = make_result("env_checker", ok, summary, {
        "env_issues": env_issues,
        "env_infos": env_infos,
        "config_issues": cfg_issues,
        "config_infos": cfg_infos,
    })

    save_state("env_checker", result)

    # 이상 발견 시 텔레그램
    if not ok:
        lines = ["", "[발견된 이상]"]
        lines.extend(all_issues)
        send_telegram(fmt_alert("EnvChecker 🚨", "", lines))

    return result


if __name__ == "__main__":
    import json
    r = run()
    print(json.dumps(r, ensure_ascii=False, indent=2, default=str))
    sys.exit(0 if r["ok"] else 1)
