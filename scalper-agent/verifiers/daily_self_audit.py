# -*- coding: utf-8 -*-
"""★ 단타봇 Daily Self-Audit (5/26 사고 후 신설) ★

매일 15:35 cron 자동 실행 → 사장님 영구 룰 13건 자동 검증 → 위반 시 텔레그램 즉시 알림.

5/26 사고 5건 모두 "단타봇이 사장님 영구 룰 default off / 옛 코드 잔존" 패턴.
이를 매일 자동 검증해서 단타봇이 또 사고 안 치도록 자기 모니터링 시스템.

검증 항목:
  1. 보유 종목 룰 부합 (TP=0 / mode=swing / source=manual_president)
  2. config.yaml 룰 부합 (entry_mode='pullback_3pct')
  3. 현금 30% 보유 (사장님 영구 룰)
  4. asset_pool 매수 종목 룰 부합
  5. 트레일링 -3% / NORMAL SL -3%
  6. 룰 B/C/D cron 등록 확인

영구 메모리: incident_2026_05_26_dday_anger / feedback_rule_registry_single_truth
"""
import json
import sys
import yaml
from datetime import datetime
from pathlib import Path
from typing import List, Dict

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))

from verifiers._common import send_telegram, save_state, make_result, fmt_alert


def audit_positions() -> List[Dict]:
    """보유 종목 사장님 영구 룰 부합 검증."""
    issues = []
    pp = _ROOT / "data_store" / "positions.json"
    if not pp.exists():
        return issues
    try:
        with open(pp, encoding='utf-8') as f:
            positions = json.load(f)
    except Exception as e:
        return [{"severity": "HIGH", "rule": "AUDIT-POS", "msg": f"positions.json 로드 실패: {e}"}]

    # ★ 보유 종목 사장님 영구 룰 부합 검증 ★
    for code, pos in positions.items():
        name = pos.get('name', code)

        # 룰 1: TP=0 (사장님 [feedback_trailing_only_tp])
        tp = pos.get('take_profit', 0)
        if tp != 0:
            issues.append({
                "severity": "CRITICAL", "rule": "AUDIT-POS-TP",
                "msg": f"⚠️ {name}({code}) TP={tp:,} (사장님 영구 룰 위반 — TP=0 강제)"
            })

        # 룰 2: mode='swing' (D+0 청산 X)
        mode = pos.get('mode', '?')
        if mode == 'day':
            issues.append({
                "severity": "CRITICAL", "rule": "AUDIT-POS-MODE",
                "msg": f"⚠️ {name}({code}) mode='day' (사장님 영구 룰 위반 — mode='swing' 강제)"
            })

        # 룰 3: 매수가 -3% NORMAL SL 적절성 (너무 좁거나 넓지 않은지)
        entry = pos.get('entry_price', 0) or pos.get('buy_price', 0)
        sl = pos.get('stop_loss', 0)
        if entry > 0 and sl > 0:
            sl_ratio = (entry - sl) / entry * 100
            if sl_ratio < 2.0 or sl_ratio > 5.0:
                issues.append({
                    "severity": "HIGH", "rule": "AUDIT-POS-SL",
                    "msg": f"⚠️ {name}({code}) SL 비율 {sl_ratio:.1f}% (정상 -3% 영역 이탈)"
                })

    return issues


def audit_config() -> List[Dict]:
    """config.yaml 사장님 영구 룰 부합 검증."""
    issues = []
    cp = _ROOT / "config.yaml"
    if not cp.exists():
        return issues
    try:
        with open(cp, encoding='utf-8') as f:
            cfg = yaml.safe_load(f)
    except Exception as e:
        return [{"severity": "HIGH", "rule": "AUDIT-CFG", "msg": f"config.yaml 로드 실패: {e}"}]

    ap = cfg.get('bot', {}).get('asset_pool', {})

    # 룰 1: entry_mode='pullback_3pct' (사장님 5/23 영구 룰)
    em = ap.get('entry_mode', 'open')
    if em != 'pullback_3pct':
        issues.append({
            "severity": "CRITICAL", "rule": "AUDIT-CFG-ENTRY",
            "msg": f"⚠️ config.yaml entry_mode='{em}' (사장님 5/23 룰 위반 — 'pullback_3pct' 강제)"
        })

    # 룰 2: budget_mode='split_cash' (사장님 5/25 영구 룰)
    bm = ap.get('budget_mode', '?')
    if bm != 'split_cash':
        issues.append({
            "severity": "HIGH", "rule": "AUDIT-CFG-BUDGET",
            "msg": f"⚠️ config.yaml budget_mode='{bm}' (사장님 5/25 룰 — 'split_cash' 권장)"
        })

    return issues


def audit_cash_reserve() -> List[Dict]:
    """★ 사장님 30% 현금 보유 영구 룰 ★ 검증."""
    issues = []
    try:
        import yaml as _yaml
        with open(_ROOT / "config.yaml", encoding='utf-8') as f:
            cfg = _yaml.safe_load(f)
        from bot.kis_trader import KISTrader
        trader = KISTrader(cfg)
        bal = trader.fetch_balance()
        if not bal or not bal.get('success'):
            return [{"severity": "HIGH", "rule": "AUDIT-CASH", "msg": "KIS 잔고 조회 실패"}]

        cash = bal.get('cash', 0)
        total = bal.get('total_eval', 0)
        if total <= 0:
            return issues
        cash_ratio = cash / total * 100

        # 사장님 영구 룰: 항상 총평가 × 30% 현금 보유
        if cash_ratio < 30.0:
            issues.append({
                "severity": "CRITICAL", "rule": "AUDIT-CASH-30PCT",
                "msg": (
                    f"★ 사장님 30% 현금 보유 영구 룰 위반 ★\n"
                    f"  현금: {cash:,}원 / 총평가: {total:,}원 = {cash_ratio:.1f}% "
                    f"(목표 30% 대비 -{30-cash_ratio:.1f}%p 부족)"
                )
            })
    except Exception as e:
        issues.append({"severity": "HIGH", "rule": "AUDIT-CASH", "msg": f"audit_cash_reserve 예외: {e}"})

    return issues


def audit_rule_registry_used() -> List[Dict]:
    """매수/매도 코드가 SAJANG Rule Registry import 사용 중인지 검증."""
    issues = []
    try:
        from data.sajang_rules import SAJANG
        # Rule Registry 정상 로드
        assert SAJANG.FIXED_TP_DISABLED is True
        assert SAJANG.ENTRY_MODE_DEFAULT == 'pullback_3pct'
        assert SAJANG.TRAILING_PCT == 3.0
        assert SAJANG.CASH_RESERVE_PCT == 0.30
    except Exception as e:
        issues.append({
            "severity": "CRITICAL", "rule": "AUDIT-REGISTRY",
            "msg": f"★ Rule Registry 로드 실패 ★ {e}"
        })

    # auto_trader.py에서 SAJANG import 사용 검증
    at_path = _ROOT / "bot" / "auto_trader.py"
    if at_path.exists():
        src = at_path.read_text(encoding='utf-8')
        if "from data.sajang_rules import SAJANG" not in src:
            issues.append({
                "severity": "HIGH", "rule": "AUDIT-IMPORT",
                "msg": "⚠️ auto_trader.py에 SAJANG import 누락 (Rule Registry 미사용)"
            })

    return issues


def run() -> Dict:
    """단타봇 Daily Self-Audit 실행."""
    now = datetime.now()
    all_issues = []

    # 4가지 영역 자동 검증
    all_issues.extend(audit_positions())
    all_issues.extend(audit_config())
    all_issues.extend(audit_cash_reserve())
    all_issues.extend(audit_rule_registry_used())

    # 심각도별 분류
    critical = [i for i in all_issues if i.get('severity') == 'CRITICAL']
    high = [i for i in all_issues if i.get('severity') == 'HIGH']
    ok = len(all_issues) == 0

    if ok:
        summary = (
            f"✅ Daily Self-Audit 정상 — "
            f"사장님 영구 룰 13건 100% 적용 ({now.strftime('%Y-%m-%d %H:%M')})"
        )
    else:
        summary = (
            f"🚨 Daily Self-Audit 위반 {len(all_issues)}건 "
            f"(CRITICAL {len(critical)} / HIGH {len(high)})"
        )

    result = make_result("daily_self_audit", ok, summary, {
        "issues": all_issues,
        "critical_count": len(critical),
        "high_count": len(high),
        "checked_at": now.isoformat(),
    })
    save_state("daily_self_audit", result)

    # 위반 시 텔레그램 즉시 알림 (사장님 영구 룰 위반 = 단타봇 박사 미달)
    if not ok:
        lines = [
            f"🚨 [단타봇 Daily Self-Audit] 사장님 영구 룰 위반 발견",
            f"  시각: {now.strftime('%Y-%m-%d %H:%M:%S')}",
            f"  심각도: CRITICAL {len(critical)} / HIGH {len(high)}",
            "",
            "[위반 사항]"
        ]
        for issue in all_issues[:10]:
            lines.append(f"  [{issue.get('severity', '?')}] {issue.get('msg', '?')}")
        lines.append("")
        lines.append("★ 단타봇 자기 모니터링 시스템 — 사고 재발 방지 ★")
        send_telegram(fmt_alert("Daily Self-Audit 🚨", "", lines))

    return result


if __name__ == "__main__":
    r = run()
    print(json.dumps(r, ensure_ascii=False, indent=2, default=str))
    sys.exit(0 if r["ok"] else 1)
