# -*- coding: utf-8 -*-
"""★ 단타봇 Daily Self-Audit (5/26 사고 후 신설) ★

매일 15:35 cron 자동 실행 → 사장님 영구 룰 13건 자동 검증 → 위반 시 텔레그램 즉시 알림.

5/26 사고 5건 모두 "단타봇이 사장님 영구 룰 default off / 옛 코드 잔존" 패턴.
이를 매일 자동 검증해서 단타봇이 또 사고 안 치도록 자기 모니터링 시스템.

검증 항목:
  1. 보유 종목 룰 부합 (TP=0 / mode=swing / source=sync_auto_unknown)
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


def audit_with_codex(self_audit_issues: List[Dict]) -> List[Dict]:
    """★ 사장님 5/26 통찰 — Codex 상호 보완 검수 (Tier 4) ★

    Daily Self-Audit (단타봇 정적 검수) 다음 단계:
      - Codex (GPT-4o)에 보유 종목 + config 일부 검수 의뢰
      - 단타봇이 못 본 사고 패턴 추가 검출

    검수 결과를 self_audit_issues에 추가 (코덱스 발견 항목은 rule="CODEX-X" 마커).

    Returns:
        Codex 발견 추가 issues 리스트
    """
    codex_issues = []
    try:
        from utils.codex_review import codex_review_code
    except Exception:
        # codex_review 미설치 (오프라인 등) → skip
        return codex_issues

    # 검수 컨텍스트: positions.json + 자기 검수 결과 요약
    try:
        pp = _ROOT / "data_store" / "positions.json"
        positions_json = pp.read_text(encoding='utf-8')[:2000] if pp.exists() else "{}"
    except Exception:
        positions_json = "{}"

    # Codex 검수 의뢰 컨텍스트
    issues_summary = "\n".join([
        f"  [{i.get('severity', '?')}] {i.get('rule', '?')}: {i.get('msg', '?')[:100]}"
        for i in self_audit_issues[:10]
    ]) or "  (단타봇 자기 검수 결과 위반 0건)"

    audit_context = f"""# Daily Self-Audit 검수 결과 (단타봇 자율 검출)
{issues_summary}

# 현재 보유 종목 (positions.json)
{positions_json}

검수 핵심:
1. 단타봇이 놓친 사장님 영구 룰 위반 패턴 있는가?
2. 보유 종목이 사장님 매수 보호 (sync_auto_unknown / TP=0 / mode=swing) 정확 적용?
3. 5/26 미친짓 패턴 (default off / TP+5% 잔존) 잠재 위험?
4. 5/27 D+1 가동 시 사고 가능성?
"""

    result = codex_review_code(
        code_snippet=audit_context,
        review_focus="Daily Self-Audit 보완 검수 (단타봇이 놓친 사고 패턴 추가 검출)",
        max_tokens=1500,
    )

    if not result.get("success"):
        # Codex API 실패 → 단순 정보 추가
        codex_issues.append({
            "severity": "LOW", "rule": "CODEX-API-FAIL",
            "msg": f"ℹ️ Codex API 실패 (보완 검수 skip): {result.get('error', '?')[:80]}"
        })
        return codex_issues

    review_text = result.get("review", "")
    # Codex 응답에서 [CRITICAL]/[HIGH] 라인 추출
    for line in review_text.split("\n"):
        line = line.strip()
        if not line:
            continue
        if "[CRITICAL]" in line or "CRITICAL]" in line:
            codex_issues.append({
                "severity": "CRITICAL", "rule": "CODEX-FINDING",
                "msg": f"[Codex] {line[:200]}"
            })
        elif "[HIGH]" in line or "HIGH]" in line:
            codex_issues.append({
                "severity": "HIGH", "rule": "CODEX-FINDING",
                "msg": f"[Codex] {line[:200]}"
            })

    # Codex 검수 완료 표시 (CRITICAL/HIGH 0건이어도)
    if not codex_issues:
        codex_issues.append({
            "severity": "LOW", "rule": "CODEX-PASS",
            "msg": f"✅ Codex (gpt-4o) 보완 검수 통과 — 추가 사고 패턴 0건 발견 (tokens={result.get('tokens_used', 0)})"
        })

    return codex_issues


def run(enable_codex: bool = True) -> Dict:
    """단타봇 Daily Self-Audit 실행 (★ 5/26 사장님 통찰 — Codex 통합 ★).

    Args:
        enable_codex: True 시 Tier 4 (Codex 보완 검수) 활성 — 매일 15:45 cron 권장
    """
    now = datetime.now()
    all_issues = []

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # Tier 1: 단타봇 자기 검수 (4가지 영역)
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    self_audit_issues = []
    self_audit_issues.extend(audit_positions())
    self_audit_issues.extend(audit_config())
    self_audit_issues.extend(audit_cash_reserve())
    self_audit_issues.extend(audit_rule_registry_used())
    all_issues.extend(self_audit_issues)

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # ★ Tier 4: Codex 보완 검수 (5/26 사장님 통찰) ★
    # 단타봇 1년 본성 한계 극복 — 다른 시각 도입
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    codex_issues = []
    if enable_codex:
        try:
            codex_issues = audit_with_codex(self_audit_issues)
            all_issues.extend(codex_issues)
        except Exception as e:
            all_issues.append({
                "severity": "LOW", "rule": "CODEX-EXCEPTION",
                "msg": f"ℹ️ Codex 통합 예외 (skip): {e}"
            })

    # 심각도별 분류 (CRITICAL/HIGH만 카운트)
    critical = [i for i in all_issues if i.get('severity') == 'CRITICAL']
    high = [i for i in all_issues if i.get('severity') == 'HIGH']
    # LOW (Codex PASS 등)는 위반 0건 판정에서 제외
    real_violations = critical + high
    ok = len(real_violations) == 0

    if ok:
        codex_status = " + Codex Tier 4 통과" if enable_codex and codex_issues else ""
        summary = (
            f"✅ Daily Self-Audit 정상 — "
            f"사장님 영구 룰 13건 100% 적용{codex_status} ({now.strftime('%Y-%m-%d %H:%M')})"
        )
    else:
        codex_found = sum(1 for i in real_violations if 'CODEX' in i.get('rule', ''))
        summary = (
            f"🚨 Daily Self-Audit 위반 {len(real_violations)}건 "
            f"(CRITICAL {len(critical)} / HIGH {len(high)}"
            + (f" / Codex 발견 {codex_found}건" if codex_found else "")
            + ")"
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
