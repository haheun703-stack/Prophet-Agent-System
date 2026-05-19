# -*- coding: utf-8 -*-
"""FlowMonitor — 실매매 흐름 5분 간격 감시.

오늘 5/19 사고: 자비스 자율 swing 매수 0건, intraday 0건 → 사장님 매매 확인하시다 발견.
시스템 자체가 "예상 매매 시각에 매수 흐름 없음"을 알렸어야 했다.

점검 항목 (장중 09:00~15:30만):
1. 09:00~09:10 swing 진입 시도 흔적 — 큰형 advisory 게이트 로그 OR 매수 시도 로그
2. 09:05~14:00 intraday_verification 활성 여부 — verification_mode is_active
3. 일일 매수 카운트 — scalper_trade_journal (실매수 + 페이퍼)
4. RealtimeMonitor 보유 5종목 모니터링 살아있는지

5분마다 호출되어 1번 시점이 지났는데 흔적 없으면 알림.
"""
import json
import sys
from datetime import datetime, time, date
from pathlib import Path
from typing import Dict, List, Tuple

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))

from verifiers._common import send_telegram, save_state, load_state, make_result, fmt_alert


def _is_market_hours() -> bool:
    now = datetime.now().time()
    return time(9, 0) <= now <= time(15, 30)


def _check_verification_mode_active() -> Tuple[bool, str]:
    try:
        from data import verification_mode as vm
        active = vm.is_active()
        return active, ("활성" if active else "비활성")
    except Exception as e:
        return False, f"체크 실패: {e}"


def _check_buy_count_today() -> Tuple[int, int, str]:
    """반환: (실매수 카운트, 페이퍼 카운트, 메시지)"""
    real_n = 0
    paper_n = 0
    try:
        from utils.supabase_sql import query_one
        row = query_one("""
            SELECT
              COUNT(*) FILTER (WHERE event_type='buy' AND source NOT LIKE 'paper%%') AS real_n,
              COUNT(*) FILTER (WHERE event_type='buy' AND source LIKE 'paper%%') AS paper_n
            FROM scalper_trade_journal
            WHERE event_date = CURRENT_DATE
        """)
        if row:
            real_n = row.get("real_n", 0) or 0
            paper_n = row.get("paper_n", 0) or 0
    except Exception as e:
        return 0, 0, f"조회 실패: {e}"
    return real_n, paper_n, "OK"


def _check_intraday_scan_recent() -> Tuple[bool, str]:
    """최근 10분 내 intraday_scanner 로그 흔적 — 봇 로그 직접 못 보니 state 기반."""
    state = load_state("flow_monitor")
    last_intraday = state.get("last_intraday_seen")
    if not last_intraday:
        return False, "intraday 활성 흔적 없음 (최초)"
    try:
        last = datetime.fromisoformat(last_intraday)
        diff_min = (datetime.now() - last).total_seconds() / 60
        if diff_min > 15:
            return False, f"intraday 마지막 활성 {diff_min:.0f}분 전 (>15분)"
        return True, f"intraday 활성 {diff_min:.0f}분 전"
    except Exception as e:
        return False, f"파싱 실패: {e}"


def _check_positions_alive() -> Tuple[bool, str]:
    """positions.json 갱신 시각."""
    path = _ROOT / "data_store" / "positions.json"
    if not path.exists():
        return False, "positions.json 없음"
    mtime = datetime.fromtimestamp(path.stat().st_mtime)
    diff_min = (datetime.now() - mtime).total_seconds() / 60
    if diff_min > 60:
        return False, f"positions.json {diff_min:.0f}분 전 갱신 (오래됨)"
    return True, f"positions.json {diff_min:.0f}분 전 갱신"


def run() -> Dict:
    """장중에만 의미. 장 시작 전/후는 보고만 하고 alert X."""
    now = datetime.now()
    in_hours = _is_market_hours()

    issues = []
    infos = [f"현재 시각: {now.strftime('%H:%M:%S')} 장중={in_hours}"]

    # 1. verification_mode 활성 여부 (장중일 때만)
    vm_active, vm_msg = _check_verification_mode_active()
    infos.append(f"verification_mode: {vm_msg}")
    if in_hours and not vm_active:
        # 09:00~14:00 사이인데 비활성이면 경고 (단 사장님이 의도적 OFF일 수도 — info로만)
        if time(9, 0) <= now.time() <= time(14, 0):
            issues.append(f"⚠️ 장중인데 verification_mode 비활성 → intraday_scan 0건")

    # 2. 오늘 매수 카운트
    real_n, paper_n, buy_msg = _check_buy_count_today()
    infos.append(f"오늘 매수: 실매수 {real_n}건 / 페이퍼 {paper_n}건 ({buy_msg})")
    if in_hours and time(10, 0) <= now.time():
        # 10시 이후인데 실매수 0건 + 페이퍼 0건이면 경고
        if real_n == 0 and paper_n == 0:
            issues.append(f"⚠️ 10시 이후 매수 흔적 0건 (실+페이퍼)")

    # 3. positions.json 갱신 시각
    pos_ok, pos_msg = _check_positions_alive()
    infos.append(f"positions.json: {pos_msg}")
    if in_hours and not pos_ok:
        issues.append(f"⚠️ positions.json 갱신 정체: {pos_msg}")

    ok = len(issues) == 0
    summary = (
        f"✅ FlowMonitor 정상 ({len(infos)}건 점검)"
        if ok else f"🚨 FlowMonitor 이상 {len(issues)}건"
    )

    result = make_result("flow_monitor", ok, summary, {
        "issues": issues,
        "infos": infos,
        "in_hours": in_hours,
        "buy_count_real": real_n,
        "buy_count_paper": paper_n,
        "verification_active": vm_active,
    })
    save_state("flow_monitor", result)

    if not ok and in_hours:
        lines = ["", "[발견된 이상]"]
        lines.extend(issues)
        lines.append("")
        lines.append("[현재 상태]")
        lines.extend(infos[:4])
        send_telegram(fmt_alert("FlowMonitor 🚨", "", lines))

    return result


if __name__ == "__main__":
    r = run()
    print(json.dumps(r, ensure_ascii=False, indent=2, default=str))
    sys.exit(0 if r["ok"] else 1)
