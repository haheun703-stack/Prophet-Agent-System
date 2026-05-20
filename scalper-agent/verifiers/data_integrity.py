# -*- coding: utf-8 -*-
"""DataIntegrity — 정보봇 / 큰형 / JGIS / Brain 데이터 도착 감시.

오늘 5/19 사고: 큰형 5/19 advisory 미도착, brain_state.json 부재 → 매크로 신호 무시
시스템 자체가 잡았어야 했다.

점검 대상 (data_store/ 파일 + Supabase 테이블):
1. 정보봇: recommendation.json, jgis_morning_context.json, today_gainers.json,
   morning_briefings (Supabase), intelligence_daytrading_picks (Supabase)
2. 큰형: quant_bot_advisory (Supabase) — MORNING_BRIEFING / LEADING / SNAPSHOT / ADVICE
3. Brain: brain_state.json
4. NIGHTWATCH 백필: overnight.json

각 항목별 시각 윈도우 검사 → 윈도우 내 데이터 없으면 알림.
"""
import json
import sys
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Dict, List, Tuple, Optional

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))

from verifiers._common import send_telegram, save_state, make_result, fmt_alert

# [5/20 fix #3+#4] timezone 명시 KST + 가동 시각 윈도우 (정보봇 막내 지적)
try:
    from zoneinfo import ZoneInfo
    KST = ZoneInfo("Asia/Seoul")
except ImportError:
    KST = None

DATA_STORE = _ROOT / "data_store"

# (파일명, 최대 허용 age 시간, 설명)
# age = 현재 - 파일 mtime 이 이 값 초과하면 stale
FILE_CHECKS = [
    ("recommendation.json", 24, "정보봇 모닝 추천 (06:34)"),
    ("jgis_morning_context.json", 24, "정보봇 JGIS 수급 (06:40)"),
    ("morning_state.json", 24, "G1 MORNING_PREP 결과"),
    ("brain_state.json", 24, "BRAIN 자산 배분 (큰형 advisory 반영)"),
]

# Supabase 테이블 (오늘 데이터 존재 확인)
# [5/20 fix #4] expected_time 추가 — 가동 시각 + 1h 이후만 0건 알림
SUPABASE_CHECKS: List[Tuple[str, str, str, Optional[time]]] = [
    ("quant_bot_advisory", "advisory_date", "큰형 advisory", None),  # 수시
    ("morning_briefings", "date", "정보봇 모닝 브리핑", time(6, 30)),
    ("intelligence_daytrading_picks", "date", "정보봇 단타 추천", time(7, 0)),
    ("intelligence_supply_demand", "date", "정보봇 수급", time(16, 20)),  # 16:20 가동
]


def _now_kst() -> datetime:
    """[5/20 fix #3] KST 명시 — timezone aware datetime."""
    return datetime.now(KST) if KST else datetime.now()


def _check_files() -> Tuple[List[str], List[str]]:
    issues = []
    infos = []
    now = _now_kst()
    for fname, max_age_hours, desc in FILE_CHECKS:
        path = DATA_STORE / fname
        if not path.exists():
            issues.append(f"❌ {desc}: {fname} 파일 없음")
            continue
        # [5/20 fix #3] mtime도 KST aware로 변환 (75.3h/1227h 오류 해결)
        mtime = datetime.fromtimestamp(path.stat().st_mtime, tz=KST) if KST else \
                datetime.fromtimestamp(path.stat().st_mtime)
        age = (now - mtime).total_seconds() / 3600
        # 음수 age (미래) 방어
        if age < -0.1:
            issues.append(f"⚠️ {desc}: {fname} mtime이 미래 ({age:.1f}h) — VPS 시계 점검")
            continue
        if age > max_age_hours:
            issues.append(f"⚠️ {desc}: {fname} stale ({age:.1f}h > {max_age_hours}h)")
        else:
            infos.append(f"{desc}: {fname} ({age:.1f}h 전 갱신)")
    return issues, infos


def _check_supabase() -> Tuple[List[str], List[str]]:
    """[5/20 fix #4] 가동 시각 윈도우 적용 — 16:20 잡은 17:20 이후만 0건 알림"""
    issues = []
    infos = []
    try:
        from utils.supabase_sql import query_one
    except Exception as e:
        issues.append(f"⚠️ Supabase 모듈 로드 실패: {e}")
        return issues, infos

    today = date.today()
    now_t = _now_kst().time()
    for table, date_col, desc, expected_time in SUPABASE_CHECKS:
        # [5/20 fix #4] 가동 시각 + 1h grace period 적용
        if expected_time:
            grace_dt = datetime.combine(today, expected_time) + timedelta(hours=1)
            grace_time = grace_dt.time()
            if now_t < grace_time:
                infos.append(
                    f"{desc}: 가동 시각 전 (현재 {now_t.strftime('%H:%M')}, "
                    f"가동 {expected_time.strftime('%H:%M')} + 1h grace)"
                )
                continue
        try:
            sql = f"SELECT COUNT(*) AS n FROM {table} WHERE {date_col} = CURRENT_DATE"
            row = query_one(sql)
            n = row["n"] if row else 0
            if n == 0:
                issues.append(f"⚠️ {desc}: 오늘({today}) 데이터 0건 ({table})")
            else:
                infos.append(f"{desc}: {n}건 ({table})")
        except Exception as e:
            issues.append(f"⚠️ {desc}: 조회 실패 ({table}): {e}")
    return issues, infos


def _check_nightwatch_backfill() -> Tuple[List[str], List[str]]:
    """A0 백필 결과 — morning_state.json에서 remaining_fail 확인."""
    issues = []
    infos = []
    path = DATA_STORE / "morning_state.json"
    if not path.exists():
        return issues, infos
    try:
        st = json.loads(path.read_text(encoding="utf-8"))
        # morning_state 안에 어떤 형식으로 들어있는지는 G1 코드에 따라 다름
        # 키 후보: backfill_remaining / a0_remaining_fail / etc
        for key in ("backfill_remaining", "a0_remaining_fail", "remaining_fail"):
            v = st.get(key)
            if v:
                issues.append(f"⚠️ A0 백필 실패 ({key}): {v}")
                break
        else:
            infos.append("A0 백필 정상 (morning_state.json 내 실패 키 없음)")
    except Exception as e:
        issues.append(f"⚠️ morning_state.json 파싱 실패: {e}")
    return issues, infos


def run() -> Dict:
    f_issues, f_infos = _check_files()
    s_issues, s_infos = _check_supabase()
    n_issues, n_infos = _check_nightwatch_backfill()

    all_issues = f_issues + s_issues + n_issues
    ok = len(all_issues) == 0

    if ok:
        summary = f"✅ DataIntegrity 정상 (파일 {len(f_infos)} + Supabase {len(s_infos)} + 백필 {len(n_infos)})"
    else:
        summary = f"🚨 DataIntegrity 이상 {len(all_issues)}건"

    result = make_result("data_integrity", ok, summary, {
        "file_issues": f_issues,
        "file_infos": f_infos,
        "supabase_issues": s_issues,
        "supabase_infos": s_infos,
        "nightwatch_issues": n_issues,
        "nightwatch_infos": n_infos,
    })
    save_state("data_integrity", result)

    if not ok:
        lines = ["", "[발견된 이상]"]
        lines.extend(all_issues)
        send_telegram(fmt_alert("DataIntegrity 🚨", "", lines))

    return result


if __name__ == "__main__":
    r = run()
    print(json.dumps(r, ensure_ascii=False, indent=2, default=str))
    sys.exit(0 if r["ok"] else 1)
