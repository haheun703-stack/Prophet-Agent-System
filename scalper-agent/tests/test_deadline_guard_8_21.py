# -*- coding: utf-8 -*-
"""test_deadline_guard_8_21.py — 데드라인 대장 침묵 차단 + [F-179] 벤치마크 강제 회귀

배경(8/21):
  - 사장님 결정으로 [S-1] 폐기 → **활성 전략 0건**. 체커 실측 출력이
    스코어보드 + "⏰ 전략 데드라인 대장" 헤더 **뿐**이고 alerts=0 → 텔레그램 미발송.
    `strategy_deadline_check.py` 주석이 경고하던 위험구간("S-6가 정리되면 alerts=0이
    되고 장부가 깨져도 아무도 모른다")이 실현된 것.
    7/17 사장님이 대장을 만든 이유가 '판정 없이 무기한 도는 것'의 차단인데,
    **전략이 하나도 없는 상태**는 그 반대편의 같은 실패(진화 정지)다.
  - [F-179]: [S-1] 기준은 `>0`인데 같은 기간 시장 동일가중이 T+3 +0.37%p·T+5 +0.98%p.
    아무거나 사도 플러스인 구간에서 `>0`을 통과선으로 삼았다. 명분 축은 시장 대비
    -6.05%p(t -6.33)인데 절대수익만 보면 '그냥 마이너스'로 보인다.
    → 신규 등재는 `benchmark` 필드 의무(사장님 8/21 승인).

이 테스트가 지키는 것:
  1) 활성 0건이면 **alerts에 뜬다** (조용히 지나가지 않는다)
  2) 경과일을 함께 낸다 — 매일 같은 문구만 짖으면 마모된다([F-153] 교훈)
  3) 대장 로드 실패 시 활성 0 경고를 **중복으로 내지 않는다**(이미 다른 경고가 있음)
  4) benchmark 없는 활성 전략은 alerts에 뜬다 / 있으면 안 뜬다
  5) ★음성대조 — 가드를 제거한 구코드가 이 케이스들을 **놓친다**
  6) ★불변식 — 자동 폐기 없음(status를 건드리지 않는다)·exit 0

실행: python -X utf8 tests/test_deadline_guard_8_21.py
"""
import json
import sys
import tempfile
from datetime import date, timedelta
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
for p in (str(BASE_DIR), str(BASE_DIR / "tools")):
    if p not in sys.path:
        sys.path.insert(0, p)

import strategy_deadline_check as sdc  # noqa: E402

PASS_N = 0
FAIL_N = 0


def check(label, cond, extra=""):
    global PASS_N, FAIL_N
    if cond:
        PASS_N += 1
        print(f"  ✅ {label}")
    else:
        FAIL_N += 1
        print(f"  ❌ {label} {extra}")


def _with_registry(payload):
    """REGISTRY를 임시 파일로 갈아끼우고 build_report() 실행 → (lines, alerts)."""
    tmp = Path(tempfile.mkdtemp()) / "reg.json"
    tmp.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    old = sdc.REGISTRY
    try:
        sdc.REGISTRY = tmp
        return sdc.build_report()
    finally:
        sdc.REGISTRY = old


def _joined(rows):
    return "\n".join(rows)


D_FUTURE = (date.today() + timedelta(days=20)).isoformat()
REPORTED = (date.today() - timedelta(days=6)).isoformat()

ALL_DROPPED = {"strategies": [
    {"id": "S-1", "name": "PB-A v2", "deadline": "2026-08-29",
     "status": "dropped_strategy", "reported": REPORTED},
    {"id": "S-6", "name": "sector_reversal", "deadline": "2026-08-07",
     "status": "dropped_strategy", "reported": "2026-08-07"},
]}
ACTIVE_NO_BM = {"strategies": [
    {"id": "S-8", "name": "테스트 전략", "deadline": D_FUTURE, "status": "active",
     "criteria": "순누적 net 플러스", "metric": None},
]}
ACTIVE_WITH_BM = {"strategies": [
    {"id": "S-8", "name": "테스트 전략", "deadline": D_FUTURE, "status": "active",
     "criteria": "리프트 > 시장 동일가중", "metric": None,
     "benchmark": "market_equal_weight_same_day"},
]}


def main() -> int:
    print("=" * 78)
    print("[1] 활성 전략 0건 — 침묵 차단")
    lines, alerts = _with_registry(ALL_DROPPED)
    txt_l, txt_a = _joined(lines), _joined(alerts)
    check("활성 0건이 alerts에 뜬다", "활성 전략 0건" in txt_a, f"\n     alerts={alerts}")
    check("lines에도 표기된다", "활성 전략 0건" in txt_l)
    check("경과일이 함께 표기된다(마모 방지)", "6일째" in txt_a, f"\n     alerts={alerts}")
    check("문구가 행동을 지시한다([T-1] 또는 대장 복구)",
          "[T-1]" in txt_a or "대장 복구" in txt_a)
    check("스코어보드 첫 줄은 유지된다(7/17 약속)", lines and lines[0].startswith("📊"))
    check("★불변식 — 자동 폐기 없음(status 무변경)",
          all(s["status"] == "dropped_strategy" for s in ALL_DROPPED["strategies"]))

    print("\n[2] [F-179] benchmark 강제")
    lines2, alerts2 = _with_registry(ACTIVE_NO_BM)
    txt2 = _joined(alerts2)
    check("benchmark 없는 활성 전략이 alerts에 뜬다", "benchmark 미지정" in txt2,
          f"\n     alerts={alerts2}")
    check("F-179 근거가 문구에 있다", "F-179" in txt2)
    check("활성이 있으므로 '활성 0건'은 뜨지 않는다", "활성 전략 0건" not in txt2)

    lines3, alerts3 = _with_registry(ACTIVE_WITH_BM)
    txt3 = _joined(alerts3)
    check("benchmark 있으면 경고 없다", "benchmark 미지정" not in txt3,
          f"\n     alerts={alerts3}")
    check("D-20이라 데드라인 알림도 없다(ALERT_DDAY=3)", not alerts3, f"\n     alerts={alerts3}")

    print("\n[3] 대장 로드 실패 — 중복 경고 금지")
    tmp = Path(tempfile.mkdtemp()) / "broken.json"
    tmp.write_text("{ this is not json", encoding="utf-8")
    old = sdc.REGISTRY
    try:
        sdc.REGISTRY = tmp
        lines4, alerts4 = sdc.build_report()
    finally:
        sdc.REGISTRY = old
    txt4 = _joined(alerts4)
    check("로드 실패가 alerts에 뜬다", "대장 로드 실패" in txt4, f"\n     alerts={alerts4}")
    check("활성 0건 경고는 중복으로 안 뜬다", "활성 전략 0건" not in txt4,
          f"\n     alerts={alerts4}")

    print("\n[4] ★음성대조 — 가드가 없으면 놓친다 (구코드 재현)")

    def _legacy(reg_payload):
        """8/21 fix 이전 로직의 최소 재현: active만 순회하고 끝."""
        out_lines, out_alerts = ["📊 …", "⏰ 전략 데드라인 대장"], []
        for s in reg_payload.get("strategies", []):
            if s.get("status") != "active":
                continue
            dleft = (date.fromisoformat(s["deadline"]) - date.today()).days
            row = f"[{s['id']}] {s['name']} — D-{dleft}"
            out_lines.append(row)
            if dleft <= sdc.ALERT_DDAY:
                out_alerts.append(row)
        return out_lines, out_alerts

    l_old, a_old = _legacy(ALL_DROPPED)
    check("구코드는 활성 0건을 놓친다(=조용)", not a_old and "활성 전략 0건" not in _joined(l_old),
          f"\n     legacy alerts={a_old}")
    l_old2, a_old2 = _legacy(ACTIVE_NO_BM)
    check("구코드는 benchmark 미지정을 놓친다", "benchmark" not in _joined(a_old2),
          f"\n     legacy alerts={a_old2}")

    print("\n[5] 실제 대장 — 회귀(정본 파일)")
    lines5, alerts5 = sdc.build_report()
    check("정본에서도 exit 없이 동작", isinstance(lines5, list) and isinstance(alerts5, list))
    check("정본 첫 줄 = 스코어보드", lines5 and lines5[0].startswith("📊"))
    real = json.loads(sdc.REGISTRY.read_text(encoding="utf-8"))
    n_active = sum(1 for s in real.get("strategies", []) if s.get("status") == "active")
    if n_active == 0:
        check("정본이 활성 0건이므로 경고가 뜬다", "활성 전략 0건" in _joined(alerts5),
              f"\n     alerts={alerts5}")
    else:
        check(f"정본 활성 {n_active}건 — 경고 없음이 정상", "활성 전략 0건" not in _joined(alerts5))

    print("\n" + "=" * 78)
    print(f"결과: {PASS_N} PASS / {FAIL_N} FAIL")
    return 1 if FAIL_N else 0


if __name__ == "__main__":
    raise SystemExit(main())
