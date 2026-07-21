# -*- coding: utf-8 -*-
"""nightly ⑳ — 산출물 신선도 자가검증 + 데이터 재검증 (7/7 전체검수 M-2/D4 fix).

배경(7/7 전체검수):
  ① record-only 러너들의 except→exit 0이 런타임 예외를 nightly ✅로 위장 가능
     (7/2 fix5 "try/except 완료위장" 사고의 현행 잔존형) → returncode가 아니라
     **산출 JSON 자체의 오늘 날짜**를 마지막에 검증하는 스텝이 필요.
  ② data_verify_result.json이 16:58(봇 D 파이프라인·fill 전) 스냅샷으로 박제되는
     "장전 박제 착시"(7/1 교훈) → nightly 마지막에 재실행해 fill 후 진실로 덮음.

동작:
  1) DataVerifier 재실행 (data_verify_result.json = fill 후 진실 갱신)
  2) 오늘 nightly가 만들었어야 할 핵심 산출물의 날짜 검증 — stale이면 ⚠ 출력 + exit 1
     (nightly 요약에 ⚠로 노출 = 완료위장 방지가 목적이므로 예외도 exit 1)

★ 안전: read-only 검증 + data_verify_result.json 갱신만. 매수/매도/picks/SAJANG/주문 0 접촉.
실행: python tools/run_nightly_freshness_check.py
"""
import json
import sys
from datetime import date
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent
if str(BASE) not in sys.path:
    sys.path.insert(0, str(BASE))

try:
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")
except Exception:
    pass

STORE = BASE / "data_store"


def _json_field(path: Path, *keys) -> str:
    """JSON에서 첫 번째로 존재하는 키 값의 앞 10글자(날짜부). 실패 시 ''."""
    try:
        d = json.loads(path.read_text(encoding="utf-8"))
        for k in keys:
            v = d
            for part in k.split("."):
                v = v.get(part) if isinstance(v, dict) else None
                if v is None:
                    break
            if v:
                return str(v)[:10]
    except Exception:
        pass
    return ""


def run() -> int:
    today = date.today().isoformat()
    try:
        from data.trading_calendar import is_trading_day
        if not is_trading_day(date.today()):
            print("[freshness] 휴장일 — skip")
            return 0
    except Exception:
        pass

    # ── 1) 데이터 재검증 (fill 후 진실로 data_verify_result.json 갱신) ──
    try:
        from data.data_verifier import DataVerifier, format_verify_oneliner
        res = DataVerifier().verify_all()
        print(f"[freshness] 데이터 재검증(장전 박제 해소): {format_verify_oneliner(res)}")
    except Exception as e:  # 재검증 실패도 가시화 (exit 1 경로)
        print(f"[freshness] ⚠ 데이터 재검증 실패: {e}")

    # ── 2) nightly 핵심 산출물 신선도 (결정적 매일 산출물만 hard check) ──
    checks = [
        ("④ ledger", (STORE / "paper_3type" / f"ledger_{today}.json").exists() and today or
         _latest_ledger_date()),
        ("⑰ market_regime", _json_field(STORE / "market_regime.json", "latest.based_on")),
        ("⑱ morning_briefing", _json_field(STORE / "morning_briefing.json",
                                           "generated_at", "based_on", "timestamp")),
        ("⑲ playbook_shadow", _json_field(STORE / "playbook_shadow.json", "timestamp")),
        ("⑯ cluster_harvest", _json_field(STORE / "cluster_harvest_paper.json", "timestamp")),
        ("⑪ catalyst_archive", today if (STORE / "catalyst_archive" / f"catalyst_{today}.json").exists() else ""),
    ]
    stale = []
    for name, got in checks:
        ok = (got == today)
        mark = "OK" if ok else "⚠ STALE"
        print(f"[freshness] {name}: {got or '(없음)'} {mark}")
        if not ok:
            stale.append(name)

    # 조건부 hard check (7/10 검수 M-1 fix): 장중 OBSERVE intent가 오늘자로 존재하면
    # ⑲-2 대조도 오늘자여야 함 — --compare 예외 삼킴(exit 0)의 완료위장을 여기서 적발.
    # intent 없는 날(가동 전·휴장·cron 미동작)은 판정 제외(오탐 방지).
    intents_date = _json_field(STORE / "observe_v2_intents.json", "date")
    if intents_date and intents_date == today.replace("-", ""):   # intent 포맷=YYYYMMDD
        cmp_date = _json_field(STORE / "observe_v2_compare.json", "date")
        ok = (cmp_date == intents_date)
        print(f"[freshness] ⑲-2 observe_compare: {cmp_date or '(없음)'} "
              f"{'OK' if ok else '⚠ STALE (intent 있는 날 대조 부재=⑲-2 예외 의심)'}")
        if not ok:
            stale.append("⑲-2 observe_compare")
        # ⑲-3 페이퍼 장부(7/16) — intent 있는 날은 당일 정산도 있어야 함 (동일 완료위장 방어)
        try:
            led = json.loads((STORE / "observe_v2_paper_ledger.json").read_text(encoding="utf-8"))
            ok3 = intents_date in (led.get("days") or {})
        except Exception:
            ok3 = False
        print(f"[freshness] ⑲-3 v2_paper_ledger: "
              f"{'OK' if ok3 else '⚠ STALE (intent 있는 날 정산 부재=⑲-3 예외 의심)'}")
        if not ok3:
            stale.append("⑲-3 v2_paper_ledger")

    # ticks 판정원천 건강도 (7/21 F-15) — 거래일이면 항상 확인. BROKEN/TRUNCATED = 그날
    # ⑲/⑲-3 판정 증거 오염 → nightly 요약에 ⚠ 노출로 사장님이 저녁에 인지(그간 사각).
    try:
        import sys as _sys
        _sys.path.insert(0, str(BASE / "tools"))
        import playbook_shadow as _pb  # type: ignore
        _day = today.replace("-", "")
        # win32(노트북)은 ticks 미동기 → false STALE 방지 위해 건너뜀(L-1). ⑳은 VPS 전용이라 무해.
        if _sys.platform != "win32" and (_pb.TICKS / _day).exists():
            _tv = (_pb.ticks_health(_day) or {}).get("verdict")
            if _tv is not None:
                print(f"[freshness] ⑲ ticks_health: {_tv}")
                if _tv in ("BROKEN", "TRUNCATED"):
                    stale.append(f"ticks_{_tv}")
    except Exception as e:  # noqa: BLE001
        print(f"[freshness] ⑲ ticks_health 확인 실패(무시): {e}")

    # soft check (매일 갱신이 보장 안 되는 것 — 정보만)
    for name, path, key in (("③-2 early(soft)", STORE / "early_variant_shadow.json", "updated_at"),
                            ("③-3 reentry(soft)", STORE / "reentry_shadow.json", "updated_at")):
        got = _json_field(path, key)
        print(f"[freshness] {name}: {got or '(없음)'} {'OK' if got == today else '(후보 없던 날은 정상)'}")

    if stale:
        print(f"[freshness] ⚠⚠ 오늘 산출물 STALE {len(stale)}건: {', '.join(stale)} — 해당 스텝 로그 확인 필요")
        return 1
    print("[freshness] ✅ 핵심 산출물 전부 오늘 날짜 — 완료위장 없음")
    return 0


def _latest_ledger_date() -> str:
    try:
        files = sorted((STORE / "paper_3type").glob("ledger_*.json"))
        return files[-1].stem.replace("ledger_", "") if files else ""
    except Exception:
        return ""


if __name__ == "__main__":
    try:
        sys.exit(run())
    except Exception as e:  # 이 스텝만은 예외도 가시화 (완료위장 방지가 존재 이유)
        print(f"[freshness] ⚠ 치명 예외: {e}")
        sys.exit(1)
