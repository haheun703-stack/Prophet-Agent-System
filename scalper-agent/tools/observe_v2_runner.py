# -*- coding: utf-8 -*-
"""OBSERVE 러너 — PB-A 정제판 v2 실시간 관측 (7/10 사장님 승인·압축 로드맵 1단계).

목적: v2 신호(오전 10시前 +5% 도달·체결강도 200+·추격 8% 미만 = 73일 백테스트
승51.6%·avg +0.37%/건)를 **장중 실시간으로 포착 → 주문 intent 기록 + 텔레그램**.
백테스트(⑲ replay)와 동일 원천(ticks)·동일 파서·동일 상수(playbook_shadow 단일진실)
를 쓰므로, 저녁 --compare가 "실시간 경로가 최종 데이터와 같은 신호를 봤는가"를
정직하게 판정한다(누락/유령/지연 = 라이브 전환 전 반드시 알아야 할 간극).

동작:
  --once    (cron */5 09~10시 KST·VPS): 오늘 ticks 전량 스캔(결정적 — 종목당 최초
            조건행) → 신규 신호 intent 추가(원자쓰기) + 신규분 텔레그램 1회 배치 알림
            + 기존 intent 진입행(신호 다음 관측행) enrich.
  --compare (nightly ⑲-2·18시): 최종 ticks 기준 진실 신호 셋 ↔ 장중 기록 intent 대조
            → observe_v2_compare.json (matched/missed/phantom/지연 통계).

★★★ 안전 불변식 ★★★
- OBSERVE 모드 = 실주문 0. 주문/계좌 API 0접촉 — intent는 JSON 기록일 뿐이다.
- 읽기: ticks·universe.json·trading_calendar / 쓰기: observe_v2_intents.json·
  observe_v2_compare.json (원자쓰기). 매수/매도/picks/SAJANG/order_intent 0접촉.
- 실주문 전환(소액 라이브) = 대조 통과 후 사장님 결정. 관측 없이 flip 금지.

실행: python tools/observe_v2_runner.py --once | --compare [--day YYYYMMDD]
"""
import argparse
import json
import sys
from datetime import date, datetime
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent
if str(BASE) not in sys.path:
    sys.path.insert(0, str(BASE))
if str(BASE / "tools") not in sys.path:
    sys.path.insert(0, str(BASE / "tools"))

try:
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")
except Exception:
    pass

import playbook_shadow as pb  # noqa: E402 — ticks 파서·v2 상수 단일진실

DS = BASE / "data_store"
INTENTS_PATH = DS / "observe_v2_intents.json"
COMPARE_PATH = DS / "observe_v2_compare.json"
HISTORY_PATH = DS / "observe_v2_history.json"   # 일별 대조+intent 누적(라이브 판정 증거)
HISTORY_KEEP = 90
ALERT_BATCH_MAX = 12   # 텔레그램 1회 알림에 담는 최대 신규 신호(스팸 방지·초과분은 건수만)


def _atomic_write(path: Path, data) -> None:
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=1), encoding="utf-8")
    tmp.replace(path)


def _names() -> dict:
    try:
        u = json.loads((DS / "universe.json").read_text(encoding="utf-8"))
        return {k: (v.get("name") or "") for k, v in u.items()}
    except Exception:  # noqa: BLE001
        return {}


def _scan_v2_signals(day: str) -> dict:
    """오늘 ticks 결정적 스캔 → {code: signal dict}. ⑲ replay와 동일 규칙:
    종목당 최초 (change_rate>=SIGNAL_PCT ∧ time<=SIGNAL_CUTOFF) 행이 신호행이고,
    v2 = 그 행이 (time<=REFINED_CUTOFF ∧ strength>=REFINED_STRENGTH ∧
    change_rate<CHASE_MAX)일 때만. 진입행(신호 다음 관측행)은 존재 시 enrich."""
    out = {}
    tdir = pb.TICKS / day
    if not tdir.exists():
        return out
    for f in sorted(tdir.glob("*.csv")):
        code = f.stem
        rows = pb._read_ticks(day, code)
        if len(rows) < 2:
            continue
        sig = next((i for i, (t, px, cr, st) in enumerate(rows)
                    if cr >= pb.SIGNAL_PCT and t <= pb.SIGNAL_CUTOFF), None)
        if sig is None:
            continue
        t, px, cr, st = rows[sig]
        if not (t <= pb.REFINED_CUTOFF and st >= pb.REFINED_STRENGTH
                and cr < pb.CHASE_MAX):
            continue
        d = {"code": code, "signal_time": t, "signal_price": px,
             "signal_chg": round(cr, 2), "strength": round(st, 1)}
        if sig + 1 < len(rows):
            d["entry_time"] = rows[sig + 1][0]
            d["entry_price"] = rows[sig + 1][1]
        out[code] = d
    return out


def _merge_intents(state: dict, found: dict, day: str, now_hms: str) -> tuple:
    """state에 신규 신호 병합(멱등) + 기존 intent 진입행 enrich. → (state, 신규 리스트).
    신규만 detected_at=now_hms 스탬프(지연 측정용) — 기존 레코드 무수정(additive)."""
    if state.get("date") != day:
        state = {"date": day, "intents": {}}
    intents = state["intents"]
    fresh = []
    for code, d in found.items():
        if code in intents:
            cur = intents[code]
            for k in ("entry_time", "entry_price"):   # enrich만 — 신호 필드 무수정
                if k in d and k not in cur:
                    cur[k] = d[k]
            continue
        rec = dict(d)
        rec["detected_at"] = now_hms
        intents[code] = rec
        fresh.append(rec)
    return state, fresh


def run_once(day: str = None, send=None, no_tg: bool = False) -> int:
    from data.trading_calendar import is_trading_day
    if not is_trading_day(date.today()) and day is None:
        print("[observe_v2] 휴장일 — skip")
        return 0
    day = day or date.today().strftime("%Y%m%d")
    if not (pb.TICKS / day).exists():
        print(f"[observe_v2] ticks/{day} 없음 (VPS 전용·수집 전) — skip")
        return 0
    now_hms = datetime.now().strftime("%H:%M:%S")
    try:
        state = json.loads(INTENTS_PATH.read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001
        state = {}
    state, fresh = _merge_intents(state, _scan_v2_signals(day), day, now_hms)
    state["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    _atomic_write(INTENTS_PATH, state)
    n_total = len(state.get("intents", {}))
    print(f"[observe_v2] {day} {now_hms} — 신규 {len(fresh)} / 누적 {n_total} "
          f"(OBSERVE·실주문 0)")
    if fresh and no_tg:
        print(f"[observe_v2] --no-tg — 알림 생략(신규 {len(fresh)}건 기록만)")
    elif fresh:
        names = _names()
        lines = [f"🔭 OBSERVE v2 신호 {len(fresh)}건 (실주문 0·관측)"]
        for r in fresh[:ALERT_BATCH_MAX]:
            nm = names.get(r["code"], "")
            lines.append(f"· {nm}({r['code']}) {r['signal_time']} "
                         f"+{r['signal_chg']}% 강도{r['strength']}")
        if len(fresh) > ALERT_BATCH_MAX:
            lines.append(f"· … 외 {len(fresh) - ALERT_BATCH_MAX}건")
        lines.append(f"오늘 누적 {n_total}건 · 저녁 replay 대조 예정")
        try:
            if send is None:
                from verifiers._common import send_telegram as send
            send("\n".join(lines))
        except Exception as e:  # noqa: BLE001
            print(f"[observe_v2] 텔레그램 실패(기록은 완료): {e}")
    return 0


def _compare(intents: dict, truth: dict) -> dict:
    """장중 기록 ↔ 최종파일 진실 대조. matched=신호행 일치(시각+가격),
    time_drift=같은 종목인데 신호행 상이(late-arrival로 최초행이 바뀜),
    missed=장중 미포착, phantom=장중엔 있었는데 최종엔 없음."""
    matched, drift, missed, phantom, latencies = [], [], [], [], []
    for code, tr in truth.items():
        li = intents.get(code)
        if li is None:
            missed.append(code)
            continue
        if li.get("signal_time") == tr.get("signal_time"):
            matched.append(code)
            det = li.get("detected_at") or ""
            if det and det >= li["signal_time"]:
                h1, m1, s1 = (int(x) for x in li["signal_time"].split(":"))
                h2, m2, s2 = (int(x) for x in det.split(":"))
                latencies.append((h2 * 3600 + m2 * 60 + s2) - (h1 * 3600 + m1 * 60 + s1))
        else:
            drift.append({"code": code, "live": li.get("signal_time"),
                          "final": tr.get("signal_time")})
    for code in intents:
        if code not in truth:
            phantom.append(code)
    n_truth = len(truth)
    lat_sorted = sorted(latencies)
    if n_truth and len(matched) == n_truth and not phantom:
        verdict = "PASS"
    elif not n_truth and not intents:
        verdict = "EMPTY"          # 신호 자체가 없던 날 (판정 불가·정상)
    else:
        verdict = "CHECK"          # phantom-only 포함 — 불일치는 전부 규명 대상
    return {
        "truth_total": n_truth,
        "matched": len(matched),
        "match_pct": round(100 * len(matched) / n_truth, 1) if n_truth else None,
        "time_drift": drift,
        "missed": missed,
        "phantom": phantom,
        "latency_sec": {
            "n": len(lat_sorted),
            "median": lat_sorted[len(lat_sorted) // 2] if lat_sorted else None,
            "max": lat_sorted[-1] if lat_sorted else None,
        },
        "verdict": verdict,
    }


def run_compare(day: str = None) -> int:
    day = day or date.today().strftime("%Y%m%d")
    try:
        state = json.loads(INTENTS_PATH.read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001
        state = {}
    if state.get("date") != day:
        print(f"[observe_v2] compare: {day} intent 기록 없음(가동 전/휴장) — skip")
        return 0
    if not (pb.TICKS / day).exists():
        print(f"[observe_v2] compare: ticks/{day} 없음 — 진실 셋 불가·skip")
        return 0
    result = _compare(state.get("intents", {}), _scan_v2_signals(day))
    result["date"] = day
    result["generated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    result["note"] = ("OBSERVE 대조 — 실주문 0. matched=실시간이 최종파일과 동일 신호행. "
                      "drift/missed/phantom>0이면 라이브 전환 전 원인 규명 의무.")
    _atomic_write(COMPARE_PATH, result)
    # 일별 증거 누적 (덮어쓰기 방지 — 라이브 전환 판정은 이 히스토리로)
    try:
        hist = json.loads(HISTORY_PATH.read_text(encoding="utf-8"))
        if not isinstance(hist, list):
            hist = []
    except Exception:  # noqa: BLE001
        hist = []
    hist = [h for h in hist if h.get("date") != day]   # 재실행 멱등
    hist.append(dict(result, intents=state.get("intents", {})))
    _atomic_write(HISTORY_PATH, hist[-HISTORY_KEEP:])
    lat = result["latency_sec"]
    print(f"[observe_v2] {day} 대조 — {result['verdict']}: "
          f"일치 {result['matched']}/{result['truth_total']} "
          f"drift {len(result['time_drift'])} miss {len(result['missed'])} "
          f"phantom {len(result['phantom'])} / 지연 med {lat['median']}s max {lat['max']}s")
    return 0


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--once", action="store_true")
    ap.add_argument("--compare", action="store_true")
    ap.add_argument("--day", default=None, help="YYYYMMDD (기본 오늘)")
    ap.add_argument("--no-tg", action="store_true", help="텔레그램 생략(스모크 테스트용)")
    args = ap.parse_args()
    try:
        rc = run_compare(args.day) if args.compare else run_once(args.day, no_tg=args.no_tg)
    except Exception as e:  # noqa: BLE001
        print(f"[observe_v2] 치명 예외(관측 도구 — 무시): {e}")
        rc = 0
    sys.exit(rc)
