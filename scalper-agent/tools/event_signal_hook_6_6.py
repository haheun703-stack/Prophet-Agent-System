# -*- coding: utf-8 -*-
"""event_signal_hook — A타입 명분 보강 EVENT hook (6/6, read-only 태그/관측 전용).

사장님 6/6: A를 둘로 나눠 기록 —
  A1 = STEADY + DART_S        (DART 단독, primary 유지)
  A2 = STEADY + DART_S + NEWS/EVENT  (보강형)
→ 6/12: "DART만 봐도 되는가, 뉴스까지 붙이면 좋아지는가"를 숫자(수익/MFE/MAE)로 비교.

★★ NEWS/EVENT는 진입신호 아님 — forward 비교 태그/가산 관측만. 매매 확정권 0. ★★
  event_layer/a_variant는 ledger 기록일 뿐 어떤 진입/필터/분기에도 안 쓰임(hard gate 금지).
  실주문0 / scheduler0 / SAJANG무변경 / 봇OFF.

명분 소스: data/event_detector.py run_event_scan → data_store/events.json
  {scanned_at, beneficiaries:[{ticker,name,total_score,direction,events,metric}]}.
★ NEWS 백필 불가(설계 전제): events.json scanned_at == asof(같은 날)일 때만 fresh.
  과거 asof거나 stale이면 보강 없음(A1) — 있는 척 X.

라벨(신규/레짐재부각/오류/경계): 정보봇 event_signals.jsonl novelty 미생성 →
  가진 데이터(EVENT_TYPE/THEME/direction) 휴리스틱 근사. ★자동 novelty 아님(명시)★.
  forward 누적되면 '신규'(첫 등장) 판정 정교화 가능.

사용(단위): python tools/event_signal_hook_6_6.py --selftest
  (실데이터 스캔은 paper_3type_daily_run_6_6.py --scan-events 가 호출)
"""
import argparse
import json
import sys
from pathlib import Path

SA = Path(__file__).resolve().parent.parent
EVENTS_PATH = SA / "data_store" / "events.json"

SCORE_STRONG = 120.0   # event_layer 강도 라벨 구분(STRONG/MODERATE) — 게이트 아님, 기록용


def load_event_snapshot(asof, path=EVENTS_PATH):
    """events.json → {fresh, scanned_date, index:{ticker:ben}, count, reason}.
    fresh = scanned_at 날짜 == asof (NEWS 백필 불가 — 과거/stale은 fresh=False)."""
    p = Path(path)
    if not p.exists():
        return {"fresh": False, "scanned_date": None, "index": {}, "count": 0,
                "reason": "events.json 없음"}
    try:
        d = json.loads(p.read_text(encoding="utf-8"))
    except Exception as e:                                   # noqa: BLE001
        return {"fresh": False, "scanned_date": None, "index": {}, "count": 0,
                "reason": f"파싱실패:{e}"}
    scanned = (d.get("scanned_at") or "")[:10]
    bens = d.get("beneficiaries") or []
    idx = {}
    for b in bens:
        t = b.get("ticker")
        if t:
            idx[str(t)] = b
            idx[str(t).zfill(6)] = b
    fresh = bool(scanned and scanned == asof)
    return {"fresh": fresh, "scanned_date": scanned, "index": idx, "count": len(bens),
            "reason": ("ok" if fresh else f"stale(scanned={scanned} vs asof={asof})")}


def derive_labels(ben):
    """가진 데이터로 사장님 4라벨 근사 (자동 novelty 소스 미생성 — 휴리스틱).
    레짐재부각=THEME 명분 / 신규=DART 직접공시 / 경계=NEGATIVE·MIXED / 오류=데이터 이상."""
    labels = []
    evs = ben.get("events") or []
    has_theme = any(str(e).startswith("THEME:") for e in evs)
    has_dart = any(not str(e).startswith("THEME:") for e in evs)
    if has_theme:
        labels.append("레짐재부각")
    if has_dart:
        labels.append("신규")
    if ben.get("direction") in ("NEGATIVE", "MIXED"):
        labels.append("경계")
    # 오류 = 진짜 데이터 이상(ticker 형식·score 결측)만. 음수 score는 NEGATIVE 정상값이라 제외.
    t = ben.get("ticker")
    if not t or not str(t).isdigit() or ben.get("total_score") is None:
        labels.append("오류")
    return labels


def match_event_layer(code, snapshot):
    """종목코드 → event_layer dict 또는 None(보강 없음). fresh=False면 None(백필 불가)."""
    if not snapshot.get("fresh"):
        return None
    ben = snapshot["index"].get(str(code)) or snapshot["index"].get(str(code).zfill(6))
    if not ben:
        return None
    return {
        "matched": True,
        "total_score": ben.get("total_score"),
        "direction": ben.get("direction"),
        "event_types": ben.get("events"),
        "metric": ben.get("metric"),
        "strength": ("STRONG" if (ben.get("total_score") or 0) >= SCORE_STRONG else "MODERATE"),
        "labels": derive_labels(ben),
        "scanned_date": snapshot.get("scanned_date"),
    }


def event_layer_for(code, snapshot):
    """항상 dict 반환(기록용). 매칭=상세 / 미매칭=사유. variant 판정은 matched 키로."""
    layer = match_event_layer(code, snapshot)
    if layer:
        return layer
    return {"matched": False,
            "reason": ("no_event_for_ticker" if snapshot.get("fresh")
                       else snapshot.get("reason", "not_fresh")),
            "scanned_date": snapshot.get("scanned_date")}


def variant_and_source(base_source, event_layer):
    """A1/A2 + signal_source. DART_S primary 유지, NEWS는 태그 추가만(가산 관측, gate 아님)."""
    if event_layer and event_layer.get("matched"):
        return "A2", f"{base_source}+NEWS"
    return "A1", base_source


# ─────────────────────────── selftest (합성 snapshot, 네트워크 0) ───────────────────────────
def selftest():
    ok = []
    snap = {"fresh": True, "scanned_date": "2026-06-08", "count": 2, "reason": "ok",
            "index": {
                "005930": {"ticker": "005930", "name": "삼성전자", "total_score": 150.0,
                           "direction": "POSITIVE", "events": ["BIG_CONTRACT", "THEME:반도체"],
                           "metric": "HBM 세계1위"},
                "000660": {"ticker": "000660", "name": "SK하이닉스", "total_score": -30.0,
                           "direction": "NEGATIVE", "events": ["THEME:관세"], "metric": ""}}}
    l1 = match_event_layer("005930", snap)
    ok.append(("T1 매칭 → A2 layer STRONG", bool(l1) and l1["matched"] and l1["strength"] == "STRONG"))
    ok.append(("T2 라벨 신규+레짐재부각", "신규" in l1["labels"] and "레짐재부각" in l1["labels"]))
    l2 = match_event_layer("000660", snap)
    ok.append(("T3 NEGATIVE → 경계 라벨", "경계" in l2["labels"]))
    ok.append(("T4 미매칭 ticker → None", match_event_layer("999999", snap) is None))
    snap_stale = dict(snap, fresh=False, reason="stale")
    ok.append(("T5 stale → None(NEWS 백필불가)", match_event_layer("005930", snap_stale) is None))
    v1, s1 = variant_and_source("DART_S", l1)
    ok.append(("T6 매칭 → A2 / DART_S+NEWS", v1 == "A2" and s1 == "DART_S+NEWS"))
    v2, s2 = variant_and_source("DART_S", None)
    ok.append(("T7 미매칭 → A1 / DART_S primary유지", v2 == "A1" and s2 == "DART_S"))
    ef = event_layer_for("999999", snap)
    ok.append(("T8 미매칭 event_layer_for dict(matched False)",
               isinstance(ef, dict) and ef["matched"] is False and ef["reason"] == "no_event_for_ticker"))
    ef2 = event_layer_for("005930", snap_stale)
    ok.append(("T9 stale event_layer_for 사유기록", ef2["matched"] is False and "stale" in ef2["reason"]))
    # 데이터 이상 → 오류 라벨
    bad = {"ticker": "12AB", "total_score": 0, "direction": "NEUTRAL", "events": []}
    ok.append(("T10 데이터이상 → 오류 라벨", "오류" in derive_labels(bad)))
    # load_event_snapshot: 없는 경로 → fresh False 안전
    snp = load_event_snapshot("2026-06-08", path=SA / "data_store" / "__no_such__.json")
    ok.append(("T11 파일없음 → fresh False 안전", snp["fresh"] is False and snp["count"] == 0))
    print("event_signal_hook 셀프테스트:")
    for n, p in ok:
        print(f"  [{'PASS' if p else 'FAIL'}] {n}")
    return all(p for _, p in ok)


def main():
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    ap = argparse.ArgumentParser()
    ap.add_argument("--selftest", action="store_true")
    ap.add_argument("--asof", default=None, help="events.json fresh 여부 점검")
    a = ap.parse_args()
    if a.selftest:
        sys.exit(0 if selftest() else 1)
    if a.asof:
        snp = load_event_snapshot(a.asof)
        print(f"events.json: scanned={snp['scanned_date']} fresh={snp['fresh']} "
              f"count={snp['count']} reason={snp['reason']}")
    else:
        ap.print_help()


if __name__ == "__main__":
    main()
