# -*- coding: utf-8 -*-
"""명분 클러스터 매일수확 — forward 페이퍼 트랙 (7/1 사장님 지시).

사장님 전략: 뜨거운 명분 클러스터(예: 호남 반도체 1,000조 → 건설/전선 상한가) 안에서
후발주가 릴레이로 오른다 → 오를 놈 잡아 +5~8% 익절 → 내일 또 다른 놈 → 매일 수확.

이 모듈 = 그 전략을 forward 페이퍼로 매일 축적·측정(흑자 엣지 증명용). record-only.

방식(결정적 replay — 매 실행마다 전체 이력 재생 → 상태오염 0, 하루씩 자동 누적):
  각 거래일 D:
    1) 전일 결정된 대기진입을 D 시가에 paper 매수
    2) 보유 포지션을 D의 OHLC로 청산체크 (SL 우선 → TP → 타임스톱)
    3) D 상한가에서 명분 클러스터 판정(hot) → 후발주 top K를 내일 진입 대기로

★★★ 안전 불변식 ★★★
- record-only: data_store/cluster_harvest_paper.json 만 기록. 매수/매도/picks/
  recommendation/SAJANG/order_intent 0 접촉. 봇 OFF·실주문 0. 한국스윙(FLOWX) 무접촉.
- 관측 검증(2~3주 forward) 후 사장님 승인 → 매매 연결(관측 없이 flip 금지).
- read-only 입력: ranking_snapshots/uplowprice.csv + universe.json + daily/*.csv.

실행: python tools/cluster_harvest_paper.py [--tp 6] [--sl -3] [--hold 3] [--k 3]
"""
import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent
DS = BASE / "data_store"
DAILY = DS / "daily"
UNIVERSE = DS / "universe.json"
UPLOW = DS / "ranking_snapshots" / "uplowprice.csv"
OUT = DS / "cluster_harvest_paper.json"

# 명분 클러스터 = 종목명 키워드(인프라 = 반도체클러스터 건설/전력 수혜). 확장 가능.
INFRA_KW = ["건설", "토건", "토목", "산업개발", "전선", "전력", "케이블", "중전", "레미콘", "건재"]

# 기본 파라미터(사장님 +5~8% 익절 · −3% 손절 · 짧은 보유)
DEF_TP, DEF_SL, DEF_HOLD, DEF_K = 6.0, -3.0, 3, 3
MIN_CLUSTER = 3          # 인프라 상한가 >=3 = 명분 클러스터 hot
FOLLOW_LO, FOLLOW_HI = 0.0, 12.0   # 후발주 = D일 0~+12%(아직 안 터짐 = 맥점)
LIMIT_UP_CHG = 29.0

# 7/4 레짐검증 게이트 (record-only 병렬 코호트 — 기존 summary/필드 100% 불변·additive만)
# decide_day(결정일) breadth≤0.45 → 익일 진입 스킵 코호트. decide_day 마감 데이터 = look-ahead 0.
# ★임계 단일진실 = data/market_regime_gate.py NO_GO_BREADTH(0.45) — 이 파일은 단독실행
# 스크립트(sys.path 무의존)라 로컬 상수 유지. 저쪽 변경 시 여기도 동기 필수(검수 M1).
GATE_BREADTH = 0.45

_daily_cache = {}
_breadth_cache = {}


def _breadth_of_day(d):
    """decide_day의 breadth_pct — nightly ④ ledger의 market_context 재사용 (없으면 None)."""
    if d in _breadth_cache:
        return _breadth_cache[d]
    v = None
    try:
        p = DS / "paper_3type" / f"ledger_{d}.json"
        if p.exists():
            mc = (json.loads(p.read_text(encoding="utf-8")).get("market_context") or {})
            v = mc.get("breadth_pct")
    except Exception:  # noqa: BLE001
        v = None
    _breadth_cache[d] = v
    return v


def _gate_pass(decide_day):
    """게이트 통과 여부 — breadth 미상(None)은 보수적 통과(게이트 미작동)."""
    b = _breadth_of_day(decide_day)
    return not (b is not None and b <= GATE_BREADTH)


def _load_daily(code):
    if code in _daily_cache:
        return _daily_cache[code]
    f = DAILY / f"{code}.csv"
    out = {}
    if f.exists():
        try:
            lines = f.read_text(encoding="utf-8").splitlines()[1:]
        except Exception:
            lines = []
        for ln in lines:  # M-2: 행 단위 가드 — 한 행 손상돼도 종목 전체 손실 방지
            p = ln.split(",")
            if len(p) >= 5:
                try:
                    out[p[0]] = (float(p[1]), float(p[2]), float(p[3]), float(p[4]))
                except ValueError:
                    continue
    _daily_cache[code] = out
    return out


def _load_universe():
    try:
        uni = json.loads(UNIVERSE.read_text(encoding="utf-8"))
    except Exception:
        return {}, {}, []
    name_of = {c: uni[c].get("name", c) for c in uni}
    infra = [c for c in uni if any(k in name_of[c] for k in INFRA_KW)]
    return uni, name_of, infra


def _limitup_history():
    """ranking_snapshots/uplowprice.csv → {date: [(code, name)]}."""
    hist = {}
    if not UPLOW.exists():
        return hist
    try:
        for ln in UPLOW.read_text(encoding="utf-8").splitlines()[1:]:
            p = ln.split(",")
            if len(p) >= 5:
                try:
                    if float(p[4]) >= LIMIT_UP_CHG:
                        hist.setdefault(p[0], []).append((p[1], p[2]))
                except ValueError:
                    pass
    except Exception:
        return {}
    return hist


def _is_infra(name):
    return any(k in name for k in INFRA_KW)


def _trading_days():
    return sorted(_load_daily("005930").keys())


def _select_followers(D, prev_day, up_codes, infra_codes, name_of, k):
    """D일 후발주 = 인프라 유니버스·상한가 아님·D일 0~+12%(휴면/초입). top K."""
    cands = []
    for c in infra_codes:
        if c in up_codes:
            continue
        dd = _load_daily(c)
        if D not in dd or prev_day not in dd or dd[prev_day][3] <= 0:
            continue
        chg_d = (dd[D][3] / dd[prev_day][3] - 1) * 100
        if FOLLOW_LO <= chg_d <= FOLLOW_HI:
            cands.append((c, round(chg_d, 1)))
    cands.sort(key=lambda x: -x[1])  # 막 반응 시작한(살짝 오른) 것부터
    return cands[:k]


def run(tp=DEF_TP, sl=DEF_SL, hold=DEF_HOLD, k=DEF_K, save=True):
    uni, name_of, infra = _load_universe()
    hist = _limitup_history()
    days = _trading_days()
    if not uni or not hist or not days:
        return {"error": "입력 부족(universe/ranking/daily)", "trades": []}

    hist_dates = set(hist.keys())
    open_pos = []        # 보유중 [{code,name,entry_day,entry,exit_by,tp_px,sl_px}]
    pending = []         # 내일 진입 대기 [(code, name, decide_day, chg_d)]
    closed = []          # 청산완료
    daily_log = []

    for i, D in enumerate(days):
        if D < min(hist_dates):
            continue
        # 1) 전일 결정 대기진입 → D 시가 매수
        newly = []
        for code, nm, dday, chgd in pending:
            dd = _load_daily(code)
            if D not in dd or dd[D][0] <= 0:
                continue
            entry = dd[D][0]
            newly.append({"code": code, "name": nm, "decide_day": dday,
                          "entry_day": D, "entry": entry, "chg_decide": chgd,
                          "tp_px": entry * (1 + tp / 100), "sl_px": entry * (1 + sl / 100),
                          "exit_by_idx": i + hold})
        open_pos.extend(newly)
        pending = []

        # 2) 보유 포지션 D OHLC로 청산체크 (SL 우선 → TP → 타임스톱)
        still = []
        for pos in open_pos:
            dd = _load_daily(pos["code"])
            if D not in dd:
                still.append(pos)
                continue
            o, h, l, cl = dd[D]
            ret, why = None, None
            if D != pos["entry_day"]:   # 진입 당일은 다음날부터 청산체크(시가진입)
                if l <= pos["sl_px"]:
                    ret, why = sl, "SL"
                elif h >= pos["tp_px"]:
                    ret, why = tp, "TP"
            if ret is None and i >= pos["exit_by_idx"]:
                ret = (cl / pos["entry"] - 1) * 100 if pos["entry"] > 0 else 0.0
                why = "HOLD"
            if ret is not None:
                closed.append({"decide_day": pos["decide_day"], "entry_day": pos["entry_day"],
                               "exit_day": D, "code": pos["code"], "name": pos["name"],
                               "ret": round(ret, 2), "why": why,
                               # 7/4 additive — 게이트 코호트 주석(기존 7필드 불변)
                               "decide_breadth": _breadth_of_day(pos["decide_day"]),
                               "gate_pass": _gate_pass(pos["decide_day"])})
            else:
                still.append(pos)
        open_pos = still

        # 3) D 명분 클러스터 판정 → 후발주 대기(내일 진입)
        if D in hist:
            ups = hist[D]
            infra_up = [c for c, nm in ups if _is_infra(nm)]
            up_codes = {c for c, _ in ups}
            if len(infra_up) >= MIN_CLUSTER and i > 0:
                picks = _select_followers(D, days[i - 1], up_codes, infra, name_of, k)
                pending = [(c, name_of.get(c, c), D, ch) for c, ch in picks]

        daily_log.append({"date": D, "open_pos": len(open_pos),
                          "pending": len(pending), "closed_cum": len(closed)})

    rets = [t["ret"] for t in closed]
    wins = [r for r in rets if r > 0]
    summary = {
        "trades": len(closed),
        "win_pct": round(len(wins) / len(rets) * 100, 1) if rets else 0.0,
        "avg_ret": round(sum(rets) / len(rets), 2) if rets else 0.0,
        "sum_ret": round(sum(rets), 1),
        "open_now": len(open_pos),
        "pending_next": len(pending),
    }
    # 7/4 additive — 게이트 코호트(결정일 breadth≤0.45 스킵) 병렬 요약.
    # 트레이드가 완전 독립(자본 제약·상호작용 0)이라 부분집합 집계 = 게이트 시뮬과 수학적 동치.
    g_rets = [t["ret"] for t in closed if t.get("gate_pass")]
    g_wins = [r for r in g_rets if r > 0]
    summary_gated = {
        "trades": len(g_rets),
        "win_pct": round(len(g_wins) / len(g_rets) * 100, 1) if g_rets else 0.0,
        "avg_ret": round(sum(g_rets) / len(g_rets), 2) if g_rets else 0.0,
        "sum_ret": round(sum(g_rets), 1),
        "skipped_by_gate": len(rets) - len(g_rets),
        "breadth_coverage": sum(1 for t in closed if t.get("decide_breadth") is not None),
        "gate_rule": f"decide_day breadth<={GATE_BREADTH} 스킵 (7/4 레짐검증·회피 전용·미상=통과)",
    }
    result = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "params": {"tp": tp, "sl": sl, "hold": hold, "k": k,
                   "cluster": "infra(건설/전선/전력)", "min_cluster": MIN_CLUSTER},
        "summary": summary,
        "summary_gated": summary_gated,
        "closed_trades": closed,
        "pending_entries": [{"code": c, "name": nm, "decide_day": dd, "chg_decide": ch,
                             "decide_breadth": _breadth_of_day(dd),
                             "gate_pass": _gate_pass(dd)}
                            for c, nm, dd, ch in pending],
        "daily_log": daily_log[-10:],
        "note": "record-only / 매수·매도·picks·SAJANG·order 무접촉 / 관측 없이 flip 금지 / 한국스윙 무접촉"
                " / 7/4 게이트 코호트=병렬 기록(summary 불변·additive)",
    }
    if save:
        tmp = OUT.with_suffix(".tmp")
        tmp.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(OUT)
    return result


def format_report(r):
    if r.get("error"):
        return f"[cluster_harvest] {r['error']}"
    s = r["summary"]
    p = r["params"]
    lines = [f"=== 명분 클러스터 매일수확 페이퍼 (TP+{p['tp']}/SL{p['sl']}/D+{p['hold']}/K{p['k']}) "
             f"— record-only ===",
             f"거래 {s['trades']} | 승률 {s['win_pct']}% | 평균 {s['avg_ret']:+.2f}% | "
             f"합 {s['sum_ret']:+.1f}% | 보유중 {s['open_now']} | 내일대기 {s['pending_next']}"]
    for t in r["closed_trades"][-15:]:
        lines.append(f"  {t['entry_day']} {t['name']}({t['code']}) "
                     f"-> {t['ret']:+.1f}% {t['why']}@{t['exit_day']}")
    if r["pending_entries"]:
        lines.append("\n[내일 진입 대기 — 오늘 클러스터 후발주]")
        for e in r["pending_entries"]:
            lines.append(f"  {e['name']}({e['code']}) 오늘 {e['chg_decide']:+.0f}%")
    return "\n".join(lines)


if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    ap = argparse.ArgumentParser()
    ap.add_argument("--tp", type=float, default=DEF_TP)
    ap.add_argument("--sl", type=float, default=DEF_SL)
    ap.add_argument("--hold", type=int, default=DEF_HOLD)
    ap.add_argument("--k", type=int, default=DEF_K)
    ap.add_argument("--no-save", action="store_true")
    args = ap.parse_args()
    try:  # M-4: record-only 관측 — 어떤 예외도 nightly 파이프라인 위협 0 (fail-soft)
        res = run(tp=args.tp, sl=args.sl, hold=args.hold, k=args.k, save=not args.no_save)
        print(format_report(res))
    except Exception as e:  # noqa: BLE001
        print(f"[cluster_harvest] ERROR (fail-soft·record-only): {str(e)[:200]}")
