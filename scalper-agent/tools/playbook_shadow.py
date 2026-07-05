# -*- coding: utf-8 -*-
"""플레이북 페이퍼 엔진 v1 (7/4 신설) — 사장님 "바둑처럼 경우의 수" 지시의 관측 구현.

사장님 7/4: "장 시작 전 정찰을 약간의 토대로, 장중 이미 오르는 종목이 더 갈 것 같으면
잠깐 들어가 먹고 나오고 / 상한가 엔진 / 소문에 사서 뉴스에 팔고 / 뉴스에 사서 개미
유입에 팔고 — 여러 경우의 수를 바둑처럼 유연하게."

v1 구현 플레이북 (ticks 결정적 replay — data_store/ticks/YYYYMMDD/{code}.csv,
전 2,596종목 ~20분 간격 체결강도 포함·7/4 정찰로 발굴된 미사용 자산 69거래일):

  PB-A 추매단타(momentum_5pct): 장중 +5% 도달 관측 → "다음 관측행" 가격 진입(관측
       지연 정직 반영) → +5% TP / -3% SL / 15:20+ 마지막 관측가 청산. 14:30 이후 신호 제외.
  PB-B 상한가 D+1 스파이크(limitup_d1): D0 상한가 마감(마지막 관측 change_rate>=29)
       → D+1 첫 관측가 진입 → 동일 청산. ★7/4 실측: D+1 시가 홀드는 음엣지(종가 -7.6%·
       SL터치 85%) — 스파이크 단타의 성패는 고가/저가 "도달 순서"이고 그걸 ticks가 판별.

기록 코호트(진입 시점에 알 수 있는 값만 — look-ahead 0):
  · strength_gate: 신호 관측행의 체결강도 >= 100
  · regime_gate: 진입일 직전 거래일 ledger breadth_pct > 0.45 (7/4 레짐검증·미상=통과)

정직 한계(해석 의무): ①관측 해상도 ~20분 — TP/SL 교차 가격은 "처음 관측된 교차행의
실측가"를 사용(윈도우 내부 경로 가정 0·같은 행에서 둘 다 성립하면 SL 우선=보수적).
②진입도 신호 다음 관측행이라 실전(초 단위)보다 불리한 체결 가정=보수적.
③ticks에 간헐 price=0 불량행 → 필터.

★ 안전 불변식: record-only(data_store/playbook_shadow.json 기록만)·읽기전용 입력·
  매수/매도/picks/SAJANG/주문 0접촉·관측 없이 flip 금지(라이브 연결=사장님 결정).
사용: python tools/playbook_shadow.py [--days N(최근 N거래일만)]
"""
import csv
import json
import sys
from datetime import datetime
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent
DS = BASE / "data_store"
TICKS = DS / "ticks"
LEDGER_DIR = DS / "paper_3type"
OUT = DS / "playbook_shadow.json"

# 파라미터 (관측용 — 라이브 승격 시 사장님 결정+SAJANG 등재)
TP_PCT = 5.0            # 사장님 "5~8% 먹고 나온다" 하단
SL_PCT = -3.0           # 사장님 영구 -3% 타이트 손절과 동일 값(관측 정합)
SIGNAL_PCT = 5.0        # PB-A: 장중 +5% 도달 = "이미 오르는 종목"
SIGNAL_CUTOFF = "14:30:00"   # 이후 신호는 진입 제외(청산 시간 부족)
LIMITUP_PCT = 29.0      # PB-B: D0 마지막 관측 등락률 >= 29 = 상한가 마감 근사
STRENGTH_GATE = 100.0   # 체결강도 코호트 임계
REGIME_BREADTH = 0.45   # 단일진실=data/market_regime_gate.NO_GO_BREADTH (단독스크립트라 로컬·동기 필수)


def _read_ticks(day: str, code: str) -> list:
    """ticks/{day}/{code}.csv → [(time, price, chg_rate, strength)] price>0만, 시간순."""
    p = TICKS / day / f"{code}.csv"
    if not p.exists():
        return []
    out = []
    try:
        with p.open(encoding="utf-8") as f:
            for row in csv.DictReader(f):
                try:
                    t = str(row.get("time", ""))
                    px = float(row.get("price") or 0)
                    cr = float(row.get("change_rate") or 0)
                    st = float(row.get("strength") or 0)
                except (ValueError, TypeError):
                    continue
                if t and px > 0:
                    out.append((t, px, cr, st))
    except Exception:  # noqa: BLE001
        return []
    out.sort(key=lambda x: x[0])
    return out


def _simulate_exit(rows: list, ei: int, entry: float):
    """진입행 ei 이후 관측행 스캔 — SL 우선(보수) → TP → 마지막 관측가 청산."""
    tp_px = entry * (1 + TP_PCT / 100)
    sl_px = entry * (1 + SL_PCT / 100)
    for j in range(ei + 1, len(rows)):
        _, px, _, _ = rows[j]
        if px <= sl_px:
            return (px / entry - 1) * 100, "SL", rows[j][0]
        if px >= tp_px:
            return (px / entry - 1) * 100, "TP", rows[j][0]
    if ei + 1 <= len(rows) - 1:
        px = rows[-1][1]
        return (px / entry - 1) * 100, "EOD", rows[-1][0]
    return None, None, None   # 진입 후 관측행 없음 → 거래 불성립


def _ledger_breadth_map() -> dict:
    """date(YYYY-MM-DD) → breadth_pct (후보 0인 날 포함 전체 ledger)."""
    out = {}
    if not LEDGER_DIR.exists():
        return out
    for f in sorted(LEDGER_DIR.glob("ledger_2026-*.json")):
        if "_bc_only" in f.name:
            continue
        try:
            led = json.loads(f.read_text(encoding="utf-8"))
            b = (led.get("market_context") or {}).get("breadth_pct")
            if b is not None:
                out[led.get("date", f.stem.replace("ledger_", ""))] = b
        except Exception:  # noqa: BLE001
            continue
    return out


def _regime_pass(day: str, days_sorted: list, bmap: dict):
    """진입일 직전 '틱 존재 거래일'의 breadth — >0.45 통과·미상 통과(보수)."""
    i = days_sorted.index(day)
    if i == 0:
        return True, None
    prev = days_sorted[i - 1]
    prev_iso = f"{prev[:4]}-{prev[4:6]}-{prev[6:]}"
    b = bmap.get(prev_iso)
    return (not (b is not None and b <= REGIME_BREADTH)), b


def _pb_a_day(day: str, codes: list, regime_ok: bool, breadth_d1):
    """PB-A 추매단타 — day 하루 전 종목 replay."""
    trades = []
    for code in codes:
        rows = _read_ticks(day, code)
        if len(rows) < 3:
            continue
        sig = next((i for i, (t, px, cr, st) in enumerate(rows)
                    if cr >= SIGNAL_PCT and t <= SIGNAL_CUTOFF), None)
        if sig is None or sig + 1 >= len(rows):
            continue
        entry = rows[sig + 1][1]
        ret, why, exit_t = _simulate_exit(rows, sig + 1, entry)
        if ret is None:
            continue
        trades.append({
            "date": day, "code": code, "signal_time": rows[sig][0],
            "entry_time": rows[sig + 1][0], "exit_time": exit_t,
            "signal_chg": round(rows[sig][2], 2), "strength_at_signal": round(rows[sig][3], 1),
            "ret": round(ret, 2), "why": why,
            "strength_gate": rows[sig][3] >= STRENGTH_GATE,
            "regime_gate": regime_ok, "breadth_d1": breadth_d1,
        })
    return trades


def _pb_b_pairs(days_sorted: list):
    """PB-B — D0 상한가 마감 종목 → (D0, D+1, code) 쌍."""
    pairs = []
    for i in range(len(days_sorted) - 1):
        d0, d1 = days_sorted[i], days_sorted[i + 1]
        d0_dir = TICKS / d0
        for f in d0_dir.glob("*.csv"):
            rows = _read_ticks(d0, f.stem)
            if rows and rows[-1][2] >= LIMITUP_PCT:
                pairs.append((d0, d1, f.stem, rows[-1][1]))
    return pairs


def _pb_b_trades(pairs: list, days_sorted: list, bmap: dict):
    trades = []
    for d0, d1, code, d0_close in pairs:
        rows = _read_ticks(d1, code)
        if len(rows) < 2:
            continue
        entry = rows[0][1]
        ret, why, exit_t = _simulate_exit(rows, 0, entry)
        if ret is None:
            continue
        regime_ok, b = _regime_pass(d1, days_sorted, bmap)
        trades.append({
            "date": d1, "code": code, "d0": d0,
            "gap_pct": round((entry / d0_close - 1) * 100, 2) if d0_close else None,
            "entry_time": rows[0][0], "exit_time": exit_t,
            "strength_at_entry": round(rows[0][3], 1),
            "ret": round(ret, 2), "why": why,
            "strength_gate": rows[0][3] >= STRENGTH_GATE,
            "regime_gate": regime_ok, "breadth_d1": b,
        })
    return trades


def _summarize(trades: list, gate_key=None) -> dict:
    sel = [t for t in trades if (t.get(gate_key) if gate_key else True)]
    rets = [t["ret"] for t in sel]
    if not rets:
        return {"trades": 0}
    wins = sum(1 for r in rets if r > 0)
    return {
        "trades": len(rets),
        "win_pct": round(100 * wins / len(rets), 1),
        "avg_ret": round(sum(rets) / len(rets), 2),
        "sum_ret": round(sum(rets), 1),
        "tp": sum(1 for t in sel if t["why"] == "TP"),
        "sl": sum(1 for t in sel if t["why"] == "SL"),
        "eod": sum(1 for t in sel if t["why"] == "EOD"),
    }


def run(days_limit=None, save=True) -> dict:
    if not TICKS.exists():
        return {"error": "ticks 없음 (VPS 전용 자산 — VPS에서 실행)"}
    days_sorted = sorted(d.name for d in TICKS.iterdir() if d.is_dir())
    if days_limit:
        days_sorted = days_sorted[-int(days_limit):]
    bmap = _ledger_breadth_map()

    a_trades = []
    daily_log = []
    for day in days_sorted:
        codes = [f.stem for f in (TICKS / day).glob("*.csv")]
        regime_ok, b = _regime_pass(day, days_sorted, bmap)
        day_trades = _pb_a_day(day, codes, regime_ok, b)
        a_trades.extend(day_trades)
        daily_log.append({"date": day, "pb_a_signals": len(day_trades)})

    b_trades = _pb_b_trades(_pb_b_pairs(days_sorted), days_sorted, bmap)

    out = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "params": {"tp": TP_PCT, "sl": SL_PCT, "signal_pct": SIGNAL_PCT,
                   "cutoff": SIGNAL_CUTOFF, "limitup": LIMITUP_PCT,
                   "strength_gate": STRENGTH_GATE, "regime_breadth": REGIME_BREADTH,
                   "days": len(days_sorted),
                   "range": f"{days_sorted[0]}~{days_sorted[-1]}" if days_sorted else ""},
        "pb_a_momentum": {
            "desc": "추매단타 — 장중 +5% 도달 다음 관측행 진입·TP+5/SL-3/EOD",
            "all": _summarize(a_trades),
            "strength_gate": _summarize(a_trades, "strength_gate"),
            "regime_gate": _summarize(a_trades, "regime_gate"),
            # 7/4 그리드 실측(69일·net 0.2% 비용 차감): 강도 단조개선·오전 우위.
            # 정제판(오전 10시前 신호+강도200+·TP5/SL3)만 net 양(+0.132%/건·승49%·합+270.7%p).
            # → 정식 관측 코호트로 고정, forward 매일 자가검증. flip은 사장님 결정.
            "refined_am_st200": _summarize(
                [t for t in a_trades
                 if t["strength_at_signal"] >= 200 and t["signal_time"] <= "10:00:00"]),
            "recent_trades": a_trades[-40:],
        },
        "pb_b_limitup_d1": {
            "desc": "상한가 D+1 스파이크 — D0 상한마감→D+1 첫 관측 진입·TP+5/SL-3/EOD",
            "all": _summarize(b_trades),
            "strength_gate": _summarize(b_trades, "strength_gate"),
            "regime_gate": _summarize(b_trades, "regime_gate"),
            "recent_trades": b_trades[-40:],
        },
        "daily_log": daily_log[-15:],
        "note": "record-only 플레이북 관측(7/4 신설·ticks ~20분 해상도). 진입=신호 다음 "
                "관측행(보수)·같은행 TP/SL은 SL 우선(보수)·경로 가정 0. 매수/매도/picks/"
                "SAJANG/order 무접촉·관측 없이 flip 금지. 소문→뉴스/뉴스→개미 플레이북은 "
                "뉴스 게시시각 미저장으로 v1 제외(forward 일단위 설계 별도).",
    }
    if save:
        tmp = OUT.with_suffix(".tmp")
        tmp.write_text(json.dumps(out, ensure_ascii=False, indent=1), encoding="utf-8")
        tmp.replace(OUT)
    return out


def _fmt(s: dict) -> str:
    if not s or not s.get("trades"):
        return "거래 0"
    return (f"{s['trades']}건 승{s['win_pct']}% 평균{s['avg_ret']:+.2f}% "
            f"합{s['sum_ret']:+.1f}%p (TP{s['tp']}/SL{s['sl']}/EOD{s['eod']})")


if __name__ == "__main__":
    import argparse
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=None)
    args = ap.parse_args()
    try:
        r = run(days_limit=args.days)
        if r.get("error"):
            print(f"[playbook] {r['error']}")
        else:
            print(f"=== 플레이북 페이퍼 ({r['params']['range']}·{r['params']['days']}일) ===")
            a, b = r["pb_a_momentum"], r["pb_b_limitup_d1"]
            print(f"PB-A 추매단타     전체: {_fmt(a['all'])}")
            print(f"                강도게이트: {_fmt(a['strength_gate'])}")
            print(f"                레짐게이트: {_fmt(a['regime_gate'])}")
            print(f"                ★정제판(오전+강도200): {_fmt(a.get('refined_am_st200', {}))}")
            print(f"PB-B 상한가D+1   전체: {_fmt(b['all'])}")
            print(f"                강도게이트: {_fmt(b['strength_gate'])}")
            print(f"                레짐게이트: {_fmt(b['regime_gate'])}")
            print("★ record-only·~20분 관측해상도(보수적 체결 가정)·flip은 사장님 결정")
    except Exception as e:  # noqa: BLE001
        print(f"[playbook] 치명 예외(무시): {e}")
    sys.exit(0)
