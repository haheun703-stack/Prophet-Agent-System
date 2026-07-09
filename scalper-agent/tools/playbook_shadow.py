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

7/9 3축 결합 관측 (사장님 승인): PB-A 정제판(오전10시前+강도200+) 거래에 전일(D-1)
  마감 기준 끼/명분 라벨을 additive 부착 → 레짐×끼×명분 스택 코호트 성적을
  pb_a_momentum.stack_7_9 에 기여도 분해로 기록. 세 라벨 전부 진입 시점에 알 수 있던
  D-1 정보만 사용(look-ahead 0·7/4 D0 정정 교훈). 라벨 불가(아카이브 없음·일봉 부족)는
  미라벨=코호트 제외로 정직 처리(coverage에 노출). 기존 필드/요약 무수정.

7/9 매매원칙 지시서 관찰 백테스트 (docs/01-plan/TRADING_PRINCIPLES_단타봇_7_9.md):
  규칙2(추격 상한)·규칙3(서킷브레이커 streak/일일한도)·규칙4b(섹터 쿨다운)를 정제판
  거래 이력으로 '발동됐을 것' 정량화 → pb_a_momentum.principles_7_9. 차단 0·기록만
  (지시서 롤아웃 1~2단계 병합) — 활성화는 검증기준(규칙당 20회+) 통과 후 사장님 결정.

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
if str(BASE) not in sys.path:
    sys.path.insert(0, str(BASE))   # 7/9: 끼 단일진실(_kki_grade_at) 재사용용
DS = BASE / "data_store"
TICKS = DS / "ticks"
LEDGER_DIR = DS / "paper_3type"
OUT = DS / "playbook_shadow.json"
ARCHIVE_DIR = DS / "catalyst_archive"   # 명분 라벨 원천(⑪ 산출·읽기전용)

# 파라미터 (관측용 — 라이브 승격 시 사장님 결정+SAJANG 등재)
TP_PCT = 5.0            # 사장님 "5~8% 먹고 나온다" 하단
SL_PCT = -3.0           # 사장님 영구 -3% 타이트 손절과 동일 값(관측 정합)
SIGNAL_PCT = 5.0        # PB-A: 장중 +5% 도달 = "이미 오르는 종목"
SIGNAL_CUTOFF = "14:30:00"   # 이후 신호는 진입 제외(청산 시간 부족)
LIMITUP_PCT = 29.0      # PB-B: D0 마지막 관측 등락률 >= 29 = 상한가 마감 근사
STRENGTH_GATE = 100.0   # 체결강도 코호트 임계
REGIME_BREADTH = 0.45   # 단일진실=data/market_regime_gate.NO_GO_BREADTH (단독스크립트라 로컬·동기 필수)

# ── 7/9 3축 결합 관측 (사장님 승인) — 정제판 기준 + D-1 라벨 ──
REFINED_STRENGTH = 200.0     # 정제판: 신호행 체결강도 하한 (7/4 그리드 실측 net+ 유일 구간)
REFINED_CUTOFF = "10:00:00"  # 정제판: 오전 신호만
KKI_OK = ("HUNTABLE", "EXPLOSIVE")   # 끼 축 통과 등급(SAJANG 서열 상위 2)
MEONGBUN_OK = ("강", "중")           # 명분 축 통과 = meongbun_grade 상위 2(강/중 — 보통/약/무명분 제외)

# ── 7/9 매매원칙 지시서 관찰 백테스트 (docs/01-plan/TRADING_PRINCIPLES_단타봇_7_9.md) ──
# 관찰 전용 — 차단 0·기록만. 파라미터는 지시서 시작값(정답 아님·데이터로 조정).
CHASE_MAX = 8.0          # 규칙2: 신호 시점 당일 등락률 상한 후보
CB_STREAK = 3            # 규칙3: 연속 손절 발동 임계
CB_DAILY_LIMITS = (-6.0, -9.0)   # 규칙3: 일일 누적 %p 발동 후보(동일가중 프록시)
SECTOR_COOLDOWN_N = 3    # 규칙4b: 동일 섹터 당일 손절 누적 발동 임계


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


# ── 7/9 3축 결합 관측 헬퍼 (record-only·additive — 기존 필드/요약 무수정) ──

def _is_refined(t: dict) -> bool:
    """PB-A 정제판(7/4 실측 net+ 유일 구간): 오전 10시前 신호 + 신호행 체결강도 200+."""
    return (t.get("strength_at_signal", 0) >= REFINED_STRENGTH
            and t.get("signal_time", "99") <= REFINED_CUTOFF)


def _meongbun_map() -> dict:
    """catalyst_archive → {date(YYYY-MM-DD): {code: 명분등급(강/중/약/무명분)}}.
    ⑪ 산출물 읽기전용 — 디렉터리/파일 없으면 빈 dict(미라벨 유지)."""
    out = {}
    if not ARCHIVE_DIR.exists():
        return out
    for f in sorted(ARCHIVE_DIR.glob("catalyst_*.json")):
        try:
            d = json.loads(f.read_text(encoding="utf-8"))
        except Exception:  # noqa: BLE001
            continue
        codes = {}
        for it in d.get("items", []) or []:
            c = it.get("code")
            if c:
                codes[c] = it.get("meongbun_grade") or "무명분"
        out[f.stem.replace("catalyst_", "")] = codes
    return out


def _kki_d1_fn(code: str, d1_iso: str):
    """전일(D-1) 마감 기준 끼 등급 — paper_sim_portfolio._kki_grade_at 재사용
    (SAJANG.kki_grade/score_kki 단일진실·(code,date) 캐시). 실패 시 None(미라벨=보수)."""
    try:
        from data.paper_sim_portfolio import _kki_grade_at
        return _kki_grade_at(code, d1_iso)
    except Exception:  # noqa: BLE001
        return None


def _label_stack(a_trades: list, prev_iso: dict, arch: dict, kki_fn=_kki_d1_fn) -> None:
    """정제판 거래에만 D-1 라벨 additive 부착(kki_d1/meongbun_d1) — look-ahead 0.
    meongbun: D-1 아카이브 있는데 미수록='비수록'(명분끼 후보 아니었음) / 아카이브 없음=None.
    D-1 미상(첫 관측일)=None. 비정제 거래는 무수정(필드 자체 미부착)."""
    for t in a_trades:
        if not _is_refined(t):
            continue
        d1 = prev_iso.get(t.get("date"))
        if not d1:
            t["kki_d1"] = None
            t["meongbun_d1"] = None
            continue
        t["kki_d1"] = kki_fn(t["code"], d1)
        t["meongbun_d1"] = (arch[d1].get(t["code"], "비수록") if d1 in arch else None)


def _stack_cohorts(a_trades: list) -> dict:
    """3축 스택 코호트 성적 — 전부 정제판의 부분집합(기여도 분해)·빈도(trades/day) 포함."""
    ref = [t for t in a_trades if _is_refined(t)]

    def _pack(sel: list) -> dict:
        s = _summarize(sel)
        days = {t["date"] for t in sel}
        s["days_with_trades"] = len(days)
        s["trades_per_day"] = round(len(sel) / len(days), 1) if days else 0.0
        return s

    def _kki_ok(t):
        return t.get("kki_d1") in KKI_OK

    def _mb_ok(t):
        return t.get("meongbun_d1") in MEONGBUN_OK

    def _rg(t):
        return bool(t.get("regime_gate"))

    return {
        "refined": _pack(ref),
        "refined_x_regime": _pack([t for t in ref if _rg(t)]),
        "refined_x_kki": _pack([t for t in ref if _kki_ok(t)]),
        "refined_x_meongbun": _pack([t for t in ref if _mb_ok(t)]),
        "stack3_any": _pack([t for t in ref if _rg(t) and (_kki_ok(t) or _mb_ok(t))]),
        "stack3_strict": _pack([t for t in ref if _rg(t) and _kki_ok(t) and _mb_ok(t)]),
        "coverage": {
            "refined_total": len(ref),
            "kki_labeled": sum(1 for t in ref if t.get("kki_d1") is not None),
            "meongbun_labeled": sum(1 for t in ref if t.get("meongbun_d1") is not None),
        },
    }


# ── 7/9 매매원칙 지시서 관찰 백테스트 헬퍼 (record-only — 차단 0·기록만) ──

def _sector_map() -> dict:
    """universe.json → {code: sector}. 실패 시 빈 dict(섹터 시뮬 skip)."""
    try:
        u = json.loads((DS / "universe.json").read_text(encoding="utf-8"))
        return {k: (v.get("sector") or "") for k, v in u.items()}
    except Exception:  # noqa: BLE001
        return {}


def _chase_bands(ref: list) -> dict:
    """규칙2 — 신호 시점 등락률 대역별 성적(추격 상한 CHASE_MAX 검증).
    주의: 지시서 2b(5분 기울기)는 ~20분 관측 해상도로 검증 불가(정직 미포함)."""
    bands = [(5, 6), (6, 7), (7, 8), (8, 10), (10, 999)]
    out = {f"chg_{lo}~{hi if hi < 999 else ''}": _summarize(
        [t for t in ref if lo <= t.get("signal_chg", 0) < hi]) for lo, hi in bands}
    out["under_chase_max"] = _summarize(
        [t for t in ref if t.get("signal_chg", 0) < CHASE_MAX])
    out["over_chase_max"] = _summarize(
        [t for t in ref if t.get("signal_chg", 0) >= CHASE_MAX])
    return out


def _cb_sim(ref: list, mode: str, streak: int = CB_STREAK, limit: float = -6.0) -> dict:
    """규칙3 — 서킷브레이커 '발동됐을 것' 시뮬. 발동 이후 당일 잔여 진입=차단분.
    streak: 연속 SL n회(TP=리셋·EOD=중립) / loss: 실현 %p 누적 ≤ limit(동일가중 프록시).
    차단분 합이 음수 = 서킷브레이커가 살렸을 손실."""
    by_day = {}
    for t in ref:
        by_day.setdefault(t["date"], []).append(t)
    blocked, days_triggered = [], 0
    for day in sorted(by_day):
        trades = by_day[day]
        closed = sorted((t for t in trades if t.get("exit_time")),
                        key=lambda x: x["exit_time"])
        trig, run, cum = None, 0, 0.0
        for t in closed:
            if mode == "streak":
                if t["why"] == "SL":
                    run += 1
                elif t["why"] == "TP":
                    run = 0
                if run >= streak:
                    trig = t["exit_time"]
                    break
            else:
                cum += t["ret"]
                if cum <= limit:
                    trig = t["exit_time"]
                    break
        if trig is None:
            continue
        days_triggered += 1
        blocked.extend(t for t in trades if t["entry_time"] > trig)
    out = _summarize(blocked)
    out["days_triggered"] = days_triggered
    return out


def _sector_cooldown_sim(ref: list, smap: dict, n: int = SECTOR_COOLDOWN_N,
                         all_days: list = None) -> dict:
    """규칙4b — 동일 섹터 당일 SL n회 누적 → 해당 섹터 당일 잔여+익일 진입 차단 시뮬.
    익일 = 전 ticked 거래일 기준(all_days) — 정제판 거래 없는 날을 건너뛰면 쿨다운이
    과확장되어 절감액 상향 편향(Tier1 MEDIUM fix). 미지정 시 ref 날짜 fallback."""
    days_sorted = sorted(all_days) if all_days else sorted({t["date"] for t in ref})
    nxt = dict(zip(days_sorted[:-1], days_sorted[1:]))
    by_day = {}
    for t in ref:
        by_day.setdefault(t["date"], []).append(t)
    blocked, trigger_events = {}, 0
    for day in days_sorted:
        sl_cnt, trig = {}, {}
        # all_days 기준 순회 — 정제판 거래 0인 거래일 존재(.get 필수·KeyError 방지)
        for t in sorted((x for x in by_day.get(day, []) if x["why"] == "SL"),
                        key=lambda x: x["exit_time"]):
            sec = smap.get(t["code"])
            if not sec:
                continue
            sl_cnt[sec] = sl_cnt.get(sec, 0) + 1
            if sl_cnt[sec] == n:
                trig[sec] = t["exit_time"]
        trigger_events += len(trig)
        for sec, tt in trig.items():
            for t in by_day.get(day, []):
                if smap.get(t["code"]) == sec and t["entry_time"] > tt:
                    blocked[(t["date"], t["code"], t["entry_time"])] = t
            for t in by_day.get(nxt.get(day, ""), []):
                if smap.get(t["code"]) == sec:
                    blocked[(t["date"], t["code"], t["entry_time"])] = t
    out = _summarize(list(blocked.values()))
    out["trigger_events"] = trigger_events
    return out


def _principles_7_9(a_trades: list, all_days: list = None) -> dict:
    """지시서 규칙2/3/4b 관찰 백테스트 — 정제판 기준(유일 net+ 후보 = 의사결정 대상).
    예외 시 {} 반환(Tier1 LOW fix) — 관측 블록 실패가 전체 산출 저장을 막지 않게 격리."""
    try:
        ref = [t for t in a_trades if _is_refined(t)]
        smap = _sector_map()
        cb = {"streak3": _cb_sim(ref, "streak")}
        for lim in CB_DAILY_LIMITS:
            cb[f"daily_{lim:+.0f}p"] = _cb_sim(ref, "loss", limit=lim)
        return {
            "rule2_chase": _chase_bands(ref),
            "rule3_circuit_breaker": cb,
            "rule4b_sector_cooldown": _sector_cooldown_sim(ref, smap, all_days=all_days),
            "note": "관찰 전용(차단 0·발동됐을 것 정량화만). 동일가중 %p 프록시·"
                    "규칙2b(5분 기울기)는 ~20분 해상도로 검증 불가. 차단분 합이 음수="
                    "규칙이 살렸을 손실. 활성화=검증기준 통과+사장님 결정.",
        }
    except Exception:  # noqa: BLE001
        return {}


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

    # 7/9 3축 결합: 정제판 거래에 D-1 끼/명분 라벨 부착(additive·look-ahead 0)
    prev_iso = {day: f"{p[:4]}-{p[4:6]}-{p[6:]}"
                for day, p in zip(days_sorted[1:], days_sorted[:-1])}
    _label_stack(a_trades, prev_iso, _meongbun_map())

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
            "refined_am_st200": _summarize([t for t in a_trades if _is_refined(t)]),
            # 7/9 3축 결합(사장님 승인): 정제판 × D-1 레짐/끼/명분 — 기여도 분해.
            # stack3_any=레짐∧(끼 or 명분='오를 근거')·strict=전부∧.
            # 명분 아카이브(⑪) 시작 전 날짜는 미라벨=코호트 제외(coverage 참조).
            "stack_7_9": _stack_cohorts(a_trades),
            # 7/9 매매원칙 지시서(01-plan/TRADING_PRINCIPLES_단타봇_7_9.md) 규칙2/3/4b
            # 관찰 백테스트 — 차단 0·'발동됐을 것' 정량화만(롤아웃 1~2단계 병합).
            "principles_7_9": _principles_7_9(a_trades, all_days=days_sorted),
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
            st = a.get("stack_7_9", {})
            if st:
                print(f"                3축 ×레짐: {_fmt(st.get('refined_x_regime', {}))}")
                print(f"                3축 ×끼(HUNTABLE+): {_fmt(st.get('refined_x_kki', {}))}")
                print(f"                3축 ×명분(강/중): {_fmt(st.get('refined_x_meongbun', {}))}")
                print(f"                ★3축 완성판(레짐∧[끼or명분]): {_fmt(st.get('stack3_any', {}))}")
                cv = st.get("coverage", {})
                print(f"                라벨 커버리지: 정제판 {cv.get('refined_total', 0)}건 중 "
                      f"끼 {cv.get('kki_labeled', 0)}·명분 {cv.get('meongbun_labeled', 0)}")
            pr = a.get("principles_7_9", {})
            if pr:
                r2 = pr.get("rule2_chase", {})
                print(f"원칙2 추격: <{int(CHASE_MAX)}% {_fmt(r2.get('under_chase_max', {}))}")
                print(f"           ≥{int(CHASE_MAX)}% {_fmt(r2.get('over_chase_max', {}))}")
                for k, v in pr.get("rule3_circuit_breaker", {}).items():
                    print(f"원칙3 CB[{k}]: 발동 {v.get('days_triggered', 0)}일 · "
                          f"차단분 {_fmt(v)}")
                sc = pr.get("rule4b_sector_cooldown", {})
                print(f"원칙4b 섹터쿨다운: 발동 {sc.get('trigger_events', 0)}회 · 차단분 {_fmt(sc)}")
            print(f"PB-B 상한가D+1   전체: {_fmt(b['all'])}")
            print(f"                강도게이트: {_fmt(b['strength_gate'])}")
            print(f"                레짐게이트: {_fmt(b['regime_gate'])}")
            print("★ record-only·~20분 관측해상도(보수적 체결 가정)·flip은 사장님 결정")
    except Exception as e:  # noqa: BLE001
        print(f"[playbook] 치명 예외(무시): {e}")
    sys.exit(0)
