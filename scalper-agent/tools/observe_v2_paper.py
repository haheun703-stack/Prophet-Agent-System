# -*- coding: utf-8 -*-
"""PB-A v2 페이퍼트레이딩 장부 — 승률·수익금 누적 (7/16 사장님 지시).

배경: 7/16 사장님 "페이퍼트레이딩으로 진행해서 일단 승률 및 수익금부터 올리고
난 다음에 (라이브) 해야돼" — 소액 실탄 보류. OBSERVE v2 러너가 장중 실시간으로
포착한 intent(observe_v2_intents/history)를 저녁 최종 ticks로 페이퍼 체결·청산해
승률/수익금을 매일 누적하고 텔레그램으로 보고한다. 라이브 전환 판정의 증거 장부.

체결 규약 (⑲ replay와 동일 — playbook_shadow 단일진실):
  진입 = intent signal_time **다음** 관측행 가격(보수·~20분 해상도)
  청산 = pb._simulate_exit (SL -3% 우선 → TP +5% → EOD 마지막 관측가)
  비용 = 왕복 pb.COST_RT(0.2%)/건 차감한 net 기준으로 승패·누적 집계.
  intent 종목이 최종 ticks에 없거나 진입행이 없으면 체결 불성립(스킵 기록) —
  실시간이 본 것만 체결한다(전종목 replay ⑲와 다른 점 = "라이브였다면"의 정직한 재현).

안전핀 리허설 (7/10 사전합의 — 라이브 전 페이퍼에서 동일 적용):
  일일 CB: pb._cb_sim(loss 모드)과 동일 규약 — **실현(exit_time) 순 gross 누적**이
  CB_DAILY_PCT(-6.0%p) 도달한 시각 이후의 진입만 차단(look-ahead 금지 —
  신호시각 순 최종손익 누적은 미실현 손실을 미리 아는 격이라 오차단).
  추격<8%·오전10시前·체결강도200+ = v2 신호 정의에 내장(러너/pb 단일진실).
  섹터 쿨다운은 후속(DAILY_ROUTINE §4 각주 — 섹터맵 결합 필요).
  일일 건수 상한은 미확정(사장님 결정 보류) — CAP_VIEW_N건 시나리오를 병행 집계만.

★★★ 안전 불변식 ★★★
- 실주문 0. 주문/계좌 API 0접촉 — 페이퍼 장부(JSON) 기록과 텔레그램 보고뿐.
- 읽기: observe_v2_intents/history·ticks / 쓰기: observe_v2_paper_ledger.json(원자쓰기).
- 매수/매도/picks/SAJANG/order_intent 0접촉. 관측 없이 flip 금지 — 라이브는 사장님 결정.

실행: python tools/observe_v2_paper.py --daily [--no-tg]      # nightly ⑲-3 (정산+보고)
      python tools/observe_v2_paper.py --settle --day YYYYMMDD
      python tools/observe_v2_paper.py --backfill              # history 7/13~ 미정산분
      python tools/observe_v2_paper.py --report [--no-tg]
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

import playbook_shadow as pb  # noqa: E402 — ticks 파서·체결/청산·비용 상수 단일진실

DS = BASE / "data_store"
INTENTS_PATH = DS / "observe_v2_intents.json"
HISTORY_PATH = DS / "observe_v2_history.json"
LEDGER_PATH = DS / "observe_v2_paper_ledger.json"

NOTIONAL_KRW = 300_000     # 수익금 환산 명목(종목당) — trade_runtime_config live_limits 기본 단위
CB_DAILY_PCT = pb.CB_DAILY_LIMITS[0]   # -6.0 — 7/10 사전합의 안전핀(pb 단일진실·principles_7_9 검증값)
CAP_VIEW_N = 5             # 일일 건수 상한 '시나리오 뷰' (실제 상한은 사장님 결정 보류)
LEDGER_KEEP = 180          # 장부 보존 일수
# ticks 건강도 (7/20 수집장애 사고 fix) — 청산 시뮬은 신호행 '다음' 관측행이 있어야 성립.
# 수집이 죽은 날은 대부분 파일이 2행 이하로 끝나 체결 0건이 되는데, 그걸 '신호 없던 날'과
# 구분하지 않으면 판정 증거(승률·수익금)가 조용히 오염된다(7/13·7/20 실측 유효행 8%).
TICKS_MIN_ROWS = 3
TICKS_HEALTH_SAMPLE = 40
TICKS_BROKEN_PCT = 50.0
TICKS_MIN_FILES = 100      # 유니버스 ~2500 대비 극소 = 전면 장애(파일 미생성 위장 차단)
BACKFILL_FROM = "20260713" # 첫 실전 가동일 — 7/10 사후 스모크(latency ~7h)는 장부 제외
_WD = "월화수목금토일"


def _is_trading_yyyymmdd(day: str) -> bool:
    """YYYYMMDD가 거래일인지 — 휴장일 스테일 intents/ticks의 장부 유입 차단(7/17 Tier1 Med fix).

    --daily뿐 아니라 --settle/--backfill 수동 경로도 대칭 방어(⑲ replay 필터와 동일 원리).
    파싱/캘린더 실패 시 True(기존 동작 보수 유지)."""
    try:
        from data.trading_calendar import is_trading_day
        return is_trading_day(date(int(day[:4]), int(day[4:6]), int(day[6:8])))
    except Exception:  # noqa: BLE001
        return True


def _ticks_health(day: str):
    """그날 ticks가 체결/청산 시뮬에 쓸 수 있는 상태인지 — 결정적 표본(정렬 후 균등 추출).

    verdict=BROKEN = 수집 장애일(판정 증거로 쓰면 안 되는 날). 실패 시 None(판정 보류)."""
    try:
        d = pb.TICKS / day
        files = sorted(d.glob("*.csv")) if d.exists() else []
        # 파일 자체가 없거나 극소 = 1번째 사이클부터 죽은 전면 장애. None(판정보류)로 두면
        # broken_days에 안 잡혀 "정상 0건 날"로 위장된다 — 게다가 price=0 행 미기록 fix로
        # 파일 미생성 확률이 올라갔다(7/20 Tier1 M-2).
        if len(files) < TICKS_MIN_FILES:
            return {"sample": len(files), "usable_pct": 0.0, "verdict": "BROKEN",
                    "reason": "파일 미생성/극소 (전면 수집장애)"}
        step = max(1, len(files) // TICKS_HEALTH_SAMPLE)
        sample = files[::step][:TICKS_HEALTH_SAMPLE]
        usable = sum(1 for f in sample if len(pb._read_ticks(day, f.stem)) >= TICKS_MIN_ROWS)
        pct = round(100 * usable / len(sample), 1)
        return {"sample": len(sample), "usable_pct": pct,
                "verdict": "OK" if pct >= TICKS_BROKEN_PCT else "BROKEN"}
    except Exception:  # noqa: BLE001
        return None


def _atomic_write(path: Path, data) -> None:
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=1), encoding="utf-8")
    tmp.replace(path)


def _load_ledger() -> dict:
    try:
        led = json.loads(LEDGER_PATH.read_text(encoding="utf-8"))
        if isinstance(led, dict) and isinstance(led.get("days"), dict):
            return led
    except Exception:  # noqa: BLE001
        pass
    return {"days": {}}


def _intents_for(day: str):
    """day(YYYYMMDD)의 장중 intent {code: rec}. 오늘 파일 우선, 과거는 history.

    반환: dict(빈 dict = 러너는 돌았으나 신호 0 — n=0 정산 기록 대상) /
          None(해당 날 기록 자체 없음 — 가동 전·cron 미동작 → 정산 skip).
    구분 이유: 신호 0인 날을 skip하면 ⑳ freshness가 ⑲-3 STALE 오탐(compare EMPTY와 동일 원리)."""
    try:
        state = json.loads(INTENTS_PATH.read_text(encoding="utf-8"))
        if state.get("date") == day:
            return state.get("intents", {}) or {}
    except Exception:  # noqa: BLE001
        pass
    try:
        hist = json.loads(HISTORY_PATH.read_text(encoding="utf-8"))
        for h in hist:
            if h.get("date") == day:
                return h.get("intents", {}) or {}
    except Exception:  # noqa: BLE001
        pass
    return None


def settle_day(day: str, intents: dict) -> dict:
    """day 하루 페이퍼 정산 — intent를 최종 ticks로 체결·청산. 시간순·CB 리허설."""
    recs = sorted(intents.values(), key=lambda r: (r.get("signal_time") or "", r.get("code") or ""))
    fills, skipped = [], []
    for r in recs:
        code = r.get("code") or ""
        sig_t = r.get("signal_time") or ""
        rows = pb._read_ticks(day, code)
        ei = next((i for i, row in enumerate(rows) if row[0] > sig_t), None)
        if not rows or ei is None:
            skipped.append({"code": code, "reason": "no_fill"})   # phantom/진입행 없음
            continue
        entry = rows[ei][1]
        ret, why, exit_t = pb._simulate_exit(rows, ei, entry)
        if ret is None:
            skipped.append({"code": code, "reason": "no_fill"})
            continue
        fills.append({"code": code, "signal_time": sig_t,
                      "entry_time": rows[ei][0], "entry_price": entry,
                      "exit_time": exit_t, "why": why,
                      "ret": round(ret, 2), "net": round(ret - pb.COST_RT, 2),
                      "detected_at": r.get("detected_at")})
    # 일일 CB — pb._cb_sim(loss 모드) 동일 규약: 실현(exit_time) 순 gross 누적 ≤ 임계
    # 도달 '시각' 이후의 진입만 차단. (신호순 최종손익 누적 = look-ahead 오차단 — 7/16 검수 fix)
    trig, cum = None, 0.0
    for t in sorted((f for f in fills if f.get("exit_time")), key=lambda x: x["exit_time"]):
        cum += t["ret"]
        if cum <= CB_DAILY_PCT:
            trig = t["exit_time"]
            break
    trades = [f for f in fills if trig is None or f["entry_time"] <= trig]
    skipped += [{"code": f["code"], "reason": "cb"}
                for f in fills if trig is not None and f["entry_time"] > trig]
    cb_hit = trig is not None
    wins = sum(1 for t in trades if t["net"] > 0)
    cap = trades[:CAP_VIEW_N]
    summary = {
        "n": len(trades), "wins": wins,
        "win_rate": round(100 * wins / len(trades), 1) if trades else None,
        "sum_net": round(sum(t["net"] for t in trades), 2),
        "avg_net": round(sum(t["net"] for t in trades) / len(trades), 3) if trades else None,
        "cb_triggered": cb_hit,
        "cb_trigger_time": trig,
        "skipped_cb": sum(1 for s in skipped if s["reason"] == "cb"),
        "skipped_no_fill": sum(1 for s in skipped if s["reason"] == "no_fill"),
        f"cap{CAP_VIEW_N}_sum_net": round(sum(t["net"] for t in cap), 2),
        f"cap{CAP_VIEW_N}_wins": sum(1 for t in cap if t["net"] > 0),
        f"cap{CAP_VIEW_N}_n": len(cap),
        "ticks_health": _ticks_health(day),   # BROKEN = 수집장애일(판정 증거 제외 대상)
    }
    return {"date": day, "trades": trades, "skipped": skipped, "summary": summary,
            "settled_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}


def save_day(rec: dict) -> None:
    led = _load_ledger()
    led["days"][rec["date"]] = rec
    days = sorted(led["days"])[-LEDGER_KEEP:]
    led["days"] = {d: led["days"][d] for d in days}
    led["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    led["note"] = ("PB-A v2 페이퍼 장부 — 실주문 0. 체결규약=⑲ replay 동일(진입=신호 다음 관측행·"
                   "SL우선·net=왕복 0.2% 차감). CB -6%p 리허설 포함. 라이브 전환은 사장님 결정.")
    _atomic_write(LEDGER_PATH, led)


def _cum(led: dict) -> dict:
    days = led.get("days", {})
    n = wins = 0
    sum_net = cap_sum = 0.0
    cap_n = cap_w = cb_days = broken_days = 0
    vn = vwins = 0
    vsum = 0.0                      # 유효일(수집 정상)만의 집계 — 판정은 이 숫자로
    for d in days.values():
        s = d.get("summary", {})
        broken = (s.get("ticks_health") or {}).get("verdict") == "BROKEN"
        if broken:
            broken_days += 1        # 수집장애일 — 판정 시 표본에서 제외해야 하는 날
        else:
            vn += s.get("n", 0)
            vwins += s.get("wins", 0)
            vsum += s.get("sum_net", 0) or 0
        n += s.get("n", 0)
        wins += s.get("wins", 0)
        sum_net += s.get("sum_net", 0) or 0
        cap_sum += s.get(f"cap{CAP_VIEW_N}_sum_net", 0) or 0
        cap_n += s.get(f"cap{CAP_VIEW_N}_n", 0)
        cap_w += s.get(f"cap{CAP_VIEW_N}_wins", 0)
        cb_days += 1 if s.get("cb_triggered") else 0
    return {"days": len(days), "n": n, "wins": wins,
            "win_rate": round(100 * wins / n, 1) if n else None,
            "sum_net": round(sum_net, 2),
            "avg_net": round(sum_net / n, 3) if n else None,
            "krw": int(sum_net / 100 * NOTIONAL_KRW),
            "cap_sum": round(cap_sum, 2), "cap_n": cap_n, "cap_w": cap_w,
            "cb_days": cb_days, "broken_days": broken_days,
            "valid_days": len(days) - broken_days, "valid_n": vn, "valid_wins": vwins,
            "valid_sum": round(vsum, 2),
            "valid_win_rate": round(100 * vwins / vn, 1) if vn else None,
            "valid_krw": int(vsum / 100 * NOTIONAL_KRW)}


def build_report(day: str) -> str:
    led = _load_ledger()
    c = _cum(led)
    d = led.get("days", {}).get(day)
    y, m, dd = day[:4], day[4:6], day[6:]
    wd = _WD[date(int(y), int(m), int(dd)).weekday()]
    lines = [f"📈 PB-A v2 페이퍼 성과 (실주문 0·관측)"]
    if d:
        s = d["summary"]
        day_krw = int((s.get("sum_net") or 0) / 100 * NOTIONAL_KRW)
        th = s.get("ticks_health") or {}
        if th.get("verdict") == "BROKEN":
            lines.append(f"{int(m)}/{int(dd)}({wd}) 🚨 ticks 수집장애 "
                         f"(사용가능 {th.get('usable_pct')}%) — 체결 {s['n']}건은 "
                         f"'신호 없음'이 아니라 데이터 결손·판정 표본 제외 대상")
        else:
            lines.append(f"{int(m)}/{int(dd)}({wd}) 체결 {s['n']}건 · 승 {s['wins']}"
                         f"({s['win_rate'] or 0}%) · 순합 {s['sum_net']:+.2f}%p ({day_krw:+,}원)"
                         + (" · 🚨CB -6%p 발동" if s["cb_triggered"] else ""))
    else:
        lines.append(f"{int(m)}/{int(dd)}({wd}) 정산 없음 (신호 0/휴장)")
    lines.append(f"누적 {c['days']}일 {c['n']}건 · 승률 {c['win_rate'] or 0}% · "
                 f"평균 {c['avg_net'] or 0:+.3f}%p/건 · 순누적 {c['sum_net']:+.2f}%p")
    lines.append(f"30만원/건 환산 누적 {c['krw']:+,}원 (왕복비용 0.2% 차감)")
    cap_wr = round(100 * c["cap_w"] / c["cap_n"], 1) if c["cap_n"] else 0
    lines.append(f"[상한 {CAP_VIEW_N}건/일 뷰] {c['cap_n']}건 승률 {cap_wr}% 누적 {c['cap_sum']:+.2f}%p")
    if c["cb_days"]:
        lines.append(f"CB 발동일 누적 {c['cb_days']}일")
    if c["broken_days"]:
        lines.append(f"⚠ ticks 수집장애일 {c['broken_days']}일 제외 — "
                     f"★유효 {c['valid_days']}일 {c['valid_n']}건 · 승률 {c['valid_win_rate'] or 0}% · "
                     f"순 {c['valid_sum']:+.2f}%p ({c['valid_krw']:+,}원) ← S-1 판정 기준")
    lines.append("라이브 전환은 사장님 결정 — 관측 없이 flip 금지")
    return "\n".join(lines)


def _send(msg: str, no_tg: bool) -> None:
    if sys.platform == "win32":   # 노트북=분석 전용 — 오알림 금지 (러너와 동일 가드)
        no_tg = True
    if no_tg:
        print("[v2_paper] --no-tg — 알림 생략")
        return
    try:
        from verifiers._common import send_telegram
        if not send_telegram(msg):   # 반환 False = silent 실패 — 가시화(7/14 M-1 원리·7/17 fix)
            print("[v2_paper] 텔레그램 발송 실패 — 사장님 미수신 가능(토큰/네트워크 확인)")
    except Exception as e:  # noqa: BLE001
        print(f"[v2_paper] 텔레그램 실패(장부는 완료): {e}")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--daily", action="store_true", help="정산+보고 (nightly ⑲-3)")
    ap.add_argument("--settle", action="store_true")
    ap.add_argument("--report", action="store_true")
    ap.add_argument("--backfill", action="store_true", help=f"history {BACKFILL_FROM}~ 미정산분")
    ap.add_argument("--day", default=None, help="YYYYMMDD (기본 오늘)")
    ap.add_argument("--no-tg", action="store_true")
    args = ap.parse_args()
    today = date.today().strftime("%Y%m%d")
    day = args.day or today

    if args.daily and not args.day:
        from data.trading_calendar import is_trading_day
        if not is_trading_day(date.today()):
            print("[v2_paper] 휴장일 — skip")
            return 0

    if args.backfill:
        led = _load_ledger()
        try:
            hist = json.loads(HISTORY_PATH.read_text(encoding="utf-8"))
        except Exception:  # noqa: BLE001
            hist = []
        for h in hist:
            hd = h.get("date", "")
            if not _is_trading_yyyymmdd(hd):
                print(f"[v2_paper] {hd} 휴장일(스테일 의심) — 백필 제외")
                continue
            if hd >= BACKFILL_FROM and hd not in led.get("days", {}):
                rec = settle_day(hd, h.get("intents", {}) or {})
                save_day(rec)
                s = rec["summary"]
                print(f"[v2_paper] {hd} 백필 — {s['n']}건 승률 {s['win_rate']} "
                      f"순합 {s['sum_net']:+.2f}%p cb={s['cb_triggered']}")
        return 0

    if args.settle or args.daily:
        if not _is_trading_yyyymmdd(day):
            print(f"[v2_paper] {day} 휴장일 — 정산 제외(스테일 유입 차단)")
            return 0
        if not (pb.TICKS / day).exists():
            print(f"[v2_paper] ticks/{day} 없음 — 정산 불가·skip")
            return 0
        ints = _intents_for(day)
        if ints is None:
            print(f"[v2_paper] {day} intent 기록 없음 (가동 전/cron 미동작) — skip")
            return 0
        rec = settle_day(day, ints)
        save_day(rec)
        s = rec["summary"]
        print(f"[v2_paper] {day} 정산 — {s['n']}건 승률 {s['win_rate']} "
              f"순합 {s['sum_net']:+.2f}%p cb={s['cb_triggered']} "
              f"(skip cb {s['skipped_cb']}/no_fill {s['skipped_no_fill']}) · 실주문 0")

    if args.report or args.daily:
        msg = build_report(day)
        print(msg)
        _send(msg, args.no_tg)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:  # noqa: BLE001 — record-only 도구: nightly 차단 금지(신선도는 ⑳이 적발)
        print(f"[v2_paper] 치명 예외(관측 도구 — 무시): {e}")
        sys.exit(0)
