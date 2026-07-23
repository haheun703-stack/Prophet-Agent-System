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
  ★ 일일 건수 상한 = CAP_VIEW_N건 (7/23 사장님 확정 — 판정 기준으로 승격).
    근거: 무제한 합산은 사장님 영구 자금 룰로 실행 불가능(SAJANG.CASH_RESERVE_PCT
    30% 현금보유 + BUDGET_MODE split_cash / top_k). 7/22 실측에서 무제한 -244.15%p
    vs 상한 +3.34%p로 부호가 반대라(7/15도 반대) 지표 선택이 곧 판정 —
    실행 가능한 쪽을 기준으로 삼는다.
    ⚠ 상한값 5의 근거 정정(7/23 Tier1 M-2): 봇 설계 상한은 09:15 top_k 3 +
    14:50 top_k 2 = 5종목/일이지만, v2 신호는 pb.REFINED_CUTOFF("10:00:00") 하드
    필터라 14:50 트랜치를 채울 수 없다 → **v2만 놓고 보면 실행 가능 상한은 3**.
    5 유지 = 더 보수적(불리한) 쪽 선택이며(실측 cap3 -6.16%p < cap5 -10.78%p,
    즉 5가 성적이 더 나쁨 → 유리한 지표 고르기 아님), 3으로의 정정은 사장님 결정.

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
CAP_VIEW_N = 5             # ★일일 건수 상한 = S-1 판정 기준(7/23 사장님 확정). 근거는 docstring
MIN_JUDGE_N = 10           # 이 미만이면 '판정 유예' 표기 — 무표본(수집장애)과 '측정 0' 구분(7/23 M-3)
LEDGER_KEEP = 180          # 장부 보존 일수
BACKFILL_FROM = "20260713" # 첫 실전 가동일 — 7/10 사후 스모크(latency ~7h)는 장부 제외
# ticks 오염일 verdict(판정 증거 제외 대상) — 단일진실 pb.ticks_health와 동기(7/21 F-15/16)
TICKS_BAD = ("BROKEN", "TRUNCATED")
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
    """ticks 건강도 — 단일진실 pb.ticks_health 위임(7/21 F-15: 재구현 금지·정오죽음 축 포함)."""
    return pb.ticks_health(day)


def _atomic_write(path: Path, data) -> None:
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=1), encoding="utf-8")
    tmp.replace(path)


class LedgerLoadError(Exception):
    """장부 파일은 존재하는데 파싱/스키마 실패 — 이 상태로 저장하면 과거 전량 소실."""


def _load_ledger(strict: bool = False) -> dict:
    """장부 로드. strict=True(저장 경로)는 '파일 부재'와 '파싱 실패'를 구분한다.

    7/23 Tier1 M-1: 기존엔 파싱 실패도 조용히 {"days": {}}를 돌려줬고, save_day가
    그걸 받아 오늘 1일치만 남기고 **과거 전량을 경고 없이 덮어썼다**. 그러면 다음날
    스코어보드는 "유효 1일"이 되고 S-1은 그 1일로 판정된다 — H-1이 막으려던
    '증거 부재 → 조용한 0 → ❌미달 위장'과 정확히 같은 실패 모드이고, HISTORY_KEEP(90일)
    밖은 재정산조차 불가라 복구가 안 된다. 저장 경로에서만 예외로 승격해 쓰기를 막는다.
    """
    if not LEDGER_PATH.exists():
        return {"days": {}}            # 정상 초회(파일 없음) — 빈 장부로 시작 OK
    try:
        led = json.loads(LEDGER_PATH.read_text(encoding="utf-8"))
        if isinstance(led, dict) and isinstance(led.get("days"), dict):
            return led
        raise ValueError("스키마 불일치(days가 dict 아님)")
    except Exception as e:  # noqa: BLE001
        if strict:
            raise LedgerLoadError(str(e)) from e
        print(f"[v2_paper] ⚠ 장부 파싱 실패({e}) — 읽기 경로는 빈 장부로 진행(저장은 차단됨)")
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
        # 상한 집계는 **고정 키명 + 상한값 stamp**로 기록한다(7/23 Tier1 (d)·M-5).
        # 옛 방식 f"cap{N}_*"는 CAP_VIEW_N을 바꾸는 순간 장부에 cap5_*(과거)와
        # cap3_*(신규)가 혼재해, 사람이 JSON을 보거나 신규 도구가 cap5_를 grep하면
        # 과거일만 잡히고 신규일은 miss = H-1 재발. 코드가 읽지 않아도 통로는 남는다.
        "cap_sum_net": round(sum(t["net"] for t in cap), 2),
        "cap_wins": sum(1 for t in cap if t["net"] > 0),
        "cap_n": len(cap),
        "ticks_health": _ticks_health(day),   # BROKEN = 수집장애일(판정 증거 제외 대상)
        # 박제값의 vintage — 임계/비용 상수를 바꾸면 과거일은 소급되지 않으므로
        # 어느 세대 규약으로 계산된 값인지 남긴다(M-5·mixed-vintage 조용한 오독 차단).
        "params": {"cap_limit": CAP_VIEW_N, "cb_daily_pct": CB_DAILY_PCT,
                   "cost_rt": pb.COST_RT},
    }
    return {"date": day, "trades": trades, "skipped": skipped, "summary": summary,
            "settled_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}


def save_day(rec: dict) -> None:
    # strict — 파싱 실패 시 예외로 중단(과거 전량 소실 차단·7/23 Tier1 M-1).
    # 예외는 main 최상위에서 출력되고 그날 정산이 장부에 없으므로 ⑳ freshness가
    # ⑲-3 STALE로 적발한다(조용한 성공 위장 대신 표면화).
    led = _load_ledger(strict=True)
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
    vsum = 0.0                      # 유효일(verdict OK만)의 집계 — 판정은 이 숫자로
    vcap_n = vcap_w = 0
    vcap_sum = 0.0                  # ★유효일 × 상한 CAP_VIEW_N건 = S-1 판정 기준(7/23 사장님 확정)
    valid_days = unknown_days = 0   # OK일 수 / ticks_health 미기입(pre-7/20 미정산·B 누출 fix)
    torn_days = []                  # trades↔summary.n 불일치일(증거 훼손 — 판정 제외)
    for d in days.values():
        s = d.get("summary", {})
        verdict = (s.get("ticks_health") or {}).get("verdict")
        # ★M-2(7/23 Tier1): 판정선은 trades에, 참고선은 summary.n/sum_net에 의존한다.
        #   둘이 어긋나면(장부 슬리밍·부분 sync·수동 편집) 판정만 조용히 0이 되고
        #   참고선은 "304건 +15.29%p"를 그대로 출력해 자기모순 메시지가 된다.
        #   → 불일치일은 유효일에서 제외하고 표면화한다(H-1의 본질=증거 부재의 은폐 차단).
        if verdict == "OK" and len(d.get("trades") or []) != (s.get("n") or 0):
            torn_days.append(d.get("date", "?"))
            verdict = "TORN"        # OK도 BAD도 아닌 제3상태 → unknown으로 분류
        # ★유효일 = verdict가 명시적 OK인 날만. BROKEN/TRUNCATED(수집장애)와
        #   None(구 장부·재정산 필요)은 전부 제외 → 판정 증거 오염 차단(7/21 F-16·B).
        if verdict == "OK":
            valid_days += 1
            vn += s.get("n", 0)
            vwins += s.get("wins", 0)
            vsum += s.get("sum_net", 0) or 0
            # 상한 뷰도 반드시 '유효일만' 누적 — all-days로 두면 수집장애일 체결이
            # 판정 기준에 누출된다(7/21 F-16과 동일 오염 경로·승격과 함께 차단).
            # ★요약의 cap{N}_* 박제 키가 아니라 원본 trades에서 직접 계산한다(7/23 Tier1 H-1):
            #   박제 키를 쓰면 CAP_VIEW_N을 바꾸는 순간 과거 전 일자가 조회 miss→0이 되어
            #   스코어보드가 "0건 +0.00%p"로 조용히 붕괴하고, S-1이 ❌미달로 자동 평가된다
            #   (= 데이터 부재가 전략 폐기 근거로 위장 — 완료위장과 동일 계열).
            #   trades는 장부에 전량 보존(CB 필터 적용 후)되므로 상수 변경에 면역.
            _cap = (d.get("trades") or [])[:CAP_VIEW_N]
            vcap_n += len(_cap)
            vcap_w += sum(1 for t in _cap if (t.get("net") or 0) > 0)
            vcap_sum += sum((t.get("net") or 0) for t in _cap)
        elif verdict in TICKS_BAD:
            broken_days += 1        # 수집장애일(BROKEN·TRUNCATED)
        else:
            unknown_days += 1       # 미정산(--settle 재정산 필요)
        n += s.get("n", 0)
        wins += s.get("wins", 0)
        sum_net += s.get("sum_net", 0) or 0
        # all-days 상한 집계(참고 전용)도 동일하게 trades 직접 계산 — 파일 안에
        # 박제 키 의존을 하나도 남기지 않는다(H-1이 어느 줄에도 재발하지 않게).
        _acap = (d.get("trades") or [])[:CAP_VIEW_N]
        cap_sum += sum((t.get("net") or 0) for t in _acap)
        cap_n += len(_acap)
        cap_w += sum(1 for t in _acap if (t.get("net") or 0) > 0)
        cb_days += 1 if s.get("cb_triggered") else 0
    return {"days": len(days), "n": n, "wins": wins,
            "win_rate": round(100 * wins / n, 1) if n else None,
            "sum_net": round(sum_net, 2),
            "avg_net": round(sum_net / n, 3) if n else None,
            "krw": int(sum_net / 100 * NOTIONAL_KRW),
            # ⚠ cap_*(all-days)는 수집장애일 포함 = 참고 전용. 판정엔 절대 쓰지 말 것
            #    — 판정 기준은 아래 valid_cap_*(F-16 오염 경로와 동일한 함정).
            "cap_sum": round(cap_sum, 2), "cap_n": cap_n, "cap_w": cap_w,
            "cb_days": cb_days, "broken_days": broken_days, "unknown_days": unknown_days,
            "torn_days": torn_days,   # trades↔n 불일치일 — 발견 시 반드시 표면화(M-2)
            "valid_days": valid_days, "valid_n": vn, "valid_wins": vwins,
            "valid_sum": round(vsum, 2),
            "valid_win_rate": round(100 * vwins / vn, 1) if vn else None,
            # 반올림 後 %p에서 환산 — valid_cap_krw와 규약 통일(L-4·F-17 씨앗 제거)
            "valid_krw": int(round(vsum, 2) / 100 * NOTIONAL_KRW),
            # ★S-1 판정 기준(7/23 사장님 확정) = 유효일 × 상한 CAP_VIEW_N건
            # krw는 반드시 '반올림된 %p'에서 환산 — 부동소수 누적분이 남으면 데드라인
            # 체커(=round된 값 사용)와 원화가 1원 어긋난다(F-17 '두 도구 불일치' 재발 방지).
            "valid_cap_n": vcap_n, "valid_cap_wins": vcap_w,
            "valid_cap_sum": round(vcap_sum, 2),
            "valid_cap_win_rate": round(100 * vcap_w / vcap_n, 1) if vcap_n else None,
            "valid_cap_krw": int(round(vcap_sum, 2) / 100 * NOTIONAL_KRW)}


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
        if th.get("verdict") in TICKS_BAD:
            kind = "수집장애" if th.get("verdict") == "BROKEN" else f"오후결손(마감 {th.get('last_med')})"
            lines.append(f"{int(m)}/{int(dd)}({wd}) 🚨 ticks {kind} "
                         f"(사용가능 {th.get('usable_pct')}%) — 체결 {s['n']}건은 "
                         f"'신호 없음'이 아니라 데이터 결손·판정 표본 제외 대상")
        elif th.get("verdict") != "OK":
            # M-3(7/23 Tier1): verdict None(구 장부·ticks_health 예외)은 TICKS_BAD에
            # 안 걸려 정상 분기로 새고, 그런데 _cum은 unknown으로 헤드라인에서 제외한다
            # → "방금 정산했는데 미정산" 자기모순 메시지. 정상 분기는 OK일 때만 진입.
            lines.append(f"{int(m)}/{int(dd)}({wd}) ⚠ ticks 건강도 미확인 — 체결 {s['n']}건, "
                         f"판정 표본 제외(--settle 재정산 필요)")
        else:
            # 일일 표기도 상한 기준을 주 수치로 — 헤드라인(누적)과 단위를 맞춘다.
            # 전 신호 합산은 실행 불가능 수치라 괄호 참고로만(7/23 사장님 확정).
            # _cum과 동일하게 원본 trades에서 직접 계산 — 박제 키(cap{N}_*)를 쓰면
            # CAP_VIEW_N 변경 시 이 줄만 0건으로 나와 헤드라인과 모순된 메시지가
            # 사장님께 발송된다(H-1을 _cum에서만 막으면 여기가 남는다·7/23 재점검 적발).
            _dcap = (d.get("trades") or [])[:CAP_VIEW_N]
            cn = len(_dcap)
            cs = round(sum((t.get("net") or 0) for t in _dcap), 2)   # 반올림 後 환산(L-4 규약)
            cw = sum(1 for t in _dcap if (t.get("net") or 0) > 0)
            cap_krw = int(cs / 100 * NOTIONAL_KRW)
            lines.append(f"{int(m)}/{int(dd)}({wd}) 체결 {cn}건(상한) · 승 {cw} · "
                         f"순합 {cs:+.2f}%p ({cap_krw:+,}원)")
            # M-4(7/23 Tier1): CB 배지를 상한 수치 옆에 붙이면 "안전핀이 저 수치를
            # 지켰다"로 오독된다. 실측상 CB 트리거(exit_time 기준·09:47~10:45)는 상한
            # 5건의 진입(09:21~09:41)보다 늦어 4개 발동일 전부 상한 수치에 영향 0.
            # CB는 '전 신호' 기준 리허설이므로 참고선에 붙인다(정합은 F-19).
            lines.append(f"  (참고·전 신호 {s['n']}건 합산 {s['sum_net']:+.2f}%p "
                         f"({day_krw:+,}원) — 자금룰상 실행 불가"
                         + (" · 🚨CB -6%p 발동(전 신호 기준·상한 수치엔 미적용)"
                            if s["cb_triggered"] else "") + ")")
    else:
        lines.append(f"{int(m)}/{int(dd)}({wd}) 정산 없음 (신호 0/휴장)")
    # ★ 헤드라인 = 유효일(ticks OK) × 일일 상한 CAP_VIEW_N건 = S-1 판정 숫자.
    #   7/17 "첫 줄=숫자" + 7/21 F-16/17(유효일만) + 7/23 사장님 확정(상한 승격)의 합.
    #   전 신호 합산은 자금룰상 실행 불가능(30% 현금보유·split_cash top_k)이라 참고로 강등.
    # 문구 주의(L-2): '유효N일 × 상한M' 곱셈으로 읽히면 신호 5건 미만인 유효일이
    # 생기는 순간 산수가 안 맞는다("숫자가 틀렸다"로 보임) → '적용'으로 표기.
    lines.append(f"★유효 {c['valid_days']}일 · 상한 {CAP_VIEW_N}건/일 적용 {c['valid_cap_n']}건 · "
                 f"승률 {c['valid_cap_win_rate'] or 0}% · 순누적 {c['valid_cap_sum']:+.2f}%p "
                 f"({c['valid_cap_krw']:+,}원) ← S-1 판정 기준")
    # 무표본(수집장애 연속)과 '측정했는데 0'을 구분 — 전자를 전략 실패로 오판하지 않게(M-3)
    if c["valid_cap_n"] < MIN_JUDGE_N:
        lines.append(f"  ⚠ 표본 {c['valid_cap_n']}건 < {MIN_JUDGE_N}건 — 판정 유예 권고"
                     f"(수집장애로 유효일이 줄면 '무표본'이 ❌미달로 위장됨)")
    lines.append(f"  (참고·실행 불가 수치 — 전 신호 합산 {c['valid_n']}건 {c['valid_sum']:+.2f}%p)")
    # M-2 표면화: 증거 훼손일은 조용히 판정에서 빠지면 안 된다(H-1 본질과 동일)
    if c["torn_days"]:
        lines.append(f"  🚨 증거 훼손 {len(c['torn_days'])}일 판정 제외 "
                     f"({', '.join(c['torn_days'][:5])}) — trades↔n 불일치·재정산 필요")
    if c["broken_days"] or c["unknown_days"]:
        extra = f"·미정산 {c['unknown_days']}일(재정산 필요)" if c["unknown_days"] else ""
        lines.append(f"  (수집장애 {c['broken_days']}일 제외{extra} · 전체 {c['days']}일 기록 보유)")
    if c["cb_days"]:
        lines.append(f"CB 발동일 누적 {c['cb_days']}일")
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
