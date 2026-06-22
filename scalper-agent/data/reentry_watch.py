# -*- coding: utf-8 -*-
"""재진입 라이브 감시 원장 (6/22 구현, flag OFF·paper-first).

설계: docs/02-design/reentry_rule_5_31.md  (§2.1 트리거체인 / §4 통합위치 / §7 사장님 결정)
shadow 검증: data/reentry_shadow.py (lift +6.33%p 입증, 65표본). 본 모듈은 그 룰을 라이브 경로에 배선.

★★★ 안전 불변식 ★★★
- 매도 무접촉: 손절 매도 코드를 전혀 수정하지 않는다. trade_journal.register_trade_callback 로
  '이미 체결·적재된' 매도 이벤트를 read-only 구독해 감시 원장만 채운다.
  (5/19 사고 후 프로젝트 불변식 = 모든 매도는 log_sell 로 적재 → 콜백이 매도 이벤트를 빠짐없이 수신.)
  단 '재진입 대상'은 -3% 흔들기 손절만 → note 토큰으로 정밀 필터(_STOP_NOTES). TP·모멘텀이탈·갭다운·hard_kill 제외.
- 실주문 게이트: SAJANG.REENTRY_LIVE=False(기본)면 재진입 판정은 하되, 실제 매수 대신
  관측 intent(allowed=False)만 기록한다(실주문 0). 라이브 매수는 auto_trader.reentry_buy_one 이 담당.
- SAJANG 단일진실: 재진입 자격/윈도우/손절선은 SAJANG.should_reenter / effective_hold_days 만 사용.
  리터럴 곱셈(0.97 등) 0건. reclaim 판정은 곱셈 없는 'close >= last_stop' (설계 §2.2 = 버퍼 없음).
- 멱등: code 키 단일 활성 감시(설계 §4 reentry_watch[code]). 콜백 중복/재실행해도 원장 1건.
- 콜백은 절대 raise 안 함(매매 흐름 보호). 네트워크 0(로컬 json 만 읽고 쓴다).

★ 한계(정직, 설계 §5 라이브숙제 · §8 의존):
- kki_grade: 4단 Stage1 끼가 라이브 '선정'에 미배선 → early_variant_shadow.json best-effort 조회,
  없으면 'UNKNOWN' → should_reenter False(보수적: 끼 모르는 종목 재추격 안 함). 4단 끼 라이브 배선 선결.
- thesis_ok: Stage3 invalidation 신호 미배선 → True 가정. 라이브 flip 전 명분붕괴 차단 추가 필요(§7.4).
- 윈도우 anchor: 콜백은 원진입일이 아닌 '손절일'만 안다 → 윈도우를 stop_date 기준으로 잰다(설계는 원진입 기준).
  손절은 보통 진입 직후라 근사 타당하나, 정밀화는 §5 paper 숙제.
"""
from __future__ import annotations

import json
import logging
from datetime import date, datetime
from pathlib import Path
from typing import Callable, Optional

from data.sajang_rules import SAJANG
from data.trading_calendar import get_trading_days_between

logger = logging.getLogger("BH.ReentryWatch")

SCALPER_ROOT = Path(__file__).resolve().parents[1]
WATCH_OUT = SCALPER_ROOT / "data_store" / "reentry_watch_live.json"
EARLY_SHADOW = SCALPER_ROOT / "data_store" / "early_variant_shadow.json"

# 재진입 트리거 = SAJANG -3% '흔들기' 손절(진입가 안전망 + 고점 트레일링)만.
# trade_journal callback note 토큰(auto_trader 전수 확인 6/22):
#   "stop_loss"     = _job_monitor 진입가 -3% 안전망 (auto_trader.py:4521)
#   "trailing_stop" = _job_monitor 고점 -3% 트레일링 (auto_trader.py:4521, trailing_activated)
#   "dynamic_sl"    = daily 재평가 ACTION_STOP_LOSS (auto_trader.py:4901, target_state.dynamic_sl=SAJANG -3% 라인)
# ★ 제외(shake-out 아님): take_profit / dynamic_sell(모멘텀이탈) / sell_jarvis_weak / 검증강제청산 /
#   hard_kill(재앙손절) / pending_sells(룰C 갭다운) / sync_remove. 이들 note엔 위 토큰 미포함 확인.
#   ("dynamic_sell"에 "dynamic_sl" 부분문자열 없음 — 'dynamic_se'에서 분기, 오포함 0.)
_STOP_NOTES = ("stop_loss", "trailing_stop", "dynamic_sl")

# 감시 상태
WATCHING = "WATCHING"     # 손절 후 reclaim 대기
ACTED = "ACTED"           # 이번 사이클 재진입(라이브) 또는 관측 intent 발생 → 재발 방지
EXPIRED = "EXPIRED"       # 윈도우 종료(재진입 없음)


# ───────────────────────── 저장 (멱등, 로컬 json) ─────────────────────────
def _load(path: Optional[Path] = None) -> dict:
    path = path or WATCH_OUT
    if path.exists():
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(data, dict) and isinstance(data.get("watches"), dict):
                return data
        except Exception:
            pass
    return {"watches": {}, "note": "재진입 라이브 감시 원장(매도 무접촉 콜백 구독·실주문은 REENTRY_LIVE 게이트)"}


def _save(data: dict, path: Optional[Path] = None) -> None:
    path = path or WATCH_OUT
    data["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def _lookup_kki_grade(code: str, early_path: Optional[Path] = None) -> str:
    """4단 Stage1 끼 등급 best-effort 조회 (라이브 선정 미배선 → shadow 기록에서 최신 매칭).

    없으면 'UNKNOWN' → should_reenter 가 rank 0 < MODERATE 로 차단(보수적).
    """
    early_path = early_path or EARLY_SHADOW
    if not early_path.exists():
        return "UNKNOWN"
    try:
        recs = json.loads(early_path.read_text(encoding="utf-8")).get("records", [])
    except Exception:
        return "UNKNOWN"
    best = None
    for r in recs:
        if str(r.get("code")) == str(code) and r.get("kki_grade"):
            # 최신 date 우선
            if best is None or str(r.get("date", "")) >= str(best.get("date", "")):
                best = r
    return str(best.get("kki_grade")) if best else "UNKNOWN"


def _day_offset(stop_date_str: str, today: date) -> int:
    """손절일 → today 까지의 거래일 offset (shadow 의 r-di 와 동일 의미, stop_date anchor).

    같은 날(손절 당일 종가) = 0. 다음 거래일 = 1 ...
    """
    try:
        sd = datetime.strptime(stop_date_str, "%Y-%m-%d").date()
    except Exception:
        return 0
    if today < sd:
        return 0
    days = get_trading_days_between(sd, today)
    return max(0, len(days) - 1)


# ───────────────────────── 등록 (콜백, 매도 무접촉·raise 금지) ─────────────────────────
def register_stop(code: str, name: str, entry_price: float, stop_price: float,
                  stop_date: str, kki_grade: Optional[str] = None,
                  path: Optional[Path] = None) -> bool:
    """손절 1건을 감시 원장에 등록/갱신 (멱등, code 단일 활성).

    이미 WATCHING/ACTED 인 code 면 last_stop_price/stop_date 갱신 + WATCHING 재무장
    (재진입 레그가 또 손절된 경우). re_count 는 보존(mark_acted 에서만 증가).
    반환: True=기록됨.
    """
    path = path or WATCH_OUT
    code = str(code).zfill(6)
    if entry_price is None or stop_price is None or stop_price <= 0:
        return False
    data = _load(path)
    watches = data["watches"]
    grade = kki_grade if kki_grade else _lookup_kki_grade(code)
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    prev = watches.get(code)
    if prev and prev.get("status") in (WATCHING, ACTED):
        prev["last_stop_price"] = int(stop_price)
        prev["stop_date"] = stop_date
        prev["status"] = WATCHING
        prev["kki_grade"] = grade
        prev["updated_at"] = now
    else:
        watches[code] = {
            "code": code, "name": name or code,
            "entry_price": int(entry_price) if entry_price else 0,
            "last_stop_price": int(stop_price),
            "stop_date": stop_date,
            "re_count": 0,
            "kki_grade": grade,
            "status": WATCHING,
            "created_at": now, "updated_at": now,
        }
    _save(data, path)
    logger.info(f"[reentry_watch] 등록 {name}({code}) stop={int(stop_price):,} grade={grade} date={stop_date}")
    return True


def on_trade_event(side: str, code: str, name: str, qty: int, price: float,
                   source: str = "", entry_price: float = 0, note: str = "") -> None:
    """trade_journal callback 어댑터 — 매도(손절/트레일링) 이벤트만 감시 원장에 등록.

    ★ 절대 raise 안 함 (매매 흐름 보호). 매도 코드 무접촉 (이미 체결·적재된 이벤트만 read).
    note 예: "sell_sl stop_loss" / "sell_close trailing_stop" (log_sell 이 event_type+note 합성).
    """
    try:
        if str(side).upper() != "SELL":
            return
        note_l = str(note or "").lower()
        if not any(tag in note_l for tag in _STOP_NOTES):
            return  # TP·룰B/C/D·EOD 마감청산 등은 재진입 대상 아님
        stop_date = datetime.now().strftime("%Y-%m-%d")
        register_stop(code=code, name=name, entry_price=entry_price,
                      stop_price=price, stop_date=stop_date)
    except Exception as e:  # noqa: BLE001 — 콜백은 매매 흐름 보호 위해 절대 전파 금지
        logger.warning(f"[reentry_watch] on_trade_event 무시(매매 흐름 보호): {type(e).__name__}: {e}")


# ───────────────────────── 평가 (순수 판정 — 주문 0, 윈도우 housekeeping) ─────────────────────────
def evaluate_watch(price_fn: Callable[[str], Optional[float]],
                   today: Optional[date] = None,
                   path: Optional[Path] = None) -> list:
    """감시 원장 평가 → 재진입 결정 리스트 반환 (이 함수는 주문 0 / EXPIRE housekeeping 만 persist).

    각 결정: {code, name, action, close, last_stop, day_offset, window, re_count, kki_grade, reason}
      action ∈ {REENTER, WAIT, EXPIRE}.
    REENTER 의 실제 매수/관측은 호출자(_job_reentry_check)가 SAJANG.REENTRY_LIVE 게이트로 분기.
    """
    path = path or WATCH_OUT
    today = today or datetime.now().date()
    data = _load(path)
    watches = data["watches"]
    decisions = []
    dirty = False
    for code, w in watches.items():
        if w.get("status") != WATCHING:
            continue
        grade = w.get("kki_grade", "UNKNOWN")
        re_count = int(w.get("re_count", 0))
        last_stop = float(w.get("last_stop_price", 0) or 0)
        window = SAJANG.effective_hold_days(grade)
        day_offset = _day_offset(w.get("stop_date", ""), today)
        if day_offset > window:
            w["status"] = EXPIRED
            w["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            dirty = True
            decisions.append({"code": code, "name": w.get("name", code), "action": "EXPIRE",
                              "day_offset": day_offset, "window": window, "kki_grade": grade,
                              "reason": "윈도우 종료(재진입 없음)"})
            continue
        close = price_fn(code)
        if close is None or close <= 0:
            decisions.append({"code": code, "name": w.get("name", code), "action": "WAIT",
                              "reason": "종가 조회 실패", "day_offset": day_offset, "window": window})
            continue
        should = SAJANG.should_reenter(float(close), last_stop, grade, re_count,
                                       day_offset, thesis_ok=True)
        base = {"code": code, "name": w.get("name", code), "close": int(close),
                "last_stop": int(last_stop), "day_offset": day_offset, "window": window,
                "re_count": re_count, "kki_grade": grade}
        if should:
            base["action"] = "REENTER"
            base["reason"] = f"reclaim(종가 {int(close):,} ≥ 손절선 {int(last_stop):,}) + 자격 통과"
        else:
            base["action"] = "WAIT"
            base["reason"] = _why_not(float(close), last_stop, grade, re_count, day_offset, window)
        decisions.append(base)
    if dirty:
        _save(data, path)
    return decisions


def _why_not(close: float, last_stop: float, grade: str, re_count: int,
             day_offset: int, window: int) -> str:
    """should_reenter False 사유 (관측 로그용, SAJANG 룰과 동일 순서)."""
    if not SAJANG.REENTRY_ENABLED:
        return "REENTRY_ENABLED=False"
    if re_count >= SAJANG.REENTRY_MAX:
        return f"재진입 한도 소진(re_count={re_count}≥{SAJANG.REENTRY_MAX})"
    rank = SAJANG._KKI_GRADE_RANK.get(grade, 0)
    if rank < SAJANG._KKI_GRADE_RANK.get(SAJANG.REENTRY_MIN_KKI_GRADE, 1):
        return f"끼 부족({grade} < {SAJANG.REENTRY_MIN_KKI_GRADE})"
    if day_offset > window:
        return f"윈도우 초과({day_offset}>{window})"
    if close < last_stop:
        return f"reclaim 미달(종가 {int(close):,} < 손절선 {int(last_stop):,})"
    return "자격 미통과"


# ───────────────────────── 상태 갱신 ─────────────────────────
def mark_acted(code: str, path: Optional[Path] = None) -> None:
    """재진입(라이브) 또는 관측 intent 발생 후 호출 — re_count++ + ACTED(재발 방지).

    이후 그 code 의 새 손절 콜백이 오면 register_stop 이 WATCHING 으로 재무장(다음 reclaim 대기).
    """
    path = path or WATCH_OUT
    code = str(code).zfill(6)
    data = _load(path)
    w = data["watches"].get(code)
    if not w:
        return
    w["re_count"] = int(w.get("re_count", 0)) + 1
    w["status"] = ACTED
    w["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    _save(data, path)


def active_watches(path: Optional[Path] = None) -> dict:
    """현재 WATCHING 상태 감시 (점검/텔레그램용)."""
    return {c: w for c, w in _load(path or WATCH_OUT)["watches"].items() if w.get("status") == WATCHING}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    aw = active_watches()
    print(f"[reentry_watch] WATCHING {len(aw)}건")
    for c, w in aw.items():
        print(f"  {w.get('name')}({c}) stop={w.get('last_stop_price'):,} "
              f"grade={w.get('kki_grade')} date={w.get('stop_date')} re_count={w.get('re_count')}")
    print(f"★ REENTRY_LIVE={SAJANG.REENTRY_LIVE} — False 면 _job_reentry_check 가 관측 intent 만(실주문 0).")
