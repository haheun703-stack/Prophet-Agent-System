# -*- coding: utf-8 -*-
"""재진입 라이브 배선 테스트 (6/22) — reentry_watch + auto_trader.reentry_buy_one 게이트.

검증 범위:
  A. on_trade_event / register_stop : 손절·트레일링 매도만 등록 / 비손절(TP·마감) 무시 / BUY 무시 / 멱등 / raise 금지
  B. evaluate_watch                  : reclaim / 끼게이트(SLUGGISH·UNKNOWN) / 윈도우 EXPIRE / re_count 한도 / 종가실패
  C. reentry_buy_one                 : OBSERVE 모드(실주문 0·intent allowed=False) / 가드(disabled·중복보유·보호) / live 예산부족 SKIP
  D. 리터럴 0건                       : 0.97 등 SAJANG 단일진실 우회 금지

★ 매도 무접촉·flag OFF 불변식 검증. 실행: python tests/test_reentry_watch_6_22.py
"""
import sys
import asyncio
import json
import tempfile
import re
from pathlib import Path
from datetime import date, timedelta, datetime

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from data import reentry_watch as rw
from data.sajang_rules import SAJANG
from data.trading_calendar import get_trading_days_between

PASS = 0
FAILS = []


def check(cond, label):
    global PASS
    if cond:
        PASS += 1
    else:
        FAILS.append(label)


def _tmp():
    return Path(tempfile.mkdtemp()) / "reentry_watch_test.json"


def _today_str():
    return datetime.now().strftime("%Y-%m-%d")


def _trading_day_before(n):
    """n 거래일 전 날짜 문자열 (윈도우 테스트용)."""
    end = datetime.now().date()
    start = end - timedelta(days=n + 12)   # 충분히 과거
    tds = get_trading_days_between(start, end)
    # tds[-1] == 오늘(또는 직전 거래일). n 거래일 전 = tds[-(n+1)]
    idx = max(0, len(tds) - 1 - n)
    return tds[idx].strftime("%Y-%m-%d")


# ───────────────────────── A. 등록 / 콜백 ─────────────────────────
def test_register_and_callback():
    p = _tmp()
    rw.WATCH_OUT = p                       # None-resolve 덕에 호출시점 반영
    rw.EARLY_SHADOW = p.parent / "no_early.json"   # 없음 → UNKNOWN

    # register_stop 직접
    ok = rw.register_stop("000660", "SK하이닉스", 100000, 97000, _today_str(),
                          kki_grade="MODERATE", path=p)
    check(ok is True, "A1 register_stop 반환 True")
    d = json.loads(p.read_text(encoding="utf-8"))
    check(d["watches"].get("000660", {}).get("status") == rw.WATCHING, "A2 WATCHING 등록")
    check(d["watches"]["000660"]["last_stop_price"] == 97000, "A3 손절선 기록")

    # 멱등: 같은 code 두 번째 손절 → 1건, 갱신
    rw.register_stop("000660", "SK하이닉스", 100000, 95000, _today_str(),
                     kki_grade="MODERATE", path=p)
    d = json.loads(p.read_text(encoding="utf-8"))
    check(len(d["watches"]) == 1, "A4 멱등(code 1건)")
    check(d["watches"]["000660"]["last_stop_price"] == 95000, "A5 재등록 시 손절선 갱신")

    # 콜백: 손절 매도만 등록
    p2 = _tmp(); rw.WATCH_OUT = p2
    rw.on_trade_event(side="SELL", code="005930", name="삼성전자", qty=10, price=70000,
                      source="auto_trader", entry_price=72000, note="sell_sl stop_loss")
    d2 = json.loads(p2.read_text(encoding="utf-8"))
    check("005930" in d2["watches"], "A6 콜백 손절매도 등록")

    p3 = _tmp(); rw.WATCH_OUT = p3
    rw.on_trade_event(side="SELL", code="005930", name="삼성전자", qty=10, price=70000,
                      source="auto_trader", entry_price=72000, note="sell_close trailing_stop")
    check(p3.exists() and "005930" in json.loads(p3.read_text(encoding="utf-8"))["watches"],
          "A7 콜백 트레일링 등록")

    # 비손절(TP/마감) · BUY → 무시
    p4 = _tmp(); rw.WATCH_OUT = p4
    rw.on_trade_event(side="SELL", code="111111", name="익절", qty=1, price=100,
                      source="x", entry_price=90, note="sell_tp take_profit")
    rw.on_trade_event(side="SELL", code="222222", name="마감", qty=1, price=100,
                      source="x", entry_price=90, note="sell_close 검증모드 15:25 강제 청산")
    rw.on_trade_event(side="BUY", code="333333", name="매수", qty=1, price=100,
                      source="x", entry_price=0, note="buy")
    no_watch = (not p4.exists()) or len(json.loads(p4.read_text(encoding="utf-8"))["watches"]) == 0
    check(no_watch, "A8 비손절매도(TP/강제청산)/BUY 무시")

    # A10 dynamic_sl(daily 재평가 -3% 손절) → 등록 (Tier1 분석기 High 발견 fix)
    p5 = _tmp(); rw.WATCH_OUT = p5
    rw.on_trade_event(side="SELL", code="444444", name="다이나믹SL", qty=1, price=970,
                      source="auto_trader", entry_price=1000, note="sell_sl dynamic_sl: 추세 약화 -3%")
    check(p5.exists() and "444444" in json.loads(p5.read_text(encoding="utf-8"))["watches"],
          "A10 dynamic_sl 손절 등록(놓쳤던 경로)")

    # A11 흔들기 아닌 매도(dynamic_sell 모멘텀이탈·hard_kill 재앙·갭다운·sync) → 무시
    p6 = _tmp(); rw.WATCH_OUT = p6
    for cd, nt in [("555551", "sell_close dynamic_sell: 모멘텀 이탈"),
                   ("555552", "sell_sl hard_kill: -7% 강제매도 (손실 -7.20%)"),
                   ("555553", "sell_sl pending_sells 큐 시초 매도: d1_gap -7%"),
                   ("555554", "sell_close sync_remove: KIS 잔고 사라짐")]:
        rw.on_trade_event(side="SELL", code=cd, name="x", qty=1, price=100,
                          source="x", entry_price=110, note=nt)
    no_w6 = (not p6.exists()) or len(json.loads(p6.read_text(encoding="utf-8"))["watches"]) == 0
    check(no_w6, "A11 모멘텀이탈/hard_kill/갭다운/sync 무시(흔들기 손절 아님)")

    # raise 금지 (불량 입력)
    raised = False
    try:
        rw.on_trade_event(side="SELL", code=None, name=None, qty=None, price=None,
                          source=None, entry_price=None, note="stop_loss")
    except Exception:
        raised = True
    check(not raised, "A9 콜백 raise 금지(매매 흐름 보호)")


# ───────────────────────── B. 평가 ─────────────────────────
def test_evaluate():
    # (코드는 register_stop 이 zfill(6) 하므로 6자리 숫자코드 사용)
    # B1 reclaim + MODERATE + 당일 → REENTER
    p = _tmp()
    rw.register_stop("100001", "에이", 1000, 990, _today_str(), kki_grade="MODERATE", path=p)
    dec = rw.evaluate_watch(lambda c: 995.0, today=datetime.now().date(), path=p)
    check(dec and dec[0]["action"] == "REENTER", "B1 reclaim+MODERATE → REENTER")

    # B2 reclaim 미달 → WAIT
    p = _tmp()
    rw.register_stop("100001", "에이", 1000, 990, _today_str(), kki_grade="MODERATE", path=p)
    dec = rw.evaluate_watch(lambda c: 980.0, today=datetime.now().date(), path=p)
    check(dec and dec[0]["action"] == "WAIT" and "reclaim" in dec[0]["reason"], "B2 reclaim 미달 → WAIT")

    # B3 SLUGGISH → WAIT(끼 게이트)
    p = _tmp()
    rw.register_stop("100002", "비", 1000, 990, _today_str(), kki_grade="SLUGGISH", path=p)
    dec = rw.evaluate_watch(lambda c: 995.0, today=datetime.now().date(), path=p)
    check(dec and dec[0]["action"] == "WAIT" and "끼" in dec[0]["reason"], "B3 SLUGGISH 차단")

    # B4 UNKNOWN → WAIT
    p = _tmp()
    rw.register_stop("100003", "씨", 1000, 990, _today_str(), kki_grade="UNKNOWN", path=p)
    dec = rw.evaluate_watch(lambda c: 995.0, today=datetime.now().date(), path=p)
    check(dec and dec[0]["action"] == "WAIT", "B4 UNKNOWN 차단(보수적)")

    # B5 윈도우 초과 → EXPIRE + 상태 persist
    p = _tmp()
    old = _trading_day_before(5)   # MODERATE window=3 → 5거래일전이면 초과
    rw.register_stop("100004", "디", 1000, 990, old, kki_grade="MODERATE", path=p)
    dec = rw.evaluate_watch(lambda c: 995.0, today=datetime.now().date(), path=p)
    check(dec and dec[0]["action"] == "EXPIRE", "B5 윈도우 초과 → EXPIRE")
    d = json.loads(p.read_text(encoding="utf-8"))
    check(d["watches"]["100004"]["status"] == rw.EXPIRED, "B6 EXPIRE 상태 persist")

    # B7 re_count 한도 소진 → WAIT
    p = _tmp()
    rw.register_stop("100005", "이", 1000, 990, _today_str(), kki_grade="MODERATE", path=p)
    rw.mark_acted("100005", path=p)
    rw.mark_acted("100005", path=p)   # re_count=2=MAX, status ACTED
    rw.register_stop("100005", "이", 1000, 985, _today_str(), kki_grade="MODERATE", path=p)  # 재무장 WATCHING(re_count 보존)
    d = json.loads(p.read_text(encoding="utf-8"))
    check(d["watches"]["100005"]["re_count"] == SAJANG.REENTRY_MAX and d["watches"]["100005"]["status"] == rw.WATCHING,
          "B7a 재무장 시 re_count 보존")
    dec = rw.evaluate_watch(lambda c: 995.0, today=datetime.now().date(), path=p)
    check(dec and dec[0]["action"] == "WAIT" and "한도" in dec[0]["reason"], "B7b re_count 한도 → WAIT")

    # B8 종가 조회 실패 → WAIT
    p = _tmp()
    rw.register_stop("100006", "에프", 1000, 990, _today_str(), kki_grade="MODERATE", path=p)
    dec = rw.evaluate_watch(lambda c: None, today=datetime.now().date(), path=p)
    check(dec and dec[0]["action"] == "WAIT" and "종가" in dec[0]["reason"], "B8 종가 실패 → WAIT")

    # B9 EXPLOSIVE RIDE 윈도우(D+10) — 5거래일전도 윈도우 내 → REENTER
    p = _tmp()
    old5 = _trading_day_before(5)
    rw.register_stop("100007", "지", 1000, 990, old5, kki_grade="EXPLOSIVE", path=p)
    dec = rw.evaluate_watch(lambda c: 995.0, today=datetime.now().date(), path=p)
    check(dec and dec[0]["action"] == "REENTER", "B9 EXPLOSIVE RIDE 윈도우 내 REENTER")


# ───────────────────────── C. reentry_buy_one 게이트 ─────────────────────────
class _FakeTrader:
    def __init__(self, price=995, cash=10_000_000, total_eval=0):
        self._price = price; self._cash = cash; self._eval = total_eval
    def fetch_price(self, code):
        return {"success": True, "current_price": self._price}
    def fetch_balance(self):
        return {"success": True, "cash": self._cash, "total_eval": self._eval}


class _FakeSelf:
    def __init__(self, trader, disabled=False, protected=False, holdings=None, positions=None):
        self.trader = trader
        self._positions = positions if positions is not None else {}
        self._real_holdings_codes = holdings if holdings is not None else set()
        self._send_alert = None
        self._disabled = disabled
        self._protected = protected
    def _auto_trade_disabled(self):
        return self._disabled
    def _is_sell_protected(self, code, ctx):
        return self._protected
    def _save_positions(self):
        pass


def test_reentry_buy_one():
    try:
        import bot.auto_trader as at_mod
        from bot.auto_trader import AutoTrader
    except Exception as e:
        FAILS.append(f"C0 auto_trader import 실패: {type(e).__name__}: {e}")
        return
    # 결정성: 오늘이 휴장일이어도 통과하도록 패치
    at_mod.is_trading_day = lambda d=None: True
    import bot.order_intent as oi
    fn = AutoTrader.reentry_buy_one
    rec = {"name": "테스트", "last_stop_price": 990, "kki_grade": "MODERATE", "re_count": 0}

    # C1 OBSERVE 모드(기본 REENTRY_LIVE=False) → 실주문 0, intent allowed=False
    intent_dir = Path(tempfile.mkdtemp())
    oi.INTENT_DIR = intent_dir
    fake = _FakeSelf(_FakeTrader())
    res = asyncio.run(fn(fake, "000660", rec, live=False))
    check(res.get("action") == "OBSERVE", "C1 live=False → OBSERVE")
    check(len(fake._positions) == 0, "C2 OBSERVE는 포지션 생성 0(실주문 0)")
    # intent 파일 검증
    files = list(intent_dir.glob("order_intents_*.jsonl"))
    rows = []
    for f in files:
        for ln in f.read_text(encoding="utf-8").splitlines():
            rows.append(json.loads(ln))
    obs = [r for r in rows if r.get("reason") == "REENTRY_OBSERVE"]
    check(len(obs) == 1, "C3 관측 intent 1건 기록")
    check(obs and obs[0]["side"] == "BUY" and obs[0]["allowed"] is False, "C4 intent BUY allowed=False(No Intent No Order)")

    # C5 AUTO_TRADE_DISABLED → SKIP
    fake = _FakeSelf(_FakeTrader(), disabled=True)
    res = asyncio.run(fn(fake, "000660", rec, live=False))
    check(res.get("action") == "SKIP" and "DISABLED" in res.get("reason", ""), "C5 disabled → SKIP")

    # C6 이미 보유(원래 슬롯) → SKIP
    fake = _FakeSelf(_FakeTrader(), positions={"000660": {"qty": 1}})
    res = asyncio.run(fn(fake, "000660", rec, live=False))
    check(res.get("action") == "SKIP" and "보유" in res.get("reason", ""), "C6 중복보유 → SKIP")

    # C7 보호 종목 → SKIP
    fake = _FakeSelf(_FakeTrader(), protected=True)
    res = asyncio.run(fn(fake, "000660", rec, live=False))
    check(res.get("action") == "SKIP" and "protected" in res.get("reason", ""), "C7 sell_protected → SKIP")

    # C8 live=True 라도 예산 0(cash 0) → 매수 전 SKIP(실주문 0)
    fake = _FakeSelf(_FakeTrader(cash=0, total_eval=0))
    res = asyncio.run(fn(fake, "000660", rec, live=True))
    check(res.get("action") == "SKIP" and "예산" in res.get("reason", ""), "C8 live 예산부족 → SKIP(실주문 0)")
    check(len(fake._positions) == 0, "C9 live 예산부족 시 포지션 생성 0")


# ───────────────────────── D. 리터럴 0건 ─────────────────────────
def test_no_literals():
    src = (ROOT / "data" / "reentry_watch.py").read_text(encoding="utf-8")
    # SAJANG 우회 손절선 '곱셈' 리터럴 금지 (예: price * 0.97 / 0.97 * price). 문서 언급은 제외(곱셈 인접만).
    bad = re.findall(r"\*\s*0\.9\d|0\.9\d\s*\*", src)
    check(len(bad) == 0, f"D1 리터럴 곱셈 0건 (발견: {bad})")
    # SAJANG 단일진실 헬퍼 사용 확인
    check("SAJANG.should_reenter" in src, "D2 should_reenter 사용")
    check("SAJANG.effective_hold_days" in src, "D3 effective_hold_days 사용")


def run():
    for t in (test_register_and_callback, test_evaluate, test_reentry_buy_one, test_no_literals):
        try:
            t()
        except Exception as e:
            FAILS.append(f"{t.__name__} 예외: {type(e).__name__}: {e}")
    total = PASS + len(FAILS)
    print(f"\n[test_reentry_watch_6_22] PASS {PASS} / FAIL {len(FAILS)}  (total {total})")
    if FAILS:
        print("  실패:")
        for f in FAILS:
            print("   -", f)
    return len(FAILS) == 0


if __name__ == "__main__":
    ok = run()
    sys.exit(0 if ok else 1)
