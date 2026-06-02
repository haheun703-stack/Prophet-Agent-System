"""하락후반등+피라미딩 전략 정직 재검증 (6/2, read-only).

사장님이 flowx 세션서 가져온 compare_down_market_strategies.py 결과(+81%)를
같은 데이터(Naver 일봉)·같은 종목으로 재현한 뒤, 편향을 하나씩 꺼서
+81%가 어디서 나오는지 분해한다.

분해 사다리(누적):
  boss   : 사장님 로직 그대로 (green_day 종가필터 ON, 체결 정확, 비용 0) → 재현 검증
  fill   : + 현실 체결 (갭손절=시가체결, 갭업진입=시가추격)
  nolook : + green_day lookahead 제거 (+1% 터치하면 종가 무관 진입)
  net    : + 비용 (매수 0.15% / 매도 0.33%)

봇/룰/주문/SAJANG 무접촉. 순수 분석. SAJANG·order import 없음(게이트 무관).
"""
from __future__ import annotations

import argparse
import ast
import math
import sys
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from typing import Any

LARGE = ["005930","000660","035420","035720","005380","012450","373220",
         "105560","055550","086790","068270","207940","005490","010120","454910"]
# 사장님 중소형 14 (043260 아웃라이어 제외 — JSON과 동일)
SMALL_MID = ["003080","012030","035900","041190","053950","064260","078340",
             "089980","101490","108490","112040","194480","215600","225570"]

BUY_COST = 0.0015
SELL_COST = 0.0033


def naver_daily(ticker: str, start: str, end: str) -> list[dict[str, Any]]:
    params = {"symbol": ticker, "requestType": "1", "startTime": start,
              "endTime": end, "timeframe": "day"}
    url = "https://api.finance.naver.com/siseJson.naver?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0",
                                               "Accept": "text/plain, */*"})
    with urllib.request.urlopen(req, timeout=20) as resp:  # noqa: S310
        txt = resp.read().decode("utf-8", "ignore").strip()
    data = ast.literal_eval(txt)
    rows = []
    for it in data[1:]:
        if not isinstance(it, list) or len(it) < 6:
            continue
        d = str(it[0])
        rows.append({"date": f"{d[:4]}-{d[4:6]}-{d[6:8]}",
                     "open": float(it[1]), "high": float(it[2]),
                     "low": float(it[3]), "close": float(it[4]), "volume": int(it[5])})
    rows = [r for r in rows if r["open"] > 0 and r["high"] > 0 and r["low"] > 0 and r["close"] > 0]
    rows.sort(key=lambda r: r["date"])
    return rows


@dataclass
class Position:
    shares: int = 0
    avg_price: float = 0.0
    invested: float = 0.0   # 비용 포함 실투입(현금 차감액)
    cost_basis: float = 0.0  # 순수 주식가치(평단 계산용)
    peak: float = 0.0

    def buy(self, shares: int, price: float, cost_on: bool) -> float:
        if shares <= 0:
            return 0.0
        value = shares * price
        fee = value * BUY_COST if cost_on else 0.0
        self.cost_basis += value
        self.shares += shares
        self.avg_price = self.cost_basis / self.shares
        self.invested += value + fee
        self.peak = max(self.peak, price)
        return value + fee

    def reset(self):
        self.__init__()


def _sim(rows, cap, mode, *, kind, down=3.0, rebound=1.0, stop=4.0, trail=4.0,
         ladder=(3.0, 5.0, 7.0), add_levels=(1.0, 2.0, 3.0), alloc=(10, 30, 30, 30)):
    """kind: 'A'(물타기) | 'B'(반등 풀진입) | 'P'(피라미딩).
    mode: boss|fill|nolook|net  (누적 토글)."""
    use_green = mode in ("boss", "fill")             # green_day lookahead: nolook부터 OFF
    realistic = mode in ("fill", "nolook", "net")    # 현실 체결: fill부터 ON
    cost_on = mode == "net"                           # 비용: net만 ON

    cash = float(cap)
    pos = Position()
    realized = 0.0
    nwin = nloss = ntr = 0
    dn, rb, st, tr = down/100, rebound/100, stop/100, trail/100
    anchor = 0.0
    add_idx = 0

    def sell_all(price):
        nonlocal cash, realized, nwin, nloss, ntr
        proceeds = pos.shares * price
        if cost_on:
            proceeds *= (1 - SELL_COST)
        pnl = proceeds - pos.invested
        cash += proceeds
        realized += pnl
        if pnl > 0:
            nwin += 1
        else:
            nloss += 1
        ntr += 1
        pos.reset()

    start_i = 1 if kind == "A" else 2
    for i in range(start_i, len(rows)):
        day = rows[i]
        prev = rows[i-1]

        # ---- 보유 중: 청산 체크 ----
        if pos.shares > 0:
            if kind == "A":
                stop_px = pos.peak * (1 - tr)
                if day["low"] <= stop_px:
                    fill = min(day["open"], stop_px) if realistic else stop_px
                    sell_all(fill)
                    continue
                pos.peak = max(pos.peak, day["high"])
            else:
                active_stop = max(pos.avg_price * (1 - st), pos.peak * (1 - tr))
                if day["low"] <= active_stop:
                    fill = min(day["open"], active_stop) if realistic else active_stop
                    sell_all(fill)
                    anchor = 0.0; add_idx = 0
                    continue
                pos.peak = max(pos.peak, day["high"])
                # 피라미딩 추가매수
                if kind == "P":
                    while add_idx < len(add_levels) and cash > 0:
                        add_px = anchor * (1 + add_levels[add_idx]/100)
                        if day["high"] < add_px:
                            break
                        fill = max(day["open"], add_px) if realistic else add_px
                        spend = min(cash, cap * alloc[add_idx+1] / 100)
                        sh = math.floor(spend / fill)
                        if sh > 0:
                            cash -= pos.buy(sh, fill, cost_on)
                        add_idx += 1
                    pos.peak = max(pos.peak, day["high"])

        if pos.shares > 0 or cash <= 0:
            continue

        # ---- 신규 진입 ----
        if kind == "A":
            for pct in ladder:
                if cash <= 0:
                    break
                entry = prev["close"] * (1 - pct/100)
                if day["low"] <= entry:
                    spend = min(cash, cap / len(ladder))
                    sh = math.floor(spend / entry)
                    if sh > 0:
                        cash -= pos.buy(sh, entry, cost_on)
            if pos.shares > 0:
                pos.peak = max(pos.peak, day["high"])
        else:
            prev_prev = rows[i-2]
            down_setup = (prev["close"] <= prev_prev["close"]*(1-dn)) or (prev["low"] <= prev_prev["close"]*(1-dn))
            rebound_px = prev["close"] * (1 + rb)
            green = day["close"] > day["open"]
            ok = down_setup and day["high"] >= rebound_px and (green if use_green else True)
            if ok:
                fill = max(day["open"], rebound_px) if realistic else rebound_px
                if kind == "B":
                    sh = math.floor(cash / fill)
                    if sh > 0:
                        cash -= pos.buy(sh, fill, cost_on)
                else:  # P
                    anchor = rebound_px; add_idx = 0
                    spend = min(cash, cap * alloc[0] / 100)
                    sh = math.floor(spend / fill)
                    if sh > 0:
                        cash -= pos.buy(sh, fill, cost_on)
                pos.peak = max(pos.peak, day["high"])

    last = rows[-1]["close"]
    pos_val = pos.shares * last
    if cost_on and pos.shares:
        pos_val *= (1 - SELL_COST)
    equity = cash + pos_val
    return {"profit": equity - cap, "realized": realized,
            "unreal": pos_val - pos.invested if pos.shares else 0.0,
            "wins": nwin, "losses": nloss, "trades": ntr}


def run(universe, tickers, start, end):
    sys.stdout.reconfigure(encoding="utf-8")
    print(f"\n데이터 수집: {universe} {len(tickers)}종목 ({start}~{end}) ...")
    rows_by = {}
    for t in tickers:
        try:
            r = naver_daily(t, start, end)
            if len(r) >= 20:
                rows_by[t] = r
            else:
                print(f"  skip {t}: rows={len(r)}")
        except Exception as e:  # noqa: BLE001
            print(f"  ERROR {t}: {e}")
    print(f"  수집 완료 {len(rows_by)}종목, 거래일 {len(next(iter(rows_by.values())))}")

    cap = 10_000_000
    modes = ["boss", "fill", "nolook", "net"]
    kinds = {"A 물타기": "A", "B 반등풀진입": "B", "P 피라미딩(10/40/70/100)": "P"}
    print(f"\n{'='*78}\n{universe}  (종목당 {cap:,}원, 총자본 {cap*len(rows_by):,}원)\n{'='*78}")
    print(f"{'전략':<26}{'boss':>13}{'fill':>13}{'nolook':>13}{'net':>13}")
    for kname, kind in kinds.items():
        line = f"{kname:<26}"
        wr = {}
        for mode in modes:
            agg = {"profit": 0.0, "wins": 0, "losses": 0, "trades": 0}
            for t, rows in rows_by.items():
                s = _sim(rows, cap, mode, kind=kind)
                agg["profit"] += s["profit"]; agg["wins"] += s["wins"]
                agg["losses"] += s["losses"]; agg["trades"] += s["trades"]
            tot = cap*len(rows_by)
            line += f"{agg['profit']/1e6:>9,.1f}M".rjust(13)
            wr[mode] = (100*agg['wins']/agg['trades'] if agg['trades'] else 0, agg['trades'])
        print(line)
        print(f"{'  승률%/거래수':<26}" + "".join(f"{wr[m][0]:>6.0f}/{wr[m][1]:<6}".rjust(13) for m in modes))
    print(f"\n  ※ 누적분해: boss(사장님재현) → fill(현실체결) → nolook(lookahead제거) → net(+비용)")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="20251202")
    ap.add_argument("--end", default="20260602")
    ap.add_argument("--universe", choices=["large", "small_mid", "both"], default="both")
    a = ap.parse_args()
    if a.universe in ("large", "both"):
        run("대형주 15", LARGE, a.start, a.end)
    if a.universe in ("small_mid", "both"):
        run("중소형 14", SMALL_MID, a.start, a.end)


if __name__ == "__main__":
    main()
