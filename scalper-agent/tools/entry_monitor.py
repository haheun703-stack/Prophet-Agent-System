# -*- coding: utf-8 -*-
"""실시간 진입 감시 모니터 (3종목 집중)"""
import sys, os, time, json
from datetime import datetime
from pathlib import Path

# PYTHONPATH
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
os.environ.setdefault("PYTHONIOENCODING", "utf-8")

from bot.kis_trader import KISTrader

TARGETS = [
    {
        "code": "028670", "name": "팬오션", "tier": "T5_해운",
        "entry": 5090, "entry_agg": 5490, "sl": 4670, "tp": 5920,
        "memo": "기관+482만 매집, R:R 2.0"
    },
    {
        "code": "047040", "name": "대우건설", "tier": "T2_건설",
        "entry": 8810, "entry_agg": 9200, "sl": 7900, "tp": 10500,
        "memo": "외국인+540만 폭매수, 거래량 압도"
    },
    {
        "code": "000720", "name": "현대건설", "tier": "T2_건설",
        "entry": 146400, "entry_agg": 152000, "sl": 131000, "tp": 170000,
        "memo": "건설 대장, 외국인+92만 매집"
    },
]

def check_entry_conditions(t, price_info):
    """진입 조건 체크 — 5개 중 3개 이상이면 ENTRY"""
    cp = price_info["current_price"]
    vol = price_info.get("volume", 0)
    high = price_info.get("high", cp)
    low = price_info.get("low", cp)
    open_p = price_info.get("open", cp)

    conditions = []
    score = 0

    # 1) 가격 안정: 시가 대비 -2% 이상 안 빠짐
    if open_p > 0:
        drop = (cp / open_p - 1) * 100
        stable = drop >= -2.0
        conditions.append(f"가격안정({'O' if stable else 'X'} {drop:+.1f}%)")
        if stable:
            score += 1

    # 2) 양봉: 현재가 > 시가
    bullish = cp > open_p if open_p > 0 else False
    conditions.append(f"양봉({'O' if bullish else 'X'})")
    if bullish:
        score += 1

    # 3) 거래량 활발: 전일 대비 (간이판단 - 10시 기준 50만주+면 OK)
    vol_ok = vol > 500000
    conditions.append(f"거래량({'O' if vol_ok else 'X'} {vol:,})")
    if vol_ok:
        score += 1

    # 4) 목표가 업사이드 5%+
    tp = t["tp"]
    upside = (tp / cp - 1) * 100 if cp > 0 else 0
    up_ok = upside >= 5.0
    conditions.append(f"업사이드({'O' if up_ok else 'X'} {upside:.1f}%)")
    if up_ok:
        score += 1

    # 5) 진입가 근접: 현재가가 적극진입가 이내
    entry_agg = t["entry_agg"]
    near_entry = cp <= entry_agg
    conditions.append(f"진입가근접({'O' if near_entry else 'X'} vs {entry_agg:,})")
    if near_entry:
        score += 1

    signal = "ENTRY" if score >= 3 else "WAIT"
    return signal, score, conditions


def run_monitor(interval=30, max_minutes=120):
    trader = KISTrader()
    print(f"[{datetime.now():%H:%M:%S}] 진입 감시 시작 — {len(TARGETS)}종목, {interval}초 간격")
    print(f"  감시 종료: {max_minutes}분 후 또는 12:00")
    print("=" * 60)

    start = time.time()
    check_count = 0

    while True:
        now = datetime.now()
        # 12시 이후 종료
        if now.hour >= 12:
            print(f"\n[{now:%H:%M:%S}] 12시 — 감시 종료")
            break
        # 최대 시간 초과
        if (time.time() - start) > max_minutes * 60:
            print(f"\n[{now:%H:%M:%S}] {max_minutes}분 경과 — 감시 종료")
            break

        check_count += 1
        print(f"\n--- [{now:%H:%M:%S}] 체크 #{check_count} ---")

        for t in TARGETS:
            try:
                p = trader.fetch_price(t["code"])
                if not p or not p.get("success"):
                    print(f"  {t['name']}: 조회실패")
                    continue

                cp = p["current_price"]
                chg = p.get("change_rate", 0)
                vol = p.get("volume", 0)
                high = p.get("high", cp)
                low = p.get("low", cp)

                signal, score, conds = check_entry_conditions(t, p)

                arrow = "+" if chg > 0 else ""
                icon = ">> ENTRY!" if signal == "ENTRY" else "   WAIT  "

                vs_sl = (cp / t["sl"] - 1) * 100
                vs_tp = (t["tp"] / cp - 1) * 100

                print(
                    f"  {icon} {t['name']}({t['code']}) "
                    f"{cp:,}원({arrow}{chg:.1f}%) "
                    f"vol={vol:,} H:{high:,} L:{low:,}"
                )
                print(
                    f"           조건 {score}/5: {' | '.join(conds)}"
                )
                print(
                    f"           SL:{t['sl']:,}({vs_sl:+.1f}%) → TP:{t['tp']:,}(+{vs_tp:.1f}%)"
                )

                if signal == "ENTRY":
                    print(f"  *** {t['name']} 진입 시그널! 수동 매수 검토하세요 ***")

            except Exception as e:
                print(f"  {t['name']}: ERR {e}")

            time.sleep(0.2)

        time.sleep(interval)


if __name__ == "__main__":
    run_monitor(interval=30, max_minutes=120)
