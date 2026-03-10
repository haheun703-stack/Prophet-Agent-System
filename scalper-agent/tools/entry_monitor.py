# -*- coding: utf-8 -*-
"""실시간 진입 감시 모니터 (6종목 — 기초분석 TOP4 + 수급검증 2종목)"""
import sys, os, time, json
from datetime import datetime
from pathlib import Path

# PYTHONPATH
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
os.environ.setdefault("PYTHONIOENCODING", "utf-8")

from bot.kis_trader import KISTrader

# --- 감시 대상 ---
# 기초분석 TOP4 (PBR/PER 저평가 + 흑자 + 배당)
FUNDAMENTAL_PICKS = [
    {
        "code": "241560", "name": "두산밥캣", "tier": "T7_건설장비",
        "entry": 58800, "entry_agg": 60000, "sl": 52900, "tp": 72000,
        "memo": "PBR0.81 PER12.9 배당2.9% 기초분석1위"
    },
    {
        "code": "375500", "name": "DL이앤씨", "tier": "T2_건설",
        "entry": 44230, "entry_agg": 46230, "sl": 39770, "tp": 54700,
        "memo": "PBR0.38극저평가 PER7.0 BPS12만vs현4.6만"
    },
    {
        "code": "005490", "name": "POSCO홀딩스", "tier": "T4_철강",
        "entry": 343840, "entry_agg": 359830, "sl": 308460, "tp": 427500,
        "memo": "PBR0.46 배당3.0% BPS73만vs현34만"
    },
    {
        "code": "028050", "name": "삼성E&A", "tier": "T2_건설",
        "entry": 32050, "entry_agg": 34180, "sl": 29780, "tp": 39800,
        "memo": "PER11.1 배당2.5% 중동수주95조"
    },
]

# 수급검증 통과 종목 (외국인/기관 매집)
SUPPLY_PICKS = [
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
]

TARGETS = FUNDAMENTAL_PICKS + SUPPLY_PICKS


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

    # 3) 거래량 활발: 종목별 차등 (대형주는 10만, 소형주는 50만)
    vol_threshold = 100000 if cp > 100000 else 500000
    vol_ok = vol > vol_threshold
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


def run_monitor(interval=30, max_minutes=180):
    trader = KISTrader()
    print(f"[{datetime.now():%H:%M:%S}] 진입 감시 시작 -- {len(TARGETS)}종목, {interval}초 간격")
    print(f"  기초분석 TOP4: {', '.join(t['name'] for t in FUNDAMENTAL_PICKS)}")
    print(f"  수급검증 2종목: {', '.join(t['name'] for t in SUPPLY_PICKS)}")
    print(f"  감시 종료: {max_minutes}분 후 또는 12:00")
    print("=" * 70)

    start = time.time()
    check_count = 0

    while True:
        now = datetime.now()
        # 12시 이후 종료
        if now.hour >= 12:
            print(f"\n[{now:%H:%M:%S}] 12시 -- 감시 종료")
            break
        # 최대 시간 초과
        if (time.time() - start) > max_minutes * 60:
            print(f"\n[{now:%H:%M:%S}] {max_minutes}분 경과 -- 감시 종료")
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
                    f"           SL:{t['sl']:,}({vs_sl:+.1f}%) -> TP:{t['tp']:,}(+{vs_tp:.1f}%)"
                )

                if signal == "ENTRY":
                    print(f"  *** {t['name']} 진입 시그널! [{t['memo']}] ***")

            except Exception as e:
                print(f"  {t['name']}: ERR {e}")

            time.sleep(0.2)

        time.sleep(interval)


if __name__ == "__main__":
    run_monitor(interval=30, max_minutes=180)
