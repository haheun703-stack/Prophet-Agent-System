# -*- coding: utf-8 -*-
"""만약 분석기 (What-If Analyzer)
=================================
사장님 5/19 11:43 질문 2건:
  1. 오늘 뭘 샀으면 제일 돈을 많이 벌었나?
  2. 어떤 행동패턴으로 했으면 더 많은 종목에서 수익?

분석:
  A) 09:00 시초가 → 현재가 (실제 보유 시나리오)
  B) 09:00 시초가 → 당일 고점 (이상적 익절 시나리오)
  C) 당일 저점 → 당일 고점 (이상적 진입+이상적 익절)
  D) 사전 시그널 매칭 — 실제로 잡혔던 종목인가
"""
import sys, json
from pathlib import Path
from datetime import datetime

if sys.platform == "win32":
    for _s in ("stdout", "stderr"):
        s = getattr(sys, _s, None)
        if s and hasattr(s, "reconfigure"):
            try: s.reconfigure(encoding="utf-8", errors="replace")
            except: pass

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from bot.kis_trader import KISTrader
from utils.asset_pool_loader import (
    get_high_confidence_candidates,
    get_candidate_source_map,
    load_limit_up_triggers,
    load_limit_up_watchlist,
    load_massive_dual_buy,
    load_oneshot_stealth,
    load_accumulation_radar,
)


def main():
    trader = KISTrader()

    print("=" * 90)
    print(f"🤔 만약 분석기 — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 90)

    # 1) 등락률 TOP 50
    flu = trader.fetch_ranking_fluctuation(market="J", top_n=50)
    print(f"\n📊 등락률 TOP 50 (현재) 로드 완료")

    # 2) 각 종목 OHLC 조회
    rows = []
    for x in flu[:30]:
        code = x["code"]
        name = x["name"]
        try:
            r = trader.fetch_price(code)
            if not r.get("success"):
                continue
            cur = r.get("current_price", 0)
            op = r.get("open", 0)
            hi = r.get("high", 0)
            lo = r.get("low", 0)
            ch_pct = r.get("change_rate", 0)
            vol = r.get("volume", 0)
            if op <= 0:
                continue

            # 수익 계산 (1주 기준)
            pnl_A = cur - op            # 시초 → 현재 보유
            pnl_B = hi - op             # 시초 → 고점 익절
            pnl_C = hi - lo             # 저점 → 고점 (이상적)
            pnl_A_pct = (cur/op - 1) * 100 if op > 0 else 0
            pnl_B_pct = (hi/op - 1) * 100 if op > 0 else 0
            pnl_C_pct = (hi/lo - 1) * 100 if lo > 0 else 0

            rows.append({
                "code": code, "name": name,
                "open": op, "high": hi, "low": lo, "current": cur,
                "change_pct": ch_pct, "volume": vol,
                "pnl_A": pnl_A, "pnl_A_pct": pnl_A_pct,
                "pnl_B": pnl_B, "pnl_B_pct": pnl_B_pct,
                "pnl_C": pnl_C, "pnl_C_pct": pnl_C_pct,
            })
        except Exception as e:
            print(f"  [{code}] 조회 실패: {e}")

    # 3) 사전 시그널 매칭
    triggers = {t.get("code") for t in load_limit_up_triggers() if isinstance(t, dict)}
    watchlist = {w.get("code") for w in load_limit_up_watchlist() if isinstance(w, dict)}
    massive = {x.get("code") for x in load_massive_dual_buy() if isinstance(x, dict)}
    oneshot = {x.get("code") for x in load_oneshot_stealth() if isinstance(x, dict)}
    radar = {x.get("code") for x in load_accumulation_radar() if isinstance(x, dict)}
    high_conf = set(get_high_confidence_candidates())

    # 정보봇 daytrading
    try:
        dp = json.load(open(ROOT / "data_store" / "daytrading_picks.json", encoding="utf-8"))
        daytrading = {p.get("ticker") or p.get("code") for p in (dp if isinstance(dp, list) else dp.get("picks", []))}
    except Exception:
        daytrading = set()

    for r in rows:
        sigs = []
        if r["code"] in triggers: sigs.append("상한가-trigger")
        if r["code"] in watchlist: sigs.append("상한가-watchlist")
        if r["code"] in massive: sigs.append("쌍매수")
        if r["code"] in oneshot: sigs.append("원샷잠복")
        if r["code"] in radar: sigs.append("매집레이더")
        if r["code"] in high_conf: sigs.append("⭐고신뢰")
        if r["code"] in daytrading: sigs.append("정보봇")
        r["signals"] = sigs

    # 4) 출력: 시나리오 A (시초→현재) TOP 15
    rows.sort(key=lambda x: -x["pnl_A"])
    print()
    print("=" * 90)
    print("🥇 시나리오 A: 09:00 시초가 → 현재가 (보유 중) TOP 15")
    print("=" * 90)
    print(f"{'순':<3}{'종목':<14}{'시가':>9}{'현재':>9}{'1주수익':>10}{'%':>7}  사전시그널")
    print("-" * 90)
    for i, r in enumerate(rows[:15], 1):
        sig_str = ", ".join(r["signals"]) if r["signals"] else "-"
        print(f"{i:<3}{r['name'][:13]:<14}{r['open']:>9,}{r['current']:>9,}{r['pnl_A']:>10,}{r['pnl_A_pct']:>6.2f}%  {sig_str}")

    # 5) 시나리오 B: 시초 → 고점 익절
    rows.sort(key=lambda x: -x["pnl_B"])
    print()
    print("=" * 90)
    print("🥈 시나리오 B: 09:00 시초가 → 당일 고점 매도 (이상 익절) TOP 15")
    print("=" * 90)
    print(f"{'순':<3}{'종목':<14}{'시가':>9}{'고점':>9}{'1주수익':>10}{'%':>7}  사전시그널")
    print("-" * 90)
    for i, r in enumerate(rows[:15], 1):
        sig_str = ", ".join(r["signals"]) if r["signals"] else "-"
        print(f"{i:<3}{r['name'][:13]:<14}{r['open']:>9,}{r['high']:>9,}{r['pnl_B']:>10,}{r['pnl_B_pct']:>6.2f}%  {sig_str}")

    # 6) 시나리오 C: 저점 → 고점 (이상)
    rows.sort(key=lambda x: -x["pnl_C"])
    print()
    print("=" * 90)
    print("🏆 시나리오 C: 당일 저점 → 당일 고점 매도 (이상 진입+이상 익절) TOP 15")
    print("=" * 90)
    print(f"{'순':<3}{'종목':<14}{'저점':>9}{'고점':>9}{'1주수익':>10}{'%':>7}  사전시그널")
    print("-" * 90)
    for i, r in enumerate(rows[:15], 1):
        sig_str = ", ".join(r["signals"]) if r["signals"] else "-"
        print(f"{i:<3}{r['name'][:13]:<14}{r['low']:>9,}{r['high']:>9,}{r['pnl_C']:>10,}{r['pnl_C_pct']:>6.2f}%  {sig_str}")

    # 7) 메타 분석: 사전 시그널 매칭 비율
    captured = [r for r in rows if r["signals"]]
    missed = [r for r in rows if not r["signals"]]
    print()
    print("=" * 90)
    print("🎯 메타 분석: 사전 시그널 매칭 비율")
    print("=" * 90)
    print(f"등락률 TOP 30 중:")
    print(f"  ✅ 사전 시그널 잡힌 종목: {len(captured)}개")
    print(f"  ❌ 사전 시그널 미포착: {len(missed)}개")

    # 평균 수익
    if captured:
        avg_cap_A = sum(r["pnl_A_pct"] for r in captured) / len(captured)
        avg_cap_B = sum(r["pnl_B_pct"] for r in captured) / len(captured)
        print(f"\n  잡힌 종목 평균 등락률:")
        print(f"    시나리오 A (보유): {avg_cap_A:+.2f}%")
        print(f"    시나리오 B (이상익절): {avg_cap_B:+.2f}%")
    if missed:
        avg_miss_A = sum(r["pnl_A_pct"] for r in missed) / len(missed)
        avg_miss_B = sum(r["pnl_B_pct"] for r in missed) / len(missed)
        print(f"\n  미포착 종목 평균 등락률:")
        print(f"    시나리오 A (보유): {avg_miss_A:+.2f}%")
        print(f"    시나리오 B (이상익절): {avg_miss_B:+.2f}%")

    # 8) 행동 패턴 시뮬레이션
    print()
    print("=" * 90)
    print("💡 행동패턴별 가상 손익 (등락률 TOP 30 전체 기준)")
    print("=" * 90)

    top5_by_A = sorted(rows, key=lambda x: -x["pnl_A"])[:5]
    top5_by_B = sorted(rows, key=lambda x: -x["pnl_B"])[:5]
    patterns = [
        ("1주 균등 매수 + 종일 보유 (현재까지)",
         sum(r["pnl_A"] for r in rows), 30),
        ("1주 균등 매수 + 고점 익절 (이상)",
         sum(r["pnl_B"] for r in rows), 30),
        ("사전 시그널 종목만 1주씩 + 종일 보유",
         sum(r["pnl_A"] for r in captured), len(captured)),
        ("사전 시그널 종목만 1주씩 + 고점 익절",
         sum(r["pnl_B"] for r in captured), len(captured)),
        ("TOP 5 보유 (사후 best) × 1주",
         sum(r["pnl_A"] for r in top5_by_A), 5),
        ("TOP 5 고점익절 (사후 best) × 1주",
         sum(r["pnl_B"] for r in top5_by_B), 5),
    ]
    print(f"{'패턴':<55}{'종목수':>6}{'1주씩 손익':>15}")
    print("-" * 90)
    for pat_name, total, n in patterns:
        print(f"{pat_name:<55}{n:>6}{total:>15,}")

    return rows, captured, missed


if __name__ == "__main__":
    main()
