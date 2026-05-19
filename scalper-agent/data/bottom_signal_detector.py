# -*- coding: utf-8 -*-
"""바닥 신호 감지기 (Bottom Signal Detector)
============================================
5/19 사장님 결정: "MDD -30% 견디면 실적 좋은 놈은 회복. 전력섹터 바닥 보이면 분할매수."

5가지 바닥 신호 동시 감지:
  ① RSI < 30 (과매도)
  ② 거래량 ≥ 20일 평균 × 2 + 양봉 (반전 신호)
  ③ 볼린저 하단(20일, 2σ) 터치
  ④ 매크로 반등 — KOSPI +1%+ AND KODEX 인버스 -1%+ (시장 바닥)
  ⑤ 섹터 동조 — 전력기기 BIG3(효성298040/HD현대일렉267260/LS일렉010120) 중 1개 +5%+

5개 중 3개+ 동시 충족 → 1차 분할매수 추천 (텔레그램)
5개 중 4개+ → 2차 추천 (강한 신호)
5개 모두 → 3차 추천 (강력 바닥 확정)

사용:
  python -X utf8 data/bottom_signal_detector.py            # 실시간 스캔
  python -X utf8 data/bottom_signal_detector.py --code 062040  # 특정 종목
"""
from __future__ import annotations

import sys
import json
import argparse
import logging
from datetime import datetime
from pathlib import Path

if sys.platform == "win32":
    for _s in ("stdout", "stderr"):
        s = getattr(sys, _s, None)
        if s and hasattr(s, "reconfigure"):
            try: s.reconfigure(encoding="utf-8", errors="replace")
            except: pass

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import pandas as pd
from utils.stock_utils import load_daily

DAILY_DIR = ROOT / "data_store" / "daily"
OUTPUT_PATH = ROOT / "data_store" / "bottom_signals.json"
POSITIONS_PATH = ROOT / "data_store" / "positions.json"

# 전력기기 BIG3 (섹터 동조 판별용)
POWER_BIG3 = {
    "298040": "효성중공업",
    "267260": "HD현대일렉트릭",
    "010120": "LS일렉트릭",
}

# 매크로 ETF
KOSPI_INDEX_PROXY = "069500"   # KODEX 200
KODEX_INVERSE = "114800"        # KODEX 인버스

logger = logging.getLogger("BH.BottomSignal")


def calc_rsi(closes, period=14):
    """RSI 계산."""
    if len(closes) < period + 1:
        return 50.0
    delta = pd.Series(closes).diff()
    gain = delta.where(delta > 0, 0).rolling(period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(period).mean()
    if loss.iloc[-1] <= 0:
        return 100.0
    rs = gain.iloc[-1] / loss.iloc[-1]
    return 100 - (100 / (1 + rs))


def calc_bollinger(closes, period=20, k=2):
    """볼린저 밴드 하단."""
    if len(closes) < period:
        return 0
    s = pd.Series(closes[-period:])
    return float(s.mean() - k * s.std())


def check_macro_rebound(trader) -> tuple[bool, str]:
    """KOSPI +1%+ AND 인버스 -1%+ 동시 충족 시 매크로 바닥 신호."""
    try:
        kospi = trader.fetch_price(KOSPI_INDEX_PROXY)
        inverse = trader.fetch_price(KODEX_INVERSE)
        if not (kospi.get("success") and inverse.get("success")):
            return False, "데이터 부족"
        k_ch = kospi.get("change_rate", 0)
        i_ch = inverse.get("change_rate", 0)
        if k_ch >= 1.0 and i_ch <= -1.0:
            return True, f"KOSPI {k_ch:+.2f}% / 인버스 {i_ch:+.2f}%"
        return False, f"KOSPI {k_ch:+.2f}% / 인버스 {i_ch:+.2f}% (미충족)"
    except Exception as e:
        return False, f"조회 실패: {e}"


def check_sector_concurrent(trader) -> tuple[bool, str]:
    """전력기기 BIG3 중 1개라도 +5%+ 시 섹터 반등 신호."""
    try:
        for code, name in POWER_BIG3.items():
            r = trader.fetch_price(code)
            if r.get("success") and r.get("change_rate", 0) >= 5.0:
                return True, f"{name} {r['change_rate']:+.2f}%"
        return False, "BIG3 모두 +5% 미만"
    except Exception as e:
        return False, f"조회 실패: {e}"


def detect_bottom_signal(code: str, name: str, entry_price: int,
                         macro_ok: bool, macro_reason: str,
                         sector_ok: bool, sector_reason: str,
                         trader=None) -> dict:
    """단일 종목 바닥 신호 5종 체크."""
    df = load_daily(code, DAILY_DIR)
    if df is None or len(df) < 30:
        return {"code": code, "name": name, "error": "데이터 부족"}

    close_col = "종가" if "종가" in df.columns else "close"
    vol_col = "거래량" if "거래량" in df.columns else "volume"
    open_col = "시가" if "시가" in df.columns else "open"

    closes = df[close_col].values
    vols = df[vol_col].values
    opens = df[open_col].values
    cur = float(closes[-1])
    if trader:
        try:
            r = trader.fetch_price(code)
            if r.get("success"):
                cur = r.get("current_price", cur)
        except Exception:
            pass

    # 5개 신호
    signals = {}

    # ① RSI < 30
    rsi = calc_rsi(closes[-30:])
    signals["①_RSI<30"] = {
        "passed": bool(rsi < 30),
        "value": round(rsi, 1),
        "detail": f"RSI {rsi:.1f}",
    }

    # ② 거래량 폭증 + 양봉
    vol_avg20 = vols[-20:].mean()
    today_vol = vols[-1]
    today_open = opens[-1]
    today_close = closes[-1]
    vol_ratio = today_vol / vol_avg20 if vol_avg20 > 0 else 0
    is_bullish = today_close > today_open
    signals["②_거래량×2+양봉"] = {
        "passed": bool(vol_ratio >= 2.0 and is_bullish),
        "value": f"{vol_ratio:.1f}x",
        "detail": f"거래량 {vol_ratio:.1f}x / {'양봉' if is_bullish else '음봉'}",
    }

    # ③ 볼린저 하단 터치
    bb_low = calc_bollinger(closes)
    bb_touch = cur <= bb_low * 1.02  # 2% 마진
    signals["③_볼린저하단"] = {
        "passed": bool(bb_touch),
        "value": int(bb_low),
        "detail": f"하단 {bb_low:,.0f} / 현재 {cur:,.0f}",
    }

    # ④ 매크로 반등 (외부 입력)
    signals["④_매크로반등"] = {
        "passed": macro_ok,
        "value": macro_reason,
        "detail": macro_reason,
    }

    # ⑤ 섹터 동조 (외부 입력)
    signals["⑤_섹터동조"] = {
        "passed": sector_ok,
        "value": sector_reason,
        "detail": sector_reason,
    }

    passed_count = sum(1 for s in signals.values() if s["passed"])

    # 추천 단계
    if passed_count >= 5:
        recommendation = "🔥 3차 분할매수 (강력 바닥 확정)"
        tier = 3
    elif passed_count >= 4:
        recommendation = "🟢 2차 분할매수 (강한 바닥 신호)"
        tier = 2
    elif passed_count >= 3:
        recommendation = "🟡 1차 분할매수 (바닥 신호 등장)"
        tier = 1
    else:
        recommendation = f"⚪ 관망 ({passed_count}/5 신호)"
        tier = 0

    pnl_pct = (cur / entry_price - 1) * 100 if entry_price > 0 else 0

    return {
        "code": code,
        "name": name,
        "entry_price": entry_price,
        "current_price": int(cur),
        "pnl_pct": round(pnl_pct, 2),
        "rsi": round(rsi, 1),
        "vol_ratio": round(vol_ratio, 2),
        "bb_low": int(bb_low),
        "signals": signals,
        "passed_count": passed_count,
        "tier": tier,
        "recommendation": recommendation,
        "trigger_prices": {
            "1차_평단-10%": int(entry_price * 0.90),
            "2차_평단-20%": int(entry_price * 0.80),
            "3차_볼린저하단": int(bb_low),
        },
    }


def scan_holdings(scope: str = "power_sector") -> dict:
    """positions.json에서 보유 종목 → 바닥 신호 스캔.

    scope: power_sector (전력 4종목만) / all (전체)
    """
    if not POSITIONS_PATH.exists():
        return {"error": "positions.json 없음"}

    positions = json.loads(POSITIONS_PATH.read_text("utf-8"))

    # 전력 섹터 4종목
    POWER_HOLDINGS = {"001440", "011790", "033100", "062040"}

    targets = positions
    if scope == "power_sector":
        targets = {k: v for k, v in positions.items() if k in POWER_HOLDINGS}

    # 매크로 + 섹터 한 번만 조회
    from bot.kis_trader import KISTrader
    trader = KISTrader()
    macro_ok, macro_reason = check_macro_rebound(trader)
    sector_ok, sector_reason = check_sector_concurrent(trader)

    results = []
    for code, pos in targets.items():
        r = detect_bottom_signal(
            code=code,
            name=pos.get("name", ""),
            entry_price=pos.get("entry_price", 0),
            macro_ok=macro_ok,
            macro_reason=macro_reason,
            sector_ok=sector_ok,
            sector_reason=sector_reason,
            trader=trader,
        )
        results.append(r)

    # 추천 우선순위 정렬 (tier 높은 순)
    results.sort(key=lambda x: -x.get("tier", 0))

    output = {
        "timestamp": datetime.now().isoformat(),
        "scope": scope,
        "macro_signal": {"passed": macro_ok, "detail": macro_reason},
        "sector_signal": {"passed": sector_ok, "detail": sector_reason},
        "results": results,
        "alert_count": sum(1 for r in results if r.get("tier", 0) >= 1),
    }

    try:
        OUTPUT_PATH.write_text(
            json.dumps(output, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    except Exception as e:
        logger.warning(f"저장 실패: {e}")

    return output


def format_alert(scan: dict) -> str:
    """텔레그램 알림 메시지."""
    results = scan.get("results", [])
    triggered = [r for r in results if r.get("tier", 0) >= 1]
    if not triggered:
        return ""

    lines = [
        f"🛒 바닥 신호 감지 — {len(triggered)}종목 분할매수 추천!",
        f"매크로: {scan['macro_signal']['detail']}",
        f"섹터:   {scan['sector_signal']['detail']}",
        "",
    ]
    for r in triggered:
        lines.append(f"{r['recommendation']} {r['name']} ({r['code']})")
        lines.append(f"  평단 {r['entry_price']:,} / 현재 {r['current_price']:,} ({r['pnl_pct']:+.1f}%)")
        lines.append(f"  RSI {r['rsi']} / 거래량 {r['vol_ratio']}x / 볼린저하단 {r['bb_low']:,}")
        lines.append(f"  통과 신호: {r['passed_count']}/5")
        for sig_name, sig_data in r["signals"].items():
            icon = "✅" if sig_data["passed"] else "❌"
            lines.append(f"    {icon} {sig_name}: {sig_data['detail']}")
        tier = r["tier"]
        if tier >= 1:
            lines.append(f"  → 추천 매수가: {r['trigger_prices'][f'{tier}차_' + ['평단-10%','평단-20%','볼린저하단'][tier-1]]:,}원")
        lines.append("")

    lines.append("⚠️ 사장님 결정: 자동 매수 X / 수동 매수만 (1주씩 분할)")
    return "\n".join(lines)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(message)s",
        datefmt="%H:%M:%S",
    )
    parser = argparse.ArgumentParser()
    parser.add_argument("--scope", default="power_sector",
                        choices=["power_sector", "all"])
    parser.add_argument("--code", type=str, help="특정 종목만")
    args = parser.parse_args()

    if args.code:
        # 단일 종목
        positions = json.loads(POSITIONS_PATH.read_text("utf-8"))
        if args.code not in positions:
            print(f"❌ {args.code} 보유 종목 아님")
            sys.exit(1)
        from bot.kis_trader import KISTrader
        trader = KISTrader()
        m_ok, m_r = check_macro_rebound(trader)
        s_ok, s_r = check_sector_concurrent(trader)
        r = detect_bottom_signal(
            args.code, positions[args.code].get("name", ""),
            positions[args.code].get("entry_price", 0),
            m_ok, m_r, s_ok, s_r, trader,
        )
        print(json.dumps(r, ensure_ascii=False, indent=2))
    else:
        scan = scan_holdings(scope=args.scope)
        print()
        print("=" * 80)
        print(f"바닥 신호 스캔 — {scan['timestamp']}")
        print("=" * 80)
        print(f"매크로: {scan['macro_signal']['detail']}")
        print(f"섹터:   {scan['sector_signal']['detail']}")
        print()
        alert = format_alert(scan)
        if alert:
            print(alert)
        else:
            print("⚪ 모든 종목 신호 미달 (3/5 미만)")
        print()
        print(f"보고서 저장: {OUTPUT_PATH}")
