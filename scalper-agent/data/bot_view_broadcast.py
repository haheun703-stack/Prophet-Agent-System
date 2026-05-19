# -*- coding: utf-8 -*-
"""봇 시야 텔레그램 송출 (Bot View Broadcast)
==============================================
사장님 5/19 요청: "봇이 매 5분 보는 데이터를 텔레그램으로 그대로 보여달라"

봇이 매 5분 보는 데이터:
  - 등락률 TOP 후보
  - 5단계 게이트 검증 결과 (시장/VWAP/호가/체결강도/캔들)
  - 매수 결정 / 관망 / 차단 사유

매 5분 (09:05~14:00 동안) 텔레그램 송출.
"""
from __future__ import annotations

import sys
import json
import logging
from datetime import datetime, time as dtime
from pathlib import Path
from typing import Optional

if sys.platform == "win32":
    for _s in ("stdout", "stderr"):
        s = getattr(sys, _s, None)
        if s and hasattr(s, "reconfigure"):
            try: s.reconfigure(encoding="utf-8", errors="replace")
            except: pass

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

try:
    from zoneinfo import ZoneInfo
    KST = ZoneInfo("Asia/Seoul")
except ImportError:
    KST = None

logger = logging.getLogger("BH.BotView")


def _now_kst() -> datetime:
    return datetime.now(KST) if KST else datetime.now()


def _is_trading_hour() -> bool:
    """09:05~14:00 KST."""
    now = _now_kst().time()
    return dtime(9, 5) <= now <= dtime(14, 0)


def get_bot_view_snapshot(trader, max_candidates: int = 5) -> dict:
    """봇이 현재 시점에 보는 데이터 스냅샷.

    Args:
        trader: KISTrader 인스턴스
        max_candidates: 검증할 TOP 후보 수

    Returns:
        {
            "timestamp": str,
            "market": {regime, kospi_ch, inverse_ch},
            "candidates": [
                {code, name, change_rate, gates: {시장, VWAP, 호가, 강도, 캔들}, decision, reason}
            ],
        }
    """
    snap = {
        "timestamp": _now_kst().strftime("%H:%M:%S"),
        "candidates": [],
    }

    # 매크로 (advisory + KOSPI + 인버스)
    try:
        from utils.quant_advisory_subscriber import fetch_latest_advisory
        adv = None
        for mt in ('LEADING', 'SNAPSHOT', 'ADVICE', 'MORNING_BRIEFING'):
            adv = fetch_latest_advisory(msg_type=mt, fallback_to_yesterday=True)
            if adv: break
        regime = (adv or {}).get('market_regime', '미수신').upper()
    except Exception:
        regime = "미수신"

    kospi_r = trader.fetch_price("069500")  # KODEX 200
    inverse_r = trader.fetch_price("114800")  # KODEX 인버스
    kospi_ch = kospi_r.get("change_rate", 0) if kospi_r.get("success") else 0
    inverse_ch = inverse_r.get("change_rate", 0) if inverse_r.get("success") else 0

    snap["market"] = {
        "regime": regime,
        "kospi_ch": kospi_ch,
        "inverse_ch": inverse_ch,
    }

    # 시장 게이트 1 — advisory
    market_gate_pass = regime not in ("BEARISH", "PANIC")

    # 등락률 TOP 후보 (멀티시그널 합류 종목 대신, 시각화는 TOP 5만)
    try:
        flu = trader.fetch_ranking_fluctuation(market="J", top_n=max_candidates * 3)
        # 등락률 +5%+ 만
        flu = [x for x in flu if x.get("change_rate", 0) >= 5][:max_candidates]
    except Exception as e:
        snap["error"] = f"등락률 조회 실패: {e}"
        return snap

    # 각 종목 5단계 게이트 검증
    for x in flu:
        code = x["code"]
        name = x["name"]
        change = x.get("change_rate", 0)
        cur_price = x.get("price", 0)
        vol = x.get("volume", 0)

        gates = {}

        # ① 시장 게이트
        gates["①시장"] = {"pass": market_gate_pass, "value": regime}

        # ② VWAP 게이트 (사장님 영감, 봇 채택)
        try:
            pr = trader.fetch_price(code)
            cur = pr.get("current_price", cur_price)
            today_open = pr.get("open", 0)
            today_high = pr.get("high", 0)
            today_low = pr.get("low", 0)
            # 단순 VWAP 추정 = (시가+고가+저가+현재가) / 4
            vwap_est = (today_open + today_high + today_low + cur) / 4 if today_open > 0 else cur
            vwap_pass = cur >= vwap_est * 1.02
            gates["②VWAP"] = {
                "pass": bool(vwap_pass),
                "value": f"{cur:,} vs VWAP {vwap_est:,.0f}",
            }
            strength = pr.get("strength", 0)
        except Exception:
            gates["②VWAP"] = {"pass": False, "value": "조회실패"}
            strength = 0

        # ③ 호가 게이트 (단순화 — strength 기반)
        # 실제로는 fetch_quote가 필요하지만 강도 ≥ 100을 우위로 간주
        gates["③호가"] = {
            "pass": bool(strength >= 100),
            "value": f"강도 {strength:.1f} (호가우위 추정)",
        }

        # ④ 체결강도 게이트
        gates["④강도"] = {
            "pass": bool(strength >= 150),
            "value": f"체결강도 {strength:.1f}",
        }

        # ⑤ 캔들 게이트 (양봉 + 거래량)
        # KIS 응답으로 시가 < 현재가 = 양봉, 거래량은 직접 비교 어려움 → 거래량 100만주+ 기준
        is_bullish = cur > today_open if today_open > 0 else False
        vol_ok = vol >= 1_000_000
        gates["⑤캔들"] = {
            "pass": bool(is_bullish and vol_ok),
            "value": f"{'양봉' if is_bullish else '음봉'} / 거래량 {vol:,}",
        }

        passed = sum(1 for g in gates.values() if g["pass"])
        if passed >= 5:
            decision = "✅ 5/5 매수 진행"
            decision_icon = "🟢"
        elif passed == 4:
            decision = "🟡 4/5 자율판단 매수후보"
            decision_icon = "🟡"
        elif passed >= 3:
            decision = "⚪ 3/5 watchlist 등록"
            decision_icon = "⚪"
        else:
            decision = f"❌ {passed}/5 차단"
            decision_icon = "🔴"

        snap["candidates"].append({
            "code": code,
            "name": name,
            "change_rate": change,
            "current_price": cur_price,
            "passed": passed,
            "decision": decision,
            "decision_icon": decision_icon,
            "gates": gates,
        })

    return snap


def format_telegram(snap: dict) -> str:
    """텔레그램 메시지 포맷."""
    lines = [
        f"🤖 [봇 시야 {snap['timestamp']}]",
        f"매크로: regime={snap['market']['regime']} / KOSPI {snap['market']['kospi_ch']:+.2f}% / 인버스 {snap['market']['inverse_ch']:+.2f}%",
        "",
    ]

    candidates = snap.get("candidates", [])
    if not candidates:
        lines.append("⚪ +5% 이상 후보 없음")
        return "\n".join(lines)

    lines.append(f"━━ TOP {len(candidates)} 후보 5단계 게이트 ━━")
    for c in candidates:
        lines.append(f"\n{c['decision_icon']} {c['name']} ({c['code']}) +{c['change_rate']:.2f}%")
        for gate_name, gate_data in c["gates"].items():
            icon = "✅" if gate_data["pass"] else "❌"
            lines.append(f"  {icon} {gate_name}: {gate_data['value']}")
        lines.append(f"  → {c['decision']}")

    lines.append("")
    lines.append("⚠️ 실제 매수는 intraday_scanner의 멀티시그널 합류(체결강도+거래량+tipping)")
    lines.append("    기준 따로 적용. 이건 사장님 모니터링용 시각화 (TOP 5 후보 검증).")
    return "\n".join(lines)


def broadcast_now(trader=None) -> Optional[str]:
    """현재 시점 봇 시야 즉시 송출 (텔레그램 알림용 메시지 반환)."""
    if trader is None:
        from bot.kis_trader import KISTrader
        trader = KISTrader()
    try:
        snap = get_bot_view_snapshot(trader, max_candidates=5)
        return format_telegram(snap)
    except Exception as e:
        logger.error(f"봇 시야 송출 실패: {e}")
        return None


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    msg = broadcast_now()
    print()
    print("=" * 80)
    print(msg or "(메시지 없음)")
    print("=" * 80)
