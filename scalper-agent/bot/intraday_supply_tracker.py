# -*- coding: utf-8 -*-
"""장중 실시간 수급 추적 모듈 (사장님 5/22 20:30 명령).

★ 사장님 핵심 질문 ★:
> "장중에 4번의 수급들이 있는데 이걸 너가 빨리 알아서
>  장중에 더 오를지 내릴지를... 어떤 시그널을 가지고 알수 있는지"

설계:
  - 매 5분 호출 (jarvis_decision 사이클 통합)
  - 정보봇 시그널 (60%) + 단타봇 자체 분석 (40%) 결합
  - 종목별 실시간 점수 → BUY/HOLD/SELL 시그널
  - 09:30/11:00/13:00/14:30 4번 정기 알람 (텔레그램)

정보봇 60%:
  - sniper score (40%) — 5/21 score=82 → +10% 적중 검증
  - smart_money DUAL_FLOW (15%) — 외인+기관 동시 매수
  - supply_scoring (5%) — 종합 수급 점수

단타봇 자체 40%:
  - 5분봉 추세 (15%) — 3연속 양봉 / 음봉
  - 체결강도 변화 (10%) — flow_intensity.json
  - 호가 잔량 (10%) — bid/ask ratio
  - VWAP 위치 (5%) — 상단/하단

영구 메모리: [[project_5_22_evening_learning_fix]] TODO ⑱ (신규)
"""
import logging
from dataclasses import dataclass, asdict
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


@dataclass
class IntradayScore:
    """장중 실시간 점수."""
    code: str
    name: str = ""
    time: str = ""  # HHMM

    # 정보봇 60%
    jgis_composite: float = 0.0
    sniper_signal: str = ""
    smart_money_signal: str = ""
    supply_grade: str = ""

    # 단타봇 자체 40%
    trend_score: float = 0.0      # 5분봉 추세
    flow_score: float = 0.0       # 체결강도
    orderbook_score: float = 0.0  # 호가 잔량
    vwap_position: str = ""       # above/below

    # 종합
    composite: float = 0.0
    signal: str = "HOLD"  # STRONG_BUY/BUY/HOLD/WEAK/SELL
    alert_reason: str = ""

    def to_dict(self) -> dict:
        return asdict(self)


def _score_5min_trend(bars: List[Dict]) -> float:
    """5분봉 추세 점수 (0~100).

    - 3연속 양봉 / 거래량 증가 / 가격 상승 → 높은 점수
    """
    if not bars or len(bars) < 3:
        return 50.0  # 중립

    last3 = bars[-3:]
    bullish = sum(1 for b in last3 if b["close"] > b["open"])
    avg_vol_recent = sum(b["volume"] for b in last3) / 3
    avg_vol_prev = sum(b["volume"] for b in bars[-6:-3]) / 3 if len(bars) >= 6 else avg_vol_recent

    score = 50.0
    score += bullish * 10  # 양봉 1개당 +10
    if avg_vol_recent > avg_vol_prev * 1.2:
        score += 10  # 거래량 증가
    elif avg_vol_recent < avg_vol_prev * 0.8:
        score -= 5   # 거래량 감소

    # 가격 추세
    if last3[-1]["close"] > last3[0]["close"]:
        pct = (last3[-1]["close"] / last3[0]["close"] - 1) * 100
        score += min(pct * 5, 15)  # 최대 +15

    return max(0, min(100, score))


def _score_flow_intensity(code: str) -> float:
    """체결강도 점수 (flow_intensity.json 활용)."""
    try:
        import json
        from pathlib import Path
        path = Path(__file__).resolve().parent.parent / "data_store" / "flow_intensity.json"
        if not path.exists():
            return 50.0
        data = json.loads(path.read_text(encoding="utf-8"))
        items = data.get("items", []) if isinstance(data, dict) else []
        for item in items:
            if str(item.get("code")) == code:
                # exec_strength 100 = 중립, 120+ = 강세, 80- = 약세
                strength = float(item.get("exec_strength", 100))
                if strength >= 120:
                    return 80.0
                elif strength >= 100:
                    return 65.0
                elif strength >= 80:
                    return 50.0
                else:
                    return 35.0
    except Exception:
        pass
    return 50.0


def _score_orderbook(trader, code: str) -> float:
    """호가 점수 (매수벽 vs 매도벽 비율)."""
    try:
        if hasattr(trader, "fetch_orderbook"):
            ob = trader.fetch_orderbook(code)
            if ob and ob.get("success"):
                bid_qty = ob.get("total_bid_qty", 0)
                ask_qty = ob.get("total_ask_qty", 0)
                if ask_qty > 0:
                    ratio = bid_qty / ask_qty
                    # 매수벽 강하면 (ratio > 1.2) 상승 신호
                    if ratio >= 1.5:
                        return 80.0
                    elif ratio >= 1.2:
                        return 65.0
                    elif ratio >= 0.8:
                        return 50.0
                    elif ratio >= 0.5:
                        return 35.0
                    else:
                        return 20.0
    except Exception:
        pass
    return 50.0


def _vwap_position(bars: List[Dict], current_price: float) -> str:
    """현재가 vs VWAP — above/below."""
    if not bars or current_price <= 0:
        return "unknown"
    total_pv = sum(((b["high"] + b["low"] + b["close"]) / 3.0) * b["volume"] for b in bars)
    total_v = sum(b["volume"] for b in bars)
    if total_v <= 0:
        return "unknown"
    vwap = total_pv / total_v
    return "above" if current_price > vwap else "below"


def evaluate_realtime(
    code: str,
    trader=None,
    name: str = "",
    time_label: str = "",
) -> IntradayScore:
    """매 5분 호출 — 종목별 실시간 종합 점수.

    정보봇 60% + 단타봇 40% 결합.
    """
    score = IntradayScore(code=code, name=name, time=time_label)

    # 1) 정보봇 60% (jgis_signal_consumer)
    try:
        from bot.jgis_signal_consumer import fetch_all_signals
        all_sigs = fetch_all_signals(codes=[code])
        if code in all_sigs:
            jgis = all_sigs[code]
            score.jgis_composite = jgis.composite_score
            score.sniper_signal = jgis.sniper_signal
            score.smart_money_signal = jgis.smart_money_signal
            score.supply_grade = jgis.supply_grade
            if not score.name:
                score.name = jgis.name
    except Exception as e:
        logger.debug(f"[realtime] {code} jgis 조회 실패: {e}")

    # 2) 단타봇 자체 40%
    if trader is not None:
        # 5분봉 추세
        try:
            if hasattr(trader, "fetch_5min_candles"):
                bars = trader.fetch_5min_candles(code, count_5min=6)
                if bars:
                    score.trend_score = _score_5min_trend(bars)
                    # VWAP
                    if hasattr(trader, "fetch_price"):
                        p = trader.fetch_price(code)
                        if p and p.get("success"):
                            current = p.get("current_price", 0)
                            score.vwap_position = _vwap_position(bars, current)
        except Exception as e:
            logger.debug(f"[realtime] {code} 5분봉 실패: {e}")

        # 체결강도
        score.flow_score = _score_flow_intensity(code)

        # 호가
        score.orderbook_score = _score_orderbook(trader, code)

    # 3) 종합 점수 계산
    composite = 0.0
    composite += score.jgis_composite * 0.6  # 정보봇 60%
    composite += score.trend_score * 0.15    # 5분봉 추세 15%
    composite += score.flow_score * 0.10     # 체결강도 10%
    composite += score.orderbook_score * 0.10  # 호가 10%
    if score.vwap_position == "above":
        composite += 5  # VWAP 위 +5
    elif score.vwap_position == "below":
        composite -= 3  # VWAP 아래 -3

    score.composite = round(composite, 1)

    # 4) 시그널 분류
    if score.composite >= 80:
        score.signal = "STRONG_BUY"
        score.alert_reason = f"종합 {score.composite} — sniper/smart/supply 강력"
    elif score.composite >= 65:
        score.signal = "BUY"
    elif score.composite >= 45:
        score.signal = "HOLD"
    elif score.composite >= 30:
        score.signal = "WEAK"
        score.alert_reason = "수급 약화 / 추세 둔화"
    else:
        score.signal = "SELL"
        score.alert_reason = "수급 반전 / 매도 우세"

    return score


def format_4times_report(scores: Dict[str, IntradayScore], time_label: str) -> str:
    """4번 수급 알람 보고서 (텔레그램 전송용)."""
    if not scores:
        return f"📊 [{time_label}] 수급 추적 — 데이터 없음"

    sorted_scores = sorted(scores.values(), key=lambda x: -x.composite)
    lines = [
        f"📊 [{time_label} 단타봇 실시간 수급 추적]",
        "━━━━━━━━━━━━━━━━━",
        f"종합 점수 TOP {min(len(sorted_scores), 5)}:",
    ]
    for s in sorted_scores[:5]:
        emoji = {"STRONG_BUY": "🟢🟢", "BUY": "🟢", "HOLD": "🟡",
                 "WEAK": "🟠", "SELL": "🔴"}.get(s.signal, "⚪")
        lines.append(
            f"  {emoji} {s.code} {s.name[:8]:<8} {s.composite:>5.1f} [{s.signal:<11}] "
            f"sniper={s.sniper_signal[:6]} sm={s.smart_money_signal[:6]}"
        )
    # 강한 시그널 강조
    strong = [s for s in sorted_scores if s.signal in ("STRONG_BUY", "SELL")]
    if strong:
        lines.append("")
        lines.append("⚡ 핵심 시그널:")
        for s in strong[:3]:
            lines.append(f"  {s.code} {s.name} → {s.signal} ({s.alert_reason})")
    return "\n".join(lines)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
    # Mock 데모
    s = IntradayScore(code="403870", name="HPSP", time="0930",
                      jgis_composite=82.0, sniper_signal="반등임박",
                      trend_score=75.0, flow_score=65.0, orderbook_score=70.0,
                      vwap_position="above", composite=78.5, signal="BUY")
    print(format_4times_report({"403870": s}, "09:30"))
