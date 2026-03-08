# -*- coding: utf-8 -*-
"""
포지션 어드바이저 — 멀티팩터 방향 판단 + 행동 추천
====================================================
"내일 오를까? 지금 팔까? 들고 갈까?"
→ 데이터 기반 8팩터 스코어링 → STRONG_BUY ~ STRONG_SELL

사용법 (CLI):
    python strategies/position_advisor.py 103140          # 풍산 분석
    python strategies/position_advisor.py 003570 028670   # 여러 종목
    python strategies/position_advisor.py --all            # 워치리스트 전체

사용법 (코드):
    from strategies.position_advisor import PositionAdvisor
    advisor = PositionAdvisor()
    result = advisor.analyze("103140")
    print(result["action"], result["score"], result["reasons"])
"""

import sys
import json
import logging
import numpy as np
import pandas as pd
from pathlib import Path
from dataclasses import dataclass, field
from typing import Optional

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

logger = logging.getLogger("BH.Advisor")


# ═══════════════════════════════════════════
#  결과 데이터
# ═══════════════════════════════════════════
@dataclass
class AdvisorResult:
    code: str
    name: str
    current_price: int = 0
    score: float = 0.0            # -100 ~ +100
    action: str = "HOLD"          # STRONG_BUY / BUY / HOLD / SELL / STRONG_SELL
    confidence: str = "LOW"       # HIGH / MED / LOW
    factors: dict = field(default_factory=dict)   # factor_name -> {score, signal, detail}
    reasons: list = field(default_factory=list)    # 상위 3개 핵심 이유
    levels: dict = field(default_factory=dict)     # support, resistance, sl, tp
    summary: str = ""             # 한줄 요약

    def to_telegram(self) -> str:
        """텔레그램용 포맷"""
        action_icon = {
            "STRONG_BUY": "++ 적극매수 ++",
            "BUY": "+ 매수 검토 +",
            "HOLD": "= 관망 =",
            "SELL": "- 매도 검토 -",
            "STRONG_SELL": "-- 적극매도 --",
        }
        lines = [
            f"[{action_icon.get(self.action, self.action)}] {self.name}({self.code})",
            f"현재 {self.current_price:,} | 스코어 {self.score:+.0f} ({self.confidence})",
            "",
        ]

        # 팩터별 시그널
        for fname, fdata in self.factors.items():
            s = fdata["score"]
            icon = "+" if s > 3 else ("-" if s < -3 else "=")
            lines.append(f" [{icon}] {fname}: {fdata['detail']}")

        lines.append("")

        # 핵심 이유
        if self.reasons:
            lines.append("핵심:")
            for r in self.reasons[:3]:
                lines.append(f"  > {r}")

        # 가격 레벨
        if self.levels:
            lines.append("")
            lvl = self.levels
            parts = []
            if lvl.get("resistance"):
                parts.append(f"저항 {lvl['resistance']:,}")
            parts.append(f"현재 {self.current_price:,}")
            if lvl.get("support"):
                parts.append(f"지지 {lvl['support']:,}")
            if lvl.get("sl"):
                parts.append(f"SL {lvl['sl']:,}")
            lines.append(" | ".join(parts))

        return "\n".join(lines)


# ═══════════════════════════════════════════
#  메인 어드바이저
# ═══════════════════════════════════════════
class PositionAdvisor:
    """8팩터 멀티 스코어링 방향 판단 엔진"""

    def __init__(self):
        self.daily_dir = BASE_DIR / "data_store" / "daily"

    def analyze(self, code: str, name: str = "",
                intraday_vwap: float = 0, intraday_price: float = 0,
                holding: bool = False, avg_price: float = 0) -> AdvisorResult:
        """
        종목 분석 → 행동 추천

        Args:
            code: 종목코드
            name: 종목명
            intraday_vwap: 장중 VWAP (0이면 일봉만 분석)
            intraday_price: 장중 현재가
            holding: 보유 중 여부
            avg_price: 평균 매입가 (보유 중일 때)
        """
        result = AdvisorResult(code=code, name=name or code)

        # 1. 일봉 데이터 로드
        df = self._load_daily(code)
        if df is None or len(df) < 20:
            result.summary = "데이터 부족"
            return result

        close = df["close"].values.astype(float)
        high = df["high"].values.astype(float)
        low = df["low"].values.astype(float)
        opn = df["open"].values.astype(float)
        vol = df["volume"].values.astype(float)

        current = int(close[-1])
        if intraday_price > 0:
            current = int(intraday_price)
        result.current_price = current

        # 2. 8팩터 스코어링
        factors = {}
        factors["추세"] = self._score_trend(close, current)
        factors["RSI"] = self._score_rsi(close)
        factors["MACD"] = self._score_macd(close)
        factors["볼린저"] = self._score_bollinger(close, current)
        factors["거래량"] = self._score_volume(close, vol)
        factors["캔들"] = self._score_candle(opn, close, high, low)
        factors["지지저항"] = self._score_support_resistance(close, high, low, current)
        factors["VWAP"] = self._score_vwap(current, intraday_vwap)

        result.factors = factors

        # 3. 총점 계산 (가중 평균)
        weights = {
            "추세": 1.5, "RSI": 1.0, "MACD": 1.2, "볼린저": 1.0,
            "거래량": 1.3, "캔들": 0.8, "지지저항": 1.2, "VWAP": 1.0,
        }
        total_weight = sum(weights.values())
        raw_score = sum(factors[k]["score"] * weights[k] for k in factors)
        normalized = (raw_score / total_weight) * 10  # -100 ~ +100 범위로
        result.score = max(-100, min(100, normalized))

        # 4. 행동 결정
        result.action = self._decide_action(result.score, holding, avg_price, current)

        # 5. 신뢰도
        bull_count = sum(1 for f in factors.values() if f["score"] > 3)
        bear_count = sum(1 for f in factors.values() if f["score"] < -3)
        agreement = max(bull_count, bear_count)
        if agreement >= 6:
            result.confidence = "HIGH"
        elif agreement >= 4:
            result.confidence = "MED"
        else:
            result.confidence = "LOW"

        # 6. 핵심 이유 추출 (절대값 큰 순서)
        sorted_factors = sorted(factors.items(), key=lambda x: abs(x[1]["score"]), reverse=True)
        result.reasons = [f"{k}: {v['detail']}" for k, v in sorted_factors[:3]]

        # 7. 가격 레벨
        result.levels = self._calc_levels(close, high, low, current)

        # 8. 한줄 요약
        result.summary = self._make_summary(result)

        return result

    # ─── 팩터 1: 추세 (MA) ───
    def _score_trend(self, close, current) -> dict:
        ma5 = pd.Series(close).rolling(5).mean().iloc[-1]
        ma20 = pd.Series(close).rolling(20).mean().iloc[-1]
        ma60 = pd.Series(close).rolling(60).mean().iloc[-1] if len(close) >= 60 else ma20

        score = 0
        details = []

        # 정배열 / 역배열
        if current > ma5 > ma20:
            score += 6
            details.append("정배열")
        elif current < ma5 < ma20:
            score -= 6
            details.append("역배열")
        elif current > ma5 and ma5 < ma20:
            score += 2
            details.append("회복중")
        elif current < ma5 and ma5 > ma20:
            score -= 2
            details.append("꺾이는중")

        # MA20 대비 위치
        gap_ma20 = (current / ma20 - 1) * 100
        if gap_ma20 > 10:
            score -= 3
            details.append(f"MA20 +{gap_ma20:.0f}% 과이격")
        elif gap_ma20 < -10:
            score += 3
            details.append(f"MA20 {gap_ma20:.0f}% 이격 반등기대")

        # 최근 3일 방향
        if close[-1] > close[-4]:
            score += 2
            details.append("3일 상승")
        else:
            score -= 2
            details.append("3일 하락")

        return {"score": max(-10, min(10, score)), "signal": "BULL" if score > 0 else "BEAR", "detail": ", ".join(details)}

    # ─── 팩터 2: RSI ───
    def _score_rsi(self, close) -> dict:
        delta = pd.Series(close).diff()
        gain = delta.where(delta > 0, 0).rolling(14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
        rs = gain / loss
        rsi = float((100 - (100 / (1 + rs))).iloc[-1])

        if rsi < 25:
            return {"score": 10, "signal": "BULL", "detail": f"RSI {rsi:.0f} 극과매도"}
        elif rsi < 30:
            return {"score": 6, "signal": "BULL", "detail": f"RSI {rsi:.0f} 과매도"}
        elif rsi < 40:
            return {"score": 2, "signal": "BULL", "detail": f"RSI {rsi:.0f} 약세권"}
        elif rsi < 60:
            return {"score": 0, "signal": "NEUTRAL", "detail": f"RSI {rsi:.0f} 중립"}
        elif rsi < 70:
            return {"score": -1, "signal": "NEUTRAL", "detail": f"RSI {rsi:.0f} 강세"}
        elif rsi < 80:
            return {"score": -5, "signal": "BEAR", "detail": f"RSI {rsi:.0f} 과열주의"}
        else:
            return {"score": -10, "signal": "BEAR", "detail": f"RSI {rsi:.0f} 극과열"}

    # ─── 팩터 3: MACD ───
    def _score_macd(self, close) -> dict:
        s = pd.Series(close)
        ema12 = s.ewm(span=12).mean()
        ema26 = s.ewm(span=26).mean()
        macd_line = ema12 - ema26
        signal_line = macd_line.ewm(span=9).mean()
        histogram = macd_line - signal_line

        macd_val = float(macd_line.iloc[-1])
        sig_val = float(signal_line.iloc[-1])
        hist_now = float(histogram.iloc[-1])
        hist_prev = float(histogram.iloc[-2]) if len(histogram) > 1 else 0

        score = 0
        details = []

        # 골든/데드 크로스
        if macd_val > sig_val:
            score += 4
            details.append("골든크로스")
        else:
            score -= 4
            details.append("데드크로스")

        # 히스토그램 방향
        if hist_now > hist_prev:
            score += 3
            details.append("히스토 확대")
        else:
            score -= 3
            details.append("히스토 축소")

        # 0선 위/아래
        if macd_val > 0:
            score += 2
            details.append("0선 위")
        else:
            score -= 2
            details.append("0선 아래")

        return {"score": max(-10, min(10, score)), "signal": "BULL" if score > 0 else "BEAR", "detail": ", ".join(details)}

    # ─── 팩터 4: 볼린저 밴드 ───
    def _score_bollinger(self, close, current) -> dict:
        s = pd.Series(close)
        ma20 = float(s.rolling(20).mean().iloc[-1])
        std20 = float(s.rolling(20).std().iloc[-1])
        upper = ma20 + 2 * std20
        lower = ma20 - 2 * std20

        # %B 계산 (0=하단, 0.5=중앙, 1=상단)
        pct_b = (current - lower) / (upper - lower) if upper != lower else 0.5

        if pct_b < 0:
            return {"score": 8, "signal": "BULL", "detail": f"하단 이탈 (%B={pct_b:.2f}) 반등기대"}
        elif pct_b < 0.2:
            return {"score": 5, "signal": "BULL", "detail": f"하단 근접 (%B={pct_b:.2f})"}
        elif pct_b < 0.4:
            return {"score": 2, "signal": "NEUTRAL", "detail": f"하단권 (%B={pct_b:.2f})"}
        elif pct_b < 0.6:
            return {"score": 0, "signal": "NEUTRAL", "detail": f"중앙 (%B={pct_b:.2f})"}
        elif pct_b < 0.8:
            return {"score": -2, "signal": "NEUTRAL", "detail": f"상단권 (%B={pct_b:.2f})"}
        elif pct_b < 1.0:
            return {"score": -5, "signal": "BEAR", "detail": f"상단 근접 (%B={pct_b:.2f}) 저항"}
        else:
            return {"score": -8, "signal": "BEAR", "detail": f"상단 이탈 (%B={pct_b:.2f}) 과열"}

    # ─── 팩터 5: 거래량 ───
    def _score_volume(self, close, vol) -> dict:
        vol_ma20 = float(pd.Series(vol).rolling(20).mean().iloc[-1])
        vol_ratio = vol[-1] / vol_ma20 if vol_ma20 > 0 else 1.0

        # 최근 3일 거래량 추세
        v3 = vol[-3:]
        vol_declining = v3[0] > v3[1] > v3[2]
        vol_rising = v3[0] < v3[1] < v3[2]

        # 가격 방향
        price_up = close[-1] > close[-3]

        score = 0
        details = []

        if price_up and vol_rising:
            score = 8
            details.append("가격상승+거래량증가 (강한매수)")
        elif price_up and vol_declining:
            score = -4
            details.append("가격상승+거래량감소 (약한랠리)")
        elif not price_up and vol_rising:
            score = -8
            details.append("가격하락+거래량증가 (강한매도)")
        elif not price_up and vol_declining:
            score = 3
            details.append("가격하락+거래량감소 (매도소진)")
        else:
            details.append("혼조")

        # 거래량 비율
        if vol_ratio > 3:
            details.append(f"거래폭발 {vol_ratio:.1f}x")
        elif vol_ratio > 1.5:
            details.append(f"거래활발 {vol_ratio:.1f}x")
        elif vol_ratio < 0.5:
            score -= 2
            details.append(f"거래부진 {vol_ratio:.1f}x")

        return {"score": max(-10, min(10, score)), "signal": "BULL" if score > 0 else "BEAR", "detail": ", ".join(details)}

    # ─── 팩터 6: 캔들 패턴 ───
    def _score_candle(self, opn, close, high, low) -> dict:
        score = 0
        details = []

        # 최근 3일 양봉/음봉
        bullish = sum(1 for i in range(-3, 0) if close[i] > opn[i])
        if bullish >= 3:
            score += 6
            details.append("3연속 양봉")
        elif bullish == 2:
            score += 2
            details.append(f"양봉 {bullish}/3")
        elif bullish == 1:
            score -= 2
            details.append(f"양봉 {bullish}/3")
        else:
            score -= 6
            details.append("3연속 음봉")

        # 마지막 캔들 분석
        body = abs(close[-1] - opn[-1])
        wick_up = high[-1] - max(close[-1], opn[-1])
        wick_dn = min(close[-1], opn[-1]) - low[-1]
        total_range = high[-1] - low[-1]

        if total_range > 0:
            # 해머 (긴 아래꼬리 + 작은 몸통) → 바닥 반전
            if wick_dn > body * 2 and wick_up < body:
                score += 4
                details.append("해머 (반전신호)")
            # 유성 (긴 윗꼬리 + 작은 몸통) → 고점 반전
            elif wick_up > body * 2 and wick_dn < body:
                score -= 4
                details.append("유성 (하락신호)")
            # 도지 (아주 작은 몸통)
            elif body < total_range * 0.1:
                details.append("도지 (방향탐색)")
            # 장대양봉
            elif close[-1] > opn[-1] and body > total_range * 0.7:
                score += 3
                details.append("장대양봉")
            # 장대음봉
            elif close[-1] < opn[-1] and body > total_range * 0.7:
                score -= 3
                details.append("장대음봉")

        return {"score": max(-10, min(10, score)), "signal": "BULL" if score > 0 else "BEAR", "detail": ", ".join(details)}

    # ─── 팩터 7: 지지/저항 ───
    def _score_support_resistance(self, close, high, low, current) -> dict:
        # 최근 20일 고/저
        h20 = float(np.max(high[-20:]))
        l20 = float(np.min(low[-20:]))
        # 최근 5일 저가 = 단기 지지
        l5 = float(np.min(low[-5:]))

        score = 0
        details = []

        # 20일 고점 대비
        gap_high = (current / h20 - 1) * 100
        if abs(gap_high) < 2:
            score -= 3
            details.append(f"20일고점 근접 ({h20:,.0f})")
        elif gap_high > 0:
            score += 5
            details.append(f"20일고점 돌파! ({h20:,.0f})")

        # 20일 저점 대비
        gap_low = (current / l20 - 1) * 100
        if abs(gap_low) < 3:
            score += 3
            details.append(f"20일저점 근접 ({l20:,.0f}) 반등기대")
        elif gap_low < 0:
            score -= 5
            details.append(f"20일저점 이탈 ({l20:,.0f})")

        # 5일 저가 지지
        gap_l5 = (current / l5 - 1) * 100
        if gap_l5 < 2:
            details.append(f"단기지지 {l5:,.0f} 근접")

        # 피보나치 위치 (20일 range)
        fib_range = h20 - l20
        if fib_range > 0:
            fib_pos = (current - l20) / fib_range
            if fib_pos < 0.236:
                score += 3
                details.append("Fib 23.6% 이하 (바닥권)")
            elif fib_pos > 0.786:
                score -= 3
                details.append("Fib 78.6% 이상 (고점권)")

        return {"score": max(-10, min(10, score)), "signal": "BULL" if score > 0 else "BEAR", "detail": ", ".join(details)}

    # ─── 팩터 8: VWAP ───
    def _score_vwap(self, current, vwap) -> dict:
        if vwap <= 0:
            return {"score": 0, "signal": "N/A", "detail": "VWAP 데이터 없음 (장중만 가능)"}

        gap = (current / vwap - 1) * 100

        if gap > 2:
            return {"score": -5, "signal": "BEAR", "detail": f"VWAP +{gap:.1f}% 위 (고평가)"}
        elif gap > 0.5:
            return {"score": 3, "signal": "BULL", "detail": f"VWAP +{gap:.1f}% 위 (강세)"}
        elif gap > -0.5:
            return {"score": 0, "signal": "NEUTRAL", "detail": f"VWAP 근접 ({gap:+.1f}%)"}
        elif gap > -2:
            return {"score": -3, "signal": "BEAR", "detail": f"VWAP {gap:.1f}% 아래 (약세)"}
        else:
            return {"score": 5, "signal": "BULL", "detail": f"VWAP {gap:.1f}% 아래 (과매도 반등기대)"}

    # ─── 행동 결정 ───
    def _decide_action(self, score, holding, avg_price, current) -> str:
        if holding and avg_price > 0:
            pnl = (current / avg_price - 1) * 100
            # 보유 중일 때는 더 보수적으로
            if score >= 30:
                return "HOLD"  # 보유 중 + 강세 → 계속 보유
            elif score >= 0:
                return "HOLD"
            elif score >= -20:
                if pnl > 5:
                    return "SELL"  # 약간 약세인데 수익 있으면 익절
                return "HOLD"  # 손실권이면 좀 더 버텨
            elif score >= -40:
                return "SELL"
            else:
                return "STRONG_SELL"
        else:
            # 미보유
            if score >= 50:
                return "STRONG_BUY"
            elif score >= 20:
                return "BUY"
            elif score >= -20:
                return "HOLD"  # = 관망
            elif score >= -50:
                return "SELL"  # = 진입 금지
            else:
                return "STRONG_SELL"  # = 절대 진입 금지

    # ─── 가격 레벨 ───
    def _calc_levels(self, close, high, low, current) -> dict:
        h20 = float(np.max(high[-20:]))
        l20 = float(np.min(low[-20:]))
        l5 = float(np.min(low[-5:]))
        h5 = float(np.max(high[-5:]))

        ma20 = float(pd.Series(close).rolling(20).mean().iloc[-1])

        return {
            "resistance": int(h5),       # 5일 고점 = 단기 저항
            "support": int(max(l5, l20 * 1.02)),  # 5일 저가 or 20일저+2%
            "sl": int(l20 * 0.97),        # 20일 저점 -3%
            "ma20": int(ma20),
            "h20": int(h20),
            "l20": int(l20),
        }

    # ─── 한줄 요약 ───
    def _make_summary(self, result: AdvisorResult) -> str:
        action_kr = {
            "STRONG_BUY": "적극매수",
            "BUY": "매수검토",
            "HOLD": "관망",
            "SELL": "매도검토",
            "STRONG_SELL": "매도",
        }
        a = action_kr.get(result.action, result.action)
        top_reason = result.reasons[0] if result.reasons else ""
        return f"{result.name} → {a} (스코어 {result.score:+.0f}, {result.confidence}) | {top_reason}"

    # ─── 일봉 데이터 로드 ───
    def _load_daily(self, code: str) -> Optional[pd.DataFrame]:
        csv_path = self.daily_dir / f"{code}.csv"
        if not csv_path.exists():
            return None
        try:
            df = pd.read_csv(csv_path)
            df.columns = ["date", "open", "high", "low", "close", "volume", "change"]
            return df
        except Exception as e:
            logger.error(f"일봉 로드 실패 {code}: {e}")
            return None

    # ─── 워치리스트 전체 분석 ───
    def analyze_watchlist(self) -> list[AdvisorResult]:
        """전쟁릴레이 + 추천종목 전체 분석"""
        results = []

        # 전쟁릴레이
        wl_path = BASE_DIR / "data_store" / "war_relay_watchlist.json"
        if wl_path.exists():
            with open(wl_path, "r", encoding="utf-8") as f:
                wl = json.load(f)
            for s in wl.get("stocks", []):
                r = self.analyze(s["code"], s["name"])
                results.append(r)

        # 추천종목
        rec_path = BASE_DIR / "data_store" / "recommendation.json"
        if rec_path.exists():
            with open(rec_path, "r", encoding="utf-8") as f:
                rec = json.load(f)
            existing = {r.code for r in results}
            for s in rec.get("stocks", [])[:5]:
                if s["code"] not in existing:
                    r = self.analyze(s["code"], s["name"])
                    results.append(r)

        # 스코어 순 정렬 (높은 것 = 매수기회)
        results.sort(key=lambda x: x.score, reverse=True)
        return results


# ═══════════════════════════════════════════
#  CLI
# ═══════════════════════════════════════════
def main():
    import argparse
    parser = argparse.ArgumentParser(description="포지션 어드바이저 — 멀티팩터 방향 판단")
    parser.add_argument("codes", nargs="*", help="종목코드 (여러개 가능)")
    parser.add_argument("--all", action="store_true", help="워치리스트 전체 분석")
    parser.add_argument("--telegram", action="store_true", help="텔레그램 전송")
    args = parser.parse_args()

    advisor = PositionAdvisor()

    if args.all:
        results = advisor.analyze_watchlist()
    elif args.codes:
        results = [advisor.analyze(code) for code in args.codes]
    else:
        print("사용법: python strategies/position_advisor.py 103140")
        print("        python strategies/position_advisor.py --all")
        return

    # 출력
    print("=" * 50)
    print("  포지션 어드바이저 - 멀티팩터 방향 판단")
    print("=" * 50)
    print()

    for r in results:
        print(r.to_telegram())
        print("-" * 40)
        print()

    # 요약 테이블
    print("=" * 50)
    print("  요약")
    print("=" * 50)
    for r in results:
        action_icon = {
            "STRONG_BUY": "++", "BUY": " +", "HOLD": " =",
            "SELL": " -", "STRONG_SELL": "--"
        }
        icon = action_icon.get(r.action, "??")
        print(f"  [{icon}] {r.name:>12} {r.current_price:>10,}  스코어 {r.score:>+5.0f}  {r.confidence:>4}  {r.action}")

    # 텔레그램
    if args.telegram:
        from dotenv import load_dotenv
        load_dotenv(BASE_DIR.parent / ".env")
        import os
        token = os.getenv("TELEGRAM_BOT_TOKEN")
        chat = os.getenv("TELEGRAM_CHAT_ID")
        if token and chat:
            lines = ["=== 포지션 어드바이저 ===", ""]
            for r in results:
                lines.append(r.summary)
            msg = "\n".join(lines)

            import requests as req
            req.post(
                f"https://api.telegram.org/bot{token}/sendMessage",
                json={"chat_id": chat, "text": msg}
            )
            print("\n텔레그램 전송 완료")


if __name__ == "__main__":
    main()
