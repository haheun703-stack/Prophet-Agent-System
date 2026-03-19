# -*- coding: utf-8 -*-
"""
장중 AI Eye 엔진 (Intraday AI Eye)
══════════════════════════════════════
매 5분마다 보유종목 흐름 분석 → ALIVE/WEAKENING/DYING/BOUNCING/BREAKING 판정

3-레이어 분석:
  L1. 가격 구조  (30%) — EMA cross + VWAP + range position + PnL
  L2. 거래량/수급 (35%) — volume trend + strength trend + 가격-거래량 동조
  L3. 모멘텀     (35%) — 가속도 + EMA방향 + 체결강도 절대값

데이터: KIS API fetch_price() 1 call/종목 (가볍게)
버퍼: 종목별 deque(maxlen=78) = 6.5시간 / 5분
"""

import os
import sys
import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional
from collections import deque

logger = logging.getLogger("BH.IntradayEye")

# ════════════════════════════════════════════
# 데이터 구조
# ════════════════════════════════════════════

@dataclass
class IntradayBar:
    """5분봉 1개"""
    timestamp: datetime
    price: int
    open_price: int
    high: int
    low: int
    volume: int           # 5분간 순증 거래량
    cum_volume: int       # 누적 거래량 (VWAP 계산용)
    strength: float       # 체결강도
    change_rate: float    # 전일대비 등락률


@dataclass
class EyeVerdict:
    """AI Eye 분석 결과"""
    code: str
    name: str
    timestamp: datetime

    # 레이어별 점수 (각 0~100)
    l1_price_structure: float
    l2_volume_supply: float
    l3_momentum: float

    # 종합
    composite_score: float
    verdict: str          # ALIVE/WEAKENING/DYING/BOUNCING/BREAKING
    prev_verdict: str     # 이전 판정 (전이 감지)
    confidence: float     # 0~1
    summary: str          # 1줄 요약


# ════════════════════════════════════════════
# IntradayBuffer — 종목별 5분봉 메모리
# ════════════════════════════════════════════

class IntradayBuffer:
    """종목별 5분봉 deque — 최대 78개 (6.5시간/5분)"""

    def __init__(self, code: str, name: str = ""):
        self.code = code
        self.name = name
        self.bars: deque = deque(maxlen=78)
        self._prev_cum_volume: int = 0  # 이전 누적 거래량

    def add_bar(self, price_data: dict) -> Optional[IntradayBar]:
        """fetch_price 결과 → IntradayBar 생성 + 추가

        price_data: {current_price, change_rate, volume, high, low, open, strength}
        """
        cum_vol = price_data.get("volume", 0)
        # 순증 거래량 = 현재 누적 - 이전 누적
        delta_vol = max(0, cum_vol - self._prev_cum_volume) if self._prev_cum_volume > 0 else 0
        self._prev_cum_volume = cum_vol

        bar = IntradayBar(
            timestamp=datetime.now(),
            price=price_data.get("current_price", 0),
            open_price=price_data.get("open", 0),
            high=price_data.get("high", 0),
            low=price_data.get("low", 0),
            volume=delta_vol,
            cum_volume=cum_vol,
            strength=price_data.get("strength", 0.0),
            change_rate=price_data.get("change_rate", 0.0),
        )
        self.bars.append(bar)
        return bar

    @property
    def count(self) -> int:
        return len(self.bars)

    @property
    def latest(self) -> Optional[IntradayBar]:
        return self.bars[-1] if self.bars else None

    def prices(self, n: int = 0) -> List[int]:
        """최근 n개 가격 리스트 (n=0이면 전체)"""
        bars = list(self.bars)[-n:] if n > 0 else list(self.bars)
        return [b.price for b in bars]

    def volumes(self, n: int = 0) -> List[int]:
        """최근 n개 거래량 리스트"""
        bars = list(self.bars)[-n:] if n > 0 else list(self.bars)
        return [b.volume for b in bars]

    def strengths(self, n: int = 0) -> List[float]:
        """최근 n개 체결강도 리스트"""
        bars = list(self.bars)[-n:] if n > 0 else list(self.bars)
        return [b.strength for b in bars]

    # ── EMA 계산 (list 기반) ──

    def ema(self, period: int) -> List[float]:
        """EMA 계산 — 최소 period개 bar 필요, 부족시 SMA로 대체"""
        prices = self.prices()
        if len(prices) < 2:
            return prices[:] if prices else []

        if len(prices) < period:
            # SMA로 대체
            avg = sum(prices) / len(prices)
            return [avg] * len(prices)

        k = 2 / (period + 1)
        result = [float(prices[0])]
        for i in range(1, len(prices)):
            result.append(prices[i] * k + result[-1] * (1 - k))
        return result

    def vwap(self) -> float:
        """VWAP = sum(price × volume) / sum(volume)"""
        bars = list(self.bars)
        if not bars:
            return 0.0

        total_pv = sum(b.price * b.volume for b in bars if b.volume > 0)
        total_v = sum(b.volume for b in bars if b.volume > 0)
        return total_pv / total_v if total_v > 0 else float(bars[-1].price)

    def volume_trend(self, window: int = 3) -> float:
        """최근 window봉 평균 거래량 / 전체 평균 거래량 (비율)"""
        vols = self.volumes()
        if len(vols) < window + 1:
            return 1.0

        recent_avg = sum(vols[-window:]) / window
        total_avg = sum(vols) / len(vols)
        return recent_avg / total_avg if total_avg > 0 else 1.0

    def price_slope(self, window: int = 3) -> float:
        """최근 window봉 가격 변화율 (% per bar) — 단순 선형 기울기"""
        prices = self.prices(window)
        if len(prices) < 2:
            return 0.0

        base = prices[0]
        if base <= 0:
            return 0.0

        return (prices[-1] - prices[0]) / base * 100 / (len(prices) - 1)

    def strength_trend(self, window: int = 3) -> float:
        """최근 window봉 체결강도 변화 (최근 평균 - 이전 평균)"""
        strs = self.strengths()
        if len(strs) < window * 2:
            return 0.0

        recent = sum(strs[-window:]) / window
        prev = sum(strs[-window*2:-window]) / window
        return recent - prev


# ════════════════════════════════════════════
# 3-Layer 분석 엔진
# ════════════════════════════════════════════

def _clamp(val: float, lo: float = 0.0, hi: float = 100.0) -> float:
    return max(lo, min(hi, val))


class LayerEngine:
    """3-Layer 분석"""

    # ── Layer 1: 가격 구조 (0~100) ──

    @staticmethod
    def score_price_structure(buf: IntradayBuffer, entry_price: int = 0) -> float:
        """
        EMA(5) vs EMA(20): 0~30
        VWAP 대비:         0~25
        고/저 위치:         0~20
        진입가 PnL:         0~25
        """
        if buf.count < 2:
            return 50.0  # warmup — 중립

        price = buf.latest.price
        score = 0.0

        # (1) EMA(5) vs EMA(20) — 0~30
        ema5 = buf.ema(5)
        ema20 = buf.ema(20)
        if ema5 and ema20:
            e5 = ema5[-1]
            e20 = ema20[-1]
            if e20 > 0:
                gap_pct = (e5 - e20) / e20 * 100
                if gap_pct >= 1.0:
                    score += 30
                elif gap_pct >= 0.3:
                    score += 22
                elif gap_pct >= 0:
                    score += 15
                elif gap_pct >= -0.3:
                    score += 8
                else:
                    score += 0

        # (2) VWAP 대비 — 0~25
        vwap = buf.vwap()
        if vwap > 0:
            vwap_pct = (price - vwap) / vwap * 100
            if vwap_pct >= 1.0:
                score += 25
            elif vwap_pct >= 0.3:
                score += 18
            elif vwap_pct >= 0:
                score += 12
            elif vwap_pct >= -0.5:
                score += 6
            else:
                score += 0

        # (3) 당일 고/저 대비 위치 — 0~20
        high = buf.latest.high
        low = buf.latest.low
        if high > low:
            position = (price - low) / (high - low)
            score += position * 20
        else:
            score += 10  # 고=저 (시가=현재가 = 보합)

        # (4) 진입가 대비 수익률 — 0~25
        if entry_price > 0:
            pnl_pct = (price - entry_price) / entry_price * 100
            if pnl_pct >= 5:
                score += 25
            elif pnl_pct >= 3:
                score += 20
            elif pnl_pct >= 1:
                score += 17
            elif pnl_pct >= 0:
                score += 15
            elif pnl_pct >= -1:
                score += 10
            elif pnl_pct >= -3:
                score += 5
            else:
                score += 0
        else:
            score += 12  # entry 없으면 중립

        return _clamp(score)

    # ── Layer 2: 거래량/수급 (0~100) ──

    @staticmethod
    def score_volume_supply(buf: IntradayBuffer) -> float:
        """
        거래량 트렌드:     0~35
        체결강도 트렌드:   0~30
        가격-거래량 동조:  0~35
        """
        if buf.count < 3:
            return 50.0  # warmup

        score = 0.0

        # (1) 최근 3봉 거래량 vs 전체 평균 — 0~35
        vt = buf.volume_trend(3)
        if vt >= 2.0:
            score += 35
        elif vt >= 1.5:
            score += 28
        elif vt >= 1.0:
            score += 20
        elif vt >= 0.7:
            score += 12
        elif vt >= 0.5:
            score += 5
        else:
            score += 0

        # (2) 체결강도 트렌드 — 0~30
        str_trend = buf.strength_trend(3)
        latest_str = buf.latest.strength

        # 절대값 가산
        if latest_str >= 120:
            score += 15
        elif latest_str >= 100:
            score += 10
        elif latest_str >= 80:
            score += 5

        # 트렌드 가산
        if str_trend >= 10:
            score += 15
        elif str_trend >= 3:
            score += 10
        elif str_trend >= 0:
            score += 5
        else:
            score += 0

        # (3) 가격-거래량 동조 — 0~35
        price_slope = buf.price_slope(3)
        vol_trend = buf.volume_trend(3)

        if price_slope > 0 and vol_trend >= 1.2:
            # 가격↑ + 거래량↑ = 건강한 상승
            score += 35
        elif price_slope > 0 and vol_trend < 0.8:
            # 가격↑ + 거래량↓ = 의심스러운 상승
            score += 15
        elif price_slope <= 0 and vol_trend >= 1.5:
            # 가격↓ + 거래량↑ = 투매/패닉 (약간 점수)
            score += 10
        elif price_slope <= 0 and vol_trend < 0.8:
            # 가격↓ + 거래량↓ = 수급 이탈 (최악)
            score += 0
        else:
            score += 18  # 중립

        return _clamp(score)

    # ── Layer 3: 모멘텀 (0~100) ──

    @staticmethod
    def score_momentum(buf: IntradayBuffer) -> float:
        """
        가격 변화 가속도:  0~40
        EMA(5)방향×위치:   0~30
        체결강도 절대값:   0~30
        """
        if buf.count < 4:
            return 50.0  # warmup

        score = 0.0

        # (1) 가속도 — 최근 3봉 기울기 vs 이전 3봉 기울기 — 0~40
        prices = buf.prices()
        if len(prices) >= 6:
            recent_slope = buf.price_slope(3)
            # 이전 3봉 기울기
            prev_prices = prices[-6:-3]
            if prev_prices and prev_prices[0] > 0:
                prev_slope = (prev_prices[-1] - prev_prices[0]) / prev_prices[0] * 100 / max(1, len(prev_prices) - 1)
            else:
                prev_slope = 0

            accel = recent_slope - prev_slope
            if accel >= 0.5:
                score += 40  # 가속 상승
            elif accel >= 0.1:
                score += 30
            elif accel >= -0.1:
                score += 20  # 유지
            elif accel >= -0.5:
                score += 10
            else:
                score += 0   # 급감속
        else:
            score += 20

        # (2) EMA(5) 방향 × 가격 위치 — 0~30
        ema5 = buf.ema(5)
        if len(ema5) >= 3:
            ema_direction = ema5[-1] - ema5[-3]
            price = buf.latest.price
            above_ema = price > ema5[-1]

            if ema_direction > 0 and above_ema:
                score += 30  # EMA↑ + 가격>EMA
            elif ema_direction > 0 and not above_ema:
                score += 18  # EMA↑ 근데 가격<EMA (조정)
            elif ema_direction <= 0 and above_ema:
                score += 15  # EMA↓ 근데 가격>EMA (반등?)
            else:
                score += 0   # EMA↓ + 가격<EMA
        else:
            score += 15

        # (3) 체결강도 절대값 — 0~30
        latest_str = buf.latest.strength
        if latest_str >= 120:
            score += 30
        elif latest_str >= 100:
            score += 20
        elif latest_str >= 80:
            score += 10
        elif latest_str >= 60:
            score += 5
        else:
            score += 0

        return _clamp(score)


# ════════════════════════════════════════════
# Verdict Engine — 판정
# ════════════════════════════════════════════

class VerdictEngine:
    """Rule-based → Score Fallback 하이브리드 판정"""

    @staticmethod
    def judge(buf: IntradayBuffer, l1: float, l2: float, l3: float,
              prev_verdict: str) -> tuple:
        """
        Returns: (verdict, confidence, summary)
        """
        composite = l1 * 0.30 + l2 * 0.35 + l3 * 0.35

        # warmup 기간 (5봉 미만)
        if buf.count < 5:
            return "WARMUP", 0.3, f"워밍업 중 ({buf.count}/5봉)"

        price = buf.latest.price
        vwap = buf.vwap()

        # ── 1차: 룰 기반 (명확한 케이스) ──

        # BREAKING: EMA(5) 골든크로스 + 거래량 급증
        ema5 = buf.ema(5)
        ema20 = buf.ema(20)
        if len(ema5) >= 2 and len(ema20) >= 2:
            # 직전 봉에서 EMA5 < EMA20이었는데 현재 EMA5 > EMA20
            prev_cross = ema5[-2] < ema20[-2]
            curr_cross = ema5[-1] > ema20[-1]
            vol_surge = buf.volume_trend(3) >= 1.5

            if prev_cross and curr_cross and vol_surge:
                return "BREAKING", 0.85, "EMA(5) 골든크로스 + 거래량 급증"

            # 이미 BREAKING 상태에서 유지
            if curr_cross and vol_surge and composite >= 75:
                if prev_verdict == "BREAKING":
                    return "BREAKING", 0.80, "돌파 지속 중"

        # DYING: 가격 < VWAP + 거래량 감소 + 체결강도 약화
        if vwap > 0 and price < vwap:
            vol_fading = buf.volume_trend(3) < 0.7
            weak_strength = buf.latest.strength < 80

            if vol_fading and weak_strength and composite < 40:
                return "DYING", 0.85, f"VWAP 하회 + 거래량 감소 + 체결강도 {buf.latest.strength:.0f}"

        # BOUNCING: 이전 DYING에서 반등 시도
        if prev_verdict in ("DYING", "BOUNCING"):
            recent_slope = buf.price_slope(3)
            vol_up = buf.volume_trend(3) >= 1.0

            if recent_slope > 0.1 and vol_up and composite >= 45:
                return "BOUNCING", 0.70, "반등 시도 (가격↑ + 거래량 회복)"

        # ── 2차: 점수 기반 (나머지) ──

        if composite >= 70:
            return "ALIVE", 0.75, f"건강 (점수 {composite:.0f})"
        elif composite >= 45:
            # WEAKENING 세부 진단
            reasons = []
            if l2 < 40:
                reasons.append("거래량 감소")
            if l3 < 40:
                reasons.append("모멘텀 약화")
            if buf.latest.strength < 90:
                reasons.append(f"체결강도 {buf.latest.strength:.0f}")
            summary = ", ".join(reasons) if reasons else f"약화 중 (점수 {composite:.0f})"
            return "WEAKENING", 0.65, summary
        else:
            reasons = []
            if l2 < 30:
                reasons.append("거래량+가격 동반 하락")
            if buf.latest.strength < 80:
                reasons.append(f"체결강도 {buf.latest.strength:.0f}")
            summary = ", ".join(reasons) if reasons else f"위험 (점수 {composite:.0f})"
            return "DYING", 0.75, summary


# ════════════════════════════════════════════
# IntradayEye — 메인 엔진
# ════════════════════════════════════════════

class IntradayEye:
    """장중 AI Eye — fetch_price → buffer → 3-layer → verdict"""

    def __init__(self, trader=None):
        """
        trader: kis_trader.KISTrader 인스턴스 (fetch_price 호출용)
        """
        self.trader = trader
        self._buffers: Dict[str, IntradayBuffer] = {}
        self._prev_verdicts: Dict[str, str] = {}  # code → 이전 verdict
        self._names: Dict[str, str] = {}  # code → name 캐시

    def _get_buffer(self, code: str) -> IntradayBuffer:
        if code not in self._buffers:
            name = self._names.get(code, code)
            self._buffers[code] = IntradayBuffer(code, name)
        return self._buffers[code]

    def set_name(self, code: str, name: str):
        """종목명 설정"""
        self._names[code] = name
        if code in self._buffers:
            self._buffers[code].name = name

    def evaluate(self, code: str, entry_price: int = 0,
                 price_data: dict = None) -> Optional[EyeVerdict]:
        """1종목 평가

        price_data가 없으면 self.trader.fetch_price() 호출
        Returns: EyeVerdict or None (API 실패/warmup)
        """
        # 데이터 수집
        if price_data is None:
            if self.trader is None:
                logger.warning(f"trader 미설정, {code} 평가 불가")
                return None
            resp = self.trader.fetch_price(code)
            if not resp.get("success"):
                logger.warning(f"fetch_price 실패 {code}: {resp.get('message')}")
                return None
            price_data = resp

        # 버퍼에 추가
        buf = self._get_buffer(code)
        buf.add_bar(price_data)

        # 3-Layer 스코어링
        l1 = LayerEngine.score_price_structure(buf, entry_price)
        l2 = LayerEngine.score_volume_supply(buf)
        l3 = LayerEngine.score_momentum(buf)

        composite = l1 * 0.30 + l2 * 0.35 + l3 * 0.35

        # 판정
        prev_verdict = self._prev_verdicts.get(code, "WARMUP")
        verdict, confidence, summary = VerdictEngine.judge(buf, l1, l2, l3, prev_verdict)

        # 이전 verdict 업데이트
        self._prev_verdicts[code] = verdict

        name = self._names.get(code, code)

        return EyeVerdict(
            code=code,
            name=name,
            timestamp=datetime.now(),
            l1_price_structure=round(l1, 1),
            l2_volume_supply=round(l2, 1),
            l3_momentum=round(l3, 1),
            composite_score=round(composite, 1),
            verdict=verdict,
            prev_verdict=prev_verdict,
            confidence=round(confidence, 2),
            summary=summary,
        )

    def clear(self, code: str = None):
        """버퍼 초기화 (장 시작 시 또는 특정 종목)"""
        if code:
            self._buffers.pop(code, None)
            self._prev_verdicts.pop(code, None)
        else:
            self._buffers.clear()
            self._prev_verdicts.clear()

    def get_status(self) -> Dict[str, dict]:
        """전체 종목 Eye 상태 요약"""
        result = {}
        for code, buf in self._buffers.items():
            verdict = self._prev_verdicts.get(code, "WARMUP")
            result[code] = {
                "name": buf.name,
                "bars": buf.count,
                "verdict": verdict,
                "latest_price": buf.latest.price if buf.latest else 0,
            }
        return result


# ════════════════════════════════════════════
# --test 모드
# ════════════════════════════════════════════

def _test():
    """테스트: 지정 종목에 대해 1회 fetch_price → 평가 결과 출력"""
    import json

    # 프로젝트 루트 설정
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    sys.path.insert(0, project_root)
    os.chdir(project_root)

    from dotenv import load_dotenv
    parent_env = os.path.join(os.path.dirname(project_root), ".env")
    load_dotenv(parent_env)

    from bot.kis_trader import KISTrader
    trader = KISTrader()

    eye = IntradayEye(trader)

    # 테스트 종목 (현재 보유종목 or 하드코딩)
    test_codes = []

    # 포지션 파일에서 로드 시도
    pos_file = os.path.join(project_root, "data_store", "positions.json")
    if os.path.exists(pos_file):
        try:
            with open(pos_file, "r", encoding="utf-8") as f:
                positions = json.load(f)
            for code, info in positions.items():
                name = info.get("name", code)
                entry = info.get("entry_price", 0)
                eye.set_name(code, name)
                test_codes.append((code, name, entry))
        except Exception as e:
            logger.warning(f"포지션 파일 로드 실패: {e}")

    if not test_codes:
        # 기본 테스트 종목
        test_codes = [
            ("005930", "삼성전자", 0),
            ("000270", "기아", 0),
        ]
        for code, name, _ in test_codes:
            eye.set_name(code, name)

    print("=" * 60)
    print("  Intraday AI Eye — 테스트 (1회 평가)")
    print("=" * 60)

    for code, name, entry in test_codes:
        print(f"\n{'─' * 50}")
        print(f"  {name} ({code}) | 진입가: {entry:,}" if entry else f"  {name} ({code})")
        print(f"{'─' * 50}")

        verdict = eye.evaluate(code, entry_price=entry)
        if verdict is None:
            print("  [실패] API 호출 실패")
            continue

        print(f"  판정: {verdict.verdict} (신뢰도 {verdict.confidence:.0%})")
        print(f"  점수: {verdict.composite_score:.1f} (L1:{verdict.l1_price_structure:.0f} L2:{verdict.l2_volume_supply:.0f} L3:{verdict.l3_momentum:.0f})")
        print(f"  사유: {verdict.summary}")

        # 버퍼 상태
        buf = eye._get_buffer(code)
        if buf.latest:
            print(f"  현재가: {buf.latest.price:,} | 등락: {buf.latest.change_rate:+.2f}% | 강도: {buf.latest.strength:.0f}")
            print(f"  VWAP: {buf.vwap():,.0f} | 버퍼: {buf.count}봉")

    print(f"\n{'=' * 60}")
    print("  (첫 실행은 1봉이므로 warmup 상태가 정상입니다)")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    if "--test" in sys.argv:
        _test()
    else:
        print("사용법: python data/intraday_eye.py --test")
