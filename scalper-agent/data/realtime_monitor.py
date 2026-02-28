# -*- coding: utf-8 -*-
"""
장중 AI 모니터 (Real-Time AI Monitor)
══════════════════════════════════════
30초마다 보유 종목 4팩터 분석 → HOLD/SELL/트레일링 자동 결정

4팩터 (각 0~25점, 합산 0~100):
  1. 가격 모멘텀  — 등락률 + 분봉 추세
  2. 거래량 흐름  — 체결량 vs 평균 + 추세
  3. 체결강도     — KIS 체결강도 (100 기준)
  4. 호가 불균형  — 매수/매도호가 비율

결정 엔진:
  - 긴급: price ≤ SL → FULL_SELL
  - 트레일링: 수익 5%+ → SL=진입가 / 10%+ → 트레일링 3%
  - AI 점수 기반: HOLD / PARTIAL_SELL / FULL_SELL
"""

import os
import time
import logging
import requests
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from collections import deque

logger = logging.getLogger("BH.RealtimeMonitor")


# ── 데이터 구조 ──

@dataclass
class RealtimeSnapshot:
    """1회 평가 스냅샷"""
    code: str
    name: str
    timestamp: str

    # 현재가 정보
    price: int
    change_rate: float       # 등락률 %
    volume: int              # 누적거래량
    tick_volume: int          # 구간체결량
    strength: float           # 체결강도
    ask1: int                 # 매도호가1
    bid1: int                 # 매수호가1

    # 4팩터 점수 (각 0~25)
    score_momentum: float
    score_volume: float
    score_strength: float
    score_orderbook: float
    realtime_score: float     # 합산 0~100

    # 포지션 컨텍스트
    entry_price: int
    current_sl: int
    current_tp: int
    pnl_pct: float            # 수익률 %
    high_since_entry: int     # 진입 후 최고가

    # 결정
    decision: str             # HOLD / PARTIAL_SELL / FULL_SELL / TRAILING_STOP
    decision_reason: str


@dataclass
class PositionState:
    """보유 종목 상태 (트레일링/히스토리 관리)"""
    code: str
    name: str
    entry_price: int
    original_sl: int
    original_tp: int
    current_sl: int
    current_tp: int

    high_since_entry: int = 0     # 진입 후 최고가
    breakeven_activated: bool = False  # 본절 SL 발동 여부
    trailing_activated: bool = False   # 트레일링 발동 여부

    # 최근 스냅샷 점수 히스토리 (추세 악화 감지)
    score_history: deque = field(default_factory=lambda: deque(maxlen=10))
    prev_volume: int = 0          # 이전 누적거래량 (체결량 계산)


class RealtimeMonitor:
    """장중 AI 모니터 — KIS API로 4팩터 실시간 분석"""

    def __init__(self, config: dict = None):
        self._broker = None
        self._positions: Dict[str, PositionState] = {}
        self._config = config or {}

        # 설정값
        pm = self._config.get("premove", {})
        bot = self._config.get("bot", {})
        self._breakeven_pct = bot.get("trailing_breakeven_pct", 5.0)
        self._trailing_start_pct = bot.get("trailing_start_pct", 10.0)
        self._trailing_distance_pct = bot.get("trailing_distance_pct", 3.0)

        # API 장애 감지
        self._consecutive_failures = 0
        self._max_failures = 5        # 5회 연속 실패 시 경고
        self._feed_suspended = False   # 데이터 피드 중단 플래그

    # ── 브로커 ──

    def _get_broker(self):
        if self._broker is not None:
            return self._broker
        from dotenv import load_dotenv
        load_dotenv()
        import mojito
        self._broker = mojito.KoreaInvestment(
            api_key=os.getenv("KIS_APP_KEY"),
            api_secret=os.getenv("KIS_APP_SECRET"),
            acc_no=os.getenv("KIS_ACC_NO"),
            mock=False,
        )
        return self._broker

    def _reset_broker(self):
        """브로커 재생성 (토큰 갱신/재연결)"""
        self._broker = None
        logger.info("KIS 브로커 재생성 (토큰 갱신)")
        return self._get_broker()

    # ── 포지션 관리 ──

    def register_position(self, code: str, name: str,
                          entry_price: int, sl: int, tp: int):
        """매수 후 포지션 등록"""
        self._positions[code] = PositionState(
            code=code, name=name,
            entry_price=entry_price,
            original_sl=sl, original_tp=tp,
            current_sl=sl, current_tp=tp,
            high_since_entry=entry_price,
        )
        logger.info(f"포지션 등록: {name}({code}) 진입:{entry_price:,} SL:{sl:,} TP:{tp:,}")

    def unregister_position(self, code: str):
        """매도 후 포지션 해제"""
        pos = self._positions.pop(code, None)
        if pos:
            logger.info(f"포지션 해제: {pos.name}({code})")

    def get_positions(self) -> Dict[str, PositionState]:
        return self._positions.copy()

    # ── KIS API 스냅샷 ──

    def _fetch_snapshot(self, code: str) -> Optional[dict]:
        """1종목 시세 스냅샷 (3-API 조합)"""
        broker = self._get_broker()
        base = broker.base_url
        headers = {
            "content-type": "application/json; charset=utf-8",
            "authorization": broker.access_token,
            "appKey": broker.api_key,
            "appSecret": broker.api_secret,
        }
        params = {
            "fid_cond_mrkt_div_code": "J",
            "fid_input_iscd": code,
        }

        now_str = datetime.now().strftime("%H:%M:%S")
        row = {"time": now_str}

        try:
            # 1) 시세 — 현재가, 전일대비, 등락률, 거래량
            h1 = {**headers, "tr_id": "FHKST01010100"}
            r1 = requests.get(
                f"{base}/uapi/domestic-stock/v1/quotations/inquire-price",
                headers=h1, params=params, timeout=5,
            )
            d1 = r1.json().get("output", {})

            price = int(d1.get("stck_prpr", 0))
            change = int(d1.get("prdy_vrss", 0))
            sign = d1.get("prdy_vrss_sign", "0")
            if sign in ("5", "4"):
                change = -abs(change)
            change_rate = float(d1.get("prdy_ctrt", 0))
            volume = int(d1.get("acml_vol", 0))

            row["price"] = price
            row["change"] = change
            row["change_rate"] = change_rate
            row["volume"] = volume

            time.sleep(0.05)

            # 2) 체결 — 체결강도
            h2 = {**headers, "tr_id": "FHKST01010300"}
            r2 = requests.get(
                f"{base}/uapi/domestic-stock/v1/quotations/inquire-ccnl",
                headers=h2, params=params, timeout=5,
            )
            d2_list = r2.json().get("output", [])
            row["strength"] = float(d2_list[0].get("tday_rltv", 0)) if d2_list else 0.0

            time.sleep(0.05)

            # 3) 호가 — 매도호가1, 매수호가1
            h3 = {**headers, "tr_id": "FHKST01010200"}
            r3 = requests.get(
                f"{base}/uapi/domestic-stock/v1/quotations/inquire-asking-price-exp-ccn",
                headers=h3, params=params, timeout=5,
            )
            d3 = r3.json().get("output1", {})
            row["ask1"] = int(d3.get("askp1", 0))
            row["bid1"] = int(d3.get("bidp1", 0))

            # 성공 → 실패 카운터 리셋
            self._consecutive_failures = 0
            self._feed_suspended = False
            return row

        except Exception as e:
            self._consecutive_failures += 1
            logger.warning(f"[{code}] 스냅샷 실패 ({self._consecutive_failures}연속): {e}")

            # 연속 실패 시 브로커 재생성 (토큰 만료 가능성)
            if self._consecutive_failures == 3:
                logger.info("3연속 실패 → 브로커 재생성 시도")
                self._reset_broker()

            # 5회 연속 → 데이터 피드 중단 경고
            if self._consecutive_failures >= self._max_failures:
                self._feed_suspended = True
                logger.error(f"데이터 피드 중단 감지: {self._consecutive_failures}회 연속 실패")

            return None

    # ── 4팩터 스코어링 ──

    def _score_momentum(self, snap: dict, pos: PositionState) -> float:
        """가격 모멘텀 (0~25)
        - 등락률 양수 + 진입가 대비 상승 → 고점수
        - 등락률 음수 + 하락 중 → 저점수
        """
        change_rate = snap.get("change_rate", 0)
        price = snap.get("price", 0)
        pnl_pct = (price - pos.entry_price) / pos.entry_price * 100 if pos.entry_price > 0 else 0

        score = 0.0

        # 당일 등락률 (0~12)
        if change_rate >= 5:
            score += 12
        elif change_rate >= 3:
            score += 10
        elif change_rate >= 1:
            score += 8
        elif change_rate >= 0:
            score += 5
        elif change_rate >= -2:
            score += 3
        else:
            score += 0

        # 진입가 대비 수익률 (0~13)
        if pnl_pct >= 10:
            score += 13
        elif pnl_pct >= 5:
            score += 10
        elif pnl_pct >= 2:
            score += 8
        elif pnl_pct >= 0:
            score += 5
        elif pnl_pct >= -3:
            score += 3
        else:
            score += 0

        return min(25, score)

    def _score_volume_flow(self, snap: dict, pos: PositionState) -> float:
        """거래량 흐름 (0~25)
        - 체결량 증가 + 가격 상승 → 강한 매수세
        - 체결량 감소 or 가격 하락 시 거래량 급증 → 약세
        """
        volume = snap.get("volume", 0)
        price = snap.get("price", 0)
        change_rate = snap.get("change_rate", 0)

        # 체결량 계산
        tick_vol = volume - pos.prev_volume if pos.prev_volume > 0 else 0

        score = 12.0  # 기본 중립

        if tick_vol > 0 and pos.prev_volume > 0:
            # 거래량 있음
            if change_rate >= 0:
                # 상승 + 거래량 → 좋은 신호
                score = min(25, 15 + tick_vol / max(1, pos.prev_volume) * 10)
            else:
                # 하락 + 거래량 → 매도 압력
                score = max(0, 10 - abs(change_rate) * 2)
        elif tick_vol == 0 and pos.prev_volume > 0:
            # 거래량 없음 (건조)
            score = 8 if change_rate >= 0 else 5

        return min(25, max(0, score))

    def _score_strength(self, snap: dict) -> float:
        """체결강도 (0~25)
        - 체결강도 > 120: 강한 매수세 (25)
        - 체결강도 100: 중립 (12~15)
        - 체결강도 < 80: 매도세 우위 (0~5)
        """
        strength = snap.get("strength", 0)

        if strength >= 150:
            return 25
        elif strength >= 120:
            return 22
        elif strength >= 110:
            return 18
        elif strength >= 100:
            return 15
        elif strength >= 90:
            return 10
        elif strength >= 80:
            return 6
        else:
            return max(0, strength / 80 * 5)

    def _score_orderbook(self, snap: dict) -> float:
        """호가 불균형 (0~25)
        - bid > ask: 매수세 우위 (스프레드가 좁을수록 좋음)
        - ask > bid: 매도세 우위
        """
        ask1 = snap.get("ask1", 0)
        bid1 = snap.get("bid1", 0)
        price = snap.get("price", 0)

        if ask1 <= 0 or bid1 <= 0 or price <= 0:
            return 12  # 중립

        # 스프레드 비율
        spread_pct = (ask1 - bid1) / price * 100

        # 현재가가 bid에 가까우면 매수세, ask에 가까우면 매도세
        if ask1 == bid1:
            position_score = 12
        else:
            mid = (ask1 + bid1) / 2
            price_pos = (price - bid1) / (ask1 - bid1)  # 0=bid근처, 1=ask근처
            position_score = (1 - price_pos) * 20 + 5  # bid근처=25, ask근처=5

        # 스프레드 보정 (좁을수록 좋음)
        spread_bonus = max(0, 5 - spread_pct * 10)

        return min(25, max(0, position_score + spread_bonus))

    # ── 트레일링 스탑 ──

    def _update_trailing(self, pos: PositionState, current_price: int):
        """트레일링 스탑 로직
        - 수익 5%+ → SL = 진입가 (본절 확보)
        - 수익 10%+ → SL = 고점 * (1 - trailing_distance)
        """
        if current_price > pos.high_since_entry:
            pos.high_since_entry = current_price

        pnl_pct = (current_price - pos.entry_price) / pos.entry_price * 100

        # 1단계: 본절 확보 (수익 5%+)
        if pnl_pct >= self._breakeven_pct and not pos.breakeven_activated:
            pos.current_sl = pos.entry_price
            pos.breakeven_activated = True
            logger.info(f"[{pos.code}] 본절 SL 발동: SL={pos.entry_price:,} (수익 {pnl_pct:.1f}%)")

        # 2단계: 트레일링 (수익 10%+)
        if pnl_pct >= self._trailing_start_pct:
            trailing_sl = int(pos.high_since_entry * (1 - self._trailing_distance_pct / 100))
            if trailing_sl > pos.current_sl:
                pos.current_sl = trailing_sl
                pos.trailing_activated = True
                logger.info(f"[{pos.code}] 트레일링 SL: {trailing_sl:,} "
                           f"(고점 {pos.high_since_entry:,}, 수익 {pnl_pct:.1f}%)")

    # ── 결정 엔진 ──

    def _decide(self, pos: PositionState, snap: dict,
                realtime_score: float) -> Tuple[str, str]:
        """AI 결정 엔진

        Returns: (decision, reason)
        """
        price = snap.get("price", 0)
        pnl_pct = (price - pos.entry_price) / pos.entry_price * 100 if pos.entry_price > 0 else 0

        # 1) 긴급: SL 히트
        if price <= pos.current_sl:
            return "FULL_SELL", f"SL 히트 ({pos.current_sl:,}원)"

        # 2) TP 히트
        if price >= pos.current_tp:
            return "FULL_SELL", f"TP 달성 ({pos.current_tp:,}원)"

        # 3) 추세 악화 감지 (최근 5회 연속 하락)
        if len(pos.score_history) >= 5:
            recent_5 = list(pos.score_history)[-5:]
            declining = all(recent_5[i] > recent_5[i+1] for i in range(4))
            if declining and pnl_pct > 0:
                return "PARTIAL_SELL", f"5연속 점수 하락 (수익 {pnl_pct:.1f}% 확보)"

        # 4) AI 점수 기반 결정
        if realtime_score >= 70:
            return "HOLD", f"강한 모멘텀 (AI:{realtime_score:.0f})"

        elif realtime_score >= 50:
            return "HOLD", f"적절한 흐름 (AI:{realtime_score:.0f})"

        elif realtime_score >= 30:
            if pnl_pct >= self._breakeven_pct:
                return "PARTIAL_SELL", f"모멘텀 약화 + 수익 {pnl_pct:.1f}% (50% 이익실현)"
            return "HOLD", f"모멘텀 약화, 관망 (AI:{realtime_score:.0f})"

        else:  # score < 30
            if pnl_pct >= 3:
                return "PARTIAL_SELL", f"약세 전환 + 수익 {pnl_pct:.1f}% (50% 이익실현)"
            elif pnl_pct <= -3:
                return "FULL_SELL", f"악화 중 + 손실 {pnl_pct:.1f}% (SL 전 탈출)"
            return "HOLD", f"약세 but 관망 (AI:{realtime_score:.0f}, PnL:{pnl_pct:.1f}%)"

    # ── 종목별 평가 ──

    def evaluate_position(self, code: str) -> Optional[RealtimeSnapshot]:
        """1종목 실시간 평가"""
        pos = self._positions.get(code)
        if not pos:
            logger.warning(f"미등록 종목: {code}")
            return None

        snap = self._fetch_snapshot(code)
        if not snap:
            return None

        price = snap.get("price", 0)
        if price <= 0:
            return None

        # 트레일링 업데이트
        self._update_trailing(pos, price)

        # 체결량 계산
        volume = snap.get("volume", 0)
        tick_vol = volume - pos.prev_volume if pos.prev_volume > 0 else 0
        pos.prev_volume = volume

        # 4팩터 스코어링
        s_mom = self._score_momentum(snap, pos)
        s_vol = self._score_volume_flow(snap, pos)
        s_str = self._score_strength(snap)
        s_ob = self._score_orderbook(snap)
        total = s_mom + s_vol + s_str + s_ob

        # 점수 히스토리
        pos.score_history.append(total)

        # 결정
        decision, reason = self._decide(pos, snap, total)

        pnl_pct = (price - pos.entry_price) / pos.entry_price * 100

        return RealtimeSnapshot(
            code=code, name=pos.name,
            timestamp=snap.get("time", ""),
            price=price,
            change_rate=snap.get("change_rate", 0),
            volume=volume,
            tick_volume=tick_vol,
            strength=snap.get("strength", 0),
            ask1=snap.get("ask1", 0),
            bid1=snap.get("bid1", 0),
            score_momentum=s_mom,
            score_volume=s_vol,
            score_strength=s_str,
            score_orderbook=s_ob,
            realtime_score=total,
            entry_price=pos.entry_price,
            current_sl=pos.current_sl,
            current_tp=pos.current_tp,
            pnl_pct=pnl_pct,
            high_since_entry=pos.high_since_entry,
            decision=decision,
            decision_reason=reason,
        )

    def evaluate_all(self) -> List[RealtimeSnapshot]:
        """전체 보유종목 평가"""
        results = []
        for code in list(self._positions.keys()):
            snap = self.evaluate_position(code)
            if snap:
                results.append(snap)
            time.sleep(0.1)  # API 속도 제한
        return results

    # ── 리포트 포맷 ──

    def format_snapshot_report(self, snapshots: List[RealtimeSnapshot]) -> str:
        """텔레그램용 실시간 리포트"""
        if not snapshots:
            return "AI 모니터: 보유 종목 없음"

        lines = [
            "AI 모니터 실시간 리포트",
            f"{'=' * 30}",
        ]

        for s in snapshots:
            # 결정 이모지
            emoji = {
                "HOLD": "O",
                "PARTIAL_SELL": "!",
                "FULL_SELL": "X",
                "TRAILING_STOP": "T",
            }.get(s.decision, "?")

            lines.append(f"\n[{emoji}] {s.name}({s.code})")
            lines.append(f"  현재: {s.price:,}원 ({s.change_rate:+.1f}%) | PnL: {s.pnl_pct:+.1f}%")
            lines.append(f"  AI: {s.realtime_score:.0f}/100 "
                         f"[M:{s.score_momentum:.0f} V:{s.score_volume:.0f} "
                         f"S:{s.score_strength:.0f} O:{s.score_orderbook:.0f}]")
            lines.append(f"  체결강도: {s.strength:.0f} | SL: {s.current_sl:,} | TP: {s.current_tp:,}")
            lines.append(f"  결정: {s.decision} - {s.decision_reason}")

        lines.append(f"\n{datetime.now().strftime('%H:%M:%S')}")
        return "\n".join(lines)

    def format_decision_alert(self, snap: RealtimeSnapshot) -> str:
        """매도 결정 시 알림 메시지"""
        if snap.decision == "FULL_SELL":
            return (f"[전량매도] {snap.name}({snap.code})\n"
                    f"현재: {snap.price:,}원 | PnL: {snap.pnl_pct:+.1f}%\n"
                    f"사유: {snap.decision_reason}\n"
                    f"AI: {snap.realtime_score:.0f}/100")
        elif snap.decision == "PARTIAL_SELL":
            return (f"[50% 이익실현] {snap.name}({snap.code})\n"
                    f"현재: {snap.price:,}원 | PnL: {snap.pnl_pct:+.1f}%\n"
                    f"사유: {snap.decision_reason}\n"
                    f"AI: {snap.realtime_score:.0f}/100")
        return ""


# ── CLI 테스트 ──
if __name__ == "__main__":
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).parent.parent))
    from dotenv import load_dotenv
    load_dotenv(str(Path(__file__).parent.parent.parent / ".env"))

    logging.basicConfig(level=logging.INFO, format="%(message)s")

    monitor = RealtimeMonitor()

    # 테스트: 삼성전자 시뮬레이션
    test_code = "005930"
    monitor.register_position(
        code=test_code, name="삼성전자",
        entry_price=55000, sl=52000, tp=60000,
    )

    print("1회 평가 실행...")
    snap = monitor.evaluate_position(test_code)
    if snap:
        print(monitor.format_snapshot_report([snap]))
    else:
        print("스냅샷 실패 (장외시간이거나 API 오류)")
