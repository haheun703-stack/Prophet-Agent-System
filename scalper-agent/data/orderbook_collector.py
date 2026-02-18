"""
호가 잔량 수집기 — 매수벽/매도벽 감지

현재: KIS API 스냅샷 방식 (REST)
향후: KIS WebSocket 실시간 구독

핵심 지표:
  - 매수잔량 합계 vs 매도잔량 합계 → 매수/매도 비율
  - 상위 3호가 집중도 → 벽의 위치 감지
  - 호가 스프레드 → 유동성 상태

사용법:
  # REST 스냅샷 (장중 사용)
  from data.orderbook_collector import get_orderbook_snapshot
  ob = get_orderbook_snapshot("005930")

  # 분석
  from data.orderbook_collector import analyze_orderbook
  result = analyze_orderbook(ob)
"""

import os
import logging
from dataclasses import dataclass
from typing import Optional, Dict

logger = logging.getLogger(__name__)


@dataclass
class OrderbookSnapshot:
    """10호가 스냅샷"""
    code: str
    timestamp: str

    # 매도호가 (1호가=최우선, 가격 낮은 순)
    ask_prices: list    # [ask1, ask2, ..., ask10]
    ask_volumes: list   # [vol1, vol2, ..., vol10]

    # 매수호가 (1호가=최우선, 가격 높은 순)
    bid_prices: list    # [bid1, bid2, ..., bid10]
    bid_volumes: list   # [vol1, vol2, ..., vol10]

    @property
    def total_ask_volume(self) -> int:
        return sum(self.ask_volumes)

    @property
    def total_bid_volume(self) -> int:
        return sum(self.bid_volumes)

    @property
    def bid_ask_ratio(self) -> float:
        """매수/매도 비율 (>1 = 매수벽 우세)"""
        if self.total_ask_volume == 0:
            return 0
        return self.total_bid_volume / self.total_ask_volume

    @property
    def spread_pct(self) -> float:
        """호가 스프레드 (%)"""
        if not self.bid_prices or not self.ask_prices or self.bid_prices[0] == 0:
            return 0
        return (self.ask_prices[0] - self.bid_prices[0]) / self.bid_prices[0] * 100


@dataclass
class OrderbookAnalysis:
    """호가 분석 결과"""
    code: str
    bid_ask_ratio: float          # 매수/매도 비율
    top3_bid_concentration: float  # 상위 3호가 매수 집중도 (%)
    top3_ask_concentration: float  # 상위 3호가 매도 집중도 (%)
    spread_pct: float             # 스프레드 (%)
    buy_wall: bool                # 매수벽 존재
    sell_wall: bool               # 매도벽 존재
    score: float                  # 0~25 (수급분석 통합용)


def get_orderbook_snapshot_kis(code: str) -> Optional[OrderbookSnapshot]:
    """KIS API로 10호가 스냅샷 가져오기

    주의: 장중(09:00~15:30)에만 유효한 데이터
    KIS API 호가 TR: FHKST01010200
    """
    try:
        from dotenv import load_dotenv
        load_dotenv()
        import mojito

        broker = mojito.KoreaInvestment(
            api_key=os.getenv("KIS_APP_KEY"),
            api_secret=os.getenv("KIS_APP_SECRET"),
            acc_no=os.getenv("KIS_ACC_NO"),
            mock=False,
        )

        resp = broker.fetch_price(code)
        # KIS API 호가 조회는 별도 엔드포인트 필요
        # 현재 mojito2에서 직접 지원 안 할 수 있음
        # → 향후 REST 직접 호출 또는 WebSocket으로 대체

        logger.warning("KIS 호가 스냅샷: mojito2 직접 지원 확인 필요")
        return None

    except Exception as e:
        logger.warning(f"호가 스냅샷 실패 {code}: {e}")
        return None


def analyze_orderbook(ob: OrderbookSnapshot) -> OrderbookAnalysis:
    """호가 잔량 분석"""
    if ob is None:
        return OrderbookAnalysis(
            code="", bid_ask_ratio=1.0,
            top3_bid_concentration=0, top3_ask_concentration=0,
            spread_pct=0, buy_wall=False, sell_wall=False, score=12.5,
        )

    # 상위 3호가 집중도
    top3_bid = sum(ob.bid_volumes[:3])
    top3_ask = sum(ob.ask_volumes[:3])
    bid_conc = top3_bid / max(ob.total_bid_volume, 1) * 100
    ask_conc = top3_ask / max(ob.total_ask_volume, 1) * 100

    # 벽 판정 (상위 3호가에 70% 이상 집중)
    buy_wall = bid_conc > 70 and ob.bid_ask_ratio > 1.5
    sell_wall = ask_conc > 70 and ob.bid_ask_ratio < 0.7

    # 점수 (0~25)
    score = 12.5  # 기본 중립
    if ob.bid_ask_ratio > 2.0:
        score = 25       # 강한 매수벽
    elif ob.bid_ask_ratio > 1.5:
        score = 20
    elif ob.bid_ask_ratio > 1.2:
        score = 16
    elif ob.bid_ask_ratio < 0.5:
        score = 0        # 강한 매도벽
    elif ob.bid_ask_ratio < 0.7:
        score = 5
    elif ob.bid_ask_ratio < 0.9:
        score = 9

    return OrderbookAnalysis(
        code=ob.code,
        bid_ask_ratio=ob.bid_ask_ratio,
        top3_bid_concentration=bid_conc,
        top3_ask_concentration=ask_conc,
        spread_pct=ob.spread_pct,
        buy_wall=buy_wall,
        sell_wall=sell_wall,
        score=score,
    )
